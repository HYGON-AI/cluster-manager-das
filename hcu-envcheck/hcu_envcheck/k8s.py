# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Any

from .models import CommandResult


KUBECTL_OUTPUT_CAPTURE_LIMIT_BYTES = 8 * 1024 * 1024
KUBECTL_OUTPUT_READ_CHUNK_BYTES = 64 * 1024
KUBECTL_PROCESS_TERMINATION_GRACE_SECONDS = 2.0
K8S_NODE_BLOCKING_CONDITIONS = {
    "MemoryPressure",
    "DiskPressure",
    "PIDPressure",
    "NetworkUnavailable",
}
K8S_STANDARD_HEALTH_TAINT_KEYS = {
    "node.kubernetes.io/not-ready",
    "node.kubernetes.io/unreachable",
    "node.kubernetes.io/memory-pressure",
    "node.kubernetes.io/disk-pressure",
    "node.kubernetes.io/pid-pressure",
    "node.kubernetes.io/network-unavailable",
    "node.kubernetes.io/unschedulable",
    "node.kubernetes.io/out-of-service",
}


class _BoundedByteCapture:
    """Drain one pipe while retaining a bounded head/tail and exact byte count."""

    def __init__(self, limit_bytes: int):
        if limit_bytes < 1024:
            raise ValueError("kubectl output capture limit must be at least 1024 bytes")
        self.limit_bytes = limit_bytes
        self.head_limit = limit_bytes // 2
        self.tail_limit = limit_bytes - self.head_limit
        self.head = bytearray()
        self.tail = bytearray()
        self.total_bytes = 0

    def feed(self, value: str | bytes | None) -> None:
        if isinstance(value, str):
            chunk = value.encode("utf-8", "replace")
        else:
            chunk = value or b""
        if not chunk:
            return
        self.total_bytes += len(chunk)
        needed = self.head_limit - len(self.head)
        if needed > 0:
            self.head.extend(chunk[:needed])
            chunk = chunk[needed:]
        if chunk:
            self.tail.extend(chunk)
            overflow = len(self.tail) - self.tail_limit
            if overflow > 0:
                del self.tail[:overflow]

    @property
    def truncated(self) -> bool:
        return self.total_bytes > self.limit_bytes

    def render(self) -> str:
        head = bytes(self.head).decode("utf-8", "replace")
        tail = bytes(self.tail).decode("utf-8", "replace")
        if not self.truncated:
            return head + tail
        omitted = self.total_bytes - len(self.head) - len(self.tail)
        return f"{head}\n...[HCU_ENVCHECK omitted {omitted} output bytes]...\n{tail}"

    def drain(self, pipe: Any) -> None:
        try:
            while True:
                chunk = pipe.read(KUBECTL_OUTPUT_READ_CHUNK_BYTES)
                if not chunk:
                    break
                self.feed(chunk)
        finally:
            try:
                pipe.close()
            except (OSError, ValueError):
                pass


@dataclass
class _BoundedProcessResult:
    argv: list[str]
    returncode: int
    stdout: str
    stderr: str
    duration_seconds: float
    timed_out: bool
    stdout_total_bytes: int
    stderr_total_bytes: int
    stdout_truncated: bool
    stderr_truncated: bool


def _terminate_process(process: Any) -> None:
    try:
        process.terminate()
    except OSError:
        pass
    try:
        process.wait(timeout=KUBECTL_PROCESS_TERMINATION_GRACE_SECONDS)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        process.kill()
    except OSError:
        pass
    try:
        process.wait(timeout=KUBECTL_PROCESS_TERMINATION_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        # Closing controller-side pipes below lets reader threads release memory
        # even if OS-level process reaping is delayed.
        pass


def _run_bounded_process(
    argv: list[str],
    *,
    timeout: float,
    input_text: str | None = None,
    capture_limit_bytes: int = KUBECTL_OUTPUT_CAPTURE_LIMIT_BYTES,
) -> _BoundedProcessResult:
    """Run a kubectl-style command without accumulating an unbounded pipe."""

    if timeout <= 0:
        raise ValueError("kubectl command timeout must be greater than zero")
    stdout_capture = _BoundedByteCapture(capture_limit_bytes)
    stderr_capture = _BoundedByteCapture(capture_limit_bytes)
    started = time.monotonic()
    process = subprocess.Popen(
        argv,
        stdin=subprocess.PIPE if input_text is not None else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if process.stdout is None or process.stderr is None:
        _terminate_process(process)
        raise RuntimeError("kubectl process did not expose stdout/stderr pipes")
    readers = [
        threading.Thread(
            target=stdout_capture.drain,
            args=(process.stdout,),
            daemon=True,
            name="hcu-envcheck-kubectl-stdout",
        ),
        threading.Thread(
            target=stderr_capture.drain,
            args=(process.stderr,),
            daemon=True,
            name="hcu-envcheck-kubectl-stderr",
        ),
    ]
    for reader in readers:
        reader.start()

    if input_text is not None and process.stdin is not None:
        try:
            process.stdin.write(input_text.encode("utf-8"))
            process.stdin.flush()
        except (BrokenPipeError, OSError):
            # Match communicate(): an early child exit is represented by its
            # return code and stderr rather than a controller-side exception.
            pass
        finally:
            try:
                process.stdin.close()
            except OSError:
                pass

    timed_out = False
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        _terminate_process(process)
    finally:
        for reader in readers:
            reader.join(timeout=KUBECTL_PROCESS_TERMINATION_GRACE_SECONDS + 1.0)
        if any(reader.is_alive() for reader in readers):
            for pipe in (process.stdout, process.stderr):
                try:
                    pipe.close()
                except (OSError, ValueError):
                    pass
            for reader in readers:
                reader.join(timeout=1.0)
        if any(reader.is_alive() for reader in readers):
            raise RuntimeError("kubectl output reader did not terminate")

    return _BoundedProcessResult(
        argv=list(argv),
        returncode=124 if timed_out else int(process.returncode or 0),
        stdout=stdout_capture.render(),
        stderr=stderr_capture.render(),
        duration_seconds=time.monotonic() - started,
        timed_out=timed_out,
        stdout_total_bytes=stdout_capture.total_bytes,
        stderr_total_bytes=stderr_capture.total_bytes,
        stdout_truncated=stdout_capture.truncated,
        stderr_truncated=stderr_capture.truncated,
    )


def _normalized_mapping_list(value: Any, fields: tuple[str, ...]) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    normalized: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        normalized.append({field: item.get(field) for field in fields if field in item})
    return normalized


class KubectlError(RuntimeError):
    pass


@dataclass
class KubernetesPodTarget:
    namespace: str
    pod: str
    container: str
    context: str | None = None
    kubeconfig: str | None = None
    device_resource_name: str = "hygon.com/hcu"


class KubernetesPodExecutor:
    def __init__(
        self,
        target: KubernetesPodTarget,
        timeout_seconds: float = 30.0,
        output_capture_limit_bytes: int = KUBECTL_OUTPUT_CAPTURE_LIMIT_BYTES,
    ):
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero")
        if output_capture_limit_bytes < 1024:
            raise ValueError("output_capture_limit_bytes must be at least 1024")
        self.target = target
        self.timeout_seconds = timeout_seconds
        self.output_capture_limit_bytes = output_capture_limit_bytes

    def _kubectl_base(self) -> list[str]:
        argv = ["kubectl"]
        if self.target.kubeconfig:
            argv.extend(["--kubeconfig", self.target.kubeconfig])
        if self.target.context:
            argv.extend(["--context", self.target.context])
        return argv

    def _run(
        self,
        name: str,
        argv: list[str],
        timeout: float | None = None,
        input_text: str | None = None,
    ) -> CommandResult:
        completed = _run_bounded_process(
            argv,
            input_text=input_text,
            timeout=timeout if timeout is not None else self.timeout_seconds,
            capture_limit_bytes=self.output_capture_limit_bytes,
        )
        return CommandResult(
            name=name,
            argv=argv,
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
            duration_seconds=completed.duration_seconds,
            timed_out=completed.timed_out,
            stdout_total_bytes=completed.stdout_total_bytes,
            stderr_total_bytes=completed.stderr_total_bytes,
            stdout_truncated=completed.stdout_truncated,
            stderr_truncated=completed.stderr_truncated,
        )

    def pod_identity(self) -> tuple[dict[str, Any], CommandResult]:
        argv = self._kubectl_base() + [
            "get",
            "pod",
            "-n",
            self.target.namespace,
            self.target.pod,
            "-o",
            "json",
        ]
        result = self._run("pod_identity", argv)
        if result.returncode != 0:
            raise KubectlError(result.stderr.strip() or "kubectl get pod failed")
        if result.stdout_truncated or result.stderr_truncated:
            raise KubectlError(
                "kubectl get pod output truncated: "
                f"stdout_total_bytes={result.stdout_total_bytes}, "
                f"stderr_total_bytes={result.stderr_total_bytes}"
            )
        payload = json.loads(result.stdout)
        containers = {item["name"] for item in payload.get("spec", {}).get("containers", [])}
        if self.target.container not in containers:
            raise KubectlError(
                f"container {self.target.container!r} not present in pod {self.target.pod!r}"
            )
        container_spec = next(
            (
                item
                for item in payload.get("spec", {}).get("containers", [])
                if item.get("name") == self.target.container
            ),
            {},
        )
        status_by_name = {
            item.get("name"): item
            for item in payload.get("status", {}).get("containerStatuses", [])
        }
        container_status = status_by_name.get(self.target.container, {})
        state = container_status.get("state", {})
        state_name = next(
            (name for name in ("running", "waiting", "terminated") if state.get(name)),
            None,
        )
        state_detail = state.get(state_name, {}) if state_name else {}
        identity = {
            "platform": "k8s",
            "namespace": self.target.namespace,
            "pod": self.target.pod,
            "pod_uid": payload.get("metadata", {}).get("uid"),
            "container": self.target.container,
            "node": payload.get("spec", {}).get("nodeName"),
            "phase": payload.get("status", {}).get("phase"),
            "image": next(
                (
                    item.get("image")
                    for item in payload.get("spec", {}).get("containers", [])
                    if item.get("name") == self.target.container
                ),
                None,
            ),
            "image_id": container_status.get("imageID"),
            "container_id": container_status.get("containerID"),
            "container_ready": container_status.get("ready"),
            "container_restart_count": container_status.get("restartCount"),
            "container_state": state_name,
            "container_state_reason": state_detail.get("reason"),
            "container_state_message": state_detail.get("message"),
            "host_network": bool(payload.get("spec", {}).get("hostNetwork", False)),
            "container_privileged": bool(
                container_spec.get("securityContext", {}).get("privileged", False)
            ),
            "device_resource_name": self.target.device_resource_name,
            "device_request": container_spec.get("resources", {})
            .get("requests", {})
            .get(self.target.device_resource_name),
            "device_limit": container_spec.get("resources", {})
            .get("limits", {})
            .get(self.target.device_resource_name),
        }
        # Do not persist the complete Pod object: literal environment variables or
        # admission metadata may contain customer-sensitive values. Evidence keeps
        # only the identity fields required by this check.
        result.stdout = json.dumps(identity, ensure_ascii=False, sort_keys=True) + "\n"
        return identity, result

    def node_identity(self, node_name: str) -> tuple[dict[str, Any], CommandResult]:
        argv = self._kubectl_base() + ["get", "node", node_name, "-o", "json"]
        result = self._run("node_identity", argv)
        if result.returncode != 0:
            raise KubectlError(result.stderr.strip() or "kubectl get node failed")
        if result.stdout_truncated or result.stderr_truncated:
            raise KubectlError(
                "kubectl get node output truncated: "
                f"stdout_total_bytes={result.stdout_total_bytes}, "
                f"stderr_total_bytes={result.stderr_total_bytes}"
            )
        payload = json.loads(result.stdout)
        conditions = _normalized_mapping_list(
            payload.get("status", {}).get("conditions", []),
            (
                "type",
                "status",
                "reason",
                "message",
                "lastHeartbeatTime",
                "lastTransitionTime",
            ),
        )
        taints = _normalized_mapping_list(
            payload.get("spec", {}).get("taints", []),
            ("key", "value", "effect", "timeAdded"),
        )
        identity = {
            "node": node_name,
            "node_uid": payload.get("metadata", {}).get("uid"),
            "conditions": conditions,
            "taints": taints,
            "ready": next(
                (
                    item.get("status")
                    for item in payload.get("status", {}).get("conditions", [])
                    if item.get("type") == "Ready"
                ),
                None,
            ),
            "unschedulable": bool(payload.get("spec", {}).get("unschedulable", False)),
            "device_capacity": payload.get("status", {})
            .get("capacity", {})
            .get(self.target.device_resource_name),
            "device_allocatable": payload.get("status", {})
            .get("allocatable", {})
            .get(self.target.device_resource_name),
            "cpu_capacity": payload.get("status", {}).get("capacity", {}).get("cpu"),
            "memory_capacity": payload.get("status", {}).get("capacity", {}).get("memory"),
            "kernel_version": payload.get("status", {}).get("nodeInfo", {}).get("kernelVersion"),
            "os_image": payload.get("status", {}).get("nodeInfo", {}).get("osImage"),
            "container_runtime_version": payload.get("status", {})
            .get("nodeInfo", {})
            .get("containerRuntimeVersion"),
            "kubelet_version": payload.get("status", {}).get("nodeInfo", {}).get("kubeletVersion"),
        }
        result.stdout = json.dumps(identity, ensure_ascii=False, sort_keys=True) + "\n"
        return identity, result

    def exec(self, name: str, command: list[str], timeout: float | None = None) -> CommandResult:
        argv = self._kubectl_base() + [
            "exec",
            "-n",
            self.target.namespace,
            self.target.pod,
            "-c",
            self.target.container,
            "--",
        ] + command
        return self._run(name, argv, timeout=timeout)

    def exec_stdin(
        self,
        name: str,
        command: list[str],
        input_text: str,
        timeout: float | None = None,
    ) -> CommandResult:
        argv = self._kubectl_base() + [
            "exec",
            "-i",
            "-n",
            self.target.namespace,
            self.target.pod,
            "-c",
            self.target.container,
            "--",
        ] + command
        return self._run(name, argv, timeout=timeout, input_text=input_text)

    def resolve_tool(self, preferred: str, fallback: str | None = None) -> tuple[str | None, CommandResult]:
        script = f"command -v {preferred}"
        if fallback:
            script += f" || command -v {fallback}"
        result = self.exec(f"resolve_{preferred}", ["sh", "-c", script])
        path = result.stdout.strip().splitlines()[0] if result.returncode == 0 and result.stdout.strip() else None
        if path and (not path.startswith("/") or any(ch.isspace() for ch in path)):
            path = None
        return path, result
