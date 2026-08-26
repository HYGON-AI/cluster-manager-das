# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import hashlib
import math
import re
import threading
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import __version__

from .k8s import (
    K8S_NODE_BLOCKING_CONDITIONS as NODE_BLOCKING_CONDITIONS,
    K8S_STANDARD_HEALTH_TAINT_KEYS as STANDARD_HEALTH_TAINT_KEYS,
    KUBECTL_OUTPUT_CAPTURE_LIMIT_BYTES,
    KubernetesPodExecutor,
    KubernetesPodTarget,
    _BoundedProcessResult,
    _run_bounded_process,
)
from .models import Finding
from .output import atomic_write_text_exclusive, claim_output_directory
from .preflight import run_k8s_hcu_preflight, save_result, validate_environment_profile


DEFAULT_K8S_CLUSTER_CONCURRENCY = 16
MAX_K8S_CLUSTER_CONCURRENCY = 128
DEFAULT_KUBECTL_API_QPS = 20.0
DEFAULT_KUBECTL_API_BURST = 40
MAX_KUBECTL_API_QPS = 100.0
MAX_KUBECTL_API_BURST = 256


PROBE_ENV_ALLOWLIST = {
    "PATH",
    "LD_LIBRARY_PATH",
    "PYTHONPATH",
    "ROCM_PATH",
    "HIP_PATH",
    "ROCR_VISIBLE_DEVICES",
    "HIP_VISIBLE_DEVICES",
    "CUDA_VISIBLE_DEVICES",
}


class _TokenBucketRateLimiter:
    """One process-wide limiter shared by all kubectl subprocesses in a run."""

    def __init__(self, qps: float, burst: int):
        self.qps = float(qps)
        self.burst = int(burst)
        self._tokens = float(burst)
        self._updated_at = time.monotonic()
        self._request_count = 0
        self._lock = threading.Lock()

    @property
    def request_count(self) -> int:
        with self._lock:
            return self._request_count

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = max(0.0, now - self._updated_at)
                self._tokens = min(
                    float(self.burst), self._tokens + elapsed * self.qps
                )
                self._updated_at = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    self._request_count += 1
                    return
                wait_seconds = (1.0 - self._tokens) / self.qps
            time.sleep(wait_seconds)


class _RateLimitedKubernetesPodExecutor(KubernetesPodExecutor):
    def __init__(self, *args: Any, api_limiter: _TokenBucketRateLimiter, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._api_limiter = api_limiter

    def _run(self, *args: Any, **kwargs: Any):
        self._api_limiter.acquire()
        return super()._run(*args, **kwargs)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_name(value: str) -> str:
    value = re.sub(r"[^a-z0-9-]+", "-", value.lower()).strip("-")
    return value or "node"


def _probe_pod_name(run_id: str, node: str) -> str:
    digest = hashlib.sha256(node.encode("utf-8", "replace")).hexdigest()[:8]
    fixed = f"hcu-envcheck-{run_id}--{digest}"
    node_budget = 63 - len(fixed)
    node_token = _safe_name(node)[:node_budget].rstrip("-") or "node"
    return f"hcu-envcheck-{run_id}-{node_token}-{digest}"

def parse_reuse_pods(values: list[str]) -> dict[str, tuple[str, str, str]]:
    result: dict[str, tuple[str, str, str]] = {}
    for value in values:
        node, separator, reference = value.partition("=")
        parts = reference.split("/")
        if not separator or not node or len(parts) != 3 or not all(parts):
            raise ValueError(
                f"invalid --reuse-pod {value!r}; expected NODE=NAMESPACE/POD/CONTAINER"
            )
        if node in result:
            raise ValueError(f"duplicate --reuse-pod mapping for node {node!r}")
        result[node] = (parts[0], parts[1], parts[2])
    return result


def parse_probe_env(values: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        name, separator, content = value.partition("=")
        if not separator or name not in PROBE_ENV_ALLOWLIST:
            raise ValueError(
                f"invalid --probe-env {value!r}; allowed names: {', '.join(sorted(PROBE_ENV_ALLOWLIST))}"
            )
        if name in result:
            raise ValueError(f"duplicate --probe-env {name!r}")
        result[name] = content
    return result


def build_probe_manifest(
    *,
    namespace: str,
    pod_name: str,
    container_name: str,
    node: str,
    image: str,
    image_pull_policy: str,
    device_resource_name: str,
    device_count: int,
    run_id: str,
    node_taints: list[dict[str, Any]],
    active_deadline_seconds: int,
    memory_request: str = "1Gi",
    memory_limit: str = "8Gi",
    probe_env: dict[str, str] | None = None,
) -> dict[str, Any]:
    tolerations: list[dict[str, Any]] = []
    for taint in node_taints:
        effect = taint.get("effect")
        key = taint.get("key")
        if effect not in {"NoSchedule", "NoExecute"} or not key:
            continue
        # Never bypass Kubernetes health/eviction taints. The only automatic
        # exception is the explicitly selected HCU resource's dedicated taint.
        if key in STANDARD_HEALTH_TAINT_KEYS or key != device_resource_name:
            continue
        tolerations.append(
            {"key": key, "operator": "Exists", "effect": effect}
        )

    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": pod_name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "hcu-envcheck",
                "app.kubernetes.io/managed-by": "hcu-envcheck",
                "hcu-envcheck/run-id": run_id,
                "hcu-envcheck/target-node": _safe_name(node)[:63],
            },
        },
        "spec": {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "hostNetwork": True,
            "dnsPolicy": "ClusterFirstWithHostNet",
            "terminationGracePeriodSeconds": 0,
            "activeDeadlineSeconds": active_deadline_seconds,
            "nodeSelector": {"kubernetes.io/hostname": node},
            "tolerations": tolerations,
            "containers": [
                {
                    "name": container_name,
                    "image": image,
                    "imagePullPolicy": image_pull_policy,
                    "command": [
                        "sh",
                        "-c",
                        "trap 'exit 0' TERM INT; sleep 900 & wait",
                    ],
                    "resources": {
                        "requests": {
                            "cpu": "100m",
                            "memory": memory_request,
                            device_resource_name: str(device_count),
                        },
                        "limits": {
                            "cpu": "1",
                            "memory": memory_limit,
                            device_resource_name: str(device_count),
                        },
                    },
                    "securityContext": {
                        "privileged": True,
                        "allowPrivilegeEscalation": True,
                    },
                    "env": [
                        {"name": name, "value": value}
                        for name, value in sorted((probe_env or {}).items())
                    ],
                }
            ],
        },
    }


class KubectlController:
    def __init__(
        self,
        *,
        context: str | None = None,
        kubeconfig: str | None = None,
        timeout_seconds: float = 60.0,
        api_limiter: _TokenBucketRateLimiter | None = None,
    ):
        self.context = context
        self.kubeconfig = kubeconfig
        self.timeout_seconds = timeout_seconds
        self.api_limiter = api_limiter

    def _base(self) -> list[str]:
        argv = ["kubectl"]
        if self.kubeconfig:
            argv.extend(["--kubeconfig", self.kubeconfig])
        if self.context:
            argv.extend(["--context", self.context])
        return argv

    def run(
        self,
        args: list[str],
        *,
        input_text: str | None = None,
        timeout: float | None = None,
    ) -> _BoundedProcessResult:
        if self.api_limiter is not None:
            self.api_limiter.acquire()
        return _run_bounded_process(
            self._base() + args,
            input_text=input_text,
            timeout=timeout if timeout is not None else self.timeout_seconds,
        )

    def get_node(self, node: str) -> dict[str, Any]:
        completed = self.run(["get", "node", node, "-o", "json"])
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr.strip() or f"cannot get node {node}")
        if completed.stdout_truncated or completed.stderr_truncated:
            raise RuntimeError(
                f"cannot get complete node {node} evidence: kubectl output truncated "
                f"stdout_total_bytes={completed.stdout_total_bytes}, "
                f"stderr_total_bytes={completed.stderr_total_bytes}"
            )
        return json.loads(completed.stdout)

    def create_probe(self, manifest: dict[str, Any]) -> None:
        completed = self.run(
            ["create", "-f", "-"],
            input_text=json.dumps(manifest, ensure_ascii=False),
        )
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr.strip() or "probe pod creation failed")

    def wait_ready(self, namespace: str, pod: str, timeout_seconds: int) -> None:
        completed = self.run(
            [
                "wait",
                "-n",
                namespace,
                "--for=condition=Ready",
                f"pod/{pod}",
                f"--timeout={timeout_seconds}s",
            ],
            timeout=timeout_seconds + 10,
        )
        if completed.returncode != 0:
            detail = self.probe_failure_detail(namespace, pod)
            message = completed.stderr.strip() or completed.stdout.strip() or "probe pod not ready"
            raise RuntimeError(f"{message}; {detail}")

    def copy_to_pod(
        self,
        local_path: Path,
        namespace: str,
        pod: str,
        container: str,
        remote_path: str,
    ) -> None:
        completed = self.run(
            [
                "cp",
                str(local_path),
                f"{namespace}/{pod}:{remote_path}",
                "-c",
                container,
            ],
            timeout=120,
        )
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr.strip() or "kubectl cp bootstrap wheel failed")

    def probe_failure_detail(self, namespace: str, pod: str) -> str:
        pod_result = self.run(["get", "pod", "-n", namespace, pod, "-o", "json"])
        if pod_result.returncode != 0:
            return pod_result.stderr.strip() or "probe pod status unavailable"
        if pod_result.stdout_truncated or pod_result.stderr_truncated:
            return (
                "probe pod status output truncated: "
                f"stdout_total_bytes={pod_result.stdout_total_bytes}, "
                f"stderr_total_bytes={pod_result.stderr_total_bytes}"
            )
        payload = json.loads(pod_result.stdout)
        phase = payload.get("status", {}).get("phase")
        reasons: list[str] = []
        for condition in payload.get("status", {}).get("conditions", []):
            if condition.get("status") == "False" and condition.get("message"):
                reasons.append(str(condition["message"]))
        for status in payload.get("status", {}).get("containerStatuses", []):
            waiting = status.get("state", {}).get("waiting", {})
            terminated = status.get("state", {}).get("terminated", {})
            if waiting:
                reasons.append(f"{waiting.get('reason')}: {waiting.get('message', '')}".strip())
            if terminated:
                reasons.append(
                    f"{terminated.get('reason')}: exit={terminated.get('exitCode')} "
                    f"{terminated.get('message', '')}".strip()
                )
        compact = "; ".join(item for item in reasons if item)[:2000]
        return f"phase={phase}" + (f"; {compact}" if compact else "")

    def delete_probe(self, namespace: str, pod: str, run_id: str) -> tuple[str, str | None]:
        identity = self.run(["get", "pod", "-n", namespace, pod, "-o", "json"])
        if identity.returncode != 0:
            if "not found" in identity.stderr.lower():
                return "ALREADY_GONE", None
            return "CLEANUP_REQUIRED", identity.stderr.strip()
        if identity.stdout_truncated or identity.stderr_truncated:
            return (
                "CLEANUP_REQUIRED",
                "pod identity output truncated before run-id verification: "
                f"stdout_total_bytes={identity.stdout_total_bytes}, "
                f"stderr_total_bytes={identity.stderr_total_bytes}",
            )
        payload = json.loads(identity.stdout)
        actual_run_id = payload.get("metadata", {}).get("labels", {}).get("hcu-envcheck/run-id")
        if actual_run_id != run_id:
            return (
                "CLEANUP_REFUSED",
                f"pod label run-id={actual_run_id!r}, expected {run_id!r}",
            )
        completed = self.run(
            [
                "delete",
                "pod",
                "-n",
                namespace,
                pod,
                "--wait=true",
                "--timeout=60s",
            ],
            timeout=70,
        )
        if completed.returncode != 0:
            return "CLEANUP_REQUIRED", completed.stderr.strip() or completed.stdout.strip()
        return "DELETED", None


def _node_scheduling_evidence(payload: dict[str, Any]) -> dict[str, Any]:
    conditions = payload.get("status", {}).get("conditions", [])
    taints = payload.get("spec", {}).get("taints", [])
    return {
        "unschedulable": bool(payload.get("spec", {}).get("unschedulable", False)),
        "conditions": conditions if isinstance(conditions, list) else [],
        "taints": taints if isinstance(taints, list) else [],
    }


def _node_health_issues(payload: dict[str, Any]) -> list[str]:
    evidence = _node_scheduling_evidence(payload)
    issues: list[str] = []
    ready = next(
        (item.get("status") for item in evidence["conditions"] if item.get("type") == "Ready"),
        None,
    )
    if ready != "True":
        issues.append(f"Ready={ready or 'MISSING'}")
    for condition in evidence["conditions"]:
        condition_type = condition.get("type")
        if condition_type in NODE_BLOCKING_CONDITIONS and condition.get("status") != "False":
            issues.append(f"{condition_type}={condition.get('status') or 'MISSING'}")
    for taint in evidence["taints"]:
        key = taint.get("key")
        effect = taint.get("effect")
        if key in STANDARD_HEALTH_TAINT_KEYS and effect in {"NoSchedule", "NoExecute"}:
            issues.append(f"taint {key}:{effect}")
    return issues


def _node_ready(payload: dict[str, Any]) -> bool:
    return not _node_health_issues(payload)


def _int_resource(payload: dict[str, Any], resource_name: str) -> int | None:
    value = payload.get("status", {}).get("allocatable", {}).get(resource_name)
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _truncated_command_evidence(result: Any) -> list[dict[str, Any]]:
    truncated: list[dict[str, Any]] = []
    for command in getattr(result, "commands", []) or []:
        if not (command.get("stdout_truncated") or command.get("stderr_truncated")):
            continue
        truncated.append(
            {
                "name": command.get("name"),
                "stdout_total_bytes": command.get("stdout_total_bytes"),
                "stderr_total_bytes": command.get("stderr_total_bytes"),
                "stdout_truncated": bool(command.get("stdout_truncated")),
                "stderr_truncated": bool(command.get("stderr_truncated")),
            }
        )
    return truncated


def _enforce_truncated_output_status(result: Any) -> None:
    truncated = _truncated_command_evidence(result)
    if not truncated:
        return
    reason_codes = {item.reason_code for item in result.findings}
    if "KUBECTL_OUTPUT_TRUNCATED" not in reason_codes:
        result.findings.append(
            Finding(
                "UNKNOWN",
                "KUBECTL_OUTPUT_TRUNCATED",
                "one or more critical kubectl probe outputs exceeded the bounded capture",
            )
        )
    # Deterministic hardware/configuration failures stay BLOCKED. Otherwise
    # truncated evidence can never produce READY.
    if result.status != "BLOCKED":
        result.status = "INCOMPLETE"


def _node_summary(result: Any) -> dict[str, Any]:
    max_vram = max(
        (item.memory_used_percent for item in result.devices if item.memory_used_percent is not None),
        default=None,
    )
    max_util = max(
        (item.hcu_util_percent for item in result.devices if item.hcu_util_percent is not None),
        default=None,
    )
    models = sorted({item.model for item in result.devices if item.model})
    architectures = sorted({item.architecture for item in result.devices if item.architecture})
    total_mib = sorted(
        {round(item.hy_smi_total_mib, 1) for item in result.devices if item.hy_smi_total_mib is not None}
    )
    return {
        "status": result.status,
        "device_count": result.device_count,
        "expected_device_count": result.expected_device_count,
        "models": models,
        "architectures": architectures,
        "vram_total_mib": total_mib,
        "max_vram_used_percent": round(max_vram, 2) if max_vram is not None else None,
        "max_hcu_util_percent": round(max_util, 2) if max_util is not None else None,
        "reason_codes": sorted({item.reason_code for item in result.findings}),
        "environment": result.environment.get("summary", {}) if result.environment else {},
        "image": result.target.get("image"),
        "image_id": result.target.get("image_id"),
        "output_capture": {
            "per_stream_limit_bytes": KUBECTL_OUTPUT_CAPTURE_LIMIT_BYTES,
            "strategy": "bounded-head-tail",
            "truncated": bool(_truncated_command_evidence(result)),
            "truncated_commands": _truncated_command_evidence(result),
        },
    }


def _group_nodes(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for record in records:
        summary = record.get("summary", {})
        signature_payload = {
            "status": record.get("status"),
            "device_count": summary.get("device_count"),
            "expected_device_count": summary.get("expected_device_count"),
            "models": summary.get("models", []),
            "architectures": summary.get("architectures", []),
            "vram_total_mib": summary.get("vram_total_mib", []),
            "reason_codes": summary.get("reason_codes", []),
            "environment": summary.get("environment", {}),
        }
        signature = json.dumps(signature_payload, ensure_ascii=False, sort_keys=True)
        group = groups.setdefault(signature, {**signature_payload, "nodes": []})
        group["nodes"].append(record["node"])
    return sorted(groups.values(), key=lambda item: (item["status"], item["nodes"]))


def _group_environments(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for record in records:
        environment = record.get("summary", {}).get("environment", {})
        signature = json.dumps(environment, ensure_ascii=False, sort_keys=True)
        group = groups.setdefault(
            signature,
            {"environment": environment, "nodes": [], "image_ids": []},
        )
        group["nodes"].append(record["node"])
        image_id = record.get("summary", {}).get("image_id")
        if image_id and image_id not in group["image_ids"]:
            group["image_ids"].append(image_id)
    return sorted(groups.values(), key=lambda item: item["nodes"])


_HARDWARE_ENVIRONMENT_FIELDS = (
    "kernel",
    "cpu_logical_count",
    "cpu_models",
    "mem_total",
    "dtk_version",
    "driver_version",
    "hy_smi_version",
    "smi_library_version",
    "hipcc_version",
    "rccl_paths",
    "ucx_version",
    "mpi_version",
    "physical_nic_count",
    "network_scope",
    "nic_drivers",
    "nic_hardware_profile",
    "nic_link_profile",
    "nic_link_summary",
    "rdma_hardware_profile",
    "rdma_device_count",
    "rdma_active_device_count",
    "rdma_active_port_count",
    "rdma_current_protocol",
    "rdma_protocol_status",
    "rdma_hardware_protocol_capability",
    "rdma_fabric_profile",
    "rdma_protocol_profile",
    "ib_endpoint",
    "ib_counter_health",
    "rdma_userspace",
    "roce_endpoint",
    "roce_configuration_health",
    "rdma_rates",
    "vbios_versions",
    "hsw_firmware_versions",
)

_HARDWARE_DISPLAY_FIELDS = _HARDWARE_ENVIRONMENT_FIELDS + (
    "nic_inventory",
    "rdma_nic_inventory",
    "pci_name_source",
)

_SOFTWARE_ENVIRONMENT_FIELDS = (
    "container_os",
    "python_version",
    "python_packages",
    "core_python_packages",
    "torch_version",
    "torch_hip_version",
    "torch_device_count",
    "torch_hcu_available",
    "torch_distributed_available",
    "torch_nccl_backend_available",
    "torch_nccl_version",
    "runtime_env",
)


def _project_environment(environment: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    projected = {field: environment.get(field) for field in fields}
    counters = projected.get("ib_counter_health")
    if isinstance(counters, dict):
        sampling = counters.get("sampling") or {}
        projected["ib_counter_health"] = {
            key: counters.get(key)
            for key in ("status", "observed_status", "required", "ports", "status_counts", "reason_codes")
        }
        projected["ib_counter_health"]["sampling"] = {
            key: sampling.get(key)
            for key in ("status", "interval_seconds", "reason_code")
            if key in sampling
        }
    userspace = projected.get("rdma_userspace")
    if isinstance(userspace, dict):
        libraries = userspace.get("libraries") or {}
        projected["rdma_userspace"] = {
            key: userspace.get(key)
            for key in ("status", "check_status", "reason_code", "sysfs_devices", "enumerated_devices", "missing_enumerated_devices")
        }
        projected["rdma_userspace"]["libraries"] = {
            key: libraries.get(key)
            for key in ("libibverbs", "providers", "rccl_net_plugins")
        }
    roce = projected.get("roce_configuration_health")
    if isinstance(roce, dict):
        projected["roce_configuration_health"] = {
            "status": roce.get("status"),
            "policy_applied": roce.get("policy_applied"),
            "normalized_policy": roce.get("normalized_policy"),
            "summary": roce.get("summary"),
        }
    return projected


def _group_projected_environments(
    records: list[dict[str, Any]],
    *,
    fields: tuple[str, ...],
    include_image: bool,
    display_fields: tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    """Fold only on values displayed in the corresponding report section.

    Hardware groups must not split because a running container has different
    Python packages or environment variables.  Software groups intentionally
    include the image digest and runtime/package fields.
    """
    groups: dict[str, dict[str, Any]] = {}
    for record in records:
        summary = record.get("summary", {})
        source_environment = summary.get("environment", {})
        signature_environment = _project_environment(source_environment, fields)
        display_environment = _project_environment(
            source_environment,
            display_fields or fields,
        )
        signature_payload: dict[str, Any] = {"environment": signature_environment}
        if include_image:
            signature_payload["image_id"] = summary.get("image_id")
        signature = json.dumps(signature_payload, ensure_ascii=False, sort_keys=True)
        group = groups.setdefault(
            signature,
            {
                "environment": display_environment,
                **({"image_id": summary.get("image_id")} if include_image else {}),
                "nodes": [],
            },
        )
        group["nodes"].append(record["node"])
    for group in groups.values():
        group["nodes"].sort(key=_natural_node_key)
    return sorted(
        groups.values(),
        key=lambda item: _natural_node_key(item["nodes"][0]),
    )


def _group_hardware_environments(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _group_projected_environments(
        records,
        fields=_HARDWARE_ENVIRONMENT_FIELDS,
        include_image=False,
        display_fields=_HARDWARE_DISPLAY_FIELDS,
    )


def _group_software_environments(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _group_projected_environments(
        records,
        fields=_SOFTWARE_ENVIRONMENT_FIELDS,
        include_image=True,
    )


def _format_percent(value: Any) -> str:
    try:
        return f"{float(value):.1f}%"
    except (TypeError, ValueError):
        return "-"


def _natural_node_key(value: str) -> list[tuple[int, Any]]:
    return [
        (1, int(part)) if part.isdigit() else (0, part.lower())
        for part in re.split(r"(\d+)", value)
    ]


def _group_node_results(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Fold using exactly the values displayed in the result table."""
    groups: dict[str, dict[str, Any]] = {}
    for record in records:
        summary = record.get("summary", {})
        row = {
            "probe_source": record.get("probe_source", "-"),
            "status": record.get("status", "INCOMPLETE"),
            "device_count": (
                str(summary.get("device_count"))
                if summary.get("device_count") is not None
                else "-"
            ),
            "max_vram_used": _format_percent(summary.get("max_vram_used_percent")),
            "max_hcu_util": _format_percent(summary.get("max_hcu_util_percent")),
            "reason_codes": ", ".join(
                sorted(set(summary.get("reason_codes", [])))
            )
            or "-",
            "cleanup_status": record.get("cleanup_status", "NOT_APPLICABLE"),
        }
        signature = json.dumps(row, ensure_ascii=False, sort_keys=True)
        group = groups.setdefault(signature, {**row, "nodes": []})
        group["nodes"].append(record["node"])
    for group in groups.values():
        group["nodes"].sort(key=_natural_node_key)
    return sorted(
        groups.values(),
        key=lambda item: (item["status"], _natural_node_key(item["nodes"][0])),
    )


def _group_node_scheduling_evidence(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for record in records:
        evidence = record.get("node_k8s_evidence") or {
            "unschedulable": None,
            "conditions": [],
            "taints": [],
            "health_issues": ["node scheduling evidence unavailable"],
        }
        signature = json.dumps(evidence, ensure_ascii=False, sort_keys=True)
        group = groups.setdefault(signature, {"evidence": evidence, "nodes": []})
        group["nodes"].append(record["node"])
    for group in groups.values():
        group["nodes"].sort(key=_natural_node_key)
    return sorted(groups.values(), key=lambda item: _natural_node_key(item["nodes"][0]))


def _environment_field_values(
    records: list[dict[str, Any]],
    field: str,
) -> tuple[dict[str, list[str]], list[str]]:
    values: dict[str, list[str]] = {}
    missing_nodes: list[str] = []
    for record in records:
        environment = record.get("summary", {}).get("environment", {})
        if field not in environment or environment.get(field) is None:
            missing_nodes.append(record["node"])
            continue
        value = json.dumps(environment[field], ensure_ascii=False, sort_keys=True)
        values.setdefault(value, []).append(record["node"])
    for nodes in values.values():
        nodes.sort(key=_natural_node_key)
    return values, sorted(missing_nodes, key=_natural_node_key)


def _rdma_protocol_consistency_findings(
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    protocol_groups: dict[str, list[str]] = {}
    protocol_evidence_missing_nodes: list[str] = []
    mixed_protocol_nodes: list[str] = []
    for record in records:
        protocol = (
            record.get("summary", {})
            .get("environment", {})
            .get("rdma_current_protocol")
        )
        if protocol in {"NATIVE_INFINIBAND", "ROCE"}:
            protocol_groups.setdefault(protocol, []).append(record["node"])
        elif protocol == "MIXED":
            mixed_protocol_nodes.append(record["node"])
        else:
            protocol_evidence_missing_nodes.append(record["node"])
    if mixed_protocol_nodes:
        findings.append(
            {
                "severity": "FAIL",
                "reason_code": "RDMA_PROTOCOL_MIXED_ON_NODE",
                "field": "rdma_current_protocol",
                "values": {"MIXED": sorted(mixed_protocol_nodes, key=_natural_node_key)},
                "message": "native InfiniBand and RoCE ports are mixed on a node",
            }
        )
    if protocol_evidence_missing_nodes:
        findings.append(
            {
                "severity": "UNKNOWN",
                "reason_code": "RDMA_PROTOCOL_EVIDENCE_MISSING",
                "field": "rdma_current_protocol",
                "values": {},
                "missing_nodes": sorted(
                    protocol_evidence_missing_nodes, key=_natural_node_key
                ),
                "message": "current RDMA protocol evidence is unavailable on some nodes",
            }
        )
    if len(protocol_groups) > 1:
        findings.append(
            {
                "severity": "FAIL",
                "reason_code": "RDMA_PROTOCOL_CLUSTER_MIXED",
                "field": "rdma_current_protocol",
                "values": {
                    protocol: sorted(nodes, key=_natural_node_key)
                    for protocol, nodes in sorted(protocol_groups.items())
                },
                "message": "current RDMA protocol differs across target nodes",
            }
        )
    if len(protocol_groups) == 1:
        fabric_values: dict[str, dict[str, Any]] = {}
        missing_nodes: list[str] = []
        for record in records:
            environment = record.get("summary", {}).get("environment", {})
            if environment.get("rdma_current_protocol") not in protocol_groups:
                continue
            profile = environment.get("rdma_fabric_profile")
            if profile is None:
                missing_nodes.append(record["node"])
                continue
            key = json.dumps(profile, ensure_ascii=False, sort_keys=True)
            fabric_values.setdefault(
                key, {"value": profile, "nodes": []}
            )["nodes"].append(record["node"])
        if missing_nodes:
            findings.append(
                {
                    "severity": "UNKNOWN",
                    "reason_code": "RDMA_FABRIC_PROFILE_EVIDENCE_MISSING",
                    "field": "rdma_fabric_profile",
                    "values": {},
                    "missing_nodes": sorted(missing_nodes, key=_natural_node_key),
                    "message": "RDMA fabric profile is unavailable on some nodes",
                }
            )
        if len(fabric_values) > 1:
            fabric_value_groups = [
                {
                    **item,
                    "nodes": sorted(item["nodes"], key=_natural_node_key),
                }
                for item in fabric_values.values()
            ]
            findings.append(
                {
                    "severity": "FAIL",
                    "reason_code": "RDMA_FABRIC_PROFILE_INCONSISTENT",
                    "field": "rdma_fabric_profile",
                    # ``value_groups`` is the canonical grouped-value schema.
                    # Keep ``values`` for report/backward compatibility.
                    "value_groups": fabric_value_groups,
                    "values": fabric_value_groups,
                    "message": "RDMA fabric subnet/P_Key/MTU/rate profile differs across nodes",
                }
            )
    return findings


def _normalise_consistency_nodes(value: Any) -> list[str]:
    """Return a stable node list from every supported finding schema."""

    if isinstance(value, str):
        candidates = [value]
    elif isinstance(value, (list, tuple, set)):
        candidates = value
    else:
        return []
    return sorted(
        {node.strip() for node in candidates if isinstance(node, str) and node.strip()},
        key=_natural_node_key,
    )


def _consistency_finding_node_groups(finding: dict[str, Any]) -> list[list[str]]:
    """Normalise consistency finding values to ``list[list[node]]``.

    Older scalar/stack findings encode ``values`` as ``{value: [nodes]}``.
    RDMA fabric findings need to retain their structured profile and therefore
    encode ``values`` (and the canonical ``value_groups`` field) as records
    shaped like ``{"value": profile, "nodes": [...]}``.  Consumption must be
    schema-aware; malformed optional evidence is ignored instead of aborting
    the entire cluster report.
    """

    raw_groups = finding.get("value_groups")
    if not isinstance(raw_groups, (dict, list, tuple)):
        raw_groups = finding.get("values")
    if isinstance(raw_groups, dict):
        candidates = list(raw_groups.values())
    elif isinstance(raw_groups, (list, tuple)):
        candidates = list(raw_groups)
    else:
        return []

    groups: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()
    for candidate in candidates:
        raw_nodes = candidate.get("nodes") if isinstance(candidate, dict) else candidate
        nodes = _normalise_consistency_nodes(raw_nodes)
        key = tuple(nodes)
        if not nodes or key in seen:
            continue
        seen.add(key)
        groups.append(nodes)
    return groups


def _build_scale_readiness(
    records: list[dict[str, Any]],
    *,
    cluster_status: str,
    target_devices: int,
    devices_per_node: int,
    consistency_findings: list[dict[str, Any]],
) -> dict[str, Any]:
    if target_devices < 1:
        raise ValueError("--target-scale-devices must be at least 1")
    checked = [
        record
        for record in records
        if record.get("summary", {}).get("device_count") is not None
    ]
    ready = [record for record in checked if record.get("status") == "READY"]
    checked_devices = sum(int(record["summary"]["device_count"]) for record in checked)
    ready_devices = sum(int(record["summary"]["device_count"]) for record in ready)
    blocking_nodes = sorted(
        [record["node"] for record in records if record.get("status") == "BLOCKED"],
        key=_natural_node_key,
    )
    incomplete_node_names = {
        record["node"] for record in records if record.get("status") == "INCOMPLETE"
    }
    for finding in consistency_findings:
        if finding.get("severity") == "UNKNOWN":
            incomplete_node_names.update(
                _normalise_consistency_nodes(finding.get("missing_nodes"))
            )
    incomplete_nodes = sorted(incomplete_node_names, key=_natural_node_key)
    consistency_deviation_nodes: set[str] = set()
    consistency_ambiguous_nodes: set[str] = set()
    consistency_reference_ambiguous = False
    consistency_ambiguous_reason_codes: set[str] = set()
    for finding in consistency_findings:
        if finding.get("severity") != "FAIL":
            continue
        value_groups = _consistency_finding_node_groups(finding)
        if len(value_groups) < 2:
            continue
        maximum_size = max(len(nodes) for nodes in value_groups)
        largest_groups = [
            nodes for nodes in value_groups if len(nodes) == maximum_size
        ]
        if len(largest_groups) == 1:
            reference = largest_groups[0]
            for nodes in value_groups:
                if nodes is not reference:
                    consistency_deviation_nodes.update(nodes)
            continue

        # Equal-size leading groups do not provide a defensible reference.
        # Mark every related node as affected and explicitly preserve the
        # ambiguity instead of arbitrarily blaming one side of an A/B split.
        consistency_reference_ambiguous = True
        affected_nodes = {node for nodes in value_groups for node in nodes}
        consistency_deviation_nodes.update(affected_nodes)
        consistency_ambiguous_nodes.update(affected_nodes)
        consistency_ambiguous_reason_codes.add(
            str(finding.get("reason_code") or "UNKNOWN")
        )
    if cluster_status == "BLOCKED":
        readiness_status = "NOT_READY"
        conclusion = "本次样本已发现阻断项，不具备扩大到万卡训练的放行条件。"
    elif cluster_status == "INCOMPLETE":
        readiness_status = "NOT_VERIFIED"
        conclusion = "本次样本证据不完整，不能判断万卡规模可用性。"
    elif checked_devices < target_devices:
        readiness_status = "SAMPLE_READY_FULL_SCALE_UNVERIFIED"
        conclusion = "本次样本通过，仅具备扩大验证条件；目标规模尚未实测，不能据此放行。"
    else:
        readiness_status = "FULL_SCALE_STATIC_PREFLIGHT_PASSED_RUNTIME_UNVERIFIED"
        conclusion = "目标规模已完成静态启动前检查；训练运行、collective和全网数据面仍未实测。"
    return {
        "status": readiness_status,
        "target_devices": target_devices,
        "estimated_target_nodes": math.ceil(target_devices / devices_per_node),
        "devices_per_node": devices_per_node,
        "checked_nodes": len(checked),
        "checked_devices": checked_devices,
        "ready_nodes": len(ready),
        "ready_devices": ready_devices,
        "coverage_percent": round(min(100.0, checked_devices * 100.0 / target_devices), 3),
        "blocking_nodes": blocking_nodes,
        "incomplete_nodes": incomplete_nodes,
        "consistency_reason_codes": sorted(
            {
                str(finding.get("reason_code") or "UNKNOWN")
                for finding in consistency_findings
            }
        ),
        "consistency_deviation_nodes": sorted(
            consistency_deviation_nodes,
            key=_natural_node_key,
        ),
        "consistency_reference_ambiguous": consistency_reference_ambiguous,
        "consistency_ambiguous_nodes": sorted(
            consistency_ambiguous_nodes, key=_natural_node_key
        ),
        "consistency_ambiguous_reason_codes": sorted(
            consistency_ambiguous_reason_codes
        ),
        "conclusion": conclusion,
        "static_coverage_reached": checked_devices >= target_devices,
        "is_full_scale_test": False,
    }


def _nic_profile_text(item: dict[str, Any]) -> str:
    details = [
        f"{item.get('count', 0)}×{item.get('vendor', 'UNKNOWN')} {item.get('model', 'UNKNOWN')}",
        f"类型={item.get('class', 'UNKNOWN')}",
        f"PCI={item.get('pci_id', '????:????')}",
    ]
    if item.get("subsystem_pci_id"):
        details.append(f"Subsystem={item['subsystem_pci_id']}")
    driver = item.get("driver") or "UNKNOWN"
    if item.get("driver_version"):
        driver = f"{driver}/{item['driver_version']}"
    details.append(f"驱动={driver}")
    if item.get("firmware_version"):
        details.append(f"固件={item['firmware_version']}")
    details.append(f"本地链路={item.get('local_link', 'UNKNOWN')}")
    if item.get("speed_mbps") not in {None, "", "-1"}:
        details.append(f"速率={item['speed_mbps']}Mbps")
    if item.get("mtu"):
        details.append(f"MTU={item['mtu']}")
    return "；".join(details)


def _display(value: Any, fallback: str = "-") -> Any:
    """Avoid rendering Python's ``None`` as if it were collected evidence."""
    return fallback if value is None or value == "" else value


def _build_check_coverage(
    records: list[dict[str, Any]],
    *,
    include_environment: bool,
) -> list[dict[str, str]]:
    total = len(records)
    environment_count = sum(
        bool(record.get("summary", {}).get("environment")) for record in records
    )

    def collected_status(count: int) -> str:
        if total and count == total:
            return "CHECKED"
        if count:
            return "PARTIAL"
        return "NOT_EXECUTED"

    environment_status = (
        collected_status(environment_count) if include_environment else "DISABLED_BY_PROFILE"
    )
    return [
        {
            "item": "HCU枚举、显存与利用率",
            "status": collected_status(
                sum(
                    record.get("summary", {}).get("device_count") is not None
                    for record in records
                )
            ),
            "method": "rocminfo + hy-smi 多次只读采样",
        },
        {
            "item": "宿主硬件、驱动、DTK、NIC/RDMA",
            "status": environment_status,
            "method": "目标节点探针内的只读系统、sysfs 与工具查询",
        },
        {
            "item": "Torch导入、HCU可见性、设备数与RCCL后端",
            "status": environment_status,
            "method": "只导入Torch并查询运行时属性，不创建Tensor",
        },
        {
            "item": "交换机端口、PFC/ECN、队列与光模块",
            "status": "REQUIRES_EXTERNAL_ACCESS",
            "method": "本轮无交换机只读SNMP/gNMI/IB Fabric管理权限，未执行",
        },
        {
            "item": "Tensor、算子、自定义扩展编译",
            "status": "NOT_EXECUTED_BY_DESIGN",
            "method": "训练前无扰动检查不创建Tensor、不编译、不加载客户算子",
        },
        {
            "item": "RCCL collective与RDMA主动流量",
            "status": "NOT_EXECUTED_BY_DESIGN",
            "method": "本工具只做启动前环境检查；主动通信验证属于定向压测工具",
        },
    ]


def _write_cluster_markdown(report: dict[str, Any], path: Path) -> None:
    scale = report.get("scale_readiness", {})
    execution = report.get("execution", {})
    api_limit = execution.get("api_rate_limit", {})
    cleanup = execution.get("cleanup", {})
    lines = [
        "# K8s HCU 训练启动前检查",
        "",
        f"- 工具版本：`{report.get('tool_version', '-')}`",
        f"- 总结论：**{report['status']}**",
        f"- 检查节点：{len(report['nodes'])}",
        (
            f"- 采集方式：有界并行；请求并发={execution.get('requested_concurrency', '-')}，"
            f"实际并发={execution.get('effective_concurrency', '-')}"
        ),
        (
            f"- API限流：QPS={api_limit.get('qps', '-')}，burst={api_limit.get('burst', '-')}，"
            f"本次受控请求={api_limit.get('requests_observed', '-')}"
        ),
        (
            f"- 临时Pod清理：完成={cleanup.get('completed', 0)}/"
            f"{cleanup.get('temporary_pods', 0)}，需处理={cleanup.get('requires_attention', 0)}"
        ),
        f"- 万卡规模判断：**{scale.get('status', 'NOT_VERIFIED')}**（静态检查，非万卡训练实测）",
        "",
        "## 节点检查结果（相同结果折叠）",
        "",
        "| 节点 | 探针来源 | 结果 | 可见HCU | 最大显存占用 | 最大HCU利用率 | 原因码 | 清理 |",
        "|---|---|---:|---:|---:|---:|---|---|",
    ]
    for item in report.get("node_result_groups", []):
        lines.append(
            f"| {', '.join(item['nodes'])} | {item['probe_source']} | {item['status']} | "
            f"{item['device_count']} | {item['max_vram_used']} | {item['max_hcu_util']} | "
            f"{item['reason_codes']} | {item['cleanup_status']} |"
        )
    lines.extend(["", "## Kubernetes 节点 Conditions / Taints（相同证据折叠）", ""])
    for group in report.get("node_scheduling_groups", []):
        lines.extend(
            [
                f"### {', '.join(group['nodes'])}",
                "",
                "```json",
                json.dumps(group.get("evidence", {}), ensure_ascii=False, indent=2),
                "```",
                "",
            ]
        )
    truncated_nodes = [
        record
        for record in report.get("nodes", [])
        if record.get("summary", {}).get("output_capture", {}).get("truncated")
    ]
    if truncated_nodes:
        lines.extend(["## Kubectl 输出截断证据", ""])
        for record in truncated_nodes:
            capture = record.get("summary", {}).get("output_capture", {})
            lines.extend(
                [
                    f"### {record['node']}",
                    "",
                    f"- Per-stream limit: {capture.get('per_stream_limit_bytes')} bytes; strategy: {capture.get('strategy')}",
                    "```json",
                    json.dumps(capture.get("truncated_commands", []), ensure_ascii=False, indent=2),
                    "```",
                    "",
                ]
            )
    lines.extend(["", "## 硬件、驱动与通信环境（相同结果折叠）", ""])
    for group in report.get("hardware_environment_groups", []):
        env = group.get("environment", {})
        link = env.get("nic_link_summary") or {}
        ib_counters = env.get("ib_counter_health") or {}
        rdma_userspace = env.get("rdma_userspace") or {}
        roce_health = env.get("roce_configuration_health") or {}
        lines.extend(
            [
                f"### {', '.join(group['nodes'])}",
                "",
                f"- CPU：{', '.join(env.get('cpu_models') or []) or '-'}；逻辑CPU={_display(env.get('cpu_logical_count'))}；内存={_display(env.get('mem_total'))}",
                f"- 内核：{_display(env.get('kernel'))}",
                f"- DTK/驱动：{_display(env.get('dtk_version'))} / {_display(env.get('driver_version'))}",
                f"- hy-smi/SMI库：{_display(env.get('hy_smi_version'))} / {_display(env.get('smi_library_version'))}",
                f"- hipcc：{_display(env.get('hipcc_version'))}",
                f"- RCCL：{', '.join(env.get('rccl_paths') or []) or '-'}",
                f"- UCX/MPI：{_display(env.get('ucx_version'))} / {_display(env.get('mpi_version'))}",
                f"- PCI物理网口：{_display(env.get('physical_nic_count'))}；采集范围={_display(env.get('network_scope'))}；本地链路UP={_display(link.get('UP'))}，DOWN={_display(link.get('DOWN'))}，UNKNOWN={_display(link.get('UNKNOWN'))}",
                "- 网卡厂家与型号：",
            ]
        )
        nic_inventory = env.get("nic_inventory") or []
        if nic_inventory:
            lines.extend(f"  - {_nic_profile_text(item)}" for item in nic_inventory)
        else:
            lines.append("  - 未识别；仅原始PCI证据可用")
        lines.extend(
            [
                f"- RDMA本地状态：设备={_display(env.get('rdma_device_count'))}，具备Active/LinkUp端口的设备={_display(env.get('rdma_active_device_count'))}，Active/LinkUp端口={_display(env.get('rdma_active_port_count'))}，速率={','.join(env.get('rdma_rates') or []) or '-'}",
                f"- RDMA当前端口模式：{_display(env.get('rdma_current_protocol'))}；协议检查={_display(env.get('rdma_protocol_status'))}；硬件支持模式={_display(env.get('rdma_hardware_protocol_capability'))}",
                f"- RDMA userspace Verbs: {_display(rdma_userspace.get('check_status') or rdma_userspace.get('status'))}; sysfs={_display(rdma_userspace.get('sysfs_devices'))}; verbs={_display(rdma_userspace.get('enumerated_devices'))}; reason={_display(rdma_userspace.get('reason_code'))}",
                f"- IB counter health: {_display(ib_counters.get('status'))}; ports={_display(ib_counters.get('ports'))}; counts={_display(ib_counters.get('status_counts'))}; reasons={_display(ib_counters.get('reason_codes'))}",
                (
                    f"- IB端点：{_display((env.get('ib_endpoint') or {}).get('status'))}，"
                    f"端口={_display((env.get('ib_endpoint') or {}).get('ports'))}，"
                    f"Active+LinkUp={_display((env.get('ib_endpoint') or {}).get('active_linkup_ports'))}，"
                    f"有效LID/SM-LID/GID/P_Key="
                    f"{_display((env.get('ib_endpoint') or {}).get('valid_lid_ports'))}/"
                    f"{_display((env.get('ib_endpoint') or {}).get('valid_sm_lid_ports'))}/"
                    f"{_display((env.get('ib_endpoint') or {}).get('valid_gid_ports'))}/"
                    f"{_display((env.get('ib_endpoint') or {}).get('valid_pkey_ports'))}，"
                    f"MTU={_display((env.get('ib_endpoint') or {}).get('active_mtus'))}"
                ),
                (
                    f"- RoCE端点：{_display((env.get('roce_endpoint') or {}).get('status'))}，"
                    f"版本={_display((env.get('roce_endpoint') or {}).get('versions'))}，"
                    f"netdev={_display((env.get('roce_endpoint') or {}).get('netdevs'))}，"
                    f"MTU={_display((env.get('roce_endpoint') or {}).get('mtus'))}"
                ),
                f"- RoCE configuration chain: {_display(roce_health.get('status'))}; policy_applied={_display(roce_health.get('policy_applied'))}; summary={_display(roce_health.get('summary'))}",
                f"- RoCE主机QoS/DCB：{_display((env.get('roce_endpoint') or {}).get('dcb_status'))}；交换机侧QoS：NOT_VERIFIED",
                "- 训练实际RDMA/TCP数据路径：NOT_VERIFIED_BY_PREFLIGHT",
                f"- VBIOS/HSW固件：{','.join(env.get('vbios_versions') or []) or '-'} / {','.join(env.get('hsw_firmware_versions') or []) or '-'}",
                "",
            ]
        )
    lines.extend(
        [
            "",
            "## 万卡规模适用性评估（静态检查，非万卡训练实测）",
            "",
            f"- 状态：**{scale.get('status', 'NOT_VERIFIED')}**",
            f"- 目标规模：{scale.get('target_devices', '-')} HCU；按每节点 {scale.get('devices_per_node', '-')} 卡估算约 {scale.get('estimated_target_nodes', '-')} 个节点",
            f"- 本次实测：{scale.get('checked_nodes', 0)} 个节点、{scale.get('checked_devices', 0)} 张卡，覆盖目标规模 {scale.get('coverage_percent', 0)}%",
            f"- 节点级静态检查通过：{scale.get('ready_nodes', 0)} 个节点、{scale.get('ready_devices', 0)} 张卡",
            f"- 节点级阻断：{', '.join(scale.get('blocking_nodes', [])) or '无'}",
            f"- 跨节点一致性偏差：{', '.join(scale.get('consistency_deviation_nodes', [])) or '无'}；原因码={', '.join(scale.get('consistency_reason_codes', [])) or '无'}",
            (
                f"- 一致性参考歧义：是；相关节点={', '.join(scale.get('consistency_ambiguous_nodes', [])) or '未知'}"
                if scale.get("consistency_reference_ambiguous")
                else "- 一致性参考歧义：否"
            ),
            f"- 证据不完整节点：{', '.join(scale.get('incomplete_nodes', [])) or '无'}",
            f"- 结论：{scale.get('conclusion', '证据不足，不能判断规模化可用性。')}",
            "",
            "## 多节点一致性判定",
            "",
        ]
    )
    include_environment = bool(
        (report.get("policy") or {}).get("include_environment", True)
    )
    strict_consistency_enabled = bool(
        (report.get("policy") or {}).get("strict_stack_consistency")
    )
    if not include_environment:
        lines.append("- NOT_EXECUTED：未采集环境，RDMA协议/Fabric一致性未执行。")
    elif report.get("consistency_findings"):
        for finding in report["consistency_findings"]:
            missing_text = (
                f"；缺少证据节点={finding['missing_nodes']}"
                if finding.get("missing_nodes")
                else ""
            )
            reason_code = str(finding.get("reason_code") or "UNKNOWN")
            field = str(finding.get("field") or "UNKNOWN")
            values = finding.get("values")
            if values is None:
                values = finding.get("value_groups", {})
            lines.append(
                f"- **{finding.get('severity', 'UNKNOWN')}** `{reason_code}`："
                f"{field}；{values}{missing_text}"
            )
    elif not strict_consistency_enabled:
        lines.append("- RDMA协议/Fabric强制一致性未发现偏差；未启用其余软件栈严格一致性比较。")
    else:
        lines.append("- PASS：当前 profile 纳入比较的组件在所有节点一致。")
    lines.extend(["", "### 依赖分组（相同结果折叠）", ""])
    for group in report.get("software_environment_groups", []):
        env = group.get("environment", {})
        packages = env.get("python_packages") or {}
        package_text = ", ".join(f"{name}={version}" for name, version in sorted(packages.items())) or "-"
        lines.extend(
            [
                f"#### {', '.join(group['nodes'])}",
                "",
                f"- 容器OS：{_display(env.get('container_os'))}",
                f"- 镜像Digest：{group.get('image_id') or '-'}",
                f"- 关键运行环境：{env.get('runtime_env') or {}}",
                f"- Python：{_display(env.get('python_version'))}",
                f"- Torch：{_display(env.get('torch_version'))}；HIP：{_display(env.get('torch_hip_version'))}",
                f"- HCU运行时：available={_display(env.get('torch_hcu_available'))}；device_count={_display(env.get('torch_device_count'))}；distributed={_display(env.get('torch_distributed_available'))}；RCCL后端={_display(env.get('torch_nccl_backend_available'))}；RCCL版本={_display(env.get('torch_nccl_version'))}",
                f"- 相关组件：{package_text}",
                "",
            ]
        )
    atomic_write_text_exclusive(path, "\n".join(lines))


def run_k8s_cluster_preflight(
    *,
    nodes: list[str],
    namespace: str,
    image: str,
    image_pull_policy: str,
    probe_container: str,
    reuse_pods: dict[str, tuple[str, str, str]],
    context: str | None,
    kubeconfig: str | None,
    device_resource_name: str,
    expected_devices: int,
    max_vram_used_percent: float,
    max_hcu_util_percent: float,
    samples: int,
    busy_sample_quorum: int,
    sample_interval_seconds: float,
    command_timeout: float,
    pod_ready_timeout: int,
    output_dir: Path,
    include_environment: bool = True,
    require_compiler: bool = False,
    require_rdma: bool = False,
    minimum_rdma_devices: int = 0,
    expected_rdma_protocol: str = "auto",
    require_rccl: bool = False,
    require_ucx: bool = False,
    strict_stack_consistency: bool = False,
    bootstrap_wheel: Path | None = None,
    bootstrap_wheel_sha256: str | None = None,
    probe_memory_request: str = "1Gi",
    probe_memory_limit: str = "8Gi",
    probe_env: dict[str, str] | None = None,
    target_scale_devices: int = 10000,
    rdma_policy: dict[str, Any] | None = None,
    rdma_counter_interval_seconds: int = 5,
    concurrency: int = DEFAULT_K8S_CLUSTER_CONCURRENCY,
    api_qps: float = DEFAULT_KUBECTL_API_QPS,
    api_burst: int = DEFAULT_KUBECTL_API_BURST,
) -> tuple[dict[str, Any], Path, Path]:
    validate_environment_profile(
        include_environment=include_environment,
        require_compiler=require_compiler,
        require_rdma=require_rdma,
        minimum_rdma_devices=minimum_rdma_devices,
        expected_rdma_protocol=expected_rdma_protocol,
        require_rccl=require_rccl,
        require_ucx=require_ucx,
        rdma_policy=rdma_policy,
    )
    if not nodes:
        raise ValueError("at least one target node is required")
    if len(set(nodes)) != len(nodes):
        raise ValueError("duplicate nodes are not allowed")
    if expected_rdma_protocol not in {"auto", "ib", "roce"}:
        raise ValueError("expected_rdma_protocol must be auto, ib, or roce")
    if target_scale_devices < 1:
        raise ValueError("--target-scale-devices must be at least 1")
    if rdma_counter_interval_seconds != 0 and not 1 <= rdma_counter_interval_seconds <= 60:
        raise ValueError(
            "rdma_counter_interval_seconds must be 0 or between 1 and 60"
        )
    if (
        isinstance(concurrency, bool)
        or not isinstance(concurrency, int)
        or not 1 <= concurrency <= MAX_K8S_CLUSTER_CONCURRENCY
    ):
        raise ValueError(
            f"concurrency must be between 1 and {MAX_K8S_CLUSTER_CONCURRENCY}"
        )
    try:
        if isinstance(api_qps, bool):
            raise ValueError("api_qps must be a finite number")
        normalized_api_qps = float(api_qps)
    except (TypeError, ValueError) as exc:
        raise ValueError("api_qps must be a finite number") from exc
    if (
        not math.isfinite(normalized_api_qps)
        or not 0 < normalized_api_qps <= MAX_KUBECTL_API_QPS
    ):
        raise ValueError(
            f"api_qps must be greater than 0 and at most {MAX_KUBECTL_API_QPS:g}"
        )
    if (
        isinstance(api_burst, bool)
        or not isinstance(api_burst, int)
        or not 1 <= api_burst <= MAX_KUBECTL_API_BURST
    ):
        raise ValueError(
            f"api_burst must be between 1 and {MAX_KUBECTL_API_BURST}"
        )
    normalized_bootstrap_sha256: str | None = None
    if bootstrap_wheel is not None:
        if bootstrap_wheel.suffix.lower() != ".whl":
            raise ValueError("--bootstrap-wheel must point to a .whl file")
        if not bootstrap_wheel.is_file():
            raise ValueError(f"bootstrap wheel not found: {bootstrap_wheel}")
        if not bootstrap_wheel_sha256:
            raise ValueError("--bootstrap-wheel-sha256 is required with --bootstrap-wheel")
        normalized_bootstrap_sha256 = bootstrap_wheel_sha256.lower()
        if not re.fullmatch(r"[0-9a-f]{64}", normalized_bootstrap_sha256):
            raise ValueError("--bootstrap-wheel-sha256 must be 64 hexadecimal characters")
        digest = hashlib.sha256()
        with bootstrap_wheel.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != normalized_bootstrap_sha256:
            raise ValueError(
                f"bootstrap wheel SHA256 mismatch: actual={digest.hexdigest()}"
            )

    started_at = _utc_now()
    run_id = uuid.uuid4().hex[:12]
    effective_concurrency = min(concurrency, len(nodes))
    api_limiter = _TokenBucketRateLimiter(normalized_api_qps, api_burst)
    controller = KubectlController(
        context=context,
        kubeconfig=kubeconfig,
        timeout_seconds=max(command_timeout, 60),
        api_limiter=api_limiter,
    )
    claim_output_directory(output_dir)

    node_directories: dict[str, Path] = {}
    used_directory_names: set[str] = set()
    for node in nodes:
        directory_name = _safe_name(node)
        if directory_name in used_directory_names:
            digest = hashlib.sha256(node.encode("utf-8", "replace")).hexdigest()[:8]
            directory_name = f"{directory_name}-{digest}"
        used_directory_names.add(directory_name)
        node_dir = output_dir / directory_name
        node_dir.mkdir(parents=True, exist_ok=False)
        node_directories[node] = node_dir

    def incomplete_summary(reason_codes: list[str] | None = None) -> dict[str, Any]:
        return {
            "status": "INCOMPLETE",
            "device_count": None,
            "expected_device_count": expected_devices,
            "models": [],
            "architectures": [],
            "vram_total_mib": [],
            "max_vram_used_percent": None,
            "max_hcu_util_percent": None,
            "reason_codes": reason_codes or ["K8S_PROBE_FAILED"],
            "environment": {},
        }

    def probe_node(node: str) -> dict[str, Any]:
        record: dict[str, Any] = {
            "node": node,
            "status": "INCOMPLETE",
            "probe_source": "existing-pod" if node in reuse_pods else "temporary-pod",
            "cleanup_status": "NOT_APPLICABLE" if node in reuse_pods else "NOT_CREATED",
        }
        node_dir = node_directories[node]
        created_pod: str | None = None
        target_namespace = namespace
        try:
            node_payload = controller.get_node(node)
            scheduling_evidence = _node_scheduling_evidence(node_payload)
            health_issues = _node_health_issues(node_payload)
            scheduling_evidence["health_issues"] = health_issues
            record["node_k8s_evidence"] = scheduling_evidence
            eligibility_codes: list[str] = []
            eligibility_messages: list[str] = []
            if health_issues:
                eligibility_codes.append("K8S_NODE_CONDITION_UNHEALTHY")
                eligibility_messages.extend(health_issues)
            if scheduling_evidence["unschedulable"]:
                eligibility_codes.append("K8S_NODE_UNSCHEDULABLE")
                eligibility_messages.append("spec.unschedulable=true")
            if eligibility_codes:
                record["summary"] = incomplete_summary(eligibility_codes)
                raise RuntimeError(
                    "node is not eligible for training: " + "; ".join(eligibility_messages)
                )
            allocatable = _int_resource(node_payload, device_resource_name)
            if allocatable is None or allocatable < expected_devices:
                raise RuntimeError(
                    f"node allocatable {device_resource_name}={allocatable}; expected at least {expected_devices}"
                )

            if node in reuse_pods:
                target_namespace, target_pod, target_container = reuse_pods[node]
            else:
                target_container = probe_container
                target_pod = _probe_pod_name(run_id, node)
                manifest = build_probe_manifest(
                    namespace=target_namespace,
                    pod_name=target_pod,
                    container_name=target_container,
                    node=node,
                    image=image,
                    image_pull_policy=image_pull_policy,
                    device_resource_name=device_resource_name,
                    device_count=expected_devices,
                    run_id=run_id,
                    node_taints=node_payload.get("spec", {}).get("taints", []),
                    active_deadline_seconds=max(pod_ready_timeout + 300, 600),
                    memory_request=probe_memory_request,
                    memory_limit=probe_memory_limit,
                    probe_env=probe_env,
                )
                # Mark the deterministic name before CREATE. If the client times out
                # after the API server accepted the Pod, finally still finds and
                # removes exactly the Pod carrying this run-id label.
                created_pod = target_pod
                record["cleanup_status"] = "CREATE_ATTEMPTED"
                controller.create_probe(manifest)
                record["cleanup_status"] = "CREATED"
                controller.wait_ready(target_namespace, target_pod, pod_ready_timeout)

                if bootstrap_wheel is not None:
                    remote_wheel = f"/tmp/{bootstrap_wheel.name}"
                    controller.copy_to_pod(
                        bootstrap_wheel,
                        target_namespace,
                        target_pod,
                        target_container,
                        remote_wheel,
                    )
                    bootstrap_executor = _RateLimitedKubernetesPodExecutor(
                        KubernetesPodTarget(
                            namespace=target_namespace,
                            pod=target_pod,
                            container=target_container,
                            context=context,
                            kubeconfig=kubeconfig,
                            device_resource_name=device_resource_name,
                        ),
                        timeout_seconds=command_timeout,
                        api_limiter=api_limiter,
                    )
                    bootstrap_result = bootstrap_executor.exec(
                        "bootstrap_wheel_install",
                        [
                            "python3",
                            "-m",
                            "pip",
                            "install",
                            "--no-index",
                            "--no-deps",
                            "--force-reinstall",
                            remote_wheel,
                        ],
                        timeout=120,
                    )
                    bootstrap_stdout = node_dir / "bootstrap-wheel.stdout"
                    bootstrap_stderr = node_dir / "bootstrap-wheel.stderr"
                    bootstrap_stdout.write_text(bootstrap_result.stdout, encoding="utf-8")
                    bootstrap_stderr.write_text(bootstrap_result.stderr, encoding="utf-8")
                    bootstrap_stdout.chmod(0o600)
                    bootstrap_stderr.chmod(0o600)
                    record["bootstrap"] = {
                        "wheel": bootstrap_wheel.name,
                        "sha256": normalized_bootstrap_sha256,
                        "returncode": bootstrap_result.returncode,
                        "stdout_path": str(bootstrap_stdout),
                        "stderr_path": str(bootstrap_stderr),
                        "stdout_total_bytes": bootstrap_result.stdout_total_bytes,
                        "stderr_total_bytes": bootstrap_result.stderr_total_bytes,
                        "stdout_truncated": bootstrap_result.stdout_truncated,
                        "stderr_truncated": bootstrap_result.stderr_truncated,
                    }
                    if bootstrap_result.returncode != 0:
                        raise RuntimeError(
                            f"temporary probe wheel installation failed rc={bootstrap_result.returncode}"
                        )

            executor = _RateLimitedKubernetesPodExecutor(
                KubernetesPodTarget(
                    namespace=target_namespace,
                    pod=target_pod,
                    container=target_container,
                    context=context,
                    kubeconfig=kubeconfig,
                    device_resource_name=device_resource_name,
                ),
                timeout_seconds=command_timeout,
                api_limiter=api_limiter,
            )
            result = run_k8s_hcu_preflight(
                executor,
                expected_devices=expected_devices,
                max_vram_used_percent=max_vram_used_percent,
                max_hcu_util_percent=max_hcu_util_percent,
                samples=samples,
                busy_sample_quorum=busy_sample_quorum,
                sample_interval_seconds=sample_interval_seconds,
                evidence_dir=node_dir / "evidence",
                include_environment=include_environment,
                require_compiler=require_compiler,
                require_rdma=require_rdma,
                minimum_rdma_devices=minimum_rdma_devices,
                expected_rdma_protocol=expected_rdma_protocol,
                require_rccl=require_rccl,
                require_ucx=require_ucx,
                rdma_policy=rdma_policy,
                rdma_counter_interval_seconds=rdma_counter_interval_seconds,
            )
            _enforce_truncated_output_status(result)
            if result.target.get("node") != node:
                raise RuntimeError(
                    f"probe landed on {result.target.get('node')!r}; expected {node!r}"
                )
            result_path = node_dir / "preflight-result.json"
            save_result(result, result_path)
            record.update(
                {
                    "status": result.status,
                    "summary": _node_summary(result),
                    "result_path": str(result_path),
                    "evidence_dir": result.evidence_dir,
                    "target": {
                        "namespace": target_namespace,
                        "pod": target_pod,
                        "container": target_container,
                    },
                }
            )
        except Exception as exc:
            record["status"] = "INCOMPLETE"
            record["error"] = str(exc)
            record.setdefault("summary", incomplete_summary())
        finally:
            if created_pod:
                try:
                    cleanup_status, cleanup_error = controller.delete_probe(
                        target_namespace, created_pod, run_id
                    )
                except Exception as cleanup_exc:
                    cleanup_status = "CLEANUP_REQUIRED"
                    cleanup_error = str(cleanup_exc)
                record["cleanup_status"] = cleanup_status
                if cleanup_error:
                    record["cleanup_error"] = cleanup_error
                if cleanup_status not in {"DELETED", "ALREADY_GONE"}:
                    record["status"] = "INCOMPLETE"
                    record.setdefault("summary", incomplete_summary()).setdefault(
                        "reason_codes", []
                    ).append("K8S_PROBE_CLEANUP_REQUIRED")
        return record

    future_by_node: dict[str, Any] = {}
    with ThreadPoolExecutor(
        max_workers=effective_concurrency,
        thread_name_prefix="hcu-envcheck-k8s",
    ) as pool:
        for node in nodes:
            future_by_node[node] = pool.submit(probe_node, node)

        # Resolve in caller-supplied node order. Worker completion order therefore
        # cannot make JSON, Markdown, or folded groups nondeterministic.
        records: list[dict[str, Any]] = []
        for node in nodes:
            try:
                records.append(future_by_node[node].result())
            except Exception as exc:
                records.append(
                    {
                        "node": node,
                        "status": "INCOMPLETE",
                        "probe_source": (
                            "existing-pod" if node in reuse_pods else "temporary-pod"
                        ),
                        "cleanup_status": (
                            "NOT_APPLICABLE" if node in reuse_pods else "CLEANUP_REQUIRED"
                        ),
                        "error": f"unhandled worker failure: {exc}",
                        "summary": incomplete_summary(),
                    }
                )
    consistency_findings = (
        _rdma_protocol_consistency_findings(records)
        if include_environment
        else []
    )
    if strict_stack_consistency and include_environment:
        consistency_fields = [
            "kernel",
            "dtk_version",
            "driver_version",
            "hy_smi_version",
            "smi_library_version",
            "torch_version",
            "torch_hip_version",
            "python_version",
            "vbios_versions",
            "hsw_firmware_versions",
            "core_python_packages",
            "runtime_env",
        ]
        if require_compiler:
            consistency_fields.append("hipcc_version")
        if require_rccl:
            consistency_fields.extend(["rccl_paths", "torch_nccl_backend_available"])
        if require_ucx:
            consistency_fields.append("ucx_version")
        if require_rdma or minimum_rdma_devices or expected_rdma_protocol != "auto":
            consistency_fields.extend(
                [
                    "nic_hardware_profile",
                    "rdma_hardware_profile",
                    "rdma_device_count",
                    "rdma_active_device_count",
                    "rdma_active_port_count",
                    "rdma_rates",
                ]
            )
        for field in consistency_fields:
            values, missing_nodes = _environment_field_values(records, field)
            if missing_nodes:
                consistency_findings.append(
                    {
                        "severity": "UNKNOWN",
                        "reason_code": "STACK_COMPONENT_EVIDENCE_MISSING",
                        "field": field,
                        "values": values,
                        "missing_nodes": missing_nodes,
                        "message": f"stack field {field} is unavailable on some nodes",
                    }
                )
            if len(values) > 1:
                reason_code = {
                    "core_python_packages": "PYTHON_CORE_PACKAGE_INCONSISTENT",
                    "runtime_env": "CONTAINER_RUNTIME_ENV_INCONSISTENT",
                    "nic_hardware_profile": "NIC_PROFILE_INCONSISTENT",
                    "nic_link_profile": "NIC_LINK_PROFILE_INCONSISTENT",
                    "nic_link_summary": "NIC_LINK_STATE_INCONSISTENT",
                    "rdma_hardware_profile": "RDMA_ADAPTER_PROFILE_INCONSISTENT",
                }.get(field, "STACK_COMPONENT_INCONSISTENT")
                consistency_findings.append(
                    {
                        "severity": "FAIL",
                        "reason_code": reason_code,
                        "field": field,
                        "values": values,
                        "message": f"stack field {field} differs across nodes",
                    }
                )
        image_values: dict[str, list[str]] = {}
        missing_image_nodes: list[str] = []
        for record in records:
            image_id = record.get("summary", {}).get("image_id")
            if image_id is None:
                missing_image_nodes.append(record["node"])
                continue
            value = json.dumps(image_id, ensure_ascii=False)
            image_values.setdefault(value, []).append(record["node"])
        if missing_image_nodes:
            consistency_findings.append(
                {
                    "severity": "UNKNOWN",
                    "reason_code": "CONTAINER_IMAGE_DIGEST_MISSING",
                    "field": "image_id",
                    "values": image_values,
                    "missing_nodes": sorted(missing_image_nodes, key=_natural_node_key),
                    "message": "container image digest is unavailable on some nodes",
                }
            )
        if len(image_values) > 1:
            consistency_findings.append(
                {
                    "severity": "FAIL",
                    "reason_code": "CONTAINER_IMAGE_DIGEST_INCONSISTENT",
                    "field": "image_id",
                    "values": image_values,
                    "message": "container image digest differs across nodes",
                }
            )

    if any(item.get("severity") == "FAIL" for item in consistency_findings) or any(
        item["status"] == "BLOCKED" for item in records
    ):
        status = "BLOCKED"
    elif any(item.get("severity") == "UNKNOWN" for item in consistency_findings) or any(
        item["status"] == "INCOMPLETE" for item in records
    ):
        status = "INCOMPLETE"
    else:
        status = "READY"
    scale_readiness = _build_scale_readiness(
        records,
        cluster_status=status,
        target_devices=target_scale_devices,
        devices_per_node=expected_devices,
        consistency_findings=consistency_findings,
    )
    cleanup_counts = Counter(record.get("cleanup_status", "UNKNOWN") for record in records)
    temporary_pod_count = sum(
        record.get("probe_source") == "temporary-pod" for record in records
    )
    cleanup_completed_count = sum(
        cleanup_counts.get(item, 0) for item in ("DELETED", "ALREADY_GONE")
    )
    cleanup_summary = {
        "temporary_pods": temporary_pod_count,
        "completed": cleanup_completed_count,
        "requires_attention": sum(
            cleanup_counts.get(item, 0)
            for item in ("CLEANUP_REQUIRED", "CLEANUP_REFUSED")
        ),
        "status_counts": dict(sorted(cleanup_counts.items())),
    }
    report = {
        "schema_version": "1.0",
        "tool_version": __version__,
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": _utc_now(),
        "status": status,
        "mode": "k8s-bounded-parallel-node-probe",
        "execution": {
            "requested_concurrency": concurrency,
            "effective_concurrency": effective_concurrency,
            "concurrency_hard_limit": MAX_K8S_CLUSTER_CONCURRENCY,
            "api_rate_limit": {
                "qps": normalized_api_qps,
                "burst": api_burst,
                "requests_observed": api_limiter.request_count,
            },
            "cleanup": cleanup_summary,
            "result_order": "requested-node-order",
            "output_capture": {
                "strategy": "bounded-head-tail",
                "per_stream_limit_bytes": KUBECTL_OUTPUT_CAPTURE_LIMIT_BYTES,
            },
        },
        "policy": {
            "device_resource_name": device_resource_name,
            "automatic_taint_toleration_keys": [device_resource_name],
            "blocked_standard_taint_keys": sorted(STANDARD_HEALTH_TAINT_KEYS),
            "expected_devices": expected_devices,
            "target_scale_devices": target_scale_devices,
            "max_vram_used_percent": max_vram_used_percent,
            "max_hcu_util_percent": max_hcu_util_percent,
            "samples": samples,
            "busy_sample_quorum": busy_sample_quorum,
            "sample_interval_seconds": sample_interval_seconds,
            "concurrency": effective_concurrency,
            "requested_concurrency": concurrency,
            "effective_concurrency": effective_concurrency,
            "api_qps": normalized_api_qps,
            "api_burst": api_burst,
            "include_environment": include_environment,
            "require_rdma": require_rdma,
            "minimum_rdma_devices": minimum_rdma_devices,
            "expected_rdma_protocol": expected_rdma_protocol,
            "rdma_policy": rdma_policy,
            "rdma_counter_interval_seconds": rdma_counter_interval_seconds,
            "require_rccl": require_rccl,
            "require_ucx": require_ucx,
            "require_compiler": require_compiler,
            "strict_stack_consistency": strict_stack_consistency,
            "bootstrap_wheel": bootstrap_wheel.name if bootstrap_wheel else None,
            "bootstrap_wheel_sha256": normalized_bootstrap_sha256,
            "probe_memory_request": probe_memory_request,
            "probe_memory_limit": probe_memory_limit,
            "probe_env": dict(sorted((probe_env or {}).items())),
        },
        "nodes": records,
        "node_result_groups": _group_node_results(records),
        "node_scheduling_groups": _group_node_scheduling_evidence(records),
        "groups": _group_nodes(records),
        # Retained for JSON consumers of schema 1.0.  New reports use the
        # projected groups below so software drift never splits hardware.
        "environment_groups": _group_environments(records),
        "hardware_environment_groups": _group_hardware_environments(records),
        "software_environment_groups": _group_software_environments(records),
        "check_coverage": _build_check_coverage(
            records,
            include_environment=include_environment,
        ),
        "scale_readiness": scale_readiness,
        "consistency_findings": consistency_findings,
    }
    json_path = output_dir / "cluster-result.json"
    md_path = output_dir / "cluster-summary.md"
    atomic_write_text_exclusive(
        json_path,
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
    )
    _write_cluster_markdown(report, md_path)
    return report, json_path, md_path
