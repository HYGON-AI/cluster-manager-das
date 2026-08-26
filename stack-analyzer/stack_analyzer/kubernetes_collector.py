# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

from .ansible_collector import stacks_from_payload
from .models import StackSnapshot


@dataclass
class KubernetesConfig:
    kubectl_bin: str = "kubectl"
    namespace: str = "default"
    selector: str = ""
    container: str | None = None
    all_containers: bool = False
    python_bin: str = "python3"
    py_spy_path: str = "py-spy"
    capture_timeout: float = 15.0
    command_timeout: float = 60.0
    parallelism: int = 16
    nonblocking: bool = True
    remote_script: Path | None = None


def default_remote_script() -> Path:
    return Path(__file__).resolve().parent / "scripts" / "remote_capture.py"


class KubernetesStackCollector:
    """Collect stacks by running the capture script inside Kubernetes containers."""

    def __init__(self, config: KubernetesConfig | None = None) -> None:
        self.config = config or KubernetesConfig()
        if self.config.remote_script is None:
            self.config.remote_script = default_remote_script()

    def _kubectl(self, *args: str, input_text: str | None = None) -> subprocess.CompletedProcess:
        cmd = [self.config.kubectl_bin, *args]
        return subprocess.run(
            cmd,
            input=input_text,
            capture_output=True,
            text=True,
            timeout=self.config.command_timeout,
            check=False,
        )

    def _targets(self) -> list[tuple[str, str]]:
        args = ["get", "pods", "-n", self.config.namespace]
        if self.config.selector:
            args.extend(["-l", self.config.selector])
        args.extend(["-o", "json"])
        proc = self._kubectl(*args)
        if proc.returncode != 0:
            raise RuntimeError(f"kubectl get pods failed: {proc.stderr.strip()}")

        targets: list[tuple[str, str]] = []
        for pod in json.loads(proc.stdout).get("items", []):
            if pod.get("status", {}).get("phase") != "Running":
                continue
            pod_name = pod["metadata"]["name"]
            containers = [item["name"] for item in pod.get("spec", {}).get("containers", [])]
            if self.config.container:
                if self.config.container not in containers:
                    continue
                selected = [self.config.container]
            elif self.config.all_containers:
                selected = containers
            else:
                selected = containers[:1]
            targets.extend((pod_name, container) for container in selected)
        return targets

    def _collect_one(self, pod: str, container: str, script: str) -> list[StackSnapshot]:
        machine_id = f"{pod}/{container}"
        args = [
            "exec", "-i", "-n", self.config.namespace, pod, "-c", container,
            "--", self.config.python_bin, "-",
            "--machine-id", machine_id,
            "--py-spy", self.config.py_spy_path,
            "--timeout", str(self.config.capture_timeout),
        ]
        if self.config.nonblocking:
            args.append("--nonblocking")
        args.append("--ranked-only")
        proc = self._kubectl(*args, input_text=script)
        if proc.returncode != 0:
            detail = proc.stderr.strip() or proc.stdout.strip()
            raise RuntimeError(f"stack capture failed in {machine_id}: {detail}")
        try:
            payload = json.loads(proc.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"invalid capture output from {machine_id}: {proc.stdout[:500]}"
            ) from exc
        return stacks_from_payload(payload, fallback_host=machine_id, rank_start=-1)

    def collect(self, *, save_raw: Path | None = None) -> list[StackSnapshot]:
        if not shutil.which(self.config.kubectl_bin):
            raise RuntimeError(f"'{self.config.kubectl_bin}' not found")
        script_path = self.config.remote_script
        if script_path is None or not script_path.is_file():
            raise FileNotFoundError(f"Remote capture script not found: {script_path}")

        targets = self._targets()
        if not targets:
            raise RuntimeError("No running Kubernetes containers matched the selection")
        script = script_path.read_text(encoding="utf-8")
        snapshots: list[StackSnapshot] = []
        errors: list[str] = []
        workers = max(1, min(self.config.parallelism, len(targets)))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(self._collect_one, pod, container, script): (pod, container)
                for pod, container in targets
            }
            for future in as_completed(futures):
                try:
                    snapshots.extend(future.result())
                except Exception as exc:  # report all failed pods together
                    errors.append(str(exc))

        if save_raw:
            save_raw.parent.mkdir(parents=True, exist_ok=True)
            save_raw.write_text("\n".join(errors), encoding="utf-8")
        if errors:
            raise RuntimeError("Kubernetes stack collection failed:\n" + "\n".join(errors))
        if not snapshots:
            raise RuntimeError(
                "No ranked training-worker stacks were returned. "
                "The pods may have restarted/terminated during capture, or RANK/LOCAL_RANK "
                "is unavailable in the worker processes."
            )
        return snapshots
