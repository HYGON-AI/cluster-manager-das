# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

from .ansible_collector import stacks_from_payload
from .hostfile import HostEntry, parse_hostfile
from .models import StackSnapshot


@dataclass
class DockerSSHConfig:
    ssh_bin: str = "ssh"
    ssh_user: str | None = None
    ssh_port: int = 22
    ssh_options: list[str] = field(default_factory=list)
    identity_file: str | None = None
    python_bin: str = "python3"
    py_spy_path: str = "py-spy"
    capture_timeout: float = 15.0
    command_timeout: float = 60.0
    parallelism: int = 16
    nonblocking: bool = True
    remote_script: Path | None = None


def default_remote_script() -> Path:
    return Path(__file__).resolve().parent / "scripts" / "remote_capture.py"


class DockerSSHStackCollector:
    """Collect stacks by SSHing into Docker containers running sshd."""

    def __init__(self, config: DockerSSHConfig | None = None) -> None:
        self.config = config or DockerSSHConfig()
        if self.config.remote_script is None:
            self.config.remote_script = default_remote_script()

    def _target(self, host: HostEntry) -> str:
        connect_host = host.ansible_host or host.hostname
        if self.config.ssh_user:
            return f"{self.config.ssh_user}@{connect_host}"
        if host.ansible_user:
            return f"{host.ansible_user}@{connect_host}"
        return connect_host

    def _ssh(self, host: HostEntry, script: str) -> subprocess.CompletedProcess:
        args = [
            self.config.ssh_bin,
            "-p",
            str(self.config.ssh_port),
            "-o",
            "BatchMode=yes",
            "-o",
            f"ConnectTimeout={int(self.config.command_timeout)}",
        ]
        for option in self.config.ssh_options:
            args.extend(["-o", option])
        if self.config.identity_file:
            args.extend(["-i", self.config.identity_file])
        args.extend(
            [
                self._target(host),
                self.config.python_bin,
                "-",
                "--rank-start",
                str(host.rank_start),
                "--machine-id",
                host.hostname,
                "--py-spy",
                self.config.py_spy_path,
                "--timeout",
                str(self.config.capture_timeout),
            ]
        )
        if self.config.nonblocking:
            args.append("--nonblocking")
        args.append("--ranked-only")
        return subprocess.run(
            args,
            input=script,
            capture_output=True,
            text=True,
            timeout=self.config.command_timeout,
            check=False,
        )

    def _collect_one(self, host: HostEntry, script: str) -> list[StackSnapshot]:
        proc = self._ssh(host, script)
        if proc.returncode != 0:
            detail = proc.stderr.strip() or proc.stdout.strip()
            raise RuntimeError(f"stack capture failed on {host.hostname}: {detail}")
        try:
            payload = json.loads(proc.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"invalid capture output from {host.hostname}: {proc.stdout[:500]}"
            ) from exc
        return stacks_from_payload(
            payload, fallback_host=host.hostname, rank_start=host.rank_start
        )

    def collect(
        self,
        hostfile: Path | str,
        *,
        save_raw: Path | None = None,
    ) -> list[StackSnapshot]:
        if not shutil.which(self.config.ssh_bin):
            raise RuntimeError(f"'{self.config.ssh_bin}' not found")
        script_path = self.config.remote_script
        if script_path is None or not script_path.is_file():
            raise FileNotFoundError(f"Remote capture script not found: {script_path}")

        hosts = parse_hostfile(hostfile)
        script = script_path.read_text(encoding="utf-8")
        snapshots: list[StackSnapshot] = []
        errors: list[str] = []
        workers = max(1, min(self.config.parallelism, len(hosts)))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(self._collect_one, host, script): host for host in hosts}
            for future in as_completed(futures):
                host = futures[future]
                try:
                    snapshots.extend(future.result())
                except Exception as exc:  # report all failed containers together
                    errors.append(f"{host.hostname}: {exc}")

        if save_raw:
            save_raw.parent.mkdir(parents=True, exist_ok=True)
            save_raw.write_text("\n".join(errors), encoding="utf-8")
        if errors:
            raise RuntimeError("Docker stack collection failed:\n" + "\n".join(errors))
        if not snapshots:
            raise RuntimeError(
                "No ranked training-worker stacks were returned. "
                "Check that Docker containers are reachable over SSH and that "
                "RANK/LOCAL_RANK is available in the worker processes."
            )
        return snapshots
