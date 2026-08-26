# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from .hostfile import HostEntry, parse_hostfile, write_ansible_inventory
from .models import ProcessRole, StackFrame, StackSnapshot

# ansible ad-hoc: "worker01 | CHANGED | rc=0 >>\n[{...}]"
_ANSIBLE_LINE = re.compile(
    r"^(?P<host>\S+)\s+\|\s+(?P<status>\S+)\s+\|\s+rc=(?P<rc>\d+)(?:\s+>>)?\s*(?P<body>.*)$",
    re.DOTALL,
)


@dataclass
class AnsibleConfig:
    ansible_bin: str = "ansible"
    inventory_path: Path | None = None
    remote_user: str | None = None
    forks: int = 32
    timeout_sec: float = 30.0
    py_spy_path: str = "py-spy"
    capture_timeout: float = 15.0
    remote_script: Path | None = None


def default_remote_script() -> Path:
    return Path(__file__).resolve().parent / "scripts" / "remote_capture.py"


def ensure_ansible(config: AnsibleConfig) -> None:
    if not shutil.which(config.ansible_bin):
        raise RuntimeError(
            f"'{config.ansible_bin}' not found. Install Ansible: pip install ansible"
        )


def _parse_host_stdout(body: str) -> list[dict]:
    body = body.strip()
    if not body:
        return []
    return json.loads(body)


def parse_ansible_adhoc_output(output: str) -> dict[str, list[dict]]:
    """Parse default ansible ad-hoc stdout into {hostname: [stack_dict, ...]}."""
    results: dict[str, list[dict]] = {}
    current_host: str | None = None
    current_lines: list[str] = []

    def flush() -> None:
        nonlocal current_host, current_lines
        if current_host is None:
            return
        body = "\n".join(current_lines).strip()
        if body:
            results[current_host] = _parse_host_stdout(body)
        current_host = None
        current_lines = []

    for line in output.splitlines():
        match = _ANSIBLE_LINE.match(line)
        if match:
            flush()
            current_host = match.group("host")
            rc = int(match.group("rc"))
            if rc != 0:
                raise RuntimeError(
                    f"Ansible capture failed on {current_host} (rc={rc}): "
                    f"{match.group('body').strip()}"
                )
            tail = match.group("body")
            if tail:
                current_lines.append(tail)
        elif current_host is not None:
            current_lines.append(line)

    flush()
    return results


def stacks_from_payload(
    payload: list[dict],
    *,
    fallback_host: str,
    rank_start: int,
) -> list[StackSnapshot]:
    snapshots: list[StackSnapshot] = []
    for item in payload:
        frames = tuple(
            StackFrame(
                function=f["function"],
                file=f.get("file", ""),
                line=int(f.get("line", 0)),
            )
            for f in item.get("frames", [])
        )
        rank = int(item.get("rank", -1))
        if rank < 0 and rank_start >= 0:
            rank = rank_start

        role_raw = item.get("role", "trainer")
        try:
            role = ProcessRole(role_raw)
        except ValueError:
            role = ProcessRole.OTHER

        snapshots.append(
            StackSnapshot(
                machine_id=item.get("machine_id") or fallback_host,
                rank=rank,
                pid=int(item.get("pid", 0)),
                role=role,
                frames=frames,
                raw_text=item.get("raw_text", ""),
            )
        )
    return snapshots


class AnsibleStackCollector:
    """Collect py-spy stacks from all hosts listed in an MPI-style hostfile."""

    def __init__(self, config: AnsibleConfig | None = None) -> None:
        self.config = config or AnsibleConfig()
        if self.config.remote_script is None:
            self.config.remote_script = default_remote_script()

    def collect(
        self,
        hostfile: Path | str,
        *,
        save_raw: Path | None = None,
    ) -> list[StackSnapshot]:
        ensure_ansible(self.config)
        hosts = parse_hostfile(hostfile)
        host_by_name = {host.hostname: host for host in hosts}

        inventory = self.config.inventory_path
        temp_inventory: Path | None = None
        if inventory is None:
            temp_inventory = write_ansible_inventory(
                hosts, default_user=self.config.remote_user
            )
            inventory = temp_inventory

        remote_script = self.config.remote_script
        if not remote_script.is_file():
            raise FileNotFoundError(f"Remote capture script not found: {remote_script}")

        # Per-host rank_start is in inventory vars; script reads from env when using shell,
        # but ansible script module passes no inventory vars automatically.
        # Use ansible -e rank_start=... per host via `-e` with loop, or embed in `-a` args.
        # Simplest: one ansible run; remote script uses RANK from process cmdline when present,
        # otherwise rank_start passed via `-e` host var expanded in command — use shell module
        # with inventory vars instead of script module.

        script_args = (
            f"{remote_script} "
            "--rank-start {{ rank_start }} "
            "--machine-id {{ inventory_hostname }} "
            f"--py-spy {self.config.py_spy_path} "
            f"--timeout {self.config.capture_timeout}"
        )
        cmd = [
            self.config.ansible_bin,
            "-i",
            str(inventory),
            "training",
            "-m",
            "script",
            "-a",
            script_args,
            "-f",
            str(self.config.forks),
            "-T",
            str(int(self.config.timeout_sec)),
        ]
        if self.config.remote_user:
            cmd.extend(["-u", self.config.remote_user])

        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if save_raw:
            save_raw.parent.mkdir(parents=True, exist_ok=True)
            save_raw.write_text(
                (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else ""),
                encoding="utf-8",
            )

        if proc.returncode != 0:
            raise RuntimeError(
                "Ansible stack collection failed:\n"
                f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
            )

        per_host = parse_ansible_adhoc_output(proc.stdout)
        all_snapshots: list[StackSnapshot] = []

        for hostname, payload in per_host.items():
            host = host_by_name.get(hostname)
            rank_start = host.rank_start if host else -1
            all_snapshots.extend(
                stacks_from_payload(
                    payload, fallback_host=hostname, rank_start=rank_start
                )
            )

        missing = {host.hostname for host in hosts} - set(per_host)
        if missing:
            raise RuntimeError(f"No stack data returned for hosts: {sorted(missing)}")

        if temp_inventory:
            temp_inventory.unlink(missing_ok=True)

        return all_snapshots

    def collect_via_shell(
        self,
        hostfile: Path | str,
        *,
        save_raw: Path | None = None,
    ) -> list[StackSnapshot]:
        """
        Alternative: copy remote_capture.py content inline via shell module.
        Useful when script module path quoting is problematic on Windows controller.
        """
        ensure_ansible(self.config)
        hosts = parse_hostfile(hostfile)
        host_by_name = {host.hostname: host for host in hosts}

        script_body = self.config.remote_script.read_text(encoding="utf-8")
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".py", delete=False, encoding="utf-8"
        ) as handle:
            handle.write(script_body)
            temp_script = Path(handle.name)

        inventory = self.config.inventory_path or write_ansible_inventory(
            hosts, default_user=self.config.remote_user
        )

        remote_path = "/tmp/stack_analyzer_remote_capture.py"
        shell_args = (
            f"python3 {remote_path} "
            "--rank-start {{ rank_start }} "
            "--machine-id {{ inventory_hostname }} "
            f"--py-spy {self.config.py_spy_path} "
            f"--timeout {self.config.capture_timeout}"
        )
        cmd = [
            self.config.ansible_bin,
            "-i",
            str(inventory),
            "training",
            "-m",
            "shell",
            "-a",
            shell_args,
            "-f",
            str(self.config.forks),
        ]
        if self.config.remote_user:
            cmd.extend(["-u", self.config.remote_user])

        copy_cmd = [
            self.config.ansible_bin,
            "-i",
            str(inventory),
            "training",
            "-m",
            "copy",
            "-a",
            f"src={temp_script} dest={remote_path} mode=0755",
        ]
        if self.config.remote_user:
            copy_cmd.extend(["-u", self.config.remote_user])

        copy_proc = subprocess.run(copy_cmd, capture_output=True, text=True, check=False)
        temp_script.unlink(missing_ok=True)
        if copy_proc.returncode != 0:
            raise RuntimeError(f"Failed to deploy remote script:\n{copy_proc.stderr}")

        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if save_raw:
            save_raw.parent.mkdir(parents=True, exist_ok=True)
            save_raw.write_text(proc.stdout + "\n" + proc.stderr, encoding="utf-8")

        if proc.returncode != 0:
            raise RuntimeError(
                f"Ansible shell capture failed:\n{proc.stdout}\n{proc.stderr}"
            )

        per_host = parse_ansible_adhoc_output(proc.stdout)
        all_snapshots: list[StackSnapshot] = []
        for hostname, payload in per_host.items():
            host = host_by_name.get(hostname)
            rank_start = host.rank_start if host else -1
            all_snapshots.extend(
                stacks_from_payload(
                    payload, fallback_host=hostname, rank_start=rank_start
                )
            )
        return all_snapshots
