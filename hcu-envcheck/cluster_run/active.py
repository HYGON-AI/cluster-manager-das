# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Group scheduling, explicit payload profiles and run-scoped cancellation."""
from __future__ import annotations

import json
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, replace
from pathlib import Path
from typing import Sequence

from hcu_envcheck.baremetal import BaremetalClusterExecutor, BaremetalExecutionConfig
from hcu_envcheck.group_logs import direct_text, metadata_text, ensure_group_log, finish_group_log_state
from hcu_envcheck.output import claim_labeled_run_directory, run_directory_label
from .env import bootstrap_command
from .hostfile import NodeGroup, materialize_groups, split_nodes
from .preflight import ContainerPreflightError, run_container_preflight
from .task_control import RemoteTaskSession, TaskInterrupted, managed_command
from .testsuites import default_profile, default_processes, validate_profile


def _write(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


class ActiveTestExecutor:
    def __init__(self, *, scenario: str, test_name: str, nodes: Sequence[str], env_script: str | None,
                 output_dir: Path, group_size: int | None, slots: int, launcher: str,
                 strict_size: bool = False, nproc_per_node: int | None = None,
                 master_port: int = 29500, timeout_seconds: float = 0, transport: str = "ssh",
                 concurrency: int = 32, dry_run: bool = False, execution_scope: str = "host",
                 container_name: str | None = None, container_shell: str = "bash",
                 container_workdir: str | None = None, test_python: str = "python3",
                 expected_image: str | None = None,
                 check_idle: bool = True, profile: str | None = None, container_ssh_port: int = 25901,
                 ib_config=None, ib_state_config=None):
        profile = profile or default_profile(test_name)
        nproc_per_node = default_processes(profile) if nproc_per_node is None else nproc_per_node
        validate_profile(test_name, profile)
        if launcher not in {"mpirun", "direct"}:
            raise ValueError("RCCL owns its MPI launch; IB uses direct execution")
        if min(slots, nproc_per_node, concurrency) < 1:
            raise ValueError("slots, nproc_per_node and concurrency must be positive")
        if not 1 <= master_port <= 65535 or not 1 <= container_ssh_port <= 65535:
            raise ValueError("master/container SSH port must be between 1 and 65535")
        if not math.isfinite(timeout_seconds) or timeout_seconds < 0:
            raise ValueError("timeout must be non-negative")
        if execution_scope not in {"host", "container"}:
            raise ValueError("execution_scope must be host or container")
        if execution_scope == "container" and not container_name:
            raise ValueError("container_name is required")
        if container_shell not in {"bash", "sh"}:
            raise ValueError("container_shell must be bash or sh")
        self.scenario, self.test_name, self.nodes = scenario, test_name, tuple(nodes)
        self.env_script, self.output_dir = env_script, Path(output_dir)
        self.group_size, self.strict_size, self.slots = group_size, strict_size, slots
        self.launcher, self.profile = ("mpirun" if test_name == "rccl" else "direct"), profile
        self.test_python, self.expected_image = test_python, expected_image
        # Container images commonly run as root. MPI-based container launches
        # therefore opt in internally and consistently across preflight and the
        # generated mpirun command; this is not exposed as a user-facing switch.
        self.allow_root_mpi = execution_scope == "container" and test_name == "rccl"
        self.check_idle = check_idle
        self.nproc_per_node, self.master_port = nproc_per_node, master_port
        self.timeout_seconds, self.remote_timeout_seconds = timeout_seconds, timeout_seconds or 3540
        self.transport, self.concurrency, self.dry_run = transport, concurrency, dry_run
        self.execution_scope, self.container_name = execution_scope, container_name
        self.container_shell, self.container_ssh_port = container_shell, container_ssh_port
        self.container_workdir = container_workdir
        # No .resolve(): this must stay the path Python was actually invoked
        # with (set via PYTHONPATH to the site's canonical ROOT), not a
        # symlink-resolved alias (e.g. /public -> /public2) that may not be
        # mounted inside the container this path gets run in.
        self.worker_root = (container_workdir or str(Path(__file__).parents[1]))
        self.ib_config, self.ib_state_config = ib_config, ib_state_config
        self.session = None

    def _in_scope(self, command: list[str]) -> list[str]:
        if self.execution_scope == "host":
            return command
        return ["docker", "exec", "-i", *(["-w", self.container_workdir] if self.container_workdir else []),
                str(self.container_name), *command]

    def _leader_command(self, group: NodeGroup, command: list[str], exports=None) -> list[str]:
        wrapped = bootstrap_command(self.env_script, command,
            shell=self.container_shell if self.execution_scope == "container" else "bash", exports=exports,
            workdir=self.container_workdir)
        return self._in_scope(managed_command(
            ["timeout", "--signal=TERM", "--kill-after=15s", f"{self.remote_timeout_seconds:g}s", *wrapped],
            self.session.run_token))

    def _result_status(self, returncode: int, stdout: str) -> str:
        if returncode in {130, 143}:
            return "CANCELLED"
        # Payload JSON must never override the process/transport exit status.
        return "PASS" if returncode == 0 else "FAIL"

    def _single_remote_result(self, *, node: str, command_name: str, command: list[str], output_root: Path) -> dict:
        if self.session.cancel_event.is_set():
            return {"node": node, "returncode": 130, "status": "CANCELLED", "stdout": "", "stderr": "cancelled before dispatch"}
        try:
            group = self.session.config.log_group(node)
            config = replace(self.session.config, output_root=output_root, concurrency=1,
                             command_timeout_seconds=self.remote_timeout_seconds + 30,
                             group_log_groups=tuple((member, group) for member in self.session.config.log_members(group))
                             if group is not None else ())
            result = BaremetalClusterExecutor([node], config).execute(command_name, command, run_id=command_name)
            item = result.nodes.get(node)
            rc = item.returncode if item else 255
            stdout = item.stdout if item else ""
            return {"node": node, "returncode": rc, "status": self._result_status(rc, stdout),
                    "stdout": stdout, "stderr": item.stderr if item else "remote result missing",
                    "result_dir": item.result_dir if item else None}
        except (OSError, RuntimeError, ValueError) as exc:
            return {"node": node, "returncode": 127, "status": "FAIL", "stdout": "", "stderr": str(exc)}

    def _commands(self, group: NodeGroup) -> tuple[str, list[tuple[str, list[str]]]]:
        exports = {"HCU_GROUP_NAME": group.name, "HCU_GROUP_HOSTFILE": str(group.hostfile),
                   "HCU_GROUP_NODES": ",".join(group.nodes), "HCU_MASTER_ADDR": group.leader,
                   "HCU_MASTER_PORT": str(self.master_port + group.group_id)}
        if self.test_name == "rccl":
            command = ["bash", f"{self.worker_root}/hcu_envcheck/payloads/rccl_perf_test.sh"]
            exports.update({"HCU_RCCL_SCRIPT_DIR": str(group.hostfile.parent / "rccl"),
                            "HCU_TASK_GUARD_FILE": str(group.hostfile.parent.parent / "task-guard.sh"),
                            "HCU_STATE_CACHE_LOCK": str(group.hostfile.parent.parent / ".script-cache.lock")})
            exports.update({"HCU_CLUSTER_TIMEOUT_SECONDS": str(self.remote_timeout_seconds),
                            "HCU_TASK_TOKEN": self.session.run_token,
                            "HCU_ALLOW_ROOT_MPI": "1" if self.allow_root_mpi else "0"})
            if self.env_script is not None:
                exports["HCU_CLUSTER_ENV_SCRIPT"] = self.env_script
            command.extend([str(group.hostfile), "--port", str(self.container_ssh_port if self.execution_scope == "container" else 22)])
            # Different groups/runs must not overwrite the same timestamp log.
            command.extend(["--log-dir", str(group.hostfile.parent / "rccl")])
            if self.execution_scope == "container":
                # The common precheck already covers every group member.
                exports["CLUSTER_IDLE_CHECKED"] = "1"
            if not self.check_idle:
                command.append("--skip-idle-check")
            return "rccl-tests", [(group.leader, self._leader_command(group, command, exports))]
        raise ValueError(f"no command payload for {self.test_name}")

    def _run_group(self, group: NodeGroup, run_dir: Path) -> dict:
        root = run_dir / ".state" / "groups" / group.name
        root.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        if self.test_name == "ib-write-bw":
            from hcu_envcheck.cluster_checks import IBWriteBandwidthConfig
            from .extra_checks import run_group_ib
            result = run_group_ib(group.nodes, config=self.ib_config or IBWriteBandwidthConfig(enabled=True),
                ib_state_config=self.ib_state_config, env_script=self.env_script, scope=self.execution_scope,
                container=self.container_name, container_workdir=self.container_workdir,
                output_dir=root, execution_config=self.session.config, run_token=self.session.run_token,
                cancel_event=self.session.cancel_event, dry_run=self.dry_run)
            result.update({"group": group.name, "nodes": list(group.nodes), "launcher": "server-client"})
            _write(root / "result.json", result)
            return result
        effective, commands = self._commands(group)
        result = {"group": group.name, "nodes": list(group.nodes), "leader": group.leader,
                  "launcher": effective, "requested_launcher": self.launcher, "execution_profile": self.profile,
                  "execution_scope": self.execution_scope, "container_name": self.container_name,
                  "container_ssh_port": self.container_ssh_port if self.execution_scope == "container" else None,
                  "nproc_per_node": self.nproc_per_node, "np": None,
                  "master_addr": group.leader, "master_port": self.master_port + group.group_id,
                  "commands": [{"node": node, "command": command} for node, command in commands]}
        _write(root / "launch.json", result)
        if self.dry_run:
            result["status"] = "DRY_RUN"
        else:
            records = []
            with ThreadPoolExecutor(max_workers=min(len(commands), self.concurrency)) as pool:
                futures = [pool.submit(self._single_remote_result, node=node, command_name=f"{group.name}-{node}",
                    command=command, output_root=root / "remote-evidence" / node) for node, command in commands]
                for future in as_completed(futures):
                    item = future.result()
                    if item["status"] == "FAIL" and not self.session.cancel_event.is_set():
                        # Fail fast: rendezvous peers must not wait for the full
                        # remote timeout after a bootstrap/transport failure.
                        self.session.cancel_and_wait()
                    # The executor persists both streams before returning. Only
                    # dispatch failures without a transport record need a log.
                    if not item.get("result_dir"):
                        for stream in ("stdout", "stderr"):
                            direct_text(run_dir, group.name,
                                root / "nodes" / item["node"] / f"{stream}.log",
                                str(item.get(stream, "")))
                    item.pop("stdout", None)
                    item.pop("stderr", None)
                    records.append(item)
            records.sort(key=lambda item: item["node"])
            result["node_results"] = records
            statuses = {item["status"] for item in records}
            result["status"] = next((value for value in ("FAIL", "CANCELLED", "INCOMPLETE") if value in statuses), "PASS")
        result["duration_seconds"] = round(time.monotonic() - started, 3)
        _write(root / "result.json", result)
        return result

    def run(self, group_progress=None) -> tuple[dict, Path]:
        planned = split_nodes(self.nodes, group_size=self.group_size, strict_size=self.strict_size)
        self._group_count = len(planned)
        if self.master_port + len(planned) - 1 > 65535:
            raise ValueError("master_port plus group count exceeds 65535")
        needs_mpi = self.test_name == "rccl" and any(len(group) > 1 for group in planned)
        root = claim_labeled_run_directory(
            self.output_dir, run_directory_label(self.scenario, self.test_name))
        state = root / ".state"
        groups = materialize_groups(self.nodes, state / "groups", group_size=self.group_size,
                                    strict_size=self.strict_size, slots_per_node=self.nproc_per_node)
        log_groups = tuple((node, group.name) for group in groups for node in group.nodes)
        for group in groups:
            ensure_group_log(root, group.name)
        config = BaremetalExecutionConfig(output_root=state / "evidence", transport=self.transport,
            concurrency=self.concurrency, command_timeout_seconds=self.remote_timeout_seconds + 30,
            group_log_root=root, group_log_groups=log_groups)
        preflight = None
        if self.execution_scope == "container" and not self.dry_run:
            self.session = RemoteTaskSession(self.nodes,
                replace(config, output_root=state / "preflight" / "evidence", command_timeout_seconds=30),
                execution_scope=self.execution_scope, container_name=self.container_name)
            try:
                with self.session:
                    preflight = run_container_preflight(nodes=self.nodes, container_name=self.container_name,
                        expected_image=self.expected_image, launcher="mpirun" if needs_mpi else "direct",
                        allow_root_mpi=self.allow_root_mpi, env_script=self.env_script,
                        container_shell=self.container_shell, transport=self.transport, concurrency=self.concurrency,
                        run_dir=state, check_idle=self.check_idle and self.test_name == "rccl",
                        container_workdir=self.container_workdir, remote_python=self.test_python,
                        mpi_groups=planned if needs_mpi else (), container_ssh_port=self.container_ssh_port,
                        execution_config=self.session.config, run_token=self.session.run_token)
                    if preflight.get("execution_failed"):
                        preflight["cleanup"] = asdict(self.session.cancel_and_wait())
            except TaskInterrupted:
                cleanup = self.session.last_report
                preflight = {"status": "CANCELLED" if cleanup and cleanup.confirmed else "CLEANUP_UNCONFIRMED",
                    "interrupted": True, "issues": [], "cleanup": asdict(cleanup) if cleanup else None}
                _write(state / "preflight-result.json", preflight)
                raise ContainerPreflightError({**preflight, "run_dir": str(root)})
            except BaseException as exc:
                for group in groups:
                    metadata_text(root, group.name, state / "groups" / group.name / "preflight-error.log",
                        json.dumps({"stage": "preflight", "exception_type": type(exc).__name__,
                            "error": str(exc)}, ensure_ascii=False) + "\n")
                raise
            finally:
                if preflight is not None:
                    _write(state / "preflight-result.json", preflight)
                    for group in groups:
                        metadata_text(root, group.name, state / "groups" / group.name / "preflight-status.log",
                            json.dumps({"stage": "preflight", "status": preflight["status"],
                                "nodes": list(group.nodes)}, ensure_ascii=False) + "\n")
            if preflight["status"] != "PASS":
                raise ContainerPreflightError({**preflight, "run_dir": str(root)})
            preflight = {"status": "PASS", "node_count": len(self.nodes), "image_reference": preflight.get("image_reference")}
        self.session = RemoteTaskSession(self.nodes, config, execution_scope=self.execution_scope, container_name=self.container_name)
        results, cancelled = [], False
        try:
            with self.session:
                with ThreadPoolExecutor(max_workers=min(self.concurrency, len(groups))) as pool:
                    futures = {pool.submit(self._run_group, group, root): group for group in groups}
                    for future in as_completed(futures):
                        result = future.result()
                        results.append(result)
                        if group_progress is not None:
                            group_progress(str(result.get("status") or "UNKNOWN"))
                if not self.dry_run and any(item["status"] in {"FAIL", "CANCELLED"} for item in results):
                    # A failed leader/transport must not orphan still-running ranks.
                    self.session.cancel_and_wait()
        except TaskInterrupted:
            cancelled = True
        except BaseException:
            _write(state / "cleanup.json", asdict(self.session.last_report) if self.session.last_report else {"status": "UNCONFIRMED"})
            raise
        results.sort(key=lambda item: item["group"])
        status = "DRY_RUN" if self.dry_run else ("CANCELLED" if cancelled else
                 next((value for value in ("FAIL", "CANCELLED", "INCOMPLETE") if any(item["status"] == value for item in results)), "PASS"))
        cleanup = self.session.last_report
        if cleanup and not cleanup.confirmed:
            status = "CLEANUP_UNCONFIRMED"
        for group in groups:
            group_result = next((item for item in results if item.get("group") == group.name), {})
            metadata_text(root, group.name, state / "groups" / group.name / "result-status.log",
                json.dumps({"stage": self.test_name, "status": group_result.get("status", status),
                    "nodes": list(group.nodes), "cleanup": {
                        node: asdict(cleanup.nodes[node]) for node in group.nodes if cleanup and node in cleanup.nodes}},
                    ensure_ascii=False) + "\n")
        report = {"schema_version": "1.1", "kind": "active_test", "scenario": self.scenario,
                  "test_name": self.test_name, "launcher": self.launcher, "profile": self.profile,
                  "group_size": self.group_size or len(self.nodes), "slots_per_node": self.slots,
                  "node_count": len(self.nodes), "group_count": len(groups), "nodes": list(self.nodes),
                  "env_script": self.env_script, "execution_scope": self.execution_scope,
                  "container_name": self.container_name, "container_workdir": self.container_workdir,
                  "container_shell": self.container_shell, "container_ssh_port": self.container_ssh_port,
                  "allow_root_mpi": self.allow_root_mpi,
                  "expected_image": self.expected_image, "idle_check": self.check_idle, "preflight": preflight,
                  "strict_size": self.strict_size, "nproc_per_node": self.nproc_per_node, "np": None,
                  "master_addr": groups[0].leader, "master_port": self.master_port,
                  "test_python": self.test_python, "run_token": self.session.run_token,
                  "groups": results, "status": status, "interrupted": cancelled,
                  "cleanup": asdict(cleanup) if cleanup else None}
        _write(state / "active-result.json", report)
        finish_group_log_state(root, cleanup_confirmed=(cleanup.confirmed if cleanup else
            not cancelled and all(item["status"] in {"PASS", "DRY_RUN"} for item in results)))
        return report, root
