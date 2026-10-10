# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Run a diagnostic script independently on every listed node.

Unlike active benchmarks, this operation does not group nodes or start MPI.
The script path must be visible in each target execution scope.
"""

from __future__ import annotations

import json
import math
import tempfile
from dataclasses import asdict
from pathlib import Path
from typing import Sequence

from hcu_envcheck.baremetal import BaremetalClusterExecutor, BaremetalExecutionConfig
from hcu_envcheck.output import claim_labeled_run_directory, run_directory_label

from .env import bootstrap_command
from .preflight import run_container_status
from .task_control import RemoteTaskSession, TaskInterrupted, managed_command


def run_node_script(
    *, nodes: Sequence[str], scenario: str, env_script: str | None,
    script: str, script_args: Sequence[str], container_name: str | None,
    image: str | None, transport: str, concurrency: int, timeout: float,
    output_dir: Path, dry_run: bool,
    container_shell: str = "bash", container_workdir: str | None = None,
    test_python: str = "python3",
) -> tuple[dict, Path]:
    if not script.strip() or "\x00" in script:
        raise ValueError("script operation requires a target-visible --script path")
    if not script.startswith("/"):
        raise ValueError("--script must be an absolute path visible on every target node/container")
    if not math.isfinite(timeout) or timeout < 0:
        raise ValueError("--timeout must be non-negative")
    if container_shell not in {"bash", "sh"}:
        raise ValueError("container_shell must be bash or sh")
    if not test_python.strip() or "\x00" in test_python:
        raise ValueError("test_python must name an interpreter")
    if container_workdir is not None and (not container_workdir.startswith("/") or "\x00" in container_workdir):
        raise ValueError("container_workdir must be an absolute container-visible path")
    container = scenario == "per-node-container"
    if container and not container_name:
        raise ValueError("per-node-container script requires --container")
    command = [test_python if script.endswith(".py") else "bash", script, *script_args]
    remote_timeout = timeout or 600
    # Include environment setup in the remote timeout. The guard lives outside
    # it, so it can reap leftover children even when GNU timeout has fired.
    payload = ["timeout", "--signal=TERM", "--kill-after=5s", f"{remote_timeout:g}s",
               *bootstrap_command(env_script, command, shell=container_shell if container else "bash",
                                  **({"workdir": container_workdir} if container and container_workdir else {}))]
    report = {"schema_version": "1.1", "operation": "script", "scenario": scenario,
              "env_script": env_script, "script": script, "script_args": list(script_args),
              "execution_scope": "container" if container else "host", "container_name": container_name,
              "container_shell": container_shell, "container_workdir": container_workdir,
              "test_python": test_python, "timeout_seconds": remote_timeout, "interrupted": False,
              "nodes": {}, "cleanup": None, "status": "DRY_RUN" if dry_run else "PASS"}
    run_dir = claim_labeled_run_directory(
        output_dir.resolve(), run_directory_label(scenario, "script"))
    ready = list(nodes)
    if container and not dry_run:
        with tempfile.TemporaryDirectory(prefix="hcu-script-container-") as temp:
            status = run_container_status(nodes=nodes, container_name=str(container_name),
                                          expected_image=image, transport=transport,
                                          concurrency=concurrency, run_dir=Path(temp))
        ready = status["ready_nodes"]
        for issue in status["issues"]:
            if issue["node"] in nodes:
                report["nodes"].setdefault(issue["node"], {"status": "SKIPPED", "issues": []})["issues"].append(issue)
        for node in nodes:
            if node not in ready:
                report["nodes"].setdefault(node, {"status": "SKIPPED", "issues": status["issues"]})
    session = RemoteTaskSession(
        ready or list(nodes),
        BaremetalExecutionConfig(output_root=run_dir / "evidence", transport=transport,
                                 concurrency=concurrency, command_timeout_seconds=remote_timeout + 30),
        execution_scope="container" if container else "host", container_name=container_name,
    )
    report["run_token"] = session.run_token
    remote = managed_command(payload, session.run_token)
    if container:
        remote = ["docker", "exec", *(["--workdir", container_workdir] if container_workdir else []),
                  str(container_name), *remote]
    if dry_run:
        for node in nodes:
            report["nodes"][node] = {"status": "DRY_RUN", "command": remote}
    elif ready:
        try:
            with session:
                execution = BaremetalClusterExecutor(ready, session.config).execute("node-script", remote)
                for node in ready:
                    result = execution.nodes.get(node)
                    _write_node_result(report, run_dir, node, result)
                if any(report["nodes"][node]["status"] != "PASS" for node in ready):
                    session.cancel_and_wait()
        except TaskInterrupted:
            report["interrupted"] = True
        except Exception as exc:
            report["error"] = f"{type(exc).__name__}: {exc}"
        finally:
            for node in ready:
                if node not in report["nodes"]:
                    _write_node_result(report, run_dir, node, None,
                                       cancelled=report["interrupted"], reason=report.get("error"))
            if session.last_report is not None:
                report["cleanup"] = asdict(session.last_report)
                for node, evidence in session.last_report.nodes.items():
                    report["nodes"][node]["cleanup"] = asdict(evidence)
    if not dry_run:
        statuses = {item["status"] for item in report["nodes"].values()}
        report["status"] = "CANCELLED" if report["interrupted"] or "CANCELLED" in statuses else (
            "FAIL" if statuses != {"PASS"} or report.get("error") else "PASS")
        if session.last_report is not None and not session.last_report.confirmed:
            report["status"] = "CLEANUP_UNCONFIRMED"
    (run_dir / "script-result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report, run_dir


def _write_node_result(report, run_dir, node, result, *, cancelled=False, reason=None):
    """Persist both worker evidence and cancellation classification per node."""
    node_dir = run_dir / "nodes" / node
    node_dir.mkdir(parents=True, exist_ok=True)
    (node_dir / "stdout.log").write_text(result.stdout if result else "", encoding="utf-8")
    (node_dir / "stderr.log").write_text(result.stderr if result else reason or "no remote result", encoding="utf-8")
    error_kind = getattr(result, "error_kind", None) if result else "CANCELLED" if cancelled else "REMOTE_RESULT_MISSING"
    report["nodes"][node] = {
        "status": "CANCELLED" if error_kind == "CANCELLED" else "PASS" if result and result.success else "FAIL",
        "returncode": result.returncode if result else 130 if cancelled else 255,
        "error_kind": error_kind, "timed_out": getattr(result, "timed_out", False),
        "stdout": str(node_dir / "stdout.log"), "stderr": str(node_dir / "stderr.log"),
        "evidence_dir": result.result_dir if result else None,
    }
