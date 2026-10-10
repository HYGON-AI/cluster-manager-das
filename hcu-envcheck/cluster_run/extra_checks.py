# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Execution adapters for existing NHC, IB state and paired IB tests.

Platform uses the IB-state adapter internally; NHC remains a public operation.
Both run once per node and never launch a bandwidth workload.
``run_group_ib`` accepts ONE already-selected group: it reuses cluster_checks'
IB inventory, directed per-HCA plan, server/client execution and parsers. The
caller owns grouping/slots and the RemoteTaskSession for all selected nodes.
Pass session.config, session.run_token and session.cancel_event to these
functions while session.signal_handlers() is installed. Managed processes
live inside the selected host/container scope so session cleanup can find them.

Each invocation requires its own output_dir. No remote Python, MPI or torchrun
is used by these adapters. env.sh and commands must be visible in target scope.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import uuid
from dataclasses import asdict, replace
from pathlib import Path
from threading import Event
from typing import Callable, Sequence

from hcu_envcheck.baremetal import BaremetalClusterExecutor, BaremetalExecutionConfig
from hcu_envcheck.cluster_checks import (
    ClusterExtraCheckConfig,
    IBStateCheckConfig,
    IBWriteBandwidthConfig,
    NHCCheckConfig,
    run_cluster_extra_checks,
)
from hcu_envcheck.output import atomic_write_text_exclusive

from .env import bootstrap_command
from .task_control import managed_command


def _target_text(value: str | Path, label: str) -> str:
    text = str(value)
    if not text.strip() or any(char in text for char in "\0\r\n"):
        raise ValueError(f"{label} must be nonempty and contain no control characters")
    return text


def _run(
    nodes: Sequence[str], *, operation: str, config: ClusterExtraCheckConfig,
    env_script: str | Path | None, scope: str, container: str | None,
    container_workdir: str | None, transport: str, concurrency: int,
    output_dir: str | Path, execution_config: BaremetalExecutionConfig | None,
    run_token: str | None, cancel_event: Event | None,
    runner: Callable, which: Callable[[str], str | None] | None,
    dry_run: bool,
) -> dict:
    if scope not in {"host", "container"}:
        raise ValueError("scope must be host or container")
    if scope == "container" and (
        not container or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", container)
    ):
        raise ValueError("container scope requires a safe container name")
    if isinstance(nodes, (str, bytes)):
        raise ValueError("nodes must be a sequence of node names, not a string")
    env_script = _target_text(env_script, "env_script") if env_script is not None else None
    if container_workdir is not None:
        container_workdir = _target_text(container_workdir, "container_workdir")
        if not container_workdir.startswith("/"):
            raise ValueError("container_workdir must be an absolute target path")
        if scope != "container":
            raise ValueError("container_workdir requires container scope")
    config.validate()
    root = Path(output_dir).resolve()
    # An explicit execution_config carries SSH identity/ports and the session's
    # cancellation event. Defaults apply only when the caller has no config.
    execution = execution_config or BaremetalExecutionConfig(
        output_root=root / "evidence", transport=transport, concurrency=concurrency)
    event = cancel_event if cancel_event is not None else execution.cancel_event
    execution = replace(execution, output_root=root / "evidence", cancel_event=event)
    nodes = BaremetalClusterExecutor(nodes, execution).nodes
    token = run_token or uuid.uuid4().hex

    def wrap(command: Sequence[str], timeout_seconds: float) -> list[str]:
        # The guard encloses both source and payload, and runs INSIDE Docker.
        # timeout bounds remote lifetime even if the control connection fails.
        inner = managed_command(
            ["timeout", "--signal=TERM", "--kill-after=5s", f"{timeout_seconds:g}s",
             *bootstrap_command(env_script, command, workdir=container_workdir)], token)
        if scope == "container":
            return ["docker", "exec", *(["--workdir", container_workdir] if container_workdir else []),
                    str(container), *inner]
        return inner

    root.mkdir(parents=True, exist_ok=True)
    report = {
        "schema_version": "1.0", "operation": operation, "run_token": token,
        "execution_scope": scope, "container_name": container,
        "container_workdir": container_workdir, "env_script": env_script,
        "requested_transport": execution.transport, "concurrency": execution.concurrency,
        "evidence_dir": str(root / "evidence"), "selected_nodes": nodes,
    }
    if event is not None and event.is_set():
        report.update(status="CANCELLED", returncode=130, reason_code="TASK_CANCELLED",
                      nodes=[{"node": node, "status": "CANCELLED"} for node in nodes])
    elif dry_run:
        cfg = config.nhc if operation == "nhc" else config.ib_state
        command = cfg.argv() if operation == "nhc" else list(cfg.command)
        if operation == "nhc" and cfg.environment:
            command = ["env", *(f"{name}={value}" for name, value in sorted(cfg.environment.items())), *command]
        report.update(status="DRY_RUN", returncode=0, command=wrap(command, cfg.timeout_seconds),
                      nodes=[{"node": node, "status": "DRY_RUN"} for node in nodes])
        if operation == "ib-write-bw":
            report.update(transport="ssh", inventory_transport=execution.transport,
                          config=asdict(config.ib), pairing="all-directions-per-discovered-HCA",
                          pairs=[], message="Inventory and server/client pairs run only without --dry-run")
    else:
        records = [{"node": node, "status": "READY", "findings": [], "checks": []} for node in nodes]
        result = run_cluster_extra_checks(
            nodes=nodes, records=records, execution_config=execution, config=config,
            output_root=root / "evidence", runner=runner, which=which or shutil.which,
            command_wrapper=wrap, cancel_event=event,
        )
        key = {"nhc": "nhc", "ib-state": "ib_state", "ib-write-bw": "ib_write_bw"}[operation]
        check = result[key]
        report.update(check)
        report["check_status"] = check["status"]
        report["status"] = "INCOMPLETE" if check["status"] == "NOT_VERIFIED" else check["status"]
        if operation == "ib-write-bw":
            report.update(nodes=records, ib_state=result["ib_state"], config=asdict(config.ib),
                          transport="ssh", inventory_transport=result["ib_state"].get("transport"))
        if event is not None and event.is_set():
            report.update(status="CANCELLED", reason_code="TASK_CANCELLED")
        report["returncode"] = {"PASS": 0, "FAIL": 1, "INCOMPLETE": 2, "CANCELLED": 130}.get(report["status"], 2)
    report_path = root / f"{operation}-result.json"
    report["report_path"] = str(report_path)
    atomic_write_text_exclusive(report_path, json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    return report


def run_nhc(
    nodes: Sequence[str], *, config: NHCCheckConfig | None = None,
    env_script: str | Path | None, scope: str = "host", container: str | None = None,
    container_workdir: str | None = None, transport: str = "ssh", concurrency: int = 16,
    output_dir: str | Path, execution_config: BaremetalExecutionConfig | None = None,
    run_token: str | None = None, cancel_event: Event | None = None,
    runner: Callable = subprocess.run, which: Callable[[str], str | None] | None = None,
    dry_run: bool = False,
) -> dict:
    """Run real NHC per node, preserving command/config/selected/removed/env args."""
    return _run(nodes, operation="nhc", config=ClusterExtraCheckConfig(
        nhc=replace(config or NHCCheckConfig(), enabled=True)), env_script=env_script,
        scope=scope, container=container, container_workdir=container_workdir,
        transport=transport, concurrency=concurrency, output_dir=output_dir,
        execution_config=execution_config, run_token=run_token, cancel_event=cancel_event,
        runner=runner, which=which, dry_run=dry_run)


def run_ib_state(
    nodes: Sequence[str], *, config: IBStateCheckConfig | None = None,
    env_script: str | Path | None, scope: str = "host", container: str | None = None,
    container_workdir: str | None = None, transport: str = "ssh", concurrency: int = 16,
    output_dir: str | Path, execution_config: BaremetalExecutionConfig | None = None,
    run_token: str | None = None, cancel_event: Event | None = None,
    runner: Callable = subprocess.run, which: Callable[[str], str | None] | None = None,
    dry_run: bool = False,
) -> dict:
    """Read ibstat state per node; this function never starts perftest traffic."""
    return _run(nodes, operation="ib-state", config=ClusterExtraCheckConfig(
        ib_state=replace(config or IBStateCheckConfig(), enabled=True)), env_script=env_script,
        scope=scope, container=container, container_workdir=container_workdir,
        transport=transport, concurrency=concurrency, output_dir=output_dir,
        execution_config=execution_config, run_token=run_token, cancel_event=cancel_event,
        runner=runner, which=which, dry_run=dry_run)


def run_group_ib(
    nodes: Sequence[str], *, config: IBWriteBandwidthConfig | None = None,
    ib_state_config: IBStateCheckConfig | None = None,
    env_script: str | Path | None, scope: str = "host", container: str | None = None,
    container_workdir: str | None = None, transport: str = "ssh", concurrency: int = 16,
    output_dir: str | Path, execution_config: BaremetalExecutionConfig | None = None,
    run_token: str | None = None, cancel_event: Event | None = None,
    runner: Callable = subprocess.run, which: Callable[[str], str | None] | None = None,
    dry_run: bool = False,
) -> dict:
    """Run existing bidirectional, per-HCA server/client algorithm for ONE group.

    config.concurrency limits simultaneous pairs; concurrency limits inventory
    SSH/clush fan-out. Group concurrency (slots) belongs to the caller. Pair
    control ports are unique within this group; disjoint groups can reuse them.
    Return status is PASS/FAIL/INCOMPLETE/CANCELLED/DRY_RUN with returncode
    0/1/2/130/0. Legacy check_status, individual pairs and raw evidence remain.
    """
    return _run(nodes, operation="ib-write-bw", config=ClusterExtraCheckConfig(
        ib_state=replace(ib_state_config or IBStateCheckConfig(), enabled=True),
        ib=replace(config or IBWriteBandwidthConfig(), enabled=True)), env_script=env_script,
        scope=scope, container=container, container_workdir=container_workdir,
        transport=transport, concurrency=concurrency, output_dir=output_dir,
        execution_config=execution_config, run_token=run_token, cancel_event=cancel_event,
        runner=runner, which=which, dry_run=dry_run)
