# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Single supported ``hcu-cluster-run <scenario> <operation>`` interface."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import traceback
from dataclasses import asdict, replace
from collections import Counter
from pathlib import Path

from hcu_envcheck import __version__
from hcu_envcheck.output import claim_labeled_run_directory, run_directory_label
from hcu_envcheck.baremetal_cluster import (
    BaremetalPreflightPolicy,
    build_node_first_result,
    render_baremetal_markdown,
    run_baremetal_cluster_preflight,
)

from .active import ActiveTestExecutor
from .consistency import build_consistency_summary
from .env import validate_local_script
from .hostfile import read_nodes
from .launchers import LAUNCHERS
from .lifecycle import run_container_lifecycle
from .testsuites import default_profile, default_processes
from .node_script import run_node_script
from .preflight import (
    ContainerPreflightError, container_status_excluded_records,
    fold_node_names, grouped_issue_lines, run_container_status,
)


SCENARIOS = ("shared-conda", "node-local-conda", "per-node-container")
BASE_CATEGORIES = {"platform", "resource", "platform,resource"}
ACTIVE_TESTS = {"rccl", "gemm", "ib-write-bw", "custom"}
CONTAINER_OPERATIONS = {"container-status", "container-create", "container-recreate", "container-delete"}
NODE_OPERATIONS = {"script", "nhc"}
OPERATIONS = BASE_CATEGORIES | ACTIVE_TESTS | CONTAINER_OPERATIONS | NODE_OPERATIONS


_COMMON_FLAGS = {"-f", "--hostfile", "--transport", "--concurrency", "--log-detail"}
_BASIC_FLAGS = {"--env-script", "-o", "--output-dir", "--container", "-i", "--image",
                "--container-shell", "--remote-python", "--expected-devices", "--samples",
                "--sample-interval", "--busy-sample-quorum", "--max-vram-used-percent",
                "--max-hcu-util-percent", "--require-rdma", "--minimum-rdma-devices",
                "--expected-rdma-protocol", "--require-compiler", "--require-rccl",
                "--require-ucx", "--package", "--require-python-package", "--container-workdir",
                "--rdma-policy-file", "--rdma-counter-interval", "--ibstat-command", "--timeout"}
_ACTIVE_FLAGS = {"--env-script", "-o", "--output-dir", "--container", "-i", "--image",
                 "--container-shell", "--container-workdir",
                 "--group-size", "--slots", "--strict-size", "--launcher", "--script",
                 "--script-arg", "--nproc-per-node", "--np", "--master-port",
                 "--timeout", "--dry-run", "--skip-idle-check", "--profile", "--test-python", "--container-ssh-port"}
_SCRIPT_FLAGS = {"--env-script", "-o", "--output-dir", "--container", "-i", "--image",
                 "--script", "--script-arg", "--timeout", "--dry-run", "--container-shell", "--container-workdir", "--test-python"}
_EXTRA_COMMON_FLAGS = {"--env-script", "-o", "--output-dir", "--container", "-i", "--image",
                       "--container-workdir", "--timeout", "--dry-run"}
_NHC_FLAGS = {"--nhc-command", "--nhc-installation-source", "--nhc-config", "--nhc-selected", "--nhc-removed", "--nhc-arg"}
_IB_FLAGS = {"--ib-tool", "--ib-protocol", "--ib-device", "--ib-port", "--ib-gid-index", "--ib-control-port",
             "--ib-message-bytes", "--ib-iterations", "--ib-minimum-gbps", "--ib-concurrency", "--ib-max-tests", "--ibstat-command"}
_STATUS_FLAGS = {"--container", "-i", "--image"}
_LIFECYCLE_FLAGS = {"--container", "-i", "--image", "--image-tar", "-v",
                    "--volume", "--docker-arg", "--container-command", "--yes", "--dry-run", "--port"}


def _validate_used_flags(operation: str, argv: list[str]) -> None:
    allowed = _COMMON_FLAGS | (
        _BASIC_FLAGS if operation in BASE_CATEGORIES else
        (_EXTRA_COMMON_FLAGS | _IB_FLAGS | {"--group-size", "--slots", "--strict-size"}) if operation == "ib-write-bw" else
        _ACTIVE_FLAGS if operation in ACTIVE_TESTS else
        (_EXTRA_COMMON_FLAGS | _NHC_FLAGS) if operation == "nhc" else
        _SCRIPT_FLAGS if operation == "script" else
        _STATUS_FLAGS if operation == "container-status" else
        _LIFECYCLE_FLAGS
    )
    allowed |= {"-h", "--help", "--version"}
    if operation == "custom":
        allowed -= {"--launcher", "--profile", "--nproc-per-node", "--np", "--master-port", "--container-ssh-port", "--skip-idle-check"}
    for token in argv:
        if token.startswith("-"):
            option = token.split("=", 1)[0]
            if option not in allowed:
                raise ValueError(f"{option} is not valid for {operation}")


class RunnerArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        self.print_usage(sys.stderr)
        self.exit(3, f"{self.prog}: error: {message}\n")


def build_parser() -> argparse.ArgumentParser:
    parser = RunnerArgumentParser(
        prog="hcu-cluster-run",
        description="Unified HCU platform checks and active test runner",
    )
    parser.add_argument("scenario", choices=SCENARIOS)
    parser.add_argument(
        "operation", choices=sorted(OPERATIONS),
        help="basic checks, active tests, node script, or container lifecycle",
    )
    parser.add_argument("-f", "--hostfile", type=Path, required=True)
    parser.add_argument("--env-script", help="optional env.sh path visible in each target execution scope; omitted uses the current target environment")
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=None,
    )
    parser.add_argument("--container", help="existing container name for container scope")
    parser.add_argument("-i", "--image", help="expected image tag; required for all per-node-container operations except container-delete")
    parser.add_argument("--image-tar", help="node-visible offline Docker image tar for create/recreate")
    parser.add_argument("-v", "--volume", action="append", default=[], help="Docker host:container mount; repeatable")
    parser.add_argument("--docker-arg", action="append", default=[], help="one additional docker run argument; repeatable")
    parser.add_argument("--container-command", help="override image default command during create/recreate")
    parser.add_argument("--port", type=int, help="create/recreate: configure root key-only inter-container SSH on this host-network port")
    parser.add_argument("--yes", action="store_true", help="confirm destructive container-recreate/delete")
    parser.add_argument("--container-shell", choices=("bash", "sh"), default="bash")
    parser.add_argument(
        "--container-workdir",
        help="project root inside the existing container; defaults to the entry package root (must be mounted)",
    )
    parser.add_argument(
        "--transport",
        choices=("ssh", "clush"),
        default="ssh",
        help="remote execution transport (default: ssh)",
    )
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--log-detail", choices=("milestone", "every", "quiet"), default="milestone",
                        help="foreground log granularity for large clusters: milestone folds per-node "
                             "results and prints 10%% progress lines (default); every prints one block "
                             "per node/group; quiet prints only ERROR-level folded summaries")
    parser.add_argument("--remote-python", default="python3")
    parser.add_argument("--test-python", default="python3", help="interpreter resolved AFTER target env.sh (never controller Python)")
    parser.add_argument("--container-ssh-port", type=int, default=25901,
                        help="multi-node container MPI peer SSH port; verified against docker exec namespace/user")
    parser.add_argument("--profile", choices=("worker", "rccl-tests", "rocblas"), default=None,
                        help="rccl defaults to rccl-tests (all 10 collectives, required scale baselines); gemm defaults to worker; worker explicitly selects Python/torchrun")
    parser.add_argument("--rdma-policy-file", type=Path, help="explicit JSON RoCE policy (not environment configuration)")
    parser.add_argument("--rdma-counter-interval", type=float, default=1.0)
    parser.add_argument("--nhc-command", default="run_nhc")
    parser.add_argument("--nhc-installation-source")
    parser.add_argument("--nhc-config")
    parser.add_argument("--nhc-selected")
    parser.add_argument("--nhc-removed")
    parser.add_argument("--nhc-arg", action="append", default=[])
    parser.add_argument("--ibstat-command", default="ibstat", help="platform IB/RDMA state command (default: ibstat)")
    parser.add_argument("--ib-tool", choices=("ib_write_bw", "ib_read_bw", "ib_send_bw"), default="ib_write_bw")
    parser.add_argument("--ib-protocol", choices=("ib", "roce"), default="ib")
    parser.add_argument("--ib-device")
    parser.add_argument("--ib-port", type=int, default=1)
    parser.add_argument("--ib-gid-index", type=int)
    parser.add_argument("--ib-control-port", type=int, default=18515)
    parser.add_argument("--ib-message-bytes", type=int, default=1048576)
    parser.add_argument("--ib-iterations", type=int, default=1000)
    parser.add_argument("--ib-minimum-gbps", type=float)
    parser.add_argument("--ib-concurrency", type=int, default=1)
    parser.add_argument("--ib-max-tests", type=int, default=1024)

    # Basic check options.  These intentionally reuse the existing policy
    # vocabulary, preserving the current HCU/RDMA/software probes.
    parser.add_argument("--expected-devices", type=int)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--sample-interval", type=float, default=1.0)
    parser.add_argument("--busy-sample-quorum", type=int, default=2)
    parser.add_argument("--max-vram-used-percent", type=float, default=5.0)
    parser.add_argument("--max-hcu-util-percent", type=float, default=5.0)
    parser.add_argument("--require-rdma", action="store_true")
    parser.add_argument("--minimum-rdma-devices", type=int, default=0)
    parser.add_argument("--expected-rdma-protocol", choices=("auto", "ib", "roce"), default="auto")
    parser.add_argument("--require-compiler", action="store_true")
    parser.add_argument("--require-rccl", action="store_true")
    parser.add_argument("--require-ucx", action="store_true")
    parser.add_argument(
        "--package",
        "--require-python-package",
        dest="packages",
        action="append",
        default=None,
        help="package to collect from the activated env; repeatable",
    )

    # Active test options.  ``slots`` means concurrent groups, not MPI slots.
    parser.add_argument("--group-size", type=int)
    parser.add_argument("--slots", type=int, default=1, help="maximum concurrent test groups")
    parser.add_argument("--strict-size", action="store_true")
    parser.add_argument("--launcher", choices=LAUNCHERS, default="mpirun-torchrun")
    parser.add_argument("--script", help="target-visible Shell/Python script (required by script/custom)")
    parser.add_argument("--script-arg", action="append", default=[])
    parser.add_argument("--nproc-per-node", type=int, default=None,
                        help="processes per node: default 8 for rccl-tests, 1 for worker")
    parser.add_argument("--np", type=int)
    parser.add_argument("--master-port", type=int, default=29500)
    parser.add_argument("--timeout", type=float, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-idle-check", action="store_true", help="active container tests only; explicitly skip DCU idle precheck")
    parser.add_argument("--version", action="version", version=f"hcu-cluster-run {__version__}")
    return parser


_LOG_COLORS = {
    "INFO": "\033[36m",
    "SUCCESS": "\033[32m",
    "WARN": "\033[33m",
    "ERROR": "\033[31m",
}
_COLOR_RESET = "\033[0m"


def _log(level: str, message: str) -> None:
    """Write a concise levelled log; color only on an interactive terminal."""

    level = level.upper()
    label = f"[{level}]"
    color_enabled = (
        not os.environ.get("NO_COLOR")
        and hasattr(sys.stderr, "isatty")
        and sys.stderr.isatty()
    )
    if color_enabled and level in _LOG_COLORS:
        label = f"{_LOG_COLORS[level]}{label}{_COLOR_RESET}"
    print(f"{label} {message}", file=sys.stderr, flush=True)


class _ProgressLine:
    """O(1) foreground progress for large clusters.

    On a terminal the counter refreshes one line; when stderr is redirected it
    prints a milestone line every 10%%. ``quiet`` stays silent.
    """

    def __init__(self, label: str, total: int, detail: str = "milestone") -> None:
        self.label = label
        self.total = max(total, 1)
        self.detail = detail
        self.done = 0
        self.failed = 0
        self._next_milestone = 0.1
        self._started = False

    def _render(self) -> str:
        suffix = f" ({self.failed} failed)" if self.failed else ""
        return f"{self.label}: {self.done}/{self.total}{suffix}"

    def advance(self, *, failed: bool = False) -> None:
        if self.detail == "quiet":
            return
        self.done += 1
        self.failed += int(failed)
        interactive = hasattr(sys.stderr, "isatty") and sys.stderr.isatty()
        if interactive:
            print(f"\r[INFO] {self._render()}", end="", file=sys.stderr, flush=True)
            self._started = True
        elif self.done >= self.total or self.done >= self.total * self._next_milestone:
            while self.done >= self.total * self._next_milestone:
                self._next_milestone = round(self._next_milestone + 0.1, 10)
            _log("INFO", self._render())

    def finish(self, extra: str = "") -> None:
        if self.detail == "quiet" or not self._started:
            return
        suffix = f" ({self.failed} failed)" if self.failed else ""
        line = f"[INFO] {self.label}: {self.done}/{self.total}{suffix}{extra}"
        print(f"\r{line}{' ' * 8}", file=sys.stderr, flush=True)
        self._started = False


def _log_failure(exc: Exception, args: argparse.Namespace | None = None) -> None:
    """Explain the failed local step without dumping env.sh or remote commands."""

    print("RESULT        TOOL_ERROR", file=sys.stderr)
    _log("ERROR", f"{type(exc).__name__}: {exc}")
    if args is not None:
        _log("ERROR", f"scenario={args.scenario} operation={args.operation} transport={args.transport}")
        _log("ERROR", f"hostfile={args.hostfile} output_dir={args.output_dir}")
    if isinstance(exc, FileNotFoundError):
        if exc.filename:
            _log("ERROR", f"missing_path={exc.filename}")
        else:
            _log("ERROR", "current working directory may have been removed; cd to the uploaded project directory and retry")
    if os.environ.get("HCU_ENVCHECK_DEBUG") == "1":
        traceback.print_exception(type(exc), exc, exc.__traceback__, file=sys.stderr)
    else:
        _log("INFO", "set HCU_ENVCHECK_DEBUG=1 for a Python traceback")


def _stderr_excerpt(result_dir: object, filename: str = "stderr.txt") -> str:
    """Return a bounded remote diagnostic, never the full probe/launcher output."""

    if not result_dir:
        return ""
    path = Path(str(result_dir)) / filename
    try:
        with path.open("r", encoding="utf-8", errors="replace") as stream:
            lines = [line.strip() for line in stream.readlines(4096) if line.strip()]
    except OSError:
        return ""
    return " | ".join(lines[:4])[:800]


def _print_base_result(report: dict, json_path: Path, md_path: Path) -> None:
    summary = report.get("consistency_summary") or {}
    print(f"RESULT        {report.get('status', 'UNKNOWN')}")
    print(
        "NODES         "
        f"total={summary.get('node_count', report.get('summary', {}).get('node_count', 0))} "
        f"passed={summary.get('passed_node_count', 0)} "
        f"failed={summary.get('failed_node_count', 0)} "
        f"incomplete={summary.get('incomplete_node_count', 0)}"
    )
    print(f"JSON          {json_path}")
    print(f"SUMMARY       {md_path}")


def _log_basic_execution(report: dict, args: argparse.Namespace) -> None:
    """Print the small amount of runtime context needed to debug a probe.

    The JSON/Markdown reports remain the source of truth.  ``--log-detail
    every`` keeps one block per node; the default folds nodes with an identical
    status/findings signature into one line so a thousand-node run stays
    readable, and ``quiet`` keeps only ERROR-level folded lines.
    """

    _log(
        "INFO",
        f"remote transport selected={report.get('transport', '-')} "
        f"evidence_dir={report.get('evidence_dir', '-')}"
    )
    detail = getattr(args, "log_detail", "milestone")
    records = report.get("nodes") or []
    if detail == "every":
        for record in records:
            transport = record.get("probe_transport") or {}
            findings = record.get("findings") or []
            first_finding = findings[0] if findings else {}
            detail_text = str(first_finding.get("message") or "").replace("\n", " ").strip()
            if len(detail_text) > 240:
                detail_text = detail_text[:237] + "..."
            status = str(record.get("status") or "UNKNOWN")
            level = "ERROR" if status == "BLOCKED" else (
                "WARN" if status != "READY" or any(item.get("severity") == "WARN" for item in findings)
                else "SUCCESS"
            )
            _log(
                level,
                f"node={record.get('node', '-')} status={record.get('status', '-')} "
                f"reachable={record.get('reachable', '-')} "
                f"returncode={transport.get('returncode', '-')} "
                f"error={transport.get('error_kind') or '-'}"
                + (f" detail={detail_text}" if detail_text else "")
                + f" result_dir={transport.get('result_dir') or '-'}"
            )
            if status != "READY" or record.get("findings"):
                reasons = Counter(str(item.get("reason_code") or "UNKNOWN") for item in findings)
                if reasons:
                    _log(level, f"node={record.get('node', '-')} findings=" + ", ".join(f"{code}×{count}" for code, count in sorted(reasons.items())))
                result_dir = transport.get("result_dir")
                if result_dir:
                    _log("INFO", f"node={record.get('node', '-')} stderr_file={Path(str(result_dir)) / 'stderr.txt'} transport_metadata={Path(str(result_dir)) / 'result.json'}")
                    excerpt = _stderr_excerpt(result_dir)
                    if excerpt:
                        _log(level, f"node={record.get('node', '-')} remote_stderr={excerpt}")
        return

    folded: dict[tuple[str, str, tuple], list[str]] = {}
    for record in records:
        findings = record.get("findings") or []
        status = str(record.get("status") or "UNKNOWN")
        level = "ERROR" if status == "BLOCKED" else (
            "WARN" if status != "READY" or any(item.get("severity") == "WARN" for item in findings)
            else "SUCCESS"
        )
        reasons = tuple(sorted(Counter(
            str(item.get("reason_code") or "UNKNOWN") for item in findings).items()))
        folded.setdefault((level, status, reasons), []).append(str(record.get("node") or "-"))
    for (level, status, reasons), nodes in folded.items():
        if detail == "quiet" and level != "ERROR":
            continue
        line = f"{status} ×{len(nodes)} nodes={fold_node_names(nodes)}"
        if reasons:
            line += " findings=" + ",".join(f"{code}×{count}" for code, count in reasons)
        _log(level, line)


def _check_container_status(args: argparse.Namespace, nodes: list[str]) -> dict:
    """Use disposable transport evidence; status failures are terminal-only."""

    with tempfile.TemporaryDirectory(prefix="hcu-container-status-") as temp:
        return run_container_status(
            nodes=nodes, container_name=args.container,
            expected_image=args.image, transport=args.transport,
            concurrency=args.concurrency, run_dir=Path(temp),
        )


def _log_container_status(report: dict) -> None:
    _log("SUCCESS" if report["status"] == "PASS" else "ERROR",
         f"container status={report['status']} ready={len(report['ready_nodes'])}/{len(report['nodes'])} "
         f"image={report.get('image_reference') or '-'}")
    for line in grouped_issue_lines(report.get("issues") or []):
        _log("ERROR", line)


def _run_container_status(args: argparse.Namespace) -> int:
    if args.scenario != "per-node-container":
        raise ValueError("container-status requires scenario=per-node-container")
    if not args.container or not args.image:
        raise ValueError("container-status requires --container and -i/--image")
    if args.concurrency < 1:
        raise ValueError("--concurrency must be positive")
    nodes = read_nodes(args.hostfile)
    _log("INFO", f"container status start: nodes={len(nodes)} container={args.container} image={args.image} transport={args.transport}")
    report = _check_container_status(args, nodes)
    _log_container_status(report)
    print(f"RESULT        {report['status']}")
    print(f"NODES         total={len(nodes)} ready={len(report['ready_nodes'])} failed={len(nodes) - len(report['ready_nodes'])}")
    return 0 if report["status"] == "PASS" else 2


def _run_lifecycle(args: argparse.Namespace) -> int:
    if args.scenario != "per-node-container":
        raise ValueError(f"{args.operation} requires scenario=per-node-container")
    if not args.container:
        raise ValueError(f"{args.operation} requires --container")
    nodes = read_nodes(args.hostfile)
    _log("WARN" if args.operation != "container-create" else "INFO",
         f"{args.operation}: nodes={len(nodes)} container={args.container} image={args.image or '-'}")
    report = run_container_lifecycle(
        operation=args.operation, nodes=nodes, container_name=args.container,
        image=args.image, image_tar=args.image_tar, volumes=args.volume,
        docker_args=args.docker_arg, container_command=args.container_command,
        transport=args.transport, concurrency=args.concurrency,
        confirm=args.yes, dry_run=args.dry_run, port=args.port, log=_log,
    )
    for line in grouped_issue_lines(report["issues"]):
        _log("ERROR", line)
    _log("SUCCESS" if report["status"] in {"PASS", "DRY_RUN"} else "ERROR",
         f"{args.operation} result={report['status']}")
    print(f"RESULT        {report['status']}")
    print(f"NODES         total={len(nodes)}")
    if args.port is not None:
        print(f"SSH           user=root port={args.port} verification={report.get('ssh_verification', 'NOT_VERIFIED')}")
        if report["status"] == "PASS":
            _log("INFO", f"inside a configured container: ssh {nodes[0]} -p {args.port}; MPI use --container-ssh-port {args.port}")
        elif report["status"] == "FAIL" and report.get("containers_changed"):
            _log("WARN", "containers may be partially created; no automatic deletion/rollback; inspect named containers and SSH logs")
    return 0 if report["status"] in {"PASS", "DRY_RUN"} else 2


def _run_script(args: argparse.Namespace) -> int:
    if not args.script:
        raise ValueError("script requires --script")
    if args.scenario == "per-node-container" and (not args.container or not args.image):
        raise ValueError("per-node-container script requires --container and -i/--image")
    nodes = read_nodes(args.hostfile)
    report, run_dir = run_node_script(
        nodes=nodes, scenario=args.scenario, env_script=args.env_script,
        script=args.script, script_args=args.script_arg,
        container_name=args.container, image=args.image,
        transport=args.transport, concurrency=args.concurrency,
        timeout=args.timeout, output_dir=args.output_dir, dry_run=args.dry_run,
        container_shell=args.container_shell, container_workdir=args.container_workdir,
        test_python=args.test_python,
    )
    for node, result in report["nodes"].items():
        _log("SUCCESS" if result["status"] == "PASS" else "WARN" if result["status"] == "DRY_RUN" else "ERROR",
             f"node={node} status={result['status']} stderr={result.get('stderr') or '-'}")
    print(f"RESULT        {report['status']}")
    print(f"RUN_DIR       {run_dir}")
    print(f"JSON          {run_dir / 'script-result.json'}")
    return _execution_exit(report, args)


def _execution_exit(report: dict, args: argparse.Namespace | None = None) -> int:
    cleanup = report.get("cleanup")
    if cleanup:
        nodes = cleanup.get("nodes", {})
        confirmed = [node for node, item in nodes.items() if item["status"] == "CONFIRMED"]
        unconfirmed = [node for node, item in nodes.items() if item["status"] != "CONFIRMED"]
        if getattr(args, "log_detail", "milestone") == "every":
            for node, item in nodes.items():
                _log("SUCCESS" if item["status"] == "CONFIRMED" else "ERROR",
                     f"cleanup node={node} status={item['status']} reason={item.get('reason', '')}")
        else:
            if confirmed:
                _log("SUCCESS", f"cleanup confirmed {len(confirmed)}/{len(nodes)} nodes")
            if unconfirmed:
                _log("ERROR", f"cleanup unconfirmed ×{len(unconfirmed)} nodes={fold_node_names(unconfirmed)} (details in cleanup.json)")
    if report.get("status") == "CLEANUP_UNCONFIRMED":
        return 3
    if report.get("interrupted") or report.get("status") == "CANCELLED":
        return 130
    return 0 if report["status"] in {"PASS", "DRY_RUN", "INCOMPLETE"} else 2


def _run_extra(args: argparse.Namespace) -> int:
    from hcu_envcheck.baremetal import BaremetalExecutionConfig
    from hcu_envcheck.cluster_checks import NHCCheckConfig
    from .extra_checks import run_nhc
    from .task_control import RemoteTaskSession, TaskInterrupted
    nodes = read_nodes(args.hostfile)
    scope = "container" if args.scenario == "per-node-container" else "host"
    if scope == "container" and (not args.container or not args.image):
        raise ValueError("per-node-container requires --container and -i/--image")
    config = NHCCheckConfig(enabled=True, command=(args.nhc_command,),
                  installation_source=args.nhc_installation_source, config=args.nhc_config,
                  selected=args.nhc_selected, removed=args.nhc_removed,
                  extra_args=tuple(args.nhc_arg), timeout_seconds=args.timeout or 600)
    config.validate()
    if scope == "container" and not args.dry_run:
        status = _check_container_status(args, nodes)
        _log_container_status(status)
        if status["status"] != "PASS":
            print("RESULT        PRECHECK_FAILED")
            return 3
    root = claim_labeled_run_directory(
        args.output_dir, run_directory_label(args.scenario, args.operation))
    session = RemoteTaskSession(nodes, BaremetalExecutionConfig(output_root=root / "evidence",
        transport=args.transport, concurrency=args.concurrency), execution_scope=scope, container_name=args.container)
    report = None
    try:
        with session:
            report = run_nhc(nodes, config=config,
                env_script=args.env_script, scope=scope, container=args.container,
                container_workdir=args.container_workdir, output_dir=root,
                execution_config=session.config, run_token=session.run_token,
                cancel_event=session.cancel_event, dry_run=args.dry_run)
    except TaskInterrupted:
        report = report or {"operation": args.operation, "nodes": nodes}
        report.update({"status": "CANCELLED", "interrupted": True})
    if session.last_report:
        report["cleanup"] = asdict(session.last_report)
        if not session.last_report.confirmed:
            report["status"] = "CLEANUP_UNCONFIRMED"
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{args.operation}-result.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"RESULT        {report['status']}\nJSON          {path}")
    return _execution_exit(report, args)


def _run_basic(args: argparse.Namespace, operation: str) -> int:
    scope = "container" if args.scenario == "per-node-container" else "host"
    if args.scenario == "per-node-container" and (not args.container or not args.image):
        raise ValueError("per-node-container requires --container and -i/--image")
    if args.concurrency < 1:
        raise ValueError("--concurrency must be positive")
    if args.samples < 1:
        raise ValueError("--samples must be at least 1")
    if not 1 <= args.busy_sample_quorum <= args.samples:
        raise ValueError("--busy-sample-quorum must be between 1 and --samples")
    if args.minimum_rdma_devices < 0:
        raise ValueError("--minimum-rdma-devices cannot be negative")
    if args.sample_interval < 0:
        raise ValueError("--sample-interval cannot be negative")
    if args.env_script and args.env_script.startswith("~"):
        raise ValueError("--env-script must be an absolute path visible on target nodes")
    if args.env_script and Path(args.env_script).exists():
        validate_local_script(args.env_script)

    _log("INFO", f"reading hostfile={args.hostfile}")
    nodes = read_nodes(args.hostfile)
    categories = operation.split(",")
    if any(category not in {"platform", "resource"} for category in categories):
        raise ValueError("basic categories must be platform, resource, or platform,resource")
    if len(set(categories)) != len(categories):
        raise ValueError("basic categories must not be repeated")

    container_status = None
    excluded_records = None
    if scope == "container":
        _log("INFO", f"checking container status before {operation}: container={args.container}")
        container_status = _check_container_status(args, nodes)
        _log_container_status(container_status)
        excluded_records = container_status_excluded_records(container_status)
        if excluded_records:
            _log("WARN", f"skipping environment probes on {len(excluded_records)} container-unready nodes; healthy nodes will still be checked")

    output_root = args.output_dir
    _log("INFO", f"preparing output_dir={output_root}")
    output_root.mkdir(parents=True, exist_ok=True)
    _log(
        "INFO",
        f"basic check start: scenario={args.scenario} categories={operation} "
        f"nodes={len(nodes)} transport={args.transport} concurrency={args.concurrency}",
    )
    _log(
        "INFO",
        f"execution scope={scope} container={args.container or '-'} "
        f"env_script={args.env_script or '<current-target-environment>'}",
    )
    _log("INFO", "dispatching node probes")
    from hcu_envcheck.cluster_checks import ClusterExtraCheckConfig, IBStateCheckConfig
    from hcu_envcheck.rdma_policy import load_roce_policy
    policy = BaremetalPreflightPolicy(
        expected_devices=args.expected_devices,
        max_vram_used_percent=args.max_vram_used_percent,
        max_hcu_util_percent=args.max_hcu_util_percent,
        samples=args.samples,
        busy_sample_quorum=args.busy_sample_quorum,
        sample_interval_seconds=args.sample_interval,
        software_mode="host-python",
        required_python_packages=tuple(args.packages or ()),
        require_compiler=args.require_compiler,
        require_rdma=args.require_rdma,
        minimum_rdma_devices=args.minimum_rdma_devices,
        expected_rdma_protocol=args.expected_rdma_protocol,
        require_rccl=args.require_rccl,
        require_ucx=args.require_ucx,
        env_script=args.env_script,
        execution_scope=scope,
        container_name=args.container,
        container_shell=args.container_shell,
        container_workdir=args.container_workdir,
        check_categories=tuple(categories),
        rdma_policy=load_roce_policy(args.rdma_policy_file) if args.rdma_policy_file else None,
        rdma_counter_interval_seconds=args.rdma_counter_interval,
    )
    from hcu_envcheck.baremetal import BaremetalExecutionConfig

    execution = BaremetalExecutionConfig(
        output_root=output_root / "evidence",
        transport=args.transport,
        concurrency=args.concurrency,
        command_timeout_seconds=240,
    )
    run_options = {"excluded_records": excluded_records} if excluded_records else {}
    from .env import bootstrap_command
    from .task_control import RemoteTaskSession, TaskInterrupted, managed_command
    session = RemoteTaskSession(nodes, execution, execution_scope=scope, container_name=args.container)

    def wrap_platform_check(command, timeout_seconds):
        inner = managed_command(
            ["timeout", "--signal=TERM", "--kill-after=5s", f"{timeout_seconds:g}s",
             *bootstrap_command(args.env_script, command, shell=args.container_shell,
                                workdir=args.container_workdir)],
            session.run_token,
        )
        if scope == "container":
            return ["docker", "exec",
                    *(["--workdir", args.container_workdir] if args.container_workdir else []),
                    str(args.container), *inner]
        return inner

    platform_checks = ClusterExtraCheckConfig(
        ib_state=IBStateCheckConfig(
            enabled="platform" in categories,
            command=(args.ibstat_command,),
            timeout_seconds=args.timeout or 30,
        )
    )
    execution_failed_nodes = []
    probe_progress = _ProgressLine("probes", len(nodes), getattr(args, "log_detail", "milestone"))
    try:
        with session:
            report, json_path, md_path = run_baremetal_cluster_preflight(
                nodes=nodes, execution_config=session.config, policy=replace(policy, run_token=session.run_token),
                output_dir=output_root, run_label=run_directory_label(args.scenario, operation),
                remote_python=args.remote_python,
                progress=lambda _node, status: probe_progress.advance(failed=status == "INCOMPLETE"),
                extra_checks=platform_checks,
                extra_command_wrapper=wrap_platform_check if "platform" in categories else None,
                **run_options,
            )
            probe_progress.finish()
            records_by_node = {record.get("node"): record for record in report.get("nodes", [])}
            for node in nodes:
                # Rejected containers were deliberately not probed. Their
                # status remains a health finding, not a failed execution.
                if node in (excluded_records or {}):
                    continue
                transport = (records_by_node.get(node, {}).get("probe_transport") or {})
                # Missing records/return codes cannot prove successful execution.
                # Health BLOCKED/INCOMPLETE with a successful transport is valid.
                if (transport.get("returncode") != 0 or transport.get("success") is False
                        or transport.get("timed_out") or transport.get("error_kind")):
                    execution_failed_nodes.append(node)
            if execution_failed_nodes:
                report["cleanup"] = asdict(session.cancel_and_wait())
    except TaskInterrupted:
        cancelled = {"status": "CANCELLED" if session.last_report and session.last_report.confirmed else "CLEANUP_UNCONFIRMED",
                     "interrupted": True, "cleanup": asdict(session.last_report) if session.last_report else None}
        path = output_root / f"cancellation-{session.run_token}.json"
        path.write_text(json.dumps(cancelled, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"RESULT        {cancelled['status']}\nJSON          {path}")
        return _execution_exit(cancelled, args)
    if container_status is not None:
        report["container_status"] = {
            "status": container_status["status"],
            "image_reference": container_status.get("image_reference"),
            "expected_image_specified": container_status["expected_image_specified"],
            "ready_nodes": container_status["ready_nodes"],
            "issues": container_status["issues"],
        }
        for record in report["nodes"]:
            item = container_status["nodes"][record["node"]]
            record["container_status"] = {key: value for key, value in item.items() if key != "evidence_dir"}
    _log("INFO", f"node probes completed: records={len(report.get('nodes') or [])} writing reports")
    report["execution"] = {
        "status": "FAIL" if execution_failed_nodes else "PASS",
        "failed_nodes": execution_failed_nodes,
        "scenario": args.scenario,
        "scope": scope,
        "categories": categories,
        "env_script": args.env_script,
        "hostfile": str(args.hostfile),
    }
    report["consistency_summary"] = build_consistency_summary(
        report.get("nodes") or [],
        scope="platform-and-resource" if set(categories) == {"platform", "resource"} else f"{categories[0]}-only",
    )
    json_path.write_text(
        json.dumps(build_node_first_result(report), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    md_path.write_text(render_baremetal_markdown(report), encoding="utf-8")
    _log_basic_execution(report, args)
    _log("INFO", f"reports written: json={json_path} summary={md_path}")
    overall = str(report.get("status") or "UNKNOWN")
    _log("SUCCESS" if overall == "READY" else ("ERROR" if overall == "BLOCKED" else "WARN"), f"basic check result={overall}; reports remain available for user review")
    _print_base_result(report, json_path, md_path)
    if session.last_report:
        cleanup_result = {"status": ("CLEANUP_UNCONFIRMED" if not session.last_report.confirmed
                                     else "FAIL" if execution_failed_nodes else "PASS"),
                          "execution_failed_nodes": execution_failed_nodes,
                          "cleanup": asdict(session.last_report)}
        cleanup_path = json_path.parent / "cleanup.json"
        cleanup_path.write_text(json.dumps(cleanup_result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return _execution_exit(cleanup_result, args)
    return 2 if execution_failed_nodes else 0


def _run_active(args: argparse.Namespace, operation: str) -> int:
    scope = "container" if args.scenario == "per-node-container" else "host"
    if args.scenario == "per-node-container" and (not args.container or not args.image):
        raise ValueError("per-node-container active tests require --container and -i/--image")
    if args.group_size is not None and args.group_size < 1:
        raise ValueError("--group-size must be at least 1")
    if args.concurrency < 1:
        raise ValueError("--concurrency must be positive")
    from hcu_envcheck.cluster_checks import IBWriteBandwidthConfig, IBStateCheckConfig
    ib_config = None
    if operation == "ib-write-bw":
        ib_config = IBWriteBandwidthConfig(enabled=True, tool=args.ib_tool, protocol=args.ib_protocol,
            device=args.ib_device, ib_port=args.ib_port, gid_index=args.ib_gid_index,
            control_port=args.ib_control_port, message_bytes=args.ib_message_bytes, iterations=args.ib_iterations,
            minimum_average_gbps=args.ib_minimum_gbps, concurrency=args.ib_concurrency,
            max_tests=args.ib_max_tests, timeout_seconds=args.timeout or 120)
        ib_config.validate()
    _log("INFO", f"reading hostfile={args.hostfile}")
    nodes = read_nodes(args.hostfile)
    _log(
        "INFO",
        f"active test start: scenario={args.scenario} test={operation} "
        f"nodes={len(nodes)} profile={args.profile} launcher={args.launcher if args.profile == 'worker' else args.profile} transport={args.transport} "
        f"group_size={args.group_size or len(nodes)} slots={args.slots} dry_run={args.dry_run}",
    )
    _log(
        "INFO",
        f"execution scope={scope} container={args.container or '-'} env_script={args.env_script or '<current-target-environment>'}",
    )
    if scope == "container" and (args.profile == "rccl-tests" or args.launcher in {"mpirun", "mpirun-torchrun"}):
        _log("INFO", "container MPI policy automatically adds --allow-run-as-root when the selected container user is root")
    executor = ActiveTestExecutor(
        scenario=args.scenario,
        test_name=operation,
        nodes=nodes,
        env_script=args.env_script,
        output_dir=args.output_dir,
        group_size=args.group_size,
        strict_size=args.strict_size,
        slots=args.slots,
        launcher=args.launcher,
        script=args.script,
        script_args=args.script_arg,
        nproc_per_node=args.nproc_per_node,
        np=args.np,
        master_port=args.master_port,
        timeout_seconds=args.timeout,
        transport=args.transport,
        concurrency=args.concurrency,
        dry_run=args.dry_run,
        execution_scope=scope,
        container_name=args.container,
        container_shell=args.container_shell,
        container_workdir=args.container_workdir,
        expected_image=args.image,
        check_idle=not args.skip_idle_check,
        profile=args.profile, test_python=args.test_python, container_ssh_port=args.container_ssh_port,
        ib_config=ib_config, ib_state_config=IBStateCheckConfig(enabled=True, command=(args.ibstat_command,)),
    )
    group_progress_line = _ProgressLine(
        "groups", len(nodes) // max(args.group_size or len(nodes), 1), getattr(args, "log_detail", "milestone"))

    def _on_group_done(status: str) -> None:
        group_progress_line.advance(failed=status not in {"PASS", "DRY_RUN"})

    try:
        report, run_dir = executor.run(group_progress=_on_group_done)
        group_progress_line.finish()
    except ContainerPreflightError as exc:
        if exc.report.get("interrupted"):
            print(f"RESULT        {exc.report['status']}")
            return _execution_exit(exc.report, args)
        _log("ERROR", "container preflight failed; no active group was launched and no preflight report was written")
        for line in grouped_issue_lines(exc.report.get("issues") or []):
            _log("ERROR", line)
        if any(
            issue.get("code") in {"CONTAINER_MISSING", "CONTAINER_STOPPED", "CONTAINER_NAME_MISMATCH",
                                   "CONTAINER_IMAGE_MISMATCH", "CONTAINER_IMAGE_INCONSISTENT", "IMAGE_NOT_LOCAL"}
            for issue in exc.report.get("issues") or []
        ) and exc.report.get("recreate_command_template"):
            _log("WARN", "all-node recreate is destructive and not run automatically; template: " + str(exc.report["recreate_command_template"]))
            _log("WARN", str(exc.report.get("offline_image_hint") or ""))
        print("RESULT        PRECHECK_FAILED")
        if exc.report.get("cleanup"):
            _execution_exit(exc.report, args)
        return 3
    preflight = report.get("preflight") or {}
    if preflight:
        _log("SUCCESS", f"container preflight passed: nodes={preflight.get('node_count')} image={preflight.get('image_reference') or '-'}")
    detail = getattr(args, "log_detail", "milestone")
    for group in report.get("groups", []):
        status = str(group.get("status") or "UNKNOWN")
        level = "SUCCESS" if status in {"PASS", "DRY_RUN"} else "WARN" if status == "INCOMPLETE" else "ERROR"
        if level != "ERROR" and detail != "every":
            # Thousand-node runs produce hundreds of groups; healthy groups are
            # counted in the RESULT block instead of one line each.
            continue
        _log(
            level,
            f"group={group.get('group', '-')} status={status} "
            f"nodes={fold_node_names(group.get('nodes') or [])}",
        )
        if status == "FAIL":
            group_dir = run_dir / "groups" / str(group.get("group"))
            node_results = group.get("node_results") or []
            if node_results:
                for item in node_results:
                    if item.get("status") == "PASS":
                        continue
                    node = str(item.get("node") or "-")
                    stderr_path = group_dir / "nodes" / node / "stderr.log"
                    _log("ERROR", f"group={group.get('group')} node={node} returncode={item.get('returncode')} stderr_file={stderr_path} evidence={item.get('result_dir') or '-'}")
                    excerpt = _stderr_excerpt(stderr_path.parent, "stderr.log")
                    if excerpt and detail != "quiet":
                        _log("ERROR", f"group={group.get('group')} node={node} remote_stderr={excerpt}")
            else:
                _log("ERROR", f"group={group.get('group')} leader={group.get('leader') or '-'} returncode={group.get('returncode')} stderr_file={group_dir / 'stderr.log'} evidence={group.get('result_dir') or '-'}")
                excerpt = _stderr_excerpt(group_dir, "stderr.log")
                if excerpt and detail != "quiet":
                    _log("ERROR", f"group={group.get('group')} remote_stderr={excerpt}")
    _log("SUCCESS" if report["status"] in {"PASS", "DRY_RUN"} else "WARN" if report["status"] == "INCOMPLETE" else "ERROR", f"active report written: {run_dir / 'active-result.json'}")
    print(f"RESULT        {report['status']}")
    if report["status"] == "PRECHECK_FAILED":
        print(f"GROUPS        planned={report['group_count']} executed=0 launcher={report['launcher']}")
    else:
        print(
            f"GROUPS        total={report['group_count']} "
            f"slots={report['group_slots']} launcher={report['launcher']}"
        )
    print(f"RUN_DIR       {run_dir}")
    print(f"JSON          {run_dir / 'active-result.json'}")
    # Nonzero signals execution/cleanup failure, never gates a subsequent command.
    return _execution_exit(report, args)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.profile = args.profile or default_profile(args.operation)
        if args.nproc_per_node is None:
            args.nproc_per_node = default_processes(args.profile)
        _validate_used_flags(args.operation, list(argv) if argv is not None else sys.argv[1:])
        used = {item.split("=", 1)[0] for item in (argv if argv is not None else sys.argv[1:]) if item.startswith("-")}
        invalid = used & {"--launcher", "--np", "--master-port"}
        if args.profile == "rccl-tests" and args.launcher == "mpirun":
            invalid -= {"--launcher"}
        if args.profile != "worker" and invalid:
            raise ValueError("binary benchmarks own their launch; use --profile worker for Python/torchrun/--np, or --nproc-per-node for rccl-tests")
        if args.profile == "rocblas" and "--nproc-per-node" in used:
            raise ValueError("rocblas enumerates local cards; use --script-arg=--cards --script-arg=0,1")
        try:
            current_dir = Path.cwd()
        except OSError as exc:
            raise FileNotFoundError("current working directory is unavailable; cd to the uploaded project directory") from exc
        if args.output_dir is None:
            args.output_dir = current_dir / "cluster_run_results"
        else:
            args.output_dir = args.output_dir.resolve()
        if args.concurrency < 1:
            raise ValueError("--concurrency must be positive")
        if args.env_script and not args.env_script.startswith("/"):
            raise ValueError("--env-script must be an absolute path visible in each target execution scope")
        if args.container_workdir and (args.scenario != "per-node-container" or not args.container_workdir.startswith("/")):
            raise ValueError("--container-workdir requires an absolute container-visible path and per-node-container")
        if args.scenario != "per-node-container" and (args.container or args.image):
            raise ValueError("--container/--image require scenario=per-node-container")
        if args.skip_idle_check and args.scenario != "per-node-container":
            raise ValueError("--skip-idle-check applies only to container active tests")
        lifecycle_only = {"--image-tar": args.image_tar, "--volume": args.volume,
                          "--docker-arg": args.docker_arg, "--container-command": args.container_command,
                          "--yes": args.yes}
        if args.operation not in CONTAINER_OPERATIONS - {"container-status"}:
            for flag, value in lifecycle_only.items():
                if value:
                    raise ValueError(f"{flag} is only valid for container-create/recreate/delete")
        if args.operation not in ACTIVE_TESTS and args.skip_idle_check:
            raise ValueError("--skip-idle-check is only valid for active tests")
        if args.operation != "script" and args.operation not in ACTIVE_TESTS and args.script:
            raise ValueError("--script is only valid for script/custom/active operations")
        if args.operation in BASE_CATEGORIES:
            return _run_basic(args, args.operation)
        if args.operation == "container-status":
            return _run_container_status(args)
        if args.operation in CONTAINER_OPERATIONS:
            return _run_lifecycle(args)
        if args.operation == "nhc":
            return _run_extra(args)
        if args.operation == "script":
            return _run_script(args)
        if args.operation in ACTIVE_TESTS:
            return _run_active(args, args.operation)
        raise ValueError("unsupported operation")
    except (OSError, RuntimeError, ValueError) as exc:
        _log_failure(exc, args)
        return 3


def entrypoint() -> int:
    try:
        return main()
    except KeyboardInterrupt:
        print("RESULT        TOOL_ERROR", file=sys.stderr)
        _log("ERROR", "interrupted by user")
        return 130
