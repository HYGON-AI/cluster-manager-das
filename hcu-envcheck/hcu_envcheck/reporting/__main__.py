# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Run with python -m hcu_envcheck.reporting; no third-party packages required."""
from __future__ import annotations

import argparse
import signal
import socket
import sys
import time
import traceback
from pathlib import Path

from .aggregate import Run
from .model import TERMINAL, atomic, clean, number, report_lock, rows, table
from .parsers import rccl
from .progress import Display
from .render import rccl_reports, write


def probe(root: Path, *, ready=True):
    if sys.version_info < (3, 10):
        raise RuntimeError("reporting requires Python 3.10 or newer")
    if not root.is_dir():
        raise RuntimeError(f"运行目录不存在: {root}")
    if not (root / "control.tsv").is_file():
        raise RuntimeError("运行目录缺少 control.tsv；请检查共享目录映射")
    atomic(root / "report-host.tsv", "report_host\t" + socket.gethostname()
           + "\ninterpreter\t" + sys.executable + "\nreport_root\t" + str(Path.cwd()) + "\n")
    if ready:
        atomic(root / "reporter.ready", "python=" + sys.version.split()[0] + "\n")


def watch(args):
    root = Path(args.run_dir).resolve()
    final_status = "ERROR"
    interrupted = False
    finalized = False

    def stop(_signum, _frame):
        nonlocal interrupted
        interrupted = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        with report_lock(root):
            probe(root)
        run, display = Run(root), Display(args.display, args.width)
        last_report = 0.0
        # T34: the nominal stage-ack.tsv confirmation is retired — nothing
        # outside this loop ever read it. The stage-request file stays as the
        # effective trigger: a new (or not-yet-terminal) requested stage
        # refreshes the report every tick instead of waiting for the 15s
        # periodic write, matching the old durability behavior without the
        # self-referential acknowledgment file.
        refreshed_request = ""
        while not interrupted:
            with report_lock(root):
                final = (root / "done").exists()
                snapshot = run.snapshot(final)
                snapshot["run_dir"] = str(root)
                now = time.monotonic()
                request = (root / "stage-request").read_text(encoding="utf-8").strip() if (root / "stage-request").exists() else ""
                selected = [r for r in snapshot["results"] if r["stage_id"] == request]
                stage_pending = bool(request) and (not selected or any(r["status"] not in TERMINAL for r in selected))
                if final or (request and (request != refreshed_request or stage_pending)) or now - last_report >= 15:
                    write(root, snapshot)
                    refreshed_request = request
                    last_report = now
                display.show(snapshot, force=final)
                if final:
                    final_status = snapshot["status"]
                    table(root / "report-status.tsv", ["status"], [dict(status=final_status)])
                    atomic(root / "reporter.finished", final_status + "\n")
                    finalized = True
                    return 0 if final_status == "PASS" else 2
            time.sleep(1)
        raise RuntimeError("报告进程被中断；测试原始日志保留，可用 rebuild 重建报告")
    except Exception as exc:
        message = clean(f"{type(exc).__name__}: {exc}")
        # Keep the traceback beside the message so a reporting defect (e.g. a
        # parser hitting an unexpected payload shape) stays diagnosable from
        # the run directory alone.
        with report_lock(root):
            atomic(root / "reporter-error.log",
                   message + "\n" + "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
        sys.stderr.write("报告异常: " + message + "\n")
        return 1
    finally:
        if not finalized:
            with report_lock(root):
                table(root / "report-status.tsv", ["status"], [dict(status=final_status)])
                atomic(root / "reporter.finished", final_status + "\n")


def rccl_local(args):
    with report_lock(Path(args.log_dir).resolve()):
        return _rccl_local(args)


def _rccl_local(args):
    directory, plan_file = Path(args.log_dir).resolve(), Path(args.plan_file).resolve()
    plan, executed = rows(plan_file), rows(directory / "execution.tsv")
    if not plan or not executed:
        raise RuntimeError("RCCL plan 或 execution.tsv 缺失")
    results = []
    for item in plan:
        execution = next((x for x in executed if x.get("test") == item.get("test")), {})
        result = dict(item=item["test"], group="local", status="PASS" if number(execution.get("exit_code"), -1) == 0 else "FAIL",
                      exit_code=execution.get("exit_code", ""), reason_code="", detail="", metrics={})
        rccl(directory, directory, result, plan=item, directory=directory)
        results.append(result)
    rccl_reports(directory, results)
    for result in results:
        sys.stderr.write(f"{result['item']}: {result['status']} {clean(result['detail'])}\n")
    return 0 if all(r["status"] == "PASS" for r in results) else 2


def main():
    parser = argparse.ArgumentParser(description="Cluster stage progress, result parsing and reports")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("probe", "rebuild", "watch"):
        sub = commands.add_parser(name)
        sub.add_argument("--run-dir", required=True)
        if name == "watch":
            sub.add_argument("--display", choices=("tty", "plain", "quiet", "every"), default="plain")
            sub.add_argument("--width", type=int, default=80)
        if name == "rebuild":
            # T28: structured failure/unfinished export; plain rebuilds never
            # write it (explicit opt-in only, never a payload).
            sub.add_argument("--export-plan", default="", help="new absolute directory for plan export")
    sub = commands.add_parser("rccl-local")
    sub.add_argument("--log-dir", required=True)
    sub.add_argument("--plan-file", required=True)
    args = parser.parse_args()
    if args.command == "watch":
        return watch(args)
    if args.command == "rccl-local":
        return rccl_local(args)
    root = Path(args.run_dir).resolve()
    with report_lock(root):
        probe(root, ready=args.command == "probe")
        if args.command == "probe":
            return 0
        snapshot = Run(root).snapshot(final=True)
        snapshot["run_dir"] = str(root)
        write(root, snapshot)
        if args.command == "rebuild" and getattr(args, "export_plan", ""):
            from .export import export_plan
            export_plan(snapshot, root, Path(args.export_plan).resolve())
        table(root / "report-status.tsv", ["status"], [dict(status=snapshot["status"])])
    # Mirror the live controller mapping (finish_reporter): FAIL must exit 1
    # here too, so --report-only and a normal run disagree on no outcome.
    return {"PASS": 0, "WARN": 2, "FAIL": 1}.get(snapshot["status"], 70)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        # 70 = the report process did not complete normally (the live
        # controller's "报告未正常完成" class), distinct from FAIL's 1.
        print("报告异常: " + clean(error), file=sys.stderr)
        traceback.print_exc()
        raise SystemExit(70)
