#!/usr/bin/env python3
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
Run on each training node (via `ansible -m script`).
Discover training processes, dump stacks with py-spy, print JSON to stdout.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import socket
import subprocess
import sys
from typing import Any


TRAINING_PATTERNS = (
    r"torchrun|torch\.distributed\.run",
    r"train\.py|pretrain|finetune|megatron|deepspeed",
    r"dataloader|DataLoader|webdataset|prefetch",
    r"checkpoint|ByteCheckpoint|save_ckpt|load_ckpt",
)
ROLE_MAP = (
    (re.compile(r"torchrun|torch\.distributed\.run|train\.py|pretrain|megatron|deepspeed", re.I), "trainer"),
    (re.compile(r"dataloader|DataLoader|webdataset|prefetch", re.I), "dataloader"),
    (re.compile(r"checkpoint|ByteCheckpoint|save_ckpt|load_ckpt", re.I), "checkpoint"),
)


def classify_role(cmdline: str) -> str:
    for pattern, role in ROLE_MAP:
        if pattern.search(cmdline):
            return role
    return "other"


def extract_rank(
    cmdline: str, environ: dict[str, str] | None = None
) -> tuple[int, int]:
    """Return (global_rank, local_rank); -1 if missing."""
    environ = environ or {}
    global_rank = -1
    local_rank = -1

    if environ.get("RANK", "").isdigit():
        global_rank = int(environ["RANK"])
    if environ.get("LOCAL_RANK", "").isdigit():
        local_rank = int(environ["LOCAL_RANK"])

    for key in ("RANK",):
        if global_rank >= 0:
            break
        match = re.search(rf"{key}[= ](\d+)", cmdline, re.I)
        if match:
            global_rank = int(match.group(1))
            break

    for key in ("LOCAL_RANK", "--local_rank", "--local-rank"):
        if local_rank >= 0:
            break
        match = re.search(rf"{key}[= ](\d+)", cmdline, re.I)
        if match:
            local_rank = int(match.group(1))
            break

    return global_rank, local_rank


def resolve_global_rank(global_rank: int, local_rank: int, rank_start: int) -> int:
    if global_rank >= 0:
        return global_rank
    if local_rank >= 0 and rank_start >= 0:
        return rank_start + local_rank
    return -1


def _read_environ(pid: int, proc_root: str = "/proc") -> dict[str, str]:
    path = os.path.join(proc_root, str(pid), "environ")
    try:
        raw = open(path, "rb").read()
    except (OSError, PermissionError):
        return {}

    result: dict[str, str] = {}
    for entry in raw.split(b"\0"):
        if b"=" not in entry:
            continue
        key, value = entry.split(b"=", 1)
        result[key.decode("utf-8", "ignore")] = value.decode("utf-8", "ignore")
    return result


def iter_processes_psutil(proc_root: str = "/proc") -> list[dict[str, Any]]:
    import psutil

    rows: list[dict[str, Any]] = []
    for proc in psutil.process_iter(["pid", "ppid", "cmdline"]):
        try:
            info = proc.info
            cmd_parts = info.get("cmdline") or []
            cmdline = " ".join(cmd_parts)
            rows.append(
                {
                    "pid": int(info["pid"]),
                    "ppid": int(info["ppid"] or 0),
                    "cmdline": cmdline,
                    "environ": _read_environ(int(info["pid"]), proc_root),
                    "role": classify_role(cmdline),
                }
            )
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return rows


def iter_processes_procfs(proc_root: str = "/proc") -> list[dict[str, Any]]:
    """Fallback when psutil is unavailable."""
    rows: list[dict[str, Any]] = []
    try:
        pids = [int(name) for name in os.listdir(proc_root) if name.isdigit()]
    except OSError:
        return rows

    for pid in pids:
        cmdline_path = os.path.join(proc_root, str(pid), "cmdline")
        try:
            raw = open(cmdline_path, "rb").read().replace(b"\0", b" ").decode("utf-8", "ignore")
            stat_path = os.path.join(proc_root, str(pid), "stat")
            stat = open(stat_path, encoding="utf-8").read()
            ppid = int(stat.split(")", 1)[1].split()[1])
            rows.append(
                {
                    "pid": pid,
                    "ppid": ppid,
                    "cmdline": raw.strip(),
                    "environ": _read_environ(pid, proc_root),
                    "role": classify_role(raw),
                }
            )
        except OSError:
            continue
    return rows


def discover_training_processes(processes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_ppid: dict[int, list[dict[str, Any]]] = {}
    trainers: list[dict[str, Any]] = []

    for proc in processes:
        by_ppid.setdefault(proc["ppid"], []).append(proc)
        if proc["role"] == "trainer":
            trainers.append(proc)

    selected: dict[int, dict[str, Any]] = {
        proc["pid"]: proc for proc in processes if proc["role"] != "other"
    }

    queue = [trainer["pid"] for trainer in trainers]
    while queue:
        parent_pid = queue.pop()
        for child in by_ppid.get(parent_pid, []):
            if child["pid"] not in selected:
                selected[child["pid"]] = child
                queue.append(child["pid"])

    return list(selected.values())


def select_rank_worker_roots(
    processes: list[dict[str, Any]], rank_start: int
) -> list[dict[str, Any]]:
    """Keep the top-level Python worker for each rank, excluding torchrun launchers."""
    by_pid = {proc["pid"]: proc for proc in processes}
    candidates: list[dict[str, Any]] = []
    for proc in processes:
        global_rank, local_rank = extract_rank(proc["cmdline"], proc.get("environ"))
        proc["resolved_rank"] = resolve_global_rank(global_rank, local_rank, rank_start)
        is_launcher = bool(
            re.search(r"torchrun|torch\.distributed\.run", proc["cmdline"], re.I)
        )
        if proc["resolved_rank"] >= 0 and not is_launcher:
            candidates.append(proc)

    candidate_pids = {proc["pid"] for proc in candidates}
    roots: list[dict[str, Any]] = []
    for proc in candidates:
        parent_pid = proc["ppid"]
        has_same_rank_ancestor = False
        visited: set[int] = set()
        while parent_pid in by_pid and parent_pid not in visited:
            visited.add(parent_pid)
            parent = by_pid[parent_pid]
            if (
                parent_pid in candidate_pids
                and parent.get("resolved_rank") == proc["resolved_rank"]
            ):
                has_same_rank_ancestor = True
                break
            parent_pid = parent["ppid"]
        if not has_same_rank_ancestor:
            roots.append(proc)

    return roots


def parse_py_spy_json(payload: Any) -> list[dict[str, Any]]:
    frames: list[dict[str, Any]] = []
    # py-spy <=0.3 emits {"threads": [...]}; py-spy 0.4 emits [...] directly.
    if isinstance(payload, list):
        threads = payload
    elif isinstance(payload, dict):
        threads = payload.get("threads")
    else:
        threads = None
    if not threads:
        return frames

    def thread_priority(thread: dict[str, Any]) -> tuple[int, int, int]:
        is_main = (
            thread.get("thread_name") == "MainThread"
            or thread.get("name") == "MainThread"
            or (
                thread.get("os_thread_id") is not None
                and thread.get("os_thread_id") == thread.get("pid")
            )
        )
        return (
            0 if is_main else 1,
            0 if thread.get("active") else 1,
            -len(thread.get("frames") or []),
        )

    best_thread = min(threads, key=thread_priority)
    best = best_thread.get("frames") or []

    for item in best:
        frames.append(
            {
                "function": str(item.get("name") or item.get("function") or "?"),
                "file": str(item.get("filename") or item.get("file") or ""),
                "line": int(item.get("line") or 0),
            }
        )
    return frames


def dump_stack(
    py_spy: str, pid: int, timeout: float, *, nonblocking: bool = False
) -> tuple[list[dict[str, Any]], str]:
    cmd = [py_spy, "dump", "--pid", str(pid), "--json"]
    if nonblocking:
        cmd.append("--nonblocking")
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
    payload = json.loads(proc.stdout)
    frames = parse_py_spy_json(payload)
    raw = "\n".join(
        f"  {f['function']} ({f['file']}:{f['line']})" if f["file"] else f"  {f['function']}"
        for f in frames
    )
    return frames, raw


def main() -> int:
    parser = argparse.ArgumentParser(description="Remote py-spy stack capture for Ansible")
    parser.add_argument("--rank-start", type=int, default=-1, help="Global rank of local rank 0")
    parser.add_argument("--machine-id", default="", help="Override machine identifier")
    parser.add_argument("--py-spy", default="", help="Path to py-spy binary")
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument(
        "--nonblocking",
        action="store_true",
        help="Read stacks without SIGSTOP; recommended in containers/Kubernetes",
    )
    parser.add_argument(
        "--ranked-only",
        action="store_true",
        help="Capture one top-level training worker per global rank",
    )
    parser.add_argument("--debug-processes", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument(
        "--proc-root",
        default="/proc",
        help="procfs to scan (use /host/proc in a hostPID/privileged DaemonSet)",
    )
    args = parser.parse_args()

    py_spy = args.py_spy or shutil.which("py-spy") or "py-spy"
    if not shutil.which(py_spy) and py_spy == "py-spy":
        print("py-spy not found in PATH", file=sys.stderr)
        return 2

    machine_id = args.machine_id or socket.gethostname()

    try:
        processes = iter_processes_psutil(args.proc_root)
    except ImportError:
        import os

        processes = iter_processes_procfs(args.proc_root)

    training = discover_training_processes(processes)
    if args.ranked_only:
        discovered = training
        training = select_rank_worker_roots(discovered, args.rank_start)
    else:
        discovered = training
    if args.debug_processes:
        selected_pids = {proc["pid"] for proc in training}
        for proc in discovered:
            print(
                json.dumps(
                    {
                        "pid": proc["pid"],
                        "ppid": proc["ppid"],
                        "rank": proc.get("resolved_rank"),
                        "role": proc["role"],
                        "selected": proc["pid"] in selected_pids,
                        "cmdline": proc["cmdline"][:160],
                    }
                ),
                file=sys.stderr,
            )
    if not training:
        print("[]")
        return 0

    snapshots: list[dict[str, Any]] = []
    for proc in training:
        if "resolved_rank" in proc:
            rank = proc["resolved_rank"]
        else:
            global_rank, local_rank = extract_rank(
                proc["cmdline"], proc.get("environ")
            )
            rank = resolve_global_rank(global_rank, local_rank, args.rank_start)
        try:
            frames, raw = dump_stack(
                py_spy, proc["pid"], args.timeout, nonblocking=args.nonblocking
            )
        except Exception as exc:  # noqa: BLE001 - report per-process failures
            frames, raw = [], f"capture failed: {exc}"

        snapshots.append(
            {
                "machine_id": machine_id,
                "rank": rank,
                "pid": proc["pid"],
                "role": proc["role"],
                "frames": frames,
                "raw_text": raw,
            }
        )

    print(json.dumps(snapshots))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
