# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import shutil
import subprocess
from typing import Any

from .models import ProcessRole, StackFrame, StackSnapshot


class PySpyCapture:
    """On-demand stack capture via py-spy (non-invasive, safe for hung processes)."""

    def __init__(
        self,
        py_spy_path: str | None = None,
        *,
        native: bool = False,
        timeout_sec: float = 15.0,
    ) -> None:
        self.py_spy_path = py_spy_path or shutil.which("py-spy") or "py-spy"
        self.native = native
        self.timeout_sec = timeout_sec

    def ensure_available(self) -> None:
        if not shutil.which(self.py_spy_path):
            raise RuntimeError(
                f"py-spy not found at '{self.py_spy_path}'. "
                "Install: pip install py-spy  (Linux recommended for production training nodes)"
            )

    def dump_pid(
        self,
        pid: int,
        *,
        machine_id: str = "local",
        rank: int = -1,
        role: ProcessRole = ProcessRole.OTHER,
    ) -> StackSnapshot:
        self.ensure_available()
        cmd = [
            self.py_spy_path,
            "dump",
            "--pid",
            str(pid),
            "--json",
        ]
        if self.native:
            cmd.append("--native")

        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=self.timeout_sec,
            check=False,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                f"py-spy dump failed for pid={pid}: {proc.stderr.strip() or proc.stdout.strip()}"
            )

        payload = json.loads(proc.stdout)
        frames = _parse_py_spy_json(payload)
        raw_text = _format_raw_stack(frames)
        return StackSnapshot(
            machine_id=machine_id,
            rank=rank,
            pid=pid,
            role=role,
            frames=tuple(frames),
            raw_text=raw_text,
        )

    def dump_many(
        self,
        targets: list[tuple[int, str, int, ProcessRole]],
    ) -> list[StackSnapshot]:
        snapshots: list[StackSnapshot] = []
        for pid, machine_id, rank, role in targets:
            snapshots.append(
                self.dump_pid(pid, machine_id=machine_id, rank=rank, role=role)
            )
        return snapshots


def _parse_py_spy_json(payload: Any) -> list[StackFrame]:
    frames: list[StackFrame] = []

    # py-spy <=0.3: {"threads": [...]}; py-spy 0.4: [...] directly.
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
            StackFrame(
                function=str(item.get("name") or item.get("function") or "?"),
                file=str(item.get("filename") or item.get("file") or ""),
                line=int(item.get("line") or 0),
            )
        )
    return frames


def parse_py_spy_text(raw: str) -> list[StackFrame]:
    """Parse plain-text `py-spy dump` output as a fallback."""
    frames: list[StackFrame] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("Thread"):
            continue
        # Example: "    foo (bar.py:12)"
        if "(" in line and ")" in line:
            func_part, rest = line.split("(", 1)
            file_part = rest.rstrip(")").strip()
            line_no = 0
            file_name = file_part
            if ":" in file_part:
                file_name, maybe_line = file_part.rsplit(":", 1)
                if maybe_line.isdigit():
                    line_no = int(maybe_line)
            frames.append(
                StackFrame(
                    function=func_part.strip(),
                    file=file_name.strip(),
                    line=line_no,
                )
            )
        else:
            frames.append(StackFrame(function=line))
    return frames


def _format_raw_stack(frames: list[StackFrame]) -> str:
    return "\n".join(
        f"  {frame.function} ({frame.file}:{frame.line})" if frame.file else f"  {frame.function}"
        for frame in frames
    )
