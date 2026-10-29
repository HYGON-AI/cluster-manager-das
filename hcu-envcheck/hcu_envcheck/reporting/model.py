# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""The on-disk protocol is TSV; all reports are derived from execution facts."""
from __future__ import annotations

import csv
import math
import os
import re
import time
from contextlib import contextmanager
from pathlib import Path

from .facts import fact_signature, read_rows
from .log_store import open_log

TERMINAL = {"PASS", "FAIL", "TIMEOUT", "BLOCKED", "NA", "CANCELLED", "WARN"}
SUCCESS = {"PASS", "WARN", "NA"}
# Result protocol v3 removes the cross-round stability snapshot field and
# verdict. The on-disk TSV schema stays at version 1, including historical
# stage ids and iteration values. Execution state, check conclusion,
# correctness/performance verdict and cleanup confirmation remain separate.
# SUCCESS is kept for compatibility; VALID_CONCLUSION additionally excludes
# NA, so "not applicable" never counts as an accepted result on its own.
VALID_CONCLUSION = {"PASS", "WARN"}
PROTOCOL_VERSION = 3

# Stable reason_code enumeration emitted by controller.sh and the reporting
# parsers. Codes may be appended, never renamed; unknown codes classify as
# "other" instead of breaking aggregation.
REASON_CODES = frozenset({
    "OK", "PASS", "NA", "RUNNING", "PENDING",
    "EXECUTION_FAILED", "INCOMPLETE", "TIMEOUT", "CANCELLED", "LOG_WRITE_FAILED",
    "CHECK_FAILED", "CHECK_WARN", "CHECK_INCOMPLETE",
    "BELOW_BASELINE", "BASELINE_MISSING", "METRIC_MISSING", "CASE_FAILED",
    "STABILITY_FAILED",  # Historical reason code; no longer emitted.
    "CLEANUP_UNCONFIRMED",
    # T18 lifecycle per-node container facts (reason_category: "other");
    # appended, never renamed, per the rule above.
    "CONTAINER_ABSENT", "CONTAINER_STOPPED", "IMAGE_MISMATCH", "NODE_UNREACHABLE",
})
EXECUTION_REASONS = {"EXECUTION_FAILED", "INCOMPLETE", "TIMEOUT", "CANCELLED", "LOG_WRITE_FAILED", "CHECK_INCOMPLETE"}
PERFORMANCE_REASONS = {"BELOW_BASELINE", "BASELINE_MISSING", "METRIC_MISSING", "STABILITY_FAILED", "CASE_FAILED"}
CORRECTNESS_REASONS = {"CHECK_FAILED", "CHECK_WARN", "PASS", "OK", "NA"}
CLEANUP_REASONS = {"CLEANUP_UNCONFIRMED"}


def reason_category(status: object, reason_code: object) -> str:
    """Classify one result: scheduling/execution/cleanup/correctness/performance/other."""
    code = str(reason_code or "").strip()
    state = str(status or "")
    if code in CLEANUP_REASONS:
        return "cleanup"
    if code in EXECUTION_REASONS:
        return "execution"
    if code in PERFORMANCE_REASONS:
        return "performance"
    if code in CORRECTNESS_REASONS:
        return "correctness"
    if state in {"PENDING", "RUNNING"} or code in {"RUNNING", "PENDING"}:
        return "scheduling"
    return "other"

ANSI = re.compile(r"\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07]*(?:\x07|\x1b\\))")


def clean(value: object) -> str:
    text = ANSI.sub("", str(value))
    return "".join(c if ord(c) >= 32 and ord(c) != 127 else " " for c in text)


def number(value: object, default=None):
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError):
        return default


def rows(path: Path) -> list[dict]:
    projected = read_rows(path)
    if projected is not None:
        return projected
    try:
        with path.open(encoding="utf-8-sig", errors="replace", newline="") as stream:
            return list(csv.DictReader(stream, delimiter="\t"))
    except FileNotFoundError:
        return []


class IncrementalRows:
    """T23: stat-signature cache for snapshot re-reads. A file whose
    (mtime_ns, size) is unchanged is served from memory instead of being
    re-read; a rewritten, restored or truncated file has a different
    signature and is read fresh; a file that disappears stops being served.
    Entries are replaced per path (never appended), so repeated snapshots
    cannot double-count rows. A fresh process starts empty, so report-only
    rebuilds always come from disk."""
    def __init__(self) -> None:
        self._entries: dict[Path, tuple[tuple, list]] = {}

    @staticmethod
    def signature(path: Path):
        return fact_signature(path)

    def rows(self, path: Path) -> list[dict]:
        signature = self.signature(path)
        if signature is None:
            self._entries.pop(path, None)
            return []
        entry = self._entries.get(path)
        if entry is not None and entry[0] == signature:
            return entry[1]
        data = rows(path)
        self._entries[path] = (signature, data)
        return data


def control(root: Path) -> dict:
    try:
        with (root / "control.tsv").open(encoding="utf-8-sig", errors="replace") as stream:
            return dict(line.rstrip("\r\n").split("\t", 1) for line in stream if "\t" in line)
    except FileNotFoundError:
        return {}


def public_root(root: Path) -> Path:
    return root.parent if root.name == ".state" else root


@contextmanager
def report_lock(root: Path):
    """Serialize a complete reporting cycle on one stable, never-unlinked inode."""
    # Do not use atomic() or truncate/unlink this file: another reporter may
    # already be waiting on its open descriptor. The controller can precreate
    # it with the entry user's ownership before starting a container reporter.
    path = Path(root) / "report.lock"
    try:
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        descriptor = os.open(path, os.O_RDWR)
    else:
        # Container root must not leave a login-user-owned run with a lock
        # that its owner cannot reopen. Only a newly created inode is adjusted.
        if hasattr(os, "geteuid") and os.geteuid() == 0:
            owner = Path(root).stat()
            os.fchown(descriptor, owner.st_uid, owner.st_gid)
    with os.fdopen(descriptor, "a+b") as lock:
        if os.name == "posix":
            import fcntl
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        else:
            import msvcrt
            if lock.seek(0, 2) == 0:
                lock.write(b"\0")
                lock.flush()
            lock.seek(0)
            msvcrt.locking(lock.fileno(), msvcrt.LK_LOCK, 1)
        try:
            yield
        finally:
            if os.name == "posix":
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
            else:
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)


def direct_group_logs(root: Path, task: Path) -> bool:
    """One run-level format field; accept historical per-task markers."""
    return (task / "log-format").is_file() or control(root).get("log_format", "").startswith("direct-group-")


def atomic(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        stream.write(text)
    os.replace(temporary, path)


def table_text(fields: list[str], records: list[dict]) -> str:
    content = ["\t".join(fields)]
    content.extend("\t".join(clean(row.get(key, "")) for key in fields) for row in records)
    return "\n".join(content) + "\n"


def table(path: Path, fields: list[str], records: list[dict]):
    atomic(path, table_text(fields, records))


def lines(path: Path):
    try:
        with open_log(path) as stream:
            for line in stream:
                yield clean(line)
    except FileNotFoundError:
        return


def log_path(root: Path, task: Path, value: str) -> Path:
    if not value or value == "-":
        return task / "task.log"
    entry = control(root).get("entry_run_dir", "").rstrip("/\\")
    if entry and (value == entry or value.startswith(entry + "/") or value.startswith(entry + "\\")):
        return root / value[len(entry):].lstrip("/\\").replace("\\", "/")
    path = Path(value)
    if path.is_absolute():
        return path
    # Worker records are run-relative. Legacy local filenames are task-relative.
    return task / path if len(path.parts) == 1 else root / path


def age(start: object, end: object = None) -> str:
    seconds = max(0, int((number(end, time.time())) - number(start, time.time())))
    hours, rest = divmod(seconds, 3600)
    minutes, seconds = divmod(rest, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}" if hours else f"{minutes:02}:{seconds:02}"
