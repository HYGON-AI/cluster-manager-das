# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Commit raw evidence to one public group log before removing its source."""

from __future__ import annotations

import base64
import hashlib
import os
import re
import threading
import uuid
import shlex
import time
import sys
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path


_HEADER = "source\tarchive\toffset\tlength\tsha256\n"
_GROUP = re.compile(r"group-[0-9]+\Z")
_LOCKS: dict[tuple[str, str], threading.RLock] = {}
_LOCKS_GUARD = threading.Lock()
_CHUNK = 64 * 1024
_CHANNEL_LOCK = threading.RLock()
_CHANNEL_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{15,95}\Z")


@lru_cache(maxsize=1)
def _guard_body() -> str:
    return (Path(__file__).parent / "payloads" / "task_guard.sh").read_text(encoding="utf-8")


def _payload_command(command: list[str]) -> list[str]:
    """Show the complete payload argv, retaining the full guard in task metadata.

    Unwrap only our exact, known inline guard; never parse arbitrary shell text.
    """
    prefix = ["env", "-u", "BASH_ENV", "-u", "ENV", "bash", "--noprofile", "--norc", "-c"]
    for position in range(10, len(command) - 3):
        if (command[position:position + 2] == ["hcu-task-guard", "run"]
                and command[position + 3] == "--"
                and command[position - 10:position - 1] == prefix
                and command[position - 1] == _guard_body()):
            return command[:position - 10] + command[position + 4:]
    return command


class LogArchiveError(RuntimeError):
    """The source must remain available because its archive did not commit."""


def _paths(public_run: Path, group: str) -> tuple[Path, Path, Path]:
    if not _GROUP.fullmatch(group):
        raise ValueError(f"invalid log group: {group!r}")
    root = Path(public_run).resolve()
    return root, root / f"{group}.log", root / ".state" / "log-archive" / f"{group}.tsv"


def _lock(root: Path, group: str) -> threading.RLock:
    with _LOCKS_GUARD:
        return _LOCKS.setdefault((str(root), group), threading.RLock())


def ensure_group_log(public_run: Path, group: str) -> Path:
    root, archive, _ = _paths(public_run, group)
    with _lock(root, group):
        root.mkdir(parents=True, exist_ok=True)
        archive.touch(exist_ok=True)
    return archive


def finish_group_log_state(public_run: Path, *, cleanup_confirmed: bool) -> None:
    """Prune only closed Python-run locks, after all pools and writers joined.

    Fact JSON, direct-log pointers and both index formats remain unchanged.
    Aborted or unconfirmed runs retain every runtime file for diagnosis.
    """
    if not cleanup_confirmed:
        return
    import subprocess
    import sys
    state = Path(public_run).resolve() / ".state"
    if not state.is_dir():
        return
    try:
        result = subprocess.run(
            ["bash", str(Path(__file__).with_name("state_runtime.sh")),
             "finalize", str(state), "python-closed"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60,
        )
        if result.returncode == 0:
            return
    except (OSError, subprocess.TimeoutExpired):
        pass
    print(f"[WARN] temporary state cleanup incomplete; execution evidence retained: {state}", file=sys.stderr)


class DirectGroupLog:
    """Append live output once, indexing each source's possibly interleaved bytes.

    The memory capture used for command evaluation stays bounded independently.
    No temporary output file is created, read back or copied into the group log.
    """
    def __init__(self, root: Path, group: str, directory: Path, node: str,
                 command: list[str], item: str, *, members=()):
        self.root, self.archive, _ = _paths(root, group)
        self.group, self.directory, self.node = group, Path(directory), node
        self.item, self.started = item, time.monotonic()
        self.index = self.root / ".state" / "log-stream" / f"{group}.tsv"
        self.channel = os.environ.get("HCU_LOG_CHANNEL") == "1"
        self.channel_token = os.environ.get("HCU_LOG_TOKEN", "") if self.channel else ""
        if self.channel and not _CHANNEL_TOKEN.fullmatch(self.channel_token):
            raise ValueError("direct log channel requires a valid task token")
        _source(self.root, self.directory / "stdout.txt")
        if not self.channel:
            self.directory.mkdir(parents=True, exist_ok=True)
        inferred = next((part for part in reversed(self.directory.resolve().relative_to(self.root / ".state").parts)
                         if _GROUP.fullmatch(part)), None)
        if inferred != group and not self.channel:
            pointer = self.directory / "direct-log.json"
            pointer.write_text('{"group": "' + group + '"}\n', encoding="utf-8")
        self.finished = False
        member_text = ",".join(members) if members else node
        scope = "docker exec" if command[:2] == ["docker", "exec"] else "直接执行（目标环境见本项标题）"
        environment = os.environ.get("HCU_LOG_SCENARIO", scope)
        container = os.environ.get("HCU_LOG_CONTAINER", "")
        if container:
            environment += f" / {container}"
        self._append(None, (
            "\n" + "=" * 80 + f"\n【开始】子项 · {item}\n" + "=" * 80 + "\n\n"
            f"测试环境：{environment}\n参与节点：{member_text}\n启动节点：{node}\n"
            f"开始时间：{time.strftime('%Y-%m-%d %H:%M:%S%z')}\n"
            f"\n【执行命令｜在 {node} 执行】\n{shlex.join(_payload_command(command))}\n【命令结束】\n"
            "\n【测试原始输出｜子命令 stdout/stderr】\n" + "-" * 80 + "\n"
        ).encode("utf-8"))
        for stream in ("stdout", "stderr"):
            self.feed(stream, b"")

    @contextmanager
    def _locked(self):
        directory = self.root / ".state" / "log-archive"
        directory.mkdir(parents=True, exist_ok=True)
        with _lock(self.root, self.group), (directory / f"{self.group}.lock").open("ab") as lock:
            if os.name == "posix":
                import fcntl
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                if os.name == "posix":
                    fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def _append(self, source: str | None, data: bytes):
        if not data and source is None:
            return
        if self.channel:
            self._append_channel(source, data)
            return
        try:
            with self._locked():
                self.root.mkdir(parents=True, exist_ok=True)
                with self.archive.open("ab", buffering=0) as output:
                    offset = output.tell()
                    view = memoryview(data)
                    while view:
                        written = output.write(view)
                        if not written:
                            raise OSError("short direct-log write")
                        view = view[written:]
                self.index.parent.mkdir(parents=True, exist_ok=True)
                with self.index.open("a", encoding="utf-8", newline="") as index:
                    if index.tell() == 0:
                        index.write(_HEADER)
                    if source is not None:
                        index.write(f"{source}\t{self.archive.name}\t{offset}\t{len(data)}\t{hashlib.sha256(data).hexdigest()}\n")
        except OSError as exc:
            raise LogArchiveError(f"cannot write direct group log {self.archive}: {exc}") from exc

    def _append_channel(self, source: str | None, data: bytes):
        """Frame source bytes for the controller's sole FIFO log writer.

        The target cannot know final archive offsets. The finish helper scans
        these length-delimited frames after the FIFO drains and commits them.
        One lock protects the complete header/body across all writer threads.
        """
        def write_all(value):
            view = memoryview(value)
            while view:
                written = os.write(1, view)
                if not written:
                    raise OSError("short direct-log channel write")
                view = view[written:]

        try:
            with _CHANNEL_LOCK:
                sys.stdout.flush()
                if source is None:
                    write_all(data)
                    return
                encoded = base64.b64encode(source.encode("utf-8"))
                # Never buffer or concatenate a second full output copy.
                for start in range(0, max(1, len(data)), _CHUNK):
                    chunk = memoryview(data)[start:start + _CHUNK]
                    header = (b"\n[HCU-LOG-CHUNK " + self.channel_token.encode("ascii") + b"]\t"
                              + encoded + b"\t" + str(len(chunk)).encode("ascii") + b"\t"
                              + hashlib.sha256(chunk).hexdigest().encode("ascii") + b"\n")
                    write_all(header)
                    write_all(chunk)
        except OSError as exc:
            raise LogArchiveError(f"cannot write direct group log channel: {exc}") from exc

    def feed(self, stream: str, data: str | bytes | None):
        source = _source(self.root, self.directory / f"{stream}.txt")
        self._append(source, data.encode("utf-8", "replace") if isinstance(data, str) else data or b"")

    def finish(self, code: int, detail: str = ""):
        if self.finished:
            return
        self.finished = True
        outcome = "执行成功" if code == 0 else "执行失败"
        self._append(None, (
            "\n" + "-" * 80 + f"\n【结束】子项 · {self.item}\n"
            f"启动节点：{self.node}\n执行结果：{outcome}，退出码 {code}\n"
            f"执行耗时：{time.monotonic() - self.started:.3f} 秒\n任务清理：由外层任务确认\n"
            + (f"执行说明：{detail}\n" if detail else "")
        ).encode("utf-8"))
        if self.channel:
            return
        with self._locked():
            for path in (self.archive, self.index):
                if path.is_file():
                    with path.open("rb") as stream:
                        os.fsync(stream.fileno())
            for directory in (self.index.parent, self.root / ".state", self.root, self.root.parent):
                _sync_directory(directory)


def direct_text(root: Path, group: str, source: Path, text: str):
    """Append an exceptional dispatch diagnostic without a staging log."""
    writer = DirectGroupLog(root, group, source.parent, "controller", [], source.name)
    writer.feed("stderr", text)
    writer.finish(1)


def metadata_text(root: Path, group: str, source: Path, text: str):
    """Keep adapter state outside the raw test stream."""
    _paths(root, group)
    _source(Path(root).resolve(), source)
    source = Path(source).with_suffix(".json")
    source.parent.mkdir(parents=True, exist_ok=True)
    temporary = source.with_name(source.name + f".{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(text, encoding="utf-8")
        os.replace(temporary, source)
    finally:
        temporary.unlink(missing_ok=True)


def _source(root: Path, path: Path) -> str:
    value = Path(path).resolve().relative_to(root / ".state").as_posix()
    if any(character in value for character in "\t\r\n"):
        raise ValueError("log source path contains a TSV delimiter")
    return value


def _rows(index: Path) -> dict[str, tuple[str, str, str, str]]:
    if not index.exists():
        return {}
    lines = index.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != _HEADER.rstrip("\n"):
        raise ValueError(f"invalid log archive index: {index}")
    result = {}
    for line in lines[1:]:
        fields = line.split("\t")
        if len(fields) != 5 or not fields[2].isdigit() or not fields[3].isdigit():
            raise ValueError(f"invalid log archive entry: {index}")
        result[fields[0]] = tuple(fields[1:])
    return result


def _range_digest(path: Path, offset: int, length: int) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        stream.seek(offset)
        remaining = length
        while remaining:
            chunk = stream.read(min(_CHUNK, remaining))
            if not chunk:
                raise OSError(f"incomplete archived log: {path}")
            digest.update(chunk)
            remaining -= len(chunk)
    return digest.hexdigest()


def _sync_directory(directory: Path) -> None:
    if hasattr(os, "O_DIRECTORY"):
        descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _archive_file(public_run: Path, group: str, source_path: Path) -> dict[str, object]:
    """Archive a closed .state file and atomically commit its byte-range index.

    An append or index failure leaves the source in place. A repeated call can
    finish removal after a previously committed index without appending again.
    Callers must not modify the source concurrently with this operation.
    """
    root, archive, index = _paths(public_run, group)
    path = Path(source_path)
    source = _source(root, path)
    with _lock(root, group):
        ensure_group_log(root, group)
        rows = _rows(index)
        old = rows.get(source)
        if old is not None and path.is_file():
            old_archive, old_offset, old_length, old_digest = old
            archived = (root / old_archive).resolve()
            archived.relative_to(root)
            current_digest = _range_digest(path, 0, path.stat().st_size)
            if (path.stat().st_size == int(old_length) and current_digest == old_digest
                    and _range_digest(archived, int(old_offset), int(old_length)) == old_digest):
                for directory in (index.parent, root / ".state", root, root.parent):
                    _sync_directory(directory)
                path.unlink()
                return {"archive": old_archive, "offset": int(old_offset), "length": int(old_length), "sha256": old_digest}
        digest = hashlib.sha256()
        length = 0
        with path.open("rb") as original, archive.open("ab") as output:
            output.write(f"\n===== 原始输出（来源路径，已合并）：{root / '.state' / source} =====\n归档日志：{archive}\n".encode("utf-8"))
            offset = output.tell()
            while True:
                chunk = original.read(_CHUNK)
                if not chunk:
                    break
                output.write(chunk)
                digest.update(chunk)
                length += len(chunk)
            output.write(b"\n")
            output.flush()
            os.fsync(output.fileno())
        checksum = digest.hexdigest()
        if _range_digest(archive, offset, length) != checksum:
            raise OSError(f"archived log checksum mismatch: {path}")
        rows[source] = (archive.name, str(offset), str(length), checksum)
        index.parent.mkdir(parents=True, exist_ok=True)
        temporary = index.with_name(f".{index.name}.{uuid.uuid4().hex}.tmp")
        try:
            with temporary.open("w", encoding="utf-8", newline="") as output:
                output.write(_HEADER)
                for name, fields in rows.items():
                    output.write("\t".join((name, *fields)) + "\n")
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, index)
            for directory in (index.parent, root / ".state", root, root.parent):
                _sync_directory(directory)
        finally:
            temporary.unlink(missing_ok=True)
        path.unlink()
        return {"archive": archive.name, "offset": offset, "length": length, "sha256": checksum}


def archive_file(public_run: Path, group: str, source_path: Path) -> dict[str, object]:
    try:
        return _archive_file(public_run, group, source_path)
    except Exception as exc:
        raise LogArchiveError(f"cannot commit group log for {source_path}: {exc}") from exc


def archive_text(public_run: Path, group: str, source_path: Path, text: str) -> dict[str, object]:
    """Persist text before archiving so unsuccessful commits retain evidence."""
    path = Path(source_path)
    root, _, _ = _paths(public_run, group)
    _source(root, path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(text.encode("utf-8"))
    return archive_file(root, group, path)


def read_log_text(path: Path, limit: int | None = None) -> str:
    """Read an existing legacy log or its committed public-log byte range."""
    from .reporting.log_store import stream_fragments, _FragmentReader
    from .reporting.facts import log_fragments as compact_fragments
    compact = compact_fragments(Path(path))
    fragments = compact if compact is not None else stream_fragments(Path(path))
    if fragments:
        data = bytearray()
        for fragment in fragments:
            with _FragmentReader(fragment) as stream:
                amount = fragment.length if limit is None else min(fragment.length, max(0, limit - len(data)))
                data.extend(stream.read(amount))
            if limit is not None and len(data) >= limit:
                break
        return data.decode("utf-8", "replace")
    if compact is not None:
        # The compact index owns this logical source even when it has no row.
        # A stale physical file must never fill an uncommitted evidence gap.
        raise FileNotFoundError(path)
    path = Path(path)
    if path.is_file():
        with path.open("rb") as stream:
            return (stream.read() if limit is None else stream.read(limit)).decode("utf-8", "replace")
    for state in path.resolve().parents:
        if state.name != ".state":
            continue
        root = state.parent
        source = path.resolve().relative_to(state).as_posix()
        for index in sorted((state / "log-archive").glob("group-*.tsv")):
            row = _rows(index).get(source)
            if row is None:
                continue
            name, offset, length, digest = row
            archive = (root / name).resolve()
            archive.relative_to(root)
            if _range_digest(archive, int(offset), int(length)) != digest:
                raise OSError(f"archived log checksum mismatch: {path}")
            with archive.open("rb") as stream:
                stream.seek(int(offset))
                return stream.read(int(length) if limit is None else min(int(length), limit)).decode("utf-8", "replace")
        break
    raise FileNotFoundError(path)
