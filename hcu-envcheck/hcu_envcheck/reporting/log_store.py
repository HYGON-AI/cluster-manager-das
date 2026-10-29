# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Read logical task logs after their bytes move into a public group log."""
from __future__ import annotations

import csv
import io
import re
import json
import hashlib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from .facts import FactLogFragment, load_group, log_fragments as compact_fragments


INDEX_FIELDS = ("source", "archive", "offset", "length", "sha256")
_indexes: dict[Path, tuple[tuple, dict]] = {}
_stream_indexes: dict[Path, tuple[tuple, dict]] = {}


@dataclass(frozen=True)
class LogFragment:
    archive: Path
    offset: int
    length: int
    sha256: str


def _verify_compact(fragment: FactLogFragment):
    """Verify a committed range with bounded memory, including empty streams."""
    with fragment.archive.open("rb") as stream:
        import os
        stat = os.fstat(stream.fileno())
        if stat.st_size < fragment.offset + fragment.length:
            raise OSError(f"truncated compact group log: {fragment.archive}")
        # Shared filesystems can expose the same stat signature after a rapid
        # same-size rewrite. A previously verified range is therefore not proof
        # that these bytes still match: verify every open, without raw caching.
        stream.seek(fragment.offset)
        remaining, digest = fragment.length, hashlib.sha256()
        while remaining:
            chunk = stream.read(min(65536, remaining))
            if not chunk:
                raise OSError(f"truncated compact log range: {fragment.archive}")
            digest.update(chunk)
            remaining -= len(chunk)
        if digest.hexdigest() != fragment.sha256:
            raise OSError(f"compact log checksum mismatch: {fragment.archive}")


def _compact(path: Path):
    fragments = compact_fragments(path)
    if fragments is not None:
        for fragment in fragments:
            _verify_compact(fragment)
    return fragments


def _relative(value: str) -> str | None:
    path = PurePosixPath(value.replace("\\", "/"))
    if not value or path.is_absolute() or ".." in path.parts or not path.parts:
        return None
    if ":" in path.parts[0]:
        return None
    return path.as_posix()


def _index(path: Path) -> dict[str, LogFragment]:
    try:
        stat = path.stat()
    except OSError:
        _indexes.pop(path, None)
        return {}
    # The writer atomically replaces the index after every append. Include
    # inode/ctime as well as size/mtime so replacement and appended rows both
    # invalidate an already-running reporter's cache.
    signature = (stat.st_mtime_ns, stat.st_ctime_ns, stat.st_size, stat.st_ino)
    cached = _indexes.get(path)
    if cached is not None and cached[0] == signature:
        return cached[1]
    entries = {}
    try:
        with path.open(encoding="utf-8-sig", errors="replace", newline="") as stream:
            reader = csv.DictReader(stream, delimiter="\t")
            if tuple(reader.fieldnames or ()) != INDEX_FIELDS:
                return {}
            for row in reader:
                source = _relative(row.get("source") or "")
                archive = _relative(row.get("archive") or "")
                digest = row.get("sha256") or ""
                try:
                    offset, length = int(row.get("offset", "")), int(row.get("length", ""))
                except (TypeError, ValueError):
                    continue
                if (source is None or archive != path.stem + ".log" or offset < 0 or length < 0
                        or not re.fullmatch(r"[0-9a-fA-F]{64}", digest)):
                    continue
                entries[source] = LogFragment(path.parent.parent.parent / archive, offset, length, digest)
    except (OSError, csv.Error):
        return {}
    _indexes[path] = (signature, entries)
    return entries


def archived_log(path: Path) -> LogFragment | None:
    """Find an exact logical source; never use another task's group-log tail."""
    path = Path(path)
    compact = _compact(path)
    if compact is not None:
        return compact[0] if len(compact) == 1 else None
    state = next((parent for parent in path.parents if parent.name == ".state"), None)
    if state is None:
        return None
    source = path.relative_to(state).as_posix()
    group = next((part for part in reversed(path.relative_to(state).parts)
                  if re.fullmatch(r"group-\d+", part)), None)
    if group is not None:
        fragment = _index(state / "log-archive" / f"{group}.tsv").get(source)
    else:
        # Legacy per-node sources can be node-relative without a group directory.
        # Match the exact logical source and refuse ambiguous duplicate keys.
        matches = [entry for index in (state / "log-archive").glob("group-*.tsv")
                   if re.fullmatch(r"group-\d+", index.stem)
                   and (entry := _index(index).get(source)) is not None]
        fragment = matches[0] if len(matches) == 1 else None
    if fragment is None:
        return None
    try:
        if not fragment.archive.is_file() or fragment.archive.stat().st_size < fragment.offset + fragment.length:
            return None
    except OSError:
        return None
    return fragment


def group_log(state: Path, group: str) -> Path | None:
    """Return the public log only after a valid archive index is committed."""
    if state.name != ".state" or not re.fullmatch(r"group-\d+", group):
        return None
    facts = load_group(state, group)
    if facts is not None:
        path = state.parent / f"{group}.log"
        return path if facts.table("logs") and path.is_file() else None
    entries = _index(state / "log-archive" / f"{group}.tsv")
    path = state.parent / f"{group}.log"
    live = state / "log-stream" / f"{group}.tsv"
    return path if (entries or live.is_file()) and path.is_file() else None


def stream_fragments(path: Path) -> list[LogFragment]:
    """Resolve all direct-write chunks for exactly this logical source."""
    compact = _compact(path)
    if compact is not None:
        return compact
    state = next((parent for parent in path.parents if parent.name == ".state"), None)
    if state is None:
        return []
    source = path.relative_to(state).as_posix()
    group = next((part for part in reversed(path.relative_to(state).parts)
                  if re.fullmatch(r"group-\d+", part)), None)
    pointer = path.parent / "direct-log.json"
    if pointer.is_file():
        try:
            value = json.loads(pointer.read_text(encoding="utf-8")).get("group", "")
            if re.fullmatch(r"group-\d+", value):
                group = value
        except (OSError, ValueError, AttributeError, TypeError):
            pass
    directory = state / "log-stream"
    indexes = [directory / f"{group}.tsv"] if group else sorted(directory.glob("group-*.tsv"))
    result = []
    for index in indexes:
        if not index.is_file():
            continue
        if not re.fullmatch(r"group-\d+", index.stem):
            continue
        stat = index.stat()
        signature = (stat.st_mtime_ns, stat.st_ctime_ns, stat.st_size, stat.st_ino)
        cached = _stream_indexes.get(index)
        if cached is not None and cached[0] == signature:
            result.extend(cached[1].get(source, []))
            continue
        entries = {}
        with index.open(encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream, delimiter="\t")
            if tuple(reader.fieldnames or ()) != INDEX_FIELDS:
                raise OSError(f"invalid direct log index: {index}")
            for row in reader:
                try:
                    offset, length = int(row["offset"]), int(row["length"])
                    valid = (_relative(row.get("source") or "") is not None
                             and row["archive"] == index.stem + ".log" and offset >= 0 and length >= 0
                             and re.fullmatch(r"[0-9a-f]{64}", row["sha256"]))
                except (KeyError, TypeError, ValueError):
                    valid = False
                if not valid:
                    raise OSError(f"invalid direct log fragment: {index}")
                archive = state.parent / row["archive"]
                if not archive.is_file() or archive.stat().st_size < offset + length:
                    raise OSError(f"truncated direct group log: {archive}")
                entries.setdefault(row["source"], []).append(LogFragment(archive, offset, length, row["sha256"]))
        _stream_indexes[index] = (signature, entries)
        result.extend(entries.get(source, []))
    return result


class _FragmentsReader(io.RawIOBase):
    def __init__(self, fragments):
        super().__init__()
        self.fragments = iter(fragments)
        self.current = None

    def readable(self):
        return True

    def readinto(self, buffer):
        while True:
            if self.current is None:
                fragment = next(self.fragments, None)
                if fragment is None:
                    return 0
                self.current = _FragmentReader(fragment)
            size = self.current.readinto(buffer)
            if size:
                return size
            self.current.close()
            self.current = None

    def close(self):
        try:
            if self.current is not None:
                self.current.close()
        finally:
            super().close()


class _FragmentReader(io.RawIOBase):
    """A bounded binary stream for TextIOWrapper's normal UTF-8/newline rules."""
    def __init__(self, fragment: LogFragment):
        super().__init__()
        if isinstance(fragment, FactLogFragment):
            _verify_compact(fragment)
        self.stream = fragment.archive.open("rb")
        self.stream.seek(fragment.offset)
        self.remaining = fragment.length

    def readable(self):
        return True

    def readinto(self, buffer):
        if self.remaining <= 0:
            return 0
        data = self.stream.read(min(len(buffer), self.remaining))
        if not data:
            raise OSError("archived log fragment is truncated")
        buffer[:len(data)] = data
        self.remaining -= len(data)
        return len(data)

    def close(self):
        try:
            self.stream.close()
        finally:
            super().close()


def open_log(path: Path):
    compact = _compact(path)
    if compact is not None:
        if not compact:
            raise FileNotFoundError(path)
        return io.TextIOWrapper(io.BufferedReader(_FragmentsReader(compact)), encoding="utf-8", errors="replace")
    try:
        return path.open(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        fragments = stream_fragments(path)
        if fragments:
            return io.TextIOWrapper(io.BufferedReader(_FragmentsReader(fragments)), encoding="utf-8", errors="replace")
        fragment = archived_log(path)
        if fragment is None:
            raise
        return io.TextIOWrapper(io.BufferedReader(_FragmentReader(fragment)), encoding="utf-8", errors="replace")


def log_size(path: Path) -> int | None:
    compact = _compact(path)
    if compact is not None:
        return sum(fragment.length for fragment in compact) if compact else None
    try:
        if path.is_file():
            return path.stat().st_size
    except OSError:
        pass
    fragments = stream_fragments(path)
    if fragments:
        return sum(fragment.length for fragment in fragments)
    fragment = archived_log(path)
    return fragment.length if fragment is not None else None


def log_tail(path: Path, limit: int = 8192) -> str:
    compact = _compact(path)
    if compact is not None:
        pieces, remaining = [], limit
        for fragment in reversed(compact):
            if remaining <= 0:
                break
            length = min(remaining, fragment.length)
            with fragment.archive.open("rb") as stream:
                stream.seek(fragment.offset + fragment.length - length)
                pieces.append(stream.read(length))
            remaining -= length
        return b"".join(reversed(pieces)).decode("utf-8", errors="replace")
    try:
        with path.open("rb") as stream:
            stream.seek(0, io.SEEK_END)
            stream.seek(max(0, stream.tell() - limit))
            return stream.read().decode("utf-8", errors="replace")
    except FileNotFoundError:
        fragments = stream_fragments(path)
        if fragments:
            pieces = []
            remaining = limit
            for fragment in reversed(fragments):
                if remaining <= 0:
                    break
                length = min(remaining, fragment.length)
                with fragment.archive.open("rb") as stream:
                    stream.seek(fragment.offset + fragment.length - length)
                    pieces.append(stream.read(length))
                remaining -= length
            return b"".join(reversed(pieces)).decode("utf-8", errors="replace")
        fragment = archived_log(path)
        if fragment is None:
            return ""
        length = min(limit, fragment.length)
        with fragment.archive.open("rb") as stream:
            stream.seek(fragment.offset + fragment.length - length)
            return stream.read(length).decode("utf-8", errors="replace")
