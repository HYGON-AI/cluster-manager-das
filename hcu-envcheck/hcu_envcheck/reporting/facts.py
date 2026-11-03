# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Read compact group facts through the historical logical-path interface.

This module never publishes or repairs execution facts. Once facts.json exists,
it is authoritative, including absent rows; stale TSV files cannot fill gaps.
"""
from __future__ import annotations

import json
import re
import csv
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

SCHEMA_VERSION = 2
STATE_LAYOUT = "compact-group-v2"
GROUP = re.compile(r"group-[0-9]+\Z")
_cache: dict[Path, tuple[tuple, "GroupFacts"]] = {}
_snapshot: ContextVar[dict | None] = ContextVar("compact_facts_snapshot", default=None)


class FactsError(ValueError):
    """Published execution facts are malformed or inconsistent."""


def _bad_constant(value):
    raise FactsError(f"non-finite compact facts value: {value}")


def file_signature(path: Path):
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    return stat.st_mtime_ns, stat.st_ctime_ns, stat.st_size, stat.st_ino


def relative_source(value: object) -> str:
    if not isinstance(value, str) or not value or any(c in value for c in "\t\r\n"):
        raise FactsError("invalid logical log source")
    path = PurePosixPath(value.replace("\\", "/"))
    if path.is_absolute() or ".." in path.parts or ":" in path.parts[0]:
        raise FactsError("logical log source escapes run state")
    return path.as_posix()


def _identity(source: str):
    parts = PurePosixPath(source).parts
    if len(parts) >= 3 and parts[0] == "prepared" and GROUP.fullmatch(parts[1]):
        return parts[1], "preflight", parts[2:]
    if (len(parts) >= 5 and parts[0] == "stages" and parts[2] == "groups"
            and GROUP.fullmatch(parts[3]) and parts[1] not in {".", ".."}):
        return parts[3], parts[1], parts[4:]
    return None


def logical_location(path: Path):
    path = Path(path).absolute()
    state = next((parent for parent in path.parents if parent.name == ".state"), None)
    if state is None:
        return None
    source = path.relative_to(state).as_posix()
    identity = _identity(source)
    if identity is None:
        return None
    group, stage, tail = identity
    return state, group, stage, tail, source


@dataclass(frozen=True)
class FactLogFragment:
    archive: Path
    offset: int
    length: int
    sha256: str


def decode_table(value: object, name: str) -> list[dict]:
    if not isinstance(value, dict):
        raise FactsError(f"invalid facts table: {name}")
    fields, data = value.get("fields"), value.get("rows")
    if (not isinstance(fields, list) or not fields
            or any(not isinstance(field, str) or not field or any(c in field for c in "\t\r\n") for field in fields)
            or len(fields) != len(set(fields)) or not isinstance(data, list)):
        raise FactsError(f"invalid facts table fields/rows: {name}")
    result = []
    for row in data:
        if not isinstance(row, list) or len(row) != len(fields):
            raise FactsError(f"invalid facts row width: {name}")
        result.append(dict(zip(fields, row)))
    return result


class GroupFacts:
    def __init__(self, path: Path, data: object, signature=None):
        if (not isinstance(data, dict) or type(data.get("schema_version")) is not int
                or data["schema_version"] != SCHEMA_VERSION):
            raise FactsError(f"unsupported compact facts schema: {path}")
        if data.get("group") != path.parent.name or not GROUP.fullmatch(path.parent.name):
            raise FactsError(f"compact facts group identity mismatch: {path}")
        if type(data.get("revision")) is not int or data["revision"] < 1:
            raise FactsError(f"invalid compact facts revision: {path}")
        tables = data.get("tables")
        if not isinstance(tables, dict):
            raise FactsError(f"compact facts tables missing: {path}")
        self.path, self.group, self.revision = path, data["group"], data["revision"]
        self.signature = signature
        self.tables = {name: decode_table(value, name) for name, value in tables.items()}
        self._validate()

    def table(self, name: str) -> list[dict]:
        return self.tables.get(name, [])

    def _validate(self):
        tasks = {}
        for row in self.table("tasks"):
            stage, token = row.get("stage_id"), row.get("token")
            if (not isinstance(stage, str) or not stage or stage in {".", ".."}
                    or any(c in stage for c in "/\\\t\r\n")
                    or not isinstance(token, str) or not token
                    or any(c in token for c in "\t\r\n") or stage in tasks):
                raise FactsError("invalid or duplicate compact task identity")
            if not isinstance(row.get("status"), str):
                raise FactsError("compact task status missing")
            tasks[stage] = token
        unique = {
            "nodes": ("stage_id", "node"),
            "cleanup": ("stage_id", "source", "attempt", "node"),
            "execution": ("stage_id", "test"),
            "devices": ("node",),
            "ib": ("stage_id",),
        }
        for name, records in self.tables.items():
            seen = set()
            for row in records:
                if name not in {"tasks", "devices", "rccl_plan"} and "stage_id" in row:
                    if tasks.get(row["stage_id"]) != row.get("token"):
                        raise FactsError(f"compact {name} token/stage mismatch")
                if name in unique:
                    try:
                        key = tuple(row[field] for field in unique[name])
                        if key in seen:
                            raise FactsError(f"duplicate compact {name} identity")
                        seen.add(key)
                    except (KeyError, TypeError) as exc:
                        raise FactsError(f"invalid compact {name} identity") from exc
                if name == "logs":
                    self._validate_log(row, tasks)
                elif name == "devices":
                    count, pci, mask = row.get("gpus_per_node"), row.get("pci_devices"), row.get("hip_visible_devices")
                    if (type(count) is not int or count < 1 or not isinstance(pci, list) or len(pci) != count
                            or any(not isinstance(value, str) or not value for value in pci)
                            or len(set(pci)) != count or not isinstance(mask, str)
                            or len(mask.split(",")) != count or len(set(mask.split(","))) != count):
                        raise FactsError("invalid compact device selection")
                elif name == "cleanup":
                    guard = row.get("guard")
                    if guard is not None and (not isinstance(guard, dict) or guard.get("token") != row.get("token")):
                        raise FactsError("compact cleanup guard token mismatch")
                    if row.get("status") == "CONFIRMED" and (not isinstance(guard, dict)
                            or guard.get("status") != "STOPPED" or guard.get("remaining") != 0
                            or guard.get("tombstone") is not True or guard.get("uncertain") is True
                            or str(row.get("exit_code")) != "0"):
                        raise FactsError("compact cleanup confirmation lacks STOPPED proof")
        self._validate_plan(tasks)

    def _validate_plan(self, tasks):
        state = self.path.parent.parent.parent
        group_plan = state / "groups.tsv"
        if group_plan.is_file():
            with group_plan.open(encoding="utf-8-sig", newline="") as stream:
                matches = [row for row in csv.DictReader(stream, delimiter="\t") if row.get("group") == self.group]
            if len(matches) != 1:
                raise FactsError("compact facts group absent or duplicated in run plan")
            members = set(filter(None, matches[0].get("nodes", "").split(",")))
            for name in ("nodes", "devices", "cleanup", "timeout"):
                for row in self.table(name):
                    node = row.get("node")
                    anonymous_watchdog = (name == "timeout" and row.get("source") == "watchdog" and node in {None, "", "-"})
                    if node not in members and not anonymous_watchdog:
                        raise FactsError(f"compact {name} contains an unplanned node")
        stage_plan = state / "stages.tsv"
        if stage_plan.is_file():
            with stage_plan.open(encoding="utf-8-sig", newline="") as stream:
                stages = {row["stage_id"] for row in csv.DictReader(stream, delimiter="\t")}
            if any(stage != "preflight" and stage not in stages for stage in tasks):
                raise FactsError("compact facts contain an unplanned stage")

    def _validate_log(self, row: dict, tasks: dict):
        if tasks.get(row.get("stage_id")) != row.get("token"):
            raise FactsError("compact log token/stage mismatch")
        if (type(row.get("offset")) is not int or row["offset"] < 0
                or type(row.get("length")) is not int or row["length"] < 0
                or not isinstance(row.get("sha256"), str)
                or not re.fullmatch(r"[0-9a-fA-F]{64}", row["sha256"])):
            raise FactsError("invalid compact log byte range")
        sources = row.get("sources")
        if (not isinstance(sources, list) or not sources or any(not isinstance(s, str) for s in sources)
                or len(sources) != len(set(sources))):
            raise FactsError("invalid compact log sources")
        for source in sources:
            identity = _identity(relative_source(source))
            if identity is None or identity[:2] != (self.group, row["stage_id"]):
                raise FactsError("compact log source crosses group/stage")


@contextmanager
def snapshot_context():
    """Pin each group's published JSON for one report snapshot, without locks."""
    current = _snapshot.get()
    if current is not None:
        yield
        return
    token = _snapshot.set({})
    try:
        yield
    finally:
        _snapshot.reset(token)


def load_group(state: Path, group: str) -> GroupFacts | None:
    if not GROUP.fullmatch(group):
        raise FactsError("invalid compact group identifier")
    path = Path(state).absolute() / "prepared" / group / "facts.json"
    pinned = _snapshot.get()
    if pinned is not None and path in pinned:
        return pinned[path]
    signature = file_signature(path)
    if signature is None:
        result = None
        _cache.pop(path, None)
    elif (entry := _cache.get(path)) is not None and entry[0] == signature:
        result = entry[1]
    else:
        try:
            with path.open(encoding="utf-8") as stream:
                data = json.load(stream, parse_constant=_bad_constant)
                import os
                stat = os.fstat(stream.fileno())
                signature = stat.st_mtime_ns, stat.st_ctime_ns, stat.st_size, stat.st_ino
        except (OSError, ValueError) as exc:
            raise FactsError(f"cannot read compact facts {path}: {exc}") from exc
        result = GroupFacts(path, data, signature)
        _cache[path] = signature, result
    if pinned is not None:
        pinned[path] = result
    return result


def invalidate(path: Path) -> None:
    """Drop any cached compact-facts read for the group owning `path`.

    `load_group`'s cache hit path trusts a pre-open `stat()` (mtime_ns,
    ctime_ns, size, inode) match against the previous read. The write side
    (controller.sh, on the host) and the read side (this reporting process,
    inside the container via docker exec) can observe the same facts.json
    through different filesystem-attribute-cache views, so a `stat()` taken
    moments after a host-side commit can still report the pre-commit
    attributes to the container, matching the cached entry even though the
    file's revision has already advanced. The one read that must be
    authoritative (the final, `final=True` tick) calls this first to force
    a real re-open+fstat on its next `load_group`, rather than trusting that
    `stat()` comparison.
    """
    location = logical_location(path)
    if location is None:
        return
    state, group = location[0], location[1]
    if not GROUP.fullmatch(group):
        return
    facts_path = Path(state).absolute() / "prepared" / group / "facts.json"
    _cache.pop(facts_path, None)
    pinned = _snapshot.get()
    if pinned is not None:
        pinned.pop(facts_path, None)


def read_rows(path: Path) -> list[dict] | None:
    location = logical_location(path)
    if location is None:
        return None
    state, group, stage, tail, _ = location
    facts = load_group(state, group)
    if facts is None:
        return None
    table = {("task.tsv",): "tasks", ("node-results.tsv",): "nodes",
             ("cleanup.tsv",): "cleanup", ("rccl-plan.tsv",): "rccl_plan",
             ("rccl", "execution.tsv"): "execution"}.get(tail)
    if table is None:
        return None
    selected = facts.table(table)
    if table != "rccl_plan":
        selected = [row for row in selected if row.get("stage_id") == stage]
    return [{key: "" if value is None else str(value) for key, value in row.items()
             if key not in {"stage_id", "phase", "item", "iteration"}}
            for row in selected]


def fact_signature(path: Path):
    location = logical_location(path)
    if location is not None:
        facts = load_group(location[0], location[1])
        if facts is not None:
            return (*facts.signature, facts.revision)
    return file_signature(Path(path))


def log_fragments(path: Path) -> list[FactLogFragment] | None:
    location = logical_location(path)
    if location is None:
        return None
    state, group, _, _, source = location
    facts = load_group(state, group)
    if facts is None:
        return None
    archive = state.parent / f"{group}.log"
    fragments = [FactLogFragment(archive, row["offset"], row["length"], row["sha256"].lower())
                 for row in facts.table("logs") if source in row["sources"]]
    fragments.sort(key=lambda fragment: fragment.offset)
    last_end = -1
    for fragment in fragments:
        if fragment.offset < last_end:
            raise FactsError("overlapping compact log fragments")
        last_end = fragment.offset + fragment.length
    return fragments


def allocation_path(state: Path, group: str, *, operation: str | None = None) -> Path:
    if not GROUP.fullmatch(group):
        raise FactsError("invalid allocation group identifier")
    prepared = Path(state) / "prepared" / group
    slotsfile = prepared / "slotsfile"
    info = {}
    try:
        with (Path(state) / "control.tsv").open(encoding="utf-8-sig") as stream:
            info = dict(line.rstrip("\r\n").split("\t", 1) for line in stream if "\t" in line)
    except FileNotFoundError:
        pass
    operation = operation if operation is not None else info.get("operation")
    if operation in {"rccl", "deepep"}:
        compact = info.get("state_layout") == STATE_LAYOUT or load_group(state, group) is not None
        if compact and not slotsfile.is_file():
            raise FactsError(f"compact GPU allocation requires the retained slotsfile: {slotsfile}")
    return slotsfile if slotsfile.is_file() else prepared / "hostfile"
