# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Serialize closed task handoffs in the existing target Python environment.

This process NEVER publishes facts.json. It closes, validates and hashes an
exclusive candidate. The Bash controller alone checks its receipt and renames
the candidate. No worker or reporting process calls this module to publish.
"""
from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
import os
from pathlib import Path
import re
import sys

IDENTITY = ("group", "stage_id", "phase", "token")
TASK_FIELDS = ["stage_id", "phase", "item", "iteration", "token", "status",
               "exit_code", "cleanup", "started_epoch", "ended_epoch",
               "reason_code", "log", "execution_exit_code"]
TERMINAL = {"PASS", "WARN", "FAIL", "NA", "TIMEOUT", "CANCELLED", "BLOCKED"}
TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{15,95}\Z")
CHUNK = 65536


def tsv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream, delimiter="\t")
        fields = reader.fieldnames or []
        if len(fields) != len(set(fields)) or any(not key for key in fields):
            raise ValueError(f"invalid handoff header: {path}")
        result = list(reader)
        if any(None in row or any(value is None for value in row.values()) for row in result):
            raise ValueError(f"invalid handoff row: {path}")
        return result


def one(path: Path) -> dict:
    records = tsv(path)
    if len(records) != 1:
        raise ValueError(f"expected exactly one handoff: {path}")
    return records[0]


def keyvalues(path: Path) -> dict:
    result = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        key, value = line.split("\t", 1)
        if key in result:
            raise ValueError(f"duplicate identity key: {path}")
        result[key] = value
    return result


def checked(record: dict, identity: dict, path: Path) -> dict:
    if any(str(record.get(key, "")) != identity[key] for key in IDENTITY):
        raise ValueError(f"stale or foreign handoff: {path}")
    return record


def records(data: dict, name: str) -> list[dict]:
    table = data.get("tables", {}).get(name)
    if table is None:
        return []
    fields, values = table["fields"], table["rows"]
    if (not isinstance(fields, list) or not all(isinstance(x, str) for x in fields)
            or len(fields) != len(set(fields)) or not isinstance(values, list)
            or any(not isinstance(row, list) or len(row) != len(fields) for row in values)):
        raise ValueError(f"invalid facts table: {name}")
    return [dict(zip(fields, row)) for row in values]


def set_table(data: dict, name: str, fields: list[str], values: list[dict]):
    data.setdefault("tables", {})[name] = {"fields": fields,
        "rows": [[row.get(key, "") for key in fields] for row in values]}


def add_table(data: dict, name: str, fields: list[str], values: list[dict]):
    previous = records(data, name)
    set_table(data, name, fields, previous + values)


def digest_range(path: Path, offset: int, length: int) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        stream.seek(offset)
        while length:
            block = stream.read(min(CHUNK, length))
            if not block:
                raise ValueError(f"truncated log range: {path}")
            digest.update(block)
            length -= len(block)
    return digest.hexdigest()


def logical_prefix(identity: dict) -> str:
    if identity["phase"] == "prepare":
        return "prepared/" + identity["group"]
    return f"stages/{identity['stage_id']}/groups/{identity['group']}"


def log_frames(archive: Path, start: int, end: int, identity: dict, runtime: Path):
    """Read bounded headers and chunks; bodies are opaque even if marker-like."""
    prefix = b"[HCU-LOG-CHUNK " + identity["token"].encode("ascii") + b"]\t"
    generic = b"[HCU-LOG-CHUNK "
    result = []
    with archive.open("rb") as stream:
        stream.seek(start)
        at_line_start = True
        while stream.tell() < end:
            line = stream.readline(min(8192, end - stream.tell()))
            if not line:
                raise ValueError("truncated log while scanning frames")
            if at_line_start and line.startswith(generic):
                if not line.startswith(prefix) or not line.endswith(b"\n"):
                    raise ValueError("foreign or malformed log frame")
                fields = line[len(prefix):-1].split(b"\t")
                if len(fields) != 3 or not fields[1].isdigit() or not re.fullmatch(b"[0-9a-f]{64}", fields[2]):
                    raise ValueError("invalid log frame header")
                source = base64.b64decode(fields[0], validate=True).decode("utf-8")
                if source.startswith("runtime/" + identity["group"] + "/"):
                    source = logical_prefix(identity) + source[len("runtime/" + identity["group"]):]
                if (not source.startswith(logical_prefix(identity) + "/")
                        or ".." in Path(source).parts or any(c in source for c in "\t\r\n\\")):
                    raise ValueError("cross-task or unsafe log frame source")
                length, offset = int(fields[1]), stream.tell()
                if offset + length > end:
                    raise ValueError("incomplete log frame body")
                digest = hashlib.sha256()
                remaining = length
                while remaining:
                    block = stream.read(min(CHUNK, remaining))
                    if not block:
                        raise ValueError("truncated log frame body")
                    digest.update(block)
                    remaining -= len(block)
                if digest.hexdigest() != fields[2].decode("ascii"):
                    raise ValueError("log frame checksum mismatch")
                result.append(dict(stage_id=identity["stage_id"], token=identity["token"],
                                   sources=[source], offset=offset, length=length,
                                   sha256=digest.hexdigest()))
                # A body may finish at any byte. The next producer header starts
                # with its own newline, so continue scanning without buffering it.
                at_line_start = False if length else True
            else:
                at_line_start = line.endswith(b"\n")
    return result


def normalize(value, runtime: Path, logical: Path):
    if isinstance(value, dict):
        return {key: normalize(item, runtime, logical) for key, item in value.items()
                if key.lower() not in {"stdout", "stderr", "stdout_text", "stderr_text",
                                       "stdout_tail", "stderr_tail", "captured_stdout", "captured_stderr"}}
    if isinstance(value, list):
        return [normalize(item, runtime, logical) for item in value]
    if isinstance(value, str) and (value == str(runtime) or value.startswith(str(runtime) + "/")):
        return str(logical) + value[len(str(runtime)):]
    return value


def finish(data: dict, args, identity: dict, nodes: list[str], operation: str) -> tuple[dict, bool]:
    if args.runtime_dir.is_symlink():
        raise ValueError("runtime workspace is a symlink")
    runtime = args.runtime_dir.resolve()
    if runtime != args.run_dir / "runtime" / args.group:
        raise ValueError("invalid runtime workspace")
    owner = keyvalues(runtime / "identity.tsv")
    checked(owner, identity, runtime / "identity.tsv")
    task = checked(one(runtime / "task.tsv"), identity, runtime / "task.tsv")
    if task.get("item") != args.item or str(task.get("iteration")) != str(args.iteration):
        raise ValueError("task item/iteration mismatch")
    if task.get("status") not in TERMINAL - {"BLOCKED"}:
        raise ValueError("task handoff is not terminal")
    delivery = checked(one(runtime / "delivery.tsv"), identity, runtime / "delivery.tsv")
    for key in ("worker_joined", "watchdog_joined", "cleanup_joined", "collector_joined"):
        if delivery.get(key) != "1":
            raise ValueError(f"writer has not been joined: {key}")
    collector = checked(one(runtime / "collector-result.tsv"), identity, runtime / "collector-result.tsv")
    if collector.get("exit_code") != "0" or collector.get("drained") != "1":
        raise ValueError("log collector failed or did not drain")
    worker = checked(one(runtime / "worker-result.tsv"), identity, runtime / "worker-result.tsv")
    if (not re.fullmatch(r"[1-9][0-9]*", worker.get("worker_pid", ""))
            or worker.get("completed") != "1"
            or not re.fullmatch(r"-?[0-9]+", worker.get("exit_code", ""))):
        raise ValueError("invalid worker owner")
    # A killed worker can have a different wrapper rc. Its raw payload result
    # must still be retained, and cleanup confirmation is checked independently.
    if task.get("execution_exit_code", "") != worker.get("execution_exit_code", ""):
        raise ValueError("worker/task execution result mismatch")
    node_rows = [checked(row, identity, runtime / "node-results.tsv") for row in tsv(runtime / "node-results.tsv")]
    if [row.get("node") for row in node_rows] != nodes:
        raise ValueError("missing, duplicate or reordered node results")
    prefix = logical_prefix(identity)
    logical = Path(prefix)
    for row in node_rows:
        if not re.fullmatch(r"-?[0-9]+", row.get("exit_code", "")):
            raise ValueError("invalid node exit code")
        row["log"] = str(logical / (row["node"] + ".log"))
    add_table(data, "nodes", ["stage_id", "token", "node", "exit_code", "cleanup", "log"], node_rows)
    cleanup_rows = []
    for directory in sorted(runtime.glob("cleanup-*-*")):
        if not directory.is_dir() or directory.is_symlink():
            raise ValueError("unsafe cleanup attempt")
        result = checked(one(directory / "result.tsv"), identity, directory / "result.tsv")
        source, attempt = result.get("source"), result.get("attempt")
        if source not in {"normal", "fallback"} or not str(attempt).isdigit() or int(attempt) < 1 or result.get("completed") != "1":
            raise ValueError("unclosed cleanup attempt")
        cleanup = tsv(directory / "nodes.tsv")
        if [row.get("node") for row in cleanup] != nodes:
            raise ValueError("cleanup node set/order mismatch")
        for row in cleanup:
            checked(row, identity, directory / "nodes.tsv")
            if row.get("source") != source or row.get("attempt") != attempt:
                raise ValueError("cleanup source mismatch")
            evidence = Path(row["evidence"]).resolve()
            if evidence.parent != directory.resolve() or evidence.is_symlink():
                raise ValueError("cleanup evidence escaped its attempt")
            guards = []
            if evidence.is_file():
                for line in evidence.read_text(encoding="utf-8", errors="replace").splitlines():
                    if line.startswith("__HCU_TASK_STOP__="):
                        guards.append(json.loads(line.split("=", 1)[1]))
            guard = guards[0] if len(guards) == 1 else None
            if guard is not None and not isinstance(guard, dict):
                raise ValueError("cleanup guard receipt is not an object")
            if guard is not None and guard.get("token") != identity["token"]:
                raise ValueError("foreign cleanup token")
            confirmed = (row.get("exit_code") == "0" and guard is not None
                         and guard.get("status") == "STOPPED" and guard.get("remaining") == 0
                         and guard.get("tombstone") is True and not guard.get("uncertain", False))
            if (row.get("status") == "CONFIRMED") != confirmed:
                raise ValueError("cleanup status lacks matching guard evidence")
            row["guard"] = guard
            row["evidence"] = str(logical / directory.name / evidence.name)
            cleanup_rows.append(row)
    if not cleanup_rows:
        raise ValueError("missing cleanup attempt")
    add_table(data, "cleanup", ["stage_id", "token", "node", "source", "attempt", "status", "exit_code", "evidence", "guard"], cleanup_rows)
    if task.get("cleanup") == "CONFIRMED":
        if any(row.get("cleanup") != "CONFIRMED" for row in node_rows):
            raise ValueError("task/node cleanup mismatch")
        selected = [row for row in cleanup_rows if row["source"] == ("normal" if any(r["source"] == "normal" for r in cleanup_rows) else "fallback")]
        if any(row["status"] != "CONFIRMED" for row in selected):
            raise ValueError("task/attempt cleanup mismatch")
    timeout_rows = []
    for path in sorted(runtime.glob("timeout.*.tsv")):
        row = checked(one(path), identity, path)
        if row.get("source") not in {"deadline", "watchdog"} or row.get("expired") != "1":
            raise ValueError("invalid timeout witness")
        if row.get("node") not in nodes + ["-"]:
            raise ValueError("foreign timeout node")
        timeout_rows.append(row)
    add_table(data, "timeout", ["stage_id", "token", "node", "source", "exit_code", "expired"], timeout_rows)
    archive = args.run_dir.parent / (args.group + ".log")
    start, end = int(collector["start_offset"]), int(collector["end_offset"])
    if start < 0 or end < start or end > archive.stat().st_size:
        raise ValueError("invalid drained log range")
    aliases = [checked(row, identity, runtime / "logs.tsv") for row in tsv(runtime / "logs.tsv")]
    if not aliases:
        raise ValueError("missing closed log index")
    unique = {}
    for row in aliases:
        offset, length = int(row["offset"]), int(row["length"])
        source = row["source"]
        if (not source.startswith(prefix + "/") or ".." in Path(source).parts
                or offset < start or offset + length > end):
            raise ValueError("cross-task log index")
        if digest_range(archive, offset, length) != row["sha256"]:
            raise ValueError("closed log checksum mismatch")
        unique.setdefault((offset, length, row["sha256"]), []).append(source)
    logs = [dict(stage_id=args.stage_id, token=args.token, sources=sources,
                 offset=offset, length=length, sha256=checksum)
            for (offset, length, checksum), sources in unique.items()]
    logs += log_frames(archive, start, end, identity, runtime)
    add_table(data, "logs", ["stage_id", "token", "sources", "offset", "length", "sha256"], logs)
    execution = runtime / "rccl" / "execution.tsv"
    if execution.is_file():
        rows = [checked(row, identity, execution) for row in tsv(execution)]
        if not rows:
            with execution.open(encoding="utf-8-sig", newline="") as stream:
                fields = next(csv.reader(stream, delimiter="\t"), [])
            raw_code = worker.get("execution_exit_code", "")
            # The timeout can stop the payload after it writes its header but
            # before it records a measurement. Preserve that absence and all
            # earlier execution rows after the closed-writer checks above.
            if (operation != "rccl" or args.phase != "execute" or task.get("status") != "TIMEOUT"
                    or fields != [*IDENTITY, "test", "exit_code", "elapsed_seconds", "log"]
                    or not re.fullmatch(r"-?[0-9]+", raw_code) or int(raw_code) == 0):
                raise ValueError("RCCL empty execution handoff is not a valid timeout")
        elif len(rows) != 1 or rows[0].get("test") != args.item:
            raise ValueError("RCCL execution item mismatch")
        else:
            rows[0]["log"] = str(logical / "rccl" / (args.item + ".log"))
            add_table(data, "execution", ["stage_id", "token", "test", "exit_code", "elapsed_seconds", "log"], rows)
    if args.phase == "prepare":
        selected = []
        for node in nodes:
            path = runtime / "devices" / (node + ".json")
            if not path.is_file():
                continue
            record = json.loads(path.read_text(encoding="utf-8"))
            if record.get("node") != node or record.get("schema_version") != 1:
                raise ValueError("foreign device selection")
            checked(record.get("handoff", {}), identity, path)
            selected.append(record)
        if selected:
            set_table(data, "devices", ["node", "gpus_per_node", "hip_visible_devices", "pci_devices"], selected)
        plan = runtime / "rccl-plan.tsv"
        if plan.is_file():
            rows = tsv(plan)
            set_table(data, "rccl_plan", ["test", "np", "baseline_busbw_gbps", "margin_pct"], rows)
        if task["status"] in {"PASS", "WARN"} and operation in {"rccl", "deepep"} and len(selected) != len(nodes):
            raise ValueError("successful GPU preflight lacks device selection")
        if task["status"] == "PASS" and operation == "rccl" and len(records(data, "rccl_plan")) != 10:
            raise ValueError("successful RCCL preflight lacks full ten-item plan")
    ib = runtime / "ib" / "ib-write-bw-result.json"
    if ib.is_file():
        metadata = json.loads(ib.read_text(encoding="utf-8"))
        if metadata.get("run_token") != args.token or metadata.get("selected_nodes") != nodes:
            raise ValueError("foreign IB metadata")
        add_table(data, "ib", ["stage_id", "token", "metadata"],
                  [dict(stage_id=args.stage_id, token=args.token, metadata=normalize(metadata, runtime, logical))])
    task["log"] = str(logical / "group.log")
    recycle = task.get("cleanup") == "CONFIRMED" and task.get("reason_code") != "LOG_WRITE_FAILED"
    return task, recycle


def candidate(args):
    args.run_dir = args.run_dir.resolve()
    if (args.run_dir.name != ".state" or not re.fullmatch(r"group-[0-9]+", args.group)
            or not TOKEN.fullmatch(args.token) or args.phase not in {"prepare", "execute"}
            or not re.fullmatch(r"[A-Za-z0-9_,-]+", args.stage_id)):
        raise ValueError("invalid candidate identity")
    groups = tsv(args.run_dir / "groups.tsv")
    group = next((row for row in groups if row["group"] == args.group), None)
    if group is None:
        raise ValueError("group not in run plan")
    if args.phase == "prepare" and args.stage_id != "preflight":
        raise ValueError("invalid preflight identity")
    if args.phase == "execute":
        plan = next((row for row in tsv(args.run_dir / "stages.tsv") if row["stage_id"] == args.stage_id), None)
        if plan is None or plan["item"] != args.item or str(plan["iteration"]) != str(args.iteration):
            raise ValueError("task not in stage plan")
    directory = args.run_dir / "prepared" / args.group
    if args.candidate.is_symlink():
        raise ValueError("candidate is a symlink")
    destination = args.candidate.resolve()
    if destination.parent != directory.resolve() or not destination.name.startswith(".facts-candidate."):
        raise ValueError("candidate must be uniquely named in prepared group")
    published = directory / "facts.json"
    data = json.loads(published.read_text(encoding="utf-8")) if published.is_file() else {"schema_version": 2, "group": args.group, "revision": 0, "tables": {}}
    if data.get("schema_version") != 2 or data.get("group") != args.group or data.get("revision") != args.expected_revision:
        raise ValueError("facts revision/group/schema mismatch")
    for name in data.get("tables", {}):
        records(data, name)
    identity = dict(group=args.group, stage_id=args.stage_id, phase=args.phase, token=args.token)
    tasks = records(data, "tasks")
    previous = [row for row in tasks if row["stage_id"] == args.stage_id]
    if len(previous) > 1 or (previous and (previous[0]["status"] != "RUNNING" or previous[0]["token"] != args.token)):
        raise ValueError("immutable terminal task or mismatched token")
    if args.kind == "finish" and not previous:
        raise ValueError("finish requires a published RUNNING task with this token")
    task = dict(identity, item=args.item, iteration=str(args.iteration), status="RUNNING", exit_code="", cleanup="NOT_STARTED",
                started_epoch=str(args.started_epoch), ended_epoch="", reason_code="RUNNING", log=logical_prefix(identity) + "/group.log", execution_exit_code="")
    recycle = False
    if args.kind == "finish":
        info = keyvalues(args.run_dir / "control.tsv")
        task, recycle = finish(data, args, identity, group["nodes"].split(","), info.get("operation", ""))
    elif args.kind == "blocked":
        task.update(status="BLOCKED", cleanup="UNCONFIRMED", started_epoch="", reason_code=args.reason)
    tasks = [row for row in tasks if row["stage_id"] != args.stage_id] + [task]
    set_table(data, "tasks", TASK_FIELDS, tasks)
    data["revision"] += 1
    # Candidate and readers enforce exactly the same versioned protocol.
    from .reporting.facts import GroupFacts
    GroupFacts(destination, data)
    payload = (json.dumps(data, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")
    with destination.open("xb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os.chmod(destination, 0o644)
    if json.loads(destination.read_bytes()) != data:
        raise ValueError("candidate readback failed")
    receipt = dict(identity, expected_revision=args.expected_revision, revision=data["revision"],
                   sha256=hashlib.sha256(payload).hexdigest(), status=task["status"],
                   exit_code=task.get("exit_code") or "-", cleanup=task["cleanup"],
                   recyclable=int(recycle), reason_code=task["reason_code"])
    for key, value in receipt.items():
        print(f"{key}\t{value}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--group", required=True)
    parser.add_argument("--stage-id", required=True)
    parser.add_argument("--phase", required=True)
    parser.add_argument("--item", default="")
    parser.add_argument("--iteration", default="0")
    parser.add_argument("--token", required=True)
    parser.add_argument("--runtime-dir", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--expected-revision", type=int, required=True)
    parser.add_argument("--kind", choices=("start", "finish", "blocked"), required=True)
    parser.add_argument("--started-epoch", type=int, default=0)
    parser.add_argument("--reason", default="CLEANUP_UNCONFIRMED")
    candidate(parser.parse_args())


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"facts candidate rejected: {error}", file=sys.stderr)
        raise SystemExit(73)
