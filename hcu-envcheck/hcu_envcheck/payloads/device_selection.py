#!/usr/bin/env python3
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Select and revalidate devices in the target environment, never the controller.

HIP is queried in fresh processes so visibility changes precede runtime init.
PCI identities ensure a new mask cannot expand or reorder the permitted set.
No kernels, allocations, benchmarks or third-party modules are executed.
"""
from __future__ import annotations

import argparse
import ctypes
import ctypes.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys


def probe() -> list[str]:
    candidates = [ctypes.util.find_library("amdhip64"), "libamdhip64.so"]
    for variable in ("ROCM_PATH", "ROCM_HOME", "HIP_PATH", "DTK_ROOT", "DTK_HOME"):
        if os.environ.get(variable):
            for directory in ("lib", "lib64"):
                candidates.append(str(Path(os.environ[variable]) / directory / "libamdhip64.so"))
    runtime = None
    for candidate in candidates:
        if candidate:
            try:
                runtime = ctypes.CDLL(candidate)
                break
            except OSError:
                pass
    if runtime is None:
        raise ValueError("HIP runtime libamdhip64.so is unavailable in the selected environment")
    runtime.hipInit.argtypes = [ctypes.c_uint]
    runtime.hipGetDeviceCount.argtypes = [ctypes.POINTER(ctypes.c_int)]
    runtime.hipSetDevice.argtypes = [ctypes.c_int]
    runtime.hipDeviceGetPCIBusId.argtypes = [ctypes.c_char_p, ctypes.c_int, ctypes.c_int]
    for function in (runtime.hipInit, runtime.hipGetDeviceCount, runtime.hipSetDevice, runtime.hipDeviceGetPCIBusId):
        function.restype = ctypes.c_int
    def check(code: int, operation: str):
        if code:
            raise ValueError(f"{operation} failed: HIP status={code}")
    check(runtime.hipInit(0), "hipInit")
    count = ctypes.c_int()
    check(runtime.hipGetDeviceCount(ctypes.byref(count)), "hipGetDeviceCount")
    identities = []
    for ordinal in range(count.value):
        check(runtime.hipSetDevice(ordinal), f"hipSetDevice({ordinal})")
        bus = ctypes.create_string_buffer(64)
        check(runtime.hipDeviceGetPCIBusId(bus, len(bus), ordinal), "hipDeviceGetPCIBusId")
        identity = bus.value.decode("ascii").lower()
        if not identity or identity in identities:
            raise ValueError("empty or duplicate physical device identity")
        identities.append(identity)
    return identities


def visible(environment: dict[str, str]) -> list[str]:
    result = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "--probe"],
                            env=environment, capture_output=True, text=True, check=False)
    if result.returncode:
        raise ValueError(result.stderr.strip() or "target HIP visibility query failed")
    data = json.loads(result.stdout)
    if not isinstance(data, list) or any(not isinstance(item, str) for item in data):
        raise ValueError("invalid HIP device inventory")
    return data


def candidates(environment: dict[str, str], available: list[str]) -> list[str]:
    source = next((key for key in ("HIP_VISIBLE_DEVICES", "CUDA_VISIBLE_DEVICES") if key in environment), None)
    if source is None:
        return [str(index) for index in range(len(available))]
    tokens = [token.strip() for token in environment[source].split(",")]
    if any(not re.fullmatch(r"(?:[0-9]+|GPU-[0-9a-fA-F-]+)", token) for token in tokens):
        raise ValueError(f"invalid device list in {source}")
    normalized = [str(int(token)) if token.isdecimal() else token.lower() for token in tokens]
    if len(set(normalized)) != len(normalized):
        raise ValueError(f"duplicate device in {source}")
    if len(tokens) != len(available):
        raise ValueError(f"{source} lists {len(tokens)} devices but HIP sees {len(available)}; invalid or restricted devices")
    return tokens


def selection_record(record: Path, node: str = "") -> dict:
    """Read compact group facts, or a historical standalone selection record.

    The devices path is a locator in compact runs; no per-node JSON is needed.
    An existing facts document is authoritative, including when it is invalid.
    """
    facts_path = record.parent.parent / "facts.json"
    try:
        contents = facts_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        if facts_path.is_symlink():
            raise ValueError("device facts locator is a dangling symlink")
        return json.loads(record.read_text(encoding="utf-8"))
    facts = json.loads(contents)
    group = record.parent.parent.name
    if (record.parent.name != "devices" or record.suffix != ".json"
            or not re.fullmatch(r"group-[0-9]+", group)
            or not isinstance(facts, dict) or type(facts.get("schema_version")) is not int
            or facts["schema_version"] != 2 or facts.get("group") != group
            or type(facts.get("revision")) is not int or facts["revision"] < 1):
        raise ValueError("invalid compact device facts identity")
    selected_node = node or record.stem
    if node and node != record.stem:
        raise ValueError("device facts locator belongs to another node")
    tables = facts.get("tables")
    table = tables.get("devices") if isinstance(tables, dict) else None
    if not isinstance(table, dict):
        raise ValueError("compact group facts have no device selection table")
    fields, rows = table.get("fields"), table.get("rows")
    required = {"node", "gpus_per_node", "hip_visible_devices", "pci_devices"}
    if (not isinstance(fields, list) or any(not isinstance(field, str) for field in fields)
            or len(set(fields)) != len(fields) or not required.issubset(fields)
            or not isinstance(rows, list)):
        raise ValueError("invalid compact device table fields")
    found = None
    seen = set()
    for row in rows:
        if not isinstance(row, list) or len(row) != len(fields):
            raise ValueError("invalid compact device table row")
        item = dict(zip(fields, row))
        member, count = item["node"], item["gpus_per_node"]
        identities, mask = item["pci_devices"], item["hip_visible_devices"]
        if (not isinstance(member, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", member)
                or member in seen or type(count) is not int or count < 1
                or not isinstance(mask, str) or not isinstance(identities, list)
                or len(identities) != count
                or any(not isinstance(identity, str) or not identity for identity in identities)
                or len(set(identities)) != count):
            raise ValueError("invalid or duplicate compact device selection")
        candidates({"HIP_VISIBLE_DEVICES": mask}, identities)
        seen.add(member)
        if member == selected_node:
            found = {"schema_version": 1, **{field: item[field] for field in required}}
    if found is None:
        raise ValueError(f"compact device selection missing for node {selected_node}")
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", nargs="?", choices=("select", "exec"))
    parser.add_argument("--probe", action="store_true")
    parser.add_argument("--count", type=int)
    parser.add_argument("--record", type=Path)
    parser.add_argument("--node", default="")
    # Parse the payload separately: its arguments are opaque argv, never shell text.
    raw = sys.argv[1:]
    split = raw.index("--") if "--" in raw else len(raw)
    args = parser.parse_args(raw[:split])
    command = raw[split + 1:]
    if args.probe:
        print(json.dumps(probe()))
        return 0
    if args.count is None or args.count < 1 or args.record is None:
        parser.error("positive --count and --record are required")
    environment = dict(os.environ)
    allowed = visible(environment)
    choices = candidates(environment, allowed)
    if args.mode == "select":
        if len(allowed) < args.count:
            raise ValueError(f"requested {args.count} GPUs; only {len(allowed)} are visible")
        selected = choices[:args.count]
        selected_environment = dict(environment, HIP_VISIBLE_DEVICES=",".join(selected))
        identities = visible(selected_environment)
        if identities != allowed[:args.count]:
            raise ValueError("selected mask does not match the first permitted physical devices; refusing expansion")
        data = {"schema_version": 1, "node": args.node, "gpus_per_node": args.count,
                "hip_visible_devices": ",".join(selected), "pci_devices": identities}
        if environment.get("HCU_STATE_LAYOUT") == "compact-group-v2":
            handoff = {"group": environment.get("HCU_TASK_GROUP", ""),
                       "stage_id": environment.get("HCU_TASK_STAGE_ID", ""),
                       "phase": environment.get("HCU_TASK_PHASE", ""),
                       "token": environment.get("HCU_LOG_TOKEN", "")}
            if (not re.fullmatch(r"group-[0-9]+", handoff["group"])
                    or handoff["stage_id"] != "preflight" or handoff["phase"] != "prepare"
                    or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{15,95}", handoff["token"])):
                raise ValueError("device selection lacks its preparation identity")
            data["handoff"] = handoff
        args.record.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.record.with_name(args.record.name + f".{os.getpid()}.tmp")
        try:
            temporary.write_text(json.dumps(data, sort_keys=True) + "\n", encoding="utf-8")
            os.replace(temporary, args.record)
        finally:
            temporary.unlink(missing_ok=True)
    elif args.mode == "exec":
        if not command:
            parser.error("exec requires a payload after --")
        data = selection_record(args.record, args.node)
        if data.get("schema_version") != 1 or data.get("gpus_per_node") != args.count:
            raise ValueError("device selection record does not match the requested GPU count")
        if args.node and data.get("node") != args.node:
            raise ValueError("device selection record belongs to another node")
        identities = data.get("pci_devices")
        mask = data.get("hip_visible_devices")
        if (not isinstance(identities, list) or len(identities) != args.count
                or any(not isinstance(identity, str) for identity in identities)
                or len(set(identities)) != args.count or not isinstance(mask, str)):
            raise ValueError("invalid device selection record")
        if any(identity not in allowed for identity in identities):
            raise ValueError("preflight devices are no longer permitted in the execution environment")
        environment["HIP_VISIBLE_DEVICES"] = mask
        if visible(environment) != identities:
            raise ValueError("runtime device visibility differs from the preflight selection")
        print(f"[DEVICE-SELECTION] node={data['node']} count={args.count} HIP_VISIBLE_DEVICES={mask} pci={','.join(identities)}", flush=True)
        os.execvpe(command[0], command, environment)
    else:
        parser.error("select or exec is required")
    print(f"[DEVICE-SELECTION] node={data['node']} count={args.count} HIP_VISIBLE_DEVICES={data['hip_visible_devices']} pci={','.join(data['pci_devices'])}", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        print(f"[DEVICE-SELECTION-ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1)
