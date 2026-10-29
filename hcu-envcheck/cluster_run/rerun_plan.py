#!/usr/bin/env python3
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""T29: validate a T28 export list and emit it as TSV for controller.sh.

The controller (bash, entry host) consumes this on stdout:
    ENV <key> <value>            envelope fields (scenario/operation/scale/...)
    GROUP <group> <order> <members-csv>
    STAGE <stage_id> <item> <iteration>
    CTX <nodes-csv> <reason>     non-participating context (never tasks)
    UNCONFIRMED <count>          entries whose cleanup is not CONFIRMED

Rejections (exit 2..11) keep the original plan authoritative: fingerprint
must still match its source run, the deployed reporting code must match the
export's deployment stamp, and the entry grid must be group-complete (every
listed group reruns every listed stage) so the new run's plan is exactly the
exported list — no regrouping, no node substitution, no silent extras.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hcu_envcheck.reporting.export import SCHEMA_VERSION, _config_fingerprint, _deployment_stamp  # noqa: E402
from hcu_envcheck.reporting.scale import resolve_scale  # noqa: E402
from hcu_envcheck.reporting.model import control as run_control  # noqa: E402


def fail(code: int, message: str) -> "int":
    sys.stderr.write(f"[RERUN-REJECT] {message}\n")
    return code


def read_control(source: Path) -> dict:
    for candidate in (source / ".state" / "control.tsv", source / "control.tsv"):
        if candidate.is_file():
            return run_control(candidate.parent)
    return {}


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate a T28 export list for rerun")
    parser.add_argument("--plan", required=True)
    parser.add_argument("--operation", required=True)
    args = parser.parse_args()
    try:
        data = json.loads(Path(args.plan).read_text(encoding="utf-8"))
    except OSError as exc:
        return fail(2, f"cannot read plan file: {exc}")
    except json.JSONDecodeError as exc:
        return fail(2, f"plan is not valid JSON: {exc}")
    if not isinstance(data, dict):
        return fail(2, f"plan top level must be a JSON object, got {type(data).__name__}")
    if data.get("schema_version") not in {1, SCHEMA_VERSION}:
        return fail(2, f"unsupported schema_version: {data.get('schema_version')!r}")
    if data.get("operation") != args.operation:
        return fail(3, f"operation mismatch: plan={data.get('operation')!r} invocation={args.operation!r}")
    entries = data.get("entries") or []
    if not isinstance(entries, list):
        return fail(2, f"plan entries must be a JSON array, got {type(entries).__name__}")
    if not entries:
        return fail(7, "plan has no entries; nothing to rerun")

    source = Path(str(data.get("source_run") or ""))
    control = read_control(source)
    if not control:
        return fail(4, f"source run control.tsv not found under {source}")
    if _config_fingerprint(control) != data.get("config_fingerprint"):
        return fail(5, "plan fingerprint does not match its source run (export stale or edited)")
    stamp = _deployment_stamp()
    deployment = data.get("deployment") or {}
    if not isinstance(deployment, dict):
        return fail(2, "plan deployment must be a JSON object")
    if stamp["reporting_md5"] != deployment.get("reporting_md5"):
        return fail(6, "deployment changed since the export; re-export the plan with the current code")
    scale = data.get("scale") or {}
    if not isinstance(scale, dict):
        return fail(2, "plan scale must be a JSON object")
    keys = ("group_size", "gpus_per_node") if control.get("gpus_per_node") and args.operation in {"rccl", "deepep"} else ("group_size", "slots")
    for key in keys:
        if str(control.get(key, "")) != str(scale.get(key, "")):
            return fail(8, f"scale drift on {key}: source run and plan disagree")

    groups: dict[str, dict] = {}
    stage_keys: list[tuple] = []
    cells: set[tuple] = set()
    stage_identity: dict[str, tuple[str, str]] = {}
    stage_cells: set[tuple[str, str]] = set()
    single_run_items: set[tuple[str, str]] = set()
    unconfirmed = 0
    for entry in entries:
        if not isinstance(entry, dict):
            return fail(2, f"entry is not a JSON object: {entry!r}")
        group = entry.get("group") or ""
        members = entry.get("members", [])
        if not isinstance(members, list):
            return fail(9, "entry members must be a JSON array")
        members = [m for m in members if m]
        if not group or not members:
            return fail(9, f"entry missing group or members: stage={entry.get('stage_id')!r}")
        if args.operation in {"rccl", "deepep"} and (not re.fullmatch(r"group-[0-9]+", group)
                or len(set(members)) != len(members)
                or any(not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", member) for member in members)):
            return fail(9, "invalid GPU group identifier or members")
        known = groups.setdefault(group, {"order": entry.get("group_order", 0), "members": members})
        if known["members"] != members:
            return fail(9, f"inconsistent membership for {group}: {known['members']} vs {members}")
        if entry.get("cleanup") != "CONFIRMED":
            unconfirmed += 1
        if entry.get("kind") == "stage":
            stage_id, item, iteration = (str(entry.get("stage_id")), str(entry.get("item")),
                                         str(entry.get("iteration", "")))
            identity = (item, iteration)
            if stage_id in stage_identity and stage_identity[stage_id] != identity:
                return fail(9, f"conflicting item or iteration for stage {stage_id}")
            stage_identity[stage_id] = identity
            if (stage_id, group) in stage_cells:
                return fail(9, f"duplicate stage entry: {stage_id} in {group}")
            stage_cells.add((stage_id, group))
            if args.operation in {"deepep", "byte-flux"}:
                if (group, item) in single_run_items:
                    return fail(11, f"multiple executions of {item} in {group} are no longer supported; "
                                    "select a single original stage per item for rerun")
                single_run_items.add((group, item))
            key = (stage_id, item, iteration)
            if key not in stage_keys:
                stage_keys.append(key)
            cells.add((key, group))
    ordered_groups = sorted(groups, key=lambda g: (groups[g]["order"], g))
    normalized_scale = None
    if args.operation in {"rccl", "deepep"}:
        root = source / ".state" if (source / ".state" / "control.tsv").is_file() else source
        try:
            normalized_scale = resolve_scale(control, root,
                [{"group": name, "nodes": ",".join(groups[name]["members"])} for name in ordered_groups])
            if any(len(groups[name]["members"]) != normalized_scale["group_size"] for name in ordered_groups):
                return fail(8, "source plan has a partial group; refusing to change group size")
            if control.get("gpus_per_node"):
                for key, value in normalized_scale.items():
                    if str(scale.get(key, "")) != str(value):
                        return fail(8, f"scale drift on {key}: derived scale and plan disagree")
        except (OSError, ValueError) as exc:
            return fail(8, f"cannot determine GPU scale without guessing: {exc}")
    # Group-complete grid: the rerun plan must cover every listed stage for
    # every listed group, so the new run schedules exactly the exported list.
    for key in stage_keys:
        for group in ordered_groups:
            if (key, group) not in cells:
                return fail(10, f"plan mixes per-group stage subsets (missing {key[0]} for {group}); "
                                "rerun per group instead of rescheduling others")

    context = data.get("non_participating_context") or {}
    ctx_nodes = [n for n in context.get("nodes", []) if n]
    for node in ctx_nodes:
        for group in ordered_groups:
            if node in groups[group]["members"]:
                return fail(9, f"non-participating node {node} appears as a member of {group}")
    out = sys.stdout
    for key in ("scenario", "operation"):
        out.write(f"ENV\t{key}\t{control.get(key, '')}\n")
    if normalized_scale is not None:
        for key in ("group_size", "gpus_per_node"):
            out.write(f"ENV\t{key}\t{normalized_scale[key]}\n")
    else:
        for key in ("group_size", "slots"):
            out.write(f"ENV\t{key}\t{control.get(key, '')}\n")
    out.write(f"ENV\tsource_run\t{source}\n")
    out.write(f"ENV\tfingerprint\t{data.get('config_fingerprint', '')}\n")
    out.write(f"ENV\tdeployment_md5\t{stamp['reporting_md5']}\n")
    out.write(f"ENV\tsource_status\t{control.get('status', '')}\n")
    for group in ordered_groups:
        out.write(f"GROUP\t{group}\t{groups[group]['order']}\t{','.join(groups[group]['members'])}\n")
    for stage_id, item, iteration in stage_keys:
        out.write(f"STAGE\t{stage_id}\t{item}\t{iteration}\n")
    out.write(f"CTX\t{','.join(ctx_nodes)}\t{context.get('reason', '')}\n")
    out.write(f"UNCONFIRMED\t{unconfirmed}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
