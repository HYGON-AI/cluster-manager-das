# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Structured export of failed and unfinished planned work (no retries).

T28: --export-plan writes a fixed four-file plan (failed-nodes.hostfile,
failed-groups.json, unfinished-groups.json, cleanup-unconfirmed.json) from a
report-only rebuild of a terminal run. Every entry keeps the original plan
identity — full group membership, item, iteration, scale and evidence — so a
consumer can rerun exactly what was planned without re-deriving groups from
the current hostfile. Remainder (non-participating) nodes are context only and
never enter the lists.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .model import PROTOCOL_VERSION, atomic
from .facts import allocation_path

SCHEMA_VERSION = 2
FAILED_STATUSES = {"FAIL", "TIMEOUT"}
UNFINISHED_STATUSES = {"PENDING", "RUNNING", "INCOMPLETE", "BLOCKED", "CANCELLED"}
# Volatile bookkeeping excluded from the config fingerprint; everything else
# in control.tsv describes the plan configuration.
FINGERPRINT_EXCLUDE = {"started_epoch", "status", "current_stage", "entry_run_dir",
                       "public_run_dir", "report_host"}


def _config_fingerprint(control: dict) -> str:
    digest = hashlib.sha256()
    for key in sorted(k for k in control if k not in FINGERPRINT_EXCLUDE):
        digest.update(f"{key}={control.get(key, '')}\n".encode("utf-8"))
    return digest.hexdigest()[:16]


def _deployment_stamp() -> dict:
    package = Path(__file__).resolve().parent
    digest = hashlib.sha256()
    for path in sorted(package.glob("*.py")):
        digest.update(path.name.encode("utf-8"))
        digest.update(path.read_bytes())
    return {"protocol_version": PROTOCOL_VERSION, "reporting_md5": digest.hexdigest()[:16]}


def _public_run(control: dict, root: Path) -> str:
    if root.name == ".state":
        return str(control.get("public_run_dir") or root.parent)
    return str(root)


def _stage_entry(result: dict, groups_index: dict, envelope: dict) -> dict:
    group = groups_index.get(result["group"], {})
    members = [n for n in str(group.get("nodes", "")).split(",") if n]
    position = groups_index.get(result["group"], {}).get("_order", 0)
    return {"kind": "stage", "stage_id": result["stage_id"], "group": result["group"],
            "group_order": position, "members": members,
            "item": result["item"], "iteration": result.get("iteration", ""),
            "mode": result["item"], "status": result["status"],
            "reason_code": result.get("reason_code", ""), "detail": result.get("detail", ""),
            "cleanup": result.get("cleanup", ""), "evidence": result.get("evidence", ""),
            "exit_code": result.get("exit_code", ""), **{k: envelope[k] for k in
            ("schema_version", "source_run", "operation", "scale", "config_fingerprint", "deployment")}}


def _preflight_entry(fact: dict, groups_index: dict, envelope: dict) -> dict:
    group = groups_index.get(fact["group"], {})
    members = [n for n in str(group.get("nodes", "")).split(",") if n]
    return {"kind": "preflight", "stage_id": "preflight", "group": fact["group"],
            "group_order": groups_index.get(fact["group"], {}).get("_order", 0),
            "members": members, "item": fact.get("item", "环境预检"),
            "iteration": "", "mode": "preflight", "status": fact["status"],
            "reason_code": fact.get("reason_code", ""), "detail": fact.get("detail", ""),
            "cleanup": fact.get("cleanup", ""), "evidence": fact.get("evidence", ""),
            "exit_code": fact.get("exit_code", ""), **{k: envelope[k] for k in
            ("schema_version", "source_run", "operation", "scale", "config_fingerprint", "deployment")}}


def _write_json(path: Path, envelope: dict, entries: list[dict]) -> None:
    atomic(path, json.dumps({**envelope, "entry_count": len(entries), "entries": entries},
                            ensure_ascii=False, indent=2, allow_nan=False) + "\n")


def export_plan(snapshot: dict, root: Path, dest: Path) -> Path:
    if dest.exists():
        raise RuntimeError(f"导出目录已存在，拒绝覆盖：{dest}")
    dest.mkdir(parents=True)
    control = snapshot["control"]
    groups_index = {}
    for order, group in enumerate(snapshot["groups"], start=1):
        groups_index[group["group"]] = {**group, "_order": order}
    new_gpu_scale = control.get("operation") in {"rccl", "deepep"} and bool(control.get("gpus_per_node"))
    envelope = {"schema_version": SCHEMA_VERSION if new_gpu_scale else 1,
                "source_run": _public_run(control, root),
                "operation": control.get("operation", ""),
                "scale": {"node_count": control.get("node_count", ""),
                          "participating_node_count": control.get("participating_node_count", ""),
                          "group_count": control.get("group_count", ""),
                          "group_size": control.get("group_size", ""),
                          "slots": control.get("slots", "")},
                "config_fingerprint": _config_fingerprint(control),
                "deployment": _deployment_stamp(),
                "plan": {"planned": snapshot.get("planned", 0), "executed": snapshot.get("executed", 0),
                         "completed": snapshot.get("completed", 0), "valid": snapshot.get("valid", 0)},
                "non_participating_context": snapshot.get("non_participating") or None}
    if new_gpu_scale:
        from .scale import resolve_scale
        envelope["scale"].pop("slots", None)
        envelope["scale"].update(resolve_scale(control, root, snapshot["groups"]))
        envelope["scale"]["semantics"] = "group-nodes-gpus-v1"
    results = snapshot["results"]
    preflight = snapshot.get("preflight", [])
    failed = [_stage_entry(r, groups_index, envelope) for r in results if r["status"] in FAILED_STATUSES]
    failed += [_preflight_entry(p, groups_index, envelope) for p in preflight if p["status"] == "FAIL"]
    unfinished = [_stage_entry(r, groups_index, envelope) for r in results if r["status"] in UNFINISHED_STATUSES]
    unconfirmed = [_stage_entry(r, groups_index, envelope)
                   for r in results if r.get("cleanup") != "CONFIRMED"]
    unconfirmed += [_preflight_entry(p, groups_index, envelope)
                    for p in preflight if p.get("cleanup") != "CONFIRMED"]
    _write_json(dest / "failed-groups.json", envelope, failed)
    _write_json(dest / "unfinished-groups.json", envelope, unfinished)
    _write_json(dest / "cleanup-unconfirmed.json", envelope, unconfirmed)

    # failed-nodes.hostfile: original hostfile lines of every node in a failed
    # group, in plan order. Lines come from the run's own per-group hostfiles
    # (GPU operations export pure names); groups stay intact and remainder nodes
    # never appear.
    wanted = {node for entry in failed for node in entry["members"]}
    lines: list[str] = []
    seen: set[str] = set()
    for group in snapshot["groups"]:
        hostfile = allocation_path(root, group["group"], operation=control.get("operation"))
        if not hostfile.is_file():
            continue
        text = hostfile.read_text(encoding="utf-8", errors="replace").splitlines()
        for line in text:
            node = line.split()[0] if line.split() else ""
            if node and node in wanted and node not in seen:
                seen.add(node)
                lines.append(node if control.get("operation") in {"rccl", "deepep"} else line)
    atomic(dest / "failed-nodes.hostfile", "".join(f"{line}\n" for line in lines))
    return dest
