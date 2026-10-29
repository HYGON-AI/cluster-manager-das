# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Snapshot aggregation and coverage accounting from execution facts."""
from __future__ import annotations

from collections import Counter
from copy import deepcopy
from pathlib import Path

from cluster_run.consistency import build_consistency_summary

from .log_store import group_log
from .facts import STATE_LAYOUT, load_group, snapshot_context
from .model import PROTOCOL_VERSION, TERMINAL, VALID_CONCLUSION, IncrementalRows, control, number, reason_category
from .parsers import parse, task_diagnostic


class Run:
    def __init__(self, root: Path):
        self.root = root
        self.cache: dict[tuple, dict] = {}
        # T23 incremental state reads: terminal results were already cached;
        # non-terminal tasks are now re-parsed only when their task.tsv
        # appears/changes (or the final flag flips), so future PENDING stages
        # cost one stat instead of a read+parse per refresh. The row cache
        # serves unchanged plan/fact files from memory and re-reads any file
        # whose (mtime_ns, size) signature changed (including corruption or
        # a restored backup); entries are replaced, never appended.
        self.probe_cache: dict[tuple, tuple[object, dict, bool]] = {}
        self.reader = IncrementalRows()
        self.cache_signatures: dict[tuple, tuple] = {}

    def snapshot(self, final=False) -> dict:
        with snapshot_context():
            return self._snapshot(final)

    def _snapshot(self, final=False) -> dict:
        info = control(self.root)
        stages, groups = self.reader.rows(self.root / "stages.tsv"), self.reader.rows(self.root / "groups.tsv")
        planned_scale = None
        if info.get("operation") in {"rccl", "deepep"} and info.get("gpus_per_node"):
            from .scale import resolve_scale
            planned_scale = resolve_scale(info, self.root, groups)
        stage_ids, group_ids = [x["stage_id"] for x in stages], [x["group"] for x in groups]
        if len(set(stage_ids)) != len(stage_ids) or len(set(group_ids)) != len(group_ids):
            raise ValueError("测试计划包含重复阶段或分组")
        for identifier in stage_ids + group_ids:
            if not identifier or Path(identifier).name != identifier or identifier in {".", ".."} or "/" in identifier or "\\" in identifier:
                raise ValueError("测试计划标识无效")
        results = []
        for stage in stages:
            for group in groups:
                key = stage["stage_id"], group["group"]
                path = self.root / "stages" / stage["stage_id"] / "groups" / group["group"] / "task.tsv"
                signature = IncrementalRows.signature(path)
                result = self.cache.get(key) if self.cache_signatures.get(key) == (signature, final) else None
                if result is None:
                    memo = self.probe_cache.get(key)
                    if memo is not None and memo[0] == signature and memo[2] == final:
                        result = memo[1]
                    else:
                        result = parse(self.root, stage, group, final)
                        if result["status"] in TERMINAL:
                            self.cache[key] = result
                            self.cache_signatures[key] = signature, final
                        else:
                            self.probe_cache[key] = (signature, result, final)
                # Aggregation decorates evidence paths and categories; keep
                # cached parser results and previously returned snapshots intact.
                results.append(deepcopy(result))
        for result in results:
            if planned_scale is not None:
                result["planned_scale"] = planned_scale
            # Orthogonal classification next to the legacy status.
            # valid = a real accepted conclusion (PASS/WARN) with confirmed
            # cleanup; NA, not-executed and interrupted items are never valid.
            result.setdefault("category", reason_category(result["status"], result.get("reason_code")))
            result.setdefault("valid", result["status"] in VALID_CONCLUSION and result.get("cleanup") == "CONFIRMED")
        preflight = []
        for group in groups:
            directory = self.root / "prepared" / group["group"]
            facts = self.reader.rows(directory / "task.tsv")
            if not facts:
                continue
            fact = facts[0]
            evidence, detail = task_diagnostic(self.root, directory, fact, self.reader.rows)
            status = fact.get("status", "INCOMPLETE")
            reason = fact.get("reason_code", "")
            preflight.append(dict(stage_id="preflight", item="环境预检", group=group["group"],
                                  nodes=group.get("nodes", ""), status=status,
                                  exit_code=fact.get("exit_code", ""), cleanup=fact.get("cleanup", ""),
                                  reason_code=reason, detail=("预检通过" if status == "PASS" else detail),
                                  evidence=str(evidence),
                                  category=reason_category(status, reason),
                                  valid=status in VALID_CONCLUSION and fact.get("cleanup") == "CONFIRMED"))
        summaries = []
        for stage in stages:
            selected = [r for r in results if r["stage_id"] == stage["stage_id"]]
            counts = Counter(r["status"] for r in selected)
            summaries.append(dict(stage, planned=len(selected), passed=counts["PASS"], warned=counts["WARN"],
                                  failed=sum(counts[x] for x in ("FAIL", "TIMEOUT")), na=counts["NA"],
                                  incomplete=sum(n for s, n in counts.items() if s not in TERMINAL or s in {"BLOCKED", "CANCELLED"}),
                                  running=counts["RUNNING"], pending=counts["PENDING"],
                                  completed=sum(n for s, n in counts.items() if s in TERMINAL),
                                  status=("FAIL" if any(r["status"] not in {"PASS", "WARN", "NA"} for r in selected)
                                          else "WARN" if counts["WARN"] else "NA" if counts["NA"] == len(selected) else "PASS")))
        failures = []
        precheck_failed = info.get("status") == "PRECHECK_FAILED"
        for result in ([] if precheck_failed else results):
            checks = result.get("checks", [])
            for check in checks:
                if check["status"] != "PASS":
                    reason = {"FAIL": "CHECK_FAILED", "WARN": "CHECK_WARN", "INCOMPLETE": "CHECK_INCOMPLETE"}.get(check["status"], "CHECK_FAILED")
                    failures.append(dict(stage_id=result["stage_id"], item=check["check"], group=result["group"],
                                         nodes=check["node"], status=check["status"], reason_code=reason,
                                         detail=check["detail"], evidence=check["evidence"],
                                         category=reason_category(check["status"], reason)))
            if (result["status"] not in {"PASS", "NA", "PENDING", "RUNNING"}
                    or (result["status"] == "NA" and result["reason_code"] == "CLEANUP_UNCONFIRMED")) \
                    and (not checks or result["reason_code"] not in {"CHECK_FAILED", "CHECK_WARN"}):
                failures.append({k: result[k] for k in ("stage_id", "item", "group", "nodes", "status", "reason_code", "detail", "evidence", "category")})
        failures.extend({k: item[k] for k in ("stage_id", "item", "group", "nodes", "status", "reason_code", "detail", "evidence", "category")}
                        for item in preflight if item["status"] not in {"PASS", "WARN", "NA"})
        complete = bool(stages and groups) and all(r["status"] in {"PASS", "WARN", "NA"} for r in results)
        declared_groups = number(info.get("group_count"))
        if declared_groups is not None and declared_groups != len(groups):
            complete = False
        valid_count = sum(1 for r in results if r.get("valid"))
        executed_count = sum(1 for r in results if r.get("dispatched"))
        # Preflight failures are hard failures even when every dispatched task
        # itself reached a passing status. An unconfirmed cleanup is equally
        # hard, including on NA items where the status itself stays NA.
        preflight_failed = any(p["status"] not in {"PASS", "WARN", "NA"} for p in preflight)
        cleanup_failed = any(r.get("reason_code") == "CLEANUP_UNCONFIRMED" for r in results)
        failed = (not complete) or preflight_failed or cleanup_failed
        # PASS requires a valid conclusion for every planned group item; NA and
        # never-dispatched items cap the overall status at WARN, never PASS.
        overall = ("FAIL" if failed else
                   "WARN" if any(r["status"] == "WARN" for r in results) or valid_count != len(results) else "PASS")
        if info.get("status") in {"CANCELLED", "PRECHECK_FAILED"}:
            overall = "FAIL"
        # Remainder (non-participating) nodes recorded by the planner: kept for
        # traceability only. They carry no task rows, never enter the planned/
        # executed/valid denominators and never downgrade the result alone.
        unparticipated = [node for node in info.get("unparticipated_nodes", "").split(",") if node]
        non_participating = (dict(source_nodes=number(info.get("source_node_count")),
                                  participating_nodes=number(info.get("participating_node_count")),
                                  nodes=unparticipated,
                                  reason=info.get("unparticipated_reason", ""))
                             if unparticipated or info.get("source_node_count") else None)
        # T06: cross-node static configuration consistency for the health-style
        # operations. Records come from the health parsers; PASS maps to READY,
        # FAIL to BLOCKED, anything else leaves the node's conclusion evidence
        # incomplete. Grouping stays pure in cluster_run.consistency: missing
        # collection is listed per node and never reads as equality.
        consistency = None
        if info.get("operation") in {"platform", "resource", "platform,resource"}:
            status_map = {"PASS": "READY", "FAIL": "BLOCKED"}
            records = [{"node": node, "status": status_map.get(result["status"], "INCOMPLETE"), **record}
                       for result in results
                       for node, record in (result.get("environment_records") or {}).items()]
            if records:
                scope = {"platform": "platform-only", "resource": "resource-only"}.get(info["operation"], "platform-and-resource")
                consistency = build_consistency_summary(records, scope=scope)
        if self.root.name == ".state":
            public = Path(info.get("public_run_dir") or str(self.root.parent))
            for record in [*results, *preflight, *failures]:
                group = record.get("group")
                if not group:
                    continue
                archived = group_log(self.root, group)
                legacy = self.root.parent / "logs" / f"{group}.log"
                compact = info.get("state_layout") == STATE_LAYOUT or load_group(self.root, group) is not None
                evidence = (str(public / archived.name) if archived is not None else
                            str(public / "logs" / legacy.name) if not compact and legacy.is_file() else None)
                if evidence is not None:
                    for item in [record, *record.get("checks", [])]:
                        item.setdefault("raw_evidence", item.get("evidence", ""))
                        item["evidence"] = evidence
        # environment_records are adapter input for the consistency core; the
        # published snapshot keeps the comparison summary instead of duplicating
        # full per-node payloads (raw evidence stays in the node logs).
        published = [{k: v for k, v in r.items() if k != "environment_records"} for r in results]
        return {"schema_version": 1, "protocol_version": PROTOCOL_VERSION,
                "control": info, "stages": summaries, "groups": groups, "preflight": preflight,
                "results": published, "failures": failures, "status": overall,
                "planned": len(stages) * len(groups), "completed": sum(r["status"] in TERMINAL for r in results),
                "executed": executed_count, "valid": valid_count,
                "not_executed": len(results) - executed_count,
                "non_participating": non_participating,
                **({"planned_scale": planned_scale} if planned_scale is not None else {}),
                "consistency_summary": consistency,
                "final": final}
