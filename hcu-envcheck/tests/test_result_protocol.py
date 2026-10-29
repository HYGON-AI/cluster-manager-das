# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Result protocol contracts: no status combination silently counts as acceptance.

Fixtures are synthetic .state directories; no SSH, Docker or GPU payload is
involved. The protocol-fixture operation isolates generic execution-fact
aggregation from operation-specific health parsers. Explicit platform fixtures
also verify that missing health evidence cannot count as a passing conclusion.
Covered combinations: partial failure, missing evidence, WARN, cancelled run,
cleanup-unconfirmed, timeout, NA-only coverage and preflight failure with
passing tasks.
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from hcu_envcheck.reporting import render
from hcu_envcheck.reporting.aggregate import Run
from hcu_envcheck.reporting.model import (
    PROTOCOL_VERSION,
    REASON_CODES,
    reason_category,
)

GROUPS = [("group-001", "n01,n02"), ("group-002", "n03,n04")]
STAGES = [("01-r1-item", "item", "1", "第1轮"), ("02-r2-item", "item", "2", "第2轮")]
TASK_HEADER = ["status", "exit_code", "cleanup", "started_epoch", "ended_epoch", "reason_code", "log"]
NODE_HEADER = ["node", "exit_code", "cleanup", "log"]
DEFAULT_TASK = ("PASS", "OK", "CONFIRMED", "0", (0, 0))


def write_tsv(path: Path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["\t".join(header)] + ["\t".join(str(cell) for cell in row) for row in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


class ResultProtocolTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def build(self, *, operation="protocol-fixture", control_status="FINISHED", tasks=None, preflight=None, control_extra=None):
        """tasks maps (stage_id, group) -> task spec tuple; None means "never dispatched".

        Task spec: (status, reason_code, cleanup, exit_code, per-node exit codes).
        """
        control = [
            "schema_version\t1", f"operation\t{operation}", "scenario\tper-node-container",
            "started_epoch\t100", "node_count\t4", f"group_count\t{len(GROUPS)}",
            "group_size\t2", "slots\t8", "concurrency\t32",
            "scope\tfull", f"status\t{control_status}", "current_stage\t2",
            f"entry_run_dir\t{self.root}", f"public_run_dir\t{self.root}",
        ]
        control.extend(control_extra or [])
        (self.root / "control.tsv").write_text("\n".join(control) + "\n", encoding="utf-8")
        write_tsv(self.root / "groups.tsv", ["group", "nodes", "hostfile"],
                  [(group, nodes, f"{self.root}/prepared/{group}/hostfile") for group, nodes in GROUPS])
        write_tsv(self.root / "stages.tsv", ["stage_id", "item", "iteration", "label"], STAGES)
        for group, spec in (preflight or {}).items():
            status, reason = spec
            write_tsv(self.root / "prepared" / group / "task.tsv", TASK_HEADER,
                      [(status, "1" if status == "FAIL" else "0", "CONFIRMED", "100", "200", reason, "group.log")])
        for stage_id, _, _, _ in STAGES:
            for group, nodes in GROUPS:
                spec = DEFAULT_TASK if tasks is None else tasks.get((stage_id, group), DEFAULT_TASK)
                if spec is None:
                    continue
                status, reason, cleanup, exit_code, node_codes = spec
                task_dir = self.root / "stages" / stage_id / "groups" / group
                write_tsv(task_dir / "task.tsv", TASK_HEADER,
                          [(status, exit_code, cleanup, "100", "200", reason, "group.log")])
                write_tsv(task_dir / "node-results.tsv", NODE_HEADER,
                          [(node, code, cleanup, "node.log") for node, code in zip(nodes.split(","), node_codes)])
        return Run(self.root).snapshot(final=True)

    @staticmethod
    def cell(snapshot, stage_id, group):
        return next(r for r in snapshot["results"] if r["stage_id"] == stage_id and r["group"] == group)

    def test_all_pass_counts_every_planned_item_valid(self):
        snap = self.build()
        self.assertEqual(snap["status"], "PASS")
        self.assertEqual(snap["protocol_version"], PROTOCOL_VERSION)
        self.assertEqual((snap["planned"], snap["executed"], snap["completed"], snap["valid"], snap["not_executed"]),
                         (4, 4, 4, 4, 0))
        for record in snap["results"]:
            self.assertTrue(record["valid"])
            self.assertEqual(record["category"], "correctness")

    def test_partial_failure_is_fail_and_counts_valid_minority(self):
        snap = self.build(tasks={("01-r1-item", "group-002"): ("FAIL", "EXECUTION_FAILED", "CONFIRMED", "1", (0, 1))})
        self.assertEqual(snap["status"], "FAIL")
        self.assertEqual(snap["valid"], 3)
        record = self.cell(snap, "01-r1-item", "group-002")
        self.assertEqual((record["status"], record["reason_code"], record["category"], record["valid"]),
                         ("FAIL", "EXECUTION_FAILED", "execution", False))
        self.assertTrue(any(f["reason_code"] == "EXECUTION_FAILED" and f["category"] == "execution"
                            for f in snap["failures"]))

    def test_platform_collection_exit_zero_without_payload_is_not_healthy(self):
        snap = self.build(operation="platform")
        self.assertEqual((snap["status"], snap["executed"], snap["completed"], snap["valid"]),
                         ("FAIL", 4, 4, 0))
        for record in snap["results"]:
            self.assertEqual(record["status"], "FAIL")
            self.assertFalse(record["valid"])
            self.assertEqual(record["reason_code"], "PLATFORM_HEALTH_FAILED")
            self.assertEqual({check["node"] for check in record["checks"]},
                             set(record["nodes"].split(",")))
            self.assertTrue(all(check["check"] == "PAYLOAD_PARSE" and check["status"] == "FAIL"
                                for check in record["checks"]))
        self.assertTrue(any(f["item"] == "PAYLOAD_PARSE" for f in snap["failures"]))

    def test_missing_evidence_is_never_valid(self):
        snap = self.build(tasks={("01-r1-item", "group-001"): None})
        record = self.cell(snap, "01-r1-item", "group-001")
        self.assertEqual((record["status"], record["reason_code"]), ("INCOMPLETE", "INCOMPLETE"))
        self.assertFalse(record["dispatched"])
        self.assertFalse(record["valid"])
        self.assertEqual((snap["executed"], snap["not_executed"], snap["valid"]), (3, 1, 3))
        self.assertEqual(snap["status"], "FAIL")

    def test_warn_counts_valid_but_blocks_clean_pass(self):
        snap = self.build(tasks={("01-r1-item", "group-001"): ("WARN", "CHECK_WARN", "CONFIRMED", "0", (0, 0))})
        self.assertEqual(snap["status"], "WARN")
        self.assertEqual(snap["valid"], 4)
        self.assertTrue(self.cell(snap, "01-r1-item", "group-001")["valid"])

    def test_cancelled_run_is_fail_even_with_passing_tasks(self):
        snap = self.build(control_status="CANCELLED")
        self.assertEqual(snap["status"], "FAIL")

    def test_cleanup_unconfirmed_fails_and_classifies_as_cleanup(self):
        snap = self.build(tasks={("01-r1-item", "group-001"): ("PASS", "OK", "UNCONFIRMED", "0", (0, 0))})
        record = self.cell(snap, "01-r1-item", "group-001")
        self.assertEqual((record["reason_code"], record["category"]), ("CLEANUP_UNCONFIRMED", "cleanup"))
        self.assertFalse(record["valid"])
        self.assertEqual(snap["status"], "FAIL")
        self.assertTrue(any(f["category"] == "cleanup" for f in snap["failures"]))

    def test_na_never_yields_pass(self):
        snap = self.build(tasks={("01-r1-item", "group-001"): ("NA", "NA", "CONFIRMED", "0", (0, 0))})
        self.assertFalse(self.cell(snap, "01-r1-item", "group-001")["valid"])
        self.assertEqual((snap["valid"], snap["status"]), (3, "WARN"))
        all_na = {(stage_id, group): ("NA", "NA", "CONFIRMED", "0", (0, 0))
                  for stage_id, _, _, _ in STAGES for group, _ in GROUPS}
        only_na = self.build(tasks=all_na)
        self.assertEqual((only_na["valid"], only_na["status"]), (0, "WARN"))

    def test_na_with_unconfirmed_cleanup_is_flagged_and_fails(self):
        snap = self.build(tasks={("01-r1-item", "group-001"): ("NA", "NA", "UNCONFIRMED", "0", (0, 0))})
        record = self.cell(snap, "01-r1-item", "group-001")
        self.assertEqual((record["status"], record["reason_code"], record["category"]),
                         ("NA", "CLEANUP_UNCONFIRMED", "cleanup"))
        self.assertFalse(record["valid"])
        self.assertTrue(any(f["group"] == "group-001" and f["reason_code"] == "CLEANUP_UNCONFIRMED"
                            and f["category"] == "cleanup" for f in snap["failures"]))
        self.assertEqual(snap["status"], "FAIL")

    def test_blocked_placeholder_is_not_executed(self):
        snap = self.build(tasks={("01-r1-item", "group-001"): ("BLOCKED", "CLEANUP_UNCONFIRMED", "UNCONFIRMED", "", ("", ""))})
        record = self.cell(snap, "01-r1-item", "group-001")
        self.assertEqual(record["status"], "BLOCKED")
        self.assertFalse(record["dispatched"])
        self.assertFalse(record["valid"])
        self.assertEqual((snap["planned"], snap["executed"], snap["completed"], snap["valid"], snap["not_executed"]),
                         (4, 3, 4, 3, 1))
        self.assertEqual(snap["status"], "FAIL")

    def test_non_participating_metadata_stays_outside_denominator(self):
        # Groups n01..n04 (4 participating nodes); the hostfile source had a
        # 5th node left over as remainder, consistent with the plan metadata.
        extra = ["source_node_count\t5", "participating_node_count\t4",
                 "unparticipated_nodes\tn05", "unparticipated_reason\t不足完整组"]
        snap = self.build(control_extra=extra)
        self.assertIsNone(self.build().get("non_participating"))
        remainder = snap["non_participating"]
        self.assertEqual((remainder["source_nodes"], remainder["participating_nodes"],
                          remainder["nodes"], remainder["reason"]),
                         (5, 4, ["n05"], "不足完整组"))
        # Remainder nodes never enter the task denominators and never downgrade.
        self.assertEqual((snap["planned"], snap["executed"], snap["completed"], snap["valid"]), (4, 4, 4, 4))
        self.assertEqual(snap["status"], "PASS")
        render.write(self.root, snap)
        summary = (self.root / "summary.md").read_text(encoding="utf-8")
        self.assertIn("未参与节点（不足完整组）：n05", summary)
        self.assertIn("不计入任务分母", summary)

    def test_timeout_is_execution_failure(self):
        snap = self.build(tasks={("01-r1-item", "group-001"): ("TIMEOUT", "TIMEOUT", "CONFIRMED", "124", (124, 0))})
        record = self.cell(snap, "01-r1-item", "group-001")
        self.assertEqual((record["status"], record["category"], record["valid"]), ("TIMEOUT", "execution", False))
        self.assertEqual(snap["status"], "FAIL")

    def test_preflight_failure_fails_even_when_all_tasks_pass(self):
        snap = self.build(preflight={"group-001": ("FAIL", "EXECUTION_FAILED")})
        self.assertEqual(snap["status"], "FAIL")
        self.assertTrue(any(f["group"] == "group-001" and f["stage_id"] == "preflight" for f in snap["failures"]))

    def test_reason_category_enum_is_total_and_stable(self):
        mapping = {
            "EXECUTION_FAILED": "execution", "INCOMPLETE": "execution", "TIMEOUT": "execution",
            "CANCELLED": "execution", "LOG_WRITE_FAILED": "execution", "CHECK_INCOMPLETE": "execution",
            "CLEANUP_UNCONFIRMED": "cleanup",
            "CHECK_FAILED": "correctness", "CHECK_WARN": "correctness",
            "OK": "correctness", "PASS": "correctness", "NA": "correctness",
            "BELOW_BASELINE": "performance", "BASELINE_MISSING": "performance",
            "METRIC_MISSING": "performance", "STABILITY_FAILED": "performance", "CASE_FAILED": "performance",
            "CONTAINER_ABSENT": "other", "CONTAINER_STOPPED": "other",
            "IMAGE_MISMATCH": "other", "NODE_UNREACHABLE": "other",
        }
        for code, expected in mapping.items():
            self.assertEqual(reason_category("FAIL", code), expected, code)
        self.assertEqual(reason_category("PENDING", ""), "scheduling")
        self.assertEqual(reason_category("RUNNING", "RUNNING"), "scheduling")
        self.assertEqual(reason_category("FAIL", "SOMETHING_NEW"), "other")
        self.assertEqual(set(mapping) | {"RUNNING", "PENDING"}, set(REASON_CODES))

    def test_legacy_fields_are_preserved(self):
        snap = self.build()
        legacy = {"stage_id", "item", "iteration", "group", "nodes", "status", "exit_code", "cleanup",
                  "started_epoch", "ended_epoch", "reason_code", "detail", "evidence", "metrics"}
        for record in snap["results"]:
            self.assertTrue(legacy <= set(record), sorted(legacy - set(record)))
        for key in ("schema_version", "planned", "completed"):
            self.assertIn(key, snap)

    def test_render_keeps_tsv_contract_and_reports_valid_counts(self):
        snap = self.build(tasks={("02-r2-item", "group-001"): ("FAIL", "EXECUTION_FAILED", "CONFIRMED", "1", (1, 0))})
        render.write(self.root, snap)
        summary = (self.root / "summary.md").read_text(encoding="utf-8")
        self.assertIn("计划 / 已执行 / 终态 / 有效结论：4 / 4 / 4 / 3", summary)
        self.assertIn("类别", summary)
        header = (self.root / "failures.tsv").read_text(encoding="utf-8").splitlines()[0]
        self.assertEqual(header.split("\t"), render.FAILURE_FIELDS)


if __name__ == "__main__":
    unittest.main()
