# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Compact/legacy evidence equivalence, bounded log reads and report locking.

These fixtures run on the validation server; no GPU or SSH is needed.
"""
from __future__ import annotations

import copy
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import contextmanager, redirect_stderr
from pathlib import Path
from unittest import mock

from cluster_run.rerun_plan import read_control
from hcu_envcheck.group_logs import read_log_text
from hcu_envcheck.reporting import __main__ as reporting_main
from hcu_envcheck.reporting import export, render
from hcu_envcheck.reporting.aggregate import Run
from hcu_envcheck.reporting.facts import FactsError, allocation_path, load_group, snapshot_context
from hcu_envcheck.reporting.log_store import log_size, log_tail, open_log
from hcu_envcheck.reporting.model import IncrementalRows, report_lock, rows
from hcu_envcheck.reporting.scale import resolve_scale
from hcu_envcheck.reporting.progress import Display

GROUP = "group-001"
STAGES = ("01-item-one", "02-item-two")
TASK_FIELDS = ["stage_id", "phase", "item", "iteration", "token", "status", "exit_code", "cleanup",
               "started_epoch", "ended_epoch", "reason_code", "log", "execution_exit_code"]
NODE_FIELDS = ["stage_id", "token", "node", "exit_code", "cleanup", "log"]
LOG_FIELDS = ["stage_id", "token", "sources", "offset", "length", "sha256"]


def write_tsv(path, fields, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    text = ["\t".join(fields)]
    text.extend("\t".join(str(value) for value in record) for record in records)
    path.write_text("\n".join(text) + "\n", encoding="utf-8")


class CompactFactsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.public = Path(self.temporary.name) / "run"
        self.state = self.public / ".state"
        self.prepared = self.state / "prepared" / GROUP
        self.prepared.mkdir(parents=True)
        self.facts_path = self.prepared / "facts.json"
        self.archive = self.public / f"{GROUP}.log"
        self.archive.write_bytes(b"")
        (self.state / "control.tsv").write_text(
            "schema_version\t1\noperation\tprotocol-fixture\nscenario\tper-node-container\n"
            "status\tFINISHED\nstarted_epoch\t100\ngroup_count\t1\ngroup_size\t2\n"
            "node_count\t2\nstate_layout\tcompact-group-v2\nlog_format\tdirect-group-v2\n"
            f"entry_run_dir\t{self.state}\npublic_run_dir\t{self.public}\n", encoding="utf-8")
        write_tsv(self.state / "groups.tsv", ["group", "nodes", "hostfile"],
                  [[GROUP, "n01,n02", str(self.prepared / "slotsfile")]])
        write_tsv(self.state / "stages.tsv", ["stage_id", "item", "iteration", "label"],
                  [[stage, f"item-{index}", "0", f"Item {index}"] for index, stage in enumerate(STAGES, 1)])
        (self.prepared / "slotsfile").write_text("n01 slots=8\nn02 slots=8\n", encoding="utf-8")

    def logical(self, stage, name="group.log"):
        return self.state / "stages" / stage / "groups" / GROUP / name

    def facts(self):
        tasks, nodes = [], []
        for index, stage in enumerate(STAGES, 1):
            token = f"hcu_fixture_token_{index}"
            source = self.logical(stage).relative_to(self.state).as_posix()
            tasks.append([stage, "execute", f"item-{index}", "0", token, "PASS", 0, "CONFIRMED",
                          100, 200, "OK", source, 0])
            for node in ("n01", "n02"):
                node_source = self.logical(stage, node + ".log").relative_to(self.state).as_posix()
                nodes.append([stage, token, node, 0, "CONFIRMED", node_source])
        return {"schema_version": 2, "group": GROUP, "revision": 1, "tables": {
            "tasks": {"fields": TASK_FIELDS, "rows": tasks},
            "nodes": {"fields": NODE_FIELDS, "rows": nodes},
            "logs": {"fields": LOG_FIELDS, "rows": []},
        }}

    def publish(self, data, *, preserve_mtime=False):
        previous = self.facts_path.stat() if preserve_mtime and self.facts_path.exists() else None
        candidate = self.prepared / ".candidate.json"
        candidate.write_text(json.dumps(data, separators=(",", ":"), ensure_ascii=False) + "\n", encoding="utf-8")
        if previous is not None:
            os.utime(candidate, ns=(previous.st_atime_ns, previous.st_mtime_ns))
        os.replace(candidate, self.facts_path)

    def legacy(self, data):
        fields = [field for field in TASK_FIELDS if field not in {"stage_id", "phase", "item", "iteration"}]
        for row in data["tables"]["tasks"]["rows"]:
            record = dict(zip(TASK_FIELDS, row))
            directory = self.logical(record["stage_id"]).parent
            write_tsv(directory / "task.tsv", fields, [[record[field] for field in fields]])
            selected = [dict(zip(NODE_FIELDS, node)) for node in data["tables"]["nodes"]["rows"]
                        if node[0] == record["stage_id"]]
            node_fields = ["node", "exit_code", "cleanup", "log"]
            write_tsv(directory / "node-results.tsv", node_fields,
                      [[node[field] for field in node_fields] for node in selected])

    @staticmethod
    def comparable(snapshot):
        fields = ("stage_id", "item", "iteration", "group", "nodes", "status", "exit_code", "cleanup",
                  "reason_code", "metrics", "dispatched", "valid")
        return ({field: snapshot[field] for field in ("status", "planned", "executed", "completed", "valid", "not_executed")},
                [{field: row[field] for field in fields} for row in snapshot["results"]])

    def test_legacy_and_compact_snapshots_are_equivalent(self):
        data = self.facts()
        self.legacy(data)
        old = Run(self.state).snapshot(final=True)
        self.publish(data)
        new = Run(self.state).snapshot(final=True)
        self.assertEqual(self.comparable(old), self.comparable(new))

    def test_absent_compact_stage_never_falls_back_to_stale_tsv(self):
        data = self.facts()
        self.legacy(data)
        data["tables"]["tasks"]["rows"] = data["tables"]["tasks"]["rows"][:1]
        data["tables"]["nodes"]["rows"] = data["tables"]["nodes"]["rows"][:2]
        self.publish(data)
        self.assertEqual(rows(self.logical(STAGES[1], "task.tsv")), [])
        result = Run(self.state).snapshot(final=False)["results"][1]
        self.assertEqual(result["status"], "PENDING")
        self.assertFalse(result["dispatched"])

    def test_atomic_replacements_refresh_live_and_terminal_cache(self):
        data = self.facts()
        data["tables"]["tasks"]["rows"][0][5] = "RUNNING"
        self.publish(data)
        run = Run(self.state)
        self.assertEqual(run.snapshot()["results"][0]["status"], "RUNNING")
        data["revision"] = 2
        data["tables"]["tasks"]["rows"][0][5] = "PASS"
        self.publish(data)
        self.assertEqual(run.snapshot()["results"][0]["status"], "PASS")
        before = self.facts_path.stat()
        data["revision"] = 3
        data["tables"]["tasks"]["rows"][0][5] = "FAIL"
        data["tables"]["tasks"]["rows"][0][6] = 1
        data["tables"]["nodes"]["rows"][0][3] = 1
        self.publish(data, preserve_mtime=True)
        self.assertEqual(self.facts_path.stat().st_size, before.st_size)
        self.assertEqual(self.facts_path.stat().st_mtime_ns, before.st_mtime_ns)
        self.assertEqual(run.snapshot()["results"][0]["status"], "FAIL")

    def test_snapshot_pins_one_group_revision(self):
        data = self.facts()
        self.publish(data)
        path = self.logical(STAGES[0], "task.tsv")
        with snapshot_context():
            self.assertEqual(rows(path)[0]["status"], "PASS")
            data["revision"] = 2
            data["tables"]["tasks"]["rows"][0][5] = "FAIL"
            self.publish(data)
            self.assertEqual(rows(path)[0]["status"], "PASS")
        self.assertEqual(rows(path)[0]["status"], "FAIL")

    def test_row_cache_signature_tracks_facts_instead_of_missing_tsv(self):
        data = self.facts()
        self.publish(data)
        path = self.logical(STAGES[0], "task.tsv")
        reader = IncrementalRows()
        self.assertFalse(path.exists())
        self.assertEqual(reader.rows(path)[0]["exit_code"], "0")
        data["revision"] = 2
        data["tables"]["tasks"]["rows"][0][6] = 7
        self.publish(data)
        self.assertEqual(reader.rows(path)[0]["exit_code"], "7")

    def test_invalid_schema_tables_and_duplicate_identity_are_rejected(self):
        data = self.facts()
        self.legacy(data)
        invalid = []
        entry = copy.deepcopy(data)
        entry["schema_version"] = 99
        invalid.append(entry)
        entry = copy.deepcopy(data)
        entry["tables"]["tasks"]["fields"][0] = "token"
        invalid.append(entry)
        entry = copy.deepcopy(data)
        entry["tables"]["nodes"]["rows"][0].pop()
        invalid.append(entry)
        entry = copy.deepcopy(data)
        entry["tables"]["tasks"]["rows"].append(entry["tables"]["tasks"]["rows"][0][:])
        invalid.append(entry)
        entry = copy.deepcopy(data)
        entry["tables"]["nodes"]["rows"][0][1] = "old_token"
        invalid.append(entry)
        entry = copy.deepcopy(data)
        entry["tables"]["nodes"]["rows"][0][2] = "outside-node"
        invalid.append(entry)
        for entry in invalid:
            with self.subTest(entry=invalid.index(entry)):
                self.publish(entry)
                with self.assertRaises(FactsError):
                    rows(self.logical(STAGES[0], "task.tsv"))

    def add_fragment(self, data, stage, payload, sources=None):
        offset = self.archive.stat().st_size
        with self.archive.open("ab") as stream:
            stream.write(payload)
        source = self.logical(stage).relative_to(self.state).as_posix()
        data["tables"]["logs"]["rows"].append(
            [stage, f"hcu_fixture_token_{STAGES.index(stage) + 1}", sources or [source],
             offset, len(payload), hashlib.sha256(payload).hexdigest()])

    def test_exact_log_sources_and_utf8_fragments_do_not_read_group_tail(self):
        data = self.facts()
        payload = "节点一\n".encode("utf-8")
        self.add_fragment(data, STAGES[0], payload[:2])
        self.add_fragment(data, STAGES[1], b"other task must not appear\n")
        self.add_fragment(data, STAGES[0], payload[2:])
        self.publish(data)
        with open_log(self.logical(STAGES[0])) as stream:
            self.assertEqual(stream.read(), "节点一\n")
        self.assertEqual(log_size(self.logical(STAGES[0])), len(payload))
        self.assertEqual(log_tail(self.logical(STAGES[0])), "节点一\n")

    def test_missing_compact_log_never_reads_stale_physical_log(self):
        self.publish(self.facts())
        path = self.logical(STAGES[0])
        path.parent.mkdir(parents=True)
        path.write_text("stale success", encoding="utf-8")
        with self.assertRaises(FileNotFoundError):
            open_log(path)
        self.assertIsNone(log_size(path))

    def test_group_log_reader_rejects_physical_decoy_for_missing_compact_source(self):
        self.publish(self.facts())
        path = self.logical(STAGES[0])
        path.parent.mkdir(parents=True)
        path.write_text("stale success must not fill compact evidence", encoding="utf-8")
        with self.assertRaises(FileNotFoundError):
            read_log_text(path)
        with self.assertRaises(FileNotFoundError):
            read_log_text(path, limit=4)

    def test_group_log_reader_uses_valid_compact_range_over_physical_decoy(self):
        data = self.facts()
        self.add_fragment(data, STAGES[0], b"committed raw output\n")
        self.add_fragment(data, STAGES[1], b"another task tail\n")
        self.publish(data)
        path = self.logical(STAGES[0])
        path.parent.mkdir(parents=True)
        path.write_text("stale physical output", encoding="utf-8")
        self.assertEqual(read_log_text(path), "committed raw output\n")
        self.assertEqual(read_log_text(path, limit=9), "committed")

    def test_group_log_reader_preserves_legacy_physical_fallback_without_facts(self):
        path = self.logical(STAGES[0])
        path.parent.mkdir(parents=True)
        path.write_text("legacy raw output\n", encoding="utf-8")
        self.assertEqual(read_log_text(path), "legacy raw output\n")
        self.assertEqual(read_log_text(path, limit=6), "legacy")

    def test_log_truncation_and_same_size_corruption_are_rejected(self):
        data = self.facts()
        self.add_fragment(data, STAGES[0], b"original evidence\n")
        self.publish(data)
        with open_log(self.logical(STAGES[0])) as stream:
            self.assertEqual(stream.read(), "original evidence\n")
        self.archive.write_bytes(b"tampered evidence\n")
        with self.assertRaises(OSError):
            open_log(self.logical(STAGES[0]))
        self.archive.write_bytes(b"short")
        with self.assertRaises(OSError):
            log_tail(self.logical(STAGES[0]))

    def test_cross_group_and_runtime_log_sources_are_rejected(self):
        for source in (f"stages/{STAGES[0]}/groups/group-002/n01.log", "runtime/group-001/n01.log", "../escape.log"):
            data = self.facts()
            data["tables"]["logs"]["rows"] = [[STAGES[0], "hcu_fixture_token_1", [source], 0, 0,
                                                   hashlib.sha256(b"").hexdigest()]]
            self.publish(data)
            with self.assertRaises(FactsError):
                load_group(self.state, GROUP)

    def test_cleanup_confirmation_requires_matching_stopped_proof(self):
        data = self.facts()
        guard = {"token": "hcu_fixture_token_1", "status": "STOPPED", "remaining": 0, "tombstone": True}
        data["tables"]["cleanup"] = {"fields": ["stage_id", "token", "node", "source", "attempt", "status", "exit_code", "evidence", "guard"],
                                          "rows": [[STAGES[0], "hcu_fixture_token_1", "n01", "normal", 1, "CONFIRMED", 0, "cleanup.log", guard]]}
        self.publish(data)
        self.assertEqual(load_group(self.state, GROUP).table("cleanup")[0]["guard"], guard)
        guard["token"] = "previous_task_token"
        self.publish(data)
        with self.assertRaises(FactsError):
            load_group(self.state, GROUP)

    def test_legacy_scale_uses_historical_hostfile_and_saved_plan(self):
        path = self.state / "control.tsv"
        path.write_text("\n".join(line for line in path.read_text().splitlines() if not line.startswith("state_layout\t")) + "\n", encoding="utf-8")
        write_tsv(self.prepared / "rccl-plan.tsv", ["test", "np"], [["all_reduce", 16]])
        os.replace(self.prepared / "slotsfile", self.prepared / "hostfile")
        info = {"operation": "rccl", "group_size": "2", "slots": "999"}
        groups = [{"group": GROUP, "nodes": "n01,n02"}]
        self.assertEqual(resolve_scale(info, self.state, groups)["gpus_per_node"], 8)
        with self.assertRaises(ValueError):
            resolve_scale({**info, "operation": "deepep"}, self.state, groups)

    def test_compact_saved_plan_projects_with_retained_slotsfile(self):
        data = self.facts()
        data["tables"]["rccl_plan"] = {"fields": ["test", "np"], "rows": [["all_reduce", 16]]}
        self.publish(data)
        info = {"operation": "rccl", "group_size": "2", "slots": "999"}
        groups = [{"group": GROUP, "nodes": "n01,n02"}]
        self.assertEqual(resolve_scale(info, self.state, groups)["gpus_per_node"], 8)
        with self.assertRaises(ValueError):
            resolve_scale({**info, "operation": "deepep"}, self.state, groups)

    def test_compact_gpu_missing_slotsfile_rejects_stale_hostfile_for_scale_and_export(self):
        self.publish(self.facts())
        snapshot = Run(self.state).snapshot(final=True)
        os.replace(self.prepared / "slotsfile", self.prepared / "hostfile")
        groups = [{"group": GROUP, "nodes": "n01,n02"}]
        for operation in ("rccl", "deepep"):
            with self.subTest(operation=operation):
                info = {"operation": operation, "group_size": "2", "gpus_per_node": "8"}
                with self.assertRaisesRegex(FactsError, "retained slotsfile"):
                    resolve_scale(info, self.state, groups)
                source = copy.deepcopy(snapshot)
                source["control"].update(info)
                with self.assertRaisesRegex(FactsError, "retained slotsfile"):
                    export.export_plan(source, self.state, self.public / (operation + "-export"))

    def test_compact_gpu_marker_requires_slotsfile_before_first_facts_commit(self):
        os.replace(self.prepared / "slotsfile", self.prepared / "hostfile")
        self.assertFalse(self.facts_path.exists())
        with self.assertRaisesRegex(FactsError, "retained slotsfile"):
            allocation_path(self.state, GROUP, operation="rccl")

    def test_compact_non_gpu_operations_keep_hostfile_allocation(self):
        self.publish(self.facts())
        snapshot = Run(self.state).snapshot(final=True)
        hostfile = self.prepared / "hostfile"
        os.replace(self.prepared / "slotsfile", hostfile)
        for operation in ("platform", "resource", "platform,resource", "byte-flux", "ib-write-bw"):
            with self.subTest(operation=operation):
                self.assertEqual(allocation_path(self.state, GROUP, operation=operation), hostfile)
                source = copy.deepcopy(snapshot)
                source["control"]["operation"] = operation
                target = self.public / (operation.replace(",", "-") + "-export")
                self.assertEqual(export.export_plan(source, self.state, target), target)

    def test_compact_report_never_links_legacy_group_log_decoy(self):
        self.publish(self.facts())
        decoy = self.public / "logs" / f"{GROUP}.log"
        decoy.parent.mkdir()
        decoy.write_text("unrelated legacy evidence\n", encoding="utf-8")
        snapshot = Run(self.state).snapshot(final=True)
        self.assertTrue(all(row["evidence"] != str(decoy) for row in snapshot["results"]))
        render.write(self.state, snapshot)
        self.assertNotIn(str(decoy), (self.public / "summary.md").read_text(encoding="utf-8"))

    def test_legacy_report_keeps_historical_group_log_pointer(self):
        data = self.facts()
        self.legacy(data)
        path = self.state / "control.tsv"
        path.write_text("\n".join(line for line in path.read_text().splitlines() if not line.startswith("state_layout\t")) + "\n", encoding="utf-8")
        decoy = self.public / "logs" / f"{GROUP}.log"
        decoy.parent.mkdir()
        decoy.write_text("historical raw evidence\n", encoding="utf-8")
        snapshot = Run(self.state).snapshot(final=True)
        self.assertTrue(all(row["evidence"] == str(decoy) for row in snapshot["results"]))

    def test_compact_render_and_rebuild_preserve_facts_and_create_no_stage_tree(self):
        self.publish(self.facts())
        facts_before, raw_before = self.facts_path.read_bytes(), self.archive.read_bytes()
        with mock.patch.object(sys, "argv", ["reporting", "rebuild", "--run-dir", str(self.state)]):
            self.assertEqual(reporting_main.main(), 0)
        self.assertEqual(self.facts_path.read_bytes(), facts_before)
        self.assertEqual(self.archive.read_bytes(), raw_before)
        self.assertFalse((self.state / "stages").exists())
        self.assertFalse((self.state / "reporter.ready").exists())
        self.assertTrue((self.public / "summary.md").is_file())

    def test_compact_rccl_render_keeps_one_accumulated_group_report(self):
        self.publish(self.facts())
        snapshot = Run(self.state).snapshot(final=True)
        snapshot["control"]["operation"] = "rccl"
        render.write(self.state, snapshot)
        self.assertTrue((self.prepared / "rccl-summary.tsv").is_file())
        self.assertEqual(len(rows(self.prepared / "rccl-summary.tsv")), 2)
        self.assertFalse((self.state / "stages").exists())
        summary = (self.public / "summary.md").read_text(encoding="utf-8")
        self.assertIn(".state/prepared/<分组>/rccl-summary.md", summary)
        self.assertNotIn(".state/stages/<阶段>/groups/<分组>/rccl/rccl-summary.md", summary)

    def test_compact_progress_shows_root_log_path_before_first_collector(self):
        self.archive.unlink()
        (self.public / "logs").mkdir()
        snapshot = Run(self.state).snapshot(final=False)
        snapshot["run_dir"] = str(self.state)
        expected = str(self.public) + "/group-*.log"
        self.assertEqual(Display.logs_label(snapshot), expected)
        output = io.StringIO()
        with redirect_stderr(output):
            Display("plain").show(snapshot, force=True)
        self.assertIn("  日志    " + expected + "\n", output.getvalue())
        self.assertNotIn(str(self.public) + "/logs/group-*.log", output.getvalue())
        self.assertFalse((self.state / "log-archive").exists())
        self.assertFalse((self.state / "log-stream").exists())

    def test_legacy_progress_keeps_historical_logs_subdirectory(self):
        self.archive.unlink()
        (self.public / "logs").mkdir()
        snapshot = {"control": {"public_run_dir": str(self.public)}, "run_dir": str(self.state)}
        self.assertEqual(Display.logs_label(snapshot), str(self.public) + "/logs/group-*.log")

    def test_compact_live_progress_reads_collector_without_legacy_indexes(self):
        data = self.facts()
        data["tables"]["tasks"]["rows"][0][5] = "RUNNING"
        self.publish(data)
        self.archive.write_text("current item stdout\n", encoding="utf-8")
        record = Run(self.state).snapshot(final=False)["results"][0]
        self.assertEqual(record["status"], "RUNNING")
        lines = Display("plain").live_log(record)
        self.assertIn("  current item stdout", lines)
        self.assertNotIn("  等待日志输出…", lines)
        self.assertFalse((self.state / "log-archive").exists())
        self.assertFalse((self.state / "log-stream").exists())

    def test_external_report_replace_invalidates_render_write_cache(self):
        path = self.state / "derived.tsv"
        render.write_if_changed(path, "live\n")
        replacement = self.state / "rebuild.tmp"
        replacement.write_text("rebuild\n", encoding="utf-8")
        os.replace(replacement, path)
        render.write_if_changed(path, "live\n")
        self.assertEqual(path.read_text(encoding="utf-8"), "live\n")

    def test_export_and_rerun_control_keep_original_group_scale(self):
        data = self.facts()
        data["tables"]["tasks"]["rows"][0][5:7] = ["FAIL", 1]
        data["tables"]["nodes"]["rows"][0][3] = 1
        self.publish(data)
        snapshot = Run(self.state).snapshot(final=True)
        target = Path(self.temporary.name) / "export"
        export.export_plan(snapshot, self.state, target)
        plan = json.loads((target / "failed-groups.json").read_text(encoding="utf-8"))
        self.assertEqual(plan["entries"][0]["members"], ["n01", "n02"])
        self.assertEqual((target / "failed-nodes.hostfile").read_text(encoding="utf-8"), "n01 slots=8\nn02 slots=8\n")
        self.assertEqual(read_control(self.public)["group_count"], "1")

    def test_rebuild_holds_one_lock_for_snapshot_write_export_and_status(self):
        self.publish(self.facts())
        active, observed = [False], []

        @contextmanager
        def traced_lock(root):
            with report_lock(root):
                active[0] = True
                try:
                    yield
                finally:
                    active[0] = False

        def check(name, result=None):
            def call(*args, **kwargs):
                self.assertTrue(active[0], name)
                observed.append(name)
                return result
            return call

        argv = ["reporting", "rebuild", "--run-dir", str(self.state), "--export-plan", str(self.public / "export")]
        with mock.patch.object(sys, "argv", argv), mock.patch.object(reporting_main, "report_lock", traced_lock), \
                mock.patch.object(reporting_main.Run, "snapshot", side_effect=check("snapshot", {"status": "PASS"})), \
                mock.patch.object(reporting_main, "write", side_effect=check("write")), \
                mock.patch.object(export, "export_plan", side_effect=check("export")), \
                mock.patch.object(reporting_main, "table", side_effect=check("status")):
            self.assertEqual(reporting_main.main(), 0)
        self.assertEqual(observed, ["snapshot", "write", "export", "status"])

    @unittest.skipUnless(os.name == "posix", "cross-process flock is a Linux server contract")
    def test_report_lock_serializes_processes_and_preserves_inode(self):
        pending, acquired = self.public / "pending", self.public / "acquired"
        code = (
            "import fcntl,sys\nfrom pathlib import Path\nfrom hcu_envcheck.reporting.model import report_lock\n"
            "root,pending,acquired=map(Path,sys.argv[1:])\n"
            "with (root/'report.lock').open('a+b') as probe:\n"
            " try:\n  fcntl.flock(probe.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)\n"
            " except BlockingIOError:\n  pending.write_text('blocked')\n"
            " else:\n  pending.write_text('unlocked')\n  fcntl.flock(probe.fileno(),fcntl.LOCK_UN)\n"
            "with report_lock(root):\n acquired.write_text('acquired')\n"
        )
        environment = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]), PYTHONDONTWRITEBYTECODE="1")
        process = None
        try:
            with report_lock(self.state):
                inode = (self.state / "report.lock").stat().st_ino
                process = subprocess.Popen([sys.executable, "-B", "-c", code, str(self.state), str(pending), str(acquired)],
                                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=environment)
                deadline = time.monotonic() + 5
                while not pending.exists() and time.monotonic() < deadline:
                    if process.poll() is not None:
                        break
                    time.sleep(.01)
                self.assertTrue(pending.exists())
                self.assertEqual(pending.read_text(), "blocked")
                self.assertFalse(acquired.exists())
            stdout, stderr = process.communicate(timeout=5)
            self.assertEqual(process.returncode, 0, (stdout, stderr))
            self.assertEqual(acquired.read_text(), "acquired")
            with report_lock(self.state):
                self.assertEqual((self.state / "report.lock").stat().st_ino, inode)
        finally:
            if process is not None and process.poll() is None:
                process.kill()
                process.communicate()


if __name__ == "__main__":
    unittest.main()
