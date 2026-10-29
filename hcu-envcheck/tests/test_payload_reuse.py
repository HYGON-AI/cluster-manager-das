# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Compact payload facts and lossless IB log transport; no hardware traffic."""

from __future__ import annotations

import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import unittest
import uuid
from unittest.mock import patch

from cluster_run import extra_checks, task_control
from hcu_envcheck import group_logs
from hcu_envcheck import target_ib
from hcu_envcheck.payloads import device_selection
from hcu_envcheck.reporting.log_store import open_log


TOKEN = "hcu-payload-test-0123456789"
ROOT = Path(__file__).resolve().parents[1]
PAYLOADS = ROOT / "hcu_envcheck" / "payloads"
DEVICE_FIELDS = ["node", "gpus_per_node", "hip_visible_devices", "pci_devices"]
PCI = ["0000:03:00.0", "0000:04:00.0"]


class DeviceFactsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.group = Path(self.temp.name) / "prepared" / "group-000"
        self.group.mkdir(parents=True)
        self.record = self.group / "devices" / "node01.json"
        self.facts = self.group / "facts.json"

    def write_facts(self, *, rows=None, **updates):
        value = {"schema_version": 2, "group": "group-000", "revision": 1,
                 "tables": {"devices": {"fields": DEVICE_FIELDS,
                                         "rows": rows if rows is not None else [["node01", 2, "3,1", PCI]]}}}
        value.update(updates)
        self.facts.write_text(json.dumps(value), encoding="utf-8")
        return value

    def legacy(self, *, mask="0,1"):
        self.record.parent.mkdir(parents=True, exist_ok=True)
        value = {"schema_version": 1, "node": "node01", "gpus_per_node": 2,
                 "hip_visible_devices": mask, "pci_devices": PCI}
        self.record.write_text(json.dumps(value), encoding="utf-8")
        return value

    def test_virtual_locator_reads_authoritative_facts_without_device_files(self):
        self.write_facts()
        result = device_selection.selection_record(self.record)
        self.assertEqual(result["node"], "node01")
        self.assertEqual(result["hip_visible_devices"], "3,1")
        self.assertEqual(result["pci_devices"], PCI)
        self.assertFalse(self.record.parent.exists())

    def test_facts_override_existing_legacy_selection(self):
        self.legacy()
        self.write_facts()
        self.assertEqual(device_selection.selection_record(self.record)["hip_visible_devices"], "3,1")

    def test_absent_facts_keep_standalone_record_compatibility(self):
        expected = self.legacy()
        self.assertEqual(device_selection.selection_record(self.record, "node01"), expected)

    def test_invalid_or_missing_facts_never_fall_back_to_legacy_record(self):
        self.legacy()
        cases = [
            {"schema_version": 1}, {"schema_version": True}, {"group": "group-001"},
            {"revision": 0}, {"tables": {}},
            {"tables": {"devices": {"fields": DEVICE_FIELDS, "rows": []}}},
            {"tables": {"devices": {"fields": DEVICE_FIELDS, "rows": [["node01", 2]]}}},
            {"tables": {"devices": {"fields": DEVICE_FIELDS + ["node"], "rows": []}}},
        ]
        for update in cases:
            with self.subTest(update=update):
                self.write_facts(**update)
                with self.assertRaises(ValueError):
                    device_selection.selection_record(self.record)
        self.facts.write_text("{partial", encoding="utf-8")
        with self.assertRaises(ValueError):
            device_selection.selection_record(self.record)

    def test_duplicate_nodes_and_disagreeing_locator_are_rejected(self):
        self.write_facts(rows=[["node01", 2, "3,1", PCI], ["node01", 2, "3,1", PCI]])
        with self.assertRaises(ValueError):
            device_selection.selection_record(self.record)
        self.write_facts()
        with self.assertRaisesRegex(ValueError, "another node"):
            device_selection.selection_record(self.record, "node02")

    def run_exec(self, inventories):
        argv = ["device_selection.py", "exec", "--count", "2", "--record", str(self.record),
                "--", "payload", "a b", "x; y"]
        with patch.dict(os.environ, {}, clear=True), patch.object(device_selection.sys, "argv", argv), \
                patch.object(device_selection, "visible", side_effect=inventories) as probe, \
                patch.object(device_selection.os, "execvpe") as execute:
            device_selection.main()
        return probe, execute

    def test_execution_reprobes_current_allowance_and_exact_selected_pci_order(self):
        self.write_facts()
        probe, execute = self.run_exec([PCI + ["0000:05:00.0"], PCI])
        self.assertEqual(probe.call_count, 2)
        command, argv, environment = execute.call_args.args
        self.assertEqual((command, argv), ("payload", ["payload", "a b", "x; y"]))
        self.assertEqual(environment["HIP_VISIBLE_DEVICES"], "3,1")

    def test_execution_rejects_removed_or_reordered_physical_devices(self):
        self.write_facts()
        for inventories in ([PCI[:1]], [PCI, list(reversed(PCI))]):
            with self.subTest(inventories=inventories):
                with patch.dict(os.environ, {}, clear=True), \
                        patch.object(device_selection.sys, "argv", ["selector", "exec", "--count", "2",
                        "--record", str(self.record), "--", "payload"]), \
                        patch.object(device_selection, "visible", side_effect=inventories), \
                        patch.object(device_selection.os, "execvpe") as execute:
                    with self.assertRaises(ValueError):
                        device_selection.main()
                    execute.assert_not_called()


class DirectLogChannelTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "public"
        self.directory = self.root / ".state" / "runtime" / "group-000" / "ib" / "server"

    @staticmethod
    def frames(data):
        prefix = b"\n[HCU-LOG-CHUNK " + TOKEN.encode() + b"]\t"
        result = []
        position = 0
        while True:
            start = data.find(prefix, position)
            if start < 0:
                return result
            end = data.index(b"\n", start + len(prefix))
            source, size, digest = data[start + len(prefix):end].split(b"\t")
            source = base64.b64decode(source, validate=True).decode("utf-8")
            length = int(size)
            body = data[end + 1:end + 1 + length]
            if len(body) != length or hashlib.sha256(body).hexdigest().encode() != digest:
                raise AssertionError("truncated or interleaved channel body")
            result.append((source, body))
            position = end + 1 + length

    def test_short_writes_large_binary_output_and_concurrent_sources_are_lossless(self):
        captured = bytearray()

        def short_write(fd, value):
            self.assertEqual(fd, 1)
            part = bytes(value[:4093])
            captured.extend(part)
            time.sleep(0)  # Yield between short writes to exercise writer serialization.
            return len(part)

        env = {"HCU_LOG_CHANNEL": "1", "HCU_LOG_TOKEN": TOKEN}
        expected = {}
        with patch.dict(os.environ, env), patch.object(group_logs.os, "write", side_effect=short_write), \
                patch.object(group_logs.os, "fsync", side_effect=AssertionError("channel must not fsync archive")):
            writers = []
            for number in range(4):
                directory = self.directory / str(number)
                writer = group_logs.DirectGroupLog(self.root, "group-000", directory, "node01", [], "IB")
                source = directory.relative_to(self.root / ".state").as_posix() + "/stdout.txt"
                payload = (bytes([number, 0, 255]) * 25000
                           + b"\n[HCU-LOG-CHUNK " + TOKEN.encode() + b"]\tinside-body\n")
                expected[source] = payload
                writers.append((writer, payload))
            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = [pool.submit(writer.feed, "stdout", payload) for writer, payload in writers]
                for future in futures:
                    future.result()
            for writer, _ in writers:
                writer.finish(0)
        actual = {}
        for source, body in self.frames(bytes(captured)):
            actual.setdefault(source, bytearray()).extend(body)
            self.assertLessEqual(len(body), 64 * 1024)
        for source, payload in expected.items():
            self.assertEqual(bytes(actual[source]), payload)
            self.assertEqual(actual[source.replace("stdout.txt", "stderr.txt")], b"")
        self.assertFalse(self.root.exists(), "channel must not create public logs, indexes, locks or pointers")

    def test_broken_channel_fails_without_creating_archive(self):
        with patch.dict(os.environ, {"HCU_LOG_CHANNEL": "1", "HCU_LOG_TOKEN": TOKEN}), \
                patch.object(group_logs.os, "write", side_effect=BrokenPipeError("closed FIFO")):
            with self.assertRaises(group_logs.LogArchiveError):
                group_logs.DirectGroupLog(self.root, "group-000", self.directory, "node01", [], "IB")
        self.assertFalse(self.root.exists())

    def test_invalid_token_fails_before_output(self):
        with patch.dict(os.environ, {"HCU_LOG_CHANNEL": "1", "HCU_LOG_TOKEN": "bad"}), \
                patch.object(group_logs.os, "write") as write:
            with self.assertRaises(ValueError):
                group_logs.DirectGroupLog(self.root, "group-000", self.directory, "node01", [], "IB")
            write.assert_not_called()
        self.assertFalse(self.root.exists())

    def test_legacy_direct_log_index_remains_readable(self):
        with patch.dict(os.environ, {"HCU_LOG_CHANNEL": "0"}):
            writer = group_logs.DirectGroupLog(self.root, "group-000", self.directory, "node01", [], "IB")
            writer.feed("stdout", b"legacy stdout\n")
            writer.feed("stderr", b"legacy stderr\n")
            writer.finish(7)
        with open_log(self.directory / "stdout.txt") as stream:
            self.assertEqual(stream.read(), "legacy stdout\n")
        with open_log(self.directory / "stderr.txt") as stream:
            self.assertEqual(stream.read(), "legacy stderr\n")


@unittest.skipUnless(os.name == "posix" and shutil.which("bash"), "payload shell checks run on the Linux test server")
class SharedGuardPayloadTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.guard = self.root / "prepared" / "task-guard.sh"
        self.guard.parent.mkdir()
        self.guard.write_text("#!/bin/bash\n# controller-published immutable guard\n", encoding="utf-8")
        self.guard.chmod(0o444)
        self.env = {name: value for name, value in os.environ.items()
                    if not name.startswith(("HCU_", "RCCL_", "CLUSTER_", "NCCL_", "OMPI_", "DEFAULTS_"))}
        self.env.update(PATH=str(self.bin) + os.pathsep + os.environ.get("PATH", ""),
                        CLUSTER_ENV_FILE=str(self.root / "absent-site.conf"),
                        HCU_TASK_TOKEN=TOKEN, HCU_TASK_GUARD_FILE=str(self.guard),
                        HCU_GPUS_PER_NODE="1", HCU_DEVICE_SELECTION_DIR=str(self.root / "devices"),
                        RCCL_BIN_DIR=str(self.bin), RCCL_BASELINE_TEXT="1 all_reduce 100\n")
        tool = self.bin / "all_reduce_perf"
        tool.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        tool.chmod(0o755)
        self.hostfile = self.root / "slotsfile"
        self.hostfile.write_text("node01 slots=1\n", encoding="utf-8")
        self.env["HCU_GROUP_HOSTFILE"] = str(self.hostfile)

    def rccl_prepare(self, **updates):
        return subprocess.run(["bash", str(PAYLOADS / "rccl_perf_test.sh"), str(self.hostfile),
                               "--gpus-per-node", "1", "--tests", "all_reduce", "--dry-run",
                               "--prepare-only", "--skip-idle-check", "--log-dir", str(self.root / "rccl")],
                              env=self.env | updates, text=True, capture_output=True, timeout=20)

    def test_rccl_reuses_explicit_guard_and_preserves_changed_rank_versions(self):
        original = self.guard.read_bytes()
        stamp = self.guard.stat().st_mtime_ns
        first = self.rccl_prepare(NCCL_SOCKET_IFNAME="net0")
        self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
        rank = self.root / "rccl" / "rank-body.sh"
        old_rank = rank.read_bytes()
        second = self.rccl_prepare(NCCL_SOCKET_IFNAME="net1")
        self.assertEqual(second.returncode, 0, second.stdout + second.stderr)
        self.assertEqual(rank.read_bytes(), old_rank)
        variants = list(rank.parent.glob("rank-body.*.sh"))
        self.assertEqual(len(variants), 1)
        self.assertIn(b"export NCCL_SOCKET_IFNAME=net1", variants[0].read_bytes())
        self.assertEqual((self.guard.read_bytes(), self.guard.stat().st_mtime_ns), (original, stamp))
        self.assertFalse((rank.parent / "task-guard.sh").exists())

    def test_rccl_missing_explicit_guard_fails_without_republishing(self):
        missing = self.guard.with_name("missing.sh")
        result = self.rccl_prepare(HCU_TASK_GUARD_FILE=str(missing))
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(missing.exists())

    def test_deepep_uses_controller_guard_for_every_control_rank(self):
        self.hostfile.write_text("node01 slots=1\nnode02 slots=1\n", encoding="utf-8")
        tests = self.root / "deepep-tests"
        tests.mkdir()
        (tests / "test_low_latency.py").touch()
        evidence = self.root / "guard-evidence"
        mpi = self.bin / "mpirun"
        mpi.write_text('#!/bin/sh\nprintf "%s\\n" "$HCU_DEEPEP_GUARD" > "$GUARD_EVIDENCE"\n', encoding="utf-8")
        mpi.chmod(0o755)
        result = subprocess.run(["bash", str(PAYLOADS / "deepep_test.sh"), "group", str(self.hostfile),
                                 "lowl", "29521", "22", ""],
                                env=self.env | {"HCU_GPUS_PER_NODE": "8", "HCU_DEEPEP_TEST_DIR": str(tests),
                                                "GUARD_EVIDENCE": str(evidence)},
                                text=True, capture_output=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(evidence.read_text().strip(), str(self.guard))


class IBSharedGuardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.guard = self.root / "prepared" / "task-guard.sh"
        self.guard.parent.mkdir()
        self.guard.write_text("# controller-published guard\n", encoding="utf-8")

    def test_file_guard_preserves_argv_without_reading_or_inlining_body(self):
        payload = ["payload", "space ; literal", "$(not-evaluated)", ""]
        with patch.object(Path, "read_text", side_effect=AssertionError("guard body must not be read")):
            command = task_control.managed_command(payload, TOKEN, guard_file="/prepared path/task-guard.sh")
        self.assertEqual(command, ["env", "-u", "BASH_ENV", "-u", "ENV", "bash", "--noprofile", "--norc",
                                   "/prepared path/task-guard.sh", "run", TOKEN, "--", *payload])
        for token, path in (("bad", "/guard.sh"), (TOKEN, "relative.sh"), (TOKEN, "/guard\n.sh")):
            with self.subTest(token=token, path=path), self.assertRaises(ValueError):
                task_control.managed_command(payload, token, guard_file=path)

    def test_legacy_default_keeps_inline_guard_for_remote_without_shared_install(self):
        with patch.object(Path, "read_text", return_value="legacy-inline-body") as read:
            command = task_control.managed_command(["payload", "a b"], TOKEN)
        read.assert_called_once()
        self.assertEqual(command, ["env", "-u", "BASH_ENV", "-u", "ENV", "bash", "--noprofile", "--norc",
                                   "-c", "legacy-inline-body", "hcu-task-guard", "run", TOKEN, "--", "payload", "a b"])

    def test_ib_wrappers_keep_guard_outside_timeout_source_and_container_inside_scope(self):
        for name, wrapper in (("state", extra_checks.run_ib_state), ("paired", extra_checks.run_group_ib)):
            with self.subTest(wrapper=name), patch.object(task_control, "_guard_command", side_effect=AssertionError("inline guard used")):
                result = wrapper(["n01", "n02"], env_script="/target/env.sh", scope="container", container="worker",
                    container_workdir="/target/work", run_token=TOKEN, output_dir=self.root / name,
                    dry_run=True, guard_file="/prepared/task-guard.sh")
            command = result["command"]
            self.assertEqual(command[:5], ["docker", "exec", "--workdir", "/target/work", "worker"])
            start = command.index("/prepared/task-guard.sh")
            self.assertEqual(command[start:start + 5], ["/prepared/task-guard.sh", "run", TOKEN, "--", "timeout"])
            self.assertIn("source /target/env.sh", command[-1])
            self.assertLess(command[-1].index("cd -- /target/work"), command[-1].index("source /target/env.sh"))
            self.assertNotIn("HCU_TASK_MEMBER", "\n".join(command))

    def target_args(self):
        hostfile = self.root / "hosts"
        hostfile.write_text("n01\nn02\n", encoding="utf-8")
        return ["target_ib", "--hostfile", str(hostfile), "--output-dir", str(self.root / "result"), "--run-token", TOKEN]

    def test_compact_target_requires_controller_guard_and_legacy_ignores_guard_environment(self):
        argv = self.target_args()
        result = {"status": "PASS", "report_path": "ib-result.json", "returncode": 0}
        for guard in ("", str(self.guard.with_name("missing.sh"))):
            with patch.dict(os.environ, {"HCU_STATE_LAYOUT": "compact-group-v2", "HCU_TASK_GUARD_FILE": guard}), \
                    patch.object(target_ib, "run_group_ib") as run, patch("sys.argv", argv):
                with self.assertRaises(ValueError):
                    target_ib.main()
                run.assert_not_called()
        with patch.dict(os.environ, {"HCU_STATE_LAYOUT": "compact-group-v2", "HCU_TASK_GUARD_FILE": str(self.guard)}), \
                patch.object(target_ib, "run_group_ib", return_value=result) as run, patch("sys.argv", argv):
            self.assertEqual(target_ib.main(), 0)
            self.assertEqual(run.call_args.kwargs["guard_file"], str(self.guard))
        with patch.dict(os.environ, {"HCU_STATE_LAYOUT": "legacy", "HCU_TASK_GUARD_FILE": "/missing/ignored.sh"}), \
                patch.object(target_ib, "run_group_ib", return_value=result) as run, patch("sys.argv", argv):
            self.assertEqual(target_ib.main(), 0)
            self.assertNotIn("guard_file", run.call_args.kwargs)

    @unittest.skipUnless(os.name == "posix" and Path("/proc/self/stat").is_file()
                         and all(shutil.which(name) for name in ("bash", "setsid", "flock", "timeout")),
                         "real guard execution runs on the Linux validation server")
    def test_real_file_guard_preserves_source_literal_arguments_stderr_and_exit_code(self):
        from cluster_run.env import bootstrap_command
        self.guard.write_bytes((PAYLOADS / "task_guard.sh").read_bytes())
        env_script = self.root / "selected env.sh"
        env_script.write_text("export IB_GUARD_VALUE=loaded-from-target\n", encoding="utf-8")
        startup = self.root / "unexpected-startup.sh"
        startup.write_text("exit 88\n", encoding="utf-8")
        token = "hcu_ib_guard_" + uuid.uuid4().hex
        payload = ["bash", "-c", 'printf "%s|%s|%s" "$HCU_TASK_TOKEN" "$IB_GUARD_VALUE" "$1"; '
                   'printf "endpoint stderr" >&2; exit 17', "ib-fixture", "space ; $(literal)"]
        inner = ["timeout", "--signal=TERM", "--kill-after=1s", "5s", *bootstrap_command(env_script, payload)]
        command = task_control.managed_command(inner, token, guard_file=self.guard)
        result = subprocess.run(command, env=os.environ | {"BASH_ENV": str(startup), "ENV": str(startup)},
                                text=True, capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 17, result.stdout + result.stderr)
        self.assertEqual(result.stdout, token + "|loaded-from-target|space ; $(literal)")
        self.assertEqual(result.stderr, "endpoint stderr")


if __name__ == "__main__":
    unittest.main()
