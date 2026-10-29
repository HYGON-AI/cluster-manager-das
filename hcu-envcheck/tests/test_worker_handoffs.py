# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Linux worker handoff integration fixtures, without SSH, Docker or hardware.

Run on the validation server. The fake transport exposes execution, cleanup,
late-write and signal behavior through the real Bash worker entry point.
"""
from __future__ import annotations

import csv
import os
import signal
import subprocess
import tempfile
import time
import unittest
from pathlib import Path


WORKER = Path(__file__).resolve().parents[1] / "hcu_envcheck" / "worker.sh"
TOKEN = "hcu_worker_fixture_1234567890"
FAKE_SSH = r'''#!/usr/bin/env bash
set -u
printf '%s\n' "$*" >> "$HCU_TEST_ROOT/ssh-calls.txt"
if [[ ( $* == *task_guard.sh* || $* == *task-guard.sh* ) && $* == *stop* ]]; then
    if [[ ${HCU_TEST_WAIT:-0} == 1 && -r $HCU_TEST_ROOT/transport-pid ]]; then
        read -r executing < "$HCU_TEST_ROOT/transport-pid"
        kill -TERM "$executing" 2>/dev/null || true
    fi
    if [[ ${HCU_TEST_CLEANUP_WAIT:-0} == 1 ]]; then
        printf 'ready\n' > "$HCU_TEST_ROOT/cleanup-ready"
        sleep 1
        printf 'closed\n' > "$HCU_TEST_ROOT/cleanup-closed"
    fi
    printf '__HCU_TASK_STOP__={"token":"%s","status":"STOPPED","remaining":0,"tombstone":true}\n' "${HCU_TEST_STOP_TOKEN:-$HCU_TEST_TOKEN}"
    exit "${HCU_TEST_STOP_RC:-0}"
fi
if [[ ${HCU_TEST_WAIT:-0} == 1 ]]; then
    sleep 30 & sleeping=$!
    trap 'kill "$sleeping" 2>/dev/null; wait "$sleeping" 2>/dev/null; printf "closed\n" > "$HCU_TEST_ROOT/transport-closed"; exit 143' TERM INT
    printf '%s\n' "$BASHPID" > "$HCU_TEST_ROOT/transport-pid"
    printf 'ready\n' > "$HCU_TEST_ROOT/transport-ready"
    wait "$sleeping"
    printf 'late\n' > "$HCU_TEST_ROOT/late-writer"
fi
if [[ ${HCU_TEST_RCCL:-0} == 1 ]]; then
    printf 'test\texit_code\telapsed_seconds\tlog\nall_reduce\t%s\t1\t%s/rccl/all_reduce.log\n' "${HCU_TEST_EXEC_RC:-0}" "$HCU_TEST_RUNTIME" > "$HCU_TEST_RUNTIME/rccl/execution.tsv"
fi
printf '[FAKE-RAW] full transport stdout\n'
printf '[FAKE-RAW] full transport stderr\n' >&2
exit "${HCU_TEST_EXEC_RC:-0}"
'''


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream, delimiter="\t"))


class WorkerHandoffTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.prepared = self.root / ".state" / "prepared" / "group-001"
        self.runtime = self.root / ".state" / "runtime" / "group-001"
        self.prepared.mkdir(parents=True)
        self.runtime.mkdir(parents=True)
        self.hostfile = self.prepared / "slotsfile"
        self.hostfile.write_text("node01 slots=8\n", encoding="utf-8")
        fake_bin = self.root / "bin"
        fake_bin.mkdir()
        fake = fake_bin / "ssh"
        fake.write_text(FAKE_SSH, encoding="utf-8")
        fake.chmod(0o755)
        os.mkfifo(self.runtime / "log.pipe")
        self.env = dict(os.environ, PATH=str(fake_bin) + os.pathsep + os.environ["PATH"],
                        HCU_STATE_LAYOUT="compact-group-v2", HCU_LOG_CHANNEL="1",
                        HCU_GROUP_LOG=str(self.runtime / "log.pipe"),
                        HCU_PUBLIC_GROUP_LOG=str(self.root / "group-001.log"),
                        HCU_GROUP_NAME="group-001", HCU_TEST_ROOT=str(self.root),
                        HCU_TEST_RUNTIME=str(self.runtime), HCU_TEST_TOKEN=TOKEN)
        self.identity()

    def identity(self, *, token=TOKEN, phase="execute", item="resource"):
        self.phase, self.item = phase, item
        values = dict(schema_version="2", group="group-001", stage_id="01-resource",
                      phase=phase, item=item, iteration="0", token=token)
        (self.runtime / "identity.tsv").write_text(
            "".join(f"{key}\t{value}\n" for key, value in values.items()), encoding="utf-8")

    def command(self, *, action=None, operation="resource"):
        command = ["bash", str(WORKER), "shared-conda", operation, "-f", str(self.hostfile),
                   "--phase", action or self.phase, "--stage-dir", str(self.runtime),
                   "--prepared-dir", str(self.prepared), "--run-token", TOKEN,
                   "--stage-iteration", "0", "--timeout", "10"]
        if self.item:
            command += ["--stage-item", self.item]
        if operation in {"rccl", "deepep"}:
            command += ["--gpus-per-node", "8"]
        return command

    def run_worker(self, **kwargs):
        return subprocess.run(self.command(**kwargs), env=self.env, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True, timeout=15, check=False)

    def assert_identity(self, record):
        self.assertEqual([record[key] for key in ("group", "stage_id", "phase", "token")],
                         ["group-001", "01-resource", self.phase, TOKEN])

    def test_success_commits_only_current_identity_and_joined_worker(self):
        completed = self.run_worker()
        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        result = rows(self.runtime / "worker-result.tsv")[0]
        self.assert_identity(result)
        self.assertEqual((result["execution_exit_code"], result["exit_code"], result["completed"]),
                         ("0", "0", "1"))
        self.assertGreater(int(result["worker_pid"]), 0)
        node = rows(self.runtime / "node-exit.node01.tsv")[0]
        self.assert_identity(node)
        self.assertEqual(node["exit_code"], "0")
        cleanup = rows(self.runtime / "cleanup-normal-1" / "nodes.tsv")[0]
        self.assert_identity(cleanup)
        self.assertEqual((cleanup["source"], cleanup["attempt"], cleanup["status"]),
                         ("normal", "1", "CONFIRMED"))
        self.assertIn("full transport stdout", completed.stdout)
        self.assertIn("full transport stderr", completed.stdout)
        for legacy in ("cleanup-complete", "cleanup-unconfirmed", "cleanup.tsv",
                       "execution-exit-code", "node01.exit-code"):
            self.assertFalse((self.runtime / legacy).exists(), legacy)

    def test_stale_identity_rejected_before_any_transport(self):
        self.identity(token="old_worker_fixture_1234567890")
        completed = self.run_worker()
        self.assertEqual(completed.returncode, 2)
        self.assertFalse((self.root / "ssh-calls.txt").exists())
        self.assertFalse((self.runtime / "worker-result.tsv").exists())

    def test_foreign_cleanup_token_cannot_confirm_stop(self):
        self.env["HCU_TEST_STOP_TOKEN"] = "foreign_worker_fixture_1234567890"
        completed = self.run_worker()
        self.assertEqual(completed.returncode, 70, completed.stderr + completed.stdout)
        cleanup = rows(self.runtime / "cleanup-normal-1" / "nodes.tsv")[0]
        self.assertEqual(cleanup["status"], "UNCONFIRMED")
        result = rows(self.runtime / "worker-result.tsv")[0]
        self.assertEqual((result["execution_exit_code"], result["exit_code"]), ("0", "70"))

    def test_transport_cleanup_failure_is_not_confirmed_by_stdout(self):
        self.env["HCU_TEST_STOP_RC"] = "9"
        completed = self.run_worker()
        self.assertEqual(completed.returncode, 70, completed.stderr + completed.stdout)
        cleanup = rows(self.runtime / "cleanup-normal-1" / "nodes.tsv")[0]
        self.assertEqual((cleanup["status"], cleanup["exit_code"]), ("UNCONFIRMED", "9"))

    def test_fallback_has_independent_attempt_and_never_rewrites_worker_result(self):
        first = self.run_worker()
        self.assertEqual(first.returncode, 0, first.stderr)
        result_path = self.runtime / "worker-result.tsv"
        original = result_path.read_bytes()
        completed = self.run_worker(action="cleanup")
        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        self.assertEqual(result_path.read_bytes(), original)
        for source in ("normal", "fallback"):
            attempt = self.runtime / f"cleanup-{source}-1"
            record = rows(attempt / "result.tsv")[0]
            self.assert_identity(record)
            self.assertEqual((record["source"], record["attempt"], record["completed"]), (source, "1", "1"))
            self.assertTrue((attempt / "node01.guard.log").is_file())

    def test_payload_failure_preserves_original_execution_code(self):
        self.env["HCU_TEST_EXEC_RC"] = "7"
        completed = self.run_worker()
        self.assertEqual(completed.returncode, 7, completed.stderr + completed.stdout)
        result = rows(self.runtime / "worker-result.tsv")[0]
        self.assertEqual((result["execution_exit_code"], result["exit_code"]), ("7", "7"))

    def test_collective_peer_rows_and_execution_table_keep_task_identity(self):
        self.identity(item="all_reduce")
        self.hostfile.write_text("node01 slots=8\nnode02 slots=8\n", encoding="utf-8")
        self.env["HCU_TEST_RCCL"] = "1"
        completed = self.run_worker(operation="rccl")
        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        records = rows(self.runtime / "node-results.tsv")
        self.assertEqual([record["node"] for record in records], ["node01", "node02"])
        for record in records:
            self.assert_identity(record)
        execution = rows(self.runtime / "rccl" / "execution.tsv")[0]
        self.assert_identity(execution)
        self.assertEqual(execution["test"], "all_reduce")
        self.assertIn("/stages/01-resource/groups/group-001/rccl/", execution["log"])

    def test_signal_waits_transport_before_main_publishes_result(self):
        self.env["HCU_TEST_WAIT"] = "1"
        process = subprocess.Popen(self.command(), env=self.env, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True)
        self.addCleanup(lambda: process.poll() is None and process.kill())
        deadline = time.monotonic() + 5
        while not (self.root / "transport-ready").exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertTrue((self.root / "transport-ready").exists())
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=10)
        self.assertEqual(process.returncode, 143, stderr + stdout)
        self.assertTrue((self.root / "transport-closed").exists())
        self.assertFalse((self.root / "late-writer").exists())
        result = rows(self.runtime / "worker-result.tsv")[0]
        self.assertEqual(result["worker_pid"], str(process.pid))
        self.assertEqual((result["execution_exit_code"], result["interrupted_exit_code"]), ("143", "143"))

    def test_signal_during_cleanup_wait_joins_the_existing_attempt(self):
        self.env["HCU_TEST_CLEANUP_WAIT"] = "1"
        process = subprocess.Popen(self.command(), env=self.env, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True)
        self.addCleanup(lambda: process.poll() is None and process.kill())
        deadline = time.monotonic() + 5
        while not (self.root / "cleanup-ready").exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertTrue((self.root / "cleanup-ready").exists())
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=10)
        self.assertEqual(process.returncode, 143, stderr + stdout)
        self.assertTrue((self.root / "cleanup-closed").exists())
        self.assertEqual(len(list(self.runtime.glob("cleanup-normal-*"))), 1)
        self.assertEqual(rows(self.runtime / "cleanup-normal-1" / "nodes.tsv")[0]["status"], "CONFIRMED")
        self.assertEqual(rows(self.runtime / "worker-result.tsv")[0]["interrupted_exit_code"], "143")
        self.assertEqual(rows(self.runtime / "worker-result.tsv")[0]["execution_exit_code"], "0")

    def test_legacy_worker_still_emits_legacy_handoffs_without_layout(self):
        for key in ("HCU_STATE_LAYOUT", "HCU_GROUP_LOG", "HCU_LOG_CHANNEL", "HCU_PUBLIC_GROUP_LOG"):
            self.env.pop(key)
        completed = self.run_worker()
        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        self.assertTrue((self.runtime / "node01.exit-code").is_file())
        self.assertTrue((self.runtime / "cleanup-complete").is_file())
        self.assertTrue((self.runtime / "execution-exit-code").is_file())
        self.assertFalse((self.runtime / "worker-result.tsv").exists())


if __name__ == "__main__":
    unittest.main()
