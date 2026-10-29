# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Controller handoff and candidate invariants, without GPU or network traffic.

Run on the validation server. The SSH double only supplies transport failures
and executes the real reporting/candidate entrypoints in the target scope.
"""
from __future__ import annotations

import argparse
import base64
from contextlib import redirect_stdout
import copy
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import unittest

from hcu_envcheck import state_facts

ROOT = Path(__file__).resolve().parents[1]
GROUP = "group-000"
TOKEN = "hcu_commit_fixture_0123456789"
OTHER_TOKEN = "hcu_other_fixture_0123456789"
NODES = ["n01", "n02"]
IDENTITY = ["group", "stage_id", "phase", "token"]


def write_tsv(path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fields, delimiter="\t", lineterminator="\n", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


class _CommitFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.public = Path(self.temp.name) / "run"
        self.state = self.public / ".state"
        self.prepared = self.state / "prepared" / GROUP
        self.prepared.mkdir(parents=True)
        self.runtime = self.state / "runtime" / GROUP
        self.archive = self.public / (GROUP + ".log")
        self.archive.write_bytes(b"older evidence\n")
        write_tsv(self.state / "groups.tsv", ["group", "nodes", "hostfile"],
                  [dict(group=GROUP, nodes=",".join(NODES), hostfile=str(self.prepared / "slotsfile"))])
        write_tsv(self.state / "stages.tsv", ["stage_id", "item", "iteration", "label"],
                  [dict(stage_id="01-first", item="first", iteration="0", label="first"),
                   dict(stage_id="02-second", item="second", iteration="0", label="second")])
        (self.state / "control.tsv").write_text("operation\tprotocol-fixture\nstate_layout\tcompact-group-v2\n", encoding="utf-8")
        self.serial = 0

    def args(self, *, kind="start", revision=0, stage="01-first", phase="execute", token=TOKEN, item="first"):
        self.serial += 1
        return argparse.Namespace(run_dir=self.state, group=GROUP, stage_id=stage, phase=phase, token=token,
            item=item, iteration="0", runtime_dir=self.runtime, expected_revision=revision, kind=kind,
            candidate=self.prepared / f".facts-candidate.fixture-{self.serial}.json", started_epoch=100,
            reason="CLEANUP_UNCONFIRMED")

    def identify(self, args):
        self.runtime.mkdir(parents=True, exist_ok=True)
        identity = {field: str(getattr(args, field)) for field in IDENTITY}
        values = {"schema_version": "2", **identity, "item": args.item, "iteration": "0"}
        (self.runtime / "identity.tsv").write_text(
            "".join(f"{field}\t{value}\n" for field, value in values.items()), encoding="utf-8")
        return identity

    def generate(self, args):
        output = io.StringIO()
        with redirect_stdout(output):
            state_facts.candidate(args)
        receipt = dict(line.split("\t", 1) for line in output.getvalue().splitlines())
        return json.loads(args.candidate.read_text()), receipt

    def publish(self, args):
        # This explicit rename models the controller's publication boundary;
        # the helper under test must never perform it itself.
        os.replace(args.candidate, self.prepared / "facts.json")

    def started(self, **kwargs):
        args = self.args(**kwargs)
        self.identify(args)
        self.generate(args)
        self.publish(args)
        return args

    def closed(self, args, *, status="PASS", code="0", cleanup="CONFIRMED", reason="OK"):
        identity = self.identify(args)
        prefix = state_facts.logical_prefix(identity)
        self.task = {**identity, "item": args.item, "iteration": "0", "status": status, "exit_code": code,
            "execution_exit_code": code, "cleanup": cleanup, "started_epoch": "100", "ended_epoch": "101",
            "reason_code": reason, "log": "group.log"}
        write_tsv(self.runtime / "task.tsv", ["group"] + state_facts.TASK_FIELDS, [self.task])
        write_tsv(self.runtime / "session.tsv", IDENTITY + ["pid", "start_tick"],
                  [{**identity, "pid": "424242", "start_tick": "12345"}])
        self.worker = {**identity, "worker_pid": "424242", "execution_exit_code": code, "exit_code": code,
                       "interrupted_exit_code": "0", "cleanup_source": "normal", "cleanup_attempt": "1", "completed": "1"}
        write_tsv(self.runtime / "worker-result.tsv", list(self.worker), [self.worker])
        self.delivery = {**identity, **{key: "1" for key in
            ("worker_joined", "watchdog_joined", "cleanup_joined", "collector_joined")}}
        write_tsv(self.runtime / "delivery.tsv", list(self.delivery), [self.delivery])
        offset = self.archive.stat().st_size
        payload = b"this task raw output\n"
        with self.archive.open("ab") as stream:
            stream.write(payload)
        self.collector = {**identity, "collector_pid": "424243", "exit_code": "0", "drained": "1",
                          "start_offset": str(offset), "end_offset": str(offset + len(payload))}
        write_tsv(self.runtime / "collector-result.tsv", list(self.collector), [self.collector])
        self.logs = {**identity, "source": prefix + "/group.log", "offset": str(offset),
                     "length": str(len(payload)), "sha256": hashlib.sha256(payload).hexdigest()}
        write_tsv(self.runtime / "logs.tsv", list(self.logs), [self.logs])
        rows = [{**identity, "node": node, "exit_code": code, "cleanup": cleanup, "log": "temporary.log"} for node in NODES]
        write_tsv(self.runtime / "node-results.tsv", IDENTITY + ["node", "exit_code", "cleanup", "log"], rows)
        directory = self.runtime / "cleanup-normal-1"
        directory.mkdir(exist_ok=True)
        self.cleanup = []
        for node in NODES:
            guard = dict(token=args.token, status="STOPPED" if cleanup == "CONFIRMED" else "FAILED",
                         remaining=0 if cleanup == "CONFIRMED" else 1, tombstone=True, uncertain=False)
            evidence = directory / (node + ".txt")
            evidence.write_text("__HCU_TASK_STOP__=" + json.dumps(guard) + "\n", encoding="utf-8")
            self.cleanup.append({**identity, "node": node, "source": "normal", "attempt": "1", "status": cleanup,
                                 "exit_code": "0" if cleanup == "CONFIRMED" else "1", "evidence": str(evidence)})
        write_tsv(directory / "nodes.tsv", list(self.cleanup[0]), self.cleanup)
        result = {**identity, "source": "normal", "attempt": "1", "completed": "1"}
        write_tsv(directory / "result.tsv", list(result), [result])
        return identity

    def finished_args(self, started, **updates):
        values = dict(kind="finish", revision=1, stage=started.stage_id, phase=started.phase,
                      token=started.token, item=started.item)
        values.update(updates)
        return self.args(**values)


class StateCommitTests(_CommitFixture):
    def test_candidate_is_exclusive_and_never_publishes_facts(self):
        args = self.args()
        self.identify(args)
        data, receipt = self.generate(args)
        self.assertFalse((self.prepared / "facts.json").exists())
        self.assertEqual(data["revision"], 1)
        self.assertEqual(receipt["sha256"], hashlib.sha256(args.candidate.read_bytes()).hexdigest())
        self.assertEqual(receipt["recyclable"], "0")
        before = args.candidate.read_bytes()
        with self.assertRaises(FileExistsError):
            self.generate(args)
        self.assertEqual(args.candidate.read_bytes(), before)

    def test_stale_revision_or_token_cannot_replace_current_running_task(self):
        started = self.started()
        self.closed(started)
        original = (self.prepared / "facts.json").read_bytes()
        for updates in ({"revision": 0}, {"token": OTHER_TOKEN}):
            args = self.finished_args(started, **updates)
            with self.subTest(updates=updates), self.assertRaises(ValueError):
                self.generate(args)
            self.assertFalse(args.candidate.exists())
            self.assertEqual((self.prepared / "facts.json").read_bytes(), original)
        self.assertTrue(self.runtime.exists())

    def test_finish_requires_published_running_task(self):
        args = self.args(kind="finish")
        self.closed(args)
        with self.assertRaises(ValueError):
            self.generate(args)
        self.assertTrue(self.runtime.exists())

    def test_finish_persists_exact_guard_proof_and_immutable_terminal_task(self):
        started = self.started()
        self.closed(started)
        args = self.finished_args(started)
        data, receipt = self.generate(args)
        self.assertEqual(receipt["recyclable"], "1")
        cleanup = state_facts.records(data, "cleanup")
        self.assertEqual([row["node"] for row in cleanup], NODES)
        self.assertTrue(all(row["guard"]["token"] == TOKEN and row["guard"]["tombstone"] is True for row in cleanup))
        self.assertEqual(state_facts.records(data, "tasks")[0]["execution_exit_code"], "0")
        self.assertEqual(state_facts.records(data, "nodes")[0]["log"],
                         f"stages/01-first/groups/{GROUP}/n01.log")
        self.publish(args)
        with self.assertRaises(ValueError):
            self.generate(self.finished_args(started, revision=2))

    def test_incomplete_worker_or_unjoined_writer_cannot_commit(self):
        started = self.started()
        self.closed(started)
        for field in ("worker_joined", "watchdog_joined", "cleanup_joined", "collector_joined"):
            changed = self.delivery | {field: "0"}
            write_tsv(self.runtime / "delivery.tsv", list(changed), [changed])
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.generate(self.finished_args(started))
        write_tsv(self.runtime / "delivery.tsv", list(self.delivery), [self.delivery])
        worker = self.worker | {"completed": "0"}
        write_tsv(self.runtime / "worker-result.tsv", list(worker), [worker])
        with self.assertRaises(ValueError):
            self.generate(self.finished_args(started))
        self.assertTrue(self.runtime.exists())

    def test_collector_failure_or_bad_sha_retains_workspace_and_running_facts(self):
        started = self.started()
        self.closed(started)
        published = (self.prepared / "facts.json").read_bytes()
        for collector in (self.collector | {"exit_code": "1"}, self.collector | {"drained": "0"}):
            write_tsv(self.runtime / "collector-result.tsv", list(collector), [collector])
            with self.assertRaises(ValueError):
                self.generate(self.finished_args(started))
        write_tsv(self.runtime / "collector-result.tsv", list(self.collector), [self.collector])
        changed = self.logs | {"sha256": "0" * 64}
        write_tsv(self.runtime / "logs.tsv", list(changed), [changed])
        with self.assertRaises(ValueError):
            self.generate(self.finished_args(started))
        self.assertTrue(self.runtime.exists())
        self.assertEqual((self.prepared / "facts.json").read_bytes(), published)

    def test_cleanup_confirmation_requires_matching_unambiguous_guard_json(self):
        started = self.started()
        self.closed(started)
        evidence = Path(self.cleanup[0]["evidence"])
        good = dict(token=TOKEN, status="STOPPED", remaining=0, tombstone=True, uncertain=False)
        for guard in (good | {"token": OTHER_TOKEN}, good | {"uncertain": True}, good | {"remaining": 1}, ["not-a-guard"]):
            evidence.write_text("__HCU_TASK_STOP__=" + json.dumps(guard) + "\n", encoding="utf-8")
            with self.subTest(guard=guard), self.assertRaises(ValueError):
                self.generate(self.finished_args(started))

    @unittest.skipUnless(os.name == "posix", "candidate symlink validation runs on Linux")
    def test_symlink_candidate_is_rejected_before_creating_its_target(self):
        args = self.args()
        self.identify(args)
        target = self.prepared / ".facts-candidate.symlink-target.json"
        args.candidate.symlink_to(target)
        with self.assertRaises(ValueError):
            self.generate(args)
        self.assertFalse(target.exists())
        self.assertFalse((self.prepared / "facts.json").exists())

    def test_unconfirmed_cleanup_is_persisted_but_not_recyclable(self):
        started = self.started()
        self.closed(started, status="FAIL", code="70", cleanup="UNCONFIRMED", reason="CLEANUP_UNCONFIRMED")
        data, receipt = self.generate(self.finished_args(started))
        self.assertEqual(receipt["recyclable"], "0")
        self.assertEqual(state_facts.records(data, "cleanup")[0]["guard"]["remaining"], 1)
        self.assertTrue(self.runtime.exists())

    def test_cancelled_execution_keeps_raw_exit_without_inventing_timeout(self):
        started = self.started()
        self.closed(started, status="CANCELLED", code="130", reason="CANCELLED")
        data, _ = self.generate(self.finished_args(started))
        self.assertEqual(state_facts.records(data, "tasks")[0]["status"], "CANCELLED")
        self.assertEqual(state_facts.records(data, "tasks")[0]["execution_exit_code"], "130")
        self.assertEqual(state_facts.records(data, "timeout"), [])

    def test_timeout_witness_is_phase_and_token_scoped(self):
        started = self.started()
        identity = self.closed(started, status="TIMEOUT", code="124", reason="TIMEOUT")
        witness = {**identity, "node": "n01", "source": "deadline", "exit_code": "124", "expired": "1"}
        path = self.runtime / "timeout.deadline.n01.tsv"
        for changed in (witness | {"phase": "prepare", "stage_id": "preflight"}, witness | {"token": OTHER_TOKEN}):
            write_tsv(path, list(changed), [changed])
            with self.assertRaises(ValueError):
                self.generate(self.finished_args(started))
        write_tsv(path, list(witness), [witness])
        data, _ = self.generate(self.finished_args(started))
        self.assertEqual(state_facts.records(data, "timeout")[0]["stage_id"], "01-first")
        self.assertEqual(state_facts.records(data, "timeout")[0]["token"], TOKEN)

    def test_rccl_empty_execution_rejects_non_timeout_zero_raw_or_bad_header(self):
        (self.state / "control.tsv").write_text("operation\trccl\nstate_layout\tcompact-group-v2\n", encoding="utf-8")
        started = self.started()
        published = (self.prepared / "facts.json").read_bytes()
        fields = IDENTITY + ["test", "exit_code", "elapsed_seconds", "log"]
        cases = [
            ("pass", "PASS", "0", "0", fields),
            ("fail", "FAIL", "124", "124", fields),
            ("cancelled", "CANCELLED", "137", "137", fields),
            ("zero-raw", "TIMEOUT", "124", "0", fields),
            ("invalid-raw", "TIMEOUT", "124", "unknown", fields),
            ("missing-header", "TIMEOUT", "124", "124", []),
            ("partial-header", "TIMEOUT", "124", "124", fields[:-1]),
            ("wrong-header", "TIMEOUT", "124", "124", fields[:-1] + ["other"]),
            ("duplicate-header", "TIMEOUT", "124", "124", fields[:-1] + ["test"]),
            ("reordered-header", "TIMEOUT", "124", "124", list(reversed(fields))),
        ]
        for name, status, code, raw_code, header in cases:
            with self.subTest(case=name):
                self.closed(started, status=status, code=code, reason=status)
                self.task["execution_exit_code"] = raw_code
                self.worker["execution_exit_code"] = raw_code
                write_tsv(self.runtime / "task.tsv", ["group"] + state_facts.TASK_FIELDS, [self.task])
                write_tsv(self.runtime / "worker-result.tsv", list(self.worker), [self.worker])
                execution = self.runtime / "rccl" / "execution.tsv"
                execution.parent.mkdir(exist_ok=True)
                execution.write_text("\t".join(header) + "\n" if header else "", encoding="utf-8")
                args = self.finished_args(started)
                with self.assertRaises(ValueError):
                    self.generate(args)
                self.assertFalse(args.candidate.exists())
                self.assertEqual((self.prepared / "facts.json").read_bytes(), published)
                self.assertTrue(self.runtime.exists(), "rejected handoff must retain its workspace")

    def test_rccl_nonempty_execution_still_rejects_wrong_item_token_or_multiple_rows(self):
        (self.state / "control.tsv").write_text("operation\trccl\nstate_layout\tcompact-group-v2\n", encoding="utf-8")
        started = self.started()
        identity = self.closed(started, status="TIMEOUT", code="124", reason="TIMEOUT")
        published = (self.prepared / "facts.json").read_bytes()
        row = {**identity, "test": started.item, "exit_code": "124", "elapsed_seconds": "300", "log": "raw.log"}
        fields = IDENTITY + ["test", "exit_code", "elapsed_seconds", "log"]
        for name, rows in (("wrong-item", [row | {"test": "second"}]),
                           ("wrong-token", [row | {"token": OTHER_TOKEN}]),
                           ("multiple-rows", [row, row])):
            with self.subTest(case=name):
                write_tsv(self.runtime / "rccl" / "execution.tsv", fields, rows)
                args = self.finished_args(started)
                with self.assertRaises(ValueError):
                    self.generate(args)
                self.assertFalse(args.candidate.exists())
                self.assertEqual((self.prepared / "facts.json").read_bytes(), published)
                self.assertTrue(self.runtime.exists(), "rejected handoff must retain its workspace")


class FrameCommitTests(_CommitFixture):
    @staticmethod
    def frame(source, payload, token=TOKEN):
        return (b"\n[HCU-LOG-CHUNK " + token.encode() + b"]\t" + base64.b64encode(source.encode()) + b"\t"
                + str(len(payload)).encode() + b"\t" + hashlib.sha256(payload).hexdigest().encode() + b"\n" + payload)

    def test_offsets_and_multiple_fragments_select_exact_source_bytes(self):
        identity = dict(group=GROUP, stage_id="01-first", phase="execute", token=TOKEN)
        source = f"runtime/{GROUP}/ib/server/stdout.txt"
        first = b"start\x00\xff\n[HCU-LOG-CHUNK " + TOKEN.encode() + b"]\tfake-body\n"
        middle, last = b"other endpoint\n", b"end without newline"
        base = b"outside before\n"
        payload = self.frame(source, first) + self.frame(source.replace("server", "client"), middle) + self.frame(source, last)
        self.archive.write_bytes(base + payload + b"outside after\n")
        fragments = state_facts.log_frames(self.archive, len(base), len(base + payload), identity, self.runtime)
        self.assertEqual(len(fragments), 3)
        expected = [first, middle, last]
        with self.archive.open("rb") as stream:
            for fragment, raw in zip(fragments, expected):
                stream.seek(fragment["offset"])
                self.assertEqual(stream.read(fragment["length"]), raw)
                self.assertEqual(fragment["sha256"], hashlib.sha256(raw).hexdigest())
        self.assertEqual(fragments[0]["sources"], [f"stages/01-first/groups/{GROUP}/ib/server/stdout.txt"])
        self.assertEqual(fragments[0]["sources"], fragments[2]["sources"])

    def test_bad_foreign_and_truncated_frames_are_rejected(self):
        identity = dict(group=GROUP, stage_id="01-first", phase="execute", token=TOKEN)
        good = self.frame(f"runtime/{GROUP}/ib/stdout.txt", b"raw data")
        cases = [good[:-1], good.replace(hashlib.sha256(b"raw data").hexdigest().encode(), b"0" * 64),
                 self.frame(f"runtime/{GROUP}/ib/stdout.txt", b"raw", OTHER_TOKEN),
                 self.frame("runtime/group-999/ib/stdout.txt", b"raw"),
                 self.frame(f"runtime/{GROUP}/../outside", b"raw"),
                 b"\n[HCU-LOG-CHUNK " + TOKEN.encode() + b"]\tpartial"]
        for value in cases:
            self.archive.write_bytes(value)
            with self.subTest(value=value[:100]), self.assertRaises(ValueError):
                state_facts.log_frames(self.archive, 0, len(value), identity, self.runtime)


@unittest.skipUnless(os.name == "posix" and shutil.which("bash"), "workspace shell checks run on Linux")
class WorkspaceRecycleTests(_CommitFixture):
    def recycle(self, args):
        return subprocess.run(["bash", "-c", 'source "$1"; hcu_recycle_workspace "$2" "$3" "$4" "$5" "$6"',
            "recycle-test", str(ROOT / "hcu_envcheck/state_runtime.sh"), str(self.state), GROUP,
            args.stage_id, args.phase, args.token], text=True, capture_output=True, timeout=15)

    def test_closed_workspace_removes_completion_and_timeout_witnesses(self):
        started = self.started()
        self.closed(started)
        args = self.finished_args(started)
        self.generate(args)
        self.publish(args)
        # Legacy handoff names can survive an interrupted previous owner.
        (self.runtime / "cleanup-complete").touch()
        (self.runtime / ".timeout").write_text(TOKEN)
        # Successful RCCL preparation leaves this handoff until C commits
        # its full plan into facts. It must not block the first execute item.
        (self.runtime / "rccl-plan.tsv").write_text("test\tnp\tbaseline_busbw_gbps\tmargin_pct\n")
        (self.runtime / "timeout.deadline.n01.tsv").write_text("closed witness already committed\n")
        result = self.recycle(args)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(self.runtime.exists())
        self.assertTrue((self.prepared / "facts.json").exists())

    def test_foreign_identity_or_unknown_file_is_retained(self):
        started = self.started()
        self.closed(started)
        foreign = copy.copy(started)
        foreign.token = OTHER_TOKEN
        self.assertNotEqual(self.recycle(foreign).returncode, 0)
        (self.runtime / "unexpected-user-data").write_text("retain me")
        self.assertNotEqual(self.recycle(started).returncode, 0)
        self.assertEqual((self.runtime / "unexpected-user-data").read_text(), "retain me")

    def test_rccl_header_only_timeout_preserves_prior_execution_and_recycles(self):
        (self.state / "control.tsv").write_text("operation\trccl\nstate_layout\tcompact-group-v2\n", encoding="utf-8")
        write_tsv(self.state / "stages.tsv", ["stage_id", "item", "iteration", "label"],
                  [dict(stage_id=f"{number:02d}-{item}", item=item, iteration="0", label=item)
                   for number, item in enumerate(("first", "second", "third"), 1)])
        fields = IDENTITY + ["test", "exit_code", "elapsed_seconds", "log"]
        first = self.started()
        identity = self.closed(first)
        write_tsv(self.runtime / "rccl" / "execution.tsv", fields,
                  [{**identity, "test": first.item, "exit_code": "0", "elapsed_seconds": "3", "log": "first.log"}])
        finished = self.finished_args(first)
        first_data, _ = self.generate(finished)
        prior_execution = copy.deepcopy(first_data["tables"]["execution"])
        self.publish(finished)
        recycled = self.recycle(finished)
        self.assertEqual(recycled.returncode, 0, recycled.stdout + recycled.stderr)
        self.assertFalse(self.runtime.exists())
        revision = first_data["revision"]
        for code, stage, item in (("124", "02-second", "second"), ("137", "03-third", "third")):
            with self.subTest(raw_exit_code=code):
                started = self.started(revision=revision, stage=stage, item=item,
                                       token=f"hcu_timeout_{code}_fixture_0123456789")
                identity = self.closed(started, status="TIMEOUT", code=code, reason="TIMEOUT")
                write_tsv(self.runtime / "rccl" / "execution.tsv", fields, [])
                published = (self.prepared / "facts.json").read_bytes()
                args = self.finished_args(started, revision=revision + 1)
                data, receipt = self.generate(args)
                self.assertTrue(args.candidate.is_file())
                self.assertEqual((self.prepared / "facts.json").read_bytes(), published,
                                 "candidate helper must not publish the terminal task")
                self.assertEqual(receipt["sha256"], hashlib.sha256(args.candidate.read_bytes()).hexdigest())
                self.assertEqual(receipt["recyclable"], "1")
                self.assertEqual(receipt["status"], "TIMEOUT")
                self.assertEqual(receipt["exit_code"], code)
                for key in IDENTITY:
                    self.assertEqual(receipt[key], identity[key])
                task = next(row for row in state_facts.records(data, "tasks") if row["stage_id"] == stage)
                self.assertEqual((task["status"], task["exit_code"], task["execution_exit_code"], task["cleanup"]),
                                 ("TIMEOUT", code, code, "CONFIRMED"))
                self.assertEqual([row["exit_code"] for row in state_facts.records(data, "nodes")
                                  if row["stage_id"] == stage], [code] * len(NODES))
                self.assertEqual(data["tables"]["execution"], prior_execution,
                                 "timeout without a measurement must not add or change execution rows")
                self.publish(args)
                committed = json.loads((self.prepared / "facts.json").read_text())
                self.assertEqual(committed, data)
                recycled = self.recycle(args)
                self.assertEqual(recycled.returncode, 0, recycled.stdout + recycled.stderr)
                self.assertFalse(self.runtime.exists())
                self.assertEqual(json.loads((self.prepared / "facts.json").read_text())["tables"]["execution"], prior_execution)
                revision = data["revision"]


@unittest.skipUnless(os.name == "posix" and shutil.which("bash"), "public CLI fixture runs on Linux")
class PublicPlanCommitTests(unittest.TestCase):
    def run_cli(self, root, *, node_count=10, reject_finish=""):
        tools = root / "bin"
        tools.mkdir()
        ssh = tools / "ssh"
        ssh.write_text('#!/usr/bin/env python3\nimport json,os,shlex,subprocess,sys\n'
            'command=sys.argv[-1]\n'
            'if "hcu_envcheck.state_facts" in command:\n'
            '    words=shlex.split(shlex.split(command)[-1])\n'
            '    group=words[words.index("--group")+1]; kind=words[words.index("--kind")+1]\n'
            '    if kind=="finish" and group==os.environ.get("FAKE_REJECT_FINISH_GROUP"):\n'
            '        with open(os.environ["SSH_FAILURE_TRACE"],"a") as stream: stream.write(json.dumps({"group":group,"kind":kind,"exit_code":73})+"\\n")\n'
            '        print("fixture: candidate serialization refused",file=sys.stderr)\n'
            '        raise SystemExit(73)\n'
            'if "hcu_envcheck.reporting" in command or "hcu_envcheck.state_facts" in command:\n'
            '    raise SystemExit(subprocess.run(["bash","-c",command]).returncode)\n'
            'print("fixture: target transport unavailable", file=sys.stderr)\nraise SystemExit(255)\n', encoding="utf-8")
        ssh.chmod(0o755)
        nodes = [f"n{number:02d}" for number in range(1, node_count + 1)]
        hostfile = root / "hosts"
        hostfile.write_text("\n".join(nodes) + "\n", encoding="utf-8")
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(("HCU_", "RCCL_", "CLUSTER_", "NCCL_", "DEFAULTS_"))}
        env.update(PATH=str(tools) + os.pathsep + os.environ["PATH"], PYTHONPATH=str(ROOT), PYTHONDONTWRITEBYTECODE="1",
                   FAKE_REJECT_FINISH_GROUP=reject_finish, SSH_FAILURE_TRACE=str(root / "ssh-failure.jsonl"))
        command = ["bash", str(ROOT / "bin/hcu-cluster-run"), "shared-conda", "rccl",
            "-f", str(hostfile), "--group-size", "2", "--gpus-per-node", "8", "--concurrency", "2",
            "--node-concurrency", "3", "--timeout", "1", "--output-dir", str(root / "output"),
            "--report-python", sys.executable, "--log-detail", "quiet"]
        process = subprocess.Popen(command, env=env, cwd=ROOT, text=True,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        try:
            stdout, stderr = process.communicate(timeout=150)
        except subprocess.TimeoutExpired:
            # Only the fixture's new session is eligible for cancellation.
            # Killing just Bash leaves its fake reporting child holding pipes.
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                stdout, stderr = process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                stdout, stderr = process.communicate(timeout=5)
            self.fail("public CLI fixture exceeded its deadline; its private process group was stopped\n"
                      + stdout + "\n" + stderr)
        result = subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
        self.assertNotEqual(result.returncode, 0, "failed transport must never pass")
        runs = list((root / "output").glob("*/.state"))
        self.assertEqual(len(runs), 1, result.stdout + result.stderr)
        return result, runs[0], nodes

    def test_ten_nodes_keep_five_allocations_and_all_ten_blocked_stages_in_order(self):
        with tempfile.TemporaryDirectory() as temp:
            result, state, nodes = self.run_cli(Path(temp))
            groups, stages = state_facts.tsv(state / "groups.tsv"), state_facts.tsv(state / "stages.tsv")
            self.assertEqual(len(groups), 5)
            self.assertEqual([stage["item"] for stage in stages],
                ["all_reduce", "all_gather", "broadcast", "reduce", "reduce_scatter", "gather", "scatter", "alltoall", "alltoallv", "sendrecv"])
            self.assertEqual(len(list((state / "prepared").glob("group-*/slotsfile"))), 5)
            for index, group in enumerate(groups):
                members = nodes[index * 2:index * 2 + 2]
                self.assertEqual(group["nodes"].split(","), members)
                allocation = state / "prepared" / group["group"] / "slotsfile"
                self.assertEqual(allocation.read_text().splitlines(), [node + " slots=8" for node in members])
                facts = json.loads((allocation.parent / "facts.json").read_text())
                tasks = state_facts.records(facts, "tasks")
                self.assertEqual([task["stage_id"] for task in tasks], ["preflight"] + [stage["stage_id"] for stage in stages])
                self.assertEqual(tasks[0]["cleanup"], "UNCONFIRMED")
                self.assertEqual([task["status"] for task in tasks[1:]], ["BLOCKED"] * 10)
                self.assertTrue((state / "runtime" / group["group"]).exists(), "UNCONFIRMED workspace must remain")

    def test_failed_candidate_commit_retains_running_fact_and_blocks_every_later_stage(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            result, state, _ = self.run_cli(root, node_count=4, reject_finish="group-001")
            trace = [json.loads(line) for line in (root / "ssh-failure.jsonl").read_text().splitlines()]
            self.assertEqual(trace, [{"group": "group-001", "kind": "finish", "exit_code": 73}])
            failed = state / "prepared/group-001/facts.json"
            tasks = state_facts.records(json.loads(failed.read_text()), "tasks")
            self.assertEqual(tasks[0]["stage_id"], "preflight")
            self.assertEqual(tasks[0]["status"], "RUNNING", "unpublished finish must never replace the last committed task")
            self.assertEqual([row["status"] for row in tasks[1:]], ["BLOCKED"] * 10)
            self.assertEqual([row["reason_code"] for row in tasks[1:]], ["INCOMPLETE"] * 10)
            retained = state / "runtime/group-001"
            for name in ("identity.tsv", "task.tsv", "worker-result.tsv", "collector-result.tsv", "logs.tsv", "delivery.tsv"):
                self.assertTrue((retained / name).is_file(), name)
            self.assertIn("fixture: target transport unavailable", (state.parent / "group-001.log").read_text())
            self.assertIn("workspace retained", result.stderr)
            other = state_facts.records(json.loads((state / "prepared/group-002/facts.json").read_text()), "tasks")
            self.assertEqual(other[0]["status"], "FAIL")
            self.assertEqual(other[0]["cleanup"], "UNCONFIRMED")


if __name__ == "__main__":
    unittest.main()
