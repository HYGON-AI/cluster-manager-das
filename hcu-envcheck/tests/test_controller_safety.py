# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Server-only public CLI safety fixtures; no network, Docker or GPU workload.

The transport double executes the real reporter, candidate serializer, token
guard and deadline wrapper. Only the target command is replaced with a bounded
fixture. Every controller has its own process group and temporary output tree.
"""
from __future__ import annotations

import csv
import fcntl
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest


ROOT = Path(__file__).resolve().parents[1]

# One executable body serves the private ssh/cat/mv doubles and target payload.
# No implementation source is rewritten and no internal payload is invoked as
# a substitute for the public command under test.
TOOL = r'''#!/usr/bin/env python3
import json, os, pathlib, shlex, signal, subprocess, sys, time
root = pathlib.Path(os.environ['HCU_FIXTURE_ROOT'])
mode = os.environ['HCU_FIXTURE_MODE']
name = pathlib.Path(sys.argv[0]).name

def event(event_name, **values):
    record = dict(event=event_name, pid=os.getpid(), monotonic_ns=time.monotonic_ns(), **values)
    fd = os.open(root/'events.jsonl', os.O_WRONLY|os.O_CREAT|os.O_APPEND, 0o600)
    try: os.write(fd, (json.dumps(record)+'\n').encode())
    finally: os.close(fd)
    return record

def ready(label, record):
    temporary = root/(label+'.ready.tmp.'+str(os.getpid()))
    temporary.write_text(json.dumps(record))
    os.replace(temporary, root/(label+'.ready'))

def gate(label, **values):
    record = event(label, **values)
    ready(label, record)
    until = time.monotonic()+25
    while not (root/(label+'.release')).exists():
        if time.monotonic() > until:
            print('fixture gate expired: '+label, file=sys.stderr)
            raise SystemExit(92)
        time.sleep(.02)

def identity(directory):
    return dict(line.split('\t',1) for line in (directory/'identity.tsv').read_text().splitlines())

def target_argv(command):
    args = shlex.split(command)
    if args[:2] == ['docker','exec']:
        # Only the declared container's shell is emulated; no docker command
        # reaches the actual daemon.
        args = args[args.index('bash'):]
    return args

if name == 'fixture-payload':
    phase, stage, token = sys.argv[1:4]
    event('payload-start', phase=phase, stage_id=stage, token=token)
    print('FIXTURE_RAW_START phase='+phase+' stage='+stage+' token='+token, flush=True)
    if phase == 'execute' and mode == 'execution-timeout':
        # Keep the real TERM/kill-after budget and the real deadline witness.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        time.sleep(30)
    if phase == 'execute' and mode == 'deadline-temporary-interrupted':
        signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
        time.sleep(30)
    print('FIXTURE_RAW_END phase='+phase+' stage='+stage, flush=True)
    event('payload-end', phase=phase, stage_id=stage, token=token)
    raise SystemExit(0)

if name == 'timeout':
    if mode == 'deadline-temporary-interrupted' and os.environ.get('HCU_FIXTURE_PHASE') == 'execute':
        # The witness printf hook is entered after >temporary created the
        # zero-byte file. Start the real GNU timeout only after that point.
        marker = root/'deadline-temporary.ready'
        until = time.monotonic()+10
        while not marker.exists():
            if time.monotonic()>until: raise SystemExit('deadline temporary was not opened')
            time.sleep(.01)
        owner = json.loads(marker.read_text())
        temporary = pathlib.Path(owner['temporary'])
        if temporary.stat().st_size != 0: raise SystemExit('deadline temporary was already written')
        event('deadline-temporary-open', witness_pid=owner['witness_pid'], temporary=str(temporary), bytes=0)
        result = subprocess.run([os.environ['HCU_FIXTURE_REAL_TIMEOUT'], *sys.argv[1:]])
        if result.returncode != 124: raise SystemExit('expected real GNU timeout TERM result 124')
        # Force interruption before printf/mv, bypassing the owner EXIT trap
        # so the parent's post-join exact-path cleanup is also required.
        os.kill(owner['witness_pid'], signal.SIGKILL)
        event('deadline-witness-interrupted', witness_pid=owner['witness_pid'], temporary=str(temporary))
        raise SystemExit(result.returncode)
    os.execv(os.environ['HCU_FIXTURE_REAL_TIMEOUT'], [os.environ['HCU_FIXTURE_REAL_TIMEOUT'], *sys.argv[1:]])

if name == 'cat':
    source = pathlib.Path(sys.argv[-1]) if len(sys.argv)>1 else None
    if source is not None and source.name == 'log.pipe' and mode == 'collector-write-failure':
        owner = identity(source.parent)
        if owner['phase'] == 'execute':
            # Open the FIFO, write a short prefix, then fail as a broken
            # collector would. Producers must reach cleanup despite EPIPE.
            with source.open('rb', buffering=0) as stream:
                data = stream.read(4096)
                if data: os.write(1, data[:64])
            event('collector-write-failed', **{k:owner[k] for k in ('group','stage_id','phase','token')})
            print('fixture: collector write failed', file=sys.stderr)
            raise SystemExit(74)
    os.execv(os.environ['HCU_FIXTURE_REAL_CAT'], [os.environ['HCU_FIXTURE_REAL_CAT'], *sys.argv[1:]])

if name == 'mv':
    args = sys.argv[1:]
    destination = pathlib.Path(args[-1])
    source = pathlib.Path(args[-2]) if len(args)>1 else None
    candidate = source is not None and source.name.startswith('.facts-candidate.') and destination.name == 'facts.json'
    result = subprocess.run([os.environ['HCU_FIXTURE_REAL_MV'], *args])
    if result.returncode: raise SystemExit(result.returncode)
    if candidate and '.finish.' in source.name and mode == 'cancel-after-publication':
        gate('after-publication', state=str(destination.parent.parent.parent), published=str(destination))
    if candidate and '.start.' in source.name and mode == 'archive-open-failure':
        state = destination.parent.parent.parent
        log = state.parent/(destination.parent.name+'.log')
        log.rename(log.with_name(log.name+'.fixture-original'))
        log.mkdir()
        event('archive-open-failed', state=str(state), archive=str(log))
    raise SystemExit(0)

if name != 'ssh': raise SystemExit('unexpected fixture tool')
command = sys.argv[-1]
args = target_argv(command)
if 'hcu_envcheck.state_facts' in command:
    words = shlex.split(args[-1])
    def option(flag): return words[words.index(flag)+1]
    kind, group, phase = option('--kind'), option('--group'), option('--phase')
    state = option('--run-dir')
    event('candidate-call', kind=kind, group=group, phase=phase, stage_id=option('--stage-id'), state=state)
    if mode == 'cancel-held-permit' and kind == 'start' and group == 'group-002' and phase == 'prepare':
        gate('held-permit', state=state)
    if mode == 'cancel-finish-receipt' and kind == 'finish' and phase == 'prepare':
        result = subprocess.run(args, stdout=subprocess.PIPE)
        if result.returncode: raise SystemExit(result.returncode)
        gate('finish-receipt', state=state, candidate=option('--candidate'))
        sys.stdout.buffer.write(result.stdout); sys.stdout.buffer.flush()
        raise SystemExit(0)
    os.execvp(args[0], args)
if 'hcu_envcheck.reporting' in command:
    os.execvp(args[0], args)
raw = shlex.split(command)
if raw[:2] == ['docker','inspect']:
    print('true|fixture/image:local|fixture-container-id')
    raise SystemExit(0)
if len(args)<3 or args[0]!='bash' or args[1]!='-c':
    raise SystemExit('unexpected target transport')
guard = shlex.split(args[2])
if 'hcu-expiry-watch' in guard:
    position = guard.index('hcu-expiry-watch')
    runtime = pathlib.Path(guard[position+1]).parent
    owner = identity(runtime)
    phase, stage, token = owner['phase'], owner['stage_id'], owner['token']
    event('target-command', **{k:owner[k] for k in ('group','stage_id','phase','token')})
    if mode == 'deadline-temporary-interrupted' and phase == 'execute':
        os.environ['HCU_FIXTURE_PHASE'] = phase
        ready_path = shlex.quote(str(root/'deadline-temporary.ready'))
        release_path = shlex.quote(str(root/'deadline-temporary.release'))
        # A private Bash printf function observes the real redirection's
        # zero-byte file before the witness writes or renames it. No hook or
        # environment switch is added to production worker code.
        hook = ('printf() { if [[ $1 == *deadline* ]]; then '
                'builtin printf \'{"witness_pid":%s,"temporary":"%s"}\\n\' "$BASHPID" "$temporary" > '+ready_path+'.tmp; '
                'command mv -- '+ready_path+'.tmp '+ready_path+'; '
                'while [[ ! -e '+release_path+' ]]; do sleep .01; done; fi; builtin printf "$@"; };\n')
        guard[position-1] = hook+guard[position-1]
    if mode == 'late-fifo-writer' and phase == 'execute':
        child = os.fork()
        if child == 0:
            event('late-writer', stage_id=stage, token=token)
            ready('late-writer', dict(pid=os.getpid(), state=str(runtime.parent.parent)))
            until=time.monotonic()+25
            while not (root/'late-writer.release').exists() and time.monotonic()<until: time.sleep(.02)
            os.write(1, b'FIXTURE_LATE_WRITER_BYTES\n')
            event('late-writer-closed', stage_id=stage, token=token)
            os._exit(0)
    # Preserve the real token owner, timeout and deadline wrapper. Only its
    # inner target command is replaced with a CPU-only fixture.
    guard[position+6] = shlex.join([sys.executable, str(root/'bin/fixture-payload'), phase, stage, token])
    os.execvp(guard[0], guard)
if 'stop' in guard:
    token = guard[guard.index('stop')+1]
    event('token-stop', token=token)
    os.execvp(args[0], args)
raise SystemExit('unexpected worker command')
'''


def table(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream, delimiter="\t"))


def fact_rows(document: dict, name: str) -> list[dict]:
    value = document.get("tables", {}).get(name, {"fields": [], "rows": []})
    return [dict(zip(value["fields"], row)) for row in value["rows"]]


class ControllerSafetyTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.process = None
        self.addCleanup(self.temporary.cleanup)
        self.addCleanup(self.stop_fixture)
        tools = self.root / "bin"
        tools.mkdir()
        for name in ("ssh", "cat", "mv", "timeout", "fixture-payload"):
            path = tools / name
            path.write_text(TOOL, encoding="utf-8")
            path.chmod(0o755)
        self.env = {key: value for key, value in os.environ.items()
                    if not key.startswith(("HCU_", "RCCL_", "CLUSTER_", "NCCL_", "DEFAULTS_"))}
        self.env.update(PATH=str(tools) + os.pathsep + os.environ["PATH"], PYTHONPATH=str(ROOT),
                        PYTHONDONTWRITEBYTECODE="1", HCU_FIXTURE_ROOT=str(self.root),
                        HCU_FIXTURE_REAL_CAT=shutil.which("cat"), HCU_FIXTURE_REAL_MV=shutil.which("mv"),
                        HCU_FIXTURE_REAL_TIMEOUT=shutil.which("timeout"))

    def start(self, mode, *, nodes=1, timeout=3, operation="resource"):
        self.env["HCU_FIXTURE_MODE"] = mode
        hosts = self.root / "hosts"
        hosts.write_text("".join(f"n{number:02d}\n" for number in range(1, nodes+1)), encoding="utf-8")
        command = ["bash", str(ROOT / "bin/hcu-cluster-run"), "per-node-container", operation,
                   "-f", str(hosts), "--group-size", "1", "--container", "fixture-container",
                   "--image", "fixture/image:local", "--concurrency", str(nodes), "--node-concurrency", "1",
                   "--timeout", str(timeout), "--report-python", sys.executable, "--skip-idle-check",
                   "--log-detail", "quiet", "--output-dir", str(self.root / "output")]
        self.process = subprocess.Popen(command, cwd=ROOT, env=self.env, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, text=True, start_new_session=True)

    def wait_for(self, predicate, *, seconds=15, description="fixture condition"):
        end = time.monotonic()+seconds
        while time.monotonic()<end:
            value = predicate()
            if value:
                return value
            if self.process.poll() is not None:
                out, err = self.process.communicate(timeout=5)
                self.fail(description+" was not reached before CLI exit\n"+out+"\n"+err)
            time.sleep(.02)
        self.fail("deadline waiting for "+description+"; events="+repr(self.events()))

    def marker(self, name):
        path = self.root / (name+".ready")
        return self.wait_for(lambda: json.loads(path.read_text()) if path.exists() else None,
                             description=name)

    def release(self, name):
        (self.root / (name+".release")).touch()

    def state(self):
        states = list((self.root / "output").glob("*/.state"))
        self.assertEqual(len(states), 1)
        return states[0]

    def facts(self, group="group-001"):
        return json.loads((self.state()/"prepared"/group/"facts.json").read_text())

    def events(self):
        path = self.root / "events.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def finish(self, *, seconds=45):
        try:
            out, err = self.process.communicate(timeout=seconds)
        except subprocess.TimeoutExpired:
            self.stop_fixture()
            self.fail("public CLI exceeded bounded fixture deadline; events="+repr(self.events()))
        return self.process.returncode, out, err

    def stop_fixture(self):
        for label in ("held-permit", "finish-receipt", "after-publication", "late-writer", "deadline-temporary"):
            self.release(label)
        if self.process is not None and self.process.poll() is None:
            # This controller was started in its own new session. Never signal
            # a caller's process group, a program name or another GPU workload.
            try: os.killpg(self.process.pid, signal.SIGTERM)
            except ProcessLookupError: pass
            try: self.process.communicate(timeout=6)
            except subprocess.TimeoutExpired:
                try: os.killpg(self.process.pid, signal.SIGKILL)
                except ProcessLookupError: pass
                self.process.communicate(timeout=6)

    def assert_single_preflight_finish(self):
        calls = [item for item in self.events() if item["event"] == "candidate-call"
                 and item["kind"] == "finish" and item["phase"] == "prepare"]
        self.assertEqual(len(calls), 1, calls)
        tasks = fact_rows(self.facts(), "tasks")
        self.assertEqual([(item["stage_id"], item["status"]) for item in tasks], [("preflight", "PASS")])
        self.assertEqual(self.facts()["revision"], 2)

    def test_cancel_while_only_ordinary_permit_is_held_does_not_self_deadlock(self):
        self.start("cancel-held-permit", nodes=2)
        marker = self.marker("held-permit")
        permit = Path(marker["state"]) / ".node-permits/permit.1"
        with permit.open("ab") as stream:
            with self.assertRaises(BlockingIOError):
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.process.send_signal(signal.SIGTERM)
        self.release("held-permit")
        rc, out, err = self.finish()
        self.assertEqual(rc, 130, out+err)
        self.assertFalse(any(item["event"] == "payload-start" and item["phase"] == "execute" for item in self.events()))
        self.assertNotIn("immutable terminal", err)

    def test_combined_operation_keeps_its_public_comma_stage_through_commit_and_recycle(self):
        self.start("combined-stage", operation="platform,resource")
        rc, out, err = self.finish()
        self.assertEqual(rc, 1, out+err)  # CPU fixture bytes are not valid health probe JSON.
        facts = self.facts()
        tasks = fact_rows(facts, "tasks")
        self.assertEqual(facts["revision"], 4)
        self.assertEqual([(row["stage_id"], row["status"], row["cleanup"]) for row in tasks],
                         [("preflight", "PASS", "CONFIRMED"),
                          ("01-platform,resource", "PASS", "CONFIRMED")])
        self.assertEqual(len({row["token"] for row in tasks}), 2)
        self.assertTrue(any(row["event"] == "payload-start" and row["phase"] == "execute"
                            and row["stage_id"] == "01-platform,resource" for row in self.events()))
        self.assertFalse((self.state()/"runtime").exists())
        report = json.loads((self.state()/"report.json").read_text())
        self.assertTrue(report["final"])
        self.assertEqual(report["executed"], 1)
        self.assertEqual([row["stage_id"] for row in report["results"]], ["01-platform,resource"])
        self.assertNotIn("invalid candidate identity", err)
        self.assertNotIn("invalid runtime group/stage", err)

    def test_cancel_after_finish_receipt_publishes_once_and_removes_reaped_pid(self):
        self.start("cancel-finish-receipt")
        marker = self.marker("finish-receipt")
        self.assertTrue(Path(marker["candidate"]).is_file())
        self.assertEqual(fact_rows(self.facts(), "tasks")[0]["status"], "RUNNING")
        self.process.send_signal(signal.SIGTERM)
        self.release("finish-receipt")
        rc, out, err = self.finish()
        self.assertEqual(rc, 130, out+err)
        self.assert_single_preflight_finish()
        self.assertFalse((self.state()/"runtime/group-001").exists())
        self.assertNotIn("facts candidate failed", err)

    def test_cancel_after_atomic_publication_never_recommits_terminal_task(self):
        self.start("cancel-after-publication")
        marker = self.marker("after-publication")
        published = json.loads(Path(marker["published"]).read_text())
        self.assertEqual(fact_rows(published, "tasks")[0]["status"], "PASS")
        self.process.send_signal(signal.SIGTERM)
        self.release("after-publication")
        rc, out, err = self.finish()
        self.assertEqual(rc, 130, out+err)
        self.assert_single_preflight_finish()
        self.assertNotIn("facts candidate failed", err)

    def test_archive_open_failure_exits_without_blocked_fifo_or_dispatch(self):
        self.start("archive-open-failure")
        rc, out, err = self.finish()
        self.assertNotEqual(rc, 0, out+err)
        self.assertTrue(any(item["event"] == "archive-open-failed" for item in self.events()))
        self.assertFalse(any(item["event"] == "payload-start" for item in self.events()))
        self.assertEqual(fact_rows(self.facts(), "tasks")[0]["status"], "RUNNING")
        self.assertTrue((self.state()/"runtime/group-001").is_dir())
        self.assertTrue((self.state().parent/"group-001.log").is_dir())

    def test_collector_write_failure_stops_token_and_retains_uncommitted_scene(self):
        self.start("collector-write-failure")
        rc, out, err = self.finish()
        self.assertNotEqual(rc, 0, out+err)
        failed = [item for item in self.events() if item["event"] == "collector-write-failed"]
        self.assertEqual(len(failed), 1)
        runtime = self.state()/"runtime/group-001"
        owner = dict(line.split("\t", 1) for line in (runtime/"identity.tsv").read_text().splitlines())
        self.assertEqual(owner["phase"], "execute")
        collector = table(runtime/"collector-result.tsv")[0]
        self.assertEqual((collector["exit_code"], collector["drained"]), ("74", "0"))
        self.assertEqual(table(runtime/"task.tsv")[0]["reason_code"], "LOG_WRITE_FAILED")
        self.assertTrue(any(item["event"] == "token-stop" and item["token"] == owner["token"] for item in self.events()))
        guard = (runtime/"cleanup-normal-1/n01.guard.log").read_text()
        self.assertIn('"token":"'+owner["token"]+'"', guard)
        self.assertIn('"status":"STOPPED"', guard)
        tasks = fact_rows(self.facts(), "tasks")
        self.assertEqual(tasks[0]["status"], "PASS")
        self.assertEqual(tasks[1]["status"], "RUNNING", "failed log persistence must not publish terminal success")

    def test_late_fifo_writer_prevents_commit_and_workspace_reuse(self):
        self.start("late-fifo-writer")
        self.marker("late-writer")
        runtime = self.state()/"runtime/group-001"
        self.wait_for(lambda: (runtime/"worker-result.tsv").exists(), description="joined worker result")
        self.wait_for(lambda: not Path('/proc/'+table(runtime/"worker-result.tsv")[0]["worker_pid"]).exists(),
                      description="worker owner exit")
        published = (self.state()/"prepared/group-001/facts.json").read_bytes()
        self.assertEqual(fact_rows(self.facts(), "tasks")[-1]["status"], "RUNNING")
        self.assertFalse((runtime/"delivery.tsv").exists(), "open FIFO writer must prevent drained delivery")
        self.assertEqual((self.state()/"prepared/group-001/facts.json").read_bytes(), published)
        self.release("late-writer")
        rc, out, err = self.finish()
        self.assertNotEqual(rc, 0, out+err)
        self.assertEqual(table(runtime/"delivery.tsv")[0]["worker_joined"], "0")
        self.assertEqual(table(runtime/"collector-result.tsv")[0]["drained"], "1")
        self.assertEqual((self.state()/"prepared/group-001/facts.json").read_bytes(), published)
        self.assertIn("FIXTURE_LATE_WRITER_BYTES", (self.state().parent/"group-001.log").read_text())
        self.assertTrue(any(item["event"] == "late-writer-closed" for item in self.events()))

    def test_execution_deadline_after_successful_preflight_preserves_timeout_proof(self):
        self.start("execution-timeout", timeout=1)
        rc, out, err = self.finish(seconds=60)
        self.assertNotEqual(rc, 0, out+err)
        facts = self.facts()
        tasks = fact_rows(facts, "tasks")
        self.assertEqual([(item["stage_id"], item["status"]) for item in tasks],
                         [("preflight", "PASS"), ("01-resource", "TIMEOUT")])
        self.assertEqual(tasks[1]["execution_exit_code"], "137")
        self.assertEqual(tasks[1]["cleanup"], "CONFIRMED")
        witnesses = fact_rows(facts, "timeout")
        self.assertEqual([(item["stage_id"], item["token"], item["source"], item["expired"])
                          for item in witnesses], [("01-resource", tasks[1]["token"], "deadline", "1")])
        log = (self.state().parent/"group-001.log").read_text()
        self.assertIn("FIXTURE_RAW_START phase=execute stage=01-resource token="+tasks[1]["token"], log)
        self.assertNotIn("FIXTURE_RAW_END phase=execute", log)
        self.assertFalse((self.state()/"runtime/group-001").exists(), "fully committed confirmed timeout may recycle")

    def test_term_timeout_cleans_interrupted_deadline_temporary_before_recycle(self):
        self.start("deadline-temporary-interrupted", timeout=1)
        rc, out, err = self.finish(seconds=45)
        self.assertNotEqual(rc, 0, out+err)
        opened = [row for row in self.events() if row["event"] == "deadline-temporary-open"]
        interrupted = [row for row in self.events() if row["event"] == "deadline-witness-interrupted"]
        self.assertEqual(len(opened), 1)
        self.assertEqual(len(interrupted), 1)
        self.assertEqual(opened[0]["bytes"], 0)
        self.assertEqual(opened[0]["witness_pid"], interrupted[0]["witness_pid"])
        self.assertEqual(Path(opened[0]["temporary"]).name,
                         "timeout.deadline.n01.tsv.tmp."+str(opened[0]["witness_pid"]))
        self.assertFalse(Path(opened[0]["temporary"]).exists())
        facts = self.facts()
        tasks = fact_rows(facts, "tasks")
        self.assertEqual([(row["stage_id"], row["status"]) for row in tasks],
                         [("preflight", "PASS"), ("01-resource", "TIMEOUT")])
        self.assertEqual((tasks[1]["exit_code"], tasks[1]["execution_exit_code"], tasks[1]["cleanup"]),
                         ("124", "124", "CONFIRMED"))
        self.assertEqual(facts["revision"], 4)
        self.assertEqual(fact_rows(facts, "timeout"), [])
        self.assertFalse((self.state()/"runtime/group-001").exists(), "closed TERM timeout must recycle")
        self.assertFalse(list(self.state().rglob("timeout.deadline.*.tsv.tmp.*")))
        self.assertNotIn("workspace recycling failed", err)


if __name__ == "__main__":
    unittest.main()
