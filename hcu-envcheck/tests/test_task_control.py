# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Cancellation regressions.

Linux integration: python3 -B -m unittest discover -s tests -p test_task_control.py -v
Real guard tests need Bash, setsid, flock and Linux /proc; no MPI/DCU/Docker/SSH.
The different-UID case additionally runs when the test process is root.
"""

import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from cluster_run.task_control import RemoteTaskSession, TaskInterrupted, _guard_command, managed_command
from hcu_envcheck.baremetal import BaremetalClusterExecutor, BaremetalExecutionConfig, BaremetalNodeResult


def stop_evidence(token, status="STOPPED", remaining=0):
    return "__HCU_TASK_STOP__=" + json.dumps(dict(token=token, status=status, remaining=remaining, tombstone=True)) + "\n"


def node_result(node, stdout="", returncode=0, error_kind=None, stderr=""):
    return BaremetalNodeResult(node, "ssh", "task-stop", [], returncode, stdout, stderr, 0.0,
                              error_kind=error_kind)


class TaskSessionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.config = BaremetalExecutionConfig(output_root=Path(self.temp.name))
        self.token = uuid.uuid4().hex

    def test_inline_wrapper_quotes_arguments_without_a_remote_file(self):
        args = ["bash", "-c", "printf '%s' \"$1\"", "x", "spaces;$(nope)'"]
        command = managed_command(args, self.token)
        self.assertEqual(command[:5], ["env", "-u", "BASH_ENV", "-u", "ENV"])
        body = command[command.index("-c") + 1]
        self.assertEqual(command[-len(args):], args)
        self.assertIn("setsid", body)
        self.assertIn("/STOP", body)
        for token in ("", "short", "../../etc/passwd", "a" * 100, "a" * 16 + ";id"):
            with self.assertRaises(ValueError):
                managed_command(args, token)

    def test_cancel_visits_all_nodes_and_never_inherits_set_event(self):
        seen = []

        class FakeExecutor:
            def __init__(self, nodes, config):
                seen.append((tuple(nodes), config))
            def execute(_self, name, command):
                seen.append(command)
                return SimpleNamespace(nodes={
                    "node01": node_result("node01", stop_evidence(self.token)),
                    "node02": node_result("node02", returncode=255, error_kind="SSH_TRANSPORT_FAILED"),
                    "node03": node_result("node03", stop_evidence(self.token, "FAILED", 2), 1),
                    "node04": node_result("node04", stop_evidence("other")),
                })

        session = RemoteTaskSession(["node01", "node02", "node03", "node04", "missing"], self.config,
                                    run_token=self.token, execution_scope="container", container_name="worker",
                                    executor_factory=FakeExecutor)
        report = session.cancel_and_wait()
        self.assertIsNone(seen[0][1].cancel_event)
        self.assertEqual(seen[0][0], session.nodes)
        self.assertEqual(seen[1][:3], ["docker", "exec", "worker"])
        self.assertNotIn("source ", seen[1][seen[1].index("-c") + 1])
        self.assertTrue(session.cancel_event.is_set())
        self.assertIs(session.config.cancel_event, session.cancel_event)
        self.assertFalse(report.confirmed)
        self.assertEqual([report.nodes[n].status for n in session.nodes],
                         ["CONFIRMED", "UNCONFIRMED", "FAILED", "UNCONFIRMED", "UNCONFIRMED"])
        json.dumps(asdict(report))

    def test_root_task_user_stop_permission_failure_is_unconfirmed(self):
        class FakeExecutor:
            def __init__(self, nodes, config):
                pass
            def execute(_self, name, command):
                self.assertEqual(command[:5], ["docker", "exec", "--user", "1000", "worker"])
                return SimpleNamespace(nodes={"node01": node_result(
                    "node01", returncode=77, error_kind="REMOTE_COMMAND_FAILED",
                    stderr="task guard: initialization failed rc=77 (owner root, stopping uid=1000)")})
        session = RemoteTaskSession(["node01"], self.config, execution_scope="container",
                                    container_name="worker", container_user="1000", executor_factory=FakeExecutor)
        report = session.cancel_and_wait()
        self.assertFalse(report.confirmed)
        self.assertEqual(report.nodes["node01"].status, "UNCONFIRMED")

    def test_transport_error_does_not_accept_valid_stop_text(self):
        class FakeExecutor:
            def __init__(self, nodes, config):
                pass
            def execute(_self, name, command):
                return SimpleNamespace(nodes={"n": node_result(
                    "n", stop_evidence(self.token), returncode=255, error_kind="SSH_TRANSPORT_FAILED")})
        session = RemoteTaskSession(["n"], self.config, run_token=self.token, executor_factory=FakeExecutor)
        self.assertFalse(session.cancel_and_wait().confirmed)

    def test_unreadable_proc_without_known_survivors_is_unconfirmed(self):
        class FakeExecutor:
            def __init__(self, nodes, config):
                pass
            def execute(_self, name, command):
                payload = dict(token=self.token, status="FAILED", remaining=0, tombstone=True, uncertain=True)
                return SimpleNamespace(nodes={"n": node_result(
                    "n", "__HCU_TASK_STOP__=" + json.dumps(payload), returncode=1,
                    error_kind="REMOTE_COMMAND_FAILED", stderr="unreadable_proc=1")})
        session = RemoteTaskSession(["n"], self.config, run_token=self.token, executor_factory=FakeExecutor)
        report = session.cancel_and_wait()
        self.assertFalse(report.confirmed)
        self.assertEqual(report.nodes["n"].status, "UNCONFIRMED")

    def test_repeat_stop_retries_unreachable_nodes(self):
        attempts = []
        class FakeExecutor:
            def __init__(self, nodes, config):
                attempts.append(tuple(nodes))
            def execute(_self, name, command):
                if len(attempts) == 1:
                    raise OSError("connection down")
                return SimpleNamespace(nodes={"n": node_result("n", stop_evidence(self.token))})
        session = RemoteTaskSession(["n"], self.config, run_token=self.token, executor_factory=FakeExecutor)
        self.assertFalse(session.cancel_and_wait().confirmed)
        self.assertTrue(session.cancel_and_wait().confirmed)
        self.assertEqual(len(attempts), 2)

    def test_signal_sets_event_before_pool_release_then_confirms_cleanup(self):
        class FakeExecutor:
            def __init__(self, nodes, config):
                pass
            def execute(_self, name, command):
                return SimpleNamespace(nodes={"n": node_result("n", stop_evidence(self.token))})
        session = RemoteTaskSession(["n"], self.config, run_token=self.token, executor_factory=FakeExecutor)
        previous = signal.getsignal(signal.SIGTERM)
        sequence = []
        with self.assertRaises(TaskInterrupted) as caught:
            with session:
                with ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(session.cancel_event.wait, 3)
                    signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
                    sequence.append("handler returned")
                    self.assertTrue(future.result(timeout=1))
                sequence.append("pool released")
                signal.getsignal(signal.SIGINT)(signal.SIGINT, None)  # repeated signal stays non-raising
        self.assertEqual(sequence, ["handler returned", "pool released"])
        self.assertTrue(caught.exception.report.confirmed)
        self.assertIs(signal.getsignal(signal.SIGTERM), previous)

    def test_runner_cancel_reaps_real_local_process_and_preserves_output(self):
        session = RemoteTaskSession(["n"], self.config)
        timer = threading.Timer(0.25, session.cancel_event.set)
        timer.start()
        self.addCleanup(timer.cancel)
        started = time.monotonic()
        result = session.runner([sys.executable, "-u", "-c",
                                 "import time; print('started', flush=True); time.sleep(60)"],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 130)
        self.assertIn("started", result.stdout)
        self.assertLess(time.monotonic() - started, 5)
        self.assertFalse(session._local_processes)

    def test_runner_timeout_and_exit_code_are_not_cancelled(self):
        session = RemoteTaskSession(["n"], self.config)
        result = session.runner([sys.executable, "-c", "import sys; sys.exit(19)"], capture_output=True)
        self.assertEqual(result.returncode, 19)
        with self.assertRaises(subprocess.TimeoutExpired):
            session.runner([sys.executable, "-c", "import time; time.sleep(60)"], timeout=0.15, capture_output=True)
        self.assertFalse(session.cancel_event.is_set())
        self.assertFalse(session._local_processes)

    def test_cancel_before_runner_launch_does_not_spawn(self):
        session = RemoteTaskSession(["n"], self.config)
        session.cancel_event.set()
        with patch("subprocess.Popen", side_effect=AssertionError("must not launch")):
            self.assertEqual(session.runner(["anything"], capture_output=True).returncode, 130)

    def test_executor_real_local_wait_observes_event(self):
        event = threading.Event()
        config = BaremetalExecutionConfig(output_root=Path(self.temp.name), cancel_event=event)
        executor = BaremetalClusterExecutor(["n"], config)
        timer = threading.Timer(0.2, event.set)
        timer.start()
        self.addCleanup(timer.cancel)
        result = executor._run_bounded_process([sys.executable, "-c", "import time; time.sleep(60)"], 60)
        self.assertTrue(result["cancelled"])
        self.assertEqual(result["returncode"], 130)
        self.assertFalse(result["timed_out"])
        self.assertFalse(executor._processes)

    def test_pre_cancelled_ssh_and_clush_never_spawn_or_report_success(self):
        for transport in ("ssh", "clush"):
            event = threading.Event()
            event.set()
            config = BaremetalExecutionConfig(output_root=Path(self.temp.name), cancel_event=event,
                                              transport=transport, ssh_executable="ssh", clush_executable="clush")
            executor = BaremetalClusterExecutor(["n1", "n2"], config,
                        popen=lambda *a, **kw: self.fail("must not start transport"))
            result = executor.execute("cancelled", ["true"])
            self.assertEqual(result.status, "CANCELLED")
            self.assertTrue(all(n.error_kind == "CANCELLED" and not n.success for n in result.nodes.values()))

    def test_cancellation_dominates_injected_runner_success_sentinel(self):
        event = threading.Event()
        def runner(argv, **kwargs):
            event.set()
            sentinel = re.search(r"(__HCU_ENVCHECK_RC_[0-9a-f]+__)", argv[-1]).group(1)
            return subprocess.CompletedProcess(argv, 0, sentinel + "=0\n", "")
        executor = BaremetalClusterExecutor(["n"], BaremetalExecutionConfig(
            output_root=Path(self.temp.name), cancel_event=event, ssh_executable="ssh"), runner=runner)
        result = executor.execute("race", ["true"])
        self.assertEqual(result.nodes["n"].error_kind, "CANCELLED")
        self.assertEqual(result.nodes["n"].returncode, 130)


LINUX_GUARD = sys.platform.startswith("linux") and all(shutil.which(x) for x in ("bash", "setsid", "flock"))


@unittest.skipUnless(LINUX_GUARD, "real guard process tests require Linux /proc + bash/setsid/flock")
class LinuxTaskGuardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.tokens = []
        self.processes = []
        self.token = self.new_token()

    def new_token(self):
        token = uuid.uuid4().hex
        self.tokens.append(token)
        return token

    def tearDown(self):
        for token in self.tokens:
            subprocess.run(_guard_command("stop", token, ["0", "5"]), capture_output=True, timeout=20)
        for process in self.processes:
            try:
                process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate(timeout=5)
        self.temp.cleanup()

    def start(self, body, token=None):
        process = subprocess.Popen(managed_command(["bash", "-c", body], token or self.token),
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.processes.append(process)
        return process

    def stop(self, token=None, **kwargs):
        return subprocess.run(_guard_command("stop", token or self.token, ["0", "5"]),
                              capture_output=True, text=True, timeout=20, **kwargs)

    def wait_file(self, name):
        path = self.directory / name
        until = time.monotonic() + 8
        while time.monotonic() < until:
            if path.exists() and path.read_text().strip():
                return path
            time.sleep(0.03)
        self.fail(f"missing worker evidence: {path}")
        return path

    def test_user_bash_startup_hook_is_not_sourced_by_guard_or_stop(self):
        hook = self.directory / "hook.sh"
        hook.write_text(f"touch '{self.directory}/hook-ran'; exit 88\n")
        env = dict(os.environ, BASH_ENV=str(hook))
        result = subprocess.run(managed_command(["true"], self.token), capture_output=True, text=True,
                                timeout=15, env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.stop(env=env).returncode, 0)
        self.assertFalse((self.directory / "hook-ran").exists())

    def assert_dead(self, pid):
        path = Path(f"/proc/{pid}/stat")
        if path.exists():
            self.assertIn(path.read_text().rsplit(") ", 1)[1].split()[0], ("Z", "X"))

    def test_exit_code_and_arguments_preserved(self):
        result = subprocess.run(managed_command(["bash", "-c", "printf '%s' \"$1\"; exit 17", "x", "a; b ' c"], self.token),
                                capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 17, result.stderr)
        self.assertEqual(result.stdout, "a; b ' c")

    def test_payload_preserves_callers_umask_and_state_stays_private(self):
        boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        ns = re.sub(r"\D", "", os.readlink("/proc/self/ns/pid"))
        for mask in (0o022, 0o002, 0o027, 0o077):
            with self.subTest(umask=oct(mask)):
                token = self.new_token()
                directory = self.directory / f"umask-{mask:o}"
                directory.mkdir()
                payload = ["bash", "-c", 'printf ok > "$1/output"; mkdir -- "$1/directory"; umask',
                           "payload", str(directory)]
                # These names must not override the umask captured from the
                # process and explicitly forwarded through setsid's argv.
                env = dict(os.environ, payload_umask="0077", HCU_TASK_UMASK="0077")
                result = subprocess.run(managed_command(payload, token), umask=mask, env=env,
                                        capture_output=True, text=True, timeout=15)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(int(result.stdout.strip(), 8), mask)
                self.assertEqual((directory / "output").stat().st_mode & 0o777, 0o666 & ~mask)
                self.assertEqual((directory / "directory").stat().st_mode & 0o777, 0o777 & ~mask)
                state = Path(f"/tmp/hcu-task-{boot}-{ns}-{token}")
                self.assertEqual(state.stat().st_mode & 0o777, 0o700)
                self.assertEqual((state / "lock").stat().st_mode & 0o777, 0o600)
                members = list(state.glob("member.*"))
                self.assertTrue(members)
                self.assertTrue(all(member.stat().st_mode & 0o777 == 0o600 for member in members))

    def test_forked_and_term_ignoring_descendants_stop_other_job_survives(self):
        other = self.new_token()
        self.start(f"echo $$ > '{self.directory}/other'; sleep 60", other)
        self.start(f"trap '' TERM; echo $$ > '{self.directory}/parent'; sleep 60 & echo $! > '{self.directory}/child'; wait")
        other_pid = self.wait_file("other").read_text().strip()
        parent = self.wait_file("parent").read_text().strip()
        child = self.wait_file("child").read_text().strip()
        result = self.stop()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assert_dead(parent)
        self.assert_dead(child)
        self.assertTrue(Path(f"/proc/{other_pid}").exists())
        self.assertNotIn(Path(f"/proc/{other_pid}/stat").read_text().rsplit(") ", 1)[1].split()[0], ("Z", "X"))

    def test_detached_setsid_child_is_identified_by_token(self):
        self.start(f"setsid bash -c 'trap \"\" TERM; echo $$ > \"{self.directory}/detached\"; sleep 60' & wait")
        pid = self.wait_file("detached").read_text().strip()
        result = self.stop()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assert_dead(pid)

    def test_scrubbed_environment_child_remains_in_authenticated_session(self):
        self.start(f"env -u HCU_TASK_TOKEN -u HCU_TASK_MEMBER bash -c 'echo $$ > \"{self.directory}/scrubbed\"; sleep 60' & wait")
        pid = self.wait_file("scrubbed").read_text().strip()
        result = self.stop()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assert_dead(pid)

    def test_normal_exit_reaps_background_children_without_cancelling_peer_rank(self):
        self.start(f"echo $$ > '{self.directory}/peer'; sleep 60")
        peer = self.wait_file("peer").read_text().strip()
        process = self.start(f"sleep 60 & echo $! > '{self.directory}/orphan'; exit 23")
        orphan = self.wait_file("orphan").read_text().strip()
        _, stderr = process.communicate(timeout=15)
        self.assertEqual(process.returncode, 23, stderr)
        self.assert_dead(orphan)
        self.assertTrue(Path(f"/proc/{peer}").exists())

    def test_stop_before_first_rank_and_repeat_stop_are_idempotent(self):
        self.assertEqual(self.stop().returncode, 0)
        process = self.start(f"touch '{self.directory}/must-not-run'")
        _, stderr = process.communicate(timeout=15)
        self.assertEqual(process.returncode, 125, stderr)
        self.assertFalse((self.directory / "must-not-run").exists())
        self.assertEqual(self.stop().returncode, 0)

    def test_concurrent_rank_start_stop_race_never_leaves_workers(self):
        with ThreadPoolExecutor(max_workers=6) as pool:
            future = pool.submit(self.stop)
            ranks = [pool.submit(self.start, f"echo $$ > '{self.directory}/rank-{i}'; sleep 60") for i in range(5)]
            self.assertEqual(future.result().returncode, 0)
            processes = [future.result() for future in ranks]
        for process in processes:
            process.communicate(timeout=15)
            self.assertNotEqual(process.returncode, 0)
        for path in self.directory.glob("rank-*"):
            self.assert_dead(path.read_text().strip())
        self.assertEqual(self.stop().returncode, 0)

    def test_forged_stale_pid_registration_does_not_kill_unrelated_process(self):
        initialized = subprocess.run(managed_command(["true"], self.token), capture_output=True, text=True, timeout=15)
        self.assertEqual(initialized.returncode, 0, initialized.stderr)
        unrelated = subprocess.Popen(["sleep", "60"])
        self.addCleanup(lambda: (unrelated.terminate(), unrelated.wait(timeout=3)))
        boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        ns = re.sub(r"\D", "", os.readlink("/proc/self/ns/pid"))
        state = Path(f"/tmp/hcu-task-{boot}-{ns}-{self.token}")
        (state / f"member.{unrelated.pid}.1").write_text(f"{unrelated.pid} 1\n")
        result = self.stop()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIsNone(unrelated.poll())

    def unreadable_process(self, name):
        """A real same-UID process whose proc environment is non-dumpable."""
        code = ("import ctypes,os,sys,time; from pathlib import Path; "
                "libc=ctypes.CDLL(None); rc=libc.prctl(4,0,0,0,0); "
                "assert rc == 0, rc; Path(sys.argv[1]).write_text(str(os.getpid())); time.sleep(60)")
        process = subprocess.Popen([sys.executable, "-c", code, str(self.directory / name)],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

        def reap():
            if process.poll() is None:
                process.terminate()
            process.communicate(timeout=5)
        self.addCleanup(reap)
        self.wait_file(name)
        try:
            with Path(f"/proc/{process.pid}/environ").open("rb") as stream:
                stream.read(1)
        except PermissionError:
            return process
        self.skipTest("test user has ptrace capability; non-dumpable environment remains readable")

    def test_preexisting_unreadable_process_is_excluded_by_proven_start_time(self):
        other = self.unreadable_process("preexisting")
        time.sleep(0.04)  # separate Linux start-time ticks; equality must remain conservative
        result = subprocess.run(managed_command(["true"], self.token), capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)
        stopped = self.stop()
        self.assertEqual(stopped.returncode, 0, stopped.stderr)
        self.assertIsNone(other.poll())

    def test_contemporaneous_unreadable_process_is_not_killed_or_confirmed_absent(self):
        initialized = subprocess.run(managed_command(["true"], self.token), capture_output=True, text=True, timeout=15)
        self.assertEqual(initialized.returncode, 0, initialized.stderr)
        other = self.unreadable_process("contemporaneous")
        stopped = subprocess.run(_guard_command("stop", self.token, ["0", "1"]),
                                 capture_output=True, text=True, timeout=15)
        self.assertNotEqual(stopped.returncode, 0)
        self.assertIn('"uncertain":true', stopped.stdout)
        self.assertIn("unreadable_proc=1", stopped.stderr)
        self.assertIsNone(other.poll())
        other.terminate()
        other.communicate(timeout=5)
        self.assertEqual(self.stop().returncode, 0)

    @unittest.skipUnless(hasattr(os, "geteuid") and os.geteuid() == 0, "different UID test requires root")
    def test_root_task_cannot_be_confirmed_stopped_by_unprivileged_user(self):
        import pwd
        try:
            nobody = pwd.getpwnam("nobody")
        except KeyError:
            self.skipTest("nobody account unavailable")
        self.start(f"echo $$ > '{self.directory}/root-task'; sleep 60")
        pid = self.wait_file("root-task").read_text().strip()
        result = self.stop(user=nobody.pw_uid, group=nobody.pw_gid)
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn('"status":"STOPPED"', result.stdout)
        self.assertTrue(Path(f"/proc/{pid}").exists())
        self.assertEqual(self.stop().returncode, 0)


if __name__ == "__main__":
    unittest.main()
