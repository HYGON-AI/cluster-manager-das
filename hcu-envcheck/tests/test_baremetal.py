# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import io
import re
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path

from hcu_envcheck.baremetal import (
    BaremetalClusterExecutor,
    BaremetalConfigurationError,
    BaremetalExecutionConfig,
    NodeFileError,
    parse_nodes_file,
)


SENTINEL = re.compile(r"(__HCU_ENVCHECK_RC_[0-9a-f]+__)")


def sentinel_from_argv(argv):
    match = SENTINEL.search(argv[-1])
    if not match:
        raise AssertionError(f"remote sentinel missing from argv: {argv}")
    return match.group(1)


class NodeFileTests(unittest.TestCase):
    def test_parses_hostfile_ranges_comments_and_deduplicates(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "hosts"
            path.write_text(
                "# training nodes\n"
                "e06r1n[08-09] slots=8\n"
                "e06r1n09\n"
                "node01,node02 # comma list\n",
                encoding="utf-8",
            )
            self.assertEqual(
                parse_nodes_file(path),
                ["e06r1n08", "e06r1n09", "node01", "node02"],
            )

    def test_rejects_shell_metacharacters(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "hosts"
            path.write_text("node01;touch /tmp/bad\n", encoding="utf-8")
            with self.assertRaises(NodeFileError):
                parse_nodes_file(path)

    def test_empty_file_is_an_error(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "hosts"
            path.write_text("# none\n", encoding="utf-8")
            with self.assertRaises(NodeFileError):
                parse_nodes_file(path)


class BaremetalExecutorTests(unittest.TestCase):
    def test_concurrency_has_a_hard_controller_safety_cap(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(BaremetalConfigurationError):
                BaremetalExecutionConfig(
                    output_root=Path(temp), concurrency=129
                ).validate()

    def test_auto_prefers_clush(self):
        with tempfile.TemporaryDirectory() as temp:
            config = BaremetalExecutionConfig(output_root=Path(temp))
            executor = BaremetalClusterExecutor(
                ["node01"],
                config,
                which=lambda name: f"/usr/bin/{name}" if name in {"clush", "ssh"} else None,
            )
            self.assertEqual(executor.selected_transport(), ("clush", "/usr/bin/clush"))

    def test_auto_falls_back_to_ssh_when_clush_is_absent(self):
        with tempfile.TemporaryDirectory() as temp:
            config = BaremetalExecutionConfig(output_root=Path(temp))
            executor = BaremetalClusterExecutor(
                ["node01"],
                config,
                which=lambda name: "/usr/bin/ssh" if name == "ssh" else None,
            )
            self.assertEqual(executor.selected_transport(), ("ssh", "/usr/bin/ssh"))

    def test_transport_override_cannot_run_an_unrelated_local_program(self):
        with tempfile.TemporaryDirectory() as temp:
            config = BaremetalExecutionConfig(
                output_root=Path(temp),
                transport="ssh",
                ssh_executable="/usr/bin/codex",
            )
            executor = BaremetalClusterExecutor(["node01"], config)
            with self.assertRaises(ValueError):
                executor.selected_transport()

    def test_ssh_is_bounded_and_node_failures_are_isolated(self):
        active = 0
        maximum_active = 0
        lock = threading.Lock()
        local_executables = []

        def runner(argv, **kwargs):
            nonlocal active, maximum_active
            local_executables.append(Path(argv[0]).name)
            node = argv[-2]
            sentinel = sentinel_from_argv(argv)
            with lock:
                active += 1
                maximum_active = max(maximum_active, active)
            time.sleep(0.01)
            with lock:
                active -= 1
            if node == "node02":
                return subprocess.CompletedProcess(
                    argv, 7, f"bad\n{sentinel}=7\n", "probe failed\n"
                )
            return subprocess.CompletedProcess(argv, 0, f"ok\n{sentinel}=0\n", "")

        with tempfile.TemporaryDirectory() as temp:
            config = BaremetalExecutionConfig(
                output_root=Path(temp),
                transport="ssh",
                concurrency=2,
                ssh_executable="/usr/bin/ssh",
            )
            executor = BaremetalClusterExecutor(
                ["node01", "node02", "node03"], config, runner=runner
            )
            result = executor.execute("inventory", ["uname", "-r"], run_id="test")

            self.assertEqual(result.status, "PARTIAL")
            self.assertTrue(result.nodes["node01"].success)
            self.assertEqual(result.nodes["node02"].returncode, 7)
            self.assertEqual(result.nodes["node02"].error_kind, "REMOTE_COMMAND_FAILED")
            self.assertLessEqual(maximum_active, 2)
            self.assertEqual(set(local_executables), {"ssh"})
            for node_result in result.nodes.values():
                node_dir = Path(node_result.result_dir)
                self.assertTrue((node_dir / "stdout.txt").is_file())
                self.assertTrue((node_dir / "stderr.txt").is_file())
                self.assertTrue((node_dir / "result.json").is_file())
            self.assertTrue((Path(result.run_dir) / "run.json").is_file())

    def test_ssh_timeout_does_not_hide_other_node_success(self):
        def runner(argv, **kwargs):
            node = argv[-2]
            if node == "node01":
                raise subprocess.TimeoutExpired(argv, kwargs["timeout"], output="partial")
            sentinel = sentinel_from_argv(argv)
            return subprocess.CompletedProcess(argv, 0, f"ok\n{sentinel}=0\n", "")

        with tempfile.TemporaryDirectory() as temp:
            config = BaremetalExecutionConfig(
                output_root=Path(temp),
                transport="ssh",
                ssh_executable="/usr/bin/ssh",
            )
            result = BaremetalClusterExecutor(
                ["node01", "node02"], config, runner=runner
            ).execute("inventory", ["true"], run_id="timeout")

            self.assertEqual(result.status, "PARTIAL")
            self.assertTrue(result.nodes["node01"].timed_out)
            self.assertEqual(result.nodes["node01"].error_kind, "COMMAND_TIMEOUT")
            self.assertTrue(result.nodes["node02"].success)

    def test_clush_reads_individual_node_outputs_and_return_codes(self):
        invoked = []

        def runner(argv, **kwargs):
            invoked.append(argv)
            stdout_dir = Path(argv[argv.index("--outdir") + 1])
            stderr_dir = Path(argv[argv.index("--errdir") + 1])
            nodes = argv[argv.index("-w") + 1].split(",")
            sentinel = sentinel_from_argv(argv)
            for node in nodes:
                rc = 0 if node == "node01" else 3
                (stdout_dir / node).write_text(
                    f"{node}\n{sentinel}={rc}\n", encoding="utf-8"
                )
                (stderr_dir / node).write_text(
                    "" if rc == 0 else "remote failure\n", encoding="utf-8"
                )
            return subprocess.CompletedProcess(argv, 3, "", "")

        with tempfile.TemporaryDirectory() as temp:
            config = BaremetalExecutionConfig(
                output_root=Path(temp),
                transport="auto",
                clush_executable="/usr/bin/clush",
            )
            result = BaremetalClusterExecutor(
                ["node01", "node02"], config, runner=runner
            ).execute("inventory", ["hostname"], run_id="clush")

            self.assertEqual(result.transport, "clush")
            self.assertEqual(result.status, "PARTIAL")
            self.assertTrue(result.nodes["node01"].success)
            self.assertEqual(result.nodes["node02"].returncode, 3)
            self.assertEqual(result.nodes["node02"].error_kind, "REMOTE_COMMAND_FAILED")
            self.assertEqual(Path(invoked[0][0]).name, "clush")

    def test_missing_remote_sentinel_is_not_reported_as_success(self):
        def runner(argv, **kwargs):
            return subprocess.CompletedProcess(argv, 0, "ordinary output\n", "")

        with tempfile.TemporaryDirectory() as temp:
            config = BaremetalExecutionConfig(
                output_root=Path(temp),
                transport="ssh",
                ssh_executable="/usr/bin/ssh",
            )
            result = BaremetalClusterExecutor(
                ["node01"], config, runner=runner
            ).execute("inventory", ["true"], run_id="missing-sentinel")

            self.assertEqual(result.status, "FAILED")
            self.assertEqual(result.nodes["node01"].error_kind, "REMOTE_RESULT_MISSING")
            self.assertEqual(result.nodes["node01"].returncode, 255)

    def test_string_is_not_accepted_as_a_command_sequence(self):
        with tempfile.TemporaryDirectory() as temp:
            config = BaremetalExecutionConfig(
                output_root=Path(temp),
                transport="ssh",
                ssh_executable="/usr/bin/ssh",
            )
            executor = BaremetalClusterExecutor(["node01"], config)
            with self.assertRaises(ValueError):
                executor.execute("inventory", "uname -r", run_id="string")

    def test_clush_controller_error_is_attached_only_to_named_node(self):
        def runner(argv, **kwargs):
            stdout_dir = Path(argv[argv.index("--outdir") + 1])
            sentinel = sentinel_from_argv(argv)
            (stdout_dir / "node01").write_text(
                f"ok\n{sentinel}=0\n", encoding="utf-8"
            )
            return subprocess.CompletedProcess(
                argv,
                255,
                "",
                "clush: node02: ssh: connect to host node02: Connection refused\n",
            )

        with tempfile.TemporaryDirectory() as temp:
            config = BaremetalExecutionConfig(
                output_root=Path(temp),
                transport="clush",
                clush_executable="/usr/bin/clush",
            )
            result = BaremetalClusterExecutor(
                ["node01", "node02"], config, runner=runner
            ).execute("inventory", ["hostname"], run_id="clush-errors")

            self.assertTrue(result.nodes["node01"].success)
            self.assertEqual(result.nodes["node01"].stderr, "")
            self.assertEqual(result.nodes["node02"].error_kind, "SSH_TRANSPORT_FAILED")
            self.assertIn("Connection refused", result.nodes["node02"].stderr)

    def test_default_ssh_streams_and_bounds_output_before_persisting(self):
        class FakeProcess:
            def __init__(self, argv):
                sentinel = sentinel_from_argv(argv)
                self.stdout = io.BytesIO(("A" * 8192 + f"\n{sentinel}=0\n").encode())
                self.stderr = io.BytesIO(("E" * 4096).encode())
                self.returncode = None

            def wait(self, timeout=None):
                self.returncode = 0
                return 0

            def terminate(self):
                self.returncode = -15

            def kill(self):
                self.returncode = -9

        with tempfile.TemporaryDirectory() as temp:
            seen = []
            config = BaremetalExecutionConfig(
                output_root=Path(temp),
                transport="ssh",
                ssh_executable="/usr/bin/ssh",
                max_stdout_bytes=1024,
                max_stderr_bytes=1024,
            )
            executor = BaremetalClusterExecutor(
                ["node01"], config, popen=lambda argv, **kwargs: FakeProcess(argv)
            )
            result = executor.execute(
                "inventory",
                ["true"],
                run_id="bounded-popen",
                result_handler=lambda item: seen.append((item.node, item.stdout_truncated)),
                release_output=True,
            )

            node = result.nodes["node01"]
            self.assertEqual(seen, [("node01", True)])
            self.assertEqual(node.stdout, "")
            self.assertEqual(node.stderr, "")
            self.assertTrue(node.stdout_truncated)
            self.assertTrue(node.stderr_truncated)
            self.assertGreater(node.stdout_total_bytes, 8192)
            stdout_path = Path(node.result_dir) / "stdout.txt"
            self.assertLess(stdout_path.stat().st_size, 1200)
            self.assertIn("omitted", stdout_path.read_text(encoding="utf-8"))

    def test_default_clush_streams_and_bounds_controller_output(self):
        class FakeProcess:
            def __init__(self, argv):
                stdout_dir = Path(argv[argv.index("--outdir") + 1])
                sentinel = sentinel_from_argv(argv)
                (stdout_dir / "node01").write_text(
                    f"ok\n{sentinel}=0\n", encoding="utf-8"
                )
                self.stdout = io.BytesIO(b"C" * 8192)
                self.stderr = io.BytesIO(b"E" * 4096)
                self.returncode = None

            def wait(self, timeout=None):
                self.returncode = 0
                return 0

            def terminate(self):
                self.returncode = -15

            def kill(self):
                self.returncode = -9

        with tempfile.TemporaryDirectory() as temp:
            config = BaremetalExecutionConfig(
                output_root=Path(temp),
                transport="clush",
                clush_executable="/usr/bin/clush",
                max_stdout_bytes=1024,
                max_stderr_bytes=1024,
            )
            result = BaremetalClusterExecutor(
                ["node01"], config, popen=lambda argv, **kwargs: FakeProcess(argv)
            ).execute("inventory", ["true"], run_id="bounded-clush-controller")

            self.assertTrue(result.nodes["node01"].success)
            run_dir = Path(result.run_dir)
            self.assertLess((run_dir / "controller.stdout").stat().st_size, 1200)
            self.assertLess((run_dir / "controller.stderr").stat().st_size, 1200)
            self.assertIn(
                "omitted", (run_dir / "controller.stdout").read_text(encoding="utf-8")
            )
            self.assertIn(
                "omitted", (run_dir / "controller.stderr").read_text(encoding="utf-8")
            )
    def test_large_node_set_is_consumed_and_released_as_each_future_completes(self):
        nodes = [f"node{index:04d}" for index in range(257)]
        consumed = []

        def runner(argv, **kwargs):
            sentinel = sentinel_from_argv(argv)
            return subprocess.CompletedProcess(
                argv, 0, "X" * 4096 + f"\n{sentinel}=0\n", ""
            )

        with tempfile.TemporaryDirectory() as temp:
            config = BaremetalExecutionConfig(
                output_root=Path(temp),
                transport="ssh",
                concurrency=128,
                ssh_executable="/usr/bin/ssh",
                max_stdout_bytes=1024,
            )
            result = BaremetalClusterExecutor(nodes, config, runner=runner).execute(
                "inventory",
                ["true"],
                run_id="scale-release",
                result_handler=lambda item: consumed.append(item.node),
                release_output=True,
            )

            self.assertEqual(set(consumed), set(nodes))
            self.assertEqual(len(result.nodes), len(nodes))
            self.assertTrue(all(item.stdout == "" for item in result.nodes.values()))
            self.assertTrue(all(item.stdout_truncated for item in result.nodes.values()))
            self.assertTrue(all(item.stdout_total_bytes > 4096 for item in result.nodes.values()))

if __name__ == "__main__":
    unittest.main()
