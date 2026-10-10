# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Regression tests for the post-env.sh probe execution contract."""

import ast
import base64
import io
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
import zlib
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from hcu_envcheck import pod_probe
from hcu_envcheck.baremetal_cluster import BaremetalPreflightPolicy, build_remote_probe_command
from hcu_envcheck.baremetal_cluster import run_baremetal_cluster_preflight
from hcu_envcheck.baremetal import BaremetalExecutionConfig
from hcu_envcheck.environment import evaluate_environment
from cluster_run.env import bootstrap_command, source_body


def embedded_source(command):
    encoded = re.search(r"b64decode\('([^']+)'\)", command[-1]).group(1)
    return zlib.decompress(base64.b64decode(encoded)).decode("utf-8")


class EnvironmentContractTests(unittest.TestCase):
    def test_selected_non_opt_dtk_and_reported_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "shared-dtk"
            root.mkdir()
            (root / ".dtk_version").write_text("DTK-26.04-selected\n", encoding="utf-8")
            with patch.dict(os.environ, {"ROCM_PATH": str(root), "PATH": ""}, clear=True):
                inventory = pod_probe.collect_dtk({})
            self.assertEqual(inventory["version_file"]["value"], "DTK-26.04-selected")
            self.assertEqual(inventory["version_file"]["source"], "ROCM_PATH")
            self.assertEqual(inventory["selected_root"]["path"], os.path.realpath(root))
            payload = {"dtk": inventory, "python": {"version": "3.12", "packages": {}},
                       "system": {}, "network": {}, "driver": {}, "torch": {}}
            _, summary, _ = evaluate_environment(payload, expected_device_count=None,
                require_compiler=False, require_rdma=False, minimum_rdma_devices=0,
                require_rccl=False, require_ucx=False, required_python_packages=())
            self.assertEqual(summary["dtk_version"], "DTK-26.04-selected")
            self.assertEqual(summary["dtk_source"]["version_file"]["source"], "ROCM_PATH")

    def test_missing_selected_version_does_not_use_another_installation(self):
        with tempfile.TemporaryDirectory() as tmp:
            other = Path(tmp) / "other"
            other.mkdir()
            (other / ".dtk_version").write_text("wrong", encoding="utf-8")
            with patch.dict(os.environ, {"DTK_PATH": str(Path(tmp) / "missing"), "ROCM_PATH": str(other)}, clear=True):
                inventory = pod_probe.collect_dtk({})
            self.assertIsNone(inventory["version_file"])
            self.assertEqual(inventory["selected_root"]["source"], "DTK_PATH")

    def test_path_tool_is_authoritative_and_dtk_root_can_come_from_tool(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".dtk_version").write_text("path-selected", encoding="utf-8")
            hipcc = str(root / "hip" / "bin" / "hipcc")
            with patch.dict(os.environ, {"PATH": str(root / "hip" / "bin")}, clear=True), \
                 patch.object(pod_probe.shutil, "which", side_effect=lambda name: hipcc if name == "hipcc" else None):
                tool = pod_probe.resolve_tool("hipcc")
                inventory = pod_probe.collect_dtk({"hipcc": tool})
            self.assertEqual(tool["source"], "PATH")
            self.assertEqual(inventory["version_file"]["value"], "path-selected")

    def test_libraries_follow_loader_environment_and_keep_alternative_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            override, toolkit, conda = root / "override", root / "dtk", root / "conda"
            for directory in (override, toolkit / "lib", conda / "lib"):
                directory.mkdir(parents=True)
            (override / "librccl.so.2").write_text("", encoding="utf-8")
            (toolkit / "lib" / "librccl.so.1").write_text("", encoding="utf-8")
            (toolkit / "lib" / "libamdhip64.so.6").write_text("", encoding="utf-8")
            (conda / "lib" / "libucp.so.0").write_text("", encoding="utf-8")
            with patch.dict(os.environ, {"LD_LIBRARY_PATH": str(override), "ROCM_PATH": str(toolkit),
                                          "CONDA_PREFIX": str(conda), "PATH": ""}, clear=True):
                inventory = pod_probe.collect_libraries()
            self.assertEqual(inventory["selected_components"]["rccl"]["source"], "LD_LIBRARY_PATH")
            self.assertEqual(inventory["selected_components"]["ucp"]["source"], "CONDA_PREFIX")
            self.assertGreaterEqual(len(inventory["component_candidates"]["rccl"]), 2)
            self.assertTrue(inventory["hcu_hip_runtime"]["detected"])
            self.assertEqual(inventory["selected_components"]["hcu_hip_runtime"]["source"], "ROCM_PATH")
            self.assertNotIn("libamdhip64", json.dumps(inventory))

    def test_environment_snapshot_has_runtime_settings_but_not_rank_or_secrets(self):
        values = {"CONDA_PREFIX": "/share/train", "DTK_HOME": "/share/dtk", "UCX_TLS": "rc",
                  "NCCL_IB_HCA": "mlx5_0", "RANK": "7", "WORLD_SIZE": "64", "API_TOKEN": "hidden"}
        with patch.dict(os.environ, values, clear=True):
            actual = pod_probe.collect_runtime_env()
        self.assertEqual(set(actual), {"CONDA_PREFIX", "DTK_HOME", "UCX_TLS", "NCCL_IB_HCA"})

    def test_embedded_selected_software_uses_shared_collectors_without_opt_prepend(self):
        command = build_remote_probe_command(BaremetalPreflightPolicy(), "python3")
        source = embedded_source(command)
        self.assertNotIn('_default_tool_paths', source)
        module = ast.parse(source)
        assignment = next(node for node in module.body if isinstance(node, ast.Assign)
                          and any(isinstance(target, ast.Name) and target.id == "_software_probe_source" for target in node.targets))
        software_source = ast.literal_eval(assignment.value)
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / ".dtk_version").write_text("embedded-selected", encoding="utf-8")
            output = io.StringIO()
            with patch.dict(os.environ, {"ROCM_PATH": tmp, "PATH": ""}, clear=True), redirect_stdout(output):
                exec(compile(software_source, "selected-software-probe", "exec"), {"__name__": "probe_test"})
            inventory = json.loads(output.getvalue().strip().splitlines()[-1])
            self.assertEqual(inventory["dtk"]["version_file"]["value"], "embedded-selected")
            self.assertEqual(inventory["runtime_env"]["ROCM_PATH"], tmp)

    def test_resource_collector_does_not_probe_platform_software(self):
        with patch.object(pod_probe, "collect_system", return_value={"meminfo": {"MemTotal": "1024 kB"}}), \
             patch.object(pod_probe, "collect_python", side_effect=AssertionError("platform only")), \
             patch.object(pod_probe, "collect_dtk", side_effect=AssertionError("platform only")), \
             patch.object(pod_probe, "collect_network", side_effect=AssertionError("platform only")), \
             patch.object(pod_probe, "collect_libraries", side_effect=AssertionError("platform only")):
            output = io.StringIO()
            with redirect_stdout(output):
                pod_probe.main(("resource",))
            self.assertEqual(json.loads(output.getvalue())["system"]["meminfo"]["MemTotal"], "1024 kB")

    def test_container_workdir_precedes_source_and_handles_spaces(self):
        policy = BaremetalPreflightPolicy(execution_scope="container", container_name="worker",
                                         container_workdir="/workspace/my project", env_script="./env/setup.sh")
        command = build_remote_probe_command(policy, "python3")
        docker = shlex.split(command[2])[1:]
        self.assertEqual(docker[:5], ["docker", "exec", "-w", "/workspace/my project", "worker"])
        body = docker[-1]
        self.assertLess(body.index("cd -- '/workspace/my project'"), body.index("source ./env/setup.sh"))
        self.assertLess(body.index("source ./env/setup.sh"), body.index("exec python3"))

    def test_container_scope_without_env_script_still_enters_container(self):
        policy = BaremetalPreflightPolicy(execution_scope="container", container_name="worker", container_shell="sh")
        docker = shlex.split(build_remote_probe_command(policy, "python3")[2])
        self.assertEqual(docker[:6], ["exec", "docker", "exec", "worker", "sh", "-c"])
        self.assertNotIn("source ", docker[-1])

    def test_sh_bootstrap_uses_dot_and_validates_workdir_overrides(self):
        policy = BaremetalPreflightPolicy(execution_scope="container", container_name="worker", env_script="./env.sh")
        command = build_remote_probe_command(policy, "python3", container_shell="sh", container_workdir="/workspace")
        body = shlex.split(command[2])[-1]
        self.assertIn("\n. ./env.sh\n", body)
        for value in ("relative", "/bad\npath", ""):
            with self.assertRaises(ValueError):
                build_remote_probe_command(policy, "python3", container_workdir=value)

    def test_optional_task_guard_covers_shell_startup_and_environment_bootstrap(self):
        token = "contract_run_token_123"
        for scope in ("host", "container"):
            with self.subTest(scope=scope):
                policy = BaremetalPreflightPolicy(execution_scope=scope,
                    container_name="worker" if scope == "container" else None,
                    container_workdir="/workspace" if scope == "container" else None,
                    env_script="./env.sh", run_token=token)
                with patch("cluster_run.task_control.managed_command",
                           side_effect=lambda command, token: ["guard-marker", token, *command]) as guard:
                    command = build_remote_probe_command(policy, "python3")
                guard.assert_called_once()
                guarded_command = guard.call_args.args[0]
                self.assertEqual(guarded_command[:2], ["bash", "-lc"])
                self.assertEqual(guard.call_args.args[1], token)
                body = guarded_command[2]
                self.assertLess(body.index("source ./env.sh"), body.index("exec python3"))
                if scope == "container":
                    self.assertLess(body.index("cd -- /workspace"), body.index("source ./env.sh"))
                    self.assertEqual(shlex.split(command[2]),
                                     ["exec", "docker", "exec", "-w", "/workspace", "worker",
                                      "guard-marker", token, *guarded_command])
                else:
                    self.assertEqual(command, ["guard-marker", token, *guarded_command])

    def test_guard_also_encloses_sh_bootstrap_without_env_script(self):
        policy = BaremetalPreflightPolicy(execution_scope="container", container_name="worker",
            container_shell="sh", container_workdir="/workspace", run_token="guard_without_env_123")
        with patch("cluster_run.task_control.managed_command",
                   side_effect=lambda command, token: ["guard-marker", token, *command]) as guard:
            command = build_remote_probe_command(policy, "python3")
        shell = guard.call_args.args[0]
        self.assertEqual(shell[:2], ["sh", "-c"])
        self.assertTrue(shell[2].startswith("set -e\ncd -- /workspace\nexec python3"))
        self.assertEqual(shlex.split(command[2])[6], "guard-marker")

    def test_bootstrap_workdir_is_restored_before_source_in_both_shells(self):
        for shell, operator, option in (("bash", "source", "-lc"), ("sh", ".", "-c")):
            with self.subTest(shell=shell):
                command = bootstrap_command("./env.sh", ["python3", "check.py"], shell=shell,
                    workdir="/workspace/a 'quoted' directory", exports={"HCU_GROUP_NAME": "g0"})
                self.assertEqual(command[:2], [shell, option])
                lines = command[2].splitlines()
                self.assertEqual(lines[0], "set -e")
                self.assertEqual(shlex.split(lines[1]), ["cd", "--", "/workspace/a 'quoted' directory"])
                self.assertEqual(lines[2], f"{operator} ./env.sh")
                self.assertEqual(lines[3], "export HCU_GROUP_NAME=g0")
                self.assertEqual(lines[4], "exec python3 check.py")

    def test_default_bootstrap_prefix_stays_compatible_with_rank_builder(self):
        self.assertEqual(source_body("/share/env.sh", ["true"]).splitlines()[:2],
                         ["set -e", "source /share/env.sh"])
        self.assertEqual(bootstrap_command("/share/env.sh", ["true"]),
                         ["bash", "-lc", "set -e\nsource /share/env.sh\nexec true"])
        for directory in ("", "  ", "/bad\npath", "/bad\x00path", "/bad\x7fpath"):
            with self.subTest(directory=directory), self.assertRaises(ValueError):
                source_body("/env.sh", ["true"], workdir=directory)

    def test_default_probe_does_not_invoke_task_control(self):
        with patch("cluster_run.task_control.managed_command", side_effect=AssertionError("not opted in")):
            command = build_remote_probe_command(BaremetalPreflightPolicy(), "python3")
        self.assertEqual(command[:2], ["python3", "-c"])
        for token in ("short", "bad token" * 4, "a" * 97):
            with self.assertRaises(ValueError):
                BaremetalPreflightPolicy(run_token=token).validate()

    def test_basic_preflight_preserves_cli_session_cancellation_event(self):
        event = threading.Event()
        with tempfile.TemporaryDirectory() as tmp:
            config = BaremetalExecutionConfig(output_root=Path(tmp), cancel_event=event)
            fake_result = SimpleNamespace(node="n01")
            fake_record = {"node": "n01", "status": "READY", "reachable": True, "device_count": 0,
                           "environment": {"mem_total": "1024 kB"}, "devices": []}
            def execute(_stage, _command, *, result_handler, release_output):
                result_handler(fake_result)
                return SimpleNamespace(transport="ssh", run_dir=tmp, nodes={"n01": fake_result})
            with patch("hcu_envcheck.baremetal_cluster.BaremetalClusterExecutor") as executor, \
                 patch("hcu_envcheck.baremetal_cluster.evaluate_node_result", return_value=fake_record):
                executor.return_value.execute.side_effect = execute
                run_baremetal_cluster_preflight(nodes=["n01"], execution_config=config,
                    policy=BaremetalPreflightPolicy(check_categories=("resource",)), output_dir=Path(tmp),
                    run_label="container_resource")
                self.assertIs(executor.call_args.args[1].cancel_event, event)


@unittest.skipUnless(sys.platform.startswith("linux") and all(shutil.which(name) for name in ("bash", "setsid", "flock")),
                     "real bootstrap cancellation requires Linux /proc + bash/setsid/flock")
class BasicBootstrapCancellationTests(unittest.TestCase):
    def test_stop_during_env_script_kills_bootstrap_children_before_probe_starts(self):
        from cluster_run.task_control import _guard_command
        token = uuid.uuid4().hex
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            parent_path, child_path, probe_path = (root / name for name in ("env.pid", "child.pid", "probe.started"))
            env_script = root / "env.sh"
            env_script.write_text(
                "trap '' TERM\n"
                f"printf '%s' \"$BASHPID\" > {shlex.quote(str(parent_path))}\n"
                "sleep 60 &\n"
                f"printf '%s' \"$!\" > {shlex.quote(str(child_path))}\nwait\n", encoding="utf-8")
            probe = root / "probe.sh"
            probe.write_text(f"#!/bin/sh\nprintf started > {shlex.quote(str(probe_path))}\n", encoding="utf-8")
            probe.chmod(0o700)
            command = build_remote_probe_command(BaremetalPreflightPolicy(env_script=str(env_script), run_token=token), str(probe))
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                deadline = time.monotonic() + 8
                while (not child_path.exists() or not child_path.read_text().strip()) and time.monotonic() < deadline:
                    time.sleep(0.03)
                self.assertTrue(child_path.exists(), "env.sh never reached the blocking setup stage")
                pids = [int(path.read_text()) for path in (parent_path, child_path)]
                self.assertFalse(probe_path.exists())
                stopped = subprocess.run(_guard_command("stop", token, ["0", "5"]),
                    capture_output=True, text=True, timeout=20)
                self.assertEqual(stopped.returncode, 0, stopped.stderr)
                evidence = next(json.loads(line.split("=", 1)[1]) for line in stopped.stdout.splitlines()
                                if line.startswith("__HCU_TASK_STOP__="))
                self.assertEqual((evidence["token"], evidence["status"], evidence["remaining"]), (token, "STOPPED", 0))
                process.communicate(timeout=5)
                self.assertFalse(probe_path.exists(), "probe started after cancellation during env.sh")
                for pid in pids:
                    stat = Path(f"/proc/{pid}/stat")
                    if stat.exists():
                        self.assertIn(stat.read_text().rsplit(") ", 1)[1].split()[0], ("Z", "X"))
            finally:
                subprocess.run(_guard_command("stop", token, ["0", "5"]), capture_output=True, timeout=20)
                try:
                    process.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate(timeout=5)


if __name__ == "__main__":
    unittest.main()
