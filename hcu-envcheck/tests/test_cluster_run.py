# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import io
import os
import shlex
import subprocess
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch

from cluster_run.active import ActiveTestExecutor
from cluster_run.cli import _log, _log_basic_execution, build_parser, main
from cluster_run.consistency import build_consistency_summary
from cluster_run.env import bootstrap_command, source_body
from cluster_run.hostfile import materialize_groups, split_nodes
from cluster_run.launchers import LaunchContext, build_mpirun_command, build_ssh_torchrun_commands
from cluster_run.testsuites import resolve_test_command
from cluster_run.preflight import ContainerPreflightError
from cluster_run.payloads import ib_write_bw_worker
from cluster_run.payloads import rccl_worker


class ClusterRunEnvironmentTests(unittest.TestCase):
    def test_relative_output_dir_materializes_absolute_group_hostfile(self):
        with tempfile.TemporaryDirectory() as temp:
            original = Path.cwd()
            try:
                os.chdir(temp)
                Path("hosts").write_text("node01\nnode02\n", encoding="utf-8")
                with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                    rc = main(["shared-conda", "rccl", "-f", "hosts",
                               "--profile", "worker",
                               "--env-script", "/share/env.sh", "--group-size", "2",
                               "--dry-run", "-o", "relative-out"])
                self.assertEqual(rc, 0)
                report = json.loads(next(Path("relative-out").glob("conda_rccl_*/active-result.json")).read_text())
                body = report["groups"][0]["commands"][0]["command"][-1]
                # Rank guard is a quoted multiline argv; parse the complete exec.
                argv = shlex.split(body.split("\nexec ", 1)[1])
                self.assertTrue(Path(argv[argv.index("--hostfile") + 1]).is_absolute())
            finally:
                os.chdir(original)

    def test_ib_binary_presence_does_not_count_as_bandwidth_pass(self):
        output = io.StringIO()
        with patch("cluster_run.payloads.ib_write_bw_worker.shutil.which", return_value="/usr/bin/ib_write_bw"):
            with redirect_stdout(output):
                rc = ib_write_bw_worker.main([])
        self.assertNotEqual(rc, 0)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["status"], "PREREQUISITE_ONLY")

    def test_ib_prerequisite_output_is_incomplete_not_pass(self):
        with tempfile.TemporaryDirectory() as temp:
            executor = ActiveTestExecutor(
                scenario="shared-conda", test_name="ib-write-bw", nodes=["node01"],
                env_script="/share/env.sh", output_dir=Path(temp), group_size=1,
                slots=1, launcher="mpirun", dry_run=True,
            )
            evidence = json.dumps({"test": "ib-write-bw", "status": "PREREQUISITE_ONLY"})
            # The active IB adapter must perform pairs, not accept the retired prerequisite-only worker.
            self.assertEqual(executor._result_status(2, evidence), "FAIL")
            self.assertEqual(executor._result_status(255, "ssh failed"), "FAIL")

    def test_single_rank_rccl_never_claims_collective_pass(self):
        output = io.StringIO()
        topology = {"rank": 0, "world_size": 1, "local_rank": 0, "host": "n01"}
        with patch("cluster_run.payloads.rccl_worker.configure_direct_mpi_environment", return_value=topology):
            with redirect_stdout(output):
                rc = rccl_worker.main([])
        self.assertEqual(rc, 2)
        self.assertEqual(json.loads(output.getvalue())["status"], "INCOMPLETE")

    def test_missing_hostfile_error_includes_operation_and_path(self):
        with tempfile.TemporaryDirectory() as temp:
            missing = Path(temp) / "missing-hostfile"
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                rc = main(["per-node-container", "resource", "-f", str(missing),
                           "--env-script", "/remote/env.sh", "--container", "worker"])
        self.assertEqual(rc, 3)
        self.assertIn("RESULT        TOOL_ERROR", stderr.getvalue())
        self.assertIn("scenario=per-node-container operation=resource", stderr.getvalue())
        self.assertIn(str(missing), stderr.getvalue())
        self.assertIn("HCU_ENVCHECK_DEBUG=1", stderr.getvalue())

    def test_failed_node_prints_reason_counts_and_remote_stderr_excerpt(self):
        with tempfile.TemporaryDirectory() as temp:
            evidence = Path(temp)
            (evidence / "stderr.txt").write_text("docker: container missing\nmore detail\n", encoding="utf-8")
            report = {"transport": "ssh", "evidence_dir": temp, "nodes": [{
                "node": "node01", "status": "INCOMPLETE", "reachable": False,
                "probe_transport": {"returncode": 255, "error_kind": "SSH_TRANSPORT_FAILED", "result_dir": temp},
                "findings": [{"reason_code": "REMOTE_RESULT_MISSING", "message": "remote probe failed"}],
            }]}
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                _log_basic_execution(report, SimpleNamespace())
            stderr_every = io.StringIO()
            with redirect_stderr(stderr_every):
                _log_basic_execution(report, SimpleNamespace(log_detail="every"))
        output = stderr.getvalue()
        # milestone (default): one folded line per identical status+findings signature.
        self.assertIn("REMOTE_RESULT_MISSING×1", output)
        self.assertIn("INCOMPLETE ×1 nodes=node01", output)
        self.assertNotIn("stderr_file=", output)

        output_every = stderr_every.getvalue()
        self.assertIn("REMOTE_RESULT_MISSING×1", output_every)
        self.assertIn("remote_stderr=docker: container missing", output_every)
        self.assertIn("stderr_file=", output_every)
        self.assertIn("transport_metadata=", output_every)

    def test_failed_active_group_prints_stderr_path_and_excerpt(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            hostfile = root / "hostfile"
            hostfile.write_text("node01\n", encoding="utf-8")
            group_dir = root / "groups" / "group-000"
            group_dir.mkdir(parents=True)
            (group_dir / "stderr.log").write_text("mpirun: launcher failed\n", encoding="utf-8")
            report = {"status": "FAIL", "group_count": 1, "group_slots": 1,
                      "launcher": "mpirun", "groups": [{"group": "group-000", "nodes": ["node01"],
                      "status": "FAIL", "leader": "node01", "returncode": 127}]}
            with patch("cluster_run.cli.ActiveTestExecutor") as executor:
                executor.return_value.run.return_value = report, root
                stderr = io.StringIO()
                with redirect_stdout(io.StringIO()), redirect_stderr(stderr):
                    rc = main(["shared-conda", "rccl", "-f", str(hostfile),
                               "--env-script", "/remote/env.sh", "--launcher", "mpirun"])
        self.assertEqual(rc, 2)  # The current operation failed; this never gates another command.
        self.assertIn("returncode=127", stderr.getvalue())
        self.assertIn("mpirun: launcher failed", stderr.getvalue())

    def test_container_precheck_failure_returns_reason_without_launch(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            hostfile = root / "hostfile"
            hostfile.write_text("node01\n", encoding="utf-8")
            preflight = {"status": "FAIL", "issues": [{"node": "node01", "code": "CONTAINER_MISSING", "message": "container missing"}],
                         "recreate_command_template": "bin/hcu-cluster-run ... --recreate"}
            with patch("cluster_run.cli.ActiveTestExecutor") as executor:
                executor.return_value.run.side_effect = ContainerPreflightError(preflight)
                stderr = io.StringIO()
                stdout = io.StringIO()
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    rc = main(["per-node-container", "rccl", "-f", str(hostfile),
                               "--env-script", "/remote/env.sh", "--container", "worker",
                               "-i", "image:tag"])
        self.assertEqual(rc, 3)
        self.assertIn("CONTAINER_MISSING", stderr.getvalue())
        self.assertIn("not run automatically", stderr.getvalue())
        self.assertIn("PRECHECK_FAILED", stdout.getvalue())
        self.assertNotIn("PREFLIGHT", stdout.getvalue())

    def test_ib_state_is_integrated_into_platform_not_a_public_operation(self):
        stderr = io.StringIO()
        with redirect_stderr(stderr), self.assertRaises(SystemExit) as raised:
            main(["shared-conda", "ib-state", "-f", "nodes.txt"])
        self.assertEqual(raised.exception.code, 3)
        self.assertIn("invalid choice", stderr.getvalue())

    def test_cli_defaults_to_ssh_and_accepts_explicit_clush(self):
        common = [
            "shared-conda",
            "platform",
            "-f",
            "nodes.txt",
            "--env-script",
            "/share/hcu/env.sh",
        ]
        self.assertEqual(build_parser().parse_args(common).transport, "ssh")
        self.assertEqual(
            build_parser().parse_args([*common, "--transport", "clush"]).transport,
            "clush",
        )

    def test_level_log_uses_color_only_for_interactive_terminal(self):
        class TtyBuffer(io.StringIO):
            def isatty(self):
                return True

        interactive = TtyBuffer()
        with patch.dict(os.environ, {}, clear=True):
            with patch("cluster_run.cli.sys.stderr", interactive):
                _log("WARN", "attention")
        self.assertIn("\033[33m[WARN]\033[0m", interactive.getvalue())

        redirected = io.StringIO()
        with patch("cluster_run.cli.sys.stderr", redirected):
            _log("WARN", "attention")
        self.assertEqual(redirected.getvalue(), "[WARN] attention\n")

    def test_basic_command_persists_node_first_json_after_consistency_update(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            hostfile = root / "nodes.txt"
            hostfile.write_text("node01\n", encoding="utf-8")
            run_dir = root / "result"
            run_dir.mkdir()
            json_path = run_dir / "cluster-result.json"
            markdown_path = run_dir / "cluster-summary.md"
            markdown_path.write_text("# summary\n", encoding="utf-8")
            report = {
                "status": "READY",
                "transport": "ssh",
                "evidence_dir": str(run_dir / "evidence"),
                "summary": {"node_count": 1},
                "nodes": [
                    {
                        "node": "node01",
                        "status": "READY",
                        "reachable": True,
                        "checks": [{"check_id": "DRIVER", "status": "PASS"}],
                        "environment": {},
                        "metric_summary": {},
                        "findings": [],
                        "probe_transport": {"returncode": 0},
                    }
                ],
            }
            with patch(
                "cluster_run.cli.run_baremetal_cluster_preflight",
                return_value=(report, json_path, markdown_path),
            ) as preflight:
                with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                    rc = main(
                        [
                            "shared-conda",
                            "platform",
                            "-f",
                            str(hostfile),
                            "-o",
                            str(root / "output"),
                        ]
                    )

            persisted = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertTrue(preflight.call_args.kwargs["extra_checks"].ib_state.enabled)
            self.assertIsNotNone(preflight.call_args.kwargs["extra_command_wrapper"])
            self.assertEqual(rc, 0)
            self.assertEqual(persisted["schema_version"], "2.0")
            self.assertIn("node01", persisted["node_status"]["nodes"])
            self.assertIn("consistency_summary", persisted["cluster"])
            self.assertIn("## 集群汇总与一致性", markdown_path.read_text(encoding="utf-8"))

    def test_env_script_is_the_first_step_of_every_wrapper(self):
        body = source_body("/share/hcu/env.sh", ["python", "probe.py"])
        self.assertTrue(body.startswith("set -e\nsource /share/hcu/env.sh"))
        self.assertIn("exec python probe.py", body)
        self.assertEqual(
            bootstrap_command("/share/hcu/env.sh", ["true"])[:2],
            ["bash", "-lc"],
        )

    def test_omitted_env_script_uses_current_target_environment(self):
        body = source_body(None, ["python3", "probe.py"])
        self.assertEqual(body.splitlines(), ["set -e", "exec python3 probe.py"])
        self.assertNotIn("source ", body)

    def test_failed_env_script_stops_worker(self):
        with tempfile.TemporaryDirectory() as temp:
            script = Path(temp) / "env.sh"
            script.write_text("false\n", encoding="utf-8")
            worker = Path(temp) / "worker.marker"
            command = bootstrap_command(str(script), ["touch", str(worker)])
            # Git Bash is available on the Windows development host.
            shell = os.environ.get("GIT_BASH") or ("C:/Program Files/Git/bin/bash.exe" if os.name == "nt" else "bash")
            command[0] = shell
            result = subprocess.run(command, capture_output=True, text=True, check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(worker.exists())


class ClusterRunGroupingTests(unittest.TestCase):
    def test_group_size_and_remainder_are_preserved(self):
        self.assertEqual(
            split_nodes(["n1", "n2", "n3", "n4", "n5"], group_size=2),
            [("n1", "n2"), ("n3", "n4"), ("n5",)],
        )

    def test_materialized_groups_use_one_shared_hostfile_per_group(self):
        with tempfile.TemporaryDirectory() as temp:
            groups = materialize_groups(
                ["n1", "n2", "n3"],
                Path(temp),
                group_size=2,
            )
            self.assertEqual(groups[0].name, "group-000")
            self.assertEqual(
                groups[0].hostfile.read_text(encoding="utf-8"),
                "n1 slots=1\nn2 slots=1\n",
            )
            self.assertEqual(groups[1].nodes, ("n3",))

    def test_materialized_hostfile_can_reserve_multiple_mpirun_slots(self):
        with tempfile.TemporaryDirectory() as temp:
            groups = materialize_groups(
                ["n1", "n2"],
                Path(temp),
                group_size=2,
                slots_per_node=8,
            )
            self.assertEqual(
                groups[0].hostfile.read_text(encoding="utf-8"),
                "n1 slots=8\nn2 slots=8\n",
            )


class ClusterRunConsistencyTests(unittest.TestCase):
    def test_transient_utilization_does_not_split_configuration(self):
        records = [
            {"node": node, "status": "READY", "device_count": 8,
             "metric_summary": {"max_hcu_util_percent": utilization}}
            for node, utilization in (("n01", 0), ("n02", 50))
        ]
        self.assertEqual(build_consistency_summary(records)["configuration_group_count"], 1)

    def test_report_counts_same_configuration_and_statuses(self):
        records = [
            {
                "node": "node01",
                "status": "READY",
                "device_count": 8,
                "environment": {"driver_version": "6.4", "dtk_version": "26.04"},
                "metric_summary": {"max_hcu_util_percent": 0},
            },
            {
                "node": "node02",
                "status": "READY",
                "device_count": 8,
                "environment": {"driver_version": "6.4", "dtk_version": "26.04"},
                "metric_summary": {"max_hcu_util_percent": 0},
            },
            {
                "node": "node03",
                "status": "BLOCKED",
                "device_count": 8,
                "environment": {"driver_version": "6.3", "dtk_version": "26.04"},
                "metric_summary": {"max_hcu_util_percent": 0},
            },
        ]
        report = build_consistency_summary(records)
        self.assertEqual(report["node_count"], 3)
        self.assertEqual(report["configuration_group_count"], 2)
        self.assertEqual(report["passed_node_count"], 2)
        self.assertEqual(report["failed_node_count"], 1)
        self.assertEqual(report["configuration_groups"][0]["nodes"], ["node01", "node02"])
        self.assertTrue(report["differences_from_reference"])


class ClusterRunLauncherTests(unittest.TestCase):
    def _context(self, launcher: str) -> LaunchContext:
        return LaunchContext(
            test_name="rccl",
            launcher=launcher,
            env_script="/share/hcu/env.sh",
            group_name="group-000",
            group_hostfile=Path("/share/job/group-000/hostfile"),
            nodes=("node01", "node02"),
            test_command=("bash", "test.sh"),
            nproc_per_node=8,
            np=None,
            master_port=29500,
        )

    def test_default_mpirun_torchrun_has_torchrun_and_bootstrap(self):
        command = build_mpirun_command(self._context("mpirun-torchrun"))
        text = json.dumps(command)
        self.assertIn("mpirun", command[0])
        self.assertIn("torch.distributed.run", text)
        self.assertIn("/share/hcu/env.sh", text)
        self.assertIn("HCU_RANK", text)
        self.assertEqual(command[command.index("--map-by") + 1], "ppr:1:node")
        self.assertIn("set -e\\nsource", text)

    def test_mpi_rank_has_its_own_remote_timeout(self):
        original = self._context("mpirun-torchrun")
        bounded = LaunchContext(**{**original.__dict__, "timeout_seconds": 60})
        command = build_mpirun_command(bounded)
        self.assertIn("timeout --signal=TERM --kill-after=15s 60s python3 -m torch.distributed.run", command[-1])

    def test_direct_mpirun_uses_mpi_rank_normalization(self):
        command = build_mpirun_command(self._context("mpirun"))
        text = json.dumps(command)
        self.assertIn("OMPI_COMM_WORLD_RANK", text)
        self.assertNotIn("torchrun", text)
        self.assertEqual(command[command.index("--map-by") + 1], "ppr:8:node")

    def test_direct_mpirun_rejects_partial_node_rank_layout(self):
        original = self._context("mpirun")
        partial = LaunchContext(**{**original.__dict__, "np": 2})
        with self.assertRaisesRegex(ValueError, "--np must equal"):
            build_mpirun_command(partial)

    def test_ssh_torchrun_assigns_rank_by_group_hostfile_order(self):
        commands = build_ssh_torchrun_commands(self._context("ssh-torchrun"))
        self.assertEqual(commands[0][0], "node01")
        self.assertEqual(commands[1][0], "node02")
        self.assertIn("__hcu_rank=0;", " ".join(commands[0][1]))
        self.assertIn("__hcu_rank=1;", " ".join(commands[1][1]))
        self.assertIn('--node-rank "$HCU_RANK"', commands[1][1][-1])

    def test_torchrun_uses_module_mode_for_builtin_worker(self):
        self.assertEqual(
            resolve_test_command("rccl", "ssh-torchrun", script=None, script_args=()),
            ("-m", "cluster_run.payloads.rccl_worker"),
        )

    def test_torchrun_supports_shell_custom_payload(self):
        self.assertEqual(
            resolve_test_command(
                "custom", "ssh-torchrun", script="check.sh", script_args=()
            ),
            ("--no-python", "bash", "check.sh"),
        )

    def test_container_mpirun_keeps_rank_command_inside_container(self):
        context = LaunchContext(
            test_name="rccl",
            launcher="mpirun-torchrun",
            env_script="/share/hcu/env.sh",
            group_name="group-000",
            group_hostfile=Path("/share/job/group-000/hostfile"),
            nodes=("node01", "node02"),
            test_command=("-m", "cluster_run.payloads.rccl_worker"),
            nproc_per_node=1,
            np=None,
            master_port=29500,
            execution_scope="container",
            container_name="hcu-worker",
            container_workdir="/share/hcu",
        )
        command = build_mpirun_command(context)
        text = json.dumps(command)
        self.assertIn("OMPI_COMM_WORLD_RANK", text)
        self.assertIn("cluster_run.payloads.rccl_worker", text)

    def test_container_mpirun_is_submitted_inside_container_scope(self):
        with tempfile.TemporaryDirectory() as temp:
            executor = ActiveTestExecutor(
                profile="worker",
                scenario="per-node-container",
                test_name="rccl",
                nodes=("node01", "node02"),
                env_script="/share/hcu/env.sh",
                output_dir=Path(temp),
                group_size=2,
                slots=1,
                launcher="mpirun",
                script="/share/tests/test.sh",
                execution_scope="container",
                container_name="hcu-worker",
                container_workdir="/share/hcu",
                dry_run=True,
            )
            report, run_dir = executor.run()
            command = report["groups"][0]["commands"][0]["command"]
            self.assertEqual(command[:4], ["docker", "exec", "-i", "-w"])
            self.assertIn("mpirun", json.dumps(command))
            self.assertTrue((run_dir / "groups" / "group-000" / "launch.json").is_file())

    def test_legacy_rccl_shell_owns_group_mpi_launch(self):
        with tempfile.TemporaryDirectory() as temp:
            executor = ActiveTestExecutor(
                scenario="shared-conda", test_name="rccl", nodes=("node01", "node02"),
                env_script="/share/env.sh", output_dir=Path(temp), group_size=2,
                slots=1, launcher="mpirun-torchrun",
                profile="rccl-tests", dry_run=True,
            )
            report, _ = executor.run()
        group = report["groups"][0]
        self.assertEqual(group["execution_profile"], "rccl-tests")
        self.assertIn("rccl_perf_test.sh", json.dumps(group["commands"]))
        self.assertIn("group-000", json.dumps(group["commands"]))
        self.assertNotIn("torch.distributed.run", json.dumps(group["commands"]))
        self.assertIn("HCU_CLUSTER_ENV_SCRIPT", json.dumps(group["commands"]))
        self.assertIn("--port 22", json.dumps(group["commands"]))

    def test_legacy_gemm_shell_runs_once_on_each_node(self):
        with tempfile.TemporaryDirectory() as temp:
            executor = ActiveTestExecutor(
                scenario="shared-conda", test_name="gemm", nodes=("node01", "node02"),
                env_script="/share/env.sh", output_dir=Path(temp), group_size=2,
                slots=1, launcher="mpirun-torchrun",
                profile="rocblas", dry_run=True,
            )
            report, _ = executor.run()
        group = report["groups"][0]
        self.assertEqual(group["execution_profile"], "rocblas")
        self.assertEqual(len(group["commands"]), 2)
        self.assertNotIn("mpirun", json.dumps(group["commands"]))


class ClusterRunRemoteExecutionTests(unittest.TestCase):
    def test_mpirun_is_submitted_to_group_leader_after_env_bootstrap(self):
        calls = []

        class FakeExecutor:
            def __init__(self, nodes, config):
                self.nodes = list(nodes)
                calls.append(("init", self.nodes))

            def execute(self, name, command, **kwargs):
                calls.append(("execute", self.nodes, list(command)))
                node = self.nodes[0]
                result = SimpleNamespace(
                    returncode=0,
                    success=True,
                    stdout="ok\n",
                    stderr="",
                    result_dir="remote-result",
                )
                return SimpleNamespace(nodes={node: result})

        with tempfile.TemporaryDirectory() as temp:
            with patch("cluster_run.active.BaremetalClusterExecutor", FakeExecutor):
                executor = ActiveTestExecutor(
                    profile="worker",
                    scenario="shared-conda",
                    test_name="rccl",
                    nodes=("node01", "node02"),
                    env_script="/share/hcu/env.sh",
                    output_dir=Path(temp),
                    group_size=2,
                    slots=1,
                    launcher="mpirun",
                    script="/share/tests/test.sh",
                )
                report, _run_dir = executor.run()

        self.assertEqual(report["status"], "PASS")
        execute_calls = [item for item in calls if item[0] == "execute"]
        self.assertEqual(len(execute_calls), 1)
        self.assertEqual(execute_calls[0][1], ["node01"])
        remote_command = execute_calls[0][2]
        self.assertIn("hcu-task-guard", remote_command)  # task guard outside env bootstrap
        self.assertIn("source /share/hcu/env.sh", remote_command[-1])
        self.assertIn("mpirun", remote_command[-1])


class ClusterRunDryRunTests(unittest.TestCase):
    def test_active_dry_run_creates_group_report_without_running_mpirun(self):
        with tempfile.TemporaryDirectory() as temp:
            executor = ActiveTestExecutor(
                scenario="shared-conda",
                test_name="custom",
                nodes=("node01", "node02", "node03"),
                env_script="/share/hcu/env.sh",
                output_dir=Path(temp),
                group_size=2,
                strict_size=False,
                slots=2,
                launcher="mpirun",
                script="/share/tests/test.sh",
                dry_run=True,
            )
            report, run_dir = executor.run()
            self.assertEqual(report["status"], "DRY_RUN")
            self.assertEqual(report["group_count"], 2)
            self.assertTrue((run_dir / "active-result.json").is_file())
            self.assertTrue((run_dir / "groups" / "group-000" / "launch.json").is_file())


if __name__ == "__main__":
    unittest.main()
