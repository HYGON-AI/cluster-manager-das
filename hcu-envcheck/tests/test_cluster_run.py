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
from cluster_run.preflight import ContainerPreflightError
from hcu_envcheck.group_logs import archive_text
from tests.test_launch_contract import run_cli


def controller_failure(case, mode):
    # Reuse the existing transport fixture's real reporter, facts serializer,
    # token guard, cleanup and FIFO handling. Extend only its CPU payload.
    from tests import test_controller_safety as safety
    fixture = safety.ControllerSafetyTests("runTest")
    fixture.setUp()
    case.addCleanup(fixture.doCleanups)
    body = safety.TOOL.replace("phase, stage, token = sys.argv[1:4]",
                               "phase, stage, token = sys.argv[1:4]\n    runtime = pathlib.Path(sys.argv[4])")
    body = body.replace("    print('FIXTURE_RAW_END phase='+phase+' stage='+stage, flush=True)", r'''
    if phase == 'prepare' and mode == 'rccl-launch-failure':
        owner = identity(runtime)
        directory = runtime/'devices'
        directory.mkdir(exist_ok=True)
        (directory/'n01.json').write_text(json.dumps(dict(schema_version=1, node='n01', gpus_per_node=8,
            hip_visible_devices='0,1,2,3,4,5,6,7', pci_devices=[f'0000:{i+3:02x}:00.0' for i in range(8)],
            handoff={key:owner[key] for key in ('group','stage_id','phase','token')})))
        tests = ('all_reduce','all_gather','broadcast','reduce','reduce_scatter','gather','scatter','alltoall','alltoallv','sendrecv')
        (runtime/'rccl-plan.tsv').write_text('test\tnp\tbaseline_busbw_gbps\tmargin_pct\n'+''.join(t+'\t8\t100\t1\n' for t in tests))
    if phase == 'execute' and mode == 'rccl-launch-failure':
        print('mpirun: launcher failed', file=sys.stderr, flush=True)
        raise SystemExit(127)
    print('FIXTURE_RAW_END phase='+phase+' stage='+stage, flush=True)''')
    body = body.replace("    print('true|fixture/image:local|fixture-container-id')", """
    event('container-inspect')
    if mode == 'missing-container':
        print('No such container: fixture-container', file=sys.stderr)
        raise SystemExit(1)
    print('true|fixture/image:local|fixture-container-id')""")
    body = body.replace("str(root/'bin/fixture-payload'), phase, stage, token]",
                        "str(root/'bin/fixture-payload'), phase, stage, token, str(runtime)]")
    for name in ("ssh", "cat", "mv", "timeout", "fixture-payload"):
        (fixture.root / "bin" / name).write_text(body, encoding="utf-8")
    fixture.start(mode, operation="rccl", timeout=10)
    result = fixture.finish(seconds=60)
    return result, fixture.state(), fixture.events()


class ClusterRunEnvironmentTests(unittest.TestCase):
    def test_relative_output_dir_dry_run_preserves_absolute_group_plan_without_artifacts(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "hosts").write_text("node01\nnode02\n", encoding="utf-8")
            result, calls = run_cli(self, ["shared-conda", "rccl", "-f", "hosts", "--env-script", "/share/env.sh",
                                         "--group-size", "2", "--dry-run", "-o", "relative-out"], cwd=root)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("nodes=2 groups=1 group-size=2 gpus-per-node=8", result.stdout)
            self.assertIn("PARTICIPATING node01,node02", result.stdout)
            self.assertEqual(sum(line.startswith("STAGE ") for line in result.stdout.splitlines()), 10)
            self.assertEqual(calls, [])
            self.assertFalse((root / "relative-out").exists())
            # Retained Python adapter still materializes absolute group paths.
            executor = ActiveTestExecutor(scenario="shared-conda", test_name="rccl", nodes=["node01", "node02"],
                env_script="/share/env.sh", output_dir=root / "relative-out", group_size=2, slots=1,
                launcher="mpirun", dry_run=True)
            report, output = executor.run()
            body = report["groups"][0]["commands"][0]["command"][-1]
            argv = shlex.split(body.split("\nexec ", 1)[1])
            hostfile = Path(argv[argv.index("bash") + 2])
            self.assertTrue(hostfile.is_absolute())
            self.assertEqual(hostfile.read_text().splitlines(), ["node01 slots=8", "node02 slots=8"])

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
        result, state, events = controller_failure(self, "rccl-launch-failure")
        self.assertEqual(result[0], 1, result)
        report = json.loads((state / "report.json").read_text())
        self.assertEqual(report["status"], "FAIL")
        facts = json.loads((state / "prepared/group-001/facts.json").read_text())
        table = facts["tables"]["tasks"]
        tasks = [dict(zip(table["fields"], row)) for row in table["rows"]]
        executed = [task for task in tasks if task["phase"] == "execute"]
        self.assertEqual(len(executed), 10)
        self.assertTrue(all(task["execution_exit_code"] == "127" and task["status"] == "FAIL" for task in executed))
        self.assertTrue(all(task["cleanup"] == "CONFIRMED" for task in tasks))
        log = state.parent / "group-001.log"
        self.assertEqual(log.read_text().count("mpirun: launcher failed"), 10)
        self.assertIn("group-001.log", (state.parent / "summary.md").read_text())
        self.assertIn(str(state.parent / "summary.md"), result[1] + result[2])
        self.assertEqual(sum(event["event"] == "payload-start" and event["phase"] == "execute" for event in events), 10)

    def test_container_precheck_failure_returns_reason_without_launch(self):
        result, state, events = controller_failure(self, "missing-container")
        self.assertEqual(result[0], 1, result)
        report = json.loads((state / "report.json").read_text())
        self.assertEqual(report["status"], "FAIL")
        self.assertEqual([(row["status"], row["dispatched"]) for row in report["results"]], [("BLOCKED", False)] * 10)
        self.assertEqual(report["preflight"][0]["status"], "FAIL")
        log = (state.parent / "group-001.log").read_text()
        self.assertIn("container/image mismatch", log)
        self.assertIn("No such container: fixture-container", log)
        self.assertFalse(any(event["event"] == "payload-start" for event in events))
        self.assertFalse(any(event["event"] == "target-command" for event in events))
        self.assertTrue(any(event["event"] == "container-inspect" for event in events))
        self.assertIn("RESULT        FAIL", result[1])

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


    def test_legacy_rccl_shell_owns_group_mpi_launch(self):
        with tempfile.TemporaryDirectory() as temp:
            executor = ActiveTestExecutor(
                scenario="shared-conda", test_name="rccl", nodes=("node01", "node02"),
                env_script="/share/env.sh", output_dir=Path(temp), group_size=2,
                slots=1, launcher="mpirun",
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
                    scenario="shared-conda",
                    test_name="rccl",
                    nodes=("node01", "node02"),
                    env_script="/share/hcu/env.sh",
                    output_dir=Path(temp),
                    group_size=2,
                    slots=1,
                    launcher="mpirun",
                )
                report, _run_dir = executor.run()

        self.assertEqual(report["status"], "PASS")
        execute_calls = [item for item in calls if item[0] == "execute"]
        self.assertEqual(len(execute_calls), 1)
        self.assertEqual(execute_calls[0][1], ["node01"])
        remote_command = execute_calls[0][2]
        self.assertIn("hcu-task-guard", remote_command)  # task guard outside env bootstrap
        self.assertIn("source /share/hcu/env.sh", remote_command[-1])
        self.assertIn("rccl_perf_test.sh", remote_command[-1])


if __name__ == "__main__":
    unittest.main()
