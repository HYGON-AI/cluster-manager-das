# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Behavioral regressions for launch location/environment, not just option strings."""
from __future__ import annotations
import io
import json
import os
import subprocess
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from cluster_run.active import ActiveTestExecutor
from cluster_run.cli import main
from cluster_run.launchers import LaunchContext, build_local_command, build_mpirun_command, build_ssh_torchrun_commands
from cluster_run.preflight import verify_container_mpi_peers

ROOT = Path(__file__).resolve().parents[1]
BASH = "C:/Program Files/Git/bin/bash.exe" if os.name == "nt" else "bash"


class LaunchContractTests(unittest.TestCase):
    def test_default_rccl_uses_binary_payload_eight_processes_and_container_root(self):
        with tempfile.TemporaryDirectory() as temp:
            report, root = ActiveTestExecutor(scenario="per-node-container", test_name="rccl", nodes=["n1", "n2"],
                env_script="/share/env.sh", output_dir=Path(temp), group_size=2, slots=1,
                launcher="mpirun-torchrun", execution_scope="container", container_name="worker", dry_run=True).run()
            self.assertEqual(report["profile"], "rccl-tests")
            self.assertEqual(report["launcher"], "mpirun")
            self.assertEqual(report["nproc_per_node"], 8)
            self.assertTrue(report["allow_root_mpi"])
            self.assertEqual((root / "groups/group-000/hostfile").read_text().splitlines(), ["n1 slots=8", "n2 slots=8"])
            command = report["groups"][0]["commands"][0]["command"]
            self.assertEqual(command[:4], ["docker", "exec", "-i", "worker"])
            self.assertIn("rccl_perf_test.sh", command[-1])
            self.assertIn("HCU_ALLOW_ROOT_MPI=1", command[-1])
            self.assertIn(str(root / "groups/group-000/rccl"), command[-1])
            self.assertNotIn("torch.distributed.run", command[-1])

    def test_binary_preflight_and_actual_root_permission_agree(self):
        from cluster_run.preflight import ContainerPreflightError
        with tempfile.TemporaryDirectory() as temp, patch("cluster_run.active.run_container_preflight") as check:
            check.return_value = {"status": "FAIL", "issues": []}  # stop before any remote launch
            with self.assertRaises(ContainerPreflightError):
                ActiveTestExecutor(scenario="per-node-container", test_name="rccl", nodes=["n1", "n2"],
                    env_script="/env.sh", output_dir=Path(temp), group_size=2, slots=1,
                    launcher="mpirun-torchrun", execution_scope="container", container_name="worker").run()
            self.assertTrue(check.call_args.kwargs["allow_root_mpi"])
            self.assertEqual(check.call_args.kwargs["launcher"], "mpirun")

    def test_container_mpi_automatically_allows_root_but_host_mpi_does_not(self):
        common = dict(test_name="rccl", nodes=["n1", "n2"], env_script="/env.sh", output_dir=Path("."),
                      group_size=2, slots=1, launcher="mpirun-torchrun")
        host = ActiveTestExecutor(scenario="shared-conda", **common)
        worker = ActiveTestExecutor(scenario="per-node-container", execution_scope="container", container_name="worker",
                                    profile="worker", **common)
        self.assertFalse(host.allow_root_mpi)
        self.assertTrue(worker.allow_root_mpi)
        self.assertEqual(worker.nproc_per_node, 1)

    def test_removed_allow_root_mpi_is_not_exposed_by_cli(self):
        from cluster_run.cli import build_parser
        parser = build_parser()
        self.assertNotIn("--allow-root-mpi", parser.format_help())
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as stopped:
                parser.parse_args(["per-node-container", "rccl", "-f", "nodes", "--env-script", "/env.sh",
                                   "--container", "worker", "--allow-root-mpi"])
        self.assertEqual(stopped.exception.code, 3)

    def test_cli_rccl_defaults_and_explicit_worker_are_distinct(self):
        with tempfile.TemporaryDirectory() as temp:
            nodes = Path(temp) / "nodes"
            nodes.write_text("n1\nn2\n")
            for options, expected in (([], "rccl-tests"), (["--profile", "worker"], "worker"),
                                      (["--launcher", "mpirun"], "rccl-tests")):
                folder = Path(temp) / (expected + str(len(options)))
                with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                    rc = main(["per-node-container", "rccl", "-f", str(nodes), "--env-script", "/env.sh",
                               "--container", "worker", "-i", "image:tag", "--dry-run", "-o", str(folder), *options])
                self.assertEqual(rc, 0)
                report = json.loads(next(folder.glob("container_rccl_*/active-result.json")).read_text())
                self.assertEqual(report["profile"], expected)
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                rc = main(["per-node-container", "rccl", "-f", str(nodes), "--env-script", "/env.sh",
                           "--container", "worker", "-i", "image:tag",
                           "--launcher", "ssh-torchrun", "--dry-run"])
            self.assertEqual(rc, 3)  # explicit worker required, never silently substitutes Python

    def context(self, **kwargs):
        values = dict(test_name="rccl", launcher="mpirun", env_script="/share/env.sh",
            group_name="group-000", group_hostfile=Path("/share/hostfile"), nodes=("n1", "n2"),
            test_command=("true",), nproc_per_node=8, np=None, master_port=29500)
        values.update(kwargs)
        return LaunchContext(**values)

    def test_mpi_environment_is_snapshotted_then_training_values_are_overridden(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "env.sh"
            path.write_text("export RANK=999 WORLD_SIZE=999 LOCAL_RANK=999 MASTER_ADDR=wrong MASTER_PORT=1\n"
                            "export OMPI_COMM_WORLD_RANK=900 OMPI_COMM_WORLD_SIZE=900\n", encoding="utf-8")
            probe = ("bash", "-c", 'printf "%s|%s|%s|%s|%s|%s|%s" "$RANK" "$WORLD_SIZE" "$LOCAL_RANK" "$MASTER_ADDR" "$MASTER_PORT" "$OMPI_COMM_WORLD_RANK" "$OMPI_COMM_WORLD_SIZE"')
            ctx = self.context(env_script=path.as_posix(), test_command=probe)
            env = dict(os.environ, OMPI_COMM_WORLD_RANK="9", OMPI_COMM_WORLD_SIZE="16", OMPI_COMM_WORLD_LOCAL_RANK="1")
            result = subprocess.run([BASH, "-c", build_mpirun_command(ctx)[-1]], env=env,
                                    capture_output=True, encoding="utf-8", check=False)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, "9|16|1|n1|29500|9|16")

    def test_pythonpath_is_injected_after_env_source_for_all_launchers(self):
        ctx = self.context(launcher="ssh-torchrun", worker_root="/mapped/project", python_executable="/conda/bin/python")
        for _, command in build_ssh_torchrun_commands(ctx):
            body = command[-1]
            self.assertLess(body.index("source /share/env.sh"), body.index("export PYTHONPATH="))
            self.assertIn("/conda/bin/python -m torch.distributed.run", body)
        self.assertLess(build_mpirun_command(replace(ctx, launcher="mpirun"))[-1].index("source "),
                        build_mpirun_command(replace(ctx, launcher="mpirun"))[-1].index("export PYTHONPATH="))

    def test_container_mpi_uses_explicit_ssh_port_and_rank_cwd_before_env(self):
        command = build_mpirun_command(self.context(execution_scope="container", container_name="worker",
                                  container_ssh_port=25222, container_workdir="/work/project"))
        self.assertEqual(command[command.index("--mca") + 1:command.index("--mca") + 3], ["plm_rsh_args", "-p 25222"])
        self.assertLess(command[-1].index("cd /work/project"), command[-1].index("source /share/env.sh"))

    def test_single_node_never_uses_mpi_even_with_direct_launcher_and_eight_processes(self):
        for launcher in ("mpirun", "mpirun-torchrun", "ssh-torchrun"):
            with self.subTest(launcher=launcher), tempfile.TemporaryDirectory() as temp:
                report, _ = ActiveTestExecutor(scenario="shared-conda", test_name="gemm", nodes=["n1"],
                    env_script="/share/env.sh", output_dir=Path(temp), group_size=1, slots=1,
                    launcher=launcher, nproc_per_node=8, dry_run=True).run()
                group = report["groups"][0]
                self.assertEqual(group["launcher"], "local")
                body = group["commands"][0]["command"][-1]
                self.assertNotIn("exec mpirun", body)
                self.assertIn("torch.distributed.run", body)
                self.assertIn("--nproc-per-node 8", body)

    def test_single_node_container_preflight_does_not_request_mpi(self):
        with tempfile.TemporaryDirectory() as temp, patch("cluster_run.active.run_container_preflight") as check:
            check.return_value = {"status": "FAIL", "issues": []}
            from cluster_run.preflight import ContainerPreflightError
            with self.assertRaises(ContainerPreflightError):
                ActiveTestExecutor(scenario="per-node-container", test_name="gemm", nodes=["n1"],
                    env_script="/share/env.sh", output_dir=Path(temp), group_size=1, slots=1,
                    launcher="mpirun-torchrun", execution_scope="container", container_name="worker").run()
            self.assertEqual(check.call_args.kwargs["launcher"], "direct")
            self.assertFalse(check.call_args.kwargs["mpi_groups"])

    def test_invalid_remainder_np_is_rejected_before_remote_precheck(self):
        with tempfile.TemporaryDirectory() as temp, patch("cluster_run.active.run_container_preflight") as check:
            with self.assertRaisesRegex(ValueError, "every group"):
                ActiveTestExecutor(scenario="per-node-container", test_name="rccl", nodes=["n1", "n2", "n3"],
                    profile="worker",
                    env_script="/share/env.sh", output_dir=Path(temp), group_size=2, slots=1,
                    launcher="mpirun-torchrun", np=2, execution_scope="container", container_name="worker").run()
            check.assert_not_called()

    def test_unknown_script_basename_does_not_switch_execution_profile(self):
        with tempfile.TemporaryDirectory() as temp:
            report, _ = ActiveTestExecutor(scenario="shared-conda", test_name="custom", nodes=["n1", "n2"],
                env_script="/share/env.sh", output_dir=Path(temp), group_size=2, slots=1,
                launcher="mpirun-torchrun", script="/share/rccl_perf_test.sh", dry_run=True).run()
            self.assertEqual(report["groups"][0]["launcher"], "node-script")
            self.assertEqual(len(report["groups"][0]["commands"]), 2)

    def test_partial_worker_json_never_masks_timeout_or_transport_failure(self):
        executor = ActiveTestExecutor(scenario="shared-conda", test_name="rccl", nodes=["n1", "n2"],
            profile="worker",
            env_script="/env.sh", output_dir=Path("."), group_size=2, slots=1, launcher="mpirun")
        evidence = json.dumps({"test": "rccl", "status": "INCOMPLETE"})
        self.assertEqual(executor._result_status(2, evidence), "INCOMPLETE")
        for rc in (1, 124, 125, 127, 137, 255):
            self.assertEqual(executor._result_status(rc, evidence), "FAIL")
        self.assertEqual(executor._result_status(130, evidence), "CANCELLED")

    def test_custom_rejects_ignored_mpi_parameters(self):
        with tempfile.TemporaryDirectory() as temp:
            nodes = Path(temp) / "nodes"
            nodes.write_text("n1\n")
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                code = main(["shared-conda", "custom", "-f", str(nodes), "--env-script", "/env.sh",
                             "--script", "/test.sh", "--launcher", "mpirun", "--dry-run"])
            self.assertEqual(code, 3)

    def test_container_ssh_identity_mismatch_is_not_a_valid_peer(self):
        def item(output):
            return SimpleNamespace(stdout=output, stderr="", returncode=0, success=True, error_kind=None)
        class Fake:
            def __init__(self, nodes, config):
                self.nodes = nodes
            def execute(self, name, command):
                if name == "mpi-docker-identity":
                    return SimpleNamespace(nodes={n: item("mnt:[123]\npid:[456]\n0") for n in self.nodes})
                return SimpleNamespace(nodes={self.nodes[0]: item("mnt:[999]\npid:[456]\n0")})
        with tempfile.TemporaryDirectory() as temp, patch("cluster_run.preflight.BaremetalClusterExecutor", Fake):
            issues = verify_container_mpi_peers(nodes=["n1", "n2"], groups=[("n1", "n2")],
                container_name="worker", port=25901, transport="ssh", concurrency=2, run_dir=Path(temp))
        self.assertEqual({v["node"] for v in issues}, {"n1", "n2"})
        self.assertEqual({v["code"] for v in issues}, {"CONTAINER_SSH_IDENTITY_MISMATCH"})

    def test_container_identity_allows_banner_but_requires_same_kernel_and_namespaces(self):
        def identity(boot="kernel-a"):
            return f"__HCU_MPI_BOOT__={boot}\n__HCU_MPI_MNT__=mnt:[12]\n__HCU_MPI_PID__=pid:[34]\n__HCU_MPI_UID__=0\n"
        for different_kernel in (False, True):
            class Fake:
                def __init__(self, nodes, config):
                    self.nodes = nodes
                def execute(self, name, command):
                    text = identity() if name == "mpi-docker-identity" else "Welcome to container\n" + identity("kernel-b" if different_kernel else "kernel-a")
                    return SimpleNamespace(nodes={n: SimpleNamespace(stdout=text, stderr="", returncode=0,
                                                                     success=True, error_kind=None) for n in self.nodes})
            with tempfile.TemporaryDirectory() as temp, patch("cluster_run.preflight.BaremetalClusterExecutor", Fake):
                issues = verify_container_mpi_peers(nodes=["n1", "n2"], groups=[("n1", "n2")],
                    container_name="worker", port=25901, transport="ssh", concurrency=2, run_dir=Path(temp))
            self.assertEqual(bool(issues), different_kernel)

if __name__ == "__main__":
    unittest.main()
