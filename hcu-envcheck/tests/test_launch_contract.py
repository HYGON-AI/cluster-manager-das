# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Behavioral regressions for launch location/environment, not just option strings."""
from __future__ import annotations
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from cluster_run.active import ActiveTestExecutor
from cluster_run.cli import main
from cluster_run.preflight import verify_container_mpi_peers


ROOT = Path(__file__).resolve().parents[1]


def run_cli(case, args, *, cwd=None):
    temporary = tempfile.TemporaryDirectory()
    case.addCleanup(temporary.cleanup)
    root = Path(temporary.name)
    calls = root / "transport-calls"
    for name in ("ssh", "docker"):
        path = root / name
        path.write_text('#!/bin/sh\nprintf "%s\\n" "$0 $*" >> "$TEST_TRANSPORT_CALLS"\nexit 97\n', encoding="utf-8")
        path.chmod(0o755)
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("HCU_", "RCCL_", "CLUSTER_", "NCCL_", "DEFAULTS_"))}
    env.update(PATH=str(root) + os.pathsep + os.environ["PATH"], PYTHONPATH=str(ROOT),
               PYTHONDONTWRITEBYTECODE="1", TEST_TRANSPORT_CALLS=str(calls))
    command = [sys.executable, "-B", "-c", "import sys; from cluster_run.cli import main; sys.exit(main(sys.argv[1:]))", *args]
    result = subprocess.run(command, cwd=cwd or ROOT, env=env, text=True, capture_output=True, timeout=20)
    return result, calls.read_text().splitlines() if calls.exists() else []


class LaunchContractTests(unittest.TestCase):
    def test_removed_test_modes_are_rejected_before_reading_nodes(self):
        for operation, options in (("gemm", []), ("rccl", ["--profile", "worker"]),
                                   ("rccl", ["--profile", "rocblas"])):
            with self.subTest(operation=operation, options=options):
                result, calls = run_cli(self, ["shared-conda", operation, "-f", "missing-hostfile", *options, "--dry-run"])
                self.assertEqual(result.returncode, 3 if operation == "gemm" else 2)
                self.assertIn("invalid choice" if operation == "gemm" else "unsupported staged argument: --profile", result.stderr)
                self.assertNotIn("readable hostfile required", result.stderr)
                self.assertEqual(calls, [], "invalid modes must fail before transport")

    def test_executor_rejects_removed_builtin_tests(self):
        for name, profile in (("gemm", None), ("gemm", "rocblas"), ("rccl", "worker")):
            with self.subTest(name=name, profile=profile), self.assertRaises(ValueError):
                ActiveTestExecutor(scenario="shared-conda", test_name=name, nodes=["n1"],
                    env_script=None, output_dir=Path("."), group_size=1, slots=1,
                    launcher="mpirun", profile=profile, dry_run=True)

    def test_default_rccl_uses_binary_payload_eight_processes_and_container_root(self):
        with tempfile.TemporaryDirectory() as temp:
            report, root = ActiveTestExecutor(scenario="per-node-container", test_name="rccl", nodes=["n1", "n2"],
                env_script="/share/env.sh", output_dir=Path(temp), group_size=2, slots=1,
                launcher="mpirun", execution_scope="container", container_name="worker", dry_run=True).run()
            self.assertEqual(report["profile"], "rccl-tests")
            self.assertEqual(report["launcher"], "mpirun")
            self.assertEqual(report["nproc_per_node"], 8)
            self.assertTrue(report["allow_root_mpi"])
            self.assertEqual((root / ".state/groups/group-000/hostfile").read_text().splitlines(), ["n1 slots=8", "n2 slots=8"])
            command = report["groups"][0]["commands"][0]["command"]
            self.assertEqual(command[:4], ["docker", "exec", "-i", "worker"])
            self.assertIn("rccl_perf_test.sh", command[-1])
            self.assertIn("HCU_ALLOW_ROOT_MPI=1", command[-1])
            self.assertIn(str(root / ".state/groups/group-000/rccl"), command[-1])
            self.assertNotIn("torch.distributed.run", command[-1])

    def test_binary_preflight_and_actual_root_permission_agree(self):
        from cluster_run.preflight import ContainerPreflightError
        with tempfile.TemporaryDirectory() as temp, patch("cluster_run.active.run_container_preflight") as check:
            check.return_value = {"status": "FAIL", "issues": []}  # stop before any remote launch
            with self.assertRaises(ContainerPreflightError):
                ActiveTestExecutor(scenario="per-node-container", test_name="rccl", nodes=["n1", "n2"],
                    env_script="/env.sh", output_dir=Path(temp), group_size=2, slots=1,
                    launcher="mpirun", execution_scope="container", container_name="worker").run()
            self.assertTrue(check.call_args.kwargs["allow_root_mpi"])
            self.assertEqual(check.call_args.kwargs["launcher"], "mpirun")

    def test_container_mpi_automatically_allows_root_but_host_mpi_does_not(self):
        common = dict(test_name="rccl", nodes=["n1", "n2"], env_script="/env.sh", output_dir=Path("."),
                      group_size=2, slots=1, launcher="mpirun")
        host = ActiveTestExecutor(scenario="shared-conda", **common)
        worker = ActiveTestExecutor(scenario="per-node-container", execution_scope="container", container_name="worker",
                                    **common)
        self.assertFalse(host.allow_root_mpi)
        self.assertTrue(worker.allow_root_mpi)
        self.assertEqual(worker.nproc_per_node, 8)

    def test_removed_allow_root_mpi_is_not_exposed_by_cli(self):
        from cluster_run.cli import build_parser
        parser = build_parser()
        self.assertNotIn("--allow-root-mpi", parser.format_help())
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as stopped:
                parser.parse_args(["per-node-container", "rccl", "-f", "nodes", "--env-script", "/env.sh",
                                   "--container", "worker", "--allow-root-mpi"])
        self.assertEqual(stopped.exception.code, 3)

    def test_cli_rccl_defaults_and_explicit_scale_match_and_legacy_flags_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            nodes = Path(temp) / "nodes"
            nodes.write_text("n1\nn2\n", encoding="utf-8")
            outputs = []
            for index, options in enumerate(([], ["--group-size", "1", "--gpus-per-node", "8"])):
                folder = Path(temp) / str(index)
                result, calls = run_cli(self, ["per-node-container", "rccl", "-f", str(nodes), "--env-script", "/env.sh",
                    "--container", "worker", "-i", "image:tag", "--dry-run", "-o", str(folder), *options])
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn("groups=2 group-size=1 gpus-per-node=8", result.stdout)
                self.assertEqual([line.split()[2] for line in result.stdout.splitlines() if line.startswith("STAGE ")],
                    ["all_reduce", "all_gather", "broadcast", "reduce", "reduce_scatter", "gather", "scatter", "alltoall", "alltoallv", "sendrecv"])
                self.assertEqual(calls, [])
                self.assertFalse(folder.exists(), "dry-run must not invent execution reports")
                outputs.append(result.stdout)
            self.assertEqual(outputs[0], outputs[1])
            for options in (["--profile", "rccl-tests"], ["--launcher", "mpirun"]):
                result, calls = run_cli(self, ["per-node-container", "rccl", "-f", str(nodes), "--dry-run", *options])
                self.assertEqual(result.returncode, 2)
                self.assertIn("unsupported staged argument: " + options[0], result.stderr)
                self.assertEqual(calls, [])


    def test_payload_json_never_masks_execution_failure(self):
        executor = ActiveTestExecutor(scenario="shared-conda", test_name="rccl", nodes=["n1", "n2"],
            env_script="/env.sh", output_dir=Path("."), group_size=2, slots=1, launcher="mpirun")
        evidence = json.dumps({"test": "rccl", "status": "PASS"})
        for code in (1, 2, 124, 125, 127, 137, 255):
            self.assertEqual(executor._result_status(code, evidence), "FAIL")
        self.assertEqual(executor._result_status(130, evidence), "CANCELLED")


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
