# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Read-only container active-test preflight contracts."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from cluster_run.active import ActiveTestExecutor
from cluster_run.launchers import LaunchContext, build_mpirun_command
from cluster_run.preflight import ContainerPreflightError, evaluate_container_inventory, fold_node_names, grouped_issue_lines, run_container_preflight


IMAGE = "registry.example/hcu:tag"
IMAGE_ID = "sha256:" + "a" * 64


def remote(*, name: str = "zy-bridge2", image: str = IMAGE, image_id: str = IMAGE_ID,
           running: bool = True, user: str = "", success: bool = True,
           stderr: str = "") -> SimpleNamespace:
    return SimpleNamespace(
        success=success, returncode=0 if success else 1,
        error_kind=None, result_dir="/tmp/evidence",
        stdout=f"/{name}|{image}|{image_id}|{str(running).lower()}|{user}\n" if success else "",
        stderr=stderr,
    )


class ContainerPreflightTests(unittest.TestCase):
    def test_identical_issues_fold_node_names_once(self):
        lines = grouped_issue_lines([
            {"node": f"m09r2n{index:02d}", "code": "CONTAINER_MISSING", "message": "missing"}
            for index in (7, 8, 9, 11)
        ])
        self.assertEqual(lines, ["nodes=m09r2n[07-09,11] code=CONTAINER_MISSING reason=missing"])
        self.assertEqual(fold_node_names(["node2", "node10"]), "node2,node10")

    def test_missing_container_and_root_mpi_are_reported_before_launch(self):
        evaluated = evaluate_container_inventory(
            ["node01", "node02"],
            {"node01": remote(), "node02": remote(success=False, stderr="Error: No such object: zy-bridge2")},
            container_name="zy-bridge2", expected_image=IMAGE,
            launcher="mpirun-torchrun", allow_root_mpi=False,
        )
        codes = {(issue["node"], issue["code"]) for issue in evaluated["issues"]}
        self.assertIn(("node01", "MPI_ROOT_FORBIDDEN"), codes)
        self.assertIn(("node02", "CONTAINER_MISSING"), codes)

    def test_image_id_inconsistency_and_exact_name_check(self):
        evaluated = evaluate_container_inventory(
            ["node01", "node02", "node03"],
            {"node01": remote(user="1000"),
             "node02": remote(image_id="sha256:" + "b" * 64, user="1000"),
             "node03": remote(name="other", user="1000")},
            container_name="zy-bridge2", expected_image=None,
            launcher="ssh-torchrun", allow_root_mpi=False,
        )
        codes = {issue["code"] for issue in evaluated["issues"]}
        self.assertIn("CONTAINER_IMAGE_INCONSISTENT", codes)
        self.assertIn("CONTAINER_NAME_MISMATCH", codes)
        self.assertNotIn("MPI_ROOT_FORBIDDEN", codes)

    def test_root_mpi_requires_explicit_opt_in_for_command(self):
        context = LaunchContext(
            test_name="rccl", launcher="mpirun", env_script="/share/env.sh",
            group_name="group-000", group_hostfile=Path("/share/hostfile"),
            nodes=("node01", "node02"), test_command=("python3", "test.py"),
            nproc_per_node=1, np=None, master_port=29500,
        )
        self.assertNotIn("--allow-run-as-root", build_mpirun_command(context))
        allowed = LaunchContext(**{**context.__dict__, "allow_root_mpi": True})
        self.assertIn("--allow-run-as-root", build_mpirun_command(allowed))

    def test_missing_container_without_image_option_does_not_report_local_image(self):
        with tempfile.TemporaryDirectory() as temp:
            with patch("cluster_run.preflight.BaremetalClusterExecutor") as executor:
                executor.return_value.execute.return_value = SimpleNamespace(nodes={
                    "node01": remote(success=False, stderr="Error: No such object: zy-bridge2"),
                })
                report = run_container_preflight(
                    nodes=["node01"], container_name="zy-bridge2", expected_image=None,
                    launcher="mpirun-torchrun", allow_root_mpi=False,
                    env_script="/share/env.sh", container_shell="bash",
                    transport="ssh", concurrency=1, run_dir=Path(temp),
                )
            self.assertEqual([issue["code"] for issue in report["issues"]], ["CONTAINER_MISSING"])
            self.assertEqual(executor.return_value.execute.call_count, 1)

    def test_resource_failure_is_reported_without_running_active_groups(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with patch("cluster_run.preflight.BaremetalClusterExecutor") as executor:
                def execute(_name, command, **_kwargs):
                    if command[:2] == ["docker", "inspect"]:
                        return SimpleNamespace(nodes={"node01": remote(user="1000")})
                    if command[:3] == ["docker", "image", "inspect"]:
                        return SimpleNamespace(nodes={"node01": SimpleNamespace(success=True, stdout=IMAGE_ID + "\n")})
                    if _name == "active-environment-check":
                        return SimpleNamespace(nodes={"node01": SimpleNamespace(success=True, stderr="")})
                    return SimpleNamespace(nodes={"node01": SimpleNamespace(stderr="cp: cannot stat /missing.py\n")})
                executor.return_value.execute.side_effect = execute
                with patch("cluster_run.preflight.evaluate_node_result", return_value={
                    "status": "BLOCKED", "reachable": True, "device_count": 1,
                    "devices": [{"device_id": 0, "status": "FAIL", "reason_codes": ["VRAM_IN_USE"], "used_mib": 60000}],
                    "metric_summary": {"max_vram_used_percent": 92}, "probe_transport": {"result_dir": "/tmp/probe"},
                }):
                    with patch("cluster_run.preflight.build_remote_probe_command", return_value=["python3", "probe"]):
                        report = run_container_preflight(
                            nodes=["node01"], container_name="zy-bridge2", expected_image=IMAGE,
                            launcher="ssh-torchrun", allow_root_mpi=False,
                            env_script="/share/env.sh", container_shell="bash",
                            transport="ssh", concurrency=1, run_dir=root,
                        )
            self.assertEqual(report["status"], "FAIL")
            self.assertIn("DCU_BUSY", {issue["code"] for issue in report["issues"]})
            self.assertIn("ENV_SCRIPT_ERROR", {issue["code"] for issue in report["issues"]})
            self.assertFalse((root / "preflight.json").exists())
            self.assertNotIn("-J", report["recreate_command_template"])

            active = ActiveTestExecutor(
                scenario="per-node-container", test_name="rccl", nodes=["node01"],
                env_script="/share/env.sh", output_dir=root, group_size=1,
                slots=1, launcher="mpirun-torchrun", execution_scope="container",
                container_name="zy-bridge2",
            )
            with patch("cluster_run.active.run_container_preflight", return_value=report):
                with patch.object(active, "_run_group", side_effect=AssertionError("must not launch")):
                    with self.assertRaises(ContainerPreflightError):
                        active.run()
            self.assertEqual(list(root.glob("active_*/active-result.json")), [])

    def test_skip_idle_check_still_checks_env_script(self):
        with tempfile.TemporaryDirectory() as temp:
            with patch("cluster_run.preflight.run_container_status", return_value={
                "status": "PASS", "ready_nodes": ["node01"],
                "nodes": {"node01": {"configured_user": "1000"}}, "issues": [],
                "image_reference": "image:tag",
            }), patch("cluster_run.preflight.BaremetalClusterExecutor") as executor:
                executor.return_value.execute.return_value = SimpleNamespace(nodes={
                    "node01": SimpleNamespace(success=False, stderr="env.sh: module load failed"),
                })
                report = run_container_preflight(
                    nodes=["node01"], container_name="worker", expected_image=None,
                    launcher="ssh-torchrun", allow_root_mpi=False,
                    env_script="/share/env.sh", container_shell="bash",
                    transport="ssh", concurrency=1, run_dir=Path(temp), check_idle=False,
                )
        self.assertEqual(report["status"], "FAIL")
        self.assertIn("ENV_SCRIPT_ERROR", {item["code"] for item in report["issues"]})
        self.assertIn("set -e", executor.return_value.execute.call_args.args[1][-1])


if __name__ == "__main__":
    unittest.main()
