# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""New-only command grammar, Docker lifecycle safety, and node scripts."""

from __future__ import annotations

import io
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from cluster_run.cli import main
from cluster_run.lifecycle import run_container_lifecycle
from cluster_run.node_script import run_node_script
from cluster_run.active import ActiveTestExecutor


def result(stdout: str = "", *, success: bool = True, stderr: str = ""):
    return SimpleNamespace(stdout=stdout, success=success, stderr=stderr,
                           returncode=0 if success else 1, error_kind=None, result_dir="/tmp/evidence")


class NewInterfaceTests(unittest.TestCase):
    def test_total_interface_script_fails_when_every_entry_call_fails(self):
        script = Path(__file__).resolve().parents[1] / "scripts" / "test.sh"
        shell = "C:/Program Files/Git/bin/bash.exe" if os.name == "nt" else "bash"
        env = dict(os.environ)
        # Exercise the real fixture generator and report validator. A fake
        # Python such as /bin/true can conceal broken bootstrap/validation.
        env["HCU_ACCEPTANCE_NESTED_TEST"] = "1"
        env.update(LC_ALL="C.UTF-8", LANG="C.UTF-8")
        for variable in ("CONTROLLER_ENV_SCRIPT", "RUN_ACTIVE", "RUN_PROFILES", "RUN_NETWORK",
                         "RUN_DIAGNOSTICS", "RUN_SCRIPTS", "RUN_CONTEXT"):
            env.pop(variable, None)
        with tempfile.TemporaryDirectory(dir=script.parent.parent) as temp:
            # Relative ASCII temp name also works under Git Bash when the
            # Windows account name cannot be round-tripped in its environment.
            folder = Path(temp)
            (folder / "hostfile").write_text("n01\nn02\n", encoding="utf-8")
            (folder / "failed-cli.sh").write_text("#!/usr/bin/env bash\nexit 37\n", encoding="utf-8")
            env.update(OUTPUT_DIR=folder.name + "/acceptance", HOSTFILE=folder.name + "/hostfile",
                       HCU_CLUSTER_RUN=folder.name + "/failed-cli.sh",
                       ENV_SCRIPT="//target/env.sh", SHARED_ENV_SCRIPT="//target/shared.sh",
                       NODE_LOCAL_ENV_SCRIPT="//target/local.sh", CONTAINER_ENV_SCRIPT="//target/container.sh")
            python = Path(sys.executable).as_posix()
            target = script.as_posix()
            if os.name == "nt":
                python = "/" + python[0].lower() + python[2:]
                target = "/" + target[0].lower() + target[2:]
            bootstrap = 'export PYTHON_BIN="$1" CONTROLLER_PYTHON="$1"; '
            if os.name == "nt":
                # Git for Windows coreutils may emit the account directory in
                # the system codepage. Keep this fixture's output relative and
                # ASCII while still executing the real Python validator.
                env["ACCEPTANCE_FIXTURE_RUN"] = folder.name + "/acceptance/run_fixture"
                bootstrap += 'mktemp() { mkdir -p "$ACCEPTANCE_FIXTURE_RUN"; printf "%s\\n" "$ACCEPTANCE_FIXTURE_RUN"; }; export -f mktemp; '
            # Pass Unicode paths through argv, not the Windows/MSYS environment.
            run = subprocess.run([shell, "-c", bootstrap + 'exec bash "$2"',
                                  "acceptance-test", python, target], cwd=script.parent.parent,
                                 env=env, encoding="utf-8", errors="replace",
                                 capture_output=True, check=False, timeout=60)
            receipts = list((folder / "acceptance").glob("run_*/calls.tsv"))
            self.assertEqual(len(receipts), 1, run.stdout + run.stderr)
            self.assertGreater(len(receipts[0].read_text(encoding="utf-8").splitlines()), 2, run.stdout + run.stderr)
        self.assertNotEqual(run.returncode, 0)
        self.assertIn("[RESULT]", run.stdout, run.stderr)

    def test_retired_flag_only_and_baremetal_command_are_rejected(self):
        for argv in (["-f", "nodes.txt", "-g", "1", "-s", "check.sh"],
                     ["baremetal-cluster", "--nodes-file", "nodes.txt"]):
            with self.subTest(argv=argv), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
                main(argv)
            self.assertEqual(raised.exception.code, 3)

    def test_script_dry_run_uses_docker_exec_and_no_mpi(self):
        with tempfile.TemporaryDirectory() as temp:
            report, path = run_node_script(
                nodes=["n01", "n02"], scenario="per-node-container", env_script="/shared/env.sh",
                script="/shared/check.sh", script_args=["--quick"], container_name="worker",
                image=None, transport="ssh", concurrency=2, timeout=0,
                output_dir=Path(temp), dry_run=True,
            )
            self.assertEqual(report["status"], "DRY_RUN")
            command = report["nodes"]["n01"]["command"]
            self.assertEqual(command[:3], ["docker", "exec", "worker"])
            self.assertIn("source /shared/env.sh", command[-1])
            self.assertIn("bash /shared/check.sh --quick", command[-1])
            self.assertNotIn("mpirun", command[-1])
            self.assertTrue((path / "script-result.json").exists())

    def test_script_requires_absolute_target_visible_path(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(ValueError, "absolute path"):
                run_node_script(nodes=["n01"], scenario="shared-conda", env_script="/env.sh",
                                script="relative.sh", script_args=[], container_name=None,
                                image=None, transport="ssh", concurrency=1, timeout=0,
                                output_dir=Path(temp), dry_run=True)

    def test_builtin_workers_are_importable_without_explicit_container_workdir(self):
        with tempfile.TemporaryDirectory() as temp:
            executor = ActiveTestExecutor(
                scenario="per-node-container", test_name="gemm", nodes=["n01"],
                env_script="/share/env.sh", output_dir=Path(temp), group_size=1,
                slots=1, launcher="ssh-torchrun", container_name="worker",
                execution_scope="container", dry_run=True,
            )
            report, _ = executor.run()
            command = report["groups"][0]["commands"][0]["command"]
            self.assertNotIn("-w", command)
            self.assertIn("cluster_run.payloads.gemm_worker", command[-1])
            self.assertIn("export PYTHONPATH=", command[-1])
            self.assertLess(command[-1].index("source /share/env.sh"),
                            command[-1].index("export PYTHONPATH="))

    def test_recreate_and_delete_require_explicit_confirmation(self):
        for operation, image in (("container-recreate", "image:tag"), ("container-delete", None)):
            with self.subTest(operation=operation), self.assertRaisesRegex(ValueError, "--yes"):
                run_container_lifecycle(operation=operation, nodes=["n01"], container_name="worker",
                                        image=image, image_tar=None, volumes=[], docker_args=[],
                                        container_command=None, transport="ssh", concurrency=1,
                                        confirm=False, dry_run=False)

    def test_image_acquisition_failure_never_removes_containers(self):
        with patch("cluster_run.lifecycle._execute", side_effect=[
            {"n01": result("PRESENT\n"), "n02": result("PRESENT\n")},
            {"n01": result(), "n02": result(success=False, stderr="pull failed")},
        ]) as execute:
            report = run_container_lifecycle(
                operation="container-recreate", nodes=["n01", "n02"],
                container_name="worker", image="image:tag", image_tar=None,
                volumes=["/share:/share"], docker_args=[], container_command=None,
                transport="ssh", concurrency=2, confirm=True, dry_run=False,
            )
        self.assertEqual(report["status"], "FAIL")
        self.assertEqual(execute.call_count, 2)
        self.assertEqual(report["issues"][0]["code"], "IMAGE_ACQUISITION_FAILED")

    def test_create_refuses_existing_container_without_pull(self):
        with patch("cluster_run.lifecycle._execute", return_value={"n01": result("欢迎使用\nPRESENT\n\n")}) as execute:
            report = run_container_lifecycle(
                operation="container-create", nodes=["n01"], container_name="worker",
                image="image:tag", image_tar=None, volumes=[], docker_args=[],
                container_command=None, transport="ssh", concurrency=1,
                confirm=False, dry_run=False,
            )
        self.assertEqual(report["issues"][0]["code"], "CONTAINER_ALREADY_EXISTS")
        execute.assert_called_once()

    def test_create_acquires_image_before_running_and_verifies_state(self):
        with patch("cluster_run.lifecycle._execute", side_effect=[
            {"n01": result("ABSENT\n")}, {"n01": result()},
            {"n01": result("sha256:abc\n")}, {"n01": result()},
            {"n01": result()},
        ]) as execute:
            report = run_container_lifecycle(
                operation="container-create", nodes=["n01"], container_name="worker",
                image="registry/image:tag", image_tar="/share/image.tar",
                volumes=["/share:/share"], docker_args=["--network=host"],
                container_command="sleep infinity", transport="ssh", concurrency=1,
                confirm=False, dry_run=False,
            )
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(execute.call_count, 5)
        self.assertIn("docker info", execute.call_args_list[0].args[1][-1])
        self.assertIn("docker load", execute.call_args_list[1].args[1][-1])
        self.assertIn("docker run", execute.call_args_list[3].args[1][-1])
        self.assertIn("--network=host", execute.call_args_list[3].args[1][-1])
        self.assertIn(".State.Running", execute.call_args_list[4].args[1][-1])

    def test_lifecycle_dry_run_does_not_contact_nodes(self):
        with patch("cluster_run.lifecycle._execute") as execute:
            report = run_container_lifecycle(
                operation="container-delete", nodes=["n01"], container_name="worker",
                image=None, image_tar=None, volumes=[], docker_args=[],
                container_command=None, transport="ssh", concurrency=1,
                confirm=False, dry_run=True,
            )
        self.assertEqual(report["status"], "DRY_RUN")
        execute.assert_not_called()

    def test_recreate_refuses_same_tag_with_different_image_ids(self):
        with patch("cluster_run.lifecycle._execute", side_effect=[
            {"n01": result("PRESENT\n"), "n02": result("PRESENT\n")},
            {"n01": result(), "n02": result()},
            {"n01": result("sha256:aaa\n"), "n02": result("sha256:bbb\n")},
        ]) as execute:
            report = run_container_lifecycle(
                operation="container-recreate", nodes=["n01", "n02"],
                container_name="worker", image="image:tag", image_tar=None,
                volumes=[], docker_args=[], container_command=None,
                transport="ssh", concurrency=2, confirm=True, dry_run=False,
            )
        self.assertEqual(report["status"], "FAIL")
        self.assertIn("IMAGE_ID_INCONSISTENT", {item["code"] for item in report["issues"]})
        self.assertEqual(execute.call_count, 3)


if __name__ == "__main__":
    unittest.main()
