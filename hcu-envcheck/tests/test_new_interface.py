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


    def _shell_image_preflight(self, operation, mode, *, nodes=1, image_tar=False):
        """Run the real Bash entry; a local SSH double prevents real Docker use."""
        root = Path(__file__).resolve().parents[1]
        shell = "C:/Program Files/Git/bin/bash.exe" if os.name == "nt" else "bash"
        with tempfile.TemporaryDirectory() as temp:
            folder = Path(temp)
            tools = folder / "bin"
            tools.mkdir()
            hostfile = folder / "nodes"
            hostfile.write_text("".join(f"n{i:02d}\n" for i in range(1, nodes+1)), encoding="utf-8")
            tool = tools / "ssh"
            tool.write_text(r"""#!/usr/bin/env bash
cmd=${!#}; position=$(($#-1)); node=${!position}; node=${node##*@}
printf '%s|%s\n' "$node" "$cmd" >> "$FIXTURE_CALLS"
case $cmd in
  'docker info '*) exit 0 ;;
  'docker image inspect '*)
    case $FIXTURE_MODE in
      inspect-error) echo 'permission denied while inspecting image' >&2; exit 1 ;;
      transport-error) echo 'ssh: connection closed' >&2; exit 255 ;;
      missing|pull-failure)
        if [[ ! -f $FIXTURE_READY.$node ]]; then
          echo 'Error response from daemon: No such image: fixture/train:local' >&2
          exit 1
        fi ;;
    esac
    digit=a
    [[ $FIXTURE_MODE != mismatch || $node != n02 ]] || digit=b
    printf 'sha256:'; printf '%s' "$digit"{1..64} | tr -d '0-9'; printf '\n'
    ;;
  'docker pull '*)
    [[ $FIXTURE_MODE != pull-failure ]] || { echo 'fixture pull failed' >&2; exit 7; }
    touch "$FIXTURE_READY.$node" ;;
  'docker load -i '*)
    [[ $FIXTURE_MODE != load-failure ]] || { echo 'fixture load failed' >&2; exit 8; } ;;
  'docker inspect --type container '*) exit 1 ;;
  'docker run --rm '*)
    [[ $FIXTURE_MODE == mismatch ]] && exit 0
    echo 'fixture stops at SSH-tool preflight, before container mutation' >&2; exit 42 ;;
  'bash -c '*) exit 0 ;;
  *) echo "unexpected fixture command: $cmd" >&2; exit 99 ;;
esac
""", encoding="utf-8")
            tool.chmod(0o755)
            env = dict(os.environ)
            env["LC_ALL"] = env["LANG"] = "C.UTF-8"
            shell_root = folder.resolve().as_posix()
            if os.name == "nt":
                shell_root = "/" + shell_root[0].lower() + shell_root[2:]
            env.update(FIXTURE_MODE=mode,
                       FIXTURE_CALLS=shell_root+"/calls", FIXTURE_READY=shell_root+"/ready")
            target = (root / "bin/hcu-cluster-run").as_posix()
            if os.name == "nt":
                target = "/" + target[0].lower() + target[2:]
            command = [shell, "-c", 'export PATH="$1:/usr/bin:/bin:$PATH"; shift; exec bash "$@"',
                "image-fixture", shell_root+"/bin", target, "per-node-container", operation, "-f", shell_root+"/nodes",
                "--container", "worker", "-i", "fixture/train:local", "--port", "25901",
                "--log-detail", "every", "-o", shell_root+"/output"]
            if operation == "container-recreate":
                command.append("--yes")
            if image_tar:
                command += ["--image-tar", "/fixture/image.tar"]
            run = subprocess.run(command, cwd=folder, env=env, text=True, encoding="utf-8",
                errors="replace", capture_output=True, timeout=20)
            self.assertTrue((folder/"calls").exists(), run.stdout + run.stderr)
            calls = (folder/"calls").read_text(encoding="utf-8").splitlines()
            return run, [line.split("|", 1)[1] for line in calls]

    def test_shell_create_and_recreate_prepare_images_before_container_changes(self):
        cases = (("local", False, 0, 0, True), ("missing", False, 1, 0, True),
                 ("pull-failure", False, 1, 0, False), ("local", True, 0, 1, True),
                 ("load-failure", True, 0, 1, False), ("inspect-error", False, 0, 0, False),
                 ("transport-error", False, 0, 0, False))
        for operation in ("container-create", "container-recreate"):
            for mode, tar, pulls, loads, reaches_tools in cases:
                with self.subTest(operation=operation, mode=mode, tar=tar):
                    run, calls = self._shell_image_preflight(operation, mode, image_tar=tar)
                    self.assertEqual(run.returncode, 2, run.stdout + run.stderr)
                    self.assertEqual(sum(cmd.startswith("docker pull ") for cmd in calls), pulls)
                    self.assertEqual(sum(cmd.startswith("docker load -i ") for cmd in calls), loads)
                    self.assertEqual(any(cmd.startswith("docker run --rm ") for cmd in calls), reaches_tools)
                    self.assertFalse(any(cmd.startswith(("docker run -dit ", "docker stop ", "docker rm ")) for cmd in calls))
                    if reaches_tools:
                        self.assertIn("image SSH tools missing", run.stdout)
                    if mode == "local" and not tar:
                        self.assertIn("source=local", run.stdout)
                    if mode == "missing":
                        self.assertIn("source=pull", run.stdout)
                    if mode == "inspect-error":
                        self.assertIn("permission denied while inspecting image", run.stdout)
                    if mode == "transport-error":
                        self.assertIn("ssh: connection closed", run.stdout)
                    expected_rc = {"pull-failure": 7, "load-failure": 8,
                                   "inspect-error": 1, "transport-error": 255}.get(mode)
                    if expected_rc is not None:
                        self.assertRegex(run.stdout, rf"stage=preflight status=FAIL rc={expected_rc} ")
                    if mode == "pull-failure":
                        self.assertIn("fixture pull failed", run.stdout)
                    if mode == "load-failure":
                        self.assertIn("fixture load failed", run.stdout)

    def test_shell_recreate_rejects_different_local_image_ids_without_pull_or_removal(self):
        run, calls = self._shell_image_preflight("container-recreate", "mismatch", nodes=2)
        self.assertEqual(run.returncode, 2, run.stdout + run.stderr)
        self.assertIn("image IDs differ between nodes", run.stdout)
        self.assertFalse(any(cmd.startswith(("docker pull ", "docker load ", "docker run -dit ",
                                            "docker stop ", "docker rm ")) for cmd in calls))

if __name__ == "__main__":
    unittest.main()
