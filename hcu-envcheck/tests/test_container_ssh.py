# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Creation-time SSH contract: no host mutation, no private-key distribution."""
from __future__ import annotations

import base64
import io
import os
import shlex
import shutil
import subprocess
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from cluster_run import container_ssh as ssh
from cluster_run.cli import build_parser, main
from cluster_run.lifecycle import run_container_lifecycle


def public_key(number):
    blob = b"\x00\x00\x00\x0bssh-ed25519\x00\x00\x00\x20" + bytes([number]) * 32
    return "ssh-ed25519 " + base64.b64encode(blob).decode()


def result(stdout="", success=True, stderr=""):
    return SimpleNamespace(stdout=stdout, success=success, stderr=stderr,
                           error_kind=None, returncode=0 if success else 1)


class ContainerSSHTests(unittest.TestCase):
    def lifecycle(self, **kwargs):
        options = dict(operation="container-create", nodes=["n01", "n02"], container_name="worker",
                       image="image:tag", image_tar=None, volumes=[], docker_args=[],
                       container_command=None, transport="ssh", concurrency=2,
                       confirm=True, dry_run=False, port=26666)
        options.update(kwargs)
        return run_container_lifecycle(**options)

    def executor(self, *, fail_phase=None, duplicate_keys=False):
        def execute(nodes, command, **kwargs):
            phase = kwargs["name"]
            data = {}
            for index, node in enumerate(nodes):
                output = ""
                if phase == "inspect":
                    output = "ABSENT\n"
                elif phase == "verify-image-identity":
                    output = "sha256:abc\n"
                elif phase == "ssh-image-command":
                    output = '[["/entrypoint.sh"],["bash"]]\n'
                elif phase == "ssh-collect-public-keys":
                    number = 1 if duplicate_keys else 1 + 2 * index
                    output = f"HCU_USER_KEY={public_key(number)}\nHCU_HOST_KEY={public_key(number+1)}\n"
                data[node] = result(output, success=phase != fail_phase, stderr="failed " + phase if phase == fail_phase else "")
            return data
        return execute

    def test_port_is_opt_in_and_not_mpi_port_alias(self):
        parser = build_parser()
        args = parser.parse_args(["per-node-container", "container-create", "-f", "nodes", "--port", "26666"])
        self.assertEqual(args.port, 26666)
        self.assertEqual(args.container_ssh_port, 25901)
        self.assertIsNone(parser.parse_args(["per-node-container", "container-create", "-f", "nodes"]).port)

    def test_invalid_ports_do_not_contact_nodes(self):
        for port in (-1, 0, 65536, True):
            with self.subTest(port=port), patch("cluster_run.lifecycle._execute") as execute:
                with self.assertRaisesRegex(ValueError, "--port"):
                    self.lifecycle(port=port, dry_run=True)
                execute.assert_not_called()

    def test_port_delete_and_non_lifecycle_rejected(self):
        with self.assertRaisesRegex(ValueError, "only valid"):
            self.lifecycle(operation="container-delete", image=None, dry_run=True)
        with redirect_stderr(io.StringIO()), redirect_stdout(io.StringIO()):
            rc = main(["per-node-container", "platform", "-f", "not-read", "--port", "26666"])
        self.assertEqual(rc, 3)

    def test_plain_nodes_only(self):
        for node in ("root@n01", "*", "-bad", "n01\nHost *", "2001:db8::1"):
            with self.subTest(node=node), self.assertRaises(ValueError):
                ssh.validate_options(26666, [node], [], [])

    def test_host_network_is_added_or_normalized(self):
        for args in ([], ["--network=host"], ["--net", "host"]):
            self.assertEqual(ssh.validate_options(26666, ["n01"], [], args), ["--network=host"])
        self.assertIn("--device=/dev/kfd", ssh.validate_options(26666, ["n01"], ["/share:/share"], ["--device=/dev/kfd"]))

    def test_conflicting_docker_flags_rejected(self):
        for arg in ("--network=bridge", "--entrypoint=bash", "--user=0", "-uroot", "--pid=host",
                    "--mount=type=bind,src=/root,dst=/root", "--volumes-from=x", "-v/root:/root",
                    "--tmpfs=/run", "--read-only", "--publish=26666:22", "-p26666:22", "--rm", "--name=x"):
            with self.subTest(arg=arg), self.assertRaises(ValueError):
                ssh.validate_options(26666, ["n01"], [], [arg])

    def test_protected_mounts_rejected_including_parents_and_dotdot(self):
        for volume in ("/root:/root", "/x:/", "/x:/etc", "/x:/etc/ssh", "/x:/var/lib",
                       "/x:/var/run", "/x:/data/../root", "/x:/root/.ssh:ro", "/root"):
            with self.subTest(volume=volume), self.assertRaises(ValueError):
                ssh.validate_options(26666, ["n01"], [volume], [])

    def test_dry_run_does_not_pull_or_probe(self):
        with patch("cluster_run.lifecycle._execute") as execute:
            report = self.lifecycle(dry_run=True)
        execute.assert_not_called()
        self.assertEqual(report["status"], "DRY_RUN")
        self.assertEqual(report["ssh_port"], 26666)

    def test_dependency_or_port_failure_precedes_removal(self):
        for phase in ("ssh-port-check", "ssh-image-check", "ssh-image-command"):
            with self.subTest(phase=phase), patch("cluster_run.lifecycle._execute", side_effect=self.executor(fail_phase=phase)) as execute:
                report = self.lifecycle(operation="container-recreate")
            self.assertEqual(report["status"], "FAIL")
            names = [call.kwargs["name"] for call in execute.call_args_list]
            self.assertNotIn("ssh-create-container", names)

    def test_create_preserves_entrypoint_and_configures_then_verifies_ssh(self):
        with patch("cluster_run.lifecycle._execute", side_effect=self.executor()) as execute, \
                patch("cluster_run.preflight.verify_container_mpi_peers", return_value=[]) as verify:
            report = self.lifecycle(container_command="sleep 'infinity'")
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["ssh_verification"], "all-to-all")
        calls = {call.kwargs["name"]: call.args[1] for call in execute.call_args_list}
        self.assertIn("--network=host", calls["ssh-create-container"][-1])
        self.assertIn("/entrypoint.sh sleep infinity", calls["ssh-create-container"][-1])
        self.assertIn("--entrypoint=/bin/bash", calls["ssh-create-container"][-1])
        self.assertIn("ssh-install-trust", calls)
        self.assertEqual(verify.call_args.kwargs["port"], 26666)
        self.assertEqual(verify.call_args.kwargs["groups"], [("n01", "n02"), ("n02", "n01")])

    def test_start_or_install_failure_not_reported_as_pass(self):
        for phase in ("ssh-create-container", "ssh-collect-public-keys", "ssh-public-key-batch-0000", "ssh-install-trust"):
            with self.subTest(phase=phase), patch("cluster_run.lifecycle._execute", side_effect=self.executor(fail_phase=phase)), \
                    patch("cluster_run.preflight.verify_container_mpi_peers") as verify:
                report = self.lifecycle()
            self.assertEqual(report["status"], "FAIL")
            verify.assert_not_called()

    def test_wrong_ssh_namespace_is_failure(self):
        issue = {"node": "n02", "code": "CONTAINER_SSH_IDENTITY_MISMATCH", "message": "wrong container"}
        with patch("cluster_run.lifecycle._execute", side_effect=self.executor()), \
                patch("cluster_run.preflight.verify_container_mpi_peers", return_value=[issue]):
            report = self.lifecycle()
        self.assertEqual(report["status"], "FAIL")
        self.assertIn(issue, report["issues"])

    def test_duplicate_private_key_images_are_not_accepted(self):
        with patch("cluster_run.lifecycle._execute", side_effect=self.executor(duplicate_keys=True)), \
                patch("cluster_run.preflight.verify_container_mpi_peers") as verify:
            report = self.lifecycle()
        self.assertEqual(report["issues"][0]["code"], "CONTAINER_SSH_KEY_REUSED")
        verify.assert_not_called()

    def test_bootstrap_has_only_publickey_auth_and_no_password_reset(self):
        body = ssh.bootstrap(26666)
        self.assertIn("PasswordAuthentication no", body)
        self.assertIn("StrictModes yes", body)
        self.assertIn("PermitRootLogin prohibit-password", body)
        self.assertNotIn("\npasswd root", body)
        self.assertNotIn("service ssh", body)
        self.assertNotIn("/etc/ssh/sshd_config", body)
        self.assertIn('exec "$@"', body)
        self.assertIn('if [[ ! -e', body)
        self.assertNotIn("sshd -f", ssh.bootstrap(26666, check_only=True))
        self.assertIn("image contains managed SSH state", ssh.bootstrap(26666, check_only=True))

    def test_entrypoint_argv_and_override_are_not_shell_evaluated(self):
        self.assertEqual(ssh.image_command('[["entry", "a b"],["cmd"]]', 'test "x y"'), ["entry", "a b", "test", "x y"])
        self.assertEqual(ssh.image_command('[null,null]', None), [])
        for invalid in ('{}', '["oops",[]]', '[[1],[]]', 'bad'):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                ssh.image_command(invalid, None)

    def test_public_evidence_is_validated(self):
        line = f"HCU_USER_KEY={public_key(1)}\nHCU_HOST_KEY={public_key(2)}\n"
        self.assertEqual(ssh.parse_public_keys("banner\n" + line), (public_key(1), public_key(2)))
        for text in ("", line + line, line.replace("ssh-ed25519", "ssh-rsa"), line.replace(public_key(1), "ssh-ed25519 AAAA")):
            with self.subTest(text=text), self.assertRaises(ValueError):
                ssh.parse_public_keys(text)

    def test_trust_pins_host_keys_without_private_material(self):
        files = ssh.trust_files({"n01": (public_key(1), public_key(2))}, 26666)
        self.assertIn("[n01]:26666 " + public_key(2), files["known_hosts"])
        self.assertEqual(files["authorized_keys"], public_key(1) + "\n")
        self.assertIn("StrictHostKeyChecking yes", files["client_config"])
        self.assertIn("Host n01\n", files["client_config"])
        self.assertTrue(files["client_config"].endswith("Host *\n"))
        self.assertEqual(ssh.trust_files({"n01": (public_key(1), public_key(2))}, 22)["known_hosts"], "n01 " + public_key(2) + "\n")

    def test_thousand_node_data_is_chunked_and_verification_is_linear(self):
        nodes = [f"n{index:04}" for index in range(1250)]
        keys = {node: (public_key(1), public_key(2)) for node in nodes}
        commands = list(ssh.upload_commands("worker", ssh.trust_files(keys, 26666)))
        self.assertGreater(len(commands), 3)
        self.assertLess(max(len(shlex.join(command)) for command in commands), 8192)
        groups, topology = ssh.verification_groups(nodes)
        self.assertEqual(topology, "leader-star+ring")
        self.assertLess(sum(len(group) for group in groups), 5 * len(nodes))
        self.assertEqual({group[0] for group in groups}, set(nodes))

    def test_single_node_ssh_self_verification_is_not_skipped(self):
        self.assertEqual(ssh.verification_groups(["n01"]), ([("n01", "n01")], "self"))

    def test_generated_shell_syntax(self):
        bash = "C:/Program Files/Git/bin/bash.exe" if os.name == "nt" else shutil.which("bash")
        if not bash or not Path(bash).exists():
            self.skipTest("Bash not installed")
        scripts = [ssh.bootstrap(26666), ssh.bootstrap(26666, check_only=True),
                   ssh.port_check("worker", 26666)[-1], ssh.install_trust("worker")[-1]]
        scripts += [command[-1] for command in ssh.upload_commands("worker", ssh.trust_files({"n01": (public_key(1), public_key(2))}, 26666))]
        for script in scripts:
            run = subprocess.run([bash, "-n"], input=script, text=True, capture_output=True, timeout=10)
            self.assertEqual(run.returncode, 0, run.stderr)


if __name__ == "__main__":
    unittest.main()
