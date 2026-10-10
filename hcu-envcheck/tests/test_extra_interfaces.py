# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Entry adapters: real shell/env + fake IB server/client, no device traffic."""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from cluster_run.extra_checks import run_group_ib, run_ib_state, run_nhc
from hcu_envcheck.baremetal import BaremetalExecutionConfig
from hcu_envcheck.cluster_checks import IBWriteBandwidthConfig, NHCCheckConfig


TOKEN = "extra-checks-test-12345678"
IBSTAT = """CA 'mlx5_0'
    Port 1:
        State: Active
        Physical state: LinkUp
        Rate: 400
        Link layer: InfiniBand
"""
BANDWIDTH = """Device : mlx5_0
Transport type : IB
Link type : IB
 #bytes #iterations BW peak[Gb/sec] BW average[Gb/sec] MsgRate[Mpps]
 1048576 1000 190.00 188.50 0.02
"""


def _bash() -> str | None:
    candidates = [shutil.which("bash"), r"C:\Program Files\Git\bin\bash.exe"]
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            try:
                result = subprocess.run([candidate, "-c", "exit 0"], capture_output=True, timeout=5)
                if result.returncode == 0:
                    return candidate
            except (OSError, subprocess.TimeoutExpired):
                pass
    return None


BASH = _bash()


def _target_path(path: Path) -> str:
    value = path.resolve().as_posix()
    if re.match(r"^[A-Za-z]:/", value):
        return "/" + value[0].lower() + value[2:]
    return value


class FakeRemote:
    """Implements the existing SSH sentinel protocol without running SSH."""

    def __init__(self, *, client_rc=0, omit_marker=False, event=None):
        self.calls = []
        self.client_rc = client_rc
        self.omit_marker = omit_marker
        self.event = event
        self.lock = threading.Lock()

    def __call__(self, argv, **kwargs):
        with self.lock:
            self.calls.append(argv)
        marker = re.search(r"(__HCU_ENVCHECK(?:_IB)?_RC_[0-9a-f]+__)", argv[-1]).group(1)
        if "ibstat" in argv[-1]:
            output, code = IBSTAT, 0
        elif "run_nhc" in argv[-1]:
            output, code = "[CHECK RESULT]: PASSED\n", 0
        else:
            output, code = BANDWIDTH, self.client_rc
            if self.event is not None:
                self.event.set()
                code = 130
        suffix = "" if self.omit_marker and "_IB_" in marker else f"\n{marker}={code}\n"
        return subprocess.CompletedProcess(argv, code, output + suffix, "simulated failure" if code else "")


class _ExtraTestFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.execution = BaremetalExecutionConfig(
            output_root=self.root, transport="ssh", concurrency=2, ssh_executable="/usr/bin/ssh")

    def options(self, name):
        return dict(env_script="/shared/env.sh", output_dir=self.root / name,
                    execution_config=self.execution, run_token=TOKEN,
                    which=lambda name: "/usr/bin/ssh" if name == "ssh" else None)


class ExtraInterfaceTests(_ExtraTestFixture):
    def test_nhc_retains_config_selection_removal_and_env(self):
        remote = FakeRemote()
        result = run_nhc(["node1", "node2"], runner=remote,
                         config=NHCCheckConfig(config="/site/nhc.conf", selected="network,gpu",
                                               removed="disk", extra_args=("--verbose",),
                                               environment={"NHC_LEVEL": "site value"}),
                         **self.options("nhc"))
        self.assertEqual((result["status"], result["returncode"]), ("PASS", 0))
        self.assertEqual(len(result["nodes"]), 2)
        command = result["nodes"][0]["command"]["argv"]
        rendered = shlex.join(command)
        for expected in ("--config", "/site/nhc.conf", "--selected", "network,gpu",
                         "--removed", "disk", "--verbose", "NHC_LEVEL=site value", TOKEN):
            self.assertIn(expected, rendered)
        self.assertIn("source /shared/env.sh", rendered)
        self.assertTrue(Path(result["report_path"]).is_file())

    def test_ib_state_only_runs_ibstat(self):
        remote = FakeRemote()
        result = run_ib_state(["node1", "node2"], runner=remote, **self.options("state"))
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(len(remote.calls), 2)
        self.assertTrue(all("ibstat" in call[-1] for call in remote.calls))
        self.assertFalse(any("ib_write_bw" in call[-1] for call in remote.calls))

    def test_group_uses_existing_pair_plan_and_saves_endpoint_evidence(self):
        remote = FakeRemote()
        cfg = IBWriteBandwidthConfig(protocol="roce", device="mlx5_0", ib_port=2,
                                     gid_index=3, control_port=24000, message_bytes=4096,
                                     iterations=12, minimum_average_gbps=100,
                                     startup_grace_seconds=0, concurrency=2)
        result = run_group_ib(["node1", "node2"], config=cfg, runner=remote,
                              scope="container", container="worker", container_workdir="/work",
                              **self.options("pairs"))
        self.assertEqual((result["status"], result["returncode"]), ("PASS", 0))
        self.assertEqual(result["transport"], "ssh")
        self.assertEqual({(p["source"], p["destination"]) for p in result["pairs"]},
                         {("node1", "node2"), ("node2", "node1")})
        self.assertEqual({p["control_port"] for p in result["pairs"]}, {24000, 24001})
        for pair in result["pairs"]:
            self.assertEqual(pair["average_gbps"], 188.5)
            for command in pair["commands"]:
                for expected in ("docker exec --workdir /work worker", "source /shared/env.sh",
                                 "--ib-dev=mlx5_0", "--ib-port=2", "--gid-index=3",
                                 "--size=4096", "--iters=12", TOKEN):
                    self.assertIn(expected, command["argv"][-1])
                self.assertTrue(Path(command["stdout_path"]).is_file())
                self.assertIn("188.50", Path(command["stdout_path"]).read_text())
                self.assertTrue(Path(command["stderr_path"]).is_file())
        self.assertFalse(any("mpirun" in call[-1] or "torchrun" in call[-1] for call in remote.calls))

    def test_ib_nonzero_and_missing_result_never_pass(self):
        for name, remote in (("nonzero", FakeRemote(client_rc=7)), ("missing", FakeRemote(omit_marker=True))):
            with self.subTest(name=name):
                result = run_group_ib(["node1", "node2"], runner=remote,
                                      config=IBWriteBandwidthConfig(startup_grace_seconds=0),
                                      **self.options(name))
                self.assertEqual((result["status"], result["returncode"]), ("FAIL", 1))
                self.assertTrue(all(pair["reason_code"] == "IB_WRITE_BW_FAILED" for pair in result["pairs"]))

    def test_threshold_and_too_small_group(self):
        low = run_group_ib(["node1", "node2"], runner=FakeRemote(),
                           config=IBWriteBandwidthConfig(startup_grace_seconds=0, minimum_average_gbps=200),
                           **self.options("threshold"))
        self.assertEqual(low["status"], "FAIL")
        self.assertEqual(low["pairs"][0]["reason_code"], "IB_BANDWIDTH_BELOW_THRESHOLD")
        small = run_group_ib(["node1"], runner=FakeRemote(), **self.options("one"))
        self.assertEqual((small["status"], small["returncode"]), ("INCOMPLETE", 2))
        self.assertEqual(small["reason_code"], "IB_WRITE_BW_NOT_ENOUGH_NODES")

    def test_pair_plan_limit_checked_before_building_or_traffic(self):
        remote = FakeRemote()
        with patch("hcu_envcheck.cluster_checks.build_ib_test_plan", side_effect=AssertionError("must not allocate plan")):
            result = run_group_ib(["node1", "node2", "node3"], runner=remote,
                                  config=IBWriteBandwidthConfig(max_tests=2),
                                  **self.options("limit"))
        self.assertEqual(result["status"], "INCOMPLETE")
        self.assertEqual(result["reason_code"], "IB_WRITE_BW_TEST_LIMIT_EXCEEDED")
        self.assertTrue(all("ibstat" in call[-1] for call in remote.calls))

    def test_remote_timeout_is_explicit_in_nhc_evidence(self):
        def timeout_runner(argv, **kwargs):
            marker = re.search(r"(__HCU_ENVCHECK_RC_[0-9a-f]+__)", argv[-1]).group(1)
            return subprocess.CompletedProcess(argv, 124, f"{marker}=124\n", "")
        result = run_nhc(["node1"], runner=timeout_runner, **self.options("timeout"))
        self.assertEqual((result["status"], result["returncode"]), ("INCOMPLETE", 2))
        self.assertEqual(result["nodes"][0]["reason_code"], "NHC_CHECK_TIMEOUT")
        self.assertTrue(result["nodes"][0]["command"]["timed_out"])

    def test_cancelled_before_start_does_not_launch(self):
        event = threading.Event()
        event.set()
        remote = FakeRemote()
        result = run_group_ib(["node1", "node2"], cancel_event=event, runner=remote,
                              **self.options("cancel"))
        self.assertEqual((result["status"], result["returncode"]), ("CANCELLED", 130))
        self.assertEqual(remote.calls, [])

    def test_cancelled_pair_does_not_launch_pending_work(self):
        event = threading.Event()
        remote = FakeRemote(event=event)
        result = run_group_ib(["node1", "node2"], cancel_event=event, runner=remote,
                              config=IBWriteBandwidthConfig(startup_grace_seconds=0.1, concurrency=1),
                              **self.options("cancel-pair"))
        self.assertEqual((result["status"], result["returncode"]), ("CANCELLED", 130))
        self.assertEqual(sum("ib_write_bw" in call[-1] for call in remote.calls), 1)
        self.assertTrue(all(pair["status"] == "CANCELLED" for pair in result["pairs"]))

    def test_dry_run_does_not_start_inventory_or_traffic(self):
        remote = FakeRemote()
        result = run_group_ib(["node1", "node2"], runner=remote, dry_run=True,
                              **self.options("dry"))
        self.assertEqual((result["status"], result["returncode"]), ("DRY_RUN", 0))
        self.assertEqual(remote.calls, [])
        self.assertEqual(result["pairs"], [])

    def test_invalid_scope_and_container_rejected(self):
        for kwargs in ({"scope": "pod"}, {"scope": "container"},
                       {"scope": "container", "container": "bad;name"}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                run_nhc(["node1"], **kwargs, **self.options("invalid"))


@unittest.skipUnless(BASH, "Bash required for real environment/server-client wrapper regression")
class ShellExtraInterfaceTests(_ExtraTestFixture):
    def setUp(self):
        super().setUp()
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.work = self.root / "work"
        self.work.mkdir()
        self.events = self.root / "events"
        self.events.mkdir()
        self.env = self.work / "env.sh"
        self.env.write_text(
            f"export HCU_EXPECT_ENV=loaded\nexport PATH={shlex.quote(_target_path(self.bin))}:$PATH\n"
            f"export IB_FAKE_DIR={shlex.quote(_target_path(self.events))}\n", encoding="utf-8")
        self._tool("ibstat", "cat <<'EOF'\n" + IBSTAT + "EOF\n")
        self._tool("run_nhc", 'printf "[CHECK RESULT]: ${FAKE_NHC_STATUS:-PASSED}\\n"\n')
        self._tool("ib_write_bw", '''
port= server=
for arg in "$@"; do
  case "$arg" in --port=*) port=${arg#*=};; node*) server=$arg;; esac
done
[[ -n $port ]] || exit 91
if [[ -z $server ]]; then
  pair="$IB_FAKE_DIR/$FAKE_NODE-$port"
  : > "$pair.ready"
  for ((i=0; i<100; i++)); do [[ -f $pair.done ]] && break; sleep 0.02; done
  [[ -f $pair.done ]] || exit 92
else
  pair="$IB_FAKE_DIR/$server-$port"
  for ((i=0; i<100; i++)); do [[ -f $pair.ready ]] && break; sleep 0.02; done
  [[ -f $pair.ready ]] || exit 93
  : > "$pair.done"
  [[ ${FAKE_CLIENT_RC:-0} -eq 0 ]] || exit "$FAKE_CLIENT_RC"
fi
cat <<'EOF'
''' + BANDWIDTH + "EOF\n")
        docker = self.bin / "docker"
        docker.write_text('''#!/usr/bin/env bash
set -e
[[ -z ${HCU_EXPECT_ENV:-} ]] || exit 94
[[ $1 == exec ]] || exit 95
shift
if [[ $1 == --workdir ]]; then cd "$2"; shift 2; fi
[[ $1 == worker ]] || exit 96
shift
export FAKE_IN_CONTAINER=yes
exec "$@"
''', encoding="utf-8")
        docker.chmod(0o755)
        self.extra_env = {}
        self.calls = []

    def _tool(self, name, body):
        path = self.bin / name
        path.write_text('#!/usr/bin/env bash\nset -e\n[[ $HCU_EXPECT_ENV == loaded ]] || exit 97\n'
                        + 'printf "%s|%s|%s|%s\\n" "$FAKE_NODE" "' + name + '" "${FAKE_IN_CONTAINER:-host}" "$PWD" >> "$IB_FAKE_DIR/calls"\n'
                        + body, encoding="utf-8")
        path.chmod(0o755)

    def shell_runner(self, argv, **kwargs):
        self.calls.append(argv)
        env = dict(os.environ, FAKE_NODE=argv[-2], **self.extra_env)
        env.pop("HCU_EXPECT_ENV", None)
        shell = 'export PATH=' + shlex.quote(_target_path(self.bin)) + ':$PATH\n' + argv[-1]
        return subprocess.run([BASH, "-c", shell], env=env, **kwargs)

    def test_shell_nhc_sources_env_and_failed_source_blocks_command(self):
        opts = self.options("shell-nhc") | {"env_script": _target_path(self.env)}
        # task_guard has its own Linux process-tree tests. Here execute the real
        # timeout/bootstrap/Docker payload to isolate adapter correctness.
        with patch("cluster_run.extra_checks.managed_command", side_effect=lambda cmd, token: list(cmd)):
            result = run_nhc(["node1"], runner=self.shell_runner, **opts)
            self.assertEqual(result["status"], "PASS", result)
            self.env.write_text("false\nexport HCU_EXPECT_ENV=loaded\n", encoding="utf-8")
            failed = run_nhc(["node1"], runner=self.shell_runner,
                             **(opts | {"output_dir": self.root / "failed-env"}))
        self.assertEqual((failed["status"], failed["returncode"]), ("INCOMPLETE", 2))
        self.assertEqual(len((self.events / "calls").read_text(encoding="utf-8").splitlines()), 1)

    def test_shell_paired_server_client_load_env_inside_container_workdir(self):
        with patch("cluster_run.extra_checks.managed_command", side_effect=lambda cmd, token: list(cmd)):
            result = run_group_ib(
                ["node1", "node2"], scope="container", container="worker",
                container_workdir=_target_path(self.work), runner=self.shell_runner,
                config=IBWriteBandwidthConfig(startup_grace_seconds=0, timeout_seconds=8),
                **(self.options("shell-pairs") | {"env_script": "env.sh"}))
        self.assertEqual((result["status"], result["returncode"]), ("PASS", 0), result)
        calls = (self.events / "calls").read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(calls), 6)  # two inventories, two servers, two clients
        self.assertTrue(all("|yes|" in line for line in calls))
        self.assertTrue(all(line.endswith(_target_path(self.work)) for line in calls), calls)
        self.assertEqual(len(list(self.events.glob("*.done"))), 2)

    def test_shell_pair_client_failure_propagates(self):
        self.extra_env["FAKE_CLIENT_RC"] = "7"
        with patch("cluster_run.extra_checks.managed_command", side_effect=lambda cmd, token: list(cmd)):
            result = run_group_ib(
                ["node1", "node2"], runner=self.shell_runner,
                config=IBWriteBandwidthConfig(startup_grace_seconds=0, timeout_seconds=8),
                **(self.options("shell-fail") | {"env_script": _target_path(self.env)}))
        self.assertEqual((result["status"], result["returncode"]), ("FAIL", 1), result)
        self.assertTrue(all(pair["returncode"] == 7 for pair in result["pairs"]))
        self.assertTrue(all(pair["commands"][1]["returncode"] == 7 for pair in result["pairs"]))

    def test_shell_nhc_health_failure_not_tool_failure(self):
        self.extra_env["FAKE_NHC_STATUS"] = "FAILED"
        with patch("cluster_run.extra_checks.managed_command", side_effect=lambda cmd, token: list(cmd)):
            result = run_nhc(["node1"], runner=self.shell_runner,
                             **(self.options("nhc-fail") | {"env_script": _target_path(self.env)}))
        self.assertEqual((result["status"], result["returncode"]), ("FAIL", 1))
        self.assertEqual(result["nodes"][0]["reason_code"], "NHC_CHECK_FAILED")


if __name__ == "__main__":
    unittest.main()
