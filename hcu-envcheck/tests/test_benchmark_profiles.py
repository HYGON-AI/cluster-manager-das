# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Execute full shell profiles against fake tools; never touch GPUs or MPI."""

from __future__ import annotations

import os
import csv
import shlex
import shutil
import subprocess
import tempfile
import unittest
import uuid
from pathlib import Path

from tests.test_extra_interfaces import BASH, _target_path


PAYLOADS = Path(__file__).resolve().parents[1] / "cluster_run" / "payloads"
RCCL_TESTS = ("all_reduce", "all_gather", "broadcast", "reduce", "reduce_scatter",
              "gather", "scatter", "alltoall", "alltoallv", "sendrecv")


@unittest.skipUnless(BASH, "Bash is required for benchmark payload tests")
class BenchmarkProfileTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.calls = self.root / "tool-calls"
        self.mpi_calls = self.root / "mpi-args"
        self.env_script = self.root / "env.sh"
        self.env_script.write_text(
            'export BENCH_ENV_READY="${FAKE_RANK_NODE:-leader}"\n'
            # On Windows prefer MSYS tools over unrelated native Windows mkdir
            # found on the inherited PATH; /c/... must retain POSIX semantics.
            f'export PATH={shlex.quote(_target_path(self.bin))}:/usr/bin:/bin:$PATH\n', encoding="utf-8")
        self.env = {name: value for name, value in os.environ.items()
                    if not name.startswith(("RCCL_", "GEMM_", "HCU_", "CLUSTER_", "NCCL_", "OMPI_", "PRTE_", "DEFAULTS_"))
                    and name not in {"MPIRUN_BIN", "MPI_BIN", "SSH_PORT", "UCX_NET_DEVICES"}}
        self.env.update(TOOL_CALLS=_target_path(self.calls), MPI_ARGS=_target_path(self.mpi_calls),
                        # A real cluster_env.conf next to the payloads (site port,
                        # plugin paths) must never leak into unit-test defaults.
                        CLUSTER_ENV_FILE="/nonexistent-hcu-site-conf",
                        LC_ALL="C.UTF-8", LANG="C.UTF-8",
                        HCU_CLUSTER_ENV_SCRIPT=_target_path(self.env_script),
                        HCU_CLUSTER_TIMEOUT_SECONDS="5", CLUSTER_IDLE_CHECKED="1",
                        HCU_ALLOW_ROOT_MPI="1")
        # Deterministic synthetic baselines; never read site data from the repo.
        fixture = self.root / "fixture-baselines.conf"
        fixture.write_text("".join(f"{np} {test} 100\n" for np in (1, 2, 4, 8, 16) for test in RCCL_TESTS), encoding="utf-8")
        self.env["RCCL_BASELINE_FILE"] = _target_path(fixture)
        # Ask the tested shell for its physical path. MSYS mounts Windows TEMP
        # at /tmp; reconstructing /c/Users/... makes mkdir -p recheck protected
        # ancestors under the Windows sandbox. Linux retains its absolute path.
        self.shell_root = subprocess.run(
            [BASH, "-c", "pwd -P"], cwd=self.root, env=self.env,
            capture_output=True, text=True, encoding="utf-8", check=True,
        ).stdout.strip()
        self.rccl_body = '''
[[ ${BENCH_ENV_READY:-unset} == ${FAKE_RANK_NODE:-leader} ]] || exit 91
cards=
for ((i=1; i<=$#; i++)); do if [[ ${!i} == -g ]]; then j=$((i+1)); cards=${!j}; fi; done
printf '%s|%s|g=%s|iface=%s\n' "$(basename "$0")" "${FAKE_RANK_NODE:-leader}" "$cards" "${NCCL_SOCKET_IFNAME:-}" >> "$TOOL_CALLS"
[[ ${FAKE_BENCH_RC:-0} == 0 ]] || exit "$FAKE_BENCH_RC"
echo '8 1 float sum -1 1 200 200 0 1 200 200 0'
'''
        for name in RCCL_TESTS:
            self.tool(name + "_perf", self.rccl_body)
        self.tool("rocblas-bench", '''
[[ ${BENCH_ENV_READY:-unset} == leader ]] || exit 91
printf 'rocblas|card=%s|%s\n' "$HIP_VISIBLE_DEVICES" "$*" >> "$TOOL_CALLS"
[[ ${FAKE_BENCH_RC:-0} == 0 ]] || exit "$FAKE_BENCH_RC"
echo 'transA, transB, M, N, K, rocblas-Gflops, us'
echo 'N, N, 4096, 4096, 4096, 50000.0, 10'
''')
        self.tool("hy-smi", 'for ((i=0; i<8; i++)); do echo "$i 40 0 perf clk 0% 0%"; done\n')
        self.tool("mpirun", '''
printf '%s\n' "$@" > "$MPI_ARGS"
hosts= np=
while (($#)); do
  case "$1" in
    --allow-run-as-root) shift;;
    --mca) shift 3;;
    --host) hosts=$2; shift 2;;
    -np) np=$2; shift 2;;
    -x|--map-by|--bind-to|--wdir) shift 2;;
    *) break;;
  esac
done
[[ -n $hosts && -n $np ]] || exit 92
rank=0
IFS=',' read -r -a items <<< "$hosts"
for item in "${items[@]}"; do
  node=${item%:*}; count=${item##*:}
  for ((local_rank=0; local_rank<count; local_rank++)); do
    env -u BENCH_ENV_READY FAKE_RANK_NODE="$node" OMPI_COMM_WORLD_RANK="$rank" OMPI_COMM_WORLD_LOCAL_RANK="$local_rank" "$@" || exit $?
    rank=$((rank+1))
  done
done
[[ $rank == "$np" ]]
''')

    def tool(self, name, body, directory=None):
        path = (directory or self.bin) / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("#!/usr/bin/env bash\nset -e\n" + body, encoding="utf-8")
        path.chmod(0o755)
        return path

    def hostfile(self, nodes=1, slots=8):
        path = self.root / "hostfile"
        path.write_text("\n".join(f"node{i} slots={slots}" for i in range(nodes)), encoding="utf-8")
        return _target_path(path)  # deliberately no trailing newline

    def baseline(self, np=8):
        path = self.root / "rccl.conf"
        path.write_text("\n".join(f"{np} {name} 100 1" for name in RCCL_TESTS) + "\n", encoding="utf-8")
        return _target_path(path)

    def run_payload(self, script, args, *, env=None):
        command = ["bash", _target_path(PAYLOADS / script) if isinstance(script, str) else _target_path(script), *args]
        shell = f"set -e; source {shlex.quote(_target_path(self.env_script))}; exec {shlex.join(command)}"
        return subprocess.run([BASH, "-c", shell], cwd=self.root, env=self.env | (env or {}),
                              capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=30)

    def rccl_args(self, nodes=1, slots=8):
        return [self.hostfile(nodes, slots), "--baseline-file", self.baseline(nodes * slots),
                "--log-dir", self.shell_root + "/rccl-logs", "--skip-idle-check"]

    def tool_lines(self):
        return self.calls.read_text(encoding="utf-8").splitlines() if self.calls.exists() else []

    def test_rccl_single_node_runs_all_ten_without_mpi(self):
        result = self.run_payload("rccl_perf_test.sh", self.rccl_args(),
                                  env={"MPIRUN_BIN": "/missing/mpi", "HCU_ALLOW_ROOT_MPI": "0"})
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(self.mpi_calls.exists())
        self.assertEqual(len(self.tool_lines()), 10)
        self.assertEqual({line.split("|")[0] for line in self.tool_lines()}, {name + "_perf" for name in RCCL_TESTS})
        self.assertTrue(all("|g=8|" in line for line in self.tool_lines()))

    def test_rccl_multinode_sources_every_rank_and_uses_container_ssh_port(self):
        # node-local env selection must resolve the binary AFTER rank bootstrap.
        for node in ("leader", "node0", "node1"):
            self.tool("all_reduce_perf", self.rccl_body, self.root / node)
        with self.env_script.open("a", encoding="utf-8") as stream:
            stream.write(f'export RCCL_BIN_DIR={shlex.quote(_target_path(self.root))}/"${{FAKE_RANK_NODE:-leader}}"\n')
        result = self.run_payload("rccl_perf_test.sh", self.rccl_args(2, 2) +
                                  ["--tests", "all_reduce", "--port", "25901", "--iface", "test0"])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        mpi = self.mpi_calls.read_text(encoding="utf-8").splitlines()
        self.assertIn("-p 25901", mpi)
        self.assertIn("--allow-run-as-root", mpi)
        self.assertEqual(len(self.tool_lines()), 4)
        self.assertEqual({line.split("|")[1] for line in self.tool_lines()}, {"node0", "node1"})
        self.assertTrue(all("|g=1|iface=test0" in line for line in self.tool_lines()))

    def test_original_broadcast_command_semantics_two_nodes_eight_ranks(self):
        """Execute 16 fake ranks and assert argv/env, not just a dry-run string."""
        self.env.pop("HCU_ALLOW_ROOT_MPI", None)
        with self.env_script.open("a", encoding="utf-8") as stream:
            stream.write('export LD_LIBRARY_PATH=/chosen/runtime/lib ROCM_PATH=/chosen/dtk RCCL_NET_PLANE=plane-test\n')
        evidence = self.root / "broadcast-evidence"
        body = r'''
for name in NCCL_SOCKET_IFNAME NCCL_PXN_DISABLE RCCL_PXN_GPU_BALANCE RCCL_NET_PLANE NCCL_NET_PLUGIN NCCL_PLUGIN_P2P NCCL_NET_GDR_LEVEL NCCL_NET_GDR_READ NCCL_TOPO_FILE UCX_NET_DEVICES LD_LIBRARY_PATH ROCM_PATH; do
  printf '%s=%s\n' "$name" "${!name:-}" >> "$CORE_EVIDENCE"
done
printf 'ARG=%s\n' "$@" >> "$CORE_EVIDENCE"
''' + self.rccl_body
        self.tool("broadcast_perf", body)
        result = self.run_payload("rccl_perf_test.sh", [self.hostfile(2, 8), "--tests", "broadcast",
                                  "--log-dir", self.shell_root + "/core", "--skip-idle-check"],
                                  env={"CORE_EVIDENCE": _target_path(evidence)})
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        mpi = self.mpi_calls.read_text(encoding="utf-8").splitlines()
        self.assertIn("--allow-run-as-root", mpi)
        self.assertEqual(mpi[mpi.index("--mca") + 1:mpi.index("--mca") + 3], ["plm_rsh_args", "-p 25901"])
        self.assertEqual(mpi[mpi.index("--host") + 1], "node0:8,node1:8")
        self.assertEqual(mpi[mpi.index("-np") + 1], "16")
        self.assertEqual(mpi[mpi.index("--bind-to") + 1], "none")
        exports = [mpi[index+1] for index, token in enumerate(mpi[:-1]) if token == "-x"]
        self.assertIn("LD_LIBRARY_PATH", exports)
        self.assertIn("ROCM_PATH", exports)
        expected = dict(NCCL_SOCKET_IFNAME="eth0", NCCL_PXN_DISABLE="0", RCCL_PXN_GPU_BALANCE="1",
                        RCCL_NET_PLANE="plane-test", NCCL_NET_PLUGIN="shca", NCCL_PLUGIN_P2P="ib",
                        NCCL_NET_GDR_LEVEL="4", NCCL_NET_GDR_READ="1",
                        NCCL_TOPO_FILE="/usr/local/built-in-508-topo-input-tj-default.xml", UCX_NET_DEVICES="ib0")
        lines = evidence.read_text(encoding="utf-8").splitlines()
        for name, value in expected.items():
            self.assertIn(f"{name}={value}", exports)
            self.assertEqual(lines.count(f"{name}={value}"), 16)
        self.assertEqual(lines.count("LD_LIBRARY_PATH=/chosen/runtime/lib"), 16)
        flags = [line[4:] for line in lines if line.startswith("ARG=")]
        self.assertEqual(flags, ["-g", "1", "-b", "4", "-e", "1G", "-f", "2", "-n", "20", "-w", "5"] * 16)
        self.assertEqual(len(self.tool_lines()), 16)
        self.assertIn("MEETS_BASELINE", result.stdout)
        self.assertNotIn("torchrun", "\n".join(mpi))
        self.assertTrue((self.root / "core" / "broadcast.command.sh").is_file())

    def test_rccl_without_baseline_fails_before_launch(self):
        standalone = self.root / "standalone" / "rccl_perf_test.sh"
        standalone.parent.mkdir()
        shutil.copyfile(PAYLOADS / "rccl_perf_test.sh", standalone)
        result = self.run_payload(standalone, [self.hostfile(1), "--skip-idle-check"], env={"RCCL_BASELINE_FILE": ""})
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertFalse(self.tool_lines())
        self.assertIn("np=8", result.stderr)
        self.assertNotIn("[PASS]", result.stdout)

    def test_partial_baseline_is_configuration_error_not_silent_subset(self):
        path = self.root / "partial.conf"
        path.write_text("8 broadcast 100\n", encoding="utf-8")
        result = self.run_payload("rccl_perf_test.sh", [self.hostfile(1), "--baseline-file", _target_path(path), "--skip-idle-check"])
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertFalse(self.tool_lines())
        self.assertIn("sendrecv", result.stderr)
        self.assertIn("all_reduce", result.stderr)

    def test_other_scale_baseline_is_not_borrowed_and_explicit_missing_file_fails(self):
        result = self.run_payload("rccl_perf_test.sh", [self.hostfile(1), "--tests", "broadcast",
                                  "--baseline-file", self.baseline(16), "--skip-idle-check"])
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("np=8", result.stderr)
        self.assertFalse(self.tool_lines())
        bad = self.run_payload("rccl_perf_test.sh", [self.hostfile(1), "--tests", "broadcast",
                               "--baseline-file", self.shell_root + "/absent.conf", "--skip-idle-check"])
        self.assertEqual(bad.returncode, 1, bad.stdout + bad.stderr)
        self.assertFalse(self.tool_lines())

    def test_baseline_does_not_hide_execution_or_parse_failure(self):
        args = [self.hostfile(1), "--tests", "broadcast", "--skip-idle-check"]
        failed = self.run_payload("rccl_perf_test.sh", args, env={"FAKE_BENCH_RC": "7"})
        self.assertEqual(failed.returncode, 2, failed.stdout + failed.stderr)
        self.assertNotIn("[PASS]", failed.stdout)
        self.tool("broadcast_perf", "echo no-bandwidth-output\n")
        bad_output = self.run_payload("rccl_perf_test.sh", args)
        self.assertEqual(bad_output.returncode, 2, bad_output.stdout + bad_output.stderr)
        self.assertNotIn("[PASS]", bad_output.stdout)

    def test_complete_suite_reports_four_metrics_and_ten_comparisons(self):
        result = self.run_payload("rccl_perf_test.sh", self.rccl_args(2, 1))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        with (self.root / "rccl-logs/rccl-summary.tsv").open(encoding="utf-8") as stream:
            rows = list(csv.DictReader(stream, delimiter="\t"))
        self.assertEqual([row["test"] for row in rows], list(RCCL_TESTS))
        self.assertEqual(len(self.tool_lines()), 20)
        for row in rows:
            self.assertEqual(row["status"], "PASS")
            self.assertEqual(row["reason"], "MEETS_BASELINE")
            for field in ("out_of_place_algbw_gbps", "out_of_place_busbw_gbps", "in_place_algbw_gbps", "in_place_busbw_gbps"):
                self.assertEqual(float(row[field]), 200.0)
            self.assertEqual(float(row["baseline_busbw_gbps"]), 100.0)
            self.assertEqual(float(row["threshold_busbw_gbps"]), 99.0)
        markdown = (self.root / "rccl-logs/rccl-summary.md").read_text(encoding="utf-8")
        self.assertIn("out-of-place algbw", markdown)
        self.assertIn("sendrecv_perf", markdown)
        self.assertIn("全部 10 项性能达标", result.stdout)

    def test_one_failure_does_not_skip_remaining_collectives(self):
        self.tool("all_reduce_perf", self.rccl_body.replace('echo \'8 1', 'exit 7\necho \'8 1'))
        result = self.run_payload("rccl_perf_test.sh", self.rccl_args())
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertEqual(len(self.tool_lines()), 10)
        with (self.root / "rccl-logs/rccl-summary.tsv").open(encoding="utf-8") as stream:
            rows = list(csv.DictReader(stream, delimiter="\t"))
        self.assertEqual(rows[0]["status"], "FAIL")
        self.assertEqual(rows[0]["returncode"], "7")
        self.assertTrue(all(row["status"] == "PASS" for row in rows[1:]))

    def test_item_margin_and_real_bandwidth_columns_control_comparison(self):
        baseline = self.root / "threshold.conf"
        baseline.write_text("8 broadcast 125 5\n", encoding="utf-8")
        self.tool("broadcast_perf", "echo '8 1 float sum -1 1 130.96 122.78 0 1 131.15 122.95 0'\n")
        args = [self.hostfile(1), "--tests", "broadcast", "--baseline-file", _target_path(baseline),
                "--log-dir", self.shell_root + "/rccl-logs", "--skip-idle-check"]
        result = self.run_payload("rccl_perf_test.sh", args)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        with (self.root / "rccl-logs/rccl-summary.tsv").open(encoding="utf-8") as stream:
            row = next(csv.DictReader(stream, delimiter="\t"))
        self.assertEqual(row["out_of_place_algbw_gbps"], "130.96")
        self.assertEqual(row["out_of_place_busbw_gbps"], "122.78")
        self.assertEqual(row["in_place_algbw_gbps"], "131.15")
        self.assertEqual(row["in_place_busbw_gbps"], "122.95")
        self.assertEqual(float(row["peak_busbw_gbps"]), 122.95)
        self.assertEqual(float(row["threshold_busbw_gbps"]), 118.75)
        baseline.write_text("8 broadcast 125 1\n", encoding="utf-8")
        result = self.run_payload("rccl_perf_test.sh", args)
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)

    def test_invalid_baseline_cannot_turn_missing_performance_into_pass(self):
        for content in ("8 broadcast 0\n", "8 broadcast 100 100\n", "8 broadcast 100\n8 broadcast 200\n"):
            with self.subTest(content=content):
                path = self.root / "invalid.conf"
                path.write_text(content, encoding="utf-8")
                result = self.run_payload("rccl_perf_test.sh", [self.hostfile(1), "--tests", "broadcast", "--baseline-file", _target_path(path)])
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertFalse(self.tool_lines())

    def test_standalone_idle_check_still_checks_kfd_processes(self):
        self.tool("hy-smi", 'if [[ ${1:-} == --showpids ]]; then echo "PID 123 active"; else echo "0 40 0 perf clk 0% 0%"; fi\n')
        result = self.run_payload("rccl_perf_test.sh", [self.hostfile(1)], env={"CLUSTER_IDLE_CHECKED": "0"})
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertFalse(self.tool_lines())
        self.assertIn("KFD", result.stderr)

    def test_missing_binary_is_a_failed_item_not_a_smaller_suite(self):
        (self.bin / "all_reduce_perf").unlink()
        result = self.run_payload("rccl_perf_test.sh", self.rccl_args() + ["--bin-dir", _target_path(self.bin)])
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertEqual(len(self.tool_lines()), 9)
        with (self.root / "rccl-logs/rccl-summary.tsv").open(encoding="utf-8") as stream:
            rows = list(csv.DictReader(stream, delimiter="\t"))
        self.assertEqual(len(rows), 10)
        self.assertEqual(rows[0]["returncode"], "127")
        self.assertEqual(rows[-1]["test"], "sendrecv")

    def test_average_bandwidth_fallback_never_invents_four_metrics(self):
        self.tool("broadcast_perf", "echo '# Avg bus bandwidth : 200'\n")
        result = self.run_payload("rccl_perf_test.sh", self.rccl_args() + ["--tests", "broadcast"])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        with (self.root / "rccl-logs/rccl-summary.tsv").open(encoding="utf-8") as stream:
            row = next(csv.DictReader(stream, delimiter="\t"))
        self.assertEqual(row["peak_busbw_gbps"], "200.00")
        self.assertEqual(row["out_of_place_algbw_gbps"], "-")
        self.assertEqual(row["in_place_busbw_gbps"], "-")

    def test_network_environment_overrides_defaults_and_flags_win_after_rank_source(self):
        with self.env_script.open("a", encoding="utf-8") as stream:
            stream.write('export NCCL_NET_PLUGIN=site-plugin NCCL_PXN_DISABLE=1 RCCL_PXN_GPU_BALANCE=0\n'
                         'export NCCL_NET_GDR_LEVEL=2 NCCL_NET_GDR_READ=0 NCCL_TOPO_FILE=/site/topo.xml\n'
                         'export NCCL_SOCKET_IFNAME=env0 UCX_NET_DEVICES=env-ib\n')
        result = self.run_payload("rccl_perf_test.sh", [self.hostfile(2, 1), "--tests", "broadcast",
                                  "--iface", "flag0", "--ucx", "flag-ib", "--topo", "/flag/topo.xml",
                                  "--port", "26930", "--skip-idle-check"])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        mpi = self.mpi_calls.read_text(encoding="utf-8").splitlines()
        for setting in ("NCCL_NET_PLUGIN=site-plugin", "NCCL_PXN_DISABLE=1", "RCCL_PXN_GPU_BALANCE=0",
                        "NCCL_NET_GDR_LEVEL=2", "NCCL_NET_GDR_READ=0", "NCCL_TOPO_FILE=/flag/topo.xml",
                        "NCCL_SOCKET_IFNAME=flag0", "UCX_NET_DEVICES=flag-ib"):
            self.assertIn(setting, mpi)
        self.assertIn("-p 26930", mpi)
        self.assertTrue(all("iface=flag0" in line for line in self.tool_lines()))

    def test_unset_plane_is_not_invented_and_unknown_collective_is_rejected(self):
        result = self.run_payload("rccl_perf_test.sh", [self.hostfile(2, 1), "--tests", "broadcast", "--skip-idle-check"])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        mpi = self.mpi_calls.read_text(encoding="utf-8").splitlines()
        self.assertFalse(any(item.startswith("RCCL_NET_PLANE=") for item in mpi))
        self.assertIn("RCCL_NET_PLANE=<", result.stdout)
        before = len(self.tool_lines())
        bad = self.run_payload("rccl_perf_test.sh", [self.hostfile(2, 1), "--tests", "bad_collective", "--skip-idle-check"])
        self.assertNotEqual(bad.returncode, 0)
        self.assertEqual(len(self.tool_lines()), before)

    def test_network_values_are_quoted_not_evaluated(self):
        setting = "iface name; touch injected-command"
        result = self.run_payload("rccl_perf_test.sh", [self.hostfile(2, 1), "--tests", "broadcast",
                                  "--iface", setting, "--skip-idle-check"])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse((self.root / "injected-command").exists())
        self.assertTrue(all("iface=" + setting in line for line in self.tool_lines()))

    def test_rccl_explicit_binary_directory_and_low_baseline_result(self):
        other = self.root / "explicit binaries"
        self.tool("all_reduce_perf", self.rccl_body.replace("200 200", "20 20"), other)
        result = self.run_payload("rccl_perf_test.sh", self.rccl_args() +
                                  ["--tests", "all_reduce", "--bin-dir", _target_path(other)])
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("[FAIL]", result.stdout)

    def _site_conf(self, **values):
        path = self.root / "site.conf"
        path.write_text("".join(f': "${{{key}:={value}}}"\n' for key, value in values.items()),
                        encoding="utf-8")
        return _target_path(path)

    def _mpi_exports(self):
        mpi = self.mpi_calls.read_text(encoding="utf-8").splitlines()
        return [mpi[index + 1] for index, token in enumerate(mpi[:-1]) if token == "-x"]

    def test_site_conf_provides_defaults_when_environment_has_no_value(self):
        conf = self._site_conf(RCCL_IFACE="site-iface0", NCCL_NET_PLUGIN="siteplug",
                               DEFAULTS_CONTAINER_SSH_PORT="26931",
                               RCCL_EXTRA_LD_PATH="/extra/site-lib")
        # The fake binary records the first LD_LIBRARY_PATH entry it sees, so the
        # rank environment proves the site library path survived MPI forwarding.
        self.tool("all_reduce_perf", self.rccl_body.replace(
            "iface=%s", "ld=%s|iface=%s").replace(
            '"${NCCL_SOCKET_IFNAME:-}"', '"${LD_LIBRARY_PATH%%:*}" "${NCCL_SOCKET_IFNAME:-}"'))
        result = self.run_payload("rccl_perf_test.sh", self.rccl_args(2, 1) +
                                  ["--tests", "all_reduce"],
                                  env={"CLUSTER_ENV_FILE": conf})
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        mpi = self.mpi_calls.read_text(encoding="utf-8").splitlines()
        self.assertIn("-p 26931", mpi)
        exports = self._mpi_exports()
        self.assertIn("NCCL_SOCKET_IFNAME=site-iface0", exports)
        self.assertIn("NCCL_NET_PLUGIN=siteplug", exports)
        self.assertTrue(all("ld=/extra/site-lib" in line for line in self.tool_lines()),
                        self.tool_lines())

    def test_env_value_beats_site_conf_default(self):
        conf = self._site_conf(RCCL_IFACE="site-iface0", NCCL_NET_PLUGIN="siteplug")
        result = self.run_payload("rccl_perf_test.sh", self.rccl_args(2, 1) +
                                  ["--tests", "all_reduce"],
                                  env={"CLUSTER_ENV_FILE": conf,
                                       "NCCL_SOCKET_IFNAME": "env-iface9",
                                       "NCCL_NET_PLUGIN": "envplug"})
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        exports = self._mpi_exports()
        self.assertIn("NCCL_SOCKET_IFNAME=env-iface9", exports)
        self.assertIn("NCCL_NET_PLUGIN=envplug", exports)
        self.assertNotIn("NCCL_SOCKET_IFNAME=site-iface0", exports)
        self.assertNotIn("NCCL_NET_PLUGIN=siteplug", exports)

    def test_rccl_root_flag_is_opt_in_and_guard_file_covers_each_rank(self):
        token = uuid.uuid4().hex
        result = self.run_payload("rccl_perf_test.sh", self.rccl_args(2, 1) +
                                  ["--tests", "all_reduce", "--dry-run"],
                                  env={"HCU_TASK_TOKEN": token, "HCU_ALLOW_ROOT_MPI": "0"})
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn("--allow-run-as-root", result.stdout)
        # 多节点 rank 命令只能携带无空格路径：guard 与 rank 本体落盘到共享
        # log-dir，每个 rank 执行同一份 guard 文件，token 契约不因文件化削弱。
        self.assertIn("task-guard.sh run " + token, result.stdout)
        self.assertIn("rank-body.sh", result.stdout)
        guard_file = self.root / "rccl-logs" / "task-guard.sh"
        rank_file = self.root / "rccl-logs" / "rank-body.sh"
        self.assertTrue(guard_file.is_file(), result.stdout)
        self.assertTrue(rank_file.is_file(), result.stdout)
        self.assertIn("HCU_TASK_MEMBER", guard_file.read_text(encoding="utf-8"))
        self.assertIn("hcu_rccl_args", rank_file.read_text(encoding="utf-8"))
        self.assertFalse(self.tool_lines())

    def test_root_without_optin_fails_before_mpi(self):
        uid = subprocess.run([BASH, "-c", 'printf "%s" "$EUID"'], capture_output=True, text=True).stdout
        if uid != "0":
            self.skipTest("root-only authorization branch")
        result = self.run_payload("rccl_perf_test.sh", self.rccl_args(2, 1) + ["--tests", "all_reduce"],
                                  env={"HCU_ALLOW_ROOT_MPI": "0", "OMPI_ALLOW_RUN_AS_ROOT": "1",
                                       "OMPI_ALLOW_RUN_AS_ROOT_CONFIRM": "1"})
        self.assertEqual(result.returncode, 1)
        self.assertFalse(self.mpi_calls.exists())

    def test_rccl_failed_env_prevents_binary_execution(self):
        rank_env = self.root / "bad-env.sh"
        rank_env.write_text("false\nexport BENCH_ENV_READY=leader\n", encoding="utf-8")
        result = self.run_payload("rccl_perf_test.sh", self.rccl_args() + ["--tests", "all_reduce"],
                                  env={"HCU_CLUSTER_ENV_SCRIPT": _target_path(rank_env)})
        self.assertEqual(result.returncode, 2)
        self.assertFalse(self.tool_lines())

    def test_rccl_environment_root_resolves_test_without_fixed_install_path(self):
        root = self.root / "site-dtk"
        target_dir = root / "rccl-tests" / "build"
        target_dir.mkdir(parents=True)
        shutil.move(self.bin / "all_reduce_perf", target_dir / "all_reduce_perf")
        result = self.run_payload("rccl_perf_test.sh", self.rccl_args() + ["--tests", "all_reduce"],
                                  env={"DTK_ROOT": _target_path(root)})
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(self.tool_lines()), 1)

    def test_cancelled_benchmark_stops_remaining_tests(self):
        rccl = self.run_payload("rccl_perf_test.sh", self.rccl_args(), env={"FAKE_BENCH_RC": "130"})
        self.assertEqual(rccl.returncode, 130, rccl.stdout + rccl.stderr)
        self.assertEqual(len(self.tool_lines()), 1)
        self.calls.unlink()
        gemm = self.run_payload("gemm_perf_test.sh", ["--cards", "0,1", "--skip-idle-check"],
                                env={"FAKE_BENCH_RC": "130"})
        self.assertEqual(gemm.returncode, 130, gemm.stdout + gemm.stderr)
        self.assertEqual(len(self.tool_lines()), 1)

    @unittest.skipUnless(os.name == "posix", "task guard requires Linux /proc/setsid/flock")
    def test_linux_managed_profiles_execute_real_guard(self):
        rccl = self.run_payload("rccl_perf_test.sh", self.rccl_args(2, 1) + ["--tests", "all_reduce"],
                                env={"HCU_TASK_TOKEN": uuid.uuid4().hex})
        self.assertEqual(rccl.returncode, 0, rccl.stdout + rccl.stderr)
        self.assertEqual(len(self.tool_lines()), 2)
        gemm = self.run_payload("gemm_perf_test.sh", ["--shapes", "16x16x16", "--cards", "0", "--skip-idle-check"],
                                env={"HCU_TASK_TOKEN": uuid.uuid4().hex})
        self.assertEqual(gemm.returncode, 0, gemm.stdout + gemm.stderr)

    def test_gemm_preserves_default_shapes_and_eight_card_enumeration(self):
        result = self.run_payload("gemm_perf_test.sh", ["--dry-run"])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(result.stdout.count("[dry-run] HIP_VISIBLE_DEVICES="), 24)
        for shape in ("8192x8192x8192", "4096x4096x4096", "2048x2048x8192"):
            self.assertIn(shape, result.stdout)

    def test_gemm_csv_result_and_baselines_all_selected_cards(self):
        baseline = self.root / "gemm.conf"
        baseline.write_text("4096x4096x4096 bf16_r 49 3\n2048x2048x8192 bf16_r 49 3\n", encoding="utf-8")
        result = self.run_payload("gemm_perf_test.sh", ["--baseline-file", _target_path(baseline),
                                  "--cards", "0,1", "--dtype", "bf16_r", "--skip-idle-check"])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(self.tool_lines()), 4)
        self.assertEqual(result.stdout.count("[PASS]"), 4)
        self.assertTrue(all("-r bf16_r" in line for line in self.tool_lines()))

    def test_gemm_environment_root_and_explicit_binary(self):
        root = self.root / "custom-dtk"
        original = self.bin / "rocblas-bench"
        target = root / "rocblas" / "bin" / "rocblas-bench"
        target.parent.mkdir(parents=True)
        shutil.move(original, target)
        result = self.run_payload("gemm_perf_test.sh", ["--shapes", "16x16x16", "--cards", "0", "--skip-idle-check"],
                                  env={"DTK_ROOT": _target_path(root)})
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(_target_path(target), result.stdout)
        explicit = self.run_payload("gemm_perf_test.sh", ["--bin", _target_path(target),
                                    "--shapes", "16x16x16", "--cards", "1", "--skip-idle-check"])
        self.assertEqual(explicit.returncode, 0, explicit.stdout + explicit.stderr)
        self.assertEqual(len(self.tool_lines()), 2)

    def test_gemm_missing_shape_baseline_is_not_a_zero_baseline_pass(self):
        baseline = self.root / "gemm.conf"
        baseline.write_text("4096x4096x4096 f16_r 100\n", encoding="utf-8")
        result = self.run_payload("gemm_perf_test.sh", ["--baseline-file", _target_path(baseline),
                                  "--shapes", "16x16x16", "--cards", "0"])
        self.assertEqual(result.returncode, 1)
        self.assertFalse(self.tool_lines())

    def test_gemm_tool_failure_and_baseline_failure(self):
        args = ["--shapes", "16x16x16", "--cards", "0", "--skip-idle-check"]
        failed = self.run_payload("gemm_perf_test.sh", args, env={"FAKE_BENCH_RC": "7"})
        self.assertEqual(failed.returncode, 2, failed.stdout + failed.stderr)
        baseline = self.root / "gemm.conf"
        baseline.write_text("16x16x16 f16_r 100\n", encoding="utf-8")
        slow = self.run_payload("gemm_perf_test.sh", args + ["--baseline-file", _target_path(baseline)])
        self.assertEqual(slow.returncode, 2, slow.stdout + slow.stderr)

    def test_payload_does_not_source_an_implicit_common_script(self):
        copied = self.root / "gemm_perf_test.sh"
        shutil.copyfile(PAYLOADS / copied.name, copied)
        (self.root / "common.sh").write_text("exit 99\n", encoding="utf-8")
        result = self.run_payload(copied, ["--shapes", "16x16x16", "--cards", "0", "--dry-run"])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
