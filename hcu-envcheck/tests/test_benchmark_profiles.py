# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Execute full shell profiles against fake tools; never touch GPUs or MPI."""

from __future__ import annotations

import os
import json
import sys
import csv
import shlex
import shutil
import subprocess
import tempfile
import unittest
import uuid
from pathlib import Path

from tests.test_extra_interfaces import BASH, _target_path


PAYLOADS = Path(__file__).resolve().parents[1] / "hcu_envcheck" / "payloads"
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
        self.mpi_hosts = self.root / "mpi-hosts"
        self.guard_calls = self.root / "guard-calls"
        self.env_script = self.root / "env.sh"
        self.env_script.write_text(
            'export BENCH_ENV_READY="${FAKE_RANK_NODE:-leader}"\n'
            # On Windows prefer MSYS tools over unrelated native Windows mkdir
            # found on the inherited PATH; /c/... must retain POSIX semantics.
            f'export PATH={shlex.quote(_target_path(self.bin))}:/usr/bin:/bin:$PATH\n', encoding="utf-8")
        self.env = {name: value for name, value in os.environ.items()
                    if not name.startswith(("RCCL_", "HCU_", "CLUSTER_", "NCCL_", "OMPI_", "PRTE_", "DEFAULTS_"))
                    and name not in {"MPIRUN_BIN", "MPI_BIN", "SSH_PORT", "UCX_NET_DEVICES"}}
        self.env.update(TOOL_CALLS=_target_path(self.calls), MPI_ARGS=_target_path(self.mpi_calls),
                        MPI_HOSTS=_target_path(self.mpi_hosts),
                        GUARD_CALLS=_target_path(self.guard_calls),
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
        # Exercise the actual selector, including record/mask/PCI checks, with
        # a CPU-only HIP probe. No real runtime, GPU or performance tool is used.
        probe = self.root / "python-fixture.py"
        probe.write_text(r'''
import json, os, runpy, subprocess, sys
args = sys.argv[1:]
if args[:1] == ["-B"]:
    args = args[1:]
if args and args[0].endswith("/device_selection.py"):
    real_run = subprocess.run
    def run(command, **kwargs):
        if command[-1:] == ["--probe"] and command[-2].endswith("/device_selection.py"):
            env = kwargs["env"]
            mask = env.get("HIP_VISIBLE_DEVICES", ",".join(map(str, range(8))))
            node = env.get("FAKE_RANK_NODE", "node0")
            index = int(node[4:]) if node.startswith("node") else 0
            pci = [f"0000:{3 + index * 16 + int(i):02x}:00.0" for i in mask.split(",")]
            return subprocess.CompletedProcess(command, 0, json.dumps(pci), "")
        return real_run(command, **kwargs)
    subprocess.run = run
    sys.argv = args
    runpy.run_path(args[0], run_name="__main__")
else:
    os.execv(sys.executable, [sys.executable, *sys.argv[1:]])
''', encoding="utf-8")
        self.tool("python3", "exec " + shlex.quote(_target_path(Path(sys.executable)))
                  + " " + shlex.quote(_target_path(probe)) + ' "$@"\n')
        self.rccl_body = r'''
[[ ${BENCH_ENV_READY:-unset} == ${FAKE_RANK_NODE:-leader} ]] || exit 91
if [[ -n ${HCU_TASK_TOKEN:-} ]]; then
  [[ ${HCU_TASK_MEMBER:-} =~ ^[0-9]+:[0-9]+$ ]] || exit 93
  printf '%s|%s\n' "$HCU_TASK_TOKEN" "$HCU_TASK_MEMBER" >> "$GUARD_CALLS"
fi
expected_mask=$(seq -s, 0 "$((HCU_GPUS_PER_NODE - 1))")
[[ ${HIP_VISIBLE_DEVICES:-} == "$expected_mask" ]] || exit 94
cards= minimum= maximum= factor=
for ((i=1; i<=$#; i++)); do
  j=$((i+1))
  case ${!i} in
    -g) cards=${!j};;
    -b) minimum=${!j};;
    -e) maximum=${!j};;
    -f) factor=${!j};;
  esac
done
printf '%s|%s|g=%s|iface=%s\n' "$(basename "$0")" "${FAKE_RANK_NODE:-leader}" "$cards" "${NCCL_SOCKET_IFNAME:-}" >> "$TOOL_CALLS"
[[ ${FAKE_BENCH_RC:-0} == 0 ]] || exit "$FAKE_BENCH_RC"
bytes() {
  local value=${1^^}
  case $value in
    *K) printf '%s' "$(( ${value%K} * 1024 ))";;
    *M) printf '%s' "$(( ${value%M} * 1024 * 1024 ))";;
    *G) printf '%s' "$(( ${value%G} * 1024 * 1024 * 1024 ))";;
    *) printf '%s' "$value";;
  esac
}
minimum=$(bytes "$minimum"); maximum=$(bytes "$maximum")
printf '# nThread 1 nGpus %s minBytes %s maxBytes %s step: %s(factor)\n' "${cards:-1}" "$minimum" "$maximum" "$factor"
for ((device=0; device<${cards:-1}; device++)); do
  printf '# Rank %s Group 0 Pid %s on %s device %s\n' "${OMPI_COMM_WORLD_RANK:-$device}" "$$" "${FAKE_RANK_NODE:-leader}" "${OMPI_COMM_WORLD_LOCAL_RANK:-$device}"
done
[[ ${OMPI_COMM_WORLD_RANK:-0} == 0 ]] || exit 0
for ((size=minimum; size<=maximum; size*=factor)); do
  printf '%s 1 float sum -1 1 200 200 0 1 200 200 0\n' "$size"
done
echo '# Avg bus bandwidth : 200'
'''
        for name in RCCL_TESTS:
            self.tool(name + "_perf", self.rccl_body)
        self.tool("hy-smi", 'for ((i=0; i<8; i++)); do echo "$i 40 0 perf clk 0% 0%"; done\n')
        self.tool("mpirun", r'''
printf '%s\n' "$@" > "$MPI_ARGS"
hosts= np= hostfile=
exports=()
while (($#)); do
  case "$1" in
    --allow-run-as-root) shift;;
    --mca) shift 3;;
    --host) hosts=$2; shift 2;;
    --hostfile) hostfile=$2; shift 2;;
    -np) np=$2; shift 2;;
    -x)
      setting=$2
      if [[ $setting == *=* ]]; then exports+=("$setting")
      elif [[ -v $setting ]]; then exports+=("$setting=${!setting}"); fi
      shift 2;;
    --map-by|--rank-by|--bind-to|--wdir) shift 2;;
    *) break;;
  esac
done
if [[ -n $hostfile ]]; then
  [[ -f $hostfile ]] || exit 92
  cp -- "$hostfile" "$MPI_HOSTS"
  while read -r node slots _ || [[ -n $node ]]; do
    [[ -n $node && $slots =~ ^slots=([1-9][0-9]*)$ ]] || exit 92
    hosts+="${hosts:+,}${node}:${BASH_REMATCH[1]}"
  done < "$hostfile"
fi
[[ -n $hosts && -n $np ]] || exit 92
rank=0
IFS=',' read -r -a items <<< "$hosts"
for item in "${items[@]}"; do
  node=${item%:*}; count=${item##*:}
  for ((local_rank=0; local_rank<count; local_rank++)); do
    env -u BENCH_ENV_READY "${exports[@]}" FAKE_RANK_NODE="$node" OMPI_COMM_WORLD_RANK="$rank" OMPI_COMM_WORLD_LOCAL_RANK="$local_rank" "$@" || exit $?
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
        self.gpus_per_node = slots
        group = self.root / "prepared" / "group-000"
        group.mkdir(parents=True, exist_ok=True)
        fields = ["node", "gpus_per_node", "hip_visible_devices", "pci_devices"]
        rows = [[f"node{i}", slots, ",".join(map(str, range(slots))),
                 [f"0000:{3 + i * 16 + j:02x}:00.0" for j in range(slots)]] for i in range(nodes)]
        (group / "facts.json").write_text(json.dumps({"schema_version": 2, "group": "group-000",
            "revision": 1, "tables": {"devices": {"fields": fields, "rows": rows}}}), encoding="utf-8")
        self.env.update(HCU_DEVICE_SELECTION_DIR=_target_path(group / "devices"), HCU_GROUP_HOSTFILE=_target_path(path))
        return _target_path(path)  # deliberately no trailing newline

    def baseline(self, np=8):
        path = self.root / "rccl.conf"
        path.write_text("\n".join(f"{np} {name} 100 1" for name in RCCL_TESTS) + "\n", encoding="utf-8")
        return _target_path(path)

    def run_payload(self, script, args, *, env=None):
        if script == "rccl_perf_test.sh":
            args = [*args, "--gpus-per-node", str(self.gpus_per_node)]
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
                                  ["--tests", "all_reduce", "--port", "25901"], env={"NCCL_SOCKET_IFNAME": "test0"})
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        mpi = self.mpi_calls.read_text(encoding="utf-8").splitlines()
        self.assertIn("-p 25901", mpi)
        self.assertIn("--allow-run-as-root", mpi)
        self.assertEqual(len(self.tool_lines()), 4)
        self.assertEqual({line.split("|")[1] for line in self.tool_lines()}, {"node0", "node1"})
        self.assertTrue(all("|g=|iface=test0" in line for line in self.tool_lines()))
        self.assertEqual(self.mpi_hosts.read_text(encoding="utf-8").splitlines(),
                         ["node0 slots=2", "node1 slots=2"])

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
        rsh = next(index for index in range(len(mpi) - 2)
                   if mpi[index:index + 2] == ["--mca", "plm_rsh_args"])
        self.assertEqual(mpi[rsh + 2], "-p 25901")
        self.assertIn("--hostfile", mpi)
        self.assertEqual(self.mpi_hosts.read_text(encoding="utf-8").splitlines(),
                         ["node0 slots=8", "node1 slots=8"])
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
            if name in {"NCCL_NET_GDR_LEVEL", "NCCL_NET_GDR_READ"}:
                self.assertIn(name, exports)
            else:
                self.assertIn(f"{name}={value}", exports)
            self.assertEqual(lines.count(f"{name}={value}"), 16)
        self.assertEqual(lines.count("LD_LIBRARY_PATH=/chosen/runtime/lib"), 16)
        flags = [line[4:] for line in lines if line.startswith("ARG=")]
        self.assertEqual(flags, ["-b", "4", "-e", "1G", "-f", "2", "-n", "20", "-w", "5"] * 16)
        self.assertEqual(len(self.tool_lines()), 16)
        self.assertIn("broadcast: PASS", result.stderr)
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
            self.assertEqual(row["reason"], "PASS")
            for field in ("out_of_place_algbw_gbps", "out_of_place_busbw_gbps", "in_place_algbw_gbps", "in_place_busbw_gbps"):
                self.assertEqual(float(row[field]), 200.0)
            self.assertEqual(float(row["baseline_busbw_gbps"]), 100.0)
            self.assertEqual(float(row["threshold_busbw_gbps"]), 99.0)
            self.assertEqual(int(row["planned_sizes"]), 29)
            self.assertEqual(int(row["table_rows"]), 29)
            self.assertEqual(int(row["missing_sizes"]), 0)
            self.assertEqual(int(row["invalid_rows"]), 0)
            self.assertEqual(int(row["actual_np"]), 2)
            self.assertEqual(int(row["actual_nodes"]), 2)
            self.assertEqual(int(row["devices_per_node"]), 1)
            self.assertEqual(int(row["rank_duplicates"]), 0)
            self.assertEqual(int(row["wrong_out"]), 0)
            self.assertEqual(int(row["wrong_in"]), 0)
            self.assertEqual(int(row["end_evidence"]), 1)
        markdown = (self.root / "rccl-logs/rccl-summary.md").read_text(encoding="utf-8")
        self.assertIn("out algbw", markdown)
        self.assertIn("sendrecv", markdown)
        self.assertEqual(sum(": PASS " in line for line in result.stderr.splitlines()), 10)

    def test_one_failure_does_not_skip_remaining_collectives(self):
        self.tool("all_reduce_perf", self.rccl_body.replace(
            '[[ ${FAKE_BENCH_RC:-0} == 0 ]]', 'FAKE_BENCH_RC=7\n[[ ${FAKE_BENCH_RC:-0} == 0 ]]'))
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
        self.tool("broadcast_perf", self.rccl_body.replace(
            "200 200 0 1 200 200", "130.96 122.78 0 1 131.15 122.95"))
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

    def test_missing_binary_blocks_complete_suite_before_any_launch(self):
        (self.bin / "all_reduce_perf").unlink()
        result = self.run_payload("rccl_perf_test.sh", self.rccl_args(), env={"RCCL_BIN_DIR": _target_path(self.bin)})
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("all_reduce_perf", result.stderr)
        self.assertFalse(self.tool_lines())
        self.assertFalse(self.mpi_calls.exists())
        self.assertFalse((self.root / "rccl-logs/execution.tsv").exists())
        self.assertFalse((self.root / "rccl-logs/rccl-summary.tsv").exists())

    def test_average_bandwidth_alone_cannot_replace_complete_table(self):
        self.tool("broadcast_perf", "echo '# Avg bus bandwidth : 200'\n")
        result = self.run_payload("rccl_perf_test.sh", self.rccl_args() + ["--tests", "broadcast"])
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        with (self.root / "rccl-logs/rccl-summary.tsv").open(encoding="utf-8") as stream:
            row = next(csv.DictReader(stream, delimiter="\t"))
        self.assertEqual(row["status"], "FAIL")
        self.assertEqual(row["reason"], "DATA_TABLE_MISSING")
        self.assertEqual(float(row["avg_busbw_gbps"]), 200.0)
        # table_text serializes missing numeric values with clean(None),
        # whose current TSV spelling is the literal "None", never zero/Avg.
        self.assertEqual(row["peak_busbw_gbps"], "None")
        for field in ("out_of_place_algbw_gbps", "out_of_place_busbw_gbps",
                      "in_place_algbw_gbps", "in_place_busbw_gbps"):
            self.assertEqual(row[field], "None")

    def test_network_environment_overrides_defaults_and_snapshot_wins_after_rank_source(self):
        with self.env_script.open("a", encoding="utf-8") as stream:
            stream.write('export NCCL_NET_PLUGIN=site-plugin NCCL_PXN_DISABLE=1 RCCL_PXN_GPU_BALANCE=0\n'
                         'export NCCL_NET_GDR_LEVEL=2 NCCL_NET_GDR_READ=0 NCCL_TOPO_FILE=/site/topo.xml\n'
                         'export NCCL_SOCKET_IFNAME=env0 UCX_NET_DEVICES=env-ib\n'
                         # The leader chooses the target configuration; ranks
                         # source conflicting defaults before the saved snapshot.
                         'if [[ ${FAKE_RANK_NODE:-leader} == leader ]]; then\n'
                         '  export NCCL_SOCKET_IFNAME=chosen0 UCX_NET_DEVICES=chosen-ib NCCL_TOPO_FILE=/chosen/topo.xml\n'
                         'fi\n')
        evidence = self.root / "network-evidence"
        self.tool("broadcast_perf", r'''
for name in NCCL_NET_PLUGIN NCCL_PXN_DISABLE RCCL_PXN_GPU_BALANCE NCCL_NET_GDR_LEVEL NCCL_NET_GDR_READ NCCL_TOPO_FILE NCCL_SOCKET_IFNAME UCX_NET_DEVICES; do
  printf '%s=%s\n' "$name" "${!name:-}" >> "$CORE_EVIDENCE"
done
''' + self.rccl_body)
        result = self.run_payload("rccl_perf_test.sh", [self.hostfile(2, 1), "--tests", "broadcast",
                                  "--port", "26930", "--skip-idle-check"],
                                  env={"CORE_EVIDENCE": _target_path(evidence)})
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        mpi = self.mpi_calls.read_text(encoding="utf-8").splitlines()
        exports = self._mpi_exports()
        actual = evidence.read_text(encoding="utf-8").splitlines()
        for setting in ("NCCL_NET_PLUGIN=site-plugin", "NCCL_PXN_DISABLE=1", "RCCL_PXN_GPU_BALANCE=0",
                        "NCCL_NET_GDR_LEVEL=2", "NCCL_NET_GDR_READ=0", "NCCL_TOPO_FILE=/chosen/topo.xml",
                        "NCCL_SOCKET_IFNAME=chosen0", "UCX_NET_DEVICES=chosen-ib"):
            name = setting.split("=", 1)[0]
            if name in {"NCCL_NET_GDR_LEVEL", "NCCL_NET_GDR_READ"}:
                self.assertIn(name, exports)
            else:
                self.assertIn(setting, exports)
            self.assertEqual(actual.count(setting), 2)
        self.assertIn("-p 26930", mpi)
        self.assertTrue(all("iface=chosen0" in line for line in self.tool_lines()))

    def test_unset_plane_is_not_invented_and_unknown_collective_is_rejected(self):
        result = self.run_payload("rccl_perf_test.sh", self.rccl_args(2, 1) + ["--tests", "broadcast"])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        mpi = self.mpi_calls.read_text(encoding="utf-8").splitlines()
        self.assertFalse(any(item.startswith("RCCL_NET_PLANE=") for item in mpi))
        rank_body = (self.root / "rccl-logs/rank-body.sh").read_text(encoding="utf-8")
        self.assertNotIn("export RCCL_NET_PLANE=", rank_body)
        before = len(self.tool_lines())
        bad = self.run_payload("rccl_perf_test.sh", [self.hostfile(2, 1), "--tests", "bad_collective", "--skip-idle-check"])
        self.assertNotEqual(bad.returncode, 0)
        self.assertEqual(len(self.tool_lines()), before)

    def test_rccl_explicit_binary_directory_and_low_baseline_result(self):
        other = self.root / "explicit binaries"
        self.tool("all_reduce_perf", self.rccl_body.replace("200 200", "20 20"), other)
        result = self.run_payload("rccl_perf_test.sh", self.rccl_args() +
                                  ["--tests", "all_reduce"], env={"RCCL_BIN_DIR": _target_path(other)})
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        with (self.root / "rccl-logs/rccl-summary.tsv").open(encoding="utf-8") as stream:
            row = next(csv.DictReader(stream, delimiter="\t"))
        self.assertEqual(row["status"], "FAIL")
        self.assertEqual(row["reason"], "BELOW_BASELINE")
        self.assertEqual(float(row["peak_busbw_gbps"]), 20.0)
        self.assertEqual(float(row["threshold_busbw_gbps"]), 99.0)

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
        self.assertIn("HCU_RCCL_GUARD_SCRIPT=", result.stdout)
        self.assertIn("task-guard.sh", result.stdout)
        self.assertIn("rank-body.sh", result.stdout)
        guard_file = self.root / "rccl-logs" / "task-guard.sh"
        rank_file = self.root / "rccl-logs" / "rank-body.sh"
        self.assertTrue(guard_file.is_file(), result.stdout)
        self.assertTrue(rank_file.is_file(), result.stdout)
        self.assertIn("HCU_TASK_MEMBER", guard_file.read_text(encoding="utf-8"))
        rank_body = rank_file.read_text(encoding="utf-8")
        self.assertIn("source " + shlex.quote(_target_path(self.env_script)), rank_body)
        self.assertIn('bin=$(resolve_test_bin "$1")', rank_body)
        self.assertIn('exec "$python_bin" -B ', rank_body)
        self.assertIn('device_selection.py exec --count "$HCU_GPUS_PER_NODE"', rank_body)
        self.assertIn('--record "$HCU_DEVICE_SELECTION_DIR/$node.json" --node "$node" -- "$bin" "$@"', rank_body)
        launcher = (PAYLOADS / "test_rccl.sh").read_text(encoding="utf-8")
        self.assertIn('"${HCU_RCCL_GUARD_SCRIPT}" run "${HCU_TASK_TOKEN}" -- "${RANK_CMD[@]}"', launcher)
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

    @unittest.skipUnless(os.name == "posix", "task guard requires Linux /proc/setsid/flock")
    def test_linux_managed_profiles_execute_real_guard(self):
        token = uuid.uuid4().hex
        rccl = self.run_payload("rccl_perf_test.sh", self.rccl_args(2, 1) + ["--tests", "all_reduce"],
                                env={"HCU_TASK_TOKEN": token})
        self.assertEqual(rccl.returncode, 0, rccl.stdout + rccl.stderr)
        self.assertEqual(len(self.tool_lines()), 2)
        guarded = [line.split("|") for line in self.guard_calls.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(len(guarded), 2)
        self.assertEqual({entry[0] for entry in guarded}, {token})
        self.assertEqual(len({entry[1] for entry in guarded}), 2)
        for _, member in guarded:
            self.assertRegex(member, r"^[0-9]+:[0-9]+$")


if __name__ == "__main__":
    unittest.main()
