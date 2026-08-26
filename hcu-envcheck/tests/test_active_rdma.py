# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from hcu_envcheck.active_rdma import (
    RcclCheckConfig,
    SlurmActiveCheckRunner,
    SlurmActiveContext,
    TorchRcclCheckConfig,
    VerbsCheckConfig,
)


def context(**overrides):
    values = {
        "job_id": "674118",
        "selected_nodes": ("e06r1n08", "e06r1n09"),
        "enabled": True,
        "confirm_allocation_idle": True,
        "current_user": "qianyj1",
        "controller_hostname": "zz-login01",
    }
    values.update(overrides)
    return SlurmActiveContext(**values)


VERBS_METADATA = (
    "Device : shca_0\n"
    "Transport type : IB\n"
    "Link type : IB\n"
)


def allocation_control_result(argv, **kwargs):
    executable = Path(argv[0]).name
    if executable == "squeue" and "--steps" in argv:
        if argv[argv.index("-o") + 1] != "%i|%N":
            raise AssertionError("step query must use portable Slurm fields")
        return subprocess.CompletedProcess(
            argv,
            0,
            "674118.extern|e06r1n08\n",
            "",
        )
    if executable == "squeue" and "-w" in argv:
        if argv[argv.index("-o") + 1] != "%i|%T|%u|%N":
            raise AssertionError("node occupancy query must preserve job identity")
        return subprocess.CompletedProcess(
            argv,
            0,
            "674118|RUNNING|qianyj1|e06r1n[08-09]\n",
            "",
        )
    if executable == "squeue" and "-j" in argv:
        return subprocess.CompletedProcess(
            argv, 0, "674118|RUNNING|qianyj1|e06r1n[08-09]\n", ""
        )
    if executable == "scontrol" and argv[1:3] == ["show", "hostnames"]:
        return subprocess.CompletedProcess(argv, 0, "e06r1n08\ne06r1n09\n", "")
    if executable == "scontrol" and argv[1:4] == ["show", "job", "-o"]:
        return subprocess.CompletedProcess(
            argv,
            0,
            "JobId=674118 JobState=RUNNING Exclusive=NODE NodeList=e06r1n[08-09]\n",
            "",
        )
    raise AssertionError(f"unexpected controller command: {argv}")



def strict_rccl_output(
    *,
    nodes=("e06r1n08", "e06r1n09"),
    tasks_per_node=1,
    devices_per_task=1,
    transport="IBext_v8",
    gdr="DISABLED",
    minimum=8 * 1024 * 1024,
    maximum=128 * 1024 * 1024,
    factor=2,
    algbw=84.0,
    busbw=84.0,
    average_busbw=84.2,
    wrong="0",
    omit_transport_ranks=(),
    omit_device_ranks=(),
    extra="",
):
    """Return complete rccl-tests evidence accepted by the strict parser."""

    devices_per_node = tasks_per_node * devices_per_task
    nranks = len(nodes) * devices_per_node
    omitted = set(omit_transport_ranks)
    missing_devices = set(omit_device_ranks)
    lines = [
        f"# nThread 1 nGpus {devices_per_task} minBytes {minimum} "
        f"maxBytes {maximum} step: {factor}(factor)",
        "# Using devices",
    ]
    rank = 0
    for node_index, node in enumerate(nodes):
        for local_device in range(devices_per_node):
            task = local_device // devices_per_task
            pid = 1000 + node_index * 100 + task
            bdf = f"0000:{0x20 + local_device:02x}:00.0"
            if rank not in missing_devices:
                lines.append(
                    f"# Rank {rank} Pid {pid} on {node} device {local_device} [{bdf}] BW"
                )
            if rank not in omitted:
                lines.append(
                    f"{node}:{pid}:{2000 + rank} [{local_device}] "
                    f"NCCL INFO Using network {transport}"
                )
                if gdr == "ENABLED":
                    lines.append(
                        f"{node}:{pid}:{2000 + rank} [{local_device}] NCCL INFO "
                        f"Channel 00 via NET/{transport}/0/GDRDMA"
                    )
                elif gdr == "DISABLED":
                    lines.append(
                        f"{node}:{pid}:{2000 + rank} [{local_device}] NCCL INFO "
                        "GPU Direct RDMA Disabled for HCA 0"
                    )
            lines.append(
                f"{node}:{pid}:{2000 + rank} [{local_device}] NCCL INFO "
                f"ncclCommInitRank rank {rank} nranks {nranks}"
            )
            rank += 1
    if extra:
        lines.extend(extra.rstrip("\n").splitlines())
    lines.append(
        "# size count type redop root time algbw busbw #wrong "
        "time algbw busbw #wrong"
    )
    size = minimum
    while size <= maximum:
        lines.append(
            f"{size} {size // 4} float sum -1 10.0 {algbw} {busbw} {wrong} "
            f"9.0 {algbw} {busbw} {wrong}"
        )
        if size == maximum:
            break
        size *= factor
    lines.extend(
        [
            "# Out of bounds values : 0 OK",
            f"# Avg bus bandwidth : {average_busbw}",
        ]
    )
    return "\n".join(lines) + "\n"
def legacy_job_record(
    *,
    num_nodes=2,
    num_cpus=256,
    allocated_cpus=256,
    allocated_hcus=16,
    include_allocated_tres=True,
):
    allocated_tres = (
        f" AllocTRES=cpu={allocated_cpus},mem=972G,node={num_nodes},"
        f"gres/hcu={allocated_hcus}"
        if include_allocated_tres
        else ""
    )
    return (
        "JobId=674118 JobState=RUNNING OverSubscribe=NO "
        f"NumNodes={num_nodes} NumCPUs={num_cpus}"
        f"{allocated_tres} NodeList=e06r1n[08-09]\n"
    )


def legacy_node_record(
    node,
    *,
    state="ALLOCATED",
    cpu_alloc=128,
    cpu_total=128,
    configured_hcus=8,
    allocated_hcus=8,
    include_allocated_tres=True,
):
    allocated_tres = (
        f" AllocTRES=cpu={cpu_alloc},mem=486G,gres/hcu={allocated_hcus}"
        if include_allocated_tres
        else ""
    )
    return (
        f"NodeName={node} CPUAlloc={cpu_alloc} CPUEfctv={cpu_total} "
        f"CPUTot={cpu_total} RealMemory=510000 AllocMem=497664 "
        f"MemSpecLimit=12288 State={state} "
        f"CfgTRES=cpu={cpu_total},mem=510000M,gres/hcu={configured_hcus}"
        f"{allocated_tres}\n"
    )


class FakePopen:

    def __init__(self, argv, **kwargs):
        self.argv = argv
        self.returncode = None

    def poll(self):
        return self.returncode

    def communicate(self, timeout=None):
        self.returncode = 0
        return (
            VERBS_METADATA + "#bytes #iterations BW peak[Gb/sec] BW average[Gb/sec] MsgRate[Mpps]\n"
            "1048576 1000 91.00 90.50 0.010\n",
            "",
        )

    def terminate(self):
        self.returncode = -15

    def kill(self):
        self.returncode = -9


class ActiveSafetyTests(unittest.TestCase):

    def test_disabled_is_not_verified_and_starts_no_process(self):
        calls = []

        def runner(argv, **kwargs):
            calls.append(argv)
            raise AssertionError("disabled check must not query Slurm or run a workload")

        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "disabled"
            result = SlurmActiveCheckRunner(runner=runner).run_verbs(
                context(enabled=False), VerbsCheckConfig(), output_dir=output
            )
            self.assertEqual(result.status, "NOT_VERIFIED")
            self.assertEqual(result.reason_code, "ACTIVE_CHECKS_DISABLED")
            self.assertEqual(result.backend, "VERBS")
            self.assertEqual(result.commands, [])
            self.assertEqual(calls, [])
            persisted = json.loads((output / "active-result.json").read_text())
            self.assertEqual(persisted["status"], "NOT_VERIFIED")

    def test_active_node_count_is_bounded_before_controller_queries(self):
        calls = []

        def forbidden(*args, **kwargs):
            calls.append((args, kwargs))
            raise AssertionError("oversized active check must not query Slurm")

        nodes = tuple(f"node{index:02d}" for index in range(17))
        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=forbidden).run_rccl(
                context(selected_nodes=nodes),
                RcclCheckConfig(binary="all_reduce_perf"),
                output_dir=Path(temp) / "too-many-nodes",
            )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(result.reason_code, "INVALID_ACTIVE_CHECK_CONFIGURATION")
        self.assertEqual(calls, [])

    def test_default_batch_step_blocks_before_workload(self):
        workload_calls = []

        def runner(argv, **kwargs):
            if Path(argv[0]).name == "squeue" and "--steps" in argv:
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    "674118.batch|e06r1n08\n674118.extern|e06r1n08\n",
                    "",
                )
            if Path(argv[0]).name == "srun":
                workload_calls.append(argv)
            return allocation_control_result(argv)

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_verbs(
                context(),
                VerbsCheckConfig(),
                output_dir=Path(temp) / "batch-step",
            )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(result.reason_code, "SLURM_ALLOCATION_HAS_BATCH_STEP")
        self.assertEqual(workload_calls, [])

    def test_max_selected_nodes_can_be_explicitly_raised_for_fabric_only(self):
        nodes = tuple(f"node{index:03d}" for index in range(17))
        with self.assertRaisesRegex(ValueError, "at most 16"):
            context(selected_nodes=nodes).validate()
        context(selected_nodes=nodes, max_selected_nodes=17).validate()
        with self.assertRaisesRegex(ValueError, "between 2 and 256"):
            context(max_selected_nodes=257).validate()

    def test_idle_confirmation_is_required_before_workload(self):
        workload_calls = []

        def runner(argv, **kwargs):
            if Path(argv[0]).name == "srun":
                workload_calls.append(argv)
            return allocation_control_result(argv)

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_verbs(
                context(confirm_allocation_idle=False),
                VerbsCheckConfig(),
                output_dir=Path(temp) / "idle",
            )
            self.assertEqual(result.status, "NOT_VERIFIED")
            self.assertEqual(result.reason_code, "ALLOCATION_IDLE_NOT_CONFIRMED")
            self.assertEqual(workload_calls, [])

    def test_selected_node_must_belong_to_allocation(self):
        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=allocation_control_result).run_verbs(
                context(selected_nodes=("e06r1n08", "e06r1n10")),
                VerbsCheckConfig(),
                output_dir=Path(temp) / "outside",
            )
            self.assertEqual(result.status, "NOT_VERIFIED")
            self.assertEqual(result.reason_code, "ACTIVE_TEST_NODE_OUTSIDE_ALLOCATION")

    def test_controller_login_node_is_rejected(self):
        def runner(argv, **kwargs):
            if Path(argv[0]).name == "scontrol":
                return subprocess.CompletedProcess(
                    argv, 0, "zz-login01\ne06r1n09\n", ""
                )
            if Path(argv[0]).name == "squeue" and "--steps" not in argv:
                return subprocess.CompletedProcess(
                    argv, 0, "674118|RUNNING|qianyj1|zz-login01,e06r1n09\n", ""
                )
            return allocation_control_result(argv)

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_verbs(
                context(selected_nodes=("zz-login01", "e06r1n09")),
                VerbsCheckConfig(),
                output_dir=Path(temp) / "login",
            )
            self.assertEqual(result.status, "NOT_VERIFIED")
            self.assertEqual(result.reason_code, "LOGIN_NODE_SELECTED_FOR_ACTIVE_TEST")

    def test_unexpected_active_job_step_blocks_by_default(self):
        workload_calls = []

        def runner(argv, **kwargs):
            if Path(argv[0]).name == "squeue" and "--steps" in argv:
                return subprocess.CompletedProcess(
                    argv, 0, "674118.7|RUNNING|e06r1n08\n", ""
                )
            if Path(argv[0]).name == "srun":
                workload_calls.append(argv)
            return allocation_control_result(argv)

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_verbs(
                context(),
                VerbsCheckConfig(),
                output_dir=Path(temp) / "steps",
            )
            self.assertEqual(result.status, "NOT_VERIFIED")
            self.assertEqual(result.reason_code, "SLURM_ALLOCATION_HAS_ACTIVE_STEPS")
            self.assertEqual(workload_calls, [])

    def test_allocation_owner_mismatch_blocks(self):
        def runner(argv, **kwargs):
            if Path(argv[0]).name == "squeue" and "--steps" not in argv:
                return subprocess.CompletedProcess(
                    argv, 0, "674118|RUNNING|someone-else|e06r1n[08-09]\n", ""
                )
            return allocation_control_result(argv)

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_verbs(
                context(),
                VerbsCheckConfig(),
                output_dir=Path(temp) / "owner",
            )
            self.assertEqual(result.status, "NOT_VERIFIED")
            self.assertEqual(result.reason_code, "SLURM_ALLOCATION_OWNER_MISMATCH")

    def test_formal_path_rejects_every_non_node_exclusive_mode(self):
        for mode in ("NO", "USER", "MCS", "TOPO"):
            workload_calls = []

            def runner(argv, **kwargs):
                if Path(argv[0]).name == "scontrol" and argv[1:4] == [
                    "show",
                    "job",
                    "-o",
                ]:
                    return subprocess.CompletedProcess(
                        argv,
                        0,
                        f"JobId=674118 JobState=RUNNING Exclusive={mode}\n",
                        "",
                    )
                if Path(argv[0]).name == "srun":
                    workload_calls.append(argv)
                return allocation_control_result(argv)

            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                result = SlurmActiveCheckRunner(runner=runner).run_verbs(
                    context(),
                    VerbsCheckConfig(),
                    output_dir=Path(temp) / f"exclusive-{mode.lower()}",
                )
            self.assertEqual(result.status, "NOT_VERIFIED")
            self.assertEqual(
                result.reason_code, "SLURM_ALLOCATION_NOT_NODE_EXCLUSIVE"
            )
            self.assertIn(f"Exclusive={mode}", result.message)
            self.assertEqual(workload_calls, [])

    def test_missing_exclusive_field_fails_closed(self):
        workload_calls = []

        def runner(argv, **kwargs):
            if Path(argv[0]).name == "scontrol" and argv[1:4] == [
                "show",
                "job",
                "-o",
            ]:
                return subprocess.CompletedProcess(
                    argv, 0, "JobId=674118 JobState=RUNNING\n", ""
                )
            if Path(argv[0]).name == "srun":
                workload_calls.append(argv)
            return allocation_control_result(argv)

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_verbs(
                context(),
                VerbsCheckConfig(),
                output_dir=Path(temp) / "exclusive-missing",
            )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(
            result.reason_code, "SLURM_ALLOCATION_EXCLUSIVE_EVIDENCE_MISSING"
        )
        self.assertEqual(workload_calls, [])

    def test_exclusive_query_failure_fails_closed(self):
        workload_calls = []

        def runner(argv, **kwargs):
            if Path(argv[0]).name == "scontrol" and argv[1:4] == [
                "show",
                "job",
                "-o",
            ]:
                return subprocess.CompletedProcess(argv, 1, "", "controller unavailable")
            if Path(argv[0]).name == "srun":
                workload_calls.append(argv)
            return allocation_control_result(argv)

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_verbs(
                context(),
                VerbsCheckConfig(),
                output_dir=Path(temp) / "exclusive-query-failed",
            )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(result.reason_code, "SLURM_CONTROL_QUERY_FAILED")
        self.assertEqual(workload_calls, [])

    def test_foreign_active_job_on_selected_node_blocks_formal_path(self):
        workload_calls = []

        def runner(argv, **kwargs):
            if Path(argv[0]).name == "squeue" and "-w" in argv:
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    "674118|RUNNING|qianyj1|e06r1n[08-09]\n"
                    "700001|RUNNING|other|e06r1n08\n",
                    "",
                )
            if Path(argv[0]).name == "srun":
                workload_calls.append(argv)
            return allocation_control_result(argv)

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_verbs(
                context(),
                VerbsCheckConfig(),
                output_dir=Path(temp) / "foreign-active-job",
            )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(
            result.reason_code, "SLURM_ALLOCATION_HAS_FOREIGN_ACTIVE_JOBS"
        )
        self.assertIn("700001", result.message)
        self.assertEqual(workload_calls, [])

    def test_foreign_job_query_failure_fails_closed(self):
        workload_calls = []

        def runner(argv, **kwargs):
            if Path(argv[0]).name == "squeue" and "-w" in argv:
                return subprocess.CompletedProcess(argv, 1, "", "scheduler timeout")
            if Path(argv[0]).name == "srun":
                workload_calls.append(argv)
            return allocation_control_result(argv)

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_verbs(
                context(),
                VerbsCheckConfig(),
                output_dir=Path(temp) / "foreign-query-failed",
            )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(result.reason_code, "SLURM_CONTROL_QUERY_FAILED")
        self.assertEqual(workload_calls, [])
    def test_legacy_full_node_capacity_equivalent_is_accepted(self):
        def runner(argv, **kwargs):
            if Path(argv[0]).name == "scontrol" and argv[1:4] == [
                "show",
                "job",
                "-o",
            ]:
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    "JobId=674118 JobState=RUNNING OverSubscribe=NO "
                    "NumNodes=2 NumCPUs=256 "
                    "AllocTRES=cpu=256,mem=972G,node=2,gres/hcu=16\n",
                    "",
                )
            if Path(argv[0]).name == "scontrol" and argv[1:4] == [
                "show",
                "node",
                "-o",
            ]:
                return subprocess.CompletedProcess(
                    argv, 0, legacy_node_record(argv[4]), ""
                )
            if Path(argv[0]).name == "srun":
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    VERBS_METADATA
                    + "#bytes #iterations BW peak[Gb/sec] BW average[Gb/sec] MsgRate[Mpps]\n"
                    "1048576 1000 92.00 91.25 0.011\n",
                    "",
                )
            return allocation_control_result(argv)

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(
                runner=runner, popen=FakePopen, sleeper=lambda _: None
            ).run_verbs(
                context(),
                VerbsCheckConfig(
                    device="shca_0",
                    minimum_average_gbps=80.0,
                    startup_grace_seconds=0,
                ),
                output_dir=Path(temp) / "legacy-full-node",
            )
        self.assertEqual(result.status, "PASS")
        self.assertEqual(
            result.metrics["safety_boundary"],
            "EXCLUSIVE_SLURM_ALLOCATION_AND_STEP",
        )
        self.assertIsNone(result.metrics["allocation_exclusive_mode"])
        self.assertEqual(result.metrics["allocation_oversubscribe_mode"], "NO")
        self.assertEqual(
            result.metrics["allocation_exclusivity_proof_source"],
            "SCONTROL_LEGACY_OVERSUBSCRIBE_NO_FULL_JOB_AND_NODE_CAPACITY",
        )
        self.assertEqual(
            [item["node"] for item in result.metrics["allocation_node_capacity_evidence"]],
            ["e06r1n08", "e06r1n09"],
        )
        self.assertTrue(
            all(
                item["full_capacity_proven"]
                for item in result.metrics["allocation_node_capacity_evidence"]
            )
        )
        job_evidence = result.metrics["allocation_job_capacity_evidence"]
        self.assertEqual(job_evidence["num_nodes"], 2)
        self.assertEqual(job_evidence["num_cpus"], 256)
        self.assertEqual(job_evidence["allocated_cpus"], 256)
        self.assertEqual(job_evidence["allocated_hcus"], 16)
        self.assertEqual(job_evidence["selected_cpu_capacity"], 256)
        self.assertTrue(job_evidence["full_capacity_proven"])

    def test_legacy_mixed_or_partial_node_is_rejected(self):
        workload_calls = []

        def runner(argv, **kwargs):
            if Path(argv[0]).name == "scontrol" and argv[1:4] == [
                "show",
                "job",
                "-o",
            ]:
                return subprocess.CompletedProcess(
                    argv, 0, legacy_job_record(), ""
                )
            if Path(argv[0]).name == "scontrol" and argv[1:4] == [
                "show",
                "node",
                "-o",
            ]:
                node = argv[4]
                output = (
                    legacy_node_record(node)
                    if node == "e06r1n08"
                    else legacy_node_record(
                        node, state="MIXED", cpu_alloc=1, allocated_hcus=8
                    )
                )
                return subprocess.CompletedProcess(argv, 0, output, "")
            if Path(argv[0]).name == "srun":
                workload_calls.append(argv)
            return allocation_control_result(argv)

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_verbs(
                context(),
                VerbsCheckConfig(),
                output_dir=Path(temp) / "legacy-partial-node",
            )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(
            result.reason_code, "SLURM_ALLOCATION_NOT_FULL_NODE_CAPACITY"
        )
        self.assertIn("e06r1n09", result.message)
        self.assertEqual(workload_calls, [])

    def test_legacy_missing_node_capacity_field_fails_closed(self):
        workload_calls = []

        def runner(argv, **kwargs):
            if Path(argv[0]).name == "scontrol" and argv[1:4] == [
                "show",
                "job",
                "-o",
            ]:
                return subprocess.CompletedProcess(
                    argv, 0, legacy_job_record(), ""
                )
            if Path(argv[0]).name == "scontrol" and argv[1:4] == [
                "show",
                "node",
                "-o",
            ]:
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    legacy_node_record(
                        argv[4], include_allocated_tres=False
                    ),
                    "",
                )
            if Path(argv[0]).name == "srun":
                workload_calls.append(argv)
            return allocation_control_result(argv)

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_verbs(
                context(),
                VerbsCheckConfig(),
                output_dir=Path(temp) / "legacy-capacity-missing",
            )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(
            result.reason_code, "SLURM_NODE_CAPACITY_EVIDENCE_MISSING"
        )
        self.assertEqual(workload_calls, [])

    def test_legacy_job_capacity_blocks_private_data_false_exclusive(self):
        workload_calls = []

        def runner(argv, **kwargs):
            if Path(argv[0]).name == "scontrol" and argv[1:4] == [
                "show",
                "job",
                "-o",
            ]:
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    legacy_job_record(
                        num_cpus=128,
                        allocated_cpus=128,
                        allocated_hcus=8,
                    ),
                    "",
                )
            if Path(argv[0]).name == "scontrol" and argv[1:4] == [
                "show",
                "node",
                "-o",
            ]:
                return subprocess.CompletedProcess(
                    argv, 0, legacy_node_record(argv[4]), ""
                )
            if Path(argv[0]).name == "srun":
                workload_calls.append(argv)
            # Simulates PrivateData: the occupancy query exposes only this Job.
            return allocation_control_result(argv)

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_verbs(
                context(),
                VerbsCheckConfig(),
                output_dir=Path(temp) / "legacy-private-data-partial-job",
            )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(result.reason_code, "SLURM_JOB_NOT_FULL_NODE_CAPACITY")
        self.assertIn("allocated_cpus", result.message)
        self.assertEqual(workload_calls, [])

    def test_legacy_missing_job_capacity_field_fails_closed(self):
        workload_calls = []

        def runner(argv, **kwargs):
            if Path(argv[0]).name == "scontrol" and argv[1:4] == [
                "show",
                "job",
                "-o",
            ]:
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    "JobId=674118 OverSubscribe=NO NumNodes=2 NumCPUs=256\n",
                    "",
                )
            if Path(argv[0]).name == "scontrol" and argv[1:4] == [
                "show",
                "node",
                "-o",
            ]:
                return subprocess.CompletedProcess(
                    argv, 0, legacy_node_record(argv[4]), ""
                )
            if Path(argv[0]).name == "srun":
                workload_calls.append(argv)
            return allocation_control_result(argv)

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_verbs(
                context(),
                VerbsCheckConfig(),
                output_dir=Path(temp) / "legacy-job-capacity-missing",
            )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(result.reason_code, "SLURM_JOB_CAPACITY_EVIDENCE_MISSING")
        self.assertEqual(workload_calls, [])

    def test_legacy_selected_subset_cannot_prove_full_job_ownership(self):
        workload_calls = []

        def runner(argv, **kwargs):
            if Path(argv[0]).name == "squeue" and "-j" in argv:
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    "674118|RUNNING|qianyj1|e06r1n[08-10]\n",
                    "",
                )
            if Path(argv[0]).name == "scontrol" and argv[1:3] == [
                "show", "hostnames"
            ]:
                return subprocess.CompletedProcess(argv, 0, "e06r1n08\ne06r1n09\ne06r1n10\n", "")
            if Path(argv[0]).name == "scontrol" and argv[1:4] == [
                "show",
                "job",
                "-o",
            ]:
                return subprocess.CompletedProcess(argv, 0, legacy_job_record(), "")
            if Path(argv[0]).name == "scontrol" and argv[1:4] == [
                "show",
                "node",
                "-o",
            ]:
                return subprocess.CompletedProcess(
                    argv, 0, legacy_node_record(argv[4]), ""
                )
            if Path(argv[0]).name == "srun":
                workload_calls.append(argv)
            return allocation_control_result(argv)

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_verbs(
                context(),
                VerbsCheckConfig(),
                output_dir=Path(temp) / "legacy-selected-subset",
            )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(
            result.reason_code,
            "SLURM_LEGACY_PROOF_REQUIRES_FULL_JOB_NODESET",
        )
        self.assertEqual(workload_calls, [])

    def test_legacy_oversubscribe_ok_is_not_an_exclusive_equivalent(self):
        workload_calls = []

        def runner(argv, **kwargs):
            if Path(argv[0]).name == "scontrol" and argv[1:4] == [
                "show",
                "job",
                "-o",
            ]:
                return subprocess.CompletedProcess(
                    argv, 0, "JobId=674118 OverSubscribe=OK\n", ""
                )
            if Path(argv[0]).name == "srun":
                workload_calls.append(argv)
            return allocation_control_result(argv)

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_verbs(
                context(),
                VerbsCheckConfig(),
                output_dir=Path(temp) / "legacy-oversubscribe-ok",
            )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(
            result.reason_code, "SLURM_ALLOCATION_NOT_NODE_EXCLUSIVE"
        )
        self.assertIn("OverSubscribe=OK", result.message)
        self.assertEqual(workload_calls, [])

class VerbsActiveTests(unittest.TestCase):

    def test_verbs_threshold_must_be_finite(self):
        for value in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                VerbsCheckConfig(minimum_average_gbps=value).validate()

    def test_verbs_pass_records_nodes_commands_metrics_and_evidence(self):
        workload_calls = []

        def runner(argv, **kwargs):
            if Path(argv[0]).name != "srun":
                return allocation_control_result(argv)
            workload_calls.append(argv)
            return subprocess.CompletedProcess(
                argv,
                0,
                VERBS_METADATA + "#bytes #iterations BW peak[Gb/sec] BW average[Gb/sec] MsgRate[Mpps]\n"
                "1048576 1000 92.00 91.25 0.011\n",
                "",
            )

        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "verbs"
            result = SlurmActiveCheckRunner(
                runner=runner, popen=FakePopen, sleeper=lambda _: None
            ).run_verbs(
                context(),
                VerbsCheckConfig(
                    device="shca_0", minimum_average_gbps=80.0, startup_grace_seconds=0
                ),
                output_dir=output,
            )
            self.assertEqual(result.status, "PASS")
            self.assertEqual(result.reason_code, "VERBS_END_TO_END_PASSED")
            self.assertEqual(result.backend, "VERBS")
            self.assertEqual(result.data_transport, "IB_VERBS")
            self.assertEqual(result.nodes, ["e06r1n08", "e06r1n09"])
            self.assertEqual(result.metrics["average_gbps"], 91.25)
            self.assertEqual(result.metrics["safety_boundary"], "EXCLUSIVE_SLURM_ALLOCATION_AND_STEP")
            self.assertEqual(result.metrics["allocation_exclusive_mode"], "NODE")
            self.assertEqual(result.metrics["allocation_foreign_active_job_ids"], [])
            self.assertTrue(
                result.metrics["allocation_node_exclusivity_proven"]
            )
            self.assertEqual(len(result.commands), 2)
            for command in result.commands:
                self.assertIn("--exclusive", command.argv)
                self.assertIn("--exact", command.argv)
                self.assertIn("--immediate=1", command.argv)
                self.assertIn("--export=ALL", command.argv)
                self.assertNotIn("--overlap", command.argv)
                self.assertFalse(any(item.startswith("--force-link=") for item in command.argv))
            self.assertTrue(all(Path(item.argv[0]).name == "srun" for item in result.commands))
            self.assertTrue(all(Path(item.stdout_path).is_file() for item in result.commands))
            self.assertEqual(len(workload_calls), 1)  # server is launched through FakePopen
            persisted = json.loads((output / "active-result.json").read_text())
            self.assertEqual(persisted["allocation"]["owner"], "qianyj1")
            self.assertEqual(persisted["allocation"]["exclusive_mode"], "NODE")
            self.assertEqual(
                persisted["allocation"]["foreign_active_job_ids"], []
            )
            summary = (output / "active-summary.md").read_text(encoding="utf-8")
            self.assertIn("| Verdict | PASS |", summary)
            self.assertIn("| Backend | VERBS |", summary)
            self.assertIn("| Actual transport | IB_VERBS |", summary)
            self.assertIn("01-verbs-server.stdout.txt", summary)

    def test_unsafe_overlap_collects_evidence_but_suppresses_formal_pass(self):
        def runner(argv, **kwargs):
            if Path(argv[0]).name == "squeue" and "--steps" in argv:
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    "674118.batch|e06r1n08\n674118.extern|e06r1n08\n",
                    "",
                )
            if Path(argv[0]).name != "srun":
                return allocation_control_result(argv)
            return subprocess.CompletedProcess(
                argv,
                0,
                VERBS_METADATA
                + "#bytes #iterations BW peak[Gb/sec] BW average[Gb/sec]\n"
                "1048576 1000 92.00 91.25 0.011\n",
                "",
            )

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(
                runner=runner, popen=FakePopen, sleeper=lambda _: None
            ).run_verbs(
                context(unsafe_allow_overlap=True),
                VerbsCheckConfig(device="shca_0", startup_grace_seconds=0),
                output_dir=Path(temp) / "unsafe-overlap",
            )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(result.reason_code, "OVERLAP_NOT_PROVEN_IDLE")
        self.assertEqual(result.metrics["safety_boundary"], "OVERLAP_NOT_PROVEN_IDLE")
        self.assertEqual(result.metrics["pre_safety_status"], "PASS")
        self.assertEqual(
            result.metrics["pre_safety_reason_code"], "VERBS_END_TO_END_PASSED"
        )
        self.assertEqual(result.allocation["active_steps"], ("674118.batch",))
        for command in result.commands:
            self.assertIn("--overlap", command.argv)
            self.assertNotIn("--exclusive", command.argv)
            self.assertNotIn("--exact", command.argv)

    def test_verbs_container_scope_is_explicit_docker_exec(self):
        server, client = SlurmActiveCheckRunner().build_verbs_commands(
            context(),
            VerbsCheckConfig(container_name="zytest", device="shca_0"),
        )
        for argv in (server, client):
            docker_index = argv.index("docker")
            self.assertEqual(
                argv[docker_index : docker_index + 3],
                ["docker", "exec", "zytest"],
            )
            self.assertIn("--ib-dev=shca_0", argv)
        self.assertEqual(client[-1], "e06r1n08")

    def test_verbs_link_type_mismatch_is_fail(self):
        class EthernetPopen(FakePopen):
            def communicate(self, timeout=None):
                self.returncode = 0
                return (
                    "Device : shca_0\nTransport type : IB\nLink type : Ethernet\n"
                    "#bytes #iterations BW peak[Gb/sec] BW average[Gb/sec]\n"
                    "1048576 1000 91.00 90.50 0.010\n",
                    "",
                )

        def runner(argv, **kwargs):
            if Path(argv[0]).name != "srun":
                return allocation_control_result(argv)
            return subprocess.CompletedProcess(
                argv,
                0,
                "Device : shca_0\nTransport type : IB\nLink type : Ethernet\n"
                "#bytes #iterations BW peak[Gb/sec] BW average[Gb/sec]\n"
                "1048576 1000 91.00 90.50 0.010\n",
                "",
            )

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(
                runner=runner, popen=EthernetPopen, sleeper=lambda _: None
            ).run_verbs(
                context(),
                VerbsCheckConfig(
                    protocol="ib", device="shca_0", startup_grace_seconds=0
                ),
                output_dir=Path(temp) / "verbs-link-mismatch",
            )
        self.assertEqual(result.status, "FAIL")
        self.assertEqual(result.reason_code, "VERBS_LINK_TYPE_MISMATCH")

    def test_invalid_verbs_bandwidth_values_are_not_evidence(self):
        for value in ("0", "-1", "nan", "inf", "-inf"):
            with self.subTest(value=value):
                parsed = SlurmActiveCheckRunner._parse_verbs_average_gbps(
                    "#bytes #iterations BW peak[Gb/sec] BW average[Gb/sec]\n"
                    f"1048576 1000 1.0 {value} 0.01\n"
                )
                self.assertIsNone(parsed)

    def test_zero_exit_without_bandwidth_proof_is_not_verified(self):
        class NoMetricPopen(FakePopen):
            def communicate(self, timeout=None):
                self.returncode = 0
                return ("completed\n", "")

        def runner(argv, **kwargs):
            if Path(argv[0]).name != "srun":
                return allocation_control_result(argv)
            return subprocess.CompletedProcess(argv, 0, "completed\n", "")

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(
                runner=runner, popen=NoMetricPopen, sleeper=lambda _: None
            ).run_verbs(
                context(),
                VerbsCheckConfig(startup_grace_seconds=0),
                output_dir=Path(temp) / "no-metric",
            )
            self.assertEqual(result.status, "NOT_VERIFIED")
            self.assertEqual(result.reason_code, "VERBS_METRIC_EVIDENCE_MISSING")
            self.assertIsNone(result.data_transport)

    def test_roce_requires_gid_index(self):
        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner().run_verbs(
                context(),
                VerbsCheckConfig(protocol="roce"),
                output_dir=Path(temp) / "roce-invalid",
            )
            self.assertEqual(result.status, "NOT_VERIFIED")
            self.assertEqual(result.reason_code, "INVALID_ACTIVE_CHECK_CONFIGURATION")


class RcclActiveTests(unittest.TestCase):

    def test_rccl_threshold_must_be_finite(self):
        threshold_fields = (
            "minimum_average_busbw_gbytes_per_second",
            "minimum_algbw_gbytes_per_second",
            "minimum_busbw_gbytes_per_second",
        )
        for field_name in threshold_fields:
            for value in (0.0, -1.0, float("nan"), float("inf"), float("-inf")):
                with self.subTest(field=field_name, value=value), self.assertRaises(
                    ValueError
                ):
                    RcclCheckConfig(
                        binary="all_reduce_perf",
                        **{field_name: value},
                    ).validate()


    def test_summary_only_output_can_never_pass(self):
        def runner(argv, **kwargs):
            if Path(argv[0]).name != "srun":
                return allocation_control_result(argv)
            return subprocess.CompletedProcess(
                argv,
                0,
                "# Out of bounds values : 0 OK\n# Avg bus bandwidth : 84.2\n",
                "NCCL INFO Using network IBext_v8\n"
                "NCCL INFO ncclCommInitRank rank 0 nranks 2\n",
            )

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_rccl(
                context(),
                RcclCheckConfig(binary="all_reduce_perf"),
                output_dir=Path(temp) / "summary-only",
            )

        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(result.reason_code, "RCCL_OUTPUT_VALIDATION_FAILED")
        self.assertFalse(result.metrics["strict_validation_valid"])
        self.assertIn(
            "TABLE_ROWS_MISSING",
            result.metrics["strict_validation_issue_codes"],
        )


    def test_single_socket_rank_causes_mixed_transport_failure(self):
        payload = strict_rccl_output().replace(
            "Using network IBext_v8",
            "Using network Socket_v8",
            1,
        )

        def runner(argv, **kwargs):
            if Path(argv[0]).name != "srun":
                return allocation_control_result(argv)
            return subprocess.CompletedProcess(argv, 0, payload, "")

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_rccl(
                context(),
                RcclCheckConfig(binary="all_reduce_perf"),
                output_dir=Path(temp) / "one-socket-rank",
            )

        self.assertEqual(result.status, "FAIL")
        self.assertEqual(result.reason_code, "RCCL_USED_SOCKET_TRANSPORT")
        self.assertEqual(result.data_transport, "MIXED")

    def test_wrong_or_nonzero_oob_is_data_corruption_failure(self):
        cases = (
            ("wrong", strict_rccl_output(wrong="1"), "WRONG_VALUE_NONZERO"),
            (
                "oob",
                strict_rccl_output().replace(
                    "# Out of bounds values : 0 OK",
                    "# Out of bounds values : 2",
                ),
                "SUMMARY_OUT_OF_BOUNDS_NONZERO",
            ),
        )
        for name, payload, issue_code in cases:
            def runner(argv, **kwargs):
                if Path(argv[0]).name != "srun":
                    return allocation_control_result(argv)
                return subprocess.CompletedProcess(argv, 0, payload, "")

            with self.subTest(name=name), tempfile.TemporaryDirectory() as temp:
                result = SlurmActiveCheckRunner(runner=runner).run_rccl(
                    context(),
                    RcclCheckConfig(binary="all_reduce_perf"),
                    output_dir=Path(temp) / name,
                )

            self.assertEqual(result.status, "FAIL")
            self.assertEqual(
                result.reason_code,
                "RCCL_DATA_CORRUPTION_DETECTED",
            )
            self.assertIn(
                issue_code,
                result.metrics["strict_validation_issue_codes"],
            )
    def test_p1xg8_and_p8xg1_require_all_sixteen_ranks(self):
        cases = (("p1xg8", 1, 8), ("p8xg1", 8, 1))
        for name, tasks_per_node, devices_per_task in cases:
            commands = []

            def runner(argv, **kwargs):
                if Path(argv[0]).name != "srun":
                    return allocation_control_result(argv)
                commands.append(argv)
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    strict_rccl_output(
                        tasks_per_node=tasks_per_node,
                        devices_per_task=devices_per_task,
                    ),
                    "",
                )

            with self.subTest(name=name), tempfile.TemporaryDirectory() as temp:
                result = SlurmActiveCheckRunner(runner=runner).run_rccl(
                    context(),
                    RcclCheckConfig(
                        binary="all_reduce_perf",
                        tasks_per_node=tasks_per_node,
                        devices_per_task=devices_per_task,
                    ),
                    output_dir=Path(temp) / name,
                )

            self.assertEqual(result.status, "PASS")
            self.assertEqual(result.metrics["expected_nranks"], 16)
            self.assertEqual(len(result.metrics["strict_rccl"]["devices"]), 16)
            self.assertIn(f"--ntasks={2 * tasks_per_node}", commands[0])
            self.assertIn(f"--ntasks-per-node={tasks_per_node}", commands[0])
            self.assertEqual(
                commands[0][commands[0].index("-g") + 1],
                str(devices_per_task),
            )

    def test_missing_device_assignment_is_strictly_not_verified(self):
        def runner(argv, **kwargs):
            if Path(argv[0]).name != "srun":
                return allocation_control_result(argv)
            return subprocess.CompletedProcess(
                argv,
                0,
                strict_rccl_output(omit_device_ranks=(1,)),
                "",
            )

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_rccl(
                context(),
                RcclCheckConfig(binary="all_reduce_perf"),
                output_dir=Path(temp) / "missing-device-rank",
            )

        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(result.reason_code, "RCCL_OUTPUT_VALIDATION_FAILED")
        self.assertIn(
            "DEVICE_RANK_MISSING",
            result.metrics["strict_validation_issue_codes"],
        )

    def test_gdr_requirement_distinguishes_disabled_and_selected_gdrrdma(self):
        for gdr, expected_status, expected_reason in (
            ("DISABLED", "FAIL", "RCCL_GDR_REQUIRED_BUT_DISABLED"),
            ("ENABLED", "PASS", "RCCL_MULTI_NODE_RDMA_PASSED"),
        ):
            def runner(argv, **kwargs):
                if Path(argv[0]).name != "srun":
                    return allocation_control_result(argv)
                return subprocess.CompletedProcess(
                    argv, 0, strict_rccl_output(gdr=gdr), ""
                )

            with self.subTest(gdr=gdr), tempfile.TemporaryDirectory() as temp:
                result = SlurmActiveCheckRunner(runner=runner).run_rccl(
                    context(),
                    RcclCheckConfig(binary="all_reduce_perf", require_gdr=True),
                    output_dir=Path(temp) / f"gdr-{gdr.lower()}",
                )

            self.assertEqual(result.status, expected_status)
            self.assertEqual(result.reason_code, expected_reason)
            self.assertEqual(result.metrics["gpudirect_status"], gdr)

    def test_performance_threshold_missing_failed_and_passed_are_separate(self):
        cases = (
            (
                "missing",
                strict_rccl_output().replace("# Avg bus bandwidth : 84.2\n", ""),
                {"minimum_average_busbw_gbytes_per_second": 80.0},
                "NOT_VERIFIED",
                "RCCL_BANDWIDTH_EVIDENCE_MISSING",
                "NOT_VERIFIED",
            ),
            (
                "failed",
                strict_rccl_output(algbw=84.0),
                {"minimum_algbw_gbytes_per_second": 85.0},
                "FAIL",
                "RCCL_BANDWIDTH_BELOW_THRESHOLD",
                "FAIL",
            ),
            (
                "passed",
                strict_rccl_output(algbw=84.0, busbw=84.0, average_busbw=84.2),
                {
                    "minimum_average_busbw_gbytes_per_second": 80.0,
                    "minimum_algbw_gbytes_per_second": 83.0,
                    "minimum_busbw_gbytes_per_second": 83.0,
                },
                "PASS",
                "RCCL_MULTI_NODE_RDMA_PASSED",
                "PASS",
            ),
        )
        for name, payload, thresholds, status, reason, performance in cases:
            def runner(argv, **kwargs):
                if Path(argv[0]).name != "srun":
                    return allocation_control_result(argv)
                return subprocess.CompletedProcess(argv, 0, payload, "")

            with self.subTest(name=name), tempfile.TemporaryDirectory() as temp:
                result = SlurmActiveCheckRunner(runner=runner).run_rccl(
                    context(),
                    RcclCheckConfig(binary="all_reduce_perf", **thresholds),
                    output_dir=Path(temp) / name,
                )

            self.assertEqual(result.status, status)
            self.assertEqual(result.reason_code, reason)
            self.assertEqual(result.metrics["performance_status"], performance)

    def test_no_threshold_is_functional_pass_but_performance_not_verified(self):
        def runner(argv, **kwargs):
            if Path(argv[0]).name != "srun":
                return allocation_control_result(argv)
            return subprocess.CompletedProcess(argv, 0, strict_rccl_output(), "")

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_rccl(
                context(),
                RcclCheckConfig(binary="all_reduce_perf"),
                output_dir=Path(temp) / "no-threshold",
            )

        self.assertEqual(result.status, "PASS")
        self.assertEqual(result.metrics["performance_status"], "NOT_VERIFIED")
        self.assertIn("performance NOT_VERIFIED", result.message)

    def test_dmabuf_is_allowlisted_only_when_explicitly_requested(self):
        argv = SlurmActiveCheckRunner().build_rccl_command(
            context(),
            RcclCheckConfig(
                binary="all_reduce_perf",
                environment={"NCCL_DMABUF_ENABLE": "1"},
            ),
        )
        self.assertIn("NCCL_DMABUF_ENABLE=1", argv)
        default_argv = SlurmActiveCheckRunner().build_rccl_command(
            context(), RcclCheckConfig(binary="all_reduce_perf")
        )
        self.assertNotIn("NCCL_DMABUF_ENABLE=1", default_argv)
    def test_rccl_rdma_pass_proves_multi_node_backend(self):
        commands = []

        def runner(argv, **kwargs):
            if Path(argv[0]).name != "srun":
                return allocation_control_result(argv)
            commands.append(argv)
            return subprocess.CompletedProcess(
                argv,
                0,
                strict_rccl_output(),
                "",
            )

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_rccl(
                context(),
                RcclCheckConfig(
                    binary="/opt/rccl-tests/all_reduce_perf",
                    minimum_average_busbw_gbytes_per_second=80.0,
                ),
                output_dir=Path(temp) / "rccl-pass",
            )
            self.assertEqual(result.status, "PASS")
            self.assertEqual(result.reason_code, "RCCL_MULTI_NODE_RDMA_PASSED")
            self.assertEqual(result.backend, "RCCL")
            self.assertEqual(result.data_transport, "RDMA")
            self.assertEqual(result.metrics["maximum_reported_nranks"], 2)
            self.assertIn("--nodes=2", commands[0])
            self.assertIn("--ntasks=2", commands[0])
            self.assertIn("NCCL_DEBUG_SUBSYS=INIT,NET", commands[0])
            self.assertEqual(Path(commands[0][0]).name, "srun")

    def test_rccl_rank_evidence_covers_devices_per_task(self):
        def runner(argv, **kwargs):
            if Path(argv[0]).name != "srun":
                return allocation_control_result(argv)
            return subprocess.CompletedProcess(
                argv,
                0,
                strict_rccl_output(devices_per_task=2, omit_transport_ranks=(3,)),
                "",
            )

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_rccl(
                context(),
                RcclCheckConfig(binary="all_reduce_perf", devices_per_task=2),
                output_dir=Path(temp) / "rccl-rank-coverage",
            )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(result.reason_code, "RCCL_RDMA_TRANSPORT_EVIDENCE_MISSING")
        self.assertEqual(result.metrics["expected_nranks"], 4)

    def test_rccl_socket_fallback_is_fail(self):
        def runner(argv, **kwargs):
            if Path(argv[0]).name != "srun":
                return allocation_control_result(argv)
            return subprocess.CompletedProcess(
                argv,
                0,
                strict_rccl_output(
                    transport="Socket_v8",
                    average_busbw=4.2,
                    extra="NCCL INFO NET/Plugin: Could not find librccl-net.so\n"
                    "NCCL INFO NET/IB : No device found",
                ),
                "",
            )

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_rccl(
                context(),
                RcclCheckConfig(binary="all_reduce_perf"),
                output_dir=Path(temp) / "rccl-socket",
            )
            self.assertEqual(result.status, "FAIL")
            self.assertEqual(result.reason_code, "RCCL_USED_SOCKET_TRANSPORT")
            self.assertEqual(result.data_transport, "SOCKET")
            self.assertEqual(
                result.root_cause_candidates,
                [
                    "RCCL_NET_PLUGIN_MISSING",
                    "RCCL_RDMA_DEVICE_NOT_DISCOVERED",
                    "RCCL_USED_SOCKET_TRANSPORT",
                ],
            )
            self.assertTrue(
                {
                    "NET_PLUGIN_LOAD_FAILED",
                    "NET_IB_NO_DEVICE_FOUND",
                    "NET_SOCKET_DATA_PATH_SELECTED",
                }.issubset(set(result.evidence_markers))
            )
            summary = (Path(result.evidence_dir) / "active-summary.md").read_text(
                encoding="utf-8"
            )
            self.assertIn("RCCL_RDMA_DEVICE_NOT_DISCOVERED", summary)
            self.assertIn("| Actual transport | SOCKET |", summary)

    def test_versioned_socket_selection_cannot_receive_rccl_pass(self):
        def runner(argv, **kwargs):
            if Path(argv[0]).name != "srun":
                return allocation_control_result(argv)
            return subprocess.CompletedProcess(
                argv,
                0,
                strict_rccl_output(transport="Socket_v8", average_busbw=4.2),
                "",
            )

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_rccl(
                context(),
                RcclCheckConfig(binary="all_reduce_perf"),
                output_dir=Path(temp) / "rccl-versioned-socket",
            )
        self.assertEqual(result.status, "FAIL")
        self.assertEqual(result.reason_code, "RCCL_USED_SOCKET_TRANSPORT")
        self.assertEqual(result.data_transport, "SOCKET")

    def test_rccl_zero_exit_without_rdma_log_is_not_verified(self):
        def runner(argv, **kwargs):
            if Path(argv[0]).name != "srun":
                return allocation_control_result(argv)
            return subprocess.CompletedProcess(
                argv,
                0,
                strict_rccl_output(omit_transport_ranks=(1,)),
                "",
            )

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_rccl(
                context(),
                RcclCheckConfig(binary="all_reduce_perf"),
                output_dir=Path(temp) / "rccl-unknown",
            )
            self.assertEqual(result.status, "NOT_VERIFIED")
            self.assertEqual(result.reason_code, "RCCL_RDMA_TRANSPORT_EVIDENCE_MISSING")

    def test_no_device_found_is_negative_not_rdma_transport_evidence(self):
        metrics = SlurmActiveCheckRunner._parse_rccl(
            "NCCL INFO NET/IB : No device found\n"
        )
        self.assertEqual(metrics["data_transport"], "UNKNOWN")
        self.assertTrue(metrics["rdma_device_discovery_failed"])
        self.assertIn(
            "RCCL_RDMA_DEVICE_NOT_DISCOVERED", metrics["root_cause_candidates"]
        )
        self.assertNotIn("NET_IB_DATA_PATH_SELECTED", metrics["evidence_markers"])

    def test_any_nonzero_rccl_correctness_value_cannot_be_hidden_by_later_zero(self):
        metrics = SlurmActiveCheckRunner._parse_rccl(
            "Out of bounds values : 2\nOut of bounds values : 0 OK\n"
        )
        self.assertEqual(metrics["out_of_bounds_values"], 2)
        self.assertEqual(metrics["reported_out_of_bounds_values"], [2, 0])

    def test_conflicting_rdma_selection_and_no_device_never_passes(self):
        def runner(argv, **kwargs):
            if Path(argv[0]).name != "srun":
                return allocation_control_result(argv)
            return subprocess.CompletedProcess(
                argv,
                0,
                strict_rccl_output(
                    extra="NCCL INFO NET/IB : No device found",
                ),
                "",
            )

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_rccl(
                context(),
                RcclCheckConfig(binary="all_reduce_perf"),
                output_dir=Path(temp) / "rccl-conflict",
            )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(result.reason_code, "RCCL_RDMA_EVIDENCE_CONFLICTING")
        self.assertIn("NET_IB_EVIDENCE_CONFLICTING", result.evidence_markers)

    def test_rccl_parser_marks_mixed_socket_and_rdma_transport(self):
        metrics = SlurmActiveCheckRunner._parse_rccl(
            "# Avg bus bandwidth : 84.20\n"
            "# Out of bounds values : 0 OK\n"
            "NCCL INFO [nranks=2] NET/IB : Using mlx5_0\n"
            "NCCL INFO NET/Socket : Using eth0\n"
        )
        self.assertEqual(metrics["data_transport"], "MIXED")

    def test_versioned_network_plugins_are_classified_without_false_rdma(self):
        rdma = SlurmActiveCheckRunner._parse_rccl(
            "NCCL INFO Using network IBext_v8\n"
        )
        socket = SlurmActiveCheckRunner._parse_rccl(
            "NCCL INFO Using network Socket_v8\n"
        )
        mixed = SlurmActiveCheckRunner._parse_rccl(
            "NCCL INFO NET/IB : Using shca_0\n"
            "NCCL INFO Using network Socket_v8\n"
        )
        via_mixed = SlurmActiveCheckRunner._parse_rccl(
            "via NET/IBext_v8/0\nvia NET/Socket_v8/0\n"
        )
        self.assertEqual(rdma["data_transport"], "RDMA")
        self.assertEqual(socket["data_transport"], "SOCKET")
        self.assertEqual(mixed["data_transport"], "MIXED")
        self.assertEqual(via_mixed["data_transport"], "MIXED")
        self.assertEqual(rdma["selected_network_plugins"], ["IBext_v8"])
        self.assertIn("NET_SOCKET_DATA_PATH_SELECTED", mixed["evidence_markers"])

    def test_rccl_docker_command_executes_inside_explicit_container(self):
        argv = SlurmActiveCheckRunner().build_rccl_command(
            context(),
            RcclCheckConfig(
                binary="/opt/rccl-tests/all_reduce_perf",
                container_name="zytest",
                environment={"NCCL_IB_HCA": "mlx5_0"},
            ),
        )
        docker_index = argv.index("docker")
        self.assertEqual(Path(argv[0]).name, "srun")
        self.assertEqual(argv[docker_index : docker_index + 2], ["docker", "exec"])
        self.assertIn("NCCL_DEBUG=INFO", argv)
        self.assertIn("NCCL_DEBUG_SUBSYS=INIT,NET", argv)
        self.assertIn("NCCL_IB_HCA=mlx5_0", argv)
        self.assertLess(argv.index("zytest"), argv.index("/opt/rccl-tests/all_reduce_perf"))

    def test_rccl_docker_container_name_is_validated(self):
        with self.assertRaises(ValueError):
            RcclCheckConfig(
                binary="all_reduce_perf", container_name="bad;container"
            ).validate()

    def test_rccl_docker_exec_is_fail_closed_before_slurm_or_workload(self):
        calls = []

        def runner(argv, **kwargs):
            calls.append(argv)
            raise AssertionError("unsupported rccl docker mode must not run anything")

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_rccl(
                context(),
                RcclCheckConfig(binary="all_reduce_perf", container_name="zytest"),
                output_dir=Path(temp) / "unsupported-container",
            )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(
            result.reason_code, "RCCL_DOCKER_MPI_LAUNCH_UNSUPPORTED"
        )
        self.assertEqual(result.container_name, "zytest")
        self.assertIn("torch-rccl", result.message)
        self.assertEqual(result.commands, [])
        self.assertEqual(calls, [])

    def test_rccl_environment_is_allowlisted(self):
        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner().run_rccl(
                context(),
                RcclCheckConfig(binary="all_reduce_perf", environment={"SECRET": "x"}),
                output_dir=Path(temp) / "rccl-env",
            )
            self.assertEqual(result.status, "NOT_VERIFIED")
            self.assertEqual(result.reason_code, "INVALID_ACTIVE_CHECK_CONFIGURATION")

    def test_truncated_rccl_output_suppresses_pass_and_records_byte_counts(self):
        payload = (
            "# Avg bus bandwidth : 84.20\n# Out of bounds values : 0 OK\n"
            + ("x" * (8 * 1024 * 1024 + 1024))
        )

        def runner(argv, **kwargs):
            if Path(argv[0]).name != "srun":
                return allocation_control_result(argv)
            return subprocess.CompletedProcess(
                argv,
                0,
                payload,
                "NCCL INFO comm rank 0 [nranks=2]\n"
                "NCCL INFO NET/IB : Using [0]shca_0:1\n",
            )

        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "rccl-truncated"
            result = SlurmActiveCheckRunner(runner=runner).run_rccl(
                context(),
                RcclCheckConfig(binary="all_reduce_perf"),
                output_dir=output,
            )
            self.assertEqual(result.status, "NOT_VERIFIED")
            self.assertEqual(result.reason_code, "ACTIVE_OUTPUT_TRUNCATED")
            self.assertTrue(result.metrics["output_truncated"])
            self.assertTrue(result.commands[0].stdout_truncated)
            self.assertGreater(result.commands[0].stdout_total_bytes, 8 * 1024 * 1024)
            persisted = (output / "01-rccl-all-reduce.stdout.txt").read_text(
                encoding="utf-8"
            )
            self.assertIn("HCU_ENVCHECK omitted", persisted)
            self.assertLess(len(persisted.encode("utf-8")), 8 * 1024 * 1024 + 256)

    def test_library_path_override_is_explicitly_marked_as_test_runtime(self):
        def runner(argv, **kwargs):
            if Path(argv[0]).name != "srun":
                return allocation_control_result(argv)
            return subprocess.CompletedProcess(
                argv,
                0,
                strict_rccl_output(),
                "",
            )

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_rccl(
                context(),
                RcclCheckConfig(
                    binary="all_reduce_perf",
                    environment={"LD_LIBRARY_PATH": "/tmp/ab-provider"},
                ),
                output_dir=Path(temp) / "rccl-runtime-override",
            )
        self.assertEqual(result.status, "PASS")
        self.assertTrue(result.metrics["runtime_modified_for_test"])
        self.assertIn("RUNTIME_LIBRARY_PATH_OVERRIDDEN", result.evidence_markers)

    def test_ambient_runtime_environment_is_recorded_separately(self):
        def runner(argv, **kwargs):
            if Path(argv[0]).name != "srun":
                return allocation_control_result(argv)
            return subprocess.CompletedProcess(
                argv,
                0,
                strict_rccl_output(),
                "",
            )

        with patch.dict("os.environ", {"LD_LIBRARY_PATH": "/ambient/provider"}), tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_rccl(
                context(),
                RcclCheckConfig(binary="all_reduce_perf"),
                output_dir=Path(temp) / "rccl-ambient-runtime",
            )
        self.assertEqual(result.status, "PASS")
        self.assertFalse(result.metrics["runtime_modified_for_test"])
        self.assertTrue(result.metrics["runtime_environment_modified"])
        self.assertEqual(
            result.metrics["ambient_runtime_environment"]["LD_LIBRARY_PATH"],
            "/ambient/provider",
        )
        self.assertIn(
            "AMBIENT_RUNTIME_ENVIRONMENT_INHERITED", result.evidence_markers
        )

class TorchRcclActiveTests(unittest.TestCase):
    @staticmethod

    def markers(*, rank1_value=3.0, rank1_correct=True):
        return (
            'HCU_ENVCHECK_TORCH_RCCL {"correct":true,"expected":3.0,'
            '"node":"e06r1n08","rank":0,"value":3.0,"world":2}\n'
            'HCU_ENVCHECK_TORCH_RCCL {"correct":'
            + ("true" if rank1_correct else "false")
            + ',"expected":3.0,"node":"e06r1n09","rank":1,"value":'
            + str(rank1_value)
            + ',"world":2}\n'
        )

    def test_host_command_is_one_srun_task_per_node_and_builtin_python(self):
        argv = SlurmActiveCheckRunner().build_torch_rccl_command(
            context(),
            TorchRcclCheckConfig(
                python_binary="/usr/bin/python3",
                master_port=29601,
                environment={"NCCL_IB_HCA": "shca_0:1"},
            ),
        )
        self.assertEqual(Path(argv[0]).name, "srun")
        self.assertIn("--nodes=2", argv)
        self.assertIn("--ntasks=2", argv)
        self.assertIn("--ntasks-per-node=1", argv)
        self.assertIn("MASTER_ADDR=e06r1n08", argv)
        self.assertIn("MASTER_PORT=29601", argv)
        self.assertIn("NCCL_IB_HCA=shca_0:1", argv)
        self.assertIn("/usr/bin/python3", argv)
        python_index = argv.index("/usr/bin/python3")
        self.assertEqual(argv[python_index + 1], "-c")
        self.assertIn("HCU_ENVCHECK_TORCH_RCCL", argv[python_index + 2])
        self.assertNotIn("bash", [Path(item).name for item in argv])

    def test_container_command_forwards_slurm_rank_without_interpolation(self):
        argv = SlurmActiveCheckRunner().build_torch_rccl_command(
            context(),
            TorchRcclCheckConfig(container_name="zytest", python_binary="python3"),
        )
        docker_index = argv.index("docker")
        self.assertEqual(argv[docker_index : docker_index + 2], ["docker", "exec"])
        for name in (
            "SLURM_PROCID",
            "SLURM_NTASKS",
            "SLURM_LOCALID",
            "SLURMD_NODENAME",
        ):
            index = argv.index(name)
            self.assertEqual(argv[index - 1], "--env")
            self.assertNotIn(f"{name}=", argv)
        self.assertLess(argv.index("zytest"), argv.index("python3"))

    def test_two_rank_rdma_pass_with_gpudirect_disabled(self):
        def runner(argv, **kwargs):
            if Path(argv[0]).name != "srun":
                return allocation_control_result(argv)
            return subprocess.CompletedProcess(
                argv,
                0,
                self.markers(),
                "NCCL INFO NET/IB : Using [0]shca_0:1\n"
                "NCCL INFO GPU Direct RDMA Disabled\n",
            )

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_torch_rccl(
                context(),
                TorchRcclCheckConfig(container_name="zytest"),
                output_dir=Path(temp) / "torch-rdma",
            )
        self.assertEqual(result.status, "PASS")
        self.assertEqual(result.reason_code, "TORCH_RCCL_MULTI_NODE_RDMA_PASSED")
        self.assertEqual(result.data_transport, "RDMA")
        self.assertEqual(result.metrics["gpudirect_status"], "DISABLED")
        self.assertEqual(result.metrics["collective_correctness"], "PASS")
        self.assertIn("GPU_DIRECT_RDMA_DISABLED", result.evidence_markers)
        self.assertNotIn("RCCL_GPU_DIRECT_RDMA_DISABLED", result.root_cause_candidates)

    def test_socket_transport_is_fail_even_when_collective_is_correct(self):
        def runner(argv, **kwargs):
            if Path(argv[0]).name != "srun":
                return allocation_control_result(argv)
            return subprocess.CompletedProcess(
                argv, 0, self.markers(), "NCCL INFO Using network Socket\n"
            )

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_torch_rccl(
                context(),
                TorchRcclCheckConfig(),
                output_dir=Path(temp) / "torch-socket",
            )
        self.assertEqual(result.status, "FAIL")
        self.assertEqual(result.reason_code, "RCCL_USED_SOCKET_TRANSPORT")
        self.assertEqual(result.data_transport, "SOCKET")
        self.assertIn("RCCL_USED_SOCKET_TRANSPORT", result.root_cause_candidates)

    def test_missing_rank_marker_is_not_verified(self):
        def runner(argv, **kwargs):
            if Path(argv[0]).name != "srun":
                return allocation_control_result(argv)
            one_rank = self.markers().splitlines()[0] + "\n"
            return subprocess.CompletedProcess(
                argv, 0, one_rank, "NCCL INFO NET/IB : Using shca_0:1\n"
            )

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_torch_rccl(
                context(),
                TorchRcclCheckConfig(),
                output_dir=Path(temp) / "torch-missing-rank",
            )
        self.assertEqual(result.status, "NOT_VERIFIED")
        self.assertEqual(
            result.reason_code, "TORCH_RCCL_MULTI_NODE_EVIDENCE_MISSING"
        )
        self.assertEqual(result.metrics["missing_ranks"], [1])
        self.assertEqual(result.metrics["missing_nodes"], ["e06r1n09"])

    def test_wrong_collective_value_is_data_corruption_fail(self):
        def runner(argv, **kwargs):
            if Path(argv[0]).name != "srun":
                return allocation_control_result(argv)
            return subprocess.CompletedProcess(
                argv,
                0,
                self.markers(rank1_value=2.0, rank1_correct=False),
                "NCCL INFO NET/IB : Using shca_0:1\n",
            )

        with tempfile.TemporaryDirectory() as temp:
            result = SlurmActiveCheckRunner(runner=runner).run_torch_rccl(
                context(),
                TorchRcclCheckConfig(),
                output_dir=Path(temp) / "torch-bad-value",
            )
        self.assertEqual(result.status, "FAIL")
        self.assertEqual(result.reason_code, "RCCL_DATA_CORRUPTION_DETECTED")
        self.assertEqual(result.metrics["collective_correctness"], "FAIL")
        self.assertIn("RCCL_COLLECTIVE_DATA_MISMATCH", result.root_cause_candidates)

    def test_gpudirect_parser_distinguishes_enabled_disabled_and_unknown(self):
        parser = SlurmActiveCheckRunner._parse_gpudirect_status
        self.assertEqual(parser("GPU Direct RDMA Disabled")[0], "DISABLED")
        self.assertEqual(parser("GPU Direct RDMA Enabled")[0], "ENABLED")
        self.assertEqual(parser("no GDR evidence")[0], "UNKNOWN")
        selected = parser(
            "GPU Direct RDMA Disabled for candidate HCA 1\n"
            "GPU Direct RDMA Enabled for candidate HCA 0\n"
            "Channel 00 via NET/IBext_v8/0/GDRDMA\n"
        )
        self.assertEqual(selected[0], "ENABLED")
        self.assertEqual(selected[1], ["GPU_DIRECT_RDMA_DATA_PATH_SELECTED"])

    def test_python_basename_master_port_and_container_are_strictly_validated(self):
        with self.assertRaises(ValueError):
            TorchRcclCheckConfig(python_binary="python3;sh").validate()
        with self.assertRaises(ValueError):
            TorchRcclCheckConfig(master_port=22).validate()
        with self.assertRaises(ValueError):
            TorchRcclCheckConfig(container_name="zytest;other").validate()

    def test_cli_accepts_torch_rccl_backend_options(self):
        from hcu_envcheck.cli import build_parser

        args = build_parser(prog="hcu-envcheck").parse_args(
            [
                "active-rdma-slurm",
                "--node",
                "e06r1n08",
                "--node",
                "e06r1n09",
                "--slurm-job-id",
                "674118",
                "--backend",
                "torch-rccl",
                "--enable-active-checks",
                "--confirm-allocation-idle",
                "--container-name",
                "zytest",
                "--python-binary",
                "python3.10",
                "--master-port",
                "29601",
                "--output-dir",
                "result",
            ]
        )
        self.assertEqual(args.backend, "torch-rccl")
        self.assertEqual(args.python_binary, "python3.10")
        self.assertEqual(args.master_port, 29601)

if __name__ == "__main__":
    unittest.main()
