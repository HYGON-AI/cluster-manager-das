# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import argparse
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from . import __version__
from .active_rdma import (
    RcclCheckConfig,
    SlurmActiveCheckRunner,
    SlurmActiveContext,
    TorchRcclCheckConfig,
    VerbsCheckConfig,
)
from .baremetal import BaremetalExecutionConfig, parse_nodes_file
from .cluster_checks import (
    ClusterExtraCheckConfig,
    IBStateCheckConfig,
    IBWriteBandwidthConfig,
    NHCCheckConfig,
)
from .conda_mode import validate_environment_selection
from .k8s import KubectlError, KubernetesPodExecutor, KubernetesPodTarget
from .k8s_cluster import (
    DEFAULT_K8S_CLUSTER_CONCURRENCY,
    DEFAULT_KUBECTL_API_BURST,
    DEFAULT_KUBECTL_API_QPS,
    MAX_K8S_CLUSTER_CONCURRENCY,
    MAX_KUBECTL_API_BURST,
    MAX_KUBECTL_API_QPS,
    parse_probe_env,
    parse_reuse_pods,
    run_k8s_cluster_preflight,
)
from .ib_fabric import (
    IBFabricCheckConfig,
    SlurmIBFabricRunner,
    write_ib_fabric_reports,
)
from .output import claim_output_directory, require_new_output_path, validate_output_layout
from .preflight import run_k8s_hcu_preflight, save_result
from .rdma_policy import load_roce_policy
from .slurm_cluster import (
    BaremetalPreflightPolicy,
    collect_slurm_node_states,
    resolve_baremetal_nodes,
    run_baremetal_cluster_preflight,
)


EXIT_CODES = {"READY": 0, "BLOCKED": 1, "INCOMPLETE": 2}
ACTIVE_EXIT_CODES = {"PASS": 0, "FAIL": 1, "NOT_VERIFIED": 2}
FABRIC_EXIT_CODES = {"PASS": 0, "WARN": 1, "FAIL": 1, "NOT_VERIFIED": 2}


class ToolArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        self.print_usage(sys.stderr)
        self.exit(3, f"{self.prog}: error: {message}\n")


def _metric(value: float | None, suffix: str = "") -> str:
    return "-" if value is None else f"{value:.1f}{suffix}"


def print_summary(result) -> None:
    target = result.target
    print(f"RESULT        {result.status}")
    print(
        "TARGET        "
        f"k8s {target.get('namespace')}/{target.get('pod')} "
        f"container={target.get('container')} node={target.get('node')}"
    )
    expected = result.expected_device_count if result.expected_device_count is not None else "unspecified"
    print(f"DEVICES       {result.device_count}/{expected}")
    print(
        "THRESHOLDS    "
        f"VRAM<={result.thresholds['max_vram_used_percent']:.1f}% "
        f"HCU<={result.thresholds['max_hcu_util_percent']:.1f}% "
        f"quorum={int(result.thresholds['busy_sample_quorum'])}"
    )
    print()
    print("CARD  BDF           MODEL  TOTAL_MiB  USED_MiB  AVAILABLE_MiB  VRAM%  HCU%  STATUS")
    for device in result.devices:
        print(
            f"{device.device_id:<5} "
            f"{(device.bdf or '-'):<13} "
            f"{(device.model or '-'):<6} "
            f"{_metric(device.hy_smi_total_mib):>9} "
            f"{_metric(device.used_mib):>9} "
            f"{_metric(device.available_mib):>9} "
            f"{_metric(device.memory_used_percent, '%'):>6} "
            f"{_metric(device.hcu_util_percent, '%'):>5} "
            f"{device.status}"
        )
    if result.findings:
        print("\nFINDINGS")
        for finding in result.findings:
            scope = f"card{finding.device_id}" if finding.device_id is not None else "target"
            print(f"[{finding.severity}] {scope} {finding.reason_code}: {finding.message}")
    print(f"\nJSON          {getattr(result, '_output_path', '-')}")
    print(f"EVIDENCE      {result.evidence_dir or '-'}")


def _parse_active_environment(values: list[str]) -> dict[str, str]:
    environment: dict[str, str] = {}
    for item in values:
        if "=" not in item:
            raise ValueError(f"--rccl-env must be NAME=VALUE, got {item!r}")
        name, value = item.split("=", 1)
        if not name:
            raise ValueError("--rccl-env variable name cannot be empty")
        if name in environment:
            raise ValueError(f"duplicate --rccl-env variable: {name}")
        environment[name] = value
    return environment


def _run_active_rdma_slurm(args, parser: argparse.ArgumentParser) -> int:
    try:
        nodes = list(args.node) if args.node is not None else parse_nodes_file(args.nodes_file)
        context = SlurmActiveContext(
            job_id=args.slurm_job_id,
            selected_nodes=tuple(nodes),
            enabled=args.enable_active_checks,
            confirm_allocation_idle=args.confirm_allocation_idle,
            unsafe_allow_overlap=args.unsafe_allow_overlap,
            control_timeout_seconds=args.control_timeout,
        )
        context.validate()
        if args.backend == "verbs":
            if len(nodes) != 2:
                raise ValueError("verbs backend requires exactly two explicit nodes")
            config = VerbsCheckConfig(
                tool=args.verbs_tool,
                protocol=args.rdma_protocol,
                device=args.verbs_hca,
                container_name=args.container_name,
                ib_port=args.verbs_port,
                gid_index=args.verbs_gid_index,
                control_port=args.verbs_control_port,
                message_bytes=args.verbs_message_bytes,
                iterations=args.verbs_iterations,
                minimum_average_gbps=args.minimum_verbs_gbps,
                startup_grace_seconds=args.verbs_startup_grace,
                command_timeout_seconds=args.command_timeout,
            )
            config.validate()
            result = SlurmActiveCheckRunner().run_verbs(
                context, config, output_dir=args.output_dir
            )
        elif args.backend == "rccl":
            config = RcclCheckConfig(
                binary=args.rccl_binary,
                tasks_per_node=args.rccl_tasks_per_node,
                devices_per_task=args.rccl_devices_per_task,
                mpi_mode=args.rccl_mpi_mode,
                container_name=args.container_name,
                minimum_bytes=args.rccl_minimum_bytes,
                maximum_bytes=args.rccl_maximum_bytes,
                step_factor=args.rccl_step_factor,
                warmup_iterations=args.rccl_warmup_iterations,
                iterations=args.rccl_iterations,
                minimum_average_busbw_gbytes_per_second=args.minimum_rccl_busbw_gbytes_per_second,
                minimum_algbw_gbytes_per_second=args.minimum_rccl_algbw_gbytes_per_second,
                minimum_busbw_gbytes_per_second=args.minimum_rccl_row_busbw_gbytes_per_second,
                require_gdr=args.require_rccl_gdr,
                environment=_parse_active_environment(args.rccl_env),
                command_timeout_seconds=args.command_timeout,
            )
            config.validate()
            result = SlurmActiveCheckRunner().run_rccl(
                context, config, output_dir=args.output_dir
            )
        else:
            config = TorchRcclCheckConfig(
                python_binary=args.python_binary,
                container_name=args.container_name,
                master_port=args.master_port,
                environment=_parse_active_environment(args.rccl_env),
                command_timeout_seconds=args.command_timeout,
            )
            config.validate()
            result = SlurmActiveCheckRunner().run_torch_rccl(
                context, config, output_dir=args.output_dir
            )
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"RESULT        TOOL_ERROR\nERROR         {exc}")
        return 3

    print(f"RESULT        {result.status}")
    print(f"BACKEND       {result.backend}")
    print(f"NODES         {','.join(result.nodes)}")
    print(f"TRANSPORT     {result.data_transport or 'NOT_VERIFIED'}")
    print(f"GDR           {result.metrics.get('gpudirect_status', 'NOT_VERIFIED')}")
    print(f"PERFORMANCE   {result.metrics.get('performance_status', 'NOT_VERIFIED')}")
    print(f"REASON        {result.reason_code}")
    if result.root_cause_candidates:
        print(f"ROOT_CAUSE    {','.join(result.root_cause_candidates)}")
    print(f"JSON          {args.output_dir / 'active-result.json'}")
    print(f"SUMMARY       {args.output_dir / 'active-summary.md'}")
    print(f"EVIDENCE      {result.evidence_dir}")
    return ACTIVE_EXIT_CODES[result.status]


def _run_ib_fabric_slurm(args, parser: argparse.ArgumentParser) -> int:
    try:
        nodes = (
            list(args.node)
            if args.node is not None
            else parse_nodes_file(args.nodes_file, max_nodes=args.max_nodes)
        )
        context = SlurmActiveContext(
            job_id=args.slurm_job_id,
            selected_nodes=tuple(nodes),
            enabled=args.enable_fabric_check,
            confirm_allocation_idle=args.confirm_allocation_idle,
            unsafe_allow_overlap=args.unsafe_allow_overlap,
            max_selected_nodes=args.max_nodes,
            control_timeout_seconds=args.control_timeout,
        )
        config = IBFabricCheckConfig(
            hcas=tuple(args.hca),
            ib_port=args.ib_port,
            container_name=args.container_name,
            sample_interval_seconds=args.counter_interval,
            command_timeout_seconds=args.command_timeout,
            query_qps=args.query_qps,
            max_workers=args.max_workers,
            overall_timeout_seconds=args.overall_timeout,
            max_nodes=args.max_nodes,
            max_hcas_per_node=args.max_hcas_per_node,
            max_unique_leaf_ports=args.max_unique_leaf_ports,
            expected_link_width=args.expected_link_width,
            minimum_link_speed_gbps=args.minimum_link_speed_gbps,
        )
        context.validate()
        config.validate(len(nodes))
        if not 1 <= args.counter_interval <= 60:
            raise ValueError("--counter-interval must be between 1 and 60")
        claim_output_directory(args.output_dir)
        result = SlurmIBFabricRunner().run(context, config)
        json_path, markdown_path = write_ib_fabric_reports(
            result, args.output_dir, output_dir_claimed=True
        )
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"RESULT        TOOL_ERROR\nERROR         {exc}")
        return 3

    leaf_switches = sorted({link.switch_name for link in result.adjacency_links})
    print(f"RESULT        {result.status}")
    print(f"NODES         {','.join(result.nodes)}")
    print(f"LINKS         {len(result.adjacency_links)}")
    print(f"LEAF_PORTS    {len(result.counter_health)}")
    print(f"LEAF_SWITCHES {','.join(leaf_switches) or 'NOT_VERIFIED'}")
    print(f"REASON        {result.reason_code}")
    print(f"SAFETY        {result.safety_boundary}")
    print(
        "SWITCH_POLICY "
        f"{result.switch_configuration_policy['status']}:"
        f"{result.switch_configuration_policy['reason_code']}"
    )
    print(f"JSON          {json_path}")
    print(f"SUMMARY       {markdown_path}")
    return FABRIC_EXIT_CODES[result.status]


def build_parser(*, prog: str | None = None) -> argparse.ArgumentParser:
    parser = ToolArgumentParser(prog=prog, description="Read-only HCU startup preflight")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)
    k8s = subparsers.add_parser("k8s-pod", help="check one explicit Kubernetes pod/container")
    k8s.add_argument("--namespace", required=True)
    k8s.add_argument("--pod", required=True)
    k8s.add_argument("--container", required=True)
    k8s.add_argument("--context")
    k8s.add_argument("--kubeconfig")
    k8s.add_argument("--device-resource-name", default="hygon.com/hcu")
    k8s.add_argument("--expected-devices", type=int)
    k8s.add_argument("--max-vram-used-percent", type=float, default=5.0)
    k8s.add_argument("--max-hcu-util-percent", type=float, default=5.0)
    k8s.add_argument("--samples", type=int, default=3)
    k8s.add_argument("--busy-sample-quorum", type=int, default=2)
    k8s.add_argument("--sample-interval", type=float, default=1.0)
    k8s.add_argument("--command-timeout", type=float, default=30.0)
    k8s.add_argument("--skip-environment", action="store_true")
    k8s.add_argument("--require-compiler", action="store_true")
    k8s.add_argument("--require-rdma", action="store_true")
    k8s.add_argument("--minimum-rdma-devices", type=int, default=0)
    k8s.add_argument(
        "--expected-rdma-protocol",
        choices=("auto", "ib", "roce"),
        default="auto",
        help="read-only expected current port protocol; auto only detects",
    )
    k8s.add_argument("--rdma-policy-file", type=Path)
    k8s.add_argument(
        "--rdma-counter-interval",
        type=int,
        default=5,
        help="RDMA counter observation seconds: 1-60, or 0 to disable",
    )
    k8s.add_argument("--require-rccl", action="store_true")
    k8s.add_argument("--require-ucx", action="store_true")
    k8s.add_argument("--output", type=Path)
    k8s.add_argument("--evidence-dir", type=Path)

    cluster = subparsers.add_parser(
        "k8s-cluster",
        help="concurrently check explicit Kubernetes nodes and merge one report",
    )
    cluster_node_source = cluster.add_mutually_exclusive_group(required=True)
    cluster_node_source.add_argument("--node", action="append")
    cluster_node_source.add_argument("--nodes-file", type=Path)
    cluster.add_argument("--namespace", required=True)
    cluster.add_argument("--image", required=True)
    cluster.add_argument("--image-pull-policy", choices=("IfNotPresent", "Always", "Never"), default="IfNotPresent")
    cluster.add_argument("--probe-container", default="hcu-envcheck")
    cluster.add_argument(
        "--reuse-pod",
        action="append",
        default=[],
        metavar="NODE=NAMESPACE/POD/CONTAINER",
    )
    cluster.add_argument("--context")
    cluster.add_argument("--kubeconfig")
    cluster.add_argument("--device-resource-name", default="hygon.com/hcu")
    cluster.add_argument("--expected-devices", type=int, required=True)
    cluster.add_argument("--target-scale-devices", type=int, default=10000)
    cluster.add_argument("--max-vram-used-percent", type=float, default=5.0)
    cluster.add_argument("--max-hcu-util-percent", type=float, default=5.0)
    cluster.add_argument("--samples", type=int, default=3)
    cluster.add_argument("--busy-sample-quorum", type=int, default=2)
    cluster.add_argument("--sample-interval", type=float, default=1.0)
    cluster.add_argument("--command-timeout", type=float, default=30.0)
    cluster.add_argument("--skip-environment", action="store_true")
    cluster.add_argument("--require-compiler", action="store_true")
    cluster.add_argument("--require-rdma", action="store_true")
    cluster.add_argument("--minimum-rdma-devices", type=int, default=0)
    cluster.add_argument(
        "--expected-rdma-protocol",
        choices=("auto", "ib", "roce"),
        default="auto",
        help="read-only expected current port protocol; auto only detects",
    )
    cluster.add_argument("--rdma-policy-file", type=Path)
    cluster.add_argument(
        "--rdma-counter-interval",
        type=int,
        default=5,
        help="RDMA counter observation seconds: 1-60, or 0 to disable",
    )
    cluster.add_argument("--require-rccl", action="store_true")
    cluster.add_argument("--require-ucx", action="store_true")
    cluster.add_argument("--strict-stack-consistency", action="store_true")
    cluster.add_argument("--bootstrap-wheel", type=Path)
    cluster.add_argument("--bootstrap-wheel-sha256")
    cluster.add_argument("--probe-memory-request", default="1Gi")
    cluster.add_argument("--probe-memory-limit", default="8Gi")
    cluster.add_argument("--probe-env", action="append", default=[], metavar="NAME=VALUE")
    cluster.add_argument("--pod-ready-timeout", type=int, default=180)
    cluster.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_K8S_CLUSTER_CONCURRENCY,
        help=f"node worker concurrency (1-{MAX_K8S_CLUSTER_CONCURRENCY})",
    )
    cluster.add_argument(
        "--api-qps",
        type=float,
        default=DEFAULT_KUBECTL_API_QPS,
        help=f"global kubectl request QPS limit (0-{MAX_KUBECTL_API_QPS:g}]",
    )
    cluster.add_argument(
        "--api-burst",
        type=int,
        default=DEFAULT_KUBECTL_API_BURST,
        help=f"global kubectl token burst (1-{MAX_KUBECTL_API_BURST})",
    )
    cluster.add_argument("--output-dir", type=Path, required=True)

    baremetal = subparsers.add_parser(
        "baremetal-cluster",
        help="concurrently check bare-metal or Slurm nodes and merge one report",
    )
    node_source = baremetal.add_mutually_exclusive_group(required=True)
    node_source.add_argument("--node", action="append")
    node_source.add_argument("--nodes-file", type=Path)
    node_source.add_argument("--slurm-job-id")
    node_source.add_argument("--slurm-nodelist")
    baremetal.add_argument("--transport", choices=("auto", "clush", "ssh"), default="auto")
    baremetal.add_argument("--concurrency", type=int, default=32)
    baremetal.add_argument("--connect-timeout", type=float, default=10.0)
    baremetal.add_argument("--command-timeout", type=float, default=240.0)
    baremetal.add_argument("--ssh-user")
    baremetal.add_argument("--ssh-port", type=int, default=22)
    baremetal.add_argument("--identity-file", type=Path)
    baremetal.add_argument("--ssh-config-file", type=Path)
    baremetal.add_argument("--known-hosts-file", type=Path)
    baremetal.add_argument(
        "--strict-host-key-checking", choices=("yes", "accept-new"), default="yes"
    )
    baremetal.add_argument("--remote-python", default="python3")
    baremetal.add_argument("--expected-devices", type=int)
    baremetal.add_argument("--target-scale-devices", type=int, default=10000)
    baremetal.add_argument("--max-vram-used-percent", type=float, default=5.0)
    baremetal.add_argument("--max-hcu-util-percent", type=float, default=5.0)
    baremetal.add_argument("--samples", type=int, default=3)
    baremetal.add_argument("--busy-sample-quorum", type=int, default=2)
    baremetal.add_argument("--sample-interval", type=float, default=1.0)
    baremetal.add_argument(
        "--software-mode",
        choices=("host-python", "conda", "docker"),
        required=True,
        help="explicit training software target; exactly one mode is required",
    )
    baremetal.add_argument("--conda-prefix")
    baremetal.add_argument(
        "--conda-storage",
        choices=("shared", "node-local"),
        help="required with --software-mode conda",
    )
    baremetal.add_argument(
        "--docker-image",
        help="required with --software-mode docker; one temporary container is used per node",
    )
    baremetal.add_argument("--container-python", default="python3")
    baremetal.add_argument(
        "--require-python-package",
        action="append",
        default=[],
        metavar="PACKAGE",
        help=(
            "require one Python package in the selected software target; "
            "repeat as needed, and use PACKAGE=torch to enable Torch runtime checks"
        ),
    )
    baremetal.add_argument("--require-compiler", action="store_true")
    baremetal.add_argument("--require-rdma", action="store_true")
    baremetal.add_argument("--minimum-rdma-devices", type=int, default=0)
    baremetal.add_argument(
        "--expected-rdma-protocol",
        choices=("auto", "ib", "roce"),
        default="auto",
        help="read-only expected current port protocol; auto only detects",
    )
    baremetal.add_argument("--rdma-policy-file", type=Path)
    baremetal.add_argument(
        "--rdma-counter-interval",
        type=int,
        default=5,
        help="RDMA counter observation seconds: 1-60, or 0 to disable",
    )
    baremetal.add_argument("--require-rccl", action="store_true")
    baremetal.add_argument("--require-ucx", action="store_true")
    baremetal.add_argument("--strict-hardware-consistency", action="store_true")
    baremetal.add_argument(
        "--enable-node-health-checks",
        action="store_true",
        help=(
            "run one pass of IB state, ib_write_bw and NHC checks; "
            "never changes node taints"
        ),
    )
    baremetal.add_argument(
        "--enable-ib-state",
        action="store_true",
        help="run ibstat once on every selected node and require Active/LinkUp ports",
    )
    baremetal.add_argument("--ib-state-timeout", type=float, default=30.0)
    baremetal.add_argument(
        "--enable-nhc",
        action="store_true",
        help="run NHC once on every selected node after static preflight",
    )
    baremetal.add_argument(
        "--nhc-command",
        help="remote NHC executable or wrapper path; defaults to run_nhc from PATH",
    )
    baremetal.add_argument(
        "--nhc-install-source",
        "--nhc-installation-source",
        dest="nhc_installation_source",
        default=None,
        help=(
            "optional installation hint reported when the node's run_nhc command "
            "is missing or cannot produce a health result"
        ),
    )
    baremetal.add_argument("--nhc-config", help="node_check config name, for example nhc.json")
    baremetal.add_argument("--nhc-selected", help="run only selected NHC modules")
    baremetal.add_argument("--nhc-removed", help="run NHC modules except this selection")
    baremetal.add_argument("--nhc-timeout", type=float, default=600.0)
    baremetal.add_argument(
        "--enable-ib-write-bw",
        action="store_true",
        help=(
            "run one all-direction ib_write_bw pass across selected nodes; "
            "requires --transport ssh"
        ),
    )
    baremetal.add_argument(
        "--confirm-nodes-idle",
        action="store_true",
        help="confirm selected nodes are idle enough for active ib_write_bw traffic",
    )
    baremetal.add_argument(
        "--ib-tool",
        choices=("ib_write_bw", "ib_send_bw", "ib_read_bw"),
        default="ib_write_bw",
    )
    baremetal.add_argument("--ib-protocol", choices=("ib", "roce"), default="ib")
    baremetal.add_argument("--ib-dev")
    baremetal.add_argument("--ib-port", type=int, default=1)
    baremetal.add_argument("--ib-gid-index", type=int)
    baremetal.add_argument("--ib-control-port", type=int, default=18515)
    baremetal.add_argument("--ib-message-bytes", type=int, default=1 << 20)
    baremetal.add_argument("--ib-iters", type=int, default=1000)
    baremetal.add_argument("--ib-minimum-average-gbps", type=float)
    baremetal.add_argument("--ib-startup-grace", type=float, default=1.0)
    baremetal.add_argument("--ib-timeout", type=float, default=120.0)
    baremetal.add_argument("--ib-concurrency", type=int, default=1)
    baremetal.add_argument("--ib-max-tests", type=int, default=1024)
    baremetal.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help=(
            "reusable output root; each run creates an isolated "
            "nodes_check_TIMESTAMP directory"
        ),
    )

    active = subparsers.add_parser(
        "active-rdma-slurm",
        help="opt-in verbs, rccl-tests, or PyTorch/RCCL validation in an idle Slurm allocation",
    )
    active_nodes = active.add_mutually_exclusive_group(required=True)
    active_nodes.add_argument("--node", action="append")
    active_nodes.add_argument("--nodes-file", type=Path)
    active.add_argument("--slurm-job-id", required=True)
    active.add_argument(
        "--backend", choices=("verbs", "rccl", "torch-rccl"), required=True
    )
    active.add_argument("--enable-active-checks", action="store_true", required=True)
    active.add_argument("--confirm-allocation-idle", action="store_true", required=True)
    active.add_argument(
        "--unsafe-allow-overlap",
        action="store_true",
        help=(
            "allow reuse of an allocation batch step with srun --overlap; "
            "formal PASS is suppressed"
        ),
    )
    active.add_argument("--control-timeout", type=float, default=20.0)
    active.add_argument("--command-timeout", type=float, default=300.0)
    active.add_argument("--output-dir", type=Path, required=True)

    active.add_argument(
        "--verbs-tool",
        choices=("ib_write_bw", "ib_send_bw", "ib_read_bw"),
        default="ib_write_bw",
    )
    active.add_argument("--rdma-protocol", choices=("ib", "roce"), default="ib")
    active.add_argument("--verbs-hca", "--rdma-device", dest="verbs_hca")
    active.add_argument("--verbs-port", "--ib-port", dest="verbs_port", type=int, default=1)
    active.add_argument("--verbs-gid-index", "--gid-index", dest="verbs_gid_index", type=int)
    active.add_argument("--verbs-control-port", type=int, default=18515)
    active.add_argument("--verbs-message-bytes", type=int, default=1 << 20)
    active.add_argument("--verbs-iterations", type=int, default=1000)
    active.add_argument("--minimum-verbs-gbps", type=float)
    active.add_argument("--verbs-startup-grace", type=float, default=1.0)

    active.add_argument("--rccl-binary", default="all_reduce_perf")
    active.add_argument("--container-name")
    active.add_argument("--python-binary", default="python3")
    active.add_argument("--master-port", type=int, default=29500)
    active.add_argument(
        "--rccl-env",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="repeatable allowlisted RCCL environment variable",
    )
    active.add_argument("--rccl-tasks-per-node", type=int, default=1)
    active.add_argument(
        "--rccl-mpi-mode",
        choices=("pmix", "pmix_v4", "pmi2"),
        default="pmix",
        help="explicit Slurm MPI/PMI contract for rccl-tests",
    )
    active.add_argument("--rccl-devices-per-task", type=int, default=1)
    active.add_argument("--rccl-minimum-bytes", type=int, default=8 * 1024 * 1024)
    active.add_argument("--rccl-maximum-bytes", type=int, default=128 * 1024 * 1024)
    active.add_argument("--rccl-step-factor", type=int, default=2)
    active.add_argument("--rccl-warmup-iterations", type=int, default=5)
    active.add_argument("--rccl-iterations", type=int, default=20)
    active.add_argument(
        "--minimum-rccl-busbw-gbytes-per-second",
        type=float,
        help="minimum rccl-tests average bus bandwidth; must be positive",
    )

    active.add_argument("--minimum-rccl-algbw-gbytes-per-second", type=float)
    active.add_argument("--minimum-rccl-row-busbw-gbytes-per-second", type=float)
    active.add_argument(
        "--require-rccl-gdr",
        action="store_true",
        help="require every expected Rank to prove a selected /GDRDMA data path",
    )
    fabric = subparsers.add_parser(
        "ib-fabric-slurm",
        help="opt-in bounded Native-IB one-hop fabric inspection in an idle Slurm allocation",
    )
    fabric_nodes = fabric.add_mutually_exclusive_group(required=True)
    fabric_nodes.add_argument("--node", action="append")
    fabric_nodes.add_argument("--nodes-file", type=Path)
    fabric.add_argument("--slurm-job-id", required=True)
    fabric.add_argument("--hca", action="append", required=True)
    fabric.add_argument("--ib-port", type=int, default=1)
    fabric.add_argument("--expected-link-width")
    fabric.add_argument("--minimum-link-speed-gbps", type=float)
    fabric.add_argument("--container-name")
    fabric.add_argument("--enable-fabric-check", action="store_true", required=True)
    fabric.add_argument("--confirm-allocation-idle", action="store_true", required=True)
    fabric.add_argument(
        "--unsafe-allow-overlap",
        action="store_true",
        help="diagnostic-only overlap; results cannot receive formal PASS",
    )
    fabric.add_argument("--counter-interval", type=float, default=5.0)
    fabric.add_argument("--query-qps", type=float, default=2.0)
    fabric.add_argument("--max-workers", type=int, default=16)
    fabric.add_argument("--overall-timeout", type=float, default=900.0)
    fabric.add_argument("--control-timeout", type=float, default=20.0)
    fabric.add_argument("--command-timeout", type=float, default=15.0)
    fabric.add_argument("--max-nodes", type=int, default=64)
    fabric.add_argument("--max-hcas-per-node", type=int, default=16)
    fabric.add_argument("--max-unique-leaf-ports", type=int, default=512)
    fabric.add_argument("--output-dir", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "active-rdma-slurm":
        return _run_active_rdma_slurm(args, parser)
    if args.command == "ib-fabric-slurm":
        return _run_ib_fabric_slurm(args, parser)
    if args.samples < 1:
        parser.error("--samples must be at least 1")
    if args.sample_interval < 0:
        parser.error("--sample-interval cannot be negative")
    if args.busy_sample_quorum < 1 or args.busy_sample_quorum > args.samples:
        parser.error("--busy-sample-quorum must be between 1 and --samples")
    if args.minimum_rdma_devices < 0:
        parser.error("--minimum-rdma-devices cannot be negative")
    if args.rdma_counter_interval != 0 and not 1 <= args.rdma_counter_interval <= 60:
        parser.error("--rdma-counter-interval must be 0 or between 1 and 60")
    try:
        rdma_policy = (
            load_roce_policy(args.rdma_policy_file)
            if args.rdma_policy_file is not None
            else None
        )
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    if rdma_policy is not None:
        if args.expected_rdma_protocol == "ib":
            parser.error(
                "--rdma-policy-file is a RoCE policy and conflicts with "
                "--expected-rdma-protocol ib"
            )
        args.expected_rdma_protocol = "roce"
    if args.command in {"k8s-pod", "k8s-cluster"} and args.skip_environment:
        profile_options = any(
            (
                args.require_compiler,
                args.require_rdma,
                args.minimum_rdma_devices,
                args.expected_rdma_protocol != "auto",
                args.rdma_policy_file,
                args.require_rccl,
                args.require_ucx,
            )
        )
        if profile_options:
            parser.error(
                "--skip-environment cannot be combined with compiler/RDMA/RCCL/UCX profile checks"
            )
    if args.command == "baremetal-cluster":
        try:
            enable_all_node_health = args.enable_node_health_checks
            enable_ib_state = enable_all_node_health or args.enable_ib_state
            enable_nhc = enable_all_node_health or args.enable_nhc
            enable_ib_write_bw = enable_all_node_health or args.enable_ib_write_bw
            ssh_specific = any(
                (
                    args.ssh_user,
                    args.ssh_port != 22,
                    args.identity_file,
                    args.ssh_config_file,
                    args.known_hosts_file,
                    args.strict_host_key_checking != "yes",
                )
            )
            if ssh_specific and args.transport != "ssh":
                raise ValueError(
                    "SSH-specific options require --transport ssh; clush does not consume them"
                )
            if enable_ib_write_bw and args.transport != "ssh":
                raise ValueError(
                    "ib_write_bw node-health checks require --transport ssh"
                )
            if enable_ib_write_bw and not args.confirm_nodes_idle:
                raise ValueError(
                    "ib_write_bw node-health checks require --confirm-nodes-idle"
                )
            validate_environment_selection(
                env_mode=args.software_mode,
                conda_prefix=args.conda_prefix,
                conda_storage=args.conda_storage,
                image=args.docker_image,
            )
            nodes = resolve_baremetal_nodes(
                nodes=args.node,
                nodes_file=args.nodes_file,
                slurm_job_id=args.slurm_job_id,
                slurm_nodelist=args.slurm_nodelist,
            )
            if args.slurm_job_id is not None or args.slurm_nodelist is not None:
                try:
                    slurm_states = collect_slurm_node_states(nodes)
                except RuntimeError as exc:
                    slurm_states = {
                        node: {
                            "state": "UNKNOWN",
                            "reason": f"Slurm state collection failed: {exc}",
                        }
                        for node in nodes
                    }
            else:
                slurm_states = None
            policy = BaremetalPreflightPolicy(
                expected_devices=args.expected_devices,
                max_vram_used_percent=args.max_vram_used_percent,
                max_hcu_util_percent=args.max_hcu_util_percent,
                samples=args.samples,
                busy_sample_quorum=args.busy_sample_quorum,
                sample_interval_seconds=args.sample_interval,
                software_mode=args.software_mode,
                conda_prefix=args.conda_prefix,
                conda_storage=args.conda_storage,
                docker_image=args.docker_image,
                container_python=args.container_python,
                required_python_packages=tuple(args.require_python_package),
                require_compiler=args.require_compiler,
                require_rdma=args.require_rdma,
                minimum_rdma_devices=args.minimum_rdma_devices,
                expected_rdma_protocol=args.expected_rdma_protocol,
                require_rccl=args.require_rccl,
                require_ucx=args.require_ucx,
                strict_hardware_consistency=args.strict_hardware_consistency,
                target_scale_devices=args.target_scale_devices,
                rdma_policy=rdma_policy,
                rdma_counter_interval_seconds=args.rdma_counter_interval,
            )
            execution = BaremetalExecutionConfig(
                output_root=args.output_dir / "evidence",
                transport=args.transport,
                concurrency=args.concurrency,
                connect_timeout_seconds=args.connect_timeout,
                command_timeout_seconds=args.command_timeout,
                ssh_user=args.ssh_user,
                ssh_port=args.ssh_port,
                identity_file=args.identity_file,
                ssh_config_file=args.ssh_config_file,
                known_hosts_file=args.known_hosts_file,
                strict_host_key_checking=args.strict_host_key_checking,
            )
            extra_checks = ClusterExtraCheckConfig(
                ib_state=IBStateCheckConfig(
                    enabled=enable_ib_state,
                    timeout_seconds=args.ib_state_timeout,
                ),
                nhc=NHCCheckConfig(
                    enabled=enable_nhc,
                    command=(args.nhc_command,) if args.nhc_command else ("run_nhc",),
                    installation_source=args.nhc_installation_source,
                    config=args.nhc_config,
                    selected=args.nhc_selected,
                    removed=args.nhc_removed,
                    timeout_seconds=args.nhc_timeout,
                ),
                ib=IBWriteBandwidthConfig(
                    enabled=enable_ib_write_bw,
                    tool=args.ib_tool,
                    protocol=args.ib_protocol,
                    device=args.ib_dev,
                    ib_port=args.ib_port,
                    gid_index=args.ib_gid_index,
                    control_port=args.ib_control_port,
                    message_bytes=args.ib_message_bytes,
                    iterations=args.ib_iters,
                    minimum_average_gbps=args.ib_minimum_average_gbps,
                    startup_grace_seconds=args.ib_startup_grace,
                    timeout_seconds=args.ib_timeout,
                    concurrency=args.ib_concurrency,
                    max_tests=args.ib_max_tests,
                ),
            )
            report, json_path, md_path = run_baremetal_cluster_preflight(
                nodes=nodes,
                execution_config=execution,
                policy=policy,
                output_dir=args.output_dir,
                remote_python=args.remote_python,
                slurm_node_states=slurm_states,
                extra_checks=extra_checks,
            )
        except (OSError, RuntimeError, ValueError) as exc:
            print(f"RESULT        TOOL_ERROR\nERROR         {exc}")
            return 3
        print(f"RESULT        {report['status']}")
        for item in report.get("node_result_groups", []):
            device_count = item["device_count"] if item["device_count"] is not None else "-"
            print(
                f"NODE_GROUP    nodes={','.join(item['nodes'])} status={item['status']} "
                f"reachable={item['reachable']} devices={device_count} "
                f"reasons={','.join(item['reason_codes']) or '-'}"
            )
        print(f"RUN_DIR       {json_path.parent}")
        print(f"JSON          {json_path}")
        print(f"SUMMARY       {md_path}")
        return EXIT_CODES[report["status"]]
    if args.command == "k8s-cluster":
        if args.expected_devices < 1:
            parser.error("--expected-devices must be at least 1")
        if args.target_scale_devices < 1:
            parser.error("--target-scale-devices must be at least 1")
        if args.pod_ready_timeout < 1:
            parser.error("--pod-ready-timeout must be at least 1")
        if not 1 <= args.concurrency <= MAX_K8S_CLUSTER_CONCURRENCY:
            parser.error(
                f"--concurrency must be between 1 and {MAX_K8S_CLUSTER_CONCURRENCY}"
            )
        if not 0 < args.api_qps <= MAX_KUBECTL_API_QPS:
            parser.error(
                f"--api-qps must be greater than 0 and at most {MAX_KUBECTL_API_QPS:g}"
            )
        if not 1 <= args.api_burst <= MAX_KUBECTL_API_BURST:
            parser.error(
                f"--api-burst must be between 1 and {MAX_KUBECTL_API_BURST}"
            )
        try:
            nodes = (
                args.node
                if args.node is not None
                else parse_nodes_file(args.nodes_file)
            )
            report, json_path, md_path = run_k8s_cluster_preflight(
                nodes=nodes,
                namespace=args.namespace,
                image=args.image,
                image_pull_policy=args.image_pull_policy,
                probe_container=args.probe_container,
                reuse_pods=parse_reuse_pods(args.reuse_pod),
                context=args.context,
                kubeconfig=args.kubeconfig,
                device_resource_name=args.device_resource_name,
                expected_devices=args.expected_devices,
                target_scale_devices=args.target_scale_devices,
                max_vram_used_percent=args.max_vram_used_percent,
                max_hcu_util_percent=args.max_hcu_util_percent,
                samples=args.samples,
                busy_sample_quorum=args.busy_sample_quorum,
                sample_interval_seconds=args.sample_interval,
                command_timeout=args.command_timeout,
                pod_ready_timeout=args.pod_ready_timeout,
                output_dir=args.output_dir,
                include_environment=not args.skip_environment,
                require_compiler=args.require_compiler,
                require_rdma=args.require_rdma,
                minimum_rdma_devices=args.minimum_rdma_devices,
                expected_rdma_protocol=args.expected_rdma_protocol,
                require_rccl=args.require_rccl,
                require_ucx=args.require_ucx,
                strict_stack_consistency=args.strict_stack_consistency,
                bootstrap_wheel=args.bootstrap_wheel,
                bootstrap_wheel_sha256=args.bootstrap_wheel_sha256,
                probe_memory_request=args.probe_memory_request,
                probe_memory_limit=args.probe_memory_limit,
                probe_env=parse_probe_env(args.probe_env),
                rdma_policy=rdma_policy,
                rdma_counter_interval_seconds=args.rdma_counter_interval,
                concurrency=args.concurrency,
                api_qps=args.api_qps,
                api_burst=args.api_burst,
            )
        except (OSError, RuntimeError, ValueError) as exc:
            print(f"RESULT        TOOL_ERROR\nERROR         {exc}")
            return 3
        print(f"RESULT        {report['status']}")
        for item in report.get("node_result_groups", []):
            print(
                f"NODE_GROUP    nodes={','.join(item['nodes'])} status={item['status']} "
                f"devices={item['device_count']} reasons={item['reason_codes']} "
                f"cleanup={item['cleanup_status']}"
            )
        for finding in report.get("consistency_findings", []):
            print(
                f"CLUSTER       {finding['reason_code']} field={finding['field']} "
                f"values={finding['values']}"
            )
        print(f"JSON          {json_path}")
        print(f"SUMMARY       {md_path}")
        return EXIT_CODES[report["status"]]

    if args.output is None:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        run_root = Path.cwd() / f"hcu-envcheck-k8s-pod-{stamp}-{uuid.uuid4().hex[:8]}"
        output_path = run_root / "preflight-result.json"
        evidence_dir = args.evidence_dir or run_root / "evidence"
    else:
        output_path = args.output
        evidence_dir = args.evidence_dir or output_path.parent / f"{output_path.stem}-evidence"
    try:
        validate_output_layout(output_path, evidence_dir)
        require_new_output_path(output_path, label="output file")
        require_new_output_path(evidence_dir, label="evidence directory")
    except ValueError as exc:
        print(f"RESULT        TOOL_ERROR\nERROR         {exc}")
        return 3

    target = KubernetesPodTarget(
        namespace=args.namespace,
        pod=args.pod,
        container=args.container,
        context=args.context,
        kubeconfig=args.kubeconfig,
        device_resource_name=args.device_resource_name,
    )
    executor = KubernetesPodExecutor(target, timeout_seconds=args.command_timeout)
    try:
        result = run_k8s_hcu_preflight(
            executor,
            expected_devices=args.expected_devices,
            max_vram_used_percent=args.max_vram_used_percent,
            max_hcu_util_percent=args.max_hcu_util_percent,
            samples=args.samples,
            busy_sample_quorum=args.busy_sample_quorum,
            sample_interval_seconds=args.sample_interval,
            evidence_dir=evidence_dir,
            include_environment=not args.skip_environment,
            require_compiler=args.require_compiler,
            require_rdma=args.require_rdma,
            minimum_rdma_devices=args.minimum_rdma_devices,
            expected_rdma_protocol=args.expected_rdma_protocol,
            require_rccl=args.require_rccl,
            require_ucx=args.require_ucx,
            rdma_policy=rdma_policy,
            rdma_counter_interval_seconds=args.rdma_counter_interval,
        )
    except (KubectlError, OSError, ValueError) as exc:
        print(f"RESULT        TOOL_ERROR\nERROR         {exc}")
        return 3
    try:
        save_result(result, output_path)
    except (OSError, ValueError) as exc:
        print(f"RESULT        TOOL_ERROR\nERROR         {exc}")
        return 3
    setattr(result, "_output_path", str(output_path))
    print_summary(result)
    return EXIT_CODES[result.status]
