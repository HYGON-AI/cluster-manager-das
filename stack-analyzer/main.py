#!/usr/bin/env python3
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""CLI: hostfile + Ansible py-spy capture and hang diagnosis."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from stack_analyzer.aggregator import StackAggregator
from stack_analyzer.ansible_collector import AnsibleConfig
from stack_analyzer.coordinator import (
    collect_from_hostfile,
    diagnose_from_hostfile,
    snapshots_to_json,
)
from stack_analyzer.docker_collector import DockerSSHConfig, DockerSSHStackCollector
from stack_analyzer.runtime_analyzer import RuntimeAnalyzer
from stack_analyzer.hostfile import topology_from_hostfile
from stack_analyzer.models import (
    AggregationStrategy,
    ParallelTopology,
    ProcessRole,
    StackFrame,
    StackSnapshot,
)
from stack_analyzer.kubernetes_collector import KubernetesConfig, KubernetesStackCollector
from stack_analyzer.process_tree import (
    discover_training_processes,
    list_local_processes_psutil,
)
from stack_analyzer.stack_capture import PySpyCapture, parse_py_spy_text


def _load_snapshots_from_json(path: Path) -> list[StackSnapshot]:
    data = json.loads(path.read_text(encoding="utf-8"))
    snapshots: list[StackSnapshot] = []
    for item in data:
        frames = tuple(
            StackFrame(
                function=f["function"],
                file=f.get("file", ""),
                line=int(f.get("line", 0)),
            )
            for f in item.get("frames", [])
        )
        snapshots.append(
            StackSnapshot(
                machine_id=item["machine_id"],
                rank=int(item["rank"]),
                pid=int(item.get("pid", 0)),
                role=ProcessRole(item.get("role", "trainer")),
                frames=frames,
                raw_text=item.get("raw_text", ""),
            )
        )
    return snapshots


def _load_topology(path: Path) -> ParallelTopology:
    data = json.loads(path.read_text(encoding="utf-8"))
    return ParallelTopology(
        pp_groups=[frozenset(g) for g in data.get("pp_groups", [])],
        tp_groups=[frozenset(g) for g in data.get("tp_groups", [])],
        dp_groups=[frozenset(g) for g in data.get("dp_groups", [])],
    )


def _rank_to_machine(snapshots: list[StackSnapshot]) -> dict[int, str]:
    mapping: dict[int, str] = {}
    for snap in snapshots:
        if snap.rank >= 0:
            mapping[snap.rank] = snap.machine_id
    return mapping


def _ansible_config_from_args(args: argparse.Namespace) -> AnsibleConfig:
    return AnsibleConfig(
        ansible_bin=args.ansible_bin,
        inventory_path=Path(args.inventory) if args.inventory else None,
        remote_user=args.ansible_user,
        forks=args.forks,
        timeout_sec=args.ansible_timeout,
        py_spy_path=args.py_spy,
        capture_timeout=args.timeout,
        remote_script=Path(args.remote_script) if args.remote_script else None,
    )


def _build_analyzer(args: argparse.Namespace) -> RuntimeAnalyzer:
    strategy = AggregationStrategy(getattr(args, "method", "auto"))
    return RuntimeAnalyzer(
        strategy=strategy,
        signature_depth=getattr(args, "depth", 8),
        fuzzy_match=not getattr(args, "exact", False),
    )


def _result_json(result) -> dict:
    return {
        "method": result.method.value,
        "machines_to_evict": sorted(result.machines_to_evict),
        "outlier_ranks": sorted(result.outlier_ranks),
        "patterns": StackAggregator.diagnose_hang_pattern(result),
    }


def cmd_demo(args: argparse.Namespace) -> int:
    if args.scenario == "eval":
        from examples.eval_hang_scenario import (
            build_eval_hang_snapshots,
            build_eval_hang_topology,
        )

        snapshots = build_eval_hang_snapshots()
        topology = build_eval_hang_topology()
    else:
        from examples.pp_hang_scenario import build_demo_snapshots, build_demo_topology

        snapshots = build_demo_snapshots()
        topology = build_demo_topology()

    analyzer = _build_analyzer(args)
    result = analyzer.locate_abnormal_nodes(
        snapshots,
        topology=topology,
        rank_to_machine=_rank_to_machine(snapshots),
    )
    print(result.summary())
    print("\nDetected patterns:", StackAggregator.diagnose_hang_pattern(result))
    return 0


def cmd_collect(args: argparse.Namespace) -> int:
    """Collect stacks from all hosts in hostfile via Ansible."""
    snapshots = collect_from_hostfile(
        Path(args.hostfile),
        ansible_config=_ansible_config_from_args(args),
        use_shell=args.use_shell,
        save_raw=Path(args.raw_output) if args.raw_output else None,
        save_stacks=Path(args.output),
    )
    print(f"Collected {len(snapshots)} stacks from hostfile -> {args.output}")
    return 0


def cmd_collect_k8s(args: argparse.Namespace) -> int:
    """Collect stacks from Kubernetes pods via kubectl exec."""
    config = KubernetesConfig(
        kubectl_bin=args.kubectl_bin,
        namespace=args.namespace,
        selector=args.selector,
        container=args.container,
        all_containers=args.all_containers,
        python_bin=args.python_bin,
        py_spy_path=args.py_spy,
        capture_timeout=args.timeout,
        command_timeout=args.command_timeout,
        parallelism=args.parallelism,
        nonblocking=not args.blocking,
        remote_script=Path(args.remote_script) if args.remote_script else None,
    )
    snapshots = KubernetesStackCollector(config).collect(
        save_raw=Path(args.raw_output) if args.raw_output else None
    )
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(snapshots_to_json(snapshots), indent=2), encoding="utf-8")
    print(f"Collected {len(snapshots)} stacks from Kubernetes -> {out}")
    return 0


def cmd_collect_docker(args: argparse.Namespace) -> int:
    """Collect stacks from Docker containers over SSH."""
    config = DockerSSHConfig(
        ssh_bin=args.ssh_bin,
        ssh_user=args.ssh_user,
        ssh_port=args.ssh_port,
        ssh_options=args.ssh_option or [],
        identity_file=args.identity_file,
        python_bin=args.python_bin,
        py_spy_path=args.py_spy,
        capture_timeout=args.timeout,
        command_timeout=args.command_timeout,
        parallelism=args.parallelism,
        nonblocking=not args.blocking,
        remote_script=Path(args.remote_script) if args.remote_script else None,
    )
    snapshots = DockerSSHStackCollector(config).collect(
        Path(args.hostfile),
        save_raw=Path(args.raw_output) if args.raw_output else None,
    )
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(snapshots_to_json(snapshots), indent=2), encoding="utf-8")
    print(f"Collected {len(snapshots)} stacks from Docker SSH -> {out}")
    return 0


def cmd_diagnose(args: argparse.Namespace) -> int:
    """Collect via Ansible + aggregate to find hung/outlier nodes."""
    result = diagnose_from_hostfile(
        Path(args.hostfile),
        topology_path=Path(args.topology) if args.topology else None,
        pp_size=args.pp_size,
        tp_size=args.tp_size,
        dp_size=args.dp_size,
        ansible_config=_ansible_config_from_args(args),
        use_shell=args.use_shell,
        output_dir=Path(args.output_dir) if args.output_dir else None,
        save_stacks=Path(args.stacks_output) if args.stacks_output else None,
        strategy=AggregationStrategy(args.method),
        signature_depth=args.depth,
        fuzzy_match=not args.exact,
        **_topology_kwargs_from_args(args),
    )
    print(result.summary())
    print("\nDetected patterns:", StackAggregator.diagnose_hang_pattern(result))
    if args.json:
        print(json.dumps(_result_json(result), indent=2))
    return 0


def cmd_analyze(args: argparse.Namespace) -> int:
    snapshots = _load_snapshots_from_json(Path(args.input))
    topology: ParallelTopology | None = None
    topo_kw = _topology_kwargs_from_args(args)

    if args.topology:
        topology = _load_topology(Path(args.topology))
    elif args.hostfile:
        topology, _, _ = topology_from_hostfile(args.hostfile, **topo_kw)
    else:
        print(
            "Provide -H/--hostfile with --pp-size/--tp-size/--dp-size "
            "or -t/--topology JSON for parallel-group eviction.",
            file=sys.stderr,
        )
        return 1

    analyzer = _build_analyzer(args)
    result = analyzer.locate_abnormal_nodes(
        snapshots,
        topology=topology,
        rank_to_machine=_rank_to_machine(snapshots),
    )
    print(result.summary())
    if args.json:
        print(json.dumps(_result_json(result), indent=2))
    return 0


def cmd_capture_local(args: argparse.Namespace) -> int:
    processes = list_local_processes_psutil()
    training = discover_training_processes(processes)
    if not training:
        print("No training-related processes found.", file=sys.stderr)
        return 1

    capture = PySpyCapture(timeout_sec=args.timeout)
    targets = [(proc.pid, proc.machine_id, proc.rank, proc.role) for proc in training]
    snapshots = capture.dump_many(targets)
    out = Path(args.output)
    out.write_text(json.dumps(snapshots_to_json(snapshots), indent=2), encoding="utf-8")
    print(f"Captured {len(snapshots)} stacks -> {out}")
    return 0


def _add_ansible_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "-H",
        "--hostfile",
        required=True,
        help="MPI-style hostfile (hostname slots=N per line)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="stacks.json",
        help="Output stacks JSON (collect) or stacks path (diagnose with --stacks-output)",
    )
    parser.add_argument("--ansible-bin", default="ansible")
    parser.add_argument("-u", "--ansible-user", help="SSH user for all hosts")
    parser.add_argument(
        "--inventory",
        help="Optional existing Ansible inventory (default: generated from hostfile)",
    )
    parser.add_argument("--forks", type=int, default=32, help="Ansible parallel forks")
    parser.add_argument(
        "--ansible-timeout",
        type=float,
        default=60.0,
        help="Ansible task timeout (seconds)",
    )
    parser.add_argument(
        "--py-spy",
        default="py-spy",
        help="py-spy binary path on remote hosts",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="py-spy dump timeout per process (seconds)",
    )
    parser.add_argument(
        "--remote-script",
        help="Override remote capture script path on controller",
    )
    parser.add_argument(
        "--use-shell",
        action="store_true",
        help="Deploy script via copy+shell instead of ansible script module",
    )
    parser.add_argument(
        "--raw-output",
        help="Save raw ansible stdout for debugging",
    )


def _add_method_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--method",
        choices=("auto", "trie", "signature"),
        default="auto",
        help="auto=Trie first then signature fallback; trie|signature force backend",
    )


def _add_parallel_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pp-size", type=int, default=1, help="Pipeline parallel size")
    parser.add_argument("--tp-size", type=int, default=1, help="Tensor parallel size")
    parser.add_argument(
        "--dp-size",
        type=int,
        default=None,
        help="Data parallel size (default: world_size/(pp*tp))",
    )
    parser.add_argument(
        "-t",
        "--topology",
        help="Optional topology JSON (overrides hostfile pp/tp/dp inference)",
    )
    parser.add_argument(
        "--parallel-order",
        default="tp-cp-ep-dp-pp",
        help="Megatron rank order (hcu_megatron default: tp-cp-ep-dp-pp)",
    )
    parser.add_argument(
        "--use-tp-pp-dp-mapping",
        action="store_true",
        help="Use tp-cp-ep-pp-dp order (Megatron --use-tp-pp-dp-mapping)",
    )


def _topology_kwargs_from_args(args: argparse.Namespace) -> dict:
    return {
        "pp_size": args.pp_size,
        "tp_size": args.tp_size,
        "dp_size": args.dp_size,
        "order": getattr(args, "parallel_order", "tp-cp-ep-dp-pp"),
        "use_tp_pp_dp_mapping": getattr(args, "use_tp_pp_dp_mapping", False),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stack aggregation via hostfile + Ansible + py-spy"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    demo = sub.add_parser("demo", help="Run offline hang scenarios from the paper")
    demo.add_argument(
        "--scenario",
        choices=("pp", "eval"),
        default="pp",
        help="pp=Fig.7 backward hang; eval=§5.2 evaluation hang",
    )
    demo.add_argument(
        "--method",
        choices=("auto", "trie", "signature"),
        default="auto",
    )
    demo.set_defaults(func=cmd_demo, depth=8, exact=False)

    collect = sub.add_parser(
        "collect",
        help="Ansible: collect py-spy stacks from all hostfile nodes",
    )
    _add_ansible_args(collect)
    collect.set_defaults(func=cmd_collect)

    collect_k8s = sub.add_parser(
        "collect-k8s",
        help="kubectl exec: collect py-spy stacks from Kubernetes pods",
    )
    collect_k8s.add_argument("-n", "--namespace", default="default")
    collect_k8s.add_argument("-l", "--selector", default="", help="Pod label selector")
    collect_k8s.add_argument("-c", "--container", help="Training container name")
    collect_k8s.add_argument(
        "--all-containers", action="store_true", help="Capture every regular container"
    )
    collect_k8s.add_argument("-o", "--output", default="stacks.json")
    collect_k8s.add_argument("--raw-output", help="Write per-container errors")
    collect_k8s.add_argument("--kubectl-bin", default="kubectl")
    collect_k8s.add_argument("--python-bin", default="python3")
    collect_k8s.add_argument("--py-spy", default="py-spy")
    collect_k8s.add_argument("--timeout", type=float, default=15.0)
    collect_k8s.add_argument("--command-timeout", type=float, default=60.0)
    collect_k8s.add_argument("--parallelism", type=int, default=16)
    collect_k8s.add_argument(
        "--blocking",
        action="store_true",
        help="Allow py-spy to pause targets (unsafe for hung/container workloads)",
    )
    collect_k8s.add_argument("--remote-script")
    collect_k8s.set_defaults(func=cmd_collect_k8s)

    collect_docker = sub.add_parser(
        "collect-docker",
        help="SSH: collect py-spy stacks from Docker containers running sshd",
    )
    collect_docker.add_argument(
        "-H",
        "--hostfile",
        required=True,
        help="Docker container hostfile (hostname slots=N per line)",
    )
    collect_docker.add_argument("-o", "--output", default="stacks.json")
    collect_docker.add_argument("--raw-output", help="Write per-container errors")
    collect_docker.add_argument("--ssh-bin", default="ssh")
    collect_docker.add_argument("--ssh-user", help="SSH user for all containers")
    collect_docker.add_argument(
        "--ssh-port",
        type=int,
        default=22,
        help="Same sshd port exposed by every Docker container",
    )
    collect_docker.add_argument(
        "--ssh-option",
        action="append",
        help="Extra ssh -o option, may be repeated",
    )
    collect_docker.add_argument("--identity-file", help="SSH private key path")
    collect_docker.add_argument("--python-bin", default="python3")
    collect_docker.add_argument("--py-spy", default="py-spy")
    collect_docker.add_argument("--timeout", type=float, default=15.0)
    collect_docker.add_argument("--command-timeout", type=float, default=60.0)
    collect_docker.add_argument("--parallelism", type=int, default=16)
    collect_docker.add_argument(
        "--blocking",
        action="store_true",
        help="Allow py-spy to pause targets (unsafe for hung/container workloads)",
    )
    collect_docker.add_argument("--remote-script")
    collect_docker.set_defaults(func=cmd_collect_docker)

    diagnose = sub.add_parser(
        "diagnose",
        help="Ansible collect + aggregate to locate hung nodes",
    )
    _add_ansible_args(diagnose)
    _add_parallel_args(diagnose)
    diagnose.add_argument(
        "--output-dir",
        default="./diagnosis_out",
        help="Directory for stacks.json, diagnosis.json, summary",
    )
    diagnose.add_argument(
        "--stacks-output",
        help="Explicit stacks JSON path (default: <output-dir>/stacks.json)",
    )
    diagnose.add_argument("--json", action="store_true", help="Print diagnosis JSON")
    _add_method_args(diagnose)
    diagnose.set_defaults(func=cmd_diagnose, depth=8, exact=False)

    analyze = sub.add_parser("analyze", help="Aggregate pre-collected stack JSON")
    analyze.add_argument("-i", "--input", required=True)
    analyze.add_argument("-H", "--hostfile", help="Hostfile for rank->machine mapping")
    _add_parallel_args(analyze)
    analyze.add_argument("--depth", type=int, default=8)
    analyze.add_argument("--exact", action="store_true")
    analyze.add_argument("--json", action="store_true")
    _add_method_args(analyze)
    analyze.set_defaults(func=cmd_analyze)

    cap = sub.add_parser("capture-local", help="Capture stacks on local node only")
    cap.add_argument("-o", "--output", default="stacks.json")
    cap.add_argument("--timeout", type=float, default=15.0)
    cap.set_defaults(func=cmd_capture_local)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
