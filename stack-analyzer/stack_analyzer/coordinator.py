# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Orchestrate hostfile + Ansible capture and stack aggregation."""

from __future__ import annotations

import json
from pathlib import Path

from .aggregation_result import AggregationResult
from .aggregator import StackAggregator
from .ansible_collector import AnsibleConfig, AnsibleStackCollector
from .hostfile import infer_topology, parse_hostfile, rank_to_machine, world_size
from .models import AggregationStrategy, ParallelTopology, StackSnapshot
from .runtime_analyzer import RuntimeAnalyzer


def load_topology(path: Path) -> ParallelTopology:
    data = json.loads(path.read_text(encoding="utf-8"))
    return ParallelTopology(
        pp_groups=[frozenset(g) for g in data.get("pp_groups", [])],
        tp_groups=[frozenset(g) for g in data.get("tp_groups", [])],
        dp_groups=[frozenset(g) for g in data.get("dp_groups", [])],
    )


def snapshots_to_json(snapshots: list[StackSnapshot]) -> list[dict]:
    return [
        {
            "machine_id": snap.machine_id,
            "rank": snap.rank,
            "pid": snap.pid,
            "role": snap.role.value,
            "frames": [
                {"function": f.function, "file": f.file, "line": f.line}
                for f in snap.frames
            ],
            "raw_text": snap.raw_text,
        }
        for snap in snapshots
    ]


def collect_from_hostfile(
    hostfile: Path | str,
    *,
    ansible_config: AnsibleConfig | None = None,
    use_shell: bool = False,
    save_raw: Path | None = None,
    save_stacks: Path | None = None,
) -> list[StackSnapshot]:
    """Step 1: ansible parallel capture on all hosts from hostfile."""
    collector = AnsibleStackCollector(ansible_config)
    if use_shell:
        snapshots = collector.collect_via_shell(hostfile, save_raw=save_raw)
    else:
        snapshots = collector.collect(hostfile, save_raw=save_raw)

    if save_stacks:
        save_stacks.parent.mkdir(parents=True, exist_ok=True)
        save_stacks.write_text(
            json.dumps(snapshots_to_json(snapshots), indent=2),
            encoding="utf-8",
        )
    return snapshots


def diagnose_from_hostfile(
    hostfile: Path | str,
    *,
    topology: ParallelTopology | None = None,
    topology_path: Path | None = None,
    pp_size: int = 1,
    tp_size: int = 1,
    dp_size: int | None = None,
    ansible_config: AnsibleConfig | None = None,
    use_shell: bool = False,
    output_dir: Path | None = None,
    save_stacks: Path | None = None,
    strategy: AggregationStrategy = AggregationStrategy.AUTO,
    signature_depth: int = 8,
    fuzzy_match: bool = True,
    order: str | None = None,
    use_tp_pp_dp_mapping: bool = False,
) -> AggregationResult:
    """
    Full pipeline: hostfile -> ansible py-spy capture -> three-step aggregation.
    """
    hosts = parse_hostfile(hostfile)

    if topology_path:
        topo = load_topology(topology_path)
    elif topology is not None:
        topo = topology
    else:
        topo = infer_topology(
            hosts,
            pp_size=pp_size,
            tp_size=tp_size,
            dp_size=dp_size,
            order=order,
            use_tp_pp_dp_mapping=use_tp_pp_dp_mapping,
        )

    raw_path = output_dir / "ansible_raw.txt" if output_dir else None
    stacks_path = save_stacks or (output_dir / "stacks.json" if output_dir else None)

    snapshots = collect_from_hostfile(
        hostfile,
        ansible_config=ansible_config,
        use_shell=use_shell,
        save_raw=raw_path,
        save_stacks=stacks_path,
    )

    if not snapshots:
        raise RuntimeError("No stack snapshots collected from cluster")

    rank_map = rank_to_machine(hosts)
    analyzer = RuntimeAnalyzer(
        strategy=strategy,
        signature_depth=signature_depth,
        fuzzy_match=fuzzy_match,
    )
    result = analyzer.locate_abnormal_nodes(
        snapshots,
        topology=topo,
        rank_to_machine=rank_map,
    )

    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "aggregation_summary.txt").write_text(
            result.summary(), encoding="utf-8"
        )
        payload = {
            "world_size": world_size(hosts),
            "hosts": [host.hostname for host in hosts],
            "machines_to_evict": sorted(result.machines_to_evict),
            "outlier_machines": sorted(result.outlier_machines),
            "outlier_ranks": sorted(result.outlier_ranks),
            "isolation_group": (
                {
                    "label": result.isolation_group[0],
                    "ranks": sorted(result.isolation_group[1]),
                }
                if result.isolation_group
                else None
            ),
            "method": result.method.value,
            "patterns": StackAggregator.diagnose_hang_pattern(result),
        }
        (output_dir / "diagnosis.json").write_text(
            json.dumps(payload, indent=2), encoding="utf-8"
        )

    return result
