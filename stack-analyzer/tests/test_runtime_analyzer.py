# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Tests for RuntimeAnalyzer and slow-fault multi-round aggregation."""

from stack_analyzer.runtime_analyzer import RuntimeAnalyzer
from examples.eval_hang_scenario import (
    build_eval_hang_snapshots,
    build_eval_hang_topology,
)
from examples.pp_hang_scenario import build_demo_snapshots, build_demo_topology


def _rank_to_machine(snapshots):
    return {s.rank: s.machine_id for s in snapshots}


def test_runtime_analyzer_pp_hang():
    snapshots = build_demo_snapshots()
    topology = build_demo_topology()
    analyzer = RuntimeAnalyzer()
    result = analyzer.locate_abnormal_nodes(
        snapshots,
        topology=topology,
        rank_to_machine=_rank_to_machine(snapshots),
    )
    assert result.machines_to_evict == {
        "machine-3",
        "machine-7",
        "machine-11",
        "machine-15",
    }
    assert result.isolation_group[0] == "PP"


def test_eval_hang_isolates_pp_pipeline():
    snapshots = build_eval_hang_snapshots()
    topology = build_eval_hang_topology()
    analyzer = RuntimeAnalyzer()
    result = analyzer.locate_abnormal_nodes(
        snapshots,
        topology=topology,
        rank_to_machine=_rank_to_machine(snapshots),
    )
    assert result.outlier_machines == {"machine-4", "machine-6"}
    assert result.isolation_group is not None
    label, ranks = result.isolation_group
    assert label == "PP"
    assert ranks == frozenset({0, 2, 4, 6, 8, 10})
    assert result.machines_to_evict == {
        f"machine-{r}" for r in (0, 2, 4, 6, 8, 10)
    }

