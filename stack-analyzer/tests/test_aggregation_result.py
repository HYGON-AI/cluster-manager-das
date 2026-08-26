# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from stack_analyzer.aggregation_result import StackGroup, build_aggregation_result
from stack_analyzer.models import (
    AggregationMethod,
    ParallelTopology,
    ProcessRole,
    StackFrame,
    StackSnapshot,
)


def snapshot(machine, rank):
    return StackSnapshot(
        machine_id=machine,
        rank=rank,
        pid=rank + 100,
        role=ProcessRole.TRAINER,
        frames=(StackFrame("all_reduce"),),
    )


def test_result_expands_shared_parallel_group_to_eviction_set():
    dominant = StackGroup("healthy", [snapshot("node-a", 0), snapshot("node-b", 1)])
    outlier = StackGroup("blocked", [snapshot("node-c", 2)])
    topology = ParallelTopology(pp_groups=[frozenset({2, 3})])

    result = build_aggregation_result(
        method=AggregationMethod.SIGNATURE,
        dominant=dominant,
        outliers=[outlier],
        topology=topology,
        rank_to_machine={2: "node-c", 3: "node-d"},
        min_outlier_coverage=1.0,
    )

    assert result.outlier_machines == {"node-c"}
    assert result.isolation_group == ("PP", frozenset({2, 3}))
    assert result.machines_to_evict == {"node-c", "node-d"}
    assert "Recommended eviction" in result.summary()


def test_result_without_topology_evicts_only_observed_outliers():
    result = build_aggregation_result(
        method=AggregationMethod.TRIE,
        dominant=StackGroup("healthy", [snapshot("node-a", 0)]),
        outliers=[StackGroup("blocked", [snapshot("node-c", 2)])],
        topology=None,
        rank_to_machine=None,
        min_outlier_coverage=1.0,
    )

    assert result.isolation_group is None
    assert result.machines_to_evict == {"node-c"}
