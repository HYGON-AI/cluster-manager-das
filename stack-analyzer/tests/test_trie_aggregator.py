# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from examples.pp_hang_scenario import build_demo_snapshots, build_demo_topology
from stack_analyzer.models import AggregationMethod, AggregationStrategy
from stack_analyzer.runtime_analyzer import RuntimeAnalyzer
from stack_analyzer.stack_trie import StackTrie
from stack_analyzer.trie_aggregator import TrieStackAggregator


def test_trie_pp_hang_from_snapshots():
    snapshots = build_demo_snapshots()
    topology = build_demo_topology()
    rank_map = {s.rank: s.machine_id for s in snapshots}

    analyzer = RuntimeAnalyzer(strategy=AggregationStrategy.TRIE)
    result = analyzer.locate_abnormal_nodes(
        snapshots,
        topology=topology,
        rank_to_machine=rank_map,
    )

    assert result.method == AggregationMethod.TRIE
    assert result.outlier_ranks == {3, 7, 11, 15}
    assert result.isolation_group[0] == "PP"
    assert result.machines_to_evict == {f"machine-{i}" for i in (3, 7, 11, 15)}


def test_auto_falls_back_to_signature_on_ambiguous():
    """Four identical healthy stacks -> Trie has no outlier; signature may still run."""
    topology = build_demo_topology()
    analyzer = RuntimeAnalyzer(strategy=AggregationStrategy.AUTO)
    snapshots = build_demo_snapshots()[:4]
    result = analyzer.locate_abnormal_nodes(snapshots, topology=topology)
    assert result.method in (AggregationMethod.TRIE, AggregationMethod.SIGNATURE)


def test_trie_aggregator_leaf_clusters():
    trie = StackTrie({0, 1, 2})
    trie.insert(["a", "b"], 0)
    trie.insert(["a", "b"], 1)
    trie.insert(["a", "c"], 2)
    clusters = trie.leaf_clusters()
    assert len(clusters) == 2
    dominant = max(clusters, key=lambda c: len(c.ranks))
    assert dominant.ranks == frozenset({0, 1})


def test_trie_aggregator_direct():
    snapshots = build_demo_snapshots()
    topology = build_demo_topology()
    agg = TrieStackAggregator(min_dominant_ratio=0.5)
    result = agg.aggregate_snapshots(
        snapshots,
        topology=topology,
        rank_to_machine={s.rank: s.machine_id for s in snapshots},
    )
    assert result is not None
    assert result.outlier_ranks == {3, 7, 11, 15}
