# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
Runtime analyzer: Trie rank clustering (primary) + signature clustering (fallback).

Pipeline:
  1. py-spy stack snapshots (JSON or live capture)
  2. Trie: cluster by leaf rank distribution; else signature clustering
  3. Shared parallel group among outliers -> over-evict
"""

from __future__ import annotations

from .aggregation_result import AggregationResult
from .aggregator import StackAggregator
from .models import AggregationStrategy, ParallelTopology, StackSnapshot
from .snapshot_prep import select_primary_snapshots
from .trie_aggregator import TrieStackAggregator


class RuntimeAnalyzer:
    """
    Hang diagnosis with pluggable clustering backend.

    - ``auto`` (default): Trie first; signature if Trie is inconclusive
    - ``trie``: StackTrie leaf rank sets only
    - ``signature``: string signature clustering only
    """

    def __init__(
        self,
        *,
        strategy: AggregationStrategy = AggregationStrategy.AUTO,
        signature_depth: int = 8,
        fuzzy_match: bool = True,
        min_outlier_coverage: float = 0.75,
        min_dominant_ratio: float = 0.5,
        one_stack_per_rank: bool = True,
    ) -> None:
        self.strategy = strategy
        self.one_stack_per_rank = one_stack_per_rank
        self.signature_aggregator = StackAggregator(
            signature_depth=signature_depth,
            fuzzy_match=fuzzy_match,
            min_outlier_coverage=min_outlier_coverage,
        )
        self.trie_aggregator = TrieStackAggregator(
            min_dominant_ratio=min_dominant_ratio,
            min_outlier_coverage=min_outlier_coverage,
        )

    def locate_abnormal_nodes(
        self,
        snapshots: list[StackSnapshot],
        topology: ParallelTopology | None = None,
        rank_to_machine: dict[int, str] | None = None,
    ) -> AggregationResult:
        prepared = select_primary_snapshots(
            snapshots, one_per_rank=self.one_stack_per_rank
        )
        empty = [snap for snap in prepared if not snap.frames]
        if empty:
            labels = [
                f"rank={snap.rank} pid={snap.pid} machine={snap.machine_id}"
                for snap in empty
            ]
            raise RuntimeError(
                "No usable py-spy frames for selected snapshots: "
                + "; ".join(labels)
                + ". Check py-spy JSON compatibility/ptrace permissions and recapture."
            )
        return self._analyze(prepared, topology=topology, rank_to_machine=rank_to_machine)

    def _analyze(
        self,
        snapshots: list[StackSnapshot],
        *,
        topology: ParallelTopology | None,
        rank_to_machine: dict[int, str] | None,
    ) -> AggregationResult:
        if self.strategy in (AggregationStrategy.AUTO, AggregationStrategy.TRIE):
            trie_result = self.trie_aggregator.aggregate_snapshots(
                snapshots,
                topology=topology,
                rank_to_machine=rank_to_machine,
            )
            if trie_result is not None and (
                self.strategy == AggregationStrategy.TRIE
                or self.trie_aggregator.is_confident(trie_result)
            ):
                return trie_result
            if self.strategy == AggregationStrategy.TRIE and trie_result is not None:
                return trie_result

        return self.signature_aggregator.aggregate(
            snapshots,
            topology=topology,
            rank_to_machine=rank_to_machine,
        )
