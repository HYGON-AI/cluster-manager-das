# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Trie rank-distribution clustering with signature fallback."""

from __future__ import annotations

from .aggregation_result import AggregationResult, StackGroup, build_aggregation_result
from .models import AggregationMethod, ParallelTopology, StackSnapshot
from .stack_trie import StackTrie


class TrieStackAggregator:
    """
    Cluster by complete-stack rank sets from a StackTrie.

    Dominant cluster = leaf with the largest rank set (healthy majority).
    Outlier ranks = all other ranks; then PP/TP/DP over-eviction applies.
    """

    def __init__(
        self,
        *,
        min_dominant_ratio: float = 0.5,
        min_outlier_coverage: float = 0.75,
    ) -> None:
        self.min_dominant_ratio = min_dominant_ratio
        self.min_outlier_coverage = min_outlier_coverage

    def aggregate_snapshots(
        self,
        snapshots: list[StackSnapshot],
        *,
        topology: ParallelTopology | None = None,
        rank_to_machine: dict[int, str] | None = None,
    ) -> AggregationResult | None:
        if not snapshots:
            return None

        all_ranks = {snap.rank for snap in snapshots if snap.rank >= 0}
        if not all_ranks:
            return None

        trie = StackTrie(all_ranks)
        by_rank = {snap.rank: snap for snap in snapshots}
        for snap in snapshots:
            if snap.rank < 0:
                continue
            keys = [
                f"{frame.function}@{frame.file}" if frame.file else frame.function
                for frame in snap.frames
            ]
            if keys:
                trie.insert(keys, snap.rank)

        return self._result_from_trie(
            trie=trie,
            by_rank=by_rank,
            topology=topology,
            rank_to_machine=rank_to_machine,
        )

    def is_confident(self, result: AggregationResult) -> bool:
        total = len(result.dominant_group.ranks) + len(result.outlier_ranks)
        if total == 0:
            return False
        ratio = len(result.dominant_group.ranks) / total
        return ratio >= self.min_dominant_ratio and bool(result.outlier_ranks)

    def _result_from_trie(
        self,
        *,
        trie: StackTrie,
        by_rank: dict[int, StackSnapshot],
        topology: ParallelTopology | None,
        rank_to_machine: dict[int, str] | None,
    ) -> AggregationResult | None:
        clusters = trie.leaf_clusters()
        if not clusters:
            return None

        dominant_leaf = max(clusters, key=lambda c: len(c.ranks))
        total_ranks = trie.all_ranks
        if len(dominant_leaf.ranks) < len(total_ranks) * self.min_dominant_ratio:
            return None

        dominant_ranks = set(dominant_leaf.ranks)
        outlier_ranks = total_ranks - dominant_ranks
        if not outlier_ranks:
            return None

        dominant_snaps = [by_rank[r] for r in dominant_ranks if r in by_rank]
        if not dominant_snaps:
            dominant_snaps = [
                _placeholder_snapshot(r, rank_to_machine) for r in dominant_ranks
            ]

        dominant = StackGroup(
            signature=dominant_leaf.signature,
            snapshots=dominant_snaps,
        )

        outliers: list[StackGroup] = []
        for leaf in clusters:
            if leaf.ranks <= dominant_ranks:
                continue
            diff = set(leaf.ranks) - dominant_ranks
            if not diff:
                continue
            outlier_snaps = [
                by_rank.get(r) or _placeholder_snapshot(r, rank_to_machine)
                for r in diff
            ]
            outliers.append(StackGroup(signature=leaf.signature, snapshots=outlier_snaps))

        if not outliers:
            outlier_snaps = [
                by_rank.get(r) or _placeholder_snapshot(r, rank_to_machine)
                for r in outlier_ranks
            ]
            outliers = [
                StackGroup(signature="trie-outlier", snapshots=outlier_snaps)
            ]

        return build_aggregation_result(
            method=AggregationMethod.TRIE,
            dominant=dominant,
            outliers=outliers,
            topology=topology,
            rank_to_machine=rank_to_machine,
            min_outlier_coverage=self.min_outlier_coverage,
        )


def _placeholder_snapshot(
    rank: int, rank_to_machine: dict[int, str] | None
) -> StackSnapshot:
    from .models import ProcessRole, StackFrame

    machine = (rank_to_machine or {}).get(rank, f"rank-{rank}")
    return StackSnapshot(
        machine_id=machine,
        rank=rank,
        pid=0,
        role=ProcessRole.TRAINER,
        frames=(StackFrame(function="?", file=""),),
    )
