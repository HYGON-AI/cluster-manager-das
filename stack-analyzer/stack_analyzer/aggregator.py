# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections import Counter, defaultdict

from .aggregation_result import (
    AggregationResult,
    StackGroup,
    build_aggregation_result,
)
from .models import AggregationMethod, ParallelTopology, StackSnapshot


class StackAggregator:
    """
    Signature-string clustering (fallback backend).

    Three-step aggregation:
      1. Process trees already resolved upstream -> StackSnapshot list
      2. Cluster stacks by string signature; dominant cluster = healthy
      3. Find shared parallel group among outlier ranks -> evict those nodes
    """

    def __init__(
        self,
        *,
        signature_depth: int = 8,
        fuzzy_match: bool = True,
        min_outlier_coverage: float = 0.75,
    ) -> None:
        self.signature_depth = signature_depth
        self.fuzzy_match = fuzzy_match
        self.min_outlier_coverage = min_outlier_coverage

    def aggregate(
        self,
        snapshots: list[StackSnapshot],
        topology: ParallelTopology | None = None,
        rank_to_machine: dict[int, str] | None = None,
    ) -> AggregationResult:
        if not snapshots:
            raise ValueError("No stack snapshots to aggregate")

        groups = self._cluster(snapshots)
        dominant = max(groups, key=lambda g: len(g.snapshots))
        outliers = [g for g in groups if g.signature != dominant.signature]

        return build_aggregation_result(
            method=AggregationMethod.SIGNATURE,
            dominant=dominant,
            outliers=outliers,
            topology=topology,
            rank_to_machine=rank_to_machine,
            min_outlier_coverage=self.min_outlier_coverage,
        )

    def _cluster(self, snapshots: list[StackSnapshot]) -> list[StackGroup]:
        if not self.fuzzy_match:
            buckets: dict[str, list[StackSnapshot]] = defaultdict(list)
            for snap in snapshots:
                buckets[snap.signature(self.signature_depth)].append(snap)
            return [
                StackGroup(signature=sig, snapshots=items)
                for sig, items in buckets.items()
            ]

        buckets: dict[str, list[StackSnapshot]] = defaultdict(list)
        for snap in snapshots:
            key = self._fuzzy_key(snap)
            buckets[key].append(snap)

        groups: list[StackGroup] = []
        for key, items in buckets.items():
            rep = max(items, key=lambda s: len(s.signature(self.signature_depth)))
            groups.append(
                StackGroup(
                    signature=rep.signature(self.signature_depth),
                    snapshots=items,
                )
            )
        return groups

    def _fuzzy_key(self, snap: StackSnapshot) -> str:
        funcs = [frame.function for frame in snap.frames[: self.signature_depth]]
        return " | ".join(funcs)

    @staticmethod
    def diagnose_hang_pattern(result: AggregationResult) -> list[str]:
        """Heuristic labels for common distributed-training hang signatures."""
        hints: list[str] = []
        text = " ".join(
            [result.dominant_signature]
            + [group.signature for group in result.outlier_groups]
        ).lower()

        patterns = Counter(
            {
                "backward_collective_hang": any(
                    token in text
                    for token in (
                        "all_gather_into_tensor",
                        "all_gather",
                        "reduce_scatter",
                        "broadcast",
                    )
                ),
                "p2p_gradient_hang": any(
                    token in text for token in ("isend", "irecv", "batch_isend_irecv")
                ),
                "optimizer_sync": any(
                    token in text
                    for token in ("step", "optimizer", "allreduce", "distributed")
                ),
                "dataloader_block": "dataloader" in text or "fetch" in text,
                "checkpoint_io": "checkpoint" in text or "save" in text,
            }
        )

        for name, matched in patterns.items():
            if matched:
                hints.append(name)
        return hints
