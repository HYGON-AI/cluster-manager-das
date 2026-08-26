# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass, field

from .models import AggregationMethod, ParallelTopology, StackSnapshot


@dataclass
class StackGroup:
    signature: str
    snapshots: list[StackSnapshot] = field(default_factory=list)

    @property
    def machines(self) -> set[str]:
        return {snap.machine_id for snap in self.snapshots}

    @property
    def ranks(self) -> set[int]:
        return {snap.rank for snap in self.snapshots if snap.rank >= 0}


@dataclass
class AggregationResult:
    dominant_signature: str
    dominant_group: StackGroup
    outlier_groups: list[StackGroup]
    outlier_machines: set[str]
    outlier_ranks: set[int]
    isolation_group: tuple[str, frozenset[int]] | None
    machines_to_evict: set[str]
    method: AggregationMethod = AggregationMethod.SIGNATURE

    def summary(self) -> str:
        lines = [
            "=== Stack Aggregation ===",
            f"Method: {self.method.value}",
            f"Dominant (healthy) signature: {self.dominant_signature[:120]}...",
            f"Healthy machines ({len(self.dominant_group.machines)}): "
            f"{sorted(self.dominant_group.machines)}",
            f"Outlier machines ({len(self.outlier_machines)}): "
            f"{sorted(self.outlier_machines)}",
        ]
        if self.isolation_group:
            label, group = self.isolation_group
            lines.append(
                f"Shared parallel group to isolate: {label} ranks {sorted(group)}"
            )
        lines.append(
            f"Recommended eviction: {sorted(self.machines_to_evict)}"
        )
        for idx, group in enumerate(self.outlier_groups, start=1):
            lines.append(
                f"\n--- Outlier group #{idx} ({len(group.snapshots)} stacks) ---"
            )
            lines.append(group.signature[:500])
            lines.append(f"Machines: {sorted(group.machines)}")
        return "\n".join(lines)


def build_aggregation_result(
    *,
    method: AggregationMethod,
    dominant: StackGroup,
    outliers: list[StackGroup],
    topology: ParallelTopology | None,
    rank_to_machine: dict[int, str] | None,
    min_outlier_coverage: float,
) -> AggregationResult:
    outlier_machines: set[str] = set()
    outlier_ranks: set[int] = set()
    for group in outliers:
        outlier_machines |= group.machines
        outlier_ranks |= group.ranks

    isolation_group = None
    machines_to_evict = set(outlier_machines)

    if topology and outlier_ranks:
        isolation_group = topology.shared_group(
            outlier_ranks, min_coverage=min_outlier_coverage
        )
        if isolation_group and rank_to_machine:
            _, group_ranks = isolation_group
            machines_to_evict = {
                rank_to_machine[rank]
                for rank in group_ranks
                if rank in rank_to_machine
            }

    return AggregationResult(
        method=method,
        dominant_signature=dominant.signature,
        dominant_group=dominant,
        outlier_groups=outliers,
        outlier_machines=outlier_machines,
        outlier_ranks=outlier_ranks,
        isolation_group=isolation_group,
        machines_to_evict=machines_to_evict,
    )
