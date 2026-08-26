# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections import defaultdict

from .models import ProcessRole, StackSnapshot

# Prefer trainer stacks when multiple processes exist on the same rank.
_ROLE_PRIORITY: dict[ProcessRole, int] = {
    ProcessRole.TRAINER: 0,
    ProcessRole.DATALOADER: 1,
    ProcessRole.CHECKPOINT: 2,
    ProcessRole.OTHER: 3,
}


def select_primary_snapshots(
    snapshots: list[StackSnapshot],
    *,
    one_per_rank: bool = True,
    prefer_role: ProcessRole = ProcessRole.TRAINER,
) -> list[StackSnapshot]:
    """
    Prepare snapshots for aggregation (step 1 output).

    When one_per_rank is True, keep a single representative stack per global
    rank (trainer preferred). Otherwise return all training-related stacks.
    """
    if not one_per_rank:
        return list(snapshots)

    by_rank: dict[int, list[StackSnapshot]] = defaultdict(list)
    unranked: list[StackSnapshot] = []

    for snap in snapshots:
        if snap.rank >= 0:
            by_rank[snap.rank].append(snap)
        else:
            unranked.append(snap)

    selected: list[StackSnapshot] = []
    for rank, group in by_rank.items():
        selected.append(_pick_best(group, prefer_role))

    # Unranked launchers/helpers must not participate in distributed rank
    # clustering. Only use machine-level fallback when no rank was discovered.
    if by_rank:
        return selected

    # Fall back to machine_id for genuinely unranked/single-GPU captures.
    by_machine: dict[str, list[StackSnapshot]] = defaultdict(list)
    for snap in unranked:
        by_machine[snap.machine_id].append(snap)

    for group in by_machine.values():
        selected.append(_pick_best(group, prefer_role))

    return selected


def _pick_best(
    group: list[StackSnapshot],
    prefer_role: ProcessRole,
) -> StackSnapshot:
    def sort_key(snap: StackSnapshot) -> tuple[int, int, int]:
        role_rank = _ROLE_PRIORITY.get(snap.role, 99)
        prefer_bonus = 0 if snap.role == prefer_role else 1
        return (prefer_bonus, role_rank, -len(snap.frames))

    return min(group, key=sort_key)
