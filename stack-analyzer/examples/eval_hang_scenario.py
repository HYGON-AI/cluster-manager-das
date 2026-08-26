# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Synthetic evaluation hang scenario."""

from stack_analyzer.models import (
    ParallelTopology,
    ProcessRole,
    StackFrame,
    StackSnapshot,
)


def build_eval_hang_snapshots() -> list[StackSnapshot]:
    snapshots = []
    for rank in range(12):
        if rank in {4, 6}:
            frames = (
                StackFrame("evaluation_step", "evaluate.py", 180),
                StackFrame("broadcast", "torch/distributed.py", 420),
            )
        else:
            frames = (
                StackFrame("training_step", "training.py", 300),
                StackFrame("all_reduce", "torch/distributed.py", 400),
            )
        snapshots.append(
            StackSnapshot(
                machine_id=f"machine-{rank}",
                rank=rank,
                pid=2000 + rank,
                role=ProcessRole.TRAINER,
                frames=frames,
            )
        )
    return snapshots


def build_eval_hang_topology() -> ParallelTopology:
    return ParallelTopology(
        pp_groups=[
            frozenset(range(0, 12, 2)),
            frozenset(range(1, 12, 2)),
        ]
    )
