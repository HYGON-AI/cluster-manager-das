# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Synthetic pipeline-parallel hang scenario."""

from stack_analyzer.models import (
    ParallelTopology,
    ProcessRole,
    StackFrame,
    StackSnapshot,
)


_OUTLIER_RANKS = {3, 7, 11, 15}


def build_demo_snapshots() -> list[StackSnapshot]:
    snapshots = []
    for rank in range(16):
        if rank in _OUTLIER_RANKS:
            frames = (
                StackFrame("recv_backward", "pipeline.py", 220),
                StackFrame("irecv", "torch/distributed.py", 410),
            )
        else:
            frames = (
                StackFrame("optimizer_step", "training.py", 320),
                StackFrame("all_reduce", "torch/distributed.py", 400),
            )
        snapshots.append(
            StackSnapshot(
                machine_id=f"machine-{rank}",
                rank=rank,
                pid=1000 + rank,
                role=ProcessRole.TRAINER,
                frames=frames,
            )
        )
    return snapshots


def build_demo_topology() -> ParallelTopology:
    return ParallelTopology(
        pp_groups=[
            frozenset({offset, offset + 4, offset + 8, offset + 12})
            for offset in range(4)
        ]
    )
