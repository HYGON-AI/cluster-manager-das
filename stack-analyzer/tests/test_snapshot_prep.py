# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from stack_analyzer.models import ProcessRole, StackFrame, StackSnapshot
from stack_analyzer.snapshot_prep import select_primary_snapshots


def snapshot(machine, rank, pid, role, depth):
    return StackSnapshot(
        machine_id=machine,
        rank=rank,
        pid=pid,
        role=role,
        frames=tuple(StackFrame(f"frame-{index}") for index in range(depth)),
    )


def test_primary_snapshot_prefers_trainer_per_rank():
    snapshots = [
        snapshot("node-a", 0, 10, ProcessRole.DATALOADER, 10),
        snapshot("node-a", 0, 11, ProcessRole.TRAINER, 2),
        snapshot("node-b", 1, 20, ProcessRole.TRAINER, 3),
        snapshot("launcher", -1, 1, ProcessRole.OTHER, 20),
    ]

    selected = select_primary_snapshots(snapshots)

    assert {(item.rank, item.pid) for item in selected} == {(0, 11), (1, 20)}


def test_unranked_capture_falls_back_to_one_snapshot_per_machine():
    snapshots = [
        snapshot("node-a", -1, 10, ProcessRole.OTHER, 1),
        snapshot("node-a", -1, 11, ProcessRole.CHECKPOINT, 2),
        snapshot("node-b", -1, 20, ProcessRole.TRAINER, 1),
    ]

    selected = select_primary_snapshots(snapshots)

    assert {item.pid for item in selected} == {11, 20}


def test_selection_can_return_all_snapshots():
    snapshots = [snapshot("node-a", 0, 10, ProcessRole.TRAINER, 1)]
    assert select_primary_snapshots(snapshots, one_per_rank=False) == snapshots
