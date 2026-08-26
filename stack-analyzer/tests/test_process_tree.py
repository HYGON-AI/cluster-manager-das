# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from stack_analyzer.models import ProcessRole, ProcessSnapshot
from stack_analyzer.process_tree import (
    _extract_rank,
    classify_process,
    discover_training_processes,
)


def process(pid, ppid, cmdline, role=None):
    return ProcessSnapshot(
        machine_id="node-a",
        rank=_extract_rank(cmdline),
        pid=pid,
        ppid=ppid,
        cmdline=cmdline,
        role=role or classify_process(cmdline),
    )


def test_process_classification_and_rank_extraction():
    assert classify_process("python -m torch.distributed.run train.py") == ProcessRole.TRAINER
    assert classify_process("python dataloader_worker.py") == ProcessRole.DATALOADER
    assert classify_process("python checkpoint_writer.py") == ProcessRole.CHECKPOINT
    assert classify_process("sleep 10") == ProcessRole.OTHER
    assert _extract_rank("RANK=7 python train.py") == 7
    assert _extract_rank("python train.py --local_rank 3") == 3
    assert _extract_rank("python train.py") == -1


def test_discovery_keeps_training_processes_and_trainer_descendants():
    snapshots = [
        process(10, 1, "torchrun train.py RANK=0"),
        process(11, 10, "python helper.py"),
        process(12, 11, "python nested-helper.py"),
        process(20, 1, "sleep 10"),
    ]

    selected = discover_training_processes(snapshots)

    assert {item.pid for item in selected} == {10, 11, 12}


def test_discovery_can_exclude_unrelated_children():
    snapshots = [
        process(10, 1, "torchrun train.py"),
        process(11, 10, "python helper.py"),
    ]

    selected = discover_training_processes(
        snapshots, include_other_children_of_trainer=False
    )

    assert [item.pid for item in selected] == [10]
