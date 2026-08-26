# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from stack_analyzer.scripts import remote_capture


def proc(pid, ppid, cmdline, rank):
    return {
        "pid": pid,
        "ppid": ppid,
        "cmdline": cmdline,
        "environ": {"RANK": str(rank)},
        "role": "trainer",
    }


def test_select_rank_worker_roots_excludes_launcher_and_rank_children():
    processes = [
        proc(10, 1, "python torchrun train.py", 0),
        proc(20, 10, "python pretrain_gpt.py", 0),
        proc(30, 20, "python pretrain_gpt.py checkpoint child", 0),
        proc(21, 10, "python pretrain_gpt.py", 1),
    ]
    roots = remote_capture.select_rank_worker_roots(processes, rank_start=0)
    assert [(item["pid"], item["resolved_rank"]) for item in roots] == [
        (20, 0),
        (21, 1),
    ]
