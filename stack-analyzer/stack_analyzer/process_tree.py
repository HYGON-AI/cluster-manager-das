# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import re
from typing import Iterable

from .models import ProcessRole, ProcessSnapshot

TRAINING_CMD_PATTERNS: tuple[tuple[re.Pattern[str], ProcessRole], ...] = (
    (re.compile(r"torchrun|torch\.distributed\.run", re.I), ProcessRole.TRAINER),
    (re.compile(r"train\.py|pretrain|finetune|megatron|deepspeed", re.I), ProcessRole.TRAINER),
    (re.compile(r"dataloader|DataLoader|webdataset|prefetch", re.I), ProcessRole.DATALOADER),
    (re.compile(r"checkpoint|ByteCheckpoint|save_ckpt|load_ckpt", re.I), ProcessRole.CHECKPOINT),
)


def classify_process(cmdline: str) -> ProcessRole:
    for pattern, role in TRAINING_CMD_PATTERNS:
        if pattern.search(cmdline):
            return role
    return ProcessRole.OTHER


def is_training_related(snapshot: ProcessSnapshot) -> bool:
    return snapshot.role != ProcessRole.OTHER


def discover_training_processes(
    processes: Iterable[ProcessSnapshot],
    *,
    include_other_children_of_trainer: bool = True,
) -> list[ProcessSnapshot]:
    """
    Step 1: parse process trees and keep training-related PIDs
    (torchrun, dataloader, checkpoint workers).
    """
    proc_list = list(processes)
    by_ppid: dict[int, list[ProcessSnapshot]] = {}
    trainers: list[ProcessSnapshot] = []

    for proc in proc_list:
        by_ppid.setdefault(proc.ppid, []).append(proc)
        if proc.role == ProcessRole.TRAINER:
            trainers.append(proc)

    selected: dict[int, ProcessSnapshot] = {
        proc.pid: proc for proc in proc_list if is_training_related(proc)
    }

    if include_other_children_of_trainer:
        queue = [trainer.pid for trainer in trainers]
        while queue:
            parent_pid = queue.pop()
            for child in by_ppid.get(parent_pid, []):
                if child.pid not in selected:
                    selected[child.pid] = child
                    queue.append(child.pid)

    return list(selected.values())


def list_local_processes_psutil() -> list[ProcessSnapshot]:
    """Enumerate local processes using psutil (optional dependency)."""
    try:
        import psutil
    except ImportError as exc:
        raise RuntimeError("psutil is required for local process discovery") from exc

    machine_id = _local_machine_id()
    snapshots: list[ProcessSnapshot] = []

    for proc in psutil.process_iter(["pid", "ppid", "cmdline"]):
        try:
            info = proc.info
            cmd_parts = info.get("cmdline") or []
            cmdline = " ".join(cmd_parts)
            rank = _extract_rank(cmdline)
            snapshots.append(
                ProcessSnapshot(
                    machine_id=machine_id,
                    rank=rank,
                    pid=int(info["pid"]),
                    ppid=int(info["ppid"] or 0),
                    cmdline=cmdline,
                    role=classify_process(cmdline),
                )
            )
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    return snapshots


def _extract_rank(cmdline: str) -> int:
    match = re.search(r"(?:LOCAL_RANK|RANK)[= ](\d+)", cmdline, re.I)
    if match:
        return int(match.group(1))
    match = re.search(r"--local_rank[= ](\d+)", cmdline, re.I)
    if match:
        return int(match.group(1))
    return -1


def _local_machine_id() -> str:
    import socket

    return socket.gethostname()
