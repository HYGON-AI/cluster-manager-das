# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Small runtime helpers shared by launcher-aware PyTorch workers."""

from __future__ import annotations

import json
import os
import socket
from typing import Any


# OpenMPI does not prefix every launcher name uniformly: RANK/LOCAL_RANK keep
# their names, while WORLD_SIZE is published as OMPI_COMM_WORLD_SIZE.
_OMPI_ENV_KEYS = {
    "RANK": "OMPI_COMM_WORLD_RANK",
    "WORLD_SIZE": "OMPI_COMM_WORLD_SIZE",
    "LOCAL_RANK": "OMPI_COMM_WORLD_LOCAL_RANK",
}


def rank_value(name: str, fallback: int = 0) -> int:
    for key in (name, f"HCU_{name}", _OMPI_ENV_KEYS.get(name, "OMPI_COMM_WORLD_" + name)):
        value = os.environ.get(key)
        if value is None:
            continue
        try:
            return int(value)
        except ValueError:
            continue
    return fallback


def runtime_topology() -> dict[str, int | str]:
    rank = rank_value("RANK")
    world_size = rank_value("WORLD_SIZE", 1)
    local_rank = rank_value("LOCAL_RANK")
    return {
        "rank": rank,
        "world_size": world_size,
        "local_rank": local_rank,
        "host": socket.gethostname(),
    }


def emit(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)


def configure_direct_mpi_environment() -> dict[str, int | str]:
    """Map MPI rank variables to the env:// names used by torch.distributed."""

    topology = runtime_topology()
    os.environ.setdefault("RANK", str(topology["rank"]))
    os.environ.setdefault("WORLD_SIZE", str(topology["world_size"]))
    os.environ.setdefault("LOCAL_RANK", str(topology["local_rank"]))
    os.environ.setdefault("MASTER_ADDR", os.environ.get("HCU_MASTER_ADDR", "127.0.0.1"))
    os.environ.setdefault("MASTER_PORT", os.environ.get("HCU_MASTER_PORT", "29500"))
    return topology


def load_torch():
    try:
        import torch
    except Exception as exc:  # pragma: no cover - depends on target env
        raise RuntimeError(f"cannot import torch after env.sh: {exc}") from exc
    return torch


def init_process_group(torch, topology: dict[str, int | str], backend: str = "nccl") -> bool:
    if int(topology["world_size"]) <= 1:
        return False
    distributed = getattr(torch, "distributed", None)
    if distributed is None or not distributed.is_available():
        raise RuntimeError("torch.distributed is unavailable")
    distributed.init_process_group(
        backend=backend,
        rank=int(topology["rank"]),
        world_size=int(topology["world_size"]),
        init_method="env://",
    )
    return True

