# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Launcher-aware RCCL collective worker.

This is a small active smoke/bandwidth test using the PyTorch RCCL backend.
The existing ``rccl_perf_test.sh`` remains the full rccl-tests payload for the
legacy entry point or an explicit ``--script`` invocation.
"""

from __future__ import annotations

import argparse
import time

from ._torch_common import (
    configure_direct_mpi_environment,
    emit,
    init_process_group,
    load_torch,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bytes", type=int, default=4 * 1024 * 1024)
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--warmup", type=int, default=5)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.bytes < 1 or args.iterations < 1 or args.warmup < 0:
        raise SystemExit("--bytes and --iterations must be positive; --warmup cannot be negative")
    topology = configure_direct_mpi_environment()
    if int(topology["world_size"]) < 2:
        emit({"test": "rccl", "status": "INCOMPLETE", "topology": topology,
              "message": "at least two ranks are required for a collective test"})
        return 2
    torch = load_torch()
    local_rank = int(topology["local_rank"])
    if not torch.cuda.is_available():
        raise RuntimeError("torch.cuda is unavailable after env.sh")
    device = torch.device("cuda", local_rank)
    elements = max(1, (args.bytes + 3) // 4)
    tensor = torch.ones(elements, device=device, dtype=torch.float32)
    initialized = init_process_group(torch, topology)
    distributed = torch.distributed if initialized else None
    try:
        for _ in range(args.warmup):
            distributed.all_reduce(tensor)
        torch.cuda.synchronize(device)
        started = time.perf_counter()
        for _ in range(args.iterations):
            distributed.all_reduce(tensor)
        torch.cuda.synchronize(device)
        elapsed = time.perf_counter() - started
        result = {
            "test": "rccl",
            "status": "PASS",
            "bytes": args.bytes,
            "iterations": args.iterations,
            "seconds": elapsed,
            "topology": topology,
            "backend": "nccl",
        }
        if int(topology["rank"]) == 0:
            emit(result)
    finally:
        if distributed is not None and distributed.is_initialized():
            distributed.destroy_process_group()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

