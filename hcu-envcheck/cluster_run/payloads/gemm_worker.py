# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Launcher-aware per-rank HCU GEMM worker."""

from __future__ import annotations

import argparse
import time

from ._torch_common import configure_direct_mpi_environment, emit, load_torch, runtime_topology


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--size", type=int, default=4096)
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--dtype", choices=("float16", "bfloat16", "float32"), default="float16")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.size < 1 or args.iterations < 1:
        raise SystemExit("--size and --iterations must be positive")
    torch = load_torch()
    topology = configure_direct_mpi_environment()
    if not torch.cuda.is_available():
        raise RuntimeError("torch.cuda is unavailable after env.sh")
    local_rank = int(topology["local_rank"])
    dtype = getattr(torch, args.dtype)
    device = torch.device("cuda", local_rank)
    left = torch.randn((args.size, args.size), device=device, dtype=dtype)
    right = torch.randn((args.size, args.size), device=device, dtype=dtype)
    torch.cuda.synchronize(device)
    started = time.perf_counter()
    for _ in range(args.iterations):
        torch.mm(left, right)
    torch.cuda.synchronize(device)
    elapsed = time.perf_counter() - started
    tflops = (2 * args.size**3 * args.iterations) / max(elapsed, 1e-9) / 1e12
    result = {
        "test": "gemm",
        "status": "PASS",
        "size": args.size,
        "iterations": args.iterations,
        "dtype": args.dtype,
        "tflops": tflops,
        "topology": topology,
    }
    emit(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

