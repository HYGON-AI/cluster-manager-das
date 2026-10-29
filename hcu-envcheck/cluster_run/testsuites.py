# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""RCCL and IB active-test profile validation."""

from __future__ import annotations


def default_profile(test_name: str) -> str | None:
    """RCCL preserves the established MPI -> *_perf binary benchmark."""
    return "rccl-tests" if test_name == "rccl" else None


def default_processes(profile: str | None) -> int:
    return 8 if profile == "rccl-tests" else 1


def validate_profile(test_name: str, profile: str | None) -> None:
    if test_name not in {"rccl", "ib-write-bw"}:
        raise ValueError(f"unsupported active test: {test_name}")
    if profile != default_profile(test_name):
        raise ValueError("RCCL supports only rccl-tests; other operations do not accept --profile")
