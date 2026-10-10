# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Active-test registry and launcher-aware default commands.

RCCL defaults to the existing binary payload, which owns its MPI launch.
GEMM defaults to a small module worker; its rocBLAS payload is node-local.
Explicit ``--profile worker`` selects launcher-aware Python workers.
``script/custom --script`` runs ordinary programs; a worker profile's
``--script`` must implement the per-rank contract without nested launchers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class TestSuiteDefinition:
    name: str
    worker_module: str
    description: str
    legacy_payload: str | None = None


TEST_SUITES = {
    "rccl": TestSuiteDefinition(
        name="rccl",
        worker_module="cluster_run.payloads.rccl_worker",
        description="PyTorch/RCCL collective smoke and bandwidth test",
        legacy_payload="cluster_run/payloads/rccl_perf_test.sh",
    ),
    "gemm": TestSuiteDefinition(
        name="gemm",
        worker_module="cluster_run.payloads.gemm_worker",
        description="per-rank HCU GEMM throughput test",
        legacy_payload="cluster_run/payloads/gemm_perf_test.sh",
    ),
}

PROFILES = ("worker", "rccl-tests", "rocblas")


def default_profile(test_name: str) -> str:
    """RCCL preserves the established MPI -> *_perf binary benchmark."""
    return "rccl-tests" if test_name == "rccl" else "worker"


def default_processes(profile: str) -> int:
    return 8 if profile == "rccl-tests" else 1


def validate_profile(test_name: str, profile: str, script: str | None) -> None:
    if profile not in PROFILES:
        raise ValueError(f"unknown test profile: {profile}")
    if profile == "rccl-tests" and test_name != "rccl":
        raise ValueError("--profile rccl-tests requires rccl")
    if profile == "rocblas" and test_name != "gemm":
        raise ValueError("--profile rocblas requires gemm")
    if profile != "worker" and script:
        raise ValueError("binary profiles cannot use --script; use --script-arg for benchmark options, or --profile worker for a per-rank script")


def resolve_test_command(
    test_name: str,
    launcher: str,
    *,
    script: str | None,
    script_args: Sequence[str],
    python_executable: str = "python3",
) -> tuple[str, ...]:
    """Resolve a launcher-compatible command.

    ``--script`` is an explicit escape hatch and is always preferred.  The
    built-in defaults are Python modules so that ``mpirun-torchrun``,
    ``ssh-torchrun`` and direct ``mpirun`` all receive rank information in a
    consistent way.
    """

    torchrun_launcher = launcher in {"mpirun-torchrun", "ssh-torchrun"}
    if script:
        # torchrun normally prepends the activated Python interpreter.  A
        # Python file is therefore passed directly; a shell payload uses
        # torchrun's --no-python escape hatch.  Direct mpirun keeps the
        # explicit interpreter/shell command used by legacy payloads.
        if torchrun_launcher and str(script).lower().endswith(".py"):
            command = [script]
        elif torchrun_launcher:
            command = ["--no-python", "bash", script]
        elif str(script).lower().endswith(".py"):
            command = [python_executable, script]
        else:
            command = ["bash", script]
    else:
        try:
            definition = TEST_SUITES[test_name]
        except KeyError as exc:
            raise ValueError(f"active test {test_name!r} requires --script") from exc
        command = (
            ["-m", definition.worker_module]
            if torchrun_launcher
            else [python_executable, "-m", definition.worker_module]
        )
    return tuple(command + [str(item) for item in script_args])


def suite_definition(test_name: str) -> TestSuiteDefinition | None:
    return TEST_SUITES.get(test_name)


