# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Opt-in active RDMA and RCCL validation for an existing Slurm allocation.

This module is intentionally separate from the read-only preflight evaluator.
It never compiles, installs, resets, or launches a workload directly on the
controller.  Every data-plane command is an ``srun`` job step pinned to nodes
that belong to a caller-owned, RUNNING allocation.

The public API is deliberately fail-closed:

* ``enabled`` and ``confirm_allocation_idle`` must both be true;
* test nodes must be explicit members of the allocation;
* the controller/login host cannot be a test node;
* unexpected active job steps block the check by default;
* Docker RCCL checks require an explicit existing container and never create or
  auto-select one; and
* a zero return code without backend-specific proof is ``NOT_VERIFIED``.

The module does not change the static IB/RoCE classification in ``rdma.py``.
It answers the separate runtime questions "can verbs traffic cross this pair?"
and "did a multi-node RCCL collective complete over an RDMA transport?".
"""

from __future__ import annotations

import getpass
import json
import math
import os
import re
import socket
import subprocess
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import __version__
from .output import atomic_write_text_exclusive, claim_output_directory
from .rccl_output import parse_rccl_tests_output


ACTIVE_STATUSES = {"PASS", "FAIL", "NOT_VERIFIED"}
MAX_ACTIVE_NODES = 16
ACTIVE_OUTPUT_CAPTURE_LIMIT_BYTES = 8 * 1024 * 1024
ACTIVE_OUTPUT_READ_CHUNK_BYTES = 64 * 1024
FORMAL_SLURM_SAFETY_BOUNDARY = "EXCLUSIVE_SLURM_ALLOCATION_AND_STEP"
UNPROVEN_SLURM_SAFETY_BOUNDARY = "SLURM_NODE_EXCLUSIVITY_NOT_PROVEN"
_FORMAL_SLURM_EXCLUSIVE_MODE = "NODE"
_FOREIGN_JOB_ACTIVE_STATES = "RUNNING,COMPLETING,CONFIGURING,SUSPENDED"
_JOB_ID_RE = re.compile(r"^[0-9]+(?:_[0-9]+)?$")
_NODE_RE = re.compile(r"^(?!-)[A-Za-z0-9][A-Za-z0-9_.:%-]*$")
_DEVICE_RE = re.compile(r"^[A-Za-z0-9_.:-]+$")
_ENV_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")
_CONTAINER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_PYTHON_BASENAME_RE = re.compile(r"^python(?:3(?:\.[0-9]{1,2})?)?$")
_ALLOWED_RCCL_ENV = {
    "NCCL_DEBUG",
    "NCCL_DEBUG_SUBSYS",
    "NCCL_IB_DISABLE",
    "NCCL_IB_HCA",
    "NCCL_IB_GID_INDEX",
    "NCCL_IB_TC",
    "NCCL_IB_TIMEOUT",
    "NCCL_IB_RETRY_CNT",
    "NCCL_IB_QPS_PER_CONNECTION",
    "NCCL_CROSS_NIC",
    "NCCL_SOCKET_IFNAME",
    "NCCL_DMABUF_ENABLE",
    "NCCL_NET_GDR_LEVEL",
    "NCCL_NET_GDR_READ",
    "NCCL_NET_PLUGIN",
    "NCCL_PLUGIN_P2P",
    "NCCL_PXN_DISABLE",
    "RCCL_MSCCL_ENABLE",
    "RCCL_PXN_GPU_BALANCE",
    "HSA_FORCE_FINE_GRAIN_PCIE",
    "HIP_VISIBLE_DEVICES",
    "ROCR_VISIBLE_DEVICES",
    "UCX_NET_DEVICES",
    "UCX_TLS",
    "LD_LIBRARY_PATH",
}

_TORCH_RCCL_MARKER = "HCU_ENVCHECK_TORCH_RCCL "
_TORCH_RCCL_SCRIPT = r"""
from datetime import timedelta
import json
import math
import os
import socket

import torch
import torch.distributed as dist

rank = int(os.environ["SLURM_PROCID"])
world = int(os.environ["SLURM_NTASKS"])
local_rank = int(os.environ.get("SLURM_LOCALID", "0"))
node = os.environ.get("SLURMD_NODENAME") or socket.gethostname()
timeout_seconds = int(os.environ["HCU_ENVCHECK_TORCH_TIMEOUT"])
torch.cuda.set_device(local_rank)
dist.init_process_group(
    backend="nccl",
    init_method="env://",
    rank=rank,
    world_size=world,
    timeout=timedelta(seconds=timeout_seconds),
)
value = torch.tensor([float(rank + 1)], device=torch.device("cuda", local_rank))
dist.all_reduce(value, op=dist.ReduceOp.SUM)
actual = float(value.item())
expected = float(world * (world + 1) // 2)
correct = abs(actual - expected) <= 1.0e-5
print(
    "HCU_ENVCHECK_TORCH_RCCL "
    + json.dumps(
        {
            "rank": rank,
            "world": world,
            "node": node,
            "value": actual,
            "expected": expected,
            "correct": correct,
        },
        sort_keys=True,
        separators=(",", ":"),
    ),
    flush=True,
)
dist.destroy_process_group()
if not correct:
    raise RuntimeError("all_reduce correctness mismatch")
""".strip()

Run = Callable[..., subprocess.CompletedProcess[str]]
Popen = Callable[..., subprocess.Popen[str]]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _host_key(value: str) -> str:
    return value.strip().lower().split(".", 1)[0]


def _safe_node(value: str) -> str:
    node = value.strip()
    if not node or len(node) > 253 or not _NODE_RE.fullmatch(node):
        raise ValueError(f"unsafe or invalid Slurm node: {value!r}")
    return node


def _safe_executable(value: str, allowed_basenames: set[str], label: str) -> str:
    if not value or "\x00" in value or "\n" in value or "\r" in value:
        raise ValueError(f"{label} contains unsafe characters")
    basename = Path(value).name
    if basename not in allowed_basenames:
        raise ValueError(
            f"{label} must name one of {sorted(allowed_basenames)}, got {value!r}"
        )
    return value


@dataclass(frozen=True)
class SlurmActiveContext:
    """Safety and allocation context shared by active checks."""

    job_id: str
    selected_nodes: tuple[str, ...]
    enabled: bool = False
    confirm_allocation_idle: bool = False
    allow_active_steps: bool = False
    unsafe_allow_overlap: bool = False
    max_selected_nodes: int = MAX_ACTIVE_NODES
    current_user: str | None = None
    controller_hostname: str | None = None
    srun_executable: str = "srun"
    squeue_executable: str = "squeue"
    scontrol_executable: str = "scontrol"
    control_timeout_seconds: float = 20.0

    def validate(self) -> None:
        if not _JOB_ID_RE.fullmatch(self.job_id):
            raise ValueError(f"unsafe Slurm job id: {self.job_id!r}")
        if len(self.selected_nodes) < 2:
            raise ValueError("active checks require at least two explicit selected_nodes")
        if not 2 <= self.max_selected_nodes <= 256:
            raise ValueError("max_selected_nodes must be between 2 and 256")
        if len(self.selected_nodes) > self.max_selected_nodes:
            raise ValueError(
                f"active checks support at most {self.max_selected_nodes} selected_nodes per run"
            )
        normalized = [_safe_node(node) for node in self.selected_nodes]
        if len(set(normalized)) != len(normalized):
            raise ValueError("selected_nodes contains duplicates")
        _safe_executable(self.srun_executable, {"srun", "srun.exe"}, "srun_executable")
        _safe_executable(
            self.squeue_executable, {"squeue", "squeue.exe"}, "squeue_executable"
        )
        _safe_executable(
            self.scontrol_executable,
            {"scontrol", "scontrol.exe"},
            "scontrol_executable",
        )
        if self.control_timeout_seconds <= 0 or self.control_timeout_seconds > 120:
            raise ValueError("control_timeout_seconds must be in (0, 120]")


@dataclass(frozen=True)
class VerbsCheckConfig:
    """Bounded two-node perftest configuration."""

    tool: str = "ib_write_bw"
    protocol: str = "ib"
    device: str | None = None
    container_name: str | None = None
    docker_executable: str = "docker"
    ib_port: int = 1
    gid_index: int | None = None
    control_port: int = 18515
    message_bytes: int = 1 << 20
    iterations: int = 1000
    minimum_average_gbps: float | None = None
    startup_grace_seconds: float = 1.0
    command_timeout_seconds: float = 120.0

    def validate(self) -> None:
        _safe_executable(
            self.tool,
            {"ib_write_bw", "ib_send_bw", "ib_read_bw"},
            "verbs tool",
        )
        if self.protocol not in {"ib", "roce"}:
            raise ValueError("protocol must be ib or roce")
        if self.container_name is not None:
            if not _CONTAINER_RE.fullmatch(self.container_name):
                raise ValueError(f"unsafe Docker container name: {self.container_name!r}")
            _safe_executable(
                self.docker_executable, {"docker", "docker.exe"}, "docker_executable"
            )
        if self.device is not None and not _DEVICE_RE.fullmatch(self.device):
            raise ValueError(f"unsafe RDMA device name: {self.device!r}")
        if not 1 <= self.ib_port <= 255:
            raise ValueError("ib_port must be between 1 and 255")
        if self.protocol == "roce" and self.gid_index is None:
            raise ValueError("RoCE verbs checks require an explicit gid_index")
        if self.gid_index is not None and not 0 <= self.gid_index <= 255:
            raise ValueError("gid_index must be between 0 and 255")
        if not 1024 <= self.control_port <= 65535:
            raise ValueError("control_port must be between 1024 and 65535")
        if not 1 <= self.message_bytes <= 64 * 1024 * 1024:
            raise ValueError("message_bytes must be between 1 and 64 MiB")
        if not 1 <= self.iterations <= 100_000:
            raise ValueError("iterations must be between 1 and 100000")
        if self.minimum_average_gbps is not None and (
            not math.isfinite(self.minimum_average_gbps)
            or self.minimum_average_gbps < 0
        ):
            raise ValueError("minimum_average_gbps must be finite and non-negative")
        if not 0 <= self.startup_grace_seconds <= 10:
            raise ValueError("startup_grace_seconds must be between 0 and 10")
        if not 1 <= self.command_timeout_seconds <= 600:
            raise ValueError("command_timeout_seconds must be between 1 and 600")


@dataclass(frozen=True)
class RcclCheckConfig:
    """Bounded multi-node rccl-tests all-reduce configuration."""

    binary: str
    tasks_per_node: int = 1
    devices_per_task: int = 1
    mpi_mode: str = "pmix"
    container_name: str | None = None
    docker_executable: str = "docker"
    minimum_bytes: int = 8 * 1024 * 1024
    maximum_bytes: int = 128 * 1024 * 1024
    step_factor: int = 2
    warmup_iterations: int = 5
    iterations: int = 20
    minimum_average_busbw_gbytes_per_second: float | None = None
    minimum_algbw_gbytes_per_second: float | None = None
    minimum_busbw_gbytes_per_second: float | None = None
    require_rdma_transport: bool = True
    environment: Mapping[str, str] = field(default_factory=dict)
    require_gdr: bool = False
    command_timeout_seconds: float = 300.0

    def validate(self) -> None:
        if self.container_name is not None:
            if not _CONTAINER_RE.fullmatch(self.container_name):
                raise ValueError(f"unsafe Docker container name: {self.container_name!r}")
            _safe_executable(
                self.docker_executable,
                {"docker", "docker.exe"},
                "docker_executable",
            )
        _safe_executable(self.binary, {"all_reduce_perf"}, "RCCL test binary")
        if not 1 <= self.tasks_per_node <= 16:
            raise ValueError("tasks_per_node must be between 1 and 16")
        if not 1 <= self.devices_per_task <= 16:
            raise ValueError("devices_per_task must be between 1 and 16")
        if not 1 <= self.minimum_bytes <= self.maximum_bytes <= 1024 * 1024 * 1024:
            raise ValueError("RCCL byte range must satisfy 1 <= min <= max <= 1 GiB")
        if self.tasks_per_node * self.devices_per_task > 16:
            raise ValueError("tasks_per_node * devices_per_task must not exceed 16")
        if self.mpi_mode not in {"pmix", "pmix_v4", "pmi2"}:
            raise ValueError("mpi_mode must be pmix, pmix_v4, or pmi2")
        if not 2 <= self.step_factor <= 16:
            raise ValueError("step_factor must be between 2 and 16")
        if not 0 <= self.warmup_iterations <= 20:
            raise ValueError("warmup_iterations must be between 0 and 20")
        if not 1 <= self.iterations <= 100:
            raise ValueError("iterations must be between 1 and 100")
        for field_name, threshold in (
            ("minimum_average_busbw_gbytes_per_second", self.minimum_average_busbw_gbytes_per_second),
            ("minimum_algbw_gbytes_per_second", self.minimum_algbw_gbytes_per_second),
            ("minimum_busbw_gbytes_per_second", self.minimum_busbw_gbytes_per_second),
        ):
            if threshold is not None and (
                not math.isfinite(threshold) or threshold <= 0
            ):
                raise ValueError(f"{field_name} must be finite and positive")
        if not 1 <= self.command_timeout_seconds <= 1800:
            raise ValueError("command_timeout_seconds must be between 1 and 1800")
        for name, value in self.environment.items():
            if not _ENV_NAME_RE.fullmatch(name) or name not in _ALLOWED_RCCL_ENV:
                raise ValueError(f"RCCL environment variable is not allowlisted: {name!r}")
            if not isinstance(value, str) or any(ch in value for ch in "\x00\r\n"):
                raise ValueError(f"unsafe RCCL environment value for {name}")

@dataclass(frozen=True)
class TorchRcclCheckConfig:
    """Bounded PyTorch/RCCL fallback when rccl-tests is unavailable.

    The executed Python program is the built-in constant above. Callers cannot
    provide source code or a shell fragment.
    """

    python_binary: str = "python3"
    container_name: str | None = None
    docker_executable: str = "docker"
    master_port: int = 29500
    environment: Mapping[str, str] = field(default_factory=dict)
    command_timeout_seconds: float = 300.0

    def validate(self) -> None:
        if not self.python_binary or any(
            ch in self.python_binary for ch in "\x00\r\n"
        ):
            raise ValueError("python_binary contains unsafe characters")
        if not _PYTHON_BASENAME_RE.fullmatch(Path(self.python_binary).name):
            raise ValueError(
                "python_binary basename must be python, python3, or python3.<minor>"
            )
        if self.container_name is not None:
            if not _CONTAINER_RE.fullmatch(self.container_name):
                raise ValueError(
                    f"unsafe Docker container name: {self.container_name!r}"
                )
            _safe_executable(
                self.docker_executable,
                {"docker", "docker.exe"},
                "docker_executable",
            )
        if not 1024 <= self.master_port <= 65535:
            raise ValueError("master_port must be between 1024 and 65535")
        if not 1 <= self.command_timeout_seconds <= 1800:
            raise ValueError("command_timeout_seconds must be between 1 and 1800")
        for name, value in self.environment.items():
            if not _ENV_NAME_RE.fullmatch(name) or name not in _ALLOWED_RCCL_ENV:
                raise ValueError(f"RCCL environment variable is not allowlisted: {name!r}")
            if not isinstance(value, str) or any(ch in value for ch in "\x00\r\n"):
                raise ValueError(f"unsafe RCCL environment value for {name}")

@dataclass(frozen=True)
class SlurmNodeCapacityEvidence:
    node: str
    state: str
    cpu_alloc: int
    cpu_effective: int | None
    cpu_total: int
    configured_hcus: int
    allocated_hcus: int
    full_capacity_proven: bool


@dataclass(frozen=True)
class SlurmJobCapacityEvidence:
    num_nodes: int
    num_cpus: int
    allocated_cpus: int
    allocated_hcus: int
    selected_node_count: int
    selected_cpu_capacity: int
    selected_hcu_capacity: int
    full_capacity_proven: bool


@dataclass(frozen=True)
class SlurmAllocation:
    job_id: str
    owner: str
    state: str
    nodes: tuple[str, ...]
    active_steps: tuple[str, ...]
    exclusive_mode: str | None = None
    oversubscribe_mode: str | None = None
    exclusivity_proof_source: str | None = None
    node_capacity_evidence: tuple[SlurmNodeCapacityEvidence, ...] = ()
    job_capacity_evidence: SlurmJobCapacityEvidence | None = None
    foreign_active_job_ids: tuple[str, ...] = ()
    node_exclusivity_proven: bool = False


@dataclass
class ActiveCommandEvidence:
    role: str
    node_scope: list[str]
    argv: list[str]
    returncode: int
    duration_seconds: float
    timed_out: bool = False
    stdout_path: str | None = None
    stderr_path: str | None = None
    stdout_total_bytes: int = 0
    stderr_total_bytes: int = 0
    stdout_truncated: bool = False
    stderr_truncated: bool = False


@dataclass
class ActiveCheckResult:
    status: str
    backend: str
    reason_code: str
    message: str
    job_id: str
    nodes: list[str]
    started_at: str
    finished_at: str
    evidence_dir: str
    requested_protocol: str | None = None
    data_transport: str | None = None
    container_name: str | None = None
    metrics: dict[str, Any] = field(default_factory=dict)
    root_cause_candidates: list[str] = field(default_factory=list)
    evidence_markers: list[str] = field(default_factory=list)
    commands: list[ActiveCommandEvidence] = field(default_factory=list)
    allocation: dict[str, Any] | None = None
    tool_version: str = __version__

    def to_dict(self) -> dict[str, Any]:
        if self.status not in ACTIVE_STATUSES:
            raise ValueError(f"invalid active check status: {self.status}")
        return asdict(self)


class ActiveCheckSafetyError(RuntimeError):
    def __init__(self, reason_code: str, message: str):
        super().__init__(message)
        self.reason_code = reason_code


class _BoundedByteCapture:
    """Drain one subprocess pipe while retaining only a bounded head and tail."""

    def __init__(self, limit_bytes: int = ACTIVE_OUTPUT_CAPTURE_LIMIT_BYTES):
        if limit_bytes < 2:
            raise ValueError("output capture limit must be at least two bytes")
        self.limit_bytes = limit_bytes
        self.head_limit = limit_bytes // 2
        self.tail_limit = limit_bytes - self.head_limit
        self.head = bytearray()
        self.tail = bytearray()
        self.total_bytes = 0

    def feed(self, chunk: bytes | str) -> None:
        if isinstance(chunk, str):
            chunk = chunk.encode("utf-8", "replace")
        if not chunk:
            return
        self.total_bytes += len(chunk)
        head_needed = self.head_limit - len(self.head)
        if head_needed > 0:
            self.head.extend(chunk[:head_needed])
            chunk = chunk[head_needed:]
        if chunk:
            self.tail.extend(chunk)
            overflow = len(self.tail) - self.tail_limit
            if overflow > 0:
                del self.tail[:overflow]

    @property
    def truncated(self) -> bool:
        return self.total_bytes > self.limit_bytes

    def render(self) -> str:
        head = bytes(self.head).decode("utf-8", "replace")
        tail = bytes(self.tail).decode("utf-8", "replace")
        if not self.truncated:
            return head + tail
        omitted = self.total_bytes - len(self.head) - len(self.tail)
        return f"{head}\n...[HCU_ENVCHECK omitted {omitted} output bytes]...\n{tail}"

    def drain(self, pipe: Any) -> None:
        try:
            while True:
                chunk = pipe.read(ACTIVE_OUTPUT_READ_CHUNK_BYTES)
                if not chunk:
                    break
                self.feed(chunk)
        finally:
            try:
                pipe.close()
            except OSError:
                pass


class SlurmActiveCheckRunner:
    """Inspect an allocation and execute opt-in tests only through `srun`."""

    def __init__(
        self,
        *,
        runner: Run = subprocess.run,
        popen: Popen = subprocess.Popen,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        self._runner = runner
        self._popen = popen
        self._sleep = sleeper

    def _control(self, argv: Sequence[str], timeout: float) -> list[str]:
        try:
            completed = self._runner(
                list(argv),
                stdin=subprocess.DEVNULL,
                text=True,
                encoding="utf-8",
                errors="replace",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ActiveCheckSafetyError(
                "SLURM_CONTROL_QUERY_FAILED", f"cannot run {' '.join(argv)}: {exc}"
            ) from exc
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout or "unknown error").strip()
            raise ActiveCheckSafetyError(
                "SLURM_CONTROL_QUERY_FAILED",
                f"{' '.join(argv)} failed: {detail}",
            )
        return [line.strip() for line in (completed.stdout or "").splitlines() if line.strip()]

    def _inspect_job_allocation_modes(
        self, context: SlurmActiveContext
    ) -> tuple[str | None, str | None, str]:
        """Read scheduler allocation modes without guessing legacy semantics."""

        lines = self._control(
            [
                context.scontrol_executable,
                "show",
                "job",
                "-o",
                context.job_id,
            ],
            context.control_timeout_seconds,
        )
        matching_records = [
            line
            for line in lines
            if (
                (match := re.search(r"(?:^|\s)JobId=([^\s]+)", line))
                and match.group(1) == context.job_id
            )
        ]
        if len(matching_records) != 1:
            raise ActiveCheckSafetyError(
                "SLURM_ALLOCATION_EXCLUSIVE_EVIDENCE_MISSING",
                f"Slurm job {context.job_id} did not provide one exact allocation record",
            )
        record = matching_records[0]
        exclusive_match = re.search(r"(?:^|\s)Exclusive=([^\s]+)", record, re.I)
        oversubscribe_match = re.search(
            r"(?:^|\s)OverSubscribe=([^\s]+)", record, re.I
        )
        exclusive_mode = (
            exclusive_match.group(1).strip().upper() if exclusive_match else None
        )
        oversubscribe_mode = (
            oversubscribe_match.group(1).strip().upper()
            if oversubscribe_match
            else None
        )
        if exclusive_mode is None and oversubscribe_mode is None:
            raise ActiveCheckSafetyError(
                "SLURM_ALLOCATION_EXCLUSIVE_EVIDENCE_MISSING",
                f"Slurm job {context.job_id} reports neither Exclusive nor OverSubscribe",
            )
        return exclusive_mode, oversubscribe_mode, record

    @staticmethod
    def _record_field(record: str, name: str) -> str | None:
        match = re.search(rf"(?:^|\s){re.escape(name)}=([^\s]+)", record)
        return match.group(1).strip() if match else None

    @staticmethod
    def _parse_tres_count(value: str, name: str) -> int | None:
        for item in value.split(","):
            key, separator, raw_count = item.partition("=")
            if not separator or key.strip() != name:
                continue
            match = re.fullmatch(r"(\d+)(?:\([^)]*\))?", raw_count.strip())
            return int(match.group(1)) if match else None
        return None

    def _inspect_legacy_full_node_capacity(
        self, context: SlurmActiveContext, selected_nodes: Sequence[str]
    ) -> tuple[SlurmNodeCapacityEvidence, ...]:
        evidence: list[SlurmNodeCapacityEvidence] = []
        for node in selected_nodes:
            lines = self._control(
                [
                    context.scontrol_executable,
                    "show",
                    "node",
                    "-o",
                    node,
                ],
                context.control_timeout_seconds,
            )
            records = [
                line
                for line in lines
                if (
                    (match := re.search(r"(?:^|\s)NodeName=([^\s]+)", line))
                    and _host_key(match.group(1)) == _host_key(node)
                )
            ]
            if len(records) != 1:
                raise ActiveCheckSafetyError(
                    "SLURM_NODE_CAPACITY_EVIDENCE_MISSING",
                    f"Slurm node {node} did not provide one exact capacity record",
                )
            record = records[0]
            state = self._record_field(record, "State")
            cpu_alloc_text = self._record_field(record, "CPUAlloc")
            cpu_effective_text = self._record_field(record, "CPUEfctv")
            cpu_total_text = self._record_field(record, "CPUTot")
            configured_tres = self._record_field(record, "CfgTRES")
            allocated_tres = self._record_field(record, "AllocTRES")
            try:
                cpu_alloc = int(cpu_alloc_text) if cpu_alloc_text is not None else None
                cpu_effective = (
                    int(cpu_effective_text)
                    if cpu_effective_text is not None
                    else None
                )
                cpu_total = int(cpu_total_text) if cpu_total_text is not None else None
            except ValueError as exc:
                raise ActiveCheckSafetyError(
                    "SLURM_NODE_CAPACITY_EVIDENCE_MISSING",
                    f"Slurm node {node} has an invalid CPU capacity field",
                ) from exc
            configured_hcus = (
                self._parse_tres_count(configured_tres, "gres/hcu")
                if configured_tres is not None
                else None
            )
            allocated_hcus = (
                self._parse_tres_count(allocated_tres, "gres/hcu")
                if allocated_tres is not None
                else None
            )
            required = (
                state,
                cpu_alloc,
                cpu_total,
                configured_tres,
                allocated_tres,
                configured_hcus,
                allocated_hcus,
            )
            if any(value is None for value in required):
                raise ActiveCheckSafetyError(
                    "SLURM_NODE_CAPACITY_EVIDENCE_MISSING",
                    f"Slurm node {node} lacks State/CPU/TRES/HCU capacity evidence",
                )
            full_capacity_proven = bool(
                state.upper() == "ALLOCATED"
                and cpu_alloc > 0
                and cpu_alloc == cpu_total
                and configured_hcus > 0
                and configured_hcus == allocated_hcus
            )
            evidence.append(
                SlurmNodeCapacityEvidence(
                    node=node,
                    state=state,
                    cpu_alloc=cpu_alloc,
                    cpu_effective=cpu_effective,
                    cpu_total=cpu_total,
                    configured_hcus=configured_hcus,
                    allocated_hcus=allocated_hcus,
                    full_capacity_proven=full_capacity_proven,
                )
            )
        return tuple(evidence)

    def _inspect_legacy_job_capacity(
        self,
        job_record: str,
        node_evidence: Sequence[SlurmNodeCapacityEvidence],
        selected_nodes: Sequence[str],
        allocation_nodes: Sequence[str],
    ) -> SlurmJobCapacityEvidence:
        """Prove the target Job itself owns the full selected-node capacity.

        Node totals plus an empty foreign-job query are insufficient when Slurm
        ``PrivateData`` hides other users' jobs. The legacy fallback therefore
        also requires the target Job's own CPU/HCU allocation to equal the
        aggregate configured capacity of the complete Job node set.
        """

        selected_keys = {_host_key(node) for node in selected_nodes}
        allocation_keys = {_host_key(node) for node in allocation_nodes}
        if selected_keys != allocation_keys:
            raise ActiveCheckSafetyError(
                "SLURM_LEGACY_PROOF_REQUIRES_FULL_JOB_NODESET",
                "legacy Slurm exclusivity proof requires selected nodes to equal "
                "the complete Job allocation node set",
            )
        num_nodes_text = self._record_field(job_record, "NumNodes")
        num_cpus_text = self._record_field(job_record, "NumCPUs")
        allocated_tres = self._record_field(job_record, "AllocTRES")
        try:
            num_nodes = int(num_nodes_text) if num_nodes_text is not None else None
            num_cpus = int(num_cpus_text) if num_cpus_text is not None else None
        except ValueError as exc:
            raise ActiveCheckSafetyError(
                "SLURM_JOB_CAPACITY_EVIDENCE_MISSING",
                "Slurm Job has an invalid NumNodes or NumCPUs field",
            ) from exc
        allocated_cpus = (
            self._parse_tres_count(allocated_tres, "cpu")
            if allocated_tres is not None
            else None
        )
        allocated_hcus = (
            self._parse_tres_count(allocated_tres, "gres/hcu")
            if allocated_tres is not None
            else None
        )
        if any(
            value is None
            for value in (num_nodes, num_cpus, allocated_cpus, allocated_hcus)
        ):
            raise ActiveCheckSafetyError(
                "SLURM_JOB_CAPACITY_EVIDENCE_MISSING",
                "legacy Slurm proof requires Job NumNodes/NumCPUs and "
                "AllocTRES cpu/gres/hcu evidence",
            )
        selected_cpu_capacity = sum(item.cpu_total for item in node_evidence)
        selected_hcu_capacity = sum(item.configured_hcus for item in node_evidence)
        selected_node_count = len(node_evidence)
        full_capacity_proven = bool(
            num_nodes == selected_node_count
            and num_cpus == selected_cpu_capacity
            and allocated_cpus == selected_cpu_capacity
            and allocated_hcus == selected_hcu_capacity
        )
        evidence = SlurmJobCapacityEvidence(
            num_nodes=num_nodes,
            num_cpus=num_cpus,
            allocated_cpus=allocated_cpus,
            allocated_hcus=allocated_hcus,
            selected_node_count=selected_node_count,
            selected_cpu_capacity=selected_cpu_capacity,
            selected_hcu_capacity=selected_hcu_capacity,
            full_capacity_proven=full_capacity_proven,
        )
        if not full_capacity_proven:
            raise ActiveCheckSafetyError(
                "SLURM_JOB_NOT_FULL_NODE_CAPACITY",
                "target Slurm Job CPU/HCU allocation does not equal selected-node "
                f"capacity: {asdict(evidence)}",
            )
        return evidence

    def _inspect_foreign_active_jobs(
        self, context: SlurmActiveContext, selected_nodes: Sequence[str]
    ) -> tuple[str, ...]:
        """Find active jobs other than ``context.job_id`` on selected nodes."""

        lines = self._control(
            [
                context.squeue_executable,
                "-h",
                "-w",
                ",".join(selected_nodes),
                "-t",
                _FOREIGN_JOB_ACTIVE_STATES,
                "-o",
                "%i|%T|%u|%N",
            ],
            context.control_timeout_seconds,
        )
        observed_job_ids: set[str] = set()
        foreign_job_ids: set[str] = set()
        for line in lines:
            fields = [field.strip() for field in line.split("|", 3)]
            if len(fields) != 4 or not all(fields):
                raise ActiveCheckSafetyError(
                    "SLURM_FOREIGN_JOB_EVIDENCE_UNPARSEABLE",
                    "cannot parse the active-job occupancy query for selected nodes",
                )
            job_id = fields[0]
            observed_job_ids.add(job_id)
            if job_id != context.job_id:
                foreign_job_ids.add(job_id)
        if context.job_id not in observed_job_ids:
            raise ActiveCheckSafetyError(
                "SLURM_FOREIGN_JOB_EVIDENCE_INCOMPLETE",
                f"active-job occupancy query did not return Slurm job {context.job_id}",
            )
        return tuple(sorted(foreign_job_ids))

    def inspect_allocation(self, context: SlurmActiveContext) -> SlurmAllocation:
        context.validate()
        current_user = context.current_user or getpass.getuser()
        lines = self._control(
            [
                context.squeue_executable,
                "-h",
                "-j",
                context.job_id,
                "-o",
                "%i|%T|%u|%N",
            ],
            context.control_timeout_seconds,
        )
        candidates: list[tuple[str, str, str]] = []
        expressions: list[str] = []
        for line in lines:
            fields = line.split("|", 3)
            if len(fields) != 4:
                continue
            job_id, state, owner, expression = (field.strip() for field in fields)
            if job_id == context.job_id:
                candidates.append((state, owner, expression))
                if expression not in {"", "(null)", "n/a"}:
                    expressions.append(expression)
        if not candidates or not expressions:
            raise ActiveCheckSafetyError(
                "SLURM_ALLOCATION_NOT_FOUND",
                f"Slurm job {context.job_id} has no assigned nodes",
            )
        states = {item[0].upper() for item in candidates}
        owners = {item[1] for item in candidates}
        if owners != {current_user}:
            raise ActiveCheckSafetyError(
                "SLURM_ALLOCATION_OWNER_MISMATCH",
                f"allocation owner(s) {sorted(owners)} do not match current user {current_user!r}",
            )
        if states != {"RUNNING"}:
            raise ActiveCheckSafetyError(
                "SLURM_ALLOCATION_NOT_RUNNING",
                f"Slurm job {context.job_id} state is {sorted(states)}",
            )
        nodes: list[str] = []
        for expression in expressions:
            expanded = self._control(
                [context.scontrol_executable, "show", "hostnames", expression],
                context.control_timeout_seconds,
            )
            for node in expanded:
                safe = _safe_node(node)
                if safe not in nodes:
                    nodes.append(safe)
        if not nodes:
            raise ActiveCheckSafetyError(
                "SLURM_ALLOCATION_NOT_FOUND", "allocation nodelist expanded to no nodes"
            )

        selected = [_safe_node(node) for node in context.selected_nodes]
        missing = [node for node in selected if node not in nodes]
        if missing:
            raise ActiveCheckSafetyError(
                "ACTIVE_TEST_NODE_OUTSIDE_ALLOCATION",
                f"selected nodes are outside Slurm job {context.job_id}: {missing}",
            )
        controller = context.controller_hostname or socket.gethostname()
        if _host_key(controller) in {_host_key(node) for node in selected}:
            raise ActiveCheckSafetyError(
                "LOGIN_NODE_SELECTED_FOR_ACTIVE_TEST",
                f"controller/login host {controller!r} cannot run an active data-plane test",
            )
        exclusive_mode, oversubscribe_mode, job_record = (
            self._inspect_job_allocation_modes(context)
        )
        node_capacity_evidence: tuple[SlurmNodeCapacityEvidence, ...] = ()
        job_capacity_evidence: SlurmJobCapacityEvidence | None = None
        exclusivity_proof_source: str | None = None
        if exclusive_mode == _FORMAL_SLURM_EXCLUSIVE_MODE:
            exclusivity_proof_source = "SCONTROL_JOB_EXCLUSIVE_NODE"
        elif exclusive_mode is None and oversubscribe_mode == "NO":
            node_capacity_evidence = self._inspect_legacy_full_node_capacity(
                context, selected
            )
            job_capacity_evidence = self._inspect_legacy_job_capacity(
                job_record,
                node_capacity_evidence,
                selected,
                nodes,
            )
            if all(item.full_capacity_proven for item in node_capacity_evidence) and (
                job_capacity_evidence.full_capacity_proven
            ):
                exclusivity_proof_source = (
                    "SCONTROL_LEGACY_OVERSUBSCRIBE_NO_FULL_JOB_AND_NODE_CAPACITY"
                )

        foreign_active_job_ids = self._inspect_foreign_active_jobs(context, selected)
        node_exclusivity_proven = bool(
            exclusivity_proof_source and not foreign_active_job_ids
        )
        if foreign_active_job_ids and not context.unsafe_allow_overlap:
            raise ActiveCheckSafetyError(
                "SLURM_ALLOCATION_HAS_FOREIGN_ACTIVE_JOBS",
                "selected nodes have other active Slurm jobs: "
                f"{list(foreign_active_job_ids)}",
            )
        if exclusivity_proof_source is None and not context.unsafe_allow_overlap:
            if exclusive_mode is None and oversubscribe_mode == "NO":
                failed_nodes = [
                    item.node
                    for item in node_capacity_evidence
                    if not item.full_capacity_proven
                ]
                raise ActiveCheckSafetyError(
                    "SLURM_ALLOCATION_NOT_FULL_NODE_CAPACITY",
                    "legacy Slurm allocation did not fully own CPU and HCU capacity "
                    f"on selected nodes: {failed_nodes}",
                )
            reported = (
                f"Exclusive={exclusive_mode}"
                if exclusive_mode is not None
                else f"OverSubscribe={oversubscribe_mode}"
            )
            raise ActiveCheckSafetyError(
                "SLURM_ALLOCATION_NOT_NODE_EXCLUSIVE",
                f"Slurm job {context.job_id} reports {reported}; formal validation "
                "requires Exclusive=NODE or the validated legacy full-node equivalent",
            )
        step_lines = self._control(
            [
                context.squeue_executable,
                "--steps",
                "-h",
                "-j",
                context.job_id,
                "-o",
                "%i|%N",
            ],
            context.control_timeout_seconds,
        )
        active_steps: list[str] = []
        batch_steps: list[str] = []
        workload_steps: list[str] = []
        for line in step_lines:
            # Some deployed Slurm versions expose no step-state format field.
            # ``squeue --steps`` only lists live/pending steps, so conservatively
            # treat every listed non-extern step as active. A batch step owns
            # allocation resources too and is a hard stop for the default path.
            step_id = line.split("|", 1)[0].strip()
            if not step_id:
                continue
            if step_id.endswith(".extern"):
                continue
            active_steps.append(step_id)
            if step_id.endswith(".batch"):
                batch_steps.append(step_id)
            else:
                workload_steps.append(step_id)
        if batch_steps and not context.unsafe_allow_overlap:
            raise ActiveCheckSafetyError(
                "SLURM_ALLOCATION_HAS_BATCH_STEP",
                f"allocation has an active batch step: {sorted(set(batch_steps))}; "
                "use a dedicated salloc allocation without a batch step",
            )
        if workload_steps and not context.allow_active_steps:
            raise ActiveCheckSafetyError(
                "SLURM_ALLOCATION_HAS_ACTIVE_STEPS",
                f"allocation has active job steps: {sorted(set(workload_steps))}",
            )
        if not context.confirm_allocation_idle:
            raise ActiveCheckSafetyError(
                "ALLOCATION_IDLE_NOT_CONFIRMED",
                "active validation requires explicit confirmation that training is not running",
            )
        return SlurmAllocation(
            job_id=context.job_id,
            owner=current_user,
            state="RUNNING",
            nodes=tuple(nodes),
            active_steps=tuple(sorted(set(active_steps))),
            exclusive_mode=exclusive_mode,
            oversubscribe_mode=oversubscribe_mode,
            exclusivity_proof_source=exclusivity_proof_source,
            node_capacity_evidence=node_capacity_evidence,
            job_capacity_evidence=job_capacity_evidence,
            foreign_active_job_ids=foreign_active_job_ids,
            node_exclusivity_proven=node_exclusivity_proven,
        )

    @staticmethod
    def _srun_prefix(
        context: SlurmActiveContext, nodes: Sequence[str], *, tasks: int
    ) -> list[str]:
        if not nodes or tasks < 1:
            raise ValueError("srun nodes/tasks cannot be empty")
        prefix = [
            context.srun_executable,
            f"--jobid={context.job_id}",
            f"--nodes={len(nodes)}",
            f"--ntasks={tasks}",
            f"--nodelist={','.join(nodes)}",
            "--export=ALL",
        ]
        if context.unsafe_allow_overlap:
            prefix.append("--overlap")
        else:
            prefix.extend(["--exclusive", "--exact", "--immediate=1"])
        return prefix + [
            "--cpu-bind=none",
            "--kill-on-bad-exit=1",
        ]

    @staticmethod
    def _ambient_runtime_environment(
        explicit_environment: Mapping[str, str],
    ) -> dict[str, str]:
        return {
            name: os.environ[name]
            for name in sorted(_ALLOWED_RCCL_ENV)
            if name in os.environ and name not in explicit_environment
        }
    def build_verbs_commands(
        self, context: SlurmActiveContext, config: VerbsCheckConfig
    ) -> tuple[list[str], list[str]]:
        context.validate()
        config.validate()
        if len(context.selected_nodes) != 2:
            raise ValueError("verbs checks require exactly two selected_nodes")
        common = [
            config.tool,
            "--connection=RC",
            f"--ib-port={config.ib_port}",
            f"--port={config.control_port}",
            f"--size={config.message_bytes}",
            f"--iters={config.iterations}",
            "--report_gbits",
        ]
        if config.device:
            common.append(f"--ib-dev={config.device}")
        if config.gid_index is not None:
            common.append(f"--gid-index={config.gid_index}")
        execution = common
        if config.container_name is not None:
            execution = [
                config.docker_executable,
                "exec",
                config.container_name,
                *common,
            ]
        server_node, client_node = context.selected_nodes
        server = self._srun_prefix(context, [server_node], tasks=1) + execution
        client = self._srun_prefix(context, [client_node], tasks=1) + execution + [server_node]
        return server, client

    def build_rccl_command(
        self, context: SlurmActiveContext, config: RcclCheckConfig
    ) -> list[str]:
        context.validate()
        config.validate()
        nodes = list(context.selected_nodes)
        env = {"NCCL_DEBUG": "INFO", "NCCL_DEBUG_SUBSYS": "INIT,NET", **dict(config.environment)}
        total_tasks = len(nodes) * config.tasks_per_node
        test_argv = [
            config.binary,
            "-b",
            str(config.minimum_bytes),
            "-e",
            str(config.maximum_bytes),
            "-f",
            str(config.step_factor),
            "-w",
            str(config.warmup_iterations),
            "-n",
            str(config.iterations),
            "-g",
            str(config.devices_per_task),
            "-c",
            "1",
        ]
        if config.container_name is not None:
            execution_argv = [config.docker_executable, "exec"]
            for key in sorted(env):
                execution_argv.extend(["--env", f"{key}={env[key]}"])
            execution_argv.extend([config.container_name, *test_argv])
        else:
            execution_argv = ["env"]
            execution_argv.extend(f"{key}={env[key]}" for key in sorted(env))
            execution_argv.extend(test_argv)
        return (
            self._srun_prefix(context, nodes, tasks=total_tasks)
            + [f"--mpi={config.mpi_mode}", f"--ntasks-per-node={config.tasks_per_node}"]
            + execution_argv
        )

    def build_torch_rccl_command(
        self, context: SlurmActiveContext, config: TorchRcclCheckConfig
    ) -> list[str]:
        """Build one task per node without invoking a shell or user code."""

        context.validate()
        config.validate()
        nodes = list(context.selected_nodes)
        master_addr = _safe_node(nodes[0])
        environment = {
            "HCU_ENVCHECK_TORCH_TIMEOUT": str(
                max(1, int(config.command_timeout_seconds))
            ),
            "MASTER_ADDR": master_addr,
            "MASTER_PORT": str(config.master_port),
            "NCCL_DEBUG": "INFO",
            "NCCL_DEBUG_SUBSYS": "INIT,NET",
            "PYTHONUNBUFFERED": "1",
            **dict(config.environment),
        }
        python_argv = [config.python_binary, "-c", _TORCH_RCCL_SCRIPT]
        if config.container_name is not None:
            execution_argv = [config.docker_executable, "exec"]
            # Docker copies these values from each individual srun task. No
            # launcher-side rank calculation or shell interpolation is used.
            for name in (
                "SLURM_PROCID",
                "SLURM_NTASKS",
                "SLURM_LOCALID",
                "SLURMD_NODENAME",
            ):
                execution_argv.extend(["--env", name])
            for name in sorted(environment):
                execution_argv.extend(["--env", f"{name}={environment[name]}"])
            execution_argv.extend([config.container_name, *python_argv])
        else:
            execution_argv = ["env"]
            execution_argv.extend(
                f"{name}={environment[name]}" for name in sorted(environment)
            )
            execution_argv.extend(python_argv)
        return (
            self._srun_prefix(context, nodes, tasks=len(nodes))
            + ["--ntasks-per-node=1"]
            + execution_argv
        )
    @staticmethod
    def _bounded_text_fields(stdout: str | bytes | None, stderr: str | bytes | None) -> dict[str, Any]:
        stdout_capture = _BoundedByteCapture()
        stderr_capture = _BoundedByteCapture()
        stdout_capture.feed(stdout or b"")
        stderr_capture.feed(stderr or b"")
        return {
            "stdout": stdout_capture.render(),
            "stderr": stderr_capture.render(),
            "stdout_total_bytes": stdout_capture.total_bytes,
            "stderr_total_bytes": stderr_capture.total_bytes,
            "stdout_truncated": stdout_capture.truncated,
            "stderr_truncated": stderr_capture.truncated,
        }

    @staticmethod
    def _begin_bounded_capture(process: Any) -> tuple[_BoundedByteCapture, _BoundedByteCapture, list[threading.Thread]]:
        if process.stdout is None or process.stderr is None:
            raise RuntimeError("bounded capture requires stdout and stderr pipes")
        stdout_capture = _BoundedByteCapture()
        stderr_capture = _BoundedByteCapture()
        threads = [
            threading.Thread(target=stdout_capture.drain, args=(process.stdout,), daemon=True),
            threading.Thread(target=stderr_capture.drain, args=(process.stderr,), daemon=True),
        ]
        for thread in threads:
            thread.start()
        return stdout_capture, stderr_capture, threads

    @staticmethod
    def _finish_bounded_capture(
        process: Any,
        capture_state: tuple[_BoundedByteCapture, _BoundedByteCapture, list[threading.Thread]],
        timeout: float,
    ) -> tuple[dict[str, Any], bool]:
        stdout_capture, stderr_capture, threads = capture_state
        timed_out = False
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                process.terminate()
            except OSError:
                pass
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    process.kill()
                except OSError:
                    pass
                process.wait()
        for thread in threads:
            thread.join(timeout=10)
        if any(thread.is_alive() for thread in threads):
            for pipe in (process.stdout, process.stderr):
                try:
                    pipe.close()
                except OSError:
                    pass
            for thread in threads:
                thread.join(timeout=1)
        fields = {
            "stdout": stdout_capture.render(),
            "stderr": stderr_capture.render(),
            "stdout_total_bytes": stdout_capture.total_bytes,
            "stderr_total_bytes": stderr_capture.total_bytes,
            "stdout_truncated": stdout_capture.truncated,
            "stderr_truncated": stderr_capture.truncated,
        }
        return fields, timed_out

    def _run_sync(self, role: str, nodes: Sequence[str], argv: list[str], timeout: float):
        if Path(argv[0]).name not in {"srun", "srun.exe"}:
            raise ActiveCheckSafetyError(
                "LOGIN_NODE_WORKLOAD_REJECTED", "active workload command must start with srun"
            )
        started = time.monotonic()
        if self._runner is subprocess.run and self._popen is subprocess.Popen:
            try:
                process = self._popen(
                    argv,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                capture_state = self._begin_bounded_capture(process)
                output, timed_out = self._finish_bounded_capture(
                    process, capture_state, timeout
                )
                return {
                    "role": role,
                    "nodes": list(nodes),
                    "argv": argv,
                    "returncode": 124 if timed_out else int(process.returncode or 0),
                    **output,
                    "duration": time.monotonic() - started,
                    "timed_out": timed_out,
                }
            except OSError as exc:
                output = self._bounded_text_fields("", str(exc))
                return {
                    "role": role,
                    "nodes": list(nodes),
                    "argv": argv,
                    "returncode": 127,
                    **output,
                    "duration": time.monotonic() - started,
                    "timed_out": False,
                }

        try:
            completed = self._runner(
                argv,
                stdin=subprocess.DEVNULL,
                text=True,
                encoding="utf-8",
                errors="replace",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
                check=False,
            )
            output = self._bounded_text_fields(completed.stdout, completed.stderr)
            return {
                "role": role,
                "nodes": list(nodes),
                "argv": argv,
                "returncode": completed.returncode,
                **output,
                "duration": time.monotonic() - started,
                "timed_out": False,
            }
        except subprocess.TimeoutExpired as exc:
            output = self._bounded_text_fields(exc.stdout, exc.stderr)
            return {
                "role": role,
                "nodes": list(nodes),
                "argv": argv,
                "returncode": 124,
                **output,
                "duration": time.monotonic() - started,
                "timed_out": True,
            }
        except OSError as exc:
            output = self._bounded_text_fields("", str(exc))
            return {
                "role": role,
                "nodes": list(nodes),
                "argv": argv,
                "returncode": 127,
                **output,
                "duration": time.monotonic() - started,
                "timed_out": False,
            }

    @staticmethod
    def _parse_verbs_average_gbps(text: str) -> float | None:
        if "BW average" not in text and "BW avg" not in text:
            return None
        candidates: list[float] = []
        for line in text.splitlines():
            fields = line.strip().split()
            if len(fields) < 4 or not fields[0].isdigit() or not fields[1].isdigit():
                continue
            try:
                value = float(fields[3])
            except ValueError:
                continue
            if math.isfinite(value) and value > 0:
                candidates.append(value)
        return candidates[-1] if candidates else None

    @staticmethod
    def _parse_verbs_endpoint_metadata(text: str) -> dict[str, list[str]]:
        def values(label: str) -> list[str]:
            matches = re.findall(
                rf"\b{re.escape(label)}\s*:\s*([A-Za-z0-9_.:-]+)",
                text,
                re.I,
            )
            return list(dict.fromkeys(item.strip() for item in matches if item.strip()))

        return {
            "devices": values("Device"),
            "transport_types": values("Transport type"),
            "link_types": values("Link type"),
        }

    @staticmethod
    def _parse_rccl(text: str) -> dict[str, Any]:
        average_matches = re.findall(
            r"Avg\s+bus\s+bandwidth\s*:\s*([0-9]+(?:\.[0-9]+)?)", text, re.I
        )
        error_matches = re.findall(r"Out\s+of\s+bounds\s+values\s*:\s*(\d+)", text, re.I)
        ranks = [int(item) for item in re.findall(r"\bnranks\s*(?:=|\s)\s*(\d+)\b", text, re.I)]
        # Discovery such as ``NET/IB : No device found`` is negative evidence,
        # never proof of the selected data path.  RCCL/NCCL plugins often add
        # ABI suffixes (for example IBext_v8 and Socket_v8), so classify the
        # plugin token rather than matching only the unsuffixed display name.
        selected_plugins = list(
            dict.fromkeys(
                re.findall(
                    r"\bUsing\s+network\s+([A-Za-z0-9_.+-]+)", text, re.I
                )
                + re.findall(
                    r"\bvia\s+NET/([A-Za-z0-9_.+-]+)", text, re.I
                )
                + re.findall(
                    r"\bNET/([A-Za-z0-9_.+-]+)\s*:\s*Using\b", text, re.I
                )
            )
        )

        def is_rdma_plugin(name: str) -> bool:
            token = name.casefold()
            return token == "ib" or token.startswith("ibext")

        def is_socket_plugin(name: str) -> bool:
            return name.casefold().startswith("socket")

        has_rdma = any(is_rdma_plugin(name) for name in selected_plugins)
        has_socket = any(is_socket_plugin(name) for name in selected_plugins)
        rdma_device_discovery_failed = bool(
            re.search(r"\bNET/IB\s*:\s*No\s+device(?:s)?\s+found\b", text, re.I)
        )
        net_plugin_missing = bool(
            re.search(
                r"(?:could\s+not\s+find|cannot\s+open|failed\s+to\s+(?:find|load))"
                r"[^\r\n]*(?:lib(?:rccl|nccl)-net|network\s+plugin)",
                text,
                re.I,
            )
            or re.search(
                r"(?:lib(?:rccl|nccl)-net)[^\r\n]*"
                r"(?:not\s+found|no\s+such\s+file|cannot\s+open)",
                text,
                re.I,
            )
        )
        evidence_markers: list[str] = []
        root_cause_candidates: list[str] = []
        if net_plugin_missing:
            evidence_markers.append("NET_PLUGIN_LOAD_FAILED")
            root_cause_candidates.append("RCCL_NET_PLUGIN_MISSING")
        if rdma_device_discovery_failed:
            evidence_markers.append("NET_IB_NO_DEVICE_FOUND")
            root_cause_candidates.append("RCCL_RDMA_DEVICE_NOT_DISCOVERED")
        if has_rdma:
            evidence_markers.append("NET_IB_DATA_PATH_SELECTED")
        if has_rdma and rdma_device_discovery_failed:
            evidence_markers.append("NET_IB_EVIDENCE_CONFLICTING")
        if has_socket:
            evidence_markers.append("NET_SOCKET_DATA_PATH_SELECTED")
            root_cause_candidates.append("RCCL_USED_SOCKET_TRANSPORT")
        if has_rdma and has_socket:
            transport = "MIXED"
        elif has_rdma:
            transport = "RDMA"
        elif has_socket:
            transport = "SOCKET"
        else:
            transport = "UNKNOWN"
        return {
            "average_busbw_gbytes_per_second": float(average_matches[-1]) if average_matches else None,
            "out_of_bounds_values": max(int(item) for item in error_matches) if error_matches else None,
            "reported_out_of_bounds_values": [int(item) for item in error_matches],
            "maximum_reported_nranks": max(ranks) if ranks else None,
            "data_transport": transport,
            "selected_network_plugins": selected_plugins,
            "rdma_device_discovery_failed": rdma_device_discovery_failed,
            "net_plugin_missing": net_plugin_missing,
            "root_cause_candidates": root_cause_candidates,
            "evidence_markers": evidence_markers,
        }

    @staticmethod
    def _parse_torch_rccl_markers(
        text: str, selected_nodes: Sequence[str]
    ) -> dict[str, Any]:
        decoder = json.JSONDecoder()
        parsed: list[dict[str, Any]] = []
        invalid_count = 0
        cursor = 0
        while True:
            marker_at = text.find(_TORCH_RCCL_MARKER, cursor)
            if marker_at < 0:
                break
            payload = text[marker_at + len(_TORCH_RCCL_MARKER) :].lstrip()
            cursor = marker_at + len(_TORCH_RCCL_MARKER)
            try:
                item, consumed = decoder.raw_decode(payload)
                cursor += consumed
            except (TypeError, ValueError, json.JSONDecodeError):
                invalid_count += 1
                continue
            if not isinstance(item, dict):
                invalid_count += 1
                continue
            rank = item.get("rank")
            world = item.get("world")
            node = item.get("node")
            value = item.get("value")
            expected = item.get("expected")
            correct = item.get("correct")
            if (
                isinstance(rank, bool)
                or not isinstance(rank, int)
                or isinstance(world, bool)
                or not isinstance(world, int)
                or not isinstance(node, str)
                or not node.strip()
                or isinstance(value, bool)
                or not isinstance(value, (int, float))
                or isinstance(expected, bool)
                or not isinstance(expected, (int, float))
                or not isinstance(correct, bool)
                or not math.isfinite(float(value))
                or not math.isfinite(float(expected))
            ):
                invalid_count += 1
                continue
            parsed.append(
                {
                    "rank": rank,
                    "world": world,
                    "node": node.strip(),
                    "value": float(value),
                    "expected": float(expected),
                    "correct": correct,
                }
            )

        expected_world = len(selected_nodes)
        ranks = [item["rank"] for item in parsed]
        duplicate_ranks = sorted(
            rank for rank in set(ranks) if ranks.count(rank) > 1
        )
        observed_ranks = sorted(set(ranks))
        missing_ranks = sorted(set(range(expected_world)) - set(observed_ranks))
        unexpected_ranks = sorted(set(observed_ranks) - set(range(expected_world)))
        selected_by_key = {_host_key(node): node for node in selected_nodes}
        observed_by_key = {_host_key(item["node"]): item["node"] for item in parsed}
        missing_nodes = [
            selected_by_key[key]
            for key in sorted(set(selected_by_key) - set(observed_by_key))
        ]
        unexpected_nodes = [
            observed_by_key[key]
            for key in sorted(set(observed_by_key) - set(selected_by_key))
        ]
        worlds = sorted(set(item["world"] for item in parsed))
        world_matches = worlds == [expected_world]
        expected_value = float(expected_world * (expected_world + 1) // 2)
        correctness = bool(parsed) and all(
            item["correct"]
            and abs(item["value"] - expected_value) <= 1.0e-5
            and abs(item["expected"] - expected_value) <= 1.0e-5
            for item in parsed
        )
        complete = not any(
            (
                invalid_count,
                duplicate_ranks,
                missing_ranks,
                unexpected_ranks,
                missing_nodes,
                unexpected_nodes,
                not world_matches,
            )
        )
        return {
            "rank_markers": sorted(parsed, key=lambda item: item["rank"]),
            "rank_marker_count": len(parsed),
            "invalid_rank_marker_count": invalid_count,
            "observed_ranks": observed_ranks,
            "missing_ranks": missing_ranks,
            "unexpected_ranks": unexpected_ranks,
            "duplicate_ranks": duplicate_ranks,
            "observed_nodes": sorted(observed_by_key.values()),
            "missing_nodes": missing_nodes,
            "unexpected_nodes": unexpected_nodes,
            "reported_world_sizes": worlds,
            "expected_world_size": expected_world,
            "expected_all_reduce_value": expected_value,
            "rank_markers_complete": complete,
            "collective_correctness": (
                "PASS"
                if complete and correctness
                else "FAIL"
                if parsed and not correctness
                else "NOT_VERIFIED"
            ),
        }

    @staticmethod
    def _parse_gpudirect_status(text: str) -> tuple[str, list[str]]:
        # A selected channel ending in /GDRDMA is stronger data-path evidence
        # than candidate-HCA initialization messages that may include both
        # disabled and enabled probes before RCCL chooses the final route.
        if re.search(r"\bvia\s+NET/\S*/GDRDMA\b", text, re.I):
            return "ENABLED", ["GPU_DIRECT_RDMA_DATA_PATH_SELECTED"]
        disabled = bool(
            re.search(
                r"(?:GPU\s+Direct\s+RDMA|GDR(?:DMA)?)\s*(?:is\s+)?Disabled\b",
                text,
                re.I,
            )
        )
        enabled = bool(
            re.search(
                r"(?:GPU\s+Direct\s+RDMA|GDR(?:DMA)?)\s*(?:is\s+)?Enabled\b",
                text,
                re.I,
            )
        )
        if enabled and not disabled:
            return "ENABLED", ["GPU_DIRECT_RDMA_ENABLED"]
        if disabled and not enabled:
            return "DISABLED", ["GPU_DIRECT_RDMA_DISABLED"]
        if enabled and disabled:
            return "UNKNOWN", ["GPU_DIRECT_RDMA_STATUS_CONFLICTING"]
        return "UNKNOWN", []
    @staticmethod
    def _markdown_value(value: Any) -> str:
        if value is None:
            return "-"
        if isinstance(value, (dict, list, tuple)):
            rendered = json.dumps(value, ensure_ascii=False, sort_keys=True)
        else:
            rendered = str(value)
        return rendered.replace("|", "\\|").replace("\n", " ")

    @classmethod
    def _render_summary(cls, result: ActiveCheckResult, output_dir: Path) -> str:
        lines = [
            "# Active RDMA validation summary",
            "",
            "| Item | Result |",
            "|---|---|",
            f"| Verdict | {cls._markdown_value(result.status)} |",
            f"| Reason | {cls._markdown_value(result.reason_code)} |",
            f"| Backend | {cls._markdown_value(result.backend)} |",
            f"| Slurm job | {cls._markdown_value(result.job_id)} |",
            f"| Nodes | {cls._markdown_value(', '.join(result.nodes))} |",
            f"| Requested protocol | {cls._markdown_value(result.requested_protocol)} |",
            f"| Actual transport | {cls._markdown_value(result.data_transport)} |",
            f"| Container | {cls._markdown_value(result.container_name)} |",
            f"| Message | {cls._markdown_value(result.message)} |",
            f"| Root-cause candidates | {cls._markdown_value(result.root_cause_candidates)} |",
            f"| Evidence markers | {cls._markdown_value(result.evidence_markers)} |",
            "",
            "## Metrics",
            "",
            "| Metric | Value |",
            "|---|---|",
        ]
        if result.metrics:
            for key in sorted(result.metrics):
                lines.append(
                    f"| {cls._markdown_value(key)} | "
                    f"{cls._markdown_value(result.metrics[key])} |"
                )
        else:
            lines.append("| - | - |")
        lines.extend(
            [
                "",
                "## Evidence",
                "",
                f"- Result JSON: `{output_dir / 'active-result.json'}`",
                f"- Evidence directory: `{output_dir}`",
            ]
        )
        for command in result.commands:
            lines.append(
                f"- {command.role} stdout: `{command.stdout_path}`; "
                f"stderr: `{command.stderr_path}`"
            )
        return "\n".join(lines) + "\n"

    @staticmethod
    def _persist(
        output_dir: Path,
        result: ActiveCheckResult,
        raw_commands: Sequence[dict[str, Any]],
    ) -> ActiveCheckResult:
        if (
            result.metrics.get("safety_boundary") == "OVERLAP_NOT_PROVEN_IDLE"
            and result.status in {"PASS", "WARN"}
        ):
            result.metrics["pre_safety_status"] = result.status
            result.metrics["pre_safety_reason_code"] = result.reason_code
            result.status = "NOT_VERIFIED"
            result.reason_code = "OVERLAP_NOT_PROVEN_IDLE"
            result.message = (
                "active data-path evidence was collected with --overlap, but exclusive "
                "allocation idleness was not proven; formal PASS is suppressed"
            )
        elif (
            result.metrics.get("safety_boundary") != FORMAL_SLURM_SAFETY_BOUNDARY
            and result.status in {"PASS", "WARN"}
        ):
            result.metrics["pre_safety_status"] = result.status
            result.metrics["pre_safety_reason_code"] = result.reason_code
            result.status = "NOT_VERIFIED"
            result.reason_code = "SLURM_NODE_EXCLUSIVITY_NOT_PROVEN"
            result.message = (
                "active data-path evidence was collected without scheduler proof of "
                "whole-node ownership and zero foreign active jobs; formal PASS is suppressed"
            )
        for index, command in enumerate(raw_commands, start=1):
            stem = f"{index:02d}-{command['role']}"
            stdout_path = output_dir / f"{stem}.stdout.txt"
            stderr_path = output_dir / f"{stem}.stderr.txt"
            atomic_write_text_exclusive(stdout_path, command.get("stdout", ""))
            atomic_write_text_exclusive(stderr_path, command.get("stderr", ""))
            result.commands.append(
                ActiveCommandEvidence(
                    role=command["role"],
                    node_scope=list(command["nodes"]),
                    argv=list(command["argv"]),
                    returncode=int(command["returncode"]),
                    duration_seconds=round(float(command["duration"]), 6),
                    timed_out=bool(command.get("timed_out")),
                    stdout_path=str(stdout_path),
                    stderr_path=str(stderr_path),
                    stdout_total_bytes=int(
                        command.get(
                            "stdout_total_bytes",
                            len(command.get("stdout", "").encode("utf-8", "replace")),
                        )
                    ),
                    stderr_total_bytes=int(
                        command.get(
                            "stderr_total_bytes",
                            len(command.get("stderr", "").encode("utf-8", "replace")),
                        )
                    ),
                    stdout_truncated=bool(command.get("stdout_truncated")),
                    stderr_truncated=bool(command.get("stderr_truncated")),
                )
            )
        atomic_write_text_exclusive(
            output_dir / "active-result.json",
            json.dumps(result.to_dict(), ensure_ascii=False, indent=2) + "\n",
        )
        atomic_write_text_exclusive(
            output_dir / "active-summary.md",
            SlurmActiveCheckRunner._render_summary(result, output_dir),
        )
        return result

    @staticmethod
    def _base_result(
        *,
        backend: str,
        context: SlurmActiveContext,
        output_dir: Path,
        started_at: str,
        status: str,
        reason_code: str,
        message: str,
        requested_protocol: str | None = None,
        container_name: str | None = None,
        allocation: SlurmAllocation | None = None,
    ) -> ActiveCheckResult:
        if context.unsafe_allow_overlap:
            safety_boundary = "OVERLAP_NOT_PROVEN_IDLE"
        elif allocation is not None and allocation.node_exclusivity_proven:
            safety_boundary = FORMAL_SLURM_SAFETY_BOUNDARY
        else:
            safety_boundary = UNPROVEN_SLURM_SAFETY_BOUNDARY
        metrics: dict[str, Any] = {"safety_boundary": safety_boundary}
        if allocation is not None:
            metrics.update(
                {
                    "allocation_exclusive_mode": allocation.exclusive_mode,
                    "allocation_oversubscribe_mode": allocation.oversubscribe_mode,
                    "allocation_exclusivity_proof_source": (
                        allocation.exclusivity_proof_source
                    ),
                    "allocation_node_capacity_evidence": [
                        asdict(item) for item in allocation.node_capacity_evidence
                    ],
                    "allocation_job_capacity_evidence": (
                        asdict(allocation.job_capacity_evidence)
                        if allocation.job_capacity_evidence is not None
                        else None
                    ),
                    "allocation_foreign_active_job_ids": list(
                        allocation.foreign_active_job_ids
                    ),
                    "allocation_node_exclusivity_proven": (
                        allocation.node_exclusivity_proven
                    ),
                }
            )
        return ActiveCheckResult(
            status=status,
            backend=backend,
            reason_code=reason_code,
            message=message,
            job_id=context.job_id,
            nodes=list(context.selected_nodes),
            started_at=started_at,
            finished_at=_utc_now(),
            evidence_dir=str(output_dir),
            requested_protocol=requested_protocol,
            container_name=container_name,
            metrics=metrics,
            allocation=asdict(allocation) if allocation else None,
        )

    def run_verbs(
        self,
        context: SlurmActiveContext,
        config: VerbsCheckConfig,
        *,
        output_dir: Path,
    ) -> ActiveCheckResult:
        claim_output_directory(output_dir)
        started_at = _utc_now()
        if not context.enabled:
            result = self._base_result(
                backend="VERBS",
                context=context,
                output_dir=output_dir,
                started_at=started_at,
                status="NOT_VERIFIED",
                reason_code="ACTIVE_CHECKS_DISABLED",
                message="active verbs validation is disabled by default",
                requested_protocol=config.protocol,
                container_name=config.container_name,
            )
            return self._persist(output_dir, result, [])
        try:
            context.validate()
            config.validate()
            allocation = self.inspect_allocation(context)
            server_argv, client_argv = self.build_verbs_commands(context, config)
        except (ValueError, ActiveCheckSafetyError) as exc:
            reason = getattr(exc, "reason_code", "INVALID_ACTIVE_CHECK_CONFIGURATION")
            result = self._base_result(
                backend="VERBS",
                context=context,
                output_dir=output_dir,
                started_at=started_at,
                status="NOT_VERIFIED",
                reason_code=reason,
                message=str(exc),
                requested_protocol=config.protocol,
                container_name=config.container_name,
            )
            return self._persist(output_dir, result, [])

        raw: list[dict[str, Any]] = []
        server_node, client_node = context.selected_nodes
        if Path(server_argv[0]).name not in {"srun", "srun.exe"}:
            raise ActiveCheckSafetyError(
                "LOGIN_NODE_WORKLOAD_REJECTED", "active workload command must start with srun"
            )
        server_started = time.monotonic()
        server_capture_state = None
        try:
            if self._popen is subprocess.Popen:
                server = self._popen(
                    server_argv,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                server_capture_state = self._begin_bounded_capture(server)
            else:
                server = self._popen(
                    server_argv,
                    stdin=subprocess.DEVNULL,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
        except OSError as exc:
            server = None
            output = self._bounded_text_fields("", str(exc))
            raw.append(
                {
                    "role": "verbs-server",
                    "nodes": [server_node],
                    "argv": server_argv,
                    "returncode": 127,
                    **output,
                    "duration": time.monotonic() - server_started,
                    "timed_out": False,
                }
            )
        if server is not None:
            self._sleep(config.startup_grace_seconds)
            if server.poll() is None:
                raw.append(
                    self._run_sync(
                        "verbs-client",
                        [client_node],
                        client_argv,
                        config.command_timeout_seconds,
                    )
                )
            if server_capture_state is not None:
                output, server_timed_out = self._finish_bounded_capture(
                    server, server_capture_state, config.command_timeout_seconds
                )
            else:
                try:
                    server_stdout, server_stderr = server.communicate(
                        timeout=config.command_timeout_seconds
                    )
                    server_timed_out = False
                except subprocess.TimeoutExpired:
                    server.terminate()
                    try:
                        server_stdout, server_stderr = server.communicate(timeout=5)
                    except subprocess.TimeoutExpired:
                        server.kill()
                        server_stdout, server_stderr = server.communicate()
                    server_timed_out = True
                output = self._bounded_text_fields(server_stdout, server_stderr)
            raw.insert(
                0,
                {
                    "role": "verbs-server",
                    "nodes": [server_node],
                    "argv": server_argv,
                    "returncode": 124 if server_timed_out else int(server.returncode or 0),
                    **output,
                    "duration": time.monotonic() - server_started,
                    "timed_out": server_timed_out,
                },
            )
        failures = [item for item in raw if item["returncode"] != 0 or item["timed_out"]]
        output_truncated = any(
            item.get("stdout_truncated") or item.get("stderr_truncated")
            for item in raw
        )
        combined = "\n".join(
            str(item.get(key, "")) for item in raw for key in ("stdout", "stderr")
        )
        average = self._parse_verbs_average_gbps(combined)
        endpoint_metadata = {
            item["role"]: self._parse_verbs_endpoint_metadata(
                f"{item.get('stdout', '')}\n{item.get('stderr', '')}"
            )
            for item in raw
            if item["role"] in {"verbs-server", "verbs-client"}
        }
        metadata_complete = (
            set(endpoint_metadata) == {"verbs-server", "verbs-client"}
            and all(
                len(metadata["devices"]) == 1
                and len(metadata["transport_types"]) == 1
                and len(metadata["link_types"]) == 1
                for metadata in endpoint_metadata.values()
            )
        )
        observed_devices = sorted(
            {value for metadata in endpoint_metadata.values() for value in metadata["devices"]}
        )
        observed_transport_types = sorted(
            {
                value.upper()
                for metadata in endpoint_metadata.values()
                for value in metadata["transport_types"]
            }
        )
        observed_link_types = sorted(
            {
                value.upper()
                for metadata in endpoint_metadata.values()
                for value in metadata["link_types"]
            }
        )
        expected_link_type = "IB" if config.protocol == "ib" else "ETHERNET"
        lower_combined = combined.lower()
        container_unavailable = (
            config.container_name is not None
            and any(
                marker in lower_combined
                for marker in (
                    "no such container",
                    "is not running",
                    "cannot connect to the docker daemon",
                    "permission denied while trying to connect to the docker daemon",
                )
            )
        )
        if container_unavailable:
            status, reason, message = (
                "NOT_VERIFIED",
                "VERBS_CONTAINER_EXEC_UNAVAILABLE",
                "the explicitly selected container could not execute the verbs check",
            )
        elif failures:
            status, reason, message = (
                "FAIL",
                "VERBS_ACTIVE_TEST_FAILED",
                "one or more bounded verbs server/client steps failed",
            )
        elif output_truncated:
            status, reason, message = (
                "NOT_VERIFIED",
                "ACTIVE_OUTPUT_TRUNCATED",
                "verbs output exceeded the bounded evidence limit; PASS is suppressed",
            )
        elif average is None:
            status, reason, message = (
                "NOT_VERIFIED",
                "VERBS_METRIC_EVIDENCE_MISSING",
                "verbs commands exited successfully but no bandwidth result row was parsed",
            )
        elif not metadata_complete:
            status, reason, message = (
                "NOT_VERIFIED",
                "VERBS_ENDPOINT_METADATA_MISSING",
                "perftest output did not prove Device, Transport type and Link type on both endpoints",
            )
        elif config.device is not None and observed_devices != [config.device]:
            status, reason, message = (
                "FAIL",
                "VERBS_DEVICE_MISMATCH",
                f"perftest used devices {observed_devices}, expected {config.device}",
            )
        elif observed_transport_types != ["IB"]:
            status, reason, message = (
                "FAIL",
                "VERBS_TRANSPORT_TYPE_MISMATCH",
                f"perftest reported transport types {observed_transport_types}, expected IB",
            )
        elif observed_link_types != [expected_link_type]:
            status, reason, message = (
                "FAIL",
                "VERBS_LINK_TYPE_MISMATCH",
                f"perftest reported link types {observed_link_types}, expected {expected_link_type}",
            )
        elif (
            config.minimum_average_gbps is not None
            and average < config.minimum_average_gbps
        ):
            status, reason, message = (
                "FAIL",
                "VERBS_BANDWIDTH_BELOW_THRESHOLD",
                f"average bandwidth {average} Gbit/s is below {config.minimum_average_gbps}",
            )
        else:
            status, reason, message = (
                "PASS",
                "VERBS_END_TO_END_PASSED",
                "two-node verbs data-plane validation completed",
            )
        result = self._base_result(
            backend="VERBS",
            context=context,
            output_dir=output_dir,
            started_at=started_at,
            status=status,
            reason_code=reason,
            message=message,
            requested_protocol=config.protocol,
            container_name=config.container_name,
            allocation=allocation,
        )
        result.data_transport = (
            "IB_VERBS" if status == "PASS" and config.protocol == "ib"
            else "ROCE_VERBS" if status == "PASS"
            else None
        )
        result.metrics = {
            **result.metrics,
            "average_gbps": average,
            "minimum_average_gbps": config.minimum_average_gbps,
            "message_bytes": config.message_bytes,
            "iterations": config.iterations,
            "output_capture_limit_bytes": ACTIVE_OUTPUT_CAPTURE_LIMIT_BYTES,
            "output_truncated": output_truncated,
            "endpoint_metadata": endpoint_metadata,
            "observed_devices": observed_devices,
            "observed_transport_types": observed_transport_types,
            "observed_link_types": observed_link_types,
            "expected_link_type": expected_link_type,
            "requested_ib_port": config.ib_port,
        }
        result.finished_at = _utc_now()
        return self._persist(output_dir, result, raw)

    def run_rccl(
        self,
        context: SlurmActiveContext,
        config: RcclCheckConfig,
        *,
        output_dir: Path,
    ) -> ActiveCheckResult:
        claim_output_directory(output_dir)
        started_at = _utc_now()
        if not context.enabled:
            result = self._base_result(
                backend="RCCL",
                context=context,
                output_dir=output_dir,
                started_at=started_at,
                status="NOT_VERIFIED",
                reason_code="ACTIVE_CHECKS_DISABLED",
                message="active RCCL validation is disabled by default",
                container_name=config.container_name,
            )
            return self._persist(output_dir, result, [])
        try:
            context.validate()
            config.validate()
            if config.container_name is not None:
                raise ActiveCheckSafetyError(
                    "RCCL_DOCKER_MPI_LAUNCH_UNSUPPORTED",
                    "rccl-tests under docker exec does not have a validated MPI/PMI "
                    "communicator contract; use torch-rccl with --container-name or "
                    "run rccl-tests on the host",
                )
            allocation = self.inspect_allocation(context)
            argv = self.build_rccl_command(context, config)
        except (ValueError, ActiveCheckSafetyError) as exc:
            reason = getattr(exc, "reason_code", "INVALID_ACTIVE_CHECK_CONFIGURATION")
            result = self._base_result(
                backend="RCCL",
                context=context,
                output_dir=output_dir,
                started_at=started_at,
                status="NOT_VERIFIED",
                reason_code=reason,
                message=str(exc),
                container_name=config.container_name,
            )
            return self._persist(output_dir, result, [])

        raw = [
            self._run_sync(
                "rccl-all-reduce",
                list(context.selected_nodes),
                argv,
                config.command_timeout_seconds,
            )
        ]
        command = raw[0]
        output_truncated = bool(
            command.get("stdout_truncated") or command.get("stderr_truncated")
        )
        combined = f"{command['stdout']}\n{command['stderr']}"
        lower_combined = combined.lower()
        container_unavailable = (
            config.container_name is not None
            and any(
                marker in lower_combined
                for marker in (
                    "no such container",
                    "is not running",
                    "cannot connect to the docker daemon",
                    "permission denied while trying to connect to the docker daemon",
                )
            )
        )
        legacy_metrics = self._parse_rccl(combined)
        root_cause_candidates = list(legacy_metrics.pop("root_cause_candidates"))
        evidence_markers = list(legacy_metrics.pop("evidence_markers"))
        legacy_transport = legacy_metrics.pop("data_transport")
        expected_devices_per_node = config.tasks_per_node * config.devices_per_task
        expected_nranks = len(context.selected_nodes) * expected_devices_per_node
        strict = parse_rccl_tests_output(
            combined,
            expected_nranks=expected_nranks,
            expected_devices_per_node={
                node: expected_devices_per_node for node in context.selected_nodes
            },
            min_bytes=config.minimum_bytes,
            max_bytes=config.maximum_bytes,
            step_factor=config.step_factor,
        )

        rank_transport_values = [item.transport for item in strict.rank_transports]
        has_socket = any(value in {"SOCKET", "MIXED"} for value in rank_transport_values)
        has_rdma = any(value in {"IBEXT", "MIXED"} for value in rank_transport_values)
        has_unknown_transport = (
            len(rank_transport_values) != expected_nranks
            or any(value == "UNKNOWN" for value in rank_transport_values)
        )
        if any(value == "MIXED" for value in rank_transport_values) or (
            has_socket and has_rdma
        ):
            transport = "MIXED"
        elif has_socket:
            transport = "SOCKET"
        elif has_unknown_transport:
            transport = "UNKNOWN"
        elif rank_transport_values and all(
            value == "IBEXT" for value in rank_transport_values
        ):
            transport = "RDMA"
        else:
            transport = "UNKNOWN"

        rank_gdr_values = [item.gdr_state for item in strict.rank_transports]
        if any(value == "CONFLICT" for value in rank_gdr_values):
            gpudirect_status = "CONFLICT"
        elif "ENABLED" in rank_gdr_values and "DISABLED" in rank_gdr_values:
            gpudirect_status = "MIXED"
        elif (
            len(rank_gdr_values) == expected_nranks
            and rank_gdr_values
            and all(value == "ENABLED" for value in rank_gdr_values)
        ):
            gpudirect_status = "ENABLED"
        elif rank_gdr_values and all(value == "DISABLED" for value in rank_gdr_values):
            gpudirect_status = "DISABLED"
        else:
            gpudirect_status = "UNKNOWN"

        layouts = [
            layout
            for row in strict.rows
            for layout in (row.out_of_place, row.in_place)
        ]
        minimum_observed_algbw = min(
            (layout.algbw_gbps for layout in layouts), default=None
        )
        minimum_observed_busbw = min(
            (layout.busbw_gbps for layout in layouts), default=None
        )
        average_busbw = min(strict.summary.average_bus_bandwidths, default=None)
        performance_thresholds = {
            "minimum_average_busbw_gbytes_per_second": (
                config.minimum_average_busbw_gbytes_per_second
            ),
            "minimum_algbw_gbytes_per_second": config.minimum_algbw_gbytes_per_second,
            "minimum_busbw_gbytes_per_second": config.minimum_busbw_gbytes_per_second,
        }
        performance_threshold_requested = any(
            value is not None for value in performance_thresholds.values()
        )
        performance_evidence_missing = bool(
            config.minimum_average_busbw_gbytes_per_second is not None
            and average_busbw is None
        )
        performance_failures: list[str] = []
        if (
            config.minimum_average_busbw_gbytes_per_second is not None
            and average_busbw is not None
            and average_busbw < config.minimum_average_busbw_gbytes_per_second
        ):
            performance_failures.append("average_busbw")
        if (
            config.minimum_algbw_gbytes_per_second is not None
            and minimum_observed_algbw is not None
            and minimum_observed_algbw < config.minimum_algbw_gbytes_per_second
        ):
            performance_failures.append("minimum_algbw")
        if (
            config.minimum_busbw_gbytes_per_second is not None
            and minimum_observed_busbw is not None
            and minimum_observed_busbw < config.minimum_busbw_gbytes_per_second
        ):
            performance_failures.append("minimum_busbw")
        performance_status = (
            "NOT_VERIFIED"
            if not performance_threshold_requested or performance_evidence_missing
            else "FAIL"
            if performance_failures
            else "PASS"
        )

        strict_issue_codes = {item.code for item in strict.issues}
        corruption_codes = {
            "SUMMARY_OUT_OF_BOUNDS_NONZERO",
            "WRONG_VALUE_NONZERO",
            "WRONG_VALUE_STARRED",
        }
        transport_codes = {
            "TRANSPORT_EVIDENCE_CONFLICT",
            "RANK_TRANSPORT_EVIDENCE_CONFLICT",
            "RANK_TRANSPORT_EVIDENCE_MISSING",
        }
        gdr_codes = {
            "GDR_EVIDENCE_CONFLICT",
            "RANK_GDR_EVIDENCE_CONFLICT",
        }
        structural_issues = strict_issue_codes - corruption_codes - transport_codes - gdr_codes
        metrics = {
            **legacy_metrics,
            "legacy_data_transport": legacy_transport,
            "strict_rccl": strict.to_dict(),
            "strict_validation_valid": strict.valid,
            "strict_validation_issue_codes": sorted(strict_issue_codes),
            "expected_nranks": expected_nranks,
            "expected_devices_per_node": expected_devices_per_node,
            "maximum_reported_nranks": max(strict.nranks_values, default=None),
            "reported_nranks_values": list(strict.nranks_values),
            "out_of_bounds_values": max(strict.summary.out_of_bounds_values, default=None),
            "reported_out_of_bounds_values": list(strict.summary.out_of_bounds_values),
            "average_busbw_gbytes_per_second": average_busbw,
            "minimum_observed_algbw_gbytes_per_second": minimum_observed_algbw,
            "minimum_observed_busbw_gbytes_per_second": minimum_observed_busbw,
            "gpudirect_status": gpudirect_status,
            "gpudirect_required": config.require_gdr,
            "performance_threshold_requested": performance_threshold_requested,
            "performance_thresholds": performance_thresholds,
            "performance_status": performance_status,
            "performance_failures": performance_failures,
        }
        if container_unavailable:
            status, reason, message = (
                "NOT_VERIFIED",
                "RCCL_CONTAINER_EXEC_UNAVAILABLE",
                "the explicitly selected training container could not execute the RCCL test",
            )
        elif command["returncode"] != 0 or command["timed_out"]:
            status, reason, message = (
                "FAIL",
                "RCCL_ACTIVE_TEST_FAILED",
                "bounded multi-node RCCL all-reduce failed",
            )
        elif output_truncated:
            status, reason, message = (
                "NOT_VERIFIED",
                "ACTIVE_OUTPUT_TRUNCATED",
                "RCCL output exceeded the bounded evidence limit; PASS is suppressed",
            )
        elif strict_issue_codes & corruption_codes:
            status, reason, message = (
                "FAIL",
                "RCCL_DATA_CORRUPTION_DETECTED",
                "RCCL reported a nonzero or threshold-starred correctness error",
            )
        elif structural_issues:
            status, reason, message = (
                "NOT_VERIFIED",
                "RCCL_OUTPUT_VALIDATION_FAILED",
                "RCCL output did not satisfy strict table, size, rank, device, or world validation",
            )
        elif config.require_rdma_transport and transport in {"SOCKET", "MIXED"}:
            status, reason, message = (
                "FAIL",
                "RCCL_USED_SOCKET_TRANSPORT",
                "one or more RCCL ranks selected Socket or mixed Socket/RDMA transport",
            )
        elif (
            config.require_rdma_transport
            and legacy_metrics["rdma_device_discovery_failed"]
            and transport == "RDMA"
        ):
            status, reason, message = (
                "NOT_VERIFIED",
                "RCCL_RDMA_EVIDENCE_CONFLICTING",
                "rank logs contain both final RDMA selection and an IB device-discovery failure",
            )
        elif config.require_rdma_transport and legacy_metrics[
            "rdma_device_discovery_failed"
        ]:
            status, reason, message = (
                "FAIL",
                "RCCL_RDMA_DEVICE_NOT_FOUND",
                "RCCL reported that its IB backend found no RDMA device",
            )
        elif config.require_rdma_transport and transport != "RDMA":
            status, reason, message = (
                "NOT_VERIFIED",
                "RCCL_RDMA_TRANSPORT_EVIDENCE_MISSING",
                "not every expected RCCL rank has selected RDMA transport evidence",
            )
        elif config.require_gdr and gpudirect_status in {"DISABLED", "MIXED"}:
            status, reason, message = (
                "FAIL",
                "RCCL_GDR_REQUIRED_BUT_DISABLED",
                "GPU Direct RDMA was required but disabled on one or more ranks",
            )
        elif config.require_gdr and gpudirect_status in {"UNKNOWN", "CONFLICT"}:
            status, reason, message = (
                "NOT_VERIFIED",
                "RCCL_GDR_EVIDENCE_MISSING",
                "GPU Direct RDMA final selected-path evidence is incomplete or conflicting",
            )
        elif performance_evidence_missing:
            status, reason, message = (
                "NOT_VERIFIED",
                "RCCL_BANDWIDTH_EVIDENCE_MISSING",
                "an average bus bandwidth threshold was requested but the summary is missing",
            )
        elif performance_failures:
            status, reason, message = (
                "FAIL",
                "RCCL_BANDWIDTH_BELOW_THRESHOLD",
                "one or more requested RCCL algbw/busbw thresholds were not met",
            )
        else:
            performance_clause = (
                "requested performance thresholds passed"
                if performance_status == "PASS"
                else "performance NOT_VERIFIED because no positive threshold was requested"
            )
            status, reason, message = (
                "PASS",
                "RCCL_MULTI_NODE_RDMA_PASSED"
                if transport == "RDMA"
                else "RCCL_MULTI_NODE_PASSED",
                f"multi-node RCCL functional validation passed; {performance_clause}",
            )
        result = self._base_result(
            backend="RCCL",
            context=context,
            output_dir=output_dir,
            started_at=started_at,
            status=status,
            reason_code=reason,
            message=message,
            container_name=config.container_name,
            allocation=allocation,
        )
        result.data_transport = transport
        result.root_cause_candidates = list(dict.fromkeys(root_cause_candidates))
        result.evidence_markers = list(dict.fromkeys(evidence_markers))
        if not structural_issues and not (strict_issue_codes & corruption_codes):
            result.evidence_markers.append("RCCL_STRICT_OUTPUT_VALIDATED")
        if transport == "RDMA":
            result.evidence_markers.append("RCCL_ALL_RANKS_RDMA_SELECTED")
        result.evidence_markers.append(f"RCCL_GDR_{gpudirect_status}")
        result.evidence_markers.append(f"RCCL_PERFORMANCE_{performance_status}")
        if strict_issue_codes & corruption_codes:
            result.root_cause_candidates.append("RCCL_DATA_CORRUPTION_DETECTED")
        if config.require_gdr and gpudirect_status in {"DISABLED", "MIXED"}:
            result.root_cause_candidates.append("RCCL_GDR_REQUIRED_BUT_DISABLED")
        result.root_cause_candidates = list(dict.fromkeys(result.root_cause_candidates))
        runtime_modified_for_test = "LD_LIBRARY_PATH" in config.environment
        ambient_environment = self._ambient_runtime_environment(config.environment)
        if runtime_modified_for_test:
            result.evidence_markers.append("RUNTIME_LIBRARY_PATH_OVERRIDDEN")
        if ambient_environment:
            result.evidence_markers.append("AMBIENT_RUNTIME_ENVIRONMENT_INHERITED")
        result.metrics = {
            **result.metrics,
            **metrics,
            "minimum_average_busbw_gbytes_per_second": config.minimum_average_busbw_gbytes_per_second,
            "minimum_algbw_gbytes_per_second": config.minimum_algbw_gbytes_per_second,
            "minimum_busbw_gbytes_per_second": config.minimum_busbw_gbytes_per_second,
            "minimum_bytes": config.minimum_bytes,
            "maximum_bytes": config.maximum_bytes,
            "step_factor": config.step_factor,
            "iterations": config.iterations,
            "tasks_per_node": config.tasks_per_node,
            "devices_per_task": config.devices_per_task,
            "mpi_mode": config.mpi_mode,
            "output_capture_limit_bytes": ACTIVE_OUTPUT_CAPTURE_LIMIT_BYTES,
            "output_truncated": output_truncated,
            "runtime_modified_for_test": runtime_modified_for_test,
            "ambient_runtime_environment": ambient_environment,
            "runtime_environment_modified": bool(
                runtime_modified_for_test or ambient_environment
            ),
        }
        result.finished_at = _utc_now()
        return self._persist(output_dir, result, raw)
    def run_torch_rccl(
        self,
        context: SlurmActiveContext,
        config: TorchRcclCheckConfig,
        *,
        output_dir: Path,
    ) -> ActiveCheckResult:
        """Run a tiny all-reduce and prove both correctness and transport."""

        claim_output_directory(output_dir)
        started_at = _utc_now()
        if not context.enabled:
            result = self._base_result(
                backend="TORCH_RCCL",
                context=context,
                output_dir=output_dir,
                started_at=started_at,
                status="NOT_VERIFIED",
                reason_code="ACTIVE_CHECKS_DISABLED",
                message="active PyTorch/RCCL validation is disabled by default",
                container_name=config.container_name,
            )
            return self._persist(output_dir, result, [])
        try:
            context.validate()
            config.validate()
            allocation = self.inspect_allocation(context)
            argv = self.build_torch_rccl_command(context, config)
        except (ValueError, ActiveCheckSafetyError) as exc:
            reason = getattr(exc, "reason_code", "INVALID_ACTIVE_CHECK_CONFIGURATION")
            result = self._base_result(
                backend="TORCH_RCCL",
                context=context,
                output_dir=output_dir,
                started_at=started_at,
                status="NOT_VERIFIED",
                reason_code=reason,
                message=str(exc),
                container_name=config.container_name,
            )
            return self._persist(output_dir, result, [])

        raw = [
            self._run_sync(
                "torch-rccl-all-reduce",
                list(context.selected_nodes),
                argv,
                config.command_timeout_seconds,
            )
        ]
        command = raw[0]
        output_truncated = bool(
            command.get("stdout_truncated") or command.get("stderr_truncated")
        )
        combined = f"{command['stdout']}\n{command['stderr']}"
        lower_combined = combined.lower()
        container_unavailable = (
            config.container_name is not None
            and any(
                marker in lower_combined
                for marker in (
                    "no such container",
                    "is not running",
                    "cannot connect to the docker daemon",
                    "permission denied while trying to connect to the docker daemon",
                )
            )
        )
        rank_metrics = self._parse_torch_rccl_markers(
            combined, context.selected_nodes
        )
        rccl_metrics = self._parse_rccl(combined)
        transport = rccl_metrics["data_transport"]
        gpudirect_status, gpudirect_markers = self._parse_gpudirect_status(combined)
        root_causes = list(rccl_metrics["root_cause_candidates"])
        evidence_markers = list(rccl_metrics["evidence_markers"])
        evidence_markers.extend(gpudirect_markers)
        if rank_metrics["rank_markers_complete"]:
            evidence_markers.append("TORCH_RCCL_ALL_RANKS_REPORTED")
        else:
            evidence_markers.append("TORCH_RCCL_RANK_EVIDENCE_INCOMPLETE")
            root_causes.append("TORCH_RCCL_RANK_MARKER_MISSING")
        if rank_metrics["collective_correctness"] == "PASS":
            evidence_markers.append("TORCH_RCCL_COLLECTIVE_CORRECT")
        elif rank_metrics["collective_correctness"] == "FAIL":
            evidence_markers.append("TORCH_RCCL_COLLECTIVE_VALUE_MISMATCH")
            root_causes.append("RCCL_COLLECTIVE_DATA_MISMATCH")

        if container_unavailable:
            status, reason, message = (
                "NOT_VERIFIED",
                "RCCL_CONTAINER_EXEC_UNAVAILABLE",
                "the explicitly selected training container could not execute PyTorch",
            )
        elif rank_metrics["collective_correctness"] == "FAIL":
            status, reason, message = (
                "FAIL",
                "RCCL_DATA_CORRUPTION_DETECTED",
                "one or more PyTorch/RCCL ranks reported an incorrect all-reduce value",
            )
        elif command["returncode"] != 0 or command["timed_out"]:
            status, reason, message = (
                "FAIL",
                "TORCH_RCCL_ACTIVE_TEST_FAILED",
                "bounded multi-node PyTorch/RCCL all-reduce failed",
            )
        elif output_truncated:
            status, reason, message = (
                "NOT_VERIFIED",
                "ACTIVE_OUTPUT_TRUNCATED",
                "PyTorch/RCCL output exceeded the bounded evidence limit; PASS is suppressed",
            )
        elif not rank_metrics["rank_markers_complete"]:
            status, reason, message = (
                "NOT_VERIFIED",
                "TORCH_RCCL_MULTI_NODE_EVIDENCE_MISSING",
                "output does not contain one valid correctness marker for every selected node and rank",
            )
        elif transport in {"SOCKET", "MIXED"}:
            status, reason, message = (
                "FAIL",
                "RCCL_USED_SOCKET_TRANSPORT",
                "PyTorch/RCCL completed but reported Socket or mixed Socket/RDMA transport",
            )
        elif rccl_metrics["rdma_device_discovery_failed"] and transport == "RDMA":
            status, reason, message = (
                "NOT_VERIFIED",
                "RCCL_RDMA_EVIDENCE_CONFLICTING",
                "aggregated rank logs contain both an RDMA selection and an IB device-discovery failure",
            )
        elif rccl_metrics["rdma_device_discovery_failed"]:
            status, reason, message = (
                "FAIL",
                "RCCL_RDMA_DEVICE_NOT_FOUND",
                "RCCL reported that its IB backend found no RDMA device",
            )
        elif transport != "RDMA":
            status, reason, message = (
                "NOT_VERIFIED",
                "RCCL_RDMA_TRANSPORT_EVIDENCE_MISSING",
                "all ranks completed correctly, but RCCL logs did not prove an RDMA data path",
            )
        else:
            status, reason, message = (
                "PASS",
                "TORCH_RCCL_MULTI_NODE_RDMA_PASSED",
                "all selected nodes completed a correct PyTorch/RCCL all-reduce over RDMA",
            )

        result = self._base_result(
            backend="TORCH_RCCL",
            context=context,
            output_dir=output_dir,
            started_at=started_at,
            status=status,
            reason_code=reason,
            message=message,
            container_name=config.container_name,
            allocation=allocation,
        )
        result.data_transport = transport
        result.root_cause_candidates = list(dict.fromkeys(root_causes))
        runtime_modified_for_test = "LD_LIBRARY_PATH" in config.environment
        ambient_environment = self._ambient_runtime_environment(config.environment)
        if runtime_modified_for_test:
            evidence_markers.append("RUNTIME_LIBRARY_PATH_OVERRIDDEN")
        if ambient_environment:
            evidence_markers.append("AMBIENT_RUNTIME_ENVIRONMENT_INHERITED")
        result.evidence_markers = list(dict.fromkeys(evidence_markers))
        result.metrics = {
            **result.metrics,
            **rank_metrics,
            "gpudirect_status": gpudirect_status,
            "gpudirect_validation_boundary": (
                "GPU Direct RDMA is independent of RCCL network transport: DISABLED "
                "does not invalidate an RDMA PASS, but device-memory GPUDirect was "
                "not used or proven and performance may be lower"
            ),
            "rdma_device_discovery_failed": rccl_metrics[
                "rdma_device_discovery_failed"
            ],
            "net_plugin_missing": rccl_metrics["net_plugin_missing"],
            "master_addr": context.selected_nodes[0],
            "master_port": config.master_port,
            "python_binary": config.python_binary,
            "output_capture_limit_bytes": ACTIVE_OUTPUT_CAPTURE_LIMIT_BYTES,
            "output_truncated": output_truncated,
            "runtime_modified_for_test": runtime_modified_for_test,
            "ambient_runtime_environment": ambient_environment,
            "runtime_environment_modified": bool(
                runtime_modified_for_test or ambient_environment
            ),
        }
        result.finished_at = _utc_now()
        return self._persist(output_dir, result, raw)
