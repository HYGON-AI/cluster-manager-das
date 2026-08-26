# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from . import __version__


@dataclass
class CommandResult:
    name: str
    argv: list[str]
    returncode: int
    stdout: str
    stderr: str
    duration_seconds: float
    timed_out: bool = False
    stdout_total_bytes: int | None = None
    stderr_total_bytes: int | None = None
    stdout_truncated: bool = False
    stderr_truncated: bool = False

    @property
    def output_truncated(self) -> bool:
        return self.stdout_truncated or self.stderr_truncated

    def summary(self) -> dict[str, Any]:
        stdout_captured_bytes = len(self.stdout.encode("utf-8", "replace"))
        stderr_captured_bytes = len(self.stderr.encode("utf-8", "replace"))
        return {
            "name": self.name,
            "argv": self.argv,
            "returncode": self.returncode,
            "duration_seconds": round(self.duration_seconds, 3),
            "timed_out": self.timed_out,
            "stdout_bytes": stdout_captured_bytes,
            "stderr_bytes": stderr_captured_bytes,
            "stdout_total_bytes": (
                stdout_captured_bytes
                if self.stdout_total_bytes is None
                else self.stdout_total_bytes
            ),
            "stderr_total_bytes": (
                stderr_captured_bytes
                if self.stderr_total_bytes is None
                else self.stderr_total_bytes
            ),
            "stdout_truncated": self.stdout_truncated,
            "stderr_truncated": self.stderr_truncated,
        }


@dataclass
class Finding:
    severity: str
    reason_code: str
    message: str
    device_id: int | None = None


@dataclass
class DeviceMetrics:
    device_id: int
    bdf: str | None = None
    model: str | None = None
    architecture: str | None = None
    rocminfo_agent: int | None = None
    rocminfo_total_mib: float | None = None
    hy_smi_total_mib: float | None = None
    used_mib: float | None = None
    available_mib: float | None = None
    reserved_mib: float | None = None
    memory_used_percent: float | None = None
    memory_used_percent_reported: float | None = None
    hcu_util_percent: float | None = None
    memory_used_percent_samples: list[float] = field(default_factory=list)
    hcu_util_percent_samples: list[float] = field(default_factory=list)
    memory_exceed_count: int = 0
    utilization_exceed_count: int = 0
    sample_count: int = 0
    status: str = "UNKNOWN"
    reason_codes: list[str] = field(default_factory=list)


@dataclass
class ProbeResult:
    status: str
    target: dict[str, Any]
    thresholds: dict[str, float]
    device_count: int
    expected_device_count: int | None
    devices: list[DeviceMetrics]
    findings: list[Finding]
    commands: list[dict[str, Any]]
    started_at: str
    finished_at: str
    evidence_dir: str | None = None
    environment: dict[str, Any] = field(default_factory=dict)
    tool_version: str = __version__

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
