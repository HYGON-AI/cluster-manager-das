# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Low-pressure one-hop Native InfiniBand fabric inspection for Slurm.

The module deliberately exposes only two management operations:

* four fixed, directed-route ``smpquery`` reads for one-hop adjacency; and
* targeted ``perfquery`` reads against the discovered leaf switch port.

It never runs ``ibnetdiscover``, never performs an unbounded fabric scan, and
never resets a performance counter. The adjacency path never invokes a fabric scan. Every data-plane command is wrapped in an
``srun`` step pinned to a caller-owned, RUNNING, explicitly selected Slurm
allocation.  An optional Docker container must be named explicitly and is only
entered with ``docker exec``; this module never creates, starts, or selects one.
"""

from __future__ import annotations

import json
import re
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock, Thread
from typing import Any, Callable, Mapping, Sequence

from . import __version__
from .active_rdma import (
    ActiveCheckSafetyError,
    FORMAL_SLURM_SAFETY_BOUNDARY,
    SlurmActiveCheckRunner,
    SlurmActiveContext,
    SlurmAllocation,
    UNPROVEN_SLURM_SAFETY_BOUNDARY,
)
from .output import atomic_write_text_exclusive, claim_output_directory


FABRIC_STATUSES = {"PASS", "WARN", "FAIL", "NOT_VERIFIED"}
COUNTER_STATUSES = {"PASS", "WARN", "FAIL", "UNKNOWN"}
STANDARD_COUNTER_MAX_VALUES = {
    "SymbolErrorCounter": 0xFFFF,
    "LinkErrorRecoveryCounter": 0xFF,
    "LinkDownedCounter": 0xFF,
    "PortRcvErrors": 0xFFFF,
    "PortRcvRemotePhysicalErrors": 0xFFFF,
    "PortRcvSwitchRelayErrors": 0xFFFF,
    "PortXmitDiscards": 0xFFFF,
    "PortXmitConstraintErrors": 0xFF,
    "PortRcvConstraintErrors": 0xFF,
    "LocalLinkIntegrityErrors": 0xF,
    "ExcessiveBufferOverrunErrors": 0xF,
    "VL15Dropped": 0xFFFF,
    "PortXmitWait": 0xFFFFFFFF,
}
EXTENDED_COUNTER_MAX_VALUES = {
    # PortCountersExtended fields have an attribute-defined width. The width is never
    # inferred from the observed numeric value.
    'PortXmitWait': 0xFFFFFFFFFFFFFFFF,
}
_DEVICE_RE = re.compile(r"^[A-Za-z0-9_.:-]+$")
_CONTAINER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_LINK_RE = re.compile(
    r"(?P<source_guid>0x[0-9a-fA-F]+)\s+"
    r'"(?P<source_name>[^"]+)"\s+'
    r"(?P<source_lid>\d+)\s+(?P<source_port>\d+)\[[^]]*\]\s+"
    r"==\(\s*(?P<width>\S+)\s+(?P<speed>[0-9.]+)\s+"
    r"(?P<speed_unit>[^\s]+)\s+(?P<state>[^/]+?)/\s*"
    r"(?P<physical_state>[^)]+?)\)==>\s+"
    r"(?P<switch_guid>0x[0-9a-fA-F]+)\s+"
    r"(?P<switch_lid>\d+)\s+(?P<switch_port>\d+)\[[^]]*\]\s+"
    r'"(?P<switch_name>[^"]+)"'
)
_COUNTER_RE = re.compile(
    r"^\s*(?P<name>[A-Za-z][A-Za-z0-9_]*):\.*\s*"
    r"(?P<value>0x[0-9a-fA-F]+|[0-9]+)\s*$"
)
_EXTENDED_COUNTER_HEADER_RE = re.compile(
    r'^\s*#\s*Port\s+extended\s+counters\s*:', re.I | re.M
)
_SMP_FIELD_RE = re.compile(
    r"^\s*(?P<name>[A-Za-z][A-Za-z0-9 _/-]*?):\.*\s*(?P<value>.*?)\s*$"
)
_GUID_RE = re.compile(r"0x[0-9a-fA-F]+")
_SPEED_RE = re.compile(
    r"(?P<speed>[0-9]+(?:\.[0-9]+)?)\s*(?P<unit>Gbps|Gbit/s|Gb/sec)\b",
    re.I,
)

# A positive delta in one of these counters is direct fault evidence.  Static
# non-zero values are only historical evidence and do not fail the check.
FAIL_COUNTERS = frozenset(
    {
        "SymbolErrorCounter",
        "LinkErrorRecoveryCounter",
        "LinkDownedCounter",
        "PortRcvErrors",
        "PortRcvRemotePhysicalErrors",
        "PortRcvSwitchRelayErrors",
        "PortXmitDiscards",
        "PortXmitConstraintErrors",
        "PortRcvConstraintErrors",
        "LocalLinkIntegrityErrors",
        "ExcessiveBufferOverrunErrors",
        "QP1Dropped",
        "VL15Dropped",
        "PortInactiveDiscards",
        "PortNeighborMTUDiscards",
        "PortSwLifetimeLimitDiscards",
        "PortSwHOQLifetimeLimitDiscards",
        "PortLocalPhysicalErrors",
        "PortMalformedPktErrors",
        "PortBufferOverrunErrors",
        "PortDLIDMappingErrors",
        "PortVLMappingErrors",
        "PortLoopingErrors",
    }
)
_WARN_COUNTER_RE = re.compile(
    r"^(?:PortXmitWait|PortXmitTimeCong|SWPortVLCongestion\d+|"
    r"VLXmitTimeCong\d+)$"
)

REQUIRED_STANDARD_COUNTERS = frozenset(
    {
        "SymbolErrorCounter",
        "LinkErrorRecoveryCounter",
        "LinkDownedCounter",
        "PortRcvErrors",
        "PortRcvRemotePhysicalErrors",
        "PortRcvSwitchRelayErrors",
        "PortXmitDiscards",
        "PortXmitConstraintErrors",
        "PortRcvConstraintErrors",
        "LocalLinkIntegrityErrors",
        "ExcessiveBufferOverrunErrors",
        "VL15Dropped",
        "PortXmitWait",
    }
)
REQUIRED_STANDARD_ERROR_COUNTERS = REQUIRED_STANDARD_COUNTERS - {'PortXmitWait'}
FABRIC_OUTPUT_CAPTURE_LIMIT_BYTES = 65536
FABRIC_OUTPUT_READ_CHUNK_BYTES = 65536
Run = Callable[..., subprocess.CompletedProcess[str]]
Popen = Callable[..., subprocess.Popen[bytes]]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_executable(value: str, allowed: set[str], label: str) -> str:
    if not value or any(ch in value for ch in "\x00\r\n"):
        raise ValueError(f"{label} contains unsafe characters")
    if Path(value).name not in allowed:
        raise ValueError(f"{label} must name one of {sorted(allowed)}")
    return value


class _BoundedByteCapture:
    """Drain one pipe while retaining only a bounded head and tail."""

    def __init__(self, limit_bytes: int = FABRIC_OUTPUT_CAPTURE_LIMIT_BYTES):
        if limit_bytes < 2:
            raise ValueError("output capture limit must be at least two bytes")
        self.limit_bytes = limit_bytes
        self.head_limit = limit_bytes // 2
        self.tail_limit = limit_bytes - self.head_limit
        self.head = bytearray()
        self.tail = bytearray()
        self.total_bytes = 0

    def feed(self, value: str | bytes | None) -> None:
        if isinstance(value, str):
            chunk = value.encode("utf-8", "replace")
        else:
            chunk = value or b""
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
                chunk = pipe.read(FABRIC_OUTPUT_READ_CHUNK_BYTES)
                if not chunk:
                    break
                self.feed(chunk)
        finally:
            try:
                pipe.close()
            except OSError:
                pass


def _bounded(
    value: str | bytes | None,
    limit: int = FABRIC_OUTPUT_CAPTURE_LIMIT_BYTES,
) -> tuple[str, int, bool]:
    capture = _BoundedByteCapture(limit)
    capture.feed(value)
    return capture.render(), capture.total_bytes, capture.truncated


def _markdown(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", "<br>")


@dataclass(frozen=True)
class IBFabricCheckConfig:
    """Bounded one-hop fabric query configuration.

    ``hcas`` is explicit and applied to every selected node.  The conservative
    defaults intentionally require a large deployment to be sharded instead of
    emitting a management-query burst from one controller.
    """

    hcas: tuple[str, ...]
    ib_port: int = 1
    container_name: str | None = None
    docker_executable: str = "docker"
    iblinkinfo_executable: str = "iblinkinfo"
    smpquery_executable: str = "smpquery"
    perfquery_executable: str = "perfquery"
    sample_interval_seconds: float = 5.0
    command_timeout_seconds: float = 15.0
    query_qps: float = 2.0
    max_workers: int = 16
    overall_timeout_seconds: float = 900.0
    max_nodes: int = 64
    max_hcas_per_node: int = 16
    max_unique_leaf_ports: int = 512
    expected_link_width: str | None = None
    minimum_link_speed_gbps: float | None = None
    def validate(self, node_count: int) -> None:
        if not self.hcas:
            raise ValueError("at least one explicit HCA is required")
        if len(set(self.hcas)) != len(self.hcas):
            raise ValueError("hcas contains duplicates")
        if not 1 <= self.max_hcas_per_node <= 16:
            raise ValueError("max_hcas_per_node must be between 1 and 16")
        if len(self.hcas) > self.max_hcas_per_node:
            raise ValueError("HCA count exceeds max_hcas_per_node")
        for hca in self.hcas:
            if not _DEVICE_RE.fullmatch(hca):
                raise ValueError(f"unsafe or invalid HCA name: {hca!r}")
        if not 1 <= self.ib_port <= 255:
            raise ValueError("ib_port must be between 1 and 255")
        if not 2 <= self.max_nodes <= 256:
            raise ValueError("max_nodes must be between 2 and 256")
        if node_count > self.max_nodes:
            raise ValueError("selected node count exceeds max_nodes")
        if (self.expected_link_width is None) != (self.minimum_link_speed_gbps is None):
            raise ValueError(
                "expected_link_width and minimum_link_speed_gbps must be provided together"
            )
        if self.expected_link_width is not None:
            if not re.fullmatch(r"[1-9][0-9]*X", self.expected_link_width.upper()):
                raise ValueError("expected_link_width must look like 1X, 2X, 4X, or 8X")
            if not 0 < float(self.minimum_link_speed_gbps) <= 1_000_000:
                raise ValueError("minimum_link_speed_gbps must be in (0, 1000000]")
        if not 1 <= self.max_unique_leaf_ports <= 4096:
            raise ValueError("max_unique_leaf_ports must be between 1 and 4096")
        if not 0 <= self.sample_interval_seconds <= 60:
            raise ValueError("sample_interval_seconds must be between 0 and 60")
        if not 1 <= self.command_timeout_seconds <= 60:
            raise ValueError("command_timeout_seconds must be between 1 and 60")
        if not 0.1 <= self.query_qps <= 20:
            raise ValueError("query_qps must be between 0.1 and 20")
        if not 1 <= self.max_workers <= 64:
            raise ValueError("max_workers must be between 1 and 64")
        if not 1 <= self.overall_timeout_seconds <= 3600:
            raise ValueError("overall_timeout_seconds must be between 1 and 3600")
        _safe_executable(
            self.iblinkinfo_executable, {"iblinkinfo"}, "iblinkinfo_executable"
        )
        _safe_executable(
            self.smpquery_executable, {"smpquery"}, "smpquery_executable"
        )
        _safe_executable(
            self.perfquery_executable, {"perfquery"}, "perfquery_executable"
        )
        if self.container_name is not None:
            if not _CONTAINER_RE.fullmatch(self.container_name):
                raise ValueError(f"unsafe Docker container name: {self.container_name!r}")
            _safe_executable(
                self.docker_executable, {"docker", "docker.exe"}, "docker_executable"
            )


@dataclass
class FabricCommandEvidence:
    stage: str
    node: str
    hca: str
    argv: list[str]
    returncode: int
    duration_seconds: float
    timed_out: bool
    stdout: str
    stderr: str
    deadline_exceeded: bool = False
    stdout_total_bytes: int = 0
    stderr_total_bytes: int = 0
    stdout_truncated: bool = False
    stderr_truncated: bool = False


@dataclass
class FabricIssue:
    stage: str
    node: str
    hca: str
    status: str
    reason_code: str
    message: str


@dataclass
class FabricLink:
    node: str
    hca: str
    ib_port: int
    source_guid: str
    source_name: str
    source_lid: int
    source_port: int
    switch_name: str
    switch_guid: str
    switch_lid: int
    switch_port: int
    width: str
    speed: float
    lane_count: int | None
    lane_speed_gbps: float | None
    aggregate_speed_gbps: float | None
    speed_unit: str
    rate: str
    state: str
    physical_state: str
    state_status: str
    performance_status: str
    performance_reason_code: str
    status: str
    @property
    def leaf_key(self) -> tuple[str, int, int]:
        return (self.switch_guid.lower(), self.switch_lid, self.switch_port)


@dataclass
class FabricCounterSourceEvidence:
    source: str
    attribute: str
    width_bits: dict[str, int]
    before_query_status: str
    after_query_status: str
    before: dict[str, int] = field(default_factory=dict)
    after: dict[str, int] = field(default_factory=dict)
    deltas: dict[str, int] = field(default_factory=dict)
    status: str = 'UNKNOWN'
    reason_code: str = 'COUNTER_SOURCE_NOT_EVALUATED'
    used_for_congestion_verdict: bool = False


@dataclass
class FabricCounterHealth:
    node: str
    hca: str
    ib_port: int
    switch_name: str
    switch_guid: str
    switch_lid: int
    switch_port: int
    status: str
    reason_code: str
    message: str
    sample_interval_seconds: float
    before: dict[str, int] = field(default_factory=dict)
    after: dict[str, int] = field(default_factory=dict)
    deltas: dict[str, int] = field(default_factory=dict)
    failure_deltas: dict[str, int] = field(default_factory=dict)
    congestion_deltas: dict[str, int] = field(default_factory=dict)
    saturated_counters: list[str] = field(default_factory=list)
    reset_or_wrapped_counters: list[str] = field(default_factory=list)
    missing_counters: list[str] = field(default_factory=list)
    historical_nonzero_stable: dict[str, int] = field(default_factory=dict)
    error_drop_status: str = 'UNKNOWN'
    error_drop_reason_code: str = 'STANDARD_ERROR_COUNTERS_NOT_EVALUATED'
    congestion_status: str = 'UNKNOWN'
    congestion_reason_code: str = 'XMIT_WAIT_NOT_EVALUATED'
    effective_xmit_wait_source: str = 'NONE'
    standard_counter_evidence: FabricCounterSourceEvidence | None = None
    extended_xmit_wait_evidence: FabricCounterSourceEvidence | None = None


@dataclass
class IBFabricCheckResult:
    status: str
    reason_code: str
    message: str
    job_id: str
    nodes: list[str]
    started_at: str
    finished_at: str
    sample_interval_seconds: float
    expected_link_width: str | None = None
    minimum_link_speed_gbps: float | None = None
    max_workers: int = 16
    overall_timeout_seconds: float = 900.0
    adjacency_links: list[FabricLink] = field(default_factory=list)
    counter_health: list[FabricCounterHealth] = field(default_factory=list)
    issues: list[FabricIssue] = field(default_factory=list)
    commands: list[FabricCommandEvidence] = field(default_factory=list)
    allocation: dict[str, Any] | None = None
    safety_boundary: str = UNPROVEN_SLURM_SAFETY_BOUNDARY
    switch_configuration_policy: dict[str, str] = field(
        default_factory=lambda: {
            "status": "NOT_VERIFIED",
            "reason_code": "SWITCH_MANAGEMENT_EVIDENCE_NOT_PROVIDED",
            "message": (
                "one-hop host MAD evidence does not prove the switch desired "
                "configuration, full-fabric routing/QoS, firmware, optics, or event history"
            ),
        }
    )
    tool_version: str = __version__

    def to_dict(self) -> dict[str, Any]:
        if self.status not in FABRIC_STATUSES:
            raise ValueError(f"invalid fabric result status: {self.status}")
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# Slurm Native-IB one-hop fabric check",
            "",
            "| Item | Result |",
            "|---|---|",
            f"| Verdict | {_markdown(self.status)} |",
            f"| Reason | {_markdown(self.reason_code)} |",
            f"| Detail | {_markdown(self.message)} |",
            f"| Job | {_markdown(self.job_id)} |",
            f"| Nodes | {_markdown(', '.join(self.nodes))} |",
            f"| Counter interval | {self.sample_interval_seconds:g} seconds |",
            f"| Expected link width | {_markdown(self.expected_link_width or 'NOT_PROVIDED')} |",
            f"| Minimum aggregate link speed | {_markdown(self.minimum_link_speed_gbps if self.minimum_link_speed_gbps is not None else 'NOT_PROVIDED')} Gbps |",
            f"| Fabric workers | {self.max_workers} |",
            f"| Overall deadline | {self.overall_timeout_seconds:g} seconds |",
            f"| Truncated command outputs | {sum(1 for command in self.commands if command.stdout_truncated or command.stderr_truncated)} |",
            f"| Slurm safety boundary | {_markdown(self.safety_boundary)} |",
            (
                "| Switch configuration policy | "
                f"{_markdown(self.switch_configuration_policy['status'])}: "
                f"{_markdown(self.switch_configuration_policy['reason_code'])} |"
            ),
            "",
            "## One-hop adjacency",
            "",
            "| Node | HCA | Source LID/port | Leaf switch | GUID | Leaf LID/port | Link | Lane rate | Aggregate rate | State | Rate policy | Result |",
            "|---|---|---|---|---|---:|---|---|---|---|---|---|",
        ]
        for link in self.adjacency_links:
            lines.append(
                "| "
                + " | ".join(
                    _markdown(value)
                    for value in (
                        link.node,
                        link.hca,
                        f"{link.source_lid}/{link.source_port}",
                        link.switch_name,
                        link.switch_guid,
                        f"{link.switch_lid}/{link.switch_port}",
                        f"{link.state}/{link.physical_state}",
                        f"{link.speed:g} {link.speed_unit}",
                        (
                            f"{link.aggregate_speed_gbps:g} Gbps"
                            if link.aggregate_speed_gbps is not None
                            else "NOT_VERIFIED"
                        ),
                        link.state_status,
                        f"{link.performance_status}:{link.performance_reason_code}",
                        link.status,
                    )
                )
                + " |"
            )
        if not self.adjacency_links:
            lines.append("| - | - | - | - | - | - | - | - | - | NOT_VERIFIED | NOT_VERIFIED | NOT_VERIFIED |")

        lines.extend(
            [
                "",
                "## Leaf-port counter health",
                "",
                "| Leaf switch/port | Query route | Result | Error/drop | Congestion | Effective XmitWait | Error/drop delta | Congestion delta | Saturated | Reason |",
                "|---|---|---|---|---|---|---|---|---|---|",
            ]
        )
        for item in self.counter_health:
            lines.append(
                "| "
                + " | ".join(
                    _markdown(value)
                    for value in (
                        f"{item.switch_name}:{item.switch_lid}/{item.switch_port}",
                        f"{item.node}/{item.hca}:{item.ib_port}",
                        item.status,
                        f"{item.error_drop_status}:{item.error_drop_reason_code}",
                        f"{item.congestion_status}:{item.congestion_reason_code}",
                        item.effective_xmit_wait_source,
                        item.failure_deltas or "-",
                        item.congestion_deltas or "-",
                        item.saturated_counters or "-",
                        item.reason_code,
                    )
                )
                + " |"
            )
        if not self.counter_health:
            lines.append("| - | - | UNKNOWN | UNKNOWN | UNKNOWN | NONE | - | - | - | COUNTERS_NOT_SAMPLED |")

        lines.extend(
            [
                "",
                "## Counter source evidence",
                "",
                "| Leaf switch/port | Source | Attribute | Width bits | Before query | After query | Before | After | Delta | Used for XmitWait verdict | Result |",
                "|---|---|---|---|---|---|---|---|---|---|---|",
            ]
        )
        for item in self.counter_health:
            for evidence in (
                item.standard_counter_evidence,
                item.extended_xmit_wait_evidence,
            ):
                if evidence is None:
                    continue
                lines.append(
                    "| "
                    + " | ".join(
                        _markdown(value)
                        for value in (
                            f"{item.switch_name}:{item.switch_lid}/{item.switch_port}",
                            evidence.source,
                            evidence.attribute,
                            evidence.width_bits,
                            evidence.before_query_status,
                            evidence.after_query_status,
                            evidence.before,
                            evidence.after,
                            evidence.deltas or "-",
                            evidence.used_for_congestion_verdict,
                            f"{evidence.status}:{evidence.reason_code}",
                        )
                    )
                    + " |"
                )
        if not any(
            item.standard_counter_evidence or item.extended_xmit_wait_evidence
            for item in self.counter_health
        ):
            lines.append("| - | - | - | - | NOT_EXECUTED | NOT_EXECUTED | - | - | - | False | UNKNOWN |")

        lines.extend(["", "## Inspection issues", ""])
        if self.issues:
            lines.extend(
                [
                    "| Stage | Node/HCA | Status | Reason | Detail |",
                    "|---|---|---|---|---|",
                ]
            )
            for issue in self.issues:
                lines.append(
                    f"| {_markdown(issue.stage)} | {_markdown(issue.node + '/' + issue.hca)} "
                    f"| {_markdown(issue.status)} | {_markdown(issue.reason_code)} "
                    f"| {_markdown(issue.message)} |"
                )
        else:
            lines.append("No tooling, permission, timeout, or parsing issue was observed.")
        lines.extend(
            [
                "",
                "## Boundary",
                "",
                "This result covers only the selected endpoints, their one-hop leaf links, and "
                "targeted leaf-port counter deltas. It intentionally does not claim that the "
                "full switch configuration or full fabric is verified.",
                "",
            ]
        )
        return "\n".join(lines)


def parse_iblinkinfo(
    output: str,
    *,
    node: str,
    hca: str,
    ib_port: int,
    expected_link_width: str | None = None,
    minimum_link_speed_gbps: float | None = None,
) -> FabricLink | None:
    """Parse one directed-route link and evaluate explicit rate policy."""

    for line in output.splitlines():
        match = _LINK_RE.search(line)
        if not match:
            continue
        values = match.groupdict()
        state = values["state"].strip()
        physical = values["physical_state"].strip()
        width = values["width"].strip()
        speed = float(values["speed"])
        speed_unit = values["speed_unit"].strip()
        width_match = re.fullmatch(r"([1-9][0-9]*)X", width, re.I)
        lane_count = int(width_match.group(1)) if width_match else None
        speed_is_gbps = speed_unit.casefold() in {"gbps", "gbit/s", "gb/sec"}
        lane_speed_gbps = speed if speed_is_gbps else None
        aggregate_speed_gbps = (
            lane_count * lane_speed_gbps
            if lane_count is not None and lane_speed_gbps is not None
            else None
        )
        state_status = (
            "PASS"
            if state.casefold() == "active" and physical.casefold() == "linkup"
            else "FAIL"
        )
        if expected_link_width is None or minimum_link_speed_gbps is None:
            performance_status = "NOT_VERIFIED"
            performance_reason = "IB_LINK_RATE_POLICY_NOT_PROVIDED"
        elif not speed_is_gbps:
            performance_status = "NOT_VERIFIED"
            performance_reason = "IB_LINK_SPEED_UNIT_UNSUPPORTED"
        elif aggregate_speed_gbps is None:
            performance_status = "NOT_VERIFIED"
            performance_reason = "IB_LINK_WIDTH_UNPARSABLE"
        elif (
            width.upper() != expected_link_width.upper()
            or aggregate_speed_gbps < minimum_link_speed_gbps
        ):
            performance_status = "FAIL"
            performance_reason = "IB_LINK_WIDTH_OR_SPEED_BELOW_POLICY"
        else:
            performance_status = "PASS"
            performance_reason = "IB_LINK_WIDTH_AND_SPEED_MATCH_POLICY"
        status = (
            "FAIL"
            if state_status == "FAIL" or performance_status == "FAIL"
            else "PASS"
            if performance_status == "PASS"
            else "NOT_VERIFIED"
        )
        return FabricLink(
            node=node,
            hca=hca,
            ib_port=ib_port,
            source_guid=values["source_guid"].lower(),
            source_name=values["source_name"].strip(),
            source_lid=int(values["source_lid"]),
            source_port=int(values["source_port"]),
            switch_name=values["switch_name"].strip(),
            switch_guid=values["switch_guid"].lower(),
            switch_lid=int(values["switch_lid"]),
            switch_port=int(values["switch_port"]),
            width=width,
            speed=speed,
            lane_count=lane_count,
            lane_speed_gbps=lane_speed_gbps,
            aggregate_speed_gbps=aggregate_speed_gbps,
            speed_unit=speed_unit,
            rate=f"{width} {speed:g} {speed_unit}",
            state=state,
            physical_state=physical,
            state_status=state_status,
            performance_status=performance_status,
            performance_reason_code=performance_reason,
            status=status,
        )
    return None

def _parse_smpquery_fields(output: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in output.splitlines():
        match = _SMP_FIELD_RE.match(line)
        if not match:
            continue
        key = re.sub(r"[^a-z0-9]", "", match.group("name").casefold())
        if key:
            fields[key] = match.group("value").strip()
    return fields


def _smp_field(fields: Mapping[str, str], *names: str) -> str | None:
    for name in names:
        value = fields.get(re.sub(r"[^a-z0-9]", "", name.casefold()))
        if value:
            return value
    return None


def _smp_integer(value: str | None) -> int | None:
    if value is None:
        return None
    match = re.search(r"0x[0-9a-fA-F]+|[0-9]+", value)
    if not match:
        return None
    try:
        token = match.group(0)
        return int(token, 16 if token.casefold().startswith("0x") else 10)
    except ValueError:
        return None


def parse_smpquery_fabric_link(
    *,
    local_portinfo: str,
    leaf_nodeinfo: str,
    leaf_nodedesc: str,
    leaf_portinfo: str,
    node: str,
    hca: str,
    ib_port: int,
    expected_link_width: str | None = None,
    minimum_link_speed_gbps: float | None = None,
) -> tuple[FabricLink | None, str | None, str | None]:
    """Compose one endpoint-to-leaf link from four fixed directed-route SMPs."""

    local = _parse_smpquery_fields(local_portinfo)
    leaf_node = _parse_smpquery_fields(leaf_nodeinfo)
    leaf_desc = _parse_smpquery_fields(leaf_nodedesc)
    leaf_port = _parse_smpquery_fields(leaf_portinfo)

    node_type = _smp_field(leaf_node, "NodeType")
    if node_type is not None and not re.search(r"\bSwitch\b", node_type, re.I):
        return (
            None,
            "SMPQUERY_ONE_HOP_NEIGHBOR_NOT_SWITCH",
            f"directed-route neighbor NodeType is {node_type!r}, not Switch",
        )

    local_lid = _smp_integer(_smp_field(local, "Lid"))
    source_guid_text = _smp_field(local, "Guid", "PortGuid")
    source_guid_match = _GUID_RE.search(source_guid_text or "")
    width_text = _smp_field(local, "LinkWidthActive")
    width_match = re.search(r"\b([1-9][0-9]*X)\b", width_text or "", re.I)
    width = width_match.group(1).upper() if width_match else None
    state_text = _smp_field(local, "LinkState")
    physical_text = _smp_field(local, "PhysLinkState")
    speed_text = _smp_field(local, "LinkSpeedExtActive", "LinkSpeedActive")
    speed_match = _SPEED_RE.search(speed_text or "")

    guid_text = _smp_field(leaf_node, "Guid", "NodeGuid", "SystemGuid")
    guid_match = _GUID_RE.search(guid_text or "")
    leaf_node_port = _smp_integer(_smp_field(leaf_node, "LocalPort"))
    switch_name = _smp_field(leaf_desc, "Node Description", "NodeDescription")
    switch_lid = _smp_integer(_smp_field(leaf_port, "Lid"))
    leaf_port_number = _smp_integer(_smp_field(leaf_port, "LocalPort"))

    missing: list[str] = []
    required_values = {
        "local.Lid": local_lid,
        "local.LinkWidthActive": width,
        "local.LinkState": state_text,
        "local.PhysLinkState": physical_text,
        "local.LinkSpeedExtActive": speed_match,
        "leaf.NodeType": node_type,
        "leaf.Guid": guid_match,
        "leaf.LocalPort": leaf_node_port,
        "leaf.NodeDescription": switch_name,
        "leaf.PortInfo.Lid": switch_lid,
        "leaf.PortInfo.LocalPort": leaf_port_number,
    }
    missing.extend(name for name, value in required_values.items() if value is None)
    if local_lid is not None and local_lid <= 0:
        missing.append("local.Lid(valid)")
    if leaf_node_port is not None and leaf_node_port <= 0:
        missing.append("leaf.LocalPort(valid)")
    if switch_lid is not None and switch_lid <= 0:
        missing.append("leaf.PortInfo.Lid(valid)")
    if leaf_port_number is not None and leaf_port_number <= 0:
        missing.append("leaf.PortInfo.LocalPort(valid)")
    if guid_match is not None and int(guid_match.group(0), 16) == 0:
        missing.append("leaf.Guid(valid)")
    if missing:
        return (
            None,
            "SMPQUERY_ONE_HOP_EVIDENCE_MISSING",
            "critical directed-route fields are missing or invalid: "
            + ", ".join(sorted(set(missing))),
        )
    if leaf_node_port != leaf_port_number:
        return (
            None,
            "SMPQUERY_LEAF_PORT_MISMATCH",
            f"NodeInfo LocalPort {leaf_node_port} disagrees with PortInfo LocalPort {leaf_port_number}",
        )

    assert speed_match is not None
    assert width is not None
    assert local_lid is not None
    assert state_text is not None
    assert physical_text is not None
    assert guid_match is not None
    assert leaf_node_port is not None
    assert switch_name is not None
    assert switch_lid is not None

    speed = float(speed_match.group("speed"))
    speed_unit = speed_match.group("unit")
    lane_count = int(width[:-1])
    lane_speed_gbps = speed
    aggregate_speed_gbps = lane_count * lane_speed_gbps
    state = "Active" if re.search(r"\bActive\b", state_text, re.I) else state_text
    physical_state = (
        "LinkUp" if re.search(r"\bLinkUp\b", physical_text, re.I) else physical_text
    )
    state_status = (
        "PASS"
        if state.casefold() == "active" and physical_state.casefold() == "linkup"
        else "FAIL"
    )
    if expected_link_width is None or minimum_link_speed_gbps is None:
        performance_status = "NOT_VERIFIED"
        performance_reason = "IB_LINK_RATE_POLICY_NOT_PROVIDED"
    elif (
        width.upper() != expected_link_width.upper()
        or aggregate_speed_gbps < minimum_link_speed_gbps
    ):
        performance_status = "FAIL"
        performance_reason = "IB_LINK_WIDTH_OR_SPEED_BELOW_POLICY"
    else:
        performance_status = "PASS"
        performance_reason = "IB_LINK_WIDTH_AND_SPEED_MATCH_POLICY"
    status = (
        "FAIL"
        if state_status == "FAIL" or performance_status == "FAIL"
        else "PASS"
        if performance_status == "PASS"
        else "NOT_VERIFIED"
    )
    return (
        FabricLink(
            node=node,
            hca=hca,
            ib_port=ib_port,
            source_guid=(
                source_guid_match.group(0).lower() if source_guid_match else ""
            ),
            source_name=f"{node} {hca}",
            source_lid=local_lid,
            source_port=ib_port,
            switch_name=switch_name,
            switch_guid=guid_match.group(0).lower(),
            switch_lid=switch_lid,
            switch_port=leaf_node_port,
            width=width,
            speed=speed,
            lane_count=lane_count,
            lane_speed_gbps=lane_speed_gbps,
            aggregate_speed_gbps=aggregate_speed_gbps,
            speed_unit=speed_unit,
            rate=f"{width} {speed:g} {speed_unit}",
            state=state,
            physical_state=physical_state,
            state_status=state_status,
            performance_status=performance_status,
            performance_reason_code=performance_reason,
            status=status,
        ),
        None,
        None,
    )

def parse_perfquery_counters(output: str) -> dict[str, int]:
    """Parse integer counters without treating historical values as failures."""

    counters: dict[str, int] = {}
    for line in output.splitlines():
        match = _COUNTER_RE.match(line)
        if match:
            counters[match.group("name")] = int(match.group("value"), 0)
    return counters


def parse_extended_xmit_wait(output: str) -> dict[str, int]:
    '''Parse only a genuine PortCountersExtended PortXmitWait field.

    The attribute header and exact field name are both required. Numeric range
    is evaluated later against the attribute-defined 64-bit width.
    '''

    in_extended_attribute = False
    for line in output.splitlines():
        if _EXTENDED_COUNTER_HEADER_RE.match(line):
            in_extended_attribute = True
            continue
        if in_extended_attribute and line.lstrip().startswith('#'):
            break
        if not in_extended_attribute:
            continue
        match = _COUNTER_RE.match(line)
        if match and match.group('name') == 'PortXmitWait':
            return {'PortXmitWait': int(match.group('value'), 0)}
    return {}


def _counter_severity(name: str) -> str | None:
    if name in FAIL_COUNTERS:
        return "FAIL"
    if _WARN_COUNTER_RE.fullmatch(name):
        return "WARN"
    return None


def evaluate_fabric_counter_delta(
    before: Mapping[str, int],
    after: Mapping[str, int],
    *,
    route: FabricLink,
    sample_interval_seconds: float,
    extended_before: Mapping[str, int] | None = None,
    extended_after: Mapping[str, int] | None = None,
    extended_before_query_status: str = "NOT_EXECUTED",
    extended_after_query_status: str = "NOT_EXECUTED",
) -> FabricCounterHealth:
    """Evaluate standard counters plus a read-only 64-bit XmitWait fallback.

    Standard PortCounters remain authoritative for error/drop/link-down fields.
    PortCountersExtended can close only an otherwise UNKNOWN PortXmitWait
    dimension. Attribute and width are carried explicitly and never inferred
    from the observed values.
    """

    extended_before_values = dict(extended_before or {})
    extended_after_values = dict(extended_after or {})
    standard_before_values = {
        name: int(before[name])
        for name in REQUIRED_STANDARD_COUNTERS
        if name in before
    }
    standard_after_values = {
        name: int(after[name])
        for name in REQUIRED_STANDARD_COUNTERS
        if name in after
    }
    standard_evidence = FabricCounterSourceEvidence(
        source="perfquery",
        attribute="PortCounters",
        width_bits={
            name: maximum.bit_length()
            for name, maximum in STANDARD_COUNTER_MAX_VALUES.items()
        },
        before_query_status="COMPLETE",
        after_query_status="COMPLETE",
        before=standard_before_values,
        after=standard_after_values,
    )
    extended_evidence = FabricCounterSourceEvidence(
        source="perfquery",
        attribute="PortCountersExtended",
        width_bits={"PortXmitWait": 64},
        before_query_status=extended_before_query_status,
        after_query_status=extended_after_query_status,
        before=(
            {"PortXmitWait": int(extended_before_values["PortXmitWait"])}
            if "PortXmitWait" in extended_before_values
            else {}
        ),
        after=(
            {"PortXmitWait": int(extended_after_values["PortXmitWait"])}
            if "PortXmitWait" in extended_after_values
            else {}
        ),
    )

    observed_relevant = {
        name
        for name in set(before) | set(after)
        if _counter_severity(name) is not None
    }
    non_xmit_relevant = sorted(
        (observed_relevant | REQUIRED_STANDARD_ERROR_COUNTERS) - {"PortXmitWait"}
    )
    missing: list[str] = []
    saturated: list[str] = []
    reset: list[str] = []
    stable: dict[str, int] = {}
    deltas: dict[str, int] = {}
    failures: dict[str, int] = {}
    auxiliary_congestion: dict[str, int] = {}
    error_evidence_incomplete = False

    for name in non_xmit_relevant:
        severity = _counter_severity(name)
        if name not in before or name not in after:
            missing.append(name)
            if severity == "FAIL":
                error_evidence_incomplete = True
            continue
        old, new = int(before[name]), int(after[name])
        maximum = STANDARD_COUNTER_MAX_VALUES.get(name)
        if maximum is not None and (old == maximum or new == maximum):
            saturated.append(name)
            if severity == "FAIL":
                error_evidence_incomplete = True
            continue
        if new < old:
            reset.append(name)
            if severity == "FAIL":
                error_evidence_incomplete = True
            continue
        delta = new - old
        deltas[name] = delta
        if delta > 0 and severity == "FAIL":
            failures[name] = delta
        elif delta > 0 and severity == "WARN":
            auxiliary_congestion[name] = delta
        elif delta == 0 and new > 0:
            stable[name] = new

    if failures:
        error_drop_status = "FAIL"
        error_drop_reason = "STANDARD_ERROR_OR_DROP_GREW"
    elif error_evidence_incomplete:
        error_drop_status = "UNKNOWN"
        error_drop_reason = "STANDARD_ERROR_COUNTER_EVIDENCE_INCOMPLETE"
    else:
        error_drop_status = "PASS"
        error_drop_reason = "STANDARD_ERROR_COUNTERS_STABLE"

    standard_xmit_status = "UNKNOWN"
    standard_xmit_reason = "STANDARD_XMIT_WAIT_MISSING"
    standard_xmit_delta: int | None = None
    if "PortXmitWait" not in before or "PortXmitWait" not in after:
        missing.append("PortXmitWait")
    else:
        old = int(before["PortXmitWait"])
        new = int(after["PortXmitWait"])
        if (
            old == STANDARD_COUNTER_MAX_VALUES["PortXmitWait"]
            or new == STANDARD_COUNTER_MAX_VALUES["PortXmitWait"]
        ):
            saturated.append("PortXmitWait")
            standard_xmit_reason = "STANDARD_XMIT_WAIT_SATURATED"
        elif new < old:
            reset.append("PortXmitWait")
            standard_xmit_reason = "STANDARD_XMIT_WAIT_RESET_OR_WRAPPED"
        else:
            standard_xmit_delta = new - old
            deltas["PortXmitWait"] = standard_xmit_delta
            standard_evidence.deltas["PortXmitWait"] = standard_xmit_delta
            if standard_xmit_delta > 0:
                standard_xmit_status = "WARN"
                standard_xmit_reason = "STANDARD_XMIT_WAIT_GREW"
            else:
                standard_xmit_status = "PASS"
                standard_xmit_reason = "STANDARD_XMIT_WAIT_STABLE"
                if new > 0:
                    stable["PortXmitWait"] = new

    extended_status = "UNKNOWN"
    extended_reason = "EXTENDED_XMIT_WAIT_NOT_EXPOSED"
    extended_delta: int | None = None
    query_statuses = {
        extended_before_query_status,
        extended_after_query_status,
    }
    if query_statuses != {"COMPLETE"}:
        if "OUTPUT_TRUNCATED" in query_statuses:
            extended_reason = "EXTENDED_XMIT_WAIT_OUTPUT_TRUNCATED"
        elif "NOT_EXECUTED" in query_statuses:
            extended_reason = "EXTENDED_XMIT_WAIT_NOT_EXECUTED"
        else:
            extended_reason = "EXTENDED_XMIT_WAIT_QUERY_FAILED"
    elif (
        "PortXmitWait" not in extended_before_values
        or "PortXmitWait" not in extended_after_values
    ):
        extended_reason = "EXTENDED_XMIT_WAIT_NOT_EXPOSED"
    else:
        extended_old = int(extended_before_values["PortXmitWait"])
        extended_new = int(extended_after_values["PortXmitWait"])
        if not (
            0 <= extended_old <= EXTENDED_COUNTER_MAX_VALUES["PortXmitWait"]
            and 0 <= extended_new <= EXTENDED_COUNTER_MAX_VALUES["PortXmitWait"]
        ):
            extended_reason = "EXTENDED_XMIT_WAIT_OUT_OF_RANGE"
        elif (
            extended_old == EXTENDED_COUNTER_MAX_VALUES["PortXmitWait"]
            or extended_new == EXTENDED_COUNTER_MAX_VALUES["PortXmitWait"]
        ):
            extended_reason = "EXTENDED_XMIT_WAIT_SATURATED"
        elif extended_new < extended_old:
            extended_reason = "EXTENDED_XMIT_WAIT_RESET_OR_WRAPPED"
        else:
            extended_delta = extended_new - extended_old
            extended_evidence.deltas["PortXmitWait"] = extended_delta
            if extended_delta > 0:
                extended_status = "WARN"
                extended_reason = "EXTENDED_XMIT_WAIT_GREW"
            else:
                extended_status = "PASS"
                extended_reason = "EXTENDED_XMIT_WAIT_STABLE"
    extended_evidence.status = extended_status
    extended_evidence.reason_code = extended_reason

    congestion: dict[str, int] = dict(auxiliary_congestion)
    if standard_xmit_status in {"PASS", "WARN"}:
        congestion_status = standard_xmit_status
        congestion_reason = standard_xmit_reason
        effective_xmit_wait_source = "PortCounters"
        standard_evidence.used_for_congestion_verdict = True
        if standard_xmit_delta and standard_xmit_delta > 0:
            congestion["PortXmitWait"] = standard_xmit_delta
    elif extended_status in {"PASS", "WARN"}:
        congestion_status = extended_status
        congestion_reason = extended_reason
        effective_xmit_wait_source = "PortCountersExtended"
        extended_evidence.used_for_congestion_verdict = True
        if extended_delta and extended_delta > 0:
            congestion["PortXmitWait"] = extended_delta
    else:
        congestion_status = "UNKNOWN"
        congestion_reason = extended_reason
        effective_xmit_wait_source = "NONE"
        if extended_reason == "EXTENDED_XMIT_WAIT_NOT_EXPOSED":
            missing.append("PortXmitWait[PortCountersExtended]")
        elif extended_reason == "EXTENDED_XMIT_WAIT_SATURATED":
            saturated.append("PortXmitWait[PortCountersExtended]")
        elif extended_reason == "EXTENDED_XMIT_WAIT_RESET_OR_WRAPPED":
            reset.append("PortXmitWait[PortCountersExtended]")

    if congestion_status == "PASS" and auxiliary_congestion:
        congestion_status = "WARN"
        congestion_reason = "AUXILIARY_CONGESTION_COUNTER_GREW"

    if error_drop_status == "FAIL":
        standard_evidence.status = "FAIL"
        standard_evidence.reason_code = error_drop_reason
    elif error_drop_status == "UNKNOWN" or standard_xmit_status == "UNKNOWN":
        standard_evidence.status = "UNKNOWN"
        standard_evidence.reason_code = (
            error_drop_reason
            if error_drop_status == "UNKNOWN"
            else standard_xmit_reason
        )
    elif standard_xmit_status == "WARN":
        standard_evidence.status = "WARN"
        standard_evidence.reason_code = standard_xmit_reason
    else:
        standard_evidence.status = "PASS"
        standard_evidence.reason_code = "STANDARD_COUNTERS_STABLE"
    standard_evidence.deltas.update(
        {
            name: delta
            for name, delta in deltas.items()
            if name in REQUIRED_STANDARD_COUNTERS
        }
    )

    if failures:
        status = "FAIL"
        reason = "LEAF_PORT_ERROR_OR_DROP_GREW"
        message = "one or more standard leaf-port error, link-down, or discard counters increased"
    elif error_drop_status == "UNKNOWN":
        status = "UNKNOWN"
        reason = (
            "LEAF_PORT_COUNTER_SATURATED"
            if saturated
            else "LEAF_PORT_COUNTER_RESET_OR_WRAPPED"
            if reset
            else "LEAF_PORT_COUNTER_EVIDENCE_INCOMPLETE"
        )
        message = "standard error/drop counter health cannot be fully evaluated"
    elif congestion_status == "UNKNOWN":
        status = "UNKNOWN"
        reason = (
            "LEAF_PORT_COUNTER_SATURATED"
            if saturated
            else "LEAF_PORT_COUNTER_RESET_OR_WRAPPED"
            if reset
            else "LEAF_PORT_COUNTER_EVIDENCE_INCOMPLETE"
        )
        message = "PortXmitWait is invalid and no valid read-only 64-bit fallback is available"
    elif congestion_status == "WARN":
        status = "WARN"
        reason = "LEAF_PORT_CONGESTION_GREW"
        message = "one or more evaluated congestion counters increased"
    else:
        status = "PASS"
        reason = "LEAF_PORT_COUNTERS_STABLE"
        message = "standard error/drop counters and the effective PortXmitWait source are stable"

    return FabricCounterHealth(
        node=route.node,
        hca=route.hca,
        ib_port=route.ib_port,
        switch_name=route.switch_name,
        switch_guid=route.switch_guid,
        switch_lid=route.switch_lid,
        switch_port=route.switch_port,
        status=status,
        reason_code=reason,
        message=message,
        sample_interval_seconds=sample_interval_seconds,
        before=dict(before),
        after=dict(after),
        deltas=deltas,
        failure_deltas=failures,
        congestion_deltas=congestion,
        saturated_counters=sorted(set(saturated)),
        reset_or_wrapped_counters=sorted(set(reset)),
        missing_counters=sorted(set(missing)),
        historical_nonzero_stable=stable,
        error_drop_status=error_drop_status,
        error_drop_reason_code=error_drop_reason,
        congestion_status=congestion_status,
        congestion_reason_code=congestion_reason,
        effective_xmit_wait_source=effective_xmit_wait_source,
        standard_counter_evidence=standard_evidence,
        extended_xmit_wait_evidence=extended_evidence,
    )


def _classify_command_failure(evidence: FabricCommandEvidence) -> tuple[str, str]:
    text = f"{evidence.stdout}\n{evidence.stderr}".casefold()
    if evidence.deadline_exceeded:
        return "FABRIC_GLOBAL_DEADLINE_EXCEEDED", "the bounded fabric inspection reached its global deadline"
    if "FABRIC_NODE_EXECUTION_SLOT_TIMEOUT" in text:
        return "FABRIC_NODE_EXECUTION_SLOT_TIMEOUT", "waiting for the node exclusive execution slot timed out"
    if evidence.timed_out:
        return "FABRIC_COMMAND_TIMEOUT", "the bounded Slurm fabric query timed out"
    if any(
        marker in text
        for marker in (
            "no such container",
            "is not running",
            "cannot connect to the docker daemon",
            "permission denied while trying to connect to the docker daemon",
        )
    ):
        return "FABRIC_CONTAINER_UNAVAILABLE", "the explicitly selected Docker container cannot execute the query"
    if any(
        marker in text
        for marker in (
            "can't open umad",
            "cannot open umad",
            "failed to open umad",
            "mad_rpc_open_port: can't open",
        )
    ):
        return "UMAD_PERMISSION_DENIED", "UMAD access is unavailable to the selected execution context"
    if any(
        marker in text
        for marker in (
            "command not found",
            "executable file not found",
            "no such file or directory",
            "execve():",
        )
    ):
        return "FABRIC_TOOL_MISSING", "smpquery or perfquery is unavailable in the selected execution context"
    if "permission denied" in text:
        return "FABRIC_EXECUTION_PERMISSION_DENIED", "the selected execution context lacks permission to run the fabric query"
    return "FABRIC_COMMAND_FAILED", "the bounded Slurm fabric query failed"


def _counter_query_status(evidence: FabricCommandEvidence) -> str:
    if evidence.stdout_truncated or evidence.stderr_truncated:
        return 'OUTPUT_TRUNCATED'
    if evidence.returncode != 0 or evidence.timed_out:
        return 'QUERY_FAILED'
    return 'COMPLETE'


class SlurmIBFabricRunner:
    """Run fixed, low-pressure one-hop Native-IB management queries via Slurm."""

    def __init__(
        self,
        *,
        runner: Run = subprocess.run,
        popen: Popen = subprocess.Popen,
        sleeper: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        allocation_inspector: Callable[[SlurmActiveContext], SlurmAllocation] | None = None,
    ):
        self._runner = runner
        self._popen = popen
        self._sleep = sleeper
        self._monotonic = monotonic
        if allocation_inspector is None:
            safety_runner = SlurmActiveCheckRunner(runner=runner, sleeper=sleeper)
            allocation_inspector = safety_runner.inspect_allocation
        self._inspect_allocation = allocation_inspector
        self._query_started = False
        self._rate_lock = Lock()
        self._node_locks_guard = Lock()
        self._node_locks: dict[str, Any] = {}
        self._last_query_started = 0.0
        self._deadline_monotonic: float | None = None

    @staticmethod
    def _srun_prefix(context: SlurmActiveContext, node: str) -> list[str]:
        isolation = (
            ["--overlap"]
            if context.unsafe_allow_overlap
            else ["--exclusive", "--exact", "--immediate=1"]
        )
        return [
            context.srun_executable,
            f"--jobid={context.job_id}",
            *isolation,
            "--nodes=1",
            "--ntasks=1",
            f"--nodelist={node}",
            "--cpu-bind=none",
            "--kill-on-bad-exit=1",
        ]

    @staticmethod
    def _execution_argv(config: IBFabricCheckConfig, tool_argv: Sequence[str]) -> list[str]:
        if config.container_name is None:
            return list(tool_argv)
        return [
            config.docker_executable,
            "exec",
            config.container_name,
            *tool_argv,
        ]

    @staticmethod
    def _directed_smp_tools(
        config: IBFabricCheckConfig, hca: str
    ) -> list[tuple[str, list[str]]]:
        common = [
            "-C", hca, "-P", str(config.ib_port), "-D",
        ]
        return [
            (
                "local-port-info",
                [config.smpquery_executable, "-x", *common, "portinfo", "0", "1"],
            ),
            (
                "leaf-node-info",
                [config.smpquery_executable, *common, "nodeinfo", "0,1"],
            ),
            (
                "leaf-node-description",
                [config.smpquery_executable, *common, "nodedesc", "0,1"],
            ),
            (
                "leaf-port-info",
                [config.smpquery_executable, *common, "portinfo", "0,1", "0"],
            ),
        ]

    def build_link_commands(
        self,
        context: SlurmActiveContext,
        config: IBFabricCheckConfig,
        *,
        node: str,
        hca: str,
    ) -> list[tuple[str, list[str]]]:
        """Build four exact single-target SMPs; no discovery scan is possible."""

        prefix = self._srun_prefix(context, node)
        return [
            (stage, prefix + self._execution_argv(config, tool))
            for stage, tool in self._directed_smp_tools(config, hca)
        ]

    def build_link_command(
        self,
        context: SlurmActiveContext,
        config: IBFabricCheckConfig,
        *,
        node: str,
        hca: str,
    ) -> list[str]:
        """Compatibility helper returning the first fixed local PortInfo query."""

        return self.build_link_commands(
            context, config, node=node, hca=hca
        )[0][1]
    def build_counter_command(
        self,
        context: SlurmActiveContext,
        config: IBFabricCheckConfig,
        *,
        route: FabricLink,
        per_vl_congestion: bool,
        extended: bool = False,
    ) -> list[str]:
        if per_vl_congestion and extended:
            raise ValueError('counter command cannot combine extended and per-VL attributes')
        tool = [
            config.perfquery_executable,
            "-C",
            route.hca,
            "-P",
            str(route.ib_port),
        ]
        if extended:
            tool.append('-x')
        elif per_vl_congestion:
            tool.append("--swportvlcong")
        tool.extend([str(route.switch_lid), str(route.switch_port)])
        return self._srun_prefix(context, route.node) + self._execution_argv(config, tool)

    def _deadline_evidence(
        self,
        *,
        stage: str,
        node: str,
        hca: str,
        argv: list[str],
    ) -> FabricCommandEvidence:
        return FabricCommandEvidence(
            stage=stage,
            node=node,
            hca=hca,
            argv=list(argv),
            returncode=124,
            duration_seconds=0.0,
            timed_out=True,
            stdout="",
            stderr="FABRIC_GLOBAL_DEADLINE_EXCEEDED",
            deadline_exceeded=True,
        )

    def _run_bounded_process(self, argv: list[str], timeout: float) -> dict[str, Any]:
        """Run one fabric query without retaining unbounded controller output."""

        process = self._popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if process.stdout is None or process.stderr is None:
            try:
                process.terminate()
            except OSError:
                pass
            raise OSError("fabric command did not expose stdout/stderr pipes")

        stdout_capture = _BoundedByteCapture()
        stderr_capture = _BoundedByteCapture()
        threads = [
            Thread(target=stdout_capture.drain, args=(process.stdout,), daemon=True),
            Thread(target=stderr_capture.drain, args=(process.stderr,), daemon=True),
        ]
        for thread in threads:
            thread.start()

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
                try:
                    process.wait(timeout=5)
                except (OSError, subprocess.TimeoutExpired):
                    pass
        finally:
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

        return {
            "returncode": int(
                process.returncode
                if process.returncode is not None
                else (124 if timed_out else 1)
            ),
            "timed_out": timed_out,
            "stdout": stdout_capture.render(),
            "stderr": stderr_capture.render(),
            "stdout_total_bytes": stdout_capture.total_bytes,
            "stderr_total_bytes": stderr_capture.total_bytes,
            "stdout_truncated": stdout_capture.truncated,
            "stderr_truncated": stderr_capture.truncated,
        }
    def _node_execution_lock(self, node: str) -> Any:
        with self._node_locks_guard:
            lock = self._node_locks.get(node)
            if lock is None:
                lock = Lock()
                self._node_locks[node] = lock
            return lock

    def _execute(
        self,
        *,
        stage: str,
        node: str,
        hca: str,
        argv: list[str],
        config: IBFabricCheckConfig,
    ) -> FabricCommandEvidence:
        """Serialize exclusive srun steps per node while preserving cross-node overlap."""

        lock = self._node_execution_lock(node)
        wait_started = self._monotonic()
        if self._deadline_monotonic is not None:
            wait_timeout = self._deadline_monotonic - wait_started
            if wait_timeout <= 0:
                return self._deadline_evidence(
                    stage=stage, node=node, hca=hca, argv=argv
                )
        else:
            wait_timeout = config.command_timeout_seconds
        acquired = lock.acquire(timeout=max(0.0, wait_timeout))
        if not acquired:
            if self._deadline_monotonic is not None:
                evidence = self._deadline_evidence(
                    stage=stage, node=node, hca=hca, argv=argv
                )
                evidence.duration_seconds = max(
                    0.0, self._monotonic() - wait_started
                )
                return evidence
            return FabricCommandEvidence(
                stage=stage,
                node=node,
                hca=hca,
                argv=list(argv),
                returncode=124,
                duration_seconds=max(0.0, self._monotonic() - wait_started),
                timed_out=True,
                stdout="",
                stderr="FABRIC_NODE_EXECUTION_SLOT_TIMEOUT",
            )
        try:
            return self._execute_in_node_slot(
                stage=stage,
                node=node,
                hca=hca,
                argv=argv,
                config=config,
            )
        finally:
            lock.release()

    @staticmethod
    def _validate_perfquery_argv(
        argv: Sequence[str],
        config: IBFabricCheckConfig,
        hca: str,
    ) -> None:
        indices = [
            index
            for index, token in enumerate(argv)
            if Path(token).name == 'perfquery'
        ]
        if len(indices) != 1:
            raise ActiveCheckSafetyError(
                'UNBOUNDED_FABRIC_SCAN_REJECTED',
                'fabric counter query must contain exactly one perfquery command',
            )
        tool = list(argv[indices[0]:])
        reset_long_options = {
            '--reset_after_read',
            '--reset-after-read',
            '--reset_only',
            '--reset-only',
        }
        if any(
            token in {'-r', '-R'} or token.casefold() in reset_long_options
            for token in tool
        ):
            raise ActiveCheckSafetyError(
                'FABRIC_COUNTER_RESET_REJECTED',
                'fabric inspection permanently forbids counter reset operations',
            )
        common = [
            config.perfquery_executable,
            '-C',
            hca,
            '-P',
            str(config.ib_port),
        ]
        if len(tool) not in {7, 8} or tool[:5] != common:
            raise ActiveCheckSafetyError(
                'UNBOUNDED_FABRIC_SCAN_REJECTED',
                'perfquery must be a fixed single-leaf-port read',
            )
        target = tool[-2:]
        if not all(re.fullmatch(r'[0-9]+', value) for value in target):
            raise ActiveCheckSafetyError(
                'UNBOUNDED_FABRIC_SCAN_REJECTED',
                'perfquery target LID and port must be explicit decimal integers',
            )
        if int(target[0]) <= 0 or not 1 <= int(target[1]) <= 255:
            raise ActiveCheckSafetyError(
                'UNBOUNDED_FABRIC_SCAN_REJECTED',
                'perfquery target LID or port is outside the fixed safe range',
            )
        option = tool[5:-2]
        if option not in ([], ['-x'], ['--swportvlcong']):
            raise ActiveCheckSafetyError(
                'UNBOUNDED_FABRIC_SCAN_REJECTED',
                'only standard, extended, or per-VL targeted reads are permitted',
            )

    def _execute_in_node_slot(
        self,
        *,
        stage: str,
        node: str,
        hca: str,
        argv: list[str],
        config: IBFabricCheckConfig,
    ) -> FabricCommandEvidence:
        if Path(argv[0]).name not in {"srun", "srun.exe"}:
            raise ActiveCheckSafetyError(
                "LOGIN_NODE_WORKLOAD_REJECTED", "fabric query must start with srun"
            )
        basenames = {Path(token).name for token in argv}
        if "ibnetdiscover" in basenames or "iblinkinfo" in basenames:
            raise ActiveCheckSafetyError(
                "UNBOUNDED_FABRIC_SCAN_REJECTED",
                "adjacency collection permits only fixed single-target smpquery commands",
            )
        if "smpquery" in basenames:
            allowed_tools = [
                tool for _, tool in self._directed_smp_tools(config, hca)
            ]
            if not any(
                len(argv) >= len(tool) and argv[-len(tool):] == tool
                for tool in allowed_tools
            ):
                raise ActiveCheckSafetyError(
                    "UNBOUNDED_FABRIC_SCAN_REJECTED",
                    "smpquery command is not one of the four fixed directed-route targets",
                )

        if 'perfquery' in basenames:
            self._validate_perfquery_argv(argv, config, hca)

        with self._rate_lock:
            now = self._monotonic()
            if self._deadline_monotonic is not None and now >= self._deadline_monotonic:
                return self._deadline_evidence(
                    stage=stage, node=node, hca=hca, argv=argv
                )
            if self._query_started:
                delay = max(
                    0.0,
                    (1.0 / config.query_qps) - (now - self._last_query_started),
                )
                if (
                    self._deadline_monotonic is not None
                    and now + delay >= self._deadline_monotonic
                ):
                    return self._deadline_evidence(
                        stage=stage, node=node, hca=hca, argv=argv
                    )
                if delay:
                    self._sleep(delay)
            self._query_started = True
            self._last_query_started = self._monotonic()

        started = self._monotonic()
        timeout = config.command_timeout_seconds
        if self._deadline_monotonic is not None:
            remaining = self._deadline_monotonic - started
            if remaining <= 0:
                return self._deadline_evidence(
                    stage=stage, node=node, hca=hca, argv=argv
                )
            timeout = min(timeout, remaining)
        try:
            if self._runner is subprocess.run:
                captured = self._run_bounded_process(argv, timeout)
                timed_out = bool(captured["timed_out"])
                deadline_exceeded = bool(
                    timed_out
                    and self._deadline_monotonic is not None
                    and self._monotonic() >= self._deadline_monotonic
                )
                stderr = str(captured["stderr"])
                if deadline_exceeded:
                    stderr = (
                        f"{stderr}\n" if stderr else ""
                    ) + "FABRIC_GLOBAL_DEADLINE_EXCEEDED"
                return FabricCommandEvidence(
                    stage=stage,
                    node=node,
                    hca=hca,
                    argv=list(argv),
                    returncode=124 if timed_out else int(captured["returncode"]),
                    duration_seconds=max(0.0, self._monotonic() - started),
                    timed_out=timed_out,
                    stdout=str(captured["stdout"]),
                    stderr=stderr,
                    deadline_exceeded=deadline_exceeded,
                    stdout_total_bytes=int(captured["stdout_total_bytes"]),
                    stderr_total_bytes=int(captured["stderr_total_bytes"]),
                    stdout_truncated=bool(captured["stdout_truncated"]),
                    stderr_truncated=bool(captured["stderr_truncated"]),
                )

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
            stdout, stdout_total_bytes, stdout_truncated = _bounded(completed.stdout)
            stderr, stderr_total_bytes, stderr_truncated = _bounded(completed.stderr)
            return FabricCommandEvidence(
                stage=stage,
                node=node,
                hca=hca,
                argv=list(argv),
                returncode=int(completed.returncode),
                duration_seconds=max(0.0, self._monotonic() - started),
                timed_out=False,
                stdout=stdout,
                stderr=stderr,
                stdout_total_bytes=stdout_total_bytes,
                stderr_total_bytes=stderr_total_bytes,
                stdout_truncated=stdout_truncated,
                stderr_truncated=stderr_truncated,
            )
        except subprocess.TimeoutExpired as exc:
            stdout = exc.stdout.decode("utf-8", "replace") if isinstance(exc.stdout, bytes) else exc.stdout
            stderr = exc.stderr.decode("utf-8", "replace") if isinstance(exc.stderr, bytes) else exc.stderr
            deadline_exceeded = bool(
                self._deadline_monotonic is not None
                and self._monotonic() >= self._deadline_monotonic
            )
            stderr_source = (
                "FABRIC_GLOBAL_DEADLINE_EXCEEDED" if deadline_exceeded else stderr
            )
            captured_stdout, stdout_total_bytes, stdout_truncated = _bounded(stdout)
            captured_stderr, stderr_total_bytes, stderr_truncated = _bounded(stderr_source)
            return FabricCommandEvidence(
                stage=stage,
                node=node,
                hca=hca,
                argv=list(argv),
                returncode=124,
                duration_seconds=max(0.0, self._monotonic() - started),
                timed_out=True,
                stdout=captured_stdout,
                stderr=captured_stderr,
                deadline_exceeded=deadline_exceeded,
                stdout_total_bytes=stdout_total_bytes,
                stderr_total_bytes=stderr_total_bytes,
                stdout_truncated=stdout_truncated,
                stderr_truncated=stderr_truncated,
            )
        except OSError as exc:
            return FabricCommandEvidence(
                stage=stage,
                node=node,
                hca=hca,
                argv=list(argv),
                returncode=127,
                duration_seconds=max(0.0, self._monotonic() - started),
                timed_out=False,
                stdout="",
                stderr=str(exc),
            )
    def _inspect_one_link(
        self,
        context: SlurmActiveContext,
        config: IBFabricCheckConfig,
        *,
        node: str,
        hca: str,
    ) -> tuple[list[FabricCommandEvidence], FabricLink | None, FabricIssue | None]:
        commands: list[FabricCommandEvidence] = []
        outputs: dict[str, str] = {}
        for stage, argv in self.build_link_commands(
            context, config, node=node, hca=hca
        ):
            evidence = self._execute(
                stage=stage,
                node=node,
                hca=hca,
                argv=argv,
                config=config,
            )
            commands.append(evidence)
            if evidence.returncode != 0 or evidence.timed_out:
                reason, message = _classify_command_failure(evidence)
                return commands, None, FabricIssue(
                    stage=evidence.stage,
                    node=node,
                    hca=hca,
                    status="NOT_VERIFIED",
                    reason_code=reason,
                    message=message,
                )
            outputs[stage] = f"{evidence.stdout}\n{evidence.stderr}"

        link, reason, message = parse_smpquery_fabric_link(
            local_portinfo=outputs["local-port-info"],
            leaf_nodeinfo=outputs["leaf-node-info"],
            leaf_nodedesc=outputs["leaf-node-description"],
            leaf_portinfo=outputs["leaf-port-info"],
            node=node,
            hca=hca,
            ib_port=config.ib_port,
            expected_link_width=config.expected_link_width,
            minimum_link_speed_gbps=config.minimum_link_speed_gbps,
        )
        if link is None:
            return commands, None, FabricIssue(
                stage="one-hop-adjacency",
                node=node,
                hca=hca,
                status="NOT_VERIFIED",
                reason_code=reason or "SMPQUERY_ONE_HOP_EVIDENCE_MISSING",
                message=message or "directed-route one-hop SMP evidence is incomplete",
            )
        return commands, link, None
    @staticmethod
    def _counter_unknown(
        route: FabricLink,
        *,
        reason: str,
        message: str,
        observed_interval: float,
    ) -> FabricCounterHealth:
        return FabricCounterHealth(
            node=route.node,
            hca=route.hca,
            ib_port=route.ib_port,
            switch_name=route.switch_name,
            switch_guid=route.switch_guid,
            switch_lid=route.switch_lid,
            switch_port=route.switch_port,
            status="UNKNOWN",
            reason_code=reason,
            message=message,
            sample_interval_seconds=observed_interval,
        )

    def _sample_one_route(
        self,
        context: SlurmActiveContext,
        config: IBFabricCheckConfig,
        *,
        route: FabricLink,
    ) -> tuple[
        list[FabricCommandEvidence], FabricCounterHealth, list[FabricIssue]
    ]:
        commands: list[FabricCommandEvidence] = []
        issues: list[FabricIssue] = []

        before_parts: list[FabricCommandEvidence] = []
        for per_vl, extended, stage in (
            (False, False, "leaf-counters-before"),
            (False, True, "leaf-counters-extended-before"),
            (True, False, "leaf-congestion-before"),
        ):
            evidence = self._execute(
                stage=stage,
                node=route.node,
                hca=route.hca,
                argv=self.build_counter_command(
                    context,
                    config,
                    route=route,
                    per_vl_congestion=per_vl,
                    extended=extended,
                ),
                config=config,
            )
            commands.append(evidence)
            before_parts.append(evidence)

        standard_before = before_parts[0]
        if standard_before.returncode != 0 or standard_before.timed_out:
            reason, message = _classify_command_failure(standard_before)
            issues.append(
                FabricIssue(
                    stage=standard_before.stage,
                    node=route.node,
                    hca=route.hca,
                    status="NOT_VERIFIED",
                    reason_code=reason,
                    message=message,
                )
            )
            return commands, self._counter_unknown(
                route, reason=reason, message=message, observed_interval=0.0
            ), issues

        extended_before_evidence = before_parts[1]
        optional_before = before_parts[2]
        if optional_before.returncode != 0 or optional_before.timed_out:
            reason, message = _classify_command_failure(optional_before)
            issues.append(
                FabricIssue(
                    stage=optional_before.stage,
                    node=route.node,
                    hca=route.hca,
                    status="NOT_VERIFIED",
                    reason_code=reason,
                    message=message,
                )
            )

        before_completed_at = self._monotonic()
        if (
            self._deadline_monotonic is not None
            and before_completed_at + config.sample_interval_seconds
            >= self._deadline_monotonic
        ):
            reason = "FABRIC_GLOBAL_DEADLINE_EXCEEDED"
            message = "the bounded fabric inspection cannot complete a valid counter window before its global deadline"
            issues.append(
                FabricIssue(
                    stage="leaf-counter-window",
                    node=route.node,
                    hca=route.hca,
                    status="NOT_VERIFIED",
                    reason_code=reason,
                    message=message,
                )
            )
            return commands, self._counter_unknown(
                route, reason=reason, message=message, observed_interval=0.0
            ), issues
        self._sleep(config.sample_interval_seconds)

        after_parts: list[FabricCommandEvidence] = []
        for per_vl, extended, stage in (
            (False, False, "leaf-counters-after"),
            (False, True, "leaf-counters-extended-after"),
            (True, False, "leaf-congestion-after"),
        ):
            evidence = self._execute(
                stage=stage,
                node=route.node,
                hca=route.hca,
                argv=self.build_counter_command(
                    context,
                    config,
                    route=route,
                    per_vl_congestion=per_vl,
                    extended=extended,
                ),
                config=config,
            )
            commands.append(evidence)
            after_parts.append(evidence)
        observed_interval = max(0.0, self._monotonic() - before_completed_at)

        standard_after = after_parts[0]
        if standard_after.returncode != 0 or standard_after.timed_out:
            reason, message = _classify_command_failure(standard_after)
            issues.append(
                FabricIssue(
                    stage=standard_after.stage,
                    node=route.node,
                    hca=route.hca,
                    status="NOT_VERIFIED",
                    reason_code=reason,
                    message=message,
                )
            )
            return commands, self._counter_unknown(
                route,
                reason=reason,
                message=message,
                observed_interval=observed_interval,
            ), issues

        extended_after_evidence = after_parts[1]
        optional_after = after_parts[2]
        if optional_after.returncode != 0 or optional_after.timed_out:
            reason, message = _classify_command_failure(optional_after)
            issues.append(
                FabricIssue(
                    stage=optional_after.stage,
                    node=route.node,
                    hca=route.hca,
                    status="NOT_VERIFIED",
                    reason_code=reason,
                    message=message,
                )
            )

        before = parse_perfquery_counters(standard_before.stdout)
        after = parse_perfquery_counters(standard_after.stdout)
        if optional_before.returncode == 0 and not optional_before.timed_out:
            before.update(parse_perfquery_counters(optional_before.stdout))
        if optional_after.returncode == 0 and not optional_after.timed_out:
            after.update(parse_perfquery_counters(optional_after.stdout))

        extended_before_status = _counter_query_status(extended_before_evidence)
        extended_after_status = _counter_query_status(extended_after_evidence)
        extended_before = (
            parse_extended_xmit_wait(extended_before_evidence.stdout)
            if extended_before_status == "COMPLETE"
            else {}
        )
        extended_after = (
            parse_extended_xmit_wait(extended_after_evidence.stdout)
            if extended_after_status == "COMPLETE"
            else {}
        )
        health = evaluate_fabric_counter_delta(
            before,
            after,
            route=route,
            sample_interval_seconds=observed_interval,
            extended_before=extended_before,
            extended_after=extended_after,
            extended_before_query_status=extended_before_status,
            extended_after_query_status=extended_after_status,
        )
        if (
            health.congestion_status == "UNKNOWN"
            and health.effective_xmit_wait_source == "NONE"
        ):
            issues.append(
                FabricIssue(
                    stage="leaf-counters-extended",
                    node=route.node,
                    hca=route.hca,
                    status="NOT_VERIFIED",
                    reason_code=health.congestion_reason_code,
                    message=(
                        "standard PortXmitWait needs a fallback, but the exact "
                        "PortCountersExtended field did not provide a valid "
                        "non-saturated monotonic 64-bit observation window"
                    ),
                )
            )
        return commands, health, issues
    @staticmethod
    def _base_result(
        context: SlurmActiveContext,
        config: IBFabricCheckConfig,
        started_at: str,
        *,
        status: str,
        reason_code: str,
        message: str,
    ) -> IBFabricCheckResult:
        return IBFabricCheckResult(
            status=status,
            reason_code=reason_code,
            message=message,
            job_id=context.job_id,
            nodes=list(context.selected_nodes),
            started_at=started_at,
            finished_at=_utc_now(),
            sample_interval_seconds=config.sample_interval_seconds,
            expected_link_width=config.expected_link_width,
            minimum_link_speed_gbps=config.minimum_link_speed_gbps,
            max_workers=config.max_workers,
            overall_timeout_seconds=config.overall_timeout_seconds,
            safety_boundary=(
                "OVERLAP_NOT_PROVEN_IDLE"
                if context.unsafe_allow_overlap
                else UNPROVEN_SLURM_SAFETY_BOUNDARY
            ),
        )

    def run(
        self, context: SlurmActiveContext, config: IBFabricCheckConfig
    ) -> IBFabricCheckResult:
        started_at = _utc_now()
        self._query_started = False
        self._last_query_started = 0.0
        self._deadline_monotonic = None
        if not context.enabled:
            return self._base_result(
                context,
                config,
                started_at,
                status="NOT_VERIFIED",
                reason_code="FABRIC_CHECK_DISABLED",
                message="one-hop fabric inspection is disabled by default",
            )
        try:
            context.validate()
            config.validate(len(context.selected_nodes))
            allocation = self._inspect_allocation(context)
            self._deadline_monotonic = (
                self._monotonic() + config.overall_timeout_seconds
            )
        except (ValueError, ActiveCheckSafetyError) as exc:
            return self._base_result(
                context,
                config,
                started_at,
                status="NOT_VERIFIED",
                reason_code=getattr(exc, "reason_code", "INVALID_FABRIC_CHECK_CONFIGURATION"),
                message=str(exc),
            )

        links: list[FabricLink] = []
        counters: list[FabricCounterHealth] = []
        issues: list[FabricIssue] = []
        commands: list[FabricCommandEvidence] = []

        endpoints = [
            (node, hca)
            for node in context.selected_nodes
            for hca in config.hcas
        ]
        link_workers = min(config.max_workers, len(endpoints))
        with ThreadPoolExecutor(
            max_workers=link_workers,
            thread_name_prefix="hcu-fabric-link",
        ) as executor:
            futures = {
                executor.submit(
                    self._inspect_one_link,
                    context,
                    config,
                    node=node,
                    hca=hca,
                ): (node, hca)
                for node, hca in endpoints
            }
            for future in as_completed(futures):
                node, hca = futures[future]
                try:
                    link_commands, link, issue = future.result()
                except Exception as exc:  # defensive isolation between endpoints
                    issues.append(
                        FabricIssue(
                            stage="one-hop-adjacency",
                            node=node,
                            hca=hca,
                            status="NOT_VERIFIED",
                            reason_code="FABRIC_WORKER_EXCEPTION",
                            message=str(exc),
                        )
                    )
                    continue
                commands.extend(link_commands)
                if link is not None:
                    links.append(link)
                if issue is not None:
                    issues.append(issue)
        links.sort(key=lambda item: (item.node, item.hca, item.ib_port))
        unique_routes: dict[tuple[str, int, int], FabricLink] = {}
        for link in links:
            unique_routes.setdefault(link.leaf_key, link)
        if len(unique_routes) > config.max_unique_leaf_ports:
            issues.append(
                FabricIssue(
                    stage="leaf-port-deduplication",
                    node="cluster",
                    hca="-",
                    status="NOT_VERIFIED",
                    reason_code="UNIQUE_LEAF_PORT_LIMIT_EXCEEDED",
                    message=(
                        f"discovered {len(unique_routes)} unique leaf ports, above the "
                        f"configured limit {config.max_unique_leaf_ports}"
                    ),
                )
            )
            unique_routes = {}

        routes = list(unique_routes.values())
        if routes:
            counter_workers = min(config.max_workers, len(routes))
            with ThreadPoolExecutor(
                max_workers=counter_workers,
                thread_name_prefix="hcu-fabric-counter",
            ) as executor:
                futures = {
                    executor.submit(
                        self._sample_one_route,
                        context,
                        config,
                        route=route,
                    ): route
                    for route in routes
                }
                for future in as_completed(futures):
                    route = futures[future]
                    try:
                        route_commands, counter, route_issues = future.result()
                    except Exception as exc:  # defensive isolation between leaf ports
                        reason = "FABRIC_WORKER_EXCEPTION"
                        message = str(exc)
                        issues.append(
                            FabricIssue(
                                stage="leaf-counter-window",
                                node=route.node,
                                hca=route.hca,
                                status="NOT_VERIFIED",
                                reason_code=reason,
                                message=message,
                            )
                        )
                        counters.append(
                            self._counter_unknown(
                                route,
                                reason=reason,
                                message=message,
                                observed_interval=0.0,
                            )
                        )
                        continue
                    commands.extend(route_commands)
                    counters.append(counter)
                    issues.extend(route_issues)

        counters.sort(
            key=lambda item: (
                item.node,
                item.hca,
                item.ib_port,
                item.switch_guid,
                item.switch_port,
            )
        )
        issues.sort(
            key=lambda item: (item.node, item.hca, item.stage, item.reason_code)
        )
        commands.sort(
            key=lambda item: (item.node, item.hca, item.stage, tuple(item.argv))
        )
        link_failed = any(link.status == "FAIL" for link in links)
        link_unverified = any(link.status == "NOT_VERIFIED" for link in links)
        counter_failed = any(item.status == "FAIL" for item in counters)
        counter_unknown = any(item.status == "UNKNOWN" for item in counters)
        counter_warned = any(item.status == "WARN" for item in counters)
        command_output_truncated = any(
            command.stdout_truncated or command.stderr_truncated
            for command in commands
        )
        if link_failed or counter_failed:
            status, reason, message = (
                "FAIL",
                "IB_ONE_HOP_FABRIC_FAILURE",
                "a one-hop link is down or a leaf-port error/drop counter increased",
            )
        elif command_output_truncated:
            status, reason, message = (
                "NOT_VERIFIED",
                "FABRIC_COMMAND_OUTPUT_TRUNCATED",
                "one or more fabric command outputs exceeded the evidence capture limit",
            )
        elif issues or link_unverified or counter_unknown or not links or not counters:
            status, reason, message = (
                "NOT_VERIFIED",
                "IB_ONE_HOP_FABRIC_EVIDENCE_INCOMPLETE",
                "one-hop adjacency or leaf-port counter evidence is incomplete",
            )
        elif counter_warned:
            status, reason, message = (
                "WARN",
                "IB_ONE_HOP_FABRIC_CONGESTION_GROWTH",
                "one-hop links are up, but a leaf-port congestion counter increased",
            )
        else:
            status, reason, message = (
                "PASS",
                "IB_ONE_HOP_FABRIC_HEALTHY",
                "all selected one-hop links are up and evaluated leaf-port counters are stable",
            )

        if context.unsafe_allow_overlap:
            safety_boundary = "OVERLAP_NOT_PROVEN_IDLE"
        elif allocation.node_exclusivity_proven:
            safety_boundary = FORMAL_SLURM_SAFETY_BOUNDARY
        else:
            safety_boundary = UNPROVEN_SLURM_SAFETY_BOUNDARY
        if context.unsafe_allow_overlap and status in {"PASS", "WARN"}:
            status, reason, message = (
                "NOT_VERIFIED",
                "OVERLAP_NOT_PROVEN_IDLE",
                "fabric evidence was collected with an overlapping Slurm step and cannot be accepted as an idle-allocation verdict",
            )
        elif not allocation.node_exclusivity_proven and status in {"PASS", "WARN"}:
            status, reason, message = (
                "NOT_VERIFIED",
                "SLURM_NODE_EXCLUSIVITY_NOT_PROVEN",
                "fabric evidence lacks scheduler proof of whole-node ownership and zero foreign active jobs",
            )

        return IBFabricCheckResult(
            status=status,
            reason_code=reason,
            message=message,
            job_id=context.job_id,
            nodes=list(context.selected_nodes),
            started_at=started_at,
            finished_at=_utc_now(),
            sample_interval_seconds=config.sample_interval_seconds,
            expected_link_width=config.expected_link_width,
            minimum_link_speed_gbps=config.minimum_link_speed_gbps,
            max_workers=config.max_workers,
            overall_timeout_seconds=config.overall_timeout_seconds,
            adjacency_links=links,
            counter_health=counters,
            issues=issues,
            commands=commands,
            allocation=asdict(allocation),
            safety_boundary=safety_boundary,
        )


def write_ib_fabric_reports(
    result: IBFabricCheckResult,
    output_dir: Path,
    *,
    output_dir_claimed: bool = False,
) -> tuple[Path, Path]:
    """Atomically publish JSON and Markdown without overwriting another run."""

    if not output_dir_claimed:
        claim_output_directory(output_dir)
    json_path = output_dir / "ib-fabric-result.json"
    markdown_path = output_dir / "ib-fabric-summary.md"
    atomic_write_text_exclusive(json_path, result.to_json())
    atomic_write_text_exclusive(markdown_path, result.to_markdown())
    return json_path, markdown_path
