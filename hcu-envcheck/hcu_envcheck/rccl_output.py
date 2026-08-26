# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Pure-text parser and validator for ``rccl-tests`` output.

This module deliberately has no dependency on launchers, Slurm, containers, or
the active RDMA runner.  Callers only provide the captured stdout/stderr and the
expectations that belong to the launch being diagnosed.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
import math
import re
from typing import Mapping


_DEVICE_RE = re.compile(
    r"^\s*#\s*Rank\s+(?P<rank>\d+)"
    r"(?:\s+Group\s+\d+)?\s+Pid\s+(?P<pid>\d+)\s+on\s+"
    r"(?P<host>\S+)\s+device\s+(?P<device>\d+)"
    r"\s+\[(?P<bdf>[^]]+)]",
    re.IGNORECASE,
)
_LOG_HOST_RE = re.compile(
    r"^(?P<host>[^:\s]+):(?P<pid>\d+):\d+\s+\[(?P<local_device>\d+)]\s+"
)
_NRANKS_RE = re.compile(r"\bnranks\b\s*(?:=|:)?\s*(\d+)", re.IGNORECASE)
_USING_NETWORK_RE = re.compile(r"\bUsing\s+network\s+(\S+)", re.IGNORECASE)
_VIA_NETWORK_RE = re.compile(r"\bvia\s+NET/([^\s/]+)", re.IGNORECASE)
_GDR_STATE_RE = re.compile(
    r"\b(?:GPU\s+Direct\s+RDMA|GDR)\s*[:=]?\s*(Enabled|Disabled)\b",
    re.IGNORECASE,
)
_GDRDMA_RE = re.compile(r"/GDRDMA\b", re.IGNORECASE)
_GDR_HCA_PROBE_RE = re.compile(
    r"\b(?:GPU\s+Direct\s+RDMA|GDR)\s*[:=]?\s*(Enabled|Disabled)\b"
    r"[^\r\n]*\bfor\s+(?:candidate\s+)?HCA\b",
    re.IGNORECASE,
)
_WRONG_RE = re.compile(r"^([+-]?\d+)(\*)?$")
_OOB_RE = re.compile(
    r"^\s*#\s*Out of bounds values\s*:\s*([+-]?\d+)(?:\s+\S+)?\s*$",
    re.IGNORECASE,
)
_AVG_BW_RE = re.compile(
    r"^\s*#\s*Avg bus bandwidth\s*:\s*(\S+)\s*$", re.IGNORECASE
)


@dataclass(frozen=True)
class RcclValidationIssue:
    """One deterministic validation failure."""

    code: str
    message: str
    line_number: int | None = None


@dataclass(frozen=True)
class RcclLayoutMetrics:
    """Metrics for one out-of-place or in-place table layout."""

    time_us: float
    algbw_gbps: float
    busbw_gbps: float
    wrong: int
    wrong_starred: bool


@dataclass(frozen=True)
class RcclTableRow:
    """A single 13-column rccl-tests result row."""

    size_bytes: int
    count: int
    dtype: str
    redop: str
    root: int
    out_of_place: RcclLayoutMetrics
    in_place: RcclLayoutMetrics
    line_number: int


@dataclass(frozen=True)
class RcclDeviceAssignment:
    """Rank-to-host/device mapping printed below ``# Using devices``."""

    rank: int
    pid: int
    hostname: str
    device: int
    bdf: str
    line_number: int


@dataclass(frozen=True)
class RcclHostTransportEvidence:
    """Condensed transport and GDR evidence for one host."""

    hostname: str
    using_networks: tuple[str, ...]
    socket_marker_count: int
    ibext_marker_count: int
    gdr_enabled_marker_count: int
    gdr_disabled_marker_count: int
    gdr_probe_enabled_marker_count: int
    gdr_probe_disabled_marker_count: int
    gdrrdma_marker_count: int
    transport: str
    gdr_state: str


@dataclass(frozen=True)
class RcclRankTransportEvidence:
    """Selected transport/GDR evidence mapped to one global rank."""

    rank: int
    hostname: str
    device: int
    using_networks: tuple[str, ...]
    socket_marker_count: int
    ibext_marker_count: int
    gdr_enabled_marker_count: int
    gdr_disabled_marker_count: int
    gdr_probe_enabled_marker_count: int
    gdr_probe_disabled_marker_count: int
    gdrrdma_marker_count: int
    transport: str
    gdr_state: str


@dataclass(frozen=True)
class RcclSummary:
    """Completion summaries emitted by rccl-tests, when present."""

    out_of_bounds_values: tuple[int, ...]
    average_bus_bandwidths: tuple[float, ...]


@dataclass(frozen=True)
class RcclOutputResult:
    """Structured parse result plus strict acceptance outcome."""

    rows: tuple[RcclTableRow, ...]
    devices: tuple[RcclDeviceAssignment, ...]
    nranks_values: tuple[int, ...]
    host_transports: tuple[RcclHostTransportEvidence, ...]
    rank_transports: tuple[RcclRankTransportEvidence, ...]
    summary: RcclSummary
    expected_sizes: tuple[int, ...]
    observed_sizes: tuple[int, ...]
    issues: tuple[RcclValidationIssue, ...]
    valid: bool

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable-by-convention nested dictionary."""

        return asdict(self)


@dataclass
class _MutableHostEvidence:
    networks: set[str]
    socket_markers: int = 0
    ibext_markers: int = 0
    gdr_enabled_markers: int = 0
    gdr_disabled_markers: int = 0
    gdr_probe_enabled_markers: int = 0
    gdr_probe_disabled_markers: int = 0
    gdrrdma_markers: int = 0


def _issue(
    issues: list[RcclValidationIssue],
    code: str,
    message: str,
    line_number: int | None = None,
) -> None:
    issues.append(RcclValidationIssue(code, message, line_number))


def _parse_float(value: str) -> float:
    # float() intentionally accepts NaN/Inf; validation reports those distinctly.
    return float(value)


def _parse_wrong(value: str) -> tuple[int, bool]:
    match = _WRONG_RE.fullmatch(value)
    if match is None:
        raise ValueError(f"invalid #wrong value: {value}")
    return int(match.group(1)), bool(match.group(2))


def _parse_table_row(tokens: list[str], line_number: int) -> RcclTableRow:
    if len(tokens) != 13:
        raise ValueError(f"expected 13 columns, got {len(tokens)}")
    out_wrong, out_starred = _parse_wrong(tokens[8])
    in_wrong, in_starred = _parse_wrong(tokens[12])
    return RcclTableRow(
        size_bytes=int(tokens[0]),
        count=int(tokens[1]),
        dtype=tokens[2],
        redop=tokens[3],
        root=int(tokens[4]),
        out_of_place=RcclLayoutMetrics(
            time_us=_parse_float(tokens[5]),
            algbw_gbps=_parse_float(tokens[6]),
            busbw_gbps=_parse_float(tokens[7]),
            wrong=out_wrong,
            wrong_starred=out_starred,
        ),
        in_place=RcclLayoutMetrics(
            time_us=_parse_float(tokens[9]),
            algbw_gbps=_parse_float(tokens[10]),
            busbw_gbps=_parse_float(tokens[11]),
            wrong=in_wrong,
            wrong_starred=in_starred,
        ),
        line_number=line_number,
    )


def _looks_like_headerless_row(tokens: list[str]) -> bool:
    if len(tokens) != 13:
        return False
    try:
        int(tokens[0])
        int(tokens[1])
        int(tokens[4])
    except (ValueError, IndexError):
        return False
    return True


def _looks_like_table_candidate(tokens: list[str]) -> bool:
    """Distinguish result-shaped rows from NCCL logs interleaved in the table."""

    if len(tokens) < 5:
        return False
    try:
        int(tokens[0])
        int(tokens[1])
        int(tokens[4])
    except (ValueError, IndexError):
        return False
    return True


def _expected_size_sequence(
    minimum: int,
    maximum: int,
    factor: int,
) -> tuple[tuple[int, ...], str | None]:
    if minimum <= 0 or maximum <= 0:
        return (), "min/max bytes must both be positive"
    if maximum < minimum:
        return (), "max bytes must be greater than or equal to min bytes"
    if factor <= 1:
        return (), "step factor must be greater than one"

    sizes: list[int] = []
    current = minimum
    while current <= maximum:
        sizes.append(current)
        if current == maximum:
            return tuple(sizes), None
        current *= factor
    return tuple(sizes), (
        f"factor sequence from {minimum} does not land exactly on {maximum}"
    )


def _transport_kind(socket_markers: int, ibext_markers: int) -> str:
    if socket_markers and ibext_markers:
        return "MIXED"
    if socket_markers:
        return "SOCKET"
    if ibext_markers:
        return "IBEXT"
    return "UNKNOWN"


def _gdr_kind(
    enabled: int,
    disabled: int,
    probe_enabled: int,
    probe_disabled: int,
    gdrrdma: int,
) -> str:
    """Resolve final GDR selection before initialization probe evidence.

    ``.../GDRDMA`` belongs to a selected channel and therefore outranks HCA
    candidate-probe messages. A conflict is reserved for contradictory final
    or declarative evidence, not mixed candidate probes.
    """

    final_enabled = enabled - probe_enabled
    final_disabled = disabled - probe_disabled
    if gdrrdma and final_disabled:
        return "CONFLICT"
    if gdrrdma:
        return "ENABLED"
    if final_enabled and final_disabled:
        return "CONFLICT"
    if final_enabled:
        return "ENABLED"
    if final_disabled:
        return "DISABLED"
    if probe_disabled and not probe_enabled:
        return "DISABLED"
    return "UNKNOWN"


def _record_transport_evidence(evidence: _MutableHostEvidence, line: str) -> None:
    for network in _USING_NETWORK_RE.findall(line) + _VIA_NETWORK_RE.findall(line):
        evidence.networks.add(network)
        token = network.lower()
        if token.startswith("socket"):
            evidence.socket_markers += 1
        if token == "ib" or "ibext" in token:
            evidence.ibext_markers += 1
    for state in _GDR_STATE_RE.findall(line):
        if state.lower() == "enabled":
            evidence.gdr_enabled_markers += 1
        else:
            evidence.gdr_disabled_markers += 1
    for state in _GDR_HCA_PROBE_RE.findall(line):
        if state.lower() == "enabled":
            evidence.gdr_probe_enabled_markers += 1
        else:
            evidence.gdr_probe_disabled_markers += 1
    evidence.gdrrdma_markers += len(_GDRDMA_RE.findall(line))


def _evidence_states(evidence: _MutableHostEvidence) -> tuple[str, str]:
    return (
        _transport_kind(evidence.socket_markers, evidence.ibext_markers),
        _gdr_kind(
            evidence.gdr_enabled_markers,
            evidence.gdr_disabled_markers,
            evidence.gdr_probe_enabled_markers,
            evidence.gdr_probe_disabled_markers,
            evidence.gdrrdma_markers,
        ),
    )


def parse_rccl_tests_output(
    text: str,
    *,
    expected_nranks: int | None = None,
    expected_devices_per_node: int | Mapping[str, int] | None = None,
    min_bytes: int | None = None,
    max_bytes: int | None = None,
    step_factor: int | None = None,
) -> RcclOutputResult:
    """Parse and strictly validate captured ``rccl-tests`` output.

    Explicit size-policy arguments override values in the rccl-tests command
    summary.  ``expected_devices_per_node`` can be one integer for every
    observed host or an exact ``hostname -> count`` mapping.
    """

    if expected_nranks is not None and expected_nranks <= 0:
        raise ValueError("expected_nranks must be positive")
    if isinstance(expected_devices_per_node, int) and expected_devices_per_node <= 0:
        raise ValueError("expected_devices_per_node must be positive")

    issues: list[RcclValidationIssue] = []
    rows: list[RcclTableRow] = []
    devices: list[RcclDeviceAssignment] = []
    nranks_occurrences: list[int] = []
    out_of_bounds: list[int] = []
    average_bandwidths: list[float] = []
    hosts: dict[str, _MutableHostEvidence] = {}
    endpoints: dict[tuple[str, int, int], _MutableHostEvidence] = {}

    header_min: int | None = None
    header_max: int | None = None
    header_factor: int | None = None
    in_table = False

    def host_evidence(hostname: str) -> _MutableHostEvidence:
        if hostname not in hosts:
            hosts[hostname] = _MutableHostEvidence(set())
        return hosts[hostname]

    for line_number, line in enumerate(text.splitlines(), 1):
        if header_min is None:
            match = re.search(r"\bminBytes\s+(\d+)", line, re.IGNORECASE)
            if match:
                header_min = int(match.group(1))
        if header_max is None:
            match = re.search(r"\bmaxBytes\s+(\d+)", line, re.IGNORECASE)
            if match:
                header_max = int(match.group(1))
        if header_factor is None:
            match = re.search(r"\bstep\s*:\s*(\d+)\s*\(factor\)", line, re.IGNORECASE)
            if match:
                header_factor = int(match.group(1))

        device_match = _DEVICE_RE.match(line)
        if device_match:
            assignment = RcclDeviceAssignment(
                rank=int(device_match.group("rank")),
                pid=int(device_match.group("pid")),
                hostname=device_match.group("host"),
                device=int(device_match.group("device")),
                bdf=device_match.group("bdf"),
                line_number=line_number,
            )
            devices.append(assignment)
            host_evidence(assignment.hostname)

        nranks_occurrences.extend(int(value) for value in _NRANKS_RE.findall(line))

        host_match = _LOG_HOST_RE.match(line)
        if host_match:
            evidence = host_evidence(host_match.group("host"))
            _record_transport_evidence(evidence, line)
            endpoint_key = (
                host_match.group("host"),
                int(host_match.group("pid")),
                int(host_match.group("local_device")),
            )
            endpoint = endpoints.setdefault(endpoint_key, _MutableHostEvidence(set()))
            _record_transport_evidence(endpoint, line)

        oob_match = _OOB_RE.match(line)
        if oob_match:
            value = int(oob_match.group(1))
            out_of_bounds.append(value)
            if value != 0:
                _issue(
                    issues,
                    "SUMMARY_OUT_OF_BOUNDS_NONZERO",
                    f"summary reports {value} out-of-bounds values",
                    line_number,
                )

        avg_match = _AVG_BW_RE.match(line)
        if avg_match:
            try:
                value = float(avg_match.group(1))
            except ValueError:
                _issue(
                    issues,
                    "SUMMARY_AVG_BANDWIDTH_INVALID",
                    f"invalid average bus bandwidth: {avg_match.group(1)}",
                    line_number,
                )
            else:
                average_bandwidths.append(value)
                if not math.isfinite(value) or value <= 0:
                    _issue(
                        issues,
                        "SUMMARY_AVG_BANDWIDTH_INVALID",
                        f"average bus bandwidth must be finite and positive: {value}",
                        line_number,
                    )

        lower = line.lower()
        if "#wrong" in lower and "algbw" in lower and "busbw" in lower:
            in_table = True
            continue
        if in_table and (
            "errors with asterisks" in lower
            or "out of bounds values" in lower
            or "avg bus bandwidth" in lower
        ):
            in_table = False

        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        tokens = stripped.split()
        if not (
            _looks_like_headerless_row(tokens)
            or (in_table and _looks_like_table_candidate(tokens))
        ):
            continue
        try:
            row = _parse_table_row(tokens, line_number)
        except (ValueError, OverflowError) as exc:
            _issue(
                issues,
                "TABLE_ROW_MALFORMED",
                f"malformed rccl-tests table row: {exc}",
                line_number,
            )
            continue
        rows.append(row)

    if not rows:
        _issue(issues, "TABLE_ROWS_MISSING", "no 13-column rccl-tests result rows found")
    if not out_of_bounds:
        _issue(
            issues,
            "SUMMARY_OUT_OF_BOUNDS_MISSING",
            "no rccl-tests out-of-bounds correctness summary was found",
        )

    for row in rows:
        for layout_name, metrics in (
            ("out_of_place", row.out_of_place),
            ("in_place", row.in_place),
        ):
            for field_name, value in (
                ("time_us", metrics.time_us),
                ("algbw_gbps", metrics.algbw_gbps),
                ("busbw_gbps", metrics.busbw_gbps),
            ):
                if not math.isfinite(value):
                    _issue(
                        issues,
                        "LAYOUT_METRIC_NOT_FINITE",
                        f"size {row.size_bytes} {layout_name} {field_name} is not finite",
                        row.line_number,
                    )
                elif value <= 0:
                    _issue(
                        issues,
                        "LAYOUT_METRIC_NOT_POSITIVE",
                        f"size {row.size_bytes} {layout_name} {field_name} must be positive",
                        row.line_number,
                    )
            if metrics.wrong != 0:
                _issue(
                    issues,
                    "WRONG_VALUE_NONZERO",
                    f"size {row.size_bytes} {layout_name} #wrong is {metrics.wrong}",
                    row.line_number,
                )
            if metrics.wrong_starred:
                _issue(
                    issues,
                    "WRONG_VALUE_STARRED",
                    f"size {row.size_bytes} {layout_name} #wrong is threshold-starred",
                    row.line_number,
                )

    effective_min = min_bytes if min_bytes is not None else header_min
    effective_max = max_bytes if max_bytes is not None else header_max
    effective_factor = step_factor if step_factor is not None else header_factor
    expected_sizes: tuple[int, ...] = ()
    if effective_min is None or effective_max is None or effective_factor is None:
        _issue(
            issues,
            "SIZE_POLICY_MISSING",
            "min bytes, max bytes, and factor are required for complete size validation",
        )
    else:
        expected_sizes, policy_error = _expected_size_sequence(
            effective_min, effective_max, effective_factor
        )
        if policy_error:
            _issue(issues, "SIZE_POLICY_INVALID", policy_error)

    size_counts = Counter(row.size_bytes for row in rows)
    for size, count in sorted(size_counts.items()):
        if count > 1:
            _issue(
                issues,
                "DUPLICATE_SIZE_ROW",
                f"size {size} appears {count} times",
            )
    if expected_sizes:
        expected_set = set(expected_sizes)
        observed_set = set(size_counts)
        for size in sorted(expected_set - observed_set):
            _issue(issues, "EXPECTED_SIZE_MISSING", f"expected size {size} is missing")
        for size in sorted(observed_set - expected_set):
            _issue(issues, "UNEXPECTED_SIZE", f"unexpected size {size} is present")

    rank_counts = Counter(device.rank for device in devices)
    for rank, count in sorted(rank_counts.items()):
        if count > 1:
            _issue(
                issues,
                "DEVICE_RANK_DUPLICATE",
                f"rank {rank} has {count} device assignment lines",
            )
    if expected_nranks is not None:
        expected_ranks = set(range(expected_nranks))
        observed_ranks = set(rank_counts)
        for rank in sorted(expected_ranks - observed_ranks):
            _issue(issues, "DEVICE_RANK_MISSING", f"rank {rank} has no device assignment")
        for rank in sorted(observed_ranks - expected_ranks):
            _issue(issues, "DEVICE_RANK_EXTRA", f"unexpected rank {rank} is assigned")

    per_host_devices: dict[str, list[RcclDeviceAssignment]] = defaultdict(list)
    for device in devices:
        per_host_devices[device.hostname].append(device)
    for hostname, assignments in sorted(per_host_devices.items()):
        device_counts = Counter(item.device for item in assignments)
        bdf_counts = Counter(item.bdf.lower() for item in assignments)
        for device_index, count in sorted(device_counts.items()):
            if count > 1:
                _issue(
                    issues,
                    "DEVICE_INDEX_DUPLICATE",
                    f"host {hostname} device {device_index} is assigned {count} times",
                )
        for bdf, count in sorted(bdf_counts.items()):
            if count > 1:
                _issue(
                    issues,
                    "DEVICE_BDF_DUPLICATE",
                    f"host {hostname} BDF {bdf} is assigned {count} times",
                )

    if isinstance(expected_devices_per_node, Mapping):
        expected_by_host = dict(expected_devices_per_node)
        invalid_counts = {
            host: count for host, count in expected_by_host.items() if count < 0
        }
        if invalid_counts:
            raise ValueError("expected device counts in mapping must be non-negative")
        all_hosts = set(expected_by_host) | set(per_host_devices)
        for hostname in sorted(all_hosts):
            observed = len(per_host_devices.get(hostname, ()))
            expected = expected_by_host.get(hostname, 0)
            if observed != expected:
                _issue(
                    issues,
                    "DEVICE_COUNT_PER_NODE_MISMATCH",
                    f"host {hostname} has {observed} assignments; expected {expected}",
                )
    elif isinstance(expected_devices_per_node, int):
        for hostname, assignments in sorted(per_host_devices.items()):
            if len(assignments) != expected_devices_per_node:
                _issue(
                    issues,
                    "DEVICE_COUNT_PER_NODE_MISMATCH",
                    f"host {hostname} has {len(assignments)} assignments; "
                    f"expected {expected_devices_per_node}",
                )

    nranks_values = tuple(sorted(set(nranks_occurrences)))
    if expected_nranks is not None:
        if not nranks_values:
            _issue(issues, "NRANKS_MISSING", "no nranks evidence was found")
        elif nranks_values != (expected_nranks,):
            _issue(
                issues,
                "NRANKS_MISMATCH",
                f"observed nranks values {list(nranks_values)}; expected only {expected_nranks}",
            )

    transport_results: list[RcclHostTransportEvidence] = []
    for hostname in sorted(hosts):
        evidence = hosts[hostname]
        transport, gdr_state = _evidence_states(evidence)
        transport_results.append(
            RcclHostTransportEvidence(
                hostname=hostname,
                using_networks=tuple(sorted(evidence.networks)),
                socket_marker_count=evidence.socket_markers,
                ibext_marker_count=evidence.ibext_markers,
                gdr_enabled_marker_count=evidence.gdr_enabled_markers,
                gdr_disabled_marker_count=evidence.gdr_disabled_markers,
                gdr_probe_enabled_marker_count=evidence.gdr_probe_enabled_markers,
                gdr_probe_disabled_marker_count=evidence.gdr_probe_disabled_markers,
                gdrrdma_marker_count=evidence.gdrrdma_markers,
                transport=transport,
                gdr_state=gdr_state,
            )
        )
        if transport == "MIXED":
            _issue(
                issues,
                "TRANSPORT_EVIDENCE_CONFLICT",
                f"host {hostname} selected both Socket and IBext transports",
            )
        if gdr_state == "CONFLICT":
            _issue(
                issues,
                "GDR_EVIDENCE_CONFLICT",
                f"host {hostname} has both enabled and disabled GDR evidence",
            )

    rank_transport_results: list[RcclRankTransportEvidence] = []
    for assignment in sorted(devices, key=lambda item: (item.rank, item.line_number)):
        evidence = endpoints.get(
            (assignment.hostname, assignment.pid, assignment.device)
        )
        if evidence is None:
            pid_matches = [
                item
                for (host, pid, _), item in endpoints.items()
                if host == assignment.hostname and pid == assignment.pid
            ]
            evidence = (
                pid_matches[0]
                if len(pid_matches) == 1
                else _MutableHostEvidence(set())
            )
        transport, gdr_state = _evidence_states(evidence)
        rank_transport_results.append(
            RcclRankTransportEvidence(
                rank=assignment.rank,
                hostname=assignment.hostname,
                device=assignment.device,
                using_networks=tuple(sorted(evidence.networks)),
                socket_marker_count=evidence.socket_markers,
                ibext_marker_count=evidence.ibext_markers,
                gdr_enabled_marker_count=evidence.gdr_enabled_markers,
                gdr_disabled_marker_count=evidence.gdr_disabled_markers,
                gdr_probe_enabled_marker_count=evidence.gdr_probe_enabled_markers,
                gdr_probe_disabled_marker_count=evidence.gdr_probe_disabled_markers,
                gdrrdma_marker_count=evidence.gdrrdma_markers,
                transport=transport,
                gdr_state=gdr_state,
            )
        )
        if expected_nranks is not None and transport == "UNKNOWN":
            _issue(
                issues,
                "RANK_TRANSPORT_EVIDENCE_MISSING",
                f"rank {assignment.rank} has no selected network transport evidence",
            )
        elif transport == "MIXED":
            _issue(
                issues,
                "RANK_TRANSPORT_EVIDENCE_CONFLICT",
                f"rank {assignment.rank} selected both Socket and IBext transports",
            )
        if gdr_state == "CONFLICT":
            _issue(
                issues,
                "RANK_GDR_EVIDENCE_CONFLICT",
                f"rank {assignment.rank} has contradictory final GDR evidence",
            )

    return RcclOutputResult(
        rank_transports=tuple(rank_transport_results),
        rows=tuple(rows),
        devices=tuple(devices),
        nranks_values=nranks_values,
        host_transports=tuple(transport_results),
        summary=RcclSummary(tuple(out_of_bounds), tuple(average_bandwidths)),
        expected_sizes=expected_sizes,
        observed_sizes=tuple(sorted(size_counts)),
        issues=tuple(issues),
        valid=not issues,
    )


# Short alias for callers that do not need to distinguish the test binary name.
parse_rccl_output = parse_rccl_tests_output


__all__ = [
    "RcclDeviceAssignment",
    "RcclHostTransportEvidence",
    "RcclRankTransportEvidence",
    "RcclLayoutMetrics",
    "RcclOutputResult",
    "RcclSummary",
    "RcclTableRow",
    "RcclValidationIssue",
    "parse_rccl_output",
    "parse_rccl_tests_output",
]
