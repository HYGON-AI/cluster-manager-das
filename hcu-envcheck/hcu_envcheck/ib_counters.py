# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class CounterRule:
    """Policy for one monotonically increasing IB port counter.

    ``max_delta`` is deliberately an observation-window threshold, not a
    lifetime-total threshold.  Linux exposes these values as cumulative
    counters; a non-zero but stable baseline is therefore historical evidence
    and must not fail a startup preflight.
    """

    category: str
    required: bool
    max_delta: int
    violation_status: str


_FAIL_ON_GROWTH = (
    "excessive_buffer_overrun_errors",
    "link_downed",
    "link_error_recovery",
    "local_link_integrity_errors",
    "port_rcv_constraint_errors",
    "port_rcv_errors",
    "port_rcv_remote_physical_errors",
    "port_rcv_switch_relay_errors",
    "port_xmit_constraint_errors",
    "port_xmit_discards",
    "symbol_error",
    "VL15_dropped",
)


DEFAULT_IB_COUNTER_RULES: dict[str, CounterRule] = {
    name: CounterRule(
        category="ERROR_OR_LINK_EVENT",
        required=True,
        max_delta=0,
        violation_status="FAIL",
    )
    for name in _FAIL_ON_GROWTH
}
DEFAULT_IB_COUNTER_RULES["port_xmit_wait"] = CounterRule(
    category="CONGESTION_SIGNAL",
    required=False,
    max_delta=0,
    violation_status="WARN",
)


_STATUS_ORDER = {"PASS": 0, "WARN": 1, "UNKNOWN": 2, "FAIL": 3}

_COMMON_PMA_SATURATION_VALUES = {
    "symbol_error": 0xFFFF,
    "link_error_recovery": 0xFF,
    "link_downed": 0xFF,
    "port_rcv_errors": 0xFFFF,
    "port_rcv_remote_physical_errors": 0xFFFF,
    "port_rcv_switch_relay_errors": 0xFFFF,
    "port_xmit_discards": 0xFFFF,
    "port_xmit_constraint_errors": 0xFF,
    "port_rcv_constraint_errors": 0xFF,
    "local_link_integrity_errors": 0xF,
    "excessive_buffer_overrun_errors": 0xF,
    "VL15_dropped": 0xFFFF,
    "port_xmit_wait": 0xFFFFFFFF,
}

def _parse_counter(value: Any) -> int | None:
    """Parse a sysfs counter without accepting booleans or negative values."""

    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = int(str(value).strip(), 0)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _overall_status(metric_statuses: list[str]) -> str:
    return max(metric_statuses or ["UNKNOWN"], key=_STATUS_ORDER.__getitem__)


def evaluate_ib_counter_samples(
    before: Mapping[str, Any] | None,
    after: Mapping[str, Any] | None,
    *,
    interval_seconds: float | int | None,
    rules: Mapping[str, CounterRule] | None = None,
    min_interval_seconds: float = 1.0,
    max_interval_seconds: float = 60.0,
) -> dict[str, Any]:
    """Evaluate two snapshots from the same IB device/port.

    Status semantics are intentionally conservative:

    * FAIL: a comparable error/link-event counter grew beyond policy.
    * UNKNOWN: required evidence is missing/invalid, the counter decreased
      (reset or wrap), or the observation interval is invalid.
    * WARN: only a non-fatal signal such as ``port_xmit_wait`` grew.  That
      counter alone cannot prove a broken link because it also grows under
      ordinary credit/arbitration pressure.
    * PASS: every required counter is comparable and within threshold.  A
      non-zero but stable lifetime total does not change PASS.

    FAIL takes precedence over UNKNOWN so a known fault is never hidden by a
    second missing metric.  UNKNOWN takes precedence over WARN so incomplete
    required evidence is never presented as a mere advisory.
    """

    selected_rules = dict(rules or DEFAULT_IB_COUNTER_RULES)
    try:
        interval = float(interval_seconds) if interval_seconds is not None else None
    except (TypeError, ValueError):
        interval = None
    interval_valid = bool(
        interval is not None
        and min_interval_seconds <= interval <= max_interval_seconds
    )

    before_values = before if isinstance(before, Mapping) else {}
    after_values = after if isinstance(after, Mapping) else {}
    metrics: dict[str, dict[str, Any]] = {}
    reason_codes: set[str] = set()
    required_comparable = 0
    required_total = sum(rule.required for rule in selected_rules.values())
    historical_nonzero: list[str] = []

    for name, rule in selected_rules.items():
        raw_before = before_values.get(name)
        raw_after = after_values.get(name)
        value_before = _parse_counter(raw_before)
        value_after = _parse_counter(raw_after)
        metric: dict[str, Any] = {
            "category": rule.category,
            "required": rule.required,
            "max_delta": rule.max_delta,
            "before": value_before,
            "after": value_after,
            "delta": None,
            "rate_per_second": None,
            "status": "UNKNOWN",
            "reason_code": "IB_COUNTER_EVIDENCE_MISSING",
            "counter_source": "sysfs",
            "counter_width_bits": None,
            "possible_saturation_value": _COMMON_PMA_SATURATION_VALUES.get(name),
        }

        if not interval_valid:
            metric["reason_code"] = "IB_COUNTER_INTERVAL_INVALID"
        elif value_before is None or value_after is None:
            metric["reason_code"] = "IB_COUNTER_EVIDENCE_MISSING"
        elif value_after < value_before:
            metric["reason_code"] = "IB_COUNTER_RESET_OR_WRAP"
        elif (
            value_before == value_after
            and value_before == _COMMON_PMA_SATURATION_VALUES.get(name)
        ):
            metric["reason_code"] = "POSSIBLE_COUNTER_SATURATION"
        else:
            delta = value_after - value_before
            metric["delta"] = delta
            metric["rate_per_second"] = delta / interval
            if rule.required:
                required_comparable += 1
            if value_before > 0:
                historical_nonzero.append(name)
            if delta > rule.max_delta:
                metric["status"] = rule.violation_status
                metric["reason_code"] = (
                    "IB_CONGESTION_SIGNAL_GROWTH"
                    if rule.violation_status == "WARN"
                    else "IB_COUNTER_ERROR_GROWTH"
                )
            else:
                metric["status"] = "PASS"
                metric["reason_code"] = "IB_COUNTER_STABLE"

        # Missing optional advisory evidence is visible per metric but does not
        # prevent the required counter set from passing.
        if metric["status"] != "PASS" and (
            rule.required or metric["status"] in {"WARN", "FAIL"}
        ):
            reason_codes.add(str(metric["reason_code"]))
        metrics[name] = metric

    decisive_statuses = [
        str(metric["status"])
        for metric in metrics.values()
        if bool(metric["required"]) or metric["status"] in {"WARN", "FAIL"}
    ]
    status = _overall_status(decisive_statuses)
    if status == "PASS":
        reason_codes = {"IB_COUNTERS_STABLE"}

    return {
        "schema_version": "1.0",
        "status": status,
        "counter_source": "sysfs",
        "interval_seconds": interval,
        "interval_valid": interval_valid,
        "policy": {
            "name": "strict_startup_delta_v1",
            "min_interval_seconds": min_interval_seconds,
            "max_interval_seconds": max_interval_seconds,
            "rules": {name: asdict(rule) for name, rule in selected_rules.items()},
        },
        "coverage": {
            "required_total": required_total,
            "required_comparable": required_comparable,
            "required_complete": required_comparable == required_total,
        },
        "historical_nonzero_counters": sorted(set(historical_nonzero)),
        "reason_codes": sorted(reason_codes),
        "metrics": metrics,
    }
