# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import ipaddress
import json
import re
from typing import Any

from .ib_counters import DEFAULT_IB_COUNTER_RULES, evaluate_ib_counter_samples
from .models import Finding
from .roce_health import _parse_pairs, _parse_pfc, evaluate_roce_health


PROTOCOL_AUTO = "auto"
PROTOCOL_IB = "ib"
PROTOCOL_ROCE = "roce"
EXPECTED_PROTOCOLS = {PROTOCOL_AUTO, PROTOCOL_IB, PROTOCOL_ROCE}


def _normalized_state(value: Any) -> str:
    return str(value or "").split(":", 1)[-1].strip().upper().replace("_", "")


def _normalized_link_layer(value: Any) -> str:
    normalized = str(value or "").strip().upper().replace("_", "")
    if normalized == "INFINIBAND":
        return "INFINIBAND"
    if normalized == "ETHERNET":
        return "ETHERNET"
    return "UNKNOWN"


def _is_nonzero_gid(value: Any) -> bool:
    try:
        parsed = ipaddress.IPv6Address(str(value or "").strip())
    except ipaddress.AddressValueError:
        return False
    return int(parsed) != 0


def _numeric_nonzero(value: Any, *, pkey: bool = False) -> bool:
    if value is None:
        return False
    text = str(value).strip().split()[0]
    try:
        number = int(text, 0)
    except ValueError:
        return False
    return bool(number & 0x7FFF) if pkey else number != 0


def _mtu_value(value: Any) -> int | None:
    if value is None:
        return None
    match = re.search(r"\d+", str(value))
    if not match:
        return None
    parsed = int(match.group(0))
    return parsed if parsed > 0 else None


def _normalized_rate_mbps(value: Any) -> int | float | None:
    match = re.search(
        r"(\d+(?:\.\d+)?)\s*([kmgt])?\s*b(?:it)?(?:/s|/sec|ps)",
        str(value or ""),
        flags=re.I,
    )
    if not match:
        return None
    multipliers = {None: 0.001, "k": 0.001, "m": 1, "g": 1000, "t": 1000000}
    numeric = float(match.group(1)) * multipliers[match.group(2).lower() if match.group(2) else None]
    return int(numeric) if numeric.is_integer() else numeric


def _normalized_subnet_prefix(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    try:
        numeric = int(text, 0)
    except ValueError:
        try:
            numeric = int(ipaddress.IPv6Address(text)) >> 64
        except ipaddress.AddressValueError:
            return None
    if numeric <= 0 or numeric > 0xFFFFFFFFFFFFFFFF:
        return None
    return f"0x{numeric:016x}"


def _evidence_status(port: dict[str, Any], field: str, default: str) -> str:
    return str(port.get(field) or default).strip().upper()


def _valid_gid_entries(port: dict[str, Any]) -> list[dict[str, Any]]:
    entries = port.get("gids")
    if not isinstance(entries, list):
        return []
    return [
        entry
        for entry in entries
        if isinstance(entry, dict) and _is_nonzero_gid(entry.get("gid"))
    ]


def _valid_pkeys(port: dict[str, Any]) -> list[str]:
    entries = port.get("pkeys")
    if not isinstance(entries, list):
        return []
    values = [entry.get("value") if isinstance(entry, dict) else entry for entry in entries]
    normalized: list[str] = []
    for value in values:
        if not _numeric_nonzero(value, pkey=True):
            continue
        try:
            numeric = int(str(value).strip().split()[0], 0) & 0xFFFF
        except ValueError:
            continue
        normalized.append(f"0x{numeric:04x}")
    return normalized


def _collection_complete(port: dict[str, Any], field: str) -> bool:
    status = port.get(f"{field[:-1]}_collection_status")
    if status is None:
        return port.get(field) is not None
    return status == "COMPLETE"


def classify_rdma_port(port: dict[str, Any]) -> dict[str, Any]:
    """Classify the current port mode without guessing hardware capability.

    ``IB/RoCE v1`` is an intentionally ambiguous GID type in Linux.  It is an
    IB GID when the port link layer is InfiniBand and a RoCE-v1 GID only when
    the link layer is Ethernet.
    """

    link_layer = _normalized_link_layer(port.get("link_layer"))
    raw_gids = port.get("gids")
    gid_status = _evidence_status(
        port,
        "gid_collection_status",
        "COMPLETE" if raw_gids is not None else "UNAVAILABLE",
    )
    inferred_attribute_status = (
        "COMPLETE"
        if isinstance(raw_gids, list)
        and all(
            isinstance(entry, dict) and "type" in entry and "netdev" in entry
            for entry in raw_gids
        )
        else "UNAVAILABLE"
    )
    gid_type_status = _evidence_status(port, "gid_type_collection_status", inferred_attribute_status)
    gid_ndev_status = _evidence_status(port, "gid_ndev_collection_status", inferred_attribute_status)
    gid_protocol_evidence_complete = all(
        status == "COMPLETE" for status in (gid_status, gid_type_status, gid_ndev_status)
    )
    valid_gids = _valid_gid_entries(port)
    mapped_gids = [entry for entry in valid_gids if str(entry.get("netdev") or "").strip()]
    roce_versions: set[str] = set()
    if link_layer == "ETHERNET":
        for entry in mapped_gids:
            gid_type = str(entry.get("type") or "").lower()
            if "roce v2" in gid_type:
                roce_versions.add("v2")
            elif "roce v1" in gid_type:
                roce_versions.add("v1")

    if link_layer == "INFINIBAND":
        current_protocol = "NATIVE_INFINIBAND"
    elif link_layer == "ETHERNET" and not gid_protocol_evidence_complete:
        current_protocol = "ETHERNET_RDMA_EVIDENCE_INCOMPLETE"
    elif link_layer == "ETHERNET" and roce_versions:
        current_protocol = "ROCE"
    elif link_layer == "ETHERNET":
        current_protocol = "ETHERNET_RDMA_UNCONFIRMED"
    else:
        current_protocol = "UNKNOWN"

    return {
        "current_protocol": current_protocol,
        "link_layer": link_layer,
        "roce_versions": sorted(roce_versions),
        "valid_gid_count": len(valid_gids),
        "mapped_gid_count": len(mapped_gids),
        "gid_protocol_evidence_complete": gid_protocol_evidence_complete,
        "gid_collection_status": gid_status,
        "gid_type_collection_status": gid_type_status,
        "gid_ndev_collection_status": gid_ndev_status,
        "active_linkup": (
            _normalized_state(port.get("state")) == "ACTIVE"
            and _normalized_state(port.get("phys_state")) == "LINKUP"
        ),
    }


def _interface_by_name(network: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(item.get("name")): item
        for item in network.get("interfaces", [])
        if isinstance(item, dict) and item.get("name")
    }


def _pfc_enabled_priorities(text: Any) -> list[int]:
    for line in str(text or "").splitlines():
        if line.strip().lower().startswith("prio-pfc"):
            return sorted(
                {
                    int(priority)
                    for priority, state in re.findall(
                        r"\b([0-7]):(on|off)\b", line, flags=re.I
                    )
                    if state.lower() == "on"
                }
            )
    return []


def _normalize_dcb_output(text: Any) -> list[str]:
    normalized: list[str] = []
    for line in str(text or "").splitlines():
        line = re.sub(r"\s+", " ", line.strip())
        if not line:
            continue
        line = re.sub(r"\bdev\s+\S+", "dev <netdev>", line, flags=re.I)
        normalized.append(line)
    return sorted(normalized)


def _roce_dcb_profile(
    netdevs: set[str], interfaces: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    """Summarise host DCB evidence without claiming policy correctness."""

    if not netdevs:
        return {"status": "NOT_APPLICABLE", "interfaces": {}}
    interface_profiles: dict[str, dict[str, Any]] = {}
    any_succeeded = False
    any_attempted = False
    all_required_succeeded = True
    all_required_has_output = True
    policy_complete = True
    required_sections = ("pfc", "ets", "app", "buffer", "dcbx")
    for name in sorted(netdevs):
        configuration = interfaces.get(name, {}).get("roce_configuration") or {}
        target_commands = configuration.get("dcb_targets")
        if not isinstance(target_commands, dict) or not target_commands:
            target_commands = {name: configuration.get("dcb_commands", {})}
        target_profiles: dict[str, dict[str, Any]] = {}
        for target, commands in sorted(target_commands.items()):
            sections: dict[str, dict[str, Any]] = {}
            for section in ("pfc", "ets", "app", "buffer", "dcbx"):
                command = commands.get(section) if isinstance(commands, dict) else None
                truncated = isinstance(command, dict) and any(
                    bool(command.get(field))
                    for field in (
                        "output_truncated",
                        "stdout_truncated",
                        "stderr_truncated",
                        "truncated",
                    )
                )
                succeeded = (
                    isinstance(command, dict)
                    and command.get("rc") == 0
                    and not truncated
                )
                any_attempted = any_attempted or isinstance(command, dict)
                stdout = (
                    str(command.get("stdout") or "")
                    if isinstance(command, dict)
                    else ""
                )
                sections[section] = {
                    "succeeded": succeeded,
                    "has_output": bool(stdout.strip()),
                    "truncated": truncated,
                    "normalized_output": _normalize_dcb_output(stdout),
                }
                any_succeeded = any_succeeded or succeeded
                if section in required_sections:
                    all_required_succeeded = all_required_succeeded and succeeded
                    all_required_has_output = (
                        all_required_has_output and bool(stdout.strip())
                    )
            pfc_command = commands.get("pfc") if isinstance(commands, dict) else None
            pfc_output = (
                pfc_command.get("stdout") if isinstance(pfc_command, dict) else ""
            )
            (
                pfc_priorities,
                _pfc_table_present,
                pfc_table_complete,
            ) = _parse_pfc(str(pfc_output or ""))
            app_output = " ".join(sections["app"]["normalized_output"])
            ets_output = " ".join(sections["ets"]["normalized_output"])
            dcbx_output = " ".join(sections["dcbx"]["normalized_output"])
            app_mapping_present = bool(
                re.search(
                    r"\b[a-z][a-z-]*-prio\b.*\b(?:0x)?[0-9a-f]+\s*:\s*[0-7]\b",
                    app_output,
                    flags=re.I,
                )
            )
            (
                _priority_tc,
                _ets_table_present,
                ets_table_complete,
            ) = _parse_pairs(ets_output, "prio-tc", key_max=7)
            ets_mapping_present = ets_table_complete
            dcbx_mode_present = bool(
                re.search(
                    r"\b(host|ieee|cee|static|firmware)\b",
                    dcbx_output,
                    flags=re.I,
                )
            )
            if not (
                pfc_table_complete
                and pfc_priorities
                and app_mapping_present
                and ets_mapping_present
                and dcbx_mode_present
            ):
                policy_complete = False
            target_profiles[str(target)] = {
                "sections": sections,
                "pfc_enabled_priorities": pfc_priorities,
                "pfc_table_complete": pfc_table_complete,
                "ets_table_complete": ets_table_complete,
                "app_mapping_present": app_mapping_present,
            }
        interface_profiles[name] = {"targets": target_profiles}
    if not any_succeeded:
        status = (
            "INCOMPLETE_COMMAND_FAILED" if any_attempted else "INCOMPLETE_NO_DCB_EVIDENCE"
        )
    elif not all_required_succeeded:
        status = "PARTIAL"
    elif not all_required_has_output or not policy_complete:
        status = "COLLECTED_POLICY_INCOMPLETE"
    else:
        status = "COLLECTED_POLICY_UNVALIDATED"
    return {"status": status, "interfaces": interface_profiles}


def _dcb_policy_profiles(dcb_profile: dict[str, Any]) -> list[dict[str, Any]]:
    """Return stable DCB signatures bound to their source and target rails."""

    profiles: list[dict[str, Any]] = []
    interfaces = dcb_profile.get("interfaces") or {}
    if not isinstance(interfaces, dict):
        return profiles
    for source_netdev, interface in interfaces.items():
        if not isinstance(interface, dict):
            continue
        targets = interface.get("targets") or {}
        if not isinstance(targets, dict):
            continue
        for target_netdev, target in targets.items():
            if not isinstance(target, dict):
                continue
            sections = target.get("sections") or {}
            profiles.append(
                {
                    # Interface names are the stable local rail identity.  They
                    # must not be discarded: otherwise swapping two complete
                    # DCB policies between eth4/eth5 compares equal.
                    "source_netdev": str(source_netdev),
                    "target_netdev": str(target_netdev),
                    "pfc_enabled_priorities": target.get(
                        "pfc_enabled_priorities"
                    )
                    or [],
                    "app_mapping_present": bool(target.get("app_mapping_present")),
                    "sections": {
                        name: (sections.get(name) or {}).get("normalized_output") or []
                        for name in ("pfc", "ets", "app", "buffer", "dcbx")
                    },
                }
            )
    return sorted(
        profiles,
        key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True),
    )


def _roce_configuration_rail_profiles(
    dcb_profile: dict[str, Any],
    configuration_health: dict[str, Any],
) -> tuple[list[dict[str, Any]], bool]:
    """Bind DCB, pause and FEC evidence to the same stable rail identity.

    Collection order is deliberately removed, while source/leaf target names
    are retained.  The boolean reports whether pause/FEC evidence is complete
    enough for a cross-node fabric comparison; incomplete evidence must become
    ``RDMA_FABRIC_PROFILE_EVIDENCE_MISSING`` rather than a false mismatch.
    """

    profiles_by_rail: dict[tuple[str, str], dict[str, Any]] = {}

    def profile_for(source_netdev: str, target_netdev: str) -> dict[str, Any]:
        identity = (source_netdev, target_netdev)
        return profiles_by_rail.setdefault(
            identity,
            {
                "source_netdev": source_netdev,
                "target_netdev": target_netdev,
            },
        )

    for policy in _dcb_policy_profiles(dcb_profile):
        source_netdev = str(policy["source_netdev"])
        target_netdev = str(policy["target_netdev"])
        profile_for(source_netdev, target_netdev)["dcb_policy"] = {
            key: value
            for key, value in policy.items()
            if key not in {"source_netdev", "target_netdev"}
        }

    evidence_complete = True
    health_dcb_targets = configuration_health.get("dcb_targets")
    if not isinstance(health_dcb_targets, dict) or not health_dcb_targets:
        evidence_complete = False
    else:
        for target in health_dcb_targets.values():
            if not isinstance(target, dict):
                evidence_complete = False
                continue
            section_evidence = target.get("section_evidence") or {}
            required_section_statuses = {
                str((section_evidence.get(name) or {}).get("status") or "MISSING")
                for name in ("pfc", "ets", "app", "buffer", "dcbx")
            }
            if required_section_statuses != {"COMPLETE"}:
                evidence_complete = False
            if target.get("pfc_table_complete") is not True:
                evidence_complete = False
            if target.get("ets_table_complete") is not True:
                evidence_complete = False

    for evidence_name in ("pause", "fec"):
        results = configuration_health.get(evidence_name)
        if not isinstance(results, dict) or not results:
            evidence_complete = False
            continue
        for target_netdev, result in results.items():
            if not isinstance(result, dict):
                evidence_complete = False
                continue
            source_netdev = str(result.get("source_netdev") or "").strip()
            target_name = str(target_netdev or "").strip()
            evidence_status = str(result.get("evidence_status") or "UNKNOWN").upper()
            settings = result.get("settings")
            valid_settings = isinstance(settings, dict)
            if evidence_name == "pause":
                valid_settings = valid_settings and set(settings) == {"rx", "tx"}
            else:
                valid_settings = valid_settings and settings.get("active") is not None
            if not source_netdev or not target_name:
                evidence_complete = False
                continue
            if evidence_status != "COMPLETE" or not valid_settings:
                evidence_complete = False
            normalized_settings = (
                {str(key): settings[key] for key in sorted(settings)}
                if isinstance(settings, dict)
                else None
            )
            profile_for(source_netdev, target_name)[evidence_name] = {
                "evidence_status": evidence_status,
                "settings": normalized_settings,
            }

    evidence_check_prefixes = (
        "ROCE_PAUSE_TARGETS:",
        "ROCE_FEC_TARGETS:",
        "ROCE_GLOBAL_PAUSE:",
        "ROCE_FEC:",
    )
    for check in configuration_health.get("checks") or []:
        if not isinstance(check, dict):
            continue
        check_id = str(check.get("check_id") or "")
        if check_id.startswith(evidence_check_prefixes) and check.get("status") == "UNKNOWN":
            evidence_complete = False

    profiles = sorted(
        profiles_by_rail.values(),
        key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True),
    )
    return profiles, evidence_complete


def _evaluate_ib_counter_port(
    device: str, port: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    identity = {"device": device, "port": str(port.get("port") or "UNKNOWN")}
    window = port.get("counter_window")
    if not isinstance(window, dict):
        return (
            {
                **identity,
                "status": "UNKNOWN",
                "sampling_status": "MISSING",
                "reason_codes": ["IB_COUNTER_WINDOW_MISSING"],
                "coverage": None,
                "metrics": {},
            },
            None,
        )
    sampling_status = str(window.get("status") or "UNKNOWN").upper()
    if sampling_status in {"DISABLED", "INVALID_INTERVAL"}:
        reason = (
            "IB_COUNTER_SAMPLING_DISABLED"
            if sampling_status == "DISABLED"
            else "IB_COUNTER_SAMPLING_CONFIGURATION_INVALID"
        )
        return (
            {
                **identity,
                "status": "UNKNOWN",
                "sampling_status": sampling_status,
                "reason_codes": [reason],
                "coverage": None,
                "metrics": {},
                "configured_value": window.get("configured_value"),
            },
            None,
        )
    before = window.get("before")
    after = window.get("after")
    before_counters = before.get("counters") if isinstance(before, dict) else None
    after_counters = after.get("counters") if isinstance(after, dict) else None
    try:
        configured_interval = float(window.get("configured_interval_seconds"))
    except (TypeError, ValueError):
        configured_interval = 0.0
    health = evaluate_ib_counter_samples(
        before_counters,
        after_counters,
        interval_seconds=window.get("interval_seconds"),
        max_interval_seconds=max(60.0, configured_interval * 1.25),
    )
    policy = health.pop("policy", None)
    health.update(
        {
            **identity,
            "sampling_status": sampling_status,
            "configured_interval_seconds": window.get(
                "configured_interval_seconds"
            ),
            "hw_counter_status_before": (
                before.get("hw_counter_status")
                if isinstance(before, dict)
                else None
            ),
            "hw_counter_status_after": (
                after.get("hw_counter_status") if isinstance(after, dict) else None
            ),
        }
    )
    return health, policy


_ROCE_COUNTER_STATUS_ORDER = {"PASS": 0, "WARN": 1, "UNKNOWN": 2, "FAIL": 3}


def _nonnegative_counter(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = int(str(value).strip(), 0)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


_PMA_COUNTER_SATURATION_WIDTHS = {
    (1 << bits) - 1: bits for bits in (4, 8, 16, 32)
}


def _pma_counter_saturation_width(
    name: str,
    value_before: int | None,
    value_after: int | None,
) -> int | None:
    if not name.startswith("counters:") or value_before != value_after:
        return None
    if value_before is None:
        return None
    return _PMA_COUNTER_SATURATION_WIDTHS.get(value_before)

def _roce_counter_category(name: str) -> str | None:
    normalized = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    error_tokens = (
        "error", "_err", "discard", "drop", "dropped", "timeout",
        "out_of_buffer", "buffer_overrun", "link_down", "symbol_error",
    )
    if any(token in normalized for token in error_tokens):
        return "ERROR_OR_DROP"
    congestion_tokens = ("pfc", "pause", "cnp", "ecn", "congestion", "xmit_wait")
    if any(token in normalized for token in congestion_tokens):
        return "CONGESTION_SIGNAL"
    return None


def _roce_counter_snapshot(snapshot: Any) -> dict[str, Any]:
    if not isinstance(snapshot, dict):
        return {}
    flattened: dict[str, Any] = {}
    for section in (
        "counters", "hw_counters", "roce_counters", "ethtool_stats",
        "priority_counters",
    ):
        values = snapshot.get(section)
        if not isinstance(values, dict):
            continue
        for name, value in values.items():
            flattened[f"{section}:{name}"] = value
    return flattened


def _evidence_output_truncated(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    return any(
        bool(value.get(field))
        for field in (
            "output_truncated",
            "stdout_truncated",
            "stderr_truncated",
            "truncated",
            "counter_output_truncated",
            "hw_counter_output_truncated",
            "roce_counter_output_truncated",
            "ethtool_output_truncated",
            "priority_counter_output_truncated",
        )
    )

def _evaluate_roce_counter_port(device: str, port: dict[str, Any]) -> dict[str, Any]:
    identity = {"device": device, "port": str(port.get("port") or "UNKNOWN")}
    window = port.get("counter_window")
    if not isinstance(window, dict):
        return {
            **identity,
            "status": "UNKNOWN",
            "sampling_status": "MISSING",
            "reason_codes": ["ROCE_COUNTER_WINDOW_MISSING"],
            "coverage": None,
            "metrics": {},
        }
    sampling_status = str(window.get("status") or "UNKNOWN").upper()
    if sampling_status in {"DISABLED", "INVALID_INTERVAL"}:
        return {
            **identity,
            "status": "UNKNOWN",
            "sampling_status": sampling_status,
            "reason_codes": [
                "ROCE_COUNTER_SAMPLING_DISABLED"
                if sampling_status == "DISABLED"
                else "ROCE_COUNTER_SAMPLING_CONFIGURATION_INVALID"
            ],
            "coverage": None,
            "metrics": {},
        }

    try:
        interval = float(window.get("interval_seconds"))
        configured_interval = float(window.get("configured_interval_seconds") or interval)
    except (TypeError, ValueError):
        interval = 0.0
        configured_interval = 0.0
    interval_valid = 1.0 <= interval <= max(60.0, configured_interval * 1.25)
    before = window.get("before")
    after = window.get("after")
    before_values = _roce_counter_snapshot(before)
    after_values = _roce_counter_snapshot(after)
    selected_names = sorted(
        name
        for name in set(before_values) | set(after_values)
        if _roce_counter_category(name.split(":", 1)[-1]) is not None
    )
    metrics: dict[str, dict[str, Any]] = {}
    statuses: list[str] = []
    reason_codes: set[str] = set()
    error_or_drop_comparable = 0
    saturated_metrics: list[str] = []
    for name in selected_names:
        category = str(_roce_counter_category(name.split(":", 1)[-1]))
        value_before = _nonnegative_counter(before_values.get(name))
        value_after = _nonnegative_counter(after_values.get(name))
        metric_status = "UNKNOWN"
        reason = "ROCE_COUNTER_EVIDENCE_MISSING"
        delta = None
        saturation_bits = None
        if not interval_valid:
            reason = "ROCE_COUNTER_INTERVAL_INVALID"
        elif value_before is None or value_after is None:
            reason = "ROCE_COUNTER_EVIDENCE_MISSING"
        elif value_after < value_before:
            reason = "ROCE_COUNTER_RESET_OR_WRAP"
        else:
            delta = value_after - value_before
            if category == "ERROR_OR_DROP":
                error_or_drop_comparable += 1
            if delta > 0 and category == "ERROR_OR_DROP":
                metric_status = "FAIL"
                reason = "ROCE_COUNTER_ERROR_OR_DROP_GROWTH"
            elif delta > 0:
                metric_status = "WARN"
                reason = (
                    "ROCE_CNP_COUNTER_GROWTH"
                    if "cnp" in name.lower()
                    else "ROCE_PFC_OR_CONGESTION_COUNTER_GROWTH"
                )
            else:
                saturation_bits = _pma_counter_saturation_width(
                    name, value_before, value_after
                )
                if saturation_bits is not None:
                    metric_status = "UNKNOWN"
                    reason = "POSSIBLE_COUNTER_SATURATION"
                    saturated_metrics.append(name)
                else:
                    metric_status = "PASS"
                    reason = "ROCE_COUNTER_STABLE"
        metrics[name] = {
            "category": category,
            "before": value_before,
            "after": value_after,
            "delta": delta,
            "rate_per_second": (
                delta / interval if delta is not None and interval_valid else None
            ),
            "status": metric_status,
            "reason_code": reason,
            "possible_saturation_bits": saturation_bits,
        }
        statuses.append(metric_status)
        if metric_status != "PASS":
            reason_codes.add(reason)

    explicit_statuses = []
    for snapshot in (before, after):
        if not isinstance(snapshot, dict):
            explicit_statuses.append("MISSING")
            continue
        for field in (
            "counter_status", "hw_counter_status", "roce_counter_status",
            "ethtool_status", "priority_counter_status",
        ):
            if field in snapshot:
                explicit_statuses.append(str(snapshot.get(field) or "UNKNOWN").upper())
    output_truncated = any(
        _evidence_output_truncated(item) for item in (window, before, after)
    )
    collection_incomplete = (
        sampling_status != "COMPLETE"
        or any(status != "COMPLETE" for status in explicit_statuses)
        or output_truncated
    )
    before_standard = before.get("counters") if isinstance(before, dict) else None
    after_standard = after.get("counters") if isinstance(after, dict) else None
    required_standard = {
        name for name, rule in DEFAULT_IB_COUNTER_RULES.items() if rule.required
    }
    standard_missing = sorted(
        name
        for name in required_standard
        if isinstance(before_standard, dict)
        and isinstance(after_standard, dict)
        and (name not in before_standard or name not in after_standard)
    )
    if (
        not selected_names
        or not error_or_drop_comparable
        or collection_incomplete
        or standard_missing
    ):
        statuses.append("UNKNOWN")
        reason_codes.add(
            "ROCE_COUNTER_EVIDENCE_TRUNCATED"
            if output_truncated
            else "ROCE_COUNTER_HEALTH_EVIDENCE_MISSING"
        )
    status = max(statuses or ["UNKNOWN"], key=_ROCE_COUNTER_STATUS_ORDER.__getitem__)
    if status == "PASS":
        reason_codes = {"ROCE_COUNTERS_STABLE"}
    return {
        **identity,
        "status": status,
        "sampling_status": sampling_status,
        "interval_seconds": interval if interval_valid else None,
        "interval_valid": interval_valid,
        "reason_codes": sorted(reason_codes),
        "coverage": {
            "selected_total": len(selected_names),
            "error_or_drop_comparable": error_or_drop_comparable,
            "collection_statuses": explicit_statuses,
            "missing_required_standard_counters": standard_missing,
            "output_truncated": output_truncated,
            "possible_counter_saturation": bool(saturated_metrics),
            "saturated_metrics": saturated_metrics,
            "complete": bool(
                selected_names
                and error_or_drop_comparable
                and not collection_incomplete
                and not standard_missing
                and not saturated_metrics
            ),
        },
        "metrics": metrics,
    }


def _parse_ibv_devices_output(text: Any) -> list[str]:
    devices: set[str] = set()
    for line in str(text or "").splitlines():
        fields = line.strip().split()
        if not fields:
            continue
        name = fields[0]
        if name.lower() in {"device", "------"} or set(name) == {"-"}:
            continue
        if re.fullmatch(r"[A-Za-z0-9_.:-]+", name):
            devices.add(name)
    return sorted(devices)


def _command_evidence(result: Any, *, limit: int = 2048) -> dict[str, Any] | None:
    if not isinstance(result, dict):
        return None
    evidence = {"rc": result.get("rc")}
    for field in ("stdout", "stderr", "error"):
        value = str(result.get(field) or "").strip()
        if value:
            evidence[field] = value[:limit]
    return evidence


def _rdma_userspace_library_summary(evidence: Any) -> dict[str, Any]:
    if not isinstance(evidence, dict):
        return {
            "libibverbs": [],
            "providers": [],
            "rccl_net_plugins": [],
            "truncated": False,
        }
    grouped: dict[str, list[dict[str, Any]]] = {
        "LIBIBVERBS": [],
        "VERBS_PROVIDER": [],
        "RCCL_NET_PLUGIN": [],
    }
    for item in evidence.get("libraries", []):
        if not isinstance(item, dict):
            continue
        kind = str(item.get("kind") or "")
        if kind not in grouped or len(grouped[kind]) >= 128:
            continue
        grouped[kind].append(
            {
                "path": item.get("path"),
                "realpath": item.get("realpath"),
                "directory_source": item.get("directory_source"),
                "size_bytes": item.get("size_bytes"),
            }
        )
    return {
        "libibverbs": grouped["LIBIBVERBS"],
        "providers": grouped["VERBS_PROVIDER"],
        "rccl_net_plugins": grouped["RCCL_NET_PLUGIN"],
        "search_directories": (evidence.get("search_directories") or [])[:64],
        "explicit_ld_library_path_directories": (
            evidence.get("explicit_ld_library_path_directories") or []
        )[:32],
        "ignored_ld_library_path_entries": (
            evidence.get("ignored_ld_library_path_entries") or []
        )[:32],
        "ld_library_path_truncated": bool(
            evidence.get("ld_library_path_truncated")
        ),
        "truncated": bool(evidence.get("truncated")),
        "limits": evidence.get("limits"),
    }


def _rdma_provider_config_summary(evidence: Any) -> dict[str, Any]:
    if not isinstance(evidence, dict):
        return {"directories": [], "files": [], "truncated": False}
    files: list[dict[str, Any]] = []
    for item in evidence.get("files", [])[:64]:
        if not isinstance(item, dict):
            continue
        files.append(
            {
                "path": item.get("path"),
                "realpath": item.get("realpath"),
                "content_excerpt": str(item.get("content") or "")[:512],
                "truncated": bool(item.get("truncated")),
                "error": str(item.get("error") or "")[:512] or None,
            }
        )
    return {
        "directories": (evidence.get("directories") or [])[:16],
        "files": files,
        "truncated": bool(evidence.get("truncated")),
        "limits": evidence.get("limits"),
    }


def _evaluate_rdma_userspace(network: dict[str, Any]) -> dict[str, Any]:
    """Evaluate whether sysfs HCAs are usable through the userspace verbs API."""
    rdma_devices = [
        item for item in network.get("rdma_devices", []) if isinstance(item, dict)
    ]
    target_devices = sorted(
        {str(item.get("name")) for item in rdma_devices if item.get("name")}
    )
    userspace = network.get("rdma_userspace")
    userspace = userspace if isinstance(userspace, dict) else {}
    config_summary = _rdma_provider_config_summary(userspace.get("provider_configs"))
    library_summary = _rdma_userspace_library_summary(userspace.get("libraries"))
    provider_hints: set[str] = set()
    for device in rdma_devices:
        raw_hint = str(device.get("driver") or "").strip().lower()
        if not raw_hint:
            raw_hint = re.sub(r"_?\d+$", "", str(device.get("name") or "").lower())
        for suffix in ("_core", "_roce", "_ib"):
            if raw_hint.endswith(suffix):
                raw_hint = raw_hint[: -len(suffix)]
        if raw_hint:
            provider_hints.add(raw_hint)
    config_haystack = "\n".join(
        " ".join(
            str(item.get(field) or "")
            for field in ("path", "realpath", "content_excerpt")
        ).lower()
        for item in config_summary.get("files", [])
    )
    provider_haystack = "\n".join(
        " ".join(str(item.get(field) or "") for field in ("path", "realpath")).lower()
        for item in library_summary.get("providers", [])
    )
    provider_evidence = [
        {
            "provider": hint,
            "config_present": bool(re.search(rf"(?<![a-z0-9]){re.escape(hint)}(?![a-z0-9])", config_haystack)),
            "library_present": f"lib{hint}" in provider_haystack,
        }
        for hint in sorted(provider_hints)
    ]
    result: dict[str, Any] = {
        "status": "UNKNOWN",
        "reason_code": "RDMA_USERSPACE_EVIDENCE_MISSING",
        "sysfs_devices": target_devices,
        "enumerated_devices": [],
        "missing_enumerated_devices": target_devices,
        "device_open_checks": [],
        "ibv_devices": None,
        "ibv_devices_tool_path": None,
        "ibv_devinfo_tool_path": userspace.get("ibv_devinfo_tool_path"),
        "provider_configs": config_summary,
        "libraries": library_summary,
        "expected_provider_evidence": provider_evidence,
    }
    if not target_devices:
        result.update(
            {
                "status": "NOT_APPLICABLE",
                "reason_code": "RDMA_USERSPACE_NOT_APPLICABLE",
                "message": "no RDMA HCA is visible in sysfs",
                "missing_enumerated_devices": [],
            }
        )
        return result

    ibv_devices = userspace.get("ibv_devices")
    ibv_devices = ibv_devices if isinstance(ibv_devices, dict) else {}
    devices_tool_path = ibv_devices.get("tool_path")
    devices_command = ibv_devices.get("command")
    result["ibv_devices_tool_path"] = devices_tool_path
    result["ibv_devices"] = _command_evidence(devices_command)
    if not devices_tool_path or not isinstance(devices_command, dict):
        result.update(
            {
                "status": "UNKNOWN",
                "reason_code": "RDMA_USERSPACE_TOOL_UNAVAILABLE",
                "message": "ibv_devices is unavailable; userspace provider enumeration cannot be verified",
            }
        )
        return result

    enumerated = ibv_devices.get("enumerated_devices")
    if not isinstance(enumerated, list):
        enumerated = _parse_ibv_devices_output(devices_command.get("stdout"))
    enumerated_devices = sorted(
        {str(name) for name in enumerated if str(name).strip()}
    )
    missing_enumerated = sorted(set(target_devices) - set(enumerated_devices))
    result["enumerated_devices"] = enumerated_devices
    result["missing_enumerated_devices"] = missing_enumerated
    try:
        devices_rc = int(devices_command.get("rc"))
    except (TypeError, ValueError):
        devices_rc = None
    if devices_rc is None:
        result.update(
            {
                "status": "UNKNOWN",
                "reason_code": "RDMA_USERSPACE_EVIDENCE_MISSING",
                "message": "ibv_devices return code is unavailable",
            }
        )
        return result
    if devices_rc != 0 or missing_enumerated:
        stderr = str(
            devices_command.get("stderr")
            or devices_command.get("error")
            or ""
        ).strip()[:1024]
        missing_provider_evidence = [
            item["provider"]
            for item in provider_evidence
            if not item["config_present"] and not item["library_present"]
        ]
        result.update(
            {
                "status": "FAIL",
                "reason_code": "RDMA_USERSPACE_PROVIDER_UNAVAILABLE",
                "message": (
                    f"sysfs HCAs={','.join(target_devices)}; "
                    f"ibv_devices rc={devices_rc}; "
                    f"enumerated={','.join(enumerated_devices) or 'none'}; "
                    f"missing={','.join(missing_enumerated) or 'none'}"
                    + (f"; stderr={stderr}" if stderr else "")
                    + (
                        "; bounded config/library scan did not find provider evidence for "
                        + ",".join(missing_provider_evidence)
                        if missing_provider_evidence
                        else ""
                    )
                ),
            }
        )
        return result

    open_checks: list[dict[str, Any]] = []
    open_failures: list[str] = []
    open_unknown: list[str] = []
    for device in rdma_devices:
        name = str(device.get("name") or "")
        if not name or name not in target_devices:
            continue
        command = device.get("ibv_devinfo")
        evidence = _command_evidence(command)
        item = {"device": name, "command": evidence}
        if not isinstance(command, dict) or command.get("rc") is None:
            item["status"] = "UNKNOWN"
            open_unknown.append(name)
        else:
            try:
                rc = int(command.get("rc"))
            except (TypeError, ValueError):
                rc = None
            if rc == 0:
                item["status"] = "PASS"
            elif rc is None:
                item["status"] = "UNKNOWN"
                open_unknown.append(name)
            else:
                item["status"] = "FAIL"
                open_failures.append(name)
        open_checks.append(item)
    result["device_open_checks"] = open_checks
    if open_failures:
        failed_evidence = next(
            (
                item.get("command")
                for item in open_checks
                if item.get("device") in open_failures
            ),
            None,
        ) or {}
        stderr = str(
            failed_evidence.get("stderr") or failed_evidence.get("error") or ""
        ).strip()[:1024]
        result.update(
            {
                "status": "FAIL",
                "reason_code": "RDMA_USERSPACE_DEVICE_OPEN_FAILED",
                "message": (
                    f"ibv_devices enumerated all sysfs HCAs, but ibv_devinfo could not open "
                    f"{','.join(open_failures)}"
                    + (f"; stderr={stderr}" if stderr else "")
                ),
            }
        )
    elif open_unknown or len(open_checks) != len(target_devices):
        missing_checks = sorted(
            set(target_devices) - {str(item.get("device")) for item in open_checks}
        )
        result.update(
            {
                "status": "UNKNOWN",
                "reason_code": "RDMA_USERSPACE_TOOL_UNAVAILABLE",
                "message": (
                    "ibv_devinfo evidence is unavailable for "
                    + ",".join(sorted(set(open_unknown + missing_checks)))
                ),
            }
        )
    else:
        result.update(
            {
                "status": "PASS",
                "reason_code": "RDMA_USERSPACE_READY",
                "message": (
                    f"ibv_devices enumerated and ibv_devinfo opened all "
                    f"{len(target_devices)} sysfs HCAs"
                ),
            }
        )
    return result

def evaluate_rdma_network(
    network: dict[str, Any],
    *,
    expected_protocol: str = PROTOCOL_AUTO,
    required: bool = False,
    rdma_policy: dict[str, Any] | None = None,
) -> tuple[list[Finding], list[dict[str, Any]], dict[str, Any]]:
    """Evaluate current IB/RoCE configuration and endpoint readiness.

    This is a startup-time static check.  It deliberately does not claim that
    RCCL selected RDMA for a particular training run or that TCP fallback did
    not occur.
    """

    if expected_protocol not in EXPECTED_PROTOCOLS:
        raise ValueError(
            f"expected_protocol must be one of {sorted(EXPECTED_PROTOCOLS)}"
        )

    ports: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    for device in network.get("rdma_devices", []):
        if not isinstance(device, dict):
            continue
        for port in device.get("ports", []):
            if isinstance(port, dict):
                ports.append(
                    (str(device.get("name") or "UNKNOWN"), port, classify_rdma_port(port))
                )

    protocol_counts = {
        protocol: sum(classified["current_protocol"] == protocol for _, _, classified in ports)
        for protocol in (
            "NATIVE_INFINIBAND",
            "ROCE",
            "ETHERNET_RDMA_UNCONFIRMED",
            "ETHERNET_RDMA_EVIDENCE_INCOMPLETE",
            "UNKNOWN",
        )
    }
    active_protocols = {
        classified["current_protocol"]
        for _, _, classified in ports
        if classified["current_protocol"] in {"NATIVE_INFINIBAND", "ROCE"}
    }
    if len(active_protocols) > 1:
        current_protocol = "MIXED"
    elif active_protocols and (
        protocol_counts["ETHERNET_RDMA_UNCONFIRMED"]
        or protocol_counts["ETHERNET_RDMA_EVIDENCE_INCOMPLETE"]
        or protocol_counts["UNKNOWN"]
    ):
        current_protocol = "MIXED_OR_INCOMPLETE"
    elif active_protocols == {"NATIVE_INFINIBAND"}:
        current_protocol = "NATIVE_INFINIBAND"
    elif active_protocols == {"ROCE"}:
        current_protocol = "ROCE"
    elif protocol_counts["ETHERNET_RDMA_EVIDENCE_INCOMPLETE"]:
        current_protocol = "ETHERNET_RDMA_EVIDENCE_INCOMPLETE"
    elif protocol_counts["ETHERNET_RDMA_UNCONFIRMED"]:
        current_protocol = "ETHERNET_RDMA_UNCONFIRMED"
    elif ports:
        current_protocol = "UNKNOWN"
    else:
        current_protocol = "NO_RDMA_PORTS"

    findings: list[Finding] = []
    checks: list[dict[str, Any]] = []

    def add_check(
        check_id: str,
        status: str,
        reason_code: str,
        message: str,
    ) -> None:
        checks.append({"check_id": check_id, "status": status, "message": message})
        if status != "PASS" and status != "NOT_APPLICABLE":
            findings.append(Finding(status, reason_code, message))

    if current_protocol == "MIXED":
        protocol_status = "FAIL"
        add_check(
            "RDMA_CURRENT_PROTOCOL",
            "FAIL",
            "RDMA_PROTOCOL_MIXED_ON_NODE",
            "native InfiniBand and RoCE ports are mixed on the same node",
        )
    elif current_protocol in {
        "UNKNOWN",
        "NO_RDMA_PORTS",
        "ETHERNET_RDMA_UNCONFIRMED",
        "ETHERNET_RDMA_EVIDENCE_INCOMPLETE",
        "MIXED_OR_INCOMPLETE",
    }:
        protocol_status = "UNKNOWN" if required else "WARN"
        add_check(
            "RDMA_CURRENT_PROTOCOL",
            protocol_status,
            "RDMA_PROTOCOL_EVIDENCE_MISSING",
            f"current RDMA protocol={current_protocol}",
        )
    else:
        protocol_status = "PASS"
        add_check(
            "RDMA_CURRENT_PROTOCOL",
            "PASS",
            "RDMA_CURRENT_PROTOCOL",
            f"current RDMA protocol={current_protocol}",
        )

    expected_actual = {
        PROTOCOL_IB: "NATIVE_INFINIBAND",
        PROTOCOL_ROCE: "ROCE",
    }.get(expected_protocol)
    if expected_actual is not None:
        if current_protocol == expected_actual:
            add_check(
                "RDMA_EXPECTED_PROTOCOL",
                "PASS",
                "RDMA_EXPECTED_PROTOCOL",
                f"expected={expected_protocol}, actual={current_protocol}",
            )
        else:
            expected_evidence_incomplete = current_protocol in {
                "UNKNOWN",
                "ETHERNET_RDMA_EVIDENCE_INCOMPLETE",
            }
            if current_protocol == "MIXED_OR_INCOMPLETE" and active_protocols == {expected_actual}:
                if expected_actual == "ROCE":
                    expected_evidence_incomplete = (
                        protocol_counts["ETHERNET_RDMA_UNCONFIRMED"] == 0
                    )
                else:
                    expected_evidence_incomplete = (
                        protocol_counts["ETHERNET_RDMA_UNCONFIRMED"] == 0
                        and protocol_counts["ETHERNET_RDMA_EVIDENCE_INCOMPLETE"] == 0
                    )
        if expected_actual is not None and current_protocol != expected_actual and expected_evidence_incomplete:
            protocol_status = "UNKNOWN"
            add_check(
                "RDMA_EXPECTED_PROTOCOL",
                "UNKNOWN",
                "RDMA_EXPECTED_PROTOCOL_EVIDENCE_MISSING",
                f"expected={expected_protocol}, actual cannot be verified because GID/link evidence is incomplete",
            )
        elif expected_actual is not None and current_protocol != expected_actual:
            protocol_status = "FAIL"
            add_check(
                "RDMA_EXPECTED_PROTOCOL",
                "FAIL",
                "RDMA_PROTOCOL_MISMATCH",
                f"expected={expected_protocol}, actual={current_protocol}",
            )

    rdma_userspace = _evaluate_rdma_userspace(network)
    userspace_status = str(rdma_userspace.get("status") or "UNKNOWN")
    userspace_reason = str(
        rdma_userspace.get("reason_code") or "RDMA_USERSPACE_EVIDENCE_MISSING"
    )
    userspace_message = str(
        rdma_userspace.get("message") or "RDMA userspace evidence is unavailable"
    )
    userspace_strict = required or expected_protocol in {PROTOCOL_IB, PROTOCOL_ROCE}
    if userspace_status == "NOT_APPLICABLE":
        checks.append(
            {
                "check_id": "RDMA_USERSPACE",
                "status": "NOT_APPLICABLE",
                "message": userspace_message,
            }
        )
    elif userspace_status == "PASS":
        add_check(
            "RDMA_USERSPACE",
            "PASS",
            "RDMA_USERSPACE_READY",
            userspace_message,
        )
    elif userspace_status == "FAIL":
        add_check(
            "RDMA_USERSPACE",
            "FAIL" if userspace_strict else "WARN",
            userspace_reason,
            userspace_message,
        )
    else:
        add_check(
            "RDMA_USERSPACE",
            "UNKNOWN" if userspace_strict else "WARN",
            userspace_reason,
            userspace_message,
        )
    rdma_userspace["required"] = userspace_strict
    rdma_userspace["check_status"] = checks[-1]["status"]
    ib_ports = [(device, port, classified) for device, port, classified in ports if classified["current_protocol"] == "NATIVE_INFINIBAND"]
    ib_active = sum(classified["active_linkup"] for _, _, classified in ib_ports)
    ib_valid_lid = sum(_numeric_nonzero(port.get("lid")) for _, port, _ in ib_ports)
    ib_valid_sm_lid = sum(_numeric_nonzero(port.get("sm_lid")) for _, port, _ in ib_ports)
    ib_valid_gid = sum(bool(_valid_gid_entries(port)) for _, port, _ in ib_ports)
    ib_valid_pkey = sum(bool(_valid_pkeys(port)) for _, port, _ in ib_ports)
    ib_evidence_missing = sum(
        any(
            port.get(field) is None
            for field in (
                "state",
                "phys_state",
                "lid",
                "sm_lid",
                "gids",
                "pkeys",
            )
        )
        or _normalized_rate_mbps(port.get("rate")) is None
        or _mtu_value(port.get("active_mtu")) is None
        or _mtu_value(port.get("max_mtu")) is None
        or _normalized_subnet_prefix(port.get("subnet_prefix")) is None
        or not _collection_complete(port, "gids")
        or not _collection_complete(port, "pkeys")
        for _, port, _ in ib_ports
    )
    ib_invalid_mtu = sum(
        _mtu_value(port.get("active_mtu")) is not None
        and _mtu_value(port.get("max_mtu")) is not None
        and _mtu_value(port.get("active_mtu")) > _mtu_value(port.get("max_mtu"))
        for _, port, _ in ib_ports
    )
    ib_definitive_fail = any(
        (
            port.get("state") is not None
            and port.get("phys_state") is not None
            and not classified["active_linkup"]
        )
        or (port.get("lid") is not None and not _numeric_nonzero(port.get("lid")))
        or (port.get("sm_lid") is not None and not _numeric_nonzero(port.get("sm_lid")))
        or (_collection_complete(port, "gids") and not _valid_gid_entries(port))
        or (_collection_complete(port, "pkeys") and not _valid_pkeys(port))
        or (
            _mtu_value(port.get("active_mtu")) is not None
            and _mtu_value(port.get("max_mtu")) is not None
            and _mtu_value(port.get("active_mtu")) > _mtu_value(port.get("max_mtu"))
        )
        for _, port, classified in ib_ports
    )
    if not ib_ports:
        ib_status = "NOT_APPLICABLE"
    elif ib_definitive_fail:
        ib_status = "FAIL" if (required or expected_protocol == PROTOCOL_IB) else "WARN"
        add_check(
            "IB_ENDPOINT",
            ib_status,
            "IB_ENDPOINT_NOT_READY",
            f"IB ports={len(ib_ports)}, active={ib_active}, valid_lid={ib_valid_lid}, "
            f"valid_sm_lid={ib_valid_sm_lid}, valid_gid={ib_valid_gid}, "
            f"valid_pkey={ib_valid_pkey}, invalid_mtu={ib_invalid_mtu}",
        )
    elif ib_evidence_missing:
        ib_status = "UNKNOWN" if (required or expected_protocol == PROTOCOL_IB) else "WARN"
        add_check(
            "IB_ENDPOINT",
            ib_status,
            "IB_ENDPOINT_EVIDENCE_MISSING",
            f"IB endpoint evidence is incomplete on {ib_evidence_missing}/{len(ib_ports)} ports",
        )
    else:
        ib_status = "PASS"
        add_check(
            "IB_ENDPOINT",
            "PASS",
            "IB_ENDPOINT",
            f"IB ports={len(ib_ports)}, all Active+LinkUp with valid LID/SM-LID/GID/P_Key",
        )

    ib_counter_port_results: list[dict[str, Any]] = []
    ib_counter_policy: dict[str, Any] | None = None
    for device, port, _classified in ib_ports:
        port_result, policy = _evaluate_ib_counter_port(device, port)
        ib_counter_port_results.append(port_result)
        if ib_counter_policy is None and policy is not None:
            ib_counter_policy = policy
    counter_status_order = {"PASS": 0, "WARN": 1, "UNKNOWN": 2, "FAIL": 3}
    if ib_counter_port_results:
        ib_counter_observed_status = max(
            (str(item.get("status") or "UNKNOWN") for item in ib_counter_port_results),
            key=counter_status_order.__getitem__,
        )
    else:
        ib_counter_observed_status = "NOT_APPLICABLE"
    ib_counter_status_counts = {
        status: sum(item.get("status") == status for item in ib_counter_port_results)
        for status in ("PASS", "WARN", "UNKNOWN", "FAIL")
    }
    ib_counter_reason_codes = sorted(
        {
            str(reason)
            for item in ib_counter_port_results
            for reason in item.get("reason_codes", [])
        }
    )
    ib_counter_required = required or expected_protocol == PROTOCOL_IB
    if not ib_ports:
        ib_counter_status = "NOT_APPLICABLE"
        checks.append(
            {
                "check_id": "IB_COUNTER_HEALTH",
                "status": "NOT_APPLICABLE",
                "message": "no port is currently configured in native InfiniBand mode",
            }
        )
    elif ib_counter_observed_status == "FAIL":
        ib_counter_status = "FAIL" if ib_counter_required else "WARN"
        add_check(
            "IB_COUNTER_HEALTH",
            ib_counter_status,
            "IB_COUNTER_ERROR_GROWTH",
            f"IB counter window: ports={len(ib_ports)}, "
            f"pass={ib_counter_status_counts['PASS']}, "
            f"warn={ib_counter_status_counts['WARN']}, "
            f"unknown={ib_counter_status_counts['UNKNOWN']}, "
            f"fail={ib_counter_status_counts['FAIL']}",
        )
    elif ib_counter_observed_status == "UNKNOWN":
        ib_counter_status = "UNKNOWN" if ib_counter_required else "WARN"
        if "IB_COUNTER_SAMPLING_CONFIGURATION_INVALID" in ib_counter_reason_codes:
            counter_reason = "IB_COUNTER_SAMPLING_CONFIGURATION_INVALID"
        elif "IB_COUNTER_SAMPLING_DISABLED" in ib_counter_reason_codes:
            counter_reason = "IB_COUNTER_SAMPLING_DISABLED"
        else:
            counter_reason = "IB_COUNTER_HEALTH_EVIDENCE_MISSING"
        add_check(
            "IB_COUNTER_HEALTH",
            ib_counter_status,
            counter_reason,
            f"IB counter evidence incomplete: ports={len(ib_ports)}, "
            f"unknown={ib_counter_status_counts['UNKNOWN']}, "
            f"reasons={','.join(ib_counter_reason_codes)}",
        )
    elif ib_counter_observed_status == "WARN":
        ib_counter_status = "WARN"
        add_check(
            "IB_COUNTER_HEALTH",
            "WARN",
            "IB_CONGESTION_SIGNAL_GROWTH",
            f"IB error counters are stable but congestion signal grew on "
            f"{ib_counter_status_counts['WARN']}/{len(ib_ports)} ports",
        )
    else:
        ib_counter_status = "PASS"
        add_check(
            "IB_COUNTER_HEALTH",
            "PASS",
            "IB_COUNTERS_STABLE",
            f"IB error/link-event counters are stable on {len(ib_ports)} ports",
        )

    roce_ports = [(device, port, classified) for device, port, classified in ports if classified["current_protocol"] == "ROCE"]
    interfaces = _interface_by_name(network)
    roce_versions = sorted(
        {
            version
            for _, _, classified in roce_ports
            for version in classified["roce_versions"]
        }
    )
    mapped_netdevs = {
        str(entry.get("netdev"))
        for _, port, _ in roce_ports
        for entry in _valid_gid_entries(port)
        if str(entry.get("netdev") or "").strip()
    }
    roce_candidate_netdevs = {
        str(name) for name in network.get("roce_candidate_netdevs", []) if name
    }
    roce_active = sum(classified["active_linkup"] for _, _, classified in roce_ports)
    roce_port_state_missing = sum(
        port.get("state") is None or port.get("phys_state") is None
        for _, port, _ in roce_ports
    )
    roce_rate_missing = sum(
        _normalized_rate_mbps(port.get("rate")) is None
        for _, port, _ in roce_ports
    )
    roce_inactive = sum(
        port.get("state") is not None
        and port.get("phys_state") is not None
        and not classified["active_linkup"]
        for _, port, classified in roce_ports
    )
    missing_netdevs = sorted(name for name in mapped_netdevs if name not in interfaces)
    roce_netdev_up = sum(
        interfaces.get(name, {}).get("local_link_status") == "UP"
        for name in mapped_netdevs
    )
    roce_netdev_down = sum(
        interfaces.get(name, {}).get("local_link_status") == "DOWN"
        for name in mapped_netdevs
    )
    roce_netdev_unknown = sum(
        name in interfaces
        and interfaces.get(name, {}).get("local_link_status") not in {"UP", "DOWN"}
        for name in mapped_netdevs
    )
    roce_mapped_ports = sum(classified["mapped_gid_count"] > 0 for _, _, classified in roce_ports)
    roce_mtu_values = {
        interfaces.get(name, {}).get("mtu")
        for name in mapped_netdevs
        if interfaces.get(name, {}).get("mtu")
    }
    roce_mtu_missing = sum(
        name not in interfaces or not interfaces.get(name, {}).get("mtu")
        for name in mapped_netdevs
    )
    if not roce_ports and protocol_counts["ETHERNET_RDMA_EVIDENCE_INCOMPLETE"]:
        roce_status = "UNKNOWN"
        add_check(
            "ROCE_ENDPOINT",
            "UNKNOWN",
            "ROCE_GID_EVIDENCE_MISSING",
            "Ethernet RDMA port exists but GID value/type/netdev evidence is incomplete",
        )
    elif not roce_ports and protocol_counts["ETHERNET_RDMA_UNCONFIRMED"]:
        roce_status = "FAIL" if expected_protocol == PROTOCOL_ROCE else "WARN"
        reason = (
            "ROCE_GID_NOT_CONFIGURED"
            if expected_protocol == PROTOCOL_ROCE
            else "ETHERNET_RDMA_PROTOCOL_UNCONFIRMED"
        )
        add_check(
            "ROCE_ENDPOINT",
            roce_status,
            reason,
            "Ethernet RDMA port exists but no valid mapped RoCE v1/v2 GID was found",
        )
    elif not roce_ports:
        roce_status = "NOT_ACTIVE_IN_CURRENT_PORT_MODE"
        checks.append(
            {
                "check_id": "ROCE_ENDPOINT",
                "status": "NOT_APPLICABLE",
                "message": "no port is currently configured in Ethernet/RoCE mode",
            }
        )
    elif roce_inactive or roce_mapped_ports != len(roce_ports) or roce_netdev_down or len(roce_mtu_values) > 1:
        roce_status = "FAIL" if (required or expected_protocol == PROTOCOL_ROCE) else "WARN"
        add_check(
            "ROCE_ENDPOINT",
            roce_status,
            "ROCE_ENDPOINT_NOT_READY",
            f"RoCE ports={len(roce_ports)}, active={roce_active}, mapped={roce_mapped_ports}, "
            f"netdev_up={roce_netdev_up}/{len(mapped_netdevs)}, mtus={sorted(roce_mtu_values)}",
        )
    elif roce_port_state_missing or roce_rate_missing or missing_netdevs or roce_netdev_unknown or roce_mtu_missing:
        roce_status = "UNKNOWN" if (required or expected_protocol == PROTOCOL_ROCE) else "WARN"
        add_check(
            "ROCE_ENDPOINT",
            roce_status,
            "ROCE_ENDPOINT_EVIDENCE_MISSING",
            f"state_missing={roce_port_state_missing}, rate_missing={roce_rate_missing}, "
            f"missing_netdevs={missing_netdevs}, "
            f"netdev_unknown={roce_netdev_unknown}, mtu_missing={roce_mtu_missing}",
        )
    else:
        roce_status = "PASS"
        add_check(
            "ROCE_ENDPOINT",
            "PASS",
            "ROCE_ENDPOINT",
            f"RoCE ports={len(roce_ports)}, versions={','.join(roce_versions)}, "
            f"mapped netdevs={','.join(sorted(mapped_netdevs))}, mtu={sorted(roce_mtu_values)}",
        )

    roce_counter_port_results = [
        _evaluate_roce_counter_port(device, port)
        for device, port, _classified in roce_ports
    ]
    if roce_counter_port_results:
        roce_counter_observed_status = max(
            (str(item.get("status") or "UNKNOWN") for item in roce_counter_port_results),
            key=_ROCE_COUNTER_STATUS_ORDER.__getitem__,
        )
    else:
        roce_counter_observed_status = "NOT_APPLICABLE"
    roce_counter_status_counts = {
        status: sum(item.get("status") == status for item in roce_counter_port_results)
        for status in ("PASS", "WARN", "UNKNOWN", "FAIL")
    }
    roce_counter_reason_codes = sorted(
        {
            str(reason)
            for item in roce_counter_port_results
            for reason in item.get("reason_codes", [])
        }
    )
    roce_counter_required = required or expected_protocol == PROTOCOL_ROCE
    if not roce_ports:
        roce_counter_status = "NOT_APPLICABLE"
        checks.append(
            {
                "check_id": "ROCE_COUNTER_HEALTH",
                "status": "NOT_APPLICABLE",
                "message": "no port is currently configured in RoCE mode",
            }
        )
    elif roce_counter_observed_status == "FAIL":
        roce_counter_status = "FAIL" if roce_counter_required else "WARN"
        add_check(
            "ROCE_COUNTER_HEALTH",
            roce_counter_status,
            "ROCE_COUNTER_ERROR_OR_DROP_GROWTH",
            f"RoCE counter window: ports={len(roce_ports)}, "
            f"fail={roce_counter_status_counts['FAIL']}, "
            f"unknown={roce_counter_status_counts['UNKNOWN']}, "
            f"warn={roce_counter_status_counts['WARN']}",
        )
    elif roce_counter_observed_status == "UNKNOWN":
        roce_counter_status = "UNKNOWN" if roce_counter_required else "WARN"
        add_check(
            "ROCE_COUNTER_HEALTH",
            roce_counter_status,
            "ROCE_COUNTER_HEALTH_EVIDENCE_MISSING",
            f"RoCE counter evidence incomplete: ports={len(roce_ports)}, "
            f"reasons={','.join(roce_counter_reason_codes)}",
        )
    elif roce_counter_observed_status == "WARN":
        roce_counter_status = "WARN"
        add_check(
            "ROCE_COUNTER_HEALTH",
            "WARN",
            "ROCE_CONGESTION_SIGNAL_GROWTH",
            f"RoCE error/drop counters are stable but PFC/CNP/congestion "
            f"signals grew on {roce_counter_status_counts['WARN']}/{len(roce_ports)} ports",
        )
    else:
        roce_counter_status = "PASS"
        add_check(
            "ROCE_COUNTER_HEALTH",
            "PASS",
            "ROCE_COUNTERS_STABLE",
            f"RoCE error/drop and available PFC/CNP counters are stable on "
            f"{len(roce_ports)} ports",
        )
    dcb_netdevs = mapped_netdevs or (
        roce_candidate_netdevs
        if protocol_counts["ETHERNET_RDMA_UNCONFIRMED"]
        or protocol_counts["ETHERNET_RDMA_EVIDENCE_INCOMPLETE"]
        else set()
    )
    dcb_profile = _roce_dcb_profile(dcb_netdevs, interfaces)
    dcb_status = dcb_profile["status"]
    if roce_ports:
        if dcb_status == "COLLECTED_POLICY_UNVALIDATED":
            dcb_check_status = "WARN"
            dcb_reason = "ROCE_DCB_POLICY_NOT_VALIDATED"
        else:
            dcb_check_status = "UNKNOWN" if (required or expected_protocol == PROTOCOL_ROCE) else "WARN"
            dcb_reason = "ROCE_DCB_CONFIGURATION_INCOMPLETE"
        add_check(
            "ROCE_DCB_CONFIGURATION",
            dcb_check_status,
            dcb_reason,
            f"RoCE host DCB configuration evidence={dcb_status}; switch side is not verified",
        )

    roce_configuration_health = evaluate_roce_health(network, rdma_policy)
    roce_configuration_relevant = bool(
        roce_ports
        or protocol_counts["ETHERNET_RDMA_UNCONFIRMED"]
        or protocol_counts["ETHERNET_RDMA_EVIDENCE_INCOMPLETE"]
        or expected_protocol == PROTOCOL_ROCE
        or rdma_policy is not None
    )
    if roce_configuration_relevant:
        checks.extend(
            {
                **item,
                "component": "ROCE_CONFIGURATION_CHAIN",
            }
            for item in roce_configuration_health["checks"]
        )
        detailed_status = roce_configuration_health["status"]
        detailed_required = (
            required or expected_protocol == PROTOCOL_ROCE or rdma_policy is not None
        )
        if detailed_status == "FAIL":
            exposed_status = "FAIL" if detailed_required else "WARN"
            reason_code = "ROCE_CONFIGURATION_POLICY_MISMATCH"
        elif detailed_status == "UNKNOWN":
            exposed_status = "UNKNOWN" if detailed_required else "WARN"
            reason_code = "ROCE_CONFIGURATION_EVIDENCE_MISSING"
        elif detailed_status == "UNVALIDATED":
            exposed_status = "WARN"
            reason_code = "ROCE_CONFIGURATION_POLICY_NOT_PROVIDED"
        else:
            exposed_status = "PASS"
            reason_code = "ROCE_CONFIGURATION_CHAIN_READY"
        add_check(
            "ROCE_CONFIGURATION_CHAIN",
            exposed_status,
            reason_code,
            f"RoCE configuration-chain status={detailed_status}; "
            f"policy_applied={roce_configuration_health['policy_applied']}",
        )
    ib_rates = sorted(
        {port.get("rate") for _, port, _ in ib_ports if port.get("rate")}
    )
    ib_active_mtus = sorted(
        {port.get("active_mtu") for _, port, _ in ib_ports if port.get("active_mtu")}
    )
    ib_max_mtus = sorted(
        {port.get("max_mtu") for _, port, _ in ib_ports if port.get("max_mtu")}
    )
    ib_port_profiles = sorted(
        [
            {
                # Device/port are rail identity, not enumeration noise.  Keep
                # them inside each profile before sorting so swapping two HCA
                # rails across nodes remains observable.
                "device": device,
                "port": str(port.get("port") or "UNKNOWN"),
                "subnet_prefix": _normalized_subnet_prefix(port.get("subnet_prefix")),
                "pkeys": sorted(_valid_pkeys(port)),
                "active_mtu": _mtu_value(port.get("active_mtu")),
                "max_mtu": _mtu_value(port.get("max_mtu")),
                "rate_mbps": _normalized_rate_mbps(port.get("rate")),
            }
            for device, port, _ in ib_ports
        ],
        key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True),
    )
    ib_pkeys = sorted(
        {pkey for _, port, _ in ib_ports for pkey in _valid_pkeys(port)}
    )
    ib_subnet_prefixes = sorted(
        {
            _normalized_subnet_prefix(port.get("subnet_prefix"))
            for _, port, _ in ib_ports
            if _normalized_subnet_prefix(port.get("subnet_prefix")) is not None
        }
    )
    configuration_bindings = [
        item
        for item in roce_configuration_health.get("bindings", [])
        if isinstance(item, dict)
    ]
    configuration_interfaces = roce_configuration_health.get("interfaces", {})
    configuration_interfaces = (
        configuration_interfaces if isinstance(configuration_interfaces, dict) else {}
    )
    roce_port_profiles: list[dict[str, Any]] = []
    for device, port, _classified in roce_ports:
        gid_layout: list[dict[str, Any]] = []
        for entry in _valid_gid_entries(port):
            netdev = str(entry.get("netdev") or "").strip()
            gid_type = str(entry.get("type") or "").lower()
            if not netdev or "roce" not in gid_type:
                continue
            matching_binding = next(
                (
                    item
                    for item in configuration_bindings
                    if str(item.get("rdma_device")) == device
                    and str(item.get("port")) == str(port.get("port") or "UNKNOWN")
                    and str(item.get("gid_index")) == str(entry.get("index"))
                    and str(item.get("netdev") or "") == netdev
                ),
                {},
            )
            interface_profile = configuration_interfaces.get(netdev, {})
            interface_profile = (
                interface_profile if isinstance(interface_profile, dict) else {}
            )
            addresses = interface_profile.get("addresses") or []
            ip_networks = sorted(
                {
                    str(address.get("network"))
                    for address in addresses
                    if isinstance(address, dict) and address.get("network")
                }
            )
            matched_ip = matching_binding.get("address_match")
            matched_ip_network = next(
                (
                    str(address.get("network"))
                    for address in addresses
                    if isinstance(address, dict)
                    and str(address.get("local")) == str(matched_ip)
                    and address.get("network")
                ),
                None,
            )
            vlan = interface_profile.get("vlan") or {}
            vlan = vlan if isinstance(vlan, dict) else {}
            parsed_gid = None
            try:
                parsed_gid = ipaddress.IPv6Address(str(entry.get("gid") or ""))
            except ipaddress.AddressValueError:
                pass
            gid_layout.append(
                {
                    "index": str(entry.get("index")),
                    "version": "v2" if "roce v2" in gid_type else "v1",
                    "gid_address_family": matching_binding.get("gid_address_family"),
                    # Preserve the rail's GID prefix without comparing the
                    # node-unique interface identifier/IPv4 host address.
                    "gid_subnet_prefix": (
                        f"0x{(int(parsed_gid) >> 64):016x}" if parsed_gid else None
                    ),
                    "netdev": netdev,
                    "ip_networks": ip_networks,
                    "ip_address_evidence_status": interface_profile.get(
                        "address_evidence_status"
                    ),
                    "gid_ip_network": matched_ip_network,
                    "gid_matches_netdev_ip": (
                        bool(matched_ip)
                        if matching_binding.get("version") == "v2"
                        else None
                    ),
                    "vlan_id": vlan.get("vlan_id"),
                    "vlan_protocol": vlan.get("vlan_protocol"),
                    "vlan_evidence_status": vlan.get("evidence_status"),
                    "mtu": _mtu_value(interface_profile.get("mtu")),
                }
            )
        roce_port_profiles.append(
            {
                "device": device,
                "port": str(port.get("port") or "UNKNOWN"),
                "rate_mbps": _normalized_rate_mbps(port.get("rate")),
                "gid_layout": sorted(
                    gid_layout,
                    key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True),
                ),
            }
        )
    roce_port_profiles = sorted(
        roce_port_profiles,
        key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True),
    )
    roce_dcb_policy_profiles = _dcb_policy_profiles(dcb_profile)
    (
        roce_configuration_rail_profiles,
        roce_configuration_rail_evidence_complete,
    ) = _roce_configuration_rail_profiles(
        dcb_profile,
        roce_configuration_health,
    )
    if current_protocol == "NATIVE_INFINIBAND" and ib_status == "PASS":
        fabric_profile = {
            "protocol": current_protocol,
            "ports": len(ib_ports),
            "port_profiles": ib_port_profiles,
        }
    elif (
        current_protocol == "ROCE"
        and roce_status == "PASS"
        and dcb_status == "COLLECTED_POLICY_UNVALIDATED"
        and roce_configuration_rail_evidence_complete
    ):
        fabric_profile = {
            "protocol": current_protocol,
            "ports": len(roce_ports),
            "port_profiles": roce_port_profiles,
            "dcb_policy_profiles": roce_dcb_policy_profiles,
            "configuration_rail_profiles": roce_configuration_rail_profiles,
        }
    else:
        fabric_profile = None

    summary = {
        "rdma_current_protocol": current_protocol,
        "rdma_protocol_status": protocol_status,
        "rdma_expected_protocol": expected_protocol,
        "rdma_hardware_protocol_capability": "UNKNOWN_NO_GENERIC_INTERFACE",
        "rdma_runtime_transport_verified": False,
        "rdma_userspace": rdma_userspace,
        "rdma_fabric_profile": fabric_profile,
        "rdma_protocol_profile": {
            "total_ports": len(ports),
            "ib_ports": protocol_counts["NATIVE_INFINIBAND"],
            "roce_ports": protocol_counts["ROCE"],
            "unconfirmed_ethernet_ports": protocol_counts["ETHERNET_RDMA_UNCONFIRMED"],
            "incomplete_ethernet_ports": protocol_counts["ETHERNET_RDMA_EVIDENCE_INCOMPLETE"],
            "unknown_ports": protocol_counts["UNKNOWN"],
        },
        "ib_endpoint": {
            "status": ib_status,
            "ports": len(ib_ports),
            "active_linkup_ports": ib_active,
            "valid_lid_ports": ib_valid_lid,
            "valid_sm_lid_ports": ib_valid_sm_lid,
            "valid_gid_ports": ib_valid_gid,
            "valid_pkey_ports": ib_valid_pkey,
            "rates": ib_rates,
            "active_mtus": ib_active_mtus,
            "max_mtus": ib_max_mtus,
            "pkeys": ib_pkeys,
            "subnet_prefixes": ib_subnet_prefixes,
            "port_profiles": ib_port_profiles,
        },
        "ib_counter_health": {
            "status": ib_counter_status,
            "observed_status": ib_counter_observed_status,
            "required": ib_counter_required,
            "ports": len(ib_counter_port_results),
            "status_counts": ib_counter_status_counts,
            "reason_codes": ib_counter_reason_codes,
            "sampling": network.get("rdma_counter_sampling"),
            "policy": ib_counter_policy,
            "port_results": ib_counter_port_results,
        },
        "roce_endpoint": {
            "status": roce_status,
            "ports": len(roce_ports),
            "active_linkup_ports": roce_active,
            "mapped_gid_ports": roce_mapped_ports,
            "versions": roce_versions,
            "netdevs": sorted(mapped_netdevs),
            "candidate_netdevs": sorted(roce_candidate_netdevs),
            "netdev_up": roce_netdev_up,
            "mtus": sorted(roce_mtu_values),
            "dcb_status": dcb_status,
            "dcb_profile": dcb_profile,
            "configuration_status": roce_configuration_health["status"],
        },
        "roce_counter_health": {
            "status": roce_counter_status,
            "observed_status": roce_counter_observed_status,
            "required": roce_counter_required,
            "ports": len(roce_counter_port_results),
            "status_counts": roce_counter_status_counts,
            "reason_codes": roce_counter_reason_codes,
            "sampling": network.get("rdma_counter_sampling"),
            "port_results": roce_counter_port_results,
        },
        "roce_configuration_health": roce_configuration_health,
    }
    return findings, checks, summary
