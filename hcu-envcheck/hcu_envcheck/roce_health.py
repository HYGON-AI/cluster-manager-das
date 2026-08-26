# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Pure RoCE configuration-chain evaluation.

The collector intentionally lives elsewhere.  This module consumes the
``network`` dictionary produced by :mod:`hcu_envcheck.pod_probe` (or an
equivalent JSON document), preserves the source evidence, and never turns a
collection failure into a configuration failure.

The top-level status has four values:

``PASS``
    All evidence needed by the supplied policy was collected and every
    expectation, plus the internal PFC/APP/ETS relationships, passed.
``FAIL``
    Complete evidence proves that the configuration is wrong.
``UNKNOWN``
    Required evidence is absent, unreadable, or a collection command failed.
``UNVALIDATED``
    Configuration was collected, but no expectation was supplied for at
    least one policy-dependent item.  This is not a health conclusion.
"""

from __future__ import annotations

import ipaddress
import re
from collections.abc import Mapping, Sequence
from typing import Any


PASS = "PASS"
FAIL = "FAIL"
UNKNOWN = "UNKNOWN"
UNVALIDATED = "UNVALIDATED"

_STATUS_ORDER = {PASS: 0, UNVALIDATED: 1, UNKNOWN: 2, FAIL: 3}
_POLICY_KEYS = {
    "protocol",
    "versions",
    "address_families",
    "allowed_prefixes",
    "vlan_ids",
    "minimum_mtu",
    "minimum_rate_mbps",
    "lossless_priorities",
    "dscp_to_priority",
    "app_mappings",
    "priority_to_tc",
    "dcbx_mode",
    "global_pause",
    "fec_mode",
}

_APP_SELECTOR_ALIASES = {
    "dscp": "dscp-prio",
    "dscp-prio": "dscp-prio",
    "pcp": "pcp-prio",
    "pcp-prio": "pcp-prio",
    "port": "port-prio",
    "port-prio": "port-prio",
    "stream-port": "stream-port-prio",
    "stream-port-prio": "stream-port-prio",
    "dgram-port": "dgram-port-prio",
    "dgram-port-prio": "dgram-port-prio",
    "ethertype": "ethtype-prio",
    "ethtype": "ethtype-prio",
    "ethtype-prio": "ethtype-prio",
}

_APP_SELECTOR_KEY_MAX = {
    "dscp-prio": 63,
    "pcp-prio": 7,
    "port-prio": 65535,
    "stream-port-prio": 65535,
    "dgram-port-prio": 65535,
    "ethtype-prio": 65535,
}


def _overall(statuses: Sequence[str]) -> str:
    return max(statuses or [PASS], key=lambda item: _STATUS_ORDER[item])


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(str(value).strip(), 0)
    except (TypeError, ValueError):
        return None


def _mtu(value: Any) -> int | None:
    match = re.search(r"\d+", str(value or ""))
    if not match:
        return None
    parsed = int(match.group(0))
    return parsed if parsed > 0 else None


def _rate_mbps(value: Any) -> int | float | None:
    text = str(value or "").strip()
    if re.fullmatch(r"\d+(?:\.\d+)?", text):
        parsed = float(text)
        return int(parsed) if parsed.is_integer() else parsed
    match = re.search(
        r"(\d+(?:\.\d+)?)\s*([kmgt])?\s*b(?:it)?(?:/s|/sec|ps)",
        text,
        flags=re.I,
    )
    if not match:
        return None
    multiplier = {None: 0.001, "k": 0.001, "m": 1, "g": 1000, "t": 1_000_000}
    parsed = float(match.group(1)) * multiplier[match.group(2).lower() if match.group(2) else None]
    return int(parsed) if parsed.is_integer() else parsed


def _state(value: Any) -> str:
    return str(value or "").split(":", 1)[-1].strip().upper().replace("_", "")


def _link_layer(value: Any) -> str:
    normalized = str(value or "").strip().upper().replace("_", "")
    return normalized if normalized in {"ETHERNET", "INFINIBAND"} else "UNKNOWN"


def _gid(value: Any) -> ipaddress.IPv6Address | None:
    try:
        parsed = ipaddress.IPv6Address(str(value or "").strip())
    except ipaddress.AddressValueError:
        return None
    return parsed if int(parsed) else None


def _gid_version(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    if "roce v2" in text:
        return "v2"
    if "roce v1" in text:
        return "v1"
    return None


def _normalize_family(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    aliases = {"inet": "ipv4", "4": "ipv4", "inet6": "ipv6", "6": "ipv6"}
    return aliases.get(normalized, normalized)


def _list_value(value: Any, name: str) -> list[Any]:
    if isinstance(value, str):
        return [value]
    if not isinstance(value, Sequence) or isinstance(value, (bytes, bytearray)):
        raise ValueError(f"policy.{name} must be a JSON array")
    return list(value)


def _priority_map(value: Any, name: str, *, key_max: int) -> dict[int, int]:
    if not isinstance(value, Mapping):
        raise ValueError(f"policy.{name} must be a JSON object")
    result: dict[int, int] = {}
    for raw_key, raw_priority in value.items():
        key = _as_int(raw_key)
        priority = _as_int(raw_priority)
        if key is None or not 0 <= key <= key_max or priority is None or not 0 <= priority <= 7:
            raise ValueError(f"policy.{name} contains an invalid mapping {raw_key!r}:{raw_priority!r}")
        result[key] = priority
    return result


def _app_selector_key(value: Any, selector: str) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    text = str(value).strip().lower()
    try:
        if selector == "ethtype-prio":
            # ``dcb app`` renders EtherTypes as hexadecimal, commonly without
            # the 0x prefix (for example 88cc).  Normalise both policy and
            # observed values to the numeric EtherType.
            parsed = int(text[2:], 16) if text.startswith("0x") else int(text, 16)
        else:
            parsed = int(text, 0)
    except ValueError:
        return None
    return parsed if 0 <= parsed <= _APP_SELECTOR_KEY_MAX[selector] else None


def _normalize_app_policy(value: Any) -> dict[str, dict[int, int]]:
    if not isinstance(value, Mapping):
        raise ValueError("policy.app_mappings must be a JSON object")
    result: dict[str, dict[int, int]] = {}
    for raw_selector, raw_mapping in value.items():
        selector = _APP_SELECTOR_ALIASES.get(str(raw_selector).strip().lower())
        if selector is None:
            raise ValueError(
                f"policy.app_mappings contains unknown selector {raw_selector!r}"
            )
        if selector in result:
            raise ValueError(
                f"policy.app_mappings contains duplicate selector {selector!r}"
            )
        if not isinstance(raw_mapping, Mapping):
            raise ValueError(
                f"policy.app_mappings.{raw_selector} must be a JSON object"
            )
        mapping: dict[int, int] = {}
        for raw_key, raw_priority in raw_mapping.items():
            key = _app_selector_key(raw_key, selector)
            priority = _as_int(raw_priority)
            if key is None or priority is None or not 0 <= priority <= 7:
                raise ValueError(
                    "policy.app_mappings contains an invalid mapping "
                    f"{raw_selector!r}:{raw_key!r}:{raw_priority!r}"
                )
            mapping[key] = priority
        result[selector] = mapping
    if not result:
        raise ValueError("policy.app_mappings cannot be empty")
    return result


def normalize_roce_policy(policy: Mapping[str, Any] | None) -> dict[str, Any]:
    """Validate and normalize an optional JSON-compatible policy dictionary."""

    if policy is None:
        return {}
    if not isinstance(policy, Mapping):
        raise ValueError("RoCE policy must be a JSON object")
    unknown = sorted(set(policy) - _POLICY_KEYS)
    if unknown:
        raise ValueError(f"unknown RoCE policy keys: {', '.join(unknown)}")
    result = dict(policy)

    if "protocol" in result:
        protocol = str(result["protocol"]).strip().lower().replace("_", "-")
        if protocol not in {"roce", "roce-v1", "roce-v2"}:
            raise ValueError("policy.protocol must be roce, roce-v1, or roce-v2")
        result["protocol"] = protocol
    if "versions" in result:
        versions = {str(item).strip().lower().removeprefix("roce-") for item in _list_value(result["versions"], "versions")}
        if not versions or not versions <= {"v1", "v2"}:
            raise ValueError("policy.versions must contain only v1 and/or v2")
        result["versions"] = sorted(versions)
    if "address_families" in result:
        families = {_normalize_family(item) for item in _list_value(result["address_families"], "address_families")}
        if not families or not families <= {"ipv4", "ipv6"}:
            raise ValueError("policy.address_families must contain ipv4 and/or ipv6")
        result["address_families"] = sorted(families)
    if "allowed_prefixes" in result:
        try:
            prefixes = [ipaddress.ip_network(str(item), strict=False) for item in _list_value(result["allowed_prefixes"], "allowed_prefixes")]
        except ValueError as exc:
            raise ValueError(f"policy.allowed_prefixes contains an invalid network: {exc}") from exc
        if not prefixes:
            raise ValueError("policy.allowed_prefixes cannot be empty")
        result["allowed_prefixes"] = prefixes
    if "vlan_ids" in result:
        vlan_ids = {_as_int(item) for item in _list_value(result["vlan_ids"], "vlan_ids")}
        if None in vlan_ids or any(not 0 <= item <= 4094 for item in vlan_ids if item is not None):
            raise ValueError("policy.vlan_ids must contain integers from 0 through 4094")
        result["vlan_ids"] = sorted(vlan_ids)
    for name in ("minimum_mtu", "minimum_rate_mbps"):
        if name in result:
            parsed = _as_int(result[name])
            if parsed is None or parsed <= 0:
                raise ValueError(f"policy.{name} must be a positive integer")
            result[name] = parsed
    if "lossless_priorities" in result:
        priorities = {_as_int(item) for item in _list_value(result["lossless_priorities"], "lossless_priorities")}
        if None in priorities or any(not 0 <= item <= 7 for item in priorities if item is not None):
            raise ValueError("policy.lossless_priorities must contain priorities 0 through 7")
        result["lossless_priorities"] = sorted(priorities)
    if "dscp_to_priority" in result:
        result["dscp_to_priority"] = _priority_map(result["dscp_to_priority"], "dscp_to_priority", key_max=63)
    if "app_mappings" in result:
        result["app_mappings"] = _normalize_app_policy(result["app_mappings"])
        if "dscp_to_priority" in result and (
            result["app_mappings"].get("dscp-prio", {})
            != result["dscp_to_priority"]
        ):
            raise ValueError(
                "policy.app_mappings dscp-prio must equal policy.dscp_to_priority when both are provided"
            )
    if "priority_to_tc" in result:
        result["priority_to_tc"] = _priority_map(result["priority_to_tc"], "priority_to_tc", key_max=7)
    if "dcbx_mode" in result:
        modes = {str(item).strip().lower() for item in _list_value(result["dcbx_mode"], "dcbx_mode")}
        if not modes:
            raise ValueError("policy.dcbx_mode cannot be empty")
        result["dcbx_mode"] = sorted(modes)
    if "global_pause" in result:
        value = result["global_pause"]
        if isinstance(value, bool):
            result["global_pause"] = {"rx": value, "tx": value}
        elif isinstance(value, str) and value.strip().lower() in {"on", "off"}:
            enabled = value.strip().lower() == "on"
            result["global_pause"] = {"rx": enabled, "tx": enabled}
        elif isinstance(value, Mapping) and set(value) <= {"rx", "tx"} and set(value) == {"rx", "tx"}:
            if not all(isinstance(item, bool) for item in value.values()):
                raise ValueError("policy.global_pause rx/tx values must be booleans")
            result["global_pause"] = {"rx": value["rx"], "tx": value["tx"]}
        else:
            raise ValueError("policy.global_pause must be on/off, a boolean, or {rx,tx}")
    if "fec_mode" in result:
        modes = {str(item).strip().lower() for item in _list_value(result["fec_mode"], "fec_mode")}
        if not modes:
            raise ValueError("policy.fec_mode cannot be empty")
        result["fec_mode"] = sorted(modes)
    return result


def _command_evidence(command: Any) -> tuple[str, str]:
    if not isinstance(command, Mapping):
        return "MISSING", ""
    if command.get("rc") != 0:
        return "COMMAND_FAILED", str(command.get("stdout") or "")
    output = str(command.get("stdout") or "").strip()
    if any(
        bool(command.get(field))
        for field in (
            "output_truncated",
            "stdout_truncated",
            "stderr_truncated",
            "truncated",
        )
    ):
        return "TRUNCATED", output
    return ("COMPLETE", output) if output else ("EMPTY", "")


def _parse_pairs(
    text: str,
    label: str,
    *,
    key_max: int = 63,
) -> tuple[dict[int, int], bool, bool]:
    """Parse a complete, unique numeric priority table.

    Policy normally names only priorities relevant to training traffic, but
    command evidence must still authoritatively cover every key. Missing,
    out-of-range, or duplicate entries make the table incomplete even when
    the visible policy subset happens to match.
    """

    result: dict[int, int] = {}
    seen = False
    invalid = False
    pair_count = 0
    normalized_label = label.lower()
    for line in text.splitlines():
        lowered = line.lower()
        if normalized_label not in lowered:
            continue
        seen = True
        tail = lowered.split(normalized_label, 1)[1]
        raw_pairs = re.findall(r"\b(\d+)\s*:\s*(\d+)\b", tail)
        if not raw_pairs:
            invalid = True
        for raw_key, raw_value in raw_pairs:
            pair_count += 1
            key, value = int(raw_key), int(raw_value)
            if not (0 <= key <= key_max and 0 <= value <= 7):
                invalid = True
                continue
            if key in result:
                # Repeated identical values are not unique; conflicting
                # values are necessarily ambiguous as well.
                invalid = True
                continue
            result[key] = value
    expected_keys = set(range(key_max + 1))
    complete = (
        seen
        and not invalid
        and pair_count == len(expected_keys)
        and set(result) == expected_keys
    )
    return result, seen, complete

def _parse_app_mappings(text: str) -> tuple[dict[str, dict[int, int]], list[str]]:
    """Parse all selectors emitted by ``dcb app show``.

    Returning unparsed selector fragments is deliberate: exact-policy mode
    must never silently treat a new vendor/kernel selector as absent.
    """

    result: dict[str, dict[int, int]] = {}
    unparsed: list[str] = []
    selector_re = re.compile(r"\b([a-z][a-z-]*-prio)\b", flags=re.I)
    for line in text.splitlines():
        matches = list(selector_re.finditer(line))
        for index, match in enumerate(matches):
            raw_selector = match.group(1).lower()
            selector = _APP_SELECTOR_ALIASES.get(raw_selector)
            end = matches[index + 1].start() if index + 1 < len(matches) else len(line)
            fragment = line[match.end():end]
            if selector is None:
                unparsed.append(f"{raw_selector}{fragment}".strip())
                continue
            parsed_any = False
            for raw_key, raw_priority in re.findall(
                r"\b((?:0x)?[0-9a-f]+)\s*:\s*(\d+)\b", fragment, flags=re.I
            ):
                key = _app_selector_key(raw_key, selector)
                priority = _as_int(raw_priority)
                if key is None or priority is None or not 0 <= priority <= 7:
                    unparsed.append(f"{raw_selector} {raw_key}:{raw_priority}")
                    continue
                selector_mapping = result.setdefault(selector, {})
                if key in selector_mapping and selector_mapping[key] != priority:
                    unparsed.append(
                        f"{raw_selector} conflicting {raw_key}:{selector_mapping[key]}/{priority}"
                    )
                    parsed_any = True
                    continue
                selector_mapping[key] = priority
                parsed_any = True
            if not parsed_any:
                unparsed.append(f"{raw_selector}{fragment}".strip())
    return result, sorted(set(unparsed))


def _parse_pfc(text: str) -> tuple[list[int], bool, bool]:
    """Parse PFC only when priorities 0..7 occur exactly once."""

    states: dict[int, str] = {}
    seen = False
    invalid = False
    pair_count = 0
    for line in text.splitlines():
        lowered = line.lower()
        if "prio-pfc" not in lowered:
            continue
        seen = True
        tail = lowered.split("prio-pfc", 1)[1]
        raw_pairs = re.findall(r"\b(\d+)\s*:\s*([a-z]+)\b", tail)
        if not raw_pairs:
            invalid = True
        for raw_priority, raw_state in raw_pairs:
            pair_count += 1
            priority = int(raw_priority)
            state = raw_state.lower()
            if priority not in range(8) or state not in {"on", "off"}:
                invalid = True
                continue
            if priority in states:
                invalid = True
                continue
            states[priority] = state
    complete = (
        seen
        and not invalid
        and pair_count == 8
        and set(states) == set(range(8))
    )
    enabled = sorted(
        priority for priority, state in states.items() if state == "on"
    )
    return enabled, seen, complete

def _parse_dcbx(text: str) -> str | None:
    normalized = " ".join(text.lower().split())
    if not normalized:
        return None
    match = re.search(r"(?:mode\s+|dcbx\s+)(host|ieee|cee|static|firmware)\b", normalized)
    if match:
        return match.group(1)
    for mode in ("host", "ieee", "cee", "static", "firmware"):
        if re.fullmatch(rf".*\b{mode}\b.*", normalized):
            return mode
    return None


def _parse_pause(text: str) -> dict[str, bool] | None:
    values: dict[str, bool] = {}
    for label, state in re.findall(r"^\s*(RX|TX)\s*:\s*(on|off)\s*$", text, flags=re.I | re.M):
        values[label.lower()] = state.lower() == "on"
    return values if set(values) == {"rx", "tx"} else None


def _parse_fec(text: str) -> dict[str, Any] | None:
    active = re.search(r"^\s*Active FEC encoding\s*:\s*(.+?)\s*$", text, flags=re.I | re.M)
    configured = re.search(r"^\s*Configured FEC encodings?\s*:\s*(.+?)\s*$", text, flags=re.I | re.M)
    if not active and not configured:
        return None
    return {
        "active": active.group(1).strip().lower() if active else None,
        "configured": configured.group(1).strip().lower() if configured else None,
    }


def _interface_vlan(interface: Mapping[str, Any]) -> dict[str, Any]:
    configuration = interface.get("roce_configuration")
    configuration = configuration if isinstance(configuration, Mapping) else {}
    protocol = interface.get("vlan_protocol") or configuration.get("vlan_protocol")
    kind = interface.get("vlan_kind") or configuration.get("vlan_kind")
    candidates = (
        (interface.get("vlan_id"), "interface.vlan_id"),
        (configuration.get("vlan_id"), "roce_configuration.vlan_id"),
        ((interface.get("vlan") or {}).get("id") if isinstance(interface.get("vlan"), Mapping) else None, "interface.vlan.id"),
    )
    for value, source in candidates:
        parsed = _as_int(value)
        if parsed is not None and 0 <= parsed <= 4094:
            return {
                "vlan_id": parsed,
                "vlan_protocol": protocol,
                "vlan_kind": kind,
                "source": source,
                "evidence_status": "COMPLETE",
            }
    status = str(configuration.get("vlan_collection_status") or interface.get("vlan_collection_status") or "NOT_COLLECTED").upper()
    return {
        "vlan_id": None,
        "vlan_protocol": protocol,
        "vlan_kind": kind,
        "source": None,
        "evidence_status": status,
    }


def _topology(name: str, interfaces: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    paths: list[list[str]] = []
    cycles: list[list[str]] = []
    missing: set[str] = set()

    def walk(current: str, path: list[str]) -> None:
        if current in path:
            cycle = path[path.index(current):] + [current]
            cycles.append(cycle)
            paths.append(path + [current])
            return
        interface = interfaces.get(current)
        if interface is None:
            missing.add(current)
            paths.append(path + [current])
            return
        children = sorted(
            {
                str(item)
                for field in ("bond_slaves", "lower_interfaces")
                for item in (interface.get(field) or [])
                if item
            }
        )
        if not children:
            paths.append(path + [current])
            return
        for child in children:
            walk(child, path + [current])

    walk(name, [])
    leaves = sorted({path[-1] for path in paths if path})
    source_configuration = (interfaces.get(name) or {}).get("roce_configuration") or {}
    collected_topology = (
        source_configuration.get("topology")
        if isinstance(source_configuration, Mapping)
        and isinstance(source_configuration.get("topology"), Mapping)
        else {}
    )
    collected_leaf_evidence = collected_topology.get("leaf_evidence") or {}
    leaf_evidence: dict[str, Any] = {}
    for leaf in leaves:
        interface = interfaces.get(leaf) or {}
        collected = (
            collected_leaf_evidence.get(leaf)
            if isinstance(collected_leaf_evidence, Mapping)
            and isinstance(collected_leaf_evidence.get(leaf), Mapping)
            else {}
        )
        leaf_evidence[leaf] = {
            "local_link_status": interface.get("local_link_status") or collected.get("local_link_status") or "UNKNOWN",
            "mtu": interface.get("mtu") or collected.get("mtu"),
            "speed_mbps": interface.get("speed_mbps") or collected.get("speed_mbps"),
            "bond_slave": interface.get("bond_slave") or collected.get("bond_slave"),
        }
    return {
        "kind": "BOND_OR_LAYERED" if any(len(path) > 1 for path in paths) else "DIRECT",
        "paths": paths,
        "leaf_interfaces": leaves,
        "missing_interfaces": sorted(missing),
        "cycles": cycles,
        "leaf_states": {
            leaf: leaf_evidence[leaf]["local_link_status"] for leaf in leaves
        },
        "leaf_mtus": {leaf: leaf_evidence[leaf]["mtu"] for leaf in leaves},
        "leaf_rates_mbps": {
            leaf: leaf_evidence[leaf]["speed_mbps"] for leaf in leaves
        },
        "leaf_evidence": leaf_evidence,
        "collection_status": collected_topology.get("status") or "DERIVED_FROM_INTERFACE_INVENTORY",
        "leaf_evidence_collected": bool(collected_topology),
        "collected_paths": collected_topology.get("paths") or [],
    }


def _address_records(interface: Mapping[str, Any]) -> tuple[list[dict[str, Any]], str]:
    configuration = interface.get("roce_configuration")
    configuration = configuration if isinstance(configuration, Mapping) else {}
    raw = configuration.get("ip_addresses")
    status = str(configuration.get("ip_address_collection_status") or ("COMPLETE" if isinstance(raw, list) and raw else "NOT_RECORDED")).upper()
    records: list[dict[str, Any]] = []
    for item in raw or []:
        if not isinstance(item, Mapping):
            continue
        try:
            address = ipaddress.ip_address(str(item.get("local") or ""))
        except ValueError:
            continue
        family = "ipv4" if address.version == 4 else "ipv6"
        prefixlen = _as_int(item.get("prefixlen"))
        network = None
        if prefixlen is not None:
            try:
                network = str(ipaddress.ip_network(f"{address}/{prefixlen}", strict=False))
            except ValueError:
                network = None
        records.append(
            {
                "family": family,
                "local": str(address),
                "prefixlen": prefixlen,
                "network": network,
                "scope": item.get("scope"),
            }
        )
    return records, status


def evaluate_roce_health(
    network: Mapping[str, Any],
    policy: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate one node's static RoCE configuration chain.

    The function is deterministic and performs no I/O, making it suitable for
    the bare-metal, Docker and Kubernetes collectors alike.  ``policy`` is a
    JSON-compatible dictionary; see :func:`normalize_roce_policy` for accepted
    keys.
    """

    expected = normalize_roce_policy(policy)
    checks: list[dict[str, Any]] = []

    def add(check_id: str, status: str, reason_code: str, message: str, evidence: Any = None) -> None:
        item = {"check_id": check_id, "status": status, "reason_code": reason_code, "message": message}
        if evidence is not None:
            item["evidence"] = evidence
        checks.append(item)

    interfaces = {
        str(item.get("name")): item
        for item in network.get("interfaces", [])
        if isinstance(item, Mapping) and item.get("name")
    }
    bindings: list[dict[str, Any]] = []
    ethernet_ports = 0
    gid_evidence_incomplete = False
    complete_ports_without_binding = 0
    for device in network.get("rdma_devices", []):
        if not isinstance(device, Mapping):
            continue
        for port in device.get("ports", []):
            if not isinstance(port, Mapping) or _link_layer(port.get("link_layer")) != "ETHERNET":
                continue
            ethernet_ports += 1
            raw_gids = port.get("gids")
            default_status = "COMPLETE" if isinstance(raw_gids, list) else "UNAVAILABLE"
            statuses = [
                str(port.get(name) or default_status).upper()
                for name in ("gid_collection_status", "gid_type_collection_status", "gid_ndev_collection_status")
            ]
            complete = all(item == "COMPLETE" for item in statuses)
            gid_evidence_incomplete = gid_evidence_incomplete or not complete
            port_bindings = 0
            for entry in raw_gids or []:
                if not isinstance(entry, Mapping):
                    continue
                parsed_gid = _gid(entry.get("gid"))
                version = _gid_version(entry.get("type"))
                if parsed_gid is None or version is None:
                    continue
                netdev = str(entry.get("netdev") or "").strip() or None
                binding = {
                    "rdma_device": str(device.get("name") or "UNKNOWN"),
                    "port": str(port.get("port") or "UNKNOWN"),
                    "gid_index": entry.get("index"),
                    "gid": str(parsed_gid),
                    "gid_address_family": "ipv4" if parsed_gid.ipv4_mapped else "ipv6",
                    "version": version,
                    "netdev": netdev,
                    "port_active_linkup": _state(port.get("state")) == "ACTIVE" and _state(port.get("phys_state")) == "LINKUP",
                    "port_rate_mbps": _rate_mbps(port.get("rate")),
                    "gid_evidence_status": "COMPLETE" if complete else "INCOMPLETE",
                }
                binding["topology"] = _topology(netdev, interfaces) if netdev else None
                bindings.append(binding)
                port_bindings += 1
            if complete and not port_bindings:
                complete_ports_without_binding += 1

    if not ethernet_ports:
        add("ROCE_PROTOCOL", FAIL if expected.get("protocol", "").startswith("roce") else UNVALIDATED, "ROCE_ETHERNET_PORT_NOT_FOUND", "no Ethernet-link-layer RDMA port was found")
    elif gid_evidence_incomplete:
        add("ROCE_PROTOCOL", UNKNOWN, "ROCE_GID_EVIDENCE_MISSING", "GID value/type/netdev evidence is incomplete")
    elif complete_ports_without_binding or not bindings:
        add("ROCE_PROTOCOL", FAIL, "ROCE_GID_NOT_CONFIGURED", "complete GID evidence contains no mapped RoCE v1/v2 endpoint")
    else:
        add("ROCE_PROTOCOL", PASS, "ROCE_PROTOCOL_CONFIRMED", f"confirmed {len(bindings)} RoCE GID binding(s)")

    binding_evidence_unknown = gid_evidence_incomplete and not bindings
    binding_absent_definitive = bool(
        ethernet_ports and not bindings and not gid_evidence_incomplete
    )

    if "protocol" not in expected:
        add("ROCE_POLICY_PROTOCOL", UNVALIDATED, "ROCE_PROTOCOL_POLICY_NOT_PROVIDED", "protocol expectation was not provided")
    elif binding_evidence_unknown:
        add("ROCE_POLICY_PROTOCOL", UNKNOWN, "ROCE_GID_EVIDENCE_MISSING", "protocol policy cannot be evaluated because GID evidence is incomplete")
    else:
        required_version = {"roce-v1": "v1", "roce-v2": "v2"}.get(expected["protocol"])
        configured = {item["version"] for item in bindings}
        if required_version and required_version not in configured:
            add("ROCE_POLICY_PROTOCOL", FAIL, "ROCE_PROTOCOL_VERSION_MISMATCH", f"expected {required_version}, configured={sorted(configured)}")
        else:
            add("ROCE_POLICY_PROTOCOL", PASS, "ROCE_PROTOCOL_POLICY_MATCH", f"expected={expected['protocol']}")

    configured_versions = sorted({item["version"] for item in bindings})
    if "versions" not in expected:
        add("ROCE_VERSIONS", UNVALIDATED, "ROCE_VERSION_POLICY_NOT_PROVIDED", f"configured versions={configured_versions}")
    elif not bindings and gid_evidence_incomplete:
        add("ROCE_VERSIONS", UNKNOWN, "ROCE_GID_EVIDENCE_MISSING", "RoCE versions cannot be verified")
    else:
        unexpected = sorted(set(configured_versions) - set(expected["versions"]))
        if unexpected or not set(configured_versions) & set(expected["versions"]):
            add("ROCE_VERSIONS", FAIL, "ROCE_VERSION_POLICY_MISMATCH", f"allowed={expected['versions']}, configured={configured_versions}")
        else:
            add("ROCE_VERSIONS", PASS, "ROCE_VERSION_POLICY_MATCH", f"allowed={expected['versions']}, configured={configured_versions}")

    mapped_netdevs = sorted({item["netdev"] for item in bindings if item.get("netdev")})
    missing_netdev_bindings = [item for item in bindings if not item.get("netdev")]
    if missing_netdev_bindings:
        status = FAIL if all(item["gid_evidence_status"] == "COMPLETE" for item in missing_netdev_bindings) else UNKNOWN
        add("ROCE_GID_NETDEV_BINDINGS", status, "ROCE_GID_NETDEV_BINDING_MISSING", "one or more RoCE GIDs have no netdev binding")
    elif any(name not in interfaces for name in mapped_netdevs):
        add("ROCE_GID_NETDEV_BINDINGS", UNKNOWN, "ROCE_NETDEV_INVENTORY_MISSING", "a mapped netdev is absent from interface inventory")
    elif bindings:
        add("ROCE_GID_NETDEV_BINDINGS", PASS, "ROCE_GID_NETDEV_BINDINGS_VALID", f"mapped netdevs={mapped_netdevs}")

    interface_results: dict[str, Any] = {}
    all_addresses: list[dict[str, Any]] = []
    for name in mapped_netdevs:
        interface = interfaces.get(name)
        if interface is None:
            continue
        addresses, address_status = _address_records(interface)
        all_addresses.extend({**item, "netdev": name} for item in addresses)
        topology = _topology(name, interfaces)
        vlan = _interface_vlan(interface)
        mtu = _mtu(interface.get("mtu"))
        binding_rates = [item["port_rate_mbps"] for item in bindings if item.get("netdev") == name and item.get("port_rate_mbps") is not None]
        rate = _rate_mbps(interface.get("speed_mbps")) or (min(binding_rates) if binding_rates else None)
        interface_results[name] = {
            "local_link_status": interface.get("local_link_status", "UNKNOWN"),
            "mtu": mtu,
            "rate_mbps": rate,
            "addresses": addresses,
            "address_evidence_status": address_status,
            "vlan": vlan,
            "topology": topology,
        }

        link_state = str(interface.get("local_link_status") or "UNKNOWN").upper()
        if link_state == "DOWN":
            add(f"ROCE_LINK:{name}", FAIL, "ROCE_NETDEV_LINK_DOWN", f"{name} is down")
        elif link_state != "UP":
            add(f"ROCE_LINK:{name}", UNKNOWN, "ROCE_NETDEV_LINK_EVIDENCE_MISSING", f"{name} link state is unknown")
        else:
            add(f"ROCE_LINK:{name}", PASS, "ROCE_NETDEV_LINK_UP", f"{name} is up")
        leaf_states = {leaf: str(state).upper() for leaf, state in topology["leaf_states"].items()}
        if topology["cycles"]:
            add(f"ROCE_LEAVES:{name}", FAIL, "ROCE_INTERFACE_TOPOLOGY_CYCLE", f"interface topology cycles={topology['cycles']}")
        elif topology["missing_interfaces"] or any(state not in {"UP", "DOWN"} for state in leaf_states.values()):
            add(f"ROCE_LEAVES:{name}", UNKNOWN, "ROCE_LEAF_LINK_EVIDENCE_MISSING", f"leaf states={leaf_states}, missing={topology['missing_interfaces']}")
        elif any(state == "DOWN" for state in leaf_states.values()):
            add(f"ROCE_LEAVES:{name}", FAIL, "ROCE_LEAF_LINK_DOWN", f"leaf states={leaf_states}")
        else:
            add(f"ROCE_LEAVES:{name}", PASS, "ROCE_LEAF_LINKS_UP", f"leaf states={leaf_states}")
        if topology["leaf_evidence_collected"]:
            leaf_mtus = {
                leaf: _mtu(value) for leaf, value in topology["leaf_mtus"].items()
            }
            if not leaf_mtus or any(value is None for value in leaf_mtus.values()):
                add(f"ROCE_LEAF_MTU:{name}", UNKNOWN, "ROCE_LEAF_MTU_EVIDENCE_MISSING", f"leaf MTUs={leaf_mtus}")
            elif "minimum_mtu" not in expected:
                add(f"ROCE_LEAF_MTU:{name}", UNVALIDATED, "ROCE_MINIMUM_MTU_NOT_PROVIDED", f"leaf MTUs={leaf_mtus}")
            elif any(value < expected["minimum_mtu"] for value in leaf_mtus.values() if value is not None):
                add(
                    f"ROCE_LEAF_MTU:{name}", FAIL, "ROCE_LEAF_MTU_BELOW_POLICY",
                    f"leaf MTUs={leaf_mtus}, minimum={expected['minimum_mtu']}",
                )
            else:
                add(
                    f"ROCE_LEAF_MTU:{name}", PASS, "ROCE_LEAF_MTU_POLICY_MATCH",
                    f"leaf MTUs={leaf_mtus}",
                )

        if mtu is None:
            add(f"ROCE_MTU:{name}", UNKNOWN, "ROCE_MTU_EVIDENCE_MISSING", f"{name} MTU is unavailable")
        elif "minimum_mtu" not in expected:
            add(f"ROCE_MTU:{name}", UNVALIDATED, "ROCE_MINIMUM_MTU_NOT_PROVIDED", f"{name} MTU={mtu}")
        elif mtu < expected["minimum_mtu"]:
            add(f"ROCE_MTU:{name}", FAIL, "ROCE_MTU_BELOW_POLICY", f"{name} MTU={mtu}, minimum={expected['minimum_mtu']}")
        else:
            add(f"ROCE_MTU:{name}", PASS, "ROCE_MTU_POLICY_MATCH", f"{name} MTU={mtu}")
        if rate is None:
            add(f"ROCE_RATE:{name}", UNKNOWN, "ROCE_RATE_EVIDENCE_MISSING", f"{name} rate is unavailable")
        elif "minimum_rate_mbps" not in expected:
            add(f"ROCE_RATE:{name}", UNVALIDATED, "ROCE_MINIMUM_RATE_NOT_PROVIDED", f"{name} rate={rate} Mbps")
        elif rate < expected["minimum_rate_mbps"]:
            add(f"ROCE_RATE:{name}", FAIL, "ROCE_RATE_BELOW_POLICY", f"{name} rate={rate}, minimum={expected['minimum_rate_mbps']} Mbps")
        else:
            add(f"ROCE_RATE:{name}", PASS, "ROCE_RATE_POLICY_MATCH", f"{name} rate={rate} Mbps")

    address_evidence_missing = any(item["address_evidence_status"] not in {"COMPLETE"} for item in interface_results.values())
    if not mapped_netdevs and binding_evidence_unknown:
        add("ROCE_ADDRESSES", UNKNOWN, "ROCE_ADDRESS_BINDING_EVIDENCE_MISSING", "addresses cannot be associated until the GID-to-netdev binding is known")
    elif not mapped_netdevs and binding_absent_definitive:
        add("ROCE_ADDRESSES", FAIL, "ROCE_ADDRESS_BINDING_NOT_CONFIGURED", "no configured RoCE binding is available for address validation")
    elif not mapped_netdevs and not ethernet_ports:
        add("ROCE_ADDRESSES", FAIL if expected else UNVALIDATED, "ROCE_ADDRESS_NOT_APPLICABLE", "no Ethernet-link-layer RDMA port is present")
    elif not all_addresses and address_evidence_missing:
        add("ROCE_ADDRESSES", UNKNOWN, "ROCE_ADDRESS_EVIDENCE_MISSING", "netdev address evidence is absent or collection completeness is unknown")
    elif not all_addresses:
        add("ROCE_ADDRESSES", FAIL, "ROCE_ADDRESS_NOT_CONFIGURED", "complete evidence contains no IP address on mapped RoCE netdevs")
    else:
        add("ROCE_ADDRESSES", PASS, "ROCE_ADDRESS_PRESENT", f"collected {len(all_addresses)} address(es)")

    for binding in bindings:
        binding_id = f"{binding['rdma_device']}:{binding['port']}:{binding['gid_index']}"
        if binding["version"] == "v1":
            add(f"ROCE_GID_ADDRESS_BINDING:{binding_id}", PASS, "ROCE_V1_LAYER2_BINDING", "RoCE v1 GID does not require an IP-address equality check")
            binding["address_match"] = "NOT_APPLICABLE_ROCE_V1"
            continue
        interface_result = interface_results.get(binding.get("netdev"))
        if interface_result is None:
            add(f"ROCE_GID_ADDRESS_BINDING:{binding_id}", UNKNOWN, "ROCE_GID_ADDRESS_EVIDENCE_MISSING", "mapped netdev address evidence is unavailable")
            binding["address_match"] = None
            continue
        addresses = interface_result["addresses"]
        address_status = interface_result["address_evidence_status"]
        if not addresses and address_status != "COMPLETE":
            add(f"ROCE_GID_ADDRESS_BINDING:{binding_id}", UNKNOWN, "ROCE_GID_ADDRESS_EVIDENCE_MISSING", "mapped netdev address collection is incomplete")
            binding["address_match"] = None
            continue
        parsed_gid = ipaddress.IPv6Address(binding["gid"])
        gid_address = str(parsed_gid.ipv4_mapped or parsed_gid)
        matches = [item["local"] for item in addresses if item["local"] == gid_address]
        binding["address_match"] = matches[0] if matches else None
        add(
            f"ROCE_GID_ADDRESS_BINDING:{binding_id}",
            PASS if matches else FAIL,
            "ROCE_GID_ADDRESS_BINDING_MATCH" if matches else "ROCE_GID_ADDRESS_BINDING_MISMATCH",
            f"GID-derived address={gid_address}, netdev addresses={[item['local'] for item in addresses]}",
        )
    configured_families = sorted({item["family"] for item in all_addresses})
    if "address_families" not in expected:
        add("ROCE_ADDRESS_FAMILIES", UNVALIDATED, "ROCE_ADDRESS_FAMILY_POLICY_NOT_PROVIDED", f"configured={configured_families}")
    elif (address_evidence_missing or binding_evidence_unknown) and not all_addresses:
        add("ROCE_ADDRESS_FAMILIES", UNKNOWN, "ROCE_ADDRESS_EVIDENCE_MISSING", "address families cannot be verified")
    else:
        missing_families = sorted(set(expected["address_families"]) - set(configured_families))
        add("ROCE_ADDRESS_FAMILIES", FAIL if missing_families else PASS, "ROCE_ADDRESS_FAMILY_MISSING" if missing_families else "ROCE_ADDRESS_FAMILY_POLICY_MATCH", f"required={expected['address_families']}, configured={configured_families}")

    if "allowed_prefixes" not in expected:
        add("ROCE_ADDRESS_PREFIXES", UNVALIDATED, "ROCE_PREFIX_POLICY_NOT_PROVIDED", "allowed prefixes were not provided")
    elif (address_evidence_missing or binding_evidence_unknown) and not all_addresses:
        add("ROCE_ADDRESS_PREFIXES", UNKNOWN, "ROCE_ADDRESS_EVIDENCE_MISSING", "address prefixes cannot be verified")
    else:
        unexpected = [
            item
            for item in all_addresses
            if not any(
                ipaddress.ip_address(item["local"]) in prefix
                for prefix in expected["allowed_prefixes"]
            )
        ]
        prefix_match = bool(all_addresses) and not unexpected
        add(
            "ROCE_ADDRESS_PREFIXES",
            PASS if prefix_match else FAIL,
            "ROCE_ADDRESS_PREFIX_POLICY_MATCH" if prefix_match else "ROCE_ADDRESS_PREFIX_MISMATCH",
            f"allowed={[str(item) for item in expected['allowed_prefixes']]}, configured={[item['local'] for item in all_addresses]}, unexpected={[item['local'] for item in unexpected]}",
        )
    vlan_records = {name: item["vlan"] for name, item in interface_results.items()}
    if "vlan_ids" not in expected:
        add("ROCE_VLAN", UNVALIDATED, "ROCE_VLAN_POLICY_NOT_PROVIDED", "VLAN expectation was not provided", vlan_records)
    elif not vlan_records and binding_evidence_unknown:
        add("ROCE_VLAN", UNKNOWN, "ROCE_VLAN_BINDING_EVIDENCE_MISSING", "VLAN cannot be associated until the GID-to-netdev binding is known")
    elif not vlan_records:
        add("ROCE_VLAN", FAIL, "ROCE_VLAN_BINDING_NOT_CONFIGURED", "no configured RoCE binding is available for VLAN validation")
    elif any(item["vlan_id"] is None and item["evidence_status"] != "COMPLETE" for item in vlan_records.values()):
        add("ROCE_VLAN", UNKNOWN, "ROCE_VLAN_EVIDENCE_MISSING", "VLAN identity was not collected authoritatively", vlan_records)
    else:
        configured_vlans = {item["vlan_id"] if item["vlan_id"] is not None else 0 for item in vlan_records.values()}
        unexpected = sorted(configured_vlans - set(expected["vlan_ids"]))
        add("ROCE_VLAN", FAIL if unexpected else PASS, "ROCE_VLAN_POLICY_MISMATCH" if unexpected else "ROCE_VLAN_POLICY_MATCH", f"allowed={expected['vlan_ids']}, configured={sorted(configured_vlans)}")

    dcb_targets: dict[str, Any] = {}
    dcb_target_names_by_source: dict[str, set[str]] = {}
    for name in mapped_netdevs:
        configuration = (interfaces.get(name) or {}).get("roce_configuration") or {}
        commands_by_target = configuration.get("dcb_targets") if isinstance(configuration, Mapping) else None
        if not isinstance(commands_by_target, Mapping) or not commands_by_target:
            legacy = configuration.get("dcb_commands") if isinstance(configuration, Mapping) else None
            commands_by_target = {name: legacy} if isinstance(legacy, Mapping) else {}
        dcb_target_names_by_source[name] = {str(item) for item in commands_by_target}
        for target, commands in commands_by_target.items():
            commands = commands if isinstance(commands, Mapping) else {}
            sections = {
                section: _command_evidence(commands.get(section))
                for section in ("pfc", "ets", "app", "buffer", "dcbx")
            }
            pfc, pfc_seen, pfc_table_complete = _parse_pfc(sections["pfc"][1])
            app_mappings, app_unparsed = _parse_app_mappings(sections["app"][1])
            dscp = app_mappings.get("dscp-prio", {})
            (
                priority_tc,
                ets_seen,
                ets_table_complete,
            ) = _parse_pairs(sections["ets"][1], "prio-tc", key_max=7)
            dcbx_mode = _parse_dcbx(sections["dcbx"][1])
            dcb_targets[str(target)] = {
                "source_netdev": name,
                "section_evidence": {
                    key: {"status": value[0], "stdout": value[1]}
                    for key, value in sections.items()
                },
                "pfc_enabled_priorities": pfc,
                "pfc_table_present": pfc_seen,
                "pfc_table_complete": pfc_table_complete,
                "app_mappings": app_mappings,
                "app_unparsed_selectors": app_unparsed,
                "dscp_to_priority": dscp,
                "priority_to_tc": priority_tc,
                "ets_table_present": ets_seen,
                "ets_table_complete": ets_table_complete,
                "dcbx_mode": dcbx_mode,
            }
            required_sections = ("pfc", "ets", "app", "buffer", "dcbx")
            incomplete_sections = {
                item: sections[item][0]
                for item in required_sections
                if sections[item][0] != "COMPLETE"
            }
            parse_incomplete_sections = []
            if sections["pfc"][0] == "COMPLETE" and not pfc_table_complete:
                parse_incomplete_sections.append("pfc")
            if sections["ets"][0] == "COMPLETE" and not ets_table_complete:
                parse_incomplete_sections.append("ets")
            if incomplete_sections:
                dcb_evidence_reason = (
                    "ROCE_DCB_EVIDENCE_TRUNCATED"
                    if "TRUNCATED" in incomplete_sections.values()
                    else "ROCE_DCB_EVIDENCE_MISSING"
                )
                add(
                    f"ROCE_DCB_EVIDENCE:{target}", UNKNOWN,
                    dcb_evidence_reason,
                    f"{target} DCB evidence statuses={incomplete_sections}",
                )
            elif parse_incomplete_sections:
                add(
                    f"ROCE_DCB_EVIDENCE:{target}", UNKNOWN,
                    "ROCE_DCB_EVIDENCE_PARSE_INCOMPLETE",
                    f"{target} incomplete priority tables={parse_incomplete_sections}",
                )
            else:
                add(
                    f"ROCE_DCB_EVIDENCE:{target}", PASS,
                    "ROCE_DCB_EVIDENCE_COMPLETE",
                    f"{target} PFC/ETS/APP/DCBX command evidence is complete",
                )

            pfc_ready = (
                sections["pfc"][0] == "COMPLETE" and pfc_table_complete
            )
            if sections["pfc"][0] != "COMPLETE":
                pfc_reason = (
                    "ROCE_PFC_EVIDENCE_TRUNCATED"
                    if sections["pfc"][0] == "TRUNCATED"
                    else "ROCE_PFC_EVIDENCE_MISSING"
                )
                add(
                    f"ROCE_LOSSLESS_PRIORITIES:{target}", UNKNOWN,
                    pfc_reason,
                    f"{target} PFC evidence={sections['pfc'][0]}",
                )
            elif not pfc_seen:
                add(
                    f"ROCE_LOSSLESS_PRIORITIES:{target}", UNKNOWN,
                    "ROCE_PFC_PARSE_FAILED",
                    f"{target} PFC table could not be parsed",
                )
            elif not pfc_table_complete:
                add(
                    f"ROCE_LOSSLESS_PRIORITIES:{target}", UNKNOWN,
                    "ROCE_PFC_PARSE_INCOMPLETE",
                    f"{target} PFC must contain unique priorities 0..7",
                )
            elif "lossless_priorities" not in expected:
                add(
                    f"ROCE_LOSSLESS_PRIORITIES:{target}", UNVALIDATED,
                    "ROCE_PFC_POLICY_MISMATCH_POLICY_NOT_PROVIDED",
                    f"{target} lossless_priorities={set(pfc)}",
                )
            else:
                actual_pfc = set(pfc)
                wanted_pfc = set(expected["lossless_priorities"])
                match = actual_pfc == wanted_pfc
                add(
                    f"ROCE_LOSSLESS_PRIORITIES:{target}",
                    PASS if match else FAIL,
                    "LOSSLESS_PRIORITIES_POLICY_MATCH" if match else "ROCE_PFC_POLICY_MISMATCH",
                    f"{target} expected={wanted_pfc}, actual={actual_pfc}",
                )

            app_complete = sections["app"][0] == "COMPLETE"
            app_has_known_mapping = any(app_mappings.values())
            if "dscp_to_priority" in expected:
                wanted_dscp = expected["dscp_to_priority"]
                if not app_complete:
                    app_reason = (
                        "ROCE_APP_EVIDENCE_TRUNCATED"
                        if sections["app"][0] == "TRUNCATED"
                        else "ROCE_APP_EVIDENCE_MISSING"
                    )
                    add(
                        f"ROCE_DSCP_TO_PRIORITY:{target}", UNKNOWN,
                        app_reason,
                        f"{target} APP evidence={sections['app'][0]}",
                    )
                elif dscp != wanted_dscp:
                    add(
                        f"ROCE_DSCP_TO_PRIORITY:{target}", FAIL,
                        "ROCE_APP_POLICY_MISMATCH",
                        f"{target} expected={wanted_dscp}, actual={dscp}",
                    )
                elif app_unparsed or set(app_mappings) - {"dscp-prio"}:
                    add(
                        f"ROCE_DSCP_TO_PRIORITY:{target}", UNKNOWN,
                        "ROCE_APP_POLICY_SCOPE_INCOMPLETE",
                        f"{target} legacy DSCP-only policy cannot validate other/unparsed selectors; "
                        f"selectors={sorted(app_mappings)}, unparsed={app_unparsed}",
                    )
                else:
                    add(
                        f"ROCE_DSCP_TO_PRIORITY:{target}", PASS,
                        "DSCP_TO_PRIORITY_POLICY_MATCH",
                        f"{target} expected={wanted_dscp}, actual={dscp}",
                    )
            elif "app_mappings" not in expected:
                add(
                    f"ROCE_DSCP_TO_PRIORITY:{target}", UNVALIDATED,
                    "ROCE_APP_POLICY_MISMATCH_POLICY_NOT_PROVIDED",
                    f"{target} dscp_to_priority={dscp}",
                )

            if "app_mappings" in expected:
                wanted_app = expected["app_mappings"]
                if not app_complete:
                    app_reason = (
                        "ROCE_APP_EVIDENCE_TRUNCATED"
                        if sections["app"][0] == "TRUNCATED"
                        else "ROCE_APP_EVIDENCE_MISSING"
                    )
                    add(
                        f"ROCE_APP_MAPPINGS:{target}", UNKNOWN,
                        app_reason,
                        f"{target} APP evidence={sections['app'][0]}",
                    )
                elif app_mappings != wanted_app:
                    add(
                        f"ROCE_APP_MAPPINGS:{target}", FAIL,
                        "ROCE_APP_EXACT_POLICY_MISMATCH",
                        f"{target} expected={wanted_app}, actual={app_mappings}, "
                        f"unparsed={app_unparsed}",
                    )
                elif app_unparsed:
                    add(
                        f"ROCE_APP_MAPPINGS:{target}", UNKNOWN,
                        "ROCE_APP_SELECTOR_UNPARSED",
                        f"{target} exact APP policy cannot validate selectors={app_unparsed}",
                    )
                else:
                    add(
                        f"ROCE_APP_MAPPINGS:{target}", PASS,
                        "ROCE_APP_EXACT_POLICY_MATCH",
                        f"{target} expected={wanted_app}, actual={app_mappings}",
                    )

            ets_complete = (
                sections["ets"][0] == "COMPLETE" and ets_table_complete
            )
            if sections["ets"][0] != "COMPLETE":
                ets_reason = (
                    "ROCE_ETS_EVIDENCE_TRUNCATED"
                    if sections["ets"][0] == "TRUNCATED"
                    else "ROCE_ETS_EVIDENCE_MISSING"
                )
                add(
                    f"ROCE_PRIORITY_TO_TC:{target}", UNKNOWN,
                    ets_reason,
                    f"{target} ETS evidence={sections['ets'][0]}",
                )
            elif not ets_seen:
                add(
                    f"ROCE_PRIORITY_TO_TC:{target}", UNKNOWN,
                    "ROCE_ETS_PARSE_FAILED",
                    f"{target} ETS prio-tc table could not be parsed",
                )
            elif not ets_table_complete:
                add(
                    f"ROCE_PRIORITY_TO_TC:{target}", UNKNOWN,
                    "ROCE_ETS_PARSE_INCOMPLETE",
                    f"{target} ETS prio-tc must contain unique priorities 0..7",
                )
            elif "priority_to_tc" not in expected:
                add(
                    f"ROCE_PRIORITY_TO_TC:{target}", UNVALIDATED,
                    "ROCE_ETS_POLICY_MISMATCH_POLICY_NOT_PROVIDED",
                    f"{target} priority_to_tc={priority_tc}",
                )
            else:
                wanted_ets = expected["priority_to_tc"]
                # Policy may declare only lossless priorities, but the source
                # table has already proved complete 0..7 coverage above.
                match = all(
                    priority_tc.get(key) == value
                    for key, value in wanted_ets.items()
                )
                add(
                    f"ROCE_PRIORITY_TO_TC:{target}", PASS if match else FAIL,
                    "PRIORITY_TO_TC_POLICY_MATCH" if match else "ROCE_ETS_POLICY_MISMATCH",
                    f"{target} expected={wanted_ets}, actual={priority_tc}",
                )
            app_priorities = {
                priority
                for mapping in app_mappings.values()
                for priority in mapping.values()
            }
            if pfc_ready and app_complete and app_has_known_mapping:
                inconsistent_app = {
                    selector: {
                        key: priority
                        for key, priority in mapping.items()
                        if priority not in pfc
                    }
                    for selector, mapping in app_mappings.items()
                }
                inconsistent_app = {
                    selector: mapping
                    for selector, mapping in inconsistent_app.items()
                    if mapping
                }
                if inconsistent_app:
                    add(
                        f"ROCE_DCB_INTERNAL:{target}", FAIL,
                        "ROCE_APP_PRIORITY_NOT_LOSSLESS",
                        f"{target} APP mappings target priorities without PFC: {inconsistent_app}",
                    )
                elif not ets_complete:
                    add(
                        f"ROCE_DCB_INTERNAL:{target}", UNKNOWN,
                        "ROCE_ETS_EVIDENCE_INCOMPLETE",
                        f"{target} APP/PFC is consistent but ETS table is incomplete",
                    )
                else:
                    inconsistent_ets = {
                        priority: priority_tc.get(priority)
                        for priority in app_priorities
                        if priority not in priority_tc
                    }
                    if inconsistent_ets:
                        add(
                            f"ROCE_DCB_INTERNAL:{target}", FAIL,
                            "ROCE_APP_PRIORITY_WITHOUT_ETS_TC",
                            f"{target} APP priorities lack ETS TC mappings: {sorted(inconsistent_ets)}",
                        )
                    elif app_unparsed:
                        add(
                            f"ROCE_DCB_INTERNAL:{target}", UNKNOWN,
                            "ROCE_APP_SELECTOR_UNPARSED",
                            f"{target} unparsed APP selectors={app_unparsed}",
                        )
                    else:
                        add(
                            f"ROCE_DCB_INTERNAL:{target}", PASS,
                            "ROCE_DCB_INTERNAL_CONSISTENT",
                            f"{target} PFC/APP/ETS relationships are internally consistent",
                        )
            else:
                add(
                    f"ROCE_DCB_INTERNAL:{target}", UNKNOWN,
                    "ROCE_DCB_RELATIONSHIP_EVIDENCE_MISSING",
                    f"{target} cannot validate PFC/APP/ETS relationships independently; "
                    f"pfc_ready={pfc_ready}, app_evidence={sections['app'][0]}, "
                    f"known_app_mapping={app_has_known_mapping}",
                )

            if "dcbx_mode" not in expected:
                if sections["dcbx"][0] == "COMPLETE" and dcbx_mode is not None:
                    add(
                        f"ROCE_DCBX:{target}", UNVALIDATED,
                        "ROCE_DCBX_POLICY_NOT_PROVIDED",
                        f"{target} dcbx={dcbx_mode}",
                    )
                else:
                    add(
                        f"ROCE_DCBX:{target}", UNKNOWN,
                        (
                            "ROCE_DCBX_EVIDENCE_TRUNCATED"
                            if sections["dcbx"][0] == "TRUNCATED"
                            else "ROCE_DCBX_EVIDENCE_MISSING"
                        ),
                        f"{target} DCBX evidence={sections['dcbx'][0]}",
                    )
            elif sections["dcbx"][0] != "COMPLETE" or dcbx_mode is None:
                add(
                    f"ROCE_DCBX:{target}", UNKNOWN,
                    (
                        "ROCE_DCBX_EVIDENCE_TRUNCATED"
                        if sections["dcbx"][0] == "TRUNCATED"
                        else "ROCE_DCBX_EVIDENCE_MISSING"
                    ),
                    f"{target} DCBX evidence={sections['dcbx'][0]}",
                )
            else:
                match = dcbx_mode in expected["dcbx_mode"]
                add(
                    f"ROCE_DCBX:{target}", PASS if match else FAIL,
                    "ROCE_DCBX_POLICY_MATCH" if match else "ROCE_DCBX_POLICY_MISMATCH",
                    f"{target} expected={expected['dcbx_mode']}, actual={dcbx_mode}",
                )

    for name in mapped_netdevs:
        topology = (interface_results.get(name) or {}).get("topology") or {}
        expected_targets = set(topology.get("leaf_interfaces") or [])
        actual_targets = dcb_target_names_by_source.get(name, set())
        if topology.get("cycles"):
            continue
        if topology.get("missing_interfaces"):
            add(f"ROCE_DCB_TARGETS:{name}", UNKNOWN, "ROCE_DCB_TARGET_TOPOLOGY_EVIDENCE_MISSING", f"cannot verify DCB leaf coverage; topology={topology}")
        elif not actual_targets:
            add(f"ROCE_DCB_TARGETS:{name}", UNKNOWN, "ROCE_DCB_TARGET_EVIDENCE_MISSING", f"expected leaf targets={sorted(expected_targets)}")
        elif actual_targets != expected_targets:
            extra = sorted(actual_targets - expected_targets)
            missing = sorted(expected_targets - actual_targets)
            status = FAIL if extra else UNKNOWN
            add(f"ROCE_DCB_TARGETS:{name}", status, "ROCE_DCB_TARGET_MISMATCH" if extra else "ROCE_DCB_TARGET_EVIDENCE_MISSING", f"expected={sorted(expected_targets)}, actual={sorted(actual_targets)}, missing={missing}, extra={extra}")
        else:
            add(f"ROCE_DCB_TARGETS:{name}", PASS, "ROCE_DCB_TARGETS_MATCH_LEAVES", f"DCB targets={sorted(actual_targets)}")
    if mapped_netdevs and not dcb_targets:
        add("ROCE_DCB_EVIDENCE", UNKNOWN, "ROCE_DCB_EVIDENCE_MISSING", "no DCB command evidence was collected")
    elif not mapped_netdevs and binding_evidence_unknown:
        add("ROCE_DCB_EVIDENCE", UNKNOWN, "ROCE_DCB_BINDING_EVIDENCE_MISSING", "DCB policy cannot be associated until the GID-to-netdev binding is known")

    pause_results: dict[str, Any] = {}
    fec_results: dict[str, Any] = {}
    for name in mapped_netdevs:
        configuration = (interfaces.get(name) or {}).get("roce_configuration") or {}
        configuration = configuration if isinstance(configuration, Mapping) else {}
        topology = (interface_results.get(name) or {}).get("topology") or {}
        expected_targets = set(topology.get("leaf_interfaces") or [name])

        raw_pause_targets = configuration.get("pause_targets")
        pause_is_targeted = isinstance(raw_pause_targets, Mapping)
        pause_commands = (
            dict(raw_pause_targets)
            if pause_is_targeted
            else {name: configuration.get("pause")}
        )
        if pause_is_targeted and set(pause_commands) != expected_targets:
            missing = sorted(expected_targets - set(pause_commands))
            extra = sorted(set(pause_commands) - expected_targets)
            add(
                f"ROCE_PAUSE_TARGETS:{name}",
                FAIL if extra else UNKNOWN,
                "ROCE_PAUSE_TARGET_MISMATCH" if extra else "ROCE_PAUSE_TARGET_EVIDENCE_MISSING",
                f"expected={sorted(expected_targets)}, actual={sorted(pause_commands)}, missing={missing}, extra={extra}",
            )
        elif pause_is_targeted:
            add(
                f"ROCE_PAUSE_TARGETS:{name}", PASS, "ROCE_PAUSE_TARGETS_MATCH_LEAVES",
                f"pause targets={sorted(pause_commands)}",
            )
        for target, command in sorted(pause_commands.items()):
            pause_status, pause_text = _command_evidence(command)
            pause = _parse_pause(pause_text)
            pause_results[str(target)] = {
                "source_netdev": name,
                "evidence_status": pause_status,
                "settings": pause,
                "stdout": pause_text,
            }
            if pause_status != "COMPLETE" or pause is None:
                pause_reason = (
                    "ROCE_GLOBAL_PAUSE_EVIDENCE_TRUNCATED"
                    if pause_status == "TRUNCATED"
                    else "ROCE_GLOBAL_PAUSE_EVIDENCE_MISSING"
                )
                add(
                    f"ROCE_GLOBAL_PAUSE:{target}", UNKNOWN, pause_reason,
                    f"{target} pause evidence={pause_status}",
                )
            elif "global_pause" not in expected:
                add(f"ROCE_GLOBAL_PAUSE:{target}", UNVALIDATED, "ROCE_GLOBAL_PAUSE_POLICY_NOT_PROVIDED", f"{target} pause={pause}")
            else:
                match = pause == expected["global_pause"]
                add(f"ROCE_GLOBAL_PAUSE:{target}", PASS if match else FAIL, "ROCE_GLOBAL_PAUSE_POLICY_MATCH" if match else "ROCE_GLOBAL_PAUSE_POLICY_MISMATCH", f"{target} expected={expected['global_pause']}, actual={pause}")

        raw_fec_targets = configuration.get("fec_targets")
        fec_is_targeted = isinstance(raw_fec_targets, Mapping)
        fec_commands = (
            dict(raw_fec_targets)
            if fec_is_targeted
            else {name: configuration.get("fec")}
        )
        if fec_is_targeted and set(fec_commands) != expected_targets:
            missing = sorted(expected_targets - set(fec_commands))
            extra = sorted(set(fec_commands) - expected_targets)
            add(
                f"ROCE_FEC_TARGETS:{name}",
                FAIL if extra else UNKNOWN,
                "ROCE_FEC_TARGET_MISMATCH" if extra else "ROCE_FEC_TARGET_EVIDENCE_MISSING",
                f"expected={sorted(expected_targets)}, actual={sorted(fec_commands)}, missing={missing}, extra={extra}",
            )
        elif fec_is_targeted:
            add(
                f"ROCE_FEC_TARGETS:{name}", PASS, "ROCE_FEC_TARGETS_MATCH_LEAVES",
                f"FEC targets={sorted(fec_commands)}",
            )
        for target, command in sorted(fec_commands.items()):
            fec_status, fec_text = _command_evidence(command)
            fec = _parse_fec(fec_text)
            fec_results[str(target)] = {
                "source_netdev": name,
                "evidence_status": fec_status,
                "settings": fec,
                "stdout": fec_text,
            }
            if fec_status != "COMPLETE" or fec is None or fec.get("active") is None:
                fec_reason = (
                    "ROCE_FEC_EVIDENCE_TRUNCATED"
                    if fec_status == "TRUNCATED"
                    else "ROCE_FEC_EVIDENCE_MISSING"
                )
                add(
                    f"ROCE_FEC:{target}", UNKNOWN, fec_reason,
                    f"{target} FEC evidence={fec_status}",
                )
            elif "fec_mode" not in expected:
                add(f"ROCE_FEC:{target}", UNVALIDATED, "ROCE_FEC_POLICY_NOT_PROVIDED", f"{target} FEC={fec}")
            else:
                active = str(fec.get("active") or "").lower()
                match = any(mode == active or mode in active.split() for mode in expected["fec_mode"])
                add(f"ROCE_FEC:{target}", PASS if match else FAIL, "ROCE_FEC_POLICY_MATCH" if match else "ROCE_FEC_POLICY_MISMATCH", f"{target} expected={expected['fec_mode']}, actual={active}")

    overall = _overall([item["status"] for item in checks])
    return {
        "schema_version": "1.0",
        "status": overall,
        "policy_applied": bool(expected),
        "normalized_policy": {
            key: ([str(item) for item in value] if key == "allowed_prefixes" else value)
            for key, value in expected.items()
        },
        "bindings": bindings,
        "interfaces": interface_results,
        "addresses": all_addresses,
        "dcb_targets": dcb_targets,
        "pause": pause_results,
        "fec": fec_results,
        "checks": checks,
        "summary": {
            "ethernet_rdma_ports": ethernet_ports,
            "roce_bindings": len(bindings),
            "versions": configured_versions,
            "mapped_netdevs": mapped_netdevs,
            "failed_checks": [item["check_id"] for item in checks if item["status"] == FAIL],
            "unknown_checks": [item["check_id"] for item in checks if item["status"] == UNKNOWN],
            "unvalidated_checks": [item["check_id"] for item in checks if item["status"] == UNVALIDATED],
        },
    }


# Explicit name for callers that view this as policy evaluation rather than a
# health check.  Keep one implementation to prevent semantic drift.
evaluate_roce_configuration = evaluate_roce_health
