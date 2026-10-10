# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Post-processing for node configuration and result consistency."""

from __future__ import annotations

import json
import re
from collections import Counter
from typing import Any, Iterable

from hcu_envcheck.environment import static_configuration_sections


_NATURAL_PART = re.compile(r"(\d+)")


def _natural_key(value: str) -> list[tuple[int, object]]:
    return [
        (1, int(part)) if part.isdigit() else (0, part.lower())
        for part in _NATURAL_PART.split(value)
        if part
    ]


def _environment_view(record: dict[str, Any]) -> dict[str, Any]:
    sections = static_configuration_sections(record, scope="platform-only")
    return {field: value for section in sections.values() for field, value in section.items()}


def _resource_view(record: dict[str, Any]) -> dict[str, Any]:
    sections = static_configuration_sections(record, scope="resource-only")
    return {field: value for section in sections.values() for field, value in section.items()}


def _missing_configuration(record: dict[str, Any], *, platform: bool, resource: bool) -> list[str]:
    if record.get("reachable") is False:
        return ["node_unreachable"]
    environment = record.get("environment") or {}
    missing = []
    if platform:
        for field in ("container_os", "kernel", "driver_version", "dtk_version", "python_version", "python_packages"):
            if field not in environment or environment[field] is None:
                missing.append("platform." + field)
    if resource:
        if environment.get("mem_total") is None:
            missing.append("resource.mem_total")
        if record.get("device_count") is None:
            missing.append("resource.device_count")
        elif record["device_count"] > 0:
            devices = record.get("devices") or []
            if len(devices) != record["device_count"] or any(
                not device.get("model") or not (device.get("hy_smi_total_mib") or device.get("rocminfo_total_mib"))
                for device in devices
            ):
                missing.append("resource.device_profiles")
    return missing


def build_consistency_summary(
    records: Iterable[dict[str, Any]],
    *,
    scope: str = "platform-and-resource",
) -> dict[str, Any]:
    ordered = sorted(list(records), key=lambda item: _natural_key(str(item.get("node", ""))))
    groups: dict[str, dict[str, Any]] = {}
    missing_by_node: dict[str, list[str]] = {}
    platform_enabled = scope != "resource-only"
    resource_enabled = scope != "platform-only"
    for record in ordered:
        node = str(record.get("node", ""))
        missing = _missing_configuration(record, platform=platform_enabled, resource=resource_enabled)
        if missing:
            missing_by_node[node] = missing
        configuration = {}
        if platform_enabled:
            configuration["platform"] = _environment_view(record)
        if resource_enabled:
            configuration["resource"] = _resource_view(record)
        key = json.dumps(configuration, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        group = groups.setdefault(
            key,
            {
                "configuration": configuration,
                "nodes": [],
                "passed_nodes": [],
                "failed_nodes": [],
                "incomplete_nodes": [],
            },
        )
        group["nodes"].append(node)
        status = record.get("status")
        if status == "READY":
            group["passed_nodes"].append(node)
        elif status == "BLOCKED":
            group["failed_nodes"].append(node)
        else:
            group["incomplete_nodes"].append(node)

    configuration_groups = list(groups.values())
    for group in configuration_groups:
        for key in ("nodes", "passed_nodes", "failed_nodes", "incomplete_nodes"):
            group[key].sort(key=_natural_key)
        group["node_count"] = len(group["nodes"])
        group["passed_count"] = len(group["passed_nodes"])
        group["failed_count"] = len(group["failed_nodes"])
        group["incomplete_count"] = len(group["incomplete_nodes"])
        group["partial"] = bool(group["failed_nodes"] or group["incomplete_nodes"])
        group["configuration_evidence"] = "PARTIAL" if any(node in missing_by_node for node in group["nodes"]) else "COLLECTED"
    configuration_groups.sort(key=lambda item: _natural_key(item["nodes"][0]))

    status_counts = Counter(str(record.get("status", "UNKNOWN")) for record in ordered)
    reference = configuration_groups[0]["configuration"] if configuration_groups else {}
    differences: list[dict[str, Any]] = []
    for group in configuration_groups:
        if group["configuration"] == reference:
            continue
        changed: dict[str, Any] = {}
        for section in ("platform", "resource"):
            current = group["configuration"].get(section, {})
            baseline = reference.get(section, {})
            section_changes = {
                field: {"reference": baseline.get(field), "actual": current.get(field)}
                for field in sorted(set(current) | set(baseline))
                if current.get(field) != baseline.get(field)
            }
            if section_changes:
                changed[section] = section_changes
        differences.append(
            {
                "nodes": group["nodes"],
                "changes": changed,
            }
        )

    return {
        "schema_version": "1.0",
        "scope": scope,
        "comparison_status": "DIFFERENT" if differences else ("UNVERIFIED" if missing_by_node or len(ordered) < 2 else "SAME_OBSERVED_CONFIGURATION"),
        "configuration_missing_by_node": missing_by_node,
        "configuration_unknown_nodes": list(missing_by_node),
        "comparison_note": "Groups compare collected static values only; missing values are not evidence of equality.",
        "node_count": len(ordered),
        "configuration_group_count": len(configuration_groups),
        "status_counts": dict(sorted(status_counts.items())),
        "passed_node_count": sum(status == "READY" for status in (record.get("status") for record in ordered)),
        "failed_node_count": sum(status == "BLOCKED" for status in (record.get("status") for record in ordered)),
        "incomplete_node_count": sum(record.get("status") not in {"READY", "BLOCKED"} for record in ordered),
        "configuration_groups": configuration_groups,
        "differences_from_reference": differences,
    }


def render_consistency_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "## 节点一致性汇总",
        "",
        f"- 检测范围：`{summary.get('scope', '-')}`",
        f"- 节点总数：`{summary.get('node_count', 0)}`",
        f"- 配置组数量：`{summary.get('configuration_group_count', 0)}`",
        f"- 比较结论：`{summary.get('comparison_status', 'UNVERIFIED')}`（仅比较已采集静态配置）",
        f"- 通过节点：`{summary.get('passed_node_count', 0)}`",
        f"- 未通过节点：`{summary.get('failed_node_count', 0)}`",
        f"- 证据不完整节点：`{summary.get('incomplete_node_count', 0)}`",
        "",
        "| 配置组 | 节点数 | 通过 | 未通过 | 不完整 | 节点 |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for index, group in enumerate(summary.get("configuration_groups", []), start=1):
        lines.append(
            f"| group-{index:03d} | {group['node_count']} | "
            f"{group['passed_count']} | {group['failed_count']} | "
            f"{group['incomplete_count']} | {', '.join(group['nodes'])} |"
        )

    lines.extend(["", "### 与参考配置的差异", ""])
    differences = summary.get("differences_from_reference") or []
    if not differences:
        lines.append("证据不足，不能确认节点配置一致。" if summary.get("comparison_status") == "UNVERIFIED" else "已采集的静态配置未发现差异。")
    else:
        for item in differences:
            lines.append(f"- 节点：`{', '.join(item['nodes'])}`")
            for section, fields in item["changes"].items():
                for field, values in fields.items():
                    lines.append(
                        f"  - `{section}.{field}`：参考={values['reference']!r}；"
                        f"实际={values['actual']!r}"
                    )
    for node, missing in (summary.get("configuration_missing_by_node") or {}).items():
        lines.append(f"- `{node}` 配置证据缺失：{', '.join(missing)}")
    return "\n".join(lines) + "\n"
