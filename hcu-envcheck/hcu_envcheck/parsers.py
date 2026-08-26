# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any


ANSI_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


class ParseError(ValueError):
    pass


def strip_ansi(text: str) -> str:
    return ANSI_RE.sub("", text)


def extract_json_object(text: str) -> dict[str, Any]:
    clean = strip_ansi(text)
    decoder = json.JSONDecoder()
    for match in re.finditer(r"\{", clean):
        try:
            value, _ = decoder.raw_decode(clean[match.start() :])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    raise ParseError("command output does not contain a JSON object")


def parse_card_json(text: str) -> dict[int, dict[str, Any]]:
    payload = extract_json_object(text)
    cards: dict[int, dict[str, Any]] = {}
    for key, value in payload.items():
        match = re.search(r"(?:card|hcu)\s*\[?(\d+)\]?", str(key), re.I)
        if not match or not isinstance(value, dict):
            continue
        cards[int(match.group(1))] = value
    if not cards:
        raise ParseError("JSON object contains no card/HCU entries")
    return cards


def _normal_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _number(value: Any) -> float:
    match = re.search(r"[-+]?\d+(?:\.\d+)?", str(value).replace(",", ""))
    if not match:
        raise ParseError(f"not a numeric value: {value!r}")
    return float(match.group(0))


def _find_value(fields: dict[str, Any], *required_tokens: str, excluded_tokens: tuple[str, ...] = ()) -> Any | None:
    for key, value in fields.items():
        normalized = _normal_key(key)
        if all(token in normalized for token in required_tokens) and not any(
            token in normalized for token in excluded_tokens
        ):
            return value
    return None


def parse_hy_smi_samples(
    memory_outputs: list[str],
    available_outputs: list[str],
    memory_percent_outputs: list[str],
    utilization_outputs: list[str],
    bus_output: str | None = None,
) -> dict[int, dict[str, Any]]:
    if not memory_outputs or not utilization_outputs:
        raise ParseError("memory and utilization samples are required")

    samples: list[dict[int, dict[str, Any]]] = []
    sample_count = max(
        len(memory_outputs),
        len(available_outputs),
        len(memory_percent_outputs),
        len(utilization_outputs),
    )
    for index in range(sample_count):
        merged: dict[int, dict[str, Any]] = {}
        sources = (
            (memory_outputs, "memory"),
            (available_outputs, "available"),
            (memory_percent_outputs, "memory_percent"),
            (utilization_outputs, "utilization"),
        )
        for outputs, source_name in sources:
            if index >= len(outputs):
                continue
            for card, fields in parse_card_json(outputs[index]).items():
                merged.setdefault(card, {})[source_name] = fields
        samples.append(merged)

    bus_cards = parse_card_json(bus_output) if bus_output else {}
    device_ids = sorted({card for sample in samples for card in sample} | set(bus_cards))
    result: dict[int, dict[str, Any]] = {}
    for card in device_ids:
        totals: list[float] = []
        used_values: list[float] = []
        available_values: list[float] = []
        memory_percent_values: list[float] = []
        utilization_values: list[float] = []

        for sample in samples:
            values = sample.get(card, {})
            memory = values.get("memory", {})
            available = values.get("available", {})
            memory_percent = values.get("memory_percent", {})
            utilization = values.get("utilization", {})

            total_value = _find_value(memory, "total", "memory", excluded_tokens=("used",))
            used_value = _find_value(memory, "used", "memory")
            available_value = _find_value(available, "available", "memory")
            mem_pct_value = _find_value(memory_percent, "memory", "use")
            util_value = _find_value(utilization, "hcu", "use")
            if util_value is None:
                util_value = _find_value(utilization, "hcu", "util")

            if total_value is not None:
                totals.append(_number(total_value))
            if used_value is not None:
                used_values.append(_number(used_value))
            if available_value is not None:
                available_values.append(_number(available_value))
            if mem_pct_value is not None:
                memory_percent_values.append(_number(mem_pct_value))
            if util_value is not None:
                utilization_values.append(_number(util_value))

        bus_value = _find_value(bus_cards.get(card, {}), "pci", "bus")
        if bus_value is None:
            bus_value = _find_value(bus_cards.get(card, {}), "bus")
        bdf_match = re.search(r"(?:[0-9a-f]{4}:)?[0-9a-f]{2}:[0-9a-f]{2}\.[0-7]", str(bus_value or ""), re.I)
        bdf = bdf_match.group(0).lower() if bdf_match else None
        if bdf and len(bdf) == 7:
            bdf = "0000:" + bdf

        result[card] = {
            "total_mib": totals[-1] if totals else None,
            "used_mib": max(used_values) if used_values else None,
            "available_mib": min(available_values) if available_values else None,
            "memory_used_percent_reported": max(memory_percent_values) if memory_percent_values else None,
            "hcu_util_percent": max(utilization_values) if utilization_values else None,
            "used_mib_samples": used_values,
            "total_mib_samples": totals,
            "available_mib_samples": available_values,
            "memory_used_percent_reported_samples": memory_percent_values,
            "hcu_util_percent_samples": utilization_values,
            "bdf": bdf,
            "sample_count": sample_count,
        }
    return result


@dataclass
class RocmAgent:
    agent_id: int
    architecture: str | None
    model: str | None
    uuid: str | None
    bdf: str | None
    total_mib: float | None


def _field(block: str, name: str) -> str | None:
    match = re.search(rf"(?m)^\s*{re.escape(name)}:\s*(.*?)\s*$", block)
    return match.group(1).strip() if match else None


def _bdfid_to_bdf(value: int) -> str:
    bus = (value >> 8) & 0xFF
    device = (value >> 3) & 0x1F
    function = value & 0x7
    return f"0000:{bus:02x}:{device:02x}.{function}"


def parse_rocminfo(text: str) -> list[RocmAgent]:
    clean = strip_ansi(text)
    starts = list(re.finditer(r"(?m)^\s*Agent\s+(\d+)\s*$", clean))
    agents: list[RocmAgent] = []
    for index, start in enumerate(starts):
        end = starts[index + 1].start() if index + 1 < len(starts) else len(clean)
        block = clean[start.end() : end]
        device_type = (_field(block, "Device Type") or "").upper()
        if device_type not in {"HCU", "GPU"}:
            continue

        total_candidates: list[float] = []
        pool_section = re.search(r"(?s)Pool Info:(.*?)(?:ISA Info:|$)", block)
        if pool_section:
            pool_blocks = re.split(r"(?m)^\s*Pool\s+\d+\s*$", pool_section.group(1))
            for pool in pool_blocks:
                if "GLOBAL" not in pool or "COARSE GRAINED" not in pool:
                    continue
                size = re.search(r"(?m)^\s*Size:\s*(\d+)(?:\([^)]*\))?\s*KB", pool)
                if size:
                    total_candidates.append(int(size.group(1)) / 1024.0)

        bdf = None
        bdfid = _field(block, "BDFID")
        if bdfid:
            match = re.search(r"\d+", bdfid)
            if match:
                bdf = _bdfid_to_bdf(int(match.group(0)))

        agents.append(
            RocmAgent(
                agent_id=int(start.group(1)),
                architecture=_field(block, "Name"),
                model=_field(block, "Marketing Name"),
                uuid=_field(block, "Uuid"),
                bdf=bdf,
                total_mib=max(total_candidates) if total_candidates else None,
            )
        )
    if not agents:
        raise ParseError("rocminfo output contains no HCU/GPU agents")
    return agents
