# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Parsers for perftest/Verbs output used by bare-metal checks."""

from __future__ import annotations

import math
import re


def parse_verbs_average_gbps(text: str) -> float | None:
    """Return the last positive bandwidth value from a perftest result."""

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


def parse_verbs_endpoint_metadata(text: str) -> dict[str, list[str]]:
    """Extract stable endpoint metadata from perftest output."""

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
