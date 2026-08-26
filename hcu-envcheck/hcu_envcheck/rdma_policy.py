# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Load and validate explicit RDMA policy files without performing I/O remotely."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .roce_health import normalize_roce_policy


MAX_POLICY_BYTES = 1024 * 1024


def load_roce_policy(path: Path) -> dict[str, Any]:
    """Return a JSON-compatible RoCE policy after strict local validation.

    Validation happens on the controller before any SSH, Slurm or Kubernetes
    action.  The raw JSON-compatible values are retained so the policy can be
    embedded in reports; ``normalize_roce_policy`` remains the single source
    of truth for accepted keys and value ranges.
    """

    if not path.is_file():
        raise ValueError(f"RDMA policy file not found: {path}")
    size = path.stat().st_size
    if size > MAX_POLICY_BYTES:
        raise ValueError(
            f"RDMA policy file is too large: {size} bytes; maximum={MAX_POLICY_BYTES}"
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except UnicodeError as exc:
        raise ValueError(f"RDMA policy file is not valid UTF-8: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"RDMA policy file is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("RDMA policy file must contain one JSON object")
    if not payload:
        raise ValueError("RDMA policy file cannot contain an empty JSON object")
    normalize_roce_policy(payload)
    return payload


def policy_requires_roce(policy: dict[str, Any] | None) -> bool:
    """An explicit RoCE policy always requires a RoCE current port mode."""

    return bool(policy)
