# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
import uuid
from datetime import datetime
from pathlib import Path

# Output run directories are named "<scenario-label>_<operation>_<timestamp>"
# so that a single glance identifies what produced them.
SCENARIO_DIRECTORY_LABELS = {
    "per-node-container": "container",
    "shared-conda": "conda",
    "node-local-conda": "lconda",
}


def run_directory_label(scenario: str, operation: str) -> str:
    label = SCENARIO_DIRECTORY_LABELS.get(scenario)
    if label is None:
        raise ValueError(f"unknown scenario for run directory label: {scenario}")
    operation = operation.replace(",", "_").replace("-", "_")
    if not operation:
        raise ValueError("operation must not be empty")
    return f"{label}_{operation}"


def claim_output_directory(path: Path) -> Path:
    """Atomically reserve a run directory and refuse every pre-existing path."""
    try:
        path.mkdir(parents=True, exist_ok=False)
    except FileExistsError as exc:
        raise ValueError(
            f"output directory already exists: {path}; choose a new run directory"
        ) from exc
    path.chmod(0o700)
    return path


def claim_labeled_run_directory(
    output_root: Path,
    label: str,
    *,
    timestamp: datetime | None = None,
) -> Path:
    """Create ``<label>_<YYYYmmdd_HHMMSS>`` under a reusable root.

    Same-second reruns append ``_1``, ``_2`` ... instead of failing, and an
    existing directory is never overwritten.
    """
    try:
        output_root.mkdir(parents=True, exist_ok=True)
    except FileExistsError as exc:
        raise ValueError(
            f"output root is not a directory: {output_root}"
        ) from exc
    if not output_root.is_dir():
        raise ValueError(f"output root is not a directory: {output_root}")
    moment = timestamp or datetime.now().astimezone()
    base = f"{label}_{moment.strftime('%Y%m%d_%H%M%S')}"
    for suffix in ("", *(f"_{index}" for index in range(1, 100))):
        try:
            return claim_output_directory(output_root / f"{base}{suffix}")
        except ValueError:
            # Same-second rerun: the next suffix gets a fresh directory.
            continue
    raise ValueError(f"too many run directories for this second under {output_root}")


def require_new_output_path(path: Path, *, label: str) -> None:
    if path.exists() or path.is_symlink():
        raise ValueError(f"{label} already exists: {path}; choose a new path")


def validate_output_layout(output_file: Path, evidence_directory: Path) -> None:
    output = output_file.absolute()
    evidence = evidence_directory.absolute()
    if output == evidence or output in evidence.parents or evidence in output.parents:
        raise ValueError(
            "output file and evidence directory must be separate, non-nested paths"
        )


def atomic_write_text_exclusive(path: Path, content: str, *, mode: int = 0o600) -> None:
    """Publish a complete file without overwriting an existing run result."""
    path.parent.mkdir(parents=True, exist_ok=True)
    require_new_output_path(path, label="output file")
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
        path.chmod(mode)
    except FileExistsError as exc:
        raise ValueError(f"output file already exists: {path}; choose a new path") from exc
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
