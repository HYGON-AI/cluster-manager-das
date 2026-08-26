# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
import uuid
from datetime import datetime
from pathlib import Path


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


def claim_nodes_check_run_directory(
    output_root: Path,
    *,
    timestamp: datetime | None = None,
) -> Path:
    """Create one private timestamped bare-metal run directory under a reusable root."""
    try:
        output_root.mkdir(parents=True, exist_ok=True)
    except FileExistsError as exc:
        raise ValueError(
            f"output root is not a directory: {output_root}"
        ) from exc
    if not output_root.is_dir():
        raise ValueError(f"output root is not a directory: {output_root}")
    moment = timestamp or datetime.now().astimezone()
    stamp = moment.strftime("%Y%m%d_%H%M%S_%f")
    return claim_output_directory(output_root / f"nodes_check_{stamp}")


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
