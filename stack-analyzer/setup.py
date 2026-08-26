# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Setuptools hook that stages repository-level legal files for packaging."""

import atexit
import shutil
from pathlib import Path

from setuptools import setup


project_dir = Path(__file__).resolve().parent
repository_root = project_dir.parent
created_files = []

for filename in ("LICENSE", "NOTICE", "THIRD_PARTY_NOTICES.md"):
    target = project_dir / filename
    if target.exists():
        continue

    source = repository_root / filename
    if not source.is_file():
        raise FileNotFoundError(f"Required repository legal file not found: {source}")
    shutil.copy2(source, target)
    created_files.append(target)


def cleanup():
    for path in created_files:
        path.unlink(missing_ok=True)


atexit.register(cleanup)
setup()
