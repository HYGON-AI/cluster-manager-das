# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Shell environment bootstrap helpers.

The cluster runner deliberately treats ``env.sh`` as executable configuration.
It does not parse or infer module/Conda metadata from the file.  The same
bootstrap command is used for node checks and active tests.
"""

from __future__ import annotations

import shlex
import subprocess
from pathlib import Path
from typing import Sequence


def shell_join(argv: Sequence[str]) -> str:
    if not argv or any("\x00" in item for item in argv):
        raise ValueError("command must contain at least one safe argument")
    return shlex.join([str(item) for item in argv])


def source_body(
    env_script: str | Path | None,
    command: Sequence[str],
    *,
    shell: str = "bash",
    exports: dict[str, str] | None = None,
    workdir: str | Path | None = None,
) -> str:
    """Restore *workdir*, optionally source *env_script*, then exec *command*.

    The cd is part of the body, so it runs after login-shell startup files.
    With workdir=None the historic set-e/source prefix is unchanged.
    """

    if env_script is not None and not str(env_script).strip():
        raise ValueError("env_script must not be empty when supplied")
    if shell not in {"bash", "sh"}:
        raise ValueError("shell must be bash or sh")
    source_operator = "source" if shell == "bash" else "."
    # Keep errexit active *inside* the sourced script.  `source file || exit`
    # suppresses errexit for the entire script, hiding failed module/DTK steps.
    lines = ["set -e"]
    if workdir is not None:
        directory = str(workdir)
        if not directory.strip() or any(ord(char) < 32 or ord(char) == 127 for char in directory):
            raise ValueError("workdir must be a non-empty path without control characters")
        lines.append(f"cd -- {shlex.quote(directory)}")
    if env_script is not None:
        lines.append(f"{source_operator} {shlex.quote(str(env_script))}")
    for name, value in (exports or {}).items():
        if not name or not name.replace("_", "").isalnum() or name[0].isdigit():
            raise ValueError(f"unsafe environment variable name: {name!r}")
        lines.append(f"export {name}={shlex.quote(str(value))}")
    lines.append(f"exec {shell_join(command)}")
    return "\n".join(lines)


def bootstrap_command(
    env_script: str | Path | None,
    command: Sequence[str],
    *,
    shell: str = "bash",
    exports: dict[str, str] | None = None,
    workdir: str | Path | None = None,
) -> list[str]:
    """Build a shell argv; optional workdir is restored before source."""

    return [shell, "-lc" if shell == "bash" else "-c",
            source_body(env_script, command, shell=shell, exports=exports, workdir=workdir)]


def validate_local_script(path: str | Path) -> None:
    """Validate a locally available env script without executing it."""

    script = Path(path)
    if not script.is_file():
        raise ValueError(f"env script does not exist locally: {script}")
    completed = subprocess.run(
        ["bash", "-n", str(script)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or "bash syntax check failed"
        raise ValueError(f"invalid env script {script}: {detail}")
