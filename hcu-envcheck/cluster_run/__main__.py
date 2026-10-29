# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Guarded console entry point for the unified cluster runner.

The installed console script forwards public command lines to the unique
Bash dispatch source (cluster_run/dispatch.sh) so the source-tree launcher
and the installed entry point share one operation table, parameter boundary,
and exit-code contract. ``python -m cluster_run`` (the dispatcher's internal
Python path) stays on the Python core and never forwards back, which keeps
the forwarding direction loop-free.
"""

from __future__ import annotations

import os
import shutil
import sys
import traceback
from pathlib import Path

from .cli import SCENARIOS, main

_FORWARDED_FLAGS = {"--help", "-h", "--version", "--help-full"}
_LEADING_CONTROLLER_OPTIONS = {"--controller-env-script", "--controller-python"}


def _forward_public_argv() -> bool:
    """Exec the Bash dispatch source when argv looks like a public command.

    Only the leading controller bootstrap options may precede the scenario;
    everything after them is left to the dispatcher, which owns the real
    argument boundary. Any other argv (module, tests, internal callers) is not
    forwarded and falls back to the Python core.
    """
    dispatch = Path(__file__).resolve().parent / "dispatch.sh"
    if not dispatch.is_file():
        return False
    argv = sys.argv[1:]
    index = 0
    while index < len(argv):
        token = argv[index]
        if token in _LEADING_CONTROLLER_OPTIONS:
            index += 2
            continue
        if token.startswith("--controller-env-script=") or token.startswith("--controller-python="):
            index += 1
            continue
        break
    if index < len(argv) and argv[index] not in SCENARIOS and argv[index] not in _FORWARDED_FLAGS:
        return False
    bash = shutil.which("bash")
    if not bash:
        return False
    # Preserve the console script's interpreter even when its environment has
    # not been activated in the calling shell. The module path stays one-way.
    environment = os.environ.copy()
    environment["HCU_ENVCHECK_CONSOLE_PYTHON"] = sys.executable
    try:
        os.execve(bash, [bash, str(dispatch), *argv], environment)
    except OSError:
        return False
    return True  # pragma: no cover - a successful exec never returns


def _python_core() -> int:
    try:
        return main()
    except KeyboardInterrupt:
        print("RESULT        TOOL_ERROR\nERROR         interrupted by user", file=sys.stderr)
        return 130
    except Exception as exc:  # pragma: no cover - defensive console boundary
        print("RESULT        TOOL_ERROR", file=sys.stderr)
        if os.environ.get("HCU_ENVCHECK_DEBUG") == "1":
            traceback.print_exc()
        else:
            print(f"ERROR         {type(exc).__name__}: {exc}", file=sys.stderr)
            print("HINT          set HCU_ENVCHECK_DEBUG=1 for a Python traceback", file=sys.stderr)
        return 3


def entrypoint() -> int:
    # Console-script boundary (pyproject: cluster_run.__main__:entrypoint).
    if _forward_public_argv():
        return 0  # pragma: no cover - process replaced by the dispatcher
    return _python_core()


if __name__ == "__main__":
    # Internal module path (python -m cluster_run, reached from the Bash
    # dispatcher): run the Python core directly, never forward back.
    raise SystemExit(_python_core())
