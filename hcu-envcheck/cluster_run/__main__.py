# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Guarded console entry point for the unified cluster runner."""

from __future__ import annotations

import os
import sys
import traceback

from .cli import main


def entrypoint() -> int:
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


if __name__ == "__main__":
    raise SystemExit(entrypoint())
