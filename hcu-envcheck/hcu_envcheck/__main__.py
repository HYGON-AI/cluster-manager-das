# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import os
import sys
import traceback

from .cli import main


def entrypoint() -> int:
    """Keep unexpected tool failures distinct from a diagnosed BLOCKED result."""
    try:
        return main()
    except KeyboardInterrupt:
        print("RESULT        TOOL_ERROR", file=sys.stderr)
        print("ERROR         interrupted by user", file=sys.stderr)
        return 130
    except Exception as exc:
        print("RESULT        TOOL_ERROR", file=sys.stderr)
        print(f"ERROR         {type(exc).__name__}: {exc}", file=sys.stderr)
        if os.environ.get("HCU_ENVCHECK_DEBUG") == "1":
            traceback.print_exc()
        return 3


if __name__ == "__main__":
    raise SystemExit(entrypoint())
