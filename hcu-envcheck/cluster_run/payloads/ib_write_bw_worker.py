# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Launcher-aware ib_write_bw prerequisite worker.

The full server/client orchestration remains an extension point because
device, port and message-size policy is site-specific.  This worker performs
the safe, launcher-independent prerequisite check and records rank topology.
Callers that need a site-specific command can pass ``--script``.
"""

from __future__ import annotations

import argparse
import shutil

from ._torch_common import configure_direct_mpi_environment, emit


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="ib_write_bw")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    topology = configure_direct_mpi_environment()
    path = shutil.which(args.binary)
    status = "PREREQUISITE_ONLY" if path else "INCOMPLETE"
    emit(
        {
            "test": "ib-write-bw",
            "status": status,
            "binary": path,
            "topology": topology,
            "message": (
                "ib_write_bw is available, but no bandwidth traffic was run; "
                "supply a group-aware server/client test with --script"
                if path
                else f"{args.binary} was not found after env.sh"
            ),
        }
    )
    # A successful prerequisite is not a successful bandwidth measurement.
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

