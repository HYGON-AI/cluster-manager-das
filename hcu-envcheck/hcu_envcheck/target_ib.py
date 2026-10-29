# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Run the existing paired IB bandwidth implementation from a group leader."""

from __future__ import annotations

import argparse
import json
import os
import shlex
from pathlib import Path

from cluster_run.extra_checks import run_group_ib
from hcu_envcheck.baremetal import BaremetalExecutionConfig
from hcu_envcheck.cluster_checks import IBStateCheckConfig, IBWriteBandwidthConfig


def _nodes(path: Path) -> list[str]:
    result: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        fields = raw.split()
        if fields and not fields[0].startswith("#"):
            result.append(fields[0])
    if len(result) < 2:
        raise ValueError("ib-write-bw requires at least two nodes in a group")
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hostfile", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--ssh-port", type=int, default=22)
    parser.add_argument("--ssh-user", default="root")
    parser.add_argument("--env-script")
    parser.add_argument("--run-token", required=True)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--ibstat-command", default="ibstat")
    parser.add_argument("--ib-tool", choices=("ib_write_bw", "ib_read_bw", "ib_send_bw"), default="ib_write_bw")
    parser.add_argument("--ib-protocol", choices=("ib", "roce"), default="ib")
    parser.add_argument("--ib-device")
    parser.add_argument("--ib-port", type=int, default=1)
    parser.add_argument("--ib-gid-index", type=int)
    parser.add_argument("--ib-control-port", type=int, default=18515)
    parser.add_argument("--ib-message-bytes", type=int, default=1 << 20)
    parser.add_argument("--ib-iterations", type=int, default=1000)
    parser.add_argument("--ib-minimum-gbps", type=float)
    parser.add_argument("--ib-concurrency", type=int, default=1)
    parser.add_argument("--ib-max-tests", type=int, default=1024)
    args = parser.parse_args()
    nodes = _nodes(args.hostfile)
    guard_options = {}
    if os.environ.get("HCU_STATE_LAYOUT") == "compact-group-v2":
        explicit_guard = os.environ.get("HCU_TASK_GUARD_FILE", "")
        if not explicit_guard or not explicit_guard.startswith("/") or any(ord(char) < 32 for char in explicit_guard):
            raise ValueError("compact IB requires an explicit absolute controller guard file")
        guard = Path(explicit_guard)
        if guard.is_symlink() or not guard.is_file() or not os.access(guard, os.R_OK):
            raise ValueError(f"compact IB controller guard is unavailable: {guard}")
        guard_options["guard_file"] = explicit_guard
    direct_log = os.environ.get("HCU_GROUP_LOG", "")
    public = Path(direct_log).parent if direct_log.startswith("/") else None
    group = Path(direct_log).stem if public is not None else None
    execution = BaremetalExecutionConfig(
        output_root=args.output_dir / "evidence",
        transport="ssh",
        concurrency=min(32, len(nodes)),
        command_timeout_seconds=args.timeout + 30,
        ssh_user=args.ssh_user,
        ssh_port=args.ssh_port,
        strict_host_key_checking="accept-new",
        group_log_root=public,
        group_log_groups=tuple((node, group) for node in nodes) if group else (),
    )
    try:
        report = run_group_ib(
            nodes,
            config=IBWriteBandwidthConfig(
                enabled=True, tool=args.ib_tool, protocol=args.ib_protocol,
                device=args.ib_device, ib_port=args.ib_port, gid_index=args.ib_gid_index,
                control_port=args.ib_control_port, message_bytes=args.ib_message_bytes,
                iterations=args.ib_iterations, minimum_average_gbps=args.ib_minimum_gbps,
                timeout_seconds=args.timeout, concurrency=args.ib_concurrency,
                max_tests=args.ib_max_tests,
            ),
            ib_state_config=IBStateCheckConfig(enabled=True, command=tuple(shlex.split(args.ibstat_command))),
            env_script=args.env_script or None,
            scope="host",
            transport="ssh",
            concurrency=min(32, len(nodes)),
            output_dir=args.output_dir,
            execution_config=execution,
            run_token=args.run_token,
            **guard_options,
        )
    finally:
        # The login user creates the stage parent. Return ownership of only
        # this generated tree so its controller can archive and unlink logs.
        if args.output_dir.is_dir():
            owner = args.output_dir.parent.stat()
            for path in [args.output_dir, *args.output_dir.rglob("*")]:
                if path.is_symlink() or not path.exists():
                    continue
                current = path.stat()
                if (hasattr(os, "geteuid") and os.geteuid() == 0
                        and (current.st_uid, current.st_gid) != (owner.st_uid, owner.st_gid)):
                    os.chown(path, owner.st_uid, owner.st_gid)
                path.chmod(0o755 if path.is_dir() else 0o644)
    print(json.dumps({"status": report["status"], "report": report["report_path"]}, ensure_ascii=False))
    return int(report["returncode"])


if __name__ == "__main__":
    raise SystemExit(main())
