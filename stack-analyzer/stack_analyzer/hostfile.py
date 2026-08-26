# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from .megatron_topology import infer_parallel_topology
from .models import ParallelTopology

# OpenMPI / Slurm-style: "worker01 slots=4" or "worker01 ansible_host=10.0.0.1 slots=2"
_HOST_LINE = re.compile(
    r"^(?P<hostname>\S+)(?:\s+(?P<params>.+))?$"
)
_KV = re.compile(r"(\w+)=(\S+)")


@dataclass
class HostEntry:
    hostname: str
    slots: int = 1
    ansible_host: str | None = None
    ansible_user: str | None = None
    rank_start: int = 0
    ranks: list[int] = field(default_factory=list)

    @property
    def machine_id(self) -> str:
        return self.hostname


def parse_hostfile(path: Path | str) -> list[HostEntry]:
    """
    Parse MPI-style hostfile and assign consecutive global ranks by slot count.

    Example::
        worker01 slots=4
        worker02 slots=4
        worker03 slots=4
        worker04 slots=4
    """
    text = Path(path).read_text(encoding="utf-8")
    hosts: list[HostEntry] = []
    next_rank = 0

    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue

        match = _HOST_LINE.match(line)
        if not match:
            raise ValueError(f"Invalid hostfile line: {raw_line!r}")

        hostname = match.group("hostname")
        params = match.group("params") or ""
        slots = 1
        ansible_host: str | None = None
        ansible_user: str | None = None

        for key, value in _KV.findall(params):
            if key == "slots":
                slots = int(value)
            elif key == "ansible_host":
                ansible_host = value
            elif key == "ansible_user":
                ansible_user = value

        if slots < 1:
            raise ValueError(f"slots must be >= 1 on host {hostname}")

        ranks = list(range(next_rank, next_rank + slots))
        hosts.append(
            HostEntry(
                hostname=hostname,
                slots=slots,
                ansible_host=ansible_host,
                ansible_user=ansible_user,
                rank_start=next_rank,
                ranks=ranks,
            )
        )
        next_rank += slots

    if not hosts:
        raise ValueError(f"No hosts found in hostfile: {path}")

    return hosts


def world_size(hosts: list[HostEntry]) -> int:
    return sum(host.slots for host in hosts)


def rank_to_machine(hosts: list[HostEntry]) -> dict[int, str]:
    mapping: dict[int, str] = {}
    for host in hosts:
        for rank in host.ranks:
            mapping[rank] = host.machine_id
    return mapping


def write_ansible_inventory(
    hosts: list[HostEntry],
    *,
    default_user: str | None = None,
    output_path: Path | None = None,
) -> Path:
    """Write a temporary Ansible INI inventory with per-host rank_start/slots vars."""
    lines = ["[training]"]
    for host in hosts:
        parts = [host.hostname]
        if host.ansible_host:
            parts.append(f"ansible_host={host.ansible_host}")
        user = host.ansible_user or default_user
        if user:
            parts.append(f"ansible_user={user}")
        parts.append(f"rank_start={host.rank_start}")
        parts.append(f"slots={host.slots}")
        lines.append(" ".join(parts))

    lines.append("")
    lines.append("[training:vars]")
    lines.append("ansible_python_interpreter=auto_silent")

    content = "\n".join(lines) + "\n"
    if output_path:
        output_path.write_text(content, encoding="utf-8")
        return output_path

    handle = tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".ini",
        prefix="stack_analyzer_inventory_",
        delete=False,
        encoding="utf-8",
    )
    handle.write(content)
    handle.close()
    return Path(handle.name)


def infer_topology(
    hosts: list[HostEntry],
    *,
    pp_size: int = 1,
    tp_size: int = 1,
    dp_size: int | None = None,
    cp_size: int = 1,
    ep_size: int = 1,
    order: str | None = None,
    use_tp_pp_dp_mapping: bool = False,
) -> ParallelTopology:
    """
    Build PP/TP/DP groups from hostfile world size and parallel sizes.

    Uses hcu_megatron / Megatron ``RankGenerator`` layout (default order
    ``tp-cp-ep-dp-pp``). Ranks follow hostfile slot order.
    """
    return infer_parallel_topology(
        world_size(hosts),
        pp_size=pp_size,
        tp_size=tp_size,
        dp_size=dp_size,
        cp_size=cp_size,
        ep_size=ep_size,
        order=order,
        use_tp_pp_dp_mapping=use_tp_pp_dp_mapping,
    )


def topology_from_hostfile(
    hostfile: Path | str,
    *,
    pp_size: int,
    tp_size: int = 1,
    dp_size: int | None = None,
    cp_size: int = 1,
    ep_size: int = 1,
    order: str | None = None,
    use_tp_pp_dp_mapping: bool = False,
) -> tuple[ParallelTopology, list[HostEntry], dict[int, str]]:
    """Parse hostfile and derive Megatron-style topology + rank mapping."""
    hosts = parse_hostfile(hostfile)
    topology = infer_topology(
        hosts,
        pp_size=pp_size,
        tp_size=tp_size,
        dp_size=dp_size,
        cp_size=cp_size,
        ep_size=ep_size,
        order=order,
        use_tp_pp_dp_mapping=use_tp_pp_dp_mapping,
    )
    return topology, hosts, rank_to_machine(hosts)
