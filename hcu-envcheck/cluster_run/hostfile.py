# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Hostfile reading and active-test group construction."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from hcu_envcheck.baremetal import parse_nodes_file


@dataclass(frozen=True)
class NodeGroup:
    group_id: int
    name: str
    nodes: tuple[str, ...]
    hostfile: Path

    @property
    def leader(self) -> str:
        return self.nodes[0]

    @property
    def size(self) -> int:
        return len(self.nodes)


def read_nodes(path: str | Path) -> list[str]:
    return parse_nodes_file(path)


def split_nodes(
    nodes: Iterable[str],
    *,
    group_size: int | None,
    strict_size: bool = False,
) -> list[tuple[str, ...]]:
    ordered = tuple(dict.fromkeys(str(node) for node in nodes if str(node).strip()))
    if not ordered:
        raise ValueError("hostfile contains no nodes")
    if group_size is None:
        return [ordered]
    if group_size < 1:
        raise ValueError("group_size must be at least 1")
    if group_size > len(ordered):
        raise ValueError(
            f"group_size={group_size} is greater than node count={len(ordered)}"
        )
    remainder = len(ordered) % group_size
    if strict_size and remainder:
        raise ValueError(
            f"node count {len(ordered)} is not divisible by group_size={group_size}"
        )
    return [
        ordered[index : index + group_size]
        for index in range(0, len(ordered), group_size)
    ]


def materialize_groups(
    nodes: Iterable[str],
    output_dir: str | Path,
    *,
    group_size: int | None,
    strict_size: bool = False,
    slots_per_node: int = 1,
) -> list[NodeGroup]:
    if slots_per_node < 1:
        raise ValueError("slots_per_node must be at least 1")
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    groups: list[NodeGroup] = []
    for index, group_nodes in enumerate(
        split_nodes(nodes, group_size=group_size, strict_size=strict_size)
    ):
        name = f"group-{index:03d}"
        group_dir = root / name
        group_dir.mkdir(parents=True, exist_ok=True)
        hostfile = group_dir / "hostfile"
        # Explicit slots are required by the PRRTE/Open MPI build used by
        # container deployments. A bare one-host-per-line file can cause
        # PRRTE to collapse the allocation onto the leader. The same file
        # remains valid for host launchers and direct mpirun.
        hostfile.write_text(
            "\n".join(f"{node} slots={slots_per_node}" for node in group_nodes)
            + "\n",
            encoding="utf-8",
        )
        groups.append(
            NodeGroup(
                group_id=index,
                name=name,
                nodes=group_nodes,
                hostfile=hostfile,
            )
        )
    return groups
