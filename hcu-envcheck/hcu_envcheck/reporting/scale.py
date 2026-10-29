# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Explicit planned scale; legacy slots are interpreted only with evidence."""
from pathlib import Path
import re

from .facts import allocation_path
from .model import rows


def positive(value, name):
    text = str(value)
    if not text.isdecimal() or int(text) < 1:
        raise ValueError(f"invalid {name}: {value!r}")
    return int(text)


def resolve_scale(control: dict, root: Path, groups: list[dict]) -> dict:
    operation = control.get("operation")
    group_size = positive(control.get("group_size"), "group_size")
    if operation in {"rccl", "deepep"}:
        # Explicit G/C must not hide a missing compact allocation. Historical
        # runs without compact facts retain their evidence-based hostfile path.
        for group in groups:
            allocation_path(root, group["group"], operation=operation)
    if control.get("gpus_per_node"):
        count = positive(control["gpus_per_node"], "gpus_per_node")
    elif operation == "rccl":
        # Old RCCL hostfiles are MPI allocations. Confirm their uniform card
        # count against every saved binary baseline plan, not control.slots.
        counts = set()
        for group in groups:
            name = group["group"]
            if not re.fullmatch(r"group-[0-9]+", name):
                raise ValueError("invalid legacy group identifier")
            hostfile = allocation_path(root, name, operation=operation)
            planfile = hostfile.parent / "rccl-plan.tsv"
            plans = rows(planfile)
            if not hostfile.is_file() or not plans:
                raise ValueError("legacy RCCL GPU count is ambiguous: allocation/baseline plan missing")
            allocation_rows = [line.split() for line in hostfile.read_text(encoding="utf-8").splitlines()
                               if line.strip() and not line.lstrip().startswith("#")]
            members = group.get("nodes", "").split(",")
            if [row[0] for row in allocation_rows] != members or len(allocation_rows) != group_size:
                raise ValueError("legacy RCCL group membership/scale is inconsistent")
            local_counts = []
            for row in allocation_rows:
                if len(row) != 2 or not row[1].startswith("slots="):
                    raise ValueError("legacy RCCL allocation does not declare per-node cards")
                local_counts.append(positive(row[1][6:], "legacy allocation slots"))
            if len(set(local_counts)) != 1:
                raise ValueError("legacy heterogeneous GPU counts cannot become uniform --gpus-per-node")
            counts.add(local_counts[0])
            if not plans or any(positive(row.get("np"), "legacy np") != sum(local_counts) for row in plans):
                raise ValueError("legacy RCCL GPU allocation and saved np disagree")
        if len(counts) != 1:
            raise ValueError("legacy RCCL plans do not establish one GPU count")
        count = counts.pop()
    else:
        raise ValueError("legacy GPU scale is ambiguous; slots alone cannot establish DeepEP GPU workers")
    mpi_ranks = (group_size * count if operation == "rccl" else group_size) if group_size > 1 else 0
    return {"group_size": group_size, "gpus_per_node": count,
            "mpi_ranks_per_group": mpi_ranks, "planned_gpus_per_group": group_size * count,
            **({"planned_deepep_gpu_workers_per_node": count} if operation == "deepep" else {})}
