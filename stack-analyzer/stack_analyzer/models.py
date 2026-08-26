# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Iterable


class ProcessRole(str, Enum):
    TRAINER = "trainer"
    DATALOADER = "dataloader"
    CHECKPOINT = "checkpoint"
    OTHER = "other"


class AggregationMethod(str, Enum):
    """Which clustering backend produced the result."""

    TRIE = "trie"
    SIGNATURE = "signature"


class AggregationStrategy(str, Enum):
    """Runtime selection: Trie primary, signature fallback when auto."""

    AUTO = "auto"
    TRIE = "trie"
    SIGNATURE = "signature"


@dataclass(frozen=True)
class ProcessSnapshot:
    machine_id: str
    rank: int
    pid: int
    ppid: int
    cmdline: str
    role: ProcessRole


@dataclass(frozen=True)
class StackFrame:
    function: str
    file: str = ""
    line: int = 0

    def normalized(self) -> str:
        # Drop line numbers for cross-machine matching; keep function + module path.
        if self.file:
            return f"{self.function} ({self.file})"
        return self.function


@dataclass(frozen=True)
class StackSnapshot:
    machine_id: str
    rank: int
    pid: int
    role: ProcessRole
    frames: tuple[StackFrame, ...]
    raw_text: str = ""

    def signature(self, depth: int = 8) -> str:
        top = self.frames[:depth]
        return " | ".join(frame.normalized() for frame in top)


@dataclass
class ParallelTopology:
    """Maps global ranks to parallel groups (PP / TP / DP)."""

    pp_groups: list[frozenset[int]] = field(default_factory=list)
    tp_groups: list[frozenset[int]] = field(default_factory=list)
    dp_groups: list[frozenset[int]] = field(default_factory=list)

    def groups_for_rank(self, rank: int) -> list[tuple[str, frozenset[int]]]:
        result: list[tuple[str, frozenset[int]]] = []
        for label, groups in (
            ("PP", self.pp_groups),
            ("TP", self.tp_groups),
            ("DP", self.dp_groups),
        ):
            for group in groups:
                if rank in group:
                    result.append((label, group))
        return result

    def shared_group(
        self, ranks: Iterable[int], min_coverage: float = 1.0
    ) -> tuple[str, frozenset[int]] | None:
        """Find the smallest parallel group covering outlier ranks."""
        rank_set = frozenset(ranks)
        if not rank_set:
            return None

        candidates: list[tuple[str, frozenset[int], int]] = []
        for label, groups in (
            ("PP", self.pp_groups),
            ("TP", self.tp_groups),
            ("DP", self.dp_groups),
        ):
            for group in groups:
                overlap = rank_set & group
                if not overlap:
                    continue
                coverage = len(overlap) / len(rank_set)
                if coverage >= min_coverage:
                    candidates.append((label, group, len(group)))

        if not candidates:
            return None

        # Prefer the tightest group; tie-break PP > TP > DP (paper §5.1 over-evicts PP).
        label_order = {"PP": 0, "TP": 1, "DP": 2}
        candidates.sort(key=lambda item: (item[2], label_order.get(item[0], 99)))
        label, group, _ = candidates[0]
        return label, group
