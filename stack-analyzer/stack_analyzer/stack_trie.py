# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Stack Trie with per-frame rank distribution."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class TrieNode:
    children: dict[str, TrieNode] = field(default_factory=dict)
    is_end_of_stack: bool = False
    ranks: set[int] = field(default_factory=set)
    end_ranks: set[int] = field(default_factory=set)


@dataclass(frozen=True)
class LeafCluster:
    """One complete stack path and the ranks that ended on it."""

    signature: str
    ranks: frozenset[int]


class StackTrie:
    """
    Prefix tree over stack frames.

    ``ranks`` on each node = ranks whose stack passes through this frame.
    ``end_ranks`` on a leaf = ranks whose full stack terminates at that leaf.
    Clustering uses ``end_ranks`` only.
    """

    def __init__(self, all_ranks: set[int]) -> None:
        self.root = TrieNode()
        self.all_ranks = all_ranks

    def insert(self, frames: list[str], rank: int) -> None:
        node = self.root
        for word in frames:
            if "lto_priv" in word:
                break
            if word not in node.children:
                node.children[word] = TrieNode()
            node = node.children[word]
            node.ranks.add(rank)
        node.is_end_of_stack = True
        node.end_ranks.add(rank)

    def leaf_clusters(self) -> list[LeafCluster]:
        clusters: list[LeafCluster] = []

        def walk(node: TrieNode, path: list[str]) -> None:
            if node.is_end_of_stack and node.end_ranks:
                clusters.append(
                    LeafCluster(
                        signature=" | ".join(path),
                        ranks=frozenset(node.end_ranks),
                    )
                )
            for word, child in node.children.items():
                walk(child, path + [word])

        walk(self.root, [])
        return clusters

    def rank_signature_map(self) -> dict[int, str]:
        """Map each rank to its terminal leaf signature."""
        by_rank: dict[int, str] = {}
        for cluster in self.leaf_clusters():
            for rank in cluster.ranks:
                by_rank[rank] = cluster.signature
        return by_rank
