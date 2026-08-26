# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Stack aggregation for distributed training hang diagnosis."""

from .aggregation_result import AggregationResult
from .aggregator import StackAggregator
from .ansible_collector import AnsibleConfig, AnsibleStackCollector
from .coordinator import collect_from_hostfile, diagnose_from_hostfile
from .docker_collector import DockerSSHConfig, DockerSSHStackCollector
from .hostfile import HostEntry, infer_topology, parse_hostfile, topology_from_hostfile
from .kubernetes_collector import KubernetesConfig, KubernetesStackCollector
from .megatron_topology import (
    DEFAULT_PARALLEL_ORDER,
    MegatronRankGenerator,
    ParallelLayout,
    infer_parallel_topology,
)
from .models import (
    AggregationMethod,
    AggregationStrategy,
    ParallelTopology,
    ProcessSnapshot,
    StackSnapshot,
)
from .runtime_analyzer import RuntimeAnalyzer
from .stack_capture import PySpyCapture
from .stack_trie import StackTrie
from .trie_aggregator import TrieStackAggregator

__all__ = [
    "StackAggregator",
    "AggregationResult",
    "AggregationMethod",
    "AggregationStrategy",
    "AnsibleConfig",
    "AnsibleStackCollector",
    "DockerSSHConfig",
    "DockerSSHStackCollector",
    "HostEntry",
    "KubernetesConfig",
    "KubernetesStackCollector",
    "ParallelTopology",
    "ProcessSnapshot",
    "StackSnapshot",
    "PySpyCapture",
    "RuntimeAnalyzer",
    "StackTrie",
    "TrieStackAggregator",
    "collect_from_hostfile",
    "diagnose_from_hostfile",
    "infer_topology",
    "infer_parallel_topology",
    "topology_from_hostfile",
    "MegatronRankGenerator",
    "ParallelLayout",
    "DEFAULT_PARALLEL_ORDER",
    "parse_hostfile",
]
