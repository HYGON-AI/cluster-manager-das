# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
node_management — 训练节点管理模块

对外暴露黑名单机制的核心接口：
    from node_management import BlacklistManager, FaultType, BlacklistConfig
"""

from cluster_manager.node_management.node_blacklist_manager import (
    BlacklistManager,
    BlacklistConfig,
    AllocationResult,
    FaultType,
    FaultSeverity,
    FaultRecord,
    NodeRecord,
    RackFaultEvent,
    ScoredNode,
    ScoringEngine,
)

__all__ = [
    "BlacklistManager",
    "BlacklistConfig",
    "AllocationResult",
    "FaultType",
    "FaultSeverity",
    "FaultRecord",
    "NodeRecord",
    "RackFaultEvent",
    "ScoredNode",
    "ScoringEngine",
]
