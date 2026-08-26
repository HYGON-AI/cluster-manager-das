# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

from concurrent.futures import ThreadPoolExecutor
from types import MethodType
from unittest.mock import MagicMock
import threading

import pytest

from cluster_manager.node_management.hostfile_handler import HostfileHandler
from cluster_manager.node_management.node_blacklist_manager import (
    AllocationResult,
    BlacklistConfig,
    BlacklistManager,
    FaultType,
)
from cluster_manager.node_management.node_pool import NodePool, NodePoolErrorCode


def make_blacklist_manager(tmp_path, **overrides):
    config = BlacklistConfig(
        persistence_path=str(tmp_path / "blacklist.json"),
        persistence_backup_path=str(tmp_path / "blacklist.json.bak"),
        **overrides,
    )
    return BlacklistManager(config)


def test_fatal_node_is_rejected_and_shortage_is_reported(tmp_path):
    manager = make_blacklist_manager(tmp_path)
    assert manager.report_fault("node01", FaultType.HCU, error_code="76")

    result = manager.allocate_nodes(["node01", "node02"], required_count=2)

    assert result.healthy_nodes == ["node02"]
    assert result.rejected_nodes == ["node01"]
    assert result.shortage == 1
    assert manager.is_blacklisted("node01") == (True, True)


def test_repeated_network_faults_soft_ban_without_duplicate_nodes(tmp_path):
    manager = make_blacklist_manager(
        tmp_path,
        fault_soft_ban_thresholds={int(FaultType.NETWORK): 3},
        fault_auto_ban_thresholds={int(FaultType.NETWORK): 10},
    )

    for _ in range(3):
        assert manager.report_fault("node01", FaultType.NETWORK)

    info = manager.get_node_info("node01")
    assert info["total_fault_count"] == 3
    assert manager.get_stats()["total_blacklisted"] == 1
    assert manager.is_blacklisted("node01") == (True, False)


def test_concurrent_fault_reports_keep_every_node(tmp_path):
    manager = make_blacklist_manager(tmp_path)
    nodes = [f"node{i:02d}" for i in range(32)]

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(
            executor.map(
                lambda node: manager.report_fault(node, FaultType.HCU), nodes
            )
        )

    assert all(results)
    assert set(manager.get_all_blacklisted()) == set(nodes)


def test_hostfile_updates_are_sorted_deduplicated_and_slot_aware(tmp_path):
    hostfile = tmp_path / "hosts"
    HostfileHandler.write(str(hostfile), ["node02", "node01", "node02"])
    assert HostfileHandler.read(str(hostfile)) == ["node01", "node02"]

    HostfileHandler.write(str(hostfile), ["node02", "node01"], hcu_per_node=8)
    assert hostfile.read_text(encoding="utf-8").splitlines() == [
        "node01 slots=8",
        "node02 slots=8",
    ]
    assert HostfileHandler.read(str(hostfile)) == ["node01", "node02"]


def make_minimal_pool():
    pool = object.__new__(NodePool)
    pool._lock = threading.Lock()
    pool._total_nodes = ["node01", "node02"]
    pool._running_nodes = []
    pool._last_running_nodes = []
    pool._backup_nodes = ["node01", "node02"]
    pool._abnormal_nodes = []
    pool._normal_nodes = ["node01", "node02"]
    pool._persist_all_nodes = MagicMock(return_value=[])
    pool._validate_and_fix_constraints = MagicMock()
    pool._write_slots_running_nodes_file = MagicMock(return_value="slots.txt")
    return pool


def test_node_pool_refuses_allocation_when_capacity_is_short():
    pool = make_minimal_pool()
    pool._get_available_nodes_with_blacklist = MethodType(
        lambda self, nodes: (["node01"], [], ["node02"]), pool
    )

    assert pool.apply_node_num_resources(2, 8) == (None, None)
    assert pool._running_nodes == []
    assert pool._backup_nodes == ["node01", "node02"]


def test_running_node_fault_clears_allocation_and_is_idempotent():
    pool = make_minimal_pool()
    pool._running_nodes = ["node01"]
    pool._backup_nodes = ["node02"]
    pool._validate_abnormal_candidates = MethodType(
        lambda self, nodes: (
            (["node01"], NodePoolErrorCode.SUCCESS)
            if "node01" not in self._abnormal_nodes
            else ([], NodePoolErrorCode.NODE_ALREADY_ABNORMAL)
        ),
        pool,
    )

    first = pool.add_abnormal_nodes("node01")
    second = pool.add_abnormal_nodes("node01")

    assert first == (True, True, ["node01"], ["node01"])
    assert second == (False, False, None, None)
    assert pool._running_nodes == []
    assert pool._last_running_nodes == ["node01"]
    assert pool._abnormal_nodes == ["node01"]
    assert pool._backup_nodes == ["node02"]


class HealthyBlacklist:
    def start(self):
        return None

    def allocate_nodes(self, nodes, required_count):
        return AllocationResult(
            healthy_nodes=list(nodes),
            backup_nodes=[],
            rejected_nodes=[],
            total_healthy=len(nodes),
            total_backup=0,
            total_rejected=0,
            shortage=0,
        )


@pytest.fixture
def real_pool(tmp_path, monkeypatch):
    hostfile = tmp_path / "hostfile"
    HostfileHandler.write(
        str(hostfile), ["node04", "node01", "node03", "node02"]
    )
    blacklist = HealthyBlacklist()
    monkeypatch.setattr(
        "cluster_manager.node_management.node_pool.BlacklistManager.get_instance",
        MagicMock(return_value=blacklist),
    )
    return NodePool(tmp_path, hostfile)


def test_real_node_pool_initializes_and_persists_partitions(real_pool):
    assert real_pool.total_nodes == ["node01", "node02", "node03", "node04"]
    assert real_pool.running_nodes == []
    assert real_pool.backup_nodes == real_pool.total_nodes
    assert real_pool.normal_nodes == real_pool.total_nodes
    assert real_pool.total_nodes_file.read_text().splitlines() == real_pool.total_nodes


def test_real_node_pool_allocates_writes_slots_and_releases(real_pool):
    master, slots_file = real_pool.apply_node_num_resources(2, 8)
    assert master == "node01"
    assert real_pool.running_nodes == ["node01", "node02"]
    assert slots_file.read_text().splitlines() == [
        "node01 slots=8",
        "node02 slots=8",
    ]

    real_pool.release_runing_nodes()
    assert real_pool.running_nodes == []
    assert real_pool.backup_nodes == real_pool.total_nodes


def test_real_node_pool_fault_and_recovery_preserve_constraints(real_pool):
    changed, cleared, nodes, conflicts = real_pool.add_abnormal_nodes("node03")
    assert (changed, cleared, nodes, conflicts) == (
        True,
        False,
        ["node03"],
        None,
    )
    assert real_pool.abnormal_nodes == ["node03"]
    assert "node03" not in real_pool.normal_nodes
    assert "node03" not in real_pool.backup_nodes

    code, restored, restored_nodes = real_pool.add_normal_nodes("node03")
    assert (code, restored, restored_nodes) == (
        NodePoolErrorCode.SUCCESS,
        True,
        ["node03"],
    )
    assert real_pool.abnormal_nodes == []
    assert "node03" in real_pool.backup_nodes


def test_real_node_pool_adds_nodes_and_reports_running_differences(real_pool):
    assert real_pool.add_total_nodes(["node05", "node01"]) == ["node05"]
    real_pool.apply_node_list_resources(["node01", "node02"], 4)

    result = real_pool.validate_running_state_consistency(
        ["node02", "node03", "outside"]
    )
    assert result == {
        "consistent": False,
        "invalid_nodes": ["outside"],
        "added_nodes": ["node03"],
        "missing_nodes": ["node01"],
    }


def test_real_node_pool_rejects_invalid_reset_and_slots(real_pool):
    with pytest.raises(ValueError, match="abnormal nodes not in total_nodes"):
        real_pool.reset_node_pool(["node01"], ["outside"])

    real_pool.reset_node_pool(["node01", "node02"], [])
    real_pool.apply_node_num_resources(1, 8)
    with pytest.raises(ValueError):
        real_pool._write_slots_running_nodes_file(0)
