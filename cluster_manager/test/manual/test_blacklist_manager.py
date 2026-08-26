#!/usr/bin/env python3
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
黑名单机制手工回归测试
不依赖第三方测试框架，可直接运行此脚本。
"""
import os
import sys
import time
import json
import threading
from datetime import datetime, timezone, timedelta

# 确保能找到上层模块
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from cluster_manager.node_management.node_blacklist_manager import (
    BlacklistManager, BlacklistConfig, FaultType, FaultSeverity,
    FaultRecord, NodeRecord, ScoringEngine, RackFaultDetector,
    PersistenceManager,
)

# 辅助函数：构造一个干净的 Manager 用于独立测试
TEST_JSON = "./test_blacklist_raw.json"
TEST_BAK = "./test_blacklist_raw.json.bak"

def clean_files():
    for p in [TEST_JSON, TEST_BAK]:
        if os.path.exists(p):
            os.unlink(p)

def get_clean_manager() -> BlacklistManager:
    clean_files()
    cfg = BlacklistConfig(
        persistence_path=TEST_JSON,
        persistence_backup_path=TEST_BAK,
    )
    # 绕过单例，直接实例化
    mgr = BlacklistManager.__new__(BlacklistManager)
    mgr.config = cfg
    mgr._nodes = {}
    mgr._scoring = ScoringEngine(cfg)
    mgr._rack_detector = RackFaultDetector(cfg)
    mgr._persistence = PersistenceManager(cfg)
    mgr._dirty = False
    mgr._persist_thread = None
    mgr._stop_event = threading.Event()
    mgr._inf_tracker = {}
    return mgr

# 全局计数器
PASSED = 0
FAILED = 0

def run_test(name, func):
    """测试运行器"""
    global PASSED, FAILED
    print(f"\n{'='*60}")
    print(f"▶ 测试: {name}")
    print(f"{'='*60}")
    try:
        func()
        print(f"✅ [PASS] {name}")
        PASSED += 1
    except AssertionError as e:
        print(f"❌ [FAIL] {name} -> {e}")
        FAILED += 1
    except Exception as e:
        print(f"💥 [ERROR] {name} -> {type(e).__name__}: {e}")



# ================================================================
#                         测试用例
# ================================================================

def test_01_HCU_fatal_ban():
    """HCU 致命故障应一票否决"""
    mgr = get_clean_manager()
    mgr.report_fault("node-001", FaultType.HCU, error_code="43")
    
    is_banned, is_fatal = mgr.is_blacklisted("node-001")
    assert is_banned is True, "节点应被拉黑"
    assert is_fatal is True, "节点应是致命拉黑"
    
    info = mgr.get_node_info("node-001")
    assert info["fatal"] is True
    assert info["total_fault_count"] == 1

def test_02_manual_no_ban():
    """人为故障不应计入评分和拉黑"""
    mgr = get_clean_manager()
    mgr.report_fault("node-002", FaultType.MANUAL, "运维误杀进程")

    is_banned, is_fatal = mgr.is_blacklisted("node-002")
    assert is_banned is False, "人为故障不应拉黑"
    assert is_fatal is False

def test_03_network_soft_ban():
    """短时间多次网络故障应触发软拉黑"""
    mgr = get_clean_manager()
    # 默认软拉黑阈值是 3 次，窗口 1 小时
    for _ in range(3):
        mgr.report_fault("node-003", FaultType.NETWORK, "NCCL超时")
        
    is_banned, is_fatal = mgr.is_blacklisted("node-003")
    assert is_banned is True, "应触发软拉黑"
    assert is_fatal is False, "不应该是致命拉黑"

def test_04_allocate_normal():
    """正常分配：剔除坏节点，其余全用健康节点"""
    mgr = get_clean_manager()
    all_nodes = [f"n-{i:04d}" for i in range(100)]
    # 拉黑 5 个
    for i in range(5):
        mgr.report_fault(f"n-{i:04d}", FaultType.HCU, error_code="61")
        
    result = mgr.allocate_nodes(all_nodes, required_count=80)
    
    assert result.total_healthy == 95, f"应有 95 个健康节点，实际 {result.total_healthy}"
    assert result.total_rejected == 5, f"应剔除 5 个，实际 {result.total_rejected}"
    assert result.total_backup == 0, "不需要降级补充"
    assert result.shortage == 0, "不应有缺口"

def test_05_allocate_with_backup():
    """降级补充：健康节点不够时，从软拉黑节点中按评分补充"""
    mgr = get_clean_manager()
    all_nodes = [f"n-{i:04d}" for i in range(100)]
    
    # 制造 20 个软拉黑节点
    for i in range(20):
        for _ in range(3):
            mgr.report_fault(f"n-{i:04d}", FaultType.NETWORK)
            
    # 需要 100 个节点，健康节点只有 80 个
    result = mgr.allocate_nodes(all_nodes, required_count=100)
    
    assert result.total_healthy == 80
    assert result.total_backup == 20, f"应补充 20 个，实际 {result.total_backup}"
    assert result.shortage == 0
    assert len(result.healthy_nodes) + len(result.backup_nodes) == 100

def test_06_allocate_fatal_excluded():
    """绝对剔除：fatal 节点即使不够也绝对不能用来补充"""
    mgr = get_clean_manager()
    all_nodes = [f"n-{i:04d}" for i in range(50)]
    
    # 40 个致命，10 个软拉黑
    for i in range(40):
        mgr.report_fault(f"n-{i:04d}", FaultType.HCU, error_code="43")
    for i in range(40, 50):
        for _ in range(3):
            mgr.report_fault(f"n-{i:04d}", FaultType.NETWORK)
            
    result = mgr.allocate_nodes(all_nodes, required_count=50)
    
    assert result.total_healthy == 0
    assert result.total_rejected == 40
    # 只有 10 个软拉黑可用，缺口 40
    assert result.total_backup == 10
    assert result.shortage == 40, "fatal 节点绝对不能补充，必须有缺口"

def test_07_scoring_and_ranking():
    """评分排序：网络(10) < INF(30) < HCU致命"""
    mgr = get_clean_manager()
    mgr.report_fault("node-A", FaultType.NETWORK)
    mgr.report_fault("node-B", FaultType.HCU, error_code="43") # fatal
    mgr.report_fault("node-C", FaultType.GRADIENT_INF)
    # mgr.force_persist()
    ranked = mgr.get_ranked_list()
    print(ranked)
    names = [r["node_name"] for r in ranked]
    print(names)
    
    assert names.index("node-A") < names.index("node-C"), "网络故障应排在 INF 前面"
    assert names.index("node-C") < names.index("node-B"), "INF 应排在致命 HCU 前面"
    assert names[-1] == "node-B", "致命节点必须排在最后"

def test_08_rack_fault_intercept():
    """机架感知：短时间内大量网络故障应拦截，不上报个体"""
    cfg = BlacklistConfig(rack_fault_threshold=5, rack_fault_window_seconds=10.0)
    det = RackFaultDetector(cfg)
    
    events = []
    det.register_alert_callback(lambda e: events.append(e))
    
    now = datetime.now(timezone.utc)
    # 模拟 8 个节点在 5 秒内报网络故障
    for i in range(8):
        det.record_network_fault(f"rack-node-{i}", now + timedelta(seconds=i))
        
    assert len(events) == 1, "应触发一次机架级报警"
    assert len(events[0].affected_nodes) == 5, "应包含 8 个节点"

def test_10_decay_scoring():
    """时间衰减：很久以前的故障分数应大幅降低"""
    cfg = BlacklistConfig(decay_half_life_hours=168.0) # 7天半衰期
    eng = ScoringEngine(cfg)
    
    node = NodeRecord("old-node")
    now = datetime.now(timezone.utc)
    # 30 天前的一次 HCU 故障 (权重 100)
    node.add_fault(FaultRecord(FaultType.HCU, now - timedelta(days=30)))
    
    score = eng.compute_score(node, now)
    # 30天 ≈ 4.28 个半衰期 -> 100 * (0.5^4.28) ≈ 5.3
    assert score < 10.0, f"30天前的故障分数应很低，实际 {score}"
    assert score > 0.0, "分数不应为 0"

def test_11_persistence_and_recovery():
    """持久化与损坏恢复"""
    mgr = get_clean_manager()
    mgr.report_fault("n1", FaultType.HCU, error_code="43")
    print(mgr.get_node_info("n1"))
    mgr.force_persist()
    # 人为破坏主文件
    with open(TEST_JSON, "w") as f:
        f.write("{{{{{BROKEN JSON")
        
    # 重新加载（内部会走备份恢复逻辑）
    pm = PersistenceManager(mgr.config)
    loaded = pm.load()
    
    assert "n1" in loaded, "应从备份中恢复出 n1"
    assert loaded["n1"].fatal is True, "恢复后致命状态应保留"
    # 检查主文件是否已被修复
    assert os.path.exists(TEST_JSON), "恢复后应重写主文件"

def test_12_management_operations():
    """管理操作：移除节点与清除历史"""
    mgr = get_clean_manager()
    mgr.report_fault("n1", FaultType.HCU, error_code="43")
    # 清除历史（保留记录但重置状态）
    mgr.clear_node_history("n1")
    info = mgr.get_node_info("n1")
    assert info["fatal"] is False, "历史清除后应非致命"
    assert info["total_fault_count"] == 0, "记录数应为 0"
    
    # 彻底移除
    mgr.remove_node("n1")
    info = mgr.get_node_info("n1")
    assert info is None, "移除后查询应为空"

def test_13_inf_non_consecutive_ignored():
    """INF 精准判定：不连续的步数不应被记录"""
    mgr = get_clean_manager()
    # 只报第 100 步
    r1 = mgr.report_fault("inf-node", FaultType.GRADIENT_INF, micro_step=100)
    assert r1 is False, "单独一步不应记录"
    # 跳到第 105 步
    r2 = mgr.report_fault("inf-node", FaultType.GRADIENT_INF, micro_step=105)
    assert r2 is False, "不连续步不应记录"
    
    is_banned, _ = mgr.is_blacklisted("inf-node")
    assert is_banned is False, "节点不应被拉黑"

def test_14_init_from_scratch():
    """首次运行：无文件时应自动创建初始化文件"""
    mgr = get_clean_manager()
    
    # 确保文件完全不存在
    assert not os.path.exists(TEST_JSON), "测试前主文件不应存在"
    assert not os.path.exists(TEST_BAK), "测试前备份文件不应存在"

    mgr.report_fault("brand-new-node", FaultType.NETWORK)
    # mgr.force_persist()

# ================================================================
#                         启动执行
# ================================================================

if __name__ == "__main__":
    print("🚀 开始执行黑名单机制测试...\n")
    
    # 注册所有要跑的测试
    tests = [
        ("HCU 致命故障一票否决", test_01_HCU_fatal_ban),
        ("人为故障不计分拉黑", test_02_manual_no_ban),
        ("网络故障软拉黑触发", test_03_network_soft_ban),
        ("节点分配 - 正常剔除", test_04_allocate_normal),
        ("节点分配 - 降级补充", test_05_allocate_with_backup),
        ("节点分配 - Fatal绝对不补充", test_06_allocate_fatal_excluded),
        ("评分与可用性排序", test_07_scoring_and_ranking),
        ("机架级故障拦截", test_08_rack_fault_intercept),
        ("故障时间衰减评分", test_10_decay_scoring),
        ("持久化与备份恢复", test_11_persistence_and_recovery),
        ("节点移除与历史清除", test_12_management_operations),
        ("INF非连续步数忽略", test_13_inf_non_consecutive_ignored),
        ("首次运行自动初始化文件", test_14_init_from_scratch), 
    ]

    for name, func in tests:
        run_test(name, func)

    # 清理测试残留文件
    # clean_files()
    
    print(f"\n{'='*60}")
    print(f"🏁 测试完成: {PASSED} 通过, {FAILED} 失败 (共 {PASSED + FAILED} 项)")
    print(f"{'='*60}")
    
    # 返回非 0 退出码表示有失败（方便 CI/CD 识别）
    sys.exit(1 if FAILED > 0 else 0)
