# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import os
import sys
import time
# import threading
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from cluster_manager.node_management.node_blacklist_manager import BlacklistManager, BlacklistConfig
from cluster_manager.config import global_config

blacklist_file = global_config.BLACKLIST_PERSISTENCE_PATH
blacklist_file_BAK = global_config.BLACKLIST_PERSISTENCE_BACKUP_PATH

bl_cfg = BlacklistConfig(
    persistence_path=blacklist_file,
    persistence_backup_path=blacklist_file_BAK,
)

bl_mgr =  BlacklistManager.get_instance(bl_cfg)
bl_mgr.start() # 启动异步落盘后台线程

# bl_mgr.report_healthy("node_name")  # 上报接点健康
# bl_mgr.is_blacklisted("node_name")  # 检查节点是否在黑名单中,返回两个值
# bl_mgr.get_node_info("node_name")   # 查看节点信息
# bl_mgr.remove_node("node_name")         # 移除故障节点
# bl_mgr.clear_node_history("node_name")  # 清除节点历史记录
# bl_mgr.force_persist()                  # 强制保存黑名单
# bl_mgr.allocate_nodes(all_nodes, required_count=512) # 根据总的节点列表选择节点


# bl_mgr.report_fault("node_name", FaultType.NETWORK, error_code="43", HCU_id="故障卡号", micro_step="INF的Iter", description="故障描述")
# bl_mgr.report_fault("node_name", FaultType.HCU, error_code="43", HCU_id="故障卡号", micro_step="INF的Iter", description="故障描述")
# bl_mgr.report_fault("node_name", FaultType.GRADIENT_INF, error_code="43", HCU_id="故障卡号", micro_step="INF的Iter", description="故障描述")
# bl_mgr.report_fault("node_name", FaultType.MANUAL, error_code="43", HCU_id="故障卡号", micro_step="INF的Iter", description="故障描述")
 

host_list = [
            "gc03r1n16",
            "gc03r4n20",
            "gc04r4n04",
            "gc05r3n09",
            "gc07r3n12",
            "gc02r3n09",
            "gc07r1n11",
            "gc07r3n17",
            "gc06r1n03",
            "gc02r3n10",
            "gc05r2n19",
            "gc03r4n13",
            "gc03r3n01",
            "gc04r3n08",
            "gc07r1n09",
            "gc05r3n18",
            "gc04r1n16",
            "gc03r2n18",
            "gc02r4n19",
            "gc02r1n13",
            "gc02r4n13",
            "gc01r3n10"
            ]
result = bl_mgr.allocate_nodes(host_list, required_count=15)
print(result)
print("-"*20)
print(result.healthy_nodes)
print("-"*20)
print(result.backup_nodes)
print("-"*20)
print(result.rejected_nodes)
print("-"*20)

# print(bl_mgr.report_healthy("node_name")) # 上报接地健康
# print("-"*20)
# print(bl_mgr.is_blacklisted("node_name"))  # 检查节点是否在黑名单中,返回两个值
# print("-"*20)
# print(bl_mgr.get_node_info("node_name")) # 查看节点信息
# print("-"*20)
# print(bl_mgr.get_all_blacklisted()) 
# print("-"*20)
# print(bl_mgr.get_ranked_list())
# print("-"*20)
# print(bl_mgr.get_stats())
# print("-"*20)

bl_mgr.stop()
