# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
from typing import List, Union, Tuple, Optional, Dict
from pathlib import Path
import threading
from enum import Enum, unique
import sys
from collections import defaultdict
from cluster_manager.config.global_config import logger
from cluster_manager.node_management.hostfile_handler import HostfileHandler
from cluster_manager.utils.string_utils import compress_nodes
from cluster_manager.node_management.node_blacklist_manager import BlacklistManager, BlacklistConfig, AllocationResult
from cluster_manager.config.global_config import BLACKLIST_PERSISTENCE_PATH, BLACKLIST_PERSISTENCE_BACKUP_PATH
import shutil 

@unique
class NodePoolErrorCode(Enum):
    """节点池错误码枚举：明确区分不同异常场景，便于调用方处理"""
    SUCCESS = (0, "操作成功")
    ABNORMAL_IN_RUNNING = (1001, "新增异常节点包含运行中节点，已清空运行中列表")
    NODE_NOT_IN_TOTAL = (1002, "节点不在总节点中，无法执行操作")
    NODE_ALREADY_ABNORMAL = (1003, "节点已为异常节点，无需重复标记")
    NODE_NOT_IN_BACKUP_OR_NORMAL = (1004, "节点不在备用/正常节点中，无法流转为异常")
    NO_VALID_NODE = (1005, "无有效节点可执行操作")
    INVALID_TARGET_POOL = (1006, "目标节点池无效（仅支持 backup/running）")

class NodePool:
    """
    节点池管理器核心类

    核心成员变量（私有，仅通过接口修改/访问）：
    - _total_nodes: 总节点列表（唯一修改接口：add_total_nodes/remove_total_nodes）
    - _running_nodes: 运行中节点列表（唯一添加接口：add_running_nodes）
    - _backup_nodes: 备用节点列表（自动分配/流转，无专属添加接口）
    - _abnormal_nodes: 异常节点列表（修改接口：add_abnormal_nodes/remove_abnormal_nodes）
    - _normal_nodes: 正常节点列表（自动计算：normal_nodes = total_nodes - abnormal_nodes）

    核心规则（硬约束，全程校验）：
    1. 总节点稳定性：仅通过专属接口修改，内部流转（标记/恢复异常）不改变
    2. 节点集约束：total_nodes = running_nodes ∪ backup_nodes ∪ abnormal_nodes（三者无交集、无遗漏）
    3. 正常节点规则：normal_nodes = total_nodes - abnormal_nodes（自动同步，无需手动维护）
    4. 异常流转规则：新增异常节点仅从 backup_nodes/normal_nodes 流转
    5. 冲突处理规则：新增异常节点含运行中节点 → 返回错误码1001 + 清空运行中节点
    """

    def __init__(
        self,
        pool_root_path: Union[str, Path],
        hostfile,
    ):
        """
        初始化节点池（自动校验约束，确保初始状态符合所有规则）
        初始化失败时会抛出异常并强制进程退出

        :param pool_root_path: 节点池数据存储根路径（用于持久化文件）
        :param hostfile: 主机文件路径
        :raises RuntimeError: 初始化失败时抛出
        """
        try:
            # ------------------------------ 1. 初始化存储路径 ------------------------------
            self._root_path = Path(pool_root_path).resolve()
            self._node_pool_path: Optional[Path] = None
            self._init_storage_path()

            # ------------------------------ 2. 定义各节点类型的文件路径成员变量 ------------------------------
            self.total_nodes_filepath: Path = self._node_pool_path / "total_nodes.txt"
            self.running_nodes_filepath: Path = self._node_pool_path / "running_nodes.txt"
            self.backup_nodes_filepath: Path = self._node_pool_path / "backup_nodes.txt"
            self.abnormal_nodes_filepath: Path = self._node_pool_path / "abnormal_nodes.txt"
            self.normal_nodes_filepath: Path = self._node_pool_path / "normal_nodes.txt"
            self.slots_filepath: Path = self._node_pool_path / "slots_nodes.txt"
            self.additional_nodes_filepath: Path = self._node_pool_path / "additional_nodes.txt"
            # ------------------------------ 3. 创建所有节点文件 ------------------------------
            self._create_node_files()
            self.len_running_abnormal_nodes = 0

            # ------------------------------ 3.5 初始化黑名单管理器（节点池内部实例化） ------------------------------
            bl_cfg = BlacklistConfig(
                persistence_path=str(BLACKLIST_PERSISTENCE_PATH),
                persistence_backup_path=str(BLACKLIST_PERSISTENCE_BACKUP_PATH),
            )
            self._blacklist_mgr = BlacklistManager.get_instance(bl_cfg)
            self._blacklist_mgr.start()
            logger.info(
                f"黑名单管理器已实例化并启动: "
                f"persist={BLACKLIST_PERSISTENCE_PATH}, "
                f"backup={BLACKLIST_PERSISTENCE_BACKUP_PATH}"
            )

            # ------------------------------ 4. 初始化核心成员变量 ------------------------------
            self._total_nodes = self._deduplicate_and_sort(HostfileHandler.read(hostfile) or []  )
            self._check_and_sync_total_nodes()
            logger.info(f"total_nodes:{compress_nodes(self._total_nodes)} num:{len(self._total_nodes)}")
            logger.info(f"run_nodes:{compress_nodes(self._running_nodes)} num:{len(self._running_nodes)}")
            logger.info(f"backup_nodes:{compress_nodes(self._backup_nodes)} num:{len(self._backup_nodes)}")
            # ------------------------------ 5. 线程安全锁 ------------------------------
            self._lock = threading.Lock()

            # ------------------------------ 6. 约束校验 ------------------------------
            self._validate_and_fix_constraints()
            self._persist_all_nodes()

        except Exception as e:
            # 记录致命错误日志（包含异常堆栈）
            logger.critical(f"节点池初始化失败：{str(e)}, 容错进程退出", exc_info=True)
            # 强制进程退出（退出码1表示异常退出，符合Unix进程规范）
            sys.exit(1)

    @classmethod
    def clean_init(cls, pool_root_path: Union[str, Path], hostfile):
        """
        干净初始化：先删除已有的持久化文件，再初始化节点池。
        用于测试或重置场景。

        :param pool_root_path: 节点池数据存储根路径（同 __init__）
        :param hostfile: 主机文件路径（同 __init__）
        :return: NodePool 实例
        """
        pool_path = Path(pool_root_path).resolve() / ".node_pool"
        if pool_path.exists():
            shutil.rmtree(pool_path)
            logger.info(f"已删除持久化目录: {pool_path}")
        # 重置黑名单管理器单例，确保干净初始化
        BlacklistManager.reset_instance()
        return cls(pool_root_path, hostfile)
    
    # ------------------------------ 内部辅助函数（私有，仅类内部调用） ------------------------------
    def _check_and_sync_total_nodes(self):
        """
        校验传入的 total_nodes 与持久化文件（total_nodes.txt）中的节点是否一致：
        1. 文件为空 → 直接将传入节点写入文件
        2. 节点一致 → 从持久化文件恢复状态
        3. 节点不一致 → 检查 hostfile + succ_additional_nodes 是否与 total_nodes.txt 一致
           - 如果一致 → 从持久化文件恢复状态（保留运行状态）
           - 如果不一致 → 清空运行节点
        
        设计原则：
        - succ_additional_nodes.txt 保存了通过 NHC 检测并成功添加的节点
        - 当 hostfile + succ_additional_nodes 与 total_nodes.txt 一致时，说明是正常扩容，应保留运行状态
        """
        try:
            # 1. 读取文件中的总节点（自动过滤空行，去重排序，与内存格式对齐）
            file_nodes = HostfileHandler.read(self.total_nodes_filepath)
            file_nodes_sorted = self._deduplicate_and_sort(file_nodes)

            # 2. 内存中的总节点（已在 __init__ 中去重排序，来自 hostfile）
            mem_nodes = self._total_nodes
            self._last_running_nodes = []
            
            # 3. 对比一致性
            if mem_nodes == file_nodes_sorted:
                # 节点一致：从持久化文件恢复完整状态
                logger.info(
                    f"Node consistency check passed: "
                    f"input nodes are fully consistent with persisted nodes, "
                    f"num:{len(mem_nodes)} {compress_nodes(mem_nodes)}"
                )
                self._abnormal_nodes = HostfileHandler.read(self.abnormal_nodes_filepath)
                self._running_nodes = HostfileHandler.read(self.running_nodes_filepath)
                self._normal_nodes = HostfileHandler.read(self.normal_nodes_filepath)
                self._backup_nodes = HostfileHandler.read(self.backup_nodes_filepath)
            elif not file_nodes_sorted:
                # 持久化文件为空：首次初始化，使用 hostfile 节点
                logger.info(
                    f"Node pool first initialization: "
                    f"no persisted nodes, using hostfile nodes ({len(mem_nodes)})"
                )
                self._abnormal_nodes = []
                self._sync_normal_nodes()
                self._running_nodes = []
                self._sync_backup_nodes()
            else:
                # 节点不一致：检查 hostfile + succ_additional_nodes 是否与 total_nodes.txt 一致
                succ_additional_nodes_filepath = self._node_pool_path / "succ_additional_nodes.txt"
                succ_additional_nodes = []
                if succ_additional_nodes_filepath.exists():
                    succ_additional_nodes = HostfileHandler.read(succ_additional_nodes_filepath)
                
                # 合并 hostfile 节点和成功添加的节点
                combined_nodes = self._deduplicate_and_sort(mem_nodes + succ_additional_nodes)
                
                if combined_nodes == file_nodes_sorted:
                    # hostfile + succ_additional_nodes 与 total_nodes.txt 一致
                    # 说明是正常扩容，从持久化文件恢复状态（保留运行状态）
                    logger.info(
                        f"Node consistency check passed (with succ_additional_nodes): "
                        f"hostfile ({len(mem_nodes)}) + succ_additional ({len(succ_additional_nodes)}) "
                        f"= persisted nodes ({len(file_nodes_sorted)})"
                    )
                    self._total_nodes = file_nodes_sorted
                    self._abnormal_nodes = HostfileHandler.read(self.abnormal_nodes_filepath)
                    self._running_nodes = HostfileHandler.read(self.running_nodes_filepath)
                    self._normal_nodes = HostfileHandler.read(self.normal_nodes_filepath)
                    self._backup_nodes = HostfileHandler.read(self.backup_nodes_filepath)
                    logger.info(
                        f"State restored from persisted files: "
                        f"total={len(self._total_nodes)}, running={len(self._running_nodes)}, "
                        f"backup={len(self._backup_nodes)}, abnormal={len(self._abnormal_nodes)}"
                    )
                else:
                    # 真正的不一致，清空运行节点
                    logger.warning(
                        f"Node consistency check failed: "
                        f"input nodes ({len(mem_nodes)}) != persisted nodes ({len(file_nodes_sorted)}), "
                        f"even with succ_additional ({len(succ_additional_nodes)}), "
                        f"resetting running_nodes"
                    )
                    logger.debug(f"Input nodes (hostfile): {compress_nodes(mem_nodes)}")
                    logger.debug(f"Succ additional nodes: {compress_nodes(succ_additional_nodes)}")
                    logger.debug(f"Combined nodes: {compress_nodes(combined_nodes)}")
                    logger.debug(f"Persisted nodes: {compress_nodes(file_nodes_sorted)}")
        
                    self._abnormal_nodes = []
                    self._sync_normal_nodes()
                    self._running_nodes = []
                    self._sync_backup_nodes()
        except OSError as e:
            # 捕获文件读写异常（权限、磁盘满、文件损坏等）
            raise RuntimeError(
                f"读写总节点文件失败：{str(e)}，路径：{self.total_nodes_filepath.resolve()}"
            ) from e
        except Exception as e:
            raise RuntimeError(f"校验并同步总节点失败：{str(e)}") from e
        
    def _deduplicate_and_sort(self, nodes: List[str]) -> List[str]:
        """
        通用工具函数：过滤无效节点（空字符串/None）、去重、排序
        :param nodes: 原始节点列表
        :return: 处理后的纯净节点列表
        """
        # 过滤无效值 + 去重 + 排序
        valid_nodes = [node.strip() for node in nodes if node and str(node).strip()]
        return sorted(list(set(valid_nodes)))

    def _filter_nodes_in_total(self, nodes: List[str]) -> List[str]:
        """
        过滤节点列表：仅保留存在于总节点中的节点（防止子节点超出总节点范围）
        :param nodes: 待过滤节点列表
        :return: 过滤后的节点列表（去重排序）
        """
        total_set = set(self._total_nodes)
        filtered = [node for node in self._deduplicate_and_sort(nodes) if node in total_set]
        # 日志提示：如果有节点被过滤，输出警告
        filtered_out = set(self._deduplicate_and_sort(nodes)) - set(filtered)
        if filtered_out:
            logger.warning(f"初始化时过滤超出总节点的子节点：{sorted(filtered_out)}")
        return filtered
    
    def _get_available_nodes_with_blacklist(
        self, candidate_nodes: List[str]
    ) -> Tuple[List[str], List[str], List[str]]:
        """
        对候选节点进行黑名单过滤，分为三组：
        - 健康节点：不在黑名单中，优先使用
        - 降级节点：在黑名单中但非致命（backup_nodes），健康节点不足时降级补充
        - 致命剔除节点：在黑名单中且为致命故障，绝对不可使用

        :param candidate_nodes: 候选节点列表
        :return: (健康节点列表, 降级节点列表, 致命剔除节点列表)
        """
        try:
            allocation_result = self._blacklist_mgr.allocate_nodes(
                candidate_nodes, required_count=len(candidate_nodes)
            )
        except Exception as e:
            logger.error(f"黑名单查询异常，跳过黑名单过滤: {e}")
            return candidate_nodes.copy(), [], []

        candidate_set = set(candidate_nodes)
        healthy_set = set(allocation_result.healthy_nodes)
        backup_set = set(allocation_result.backup_nodes)
        rejected_set = set(allocation_result.rejected_nodes)

        # 只保留在候选列表中的节点（避免 allocate_nodes 返回的节点超出候选范围）
        healthy = self._deduplicate_and_sort(list(healthy_set & candidate_set))
        degraded = self._deduplicate_and_sort(list(backup_set & candidate_set))
        rejected = self._deduplicate_and_sort(list(rejected_set & candidate_set))

        if rejected:
            logger.warning(
                f"黑名单过滤：以下节点为致命故障节点，已被剔除 → {rejected}"
            )

        if degraded:
            logger.info(
                f"黑名单过滤：以下节点为降级备用节点（非致命，可降级使用） → {degraded}"
            )

        logger.info(
            f"黑名单过滤结果: 候选 {len(candidate_nodes)} 个, "
            f"健康 {len(healthy)} 个, 降级 {len(degraded)} 个, 致命剔除 {len(rejected)} 个"
        )

        return healthy, degraded, rejected

    def _calculate_backup_nodes(self) -> List[str]:
        """
        计算备用节点列表：backup_nodes = total_nodes - abnormal_nodes - running_nodes（集合差集）
        逻辑说明：总节点中排除异常节点和运行中节点，剩余节点为备用节点
        :return: 备用节点列表（去重排序）
        """        
        normal_set = set(self._normal_nodes)
        running_set = set(self._running_nodes)
        return self._deduplicate_and_sort(list(normal_set - running_set))
    
    def _calculate_normal_nodes(self) -> List[str]:
        """
        计算正常节点列表：normal_nodes = total_nodes - abnormal_nodes（集合差集）
        :return: 正常节点列表（去重排序）
        """
        total_set = set(self._total_nodes)
        abnormal_set = set(self._abnormal_nodes)
        return self._deduplicate_and_sort(list(total_set - abnormal_set))

    def _sync_normal_nodes(self) -> None:
        """
        同步更新正常节点列表（当 total_nodes 或 abnormal_nodes 变化时必须调用）
        确保 normal_nodes 始终符合规则：normal_nodes = total_nodes - abnormal_nodes
        """
        self._normal_nodes = self._calculate_normal_nodes()
        logger.debug(f"正常节点列表已同步：{self._normal_nodes}")


    def _sync_backup_nodes(self) -> None:
        """
        同步更新正常节点列表（当 total_nodes 或 abnormal_nodes 变化时必须调用）
        确保 normal_nodes 始终符合规则：normal_nodes = total_nodes - abnormal_nodes
        """
        self._backup_nodes = self._calculate_backup_nodes()
        logger.debug(f"正常节点列表已同步：{self._backup_nodes}")

    def _init_storage_path(self) -> None:
        """初始化存储路径：创建根目录（.node_pool）"""
        self._node_pool_path = self._root_path / ".node_pool"
        try:
            self._node_pool_path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"节点池存储路径初始化成功：{self._node_pool_path}")
        except PermissionError:
            raise RuntimeError(f"权限不足，无法创建存储路径 → {self._node_pool_path}")
        except OSError as e:
            raise RuntimeError(f"系统错误，创建存储路径失败 → {self._node_pool_path}：{str(e)}")
        except Exception as e:
            raise RuntimeError(f"未知错误，创建存储路径失败 → {self._node_pool_path}：{str(e)}")
    
    def _create_node_files(self) -> None:
        """创建所有节点类型对应的文件（不存在则创建，存在则不修改）"""
        node_files = [
            self.total_nodes_filepath,
            self.running_nodes_filepath,
            self.backup_nodes_filepath,
            self.abnormal_nodes_filepath,
            self.normal_nodes_filepath,
            self.additional_nodes_filepath        ]

        try:
            for file_path in node_files:
                file_path.touch(exist_ok=True)
                logger.debug(f"节点文件创建/验证成功：{file_path}")
            logger.debug(f"所有节点文件初始化完成（共{len(node_files)}个文件）")
        except PermissionError as e:
            raise RuntimeError(f"权限不足，无法创建文件 → {e.filename}")
        except OSError as e:
            raise RuntimeError(f"系统错误，创建文件失败 → {e.filename}：{str(e)}")
        except Exception as e:
            raise RuntimeError(f"未知错误，创建节点文件失败：{str(e)}")
        
    def _validate_and_fix_constraints(self) -> None:
        """
        校验并修复所有核心约束（确保节点池状态始终符合规则）
        修复逻辑：仅调整子节点分配，不修改总节点（总节点仅通过专属接口修改）
        """
        total_set = set(self._total_nodes)
        running_set = set(self._running_nodes)
        backup_set = set(self._backup_nodes)
        abnormal_set = set(self._abnormal_nodes)
     
        # ------------------------------ 约束1：子节点必须全部属于总节点（已通过_filter_nodes_in_total过滤，二次校验） ------------------------------
        self._backup_nodes = self._deduplicate_and_sort(list(backup_set & total_set))
        self._abnormal_nodes = self._deduplicate_and_sort(list(abnormal_set & total_set))
      
        # ------------------------------ 约束2：running/backup/abnormal 三者无交集（优先级：abnormal > running > backup） ------------------------------
        # 1. 运行中与异常交集 → 移至异常（异常优先级最高）
        running_abnormal_conflict = running_set & abnormal_set
        # 格式：assert 条件, f"错误描述：变量名={变量值}（类型：{type(变量)}）"
        logger.debug(f"running_abnormal_conflict={running_abnormal_conflict} running_set:{running_set} abnormal_set:{abnormal_set}")
        assert running_abnormal_conflict == set() 
        # if running_abnormal_conflict:
        #     logger.warning(f"约束冲突：运行中与异常节点重叠 → {sorted(running_abnormal_conflict)}，移至异常节点")
        #     self._running_nodes = self._deduplicate_and_sort(list(running_set - abnormal_set))
        #     self._abnormal_nodes = self._deduplicate_and_sort(list(abnormal_set | running_abnormal_conflict))

        # 2. 备用与异常交集 → 移至异常（异常优先级高于备用）
        backup_abnormal_conflict = backup_set & abnormal_set
        logger.debug(f"backup_abnormal_conflict={backup_abnormal_conflict} backup_set:{backup_set} abnormal_set:{abnormal_set}")
        assert backup_abnormal_conflict == set()
        # if backup_abnormal_conflict:
        #     logger.warning(f"约束冲突：备用与异常节点重叠 → {sorted(backup_abnormal_conflict)}，移至异常节点")
        #     self._backup_nodes = self._deduplicate_and_sort(list(backup_set - abnormal_set))
        #     self._abnormal_nodes = self._deduplicate_and_sort(list(abnormal_set | backup_abnormal_conflict))

        # 3. 运行中与备用交集 → 移至运行中（运行中优先级高于备用）
        running_backup_conflict = running_set & backup_set
        logger.debug(f"running_backup_conflict={running_backup_conflict} running_set:{running_set} backup_set:{backup_set}")
        assert running_backup_conflict == set()
        # if running_backup_conflict:
        #     logger.warning(f"约束冲突：运行中与备用节点重叠 → {sorted(running_backup_conflict)}，移至运行中节点")
        #     self._backup_nodes = self._deduplicate_and_sort(list(backup_set - running_set))

        # ------------------------------ 约束3：running + backup + abnormal = total（无遗漏） ------------------------------
        assigned_nodes = set(self._running_nodes) | set(self._backup_nodes) | set(self._abnormal_nodes)
        unassigned_nodes = total_set - assigned_nodes
        logger.debug(f"running_backup_conflict={running_backup_conflict} running_set:{running_set} backup_set:{backup_set} abnormal_set:{abnormal_set}")
        assert unassigned_nodes == set()

        # if unassigned_nodes:
        #     logger.warning(f"约束冲突：总节点存在未分配节点 → {sorted(unassigned_nodes)}，自动分配至备用节点")
        #     self._backup_nodes = self._deduplicate_and_sort(list(backup_set | unassigned_nodes))


    def _validate_abnormal_candidates(self, nodes: List[str]) -> Tuple[List[str], NodePoolErrorCode]:
        """
        校验新增异常节点的合法性（严格遵循流转规则）
        合法条件：1. 在总节点中；2. 不在运行节点中；3. 不在异常中；4. 在备用/正常节点中
        :param nodes: 待校验的节点列表
        :return: (合法节点列表, 错误码)
        """
        # 预处理：去重、过滤无效值
        pure_nodes = self._deduplicate_and_sort(nodes)
        total_set = set(self._total_nodes)
        abnormal_set = set(self._abnormal_nodes)

        
        # 校验1：是否在总节点中（基础合法性）
        not_in_total = [node for node in pure_nodes if node not in total_set]
        if not_in_total:
            logger.debug(f"新增异常节点不在总节点中 → {sorted(not_in_total)}")
            return [], NodePoolErrorCode.NODE_NOT_IN_TOTAL

        # 校验2：是否已为异常节点
        already_abnormal = [node for node in pure_nodes if node in abnormal_set]
        if already_abnormal:
            logger.debug(f"新增异常节点节点已为异常 → {sorted(already_abnormal)}")
            return [], NodePoolErrorCode.NODE_ALREADY_ABNORMAL

        # 所有校验通过，返回合法节点
        return pure_nodes, NodePoolErrorCode.SUCCESS

        # ------------------------------ 新增私有工具方法：写入 slots_running_nodes 文件 ------------------------------
    def _write_slots_running_nodes_file(self, slots: int = 8):
        """将当前运行节点按 "节点名 slots=8" 格式写入 slots_running_nodes 文件"""

        if not isinstance(slots, int) or slots < 1:
            err_msg = f"slots 参数非法：必须为正整数 → 输入：{slots}"
            logger.error(err_msg)
            raise ValueError(err_msg)
    
        slots_file = self.slots_filepath

        try:
            # 2. 构建文件内容：每个运行节点一行，格式 "节点名 slots=8"
            running_nodes = self._running_nodes  # 通过属性获取最新运行节点（去重排序后）
            file_content = [f"{node} slots={slots}" for node in running_nodes]

            # 3. 覆盖写入文件（空运行节点生成空文件，保持文件存在性）
            with open(slots_file, "w", encoding="utf-8") as f:
                f.write("\n".join(file_content))

            # 4. 日志提示（仅输出关键信息，避免冗余）
            logger.debug(f"slots_running_nodes 文件写入成功：{len(running_nodes)} 个运行节点 → {slots_file}")
            return slots_file
        except PermissionError as e:
            # 权限错误单独包装，提示更精准
            err_msg = f"写入 slots_running_nodes 文件失败：权限不足 → 路径：{slots_file}"
            logger.error(f"{err_msg}，原错误：{str(e)}")
            raise RuntimeError(err_msg) from e  # 抛出异常，直接失败
        except Exception as e:
            # 其他异常统一包装，保留原异常链
            err_msg = f"写入 slots_running_nodes 文件失败 → 路径：{slots_file}"
            logger.error(f"{err_msg}，原错误：{str(e)}")
            raise RuntimeError(err_msg) from e  # 抛出异常，直接失败
        
    def _persist_all_nodes(self) -> List[Path]:
        """
        持久化所有节点列表到文件（适配已定义的文件路径成员变量）
        存储路径：通过 self.xxx_nodes_filepath 读取预定义路径

        :return: List[Path] - 所有成功持久化的文件路径列表
        """
        success_paths: List[Path] = []  # 存储成功持久化的路径
        # 核心修改：复用预定义的文件路径成员变量，避免手动拼接
        # 格式：(节点类型名称, 节点列表, 预定义文件路径)
        node_data_path_map: List[Tuple[str, List[str], Path]] = [
            ("total_nodes", self._total_nodes, self.total_nodes_filepath),
            ("running_nodes", self._running_nodes, self.running_nodes_filepath),
            ("backup_nodes", self._backup_nodes, self.backup_nodes_filepath),
            ("abnormal_nodes", self._abnormal_nodes, self.abnormal_nodes_filepath),
            ("normal_nodes", self._normal_nodes, self.normal_nodes_filepath),
        ]

        for node_type, nodes, file_path in node_data_path_map:
            try:
                # 写入文件（空列表会写入空文件，符合持久化逻辑）
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(nodes))
                logger.debug(f"持久化 {node_type} 成功 → {file_path.resolve()}")
                success_paths.append(file_path.resolve())  # 存储绝对路径，便于外部使用
            except PermissionError as e:
                logger.error(f"持久化 {node_type} 失败：权限不足 → {file_path.resolve()}")
            except OSError as e:
                logger.error(f"持久化 {node_type} 失败：系统错误（{str(e)}）→ {file_path.resolve()}")
            except Exception as e:
                logger.error(f"持久化 {node_type} 失败：未知错误（{str(e)}）→ {file_path.resolve()}")

        return success_paths  # 返回成功持久化的路径列表

    # ------------------------------ 公共接口（只读属性：防止外部修改私有变量） ------------------------------
    @property
    def total_nodes(self) -> List[str]:
        """获取总节点列表（返回副本，外部无法修改原数据）"""
        with self._lock:
            return self._total_nodes.copy()

    @property
    def running_nodes(self) -> List[str]:
        """获取运行中节点列表（返回副本，外部无法修改原数据）"""
        with self._lock:
            return self._running_nodes.copy()

    @property
    def backup_nodes(self) -> List[str]:
        """获取备用节点列表（返回副本，外部无法修改原数据）"""
        with self._lock:
            return self._backup_nodes.copy()

    @property
    def abnormal_nodes(self) -> List[str]:
        """获取异常节点列表（返回副本，外部无法修改原数据）"""
        with self._lock:
            return self._abnormal_nodes.copy()

    @property
    def normal_nodes(self) -> List[str]:
        """获取正常节点列表（返回副本，外部无法修改原数据）"""
        with self._lock:
            return self._normal_nodes.copy()

    @property
    def total_nodes_file(self) -> Path:
        """获取总节点文件路径（返回 Path 对象，外部仅可读，无法修改原路径）"""
        with self._lock:
            # Path 是不可变对象，直接返回即可（外部修改会创建新 Path，不影响内部属性）
            return self.total_nodes_filepath

    @property
    def running_nodes_file(self) -> Path:
        """获取运行中节点文件路径（返回 Path 对象，外部仅可读，无法修改原路径）"""
        with self._lock:
            return self.running_nodes_filepath

    @property
    def backup_nodes_file(self) -> Path:
        """获取备用节点文件路径（返回 Path 对象，外部仅可读，无法修改原路径）"""
        with self._lock:
            return self.backup_nodes_filepath

    @property
    def abnormal_nodes_file(self) -> Path:
        """获取异常节点文件路径（返回 Path 对象，外部仅可读，无法修改原路径）"""
        with self._lock:
            return self.abnormal_nodes_filepath

    @property
    def normal_nodes_file(self) -> Path:
        """获取正常节点文件路径（返回 Path 对象，外部仅可读，无法修改原路径）"""
        with self._lock:
            return self.normal_nodes_filepath

    # ------------------------------ 核心功能接口（用户明确要求） ------------------------------
    def reset_node_pool(
        self,
        total_nodes: list[str],
        abnormal_nodes: list[str] | None = None,
    ) -> None:
        """
        Reset node pool using new cluster state.

        Args:
            total_nodes: all cluster nodes
            abnormal_nodes: abnormal nodes in cluster
        """

        with self._lock:

            logger.warning("[NodePool] Reset node pool")

            # ---------- 更新 total ----------
            self._total_nodes = self._deduplicate_and_sort(total_nodes)

            # ---------- 清空运行状态 ----------
            self._running_nodes = []
            self._last_running_nodes = []

            # ---------- 更新 abnormal ----------
            self._abnormal_nodes = self._deduplicate_and_sort(abnormal_nodes or [])

            # ---------- 校验 ----------
            abnormal_set = set(self._abnormal_nodes)
            total_set = set(self._total_nodes)
            invalid = abnormal_set - total_set
            if invalid:
                raise ValueError(
                    f"abnormal nodes not in total_nodes: {invalid}"
                )

            # ---------- 重新计算 normal ----------
            self._sync_normal_nodes()

            # ---------- 重新计算 backup ----------
            self._backup_nodes = self._deduplicate_and_sort(list(total_set - abnormal_set))

            # ---------- 约束校验 ----------
            self._validate_and_fix_constraints()

            # ---------- 持久化 ----------
            self._persist_all_nodes()

            logger.info(
                "[NodePool] Reset finished: "
                f"total={len(self._total_nodes)} "
                f"backup={len(self._backup_nodes)} "
                f"abnormal={len(self._abnormal_nodes)}"
            )

    def add_total_nodes(self, nodes: Union[str, List[str]]) -> Optional[List[str]]:
        """
        【唯一添加总节点的接口】总节点仅通过此接口修改，内部流转不改变
        新增总节点自动分配至备用节点（符合约束：未分配节点→备用）

        :param nodes: 待添加的总节点（单个节点字符串或节点列表）
        :return: 实际新增的节点列表；无新增节点时返回 None
        """
        with self._lock:
            # 预处理：转为列表、去重、过滤无效值
            input_nodes = [nodes] if isinstance(nodes, str) else nodes
            pure_nodes = self._deduplicate_and_sort(input_nodes)
            total_set = set(self._total_nodes)

            # 筛选新增节点（排除已存在于总节点的节点）
            new_total_nodes = [node for node in pure_nodes if node not in total_set]
            if not new_total_nodes:
                logger.info("添加总节点：无新增有效节点（已存在或无效）")
                return None

            # 核心操作：添加到总节点 + 自动分配至备用节点
            self._total_nodes.extend(new_total_nodes)
            self._total_nodes = self._deduplicate_and_sort(self._total_nodes)
            self._backup_nodes.extend(new_total_nodes)
            self._backup_nodes = self._deduplicate_and_sort(self._backup_nodes)

            # 校验约束 + 同步正常节点
            self._validate_and_fix_constraints()

            # 日志输出
            logger.info(f"添加总节点成功：新增 {len(new_total_nodes)} 个 → {sorted(new_total_nodes)}")
            logger.info(f"当前总节点状态：{len(self._total_nodes)} 个 → {self._total_nodes}")
            logger.info(f'add_total_nodes增加持久化落盘操作_persist_all_nodes')
            self._persist_all_nodes()
            return new_total_nodes


    def add_normal_nodes(self, nodes: Union[str, List[str]]) -> Tuple[NodePoolErrorCode, bool, List[str]]:
        """
        将节点标记为正常节点（从异常中恢复，放入备用节点池）

        流转逻辑：
            1. 节点必须在总节点中，否则返回 NODE_NOT_IN_TOTAL。
            2. 筛选出当前为异常的节点，将其从异常列表中移除，并加入备用列表。
            3. 已在正常池（备用/运行中）的节点被忽略，不产生错误。
            4. 操作后自动校验约束并持久化。

        Args:
            nodes: 单个节点字符串或节点列表

        Returns:
            (错误码, 是否执行了实际恢复操作, 实际恢复的节点列表)
        """
        with self._lock:
            # 1. 预处理：去重、过滤空值
            input_nodes = [nodes] if isinstance(nodes, str) else nodes
            pure_nodes = self._deduplicate_and_sort(input_nodes)
            total_set = set(self._total_nodes)
            abnormal_set = set(self._abnormal_nodes)

            # 2. 校验节点是否在总节点中
            not_in_total = [node for node in pure_nodes if node not in total_set]
            if not_in_total:
                logger.error(f"标记正常节点失败：节点不在总节点中 → {sorted(not_in_total)}")
                return NodePoolErrorCode.NODE_NOT_IN_TOTAL, False, []

            # 3. 筛选需要恢复的异常节点
            nodes_to_restore = [node for node in pure_nodes if node in abnormal_set]
            if not nodes_to_restore:
                logger.info("标记正常节点：无异常节点需要恢复")
                return NodePoolErrorCode.SUCCESS, False, []

            # 4. 执行流转：从异常移除，加入备用
            self._abnormal_nodes = self._deduplicate_and_sort(list(abnormal_set - set(nodes_to_restore)))
            self._backup_nodes.extend(nodes_to_restore)
            self._backup_nodes = self._deduplicate_and_sort(self._backup_nodes)

            # 5. 同步正常节点（派生属性）并校验所有约束
            self._sync_normal_nodes()
            self._validate_and_fix_constraints()

            # 6. 持久化
            self._persist_all_nodes()

            logger.info(f"标记正常节点成功：恢复 {len(nodes_to_restore)} 个异常节点 → {sorted(nodes_to_restore)}")
            
            return NodePoolErrorCode.SUCCESS, True, nodes_to_restore
    
    def add_abnormal_nodes(self, nodes: Union[str, List[str]]) -> Tuple[bool, bool]:
        """
        新增异常节点（严格遵循流转规则和冲突处理规则）
        流转逻辑：备用节点 → 异常节点；正常节点同步减少（因 abnormal 增加）
        冲突处理：若节点在运行中 → 清空运行中节点 + 合并运行节点到备用节点

        :param nodes: 待标记为异常的节点（单个节点字符串或节点列表）
        :return: (是否存在有效故障节点, 是否触发 ABNORMAL_IN_RUNNING 状态)
            - 第一个值：True = 存在有效节点并已转为故障节点；False = 无有效节点或校验失败
            - 第二个值：True = 触发运行中节点冲突（ABNORMAL_IN_RUNNING）；False = 未触发该状态
        """
        with self._lock:
            # 预处理：转为列表
            input_nodes = [nodes] if isinstance(nodes, str) else nodes

            # 步骤1：校验节点合法性
            valid_nodes, err_code = self._validate_abnormal_candidates(input_nodes)
            if err_code != NodePoolErrorCode.SUCCESS:
                return False, False, None, None
            if not valid_nodes:
                logger.debug("节点池中无新增异常节点")
                return False, False, None, None

            running_set = set(self._running_nodes)
            runing_conflict_nodes = set(valid_nodes) & running_set
            has_cleared_running = False
            if runing_conflict_nodes:
                # 触发冲突处理：返回错误码 + 清空运行中节点
                logger.error(f"冲突触发：新增异常节点包含运行中节点 → {sorted(runing_conflict_nodes)}")
                has_cleared_running = True
                self._last_running_nodes.clear()
                self._last_running_nodes = self._running_nodes.copy()
                self._running_nodes.clear()

            # # 备用节点列表移除
            # backup_set = set(self._backup_nodes)
            # self._backup_nodes = self._deduplicate_and_sort(list(backup_set - set(valid_nodes)))

            # 加入异常节点
            self._abnormal_nodes.extend(valid_nodes)
            self._abnormal_nodes = self._deduplicate_and_sort(self._abnormal_nodes)

            self._sync_normal_nodes()
            self._sync_backup_nodes()
            # 步骤4：校验约束 + 同步正常节点
            self._validate_and_fix_constraints()

            self._persist_all_nodes()

            # 步骤5：日志输出 + 返回结果
            logger.info(f"新增异常节点成功：新增 {len(valid_nodes)} 个 → {sorted(valid_nodes)}")

            if has_cleared_running:
                return True, True, valid_nodes, sorted(runing_conflict_nodes)
            else:
                return True, False, valid_nodes, None
            
    def add_abnormal_runing_nodeno(self, nodeno) -> Tuple[bool, bool]:
        """
        新增异常节点（严格遵循流转规则和冲突处理规则）
        流转逻辑：备用节点 → 异常节点；正常节点同步减少（因 abnormal 增加）
        冲突处理：若节点在运行中 → 清空运行中节点 + 合并运行节点到备用节点

        :param nodes: 待标记为异常的节点（单个节点字符串或节点列表）
        :return: (是否存在有效故障节点, 是否触发 ABNORMAL_IN_RUNNING 状态)
            - 第一个值：True = 存在有效节点并已转为故障节点；False = 无有效节点或校验失败
            - 第二个值：True = 触发运行中节点冲突（ABNORMAL_IN_RUNNING）；False = 未触发该状态
        """

        assert len(self._running_nodes) and len(self._running_nodes) > nodeno
    
        err_node = self._running_nodes[nodeno]
        logger.error(f"外部检测：运行中故障节点 → {err_node}")
        self.len_running_abnormal_nodes += 1 
        return self.add_abnormal_nodes(err_node)
    
    def add_abnormal_runing_error_nodes(self, err_nodes) -> Tuple[bool, bool]:
        """
        新增异常节点（严格遵循流转规则和冲突处理规则）
        流转逻辑：备用节点 → 异常节点；正常节点同步减少（因 abnormal 增加）
        冲突处理：若节点在运行中 → 清空运行中节点 + 合并运行节点到备用节点

        :param nodes: 待标记为异常的节点（单个节点字符串或节点列表）
        :return: (是否存在有效故障节点, 是否触发 ABNORMAL_IN_RUNNING 状态)
            - 第一个值：True = 存在有效节点并已转为故障节点；False = 无有效节点或校验失败
            - 第二个值：True = 触发运行中节点冲突（ABNORMAL_IN_RUNNING）；False = 未触发该状态
        """
    
        if isinstance(err_nodes, str):
            err_nodes = [err_nodes]
        
        # 校验节点是否在运行节点中
        valid_err_nodes = [node for node in err_nodes if node in self._running_nodes]
        
        if not valid_err_nodes:
            logger.warning(f"传入的故障节点 {err_nodes} 不在当前运行节点中")
            return False, False
        logger.info(f">= len(self._running_nodes):{len(self._running_nodes)}; err_nodes:{err_nodes} <=")
        logger.warning(f"外部检测：运行中故障节点 → {valid_err_nodes}")
        self.len_running_abnormal_nodes = len(valid_err_nodes)
        # 调用 add_abnormal_nodes 并只返回前两个值
        return self.add_abnormal_nodes(valid_err_nodes)


    def remove_abnormal_nodes(self, nodes: Union[str, List[str]], target_pool: str = "backup") -> Tuple[NodePoolErrorCode, bool]:
        """
        删除异常节点（恢复为正常节点，流转至备用/运行中节点）
        流转逻辑：异常节点 → 目标节点池（默认备用）；正常节点同步增加（因 abnormal 减少）

        :param nodes: 待恢复的异常节点（单个节点字符串或节点列表）
        :param target_pool: 恢复目标池（支持 "backup" 备用 / "running" 运行中，默认 "backup"）
        :return: (错误码, 是否恢复成功)
        """
        with self._lock:
            logger.debug(f'>= remove_ab_nodes <=')
            # 步骤1：校验目标池合法性
            if target_pool not in ["backup", "running"]:
                logger.error(f"恢复异常节点失败：无效目标池 → {target_pool}（仅支持 backup/running）")
                return NodePoolErrorCode.INVALID_TARGET_POOL, False

            # 步骤2：预处理节点列表
            input_nodes = [nodes] if isinstance(nodes, str) else nodes
            pure_nodes = self._deduplicate_and_sort(input_nodes)
            abnormal_set = set(self._abnormal_nodes)
            total_set = set(self._total_nodes)

            # 步骤3：校验节点合法性（在总节点中 + 在异常中）
            # 校验3.1：是否在总节点中
            not_in_total = [node for node in pure_nodes if node not in total_set]
            if not_in_total:
                logger.warning(f"恢复异常节点不合法：节点不在总节点中 → {sorted(not_in_total)}")
                return NodePoolErrorCode.NODE_NOT_IN_TOTAL, False

            # 校验3.2：是否在异常节点中
            valid_nodes = [node for node in pure_nodes if node in abnormal_set]
            if not valid_nodes:
                logger.info("恢复异常节点：无有效节点可操作（节点不在异常中）")
                return NodePoolErrorCode.NO_VALID_NODE, False

            # 步骤4：核心流转逻辑（从异常移除 → 加入目标池）
            logger.info(f"开始恢复异常节点：{sorted(valid_nodes)} → 目标池：{target_pool}")
            # 从异常节点移除
            self._abnormal_nodes = self._deduplicate_and_sort(list(abnormal_set - set(valid_nodes)))
            # 加入目标池
            if target_pool == "running":
                self._running_nodes.extend(valid_nodes)
                self._running_nodes = self._deduplicate_and_sort(self._running_nodes)
            else:
                self._backup_nodes.extend(valid_nodes)
                self._backup_nodes = self._deduplicate_and_sort(self._backup_nodes)

            # 步骤5：校验约束 + 同步正常节点
            self._validate_and_fix_constraints()

            # 步骤6：日志输出 + 返回结果
            logger.info(f"恢复异常节点成功：{len(valid_nodes)} 个 → {sorted(valid_nodes)}")
            logger.info(f"流转后状态：")
            logger.info(f"  {target_pool}节点：{len(self._running_nodes) if target_pool=='running' else len(self._backup_nodes)} 个 → {self._running_nodes if target_pool=='running' else self._backup_nodes}")
            logger.info(f"  异常节点：{len(self._abnormal_nodes)} 个 → {self._abnormal_nodes}")
            logger.info(f"  正常节点：{len(self._normal_nodes)} 个 → {self._normal_nodes}")

            return NodePoolErrorCode.SUCCESS, True
        
    def validate_running_state_consistency(self, running_host: List[str]) -> Dict[str, Union[bool, List[str]]]:
        """
        校验入参运行节点与节点池内 _running_nodes 的一致性，并返回差异节点

        核心逻辑：
        1. 入参预处理（去重、排序、过滤无效值，与节点池内部节点处理逻辑一致）
        2. 线程安全访问内部 _running_nodes
        3. 计算三类差异：无效节点（入参中不在总节点的节点）、新增节点（入参有但内部无）、缺失节点（内部有但入参无）
        4. 一致性判定：无无效/新增/缺失节点则视为一致

        :param running_host: 待校验的运行节点列表（支持重复、空值、无效节点，内部自动处理）
        :return: 结构化校验结果
            - consistent: bool - 是否一致
            - invalid_nodes: List[str] - 入参中不在总节点的无效节点（违反约束1）
            - added_nodes: List[str] - 入参有但节点池 _running_nodes 无的新增节点
            - missing_nodes: List[str] - 节点池 _running_nodes 有但入参无的缺失节点
        """
        # 1. 入参预处理：去重、排序、过滤空值（与内部节点处理逻辑对齐）
        processed_input = self._deduplicate_and_sort(
            [node for node in (running_host or []) if node and isinstance(node, str)]
        )

        # 2. 线程安全访问内部 _running_nodes（避免多线程并发修改导致的脏读）
        with self._lock:
            internal_running = self._running_nodes.copy()  # 拷贝一份，避免外部修改内部状态

        # 3. 计算差异节点
        total_set = set(self._total_nodes)
        input_set = set(processed_input)
        internal_set = set(internal_running)

        # 无效节点：入参存在但不在总节点（违反约束1）
        invalid_nodes = self._deduplicate_and_sort(list(input_set - total_set))
        # 有效入参节点：入参中属于总节点的部分
        valid_input_set = input_set & total_set
        # 新增节点：有效入参有但内部无
        added_nodes = self._deduplicate_and_sort(list(valid_input_set - internal_set))
        # 缺失节点：内部有但有效入参无
        missing_nodes = self._deduplicate_and_sort(list(internal_set - valid_input_set))

        # 4. 判定是否一致（无无效/新增/缺失节点）
        consistent = len(invalid_nodes) == 0 and len(added_nodes) == 0 and len(missing_nodes) == 0

        # 5. 日志输出（差异节点非空时记录，便于问题定位）
        if not consistent:
            logger.warning(
                f"运行节点状态不一致："
                f"无效节点={invalid_nodes}，"
                f"新增节点={added_nodes}，"
                f"缺失节点={missing_nodes}"
            )
        else:
            logger.debug(f"运行节点状态一致：入参与节点池 _running_nodes 完全匹配")

        # 6. 返回结构化结果
        return {
            "consistent": consistent,
            "invalid_nodes": invalid_nodes,
            "added_nodes": added_nodes,
            "missing_nodes": missing_nodes
        }
    
    # ------------------------------ 扩展功能接口（运行中节点管理，符合之前规则） ------------------------------
    def apply_node_num_resources(self, nodes_num, slots):
        """
        从备用节点中分配指定数量的节点到运行节点（集成黑名单过滤）

        分配策略（黑名单感知）：
          1. 优先使用不在黑名单中的健康备用节点
          2. 健康节点不足时，使用黑名单中的降级备用节点（非致命）
          3. 致命故障节点绝对不参与分配
          4. 支持回收模式：上次运行节点优先复用，不可用时从备用节点替换

        :param nodes_num: 需要分配的节点数量
        :param slots: 每个节点的 slot 数量
        :return: (第一个运行节点名称, slots文件路径)；分配失败返回 (None, None)
        """
        with self._lock:
            # ======== 黑名单过滤：将备用节点分为健康、降级、致命三组 ========
            healthy_backup, degraded_backup, rejected_backup = self._get_available_nodes_with_blacklist(
                self._backup_nodes
            )
            total_available = len(healthy_backup) + len(degraded_backup)

            # ======== 可用节点数量校验 ========
            if nodes_num > total_available:
                logger.error(
                    f"Insufficient node pool resources: "
                    f"need {nodes_num}, healthy {len(healthy_backup)}, "
                    f"degraded {len(degraded_backup)}, fatal excluded {len(rejected_backup)}"
                )

                logger.info(f"  Normal nodes   : {len(self._normal_nodes)} → {compress_nodes(self._normal_nodes)}")
                logger.info(f"  Abnormal nodes : {len(self._abnormal_nodes)} → {compress_nodes(self._abnormal_nodes)}")
                logger.info(f"  Running nodes  : {len(self._running_nodes)} → {compress_nodes(self._running_nodes)}")
                logger.info(f"  Backup nodes   : {len(self._backup_nodes)} (healthy={len(healthy_backup)}, degraded={len(degraded_backup)}, fatal={len(rejected_backup)})")

                return None, None

            logger.info(
                f"Backup nodes before allocation: "
                f"{len(self._backup_nodes)} (healthy={len(healthy_backup)}, degraded={len(degraded_backup)})"
            )

            # Step 3: Core operation: allocate nodes_num nodes from backup nodes
            new_running_nodes = []

            logger.info(
                f"Last running nodes: "
                f"{len(self._last_running_nodes)} → {compress_nodes(self._last_running_nodes) if self._last_running_nodes else '[]'}"
            )

            if self._last_running_nodes and nodes_num == len(self._last_running_nodes):
                # ======== 回收模式：优先复用上次运行的节点 ========
                idx = 0
                set_last_running = set(self._last_running_nodes)
                set_backup = set(self._backup_nodes)
                set_healthy = set(healthy_backup)
                set_degraded = set(degraded_backup)

                # 构建备用替换池
                standby_healthy = self._deduplicate_and_sort(
                    [n for n in list(set_backup - set_last_running) if n in set_healthy]
                )
                standby_degraded = self._deduplicate_and_sort(
                    [n for n in list(set_backup - set_last_running) if n in set_degraded]
                )

                # 第一轮：统计本轮需要替换的节点数
                # 需要替换 = 上次运行节点中 非健康 的节点（降级 + 致命 + 不在备用池）
                replacements_needed = sum(
                    1 for n in self._last_running_nodes
                    if not (n in set_backup and n in set_healthy)
                )

                # 根据健康备用节点是否充足，决定替换策略
                if len(standby_healthy) >= replacements_needed:
                    # ★ 健康备用节点充足，坚决不使用任何降级/致命节点
                    standby_nodes = standby_healthy
                    allow_degraded_reuse = False
                    logger.info(
                        f"回收模式: 需要 {replacements_needed} 个替换节点, "
                        f"standby_healthy 有 {len(standby_healthy)} 个, 充足, "
                        f"坚决不使用降级/致命节点"
                    )
                else:
                    # 健康备用节点不足，允许使用降级备用节点补充
                    standby_nodes = standby_healthy + standby_degraded
                    allow_degraded_reuse = True
                    logger.warning(
                        f"回收模式: 需要 {replacements_needed} 个替换节点, "
                        f"standby_healthy 仅 {len(standby_healthy)} 个, 不足, "
                        f"启用 {len(standby_degraded)} 个降级节点补充"
                    )

                for node_name in self._last_running_nodes:
                    if len(new_running_nodes) >= nodes_num:
                        break

                    if node_name in set_backup:
                        if node_name in set_healthy:
                            # 健康节点，直接复用
                            new_running_nodes.append(node_name)
                            self._backup_nodes.remove(node_name)
                            logger.debug(f"复用健康节点: {node_name}")
                        elif node_name in set_degraded:
                            # 降级节点（黑名单中非致命）
                            if allow_degraded_reuse:
                                # 健康备用不足，允许降级复用
                                logger.warning(
                                    f"复用降级节点（黑名单中非致命，standby_healthy不足）: {node_name}"
                                )
                                new_running_nodes.append(node_name)
                                self._backup_nodes.remove(node_name)
                            else:
                                # ★ 健康备用充足，用健康备用替换降级节点，坚决不使用
                                logger.warning(
                                    f"上次运行节点 {node_name} 为降级黑名单节点, "
                                    f"使用健康备用节点替换（standby_healthy充足）"
                                )
                                self._pick_and_assign_standby(
                                    standby_nodes, new_running_nodes, node_name, idx
                                )
                        else:
                            # 致命故障节点（在rejected中），使用备用节点替换
                            logger.warning(
                                f"上次运行节点 {node_name} 为致命黑名单节点，使用备用节点替换"
                            )
                            self._pick_and_assign_standby(
                                standby_nodes, new_running_nodes, node_name, idx
                            )
                    else:
                        # 节点不在备用池中，使用备用节点替换
                        self._pick_and_assign_standby(
                            standby_nodes, new_running_nodes, node_name, idx
                        )

                    idx += 1

                logger.info(
                    f"Recycled allocation: {nodes_num} nodes → "
                    f"{compress_nodes(new_running_nodes)}"
                )

            else:
                # ======== 新分配模式：优先健康节点，不足时补充降级节点 ========
                # 先从健康节点中选取
                selected_from_healthy = healthy_backup[:nodes_num]
                shortage = nodes_num - len(selected_from_healthy)

                # 健康节点不足时，从降级节点中补充
                selected_from_degraded = []
                if shortage > 0 and degraded_backup:
                    selected_from_degraded = degraded_backup[:shortage]
                    logger.warning(
                        f"健康备用节点不足（需要 {nodes_num}，健康 {len(healthy_backup)}），"
                        f"使用 {len(selected_from_degraded)} 个降级节点补充: "
                        f"{sorted(selected_from_degraded)}"
                    )

                new_running_nodes = selected_from_healthy + selected_from_degraded

                # 从备用节点中移除已分配的节点
                for node in new_running_nodes:
                    self._backup_nodes.remove(node)

                logger.info(
                    f"New allocation: {nodes_num} nodes → "
                    f"{compress_nodes(new_running_nodes)}"
                )

            self._running_nodes.extend(new_running_nodes)

            # Step 5: Validate constraints to ensure node pool consistency
            self._validate_and_fix_constraints()

            slots_file = self._write_slots_running_nodes_file(slots)

            self._persist_all_nodes()

            logger.info(
                f"Current running nodes: "
                f"{len(self._running_nodes)} → {compress_nodes(self._running_nodes)}"
            )

            return self._running_nodes[0] if self._running_nodes else None, slots_file

    def _pick_and_assign_standby(
        self,
        standby_nodes: List[str],
        new_running_nodes: List[str],
        original_node: str,
        idx: int,
    ) -> None:
        """
        从备用替换池中选取一个节点并分配（内部辅助方法）

        替换策略：优先使用健康备用节点，健康不足时使用降级备用节点。
        选取后会从 standby_nodes 和 self._backup_nodes 中移除。

        :param standby_nodes: 备用替换池（健康优先排序，会被修改）
        :param new_running_nodes: 新运行节点列表（会被追加）
        :param original_node: 被替换的原始节点名称（用于日志和通知）
        :param idx: 节点索引（用于计算 rank）
        """
        if not standby_nodes:
            logger.error(f"无可用备用替换节点，无法替换 {original_node}")
            return

        popped_node = standby_nodes.pop(0)
        self._backup_nodes.remove(popped_node)
        new_running_nodes.append(popped_node)

        start = idx * 8
        end = start + 8
        ranks = list(range(start, end))

        send_data_to_server(popped_node, 16881, original_node, ranks)

        logger.info(
            f"Node replacement: "
            f"{popped_node} replaced failed node {original_node}"
        )
        
    def apply_node_list_resources(self, target_nodes, slots):
        """
        从备用节点中分配指定的节点列表到运行节点（集成黑名单过滤）

        分配策略（黑名单感知）：
          1. 目标节点为健康备用节点 → 直接使用
          2. 目标节点为降级备用节点（非致命黑名单） → 使用并警告
          3. 目标节点为致命黑名单节点或不在备用池中 → 用备用节点替换（健康优先）
          4. 替换备用节点时优先选择健康节点，不足时使用降级节点

        :param target_nodes: list，要分配的目标节点名称列表（如 ["node1", "node2"]）
        :param slots: 每个节点的 slot 数量
        :return: 第一个运行节点名称, slots文件路径；分配失败返回 (None, None)
        """
        with self._lock:
            if not isinstance(target_nodes, list) or len(target_nodes) == 0:
                logger.error(f"目标节点列表无效：必须是非空列表，当前输入：{target_nodes}")
                return None, None
            
            # ======== 黑名单过滤备用节点 ========
            healthy_backup, degraded_backup, rejected_backup = self._get_available_nodes_with_blacklist(
                self._backup_nodes
            )
            total_available = len(healthy_backup) + len(degraded_backup)

            if len(target_nodes) > total_available:
                logger.error(
                    f"可用备用节点不足: 需要 {len(target_nodes)}, "
                    f"健康 {len(healthy_backup)}, 降级 {len(degraded_backup)}, "
                    f"致命剔除 {len(rejected_backup)}"
                )
                return None, None
            
            logger.info(
                f"分配前备用节点: {len(self._backup_nodes)} 个 "
                f"(健康={len(healthy_backup)}, 降级={len(degraded_backup)}, 致命={len(rejected_backup)})"
            )

            # 构建快速查找集合
            healthy_set = set(healthy_backup)
            degraded_set = set(degraded_backup)

            # 构建有序的备用替换池（健康优先，降级在后）
            replacement_pool = list(healthy_backup) + list(degraded_backup)
            
            new_running_nodes = []
            
            # 遍历目标节点，按顺序分配
            idx = 0
            for node in target_nodes:
                if node in healthy_set:
                    # 目标节点为健康备用节点，直接使用
                    new_running_nodes.append(node)
                    self._backup_nodes.remove(node)
                    healthy_set.discard(node)
                    if node in replacement_pool:
                        replacement_pool.remove(node)
                    logger.debug(f"分配健康备用节点: {node}")
                elif node in degraded_set:
                    # 目标节点为降级备用节点（非致命黑名单），使用但警告
                    logger.warning(
                        f"目标节点 {node} 在黑名单中（非致命），降级使用"
                    )
                    new_running_nodes.append(node)
                    self._backup_nodes.remove(node)
                    degraded_set.discard(node)
                    if node in replacement_pool:
                        replacement_pool.remove(node)
                else:
                    # 目标节点不在备用池中 或 为致命黑名单节点，使用替换节点
                    if not replacement_pool:
                        logger.error(f"无可用替换节点，无法分配节点替代 {node}")
                        break
                    
                    # 从替换池中选取（健康优先）
                    popped_node = replacement_pool.pop(0)
                    self._backup_nodes.remove(popped_node)
                    new_running_nodes.append(popped_node)

                    # 判断替换节点是降级节点，记录警告
                    if popped_node in degraded_set:
                        logger.warning(f"替换节点 {popped_node} 为黑名单降级节点")
                        degraded_set.discard(popped_node)
                    else:
                        healthy_set.discard(popped_node)

                    start = idx * 8
                    end = start + 8
                    ranks = list(range(start, end))
                    send_data_to_server(popped_node, 16881, node, ranks)
                    logger.info(f"节点:{popped_node} 替换目标节点:{node}")
                
                idx += 1
            
            self._running_nodes.extend(new_running_nodes)
            
            # 校验约束（确保节点池状态符合规则）
            self._validate_and_fix_constraints()
            
            # 持久化配置
            slots_file = self._write_slots_running_nodes_file(slots)
            self._persist_all_nodes()
            
            # 日志输出 + 返回结果
            logger.info(f"添加运行中节点成功：新增 {len(new_running_nodes)} 个 → {sorted(new_running_nodes)}")
            logger.info(f"当前运行中节点状态：{len(self._running_nodes)} 个 → {self._running_nodes}")
            logger.info(f"分配后备用节点状态：{len(self._backup_nodes)} 个 → {sorted(self._backup_nodes)}")
            
            # 返回第一个运行节点（与原逻辑一致）和slots文件路径
            return (self._running_nodes[0] if self._running_nodes else None), slots_file
    
    def release_runing_nodes(self) -> Tuple[List[str], Optional[Path]]:
        with self._lock:
            self._backup_nodes = self._deduplicate_and_sort(list(set(self._running_nodes) | set(self._backup_nodes)))
            if self._running_nodes:
                self._last_running_nodes.clear()
                self._last_running_nodes = self._running_nodes.copy()
                self._running_nodes.clear()
                self._persist_all_nodes()
    
    # ------------------------------ 辅助功能接口（持久化+状态查看） ------------------------------
    def persist_all_nodes(self) -> List[Path]:
        """
        持久化所有节点列表到文件（适配已定义的文件路径成员变量）
        存储路径：通过 self.xxx_nodes_filepath 读取预定义路径

        :return: List[Path] - 所有成功持久化的文件路径列表
        """
        with self._lock:
            return self._persist_all_nodes()

    def print_node_pool_status(self) -> None:
        """打印节点池完整状态（含规则校验结果，便于调试）"""
        with self._lock:
            # 规则校验结果
            rule1 = len(self._running_nodes) + len(self._backup_nodes) + len(self._abnormal_nodes) == len(self._total_nodes)
            rule2 = set(self._normal_nodes) == set(self._total_nodes) - set(self._abnormal_nodes)
            rule3 = len(set(self._running_nodes) & set(self._backup_nodes) & set(self._abnormal_nodes)) == 0

            print("=" * 120)
            print("节点池完整状态（核心规则校验）")
            print(f"  存储路径：{self._node_pool_path}")
            print(f"  总节点数：{len(self._total_nodes)} → {self._total_nodes}（仅 add_total_nodes 可修改）")
            print(f"  运行中节点数：{len(self._running_nodes)} → {self._running_nodes}（仅 add_running_nodes 可添加）")
            print(f"  备用节点数：{len(self._backup_nodes)} → {self._backup_nodes}")
            print(f"  异常节点数：{len(self._abnormal_nodes)} → {self._abnormal_nodes}")
            print(f"  正常节点数：{len(self._normal_nodes)} → {self._normal_nodes}")
            print(f"  规则1：total = running+backup+abnormal → {'✅ 成立' if rule1 else '❌ 不成立'}")
            print(f"  规则2：normal = total - abnormal → {'✅ 成立' if rule2 else '❌ 不成立'}")
            print(f"  规则3：running/backup/abnormal 无交集 → {'✅ 成立' if rule3 else '❌ 不成立'}")
            print("=" * 120)


# ------------------------------ 测试代码（验证所有核心规则） ------------------------------
if __name__ == "__main__":
    # 1. 初始化节点池（总节点=A,B,C,D，运行中=A，备用=B,C，异常=D）
    pool = NodePool(
        pool_root_path="./node_pool_data",
        total_nodes=["A", "B", "C", "D"],
        running_nodes=["A"],
        backup_nodes=["B", "C"],
        abnormal_nodes=["D"]
    )
    pool.print_node_pool_status()

    # 2. 新增异常节点（含运行中节点A，触发冲突）
    print("\n=== 测试：新增异常节点（A,B）===")
    err_code, has_cleared = pool.add_abnormal_nodes(["A", "B"])
    print(f"新增结果：错误码={err_code.code}，错误信息={err_code.msg}，清空运行中={has_cleared}")
    pool.print_node_pool_status()

    # 3. 恢复异常节点A到运行中
    print("\n=== 测试：恢复异常节点A到运行中 ===")
    err_code, has_recovered = pool.remove_abnormal_nodes("A", target_pool="running")
    print(f"恢复结果：错误码={err_code.code}，错误信息={err_code.msg}，恢复成功={has_recovered}")
    pool.print_node_pool_status()

    # 4. 添加总节点E
    print("\n=== 测试：添加总节点E ===")
    err_code, has_added = pool.add_total_nodes("E")
    print(f"添加结果：错误码={err_code.code}，错误信息={err_code.msg}，添加成功={has_added}")
    pool.print_node_pool_status()

    # 5. 持久化所有节点到文件
    print("\n=== 测试：持久化节点 ===")
    pool.persist_all_nodes()
