# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
HCU 训练节点黑名单机制（核心模块）

功能：
  - 故障记录：按节点记录故障类型、次数、详情，支持 Xid / 错误码
  - 评分排序：加权指数衰减评分，按可用性优先级排序
  - 节点分配：健康节点优先，不足时从黑名单降级补充，fatal 节点绝对剔除
  - 机架感知：短时多节点同时网络故障 → 判定为拓扑级故障，不上报个体
  - 线程安全：读写锁保护内存状态，异步落盘，原子写入 + 备份恢复
  - 自动白名单：连续健康运行一段时间后自动解除拉黑

使用方式：
    from node_management.node_blacklist_manager import BlacklistManager, FaultType

    manager = BlacklistManager.get_instance()
    manager.start()
    manager.report_fault("node-001", FaultType.HCU, error_code="43")
    result = manager.allocate_nodes(all_nodes, required_count=512)
    manager.stop()
"""

import json
import os
import fcntl
import shutil
import math
import time
import logging
import threading
from enum import IntEnum
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Set, Callable, Any

logger = logging.getLogger(__name__)


# ================================================================
#  一、配置与常量
# ================================================================

class FaultType(IntEnum):
    """故障类型枚举"""
    NETWORK = 1       # 网络故障
    HCU = 2           # HCU 硬件故障
    GRADIENT_INF = 3  # 训练梯度 INF / NAN
    MANUAL = 4        # 人为故障


class FaultSeverity(IntEnum):
    """故障严重性等级"""
    NORMAL = 0    # 普通故障，计入评分
    WARNING = 1   # 警告级，软拉黑
    FATAL = 2     # 致命故障，一票否决

FATAL_XID_CODES = {76, 61, 62, 63, 64, 65, 68}
WARNING_XID_CODES = {31, 32, 48, 49, 55}
FATAL_ERROR_CODES = {"HCU_RESET", "BAD_OP", "SDMA_QUEUE_ERROR", "CP_QUEUE_ERROR"}


# 故障类型权重配置：{FaultType: (权重, 默认严重性, 软拉黑阈值, 正式拉黑阈值, 描述)}
_FAULT_TYPE_DEFAULTS = {
    FaultType.NETWORK: (10.0, FaultSeverity.NORMAL, 3, 5,
                        "网络故障（网卡/光模块/交换机）"),
    FaultType.HCU: (100.0, FaultSeverity.FATAL, 1, 2,
                    "HCU硬件故障（显存/计算单元/PCIE）"),
    FaultType.GRADIENT_INF: (30.0, FaultSeverity.WARNING, 2, 5,
                             "训练梯度INF/NAN故障"),
    FaultType.MANUAL: (0.0, FaultSeverity.NORMAL, 999, 999,
                       "人为故障（不计入健康评分）"),
}


@dataclass
class BlacklistConfig:
    """黑名单全局配置，所有参数可按需调整"""
    # 故障衰减：半衰期 168 小时 = 7 天，经过一个半衰期分数减半
    decay_half_life_hours: float = 72.0

    # 软拉黑判定时间窗口（小时内），窗口内达阈值才软拉黑，防止偶发误杀
    soft_ban_window_hours: float = 1.0

    # 自动白名单化：连续健康运行多久（小时）后自动解除拉黑
    auto_whitelist_hours: float = 48.0

    # 机架级故障感知：时间窗口（秒）内多少节点同时报网络故障 → 判定为机架故障
    rack_fault_threshold: int = 5
    rack_fault_window_seconds: float = 120.0

    # INF 故障精准判定：连续多少个 micro-step 出现 INF 才判定为节点故障
    inf_fault_min_steps: int = 1

    # 持久化路径
    persistence_path: str = "/tmp/HCU_blacklist.json"
    persistence_backup_path: str = "/tmp/HCU_blacklist.json.bak"
    persistence_interval: float = 5.0  # 异步落盘间隔（秒）

    # 每种故障类型的权重与阈值，可通过此字段完全自定义
    fault_weights: Dict[int, float] = field(default_factory=dict)
    fault_severities: Dict[int, FaultSeverity] = field(default_factory=dict)
    fault_soft_ban_thresholds: Dict[int, int] = field(default_factory=dict)
    fault_auto_ban_thresholds: Dict[int, int] = field(default_factory=dict)

    # 正常运行时间惩罚参数
    # uptime_penalty_weight: 惩罚系数，0.5 表示最多在基础分上增加 50% 的惩罚
    #   - uptime=0（刚恢复就故障）:  penalty = 0.5 × base_score（最大惩罚）
    #   - uptime=τ（等于参考时间）:  penalty = 0.25 × base_score
    #   - uptime→∞（长期稳定）:       penalty → 0（无惩罚）
    # uptime_reference_hours: 参考时间常数（小时），默认 24 小时
    uptime_penalty_weight: float = 0.5
    uptime_reference_hours: float = 24.0

    # 内部计算的衰减系数（由半衰期自动算出，无需手动设置）
    decay_lambda: float = field(init=False)

    def __post_init__(self):
        self.decay_lambda = math.log(2) / max(self.decay_half_life_hours, 0.1)

    # ---------- 便捷查询方法 ----------

    def weight(self, ft: FaultType) -> float:
        return self.fault_weights.get(int(ft), _FAULT_TYPE_DEFAULTS[ft][0])

    def severity(self, ft: FaultType) -> FaultSeverity:
        return self.fault_severities.get(int(ft), _FAULT_TYPE_DEFAULTS[ft][1])

    def soft_ban_threshold(self, ft: FaultType) -> int:
        return self.fault_soft_ban_thresholds.get(int(ft), _FAULT_TYPE_DEFAULTS[ft][2])

    def auto_ban_threshold(self, ft: FaultType) -> int:
        return self.fault_auto_ban_thresholds.get(int(ft), _FAULT_TYPE_DEFAULTS[ft][3])

    def description(self, ft: FaultType) -> str:
        return _FAULT_TYPE_DEFAULTS[ft][4]


# ================================================================
#  二、数据模型
# ================================================================

@dataclass
class FaultRecord:
    """单条故障记录"""
    fault_type: FaultType
    timestamp: datetime
    description: str = ""
    error_code: Optional[str] = None
    HCU_id: Optional[int] = None
    micro_step: Optional[int] = None

    # 评分字段（由 BlacklistManager 在 report_fault 时计算写入）
    # base_score: 本条故障的基础权重分（= Weight(fault_type)）
    base_score: float = 0.0
    # uptime_hours: 本次故障发生前，节点的正常运行时间（小时），首条故障为 None
    uptime_hours: Optional[float] = None
    # uptime_penalty: 正常运行时间贡献的惩罚分
    #   uptime=0 → penalty = U × base_score (最大)
    #   uptime=τ → penalty = U × base_score / 2
    #   uptime→∞ → penalty → 0
    #   首次故障 → penalty = 0
    uptime_penalty: float = 0.0

    # --- 序列化 ---
    def to_dict(self) -> dict:
        return {
            "fault_type": int(self.fault_type),
            "timestamp": self.timestamp.isoformat(),
            "description": self.description,
            "error_code": self.error_code,
            "HCU_id": self.HCU_id,
            "micro_step": self.micro_step,
            "base_score": round(self.base_score, 2),
            "uptime_hours": round(self.uptime_hours, 2) if self.uptime_hours is not None else None,
            "uptime_penalty": round(self.uptime_penalty, 2),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "FaultRecord":
        return cls(
            fault_type=FaultType(d["fault_type"]),
            timestamp=datetime.fromisoformat(d["timestamp"]),
            description=d.get("description", ""),
            error_code=d.get("error_code"),
            HCU_id=d.get("HCU_id"),
            micro_step=d.get("micro_step"),
            base_score=d.get("base_score", 0.0),
            uptime_hours=d.get("uptime_hours"),
            uptime_penalty=d.get("uptime_penalty", 0.0),
        )


@dataclass
class NodeRecord:
    """单节点完整记录（内存中对应一个被拉黑的节点）"""
    node_name: str
    fault_records: List[FaultRecord] = field(default_factory=list)
    fatal: bool = False
    soft_banned: bool = False
    last_fault_time: Optional[datetime] = None
    last_healthy_time: Optional[datetime] = None
    fault_score: float = 0.0  # 节点当前的综合故障评分（包含时间衰减，动态更新）
    _dirty: bool = False

    @property
    def total_fault_count(self) -> int:
        return sum(1 for r in self.fault_records if r.fault_type != FaultType.MANUAL)

    def add_fault(self, record: FaultRecord) -> None:
        # 添加故障时自动计算 uptime_hours
        if self.fault_records and self.last_fault_time is not None:
            uptime_seconds = (record.timestamp - self.last_fault_time).total_seconds()
            print(uptime_seconds)
            uptime_h = max(uptime_seconds / 3600.0, 0.0)
            record.uptime_hours = uptime_h
        # else: 首条故障，uptime_hours 保持 None

        self.fault_records.append(record)
        if self.last_fault_time is None or record.timestamp > self.last_fault_time:
            self.last_fault_time = record.timestamp
        self._dirty = True

    def update_fault_score(self, score: float) -> None:
        self.fault_score = round(score, 2)
        self._dirty = True

    def mark_healthy(self, now: datetime = None) -> None:
        self.last_healthy_time = now or datetime.now()
        self._dirty = True

    def clear(self) -> None:
        self.fault_records.clear()
        self.fatal = False
        self.soft_banned = False
        self.last_fault_time = None
        self.fault_score = 0.0  # 清空时重置评分
        self._dirty = True

    # --- 序列化 ---
    def to_dict(self) -> dict:
        return {
            "node_name": self.node_name,
            "fatal": self.fatal,
            "soft_banned": self.soft_banned,
            "fault_score": self.fault_score,
            "total_fault_count": self.total_fault_count,
            "last_fault_time": self.last_fault_time.isoformat() if self.last_fault_time else None,
            "last_healthy_time": self.last_healthy_time.isoformat() if self.last_healthy_time else None,
            "fault_records": [r.to_dict() for r in self.fault_records],
        }

    @classmethod
    def from_dict(cls, d: dict) -> "NodeRecord":
        records = [FaultRecord.from_dict(r) for r in d.get("fault_records", [])]
        node = cls(
            node_name=d["node_name"],
            fault_records=records,
            fatal=d.get("fatal", False),
            soft_banned=d.get("soft_banned", False),
            fault_score=d.get("fault_score", 0.0),
        )
        if d.get("last_fault_time"):
            node.last_fault_time = datetime.fromisoformat(d["last_fault_time"])
        if d.get("last_healthy_time"):
            node.last_healthy_time = datetime.fromisoformat(d["last_healthy_time"])
        return node


@dataclass
class AllocationResult:
    """节点分配结果"""
    healthy_nodes: List[str]
    backup_nodes: List[str]
    rejected_nodes: List[str]
    total_healthy: int
    total_backup: int
    total_rejected: int
    shortage: int

    def summary(self) -> str:
        lines = [
            f"节点分配结果:",
            f"  ✅ 正常节点:   {self.total_healthy}",
            f"  ⚠️  降级备用:   {self.total_backup}",
            f"  ❌ 致命剔除:   {self.total_rejected}",
            f"  🔴 缺口:       {self.shortage}",
        ]
        if self.backup_nodes:
            lines.append(f"  降级备用节点: {self.backup_nodes}")
        if self.rejected_nodes:
            lines.append(f"  致命剔除节点: {self.rejected_nodes}")
        return "\n".join(lines)


@dataclass
class RackFaultEvent:
    """机架级故障事件"""
    affected_nodes: List[str]
    fault_time: datetime
    estimated_rack_id: Optional[str]
    description: str


@dataclass
class ScoredNode:
    """带评分的节点（用于排序）"""
    node_name: str
    fault_score: float
    is_fatal: bool
    is_soft_banned: bool
    total_fault_count: int
    last_fault_weight: float


# ================================================================
#  三、评分引擎
# ================================================================

class ScoringEngine:
    """
    故障衰减评分与优先级排序引擎

    ★ 评分公式（已加入正常运行时间惩罚）:
      score_i = (base_score_i + uptime_penalty_i) × e^(-λ × Δt_i)

    其中:
      base_score_i     = Weight(fault_type_i)                    故障类型基础分
      uptime_penalty_i = U × base_score_i / (1 + uptime_h_i / τ) 正常运行时间惩罚分
        U = uptime_penalty_weight (默认 0.5)
        τ = uptime_reference_hours (默认 24h)
        首条故障(uptime=None) → uptime_penalty = 0
      Δt_i = 故障 i 发生距今的小时数
      λ   = ln(2) / half_life

    排序规则（升序 = 最可用排前面）:
      1. 非 fatal 优先
      2. fault_score 升序
      3. last_fault_weight 升序
      4. total_fault_count 升序
    """

    def __init__(self, config: BlacklistConfig):
        self.cfg = config

    def compute_uptime_penalty(
            self,
            base_score: float,
            uptime_hours: Optional[float],
        ) -> float:
        """
        计算正常运行时间贡献的惩罚分

        公式:  penalty = U × base_score / (1 + uptime_hours / τ)

        典型值 (U=0.5, τ=24h):
          uptime=None (首次): penalty = 0          （无信息，不惩罚）
          uptime=0h           : penalty = 0.5×base  （最大惩罚）
          uptime=1h           : penalty ≈ 0.48×base
          uptime=24h          : penalty = 0.25×base
          uptime=168h (7天)   : penalty ≈ 0.063×base
          uptime→∞            : penalty → 0          （长期稳定，不惩罚）
        """
        if uptime_hours is None:
            return 0.0
        if base_score <= 0.0:
            return 0.0

        U = self.cfg.uptime_penalty_weight
        tau = max(self.cfg.uptime_reference_hours, 0.01)
        return U * base_score / (1.0 + uptime_hours / tau)


    # ---------- 单节点评分 ----------

    def compute_score(self, node: NodeRecord, now: datetime = None) -> float:
        """
        计算单节点的综合故障衰减评分
        ★ 公式: Score = Σ [ (base_score + uptime_penalty) × e^(-λ × Δt) ]
        """
        now = now or datetime.now()
        score = 0.0

        for r in node.fault_records:
            if r.fault_type == FaultType.MANUAL:
                continue

            # 使用记录中已保存的评分字段（兼容旧记录中为 0 的情况）
            if r.base_score > 0.0:
                base = r.base_score
                penalty = r.uptime_penalty
            else:
                # 兼容旧格式记录：用配置中的权重实时计算
                base = self.cfg.weight(r.fault_type)
                penalty = self.compute_uptime_penalty(base, r.uptime_hours)

            delta_h = max((now - r.timestamp).total_seconds() / 3600.0, 0.0)
            decay = math.exp(-self.cfg.decay_lambda * delta_h)
            score += (base + penalty) * decay

        return round(score, 2)

    # ---------- 批量评分 & 排序 ----------

    def get_scored_nodes(self, nodes: Dict[str, NodeRecord], now: datetime = None) -> List[ScoredNode]:
        now = now or datetime.now()
        result = []
        for name, node in nodes.items():
            score = self.compute_score(node, now)
            last_w = 0.0
            for r in reversed(node.fault_records):
                if r.fault_type != FaultType.MANUAL:
                    last_w = self.cfg.weight(r.fault_type)
                    break
            result.append(ScoredNode(
                node_name=name, fault_score=score,
                is_fatal=node.fatal, is_soft_banned=node.soft_banned,
                total_fault_count=node.total_fault_count,
                last_fault_weight=last_w,
            ))
        return result

    def sort_by_availability(self, scored: List[ScoredNode]) -> List[ScoredNode]:
        return sorted(scored, key=lambda n: (
            int(n.is_fatal), n.fault_score, n.last_fault_weight, n.total_fault_count
        ))

    # ---------- 拉黑/白名单判定 ----------

    def check_soft_ban(self, node: NodeRecord, now: datetime = None) -> bool:
        """时间窗口内某类型故障次数 >= 软拉黑阈值"""
        now = now or datetime.now()
        window_s = self.cfg.soft_ban_window_hours * 3600
        counts: Dict[FaultType, int] = {}
        for r in node.fault_records:
            if 0 <= (now - r.timestamp).total_seconds() <= window_s:
                counts[r.fault_type] = counts.get(r.fault_type, 0) + 1
        for ft, cnt in counts.items():
            if cnt >= self.cfg.soft_ban_threshold(ft):
                return True
        return False

    def should_auto_ban(self, node: NodeRecord) -> bool:
        """某类型故障总次数 >= 正式拉黑阈值"""
        counts: Dict[FaultType, int] = {}
        for r in node.fault_records:
            counts[r.fault_type] = counts.get(r.fault_type, 0) + 1
        for ft, cnt in counts.items():
            if cnt >= self.cfg.auto_ban_threshold(ft):
                return True
        return False

    def should_auto_whitelist(self, node: NodeRecord, now: datetime = None) -> bool:
        """连续 auto_whitelist_hours 没有新故障 → 自动解除拉黑"""
        if not node.last_fault_time:
            return False
        now = now or datetime.now()
        return (now - node.last_fault_time).total_seconds() / 3600 >= self.cfg.auto_whitelist_hours

    # ---------- 错误码严重性判定 ----------

    @staticmethod
    def determine_severity(error_code: Optional[str]) -> FaultSeverity:
        """根据错误码判断严重性"""
        if not error_code:
            return FaultSeverity.NORMAL
        try:
            xid = int(str(error_code).replace("Xid", "").strip())
            if xid in FATAL_XID_CODES:
                return FaultSeverity.FATAL
            if xid in WARNING_XID_CODES:
                return FaultSeverity.WARNING
        except (ValueError, AttributeError):
            pass
        if error_code in FATAL_ERROR_CODES:
            return FaultSeverity.FATAL
        return FaultSeverity.NORMAL


# ================================================================
#  四、机架级故障检测器
# ================================================================

class RackFaultDetector:
    """
    机架/交换机级故障感知

    逻辑: 短时间内大量节点同时报 NETWORK 故障 → 判定为网络拓扑级故障，
    不增加这些节点的个体黑名单分数，而是触发报警。
    """

    def __init__(self, config: BlacklistConfig):
        self.cfg = config
        self._timeline: List[Tuple[datetime, str]] = []
        self._node_to_rack: Dict[str, str] = {}
        self._callbacks: List[Callable[[RackFaultEvent], None]] = []

    def register_node_rack(self, node_name: str, rack_id: str) -> None:
        self._node_to_rack[node_name] = rack_id

    def register_alert_callback(self, callback: Callable[[RackFaultEvent], None]) -> None:
        self._callbacks.append(callback)

    def record_network_fault(self, node_name: str, ts: datetime) -> Optional[RackFaultEvent]:
        self._timeline.append((ts, node_name))
        self._cleanup(ts)
        return self._detect(ts)

    def _cleanup(self, now: datetime) -> None:
        cutoff = now.timestamp() - self.cfg.rack_fault_window_seconds
        self._timeline = [(t, n) for t, n in self._timeline if t.timestamp() > cutoff]

    def _detect(self, now: datetime) -> Optional[RackFaultEvent]:
        unique = list({n for _, n in self._timeline})
        if len(unique) < self.cfg.rack_fault_threshold:
            return None
        racks = {self._node_to_rack[n] for n in unique if n in self._node_to_rack}
        rack_id = racks.pop() if len(racks) == 1 else None
        event = RackFaultEvent(
            affected_nodes=unique, fault_time=now, estimated_rack_id=rack_id,
            description=(
                f"机架级故障: {len(unique)} 节点在 "
                f"{self.cfg.rack_fault_window_seconds}s 内同时报网络故障"
                + (f"，疑似机架 {rack_id}" if rack_id else "")
            ),
        )
        for cb in self._callbacks:
            try:
                cb(event)
            except Exception:
                pass
        self._timeline.clear()
        return event


# ================================================================
#  五、持久化管理器
# ================================================================

class PersistenceManager:
    """
    线程安全的 JSON 持久化
    - 原子写入:  先写 .tmp 再 os.replace，防止写一半断电损坏
    - 自动备份:  每次写入前备份上一版本
    - 损坏恢复:  主文件损坏时自动从备份恢复
    - 文件锁:    fcntl.flock 保证多进程安全
    """

    def __init__(self, config: BlacklistConfig):
        self.cfg = config
        # self._lock = threading.Lock()

    def save(self, nodes: Dict[str, NodeRecord], version: str = "1.0") -> bool:
        try:
            data = {
                "version": version,
                "last_updated": datetime.now().isoformat(),
                "total_nodes_in_blacklist": len(nodes),
                "blacklist": {name: node.to_dict() for name, node in nodes.items()},
            }
            path = self.cfg.persistence_path
            bak = self.cfg.persistence_backup_path
            tmp = path + ".tmp"

            # 确保目录存在
            file_dir = os.path.dirname(path)
            if file_dir:
                os.makedirs(file_dir, exist_ok=True)

            with open(tmp, "w", encoding="utf-8") as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                try:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            os.replace(tmp, path)

            # 备份旧文件（仅在旧文件存在时）
            if os.path.exists(path):
                try:
                    shutil.copy2(path, bak)
                except Exception as e:
                    logger.warning(f"备份黑名单失败: {e}")

            logger.info(f"黑名单持久化完成: {len(nodes)} 节点 → {path}")
            return True
        except Exception as e:
            logger.error(f"持久化失败: {e}")
            tmp = self.cfg.persistence_path + ".tmp"
            if os.path.exists(tmp):
                try:
                    os.unlink(tmp)
                except Exception:
                    pass
            return False

    def load(self) -> Dict[str, NodeRecord]:
        main_exists = os.path.exists(self.cfg.persistence_path)
        bak_exists = os.path.exists(self.cfg.persistence_backup_path)

        # 主文件存在 → 正常加载
        if main_exists:
            nodes = self._load_file(self.cfg.persistence_path)
            if nodes is not None:
                return nodes
            # 主文件存在但损坏，尝试备份
            logger.warning("主黑名单文件损坏，尝试从备份恢复...")
            if bak_exists:
                nodes = self._load_file(self.cfg.persistence_backup_path)
                if nodes is not None:
                    logger.info("从备份文件恢复成功")
                    self.save(nodes)
                    return nodes
            logger.error("主文件和备份均不可用，返回空黑名单")
            return {}

        # 主文件不存在 → 初始化新文件
        logger.info("黑名单文件不存在，正在初始化...")
        empty_data = {
            "version": "1.0",
            "last_updated": datetime.now().isoformat(),
            "total_nodes_in_blacklist": 0,
            "blacklist": {},
        }
        file_dir = os.path.dirname(self.cfg.persistence_path)
        if file_dir:
            os.makedirs(file_dir, exist_ok=True)
        try:
            with open(self.cfg.persistence_path, "w", encoding="utf-8") as f:
                json.dump(empty_data, f, ensure_ascii=False, indent=2)
            logger.info(f"黑名单文件已创建: {self.cfg.persistence_path}")
        except Exception as e:
            logger.error(f"创建黑名单文件失败: {e}")
        return {}

    def _load_file(self, path: str) -> Optional[Dict[str, NodeRecord]]:
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                try:
                    data = json.load(f)
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            nodes = {}
            for name, nd in data.get("blacklist", {}).items():
                try:
                    nodes[name] = NodeRecord.from_dict(nd)
                except Exception as e:
                    logger.warning(f"跳过损坏记录 [{name}]: {e}")
            return nodes
        except json.JSONDecodeError as e:
            logger.error(f"JSON 解析失败 [{path}]: {e}")
            return None
        except Exception as e:
            logger.error(f"加载失败 [{path}]: {e}")
            return None


# ================================================================
#  六、黑名单管理器（主类，对外统一接口）
# ================================================================

class BlacklistManager:
    """
    黑名单核心管理器 — 全局单例，线程安全

    使用流程:
        manager = BlacklistManager.get_instance(config)
        manager.start()                          # 启动异步落盘线程
        manager.report_fault(...)                # 容错线程上报故障
        result = manager.allocate_nodes(...)     # 训练调度获取可用节点
        manager.stop()                           # 停止并持久化
    """

    _instance: Optional["BlacklistManager"] = None
    _instance_lock = threading.Lock()

    @classmethod
    def get_instance(cls, config: BlacklistConfig = None) -> "BlacklistManager":
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = cls(config or BlacklistConfig())
            return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """重置单例（仅用于测试）"""
        with cls._instance_lock:
            if cls._instance is not None:
                cls._instance.stop()
            cls._instance = None

    def __init__(self, config: BlacklistConfig = None):
        self.config = config or BlacklistConfig()
        self._nodes: Dict[str, NodeRecord] = {}
        self._scoring = ScoringEngine(self.config)
        self._rack_detector = RackFaultDetector(self.config)
        self._persistence = PersistenceManager(self.config)
        self._dirty = False
        self._persist_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._inf_tracker: Dict[str, int] = {}      # INF 连续步数追踪
        self._load()

    # ---------- 生命周期 ----------

    def start(self):
        """启动异步落盘后台线程"""
        if self._persist_thread is not None:
            return
        self._stop_event.clear()
        self._persist_thread = threading.Thread(target=self._persist_loop, name="bl-persist", daemon=True)
        self._persist_thread.start()
        logger.info("黑名单管理器已启动")

    def stop(self):
        """停止后台线程，执行最后一次持久化"""
        self._stop_event.set()
        if self._persist_thread:
            self._persist_thread.join(timeout=5)
            self._persist_thread = None
        self._do_persist()
        logger.info("黑名单管理器已停止")

    def _persist_loop(self):
        while not self._stop_event.is_set():
            self._stop_event.wait(self.config.persistence_interval)
            if self._dirty:
                self._do_persist()

    def _do_persist(self):
        snapshot = dict(self._nodes)
        if self._persistence.save(snapshot):
            self._dirty = False

    def _load(self):
        loaded = self._persistence.load()
        self._nodes = loaded
        logger.info(f"黑名单加载完成: {len(loaded)} 个节点")

    # ================================================================
    #  故障上报
    # ================================================================

    def report_fault(
        self,
        node_name: str,
        fault_type: FaultType,
        description: str = "",
        error_code: Optional[str] = None,
        HCU_id: Optional[int] = None,
        micro_step: Optional[int] = None,
    ) -> bool:
        """
        上报一次故障（线程安全，可从任意容错线程调用）

        ★ 上报时会自动计算并保存:
          - base_score:    故障类型的基础权重分
          - uptime_hours:  本次故障前的正常运行时间
          - uptime_penalty: 正常运行时间贡献的惩罚分

        Args:
            node_name:    节点名称
            fault_type:   故障类型
            description:  故障描述
            error_code:   错误码（NVIDIA Xid 号 或 AMD 错误码）
            gpu_id:       具体哪张 GPU（None = 节点级）
            micro_step:   训练步数（用于 INF 精准判定）

        Returns:
            True 故障被成功记录并生效
        """
        now = datetime.now()

        # ---- INF 精准判定：连续多步才记录 ----
        # if fault_type == FaultType.GRADIENT_INF and micro_step is not None:
        #     prev = self._inf_tracker.get(node_name)
        #     if prev is not None and prev == micro_step - 1:
        #         self._inf_tracker[node_name] = micro_step
        #     else:
        #         self._inf_tracker[node_name] = micro_step
        #         # 不连续，不记录
        #         logger.debug(f"节点 {node_name} INF 步数不连续（{micro_step}），暂不记录")
        #         # return False
        # ---- 机架级故障检测（仅网络故障） ----
        if fault_type == FaultType.NETWORK:
            rack_event = self._rack_detector.record_network_fault(node_name, now)
            if rack_event:
                logger.warning(f"机架级故障: {rack_event.description}")
                return False  # 不记录到个体节点
        # ---- 计算评分字段 ----
        base_score = self.config.weight(fault_type)
        # uptime_hours 在 NodeRecord.add_fault 中自动计算，这里先置 None
        # uptime_penalty 需要已知 uptime 才能算，先置 0，add_fault 后再补算
        # ---- 构造故障记录 ----
        record = FaultRecord(
            fault_type=fault_type, timestamp=now,
            description=description or self.config.description(fault_type),
            error_code=error_code, HCU_id=HCU_id, micro_step=micro_step,
            base_score=base_score,
            uptime_hours=None,
            uptime_penalty=0.0,
        )
        if node_name not in self._nodes:
            self._nodes[node_name] = NodeRecord(node_name=node_name)

        node = self._nodes[node_name]
        # ★ add_fault 会自动计算 uptime_hours 并写入 record
        node.add_fault(record)
        # ★ uptime_hours 已由 add_fault 填充，现在补算 uptime_penalty
        record.uptime_penalty = self._scoring.compute_uptime_penalty(
            base_score, record.uptime_hours
        )
        # ★ 记录本条故障的即时总分（不含全局衰减）
        record.fault_score = round(record.base_score + record.uptime_penalty, 2)
        # ★★★ 新增：重新计算并更新节点的综合衰减评分 ★★★
        current_total_score = self._scoring.compute_score(node)
        node.update_fault_score(current_total_score)

        # 严重性判定
        sev = ScoringEngine.determine_severity(error_code)
        cfg_sev = self.config.severity(fault_type)

        # 致命判定
        if sev == FaultSeverity.FATAL or cfg_sev == FaultSeverity.FATAL:
            node.fatal = True
            logger.error(f"节点 {node_name} 标记为致命故障 (code={error_code})")

        # 软拉黑检查
        if not node.soft_banned and not node.fatal and self._scoring.check_soft_ban(node, now):
            node.soft_banned = True
            logger.warning(f"节点 {node_name} 被软拉黑")

        # 正式拉黑检查
        if self._scoring.should_auto_ban(node):
            node.fatal = True
            logger.error(f"节点 {node_name} 故障次数超限，正式拉黑")

        # 自动白名单化
        if (node.fatal or node.soft_banned) and self._scoring.should_auto_whitelist(node, now):
            node.fatal = False
            node.soft_banned = False
            logger.info(f"节点 {node_name} 自动白名单化（连续健康 {self.config.auto_whitelist_hours}h）")

        self._dirty = True

        return True

    # ================================================================
    #  健康上报
    # ================================================================

    def report_healthy(self, node_name: str) -> None:
        """上报节点健康"""
        now = datetime.now()
        if node_name in self._nodes:
            self._nodes[node_name].mark_healthy(now)
            self._dirty = True

    # ================================================================
    #  节点分配
    # ================================================================

    def allocate_nodes(self, all_nodes: List[str], required_count: int) -> AllocationResult:
        """
        从节点池中分配 required_count 个节点

        逻辑:
          1. 剔除 fatal 节点
          2. 健康节点优先使用
          3. 不够时从黑名单中按可用性排序降级补充
          4. fatal 节点绝对不参与补充

        Args:
            all_nodes:      全部候选节点列表
            required_count: 需要的节点数量

        Returns:
            AllocationResult
        """
        # 快照读
        bl_snapshot = dict(self._nodes)

        # 分类
        fatal_set: Set[str] = set()
        bl_recoverable: Dict[str, NodeRecord] = {}
        healthy: List[str] = []

        for name in all_nodes:
            if name in bl_snapshot:
                node = bl_snapshot[name]
                if node.fatal:
                    fatal_set.add(name)
                else:
                    bl_recoverable[name] = node
            else:
                healthy.append(name)

        shortage = max(0, required_count - len(healthy))
        backup: List[str] = []

        if shortage > 0 and bl_recoverable:
            scored = self._scoring.get_scored_nodes(bl_recoverable)
            for sn in self._scoring.sort_by_availability(scored):
                if len(backup) >= shortage:
                    break
                backup.append(sn.node_name)

        actual_shortage = max(0, required_count - len(healthy) - len(backup))

        return AllocationResult(
            healthy_nodes=healthy,
            backup_nodes=backup,
            rejected_nodes=list(fatal_set),
            total_healthy=len(healthy),
            total_backup=len(backup),
            total_rejected=len(fatal_set),
            shortage=actual_shortage,
        )

    # ================================================================
    #  查询接口
    # ================================================================

    def is_blacklisted(self, node_name: str) -> Tuple[bool, bool]:
        """Returns: (is_banned, is_fatal)"""
        node = self._nodes.get(node_name)
        if node is None:
            return False, False
        return (node.fatal or node.soft_banned), node.fatal

    def get_node_info(self, node_name: str) -> Optional[dict]:
        node = self._nodes.get(node_name)
        return node.to_dict() if node else None

    def get_all_blacklisted(self) -> Dict[str, dict]:
        return {n: nd.to_dict() for n, nd in self._nodes.items()}

    def get_ranked_list(self) -> List[dict]:
        """按可用性排序的黑名单列表（score 越低越优先）"""
        snapshot = dict(self._nodes)

        if not snapshot:
            return []
        scored = self._scoring.get_scored_nodes(snapshot)
        sorted_nodes = self._scoring.sort_by_availability(scored)
        return [
            {
                "node_name": s.node_name,
                "fault_score": s.fault_score,
                "is_fatal": s.is_fatal,
                "is_soft_banned": s.is_soft_banned,
                "total_fault_count": s.total_fault_count,
            }
            for s in sorted_nodes
        ]

    def get_stats(self) -> dict:
        """黑名单统计概览"""
        total = len(self._nodes)
        fatal = sum(1 for n in self._nodes.values() if n.fatal)
        soft = sum(1 for n in self._nodes.values() if n.soft_banned and not n.fatal)
        type_dist = {}
        for node in self._nodes.values():
            for r in node.fault_records:
                key = str(r.fault_type)
                type_dist[key] = type_dist.get(key, 0) + 1
        return {
            "total_blacklisted": total,
            "fatal_nodes": fatal,
            "soft_banned_nodes": soft,
            "recoverable_nodes": total - fatal,
            "fault_type_distribution": type_dist,
        }

    # ================================================================
    #  管理操作
    # ================================================================

    def remove_node(self, node_name: str) -> bool:
        """手动移除节点（维修后）"""
        if node_name in self._nodes:
            del self._nodes[node_name]
            self._dirty = True
            logger.info(f"节点 {node_name} 已从黑名单移除")
            return True
        return False

    def clear_node_history(self, node_name: str) -> bool:
        """清除节点故障历史（维修后重新启用）"""
        if node_name in self._nodes:
            self._nodes[node_name].clear()
            self._dirty = True
            logger.info(f"节点 {node_name} 故障历史已清除")
            return True
        return False

    def force_persist(self) -> bool:
        """立即持久化"""
        self._do_persist()
        return True

    def register_node_rack(self, node_name: str, rack_id: str) -> None:
        """注册节点所属机架"""
        self._rack_detector.register_node_rack(node_name, rack_id)

    def register_rack_alert_callback(self, callback: Callable) -> None:
        """注册机架故障报警回调"""
        self._rack_detector.register_alert_callback(callback)
