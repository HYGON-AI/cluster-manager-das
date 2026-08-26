# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# cluster_manager/config/global_config.py
"""
全局配置 - 只存储环境变量和运行时配置
"""
import logging
import os

# 全局日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(threadName)s - %(filename)s:%(lineno)d - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ==============================================================
# 运行时环境变量
# ==============================================================
WORK_DIR = os.environ.get("WORK_DIR") or os.getcwd()
LOG_DIR = os.environ.get("LOG_DIR", f"{WORK_DIR}/hcu_megatron/examples/aibenchmark")
MPI_LAUNCH_TIMEOUT = os.environ.get("MPI_LAUNCH_TIMEOUT", "300")
MAX_RESTART_TIMES = os.environ.get("MAX_RESTART_TIMES", "3")
TRAIN_ALERT_THRESHOLD = os.environ.get("TRAIN_ALERT_THRESHOLD", "20000")
TRAIN_NO_UPDATE_THRESHOLD = os.environ.get("TRAIN_NO_UPDATE_THRESHOLD", "1800")
LOG_PARSER_TYPE = os.environ.get("LOG_PARSER_TYPE", "base")
CLUSTER_SCHEDULE = os.environ.get("CLUSTER_SCHEDULE", "NONE").strip().upper()
STARTUP_NO_LOG_TIMEOUT_SEC = os.environ.get("STARTUP_NO_LOG_TIMEOUT_SEC", "1800")
SBATCH_SCRIPT = os.environ.get("SBATCH_SCRIPT", WORK_DIR + "/sbatch.sh")
FEISHU_WEBHOOK_URL = os.environ.get("FEISHU_WEBHOOK_URL", None)
ENABLE_LOSS_GRAD_CHECK = os.environ.get("ENABLE_LOSS_GRAD_CHECK", "true").lower() in ("true", "1", "yes")
ENABLE_REGULAR_NOTIFY = os.environ.get("ENABLE_REGULAR_NOTIFY", "true").lower() in ("true", "1", "yes")
ENABLE_ITER_DUMPER = os.environ.get("ENABLE_ITER_DUMPER", "true").lower() in ("true", "1", "yes")
INTERVAL_SEC = os.environ.get("INTERVAL_MONITOR", '60')
SNAPSHOT_START_OFFSET = os.environ.get("SNAPSHOT_START_OFFSET", 0)
BLACKLIST_PERSISTENCE_PATH = os.environ.get("BLACKLIST_PERSISTENCE_PATH", f"{WORK_DIR}/blacklist.json")
BLACKLIST_PERSISTENCE_BACKUP_PATH = os.environ.get("BLACKLIST_PERSISTENCE_BACKUP_PATH", f"{WORK_DIR}/blacklist.json.bak")

# ==============================================================
# 未初始化的全局变量（由外部设置）
# ==============================================================
BASE_DIR = None
SLOTS = None
LAUNCH_MODEL = None
PROGRAM = None
TRAINING_EXEC_PATH = None
TRAINING_OUTPUT_LOG = None
CKPT_FILE = None
MPI_ENV = None
DTK_ENV = None
NCCL_ENV = None
CONDA_ENV = None
CONDA_NAME = None
FT_NODES_PROCESS = None
RELATIVE_THRESHOLD = None
STD_MULTIPLIER = None
IS_GEMM_STD = None
CLUSH_F_NUM = None
EXEC_PROC_NAME = None
INTERVAL_MONITOR = None
SNAPSHOT_START_OFFSET = 0
EFFICIENCY_FACTOR = 1.0

# ==============================================================
# 参数配置定义（方便后续添加修改）
# ==============================================================
# 自定义目标参数（各模块按需获取）
MEGATRON_TARGET_ARGS = [
    "--save-interval",
    "--train-samples",
    "--eval-interval",
]

# 落盘参数（模型参数，容错重启时对比，区分模型是否变化）
# 保持 Megatron 原生参数形式
PERSISTENT_KEYS = [
    # 并行配置
    "--tensor-model-parallel-size",
    "--pipeline-model-parallel-size",
    "--expert-model-parallel-size",
    "--expert-tensor-parallel-size",
    "--context-parallel-size",
    "--sequence-parallel",
    "--num-layers-per-virtual-pipeline-stage",
    # 模型架构（决定模型结构的关键参数）
    "--num-layers",
    "--hidden-size",
    "--num-attention-heads",
    "--ffn-hidden-size",
    "--seq-length",
    "--max-position-embeddings",
    "--num-experts",
    "--moe-ffn-hidden-size",
    "--kv-channels",
    "--num-query-groups",
    # 运行时
    "--world-size",
    # 训练超参数
    "--micro-batch-size",
    "--global-batch-size",
    "--gradient-accumulation-steps",
]

# 默认训练配置（与 PERSISTENT_KEYS 一致）
DEFAULT_TRAIN_CONFIG = {key: None for key in PERSISTENT_KEYS}

# ==============================================================
# 兼容旧代码：从 train_config 导入函数
# ==============================================================
from cluster_manager.config.train_config import (
    load_train_config,
    get_config,
    get_train_config,
    get_megatron_config,
    get_persistent_config,
    set_megatron_config,
)

# ==============================================================
# 进程启动时自动加载训练配置
# ==============================================================
_MEGATRON_SCRIPT_PATH = os.environ.get("MEGATRON_SCRIPT_PATH")
if _MEGATRON_SCRIPT_PATH:
    load_train_config(_MEGATRON_SCRIPT_PATH)

# 兼容旧代码：MEGATRON_CONFIG 作为代理
# 继承 dict 以支持 json.dumps() 等
class _ConfigProxy(dict):
    """配置代理类，支持字典操作和 JSON 序列化"""
    
    def __getitem__(self, key):
        return get_megatron_config().get(key)
    
    def __setitem__(self, key, value):
        set_megatron_config(key, value)
    
    def get(self, key, default=None):
        return get_megatron_config().get(key, default)
    
    def keys(self):
        return get_megatron_config().keys()
    
    def values(self):
        return get_megatron_config().values()
    
    def items(self):
        return get_megatron_config().items()
    
    def __iter__(self):
        return iter(get_megatron_config())
    
    def __len__(self):
        return len(get_megatron_config())
    
    def __contains__(self, key):
        return key in get_megatron_config()
    
    def __repr__(self):
        return repr(get_megatron_config())
    
    # json.dumps() 会调用父类 dict 的序列化方法
    # 但我们需要返回实际的配置字典
    def __dict__(self):
        return get_megatron_config()

# 创建代理实例，初始化为空字典
MEGATRON_CONFIG = _ConfigProxy()
TRAIN_CONFIG = property(lambda self: get_train_config())

# ==============================================================
# 类型转换
# ==============================================================
try:
    WORK_DIR = str(WORK_DIR)
    LOG_DIR = str(LOG_DIR)
    MPI_LAUNCH_TIMEOUT = int(MPI_LAUNCH_TIMEOUT)
    MAX_RESTART_TIMES = int(MAX_RESTART_TIMES)
    TRAIN_ALERT_THRESHOLD = int(TRAIN_ALERT_THRESHOLD)
    TRAIN_NO_UPDATE_THRESHOLD = int(TRAIN_NO_UPDATE_THRESHOLD)
    LOG_PARSER_TYPE = str(LOG_PARSER_TYPE)
    STARTUP_NO_LOG_TIMEOUT_SEC = int(STARTUP_NO_LOG_TIMEOUT_SEC)
    SBATCH_SCRIPT = str(SBATCH_SCRIPT)
    INTERVAL_MONITOR = int(INTERVAL_SEC)
    SNAPSHOT_START_OFFSET = int(SNAPSHOT_START_OFFSET)
except ValueError:
    WORK_DIR = str(os.getcwd())
    LOG_DIR = str(LOG_DIR)
    MPI_LAUNCH_TIMEOUT = 300
    MAX_RESTART_TIMES = 3
    TRAIN_ALERT_THRESHOLD = 20000
    TRAIN_NO_UPDATE_THRESHOLD = 1800
    LOG_PARSER_TYPE = "base"
    STARTUP_NO_LOG_TIMEOUT_SEC = 1800
    SBATCH_SCRIPT = WORK_DIR + "/sbatch.sh"  # 修正了原代码中可能报错的 WORK_DIR / "sbatch.sh"
    INTERVAL_MONITOR = 60
    SNAPSHOT_START_OFFSET = 0
