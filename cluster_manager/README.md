<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# cluster_manager：Slurm/MPI 训练监控与故障恢复

`cluster_manager` 运行在登录节点或控制节点上，用于管理裸机 hostfile 或 Slurm 分配中的 MPI 分布式训练任务。它维护训练节点池、监控训练日志和节点状态，并在检测到故障后选择备用节点重新拉起训练。

当前可用于生产的主路径包括 **裸金属 + MPI** 和 **Slurm + MPI**。Kubernetes 训练容错请使用同级项目 [`hygon-ft-k8s`](../hygon-ft-k8s/README.md)。

## 主要能力

- 校验 Slurm 作业、作业名、申请节点和 hostfile。
- 维护运行、备用和异常节点池并持久化运行状态。
- 启动 MPI 训练，监控训练日志、NHC 和硬件信息。
- 故障后剔除异常节点、选择备用节点并重启训练。
- 周期性复检异常节点，恢复后重新放回正常节点池。
- 可选检查 Slurm 队列、发送飞书通知和执行训练前性能筛机。

## 使用前必须确认

- 目标 Slurm 节点为本次训练独占，节点上没有其他 Python 任务。
- Slurm 模式下，`--job_id` 对应的作业已存在或允许重新提交，作业名与 `--job_name` 一致。
- Slurm 模式下，hostfile 中的运行节点和备用节点都属于该 Slurm 分配；裸机模式不使用 Slurm 参数。
- 训练脚本已配置 checkpoint；本项目负责编排停止、换节点和重启，不负责保存 checkpoint。
- 启动脚本应从本目录的 `examples/start_none.sh` 或 `examples/start_slurm.sh` 复制并按现场环境修改；不要与 `hygon-ft-k8s` 或其他项目的启动脚本混用。

故障恢复会在运行节点执行 `pkill -9 -f python`。该命令不会只匹配当前训练，因此非独占节点禁止使用。

## 源码启动

`cluster_manager` 不要求先安装 Python 包，可以直接从源码运行。运行环境需要具备 `setup.py` 中声明的 Python 依赖。

从仓库根目录验证源码入口：

```bash
cd /path/to/hcu_cluster_manager
python cluster_manager/cluster_manager/main.py --help
```

项目提供裸机和 Slurm 两份源码启动示例。建议复制对应示例作为现场启动脚本；`start.sh` 保留为完整环境模板，其中包含占位路径，不能直接运行。

进入子项目目录，选择并复制启动示例：

```bash
cd cluster_manager
cp examples/start_none.sh start.local.sh        # 裸机 hostfile
# 或
cp examples/start_slurm.sh start.slurm.local.sh # Slurm
```

编辑复制后的脚本，必须根据当前任务配置以下内容：

| 配置 | 说明 |
| --- | --- |
| `WORK_DIR` | 容错控制器的工作目录，节点池和运行状态会保存在其 `workspace/` 下 |
| `LOG_DIR` | 训练日志目录，日志文件需要满足监控器的命名规则 |
| `MEGATRON_SCRIPT_PATH` | 包含 TP、PP、EP、CP、batch size 和 checkpoint 间隔等参数的训练配置脚本；`TRIAN_PATH` 只是 `start.sh` 模板中的本地变量名 |
| `NODES_NUM` | 每轮训练需要的节点数 |
| `SLOTS` | 每个节点启动的训练进程数 |
| `RUN_PATH` | 真正执行训练的启动脚本；该脚本需要接收 slots hostfile 作为第一个参数 |
| `HOSTFILE` | Slurm allocation 内的运行节点和备用节点列表 |
| `JOB_ID` | 已存在的 Slurm Job ID |
| `JOB_NAME` | Slurm 作业名，必须与 sbatch 脚本及 Slurm 队列中的名称一致 |
| `SBATCH_SCRIPT` | 本次任务对应的 sbatch 脚本 |
| Conda 配置 | `conda.sh` 路径和训练环境名称 |
| 控制日志路径 | `nohup` 输出文件，建议放在 `WORK_DIR` 或独立日志目录 |
| `LOG_PARSER_TYPE` | 日志解析器类型；默认 `base` 表示 Megatron 日志，其他特定训练框架日志使用 `special` |
| `CLUSTER_SCHEDULE` | 调度场景；`NONE`（默认）表示裸机 hostfile，`SLURM` 表示 Slurm 作业 |

MPI 生产路径配置：

```bash
CLUSTER_LAUNCH_MODE=mpi
LOG_PARSER_TYPE=base
CLUSTER_SCHEDULE=NONE
```

裸机场景只通过 hostfile 使用 `mpirun` 启动，不要求 `JOB_ID`、`JOB_NAME` 和 `SBATCH_SCRIPT`，也不会执行 `squeue`、`sinfo` 或 `scontrol`。使用 Slurm 时设置 `CLUSTER_SCHEDULE=SLURM`，此时三个 Slurm 参数均为必填项。完整示例见 `examples/start_none.sh` 和 `examples/start_slurm.sh`。

需要解析 `epoch ... loss=... num_updates=...` 等其他特定训练框架日志时，显式配置 `LOG_PARSER_TYPE=special`。其他值会被拒绝，避免配置拼写错误后静默选择错误解析器。

Slurm 启动脚本必须使用 `JOB_ID` 传递 `--job_id`，并显式传递 `--job_name`：

```bash
--job_id "${JOB_ID}" \
--job_name "${JOB_NAME}" \
--sbatch_script "${SBATCH_SCRIPT}"
```

配置完成后先做 Shell 语法检查，再启动。例如裸机脚本：

```bash
bash -n start.local.sh
bash start.local.sh
```

示例脚本以前台方式启动，便于首次验证；需要后台运行时，由现场进程管理方式接管。若使用包含 `nohup` 的完整 `start.sh` 模板，启动后检查控制日志：

```bash
tail -f /path/to/cluster_manager.log
```

两种模式的日志中都应看到节点池初始化、NHC monitor 启动和 `[MPI-LAUNCH]`；Slurm 模式还应看到作业校验。如果没有这些信息，先按
[常见故障排查](docs/TROUBLESHOOTING_CN.md) 检查参数和环境。

完整的参数准备、hostfile、sbatch、训练脚本和日志要求见
[部署与启动指南](docs/deployment_guide.md)。


## 文档

本 README 是使用入口；实现架构统一维护在 `docs/architecture.md`，Python 包目录内不再保存第二份架构说明。

| 文档 | 内容 |
| --- | --- |
| [部署与启动指南](docs/deployment_guide.md) | 环境检查、输入文件准备和源码启动，可选安装方式 |
| [配置与参数参考](docs/config_guide.md) | 命令行参数和环境变量 |
| [故障恢复与日常操作](docs/recovery_workflow.md) | 故障处理顺序、状态文件、控制进程恢复和训练前筛机 |
| [常见故障排查](docs/TROUBLESHOOTING_CN.md) | 作业校验、日志、NHC、备用节点和实现边界 |
| [代码结构与处理链路](docs/architecture.md) | 核心模块、事件流和状态持久化 |
| [node_check](cluster_manager/node_check/README.md) | 独立的两阶段性能筛机工具 |
| [辅助工具](tools/README.md) | 训练前检查、时间戳、节点检查和网络拓扑验证 |

## 项目结构

```text
cluster_manager/
├── cluster_manager/        # Python 包和 node_check
├── docs/                   # 部署、配置、恢复、架构和排障文档
├── tools/                  # 独立辅助工具
├── setup.py                # 可选的打包安装与命令行入口
└── README.md
```

## 测试

```bash
python3 -m pytest test/ -v
```

## License

This subproject is licensed under the Apache License, Version 2.0. See the
repository-level [LICENSE](../LICENSE), [NOTICE](../NOTICE), and
[THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md).
