<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# cluster_manager 故障恢复与日常操作

本文说明 `cluster_manager` 在裸机 MPI 和 Slurm + MPI 场景中的故障处理顺序、磁盘状态以及控制进程重启后的恢复行为。部署参数见[部署与启动指南](deployment_guide.md)，内部模块关系见[代码结构与处理链路](architecture.md)。

## 恢复边界

`cluster_manager` 负责发现异常、隔离节点、选择备用节点、停止当前训练并重新执行训练启动脚本。训练状态能否从正确位置继续，取决于训练框架自身的 checkpoint 保存和加载逻辑。

使用前必须满足以下条件：

- 训练启动脚本能够从最近一次有效 checkpoint 恢复。
- hostfile 中除运行节点外还包含足够的备用节点。
- 目标节点为任务独占；当前停止实现会按 `python` 关键字终止运行节点上的进程。
- 裸机场景设置 `CLUSTER_SCHEDULE=NONE`；Slurm 场景设置 `CLUSTER_SCHEDULE=SLURM` 并提供有效的 Job ID、Job Name 和 sbatch 脚本。

## 正常启动流程

1. 入口校验节点数、slots、训练脚本和 hostfile。
2. Slurm 场景额外校验作业、作业名、申请节点和 sbatch 脚本。
3. 在 `${WORK_DIR}/workspace` 初始化节点池和运行状态。
4. 从可用节点中申请本轮运行节点，生成 slots hostfile。
5. 执行训练启动脚本，状态由 `STARTING` 进入 `PENDING`。
6. 日志解析器观察到有效训练日志后，状态进入 `RUNNING`。
7. 日志监控和 NHC 监控持续向状态机发送事件。

## 故障恢复流程

```text
RUNNING/PENDING
    │ 日志 hang、退出、loss/inf 异常、NHC 节点异常或 Slurm 作业释放
    ▼
RECOVERING (stopping)
    │ 停止当前训练、记录故障、释放本轮运行节点
    ▼
RECOVERING (starting)
    │ 隔离异常节点、从正常/降级节点中重新申请足量节点
    ▼
PENDING
    │ 观察到训练迭代日志
    ▼
RUNNING
```

故障节点会进入异常节点集合和黑名单评分流程。异常节点只有在后续健康检查通过并满足黑名单恢复策略后，才会重新参与分配。

## 状态文件

以下文件位于 `${WORK_DIR}/workspace`，控制进程运行时不应手工编辑：

| 路径 | 用途 |
| --- | --- |
| `running_state.json` | 当前状态和影响恢复判断的训练配置 |
| `.node_pool/total_nodes.txt` | hostfile 中的全部节点 |
| `.node_pool/running_nodes.txt` | 本轮训练节点 |
| `.node_pool/backup_nodes.txt` | 可用于替换的备用节点 |
| `.node_pool/abnormal_nodes.txt` | 已判定异常的节点 |
| `.node_pool/normal_nodes.txt` | 当前非异常节点 |
| `.node_pool/slots_nodes.txt` | 传给训练脚本的 slots hostfile |
| `current_snapshot.json.tmp` | 尚未结束的一轮训练快照 |
| `node_snapshots.json` | 已结束训练轮次的追加式快照 |
| `node_status.json` | NHC 节点状态 |
| `hw_summary.json`、`hw_detail/` | 节点硬件检查摘要与明细 |

黑名单持久化路径由 `BLACKLIST_PERSISTENCE_PATH` 和 `BLACKLIST_PERSISTENCE_BACKUP_PATH` 控制，具体值见[配置与参数参考](config_guide.md)。

## 控制进程重启

1. 确认训练进程和控制进程的实际状态，避免同时启动两个控制器。
2. 保留原 `${WORK_DIR}/workspace`，并使用与原任务一致的训练拓扑配置启动。
3. 控制器读取 `running_state.json`、节点池和未完成快照。
4. 如果磁盘状态为 `PENDING` 或 `RUNNING`，控制器恢复日志和节点监控；其他未完成状态会进入恢复流程。

如果更换了 TP、PP、CP、EP、节点数等持久配置，不应直接复用原 workspace。先备份旧目录，再使用新的 `WORK_DIR` 启动新任务。

## 人工处置原则

- 备用节点不足时，不要手工把未检查的异常节点移入运行集合；先修复节点或扩大资源池。
- NHC 命令执行失败表示“健康状态未知”，不能当作节点健康。
- loss/inf、软件异常或无法定位节点的退出事件需要结合训练日志复核。
- 删除或重建 workspace 会丢失节点池、状态和历史快照，只能在确认不需要恢复旧任务后进行。
- 停止或重启训练前确认 checkpoint 已成功写入持久存储。

常见启动和恢复失败见[常见故障排查](TROUBLESHOOTING_CN.md)。
