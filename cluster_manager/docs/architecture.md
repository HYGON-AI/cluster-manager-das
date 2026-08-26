<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# cluster_manager 代码结构与处理链路

本文是 `cluster_manager` 当前实现的唯一架构说明；面向用户的安装、配置和启动方式统一维护在项目 [README](../README.md) 及对应操作文档中。

## 定位

`cluster_manager` 是单训练任务控制面，支持裸机 hostfile + MPI 和 Slurm + MPI。它不实现训练算法，也不保存 checkpoint；它负责节点分配、训练启停、运行事件监控和故障后的重新编排。

## 核心组件

| 组件 | 代码目录 | 职责 |
| --- | --- | --- |
| 入口与校验 | `cluster_manager/main.py` | 解析参数，区分裸机/Slurm，校验文件并创建 workspace |
| 控制器 | `controller/distributed_job_manager.py` | 初始化组件，消费事件/状态命令，执行训练启停 |
| 状态机 | `runtime/job_state_machine.py` | 将日志、NHC 和启动结果转换为状态及下一命令 |
| 运行上下文 | `runtime/runtime_context.py` | 组合任务参数、训练拓扑、节点池和持久状态 |
| 状态持久化 | `runtime/run_state_manager.py` | 保存并恢复 `running_state.json` |
| 节点池 | `node_management/node_pool.py` | 管理 total/running/backup/abnormal/normal 节点集合 |
| 黑名单 | `node_management/node_blacklist_manager.py` | 记录故障、评分、降级分配和恢复策略 |
| MPI launcher | `launcher/mpirun_launcher.py` | 调用训练脚本并停止目标节点训练进程 |
| 日志监控 | `monitor/log_monitor.py` 及解析器 | 发现日志、解析迭代与异常并发布事件 |
| 节点监控 | `monitor/nhc_monitor.py` | 周期性执行 NHC、维护节点状态与训练快照 |
| Slurm 适配 | `node_management/slurm_manager.py` | 查询/提交作业并同步 allocation hostfile |
| 事件总线 | `event/event_bus.py` | 在线程之间传递日志和节点监控事件 |

## 主处理链路

```text
CLI/start.sh
    │ 参数与文件校验
    ▼
DistributedJobManager
    ├─ SlurmMgr（仅 SLURM）
    ├─ NodePool ─ BlacklistManager
    ├─ NodePoolProxy ─ NHC monitor
    ├─ LogMonitor ─ base/special parser
    ├─ JobStateMachine
    └─ MPIRunLauncher

LogMonitor/NHC monitor ──EventBus──> JobStateMachine
JobStateMachine ──JobCommand──> DistributedJobManager
DistributedJobManager ──start/stop──> MPIRunLauncher
```

事件优先于状态驱动命令。主循环每次先从 `EventBus` 取事件；没有事件时，状态机根据当前状态返回 `START_TRAINING`、`STOP_TRAINING` 或 `NONE`。

## 状态转换

| 状态 | 含义 | 典型下一步 |
| --- | --- | --- |
| `INIT` | 没有可恢复状态 | 释放旧运行集合并进入 `STARTING` |
| `STARTING` | 正在申请节点并启动训练 | 启动成功后进入 `PENDING` |
| `PENDING` | 已启动，等待有效训练日志 | 正常日志进入 `RUNNING`；超时或 hang 进入恢复 |
| `RUNNING` | 正常监控训练 | 故障事件进入 `RECOVERING` |
| `RECOVERING` | 先停止旧训练，再申请节点重启 | 重启成功后回到 `PENDING` |
| `FAULT_RECOVER` | 故障恢复相关运行状态 | 继续处理日志故障和迭代事件 |

`HANG`、`LOSS_DIVERGENCE`、`FAULT`、`COMPLETE` 已定义在状态枚举中，但当前主路径主要通过事件直接进入 `RECOVERING`，不能仅根据枚举名称假设这些状态都有完整处理分支。

## 节点池约束

节点池维护以下关系：

```text
total = running ∪ backup ∪ abnormal
normal = total - abnormal
running、backup、abnormal 互不重叠
```

每轮训练从可用节点中申请 `required_nodes_num` 个节点，并生成 slots hostfile。运行节点发生故障时会清空或释放本轮运行集合，将故障节点加入异常集合，再从备用节点补齐。

## 日志解析器

- `LOG_PARSER_TYPE=base`：Megatron 日志主路径。
- `LOG_PARSER_TYPE=special`：已经适配的其他训练框架日志格式。

解析器输出统一事件，例如正常运行、迭代、hang、loss/inf 或进程退出。状态机负责决定告警、标记故障节点还是触发重启，解析器本身不直接操作节点池。

## 持久化边界

运行状态、节点集合、当前快照、历史快照、节点状态和黑名单分别持久化。控制器重启时会组合这些文件恢复，而不是只读取一个状态文件。因此同一个 `WORK_DIR` 只能由一个控制器使用，清理时也必须把整个 workspace 当作一个一致性单元。

具体运行流程见[故障恢复与日常操作](recovery_workflow.md)。
