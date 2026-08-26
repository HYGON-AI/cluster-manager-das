<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# cluster_manager 配置与参数参考

当前配置由命令行参数、环境变量和 `MEGATRON_SCRIPT_PATH` 三部分组成。

## 1. 训练命令行参数

| 参数 | 裸机 | Slurm | 说明 |
|---|---:|---:|---|
| `--nodes_num` | 必填 | 必填 | 每轮训练需要的节点数，必须大于 0 |
| `--slots` | 必填 | 必填 | 每个节点的训练进程数，必须大于 0 |
| `--exec` | 必填 | 必填 | 训练启动 Shell 脚本；必须存在且可读 |
| `--hostfile` | 必填 | 必填 | 节点列表；Slurm 模式启动后会按 allocation 更新 |
| `--job_id` | 不需要 | 必填 | 已有 Slurm Job ID |
| `--job_name` | 可选 | 必填 | Slurm 作业名；Slurm 模式必须与 sbatch 脚本一致 |
| `--sbatch_script` | 不需要 | 必填 | Slurm 提交脚本 |

示例：

```bash
hcu-cluster-inspect \
  --nodes_num 4 \
  --slots 8 \
  --exec /path/to/run.sh \
  --hostfile /path/to/hostfile
```

## 2. 核心环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `CLUSTER_LAUNCH_MODE` | `mpi` | 启动器类型；当前只支持 `mpi` |
| `CLUSTER_SCHEDULE` | `NONE` | `NONE` 表示裸机 hostfile，`SLURM` 表示 Slurm |
| `WORK_DIR` | 当前目录 | 控制器工作目录；状态写入其 `workspace/` |
| `LOG_DIR` | `${WORK_DIR}/hcu_megatron/examples/aibenchmark` | 训练日志目录 |
| `MEGATRON_SCRIPT_PATH` | 空 | 训练参数来源，可指向 Shell、JSON 或 YAML 文件 |
| `LOG_PARSER_TYPE` | `base` | `base` 为 Megatron；`special` 为特定训练框架日志 |
| `MPI_LAUNCH_TIMEOUT` | `300` | MPI 启停命令超时，单位秒 |
| `INTERVAL_MONITOR` | `60` | 节点和恢复轮询基础间隔，单位秒 |
| `TRAIN_ALERT_THRESHOLD` | `20000` | 单步耗时告警阈值，单位毫秒 |
| `TRAIN_NO_UPDATE_THRESHOLD` | `1800` | 已有日志停止更新的阈值，单位秒 |
| `STARTUP_NO_LOG_TIMEOUT_SEC` | `1800` | 启动后首条有效日志等待时间，单位秒 |
| `FEISHU_WEBHOOK_URL` | 空 | 可选的飞书告警地址 |
| `BLACKLIST_PERSISTENCE_PATH` | `${WORK_DIR}/blacklist.json` | 黑名单持久化文件 |
| `BLACKLIST_PERSISTENCE_BACKUP_PATH` | `${WORK_DIR}/blacklist.json.bak` | 黑名单备份文件 |

`CLUSTER_SCHEDULE` 只接受 `NONE` 或 `SLURM`，`LOG_PARSER_TYPE` 只接受 `base` 或 `special`。配置拼写错误会导致启动失败。

## 3. 监控开关

下列布尔值接受 `true`、`1` 或 `yes`：

| 变量 | 默认值 | 说明 |
|---|---:|---|
| `ENABLE_HW_CHECK` | `true` | 是否执行硬件信息检查 |
| `ENABLE_NHC_FAULT_HANDLE` | `true` | 是否处理 NHC 故障 |
| `ENABLE_SLURM_CHECK` | `true` | 是否处理 Slurm 队列消失和重新提交；裸机模式没有 Slurm 管理器，因此不会查询 Slurm |
| `ENABLE_LOSS_GRAD_CHECK` | `true` | 是否检查 loss/grad 异常 |
| `ENABLE_REGULAR_NOTIFY` | `true` | 是否发送周期通知 |
| `ENABLE_ITER_DUMPER` | `true` | 是否落盘迭代信息 |
| `LOG_MONITOR_ENABLE_NO_UPDATE` | `true` | 是否启用日志无更新检测 |

## 4. 训练参数加载

配置文件路径只能通过环境变量提供：

```bash
export MEGATRON_SCRIPT_PATH=/path/to/train.sh
```

支持扩展名：

- `.sh`、`.bash`
- `.json`
- `.yaml`、`.yml`

主要读取的 Megatron 参数包括：

- TP、PP、CP、EP、ETP 和 sequence parallel。
- 模型层数、hidden size、attention heads、FFN、sequence length。
- micro/global batch size。
- save interval、train samples、eval interval。

Shell 文件使用 `--parameter value` 形式，例如：

```bash
TRAIN_ARGS=" \
  --tensor-model-parallel-size 8 \
  --pipeline-model-parallel-size 2 \
  --micro-batch-size 1 \
  --global-batch-size 1024
"
```

当前 Shell 解析器不保证识别 `--parameter=value`、复杂命令替换或未解析变量。加载异常会回退为空配置并记录日志，因此必须检查启动日志。

## 5. node_check 参数

节点性能筛机使用位置参数 `node_check`：

```bash
hcu-cluster-inspect node_check \
  --clushnode /path/to/nodes \
  --nodenum 4 \
  --tflops 185
```

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `--clushnode` | 无 | 待检查节点列表，必填 |
| `--nodenum` | `4` | 每组节点数 |
| `--tflops` | `185` | 通过 CLI 调用时的 TFLOPs 阈值 |
| `--healthy` | 脚本默认路径 | 健康节点输出文件 |
| `--fault` | 脚本默认路径 | 异常节点输出文件 |
| `--only-horizontal` | 关闭 | 仅运行横向阶段 |

直接执行 `cluster_manager/node_check/run_check.sh` 时，脚本自身默认阈值是 100；建议始终显式传入 `--tflops`，避免两种入口默认值不同。

## 6. 当前实现边界

- `MAX_RESTART_TIMES` 虽然可配置，但当前训练启动失败路径尚未统一使用该值限制重试次数。
- MPI 停止实现按 `python` 关键字终止目标节点进程，只适用于独占节点。
- `WORK_DIR` 中的状态文件不是配置文件，不能手工当作启动参数输入。
- 修改训练规模或模型参数前，应备份并检查 `WORK_DIR/workspace/` 的历史状态，避免错误恢复旧任务。
