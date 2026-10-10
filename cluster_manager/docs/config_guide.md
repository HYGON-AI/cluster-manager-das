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
| `CLUSTER_LAUNCH_MODE` | `mpi` | 启动器类型；支持 `mpi`、`docker`（以及兼容别名 `docker_exec`） |
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

Docker launcher 变量（仅 `CLUSTER_LAUNCH_MODE=docker` 使用）：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `CONTAINER_NAME` / `DOCKER_CONTAINER_NAME` | `cluster-manager` | 每个节点上的容器名；`DOCKER_CONTAINER_NAME` 优先 |
| `DOCKER_IMAGE` / `IMAGE` | 空（必填） | 容器镜像；节点本地不存在时自动 `docker pull` |
| `DOCKER_IMAGE_TAR` / `IMAGE_TAR` | 空 | 镜像 tar 包；存在时优先 `docker load` |
| `DOCKER_REUSE_CONTAINER` | `false` | 开启后复用同名容器：运行中直接使用，已停止则 `docker start`；不存在时按镜像配置创建。默认删除后重建 |
| `DOCKER_EXEC_PATH` | `--exec` 的路径 | 容器内训练脚本路径；宿主机与容器路径不同时必须显式设置 |
| `DOCKER_SLOTSFILE_PATH` | `--hostfile`/slots 文件路径 | 容器内可读的 slots 文件路径，通常要求挂载共享目录 |
| `DOCKER_WORKDIR` | 训练脚本父目录 | 容器内执行 `bash` 前切换到的工作目录 |
| `DOCKER_CONTAINER_SSH_PORT` | `36000` | 容器内 `sshd` 监听端口，供训练脚本的容器间 MPI 使用；这是容器端口，不是宿主机 SSH 端口 |
| `MPI_HOST_SSH_PORT` | `22` | 连接训练宿主机的 SSH 端口；优先于旧变量 `MPIRUN_PLM_RSH_ARGS` |
| `DOCKER_HOST_SHARE_ROOT` | 空 | 宿主机共享目录；与容器目录同时设置时自动挂载 |
| `DOCKER_CONTAINER_SHARE_ROOT` | 空 | 容器内共享目录 |
| `DOCKER_RUN_ARGS` | GPU/共享内存默认参数 | 追加或覆盖 `docker run` 参数 |
| `DOCKER_STOP_PATTERN` | 训练脚本文件名 | 容器内停止时传给 `pkill -f` 的匹配串 |
| `DOCKER_REMOVE_CONTAINER_ON_STOP` | `true` | 停止后是否执行 `docker rm -f`；`DOCKER_REUSE_CONTAINER` 开启时不生效 |
| `MPI_TCP_IF_INCLUDE` | 空 | 限定 PRTE/OMPI 的 TCP 选路（网卡名或网段）。不设置时 HNP 会连同 `docker0`、CNI 地址一起对外通告，远端 daemon 可能选到不可达地址并超时 |
| `MPI_RANK_ENV` | 空 | `VAR=value,VAR2=value2`，下发给**所有节点所有 rank** 的环境变量 |
| `MPI_FORWARD_ENV` | 空 | 变量名列表（逗号分隔），取**协调节点容器内的当前值**下发给所有 rank |

在 `CLUSTER_LAUNCH_MODE=docker` 下，容器准备由 `clush --hostfile` 并发执行到所有训练节点，训练脚本只在
hostfile 第一台节点的容器内通过 `docker exec -d` 启动。

`MPI_RANK_ENV` / `MPI_FORWARD_ENV` 通过 Open MPI 的 `mca_base_env_list` 下发，而不是简单
`export`：mpirun 不会把任意环境变量转发给远端 rank，只 `export` 的话仅协调节点的 rank 生效。
多网卡集群上这一点很关键——例如 `NCCL_SOCKET_IFNAME` 只在一侧生效时，两个节点会各自
自动选网卡，bootstrap 连接直接被对端 reset。

`MPI_FORWARD_ENV` 针对的是 docker 模式特有的环境不对称：协调节点的 rank 由 `docker exec`
拉起，带着镜像的 `ENV`；对端节点的 rank 由 mpirun 经容器 sshd 拉起，**不带镜像 ENV**。
典型后果是 `LD_LIBRARY_PATH` 丢失，对端找不到 RCCL 网络插件而退化成 `NET/Socket`，
协调节点却在用 IB 插件，两边传输类型不一致，报 `socketFinalizeAccept: wrong type 3 != 4`：

```bash
export MPI_FORWARD_ENV="LD_LIBRARY_PATH"
export MPI_RANK_ENV="NCCL_SOCKET_IFNAME=ib1,GLOO_SOCKET_IFNAME=ib1"
```

注意优先级：rank 启动后如果训练脚本再 `source` 一份 env 并导出同名变量，会覆盖这里的值，
同一个变量不要两处都设。

Docker 模式会先通过 `clush --hostfile` 并发在每个训练节点执行镜像检查、
`docker load`/`docker pull`、旧容器清理和 `docker run -dit`；容器准备完成后，
通过宿主机 SSH 连接到实际 slots hostfile 的第一台训练节点，在该节点的容器内执行一次
`docker exec -d` 启动训练脚本。

设置 `DOCKER_REUSE_CONTAINER=1` 时，先检测同名容器；存在则跳过镜像准备和容器重建，保留原镜像、挂载和启动参数。`DOCKER_IMAGE` 仍需配置，供没有容器的节点创建使用。若停止训练后也需要保留容器，同时设置 `DOCKER_REMOVE_CONTAINER_ON_STOP=0`。复用运行中的容器不会判断其中是否已有训练任务。

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
