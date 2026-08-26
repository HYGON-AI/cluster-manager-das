<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# cluster_manager 部署与启动指南

本文说明当前支持的裸机 hostfile + MPI 和 Slurm allocation + MPI 两种部署方式。Kubernetes 训练容错请使用同仓库的 `hygon-ft-k8s`。

## 1. 环境要求

- Linux 控制节点或登录节点。
- Python 3.10、3.11 或 3.12。
- 控制节点能够访问训练脚本、hostfile、训练日志和 `WORK_DIR`。
- 训练节点之间已经配置 MPI、SSH 或集群要求的远程启动方式。
- `mpirun` 和 `clush` 可用；当前停止流程会通过 `clush` 在目标节点执行命令。
- 使用 Slurm 时，`squeue`、`sinfo`、`scontrol` 和 `sbatch` 可用。
- 启用 NHC 时，目标节点能够执行现场提供的 `run_nhc`。

当前停止实现会在 hostfile 节点上执行 `pkill -9 -f python`。只能在训练任务独占的节点上使用，不能与其他 Python 任务共享节点。

## 2. 源码运行与可选安装

项目不要求先安装 Python 包。确保运行环境已经具备 `setup.py` 中声明的依赖后，可以从仓库根目录直接验证源码入口：

```bash
python3 cluster_manager/cluster_manager/main.py --help
```

仓库提供的 `examples/start_none.sh`、`examples/start_slurm.sh` 和 `start.sh` 都直接调用源码，现场使用时只需把其中的源码绝对路径改为实际仓库位置。

如需在环境中使用 `hcu-cluster-inspect` 命令，也可以选择 editable 安装：

```bash
cd cluster_manager
python3 -m pip install -e .
hcu-cluster-inspect --help
```

构建 wheel 也是可选发布方式；发布 wheel 前必须确认其中包含 `cluster_manager/node_check` 所需的 Shell 资源。

## 3. 准备输入文件

### 3.1 hostfile

每行填写一个节点名。文件可以包含额外的 `slots=N` 字段，但节点池读取时只使用每行第一个字段：

```text
train-node-01 slots=8
train-node-02 slots=8
train-node-03 slots=8
train-node-04 slots=8
```

`--nodes_num` 表示每轮训练实际使用的节点数。hostfile 中多出的健康节点作为备用节点。

### 3.2 训练启动脚本

`--exec` 指向真正启动训练的 Shell 脚本。MPI launcher 会在该脚本所在目录执行：

```bash
bash /path/to/run.sh /path/to/generated-slots-file
```

因此启动脚本必须把第一个位置参数当作本轮生成的 MPI slots hostfile，并负责设置训练环境、调用 `mpirun` 或现场训练入口。

### 3.3 训练参数来源

通过环境变量 `MEGATRON_SCRIPT_PATH` 指向包含 Megatron 参数的 Shell、JSON 或 YAML 文件：

```bash
export MEGATRON_SCRIPT_PATH=/path/to/train.sh
```

Shell 解析器支持 `--parameter value` 形式。无法解析的变量表达式会得到空值，因此启动前应查看控制日志中的训练配置。

### 3.4 Slurm 输入

仅当 `CLUSTER_SCHEDULE=SLURM` 时准备：

- 已存在或允许重新提交的 Job ID。
- 与 `--job_name` 一致的 sbatch 作业名。
- `#SBATCH -N` 不小于 `--nodes_num` 的 sbatch 脚本。

## 4. 裸机 hostfile + MPI

复制示例并修改其中的路径：

```bash
cp examples/start_none.sh start.local.sh
vi start.local.sh
bash -n start.local.sh
bash start.local.sh
```

关键配置：

```bash
export CLUSTER_LAUNCH_MODE=mpi
export CLUSTER_SCHEDULE=NONE
export WORK_DIR=/path/to/controller-workdir
export LOG_DIR=/path/to/training-logs
export MEGATRON_SCRIPT_PATH=/path/to/train.sh
```

裸机场景要求 `--nodes_num`、`--slots`、`--exec` 和 `--hostfile`，不要求 Job ID、Job Name 或 sbatch 脚本。

## 5. Slurm allocation + MPI

复制 Slurm 示例并修改：

```bash
cp examples/start_slurm.sh start.slurm.local.sh
vi start.slurm.local.sh
bash -n start.slurm.local.sh
bash start.slurm.local.sh
```

关键配置：

```bash
export CLUSTER_LAUNCH_MODE=mpi
export CLUSTER_SCHEDULE=SLURM
```

Slurm 场景额外要求 `--job_id`、`--job_name` 和 `--sbatch_script`。启动时会校验 sbatch 作业名和节点数，并从 Slurm 更新 hostfile。

## 6. 使用 `start.sh` 模板

根目录下的 `cluster_manager/start.sh` 是现场配置模板，不是开箱即用脚本。使用前必须替换：

- `LOG_DIR`、`TRIAN_PATH`、`RUN_PATH` 和 `HOSTFILE`。
- 节点数和每节点进程数。
- Conda 初始化脚本与环境名。
- `nohup` 的 Python 路径和控制日志路径。
- 裸机或 Slurm 对应的 `CLUSTER_SCHEDULE` 及参数。

建议优先从 `examples/start_none.sh` 或 `examples/start_slurm.sh` 创建本地启动脚本，避免误用 `start.sh` 中的占位路径。

## 7. 启动后验证

检查控制进程和日志：

```bash
ps -ef | grep '[h]cu-cluster-inspect\|[c]luster_manager.main'
tail -f /path/to/cluster-manager.log
```

至少确认：

- 启动参数校验通过。
- `WORK_DIR/workspace/` 可以写入。
- hostfile 节点数量满足 `--nodes_num`。
- 日志中出现节点池初始化和 `[MPI-LAUNCH]`。
- 训练日志出现在 `LOG_DIR`，且 LogMonitor 能识别迭代信息。

状态文件和恢复语义见[故障恢复与日常操作](recovery_workflow.md)，启动失败见[常见故障排查](TROUBLESHOOTING_CN.md)。

## 8. 停止与可选卸载

先停止控制进程。当前版本没有独立的安全远程停止子命令；如果还需要停止训练，必须在确认目标节点独占后按现场作业流程处理。

直接运行源码不需要卸载。只有执行过 editable 安装时才需要使用：

```bash
python3 -m pip uninstall hcu-cluster-inspect
```

卸载 Python 包不会删除以下运行数据：

- `WORK_DIR/workspace/`
- 黑名单及备份文件
- 训练日志和 checkpoint
- Slurm Job

确认不再需要且已经备份后，再由管理员清理这些目录或作业。
