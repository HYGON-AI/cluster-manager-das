<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# cluster_manager 常见故障排查

## 启动参数校验失败

### 缺少 Slurm 参数

当 `CLUSTER_SCHEDULE=SLURM` 时，`--job_id`、`--job_name` 和 `--sbatch_script` 都是必填项。裸机 hostfile 场景应设置：

```bash
export CLUSTER_SCHEDULE=NONE
```

### sbatch 作业名或节点数不一致

启动入口会检查 `#SBATCH -J/--job-name` 和 `#SBATCH -N/--nodes`。确保作业名与 `--job_name` 完全一致，并且申请节点数不少于 `--nodes_num`。

### hostfile 节点不足

裸机场景要求 hostfile 中唯一节点数不少于 `--nodes_num`。空行和以 `#` 开头的行不计入节点数。建议格式：

```text
node01 slots=8
node02 slots=8
```

## 启动后没有训练日志

依次检查：

1. 控制日志中是否出现 `[MPI-LAUNCH]` 和非零返回码。
2. `RUN_PATH/--exec` 是否可读，且能接收 slots hostfile 作为第一个参数。
3. 训练脚本中的工作目录、Conda 环境、共享路径和 checkpoint 路径是否对所有节点可见。
4. `LOG_DIR` 是否是训练实际写日志的目录。
5. 日志文件名是否符合 `LogMonitor` 的发现规则。
6. `LOG_PARSER_TYPE` 是否正确：Megatron 日志使用 `base`，其他已适配格式使用 `special`。

状态停留在 `PENDING` 通常表示训练已发起，但解析器尚未观察到可识别的正常迭代日志。

## NHC 检查失败

- 确认控制节点可以执行 `clush`，目标节点可以执行 `run_nhc`。
- 检查远程用户、SSH 免密、节点名解析和并发限制。
- 区分“节点检查未通过”和“NHC 命令没有成功执行”。后者的健康状态未知，不应自动恢复节点。
- 裸机场景不会调用 `sinfo`、`squeue` 和 `scontrol`；如果仍出现这些调用，检查 `CLUSTER_SCHEDULE` 是否在启动控制器前正确导出。

## Slurm 作业查询或提交失败

- 使用启动控制器的同一账号执行 `squeue -j <job_id>`。
- 确认 Job ID 尚未释放，Job Name 与 sbatch 文件一致。
- 检查 sbatch 脚本能否独立提交，以及 hostfile 是否可写。
- 控制器会对查询失败进行重试；持续失败时应先恢复 Slurm 命令和控制节点连接，不要反复启动多个控制器。

## 备用节点不足

检查 `${WORK_DIR}/workspace/.node_pool/` 下的节点集合：

- `total_nodes.txt` 是否包含预期节点。
- `abnormal_nodes.txt` 和黑名单是否排除了过多节点。
- `running_nodes.txt`、`backup_nodes.txt`、`abnormal_nodes.txt` 是否存在交叉或遗漏。
- Slurm allocation 是否仍包含所有候选节点。

不要直接修改单个节点文件来绕过约束。需要重建资源池时，先停止控制器并备份完整 workspace。

## 控制器重启后状态异常

- 查看 `running_state.json` 是否为完整 JSON。
- 检查当前训练拓扑与状态文件中的持久配置是否一致。
- 查看 `current_snapshot.json.tmp` 是否存在，以及控制日志中的恢复信息。
- 确认没有两个控制器同时使用同一个 `WORK_DIR`。

若确定要作为全新任务启动，应使用新的 `WORK_DIR`。不要只删除 `running_state.json`，否则节点池和快照可能与新状态不一致。

## 恢复循环或频繁重启

常见原因包括：

- 日志解析器类型错误，正常日志被判断为超时。
- 训练脚本启动后立即退出。
- NHC 在运行节点上持续报告同一故障。
- checkpoint 无效，训练每次从同一错误状态恢复。
- 节点数、slots 与 TP/PP/CP/EP 拓扑不匹配。

应同时保留控制日志、训练日志、`${WORK_DIR}/workspace` 和 NHC 输出，再判断是训练故障、节点故障还是控制器配置错误。

## 安全提示

当前 MPI launcher 停止训练时会在目标节点执行按 `python` 关键字匹配的强制终止命令。因此只能用于任务独占节点。若节点上存在其他 Python 服务，应先改造成按训练 PID、进程组或作业 ID 精确停止，再投入使用。
