<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# hygon-ft Kubernetes 运行与恢复手册

本文面向日常操作，说明任务提交、状态观察、故障确认和节点恢复。配置任务前先阅读[训练任务配置](WORKLOAD_CONFIGURATION_CN.md)，故障机制见[故障处理语义](FAULT_HANDLING_CN.md)。

## 1. 提交训练任务

提交前确认镜像、共享目录、checkpoint、HCU 资源、副本数和 FT annotation 均已按现场环境配置。

PyTorchJob 示例：

```bash
bash scripts/submit_job.sh default manifests/examples/pytorchjob-2n16g-ft.yaml
```

Volcano Job 示例：

```bash
bash scripts/submit_job.sh default manifests/examples/train_llama_volcano.yaml
```

第一个参数是 Kubernetes namespace。它必须与 YAML 的 `metadata.namespace` 一致，不是 Volcano queue。

## 2. 观察运行状态

先执行只读状态检查：

```bash
bash start.sh status
TRAIN_YAML=/path/to/train.yaml bash scripts/check_status.sh
```

常用 Kubernetes 查询：

```bash
kubectl -n hygon-ft get deployments,daemonsets,pods,services -o wide
kubectl get pods -A -l ft.hygon.io/enabled=true -o wide
kubectl -n hygon-ft get faultevents -o wide
kubectl get nodes -o custom-columns=NAME:.metadata.name,TAINTS:.spec.taints
```

控制面日志：

```bash
kubectl -n hygon-ft logs deployment/ft-operator --tail=200
kubectl -n hygon-ft logs deployment/ft-webhook --tail=200
kubectl -n hygon-ft logs daemonset/nodehealth-agent --tail=200
```

训练 Pod 持久化目录中的常见文件：

| 文件 | 内容 |
|---|---|
| `launcher-<pod>.log` | launcher 启动、上报和确认过程 |
| `train-<pod>.log` | 训练标准输出和错误输出 |
| `log-monitor-events-<pod>.jsonl` | LogMonitor 结构化事件 |
| `.offset/<pod-uid>/*.offset` | 同一 Pod 内监控进程重启后的读取偏移 |

## 3. 故障后确认

发现训练停止、重建或节点被隔离时，按顺序检查：

1. 在训练日志和 `log-monitor-events-*.jsonl` 中确认最早故障。
2. 查看 `launcher-*.log` 是否成功上报并获得 Operator 确认。
3. 查看 FaultEvent 的 `status.processed` 和 `status.actions`。
4. 查看 Operator 日志中的根因选择、taint 和 Pod 删除结果。
5. 确认新 Pod 没有调度到故障节点。
6. 确认所有 rank 重新建立通信并从有效 checkpoint 继续训练。

```bash
kubectl -n hygon-ft describe faultevent <event-name>
kubectl describe node <fault-node>
kubectl get pods -A -l ft.hygon.io/enabled=true -o wide
```

Volcano Job 还应确认 `PodFailed/PodEvicted -> RestartJob` 实际触发；PyTorchJob 应根据故障类型确认是整组 Pod 删除还是 replica `OnFailure` 恢复。

## 4. 恢复故障节点

只有硬件、驱动、网络和宿主机 `run_nhc` 均恢复后，才可以移除 Hygon FT taint。

由程序自动选择正常参考节点：

```bash
bash start.sh recover fault-node
```

显式指定两个正常参考节点：

```bash
bash start.sh recover fault-node normal-node-1,normal-node-2
```

无人值守执行必须显式确认：

```bash
CONFIRM_TAINT_RECOVERY=yes \
bash start.sh recover fault-node normal-node-1,normal-node-2
```

恢复检查会执行多轮宿主机和 Pod 内检查，并比较目标节点与参考节点。必须满足：

- 目标节点和参考节点没有正式训练任务。
- 每一轮检查均通过。
- 目标节点不再出现原故障。
- 程序只删除 `ft.hygon.io/node-unhealthy:NoSchedule`，不修改其他 taint。

恢复后复核：

```bash
kubectl describe node fault-node | sed -n '/Taints:/,/Unschedulable:/p'
kubectl -n hygon-ft get faultevents -o wide
```

任一检查失败时保留 taint，按[常见问题排查](TROUBLESHOOTING_CN.md)处理，不要直接执行 `kubectl taint ...-` 绕过验证。

## 5. 故障演练

NHC 和 NodeNotReady 的完整演练步骤统一维护在[故障模拟与验证](FAULT_INJECTION_CN.md)。只能在测试集群或确认空闲的节点执行，演练结束后必须复核 FaultEvent、taint、训练任务和 checkpoint 状态。
