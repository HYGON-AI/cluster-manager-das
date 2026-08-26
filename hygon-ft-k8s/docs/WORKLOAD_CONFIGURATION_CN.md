<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# Hygon FT 训练任务配置

本文是训练任务配置的唯一说明，介绍如何为 Volcano Job 和 PyTorchJob 开启 Hygon FT，以及两种工作负载各自必须满足的约束。安装控制面参见[安装指南](INSTALLATION_CN.md)，提交和恢复命令参见[运行与恢复手册](OPERATIONS_AND_RECOVERY_CN.md)。

## 1. 公共容错配置

PyTorchJob 和 Volcano Job 顶层都需要设置容错 label 和 annotation：

```yaml
metadata:
  labels:
    ft.hygon.io/enabled: "true"
  annotations:
    ft.hygon.io/enabled: "true"
    ft.hygon.io/inject-launcher: "true"
    ft.hygon.io/log-dir: /path/to/shared_root/workspace/hygon-ft
    ft.hygon.io/log-monitor-roles: all
    ft.hygon.io/log-monitor-command: >-
      python /path/to/log_monitor.py --log-file "${FT_LOG_FILE}" --mode k8s
    ft.hygon.io/exit-after-fault-event-report: "true"
```

`metadata.labels.ft.hygon.io/enabled` 必须是字符串 `"true"`，否则 Admission Webhook 的 object selector 不会匹配该任务。

## 2. Annotation 说明

| annotation | 默认值 | 说明 |
|---|---|---|
| `ft.hygon.io/enabled` | `false` | 开启任务容错注入 |
| `ft.hygon.io/inject-launcher` | `true` | 使用 ft-launcher 包装训练命令 |
| `ft.hygon.io/runtime-image` | 控制器镜像 | 可选的单任务 runtime 镜像覆盖 |
| `ft.hygon.io/launcher-path` | `/opt/hygon-ft/ft-launcher` | 训练 Pod 内的 launcher 路径 |
| `ft.hygon.io/launcher-interpreter` | 空 | 共享脚本需要显式解释器时可设为 `/bin/bash` |
| `ft.hygon.io/log-dir` | 空 | 持久化训练和 launcher 日志目录 |
| `ft.hygon.io/log-file` | `/tmp/hygon-ft-train.log` | 精确日志文件；配置 `log-dir` 时通常不设置 |
| `ft.hygon.io/log-monitor-roles` | `all` | 启动 LogMonitor 的任务角色 |
| `ft.hygon.io/log-monitor-command` | 空 | LogMonitor 命令，必须监控 `${FT_LOG_FILE}` |
| `ft.hygon.io/fault-marker-file` | `/tmp/hygon-ft-fault.json` | LogMonitor 与 launcher 间的 marker |
| `ft.hygon.io/exit-after-fault-event-report` | `true` | Operator 确认后是否让 launcher 非零退出 |
| `ft.hygon.io/fault-event-ack-timeout-seconds` | `30` | 等待 Operator 处理的最长时间 |
| `ft.hygon.io/fault-event-ack-interval-seconds` | `1` | 查询 FaultEvent 状态的间隔 |
| `ft.hygon.io/scan-interval-seconds` | `10` | launcher 检查 marker 的间隔 |

Webhook 自动注入 Pod、节点、任务和日志上下文，不要在训练模板中重复配置对应的 `FT_*` 环境变量，也不要再次手工包装 launcher。

默认情况下，每个训练 Pod 都运行 LogMonitor，以捕获本地异常。Webhook 只为最后一个活动副本组启用日志无更新检查，并通过 `LOG_MONITOR_LAST_NODE_ONLY=true` 让 LogMonitor 在该组中选择最后一个节点执行检查，避免不打印 iteration 的 Pod 误报 hang。

## 3. LogMonitor 接入

默认 runtime initContainer 只将 `ft-launcher` 复制到训练容器，不会复制完整的 LogMonitor Python 包。LogMonitor 可以通过以下方式提供：

- 在训练 Pod 中挂载共享代码目录，让 `log-monitor-command` 指向共享目录中的 `log_monitor.py`。
- 将 LogMonitor 和依赖直接安装到训练镜像中。

无论采用哪种方式，LogMonitor 都应使用 Webhook 提供的 `${FT_LOG_FILE}`，例如：

```yaml
ft.hygon.io/log-monitor-command: >-
  python /path/to/log_monitor.py --log-file "${FT_LOG_FILE}" --mode k8s
```

## 4. Volcano Job 约束

```yaml
spec:
  minAvailable: 4
  tasks:
    - name: master
      replicas: 1
      policies:
        - events:
            - PodEvicted
            - PodFailed
          action: RestartJob
    - name: worker
      replicas: 3
      policies:
        - events:
            - PodEvicted
            - PodFailed
          action: RestartJob
```

Volcano Job 必须满足：

- `schedulerName` 设置为 `volcano`。
- `spec.minAvailable` 不大于 task 副本总数。
- task 副本总数、训练节点数和 `NNODES` 一致。
- 所有 task 都配置 `PodFailed/PodEvicted -> RestartJob`，确保任一训练 Pod 失败或被驱逐时重建整个任务。task 级策略会覆盖 Job 级默认策略，不要只为部分 task 配置。
- 一节点一个训练 Pod 时，`FT_RANKS_PER_POD` 与每 Pod 进程数一致。

运行中的 Volcano Job 不支持更新 Pod 模板。修改 annotation、训练命令或 launcher 配置后，应在 checkpoint 安全点删除并重新提交：

```bash
kubectl delete -f "${TRAIN_YAML}"
bash scripts/submit_job.sh <namespace> "${TRAIN_YAML}"
```

## 5. PyTorchJob 约束

```yaml
spec:
  runPolicy:
    schedulingPolicy:
      minAvailable: 4
```

PyTorchJob 必须满足：

- 集群已安装 Kubeflow Training Operator。
- 集群已安装 Volcano PodGroup CRD。
- `minAvailable` 等于 Master 和 Worker 副本总数。
- Training Operator 使用 `--gang-scheduler-name=volcano`。

传入 PyTorchJob YAML 时，`install.sh` 会检查这些条件，并在允许时为 Training Operator 补充 gang scheduler 参数。如果 Training Operator 完全由外部 Helm 或 Kustomize 管理，设置：

```bash
export AUTO_CONFIGURE_TRAINING_OPERATOR_GANG_SCHEDULER=false
```

此时安装脚本只检查参数；缺少配置时直接报错，不修改 Training Operator Deployment。

## 6. 示例与提交前检查

- `manifests/examples/` 提供通用配置骨架；其中的镜像、路径和资源数仍需按集群修改。
- `docs/train_volcano.yaml` 是较完整的环境适配参考，不是可直接运行的示例。文件中的 `your-registry.example.com/...` 是镜像占位符；提交前必须替换 master 和 worker 的训练镜像，并调整本地路径、wheel 和 hostPath。
- 训练镜像、共享目录和 checkpoint 路径在所有训练节点可用。
- `NNODES`、副本数、`minAvailable` 和实际训练节点数一致。
- HCU、CPU、内存和 `/dev/shm` 申请符合节点资源。
- `metadata.namespace` 与 `submit_job.sh` 的 namespace 参数一致。
- `ft.hygon.io/log-dir` 已挂载且训练容器可写。
- `log-monitor-command` 引用的代码和 Python 依赖在训练容器中存在。
