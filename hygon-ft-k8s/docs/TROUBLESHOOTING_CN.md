<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# Hygon FT 常见问题排查

本文按可观察症状组织安装、调度、日志和故障恢复问题。配置值以[训练任务配置](WORKLOAD_CONFIGURATION_CN.md)为准，故障动作以[故障处理语义](FAULT_HANDLING_CN.md)为准。建议先执行：

```bash
bash start.sh status
kubectl -n hygon-ft get pods -o wide
kubectl -n hygon-ft get faultevents -o wide
```

## 1. 训练 Pod 没有创建

先确认工作负载对象是否存在：

```bash
kubectl get pytorchjobs,jobs.batch.volcano.sh -A
kubectl describe pytorchjob <job-name> -n <namespace>
kubectl describe job.batch.volcano.sh <job-name> -n <namespace>
```

重点检查：

- 是否只执行了 `start.sh install`，没有执行 `submit_job.sh`。
- YAML 中的 namespace 与提交命令是否一致。
- PyTorchJob 或 Volcano Job CRD 是否已经安装。
- Admission Webhook 是否可用，API Server 是否返回 Webhook 错误。
- Training Operator 或 Volcano Controller 是否正常运行。

## 2. 训练 Pod 长期 Pending

```bash
kubectl describe pod <pod-name> -n <namespace>
kubectl get nodes -o wide
kubectl get nodes -o custom-columns='NODE:.metadata.name,READY:.status.conditions[?(@.type=="Ready")].status,TAINTS:.spec.taints'
```

重点检查：

- HCU、CPU 或内存是否不足。
- 节点是否带有任务没有 toleration 的 taint。
- 节点亲和、反亲和或 selector 是否无法满足。
- PVC、hostPath 或共享目录是否不可用。
- `minAvailable` 是否大于可调度副本数。
- PyTorchJob 的 Volcano gang scheduling 是否已经生效。

gang scheduling 生效时，资源不足会导致整组 Pod 保持 Pending，而不是部分 Pod 先 Running。

## 3. 没有训练日志

依次检查：

1. 是否已经提交训练任务，而不只是安装 FT 控制面。
2. 训练 Pod 是否处于 Running。
3. Job 顶层是否存在 `ft.hygon.io/enabled: "true"` label 和 annotation。
4. Webhook 是否注入 ft-launcher。
5. `ft.hygon.io/log-dir` 是否在所有训练节点挂载且可写。
6. `log-monitor-command` 及其 Python 依赖是否能在训练容器中访问。
7. `launcher-<pod>.log` 是否存在命令、权限或 Python 导入错误。

检查 Webhook 注入结果：

```bash
kubectl apply --dry-run=server -o yaml -f "${TRAIN_YAML}" | \
grep -E 'ft-launcher|ft.hygon.io/injected'
```

检查训练 Pod：

```bash
kubectl get pod <pod-name> -n <namespace> -o yaml
kubectl logs <pod-name> -n <namespace> --tail=200
```

## 4. LogMonitor 没有识别故障

检查：

- `ft.hygon.io/log-monitor-roles` 是否包含当前 Pod 角色。
- LogMonitor 是否使用 `--log-file "${FT_LOG_FILE}" --mode k8s`。
- `launcher-<pod>.log` 是否显示 LogMonitor 已启动。
- `train-<pod>.log` 中的异常格式是否符合现有解析规则。
- marker 和 `log-monitor-events-<pod>.jsonl` 是否生成。
- 指定 `--log-file` 时，当前 Pod 是否实际写入同一个文件。

数值和训练趋势解析主要适配 Megatron 日志。普通 PyTorch 自定义日志需要增加解析规则或输出统一格式。

## 5. Pod 反复删除和重建

```bash
kubectl -n hygon-ft get faultevents -o wide
kubectl -n hygon-ft describe faultevent <event-name>
kubectl -n hygon-ft logs deployment/ft-operator --tail=300
```

重点查看：

- `FaultEvent.status.actions` 中最终选择的根因。
- 故障节点 taint 是否成功。
- 同一任务其他事件是否已标记为 suppressed。
- launcher 是否在 Operator 确认处理完成后才退出。
- 故障节点是否在恢复前被手工删除 taint。
- 训练脚本是否从 checkpoint 恢复后再次触发同一个应用错误。

不要只根据最后出现的通信错误判断根因，应优先查看明确节点故障和 Torchrun Root Cause。

## 6. 故障节点仍被重新调度

```bash
kubectl get node <node-name> \
  -o custom-columns='NODE:.metadata.name,TAINTS:.spec.taints'
```

确认节点存在：

```text
ft.hygon.io/node-unhealthy:NoSchedule
```

`NoSchedule` 不会驱逐已经运行的 Pod，因此 Operator 必须在 taint 成功后再删除或停止训练 Pod。如果 taint 失败，检查 Operator RBAC 和 `FaultEvent.status.actions`。

## 7. FT 控制面异常

```bash
kubectl -n hygon-ft get pods -o wide
kubectl -n hygon-ft describe deployment ft-operator
kubectl -n hygon-ft describe deployment ft-webhook
kubectl -n hygon-ft describe daemonset nodehealth-agent
kubectl -n hygon-ft logs deployment/ft-operator --tail=200
kubectl -n hygon-ft logs deployment/ft-webhook --tail=200
```

重点检查镜像是否已加载到目标节点、system/training label 是否正确、system taint 与 toleration 是否匹配，以及 Webhook TLS Secret 是否存在。

## 8. NHC 没有自动隔离节点

```bash
kubectl -n hygon-ft get pod -l app=nodehealth-agent -o wide
kubectl -n hygon-ft logs <nodehealth-agent-pod> --tail=300
```

NHC 返回值处理：

| 结果 | 处理 |
|---|---|
| `0` | 节点健康 |
| `2` | 创建 `NodeHealthCheckFailed` FaultEvent |
| `3` | report-only，只记录不隔离 |
| 探针异常 | 默认只记录，避免误隔离 |
| 检查超时 | 默认只记录；启用 `NHC_TIMEOUT_IS_FAILURE` 后按故障处理 |

如果返回值与预期不符，先在节点上直接运行 `run_nhc`，再检查 NodeHealth Agent 是否通过 `nsenter` 找到了同一命令。完整的 NHC 动作语义参见[故障处理语义](FAULT_HANDLING_CN.md)。

## 9. 节点恢复后仍带 taint

推荐执行完整恢复检查：

```bash
bash start.sh recover <fault-node> <normal-node-1>,<normal-node-2>
```

检查全部通过后，程序只删除 Hygon FT 管理的 unhealthy taint。确认节点已经恢复且无需自动检查时，可以手工删除：

```bash
kubectl taint node <fault-node> ft.hygon.io/node-unhealthy-
```

不要在节点硬件、网络或 NHC 仍异常时直接删除 taint。
