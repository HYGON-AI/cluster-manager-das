<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# Hygon FT 故障模拟与验证

本文用于在测试集群验证 NHC、FaultEvent、节点隔离和训练任务恢复链路。不要在生产训练节点或仍承载业务的节点上执行故障模拟。

## 1. 验证目标

完整的 NHC 故障验证应观察到：

```text
NodeHealth Agent 检测模拟故障
  -> 创建 NodeHealthCheckFailed FaultEvent
  -> Operator 处理 FaultEvent
  -> 节点添加 ft.hygon.io/node-unhealthy:NoSchedule
  -> 训练任务停止或删除故障 Pod
  -> Volcano/Training Operator 在健康节点重建训练任务
```

测试前确认：

- 目标节点没有不能中断的训练或业务 Pod。
- 集群至少还有足够的健康训练节点完成任务重建。
- FT 控制面、NodeHealth Agent 和训练任务处于正常状态。
- 训练任务已经保存可恢复的 checkpoint。

## 2. 选择目标 NodeHealth Agent

```bash
TARGET_NODE=train-node-1

POD=$(kubectl -n hygon-ft get pod \
  -l app=nodehealth-agent \
  --field-selector spec.nodeName="${TARGET_NODE}" \
  -o jsonpath='{.items[0].metadata.name}')

echo "${POD}"
```

确认 Pod 与目标节点对应：

```bash
kubectl -n hygon-ft get pod "${POD}" -o wide
```

## 3. 模拟 NHC 故障

```bash
bash scripts/simulate_nhc_fault.sh "${POD}"
```

脚本通过 NodeHealth Agent 在目标节点的 `/tmp/hygon-ft-nhc-fail` 创建模拟标记。后续 NHC 检查会将该节点判定为异常。

## 4. 观察处理过程

分别观察 NodeHealth Agent、FaultEvent、Operator、节点 taint 和训练 Pod：

```bash
kubectl -n hygon-ft logs "${POD}" --tail=200 -f
kubectl -n hygon-ft get faultevents -w
kubectl -n hygon-ft logs deployment/ft-operator --tail=200 -f
kubectl get node "${TARGET_NODE}" \
  -o custom-columns='NODE:.metadata.name,TAINTS:.spec.taints'
kubectl get pods -A -l ft.hygon.io/enabled=true -o wide -w
```

检查最终 FaultEvent：

```bash
kubectl -n hygon-ft describe faultevent <event-name>
```

验证结果应满足：

- FaultEvent 类型为 `NodeHealthCheckFailed`。
- `spec.nodeName` 是目标节点。
- `status.processed` 为 `true`。
- `status.actions` 中节点 taint 成功。
- 目标节点带有 `ft.hygon.io/node-unhealthy:NoSchedule`。
- 训练任务按工作负载策略完成重建。

## 5. 清理 NHC 模拟故障

```bash
bash scripts/clear_nhc_fault.sh "${POD}"
```

该脚本删除目标节点上的模拟标记，并删除 Hygon FT unhealthy taint。清理后检查：

```bash
kubectl get node "${TARGET_NODE}" \
  -o custom-columns='NODE:.metadata.name,TAINTS:.spec.taints'
kubectl -n hygon-ft logs "${POD}" --tail=100
```

如果节点存在真实硬件或网络故障，不要使用清理脚本直接恢复调度，应先修复节点并执行：

```bash
bash start.sh recover "${TARGET_NODE}"
```

## 6. 模拟 NodeNotReady

仓库提供实验性脚本：

```bash
bash scripts/simulate_node_notready_fault.sh "${TARGET_NODE}"
```

部分 Kubernetes 集群不允许普通 `kubectl patch node` 修改 Node status，此时脚本不会真正改变 Ready condition。需要验证真实 NodeNotReady 时，只能在确认安全的测试节点上停止 kubelet 或隔离节点网络，并准备节点外恢复手段。

观察：

```bash
kubectl get node "${TARGET_NODE}" -w
kubectl -n hygon-ft get faultevents -w
kubectl -n hygon-ft logs deployment/ft-operator -f
```

Operator 默认在节点持续 NotReady 超过宽限时间后创建 `NodeNotReady` FaultEvent。

## 7. 测试结束检查

```bash
kubectl -n hygon-ft get faultevents -o wide
kubectl get nodes \
  -o custom-columns='NODE:.metadata.name,READY:.status.conditions[?(@.type=="Ready")].status,TAINTS:.spec.taints'
kubectl get pods -A -l ft.hygon.io/enabled=true -o wide
```

确认没有遗留模拟标记、非预期 unhealthy taint、Pending 训练 Pod或持续重建的训练任务。
