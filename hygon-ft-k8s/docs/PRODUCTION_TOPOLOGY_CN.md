<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# 生产部署拓扑

本文只说明生产环境的节点分工、高可用和容量要求。具体故障动作以[故障处理语义](FAULT_HANDLING_CN.md)为准，日常操作参见[运行与恢复手册](OPERATIONS_AND_RECOVERY_CN.md)。

## 1. 节点角色

建议将管理面和训练面分开：

```text
system node:
  - ft-operator Deployment
  - ft-webhook Deployment

training node:
  - nodehealth-agent DaemonSet
  - PyTorchJob 或 Volcano Job 训练 Pod
```

需要先给节点打标签：

```bash
SYSTEM_NODES="system-node-1 system-node-2" \
TRAINING_NODES="train-node-1 train-node-2 train-node-3 train-node-4 train-node-5 train-node-6 train-node-7 train-node-8" \
bash scripts/label_nodes_example.sh
```

`system` 节点会被加 taint：

```text
node-role.hygon.io/system=true:NoSchedule
```

`ft-operator` 和 `ft-webhook` 具有对应 toleration，可以调度到 system 节点；普通训练 Pod 不应容忍该 taint。NodeHealth Agent 具有 training `nodeSelector`，只在训练节点运行。

## 2. 管理面 HA

`ft-operator` 现在是：

```yaml
replicas: 2
```

两个 Pod 都会启动 `/report` HTTP 接口，但只有 leader 处理 `FaultEvent`。

leader election 通过 Kubernetes Lease 实现：

```text
coordination.k8s.io/v1 Lease
namespace: hygon-ft
name: ft-operator-leader
```

查看当前 leader：

```bash
kubectl -n hygon-ft get lease ft-operator-leader -o yaml
```

如果 leader 所在节点宕机，另一个 `ft-operator` Pod 会续租成为新 leader。

生产环境至少应提供两个 system 节点，并将两个 `ft-operator` 副本分散部署。Webhook 也应避免形成单节点故障点；实际副本数和反亲和规则需结合集群规模配置。

## 3. 8 节点 64 HCU 示例

示例文件：

```text
manifests/examples/pytorchjob-8n64g-ft.yaml
```

规模：

```text
Master: 1 Pod x 8 HCU
Worker: 7 Pod x 8 HCU
Total: 8 Pod x 8 HCU = 64 HCU
```

提交：

```bash
bash scripts/submit_job.sh default \
  manifests/examples/pytorchjob-8n64g-ft.yaml
```

该文件是配置骨架，不代表目标集群一定有 64 HCU 可用。提交前按[训练任务配置](WORKLOAD_CONFIGURATION_CN.md)调整镜像、存储、资源名、副本数和 `minAvailable`。

## 4. 容量与恢复余量

容错只能隔离故障节点并触发任务恢复，不能生成新的计算资源。要让 8 节点任务在单节点故障后继续运行，至少还需要一个满足相同 HCU、CPU、内存、存储和网络条件的备用训练节点。没有恢复余量时，重建后的 Pod 会保持 Pending。

生产容量规划还应确认：

- gang scheduling 所需的全部副本能够同时调度。
- checkpoint 位于故障节点之外，且所有候选节点均可访问。
- 训练镜像和 LogMonitor 依赖可从所有候选节点获取。
- 故障节点的 unhealthy taint 不会被其他自动化提前删除。

## 5. 故障检测覆盖

- 节点仍可运行进程时，NodeHealth Agent 调用宿主机 `run_nhc`；只有明确故障结果才触发隔离，report-only 和探针异常不应混同处理。
- 节点断电或 kubelet 失联时，本机 Agent 无法上报，由 Operator 的 Node watcher 在宽限期后创建 `NodeNotReady` FaultEvent。
- 训练日志异常由外部 LogMonitor、marker 和 launcher 链路上报。

返回值、超时和工作负载恢复差异参见[故障处理语义](FAULT_HANDLING_CN.md)。

## 6. 系统边界

这套代码仍然不做调度器：

```text
ft-operator 不选择具体节点，也不修复硬件
ft-operator 负责记录事件、隔离节点，并按故障类型和工作负载采取动作
重新放置 Pod 由 kube-scheduler、Volcano 和相应工作负载控制器完成
```

