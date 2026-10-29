<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# HCU 训练可靠性工具集端到端使用流程

本文保存仓库级的执行顺序和场景衔接。具体命令、恢复边界和故障排查以各子项目 docs 为准。

## 推荐使用流程

```text
训练前环境验收
    hcu-envcheck
         ↓
选择训练平台
    ├── Slurm/MPI/Docker：hcu-training-reliability
    └── Kubernetes：hygon-ft-k8s
         ↓
训练 hang 时采集和分析堆栈
    stack-analyzer
```

`hcu_resiliency_ext` 是第三方容错库适配层，不是上述主流程的必选组件。

### 第一步：执行训练前检查

先在 Slurm 等资源管理系统中分配节点，确认使用权限和空闲状态，再在 Linux 登录节点运行 `hcu-cluster-run`。工具读取 hostfile，不申请节点。当前公共场景为共享 Conda、各节点本地 Conda、各节点独立 Docker 容器。

以下为已有容器场景的通用示例，进入共享工程目录后按部署替换路径、节点和镜像：

```bash
cd /shared/cluster-manager-das/hcu-envcheck
cat > hostfile <<'EOF'
node001
node002
node003
EOF

bash bin/hcu-cluster-run per-node-container container-status \
  -f hostfile --container hcu-test -i registry.example.com/hcu/train:validated

bash bin/hcu-cluster-run per-node-container platform,resource \
  -f hostfile --container hcu-test -i registry.example.com/hcu/train:validated
```

核心分阶段检查结果默认位于 `hcu-envcheck/verify_results/<scenario>_<operation>_stages_<timestamp>_<pid>/`。先看 `summary.md`、`failures.tsv`，再按组索引进入 `.state/` 查原始日志；检查采集执行、健康规则、参与覆盖和逐节点清理，不能只看采集进程退出码。

主动 RCCL、DeepEP、byte-flux 和 IB 带宽测试会占用计算资源和网络，应在已确认空闲的节点上按需执行。容器创建、重建和删除属于显式维护操作，测试不会隐式创建容器。当前公共入口不提供 Kubernetes/Pod 场景，不能继续使用历史 doctor/check-nodes 入口。

完整说明：

- [快速使用说明](../hcu-envcheck/README.md)
- [命令示例与结果目录](../hcu-envcheck/docs/USER_GUIDE.md)
- [总体设计与运行约定](../hcu-envcheck/docs/DESIGN.md)
- [工程结构与文件职责](../hcu-envcheck/docs/PROJECT_STRUCTURE.md)
- [能力与验证边界](../hcu-envcheck/docs/KNOWN_LIMITATIONS.md)

### 第二步：选择运行期容错方案

#### Slurm/MPI

```bash
cd hcu-training-reliability
python -m pip install -e .
hcu-cluster-inspect --help
```

`hcu-training-reliability` 在登录节点或控制节点运行，主要负责：

- 校验已有 Slurm 作业和训练规模。
- 从 hostfile 初始化运行、备用、正常和异常节点池。
- 监控训练日志、NHC 和 Slurm 状态。
- 发现故障后停止当前训练、隔离异常节点、选择备用节点并重新启动。
- 持久化节点池、运行状态、迭代信息和故障快照。

正式启动需要准备已有 Slurm Job ID、hostfile、sbatch 脚本、训练启动脚本和训练参数文件。不要直接把项目目录中的 `start.sh` 当作通用启动器，具体限制见 [hcu-training-reliability README](../hcu-training-reliability/README.md)。

#### Kubernetes

```bash
cd hygon-ft-k8s

bash start.sh install
bash start.sh status
```

`hygon-ft-k8s` 通过以下组件增强 PyTorchJob 和 Volcano Job：

- Admission Webhook 注入 `ft-launcher` 和容错环境。
- Operator 消费 `FaultEvent`，执行故障聚合、节点 taint、Pod 删除和状态回写。
- `nodehealth-agent` DaemonSet 周期执行节点健康检查。
- `ft-launcher` 包装训练命令、运行日志监控并上报故障。

恢复工具拥有的 unhealthy taint：

```bash
cd hygon-ft-k8s
bash start.sh recover node95 node39,node97
```

恢复会执行多轮 IB 状态、IB 带宽、NHC 和检查 Pod 验证。目标节点和参考节点必须空闲；默认只删除 `ft.hygon.io/node-unhealthy:NoSchedule`，不会删除其他 taint。

详细部署、权限、标签、镜像和 Volcano 配置见 [hygon-ft-k8s README](../hygon-ft-k8s/README.md)。

### 第三步：训练 hang 后执行堆栈诊断

安装：

```bash
cd stack-analyzer
python -m pip install .
```

不连接真实集群即可先运行内置演示：

```bash
stack-analyzer demo --scenario pp --method trie
stack-analyzer demo --scenario eval --method auto
```

真实诊断分为两步：

1. 通过 Kubernetes、Docker SSH、Ansible 或本地采集方式执行 `py-spy dump`，生成 `stacks.json`。
2. 使用 hostfile 和 PP/TP/DP 拓扑分析异常 rank，输出 `outlier_ranks` 和 `machines_to_evict`。

`machines_to_evict` 是结合并行拓扑给出的隔离建议，不代表其中每台机器都是根因，也不会自动执行驱逐。具体采集权限、拓扑参数和结果解释见 [stack-analyzer README](../stack-analyzer/README.md)。
