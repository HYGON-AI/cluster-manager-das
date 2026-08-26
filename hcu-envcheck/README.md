<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# hcu-envcheck

`hcu-envcheck` 用于在训练启动前检查 HCU 集群环境。它采集 HCU、驱动、DTK、CPU、内存、NIC、RDMA 和软件依赖，并生成节点明细与集群汇总。

## 选择入口

| 场景 | 入口 | 创建 Pod | 主动测试流量 |
|---|---|---:|---:|
| 裸金属或 Slurm 多节点 | `baremetal-cluster` | 否 | 默认否；显式启用 `ib_write_bw` 后会产生 |
| 已运行的 Kubernetes Pod | `k8s-pod` | 否 | 否 |
| 指定镜像检查一批 Kubernetes 节点 | `k8s-cluster` | 是 | 否 |
| Slurm allocation 内主动 RDMA/RCCL 验收 | `active-rdma-slurm` | 否 | 是 |
| Native IB 链路和交换机计数器 | `ib-fabric-slurm` | 否 | 有界查询 |

普通训练前检查优先使用前三个入口。主动通信验收只能在节点空闲并满足独占条件时执行。

## 最短上手

检查控制端、填写节点并运行：

```bash
./bin/hcu-envcheck-doctor
vi examples/baremetal-nodes.txt
./examples/check-nodes.sh
```

`check-nodes.sh` 包含 IB 带宽测试，开始前会要求输入 `yes` 确认节点空闲。无人值守环境只有在外部流程已完成同等确认后，才可使用：

```bash
CONFIRM_NODES_IDLE=yes ./examples/check-nodes.sh
```

历史结果分别保存在：

```text
out/nodes_check_YYYYMMDD_HHMMSS_ffffff/
```

完整执行顺序、裸金属/Kubernetes 用法、主动验收、报告阅读顺序和故障排查见：

- [完整使用手册](docs/USER_GUIDE.md)

## 常用配置

示例脚本可通过环境变量覆盖默认值：

```bash
EXPECTED_DEVICES=8 \
EXPECTED_RDMA_DEVICES=4 \
IB_MINIMUM_AVERAGE_GBPS=100 \
SSH_USER=example-user \
OUTPUT_ROOT="$PWD/out2" \
./examples/check-nodes.sh
```

| 变量 | 默认值 | 说明 |
|---|---:|---|
| `EXPECTED_DEVICES` | `8` | 每节点预期 HCU 数量 |
| `EXPECTED_RDMA_DEVICES` | `4` | 每节点最低 RDMA HCA 数量 |
| `IB_MINIMUM_AVERAGE_GBPS` | `100` | 单条路径最低平均带宽，Gbit/s |
| `CONCURRENCY` | `16` | 静态采集最大 SSH 并发数 |
| `OUTPUT_ROOT` | `项目目录/out` | 结果根目录 |
| `REMOTE_PYTHON` | `python3` | 目标节点探针解释器 |
| `SOFTWARE_MODE` | `host-python` | `host-python`、`conda` 或 `docker` |
| `PYTHON_PACKAGES` | 空 | 显式要求检查的 Python 包 |

只有设置 `PYTHON_PACKAGES` 时才会导入相应 Python 包；默认不会导入 Torch。

## 结果含义

| 退出码 | 结果 | 含义 |
|---:|---|---|
| `0` | `READY` | 满足本次显式启用的检查要求 |
| `1` | `BLOCKED` | 已确认存在阻断项 |
| `2` | `INCOMPLETE` | 节点不可达、命令失败、超时或证据不足 |
| `3` | `TOOL_ERROR` | 参数、权限或控制端错误 |

多节点结果主要查看：

```text
cluster-summary.md
cluster-result.json
evidence/
```

`NOT_CHECKED`、`NOT_VERIFIED` 和 `UNKNOWN` 都不等于通过。

## 安全边界

普通静态检查不会重置设备、结束进程、修改驱动/网卡/ACS、恢复 Kubernetes taint，也不会自动安装 NHC 或 Python 包。

下列操作会产生额外影响，执行前必须阅读完整手册：

- `k8s-cluster` 会创建并清理短生命周期探针 Pod；
- `--enable-ib-write-bw` 会产生真实 IB 流量；
- `active-rdma-slurm` 会执行主动 RDMA/RCCL 验收；
- `ib-fabric-slurm` 依赖 Slurm allocation 和 IB Fabric 权限。

## 安装包与帮助

离线包校验：

```bash
sha256sum -c hcu-envcheck-0.4.2.tar.gz.sha256
tar -xzf hcu-envcheck-0.4.2.tar.gz
cd hcu-envcheck-0.4.2
./bin/hcu-envcheck-verify
./bin/hcu-envcheck-doctor
```

命令帮助：

```bash
./hcu-envcheck.sh --help
./hcu-envcheck.sh baremetal-cluster --help
./hcu-envcheck.sh k8s-cluster --help
```

测试：

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -v
```

## License

This subproject is licensed under the Apache License, Version 2.0. See the
repository-level [LICENSE](../LICENSE), [NOTICE](../NOTICE), and
[THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md).
