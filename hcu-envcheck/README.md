<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# hcu-envcheck

`hcu-envcheck` 是 HCU 集群在大模型训练前使用的基础环境检测与通信、算子测试工具。用户在登录节点执行唯一公共命令 `hcu-cluster-run`，对 hostfile 中已经分配的计算节点进行检查，并保存分组结果和原始证据。

工具面向共享 Conda、各节点独立 Conda、各节点独立 Docker 容器三种部署方式。节点申请和作业排队由 Slurm 等外部系统完成；本工具不负责申请计算资源。当前版本为 `0.4.2`，已知限制见[能力与验证边界](docs/KNOWN_LIMITATIONS.md)。

## 能力

| 需求 | 公共操作 | 范围 |
|---|---|---|
| 平台、软件和资源检查 | `platform`、`resource`、`platform,resource` | 逐节点采集、健康规则与节点静态配置一致性分析 |
| DeepEP 前置检查 | `checkdeepep` | 逐节点检查 ACS、XDP、RDMA 等，仅容器场景 |
| 容器管理 | `container-status/create/recreate/delete` | 查询、创建、重建、删除；命令需写完整的 `container-` 前缀 |
| RCCL 测试 | `rccl` | 单机或多机，默认十项 collective，按实际卡数匹配基准 |
| DeepEP 测试 | `deepep` | 单机 intranode 支持配置 2/4/8 卡；多机 lowl、norm 固定每节点八卡，完整组限 2/4 节点 |
| 算子测试 | `byte-flux` | 单机 TP4，64 项矩阵 |
| 保留扩展 | `ib-write-bw` | IB 带宽 |

## 快速开始

在 Linux 登录节点进入共享工程目录，准备 hostfile 和已经运行的测试容器。以下名称和路径为公开示例，使用前按集群修改。完整依赖及容器准备方法见 [DESIGN](docs/DESIGN.md#附录环境准备)。

hostfile 每行只写一个计算节点名或 IPv4 地址。RCCL、DeepEP 统一使用 `--group-size G` 指定每组节点数（默认 1）、`--gpus-per-node C` 指定每节点卡数（默认 8）；两个参数独立。两项测试拒绝旧公共 `--slots`、`--nproc-per-node` 以及 hostfile 中的 `slots=N`。内部 MPI 槽位由工具生成，详见 [hostfile 约定](docs/DESIGN.md#准备节点文件)与[设备选择](docs/DESIGN.md#测试规模与设备选择)。

```bash
cd /shared/cluster-manager-das/hcu-envcheck
cat > hostfile <<'EOF'
node001
node002
node003
EOF

# 查看公共入口与版本，不执行节点任务
bash bin/hcu-cluster-run --help
bash bin/hcu-cluster-run --version

# 镜像参数仅核对已有容器，不会自动创建或拉取
bash bin/hcu-cluster-run per-node-container container-status \
  -f hostfile --container hcu-test -i registry.example.com/hcu/train:validated

# 检查各节点的平台与资源，结果以报告中的规则结论为准
bash bin/hcu-cluster-run per-node-container platform,resource \
  -f hostfile --container hcu-test -i registry.example.com/hcu/train:validated
```

主动测试会占用计算卡、显存和网络，应在已分配且空闲的节点执行。默认保留空闲检查；不要把 `--dry-run` 的计划输出当作实测通过。

分阶段检测和测试的一次公共命令生成独立运行目录，固定本次分组与子项计划。各子项复用适用的组节点文件、设备选择和脚本；原始输出保存在根部 `group-NNN.log`，持久事实保存在 `.state/prepared/group-NNN/facts.json`。临时工作区在证据提交确认后回收，历史运行继续兼容读取。结果位置见 [USER_GUIDE](docs/USER_GUIDE.md#结果目录)，内部写入与回收约定见 [DESIGN](docs/DESIGN.md#执行事实与写入责任)；目录复用本身不表示测试已经通过。

## 文档

| 文档 | 用途 |
|---|---|
| [命令示例与结果目录](docs/USER_GUIDE.md) | 核心操作命令、单机/多机示例及结果位置 |
| [总体设计与运行约定](docs/DESIGN.md) | 总体架构、部署、调度、参数、判读及排障 |
| [工程结构与文件职责](docs/PROJECT_STRUCTURE.md) | 子目录、文件用途、模块依赖与修改定位 |
| [能力与验证边界](docs/KNOWN_LIMITATIONS.md) | 当前缺陷、使用限制和不能作为通过依据的证据 |
| [控制层文档导航](cluster_run/GUIDE.md) | 指向统一命令与设计章节 |
| [文档索引](docs/INDEX.md) | 按使用者、开发者和维护者导航 |
| [工程约定](AGENTS.md) | 修改边界与需要维持的接口契约 |

## 参与贡献与许可

开发者克隆代码和运行单元测试的方法见[单元测试说明](../CONTRIBUTING.md#hcu-envcheck-单元测试)。

贡献、问题反馈和安全报告遵循仓库的 [CONTRIBUTING](../CONTRIBUTING.md) 与 [SECURITY](../SECURITY.md)。文档示例使用通用节点、镜像和路径；真实站点配置和验收日志不应提交到公开仓库。

本项目原创内容遵循 [Apache License 2.0](../LICENSE)，参见 [NOTICE](../NOTICE) 和 [第三方声明](../THIRD_PARTY_NOTICES.md)。`third-party/` 组件及独立安装的依赖分别受其自身许可约束；离线工具包不等同于已经包含或验证这些组件。

### HCU 空闲检测与释放

在宿主机检查 hostfile 全部节点：`bash bin/hcu-cluster-run per-node-container idle-check -f hostfile`。添加 `--release` 自动停止确认占用的 Docker 容器并复查；停止会结束容器全部任务。
