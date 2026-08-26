<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# hcu-envcheck 0.4.2 使用手册

本手册按照实际使用顺序编写：先选择检查场景，再执行最短命令，最后根据结果进入参数说明或故障排查。

第一次使用建议依次阅读：

1. [选择正确的检查入口](#1-选择正确的检查入口)
2. [运行前准备](#2-运行前准备)
3. [裸金属节点快速检查](#3-裸金属节点快速检查)
4. [理解输出和结论](#7-输出目录报告与退出码)

只检查 Kubernetes 时，可以从“[Kubernetes 检查](#5-kubernetes-检查)”开始。主动 RDMA/RCCL 和 Fabric 验收属于高级功能，参见“[主动通信验收](#9-主动通信验收高级功能)”。

## 1. 选择正确的检查入口

### 1.1 五个入口分别解决什么问题

| 入口 | 适用场景 | 主要执行位置 | 是否创建 Pod | 是否产生主动流量 |
|---|---|---|---:|---:|
| `baremetal-cluster` | 裸金属或 Slurm 多节点启动前检查 | 目标计算节点 | 否 | 默认否；启用 `ib_write_bw` 后会 |
| `k8s-pod` | 检查一个已存在的 Pod/容器 | 指定容器 | 否 | 否 |
| `k8s-cluster` | 用同一训练镜像检查一批 K8s 节点 | 临时探针 Pod，或明确复用的已有 Pod | 是 | 否 |
| `active-rdma-slurm` | 两节点 Verbs、rccl-tests 或 PyTorch/RCCL 主动验收 | 空闲且满足独占条件的 Slurm allocation | 否 | 是 |
| `ib-fabric-slurm` | Native IB 一跳邻接和叶端口计数器检查 | 空闲且满足独占条件的 Slurm allocation | 否 | 有界只读查询 |

选择方法：

- 直接 SSH 到服务器检查：使用 `baremetal-cluster`。
- 已经有一个训练 Pod，希望确认其实际运行环境：使用 `k8s-pod`。
- 希望用某个训练镜像检查多台 K8s 节点：使用 `k8s-cluster`。
- 希望证明 RDMA/RCCL 数据面能够真正传输：使用 `active-rdma-slurm`。
- 希望检查 Native IB 一跳链路和交换机叶端口计数器：使用 `ib-fabric-slurm`。

### 1.2 静态检查和主动检查必须分开理解

前三个入口是启动前环境检查，主要回答：

- 节点是否可达；
- HCU 数量、驱动、DTK 和设备状态是否符合预期；
- 节点是否空闲；
- RDMA 端口、IB/RoCE 配置和 Verbs userspace 是否具备基本条件；
- 显式要求的软件包和通信组件是否可用。

它们不能证明已经完成 RCCL collective、全网数据面测试或长时间稳定性测试。

`active-rdma-slurm` 和 `ib-fabric-slurm` 是单独的高级验收入口。它们要求显式开关、空闲确认和 Slurm 安全边界，不会因为静态检查通过而自动执行。

## 2. 运行前准备

### 2.1 控制端基础要求

所有模式都需要：

- Linux 和 POSIX shell；
- Python 3.10 或更高版本；
- 对结果目录有写权限。

不同模式还需要：

| 场景 | 控制端命令 | 访问要求 |
|---|---|---|
| 裸金属节点 | `ssh` 或 `clush` | 能免交互访问目标节点 |
| Slurm 节点 | `scontrol`、`sinfo`，Job ID 场景还需要 `squeue` | 当前用户能访问已经分配的计算节点 |
| K8s | `kubectl` | 当前 context 和 RBAC 权限正确 |
| Slurm 主动验收 | `scontrol`、`squeue`、`srun` | 当前用户拥有指定 allocation |

目标节点或探针镜像也需要 Python 3.10+。要完成 HCU 检查，通常还需要 `rocminfo` 和 `hy-smi`/`Hy-smi`；RDMA 检查需要相应的 `ibstat`、`ibv_*` 或 perftest 工具。

### 2.2 离线发行包校验

拿到发行包后，先校验外层压缩包：

```bash
sha256sum -c hcu-envcheck-0.4.2.tar.gz.sha256
```

校验成功后解压：

```bash
tar -xzf hcu-envcheck-0.4.2.tar.gz
cd hcu-envcheck-0.4.2
```

再检查包内文件和控制端条件：

```bash
./bin/hcu-envcheck-verify
./bin/hcu-envcheck-doctor
./hcu-envcheck.sh --version
./hcu-envcheck.sh --help
```

说明：

- `verify` 根据发行清单检查包内文件是否缺失或损坏。
- `doctor` 只检查本地 Python、包结构和常用命令是否存在，不连接节点。
- 某个当前场景不需要的命令显示 `ABSENT`，不一定代表工具无法使用。
- 发行包不执行 `pip install`、不联网，也不要求管理员权限。

这里的“离线”只表示分发和安装过程离线；真正执行节点检查仍然需要 SSH、Slurm 或 Kubernetes API 访问。

### 2.3 指定控制端 Python

启动器会自动寻找兼容的 Python 3.10～3.14。需要固定控制端解释器时：

```bash
export HCU_ENVCHECK_PYTHON=/usr/local/python3.12/bin/python3
./hcu-envcheck.sh --help
```

`HCU_ENVCHECK_PYTHON` 只控制登录节点上的工具解释器。目标计算节点使用的 Python 由 `--remote-python` 指定。

### 2.4 可选安装

不安装也可以直接使用 `./hcu-envcheck.sh`。需要安装到个人目录时：

```bash
./install.sh
export PATH="$HOME/.local/bin:$PATH"

hcu-envcheck --version
hcu-envcheck-doctor
hcu-envcheck-verify
```

自定义安装前缀：

```bash
./install.sh --prefix /opt/hcu-tools
export PATH="/opt/hcu-tools/bin:$PATH"
```

相同版本已经存在时安装脚本会拒绝覆盖。确认确实要替换时才使用 `./install.sh --force`。

## 3. 裸金属节点快速检查

### 3.1 推荐方式：修改节点列表后直接执行

项目提供：

```text
examples/check-nodes.sh
examples/baremetal-nodes.txt
```

先编辑节点列表：

```bash
vi examples/baremetal-nodes.txt
```

推荐每行一个节点：

```text
node37
node98
```

也支持简单范围：

```text
node[37-40]
```

确认控制端可以免交互登录：

```bash
ssh node37 hostname
ssh node98 hostname
```

然后执行：

```bash
./examples/check-nodes.sh
```

脚本会运行 `ib_write_bw`。开始前必须确认节点空闲，并在提示后输入：

```text
yes
```

无人值守执行只有在外部流程已经证明全部目标节点空闲后，才可以使用：

```bash
CONFIRM_NODES_IDLE=yes ./examples/check-nodes.sh
```

### 3.2 示例脚本的执行流程

脚本只执行一轮，不需要参考节点：

1. 并发采集节点、CPU、内存、OS、内核、驱动、DTK、HCU、NIC 和 RDMA 静态信息。
2. 在每个节点执行一次 `ibstat`。
3. 动态发现 `mlx*` 或 `shca*` HCA，检查端口是否 `Active`、`LinkUp`。
4. 按节点方向和 rail 执行一次 `ib_write_bw` 测试计划。
5. 在每个节点 PATH 中直接执行一次 `run_nhc`。
6. 汇总 Markdown、JSON 和原始 evidence。

该流程：

- 不创建 Kubernetes Pod；
- 不读取、添加或删除 Kubernetes taint；
- 不执行 taint 恢复；
- 不自动安装 `run_nhc`；
- 不检查 ACS；
- 不进行多轮重试或多轮一致性判定。

### 3.3 示例脚本常用配置

默认值：

| 环境变量 | 默认值 | 含义 |
|---|---:|---|
| `NODES_FILE` | `examples/baremetal-nodes.txt` | 节点列表文件 |
| `EXPECTED_DEVICES` | `8` | 每节点预期 HCU 数量 |
| `EXPECTED_RDMA_DEVICES` | `4` | 每节点至少需要的 RDMA HCA 数量 |
| `IB_MINIMUM_AVERAGE_GBPS` | `100` | 每条带宽测试路径的最低平均 Gbit/s |
| `CONCURRENCY` | `16` | 静态检查的最大 SSH 并发数 |
| `OUTPUT_ROOT` | `项目目录/out` | 可重复使用的结果根目录 |
| `REMOTE_PYTHON` | `python3` | 目标节点探针 Python |
| `SOFTWARE_MODE` | `host-python` | `host-python`、`conda` 或 `docker` |
| `PYTHON_PACKAGES` | 空 | 需要检查的 Python 包 |

覆盖示例：

```bash
EXPECTED_DEVICES=8 \
EXPECTED_RDMA_DEVICES=4 \
IB_MINIMUM_AVERAGE_GBPS=100 \
SSH_USER=example-user \
OUTPUT_ROOT="$PWD/out2" \
./examples/check-nodes.sh
```

默认 `PYTHON_PACKAGES` 为空，因此不会导入 Torch，也不会检查 Python 依赖。需要检查实际训练环境时才显式设置：

```bash
PYTHON_PACKAGES="torch numpy" ./examples/check-nodes.sh
```

只要 `REMOTE_PYTHON` 指向的解释器能够直接导入这些包，就不需要额外指定 Python 依赖目录。

### 3.4 结果在哪里

`OUTPUT_ROOT` 可以已经存在。每次运行都会创建：

```text
<OUTPUT_ROOT>/nodes_check_YYYYMMDD_HHMMSS_ffffff/
```

命令结束时会打印：

```text
RUN_DIR
JSON
SUMMARY
```

优先打开 `SUMMARY` 指向的 `cluster-summary.md`。

## 4. 裸金属与 Slurm 完整用法

### 4.1 节点来源必须四选一

`baremetal-cluster` 只允许使用一种节点来源。

#### 方式 A：重复指定节点

```bash
./hcu-envcheck.sh baremetal-cluster \
  --node node37 \
  --node node98 \
  --transport ssh \
  --software-mode host-python \
  --expected-devices 8 \
  --output-dir ./out
```

#### 方式 B：节点文件

创建 `hosts.txt`：

```text
# 每行一个节点
compute001
compute002

# 支持逗号列表和简单范围
compute[003-006,009]
```

执行：

```bash
./hcu-envcheck.sh baremetal-cluster \
  --nodes-file hosts.txt \
  --transport ssh \
  --software-mode host-python \
  --expected-devices 8 \
  --output-dir ./out
```

节点文件支持 UTF-8 BOM、空行、`#` 注释、简单范围和 OpenMPI 风格的 `key=value` 附加字段。重复节点按首次出现顺序去重；不安全的用户名、主机名或 Shell 字符会在连接前被拒绝。

#### 方式 C：Slurm Job ID

```bash
JOB_ID="${SLURM_JOB_ID:?set an allocated Slurm job id}"

./hcu-envcheck.sh baremetal-cluster \
  --slurm-job-id "$JOB_ID" \
  --transport auto \
  --software-mode host-python \
  --remote-python python3 \
  --expected-devices 8 \
  --require-rdma \
  --minimum-rdma-devices 4 \
  --expected-rdma-protocol ib \
  --output-dir ./out
```

Job 必须已经获得节点分配。Pending 且没有 NodeList 的 Job 无法检查。

#### 方式 D：Slurm nodelist

```bash
./hcu-envcheck.sh baremetal-cluster \
  --slurm-nodelist 'compute[001-015]' \
  --transport auto \
  --concurrency 15 \
  --software-mode host-python \
  --expected-devices 8 \
  --target-scale-devices 10000 \
  --output-dir ./out
```

使用 Slurm Job ID 或 nodelist 时，报告会额外包含 `sinfo` 节点状态和 drain/down 原因。使用 `--node` 或 `--nodes-file` 时不采集 Slurm 状态。

### 4.2 选择传输方式

```text
--transport auto    本地存在 clush 时使用 clush，否则使用 SSH
--transport clush   强制使用 clush
--transport ssh     强制使用并发 SSH
```

注意：

- `auto` 只在找不到 `clush` 命令时回退到 SSH。
- 如果 `clush` 命令存在但站点配置不可用，不会在执行失败后自动切换 SSH。
- SSH 用户、端口、密钥和 config 参数只能与 `--transport ssh` 一起使用。
- `ib_write_bw` 节点健康检查必须显式使用 `--transport ssh`。
- SSH 使用非交互模式，不支持运行时密码提示。

SSH 示例：

```bash
./hcu-envcheck.sh baremetal-cluster \
  --nodes-file hosts.txt \
  --transport ssh \
  --ssh-user example-user \
  --ssh-port 22 \
  --identity-file "$HOME/.ssh/id_ed25519" \
  --ssh-config-file "$HOME/.ssh/config" \
  --known-hosts-file "$HOME/.ssh/known_hosts" \
  --strict-host-key-checking yes \
  --software-mode host-python \
  --expected-devices 8 \
  --output-dir ./out
```

`--strict-host-key-checking accept-new` 只适合接收经过授权的新主机。出现主机密钥变化时必须通过可信渠道核对，不能通过关闭校验绕过。

`--concurrency` 是静态探针的 clush fanout 或 SSH 并发上限，默认 32、最大 128。它不是 `ib_write_bw` 并发数；带宽测试由 `--ib-concurrency` 单独控制。

### 4.3 明确选择训练软件环境

`baremetal-cluster` 必须在下面三种模式中选择一个。

| 模式 | 适合场景 | 必需参数 |
|---|---|---|
| `host-python` | 训练直接使用节点上的 Python | `--remote-python` 可选，默认 `python3` |
| `conda` | 训练使用明确的 Conda 环境 | `--conda-prefix`、`--conda-storage` |
| `docker` | 训练使用明确镜像 | `--docker-image` |

#### host-python

```bash
--software-mode host-python \
--remote-python /opt/train/bin/python3
```

#### Conda

```bash
--software-mode conda \
--conda-prefix /shared/conda/envs/train \
--conda-storage shared
```

`conda` 模式不执行 `conda activate`，而是直接调用 `<prefix>/bin/python`。`--conda-storage` 必须是：

- `shared`：同一路径来自共享存储；
- `node-local`：每个节点有独立副本。

声明值与逐节点挂载证据不一致时会失败。

#### Docker

```bash
--software-mode docker \
--docker-image registry.example.com/train/dtk:tag \
--container-python python3
```

每个节点只创建一个明确镜像的短生命周期 `docker run --rm` 探针容器。工具不会枚举或进入任意业务容器。容器用于检查训练软件栈，宿主机 CPU、内存、HCU、驱动和 RDMA 仍然在宿主侧独立检查。

Docker 探针根文件系统只读，并只提供有界 tmpfs。容器清理失败会保留明确状态，不能作为完整放行。

### 4.4 Python 包检查是显式开启的

裸金属入口默认不检查任何 Python 包：

```text
没有 --require-python-package
→ 不导入 Torch
→ 不检查 Python 依赖
→ 不会产生 TORCH_IMPORT_FAILED
```

检查 Torch 和 NumPy：

```bash
--require-python-package torch \
--require-python-package numpy
```

只有指定 `torch` 时，才检查 Torch import、HCU 运行时设备数和分布式后端。

如果节点已经安装依赖，只要所选解释器能够直接导入即可，不需要设置 `PYTHON_DEPS_DIR`。只有依赖不在解释器默认 `sys.path` 时，才应先修正实际训练环境或明确使用正确的 Conda/Docker 环境。

### 4.5 能力 Profile：只要求训练真正依赖的能力

| 参数 | 启用后的判定 |
|---|---|
| `--require-compiler` | `hipcc` 缺失成为阻断项 |
| `--require-rdma` | 要求存在 RDMA HCA 和活跃端口 |
| `--minimum-rdma-devices N` | 每节点至少 N 个 RDMA 设备和 Active/LinkUp 端口 |
| `--expected-rdma-protocol auto\|ib\|roce` | `auto` 只识别；显式协议会校验证据充分的模式不匹配 |
| `--rdma-counter-interval SEC` | 对端口计数器做双采样；`0` 关闭，`1`～`60` 开启 |
| `--rdma-policy-file FILE` | 按显式 RoCE JSON 策略验收主机配置链 |
| `--require-rccl` | 要求所选软件环境具备 RCCL 或 Torch NCCL/RCCL 后端 |
| `--require-ucx` | 要求所选软件环境具备 UCX |
| `--strict-hardware-consistency` | 关键硬件或驱动差异升级为阻断 |

不要为了“检查得更多”盲目开启所有 Profile。未被训练使用的组件缺失，不应成为训练启动阻断项。

### 4.6 单轮 IB 状态、带宽和 NHC 模块

启用全部三项：

```bash
--enable-node-health-checks
```

等价于：

```text
--enable-ib-state
--enable-ib-write-bw
--enable-nhc
```

也可以单独启用其中一项。只启用 `--enable-ib-write-bw` 时，工具仍会自动执行 IB 状态检查作为前置条件。

完整示例：

```bash
./hcu-envcheck.sh baremetal-cluster \
  --node node98 \
  --node node37 \
  --transport ssh \
  --software-mode host-python \
  --remote-python python3 \
  --expected-devices 8 \
  --target-scale-devices 16 \
  --require-rdma \
  --minimum-rdma-devices 4 \
  --expected-rdma-protocol ib \
  --enable-node-health-checks \
  --confirm-nodes-idle \
  --nhc-timeout 600 \
  --ib-tool ib_write_bw \
  --ib-protocol ib \
  --ib-port 1 \
  --ib-control-port 18515 \
  --ib-message-bytes 1048576 \
  --ib-iters 1000 \
  --ib-minimum-average-gbps 100 \
  --ib-concurrency 1 \
  --ib-max-tests 64 \
  --output-dir ./out
```

#### IB 状态

每个节点只执行一次 `ibstat`：

- 动态识别名称匹配 `mlx*` 或 `shca*` 的 HCA；
- 不写死设备名称和数量；
- 要求端口同时为 `Active` 和 `LinkUp`；
- 命令缺失、执行失败、超时或证据不完整都会明确区分。

SSH 首次连接产生的：

```text
Warning: Permanently added 'node-name' ...
```

是 SSH 提示，不应单独被解释为 `ibstat` 执行失败。真正失败应同时检查远端返回码和 `ibstat` stdout/stderr。

#### IB 带宽

带宽测试特点：

- 使用本次选中的节点集合，不存在“参考节点”参数；
- 每个不同节点的有向组合、每条对应 rail 测试一次；
- 两端 HCA 名称可以不同，但数量必须一致；
- HCA 按自然顺序匹配；
- 单节点无法进行节点间测试，结果为 `NOT_VERIFIED`；
- 低于阈值时，报告列出方向、源/目标 HCA、rail、实测带宽和阈值；
- `--ib-max-tests` 限制测试总数，防止大节点集合形成无界全组合流量；
- `--ib-concurrency 1` 表示串行执行带宽测试。

由于测试会产生流量，必须同时满足：

```text
--transport ssh
--confirm-nodes-idle
```

`--confirm-nodes-idle` 只表示操作者已经确认，不等于调度器提供了独占证明。

#### NHC

默认直接执行目标节点 PATH 中的：

```text
run_nhc
```

工具不经过登录 shell，也不自动附加 `run_nhc` 可能不支持的参数。可以用 `--nhc-command` 指定其他入口。

判定方式：

- 输出 `[CHECK RESULT]: PASSED`、`[CHECK RESULT]: PASS` 或独立 `PASSED`，且返回码为 0：`PASS`；
- 输出其他明确 `[CHECK RESULT]`：`FAIL/BLOCKED`；
- 命令不存在、超时、执行异常、非零返回但没有有效故障结果、结果标记缺失：`NOT_VERIFIED/INCOMPLETE`。

默认假定每个目标节点已经安装 `run_nhc`，并且该命令可从节点 PATH 中找到。无法获得有效 NHC 结果时，工具只报告问题，不自动安装或修复。必要时可以通过 `--nhc-install-source` 提供站点自定义的安装提示；该参数默认无值。

### 4.7 裸金属参数速查

| 参数 | 默认值/要求 | 说明 |
|---|---|---|
| `--node` / `--nodes-file` / `--slurm-job-id` / `--slurm-nodelist` | 四选一 | 节点来源 |
| `--transport` | `auto` | `auto`、`clush` 或 `ssh` |
| `--concurrency` | `32` | 静态远端检查并发，范围 1～128 |
| `--connect-timeout` | `10` | SSH 建连超时 |
| `--command-timeout` | `240` | 单节点静态探针超时 |
| `--remote-python` | `python3` | 目标节点 Python 3.10+ |
| `--expected-devices` | 可选 | 每节点预期 HCU 数量 |
| `--software-mode` | 必需 | `host-python`、`conda`、`docker` |
| `--require-python-package` | 可重复 | 只检查显式列出的包 |
| `--enable-node-health-checks` | 关闭 | 启用一次 IB 状态、带宽和 NHC |
| `--output-dir` | 必需 | 可重复使用的结果根目录 |

查看全部参数：

```bash
./hcu-envcheck.sh baremetal-cluster --help
```

## 5. Kubernetes 检查

### 5.1 先检查 context 和权限

```bash
NAMESPACE=train-preflight

kubectl config current-context
kubectl auth can-i get nodes
kubectl auth can-i get pods -n "$NAMESPACE"
kubectl auth can-i create pods -n "$NAMESPACE"
kubectl auth can-i delete pods -n "$NAMESPACE"
kubectl auth can-i create pods/exec -n "$NAMESPACE"
```

权限要求：

- `k8s-pod` 至少需要读取目标 Pod/Node 并 exec 到指定容器；
- `k8s-cluster` 还需要创建、等待、读取和删除临时 Pod；
- 使用 wheel bootstrap 时还需要复制文件到临时 Pod。

不要为了运行检查直接授予 `cluster-admin`，应按最小权限补齐 RBAC。

### 5.2 检查一个已有 Pod

必须明确指定 namespace、Pod 和容器，工具不会枚举或猜测：

```bash
RUN_DIR="$PWD/out/k8s-pod-$(date +%Y%m%d-%H%M%S)-$$"

./hcu-envcheck.sh k8s-pod \
  --namespace train-preflight \
  --pod training-pod \
  --container trainer \
  --device-resource-name hygon.com/hcu \
  --expected-devices 8 \
  --max-vram-used-percent 5 \
  --max-hcu-util-percent 5 \
  --samples 3 \
  --busy-sample-quorum 2 \
  --sample-interval 1 \
  --require-rdma \
  --minimum-rdma-devices 4 \
  --expected-rdma-protocol ib \
  --require-rccl \
  --require-ucx \
  --output "$RUN_DIR/preflight-result.json" \
  --evidence-dir "$RUN_DIR/evidence"
```

已有 Pod 只读复用，不会被删除。工具会核对实际 Pod UID、所在 Node、镜像、容器状态、HCU request/limit 和容器内环境。

指定非默认集群：

```text
--kubeconfig /path/to/kubeconfig --context my-context
```

### 5.3 检查一批 K8s 节点

准备 `k8s-nodes.txt`：

```text
hcu-node-001
hcu-node-002
hcu-node-003
```

节点文件支持空行、`#` 注释、逗号列表和简单范围。

执行：

```bash
RUN_DIR="$PWD/out/k8s-cluster-$(date +%Y%m%d-%H%M%S)-$$"

./hcu-envcheck.sh k8s-cluster \
  --nodes-file k8s-nodes.txt \
  --namespace train-preflight \
  --image registry.example.com/train/dtk-image:tag \
  --image-pull-policy IfNotPresent \
  --device-resource-name hygon.com/hcu \
  --expected-devices 8 \
  --target-scale-devices 10000 \
  --samples 3 \
  --busy-sample-quorum 2 \
  --sample-interval 1 \
  --require-rdma \
  --minimum-rdma-devices 4 \
  --expected-rdma-protocol ib \
  --require-rccl \
  --require-ucx \
  --strict-stack-consistency \
  --probe-memory-request 1Gi \
  --probe-memory-limit 8Gi \
  --pod-ready-timeout 180 \
  --concurrency 16 \
  --api-qps 20 \
  --api-burst 40 \
  --output-dir "$RUN_DIR"
```

临时 Pod 的主要约束：

- 明确绑定目标 Node；
- 申请完整的预期 HCU request/limit；
- 使用特权容器和 host network；
- `restartPolicy: Never`；
- 不自动挂载 ServiceAccount Token；
- 使用唯一 run-id 标签；
- 结束时核对 run-id 后再删除。

创建特权探针 Pod 必须获得管理员授权，并可能受到 Pod Security、准入策略、ResourceQuota、污点或资源占用限制。

### 5.4 复用指定节点上的已有 Pod

格式：

```text
NODE=NAMESPACE/POD/CONTAINER
```

示例：

```bash
./hcu-envcheck.sh k8s-cluster \
  --nodes-file k8s-nodes.txt \
  --namespace train-preflight \
  --image registry.example.com/train/dtk-image:tag \
  --reuse-pod hcu-node-002=train-preflight/training-pod/trainer \
  --expected-devices 8 \
  --output-dir "$PWD/out/k8s-reuse-$(date +%Y%m%d-%H%M%S)-$$"
```

指定节点使用已有容器，其余节点仍创建临时 Pod。复用 Pod 不会被删除，工具还会核对它是否真的运行在指定 Node 上。

### 5.5 注入受限路径变量

训练镜像中的命令或库不在默认路径时：

```bash
--probe-env 'PATH=/opt/hyhal/bin:/opt/dtk/bin:/usr/local/bin:/usr/bin' \
--probe-env 'LD_LIBRARY_PATH=/opt/dtk/lib:/opt/hyhal/lib:/opt/ucx/lib' \
--probe-env 'HIP_PATH=/opt/dtk/hip'
```

只允许路径和设备可见性相关变量。不要把 Token、密码或 Secret 放入 `--probe-env`。

### 5.6 在临时 Pod 中验证已知 wheel

先获得并核对 wheel SHA256：

```bash
sha256sum packages/hcusmi-1.0.0+a788f784-py3-none-any.whl
```

然后：

```bash
./hcu-envcheck.sh k8s-cluster \
  --node hcu-node-001 \
  --namespace train-preflight \
  --image registry.example.com/train/dtk-image:tag \
  --expected-devices 8 \
  --bootstrap-wheel packages/hcusmi-1.0.0+a788f784-py3-none-any.whl \
  --bootstrap-wheel-sha256 "$WHEEL_SHA256" \
  --output-dir "$PWD/out/k8s-wheel-$(date +%Y%m%d-%H%M%S)-$$"
```

wheel 只安装到工具创建的短生命周期 Pod，不修改镜像、宿主机或复用的业务 Pod。

### 5.7 临时 Pod 清理状态

| 状态 | 含义 |
|---|---|
| `DELETED` | 已删除 |
| `ALREADY_GONE` | 已不存在 |
| `CLEANUP_REQUIRED` | 删除失败，需要人工核查 |
| `CLEANUP_REFUSED` | run-id 不一致，工具拒绝删除 |

清理未完成会使该节点变为 `INCOMPLETE`。人工处理前必须核对 namespace、Pod 名和 run-id，禁止按名称模糊删除。

查看 K8s 全部参数：

```bash
./hcu-envcheck.sh k8s-pod --help
./hcu-envcheck.sh k8s-cluster --help
```

## 6. RDMA、IB 和 RoCE 结果如何理解

### 6.1 工具不按网卡型号猜协议

协议根据每个端口当前配置判断：

- `link_layer=InfiniBand`：当前为 Native IB。
- `link_layer=Ethernet`，并且存在非零 RoCE v1/v2 GID 和对应 netdev：当前为 RoCE。
- GID、type 或 ndev 证据不可读：`UNKNOWN`/`ETHERNET_RDMA_EVIDENCE_INCOMPLETE`。
- Ethernet RDMA 端口没有有效映射 GID：`ETHERNET_RDMA_UNCONFIRMED`。
- 同一节点或目标集群出现 Native IB/RoCE 混用：协议配置冲突。

Native IB 环境中，GID 类型显示 `IB/RoCE v1` 不代表当前正在使用 RoCE，仍应以 `link_layer` 和完整端点证据为准。

### 6.2 当前端口模式、硬件能力和实际训练路径是三个结论

报告分别描述：

1. 当前端口配置；
2. 通用接口能否确认硬件支持的其他模式；
3. 是否已经证明训练实际使用了 RDMA。

通用 Linux 接口无法查询厂商固件双模能力时，硬件支持显示 `UNKNOWN_NO_GENERIC_INTERFACE`。静态检查没有执行训练 collective 时，实际训练数据路径保持 `NOT_VERIFIED_BY_PREFLIGHT`，不会推断为 RDMA。

### 6.3 Native IB 端点

检查内容包括：

- `Active`、`LinkUp`；
- LID 和 SM LID；
- 非零 GID；
- P_Key；
- 速率；
- active/max MTU；
- Subnet。

跨节点按 `(Subnet, P_Key, active/max MTU, rate)` 组合比较，不比较本来就应不同的 LID/GID 值。

### 6.4 RoCE 配置链

只有当前端口为 Ethernet 且存在有效 RoCE GID/netdev 时才检查 RoCE。GID 映射到 bond 或 VLAN 时，会继续追踪到底层物理接口采集：

- MTU 和地址；
- PFC；
- ETS；
- APP；
- buffer；
- DCBX；
- pause/FEC 等可读证据。

命令执行成功只表示证据可读。没有显式 `--rdma-policy-file` 时，主机 QoS 最高只能显示 `COLLECTED_POLICY_UNVALIDATED`，不能写成 PASS。

交换机侧 PFC/ECN、路由、队列、固件、光模块和 BER 需要独立管理面权限；没有 SNMP/gNMI/厂商 API 时保持 `NOT_VERIFIED`。

### 6.5 RDMA 计数器和 Verbs userspace

`--rdma-counter-interval SEC` 对端口计数器做两次采样：

- 错误、掉链或丢包增长：FAIL；
- 拥塞等待增长：WARN；
- 缺失、复位、回绕或饱和：UNKNOWN；
- 历史非零但观察窗口内稳定：不作为新增故障。

Verbs userspace 独立检查：

- `ibv_devices` 是否枚举 HCA；
- 每个 HCA 的 `ibv_devinfo` 是否能打开设备；
- provider 配置和相关动态库是否可见。

内核 sysfs 端点正常，不能掩盖 provider 缺失、ABI 不匹配或 userspace 无法打开设备。

## 7. 输出目录、报告与退出码

### 7.1 不同入口的输出目录规则

#### baremetal-cluster

`--output-dir` 是可重复使用的结果根目录：

```bash
--output-dir ./out
```

即使 `./out` 已经存在，每次也会创建：

```text
out/nodes_check_YYYYMMDD_HHMMSS_ffffff/
```

不会覆盖或混合历史结果。

#### k8s-cluster 和主动验收入口

这些入口的 `--output-dir` 是单次运行目录，必须使用新的、尚不存在的路径：

```bash
RUN_DIR="$PWD/out/k8s-cluster-$(date +%Y%m%d-%H%M%S)-$$"
```

已有路径会被拒绝，以防覆盖。

#### k8s-pod

显式指定的 `--output` 和 `--evidence-dir` 也必须使用新的、互不嵌套的目标。省略时工具会自动创建唯一运行目录。

### 7.2 裸金属运行目录结构

```text
nodes_check_时间戳/
├── cluster-summary.md
├── cluster-result.json
└── evidence/
    └── <run-id>/
        ├── run.json
        ├── controller.stdout
        ├── controller.stderr
        └── nodes/
            └── <node-hash>/
                ├── result.json
                ├── stdout.txt
                └── stderr.txt
```

如果启用了 IB/NHC 额外检查，还会在 evidence 下保存相应的 cluster-extra 证据。

### 7.3 四种主结果

| 退出码 | 状态 | 含义 |
|---:|---|---|
| `0` | `READY` | 满足本次启用的 Profile，仍可能包含 WARN |
| `1` | `BLOCKED` | 存在已经确认的阻断项 |
| `2` | `INCOMPLETE` | 节点不可达、超时、命令失败或证据不足 |
| `3` | `TOOL_ERROR` | 参数、节点来源、输出路径或控制器自身错误 |

启动器在主程序开始前失败时可能返回 69、70 等其他退出码。用户按 `Ctrl-C` 中断时返回 130。这些都不能解释为节点健康结论。

### 7.4 推荐的报告阅读顺序

打开 `cluster-summary.md` 后：

1. 看总体状态。
2. 找 `BLOCKED` 和 `INCOMPLETE` 节点。
3. 查看节点原因码。
4. 确认软件环境是 `CHECKED` 还是 `NOT_CHECKED`。
5. 看 RDMA 当前端口模式，再分别查看 IB 和 RoCE 端点。
6. 如果启用了额外节点检查，查看 `Cluster Extra Checks`。
7. 对异常节点进入 evidence，核对原始 stdout、stderr 和 result.json。

必须区分：

- `PASS`：已有足够证据通过；
- `NOT_CHECKED`：本次没有要求检查；
- `NOT_VERIFIED`：执行过，但证据不足；
- `UNKNOWN`：当前接口无法可靠判断。

后三者都不等于 PASS。

### 7.5 常见原因码

| 原因码 | 含义 | 建议 |
|---|---|---|
| `HCU_BUSY` | 多次采样利用率超过阈值 | 确认是否有预期作业占用 |
| `VRAM_IN_USE` | 多次采样显存超过阈值 | 检查残留或并行作业 |
| `REMOTE_RESULT_MISSING` | 远端没有返回有效探针结果 | 查看 SSH/clush、远端 Python 和 stderr |
| `SLURM_NODE_UNAVAILABLE` | 节点处于 drain/down/fail 等状态 | 查看 Slurm reason |
| `IB_PORT_NOT_ACTIVE` | IB 端口不是 Active/LinkUp | 定位具体 HCA/端口和链路 |
| `IB_BANDWIDTH_BELOW_THRESHOLD` | 某条测试路径低于阈值 | 查看方向、HCA、rail 和实测值 |
| `NHC_CHECK_FAILED` | NHC 明确报告故障 | 查看 NHC 原始输出 |
| `NHC_COMMAND_NOT_FOUND` | 节点没有 `run_nhc` | 按报告中的安装来源部署或修复 |
| `NHC_RESULT_MARKER_MISSING` | NHC 输出无法解析 | 检查脚本输出协议 |
| `TORCH_IMPORT_FAILED` | 显式要求 Torch 后导入失败 | 确认解释器/Conda/镜像是否与训练一致 |

### 7.6 万卡规模结论

`--target-scale-devices` 只用于静态适用性评估，不会自动执行对应规模压测。

| 结论 | 含义 |
|---|---|
| `NOT_READY` | 已检查样本存在阻断项 |
| `NOT_VERIFIED` | 证据不完整 |
| `SAMPLE_READY_FULL_SCALE_UNVERIFIED` | 样本通过，但没有覆盖目标卡数 |
| `FULL_SCALE_STATIC_PREFLIGHT_PASSED_RUNTIME_UNVERIFIED` | 静态覆盖达到目标，仍未证明训练或通信稳定 |

## 8. 常见故障排查

### 8.1 控制端找不到 Python 3.10+

```bash
export HCU_ENVCHECK_PYTHON=/usr/local/python3.12/bin/python3
./bin/hcu-envcheck-doctor
```

目标节点 Python 路径不同，还需要设置：

```text
--remote-python /path/to/python3
```

### 8.2 SSH 无法登录或要求密码

工具使用非交互 SSH，不会弹出密码输入。先单独验证：

```bash
ssh node37 hostname
```

需要指定用户、端口或密钥时，显式使用 `--transport ssh` 和对应参数。

出现：

```text
REMOTE HOST IDENTIFICATION HAS CHANGED
```

必须通过可信渠道核对新指纹，再修正 `known_hosts`。

### 8.3 `auto` 选择 clush 后失败

先验证：

```bash
clush -w node37,node98 hostname
```

如果站点 clush 配置不可用而 SSH 可用，明确指定：

```text
--transport ssh
```

### 8.4 `pam_slurm_adopt` 拒绝访问

常见提示：

```text
Access denied by pam_slurm_adopt: you have no active jobs on this node
```

处理步骤：

1. 按站点规则申请覆盖目标节点的 Slurm 作业。
2. 请求完整的每节点 HCU GRES。
3. 等待作业进入 RUNNING。
4. 核对 `scontrol show job <JOB_ID>` 的 NodeList 和 AllocTRES。
5. 使用 `--slurm-job-id <JOB_ID>` 运行检查。

工具不会绕过 PAM，也不会自动申请或取消作业。

### 8.5 预期 8 卡但只看到部分设备

依次检查：

1. Slurm `AllocTRES`/GRES，或 K8s HCU request/limit；
2. `ROCR_VISIBLE_DEVICES`、`HIP_VISIBLE_DEVICES`、`CUDA_VISIBLE_DEVICES`；
3. Slurm cgroup/GRES 绑定；
4. K8s 设备插件和容器设备透传；
5. `rocminfo` 与 `hy-smi` 原始证据是否一致。

### 8.6 报告中 HCU 是空数组

先区分：

- 远端命令没有采集到设备；
- 设备可见性被 Slurm/K8s 限制；
- `rocminfo`/`hy-smi` 缺失或执行失败；
- Python/Torch 检查没有启用。

HCU 硬件发现不依赖 Torch。默认没有 `--require-python-package torch` 时不导入 Torch，但仍应从宿主硬件命令采集 HCU。应查看对应节点 evidence 中的 `rocminfo`、`hy-smi`、设备文件和 stderr。

### 8.7 `TORCH_IMPORT_FAILED`

该错误只应在显式要求：

```text
--require-python-package torch
```

后出现。检查：

- `--remote-python` 是否指向实际训练解释器；
- 是否应该使用 `--software-mode conda`；
- Docker 模式镜像是否正确；
- Torch 与 DTK/运行库是否兼容。

如果本次只检查宿主硬件，不要传 `--require-python-package torch`。

### 8.8 NHC 为什么是 INCOMPLETE

重点查看原因码：

- `NHC_COMMAND_NOT_FOUND`：找不到 `run_nhc`；
- `NHC_CHECK_TIMEOUT`：超时；
- `NHC_EXECUTION_ERROR`/`NHC_EXECUTION_FAILED`：执行异常；
- `NHC_RESULT_MARKER_MISSING`：输出没有可解析结果。

先在目标节点直接执行：

```bash
command -v run_nhc
run_nhc
echo $?
```

确认输出包含明确的 `[CHECK RESULT]`。工具不会自动安装；安装来源会写入报告。

### 8.9 `ib_write_bw` 失败或低于阈值

先从 `Cluster Extra Checks` 找到：

- source 节点；
- destination 节点；
- source HCA；
- destination HCA；
- rail；
- 实测 Gbit/s；
- 阈值。

再检查两端：

```bash
ibstat
ibv_devinfo
```

常见原因包括端口未 Active、HCA rail 对应关系不一致、服务端启动失败、控制端口冲突、GID/协议错误、MTU/链路速率问题或网络拥塞。

### 8.10 K8s Forbidden

使用：

```bash
kubectl auth can-i get nodes
kubectl auth can-i get pods -n "$NAMESPACE"
kubectl auth can-i create pods -n "$NAMESPACE"
kubectl auth can-i delete pods -n "$NAMESPACE"
kubectl auth can-i create pods/exec -n "$NAMESPACE"
```

按最小权限补齐 RBAC。

### 8.11 临时 Pod Pending 或未 Ready

```bash
kubectl get pod -n "$NAMESPACE" "$POD" -o wide
kubectl describe pod -n "$NAMESPACE" "$POD"
kubectl get node "$NODE"
```

常见原因：

- 完整 HCU 资源不可分配；
- 节点 unschedulable 或有 taint；
- 设备插件异常；
- 镜像拉取失败；
- 配额不足；
- Pod Security/准入策略拒绝特权 Pod。

### 8.12 输出目录已经存在

先确认使用的是哪个入口：

- `baremetal-cluster`：允许根目录已存在，会自动创建 `nodes_check_时间戳`。
- `k8s-cluster`、主动验收入口：要求单次运行目录不存在。
- `k8s-pod`：显式 output/evidence 目标必须不存在。

不要把 K8s 的单次运行目录规则套用到裸金属 `--output-dir` 根目录。

### 8.13 TOOL_ERROR 和 traceback

先阅读：

```text
RESULT        TOOL_ERROR
ERROR         ...
```

只有定位工具自身异常时才临时开启：

```bash
HCU_ENVCHECK_DEBUG=1 ./hcu-envcheck.sh <原命令>
```

调试输出可能包含现场路径或错误细节，不应直接公开。

## 9. 主动通信验收（高级功能）

主动入口只允许在使用者拥有的专用、空闲 Slurm allocation 中运行。它们不会由静态检查自动触发。

### 9.1 Slurm 安全边界

正式验收要求：

- 节点属于指定 Job；
- 提供对应的 enable 参数；
- 提供 `--confirm-allocation-idle`；
- allocation 没有 `.batch` 或其他 workload step；
- 能证明作业级整节点独占。

优先接受 `scontrol show job -o` 的 `Exclusive=NODE`。旧版 Slurm 没有该字段时，工具会组合检查：

- `OverSubscribe=NO`；
- 所选节点等于 Job 完整节点集；
- 节点 `State=ALLOCATED`；
- `CPUAlloc=CPUTot`；
- 节点分配 HCU 数等于配置 HCU 数；
- Job 自身 `NumNodes`、`NumCPUs` 和 `AllocTRES` 与节点容量合计一致；
- 没有可见的其他活动 Job。

任何查询、字段或解析缺失都 fail-closed。`--unsafe-allow-overlap` 只用于诊断，结果标记为 `OVERLAP_NOT_PROVEN_IDLE`，不能获得正式 PASS。

### 9.2 active-rdma-slurm

支持：

| backend | 检查内容 |
|---|---|
| `verbs` | 两节点 `ib_write_bw`/`ib_send_bw`/`ib_read_bw` 可达性和带宽 |
| `rccl` | 宿主机已安装的 `all_reduce_perf` |
| `torch-rccl` | 两节点 PyTorch all-reduce 正确性和 RCCL 传输路径 |

Verbs 示例：

```bash
ACTIVE_JOB_ID="${SLURM_JOB_ID:?run inside a dedicated allocation}"

./hcu-envcheck.sh active-rdma-slurm \
  --slurm-job-id "$ACTIVE_JOB_ID" \
  --nodes-file nodes.txt \
  --backend verbs \
  --rdma-protocol ib \
  --verbs-hca shca_0 \
  --verbs-port 1 \
  --verbs-tool ib_write_bw \
  --verbs-message-bytes 1048576 \
  --verbs-iterations 1000 \
  --minimum-verbs-gbps 200 \
  --enable-active-checks \
  --confirm-allocation-idle \
  --output-dir "$PWD/out/active-verbs-$(date +%Y%m%d-%H%M%S)-$$"
```

PyTorch/RCCL 示例：

```bash
./hcu-envcheck.sh active-rdma-slurm \
  --slurm-job-id "$ACTIVE_JOB_ID" \
  --nodes-file nodes.txt \
  --backend torch-rccl \
  --container-name "$ACTIVE_CONTAINER" \
  --python-binary python3 \
  --master-port 29500 \
  --enable-active-checks \
  --confirm-allocation-idle \
  --output-dir "$PWD/out/active-torch-$(date +%Y%m%d-%H%M%S)-$$"
```

RCCL 功能、GDR 和性能是三个独立维度：

- 功能 PASS 要求结果完整、所有 Rank 正确、无 `#wrong`/OOB，并且实际选择 RDMA；
- `--require-rccl-gdr` 要求每个 Rank 有明确 GDR Enabled 证据；
- 没有设置性能阈值时，功能可以 PASS，但性能保持 `NOT_VERIFIED`。

### 9.3 ib-fabric-slurm

示例：

```bash
./hcu-envcheck.sh ib-fabric-slurm \
  --slurm-job-id "$ACTIVE_JOB_ID" \
  --nodes-file nodes.txt \
  --hca shca_0 \
  --hca shca_1 \
  --hca shca_2 \
  --hca shca_3 \
  --ib-port 1 \
  --expected-link-width 4X \
  --minimum-link-speed-gbps 400 \
  --counter-interval 5 \
  --query-qps 2 \
  --max-workers 16 \
  --overall-timeout 900 \
  --enable-fabric-check \
  --confirm-allocation-idle \
  --output-dir "$PWD/out/ib-fabric-$(date +%Y%m%d-%H%M%S)-$$"
```

该入口：

- 只适用于 Native IB；
- 通过有界一跳 MAD 查询定位叶交换机端口；
- 双采样标准和扩展端口计数器；
- 不执行全 Fabric 扫描；
- 不执行任何 reset；
- 不替代交换机管理面 PFC/ECN、路由、固件和光模块检查。

`--minimum-link-speed-gbps` 是所有 lane 的聚合速率下限。例如 `4X 106.25 Gbps` 的聚合速率是 `425 Gbps`，因此聚合阈值 `400` 可以通过。

查看全部高级参数：

```bash
./hcu-envcheck.sh active-rdma-slurm --help
./hcu-envcheck.sh ib-fabric-slurm --help
```

## 10. 工具边界和安全说明

### 10.1 静态检查会做什么

- 读取 HCU、驱动、DTK、CPU、内存、网络和 RDMA 信息；
- 只读采样显存和 HCU 利用率；
- 根据明确 Profile 判断环境是否满足训练要求；
- 生成 Markdown、JSON 和原始 evidence；
- K8s 集群模式会创建和清理带唯一 run-id 的临时 Pod。

### 10.2 静态检查不会做什么

- 不重置 HCU；
- 不结束进程或清理显存；
- 不修改驱动、固件、网卡模式或 ACS；
- 不自动安装 Python 包、NHC 或系统软件；
- 不恢复、添加或删除 Kubernetes taint；
- 不运行客户模型；
- 不编译扩展；
- 不执行 RCCL collective；
- 不在未显式启用时产生网络带宽测试流量；
- 不把样本结论描述成全规模训练验证。

### 10.3 证据和敏感信息

证据目录默认限制为当前用户访问。证据仍可能包含：

- 主机名；
- 内核和驱动版本；
- 设备标识；
- 内部路径；
- 命令错误文本。

应按内部诊断数据管理，不要直接上传公共平台。工具不采集完整环境变量，也不保存完整 Pod JSON；`--probe-env` 只接受受限变量。

## 11. 命令帮助和问题反馈

查看帮助：

```bash
./hcu-envcheck.sh --help
./hcu-envcheck.sh baremetal-cluster --help
./hcu-envcheck.sh k8s-pod --help
./hcu-envcheck.sh k8s-cluster --help
./hcu-envcheck.sh active-rdma-slurm --help
./hcu-envcheck.sh ib-fabric-slurm --help
./install.sh --help
```

反馈现场问题时至少保留：

- 工具版本；
- 完整执行命令，删除敏感密钥路径；
- 命令退出码；
- `cluster-summary.md`；
- `cluster-result.json`；
- 异常节点的 evidence 目录。

不要只截取一行错误文本。节点问题、远端执行问题和工具参数问题可能产生相似表象，完整证据可以避免误判。
