<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# Node Check - 集群节点健康检测工具

## 概述

`run_check.sh` 是一个用于大规模集群节点健康状态检测的脚本工具。通过**两阶段分组测试策略**（横向 + 纵向），高效识别集群中的故障节点，支持大规模分布式训练前的节点预检。

## 核心特性

- **两阶段检测策略**：横向分组 + 纵向分组，通过交集定位故障节点
- **并行执行**：多组测试并行运行，大幅缩短检测时间
- **灵活配置**：支持多种分组规模，TFLOPs阈值可自定义
- **性能基准验证**：基于 TFLOPs 指标判断节点健康状态
- **自动报告生成**：输出健康/故障节点列表及详细诊断报告

## 文件结构

```
node_check/
├── run_check.sh          # 主检测脚本
├── check_nodes.sh        # 单组节点测试脚本（需配置路径）
├── clush.sh              # 集群命令分发工具
├── clushnode             # 待检测节点列表文件
├── hostfile              # 生成的 hostfile（自动创建）
├── hostslice/            # 分组后的 hostfile（自动创建）
│   ├── horizontal_host_1
│   ├── horizontal_host_2
│   └── ...
├── node_checklog/        # 测试日志目录（自动创建）
│   ├── horizontal_H1.log
│   ├── horizontal_H2.log
│   └── ...
└── dual_stage_results/   # 检测结果目录（自动创建）
    ├── horizontal/       # 横向测试失败节点
    ├── vertical/         # 纵向测试失败节点
    └── diagnosis_report.txt  # 诊断报告
```

## 使用方法

### 参数说明

| 参数 | 短选项 | 长选项 | 必选 | 默认值 | 说明 |
|------|--------|--------|------|--------|------|
| 节点文件 | `-c` | `--clushnode` | ✅ | - | 待检测节点列表文件 |
| 分组数量 | `-n` | `--nodenum` | ❌ | 4 | 每组节点数 |
| TFLOPs阈值 | `-t` | `--tflops` | ❌ | 100 | 性能基准阈值 |
| 健康节点路径 | `-g` | `--healthy` | ❌ | `./node_checklog/host_check_pass` | 健康节点保存路径 |
| 故障节点路径 | `-f` | `--fault` | ❌ | `./node_checklog/host_check_error` | 异常节点保存路径 |
| 仅横向检测 | `-o` | `--only-horizontal` | ❌ | 否 | 仅执行横向检测，跳过纵向检测 |
| 帮助 | `-h` | `--help` | - | - | 显示帮助信息 |

### 使用示例

#### 模式1：简化位置参数（推荐快速使用）

```bash
# 检测 clushnode 文件中的节点，每组 4 个节点
./run_check.sh ./clushnode 4

# 检测 clushnode 文件中的节点，每组 8 个节点
./run_check.sh ./clushnode 8
```

#### 模式2：完整选项参数

```bash
# 完整参数示例
./run_check.sh -c ./clushnode -n 4 -g ./healthy_nodes.txt -f ./fault_nodes.txt

# 仅执行横向检测
./run_check.sh -c ./clushnode -n 4 --only-horizontal

# 显示帮助信息
./run_check.sh -h
```

## 工作原理

### 两阶段检测策略

以 **32 节点、每组 4 节点** 为例：

```
节点列表: [N1, N2, N3, ..., N32]（共 32 个节点）

┌─────────────────────────────────────────────────────────────┐
│                    Phase 1: 横向分组测试                      │
├─────────────────────────────────────────────────────────────┤
│  将节点按顺序分组，每组 4 个节点，共 8 组：                    │
│                                                              │
│  H1:  [N1,  N2,  N3,  N4 ]  ──测试──> PASS/FAIL             │
│  H2:  [N5,  N6,  N7,  N8 ]  ──测试──> PASS/FAIL             │
│  H3:  [N9,  N10, N11, N12]  ──测试──> PASS/FAIL             │
│  H4:  [N13, N14, N15, N16]  ──测试──> PASS/FAIL             │
│  H5:  [N17, N18, N19, N20]  ──测试──> PASS/FAIL             │
│  H6:  [N21, N22, N23, N24]  ──测试──> PASS/FAIL             │
│  H7:  [N25, N26, N27, N28]  ──测试──> PASS/FAIL             │
│  H8:  [N29, N30, N31, N32]  ──测试──> PASS/FAIL             │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│                    Phase 2: 纵向分组测试                      │
├─────────────────────────────────────────────────────────────┤
│  从每个横向组中各取 1 个节点，组成纵向组，共 8 组，每组 4 节点：│
│                                                              │
│  V1:  [N1,  N5,  N9,  N13]  ──测试──> PASS/FAIL             │
│  V2:  [N2,  N6,  N10, N14]  ──测试──> PASS/FAIL             │
│  V3:  [N3,  N7,  N11, N15]  ──测试──> PASS/FAIL             │
│  V4:  [N4,  N8,  N12, N16]  ──测试──> PASS/FAIL             │
│  V5:  [N17, N21, N25, N29]  ──测试──> PASS/FAIL             │
│  V6:  [N18, N22, N26, N30]  ──测试──> PASS/FAIL             │
│  V7:  [N19, N23, N27, N31]  ──测试──> PASS/FAIL             │
│  V8:  [N20, N24, N28, N32]  ──测试──> PASS/FAIL             │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│                    故障节点定位                               │
├─────────────────────────────────────────────────────────────┤
│  横向失败节点 ∩ 纵向失败节点 = 故障节点                        │
│                                                              │
│  例如: H2 FAIL (含 N6) + V2 FAIL (含 N6) → N6 故障           │
│                                                              │
│  原理：若某节点故障，其所在的横向组和纵向组都会测试失败        │
│        通过求交集可精确定位故障节点                           │
└─────────────────────────────────────────────────────────────┘
```

### 性能基准

默认 TFLOPs 阈值为 **100**，可通过 `-t` 或 `--tflops` 参数自定义。

```bash
# 使用默认阈值 100
./run_check.sh -c ./clushnode -n 4

# 自定义阈值为 200
./run_check.sh -c ./clushnode -n 4 -t 200
```

> 注：实际基准值可能因硬件配置不同而有所差异，请根据实际情况通过 `-t` 参数调整。

## 输出说明

### 1. 控制台输出

脚本执行过程中会实时输出测试状态：

```
========== Phase 1: Horizontal Group Testing ==========
Creating horizontal group H1: nodes 1-4
Creating horizontal group H2: nodes 5-8
...

Analyzing horizontal group results...
[PASS] Horizontal group H1 [node1,node2,node3,node4]: 190.5 TFLOPs
[FAIL] Horizontal group H2 [node5,node6,node7,node8]: 150.2 TFLOPs < 185 TFLOPs
```

### 2. 结果文件

| 文件 | 说明 |
|------|------|
| `host_check_pass` | 健康节点列表 |
| `host_check_error` | 故障节点列表 |
| `diagnosis_report.txt` | 详细诊断报告 |

### 3. 诊断报告示例

```
Dual-Stage Fault Diagnosis Report
==================================
Test Time: 2026-04-14 11:30:00
Total Nodes: 128
Nodes Participating in Test: 128
Nodes Per Group: 4
Baseline TFLOPs: 185

Phase 1 - Horizontal Groups:
  Total Groups: 32
  Failed Groups: 2 5 8

Phase 2 - Vertical Groups:
  Total Groups: 32
  Failed Groups: 3 7

Fault Nodes (Intersection):
  node42
  node86

Diagnosis Conclusion:
Faulty nodes detected,建议驱逐
```

## 配置说明

在首次使用前，需要根据实际环境修改 `check_nodes.sh` 脚本中的配置变量：

```bash
# 配置文件路径
node_check/check_nodes.sh
```

### 需要修改的配置项

打开 `check_nodes.sh`，找到以下配置变量并根据实际环境修改：

| 配置项 | 说明 | 示例值 |
|--------|------|--------|
| `MEGATRON_PATH` | Megatron-LM 安装路径 | `/public/home/user/hcu_megatron` |
| `TRAIN_PATH` | 训练脚本路径 | `${MEGATRON_PATH}/examples/gpt3` |
| `DATA_PATH` | 数据集路径 (redpajama_text_document) | `/path/to/redpajama_text_document` |
| `TOKENIZER_MODEL_PATH` | Tokenizer 模型路径 | `/path/to/tokenizer.model` |
| `CHECKPOINT_PATH` | Checkpoint 保存路径 | `./ckpt` |
| `DTK_ENV` | DTK 环境脚本路径（可选） | `""` |
| `NCCL_ENV` | NCCL 环境脚本路径 | `${MEGATRON_PATH}/requirements/env.sh` |
| `LAUNCH_WITH_BINDING` | Launch with binding 脚本路径 | `${MEGATRON_PATH}/requirements/launch_with_binding.sh` |
| `CONDA_INIT_PATH` | Conda 初始化脚本路径 | `/path/to/anaconda3/etc/profile.d/conda.sh` |
| `CONDA_ENV` | Conda 环境名称 | `cluster_manager` |
| `TRAIN_SCRIPT` | 训练脚本路径（自动生成） | `${TRAIN_PATH}/train_gpt_567B_*nodes.sh` |

### 配置示例

在 `check_nodes.sh` 中修改以下部分：

```bash
MEGATRON_PATH="/public/home/user/hcu_megatron"
TRAIN_PATH=${MEGATRON_PATH}/examples/gpt3

# Those variables need to modify
DTK_ENV=""                                                               # where env.sh of dtk
DATA_PATH="/public/home/user/dataset/gpt_datasets_samples/redpajama_text_document"
TOKENIZER_MODEL_PATH="/public/home/user/dataset/gpt_datasets_samples/tokenizer.model"
CHECKPOINT_PATH="./ckpt"
NCCL_ENV=${MEGATRON_PATH}/requirements/env.sh
LAUNCH_WITH_BINDING=${MEGATRON_PATH}/requirements/launch_with_binding.sh

# Conda and training script configuration
CONDA_INIT_PATH="/public/home/user/anaconda3/etc/profile.d/conda.sh"  # path to conda init script
CONDA_ENV="cluster_manager"                                               # conda environment name
TRAIN_SCRIPT="${TRAIN_PATH}/train_gpt_567B_$((${GPUS} / 8))nodes.sh"     # training script path
```

## 依赖要求

### 软件依赖

- **MPI 环境**：需要加载 `mpi/hpcx/2.18.0/gcc-8.5.0/shca` 模块
- **Conda 环境**：`cluster_manager`

### 文件依赖

- `check_nodes.sh`：单组节点测试脚本
- 训练脚本：`train_gpt_567B_*nodes.sh`（位于 `TRAIN_PATH` 目录下）
- 数据集路径和 tokenizer 模型路径

## 节点列表文件格式

`clushnode` 文件格式（每行一个节点名）：

```
node001
node002
node003
...
```

## 注意事项

1. **执行权限**：确保脚本有执行权限
   ```bash
   chmod +x run_check.sh check_nodes.sh
   ```

2. **路径配置**：首次使用前必须修改 `check_nodes.sh` 中的以下配置项：
   - `MEGATRON_PATH`：Megatron-LM 路径
   - `DATA_PATH`：数据集路径
   - `TOKENIZER_MODEL_PATH`：Tokenizer 模型路径
   - `NCCL_ENV`：NCCL 环境脚本路径
   - `CONDA_INIT_PATH`：Conda 初始化脚本路径
   - `CONDA_ENV`：Conda 环境名称

3. **资源占用**：测试过程会占用 GPU 资源，请确保节点空闲

4. **测试时间**：大规模节点检测可能需要较长时间，建议在后台运行

## 故障排查

### 常见问题

| 问题 | 可能原因 | 解决方案 |
|------|----------|----------|
| `Test failed or no output` | MPI 启动失败或训练脚本错误 | 检查 `node_checklog/` 下的日志文件 |
| TFLOPs 持续偏低 | 基准值设置过高 | 使用 `-t` 参数降低阈值 |

### 日志位置

- 横向测试日志：`./node_checklog/horizontal_H*.log`
- 纵向测试日志：`./node_checklog/vertical_V*.log`

## 版本历史

- **v1.0**：初始版本，支持两阶段检测
- **v1.1**：新增 `--only-horizontal` 选项，支持仅横向检测模式
- **v1.2**：新增 `-t, --tflops` 参数，TFLOPs阈值支持命令行配置，移除节点数限制

## 作者

HCU Cluster Manager Team
