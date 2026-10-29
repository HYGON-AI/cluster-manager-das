<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# hcu-envcheck 命令示例、可选参数与结果目录

## 运行位置与目录挂载

工具命令在 Linux 宿主机上执行，通常从登录节点发起。使用 `per-node-container` 场景时，检测脚本和测试载荷在各计算节点的容器内执行。

创建或重建容器时，必须通过 `-v 宿主机目录:容器内目录` 将 `hcu-envcheck` 所在目录挂载进容器，确保每个计算节点的宿主机和容器都能访问工程文件。建议容器内外使用相同的绝对路径。例如工程位于 `/shared/cluster-manager-das/hcu-envcheck`，可使用：

```text
-v /shared/cluster-manager-das/hcu-envcheck:/shared/cluster-manager-das/hcu-envcheck
```

下文创建和重建命令使用 `-v /shared:/shared`，已覆盖上述工程目录及 `/shared` 下的配置和结果目录。若只挂载工程目录，工程外的环境脚本、基准文件及结果目录也需按实际位置额外挂载，保证容器内可访问。

以下路径、节点、镜像和端口均为通用示例。环境准备、总体设计与结果判读见 [DESIGN](DESIGN.md)，当前限制见 [KNOWN_LIMITATIONS](KNOWN_LIMITATIONS.md)。

## 命令准备

```bash
cd /shared/cluster-manager-das/hcu-envcheck
cat > hostfile <<'EOF'
node001
node002
node003
EOF
bash bin/hcu-cluster-run --help
bash bin/hcu-cluster-run --version
```

## 容器操作

创建和重建时，`-i/--image` 指定的镜像会在 hostfile 的每个目标节点宿主机上检查：本地存在则直接复用，Docker 明确报告镜像不存在时才执行 `docker pull`。已有镜像不会自动更新；SSH、Docker 服务或权限检查失败时直接报错。逐节点明细记录镜像来源 `local` 或 `pull`。

显式传入 `--image-tar` 时仍按原逻辑导入归档。多节点继续核对镜像 ID 一致性；重建需在全部节点预检通过后才替换旧容器。

```bash
# 查询已有容器
bash bin/hcu-cluster-run per-node-container container-status \
  -f hostfile --container hcu-test -i registry.example.com/hcu/train:validated

# 创建计划；实际创建去掉 --dry-run
bash bin/hcu-cluster-run per-node-container container-create \
  -f hostfile --container hcu-test -i registry.example.com/hcu/train:validated \
  --port 25901 -v /shared:/shared --dry-run

# 重建计划；实际重建去掉 --dry-run 并添加 --yes，先核对已知限制
bash bin/hcu-cluster-run per-node-container container-recreate \
  -f hostfile --container hcu-test -i registry.example.com/hcu/train:validated \
  --port 25901 -v /shared:/shared --dry-run

# 删除计划；实际删除去掉 --dry-run 并添加 --yes
bash bin/hcu-cluster-run per-node-container container-delete \
  -f hostfile --container hcu-test --dry-run
```

## platform

```bash
bash bin/hcu-cluster-run per-node-container platform \
  -f hostfile --container hcu-test -i registry.example.com/hcu/train:validated
```

## resource

```bash
# 资源
bash bin/hcu-cluster-run per-node-container resource \
  -f hostfile --container hcu-test -i registry.example.com/hcu/train:validated

# 平台与资源联合检测
bash bin/hcu-cluster-run per-node-container platform,resource \
  -f hostfile --container hcu-test -i registry.example.com/hcu/train:validated

# 共享 Conda
bash bin/hcu-cluster-run shared-conda platform,resource \
  -f hostfile --env-script /shared/hcu-config/train-env.sh

# 各节点独立 Conda
bash bin/hcu-cluster-run node-local-conda platform,resource \
  -f hostfile --env-script /opt/hcu-config/train-env.sh
```

## checkdeepep

```bash
# DeepEP 前置环境检查
bash bin/hcu-cluster-run per-node-container checkdeepep \
  -f hostfile --container hcu-test -i registry.example.com/hcu/train:validated \
  --timeout 300
```

## 测试共用可选参数

下表适用于 RCCL、DeepEP 和 byte-flux 的三种运行场景。可将参数追加到对应的完整测试命令末尾；未指定时使用默认值。RCCL/DeepEP 的 G/C 参数与设备选择说明见 [DESIGN](DESIGN.md#测试规模与设备选择)。

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `--skip-idle-check` | 不开启，即先检查空闲状态 | 仅跳过计算卡空闲检查，无须传值；容器、依赖、RCCL/DeepEP 设备数量与身份、RCCL 基准和结果检查仍会执行。 |
| `--timeout SECONDS` | RCCL、DeepEP 为 `300`；byte-flux 为 `4800` | 正整数秒，按测试项和分组计时，预检也使用此预算，不是整次运行的总时长。byte-flux 的一项为一次完整的 64 项矩阵；超时后清理另有宽限。 |
| `--concurrency N` | `32` | 同时执行的分组数量上限；单节点测试中每个节点各为一组。 |
| `--node-concurrency N` | `32` | 跨 worker 的普通节点 SSH 连接预算，报告与停止/清理另有独立或保留连接；不表示计算卡或测试进程数量。 |
| `--log-detail MODE` | `milestone` | `milestone` 显示摘要，`every` 显示更详细的节点/分组快照，`quiet` 仅显示异常；不改变测试和日志采集内容。 |
| `--env-script PATH` | 不指定 | 在目标节点或容器中加载环境脚本，统一配置库路径、通信及载荷支持的环境变量；使用目标可访问的绝对路径。示例脚本需按 [DESIGN](DESIGN.md#附录场景与环境脚本)准备。 |
| `--container-workdir PATH` | 容器内外工程路径相同 | 容器内工程根目录的绝对路径；路径不同时需配合实际挂载设置。 |
| `-o PATH` / `--output-dir PATH` | 工程下的 `verify_results` | 运行目录的父目录，每次测试在其下创建独立目录；目标环境也需可访问。 |
| `--dry-run` | 不开启 | 只预览分组和测试计划，不执行测试、不生成本次测试结果目录。 |

空闲检查默认读取全部可见卡，显存占用及计算利用率阈值均为 `5%`，可由目标环境配置覆盖。跳过检查不会释放已占用显存或停止其他进程；公共入口不提供 `--vram-max`、`--util-max` 或 `--cards` 调参。

```bash
# 示例：跳过单机 DeepEP 的计算卡空闲检查，其他预检和结果判定照常执行
bash bin/hcu-cluster-run per-node-container deepep \
  -f hostfile --container hcu-test -i registry.example.com/hcu/train:validated \
  --env-script /shared/hcu-config/train-env.sh \
  --group-size 1 --gpus-per-node 4 --skip-idle-check
```

## RCCL

```bash
# 每节点各为一个单机四卡组；基准须覆盖四卡十项
bash bin/hcu-cluster-run per-node-container rccl \
  -f hostfile --container hcu-test -i registry.example.com/hcu/train:validated \
  --env-script /shared/hcu-config/train-env.sh \
  --group-size 1 --gpus-per-node 4 \
  --baseline-file /shared/hcu-config/rccl_baseline.conf

# 双机，每节点四卡；基准须覆盖八卡十项；示例第三节点不参与
bash bin/hcu-cluster-run per-node-container rccl \
  -f hostfile --container hcu-test -i registry.example.com/hcu/train:validated \
  --env-script /shared/hcu-config/train-env.sh \
  --group-size 2 --gpus-per-node 4 --container-ssh-port 25901 \
  --baseline-file /shared/hcu-config/rccl_baseline.conf

# 双机，每节点八卡；基准须覆盖十六卡十项；示例第三节点不参与
bash bin/hcu-cluster-run per-node-container rccl \
  -f hostfile --container hcu-test -i registry.example.com/hcu/train:validated \
  --env-script /shared/hcu-config/train-env.sh \
  --group-size 2 --gpus-per-node 8 --container-ssh-port 25901 \
  --baseline-file /shared/hcu-config/rccl_baseline.conf
```

### RCCL 专用可选参数

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `--group-size G` | `1` | 正整数，每组节点数；只执行按 hostfile 顺序形成的完整组，尾部节点不参与；没有完整组则报错。 |
| `--gpus-per-node C` | `8` | 正整数，每节点参与卡数；与 G 独立。基准按 G×C 匹配；目标环境不足 C 张有效设备时预检失败。 |
| `--container-ssh-port N` | `25901` | 多节点容器间 SSH 端口，须与创建容器时的 `--port` 一致；不是登录服务器的 SSH 端口。 |
| `--baseline-file PATH` | 使用已配置的基准来源 | 指定目标环境可访问的基准文件绝对路径，须覆盖当前总卡数及全部待测项；缺失时预检失败。 |

RCCL 的二进制路径、通信配置及载荷环境变量统一通过 `--env-script` 指定的脚本设置，见 [RCCL 基准与环境](DESIGN.md#基准与环境)。规模仍通过顶层 G/C 设置；默认执行完整十项。RCCL/DeepEP 不支持旧公共 `--slots`、`--nproc-per-node`，用户 hostfile 也不得包含 `slots=`。

## DeepEP

```bash
# 每节点各为一个单机四卡组；当前单机允许 2/4/8 卡
bash bin/hcu-cluster-run per-node-container deepep \
  -f hostfile --container hcu-test -i registry.example.com/hcu/train:validated \
  --env-script /shared/hcu-config/train-env.sh \
  --group-size 1 --gpus-per-node 4

# 双机，每节点固定八卡；示例第三节点不参与
bash bin/hcu-cluster-run per-node-container deepep \
  -f hostfile --container hcu-test -i registry.example.com/hcu/train:validated \
  --env-script /shared/hcu-config/train-env.sh \
  --group-size 2 --gpus-per-node 8 --container-ssh-port 25901
```

### DeepEP 专用可选参数

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `--group-size G` | `1` | 正整数，每组节点数；只运行完整组。当前完整多机测试受上游约束，G 仅允许 `2` 或 `4`。 |
| `--gpus-per-node C` | `8` | 与 G 独立；当前单机允许 `2/4/8`，多机必须为 `8`。G=2、C=4 在远程预检前拒绝，不能跳过 norm 继续测试。 |
| `--container-ssh-port N` | `25901` | 多节点容器间 SSH 端口，须与创建容器时的 `--port` 一致；不是登录服务器的 SSH 端口。 |

每组单机执行一次 `intranode`，多机分别执行一次 `lowl`、`norm`。上游 warmup 和测量迭代保持原样。卡数及专家数约束见 [DeepEP 设计](DESIGN.md#deepep)；上述范围仍需目标集群硬件验收。

## byte-flux

```bash
# 单机 TP4，完整 64 项矩阵
bash bin/hcu-cluster-run per-node-container byte-flux \
  -f hostfile --container hcu-test -i registry.example.com/hcu/train:validated \
  --env-script /shared/hcu-config/train-env.sh \
  --timeout 4800
```

### byte-flux 可选参数

每组执行一次完整的 64 项矩阵，保留上游测例自身的 warmup 和测量迭代。`--timeout` 默认 `4800` 秒，按每组的完整矩阵计算，可按实际耗时调大。共用的 `--skip-idle-check`、并发、环境、日志及输出参数同样适用。

byte-flux 固定逐节点执行单机 TP4，`--group-size` 只能为 `1`，不提供卡数或并行度配置；不支持 `--gpus-per-node`、`--container-ssh-port`。RCCL、DeepEP 和 byte-flux 的公共命令不支持 `--launcher`、`--profile`、`--master-port`、`--np` 或 `--transport`，请使用本页列出的参数。

## 报告重建、导出与重跑预览

```bash
# 重建既有双机 RCCL 报告；EXISTING_RUN 替换为真实运行目录，保留原报告环境
bash bin/hcu-cluster-run per-node-container rccl \
  -f hostfile --container hcu-test -i registry.example.com/hcu/train:validated \
  --env-script /shared/hcu-config/train-env.sh \
  --group-size 2 --gpus-per-node 8 \
  --report-only /shared/cluster-manager-das/hcu-envcheck/verify_results/EXISTING_RUN

# 重建并导出清单；导出目录必须尚不存在
bash bin/hcu-cluster-run per-node-container rccl \
  -f hostfile --container hcu-test -i registry.example.com/hcu/train:validated \
  --env-script /shared/hcu-config/train-env.sh \
  --group-size 2 --gpus-per-node 8 \
  --report-only /shared/cluster-manager-das/hcu-envcheck/verify_results/EXISTING_RUN \
  --export-plan /shared/hcu-plans/new-export

# 只预览失败重跑计划；不额外覆盖原 G/C 或测试项
bash bin/hcu-cluster-run per-node-container rccl \
  -f hostfile --container hcu-test -i registry.example.com/hcu/train:validated \
  --env-script /shared/hcu-config/train-env.sh \
  --baseline-file /shared/hcu-config/rccl_baseline.conf \
  --rerun-plan /shared/hcu-plans/new-export/failed-groups.json --dry-run
```

### 报告相关可选参数

以下参数适用于上述三项测试的分阶段报告。

| 参数 | 默认值 / 含义 |
| --- | --- |
| `--report-host HOST` | 默认第一个 hostfile 节点；显式选择报告节点，不将其加入测试分组。历史报告重建需保留原报告节点。 |
| `--report-env-script PATH` | 默认沿用 `--env-script`；指定报告节点目标环境中的环境脚本绝对路径。 |
| `--report-python PATH` | 默认在报告环境中寻找满足 Python ≥3.10 的 `python3` 或 `python`；可显式指定解释器。 |
| `--report-workdir PATH` | 默认目标工程根目录，容器场景优先使用 `--container-workdir`；指定报告节点目标环境中的工程绝对路径。 |
| `--report-only RUN_DIR` | 仅重建既有运行目录的报告，不启动测试。 |
| `--export-plan DIR` | 与 `--report-only` 配合，将失败/未完成清单导出到尚不存在的新绝对路径目录。 |
| `--rerun-plan FILE` | 读取导出的绝对路径 JSON，保持原节点、规模及项目，在新运行目录重跑；`--dry-run` 只预览，清理未确认的条目阻断实际重跑。 |

`--rerun-plan` 不与 `--report-only`、`--export-plan` 同用；RCCL/DeepEP 的 G/C 从清单恢复，不再传 `--group-size`、`--gpus-per-node` 或更改 RCCL 测试子集。历史报告仍可重建；历史计划仅在规模证据明确时转换，不猜测旧 slots 含义。完整限制见 [重跑计划](DESIGN.md#重跑计划)。

## 结果目录

### platform、resource、checkdeepep、RCCL、DeepEP 与 byte-flux

```text
verify_results/<scenario>_<operation>_stages_<timestamp>_<pid>/
├── summary.md                         # 结果与覆盖摘要
├── failures.tsv                       # 异常索引
├── group-NNN.log                      # 每组一份完整原始日志
└── .state/
    ├── control.tsv                    # 运行配置、G/C 计划规模、状态与覆盖
    ├── groups.tsv / stages.tsv         # 分组与阶段计划
    ├── report-host.tsv                # 报告节点和解释器
    ├── preflight.tsv / report.json     # 预检摘要、计划规模及实际执行证据
    ├── prepared/
    │   ├── task-guard.sh               # 执行所需的公共脚本
    │   └── group-NNN/
    │       ├── slotsfile               # RCCL: node slots=C；DeepEP: node slots=1
    │       ├── hostfile                # 其他操作的组节点文件
    │       ├── facts.json              # 本组预检、执行与清理事实
    │       ├── rccl/rank-body.sh        # 按操作生成的必要执行脚本
    │       └── rccl-summary.{md,tsv}    # RCCL 组汇总
    └── runtime/group-NNN/              # 活动组临时目录；异常时可能保留
```

报告重建需保留整个运行目录及 `.state`。目录项按实际操作和执行进度生成，设备选择记录不表示测试已经通过。旧运行仍可按原布局重建报告，详见 [结果与恢复设计](DESIGN.md#结果与恢复设计)。

### 显式导出的清单

```text
/shared/hcu-plans/new-export/
├── failed-nodes.hostfile
├── failed-groups.json
├── unfinished-groups.json
└── cleanup-unconfirmed.json
```

### 容器创建、重建与删除

```text
verify_results/per-node-container_container-<create|recreate|delete>_lifecycle_<timestamp>_<random>/
└── lifecycle-details.log              # 逐节点结果、原始错误与身份明细
```

`-o/--output-dir` 指定上述运行目录的父目录；终端末尾的 `LOG` 给出实际明细路径。

### 容器查询与 dry-run

`container-status` 输出终端逐节点结果和汇总，不生成结果目录。Bash 分阶段与容器操作的 dry-run 不生成测试结果目录。

## 一键空闲检测与释放

`bash bin/hcu-cluster-run per-node-container idle-check -f hostfile`

`bash bin/hcu-cluster-run per-node-container idle-check -f hostfile --release` 

全部卡 VRAM% 和 HCU% 都等于 0 才空闲，任意非零即查询 PID 与容器。检测失败不当作空闲。释放停止确认占用的容器全部任务，不主动删除；AutoRemove 容器由 Docker 自动删除。归属不明或宿主机进程只报告。停止后最多复查 5 次，仍忙返回失败。宿主机需 Bash、SSH、timeout、Hy-smi 和 Docker 权限；无需指定容器/镜像。支持 --concurrency（32）、--timeout（300，最少40秒）、-o 输出父目录、--dry-run（仅预览）。结果目录包含逐节点原始日志、退出码和 results.tsv；0=全部空闲，1=忙/检测或释放失败，2=参数错误，130=取消。此命令独立于既有测试预检，测试预检原阈值保持不变。
