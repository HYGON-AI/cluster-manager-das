# hcu-cluster-run

`hcu-cluster-run` 是本工程唯一对外运行入口，用于 Slurm 通过 `sbatch` 分配出的裸金属计算节点集群。工具不读取或控制 Slurm；用户将分配到的节点整理为 hostfile 后执行检测。

FTP 上传后若提示 `Permission denied`，在服务器执行 `chmod 755 bin/hcu-cluster-run`（无需 `777`）。若提示 `getcwd() failed`，说明当前 Shell 所在目录已失效，先 `cd` 到新上传的工程目录再运行；入口也会尝试切换到自身工程根目录。相对 `-f hostfile` 以实际工作目录为准，推荐传入绝对路径。

## 1. 工程分层

```text
入口层      bin/hcu-cluster-run                 Bash，定位工程、选择控制端环境及 Python
执行层      cluster_run/                         Python 编排；分组、并发、env.sh、launcher、结果落盘
检测层      hcu_envcheck/                       Python 节点探针、硬件/网络/RDMA/驱动/软件检测
主动载荷    cluster_run/payloads/                Shell 或 Python 测试程序
报告层      cluster_run/consistency.py           Python 汇总节点一致性和差异报告
```

入口节点只负责编排和报告；基础检测通过 SSH/clush 在目标计算节点执行。多节点 RCCL 二进制测试和 RCCL/GEMM worker 可采用组首 MPI；单节点使用本机启动，普通脚本每节点独立执行，IB 带宽使用 server/client 配对。执行与扩展规范见 [AGENTS.md](AGENTS.md)。

Shell 帮助/版本不依赖 Python；实际编排和报告仍需要**入口节点 Python >= 3.10**（标准库）。入口没有现成 Python 时可使用 `--controller-env-script /share/controller-env.sh --controller-python python3`，此脚本只在入口加载。`--env-script` 为可选参数；传入时只在计算节点或指定容器加载，省略时直接使用目标当前环境。基础探针使用 `--remote-python`，测试使用 `--test-python`，均从加载后的目标环境解析，不能混用控制端解释器。

## 2. 三种环境场景

| 场景 | 含义 | 基础检测执行位置 | 主动测试执行位置 |
|---|---|---|---|
| `shared-conda` | 所有计算节点使用共享 Conda 环境 | 宿主机检测；传入 env.sh 时先 source | 组首宿主机启动；传入 env.sh 时先 source |
| `node-local-conda` | 每个计算节点使用本地 Conda 环境 | 每节点检测；传入 env.sh 时 source 本地环境 | 组内每节点启动；传入 env.sh 时 source 本地环境 |
| `per-node-container` | 每节点已有一个容器 | `docker exec` 容器内检测；env.sh 可选 | 组首容器内启动，组内容器执行测试；env.sh 可选 |

`--env-script` 可选。需要 module、DTK、Conda 或额外环境变量初始化时，传入目标节点/容器可见的 `env.sh`；目标环境本身已经配置完整时可以省略。工具将其视为可执行环境初始化脚本，不解析其中的元数据，也不要求声明期望版本。脚本可以包含：

```bash
module load dtk/xxx
source /opt/dtk/env.sh
export LD_LIBRARY_PATH=...
source /opt/miniconda/etc/profile.d/conda.sh
conda activate train
```

传入时，它必须在目标节点/目标容器中可见，基础检测和主动测试都会在真正执行命令前 source；省略时不猜测脚本路径，也不额外修改目标环境。采集到的驱动、DTK、Python、Torch、RCCL、UCX 等实际版本写入报告，用户据此确认。

## 3. 统一命令

```bash
hcu-cluster-run <场景> <操作> -f HOSTFILE [操作专属选项]
```

远程传输默认使用逐节点 `ssh`；只有显式传入 `--transport clush` 时才使用 clush。日志写入标准错误，交互终端按 `INFO/SUCCESS/WARN/ERROR` 使用不同颜色，重定向时自动关闭颜色。`--log-detail` 控制前台粒度（面向千/万卡规模）：`milestone`（默认）按「状态×原因码」折叠同类节点为单行、进度按里程碑/单行刷新显示；`every` 逐节点/逐组输出完整明细；`quiet` 只保留 ERROR 级折叠摘要。报告 JSON/Markdown 内容不受该参数影响。

失败日志会包含本地路径/阶段、异常类型、节点返回码、原因码计数及证据位置；默认（milestone）按原因折叠节点列表（如 `BLOCKED ×53 nodes=r01n[02-45] findings=...`），`--log-detail every` 恢复逐节点的远端 stderr 摘要与证据文件路径；完整日志始终保留在报告的 `evidence/` 或主动测试组的 `groups/` 下。设置 `HCU_ENVCHECK_DEBUG=1` 可输出 Python traceback。

基础检测类别：

```text
platform          驱动、软件版本、网络配置
resource          内存、显卡、HCU/DCU 资源
platform,resource 两类都执行，结束后自动生成一致性报告
```

`platform` 只按驱动、软件、网络等平台检查项判定，设备采样仅用于识别硬件，不将瞬时显存占用判为平台失败；`resource` 不执行软件/网络探针，只按资源证据判定。两类同时执行时分别采集后合并节点状态。

示例：

```bash
./bin/hcu-cluster-run shared-conda platform,resource \
  -f examples/baremetal-nodes.txt \
  --env-script /share/envs/train-env.sh \
  --transport ssh --expected-devices 8
```

容器场景：

```bash
./bin/hcu-cluster-run per-node-container platform,resource \
  -f nodes.txt --env-script /workspace/env.sh \
  --container hcu-train -i "$IMAGE"
```

基础报告按节点输出明细，并汇总：相同配置节点数、通过节点数、失败节点数、不完整节点数、失败节点及差异字段。当前不设置全局门禁，用户可根据报告继续选择具体检测。

**万卡级是工具设计的处理规模，不是每次运行的验收目标。** 本次报告只统计 hostfile 中实际检测的节点和卡数，例如 4 个八卡节点为 32 卡；默认不输出“目标=10000”或对 10000 卡的覆盖率。单机预期八卡时可显式传 `--expected-devices 8`，该参数检查每节点数量，不表示集群目标规模。

`cluster-result.json` 使用 `schema_version: 2.0` 的分类结构：`run` 保存本次场景/策略，`node_status`、`hardware_devices`、`system`、`driver_dtk`、`software_components`、`network_rdma`、`resource_state`、`network_health`、`execution_evidence` 各自包含 `nodes`，`cluster` 保存集群汇总与一致性差异。每类独立折叠相同节点，折叠项的 `members` 列出真实节点名；设备相同值按 `device_ids` 折叠，动态资源、网络采样与证据仍可按节点追溯。`cluster-summary.md` 优先展示逐节点显卡使用/异常，再展示静态配置差异、网络健康和执行证据。

## 4. 主动测试

所有运行的产物目录统一命名为 `<场景>_<测试项>_<时间戳>`（场景标签 `container`/`conda`/`lconda`；同秒重跑追加 `_1`），如 `verify_results/container_rccl_20261009_180118/`；组内布局与逐项排查路径见[使用指南](docs/USER_GUIDE.md#主动测试输出目录与排查)。

```bash
hcu-cluster-run <场景> rccl       -f HOSTFILE --env-script ENV_SH --group-size 8 --slots 2
hcu-cluster-run <场景> gemm       -f HOSTFILE --env-script ENV_SH --group-size 1
hcu-cluster-run <场景> ib-write-bw -f HOSTFILE --env-script ENV_SH --group-size 8
hcu-cluster-run <场景> custom     -f HOSTFILE --env-script ENV_SH --script /share/tests/my_test.sh
```

- `--group-size`：每个主动测试组包含的节点数；工具自动切分公共 hostfile。
- `--slots`：同时运行的测试组数量，不是 MPI slots。
- RCCL/GEMM worker 的 `--launcher` 默认 `mpirun-torchrun`，也支持 `ssh-torchrun` 和 `mpirun`；单节点组不使用 MPI。`script/custom/ib-write-bw` 不接受该参数。
- `--nproc-per-node`：`rccl-tests` 每节点 slots，默认 **8**；worker 每节点测试进程数，默认 1，适用于 torchrun 和直接 MPI；`rocblas` 不接受该参数（通过脚本参数选择卡）。
- `--np`：仅 worker 的 MPI 进程数；`mpirun-torchrun` 默认等于组内节点数。RCCL 二进制测试自动按组内节点数 × 每节点 slots 计算，不用另传 `--np`。

`env.sh` 在每个执行节点以 `set -e` 加载；任一未处理命令失败会停止该节点的探针或测试。`--skip-idle-check` 只跳过空闲检查，仍验证 `env.sh`。主动测试在远端使用 `timeout` 控制进程组；未指定 `--timeout` 时最长约 59 分钟。使用 `ssh-torchrun` 时，`--concurrency` 至少覆盖“组内节点数 × 同时运行的组数”，否则工具会在启动前拒绝，以免部分 rank 被排队造成死锁。

容器场景新增独立只读检查：`hcu-cluster-run per-node-container container-status -f hostfile --container zy-bridge2 -i "$IMAGE"`，不需要 `--env-script`。它核验容器精确名称、运行状态、镜像标签和跨节点镜像 ID；不检查 DCU 空闲、不 pull 或重建，失败原因仅在终端按节点组输出。`platform/resource` 在容器内检测前复用此检查：异常节点记入基础报告且跳过环境探针，健康节点继续检测；资源占用不会阻止 `resource` 检测。容器场景的基础/主动检测与 `script` 均必填 `-i/--image`，预检核验容器实际镜像与指定镜像一致；仅 `container-delete` 不接受该参数。

容器场景的主动测试先检查容器状态，再在已有容器中 source `env.sh` 并采样 HCU/DCU 空闲状态、核验 MPI 用户。任一节点缺容器、镜像不一致、显卡忙或证据不足，终端按相同原因合并节点输出 `PRECHECK_FAILED`，不生成 `preflight.json`、`active-result.json` 或主动测试运行目录，也不会启动 RCCL/GEMM。运行中的容器即使缺少本地镜像标签，也不报 `IMAGE_NOT_LOCAL`；本地镜像只在创建/重建时需要。`--dry-run` 仍只生成计划，不访问节点。预检不会自动重建容器；若用户的 `env.sh` 自身含文件操作，它仍会按脚本内容执行。

容器内使用 MPI 的 RCCL/GEMM 测试由统一入口自动应用 `--allow-run-as-root`，预检与实际执行保持一致，不再提供额外的 root MPI 命令行开关；宿主机 MPI 不受该策略影响。主动测试默认检查 DCU 空闲；`--skip-idle-check` 只能由用户显式选择，不推荐在有业务负载的节点使用。容器缺失/镜像不一致时会给出新版 `container-recreate` 模板，但检测命令绝不会自动执行重建。

Python worker 启动语义（RCCL 须显式 `--profile worker`，GEMM 默认 worker）：

```text
mpirun-torchrun：组首按需 source env.sh -> mpirun -> 组内每节点按需 source env.sh -> torchrun -> Python 测试程序
ssh-torchrun：入口节点并行 SSH 到组内每节点 -> 按需 source env.sh -> torchrun
mpirun：组首按需 source env.sh -> mpirun -> 测试程序从 OMPI/PMI/PMIX 环境变量读取 rank
```

**`rccl` 默认执行现有 `rccl_perf_test.sh`（profile=rccl-tests），多节点由脚本直接 MPI 启动 `*_perf -g 1`，不套 torchrun；单节点直接 `*_perf -g <卡数>`。** 默认一次运行图示全部 10 项 collective，并逐项验收性能。显式 `--tests` 仅用于保留单项/子集诊断能力，不能将子集结果称为完整验收。Python smoke test 仍可用 `rccl --profile worker` 选择，少于两个 rank 返回 `INCOMPLETE`。GEMM 默认 worker，完整 rocBLAS 各卡/形状基准使用 `gemm --profile rocblas`。worker profile 的 `--script` 专用于不包含嵌套 launcher 的每 rank 程序；任意普通脚本使用 `script/custom`，不根据文件名猜测启动方式。`platform` 默认包含每节点 IB/RDMA 设备、端口、链路及状态检查，不产生带宽流量。`ib-write-bw` 继续执行真实 server/client 配对主动测试，保留逐 HCA/方向、阈值、最大测试数与原始证据，不使用 MPI；已有 NHC 检测用 `nhc`。

RCCL 两节点、每节点八卡的完整十项验收（hostfile 中仅放本次要测的节点）：

```bash
./bin/hcu-cluster-run per-node-container rccl \
  -f hostfile --env-script /share/env.sh --container zy-bridge2 \
  --group-size 2 --nproc-per-node 8 --container-ssh-port 25901 \
  --script-arg=--baseline-file --script-arg=/share/rccl_baseline.conf
```

保留原命令的通信参数默认值和 `-g 1 -b 4 -e 1G -f 2 -n 20 -w 5`；具体节点与 `-np` 从分组生成，`env.sh` 可覆盖站点通信配置，详见 [RCCL 参数说明](docs/USER_GUIDE.md#rccl-二进制测试默认路径)。性能基准**必须存在且覆盖当前 np 的全部待测项**，可用上述参数指定，或设置目标 `RCCL_BASELINE_FILE`，也可放入工程 `cluster_run/baselines/rccl_baseline.conf`。缺少对应规模/项目时执行前明确报错，不借用其他规模、不静默跳项。正常启动后单项失败仍继续剩余项（中断除外）。每组 `rccl/rccl-summary.md`、`rccl-summary.tsv` 展示 out/in-place algbw/busbw 四列实测峰值、基准、容差、偏差和结果；原始日志及完整 `*.command.sh` 同目录保存。比较规则保持原脚本的 busbw 峰值 ≥ 基准 × (1-margin%)；四列为各尺寸独立峰值，不声称是同一尺寸的一行数据。顶层 `--dry-run` 仅构造计划，不代表实际 MPI 或性能验收通过。

容器多节点 MPI 使用 `--container-ssh-port 25901`（可修改），生成 `--mca plm_rsh_args "-p 25901"`。预检比较这一路径与 `docker exec` 的命名空间和用户，错误端口不能当作容器执行。`--container-workdir` 同时用于环境预检、基础检测、脚本、组首和 MPI rank；env.sh 不必定义 rank/size，本次拓扑在 source 后由 launcher 覆盖。项目及分组输出目录必须位于计算节点/容器可见共享路径。

创建/重建时可显式 `--port 25901` 自动配置容器间 root 公钥免密，使用 host 网络并检查端口冲突；不传则不配置 SSH。例：`hcu-cluster-run per-node-container container-create -f hostfile --container worker -i "$IMAGE" --port 25901 -v /share:/share`。在配置好的容器内可直接 `ssh node -p 25901`；后续 MPI 使用同值 `--container-ssh-port 25901`。详细依赖、安全边界和重建说明见 [容器维护指南](docs/USER_GUIDE.md#6-容器维护命令)。

`Ctrl+C/SIGTERM` 会停止派发，按本次 token 并发清理所有计划节点并验证。终端默认汇总显示 `cleanup confirmed N/M`，未确认节点折叠列出并保留 `UNCONFIRMED`；报告 JSON 中始终逐节点记录 `CONFIRMED/UNCONFIRMED`（`--log-detail every` 时终端也逐节点显示）。无法联系节点时返回 `CLEANUP_UNCONFIRMED`，不会谎报清理成功。不按进程名批量杀进程。远端需要 Bash 4+、Linux `/proc`、`setsid`、`flock`、`timeout`。控制端 SIGKILL/断电不在可确认清理范围内，远程超时提供兜底。基础环境不健康仍是有效检测结果；主动程序执行失败返回非零，但不阻止用户单独执行下一条命令。

```bash
# NHC 与真正的 IB 配对带宽（不是 MPI）
./bin/hcu-cluster-run shared-conda nhc -f hostfile --env-script /share/env.sh --nhc-command run_nhc
./bin/hcu-cluster-run per-node-container ib-write-bw -f hostfile --container zy-bridge2 \
  --env-script /share/env.sh --group-size 2 --slots 1 --ib-iterations 100 --ib-max-tests 64
# 显式 RoCE 策略是检测策略 JSON，不替代环境初始化 env.sh
./bin/hcu-cluster-run shared-conda platform -f hostfile --env-script /share/env.sh \
  --rdma-policy-file /share/roce-policy.json --rdma-counter-interval 1
```

## 5. 新版逐节点脚本和容器维护

`script` 在 hostfile 的每个节点独立执行，不分组、不启动 MPI/torchrun。传入 `--env-script` 时先在目标宿主机或容器内 source，省略时直接使用当前目标环境；然后用 `bash` 执行 `.sh`、用激活后的 `python3` 执行 `.py`。`--script` 必须是目标执行环境可见的**绝对路径**；报告是 `script-result.json` 和逐节点 stdout/stderr。

```bash
./bin/hcu-cluster-run per-node-container script \
  -f hostfile --container zy-bridge2 \
  --env-script <prefix>/env/env.sh \
  --script <prefix>/cluster-manager-das/hcu-envcheck/cluster_run/payloads/check_deepep_env.sh
```

容器维护操作和检测/测试同级，运行在各节点**宿主机**，不 source `env.sh`：

```bash
./bin/hcu-cluster-run per-node-container container-status   -f hostfile --container worker -i "$IMAGE"
./bin/hcu-cluster-run per-node-container container-create   -f hostfile --container worker -i "$IMAGE" -v /share:/share
./bin/hcu-cluster-run per-node-container container-recreate -f hostfile --container worker -i "$IMAGE" -v /share:/share --yes
./bin/hcu-cluster-run per-node-container container-delete   -f hostfile --container worker --yes
```

创建遇到同名容器失败；重建/删除必须明确传入 `--yes`。`--dry-run` 不接触节点。创建/重建先在**所有节点**确认目标镜像可用，失败则不删除任何旧容器；先查本地镜像，缺失时 pull，节点离线可传 `--image-tar /share/image.tar`（tar 必须对每个节点可见）。重建不是跨节点原子事务，部分 Docker run 仍可能失败。原容器的挂载、网络、用户、启动命令不会自动继承，必须显式补齐 `-v`、`--docker-arg=--network=host`、`--container-command` 等参数。检测命令永远不会自动维护容器。

- 原有 HCU、驱动、DTK、内存、显卡、网络、RDMA、Conda、Docker、NHC、RCCL、GEMM、IB 带宽等检测实现继续保留。
- 旧版无场景 `-f/-g/-s` 调用及 `baremetal-cluster` 子命令不再支持；对外只有 `hcu-cluster-run <场景> <操作> ...`。
- 发布包和安装器只创建 `hcu-cluster-run` 一个命令链接。

## 7. 服务器手工验证总入口

测试总入口为 `scripts/test.sh`。脚本不再封装公共执行函数，每个用例直接展开一条 `hcu-cluster-run` 命令，便于服务器上复制和修改。它用于确认代码接口能被调用、能生成报告或主动测试启动计划；报告中的环境 `PASS/FAIL/INCOMPLETE` 不作为脚本门禁。

```bash
bash scripts/test.sh --list

HOSTFILE=/share/nodes.txt ENV_SCRIPT=/share/train/env.sh \
  CONTAINER_NAME=hcu-worker IMAGE=image:tag bash scripts/test.sh
```

用例含义：

| 用例 | 验证内容 |
|---|---|
| `help` | 唯一入口帮助、版本和参数解析 |
| `platform` | 驱动、软件版本、网络/RDMA 静态配置 |
| `resource` | 内存、显卡/HCU、显存和资源空闲状态 |
| `base` | `platform,resource` 合并检测及节点一致性差异报告 |
| `rccl` / `gemm` | 显式 worker smoke test，以及 RCCL 默认二进制 / 显式 rocBLAS 基准；单节点本机启动，多节点 worker 按 launcher 执行 |
| `ib-write-bw` | 真实执行时按 HCA/方向进行 server/client 配对带宽测试；dry-run 仅验证清单发现命令和配对配置，不产生流量；不接受 `--script` 或 launcher |
| `platform` 内置 IB 状态 | 每节点检查 IB/RDMA 设备、端口、链路和状态；不产生带宽流量 |
| `nhc` | 每节点 NHC 检测 |
| `script` / `custom` | 自定义 Shell/Python 每节点普通执行；custom 可分组整理结果，均不启动 MPI/torchrun |
| `context-sh` / `context-py` | 三种场景的 Shell/Python 运行上下文探针，验证目标 env.sh 标记及执行位置，不使用 DCU |
| `local` | 本地 Shell/Python 回归，不访问计算节点 |

一次执行三种场景的接口矩阵及本地单测，不接受按用例筛选。基础检测和容器状态检查默认真实执行；下列类别默认 dry-run，分别设置对应变量为 `yes` 才真实执行，互不替代：

| 变量 | 开启的真实执行 |
|---|---|
| `RUN_ACTIVE=yes` | RCCL/GEMM 显式 worker 的短小测试 |
| `RUN_PROFILES=yes` | RCCL 默认二进制 / 显式 `rocblas` 基准用例 |
| `RUN_NETWORK=yes` | IB server/client 带宽测试 |
| `RUN_DIAGNOSTICS=yes` | `nhc` 检测；IB 状态已由真实执行的 `platform` 覆盖 |
| `RUN_SCRIPTS=yes` | `DIAGNOSTIC_SCRIPT` / `CUSTOM_SCRIPT` 指定的用户脚本，运行前需确认脚本内容 |
| `RUN_CONTEXT=yes` | 三种场景的 Shell/Python 上下文探针，不使用 DCU |

容器维护和独立 launcher 构造用例始终只做 dry-run。真实上下文探针需要当次输出目录下的 `contexts/` 输入在所有目标节点/容器中同路径可见；它仍会 source 用户的 env.sh，并不保证该脚本本身无副作用。仅设置 `RUN_CONTEXT=yes` 不代表完成 RCCL/GEMM、MPI 取消或 IB 带宽实测。

逐项退出码记录在 `calls.tsv`，验收汇总为 `acceptance-result.json`，包含当次报告路径、节点范围和执行状态。`PLANNED_ONLY` 只表示 dry-run 计划验证，`BLOCKED_NOT_EXECUTED` 表示被拦截、没有执行；不能将二者计为实测通过。环境异常不等于工具异常；缺少必要报告/启动产物、接口异常或单测失败会使总入口返回非零。

常用变量：`HOSTFILE`、`ENV_SCRIPT`、`CONTAINER_NAME`、`IMAGE`、`GROUP_SIZE`、`ACTIVE_SLOTS`、`OUTPUT_DIR`、`CUSTOM_SCRIPT`、`DIAGNOSTIC_SCRIPT`。

## 8. 开发和测试

```bash
python -m unittest discover -s tests -q
```

详细说明见 [docs/USER_GUIDE.md](docs/USER_GUIDE.md) 和 [cluster_run/GUIDE.md](cluster_run/GUIDE.md)。

RCCL 二进制验收用例在 `scripts/test.sh` 中默认每节点 8 卡（`RCCL_NPROC_PER_NODE` 可覆盖），不与 Python worker 的 `NPROC_PER_NODE`（默认 1）混用。真实执行需有该规模十项完整基准；测试脚本使用短小数据量只验证功能，性能不达标是有效检测结果，不代表代码异常。
