# hcu-cluster-run 使用指南

## 1. 前置条件

直接通过 FTP 上传工程时，文件的 Linux 可执行权限可能丢失。首次运行前可执行 `chmod 755 /path/to/hcu-envcheck/bin/hcu-cluster-run`（不要使用 `chmod 777`）。如果上传过程替换了当前所在目录，先 `cd /path/to/hcu-envcheck`；否则 Shell 会报告 `getcwd() failed`，Python 可能在读取当前目录时抛出 `FileNotFoundError`。新版入口会在检测到已失效的工作目录时切换到工程根目录并提示。建议使用绝对路径传入 `-f`、`--env-script` 和 `-o`，避免依赖当前目录。

1. 使用 `sbatch` 获取裸金属节点。工具本身只接收 hostfile，不解析 Slurm Job ID。
2. 入口节点可通过 SSH 或 clush 访问计算节点。
3. 如目标环境需要初始化，准备所有目标节点可见的 `env.sh`；目标环境已配置完整时可省略 `--env-script`。
4. 主动测试时，组 hostfile 和输出目录应位于组首节点可见的共享目录；容器场景下 hostfile 也必须在容器内可见。

`--transport` 不传时默认使用 `ssh`。需要 clush 时显式传入 `--transport clush`；工具兼容不支持 `--outdir/--errdir` 的旧版 ClusterShell，并从带节点前缀的输出中拆分各节点证据。

失败排查：入口日志会标出场景、检测项、hostfile、输出目录、异常类型和缺失路径；节点失败默认按「状态×原因码」折叠为单行（含返回码信息与折叠节点列表，如 `BLOCKED ×3 nodes=r01n[02-04] findings=DTK_VERSION_UNAVAILABLE`），`--log-detail every` 恢复逐节点的返回码、错误类型、原因码计数、远端 `stderr` 摘要与证据文件路径。原始远端日志始终位于报告对应的 `evidence/.../nodes/<节点>/stderr.txt`，传输元数据位于同目录 `result.json`；主动测试失败时查看 `groups/group-NNN/stderr.log` 或 `groups/group-NNN/nodes/<节点>/stderr.log`。需要 Python 调用栈时，在命令前加 `HCU_ENVCHECK_DEBUG=1`。默认只在终端输出有界摘要，不打印完整 `env.sh` 或探测命令。

## 2. 可选 env.sh 规则

脚本只做环境初始化，不写期望版本元数据：

```bash
module load dtk/26.04
source /opt/dtk/env.sh
export UCX_NET_DEVICES=...
source /opt/miniconda/etc/profile.d/conda.sh
conda activate train
```

- `shared-conda`：脚本指向共享环境。
- `node-local-conda`：脚本可以是共享路径下的统一入口，但内部选择/激活每节点本地环境。
- `per-node-container`：路径按容器内部路径填写。
- 不要在脚本中加入 `HCU_EXPECTED_*` 等工具元数据；实际版本、环境变量和路径由报告采集。

## 3. 基础检测

```bash
./bin/hcu-cluster-run shared-conda platform \
  -f nodes.txt --env-script /share/train/env.sh \
  --transport ssh --concurrency 32

./bin/hcu-cluster-run node-local-conda resource \
  -f nodes.txt --env-script /share/tools/node-env.sh \
  --transport ssh --expected-devices 8

./bin/hcu-cluster-run shared-conda platform,resource \
  -f nodes.txt --env-script /share/train/env.sh
```

### 3.1 容器场景基础检测

容器场景可以先独立检查全节点容器状态；该命令在宿主机执行，不进入容器，也不需要 `--env-script`：

```bash
./bin/hcu-cluster-run per-node-container container-status \
  -f nodes.txt --container hcu-worker -i "$IMAGE" --transport ssh
```

随后在容器内执行基础检测：

```bash
./bin/hcu-cluster-run per-node-container platform \
  -f nodes.txt --container hcu-worker -i "$IMAGE" \
  --env-script /share/train/env.sh --transport ssh

./bin/hcu-cluster-run per-node-container resource \
  -f nodes.txt --container hcu-worker -i "$IMAGE" \
  --env-script /share/train/env.sh --expected-devices 8

./bin/hcu-cluster-run per-node-container platform,resource \
  -f nodes.txt --container hcu-worker -i "$IMAGE" \
  --env-script /share/train/env.sh
```

`platform`、`resource` 和 `platform,resource` 在进入容器执行探针前，会自动在宿主机复用容器状态检查，核验精确容器名、运行状态和跨节点镜像 ID，并按必填的 `-i/--image` 核验容器实际镜像与指定镜像一致。容器异常节点会保留在 `node_status.nodes` 与 `execution_evidence.nodes` 中并跳过容器内探针，其他健康节点继续检测。DCU/HCU 被占用属于 `resource_state` 检测结果，不会被误判为容器状态异常。

`container-status` 是可单独运行的只读检查，要求同时提供 `--container` 和 `-i/--image`；它不 source `env.sh`、不检查设备空闲、不 pull 镜像，也不会创建或重建容器。容器维护仍必须显式使用 `container-create`、`container-recreate` 或 `container-delete`。

`platform` 包括驱动、DTK/软件版本、网络配置和 RDMA 相关静态证据；`resource` 包括内存、显卡/HCU 数量、显存使用率和资源空闲度。每节点通过独立探针完成采集，入口节点负责解析、落盘和一致性聚合。

报告主要文件：

```text
cluster_run_results/                      # -o 未指定时的默认输出根
└── <场景>_<检测项>_<时间戳>/              # 如 container_platform_resource_20261009_153000
    ├── cluster-result.json               # 同秒重跑自动追加 _1、_2
    ├── cluster-summary.md
    └── evidence/
```

场景标签：`container`（per-node-container）、`conda`（shared-conda）、`lconda`（node-local-conda）；操作名中的 `,`/`-` 转为 `_`（如 `ib-write-bw`→`ib_write_bw`）。主动测试与 script 同规则（`container_rccl_.../groups/`、`lconda_script_.../`）。

一致性报告不再作为独立命令。执行 `platform,resource` 后自动追加，展示相同配置节点数、通过/失败/不完整节点数，以及差异字段和 finding。

工具按万卡级集群规模设计，但这不是单次检测的目标。报告以本次 hostfile 为范围，展示实测节点数、识别 HCU 数和单节点卡数分布；统一入口不设置 10000 卡目标，也不计算“已检测卡数/10000”的覆盖率。八卡节点可通过 `--expected-devices 8` 检查每节点数量。静态采集不等于万卡训练或通信实测。

`cluster-result.json` 的顶层依次为 `schema_version`、`run`、`node_status`、`hardware_devices`、`system`、`driver_dtk`、`software_components`、`network_rdma`、`resource_state`、`network_health`、`execution_evidence`、`cluster`。各检测类通过 `.nodes` 独立折叠相同值，`members` 始终列出真实节点；设备公共属性用 `device_ids` 折叠，设备身份保留 ID 映射。`network_health.samples_by_node` 保存原始动态采样；`run.probe_command` 保存各节点共同的探测命令；`cluster` 保存集群汇总和一致性差异。`cluster-summary.md` 将设备占用与 WARN/FAIL、静态配置差异和执行证据分开展示；`resource-only` 同样按已采集的静态配置分组，利用率/显存使用量不进入配置签名；缺失证据不视为配置相同。空值代表未采集/未要求，不自动判为失败。

## 4. 主动测试

主动测试按组执行，不与基础检测混用。

`per-node-container` 提供独立容器状态命令，`--container` 和 `-i` 均必填，**不需要 `--env-script`**：

```bash
./bin/hcu-cluster-run per-node-container container-status \
  -f hostfile --container zy-bridge2 -i "$IMAGE" --transport ssh
```

它只在宿主机核验精确容器名、运行状态、实际镜像标签及跨节点镜像 ID，不进入容器、不检查显卡空闲、不 pull，也不生成 `preflight.json`。失败时终端按相同原因合并节点输出。`platform/resource` 的容器内检测会先复用此检查，异常节点保留在基础报告中并跳过环境探针，健康节点仍继续；DCU 忙不是容器状态错误，`resource` 仍采集占用信息。容器场景的基础检测、主动测试与 `script` 的 `-i/--image` 均为必填，预检按其核验用户期望镜像。

`per-node-container` 的真实主动测试先执行该容器状态检查（`--dry-run` 除外），再在可用容器中按需 source `env.sh`、采集显卡状态并检查 MPI 用户。预检失败时终端按原因合并节点并返回 `PRECHECK_FAILED`，不生成 `preflight.json`、`active-result.json` 或主动测试运行目录，任务不会启动。已存在容器即使标签相同，镜像 ID 不同也算不一致；运行中容器不要求节点本地保留镜像标签。预检本身不重建容器，但 `env.sh` 中的操作仍会执行。

容器内使用 MPI 的 RCCL/GEMM 测试由统一入口自动应用 `--allow-run-as-root`，预检与实际执行保持一致，不再提供额外的 root MPI 命令行开关。该内部策略不绕过容器、镜像、实际 SSH 身份或显卡空闲检查，也不会扩大到宿主机 MPI。

全节点维护使用同级 `container-create`、`container-recreate`、`container-delete` 操作，检测命令绝不会代替用户执行。重建/删除必须加 `--yes`，执行前确认业务进程、数据和原容器挂载/启动参数。`-i` 是目标镜像名，不是 tar 路径；创建/重建先在所有节点获取目标镜像，本地不存在则尝试 `docker pull`。pull 失败时提示用 `--image-tar <共享镜像.tar>`。镜像获取阶段任一节点失败，不删除旧容器；后续 Docker run 不是多节点原子事务。

```bash
./bin/hcu-cluster-run shared-conda rccl \
  -f nodes.txt --env-script /share/train/env.sh \
  --group-size 8 --slots 2 --nproc-per-node 8

./bin/hcu-cluster-run per-node-container gemm \
  -f nodes.txt --env-script /workspace/env.sh \
  --container hcu-train --group-size 1

./bin/hcu-cluster-run shared-conda custom \
  -f nodes.txt --env-script /share/train/env.sh \
  --group-size 8 --script /share/tests/my_test.py
```

### RCCL 二进制测试（默认路径）

`rccl` 默认 `--profile rccl-tests`，调用已有 `cluster_run/payloads/rccl_perf_test.sh`。容器执行链为：入口 SSH 到组首宿主机 → `docker exec` 指定容器 → source env.sh → mpirun → 通过容器 SSH 端口进入各节点容器 → source env.sh → RCCL 二进制。**没有 torchrun，没有第二层 MPI。** 目标执行载荷是 Bash + MPI + RCCL 二进制，不需要 Torch；现有容器资源预检仍需要目标 Python。

```bash
# hostfile 若只有 m09r2n10、m09r2n11 两行，则生成 --host m09r2n10:8,m09r2n11:8 -np 16
./bin/hcu-cluster-run per-node-container rccl \
  -f hostfile --env-script /share/env.sh --container zy-bridge2 \
  --group-size 2 --nproc-per-node 8 --container-ssh-port 25901 \
  --script-arg=--baseline-file --script-arg=/share/rccl_baseline.conf
```

保留 `--allow-run-as-root`、`--mca plm_rsh_args "-p 25901"`、`--bind-to none`，多节点每 rank `*_perf -g 1 -b 4 -e 1G -f 2 -n 20 -w 5`。节点不硬编码；`--group-size` 切分节点，`--slots` 控制并发组数，`--nproc-per-node` 配置每节点 MPI slots（默认 8），组内 `-np` 自动求和。单节点组不启动 MPI，直接 `*_perf -g <卡数>`。可显式 `--launcher mpirun`；选择 torchrun 必须加 `--profile worker`，不能把二进制脚本嵌套到 torchrun 中。

通信配置的取值优先级（高 -> 低）：显式脚本参数 > 已加载的 `env.sh` 导出的变量 > `cluster_run/cluster_env.conf` 站点默认（可用 `CLUSTER_ENV_FILE` 换路径；从 `cluster_env.conf.example` 复制填写）> 下表内置兜底：

| 变量 | 各层均未设置时的兜底值 |
|---|---|
| `LD_LIBRARY_PATH`、`ROCM_PATH` | 有值时经 MPI `-x` 传递，不编造运行时路径 |
| `NCCL_SOCKET_IFNAME` | `eth0` |
| `NCCL_PXN_DISABLE` | `0` |
| `RCCL_PXN_GPU_BALANCE` | `1` |
| `RCCL_NET_PLANE` | 仅传递实际配置值，不编造平面配置 |
| `NCCL_NET_PLUGIN` | `shca` |
| `NCCL_PLUGIN_P2P` | `ib` |
| `NCCL_NET_GDR_LEVEL` | `4` |
| `NCCL_NET_GDR_READ` | `1` |
| `NCCL_TOPO_FILE` | `/usr/local/built-in-508-topo-input-tj-default.xml` |
| `UCX_NET_DEVICES` | `ib0`，在二进制运行前设置 |

显式脚本参数优先于环境：例如 `--script-arg=--iface --script-arg=eth1`、`--script-arg=--ucx --script-arg=ib1`、`--script-arg=--topo --script-arg=/share/topo.xml`。`RCCL_IFACE/RCCL_UCX_DEV/RCCL_TOPO_FILE` 是 conf 中的站点配置名（也可作环境变量）；`env.sh` 导出的 `NCCL_SOCKET_IFNAME/UCX_NET_DEVICES/NCCL_TOPO_FILE` 实际运行变量优先于它们。组首选择的通信参数会在各 rank source 后统一应用，避免 rank 初始化覆盖显式参数；DTK/二进制路径仍在每节点加载后解析。

可执行文件优先 `--bin-dir`、`RCCL_BIN_DIR/RCCL_TESTS_BIN_DIR`、PATH 和所选运行时根目录，最后保留原安装位置 `/opt/rccl-test/build` 作为兜底。需要严格使用原路径时加 `--script-arg=--bin-dir --script-arg=/opt/rccl-test/build`。这些默认值针对原站点，不保证适用于其他网卡、插件或拓扑；应检查日志中的实际配置。

默认一次运行 `all_reduce/all_gather/broadcast/reduce/reduce_scatter/gather/scatter/alltoall/alltoallv/sendrecv` 十项，上例执行全部十项。保留显式 `--script-arg=--tests --script-arg=broadcast` 等子集诊断，报告标明范围，不冒充完整验收。其他二进制选项用逐项 `--script-arg=VALUE` 传递。

性能基准必须覆盖当前规模的全部待测项。优先使用显式 `RCCL_BASELINE_B64/RCCL_BASELINE_TEXT` 内容，其次 `--baseline-file`，再读取 `RCCL_BASELINE/RCCL_BASELINE_FILE`；未显式配置时查找工作目录、载荷同目录、工程 `cluster_run/baselines/` 的 `rccl_baseline.conf`。显式文件不存在报错，不偷偷换基准。仍使用原格式 `<np> <test> <busbw> [margin]`：按本组实际卡数精确匹配，不能将 8/16 卡数据套用到 32 卡；十项缺任何一项在启动前报错，不能缩成少数项目或降级成功。基准必须大于零，margin 范围 [0,100)，重复条目报错。

正常启动后，单项程序失败、带宽无法解析或性能低于下限均记 FAIL，并继续后续项目；Ctrl+C/SIGTERM 取消及其清理仍优先。性能判据保持原脚本：out/in-place busbw 的最大峰值 ≥ 基准 × (1-margin%)，默认 margin=1，可由每项第四列覆盖。只有全部待测项性能达标才返回成功；配置错误退出 1，存在失败项退出 2。

每组 `groups/group-NNN/rccl/` 保存原始 `*.log`、完整 `*.command.sh`、`rccl-summary.md` 和 `rccl-summary.tsv`。汇总列出图示 out/in-place algbw/busbw 四列、busbw 基准、容差、下限、偏差、PASS/FAIL 和原因。四列分别取本次消息尺寸范围的峰值，不保证来自同一尺寸行；现有基准只有 busbw，一列基准不能冒充四列阈值。如果日志仅提供 Avg bus bandwidth，则沿用原回退比较，但缺少的四列显示 `-`，不编造数据。顶层 `--dry-run` 不访问计算节点、不加载远端基准，只验证命令构造，不证明十项实际执行或性能达标。

### Python worker launcher

- `mpirun-torchrun`：默认模式。入口节点 SSH 到组首节点，组首 source env.sh 后执行 mpirun；MPI 每节点启动一个 torchrun，torchrun 再启动本地测试进程。
- `ssh-torchrun`：入口节点并行 SSH 到组内每个节点，各节点 source env.sh 后直接启动 torchrun。
- `mpirun`：入口节点 SSH 到组首节点，组首 source env.sh 后执行 mpirun；测试程序使用 `OMPI_COMM_WORLD_RANK`、`OMPI_COMM_WORLD_SIZE`、`OMPI_COMM_WORLD_LOCAL_RANK`，并兼容 PMI/PMIX 变量。

启动命令由入口 Python 构造，在目标 Shell 中执行；torchrun 使用目标 `--test-python -m torch.distributed.run` 启动，而非入口解释器。上述 MPI 仅适用于多节点 RCCL/GEMM worker，单节点组直接本机启动。容器 MPI 使用 `--container-ssh-port`（默认 25901），启动前核验 SSH 所到命名空间和用户与指定容器一致。普通脚本使用 `script/custom`，不套 MPI/torchrun。

RCCL Python smoke test 用 `rccl --profile worker`；GEMM 默认 `--profile worker`，完整基准用 `gemm --profile rocblas`。worker profile 的 `--script` 指定的是每 rank 程序，不得再启动 MPI。不要用脚本文件名隐式选择执行方式。

入口实际编排/报告需要 Python >= 3.10；`--controller-env-script` 与 `--controller-python` 只配置入口环境。目标 `--env-script` 在每节点加载，不要求任何 rank/size/版本元数据。`--remote-python` 用于基础探针，`--test-python` 用于测试；`--container-workdir` 在目标 source 前生效。项目及报告目录中的组 hostfile 需在各目标节点/容器可见。

### 主动测试输出目录与排查

运行目录名为 `<场景>_<测试项>_<时间戳>`（同秒重跑自动追加 `_1`、`_2`），例如：

```text
verify_results/
└── container_rccl_20261009_180118/
    ├── active-result.json      # 运行总报告：组结果、预检、清理状态、run token
    ├── preflight.json          # 容器预检：容器状态/镜像一致性/SSH 可达/DCU 空闲采样
    └── groups/group-NNN/       # 按 --group-size 切分，每组一个目录
        ├── hostfile            # 该组节点文件，每行 "节点 slots=<--nproc-per-node>"
        ├── launch.json         # 组首启动命令记录（含 token guard 包装）
        ├── result.json         # 组级结果：退出码、状态、耗时
        ├── rccl/               # 十项产物
        │   ├── rccl-summary.md / .tsv      # 四列带宽、基准、容差、偏差、逐项判定
        │   ├── <测试项>.command.sh          # 每项完整 mpirun 命令，可单独复跑
        │   ├── <测试项>.log                 # 每项原始输出（busbw 解析来源）
        │   ├── rank-body.sh                 # rank 执行体（source env.sh → 二进制）
        │   └── task-guard.sh                # token guard（取消/清理凭据）
        ├── nodes/<节点>/stderr.log          # 组首侧 stderr
        └── remote-evidence/<节点>/.../      # 远端执行证据（stderr.txt、传输元数据）
```

`script`、`gemm`、`ib-write-bw`、`custom` 与基础检测使用同一命名规则（如 `lconda_script_...`、`container_platform_resource_...`）。查看最新一次结果：

```bash
R=$(ls -td verify_results/container_rccl_* | head -1)
column -t -s $'\t' "$R/groups/group-000/rccl/rccl-summary.tsv"
```

按症状定位：

| 症状 | 先查看 |
|---|---|
| 整组 FAIL，不知道哪一项 | `rccl/rccl-summary.tsv` 的 reason 列（EXECUTION_FAILED / BELOW_BASELINE） |
| 某项执行失败 | `rccl/<测试项>.log` 尾部，再看 `nodes/<节点>/stderr.log` |
| 性能不达标 | summary 的 peak 与基准列；`*.command.sh` 中核对实际生效的 `-x` 环境变量 |
| 预检拦截（PRECHECK_FAILED） | 终端输出（预检不落盘、不创建组目录）及 `active-result.json` 的 issues |
| 手工复跑某一项 | `bash rccl/<测试项>.command.sh`（完整命令已存证） |

## 5. 场景与逐节点脚本

- Conda 两场景固定在宿主机 source env.sh。
- `per-node-container` 固定在指定的 `--container NAME` 内 source env.sh；不再使用冗余的 `--scope`。
- `script` 操作在每个节点独立运行指定脚本，不分组、不启动 MPI/torchrun，适合 DeepEP 环境诊断等单节点脚本：

```bash
./bin/hcu-cluster-run per-node-container script -f hostfile \
  --container zy-bridge2 \
  --env-script <prefix>/env/env.sh \
  --script <prefix>/cluster-manager-das/hcu-envcheck/cluster_run/payloads/check_deepep_env.sh
```

`--script` 必须是各目标节点或容器可见的绝对路径。`.py` 由目标环境中的 `python3` 执行，其余脚本由 `bash` 执行。`--script-arg` 可重复传入参数，输出为 `script-result.json` 和逐节点 stdout/stderr。

## 6. 容器维护命令

### 创建时配置容器间 SSH 免密

```bash
# 全部节点创建独立 root 公钥 SSH 服务，端口需空闲；无需 env.sh
./bin/hcu-cluster-run per-node-container container-create \
  -f hostfile --container worker -i "$IMAGE" --port 25901 \
  -v /share:/share

# 重建会删除现有容器；确认业务已退出并显式重传设备/挂载等参数
./bin/hcu-cluster-run per-node-container container-recreate \
  -f hostfile --container worker -i "$IMAGE" --port 25901 \
  -v /share:/share --yes

# 在任意上述容器内，以 root 直接进入另一个节点的对应容器
ssh m09r2n09 -p 25901

# 后续多节点 MPI 必须使用相同端口；不会因为 --port 而跳过资源检查
./bin/hcu-cluster-run per-node-container rccl \
  -f hostfile --container worker --env-script /share/env.sh \
  --container-ssh-port 25901 --group-size 2
```

- `--port` 只对 create/recreate 生效，范围 1–65535；不传时不自动配置 SSH。与控制端 SSH 到宿主机的端口、IB/MPI rendezvous 端口无关。`--dry-run` 不创建容器、不检查远端端口，也不能证明免密已建立。
- 此模式自动使用 `--network=host`，容器 sshd 监听节点的指定端口，不需要 Docker `-p` 映射；拒绝 bridge 网络、额外 publish、覆盖 entrypoint/user/PID namespace 等冲突参数。只影响新建容器，不改宿主机 sshd、防火墙或已有容器配置。
- 宿主机需要 `ss` 检查端口；镜像须为 **root 用户、root home=/root** 的 Linux 环境，预装 Bash 4+、OpenSSH client/server、PAM sshd 配置和常用 coreutils，并可读取 `/proc`。容器内无需 `ss` 或 Python。缺失时明确失败，离线节点不自动安装软件。PAM 的账号过期/访问策略仍生效，不修改或解锁 root 密码。`--port` 不适用于非 root 镜像。
- 创建前全节点核验镜像、端口与依赖；端口被占用即失败，只有能证明属于同名、同端口的工具管理容器时才允许重建复用。不能确认归属时选其他端口，不能杀宿主机监听进程。
- 每容器生成独立用户私钥和 host 私钥，**只交换公钥**；固定 `known_hosts` 并严格验签，只允许 root 公钥登录，不开放密码认证。保留镜像原 Entrypoint/Cmd；sshd bootstrap 每次容器启动执行，`docker restart` 后信任和密钥保留，recreate 则生成新密钥并重新配置本次 hostfile 节点集合。勿将管理状态目录打进镜像。
- 配置写到容器内 `/var/lib/hcu-cluster-ssh/`，在 `/root/.ssh/config` 前置当前节点的专用配置并备份原配置。不允许将宿主目录挂载覆盖 `/root`、`/etc`、SSH 状态或运行时目录；业务 `/share`、`/public/home/...` 等挂载可正常使用。
- 只授权**本次 hostfile 内容器的 root 互联**。宿主机的普通用户或入口机不会自动得到容器 root 权限；从这些位置登录需自行管理相应公钥授权并写 `ssh root@node -p PORT`。后续增加节点应在维护窗口整体重新建立同一节点集合的互信，不能只建新节点就假定旧容器已信任它。
- 32 节点以内验证所有容器间 SSH 路径（含自身）；更大规模验证首节点到所有节点、各节点到首节点及环形路径，避免万卡集群 N² 连接爆炸。每条受测 SSH 的 namespace/UID 与宿主机 `docker exec` 对照，后续主动测试仍检查实际 MPI 组路径。日志标明验证拓扑，不夸大为全网络实测。
- 创建/分发/验证不是跨节点事务；后阶段失败可能留下部分新容器，终端返回失败原因，不自动删除回滚、不写 preflight.json。查看 `docker logs worker` 和容器内 `/var/lib/hcu-cluster-ssh/sshd.log`。中断维护后也应逐节点确认实际状态。

参考[容器间免密文档](https://r0ddbu55vzx.feishu.cn/wiki/RLPuwp70wiZShuk4VDvc7dSNn9g)的端口及公钥认证流程；不采用复制整个 `.ssh` 私钥目录或设置 root 密码的做法。

容器维护仅限 `per-node-container`，在宿主机运行，不需 `env.sh`：

```bash
./bin/hcu-cluster-run per-node-container container-create   -f hostfile --container worker -i "$IMAGE" -v /share:/share
./bin/hcu-cluster-run per-node-container container-recreate -f hostfile --container worker -i "$IMAGE" -v /share:/share --yes
./bin/hcu-cluster-run per-node-container container-delete   -f hostfile --container worker --yes
```

`--dry-run` 只验证命令和节点计划，不改动容器。额外 Docker 参数可重复传 `--docker-arg=--network=host`；离线镜像传 `--image-tar /share/image.tar`，即使本地已有同名 tag 也会重新加载。创建/重建在更改容器前核对各节点实际 image ID，ID 不一致即停止；重建不自动继承旧容器的挂载、网络、用户和 CMD。旧版无场景 `-f/-g/-s` 与 `baremetal-cluster` 命令已停止支持；旧源码载荷仍保留供新版调用。

## 7. 边界

不读取集群编排控制面；不依赖 Slurm Job ID、srun 或 Slurm nodelist；检测操作不自动修改驱动、网卡、容器或 Conda 环境；容器维护是显式操作；不设置全局门禁。

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
| `rccl` | 分组 `mpirun-torchrun` 与 RCCL/PyTorch 主动通信 |
| `gemm` | 分组 GEMM 主动计算 |
| `ib-write-bw` | 实际 server/client 配对带宽，按 HCA/方向测试；支持 ib/roce、阈值和测试数限制，不使用 MPI |
| `platform` 内置 IB 状态 | 每节点检查 IB/RDMA 设备、端口、链路和状态；不产生带宽流量 |
| `nhc` | 既有 NHC 检测 |
| `custom` | 自定义 Shell/Python 主动测试 |
| `local` | 本地 Shell/Python 回归，不访问计算节点 |

一次执行三种场景的接口矩阵及本地单测，不接受按用例筛选。基础检测和容器状态检查默认真实执行；其余类别默认 dry-run，需分别设置 `RUN_ACTIVE=yes`（RCCL/GEMM worker）、`RUN_PROFILES=yes`（rccl-tests/rocblas）、`RUN_NETWORK=yes`（IB 带宽）、`RUN_DIAGNOSTICS=yes`（nhc；IB 状态由 platform 覆盖）、`RUN_SCRIPTS=yes`（用户脚本）或 `RUN_CONTEXT=yes`（无 DCU 的 Shell/Python 上下文探针）。这些开关互不替代；容器维护和独立 launcher 构造用例始终为 dry-run。上下文探针仍 source 用户 env.sh，且当次 `contexts/` 输入须在各目标节点/容器内同路径可见。`calls.tsv` 和 `acceptance-result.json` 分别保存调用退出码和逐项验收结果；dry-run 与预检拦截不计为实测通过。详见 [README 总测试入口](../README.md#7-服务器手工验证总入口)。

常用变量：`HOSTFILE`、`ENV_SCRIPT`、`CONTAINER_NAME`、`IMAGE`、`GROUP_SIZE`、`ACTIVE_SLOTS`、`OUTPUT_DIR`、`CUSTOM_SCRIPT`、`DIAGNOSTIC_SCRIPT`。

## 8. 中断、失败与覆盖含义

Ctrl+C/SIGTERM 后停止新任务，在所有计划节点按本次唯一 token 清理组首、rank、脚本与 IB 两端，逐节点验证零残留；不杀其他业务进程。需要 Linux `/proc`、Bash 4+、setsid、flock、timeout。不能连接节点时返回 `CLEANUP_UNCONFIRMED` 并列出证据，不能承诺断网时已经结束；控制端 SIGKILL/断电只能依赖远端 timeout 兜底。自定义脚本不得清除任务标识并自行 daemonize。

主动程序失败或基础检测的传输、环境初始化、探针执行失败，在远端清理已确认时返回 2；取消且清理已确认返回 130；工具错误、预检拦截或清理未确认返回 3。基础检测各节点探针成功退出并取得有效健康结论时，环境 BLOCKED/INCOMPLETE 仍返回 0；远程非零返回码、缺少返回码或缺少节点结果属于执行失败，不能按环境不完整返回 0。任何返回码都不是跨命令门禁。总测试入口区分接口错误、环境异常、dry-run 和预检拦截；PRECHECK_FAILED 表示测试尚未执行，不能计作主动计算覆盖。准确扩展规则见工程 `AGENTS.md`。

RCCL 二进制验收用例在 `scripts/test.sh` 中默认每节点 8 卡（`RCCL_NPROC_PER_NODE` 可覆盖），不与 Python worker 的 `NPROC_PER_NODE`（默认 1）混用。真实执行需有该规模十项完整基准；测试脚本使用短小数据量只验证功能，性能不达标是有效检测结果，不代表代码异常。
