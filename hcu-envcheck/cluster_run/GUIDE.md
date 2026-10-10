# cluster_run 开发指南

## 层次和执行位置

```text
entry node: bin/hcu-cluster-run (Bash)
    └─ Python controller/report layer (entry node)
        ├─ basic: SSH/clush -> every compute node -> source env.sh -> probe
        └─ active: SSH -> group leader -> source env.sh -> mpirun
                                      └─ group nodes -> source env.sh -> torchrun/test
```

入口 Shell 只负责定位工程和选择解释器；Python 负责编排、远程命令构造、节点探针解析和报告。真正的驱动/网络/显卡检查在计算节点，真正的主动测试进程在测试组的计算节点。

## Python worker launcher contract

- `mpirun-torchrun`：每个 MPI rank 对应一个组内节点，rank 转成 torchrun `--node-rank`。
- `ssh-torchrun`：组内每个节点一个并行 SSH 任务，节点序号按公共 hostfile 顺序映射为 node rank。
- `mpirun`：保留 MPI 环境变量并导出统一的 `RANK/WORLD_SIZE/LOCAL_RANK`，直接执行测试命令。

`env.py` 不解析 env.sh 内容，在目标节点以 `set -e` 执行 `source env.sh`，初始化失败立即停止，再执行实际命令。
内置 Python worker 的 `PYTHONPATH` 在各节点 source env.sh 后追加工程根目录；默认保留容器工作目录，显式传 `--container-workdir <容器内工程根目录>` 时，所有目标执行及预检均在 source 前恢复该目录。目标 `--test-python -m torch.distributed.run` 启动 torchrun，不依赖入口 Python/torch。入口调度与报告需要 Python >= 3.10，可独立使用 `--controller-env-script/--controller-python` 引导。

## 输出约定

运行目录统一命名为 `<场景>_<测试项>_<时间戳>`（`hcu_envcheck.output.run_directory_label`：`per-node-container→container`、`shared-conda→conda`、`node-local-conda→lconda`；操作名 `,`/`-` 转 `_`；同秒重跑追加 `_1`）。基础检测调用既有 `hcu_envcheck.baremetal_cluster`，随后 `consistency.py` 对 `nodes` 记录分组；主动检测按 `groups/group-NNN` 写入 hostfile、命令和 stdout/stderr，每组含 `hostfile`（每行 `节点 slots=N`）、`launch.json`、`result.json`、载荷产物目录（如 `rccl/`）、`nodes/*/stderr.log` 与 `remote-evidence/`。报告失败不会形成全局门禁。

基础检测落盘 JSON 使用 schema 2.0：`run`、`node_status.nodes`、`hardware_devices.nodes`、`system.nodes`、`driver_dtk.nodes`、`software_components.nodes`、`network_rdma.nodes`、`resource_state.nodes`、`network_health.nodes`、`execution_evidence.nodes`、`cluster`。按类别独立折叠同值节点，`members` 显式列出全部节点；动态采样保留可追溯证据。总配置分组及差异报告共用静态字段契约；resource-only 同样不以瞬时利用率分配置组，未采集字段不当作相同配置证据。

## 固定载荷协议与取消

唯一维护依据为工程 [AGENTS.md](../AGENTS.md)。多节点 RCCL 二进制及 RCCL/GEMM worker 可使用 MPI；单节点本机启动。`rccl` 默认 `rccl-tests`，脚本拥有唯一一层 MPI，每节点默认 8 slots，容器内保留原命令 root 许可；不得改成 Python smoke test 或外套 torchrun。默认十项全部执行并与当前 np 的 busbw 基准比较；缺基准或缺项报错，不静默跳过。每组 `rccl/` 保存原始日志、完整命令和 `rccl-summary.md/tsv`；四列带宽为各尺寸独立峰值，判据沿用原 busbw 峰值 ≥ 基准 × (1-margin%) 规则。Python 测试须显式 `rccl --profile worker`；GEMM 默认 worker、完整基准显式 `--profile rocblas`。普通 `script/custom` 无 MPI；`platform` 默认复用现有 IB 状态检查，NHC 与 IB 配对带宽复用现有实现。容器 peer 端口通过 `--container-ssh-port` 指定，实际 SSH namespace/UID 必须与 docker exec 一致。

rank/world/master 在 source 后由 launcher 覆盖；env.sh 无需声明。组首、rank、脚本和配对两端使用统一 RemoteTaskSession/token guard；首个启动失败立即收敛其余任务，Ctrl+C 逐节点确认清理。失败/不可达不得声称已结束，远端仅依赖 Linux `/proc`、Bash、setsid/flock/timeout，不需要 Python 清理器。

## 执行职责

`cluster_run/cli.py` 是唯一参数接口；`node_script.py` 执行逐节点 Shell/Python 诊断（不分组、无 MPI）；`lifecycle.py` 在宿主机对全节点显式创建/重建/删除容器，镜像获取失败时先于删除动作返回。既有 payload 和 `hcu_envcheck` 内部探针继续复用。

显式创建/重建 `--port N` 才启用 `container_ssh.py`，不与检测预检混用。目标容器入口 Bash 每次启动 sshd 后 exec 原镜像 Entrypoint+Cmd（`--container-command` 仅覆盖 Cmd）；私钥只在容器生成，入口层收集公钥后分块分发，避免千节点时 argv 长度溢出。镜像依赖/端口失败发生在删除之前；创建后的分发或验证失败报告 FAIL，不自动删除已建容器。故障可查看 `docker logs NAME` 和容器内 `/var/lib/hcu-cluster-ssh/sshd.log`。依赖与限制见 [用户指南](../docs/USER_GUIDE.md#6-容器维护命令)。

## 手工验证脚本

统一功能验证入口是 `scripts/test.sh`。脚本不封装公共执行函数，每项直接展开 `hcu-cluster-run` 命令；一次执行覆盖三种场景、容器状态、基础检测、主动测试、逐节点脚本和容器维护 dry-run。`container-status` 不需 `env.sh`，只读核验容器名、运行状态与镜像一致性。环境结果不作为测试脚本门禁；基础报告缺失/结构错误及接口 dry-run 失败会使脚本返回非零。

```bash
HOSTFILE=./hostfile ENV_SCRIPT=/share/env.sh \
  CONTAINER_NAME=hcu-worker IMAGE=image:tag bash scripts/test.sh
```

基础检测和容器状态检查默认真实执行；其余类别默认 dry-run。真实执行由独立开关控制：`RUN_ACTIVE=yes`（RCCL/GEMM worker）、`RUN_PROFILES=yes`（rccl-tests/rocblas）、`RUN_NETWORK=yes`（IB 带宽）、`RUN_DIAGNOSTICS=yes`（nhc；IB 状态由 platform 覆盖）、`RUN_SCRIPTS=yes`（用户脚本）、`RUN_CONTEXT=yes`（无 DCU 的 Shell/Python 上下文探针）。容器维护和独立 launcher 构造用例始终 dry-run；上下文探针仍 source 用户 env.sh，所用当次 `contexts/` 输入需在各目标节点/容器同路径可见。逐项退出码见 `calls.tsv`，报告路径、节点范围及执行状态见 `acceptance-result.json`；计划验证或预检拦截不是实测通过，只有上下文实测也不能证明 MPI 取消或计算/带宽功能已验证。完整变量说明见 [README 总测试入口](../README.md#7-服务器手工验证总入口)。CI 单元测试由 `python -m unittest` 负责。

RCCL 二进制验收用例在 `scripts/test.sh` 中默认每节点 8 卡（`RCCL_NPROC_PER_NODE` 可覆盖），不与 Python worker 的 `NPROC_PER_NODE`（默认 1）混用。真实执行需有该规模十项完整基准；测试脚本使用短小数据量只验证功能，性能不达标是有效检测结果，不代表代码异常。
