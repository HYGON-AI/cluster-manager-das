# cluster_run 执行层

`cluster_run` 是 `hcu-cluster-run` 的 Python 编排层：入口节点读取 hostfile、切分 group、限制 `--slots`、构造远程命令并生成报告；基础探针在目标计算节点执行，主动测试在测试组的计算节点执行。

新版入口不再分发无场景旧参数。操作分为 `platform/resource` 基础检测、`rccl/gemm/ib-write-bw/custom` 分组主动测试、`script` 逐节点独立诊断、`container-status/create/recreate/delete` 宿主机容器管理。容器修改仅由显式维护操作执行；重建/删除需 `--yes`。

基础检测报告为 `cluster-result.json`（schema 2.0）和 `cluster-summary.md`。JSON 顶层按节点状态、硬件设备、系统、驱动/DTK、软件组件、网络/RDMA 配置、资源状态、网络健康、执行证据分类；每类各自折叠相同节点，`members` 保留真实节点名。Markdown 优先给出逐节点显卡状态与配置差异，瞬时资源占用不冒充静态配置差异。旧版内部 `report["nodes"]` 数据结构仍供检测逻辑使用，不是落盘格式。

多节点 RCCL/GEMM worker：入口 SSH 到组首 -> 按需 source env.sh -> mpirun -> 组内每节点按需 source -> torchrun/直接 worker。单节点不调用 MPI；普通 `script/custom` 每节点直接执行。容器 MPI 通过 `--container-ssh-port` 设置 `plm_rsh_args -p` 并核验 SSH 身份。

`rccl` 默认 `--profile rccl-tests`，执行已有 Shell 二进制测试：每节点默认 8 slots，多节点直接 MPI 启动 `*_perf -g 1`，单节点直接 `-g <卡数>`，没有嵌套 MPI/torchrun。容器内保留原命令的 root MPI 许可及通信参数默认值；env.sh 可以覆盖站点配置。默认完整执行十项并逐项比较当前 np 的 busbw 基准和容差；基准缺失/缺项报配置错误，不跳项或降级成功。执行失败继续剩余项，取消除外。各项日志、完整命令及含四列实测带宽的 `rccl-summary.md/tsv` 保存在每组 `rccl/` 下。RCCL Python smoke test 使用显式 `--profile worker`；GEMM 默认 worker，完整基准显式 `--profile rocblas`。不猜脚本 basename。`platform` 默认包含现有 IB 状态检查；`ib-write-bw` 复用实际 server/client 配对主动测试，不使用 MPI；`nhc` 是独立节点检测。所有执行层环境、取消和新增脚本契约见 [AGENTS.md](../AGENTS.md)。

`task_control.py` 与 `payloads/task_guard.sh` 管理任务 token、晚到任务阻断及逐节点取消确认。`env.py` 在登录 Shell 初始化后恢复显式 workdir，并在提供脚本时再 source；目标工具解析不能依赖入口节点的 Python/DTK。Python 编排与报告仍在入口节点，需要入口 Python >= 3.10；可用 `--controller-env-script` 独立引导。

容器状态可单独检查，无需 `env.sh`：

```bash
./bin/hcu-cluster-run per-node-container container-status \
  -f nodes.txt --container hcu-train -i "$IMAGE"
```

它在宿主机读取已有容器的精确名称、运行状态、镜像标签和跨节点镜像 ID，不检查 DCU 空闲、不 pull 或重建。容器内基础检测和主动测试共用此检查；基础检测保留异常节点报告且继续检测健康节点，主动测试在预检失败时不启动。容器场景的基础/主动测试与 `script` 的 `-i/--image` 均为必填，按其核验用户期望镜像；仅 `container-delete` 不接受该参数。检查运行中容器不要求本地仍保留镜像标签。

`container-create/recreate --port N` 由 `lifecycle.py` 编排、`container_ssh.py` 生成目标 Bash：全节点依赖/端口检查 → 创建并随容器启动 sshd → 只交换公钥与可信 host keys → SSH namespace/UID 验证。控制端需要 Python，目标 SSH 配置不需要 Python。`--port` 不改变宿主机传输端口；MPI 仍使用 `--container-ssh-port N`。只读 `container-status` 不隐式配置或验证 SSH 互联。
