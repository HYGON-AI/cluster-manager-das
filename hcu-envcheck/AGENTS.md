# 工程约定（实现与扩展必须遵守）

## 唯一入口与执行位置

- 公共入口仅 `bin/hcu-cluster-run <scenario> <operation> ...`。不增加旧命令兼容分流、Slurm 控制面或 Kubernetes 检测。
- 三种 scenario：`shared-conda`、`node-local-conda`、`per-node-container`。Slurm 仅由用户在工具外申请节点，工具输入是 hostfile。
- 入口 Shell 负责帮助、参数引导和控制端解释器选择。调度、结果解析及报告仍用入口节点的 Python >= 3.10 标准库；不能宣称实际检测完全不依赖入口 Python。
- `--controller-env-script` 仅在入口节点加载；`--env-script` 是可选的目标环境初始化脚本，仅在目标计算节点/指定容器加载。省略时直接使用目标当前环境；二者不得互相代替。
- 基础探针在目标环境使用 `--remote-python`；测试程序使用 `--test-python`。传入 `--env-script` 时在 source 后解析，省略时从目标当前环境解析。报告只在入口节点生成。
- Shell 载荷、NHC、IB perftest 不得为了执行命令而依赖目标 Python。受控执行需要目标 Linux `/proc`、Bash 4+、`setsid`、`flock`、`timeout`。

## 环境与配置

- 提供 `env.sh` 时，它必须是真实可 source 的脚本：module/source/export/conda activate。不解析脚本文本，不要求版本、节点、rank 等元数据变量；未提供时不得猜测或搜索环境脚本。
- 探针从 source 后的 PATH、库路径、DTK/ROCM 等环境变量和命令输出获得实际信息；保留解析来源。不能用固定 `/opt/dtk` 值覆盖用户选中的版本。
- 先进入 `--container-workdir`，再 source。预检、MPI rank、SSH worker、基础探针必须使用同一工作目录语义。
- 项目及主动测试输出目录需要在计算节点可见；容器内项目路径不同时传 `--container-workdir`。分组 hostfile 必须位于所有组员可见的共享路径，不能只在入口本地生成后假定可见。
- 版本差异、资源忙等检测结果与工具执行错误分开；无跨命令全局门禁。

## 载荷调用协议

1. `platform/resource`：每节点独立并发采集，之后自动生成一致性报告；不启动 MPI。
2. `script`：Shell/Python 脚本每节点执行一次，无 MPI、无自动 rank。`custom` 同样逐节点执行，可保留组目录组织结果；不能把任意 Shell 脚本自动套在 torchrun 上。
3. `rccl/gemm --profile worker`：无 `--script` 使用内置 Python worker；指定 `--script` 表示**每 rank worker**，不允许 worker 再调用 MPI/torchrun。GEMM 默认 worker；RCCL 使用 worker 必须显式选择，不能替换既有二进制测试。
4. 单节点组不调用 MPI；需要本机多进程时由选定 Python 的 `-m torch.distributed.run` 启动。
5. 多节点 RCCL/GEMM **worker**：默认 MPI 每节点一个 bootstrap，再 torchrun；`--launcher mpirun` 直接每卡/进程 worker；`--launcher ssh-torchrun` 由入口并发启动每节点 torchrun。
6. `rccl` 默认 `--profile rccl-tests`，每节点默认 8 个 slots，保留现有 RCCL 二进制基准脚本。脚本拥有唯一一层组级 MPI：多节点每 rank `*_perf -g 1`，单节点直接二进制 `-g <卡数>`；不得外套 MPI/torchrun。可显式 `--launcher mpirun`，其他 launcher 须选择 worker。容器内所有实际 MPI 启动由统一入口自动应用 `--allow-run-as-root`，预检与执行保持一致，不暴露用户开关；宿主机 MPI 不使用该策略。通信参数继承 env.sh，未设置时用用户原命令的站点默认值，显式脚本参数优先；不编造 RCCL_NET_PLANE。RCCL 默认一次完整执行十项并逐项比较当前 np 的 busbw 基准和 margin；缺少规模/项目基准须报配置错误，不静默跳项，不降级成功。正常启动后的单项失败不跳过剩余项，取消除外。保留显式 --tests 诊断能力，但子集结果不能宣称完整验收。报告输出 out/in-place algbw/busbw 四列独立峰值以及基准、容差、偏差和原因；不得把一列 busbw 基准当作四列阈值。`gemm --profile rocblas` 保留每节点各卡/形状 rocBLAS 基准，不接受 launcher。不能根据脚本 basename 猜测执行类型。
7. `platform` 默认包含每节点 IB/RDMA 状态检查；`nhc` 每节点独立；`ib-write-bw` 为真实 server/client 配对主动测试，不用 MPI/torchrun。平台检查不隐式启动带宽流量。
8. `--script-arg=VALUE` 逐项透传，始终按 argv 安全引用，不 eval。`--group-size` 是组内节点数，`--slots` 是并发组数，`--nproc-per-node` 是每节点进程数，三者不可混用。
9. 由 runner 在 source 后生成 `HCU_GROUP_NAME/HCU_GROUP_HOSTFILE/HCU_GROUP_NODES`；worker 的 `RANK/WORLD_SIZE/LOCAL_RANK/MASTER_ADDR/MASTER_PORT` 由本次 launcher 生成，覆盖外部遗留值。MPI 原始值在 source 前保存。用户不应在 env.sh 设置它们。
10. worker 退出 0 才可认定执行通过；内置 worker 返回 2 且输出匹配本测试的 `INCOMPLETE` 证据才标不完整。返回 124/255、信号退出、启动失败等不得被部分 worker JSON 掩盖。

## 容器 MPI 与取消

- 组首宿主机 SSH → `docker exec` 指定容器 → source → mpirun → `--mca plm_rsh_args "-p PORT"` 进入各节点容器。默认容器 SSH 端口 25901，可显式修改。
- 在启动前用实际容器 SSH 路径与 `docker exec` 比对命名空间和 UID；端口监听不等于容器身份正确。检测/测试不自动修改 sshd 或容器网络。
- 仅显式 `container-create/recreate --port N` 为新容器配置 host 网络及 root 公钥 SSH；不传时不修改 SSH 行为。保留镜像原 Entrypoint/Cmd；bootstrap 每次容器启动执行，不能只 docker exec 启动一次。先全节点检查镜像依赖与端口，再删除旧容器；端口占用无法归属时拒绝，禁止杀宿主机 sshd。
- 每容器独立生成用户及 host 私钥；只交换公钥，固定 known_hosts 且严格验签，不复制私钥、不启用密码登录、不重置 root 密码、不挂载/写宿主机 SSH 配置。该模式要求宿主机 ss，root Linux 镜像已有 Bash/OpenSSH/PAM 与 /proc，离线不自动安装。32 节点以内全互联验证；更大规模采用组首星形+环形 O(N) 路径验证，主动 MPI 仍校验实际组路径，不能声称完成所有 N² 链路实测。
- 容器状态失败终端折叠输出原因；不创建 preflight.json、不自动 pull/recreate；创建/重建/删除必须使用显式操作及确认。
- 每次执行生成不可复用 token；组首、所有 MPI rank、每节点脚本、NHC/IB 两端都受 token guard 管理。不能只终止入口 SSH。
- Ctrl+C/SIGTERM 停止派发并对所有计划节点清理，TERM→KILL 后验证，保留停止标记拒绝晚到 rank。禁止 `pkill python/mpirun` 等宽泛清理。
- `CONFIRMED` 必须有远程零存活进程及停止标记证据。节点失联/权限不足/容器不可访问必须 `UNCONFIRMED`，不得称“全部结束”。控制端被 SIGKILL 或断电不能承诺即时远程清理；远程 timeout 是兜底，不是确认凭证。
- 自定义脚本不得主动脱离任务管控（清空 task token 后 daemonize、跨用户后台启动等）；此类行为不属于可受控载荷协议。

## 报告与测试

- 前台日志按节点数必须是 O(摘要) 而非 O(节点数)：默认（`--log-detail milestone`）按「状态×原因码」折叠同类节点为单行，cleanup 汇总为计数行，健康组不逐组输出；`every` 恢复逐节点/逐组明细，`quiet` 只保留 ERROR 级折叠。折叠不得吞掉 ERROR：未确认清理和失败原因必须保留在终端可见输出中。JSON/Markdown 报告始终逐节点完整记录，不受日志粒度影响。
- 配置分组与差异报告共用静态字段契约，包含硬件、系统内存/限制、驱动/DTK、Python packages/Torch/RCCL/UCX/MPI、网络配置；不将利用率、显存使用量或设备唯一序列号算作配置差异。
- 证据缺失不能合并成“配置相同”。节点和 device_id 折叠必须可逆、且保留完整证据路径。
- `scripts/test.sh` 是单测和接口验收总入口。每个用例记录退出码、状态、报告是否存在/可读/属于本次执行、节点覆盖；不能只统计执行了几条命令。
- 环境健康 FAIL/BLOCKED 是有效检测；PRECHECK_FAILED 表示主动测试未执行，dry-run 表示仅验证命令构造，不得统计为计算实测覆盖。
- 单测需覆盖参数拒绝、环境顺序、三种场景、分组/余数组、MPI/torchrun rank、实际 SSH 身份校验、失败状态优先级、取消并验证、NHC/IB/基准脚本、报告分组。Linux 进程测试不得在 Windows 上假装通过。
