<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# 工程结构与文件职责

本文面向开发者、维护者和代码审阅者，说明 `hcu-envcheck` 当前源码的目录、文件用途、模块依赖和修改定位。对应版本为 `0.4.2`；文中“当前”指仓库现有实现，不表示所有路径均已通过真实集群验收。总体架构、运行流程和接口约定见 [DESIGN](DESIGN.md)，命令与结果目录见 [USER_GUIDE](USER_GUIDE.md)，缺陷与验证范围见 [KNOWN_LIMITATIONS](KNOWN_LIMITATIONS.md)。

## 文档与工程导航

| 内容 | 文档 | 本文用途 |
|---|---|---|
| 命令与结果目录 | [USER_GUIDE](USER_GUIDE.md) | 找到实际操作的入口文件 |
| 总体架构与设计 | [DESIGN](DESIGN.md) | 将分层、流程与接口对应到源码 |
| 目录及文件用途 | 本文 | 按功能定位维护范围 |
| 当前缺陷与证据边界 | [KNOWN_LIMITATIONS](KNOWN_LIMITATIONS.md) | 区分文件职责与尚未完成的实现 |

## 顶层目录

```text
hcu-envcheck/
├── bin/                         # 唯一源码公共命令
├── cluster_run/                 # 公共分流、Bash 控制器和保留 Python 控制路径
│   ├── baselines/               # 公开性能基准格式模板
│   ├── examples/                # 节点文件示例
│   └── tests/                   # 入口/DeepEP 协议辅助检查脚本
├── hcu_envcheck/                # 探针、执行适配器、规则、网络诊断
│   ├── state_facts.py           # 目标 Python 校验并生成唯一事实候选，不发布
│   ├── state_runtime.sh         # 脚本缓存、逐项固定工作区回收与运行收尾
│   ├── payloads/                # 测试载荷、目标设备选择与进程 guard
│   │   └── device_selection.py  # RCCL/DeepEP 目标设备选择与执行前复核
│   └── reporting/               # 解析、聚合、面板、报告及计划导出
│       ├── facts.py             # 新组 facts 与历史逻辑路径的只读兼容层
│       └── scale.py             # 计划规模与有证据的历史规模转换
├── docs/
│   ├── USER_GUIDE.md             # 命令示例与结果目录
│   ├── DESIGN.md                 # 总体架构、运行约定与参数参考
│   ├── PROJECT_STRUCTURE.md      # 目录、文件功能和模块定位
│   ├── KNOWN_LIMITATIONS.md      # 当前缺陷和验证边界
│   └── INDEX.md                  # 文档导航
├── examples/                    # 旧执行器/规则使用的输入格式示例
├── scripts/                     # 验证矩阵和离线包构建
├── tests/                       # Python 单元与契约测试
├── third-party/                 # DeepEP、flux 独立第三方源码目录
├── install.sh                   # 已构建离线包的复制式安装器
├── MANIFEST.release             # 离线发布白名单
├── pyproject.toml / setup.py    # Python 包元数据、资源与法律文件处理
├── VERSION                      # 源码/离线包版本
├── AGENTS.md                    # 工程修改约定
└── README.md                    # 定位、最短入口与文档导航
```

`dist/` 为构建产物；`verify_results/`、`cluster_run_results/` 为运行产物，通常不属于源码。真实 `hostfile`、环境脚本、站点配置和性能基准由部署者维护，不应作为公开示例提交。

## 模块依赖与执行位置

| 模块 / 目录 | 主要调用方 | 执行位置 | 依赖边界 |
|---|---|---|---|
| bin 与 dispatch | 使用者、安装后的 console script | 登录节点 | 唯一公共操作清单与路由 |
| controller、worker、remote | dispatch、controller | 登录节点 | C 唯一发布 facts；worker 交付临时结果；不在登录节点运行 GPU 载荷 |
| state_facts | controller 经 remote 的普通 SSH 许可调用 | 选定报告目标环境 | 使用既有 Python 校验、序列化并关闭候选，不发布 facts，不写派生报告 |
| lifecycle.sh | dispatch | 登录节点调度计算节点宿主机 | Docker 管理独立于分阶段报告 |
| payloads、pod_probe、target_ib | worker 构造的目标命令 | 计算节点容器 / Conda | 实际采集、MPI/算子、IB 与 guard |
| reporting | remote、显式重建 | 报告节点目标环境 | 复用 environment/preflight 规则和 consistency，不调度新测试 |
| CLI、active、extra_checks、task_control | 保留 Python 路径、目标 IB | 依实际调用位置 | 公共 Bash 路径仍复用的模块不能直接删除 |
| 纯规则与模型 | 报告、旧采集器、网络检查 | 调用方所在环境 | 输入/输出和调度分开，不负责远程修复 |

总体拓扑、执行时序、rank 所有权和失败行为集中在 [DESIGN](DESIGN.md#总体架构)。下面按实际文件列出职责，不以“旧 Python 路径”推断文件无用途。

## 入口与控制层文件

下表路径以工程根目录为基准。

| 文件 | 功能 | 主要调用关系 / 维护要点 |
|---|---|---|
| `bin/hcu-cluster-run` | 薄 Bash 启动器，定位工程并转交分流 | 不维护另一份操作清单 |
| `cluster_run/dispatch.sh` | 三场景、14 操作唯一公共清单；路由、公共帮助、Python 选择 | Python CLI 从其清单读取选择项；接口变更先核对这里 |
| `cluster_run/controller.sh` | 参数校验、完整分组、prepare、阶段循环、并发、监督、单日志通道和事实提交 | 主进程 C 唯一核验并原子发布组 facts；组子进程只交付结果 |
| `cluster_run/remote.sh` | 构造报告目标环境，启动报告及事实候选辅助命令 | 报告主机可选择；候选序列化使用普通 SSH 许可，保持清理保留额度 |
| `cluster_run/log_archive.sh` | 组日志片段索引与历史日志归档适配 | controller 调用，历史报告读取仍需保留兼容机制 |
| `cluster_run/__main__.py` | 安装入口转发、内部 Python 入口和异常边界 | console 与模块调用语义需分清 |
| `cluster_run/__init__.py` | 控制包版本导出 | 复用 `hcu_envcheck` 版本 |
| `cluster_run/cli.py` | 完整帮助、保留 argparse 与 Python 路由 | RCCL/DeepEP 转交同一 Bash controller，避免另一套 G/C 语义 |
| `cluster_run/active.py` | 内部 Python 组执行和结果 | 仍有调用方，不能按“旧文件”直接删除 |
| `cluster_run/testsuites.py` | 内部 RCCL/IB profile 校验 | 公共主动测试由 Bash controller 调度 |
| `cluster_run/hostfile.py` | Python 路径节点读取、分组和组文件 | 公共 Bash 的分组逻辑在 controller 中；修改时核对两者边界 |
| `cluster_run/env.py` | 目标 Shell 环境引导及脚本校验 | 复用于 Python 执行路径 |
| `cluster_run/preflight.py` | Python 路径容器状态与身份预检 | 不等于 Bash worker 的预检实现 |
| `cluster_run/task_control.py` | Python 远程任务 token、取消、清理适配 | 复用 payload guard |
| `cluster_run/extra_checks.py` | IB 状态和组内带宽执行适配 | 被 CLI 与 `target_ib.py` 调用 |
| `cluster_run/consistency.py` | 静态配置与结果一致性分析及 Markdown 渲染 | 被当前 reporting 复用，属于活跃依赖 |
| `cluster_run/rerun_plan.py` | 校验导出清单，生成 Bash 可读 TSV，恢复原分组和规模 | 登录节点 python3 执行；RCCL/DeepEP 复用 reporting/scale.py 解析 G/C；既有校验缺口另行管理 |
| `cluster_run/lifecycle.py` | 保留 Python Docker 生命周期实现 | 内部 CLI 仍引用；公共容器路由使用 Bash lifecycle.sh |
| `cluster_run/container_ssh.py` | 保留 Python 创建时 SSH 初始化 | 由 Python lifecycle 调用；不据此宣称 Bash 具备同等行为 |
| `cluster_run/cluster_env.conf.example` | 站点配置格式模板，含历史字段 | 当前 RCCL 载荷读取站点配置；不是全操作统一配置解析器 |
| `cluster_run/baselines/rccl_baseline.conf.example` | RCCL 总卡数/测试项/busbw/容差格式 | 仅模板；私有数值不提交 |
| `cluster_run/examples/nodes.txt.example` | Python 节点输入格式示例 | 公共 Bash 只按使用指南支持的简单 hostfile 格式使用 |
| `cluster_run/tests/help_smoke_check.sh` | 帮助接口辅助检查 | 存在历史断言，需按当前路径复核 |
| `cluster_run/tests/deepep_contract_check.sh` | DeepEP 启动协议辅助检查 | 静态/辅助检查不能代替真实八卡与多机执行 |
| `cluster_run/README.md`、`GUIDE.md` | 控制层导航和命令速查 | 不复制完整用户手册和私有站点信息 |

已删除的 `cluster_run/group_runner.sh`、旧 `cluster_run/payloads/`、`launchers.py`、`node_script.py` 等不属于当前调用链。新增文档或修复调用方时不要继续引用这些路径，也不要为兼容过期文档恢复冗余实现。

## 探针、规则与执行组件

| 文件 | 功能 | 当前关系 |
|---|---|---|
| `hcu_envcheck/__init__.py` | 包版本 | 与 VERSION、包元数据保持一致 |
| `hcu_envcheck/state_runtime.sh` | 不可变脚本缓存、提交后的固定工作区回收及运行收尾 | C 确认事实发布后按身份和白名单回收；异常现场保留；report.lock 不删除 |
| `hcu_envcheck/state_facts.py` | 校验 task/worker/node/cleanup/timeout/log 交接并生成版本化候选 | 目标 Python 辅助进程唯一命名写候选，C 核验 revision/SHA256 后原子发布 |
| `hcu_envcheck/logging.sh` | Shell 命令的统一可读展示 | 统一开始/环境/实际 argv/原始输出/结束格式，不执行或更改测试参数 |
| `hcu_envcheck/group_logs.py` | Python 命令的实时原始输出与逻辑片段身份 | compact 经继承通道交给唯一 cat 归档；保留旧 Python 日志与索引接口 |
| `hcu_envcheck/worker.sh` | 单组 prepare/execute/cleanup、目标环境拼装、SSH 额度与交接 | 固定 runtime 中交付带身份的节点、主 worker 与独立 cleanup attempt 结果，不写 facts |
| `hcu_envcheck/lifecycle.sh` | 查询、创建、重建、删除容器，记录节点及阶段结果 | 公共 Docker 管理实现；有已知缺陷 |
| `hcu_envcheck/lifecycle_output.sh` | 父进程收集逐节点日志，合并同类终端结果并保存明细 | 由 lifecycle.sh 加载；只负责展示和证据保存，不决定 Docker 操作及通过条件 |
| `hcu_envcheck/pod_probe.py` | 系统、平台、软件、网络和资源原始采集 | 名称含 pod，但当前也在容器/Conda 目标中被直接调用 |
| `hcu_envcheck/environment.py` | 平台与软件环境纯规则判定 | 被 reporting 的 platform 解析复用 |
| `hcu_envcheck/preflight.py` | 卡资源样本与阈值判定 | 被 reporting 的 resource 解析复用 |
| `hcu_envcheck/parsers.py` | hy-smi、rocminfo 等输出解析 | 规则输入，不负责远程调度 |
| `hcu_envcheck/models.py` | 命令结果、发现、设备和探针结果模型 | Python 执行器与规则共享 |
| `hcu_envcheck/baremetal.py` | Python 本地/SSH/clush 执行基础、节点输入和证据 | IB、Python 保留执行路径仍依赖 |
| `hcu_envcheck/baremetal_cluster.py` | 原有多节点采集、策略、汇总 | 内部兼容实现；不等于新增公共操作 |
| `hcu_envcheck/conda_mode.py` | Conda 部署模式、存储身份和采集规划 | Python 采集规划与兼容路径复用 |
| `hcu_envcheck/cluster_checks.py` | IB 状态、成对带宽计划/执行/判读 | 被 extra_checks 和 target_ib 复用 |
| `hcu_envcheck/target_ib.py` | 组首目标环境调用既有 IB 实现 | Bash worker 启动，不把所有 IB 控制放在登录节点 |
| `hcu_envcheck/verbs.py` | perftest/Verbs 输出解析 | IB 规则复用 |
| `hcu_envcheck/rdma.py` | RDMA 设备、用户态库、端口/计数器规则 | 平台/网络规则复用 |
| `hcu_envcheck/rdma_policy.py` | 显式 RoCE JSON 策略加载 | 保留 Python/规则能力；不是当前 Bash 平台公共参数 |
| `hcu_envcheck/roce_health.py` | RoCE 网络配置链纯规则 | 不执行远程修复 |
| `hcu_envcheck/ib_counters.py` | IB 计数器规则及样本差分评价 | RDMA 规则复用 |
| `hcu_envcheck/output.py` | 运行目录命名、独占创建、原子输出 | 主要用于 Python 保留路径，不替代 Bash 事实协议 |

### 载荷目录

载荷位于 `hcu_envcheck/payloads/`，实际在计算节点目标环境运行。一般不直接向用户公开内部 phase、token、stage-dir 参数。

| 文件 | 功能 / 固定协议 |
|---|---|
| `__init__.py` | Python 包标记，便于分发载荷资源 |
| `task_guard.sh` | 按本次 token 管理进程组、取消与清理；不使用宽泛进程名清理 |
| `dcu_idle_check.sh` | 主动载荷前的卡资源空闲检查；文件名是现有兼容路径 |
| `check_deepep_env.sh` | ACS、XDP、RDMA 等前置检查 |
| `test_rccl.sh` | RCCL 单机/多机启动器；单机 -g C，多机核对并直接使用组 slotsfile（每行 node slots=C），派发 rank 脚本 |
| `rccl_perf_test.sh` | RCCL 参数/站点配置、基准、计划及单机/多机测试驱动 |
| `deepep_test.sh` | 单机/组级 DeepEP 适配，构造 MPI 节点控制链 |
| `run_deepep_test.sh` | 工程内适配器；将显式 C 传给测例 --num-processes，保留多机 torchrun nproc=1；复用保存的设备选择 |
| `device_selection.py` | 目标环境的 HIP 查询、前 C 张允许设备选择、PCI 身份记录与执行前复核；select 保存，exec 复核后启动载荷 |
| `flux_70b_test.sh` | 单节点 TP4 的 16×4 形状矩阵，输出 case/sweep 事件 |

RCCL/DeepEP 的公共 G 为每组节点数、C 为每节点卡数，拒绝旧公共 slots/nproc-per-node。RCCL 单机直接二进制 -g C，多机每卡一个 MPI rank。DeepEP 多机每节点一个 MPI 控制 rank → 一个 torchrun 控制进程 → 测例 --num-processes C → 上游 spawn C 个 GPU worker；当前多机 C=8、G=2/4，单机允许 C=2/4/8。完整能力边界见 [DESIGN](DESIGN.md#测试规模与设备选择)。

device_selection.py 由 worker 在每个参与目标环境加载后调用 select，当前预检记录暂存于 `.state/runtime/<group>/devices/<node>.json`，由 C 提交到组 facts.devices 后回收。执行仍传 `.state/prepared/<group>/devices/<node>.json` 虚拟定位路径，由读取器从 facts 解析对应节点；历史独立 JSON 继续可读。RCCL rank 脚本与 DeepEP 适配器每项仍 source 环境并 exec 复核当前可见性、权限和 PCI 身份，不从登录节点探测或广播组首设备列表。它负责设备可见性，空闲规则仍由空闲脚本处理，GPU worker 的创建仍归第三方测例。

DeepEP 单机一次 intranode，多机各一次 lowl、norm。byte-flux 仍直接使用上游 TP4 启动约定，每组一次完整 64 项矩阵，不使用新的 C 配置。上游测例自身的 warmup 和测量迭代保持原样。

### 报告目录

`hcu_envcheck/reporting/` 使用 Python 标准库处理执行事实；不调度新测试，不负责修复容器或静默补跑。

| 文件 | 功能 |
|---|---|
| `__init__.py` | 报告包入口标记 |
| `__main__.py` | probe/watch/rebuild/rccl-local 内部命令，观察循环及异常出口 |
| `model.py` | 新旧逻辑 TSV 读取、状态、路径、原子写入及稳定 report.lock |
| `facts.py` | schema 2 组 facts 的只读解析、按用途表投影、日志范围与分组分配兼容；按 revision/文件身份刷新缓存 |
| `log_store.py` | 按片段索引读取当前组日志，兼容历史归档及原始日志布局 |
| `parsers.py` | RCCL、DeepEP、flux、check_deep、platform/resource 的结果解析与规则连接 |
| `aggregate.py` | 计划、预检、阶段和组结果汇总，覆盖与一致性；保留历史多轮结果读取 |
| `progress.py` | 终端面板、普通输出快照和显示宽度 |
| `render.py` | summary.md、failures.tsv、report.json、阶段结果和 RCCL 报告渲染 |
| `export.py` | 失败节点、失败组项、未完成组项、清理未确认清单导出 |
| `scale.py` | 统一计算 G/C、计划总卡数、MPI ranks 和 DeepEP 计划 worker 数；有证据时转换历史规模，含义不明则拒绝猜测 |

scale.py 被 aggregate/export 和登录节点的 rerun_plan 调用，只处理保存的元数据，不查询 GPU 或执行测试。它与载荷中的 device_selection.py 分工不同：前者统一“计划多少”，后者核对“目标实际允许看见哪些设备”。实际设备可见性也不能代替工作进程完整启动证据。

报告协议 v3 是派生结果协议。新 Bash 运行标记 `compact-group-v2`，持久组 facts 使用 schema 2 的 fields+rows；运行级 control/groups/stages 仍为 TSV。reporting/facts 将其投影为既有逻辑 task/node-results/cleanup/execution 路径，不为兼容创建每项整套文件。旧 schema 1 逐任务表、log-archive/log-stream、direct-log 指针及 prepared/hostfile 继续读取。新 RCCL/DeepEP 导出计划使用 schema 2 保存 G/C，其他操作及旧运行导出保留 schema 1。历史重跑只转换可明确确定的规模，扩展字段不能把旧事实解释为已实测数据。

## 运行产物与源码

| 路径 | 类型 | 维护边界 |
|---|---|---|
| verify_results/、cluster_run_results/ | 运行结果 | 不作为源码；完整保留原始证据后才可重建报告 |
| `.state/control.tsv`、groups.tsv、stages.tsv | 运行配置及固定计划，各保存一份 | C 写入；分组、节点顺序和测试项阶段屏障不随工作区回收改变 |
| `.state/prepared/<group>/facts.json` | 组持久事实 | C 唯一发布；按用途含 tasks/nodes/devices/rccl_plan/execution/cleanup/timeout/logs/ib，原始测试输出不入 JSON |
| `.state/prepared/<group>/slotsfile` | 组 MPI 分配 | 同组跨子项复用；历史及非 GPU 操作仍兼容 hostfile |
| `.state/prepared/task-guard.sh`、组 rank 脚本 | 实际命令引用的必要脚本 | 公共 guard 派发前由 C 发布一次；rank 相同正文复用，变化保留不可变版本 |
| `.state/prepared/<group>/devices/<node>.json` | 设备读取虚拟定位路径 | 新运行解析 facts.devices；历史独立记录可读；每项仍复核，不代表实际 GPU worker 计数 |
| `.state/runtime/<group>/` | 活动组固定工作区 | identity、当前交接、FIFO、准备暂存、normal/fallback attempt；完整事实发布确认后回收，同组下一项使用相同路径 |
| 根部 `group-NNN.log` | 持久原始 stdout/stderr | 每活动组 cat 为唯一归档写入方；facts.logs 保存精确范围与 SHA256 |
| `.state/report.lock` | 固定 inode 的报告锁 | 观察、原位重建及导出共用，覆盖完整派生写入周期，正常收尾也不删除 |
| dist/ | 离线构建产物 | 文件名版本相同不证明与当前源码一致 |
| hostfile、环境脚本、站点配置、真实基准 | 部署者输入 | 存储位置及公开提交边界由部署者管理 |
| third-party/ | 独立组件 | 来源、固定版本与许可需要单独维护，不按第一方生成产物删除 |

工作区正常数量按当前活动组统计，受 concurrency 限制；持久 prepared 数量按固定组统计，异常保留现场另计。facts 行数、原始日志和必要脚本版本仍可能随任务增加。未确认清理、写方未退出、日志或提交失败时保留现场并阻断同组后续派发，不能为减少文件数丢弃证据。

容器创建、重建和删除另存登录节点 `lifecycle-details.log`，不使用分阶段 `.state` 报告协议；查询与 dry-run 不生成结果目录。使用者目录示例见 [USER_GUIDE](USER_GUIDE.md#结果目录)；内部协议、五组结构示意、判读与恢复集中在 [DESIGN](DESIGN.md#结果与恢复设计)。

## 示例、脚本与测试文件

| 路径 | 功能 |
|---|---|
| `examples/baremetal-nodes.txt` | 原有裸金属节点输入示例，不代表独立公共 CLI 仍存在 |
| `examples/rdma-roce-policy.json` | RoCE 策略格式示例，供规则/保留路径使用 |
| `scripts/test.sh` | 保留的接口验证矩阵，含历史参数与报告断言；使用前需复核 |
| `scripts/build_release.sh` | Linux 离线发行包复制、法律文件纳入、清单校验和归档 |
| `scripts/build_release.ps1` | Windows 侧构建脚本；不表示工具可在 Windows 运行集群测试 |
| `tests/` | Python 单元与契约测试，覆盖解析、规则、入口和部分执行适配 |

测试文件按职责归类如下。这里只描述源码用途，不声明当前测试全部通过。

| 范围 | 文件 |
|---|---|
| 入口与公共边界 | `test_entrypoint.py`、`test_new_interface.py`、`test_launch_contract.py`、`test_cluster_run.py`、`test_extra_interfaces.py`、`test_acceptance_contract.py`、`test_base_execution_exit.py` |
| 容器与取消 | `test_container_status.py`、`test_container_preflight.py`、`test_container_ssh.py`、`test_task_control.py` |
| 资源、平台及执行 | `test_parsers.py`、`test_environment_contract.py`、`test_baremetal.py`、`test_baremetal_cluster.py`、`test_conda_mode.py`、`test_consistency_contract.py` |
| 网络规则与扩展 | `test_network_inventory.py`、`test_rdma_policy.py`、`test_rdma_protocol.py`、`test_rdma_userspace.py`、`test_roce_health.py`、`test_ib_counter_health.py`、`test_cluster_checks.py` |
| 报告、输出与基准 | `test_output.py`、`test_benchmark_profiles.py`、`test_result_protocol.py` |
| 组事实与工作区协议 | `test_compact_facts.py`、`test_state_commit.py`、`test_worker_handoffs.py`、`test_payload_reuse.py`、`test_controller_safety.py` |

Python 测试、Shell 协议检查、dry-run 和预检均不能替代实际 MPI rank、DeepEP 对应卡数与多机模式、完整 flux sweep、容器维护及中断清理验证。测试范围由具体改动和明确授权决定，不能为文档整理默认触发硬件测试。

## 顶层文件职责

| 文件 / 目录 | 约定 |
|---|---|
| `VERSION` | 当前源码/离线包版本；文档同步不自行升版 |
| `pyproject.toml` | Python ≥3.10、console entrypoint、Python 包及 Shell/payload 资源 |
| `setup.py` | 构建时临时纳入仓库级 LICENSE、NOTICE、THIRD_PARTY_NOTICES.md |
| `MANIFEST.release` | 离线发布白名单，含 docs、cluster_run、hcu_envcheck 等；不自动包含 third-party |
| `install.sh` | 检查发行清单后复制到版本目录并建立唯一命令链接；不使用 pip 或网络安装依赖 |
| `.gitattributes`、`.gitignore` | 换行、生成产物与私有站点配置管理 |
| `third-party/DeepEP`、`third-party/flux` | 独立组件源码/版本与许可；不能按原创代码覆盖其声明 |

发布布局、依赖和许可约定见 [DESIGN](DESIGN.md#扩展与发布边界)。

## 修改定位

| 修改目的 | 优先阅读 / 修改位置 | 文档同步 |
|---|---|---|
| 操作或参数 | dispatch、实际路由校验 | DESIGN 参数参考，USER_GUIDE 命令 |
| 分组、并发、阶段 | controller、worker | DESIGN 分组与调度 |
| 组事实、逐项提交与回收 | controller、state_facts、state_runtime、worker、reporting/facts、group_logs | DESIGN 写入责任与日志收尾，本文运行产物；同步新旧读取、打包及异常夹具 |
| 载荷启动 | worker、相应 payload、task_guard | DESIGN 载荷设计，本文文件表 |
| G/C 与设备选择 | controller、worker、payloads/device_selection、reporting/scale、rerun_plan | DESIGN 规模与设备选择、USER_GUIDE 示例、本文模块职责 |
| 采集与规则 | pod_probe、environment/preflight、reporting parser | DESIGN 判读，KNOWN_LIMITATIONS |
| 容器管理 | Bash lifecycle；保留 Python 调用方另行核对 | DESIGN 生命周期设计 |
| 报告与恢复 | reporting、remote、rerun_plan | DESIGN 协议与恢复，USER_GUIDE 结果目录 |
| 文件迁移 / 删除 | 所有导入、Shell 路径、测试与分发资源 | 本文目录和职责表；不得仅按文件名判断无用 |
| 分发与安装 | MANIFEST、package-data、构建脚本、install | DESIGN 发布边界 |

完整变更约定见 [AGENTS](../AGENTS.md) 与 [DESIGN](DESIGN.md#变更同步)。文档描述当前源码用途，既有缺陷和冻结问题继续独立管理。

- hcu_envcheck/idle_check.sh：idle-check 的宿主机 SSH 批量控制与证据汇总。
- hcu_envcheck/payloads/idle_check_node.sh：HCU 零值判定、PID 归属、容器停止和复查。
