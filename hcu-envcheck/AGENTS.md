<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# hcu-envcheck 工程约定

本文件约束工程维护和自动化协作。命令与结果目录见 [USER_GUIDE](docs/USER_GUIDE.md)，总体架构及运行约定见 [DESIGN](docs/DESIGN.md)，目录和文件职责见 [PROJECT_STRUCTURE](docs/PROJECT_STRUCTURE.md)，当前缺陷见 [KNOWN_LIMITATIONS](docs/KNOWN_LIMITATIONS.md)。以下接口和不变量是修改时需要维持的约定，不代表现有缺陷已经修复。

## 公共接口与修改边界

- 唯一公共命令为 `hcu-cluster-run <scenario> <operation>`。源码启动器和安装后的 console entrypoint 均将公共命令交给 `cluster_run/dispatch.sh`；公共操作清单只在该文件维护。
- 三个场景是 shared-conda、node-local-conda、per-node-container；共 13 个公共操作，model 已退出公共清单。节点申请在外部完成，不把旧内部类写成当前 Kubernetes 或其他公共能力。
- 核心检查、主动测试与 IB 走 Bash controller；容器管理走 Bash lifecycle。当前 rerun-plan 也需要登录节点 python3。
- 不将 Python 内部帮助参数自动开放给 Bash。保留模块仍有调用方时，不仅凭“旧实现”删除；consistency、IB 的 Python 依赖尤其需要核对。
- 不执行未经任务授权的提交、推送、部署、容器销毁或硬件测试。遵循用户对文档、代码、测试及真实服务器验证范围的最新明确要求。

## 参数、环境和分组

- RCCL/DeepEP 统一使用 group-size G（每组节点数，默认 1）与 gpus-per-node C（每节点卡数，默认 8），均为正整数、含义独立。拒绝旧公共 slots、nproc-per-node 和用户 hostfile 的 slots 字段；controller 在组目录生成 slotsfile，逐行写 node slots=N（RCCL 为 C，DeepEP 为 1），多机 MPI 直接使用该文件，载荷不再生成临时 MPI hostfile。
- RCCL/DeepEP 在各目标环境加载后，通过 payloads/device_selection.py 查询 HIP 可见设备，保留已有允许范围及顺序，选前 C 张并保存各节点 PCI 身份；执行前复用并复核记录。不在登录节点探测 GPU，不将组首节点的设备列表作为所有节点的选择。
- RCCL/DeepEP 只执行 hostfile 顺序中的完整组，尾部余数不预检、不派发、不清理，不进入任务分母、NA 或失败清单；零完整组在实际派发前失败，dry-run 同样适用。
- 已计划组出现节点故障不能缩组、补节点或调整计划分母。单节点检查与 byte-flux 的 group-size 为 1；IB 每组至少两节点，不能套用余数跳过规则。
- concurrency 是活跃节点/组任务上限；Bash ≥5.1 支持滚动补位，旧版本部分路径按批等待。node-concurrency 是控制 SSH 额度，不能代替 MPI 连接与清理总预算控制。
- env-script 仅在目标环境 source，目标工作目录先切换；controller-env-script 仅供保留的登录节点 Python 路径。禁止猜测脚本或解析 env.sh 文本推断运行环境。
- 工程、组节点文件（RCCL/DeepEP 为 slotsfile）、配置及输出必须在目标环境共享可见；工程外路径不自动翻译挂载前缀。参数按 argv 引用传递，不使用 eval。

## 载荷与容器

- worker.sh 是登录节点上的单组远程适配器；探针、payload、MPI/torchrun 与实际算子在计算节点目标环境运行。
- RCCL 常规入口固定完整十项，路径、通信与测量配置从目标 env-script 的受支持环境变量读取。内部阶段选择与历史重跑保持原机制。测试项在外、分组并发在内；单机直接二进制，多机只保留一层 MPI，每卡一个 rank，基准按参与总卡数匹配。
- DeepEP 经 run_deepep_test.sh：单机执行一次 intranode，当前允许 C=2/4/8；多机分别执行一次 lowl、norm，C 必须为 8，G×C 须同时整除默认专家数 256 和 288，当前完整多机组限 G=2/4。限制在远程预检前检查，不跳过 norm 或修改第三方测例扩大范围。
- DeepEP 多机内部 slots=1、MPI np=G，每节点一个 MPI 控制 rank → torchrun --nproc-per-node=1 → 测例 --num-processes C → 上游 spawn C 个 GPU worker。RCCL 单机 -g C、多机 slots=C、np=G×C；不得混淆控制进程与 GPU 工作进程。
- byte-flux 为单机 TP4、64 项矩阵；AG N=10240/57344，GemmRS 形状以 payload 为准，每组执行一次完整 sweep。工具不额外重复 DeepEP 或 byte-flux 测试，保留上游测例自身的 warmup 和测量迭代。
- 检测中的 image 只核对已有容器，不隐式 pull/create/recreate。容器创建使用明确端口和挂载；重建/删除实际执行需要 yes。不得把 Python lifecycle 行为套用为 Bash 已具备能力。
- 所有改动应保持本次 token 的进程隔离与逐节点清理；禁止宽泛按进程名 pkill。远端停止无法确认时保留 UNCONFIRMED，并阻断该组后续执行，不把 SSH 退出当作停止证明。

## 结果、文档与验证

- 分别记录执行状态、健康/正确性/性能结论、覆盖和清理；未知、缺失、NA、BLOCKED、取消及超时不计通过。采集退出 0 不能代替健康判定。
- platform/resource 已连接健康规则及静态一致性，但规则入口和缺失覆盖有局限；文档不可描述成“仅采集”，也不可声称完整站点策略已实现。
- 报告在可选择的目标节点环境运行 Python ≥3.10；默认第一个 hostfile 节点。新 Bash 运行使用 `compact-group-v2`：组事实集中到 `.state/prepared/<group>/facts.json`，按用途保存 `fields+rows`，原始测试输出仅在根部组日志。分组与阶段计划各保存一份；旧 `.state`、hostfile、日志索引和逐任务标记继续兼容读取，不能批量转换历史运行。
- controller 主进程 C 是 `facts.json` 唯一发布者。目标环境 `state_facts.py` 只生成唯一命名、关闭并校验的候选；C 核对身份、revision、SHA256 后原子发布。worker、节点、watchdog 和 reporter 不修改持久事实；reporter 只写派生报告，不补跑。重跑创建新目录和 token，保留原证据。
- 每个活动组只用固定 `.state/runtime/<group>/`；该组下一子项复用同一路径，不按测试项积累执行目录。正常清理、兜底清理按来源和 attempt 独立交付；所有交接包含 group/stage/phase/token。必须先 join 执行、SSH、deadline witness、watchdog、清理及日志收集写方，再校验并提交，token 校验不能代替等待。
- 新运行的阶段 task/node-results/execution 等路径是 facts 的逻辑读取地址，不要求存在对应物理文件。报告不创建未来阶段执行目录。公共报告、原位重建和导出使用同一个稳定 `report.lock` 覆盖完整写入周期；不能 unlink、截断或原子替换该活动锁文件。
- reporting/scale.py 统一计划规模与有证据的历史规模转换；计划卡数、MPI ranks、DeepEP worker 数与实际执行证据分开记录。新 GPU 重跑计划保存 G/C；旧 slots 含义不明时拒绝猜测，不能为历史计划重新开放旧 CLI 参数。
- 当前已有超时、清理、生命周期、解析完整性、重跑和许可收尾问题。维护者必须保留未解决状态，不能因改文档、dry-run、阻断或部分真实运行而宣称全部通过。
- 文档公开示例使用通用账号/节点/镜像/路径；真实站点配置、私有性能基准和验收材料置于工程外。原创文档保留仓库版权与 SPDX 声明，不覆盖第三方通知。
- README 只维护定位、能力和最短入口；USER_GUIDE 保留各项命令示例、必要的可选参数速查与结果目录。环境准备、完整参数语义、架构、流程及结果判读集中在 DESIGN，文件职责集中在 PROJECT_STRUCTURE，缺陷集中在 KNOWN_LIMITATIONS。cluster_run/GUIDE 只作导航，不维护第二套命令。改接口时同步对应章节和链接。
- scripts/test.sh 和部分历史测试有旧断言，先核对当前接口及授权范围；单测、静态检查或计划验证不能替代实际硬件链路证据。报告必须明确说明做了哪些验证、哪些未执行。

- `.state` 脚本缓存、逐项工作区回收与运行收尾由 state_runtime.sh 统一实现。只有 C 确认完整事实已发布且各写方已退出，才回收本项白名单临时文件；普通 FAIL 或有完整证据且清理确认的 TIMEOUT 可以回收。清理未确认、仍有写方、证据不一致、日志或提交失败时保留现场并阻断该组后续派发。存在性标记必须删除，不能仅清空。运行及报告结束后再回收公共运行期文件，稳定 report.lock、事实、slotsfile、原始日志及命令引用的必要脚本保留。

- 新运行的 `control.tsv` 标记 `state_layout=compact-group-v2`、`log_format=direct-group-v3`。每活动组的开始、命令、原始 stdout/stderr 和结束共用 FIFO 通道，唯一 cat 收集进程追加根部组日志；worker、目标 Python 和载荷不得旁路打开根日志追加。facts 只记录日志的精确 offset/length/SHA256 与逻辑 source，不逐行重写 JSON。历史日志索引和 direct-log 指针仍须可读。
- RCCL/DeepEP 的设备选择持久化在 facts.devices；`prepared/<group>/devices/<node>.json` 是执行读取的虚拟定位路径，历史独立 JSON 仍可读。每次目标 shell/rank 仍加载 env-script、复核可见性/权限/PCI 身份及实际二进制、参数和基准。组 rank 正文相同时复用，变化时保留不可变版本；公共 guard 在派发前由 C 发布一次。禁止以复用为由减少动态检查、改变 argv、隐藏原始错误或缩减测试矩阵。

- idle-check 走宿主机独立 Bash 路径；--release 明确授权停止占用 HCU 的 Docker 容器，控制与证据不得依赖释放目标容器。
