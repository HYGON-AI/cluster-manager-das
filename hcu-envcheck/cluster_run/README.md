<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# cluster_run 控制层

`cluster_run` 负责公共入口分流、节点分组、阶段调度和远程报告启动。源码入口 `bin/hcu-cluster-run` 与安装后的 console script 均将公共命令交给 `dispatch.sh`；`python -m cluster_run` 是保留的 Python 内部入口，其中 RCCL/DeepEP 也转交同一 Bash controller，使用统一的 G/C 参数和设备选择协议。

| 组件 | 职责 | 执行位置 |
|---|---|---|
| `dispatch.sh` | 维护三个场景和 14 个操作的公共清单，路由请求 | 登录节点 |
| `controller.sh` | 生成完整分组与阶段计划，调度 prepare / execute / cleanup，写入事实文件 | 登录节点 Bash |
| `remote.sh` | 在选定报告节点启动报告进程 | 登录节点发起 SSH，目标环境运行 Python |
| `../hcu_envcheck/worker.sh` | 构造并执行单组远程命令，收集节点结果与清理证据 | 适配器在登录节点，任务在计算节点 |
| `../hcu_envcheck/lifecycle.sh` | 四项 Docker 容器管理操作 | 登录节点调度计算节点宿主机 |
| `cli.py`、`active.py` 等 Python 模块 | 完整帮助及内部兼容实现 | 依调用路径执行 |
| `rerun_plan.py` | 校验导出清单并恢复原组、项目与规模；GPU 规模通过 reporting/scale.py 解析 | 登录节点 Python |

`consistency.py` 被当前报告路径复用；`extra_checks.py` 被 IB 复用。不能仅凭模块位于旧 Python 控制路径就认定其没有调用方，也不能把它们的全部接口当作公共入口能力。

RCCL/DeepEP 使用 `--group-size` 指定节点数、`--gpus-per-node` 指定每节点卡数。完整语义、设备预检及支持范围集中在 [DESIGN](../docs/DESIGN.md#测试规模与设备选择)，不在此维护另一套参数表。

阅读入口：

- [命令示例与结果目录](../docs/USER_GUIDE.md)：核心操作命令、单机/多机示例和结果位置。
- [总体设计与运行约定](../docs/DESIGN.md)：部署、架构、调度、参数与结果判读。
- [文档导航](GUIDE.md)：按操作查找统一命令与设计章节。
- [工程结构与文件职责](../docs/PROJECT_STRUCTURE.md)：本目录各模块、载荷与报告文件的用途和调用方。
- [能力与验证边界](../docs/KNOWN_LIMITATIONS.md)：当前实现与生产验收之间的差距。

新增或修改操作时，应维护 `dispatch.sh` 的唯一公共清单、对应路由的参数边界和文档。保留的内部 Python 帮助不等同于 Bash 参数全集。
