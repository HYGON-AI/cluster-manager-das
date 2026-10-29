<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# hcu-envcheck 文档索引

本文档集对应当前 `0.4.2` 源码。命令、设计、文件职责和已知限制分别维护，避免同一参数或规则出现多份定义。

| 目的 | 文档 | 内容 |
|---|---|---|
| 了解项目 | [README](../README.md) | 定位、能力、最短入口 |
| 复制命令、查结果位置 | [USER_GUIDE](USER_GUIDE.md) | 容器、platform/resource、checkdeepep、RCCL/DeepEP、byte-flux 命令与结果目录 |
| 理解部署与设计 | [DESIGN](DESIGN.md) | 总体架构、流程、分组调度、配置、参数、结果协议及排障 |
| 阅读或维护源码 | [PROJECT_STRUCTURE](PROJECT_STRUCTURE.md) | 子目录、文件用途、模块依赖和修改定位 |
| 评估当前可用范围 | [KNOWN_LIMITATIONS](KNOWN_LIMITATIONS.md) | 既有缺陷、未验证范围与证据要求 |
| 从控制层查文档 | [cluster_run README](../cluster_run/README.md)、[GUIDE](../cluster_run/GUIDE.md) | 控制层定位和统一章节导航 |
| 贡献或自动化协作 | [AGENTS](../AGENTS.md)、[仓库贡献规范](../../CONTRIBUTING.md) | 修改边界、接口契约、文档同步及公开提交要求 |

首次部署：README → DESIGN 的[环境准备](DESIGN.md#附录环境准备)和[场景配置](DESIGN.md#附录场景与环境脚本) → USER_GUIDE 所需命令 → DESIGN [结果判读](DESIGN.md#结果与恢复设计)及能力边界。开发者先看 PROJECT_STRUCTURE 定位文件，再看 DESIGN 对应约定。

RCCL/DeepEP 的统一 G/C 参数、目标设备选择和限制见 [测试规模与设备选择](DESIGN.md#测试规模与设备选择)；两个新增模块 `device_selection.py`、`scale.py` 的职责见 [载荷目录](PROJECT_STRUCTURE.md#载荷目录)和[报告目录](PROJECT_STRUCTURE.md#报告目录)。

## 文档维护规则

- USER_GUIDE 保留命令示例、必要的可选参数速查和结果目录；DESIGN 维护完整说明，PROJECT_STRUCTURE 维护文件职责。
- 公共命令以 `cluster_run/dispatch.sh` 及实际路由为依据；Python 内部参数不自动成为 Bash 公共参数。
- 控制层 GUIDE 保留为导航，不复制第二套命令；修改默认值、分组、报告路径或状态时同步其唯一说明位置。
- 公开示例使用通用节点、镜像和路径；真实站点配置、私有基准和验收材料置于工程外。
- 区分静态核对、计划、预检、真实载荷与清理证据；文档迁移不构成问题关闭，不扩大既有冻结问题清单。
