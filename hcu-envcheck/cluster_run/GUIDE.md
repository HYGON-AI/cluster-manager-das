<!--
Copyright (c) 2026 Hygon Information Technology Co., Ltd.
SPDX-License-Identifier: Apache-2.0
-->

# 控制层文档导航

核心操作的可复制命令集中在 [USER_GUIDE](../docs/USER_GUIDE.md)，本页保留为原 GUIDE 路径的导航入口。

| 操作 / 内容 | 命令或目录 | 设计说明 |
|---|---|---|
| 容器查询、创建、重建、删除 | [命令](../docs/USER_GUIDE.md#容器操作) | [生命周期](../docs/DESIGN.md#容器生命周期设计) |
| platform | [命令](../docs/USER_GUIDE.md#platform) | [平台规则](../docs/DESIGN.md#平台资源与前置环境) |
| resource、联合检测及 Conda 场景 | [命令](../docs/USER_GUIDE.md#resource) | [资源规则](../docs/DESIGN.md#平台资源与前置环境)、[场景配置](../docs/DESIGN.md#附录场景与环境脚本) |
| checkdeepep | [命令](../docs/USER_GUIDE.md#checkdeepep) | [前置检查](../docs/DESIGN.md#平台资源与前置环境) |
| RCCL 单机 / 多机 | [命令](../docs/USER_GUIDE.md#rccl) | [载荷与基准](../docs/DESIGN.md#rccl) |
| DeepEP 单机 / 多机 | [命令](../docs/USER_GUIDE.md#deepep) | [节点数、卡数与模式](../docs/DESIGN.md#deepep) |
| RCCL / DeepEP 规模与设备选择 | [参数速查](../docs/USER_GUIDE.md#rccl) | [G/C 与设备复核](../docs/DESIGN.md#测试规模与设备选择) |
| byte-flux | [命令](../docs/USER_GUIDE.md#byte-flux) | [TP4 矩阵](../docs/DESIGN.md#byte-flux) |
| 报告重建、导出与重跑预览 | [命令](../docs/USER_GUIDE.md#报告重建导出与重跑预览) | [重建](../docs/DESIGN.md#报告重建与导出)、[重跑](../docs/DESIGN.md#重跑计划) |
| 结果文件位置 | [结果目录](../docs/USER_GUIDE.md#结果目录) | [结果协议与判读](../docs/DESIGN.md#结果与恢复设计) |
| `.state` 组事实、固定工作区与写入职责 | [维护文件定位](../docs/PROJECT_STRUCTURE.md#运行产物与源码) | [协议与唯一发布者](../docs/DESIGN.md#执行事实与写入责任)、[日志与回收](../docs/DESIGN.md#分组日志与临时状态收尾) |

初次部署见 DESIGN [环境准备](../docs/DESIGN.md#附录环境准备)，参数见[参数参考](../docs/DESIGN.md#附录参数参考)，代码定位见 [PROJECT_STRUCTURE](../docs/PROJECT_STRUCTURE.md)，当前限制见 [KNOWN_LIMITATIONS](../docs/KNOWN_LIMITATIONS.md)。
