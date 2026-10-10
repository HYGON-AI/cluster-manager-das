# trace_analyzer：PyTorch/NCCL Flight Recorder 独立分析工具

`trace_analyzer` 用于离线分析多 rank 的 PyTorch/NCCL Flight Recorder（以下简称
FR）转储文件，帮助定位分布式训练中的集合通信卡住、rank 进度不一致和疑似故障
rank。

## 来源与许可

本目录中的 `trace_analyzer` 是在 NVIDIA 开源项目
[NVIDIA/nvidia-resiliency-ext](https://github.com/NVIDIA/nvidia-resiliency-ext)
的 [`v0.6.0` 发布版本](https://github.com/NVIDIA/nvidia-resiliency-ext/releases/tag/v0.6.0)
中 Flight Recorder/trace analyzer 代码基础上修改和扩展的。
本工具不是完全从零开发，也不是 NVIDIA 官方发行版本。

- 上游项目版权归 NVIDIA Corporation 及其关联方所有；
- 上游代码采用
  [Apache License 2.0](https://github.com/NVIDIA/nvidia-resiliency-ext/blob/v0.6.0/LICENSE.txt)
  许可证；
- 本项目保留源文件中的 NVIDIA 原始 SPDX 版权及许可证声明；
- 本项目相对上游版本增加或调整了独立运行适配、受限 pickle 加载、严格多数派进度
  判断、结构化候选 rank、模型并行拓扑转换与消歧、测试和中文文档等功能；
- 本项目中的修改由本项目维护者负责，不代表 NVIDIA 的官方实现、支持或背书。

使用、复制、修改或重新发布本目录代码时，应遵守 Apache License 2.0，并继续保留：

1. NVIDIA 原始版权声明；
2. Apache License 2.0 许可证文本；
3. 上游提供的 NOTICE 文件（如有）；
4. 本节关于代码来源、基线版本和修改情况的说明。

它可以完成：

- 同时读取多个 rank 的 FR 转储；
- 对齐各 rank 的集合通信进度；
- 找出进程组中缺失、落后或超前的 rank；
- 按证据强弱给出候选 rank 列表；
- 输出便于人工查看的表格，或便于程序消费的 JSON；
- 可选加载 TP、DP、PP、CP、EP、ETP、EDP 等模型并行拓扑，解决不同 rank
  中数字 Process Group ID 重复或含义不一致的问题。

需要注意：本工具给出的是**通信异常线索和候选 rank**，不会直接证明某个节点
硬件损坏，也不会自动恢复训练。最终结论仍应结合训练日志、节点健康检查和网络检查。

## 1. 最快上手

### 1.1 准备 FR 文件

在启动训练进程前，先设置以下环境变量以开启 PyTorch/NCCL Flight Recorder
记录，并在 NCCL 报错时导出转储文件：

```bash
export TRACE_DIR=/path/to/checkpoints
export TORCH_NCCL_DEBUG_INFO_TEMP_FILE=${TRACE_DIR}/trace_rank_
export TORCH_NCCL_DUMP_ON_TIMEOUT=1
export TORCH_NCCL_TRACE_BUFFER_SIZE=2000
```

其中，`TRACE_DIR` 应为已存在且所有 rank 都可写入的目录。
`TORCH_NCCL_DEBUG_INFO_TEMP_FILE` 是文件名前缀；PyTorch 会在其后附加 rank
编号，生成类似 `trace_rank_0`、`trace_rank_1` 的 FR 文件。
`TORCH_NCCL_DUMP_ON_TIMEOUT=1` 表示发生 NCCL 超时时导出记录，
`TORCH_NCCL_TRACE_BUFFER_SIZE=2000` 表示每个 rank 保留最近 2000 条 NCCL
通信事件。

典型的二进制转储目录如下：

```text
/path/to/checkpoints/
├── _dump_0
├── _dump_1
├── _dump_2
└── _dump_3
```

JSON 转储也可以使用：

```text
/path/to/checkpoints/
├── trace_rank_0.json
├── trace_rank_1.json
├── trace_rank_2.json
└── trace_rank_3.json
```

文件名最后一个下划线后的数字会被识别为 rank。例如 `_dump_7` 和
`trace_rank_7.json` 都代表 rank 7。

### 1.2 运行分析

进入 `trace_analyzer` 目录：

```bash
cd /public/home/zhaoyu/zhougf/code/env_check/dcu_cluster_check/trace_analyzer
```

分析默认的 `_dump_*` 文件，并输出人类可读的异常表格：

```bash
python3 fr_attribution.py \
  --fr-path ${TRACE_DIR} \
  --emit-stdout \
  --stdout-format table
```

如果文件名是 `trace_rank_*.json`，需要指定匹配规则：

```bash
python3 fr_attribution.py \
  --fr-path ${TRACE_DIR} \
  --pattern trace_rank_* \
  --emit-stdout \
  --stdout-format table
```

其中--pattern 是文件名rank的前缀

### 1.3 获取候选故障 rank

只输出候选 rank 的 JSON 数组：

```bash
python3 fr_attribution.py \
  --fr-path ${TRACE_DIR} \
  --pattern trace_rank_* \
  --emit-stdout \
  --stdout-format ranks
```

示例输出：

```json
[2, 5, 4]
```

列表按当前分析中的相对证据强弱排序，排在前面表示更值得优先排查，不代表已经确认
该 rank 一定是根因。

## 2. 支持哪些输入

`--fr-path` 支持三种形式。

### 2.1 目录

程序在目录内按照 `--pattern` 查找文件：

```bash
python3 fr_attribution.py \
  --fr-path ${TRACE_DIR} \
  --pattern _dump_* \
  --emit-stdout \
  --stdout-format json
```

`--pattern` 的默认值是 `_dump_*`。建议使用单引号包住通配符，避免 Shell
提前展开。

### 2.2 单个文件

```bash
python3 fr_attribution.py \
  --fr-path /path/to/checkpoints/_dump_0 \
  --emit-stdout \
  --stdout-format json
```

单文件通常只适合检查文件能否解析。通信归因需要对比多个 rank，生产排查时应尽量
提供同一次训练的完整 rank 转储。

### 2.3 文件路径前缀

如果训练日志中输出：

```text
TORCH_FR_DUMP_TEMP_FILE=/tmp/checkpoints/_dump_
```

可以直接把该前缀传给 `--fr-path`：

```bash
python3 fr_attribution.py \
  --fr-path /tmp/checkpoints/_dump_ \
  --emit-stdout \
  --stdout-format json
```

程序会匹配 `/tmp/checkpoints/_dump_*`。

### 2.4 FR 文件格式要求

- 文件名以 `.json` 结尾时，按 JSON 读取；
- 其他文件按二进制 pickle 转储读取；
- 转储内容应包含 PyTorch FR 的 `entries` 和 `pg_status`；
- 如果包含 `pg_config`，程序会同时读取进程组配置；
- 当前分析主要使用状态为 `scheduled` 的集合通信记录。

如果部分 rank 的文件没有收集到、来自不同训练任务，或者文件仍在写入，分析结果可能
把“输入不完整”表现成“rank 缺失”。分析前应先确认所有文件来自同一次训练。

## 3. 使用模型并行拓扑提高准确性

### 3.1 为什么需要拓扑文件

某些训练框架在不同 rank 的 FR 中可能复用相同的数字 Process Group ID。只依赖数字
ID 时，两个实际不同的 TP/DP/EP 通信组可能被错误合并，从而产生多个错误候选 rank。

提供模型并行拓扑后，分析器会按照真实 rank 成员关系拆分这些通信组，再比较组内进度。

### 3.2 准备 `topo.txt`

示例：

```text
tp_group: [[0, 1, 2, 3], [4, 5, 6, 7]]
dp_group: [[0, 4], [1, 5], [2, 6], [3, 7]]
pp_group: [[0], [1], [2], [3], [4], [5], [6], [7]]
cp_group: [[0], [1], [2], [3], [4], [5], [6], [7]]
ep_group: [[0, 2], [1, 3], [4, 6], [5, 7]]
etp_group: [[0, 1], [2, 3], [4, 5], [6, 7]]
```

完整格式及校验规则见 `README_TOPO_CONVERTER.md`。

### 3.3 带拓扑运行分析

```bash
python3 fr_attribution.py \
  --fr-path ${TRACE_DIR} \
  --pattern trace_rank_ \
  --topo-file /path/to/topo.txt \
  --emit-stdout \
  --stdout-format json
```

`--topo-file` 和 `--topo_file` 两种写法都支持。

执行时会：

1. 校验 `topo.txt`；
2. 在同目录生成同名 JSON，例如 `topo.txt` 生成 `topo.json`；
3. 使用拓扑映射常见 TP、DP、PP、CP、EP、ETP、EDP 及组合进程组；
4. 将同一个数字 Process Group ID 中属于不同真实通信组的 rank 拆开分析；
5. 在结果的 `topology_analysis` 中记录匹配、拆分和回退情况。

拓扑文件所在目录必须可写。拓扑的 `world_size` 必须覆盖 FR 中出现的所有 rank。
某类通信组没有对应拓扑时，程序不会直接失败，而是回退到原有的 FR 分析逻辑，并在
`topology_analysis` 中记录。

不传 `--topo-file` 时，保持原有分析行为。

## 4. 如何选择输出格式

`--stdout-format` 必须与 `--emit-stdout` 一起使用。

| 格式 | 适用场景 | 输出内容 |
| --- | --- | --- |
| `table` | 人工快速查看 | 异常进程组、通信类型和缺失 rank 表格 |
| `ranks` | Shell 脚本或告警程序 | 按相对证据排序的 JSON rank 数组 |
| `json` | 平台集成、保存完整证据 | 完整结构化分析结果 |

保存完整 JSON：

```bash
python3 fr_attribution.py \
  --fr-path ${TRACE_DIR} \
  --emit-stdout \
  --stdout-format json \
  > fr-analysis.json
```

结构化输出模式会尽量避免普通日志混入标准输出；错误和警告仍会写到标准错误。

## 5. 如何理解 JSON 结果

完整 JSON 的主要字段如下。

| 字段 | 含义 |
| --- | --- |
| `analysis_text` | 异常进程组汇总表，包含 PG、操作类型、数据量、类型和缺失 rank |
| `hanging_ranks` | 兼容旧接口的文本摘要 |
| `hanging_rank_list` | 从异常表格提取并去重后的 rank |
| `candidate_rank_list` | 按相对证据排序后的候选 rank |
| `rank_assessment` | 每个候选 rank 的分数、相对置信度、涉及的 PG 和通信类型 |
| `trace_summary` | 文件数、各 rank 条目数、通信状态、操作类型和版本信息 |
| `progress_anomalies` | rank 相对多数派基线超前或落后的进度证据 |
| `topology_analysis` | 拓扑匹配和回退统计；仅使用 `--topo-file` 时存在 |

### 5.1 `rank_assessment.recommendation`

可能出现：

- `high_confidence_single_candidate`：只有一个候选，或首个候选在当前证据中的相对
  占比至少为 70%，建议优先排查；
- `ambiguous_candidates_need_additional_evidence`：存在多个接近的候选，需要结合训练
  日志、节点状态和网络检查继续判断。

`confidence` 是本次候选之间的**相对证据占比**，不是硬件故障概率。

### 5.2 为什么采用严格多数派

进度判断以严格多数派为基线：

- 大多数 rank 在同一进度、少数 rank 不一致时，少数 rank 被记录为异常；
- 某个 rank 落后或超前，都会保留在 `progress_anomalies` 中；
- 如果各进度没有严格多数派，结果标记为歧义，不会随意把某一侧判为根因。

这样可以避免“单个 rank 跑得更快，反而把其余大多数 rank 都判成异常”的情况。

### 5.3 常见判断方式

1. `candidate_rank_list` 只有一个 rank：优先检查该 rank 对应的节点、进程和网络；
2. 候选很多且 recommendation 为歧义：先检查输入是否完整，再结合其他监控；
3. `trace_summary.entry_count_outlier_ranks` 有值：说明部分 rank 的转储条目数偏离多数派，
   这是辅助线索，不应单独作为根因；
4. `candidate_rank_list` 为空：表示当前 FR 没找到确定性缺失证据，不等于训练和硬件一定
   正常。

## 6. 常用诊断参数

### 6.1 查看详细处理过程

```bash
python3 fr_attribution.py \
  --fr-path ${TRACE_DIR} \
  --verbose \
  --emit-stdout \
  --stdout-format table
```

### 6.2 将二进制转储同时转换成 JSON

```bash
python3 fr_attribution.py \
  --fr-path ${TRACE_DIR} \
  --debug \
  --emit-stdout \
  --stdout-format json
```

`--debug` 会为成功读取的二进制文件生成相邻的 `.json` 文件，例如 `_dump_0.json`。
源目录必须可写，并应预留足够磁盘空间。输入本身已经是 JSON 时不会重复转换。

### 6.3 查看全部参数

```bash
python3 fr_attribution.py --help
```

## 7. 在 Python 代码中调用

下面的示例应从 `dcu_cluster_check` 目录运行，或者先把该目录加入
`PYTHONPATH`，确保 `trace_analyzer` 作为 Python 包导入。

异步接口示例：

```python
import asyncio

from trace_analyzer import TraceAnalyzer


async def main():
    analyzer = TraceAnalyzer(allowed_root="/path/to/run")
    result = await analyzer.analyze_fr_dump(
        "/path/to/run/checkpoints",
        topo_file="/path/to/run/topo.txt",
    )
    if result is None:
        print("未获得 FR 分析结果")
        return
    print("候选 rank：", result.candidate_rank_list)
    print("判断建议：", result.rank_assessment.get("recommendation"))


asyncio.run(main())
```

`allowed_root` 用于限制从日志发现的 FR 路径范围，适合服务端集成。
`TraceAnalyzer.discover_fr_dump_path(log_path)` 还可以：

- 优先查找 `<run>/logs/...` 对应的 `<run>/checkpoints`；
- 从训练日志前 1000 行中读取 `TORCH_FR_DUMP_TEMP_FILE=...`。

## 8. 目录中各文件的作用

| 文件 | 作用 |
| --- | --- |
| `fr_attribution.py` | 核心分析逻辑和命令行入口 |
| `fr_support.py` | FR 路径发现、统一结果对象、数据流和 Markdown 辅助函数 |
| `trace_analyzer.py` | 面向其他 Python 模块的异步封装 |
| `topo_to_json.py` | 将 `topo.txt` 转成标准 JSON 拓扑 |
| `README_TOPO_CONVERTER.md` | 拓扑文件编写和转换说明 |
| `trace_collector.py` | 从运行中的 PyTorch 分布式进程采集 FR |
| `capture.py` | 捕获日志和标准输出的辅助函数 |
| `standalone_support.py` | 独立运行所需的轻量执行框架 |
| `tests/` | 单元测试 |

## 9. 依赖与安全说明

- 分析已经生成的 FR 文件只依赖 Python 标准库；
- 建议使用 Python 3.9 或更高版本；
- `trace_collector.py` 需要安装 PyTorch，并要求分布式环境已经初始化；
- 分析功能不要求安装 `nvidia-resiliency-ext`；
- 二进制 pickle 使用受限反序列化器，只接受普通数据结构，拒绝构造任意 Python
  对象；
- 不建议分析来源不明或不可信的转储文件；
- 本工具不调用大模型，也不需要 API Key。

## 10. 常见问题

### 10.1 报错 `No files ... were processed successfully`

依次检查：

1. `--fr-path` 是否存在；
2. 目录输入时，`--pattern` 是否能匹配实际文件名rank前缀；
3. 文件名末尾是否能正确提取 rank；
4. JSON 是否完整，二进制文件是否为支持的 FR 转储；
5. 文件是否仍在写入，当前用户是否有读取权限。

可以先执行：

```bash
find ${TRACE_DIR} -maxdepth 1 -type f -name '_dump_*' | sort
```

### 10.2 表格没有内容

表格只显示检测到的异常进程组。建议改用完整 JSON 查看输入统计：

```bash
python3 fr_attribution.py \
  --fr-path ${TRACE_DIR} \
  --emit-stdout \
  --stdout-format json
```

重点检查 `trace_summary.rank_count`、`scheduled_collective_count` 和
`entry_count_by_rank`。表格为空不代表训练一定正常。

### 10.3 候选 rank 太多

先确认所有转储来自同一次训练且 rank 文件齐全。如果数字 Process Group ID 在不同
rank 中发生碰撞，补充 `--topo-file` 后重新分析。

### 10.4 拓扑校验失败

检查：

- `topo.txt` 是否覆盖 trace 中的全部 rank；
- `world_size` 是否正确；
- 同一种 group 是否重复包含 rank；
- TP、DP、PP、CP 等并行规模是否自洽。

详细规则见 `README_TOPO_CONVERTER.md`。

## 11. 运行测试

测试同时包含包导入和独立脚本导入，因此需要从 `dcu_cluster_check` 目录执行，并把
项目目录和 `trace_analyzer` 目录加入 `PYTHONPATH`：

```bash
cd /public/home/zhaoyu/zhougf/code/env_check/dcu_cluster_check

PYTHONPATH="$PWD:$PWD/trace_analyzer" \
PYTHONDONTWRITEBYTECODE=1 \
python3 -B -m unittest -v \
  trace_analyzer.tests.test_fr_attribution \
  trace_analyzer.tests.test_topo_to_json
```

测试覆盖严格多数派判断、候选 rank 排序、受限 pickle 加载、拓扑消歧、拓扑转换和
输入校验。
