# 模型并行拓扑转换工具

`topo_to_json.py` 将训练程序打印的模型并行进程组转换成结构化 JSON，供
trace 分析、故障 rank 归属和通信组查询使用。工具只依赖 Python 标准库。

## 1. 准备 `topo.txt`

每行格式为：

```text
进程组名称_group: [[rank, rank, ...], [rank, rank, ...]]
```

例如：

```text
tp_group: [[0, 1, 2, 3], [4, 5, 6, 7]]
pp_group: [[0], [1], [2], [3], [4], [5], [6], [7]]
dp_group: [[0, 4], [1, 5], [2, 6], [3, 7]]
ep_group: [[0, 2], [1, 3], [4, 6], [5, 7]]
etp_group: [[0, 1], [2, 3], [4, 5], [6, 7]]
cp_group: [[0], [1], [2], [3], [4], [5], [6], [7]]
```

列表可以跨多行。空行、以 `#` 开头的注释会被忽略。

也可以在文件中显式声明规模：

```text
world_size: 8
tp_size: 4
pp_size: 1
cp_size: 1
ep_size: 2
etp_size: 2
```

这些声明不是必需的；程序默认从进程组自动推导，并会检查声明值与进程组是否一致。

## 2. 执行转换

进入本目录后，直接执行：

```bash
python3 topo_to_json.py
```

默认读取当前目录的 `topo.txt`，生成当前目录的 `topo.json`。

也可以指定输入和输出文件：

```bash
python3 topo_to_json.py /path/to/topo.txt -o /path/to/topology.json
```

显式校验 world size：

```bash
python3 topo_to_json.py topo.txt -o topo.json --world-size 64
```

## 3. 输出格式

```json
{
  "schema_version": 1,
  "world_size": 8,
  "parallel_sizes": {
    "tp": 4,
    "pp": 1,
    "cp": 1,
    "ep": 2,
    "etp": 2
  },
  "groups": {
    "tp": [
      [0, 1, 2, 3],
      [4, 5, 6, 7]
    ],
    "dp": [
      [0, 4],
      [1, 5],
      [2, 6],
      [3, 7]
    ]
  }
}
```

输入中的所有 `*_group` 都会写入 `groups`，只去掉末尾的 `_group`。例如：

- `tp_group` 转成 `groups.tp`
- `tp-cp_group` 转成 `groups["tp-cp"]`
- `embd-pp_group` 转成 `groups["embd-pp"]`

`world_size` 默认按输入中最大 rank 加一推导。因此，如果输入只包含
`0～31`，输出一定是 `world_size: 32`；如需 `world_size: 64`，输入必须完整包含
`0～63`。

## 4. 输入校验

遇到以下情况，程序返回退出码 `2`，并且不会生成不完整的 JSON：

- 输入不是二维整数列表
- 出现负数 rank
- 同一种进程组重复包含同一 rank
- `tp/dp/pp/cp/ep/etp/edp` 没有覆盖全部 rank
- 声明的并行度与进程组长度不一致
- `tp × pp × cp × dp` 与 `world_size` 不一致
- 显式 `world_size` 与实际 rank 范围不一致

## 5. 运行测试

```bash
python3 -m unittest -v tests.test_topo_to_json
```
