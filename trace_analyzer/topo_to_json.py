#!/usr/bin/env python3
"""Convert model-parallel process groups in topo.txt to a JSON topology file."""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

SCHEMA_VERSION = 1
PARALLEL_SIZE_KEYS: Tuple[str, ...] = ("tp", "pp", "cp", "ep", "etp")
PARTITION_GROUPS = frozenset(("tp", "dp", "pp", "cp", "ep", "etp", "edp"))

_GROUP_LINE_RE = re.compile(
    r"^\s*(?P<name>[A-Za-z][A-Za-z0-9_-]*)_group\s*:\s*(?P<value>.*)$"
)
_SCALAR_LINE_RE = re.compile(
    r"^\s*(?P<name>world_size|tp|pp|cp|ep|etp|"
    r"tp_size|pp_size|cp_size|ep_size|etp_size)\s*:\s*(?P<value>.*?)\s*$"
)


class TopologyFormatError(ValueError):
    """Raised when topo.txt is malformed or internally inconsistent."""


def _format_rank_preview(ranks: Sequence[int], limit: int = 12) -> str:
    shown = ", ".join(str(rank) for rank in ranks[:limit])
    if len(ranks) > limit:
        shown += f", ...（共 {len(ranks)} 个）"
    return shown


def _parse_positive_integer(value: str, *, name: str, line_number: int) -> int:
    value_without_comment = value.split("#", 1)[0].strip()
    try:
        result = int(value_without_comment)
    except ValueError as exc:
        raise TopologyFormatError(
            f"第 {line_number} 行：{name} 必须是正整数，实际为 {value!r}"
        ) from exc
    if result <= 0:
        raise TopologyFormatError(
            f"第 {line_number} 行：{name} 必须大于 0，实际为 {result}"
        )
    return result


def _parse_group_literal(
    source: str, *, group_name: str, line_number: int
) -> List[List[int]]:
    try:
        raw_groups = ast.literal_eval(source)
    except (SyntaxError, ValueError) as exc:
        raise TopologyFormatError(
            f"第 {line_number} 行开始的 {group_name}_group 不是合法的二维整数列表"
        ) from exc

    if not isinstance(raw_groups, (list, tuple)):
        raise TopologyFormatError(
            f"第 {line_number} 行：{group_name}_group 必须是二维列表"
        )

    normalized: List[List[int]] = []
    seen_in_group_type = set()
    for group_index, raw_group in enumerate(raw_groups):
        if not isinstance(raw_group, (list, tuple)):
            raise TopologyFormatError(
                f"{group_name}_group 的第 {group_index} 项必须是 rank 列表"
            )
        if not raw_group:
            raise TopologyFormatError(
                f"{group_name}_group 的第 {group_index} 个分组不能为空"
            )

        group: List[int] = []
        seen_in_one_group = set()
        for rank in raw_group:
            if isinstance(rank, bool) or not isinstance(rank, int):
                raise TopologyFormatError(
                    f"{group_name}_group 的 rank 必须是整数，实际为 {rank!r}"
                )
            if rank < 0:
                raise TopologyFormatError(
                    f"{group_name}_group 中不能出现负数 rank：{rank}"
                )
            if rank in seen_in_one_group:
                raise TopologyFormatError(
                    f"{group_name}_group 的第 {group_index} 个分组重复包含 rank {rank}"
                )
            if rank in seen_in_group_type:
                raise TopologyFormatError(
                    f"{group_name}_group 的多个分组重复包含 rank {rank}"
                )
            seen_in_one_group.add(rank)
            seen_in_group_type.add(rank)
            group.append(rank)
        normalized.append(group)

    if not normalized:
        raise TopologyFormatError(f"{group_name}_group 至少需要包含一个分组")
    return normalized


def _collect_group_literal(
    lines: Sequence[str], start_index: int, first_value: str, group_name: str
) -> Tuple[str, int]:
    """Collect one possibly multi-line list literal and return (source, last_index)."""

    parts: List[str] = []
    bracket_depth = 0
    index = start_index
    current = first_value

    while True:
        if current.strip() and not current.lstrip().startswith("#"):
            parts.append(current)
            bracket_depth += current.count("[") - current.count("]")
            if bracket_depth < 0:
                raise TopologyFormatError(
                    f"第 {start_index + 1} 行开始的 {group_name}_group 方括号不匹配"
                )
            collected = "\n".join(parts)
            if "[" not in collected or bracket_depth == 0:
                break

        index += 1
        if index >= len(lines):
            raise TopologyFormatError(
                f"第 {start_index + 1} 行开始的 {group_name}_group 未结束"
            )
        current = lines[index]

    return "\n".join(parts), index


def _uniform_group_size(group_name: str, groups: Sequence[Sequence[int]]) -> int:
    sizes = {len(group) for group in groups}
    if len(sizes) != 1:
        raise TopologyFormatError(
            f"{group_name}_group 的分组长度不一致，无法推导 {group_name} 并行度："
            f"{sorted(sizes)}"
        )
    return next(iter(sizes))


def parse_topology_text(
    text: str, *, world_size_override: Optional[int] = None
) -> Dict[str, object]:
    """Parse topo.txt content and return the schema-version-1 topology mapping."""

    lines = text.splitlines()
    groups: Dict[str, List[List[int]]] = {}
    declared_world_size: Optional[int] = None
    declared_parallel_sizes: Dict[str, int] = {}

    index = 0
    while index < len(lines):
        raw_line = lines[index]
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            index += 1
            continue

        group_match = _GROUP_LINE_RE.match(raw_line)
        if group_match:
            group_name = group_match.group("name")
            if group_name in groups:
                raise TopologyFormatError(
                    f"第 {index + 1} 行：{group_name}_group 重复定义"
                )
            literal, last_index = _collect_group_literal(
                lines,
                index,
                group_match.group("value"),
                group_name,
            )
            groups[group_name] = _parse_group_literal(
                literal,
                group_name=group_name,
                line_number=index + 1,
            )
            index = last_index + 1
            continue

        scalar_match = _SCALAR_LINE_RE.match(raw_line)
        if scalar_match:
            scalar_name = scalar_match.group("name")
            scalar_value = _parse_positive_integer(
                scalar_match.group("value"),
                name=scalar_name,
                line_number=index + 1,
            )
            if scalar_name == "world_size":
                if declared_world_size is not None:
                    raise TopologyFormatError(
                        f"第 {index + 1} 行：world_size 重复定义"
                    )
                declared_world_size = scalar_value
            else:
                parallel_name = scalar_name.removesuffix("_size")
                if parallel_name in declared_parallel_sizes:
                    raise TopologyFormatError(
                        f"第 {index + 1} 行：{parallel_name} 并行度重复定义"
                    )
                declared_parallel_sizes[parallel_name] = scalar_value
            index += 1
            continue

        raise TopologyFormatError(
            f"第 {index + 1} 行无法识别：{raw_line!r}；"
            "应使用 name_group: [[...], ...] 或 world_size/name_size: N"
        )

    if not groups:
        raise TopologyFormatError("输入文件中没有找到任何 *_group")

    all_ranks = {
        rank
        for group_collection in groups.values()
        for group in group_collection
        for rank in group
    }
    if not all_ranks:
        raise TopologyFormatError("输入文件中没有找到 rank")

    inferred_world_size = max(all_ranks) + 1
    if world_size_override is not None and world_size_override <= 0:
        raise TopologyFormatError("--world-size 必须大于 0")
    if (
        world_size_override is not None
        and declared_world_size is not None
        and world_size_override != declared_world_size
    ):
        raise TopologyFormatError(
            f"--world-size={world_size_override} 与 topo.txt 中的 "
            f"world_size={declared_world_size} 不一致"
        )

    world_size = (
        world_size_override
        if world_size_override is not None
        else declared_world_size
        if declared_world_size is not None
        else inferred_world_size
    )

    expected_ranks = set(range(world_size))
    extra_ranks = sorted(all_ranks - expected_ranks)
    if extra_ranks:
        raise TopologyFormatError(
            f"存在超出 world_size={world_size} 范围的 rank："
            f"{_format_rank_preview(extra_ranks)}"
        )
    missing_globally = sorted(expected_ranks - all_ranks)
    if missing_globally:
        raise TopologyFormatError(
            f"拓扑未包含 world_size={world_size} 所需的全部 rank，缺少："
            f"{_format_rank_preview(missing_globally)}"
        )

    for group_name in PARTITION_GROUPS.intersection(groups):
        ranks_in_group_type = {
            rank for group in groups[group_name] for rank in group
        }
        missing = sorted(expected_ranks - ranks_in_group_type)
        if missing:
            raise TopologyFormatError(
                f"{group_name}_group 没有覆盖全部 rank，缺少："
                f"{_format_rank_preview(missing)}"
            )

    parallel_sizes: Dict[str, int] = {}
    for parallel_name in PARALLEL_SIZE_KEYS:
        inferred_size = (
            _uniform_group_size(parallel_name, groups[parallel_name])
            if parallel_name in groups
            else 1
        )
        declared_size = declared_parallel_sizes.get(parallel_name)
        if declared_size is not None and declared_size != inferred_size:
            raise TopologyFormatError(
                f"{parallel_name} 并行度声明为 {declared_size}，"
                f"但 {parallel_name}_group 推导结果为 {inferred_size}"
            )
        parallel_sizes[parallel_name] = (
            declared_size if declared_size is not None else inferred_size
        )

    if "dp" in groups:
        dp_size = _uniform_group_size("dp", groups["dp"])
        dense_world_size = (
            parallel_sizes["tp"]
            * parallel_sizes["pp"]
            * parallel_sizes["cp"]
            * dp_size
        )
        if dense_world_size != world_size:
            raise TopologyFormatError(
                "并行规模不一致："
                f"tp({parallel_sizes['tp']}) × pp({parallel_sizes['pp']}) × "
                f"cp({parallel_sizes['cp']}) × dp({dp_size}) = "
                f"{dense_world_size}，但 world_size={world_size}"
            )

    return {
        "schema_version": SCHEMA_VERSION,
        "world_size": world_size,
        "parallel_sizes": parallel_sizes,
        "groups": groups,
    }


def _write_json_atomic(
    output_path: Path, payload: Mapping[str, object], *, indent: int
) -> None:
    output_path = output_path.expanduser()
    output_parent = output_path.parent
    if not output_parent.exists():
        raise TopologyFormatError(f"输出目录不存在：{output_parent}")
    if not output_parent.is_dir():
        raise TopologyFormatError(f"输出路径的父路径不是目录：{output_parent}")

    temp_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=output_parent,
            prefix=f".{output_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temp_path = Path(handle.name)
            json.dump(payload, handle, ensure_ascii=False, indent=indent)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, output_path)
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink()


def convert_topology_file(
    input_path: Path,
    output_path: Path,
    *,
    world_size_override: Optional[int] = None,
    indent: int = 2,
) -> Dict[str, object]:
    """Convert one topology text file and atomically write its JSON output."""

    input_path = input_path.expanduser()
    output_path = output_path.expanduser()
    try:
        if input_path.resolve() == output_path.resolve():
            raise TopologyFormatError("输入文件和输出文件不能是同一个文件")
        text = input_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise TopologyFormatError(f"输入文件不存在：{input_path}") from exc
    except OSError as exc:
        raise TopologyFormatError(f"读取输入文件失败：{input_path}: {exc}") from exc

    payload = parse_topology_text(
        text,
        world_size_override=world_size_override,
    )
    _write_json_atomic(output_path, payload, indent=indent)
    return payload


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="将 topo.txt 中的模型并行进程组转换为 JSON 拓扑文件"
    )
    parser.add_argument(
        "input",
        nargs="?",
        type=Path,
        default=Path("topo.txt"),
        help="输入文件，默认：./topo.txt",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="输出文件，默认：输入文件同目录下的同名 .json 文件",
    )
    parser.add_argument(
        "--world-size",
        type=int,
        help="显式指定 world_size；未指定时从最大 rank + 1 推导",
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=2,
        choices=range(0, 9),
        metavar="0-8",
        help="JSON 缩进空格数，默认：2",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    output_path = (
        args.output
        if args.output is not None
        else args.input.with_suffix(".json")
    )

    try:
        payload = convert_topology_file(
            args.input,
            output_path,
            world_size_override=args.world_size,
            indent=args.indent,
        )
    except TopologyFormatError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    sizes = payload["parallel_sizes"]
    assert isinstance(sizes, Mapping)
    print(
        f"已生成 {output_path}：world_size={payload['world_size']}，"
        f"groups={len(payload['groups'])}，"
        + "，".join(f"{name}={sizes[name]}" for name in PARALLEL_SIZE_KEYS)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
