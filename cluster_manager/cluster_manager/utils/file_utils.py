# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import os
from collections import deque
from typing import List, Union, Optional
from pathlib import Path
from cluster_manager.config.global_config import logger



def write_hostfile(filepath: Union[str, Path], host_list: List[str]) -> None:
    """
    将节点列表写入指定主机文件（仅包含节点名，不含slots信息），自动创建不存在的父目录

    :param filepath: 主机文件路径（支持字符串或 Path 对象）
    :param host_list: 节点名称列表（需为非空字符串列表）
    """
    # 类型统一与参数校验
    path = Path(filepath)
    if not host_list:
        logger.warning("写入主机文件失败：节点列表为空")
        return

    # 创建父目录
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.parent.exists():
            raise RuntimeError(f"目录创建后仍不存在：{path.parent}")
    except Exception as e:
        logger.error(f"创建主机文件父目录失败: {e}")
        return

    # 写入文件（去重+排序）
    unique_hosts = sorted(set(host_list))
    try:
        with path.open('w', encoding='utf-8') as f:
            f.writelines(f"{host}\n" for host in unique_hosts)
        logger.debug(
            f"主机文件写入成功: {path.resolve()} "
            f"(原始{len(host_list)}个节点，去重后{len(unique_hosts)}个)"
        )
    except PermissionError:
        logger.error(f"写入主机文件失败：权限不足（{path.resolve()}）")
    except Exception as e:
        logger.error(f"写入主机文件失败: {e}")


def write_slotsfile(filepath: Union[str, Path], host_list: List[str], slots_per_host: int = 8) -> None:
    """
    将节点列表写入指定文件（包含slots信息），自动创建不存在的父目录

    :param filepath: 带slots信息的文件路径（支持字符串或 Path 对象）
    :param host_list: 节点名称列表（需为非空字符串列表）
    :param slots_per_host: 每个节点的slots数量，默认8
    """
    # 类型统一与参数校验
    path = Path(filepath)
    if not host_list:
        logger.warning("写入slots文件失败：节点列表为空")
        return
    if slots_per_host <= 0:
        logger.warning(f"写入slots文件失败：slots数量必须为正整数（当前值：{slots_per_host}）")
        return

    # 创建父目录
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.parent.exists():
            raise RuntimeError(f"目录创建后仍不存在：{path.parent}")
    except Exception as e:
        logger.error(f"创建slots文件父目录失败: {e}")
        return

    # 写入文件（去重+排序，格式：节点名 slots=数量）
    unique_hosts = sorted(set(host_list))
    try:
        with path.open('w', encoding='utf-8') as f:
            f.writelines(f"{host} slots={slots_per_host}\n" for host in unique_hosts)
        logger.debug(
            f"slots文件写入成功: {path.resolve()} "
            f"(原始{len(host_list)}个节点，去重后{len(unique_hosts)}个，每个节点slots={slots_per_host})"
        )
    except PermissionError:
        logger.error(f"写入slots文件失败：权限不足（{path.resolve()}）")
    except Exception as e:
        logger.error(f"写入slots文件失败: {e}")


def read_hostfile(filepath: Union[str, Path]) -> List[str]:
    """
    读取主机文件，返回去重排序后的节点列表（兼容纯节点名或带slots的格式）

    :param filepath: 主机文件路径（支持字符串或 Path 对象）
    :return: 节点名称列表（空列表表示读取失败或文件无有效节点）
    :raises RuntimeError: 读取文件时发生严重错误（如文件不存在、权限拒绝）
    """
    path = Path(filepath)
    if not path.exists():
        error_msg = f"主机文件不存在：{path.resolve()}"
        logger.error(error_msg)
        return []

    try:
        lines = read_lines(path)
        hosts = []
        for line in lines:
            stripped_line = line.strip()
            if not stripped_line or stripped_line.startswith('#'):
                continue
            host = stripped_line.split()[0]
            if host:
                hosts.append(host)
        unique_hosts = sorted(set(hosts))
        logger.debug(
            f"读取主机文件成功: {path.resolve()} "
            f"(共{len(unique_hosts)}个有效节点)"
        )
        return unique_hosts
    except PermissionError:
        error_msg = f"读取主机文件失败：权限不足（{path.resolve()}）"
        logger.error(error_msg)
        raise RuntimeError(error_msg)
    except Exception as e:
        error_msg = f"读取主机文件失败: {e}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)


def read_lines(file_path: Union[str, Path], tail: int = 0) -> List[str]:
    """
    读取文件内容，支持读取全部行或仅最后 N 行

    :param file_path: 文件路径（支持字符串或 Path 对象）
    :param tail: 读取最后 N 行（默认 0 表示读取全部行；负数视为 0）
    :return: 按行分割的字符串列表（空列表表示读取失败）
    """
    path = Path(file_path)
    tail = max(tail, 0)

    try:
        with path.open('r', encoding='utf-8') as f:
            if tail > 0:
                lines = list(deque(f, maxlen=tail))
            else:
                lines = f.readlines()
        return [line.rstrip('\n') for line in lines]
    except FileNotFoundError:
        logger.error(f"读取文件失败: {path.resolve()}（文件不存在）")
    except PermissionError:
        logger.error(f"读取文件失败: {path.resolve()}（权限不足）")
    except Exception as e:
        logger.error(f"读取文件失败: {path.resolve()}，错误: {e}")
    return []