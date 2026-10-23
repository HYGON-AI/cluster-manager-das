# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import json
from typing import List, Dict
from cluster_manager.utils.file_utils import write_hostfile, read_hostfile, write_slotsfile

class HostfileHandler:
    """主机文件处理工具（原file_utils.py封装）"""
    @staticmethod
    def write(hostfile_path: str, nodes: List[str], hcu_per_node: int = 0):
        """写入主机文件（调用工具类）"""
        if hcu_per_node == 0:
            write_hostfile(hostfile_path, nodes)
        else:             
            write_slotsfile(hostfile_path, nodes, hcu_per_node)

    @staticmethod
    def read(hostfile_path: str) -> List[str]:
        """读取主机文件（调用工具类）"""
        return read_hostfile(hostfile_path)

    @staticmethod
    def write_json(json_path: str, data: Dict):
        """写入JSON格式的节点状态（原逻辑保留）"""
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            raise RuntimeError(f"写入JSON文件失败：{str(e)}") from e

    @staticmethod
    def read_json(json_path: str) -> Dict:
        """读取JSON格式的节点状态（原逻辑保留）"""
        if not json_path.endswith('.json'):
            raise RuntimeError(f"非JSON文件：{json_path}")
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            raise RuntimeError(f"读取JSON文件失败：{str(e)}") from e