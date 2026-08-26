#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
Cluster Training Entry
Example:
# 分布式训练
python main.py \
    --nodes_num 4 \
    --slots 8 \
    --exec /public/home/train.py \
    --hostfile hostfile \
    --job_name my_training_job \
    --sbatch_script /public/home/submit.sbatch

# 节点健康检测
python main.py node_check \
    --clushnode ./node_check/clushnode \
    --nodenum 4 \
    --tflops 100\
    --only-horizontal
"""
import argparse
import os
import re
import sys
import json
import subprocess

if __name__ == "__main__" and __package__ is None:
    current_file = os.path.abspath(__file__)
    pkg_dir = os.path.dirname(current_file)
    project_root = os.path.dirname(pkg_dir)
    sys.path.insert(0, project_root)

from cluster_manager.controller.distributed_job_manager import DistributedJobManager
from cluster_manager.config.global_config import logger
import cluster_manager.config.global_config as global_config

def validate_sbatch_script(script_path: str, expected_job_name: str, required_nodes_num: int) -> bool:
    """
    校验 sbatch 脚本中的作业名和节点数是否符合要求。
    """
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"sbatch script not found at: {script_path}")
  
    job_name_pattern = re.compile(r'^\s*#SBATCH\s+(?:-J|--job-name)\s*=?\s*(\S+)\s*(?:#.*)?$', re.IGNORECASE)
    nodes_pattern = re.compile(r'^\s*#SBATCH\s+(?:-N|--nodes)\s*=?\s*(\d+)\s*(?:#.*)?$', re.IGNORECASE)
    script_job_name = None
    script_nodes_num = None
    try:
        with open(script_path, 'r', encoding='utf-8') as f:
            for line in f:
                if not script_job_name:
                    match_job = job_name_pattern.match(line)
                    if match_job:
                        script_job_name = match_job.group(1)
                if not script_nodes_num:
                    match_nodes = nodes_pattern.match(line)
                    if match_nodes:
                        script_nodes_num = int(match_nodes.group(1))
                if script_job_name and script_nodes_num:
                    break
    except Exception as e:
        raise ValueError(f"Failed to read sbatch script: {e}")
    if not script_job_name:
        raise ValueError("Missing '#SBATCH -J <job_name>' directive in the script.")
    if script_job_name != expected_job_name:
        raise ValueError(
            f"Job name mismatch: Script contains '{script_job_name}', "
            f"but parameter '--job_name' is '{expected_job_name}'."
        )
    if script_nodes_num is None:
        raise ValueError("Missing '#SBATCH -N <nodes_num>' directive in the script.")
    if script_nodes_num < required_nodes_num:
        raise ValueError(
            f"Insufficient nodes in script: Script applies for {script_nodes_num} nodes, "
            f"but parameter '--nodes_num' requires {required_nodes_num} nodes."
        )
    return True

def parse_args():
    parser = argparse.ArgumentParser(description="Distributed Cluster Training and Node Check")
    
    # 训练参数（保持原有使用方式）
    parser.add_argument("--nodes_num", type=int, default=None, help="required nodes number")
    parser.add_argument("--slots", type=int, default=None, help="slots per node")
    parser.add_argument("--exec", type=str, default=None, help="executable training script path")
    parser.add_argument("--hostfile", type=str, default=None, help="hostfile path")
    parser.add_argument("--job_name", type=str, default=None, help="Job name for this training job")
    parser.add_argument("--job_id", type=str, default=None, help="existing slurm job id for this manager instance")
    parser.add_argument("--sbatch_script", type=str, default=None, help="sbatch script path for Slurm job submission")
    
    # 节点检测参数
    parser.add_argument("node_check", nargs='?', default=None, help="Run node health check (use 'node_check')")
    parser.add_argument("-c", "--clushnode", type=str, default=None, help="待检测节点列表文件")
    parser.add_argument("-n", "--nodenum", type=int, default=4, help="每组节点数（默认 4）")
    parser.add_argument("-t", "--tflops", type=int, default=185, help="TFLOPs阈值（默认 100）")
    parser.add_argument("-g", "--healthy", type=str, default=None, help="健康节点保存路径")
    parser.add_argument("-f", "--fault", type=str, default=None, help="异常节点保存路径")
    parser.add_argument("-o", "--only-horizontal", action="store_true", help="仅执行横向检测，跳过纵向检测")
    
    return parser.parse_args()

def build_runtime_args(args) -> dict:
    runtime_args = {
        "required_nodes_num": args.nodes_num,
        "slots_per_node": args.slots,
        "exec_path": args.exec,
        "hostfile": args.hostfile,
        "job_name": args.job_name,
        "job_id": args.job_id,
        "sbatch_script": args.sbatch_script,
        "cluster_schedule": global_config.CLUSTER_SCHEDULE,
    }
    return runtime_args

def to_abs_path(path: str) -> str:
    """如果是相对路径则转为绝对路径，如果是绝对路径则原样返回"""
    if path and not os.path.isabs(path):
        return os.path.abspath(path)
    return path

def validate_training_args(args) -> str:
    """Validate training arguments for bare-metal or Slurm scheduling."""
    schedule = global_config.CLUSTER_SCHEDULE
    if schedule not in ("NONE", "SLURM"):
        raise ValueError(
            f"CLUSTER_SCHEDULE must be NONE or SLURM, got: {schedule}"
        )

    common_args = {
        "--nodes_num": args.nodes_num,
        "--slots": args.slots,
        "--exec": args.exec,
        "--hostfile": args.hostfile,
    }
    missing = [name for name, value in common_args.items() if not value]

    if schedule == "SLURM":
        slurm_args = {
            "--job_id": args.job_id,
            "--job_name": args.job_name,
            "--sbatch_script": args.sbatch_script,
        }
        missing.extend(name for name, value in slurm_args.items() if not value)

    if missing:
        raise ValueError(
            f"Missing required arguments for CLUSTER_SCHEDULE={schedule}: "
            f"{', '.join(missing)}"
        )
    return schedule

def validate_training_files(args, schedule: str) -> None:
    """Validate local inputs without requiring Slurm-created files."""
    if args.nodes_num <= 0:
        raise ValueError(f"--nodes_num must be a positive integer, got: {args.nodes_num}")
    if args.slots <= 0:
        raise ValueError(f"--slots must be a positive integer, got: {args.slots}")
    if not os.path.isfile(args.exec) or not os.access(args.exec, os.R_OK):
        raise ValueError(f"Training script not found or unreadable: {args.exec}")

    if schedule != "NONE":
        return

    if not os.path.isfile(args.hostfile) or not os.access(args.hostfile, os.R_OK):
        raise ValueError(f"Hostfile not found or unreadable: {args.hostfile}")

    with open(args.hostfile, "r", encoding="utf-8") as f:
        host_nodes = {
            line.split()[0]
            for line in f
            if line.strip() and not line.lstrip().startswith("#")
        }
    if len(host_nodes) < args.nodes_num:
        raise ValueError(
            f"Hostfile has {len(host_nodes)} unique nodes, "
            f"but --nodes_num requires {args.nodes_num}"
        )

def run_node_check(args):
    """执行节点健康检测"""
    # 获取 run_check.sh 脚本路径
    script_dir = os.path.dirname(os.path.abspath(__file__))
    run_check_script = os.path.join(script_dir, "node_check", "run_check.sh")
    
    if not os.path.exists(run_check_script):
        logger.exception(f"[node_check] Script not found: {run_check_script}, 容错进程退出")
        sys.exit(1)
    
    if not args.clushnode:
        logger.exception("[node_check] --clushnode is required for node check, 容错进程退出")
        sys.exit(1)
    
    # 构建命令参数
    cmd = [run_check_script, "-c", args.clushnode, "-n", str(args.nodenum), "-t", str(args.tflops)]
    
    if args.healthy:
        cmd.extend(["-g", args.healthy])
    if args.fault:
        cmd.extend(["-f", args.fault])
    if args.only_horizontal:
        cmd.append("--only-horizontal")
    
    logger.info(f"[node_check] Running command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=False)
        sys.exit(result.returncode)
    except KeyboardInterrupt:
        logger.info("[node_check] Process stopped by user")
        sys.exit(0)

def run_training(args):
    """执行分布式训练"""
    try:
        schedule = validate_training_args(args)
        validate_training_files(args, schedule)
    except ValueError as e:
        logger.error(f"[run_cluster] {e}")
        sys.exit(1)

    args.exec = to_abs_path(args.exec)
    args.hostfile = to_abs_path(args.hostfile)
    args.sbatch_script = to_abs_path(args.sbatch_script)
    runtime_args = build_runtime_args(args)
    
    # 训练配置已在 global_config 导入时自动加载（通过 MEGATRON_SCRIPT_PATH 环境变量）
    train_cfg = global_config.get_train_config()
    
    logger.info(f"[run_cluster] Runtime arguments: {runtime_args}")

    # ==========================================
    # 新增逻辑：计算 world_size 并注入 global_config
    # ==========================================
    world_size = args.nodes_num * args.slots
    global_config.MEGATRON_CONFIG["--world-size"] = world_size
    
    logger.info(f"[run_cluster] Computed world_size: {world_size} ({args.nodes_num} nodes * {args.slots} slots)")
    logger.info(f"[run_cluster] Current MEGATRON_CONFIG:\n{json.dumps(global_config.get_megatron_config(), indent=4, ensure_ascii=False)}")
    # ==========================================

    workspace_dir = os.path.join(global_config.WORK_DIR, "workspace")
    try:
        os.makedirs(workspace_dir, exist_ok=True)
        logger.info(f"[run_cluster] Workspace directory ready: {workspace_dir}")
    except Exception as e:
        logger.exception(f"[run_cluster] Failed to create workspace directory: {e}, 容错进程退出")
        sys.exit(1)

    if schedule == "SLURM":
        try:
            logger.info(f"[run_cluster] Validating sbatch script: {args.sbatch_script}")
            validate_sbatch_script(
                script_path=args.sbatch_script,
                expected_job_name=args.job_name,
                required_nodes_num=args.nodes_num
            )
            logger.info("[run_cluster] Sbatch script validation passed.")
        except (ValueError, FileNotFoundError) as e:
            logger.exception(f"[run_cluster] Sbatch script validation failed: {e}, 容错进程退出")
            sys.exit(1)

    manager = DistributedJobManager(runtime_args, workspace_dir)
    try:
        manager.run()
    except KeyboardInterrupt:
        logger.info("[run_cluster] Process stopped by user")
        sys.exit(0)

def main():
    args = parse_args()

    # 判断是节点检测模式还是训练模式
    if args.node_check == "node_check":
        run_node_check(args)
    else:
        run_training(args)

if __name__ == "__main__":
    main()
