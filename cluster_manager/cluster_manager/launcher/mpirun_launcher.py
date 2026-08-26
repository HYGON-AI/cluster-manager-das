# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import subprocess
from cluster_manager.config.global_config import logger, MPI_LAUNCH_TIMEOUT
from cluster_manager.launcher.base_launcher import BaseLauncher
from cluster_manager.executor.cmd_executor import CmdExecutor
from pathlib import Path
class MPIRunLauncher(BaseLauncher):

    def __init__(self):
        self.proc = None
    
    def start(self, exec_path, slotsfile):
        """
        Launch MPI job (debug mode: only print parameters)
        """
        # 核心修改：直接定义字符串命令，替代列表形式
        work_dir = Path(exec_path).parent
        cmd = f"cd {work_dir} ; bash {exec_path} {slotsfile} 2>&1 &"
        logger.info(f"[MPI-LAUNCH] Launch parameters:{cmd},work_dir:{work_dir}")
        return CmdExecutor.exec_mpirun_cmd(cmd, work_dir, False, MPI_LAUNCH_TIMEOUT)


    def stop(self, hostfile):
        """
        Stop job
        """
        cmd = f"clush --hostfile {hostfile} -b pkill -9 -f python"
        logger.info(f"[MPI-STOP] killing python processes, hostfile={hostfile}")
        return CmdExecutor.exec_mpirun_cmd(cmd, capture_output=False, timeout=MPI_LAUNCH_TIMEOUT)

    def is_alive(self):
        """
        Check job status (debug mode always False)
        """
        logger.info("[MPI-STATUS] is_alive() called → False")
        return False
