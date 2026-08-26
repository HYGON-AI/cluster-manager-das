# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import time
import os
from typing import List, Optional, Tuple
import sys
from cluster_manager.config.global_config import logger
from cluster_manager.executor.cmd_executor import CmdExecutor


class SlurmMgr:
    """Slurm client wrapper for sbatch/squeue/scancel operations."""

    @staticmethod
    def _clean_squeue_lines(output: str) -> List[str]:
        lines = [line.strip() for line in (output or "").splitlines() if line.strip()]
        return [line for line in lines if not line.lower().startswith("squeue:")]

    def __init__(self, job_name: str, sbatch_script: str, hostfile: str, job_id: Optional[str] = None):
        self.job_name = job_name
        self.sbatch_script = sbatch_script
        self.hostfile = hostfile
        hostfile_dir = os.path.dirname(os.path.abspath(hostfile)) if hostfile else os.getcwd()
        self.job_id_file = os.path.join(hostfile_dir, "slurm_job_id")
        self.current_job_id = job_id
        self._load_current_job_id()
        check_res = self._check_job_id_name()
        if check_res:
            self.current_job_id = str(job_id).strip()
            self._persist_current_job_id(self.current_job_id)
            logger.info(f"[SlurmMgr] Use input job_id={self.current_job_id}")
        else:
            logger.error(f'输入的 job_name:[{self.job_name}] 和 job_id:[{self.current_job_id}] 不匹配，容错无法拉起训练')
            sys.exit(1)


    def _check_job_id_name(self) -> bool:

        cmd = f"squeue -j {self.current_job_id} -n {self.job_name} -h -o '%j'"
        try:
            err_code, output = CmdExecutor.execute_command(cmd, capture_output=True, timeout=120)
            if err_code != 0:
                logger.warning(f"[SlurmMgr] Query job id&name failed for job_id={self.current_job_id}, job_name={self.job_name}, err_code={err_code}")
                return False

            check_job_name = output.strip()
            if not check_job_name:
                return False
            if check_job_name == self.job_name:
                return True
            else:
                return False
        except Exception as e:
            logger.error(f"Query job failed: {e}")
            return False



    def _load_current_job_id(self) -> None:
        try:
            if not os.path.exists(self.job_id_file):
                return
            with open(self.job_id_file, "r", encoding="utf-8") as f:
                job_id = f.read().strip()
            if job_id:
                self.current_job_id = job_id
                logger.info(f"[SlurmMgr] Recovered persisted job_id={job_id}")
        except Exception as e:
            logger.warning(f"[SlurmMgr] Failed to load job_id file: {e}")

    def _persist_current_job_id(self, job_id: str) -> None:
        try:
            os.makedirs(os.path.dirname(self.job_id_file), exist_ok=True)
            with open(self.job_id_file, "w", encoding="utf-8") as f:
                f.write(str(job_id))
        except Exception as e:
            logger.warning(f"[SlurmMgr] Failed to persist job_id={job_id}: {e}")

    def _clear_current_job_id(self) -> None:
        self.current_job_id = None
        try:
            if os.path.exists(self.job_id_file):
                os.remove(self.job_id_file)
        except Exception as e:
            logger.warning(f"[SlurmMgr] Failed to remove job_id file: {e}")

    def job_exists_by_id(self, job_id: Optional[str] = None) -> Tuple[bool, bool]:
        """
        Tri-state job-exists query.
        Returns:
            (query_ok, exists)
            - query_ok=False: query command failed / unknown
            - query_ok=True and exists=False: query succeeded, job absent
            - query_ok=True and exists=True: query succeeded, job present
        """
        target_id = job_id or self.current_job_id
        if not target_id:
            return True, False
        try:
            cmd = f"squeue -j {target_id} -h -o '%i'"
            err_code, output = CmdExecutor.execute_command(cmd, capture_output=True, timeout=60)
            if err_code != 0:
                logger.warning(f"[SlurmMgr] Query job exists failed for job_id={target_id}, err={err_code}")
                return False, False
            ids = [line.split()[0] for line in self._clean_squeue_lines(output) if line.split()]
            return True, (target_id in ids)
        except Exception as e:
            logger.warning(f"[SlurmMgr] Query job exists exception for job_id={target_id}: {e}")
            return False, False

    def clean_old_jobs(self, timeout: int = 300) -> bool:
        try:
            if not self.current_job_id:
                logger.info("[SlurmMgr] No current_job_id, skip clean_old_jobs.")
                return True

            job_id = self.current_job_id
            query_ok, exists = self.job_exists_by_id(job_id)
            if not query_ok:
                logger.error(f"[SlurmMgr] Query job exists failed for job_id={job_id}, skip clean_old_jobs.")
                return False
            if not exists:
                logger.info(f"[SlurmMgr] job_id={job_id} not in queue, clear local binding.")
                self._clear_current_job_id()
                return True

            cancel_cmd = f"scancel {job_id}"
            logger.info(f"[SlurmMgr] Cancelling job_id={job_id}")
            err_code, output = CmdExecutor.execute_command(cancel_cmd, capture_output=True, timeout=60)
            if err_code != 0:
                logger.error(f"[SlurmMgr] Cancel job_id={job_id} failed, err={err_code}, detail={output}")
                return False

            start = time.time()
            while time.time() - start < timeout:
                query_ok, exists = self.job_exists_by_id(job_id)
                if not query_ok:
                    logger.warning(f"[SlurmMgr] Query job exists failed while waiting release for job_id={job_id}, retrying...")
                    time.sleep(5)
                    continue
                if not exists:
                    logger.info(f"[SlurmMgr] job_id={job_id} terminated.")
                    self._clear_current_job_id()
                    return True
                time.sleep(5)
            logger.error(f"[SlurmMgr] Timeout waiting for job_id={job_id} to be released")
            return False
        except Exception as e:
            logger.error(f"Error cleaning old jobs: {e}")
            return False

    def submit_new_job(self, timeout: int = 60) -> Optional[str]:
        try:
            cmd = f"sbatch {self.sbatch_script}"
            logger.info(f"Executing: {cmd}")
            err_code, output = CmdExecutor.execute_command(cmd, capture_output=True, timeout=timeout)
            if err_code != 0:
                logger.error(f"sbatch submit failed (err={err_code}): {output}")
                return None

            output = (output or "").strip()
            submitted_line = None
            for line in output.splitlines():
                line = line.strip()
                if line.startswith("Submitted batch job"):
                    submitted_line = line
                    break

            if not submitted_line:
                logger.error(f"sbatch output format error: {output}")
                return None

            job_id = submitted_line.split()[-1]
            self.current_job_id = job_id
            self._persist_current_job_id(job_id)
            logger.info(f"sbatch submitted, job_id={job_id}")
            return job_id
        except Exception as e:
            logger.error(f"sbatch exception: {e}")
            return None

    def wait_for_nodes(self, job_id: str, timeout: int = 300, poll_interval: int = 5) -> str:
        start = time.time()
        while time.time() - start < timeout:
            try:
                cmd = f"squeue -j {job_id} -h -o '%N'"
                err_code, output = CmdExecutor.execute_command(cmd, capture_output=True, timeout=timeout)
                if err_code == 0:
                    lines = self._clean_squeue_lines(output)
                    if lines:
                        node_expr = lines[0].split()[0]
                        if node_expr and node_expr not in ("(null)", "None", "N/A"):
                            logger.info(f"Job {job_id} allocated nodes: {node_expr}")
                            return node_expr
                else:
                    logger.warning(f"[SlurmMgr] Query node list failed for job_id={job_id}, err={err_code}")
                time.sleep(poll_interval)
            except Exception as e:
                logger.warning(f"Query node list error: {e}")
                time.sleep(poll_interval)
        logger.error(f"Timeout waiting for job {job_id} to allocate nodes")
        return ""

    def expand_nodes(self, node_expr: str, log_detail: bool = True) -> List[str]:
        try:
            cmd = f"scontrol show hostname {node_expr}"
            if log_detail:
                logger.info(f"Executing: {cmd}")
            err_code, output = CmdExecutor.execute_command(cmd, capture_output=True, timeout=60)
            if err_code != 0:
                logger.error(f"scontrol failed (err={err_code}): {output}")
                return []
            nodes = [line.strip() for line in (output or "").splitlines() if line.strip()]
            if not nodes:
                logger.error("Expanded node list empty")
                return []
            if log_detail:
                logger.info(f"Expanded {len(nodes)} nodes")
            return nodes
        except Exception as e:
            logger.error(f"scontrol exception: {e}")
            return []

    def submit_and_get_nodes(self) -> bool:
        if not self.sbatch_script or not self.hostfile:
            logger.error("sbatch_script or hostfile not configured")
            return False

        job_id = self.submit_new_job()
        if not job_id:
            return False

        node_expr = self.wait_for_nodes(job_id)
        if not node_expr:
            return False

        nodes = self.expand_nodes(node_expr)
        if not nodes:
            return False

        try:
            with open(self.hostfile, 'w') as f:
                f.write("\n".join(nodes))
            logger.info(f"Hostfile updated: {self.hostfile}")
        except Exception as e:
            logger.error(f"Write hostfile failed: {e}")
            return False

        return True

    def get_job_nodes(self, verbose: bool = True) -> Tuple[bool, List[str]]:
        """
        Tri-state job-nodes query.
        Returns:
            (query_ok, nodes)
            - query_ok=False: query command failed / unknown
            - query_ok=True and nodes=[]: query succeeded, job absent
            - query_ok=True and nodes=[...]: query succeeded, job present
        """
        if not self.current_job_id:
            if verbose:
                logger.info("[SlurmMgr] current_job_id is empty, cannot query nodes by id.")
            else:
                logger.debug("[SlurmMgr] current_job_id is empty, cannot query nodes by id.")
            return True, []

        cmd = f"squeue -j {self.current_job_id} -h -o '%N'"
        try:
            err_code, output = CmdExecutor.execute_command(cmd, capture_output=True, timeout=120)
            if err_code != 0:
                logger.warning(f"[SlurmMgr] Query job nodes failed for job_id={self.current_job_id}, err={err_code}")
                return False, []

            lines = self._clean_squeue_lines(output)
            if not lines:
                if verbose:
                    logger.info(f"Job '{self.current_job_id}' does NOT exist in queue")
                else:
                    logger.debug(f"Job '{self.current_job_id}' does NOT exist in queue")
                return True, []

            node_expr = lines[0].split()[0]
            nodes = self.expand_nodes(node_expr, log_detail=verbose)
            if not nodes:
                logger.warning(f"Job '{self.current_job_id}' exists but node expansion failed")
                return False, []

            if verbose:
                logger.info(f"Job '{self.current_job_id}' allocated {len(nodes)} nodes: {nodes}")
            else:
                logger.debug(f"Job '{self.current_job_id}' allocated {len(nodes)} nodes")
            return True, nodes
        except Exception as e:
            logger.error(f"Query job nodes failed: {e}")
            return False, []

    def update_hostfile(self, nodes: List[str]) -> bool:
        return True
        if not nodes:
            return False
        try:
            with open(self.hostfile, 'w') as f:
                f.write("\n".join(nodes))
            logger.info(f"Hostfile updated with {len(nodes)} nodes: {self.hostfile}")
            return True
        except Exception as e:
            logger.error(f"Write hostfile failed: {e}")
            return False

    def get_job_node_count(self) -> int:
        """Return -1 on query failure, 0 when absent, >0 when present."""
        if not self.current_job_id:
            return 0

        cmd = f"squeue -j {self.current_job_id} -h -o '%N'"
        try:
            err_code, output = CmdExecutor.execute_command(cmd, capture_output=True, timeout=120)
            if err_code != 0:
                logger.warning(
                    f"[SlurmMgr] Query node count failed for job_id={self.current_job_id}, err={err_code}"
                )
                return -1

            if output is None:
                return 0

            output = str(output).strip()

            if not output:
                return 0

            if output.strip() == "命令执行成功":
                logger.warning(
                    f"[SlurmMgr] squeue command succeeded but no node list returned for job_id={self.current_job_id}, treat as absent."
                )
                return 0
            
            lines = self._clean_squeue_lines(output)
            if not lines:
                return 0

            node_expr = lines[0].split()[0]
            nodes = self.expand_nodes(node_expr, log_detail=False)
            if not nodes:
                logger.warning(
                    f"[SlurmMgr] Query node count got node_expr='{node_expr}' but expansion failed, treat as unknown."
                )
                return -1

            return len(nodes)
        except Exception as e:
            logger.warning(f"[SlurmMgr] Query node count exception: {e}")
            return -1

    def release_job(self) -> bool:
        if not self.current_job_id:
            logger.info("[SlurmMgr] current_job_id is empty, nothing to release.")
            return True
        try:
            job_id = self.current_job_id
            query_ok, exists = self.job_exists_by_id(job_id)
            if not query_ok:
                logger.error(f"[SlurmMgr] Query job exists failed for job_id={job_id}, skip release_job.")
                return False
            if not exists:
                logger.info(f"[SlurmMgr] Job '{job_id}' not found in queue, clear local binding.")
                self._clear_current_job_id()
                return True

            cancel_cmd = f"scancel {job_id}"
            logger.info(f"[SlurmMgr] Cancelling job {job_id}")
            err_code, output = CmdExecutor.execute_command(cancel_cmd, capture_output=True, timeout=120)
            if err_code != 0:
                logger.error(f"[SlurmMgr] Cancel job {job_id} failed (err={err_code}): {output}")
                return False

            logger.info(f"[SlurmMgr] Job {job_id} cancelled successfully.")
            self._clear_current_job_id()
            return True
        except Exception as e:
            logger.error(f"[SlurmMgr] Release job failed: {e}")
            return False
