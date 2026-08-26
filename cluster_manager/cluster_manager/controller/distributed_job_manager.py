# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# cluster_manager/controller/distributed_job_manager.py
import time
import sys
from datetime import datetime
from cluster_manager.runtime.runtime_context import RuntimeContext
from cluster_manager.runtime.job_state_machine import JobStateMachine, JobCommand
from cluster_manager.runtime.run_state_manager import RunState
from cluster_manager.monitor.log_monitor import LogMonitor
from cluster_manager.event.event_bus import EventBus
from cluster_manager.launcher.launcher_factory import create_launcher
from cluster_manager.config.global_config import logger
import cluster_manager.config.global_config as global_config
from cluster_manager.node_management.node_pool import NodePool
from cluster_manager.monitor.nhc_monitor import NodePoolProxy
from cluster_manager.platform.notify import Notify
from cluster_manager.node_management.slurm_manager import SlurmMgr


class DistributedJobManager:
    """
    单任务控制平面

    职责划分：
    - run()              唯一入口：初始化 → 主循环
    - _init_components()  组件创建
    - _ensure_slurm_job() Slurm 作业保障
    - _restore_state()    状态恢复 + log_monitor
    - _next_command()     获取下一个命令
    - _execute()          执行命令，sync 命令返回下一命令
    - stop()              清理
    """

    def __init__(self, runtime_args: dict, workspace_dir):
        self.runtime_args = runtime_args
        self.workspace_dir = workspace_dir
        self.job_name = runtime_args.get("job_name", "rctest")
        self.job_id = runtime_args.get("job_id", "rctest")
        self.hostfile = runtime_args.get("hostfile", f"{workspace_dir}/hostfile")
        self.sbatch_script = runtime_args.get("sbatch_script", global_config.SBATCH_SCRIPT)
        self.cluster_schedule = runtime_args.get(
            "cluster_schedule", global_config.CLUSTER_SCHEDULE
        )
        
        self.event_bus = EventBus()
        self.launcher = create_launcher()
        self.notify = Notify(job_name=self.job_name)
        self.slurm_mgr = None
        if self.cluster_schedule == "SLURM":
            self.slurm_mgr = SlurmMgr(
                self.job_name, self.sbatch_script, self.hostfile, self.job_id
            )
        self.ctx = None
        self.state_machine = None
        self.log_monitor = None
        self.running = False

    # ==================== 入口 ====================

    def run(self):
        """唯一入口：检查队列 → 更新 hostfile → 初始化 → 状态恢复 → 主循环"""
        if self.slurm_mgr is not None:
            self._ensure_slurm_job()
        self._init_components()
        self._restore_state()

        while self.running:
            cmd = self._next_command()
            while cmd != JobCommand.NONE:
                cmd = self._execute(cmd)

    # ==================== 初始化阶段 ====================

    def _init_components(self):
        """纯组件初始化：创建运行时上下文、状态机、日志监控"""
        if self.log_monitor:
            self.log_monitor.stop()
        if self.ctx and hasattr(self.ctx, 'node_pool_proxy'):
            self.ctx.node_pool_proxy.stop_monitor()

        node_pool = NodePool(self.workspace_dir, self.hostfile)
        node_pool_proxy = NodePoolProxy(node_pool, self.event_bus, slurm_mgr=self.slurm_mgr)
        node_pool_proxy.start_monitor()
        self.ctx = RuntimeContext(self.runtime_args, node_pool_proxy)
        self.state_machine = JobStateMachine(self.ctx)
        self.log_monitor = LogMonitor(
            self.event_bus,
            self.job_name,
            slots_per_node=self.runtime_args["slots_per_node"],
        )
        self.running = True

    def _ensure_slurm_job(self):
        """确保 Slurm 作业存在，获取节点列表并更新 hostfile，无作业则提交"""
        while True:
            query_ok, nodes = self.slurm_mgr.get_job_nodes()
            if not query_ok:
                logger.warning(f"[Manager] Query slurm job nodes failed, retry later: {self.job_name}")
                time.sleep(60)
                continue

            if nodes:
                if not self.slurm_mgr.update_hostfile(nodes):
                    logger.exception(f"[Manager] Update hostfile failed: {self.job_name}, 容错进程退出")
                    sys.exit(1)
                logger.info(f"[Manager] Slurm job exists, hostfile updated: {self.job_name}")
                return

            logger.warning(f"[Manager] No slurm job, submitting: {self.job_name}")
            if not self.slurm_mgr.submit_and_get_nodes():
                logger.exception(f"[Manager] Submit failed: {self.job_name} , 容错进程退出")
                sys.exit(1)
            logger.info(f"[Manager] Submit success: {self.job_name}")
            return

    def _restore_state(self):
        """根据磁盘状态恢复运行：引导状态机，按需启动 log_monitor"""
        self.state_machine.init_state()
        if self.ctx.run_state in (RunState.PENDING, RunState.RUNNING):
            self.log_monitor.start()

    # ==================== 主循环 ====================

    def _next_command(self):
        """获取下一个命令：事件驱动优先，否则状态驱动"""
        event = self.event_bus.get_event(timeout=1)
        if event is not None:
            return self.state_machine.on_event(event)
        return self.state_machine.next_action()

    # ==================== 故障报告 ====================

    def report_fault(self):
        snapshot = self.ctx.node_pool_proxy.get_current_snapshot()
        if not snapshot:
            return "当前无节点快照信息"

        sm = self.state_machine
        start_dt = sm.safe_timestamp_to_datetime(snapshot.start_time)
        stop_dt = sm.safe_timestamp_to_datetime(snapshot.stop_time)

        if start_dt and stop_dt:
            run_str = sm.fmt_duration((stop_dt - start_dt).total_seconds())
        elif start_dt and (snapshot.fault_nodes or snapshot.fault_count > 0):
            run_str = sm.fmt_duration((datetime.now() - start_dt).total_seconds())
        else:
            run_str = "无故障"

        fmt = lambda t: t.strftime("%Y-%m-%d %H:%M:%S") if isinstance(t, datetime) else "无"
        avg_str = f"{float(snapshot.avg_iter_time):.2f}s/iter" if snapshot.avg_iter_time else "无"

        message = (
            f"训练故障\n任务ID：{self.job_name}\n"
            f"当前时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"启动时间：{fmt(start_dt)}   运行时间：{run_str}\n"
            f"起始步数：{snapshot.first_iter}   时间：{fmt(sm.safe_timestamp_to_datetime(snapshot.first_iter_time))}\n"
            f"终止步数：{snapshot.last_iter}    时间：{fmt(sm.safe_timestamp_to_datetime(snapshot.last_iter_time))}\n"
            f"平均单步耗时：{avg_str}\n"
            f"故障节点：{snapshot.fault_nodes or '无故障节点'}")
        self.notify.send_feishu_alert(message)
        return message

    # ==================== 命令执行 ====================

    def _execute(self, cmd):
        """执行单个命令。sync 命令返回下一命令，async 命令返回 NONE"""

        # --- sync 命令：执行后反馈状态机，返回下一命令 ---
        if cmd == JobCommand.START_TRAINING:
            return self._start_training()
        if cmd == JobCommand.STOP_TRAINING:
            return self._stop_training()

        # --- async 命令：执行即完成 ---
        if cmd == JobCommand.START_LOG_MONITOR:
            self.log_monitor.start()

        return JobCommand.NONE

    # ==================== 同步命令实现 ====================

    def _start_training(self):
        """申请节点 → 启动训练 → 反馈状态机，返回下一命令；启动失败则释放节点并循环重试"""
        while True:
            # 等待节点就绪
            slots_file = None
            while slots_file is None:
                master, slots_file = self.ctx.node_pool_proxy.apply_node_num_resources(
                    self.runtime_args["required_nodes_num"],
                    self.runtime_args["slots_per_node"])
                if not master:
                    if self.cluster_schedule == "NONE":
                        raise RuntimeError(
                            "Bare-metal healthy nodes are fewer than required: "
                            f"{self.runtime_args['required_nodes_num']}"
                        )
                    logger.error(f"[Manager][{self.job_name}] no available nodes")
                    self.notify.send_feishu_alert(
                        f"节点资源耗尽，故障节点：{self.ctx.node_pool_proxy.abnormal_nodes()} 个（已隔离）")
                    time.sleep(global_config.INTERVAL_MONITOR + 60)
                    slots_file = None
            logger.info(f"[Manager][{self.job_name}] nodes ready: {slots_file}")

            # 启动训练进程
            success, node_list, error_info = True, None, ""
            try:
                err_code, node_list = self.launcher.start(self.runtime_args.get("exec_path"), slots_file)
                if err_code != 0:
                    success, error_info = False, f"MPI launch failed (err {err_code})"
            except Exception as e:
                logger.exception(f"[Manager][{self.job_name}] start error: {e}")
                success, error_info = False, str(e)

            if success:
                self.log_monitor.start()
                return self.state_machine.on_train_success("start", node_list, error_info)

            # 启动失败：释放节点信息，sleep 后重新申请
            logger.warning(f"[Manager][{self.job_name}] start failed: {error_info}, releasing nodes and retrying")
            self.ctx.node_pool_proxy.release_runing_nodes()
            time.sleep(global_config.INTERVAL_MONITOR + 60)

    def _stop_training(self):
        """停止训练 → 反馈状态机，返回下一命令；失败重试 3 次后放弃"""
        max_retry = 3
        for attempt in range(1, max_retry + 1):
            success, node_list, error_info = True, None, ""
            try:
                err_code, node_list = self.launcher.stop(str(self.ctx.node_pool_proxy.normal_nodes_file()))
                if err_code != 0:
                    success, error_info = False, f"Stop failed (err {err_code})"
            except Exception as e:
                logger.exception(f"[Manager][{self.job_name}] stop error: {e}")
                success, error_info = False, str(e)

            if success:
                self.report_fault()
                self.ctx.node_pool_proxy.release_runing_nodes()
                self.log_monitor.stop()
                return self.state_machine.on_train_success("stop", node_list, error_info)

            logger.warning(
                f"[Manager][{self.job_name}] stop failed (attempt {attempt}/{max_retry}): {error_info}")
            time.sleep(global_config.INTERVAL_MONITOR + 60)

        # 重试 3 次仍失败：强制推进状态，跳过 stop 直接进入 start
        logger.error(
            f"[Manager][{self.job_name}] stop failed after {max_retry} retries, skip to start")
        self.report_fault()
        self.ctx.node_pool_proxy.release_runing_nodes()
        self.log_monitor.stop()
        self.state_machine._set_state(RunState.STARTING)
        return JobCommand.START_TRAINING


    # ==================== 清理 ====================

    def stop(self):
        logger.info(f"[Manager] stopping job={self.job_name}")
        self.running = False
        self.log_monitor.stop()
