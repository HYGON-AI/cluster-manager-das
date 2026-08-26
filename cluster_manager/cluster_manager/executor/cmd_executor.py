# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import subprocess
from typing import Tuple, Optional, List
import errno
import re
import os
from cluster_manager.config import *
from cluster_manager.utils.string_utils import *
from cluster_manager.config.global_config import logger

def parse_mpi_command_result(log_content: str) -> List[str]:
    # 第一步：字节流/字符串 统一转为 字符串（核心修改：适配字节流入参）
    if isinstance(log_content, bytes):
        try:
            # 优先 UTF-8 解码（匹配用户日志中的中文编码），无法解码的字符用 � 替换
            raw_text = log_content.decode("utf-8", errors="replace")
        except Exception as e:
            logger.warning(f"UTF-8 解码失败，使用 latin-1 兜底：{str(e)}")
            raw_text = log_content.decode("latin-1", errors="replace")
    elif isinstance(log_content, str):
        # 若入参已是字符串，直接使用
        raw_text = log_content.strip()
    else:
        logger.exception(f"parse_nhc 入参格式错误：预期 bytes 或 str，实际 {type(log_content)}")
        return []
    
    all_nodes: List[str] = []
    # 按行分割日志（兼容单行/多行输入）
    log_lines = raw_text.splitlines()

    # 逐行解析日志
    for line in log_lines:
        failed = extract_failed_nodes_from_line(line)
        if failed:
            all_nodes.extend(failed)
    # 整体去重（项目支持的 Python 3.10+ 会保留节点首次出现顺序）
    unique_nodes = list(dict.fromkeys(all_nodes))
    return unique_nodes

# =============== CmdExecutor 类 ===============
class CmdExecutor:
    """统一管理远程命令执行（ssh、mpirun、sinfo）"""

    @staticmethod
    def execute_command(
        cmd: str,
        work_dir: Optional[str] = None,
        capture_output: bool = False,
        timeout: Optional[int] = 60  # 命令执行超时时间（秒），None表示无超时
    ) -> Tuple[int, str]:
        """
        执行系统命令，返回（错误码, 结果内容）元组
        
        错误码说明（遵循系统通用规范）：
        - 0：命令执行成功（Unix标准成功码）
        - 1~125：系统通用错误码（参考errno模块，如2=文件不存在、13=权限不足）
        - 126：命令存在但无法执行（如权限不足）
        - 127：命令未找到（如"sshx"拼写错误）
        - 128+n：命令被信号n终止（如137=被SIGKILL终止）
        - 255：SSH/clush连接失败（远程命令执行常见错误码）
        
        Args:
            cmd: 要执行的系统命令（如 "clush --hostfile nodes.txt 'ps -elf'"）
            capture_output: 是否捕获命令输出（stdout+stderr合并）
            timeout: 命令执行超时时间（None表示无超时）
        
        Returns:
            Tuple[int, str]: 
                - 第一个元素：错误码（0=成功，非0=失败，遵循系统通用规范）
                - 第二个元素：结果内容（成功时为命令输出，失败时为错误详情）
        """
        logger.debug(f"执行命令: {cmd}")
        
        # 配置输出流向：捕获模式下合并stdout/stderr，非捕获模式下直接打印到终端
        stdout = subprocess.PIPE if capture_output else None
        stderr = subprocess.STDOUT if capture_output else None

        try:
            proc = subprocess.run(
                cmd,
                shell=True,
                text=False,
                preexec_fn=os.setpgrp,  # 避免子进程被父进程信号终止（如Ctrl+C）
                stdout=stdout,
                stderr=stderr,
                timeout=timeout,
                check=False,  # 不主动抛CalledProcessError，保留原始返回码
                cwd=str(work_dir) if work_dir else None
            )
            # 修复编码问题（原错误根源）
            def _decode_output(output):
                if not output:
                    return ""
                for encoding in ["utf-8", "gbk", "latin-1"]:
                    try:
                        return output.decode(encoding, errors="ignore").strip()
                    except:
                        continue
                return f"[无法解码] {output.hex()[:200]}..."
            result = _decode_output(proc.stdout) if (capture_output and proc.stdout) else "命令执行成功"
            # 处理命令执行结果（直接使用子进程返回码，遵循Unix规范）
            if proc.returncode == 0:
                return (0, result)
            else:
                # 失败：返回命令实际返回码+错误输出（如clush连接失败返回255）
                error_msg = _decode_output(proc.stdout) if (capture_output and proc.stdout) else f"命令返回码: {proc.returncode}"
                logger.exception(f"命令失败 | 返回码: {proc.returncode} | 详情: \n{error_msg}...")
                logger.exception(f"cmd:{cmd}")
                return (proc.returncode, error_msg)

        # 捕获特定异常，映射为系统通用错误码
        except subprocess.TimeoutExpired as e:
            # 超时：使用errno.ETIMEDOUT（系统标准超时错误码，值为110）
            err_code = errno.ETIMEDOUT
            error_msg = f"命令超时（{timeout}秒）: {str(e)}"
            logger.exception(f"执行超时 | 错误码: {err_code} | 详情: \n{error_msg}")
            return (err_code, error_msg)

        except FileNotFoundError as e:
            # 命令/文件不存在：使用errno.ENOENT（系统标准"无此文件"错误码，值为2）
            # 对应shell中"command not found"（返回码127），此处优先用系统errno，同时标注shell返回码
            err_code = errno.ENOENT
            error_msg = f"命令/文件不存在（shell返回码127）: {str(e)}"
            logger.exception(f"文件不存在 | 错误码: {err_code} | 详情: \n{error_msg}")
            return (err_code, error_msg)

        except PermissionError as e:
            # 权限不足：使用errno.EACCES（系统标准"权限拒绝"错误码，值为13）
            # 对应shell中"Permission denied"（返回码126）
            err_code = errno.EACCES
            error_msg = f"权限不足（shell返回码126）: {str(e)}"
            logger.exception(f"权限拒绝 | 错误码: {err_code} | 详情: \n{error_msg}")
            return (err_code, error_msg)

        except OSError as e:
            # 系统调用错误（如内存不足、磁盘满）：直接使用异常自带的errno
            err_code = e.errno
            error_msg = f"系统调用错误: {str(e)}（错误码: {err_code}）"
            logger.exception(f"系统错误 | 错误码: {err_code} | 详情: \n{error_msg}")
            return (err_code, error_msg)

        except Exception as e:
            # 未知异常：使用errno.EIO（系统标准"输入输出错误"码，值为5）作为兜底
            err_code = errno.EIO
            error_msg = f"未知执行错误: {str(e)}"
            logger.exception(f"未知错误 | 错误码: {err_code} | 详情: {error_msg}", exc_info=True)  # 打印堆栈便于排查
            return (err_code, error_msg)


    @staticmethod    
    def exec_mpirun_cmd(
        cmd: str,
        work_dir: Optional[str] = None,
        capture_output: bool = False,
        timeout: Optional[int] = None  # 命令执行超时时间（秒），None表示无超时
    ) -> Tuple[int, List[str]]:
        """
        执行 mpirun 命令并返回结果
        :param cmd: 要执行的 mpirun 命令
        :param capture_output: 是否捕获输出（兼容参数，内部强制为True）
        :param timeout: 执行超时时间（秒）
        :return: Tuple[int, List[str]] - 错误码（0成功，非0失败）、节点列表（失败时为异常节点，成功时为空列表）
        """
        node_list: List[str] = []  # 初始化为空列表，确保返回值始终是列表
        # 强制捕获输出（因为需要解析日志提取IP，忽略传入的 capture_output）
        err_code, result = CmdExecutor.execute_command(cmd, work_dir, capture_output=capture_output, timeout=timeout)
        node_list = parse_mpi_command_result(result)
        return err_code, node_list
