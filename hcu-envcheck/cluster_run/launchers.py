# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Launch commands: MPI belongs to multi-node RCCL/GEMM, not to scripts."""
from __future__ import annotations
import shlex
from dataclasses import dataclass
from pathlib import Path
from .env import source_body

LAUNCHERS = ("mpirun-torchrun", "ssh-torchrun", "mpirun")

@dataclass(frozen=True)
class LaunchContext:
    test_name: str
    launcher: str
    env_script: str | None
    group_name: str
    group_hostfile: Path
    nodes: tuple[str, ...]
    test_command: tuple[str, ...]
    nproc_per_node: int
    np: int | None
    master_port: int
    execution_scope: str = "host"
    container_name: str | None = None
    container_workdir: str | None = None
    allow_root_mpi: bool = False
    worker_root: str | None = None
    timeout_seconds: float = 0
    container_ssh_port: int = 25901
    shell: str = "bash"
    run_token: str | None = None
    python_executable: str = "python3"

    @property
    def master_addr(self) -> str:
        return self.nodes[0]

def _rank_exports() -> str:
    # Never preserve rank topology inherited from env.sh or a parent job.
    return (
        'export HCU_RANK="$__hcu_rank" HCU_WORLD_SIZE="$__hcu_size" '
        'HCU_LOCAL_RANK="$__hcu_local"; '
        'export RANK="$__hcu_rank" WORLD_SIZE="$__hcu_size" '
        'LOCAL_RANK="$__hcu_local" MASTER_ADDR="$HCU_MASTER_ADDR" '
        'MASTER_PORT="$HCU_MASTER_PORT"'
    )

def group_exports(context: LaunchContext) -> dict[str, str]:
    return {"HCU_GROUP_NAME": context.group_name,
            "HCU_GROUP_HOSTFILE": str(context.group_hostfile),
            "HCU_GROUP_NODES": ",".join(context.nodes),
            "HCU_MASTER_ADDR": context.master_addr, "HCU_MASTER_PORT": str(context.master_port)}

def _body(context: LaunchContext, node_rank: int | None, *, torchrun: bool) -> str:
    # Capture MPI values BEFORE env.sh; they are launcher-owned.
    capture = (
        '__hcu_rank="${OMPI_COMM_WORLD_RANK:-${PMI_RANK:-${PMIX_RANK:-0}}}"; '
        '__hcu_size="${OMPI_COMM_WORLD_SIZE:-${PMI_SIZE:-${PMIX_SIZE:-1}}}"; '
        '__hcu_local="${OMPI_COMM_WORLD_LOCAL_RANK:-${MPI_LOCALRANKID:-0}}"'
        if node_rank is None else
        f"__hcu_rank={node_rank}; __hcu_size={len(context.nodes)}; __hcu_local=0"
    )
    environment_prefix = source_body(context.env_script, ["true"], shell=context.shell).splitlines()[:-1]
    lines = [capture, *environment_prefix]
    mpi_variables = ("OMPI_COMM_WORLD_RANK", "OMPI_COMM_WORLD_SIZE", "OMPI_COMM_WORLD_LOCAL_RANK",
                     "OMPI_COMM_WORLD_LOCAL_SIZE", "PMI_RANK", "PMI_SIZE", "PMIX_RANK", "PMIX_SIZE", "MPI_LOCALRANKID")
    if node_rank is None:
        lines[1:1] = [f'__hcu_saved_{name}="${{{name}-}}"; __hcu_present_{name}="${{{name}+x}}"'
                      for name in mpi_variables]
    if context.container_workdir:
        lines.insert(1, "cd " + shlex.quote(context.container_workdir) + " || exit 125")
    if context.worker_root:
        lines.append("export PYTHONPATH=" + shlex.quote(context.worker_root) + '${PYTHONPATH:+:$PYTHONPATH}')
    lines.extend(f"export {key}={shlex.quote(value)}" for key, value in group_exports(context).items())
    lines.extend([
        "unset RANK WORLD_SIZE LOCAL_RANK LOCAL_WORLD_SIZE GROUP_RANK ROLE_RANK "
        "ROLE_WORLD_SIZE TORCHELASTIC_RUN_ID TORCHELASTIC_RESTART_COUNT MASTER_ADDR MASTER_PORT",
        _rank_exports(),
    ])
    if node_rank is None:
        # Direct MPI programs may read raw MPI variables rather than RANK.
        lines.extend(f'if [ "$__hcu_present_{name}" = x ]; then export {name}="$__hcu_saved_{name}"; else unset {name}; fi'
                     for name in mpi_variables)
    else:
        # Local/SSH torchrun must not inherit an unrelated parent MPI topology.
        lines.append("unset " + " ".join(mpi_variables))
    command = list(context.test_command)
    if torchrun:
        # Bind torchrun to the interpreter selected by env.sh/--test-python.
        command = [context.python_executable, "-m", "torch.distributed.run",
                   "--nnodes", str(len(context.nodes)), "--nproc-per-node", str(context.nproc_per_node),
                   "--node-rank", "__HCU_NODE_RANK__", "--master-addr", context.master_addr,
                   "--master-port", str(context.master_port), *command]
    if context.timeout_seconds:
        command = ["timeout", "--signal=TERM", "--kill-after=15s",
                   f"{context.timeout_seconds:g}s", *command]
    lines.append("exec " + shlex.join(command).replace("__HCU_NODE_RANK__", '"$HCU_RANK"'))
    return "\n".join(lines)

def _guard(context: LaunchContext, command: list[str]) -> list[str]:
    if context.timeout_seconds:
        command = ["timeout", "--signal=TERM", "--kill-after=15s", f"{context.timeout_seconds:g}s", *command]
    if not context.run_token:
        return command
    from .task_control import managed_command
    return managed_command(command, context.run_token)

def build_mpirun_command(context: LaunchContext) -> list[str]:
    if context.launcher not in {"mpirun-torchrun", "mpirun"}:
        raise ValueError("not an MPI launcher")
    if context.test_name not in {"rccl", "gemm"} or len(context.nodes) < 2:
        raise ValueError("MPI is only used by multi-node rccl/gemm groups")
    per_node = 1 if context.launcher == "mpirun-torchrun" else context.nproc_per_node
    process_count = per_node * len(context.nodes)
    if context.np is not None and context.np != process_count:
        raise ValueError(f"--np must equal {process_count} for this group/launcher")
    if context.execution_scope == "container" and not context.container_name:
        raise ValueError("container_name is required")
    if not 1 <= context.container_ssh_port <= 65535:
        raise ValueError("container SSH port must be between 1 and 65535")
    rank_command = _guard(context, [context.shell, "-lc" if context.shell == "bash" else "-c",
                                   _body(context, None, torchrun=context.launcher == "mpirun-torchrun")])
    return ["mpirun", *(["--allow-run-as-root"] if context.allow_root_mpi else []),
            *(["--mca", "plm_rsh_args", f"-p {context.container_ssh_port}"]
              if context.execution_scope == "container" else []),
            "--map-by", f"ppr:{per_node}:node", "--rank-by", "slot",
            "--hostfile", str(context.group_hostfile), "-np", str(process_count), *rank_command]

def build_local_command(context: LaunchContext) -> list[str]:
    """Single node, including multi-device fanout: never start mpirun."""
    if len(context.nodes) != 1:
        raise ValueError("local launcher requires a one-node group")
    return _guard(context, [context.shell, "-lc" if context.shell == "bash" else "-c",
                            _body(context, 0, torchrun=context.launcher != "mpirun")])

def build_ssh_torchrun_commands(context: LaunchContext) -> list[tuple[str, list[str]]]:
    if context.launcher != "ssh-torchrun":
        raise ValueError("SSH command construction requires launcher=ssh-torchrun")
    return [(node, _guard(context, [context.shell, "-lc" if context.shell == "bash" else "-c",
                                   _body(context, rank, torchrun=True)]))
            for rank, node in enumerate(context.nodes)]

def validate_launcher(launcher: str) -> None:
    if launcher not in LAUNCHERS:
        raise ValueError(f"unsupported launcher {launcher!r}; choose one of {', '.join(LAUNCHERS)}")
