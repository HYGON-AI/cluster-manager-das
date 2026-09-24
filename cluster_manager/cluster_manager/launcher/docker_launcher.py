# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Docker launcher for the mpirun + docker start mode.

Two MPI layers are involved and they must not be confused:

* The *outer* layer runs on the hosts.  ``cluster_manager`` uses mpirun only to
  fan a container-preparation command out to every node in the hostfile, so each
  node ends up with the training container running.  It talks to the host sshd
  (``MPIRUN_PLM_RSH_ARGS`` contains the port, defaulted by the deployment
  script to ``36000``).
* The *inner* layer runs inside the container.  The training script starts its
  own mpirun, which reaches the peer containers through the sshd that each
  container exposes on the host network.  The port for that layer belongs to the
  training script (its own ``plm_rsh_args``), not to cluster_manager.

Because the inner layer already has a working container-to-container ssh path,
no ``plm_rsh_agent`` wrapper is installed.  PRTE invokes such an agent as
``agent <ssh args> HOST COMMAND`` -- a wrapper that reads ``$1`` as the hostname
receives ``-p`` instead and makes ssh reject the remote command as a port
number.  A docker-exec wrapper could not work from inside the container anyway,
since the container has neither a docker CLI nor the docker socket.
"""

import os
import shlex
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

from cluster_manager.config.global_config import logger, MPI_LAUNCH_TIMEOUT
from cluster_manager.executor.cmd_executor import CmdExecutor
from cluster_manager.launcher.mpirun_launcher import MPIRunLauncher


class DockerLauncher(MPIRunLauncher):
    """Prepare one container per node, then run the training script in it."""

    #: Embedded in every generated stop script.  A wrapper shell carries the
    #: whole script in its cmdline, so skipping matches that contain this marker
    #: keeps the sweep from killing itself: a plain ``pkill -f python`` matches
    #: the very ``bash -lc`` that runs it, which means the wrapper dies and the
    #: follow-up SIGKILL never runs.
    _STOP_GUARD = "CM_STOP_GUARD"

    def __init__(self) -> None:
        super().__init__()
        self.docker_enabled = True
        # Restrict PRTE/OMPI TCP selection to the fabric.  Left empty the HNP
        # advertises the docker0 bridge (172.17.0.1) and the CNI address
        # alongside the real ones, and a remote daemon may pick an unreachable
        # one and then time out as "lost communication with a remote daemon".
        self.tcp_if_include = os.getenv("MPI_TCP_IF_INCLUDE", "").strip()
        # Variables every rank must agree on -- NCCL_SOCKET_IFNAME above all.
        # Nodes with several fabric interfaces otherwise each pick their own and
        # the bootstrap sockets fail with "Connection reset by peer".
        self.rank_env = self._parse_rank_env(os.getenv("MPI_RANK_ENV", ""))
        # Names whose *current* value in the coordinator container is forwarded.
        # The peer nodes' ranks are spawned by ssh into the container sshd,
        # which does not carry the image's ENV -- unlike the docker exec that
        # starts the coordinator.  LD_LIBRARY_PATH is the one that bites: the
        # RCCL network plugin lives on a path only the image ENV mentions, so
        # without it the peer silently falls back to NET/Socket while the
        # coordinator uses the IB plugin, and the mismatched transports abort
        # with "socketFinalizeAccept: wrong type 3 != 4".
        self.forward_env = [
            name.strip()
            for name in os.getenv("MPI_FORWARD_ENV", "").split(",")
            if name.strip()
        ]
        self.stop_pattern = os.getenv("DOCKER_STOP_PATTERN", "python").strip()

    @staticmethod
    def _parse_rank_env(raw: str) -> List[Tuple[str, str]]:
        """Parse ``VAR=value,VAR2=value2`` into ordered pairs."""
        pairs: List[Tuple[str, str]] = []
        for item in raw.split(","):
            item = item.strip()
            if not item:
                continue
            if "=" not in item:
                raise ValueError(
                    f"MPI_RANK_ENV entries must be VAR=value, got {item!r}"
                )
            name, _, value = item.partition("=")
            pairs.append((name.strip(), value.strip()))
        return pairs

    # ------------------------------------------------------------------
    # launch
    # ------------------------------------------------------------------
    def prepare_containers(
        self, exec_path, hostfile, only_missing: bool = False
    ) -> Tuple[int, List[str]]:
        """Concurrently ensure one ready training container on every host.

        Container lifecycle commands are host-side fan-out work, so clush is
        used instead of an outer MPI job.  This avoids rank placement putting
        two docker-run commands on the same host and racing on the container
        name.
        """
        exec_path, hostfile = str(exec_path), str(hostfile)
        hosts = self._hosts(hostfile)
        reuse_existing = True if only_missing else None
        command = (
            f"clush -f 1000 -b --hostfile {shlex.quote(hostfile)} "
            f"{shlex.quote(self._docker_prepare_command(exec_path, hostfile, reuse_existing))}"
        )
        logger.info(
            "[DOCKER-CLUSTER-PREPARE] nodes=%d hosts=%s clush_command=%s",
            len(hosts), ",".join(hosts), command,
        )
        work_dir = Path(exec_path).parent
        command_work_dir = work_dir if work_dir.is_dir() else None
        code, output = CmdExecutor.execute_command(
            command, command_work_dir, True, MPI_LAUNCH_TIMEOUT
        )
        if code != 0:
            logger.error("[DOCKER-CLUSTER-PREPARE] failed code=%s output=%s", code, output)
        return code, []

    def prepare_new_containers(self, exec_path, hostfile) -> Tuple[int, List[str]]:
        """Prepare containers for a newly allocated/replacement node set.

        Existing containers are reused and only missing containers are
        created. This is separate from the initial preparation path, whose
        configured lifecycle may remove and recreate containers.
        """
        return self.prepare_containers(exec_path, hostfile, only_missing=True)

    def _prepare_mpirun_command(self, rank_command: str, slotsfile: str, count: int) -> str:
        """Fan ``rank_command`` out to one rank per host.

        Uses a non-login shell: the prepare script runs under ``set -eu`` and a
        login shell's profile scripts are a common source of spurious failures.
        """
        command = [self.mpirun_bin, "-np", str(count), "--hostfile", slotsfile]
        if self.allow_root:
            command.append("--allow-run-as-root")
        command.extend(
            ["--mca", "plm_rsh_args", self.mpirun_rsh_args, "bash", "-c", rank_command]
        )
        return " ".join(shlex.quote(part) for part in command)

    def _network_exports(self) -> str:
        """Pin PRTE/OMPI TCP selection to the fabric interface, when configured."""
        if not self.tcp_if_include:
            return ""
        value = shlex.quote(self.tcp_if_include)
        return "".join(
            f"export {var}={value}; "
            for var in (
                "OMPI_MCA_oob_tcp_if_include",
                "OMPI_MCA_btl_tcp_if_include",
                "PRTE_MCA_oob_tcp_if_include",
            )
        )

    def _rank_env_exports(self) -> str:
        """Make ``MPI_RANK_ENV``/``MPI_FORWARD_ENV`` reach *every* rank.

        Exporting a variable here would only reach the ranks the coordinator
        container starts locally: mpirun does not forward arbitrary environment
        variables to remote ranks, so the peer node would keep whatever its own
        environment says.  For a variable like ``NCCL_SOCKET_IFNAME`` that is
        worse than not setting it at all -- each node then autoselects a
        different fabric interface.  ``mca_base_env_list`` is the mechanism that
        does reach remote ranks: mpirun sets the listed variables in the
        environment of each process it launches, wherever it launches it.

        The training script's own mpirun inherits these variables from the shell
        this snippet runs in, so the list reaches the inner layer's ranks too.

        ``MPI_FORWARD_ENV`` values are resolved by the container's shell rather
        than by Python, because the value that matters is the one inside the
        container, which the manager process cannot see.

        Note the values still lose to anything the training script exports
        afterwards (a rank runs ``source env.sh`` *after* mpirun has set its
        environment), so the same variable must not also be set there.
        """
        if not self.rank_env and not self.forward_env:
            return ""
        # mca_base_env_list is semicolon-separated.
        seed = ";".join(f"{name}={value}" for name, value in self.rank_env)
        snippet = "".join(
            f"export {name}={shlex.quote(value)}; " for name, value in self.rank_env
        )
        snippet += f"_cm_fwd={shlex.quote(seed)}; "
        if self.forward_env:
            names = " ".join(shlex.quote(name) for name in self.forward_env)
            snippet += (
                f"for _cm_n in {names}; do "
                'eval "_cm_v=\\${$_cm_n-}"; '
                # Skip unset or empty names instead of forwarding "NAME=".
                '[ -n "$_cm_v" ] || continue; '
                '_cm_fwd="${_cm_fwd:+$_cm_fwd;}$_cm_n=$_cm_v"; '
                "done; "
            )
        snippet += (
            'if [ -n "$_cm_fwd" ]; then '
            'export OMPI_MCA_mca_base_env_list="$_cm_fwd"; '
            'export PRTE_MCA_mca_base_env_list="$_cm_fwd"; '
            "fi; "
        )
        return snippet

    def start(self, exec_path, slotsfile):
        """Start training in containers prepared during manager startup."""
        exec_path, slotsfile = str(exec_path), str(slotsfile)
        hosts = self._hosts(slotsfile)
        work_dir = Path(exec_path).parent
        # Start the training script once in the first allocated host's
        # container. Its own mpirun fans out to peer containers over sshd.
        inner = (
            "set -eu; "
            + self._network_exports()
            + self._rank_env_exports()
            + f"cd {shlex.quote(str(work_dir))}; "
            f"exec bash {shlex.quote(exec_path)} {shlex.quote(slotsfile)}"
        )
        # cluster_manager may run on a login node that is not part of the
        # allocation. The container is on the first training host, so execute
        # docker there through the normal host SSH path.
        remote_cmd = (
            f"docker exec -d {shlex.quote(self.container_name)} "
            f"bash -c {shlex.quote(inner)}"
        )
        cmd = (
            f"ssh -p {shlex.quote(self.host_ssh_port)} "
            f"{shlex.quote(hosts[0])} {shlex.quote(remote_cmd)}"
        )
        logger.info(
            "[DOCKER-LAUNCH] coordinator=%s host_ssh_port=%s command=%s",
            hosts[0], self.host_ssh_port, cmd,
        )
        # exec_path can be a container-only path, so it must not be used as
        # the manager-side subprocess cwd.
        return CmdExecutor.execute_command(cmd, None, True, MPI_LAUNCH_TIMEOUT)

    # ------------------------------------------------------------------
    # stop
    # ------------------------------------------------------------------
    @classmethod
    def _stop_script(
        cls, patterns: Sequence[str], extra_skips: Optional[Sequence[str]] = None
    ) -> str:
        """Build a self-excluding TERM-then-KILL sweep for ``patterns``."""
        skips: List[str] = [cls._STOP_GUARD]
        skips.extend(extra_skips or ())
        skip_cases = "|".join(f"*{item}*" for item in skips)
        pattern_args = " ".join(shlex.quote(item) for item in patterns)
        return (
            f": {cls._STOP_GUARD}; "
            f"_cm_sweep() {{ for _pat in {pattern_args}; do "
            'for _p in $(pgrep -f "$_pat" 2>/dev/null); do '
            # A pid may vanish between pgrep and the read; keep the sweep quiet.
            "_c=$(cat /proc/$_p/cmdline 2>/dev/null | tr '\\0' ' '); "
            '[ -n "$_c" ] || continue; '
            f'case "$_c" in {skip_cases}) continue ;; esac; '
            'kill -"$1" "$_p" 2>/dev/null || true; '
            "done; done; }; "
            "_cm_sweep TERM; sleep 2; _cm_sweep KILL; true"
        )

    def stop(self, hostfile) -> Tuple[int, List[str]]:
        """Kill the training processes inside the container on every node."""
        # A fault can remove every running node from normal_nodes.txt before
        # cleanup starts.  clush treats an empty hostfile as an error (exit 2),
        # but there is then simply no host/container left to clean up.
        hostfile_path = Path(hostfile)
        if not hostfile_path.is_file():
            logger.info("[DOCKER-STOP] hostfile=%s is absent; nothing to stop", hostfile)
            return 0, []
        nodes = [
            line.split()[0]
            for line in hostfile_path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
        if not nodes:
            logger.info("[DOCKER-STOP] hostfile=%s has no nodes; skip clush", hostfile)
            return 0, []

        # prted outlives a python-only sweep and would block the next launch.
        inner = self._stop_script([self.stop_pattern, "prted"])
        if self.remove_container_on_stop and not self.reuse_container:
            inner += (
                f"; docker rm -f {shlex.quote(self.container_name)} "
                ">/dev/null 2>&1 || true"
            )
        # The sweep runs inside the container, where it is root and where the
        # PID namespace hides the host processes -- including cluster_manager
        # itself.  Running it on the host instead would fail with EPERM on the
        # root-owned training processes.
        remote_cmd = (
            f"docker exec -u root {shlex.quote(self.container_name)} "
            f"bash -lc {shlex.quote(inner)}"
        )
        # Quote twice: the local shell strips one layer, and clush rejoins its
        # argv with spaces, so the remote command has to arrive as a single
        # argument or its word boundaries are lost.
        cmd = (
            f"clush --hostfile {shlex.quote(str(hostfile))} -b "
            f"{shlex.quote(remote_cmd)}"
        )
        logger.info("[DOCKER-STOP] hostfile=%s pattern=%s", hostfile, self.stop_pattern)
        code, nodes = CmdExecutor.exec_mpirun_cmd(
            cmd, capture_output=True, timeout=MPI_LAUNCH_TIMEOUT
        )
        if code != 0:
            logger.error("[DOCKER-STOP] failed code=%s nodes=%s", code, nodes)
        return code, nodes
