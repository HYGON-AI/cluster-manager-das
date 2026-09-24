# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import os
import posixpath
import shlex
from pathlib import Path
from typing import List
from cluster_manager.config.global_config import logger, MPI_LAUNCH_TIMEOUT
from cluster_manager.launcher.base_launcher import BaseLauncher
from cluster_manager.executor.cmd_executor import CmdExecutor
class MPIRunLauncher(BaseLauncher):

    def __init__(self):
        self.proc = None
        self.container_name = (
            os.getenv("DOCKER_CONTAINER_NAME")
            or os.getenv("CONTAINER_NAME")
            or "cluster-manager"
        ).strip()
        self.docker_enabled = os.getenv("MPIRUN_DOCKER_ENABLED", "0").lower() in {
            "1", "true", "yes", "on"
        }
        self.mpirun_bin = os.getenv("MPIRUN_BIN", "mpirun")
        # The outer MPI layer connects to host sshd; the inner training MPI
        # uses DOCKER_CONTAINER_SSH_PORT independently.
        self.host_ssh_port = self._host_ssh_port()
        self.mpirun_rsh_args = f"-p {self.host_ssh_port}"
        self.container_ssh_port = os.getenv("DOCKER_CONTAINER_SSH_PORT", "36000").strip()
        if not self.container_ssh_port.isdigit() or not 1 <= int(self.container_ssh_port) <= 65535:
            raise ValueError("DOCKER_CONTAINER_SSH_PORT must be a port number (1-65535)")
        self.allow_root = os.getenv("MPIRUN_ALLOW_RUN_AS_ROOT", "1").lower() in {
            "1", "true", "yes", "on"
        }
        self.docker_image = os.getenv("DOCKER_IMAGE", os.getenv("IMAGE", "")).strip()
        self.docker_image_tar = os.getenv("DOCKER_IMAGE_TAR", os.getenv("IMAGE_TAR", "")).strip()
        self.docker_host_share = os.getenv("DOCKER_HOST_SHARE_ROOT", "").strip()
        self.docker_container_share = os.getenv("DOCKER_CONTAINER_SHARE_ROOT", "").strip()
        self.docker_extra_args = os.getenv(
            "DOCKER_RUN_ARGS",
            "--device=/dev/kfd --device=/dev/dri -v /dev/shm:/dev/shm --group-add video "
            "--cap-add=SYS_PTRACE --security-opt seccomp=unconfined -u root",
        ).strip()
        self.remove_container_on_stop = os.getenv(
            "DOCKER_REMOVE_CONTAINER_ON_STOP", "1"
        ).lower() in {"1", "true", "yes", "on"}
        self.reuse_container = os.getenv("DOCKER_REUSE_CONTAINER", "0").strip().lower() in {
            "1", "true", "yes", "on"
        }

    @staticmethod
    def _hosts(slotsfile: str) -> List[str]:
        path = Path(slotsfile)
        if not path.is_file():
            raise FileNotFoundError(f"MPI hostfile not found: {slotsfile}")
        hosts = []
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                host = line.split()[0]
                if host not in hosts:
                    hosts.append(host)
        if not hosts:
            raise ValueError(f"MPI hostfile contains no nodes: {slotsfile}")
        return hosts

    @staticmethod
    def _host_ssh_port() -> str:
        """Read a port-only host SSH setting with legacy fallback."""
        raw = os.getenv("MPI_HOST_SSH_PORT")
        if raw is None or not raw.strip():
            raw = os.getenv("MPIRUN_PLM_RSH_ARGS", "22")
        value = raw.strip()
        if value.startswith("-p"):
            value = value[2:].strip()
        if not value.isdigit() or not 1 <= int(value) <= 65535:
            raise ValueError(
                "MPI_HOST_SSH_PORT must be a port number (1-65535), e.g. 22"
            )
        return value

    def _docker_rank_command(self, exec_path: str, slotsfile: str) -> str:
        """Command executed by one MPI rank on its local host.

        MPI supplies one rank per host.  The rank then enters the local
        container, matching the mpirun + docker contract used by code_zy.
        """
        exec_in = os.getenv("DOCKER_EXEC_PATH", exec_path)
        slots_in = os.getenv("DOCKER_SLOTSFILE_PATH", slotsfile)
        workdir = os.getenv("DOCKER_WORKDIR", posixpath.dirname(exec_in) or ".")
        gpu_num = os.getenv("GPU_NUM", os.getenv("GPUS_PER_NODE", ""))
        exports = [
            'RANK="${OMPI_COMM_WORLD_RANK:-${PMI_RANK:-0}}"',
            'NODE_NUM="${OMPI_COMM_WORLD_SIZE:-${PMI_SIZE:-1}}"',
        ]
        if gpu_num:
            exports.extend([
                f"GPU_NUM={shlex.quote(gpu_num)}",
                f"GPUS_PER_NODE={shlex.quote(gpu_num)}",
                'NUM_PROCESSES="$((NODE_NUM * GPU_NUM))"',
            ])
        inner = (
            "set -e; "
            + " ".join(f"export {item};" for item in exports)
            + f" cd {shlex.quote(workdir)}; exec bash {shlex.quote(exec_in)} {shlex.quote(slots_in)}"
        )
        return (
            f"docker exec -i {shlex.quote(self.container_name)} bash -lc "
            f"{shlex.quote(inner)}"
        )

    def _docker_prepare_command(
        self, exec_path: str, slotsfile: str, reuse_existing: bool | None = None
    ) -> str:
        """Prepare one image/container on the local MPI host."""
        if not self.docker_image:
            raise ValueError("DOCKER_IMAGE (or IMAGE) is required for MPI-Docker mode")
        image = shlex.quote(self.docker_image)
        container = shlex.quote(self.container_name)
        image_tar = shlex.quote(self.docker_image_tar)
        workdir = os.getenv("DOCKER_WORKDIR", posixpath.dirname(os.getenv("DOCKER_EXEC_PATH", exec_path)) or "/workspace")
        mounts = ""
        if self.docker_host_share and self.docker_container_share:
            mounts += f" -v {shlex.quote(self.docker_host_share)}:{shlex.quote(self.docker_container_share)}"
        # Match the known-good training container.  The host SSH directory is
        # intentionally read-only: container setup must never copy keys into
        # it; it only verifies authorized_keys before starting sshd.
        mounts += " -v /opt/hyhal:/opt/hyhal:ro -v /root/.ssh:/root/.ssh:ro"
        run_args = f" {self.docker_extra_args}" if self.docker_extra_args else ""
        # A recovery allocation may contain a newly added node. Existing
        # containers must be kept while missing containers are created.
        reuse_existing = self.reuse_container if reuse_existing is None else reuse_existing
        reuse_check = ""
        reuse_end = ""
        new_container_init = ""
        remove_command = f"docker rm -f {container} >/dev/null 2>&1 || true; "
        if reuse_existing:
            # Check before image preparation: existing containers keep their
            # original image, mounts and runtime configuration.
            reuse_check = (
                f"if running=$(docker container inspect --format '{{{{.State.Running}}}}' {container} 2>/dev/null); then "
                f"if [ \"$running\" != true ]; then docker start {container}; fi; "
                "else "
            )
            reuse_end = "fi; "
            new_container_init = "cm_new_container=1; "
            remove_command = ""
        else:
            new_container_init = "cm_new_container=1; "
        container_start = (
            f"docker run -dit --name {container} --network=host --ipc=host --privileged "
            f"-w {shlex.quote(workdir)}{mounts}{run_args} {image} bash"
        )
        return (
            "set -eu; "
            f"cm_new_container=0; "
            f"{reuse_check}"
            f"{new_container_init}"
            f"if ! docker image inspect {image} >/dev/null 2>&1; then "
            f"if [ -n {image_tar} ] && [ -f {image_tar} ]; then docker load -i {image_tar}; "
            f"else docker pull {image}; fi; fi; "
            f"docker image inspect {image} >/dev/null; "
            f"{remove_command}"
            f"{container_start}; "
            f"{reuse_end}"
            f"{self._docker_ssh_setup_command(slotsfile)}"
        )

    def _docker_ssh_setup_command(self, _slotsfile: str) -> str:
        """Configure the shared root key and container sshd for inner MPI.

        The inner training script connects to peer containers on the host
        network using the configured container SSH port.  The host's
        read-only ``/root/.ssh`` is reused; no key is copied or generated there.
        """
        container = shlex.quote(self.container_name)
        setup = (
            "set -eu; "
            "command -v sshd >/dev/null 2>&1 || "
            "{ echo 'openssh-server/sshd is required inside the Docker image'; exit 127; }; "
            "mkdir -p /run/sshd; "
            "test -r /root/.ssh/authorized_keys || "
            "{ echo 'host /root/.ssh/authorized_keys is required' >&2; exit 127; }; "
            "ssh-keygen -A >/dev/null 2>&1 || true; "
        )
        return (
            f"docker exec -u root {container} bash -lc {shlex.quote(setup)}; "
            f"if ! docker exec {container} pgrep -f '[s]shd.*-p {self.container_ssh_port}' >/dev/null 2>&1; then "
            f"docker exec -d -u root {container} /usr/sbin/sshd -D -p {self.container_ssh_port} "
            "-o PermitRootLogin=yes -o PasswordAuthentication=no -o PubkeyAuthentication=yes "
            "-o AuthorizedKeysFile=/root/.ssh/authorized_keys; fi"
        )

    def _mpirun_command(self, rank_command: str, slotsfile: str, count: int) -> str:
        command = [self.mpirun_bin, "-np", str(count), "--hostfile", slotsfile]
        if self.allow_root:
            command.append("--allow-run-as-root")
        command.extend(["--mca", "plm_rsh_args", self.mpirun_rsh_args, "bash", "-lc", rank_command])
        return " ".join(shlex.quote(part) for part in command)

    def _start_docker(self, exec_path: str, slotsfile: str):
        hosts = self._hosts(slotsfile)
        prepare_cmd = self._mpirun_command(
            self._docker_prepare_command(exec_path, slotsfile), slotsfile, len(hosts)
        )
        logger.info("[MPI-DOCKER-PREPARE] command=%s", prepare_cmd)
        prepare_code, prepare_nodes = CmdExecutor.exec_mpirun_cmd(
            prepare_cmd, Path(exec_path).parent, True, MPI_LAUNCH_TIMEOUT
        )
        if prepare_code != 0:
            logger.error("[MPI-DOCKER-PREPARE] failed nodes=%s", prepare_nodes)
            return prepare_code, prepare_nodes
        rank_command = self._docker_rank_command(exec_path, slotsfile)
        # Inline the rank command so no launcher file must be copied to every host.
        cmd = self._mpirun_command(rank_command, slotsfile, len(hosts))
        logger.info("[MPI-DOCKER-LAUNCH] hosts=%s command=%s", ",".join(hosts), cmd)
        return CmdExecutor.exec_mpirun_cmd(cmd, Path(exec_path).parent, True, MPI_LAUNCH_TIMEOUT)

    def start(self, exec_path, slotsfile):
        """
        Launch MPI job (debug mode: only print parameters)
        """
        if self.docker_enabled:
            return self._start_docker(str(exec_path), str(slotsfile))
        work_dir = Path(exec_path).parent
        cmd = f"cd {work_dir} ; bash {exec_path} {slotsfile} 2>&1 &"
        logger.info(f"[MPI-LAUNCH] Launch parameters:{cmd},work_dir:{work_dir}")
        return CmdExecutor.exec_mpirun_cmd(cmd, work_dir, False, MPI_LAUNCH_TIMEOUT)


    def stop(self, hostfile):
        """
        Stop job
        """
        hostfile_path = Path(hostfile)
        if not hostfile_path.is_file():
            logger.info("[MPI-STOP] hostfile=%s is absent; nothing to stop", hostfile)
            return 0, []
        if not any(
            line.strip() and not line.lstrip().startswith("#")
            for line in hostfile_path.read_text(encoding="utf-8").splitlines()
        ):
            logger.info("[MPI-STOP] hostfile=%s has no nodes; skip clush", hostfile)
            return 0, []
        if self.docker_enabled:
            pattern = os.getenv("DOCKER_STOP_PATTERN", "python")
            inner = f"pkill -TERM -f {shlex.quote(pattern)} || true; sleep 2; pkill -KILL -f {shlex.quote(pattern)} || true"
            if self.remove_container_on_stop:
                inner += f"; docker rm -f {shlex.quote(self.container_name)} >/dev/null 2>&1 || true"
            cmd = (
                f"clush --hostfile {shlex.quote(str(hostfile))} -b "
                f"docker exec {shlex.quote(self.container_name)} bash -lc {shlex.quote(inner)}"
            )
        else:
            cmd = f"clush --hostfile {hostfile} -b pkill -9 -f python"
        logger.info(f"[MPI-STOP] killing python processes, hostfile={hostfile}")
        return CmdExecutor.exec_mpirun_cmd(cmd, capture_output=False, timeout=MPI_LAUNCH_TIMEOUT)


    def is_alive(self):
        """
        Check job status (debug mode always False)
        """
        logger.info("[MPI-STATUS] is_alive() called → False")
        return False
