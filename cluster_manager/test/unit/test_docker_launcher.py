from unittest.mock import patch
import os
import shutil
import subprocess
import time

import pytest

from cluster_manager.launcher.docker_launcher import DockerLauncher
from cluster_manager.launcher.launcher_factory import create_launcher


def _slots_file(tmp_path):
    path = tmp_path / "slots.txt"
    path.write_text("node01 slots=8\nnode02 slots=8\n", encoding="utf-8")
    return path


@pytest.fixture(autouse=True)
def _container_share_root(monkeypatch):
    # Production start scripts set this to the host-mounted shared directory;
    # tests provide a deterministic container path for the generated setup.
    monkeypatch.setenv("DOCKER_CONTAINER_SHARE_ROOT", "/share")


def test_factory_creates_docker_launcher(monkeypatch):
    monkeypatch.setenv("CLUSTER_LAUNCH_MODE", "docker")
    assert isinstance(create_launcher(), DockerLauncher)


@pytest.mark.parametrize("value, expected", [("22", "-p 22"), ("-p 11452", "-p 11452")])
def test_outer_mpi_uses_canonical_host_ssh_port(value, expected, monkeypatch):
    monkeypatch.setenv("MPI_HOST_SSH_PORT", value)
    monkeypatch.delenv("MPIRUN_PLM_RSH_ARGS", raising=False)
    launcher = DockerLauncher()
    assert launcher.mpirun_rsh_args == expected


def test_legacy_outer_mpi_port_is_backward_compatible(monkeypatch):
    monkeypatch.delenv("MPI_HOST_SSH_PORT", raising=False)
    monkeypatch.setenv("MPIRUN_PLM_RSH_ARGS", "-p 22")
    assert DockerLauncher().mpirun_rsh_args == "-p 22"


def test_outer_mpi_rejects_container_port_syntax(monkeypatch):
    monkeypatch.setenv("MPI_HOST_SSH_PORT", "--p 11452")
    with pytest.raises(ValueError, match="port number"):
        DockerLauncher()


def test_docker_cluster_prepare_then_start_launches_coordinator(tmp_path, monkeypatch):
    monkeypatch.setenv("DOCKER_CONTAINER_NAME", "train")
    monkeypatch.setenv("DOCKER_IMAGE", "registry/train:latest")
    launcher = DockerLauncher()
    slotsfile = _slots_file(tmp_path)

    with patch(
        "cluster_manager.launcher.docker_launcher.CmdExecutor.execute_command",
        side_effect=[(0, ""), (0, "")],
    ) as execute:
        prepare_code, prepare_failed = launcher.prepare_containers(
            "/workspace/run.sh", str(slotsfile)
        )
        code, failed = launcher.start("/workspace/run.sh", str(slotsfile))

    assert (prepare_code, prepare_failed) == (0, [])
    assert code == 0
    assert failed == ""
    prepare_command = execute.call_args_list[0].args[0]
    train_command = execute.call_args_list[1].args[0]
    assert "clush" in prepare_command
    assert "--hostfile" in prepare_command
    assert "docker image inspect" in prepare_command
    assert "docker run -dit" in prepare_command
    assert "-v /opt/hyhal:/opt/hyhal:ro" in prepare_command
    assert "-v /root/.ssh:/root/.ssh:ro" in prepare_command
    assert "docker exec -d -u root train /usr/sbin/sshd -D -p" in prepare_command
    assert "nohup sshd" not in prepare_command
    # Training starts in the first allocated host's container through SSH.
    assert train_command.startswith("ssh -p 22 node01")
    assert "docker exec -d train" in train_command
    assert "/workspace/run.sh" in train_command


def test_docker_start_uses_configured_host_ssh_port(tmp_path, monkeypatch):
    monkeypatch.setenv("DOCKER_CONTAINER_NAME", "train")
    monkeypatch.setenv("DOCKER_IMAGE", "registry/train:latest")
    monkeypatch.setenv("MPI_HOST_SSH_PORT", "11452")
    launcher = DockerLauncher()
    slotsfile = _slots_file(tmp_path)

    with patch(
        "cluster_manager.launcher.docker_launcher.CmdExecutor.execute_command",
        return_value=(0, ""),
    ) as execute:
        launcher.start("/workspace/run.sh", str(slotsfile))

    assert execute.call_args.args[0].startswith("ssh -p 11452 node01")


def test_prepare_new_containers_reuses_existing_container(tmp_path, monkeypatch):
    monkeypatch.setenv("DOCKER_CONTAINER_NAME", "train")
    monkeypatch.setenv("DOCKER_IMAGE", "registry/train:latest")
    launcher = DockerLauncher()
    slotsfile = _slots_file(tmp_path)

    with patch(
        "cluster_manager.launcher.docker_launcher.CmdExecutor.execute_command",
        return_value=(0, ""),
    ) as execute:
        code, failed = launcher.prepare_new_containers(
            "/workspace/run.sh", str(slotsfile)
        )

    assert (code, failed) == (0, [])
    command = execute.call_args.args[0]
    assert "clush" in command
    assert "docker container inspect" in command
    assert "docker rm -f" not in command


def test_docker_start_never_installs_an_rsh_agent(tmp_path, monkeypatch):
    """Regression: a plm_rsh_agent wrapper made ssh reject the remote command.

    PRTE calls the agent as ``agent <ssh args> HOST COMMAND``, so a wrapper
    reading ``$1`` as the hostname gets ``-p`` and ssh fails with ``Bad port``.
    """
    monkeypatch.setenv("DOCKER_CONTAINER_NAME", "train")
    monkeypatch.setenv("DOCKER_IMAGE", "registry/train:latest")
    launcher = DockerLauncher()
    slotsfile = _slots_file(tmp_path)

    with patch(
        "cluster_manager.launcher.docker_launcher.CmdExecutor.execute_command",
        side_effect=[(0, ""), (0, "")],
    ) as execute:
        launcher.prepare_containers("/workspace/run.sh", str(slotsfile))
        launcher.start("/workspace/run.sh", str(slotsfile))

    for call in execute.call_args_list:
        command = call.args[0]
        assert "plm_rsh_agent" not in command
        assert "plm_ssh_agent" not in command


def test_docker_start_pins_tcp_interface_when_configured(tmp_path, monkeypatch):
    monkeypatch.setenv("DOCKER_CONTAINER_NAME", "train")
    monkeypatch.setenv("DOCKER_IMAGE", "registry/train:latest")
    monkeypatch.setenv("MPI_TCP_IF_INCLUDE", "10.211.9.0/24")
    launcher = DockerLauncher()
    slotsfile = _slots_file(tmp_path)

    with patch(
        "cluster_manager.launcher.docker_launcher.CmdExecutor.exec_mpirun_cmd",
        return_value=(0, []),
    ), patch(
        "cluster_manager.launcher.docker_launcher.CmdExecutor.execute_command",
        return_value=(0, ""),
    ) as launch:
        launcher.start("/workspace/run.sh", str(slotsfile))

    command = launch.call_args.args[0]
    assert "OMPI_MCA_oob_tcp_if_include=10.211.9.0/24" in command
    assert "PRTE_MCA_oob_tcp_if_include=10.211.9.0/24" in command


def test_docker_start_forwards_rank_env_to_every_rank(tmp_path, monkeypatch):
    """A bare export would only reach the coordinator node's own ranks.

    mpirun does not forward arbitrary environment variables to remote ranks, so
    an interface pin has to travel via mca_base_env_list or the peer node keeps
    autoselecting its own interface -- which is the mismatch being fixed.
    """
    monkeypatch.setenv("DOCKER_CONTAINER_NAME", "train")
    monkeypatch.setenv("DOCKER_IMAGE", "registry/train:latest")
    monkeypatch.setenv(
        "MPI_RANK_ENV", "NCCL_SOCKET_IFNAME=ib1, GLOO_SOCKET_IFNAME=ib1"
    )
    launcher = DockerLauncher()

    with patch(
        "cluster_manager.launcher.docker_launcher.CmdExecutor.exec_mpirun_cmd",
        return_value=(0, []),
    ), patch(
        "cluster_manager.launcher.docker_launcher.CmdExecutor.execute_command",
        return_value=(0, ""),
    ) as launch:
        launcher.start("/workspace/run.sh", str(_slots_file(tmp_path)))

    command = launch.call_args.args[0]
    assert "export NCCL_SOCKET_IFNAME=ib1" in command
    assert "OMPI_MCA_mca_base_env_list=" in command
    # Semicolon-separated, which is what mca_base_env_list expects.
    assert "NCCL_SOCKET_IFNAME=ib1;GLOO_SOCKET_IFNAME=ib1" in command


def test_rank_env_is_inert_when_unset(tmp_path, monkeypatch):
    monkeypatch.delenv("MPI_RANK_ENV", raising=False)
    monkeypatch.delenv("MPI_FORWARD_ENV", raising=False)
    monkeypatch.setenv("DOCKER_IMAGE", "registry/train:latest")
    assert DockerLauncher()._rank_env_exports() == ""


def test_forward_env_resolves_values_inside_the_container(monkeypatch):
    """The value that matters lives in the container, not in the manager.

    Regression: the RCCL network plugin is only on the image ENV's
    LD_LIBRARY_PATH, which the peer node's ssh-spawned ranks never receive, so
    they fall back to NET/Socket while the coordinator uses the IB plugin.
    """
    bash = shutil.which("bash")
    if not bash:
        pytest.skip("Bash is required to execute the generated snippet")
    monkeypatch.setenv("DOCKER_IMAGE", "registry/train:latest")
    monkeypatch.setenv("MPI_RANK_ENV", "NCCL_SOCKET_IFNAME=ib1")
    monkeypatch.setenv("MPI_FORWARD_ENV", "LD_LIBRARY_PATH,CM_ABSENT")

    snippet = DockerLauncher()._rank_env_exports()
    result = subprocess.run(
        [bash, "-c", "set -eu; " + snippet + 'printf "%s" "$OMPI_MCA_mca_base_env_list"'],
        env={"PATH": os.environ["PATH"], "LD_LIBRARY_PATH": "/opt/plugins/lib"},
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30,
    )
    assert result.returncode == 0, result.stderr
    # Static pairs first, then the live value read from the container's shell.
    # CM_ABSENT is unset there and must not appear as a bare "CM_ABSENT=".
    assert result.stdout.decode() == (
        "NCCL_SOCKET_IFNAME=ib1;LD_LIBRARY_PATH=/opt/plugins/lib"
    )


def test_rank_env_rejects_entries_without_a_value(monkeypatch):
    monkeypatch.setenv("DOCKER_IMAGE", "registry/train:latest")
    monkeypatch.setenv("MPI_RANK_ENV", "NCCL_SOCKET_IFNAME")
    with pytest.raises(ValueError, match="VAR=value"):
        DockerLauncher()


def test_docker_stop_sweeps_inside_container_as_root(tmp_path, monkeypatch):
    monkeypatch.setenv("DOCKER_CONTAINER_NAME", "train")
    monkeypatch.setenv("DOCKER_IMAGE", "registry/train:latest")
    launcher = DockerLauncher()
    slotsfile = _slots_file(tmp_path)

    with patch(
        "cluster_manager.launcher.docker_launcher.CmdExecutor.exec_mpirun_cmd",
        return_value=(0, []),
    ) as execute:
        code, failed = launcher.stop(str(slotsfile))

    assert code == 0
    assert failed == []
    command = execute.call_args.args[0]
    assert "clush" in command
    # Root inside the container: on the host the manager runs as an unprivileged
    # user and cannot signal the root-owned training processes.
    assert "docker exec -u root train" in command
    assert "prted" in command
    assert DockerLauncher._STOP_GUARD in command


@pytest.mark.parametrize("content", ["", "# no healthy nodes\n", "\n  # comment\n"])
def test_docker_stop_empty_hostfile_is_idempotent(tmp_path, monkeypatch, content):
    monkeypatch.setenv("DOCKER_CONTAINER_NAME", "train")
    monkeypatch.setenv("DOCKER_IMAGE", "registry/train:latest")
    hostfile = tmp_path / "normal_nodes.txt"
    hostfile.write_text(content, encoding="utf-8")

    with patch(
        "cluster_manager.launcher.docker_launcher.CmdExecutor.exec_mpirun_cmd"
    ) as execute:
        code, nodes = DockerLauncher().stop(str(hostfile))

    assert (code, nodes) == (0, [])
    execute.assert_not_called()


def test_docker_stop_missing_hostfile_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.setenv("DOCKER_CONTAINER_NAME", "train")
    monkeypatch.setenv("DOCKER_IMAGE", "registry/train:latest")

    with patch(
        "cluster_manager.launcher.docker_launcher.CmdExecutor.exec_mpirun_cmd"
    ) as execute:
        code, nodes = DockerLauncher().stop(str(tmp_path / "missing.txt"))

    assert (code, nodes) == (0, [])
    execute.assert_not_called()


def test_docker_stop_keeps_a_reused_container(tmp_path, monkeypatch):
    monkeypatch.setenv("DOCKER_CONTAINER_NAME", "train")
    monkeypatch.setenv("DOCKER_IMAGE", "registry/train:latest")
    monkeypatch.setenv("DOCKER_REUSE_CONTAINER", "1")
    monkeypatch.setenv("DOCKER_REMOVE_CONTAINER_ON_STOP", "1")
    launcher = DockerLauncher()

    with patch(
        "cluster_manager.launcher.docker_launcher.CmdExecutor.exec_mpirun_cmd",
        return_value=(0, []),
    ) as execute:
        launcher.stop(str(_slots_file(tmp_path)))

    assert "docker rm -f" not in execute.call_args.args[0]


def test_stop_script_kills_targets_without_killing_itself(tmp_path):
    """The sweep must survive its own pattern, or the SIGKILL never runs."""
    bash = shutil.which("bash")
    if not bash or not os.path.isdir("/proc"):
        pytest.skip("Requires bash and a Linux /proc")

    marker = "cm_stop_victim_marker"
    victims = [
        subprocess.Popen(["bash", "-c", f"exec -a {marker}{i} sleep 60"])
        for i in range(2)
    ]
    try:
        time.sleep(0.5)
        script = DockerLauncher._stop_script([marker])
        # The wrapper's own cmdline contains the marker; it must not self-kill.
        result = subprocess.run(
            [bash, "-lc", script], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=30,
        )
        assert result.returncode == 0, result.stderr
        assert result.stderr == b""
        for victim in victims:
            assert victim.wait(timeout=10) != 0
    finally:
        for victim in victims:
            if victim.poll() is None:
                victim.kill()



@pytest.mark.parametrize(
    "reuse,state,start_code,expected_code,expected_actions",
    [
        # Both new and reused containers check the read-only SSH mount and
        # ensure that the detached sshd is running.
        ("1", "true", 0, 0, ["container", "exec", "exec"]),
        ("true", "false", 0, 0, ["container", "start", "exec", "exec"]),
        ("1", "missing", 0, 0, ["container", "image", "image", "run", "exec", "exec"]),
        ("1", "false", 7, 7, ["container", "start"]),
        (None, "true", 0, 0, ["image", "image", "rm", "run", "exec", "exec"]),
    ],
)
def test_container_reuse_shell_branches(
    monkeypatch, reuse, state, start_code, expected_code, expected_actions
):
    bash = shutil.which("bash")
    if not bash:
        pytest.skip("Bash is required to execute the generated preparation script")
    monkeypatch.setenv("DOCKER_IMAGE", "registry/train:latest")
    monkeypatch.setenv("DOCKER_CONTAINER_NAME", "train")
    if reuse is None:
        monkeypatch.delenv("DOCKER_REUSE_CONTAINER", raising=False)
    else:
        monkeypatch.setenv("DOCKER_REUSE_CONTAINER", reuse)
    # Record calls through FD 3 even when the production command redirects
    # stdout/stderr. This shell function never invokes the real Docker CLI.
    fake_docker = f'''
exec 3>&1
docker() {{
    printf '%s\\n' "$1" >&3
    case "$1 $2" in
        'container inspect')
            [ '{state}' != missing ] || return 1
            printf '%s\\n' '{state}' ;;
        'start train') return {start_code} ;;
        *) return 0 ;;
    esac
}}
'''
    command = DockerLauncher()._docker_prepare_command("/workspace/run.sh", "/slots")
    shell = [bash, "-s"]
    if "system32" in bash.lower() and shutil.which("wsl"):
        shell = [shutil.which("wsl"), "--exec", "bash", "-s"]
    result = subprocess.run(
        shell, input=(fake_docker + command + "\n").encode("utf-8"),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30,
    )
    assert result.returncode == expected_code, result.stderr
    assert result.stdout.decode("utf-8").splitlines() == expected_actions
