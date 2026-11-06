# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import base64
import json
import os
import shutil
import socket
import struct
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cluster_manager.launcher.container_ssh import (
    ContainerSSHManager, INIT_SCRIPT, INSTALL_SCRIPT,
)
from cluster_manager.launcher.mpirun_launcher import MPIRunLauncher


def key(byte):
    raw = struct.pack('>I', 11) + b'ssh-ed25519' + struct.pack('>I', 32) + bytes([byte]) * 32
    return 'ssh-ed25519 ' + base64.b64encode(raw).decode()


def peer(host, byte):
    return dict(host=host, public_key=key(byte), host_key=key(byte + 10))


def test_exchange_installs_all_public_keys_before_any_probe(monkeypatch):
    manager = ContainerSSHManager('train', '22', '11452')
    hosts = ['p8c7', 'p8c9']
    installed = set()
    probes = []

    def run(host, command, data=None):
        if data is None:
            return 0, 'banner\nCM_SSH_KEYS=' + json.dumps(peer(host, hosts.index(host)))
        payload = json.loads(data)
        assert payload['peers'] == [peer('p8c7', 0), peer('p8c9', 1)]
        assert payload['port'] == '11452'
        assert 'PRIVATE KEY' not in data
        installed.add(host)
        return 0, ''

    def verify(host, targets):
        assert installed == set(hosts)
        probes.append((host, targets))
        return 0

    monkeypatch.setattr(manager, '_run', run)
    monkeypatch.setattr(manager, '_verify', verify)
    assert manager.configure(hosts) == (0, [])
    assert sorted(probes) == [('p8c7', hosts), ('p8c9', ['p8c7'])]


@pytest.mark.parametrize('phase', ['collect', 'install', 'verify'])
def test_failure_reports_host_and_does_not_continue_to_next_phase(monkeypatch, phase):
    manager = ContainerSSHManager('train', '22', '11452')
    installs = []
    probes = []

    def collect(host):
        if phase == 'collect' and host == 'p8c9':
            return None
        return peer(host, 1)

    def run(host, command, data=None):
        installs.append(host)
        return (1 if phase == 'install' and host == 'p8c9' else 0), ''

    def verify(host, targets):
        probes.append(host)
        return 1 if phase == 'verify' and host == 'p8c9' else 0

    monkeypatch.setattr(manager, '_collect', collect)
    monkeypatch.setattr(manager, '_run', run)
    monkeypatch.setattr(manager, '_verify', verify)
    assert manager.configure(['p8c7', 'p8c9']) == (1, ['p8c9'])
    if phase == 'collect':
        assert not installs
    if phase != 'verify':
        assert not probes


@pytest.mark.parametrize('output', [
    '', 'CM_SSH_KEYS={}', 'CM_SSH_KEYS=[]',
    'CM_SSH_KEYS=' + json.dumps(dict(public_key='ssh-ed25519 invalid', host_key=key(2))),
])
def test_invalid_collected_keys_are_rejected(monkeypatch, output):
    manager = ContainerSSHManager('train', '22', '11452')
    monkeypatch.setattr(manager, '_run', lambda *args: (0, output))
    assert manager._collect('p8c7') is None


def test_public_bundle_uses_stdin_and_host_ssh_port(monkeypatch):
    run = MagicMock(return_value=subprocess.CompletedProcess([], 0, '', ''))
    monkeypatch.setattr(subprocess, 'run', run)
    manager = ContainerSSHManager('container with spaces', '2200', '11452')
    assert manager._run('p8c7', ['python3', '-c', 'pass'], 'public bundle') == (0, '')
    args = run.call_args.args[0]
    assert args[args.index('-p') + 1] == '2200'
    assert args[-2] == 'p8c7'
    assert 'public bundle' not in args[-1]
    assert run.call_args.kwargs['input'] == 'public bundle'
    assert '-u root' in args[-1]


def install(root, peers, port='11452'):
    result = subprocess.run(
        [sys.executable, '-c', INSTALL_SCRIPT, str(root)],
        input=json.dumps(dict(peers=peers, port=port)), text=True, capture_output=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


def test_install_is_idempotent_and_replaces_only_managed_trust(tmp_path):
    (tmp_path / 'authorized_keys').write_text('user-existing-key\n')
    (tmp_path / 'config').write_text('Host external\n    User operator\n')
    peers = [peer('p8c7', 1), peer('p8c9', 2)]
    install(tmp_path, peers)
    first = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    install(tmp_path, peers)
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == first
    authorized = (tmp_path / 'authorized_keys').read_text()
    assert 'user-existing-key\n' in authorized
    assert authorized.count(key(1)) == 1
    assert authorized.count(key(2)) == 1
    assert '[p8c9]:11452 ' + key(12) in (tmp_path / 'known_hosts_cluster_manager').read_text()
    install(tmp_path, [peer('p8c7', 1), peer('p8c8', 3)])
    assert key(2) not in (tmp_path / 'authorized_keys').read_text()
    assert key(3) in (tmp_path / 'authorized_keys').read_text()
    config = (tmp_path / 'config').read_text()
    assert 'Host p8c7 p8c8\n' in config
    assert config.endswith('Host external\n    User operator\n')
    assert 'StrictHostKeyChecking yes' in config
    if os.name == 'posix':
        assert (tmp_path / 'authorized_keys').stat().st_mode & 0o777 == 0o600


@pytest.mark.skipif(not Path('/proc/self/mountinfo').exists() or not shutil.which('ssh-keygen'),
                    reason='Requires Linux and OpenSSH')
def test_init_generates_distinct_keys_and_reuses_existing_key(tmp_path):
    roots = [tmp_path / 'first', tmp_path / 'second']
    for root in roots:
        subprocess.run([sys.executable, '-c', INIT_SCRIPT, str(root)], check=True, timeout=30)
    private = roots[0] / 'id_ed25519_cluster_manager'
    before = private.read_bytes()
    subprocess.run([sys.executable, '-c', INIT_SCRIPT, str(roots[0])], check=True, timeout=30)
    assert private.read_bytes() == before
    assert private.stat().st_mode & 0o777 == 0o600
    assert (roots[0] / 'id_ed25519_cluster_manager.pub').read_bytes() != (
        roots[1] / 'id_ed25519_cluster_manager.pub').read_bytes()
    # An identity baked into the image must not be reused by a new container.
    subprocess.run([sys.executable, '-c', INIT_SCRIPT, str(roots[0])],
                   env={**os.environ, 'CM_NEW_CONTAINER': '1'}, check=True, timeout=30)
    assert private.read_bytes() != before


def test_init_rejects_mounted_ssh_directory_before_writing(tmp_path, monkeypatch):
    root = tmp_path / 'ssh'
    monkeypatch.setattr(sys, 'argv', ['init', str(root)])
    monkeypatch.setattr(Path, 'read_text', lambda *args: f'1 0 0:0 / {root} rw - ext4 /dev/test rw\n')
    with pytest.raises(RuntimeError, match='mounted'):
        exec(INIT_SCRIPT, {})
    assert not root.exists()


@pytest.mark.skipif(
    not hasattr(os, 'geteuid') or os.geteuid() != 0
    or not all(shutil.which(tool) for tool in ['ssh', 'sshd', 'ssh-keygen']),
    reason='Requires Linux root and OpenSSH client/server',
)
def test_real_ssh_mutual_authentication_and_host_key_check(tmp_path):
    """Two temporary loopback sshd instances; never touch /root/.ssh or its sshd."""
    hosts = ['127.0.0.2', '127.0.0.3']
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = str(sock.getsockname()[1])
    roots = [tmp_path / 'client1', tmp_path / 'client2']
    peers = []
    for host, root in zip(hosts, roots):
        subprocess.run([sys.executable, '-c', INIT_SCRIPT, str(root)], check=True, timeout=30)
        host_key = root / 'server_key'
        subprocess.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(host_key)],
                       check=True, timeout=30)
        peers.append(dict(host=host,
                          public_key=(root / 'id_ed25519_cluster_manager.pub').read_text().strip(),
                          host_key=host_key.with_suffix('.pub').read_text().strip()))
    for root in roots:
        install(root, peers, port)
    servers = []
    logs = []
    try:
        for host, root in zip(hosts, roots):
            log = (root / 'sshd.log').open('w+')
            logs.append(log)
            process = subprocess.Popen([
                shutil.which('sshd'), '-D', '-e', '-f', '/dev/null',
                '-p', port, '-h', str(root / 'server_key'),
                '-o', 'ListenAddress=' + host,
                '-o', 'AuthorizedKeysFile=' + str(root / 'authorized_keys'),
                '-o', 'PidFile=' + str(root / 'sshd.pid'),
                '-o', 'PermitRootLogin=yes', '-o', 'PasswordAuthentication=no',
                '-o', 'KbdInteractiveAuthentication=no', '-o', 'AuthenticationMethods=publickey',
                '-o', 'UsePAM=yes', '-o', 'StrictModes=no',
            ], stdout=subprocess.DEVNULL, stderr=log)
            servers.append(process)
            for _ in range(50):
                assert process.poll() is None, (root / 'sshd.log').read_text()
                try:
                    with socket.create_connection((host, int(port)), timeout=0.1):
                        break
                except OSError:
                    time.sleep(0.1)
            else:
                pytest.fail('Temporary sshd did not become ready')
        for root in roots:
            for host in hosts:
                result = subprocess.run(
                    ['ssh', '-F', str(root / 'config'), '-o', 'ConnectTimeout=3', host, 'true'],
                    capture_output=True, text=True, timeout=10,
                )
                assert result.returncode == 0, result.stderr
        # A replaced server key must be rejected, not silently accepted.
        install(roots[0], [peers[0], {**peers[1], 'host_key': key(99)}], port)
        result = subprocess.run(
            ['ssh', '-F', str(roots[0] / 'config'), '-o', 'ConnectTimeout=3', hosts[1], 'true'],
            capture_output=True, text=True, timeout=10,
        )
        assert result.returncode != 0
        assert 'Host key verification failed' in result.stderr
    finally:
        for process in servers:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        for log in logs:
            log.close()


def test_legacy_mpi_docker_does_not_launch_training_after_ssh_failure(tmp_path, monkeypatch):
    monkeypatch.setenv('DOCKER_IMAGE', 'train:latest')
    launcher = MPIRunLauncher()
    slots = tmp_path / 'hosts'
    slots.write_text('p8c7\np8c9\n')
    execute = MagicMock(return_value=(0, []))
    monkeypatch.setattr('cluster_manager.launcher.mpirun_launcher.CmdExecutor.exec_mpirun_cmd', execute)
    monkeypatch.setattr(launcher, '_configure_container_ssh', lambda hosts: (1, ['p8c9']))
    assert launcher._start_docker('/work/train.sh', str(slots)) == (1, ['p8c9'])
    assert execute.call_count == 1
