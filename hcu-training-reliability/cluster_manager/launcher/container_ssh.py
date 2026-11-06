# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Provision root SSH trust using public keys collected over host SSH."""

import json
import shlex
import subprocess
from concurrent.futures import ThreadPoolExecutor

from cluster_manager.config.global_config import logger, MPI_LAUNCH_TIMEOUT


# Executed inside each container. Private keys never leave that container.
INIT_SCRIPT = r'''
import os, subprocess, sys
from pathlib import Path
root = Path(sys.argv[1])
if root.is_symlink():
    raise RuntimeError("Container SSH directory must not be a symlink")
# Old launchers bind-mounted the host's SSH directory. Never modify that mount.
for line in Path('/proc/self/mountinfo').read_text().splitlines():
    mount = Path(line.split()[4].replace(r'\040', ' '))
    if mount != Path('/') and (mount == root or mount in root.parents or root in mount.parents):
        raise RuntimeError("Container SSH directory is mounted; recreate the container without the host SSH mount")
root.mkdir(mode=0o700, parents=True, exist_ok=True)
os.chmod(root, 0o700)
key = root / 'id_ed25519_cluster_manager'
if key.is_symlink() or key.with_suffix('.pub').is_symlink():
    raise RuntimeError("Managed SSH keys must not be symlinks")
# A committed image may contain the managed identity. Only a reused container
# should retain it; a newly created container needs its own key.
if os.environ.get('CM_NEW_CONTAINER') == '1':
    key.unlink(missing_ok=True)
    key.with_suffix('.pub').unlink(missing_ok=True)
if not key.exists():
    subprocess.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(key)], check=True)
os.chmod(key, 0o600)
public = subprocess.check_output(['ssh-keygen', '-y', '-P', '', '-f', str(key)], text=True)
key.with_suffix('.pub').write_text(public)
os.chmod(key.with_suffix('.pub'), 0o644)
for name in ['authorized_keys', 'config']:
    path = root / name
    if path.is_symlink():
        raise RuntimeError("SSH configuration must not be a symlink: " + str(path))
    path.touch(exist_ok=True)
    os.chmod(path, 0o600)
'''

COLLECT_SCRIPT = r'''
import json
from pathlib import Path
print('CM_SSH_KEYS=' + json.dumps({
    'public_key': Path('/root/.ssh/id_ed25519_cluster_manager.pub').read_text().strip(),
    'host_key': Path('/etc/ssh/ssh_host_ed25519_key.pub').read_text().strip(),
}))
'''

# A managed block can be replaced on recovery without discarding user entries.
# Data travels on stdin, avoiding command-length limits for large allocations.
INSTALL_SCRIPT = r'''
import json, os, sys, tempfile
from pathlib import Path
root = Path(sys.argv[1])
data = json.load(sys.stdin)
begin = '# BEGIN CLUSTER MANAGER SSH'
end = '# END CLUSTER MANAGER SSH'
def write(name, content, managed=False):
    path = root / name
    if path.is_symlink():
        raise RuntimeError("SSH configuration must not be a symlink: " + str(path))
    if managed:
        old = path.read_text() if path.exists() else ''
        if begin in old or end in old:
            if old.count(begin) != 1 or old.count(end) != 1 or old.index(begin) > old.index(end):
                raise RuntimeError("Malformed managed SSH block: " + str(path))
            before, rest = old.split(begin, 1)
            _, after = rest.split(end, 1)
            old = before + after.lstrip('\n')
        content = begin + '\n' + content + end + '\n' + old
    fd, tmp = tempfile.mkstemp(dir=root, prefix='.cm-ssh-')
    try:
        with os.fdopen(fd, 'w') as stream:
            stream.write(content)
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
keys = list(dict.fromkeys(peer['public_key'] for peer in data['peers']))
write('authorized_keys', '\n'.join(keys) + '\n', managed=True)
port = data['port']
known = []
for peer in data['peers']:
    host = peer['host']
    address = host if port == '22' else '[' + host + ']:' + port
    known.append(address + ' ' + peer['host_key'])
write('known_hosts_cluster_manager', '\n'.join(known) + '\n')
config = ('Host ' + ' '.join(peer['host'] for peer in data['peers']) + '\n'
          '    User root\n'
          '    Port ' + port + '\n'
          '    IdentityFile ' + str(root / 'id_ed25519_cluster_manager') + '\n'
          '    IdentitiesOnly yes\n'
          '    BatchMode yes\n'
          '    StrictHostKeyChecking yes\n'
          '    UserKnownHostsFile ' + str(root / 'known_hosts_cluster_manager') + '\n'
          'Host *\n')
write('config', config, managed=True)
'''


class ContainerSSHManager:
    def __init__(self, container, host_port, container_port):
        self.container = container
        self.host_port = host_port
        self.container_port = container_port

    def _run(self, host, command, data=None):
        remote = shlex.join(['docker', 'exec', '-i', '-u', 'root', self.container, *command])
        try:
            result = subprocess.run(
                ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
                 '-p', self.host_port, '--', host, remote],
                input=data or '', text=True, capture_output=True, timeout=MPI_LAUNCH_TIMEOUT,
            )
            if result.returncode:
                logger.error('[CONTAINER-SSH] host=%s code=%s stderr=%s',
                             host, result.returncode, result.stderr)
            return result.returncode, result.stdout
        except (OSError, subprocess.TimeoutExpired) as exc:
            logger.error('[CONTAINER-SSH] host=%s command failed: %s', host, exc)
            return 1, ''

    @staticmethod
    def _key(value):
        parts = str(value).split()
        if len(parts) < 2 or parts[0] != 'ssh-ed25519':
            raise ValueError('Expected an Ed25519 public key')
        # Validate the SSH wire format as well as the base64 envelope.
        import base64
        import struct
        raw = base64.b64decode(parts[1], validate=True)
        if (len(raw) != 51 or raw[:4] != struct.pack('>I', 11)
                or raw[4:15] != b'ssh-ed25519' or raw[15:19] != struct.pack('>I', 32)):
            raise ValueError('Invalid Ed25519 public key')
        return ' '.join(parts[:2])

    def _collect(self, host):
        code, output = self._run(host, ['python3', '-c', COLLECT_SCRIPT])
        if code:
            return None
        try:
            lines = [line.removeprefix('CM_SSH_KEYS=') for line in output.splitlines()
                     if line.startswith('CM_SSH_KEYS=')]
            if len(lines) != 1:
                raise ValueError('Missing or ambiguous container SSH key response')
            data = json.loads(lines[0])
            return dict(host=host, public_key=self._key(data['public_key']),
                        host_key=self._key(data['host_key']))
        except (ValueError, KeyError, TypeError) as exc:
            logger.error('[CONTAINER-SSH] invalid public keys from %s: %s', host, exc)
            return None

    def _verify(self, host, peers):
        # Verify the same root SSH configuration that inner mpirun will use.
        targets = ' '.join(shlex.quote(peer) for peer in peers)
        script = (
            'set -eu; for peer in ' + targets + '; do '
            'ok=0; for attempt in 1 2 3; do '
            'if ssh -o BatchMode=yes -o ConnectTimeout=5 -o ConnectionAttempts=1 '
            '-o StrictHostKeyChecking=yes "$peer" true; then ok=1; break; fi; '
            'sleep 1; done; '
            '[ "$ok" = 1 ] || { echo "Container SSH failed: $peer" >&2; exit 1; }; done'
        )
        return self._run(host, ['bash', '-c', script])[0]

    def configure(self, hosts):
        """Exchange keys, then verify coordinator <-> peers before training."""
        import re
        # Hostfile tokens become SSH config patterns: disallow wildcard/options.
        if any(not re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.:-]*', host) for host in hosts):
            raise ValueError('Container SSH requires literal hostnames or IP addresses')
        if not hosts:
            return 0, []
        with ThreadPoolExecutor(max_workers=min(32, len(hosts))) as executor:
            peers = list(executor.map(self._collect, hosts))
            failed = [host for host, peer in zip(hosts, peers) if peer is None]
            if failed:
                return 1, failed
            payload = json.dumps(dict(port=self.container_port, peers=peers))
            def install(host):
                return self._run(host, ['python3', '-c', INSTALL_SCRIPT, '/root/.ssh'], payload)[0]
            codes = list(executor.map(install, hosts))
            failed = [host for host, code in zip(hosts, codes) if code]
            if failed:
                return 1, failed
            # O(N) probes cover each client's key and each server's authorization.
            def verify(host):
                return self._verify(host, hosts if host == hosts[0] else [hosts[0]])
            codes = list(executor.map(verify, hosts))
            failed = [host for host, code in zip(hosts, codes) if code]
        if failed:
            logger.error('[CONTAINER-SSH] passwordless verification failed: %s', failed)
            return 1, failed
        logger.info('[CONTAINER-SSH] configured and verified %d containers', len(hosts))
        return 0, []
