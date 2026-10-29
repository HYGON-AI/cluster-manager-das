# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Acceptance contracts: environment outcomes are allowed; missing execution is not.

All fixtures are local. The optional full Shell test invokes a fixture CLI and
never opens SSH/Docker connections or runs a GPU workload.
"""
from __future__ import annotations

import csv
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stdout, redirect_stderr
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/test.sh"


def validator():
    source = SCRIPT.read_text(encoding="utf-8").split("<<'VALIDATOR' || LOCAL_RC=1\n", 1)[1].split("\nVALIDATOR\n", 1)[0]
    namespace = {"__name__": "acceptance_contract"}
    exec(compile(source, str(SCRIPT) + "::validator", "exec"), namespace)
    return namespace


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def matrix_cases(root, hostfile):
    import string
    text = SCRIPT.read_text(encoding="utf-8").split('<<CASES\n', 1)[1].split('\nCASES\n', 1)[0]
    values = dict(RUN_DIR=str(root), HOSTFILE=str(hostfile), SHARED_ENV_SCRIPT='/target/shared.sh',
                  NODE_LOCAL_ENV_SCRIPT='/target/local.sh', CONTAINER_ENV_SCRIPT='/target/container.sh',
                  SHARED_RCCL_ENV_SCRIPT='/target/shared-rccl.sh', NODE_LOCAL_RCCL_ENV_SCRIPT='/target/local-rccl.sh',
                  CONTAINER_RCCL_ENV_SCRIPT='/target/container-rccl.sh')
    values.update({key: 'dry' for key in ('PROFILE_MODE','NETWORK_MODE')})
    (root / 'launcher-hostfile').write_text('n01\nn02\n', encoding='utf-8')
    return list(csv.DictReader(io.StringIO(string.Template(text).substitute(values)), delimiter='\t'))


def emit_fixture(root, case, meta):
    """Return a valid receipt using the production report shapes, without SSH."""
    from cluster_run.hostfile import read_nodes
    nodes = read_nodes(Path(case['hostfile'])) if case['hostfile'] != '-' else []
    kind, dry = case['kind'], case['mode'] == 'dry'
    status = 'DRY_RUN' if dry else 'PASS'
    if kind == 'help':
        return 'hcu-cluster-run 0.4.2\n'
    if kind in ('status','lifecycle'):
        return f'RESULT        {status}\nNODES         total={len(nodes)}\n'
    directory = root / case['directory'] / 'run_fixture'
    if kind == 'basic':
        record = {'members': nodes, 'reachable': True, 'status': 'BLOCKED'}
        report = {'schema_version':'2.0', 'run':{'execution':{'scenario':case['scenario'], 'env_script':case['env_script'],
                  'scope':'container' if case['scenario']=='per-node-container' else 'host',
                  'categories':case['operation'].split(','), 'status':'PASS', 'failed_nodes':[]}},
                  'cluster':{'status':'BLOCKED'}}
        for section in ('node_status','execution_evidence','driver_dtk','software_components','network_rdma',
                        'network_health','hardware_devices','system','resource_state'):
            report[section] = {'nodes':{'n[01-02]':record}}
        path = directory / 'cluster-result.json'
    elif kind == 'active':
        profile = 'rccl-tests' if case['operation']=='rccl' else None
        requested = 'mpirun' if case['operation']=='rccl' else 'direct'
        effective = ('server-client' if case['operation']=='ib-write-bw' else 'rccl-tests')
        dispatched = nodes[:1]
        group = {'group':'group-000', 'nodes':list(nodes), 'leader':nodes[0], 'status':status,
                 'launcher':effective, 'requested_launcher':requested, 'execution_profile':profile,
                 'commands':[{'node':node, 'command':['bash','-c','source /target/env.sh; exec true']} for node in dispatched]}
        group_dir = directory / 'groups/group-000'
        write_json(group_dir / 'launch.json', group)
        (group_dir / 'hostfile').write_text(''.join(node + ' slots=1\n' for node in nodes), encoding='utf-8')
        if case['operation'] == 'ib-write-bw':
            write_json(group_dir / 'ib-write-bw-result.json', {'command':['ibstat'], 'pairing':'all-directions-per-discovered-HCA',
                       'config':{'enabled':True}, 'status':status, 'pairs':[]})
            group['returncode'] = 0
        if not dry:
            group['node_results'] = []
            for node in dispatched:
                stdout = group_dir / 'nodes' / node / 'stdout.log'
                stdout.parent.mkdir(parents=True, exist_ok=True)
                stdout.write_text(context_line(node,case,meta), encoding='utf-8')
                (stdout.parent/'stderr.log').write_text('', encoding='utf-8')
                group['node_results'].append(dict(node=node, status='PASS', returncode=0))
        write_json(group_dir / 'result.json',group)
        report = {'schema_version':'1.1', 'scenario':case['scenario'], 'test_name':case['operation'], 'status':status,
                  'profile':profile, 'launcher':requested, 'execution_scope':'container' if case['scenario']=='per-node-container' else 'host',
                  'env_script':case['env_script'], 'nodes':list(nodes), 'group_count':1, 'groups':[group]}
        path = directory / 'active-result.json'
    else:
        report = {'schema_version':'1.0', 'operation':case['operation'], 'status':status,
                  'scenario':case['scenario'], 'env_script':case['env_script'],
                  'execution_scope':'container' if case['scenario']=='per-node-container' else 'host', 'nodes':{}}
        for node in nodes:
            record = dict(status=status, command=['bash','-c','source /target/env.sh; exec true'])
            if not dry:
                stdout = directory / 'nodes' / node / 'stdout.log'
                stdout.parent.mkdir(parents=True, exist_ok=True)
                stdout.write_text(context_line(node,case,meta), encoding='utf-8')
                (stdout.parent/'stderr.log').write_text('', encoding='utf-8')
                record.update(returncode=0, stdout=str(stdout), stderr=str(stdout.parent/'stderr.log'))
            report['nodes'][node] = record
        path = directory / (case['operation']+'-result.json')
    write_json(path,report)
    status = report.get('status', report.get('cluster', {}).get('status'))
    return f'RESULT        {status}\nJSON          {path}\n'


def context_line(node, case, meta):
    return 'HCU_CONTEXT_JSON ' + json.dumps(dict(token=meta['token'], node=node,
        scope='container' if case['scenario']=='per-node-container' else 'host', pid=1234, python='/target/python')) + '\n'


class AcceptanceContractTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.hostfile = self.root/'hostfile'
        self.hostfile.write_text('n01\nn02\n', encoding='utf-8')
        self.meta = dict(started=time.time(), token='test-token', nodes=['n01','n02'])
        self.contract = validator()
        self.cases = matrix_cases(self.root,self.hostfile)
        self.populate()

    def populate(self):
        write_json(self.root/'meta.json',self.meta)
        with (self.root/'expected.tsv').open('w',encoding='utf-8',newline='') as stream:
            writer=csv.DictWriter(stream,fieldnames=list(self.cases[0]),delimiter='\t')
            writer.writeheader(); writer.writerows(self.cases)
        with (self.root/'calls.tsv').open('w',encoding='utf-8',newline='') as stream:
            writer=csv.DictWriter(stream,fieldnames=['case','returncode','log_returncode'],delimiter='\t')
            writer.writeheader()
            for case in self.cases:
                text=emit_fixture(self.root,case,self.meta)
                logfile=self.root/'logs'/ (case['case']+'.log')
                logfile.parent.mkdir(exist_ok=True)
                logfile.write_text(text,encoding='utf-8')
                writer.writerow(dict(case=case['case'],returncode=0,log_returncode=0))

    def validate(self):
        with redirect_stdout(io.StringIO()):
            code=self.contract['validate_run'](self.root)
        report=json.loads((self.root/'acceptance-result.json').read_text(encoding='utf-8'))
        return code, report

    def case(self, name):
        return next(item for item in self.cases if item['case']==name)

    def real_fixture(self, name):
        case=dict(self.case(name),mode='real')
        text=emit_fixture(self.root,case,self.meta)
        (self.root/'logs'/(name+'.log')).write_text(text,encoding='utf-8')
        filename='active-result.json'
        path=next((self.root/case['directory']).rglob(filename))
        return case,path,json.loads(path.read_text(encoding='utf-8'))

    def verify_one(self, case, rc=0):
        return self.contract['verify_case'](self.root,case,dict(returncode=rc,log_returncode=0),self.meta)


    def test_rccl_binary_matrix_does_not_silently_select_only_all_reduce(self):
        source = SCRIPT.read_text(encoding='utf-8')
        for scenario in ('shared-conda', 'node-local-conda', 'per-node-container'):
            block = source.split('# ' + scenario + '-rccl-tests:', 1)[1].split('\n# ', 1)[0]
            self.assertNotIn('--tests', block)
            self.assertIn('--gpus-per-node "$RCCL_GPUS_PER_NODE"', block)


    def test_stale_report_is_rejected(self):
        case=self.case('shared-conda-resource')
        path=next((self.root/case['directory']).rglob('cluster-result.json'))
        os.utime(path,(1,1))
        self.assertEqual(self.validate()[0],1)

    def test_blocked_environment_is_accepted_but_not_reported_as_healthy(self):
        code,report=self.validate()
        self.assertEqual(code,0)
        item=next(item for item in report['cases'] if item['case']=='shared-conda-resource')
        self.assertEqual(item['status'],'BLOCKED')

    def test_basic_categories_environment_and_scope_are_checked_individually(self):
        case=self.case('shared-conda-resource')
        path=next((self.root/case['directory']).rglob('cluster-result.json'))
        original=json.loads(path.read_text(encoding='utf-8'))
        for key,value in (('categories',['platform']),('env_script','/wrong/env.sh'),('scope','container')):
            with self.subTest(field=key):
                data=json.loads(json.dumps(original))
                data['run']['execution'][key]=value
                write_json(path,data)
                code,report=self.validate()
                self.assertEqual(code,1)
                self.assertEqual([item['case'] for item in report['cases'] if item.get('error')],[case['case']])

    def test_resource_health_incomplete_is_not_an_execution_error(self):
        case=self.case('shared-conda-resource')
        path=next((self.root/case['directory']).rglob('cluster-result.json'))
        data=json.loads(path.read_text(encoding='utf-8'))
        for status in ('BLOCKED','INCOMPLETE'):
            with self.subTest(status=status):
                data['cluster']['status']=status
                for record in data['node_status']['nodes'].values(): record['status']=status
                write_json(path,data)
                self.assertEqual(self.verify_one(case)['execution_state'],'EXECUTION_ATTEMPTED')
        data['run']['execution'].update(status='FAIL',failed_nodes=['n01'])
        write_json(path,data)
        self.assertEqual(self.verify_one(case)['execution_state'],'INTERFACE_ERROR')


    def test_removed_rccl_worker_is_rejected_without_execution(self):
        from tests.test_launch_contract import run_cli
        case = self.case('shared-conda-rccl-tests')
        result, calls = run_cli(self, ['shared-conda', 'rccl', '-f', case['hostfile'],
                                    '--profile', 'worker', '--dry-run'])
        self.assertEqual(result.returncode, 2)
        self.assertIn('unsupported staged argument: --profile', result.stderr)
        self.assertEqual(result.stdout, '')
        self.assertEqual(calls, [], 'rejected modes must never reach SSH or Docker')


    def test_precheck_is_blocked_not_executed_and_never_accepted_in_dryrun(self):
        case=dict(self.case('per-node-container-rccl-tests'),mode='real')
        (self.root/'logs'/ (case['case']+'.log')).write_text(
            '[ERROR] nodes=n[01-02] code=DCU_BUSY reason=in use\nRESULT        PRECHECK_FAILED\n',encoding='utf-8')
        receipt=dict(returncode='3',log_returncode='0')
        result=self.contract['verify_case'](self.root,case,receipt,self.meta)
        self.assertEqual(result['execution_state'],'BLOCKED_NOT_EXECUTED')
        self.assertIsNone(result['report'])
        case['mode']='dry'
        self.assertEqual(self.contract['verify_case'](self.root,case,receipt,self.meta)['execution_state'],'INTERFACE_ERROR')


class CompactAcceptanceContractTests(unittest.TestCase):
    """New Bash compact artifacts, independent of the frozen legacy matrix."""
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.hostfile = self.root / 'hosts'
        self.hostfile.write_text('n01\nn02\n', encoding='utf-8')
        self.case = dict(case='compact-ib', kind='active', mode='real', scenario='per-node-container',
                         operation='ib-write-bw', directory='compact-ib', hostfile=str(self.hostfile),
                         env_script='/target/env.sh', context='no')
        self.meta = dict(started=time.time(), token='acceptance-fixture-token')
        self.contract = validator()
        self.public = self.root / self.case['directory'] / 'current-run'
        self.state = self.public / '.state'
        self.prepared = self.state / 'prepared/group-001'
        self.prepared.mkdir(parents=True)
        self.archive = self.public / 'group-001.log'
        self.archive.write_bytes(b'')
        self.log = self.root / 'logs' / (self.case['case'] + '.log')
        self.log.parent.mkdir()
        self.log.write_text('current Bash run\n', encoding='utf-8')
        self.stage = '01-ib_write_bw'
        self.prefix = f'stages/{self.stage}/groups/group-001/'
        self.tokens = {'preflight': 'hcu_acceptance_preflight_0001', self.stage: 'hcu_acceptance_execute_00001'}
        self.data = dict(schema_version=2, group='group-001', revision=4, tables={})
        tasks, nodes, cleanup = [], [], []
        for stage, token in self.tokens.items():
            tasks.append(dict(stage_id=stage, phase='prepare' if stage == 'preflight' else 'execute',
                item='' if stage == 'preflight' else 'ib_write_bw', iteration='0', token=token, status='PASS',
                exit_code='0', execution_exit_code='0', cleanup='CONFIRMED', started_epoch='100', ended_epoch='101',
                reason_code='OK', log=('prepared/group-001/' if stage == 'preflight' else self.prefix) + 'group.log'))
            for node in ('n01', 'n02'):
                nodes.append(dict(stage_id=stage, token=token, node=node, exit_code='0', cleanup='CONFIRMED', log='logical.log'))
                cleanup.append(dict(stage_id=stage, token=token, node=node, source='normal', attempt='1', status='CONFIRMED',
                    exit_code='0', evidence='cleanup.json', guard=dict(token=token, status='STOPPED', remaining=0,
                                                                    tombstone=True, uncertain=False)))
        self.table('tasks', tasks)
        self.table('nodes', nodes)
        self.table('cleanup', cleanup)
        self.table('logs', [], fields=['stage_id','token','sources','offset','length','sha256'])
        self.add_log('preflight', 'prepared/group-001/group.log', b'preflight raw\n')
        self.add_log(self.stage, self.prefix + 'group.log', b'actual IB command and raw output\n')
        commands = []
        for role in ('server', 'client'):
            command = dict(returncode=0, argv=['ib_write_bw', '--ib-dev=mlx5_0'])
            for stream in ('stdout', 'stderr'):
                source = self.prefix + f'ib/evidence/pair/{role}/{stream}.txt'
                command[stream + '_path'] = source
                self.add_log(self.stage, source, (role + ' raw\n').encode() if stream == 'stdout' else b'')
            commands.append(command)
        self.metadata = dict(run_token=self.tokens[self.stage], selected_nodes=['n01','n02'], status='PASS',
            pairs=[dict(source='n01', destination='n02', status='PASS', commands=commands)])
        self.table('ib', [dict(stage_id=self.stage, token=self.tokens[self.stage], metadata=self.metadata)])
        self.report = dict(schema_version=1, protocol_version=1, final=True, status='PASS', planned=1,
            control=dict(state_layout='compact-group-v2', scenario=self.case['scenario'], operation='ib-write-bw',
                         env_script=self.case['env_script'], container='worker'),
            groups=[dict(group='group-001', nodes='n01,n02', hostfile=str(self.prepared / 'hostfile'))],
            stages=[dict(stage_id=self.stage, item='ib_write_bw', iteration='0')],
            preflight=[dict(group='group-001', status='PASS', cleanup='CONFIRMED')],
            results=[dict(stage_id=self.stage, group='group-001', nodes='n01,n02', status='PASS', exit_code='0',
                          cleanup='CONFIRMED', dispatched=True)])
        self.persist()

    def table(self, name, records, fields=None):
        fields = fields or list(records[0])
        self.data['tables'][name] = dict(fields=fields, rows=[[record[field] for field in fields] for record in records])

    def add_log(self, stage, source, payload):
        offset = self.archive.stat().st_size
        with self.archive.open('ab') as stream:
            stream.write(payload)
        self.data['tables']['logs']['rows'].append([stage, self.tokens[stage], [source], offset, len(payload),
                                                   hashlib.sha256(payload).hexdigest()])

    def persist(self):
        write_json(self.prepared / 'facts.json', self.data)
        write_json(self.state / 'report.json', self.report)

    def verify(self, rc=0):
        return self.contract['verify_case'](self.root, self.case, dict(returncode=rc,log_returncode=0), self.meta)

    def test_compact_ib_metadata_and_endpoint_fragments_need_no_runtime_or_legacy_json(self):
        result = self.verify()
        self.assertEqual(result['execution_state'], 'EXECUTION_ATTEMPTED', result)
        self.assertEqual(result['pairs_executed'], 1)
        self.assertEqual(result['nodes_reported'], ['n01','n02'])
        self.assertFalse((self.state / 'runtime').exists())
        self.assertFalse(list(self.public.rglob('ib-write-bw-result.json')))
        self.assertFalse((self.state / 'stages').exists())

    def test_staged_environment_scope_and_receipt_are_checked(self):
        original = copy.deepcopy(self.report)
        for change in ({'env_script':'/wrong.sh'}, {'container':''}, {'scenario':'shared-conda'}):
            self.report = copy.deepcopy(original)
            self.report['control'].update(change)
            self.persist()
            self.assertEqual(self.verify()['execution_state'], 'INTERFACE_ERROR')
        self.report = original
        self.persist()
        self.assertEqual(self.verify(rc=1)['execution_state'], 'INTERFACE_ERROR')

    def test_compact_missing_ib_metadata_never_uses_old_physical_json(self):
        self.data['tables']['ib']['rows'] = []
        write_json(self.state / self.prefix / 'ib-write-bw-result.json', self.metadata)
        self.persist()
        result = self.verify()
        self.assertEqual(result['execution_state'], 'INTERFACE_ERROR')
        self.assertIn('authoritative IB metadata', result['error'])

    def test_missing_endpoint_fragment_cannot_be_replaced_with_stale_physical_log(self):
        source = self.metadata['pairs'][0]['commands'][0]['stdout_path']
        self.data['tables']['logs']['rows'] = [row for row in self.data['tables']['logs']['rows'] if source not in row[2]]
        path = self.state / source
        path.parent.mkdir(parents=True)
        path.write_text('stale endpoint success', encoding='utf-8')
        self.persist()
        self.assertEqual(self.verify()['execution_state'], 'INTERFACE_ERROR')

    def test_endpoint_checksum_corruption_is_rejected(self):
        row = self.data['tables']['logs']['rows'][2]
        with self.archive.open('r+b') as stream:
            stream.seek(row[3])
            stream.write(b'X')
        self.assertEqual(self.verify()['execution_state'], 'INTERFACE_ERROR')

    def test_unconfirmed_cleanup_or_missing_guard_proof_is_rejected(self):
        original = copy.deepcopy(self.data)
        self.data['tables']['cleanup']['rows'] = []
        self.persist()
        self.assertEqual(self.verify()['execution_state'], 'INTERFACE_ERROR')
        self.data = original
        fields = self.data['tables']['tasks']['fields']
        self.data['tables']['tasks']['rows'][1][fields.index('cleanup')] = 'UNCONFIRMED'
        self.persist()
        self.assertEqual(self.verify()['execution_state'], 'INTERFACE_ERROR')

    def test_missing_or_reordered_staged_results_do_not_pass(self):
        self.report['results'] = []
        self.persist()
        self.assertEqual(self.verify()['execution_state'], 'INTERFACE_ERROR')
        self.report['results'] = [dict(stage_id=self.stage, group='group-999')]
        self.persist()
        self.assertEqual(self.verify()['execution_state'], 'INTERFACE_ERROR')


def fixture_cli(args):
    # Test-only executable boundary: every other command succeeds with complete
    # artifacts, then a single selected non-launcher returns an actual error.
    root=max(Path(os.environ['OUTPUT_DIR']).glob('run_*'),key=lambda path:path.stat().st_mtime)
    with (root/'expected.tsv').open(encoding='utf-8') as stream:
        cases=list(csv.DictReader(stream,delimiter='\t'))
    if '--output-dir' in args:
        relative=str(Path(args[args.index('--output-dir')+1]).relative_to(root))
        case=next(item for item in cases if item['directory']==relative)
    else:
        operation=next(arg for arg in args if arg in {'--help','--version','container-status','container-create','container-recreate','container-delete'})
        case=next(item for item in cases if item['operation']==operation)
    if case['case']==os.environ.get('FAIL_CASE'):
        print('RESULT        TOOL_ERROR\nERROR injected single case failure')
        return 3
    print(emit_fixture(root,case,json.loads((root/'meta.json').read_text())),end='')
    return 0


if __name__ == '__main__':
    if sys.argv[1:2] == ['--fixture-cli']:
        raise SystemExit(fixture_cli(sys.argv[2:]))
    unittest.main()
