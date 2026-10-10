# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Acceptance contracts: environment outcomes are allowed; missing execution is not.

All fixtures are local. The optional full Shell test invokes a fixture CLI and
never opens SSH/Docker connections or runs a GPU workload.
"""
from __future__ import annotations

import csv
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
                  NODE_LOCAL_ENV_SCRIPT='/target/local.sh', CONTAINER_ENV_SCRIPT='/target/container.sh')
    values.update({key: 'dry' for key in ('ACTIVE_MODE','PROFILE_MODE','NETWORK_MODE','DIAGNOSTIC_MODE','SCRIPT_MODE','CONTEXT_MODE')})
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
        leaf = Path(case['directory']).name
        profile = leaf if leaf in {'rccl-tests','rocblas'} else 'worker'
        requested = leaf.removeprefix('launcher-') if leaf.startswith('launcher-') else (
            meta.get('launcher','mpirun-torchrun') if case['operation'] in {'rccl','gemm'} and profile=='worker'
            else 'mpirun' if profile=='rccl-tests' else 'mpirun-torchrun')
        effective = ('server-client' if case['operation']=='ib-write-bw' else 'node-script' if case['operation']=='custom'
                     else profile if profile!='worker' else 'local' if len(nodes)==1 else requested)
        dispatched = nodes if effective in {'node-script','ssh-torchrun','rocblas'} else nodes[:1]
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
        report = {'schema_version':'1.1' if kind == 'script' else '1.0', 'operation':case['operation'], 'status':status,
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
        path = directory / ('script-result.json' if kind == 'script' else case['operation']+'-result.json')
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
        filename='script-result.json' if case['kind']=='script' else 'active-result.json'
        path=next((self.root/case['directory']).rglob(filename))
        return case,path,json.loads(path.read_text(encoding='utf-8'))

    def verify_one(self, case, rc=0):
        return self.contract['verify_case'](self.root,case,dict(returncode=rc,log_returncode=0),self.meta)

    def test_full_matrix_includes_all_new_operations_and_container_script(self):
        code,report=self.validate()
        self.assertEqual(code,0,[item for item in report['cases'] if item.get('error')])
        for scenario in ('shared-conda','node-local-conda','per-node-container'):
            operations={case['operation'] for case in self.cases if case['scenario']==scenario}
            self.assertTrue({'platform','resource','platform,resource','nhc','ib-write-bw','rccl','gemm','custom','script'} <= operations)
            self.assertNotIn('ib-state', operations)
        self.assertIn('per-node-container-script',[item['case'] for item in report['cases']])

    def test_rccl_binary_matrix_does_not_silently_select_only_all_reduce(self):
        source = SCRIPT.read_text(encoding='utf-8')
        for scenario in ('shared-conda', 'node-local-conda', 'per-node-container'):
            block = source.split('# ' + scenario + '-rccl-tests:', 1)[1].split('\n# ', 1)[0]
            self.assertNotIn('--script-arg=--tests', block)
            self.assertIn('--nproc-per-node "$RCCL_NPROC_PER_NODE"', block)

    def test_one_non_launcher_failure_is_not_hidden_by_all_other_successes(self):
        # Only GEMM fails to create its report; other 50 command receipts/artifacts are valid.
        case=self.case('node-local-conda-gemm')
        report_path=next((self.root/case['directory']).rglob('active-result.json'))
        report_path.unlink()
        code, report=self.validate()
        self.assertEqual(code,1)
        errors=[item['case'] for item in report['cases'] if item['execution_state']=='INTERFACE_ERROR']
        self.assertEqual(errors,['node-local-conda-gemm'])

    def test_actual_dryrun_reports_match_acceptance_contract_without_network(self):
        from cluster_run.cli import main
        for original in self.cases:
            if original['mode'] != 'dry' or original['kind'] not in {'active','script','diagnostic'}:
                continue
            with self.subTest(case=original['case']):
                case = dict(original, directory='actual/' + original['directory'], env_script='/target/fixture-env.sh')
                args = [case['scenario'], case['operation'], '-f', case['hostfile'], '--env-script',case['env_script'],
                        '--output-dir', str(self.root/case['directory']), '--dry-run']
                if case['scenario'] == 'per-node-container':
                    args += ['--container','worker','-i','image:tag']
                if case['operation'] in {'script','custom'}:
                    args += ['--script','/target/context.py']
                if case['kind'] == 'active':
                    args += ['--group-size','2' if case['operation']=='ib-write-bw' or '/launcher-' in case['directory'] else '1']
                if '/launcher-' in case['directory']:
                    args += ['--profile', 'worker', '--launcher', case['directory'].split('/launcher-')[1]]
                elif case['directory'].endswith('/rccl-tests'):
                    pass  # The RCCL default must exercise the real binary profile.
                elif case['directory'].endswith('/rocblas'):
                    args += ['--profile','rocblas']
                elif case['operation'] == 'rccl':
                    args += ['--profile', 'worker']
                output=io.StringIO()
                with patch('hcu_envcheck.baremetal.BaremetalClusterExecutor.execute', side_effect=AssertionError('dry-run contacted a node')):
                    with redirect_stdout(output), redirect_stderr(output):
                        code=main(args)
                (self.root/'logs'/(case['case']+'.log')).write_text(output.getvalue(),encoding='utf-8')
                result=self.contract['verify_case'](self.root,case,dict(returncode=code,log_returncode=0),self.meta)
                self.assertEqual(result['execution_state'],'PLANNED_ONLY',result)

    def test_container_script_missing_report_is_not_omitted_from_validation(self):
        case=self.case('per-node-container-script')
        next((self.root/case['directory']).rglob('script-result.json')).unlink()
        self.assertEqual(self.validate()[0],1)

    def test_exit_zero_without_launch_artifacts_does_not_pass(self):
        case=self.case('shared-conda-gemm')
        next((self.root/case['directory']).rglob('launch.json')).unlink()
        self.assertEqual(self.validate()[0],1)

    def test_wrong_node_scope_is_rejected(self):
        case=self.case('per-node-container-script')
        path=next((self.root/case['directory']).rglob('script-result.json'))
        data=json.loads(path.read_text()); data['nodes'].pop('n02'); write_json(path,data)
        self.assertEqual(self.validate()[0],1)

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

    def test_each_profile_and_launcher_field_is_checked_without_masking_other_cases(self):
        changes=(('shared-conda-rccl-tests','active-result.json','profile','worker'),
                 ('shared-conda-gemm','active-result.json','profile','rocblas'),
                 ('shared-conda-rccl','active-result.json','launcher','mpirun'),
                 ('per-node-container-launcher-mpirun','launch.json','requested_launcher','ssh-torchrun'),
                 ('shared-conda-rocblas','launch.json','execution_profile','worker'),
                 ('shared-conda-rccl','launch.json','launcher','ssh-torchrun'),
                 ('per-node-container-launcher-ssh-torchrun','result.json','launcher','mpirun'))
        for name,filename,field,value in changes:
            with self.subTest(case=name,artifact=filename,field=field):
                case=self.case(name)
                path=next((self.root/case['directory']).rglob(filename))
                original=path.read_text(encoding='utf-8')
                data=json.loads(original); data[field]=value; write_json(path,data)
                code,report=self.validate()
                self.assertEqual(code,1)
                self.assertEqual([item['case'] for item in report['cases'] if item.get('error')],[name])
                path.write_text(original,encoding='utf-8')

    def test_report_cannot_hide_wrong_group_profile_or_effective_launcher(self):
        case=self.case('shared-conda-gemm')
        path=next((self.root/case['directory']).rglob('active-result.json'))
        original=path.read_text(encoding='utf-8')
        for field,value in (('execution_profile','rocblas'),('launcher','local'),('requested_launcher','mpirun')):
            with self.subTest(field=field):
                data=json.loads(original); data['groups'][0][field]=value; write_json(path,data)
                self.assertEqual(self.verify_one(case)['execution_state'],'INTERFACE_ERROR')

    def test_normal_worker_uses_configured_launcher_not_hardcoded_default(self):
        self.meta['launcher']='ssh-torchrun'
        case,path,data=self.real_fixture('shared-conda-gemm')
        self.assertEqual(self.verify_one(case)['execution_state'],'EXECUTION_ATTEMPTED')
        data['launcher']='mpirun-torchrun'; write_json(path,data)
        self.assertEqual(self.verify_one(case)['execution_state'],'INTERFACE_ERROR')

    def test_actual_worker_dryrun_cannot_substitute_for_rccl_tests_profile(self):
        from cluster_run.cli import main
        case=dict(self.case('shared-conda-rccl-tests'),directory='wrong-profile/rccl-tests')
        output=io.StringIO()
        with patch('hcu_envcheck.baremetal.BaremetalClusterExecutor.execute',side_effect=AssertionError('network forbidden')):
            with redirect_stdout(output),redirect_stderr(output):
                rc=main(['shared-conda','rccl','-f',case['hostfile'],'--env-script',case['env_script'],
                         '--output-dir',str(self.root/case['directory']),'--group-size','1','--profile','worker','--dry-run'])
        self.assertEqual(rc,0,output.getvalue())
        (self.root/'logs'/(case['case']+'.log')).write_text(output.getvalue(),encoding='utf-8')
        result=self.verify_one(case)
        self.assertEqual(result['execution_state'],'INTERFACE_ERROR',result)
        self.assertIn('profile mismatch',result['error'])

    def test_active_and_script_environment_and_scope_are_checked(self):
        for name in ('shared-conda-gemm','per-node-container-script'):
            case=self.case(name)
            path=next((self.root/case['directory']).rglob('active-result.json' if case['kind']=='active' else 'script-result.json'))
            original=path.read_text(encoding='utf-8')
            for field,value in (('env_script','/wrong/env.sh'),('execution_scope','wrong-scope')):
                with self.subTest(case=name,field=field):
                    data=json.loads(original); data[field]=value; write_json(path,data)
                    self.assertEqual(self.verify_one(case)['execution_state'],'INTERFACE_ERROR')

    def test_real_mpi_has_leader_evidence_but_ssh_and_pernode_need_every_node(self):
        names=('shared-conda-gemm','shared-conda-rccl-tests','shared-conda-rocblas',
               'per-node-container-launcher-mpirun','per-node-container-launcher-ssh-torchrun','shared-conda-custom')
        for name in names:
            with self.subTest(case=name):
                case,path,data=self.real_fixture(name)
                group=data['groups'][0]
                expected=['n01','n02'] if group['launcher'] in {'ssh-torchrun','rocblas','node-script'} else ['n01']
                self.assertEqual([item['node'] for item in group['node_results']],expected)
                result=self.verify_one(case)
                self.assertEqual(result['execution_state'],'EXECUTION_ATTEMPTED',result)
                group['node_results'].pop(); write_json(path,data)
                self.assertEqual(self.verify_one(case)['execution_state'],'INTERFACE_ERROR')

    def test_duplicate_or_wrong_worker_node_cannot_replace_missing_node(self):
        for fault in ('duplicate','wrong-node'):
            with self.subTest(fault=fault):
                case,path,data=self.real_fixture('shared-conda-custom')
                data['groups'][0]['node_results'][1]['node']='n01' if fault=='duplicate' else 'n03'
                write_json(path,data)
                self.assertEqual(self.verify_one(case)['execution_state'],'INTERFACE_ERROR')

    def test_top_pass_cannot_hide_group_worker_or_returncode_disagreement(self):
        for fault in ('group-cancelled','worker-cancelled','worker-failed','pass-nonzero','unknown-worker',
                      'group-incomplete','hidden-incomplete','interrupted'):
            with self.subTest(fault=fault):
                case,path,data=self.real_fixture('shared-conda-gemm')
                group=data['groups'][0]; worker=group['node_results'][0]
                if fault=='group-cancelled': group['status']='CANCELLED'
                elif fault=='worker-cancelled': worker.update(status='CANCELLED',returncode=130)
                elif fault=='worker-failed': worker.update(status='FAIL',returncode=1)
                elif fault=='pass-nonzero': worker['returncode']=1
                elif fault=='unknown-worker': worker['status']='UNKNOWN'
                elif fault=='group-incomplete': group['status']='INCOMPLETE'
                elif fault=='hidden-incomplete':
                    group['status']='INCOMPLETE'; worker.update(status='INCOMPLETE',returncode=2)
                else: data['interrupted']=True
                write_json(path,data)
                self.assertEqual(self.verify_one(case)['execution_state'],'INTERFACE_ERROR')

    def test_valid_worker_failure_and_incomplete_remain_reportable_outcomes(self):
        for status,worker_rc,cli_rc in (('FAIL',1,2),('INCOMPLETE',2,0)):
            with self.subTest(status=status):
                case,path,data=self.real_fixture('shared-conda-gemm')
                data['status']=data['groups'][0]['status']=status
                data['groups'][0]['node_results'][0].update(status=status,returncode=worker_rc)
                write_json(path,data)
                self.assertEqual(self.verify_one(case,cli_rc)['execution_state'],'EXECUTION_ATTEMPTED')

    def test_failfast_cancelled_peer_is_valid_only_with_failed_aggregate_and_confirmed_cleanup(self):
        case,path,data=self.real_fixture('shared-conda-custom')
        data['status']=data['groups'][0]['status']='FAIL'
        first,second=data['groups'][0]['node_results']
        first.update(status='FAIL',returncode=127)
        second.update(status='CANCELLED',returncode=130)
        data['cleanup']={'run_token':'test-token','nodes':{node:{'status':'CONFIRMED'} for node in self.meta['nodes']}}
        write_json(path,data)
        self.assertEqual(self.verify_one(case,2)['execution_state'],'EXECUTION_ATTEMPTED')
        data['status']='PASS'; write_json(path,data)
        self.assertEqual(self.verify_one(case)['execution_state'],'INTERFACE_ERROR')

    def test_serialized_cleanup_node_status_not_computed_property_controls_acceptance(self):
        from dataclasses import asdict
        from cluster_run.task_control import CancellationReport,NodeCancellation
        for name in ('per-node-container-context-py','per-node-container-context-sh'):
            for status in ('CONFIRMED','UNCONFIRMED','FAILED'):
                with self.subTest(case=name,status=status):
                    case,path,data=self.real_fixture(name)
                    cleanup=asdict(CancellationReport('test-token',{node:NodeCancellation(node,
                        status if node=='n02' else 'CONFIRMED','fixture') for node in self.meta['nodes']}))
                    self.assertNotIn('confirmed',cleanup)
                    data['cleanup']=cleanup; write_json(path,data)
                    expected='EXECUTED_VERIFIED' if status=='CONFIRMED' else 'INTERFACE_ERROR'
                    self.assertEqual(self.verify_one(case)['execution_state'],expected)

    def test_child_and_basic_cleanup_cannot_be_hidden_by_top_level_status(self):
        case,path,data=self.real_fixture('per-node-container-context-py')
        data['groups'][0]['node_results'][0]['cleanup']={'status':'UNCONFIRMED'}
        write_json(path,data)
        self.assertEqual(self.verify_one(case)['execution_state'],'INTERFACE_ERROR')
        case=self.case('shared-conda-resource')
        path=next((self.root/case['directory']).rglob('cluster-result.json'))
        data=json.loads(path.read_text(encoding='utf-8'))
        data['cluster']['cleanup']={'nodes':{'n01':{'status':'UNCONFIRMED'}}}
        write_json(path,data)
        self.assertEqual(self.verify_one(case)['execution_state'],'INTERFACE_ERROR')

    def test_precheck_is_blocked_not_executed_and_never_accepted_in_dryrun(self):
        case=dict(self.case('per-node-container-rccl'),mode='real')
        (self.root/'logs'/ (case['case']+'.log')).write_text(
            '[ERROR] nodes=n[01-02] code=DCU_BUSY reason=in use\nRESULT        PRECHECK_FAILED\n',encoding='utf-8')
        receipt=dict(returncode='3',log_returncode='0')
        result=self.contract['verify_case'](self.root,case,receipt,self.meta)
        self.assertEqual(result['execution_state'],'BLOCKED_NOT_EXECUTED')
        self.assertIsNone(result['report'])
        case['mode']='dry'
        self.assertEqual(self.contract['verify_case'](self.root,case,receipt,self.meta)['execution_state'],'INTERFACE_ERROR')

    def test_real_context_verifies_env_marker_node_and_scope(self):
        case=dict(self.case('shared-conda-context-py'),mode='real')
        emit_fixture(self.root,case,self.meta)
        result=self.contract['verify_case'](self.root,case,dict(returncode=0,log_returncode=0),self.meta)
        self.assertEqual(result['execution_state'],'EXECUTED_VERIFIED',result)
        log=next((self.root/case['directory']).rglob('stdout.log'))
        log.write_text(context_line('wrong-node',case,self.meta),encoding='utf-8')
        self.assertEqual(self.contract['verify_case'](self.root,case,dict(returncode=0,log_returncode=0),self.meta)['execution_state'],'INTERFACE_ERROR')

    def test_container_hostname_is_observed_not_used_as_physical_node_identity(self):
        for name in ('per-node-container-context-sh','per-node-container-context-py'):
            with self.subTest(case=name):
                case=dict(self.case(name),mode='real')
                emit_fixture(self.root,case,self.meta)
                for log in (self.root/case['directory']).rglob('stdout.log'):
                    log.write_text(context_line('worker-container',case,self.meta),encoding='utf-8')
                result=self.contract['verify_case'](self.root,case,dict(returncode=0,log_returncode=0),self.meta)
                self.assertEqual(result['execution_state'],'EXECUTED_VERIFIED',result)
                self.assertEqual(set(result['runtime_context']),{'n01','n02'})
                self.assertTrue(all(item['node']=='worker-container' for item in result['runtime_context'].values()))

    def test_container_context_still_rejects_missing_node_evidence_bad_token_and_scope(self):
        for name in ('per-node-container-context-sh','per-node-container-context-py'):
            for fault in ('missing-log','wrong-token','wrong-scope','missing-result'):
                with self.subTest(case=name,fault=fault):
                    case=dict(self.case(name),mode='real')
                    emit_fixture(self.root,case,self.meta)
                    log=next((self.root/case['directory']).rglob('stdout.log'))
                    if fault=='missing-log':
                        log.unlink()
                    elif fault=='missing-result':
                        path=next((self.root/case['directory']).rglob('script-result.json' if case['kind']=='script' else 'active-result.json'))
                        data=json.loads(path.read_text(encoding='utf-8'))
                        if case['kind']=='script': data['nodes'].pop('n02')
                        else: data['groups'][0]['node_results'].pop()
                        write_json(path,data)
                    else:
                        observed=dict(token='wrong' if fault=='wrong-token' else self.meta['token'],
                                      node='worker-container',scope='host' if fault=='wrong-scope' else 'container',pid=1)
                        log.write_text('HCU_CONTEXT_JSON '+json.dumps(observed)+'\n',encoding='utf-8')
                    result=self.contract['verify_case'](self.root,case,dict(returncode=0,log_returncode=0),self.meta)
                    self.assertEqual(result['execution_state'],'INTERFACE_ERROR',result)

    def test_container_hostname_relaxation_does_not_allow_cross_node_log_rebinding(self):
        case,path,data=self.real_fixture('per-node-container-context-sh')
        for log in (self.root/case['directory']).rglob('stdout.log'):
            log.write_text(context_line('worker-container',case,self.meta),encoding='utf-8')
        data['nodes']['n02']['stdout']=data['nodes']['n01']['stdout']
        write_json(path,data)
        result=self.verify_one(case)
        self.assertEqual(result['execution_state'],'INTERFACE_ERROR',result)
        self.assertIn('bound to wrong node',result['error'])

    def test_tool_error_or_cleanup_unconfirmed_never_pass(self):
        case=self.case('shared-conda-gemm')
        for code,text in ((3,'RESULT        TOOL_ERROR\n'),(3,'RESULT        CLEANUP_UNCONFIRMED\n'),(130,'RESULT        CANCELLED\n')):
            with self.subTest(code=code,text=text):
                (self.root/'logs'/ (case['case']+'.log')).write_text(text,encoding='utf-8')
                result=self.contract['verify_case'](self.root,case,dict(returncode=code,log_returncode=0),self.meta)
                self.assertEqual(result['execution_state'],'INTERFACE_ERROR')

    def test_runtime_context_checks_every_group_not_only_last(self):
        case=dict(self.case('per-node-container-context-py'),mode='real')
        emit_fixture(self.root,case,self.meta)
        path=next((self.root/case['directory']).rglob('active-result.json'))
        report=json.loads(path.read_text(encoding='utf-8'))
        original=report['groups'][0]
        groups=[]
        for index,node in enumerate(self.meta['nodes']):
            name=f'group-{index:03d}'
            directory=path.parent/'groups'/name
            record=original['node_results'][index]
            group=dict(original,group=name,nodes=[node],leader=node,node_results=[record],
                       commands=[item for item in original['commands'] if item['node']==node])
            stdout=directory/'nodes'/node/'stdout.log'
            stdout.parent.mkdir(parents=True,exist_ok=True)
            stdout.write_text(context_line(node,case,self.meta),encoding='utf-8')
            (stdout.parent/'stderr.log').write_text('',encoding='utf-8')
            write_json(directory/'result.json',group)
            groups.append(group)
        report.update(groups=groups,group_count=2)
        write_json(path,report)
        receipt=dict(returncode=0,log_returncode=0)
        result=self.contract['verify_case'](self.root,case,receipt,self.meta)
        self.assertEqual(result['execution_state'],'EXECUTED_VERIFIED',result)
        self.assertEqual(set(result['runtime_context']),set(self.meta['nodes']))
        first=path.parent/'groups/group-000/nodes/n01/stdout.log'
        first.write_text(context_line('n01',case,dict(self.meta,token='stale')),encoding='utf-8')
        result=self.contract['verify_case'](self.root,case,receipt,self.meta)
        self.assertEqual(result['execution_state'],'INTERFACE_ERROR',result)
        first.unlink()
        result=self.contract['verify_case'](self.root,case,receipt,self.meta)
        self.assertEqual(result['execution_state'],'INTERFACE_ERROR',result)

    @unittest.skipUnless(os.name == 'posix' and shutil.which('bash'), 'Linux full Shell fixture, never SSH')
    def test_total_shell_reports_one_failed_non_launcher_case(self):
        fake=self.root/'fake-cli.sh'
        fake.write_text('#!/usr/bin/env bash\nexec "$PYTHON_BIN" "$FIXTURE_DRIVER" --fixture-cli "$@"\n',encoding='utf-8')
        env=dict(os.environ, HCU_ACCEPTANCE_NESTED_TEST='1', HCU_CLUSTER_RUN=str(fake),
                 PYTHON_BIN=sys.executable, CONTROLLER_PYTHON=sys.executable,
                 FIXTURE_DRIVER=str(Path(__file__).resolve()), HOSTFILE=str(self.hostfile), ENV_SCRIPT='/target/env.sh',
                 OUTPUT_DIR=str(self.root/'matrix'), FAIL_CASE='node-local-conda-gemm')
        for name in ('CONTROLLER_ENV_SCRIPT','RUN_ACTIVE','RUN_PROFILES','RUN_NETWORK','RUN_DIAGNOSTICS','RUN_SCRIPTS','RUN_CONTEXT'):
            env.pop(name,None)
        run=subprocess.run(['bash',str(SCRIPT)],env=env,cwd=ROOT,capture_output=True,text=True,timeout=120)
        self.assertNotEqual(run.returncode,0,run.stdout+run.stderr)
        path=next((self.root/'matrix').glob('run_*/acceptance-result.json'))
        report=json.loads(path.read_text())
        failures=[case['case'] for case in report['cases'] if case['execution_state']=='INTERFACE_ERROR']
        self.assertEqual(failures,['node-local-conda-gemm'],run.stdout+run.stderr)


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
