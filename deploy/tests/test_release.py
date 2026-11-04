import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
spec = importlib.util.spec_from_file_location('release', 'deploy/release.py')
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)


class StackReleaseTests(unittest.TestCase):
    def setUp(self):
        self.m = json.loads(Path('deploy/prod.json').read_text())
        self.settings = json.loads(Path('deploy/prod-stack.json').read_text())
        values = dict(ECSClusterName=self.m['cluster'], EnvironmentName='prod', AppRepositoryName=self.m['images']['app'], ClamAvRepositoryName=self.m['images']['filescanner'], TaskRoleName='avscan-service-role', TaskExecutionRoleName='ecsTaskExecutionRole', AppImageDigest='', ClamAvImageDigest='', SecretOwner='****')
        self.stack = dict(StackId='reviewed-stack', StackStatus='UPDATE_COMPLETE', Outputs=[dict(OutputKey='DeploymentContract', OutputValue='oidc-digest-v1')], Parameters=[dict(ParameterKey=k, ParameterValue=v) for k,v in values.items()])
        self.resources = [dict(ResourceType='AWS::ECS::Service', PhysicalResourceId=r.d.arn(self.m, 'ecs', f"service/{self.m['cluster']}/{self.m['service']}"))]
        self.aws = Mock()
        self.aws.call.side_effect = lambda service, operation, **kwargs: {'Stacks':[self.stack]} if operation == 'describe-stacks' else {'StackResources':self.resources}

    def test_verified_existing_stack(self):
        self.assertEqual(r.reviewed_stack(self.aws, self.m, self.settings), self.stack)

    def test_retained_parameters_must_be_detached(self):
        self.resources.append(dict(ResourceType='AWS::SSM::Parameter'))
        with self.assertRaises(r.d.DeployError): r.reviewed_stack(self.aws, self.m, self.settings)

    def test_wrong_service_rejected(self):
        self.resources[0]['PhysicalResourceId'] = 'other-service'
        with self.assertRaises(r.d.DeployError): r.reviewed_stack(self.aws, self.m, self.settings)

    def test_missing_contract_rejected(self):
        self.stack['Outputs'] = []
        with self.assertRaises(r.d.DeployError): r.reviewed_stack(self.aws, self.m, self.settings)

    def test_updates_only_two_digests_and_preserves_noecho_values(self):
        images = dict(app='app@sha256:'+'a'*64, filescanner='av@sha256:'+'b'*64)
        params = r.update_parameters(self.stack, images)
        changed = [p for p in params if 'ParameterValue' in p]
        self.assertEqual({p['ParameterKey'] for p in changed}, {'AppImageDigest','ClamAvImageDigest'})
        self.assertEqual(next(p for p in params if p['ParameterKey']=='SecretOwner'), dict(ParameterKey='SecretOwner', UsePreviousValue=True))

    def test_failed_second_scan_cannot_update_stack(self):
        self.aws.call.side_effect = None
        self.aws.call.return_value = dict(taskDefinition={},tags=[])
        publisher = Mock()
        publisher.publish.side_effect = ['app@sha256:'+'a'*64,r.d.DeployError('scan failed')]
        with patch.object(r,'reviewed_stack',return_value=self.stack),patch.object(r.d,'read_service',return_value={'taskDefinition':'old'}),patch.object(r.d,'stable',return_value=True),patch.object(r.d,'task_request'):
            with self.assertRaises(r.d.DeployError):r.release(self.aws,self.m,self.settings,publisher,'a'*40+'-1')
        self.assertNotIn('update-stack',[c.args[1] for c in self.aws.call.call_args_list])

    def test_untagged_stack_rollout_and_unexpected_tag_detection(self):
        from copy import deepcopy
        images = {'app': 'repo/app@sha256:' + 'a' * 64, 'filescanner': 'repo/av@sha256:' + 'b' * 64}
        task = dict(family=self.m['taskFamily'], status='ACTIVE', networkMode='awsvpc', requiresCompatibilities=['FARGATE'], taskRoleArn=self.m['taskRoleArn'], executionRoleArn=self.m['executionRoleArn'], containerDefinitions=[dict(name=name, image='old', healthCheck={'command':['CMD', 'check']}) for name in images])
        saved = deepcopy(task)
        for c in saved['containerDefinitions']: c['image'] = images[c['name']]
        previous = dict(taskDefinition='old', desiredCount=1)
        current = dict(taskDefinition='new', desiredCount=1)
        completed = deepcopy(self.stack)
        for param in completed['Parameters']:
            if param['ParameterKey'] in ('AppImageDigest', 'ClamAvImageDigest'):
                param['ParameterValue'] = images['app' if param['ParameterKey']=='AppImageDigest' else 'filescanner'].split('@')[1]
        for tags in ([], [{'key':'unexpected', 'value':'added'}]):
            with self.subTest(tags=tags):
                aws = Mock()
                reads = iter([{'taskDefinition':task, 'tags':[]}, {'taskDefinition':saved, 'tags':tags}])
                aws.call.side_effect = lambda service, op, **kw: next(reads) if op=='describe-task-definition' else {'Stacks':[completed]} if op=='describe-stacks' else {}
                publisher = Mock()
                publisher.publish.side_effect = lambda name, *args: images[name]
                with patch.object(r, 'reviewed_stack', return_value=self.stack), patch.object(r.d, 'read_service', side_effect=[previous,previous,current]), patch.object(r.d, 'stable', return_value=True), patch.object(r.d, 'verify_running', return_value=True):
                    if tags:
                        with self.assertRaisesRegex(r.d.DeployError, 'Non-image'):r.release(aws,self.m,self.settings,publisher,'a'*40+'-1')
                    else:self.assertEqual(r.release(aws,self.m,self.settings,publisher,'a'*40+'-1'),'new')
