#!/usr/bin/env python3
"""Promote two scanned digests through the existing administrator-owned stack."""
from copy import deepcopy
import json
import os
from pathlib import Path
import sys
import tempfile
import time
sys.path.insert(0, '/tmp/tc-deploy-scripts')
import tcdeploy as d

ACTIONS = ['cloudformation:DescribeStacks', 'cloudformation:DescribeStackResources', 'cloudformation:UpdateStack']


def reviewed_stack(aws, m, settings):
    expected_name = 'av-scanner-v6-' + m['environment']
    d.require(settings.get('stackName') == expected_name, 'Unexpected stack')
    d.require(settings.get('roleArn') == f"arn:aws:iam::{m['accountId']}:role/{expected_name}-cloudformation-deploy", 'Unexpected CloudFormation role')
    stacks = aws.call('cloudformation', 'describe-stacks', StackName=expected_name)['Stacks']
    d.require(len(stacks) == 1, 'Expected existing stack')
    stack = stacks[0]
    d.require(stack['StackStatus'] in ('CREATE_COMPLETE', 'UPDATE_COMPLETE', 'UPDATE_ROLLBACK_COMPLETE'), 'Stack is not stable')
    d.require({v['OutputKey']: v['OutputValue'] for v in stack.get('Outputs', [])}.get('DeploymentContract') == 'oidc-digest-v1', 'Administrator must install the digest deployment contract')
    parameters = {v['ParameterKey']: v.get('ParameterValue') for v in stack['Parameters']}
    for key, value in dict(ECSClusterName=m['cluster'], EnvironmentName=m['environment'], AppRepositoryName=m['images']['app'], ClamAvRepositoryName=m['images']['filescanner'], TaskRoleName=m['taskRoleArn'].rsplit('/', 1)[1], TaskExecutionRoleName=m['executionRoleArn'].rsplit('/', 1)[1]).items():
        d.require(parameters.get(key) == value, 'Stack settings differ from manifest')
    d.require({'AppImageDigest', 'ClamAvImageDigest'} <= set(parameters), 'Missing digest parameters')
    resources = aws.call('cloudformation', 'describe-stack-resources', StackName=expected_name)['StackResources']
    d.require(not any(r['ResourceType'] == 'AWS::SSM::Parameter' for r in resources), 'Administrator must detach retained deployment pointers')
    services = [r for r in resources if r['ResourceType'] == 'AWS::ECS::Service']
    d.require(len(services) == 1 and services[0]['PhysicalResourceId'] == d.arn(m, 'ecs', f"service/{m['cluster']}/{m['service']}"), 'Stack does not own the expected service')
    return stack


def update_parameters(stack, images):
    digests = dict(AppImageDigest=images['app'].split('@')[1], ClamAvImageDigest=images['filescanner'].split('@')[1])
    return [dict(ParameterKey=p['ParameterKey'], **({'ParameterValue': digests[p['ParameterKey']]} if p['ParameterKey'] in digests else {'UsePreviousValue': True})) for p in stack['Parameters']]


def release(aws, m, settings, publisher, tag):
    stack = reviewed_stack(aws, m, settings)
    service = d.read_service(aws, m)
    previous = service['taskDefinition']
    d.require(d.stable(service, previous), 'Existing service is not stable')
    response = aws.call('ecs', 'describe-task-definition', taskDefinition=previous, include=['TAGS'])
    task, tags = response['taskDefinition'], response.get('tags', [])
    d.task_request(m, task, tags, {name: 'preflight@sha256:' + '0' * 64 for name in m['images']})
    deadline = time.monotonic() + 900
    images = {name: publisher.publish(name, local, tag, deadline) for name, local in [('app', 'av-scanner-v6:candidate'), ('filescanner', 'av-scanner-v6-clamav:candidate')]}
    d.require(d.stable(d.read_service(aws, m), previous), 'Service changed during publication')
    current = reviewed_stack(aws, m, settings)
    d.require(current.get('LastUpdatedTime') == stack.get('LastUpdatedTime') and current['Parameters'] == stack['Parameters'], 'Concurrent stack update detected')
    aws.call('cloudformation', 'update-stack', StackName=stack['StackId'], UsePreviousTemplate=True,
             Parameters=update_parameters(stack, images), RoleARN=settings['roleArn'], ClientRequestToken='release-' + tag)
    deadline = time.monotonic() + 1200
    while time.monotonic() < deadline:
        current = aws.call('cloudformation', 'describe-stacks', StackName=stack['StackId'])['Stacks'][0]
        status = current['StackStatus']
        d.require(status in ('UPDATE_IN_PROGRESS', 'UPDATE_COMPLETE_CLEANUP_IN_PROGRESS', 'UPDATE_COMPLETE'), 'CloudFormation failed or rolled back')
        if status == 'UPDATE_COMPLETE':
            params = {p['ParameterKey']: p.get('ParameterValue') for p in current['Parameters']}
            d.require(params.get('AppImageDigest') == images['app'].split('@')[1] and params.get('ClamAvImageDigest') == images['filescanner'].split('@')[1], 'Stack digests differ')
            updated = d.read_service(aws, m)
            definition = updated['taskDefinition']
            if definition != previous and d.stable(updated, definition):
                saved = aws.call('ecs', 'describe-task-definition', taskDefinition=definition, include=['TAGS'])
                actual = d.task_request(m, saved['taskDefinition'], saved.get('tags', []), images)
                expected = d.task_request(m, task, tags, images)
                # Also inspect the actual stored images: task_request substitutes them.
                d.require(all(c['image'] == images[c['name']] for c in saved['taskDefinition']['containerDefinitions'] if c['name'] in images), 'Stack task images differ')
                actual['tags'] = sorted(actual.get('tags', []), key=lambda x: x['key'])
                expected['tags'] = sorted(expected.get('tags', []), key=lambda x: x['key'])
                # CloudFormation may return the same named containers in a
                # different order. Compare every field by container identity.
                actual['containerDefinitions'].sort(key=lambda x: x['name'])
                expected['containerDefinitions'].sort(key=lambda x: x['name'])
                d.require(actual == expected and updated['desiredCount'] == service['desiredCount'], 'Non-image service configuration changed')
                if d.verify_running(aws, m, definition, images, updated['desiredCount']): return definition
        time.sleep(10)
    raise d.DeployError('Stack or runtime health timed out')


def main():
    m = d.read_manifest(sys.argv[1])
    d.require(m['service'] == 'av-scanner-v6' and set(m['images']) == {'app', 'filescanner'}, 'Unexpected scanner manifest')
    d.require(os.environ.get('CIRCLE_BRANCH') == m['branch'] and not os.environ.get('CIRCLE_TAG'), 'Wrong branch')
    d.require(os.environ.get('CIRCLE_PROJECT_USERNAME', '') + '/' + os.environ.get('CIRCLE_PROJECT_REPONAME', '') == m['repository'].removeprefix('github.com/'), 'Wrong repository')
    tag = os.environ.get('CIRCLE_SHA1', '') + '-' + os.environ.get('CIRCLE_BUILD_NUM', '')
    d.require(d.re.fullmatch(r'[a-f0-9]{40}-[0-9]{1,12}', tag), 'Invalid release identity')
    settings = json.loads(Path(sys.argv[1]).with_name(m['environment'] + '-stack.json').read_text())
    aws = d.authenticate(m, os.environ.get('CIRCLE_OIDC_TOKEN_V3'), ACTIONS)
    with tempfile.TemporaryDirectory(prefix='av-release-') as directory:
        definition = release(aws, m, settings, d.Publisher(aws, m, directory), tag)
    print('Verified healthy scanner revision: ' + definition)


if __name__ == '__main__':
    try: main()
    except (d.DeployError, OSError, ValueError, KeyError, TypeError, IndexError):
        print('Scanner release stopped; inspect CloudFormation/ECS with an operator identity.', file=sys.stderr)
        sys.exit(1)
