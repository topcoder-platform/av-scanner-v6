# ECS/Fargate deployment runbook

This runbook moves AV Scanner v6 from the legacy
`file-scanning-processor-svc` ECS service into a dedicated `av-scanner-v6`
service. The services share the `file-scanner-processor` Kafka consumer group,
so exactly one service may have a nonzero desired count. Running both divides
partitions between them; changing the new service to another group would process
every event twice.

The migration uses two CloudFormation stacks:

| Purpose                | Stack                                                                                       | ECS service                   | Task family                   |
| ---------------------- | ------------------------------------------------------------------------------------------- | ----------------------------- | ----------------------------- |
| Legacy rollback source | `ECS-Console-V2-Service-file-scanning-processor-svc-av-scanner-service-serverless-e91eaaa3` | `file-scanning-processor-svc` | `file-scanning-processor-svc` |
| Dedicated v6 service   | `av-scanner-v6-dev`                                                                         | `av-scanner-v6`               | `av-scanner-v6`               |

The canonical CircleCI parameters remain under
`/config/av-scanner-v6/deployvar`. They are retained out of the legacy stack,
imported into the dedicated stack, and then updated in place to identify the new
stack and service. There must never be a moment when both stacks own them.

## Dev topology

The reviewed dev values are:

| Resource                    | Value                                                                                                             |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| Account / region            | `811668436784` / `us-east-1`                                                                                      |
| ECS cluster                 | `av-scanner-service-serverless`                                                                                   |
| New target group            | `av-scanner-v6-dev-tg` (HTTP, port 80, IP targets)                                                                |
| Target health               | `/health`, interval 120, timeout 30, thresholds 5/5                                                               |
| Deregistration delay        | 60 seconds                                                                                                        |
| HTTPS listener              | `arn:aws:elasticloadbalancing:us-east-1:811668436784:listener/app/services-alb/58c1445f76a32e27/eb5a28a369a58f3f` |
| Attachment-only rule        | priority `650`, path `/__av-scanner-v6-dev-attachment-only`                                                       |
| VPC / subnet                | `vpc-e8f0458d` / `subnet-1c8f9b34`                                                                                |
| Security group              | `sg-afba72dd`                                                                                                     |
| Log group                   | `/aws/ecs/av-scanner-v6`, retention 14 days                                                                       |
| App / ClamAV repositories   | `file-scanner-processor` / `clamav-service-processor`                                                             |
| Task / execution roles      | `avscan-service-role` / `ecsTaskExecutionRole`                                                                    |
| Runtime / common parameters | `/config/file-scanner-processor/buildvar` / `/config/common/global-appvar`                                        |
| Task size                   | 2 vCPU / 16 GiB, split evenly between the containers                                                              |

The listener rule only associates the new target group with the ALB so ECS can
register tasks. The application serves only `/health`; the attachment path is
expected to return 404 and does not replace the legacy priority-57
`/v5/scan-submission-processor` rule. AWS recommends a unique target group per
ECS service because sharing one can interfere with deployments.

## Files and ownership

- `deployment/reconcile-existing-service.yml` remains the authoritative
  template for the legacy stack after it releases the retained task definition
  and deploy parameters. It is used only to drain or restart the legacy service.
- `deployment/import-deploy-parameters.yml` is the temporary import topology for
  the new stack. It contains only the eight retained SSM parameters.
- `deployment/fargate-service.yml` is the dedicated steady-state topology. It
  owns the new service, task family, log group, target group, attachment rule,
  and the imported SSM parameters.
- `.circleci/config.yml` performs routine two-image deployments only after the
  manual bootstrap and zero-overlap cutover are complete.

The new service is created at desired count zero. Installing its task definition
and starting it are separate stack updates. The old service remains at zero for
rollback until it is deliberately retired.

## Current migration checkpoint

On 2026-07-20, dev completed the ownership transfer and zero-overlap cutover in
steps 1 through 5 below:

- the legacy stack released its retained task definition and eight deploy
  parameters and now owns only `ECSService`;
- `av-scanner-v6-dev` imported those eight parameters and then installed the
  complete 13-resource dedicated topology;
- the canonical deploy parameters identify the new stack, service, and task
  family;
- `file-scanning-processor-svc` is drained at `0/0/0`, with retained revision
  79 ready for rollback;
- `av-scanner-v6` is healthy at `1/1/0` on `av-scanner-v6:1`, with both
  containers healthy and the dedicated target healthy; and
- two post-drain Kafka checks returned `Empty` with zero members, followed by a
  post-start check that returned `Stable` with exactly one member.

The dev migration is complete. Do **not** rerun the ownership-transfer, legacy
drain, or initial start mutations. Keep the legacy stack and revision 79 at zero
through the agreed rollback window. The procedure remains below for audit and
for a separately discovered production migration.

Verify the checkpoint without reading parameter values:

```bash
set -Eeuo pipefail
set +x
export AWS_REGION=us-east-1
export ECS_CLUSTER=av-scanner-service-serverless
export LEGACY_SERVICE=file-scanning-processor-svc
export LEGACY_STACK=ECS-Console-V2-Service-file-scanning-processor-svc-av-scanner-service-serverless-e91eaaa3
export NEW_SERVICE=av-scanner-v6
export NEW_STACK=av-scanner-v6-dev

[[ "$(aws cloudformation list-stack-resources \
  --region "$AWS_REGION" \
  --stack-name "$LEGACY_STACK" \
  --query 'length(StackResourceSummaries)' \
  --output text)" == 1 ]]
[[ "$(aws cloudformation list-stack-resources \
  --region "$AWS_REGION" \
  --stack-name "$LEGACY_STACK" \
  --query 'StackResourceSummaries[0].LogicalResourceId' \
  --output text)" == ECSService ]]
[[ "$(aws cloudformation list-stack-resources \
  --region "$AWS_REGION" \
  --stack-name "$NEW_STACK" \
  --query 'length(StackResourceSummaries)' \
  --output text)" == 13 ]]

checkpoint_services="$(aws ecs describe-services \
  --region "$AWS_REGION" \
  --cluster "$ECS_CLUSTER" \
  --services "$LEGACY_SERVICE" "$NEW_SERVICE")"
jq -e --arg legacy "$LEGACY_SERVICE" --arg new "$NEW_SERVICE" '
  (.failures | length) == 0 and
  (.services | length) == 2 and
  ([.services[] | select(.serviceName == $legacy) |
      .desiredCount, .runningCount, .pendingCount] == [0, 0, 0]) and
  ([.services[] | select(.serviceName == $new) |
      .desiredCount, .runningCount, .pendingCount] == [1, 1, 0])
' <<<"$checkpoint_services" >/dev/null
unset checkpoint_services
```

## Initialize and validate

Use one protected Bash session. Do not enable shell tracing while owner values,
credentials, or parameter values are in memory.

```bash
set -Eeuo pipefail
set +x

export AWS_REGION=us-east-1
export EXPECTED_ACCOUNT=811668436784
export ECS_CLUSTER=av-scanner-service-serverless
export LEGACY_SERVICE=file-scanning-processor-svc
export LEGACY_STACK=ECS-Console-V2-Service-file-scanning-processor-svc-av-scanner-service-serverless-e91eaaa3
export NEW_SERVICE=av-scanner-v6
export NEW_STACK=av-scanner-v6-dev
export DEPLOY_PREFIX=/config/av-scanner-v6/deployvar
export VPC_ID=vpc-e8f0458d
export SUBNETS=subnet-1c8f9b34
export SECURITY_GROUPS=sg-afba72dd
export HTTPS_LISTENER_ARN=arn:aws:elasticloadbalancing:us-east-1:811668436784:listener/app/services-alb/58c1445f76a32e27/eb5a28a369a58f3f
export ATTACHMENT_RULE_PRIORITY=650
export APP_REPOSITORY=file-scanner-processor
export CLAMAV_REPOSITORY=clamav-service-processor
export TASK_ROLE=avscan-service-role
export EXECUTION_ROLE=ecsTaskExecutionRole
export RUNTIME_PREFIX=/config/file-scanner-processor/buildvar
export COMMON_PREFIX=/config/common/global-appvar
export ENVIRONMENT_NAME=dev
export KAFKA_CLIENT_CERT_PARAMETER_ARN="${KAFKA_CLIENT_CERT_PARAMETER_ARN-}"
export KAFKA_CLIENT_KEY_PARAMETER_ARN="${KAFKA_CLIENT_KEY_PARAMETER_ARN-}"
export KAFKA_CA_PARAMETER_ARN="${KAFKA_CA_PARAMETER_ARN-}"
export KAFKA_REJECT_UNAUTHORIZED="${KAFKA_REJECT_UNAUTHORIZED-}"

caller_account="$(aws sts get-caller-identity --query Account --output text)"
[[ "$caller_account" == "$EXPECTED_ACCOUNT" ]]
unset caller_account

aws cloudformation validate-template \
  --region "$AWS_REGION" \
  --template-body file://deployment/reconcile-existing-service.yml >/dev/null
aws cloudformation validate-template \
  --region "$AWS_REGION" \
  --template-body file://deployment/import-deploy-parameters.yml >/dev/null
aws cloudformation validate-template \
  --region "$AWS_REGION" \
  --template-body file://deployment/fargate-service.yml >/dev/null

corepack pnpm install --frozen-lockfile
corepack pnpm lint
corepack pnpm typecheck
corepack pnpm build
corepack pnpm test
```

Capture live legacy state rather than trusting the recorded revision:

```bash
legacy_service_json="$(aws ecs describe-services \
  --region "$AWS_REGION" \
  --cluster "$ECS_CLUSTER" \
  --services "$LEGACY_SERVICE")"
jq -e '
  (.failures | length) == 0 and
  (.services | length) == 1 and
  .services[0].status == "ACTIVE" and
  .services[0].desiredCount == .services[0].runningCount and
  .services[0].pendingCount == 0
' <<<"$legacy_service_json" >/dev/null

export LEGACY_TASK_DEFINITION="$(jq -er '.services[0].taskDefinition' \
  <<<"$legacy_service_json")"
live_legacy_desired="$(jq -er '.services[0].desiredCount' \
  <<<"$legacy_service_json")"
if (( live_legacy_desired > 0 )); then
  export LEGACY_RESTORE_COUNT="$live_legacy_desired"
else
  # Dev's reviewed pre-drain count is one. A different environment must export
  # its captured pre-drain count before resuming from an already-drained state.
  : "${LEGACY_RESTORE_COUNT:=1}"
  export LEGACY_RESTORE_COUNT
fi
unset live_legacy_desired
export LEGACY_TASK_FAMILY="$(aws ecs describe-task-definition \
  --region "$AWS_REGION" \
  --task-definition "$LEGACY_TASK_DEFINITION" \
  --query 'taskDefinition.family' \
  --output text)"
[[ "$LEGACY_TASK_FAMILY" == file-scanning-processor-svc ]]
```

Collect the legacy network and owner-tag inputs without printing owner values:

```bash
export LEGACY_TARGET_GROUP_ARN="$(jq -er \
  '.services[0].loadBalancers[0].targetGroupArn' \
  <<<"$legacy_service_json")"
unset legacy_service_json

legacy_service_arn="$(aws ecs describe-services \
  --region "$AWS_REGION" \
  --cluster "$ECS_CLUSTER" \
  --services "$LEGACY_SERVICE" \
  --query 'services[0].serviceArn' \
  --output text)"
legacy_tags="$(aws ecs list-tags-for-resource \
  --region "$AWS_REGION" \
  --resource-arn "$legacy_service_arn")"
owner_email="$(jq -er '.tags[] | select(.key=="topcoder:application:OwnerEmail") | .value' <<<"$legacy_tags")"
alt_owner_email="$(jq -er '.tags[] | select(.key=="topcoder:application:AltOwnerEmail") | .value' <<<"$legacy_tags")"
owner_manager_email="$(jq -er '.tags[] | select(.key=="topcoder:application:OwnerMgrEmail") | .value' <<<"$legacy_tags")"
owner_director_email="$(jq -er '.tags[] | select(.key=="topcoder:application:OwnerDirectorEmail") | .value' <<<"$legacy_tags")"
unset legacy_tags legacy_service_arn
```

Choose an immutable tag that exists in both repositories. When inspecting the
recorded dev deployment, read the tag already installed in the dedicated stack.
For a new environment, export `IMAGE_TAG` to a reviewed tag built and pushed to
both repositories from the same commit before running this block.

```bash
installed_image_tag="$(aws cloudformation describe-stacks \
  --region "$AWS_REGION" \
  --stack-name "$NEW_STACK" \
  --query "Stacks[0].Parameters[?ParameterKey=='ImageTag'].ParameterValue | [0]" \
  --output text 2>/dev/null || true)"
if [[ -n "$installed_image_tag" && "$installed_image_tag" != None ]]; then
  export IMAGE_TAG="$installed_image_tag"
else
  : "${IMAGE_TAG:?Set a reviewed immutable tag present in both repositories}"
  export IMAGE_TAG
fi
unset installed_image_tag
[[ "$IMAGE_TAG" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]]

for repository in "$APP_REPOSITORY" "$CLAMAV_REPOSITORY"; do
  [[ "$(aws ecr describe-repositories \
    --region "$AWS_REGION" \
    --repository-names "$repository" \
    --query 'repositories[0].imageTagMutability' \
    --output text)" == IMMUTABLE ]]
  aws ecr describe-images \
    --region "$AWS_REGION" \
    --repository-name "$repository" \
    --image-ids "imageTag=$IMAGE_TAG" >/dev/null
done
```

Helper functions used below:

```bash
parameter_json() {
  printf '%s\n' "$@" | jq -Rn '
    [inputs | capture("^(?<ParameterKey>[^=]+)=(?<ParameterValue>.*)$")]
  '
}

desired_count_parameter_json() {
  local stack="$1"
  local desired="$2"
  aws cloudformation describe-stacks \
    --region "$AWS_REGION" \
    --stack-name "$stack" \
    --output json |
    jq -c --arg desired "$desired" '
      [.Stacks[0].Parameters[] |
        if .ParameterKey == "DesiredCount" then
          {ParameterKey, ParameterValue: $desired}
        else
          {ParameterKey, UsePreviousValue: true}
        end]
    '
}

assert_service_zero() {
  local service="$1"
  local state
  state="$(aws ecs describe-services \
    --region "$AWS_REGION" \
    --cluster "$ECS_CLUSTER" \
    --services "$service")"
  jq -e '
    (.failures | length) == 0 and
    (.services | length) == 1 and
    .services[0].desiredCount == 0 and
    .services[0].runningCount == 0 and
    .services[0].pendingCount == 0
  ' <<<"$state" >/dev/null
  [[ "$(aws ecs list-tasks \
    --region "$AWS_REGION" \
    --cluster "$ECS_CLUSTER" \
    --service-name "$service" \
    --desired-status RUNNING \
    --query 'length(taskArns)' \
    --output text)" == 0 ]]
}
```

## 1. Release retained resources from the legacy stack

The reconciliation update changes the legacy stack to its one-service topology.
It must remove exactly the retained task-definition logical resource and eight
retained SSM logical resources. The physical resources must survive.

This release is safe only after the legacy stack's currently deployed template
has `DeletionPolicy: Retain` and `UpdateReplacePolicy: Retain` on all nine
resources. Dev landed that metadata in a separate retain-only update before
this procedure. In another environment, first copy the deployed legacy
template, add only those policies, and execute a change set whose resource
scopes are only `DeletionPolicy` and `UpdateReplacePolicy`. Do not use the
dedicated-service template for that metadata update. The release preview below
must then report `PolicyAction: Retain` on every removal; otherwise stop.

```bash
reconcile_parameters="$(parameter_json \
  "ECSClusterName=$ECS_CLUSTER" \
  "ECSServiceName=$LEGACY_SERVICE" \
  "SecurityGroupIDs=$SECURITY_GROUPS" \
  "SubnetIDs=$SUBNETS" \
  "TargetGroupArn=$LEGACY_TARGET_GROUP_ARN" \
  "CurrentTaskDefinitionArn=$LEGACY_TASK_DEFINITION" \
  "DesiredCount=$LEGACY_RESTORE_COUNT" \
  "EnvironmentName=$ENVIRONMENT_NAME" \
  "ApplicationOwnerEmail=$owner_email" \
  "ApplicationAltOwnerEmail=$alt_owner_email" \
  "ApplicationOwnerManagerEmail=$owner_manager_email" \
  "ApplicationOwnerDirectorEmail=$owner_director_email")"

release_change_set="av-scanner-release-retained-$(date -u +%Y%m%dT%H%M%SZ)-$RANDOM"
aws cloudformation create-change-set \
  --region "$AWS_REGION" \
  --stack-name "$LEGACY_STACK" \
  --change-set-name "$release_change_set" \
  --change-set-type UPDATE \
  --template-body file://deployment/reconcile-existing-service.yml \
  --parameters "$reconcile_parameters" >/dev/null
aws cloudformation wait change-set-create-complete \
  --region "$AWS_REGION" \
  --stack-name "$LEGACY_STACK" \
  --change-set-name "$release_change_set"

release_change_json="$(aws cloudformation describe-change-set \
  --region "$AWS_REGION" \
  --stack-name "$LEGACY_STACK" \
  --change-set-name "$release_change_set" \
  --include-property-values)"
jq '.Changes[].ResourceChange | {Action,LogicalResourceId,ResourceType,Replacement,PolicyAction}' \
  <<<"$release_change_json"
jq -e '
  .Changes as $changes |
  ([ $changes[].ResourceChange | select(.Action == "Remove") ] |
    all(.PolicyAction == "Retain")) and
  ([ $changes[].ResourceChange | select(.Action == "Remove") |
      .LogicalResourceId ] | sort) ==
    ([
      "ScannerTaskDefinition",
      "DeployCloudFormationStackParameter",
      "DeployClusterParameter",
      "DeployServiceParameter",
      "DeployTaskFamilyParameter",
      "DeployAppContainerParameter",
      "DeployClamAvContainerParameter",
      "DeployAppRepositoryParameter",
      "DeployClamAvRepositoryParameter"
    ] | sort) and
  ([ $changes[].ResourceChange |
      select(.Action != "Remove") ] |
    all(.LogicalResourceId == "ECSService" and
        .Action == "Modify" and
        .Replacement == "False" and
        ([.Details[]?.Target.Name] | unique) == ["TaskDefinition"]))
' <<<"$release_change_json" >/dev/null

# The optional ECSService modification must resolve to the task already live.
[[ "$(aws ecs describe-services \
  --region "$AWS_REGION" \
  --cluster "$ECS_CLUSTER" \
  --services "$LEGACY_SERVICE" \
  --query 'services[0].taskDefinition' \
  --output text)" == "$LEGACY_TASK_DEFINITION" ]]

aws cloudformation execute-change-set \
  --region "$AWS_REGION" \
  --stack-name "$LEGACY_STACK" \
  --change-set-name "$release_change_set"
aws cloudformation wait stack-update-complete \
  --region "$AWS_REGION" \
  --stack-name "$LEGACY_STACK"
```

Verify the logical resources left the old stack while every physical parameter
and the live task definition still exist:

```bash
old_resource_ids="$(aws cloudformation list-stack-resources \
  --region "$AWS_REGION" \
  --stack-name "$LEGACY_STACK" \
  --query 'StackResourceSummaries[].LogicalResourceId' \
  --output json)"
jq -e '
  length == 1 and .[0] == "ECSService"
' <<<"$old_resource_ids" >/dev/null
unset old_resource_ids release_change_json

deploy_suffixes=(
  AWS_CLOUDFORMATION_STACK_NAME
  AWS_ECS_CLUSTER
  AWS_ECS_SERVICE
  AWS_ECS_TASK_FAMILY
  AWS_ECS_APP_CONTAINER_NAME
  AWS_ECS_CLAMAV_CONTAINER_NAME
  AWS_REPOSITORY
  AWS_REPOSITORY_CLAMAV
)
for suffix in "${deploy_suffixes[@]}"; do
  aws ssm get-parameter \
    --region "$AWS_REGION" \
    --name "$DEPLOY_PREFIX/$suffix" \
    --query 'Parameter.Name' \
    --output text >/dev/null
done
aws ecs describe-task-definition \
  --region "$AWS_REGION" \
  --task-definition "$LEGACY_TASK_DEFINITION" >/dev/null
```

## 2. Import the deploy parameters into the new stack

The import template deliberately describes their current legacy values. An
IMPORT change set cannot add the ECS topology at the same time.

```bash
import_spec="$(mktemp)"
jq -n --arg prefix "$DEPLOY_PREFIX" '[
  {ResourceType:"AWS::SSM::Parameter",LogicalResourceId:"DeployCloudFormationStackParameter",ResourceIdentifier:{Name:($prefix+"/AWS_CLOUDFORMATION_STACK_NAME")}},
  {ResourceType:"AWS::SSM::Parameter",LogicalResourceId:"DeployClusterParameter",ResourceIdentifier:{Name:($prefix+"/AWS_ECS_CLUSTER")}},
  {ResourceType:"AWS::SSM::Parameter",LogicalResourceId:"DeployServiceParameter",ResourceIdentifier:{Name:($prefix+"/AWS_ECS_SERVICE")}},
  {ResourceType:"AWS::SSM::Parameter",LogicalResourceId:"DeployTaskFamilyParameter",ResourceIdentifier:{Name:($prefix+"/AWS_ECS_TASK_FAMILY")}},
  {ResourceType:"AWS::SSM::Parameter",LogicalResourceId:"DeployAppContainerParameter",ResourceIdentifier:{Name:($prefix+"/AWS_ECS_APP_CONTAINER_NAME")}},
  {ResourceType:"AWS::SSM::Parameter",LogicalResourceId:"DeployClamAvContainerParameter",ResourceIdentifier:{Name:($prefix+"/AWS_ECS_CLAMAV_CONTAINER_NAME")}},
  {ResourceType:"AWS::SSM::Parameter",LogicalResourceId:"DeployAppRepositoryParameter",ResourceIdentifier:{Name:($prefix+"/AWS_REPOSITORY")}},
  {ResourceType:"AWS::SSM::Parameter",LogicalResourceId:"DeployClamAvRepositoryParameter",ResourceIdentifier:{Name:($prefix+"/AWS_REPOSITORY_CLAMAV")}}
]' >"$import_spec"

import_parameters="$(parameter_json \
  "DeployParameterPrefix=$DEPLOY_PREFIX" \
  "LegacyStackName=$LEGACY_STACK" \
  "ECSClusterName=$ECS_CLUSTER" \
  "LegacyECSServiceName=$LEGACY_SERVICE" \
  "LegacyTaskFamily=$LEGACY_TASK_FAMILY" \
  "AppRepositoryName=$APP_REPOSITORY" \
  "ClamAvRepositoryName=$CLAMAV_REPOSITORY")"

import_change_set="av-scanner-import-parameters-$(date -u +%Y%m%dT%H%M%SZ)-$RANDOM"
aws cloudformation create-change-set \
  --region "$AWS_REGION" \
  --stack-name "$NEW_STACK" \
  --change-set-name "$import_change_set" \
  --change-set-type IMPORT \
  --template-body file://deployment/import-deploy-parameters.yml \
  --parameters "$import_parameters" \
  --resources-to-import "file://$import_spec" >/dev/null
aws cloudformation wait change-set-create-complete \
  --region "$AWS_REGION" \
  --stack-name "$NEW_STACK" \
  --change-set-name "$import_change_set"

import_change_json="$(aws cloudformation describe-change-set \
  --region "$AWS_REGION" \
  --stack-name "$NEW_STACK" \
  --change-set-name "$import_change_set")"
jq '.Changes[].ResourceChange | {Action,LogicalResourceId,ResourceType}' \
  <<<"$import_change_json"
jq -e '
  [.Changes[].ResourceChange] as $changes |
  ($changes | length) == 8 and
  ([$changes[].LogicalResourceId] | sort) ==
    ([
      "DeployCloudFormationStackParameter",
      "DeployClusterParameter",
      "DeployServiceParameter",
      "DeployTaskFamilyParameter",
      "DeployAppContainerParameter",
      "DeployClamAvContainerParameter",
      "DeployAppRepositoryParameter",
      "DeployClamAvRepositoryParameter"
    ] | sort) and
  all($changes[]; .Action == "Import" and
                  .ResourceType == "AWS::SSM::Parameter")
' <<<"$import_change_json" >/dev/null

aws cloudformation execute-change-set \
  --region "$AWS_REGION" \
  --stack-name "$NEW_STACK" \
  --change-set-name "$import_change_set"
aws cloudformation wait stack-import-complete \
  --region "$AWS_REGION" \
  --stack-name "$NEW_STACK"
aws cloudformation update-termination-protection \
  --region "$AWS_REGION" \
  --stack-name "$NEW_STACK" \
  --enable-termination-protection
rm -f "$import_spec"
unset import_spec import_change_json
```

Require exactly eight imported resources and no drift before changing their
values:

```bash
[[ "$(aws cloudformation list-stack-resources \
  --region "$AWS_REGION" \
  --stack-name "$NEW_STACK" \
  --query 'length(StackResourceSummaries)' \
  --output text)" == 8 ]]

drift_detection_id="$(aws cloudformation detect-stack-drift \
  --region "$AWS_REGION" \
  --stack-name "$NEW_STACK" \
  --query StackDriftDetectionId \
  --output text)"
drift_detection_status=DETECTION_IN_PROGRESS
for _attempt in {1..60}; do
  drift_detection_status="$(aws cloudformation describe-stack-drift-detection-status \
    --region "$AWS_REGION" \
    --stack-drift-detection-id "$drift_detection_id" \
    --query DetectionStatus \
    --output text)"
  case "$drift_detection_status" in
    DETECTION_COMPLETE) break ;;
    DETECTION_FAILED) exit 1 ;;
  esac
  sleep 5
done
[[ "$drift_detection_status" == DETECTION_COMPLETE ]]
[[ "$(aws cloudformation describe-stack-drift-detection-status \
  --region "$AWS_REGION" \
  --stack-drift-detection-id "$drift_detection_id" \
  --query StackDriftStatus \
  --output text)" == IN_SYNC ]]
```

## 3. Add the dedicated topology at desired count zero

The reviewed tag must exist before this update. The stack will add the task
definition, log group, target group, listener rule, and service. The imported
parameters for stack, service, and task family change from legacy to v6 values.

```bash
full_parameters="$(parameter_json \
  "ECSClusterName=$ECS_CLUSTER" \
  "SecurityGroupIDs=$SECURITY_GROUPS" \
  "SubnetIDs=$SUBNETS" \
  "VpcId=$VPC_ID" \
  "HttpsListenerArn=$HTTPS_LISTENER_ARN" \
  "AttachmentRulePriority=$ATTACHMENT_RULE_PRIORITY" \
  "DesiredCount=0" \
  "ImageTag=$IMAGE_TAG" \
  "AppRepositoryName=$APP_REPOSITORY" \
  "ClamAvRepositoryName=$CLAMAV_REPOSITORY" \
  "TaskCpu=2048" \
  "TaskMemory=16384" \
  "AppContainerCpu=1024" \
  "AppContainerMemory=8192" \
  "ClamAvContainerCpu=1024" \
  "ClamAvContainerMemory=8192" \
  "TaskRoleName=$TASK_ROLE" \
  "TaskExecutionRoleName=$EXECUTION_ROLE" \
  "RuntimeParameterPrefix=$RUNTIME_PREFIX" \
  "CommonParameterPrefix=$COMMON_PREFIX" \
  "DeployParameterPrefix=$DEPLOY_PREFIX" \
  "EnvironmentName=$ENVIRONMENT_NAME" \
  "ApplicationOwnerEmail=$owner_email" \
  "ApplicationAltOwnerEmail=$alt_owner_email" \
  "ApplicationOwnerManagerEmail=$owner_manager_email" \
  "ApplicationOwnerDirectorEmail=$owner_director_email" \
  "KafkaClientCertParameterArn=$KAFKA_CLIENT_CERT_PARAMETER_ARN" \
  "KafkaClientKeyParameterArn=$KAFKA_CLIENT_KEY_PARAMETER_ARN" \
  "KafkaCaParameterArn=$KAFKA_CA_PARAMETER_ARN" \
  "KafkaRejectUnauthorized=$KAFKA_REJECT_UNAUTHORIZED")"

install_change_set="av-scanner-install-zero-$(date -u +%Y%m%dT%H%M%SZ)-$RANDOM"
aws cloudformation create-change-set \
  --region "$AWS_REGION" \
  --stack-name "$NEW_STACK" \
  --change-set-name "$install_change_set" \
  --change-set-type UPDATE \
  --template-body file://deployment/fargate-service.yml \
  --parameters "$full_parameters" >/dev/null
aws cloudformation wait change-set-create-complete \
  --region "$AWS_REGION" \
  --stack-name "$NEW_STACK" \
  --change-set-name "$install_change_set"

install_change_json="$(aws cloudformation describe-change-set \
  --region "$AWS_REGION" \
  --stack-name "$NEW_STACK" \
  --change-set-name "$install_change_set")"
jq '.Changes[].ResourceChange | {Action,LogicalResourceId,ResourceType,Replacement}' \
  <<<"$install_change_json"
jq -e '
  [.Changes[].ResourceChange] as $changes |
  ($changes | length) == 8 and
  ($changes | map({
    id: .LogicalResourceId,
    action: .Action,
    type: .ResourceType
  }) | sort_by(.id)) == ([
    {id:"DeployCloudFormationStackParameter",action:"Modify",type:"AWS::SSM::Parameter"},
    {id:"DeployServiceParameter",action:"Modify",type:"AWS::SSM::Parameter"},
    {id:"DeployTaskFamilyParameter",action:"Modify",type:"AWS::SSM::Parameter"},
    {id:"ECSService",action:"Add",type:"AWS::ECS::Service"},
    {id:"ScannerAttachmentRule",action:"Add",type:"AWS::ElasticLoadBalancingV2::ListenerRule"},
    {id:"ScannerLogGroup",action:"Add",type:"AWS::Logs::LogGroup"},
    {id:"ScannerTargetGroup",action:"Add",type:"AWS::ElasticLoadBalancingV2::TargetGroup"},
    {id:"ScannerTaskDefinition",action:"Add",type:"AWS::ECS::TaskDefinition"}
  ] | sort_by(.id)) and
  all($changes[];
    if .Action == "Modify" then
      .Replacement == "False" and
      (.Scope | sort) == ["Properties"] and
      ([.Details[]?.Target.Name] | unique) == ["Value"]
    else
      .Action == "Add"
    end)
' <<<"$install_change_json" >/dev/null

# The old service must still be the only running consumer at this point.
[[ "$(aws ecs describe-services \
  --region "$AWS_REGION" \
  --cluster "$ECS_CLUSTER" \
  --services "$LEGACY_SERVICE" \
  --query 'services[0].runningCount' \
  --output text)" == "$LEGACY_RESTORE_COUNT" ]]

aws cloudformation execute-change-set \
  --region "$AWS_REGION" \
  --stack-name "$NEW_STACK" \
  --change-set-name "$install_change_set"
aws cloudformation wait stack-update-complete \
  --region "$AWS_REGION" \
  --stack-name "$NEW_STACK"
aws ecs wait services-stable \
  --region "$AWS_REGION" \
  --cluster "$ECS_CLUSTER" \
  --services "$NEW_SERVICE"
assert_service_zero "$NEW_SERVICE"
```

Verify the canonical deploy parameters now point exclusively at the dedicated
stack. The assertion consumes values without printing them:

```bash
deploy_values="$(aws ssm get-parameters-by-path \
  --region "$AWS_REGION" \
  --path "$DEPLOY_PREFIX" \
  --recursive \
  --output json)"
jq -e \
  --arg prefix "$DEPLOY_PREFIX" \
  --arg stack "$NEW_STACK" \
  --arg cluster "$ECS_CLUSTER" \
  --arg service "$NEW_SERVICE" \
  --arg app_repository "$APP_REPOSITORY" \
  --arg clamav_repository "$CLAMAV_REPOSITORY" '
  (.Parameters | length) == 8 and
  (reduce .Parameters[] as $parameter ({};
    .[$parameter.Name | sub("^" + $prefix + "/"; "")] = $parameter.Value)) as $p |
  $p.AWS_CLOUDFORMATION_STACK_NAME == $stack and
  $p.AWS_ECS_CLUSTER == $cluster and
  $p.AWS_ECS_SERVICE == $service and
  $p.AWS_ECS_TASK_FAMILY == $service and
  $p.AWS_ECS_APP_CONTAINER_NAME == "app" and
  $p.AWS_ECS_CLAMAV_CONTAINER_NAME == "filescanner" and
  $p.AWS_REPOSITORY == $app_repository and
  $p.AWS_REPOSITORY_CLAMAV == $clamav_repository
' <<<"$deploy_values" >/dev/null
unset deploy_values install_change_json
```

CircleCI must remain disabled until the new service is started and validated;
its post-deploy checks require running tasks.

## 4. Drain the legacy service

Dev currently has no Application Auto Scaling targets, policies, or scheduled
actions. Recheck immediately before the drain and stop if any are present:

```bash
scalable_resource="service/$ECS_CLUSTER/$LEGACY_SERVICE"
[[ "$(aws application-autoscaling describe-scalable-targets \
  --region "$AWS_REGION" \
  --service-namespace ecs \
  --resource-ids "$scalable_resource" \
  --scalable-dimension ecs:service:DesiredCount \
  --query 'length(ScalableTargets)' \
  --output text)" == 0 ]]
[[ "$(aws application-autoscaling describe-scaling-policies \
  --region "$AWS_REGION" \
  --service-namespace ecs \
  --resource-id "$scalable_resource" \
  --scalable-dimension ecs:service:DesiredCount \
  --query 'length(ScalingPolicies)' \
  --output text)" == 0 ]]
[[ "$(aws application-autoscaling describe-scheduled-actions \
  --region "$AWS_REGION" \
  --service-namespace ecs \
  --resource-id "$scalable_resource" \
  --scalable-dimension ecs:service:DesiredCount \
  --query 'length(ScheduledActions)' \
  --output text)" == 0 ]]
```

Create a fresh change set whose only resource change is
`ECSService.DesiredCount`:

```bash
legacy_drain_parameters="$(desired_count_parameter_json "$LEGACY_STACK" 0)"
drain_change_set="av-scanner-drain-legacy-$(date -u +%Y%m%dT%H%M%SZ)-$RANDOM"
aws cloudformation create-change-set \
  --region "$AWS_REGION" \
  --stack-name "$LEGACY_STACK" \
  --change-set-name "$drain_change_set" \
  --change-set-type UPDATE \
  --template-body file://deployment/reconcile-existing-service.yml \
  --parameters "$legacy_drain_parameters" >/dev/null
aws cloudformation wait change-set-create-complete \
  --region "$AWS_REGION" \
  --stack-name "$LEGACY_STACK" \
  --change-set-name "$drain_change_set"

drain_change_json="$(aws cloudformation describe-change-set \
  --region "$AWS_REGION" \
  --stack-name "$LEGACY_STACK" \
  --change-set-name "$drain_change_set")"
jq -e '
  .Changes as $changes |
  ($changes | length) == 1 and
  $changes[0].ResourceChange.LogicalResourceId == "ECSService" and
  $changes[0].ResourceChange.Action == "Modify" and
  $changes[0].ResourceChange.Replacement == "False" and
  ([$changes[0].ResourceChange.Details[]?.Target.Name] | unique) ==
    ["DesiredCount"]
' <<<"$drain_change_json" >/dev/null

assert_service_zero "$NEW_SERVICE"
[[ "$(aws ecs describe-services \
  --region "$AWS_REGION" \
  --cluster "$ECS_CLUSTER" \
  --services "$LEGACY_SERVICE" \
  --query 'services[0].taskDefinition' \
  --output text)" == "$LEGACY_TASK_DEFINITION" ]]

aws cloudformation execute-change-set \
  --region "$AWS_REGION" \
  --stack-name "$LEGACY_STACK" \
  --change-set-name "$drain_change_set"
aws cloudformation wait stack-update-complete \
  --region "$AWS_REGION" \
  --stack-name "$LEGACY_STACK"
aws ecs wait services-stable \
  --region "$AWS_REGION" \
  --cluster "$ECS_CLUSTER" \
  --services "$LEGACY_SERVICE"
assert_service_zero "$LEGACY_SERVICE"
assert_service_zero "$NEW_SERVICE"
```

Run the rest of this section from one approved host that has network access to
the brokers, AWS read access for the ECS assertions, and Apache Kafka CLI
`3.9.1`. The dev brokers use the existing plaintext configuration, so the
following creates an empty command-config file. A TLS/mTLS environment must
instead export `KAFKA_ADMIN_PROPERTIES` to a pre-provisioned, mode-600 client
properties file before this block; do not create an empty file there. Raw Kafka
output is captured and never printed.

```bash
command -v kafka-consumer-groups.sh >/dev/null
[[ "$(kafka-consumer-groups.sh --version 2>&1 | awk 'NR == 1 {print $1}')" == 3.9.1 ]]

export KAFKA_BOOTSTRAP_SERVER="$(aws ssm get-parameter \
  --region "$AWS_REGION" \
  --name "$COMMON_PREFIX/KAFKA_URL" \
  --with-decryption \
  --query Parameter.Value \
  --output text)"
[[ -n "$KAFKA_BOOTSTRAP_SERVER" ]]

if [[ -z ${KAFKA_ADMIN_PROPERTIES-} ]]; then
  [[ "$ENVIRONMENT_NAME" == dev ]]
  export KAFKA_ADMIN_PROPERTIES="$(mktemp)"
  chmod 600 "$KAFKA_ADMIN_PROPERTIES"
  trap 'rm -f "$KAFKA_ADMIN_PROPERTIES"' EXIT
fi
[[ -r "$KAFKA_ADMIN_PROPERTIES" ]]
```

Require a stable zero-member result twice, separated by more than the
configured 30-second session timeout:

```bash
require_kafka_members() (
  set +x
  local expected="${1:?Pass expected member count}"
  local group=file-scanner-processor
  local output
  [[ -n ${KAFKA_BOOTSTRAP_SERVER-} ]]
  [[ -n ${KAFKA_ADMIN_PROPERTIES-} && -r ${KAFKA_ADMIN_PROPERTIES-} ]]
  output="$(LC_ALL=C kafka-consumer-groups.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVER" \
    --command-config "$KAFKA_ADMIN_PROPERTIES" \
    --group "$group" \
    --describe \
    --state \
    --timeout 30000 2>&1)" || return 1
  LC_ALL=C awk -v group="$group" -v expected="$expected" '
    $1 == group && $NF ~ /^[0-9]+$/ {
      rows++
      state = $(NF-1)
      members = $NF + 0
      if (members != expected) bad = 1
      if (expected == 0 && state != "Empty") bad = 1
      if (expected > 0 && state != "Stable") bad = 1
    }
    END { exit !(rows == 1 && bad == 0) }
  ' <<<"$output"
)

require_kafka_members 0
sleep 35
assert_service_zero "$LEGACY_SERVICE"
assert_service_zero "$NEW_SERVICE"
require_kafka_members 0
```

If either check fails or cannot run, do not start the new service. Restore the
legacy desired count with a reviewed DesiredCount-only change set.

## 5. Start and validate the dedicated service

Create a new change set. Do not reuse the install-at-zero or legacy-drain change
set, and do not combine the start with an image or task-definition change.

```bash
new_start_parameters="$(desired_count_parameter_json \
  "$NEW_STACK" "$LEGACY_RESTORE_COUNT")"
start_change_set="av-scanner-start-v6-$(date -u +%Y%m%dT%H%M%SZ)-$RANDOM"
aws cloudformation create-change-set \
  --region "$AWS_REGION" \
  --stack-name "$NEW_STACK" \
  --change-set-name "$start_change_set" \
  --change-set-type UPDATE \
  --template-body file://deployment/fargate-service.yml \
  --parameters "$new_start_parameters" >/dev/null
aws cloudformation wait change-set-create-complete \
  --region "$AWS_REGION" \
  --stack-name "$NEW_STACK" \
  --change-set-name "$start_change_set"

start_change_json="$(aws cloudformation describe-change-set \
  --region "$AWS_REGION" \
  --stack-name "$NEW_STACK" \
  --change-set-name "$start_change_set")"
jq -e '
  .Changes as $changes |
  ($changes | length) == 1 and
  $changes[0].ResourceChange.LogicalResourceId == "ECSService" and
  $changes[0].ResourceChange.Action == "Modify" and
  $changes[0].ResourceChange.Replacement == "False" and
  ([$changes[0].ResourceChange.Details[]?.Target.Name] | unique) ==
    ["DesiredCount"]
' <<<"$start_change_json" >/dev/null

assert_service_zero "$LEGACY_SERVICE"
assert_service_zero "$NEW_SERVICE"
require_kafka_members 0
for repository in "$APP_REPOSITORY" "$CLAMAV_REPOSITORY"; do
  aws ecr describe-images \
    --region "$AWS_REGION" \
    --repository-name "$repository" \
    --image-ids "imageTag=$IMAGE_TAG" >/dev/null
done

rollout_start_ms=$(( $(date +%s) * 1000 ))
aws cloudformation execute-change-set \
  --region "$AWS_REGION" \
  --stack-name "$NEW_STACK" \
  --change-set-name "$start_change_set"
aws cloudformation wait stack-update-complete \
  --region "$AWS_REGION" \
  --stack-name "$NEW_STACK"
aws ecs wait services-stable \
  --region "$AWS_REGION" \
  --cluster "$ECS_CLUSTER" \
  --services "$NEW_SERVICE"
```

Validate the service, task family, images, container health, target health, and
Kafka membership:

```bash
new_service_json="$(aws ecs describe-services \
  --region "$AWS_REGION" \
  --cluster "$ECS_CLUSTER" \
  --services "$NEW_SERVICE")"
jq -e --argjson desired "$LEGACY_RESTORE_COUNT" '
  (.failures | length) == 0 and
  (.services | length) == 1 and
  .services[0].desiredCount == $desired and
  .services[0].runningCount == $desired and
  .services[0].pendingCount == 0 and
  .services[0].deployments[0].rolloutState == "COMPLETED"
' <<<"$new_service_json" >/dev/null

new_task_definition="$(jq -er '.services[0].taskDefinition' <<<"$new_service_json")"
[[ "${new_task_definition##*/}" == av-scanner-v6:* ]]
new_task_json="$(aws ecs describe-task-definition \
  --region "$AWS_REGION" \
  --task-definition "$new_task_definition")"
registry="$EXPECTED_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com"
[[ "$(jq -r '.taskDefinition.containerDefinitions[] | select(.name=="app") | .image' \
  <<<"$new_task_json")" == "$registry/$APP_REPOSITORY:$IMAGE_TAG" ]]
[[ "$(jq -r '.taskDefinition.containerDefinitions[] | select(.name=="filescanner") | .image' \
  <<<"$new_task_json")" == "$registry/$CLAMAV_REPOSITORY:$IMAGE_TAG" ]]

running_tasks="$(aws ecs list-tasks \
  --region "$AWS_REGION" \
  --cluster "$ECS_CLUSTER" \
  --service-name "$NEW_SERVICE" \
  --desired-status RUNNING \
  --query taskArns \
  --output text)"
read -r -a running_task_array <<<"$running_tasks"
task_json="$(aws ecs describe-tasks \
  --region "$AWS_REGION" \
  --cluster "$ECS_CLUSTER" \
  --tasks "${running_task_array[@]}")"
jq -e --arg task_definition "$new_task_definition" \
  --argjson desired "$LEGACY_RESTORE_COUNT" '
  (.failures | length) == 0 and
  (.tasks | length) == $desired and
  all(.tasks[];
    .lastStatus == "RUNNING" and
    .healthStatus == "HEALTHY" and
    .taskDefinitionArn == $task_definition and
    all(.containers[]; .lastStatus == "RUNNING" and .healthStatus == "HEALTHY"))
' <<<"$task_json" >/dev/null

new_target_group="$(aws cloudformation describe-stacks \
  --region "$AWS_REGION" \
  --stack-name "$NEW_STACK" \
  --query "Stacks[0].Outputs[?OutputKey=='TargetGroupArn'].OutputValue | [0]" \
  --output text)"
target_health="$(aws elbv2 describe-target-health \
  --region "$AWS_REGION" \
  --target-group-arn "$new_target_group")"
jq -e --argjson desired "$LEGACY_RESTORE_COUNT" '
  (.TargetHealthDescriptions | length) == $desired and
  all(.TargetHealthDescriptions[]; .TargetHealth.State == "healthy")
' <<<"$target_health" >/dev/null

assert_service_zero "$LEGACY_SERVICE"
require_kafka_members "$LEGACY_RESTORE_COUNT"
[[ "$(aws logs filter-log-events \
  --region "$AWS_REGION" \
  --log-group-name /aws/ecs/av-scanner-v6 \
  --start-time "$rollout_start_ms" \
  --filter-pattern '"Kafka consumer started"' \
  --query 'length(events)' \
  --output text)" -ge "$LEGACY_RESTORE_COUNT" ]]
```

Observe at least one representative scan through the normal Kafka/result path,
check for stopped tasks and error logs, and verify stack drift before enabling
CircleCI. Keep the legacy service at zero and preserve its stack, task revision,
target group, and images through the rollback window.

## Routine CircleCI deployment

CircleCI authenticates using the pinned Topcoder deployment-suite helper and
reads the eight canonical parameters from `/config/av-scanner-v6/deployvar`.
The job assumes the repositories, CloudFormation stack, ECS cluster, and
dedicated service already exist; it does not perform bootstrap, ownership
transfer, or legacy-service migration.

The `develop` branch deploys dev and `master` deploys prod. Each job builds both
images with one immutable build/SHA tag, pushes them to the two configured ECR
repositories, and supplies `ImageTag` and `EnvironmentName` to the existing
CloudFormation stack. It waits for ECS stability and verifies that both active
container image references match the pushed tag. CloudFormation deploys the
complete committed `deployment/fargate-service.yml`, so reviewed infrastructure
edits in that file are also applied.

The generic `master_deploy.sh` path used by single-container v6 services is not
used here because it generates a one-container task definition and would omit
the ClamAV sidecar. The repository-specific
`deployment/deploy-existing-service.sh` performs the small CloudFormation
update while preserving the existing two-container service definition.

Before enabling the branch jobs, verify the canonical values without printing
them using the assertion in step 3. A routine deployment must never target
`file-scanning-processor-svc` or the legacy stack.

## Rollback

### Roll back an image in the dedicated service

For an ordinary release regression, keep using the new service and update
`ImageTag` to a known-good tag that exists in both repositories. Review the
change set: it may replace `ScannerTaskDefinition` and modify `ECSService` to
that task definition, but must not change desired count, networking, target
group, listener rule, log group, or deploy parameters. Wait for stability and
repeat all post-deployment image, task, target, Kafka, and log checks.

### Fall back to the legacy service

Disable CircleCI first. The order is mandatory:

1. Create and review a DesiredCount-only update setting the new stack to zero.
2. Execute it, wait for stability, and run `assert_service_zero "$NEW_SERVICE"`.
3. Require two Kafka zero-member results separated by 35 seconds.
4. Create and review a DesiredCount-only update of the legacy reconciliation
   stack to `LEGACY_RESTORE_COUNT`.
5. Immediately repeat both ECS-zero assertions and the Kafka-zero gate, then
   start the legacy service.
6. Validate the exact retained legacy task definition, healthy old target group,
   logs, and expected Kafka membership. Leave the new service at zero.

Use `desired_count_parameter_json` and the same exact-change assertions shown in
steps 4 and 5. Never start the legacy service before the new one and the Kafka
group are both proven empty. The canonical deploy parameters remain owned by the
new stack, so CircleCI stays disabled during a legacy fallback. Repointing CI or
retiring either stack is a separate reviewed ownership change.

## Production

Production repeats this two-stack sequence only after fresh discovery. Do not
copy dev account IDs, cluster/VPC/subnet/security-group IDs, listener ARN or
priority, roles, Kafka transport, owner values, or runtime parameter paths.

The current template intentionally fixes the service/task family, log-group
name, and canonical deploy-parameter path. Production must therefore use a
separate AWS account from dev. If both environments must coexist in one
account, parameterize all three names and give each environment a distinct
canonical deploy-parameter path before creating the production stack; two
stacks cannot own the same named resources.

The production target stack is `av-scanner-v6-prod`, the service/task family is
still `av-scanner-v6`, and the template derives target group
`av-scanner-v6-prod-tg`. Confirm the chosen listener priority is unused and that
the ALB has target-group and rule quota. Determine TLS/mTLS parameter ARNs from
production itself. Before enabling the `master` job, confirm that the production
CircleCI context authenticates to the reviewed account and that the canonical
deploy parameters identify the discovered production stack and cluster.

Repeat, in order: retention-policy verification, legacy topology reconciliation,
eight-parameter import, full install at zero, legacy DesiredCount-only drain,
two Kafka-zero gates, new DesiredCount-only start, validation, observation, and
only then CircleCI enablement.

## Incident history and inherited constraints

The July 2026 dev migration established a healthy two-container revision 79 on
the legacy service after resolving stale CloudFormation state, short ECR
retention, ClamAV supervisor timeout behavior, and Kafka rebalance fencing. That
deployment proved the application images, but it intentionally reused the old
service. This runbook separates the service and CloudFormation ownership while
preserving the legacy revision for rollback.

Dev still has one subnet and one desired task, no Application Auto Scaling, no
public IP, and a reused security group. Deployments use 100% minimum and 200%
maximum capacity, so rolling releases temporarily need another ENI/IP and
Fargate capacity. The runtime secrets, common parameters, IAM roles, and ECR
repositories are reused; the service, task family, target group, listener rule,
log group, and stack are dedicated to v6.
