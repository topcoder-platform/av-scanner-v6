#!/usr/bin/env bash

set -euo pipefail

: "${APPNAME:?Missing application name}"
: "${DEPLOY_ENV:?Missing deployment environment selector}"
: "${DEPLOYMENT_ENVIRONMENT:?Missing deployment environment}"
: "${AWS_ACCOUNT_ID:?Missing AWS account ID}"
: "${AWS_ENVIRONMENT:?Missing authenticated AWS environment}"
: "${AWS_REGION:?Missing AWS region}"
: "${CIRCLE_BUILD_NUM:?Missing CircleCI build number}"
: "${CIRCLE_SHA1:?Missing CircleCI commit SHA}"

authenticated_account="$(aws sts get-caller-identity \
  --region "$AWS_REGION" \
  --query Account \
  --output text)"
if [[ "$authenticated_account" != "$AWS_ACCOUNT_ID" ||
  "${AWS_ENVIRONMENT^^}" != "$DEPLOY_ENV" ]]; then
  echo "AWS credentials do not match the requested deployment environment." >&2
  exit 1
fi

deploy_parameter_path="/config/${APPNAME}/deployvar"
deploy_parameters="$(aws ssm get-parameters-by-path \
  --region "$AWS_REGION" \
  --path "$deploy_parameter_path" \
  --with-decryption \
  --recursive \
  --output json)"

read_deploy_parameter() {
  local name="$1"
  jq -er \
    --arg full_name "${deploy_parameter_path}/${name}" '
      [.Parameters[] | select(.Name == $full_name) | .Value] |
      if length == 1 and .[0] != "" then .[0]
      else error("missing or duplicate deployment parameter") end
    ' <<<"$deploy_parameters"
}

AWS_CLOUDFORMATION_STACK_NAME="$(read_deploy_parameter AWS_CLOUDFORMATION_STACK_NAME)"
AWS_ECS_CLUSTER="$(read_deploy_parameter AWS_ECS_CLUSTER)"
AWS_ECS_SERVICE="$(read_deploy_parameter AWS_ECS_SERVICE)"
AWS_ECS_TASK_FAMILY="$(read_deploy_parameter AWS_ECS_TASK_FAMILY)"
AWS_ECS_APP_CONTAINER_NAME="$(read_deploy_parameter AWS_ECS_APP_CONTAINER_NAME)"
AWS_ECS_CLAMAV_CONTAINER_NAME="$(read_deploy_parameter AWS_ECS_CLAMAV_CONTAINER_NAME)"
AWS_REPOSITORY="$(read_deploy_parameter AWS_REPOSITORY)"
AWS_REPOSITORY_CLAMAV="$(read_deploy_parameter AWS_REPOSITORY_CLAMAV)"

image_tag="v6-${CIRCLE_BUILD_NUM}-${CIRCLE_SHA1:0:12}"
registry="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
app_image="${registry}/${AWS_REPOSITORY}:${image_tag}"
clamav_image="${registry}/${AWS_REPOSITORY_CLAMAV}:${image_tag}"

aws ecr get-login-password --region "$AWS_REGION" |
  docker login --username AWS --password-stdin "$registry"
docker tag "${APPNAME}:latest" "$app_image"
docker tag "${APPNAME}-clamav:latest" "$clamav_image"
docker push "$app_image"
docker push "$clamav_image"

aws cloudformation deploy \
  --region "$AWS_REGION" \
  --stack-name "$AWS_CLOUDFORMATION_STACK_NAME" \
  --template-file deployment/fargate-service.yml \
  --parameter-overrides \
  ImageTag="$image_tag" \
  EnvironmentName="$DEPLOYMENT_ENVIRONMENT" \
  --no-fail-on-empty-changeset

aws ecs wait services-stable \
  --region "$AWS_REGION" \
  --cluster "$AWS_ECS_CLUSTER" \
  --services "$AWS_ECS_SERVICE"

active_task_definition="$(aws ecs describe-services \
  --region "$AWS_REGION" \
  --cluster "$AWS_ECS_CLUSTER" \
  --services "$AWS_ECS_SERVICE" \
  --query 'services[0].taskDefinition' \
  --output text)"
active_app_image="$(aws ecs describe-task-definition \
  --region "$AWS_REGION" \
  --task-definition "$active_task_definition" \
  --query "taskDefinition.containerDefinitions[?name=='${AWS_ECS_APP_CONTAINER_NAME}'].image | [0]" \
  --output text)"
active_clamav_image="$(aws ecs describe-task-definition \
  --region "$AWS_REGION" \
  --task-definition "$active_task_definition" \
  --query "taskDefinition.containerDefinitions[?name=='${AWS_ECS_CLAMAV_CONTAINER_NAME}'].image | [0]" \
  --output text)"
active_task_family="$(aws ecs describe-task-definition \
  --region "$AWS_REGION" \
  --task-definition "$active_task_definition" \
  --query 'taskDefinition.family' \
  --output text)"

if [[ "$active_task_family" != "$AWS_ECS_TASK_FAMILY" ||
  "$active_app_image" != "$app_image" ||
  "$active_clamav_image" != "$clamav_image" ]]; then
  echo "ECS stabilized on an unexpected task definition or image." >&2
  exit 1
fi
