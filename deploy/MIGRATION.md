# OIDC deployment migration

This change replaces the Auth0 credential broker and SSM-to-shell deployment path with [tc-deploy-scripts v2.0.8](https://github.com/topcoder-platform/tc-deploy-scripts/releases/tag/v2.0.8), fetched by tag and verified against commit `75bf839e1b4fa4b8dcd5ce36dad0a4d080e9a73d`. Build and deployment run in separate jobs. The deployer cannot read Parameter Store, Secrets Manager or decrypt KMS data, including through another assumed role. ECS continues resolving the existing runtime secret references using the execution role.

## Production target

The read-only inventory on 2026-11-03 verified account `409275337247`, cluster `av-scanner-service-serverless`, service `av-scanner-v6`, family `av-scanner-v6`, and container `app`. `prod.json` preserves the observed task/execution roles and names the proposed service-specific deployment role. [prod-baseline.json](https://github.com/topcoder-platform/v6-services-iac/blob/main/services/av-scanner-v6/deploy/prod-baseline.json) records references only; it contains no parameter values. Do not recreate the service or replace its complete task definition from this baseline.

## Installed infrastructure and production cutover

The dev deployment role/boundary, restricted empty CircleCI context, and immutable ECR tags are installed. Existing enhanced scanning is preserved. The production OIDC provider, deployment role/boundary and restricted context are also installed; production runtime and ECR changes remain gated on dev validation. Installation evidence and remaining service-specific steps are private in [PROVISIONING.txt](https://github.com/topcoder-platform/v6-services-iac/blob/main/services/av-scanner-v6/deploy/PROVISIONING.txt). Deploy roles have explicit secret-store and KMS-decryption denies. Runtime values are retrieved only by ECS using the execution role.

At production cutover, stop legacy publishers, enable unconditional tag immutability, adopt missing task health checks with the reviewed image, and use the approval-gated `master` workflow. Preserve capacity, roles, secret references and rollback. Do not run production before dev validation.

## Development acceptance

This PR targets `develop`. Administrator-installed dev prerequisites are recorded in the private IaC repository. The dev digest deployment contract is installed; eight deployment pointers were retained and detached, and the running image digests/configuration were preserved.

- Dev infrastructure prerequisites are installed; merge to run the application deployment, then validate functionality.

Run the shared offline suite, validate the CircleCI config and IAM policies, and test a dev release plus an unhealthy-image rollback. Verify CloudTrail contains no SSM/Secrets Manager calls from the deployment principal. Remove its old broker grants only after migration. Check the runtime secret references and roles stayed unchanged. Production requires the manual approval job and a passing digest/scan/health verification.

The pinned executor is `cimg/aws@sha256:4cf3768db00ddf9bc439b8c84e8a60c11a14edf81c8baf76f74132144f8340d5`. Python/AWS CLI/Buildx/Git compatibility was tested with the shared offline suite. No package installation or service build runs in the deployment job. The Dockerfile retains the existing in-image quality gates. This PR does not deploy, change IAM, rotate credentials, or claim a successful live rollout.

## CloudFormation ownership and administrator adoption

CI never installs `deployment/fargate-service.yml`. An administrator must first update the existing stack to this reviewed template, preserving all current parameters and images. The eight old deployment-pointer resources already have `DeletionPolicy: Retain`; removing them from the template detaches them without deleting their values. Verify they are no longer stack resources. Retain the legacy scanner at zero desired capacity to avoid duplicate Kafka processing.

The updated template accepts an app and ClamAV digest and exposes `DeploymentContract=oidc-digest-v1`. Install the narrowly scoped CloudFormation execution role from the IAM template; both it and CI explicitly deny SSM, Secrets Manager, KMS decryption and role chaining. CI can pass only this role to CloudFormation. It may update only the named existing stack, using its previous template, two scanned digest parameters, and `UsePreviousValue` for everything else. It cannot create stacks, change networking or provision parameters. `prod-stack.json` records the verified stack name and proposed role; `dev-stack.json` now records the verified dev stack; administrator adoption is still required.

The adapter validates stack ownership, absence of SSM stack resources, stable ECS state, task roles and runtime configuration before publishing both images. It requires successful CloudFormation completion, unchanged non-image task fields and healthy running tasks with both exact digests. The default ECS update permissions are removed from the CI role. The CloudFormation role can update only this service/task family and pass only its existing task/execution roles.

ClamAV now starts both clamd and freshclam as the existing `clamav` user with pre-owned writable database/socket paths; either daemon exiting terminates the essential container. Validate signature updates, the 500 MB scan limit, malware detection, daemon failure and rolling rollback in dev. No automatic activation of the legacy service is added.

## Infrastructure source

IAM templates and detailed AWS baselines are maintained in the private [v6-services-iac service directory](https://github.com/topcoder-platform/v6-services-iac/tree/main/services/av-scanner-v6). Provision infrastructure separately with an administrator identity; application CI does not fetch that repository or install IAM. Required application manifests and health-check proposals remain here.

## Dev branch refresh

This PR targets `develop` and preserves its current application/dependency fixes. The shared suite is v2.0.8 (100 offline tests); service-specific adapters and their tests remain part of review. Use the current readiness record above for the remaining merge/deployment gates. Updating these files does not provision AWS or CircleCI resources.

Source checkout uses the public HTTPS repository and verifies the exact CircleCI commit, avoiding dependency on failing SSH checkout keys. No GitHub token is added to build or deployment contexts.
