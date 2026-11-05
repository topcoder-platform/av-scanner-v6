"""Exercise deployment ordering without contacting AWS or Docker."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("deploy-existing-service.sh")
FAKE_TOOL = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

tool = Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ["CALL_LOG"], "a") as log:
    log.write(json.dumps([tool, *args]) + "\n")
if tool == "docker":
    if args[0] == "login":
        sys.stdin.read()
    sys.exit(0)
operation = args[:2]
if operation == ["sts", "get-caller-identity"]:
    print("811668436784")
elif operation == ["ssm", "get-parameters-by-path"]:
    values = {
        "AWS_CLOUDFORMATION_STACK_NAME": "av-scanner-v6-dev",
        "AWS_ECS_CLUSTER": "av-scanner-service-serverless",
        "AWS_ECS_SERVICE": "av-scanner-v6",
        "AWS_ECS_TASK_FAMILY": "av-scanner-v6",
        "AWS_ECS_APP_CONTAINER_NAME": "app",
        "AWS_ECS_CLAMAV_CONTAINER_NAME": "filescanner",
        "AWS_REPOSITORY": "file-scanner-processor",
        "AWS_REPOSITORY_CLAMAV": "clamav-service-processor",
    }
    print(json.dumps({"Parameters": [
        {"Name": "/config/av-scanner-v6/deployvar/" + key, "Value": value}
        for key, value in values.items()
    ]}))
elif operation == ["cloudformation", "describe-stacks"]:
    if os.environ.get("DESCRIBE_FAILURE"):
        print("AccessDenied", file=sys.stderr)
        sys.exit(254)
    print(os.environ["STACK_STATUS"])
elif operation == ["cloudformation", "deploy"]:
    pass
elif operation == ["ecr", "get-login-password"]:
    print("fake-password")
elif operation == ["ecs", "wait"]:
    pass
elif operation == ["ecs", "describe-services"]:
    print("av-scanner-v6:6")
elif operation == ["ecs", "describe-task-definition"]:
    query = args[args.index("--query") + 1]
    if query == "taskDefinition.family":
        print("av-scanner-v6")
    else:
        repo = "file-scanner-processor" if "=='app'" in query else "clamav-service-processor"
        print("811668436784.dkr.ecr.us-east-1.amazonaws.com/" + repo + ":v6-32-a53c63bc5fde")
else:
    raise SystemExit("Unexpected AWS call: " + repr(args))
'''


class DeploymentPreflightTests(unittest.TestCase):
    def run_deployment(self, status, describe_failure=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("aws", "docker"):
                tool = root / name
                tool.write_text(FAKE_TOOL)
                tool.chmod(0o755)
            log = root / "calls.jsonl"
            env = {
                **os.environ,
                "PATH": directory + os.pathsep + os.environ["PATH"],
                "CALL_LOG": str(log),
                "STACK_STATUS": status,
                "DESCRIBE_FAILURE": "1" if describe_failure else "",
                "APPNAME": "av-scanner-v6",
                "DEPLOY_ENV": "DEV",
                "DEPLOYMENT_ENVIRONMENT": "dev",
                "AWS_ACCOUNT_ID": "811668436784",
                "AWS_ENVIRONMENT": "DEV",
                "AWS_REGION": "us-east-1",
                "CIRCLE_BUILD_NUM": "32",
                "CIRCLE_SHA1": "a53c63bc5fde012345678901234567890123456789",
            }
            result = subprocess.run(
                ["bash", str(SCRIPT)], env=env, capture_output=True, text=True
            )
            calls = [json.loads(line) for line in log.read_text().splitlines()]
            return result, calls

    def test_ready_stacks_check_status_before_publishing_and_deploying(self):
        for status in (
            "CREATE_COMPLETE", "UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE",
            "IMPORT_COMPLETE", "IMPORT_ROLLBACK_COMPLETE",
        ):
            with self.subTest(status=status):
                result, calls = self.run_deployment(status)
                self.assertEqual(result.returncode, 0, result.stderr)
                operations = [call[:3] for call in calls]
                check = operations.index(["aws", "cloudformation", "describe-stacks"])
                publish = operations.index(["aws", "ecr", "get-login-password"])
                deploy = operations.index(["aws", "cloudformation", "deploy"])
                self.assertLess(check, publish)
                self.assertLess(publish, deploy)
                self.assertEqual(sum(call[:2] == ["docker", "push"] for call in calls), 2)

    def test_blocked_stacks_exit_before_any_mutation(self):
        for status in (
            "UPDATE_ROLLBACK_FAILED", "UPDATE_IN_PROGRESS",
            "UPDATE_ROLLBACK_IN_PROGRESS", "ROLLBACK_COMPLETE",
            "DELETE_COMPLETE", "None", "",
        ):
            with self.subTest(status=status):
                result, calls = self.run_deployment(status)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("deployment stopped before publishing images", result.stderr)
                self.assertIn("ECS_Deployment.md", result.stderr)
                self.assertEqual([call[:3] for call in calls], [
                    ["aws", "sts", "get-caller-identity"],
                    ["aws", "ssm", "get-parameters-by-path"],
                    ["aws", "cloudformation", "describe-stacks"],
                ])

    def test_failed_stack_lookup_does_not_publish_or_deploy(self):
        result, calls = self.run_deployment("UPDATE_COMPLETE", describe_failure=True)
        self.assertEqual(result.returncode, 254)
        self.assertIn("AccessDenied", result.stderr)
        self.assertEqual(len(calls), 3)


if __name__ == "__main__":
    unittest.main()
