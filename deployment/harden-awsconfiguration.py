#!/usr/bin/env python3

"""Remove insecure TLS and shell evaluation from the pinned AWS auth helper."""

import sys
from pathlib import Path


if len(sys.argv) != 2:
    raise SystemExit(f"Usage: {sys.argv[0]} AWS_CONFIGURATION_SCRIPT")

helper = Path(sys.argv[1])
lines = helper.read_text().splitlines()
auth_lines = [
    index for index, line in enumerate(lines) if line.startswith("auth0cmd=$(echo ")
]
token_lines = [
    index
    for index, line in enumerate(lines)
    if line == "token=$( eval $auth0cmd | jq -r .access_token )"
]
if len(auth_lines) != 1 or token_lines != [auth_lines[0] + 1]:
    raise SystemExit("Pinned authentication helper no longer matches the reviewed form")

replacement = r'''if [[ "$CI_AUTH0_URL" != https://* ]]; then
  echo "CI_AUTH0_URL must use HTTPS." >&2
  exit 1
fi
auth0_payload="$(jq -cn \
  --arg client_id "$CI_AUTH0_CLIENTID" \
  --arg client_secret "$CI_AUTH0_CLIENTSECRET" \
  --arg audience "$CI_AUTH0_AUDIENCE" \
  --arg environment "$AWSENV" \
  --arg username "$CIRCLE_PROJECT_USERNAME" \
  --arg reponame "$CIRCLE_PROJECT_REPONAME" \
  --arg build_num "$CIRCLE_BUILD_NUM" \
  --arg branch "$CIRCLE_BRANCH" \
  '{client_id:$client_id,client_secret:$client_secret,audience:$audience,grant_type:"client_credentials",environment:$environment,username:$username,reponame:$reponame,build_num:$build_num,branch:$branch}')"
token="$(
  curl --fail --silent --show-error \
    -X POST "$CI_AUTH0_URL" \
    -H 'Content-Type: application/json' \
    --data "$auth0_payload" |
    jq -er '.access_token | select(type == "string" and length > 0)'
)"'''.splitlines()

lines[auth_lines[0] : token_lines[0] + 1] = replacement
if "set -euo pipefail" not in lines:
    lines.insert(1, "set -euo pipefail")

patched = "\n".join(lines) + "\n"
if any(fragment in patched for fragment in ("curl -k", "--insecure", " eval ")):
    raise SystemExit("Authentication helper still contains an insecure command")
helper.write_text(patched)
