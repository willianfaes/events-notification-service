#!/bin/bash
# Copy of deploy/ssm-run.sh in willianfaes/events-manager-api, which owns the host: change both together.
# Runs a shell command as root on the host tagged Name=events-manager, through SSM, and prints what it wrote.
#   deploy/ssm-run.sh "<command>"
# The host is found by tag because its instance id changes on every terraform apply. Exits non-zero unless the
# command succeeds. The command cannot contain double quotes or backslashes (it goes into a JSON string).
set -euo pipefail

REGION="${AWS_REGION:-eu-north-1}"
HOST_TAG="${HOST_TAG:-events-manager}"
COMMAND="${1:?usage: ssm-run.sh <command>}"

COMMAND_ID=$(aws ssm send-command --region "$REGION" \
  --targets "Key=tag:Name,Values=${HOST_TAG}" \
  --document-name AWS-RunShellScript \
  --parameters "{\"commands\":[\"${COMMAND}\"]}" \
  --query Command.CommandId --output text)

# Up to 10 minutes. The status stays None until a host with the tag is found.
STATUS=None
for _ in $(seq 1 120); do
  STATUS=$(aws ssm list-command-invocations --region "$REGION" --command-id "$COMMAND_ID" \
    --query 'CommandInvocations[0].Status' --output text)
  case "$STATUS" in
    None | Pending | InProgress | Delayed) sleep 5 ;;
    *) break ;;
  esac
done

INSTANCE_ID=$(aws ssm list-command-invocations --region "$REGION" --command-id "$COMMAND_ID" \
  --query 'CommandInvocations[0].InstanceId' --output text)
if [ "$INSTANCE_ID" = "None" ]; then
  echo "no running host tagged Name=${HOST_TAG}: is the stack up?" >&2
  exit 1
fi

aws ssm get-command-invocation --region "$REGION" --command-id "$COMMAND_ID" --instance-id "$INSTANCE_ID" \
  --query '[StandardOutputContent,StandardErrorContent]' --output text

if [ "$STATUS" != "Success" ]; then
  echo "command ended with status ${STATUS}" >&2
  exit 1
fi
