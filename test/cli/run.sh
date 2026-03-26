#!/usr/bin/env sh
set -eu

if ! command -v aws >/dev/null 2>&1; then
  echo "aws CLI is required for test/cli/run.sh" >&2
  exit 1
fi

GOPROXY="${GOPROXY:-off}" go test -tags=integration ./internal/integration -run TestCLISuite -v
