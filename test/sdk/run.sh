#!/usr/bin/env sh
set -eu

GOPROXY="${GOPROXY:-off}" go test -tags=integration ./internal/integration -run TestSDK -v
