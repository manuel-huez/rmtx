#!/usr/bin/env bash

set -euo pipefail

# AGENT NOTE: This script is intentionally authoritative and broad. Never add
# partial modes, skip checks, or reduce its reporting scope to make local runs
# easier or faster. Any branching in this file is explicit repo-level policy,
# not a precedent for adding more fast paths. If you need a narrower local
# check, run the underlying tools directly instead of changing this file.

status=0
step_index=0

timestamp_ms() {
  python3 -c 'import time; print(time.time_ns() // 1_000_000)'
}

format_elapsed() {
  local elapsed_ms="$1"
  if [ "$elapsed_ms" -lt 1000 ]; then
    printf '%sms' "$elapsed_ms"
    return
  fi

  printf '%ss' "$((elapsed_ms / 1000))"
}

run_step() {
  local label="$1"
  shift

  if [ "$step_index" -gt 0 ]; then
    echo
  fi
  step_index=$((step_index + 1))

  echo "==> $label"

  local start_ms
  start_ms="$(timestamp_ms)"
  local outcome="completed"
  if ! "$@"; then
    status=1
    outcome="failed"
  fi

  local end_ms elapsed_ms
  end_ms="$(timestamp_ms)"
  elapsed_ms=$((end_ms - start_ms))
  if [ "$elapsed_ms" -lt 0 ]; then
    elapsed_ms=0
  fi

  printf '==> %s %s in %s\n' "$label" "$outcome" "$(format_elapsed "$elapsed_ms")"
}

run_golangci_lint() {
  golangci-lint run ./...
}

run_go_deadcode() {
  deadcode -test ./cmd/rmtx ./internal/...
}

run_jscpd() {
  # Enforce zero production-code clones while ignoring intentionally repetitive
  # test scaffolding and harness setup.
  npx --yes jscpd cmd/ internal/ \
    --ignore "**/*_test.go" \
    --threshold 0
}

run_step "go vet" go vet ./...
run_step "go test" go test ./...
run_step "golangci-lint" run_golangci_lint
run_step "go deadcode" run_go_deadcode
run_step "jscpd" run_jscpd

exit "$status"
