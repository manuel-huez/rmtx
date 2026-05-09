#!/usr/bin/env bash

set -euo pipefail

# AGENT NOTE: This script is intentionally authoritative and broad. Never add
# partial modes, skip checks, or reduce its reporting scope to make local runs
# easier or faster. Any branching in this file is explicit repo-level policy,
# not a precedent for adding more fast paths. If you need a narrower local
# check, run the underlying tools directly instead of changing this file.

status=0
step_pids=()
step_logs=()
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/stx-code-health.XXXXXX")"

cleanup() {
  rm -rf "$tmp_dir"
}

terminate() {
  kill "${step_pids[@]}" 2>/dev/null || true
  cleanup
  exit 130
}

trap cleanup EXIT
trap terminate INT TERM

launch_step() {
  local label="$1"
  shift

  local step_index="${#step_pids[@]}"
  local log_path="$tmp_dir/step-$step_index.log"

  # Checks are independent read-only validators; run together to reduce hook wall time.
  (
    echo "==> $label"

    local start_seconds="$SECONDS"
    local outcome="completed"
    local step_status=0
    if ! "$@"; then
      outcome="failed"
      step_status=1
    fi

    local elapsed_seconds=$((SECONDS - start_seconds))
    printf '==> %s %s in %ss\n' "$label" "$outcome" "$elapsed_seconds"
    exit "$step_status"
  ) >"$log_path" 2>&1 &

  step_pids+=("$!")
  step_logs+=("$log_path")
}

wait_for_steps() {
  local step_index
  for step_index in "${!step_pids[@]}"; do
    if [ "$step_index" -gt 0 ]; then
      echo
    fi

    if ! wait "${step_pids[$step_index]}"; then
      status=1
    fi

    cat "${step_logs[$step_index]}"
  done
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

launch_step "go vet" go vet ./...
launch_step "go test" go test ./...
launch_step "golangci-lint" run_golangci_lint
launch_step "slopelint" slopelint ./...
launch_step "jscpd" run_jscpd

wait_for_steps

exit "$status"
