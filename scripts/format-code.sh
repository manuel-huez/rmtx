#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$repo_root"

declare -a format_goos=(
  "$(go env GOOS)"
  "linux"
  "darwin"
  "windows"
)
seen_goos=" "

for goos in "${format_goos[@]}"; do
  if [[ "$seen_goos" == *" $goos "* ]]; then
    continue
  fi
  seen_goos+="$goos "
  GOOS="$goos" golangci-lint run --fix ./...
done

golangci-lint fmt
