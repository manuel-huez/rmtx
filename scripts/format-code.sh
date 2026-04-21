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

# Native darwin formatter runs can still miss one Linux-only whitespace rewrite
# that CI applies in signal-driven TTY files. Normalize that pattern locally so
# `./scripts/format-code.sh` and the Linux formatter gate converge.
while IFS= read -r file; do
  perl -0pi -e \
    's/(\n[ \t]*signal\.Notify\([^\n]+\)\n)(\n*)([ \t]*[[:alnum:]_.]+\s*=\s*func\(\)\s*\{)/$1\n$3/g' \
    "$file"
done < <(rg --files cmd internal -g '*.go')
