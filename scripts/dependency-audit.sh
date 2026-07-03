#!/usr/bin/env bash
# Dependency-audit gate for anvilkit-export-worker (EW-REPO-004; AC-002,
# AC-010, AC-018). The worker is a pure-Go service: no frontend packages, no
# cross-repo imports from anvilkit-studio, no Rust, and no Node/TS at all
# (Node/TS is confined to the platform repo's tooling/mocks/contracts areas).
set -euo pipefail
cd "$(dirname "$0")/.."

fail=0

echo "== 1/3 No Node/TS/JS sources or manifests in the worker repo =="
node_files=$(find . -path ./.git -prune -o -type f \
  \( -name 'package.json' -o -name 'bun.lock*' -o -name '*.ts' -o -name '*.tsx' -o -name '*.js' -o -name '*.jsx' \) \
  -print)
if [ -n "$node_files" ]; then
  echo "FAIL: Node/TS/JS files found:"
  echo "$node_files"
  fail=1
else
  echo "ok"
fi

echo "== 2/3 No Rust (AC-018) =="
rust_files=$(find . -path ./.git -prune -o -type f \( -name 'Cargo.toml' -o -name '*.rs' \) -print)
if [ -n "$rust_files" ]; then
  echo "FAIL: Rust files found:"
  echo "$rust_files"
  fail=1
else
  echo "ok"
fi

echo "== 3/3 Go module graph: no frontend or cross-repo dependencies (AC-002, AC-010) =="
forbidden=$(go list -m all | grep -Ei 'anvilkit-studio|anvilkit-render-origin|/react|react-dom|nextjs|next\.js|puck' || true)
if [ -n "$forbidden" ]; then
  echo "FAIL: forbidden modules in the dependency graph:"
  echo "$forbidden"
  fail=1
else
  echo "ok"
fi

if [ "$fail" -ne 0 ]; then
  echo "dependency audit FAILED"
  exit 1
fi
echo "dependency audit passed"
