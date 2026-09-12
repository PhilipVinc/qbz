#!/usr/bin/env bash
# Run the qbzd workspace unit/doc tests (crates workspace).
#
# Same command CI uses (.github/workflows/test-crates.yml).
#
# Usage:
#   ./scripts/cargo-test.sh
#   ./scripts/cargo-test.sh -- --lib          # skip doctests
#   CARGO_BUILD_JOBS=1 ./scripts/cargo-test.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"

exec cargo test \
  --manifest-path crates/Cargo.toml \
  --workspace \
  --no-fail-fast \
  "$@"
