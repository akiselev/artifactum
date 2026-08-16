#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

python3 scripts/static_validate.py
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace --all-targets
# Keep the first compile pass useful even if pedantic lints need cleanup; the
# correctness/suspicious classes are non-negotiable.
cargo clippy --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious

echo "validation complete"
