#!/usr/bin/env sh
set -eu
python3 scripts/static_validate.py
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
