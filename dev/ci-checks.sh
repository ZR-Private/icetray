#!/usr/bin/env bash
# The lint gate. One script, three callers: CI's lint job, the pre-push hook,
# and a developer. Change it here and every caller changes with it.
# Tests are not here on purpose: this stays fast enough to run on every push.
set -euo pipefail

log() { echo "==> $*"; }

log "cargo fmt --check"
cargo fmt --all --check

log "cargo clippy --workspace --all-targets -- -D warnings"
cargo clippy --workspace --all-targets --locked -- -D warnings
