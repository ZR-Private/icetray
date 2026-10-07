#!/usr/bin/env bash
# The merge-on-read measurements (crates/icetray-testkit/examples/deletes.rs)
# that RFD 021 makes before release: lookup latency by files per bucket with
# deleted rows first in the probe order and without, the deleted-row check
# timed against a key-page decode, delete-state memory against its bound by
# deleted fraction, and load and refresh time by live delete files. It
# builds the program in release, runs it with its tables under WORK_DIR,
# removed when it exits, and keeps the printed tables beside the JSON under
# OUT_DIR. Arguments after the script's name go to the program, so
# `--measure probe --tier full` narrows it.
#
# The defaults finish in minutes on four cores and write a few hundred MB under
# WORK_DIR while they run. For numbers to publish, run on the Linux host
# the release targets, with WORK_DIR on its local disk.
set -euo pipefail

cd "$(dirname "$0")/.."

WORK_DIR="${ICETRAY_MOR_WORK:-target/merge-on-read/work}"
OUT_DIR="target/merge-on-read/$(date -u +%Y%m%dT%H%M%SZ)"

log() { echo "==> $*" >&2; }

log "cargo build --release (deletes)"
cargo build --release -p icetray-testkit --example deletes

log "measuring, tables under $WORK_DIR"
mkdir -p "$OUT_DIR"
target/release/examples/deletes --work-dir "$WORK_DIR" --out-dir "$OUT_DIR" "$@" \
  | tee "$OUT_DIR/summary.txt"

echo "results: $OUT_DIR"
