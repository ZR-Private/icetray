#!/usr/bin/env bash
# The residency sweep (crates/icetray-testkit/examples/residency.rs): how a
# disk table's lookup latency tracks the share of its chunk files the
# kernel's page cache holds, the measurement that sets the page-cache
# condition's thresholds (RFD 020). It prepares the benchmark program's
# scattered table once under TABLES_DIR, reusing it after, then runs the
# sweep over it with its chunk files under LOCAL_DIR. Arguments after the
# script's name go to the sweep, so `--resident-per-mille 1000,950,900`
# narrows it. Results are JSON under OUT_DIR; the script prints where.
#
# Linux only: the sweep drops pages with posix_fadvise. Keep LOCAL_DIR and
# TABLES_DIR on a local ext4 or xfs filesystem. Under a virtual machine
# such as colima that is the machine's own disk, not a mount of the
# host's files, whose reads the host's page cache can serve.
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "unsupported: the residency sweep drops pages with posix_fadvise, which only Linux honors" >&2
  exit 1
fi

TABLES_DIR="${ICETRAY_RESIDENCY_TABLES:-target/bench/tables}"
LOCAL_DIR="${ICETRAY_RESIDENCY_LOCAL:-target/residency/local}"
OUT_DIR="target/residency/$(date -u +%Y%m%dT%H%M%SZ)"

log() { echo "==> $*" >&2; }

log "cargo build --release (bench, residency)"
cargo build --release -p icetray-testkit --example bench --example residency
BENCH="target/release/examples/bench"
SWEEP="target/release/examples/residency"

log "the scattered table under $TABLES_DIR"
mkdir -p "$OUT_DIR"
"$BENCH" --prepare --table scattered --data-dir "$TABLES_DIR" >"$OUT_DIR/tables.tsv"
# bench --prepare prints name, key type, rows, and the first build's
# metadata file, one tab-separated line per table it prepared.
if ! IFS=$'\t' read -r _name _key_type ROWS LOCATION \
  < <(awk -F'\t' '$1 == "scattered"' "$OUT_DIR/tables.tsv"); then
  echo "bench --prepare printed no scattered table; see $OUT_DIR/tables.tsv" >&2
  exit 1
fi

log "sweeping, chunk files under $LOCAL_DIR"
"$SWEEP" --location "$LOCATION" --rows "$ROWS" --local-dir "$LOCAL_DIR" \
  --out-dir "$OUT_DIR" "$@" | tee "$OUT_DIR/sweep.txt"

echo "results: $OUT_DIR"
