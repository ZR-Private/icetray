#!/usr/bin/env bash
# The golden-table tests. They read the tables dev/golden-tables.py wrote, so
# this refuses to run rather than reporting a pass it did not earn: a gated
# test that cannot run must show as skipped or fail, never as green.
#
#   python3 dev/golden-tables.py --warehouse file:///tmp/icetray-golden
#   ICETRAY_GOLDEN_WAREHOUSE=file:///tmp/icetray-golden dev/test-golden.sh
set -euo pipefail

if [ -z "${ICETRAY_GOLDEN_WAREHOUSE:-}" ]; then
  echo "ICETRAY_GOLDEN_WAREHOUSE is not set: run dev/golden-tables.py and export the warehouse it wrote" >&2
  exit 1
fi

case "$ICETRAY_GOLDEN_WAREHOUSE" in
file://*)
  warehouse="${ICETRAY_GOLDEN_WAREHOUSE#file://}"
  if [ ! -d "$warehouse/db" ]; then
    echo "no golden tables under $ICETRAY_GOLDEN_WAREHOUSE/db: run dev/golden-tables.py --warehouse $ICETRAY_GOLDEN_WAREHOUSE" >&2
    exit 1
  fi
  ;;
esac
export ICETRAY_GOLDEN_WAREHOUSE

cargo test --workspace --locked --test golden -- --ignored
