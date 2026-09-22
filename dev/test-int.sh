#!/usr/bin/env bash
# The container-gated tests. They need the REST catalog fixture, so this
# refuses to run rather than reporting a pass it did not earn: a gated test
# that cannot run must show as skipped or fail, never as green.
#
#   dev/rest-fixture.sh up && dev/test-int.sh
set -euo pipefail

. "$(dirname "$0")/lib.sh"

: "${ICETRAY_REST_URI:=$REST_CATALOG_URI}"
export ICETRAY_REST_URI

# How long the probe waits for the catalog's config endpoint: a running
# fixture answers on loopback in milliseconds, so a longer wait would only
# delay the refusal when nothing is listening.
PROBE_TIMEOUT_SECONDS=5

if ! curl -fsS --max-time "$PROBE_TIMEOUT_SECONDS" "$ICETRAY_REST_URI/v1/config" >/dev/null 2>&1; then
  echo "no REST catalog at $ICETRAY_REST_URI: run dev/rest-fixture.sh up" >&2
  exit 1
fi

cargo test --workspace --locked --test rest_catalog -- --ignored
