#!/usr/bin/env bash
# The container-gated tests. They need the REST catalog fixture, so this
# refuses to run rather than reporting a pass it did not earn: a gated test
# that cannot run must show as skipped or fail, never as green.
#
#   dev/rest-fixture.sh up && dev/test-int.sh
set -euo pipefail

: "${ICETRAY_REST_URI:=http://127.0.0.1:8181}"
export ICETRAY_REST_URI

if ! curl -fsS --max-time 5 "$ICETRAY_REST_URI/v1/config" >/dev/null 2>&1; then
  echo "no REST catalog at $ICETRAY_REST_URI: run dev/rest-fixture.sh up" >&2
  exit 1
fi

cargo test --workspace --locked --test rest_catalog -- --ignored
