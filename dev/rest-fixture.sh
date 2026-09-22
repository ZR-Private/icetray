#!/usr/bin/env bash
# The Iceberg REST catalog fixture: the upstream reference server, in a
# container, over a warehouse directory under target/. It backs the one test
# that needs a real catalog rather than an in-process one.
#
#   dev/rest-fixture.sh up     start it and print its URI
#   dev/rest-fixture.sh down   remove the container
#
# The warehouse is bind-mounted at the same absolute path inside the container
# as outside, so a `file://` location in the catalog's metadata resolves on
# both sides.
set -euo pipefail

. "$(dirname "$0")/lib.sh"

NAME=icetray-rest
IMAGE=apache/iceberg-rest-fixture:latest

case "${1:-}" in
up)
  dev/runtime.sh status >/dev/null 2>&1 || { echo "no container runtime: run dev/runtime.sh up" >&2; exit 1; }
  warehouse="$PWD/target/rest-warehouse"
  mkdir -p "$warehouse"
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  # --user root because the image's own user cannot write into a bind mount
  # from a macOS host.
  docker run -d --name "$NAME" --user root \
    -p "127.0.0.1:$REST_CATALOG_PORT:$REST_CATALOG_PORT" \
    -v "$warehouse:$warehouse" \
    -e "CATALOG_WAREHOUSE=file://$warehouse" \
    "$IMAGE" >/dev/null
  echo "REST catalog at $REST_CATALOG_URI (warehouse $warehouse)"
  echo "run the test with: ICETRAY_REST_URI=$REST_CATALOG_URI dev/test-int.sh"
  ;;
down)
  docker rm -f "$NAME" >/dev/null 2>&1 && echo "removed $NAME" || echo "$NAME was not running"
  ;;
*) sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
