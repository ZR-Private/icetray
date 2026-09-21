#!/usr/bin/env bash
# Writes the golden tables with Spark in a container, so the recipe runs
# without Spark or a Java runtime on the host. The warehouse directory is
# bind-mounted at the same absolute path inside the container as outside, so
# the `file://` locations the Hadoop catalog records resolve on both sides,
# for the tests as much as for the writer.
#
#   dev/golden-tables.sh                 write both groups under target/golden-warehouse
#   dev/golden-tables.sh --only contract write one group; every flag goes to the recipe
#
# Then: ICETRAY_GOLDEN_WAREHOUSE=file://$PWD/target/golden-warehouse dev/test-golden.sh
set -euo pipefail

# The Spark the recipe was written for (3.5) with Python, on a multi-arch
# image, so the same run works on an arm64 laptop and an amd64 runner.
IMAGE=apache/spark:3.5.9-python3
# The driver runs everything in local mode; the wide tables hold gigabytes
# of incompressible values, and less than this spills or fails the job.
DRIVER_MEMORY=4g
# Where the Maven download of the Iceberg runtime lands inside the
# container: a mounted directory, so a second run downloads nothing.
IVY_DIR="$PWD/target/golden-ivy"

dev/runtime.sh status >/dev/null 2>&1 || { echo "no container runtime: run dev/runtime.sh up" >&2; exit 1; }
warehouse="$PWD/target/golden-warehouse"
mkdir -p "$warehouse" "$IVY_DIR"

# --user root because the image's own user cannot write into a bind mount
# from a macOS host. The recipe's own --packages is emptied: spark-submit
# adds the runtime once, into the mounted ivy directory.
docker run --rm --user root \
  -v "$PWD:$PWD" \
  -w "$PWD" \
  -e HOME=/tmp \
  "$IMAGE" \
  /opt/spark/bin/spark-submit \
  --driver-memory "$DRIVER_MEMORY" \
  --conf "spark.jars.ivy=$IVY_DIR" \
  --packages "$(python3 -c 'import re,sys; print(re.search(r"ICEBERG_PACKAGE = \"([^\"]+)\"", open("dev/golden-tables.py").read()).group(1))')" \
  dev/golden-tables.py --warehouse "file://$warehouse" --packages "" "$@"

echo
echo "run the tests with: ICETRAY_GOLDEN_WAREHOUSE=file://$warehouse dev/test-golden.sh"
