#!/usr/bin/env python3
"""Write the golden Iceberg tables the layout tests read.

One contract-shaped table per key type, one more in table format version 3,
plus the off-contract layouts whose serving cost RFD 007 prices and whose
findings RFD 016 reports. Run it against any object store or local
directory; the tests locate the result through ICETRAY_GOLDEN_WAREHOUSE.

    pip install 'pyspark==4.1.*'
    python3 dev/golden-tables.py --warehouse s3a://bucket/prefix/warehouse
    python3 dev/golden-tables.py --warehouse file:///tmp/icetray-golden
    python3 dev/golden-tables.py --only long_2m --warehouse file:///tmp/icetray-golden

Without Spark on the host, dev/golden-tables.sh runs this script with Spark in
a container against a warehouse under target/.

Needs Spark 4.1 (Java 17 or later) with the Iceberg runtime on its classpath;
--packages adds it from Maven when it is absent. 4.1 is the newest Spark line
Iceberg 1.11 ships a runtime for, and the runtime's Scala suffix (2.13) must
match the Spark build's. A Hadoop catalog is used so that a table root
and its version hint are the only handles a reader needs, with no catalog
service to run.

Keys are spaced by three, so key + 1 is always absent and inside the table's
key range: that is the miss a test wants. Values are a function of the key, so
an answer is checkable without a stored oracle.

What this script cannot write, and the in-process writer does instead: a
column of Iceberg's own `uuid` type (Spark SQL has no UUID type to declare),
a snapshot carrying a delete file, an unsorted file, a null key, and a key
column whose physical type contradicts the schema.
"""

import argparse
import time
from typing import NamedTuple, Optional

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
CATALOG = "golden"

# Decimal gigabytes, the unit the per-table line prints file bytes in.
GIGABYTE = 1_000_000_000

# The shapes below mirror the testkit's golden.rs and keys.rs constants of
# the same names; a test derives keys and values from those, so a change
# here without one there breaks the golden tests.

# Keys are spaced by this, so key + 1 is always absent inside the range.
KEY_STEP = 3
# Padding before a string key, so a table has long keys sharing a prefix.
STRING_PREFIX_LEN = 70
# Digits a string key's number is zero-padded to, so string order is
# numeric order.
STRING_DIGITS = 16
# A bijection for id below the modulus (keys unique and uncorrelated with
# input order): the multiplier is the 32-bit golden-ratio constant, the
# modulus a prime above every id the recipe writes.
PERMUTATION_MULTIPLIER = 2_654_435_761
PERMUTATION_MODULUS = 1_000_000_007
# 64-byte digests per value: a contract value is small, a wide one 16 KiB,
# the size a row limit alone leaves in pages of megabytes.
CONTRACT_REPEATS = 3
WIDE_REPEATS = 256

# Rows per table; the wide tables take fewer, since each of their rows
# carries 16 KiB.
ROWS_2M = 2_000_000
ROWS_1M = 1_000_000
ROWS_500K = 500_000
ROWS_250K = 250_000
ROWS_50K = 50_000
# Bucket counts: four for the contract tables, two for the wide and overlap
# tables, none for the unpartitioned table.
FOUR_BUCKETS = 4
TWO_BUCKETS = 2
UNPARTITIONED = 0

# RFD 002. Every entry is a standard Iceberg property or a parquet-mr key that
# Iceberg passes through unchanged.
CONTRACT = {
    "parquet.enable.dictionary": "false",
    "parquet.columnindex.truncate.length": "256",
    "write.parquet.page-row-limit": "1000",
    "write.parquet.page-size-bytes": "8388608",
    "write.parquet.row-group-size-bytes": "134217728",
    "write.parquet.compression-codec": "zstd",
    "write.parquet.bloom-filter-enabled.column.key": "true",
    "write.parquet.bloom-filter-fpp.column.key": "0.01",
    "write.metadata.metrics.column.key": "full",
    "write.distribution-mode": "hash",
    "write.target-file-size-bytes": "67108864",
}

# Large values need a page byte bound, and the bound only binds if the writer
# checks it often enough: RFD 002, prototype findings finding 6.
WIDE = {
    "write.parquet.page-size-bytes": "65536",
    "write.parquet.row-group-check-min-record-count": "4",
    "write.parquet.row-group-check-max-record-count": "16",
}

# Iceberg 1.11 creates a table in format version 2 unless told otherwise;
# this asks for version 3 (row lineage, deletion vectors), the version a
# table owner on a current writer may already have chosen.
FORMAT_V3 = {"format-version": "3"}

LONG_KEY = f"id * {KEY_STEP}"
STRING_KEY = (
    f"concat(repeat('p', {STRING_PREFIX_LEN}), 'user:', "
    f"lpad(CAST(id * {KEY_STEP} AS STRING), {STRING_DIGITS}, '0'))"
)
# The shape that makes a writer emit overlapping files.
PERMUTED_KEY = f"pmod(id * {PERMUTATION_MULTIPLIER}, {PERMUTATION_MODULUS})"
# Sixteen bytes that do not compress, the shape a UUID key has on the wire.
# Deterministic in id, unlike Spark's uuid(), so a value derived from the key
# agrees with it. A column of Iceberg's own `uuid` type is written by the
# in-process writer instead: Spark SQL has no UUID type to declare.
BIN16_KEY = "unhex(md5(CAST(id AS STRING)))"


def value_expr(key_expr, repeats, incompressible=False):
    """64 bytes per repeat, derived from the key.

    The compressible form repeats one digest; the incompressible form
    concatenates distinct digests, which zstd reduces about twofold.
    """
    if not incompressible:
        return f"CAST(repeat(sha2(CAST({key_expr} AS STRING), 256), {repeats}) AS BINARY)"
    return (
        f"CAST(concat_ws('', transform(sequence(1, {repeats}), "
        f"i -> sha2(concat(CAST({key_expr} AS STRING), ':', CAST(i AS STRING)), 256))) AS BINARY)"
    )


def properties(extra=None):
    props = dict(CONTRACT, **(extra or {}))
    return ", ".join(f"'{k}' = '{v}'" for k, v in props.items())


def create(spark, table, key_type, buckets, extra=None, sorted_by_key=True):
    name = f"{CATALOG}.db.{table}"
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    partition = f"PARTITIONED BY (bucket({buckets}, key))" if buckets else ""
    spark.sql(
        f"CREATE TABLE {name} (key {key_type}, value BINARY) USING iceberg "
        f"{partition} TBLPROPERTIES ({properties(extra)})"
    )
    if sorted_by_key:
        # Sorted within each file, which is what a reader needs; a global sort
        # is not required and costs a shuffle.
        spark.sql(f"ALTER TABLE {name} WRITE LOCALLY ORDERED BY key")
    return name


def fill(spark, name, rows, key_expr, repeats, incompressible=False):
    started = time.monotonic()
    spark.sql(
        f"INSERT INTO {name} SELECT {key_expr} AS key, "
        f"{value_expr(key_expr, repeats, incompressible)} AS value FROM range({rows})"
    )
    counts = spark.sql(f"SELECT count(*) AS n, count(DISTINCT key) AS d FROM {name}").first()
    files = spark.sql(f"SELECT count(*) AS f, sum(file_size_in_bytes) AS b FROM {name}.files").first()
    print(
        f"{name}: {counts.n:,} rows ({counts.d:,} distinct), {files.f} files, "
        f"{(files.b or 0) / GIGABYTE:.2f} GB, {time.monotonic() - started:.0f}s",
        flush=True,
    )


def skew_splitting(spark, enabled):
    """Whether one bucket's write may be split across several sorted tasks.

    With it off a bucket's files never overlap; with it on they may, which is
    the cost RFD 007 prices. Prototype findings finding 9.
    """
    spark.conf.set(
        "spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled",
        "true" if enabled else "false",
    )


# Which group a table belongs to: the contract-shaped tables or the
# off-contract layouts. `--only` takes a group or one table's name.
CONTRACT_GROUP = "contract"
OFF_CONTRACT_GROUP = "off-contract"


class Golden(NamedTuple):
    """One golden table: its name and group, and how it is written."""

    name: str
    group: str
    key_type: str
    buckets: int
    rows: int
    key_expr: str
    repeats: int
    extra: Optional[dict] = None
    incompressible: bool = False
    skew: bool = False


# Every golden table, in the order a full run writes them.
TABLES = [
    Golden("long_2m", CONTRACT_GROUP, "BIGINT", FOUR_BUCKETS, ROWS_2M, LONG_KEY, CONTRACT_REPEATS),
    Golden("str_2m", CONTRACT_GROUP, "STRING", FOUR_BUCKETS, ROWS_2M, STRING_KEY, CONTRACT_REPEATS),
    Golden("bin16_500k", CONTRACT_GROUP, "BINARY", FOUR_BUCKETS, ROWS_500K, BIN16_KEY, CONTRACT_REPEATS),
    Golden(
        "long_wide_250k",
        CONTRACT_GROUP,
        "BIGINT",
        TWO_BUCKETS,
        ROWS_250K,
        LONG_KEY,
        WIDE_REPEATS,
        extra=WIDE,
        incompressible=True,
    ),
    Golden(
        "long_v3_50k",
        CONTRACT_GROUP,
        "BIGINT",
        FOUR_BUCKETS,
        ROWS_50K,
        LONG_KEY,
        CONTRACT_REPEATS,
        extra=FORMAT_V3,
    ),
    # The writer's defaults: large key pages misaligned with the value pages,
    # no bloom filter, dictionary-encoded columns where the data allows.
    Golden(
        "default_layout_1m",
        OFF_CONTRACT_GROUP,
        "BIGINT",
        FOUR_BUCKETS,
        ROWS_1M,
        LONG_KEY,
        CONTRACT_REPEATS,
        extra={
            "parquet.enable.dictionary": "true",
            "write.parquet.page-row-limit": "20000",
            "write.parquet.bloom-filter-enabled.column.key": "false",
            "write.metadata.metrics.column.key": "truncate(16)",
            "parquet.columnindex.truncate.length": "16",
        },
    ),
    # No bucket partition: every file is a candidate, narrowed by key range.
    Golden(
        "unpartitioned_500k",
        OFF_CONTRACT_GROUP,
        "BIGINT",
        UNPARTITIONED,
        ROWS_500K,
        LONG_KEY,
        CONTRACT_REPEATS,
    ),
    # Overlapping files without duplicate keys: several sorted tasks per
    # bucket over keys uncorrelated with input order.
    Golden(
        "overlap_250k",
        OFF_CONTRACT_GROUP,
        "BIGINT",
        TWO_BUCKETS,
        ROWS_250K,
        PERMUTED_KEY,
        WIDE_REPEATS,
        extra=WIDE,
        incompressible=True,
        skew=True,
    ),
]


def write(spark, table):
    skew_splitting(spark, table.skew)
    name = create(spark, table.name, table.key_type, table.buckets, extra=table.extra)
    fill(spark, name, table.rows, table.key_expr, table.repeats, incompressible=table.incompressible)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--warehouse", required=True, help="s3a://bucket/prefix/warehouse or file:///path")
    parser.add_argument(
        "--only",
        choices=[CONTRACT_GROUP, OFF_CONTRACT_GROUP, *(table.name for table in TABLES)],
        help="write one group, or one table by name, instead of every table",
    )
    parser.add_argument("--packages", default=ICEBERG_PACKAGE, help="Maven coordinates for the Iceberg runtime; empty if it is already on the classpath")
    parser.add_argument("--shuffle-partitions", type=int, default=64)
    args = parser.parse_args()

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("icetray-golden-tables")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", args.warehouse)
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
    )
    if args.packages:
        builder = builder.config("spark.jars.packages", args.packages)
    spark = builder.getOrCreate()

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.db")
    print(f"spark {spark.version}, warehouse {args.warehouse}", flush=True)

    for table in TABLES:
        if args.only in (None, table.group, table.name):
            write(spark, table)

    print(f"\nset ICETRAY_GOLDEN_WAREHOUSE={args.warehouse} for the golden tests", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
