#!/usr/bin/env python3
"""Write the golden Iceberg tables the layout tests read.

One contract-shaped table per key type, plus the off-contract layouts whose
serving cost RFD 007 prices and whose findings RFD 016 reports. Run it against
any object store or local directory; the tests locate the result through
ICETRAY_GOLDEN_WAREHOUSE.

    pip install 'pyspark==3.5.*'
    python3 dev/golden-tables.py --warehouse s3a://bucket/prefix/warehouse
    python3 dev/golden-tables.py --warehouse file:///tmp/icetray-golden

Needs Spark 3.5 with the Iceberg runtime on its classpath; --packages adds it
from Maven when it is absent. A Hadoop catalog is used so that a table root
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

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1"
CATALOG = "golden"

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

LONG_KEY = "id * 3"
STRING_KEY = "concat(repeat('p', 70), 'user:', lpad(CAST(id * 3 AS STRING), 16, '0'))"
# A bijection for id below the modulus, so keys are unique and uncorrelated
# with input order: the shape that makes a writer emit overlapping files.
PERMUTED_KEY = "pmod(id * 2654435761, 1000000007)"
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
    started = time.time()
    spark.sql(
        f"INSERT INTO {name} SELECT {key_expr} AS key, "
        f"{value_expr(key_expr, repeats, incompressible)} AS value FROM range({rows})"
    )
    counts = spark.sql(f"SELECT count(*) AS n, count(DISTINCT key) AS d FROM {name}").first()
    files = spark.sql(f"SELECT count(*) AS f, sum(file_size_in_bytes) AS b FROM {name}.files").first()
    print(
        f"{name}: {counts.n:,} rows ({counts.d:,} distinct), {files.f} files, "
        f"{(files.b or 0) / 1e9:.2f} GB, {time.time() - started:.0f}s",
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


def contract_tables(spark):
    skew_splitting(spark, False)
    fill(spark, create(spark, "long_2m", "BIGINT", 4), 2_000_000, LONG_KEY, 3)
    fill(spark, create(spark, "str_2m", "STRING", 4), 2_000_000, STRING_KEY, 3)
    fill(spark, create(spark, "bin16_500k", "BINARY", 4), 500_000, BIN16_KEY, 3)
    fill(
        spark,
        create(spark, "long_wide_250k", "BIGINT", 2, extra=WIDE),
        250_000,
        LONG_KEY,
        256,
        incompressible=True,
    )


def off_contract_tables(spark):
    skew_splitting(spark, False)
    # The writer's defaults: large key pages misaligned with the value pages,
    # no bloom filter, dictionary-encoded columns where the data allows.
    fill(spark, create(spark, "default_layout_1m", "BIGINT", 4, extra={
        "parquet.enable.dictionary": "true",
        "write.parquet.page-row-limit": "20000",
        "write.parquet.bloom-filter-enabled.column.key": "false",
        "write.metadata.metrics.column.key": "truncate(16)",
        "parquet.columnindex.truncate.length": "16",
    }), 1_000_000, LONG_KEY, 3)
    # No bucket partition: every file is a candidate, narrowed by key range.
    fill(spark, create(spark, "unpartitioned_500k", "BIGINT", 0), 500_000, LONG_KEY, 3)
    # Overlapping files without duplicate keys: several sorted tasks per
    # bucket over keys uncorrelated with input order.
    skew_splitting(spark, True)
    fill(
        spark,
        create(spark, "overlap_250k", "BIGINT", 2, extra=WIDE),
        250_000,
        PERMUTED_KEY,
        256,
        incompressible=True,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--warehouse", required=True, help="s3a://bucket/prefix/warehouse or file:///path")
    parser.add_argument("--only", choices=["contract", "off-contract"], help="write one group instead of both")
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

    if args.only != "off-contract":
        contract_tables(spark)
    if args.only != "contract":
        off_contract_tables(spark)

    print(f"\nset ICETRAY_GOLDEN_WAREHOUSE={args.warehouse} for the golden tests", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
