# The table contract

The contract is what a table must look like on disk for icetray to serve
it at one GET per cold hit and none per miss. icetray checks it on the
files as written, never on the properties a writer was given, at every
load and in `icetray doctor`.

## The recipe

A table written with these properties meets every requirement below. In
Spark SQL, as the golden tables' recipe
[`dev/golden-tables.py`](../dev/golden-tables.py) writes it:

```sql
CREATE TABLE db.kv (key BIGINT, value BINARY) USING iceberg
PARTITIONED BY (bucket(16, key))
TBLPROPERTIES (
  'parquet.enable.dictionary' = 'false',
  'parquet.columnindex.truncate.length' = '256',
  'write.parquet.page-row-limit' = '1000',
  'write.parquet.page-size-bytes' = '8388608',
  'write.parquet.row-group-size-bytes' = '134217728',
  'write.parquet.compression-codec' = 'zstd',
  'write.parquet.bloom-filter-enabled.column.key' = 'true',
  'write.parquet.bloom-filter-fpp.column.key' = '0.01',
  'write.metadata.metrics.column.key' = 'full',
  'write.distribution-mode' = 'hash',
  'write.target-file-size-bytes' = '67108864'
);
ALTER TABLE db.kv WRITE LOCALLY ORDERED BY key;
```

For values of kilobytes per key, the recipe lowers
`write.parquet.page-size-bytes` to `65536` and sets
`write.parquet.row-group-check-min-record-count` and
`write.parquet.row-group-check-max-record-count` to small values, so the
byte bound takes effect. Iceberg's default write mode for `DELETE`,
`UPDATE`, and `MERGE` is copy-on-write, which the contract needs; a table
that sets any of them to merge-on-read writes delete files. Compaction
must carry the same file-size target, or it undoes the layout.

## Requirements

A table that misses a requirement gets one of three outcomes:

- **Refused**: icetray will not serve the table, because an answer could
  be wrong or the table cannot be held as configured. The load fails with
  the reason in the second column.
- **Inefficient**: every answer is right, and some lookups do more work.
  The doctor reports the finding with its measured penalty in extra GETs
  per hit, keys decoded per lookup, or files probed per lookup. With
  `strict` on, the default, an inefficient table is refused; with it off,
  the table serves with the penalty.
- **Advice**: the doctor names the writer property to change. Advice never
  affects whether the table serves.

The second column names the refusal reason or the finding code, as the
doctor's output and the metrics spell it.

| Requirement | If missed | What fixes it |
|---|---|---|
| One key column of Iceberg type `long`, `string`, `binary`, `fixed` (served as binary), or `uuid` | Refused: `key_type_unsupported`; a key column whose Parquet type contradicts the schema is `key_type_mismatch` | Declare the key with one of those types. A composite key is the caller's encoding into one column. |
| Keys of at most 4,096 bytes | Refused: `key_too_long`. Keys over 128 bytes draw advice, `long_keys` | A shorter key encoding |
| No null keys | Refused: `null_keys` | Filter nulls before the write |
| Every key once in the snapshot | Refused: `duplicate_keys` | Deduplicate before the write, or upsert with `MERGE` |
| One value column of type `binary`, `fixed`, or `string`; nulls allowed | Refused: `value_type_unsupported` | Pack the columns a caller needs into one binary value at write time, such as a serialized protobuf. A second column would cost a second GET per hit. |
| No delete files or deletion vectors | Refused: `delete_files` | `write.delete.mode`, `write.update.mode`, and `write.merge.mode` left at `copy-on-write`. A table written merge-on-read can be served from a tag or branch that a compaction moves after rewriting the deletes into data files. |
| Each data file sorted by key | Refused: `unsorted` | `ALTER TABLE ... WRITE LOCALLY ORDERED BY key`. A global sort is not needed. |
| Every key in the bucket its file's manifest entry records | Refused: `key_outside_bucket` | Let Iceberg's bucket transform assign files; never register files under a partition by hand |
| `bucket(N, key)` partitioning, and no other partition field | Inefficient: `no_bucket_partition`, files probed per lookup | `PARTITIONED BY (bucket(N, key))`, then rewrite the files written without it. See [Choosing the bucket count](#choosing-the-bucket-count). |
| Within a bucket, files whose key ranges do not intersect | Inefficient: `overlapping_files`, files probed per lookup | `'write.distribution-mode' = 'hash'`; `spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled=false` on the write session; or a compaction |
| Dictionary encoding off on the value column | Inefficient: `dictionary_encoded_value`, one extra GET per hit | `'parquet.enable.dictionary' = 'false'` |
| Key pages of 1,000 rows | Inefficient above 2,000 rows: `large_key_pages`, keys decoded per lookup | `'write.parquet.page-row-limit' = '1000'` |
| Value pages bounded by bytes for large values | Advice when a file's mean value page is over 1 MiB: `large_value_pages`. Over 64 MiB: `value_pages_over_ceiling`, and a lookup into a page that large is refused with an error | `'write.parquet.page-size-bytes' = '65536'` with a short page-size check interval, as the recipe above describes |
| Full-length manifest metrics on the key | Advice: `bounds_missing` | `'write.metadata.metrics.column.key' = 'full'` |
| A full-length column index on the key | Advice: `column_index_truncated` | `'parquet.columnindex.truncate.length'` at least the key length, such as `'256'` |
| A bloom filter on the key | Advice: `no_bloom_filter`. Under `bloom` residency the table is refused with the same name | `'write.parquet.bloom-filter-enabled.column.key' = 'true'` |
| A bloom filter sized for its keys | Advice above 4 bytes per key, `bloom_oversized`, or below 8 bits per key, `bloom_undersized` | `write.parquet.bloom-filter-max-bytes` and `write.parquet.bloom-filter-ndv.column.key`, which Iceberg honors from 1.11 |
| Data files of 32 to 64 MiB | Advice above 128 MiB, `large_file`; and when a bucket holds more than twice the files its bytes explain at 64 MiB each, `many_files_per_bucket` | `'write.target-file-size-bytes' = '67108864'`, and the same target in compaction |
| zstd compression | Not checked | `'write.parquet.compression-codec' = 'zstd'` |

Two refusals come from configuration rather than from the files. A `full`
table whose resident bytes exceed its `resident_budget_bytes`, or a load of
`disk` tables whose chunk files would exceed
`storage.local.index_budget_bytes`, is refused as `over_budget` before any
key chunk is fetched. `disk` residency without a local directory is a
configuration error. `resident_budget_bytes` is in
[`[[tables]]`](configuration.md#tables) and `index_budget_bytes` in
[`[storage.local]`](configuration.md#storagelocal).

A per-column property in a finding's `property` field is printed as its
prefix up to the column name, such as
`write.metadata.metrics.column.`; the finding's message names the column.

## Choosing the bucket count

`N` in `bucket(N, key)` is fixed when the table is written. Changing it
means rewriting every file.

- Aim for each bucket to receive about one target-sized file per full
  write: `N` near the table's compressed size divided by 64 MiB. A write
  with hash distribution sends each bucket to one task, so `N` is also the
  write's parallelism.
- Fewer buckets than that is harmless to lookups. A sorted task rolls a
  new file at the size target, the files of one bucket then cover disjoint
  key ranges, and a lookup still probes one of them. The cost is larger
  write tasks.
- More buckets than that makes small files. Each file adds a footer read
  at load, resident index overhead, and under `disk` residency an open
  descriptor, and a bucket of many small files draws
  `many_files_per_bucket`.
- A table that takes frequent appends gains up to one file per bucket per
  append. Fewer buckets, or a regular compaction, keeps the file count
  down.
- Partition by the key's bucket alone. A second partition field, such as
  a date, splits one bucket's keys across files whose key ranges
  intersect, which the doctor reports as `overlapping_files`.

## The checks

`icetray doctor` runs the check and prints the findings; its command line
and output are in [Configuration and the command line](configuration.md).
The decisions behind the contract:
[RFD 002](rfd/002-serving-layout-contract.md) (the contract),
[RFD 007](rfd/007-serving-tiers.md) (refused, inefficient, and advice, with
their penalties), [RFD 009](rfd/009-writes-between-builds.md) (delete
files), and [RFD 016](rfd/016-doctor.md) (the doctor). Terms such as
residency and strict are in the [glossary](glossary.md).
