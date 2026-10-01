# The table contract

What a table must look like on disk, verified on the written files. A
table that meets the contract has:

- One key column of Iceberg type `long`, `string`, `binary`, or `uuid`;
  `fixed` is served as binary. A composite key is the caller's encoding
  into that column; icetray moves the bytes unchanged. A key over 4,096
  bytes is refused; a key over 128 bytes draws advice.
- One value column of type `binary`, `fixed`, or `string`, nulls allowed.
  A null value is answered as present with no value, distinct from an
  absent key. Several columns of interest are packed into one binary value
  at write time; a second column would be a second GET per hit.
- `bucket(N, key)` partitioning with hash distribution.
- Each file sorted by key as a local write order. A global sort is not
  required; an unsorted file is refused.
- Dictionary encoding off. A dictionary-encoded value column costs one
  extra GET per hit.
- Pages of 1,000 rows.
- Value pages bounded by bytes (64 KiB) for large values, with the writer's
  page-size check interval shortened so the bound binds.
- Full-length manifest metrics and column index on the key.
- A bloom filter on the key.
- zstd compression.
- Data files of 32 to 64 MiB.

The golden tables' recipe, [`dev/golden-tables.py`](../dev/golden-tables.py),
in Spark SQL:

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
`write.parquet.row-group-check-max-record-count` to small values.
Compaction must carry the same file-size target.

## Deviations

A deviation falls in one of three tiers.

- Refused at load, with a named reason: unsorted files, null or duplicate
  keys, keys outside their file's recorded bucket, a key column whose
  physical type contradicts the schema, an unsupported key or value type,
  delete files, keys over the cap, and `bloom` residency on a table whose
  files carry no key bloom filter. `disk` residency without a local
  directory is a configuration error. The residency tiers are in
  [Operating icetray](operating.md#the-local-directory).
- Priced, with a measured penalty in extra GETs per hit, key bytes decoded
  per lookup, or files probed per lookup: a dictionary-encoded value
  column, key pages over 2,000 rows, no bucket partition, and overlapping
  files within a bucket. A priced table is refused when `strict` is on
  (the default) and served with its penalty reported when it is off.
- Advice, which names the writer property to change and does not affect
  whether the table serves: truncated bounds, oversized value pages, value
  pages above the 64 MiB ceiling (a lookup into one is refused), long
  keys, oversized files, a truncated column index, too many files per
  bucket, and a bloom filter that is absent or badly sized.

`icetray doctor` is the check; its command line and output are in
[Configuration and the command line](configuration.md). The decisions:
[RFD 002](rfd/002-serving-layout-contract.md) (the contract),
[RFD 007](rfd/007-serving-tiers.md) (the tiers and their penalties), and
[RFD 016](rfd/016-doctor.md) (the doctor).
