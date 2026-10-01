# The table contract

What a table must look like on disk for icetray to serve it at the cost it
promises, and what happens to a table that does not.

The writer decides what a lookup costs, so the contract is a description of
written files, verified on the files and never inferred from the properties
that were set. A table that meets it has:

- One key column of Iceberg type `long`, `string`, `binary`, or `uuid`
  (`fixed` is served as binary). A composite key is the caller's encoding
  into that column; icetray moves the bytes unchanged. Keys over 4,096
  bytes are refused; keys over 128 bytes draw advice, because resident
  bytes per key grow with key length.
- One value column of type `binary`, `fixed`, or `string`, nulls allowed.
  The value's bytes are served unchanged, and a null value is answered as
  present with no value, distinct from an absent key. What a null means is
  the table owner's rule. A table with more than one column of interest
  packs them into one binary value at write time; a second column would be
  a second GET per hit.
- `bucket(N, key)` partitioning with hash distribution, so a lookup routes
  to one bucket by arithmetic.
- Each file sorted by key as a local write order. No global sort is needed,
  and an unsorted file cannot be served.
- Dictionary encoding off. A dictionary-encoded value column costs one
  extra GET per hit.
- Pages of 1,000 rows, so a lookup decodes one key page and fetches one
  value page.
- Value pages bounded by bytes (64 KiB) for large values, with the writer's
  page-size check interval shortened so the bound binds. A row limit alone
  leaves value pages of many megabytes.
- Full-length manifest metrics and column index on the key. Truncated
  bounds make every file in a bucket report the same key range.
- A bloom filter on the key, which the bloom residency tier serves from.
- zstd compression.
- Data files of 32 to 64 MiB. First-byte latency of a range GET rises with
  object size.

As an example, this is how the project's own golden tables are written in
Spark SQL (the full recipe is [`dev/golden-tables.py`](../dev/golden-tables.py)):

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
`write.parquet.row-group-check-max-record-count` to small values so the
byte bound is checked often enough to hold. Compaction has to carry the
same file-size target or it undoes the layout.

Deviations fall into three tiers, and the line between them is what a
client could observe. A table icetray cannot serve correctly is refused at
load with a named reason: unsorted files, null or duplicate keys, keys
outside their file's recorded bucket, a key column whose physical type
contradicts the schema, an unsupported key or
value type, delete files, keys over the cap, and the residency refusals
below. A table that serves correctly but slowly is priced: a dictionary
encoded value column, key pages over 2,000 rows, no bucket partition, and
overlapping files within a bucket each carry a measured penalty in extra
GETs per hit, key bytes decoded per lookup, or files probed per lookup. A
priced table is refused when its `strict` setting is on, which is the
default, and served with its penalty reported when it is off. Everything
else (truncated bounds, oversized value pages, value pages above the
64 MiB ceiling past which a lookup into them is refused, long keys,
oversized files, a truncated column index, too many files per bucket, a
bloom filter that is absent or badly sized) is advice, which names the
writer property to change and never affects whether a table serves.

`icetray doctor` is the check; its command line and output are in
[Configuration and the command line](configuration.md). The decisions
behind the contract are [RFD 002](rfd/002-serving-layout-contract.md) (the
contract), [RFD 007](rfd/007-serving-tiers.md) (the tiers and their
penalties), and [RFD 016](rfd/016-doctor.md) (what the doctor checks and
reports).
