# The table contract

The contract is what a table must look like on disk for icetray to serve
it at one GET per column a cold hit reads and none per miss. icetray
checks it on the files as written, never on the properties a writer was
given, at every load and in `icetray doctor`.

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
`UPDATE`, and `MERGE` is copy-on-write. A table that sets any of them to
merge-on-read writes delete files, which icetray serves when they are
deletion vectors or Parquet position deletes
([RFD 021](rfd/021-merge-on-read.md)). A delete adds no GET to a lookup
under `full` and `disk`; under `bloom` a miss on a deleted key pays the
key-page GET a bloom filter's false positive does. An update writes the
new row to a new data file whose key range overlaps the older file's,
which caps the table at `permissive` through `overlapping_files` until a
compaction rewrites the bucket. Compaction must carry the same file-size
target, or it undoes the layout.

## Requirements

A table that misses a requirement gets one of three outcomes:

- **Refused**: icetray will not serve the table, because an answer could
  be wrong or the table cannot be held as configured. The load fails with
  the reason in the second column.
- **Caps a level**: every answer is right, and some lookups do more work.
  The doctor reports the finding with its measured penalty in extra GETs
  per hit, keys decoded per lookup, or files probed per lookup, and the
  [level](configuration.md#choosing-a-level) it caps the table at:
  `balanced` for a cost within one file, `permissive` for more files
  probed. A table capped below its `lookups` level serves and is flagged;
  under `strict` the snapshot is refused as `below_level`.
- **Advice**: the doctor names the writer property to change. Advice never
  affects whether the table serves.

The second column names the refusal reason or the finding code, as the
doctor's output and the metrics spell it.

| Requirement | If missed | What fixes it |
|---|---|---|
| One key column of Iceberg type `long`, `string`, `binary`, `fixed` (served as binary), or `uuid` | Refused: `key_type_unsupported`; a key column whose Parquet type contradicts the schema is `key_type_mismatch` | Declare the key with one of those types. A composite key is the caller's encoding into one column. |
| Keys of at most 4,096 bytes | Refused: `key_too_long`. Keys over 128 bytes draw advice, `long_keys` | A shorter key encoding |
| No null keys | Refused: `null_keys` | Filter nulls before the write |
| A key at most once within a data file, deleted rows included | Refused: `duplicate_keys` | Deduplicate before the write |
| A live key in at most one data file: a row a delete removes is not live | Refused: `duplicate_keys` | Deduplicate before the write, or upsert with `MERGE`, which rewrites the older row or deletes it |
| Each configured value column of type `binary`, `fixed`, or `string`; nulls allowed | Refused: `value_type_unsupported`, naming the column | Serve a typed field inside a binary column, such as a serialized protobuf. See [Value columns](#value-columns) for when to pack fields into one column and when to split them. |
| Each configured column held under the name and field id the snapshot's schema gives it, in data files that record a field id on every column, as Iceberg's writers do | Refused: `column_mismatch`, for a column held under another field id (a swap of names, or a name dropped and added again), its field id held under another name (a rename), a file that records no field ids and lacks the column, a file without its key column, a file holding the name as a nested group, a `fixed` key or value column stored at another width, a configured name that is not a top-level field, or a stated default that does not read as its column's type. A file that records field ids and was written before its value column was added is served, its rows answering the column's default: advice, `column_default`, with the count of such files | Write through an Iceberg writer, and change the schema as [Changing the schema](#changing-the-schema) describes; a compaction rewrites the files that predate a column |
| No equality delete that may apply to a data file | Refused: `equality_deletes`, for an equality delete newer than the snapshot's oldest data file, which may apply to one (RFD 021). An equality delete no data file is older than changes no answer and is ignored. | A table whose writer leaves equality deletes can be served from a tag or branch that a compaction moves after rewriting them into data files. |
| Only deletes icetray reads: deletion vectors and Parquet position deletes | Served (RFD 021). Refused: `delete_kind_unsupported`, for a position delete file in Avro or ORC that may apply to a data file no deletion vector masks, a Puffin blob other than a deletion vector, or a Puffin footer that is compressed or sets reserved flags. An Avro or ORC position delete file that applies to no data file the snapshot holds changes no answer and is ignored, and so is one whose every data file is masked: a deletion vector that applies to a data file masks its position deletes. | Merge-on-read writers that write deletion vectors or Parquet position deletes |
| Each data file sorted by key | Refused: `unsorted` | `ALTER TABLE ... WRITE LOCALLY ORDERED BY key`. A global sort is not needed. |
| Every key in the bucket its file's manifest entry records | Refused: `key_outside_bucket` | Let Iceberg's bucket transform assign files; never register files under a partition by hand |
| `bucket(N, key)` partitioning, and no other partition field | Caps at `permissive`: `no_bucket_partition`, files probed per lookup | `PARTITIONED BY (bucket(N, key))`, then rewrite the files written without it. See [Choosing the bucket count](#choosing-the-bucket-count). |
| Within a bucket, files whose key ranges do not intersect | Caps at `permissive`: `overlapping_files`, files probed per lookup | `'write.distribution-mode' = 'hash'`; `spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled=false` on the write session; or a compaction |
| Dictionary encoding off on each value column | Caps the column at `balanced`: `dictionary_encoded_value`, naming the column, one extra GET per hit that reads it | `'parquet.enable.dictionary' = 'false'` |
| Key pages of 1,000 rows | Caps at `balanced` above 2,000 rows: `large_key_pages`, keys decoded per lookup | `'write.parquet.page-row-limit' = '1000'` |
| Value pages bounded by bytes for large values | Advice, per column, when a file's mean value page for that column is over 1 MiB: `large_value_pages`. Over 64 MiB: `value_pages_over_ceiling`, and a lookup into a page that large is refused with an error | `'write.parquet.page-size-bytes' = '65536'` with a short page-size check interval, as the recipe above describes |
| Full-length manifest metrics on the key | Advice: `bounds_missing` | `'write.metadata.metrics.column.key' = 'full'` |
| A full-length column index on the key | Advice: `column_index_truncated` | `'parquet.columnindex.truncate.length'` at least the key length, such as `'256'` |
| A bloom filter on the key | Advice: `no_bloom_filter`. Under `bloom` residency the table is refused with the same name | `'write.parquet.bloom-filter-enabled.column.key' = 'true'` |
| A bloom filter sized for its keys | Advice above 4 bytes per key, `bloom_oversized`, or below 8 bits per key, `bloom_undersized` | `write.parquet.bloom-filter-max-bytes` and `write.parquet.bloom-filter-ndv.column.key`, which Iceberg honors from 1.11 |
| Deletes compacted into the data files | Advice while deletes remove rows: `deletes_pending`, with the rows, delete files, and delete state a compaction would remove and, where files overlap, the level the table reaches once none do | A compaction that rewrites the data files the deletes apply to |
| Data files of 32 to 64 MiB | Advice above 128 MiB, `large_file`; and when a bucket holds more than twice the files its bytes explain at 64 MiB each, `many_files_per_bucket` | `'write.target-file-size-bytes' = '67108864'`, and the same target in compaction |
| zstd compression | Not checked | `'write.parquet.compression-codec' = 'zstd'` |

Two refusals come from configuration rather than from the files. A `full`
table whose resident bytes exceed its `resident_budget_bytes`, or a load of
`disk` tables whose chunk files would exceed
`storage.local.index_budget_bytes`, is refused as `over_budget` before any
key chunk or delete is fetched. A `full` table's resident bytes count its
delete state at a bound taken from the manifests. `disk` residency without
a local directory is a configuration error. `resident_budget_bytes` is in
[`[[tables]]`](configuration.md#tables) and `index_budget_bytes` in
[`[storage.local]`](configuration.md#storagelocal).

A per-column property in a finding's `property` field is printed as its
prefix up to the column name, such as
`write.metadata.metrics.column.`; the finding's message names the column.

A finding about one value column names it in its `column` field, and each
column has a level of its own: the caps of its own findings with those of
every finding about the row or the table. The table's level is its
dearest column's, so under `strict` one column below the declared level
holds the whole table.

## Value columns

A table serves the value columns its configuration lists in
[`value_columns`](configuration.md#tables), and a lookup reads only the
columns it names ([RFD 022](rfd/022-value-columns.md)). A table may list
none, to answer only whether a key is present, and a presence-only lookup
costs only finding the row. Each column a cold hit names costs a GET of
its own, issued concurrently with the others, so a hit naming several
columns waits on the slowest of them.

One packed column performs best. A table read by one kind of caller packs
the row into one `binary` column at write time, such as a serialized
protobuf whose field addition handles evolution, and a cold hit costs one
GET. A table read by several kinds of caller splits its row along how it
is read:

- Fields read together share a column.
- A large part read on its own, such as an embedding, gets a column of
  its own, so a caller that wants only the small fields does not fetch
  and decode it, and a value page holds more rows of the rest.
- A caller that needs most of a row is better served by one packed
  column than by naming several, which costs a GET per column.

Each column is resolved in each data file on its own, so one column may
answer its default in a file where another is served.

## Changing the schema

icetray finds each configured column in a data file by its top-level name
and checks it against the field id the snapshot's schema gives that name
([RFD 023](rfd/023-column-resolution.md)). Iceberg's writers record a
field id on every column of every file they write; a file written without
them is served for the columns it holds by name, unchecked, and refused
for one it lacks, since nothing in it says whether that column was added
after it or renamed. A file with field ids on only some columns is read
as one without them, except that a column found by name that carries a
field id other than the schema's is refused. Iceberg's Java writers store a column whose name is
not a valid Avro name under an escaped one, `user-id` as `user_x2Did`
and `1st` as `_1st`; a file column under either name is the configured
column.

- A column can be added to a served table without a rewrite. A file
  written before the column answers the column's `initial-default` for
  it, or null when the schema states none, and reads no value page for
  it. The doctor reports such files as `column_default` until a
  compaction rewrites them with the column.
- A served column is not renamed. To serve a column under a new name,
  add a new column beside it, write it, and move the callers to it; then
  drop the old one from the clients, then from the configuration, then
  from the schema. A rename is refused: under the old name as a column
  the schema lacks (a table fault), under the new name as
  `column_mismatch`. A swap of two names is refused as
  `column_mismatch`. Either way the last snapshot served keeps serving.
- Dropping a column and adding another under the same name is refused
  the same way: the files written before hold the old column under that
  name, under another field id.

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

`icetray doctor` runs the check and prints the findings; its command
line and output are in [Configuration and the command
line](configuration.md). The decisions behind the contract: [RFD
002](rfd/002-serving-layout-contract.md) (the contract), [RFD
007](rfd/007-serving-tiers.md) (refusals, costs, and advice, with their
penalties), [RFD 009](rfd/009-writes-between-builds.md) (writes between
builds), [RFD 016](rfd/016-doctor.md) (the doctor), [RFD
020](rfd/020-lookup-levels.md) (lookup levels), [RFD
021](rfd/021-merge-on-read.md) (delete files), [RFD
022](rfd/022-value-columns.md) (value columns), and [RFD
023](rfd/023-column-resolution.md) (column resolution). Terms such as
residency, level, and strict are in the [glossary](glossary.md).
