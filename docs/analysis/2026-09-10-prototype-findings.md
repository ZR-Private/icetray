# Prototype findings

What a throwaway prototype of icetray established between 2026-09-07 and
2026-09-10, with the measurements behind each claim. Nothing below requires
reading the prototype's code. The decisions in `docs/rfd/` cite this document
rather than repeating it.

## Summary

- A lookup against a sorted, bucket-partitioned Iceberg table costs one S3
  range GET when the key exists and none when it does not, provided each data
  file's resident index (its Parquet footer, page indexes, compressed key
  column chunks, and the first key of every page) stays in memory. This held
  on every table tried, including tables written with the Parquet writer's
  default settings.
- The full resident index costs 1.5 to 2.1 bytes per key for 64-bit keys, of
  which the compressed key chunks are 1.2 to 1.6, and 4.3 bytes per key on
  the one 91-byte string-key table measured in full. A 50-million-row table
  on S3 loaded in under four seconds (measured before a credential fix that
  can only have shortened it) into an index of about 100 MiB, 161 MiB of
  process memory.
- From a laptop, a cold hit (a lookup that finds its key and has to fetch the
  page holding the value) took about 60 ms at the median on a table with
  20 MB data files and about twice that on a table with 80 MB files. The
  reader opened no connections during lookups, so the gap is S3's first-byte
  latency by object size, measured in isolation in
  [the range-GET latency analysis](2026-09-10-s3-range-get-latency.md).
- Four assumptions were wrong or incomplete. Hedging (issuing a duplicate of
  a slow GET after a delay and taking whichever answers first) with a fixed
  delay hurt when latency sat above the delay. Spark wrote several sorted
  files per bucket with overlapping key ranges. Two Iceberg writer
  properties were no-ops on Iceberg 1.10.1. The AWS SDK's credential chain
  had to be cached by the caller.

## What was built

Five Rust crates: a core library (murmur3 bucket routing, an object-store
layer with location rewriting and request counting, Iceberg metadata and
manifest parsing, a per-file resident index, a shared page cache, generations
(the full resident index built for one snapshot, swapped in atomically when
the table advances), a layout checker called the doctor, TOML configuration),
a gRPC service definition, a server, a command-line tool, and a test kit (an
in-process S3 server, an Iceberg writer for fixtures, a deterministic network
simulation, a locator for Spark-written golden tables). About 7,100 lines of
Rust including tests at the end, 53 tests plus one ignored scale probe, one
of the 53 gated on a container.

## Method

Units: memory in MiB; object and file sizes in decimal MB and GB as Spark
and S3 report them; page sizes in bytes.

### Fixtures

Three sources of tables:

1. **Written in-process** by the Iceberg Rust writer, with writer properties
   chosen per test: page row limit, page byte limit, dictionary encoding,
   key type, key length, value size, number of files per bucket, partitioned
   or not.
2. **Written by Spark into a local warehouse**, covering the contract layout
   (the writer properties in the recipe below, which the doctor verifies and
   the reader never assumes) and three off-contract layouts: the writer's
   default settings (key pages of about 17,900 rows, misaligned with the
   value pages); parquet-mr's default column index truncation (page
   minimums and maximums cut to 64 bytes, shorter than the table's 91-byte
   keys); and Iceberg's default manifest metrics (string bounds cut to
   16 bytes).
3. **Written by Spark 3.5.8 with Iceberg 1.10.1 into an S3 bucket**, from a
   notebook running in the same AWS region as the bucket, using a Hadoop
   catalog so that a table root and its version hint were the only handles
   needed. Read back from a laptop over a residential connection in another
   city with credentials from a named AWS profile, and by the server.

### Verifiable answers

Every fixture value is a function of its key. In-process fixtures use a
seeded pseudo-random byte stream per key (incompressible). The S3 tables use
SHA-256 of the key repeated `n` times (compressible), or, for the 16 KB-value
tables, SHA-256 digests of the key and a counter concatenated as hex text,
which zstd compresses about 2:1. Any answer can be checked without a stored
oracle. 64-bit keys are multiples of three, so key+1 is always absent and
lies inside the file's key range; that is the miss case that matters.

### The request counter is the oracle

The storage layer counts every GET it issues. Tests assert exact counts: one
per cold hit, zero per miss, two per file at load. Latency numbers come from
the command-line tool's timing output and from the server's Prometheus
histograms.

### Writer recipe for the S3 tables

Table properties on the `CREATE TABLE`, all standard Iceberg properties or
parquet-mr keys that Iceberg passes through unchanged. The "without it"
column says what the prototype measured or expects when the property is
missing; only the sort order affects correctness.

| property | value | without it |
|---|---|---|
| `parquet.enable.dictionary` | `false` | one extra GET per hit for the value dictionary page (finding 1) |
| `parquet.columnindex.truncate.length` | `256` | no effect on cost with resident key chunks; matters for tools that route by column index |
| `write.parquet.page-row-limit` | `1000` | larger key pages cost more CPU per lookup (finding 1); larger value pages cost more bytes per hit |
| `write.parquet.page-size-bytes` | `8388608` (small values) / `65536` (16 KB values) | for large values, pages of many megabytes per hit (finding 6) |
| `write.parquet.row-group-check-min-record-count` / `-max-record-count` | `4` / `16` (16 KB values only) | the byte limit is checked too late to bind (finding 6) |
| `write.parquet.row-group-size-bytes` | `134217728` | Iceberg's default; listed so the recipe is complete |
| `write.parquet.compression-codec` | `zstd` | listed so the recipe is complete |
| `write.parquet.bloom-filter-enabled.column.key` | `true` | no fallback when key chunks cannot stay resident (finding 7) |
| `write.parquet.bloom-filter-fpp.column.key` | `0.01` | as above |
| `write.parquet.bloom-filter-ndv.column.key` | `500000` (`125000` on the 16 KB-value tables) | ignored on 1.10.1 either way (finding 7) |
| `write.metadata.metrics.column.key` | `full` | manifest bounds truncated; every file in a bucket reports the same range (finding 8) |
| `write.distribution-mode` | `hash` | files from every task land in every bucket |

plus `PARTITIONED BY (bucket(N, key))` and `ALTER TABLE ... WRITE LOCALLY
ORDERED BY key` so each file is sorted without a global sort. Unsorted
files are the one thing the reader cannot serve. The 16 KB-value tables also
set `write.parquet.bloom-filter-max-bytes = 262144`, which had no effect
(finding 7). One Spark session setting, not a table property, decided how
many files each bucket got:
`spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled` (default
true) was set to false before writing `str_2m` (its second write) and
`long_wide_250k`, and left at its default for `long_2m`, `long_50m`, and
`long_wide_split_250k` (finding 9).

Tables written this way:

| table | keys | rows | files | on S3 | value |
|---|---|---|---|---|---|
| `long_2m` | 64-bit | 2,000,000 | 4 | 83 MB | 192 B, compressible |
| `str_2m` | 91-byte string | 2,000,000 | not recorded (four buckets, splitting off) | not recorded | 192 B, compressible |
| `long_wide_250k` | 64-bit | 250,000 | 4 (2 per bucket, rolled at 512 MB) | 2.14 GB | 16,384 B of hex digests, about 2:1 compressible |
| `long_wide_split_250k` | 64-bit | 250,000 | several per bucket | not recorded | as above, written with Spark's skew splitting left on |
| `long_50m` | 64-bit | 50,000,000 | 25 | 2.0 GB | 192 B, compressible |

## Findings

### 1. One GET per cold hit, none per miss, on every layout

| table | cold hit | miss | values checked |
|---|---|---|---|
| in-process fixtures, contract layout and off-contract page shapes | 1 GET | 0 GETs | every hit |
| in-process fixture, dictionary-encoded columns | 1 GET plus one for the value dictionary page (the key dictionary page is part of the resident chunk) | 0 GETs | every hit |
| Spark, contract layout (2M rows, 20 files) | 1 GET, one 23.7 KB page | 0 GETs | every hit |
| Spark, default writer (17,900-row key pages) | 1 GET | 0 GETs | every hit |
| Spark, 91-byte string keys, truncated column index | 1 GET | 0 GETs | every hit |
| `long_2m`, `str_2m`, `long_wide_250k`, `long_50m` on S3 | 1 GET, including the 16,384-byte values | 0 GETs | every hit, byte for byte |

An unpartitioned in-process table (no `bucket` transform) also served
correctly, with every file a candidate narrowed by its exact key range; the
doctor reports that layout as inefficient by files probed per lookup.

The off-contract layouts serve at the same GET count because the resident
key chunks answer "which page" exactly, without consulting the column index
or the manifest bounds. What those layouts cost is CPU per lookup (decoding a
17,900-row key page instead of a 1,000-row one) or, for dictionary
encoding, the extra GET, not correctness. Ten concurrent lookups whose keys
fall in one page coalesce to one GET through the page cache's fetch
deduplication.

### 2. Resident index size

Footers, page indexes, compressed key chunks, and the fence array (first key
of every page) per file. The full index is what the server keeps; the
key-chunk figure is what the doctor reports for the compressed key column
alone.

| table | resident bytes | full index per key | key chunks per key |
|---|---|---|---|
| Spark contract layout, 64-bit keys, 2M rows, 20 files | 3,539,514 B | 1.77 B | not recorded |
| Spark default writer, 64-bit keys, 2M rows | 2,918,496 B | 1.46 B | not recorded |
| Spark, 91-byte string keys, 2M rows | 8,567,634 B | 4.28 B | not recorded |
| `long_2m` on S3 | 3.3 MiB | 1.73 B | 1.20 B (the files' bloom filters, not kept resident, would add 2.1 B) |
| `str_2m` on S3 | not recorded | not recorded | 2.97 B |
| `long_50m` on S3 | 100.7 MiB | 2.11 B | 1.59 B |
| 1,500 files of 1,000 rows each | 9.4 MiB | 6.6 B (footers dominate small files) | not recorded |

For tables with byte-bounded value pages, most of the index is the value
column's column index rather than keys; see
[bloom residency tier, observations](2026-09-10-bloom-residency-tier.md#observations).

Process memory tracks the index. Serving `long_50m`, the command-line tool
peaked at 161 MiB of resident memory for a 100.7 MiB index; serving
`long_2m`, 42 MiB for a 3.3 MiB index. About 40 MiB is process baseline, so
the marginal overhead is roughly 1.2 times the index. An illustrative
projection to three billion 64-bit keys (beyond the largest workload in
[the workload analysis](2026-09-09-workload-archetypes.md)) at the measured
2.1 bytes per key: about 6.3 GB of index and 7.5 GB of process memory per
replica, before any page cache. The per-key figures above are from files of
100,000 to 2,000,000 rows; files of a few thousand rows sit in the regime
where footers dominate. Bytes per key for other key shapes (random
integers, UUIDs as bytes and as text, composites) are measured on synthetic
files in [key bytes by shape](2026-09-10-key-bytes-by-shape.md).

### 3. Load time

| what | time | requests |
|---|---|---|
| 2M rows, 20 files, local disk, release build, warm cache | 40 ms (64-bit keys), 120 ms (string keys) | 2 GETs per file |
| `long_2m` on S3 from a laptop, first call including single sign-on; before the credential fix, not re-measured | 4.7 s | not recorded |
| `long_50m` on S3 (25 files, 2.0 GB) from a laptop; before the credential fix, not re-measured | 3.8 s | not recorded |
| 1,500 files of 1,000 rows, local disk, release build | 83 ms | 3,002 GETs, 161 MiB read |
| append across all 1,500 buckets, refresh to 3,000 files | 96 ms | the first 1,500 files reused |
| deterministic simulation, 16 files, 5 ms link | 60 ms virtual | 35 GETs in 6 dependent round trips, including the TCP handshake |

The two S3 loads include some credential overhead (finding 11); loads issue
GETs concurrently, so the overhead is a fraction of each figure, and the
figures are upper bounds.

Load work is per file, not per row: footer with a 512 KiB tail prefetch, then
the key column chunks. The prefetch reads whole small files (the 1,500-file
probe read 161 MiB for a 9.4 MiB index). On S3 that trades bytes for round
trips: one GET for a whole small file in place of two or three dependent
GETs for footer, page index, then chunks. The comparison was not measured on
S3; the dependent-round-trip count comes from the simulation (finding 12).

Nothing was quadratic in the manifest walk, bucket map, overlap check, or
candidate filter at 1,500 files; lookups against 1,500 files took 72 µs,
flat against the 20-file fixtures.

### 4. Lookup latency

Local disk, release build, 300 keys per sample, two decoding mechanisms:

| case | Arrow record-batch stream | Parquet column reader over resident bytes |
|---|---|---|
| cold hit (page read from disk) | 357 µs mean | 319 µs |
| page-cache hit | 122 µs mean, 176 µs p95 | 100 µs, 154 µs p95 |
| miss | 22 µs | 15 µs |
| string-key hits in adjacent pages | 178 µs | 157 µs |

The column reader gained 10 to 30 percent. Of a 100 µs cached hit, about
85 µs is zstd-decompressing the 23.7 KB value page to its 216 KB uncompressed
size (1,000 rows of 216 bytes) and decoding one row; the lookup logic itself
is about 15 µs. Value pages bounded by bytes (finding 6) and a
decompressed-page cache tier are the two levers this points at.

S3 from a laptop, cold hits:

| table | data file size | p50 | p90 | p99 | sample |
|---|---|---|---|---|---|
| `long_2m` | 20 MB | 62 ms | 66 ms | 173 ms | 100 hits, after the credential fix |
| `long_50m` | 80 MB | 113 to 126 ms | not recorded | about 300 ms | 100 hits, after the credential fix |
| `long_wide_250k` | 549 MB | 233 ms | not recorded | not recorded | 12 hits, before the credential fix (about 80 ms of it credential overhead), not re-measured |

Warm hit 0.2 ms; miss 0.0 ms and no request. The reader opened no
connections during lookups (checked with the HTTP client's connection debug
output), so the gap between tables is S3's first-byte latency by object size,
which the latency analysis measures in isolation.

Every number measured before the credential fix included about 80 ms per GET
of spawning an external credential helper. Those earlier figures (for
example a 137 ms median on `long_2m`) are superseded.

### 5. Snapshot advance on S3

With the server following a table root by its version hint and polling every
five seconds, an idle poll costs one GET (the hint file); before a fix it
cost two plus a credential spawn. An append of 5,000 rows from the notebook
was picked up within one poll interval: one generation swap, the new
snapshot's info metric at 1 and the old at 0, the index grew by 25 KB (four
new small files indexed, four existing files reused by path), appended keys
served, and their key+1 misses cost nothing. The four appended files were
about 1.1 MB each, most of it the bloom filter (finding 7).

### 6. A row limit alone does not bound page size for large values, and both writers check the byte limit lazily

Both Parquet writers tried check the page byte limit between write batches
or after a minimum row count, not per row.

- arrow-rs, 16 KB incompressible values: with a 1,000-row page limit and no
  byte limit, pages of about 16 MB (one write batch); with a 64 KiB byte
  limit and 1,000-row write batches, pages of about 11 MB, the limit ignored
  within a batch; with the same byte limit and 8-row write batches, pages of
  about 64 KB. All served at one GET per hit; the difference is megabytes
  versus 64 KB transferred per cold hit.
- parquet-mr checks after a minimum of 100 rows by default (maximum 10,000).
  The first 16 KB-value table on S3 came out with 1.6 MB pages. Iceberg
  exposes the check interval as
  `write.parquet.row-group-check-min-record-count` and
  `write.parquet.row-group-check-max-record-count`, names that suggest row
  groups but set the page-size check. With those at 4 and 16 and a 64 KiB
  page byte limit, value pages came out at 65,570 bytes uncompressed (four
  rows) as the doctor reports them, and twelve hits verified byte for byte at
  one GET each.

So a writer recipe for large values needs a byte limit and a short check
interval; a row limit alone does not bind.

### 7. Two Iceberg 1.10.1 writer properties are no-ops

Neither `write.parquet.bloom-filter-ndv.column.<key>` nor
`write.parquet.bloom-filter-max-bytes` changed the bloom filter. Filters came
out at 1.0 to 1.3 MiB per file regardless, which is 2.1 bytes per key at
500,000 rows per file and 17 to 20 bytes per key at 62,500 rows per file.
Iceberg's release notes say 1.11 honors the NDV property; that was not
tested. The resident key chunks make bloom filters a fallback for tables
whose key chunks are too large to keep in memory, so this affects sizing
guidance, not correctness.

`parquet.enable.dictionary` and `parquet.columnindex.truncate.length` did
pass through as raw parquet-mr keys and behaved as documented.

### 8. Manifest bounds are truncated by default

Iceberg's default column metrics truncate string bounds to 16 bytes. On the
91-byte-key table every file in a bucket reported the same key range, and a
first overlap check built on manifest bounds refused the table wrongly.
Exact per-file ranges taken from the decoded key chunk (first and last key)
cost nothing once the chunk is resident; the prototype's routing and overlap
check moved to them after the manifest-bound check refused this table.
`write.metadata.metrics.column.<key> = full` fixes the manifest, but a reader
cannot assume it was set.

### 9. Spark writes several sorted files per bucket, sometimes overlapping

With `write.distribution-mode = hash` and adaptive query execution's skew
splitting left at its default
(`spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled = true`),
Spark split a bucket's write into two or more tasks, each locally sorted.
The resulting files overlapped in one case and not in the other:

- `long_wide_split_250k` (250,000 64-bit keys, two buckets) came out as
  disjoint slices per bucket.
- The first write of `str_2m` (2,000,000 91-byte string keys, four buckets)
  came out with overlapping key ranges within a bucket. No key was
  duplicated.

Why one overlapped and the other did not is not established. Both keys are
monotonic in the input row number (the string key is zero-padded), so key
order alone does not explain it; the two writes also differed in row count,
bucket count, and key width. A permuted key, uncorrelated with input order,
is a sufficient way to construct the overlapping case on purpose.

The prototype serves overlapping files by probing every file whose exact
range covers the key; misses cost nothing, so the price is key pages decoded
per lookup, not GETs. It refuses only a duplicate key across files, detected
by merging the sorted key runs of the overlapping group from the resident
chunks. An earlier version refused any overlap and rejected `str_2m`, which
serves correctly.

A first implementation of the merge re-verified every group on every refresh
with a linear scan over the group's cursors and a row group per cursor:
9.6 seconds for 25 files of 2M 64-bit keys, 30 seconds for string keys, at
open and again at every refresh. After review it merges only groups that
gained a file, keeps cursors ordered by their head with binary-search
reinsertion, and decodes one key page at a time; a test asserts that a
disjoint append verifies zero groups. The merge was not re-timed on the same
fixtures after the change. An S3 fixture of the overlapping case built with
a permuted 64-bit key was written later and is reported inefficient by the doctor at two
files probed per lookup
([bloom residency tier, observations](2026-09-10-bloom-residency-tier.md#observations)).

### 10. Hedging

| setting | result |
|---|---|
| simulation, uniform random latency | hedging made the maximum worse (63 ms against 53 ms): uniform latency has no tail to cut |
| simulation, a 300 ms stall on every fifth request, hedge at 25 ms | worst cold lookup fell from 310 ms to 35 ms, mean from 62 ms to 15 ms; 9 hedges in 46 GETs |
| S3 from a laptop, `long_2m` (p50 about 135 ms before the credential fix), hedge at 150 ms | 6 hedges in 100, p99 295 ms with and without |
| S3 from a laptop, `long_50m` (p50 about 240 ms before the fix), hedge at 150 ms | 95 hedges in 100, p50 fell from 237 to 222 ms, p99 rose from 425 to 508 ms |
| S3 from a laptop, `long_2m` (p50 about 60 ms after the fix), hedge at 80 ms | 1 hedge in 100, no change |
| S3 from a laptop, `long_50m` (p50 about 120 ms after the fix), hedge at 80 ms | 92 hedges in 100, p50 rose from 126 to 139 ms, p99 unchanged |
| debug build from local disk, hedge at 50 ms | four footer prefetches hedged during load |

A fixed delay set below the typical latency makes nearly every request hedge:
GETs double and the tail does not move (`long_50m`). Where a minority of
requests hedged (`long_2m`), the p99 did not change within a 100-request
sample, so the fixed-delay S3 runs neither confirm nor refute that hedging
cuts a real tail; the simulation with an injected stall is the only evidence
that it does. The evidence implies a delay that tracks the observed
distribution (a high percentile of recent GETs), a ceiling on the hedge rate,
and no hedging of load reads; none of these were run (see "What was not
verified").

### 11. Without caller-side caching, the SDK credential chain spawns the credential helper on every GET

Bridging the SDK's credential provider into the object-store client without
the SDK's identity cache spawned the profile's external credential helper on
every GET, about 80 ms each. Tests never saw it (static keys) and a raw HTTP
client never saw it (its own signing). The server log showed the tell:
"loaded base credentials" twice per poll. Caching credentials until two
minutes before expiry took cold hits on `long_2m` from 137 to 62 ms and the
idle poll from two GETs plus a spawn to one GET. A latency measurement of a
system like this needs a raw-HTTP baseline beside it.

### 12. Deterministic simulation

A real S3 client and a real in-process S3 server over a simulated network
with 5 ms link latency gave identical results across runs with one seed:
generation load of 16 files is 6 dependent round trips (60 ms virtual), a
cold hit is exactly 1 round trip on a pooled connection (10 ms), a miss is
0 ns. Round-trip counts are a better test oracle than wall time.

### 13. Key length

221-byte string keys served at one GET per hit with a doctor warning that
does not refuse the table. 5 KiB keys were refused at load from the column
statistics, against a 4,096-byte cap chosen for the prototype. Index bytes
per key scale with key length (finding 2), which is the sizing argument for
any cap; the Parquet and Iceberg formats impose none.

### 14. Metrics

A 63-bit snapshot id exported as a floating-point gauge lost its low digits
(…352600 for …352672). Snapshot ids fit in a label on an info metric.

### 15. What reviews found that tests did not

An adversarial review of the overlap change found five defects that 53
passing tests had not, two of them measured in seconds (finding 9). The
specification the code violated was a doc comment ("an incremental snapshot
costs its delta"); a test that counts work per refresh pins such a promise
where a comment cannot.

A read-through of the finished prototype, done while inventorying it for the
rebuild, found three more defects that no test exercised. None was measured;
each was confirmed by reading the code.

- **The delete-file refusal was unreachable.** The manifest walk kept only
  data manifests before checking each entry's content type, so a
  merge-on-read snapshot's delete manifests were never read and the refusal
  never fired. Such a snapshot would have served deleted rows as live. No
  fixture carried a delete file.
- **Row-group selection fell back to column statistics.** Within a file, the
  row group to search was chosen by the Parquet column-chunk statistics; a
  key the statistics did not exclude from an earlier row group resolved to
  that row group's last page and read as a miss. Statistics that are absent
  cause it, and so do statistics that are truncated, which the Arrow writer
  does by default for string and binary values longer than 64 bytes: a
  later review reproduced it with 117-byte string keys sharing a 100-byte
  prefix in a file of three row groups, where 4,000 of 6,000 present keys
  were reported absent. The exact first and last key of every row group are
  decoded at load and answer correctly; the prototype was changed to use
  them. Contract-shaped files have one row group, so no earlier fixture hit
  it.
- **Layout came from the current metadata, not the snapshot's.** The key
  column, value column, and bucket position were taken from the table's
  current schema and default partition spec rather than from the snapshot's
  schema and each manifest's partition spec, so a snapshot written under an
  earlier spec would have been misrouted. No fixture evolved a spec.

The pattern across all eight defects: the promise was stated (a doc comment,
a refusal message, a contract clause) and no fixture produced the input that
tests it.

## What was not verified

- Latency over hours or days, from a pod in the serving cluster, or in
  another region; every S3 number is from one laptop and one in-region
  notebook pod on two days.
- Load time at thousands of files on S3 (1,500 files was measured from
  local disk only). Load time on S3 after the credential fix was measured
  with the icetray binary of 2026-09-21: under four seconds for `long_50m` and `long_2m`
  loading together
  ([icetray against real S3](2026-09-21-real-s3-verification.md#load-time-after-the-credential-fix)).
- Throughput per core and behavior under concurrent load; the page cache's
  hit-rate curve under a skewed key distribution.
- Adaptive hedging under a real stall episode. The mechanism was verified
  against S3 with the icetray binary of 2026-09-21 (the window fills, the percentile is
  taken, the ceiling binds) on an afternoon whose latency had one mode, so
  it trimmed nothing
  ([icetray against real S3](2026-09-21-real-s3-verification.md#adaptive-hedging)).
- The bloom-filter fallback was measured after this document was first
  written; see [the bloom residency tier](2026-09-10-bloom-residency-tier.md).
- The overlap merge's cost after the review changes, on the 25-file fixtures.
- Merge-on-read snapshots (delete files, deletion vectors): the prototype
  intended to refuse them and the refusal was unreachable (finding 15).
  Serving or refusing them was never exercised.
- Catalog-vended credentials; a REST catalog was exercised only against a
  local fixture with a shared filesystem warehouse.
- Spark tables with several row groups per file (unit tests cover it; no
  golden table has it).
- Non-ASCII string keys against Spark output (bytewise order follows from
  UTF-8 construction; not exercised).
- A metadata-only doctor mode (footers and manifests, one GET per file); the
  doctor needs a full load today.
