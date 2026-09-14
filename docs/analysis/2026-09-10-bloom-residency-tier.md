# The bloom residency tier, measured

Measured 2026-09-10 with a prototype implementation of RFD 004's fallback:
instead of keeping the compressed key column chunks resident, a table keeps
footer, page indexes, exact fences, and the key column's bloom filters. On
the real S3 tables the tier behaved as the design predicts on latency (a cold
hit is two dependent GETs, about twice the median) and diverged from the
naive memory expectation in ways that matter for sizing: whether it saves
memory depends on rows per row group and key shape, because the writer's
filters were a fixed 1 MiB each, and one table's index turned out to be
dominated by a structure neither tier needs.

## Method

- **Implementation.** A `residency` setting per table, `full` or `bloom`.
  Both tiers load footer and page indexes and stream every key chunk once to
  verify sort order, nulls, the key length cap, and to build exact fences and
  the exact first and last key per file. `full` keeps the chunks. `bloom`
  reads each row group's filter (a load-path range read of the offset and
  length the footer states) and drops the chunks after overlap verification.
  A lookup in the bloom tier checks the filter in memory; a negative is a
  miss with no I/O; a positive fetches the one key page the fences name
  through the page cache, decodes it, and on a hit fetches the value page.
- **Tables.** The S3 tables of [prototype findings](2026-09-10-prototype-findings.md),
  unchanged; every one carries a key bloom filter written by Iceberg 1.10.1
  with the sizing properties it ignored (finding 7 there).
- **Vantage.** A laptop over a residential connection, profile credentials,
  release build, 100 cold hits and 100 misses per table per tier in one
  process each, keys sampled uniformly. Memory from the server process after
  load: `ps` RSS, the kernel's physical footprint, and the live heap as
  listed by the macOS `heap` tool.
- **Local fixtures.** The in-process writer sizes filters from the row count
  at 1 percent false positives; two fixtures of 20,000 keys (64-bit, and
  221-byte strings).

## Results

Resident bytes per tier as the doctor predicts them from footers alone, and
what the tiers measured after load. Files per table include the four small
append files on `long_2m`.

| table | files, rows per file | full | bloom | filter bytes per key |
|---|---|---|---|---|
| `long_2m` | 4 of 500k plus 4 of 1,225 | 3.3 MiB | 9.0 MiB | 2.1 on the large files, 856 on the append files |
| `str_2m` | 4 of 500k | 7.2 MiB | 5.6 MiB | 2.1 |
| `long_50m` | 25 of 2M | 100.7 MiB | 50.0 MiB | 0.5 |
| `long_wide_250k` | 4 of 62k, 4 to 5 row groups | 34.3 MiB | 38.5 MiB | 17 to 20 |
| local, 20k 64-bit keys | 16 | 100,063 B | 103,472 B | sized at 1 percent |
| local, 20k 221-byte keys | 16 | 172,281 B | 131,952 B | sized at 1 percent |

Lookups from the laptop, 100 cold hits and 100 misses each:

| table | tier | GETs per hit | hit p50 | p90 | p99 | GETs per miss |
|---|---|---|---|---|---|---|
| `long_2m` | full | 0.97 | 58 ms | 65 | 110 | 0.00 |
| `long_2m` | bloom | 1.94 | 118 ms | 138 | 268 | 0.00 |
| `str_2m` | full | 0.98 | 61 ms | 73 | 207 | 0.00 |
| `str_2m` | bloom | 1.96 | 114 ms | 123 | 268 | 0.00 |
| `long_50m` | full | 1.00 | 135 ms | 180 | 478 | 0.00 |
| `long_50m` | bloom | 2.00 | 198 ms | 277 | 369 | 0.29 |

(GETs per hit below 1.0 are two sampled keys sharing a page.) Every hit
returned the expected value; no miss returned a value. Load time was not
lower in the bloom tier (`long_50m` 3.6 s full, 4.8 s bloom), since both
stream the chunks and bloom reads the filters besides.

Server memory for `long_50m` after load, steady for 30 seconds:

| tier | index accounting | RSS | physical footprint | live heap |
|---|---|---|---|---|
| full | 100.7 MiB | 207 MiB | 196 MiB | 112.7 MB (25 blocks of 3.1 MB and metadata) |
| bloom | 50.0 MiB | 224 MiB | 213 MiB | 58.1 MB (25 blocks of 1 MiB and metadata) |

## Observations

- **Latency doubles, as the design says.** Two dependent GETs cost twice
  the median on every table (about 2.0 times on the 20 MB files, 1.5 times
  on the 80 MB files whose first GET already carries a wide tail).
- **Whether memory falls depends on the filter's size per key, which on
  Iceberg 1.10.1 is a constant 1 MiB per row group.** That makes bytes per
  key a function of rows per row group: 0.5 at 2M rows (`long_50m`, memory
  halved), 2.1 at 500k rows (`long_2m`, memory nearly tripled against
  1.2-byte compressed chunks), 856 on a 1,225-row append file. With a
  correctly sized filter (the local fixtures), the bloom tier is a wash on
  64-bit keys and saves about a quarter on 221-byte keys, consistent with
  [key bytes by shape](2026-09-10-key-bytes-by-shape.md): the tier pays for
  keys that do not compress.
- **A fixed 1 MiB filter is also too small at 2M rows.** Four bits per key
  gave 29 false positives per 100 misses on `long_50m`, so a miss there cost
  0.29 GETs instead of nothing. At 500k rows (17 bits per key) no false
  positive appeared in 100 misses. Filter sizing is therefore not advice
  only: it decides the bloom tier's miss cost, and Iceberg 1.10.1 cannot set
  it. The doctor already reports filter bytes per key per file.
- **The live heap halves; the process does not shrink on macOS.** The
  `heap` listing shows the chunks freed (113 MB live against 58 MB), while
  RSS and physical footprint stayed at their load-time peak, because the
  system allocator keeps freed medium-sized blocks in its zone. Load peaks
  at the full tier's size in both tiers, since the chunks are streamed for
  verification. Whether the saving reaches the operating system depends on
  the allocator; an allocator that purges freed pages (jemalloc with
  background purging, or glibc's unmapping of large blocks) would show it.
- **The wide tables' index is not keys at all.** 34 MiB for 250,000 keys is
  about 140 bytes per key. Their value pages hold four 16 KB rows each, so a
  file has about 15,600 value pages, and the value column's column index
  stores a minimum and a maximum per page, each truncated to the contract's
  256 bytes: about 32 of the 34 MiB. The reader never consults the value
  column's minimums and maximums; only its offset index is needed. Keeping
  the value column's column index out of the resident set would cut those
  tables' index by about 90 percent, and the same applies in proportion to
  every table.
- **The permuted-key table overlaps.** `long_wide_shuffled_250k`, written
  with skew splitting on and a permuted 64-bit key, is priced by the doctor
  as two files probed per lookup in each of its buckets, with no duplicate.
  The overlapping-files fixture on S3 exists, correcting the earlier note in
  prototype findings, finding 9.

## What the evidence implies

- The bloom tier is a memory lever for long or random keys and for tables
  whose filters are sized to their row groups, not a general one. Its miss
  cost is set by the filter's bits per key, so the doctor's report of filter
  bytes per key is the number to read before choosing it.
- The residency choice belongs to the table owner, in configuration, with
  the doctor's two predicted figures beside it; an automatic fallback would
  have chosen worse on three of the four S3 tables here.
- The resident index should exclude the value column's column index. That
  is a reader-side change with no writer cooperation needed and it dominates
  memory for large-value tables under the byte-bounded page rule.
- Process memory metrics should report the index accounting, which is live
  bytes, beside RSS, and the serving binary should use an allocator that
  returns freed memory.

## Not measured

Steady-state memory on Linux with a purging allocator; the bloom tier under
Iceberg 1.11, which honors the filter sizing properties; a UUID-keyed table
on S3 (the case the tier exists for, only measured on the local 221-byte
string fixture and by [key bytes by shape](2026-09-10-key-bytes-by-shape.md));
the tier's behavior on a refresh that widens an overlap group, which re-reads
the affected files' chunks on the load path.
