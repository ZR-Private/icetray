# Ordered rows on S3

> Updated 2026-10-20: a second run from the same host answers what the
> first left open ([Stage 4](#stage-4-2026-10-20)). Spans of 5 to 16
> pages share the one-page first-byte latency. A fifth table, whose
> merges read files of several row groups with more cursors than
> slots, loads at 0.53 to 0.64 times the serial wall and fills the
> read-ahead budget. The CPU difference between parallel and serial
> merges was the bench's resident-set sampler, not the merge: the merge's
> own threads spend the same CPU either way.

Measured 2026-10-19 from one Linux host in the same region as an S3
bucket holding the four Spark-written reference tables (the golden
`reference` group, `dev/golden-tables.py`). The earlier ordered-rows
measurements served `file://` tables on a laptop
([order fences measured](2026-10-14-order-fences.md)), so they could not
answer questions about object-store latency. This run answers four of
them. It also checks every answer against the derivation in
`crates/icetray-testkit/src/golden.rs` before measuring it.

- **Cold Scans cost what RFD 025's cost table says.** A cold Scan of a
  key held in one file takes one round to object storage under `full`
  and `disk`, and two under `bloom`. The GETs are two (order page and
  value page), one (the order column is local), and three. The p50 is
  26 to 29 ms under `full` and `disk` and 55 to 57 ms under `bloom`,
  whether the key holds 3 rows or 8,476.
- **Span GET latency does not follow span size at the sizes Scans
  issue.** 97.5% of Scan span GETs covered one page and the rest two to
  four. Both classes have the same first-byte latency (p50 25 ms, p99 53
  to 56 ms). The last byte adds 1 ms at p50 and 7 ms at p99. RFD 025
  then scaled a span's hedge delay with its pages, and these spans did
  not justify that scaling. No Scan span here reached five pages; Stage
  4 measures spans of 5 to 16, and RFD 025 has since dropped the
  scaling: every span GET races its window's one delay.
- **Parallel overlap groups halve the load and do not cost CPU here.**
  On the table of 64 buckets with five overlapping files each, merging
  groups at once cut the first load from 4.06 to 1.93 s under `full`
  (medians of nine). `disk` and `bloom` show the same ratios. Process
  CPU fell from 8.10 to 6.09 s, but Stage 4 traced that fall to the
  bench's resident-set sampler, whose cost grows with the load's wall;
  the merge's own CPU is the same either way.
- **Neither read-ahead across row groups nor the shared slot reserve
  was exercised.** Every overlapping file here has one row group. The
  table with four row groups per file is compacted, so its load runs no
  merge. Stage 4 adds a table that exercises both.

## Method

- **Host.** A Linux container limited to 4 CPUs (cgroup), aarch64,
  kernel 6.12, in the bucket's region. Credentials come from the
  container's role through the SDK's default chain. The bucket and role
  are deployment identifiers and are not recorded here.
- **Binaries.** Linux aarch64 builds from `git archive aba0a67`, made
  in a `rust:1.98.1-bookworm` container:
  - `bench` is `crates/icetray-testkit/examples/bench.rs`, built with the
    `profiling` profile.
  - `bench-serial` is the same tree with one line changed in a scratch
    copy: in `generation/ordered.rs`, `most_steps` is 1 rather than
    `shard::workers()`, so overlap groups merge one at a time.
  - `golden` is the icetray-testkit golden test binary.
- **Tables.** About 20 million rows over 200,000 keys, with 1 to about
  10,000 rows per key (heavy-tailed) and `bucket(64, key)`. The order
  column is `ts BIGINT`, plus one value column of 128 bytes: two
  hex SHA-256 digests, which ZSTD compresses about 1.9 times. Spark
  4.1.3 and Iceberg wrote the files,
  ZSTD-compressed, with key bloom filters at fpp 0.01 and no NDV.
  - `ref_compacted`: 64 files, one row group each; level `optimal`.
  - `ref_appended`: the same rows, uncompacted, giving 320 files (five
    per bucket), one row group each, which overlap. Level `permissive`.
  - `ref_string`: like `ref_compacted`, but with the order column as
    fixed-width ISO-8601 text.
  - `ref_large_files`: compacted to 64 files of 3 to 5 row groups each.

  The local warehouse was copied to the bucket as written, and two
  `--rewrite-location` rules map its `file:` locations onto the bucket
  prefix.
- **Correctness first.** The golden reference tests passed against the
  bucket (5 of 5, 129 s) with `ICETRAY_GOLDEN_REWRITE` set. The bench
  compares every request's answer whole against the derivation before
  timing it.
- **Scenarios.** For each table and tier, the bench measures:
  - Scans of five sampled keys per rows-per-key tier (`light` 3 to 19
    rows, `medium` 265 to 822, `heavy` 1,000 to 8,476), at limit 1,000,
    newest and oldest first, with and without a window bound;
  - MultiScans of 10 and 100 keys at limits 1 and 20.

  Each runs on one task and on four tasks.
  - Cold empties the page cache and the scan cache before each
    operation, outside its timing. Under `disk`, the chunk files stay
    local.
  - Warm pages every key through whole first.
  - Runs last 5 s after 1 s of warm-up, and the cold-only set (A) lasts
    10 s.
  - Loads run three rounds per tier.
  - The load A/B alternates `bench` and `bench-serial` three times each
    over `ref_appended` and `ref_large_files`, three rounds a run, for
    nine loads per binary and tier.
- **Per-GET log.** A store wrapper around the real S3 client records,
  per GET, its bytes, the data pages it spans, and its first-byte and
  last-byte times. It keeps up to 4,096 samples per run. Single-task
  cold rows are the clean ones for span latency: concurrent tasks share
  the caches, so a multi-task cold op can hit a page another task
  fetched.

## Cold and warm Scans

Single task, p50 over the 12 rows of each table and tier (three
rows-per-key tiers, two directions, bounded and not). All are in ms.

| Tier | Table | p50 range | p50 median | p99 median | GETs per Scan |
|---|---|---|---|---|---|
| `full` | `ref_compacted` | 27.8 to 31.1 | 28.9 | 63.0 | 2.03 to 2.09 |
| `full` | `ref_appended` | 28.9 to 36.8 | 32.9 | 73.3 | 2.08 to 10.35 |
| `full` | `ref_string` | 27.5 to 30.8 | 29.2 | 68.3 | 2.04 to 2.10 |
| `full` | `ref_large_files` | 28.0 to 30.8 | 28.6 | 65.7 | 2.05 to 2.10 |
| `disk` | `ref_compacted` | 25.1 to 28.4 | 26.1 | 57.0 | 1.01 to 1.06 |
| `disk` | `ref_appended` | 25.9 to 34.1 | 28.7 | 60.0 | 1.02 to 5.17 |
| `disk` | `ref_string` | 25.1 to 28.0 | 26.4 | 56.7 | 1.00 to 1.05 |
| `disk` | `ref_large_files` | 25.9 to 28.6 | 26.6 | 59.2 | 1.01 to 1.06 |
| `bloom` | `ref_compacted` | 52.0 to 60.7 | 56.5 | 105.1 | 3.07 to 3.84 |
| `bloom` | `ref_appended` | 55.3 to 68.4 | 60.2 | 128.7 | 4.82 to 16.10 |
| `bloom` | `ref_string` | 52.2 to 59.6 | 54.8 | 102.8 | 3.04 to 3.82 |
| `bloom` | `ref_large_files` | 52.3 to 57.8 | 54.9 | 104.5 | 3.04 to 3.85 |

Rounds per Scan were 1.00 to 1.08 under `full` and `disk` and 2.00 to
2.19 under `bloom`.

- **One file, one round.** Under `full`, one Scan's two GETs, about 4
  KiB of order page and 68 KiB of value page, run in parallel. Under
  `disk` the order page is a local read (2 to 3.6 local reads per Scan),
  which leaves one GET. A light key returns 1.5 KB from a 68 KiB value
  page and a heavy one 128 KB from 130 KiB, so page size, not run
  length, sets the bytes. The string order column costs nothing
  measurable over the integer one.
- **Overlapping files.**
  - On `ref_appended`, a key is in up to five files. An unbounded Scan
    reads each file's order and value pages in the same round: 10.35
    GETs and 422 KiB for a heavy key, at a p50 4 to 8 ms above one file.
  - A bounded Scan skips files whose order range misses the bound: 6.61
    GETs heavy, 3.35 medium, and 2.08 light (one file).
  - Across K files a Scan with limit L reads up to K·L rows, as RFD 025
    states.
- **Warm.** Warm Scans make no GETs, and each takes 0.2 to 0.4 ms at p50
  on one key-file (light and medium on the compacted tables). A heavy
  Scan takes 0.7 ms, and `ref_appended`'s five files take 0.7 to 1.9 ms.
  A light warm Scan allocates 182 KB to return 1.5 KB from two cached
  pages. The cache holds pages compressed, as the file holds them (RFD
  006), so each hit decompresses a 68 KiB ZSTD page into about 132 KB.
  A local profile on an M3 Pro (`/usr/bin/sample`, warm light-key
  Scans of `ref_compacted` under `full`, 86 µs at p50 there) put 93% of
  the busy samples in ZSTD decompression, 85% for the value page and 6%
  for the order page, mostly Huffman decoding of the hex literals. The
  Scan logic took about 3%, allocation 1.5%, and Parquet decoding 1.7%.
  The bench's own ordered table, whose random values ZSTD stores raw,
  takes 16.4 µs for a warm Scan of 100 rows. A decompressed-page
  level would remove most of a warm Scan on values that compress, at
  their compression ratio in memory; whether that pays is RFD 006's open
  question about such a level, which turns on the page cache's hit
  rate.

## Span GET latency by size

Pooled per-GET samples from the cold Scan and MultiScan runs (single and
multi task, A and C sets, 724,138 answered samples). The bench counted
1,318,258 one-page and 34,640 two-to-four-page Scan GETs, and none
larger.

| Class | Samples | Mean bytes | First byte p50 / p90 / p99 / max | Last byte p50 / p90 / p99 / max |
|---|---|---|---|---|
| 1 page | 690,294 | 39 KiB | 25.1 / 33.3 / 53.4 / 481 ms | 26.3 / 35.6 / 60.0 / 481 ms |
| 2 to 4 pages | 33,844 | 86 KiB | 24.7 / 33.2 / 56.1 / 309 ms | 26.0 / 35.0 / 60.1 / 501 ms |
| under 16 KiB | 314,181 | 3 KiB | 25.3 / 32.7 / 51.2 / 481 ms | 25.5 / 33.3 / 52.7 / 481 ms |
| 64 to 256 KiB | 408,242 | 70 KiB | 25.0 / 33.9 / 55.1 / 478 ms | 26.9 / 37.3 / 64.6 / 501 ms |

At these sizes a GET's latency is a fixed first-byte time plus a small
transfer term. That term is about 1.5 ms at p50 and 12 ms at p99 for
70 KiB against 3 KiB. It is not a per-page cost.

The span hedge window, as measured here, divided each span's elapsed
time by its pages, recorded the quotient, and raced a delay of the
per-page percentile times its pages. With these latencies, a two-page
span recorded half of what it took, which pulled the window's
percentile down. That span also waited twice the delay before it
hedged. The window now records each span's whole elapsed time and races
its one delay. Hedges fired on 2.9 to 3.9% of cold Scan and MultiScan
GETs per tier, under the 5% ceiling, so the effect stayed inside the
budget at this mix of 97.5% single-page spans.

Larger spans need larger limits or values, and the merge's load GETs
had them. Load GETs of 5 to 16 pages (mean 53 KiB) had a first-byte
p50 of 43 ms and a p99 of 250 ms. Those of 17 pages or more (mean 402
KiB) had a p50 of 52 ms and a p99 of 238 ms. Those GETs ran under a
load's full concurrency, though, so they do not isolate size. Spans
past four pages under a Scan's fan-out are unmeasured, and the response
bound `server.max_scan_bytes` caps how large one can grow.

## MultiScan

Cold, single task, p50 in ms, with GETs per call. The 10- and 100-key
sets mix the rows-per-key tiers. Limits 1 and 20 differ by less than
the noise, so the table shows limit 1.

| Tier | Table | 10 keys | 100 keys |
|---|---|---|---|
| `full` | `ref_compacted` | 41 (20.6) | 105 (172) |
| `full` | `ref_appended` | 64 (91.2) | 341 (766) |
| `disk` | `ref_compacted` | 40 (10.2) | 77 (85) |
| `disk` | `ref_appended` | 56 (45.5) | 187 (341) |
| `bloom` | `ref_compacted` | 77 (33.1) | 186 (284) |
| `bloom` | `ref_appended` | 95 (139) | 456 (1,064) |

- **Waves.** One call holds at most `server.batch_fan_out` GETs in
  flight, 64 here. The 100-key cold latency is about the number of
  waves times one GET's latency. On `ref_appended` under `full`, 766
  GETs make 12 waves, and 12 × 28 ms is 336 ms, close to the 341 ms
  measured. 341 GETs make 6 waves for 187 ms under `disk`, and 1,064
  make 17 waves for 456 ms under `bloom`. A caller who reads many keys
  cold from an overlapping layout waits on its waves. The fan-out
  setting trades GETs in flight for that latency.
- **Bytes.** A limit-1 MultiScan of 100 keys reads 5.9 MiB to return
  12.8 KB on `ref_compacted`, one value page per key. On `ref_appended`
  it reads 26 to 27 MiB, a value page per file that holds the key.
- **Warm.** No GETs. On `ref_compacted`, 10 keys take 2.3 ms on one
  run, and 100 keys take 6.1 ms over the three runs that spreading
  makes on four workers. With four calls in flight, 100 keys take 24 ms
  at p50, at the same throughput as one call (about 160 calls per
  second, or 16,000 keys per second). The host's four CPUs are
  saturated at about 250 µs of CPU per key, which is the per-key-file
  cost of the warm Scan above.

## Loads and the parallel merge

Load sizing, the second of three rounds of the matrix run (wall and
CPU in ms; resident index and chunk files in MiB):

| Table | Tier | Wall | CPU | GETs | GET MiB | Rounds | Index | Chunk files |
|---|---|---|---|---|---|---|---|---|
| `ref_compacted` | `full` | 654 | 1,631 | 265 | 106 | 10 | 5.0 | |
| `ref_compacted` | `disk` | 582 | 1,658 | 265 | 106 | 10 | 3.1 | 67.7 |
| `ref_compacted` | `disk`, reusing chunk files | 505 | 785 | 137 | 39 | 10 | 3.1 | 67.7 |
| `ref_compacted` | `bloom` | 653 | 1,748 | 329 | 170 | 13 | 67.1 | |
| `ref_appended` | `full` | 1,850 | 5,886 | 1,608 | 302 | 16 | 6.8 | |
| `ref_appended` | `disk` | 1,792 | 5,869 | 1,288 | 235 | 14 | 4.2 | 69.4 |
| `ref_appended` | `bloom` | 2,200 | 7,066 | 2,248 | 624 | 16 | 324.2 | |
| `ref_string` | `full` | 1,196 | 3,891 | 265 | 150 | 10 | 5.4 | |
| `ref_large_files` | `full` | 1,047 | 2,259 | 655 | 106 | 12 | 5.5 | |
| `ref_large_files` | `bloom` | 1,071 | 2,682 | 914 | 365 | 14 | 262.6 | |

- **Fences.** The order fences take 0.3 MiB for the integer order
  column and 0.5 MiB for the string one.
- **String order column.** `ref_string` loads take 2.4 times the CPU of
  `ref_compacted` at the same GETs.
- **Bloom filters.** Under `bloom`, the filters are 1 MiB per row group
  whatever the row group holds: 64 MiB for `ref_compacted`, 259 MiB for
  `ref_large_files`, and 320 MiB for `ref_appended`'s small append
  files. The writer set an fpp and no NDV, so it sized each filter at
  its byte cap. The doctor reports this as `bloom_oversized`, and the
  golden test expects it.
- **Held bytes.** The merge held up to 66 to 69 MiB, the 64 MiB
  read-ahead bound plus a row group's chosen pages, with either binary.

### Parallel groups against serial

Medians of nine loads per binary (three runs of three rounds,
alternated):

| Table | Load | Tier | Parallel wall / CPU (s) | Serial wall / CPU (s) | Wall ratio | CPU ratio |
|---|---|---|---|---|---|---|
| `ref_appended` | first | `full` | 1.93 / 6.09 | 4.06 / 8.10 | 0.47 | 0.75 |
| `ref_appended` | first | `disk` | 1.75 / 5.90 | 3.65 / 7.80 | 0.48 | 0.76 |
| `ref_appended` | first | `bloom` | 2.06 / 7.01 | 3.93 / 8.87 | 0.52 | 0.79 |
| `ref_appended` | `disk`, reusing chunk files | `disk` | 1.54 / 4.86 | 3.29 / 6.66 | 0.47 | 0.73 |
| `ref_large_files` | first | `full` | 0.79 / 1.95 | 0.62 / 1.86 | 1.26 | 1.05 |
| `ref_large_files` | first | `disk` | 0.64 / 1.93 | 0.65 / 1.91 | 0.98 | 1.01 |
| `ref_large_files` | first | `bloom` | 1.00 / 2.79 | 1.04 / 2.81 | 0.96 | 0.99 |

GETs, bytes, and rounds are identical between the binaries.
`ref_large_files` runs no merge, so it is the control, and its
differences are noise (the `full` wall spread was 0.59 to 1.25 s). On
`ref_appended`, the serial merge spends about one more CPU-second per
second of wall it adds.

The bench samples the resident set on its own thread every 1 ms, and on
Linux that reads `/proc/self/smaps`. A first per-thread count seemed to
rule that sampler out, but it counted only the threads alive when the
process ended, and each load's sampler had exited by then. Stage 4's
count keeps every thread and finds the difference there: the CPU
figures above include the sampler, so their ratio is not the merge's.

## What this does and does not establish

It establishes:

- Scan and MultiScan answers on S3 are exact for all four layouts and
  every tier, by the golden tests and by the bench's verify pass.
- A cold Scan's GETs and rounds match RFD 025's cost table, and its
  latency is one or two first-byte times regardless of run length.
- Scan span GETs at one to four pages share one latency distribution.
- On this host the parallel merge halves the overlapping first load and
  does not raise its CPU.
- Scan span GETs of 5 to 16 pages (Stage 4) share the one-page
  first-byte latency.
- Read-ahead across row groups fills its budget with more cursors than
  slots (Stage 4).

It does not establish:

- **Spans past 16 pages.** The longest run in these tables is 9,998
  rows, so no Scan span reached 17 pages.
- **The slot reserve's cost.** Stage 4 shows a load with more cursors
  than slots filling the read-ahead budget, but has no run of the same
  table with slots to spare to compare its rounds against.
- **Warm-hit cost on the host.** The profile ran on a laptop, not on the
  Linux host; the host's warm Scan is 2.7 times slower, and the shares
  there are inferred from the laptop's, not measured.
- **Generality.** Every number comes from one host, one region, and one
  evening. Latency at other times or from other networks may differ.

## Stage 4, 2026-10-20

The same host, with a fifth reference table and binaries of `c9e8842`.

- **Correctness.** The six golden reference tests passed against the
  bucket (179.8 s).
- **The fifth table.** `ref_large_appended` holds the same 19.9 million
  rows in 576 files, 9 in each of the 64 buckets: one initial file of 3
  or 4 row groups and 8 appended files of one row group. A first load's
  merge therefore starts 576 cursors against the 512 slots
  `load_concurrency()` gives four workers.
- **Spans past four pages.** Cold Scans of heavy keys at a raised
  `max_scan_rows` of 8,000 (`--reference-max-scan-rows`) issued 4,632
  span GETs of 5 to 16 pages.

### Span latency, 5 to 16 pages

Pooled samples from the cold Scans of `ref_compacted` and `ref_appended`
at a limit of 8,000, single task:

| Class | Samples | Mean bytes | First byte p50 / p90 / p99 / max | Last byte p50 / p90 / p99 / max |
|---|---|---|---|---|
| 1 page | 17,391 | 32 KiB | 24.8 / 32.7 / 49.6 / 293 ms | 25.7 / 33.8 / 52.1 / 294 ms |
| 2 to 4 pages | 2,728 | 143 KiB | 25.1 / 36.4 / 60.1 / 100 ms | 26.6 / 38.9 / 62.9 / 104 ms |
| 5 to 16 pages | 4,632 | 269 KiB | 24.6 / 33.6 / 56.6 / 267 ms | 26.9 / 36.8 / 65.0 / 280 ms |

The first byte does not depend on the span's pages up to 16, and the
transfer adds about 2 ms at p50 and 13 ms at p99 for a 269 KiB span over
a 32 KiB one. Under the page-scaled hedge delay this ran with, a 5-page
span waited five times the per-page percentile, about 150 ms, before it
hedged, where its latency is that of a one-page GET. So RFD 025 dropped
the scaling: a span GET of any pages races the window's one delay.

### The merge with more cursors than slots

Medians of nine loads of `ref_large_appended` per binary, alternated
(wall and process CPU in s):

| Load | Tier | Parallel wall / CPU | Serial wall / CPU | Wall ratio | GETs | Rounds parallel / serial | Held MiB |
|---|---|---|---|---|---|---|---|
| first | `full` | 2.94 / 8.59 | 5.29 / 11.07 | 0.56 | 3,333 | 24 / 26 | 64.0 |
| first | `disk` | 2.69 / 8.25 | 4.82 / 10.35 | 0.56 | 2,610 | 17 / 18 | 64.0 |
| first | `bloom` | 3.46 / 10.48 | 5.39 / 12.78 | 0.64 | 4,779 | 28 / 34 | 64.0 |
| `disk`, reusing chunk files | `disk` | 2.29 / 6.80 | 4.35 / 9.05 | 0.53 | 1,164 | 13 / 13 | 64.0 |

Both binaries hold the full 64 MiB read-ahead budget at their peak, so
read-ahead across row groups runs with 576 cursors against 512 slots.

### Where the serial merge's CPU went

A probe sampled every thread's CPU each 0.2 s and kept each thread's last
value, exited threads included. Over one run of three rounds of every
load:

| Run | tokio workers (s) | Threads named after the process (s) |
|---|---|---|
| parallel 1, 2, 3 | 59.5, 60.4, 62.9 | 30.4, 27.8, 26.7 |
| serial 1, 2, 3 | 61.1, 61.1, 60.7 | 56.0, 58.0, 58.2 |

The merge runs on the tokio workers and the blocking pool, which share
the name, and spends the same CPU either way. The difference is in
short-lived threads named after the process, one per measured load,
almost all of their time in the kernel: the resident-set sampler, which
reads `/proc/self/smaps` every 1 ms for as long as the load runs, so a
longer serial load costs it more. The bench has since read
`/proc/self/statm` instead, and leaves the sampler thread's own CPU out
of a load's `cpu_us`, reporting it as `sampler_cpu_us`; the settings
JSON's `rss_sampling` records both, so a rerun's `cpu_us` is the load's.
The statm resident count is the kernel's approximate per-process
counter, where smaps sums every mapping exactly, so a rerun's resident
peaks are not byte-comparable with the ones here.

## Reproducing

```sh
# REF: the warehouse prefix the tables were copied to; ORIG: the local
# warehouse they were written in (the path their metadata names).
RW="--rewrite-location file://$ORIG=$REF --rewrite-location file:$ORIG=$REF"
ICETRAY_GOLDEN_WAREHOUSE=$REF \
ICETRAY_GOLDEN_REWRITE="file://$ORIG=$REF;file:$ORIG=$REF" \
  ./golden --ignored test_golden_reference_ --test-threads=1
./bench --reference $REF $RW --out-dir results-matrix
./bench --reference $REF $RW --reference-table ref_appended,ref_large_files \
  --reference-scenario load --load-rounds 3 --out-dir results-loads
```
