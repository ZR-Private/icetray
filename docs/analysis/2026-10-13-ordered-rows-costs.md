# Ordered scan costs and the key-value path beside them

Measured 2026-10-13 on one Apple M3 Pro. The measurement answers two
questions RFD 025 (Ordered rows per key) raises. First, whether the
key-value path keeps its costs per call after the ordered-rows work: warm
and cold Get, MultiGet, and misses, in every residency tier, on a process
serving key-value tables alone and on one that also serves an ordered
table. Second, what a Scan and a MultiScan of an ordered table cost in
each tier, warm and cold.

The key-value path keeps its latency and throughput in all but one of 30
comparisons. In 27 the two binaries' ranges overlap, with medians that
move by 5% or less, and in a 28th, the Full-tier warm hit on one task,
the after runs are faster by 1 ns (12.541 against 12.542 µs). Two fall
outside the spread on the slow side, and were run again. A 1,000-key
MultiGet in the Bloom tier on one task takes 0.4% longer (10 µs on
2.64 ms), and the second set reproduced it at 0.8%. Cold-hit throughput
in the Bloom tier on eleven tasks fell 3.8% in the first set, and the
second set did not reproduce it (0.6% lower, within its spread).

The allocation counter shows a change the timings do not. Every
key-value call makes the same number of allocations as before and
requests more bytes: 96 bytes more per hit, about 290 bytes more per key
of a MultiGet, and about 8 KiB more per page a cold read fetches, the
same in every run. Serving an idle ordered table beside the key-value one
moves no figure outside the spread of the key-value runs alone, except
the Full-tier warm hit on one task, 0.7% slower and inside the spread of
the runs before the change.

A warm Scan of a key's 100 rows takes 20.5 µs on one task in the Full
and Bloom tiers and 38.4 µs in the Disk tier, where the order column is
read from the local directory. A cold one fetches the value page and the
order page, 79.2 µs in the Full tier, and in the Bloom tier the key page
as well, 123.4 µs. A MultiScan of 10 keys costs 24 µs a key warm, and
55 µs a key cold, where its reads overlap.

## Method

- **Host.** Apple M3 Pro, 11 cores (5 performance, 6 efficiency), 36 GiB,
  macOS 26.6.2 (Darwin 25.6.0, arm64). Other work shared the machine: the
  load average read 6 to 17 while the runs ran, so every comparison
  alternated its before and after binaries.
- **Program.** `crates/icetray-testkit/examples/bench.rs`, built with
  Cargo's release profile, at two commits. Before is `97f39af`, the last
  commit before the ordered-rows work, built in a separate worktree with
  its own target directory. After is `a126a1c`, which adds the ordered
  scenarios to the bench over the library code of `0f01576`. Both
  ran against one `--data-dir`, so they read the same data files. The
  bench serves tables through `Table` over `Storage`'s `file://` store, so
  no figure includes object-store latency, and data files are read once
  beforehand, so a cold read is a copy from the OS page cache. It uses
  jemalloc behind an allocation counter, with the binary's Prometheus
  recorder installed. Each lookup scenario has a 1 s warm-up and a 5 s
  window. "1 task" is one lookup at a time; "11 tasks" is one task per
  core. The warm cache is 1 GiB and the cold one 1 MiB, the page cache
  and the scan cache each sized so.
- **Key-value table.** The bench's `long` table: 2,000,000 long keys of
  100 random bytes each, in 4 buckets, one file per bucket, in the
  contract layout (1,000-row pages, zstd, a key Bloom filter).
- **Key-value comparison.** `warm-hit`, `cold-hit`, `miss`,
  `multi-get-100`, and `multi-get-1000` on the long table in the Full,
  Disk, and Bloom tiers, three runs per binary, alternating before and
  after. Each after run also ran `kv-beside-ordered`: `warm-hit`,
  `cold-hit`, `miss`, and `multi-get-100` on the long table in a process
  that also serves the ordered table over the same storage, page cache,
  scan cache, local directory, and count of calls in flight, as a server
  opens its tables. The ordered table is loaded and primed, then idle
  while the key-value scenarios are measured, so the comparison isolates
  what serving it costs a key-value call from the CPU a concurrent scan
  load would take. The tables report medians, with the lowest and highest
  of the three runs in parentheses. A comparison falls outside the spread
  when the two binaries' ranges do not overlap. The two that did were run
  again, three more times per binary, alternating.
- **Ordered table.** The bench's `ordered` table: 20,000 long keys of 100
  rows each, 2,000,000 rows, a `timestamp` order column with each key's
  rows a minute apart, 100 random bytes per value, 4 buckets, one file per
  bucket, the contract layout. Every key's run lies in one file, so each
  Scan reads one file.
- **Scan scenarios.** `scan-warm` and `scan-cold` read a key's whole run,
  newest first, at limit 0, which reads up to the default
  `max_scan_rows` of 1,000 and so returns all 100 rows. `scan-bounded`
  reads a page of 10 rows, newest first, between the order values a
  quarter and three quarters into the run. `multi-scan-10` and
  `multi-scan-10-cold` read 10 keys at limit 200, the largest the default
  `server.max_multi_scan_values` of 4,000 admits for 10 keys and one
  column, which again returns every row. Three runs, every tier, medians.
  Every Scan answer is checked against the naive-sort model inside the
  window: its order values, `more`, and one value of 100 bytes per row.
  Before each scenario is measured, the first 100 operations' answers are
  compared with the model whole, every value's bytes included, through
  both Scan and MultiScan, so a broken Scan fails the run instead of
  reading as fast.
- **Loads.** Three loads per run of each table and tier; the figure is the
  median of rounds 2 and 3 of the three runs.

## The key-value path

p50 on one task, µs (MultiGet 1,000 in ms):

| scenario | tier | before (`97f39af`) | after (`a126a1c`) | after, beside the ordered table |
|---|---|--:|--:|--:|
| warm hit | Full | 12.6 (12.5–13.3) | 12.5 (12.5–12.5) | 12.6 (12.6–12.7) |
| warm hit | Disk | 21.2 (21.2–21.4) | 21.4 (21.0–21.5) | 21.1 (21.0–21.2) |
| warm hit | Bloom | 13.0 (13.0–13.5) | 13.0 (13.0–13.1) | 13.0 (12.9–13.1) |
| cold hit | Full | 65.8 (65.8–66.1) | 66.1 (66.0–66.2) | 66.1 (65.6–66.2) |
| cold hit | Disk | 76.0 (74.9–77.6) | 75.7 (74.7–75.8) | 75.6 (75.0–75.6) |
| cold hit | Bloom | 107.2 (106.1–107.5) | 107.7 (107.4–107.9) | 107.8 (107.3–108.0) |
| miss | Full | 6.5 (6.5–6.9) | 6.5 (6.5–6.5) | 6.5 (6.5–6.5) |
| miss | Disk | 14.2 (14.0–14.3) | 14.2 (14.1–17.7) | 14.2 (14.1–14.6) |
| miss | Bloom | 0.21 (0.21–0.21) | 0.21 (0.21–0.21) | 0.21 (0.21–0.21) |
| MultiGet 100 | Full | 323.2 (322.1–323.5) | 321.8 (319.6–364.2) | 323.6 (318.4–323.8) |
| MultiGet 100 | Disk | 919.5 (909.3–922.1) | 901.8 (895.1–930.9) | 918.5 (904.7–929.0) |
| MultiGet 100 | Bloom | 331.6 (330.8–331.6) | 332.0 (325.8–333.1) | 331.4 (330.0–331.8) |
| MultiGet 1,000 | Full | 2.54 (2.54–2.86) | 2.54 (2.54–2.72) | |
| MultiGet 1,000 | Disk | 9.45 (9.36–10.10) | 9.64 (9.50–9.67) | |
| MultiGet 1,000 | Bloom | 2.638 (2.631–2.638) | 2.647 (2.644–2.657) | |

Throughput on 11 tasks, operations a second (a MultiGet's are batches):

| scenario | tier | before (`97f39af`) | after (`a126a1c`) | after, beside the ordered table |
|---|---|--:|--:|--:|
| warm hit | Full | 509,718 (508,939–522,169) | 519,036 (518,148–522,196) | 512,016 (500,188–524,821) |
| warm hit | Disk | 132,405 (121,447–135,021) | 131,192 (129,933–134,813) | 132,225 (132,196–134,716) |
| warm hit | Bloom | 487,030 (466,480–500,207) | 498,198 (486,135–499,543) | 497,459 (495,603–504,028) |
| cold hit | Full | 55,517 (55,418–57,238) | 57,245 (57,093–57,604) | 57,456 (56,017–57,525) |
| cold hit | Disk | 42,877 (41,100–43,053) | 42,845 (38,749–43,170) | 42,911 (42,850–43,290) |
| cold hit | Bloom | 28,680 (28,434–28,710) | 27,599 (27,560–28,364) | 28,344 (27,708–28,639) |
| miss | Full | 1,103,436 (1,094,663–1,107,083) | 1,085,594 (1,019,090–1,108,044) | 1,104,591 (1,103,207–1,112,464) |
| miss | Disk | 136,604 (130,960–136,964) | 133,534 (114,556–139,774) | 136,255 (136,026–138,386) |
| miss | Bloom | 8,828,198 (8,669,753–8,868,124) | 8,791,776 (8,746,582–8,823,433) | 8,766,917 (8,746,736–8,779,468) |
| MultiGet 100 | Full | 4,953 (4,778–4,977) | 5,017 (4,209–5,018) | 4,977 (4,961–4,991) |
| MultiGet 100 | Disk | 1,132 (1,131–1,190) | 1,191 (1,147–1,230) | 1,164 (1,142–1,185) |
| MultiGet 100 | Bloom | 4,791 (4,750–4,814) | 4,802 (4,781–4,819) | 4,779 (4,767–4,826) |
| MultiGet 1,000 | Full | 485 (436–486) | 491 (453–491) | |
| MultiGet 1,000 | Disk | 112 (102–116) | 109 (105–111) | |
| MultiGet 1,000 | Bloom | 465 (453–466) | 466 (449–471) | |

The two comparisons outside the spread, run again:

| comparison | first set, before → after | second set, before → after |
|---|--:|--:|
| MultiGet 1,000, Bloom, 1 task, p50 | 2.638 (2.631–2.638) → 2.647 (2.644–2.657) ms, +0.4% | 2.638 (2.631–2.650) → 2.658 (2.653–2.675) ms, +0.8% |
| cold hit, Bloom, 11 tasks, ops/s | 28,680 (28,434–28,710) → 27,599 (27,560–28,364), −3.8% | 28,667 (28,603–28,742) → 28,495 (27,151–28,658), −0.6% |

The Bloom-tier MultiGet of 1,000 keys is slower after the change in both
sets: all six after runs take 2.644 ms or more, and five of the six
before runs take 2.638 ms or less. The difference is 10 to 20 µs a
batch, 10 to 20 ns a key. The same batch in the Full tier did not move
(2.54 ms in both). The Bloom-tier cold hit on eleven tasks fell outside
the spread once and inside it the second time, so the evidence does not
separate it from the machine's other load.

Allocations per operation on one task, the median of the three runs. The
counts are unchanged; the requested bytes rose, by the same amount in
every run:

| scenario | allocations, before and after | bytes before | bytes after | difference |
|---|--:|--:|--:|--:|
| warm hit, Full | 34.0 | 138,466 | 138,562 | +96 |
| warm hit, Disk | 39.0 | 140,077 | 140,173 | +96 |
| warm hit, Bloom | 36.0 | 138,610 | 138,705 | +96 |
| cold hit, Full | 72.7 | 363,129 | 371,153 | +8,023 (0.99 page fetches) |
| cold hit, Disk | 77.7 | 364,762 | 372,795 | +8,033 (0.99 page fetches) |
| cold hit, Bloom | 113.1 | 385,003 | 400,983 | +15,980 (1.99 page fetches) |
| miss, every tier | 3.0 to 18.1 | | | 0 |
| MultiGet 100, Full | 3,313 | 15,079,609 | 15,109,350 | +29,741 |
| MultiGet 1,000, Full | 32,197 | 150,213,609 | 150,502,585 | +288,976 |

So a warm hit requests 96 bytes more, a key of a MultiGet about 290
bytes more, and each page a cold read fetches about 8 KiB more, in
allocations that already existed. A miss, which reads only the key page,
is unchanged. GETs and local reads per operation are unchanged in every
scenario. The bench does not say which allocations grew. Equal counts
with larger sizes are the mark of a type that grew, such as a future the
runtime boxes when a task is spawned, which grows with the state an
`async fn` holds across an await.

Beside an idle ordered table, the key-value figures stay within the spread
of the after runs alone in 23 of 24 comparisons. The exception is the
Full-tier warm hit on one task: 12.6 µs (12.6–12.7) beside the ordered
table against 12.5 µs in all three runs alone, inside the spread of the
before runs (12.5–13.3). Allocations per operation beside it match the
after runs alone.

## Scans of the ordered table

p50 on one task, and throughput on 11 tasks, with the reads one operation
makes: GETs from the store, and reads from the local directory under
Disk. Medians of three runs.

| scenario | tier | p50, 1 task | p99, 1 task | ops/s, 11 tasks | GETs | local reads | allocations | KiB allocated |
|---|---|--:|--:|--:|--:|--:|--:|--:|
| scan, warm, 100 rows | Full | 20.5 µs | 26.2 µs | 229,964 | 0 | 0 | 397 | 209 |
| | Disk | 38.4 µs | 46.2 µs | 48,694 | 0 | 2.10 | 404 | 213 |
| | Bloom | 20.9 µs | 26.5 µs | 221,545 | 0 | 0 | 401 | 212 |
| scan, bounded, 10 rows | Full | 16.1 µs | 22.0 µs | 249,256 | 0 | 0 | 213 | 181 |
| | Disk | 32.2 µs | 40.1 µs | 48,035 | 0 | 2.10 | 220 | 185 |
| | Bloom | 16.0 µs | 21.2 µs | 239,180 | 0 | 0 | 217 | 184 |
| scan, cold, 100 rows | Full | 79.2 µs | 93.4 µs | 28,745 | 1.99 | 0 | 495 | 434 |
| | Disk | 97.8 µs | 113.2 µs | 24,756 | 0.99 | 2.10 | 454 | 426 |
| | Bloom | 123.4 µs | 148.5 µs | 18,758 | 3.08 | 0 | 541 | 466 |
| MultiScan, warm, 10 keys | Full | 243.9 µs | 270.0 µs | 21,320 | 0 | 0 | 4,964 | 2,185 |
| | Disk | 375.4 µs | 420.2 µs | 4,959 | 0 | 21.0 | 5,034 | 2,224 |
| | Bloom | 246.8 µs | 266.5 µs | 20,612 | 0 | 0 | 5,004 | 2,214 |
| MultiScan, cold, 10 keys | Full | 550.5 µs | 682.3 µs | 2,604 | 19.8 | 0 | 5,938 | 4,435 |
| | Disk | 529.2 µs | 598.0 µs | 2,568 | 9.90 | 21.0 | 5,519 | 4,346 |
| | Bloom | 730.6 µs | 865.0 µs | 1,665 | 30.7 | 0 | 6,337 | 4,753 |

A cold Scan's reads are the ones RFD 025 counts for a compacted table: in
the Full tier one GET for the value page and one for the order page, in
the Disk tier the value page's GET with the order column read from the
local directory, and in the Bloom tier the key page's GET besides. A
Disk-tier Scan makes about two local reads warm or cold, and that sets
its cost: 38.4 µs warm against 20.5 µs in the Full tier, and on eleven
tasks about 49,000 Scans a second, some 100,000 local reads, the same
blocking-pool handoff ceiling that holds a Disk-tier Get near 135,000 a
second ([the performance sweep](2026-09-26-performance-sweep.md)).

A page of 10 rows costs 78% of a whole run of 100 (16.1 against
20.5 µs warm), so most of a warm Scan is finding the run and decoding its
pages, and little is the rows returned.

A warm MultiScan of 10 keys takes 243.9 µs, 24.4 µs a key, more than 10
Scans one after another would (10 × 20.5 µs). It reads its keys
concurrently on the calling task, which overlaps their reads but spreads
no decoding across cores, so warm it gains nothing over sequential Scans
and pays for the concurrency. Cold, the overlap pays: 550.5 µs for 10
keys against 10 × 79.2 µs one after another, with about two GETs a key in
flight together under `server.batch_fan_out`.

## Loads

The same 2,000,000 rows as the long key-value table, in 4 files:

| table | tier | wall | CPU | allocated | reads | resident index |
|---|---|--:|--:|--:|--:|--:|
| long (key-value) | Full | 4.38 ms | 15.2 ms | 26.3 MiB | 11 | 2.49 MiB |
| ordered | Full | 7.91 ms | 28.8 ms | 136.2 MiB | 15 | 0.51 MiB |
| long (key-value) | Disk | 4.82 ms | 16.9 ms | 26.4 MiB | 11 | 0.23 MiB |
| ordered | Disk | 8.23 ms | 30.3 ms | 136.3 MiB | 15 | 0.36 MiB |
| long (key-value) | Bloom | 4.45 ms | 15.4 ms | 34.3 MiB | 15 | 4.23 MiB |
| ordered | Bloom | 7.88 ms | 29.0 ms | 136.3 MiB | 19 | 0.39 MiB |

Loading the ordered table makes one more read per file than the
key-value table, the order column, as RFD 025 counts for a file of one
row group. It takes about 1.8 times the wall and CPU time and five times
the allocated bytes, for the order checks across 2,000,000 rows. Its
resident index is smaller, because it holds 20,000 distinct keys rather
than 2,000,000. A Disk-tier load that reuses the chunk files takes
1.12 ms for the ordered table and 0.98 ms for the key-value one.

## What this does and does not establish

The key-value path keeps its latency and throughput within the run-to-run
spread of this host in every comparison but one, the Bloom-tier MultiGet
of 1,000 keys on one task, 0.4 to 0.8% slower in two alternating sets.
It keeps its allocation counts, GETs, and local reads exactly. It does
not keep its allocated bytes: 96 bytes more per hit, about 290 per batched
key, and about 8 KiB per page fetched. Those are findings for the
maintainer to weigh against RFD 025's statement that a key-value table
keeps its cost per call. The bytes are deterministic, so any build can
reproduce them with the bench's allocation counter.

An idle ordered table beside the key-value one costs the key-value calls
nothing these runs resolve. A Scan load running beside them would share
the process's cores and GET throughput, as RFD 025 states; these runs do
not measure that contention. The figures come from one host with other
work on it, and from local files, so a cold read here is a copy from the
OS page cache, and a GET's latency on S3 would dominate every cold figure
above.
