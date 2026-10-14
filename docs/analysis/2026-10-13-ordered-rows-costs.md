# Ordered scan costs and the key-value path beside them

Measured 2026-10-13 on one Apple M3 Pro, with an allocation fix measured
again on 2026-10-14. The measurement answers two questions RFD 025
(Ordered rows per key) raises. First, whether the key-value path keeps
its costs per call after the ordered-rows work: warm and cold Get,
MultiGet, and misses, in every residency tier, on a process serving
key-value tables alone and on one that also serves an ordered table.
Second, what a Scan and a MultiScan of an ordered table cost in each
tier, warm and cold.

The key-value path was compared in 30 cells, p50 on one task and
throughput on eleven, for five scenarios in three tiers. In 27 the two
binaries' ranges overlap, and the medians move by at most 5.3%, the
throughput of a 100-key MultiGet in the Disk tier on eleven tasks, which
rose. Three fall outside the spread, and the two on the slow side were
run again:

- The Full-tier warm hit on one task: every after run is faster than
  every before run, by 1 ns at the closest (12.541 against 12.542 µs).
- A 1,000-key MultiGet in the Bloom tier on one task takes 0.4% longer
  (10 µs on 2.64 ms). The second set reproduced it at 0.8%, and after the
  allocation fix three more sets measured it 0.6% slower over nine runs
  per binary, outside the spread in one of the three. It is the one
  slowdown that reproduced.
- Cold-hit throughput in the Bloom tier on eleven tasks fell 3.8% in the
  first set. The second set did not reproduce it (0.6% lower, within its
  spread).

The allocation counter showed a change the timings did not. At `a126a1c`
every key-value call made the same number of allocations as before and
requested more bytes: 96 bytes more per hit, about 290 bytes more per key
of a MultiGet, and about 8 KiB more per page a cold read fetches, the
same in every run. Two `async fn` wrappers in the read path had grown
the futures those allocations hold. `22b04dc` returns the inner futures
from both, and the bytes per call match the baseline's again in every
scenario measured. Serving an idle ordered table beside the key-value one
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
- **Re-measurement.** The allocation fix, `22b04dc`, against a new build
  of `97f39af`, on 2026-10-14 with the load average at 2 to 8:
  `warm-hit`, `cold-hit`, `multi-get-100`, and `multi-get-1000` on the
  long table in every tier on one task, three runs per binary,
  alternating; then `multi-get-1000` on one task in the Bloom tier, and
  again in the Full and Bloom tiers, three runs per binary each,
  alternating.

The runs are reproduced from a checkout of the after commit. The before
binary is built in a worktree of `97f39af`, and runs from there, so its
results record that commit:

```sh
cargo build --release --locked -p icetray-testkit --example bench
git worktree add --detach ../bench-before 97f39af
(cd ../bench-before && CARGO_TARGET_DIR=target \
  cargo build --release --locked -p icetray-testkit --example bench)

DATA=$PWD/target/bench/tables OUT=$PWD/target/bench/results
target/release/examples/bench --prepare --table long,ordered --values random --data-dir $DATA

KV=warm-hit,cold-hit,miss,multi-get-100,multi-get-1000
RUN="--table long --values random --tier full,disk,bloom --tasks 11 --duration 5 --warmup 1 --data-dir $DATA"
for n in 1 2 3; do
  (cd ../bench-before && target/release/examples/bench --scenario $KV $RUN --out-dir $OUT/before-$n)
  target/release/examples/bench --scenario $KV,kv-beside-ordered $RUN --out-dir $OUT/after-$n
done
for n in 1 2 3; do
  target/release/examples/bench --scenario scan-warm,scan-cold,scan-bounded,multi-scan-10,multi-scan-10-cold,load \
    --table long,ordered --values random --tier full,disk,bloom --tasks 11 --duration 5 --warmup 1 \
    --data-dir $DATA --out-dir $OUT/scan-$n
done
```

The re-measurement runs the key-value loop with `--mode single` and
`--scenario warm-hit,cold-hit,multi-get-100,multi-get-1000`, then with
`--scenario multi-get-1000` and `--tier bloom`, then `--tier full,bloom`.

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

The Bloom-tier MultiGet of 1,000 keys on one task, measured again after
the allocation fix, p50 in ms:

| set | before (`97f39af`) | after (`22b04dc`) | difference |
|---|--:|--:|--:|
| among the other key-value scenarios | 2.641 (2.637–2.654) | 2.653 (2.642–3.143) | +0.5%, ranges overlap |
| alone | 2.632 (2.630–2.638) | 2.649 (2.648–2.669) | +0.7%, outside the spread |
| beside the Full-tier batch | 2.659 (2.632–2.661) | 2.656 (2.644–2.672) | −0.1%, ranges overlap |
| all nine runs | 2.638 (2.630–2.661) | 2.653 (2.642–3.143) | +0.6% |

Every after run takes longer than 2.641 ms, and six of the nine before
runs take 2.641 ms or less, so the slowdown of about 15 ns a key remains
with the allocated bytes restored. The larger futures do not account for
it, and these runs do not identify its cause. The Full-tier batch stays
within the spread: 2.548 ms before and 2.543 after among the other
scenarios, and 2.559 before and 2.544 after beside the Bloom-tier batch.

Allocations per operation on one task, the median of three runs. The
counts are the same in all three builds. The bytes and counts are from
the re-measurement; the `a126a1c` column is the first set's difference
against its own before runs, the `22b04dc` column the re-measurement's:

| scenario | allocations | bytes, `97f39af` | `a126a1c` | `22b04dc` |
|---|--:|--:|--:|--:|
| warm hit, Full | 34.0 | 138,465 | +96 | +0 |
| warm hit, Disk | 39.2 | 140,094 | +96 | −1 |
| warm hit, Bloom | 36.0 | 138,609 | +96 | +0 |
| cold hit, Full | 72.8 | 363,145 | +8,023 | +5 |
| cold hit, Disk | 77.7 | 364,749 | +8,033 | +2 |
| cold hit, Bloom | 113.3 | 385,027 | +15,980 | +9 |
| MultiGet 100, Full | 3,304 | 15,078,607 | +29,741 | +4 |
| MultiGet 100, Disk | 3,815 | 15,243,654 | +29,748 | +102 |
| MultiGet 100, Bloom | 3,504 | 15,093,014 | +29,679 | −5 |
| MultiGet 1,000, Full | 32,181 | 150,211,661 | +288,976 | +195 |
| MultiGet 1,000, Disk | 37,191 | 151,859,381 | +288,020 | −445 |
| MultiGet 1,000, Bloom | 34,184 | 150,355,458 | +288,954 | +0 |

At `a126a1c` a warm hit requested 96 bytes more, a key of a MultiGet
about 290 bytes more, and each page a cold read fetches about 8 KiB
more (0.99 page fetches per cold hit, 1.99 in the Bloom tier), in
allocations that already existed. A miss, which reads only the key page,
was unchanged then and was not measured again. At `22b04dc` the
differences are at most 9 bytes for a one-key call and under a byte per
key of a MultiGet, the size of the variation between runs of one binary,
which the rare allocations inside a window move. GETs and local reads
per operation are the same in every scenario and build.

A bisection of the branch, with an exact-size histogram of the
allocations, located the growth in two `async fn` wrappers. An `async
fn` keeps its arguments in its state beside the state of the future it
awaits, so a wrapper that only awaits another future holds that
future's inputs a second time:

- `Run::fetch`, the page cache's run of a caller's fetch, became an
  `async fn` awaiting `Run::run`, so its future held one more copy of
  the caller's fetch future, about 2.4 KiB for a hedged read. foyer
  stores that future three times for each page it fetches, once in the
  boxed fetch builder and twice in the boxed fetch, which is the 8 KiB a
  cold page fetch gained.
- `Storage::read_range_hedged` became an `async fn` around
  `read_span_hedged`, 48 bytes more in the read future. Every lookup
  future lays out two of them, for the value page and the dictionary
  page, warm hits included, which is the 96 bytes a hit gained. A
  batched key's future is stored three times, in the batch's buffer, its
  `FuturesUnordered` node, and the generation's vector of per-file
  lookups, which is the 290 bytes a key gained.

`22b04dc` makes both plain functions that return the inner future. Two
unit tests in `icetray-core` pin the shapes: `Run::fetch`'s future holds
at most two copies of the caller's fetch future, and
`read_range_hedged`'s future is no larger than `read_span_hedged`'s.

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
local directory, and in the Bloom tier the key page's GET besides.

A Disk-tier Scan makes about two local reads warm or cold and takes
38.4 µs warm against 20.5 µs in the Full tier; the inference, without a
profile, is that the local reads make most of the difference. On eleven
tasks the Disk tier serves about 49,000 Scans a second, about 102,000
local reads, against 130,000 to 135,000 local reads a second for a
Disk-tier Get, which makes one (the warm hit and the miss above). The
performance sweep traced a Disk-tier Get's ceiling near 135,000 a second
to the handoff of each local read to the blocking pool
([the performance sweep](2026-09-26-performance-sweep.md)). The Scan's
rate is consistent with a handoff limit as well; these runs do not
establish that it is the same one.

A page of 10 rows costs 78% of a whole run of 100 (16.1 against
20.5 µs warm). The inference, without a profile, is that finding the run
and decoding its pages take most of a warm Scan, and the rows returned
little.

A warm MultiScan of 10 keys takes 243.9 µs, 24.4 µs a key, more than 10
Scans one after another would (10 × 20.5 µs). A MultiScan reads its keys
concurrently on the calling task, which overlaps their reads and keeps
their decoding on one core. The inference from that design, without a
profile, is that warm it gains nothing over sequential Scans and pays
for the concurrency. Cold, the overlap pays: 550.5 µs for 10
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
the allocated bytes; the inference, without a profile, is that the order
checks across 2,000,000 rows account for the difference. Its
resident index is smaller, because it holds 20,000 distinct keys rather
than 2,000,000. A Disk-tier load that reuses the chunk files takes
1.12 ms for the ordered table and 0.98 ms for the key-value one.

## What this does and does not establish

The key-value path keeps its latency and throughput within the run-to-run
spread of this host in every comparison but one, the Bloom-tier MultiGet
of 1,000 keys on one task: 0.4 to 0.8% slower in the first two sets, and
0.6% slower over nine runs per binary after the allocation fix. Its cause
is not identified; it is a finding for the maintainer to weigh against
RFD 025's statement that a key-value table keeps its cost per call. The
path keeps its allocation counts, GETs, and local reads exactly, and at
`22b04dc` its allocated bytes per call as well. The bytes are
deterministic, so any build can check them with the bench's allocation
counter, and the two size tests fail on a wrapper of the kind that grew
them.

An idle ordered table beside the key-value one costs the key-value calls
nothing these runs resolve. A Scan load running beside them would share
the process's cores and GET throughput, as RFD 025 states; these runs do
not measure that contention. The figures come from one host with other
work on it, and from local files, so a cold read here is a copy from the
OS page cache, and a GET's latency on S3 would dominate every cold figure
above.
