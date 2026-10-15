# Order fences measured

Measured 2026-10-15 on one Apple M3 Pro. The branch `feat/order-fences`
replaces the writer's order column index with order fences, one pair of
key and order value per order page, which the load records and holds
resident. A Scan places its bounds by the fences, and the uniqueness merge
chooses its pages by them and reads them a row group of each file at a
time. The branch also adds the response bound `server.max_scan_bytes`,
fixes to how concurrent span reads claim pages, a direct reader for plain
byte-array value pages, and the skipping of whole pages of deleted rows.
This measurement compares the branch head with the design before it,
`39b8a7c`, and tests four claims RFD 025 (Ordered rows per key) makes for
the fences: a bounded Scan takes one round to object storage whatever the
writer's column index holds, a Scan's work follows its limit and not its
run's length, the merge reads only the pages at the keys files share, and
the fences take less memory than the column index they replace. It also
checks that the key-value path keeps its costs.

Three of the four claims hold as stated, and the fourth holds for integer
and time order columns:

- **One round.** A cold bounded Scan over a string order column whose
  column index the writer cut to 16 bytes took 2.00 rounds under `full`
  and 2.90 under `bloom`. It now takes 0.99 and 1.98, the counts of a
  timestamp-ordered run, which did not change (0.99 and 1.96). Its p50
  on one task fell from 565 to 132 µs under `full`. The one round reads
  more bytes: about 90 KB more per Scan under `full` and `bloom`, a
  value page past the bound's order page.
- **Work follows the limit.** A warm Scan of 10 rows from the middle of
  a 10,000-row run took 79.5 µs and 1,147 allocations; it now takes
  24.2 µs and 117, against 11.9 µs and 107 for the same request on a
  100-row run. Over the string order column the same Scan fell from
  445 µs to 44 µs, and its throughput on eleven tasks rose 9.5 times.
- **Edge-only merge reads.** A first load of files rolled by size fell
  from 61.7 to 11.8 ms under `full`, and a `disk` restart over its chunk
  files from 55.3 to 6.4 ms, reading 0.33 MiB from the chunk files where
  it read 6.23 MiB. A refresh after an append that overlaps the last one
  makes 29 GETs where it made 427 (41 where it made 837 under `bloom`),
  and its time did not change: two thirds of a profile's busy samples
  fall in the merge.
- **Fence memory.** Over a timestamp order column the fences take 16
  bytes per order page, 32 KB per table against 82 KB for the column
  index, and the resident index is 6 to 9% smaller. Over the string column
  the fences take 159 KB against the truncated index's 146 KB, 9% more,
  and the chunk files grow 5%.

The merge's read waves are a decision left open since the streaming
merge, `77aed9c`. The writer recipe in the
[table layout](../table-layout.md) sets 128 MiB row groups and 32 to
64 MiB data files, and advises `large_file` above 128 MiB, so a file
written to it holds one row group, as every bench table's file does.
There a first load and a refresh wait on the same rounds of GETs as
before, and the waves cost nothing. The decision concerns tables
outside the recipe, such as files at Iceberg's default 512 MiB target.
Over the same rows in row groups of 25,000 rows, about four per file,
the appended table's first load waits on 61 rounds where it waited on
9: at 30 ms a round about 1.6 s more, and 5.2 s at 100 ms.

The key-value path is slower in none of its 48 comparisons, faster
outside the spread in 20, and makes 6 fewer allocations per value read.

## Method

- **Host.** Apple M3 Pro, 11 cores (5 performance, 6 efficiency), 36 GiB,
  macOS 26.6.2 (Darwin 25.6.0, arm64). Other work shared the machine: the
  one-minute load average read 6 to 15 during the runs, so every
  comparison alternated its two binaries run by run.
- **Binaries.** Both are `crates/icetray-testkit/examples/bench.rs` built
  with the `profiling` profile (release with debug info).
  - Before is the baseline binary, the bench at `2eae81e` over the
    library code of `39b8a7c` plus the reporting-only `resident_parts`
    accessor and a test fixture's knob that `2eae81e` adds to
    `icetray-core`; no Scan, merge, or load path differs from `39b8a7c`.
  - After is `bfe2e39`, the bench with load rounds over the library code
    of `6c39252`, the branch head. The main set ran this binary;
    `821820f` changes only the bench, so the reproduction below builds
    it, over the same library.
  - The load-rounds set needs the bench's load rounds and held bytes on
    both sides. Its before binary is `2eae81e` with the two bench commits
    `bfe2e39` and `821820f` applied, built in a scratch worktree, over
    the same library as the main set's before binary. That it carries
    the old library shows in its figures: the rolled table's first load
    under `full` takes 60.8 ms there, as the main set's before binary
    takes 61.7 ms, where the after binary takes 11.5. Its after binary
    is `821820f`.
- **Serving.** The bench serves tables through `Table` over `Storage`'s
  `file://` store, so no figure includes object-store latency, and a
  cold read is a copy from the OS page cache. An ordered table's reads
  go through `RoundsStore`, which places each GET in the round after the
  deepest round answered before it was issued, by the order of issues
  and answers alone. jemalloc runs behind an allocation counter, with the
  Prometheus recorder installed. The warm cache is 1 GiB and the cold
  one 1 MiB.
- **Tables.** The key-value table `long` holds 2,000,000 keys of 100
  random bytes in 4 files. Every ordered table holds 2,000,000 rows of
  100-byte random values with a timestamp order column unless named
  otherwise, in the contract layout (1,000-row pages, zstd):
  - `ordered`: 20,000 keys of 100 rows, 4 files;
  - `ordered_long`: 200 keys of 10,000 rows, 4 files;
  - `ordered_string`: the same rows with a string order column zero-padded
    to 83 bytes, so every value shares at least 64 leading bytes, and the
    writer's column index cut to 16 bytes, 6 files;
  - `ordered_rolled`: the `ordered_long` rows in files rolled at 8 MiB,
    28 files whose neighbours share edge keys;
  - `ordered_appended`: 20,000 keys, a first commit of 20 rows per key and
    four appends that each overlap the one before in order range, 20
    files, and a fifth overlapping append for the refresh.

  Every file of every bench table is one row group. For the read waves,
  `--row-group-rows 25000` writes the same rows in row groups of 25,000
  rows: `ordered_rolled` as 30 files in 105 row groups and
  `ordered_appended` as 24 files in 102, counting the refresh's append;
  a first load reads 28 and 20 of those files.
- **Main set.** The baseline's run set, three runs per binary, alternating:
  `warm-hit`, `cold-hit`, `miss`, and `multi-get-100` on `long`; the
  `ordered` Scan and MultiScan scenarios and `kv-beside-ordered`; the
  long-run and string scenarios; and three loads and refreshes of every
  table, in every tier. Each lookup scenario has a 1 s warm-up and a 5 s
  window, on one task and on eleven. Lookup figures are medians of three
  runs, with the lowest and highest in parentheses; a difference falls
  outside the spread when the two binaries' ranges do not overlap, that
  is, when all three runs of one binary fall on one side of all three of
  the other's. Load figures are medians of rounds 2 and 3 of each run,
  six per binary, with their range read the same way; each run's first
  load is left out. Every figure is computed by that rule from the JSON
  reports the bench writes.
  Rounds are counted over 100 Scans run one at a time before each
  window, on one task only.
- **Load-rounds set.** `load,refresh` of `ordered_rolled` and
  `ordered_appended`, every tier, three loads per run, three runs per
  binary alternating, over the bench tables and over the same rows in
  row groups of 25,000. The merge's held bytes come from the load's
  `held_bytes` debug event, which only the after library emits.
- **MultiScan set.** `multi-scan-100` and `multi-scan-1000`, warm and
  cold, every tier, three runs of the after binary; these scenarios
  postdate the baseline. CPU per key is the user and system time of a
  16 s run less a 4 s run, over the operations the longer run added, on
  one task in the Full tier, three times each, for both binaries at 10
  keys and the after binary at 100 and 1,000.
- **Profiles.** samply at 1 kHz on the after binary, one task: 20 s of
  `scan-bounded-long` and `scan-bounded-string` under `full` and of
  `scan-bounded` under `disk`, shares of the samples inside the
  operation; 400 first loads of `ordered_rolled` under `full`, and 80
  loads and refreshes of `ordered_appended`, shares of the busy samples
  of every thread.

The runs are reproduced from a checkout of `821820f`. The before binaries
are built in a worktree of `2eae81e`; the main set's baseline is that build
without the bench patch, and the load-rounds baseline is that build with
it:

```sh
ROOT=$PWD
F="flox activate --"
$F cargo build --profile profiling --locked -p icetray-testkit --example bench
mkdir -p target/bench/after && cp target/profiling/examples/bench target/bench/after/

git diff 6c39252 821820f -- crates/icetray-testkit/examples/bench.rs > $ROOT/target/bench.patch
git worktree add --detach ../bench-before 2eae81e
cd ../bench-before
$F cargo build --profile profiling --locked -p icetray-testkit --example bench
mkdir -p $ROOT/target/bench/before && cp target/profiling/examples/bench $ROOT/target/bench/before/
git apply $ROOT/target/bench.patch
$F cargo build --profile profiling --locked -p icetray-testkit --example bench
mkdir -p $ROOT/target/bench/before-rounds && cp target/profiling/examples/bench $ROOT/target/bench/before-rounds/
cd $ROOT && git worktree remove --force ../bench-before

A=target/bench/after/bench; B=target/bench/before/bench; DATA=target/bench/tables
$A --prepare --table long,ordered,ordered-long,ordered-string,ordered-rolled,ordered-appended \
  --values random --data-dir $DATA
$A --prepare --table ordered-rolled,ordered-appended --values random \
  --data-dir target/bench/tables-rg --row-group-rows 25000

COMMON="--values random --tier full,disk,bloom --tasks 11 --duration 5 --warmup 1 --data-dir $DATA"
set_of() {  # binary, output directory
  $1 --scenario warm-hit,cold-hit,miss,multi-get-100 --table long $COMMON --out-dir $2/kv
  $1 --scenario scan-warm,scan-cold,scan-bounded,multi-scan-10,multi-scan-10-cold,kv-beside-ordered \
    --table ordered $COMMON --out-dir $2/ordered
  $1 --scenario scan-bounded-long,scan-bounded-long-cold,scan-head-long,scan-bounded-string,scan-bounded-string-cold \
    --table ordered-long,ordered-string $COMMON --out-dir $2/scan-long
  $1 --scenario load,refresh --table long,ordered,ordered-long,ordered-string,ordered-rolled,ordered-appended \
    --load-rounds 3 $COMMON --out-dir $2/load
}
for n in 1 2 3; do
  set_of $B target/bench/results/before-$n
  set_of $A target/bench/results/after-$n
done

LOADS="--scenario load,refresh --table ordered-rolled,ordered-appended --values random --tier full,disk,bloom --load-rounds 3"
for n in 1 2 3; do
  for side in before-rounds after; do
    target/bench/$side/bench $LOADS --data-dir $DATA --out-dir target/bench/results/$side-one-$n
    target/bench/$side/bench $LOADS --data-dir target/bench/tables-rg --row-group-rows 25000 \
      --out-dir target/bench/results/$side-rg-$n
  done
done

for n in 1 2 3; do
  $A --scenario multi-scan-100,multi-scan-100-cold,multi-scan-1000,multi-scan-1000-cold --table ordered \
    $COMMON --out-dir target/bench/results/multi-scan-$n
done
ONE="--table ordered --tier full --mode single --warmup 1 --values random --data-dir $DATA"
for n in 1 2 3; do
  for run in "before $B 10" "after $A 10" "after $A 100" "after $A 1000"; do
    set -- $run
    for d in 4 16; do
      /usr/bin/time -p $2 --scenario multi-scan-$3 $ONE --duration $d \
        --out-dir target/bench/results/cpu-$1-$3-$d-$n
    done
  done
done

PROF="--mode single --duration 20 --warmup 2 --values random --data-dir $DATA"
$F samply record -- $A --scenario scan-bounded-long --table ordered-long --tier full $PROF
$F samply record -- $A --scenario scan-bounded-string --table ordered-string --tier full $PROF
$F samply record -- $A --scenario scan-bounded --table ordered --tier disk $PROF
$F samply record -- $A --scenario load --table ordered-rolled --tier full --load-rounds 400 \
  --values random --data-dir $DATA
$F samply record -- $A --scenario load,refresh --table ordered-appended --tier full --load-rounds 80 \
  --values random --data-dir $DATA
```

## One round for a bounded Scan

Rounds of GETs per cold Scan of 10 rows from the middle of a 10,000-row
run, newest first, on one task, mean over 100 Scans with the most any
one took. The figures were the same in all three runs of each binary:

| table | tier | before | after | p50 before | p50 after | store bytes before | after |
|---|---|--:|--:|--:|--:|--:|--:|
| `ordered_string` | Full | 2.00 (2) | 0.99 (1) | 565.2 µs | 132.4 µs | 118,056 | 208,222 |
| | Disk | 0.97 (1) | 0.97 (1) | 557.6 µs | 161.5 µs | 117,718 | 106,850 |
| | Bloom | 2.90 (3) | 1.98 (2) | 644.9 µs | 199.0 µs | 118,397 | 208,958 |
| `ordered_long` | Full | 0.99 (1) | 0.99 (1) | 144.9 µs | 103.5 µs | 212,582 | 211,619 |
| | Disk | 0.99 (1) | 0.98 (1) | 155.0 µs | 127.7 µs | 211,732 | 110,511 |
| | Bloom | 1.96 (2) | 1.96 (2) | 209.0 µs | 172.3 µs | 213,364 | 212,375 |

Store bytes are the bytes GETs requested plus the bytes read from the
local directory, per Scan.

The claim holds. Over the truncated string index the old design read the
order page first and the value pages after it, a second round; under
`bloom` the key-page round came first, so three. With the fences the
string Scan takes the rounds of the timestamp one: one under `full` and
`disk`, and under `bloom` the key-page round and the row round that RFD
025 counts. A mean below one counts Scans that made no GET; the
inference is that the 1 MiB cold cache still held their pages.

The single round costs bytes. Under `full` and `bloom` the cold string
Scan requests about 90 KB more than before, about one value page of
1,000 rows of 100-byte values. A Scan that cannot decode the bound's
order page before it reads values takes the value rows of that whole
page in the same round, which is the bound RFD 025 states ("at most the
rows of the order page that holds the bound"). The timestamp Scan
already read that much under the column index, so its bytes did not
change. Under `disk` the order page is local and decoded first, and both
Scans now read only the value pages of the rows taken: 110,511 bytes for
the timestamp Scan where it read 211,732.

A Scan's p50 fell in every row, most of all over the string order column,
where the old design decoded the run's order pages to place the bound.

## Work follows the limit

Warm Scans on one task, newest first, Full tier (Bloom within 3% of it):

| Scan | run | before p50 | after p50 | allocations before → after | KiB allocated before → after |
|---|---|--:|--:|--:|--:|
| 10 rows, bounded | 100 rows | 15.8 µs | 11.9 µs | 213 → 107 | 181 → 161 |
| 10 rows, bounded | 10,000 rows | 79.5 µs | 24.2 µs | 1,147 → 117 | 561 → 253 |
| 10 rows, bounded, string order | 10,000 rows | 445.4 µs | 44.3 µs | 5,192 → 128 | 3,536 → 378 |
| 100 newest rows, no bound | 10,000 rows | 29.8 µs | 23.9 µs | 408 → 311 | 237 → 220 |
| whole run, 100 rows | 100 rows | 20.7 µs | 16.3 µs | 397 → 302 | 209 → 197 |

On eleven tasks, the bounded Scan of the long run went from 83,719 to
243,720 a second, and over the string order column from 16,008 to
152,070.

The claim holds in what the counters measure. The old Scan built a
piece per page of the run (the branch review's probe measured 106 µs of
a warm read at 10,001 pages, 99 of them in that step), so its
allocations grew with the run: 1,147 for the long run against 213 for
the short one. With the fences a bounded Scan makes 117 allocations on
the long run and 107 on the short one, and the unit test that walks runs
of 10 and 10,000 pages finds the same pages walked and a binary search
of 5 against 15 steps
(`test_read_run_warm_work_follows_the_limit_not_the_run`, which passes
in this tree).

The bounded Scan of the long run still takes twice the time of the same
Scan on a short run, 24.2 against 11.9 µs, and allocates 91 KiB more,
about one decompressed page. In the profile below, 28.6% of its samples
fall in decoding the run's first and last key pages and 26.5% in the
bound's order page. The inference from that, without a profile of the
short-run Scan, is that a run of ten pages has its first and last key on
two pages, which the Scan decompresses separately, where a 100-row run's
ends share one page. That cost is fixed per Scan whatever the run's
length.

## The uniqueness merge

Loads, refreshes, and Disk restarts of the two tables whose files meet,
medians of six, with the time's range:

| table | tier | what | wall before | wall after | CPU ms | GETs | GET MiB | local reads | local MiB |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| `ordered_rolled` | Full | first load | 61.7 (61.4–64.0) | 11.8 (11.5–12.4) | 122.2 → 65.5 | 171 → 191 | 26.2 → 20.6 | | |
| | Disk | first load | 63.4 (62.9–64.5) | 14.4 (14.0–14.9) | 136.1 → 86.6 | 143 → 143 | 20.3 → 20.3 | 56 → 96 | 6.23 → 0.33 |
| | Disk | restart | 55.3 (54.7–55.6) | 6.4 (6.2–6.8) | 64.4 → 16.1 | 31 → 31 | 14.0 → 14.0 | 56 → 96 | 6.23 → 0.33 |
| | Bloom | first load | 61.8 (61.0–62.7) | 11.3 (11.2–11.6) | 123.2 → 67.7 | 227 → 267 | 26.5 → 20.6 | | |
| | Full | refresh | 1.8 (1.6–1.9) | 1.5 (1.5–1.6) | 2.1 → 2.0 | 15 → 15 | 1.08 → 1.08 | | |
| | Disk | refresh | 2.0 (1.9–2.1) | 2.0 (1.8–2.0) | 2.7 → 2.7 | 15 → 15 | 1.08 → 1.08 | 0 → 0 | |
| | Bloom | refresh | 1.9 (1.6–1.9) | 1.5 (1.5–1.5) | 2.2 → 2.0 | 17 → 17 | 1.08 → 1.08 | | |
| `ordered_appended` | Full | first load | 48.6 (47.7–49.9) | 49.5 (49.4–50.6) | 86.2 → 83.5 | 83 → 83 | 11.8 → 11.8 | | |
| | Full | refresh | 17.1 (16.8–17.3) | 17.5 (17.3–17.9) | 22.4 → 22.2 | 427 → 29 | 2.60 → 2.60 | | |
| | Disk | restart | 44.1 (43.9–44.6) | 46.4 (45.6–47.4) | 49.3 → 51.6 | 27 → 27 | 10.0 → 10.0 | 32 → 32 | 0.95 → 0.95 |
| | Disk | refresh | 22.2 (22.0–22.4) | 17.6 (17.4–18.2) | 28.9 → 23.1 | 21 → 21 | 2.28 → 2.28 | 812 → 16 | 0.48 → 0.48 |
| | Bloom | first load | 48.0 (47.6–49.2) | 49.7 (49.6–49.9) | 88.6 → 84.5 | 119 → 119 | 12.2 → 12.2 | | |
| | Bloom | refresh | 17.2 (17.0–18.2) | 17.7 (17.3–17.9) | 22.6 → 22.2 | 837 → 41 | 2.79 → 2.79 | | |

Times are in milliseconds. For comparison, the compacted `ordered_long`,
the same rows in 4 files, loads in 10.6 ms under `full` (12.1 before)
and restarts under `disk` in 1.3 ms (1.2 before). The peak resident set
grew by at most 14 MiB in any of these loads on either binary, and by
about the same on both: 13.9 MiB before and 13.3 after for the rolled
table's first load under `full`, under 1 MiB for every load of the
appended table.

The claim holds for the files a compaction rolls by size. The merge of
`ordered_rolled` reads the pages at the edge keys neighbouring files
share, and a restart over its chunk files reads 0.33 MiB of them where
it read every joined file's key and order chunks, 6.23 MiB. The first
load fetches 5.6 MiB less. It does so in more requests: 191 GETs where it
made 171 under `full`, 267 where it made 227 under `bloom`, and 96 local
reads where it made 56 under `disk`. The inference from the merge's
code is that a file in the middle of a chain reads two runs of pages per
column, its first page and its last, where the old merge read its chunk
whole in one. The rolled table's first load still fetches 12.3 MiB more
than the compacted one. The load prefetches 512 KiB of each file's
footer, and the rolled table has 24 more files, 12 MiB, so the inference
is that the footers, not the merge, account for it. The rolled table's
refresh appends files of new keys, which meet no other file and are
read no further; it makes the same reads as before.

The appended table shows the claim's other half. Every key there has
rows in every append, so every page of every file can hold a pair
another file holds, the merge chooses them all, and its first load and
restart read what they read before. Its refresh reads the same 2.60 MiB
in 29 GETs where it made 427, and under `disk` it makes 16 local reads
where it made 812. The inference from the code is that the merge now
fetches a run of consecutive chosen pages in one GET where the old merge
fetched each page alone. The refresh's
time under `full` and `bloom` did not move with its GETs. Of the busy
samples of 80 loads and refreshes of this table, 66% fall in the merge's
CPU: `MergeState::place` 21.7%, key comparisons (`Keys::cmp_at`) 20.3%
self, and the heap's pops 17.1%. On local files a GET is a copy from the
OS page cache, so 400 fewer cost little here; against S3, each was a
request billed on its own, and they were issued `load_concurrency()` at a
time.

Three of the appended table's rows fall outside the spread on the slow
side by 2 to 5%: the Full refresh (+2.3%), the Disk restart (+5.2%), and
the Bloom first load (+3.5%) in the main set. The load-rounds set ran the
same loads three more times per binary, and there none of the three is
outside the spread: 16.8 against 17.2 ms, 43.3 against 44.4 ms, and 47.5
against 47.2 ms. These runs do not establish a slowdown.

### Held bytes

The merge's `held_bytes` in a first load, the most bytes of chosen pages
it held at once, from the load-rounds set, over the first load's 20
appended and 28 rolled files; the before library has no such counter:

| table | tier | one row group per file | row groups of 25,000 rows |
|---|---|--:|--:|
| `ordered_appended` | Full | 164.1 KiB | 40.9 KiB |
| | Disk, Bloom | 245.6 KiB | 62.4 KiB |
| `ordered_rolled` | Full | 106.3 KiB | 88.1 KiB |
| | Disk, Bloom | 110.1 KiB | 91.1 KiB |

The claim that the merge holds at most one row group's chosen pages per
file holds: with four row groups per appended file it holds a quarter of
what it holds with one. Under `disk` and `bloom` the figure includes the
key pages the chosen order pages span. The old merge read every chosen
page of a group before it merged; the inference from that code, without
a counter, is that it held what the one-row-group column shows whatever
the files' row groups, and so four times the 25,000-row figures of the
appended table. The branch review's probe of the old merge, under
`bloom` with 50 overlapping files of 1 or 4 row groups each, measured
36,896 bytes held before the first comparison, against 8,249 bytes for
one row group per file. The unit test of that shape
(`test_verify_rows_holds_one_row_group_of_chosen_pages_per_file`) holds
the new merge between the sum of each file's first row group and the sum
of each file's largest, and passes in this tree.

## The merge's read waves

The streaming merge reads each file's next row group when that file's
cursor runs dry, and it does not prefetch, so a merge over files of
several row groups waits on a round for each wave of row groups it
reads. Rounds of GETs per load or refresh, medians of six, from the
load-rounds set; under `full` and `disk` every load took the median's
count, and under `bloom` the count varied by up to three rounds between
loads:

| table | tier | what | one row group per file | row groups of 25,000 rows | added at 30 ms | added at 100 ms |
|---|---|---|--:|--:|--:|--:|
| `ordered_appended` | Full | first load | 9 → 9 | 9 → 61 | +1.56 s | +5.2 s |
| | Full | refresh | 10 → 10 | 10 → 36 | +0.78 s | +2.6 s |
| | Bloom | first load | 13.5 → 13.5 | 16 → 66 | +1.50 s | +5.0 s |
| | Bloom | refresh | 12 → 12 | 12.5 → 37 | +0.74 s | +2.5 s |
| | Disk | first load, refresh | 5 → 5, 6 → 6 | 5 → 5, 6 → 6 | 0 | 0 |
| `ordered_rolled` | Full | first load | 10 → 10 | 10 → 30 | +0.60 s | +2.0 s |
| | Bloom | first load | 17.5 → 18 | 18 → 38.5 | +0.62 s | +2.1 s |
| | Disk | first load | 6 → 6 | 6 → 6 | 0 | 0 |

The added latency multiplies the added rounds by the round's latency. It
is an estimate of a floor: it assumes one round trip per round and leaves
out the time each GET takes to transfer its bytes. Under `disk` the merge
reads its pages from the chunk files, so its waves are local reads,
which these rounds do not count.

On local files each added round costs about 50 µs. Over the row groups
of 25,000 rows the appended table's first load under `full` takes
50.4 ms (50.1–50.6) where it took 47.8 (47.6–48.1), 2.6 ms for 52 more
rounds; its refresh takes 18.6 ms (18.3–31.7) where it took 17.2
(17.0–17.7) under `full`, and 18.5 (18.2–18.8) where it took 17.1
(17.0–17.3) under `bloom`; and its Disk restart takes 46.0 ms
(45.8–49.2) where it took 44.1 (44.0–44.3). All four fall outside the
spread; the Bloom first load, 48.1 (47.7–50.6) against 50.4
(50.1–50.6), does not. The GETs fall where the rounds rise: the
appended refresh makes 81 where it made 466 (132 where it made 902
under `bloom`), and the rolled first load 341 where it made 396 (492
where it made 602 under `bloom`), while the appended first load makes
265 on both binaries. The rolled first load is still 4.8 times faster,
12.8 ms against 61.4.

The writer recipe in the [table layout](../table-layout.md) sets
128 MiB row groups and data files of 32 to 64 MiB, and the doctor
advises `large_file` above 128 MiB. A file written to it holds one row
group, as every bench table's file does, so the merge reads one wave per
overlap group, the round count equals the old design's, and the read
waves cost nothing. The decision concerns tables outside the recipe,
such as files written at Iceberg's default 512 MiB target with 128 MiB
row groups, about four row groups per file, the shape of the 25,000-row
set. There the appended table's first load waits on 52 more rounds,
across 4 overlap groups of 5 files and about 21 row groups each. The
inference from the counts, without a trace, is that most row groups
after a file's first start a wave of their own, because the merge
pauses whenever any one cursor waits and the files' row-group
boundaries fall at different pairs. The overlap groups are merged one
after another, so their waves add up.

An S3 GET of a file read moments before, as a load's later waves read
the files its first wave opened, took 26 to 32 ms at the median
in-region, and of a file read hours before about 90 ms (the
[range-GET measurements](2026-09-29-cold-range-get-latency.md)). At
30 ms a round, the appended table's first load waits about 1.8 s on GETs
where the old design waited about 0.3 s. A prefetch of each file's next
row group, or a byte budget for read-ahead, would bring the waves back
toward one per group. A prefetch holds two row groups' chosen pages per
file, so the inference from the held-bytes table is that it would hold
about twice the 25,000-row column, about 82 KiB for the appended table
under `full`, and it would replace RFD 025's bound of at most one row
group's chosen pages per file with two.

## Fence memory and the chunk files

Resident index bytes of the order column's part, per table; the other
parts did not change, and the figure is the same in every tier:

| table | order column index, before | order fences, after | resident index, Full, before → after | chunk files, Disk, before → after |
|---|--:|--:|--:|--:|
| `ordered` | 82,082 | 32,288 | 535,058 → 485,264 | 932,566 → 972,622 |
| `ordered_long` | 82,000 | 32,256 | 680,240 → 630,496 | 6,551,760 → 6,591,777 |
| `ordered_rolled` | 82,000 | 33,792 | 787,280 → 739,072 | 6,557,557 → 6,597,669 |
| `ordered_appended` | 82,410 | 33,440 | 860,165 → 811,195 | 1,163,377 → 1,203,657 |
| `ordered_string` | 146,096 | 159,384 | 771,080 → 784,368 | 3,060,558 → 3,219,594 |

Over a timestamp order column the fences take 16 bytes per order page and
64 bytes per row group: `ordered_long`'s 2,000 order pages in 4 files are
32,000 + 256 bytes, and `ordered_rolled`'s 2,000 in 28 files 32,000 +
1,792. The column index took about 41 bytes per page. The resident index
falls by 48,200 to 49,800 bytes per table, 5.7 to 9.3%.

Over the string order column a fence keeps the key, the value's end, and
the value cut one byte past the prefix it shares with the row before it.
These values share their first 67 bytes or more, so a fence takes about
80 bytes, and the fences take 13,288 bytes more than the column index the
writer cut to 16 bytes. The claim that fences cost less than the index
holds for integer and time order values, whose fences are exact longs,
and not for string values with long shared prefixes, whose fences grow
with the prefix, where the cut index held each page's minimum and
maximum to 16 bytes. What the fences buy there is the placement the cut
index could not give: the one round and the 10-fold faster warm Scan
above.

The chunk files hold the fences too, so a restart restores them without a
read. Their bytes grow by 40,017 for `ordered_long`, 0.6%, whose fences
are 32,256 bytes resident, and by 159,036 for the string table, 5.2%.

## The key-value path

The key-value scenarios on `long`, alone and beside an idle ordered
table, 48 comparisons. None is slower outside the spread; 20 are faster
outside it and 28 overlap. p50 on one task:

| scenario | tier | before | after | difference |
|---|---|--:|--:|---|
| warm hit | Full | 12.75 (12.71–12.75) | 11.46 (11.46–11.50) | −10.1%, faster |
| | Disk | 21.62 (21.25–22.29) | 19.46 (19.33–19.54) | −10.0%, faster |
| | Bloom | 13.17 (13.04–13.54) | 11.83 (11.83–11.83) | −10.1%, faster |
| cold hit | Full | 65.33 (64.88–65.92) | 63.79 (63.71–63.92) | −2.4%, faster |
| | Disk | 75.79 (75.38–76.33) | 73.46 (73.42–73.75) | −3.1%, faster |
| | Bloom | 107.29 (106.33–107.42) | 106.25 (106.25–106.83) | −1.0%, overlaps |
| miss | Full | 6.50 (6.50–6.58) | 6.54 (6.54–6.67) | +0.6%, overlaps |
| | Disk | 14.21 (14.12–14.29) | 14.33 (14.25–14.33) | +0.9%, overlaps |
| | Bloom | 0.21 (0.21–0.21) | 0.21 (0.21–0.21) | overlaps |
| MultiGet 100 | Full | 324.62 (306.00–325.00) | 306.54 (304.67–343.54) | −5.6%, overlaps |
| | Disk | 902.67 (882.08–918.25) | 888.88 (878.21–926.17) | −1.5%, overlaps |
| | Bloom | 332.58 (317.42–332.62) | 315.17 (312.46–315.62) | −5.2%, faster |

Times are in µs. A hit makes 6 fewer allocations (34.0 to 28.0 warm
under `full`, 72.7 to 66.7 cold) and allocates 3.2 KiB less, and a
100-key MultiGet makes 600 fewer and allocates 322 KiB less; a miss,
which reads no value page, is unchanged. The plain value reader
(`2550686`) is the one change on a Get's path, and the inference from
that, together with a measurement of the same counts at `1f985f6`, is
that it accounts for the difference. GETs and local reads per call are
the same in every scenario, and the key-value table's loads and
refreshes make the same reads within the spread of their times. Of the
48 comparisons, the 24 beside the idle ordered table split as the 24
alone do: 11 faster outside the spread and 13 overlapping, against 9 and
15.

## MultiScan

A warm MultiScan of 10 keys at limit 200, both binaries, p50:

| tier | 1 task, before | 1 task, after | 11 tasks, before | 11 tasks, after |
|---|--:|--:|--:|--:|
| Full | 223.4 µs | 178.0 µs | 486.8 µs | 211.3 µs |
| Disk | 348.6 µs | 297.7 µs | 2,020.4 µs | 1,997.9 µs |
| Bloom | 225.3 µs | 182.1 µs | 511.6 µs | 224.4 µs |

CPU per key on one task under `full` fell from 22.2 to 17.9 µs (22.1,
22.2, 23.0 against 17.7, 17.9, 18.2). Cold, the same MultiScan takes
496.6 µs on one task where it took 541.0 (507.4–568.6) under `full`,
49.7 µs a key against 54.1.

MultiScans of 100 and 1,000 keys at limit 1, which the baseline bench
cannot run, at the branch head only:

| scenario | tier | p50, 1 task | p50, 11 tasks | ops/s, 11 tasks | CPU per key, 1 task |
|---|---|--:|--:|--:|--:|
| 100 keys, warm | Full | 1.06 ms | 1.59 ms | 5,257 | 10.6 µs |
| | Disk | 2.74 ms | 23.40 ms | 466 | |
| | Bloom | 1.09 ms | 1.81 ms | 5,083 | |
| 100 keys, cold | Full | 4.39 ms | 40.30 ms | 269 | |
| | Disk | 4.23 ms | 44.59 ms | 244 | |
| | Bloom | 6.54 ms | 67.94 ms | 163 | |
| 1,000 keys, warm | Full | 10.39 ms | 20.14 ms | 537 | 10.6 µs |
| | Disk | 26.71 ms | 233.97 ms | 47 | |
| | Bloom | 10.69 ms | 21.28 ms | 512 | |
| 1,000 keys, cold | Full | 42.33 ms | 406.38 ms | 27 | |
| | Disk | 42.17 ms | 423.77 ms | 26 | |
| | Bloom | 63.75 ms | 632.43 ms | 17 | |

These are the figures behind the open decision on spreading a
MultiScan's keys over the runtime's workers. A measurement at `42537a8`,
on the same table, found that spreading runs of 32 keys or more cut the
one-task p50 from 1.07 to 0.32 ms at 100 keys and from 10.5 to 2.35 ms
at 1,000, for 10% and 70% more CPU per key, and the change was not kept.
The branch head measures the unspread figures again: 1.06 and 10.39 ms,
10.6 µs of CPU per key at both sizes (10.59 to 10.62 at 100 keys, 10.54
to 10.97 at 1,000). On eleven tasks, the case where every worker is
busy, a 1,000-key MultiScan takes 20 ms.

## Profiles

Warm bounded Scans on one task, Full tier, shares of samples inside the
operation:

| cost | 10 rows of a 10,000-row run, timestamp order (25.3 µs) | the same, string order (45.3 µs) |
|---|--:|--:|
| zstd decompression of cached pages, inclusive | 53.4% | 37.6% |
| decoding the order page that holds the bound (`decode_order_span`), inclusive | 26.5% | 46.8% |
| decoding the run's first and last key pages (`decode_run`), inclusive | 28.6% | 17.5% |
| decoding the value rows taken (`plain::value_rows`), inclusive | 14.1% | 7.8% |
| the walk over the rows (`Walk::rows`), inclusive | 19.5% | 15.8% |
| `_platform_memmove`, self | 13.8% | 7.9% |
| parquet's byte-array value decoder (`ColumnValueDecoderImpl<ByteArrayType>`), inclusive | | 16.6% |
| `Bytes` reference-count clone and drop, self | | 8.4% |
| UTF-8 check of each order value (`OrderRun::is_utf8`), self | | 8.2% |

Decompressing pages the cache holds compressed takes half of the long
Scan and over a third of the string Scan, the largest share of each, as
it was of the warm Scans in the
[earlier analysis](2026-10-13-ordered-rows-costs.md): RFD 006's
decompressed-page level remains the largest lever. The string Scan
decodes its order page through parquet's column reader, which builds and
reference-counts each value, and it checks every order value as UTF-8,
which the load already verified. A plain reader for byte-array order
pages like the one value pages now use, and a Scan that skips the second
check, would remove the two costs this profile names.

Under `disk`, the warm bounded Scan of a 100-row run (28.8 µs in the
profiled run) spends 9.0% waking the blocking pool for its two local
reads (`pthread_cond_signal`) and 5.8% in the chunk-file page read.

A first load of `ordered_rolled` under `full`, shares of the busy samples
of 400 loads: verifying each file's rows takes 65.2% (zstd 36.8%,
comparisons of order values and keys 21.5% self), opening and reading
the local files 9.3%, the merge 8.7%, and reading footers 3.9%. The
merge is a twelfth of the load. The compacted table's 10.6 ms load
suggests, without a profile of it, that the rest is what any ordered
load of these rows costs. In 80 loads and refreshes of
`ordered_appended` the merge takes 66.1% and verification 22.9%; the
merge section breaks the merge's share down.

## What did not move as expected

- **The Disk-tier warm Scan of a short run is slower.** `scan-bounded`
  on `ordered` under `disk` on one task takes 34.29 µs (34.25–34.54)
  where it took 32.08 (31.96–32.21), 6.9% slower and outside the spread,
  with the same 2.1 local reads and half the allocations. The long-run
  Scan under `disk` became 45% faster. Since `dc44300`, a Disk-tier Scan
  reads the order values of the rows it may take from the chunk file
  before it issues its value reads, where the old Scan issued both
  together. The inference, without a profile of the baseline, is that
  the Scan now waits on its order read through the blocking pool before
  it reads its value pages from the scan cache, where the old Scan
  overlapped the two, and that this accounts for the 2.2 µs; waking the
  blocking pool takes 9% of the Scan's samples.
- **The single round reads more bytes over a long order prefix.** A cold
  bounded Scan of the string table under `full` and `bloom` requests a
  value page more than it did; the one-round section gives the bytes.
- **Fences over long string prefixes take more memory than the cut
  index.** 159 KB against 146 KB, and 5% more chunk-file bytes.
- **The overlapping-append refresh is no faster.** Its GETs fell 93%,
  its bytes stayed the same, and two thirds of its busy samples fall in
  the merge.
- **More requests for fewer bytes.** The rolled table's first load makes
  12% to 18% more GETs, and its restart 71% more local reads, for 21%
  and 95% fewer bytes.
- **The read waves.** Over files of several row groups, outside the
  writer recipe, a first load waits on two to seven times the rounds it
  waited on; the read-waves section gives the counts.
- **The Disk-tier Scan's throughput.** On eleven tasks the Disk-tier
  Scans of `ordered` and `ordered_long` run at 47,000 to 50,000 a second
  before and after, while the Full-tier figures for the same Scans rose
  by up to 2.9 times. The string Scan under `disk`, which ran below that
  rate, rose from 14,420 to 40,751 a second. The earlier analysis traced
  the Disk tier's ceiling to the handoff of each local read to the
  blocking pool; these runs are consistent with that ceiling and do not
  establish it.
- **A concurrent Scan's extra round.** When a read claims part of a run
  while another read waits for its fan-out permit, the waiting read
  fetches the rest in a second pass, one more round (`7f19ba1`). The
  bench counts rounds one Scan at a time, so it does not measure this
  case.

## What this does and does not establish

Over these tables, a bounded Scan takes one round to object storage
whatever the writer's order column index holds, and its warm work
depends on its limit and the pages that hold its bound, not on its run's
length. The uniqueness merge reads the pages at the keys files share,
and a Disk restart of a rolled table costs a ninth of what it cost,
though still five times a compacted table's. The fences take less memory
than the column index over integer and time order columns and somewhat
more over long string prefixes. The key-value path keeps its reads and
is slower in none of its comparisons.

The figures come from local files, so no GET here waits on a network,
and the latency each round adds on S3 is estimated from the counts, not
measured. A table written to the recipe in the
[table layout](../table-layout.md), 128 MiB row groups in files of 32
to 64 MiB, holds one row group per file, where the read waves cost
nothing. The waves are measured outside the recipe at one row-group
size, 25,000 rows, about four per file, the shape of Iceberg's default
512 MiB files with 128 MiB row groups; other sizes would move the
counts. The bench
tables have no deleted rows, so the skipping of deleted pages and the
256 KiB bridge between spans are covered by their unit tests and not by
these runs. The host ran other work throughout; every comparison
alternated its binaries, and every difference called outside the spread
is one where all three runs of one binary fall on one side of all three
of the other's.
