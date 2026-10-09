# Merge-on-read costs, measured

Measured 2026-10-07 on one Linux host, an AWS Graviton2 node, with the
process limited to 4 CPUs and 32 GiB. These are the three measurements
[RFD 021](../rfd/021-merge-on-read.md) makes before release: lookup
latency by files per bucket with deleted rows in the probe order, the
delete state's memory against the bound the budget counts, and load and
refresh time with up to 1,000 live delete files.

The deleted-row check costs 7 to 10 ns where a key-page decode costs
about 20.7 µs, a ratio of at most 0.0005 against the RFD's threshold of
0.1, so the uncapped check stands. A table whose every probed copy is
deleted costs what a table with no deletes costs at every depth in the
`full` and `disk` tiers. What deletes cost a lookup is the files it
probes: about 20 µs a file, flat for a hit on an updated key and linear
in the depth for a miss on a deleted one. Scattered deletes take 2 to
3 bytes a deleted row at low fractions, exactly the bound, and a run of
consecutive deletes a small fraction of a byte.

## Why it was measured

RFD 021 serves deletion vectors and Parquet position deletes and lets a
lookup check a file's deleted rows with no cap on the level, on the
argument that a bitmap probe is small beside a key-page decode. It names
the measurements that argument rests on and says the uncapped check is
revisited if the check adds more than a tenth of a decode to a lookup.

## Method

- **Host.** An AWS r6g.8xlarge node (Graviton2, aarch64, 32 vCPU,
  256 GiB) running Linux 6.12 (Flatcar) with glibc 2.39, the process in a
  container whose cgroup limited it to 4 CPUs and 32 GiB. Tables were on
  ext4 on the node's EBS volume, whose type was not confirmed.
- **Program.** `crates/icetray-testkit/examples/deletes.rs`, built with
  Cargo's standard release profile, with no overrides, from commit
  `8c7cc95` and cross-built for aarch64 Linux, run at its defaults with
  seed 7. It writes contract-layout tables of long keys with the fixture
  writer and serves them in process through `Table` over `Storage`'s
  `file://` store, so a "GET" is a local file read and no figure includes
  gRPC or object-store latency. Lookups run one at a time.

  ```sh
  dev/merge-on-read-bench.sh
  ```

- **Probe.** Three tables grown to 1, 2, 4, 8, and 16 files per bucket,
  50,000 rows a file, every file spanning the same key range so every
  lookup's candidates are every file. `control` adds files of other keys
  and deletes nothing. `updated` rewrites the first file's keys in each
  new file and deletes the older copies, as a merge-on-read `UPDATE` does.
  `shadowed` adds files holding copies of the first file's keys, each
  deleted in the commit that adds it, so a hit passes a deleted row in
  every newer file before the live one. In `updated` and `shadowed`, one
  key in eight is deleted from every file that holds it, and those are
  the keys their misses ask; `control`'s misses ask absent keys. 10,000
  hits and 10,000 misses per run, each key asked once unmeasured first,
  so the page cache and the kernel's hold what the measured lookups read.
  In the `full` tier the program then times the key-page decode and the
  deleted-row check apart over the same lookups.
- **Memory.** Four data files of 1,000,000 rows, one per bucket, with 0.1%
  to 50% of each file's rows deleted, in three forms: a deletion vector
  per file (`vectors`), a position delete file per data file that names it
  (`scoped-positions`), and a position delete file per partition that
  names no data file (`positions`). The deleted rows are either one run of
  consecutive rows at a random start (`dense`) or rows drawn uniformly at
  random (`scattered`). Each state is loaded in the `full` tier and its
  bytes compared with the bound the budget counts, the doctor's predicted
  memory less the same table's with no deletes.
- **Load.** 1,001 data files of 1,000 rows, one per bucket, with 0, 1, 10,
  100, or 1,000 live delete files of 10 rows each, one per data file and,
  for vectors, one Puffin file each. Three rounds of a first load in the
  `full` tier and a refresh onto a snapshot with one more delete file,
  counting the store's GETs.

`dev/merge-on-read-bench.sh` writes the printed tables as `summary.txt`
and every measurement in full as `merge-on-read-<time>.json` under
`target/merge-on-read/<UTC time>/`, with the tables themselves under
`target/merge-on-read/work`, removed when it ends. The JSON of this run is
not in the repository.

## The deleted-row check against a key-page decode

Over the probe tables' lookups in the `full` tier, a key-page decode took
20.6 to 21.0 µs in every variant and depth. A deleted-row check took
6.8 ns where a lookup made one and rose to 10.3 ns at 16 a lookup, in
`shadowed` at depth 16, the worst ratio: 0.00049 of a decode, two hundred
times below the RFD's 0.1. A lookup in that table makes 16 checks and
16 decodes, so the checks add about 0.05% to its time.

The memory measurement times the check again, at 4,096 random positions
of each state, with the state's own layout. Over a run of deletes it took
7 to 27 ns; over scattered deletes, 34 to 58 ns, the most at 5% deleted,
where each block's rows are a sorted array searched for the position.
Even that is 0.003 of a decode.

## Lookups by files per bucket

Median latency in µs, by files per bucket. No lookup made a GET: every
key page and value page was in the page cache.

| hits | tier | 1 | 2 | 4 | 8 | 16 |
|---|---|--:|--:|--:|--:|--:|
| control | full | 43.3 | 65.0 | 108.6 | 191.6 | 363.3 |
| control | disk | 48.0 | 72.1 | 120.8 | 219.9 | 424.4 |
| control | bloom | 45.5 | 45.9 | 46.7 | 48.6 | 51.2 |
| updated | full | 43.7 | 44.0 | 44.7 | 44.5 | 44.3 |
| updated | disk | 47.2 | 48.5 | 48.6 | 48.1 | 47.5 |
| updated | bloom | 45.6 | 45.8 | 46.8 | 45.7 | 45.3 |
| shadowed | full | 43.6 | 65.0 | 108.8 | 194.1 | 363.6 |
| shadowed | disk | 47.7 | 72.4 | 121.1 | 223.5 | 422.7 |
| shadowed | bloom | 45.2 | 68.7 | 116.9 | 209.3 | 392.0 |

| misses | tier | 1 | 2 | 4 | 8 | 16 |
|---|---|--:|--:|--:|--:|--:|
| control | full | 20.5 | 41.0 | 82.1 | 164.6 | 330.7 |
| control | disk | 23.6 | 46.8 | 93.4 | 188.6 | 390.2 |
| control | bloom | 0.3 | 0.5 | 0.8 | 1.4 | 2.9 |
| updated | full | 20.7 | 40.9 | 82.0 | 164.5 | 332.7 |
| updated | disk | 23.7 | 48.0 | 96.1 | 192.8 | 394.8 |
| updated | bloom | 22.3 | 44.6 | 90.2 | 181.2 | 365.6 |
| shadowed | full | 20.7 | 41.0 | 81.8 | 164.6 | 331.5 |
| shadowed | disk | 23.9 | 47.5 | 94.6 | 192.5 | 388.3 |
| shadowed | bloom | 22.3 | 44.4 | 89.5 | 181.2 | 364.8 |

Each file a lookup probes adds one key-page decode: about 20.7 µs a file
in the `full` tier and 24.4 µs in `disk`, which adds the local read. A
hit adds its value page, about 23 µs, to the probes before it.

- **Deleted rows first in the probe order cost nothing beyond the
  probe.** `shadowed` matches `control` at every depth in `full` and
  `disk`, hits and misses alike, within about 2% at the median. A hit that passes 15 deleted rows before its live one pays
  for 16 key pages, as it would with 15 older files of other keys, and
  the 15 checks are lost in the spread.
- **Updated keys stay at one probe.** A hit on an updated key costs the
  same at 16 files as at one in every tier, because the newest copy is
  probed first.
- **A deleted key's miss probes every file holding a copy,** linearly in
  the depth, in every tier.
- **Under `bloom` a deleted copy is still in its file's filter.** An
  absent key's miss stays under 3 µs at 16 files, since the filters reject
  it, but a deleted key's miss in `updated` or `shadowed` pays the full
  probe, about 23 µs a file, as RFD 021 predicts. A `shadowed` hit under
  `bloom` pays the same for each newer file, where `control`'s hit is
  flat. With the page cache warm none of these probes made a GET; cold,
  each would be a key-page GET, the cost RFD 021 assigns to `bloom`.

## Delete-state memory

Bytes per deleted row, and the state's bytes over the bound the budget
counts. Four files of a million rows each.

| deleted | dense: B/row | dense: state/bound | scattered: B/row | scattered: state/bound | scattered, unscoped positions: state/bound |
|--:|--:|--:|--:|--:|--:|
| 0.1% | 0.574 | 0.181 | 3.169 | 1.000 | 0.025 |
| 1% | 0.057 | 0.027 | 2.118 | 1.000 | 0.161 |
| 5% | 0.012 | 0.006 | 2.024 | 1.000 | 0.765 |
| 10% | 0.006 | 0.005 | 1.275 | 0.964 | 0.964 |
| 25% | 0.003 | 0.005 | 0.529 | 1.000 | 1.000 |
| 50% | 0.002 | 0.007 | 0.264 | 1.000 | 1.000 |

The first four columns are deletion vectors. Position delete files that
name their data file take the same bytes within 40 bytes a file and
the same ratios; position delete files that name no data file take the
same state within about 160 bytes a file, against a different bound,
the last column.

- **Scattered deletes cost what the bound allows.** At 0.1% to 5% a block
  of 65,536 rows holds from 65 to 3,277 deleted rows, which the bitmap
  stores as an array of two bytes each, plus fixed costs per block and per
  file: 2 to 3.2 bytes a deleted row. From 10%, past 4,096 deleted rows
  in a block, each block is an 8 KiB bitset, and the cost per row falls
  with the fraction, to 0.26 bytes at 50%. The bound is exact, a ratio of
  1.000, everywhere except at 10%: there each file's last, partial block
  holds fewer than 4,096 deleted rows and stays an array, while the bound
  charges it 8 KiB.
- **A run of consecutive deletes costs almost nothing.** The bitmap stores
  a run by its ends, so the state is 600 to 900 bytes a file at every
  fraction, 0.6 bytes a deleted row at 0.1% and 0.002 at 50%. The bound
  charges two bytes a row, so it is loose by 5 to 200 times.
- **The bound is loose for position deletes that name no data file at
  low fractions:** the state is 0.025 of the bound at 0.1%, because such a file may put
  its rows in any file of its partition and the bound allows for all of
  them, here the cost of deleting every row of the bucket's file. From
  10%, where that is what the state costs anyway, it is exact.

## Load and refresh

Median of three rounds, `full` tier, with the store's GETs.

| form | delete files | load | load GETs | refresh | refresh GETs |
|---|--:|--:|--:|--:|--:|
| vectors | 0 | 109 ms | 2,006 | 35.0 ms | 7 |
| vectors | 100 | 120 ms | 2,207 | 38.5 ms | 7 |
| vectors | 1,000 | 182 ms | 4,007 | 53.6 ms | 7 |
| scoped-positions | 0 | 113 ms | 2,006 | 37.4 ms | 6 |
| scoped-positions | 100 | 118 ms | 2,107 | 36.9 ms | 6 |
| scoped-positions | 1,000 | 168 ms | 3,007 | 51.3 ms | 6 |
| positions | 0 | 114 ms | 2,006 | 36.8 ms | 6 |
| positions | 100 | 127 ms | 2,107 | 38.0 ms | 6 |
| positions | 1,000 | 175 ms | 3,007 | 53.5 ms | 6 |

A deletion vector costs two GETs at load, its Puffin file's footer and
its blob, and a position delete file one. A thousand delete files added
55 to 73 ms to a load from local disk, 55 to 73 µs a file. A refresh onto
one more delete file made 6 or 7 GETs whatever the count, reading only the
new delete. Its time grew from 34 to 38 ms at up to 100 delete files to
51 to 54 ms at 1,000; the GETs did not change, so the growth is not in
reads.

## What this does and does not establish

It establishes that the deleted-row check is thousands of times cheaper
than the key-page decode it sits beside; that a lookup's cost under
merge-on-read is the files it probes, whatever their deletes; that the
budget's bound is exact for scattered deletes and generous for runs and
for unscoped position deletes at low fractions; and that delete files add
one or two reads each to a load and none beyond the new one to a refresh.

It does not establish:

- The check under a cold CPU cache. The micro-measurement repeats the
  lookups' checks millions of times, so the bitmaps sit in the CPU's
  caches. The memory measurement's random positions are closer to a
  server's, and its slowest check, 58 ns, is still 0.003 of a decode.
- Other key types. The probe tables have long keys, the cheapest key page
  to decode, so the check's ratio to the decode is the largest any key
  type would give. A string key page costs more to decode and the ratio
  falls.
- Load in another tier. Load and refresh were measured under `full`
  only; `disk` adds writing chunk files, which deletes do not change.
- S3. Every GET was a local file read. Under S3 the load's added GETs
  cost a round trip each, overlapped by the load's reads in flight, and
  a `bloom` miss on a deleted key pays a key-page GET per file wherever
  the page cache does not hold it.
- Deletes one to a block. The scattered pattern put at least 65 deleted
  rows in each block, so the case the bound charges tens of bytes a row
  for, a single deleted row in a block, was not measured.
- Real tables' deleted fractions and patterns, which decide whether a
  `disk` table's delete state outgrows the memory it keeps per key (the
  open question RFD 021 keeps).
