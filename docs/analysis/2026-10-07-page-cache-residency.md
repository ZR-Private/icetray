# Disk-tier latency against page-cache residency

Measured 2026-10-07 on one Linux host, an AWS Graviton2 node, with the
process limited to 4 CPUs and 32 GiB. The sweep served one `disk` table,
dropped a chosen share of its chunk files from the kernel's page cache in
random 4 KiB blocks, and timed lookups over uniformly drawn keys. It is the
measurement [RFD 020](../rfd/020-lookup-levels.md) waited on to fix the two
resident fractions of the page-cache condition and the interval it is
sampled at.

A miss whose key page was in the page cache took about 100 µs at the
median, and one whose key page came from the device about 730 µs, so a
key-page read from this device added about 600 µs. Each latency
percentile moved from the first figure to the second once enough of the
chunk files was missing: the 99th percentile at a shortfall under 1%, the
90th at 5%, and the median at 50%. The page-cache condition's thresholds
are kept: a table falls below `optimal` under 0.95 resident, where the
90th percentile had moved, and recovers above 0.99, where only the 99th
had.

## Why it was measured

RFD 020 lets a `disk` table fall below `optimal` while its chunk files are
out of the page cache, judged by a sample of the files' resident pages
against two fixed fractions. The fractions in the code, 950 and 990 in a
thousand, were chosen before anything had measured how lookup latency
follows residency, and [Performance and tuning](../performance.md) called
them provisional until a measurement set them. RFD 020 left the sampling
interval open for the same reason.

## Method

- **Host.** An AWS r6g.8xlarge node (Graviton2, aarch64, 32 vCPU,
  256 GiB) running Linux 6.12 (Flatcar) with glibc 2.39. The process ran
  in a container whose cgroup limited it to 4 CPUs and 32 GiB of memory.
  The table and the chunk files were on ext4 on the node's EBS volume;
  the volume's type was not confirmed, so the device figures below are
  this volume's and not a property of EBS in general.
- **Program.** `crates/icetray-testkit/examples/residency.rs` and the
  benchmark program that prepares its table, built with Cargo's standard
  release profile, with no overrides, from commit `c3b35fc` and
  cross-built for aarch64 Linux. `dev/residency-sweep.sh` builds both,
  prepares the table, and runs the sweep; the run here passed it
  `--concurrency 4`, the cgroup's CPUs:

  ```sh
  dev/residency-sweep.sh --concurrency 4
  ```

- **Table.** The benchmark program's `scattered` table: 1.2 million string
  keys in 32 buckets, one data file per bucket, about 4.7 GiB of Parquet
  in all (the value column, 1.00 compression, is nearly all of it). Served
  at `disk` residency, it has 32 chunk files. Their key chunks total about
  18 MiB, about 570 KiB per file, a figure the eviction counts below give;
  the program prints the total rounded to 0.0 GiB.
- **Warehouse.** A local `file://` path, so a hit's value-page "GET" is a
  read of a local file, not a request to S3. Every figure here is the
  relative cost of the page cache and the device, not S3 latency, and a
  hit's latency carries a local value read that a deployment pays as a
  GET of tens of milliseconds.
- **Sweep.** For each eviction block size (4 KiB, the page size, and
  64 KiB, about a key page of this table) and each target fraction, the
  program warms every chunk file, drops the target's share of each file's
  key chunks in blocks chosen at random with a fixed seed (`posix_fadvise`
  with `POSIX_FADV_DONTNEED`), and reads the resident fraction with the
  server's own sampler. It then makes 20,000 hits and 20,000 misses, one
  at a time and four at once, drops the same blocks again before every
  1,000 lookups, since lookups fault pages back in, and samples the
  fraction again at the end. The targets are every tenth from 1 to 0 and
  the points around the two thresholds: 0.995, 0.991, 0.990, 0.970, 0.950,
  and 0.949.
- **Lookups.** A miss asks a key just above one the table holds, and
  reads one key page from its chunk file. A hit reads the key page and
  then the value page from the data file.

`dev/residency-sweep.sh` writes its results under
`target/residency/<UTC time>/`: the printed table as `sweep.txt`, the
prepared table's line as `tables.tsv`, and every run in full as
`residency-<time>.json`. The JSON of this run is not in the repository.

## Results

### Where each percentile reaches the device

Latency in µs as p50 / p90 / p99, one lookup at a time, 4 KiB blocks.
"Before" is the resident fraction the sampler read after the eviction and
before the lookups; "after" is what it read once they ended.

| before | after | miss | hit |
|--:|--:|--:|--:|
| 1.000 | 1.000 | 102 / 111 / 128 | 277 / 363 / 1207 |
| 0.993 | 0.998 | 102 / 112 / 742 | 256 / 312 / 986 |
| 0.972 | 0.990 | 104 / 119 / 803 | 257 / 380 / 1029 |
| 0.951 | 0.987 | 104 / 655 / 807 | 255 / 825 / 1092 |
| 0.903 | 0.982 | 105 / 728 / 834 | 269 / 920 / 1376 |
| 0.805 | 0.963 | 109 / 756 / 858 | 275 / 938 / 1281 |
| 0.707 | 0.949 | 116 / 783 / 1190 | 298 / 951 / 1529 |
| 0.605 | 0.911 | 127 / 778 / 1178 | 582 / 988 / 2015 |
| 0.505 | 0.894 | 604 / 792 / 1488 | 775 / 1019 / 2002 |
| 0.409 | 0.853 | 621 / 781 / 1083 | 809 / 1035 / 2108 |
| 0.308 | 0.833 | 642 / 808 / 5328 | 801 / 987 / 1910 |
| 0.211 | 0.797 | 682 / 822 / 1283 | 832 / 1064 / 3109 |
| 0.111 | 0.757 | 715 / 837 / 1100 | 820 / 989 / 1559 |
| 0.007 | 0.730 | 727 / 849 / 1105 | 854 / 1034 / 1598 |

The targets 0.995 and 1.000 evicted the same, nothing, and so did 0.991
and 0.990, one block per file; 0.949 matched 0.950 within the runs'
spread. The table shows one row for each.

A miss has two latencies and little between them: about 100 µs with its
key page in the page cache, mostly the decode of a string key page, and
about 730 µs with the page read from the device. Each miss percentile
stays at the first until the shortfall reaches a threshold of its own and
then jumps to the second. The 99th percentile jumped by a 0.7% shortfall,
the smallest the 4 KiB blocks produced, the 90th between 2.8% and 4.9%,
and the median between 39.5% and 49.5%.

For p99 and p50 those points are close to the share of lookups each
percentile leaves above it, 1% and 50%. The 90th percentile moved earlier
than that reading predicts, at 5% rather than 10%. The program does not
count device reads per lookup, so the share of lookups that paid one is
not measured. A key page spans several 4 KiB pages, and a lookup pays a
device read when any of them is gone, which would make the share of
lookups paying exceed the share of pages missing; this run does not
separate that from the effect of refaults.

A hit adds the value page to the miss, here a read of the 4.7 GiB data
files that the page cache holds only in part, so its tail is the value
read's: a hit's p99 was about a millisecond at full residency. Its p90
moved with the miss's, at a 4.9% shortfall, and its median earlier, by
39.5%.

### The before and after columns

Lookups fault the pages they read back into the cache, and the kernel's
readahead brings neighbors with them, so residency rose through each round
of 1,000 lookups from the before column toward the after column. From
0.951 it climbed to 0.987; from nothing resident, to 0.73. The program
drops the same blocks before every round, so the before column is the
floor each round restarts from, and the lookups of a row ran at a
residency between its two columns.

Read against the before column, the table is generous: a table held at a
fraction for the whole run would have more of its pages missing on
average than the row that starts there, and would reach each latency
jump at a higher fraction than the row shows. The jumps are therefore an
upper bound on the shortfall a percentile tolerates under uniform keys.

### Four lookups at once

With all chunk files resident, four lookups at once cost what one did
(miss 102 / 111 / 136 µs). From about 0.8 resident down they queued on the
device: at 0.707 the miss p90 was 1,975 µs at four against 783 µs at one,
and the p99 4,413 against 1,190 µs. At the thresholds' end of the table
the two agree (at 0.951, 104 / 656 / 785 µs at four), so the thresholds
do not depend on the concurrency.

### The 64 KiB blocks

The 64 KiB rows are of little use at this table's size. A chunk file's
key chunks are about nine 64 KiB blocks, and the program evicts the whole
number of blocks per file that the target leaves out, rounded down, so
every target from 0.995 to 0.900 evicted nothing (before 1.000) and the
first eviction came at 0.800, one block per file, to 0.890 resident. The
same rounding at 4 KiB, about 142 blocks per file, is why 0.995 evicted
nothing and 0.991 and 0.990 evicted the same block. Where the 64 KiB rows
did evict, they agree with the 4 KiB rows at the same fraction: at 0.780
resident the miss read 105 / 734 / 966 µs. Measuring key-page-sized
eviction near the thresholds needs a hundred or more 64 KiB blocks per
chunk file, several megabytes each, a table larger than this one; it is a limit of the harness at this size,
not a finding about the block size.

### The sampler

A sample of the chunk files' residency took about 48 ms per GiB of chunk
files, the median over the 136 runs. Above 0.9 resident the median was
52 ms, and all but three runs read between 42 and 66 ms. A sample costs
less when fewer pages are resident, 22 to 24 ms per GiB with nothing
resident, and one run at 0.5 read 173 ms. These figures
come from samples of about 18 MiB in 32 files, under a millisecond each,
so the cost per GiB mixes a cost per file with a cost per page and is an
extrapolation to larger tables, not a measurement of them. At that rate a
sample of 10 GiB of chunk files would take about half a second of one
core, about 3% of a core at one sample every 15 seconds.

## What this does and does not establish

It establishes, for this host and volume, that a `disk` lookup's latency
has two levels a factor of about seven apart, and that under uniform keys
and random 4 KiB eviction each percentile moves between them at a shortfall
of its own: under 1% for p99, about 5% for p90, about 50% for p50. Those
shortfalls are upper bounds, for the reason the before and after columns
give.

It does not establish:

- Skewed access. Every key was drawn uniformly, and every evicted block
  was chosen at random, the worst case for a cache. Under real reclaim the
  kernel drops the least recently used pages first, so a table whose
  lookups favor some keys loses its coldest key pages first, and a
  shortfall costs fewer lookups than it does here.
- Other devices. A device read cost about 600 µs on this volume, whose
  type was not confirmed. Another volume or a local NVMe device moves the
  upper level, not where the percentiles jump.
- S3. The warehouse was local, so a hit's value read is a local read.
- Key-page-sized eviction near the thresholds, for the reason above.
- The sampler's cost on chunk files of gigabytes.

## The decision

The thresholds stay where they are: `FALL_PER_MILLE` 950 and
`RECOVER_PER_MILLE` 990, sampled every 15 seconds. They track the 90th
percentile. A table falls below `optimal` when one chunk-file page in
twenty is missing, the shortfall at which a uniform miss's p90 reached the
device in this sweep, and recovers when at most one in a hundred is,
inside the range from 0.972 up where the p90 stayed at the page cache and
only the p99 was on the device. Under skewed access the kernel evicts the
coldest pages first, so at a given fraction fewer lookups pay than here
and the cap is conservative.

The cap answers whether a table's chunk files still fit in memory. Tail
latency itself is the job of `icetray_lookup_seconds`, which sees every
cause of a slow lookup and not only this one.

A cutoff on the 99th percentile was considered: falling at about 0.99 and
recovering at about 0.997, where the p99 moved here. It was rejected
because the kernel reclaims cold pages of a cached file in the ordinary
course of running, and a cap that fires at a 1% shortfall would flag
tables whose working set fits. The sampling interval stays at about one
Prometheus scrape, since at the measured cost a sample is a small share
of a core on tables of the sizes the disk tier serves.
