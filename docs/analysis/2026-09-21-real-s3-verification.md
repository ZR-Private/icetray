# icetray against real S3

Measured 2026-09-21 with the `icetray` binary of that date, ahead of the
0.1.0 release, against the S3 tables the prototype wrote ([prototype findings, writer
recipe](2026-09-10-prototype-findings.md#writer-recipe-for-the-s3-tables)),
from the same laptop and the same kind of residential connection, with
profile credentials. Everything the design promises about cost held on
every table: one GET per cold hit, none per warm hit or miss, every value
byte-identical to the recipe's function of its key, the doctor's verdicts as
RFD 007 classifies the layouts, a load of the 50-million-row table in under
four seconds, and a snapshot appended by Spark swapped in within one poll.
Adaptive hedging spent GETs inside its ceiling and trimmed nothing, because
this connection's latency had no second mode to trim. Two things the
analysis lacked are now numbers: load time after the credential fix, and
hedging that adapts.

## Method

- **Binary.** A release build of the `icetray` crate as of 2026-09-21,
  run as `doctor`, `get`, and `serve` with a `--location` table root or a
  JSON configuration; credentials through the AWS SDK's default chain from
  a named profile with an external credential helper; the storage layer's
  own counters (`gets` on every `get` line) as the cost oracle; wall-clock
  latency from the `get` lines' `elapsed_micros`.
- **Tables.** The six tables the prototype's notebook wrote with Spark 3.5.8
  and Iceberg 1.10.1 into one bucket under a Hadoop catalog, read through
  their roots and version hints: `long_2m` (2,005,000 rows after the
  prototype's own append, 8 files, 87 MB), `str_2m` (2,000,000 rows of
  91-byte keys, 4 files, 87 MB), `long_wide_250k` and `long_wide_split_250k`
  (250,000 rows of 16,384-byte values, 4 files, 2.14 GB each),
  `long_wide_shuffled_250k` (the same, written so that files overlap), and
  `long_50m` (50,000,000 rows, 25 files, 2.0 GB). The bucket and the profile
  are deployment identifiers and are not recorded here.
- **Vantage.** One laptop, one afternoon, one residential connection to a
  bucket in another region. Every number below carries that connection's
  latency; the in-region column is the prototype findings' and was not
  re-measured.
- **Values.** The recipe's: for the 192-byte tables the hex SHA-256 of the
  key's decimal text repeated three times; for the wide tables the hex
  SHA-256 of `key:i` for `i` from 1 to 256 concatenated. Every `present`
  answer was decoded from its `value_base64` and compared whole; every key
  that is not a multiple of three was expected `absent` at zero GETs.

## Results

### The doctor

`icetray doctor --files` on each table root, one process per table, wall
clock including the credential helper and the load:

| table | verdict | files | index bytes | key chunk bytes per row | findings | wall |
|---|---|---|---|---|---|---|
| `long_2m` | efficient | 8 | 2.66 MB | 1.20 | `large_value_pages` on the 4 large files, `bloom_oversized` on the 4 append files | 2.9 s |
| `str_2m` | efficient | 4 | 6.75 MB | 2.97 | `large_value_pages` | 2.0 s |
| `long_wide_250k` | efficient | 4 | 2.34 MB | 1.15 | `large_file`, `bloom_oversized` | 3.8 s |
| `long_wide_split_250k` | efficient | 4 | 2.34 MB | 1.15 | `large_file`, `bloom_oversized` | 2.5 s |
| `long_wide_shuffled_250k` | inefficient, exit 2 | 4 | 2.68 MB | 2.54 | `overlapping_files` (cost, two buckets, two files probed), `large_file`, `bloom_oversized` | 2.6 s |
| `long_50m` | efficient | 25 | 85.1 MB | 1.59 | `large_value_pages`, `bloom_undersized` (4.2 bits per key) | 6.8 s |

Every file of every table reports 1,000-row key pages, and the 192-byte
tables report aligned key and value pages. The wide tables report
`pages_aligned: false`, as byte-bounded value pages must, and value pages of
65,571 bytes against the 64 KiB bound. The predicted resident bytes per tier
came from the footers alone: for `long_50m`, 85.1 MB full, 5.7 MB in memory
plus 79.4 MB on disk for the disk tier, and 31.9 MB for the bloom tier, whose
4.2 bits per key the doctor flags as undersized.

Two of the findings describe the recipe rather than the table. The 192-byte
tables' value pages decompress to 160 to 196 KB, above the doctor's 128 KiB
advice threshold of that date, because the recipe bounds pages by 1,000
rows and leaves the byte bound at 8 MiB for small values; the same advice
appeared then on the golden tables written by the checked-in recipe. The
threshold is 1 MiB now ([RFD 016](../rfd/016-doctor.md)), and pages of
this size draw no advice. The prototype's four
append files on `long_2m`, of 1,225 rows each, carry the writer's fixed 1
MiB filters, or about 6,600 bits per key. Neither affects a lookup's cost.

### Lookups, verified

| table | cold hits | misses | warm hits | GETs per cold hit | GETs per miss or warm hit | cold p50 | cold p90 | cold max |
|---|---|---|---|---|---|---|---|---|
| `long_2m` | 24 | 6 | 4 | 1 | 0 | 62 ms | 102 ms | 124 ms |
| `long_wide_250k` | 6 | 2 | 2 | 1 | 0 | 68 ms | 191 ms | 260 ms |
| `long_50m`, hedging off | 399 | 0 | 1 | 1 | 0 | 76 ms | 159 ms | 388 ms |
| `long_50m`, hedging on | 399 | 0 | 1 | 1, plus 9 hedges | 0 | 97 ms | 180 ms | 373 ms |

Every value verified. A warm hit (the same key later in one process) took
0.15 to 0.20 ms with no GET. The `long_50m` samples are 400 keys drawn
uniformly, one process each; the two runs were minutes apart, and the 20 ms
between their medians is within what this connection moved by during the
afternoon, so the table's cold cost is the 75 to 100 ms range rather than
either number. Against the prototype's numbers from the same tables
([finding 4](2026-09-10-prototype-findings.md#4-lookup-latency)) the medians
are lower by half; the connection, not the reader, is the likelier cause,
since the reader still issues one GET per hit.

### Load time after the credential fix

The daemon started with both `long_2m` and `long_50m` configured, listeners
first, both tables loading concurrently. From the `listening` log event to
`table serving`: 3.83 s on the first run and 4.00 s on the second, for the
two tables together, 33 files, 87.8 MB of resident index. `long_2m` alone,
in the snapshot-advance runs below, served 1.44 s after listening. The
prototype recorded 3.8 s for `long_50m` as a load plus doctor from the
command line, before the credential bridge cached its provider; the daemon
now loads the same table, and a second one beside it, in the same time with
the credential helper spawned once at startup. The doctor's wall times
above, which include the process start and the credential helper, are
larger by about two seconds each.

### Snapshot advance

With the daemon serving `long_2m` at a 10-second poll, a Spark job appended
5,000 rows (keys 9,000,000 to 9,014,997) as a new snapshot through the
Hadoop catalog on S3, committing 18.7 s after it started. The daemon logged
`table advanced` with the new snapshot id 3 s after the commit;
`icetray_generation_swaps_total{table="long_2m"}` read 1,
`icetray_generation_info` read 1 for the new snapshot and 0 for the old, and
`icetray_index_bytes` grew from 2,657,515 to 2,677,444 (19.9 KB for the new
files; the eight existing files were reused). A `get` of the first and last
appended keys answered `present` with verified values at one GET each, a
key past the appended range answered `absent`, and an old key still served.
Idle polls between the start and the append cost nothing visible in the
log; the daemon drained on SIGTERM and exited 0.

### Adaptive hedging

The two `long_50m` runs above differ only in `storage.hedge.enabled`. With
hedging on at the defaults (the 95th percentile of the last 256 lookup GETs
as the delay, a ceiling of 5 percent), nothing hedged until the window had
filled, as designed; over the 144 lookups after it, 9 hedged (6.2 percent of
those lookups, 3.5 percent of the window, under the ceiling), each costing
one extra GET, for 9 extra GETs over 399 hits. The hedged lookups took 206
to 265 ms: the first request had crossed the p95 delay of about 180 ms and
the hedge, issued then, landed with the first answer's tail rather than
before it. The maximum over the same 144 lookups was 266 ms hedged and 253
ms unhedged, so on this connection hedging trimmed nothing. That is the
expected outcome for a latency distribution with one mode: RFD 013 buys
insurance against a second mode, a stall, and this afternoon had none. The
mechanism is now verified against real S3 (the window fills, the percentile
is taken, the ceiling binds); whether it pays is a property of the
connection, and the simulation tests hold the bimodal case.

## What this establishes

- icetray serves the prototype's S3 tables with the cost model
  intact: one GET per cold hit, none otherwise, values exact, on 64-bit
  keys, 91-byte string keys, 192-byte values, and 16 KB values, across 4 to
  25 files per table.
- The doctor's verdicts on real Spark output match RFD 007's tiers: the
  contract tables are efficient with advice only, and the one overlapping
  table is inefficient at two files probed and refused under `strict`.
- Load time on S3 with the credential bridge in place is a few seconds for
  a 2 GB table of 25 files, and the daemon serves while it loads.
- A snapshot committed by a Spark job is picked up within one poll, reusing
  every unchanged file, and the appended keys serve at one GET.
- Adaptive hedging fills its window, hedges at the configured percentile,
  and stays under its ceiling against real S3.

## What it does not establish

- Whether hedging improves the tail under a real stall episode; none
  occurred. The simulation tests inject one; a long-running daemon's
  histograms would show a real one.
- In-region latency, throughput per core, or behavior under concurrent
  load; every lookup here was sequential from one process.
- Load time at thousands of files on S3; the largest table has 25.
- The disk and bloom residency tiers against S3; both ran here in the full
  tier only. The doctor's per-tier predictions above are the numbers a table
  owner would choose from.

## Operational notes

- The AWS SDK for Rust rejects a profile whose name contains parentheses
  (`[profile team-(account)/role]`) as an invalid section and resolves no
  credentials from it; the storage layer then builds an S3 client with no
  provider, and every read is refused with the object store's "credential
  provider was not enabled". The credential helper in use writes both that
  form and a plain `[profile account/role]` alias; the alias works. A
  deployer whose profiles carry such names needs the alias or static keys.
  RFD 012 (Deployment contract) may want a sentence on it.
- jemalloc's background purging is unavailable on macOS; the daemon logs
  the refusal at `warn` and runs without it, as designed. The memory
  measurements RFD 017 cares about are Linux numbers and were not taken
  here.
