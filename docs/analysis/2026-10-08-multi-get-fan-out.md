# MultiGet fan-out, measured against S3

Measured 2026-10-08 on one Linux host in AWS, an x86-64 node with the
process limited to 4 CPUs and 32 GiB, reading a table from S3 in the same
region. The measurement sets the default of `server.multi_get_fan_out`,
the most GETs one `MultiGet` holds in flight
([RFD 013](../rfd/013-tail-latency.md), [RFD 022](../rfd/022-value-columns.md)).

A lone cold batch of 100 keys gets faster as the fan-out rises to about
128, where it takes about three GET round trips (86 ms requesting one
column, 109 ms requesting two), and no faster above it. Eight batches
running at once reach the process's shared GET throughput sooner, about
5,400 to 6,400 GETs a second: by a fan-out of 128 for one-column batches
and by 64 for two-column ones. Past that point a larger fan-out speeds no
batch up and only lets one batch hold more of the shared throughput. The
default is 64: a lone batch keeps most of its gain (117 ms at one column,
139 ms at two, against 152 ms and 228 ms at the former 32), concurrent
batches reach or nearly reach the shared ceiling, and one caller holds no
more than its share.

## Method

- **Host.** An AWS node with an Intel Xeon Platinum 8259CL (x86-64)
  running Linux 6.12 (Flatcar), the process in a container whose cgroup
  limited it to 4 CPUs and 32 GiB. The table was in an S3 general-purpose
  bucket in the node's region.
- **Program.** `crates/icetray-testkit/examples/bench.rs`, built with
  Cargo's standard release profile at commit `920b2e9` on the host, with
  `--rewrite-location` reading the table from S3 and
  `--multi-get-fan-out` set per run. Lookups run in process through
  `Table` over `Storage`, with no gRPC, against a 1 MiB page cache, so
  nearly every hit fetches its value pages.
- **Table.** The bench's `long_protobuf` table: 2,000,000 long keys in
  7 buckets, the optimal layout, two value columns of about 118 bytes a
  row, `full` residency.
- **Scenarios.** `multi-get-100-cold`, a `MultiGet` of 100 random keys
  requesting one column (100 value-page GETs), and
  `multi-get-100-cold-both`, requesting both (200 GETs). Each ran with one
  task and with eight tasks issuing batches at once, 3 s of warm-up and
  15 s measured, at fan-outs of 8, 16, 32, 64, 128, 256, and 512.

## Results

A lone batch, p50 latency:

| Fan-out | 8 | 16 | 32 | 64 | 128 | 256 | 512 |
|---|---|---|---|---|---|---|---|
| One column, 100 GETs | 427 ms | 250 ms | 152 ms | 117 ms | 86 ms | 82 ms | 92 ms |
| Two columns, 200 GETs | 835 ms | 430 ms | 228 ms | 139 ms | 109 ms | 107 ms | 122 ms |

Eight concurrent batches, batches a second across all of them:

| Fan-out | 8 | 16 | 32 | 64 | 128 | 256 | 512 |
|---|---|---|---|---|---|---|---|
| One column | 12 | 20 | 32 | 46 | 54 | 52 | 51 |
| Two columns | 8 | 15 | 25 | 32 | 31 | 30 | 31 |

With eight batches, p50 latency for one column fell from 231 ms at 32 to
153 ms at 64 and 129 ms at 128, and stayed there above 128. For two
columns it fell from 291 ms at 32 to 226 ms at 64 and moved no further.

## What this does and does not establish

The shared ceiling is a property of this host and this table: 4 CPUs
decoding about 118 KB value pages, and one S3 prefix per table, whose
documented GET rate is of the same order. The run does not separate the
two, so a host with more CPUs, or a table spread over more prefixes, may
reach a higher ceiling and gain from a fan-out above 64. A deployment
with few concurrent batch callers can raise the fan-out toward 128; above
that a lone batch gains nothing here. The run measured cold batches only;
a warm batch issues no GETs and is unaffected by the fan-out.
