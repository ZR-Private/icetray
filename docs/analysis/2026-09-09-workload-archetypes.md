# Workload archetypes

Three shapes recur among batch-built, read-by-key datasets: large per-user
vectors with a small active set (A), small JSON pages read cold (B), and
roughly 100-byte per-user feature rows (C). All are rebuilt on a schedule,
and A's dormant tail is read sparsely relative to its size (B and C are
assumed to be), so a design for serving them from the lake would be judged
on rebuild cost, index bytes per key, and bounded cold-read latency more
than on cache hit rate. This document does not claim that any particular
workload should move; A is the most detailed example, not a design target,
and a design for this layer would need to hold for at least these three
shapes.

No workload figure here is measured. The shapes are illustrative round
numbers, and where a figure is an inference or an assumption it says so.
The index-size figures in the cost section are the prototype's
measurements, cited from the findings document.

## Where the workloads come from

Batch jobs (Spark, dbt) compute per-entity results in a data lake and copy
them into a managed key-value store so that online services can read them by
key. The store holds a second copy of data the lake already has, rebuilt on
the job's schedule whether or not anyone reads it, and an owner who rebuilds
a table whole tends to write a rebuild-and-swap procedure around the store:
write the new version to a fresh table, flip a pointer, delete the old. The
copy step is removable: the lake already holds the data in Parquet, which
supports point reads when files are sorted and bucketed.

## Archetype A: batch-computed per-user vectors

- **Shape.** One value per user, 4 to 16 KB (an embedding or a feature
  vector), keyed by a high-cardinality string or composite id. Tens to
  hundreds of millions of keys. Rebuild cadence: daily is the working
  assumption.
- **Access.** A small fraction of users is active at any time. The working
  assumption is that 90 to 99 percent of users are dormant after a few
  interactions and that the dormant tail grows monotonically. A lake query
  for the share of users with no activity in the last N days, for a few N,
  settles it for a given dataset. Request rate and tail-latency budget on
  the request path vary by deployment.
- **Why the read rate looks high.** A store with no cold tier is read at a
  high rate because every user, active or dormant, is read from the same
  place. The miss rate of a small online store would be the real request
  rate for a cold tier: whatever share of reads is for dormant users. A
  dormancy share bounds users, not requests; if reads track activity that
  share is small, if reads are spread across all users it is not.
- **One deployment that fits.** A small online store for live users in front
  of the lake-served tail; the client reads the online store first and falls
  through, or fans out concurrently, to the lake-served tier. That tier
  would then need bounded tail latency on the fall-through path and partial
  results on multi-key reads, so the online store can answer what it has
  while the tail fills in the rest. Hedging (a duplicate request when the
  first is slow; whichever answers first wins) is the usual mechanism; the
  prototype's fixed-delay hedging did not bound the tail on S3 and an
  adaptive delay is untested
  ([prototype findings, finding 10](2026-09-10-prototype-findings.md#10-hedging)).
  Where the
  client/server boundary sits is a design decision, not a property of the
  workload.
- **Implication for builds.** A growing tail rewards incremental rebuilds at
  bucket granularity: rewrite only the buckets whose keys changed. Iceberg
  snapshots reference unchanged data files by path, so a server that diffs
  two snapshots reloads only the files that changed; the prototype did this
  ([prototype findings, finding 5](2026-09-10-prototype-findings.md#5-snapshot-advance-on-s3)).

## Archetype B: content pages

- **Shape.** A few kilobytes of JSON per page, rebuilt weekly. Key count and
  key type vary.
- **Access.** Low request rate (assumption), with reads spread across the
  key space because crawlers visit everything, so nearly every read is cold.
- **What it would stress.** Under that assumption, load time and index bytes
  per key matter more than cache hit rate.

## Archetype C: per-user feature rows

- **Shape.** About 100 bytes per user, rebuilt daily. Hundreds of millions
  of keys is the working assumption.
- **Access.** Assumed to be read on request paths for active users, so A's
  hot/cold split would apply, with much smaller values.
- **What it would stress.** Bytes per key in the in-memory index relative to
  a 100-byte value, and the cost of the full daily rebuild.

## The cost shape

A managed key-value store bills for every item written, so a full rebuild
costs the whole table's worth of writes every cycle, whether or not anyone
reads the result. Storage is billed again on top of the lake copy. Reads are
billed per request. Serving from the lake replaces the per-item rebuild cost
with the batch job's own output writes (one object per data file, so
hundreds to tens of thousands of objects depending on table and file size),
keeps one copy of the data, pays per GET on reads, and, if the serving layer
keeps an in-memory index that answers key-absent without a fetch (a design
assumption, not a property of the lake), pays nothing for a miss. It adds
the memory of the serving replicas, which scales with key count: the
prototype measured 1.5 to 2.1 bytes per 64-bit key on files of 100,000 to
2,000,000 rows, 4.3 bytes per 91-byte string key, several times that when
files hold only thousands of rows (as large-value tables do at tens of MiB
per file), and process memory about 1.2 times the index
([prototype findings, finding 2](2026-09-10-prototype-findings.md#2-resident-index-size)).
Which side wins depends on the ratio of rebuild volume to read volume; for
archetypes rebuilt on a schedule and read sparsely (A's tail, B, C) the
rebuild writes are expected to be the larger share of the bill. Exact list
prices belong in a decision-time comparison, not here.

## A worked cost example

Added 2026-09-10. Archetype A at one billion keys and 8 KB values, rebuilt
daily, keys as 16-byte UUIDs, served by two replicas. Prices are approximate
on-demand list prices in one US region as of September 2026 and must be
verified before any decision: memory-optimized compute about $5 per GiB-month
with one vCPU per 8 GiB included; S3 storage about $0.023 per GB-month and
GETs about $0.40 per million; the managed key-value store about $0.625 per
million 1 KB write units, $0.125 per million 8 KB eventually consistent read
units, and $0.25 per GB-month of storage.

| item | serving from the lake, two replicas | managed key-value store |
|---|---|---|
| serving compute | 2 × 20 GiB (17.5 GB of index per billion UUID keys, [key bytes by shape](2026-09-10-key-bytes-by-shape.md#sizing-rule)), about $200 to $250 per month | none |
| storage | 8 TB at S3 rates, about $185, or nothing where the lake copy already exists | 8 TB, about $2,000 |
| rebuild | the batch job's own file writes, under $1 | full rewrite: 8 billion write units per day, about $150,000 per month; 10 percent incremental, about $15,000 |
| reads at 100 per second | about $100 in GETs | about $33 |
| reads at 1,000 per second | about $1,050 in GETs | about $325 |

The rebuild row dominates: a daily rewrite of the table into the managed
store costs more per day than a year of the serving replicas, and even a 10
percent incremental rebuild is fifty times the compute bill. Per read, the
object store is about three times the managed store's price for values of
8 KB or less at these rates, and the gap widens for very small values (a
100-byte read is half a read unit but still one whole GET), so the comparison
turns on the ratio of rebuild volume to cold reads. The archetypes here are
rebuilt on a schedule and read sparsely; a table read at tens of thousands
per second with a low cache hit rate would invert it, and for such a table
the online store in front, with the lake tier taking only the miss stream,
is the arrangement that keeps the economics.
