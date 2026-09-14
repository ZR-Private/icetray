# Serving layout contract

## Problem

icetray serves a lookup by reading the Parquet data files a batch pipeline
already wrote, so the cost of a lookup is fixed at write time. Standard
Iceberg writers default to none of the shapes a point read wants, and values
in the workloads this is for run to kilobytes per key
([workload archetypes, archetype A](../analysis/2026-09-09-workload-archetypes.md#archetype-a-batch-computed-per-user-vectors)),
so one page shape does not fit every value. Where the layout comes from, and
whether the reader may assume it, must be settled before the index, tiers,
and doctor are designed.

## Decision

Tables will be served from a writer-produced layout (one key column of type
long, string, binary, or uuid; one value column of type binary, fixed, or string,
nulls allowed; `bucket(N, key)` partitioning;
each file sorted by key; dictionary encoding off; 1,000-row pages;
full-length column index and manifest metrics on the key; a bloom filter on
the key; zstd compression; data files of 32 to 64 MiB; value pages bounded by
bytes for large values), and the doctor will check the clauses RFD 016
(Doctor) lists rather than
the reader assuming any of it.

Each clause and its reason; the properties behind them are the writer recipe
in the
[prototype findings, method](../analysis/2026-09-10-prototype-findings.md#writer-recipe-for-the-s3-tables).

- One key column and one value column of type binary, fixed, or string,
  nulls allowed. icetray serves the value's bytes unchanged and answers a null as
  present with no value, distinct from absent (RFD 010 (Transport and client
  contract)); what a null means is the table owner's rule, and a table used
  as a membership set spends no bytes on values at all. The key's rules are
  RFD 008 (Keys).
- `bucket(N, key)` partitioning with hash distribution, so a lookup routes
  to one bucket. N is chosen once at write time; a multiple of the node
  counts a table might later be sharded across (RFD 017 (Residency tiers))
  keeps that option open.
- Each file sorted by key as a local write order, with no global sort. The
  one clause that decides correctness: an unsorted file cannot be served.
- Dictionary encoding off; a dictionary-encoded value column costs a second
  GET per hit
  ([prototype findings, finding 1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout)).
- 1,000-row pages, so a lookup decodes one key page and fetches one value
  page; the row count sets the CPU a lookup spends decoding a key page, and
  1,000 is the value the layout was measured at
  ([prototype findings, finding 1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout)).
- Value pages bounded by bytes for large values, with the writer's
  page-size check interval shortened so the bound binds; a row limit alone
  leaves pages of many megabytes per cold hit
  ([prototype findings, finding 6](../analysis/2026-09-10-prototype-findings.md#6-a-row-limit-alone-does-not-bound-page-size-for-large-values-and-both-writers-check-the-byte-limit-lazily)).
- Full-length manifest metrics on the key; truncated bounds make every file
  in a bucket report the same key range
  ([prototype findings, finding 8](../analysis/2026-09-10-prototype-findings.md#8-manifest-bounds-are-truncated-by-default)).
- Full-length column index on the key. icetray does not route by it, since
  the resident key chunks answer exactly; the clause is there for other
  readers of the same files and for the doctor's advice (RFD 016 (Doctor)).
- A bloom filter on the key, which the bloom residency of RFD 017
  (Residency tiers) serves from.
- zstd compression, the codec the layout was verified with; no codec
  comparison was run.
- Data files of 32 to 64 MiB; a small range read answers faster from a small
  object than from a large one
  ([range-GET latency, results](../analysis/2026-09-10-s3-range-get-latency.md#results)).

A table that misses a checked clause is refused, served at a penalty, or
served with advice, by tier (RFD 007 (Serving tiers)); what the doctor
checks and how it reports is RFD 016 (Doctor). Two clauses are there for
completeness and draw no finding: the codec, and dictionary encoding on the
key column, whose dictionary page costs nothing extra once the chunk is
resident. The file-size clause draws advice only above the doctor's
threshold, so a file between the target and that threshold passes
unremarked.

## Alternatives Considered

A sidecar index that icetray builds itself, so any layout serves. Rejected:
a build step and a second artifact to keep consistent with every snapshot.

An icetray-owned Parquet writer. Rejected: a standard Iceberg writer reaches
this layout with table properties and a local write order
([prototype findings, method](../analysis/2026-09-10-prototype-findings.md#writer-recipe-for-the-s3-tables),
[finding 1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout)).

Iceberg's default file-size target. Rejected: first-byte latency rises with
object size, and the default sits in the slow class
([range-GET latency, results](../analysis/2026-09-10-s3-range-get-latency.md#results)).

Bounding value pages by rows only. Rejected: both writers tried check the
byte limit lazily
([prototype findings, finding 6](../analysis/2026-09-10-prototype-findings.md#6-a-row-limit-alone-does-not-bound-page-size-for-large-values-and-both-writers-check-the-byte-limit-lazily)).

Several value columns, with the caller choosing which to read. Rejected for
the first version: a row's columns live in separate column chunks at
unrelated offsets, so each column asked for is one more range GET per cold
hit, and typed per-column values with projection are most of a query API.
Packing the row into one binary column at write time, as a serialized
protobuf whose field addition handles evolution, gives one GET instead. If
projection is ever wanted, its tier is RFD 007's call.

Refusing null values. Rejected: a key with a null value is a legitimate
row, the wire can say so without ambiguity, and refusing would rule out the
membership-set use for no gain in correctness.

Requiring the contract and refusing anything else. Rejected: off-contract
tables serve correctly at a cost
([prototype findings, finding 1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout));
pricing that cost is RFD 007's domain.

## Consequences

Table builders set about a dozen table properties and one local write order
on the key, and compaction must carry the same file-size target or undo the
layout
([range-GET latency, what the evidence implies](../analysis/2026-09-10-s3-range-get-latency.md#what-the-evidence-implies)).
The doctor (RFD 016) reports each deviation with the property that fixes it.

The contract is verified on written files, never inferred from properties
set: two bloom-filter sizing properties were no-ops on the Iceberg release
the prototype used
([prototype findings, finding 7](../analysis/2026-09-10-prototype-findings.md#7-two-iceberg-1101-writer-properties-are-no-ops)),
so the filter's presence is contract and its sizing is guidance.

Two calls are open.

The file-size target rests on one measurement
([range-GET latency, what this does and does not establish](../analysis/2026-09-10-s3-range-get-latency.md#what-this-does-and-does-not-establish))
and may move. Smaller files keep every GET in the fast size class at the
price of more files, more GETs at load, and more index bytes per key. Larger
files cut the file count, and the slow mode may begin anywhere in the
unmeasured band above the target.

Whether the contract requires the writer's adaptive skew splitting off. Off
gives each bucket sorted files that do not overlap, however many the
file-size target rolls. On is the writer's default and balances a skewed
bucket across tasks, at the price that a bucket may hold overlapping sorted
files
([prototype findings, finding 9](../analysis/2026-09-10-prototype-findings.md#9-spark-writes-several-sorted-files-per-bucket-sometimes-overlapping)),
a cost finding RFD 007 (Serving tiers) prices.
