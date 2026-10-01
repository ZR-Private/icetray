# Serving layout contract

## Problem

A lookup's cost is fixed at write time, in the Parquet files a batch
pipeline wrote. Standard Iceberg writers default to none of the shapes a
point read wants, and values run to kilobytes per key
([workload archetypes, archetype A](../analysis/2026-09-09-workload-archetypes.md#archetype-a-batch-computed-per-user-vectors)),
so one page shape does not fit every value.

## Decision

Tables will be served from a writer-produced layout (one key column of type
long, string, binary, or uuid; one value column of type binary, fixed, or
string, nulls allowed; `bucket(N, key)` partitioning; each file sorted by
key; dictionary encoding off; 1,000-row pages; full-length column index and
manifest metrics on the key; a bloom filter on the key; zstd compression;
data files of 32 to 64 MiB; value pages bounded by bytes for large values),
and the doctor will check the clauses RFD 016 (Doctor) lists rather than
the reader assuming any of it.

The writer properties behind the clauses are in the
[prototype findings, method](../analysis/2026-09-10-prototype-findings.md#writer-recipe-for-the-s3-tables).

- One key column; one value column of type binary, fixed, or string, nulls
  allowed. Value bytes are served unchanged; a null is present with no
  value, distinct from absent (RFD 010 (Transport and client contract)),
  and its meaning is the table owner's rule; a membership-set table spends
  no bytes on values. Keys: RFD 008 (Keys).
- `bucket(N, key)` partitioning with hash distribution. N is chosen once at
  write time; a multiple of the node counts a table might be sharded across
  (RFD 017 (Residency tiers)) keeps that option open.
- Each file sorted by key, as a local write order; a global sort is not
  required. An unsorted file cannot be served.
- Dictionary encoding off; a dictionary-encoded value column costs a second
  GET per hit
  ([prototype findings, finding 1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout)).
- 1,000-row pages, the value the layout was measured at
  ([prototype findings, finding 1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout)).
- Value pages bounded by bytes for large values, with the writer's
  page-size check interval shortened so the bound binds
  ([prototype findings, finding 6](../analysis/2026-09-10-prototype-findings.md#6-a-row-limit-alone-does-not-bound-page-size-for-large-values-and-both-writers-check-the-byte-limit-lazily)).
- Full-length manifest metrics on the key; truncated bounds make every file
  in a bucket report the same key range
  ([prototype findings, finding 8](../analysis/2026-09-10-prototype-findings.md#8-manifest-bounds-are-truncated-by-default)).
- Full-length column index on the key, for other readers of the same files
  and for the doctor's advice (RFD 016 (Doctor)).
- A bloom filter on the key, which the bloom residency of RFD 017
  (Residency tiers) serves from.
- zstd compression, the codec the layout was verified with; no codec
  comparison was run.
- Data files of 32 to 64 MiB. First-byte latency rises with object size,
  warm
  ([range-GET latency, results](../analysis/2026-09-10-s3-range-get-latency.md#results))
  and cold, where the step falls between 16 and 64 MiB
  ([cold range-GET latency, results](../analysis/2026-09-29-cold-range-get-latency.md#results)).
  Whether to lower the target is open, below.

A table that misses a checked clause is refused, served at a penalty, or
served with advice, by tier (RFD 007 (Serving tiers)); the checks are RFD
016 (Doctor). The codec and dictionary encoding on the key column draw no
finding; the file-size clause draws advice only above the doctor's
threshold.

## Alternatives Considered

A sidecar index that icetray builds itself. Rejected: a build step and a
second artifact to keep consistent with every snapshot.

An icetray-owned Parquet writer. Rejected: a standard Iceberg writer reaches
this layout with table properties and a local write order
([prototype findings, method](../analysis/2026-09-10-prototype-findings.md#writer-recipe-for-the-s3-tables),
[finding 1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout)).

Iceberg's default file-size target. Rejected: it sits in the slow class
([range-GET latency, results](../analysis/2026-09-10-s3-range-get-latency.md#results)).

Bounding value pages by rows only. Rejected: both writers tried check the
byte limit lazily
([prototype findings, finding 6](../analysis/2026-09-10-prototype-findings.md#6-a-row-limit-alone-does-not-bound-page-size-for-large-values-and-both-writers-check-the-byte-limit-lazily)).

Several value columns, with the caller choosing which to read. Rejected for
the first version: each column read is one more range GET per cold hit, and
projection is most of a query API. A serialized protobuf in one binary
column gives one GET; projection's tier is RFD 007's call.

Refusing null values. Rejected: a key with a null value is a legitimate
row, and refusing would rule out the membership-set use.

Requiring the contract and refusing anything else. Rejected: off-contract
tables serve correctly at a cost
([prototype findings, finding 1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout));
pricing it is RFD 007's domain.

## Consequences

Table builders set about a dozen table properties and one local write
order; compaction must carry the same file-size target
([range-GET latency, what the evidence implies](../analysis/2026-09-10-s3-range-get-latency.md#what-the-evidence-implies)).
The doctor (RFD 016) reports each deviation with the property that fixes it.

The contract is verified on written files: two bloom-filter sizing
properties were no-ops on the Iceberg release the prototype used
([prototype findings, finding 7](../analysis/2026-09-10-prototype-findings.md#7-two-iceberg-1101-writer-properties-are-no-ops)),
so the filter's presence is contract and its sizing is guidance.

Two calls are open.

The file-size target rests on two measurements
([range-GET latency, what this does and does not establish](../analysis/2026-09-10-s3-range-get-latency.md#what-this-does-and-does-not-establish);
[cold range-GET latency, what this does and does not establish](../analysis/2026-09-29-cold-range-get-latency.md#what-this-does-and-does-not-establish))
and may move. Cold, a 64 MiB file answers like a 256 MiB one, about 55 ms
slower at the median than a 16 MiB one. Smaller files keep every GET in
the fast size class at the price of more files, more GETs at load, and more
index bytes per key; larger files cut the file count, and the slow mode may
begin anywhere in the unmeasured band above the target.

Whether the contract requires the writer's adaptive skew splitting off. Off
gives each bucket sorted files that do not overlap. On, the writer's
default, balances a skewed bucket across tasks and may leave overlapping
sorted files
([prototype findings, finding 9](../analysis/2026-09-10-prototype-findings.md#9-spark-writes-several-sorted-files-per-bucket-sometimes-overlapping)),
a cost finding RFD 007 (Serving tiers) prices.
