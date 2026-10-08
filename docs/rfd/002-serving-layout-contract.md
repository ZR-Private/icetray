# Serving layout contract

> Updated 2026-10-07: a configured column is found in each data file by
> its name and the field id the snapshot's schema gives it, so a value
> column can be added to a served table without a rewrite, and a served
> column is not renamed. See RFD 023.

> Updated 2026-10-07: a table serves one or more value columns, or none
> for a membership set, each read only when a lookup names it, and one
> packed column performs best. A null is a value of a present row. See
> RFD 022.

## Problem

icetray serves a lookup by reading the Parquet data files a batch pipeline
already wrote, so the cost of a lookup is fixed at write time. Standard
Iceberg writers default to none of the shapes a point read wants. Values
in the workloads icetray is for run to kilobytes per key
([workload archetypes, archetype A](../analysis/2026-09-09-workload-archetypes.md#archetype-a-batch-computed-per-user-vectors)),
so one page shape does not fit every value. Before the index, the tiers,
and the doctor can be designed, it has to be settled where the layout
comes from and whether the reader may assume it.

## Decision

Tables will be served from a layout their writer produces, and the
doctor will check the clauses RFD 016 (Doctor) lists rather than the
reader assuming any of it. The layout is one key column of type long,
string, binary, or uuid; one or more value columns of type binary,
fixed, or string, with nulls allowed, one packed column performing best;
`bucket(N, key)` partitioning; each file sorted by key; dictionary
encoding off; 1,000-row pages; a full-length column index and manifest
metrics on the key; a bloom filter on the key; zstd compression; data
files of 32 to 64 MiB; and value pages bounded by bytes for large
values.

The clauses and their reasons follow. The table properties that produce
them are the writer recipe in the
[prototype findings, method](../analysis/2026-09-10-prototype-findings.md#writer-recipe-for-the-s3-tables).

- One key column and one or more value columns of type binary, fixed, or
  string, with nulls allowed (RFD 022 (Value columns)). icetray serves
  each value's bytes unchanged and answers a null as a null value of a
  present row, which is distinct from absent and from a zero-length value
  (RFD 010 (Transport and client contract)). What a null means is the
  table owner's rule, and a table used as a membership set names no value
  columns at all. The key's rules are in RFD 008 (Keys).
- One packed column performs best, since each column a lookup names adds
  a range GET to its cold hit. A row packed into one binary column at
  write time, as a serialized protobuf whose field addition handles
  evolution, costs one GET. A table read by several kinds of caller
  splits its row along how it is read: fields read together share a
  column, and a large part read on its own, such as an embedding, gets a
  column of its own.
- Columns found by name and field id, as Iceberg's writers record them
  (RFD 023 (Column resolution)). A value column added to the schema after
  a file was written answers its default in that file, so a column can be
  added without a rewrite. A served column is not renamed: a new column is
  added beside it, written, and moved to by the callers, and the old one
  is dropped from the clients, then the configuration, then the schema.
- `bucket(N, key)` partitioning with hash distribution, so that a lookup
  routes to one bucket. N is chosen once, at write time. Choosing a
  multiple of the node counts a table might later be sharded across
  (RFD 017 (Residency tiers)) keeps that option open.
- Each file sorted by key, as a local write order; the table needs no
  global sort. Correctness depends on this clause alone, because an
  unsorted file cannot be served.
- Dictionary encoding off, since a dictionary-encoded value column costs a
  second GET per hit
  ([prototype findings, finding 1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout)).
- 1,000-row pages, so that a lookup decodes one key page and fetches one
  value page per column it names. The row count sets the CPU a lookup
  spends decoding a key page, and 1,000 is the count behind the layout's
  measurements ([prototype findings, finding
  1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout)).
- Value pages bounded by bytes for large values, with the writer's
  page-size check interval shortened so that the bound takes effect. A row
  limit alone leaves pages of many megabytes per cold hit
  ([prototype findings, finding 6](../analysis/2026-09-10-prototype-findings.md#6-a-row-limit-alone-does-not-bound-page-size-for-large-values-and-both-writers-check-the-byte-limit-lazily)).
- Full-length manifest metrics on the key, because truncated bounds make
  every file in a bucket report the same key range
  ([prototype findings, finding 8](../analysis/2026-09-10-prototype-findings.md#8-manifest-bounds-are-truncated-by-default)).
- A full-length column index on the key. icetray does not route by it,
  since the resident key chunks answer exactly. The clause is there for
  other readers of the same files and for the doctor's advice (RFD 016
  (Doctor)).
- A bloom filter on the key, which the bloom residency of RFD 017
  (Residency tiers) serves from.
- zstd compression. It is the codec behind the layout's evidence, and the
  clause makes no claim about any other codec.
- Data files of 32 to 64 MiB, because a small range read answers faster
  from a small object than from a large one. That holds warm
  ([range-GET latency, results](../analysis/2026-09-10-s3-range-get-latency.md#results))
  and cold, where the step falls lower, between 16 and 64 MiB, so a 64 MiB
  file read cold is already in the slow class
  ([cold range-GET latency, results](../analysis/2026-09-29-cold-range-get-latency.md#results)).
  Whether to lower the target is open, below.

A table that misses a checked clause is refused, served as inefficient,
or served with advice, depending on its tier (RFD 007 (Serving tiers)).
What the doctor checks and how it reports is RFD 016 (Doctor). Two
clauses are listed for completeness and draw no finding. One is the codec.
The other is dictionary encoding on the key column, whose dictionary page
costs nothing extra once the chunk is resident. The file-size clause draws
advice only above the doctor's threshold, so a file between the target and
that threshold passes unremarked.

## Alternatives Considered

A sidecar index that icetray builds itself, so that any layout serves.
Rejected because it adds a build step and a second artifact that has to be
kept consistent with every snapshot.

An icetray-owned Parquet writer. Rejected because a standard Iceberg
writer reaches this layout with table properties and a local write order
([prototype findings, method](../analysis/2026-09-10-prototype-findings.md#writer-recipe-for-the-s3-tables),
[finding 1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout)).

Iceberg's default file-size target. Rejected because first-byte latency
rises with object size, and the default sits in the slow class
([range-GET latency, results](../analysis/2026-09-10-s3-range-get-latency.md#results)).

Bounding value pages by rows only. Rejected because the writers examined
both check the byte limit lazily
([prototype findings, finding 6](../analysis/2026-09-10-prototype-findings.md#6-a-row-limit-alone-does-not-bound-page-size-for-large-values-and-both-writers-check-the-byte-limit-lazily)).

Typed value columns, such as a long or a double read by name. Rejected
because such a value has no "bytes unchanged" form, and typed per-column
values with projection amount to most of a query API. Several columns of
bytes, each read only when a lookup names it, are RFD 022 (Value
columns). If projection inside a packed column is ever wanted, which
tier it belongs to is RFD 007's call.

Refusing null values. Rejected because a key with a null value is a
legitimate row and the wire can say so without ambiguity. Refusing nulls
would rule out the membership-set use and gain nothing in correctness.

Requiring the contract and refusing every other table. Rejected because
off-contract tables serve correctly, if inefficiently
([prototype findings, finding 1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout)),
and measuring that inefficiency is RFD 007's domain.

## Consequences

Table builders set about a dozen table properties and one local write
order on the key, and compaction must carry the same file-size target or
it will undo the layout
([range-GET latency, what the evidence implies](../analysis/2026-09-10-s3-range-get-latency.md#what-the-evidence-implies)).
The doctor (RFD 016) reports each deviation with the property that fixes
it.

The contract is verified on the files as written and never inferred from
the properties that were set. Two bloom-filter sizing properties are
no-ops in the Iceberg release behind the findings
([prototype findings, finding 7](../analysis/2026-09-10-prototype-findings.md#7-two-iceberg-1101-writer-properties-are-no-ops)),
so the filter's presence is contract and its sizing is guidance.

Two calls are open.

The file-size target rests on two measurements
([range-GET latency, what this does and does not establish](../analysis/2026-09-10-s3-range-get-latency.md#what-this-does-and-does-not-establish);
[cold range-GET latency, what this does and does not establish](../analysis/2026-09-29-cold-range-get-latency.md#what-this-does-and-does-not-establish))
and may move. Read cold, as most cold hits are, a 64 MiB file answers like
a 256 MiB one, tens of milliseconds slower at the median than a 16 MiB
one. Smaller files keep every GET in the fast size class at the price of
more files, more GETs at load, and more index bytes per key. Larger files
cut the file count, but the slow mode may begin anywhere in the band above
the target, which the evidence does not cover.

The other is whether the contract requires the writer's adaptive skew
splitting to be off. With it off, each bucket gets sorted files that do
not overlap, however many the file-size target rolls. On is the writer's
default and balances a skewed bucket across tasks, at the price that a
bucket may hold overlapping sorted files
([prototype findings, finding 9](../analysis/2026-09-10-prototype-findings.md#9-spark-writes-several-sorted-files-per-bucket-sometimes-overlapping)),
which RFD 007 (Serving tiers) reports as an inefficient finding.
