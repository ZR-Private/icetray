# Serving tiers

> Updated 2026-10-04: the middle tier, once called cost, is called
> inefficient, the word the doctor's serviceability already used; the
> tiers and their contents are unchanged.

> Updated 2026-10-04: the doctor honors each configured table's `strict`,
> as the load does. See RFD 016.

> Updated 2026-10-05: RFD 020 (Lookup levels) replaces the strict gate
> and the inefficient tier. An inefficient finding now caps the table's
> lookup level, `strict` refuses a snapshot whose layout puts the table
> below the level it declares, and `strict` defaults to off, which closes
> the open question below. The refusals, the penalty units, and advice
> are unchanged.

> Updated 2026-10-06: the refusal of delete files narrows to equality
> deletes and unreadable deletes that may apply. See RFD 021
> (Merge-on-read).

> Updated 2026-10-07: a configured column that a data file does not hold
> as the snapshot's schema means it is refused as `column_mismatch`,
> which replaces the file fault `MissingColumn`, and a file written
> before its value column was added answers the column's default. See
> RFD 023.

> Updated 2026-10-07: a table may serve several value columns. The
> `value_type_unsupported` refusal names its column, and a
> dictionary-encoded value column's extra GET falls on the hits that name
> that column. See RFD 022.

> Updated 2026-10-08: a data file whose manifest entry records a format
> other than Parquet, such as ORC or Avro, which Iceberg also allows, is
> refused as `data_format_unsupported`, from the manifests before any
> read of the file. See
> [the optimal layout's requirements](../table-layout.md#requirements).

> Updated 2026-10-14: an ordered table adds the refusals
> `duplicate_rows`, `null_order`, `order_type_unsupported`, and
> `order_type_mismatch`, the findings `shared_edge_keys`,
> `dictionary_encoded_order`, and `order_index_missing`, and the advice
> `order_index_truncated`, `long_runs`, and `large_runs`; a key repeated
> within or across its files is no duplicate. See RFD 025.

## Problem

icetray serves tables it did not write, and tables that miss a clause of
the optimal layout in RFD 002 (Serving layout) miss it in different
ways. An unsorted file cannot be searched, and a duplicate key has two
answers, so a lookup into either may be wrong. A dictionary-encoded value
column
([prototype findings, finding 1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout))
or overlapping files in a bucket
([prototype findings, finding 9](../analysis/2026-09-10-prototype-findings.md#9-spark-writes-several-sorted-files-per-bucket-sometimes-overlapping))
still answers correctly, with more work per lookup. Truncated manifest
bounds change nothing a client can observe. The server has to decide at
load whether a table serves, the doctor has to tell a table owner what a
deviation costs, and the two must agree on one classification.

## Decision

A table icetray cannot serve correctly (a data file in a format other
than Parquet, unsorted files, null or duplicate keys, keys outside their
file's recorded bucket, a key column whose physical type does not match
the schema, a configured column a data file does not hold as the schema
means it, a configured value column that is not binary, fixed, or
string, equality deletes and unreadable deletes that may apply, keys over
4,096 bytes, and the residency refusals of RFD 017 (Residency tiers))
will be refused at load with a named reason; a table it
can serve only inefficiently (a dictionary-encoded value column, key pages
over 2,000 rows, no bucket partition, overlapping files in a bucket) will
be served with its penalty measured and reported, and each such finding
caps the table's lookup level (RFD 020); tuning advice will never affect
whether a table serves.

The tiers are refusal, inefficient, and advice, and the line between them
is what a client could observe. A refusal means icetray cannot serve the
table as configured, either because an answer could be wrong or because
the table cannot be held the way its residency (RFD 017 (Residency tiers))
requires. An inefficient finding means every answer is right and some
lookups do more work. It carries its penalty in one of three units: extra
GETs per hit, key bytes or rows decoded per lookup, or files probed per
lookup. Advice concerns memory, load time, or tuning, and nothing it flags
can make a lookup's answer wrong.

Placements settled elsewhere:

- Missing or truncated manifest bounds are advice. Routing uses each data
  file's exact key range from the resident index (RFD 004 (Resident
  key-chunk index)), so bounds that make every file in a bucket look alike
  ([prototype findings, finding 8](../analysis/2026-09-10-prototype-findings.md#8-manifest-bounds-are-truncated-by-default))
  stay off the lookup path.
- A dictionary-encoded key column costs nothing extra, since its dictionary
  page is in the resident chunk. A dictionary-encoded value column is
  inefficient by one extra GET per hit that names it, and the finding
  names the column (RFD 022 (Value columns)).
- Overlapping files in a bucket, and a table with no bucket partition, are
  inefficient, measured in files probed per lookup. A duplicate key across overlapping
  files is a refusal, detected as RFD 005 (Generations and refresh)
  describes ([prototype findings, finding 9](../analysis/2026-09-10-prototype-findings.md#9-spark-writes-several-sorted-files-per-bucket-sometimes-overlapping)).
- Keys outside their file's recorded bucket are a refusal. Routing takes
  each file's bucket from its manifest entry and sends a key only to the
  bucketed files of the bucket it hashes to, so a key held in a file
  recorded under another bucket is answered as absent although it is
  present. The load hashes each new file's fences (the first key of every
  key page, RFD 004) and its first and last key under the file's own
  bucketing. That costs one hash per key page and refuses a file recorded
  under the wrong bucket. It still passes a file whose keys change bucket
  between two fences, which a writer that partitions by bucket does not
  produce. Hashing every key at every load would cost tens of nanoseconds
  per row, seconds to minutes on a large table, so the doctor is the one
  that hashes every key, and it refuses such a file under the same reason
  (RFD 016).
- Value pages whose mean uncompressed size is above the lookup's 64 MiB
  value page ceiling (RFD 004) are advice. A lookup into a page over the
  ceiling is refused with a file fault instead of being answered, so every
  answer given is right and there is nothing for a penalty unit to measure.
  The trigger is also a file's mean page size, which can miss one
  oversized page and can be pushed above the ceiling by a large value
  dictionary. It cannot name the lookups that will be refused, so it does
  not decide whether the table serves (RFD 016).
- A configured value column that is not binary, fixed, or string is
  refused as `value_type_unsupported`, naming the column, because the
  wire carries a value as opaque bytes (RFD 010 (Transport and client
  contract)). A typed column has no defined encoding there, and any answer
  would be a guess. RFD 002 (Serving layout) states the clause.
- A configured column a data file does not hold as the snapshot's schema
  means it is refused as `column_mismatch`: a column of its name under
  another field id, its field id under another name, a file that records
  no field ids and lacks the name, a file without its key column, a file
  holding the name as a nested group, a `fixed` column at another width,
  a name that is not a top-level field, and a default that does not
  read. A swap of two names would otherwise
  serve one column's bytes under the other's name. A file that records
  field ids and lacks a value column was written before the column was
  added and answers its default, which is no deviation. RFD 023 (Column
  resolution) gives the rule.
- A data file whose manifest entry records ORC, Avro, or any format other
  than Parquet is refused as `data_format_unsupported`, naming the file
  and the format. icetray reads rows from Parquet alone, and the manifest
  entry records the format, so the snapshot is refused from its manifests
  before any read of the file.
- Equality deletes and unreadable deletes that may apply are refused;
  RFD 021 (Merge-on-read) serves deletion vectors and Parquet position
  deletes and says why the rest stay refused.
- Two refusals belong to RFD 017 (Residency tiers) and are named there: a
  full-tier load over its resident-bytes budget, and bloom residency on a
  table whose files carry no key bloom filter. Disk residency with no
  local directory configured is a configuration violation under RFD 011
  rather than a refusal.
- The key length cap belongs to RFD 008 (Keys). It is a refusal rather
  than inefficient because the resident index and its fence array hold whole
  keys, so the cap is the bound on what a key may cost there, and memory
  is not one of the penalty units
  ([prototype findings, finding 13](../analysis/2026-09-10-prototype-findings.md#13-key-length)).

`strict` is per table; its meaning and its default, off, are RFD 020's
(Lookup levels). The doctor applies the same classification as the load
path and the same `strict` to a configured table; what it reports, and
how, is RFD 016 (Doctor).

## Alternatives Considered

One severity ladder (fail, warn, info). Rejected because it mixed wrong
answers with slow ones. A warning worth one extra GET per hit looked the
same to the server as one about truncated manifest bounds, when only the
first should bear on whether to serve.

Refuse everything outside the optimal layout. Rejected because those
tables serve correctly (finding 1), and an optimal layout that gates
serving would turn every writer default into an outage.

Refuse overlapping files. Rejected because overlap without duplicates is
inefficient, not wrong, and a batch writer at its default settings can produce it
(finding 9).

## Consequences

Every refusal reason and inefficient finding has a name that appears in
logs, in the doctor's output, and in the metrics. The penalty vocabulary is
fixed to the three units above, so an inefficient finding that fits none of
them is advice or a refusal.

The open question of whether `strict` should stay on by default was
closed by RFD 020 (Lookup levels), which chose off. On means a regressed
layout fails loudly at load, before it reaches clients, and a table
owner who wants an inefficient table served turns it off knowingly, with
one line per table. The price is a refusal at first contact for an
existing table that would have served correctly. Off means an
inefficient table serves and the doctor tells you, the way a database
runs a full table scan rather than refusing the query, and a build
pipeline can still get a loud failure from the doctor's exit code (RFD
016). The default is one line to change.
