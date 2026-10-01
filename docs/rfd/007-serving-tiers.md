# Serving tiers

## Problem

icetray serves tables it did not write, and deviations from the contract
(RFD 002 (Serving layout contract)) differ in kind. An unsorted file or a
duplicate key makes a lookup wrong. A dictionary-encoded value column
([prototype findings, finding 1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout))
or overlapping files in a bucket
([prototype findings, finding 9](../analysis/2026-09-10-prototype-findings.md#9-spark-writes-several-sorted-files-per-bucket-sometimes-overlapping))
answer correctly at a price per lookup. Truncated manifest bounds change
nothing a client can observe. The server and the doctor need one
classification.

## Decision

A table icetray cannot serve correctly (unsorted files, null or duplicate
keys, keys outside their file's recorded bucket, a key column whose
physical type does not match the schema, a value column that is not
binary, fixed, or string, delete files, keys over 4,096 bytes, and the
residency refusals of RFD 017 (Residency tiers)) will be refused at load
with a named reason; a table it can serve at a cost (a dictionary-encoded
value column, key pages over 2,000 rows, no bucket partition, overlapping
files in a bucket) will be served with its penalty measured and reported,
and refused only when the table's `strict` setting is on; tuning advice
will never affect whether a table serves.

The line between tiers is what a client could observe:

- Refusal: an answer could be wrong, or the table cannot be held the way
  its residency (RFD 017 (Residency tiers)) requires.
- Cost: every answer is right and some lookups do more work. The penalty
  is in one of three units: extra GETs per hit, key bytes or rows decoded
  per lookup, or files probed per lookup.
- Advice: memory, load time, or tuning.

Placements:

- Missing or truncated manifest bounds are advice. Routing uses the
  resident index's exact key ranges (RFD 004 (Resident key-chunk index)),
  so manifest bounds
  ([prototype findings, finding 8](../analysis/2026-09-10-prototype-findings.md#8-manifest-bounds-are-truncated-by-default))
  are off the lookup path.
- A dictionary-encoded key column costs nothing extra; its dictionary page
  is in the resident chunk. A dictionary-encoded value column costs one
  extra GET per hit.
- Overlapping files in a bucket, and no bucket partition, are priced in
  files probed per lookup. A duplicate key across overlapping files is a
  refusal, detected as RFD 005 (Generations and refresh) describes
  ([prototype findings, finding 9](../analysis/2026-09-10-prototype-findings.md#9-spark-writes-several-sorted-files-per-bucket-sometimes-overlapping)).
- Keys outside their file's recorded bucket are a refusal: routing takes
  each file's bucket from its manifest entry, so such a key answers
  absent. The load hashes each new file's fences (the first key of every
  key page, RFD 004) and its first and last key; a file whose keys change
  bucket between two fences still serves, since hashing every key costs
  tens of nanoseconds per row, seconds to minutes on a large table. The
  doctor hashes every key and refuses it under the same reason (RFD 016).
- Value pages whose mean uncompressed size is above the 64 MiB value page
  ceiling (RFD 004) are advice; a lookup into such a page is refused with
  a file fault. The mean can miss one oversized page or be raised by a
  large value dictionary, so it does not decide whether the table serves
  (RFD 016).
- A value column that is not binary, fixed, or string is refused: the wire
  carries a value as opaque bytes (RFD 010 (Transport and client
  contract)), so a typed column has no defined encoding (RFD 002).
- Delete files are refused. RFD 009 (Writes between builds) defers
  merge-on-read snapshots.
- Two refusals are RFD 017's: a full-tier load over its resident-bytes
  budget, and bloom residency on a table whose files carry no key bloom
  filter. Disk residency with no local directory configured is a
  configuration violation (RFD 011).
- The key length cap is RFD 008's (Keys); it is a refusal because the
  resident index holds whole keys and no penalty unit expresses memory
  ([prototype findings, finding 13](../analysis/2026-09-10-prototype-findings.md#13-key-length)).

`strict` is per table and defaults to on. The doctor applies the same
classification; its reporting is RFD 016 (Doctor).

## Alternatives Considered

One severity ladder (fail, warn, info). Rejected: it mixed wrong answers
with slow ones, and only the first should bear on whether to serve.

Refuse everything off-contract. Rejected: those tables serve correctly
(finding 1), and every writer default would be an outage.

Refuse overlapping files. Rejected: overlap without duplicates is a cost,
and a batch writer produced it at default settings in one of two writes
(finding 9).

## Consequences

Every refusal reason and cost finding has a name that appears in logs, the
doctor's output, and the metrics. A cost finding that fits none of the
three units is advice or a refusal.

Open question: whether `strict` should stay on by default. On: a regressed
layout fails loudly at load, at the price of refusing an existing table
that would have served correctly. Off: a slow table serves, the doctor
reports it, and a build pipeline still gets a loud failure from the
doctor's exit code (RFD 016). The default is one line to change.
