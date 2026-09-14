# Serving tiers

## Problem

icetray serves tables it did not write, and tables that miss a clause of
the contract (RFD 002 (Serving layout contract)) do not all miss it in the
same way. An unsorted file cannot be searched and a duplicate key has two
answers, so a lookup may be wrong. A dictionary-encoded value column
([prototype findings, finding 1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout))
or overlapping files in a bucket
([prototype findings, finding 9](../analysis/2026-09-10-prototype-findings.md#9-spark-writes-several-sorted-files-per-bucket-sometimes-overlapping))
answer correctly at a price per lookup.
Truncated manifest bounds change nothing a client can observe. The server
has to decide at load whether a table serves, the doctor has to tell a table
owner what a deviation costs, and the two must agree on one classification.

## Decision

A table icetray cannot serve correctly (unsorted files, null or duplicate
keys, a key column whose physical type does not match the schema, a value
column that is not binary, fixed, or string, delete files, keys over 4,096
bytes, and the residency refusals of RFD 017 (Residency tiers)) will be
refused at load with a named reason; a
table it can serve at a cost (a dictionary-encoded value column, key pages
over 2,000 rows, no bucket partition, overlapping files in a bucket) will be
served with its penalty measured and reported, and refused only when the
table's `strict` setting is on; tuning advice will never affect whether a
table serves.

The tiers are refusal, cost, and advice, and the line between them is what
a client could observe. A refusal means icetray cannot serve the table as
configured: an answer could be wrong, or the table cannot be held the way
its residency (RFD 017 (Residency tiers)) requires.
A cost finding means every answer is right and some lookups do more work,
so it carries its penalty in one of three units: extra GETs per hit, key
bytes or rows decoded per lookup, or files probed per lookup. Advice affects
memory, load time, or tuning, and nothing a lookup returns.

Placements settled elsewhere:

- Missing or truncated manifest bounds are advice: routing uses each data
  file's exact key range from the resident index (RFD 004 (Resident
  key-chunk index)), so bounds that make every file in a bucket look alike
  ([prototype findings, finding 8](../analysis/2026-09-10-prototype-findings.md#8-manifest-bounds-are-truncated-by-default))
  are not on the lookup path.
- A dictionary-encoded key column costs nothing extra, since its dictionary
  page is in the resident chunk; a dictionary-encoded value column costs
  one extra GET per hit.
- Overlapping files in a bucket, and a table with no bucket partition, are
  priced in files probed per lookup; a duplicate key across overlapping
  files is a refusal, detected as RFD 005 (Generations and refresh)
  describes ([prototype findings, finding 9](../analysis/2026-09-10-prototype-findings.md#9-spark-writes-several-sorted-files-per-bucket-sometimes-overlapping)).
- A value column that is not binary, fixed, or string is refused because
  the wire carries a value as opaque bytes (RFD 010 (Transport and client
  contract)), so a typed column has no defined encoding and any answer
  would be a guess; RFD 002 (Serving layout contract) states the clause.
- Delete files are refused; RFD 009 (Writes between builds) defers
  merge-on-read snapshots.
- Three refusals belong to RFD 017 (Residency tiers) and are named there: a
  full-tier load over its resident-bytes budget, disk residency with no
  local directory configured, and bloom residency on a table whose files
  carry no key bloom filter.
- The key length cap is RFD 008's (Keys); it is a refusal rather than a
  cost because the resident index and its fence array hold whole keys, so
  the cap is the bound on what a key may cost there, and no penalty unit
  expresses memory ([prototype findings, finding 13](../analysis/2026-09-10-prototype-findings.md#13-key-length)).

`strict` is per table and defaults to on. The doctor applies the same
classification as the load path; what it reports, and how, is RFD 016
(Doctor).

## Alternatives Considered

One severity ladder (fail, warn, info). Rejected: it mixed "wrong answers"
with "slow answers", and a warning worth one extra GET per hit looked the
same to the server as one about truncated manifest bounds, when only the
first should bear on whether to serve.

Refuse everything off-contract. Rejected: those tables serve correctly
(finding 1), and a contract that gates serving makes every writer default
an outage.

Refuse overlapping files. Rejected: overlap without duplicates is a cost,
and a batch writer produced it at its default settings in one of two writes
(finding 9).

## Consequences

Every refusal reason and cost finding has a name that appears in logs, the
doctor's output, and the metrics. The penalty vocabulary is fixed to the
three units above; a cost finding that fits none of them is advice or a
refusal.

Open question: whether `strict` should stay on by default. On means a
regressed layout fails loudly at load, before it reaches clients, and a
table owner who wants a slow table served turns it off knowingly, one line
per table; the price is a refusal at first contact for an existing table
that would have served correctly. Off means a slow table serves and the
doctor tells you, the way a database runs a full table scan rather than
refusing the query, and a build pipeline can still get a loud failure from
the doctor's exit code (RFD 016). The default is one line to change.
