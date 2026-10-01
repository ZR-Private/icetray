# Writes between builds

## Problem

The tables icetray serves are rebuilt on a schedule and change between
rebuilds through appends and upserts ([workload archetypes, archetype A](../analysis/2026-09-09-workload-archetypes.md#archetype-a-batch-computed-per-user-vectors)).
A copy-on-write writer rewrites the affected data files; a merge-on-read
writer adds delete files (positional deletes, equality deletes, or
deletion vectors) that every reader must apply. A lookup answers from the
resident index and at most one value page (RFD 006, Lookup hot path), and
applying delete files on that path was never priced or exercised
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)).

## Decision

Snapshots whose data files are the whole truth (appends and copy-on-write
upserts) will be served, re-indexing only the files that changed; snapshots
carrying delete files or deletion vectors (merge-on-read) will be refused in
the first version, with a compaction-advanced serving ref as the recommended
pattern and native merge-on-read reads as a later cost tier.

Whole-truth snapshots need nothing new: under RFD 005 (Generations and
refresh) a refresh pays only for the files that changed.

The recommended pattern for merge-on-read writers: writers use any write
mode on the table's main branch; a compaction job rewrites the deletes into
data files and moves a serving ref (a tag or a branch); icetray follows
that ref. The served snapshot lags by the compaction interval.

A delete file in the followed snapshot is a named refusal reason under
RFD 007 (Serving tiers). The refusal is of the snapshot: the last good
generation keeps serving, and the table advances once the ref moves to a
snapshot without delete files. At boot, RFD 005's start-or-fail rule
applies.

The later cost tier would apply deletion vectors and equality deletes newest
file first, priced by uncompacted deltas per bucket, a new RFD 007 penalty
unit.

## Alternatives Considered

Native merge-on-read in the first version. Rejected: the per-lookup cost of
applying deletes is unpriced, and the prototype never exercised a snapshot
with a delete file ([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)).

Serve only full rebuilds. Rejected: appends and copy-on-write upserts
produce data files that are the truth, and refresh already pays only for the
files that changed ([prototype findings, finding 3](../analysis/2026-09-10-prototype-findings.md#3-load-time)
and [finding 5](../analysis/2026-09-10-prototype-findings.md#5-snapshot-advance-on-s3)).

Serve merge-on-read snapshots by ignoring their delete files. Rejected: it
returns deleted rows as live.

## Consequences

Table owners who need freshness finer than the compaction interval compact
more often or accept the lag.

A delete file on the followed ref stalls the table until compaction catches
up, and that condition has to be visible to whoever runs the table.

The prototype's delete-file refusal was unreachable
([prototype findings, finding 15](../analysis/2026-09-10-prototype-findings.md#15-what-reviews-found-that-tests-did-not));
under RFD 014 (Test layering) a fixture whose snapshot carries a delete
file pins it.

Open questions:

- How a table stuck behind a delete file is made visible. RFD 005 leaves
  per-table lifecycle states with a health surface as an open question, and
  a stuck table is a natural state in that model; short of that, the named
  refusal reason in logs and metrics plus the age of the served snapshot may
  be all an operator needs.
- Whether the recommended serving ref is a tag or a branch. A tag is the
  simpler pointer and cannot receive writes by accident; a branch carries
  its own snapshot retention, which is one answer to the expiration hazard
  RFD 005 describes.
