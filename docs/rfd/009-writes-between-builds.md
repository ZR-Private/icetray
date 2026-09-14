# Writes between builds

## Problem

The tables icetray serves are rebuilt on a schedule and change between
rebuilds through appends and upserts ([workload archetypes, archetype A](../analysis/2026-09-09-workload-archetypes.md#archetype-a-batch-computed-per-user-vectors)).
Iceberg records a change in one of two ways. A copy-on-write writer rewrites
the affected data files, so the new snapshot's data files are the whole
truth. A merge-on-read writer leaves the data files alone and adds delete
files (positional deletes, equality deletes, or deletion vectors) that every
reader must apply before it answers.

A lookup in icetray answers from the resident index and at most one value
page, as RFD 006 (Lookup hot path) decides. Applying delete files on that
path is work the prototype never priced or exercised
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)),
and skipping them returns rows the writer deleted. The table needs a rule
for which snapshots it serves.

## Decision

Snapshots whose data files are the whole truth (appends and copy-on-write
upserts) will be served, re-indexing only the files that changed; snapshots
carrying delete files or deletion vectors (merge-on-read) will be refused in
the first version, with a compaction-advanced serving ref as the recommended
pattern and native merge-on-read reads as a later cost tier.

Whole-truth snapshots need nothing new: under RFD 005 (Generations and
refresh) a refresh already pays only for the files that changed, so an
append or upsert costs its delta.

The recommended pattern for tables whose writers use merge-on-read: writers
use whatever write mode they like on the table's main branch; a compaction
job rewrites the deletes into data files and moves a serving ref (a tag or a
branch); icetray follows that ref. The served snapshot lags by the compaction
interval and never contains a delete file.

A delete file in the followed snapshot is a named refusal reason under
RFD 007 (Serving tiers). The refusal is of the snapshot: the last good
generation keeps serving, and the table advances again once the ref moves to
a snapshot without delete files. At boot, with no generation to keep, RFD
005's start-or-fail rule applies.

The later cost tier would apply deletion vectors and equality deletes newest
file first, priced by uncompacted deltas per bucket, a unit RFD 007
(Serving tiers) would have to add to its penalty vocabulary.

## Alternatives Considered

Native merge-on-read in the first version. Rejected because the per-lookup
cost of applying deletes is unpriced and the prototype never exercised a
snapshot with a delete file ([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)).
A cost tier needs a measured penalty before it can be one.

Serve only full rebuilds. Rejected because appends and copy-on-write upserts
produce data files that are the truth, and refresh already pays only for the
files that changed ([prototype findings, finding 3](../analysis/2026-09-10-prototype-findings.md#3-load-time)
and [finding 5](../analysis/2026-09-10-prototype-findings.md#5-snapshot-advance-on-s3)).

Serve merge-on-read snapshots by ignoring their delete files. Rejected
because it returns deleted rows as live, and wrong answers are what the
refusal tier exists to prevent.

## Consequences

Table owners who need freshness finer than the compaction interval must
compact more often or accept the lag; the first version offers no third
option.

A delete file on the followed ref stops the table advancing until compaction
catches up, and that condition has to be visible to whoever runs the table.

The prototype's delete-file refusal was unreachable: delete manifests were
filtered out before the check ran, and no fixture wrote a delete file
([prototype findings, finding 15](../analysis/2026-09-10-prototype-findings.md#15-what-reviews-found-that-tests-did-not)).
Under RFD 014 (Test layering) every refusal gets a fixture that triggers it,
so this one is pinned by a fixture whose snapshot carries a delete file.

Open questions:

- How a table stuck behind a delete file is made visible. RFD 005 leaves
  per-table lifecycle states with a health surface as an open question, and
  a stuck table is a natural state in that model; short of that, the named
  refusal reason in logs and metrics plus the age of the served snapshot may
  be all an operator needs.
- Whether the recommended serving ref is a tag or a branch. A tag is the
  simpler pointer and cannot receive writes by accident; a branch carries its
  own snapshot retention, which is one answer to the expiration hazard RFD
  005 describes.
