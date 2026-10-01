# Generations and refresh

## Problem

Batch jobs rebuild the tables icetray serves on a schedule and publish each
build by advancing an Iceberg ref. As a table grows, it pays to have a
refresh that costs only what changed
([workload archetypes, archetype A](../analysis/2026-09-09-workload-archetypes.md#archetype-a-batch-computed-per-user-vectors)).
A server that keeps a resident index per data file (RFD 004) has to notice
the new snapshot, build the state that serves it, and switch over without
lookups seeing a half-built table or paying for the load. It also has to
cope with a snapshot that cannot be served, and with data files that
overlap within a bucket, which a batch writer produces at its default
settings
([prototype findings, finding 9](../analysis/2026-09-10-prototype-findings.md#9-spark-writes-several-sorted-files-per-bucket-sometimes-overlapping)).
A key duplicated across those files would make an answer depend on probe
order.

## Decision

A table will follow one Iceberg ref, build each new snapshot as an immutable
generation beside the current one while reusing the resident index of every
data file whose path is unchanged, verify key uniqueness across overlapping
files only for groups that gained a file, and swap atomically; a poll that
finds the ref unchanged costs one read.

The source is either a static table root, followed through its version
hint file, or a catalog. A poll compares the metadata location it knows
against the current one and stops when they match
([prototype findings, finding 5](../analysis/2026-09-10-prototype-findings.md#5-snapshot-advance-on-s3)).

A generation takes its key column, value column, and bucket transform from
the snapshot's own schema and from each manifest's partition spec, so a
schema or spec change between snapshots cannot misroute a key. The
table's current schema and default spec play no part
([prototype findings, finding 15](../analysis/2026-09-10-prototype-findings.md#15-what-reviews-found-that-tests-did-not)).

A data file that the new snapshot still references by the same path, at
the same size, keeps its resident index from the previous generation, and
only new files are loaded. Because load work is per file and unchanged
files are reused, a refresh costs its delta
([prototype findings, finding 3](../analysis/2026-09-10-prototype-findings.md#3-load-time)).
In simulation a build of 16 files takes 6 dependent round trips; how that
depth grows with more files is open
([prototype findings, finding 12](../analysis/2026-09-10-prototype-findings.md#12-deterministic-simulation)).

Data files in one bucket whose exact key ranges intersect form an overlap
group. The group's sorted key runs are merged from the resident key column
chunks to find any key present in more than one file. A duplicate key is a
refusal. Overlap without duplicates is a cost finding, priced under RFD 007
(Serving tiers).

When a refresh fails, the last good generation keeps serving and the
failure is reported. A server refuses to start if any configured table
fails to open. A table that goes silently missing does more harm than a
crash at boot, while a stale table still answers, which is better than
having none. A running server applies a change to its configured tables
under the same all-or-nothing rule (RFD 018 (Configuration reload)).
Refreshes of one table are serialized.

## Alternatives Considered

Rebuilding the whole index for every snapshot. Rejected because load is
per file, so a small change would cost as much as the whole table
([prototype findings, finding 3](../analysis/2026-09-10-prototype-findings.md#3-load-time)).

Pinning a snapshot id in configuration. Rejected because batch jobs
publish by moving a ref, so a pinned id would need editing for every
build.

Verifying every overlap group on every refresh. Rejected because its cost
tracks the size of the whole table rather than the change
([prototype findings, finding 9](../analysis/2026-09-10-prototype-findings.md#9-spark-writes-several-sorted-files-per-bucket-sometimes-overlapping)).

Starting degraded when a table fails to open. Rejected because a running
server that is missing a table is harder to notice than one that did not
start.

## Consequences

Snapshot expiration is a hazard. If the writer expires a snapshot that a
lagging replica still serves, that snapshot's data files vanish. Retention
on the serving ref must exceed the longest lag a replica can have. icetray
cannot enforce that; it can only tell table owners and expose how far a
replica lags.

The duplicate-key merge is paid once per group that gained a file, so an
appended table pays it only for the buckets that grew.

A delete file on the followed ref stops the table advancing while the last
good generation serves. RFD 009 (Writes between builds) gives the pattern
that avoids it.

Open question: per-table lifecycle states (loading, serving, degraded)
behind a health surface, or start-or-fail as the only model. Start-or-fail
is simple, and when the process is down a broken configuration cannot go
unnoticed. Lifecycle states would let one table's refusal leave the others
serving and would show operators a table stuck behind one, but the server
would then have to define its answer for a table that is not serving.
