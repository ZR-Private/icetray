# Generations and refresh

## Problem

Batch jobs rebuild the tables icetray serves on a schedule and publish each
build by advancing an Iceberg ref; a growing table rewards a refresh that
pays only for what changed
([workload archetypes, archetype A](../analysis/2026-09-09-workload-archetypes.md#archetype-a-batch-computed-per-user-vectors)).
A server that keeps a resident index per data file (RFD 004) has to notice
the new snapshot, build the state that serves it, and switch without lookups
seeing a half-built table or paying for the load. It has to cope with a
snapshot that cannot be served, and with data files overlapping within a
bucket, which a batch writer produced at its default settings
([prototype findings, finding 9](../analysis/2026-09-10-prototype-findings.md#9-spark-writes-several-sorted-files-per-bucket-sometimes-overlapping));
a duplicate key across those files would make an answer depend on probe
order.

## Decision

A table will follow one Iceberg ref, build each new snapshot as an immutable
generation beside the current one while reusing the resident index of every
data file whose path is unchanged, verify key uniqueness across overlapping
files only for groups that gained a file, and swap atomically; a poll that
finds the ref unchanged costs one read.

The source is a static table root, followed through its version hint file,
or a catalog; a poll compares the metadata location it knows against the
current one and stops when they match
([prototype findings, finding 5](../analysis/2026-09-10-prototype-findings.md#5-snapshot-advance-on-s3)).

A generation takes its key column, value column, and bucket transform from
the snapshot's own schema and from each manifest's partition spec, so a
schema or spec change between snapshots cannot misroute a key; the table's
current schema and default spec play no part
([prototype findings, finding 15](../analysis/2026-09-10-prototype-findings.md#15-what-reviews-found-that-tests-did-not)).

A data file the new snapshot still references by the same path keeps its
resident index from the previous generation; only new files are loaded.
Load work is per file and unchanged files are reused, so a refresh costs
its delta
([prototype findings, finding 3](../analysis/2026-09-10-prototype-findings.md#3-load-time));
in simulation, a build's depth in dependent round trips was fixed for the
file count tried
([prototype findings, finding 12](../analysis/2026-09-10-prototype-findings.md#12-deterministic-simulation)).

Data files in one bucket whose exact key ranges intersect form an overlap
group; the group's sorted key runs are merged from the resident key column
chunks to detect a key present in more than one file. A duplicate key is a
refusal; overlap without duplicates is a cost finding, priced under RFD 007
(Serving tiers).

Refresh failure keeps the last good generation serving and reports it. A
server refuses to start if any configured table fails to open: a silently
missing table is worse than a crash at boot, and a stale table is better
than none. Refreshes of one table are serialized.

## Alternatives Considered

Rebuild the whole index for every snapshot. Rejected: load is per file, so a
small change would cost the whole table
([prototype findings, finding 3](../analysis/2026-09-10-prototype-findings.md#3-load-time)).

Pin a snapshot id in configuration. Rejected: batch jobs publish by moving a
ref, so a pinned id would need editing for every build.

Verify every overlap group on every refresh. Rejected by measurement: the
prototype's first merge did this and its cost tracked the whole table
([prototype findings, finding 9](../analysis/2026-09-10-prototype-findings.md#9-spark-writes-several-sorted-files-per-bucket-sometimes-overlapping)).

Start degraded when a table fails to open. Rejected: a running server
missing a table is harder to notice than one that did not start.

## Consequences

Snapshot expiration is a hazard: if the writer expires a snapshot a lagging
replica still serves, its data files vanish. Retention on the serving ref
must exceed the longest lag a replica can have; icetray cannot enforce that,
only tell table owners and expose how far a replica lags.

The duplicate-key merge is paid once per group that gained a file; an
appended table pays it only for the buckets that grew.

A delete file on the followed ref stops the table advancing while the last
good generation serves; RFD 009 (Writes between builds) gives the pattern
that avoids it.

Open question: per-table lifecycle states (loading, serving, degraded)
behind a health surface, or start-or-fail as the only model. Start-or-fail
is simple, and a broken configuration cannot go unnoticed when the process
is down. Lifecycle states let one table's refusal leave the others serving
and show operators a table stuck behind one, but then the server must define
its answer for a table that is not serving.
