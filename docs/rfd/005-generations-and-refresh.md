# Generations and refresh

> Updated 2026-10-05: a refresh failure that is a function of the snapshot
> and the table's configuration alone, a refusal or a fault of the
> snapshot's metadata or data files, is remembered against the metadata
> location it was read from, and the table does not build that snapshot
> again until its source names another location or a reload changes the
> table's entry. A failure like that cannot succeed on retry, and retrying
> it on every poll re-read the snapshot's metadata and manifests and
> logged and counted the same failure each time. Transient failures are
> still retried on every poll.

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
In simulation a small build takes well under one dependent round trip per
file; how that depth grows with more files is open
([prototype findings, finding 12](../analysis/2026-09-10-prototype-findings.md#12-deterministic-simulation)).

Data files in one bucket whose exact key ranges intersect form an overlap
group. The group's sorted key runs are merged from the resident key column
chunks to find any key present in more than one file. A duplicate key is a
refusal. Overlap without duplicates is an inefficient finding, measured under
RFD 007 (Serving tiers).

When a refresh fails, the last good generation keeps serving and the
failure is reported. A failure that is a function of the snapshot's
metadata and data files and the table's configuration alone is
deterministic. That covers the strict gate, and every refusal reason of
RFD 007 (Serving tiers) but one, a disk load over the local directory's
index budget, which every disk table shares, so other tables' chunk files
decide it too. It covers a fault of the snapshot's metadata or manifests:
a ref, schema, partition spec, or column the metadata lacks, a bucket
transform of zero buckets, a data file whose bucket value is missing or out
of range, or a snapshot timestamp outside the range a timestamp holds.
It covers a fault of a data file that its footer or bloom filters show.
The table remembers a deterministic failure against the metadata location
it read, reports it once, and stays degraded. While the source names that
location, each poll costs its one read and builds nothing. A new location
is tried, and a reload that changes the table's entry builds a new table
that remembers nothing, so a failure its old configuration caused is
tried under the new one. Every other failure is transient and retried on
the next poll: a store, catalog, cache, or local-directory error, a
timeout, a deadline, or a cancellation; a version hint or catalog record
that names no usable metadata, since a commit or the catalog changes it;
a library error, which does not say whether it met bad bytes or a failed
read; and a fault met decoding a data file's pages, since a page may be
the local directory's copy of it rather than the file. When a failure
cannot tell which it is, it is transient: retrying a deterministic one
costs a poll's reads, while remembering a transient one leaves the table
behind until its source moves. Only a refresh over a serving generation
remembers: at boot there is no generation to keep, and a refusal fails
the start.

A server refuses to start if any configured table fails to open. A table
that goes silently missing does more harm than a crash at boot, while a
stale table still answers, which is better than having none. A running server applies a change to its configured tables
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
that avoids it. The refusal is reported once, not once per poll, so a
table stuck behind it is seen through its degraded state and the age of
its snapshot rather than a rising error count.

Open question: per-table lifecycle states (loading, serving, degraded)
behind a health surface, or start-or-fail as the only model. Start-or-fail
is simple, and when the process is down a broken configuration cannot go
unnoticed. Lifecycle states would let one table's refusal leave the others
serving and would show operators a table stuck behind one, but the server
would then have to define its answer for a table that is not serving.
