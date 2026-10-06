# Merge-on-read

> Updated 2026-10-06: corrected against the code before it was built. The
> bitmap's memory bound gains a per-block overhead that dominates for
> scattered deletes; a Puffin file's footer is read and costs GETs; icetray
> inherits data sequence numbers itself; the oldest-delete age can read
> high as well as low; a position delete file that names no data file is
> read once to learn it applies to nothing; and a malformed blob is
> transient because a damaged read cannot be told from a damaged file,
> not because of a local copy, which delete reads never use.

## Problem

RFD 009 (Writes between builds) refuses every snapshot that carries a
delete file and recommends a serving ref that a compaction job moves past
the deletes. That keeps lookups at their floor, but it makes every table
whose writers use merge-on-read run a compaction schedule and a ref before
icetray serves it at all, and the served data lags by the compaction
interval. Spark writes deletes this way for `MERGE`, `UPDATE`, and `DELETE`
on any table set to merge-on-read, so the refusal turns away tables whose
only problem is how their writer records a change.

RFD 009 deferred native reads because their per-lookup cost was unknown,
and RFD 007 (Serving tiers) would not admit a cost it could not measure.
RFD 020 (Lookup levels) changes what has to be known. A table declares
the cost it accepts, the level a snapshot reaches follows from its layout,
and an owner who wants lookups at their floor can still have them. What
remains is how deletes are applied so that a lookup stays exact, what
they cost, and which kinds are served.

Iceberg records a merge-on-read change in three ways. A deletion vector
(format v3) is a bitmap of deleted row positions for one data file, stored
as a blob in a Puffin file. A position delete file (v2) lists data file
paths and row positions. An equality delete file lists column values and
deletes every matching row of older data files in its partition, or in
every partition when written under an unpartitioned spec. An update is a
delete of the old row plus an insert of the new one in a new data file.

## Decision

Snapshots carrying deletion vectors or position deletes will be served,
with each data file's deleted rows held in memory and checked before a
lookup reads a value page; equality deletes will stay refused; and a
merge-on-read snapshot's level will follow from RFD 020's rules with no
cap of its own.

### Delete state

Each generation holds, beside its files, the set of deleted row positions
for every data file a delete names, as a compressed bitmap. It is built at
load from the snapshot's live delete entries, read through the storage
stack (RFD 003), and follows Iceberg's rules:

- A deletion vector or position delete applies to the data file it names
  when that file's data sequence number is no greater than the delete's,
  so a delete committed with its data file applies to it.
- A data file has at most one deletion vector. Where one applies, position
  delete files naming that data file are ignored, as the spec requires.
- A delete that applies to no data file the snapshot holds is ignored:
  a position delete or deletion vector naming a file compacted away, or
  an equality delete whose data sequence number is no greater than that
  of every data file in the snapshot, which the manifests decide without
  working out the delete's partition scope. Compaction leaves such
  dangling deletes behind, and they change no answer. A position delete
  file that does not name its data file must be read once to learn that
  it applies to nothing; it is then remembered by its path, like any
  delete already read.
- Liveness and data sequence numbers come from the current snapshot's
  manifests every generation, never from a delete read before. A
  manifest entry may leave its sequence number unset and inherit its
  manifest's, and icetray applies that inheritance itself, since the
  Iceberg library offers it only through a reader of its own outside the
  storage stack. An entry that has no sequence number to inherit is a
  deterministic fault of the snapshot.

A deletion vector is identified by its Puffin file's path, offset, and
length, and a position delete file by its path, so a refresh reuses what
the previous generation read and reads only what is new. A new Puffin
file's footer is read once, at one or two GETs, so that every vector the
manifests name can be checked against it; then only the blobs the
manifests list as live are read, since a Puffin file may still hold
vectors a later commit replaced.

A bitmap keeps its rows in blocks of 65,536. A block costs at most two
bytes per deleted row in it and never more than 8 KiB, plus a fixed
overhead of a few tens of bytes, so dense deletes cost about two bytes a
row and scattered deletes, one to a block, cost tens of bytes a row. For
a data file with D deleted rows in n blocks the bound is the lesser of
2·D and 8 KiB·n, plus the block overhead times the lesser of D and n,
plus a fixed overhead per file. Every term follows from the manifests'
record counts, so the delete state is counted in the table's resident
bytes as that upper bound before any delete file is read, and a `full` table whose deletes would exceed its
resident budget (RFD 017, Residency tiers) is refused as `over_budget`
before the reads, as its keys would be. `disk` and `bloom` have no memory
budget under RFD 017, and their delete state is reported in the doctor's
predicted memory and in the gauges below.

Three conditions are refused, each deterministic for its snapshot and
remembered as RFD 005 (Generations and refresh) describes:

- An equality delete that applies to a data file the snapshot holds, as
  `equality_deletes`.
- A delete icetray cannot read, as `delete_kind_unsupported`: position
  deletes in Avro or ORC, or a Puffin blob of a type other than a deletion
  vector.
- A delete that contradicts the snapshot: a deletion vector whose
  `referenced-data-file` is not the data file its manifest entry names,
  or whose cardinality, as its blob property or as decoded, differs from
  the entry's record count; or a position at or past its data file's
  record count, which the spec does not forbid but icetray refuses, since
  a writer that wrote it has misread the file. These are deterministic
  faults of the snapshot's files, like its other file faults. A blob whose
  length, magic, or checksum is wrong is not judged: icetray cannot tell
  a damaged read from a damaged file, so under RFD 005 it is a transient
  fault, and only a blob that passes those checks can be refused for
  what it says.

### Lookups

A key appears at most once within a data file, before deletes, as today.
Across files, the duplicate check of RFD 005 applies to live rows: an
updated key whose older row is deleted is not a duplicate, and two live
copies are refused as `duplicate_keys`.

A lookup finds the key's row in a candidate file exactly as RFD 006
(Lookup hot path) describes, then checks that file's bitmap before it
reads anything more. A deleted row is skipped and the lookup moves to the
next candidate, and only a live row's value page is fetched.

Candidates are probed in descending data sequence number across every
bucketing, with ties broken by the generation's file order, and the cost
oracle that tests count GETs against probes in the same order. Live
duplicates are refused, so the order cannot change an answer. It puts the
newest copy of an updated key first, so a hit on an updated key finds the
live row on its first probe and only a deleted key's miss reaches the
older copies.

What a lookup pays, by residency:

- `full`: the key page is in memory. A hit costs one GET and a miss none,
  deleted or not.
- `disk`: the key page is read from the local chunk file. A hit costs one
  GET and a miss none.
- `bloom`: a deleted row is still in its file's bloom filter, so a miss on
  a deleted key costs one key-page GET for each file holding a deleted
  copy, where an absent key costs one only at the false-positive rate. A
  delete-only commit adds no file, so that cost per lookup is fixed at one
  GET; what grows with deletes is how many misses pay it, which the
  deleted fraction shows. RFD 020's `balanced` counts that GET, and the
  predicted GETs per miss it exports stay the cost of one lookup in the
  worst case.

### Refresh

A refresh verifies an overlap group when it gains a file, as today, and
also when any member's deleted rows are not a superset of what the
previous generation applied. Deletes accumulate as a table moves forward,
but a ref moved backward or a rolled-back branch can drop a deletion
vector and bring a deleted copy back to life, and the check must see it.
Comparing two bitmaps is cheap. A commit that only deletes reuses every
data file, rebuilds the delete state, and verifies nothing unless deletes
shrank.

A compaction arrives as new data files that replace the old ones; the
deletes that named the old files are ignored from then on.

### Levels

Merge-on-read needs no cap of its own in RFD 020's table:

- A commit that only deletes adds no file to probe, so the table keeps
  its level. Under `full` and `disk` a deleted key's lookup decodes the
  same one key page as before; under `bloom` it pays the one GET above,
  which is within `balanced`.
- A commit that updates adds a data file whose key range overlaps the
  older file's, which is `overlapping_files` and caps the table at
  `permissive`. The probes grow with each such commit until a compaction
  rewrites the bucket, which is the growing cost `permissive` names.
- The bitmap check is uncapped. It is a probe of an in-memory bitmap
  beside a key-page decode that costs far more.

A table that wants `optimal` under merge-on-read updates keeps RFD 009's
serving ref behind compaction, or declares `strict` and is held at the
last compacted snapshot until the next compaction, which is the same
behavior reached through RFD 020 rather than a refusal of its own.

### Measurements

The measurements RFD 020 leaves to this RFD are per-table gauges, each
usable alone as an alert:

Each counts only deletes that apply to a data file the generation holds,
so dangling deletes left by a compaction do not keep an alert firing.

- `icetray_table_deleted_rows{table}`: rows the delete state removes.
- `icetray_table_deleted_fraction{table}`: those rows over the rows of the
  snapshot's data files, so one threshold fits tables of any size.
- `icetray_table_delete_files{table}`: live position delete files and
  deletion vectors.
- `icetray_table_oldest_delete_age_seconds{table}`: the age of the
  snapshot that committed the oldest applying delete, found by its data
  sequence number, which grows while compaction stalls. 0 with no applying
  delete. When that snapshot has expired from the metadata, the oldest
  snapshot still listed is used, which understates the age, and a
  compaction that keeps its starting sequence number maps a delete to an
  older snapshot than the one that wrote it, which overstates it. The
  gauge shows compaction stalling; it is not an exact age.
- `icetray_table_delete_state_bytes{table}`: the delete state's memory,
  also counted in `icetray_index_bytes`.

The files a lookup may probe per bucket, which RFD 020 already exports,
is the uncompacted depth per bucket. The doctor reports the same counts
and gains the advice finding `deletes_pending`, naming the deleted rows
and delete files a compaction would remove and, where updates overlap,
the level it would restore.

Before release, three measurements are made on the testkit's contract
tables, and their results go into `docs/performance.md` and the doctor's
explanations: hit and miss latency at 1, 2, 4, 8, and 16 files per bucket
under each residency, with deleted rows first in the probe order;
delete-state memory at deleted fractions from 0.1% to 50%; and load and
refresh time with up to 1,000 live delete files. If the bitmap check
adds more than a tenth of a key-page decode to a lookup, the uncapped
check is revisited in this RFD. Correctness is
pinned by fixtures (RFD 014) for each delete kind, for a delete committed
with its data file, for a deletion vector beside position deletes, for a
Puffin file holding a replaced vector, for a dangling delete, for a
deleted row in a later row group of a file with several, since positions
count from the file's first row, and for a ref moved backward past a
deletion vector, each checked against an oracle
that applies the spec's rules by brute force over the fixture's rows.

## Alternatives Considered

**Keep refusing delete files**, as RFD 009 decided. Rejected as the only
choice: it serves no merge-on-read table without a compaction job and a
ref. It stays the recommended pattern for a table that needs `optimal`.

**Serve equality deletes on the key column.** Deferred until a table
needs it. Flink's upsert mode, their main writer, writes rows unsorted
within a file, which icetray refuses whatever its deletes, and Flink on a
v3 table can convert its equality deletes to deletion vectors, which are
served. Applying them exactly needs a key set per partition spec and
partition plus a global one for deletes written unpartitioned, and their
memory grows with every delete until a rewrite removes them, unbounded by
the table's rows.

**Union a deletion vector with position deletes** for the same data file.
Rejected because the spec says position deletes are ignored where a
deletion vector applies. A compliant writer folds the position deletes
into the vector, so a union differs only for a writer that broke that
rule, and the spec's reading is the one other readers give.

**Verify only groups that gained a file**, trusting refs to move forward.
Rejected because nothing makes a ref move forward, and a skipped check
would serve two live copies of a key.

**Rewrite the key chunks without deleted rows** at load. Rejected because
key chunks are the writer's Parquet pages, decoded by the column reader
(RFD 006). Rewriting them means icetray's own encoding, and a data file's
index could no longer be reused across generations whose deletes differ.

**Let the newest copy win and ignore delete files.** Rejected because a
pure delete has no newer copy, and returning a deleted row as live is the
wrong answer refusals exist to prevent.

## Consequences

RFD 009's refusal of every delete file is replaced: its recommended
serving ref stays the pattern for `optimal`, and its deferred native reads
are this RFD. The table contract replaces `delete_files` with
`equality_deletes` and `delete_kind_unsupported`, and its rule that every
key appears once becomes two: a key appears at most once within a data
file, and a live key appears in at most one file. RFD 020's
`balanced` counts a `bloom` table's GET for a deleted key.

A generation now owns state that is not per file. RFD 005 reuses a data
file's index across generations by sharing it; the delete state is rebuilt
per generation from reused and new deletes, and a reused file's index is
unchanged by it. Overlap verification gains a second trigger, deletes that
shrank.

The lookup gains a step and a probe order. The cost tests of RFD 006,
which count GETs per lookup, gain cases with deleted rows at every
position in the probe order, so a delete that costs an unplanned GET
fails a test.

Open questions:

- Whether a `disk` table's delete state should move to the local
  directory once it is large, as its key chunks do. At about two bytes
  per deleted row when deletes are dense, and tens of bytes when they are
  scattered, it can exceed the memory `disk` keeps per key, and the
  delete-state measurements decide whether that happens on real tables.
