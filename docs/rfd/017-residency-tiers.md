# Residency tiers

> Updated 2026-09-26: on Linux a disk-tier page read that the operating
> system's page cache serves without blocking runs on the worker; a read
> that would block still runs on the blocking pool.

> Updated 2026-09-29: a chunk file stores what verifying its keys
> established and a checksum of its payload, and a restart trusts a file
> whose checksum and verification context match without decoding a key;
> chunk files are no longer synced when written.

## Problem

The resident index of RFD 004 (Resident key-chunk index) costs anything
from about a byte per key to well over a dozen, depending on how well the
sorted keys compress
([key bytes by shape, results](../analysis/2026-09-10-key-bytes-by-shape.md#results)).
A replica's memory therefore bounds the tables it can serve, and a billion
random-shaped keys take tens of gigabytes. Tables past that bound need
somewhere other than memory to keep their key chunks, preferably without
giving up the one-GET hit and the free miss.

The measured alternatives are not interchangeable. Whether a bloom filter
costs less memory than the chunks it replaces depends on the writer's
filter size and on the key shape, and it was worse on three of the four
tables tried
([bloom residency tier, observations](../analysis/2026-09-10-bloom-residency-tier.md#observations)).
Whoever chooses has to see the numbers, and a silent choice by the loader
could double a table's GET cost and raise its memory at the same time.

## Decision

Where a table keeps its key chunks will be an explicit per-table
`residency` setting with three values ranked by lookup speed (`full` in
memory, `disk` in a local file with fences in memory, `bloom` as bloom
filters in memory), chosen by the table owner and never by the loader,
with an optional resident-bytes budget that refuses an over-budget load at
the footer stage; the same local directory will hold a size-bounded disk
level of the page cache, keyed by file and offset and never evicted for
staleness.

`full`, the default, is RFD 004 as stated: one GET per hit, none per miss,
and a few bytes of memory per key. `disk` writes the chunks to a file in a
local directory as they stream through verification, and keeps the
routing set in memory without any key bytes. A lookup reads one key page
from local disk, a hit still costs one GET and a miss none, and memory per
key falls to the fence array, a fraction of a byte. `bloom` keeps the
routing set and the key column's bloom filters and needs no local disk. A
hit costs two GETs (the key page, then the value page), and a miss costs a
GET at the filter's false-positive rate, which the writer's filter size
sets
([bloom residency tier, results](../analysis/2026-09-10-bloom-residency-tier.md#results)).

The routing set every tier keeps is the footer, the key column's page
indexes and the value column's offset index, each file's exact key range
and row-group bounds, and the fence array. `bloom` adds the filters to it,
and `full` adds the chunks. Every tier streams the chunks once at load for
the same refusals (RFD 007 (Serving tiers)), so the tier changes what
stays resident and leaves what is checked alone. Two of RFD 007's
refusals belong to this RFD: a `full` load over its resident-bytes budget,
and `bloom` on a table whose files carry no key bloom filter. A `disk`
table with no local directory configured is a configuration violation and
sits outside RFD 007's refusals. The configuration parser refuses it, and
the loader raises the same violation for a caller that built the
configuration by hand.

Once it has read the footers, and before it fetches any key chunk, the
loader knows every tier's resident bytes. The doctor reports them (RFD 016
(Doctor)), so the owner chooses with the numbers in front of them. The
budget, set per table, turns a `full` load that would exceed it into a
refusal that carries the bytes needed and the budget, before anything
expensive is fetched.

The disk tier's chunk files are named by the data file they hold and carry
its identity (path, size, footer digest). Each file also stores what
verifying the data file's keys established (the fences, each row group's
first and last key, the longest key), the key type and the version of the
verifier's rules it was established under, and a crc32c of everything
after its header. A data file is immutable, so a replica that restarts
reuses every chunk file already on its disk whose identity, verification
context, and checksum match. It does so without fetching the chunks again
or decoding a key, and checking the checksum is one sequential local read
of the file. A chunk file that fails any of these checks, holds chunks
other than those the footer names, or stores a result that does not
describe the footer's pages is stale, and it is rebuilt from object
storage exactly as a missing file is. A stale file never fails a load. A
generation that drops a data file deletes its chunk file.

Pages are read from the chunk files with explicit positioned reads, which
are counted beside GETs in the accounting of RFD 003 (One storage stack).
The operating system's page cache is not the memory knob. No async worker
waits for data from the disk: a read the operating system's page cache can
serve without blocking may run on the worker that asks for it, and a read
that would block runs on the blocking pool. On Linux a read is first tried
on the worker with `RWF_NOWAIT`, and it goes to the blocking pool when the
page cache cannot return all of it. On other systems every read goes to
the blocking pool.

The local directory is optional and disposable. Everything in it is
rebuilt from object storage, so it needs capacity and nothing else, and
how a deployer supplies its path is RFD 012 (Deployment contract). Beside
the chunk files it holds the page cache's disk level, whose value pages
are evicted by size and never for staleness: a page is keyed by file and
offset, and a file never changes. That level survives a restart, so
repeated cold reads become local reads instead of GETs.

## Alternatives Considered

An automatic fallback chosen by the loader from a memory budget. Rejected:
on the measured tables the bloom tier raised memory on two of four and
raised miss cost on a third, because the writer's filter size was a
constant per row group rather than a function of the keys. A loader
picking by memory alone would have chosen worse on three of the four
([bloom residency tier, what the evidence implies](../analysis/2026-09-10-bloom-residency-tier.md#what-the-evidence-implies)),
so the owner chooses.

Memory-mapping the chunk files. Rejected: a page fault stalls the async
worker that took it, the reads are invisible to the request accounting,
and the memory cannot be attributed. Explicit reads keep every byte an
intended, counted operation.

Bloom filters only, with no disk tier. Rejected: the disk tier beats the
bloom tier on hit cost, miss cost, and memory, and loses only by needing a
disk, so it would be the tier most large tables actually run. Bloom stays
for deployments that cannot attach a disk.

Re-verifying a reused chunk file's keys on every restart, with a synced
write-then-rename as the only guard against a torn file. Rejected: a
restart over reused chunk files cost 85 to 90% of a first load's CPU on a
four-core Linux pod, minutes for a table of a billion rows, to
re-establish what the first load had already established from the same
immutable bytes. A checksum over the payload turns a torn file into a
stale one, and a torn file is what the re-verification guarded against.

An index built once into a separate store beside the data, as some
published designs do. Rejected: it is a second system and a second
artifact to keep consistent with every snapshot. The chunk files here are
a local copy of bytes already in the data files, rebuilt from them, and
need no one to maintain them.

## Consequences

A replica that wants the disk tier runs on hardware with local solid-state
storage or a local volume, and the state on it is disposable.

A disk-tier read on the worker needs a filesystem that serves `RWF_NOWAIT`
reads. On Linux 6.8, ext4 does and overlayfs, a container's own writable
layer, does not, and kernels before 4.14 do not support the flag. On those
every read takes the blocking pool's handoff, which is most of what the
disk tier costs over the full tier
([performance sweep, the disk tier on Linux](../analysis/2026-09-26-performance-sweep.md#the-disk-tier-on-linux)).
The local directory therefore belongs on a mounted volume rather than in
the container's filesystem. A read the page cache serves can still wait on
the file's access-time update. Under `relatime` that update comes on the
first read after the file is written and at most once a day after that,
and a directory mounted `noatime` makes none.

The disk tier holds one open file descriptor per data file for the life of
its generation, and twice that while a refresh that rewrote the table
loads beside the serving generation. A deployment sizes its descriptor
limit to at least twice its disk-tier file count plus a headroom of 1,024
for sockets, the page cache's disk level, and the one chunk file that
retention opens at a time.

Load peaks differ by tier. The disk tier holds one overlap group's chunks
at a time, since a group is verified by reading pages back from the chunk
files and its chunks are dropped before the next group's are read. The
bloom tier has no local copy to read back, so its load still peaks at the
full tier's size while a group is verified in memory, once per load
([bloom residency tier, observations](../analysis/2026-09-10-bloom-residency-tier.md#observations)).

The bloom tier's miss cost is set by the writer's filter sizing, which one
Iceberg release could not control. The doctor reports the filter's size
per key (RFD 016 (Doctor)), and the owner reads it before choosing that
tier.

The chunk files are the persisted index that RFD 004 (Resident key-chunk
index) rejected for the in-memory tier, where load time never made one
necessary
([prototype findings, finding 3](../analysis/2026-09-10-prototype-findings.md#3-load-time)).
Here they cost nothing extra to keep. A restart of a disk-tier table
fetches footers only and decodes no keys except those of files in overlap
groups, which a first load merges to prove key uniqueness (RFD 005
(Generations and refresh)). A chunk file's header follows the
persisted-state envelope of RFD 011 (Configuration). A file of another
format version is stale, since the directory is disposable and nothing
converts it. A change to the rules verification enforces raises the
verifier's version, so a result established under other rules is never
trusted.

A chunk file is written under a temporary name and renamed into place, and
neither the file nor the directory is synced. A crash can leave a file
short or holding blocks that were never written. Its length check or its
checksum rejects it and the next load rebuilds it, and a lost rename costs
one fetch of the file's chunks. The rename is what keeps a reader from
seeing a partial file under its name. The checksum guards against
accidental damage only, not against a file changed on purpose, since the
directory is the replica's own.

This design permits two extensions beyond a replica's memory and disk,
both outside the first version. A fences-only tier with no local disk,
keeping page indexes and fences but neither chunks nor filters, would cost
two GETs per hit and one per miss; the disk tier supersedes it wherever a
local disk exists. Sharding by bucket, where a node loads only the files
of the buckets assigned to it, keeps one GET per hit and none per miss
and scales memory with nodes. The bucket transform is already the shard
key, so a shard is a filter on the manifest walk, and moving a bucket is a
reload rather than a data migration. Sharding needs a bucket-to-node map,
a router, and a rule for replicas of one shard that poll independently.

The index's live bytes are exported beside process memory (RFD 012
(Deployment contract)), because a tier that sheds bytes does not show the
saving in process memory on every platform. On one, the allocator kept the
shed chunks' pages
([bloom residency tier, observations](../analysis/2026-09-10-bloom-residency-tier.md#observations)).

The `full` budget is per table rather than per replica. A replica-wide
bound would refuse whichever table happened to load last, so what a
replica serves after a restart would depend on load order, and two
replicas with one configuration could refuse different tables. Per-table
budgets refuse the same table every time, and an operator bounds the
process by dividing its memory among them. The disk tier's
`index_budget_bytes` is replica-wide because it bounds one shared
directory whose files outlive the process, and no per-table bound can keep
that directory from filling. A replica-wide `full` budget can be added
later as one more server field and one more sum in the same check.

The chunk-file budget is a bound on disk, so the loader checks it against
the `index/` directory as it is when the load begins, plus what the new
generation would write. A file's footer gives what the file would write,
except for its stored verification result, a key per key page. That part
is exact for keys of one width (longs, uuids, fixed-width binary) and
estimated for string and binary keys the same way the fences' memory is,
and it is a small fraction of the chunks beside it. Several disk tables
load at once, so each load reserves what it would write from the check
until its writes finish, and the check counts every other load's
reservation too. Two loads that fit alone but not together cannot both
pass, and which one is refused depends on which reaches the check first. A
reservation still held after its chunk files land counts them twice. That
errs toward refusal, and it can refuse a load that would have fit.

The serving generation's chunk files count as part of what is there. They
answer lookups until the swap and are deleted only after it, so a refresh
that replaces every data file needs room for the old set and the new one
at once, and a table rewritten wholesale is sized at about twice its
steady footprint. Measuring against the directory as it would be after the
swap would admit a write that overfills the volume during the load, which
is the failure the budget exists to prevent. A generation the strict gate
refuses has already written its new files' chunks. They stay until a
later swap deletes what no generation references, because the next poll
rebuilds the same snapshot and reuses them, and deleting them at once
would make every poll fetch and write them again for as long as the
refusal lasts.

Chunk files that no serving generation references are deleted with every
table's load lock held, taken in configuration order. A load in flight has
written chunk files that its generation will reference once it swaps in,
and a live set computed beside it would delete them. The tables a reload
is loading count as live in the same way until it swaps them in or drops
them (RFD 018 (Configuration reload)), and deletions run one at a time.
The cost is that a slow table holds the deletion up, and with it the next
refresh of every table whose lock the deletion already holds. A refresh
gives its lock up once its deadline under RFD 013 (Tail latency) has
passed, five minutes by default and thirty for a first load, so one slow
table holds the others up at most that long. When several are slow, a
table early in the order can wait one deadline for each slow table after
it, since a table whose lock is not yet taken can start a refresh in the
meantime. The cost is accepted, because a live set computed without the
locks could miss a load in flight.
