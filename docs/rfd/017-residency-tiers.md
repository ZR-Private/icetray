# Residency tiers

## Problem

The resident index of RFD 004 (Resident key-chunk index) costs, per key,
anything from about a byte to well over a dozen, depending on how well
sorted keys compress
([key bytes by shape, results](../analysis/2026-09-10-key-bytes-by-shape.md#results)),
so a replica's memory bounds the tables it can serve, and a billion
random-shaped keys is tens of gigabytes. Tables past that bound need somewhere other than memory to keep
their key chunks, preferably without giving up the one-GET hit and the free
miss. The measured alternatives are not interchangeable: whether a bloom
filter costs less memory than the chunks it replaces depends on the writer's
filter size and the key shape, and it was worse on three of four tables
tried ([bloom residency tier, observations](../analysis/2026-09-10-bloom-residency-tier.md#observations)), so whoever
chooses has to see the numbers, and a silent choice by the loader could
double a table's GET cost and raise its memory at once.

## Decision

Where a table keeps its key chunks will be an explicit per-table
`residency` setting with three values ranked by lookup speed (`full` in
memory, `disk` in a local file with fences in memory, `bloom` as bloom
filters in memory), chosen by the table owner and never by the loader, with
an optional resident-bytes budget that refuses an over-budget load at the
footer stage, and the same local directory will hold a size-bounded disk
level of the page cache, keyed by file and offset and never evicted for
staleness.

The tiers, in words. `full`, the default, is RFD 004 as stated: one GET per
hit, none per miss, memory of a few bytes per key. `disk` writes the
chunks to a file in a local directory as they stream through verification
and keeps the routing set in memory but no key bytes: a lookup reads one key page
from local disk, a hit still costs one GET and a miss none, and memory per
key falls to the fence array, a fraction of a byte. `bloom` keeps the
routing set and the key column's bloom filters and needs no local disk: a hit
costs two GETs (the key page, then the value page) and a miss costs a GET at
the filter's false-positive rate, which the writer's filter size sets
([bloom residency tier, results](../analysis/2026-09-10-bloom-residency-tier.md#results)). The routing set every
tier keeps is the footer, the key column's page indexes and the value
column's offset index, each file's exact key range and row-group bounds, and
the fence array; `bloom` adds the filters, `full` the chunks. Every tier
streams the chunks once at load for the same refusals (RFD 007 (Serving
tiers)); the tier changes what stays, not what is checked. Two of RFD
007's refusals are this RFD's: a `full` load over its resident-bytes budget
and `bloom` on a table whose files carry no key bloom filter. `disk` with no
local directory configured is not a refusal but a configuration violation:
the configuration parser refuses it, and the loader raises the same
violation for a caller that built the configuration by hand.

After reading footers, and before fetching any key chunk, the loader knows
every tier's resident bytes, and the doctor reports them (RFD 016 (Doctor)),
so the owner chooses with the numbers in front of them.
The budget, set per table, turns a `full` load that would exceed
it into a refusal carrying the bytes needed and the budget, before anything
expensive is fetched.

The disk tier's chunk files are named by the data file they hold and carry
its identity (path, size, footer digest). A data file is immutable, so a
replica that restarts reuses every chunk file already on its disk without
fetching the chunks again, and a generation that drops a file deletes its
chunk file. Pages are read from the chunk files with explicit positioned
reads, counted in the accounting of RFD 003 (One storage stack) beside GETs;
the operating system's page cache is not the memory knob.

The local directory is optional and disposable; everything in it is rebuilt
from object storage, so it needs capacity and nothing else, and how a
deployer supplies its path is RFD 012 (Deployment contract). Beside the
chunk files it holds the page cache's disk level: value pages evicted by
size and never for staleness, since a page is keyed by file and offset and a
file never changes. That level survives a restart, so repeated cold reads
become local reads instead of GETs.

## Alternatives Considered

An automatic fallback chosen by the loader from a memory budget. Rejected:
on the measured tables the bloom tier raised memory on two of four and
raised miss cost on a third, because the writer's filter size was a constant
per row group rather than a function of the keys, so a loader picking by
memory alone would have chosen worse on three of the four
([bloom residency tier, what the evidence implies](../analysis/2026-09-10-bloom-residency-tier.md#what-the-evidence-implies));
the owner chooses.

Memory-mapping the chunk files. Rejected: a page fault stalls the async
worker that took it, the reads are invisible to the request accounting, and
the memory is unattributable; explicit reads keep every byte an intended,
counted operation.

Bloom filters only, with no disk tier. Rejected: the disk tier beats the
bloom tier on hit cost, miss cost, and memory, losing only the disk
dependency, and would be the tier most large tables actually run; bloom
stays for deployments that cannot attach a disk.

An index built once into a separate store beside the data, as some
published designs do. Rejected: a second system and a second artifact to
keep consistent with every snapshot; the chunk files here are a local copy
of bytes already in the data files, rebuilt from them, and nobody maintains
them.

## Consequences

A replica wanting the disk tier runs on hardware with local solid-state
storage or a local volume; the state on it is disposable.

Load peaks differ by tier. The disk tier holds one overlap group's chunks
at a time, since a group is verified by reading pages back from the chunk
files and its chunks are dropped before the next group's are read. The
bloom tier has no local copy to read back, so its load still peaks at the
full tier's size while a group is verified in memory, once per load
([bloom residency tier, observations](../analysis/2026-09-10-bloom-residency-tier.md#observations)).

The bloom tier's miss cost is set by the writer's filter sizing, which one
Iceberg release could not control; the doctor reports the filter's size per
key (RFD 016 (Doctor)) and the owner reads it before choosing that tier.

The chunk files are the persisted index RFD 004 (Resident key-chunk index)
rejected for the in-memory tier, where load time never made one necessary
([prototype findings, finding 3](../analysis/2026-09-10-prototype-findings.md#3-load-time));
here they cost nothing extra to keep, and a restart of a disk-tier table
fetches footers only. Their header follows the persisted-state envelope of
RFD 011 (Configuration).

Two extensions beyond a replica's memory and disk are permitted by this
design and not built in the first version. A fences-only tier with no local
disk, keeping page indexes and fences but neither chunks nor filters, would
cost two GETs per hit and one per miss; the disk tier supersedes it wherever
a local disk exists. Sharding by bucket, where a node loads only the files of
the buckets assigned to it, keeps one GET per hit and no GET per miss and
scales memory with nodes; the bucket transform is already the shard key, so
a shard is a filter on the manifest walk and moving a bucket is a reload,
not a data migration. What it needs is a bucket-to-node map, a router, and a
rule for replicas of one shard that poll independently.

The index's live bytes are exported beside process memory (RFD 012
(Deployment contract)), because a tier that sheds bytes is not visible in
process memory on every platform: on one, the allocator kept the shed
chunks' pages
([bloom residency tier, observations](../analysis/2026-09-10-bloom-residency-tier.md#observations)).

The `full` budget is per table and not per replica. A replica-wide bound
would refuse whichever table happened to load last, so what a replica
serves after a restart would depend on load order and two replicas with one
configuration could refuse different tables; per-table budgets refuse the
same table every time, and an operator bounds the process by dividing its
memory among them. The disk tier's `index_budget_bytes` is replica-wide
because it bounds one shared directory whose files outlive the process,
which no per-table bound can keep from filling. A replica-wide `full`
budget can be added later as one more server field and one more sum in the
same check.

The chunk-file budget is a bound on disk, so the loader checks it against
the `index/` directory as it is when the load begins, plus what the new
generation would write. Several disk tables load at once, so each load
reserves what it would write from the check until its writes finish, and
the check counts every other load's reservation too: two loads that fit
alone but not together cannot both pass, and which one is refused depends
on which reaches the check first. A reservation still held after its chunk
files land counts them twice, which errs toward refusal: it can refuse a
load that would have fit. The serving generation's chunk files are among what
is there: they answer lookups until the swap and are deleted only after it,
so a refresh that replaces every data file needs room for the old set and
the new one at once, and a table rewritten wholesale is sized at about twice
its steady footprint. Measuring against the directory as it would be after
the swap would admit a write that overfills the volume during the load,
which is the failure the budget exists to prevent. A generation the strict
gate refuses has already written its new files' chunks; they stay until a
later swap deletes what no generation references, because the next poll
rebuilds the same snapshot and reuses them, and deleting them at once would
make every poll fetch and write them again while the refusal lasts.
