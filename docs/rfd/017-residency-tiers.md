# Residency tiers

> Updated 2026-09-26: on Linux a disk-tier page read that the operating
> system's page cache serves without blocking runs on the worker; a read
> that would block still runs on the blocking pool.

> Updated 2026-09-29: a chunk file stores what verifying its keys
> established and a checksum of its payload, and a restart trusts a file
> whose checksum and verification context match without decoding a key;
> chunk files are no longer synced when written.

## Problem

The resident index of RFD 004 (Resident key-chunk index) costs about a
byte to well over a dozen per key, by how well sorted keys compress
([key bytes by shape, results](../analysis/2026-09-10-key-bytes-by-shape.md#results));
a billion random-shaped keys is tens of gigabytes. Tables past a replica's
memory need somewhere else for their key chunks, and the bloom filter
alternative, whose memory depends on the writer's filter size and the key
shape, was worse than the chunks on three of four tables tried
([bloom residency tier, observations](../analysis/2026-09-10-bloom-residency-tier.md#observations)),
so the owner chooses.

## Decision

Where a table keeps its key chunks will be an explicit per-table
`residency` setting with three values ranked by lookup speed (`full` in
memory, `disk` in a local file with fences in memory, `bloom` as bloom
filters in memory), chosen by the table owner and never by the loader, with
an optional resident-bytes budget that refuses an over-budget load at the
footer stage, and the same local directory will hold a size-bounded disk
level of the page cache, keyed by file and offset and never evicted for
staleness.

The tiers:

- `full`, the default, is RFD 004 as stated: one GET per hit, none per
  miss, a few bytes of memory per key.
- `disk` streams the chunks to a local file during verification and keeps
  the routing set in memory. A lookup reads one key page from local disk; a
  hit costs one GET, a miss none; memory per key is the fence array, a
  fraction of a byte.
- `bloom` keeps the routing set and the key column's bloom filters in
  memory, with no local disk. A hit costs two GETs (key page, then value
  page); a miss costs a GET at the filter's false-positive rate, set by the
  writer's filter size
  ([bloom residency tier, results](../analysis/2026-09-10-bloom-residency-tier.md#results)).

The routing set, kept by every tier: the footer, the key column's page
indexes, the value column's offset index, each file's exact key range and
row-group bounds, and the fence array.

Every tier streams the chunks once at load for RFD 007's (Serving tiers)
refusals, two of which are this RFD's: a `full` load over its
resident-bytes budget, and `bloom` on a table whose files carry no key
bloom filter. `disk` without a local directory is a configuration
violation, refused by the parser and by the loader for a configuration
built by hand.

After the footers and before any key chunk, the loader knows every tier's
resident bytes and the doctor reports them (RFD 016 (Doctor)); the budget
refuses a `full` load there, with the bytes needed and the budget.

The disk tier's chunk files:

- A chunk file is named by the data file it holds and carries its identity
  (path, size, footer digest), what verifying the keys established (the
  fences, each row group's first and last key, the longest key), the key
  type, the verifier's rule version, and a crc32c of everything after its
  header.
- A restart reuses every chunk file whose identity, verification context,
  and checksum match, with one sequential local read per file and no key
  decoded.
- A file that fails any of these, holds other chunks than the footer names,
  or whose stored result does not describe the footer's pages is stale and
  is rebuilt as a missing file is, never failing a load.
- A generation that drops a data file deletes its chunk file.
- Pages are read with explicit positioned reads, counted beside GETs (RFD
  003 (One storage stack)). The operating system's page cache is not the
  memory knob.
- On Linux a read is first tried on the asking worker with `RWF_NOWAIT` and
  falls to the blocking pool when the page cache cannot return all of it;
  elsewhere every read goes to the blocking pool.

The local directory is optional and disposable; everything in it is
rebuilt from object storage, and its path is RFD 012's (Deployment
contract). Beside the chunk files it holds the page cache's disk level,
value pages evicted by size only, which survives a restart.

## Alternatives Considered

An automatic fallback chosen by the loader from a memory budget. Rejected:
the bloom tier raised memory on two of four measured tables and miss cost
on a third, the writer's filter size being a constant per row group
([bloom residency tier, what the evidence implies](../analysis/2026-09-10-bloom-residency-tier.md#what-the-evidence-implies)).

Memory-mapping the chunk files. Rejected: a page fault stalls the async
worker, the reads are invisible to the request accounting, and the memory
is unattributable.

Bloom filters only, with no disk tier. Rejected: the disk tier beats it on
hit cost, miss cost, and memory, losing only on the disk dependency; bloom
stays for deployments that cannot attach a disk.

Re-verifying a reused chunk file's keys on every restart, guarded against
a torn file by a synced write-then-rename. Rejected: the restart cost 85 to
90% of a first load's CPU on a four-core Linux pod, minutes for a billion
rows; a checksum turns a torn file into a stale one.

An index built once into a separate store beside the data. Rejected: a
second system and artifact to keep consistent with every snapshot. The
chunk files are a local copy of the data files' bytes, rebuilt from them.

## Consequences

A disk-tier replica needs local solid-state storage or a local volume.

A worker-side disk read needs a filesystem that serves `RWF_NOWAIT`: ext4
does, overlayfs (a container's writable layer) does not, both checked on
Linux 6.8, and kernels before 4.14 lack the flag. Without it every read
takes the blocking pool's handoff, most of the disk tier's cost over the
full tier
([performance sweep, the disk tier on Linux](../analysis/2026-09-26-performance-sweep.md#the-disk-tier-on-linux)),
so the local directory belongs on a mounted volume. Under `relatime` a read
can still wait on the access-time update, on the first read after a write
and at most once a day after; `noatime` makes none.

The disk tier holds one open descriptor per data file per generation, two
per file while a refresh that rewrote the table loads beside the serving
one. Set the descriptor limit to at least twice the disk-tier file count
plus 1,024 for sockets, the page cache's disk level, and the one chunk file
retention opens at a time.

The disk tier's load holds one overlap group's chunks at a time, read back
from the chunk files for verification; the bloom tier verifies a group in
memory, so its load peaks at the full tier's size once per load
([bloom residency tier, observations](../analysis/2026-09-10-bloom-residency-tier.md#observations)).

The bloom tier's miss cost follows the writer's filter sizing, which one
Iceberg release could not control; the doctor reports the filter's size
per key (RFD 016 (Doctor)).

The chunk files are the persisted index RFD 004 (Resident key-chunk index)
rejected for the in-memory tier
([prototype findings, finding 3](../analysis/2026-09-10-prototype-findings.md#3-load-time)).
A restart of a disk-tier table fetches footers only and decodes keys only
in overlap groups, which a first load merges to prove key uniqueness (RFD
005 (Generations and refresh)). The header follows the persisted-state
envelope of RFD 011 (Configuration); a file of another format version is
stale, and a change to the rules verification enforces raises the
verifier's version, which makes earlier results stale.

A chunk file is written under a temporary name and renamed into place;
neither file nor directory is synced. A crash can leave a file short or
holding unwritten blocks; the length check or the checksum rejects it and
the next load rebuilds it, and a lost rename costs one fetch of the file's
chunks. The checksum covers accidental damage, not a file changed on
purpose.

Two extensions are permitted and not built in the first version: a
fences-only tier with no local disk (page indexes and fences, neither
chunks nor filters) at two GETs per hit and one per miss, superseded by
the disk tier wherever a local disk exists; and sharding by bucket, where a
node loads only its assigned buckets' files, at one GET per hit and none
per miss with memory scaling with nodes. The bucket transform is the shard
key, so a shard is a filter on the manifest walk and moving a bucket is a
reload; it needs a bucket-to-node map, a router, and a rule for replicas of
one shard that poll independently.

The index's live bytes are exported beside process memory (RFD 012
(Deployment contract)): on one platform the allocator kept the shed
chunks' pages
([bloom residency tier, observations](../analysis/2026-09-10-bloom-residency-tier.md#observations)).

The `full` budget is per table: a replica-wide bound would refuse whichever
table loaded last, so two replicas with one configuration could refuse
different tables; an operator divides the process's memory among the
tables. The disk tier's `index_budget_bytes` is replica-wide, bounding one
shared directory whose files outlive the process. A replica-wide `full`
budget can be added later as one more server field and one more sum in the
same check.

The chunk-file budget is checked against the `index/` directory as it is
when the load begins, plus what the new generation would write:

- A file's write size is known from its footer, except its stored
  verification result (a key per key page): exact for keys of one width
  (longs, uuids, fixed-width binary), estimated for string and binary keys,
  and a small fraction of the chunks.
- Each load reserves its write until it finishes, and the check counts
  every other load's reservation; two loads that fit alone but not together
  cannot both pass, and the first to the check wins.
- A reservation still held after its chunk files land counts them twice,
  erring toward refusal.
- The serving generation's chunk files count, since they are deleted after
  the swap, so a table rewritten wholesale is sized at about twice its
  steady footprint.
- A generation the strict gate refuses keeps its written chunks until a
  later swap deletes what no generation references.

Chunk files no serving generation references are deleted with every
table's load lock held, taken one at a time, and deletions run one at a
time. The tables a reload is loading count as live until it swaps them in
or drops them (RFD 018 (Configuration reload)). A slow table holds
the deletion up, and with it the next refresh of every table whose lock
the deletion holds, for at most its deadline under RFD 013 (Tail latency),
five minutes by default and thirty for a first load; a table early in the
order can wait one deadline per slow table after it. Computing the live
set without the locks could miss a load in flight.
