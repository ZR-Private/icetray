# Performance and tuning

A lookup in icetray costs three kinds of work, and they differ by orders
of magnitude. A GET to object storage takes tens of milliseconds, and
longer for a file nobody has read for hours. A read from the local
directory is a memory copy when the operating system's page cache holds
the file, and a device read when it does not. The CPU work of a lookup,
which decodes one key page and decompresses one value page per column it
reads, takes microseconds to tens of microseconds. Most tuning decides
how many lookups pay for a GET, and how much memory or local disk serves
the others.

Three outcomes recur below. A **cold hit** finds its key and fetches a
value page from object storage. A **warm hit** finds every value page it
needs in the page cache. A **miss** finds that the key is absent. Terms
such as [residency](glossary.md), [key chunk](glossary.md), and [fence
array](glossary.md) are in the glossary.

## What a lookup costs

| Residency | Cold hit | Warm hit | Miss |
|---|---|---|---|
| `full` | 1 GET | nothing outside memory | nothing outside memory |
| `disk` | 1 local read and 1 GET | 1 local read | 1 local read |
| `bloom` | 2 GETs: the key page, then the value page | nothing outside memory | nothing, or 1 GET when the filter passes an absent key |

The table is for a lookup that names one value column. Each further
column it names adds one value-page GET to a cold hit, two for a
dictionary-encoded column, and none for a column the row's file answers
by its default. A lookup that names no column asks only whether its key
is present, and costs only finding the row: nothing outside memory under
`full`, a local read under `disk`, and the key page's GET under `bloom`.

A cold hit spends nearly all of its time waiting on its GET, so its latency
is the object store's. A warm hit and a miss under `full` never leave
memory and take microseconds. Under `disk` they add a local read of one
key page, a memory copy while the chunk file is in the kernel's page cache
and a device read when it is not. `icetray get` prints each lookup's
`gets`, so the GET count of a residency can be checked on a real table
before choosing it.

A `MultiGet` holds at most 32 GETs in flight across the batch, each GET
a lookup issues taking one slot while it runs, so a batch whose keys
are all cold waits on the order of one GET's latency for every 32 GETs:
every 32 keys when it names one column, every 16 when it names two, and
fewer when a column is dictionary-encoded, since its dictionary page is
a GET of its own.

## Packing or splitting a row

A table may serve several [value columns](glossary.md), and a lookup
reads only the columns it names ([RFD 022](rfd/022-value-columns.md)).
The GETs of a cold hit naming several columns are issued concurrently,
each hedged on its own, so the hit waits on the slowest of them, and its
tail is wider than a one-column hit's even with every GET hedged.
Columns share the page cache, which is keyed by file and offset.

- One packed column performs best: a row packed into one `binary` column,
  such as a serialized protobuf, costs one GET per cold hit. It is the
  right layout for a table read by one kind of caller.
- A table read by several kinds of caller splits its row along how it is
  read. Fields read together share a column. A large part read on its
  own, such as an embedding, gets a column of its own, so a caller that
  wants only the small fields neither fetches nor decodes it, and the
  page cache holds more of the small fields' hot rows.
- A caller that needs most of a row is better served by one packed
  column than by naming several, which costs a GET per column.

`icetray_value_page_gets_total` counts each column's GETs, and
`icetray_table_column_gets_per_hit` gives each column's cold cost, so a
caller can add up what a request will spend
([Metrics](operating.md#metrics)).

## Choosing a residency

A table's residency decides where its key chunks live, and so trades
memory against GETs against local disk. Every residency gives the same
answers. The full comparison is in
[Choosing a residency](configuration.md#choosing-a-residency), and the
decision behind the three tiers is
[RFD 017](rfd/017-residency-tiers.md).

- **`full`** keeps every key chunk in memory. Its cost is the key column's
  compressed size, which depends on how well the sorted keys compress:
  about a byte per key for sequential integers, and over a dozen for random
  ones such as UUIDs, so a billion random keys take tens of gigabytes. It
  pays the fewest GETs and is the fastest on a hit, and it is the right
  choice whenever its bytes fit beside the page cache.
- **`disk`** keeps the key chunks in a chunk file in the local directory
  and holds only footers, page indexes, and fences in memory: a fraction of
  a byte per key at the contract's page sizes. The chunk files take about
  what `full` would hold in memory. A hit still costs one GET and a miss
  none, so `disk` serves tables too large for memory at the same GET count,
  as long as the replica has a local volume and memory left for the
  kernel to cache the chunk files ([below](#the-memory-limit-and-the-kernels-page-cache)).
- **`bloom`** replaces the key chunks with the key column's bloom filters
  and needs no local disk. Its memory is the filters, sized in bits per key
  by the writer, plus the same fraction of a byte. It pays a second GET on
  every cold hit, for the key page, and a GET on each miss the filter fails
  to reject. Writers often size the filter per row group rather than per
  key, so `bloom` can take more memory than `full`. It suits tables whose
  keys do not fit in memory on a replica that cannot mount a volume.

Memory also has a part per data file: each file's footer and page indexes
stay resident under every residency. At the contract's file sizes that
part is small beside the keys. Many small files raise it, and so do values
of kilobytes, where each value page holds only a few values and each
value column's offset index carries an entry per page. Only the offset
indexes of the key and of the configured value columns stay resident;
every other column's are dropped at load, so a column the table does not
serve costs no memory.

## Sizing from the doctor's predicted bytes

`icetray info` and `icetray doctor` print each residency's resident bytes
under `predicted`, computed from the data files' footers before any key
chunk is read. For the golden table `long_2m`, 2 million long keys:

```json
"records":2000000,"index_bytes":2623999,"predicted":{"full":2623999,"disk_memory":239082,"disk_bytes":2405869,"bloom":4433458,"bloom_bits_per_key":16.777504,"key_chunk_bytes":2384917,"filter_bytes":4194376}
```

- `predicted.full`, `predicted.disk_memory`, and `predicted.bloom` are the
  memory each residency would hold. Divided by `records` they give bytes
  per key, which carries over, roughly, to a larger table of the same key
  shape and layout.
- `predicted.disk_bytes` is what the chunk files of `disk` would occupy on
  local disk, as `storage.local.index_budget_bytes` counts them.
- `predicted.key_chunk_bytes` and `predicted.filter_bytes` are the key
  column's compressed bytes and its bloom filters' bytes, the parts that
  `full` and `bloom` add to the routing set every residency keeps.
- `predicted.bloom_bits_per_key` is the filter's size per key. Under
  `bloom`, a filter below about 8 bits per key lets a noticeable share of
  misses through to a GET, and one above 4 bytes per key spends memory for
  little gain; the doctor reports either as advice.

Here `full` holds about 1.3 bytes per key, `disk` about a tenth of a byte
in memory and about 1.2 bytes on disk, and `bloom` more than `full`,
because the writer gave each file a 1 MiB filter whatever its key count.

[Memory](operating.md#memory) turns these figures into a container memory
limit: the predicted bytes for each table's residency, doubled for a table
a refresh can rewrite wholesale, plus the page cache, plus a margin for the
process.

## The page cache

Value pages are cached compressed, keyed by file and offset. A data file
never changes, so a cached page never goes stale and is evicted by size
alone. Concurrent lookups that need the same uncached page share one GET.

- **The memory level**, `server.page_cache_bytes` (256 MiB by default),
  holds the hottest pages. A hit there costs no I/O. The level is split
  into shards, one per worker thread rounded up to a power of two, and
  each shard evicts within its own share of the budget.
- **The disk level**, `storage.local.page_cache_disk_bytes`, takes the
  pages the memory level evicts, in 4 MiB blocks in the local directory. A
  hit there costs one local read and no GET, and the level survives a
  restart, so a replica that comes back keeps its warm pages. Each block
  holds one open file descriptor
  ([File descriptors](operating.md#file-descriptors)).

A cold hit costs a GET of tens of milliseconds, and a warm hit from memory
costs microseconds, so the cache's hit rate sets most of a table's average
lookup latency. Each hit caches a whole value page, so the page
size decides how much of the budget one hot key takes; the table contract
bounds value pages by bytes for large values for that reason
([Page and file sizes](#page-and-file-sizes)).

`icetray_lookups_total` by `result` gives the split of cold hits, warm
hits, and misses, and `icetray_page_cache_hits_total` against
`icetray_page_cache_fetches_total` gives the hit rate at the budget a
replica runs ([Metrics](operating.md#metrics)). A hit rate that climbs
with a larger budget on one replica is the case for raising it on all of
them.

## Hedging slow GETs

A lookup GET still unanswered after a delay is sent a second time, and the
first answer wins. The delay is a high percentile of the table's recent
lookup GETs, by default the 95th of the last 256, so it follows the object
store as its latency drifts. A ceiling, by default 5 percent, bounds the
share of hedged GETs. Load reads are never hedged.

- A lower percentile hedges sooner and cuts more of the tail, at the price
  of more GETs. A higher one wastes fewer GETs and lets a stall run longer.
- A table hedges only once it has made a full window of lookup GETs, so a
  rarely read table may never hedge; a smaller `window` lets it start
  sooner, with a noisier delay.
- `icetray_hedges_total` against `icetray_object_gets_total{kind="lookup"}`
  shows what hedging spends.

The settings are under [`[storage.hedge]`](configuration.md#storagehedge),
and the decision is [RFD 013](rfd/013-tail-latency.md).

## Load time

A first load reads every data file's footer and key chunks, two GETs for a
contract-shaped file, and verifies every key. Past enough reads in flight,
its time goes to CPU: decoding and verifying key chunks, and for
`disk` tables writing chunk files. icetray runs one worker thread per core
the process sees, or `TOKIO_WORKER_THREADS`, and each load keeps up to 128
reads in flight per worker, at most 1,024. Load time therefore scales with
the cores the process is given, and a tight CPU limit stretches a load
toward `server.load_deadline` ([CPU](operating.md#cpu)).

- A refresh reuses the index of every data file the previous generation
  held, so it costs what changed rather than the whole table.
- A restarted replica with `disk` tables reuses its chunk files, checked by
  checksum, and reads footers only for those files.
- The doctor decodes and hashes every key to check its bucket, which a load
  does not, so it takes longer than `info` on the same table.

## Deletes

A merge-on-read table's deleted rows are held in memory and checked before
a lookup reads a value page ([RFD 021](rfd/021-merge-on-read.md)). The
check costs nanoseconds, a few thousandths of the key-page decode beside
it, so a lookup's cost under deletes is the files it probes, one key-page
decode each ([analysis](analysis/2026-10-07-merge-on-read-costs.md)):

- A hit on an updated key probes one file however many commits updated
  it, because the newest copy is probed first.
- A miss on a deleted key probes every file that holds a deleted copy,
  under `bloom` too, since a deleted row stays in its file's filter, and
  there each probe costs a key-page GET when the page is not cached.
  Commits that update add such files until a compaction rewrites the
  bucket; `icetray_table_files_probed_per_lookup` shows how many.
- The delete state takes about two bytes a deleted row when deletes are
  scattered through a block, and a small fraction of a byte when they fall
  in runs, as a delete of a key range leaves them. A `full` table's budget
  counts an upper bound, which is exact for scattered deletes and
  generous for runs and for position delete files that name no data file.
  `icetray_table_delete_state_bytes` shows what it holds.
- Each delete file adds one read to a load, two for a deletion vector (its
  Puffin footer and its blob). A refresh reads only the deletes that are
  new.

## File descriptors

A replica's descriptor needs grow with three settings: the disk level's
size, one descriptor per 4 MiB block; the number of tables, since every
table loads at once at start with up to 1,024 reads each; and the data
files of `disk` tables, one chunk file each, twice that during a refresh
that rewrites them. icetray raises its soft limit to the hard limit at
start and warns when that is short. The rule and the messages are in
[File descriptors](operating.md#file-descriptors).

## Page and file sizes

The writer fixes several costs when it writes the table, and the
[table contract](table-contract.md) sets each one where a lookup is
cheapest:

- **Key pages of 1,000 rows.** A lookup decodes one key page, so the
  page's row count is the keys a lookup decodes. Pages above 2,000 rows
  draw `large_key_pages`, which caps the table's level at `balanced`.
- **Value pages bounded by bytes.** A hit fetches and decompresses one
  whole value page for each column it names and caches it. For values of
  kilobytes the contract bounds pages at 64 KiB; a page bounded by rows
  alone grows to megabytes, which every hit transfers and the doctor
  advises against above 1 MiB.
- **Data files of 32 to 64 MiB.** Each file adds a footer read at load,
  resident index overhead, and under `disk` a descriptor. A range GET's
  first-byte time also varies with the size of the file it reads.
- **The bucket count.** `bucket(N, key)` lets a lookup probe one file. `N`
  near the table's compressed size divided by 64 MiB gives one
  target-sized file per bucket;
  [Choosing the bucket count](table-contract.md#choosing-the-bucket-count)
  covers the trade-offs.
- **The key bloom filter.** Its size per key sets `bloom` residency's
  memory and its misses' GETs, and it matters for no other residency.

## The memory limit and the kernel's page cache

A `disk` table's local reads are memory copies only while the kernel's
page cache holds its chunk files. On Linux that cache counts against a
container's memory limit, so a limit sized for the process alone leaves
the chunk files nowhere to stay cached, and every lookup waits on the
device. Raise the limit by the `disk` tables' `predicted.disk_bytes`. The
extra is cache the kernel can reclaim, so a scheduler's reservation, such
as a Kubernetes memory request, can stay at the process's own size
([Memory](operating.md#memory)).

The server checks both ends of this. At start it warns when the chunk
files and the disk level need more than the limit leaves beside the
process. While it serves, it samples how much of each `disk` table's
chunk files the page cache holds, `icetray_table_chunk_resident_fraction`.
Below 0.95 the table reads as `balanced`, below an `optimal` declaration,
and it recovers above 0.99. The thresholds come from a sweep of `disk`
lookups over uniform keys with chunk-file pages evicted at random, the
worst case for a cache
([analysis](analysis/2026-10-07-page-cache-residency.md)). A miss there
took about 100 µs with its key page in the page cache and about 730 µs
with it on the device. Its 90th percentile reached the device once one
page in twenty was missing, and above 0.97 only its 99th percentile was
there. Under skewed access the kernel drops the coldest pages first, so a
real table at the same fraction loses less. The fraction says whether the
chunk files still fit in memory; `icetray_lookup_seconds` shows the
latency itself.

The local directory belongs on a mounted volume with a filesystem such as
ext4. A container's overlay filesystem cannot serve a cached read without
blocking, so every local read there moves to another thread
([The local directory](operating.md#the-local-directory)).

## The decisions behind these trade-offs

- [RFD 004](rfd/004-resident-key-chunk-index.md): what stays resident per
  data file, and why a hit costs one GET per column it reads and a miss
  none.
- [RFD 006](rfd/006-lookup-hot-path.md): the CPU a lookup spends decoding
  its key page and its value pages.
- [RFD 013](rfd/013-tail-latency.md): hedging, deadlines, and the bounds on
  loads.
- [RFD 017](rfd/017-residency-tiers.md): the three residencies, the local
  directory, and the descriptors they hold.
- [RFD 002](rfd/002-serving-layout-contract.md): the layout the table
  contract asks writers for.
- [RFD 022](rfd/022-value-columns.md): value columns, and when to split a
  row across them.
