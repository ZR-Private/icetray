# Resident key-chunk index

## Problem

A server that reads Iceberg tables in place pays one GET for every byte it
does not hold. The workloads are judged on index bytes per key and bounded
cold-read latency, and lean on a miss that costs nothing
([workload archetypes, the cost shape](../analysis/2026-09-09-workload-archetypes.md#the-cost-shape)).
So a hit may issue one GET, a miss none, and memory must grow with key
count at a rate a replica can afford.

## Decision

Each served data file will keep its footer, the key column's page indexes
and the value column's offset index, its compressed key column chunks, its
exact key range, and its fence array resident in memory, so that a lookup
decodes exactly one key page and fetches at most one value page, and a miss
fetches nothing.

This set is the resident index.

Routing uses each file's exact key range, decoded from the resident chunk at
load, since manifest bounds are truncated by default
([prototype findings, finding 8](../analysis/2026-09-10-prototype-findings.md#8-manifest-bounds-are-truncated-by-default)).
The row group is chosen from each row group's exact first and last key,
also decoded at load, so a file written without statistics cannot produce
a false miss
([prototype findings, finding 15](../analysis/2026-09-10-prototype-findings.md#15-what-reviews-found-that-tests-did-not)).
The fence array picks the key page to decode; the key's position in it
names the value page to fetch on a cold hit. RFD 006 (Lookup hot path)
covers the decode.

Load issues one GET per file for the footer, with a tail prefetch that
carries the page indexes, then one per row group for its key chunk; a
contract-shaped file has one row group, so two GETs a file. Memory per key
is a small multiple of the compressed key bytes
([prototype findings, finding 2](../analysis/2026-09-10-prototype-findings.md#2-resident-index-size)).

The value column's column index is excluded; only its offset index is
kept, since the reader never consults value minimums and maximums and for
large-value tables it would dominate the index
([bloom residency tier, observations](../analysis/2026-09-10-bloom-residency-tier.md#observations)).

Tables whose keys are too costly to hold in memory choose another residency
under RFD 017 (Residency tiers); this RFD is the in-memory tier the others
are measured against.

## Alternatives Considered

Fully decoded key arrays per file. Rejected: several times the memory of the
compressed chunks; it may return as an opt-in under a budget (open question
below).

Page indexes plus bloom filter, no key chunks, for every table. Rejected: a
bloom filter's "maybe" costs a GET on a miss, and on off-contract layouts
the column index cannot say which page holds a key
([prototype findings, finding 1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout)).
It is one of RFD 017's per-table tiers.

A persisted index file. Rejected for this tier until restart time is a
measured problem: it reads the same bytes and saves only decode CPU. RFD
017 (Residency tiers) keeps one as a by-product of its disk tier.

Lazy loading on first lookup. Rejected for the first version: the first
lookup pays the load.

## Consequences

Process memory per replica scales with key count, whatever the value size
([prototype findings, finding 2](../analysis/2026-09-10-prototype-findings.md#2-resident-index-size)).
Load time follows file count
([prototype findings, finding 3](../analysis/2026-09-10-prototype-findings.md#3-load-time)),
which the file-size target in RFD 002 weighs.

A lookup's CPU cost follows the size of the key page it decodes, so RFD 007
(Serving tiers) lists oversized key pages as a cost finding. A miss costs
no round trip
([prototype findings, finding 12](../analysis/2026-09-10-prototype-findings.md#12-deterministic-simulation)).
Resident bytes per key grow with key length
([prototype findings, finding 13](../analysis/2026-09-10-prototype-findings.md#13-key-length)),
pushing long keys toward another residency (RFD 017) and the cap in RFD
008.

Parquet sizes buffers from a page's header before the index can check the
page: the declared uncompressed size, up to 2 GiB, and a dictionary's
declared value count, so a lying file makes every replica allocate what it
claims. Load reads every key page's header ahead of parquet, with its own
Thrift compact reader, and refuses as a file fault:

- a header parquet would read to other numbers;
- a data page declaring more uncompressed bytes than its rows could encode
  to with every key at the cap of RFD 008 (Keys);
- a dictionary of more values than its row group has rows, or of more
  bytes than those values at the cap;
- a compressed size past the page's place in the offset index;
- a v2 data page whose level lengths are negative or add up past its
  uncompressed size; parquet adds them as 32-bit integers, which two
  lengths near the maximum overflow.

Those bounds rest on the footer's row counts; a ceiling independent of the
footer covers a footer claiming billions of rows:

- No key page, data or dictionary, may declare more than 64 MiB
  uncompressed: sixteen times the contract's largest key page (1,000 keys
  at the cap, about 4 MiB) and far above the 1 MiB pages writers aim for.
- No key dictionary may count more entries than 64 MiB holds at the larger
  of the fewest bytes an entry of its type encodes to (8 for a long, the
  4-byte length of a byte array, a fixed-width column's width) and the
  bytes parquet holds a decoded entry in (8 for a long, 32 for a byte array
  or fixed-width handle in parquet 58), so the ceiling binds in memory as
  well as on the wire.

Parquet's fixed-width decoder asserts a width above 0, so the footer check
refuses a fixed-width key column of width 0 as a key type mismatch and a
value column of width 0 as a file fault, before any page is read. On a
refresh a refusal leaves the previous generation serving (RFD 005
(Generations and refresh)). A file whose keys run far past the cap can
meet these refusals before the key-length one.

A lookup reads the headers of the value page it fetched, and of the value
column's dictionary page when there is one, before parquet decodes them,
and refuses as a file fault, under a 64 MiB value page ceiling:

- a header parquet would read to other numbers;
- an uncompressed size above the ceiling;
- a dictionary of more values than its row group has rows, or than the
  ceiling holds at the larger of an entry's encoded and in-memory size;
- a compressed size past the page's place;
- v2 level lengths past the page's size.

The contract caps no value's length, so no bound follows a value page's
rows. The ceiling is sixteen times the 4 MiB message a gRPC client on tonic's
defaults accepts and far above the contract's 64 KiB value pages (RFD 002
(Serving layout contract)); a page of large values bounded by rows alone
can exceed it (a thousand 128 KiB values is 125 MiB), and a lookup into it
is refused though each value would fit a message.

Unchecked by the pre-scan: the value counts inside a page body, and the
lengths a delta-length or delta byte-array page carries. Iceberg's default
writer uses v1 pages, which carry neither for key columns. A memory limit
on the process is the backstop (RFD 012 (Deployment contract)). A page body
holding fewer values than its header counts makes parquet panic for a
byte-array page, in release builds too. The footer load, a bloom filter's
parse, and the decode of a column chunk's pages run under `catch_unwind`,
so such a panic is a file fault, `DecoderPanicked`: on a refresh the
previous generation keeps serving, and at lookup that lookup fails.

Open question: decoded key arrays as an opt-in under a per-table memory
budget, removing the per-lookup decode. Withholding them keeps one code
path and one memory model until a workload shows the decode is what it
waits on.

The performance sweep measured the decode
([performance sweep, decompression](../analysis/2026-09-26-performance-sweep.md#decompression-the-open-question)).
A Full-tier miss decodes one key page and nothing else: 6.5 µs on the
long-key table, 84% of it zstd, against 0.2 µs for a Bloom-tier miss. The
key page costs a warm hit 6.4 µs of 12.8 µs with random values and 6.7 µs
of 71.2 µs with values that compress 4.38 times. Estimated, not measured:
decoded keys would take a miss under a microsecond and save about 6.4 µs
per warm hit and per MultiGet key, half of a random-valued warm hit and a
tenth of a text-valued one. Their cost is each key's uncompressed size: on
the sweep's two million keys, about 15.4 MiB for the long keys and 48 MiB
for the 21-byte string keys, against a Full index of 2.49 and 5.18 MiB, so
6 and 9 times the index; three billion 64-bit keys take about 22 GiB
decoded. `icetray_lookups_total` by result gives a table's share of misses,
`icetray_lookup_seconds` for misses gives the decode's latency, and
`icetray_index_bytes` gives the index that multiple applies to.
