# Resident key-chunk index

## Problem

A server that reads Iceberg tables in place pays one GET for every byte it
does not already hold, and GETs set a lookup's latency. The workloads
icetray is for are rebuilt on a schedule and read sparsely. They are judged
on index bytes per key and on bounded cold-read latency, and they lean on a
miss that costs nothing
([workload archetypes, the cost shape](../analysis/2026-09-09-workload-archetypes.md#the-cost-shape)).
So a hit may issue one GET and a miss none, and the memory that buys this
must grow with key count at a rate a replica can afford. The decision is
what to keep per data file.

## Decision

Each served data file will keep its footer, the key column's page indexes
and the value column's offset index, its compressed key column chunks, its
exact key range, and its fence array resident in memory, so that a lookup
decodes exactly one key page and fetches at most one value page, and a miss
fetches nothing.

This set is the resident index.

Routing uses each file's exact key range, decoded from the resident chunk
at load. Manifest bounds are never used for routing, because they are
truncated by default
([prototype findings, finding 8](../analysis/2026-09-10-prototype-findings.md#8-manifest-bounds-are-truncated-by-default)).
Within a file, the row group holding a key is chosen from the exact first
and last key of each row group, also decoded at load, and never from
Parquet column-chunk statistics. Choosing from statistics would let a file
written without them produce a false miss
([prototype findings, finding 15](../analysis/2026-09-10-prototype-findings.md#15-what-reviews-found-that-tests-did-not)).
The fence array picks the one key page to decode, and the key's position
in that page names the one value page to fetch on a cold hit. RFD 006
(Lookup hot path) covers the decode.

Load issues one GET per file for the footer, with a tail prefetch that
carries the page indexes, and then one per row group for its key chunk. A
contract-shaped file has one row group, so it costs two GETs. Memory per
key is a small multiple of the compressed key bytes
([prototype findings, finding 2](../analysis/2026-09-10-prototype-findings.md#2-resident-index-size)).

The resident set leaves out the value column's column index and keeps
only its offset index. The reader never consults value minimums and
maximums, and for large-value tables that structure would otherwise
dominate the index
([bloom residency tier, observations](../analysis/2026-09-10-bloom-residency-tier.md#observations)).

Tables whose keys are too costly to hold in memory choose another
residency, on local disk or as bloom filters, under RFD 017 (Residency
tiers). This RFD defines the in-memory tier and the promises the other
tiers are measured against.

## Alternatives Considered

Fully decoded key arrays per file. These remove the decode from every
lookup but take several times the memory of the compressed chunks.
Rejected, though they may return as an opt-in under a budget, which is an
open question below.

Page indexes plus a bloom filter, without key chunks, for every table. A
bloom filter's "maybe" costs a GET on a miss, and on off-contract layouts
the column index cannot say which page holds a key. Resident chunks answer
that exactly on every layout the findings cover
([prototype findings, finding 1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout)).
As a per-table choice rather than the design, it is one of RFD 017's
tiers.

A persisted index file. Loading it reads the same bytes as loading the
footer and chunks, and saves only the decode CPU. Rejected for this tier
until restart time is shown to be a problem. RFD 017 (Residency tiers)
keeps such a file as a by-product of its disk tier.

Lazy loading on first lookup. Rejected for the first version, because the
first lookup would pay for the load.

## Consequences

Process memory per replica scales with key count, whatever the value size.
The projection to very large tables is in
[prototype findings, finding 2](../analysis/2026-09-10-prototype-findings.md#2-resident-index-size).
Load is per file, so load time follows file count
([prototype findings, finding 3](../analysis/2026-09-10-prototype-findings.md#3-load-time)),
and that pressure is one the file-size target in RFD 002 weighs.

A lookup's CPU cost depends on the size of the key page it decodes. That
is why RFD 007 (Serving tiers) lists oversized key pages as a cost
finding: they serve at the same GET count but cost more to decode. A miss
costs no round trip
([prototype findings, finding 12](../analysis/2026-09-10-prototype-findings.md#12-deterministic-simulation)).
Resident bytes per key grow with key length
([prototype findings, finding 13](../analysis/2026-09-10-prototype-findings.md#13-key-length)),
so long keys push a table toward another residency (RFD 017) and toward
the cap in RFD 008.

Parquet sizes its buffers from the numbers in a page's header before the
index has a chance to check the page. It reserves the page's declared
uncompressed size, up to 2 GiB, and sizes a dictionary by its declared
value count. Every replica loads the same snapshot, so a file that lies in
either number would make each replica that loads it allocate what it
claims.

Load therefore reads the header of every key page ahead of parquet, with a
reader of its own for the Thrift compact encoding. It refuses any header
that parquet would read to different numbers. It also refuses, as a file
fault:

- a data page declaring more uncompressed bytes than its rows could encode
  to with every key at the cap of RFD 008 (Keys);
- a dictionary of more values than its row group has rows, or of more
  bytes than those values at the cap;
- a compressed size past the page's place in the offset index;
- a v2 data page whose level lengths are negative or add up past its
  uncompressed size. Parquet adds those lengths as 32-bit integers, which
  two lengths near the maximum overflow.

Those bounds rest on the footer's row counts, so a footer claiming
billions of rows widens them. For key pages, a ceiling that does not rest
on the footer closes that gap. No key page, data or dictionary, may
declare more than 64 MiB uncompressed. That is sixteen times the
contract's largest key page of 1,000 keys at the cap (about 4 MiB) and far
above the 1 MiB pages writers aim for. No key dictionary may count more
entries than 64 MiB holds at the larger of two sizes: the fewest bytes an
entry of its type encodes to (8 for a long, the 4-byte length of a byte
array, a fixed-width column's width) and the bytes parquet holds a decoded
entry in (8 for a long, 32 for a byte array or fixed-width handle in
parquet 58). A key dictionary therefore fits the ceiling in memory as well
as on the wire.

A key stored as a fixed-width column of width 0 holds one distinct value,
so it cannot key a table of more than one row, and parquet's fixed-width
decoder asserts a width above 0. The footer check refuses such a key as a
key type mismatch before any page is read. A value column of width 0
would hit that assertion at lookup time, so the footer check refuses it
too, as a file fault. On a refresh, any of these refusals leaves the
previous generation serving (RFD 005 (Generations and refresh)). A file
whose keys run far past the cap can meet this refusal before the
key-length one.

A lookup applies the same checks to the value side. Before parquet decodes
them, it reads the headers of the value page it fetched, and of the value
column's dictionary page when there is one, from those same bytes. It
refuses as a file fault the same faults under a 64 MiB value page ceiling:
a header parquet would read to different numbers, an uncompressed size
above the ceiling, a dictionary of more values than its row group has rows
or than the ceiling holds at the larger of an entry's encoded and
in-memory size, a compressed size past the page's place, and v2 level
lengths past the page's size. The contract caps no value's length, so a
value page's rows imply no bound. The ceiling is sixteen times the 4 MiB
message a gRPC client on tonic's defaults accepts, and far above the
64 KiB value pages of the contract (RFD 002 (Serving layout contract)). A
page of large values bounded by rows alone, which the doctor advises
against, can exceed it (a thousand values of 128 KiB is 125 MiB), and a
lookup into such a page is refused even though each value would fit in a
message.

The pre-scan stops at the page header. The value counts inside a page
body, the lengths a delta-length or delta byte-array page carries, stay
unchecked. Parquet's decoder reads those, and Iceberg's default writer
uses v1 pages, which do not carry them for key columns. A memory limit on
the process is the backstop for whatever the checks miss (RFD 012
(Deployment contract)).

A page body that holds fewer values than its header counts is invisible to
the header checks, and for a byte-array page it makes parquet panic, in
release builds too. The index runs the footer load, a bloom filter's
parse, and the decode of a column chunk's pages under `catch_unwind`, so
such a panic becomes a file fault, `DecoderPanicked`, like the refusals
above. On a refresh the previous generation keeps serving, and at lookup
only that lookup fails.

One question stays open: decoded key arrays as an opt-in. Under a
per-table memory budget they would remove the per-lookup decode for tables
that can afford the memory. Withholding them keeps one code path and one
memory model until a workload shows that the decode is what it waits on.

In the Full tier a miss decodes one key page and nothing else, and
decompression is most of that decode
([performance sweep, decompression](../analysis/2026-09-26-performance-sweep.md#decompression-the-open-question)).
A Bloom-tier miss, which decodes nothing, costs a small fraction of a
Full-tier one. A warm hit pays the same key page decode, which is about
half of a warm hit whose values do not compress and about a tenth of one
whose values compress well.

By estimate from the sweep, decoded keys would take a miss to under
a microsecond and remove that decode from every warm hit and from every
key of a MultiGet. Their cost
is each key's uncompressed size, which comes to roughly the key column's
compression ratio times the resident index: several times the index on
the tables measured, and more for string keys than for 64-bit ones. At
8 bytes a key, three billion 64-bit keys take about 22 GiB decoded.

The answer depends on the workload. On a production table,
`icetray_lookups_total` by result gives its share of misses, and
`icetray_lookup_seconds` for misses gives the decode's latency there,
since a Full-tier miss is little else. `icetray_index_bytes` gives each
table's index, which is the memory that multiple applies to.
