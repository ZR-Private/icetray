# Resident key-chunk index

## Problem

A server that reads Iceberg tables in place pays one GET for every byte it
does not already hold, and GETs set a lookup's latency. The workloads this
is for are rebuilt on a schedule and read sparsely; they are judged on index
bytes per key and bounded cold-read latency, and they lean on a miss that
costs nothing
([workload archetypes, the cost shape](../analysis/2026-09-09-workload-archetypes.md#the-cost-shape)).
So a hit may issue one GET, a miss none, and the memory behind that must
grow with key count at a rate a replica can afford. What to keep per data
file is the decision.

## Decision

Each served data file will keep its footer, the key column's page indexes
and the value column's offset index, its compressed key column chunks, its
exact key range, and its fence array resident in memory, so that a lookup
decodes exactly one key page and fetches at most one value page, and a miss
fetches nothing.

This set is the resident index.

Routing uses each file's exact key range, decoded from the resident chunk at
load, never manifest bounds, which are truncated by default
([prototype findings, finding 8](../analysis/2026-09-10-prototype-findings.md#8-manifest-bounds-are-truncated-by-default)).
Within a file, the row group holding a key is chosen from the exact first
and last key of each row group, also decoded at load, never from Parquet
column-chunk statistics,
so a file written without statistics cannot produce a false miss; the
prototype had that defect
([prototype findings, finding 15](../analysis/2026-09-10-prototype-findings.md#15-what-reviews-found-that-tests-did-not)).
The fence array picks the one key page to decode, and the key's position in
it names the one value page to fetch on a cold hit. RFD 006 (Lookup hot
path) covers the decode.

Load issues one GET per file for the footer, with a tail prefetch that
carries the page indexes, then one per row group for its key chunk; a
contract-shaped file has one row group, so two GETs a file. Memory per key
is a small multiple of the compressed key bytes
([prototype findings, finding 2](../analysis/2026-09-10-prototype-findings.md#2-resident-index-size)).

The resident set excludes the value column's column index; only its offset
index is kept, since the reader never consults value minimums and maximums,
and for large-value tables that structure would otherwise dominate the index
([bloom residency tier, observations](../analysis/2026-09-10-bloom-residency-tier.md#observations)).

Tables whose keys are too costly to hold in memory choose another residency,
on local disk or as bloom filters, under RFD 017 (Residency tiers); this RFD
is the in-memory tier and the promises the others are measured against.

## Alternatives Considered

Fully decoded key arrays per file. No decode per lookup, but several times
the memory of the compressed chunks. Rejected; it may return as an opt-in
under a budget, an open question below.

Page indexes plus bloom filter, no key chunks, for every table. A bloom
filter's "maybe" costs a GET on a miss, and on off-contract layouts the
column index cannot say which page holds a key; resident chunks answer that
exactly on every layout tried
([prototype findings, finding 1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout)).
As a per-table choice rather than the design, it is one of RFD 017's tiers.

A persisted index file. Loading it reads the same bytes as loading footer
and chunks; only decode CPU is saved. Rejected for this tier until restart
time is a measured problem; RFD 017 (Residency tiers) keeps one as a
by-product of its disk tier.

Lazy loading on first lookup. Rejected for the first version: the first
lookup pays the load.

## Consequences

Process memory per replica scales with key count, whatever the value size;
the projection to very large tables is in
[prototype findings, finding 2](../analysis/2026-09-10-prototype-findings.md#2-resident-index-size).
Load is per file, so load time follows file count
([prototype findings, finding 3](../analysis/2026-09-10-prototype-findings.md#3-load-time)),
a pressure the file-size target in RFD 002 weighs.

A lookup's CPU cost depends on the size of the key page it decodes, which is
why RFD 007 (Serving tiers) lists oversized key pages as a cost finding:
they serve at the same GET count. A miss costs no round trip; the
deterministic simulation pins that
([prototype findings, finding 12](../analysis/2026-09-10-prototype-findings.md#12-deterministic-simulation)).
Resident bytes per key grow with key length
([prototype findings, finding 13](../analysis/2026-09-10-prototype-findings.md#13-key-length)),
so long keys push a table toward another residency (RFD 017) and the cap in
RFD 008.

Parquet sizes buffers from numbers in a page's header before the index can
check the page: it reserves the page's declared uncompressed size, up to 2
GiB, and sizes a dictionary by its declared value count. Every replica
loads the same snapshot, so a file that lies in either would make each one
that loads it allocate what it claims. Load therefore reads the header of
every key page ahead of parquet, with a reader of its own for the Thrift
compact encoding that refuses any header parquet would read to other
numbers, and refuses as a file fault a data page declaring more
uncompressed bytes than its rows could encode to with every key at the cap
of RFD 008 (Keys), a dictionary of more values than its row group has rows
or of more bytes than those values at the cap, a compressed size past the
page's place in the offset index, and a v2 data page whose level lengths
are negative or add up past its uncompressed size, which parquet adds as
32-bit integers that two lengths near the maximum overflow. Those bounds
rest on the footer's row counts, so a footer claiming billions of rows
widens them; a ceiling that does not rest on the footer closes that for key
pages. No key page, data or dictionary, may declare more than 64 MiB
uncompressed, sixteen times the contract's largest key page of 1,000 keys
at the cap (about 4 MiB) and far above the 1 MiB pages writers aim for; and
no key dictionary may count more entries than 64 MiB holds at the larger of
the fewest bytes an entry of its type encodes to (8 for a long, the 4-byte
length of a byte array, a fixed-width column's width) and the bytes parquet
holds a decoded entry in (8 for a long, 32 for a byte array or fixed-width
handle in parquet 58), so a key dictionary fits the ceiling in memory as
well as on the wire. A key stored as a fixed-width column of width 0 holds
one distinct value, so it cannot key a table of more than one row, and
parquet's fixed-width decoder asserts a width above 0; the footer check
refuses it as a key type mismatch before any page is read. On a refresh the
refusal leaves the previous generation serving (RFD 005 (Generations and
refresh)). A file whose keys run far past the cap can meet this refusal
before the key-length one. A lookup reads the headers of the value page it
fetched, and of the value column's dictionary page when there is one, from
those same bytes before parquet decodes them, and refuses as a file fault
the same faults under a 64 MiB value page ceiling: a header parquet would
read to other numbers, an uncompressed size above the ceiling, a dictionary
of more values than its row group has rows or than the ceiling holds at the
larger of an entry's encoded and in-memory size, a compressed size past the
page's place, and v2 level lengths past the page's size. The contract caps
no value's length, so no bound follows a value page's rows. The ceiling is
sixteen times the 4 MiB message a gRPC client on tonic's defaults accepts,
and far above the 64 KiB value pages of the contract (RFD 002 (Serving
layout contract)); a page of large values bounded by rows alone, which the
doctor advises against, can pass it (a thousand values of 128 KiB is 125
MiB), and a lookup into such a page is refused though each value would fit
a message. What the pre-scan does not cover: the value counts inside a page
body, the lengths a delta-length or delta byte-array page carries, stay
unchecked. Parquet's decoder reads those; Iceberg's default writer uses v1
pages, which do not carry them for key columns. A memory limit on the
process is the backstop for what the checks do not cover (RFD 012
(Deployment contract)).

One question stays open. Decoded key arrays as an opt-in: under a per-table
memory budget they remove the per-lookup decode for tables that can afford
the memory; withholding them keeps one code path and one memory model until
a workload shows the decode is what it waits on.
