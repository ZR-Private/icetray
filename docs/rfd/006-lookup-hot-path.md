# Lookup hot path

## Problem

RFD 004 (Resident key-chunk index) settles what stays in memory for each
data file, and the page cache settles where a value page comes from.
Between them sits the work a lookup does with the bytes it already holds.
It has to find the key's row inside one key page and then pull one row out
of one value page. Two things about that work are undecided: which decoder
does it, and what happens when the decoder wants a byte it was not given.

Both bear on what a lookup spends. The workloads icetray is built for are
billed per GET and are meant to pay nothing for a miss
([workload archetypes, the cost shape](../analysis/2026-09-09-workload-archetypes.md#the-cost-shape)),
and RFD 004 promises one GET per cold hit and none per miss, a promise the
prototype kept on every layout
([prototype findings, finding 1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout)).
A general-purpose Parquet reader fetches whatever it needs, and a single
such fetch turns an exact count into an estimate. The decoder also sets
the CPU a lookup spends beside its GET.

## Decision

Lookups will decode keys and values with the Parquet column reader over the
in-memory byte segments they are handed (a resident key chunk, a page from
the page cache, or a key page from a disk-tier chunk file), keeping Arrow
out of the path and treating any attempt to read a byte outside those
segments as an error rather than a fetch.

The path is the same for every table whose key chunks are resident:

1. The fence array picks the one key page that can hold the key.
2. The column reader decodes that page from the resident key chunk and
   either finds the row or answers the miss. A miss ends here, before any
   value page is read.
3. The offset index locates the value page and the page cache supplies it.
   A cold hit costs one GET, a warm hit costs none, and concurrent lookups
   for one page share one GET.
4. The column reader decodes one row from that page and returns the value.

Two more segments are intended reads, counted as reads rather than raised
as errors. One is the value column's dictionary page when one exists, which
is the GET that RFD 007 (Serving tiers) prices. The other is the key page
itself under the disk and bloom tiers of RFD 017 (Residency tiers), which
fixes where that page comes from and what it costs.

The column reader is handed exactly those segments, and a request for
anything beyond them fails the lookup with an error. That keeps cost
accounting exact. The only GETs are the ones the design intends, so the
request counter of RFD 003 (One storage stack) and the penalties of RFD 007
(Serving tiers) describe the server as it actually runs.

## Alternatives Considered

The Arrow record-batch stream per lookup. It assembles a whole batch to
hand back one row, and it puts Arrow in the hot path. On the same fixtures
the column reader was faster in every case
([prototype findings, finding 4](../analysis/2026-09-10-prototype-findings.md#4-lookup-latency)).

A hand-written plain or delta decoder over the key chunk. It would have to
re-grow the encodings, compression, and definition-level handling the
column reader already has, and every layout the doctor accepts would need
its own path.

Decoded key arrays kept in memory, so that no key page is decoded per
lookup. Deferred: RFD 004 (Resident key-chunk index) leaves them open
pending a per-table memory budget, and that budget is itself undecided.

A reader that fetches on demand. It would serve every layout without
ceremony, and in doing so it would hide the GETs that make one layout
cheaper than another.

## Consequences

Per-lookup CPU is two pages: one key page decoded and one value page
decompressed. On a warm hit most of the time goes to decompressing the
value page and little to the lookup itself
([prototype findings, finding 4](../analysis/2026-09-10-prototype-findings.md#4-lookup-latency)).
That is why RFD 002 (Serving layout contract) bounds value pages by bytes
for large values, and why large key pages are a cost finding priced in
RFD 007.

A layout the resident index does not cover fails loudly, as an error in
the counting tests of RFD 014 (Test layering), instead of showing up as an
unexplained GET in production.

Open question: a decompressed-page cache level. The case for it is that
decompression is most of a warm hit, so caching decompressed pages would
bring a repeated hit down to the lookup logic alone. The case against is
that a decompressed page costs its compression ratio in extra memory, so
the same budget holds fewer pages. Whether hot pages repeat often enough
to pay for that depends on the page cache's hit-rate curve under a skewed
key distribution, and that curve is the unknown the question turns on
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)).

The decompression figures bound what the level could buy
([performance sweep, decompression](../analysis/2026-09-26-performance-sweep.md#decompression-the-open-question)).
On the long-key table in the Full tier, zstd is 61.5% of a warm hit with
values it stores as they are (random bytes, ratio 1.00) and 92.4% with
JSON-like values it compresses 4.38 times. It is 84% of a miss with either,
since a miss decompresses only the key page. The value page's share is
about 2.3 µs of a 12.8 µs warm hit with the random values and about 60 µs
of 71.2 µs with the text ones. By estimate from those figures, the level
would take a text-valued warm hit to about 11 µs, some six times the
throughput per core, and a random-valued one to about 10.5 µs. It would
save nothing on a miss or on a cold hit's GET.

What the level costs is the ratio. For random values that is nothing. For
the text values it is 4.38 times the memory per page (the fixture's value
pages are 45.7 MiB compressed and 200 MiB decompressed), so the same budget
holds 4.4 times fewer pages. A page the smaller cache loses comes back as a
cold hit, one S3 GET of about 90 to 175 ms at the in-region median for a
file not read recently
([cold range-GET latency](../analysis/2026-09-29-cold-range-get-latency.md)).
That one GET costs what some 1,500 to 2,900 text-valued warm hits would
save at 60 µs each, so the level pays only while it turns fewer than about
0.03 to 0.07% of lookups into extra cold hits. A small decompressed level
in front of the compressed cache, holding only the hottest pages, is one
possible arrangement. The question stays open on the hit-rate curve. A
production replica's `icetray_page_cache_hits_total` and
`icetray_page_cache_fetches_total` give its hit rate at the budget it runs,
and the same counters on a replica whose budget is divided by its tables'
value compression ratio give the rate the level would leave it.
