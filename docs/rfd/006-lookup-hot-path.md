# Lookup hot path

## Problem

RFD 004 (Resident key-chunk index) settles what stays in memory for each
data file, and the page cache settles where a value page comes from.
Between them sits the work a lookup does with the bytes it already holds:
find the key's row inside one key page, then pull one row out of one value
page. Two things are undecided: which decoder does it, and what happens
when the decoder wants a byte it was not given.

Both bear on what a lookup spends. The workloads icetray is built for are
billed per GET and are meant to pay nothing for a miss
([workload archetypes, the cost shape](../analysis/2026-09-09-workload-archetypes.md#the-cost-shape)),
and RFD 004 promises one GET per cold hit and none per miss, which the
prototype held on every layout it tried
([prototype findings, finding 1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout)).
A general-purpose Parquet reader fetches whatever it needs, and one such
fetch turns an exact count into an estimate. The decoder also sets the CPU
a lookup spends beside its GET.

## Decision

Lookups will decode keys and values with the Parquet column reader over the
in-memory byte segments they are handed (a resident key chunk, a page from
the page cache, or a key page from a disk-tier chunk file), with no Arrow in
the path and with any attempt to read a byte outside those segments treated
as an error rather than a fetch.

The path is the same for every table whose key chunks are resident:

1. The fence array picks the one key page that can hold the key.
2. The column reader decodes that page from the resident key chunk and
   either finds the row or answers the miss. A miss ends here and touches
   no value page.
3. The offset index locates the value page and the page cache supplies it:
   one GET on a cold hit, none on a warm hit, and concurrent lookups for
   one page share one GET.
4. The column reader decodes one row from that page and returns the value.

Two more segments are intended, counted reads rather than errors: the value
column's dictionary page when one exists, the GET that RFD 007 (Serving
tiers) prices, and the key page itself under the disk and bloom tiers of
RFD 017 (Residency tiers), which fixes where it comes from and what it
costs.

The column reader is handed exactly those segments; a request for anything
beyond them fails the lookup with an error. That keeps cost accounting
exact: the only GETs are the ones the design intends, so the request
counter of RFD 003 (One storage stack) and the penalties of RFD 007
(Serving tiers) describe the server as it runs.

## Alternatives Considered

The Arrow record-batch stream per lookup. It assembles a batch to hand
back one row and puts Arrow in the hot path. The prototype ran both on the
same fixtures and the column reader was faster on every case measured
([prototype findings, finding 4](../analysis/2026-09-10-prototype-findings.md#4-lookup-latency)).

A hand-written plain or delta decoder over the key chunk. It would have to
re-grow the encodings, compression, and definition-level handling the
column reader already has, and every layout the doctor accepts would need
its own path.

Decoded key arrays kept in memory, so no key page is decoded per lookup.
Deferred; RFD 004 (Resident key-chunk index) leaves them open under a
per-table memory budget that no RFD yet defines.

A reader that fetches on demand. It serves every layout without ceremony
and hides the GETs that make one layout cheaper than another.

## Consequences

Per-lookup CPU is two pages: one key page decoded, one value page
decompressed. On a warm hit most of the time goes to decompressing the
value page, and little to the lookup itself
([prototype findings, finding 4](../analysis/2026-09-10-prototype-findings.md#4-lookup-latency)).
That is why RFD 002 (Serving layout contract) bounds value pages by bytes
for large values, and why large key pages are a cost finding priced in
RFD 007.

A layout the resident index does not cover fails loudly, as an error in
the counting tests of RFD 014 (Test layering), instead of as an
unexplained GET in production.

Open question: a decompressed-page cache level. For it: decompression is
most of a warm hit, so caching decompressed pages would bring a repeated
hit down to the lookup logic alone. Against it: a decompressed page costs
its compression ratio in extra memory, so the same budget holds fewer
pages, and whether hot pages repeat enough to pay for it depends on the
page cache's hit-rate curve under a skewed key distribution, which has not
been measured
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)).

The performance sweep measured what that level would act on
([performance sweep, decompression](../analysis/2026-09-26-performance-sweep.md#decompression-the-open-question)).
On the long-key table in the Full tier, zstd is 61.5% of a warm hit with
values it stores as they are (random bytes, ratio 1.00) and 92.4% with
JSON-like values it compresses 4.38 times, and 84% of a miss with either,
since a miss decompresses only the key page. The value page's share is
about 2.3 µs of a 12.8 µs warm hit with the random values and about 60 µs
of 71.2 µs with the text ones. Estimated from those figures, not measured:
the level would take a text-valued warm hit to about 11 µs, some six times
the throughput per core, and a random-valued one to about 10.5 µs, and
would save nothing on a miss or on a cold hit's GET. What it costs is the
ratio: nothing for random values, and for the text values 4.38 times the
memory per page (the fixture's value pages are 45.7 MiB compressed and 200
MiB decompressed), so the same budget holds 4.4 times fewer pages. A page
the smaller cache loses comes back as a cold hit, one S3 GET of about 25 to
30 ms at the in-region median
([S3 range GET latency](../analysis/2026-09-10-s3-range-get-latency.md)),
which is what some 400 to 500 text-valued warm hits would save, so the
level pays only while it turns fewer than about 0.2% of lookups into extra
cold hits. A small decompressed level in front of the compressed cache, for
the hottest pages alone, is one arrangement the sweep named and did not
build. The question stays open on the hit-rate curve: a production
replica's `icetray_page_cache_hits_total` and
`icetray_page_cache_fetches_total` give its hit rate at the budget it runs,
and the same counters on a replica whose budget is divided by its tables'
value compression ratio give the rate the level would leave it.
