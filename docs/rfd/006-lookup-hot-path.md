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

Open question: a decompressed-page cache tier. For it: decompression is
most of a warm hit, so caching decompressed pages would bring a repeated
hit down to the lookup logic alone. Against it: a decompressed page costs
its compression ratio in extra memory, so the same budget holds fewer
pages, and whether hot pages repeat enough to pay for it depends on the
page cache's hit-rate curve under a skewed key distribution, which has not
been measured
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)).
