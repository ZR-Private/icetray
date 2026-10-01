# Lookup hot path

## Problem

RFD 004 (Resident key-chunk index) fixes what stays in memory per data
file; the page cache fixes where a value page comes from. Undecided between
them: which decoder reads a key page and a value page, and what happens
when it wants a byte it was not given. icetray's workloads are billed per
GET and expect a free miss
([workload archetypes, the cost shape](../analysis/2026-09-09-workload-archetypes.md#the-cost-shape));
RFD 004 promises one GET per cold hit and none per miss
([prototype findings, finding 1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout)).
A reader that fetches what it needs turns that exact count into an
estimate.

## Decision

Lookups will decode keys and values with the Parquet column reader over the
in-memory byte segments they are handed (a resident key chunk, a page from
the page cache, or a key page from a disk-tier chunk file), with no Arrow in
the path, and a read of any byte outside those segments is an error.

The path for a table whose key chunks are resident:

1. The fence array picks the one key page that can hold the key.
2. The column reader decodes that page from the resident key chunk and
   finds the row or answers the miss. A miss ends here.
3. The offset index locates the value page and the page cache supplies it:
   one GET on a cold hit, none on a warm hit, shared by concurrent lookups
   for one page.
4. The column reader decodes one row from that page.

Two more segments are intended, counted reads: the value column's
dictionary page, the GET RFD 007 (Serving tiers) prices; and the key page
under the disk and bloom tiers of RFD 017 (Residency tiers). Every other
read is an error, so RFD 003's (One storage stack) request counter and RFD
007's penalties describe the server as it runs.

## Alternatives Considered

The Arrow record-batch stream per lookup. It builds a batch to return one
row and was slower on every case the prototype measured
([prototype findings, finding 4](../analysis/2026-09-10-prototype-findings.md#4-lookup-latency)).

A hand-written plain or delta decoder over the key chunk. It would re-grow
the encodings, compression, and definition-level handling the column
reader has, once per accepted layout.

Decoded key arrays kept in memory. Deferred; RFD 004 leaves them open under
a per-table memory budget no RFD yet defines.

A reader that fetches on demand. It hides the GETs that make one layout
cheaper than another.

## Consequences

Per-lookup CPU is one key page decoded and one value page decompressed,
and on a warm hit the decompression dominates
([prototype findings, finding 4](../analysis/2026-09-10-prototype-findings.md#4-lookup-latency));
hence RFD 002 (Serving layout contract) bounds value pages by bytes for
large values and RFD 007 prices large key pages.

A layout the resident index does not cover fails as an error in the
counting tests of RFD 014 (Test layering).

Open question: a decompressed-page cache level. A repeated hit would cost
the lookup logic alone, and each page its compression ratio in extra
memory; whether hot pages repeat enough to pay depends on the page cache's
hit-rate curve under a skewed key distribution, unmeasured
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)).
The performance sweep measured what the level would act on
([performance sweep, decompression](../analysis/2026-09-26-performance-sweep.md#decompression-the-open-question)):

- On the long-key table in the Full tier, zstd is 61.5% of a warm hit with
  random-byte values (ratio 1.00), 92.4% with JSON-like values (ratio
  4.38), and 84% of a miss with either: about 2.3 µs of a 12.8 µs warm hit
  with random values, about 60 µs of 71.2 µs with text.
- Estimated, not measured: a text-valued warm hit would drop to about
  11 µs, some six times the throughput per core, and a random-valued one to
  about 10.5 µs; a miss and a cold hit's GET are unchanged.
- For text the level costs 4.38 times the memory per page (45.7 MiB
  compressed, 200 MiB decompressed in the fixture), so the same budget
  holds 4.4 times fewer pages.
- A page the smaller cache loses is a cold hit, one S3 GET of about 90 to
  175 ms at the in-region median for a file not read recently
  ([cold range-GET latency](../analysis/2026-09-29-cold-range-get-latency.md)),
  the time of some 1,500 to 2,900 text-valued warm hits at 60 µs each; the
  level pays while fewer than about 0.03 to 0.07% of lookups become extra
  cold hits.
- A small decompressed level for the hottest pages alone, named by the
  sweep, was not built.

`icetray_page_cache_hits_total` and `icetray_page_cache_fetches_total` give
a replica's hit rate at its budget; the same counters on a replica whose
budget is divided by its tables' value compression ratio give the rate the
level would leave it.
