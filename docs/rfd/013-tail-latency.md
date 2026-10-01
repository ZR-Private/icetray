# Tail latency

> Updated 2026-09-24: bounds on the live and background paths, for an
> object store or catalog that stops answering.

## Problem

A cold hit costs one GET, and the latency of that GET belongs to the
object store. First-byte time for a range read varies with the size of the
data file and carries a tail even at the file sizes RFD 002 (Serving
layout contract) targets
([range-GET latency analysis, what the evidence implies](../analysis/2026-09-10-s3-range-get-latency.md#what-the-evidence-implies)).
It varies more with how recently the file was read: in-region, the median
is about 26 ms for a file read moments before, about 90 ms for one read
hours before, and up to about 175 ms for one never read
([cold range-GET latency, results](../analysis/2026-09-29-cold-range-get-latency.md#results)).

The tail matters because of where icetray sits. The workload it is built
around puts it behind a small online store, answering the fall-through
reads that store cannot. The client waits on the slower of the two paths,
so the fall-through path is judged on its worst reads
([workload archetypes, archetype A](../analysis/2026-09-09-workload-archetypes.md#archetype-a-batch-computed-per-user-vectors)).

The usual remedy is a hedge: a second identical GET when the first has not
answered within a delay, with whichever answers first winning. With a
fixed delay, the outcome depends entirely on where that delay sits
relative to the latency of the table being read
([prototype findings, finding 10](../analysis/2026-09-10-prototype-findings.md#10-hedging)).

## Decision

Value-page reads will be hedged with a delay derived from the observed
request-latency distribution rather than a fixed delay, capped by a
hedge-rate ceiling, and load reads will never be hedged.

The delay is a high percentile of recent lookup GETs over a sliding
window, so that it sits above the typical read and follows the object
store as it drifts. The ceiling is a share of requests: while hedges would
exceed it, no new hedge is issued. Both values, and the window's length,
are configuration with defaults (RFD 011 (Configuration)), because the
defaults are starting points that measurement may move. The window is per
table, since a table's data file size sets its first-byte latency and one
pooled window would set one table's delay from another table's reads.

A hedge is a GET. It passes through the storage layer of RFD 003 (One
storage stack), is counted there, and increments its own counter, so its
cost is visible in the metrics of RFD 012 (Deployment contract).

The GETs a client waits on are eligible: the value-page GET of a cold hit,
and under the bloom tier of RFD 017 (Residency tiers) the key-page GET
before it. Load reads, the footers and key column chunks read while a
generation is built, are bulk work nobody waits on, and they are never
hedged.

A hedge cuts the tail of a store that answers. A store or catalog that
stops answering is bounded instead, on two paths that share one S3 client
per bucket.

The live path, a gRPC request, fails fast. A `Get` has 5 seconds and a
`MultiGet` 15 seconds whatever its size, and expiry is
`DEADLINE_EXCEEDED`, which drops the lookup. The gRPC layer, rather than
icetray, enforces a deadline the client sends: the client's library ends
the call at it and reports the expiry as the client's own, and tonic's
server drops the call at the same deadline, which frees its slot. A page
fetch the lookup started runs on its own task inside the page cache, so
dropping the lookup leaves the fetch running. The fetch instead has the
`Get` deadline from its own start. At that deadline the GET is dropped,
which cancels the request; every lookup waiting on the fetch fails, and
nothing is cached. A `MultiGet`'s page fetches have the same 5 seconds, so
over a store that has stopped answering a batch fails at 5 seconds rather
than 15.

At most 1024 requests run at once, and one more is refused with
`RESOURCE_EXHAUSTED` rather than queued. A `MultiGet` carries at most 1000
keys and runs 32 of its lookups at a time. A live lookup expects well
under 200 ms, so 5 seconds is already a failure, and the refusals keep a
wedged store from piling up requests until the process runs out of memory.
The cap counts requests, and a page GET can outlast the request that
started it by at most one `Get` deadline: the request was open when the
fetch began, and the fetch ends 5 seconds after that.

The background path, loads and refreshes, is patient but bounded. A
catalog call has 30 seconds, a first load 30 minutes, and a refresh 5
minutes. Past its bound the work fails, the table is degraded, and the
next poll tries again, so no load holds its table's lock longer than that.
The two load bounds differ because the work does. A first load, or any
build while no generation serves, reads every file's footer and key
chunks, where a refresh reuses every file the new snapshot still names and
reads only the rest. A first load of a table of 1.2 billion rows in
32,768 files from real S3 takes 835 seconds at 16 files in flight, past a
five-minute bound, and 79 seconds on four cores at 128 files per worker
thread.

Both paths share the S3 client's bounds: two retries, none started more
than a minute after the first attempt, a 1-second connect timeout, and 30
seconds per attempt. Together they end inside the two minutes a cached
credential is kept short of its expiry (RFD 003). Shutdown waits 20
seconds for open requests, which the 15-second cap lets finish, and leaves
time to flush the page cache before an orchestrator's usual 30-second
kill.

The deadlines, the two limits, the retry count, and the connect and
catalog timeouts are configuration with these defaults (RFD 011). The
retry window, the per-attempt timeout, and the fan-out of 32 are
constants, the first two because the credential margin bounds their sum.
The 1024 requests in flight and 1000 keys per batch are single-pod
guesses, and how they hold under concurrent load is open.

## Alternatives Considered

A fixed configured delay. Rejected on the evidence. Set below the object
store's typical latency, nearly every request hedges and the tail does not
move; set above it, almost none do. The right value also differs by data
file size, since first-byte latency does, so one number cannot serve every
table
([prototype findings, finding 10](../analysis/2026-09-10-prototype-findings.md#10-hedging)).

Hedge every read. Rejected, because it hedges footer prefetches during
load and spends GETs on bulk work whose latency nobody observes.

No hedging. Rejected. When a minority of reads stall, nothing else removes
them. In simulation with an injected stall, hedging collapses the worst
cold lookup and brings the mean down with it
([prototype findings, finding 10](../analysis/2026-09-10-prototype-findings.md#10-hedging)).

## Consequences

The evidence for hedging comes from fixed delays
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)),
so the percentile and ceiling that ship are starting points, and how
adaptive hedging behaves against real object storage may move both.

When the distribution has no tail, hedging buys nothing and costs a few
GETs
([prototype findings, finding 10](../analysis/2026-09-10-prototype-findings.md#10-hedging));
the ceiling bounds that waste.

Three calls are open.

Which percentile, over what window. A lower percentile fires sooner and
cuts more of the tail, at the price of more hedges; a higher one wastes
fewer GETs but lets a stall run longer before the second read goes out. A
short window follows a stall episode quickly and is noisy at low request
rates, while a long one is stable and slow to adapt.

The ceiling's value. A tight ceiling keeps the GET overhead near nothing
but leaves any tail wider than the ceiling uncut. A loose one cuts more of
the tail and pays for it on every hit, in workloads billed per GET.

Whose distribution. A table's data file size sets its first-byte latency,
so a window pooled across tables would set one table's delay from another
table's reads and rebuild the fixed-delay failure
([range-GET latency analysis, what this does and does not establish](../analysis/2026-09-10-s3-range-get-latency.md#what-this-does-and-does-not-establish)).
A per-table window avoids that but has fewer samples, and a rarely read
table may never accumulate enough to estimate a percentile.
