# Tail latency

> Updated 2026-09-24: bounds on the live and background paths, for an
> object store or catalog that stops answering.

## Problem

A cold hit costs one GET, and the latency of that GET belongs to the
object store. First-byte time for a range read varies with the size of the
data file and carries a tail even at the file sizes RFD 002 (Serving
layout contract) targets
([range-GET latency analysis, what the evidence implies](../analysis/2026-09-10-s3-range-get-latency.md#what-the-evidence-implies)).

The tail matters because of where icetray sits. The workload it is built
around puts it behind a small online store, answering the fall-through
reads that store cannot; the client waits on the slower of the two paths,
so the fall-through path is judged on its worst reads
([workload archetypes, archetype A](../analysis/2026-09-09-workload-archetypes.md#archetype-a-batch-computed-per-user-vectors)).

The usual remedy is a hedge: a second identical GET when the first has not
answered within a delay, and whichever answers first wins. The prototype
hedged with a fixed delay, and the outcome depended entirely on where that
delay sat relative to the latency of the table being read
([prototype findings, finding 10](../analysis/2026-09-10-prototype-findings.md#10-hedging)).

## Decision

Value-page reads will be hedged with a delay derived from the observed
request-latency distribution and capped by a hedge-rate ceiling, never a
fixed delay, and load reads will never be hedged.

The delay is a high percentile of recent lookup GETs over a sliding window,
so that it sits above the typical read and follows the object store as it
drifts, and the ceiling is a share of requests: while hedges would exceed
it, no new hedge is issued. Both values, and the window's length, are
configuration with defaults (RFD 011 (Configuration)), because the first
real measurement may move them. The window is per table, since a table's
data file size sets its first-byte latency and one pooled window would set
one table's delay from another table's reads. A hedge is a GET. It passes through the storage
layer of RFD 003 (One storage stack), is counted there, and increments its
own counter, so its cost is visible in the metrics of RFD 012 (Deployment
contract).

The GETs a client waits on are eligible: the value-page GET of a cold hit,
and under the bloom tier of RFD 017 (Residency tiers) the key-page GET
before it. Load reads, the footers and key column chunks read while a
generation is built, are bulk work nobody waits on, and are never hedged.

A hedge cuts the tail of a store that answers; a store or catalog that
stops answering is bounded instead, on two paths that share one S3 client
per bucket. The live path, a gRPC request, fails fast. A `Get` has 5
seconds and a `MultiGet` 15 seconds whatever its size, a shorter deadline
the client sends wins, and expiry is `DEADLINE_EXCEEDED`, which drops the
lookup and cancels its GETs; at most 1024 requests run at once, and one
more is refused with `RESOURCE_EXHAUSTED` rather than queued; a `MultiGet`
carries at most 1000 keys and runs 32 of its lookups at a time. A live
lookup expects well under 200 ms, so 5 seconds is already a failure, and
the refusals keep a wedged store from piling up requests until the process
runs out of memory. The background path, loads and refreshes, is patient
but bounded: a catalog call has 30 seconds, and a first load or a refresh
5 minutes, after which it fails, the table is degraded, and the next poll
tries again, so no load holds its table's lock longer than that. Both
paths share the S3 client's bounds: two retries, none started more than a
minute after the first attempt, a 1-second connect timeout, and 30 seconds
per attempt, which together end inside the two minutes a cached credential
is kept short of its expiry (RFD 003). Shutdown waits 20 seconds for open
requests, which the 15-second cap lets finish, and leaves time to flush
the page cache before an orchestrator's usual 30-second kill. The
deadlines, the two limits, the retry count, and the connect and catalog
timeouts are configuration with these defaults (RFD 011); the retry window,
the per-attempt timeout, and the fan-out of 32 are constants, the first two
because the credential margin bounds their sum. 1024 requests in flight
and 1000 keys per batch are single-pod guesses, to be revisited with
measurement.

## Alternatives Considered

A fixed configured delay. Rejected by measurement. Set below the object
store's typical latency, nearly every request hedged and the tail did not
move; set above it, almost none did. The right value also differs by data file size,
since first-byte latency does, so one number cannot serve every table
([prototype findings, finding 10](../analysis/2026-09-10-prototype-findings.md#10-hedging)).

Hedge every read. Rejected. Footer prefetches hedged during load, spending
GETs on bulk work whose latency nobody observes.

No hedging. Rejected. When a minority of reads stall, nothing else removes
them. In simulation with an injected stall, hedging collapsed the worst
cold lookup and brought the mean down with it
([prototype findings, finding 10](../analysis/2026-09-10-prototype-findings.md#10-hedging)).

## Consequences

Adaptive hedging has not been run against real object storage; only fixed
delays were
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)).
The percentile and ceiling that ship are starting points, and the first
real measurement may move both.

When the distribution has no tail, hedging does nothing useful and costs a
few GETs
([prototype findings, finding 10](../analysis/2026-09-10-prototype-findings.md#10-hedging));
the ceiling bounds that waste.

Three calls are open.

Which percentile, over what window. A lower percentile fires sooner and
cuts more of the tail, at more hedges; a higher one wastes fewer GETs but
lets a stall run longer before the second read goes out. A short window
follows a stall episode quickly and is noisy at low request rates; a long
one is stable and slow to adapt.

The ceiling's value. A tight ceiling keeps the GET overhead near nothing
but leaves any tail wider than the ceiling uncut; a loose one cuts more of
the tail and pays for it on every hit, in workloads billed per GET.

Whose distribution. A table's data file size sets its first-byte latency,
so a window pooled across tables would set one table's delay from another
table's reads and rebuild the fixed-delay failure
([range-GET latency analysis, what this does and does not establish](../analysis/2026-09-10-s3-range-get-latency.md#what-this-does-and-does-not-establish)).
A per-table window avoids that but has fewer samples, and a rarely read
table may never accumulate enough to estimate a percentile.
