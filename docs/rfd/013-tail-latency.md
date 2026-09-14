# Tail latency

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
