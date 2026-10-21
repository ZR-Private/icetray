# Tail latency

> Updated 2026-09-24: bounds on the live and background paths, for an
> object store or catalog that stops answering.

> Updated 2026-10-07: a hit reads one value page per column it names,
> each GET hedged on its own, and the hedge-rate ceiling counts GETs. A
> MultiGet carries at most `server.max_multi_get_values` values, keys
> times columns, and its fan-out bounds GETs in flight across the batch
> rather than lookups. See RFD 022.

> Updated 2026-10-08: the MultiGet fan-out is configuration,
> `server.multi_get_fan_out`, which a restart applies like every server
> setting (RFD 018). Measured against S3, a lone cold batch stops
> getting faster near 128 and concurrent batches share the process's GET
> throughput by 64; see [the
> measurements](../analysis/2026-10-08-multi-get-fan-out.md).

> Updated 2026-10-14: the fan-out is `server.batch_fan_out` and bounds
> the GETs of a Scan and a MultiScan as of a MultiGet; a Scan has
> `server.scan_deadline` and a MultiScan `server.multi_scan_deadline`,
> and span GETs are hedged in a window of their own per table whose
> delay scales with a span's pages. See RFD 025.

> Updated 2026-10-20: a span GET races its window's one delay, whatever
> its pages, and enters the window with its whole elapsed time. See RFD
> 025.

## Problem

A cold hit costs one GET per value column it names, and the latency of
those GETs belongs to the object store. First-byte time for a range read
varies with the size of the data file and carries a tail even at the
file sizes RFD 002 (Serving layout) targets ([range-GET latency
analysis, what the evidence
implies](../analysis/2026-09-10-s3-range-get-latency.md#what-the-evidence-implies)).
It varies more with how recently the file was read: in-region, a file
read moments before answers in tens of milliseconds at the median, one
read hours before takes several times as long, and one never read takes
longer still ([cold range-GET latency,
results](../analysis/2026-09-29-cold-range-get-latency.md#results)).

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

The GETs a client waits on are eligible: each value-page GET of a cold
hit, one per column it names (RFD 022 (Value columns)), and under the
bloom tier of RFD 017 (Residency tiers) the key-page GET before it. Each
GET is hedged on its own, in the table's one window, and the ceiling's
share counts GETs, so a hit that names several columns spends the ceiling
once per GET. Load reads, the footers and key column chunks read while a
generation is built, are bulk work nobody waits on, and they are never
hedged.

A hedge cuts the tail of a store that answers. A store or catalog that
stops answering is bounded instead, on two paths that share one S3 client
per bucket.

The live path, a gRPC request, fails fast. A `Get` has
`server.get_deadline` and a `MultiGet` `server.multi_get_deadline`,
whatever its size, and expiry is `DEADLINE_EXCEEDED`, which drops the
lookup. The gRPC layer, rather than icetray, enforces a deadline the
client sends: the client's library ends the call at it and reports the
expiry as the client's own, and tonic's server drops the call at the
same deadline, which frees its slot. A page fetch the lookup started
runs on its own task inside the page cache, so dropping the lookup
leaves the fetch running. The fetch instead has the `Get` deadline from
its own start. At that deadline the GET is dropped, which cancels the
request; every lookup waiting on the fetch fails, and nothing is cached.
A `MultiGet`'s page fetches have the same `Get` deadline, so over a
store that has stopped answering a batch fails at that deadline rather
than its own.

At most `server.max_in_flight` requests run at once, and one more is
refused with `RESOURCE_EXHAUSTED` rather than queued. A `MultiGet`
carries at most `server.max_multi_get_values` values, its keys times the
columns it names with a presence-only call counting one per key, and
holds at most `server.batch_fan_out` GETs in flight across the batch:
each GET a lookup issues, a key page under `bloom`, a value page, and a
dictionary page alike, holds one slot from before it starts until it
ends, and none waits for a slot while holding one, so a batch holds no
more GETs open whatever its keys and columns, each with at most one
hedge beside it. A live lookup expects well under 200 ms, so a deadline
of seconds is already a failure, and the refusals keep a wedged store
from piling up requests until the process runs out of memory. The cap
counts requests, and a page GET can outlast the request that started it
by at most one `Get` deadline: the request was open when the fetch
began, and the fetch ends one deadline after that.

The background path, loads and refreshes, is patient but bounded. A
catalog call has `storage.catalog_timeout`, a first load
`server.load_deadline`, and a refresh `server.refresh_deadline`. Past
its bound the work fails, the table is degraded, and the next poll tries
again, so no load holds its table's lock longer than that. The two load
bounds differ because the work does. A first load, or any build while no
generation serves, reads every file's footer and key chunks, where a
refresh reuses every file the new snapshot still names and reads only
the rest. A first load from real S3 of a table of over a billion rows in
tens of thousands of files runs past a refresh's bound when few files
are in flight. At the load's full concurrency, a fixed number of files
per worker thread ([Load time](../performance.md#load-time)), it is
bound by CPU instead and takes a fraction of that.

Both paths share the S3 client's bounds: `storage.max_retries` retries,
none started later than a retry window after the first attempt,
`storage.connect_timeout` to connect, and a timeout per attempt. The
window, one backoff, and one attempt end inside the margin by which a
cached credential is refreshed ahead of its expiry (RFD 003), so no
retry is signed with an expired credential. Shutdown waits
`server.drain_deadline` for open requests. Its default outlasts the
`MultiGet` deadline's, so open requests finish, and leaves time to flush
the page cache before an orchestrator's usual 30-second kill.

The deadlines, the two limits, the fan-out, the retry count, and the
connect and catalog timeouts are configuration (RFD 011), and [the
configuration reference](../configuration.md) gives their defaults. The
retry window and the per-attempt timeout are constants, because the
credential margin bounds their sum. The storage layer holds the three as
`S3_RETRY_TIMEOUT`, `S3_REQUEST_TIMEOUT`, and
`CREDENTIAL_REFRESH_MARGIN`, and a compile-time assertion checks the
sum. The defaults of `server.max_in_flight` and
`server.max_multi_get_values` are single-pod guesses, and how they hold
under concurrent load is open.

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
