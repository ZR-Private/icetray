# Tail latency

> Updated 2026-09-24: bounds on the live and background paths, for an
> object store or catalog that stops answering.

## Problem

A cold hit costs one GET. First-byte time for a range read varies with
data file size and carries a tail even at the sizes RFD 002 (Serving
layout contract) targets
([range-GET latency analysis, what the evidence implies](../analysis/2026-09-10-s3-range-get-latency.md#what-the-evidence-implies)),
and more with how recently the file was read: about 26 ms at the median
in-region for a file read moments before, about 90 ms for one read hours
before, and up to about 175 ms for one never read
([cold range-GET latency, results](../analysis/2026-09-29-cold-range-get-latency.md#results)).
icetray answers a small online store's fall-through reads, so the client
waits on icetray's worst reads
([workload archetypes, archetype A](../analysis/2026-09-09-workload-archetypes.md#archetype-a-batch-computed-per-user-vectors)).
A hedge is a second identical GET after a delay; the prototype's fixed
delay worked only where it happened to sit relative to the table's latency
([prototype findings, finding 10](../analysis/2026-09-10-prototype-findings.md#10-hedging)).

## Decision

Value-page reads will be hedged with a delay derived from the observed
request-latency distribution and capped by a hedge-rate ceiling, never a
fixed delay, and load reads will never be hedged.

Hedging:

- The delay is a high percentile of recent lookup GETs over a sliding
  window. The ceiling is a share of requests; while hedges would exceed
  it, no new hedge is issued.
- Percentile, ceiling, and window length are configuration with defaults
  (RFD 011 (Configuration)).
- The window is per table, since data file size sets first-byte latency.
- A hedge is a GET through the storage layer of RFD 003 (One storage
  stack), counted there and on its own counter (RFD 012 (Deployment
  contract)).
- Eligible GETs are the ones a client waits on: the value-page GET of a
  cold hit, and under the bloom tier of RFD 017 (Residency tiers) the
  key-page GET before it. Load reads (footers and key column chunks) are
  never hedged.

A store or catalog that stops answering is bounded instead, on two paths
sharing one S3 client per bucket.

The live path, a gRPC request, fails fast:

- A `Get` has 5 seconds and a `MultiGet` 15 whatever its size; expiry is
  `DEADLINE_EXCEEDED` and drops the lookup.
- A deadline the client sends is enforced by the gRPC layer alone; tonic's
  server drops the call at it, freeing its slot.
- A page fetch runs on its own task inside the page cache and outlives its
  lookup. It has the `Get` deadline from its own start; at it the GET is
  dropped, every waiting lookup fails, and nothing is cached. A
  `MultiGet`'s page fetches have the same 5 seconds, so over a dead store
  a batch fails at 5 seconds.
- At most 1024 requests run at once; one more is refused at once with
  `RESOURCE_EXHAUSTED`. A `MultiGet` carries at most 1000 keys and runs 32
  lookups at a time. A live lookup expects well under 200 ms, so 5 seconds
  is already a failure.
- The cap counts requests; a page GET can outlast its request by at most
  one `Get` deadline.

The background path, loads and refreshes, is patient but bounded:

- A catalog call has 30 seconds, a first load 30 minutes, and a refresh 5
  minutes. At the bound the work fails, the table is degraded, and the
  next poll tries again.
- A first load, or any build while no generation serves, reads every
  file's footer and key chunks; a refresh reads only the files the new
  snapshot adds. A first load of 1.2 billion rows in 32,768 files
  against real S3 took 835 seconds at 16 files in flight, past a
  five-minute bound, and 79 seconds on four cores at 128 files per worker
  thread.

Both paths share the S3 client's bounds: two retries, none started more
than a minute after the first attempt, a 1-second connect timeout, and 30
seconds per attempt, which end inside the two minutes a cached credential
is kept short of its expiry (RFD 003). Shutdown waits 20 seconds for open
requests, inside an orchestrator's usual 30-second kill, with time to
flush the page cache.

The deadlines, the two limits, the retry count, and the connect and
catalog timeouts are configuration with these defaults (RFD 011). The
retry window, the per-attempt timeout, and the fan-out of 32 are
constants, the first two because the credential margin bounds their sum.
1024 requests in flight and 1000 keys per batch are single-pod
guesses, to be revisited with measurement.

## Alternatives Considered

A fixed configured delay. Rejected by measurement: below the store's
typical latency nearly every request hedged and the tail did not move;
above it, almost none did; and the right value differs by data file size
([prototype findings, finding 10](../analysis/2026-09-10-prototype-findings.md#10-hedging)).

Hedge every read. Rejected: footer prefetches hedged during load, spending
GETs on work nobody waits on.

No hedging. Rejected: in simulation with an injected stall, hedging
collapsed the worst cold lookup and brought the mean down with it
([prototype findings, finding 10](../analysis/2026-09-10-prototype-findings.md#10-hedging)).

## Consequences

Adaptive hedging has not been run against real object storage; only fixed
delays were
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)).
The shipped percentile and ceiling are starting points.

When the distribution has no tail, hedging costs a few GETs for nothing
([prototype findings, finding 10](../analysis/2026-09-10-prototype-findings.md#10-hedging));
the ceiling bounds that waste.

Three calls are open.

Which percentile, over what window. A lower percentile cuts more of the
tail at more hedges; a higher one wastes fewer GETs but lets a stall run
longer. A short window adapts quickly and is noisy at low request rates; a
long one is stable and slow.

The ceiling's value. A tight ceiling leaves any tail wider than it uncut;
a loose one pays on every hit, in workloads billed per GET.

Whose distribution. A pooled window would set one table's delay from
another's reads and rebuild the fixed-delay failure
([range-GET latency analysis, what this does and does not establish](../analysis/2026-09-10-s3-range-get-latency.md#what-this-does-and-does-not-establish));
a per-table window has fewer samples, and a rarely read table may never
accumulate enough.
