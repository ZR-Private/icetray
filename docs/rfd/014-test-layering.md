# Test layering

> Updated 2026-10-06: a fixture value is a function of its key and of the
> version that wrote it. Under merge-on-read (RFD 021) a key's deleted
> copy and its live copy sit in different files, and a value of the key
> alone would let a lookup that answered from the deleted copy pass.

## Problem

icetray's value is a set of cost promises: one GET per cold hit, none per
miss, load paid per data file, refresh paid per changed file. A functional
test cannot see any of them, because a lookup that issues three GETs
returns the same bytes as one that issues one. The prototype showed two
ways this goes wrong. Cost promises stated in comments had no test that
counted them, and a review found regressions that a passing suite
had missed. One refusal had no fixture that produced its input, so it
could never fire
([prototype findings, finding 15](../analysis/2026-09-10-prototype-findings.md#15-what-reviews-found-that-tests-did-not)).
Latency asserted in milliseconds depends on the machine and the network.
And real batch writers produce layouts that a hand-built fixture does not
anticipate
([prototype findings, finding 9](../analysis/2026-09-10-prototype-findings.md#9-spark-writes-several-sorted-files-per-bucket-sometimes-overlapping)).
The project needs a rule for which layer proves what.

## Decision

Correctness and cost will be proven in layers (unit tests; in-process
tests over an in-memory store and an in-process S3 server, with the GET
counter as the oracle; deterministic network simulation, with round trips
as the oracle; container-gated catalog tests; and golden tables produced
by real writers), every cost promise will get a counting test, and every
change to a cost path will get an adversarial review before merge.

Every fixture value is a function of its key and of the version that
wrote it, so any answer can be checked without a stored oracle, and a test
can tell a live row from a deleted copy of the same key
([prototype findings, method](../analysis/2026-09-10-prototype-findings.md#verifiable-answers)).

The in-process S3 server is driven by the real client, so the tests
exercise the same request path the server uses. The GET counter of RFD 003
(One storage stack) is the oracle. Tests assert exact counts: one per cold
hit, none per miss, and the per-file load and per-delta refresh costs that
RFD 004 (Resident key-chunk index) and RFD 005 (Generations and refresh)
promise
([prototype findings, method](../analysis/2026-09-10-prototype-findings.md#the-request-counter-is-the-oracle)).

Simulation runs the same client and server on a simulated network and
yields identical round-trip counts on every run. Latency behavior,
including the hedging of RFD 013 (Tail latency), is therefore asserted in
round trips
([prototype findings, finding 12](../analysis/2026-09-10-prototype-findings.md#12-deterministic-simulation)).

Golden tables come from a real batch writer, written both locally and on
S3. They cover the contract of RFD 002 (Serving layout contract) and the
off-contract layouts that RFD 007 (Serving tiers) finds inefficient
([prototype findings, method](../analysis/2026-09-10-prototype-findings.md#fixtures)).
The tests find them through an explicit setting, and the test output shows
when they are absent.

Every refusal reason of RFD 007 and every doctor finding of RFD 016
(Doctor) gets a fixture that triggers it, such as a delete file, an
unsorted data file, or a duplicate key across data files. Each residency
tier of RFD 017 (Residency tiers) runs the same lookup suite against its
own cost oracle of GETs and local reads, and the disk tier's restart reuse
and chunk-file cleanup are counted in a temporary directory. A gated test
that cannot run reports as skipped, and a skip is never reported as a
pass.

A change to a cost path gets an adversarial review before merge. Tests
pin down the promises someone thought to count, and the review is there to
find the ones nobody did
([prototype findings, finding 15](../analysis/2026-09-10-prototype-findings.md#15-what-reviews-found-that-tests-did-not)).

## Alternatives Considered

Hand-written object-store mocks. Rejected: a mock answers only the calls
its author imagined, so it exercises none of the real client, and a count
taken against it measures the mock.

Wall-clock latency assertions. Rejected: a tight bound is flaky, a loose
one proves little, and either is tied to the machine that ran it. Round
trips are deterministic
([prototype findings, finding 12](../analysis/2026-09-10-prototype-findings.md#12-deterministic-simulation)).

Containers for everything. Rejected: they are slow and are not always
available where the tests run. A container gates only the catalog tests,
which need one.

## Consequences

The catalog tests need a running container and skip without it. A green
run on a machine without one has proven less than it appears to, and the
skip count says how much less.

Golden tables are large and live outside the repository. A contributor
without them runs a smaller suite, and the output says so.

The delete-file refusal of RFD 009 (Writes between builds) depends on a
fixture the prototype lacked, a snapshot carrying a delete file
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)).
That refusal counts as proven only once a test produces such a snapshot.

Each change to a cost path needs a second reader, and that reader's time
is a cost the project accepts.

Open question: whether a golden table is the recipe that regenerates it or
a fixed set of bytes. A recipe lets anyone rebuild the tables and follows
the writer as it changes. Fixed bytes pin the exact layouts the findings
were measured on, including layouts the writer produced for reasons that
were never established
([prototype findings, finding 9](../analysis/2026-09-10-prototype-findings.md#9-spark-writes-several-sorted-files-per-bucket-sometimes-overlapping)).
