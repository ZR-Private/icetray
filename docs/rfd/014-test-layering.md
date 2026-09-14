# Test layering

## Problem

icetray's value is a set of cost promises: one GET per cold hit, none per
miss, load paid per data file, refresh paid per changed file. A functional
test cannot see any of that. A lookup that issues three GETs returns the
same bytes as one that issues one. The prototype showed both ways this
fails. Cost promises stated in comments had no test that counted, and a
review found regressions a passing suite had missed. One refusal had no fixture
that produced its input, so it could never fire
([prototype findings, finding 15](../analysis/2026-09-10-prototype-findings.md#15-what-reviews-found-that-tests-did-not)).
Latency asserted in milliseconds depends on the machine and the network.
And real batch writers produce layouts a hand-built fixture does not
anticipate
([prototype findings, finding 9](../analysis/2026-09-10-prototype-findings.md#9-spark-writes-several-sorted-files-per-bucket-sometimes-overlapping)).
The project needs a rule for which layer proves what.

## Decision

Correctness and cost will be proven in layers: unit tests; in-process tests
over an in-memory store and an in-process S3 server with the GET counter as
the oracle; deterministic network simulation with round trips as the oracle;
container-gated catalog tests; and golden tables produced by real writers;
every cost promise gets a counting test, and every change to a cost path
gets an adversarial review before merge.

Every fixture value is a function of its key, so any answer is checkable
without a stored oracle
([prototype findings, method](../analysis/2026-09-10-prototype-findings.md#verifiable-answers)).

The in-process S3 server is driven by the real client, so tests exercise the
request path the server uses. The GET counter of RFD 003 (One storage stack)
is the oracle: tests assert exact counts, one per cold hit, none per miss,
and the per-file load and per-delta refresh costs that RFD 004 (Resident
key-chunk index) and RFD 005 (Generations and refresh) promise
([prototype findings, method](../analysis/2026-09-10-prototype-findings.md#the-request-counter-is-the-oracle)).

Simulation puts the same client and server on a simulated network and
yields identical round-trip counts on every run, so latency behavior,
including the hedging of RFD 013 (Tail latency), is asserted in round trips
([prototype findings, finding 12](../analysis/2026-09-10-prototype-findings.md#12-deterministic-simulation)).

Golden tables come from a real batch writer, written locally and on S3, and
cover the contract of RFD 002 (Serving layout contract) and the off-contract
layouts that RFD 007 (Serving tiers) prices
([prototype findings, method](../analysis/2026-09-10-prototype-findings.md#fixtures)).
They are located through an explicit setting, and their absence shows in
the test output.

Every refusal reason of RFD 007 and every doctor finding of RFD 016 (Doctor)
gets a fixture that triggers it: a delete file, an unsorted data file, a
duplicate key across data files. Each residency tier of RFD 017 (Residency
tiers) runs the same lookup suite with its own cost oracle (GETs
and local reads), and the disk tier's restart reuse and chunk-file cleanup
are counted in a temporary directory. A gated test that cannot run reports
as skipped, never as passed.

A change to a cost path gets an adversarial review before merge. Tests pin
the promises someone thought to count; the review looks for the ones nobody
did
([prototype findings, finding 15](../analysis/2026-09-10-prototype-findings.md#15-what-reviews-found-that-tests-did-not)).

## Alternatives Considered

Hand-written object-store mocks. Rejected: a mock answers the calls its
author imagined, so it exercises nothing of the real client, and a count
taken against it counts the mock.

Wall-clock latency assertions. Rejected: flaky where tight, empty where
loose, and tied to the machine that ran them. Round trips are deterministic
([prototype findings, finding 12](../analysis/2026-09-10-prototype-findings.md#12-deterministic-simulation)).

Containers for everything. Rejected: slow, and not always available where
the tests run. A container gates only the catalog tests, which need one.

## Consequences

The catalog tests need a running container and skip without it, so a green
run on a machine without one has proven less than it looks; the skip count
says how much.

Golden tables are large and live outside the repository. A contributor
without them runs a smaller suite, and the output says so.

One fixture the decision requires did not exist in the prototype: a
snapshot carrying a delete file
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)).
Until it exists, the delete-file refusal of RFD 009 (Writes between builds)
is unproven.

Every cost-path change costs a second reader.

Open question: whether a golden table is the recipe that regenerates it or a
fixed set of bytes. A recipe lets anyone rebuild the tables and follows the
writer as it changes; fixed bytes pin the exact layouts the findings were
measured on, including ones the writer produced for reasons not established
([prototype findings, finding 9](../analysis/2026-09-10-prototype-findings.md#9-spark-writes-several-sorted-files-per-bucket-sometimes-overlapping)).
