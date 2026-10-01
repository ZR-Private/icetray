# Test layering

## Problem

icetray's value is a set of cost promises: one GET per cold hit, none per
miss, load paid per data file, refresh paid per changed file. A functional
test sees none of them. In the prototype, cost promises in comments had no
counting test, a review found regressions a passing suite had missed, and
one refusal had no fixture that could fire it
([prototype findings, finding 15](../analysis/2026-09-10-prototype-findings.md#15-what-reviews-found-that-tests-did-not)).
Latency in milliseconds depends on the machine, and real batch writers
produce layouts a hand-built fixture does not anticipate
([prototype findings, finding 9](../analysis/2026-09-10-prototype-findings.md#9-spark-writes-several-sorted-files-per-bucket-sometimes-overlapping)).

## Decision

Correctness and cost will be proven in layers: unit tests; in-process tests
over an in-memory store and an in-process S3 server with the GET counter as
the oracle; deterministic network simulation with round trips as the oracle;
container-gated catalog tests; and golden tables produced by real writers;
every cost promise gets a counting test, and every change to a cost path
gets an adversarial review before merge.

- Every fixture value is a function of its key, so any answer is checkable
  without a stored oracle
  ([prototype findings, method](../analysis/2026-09-10-prototype-findings.md#verifiable-answers)).
- The in-process S3 server is driven by the real client. The GET counter
  of RFD 003 (One storage stack) is the oracle: exact counts per lookup,
  and the per-file load and per-delta refresh costs RFD 004 (Resident
  key-chunk index) and RFD 005 (Generations and refresh) promise
  ([prototype findings, method](../analysis/2026-09-10-prototype-findings.md#the-request-counter-is-the-oracle)).
- Simulation runs the same client and server on a simulated network;
  latency behavior, including the hedging of RFD 013 (Tail latency), is
  asserted in round trips, identical on every run
  ([prototype findings, finding 12](../analysis/2026-09-10-prototype-findings.md#12-deterministic-simulation)).
- Golden tables come from a real batch writer, locally and on S3, and
  cover the contract of RFD 002 (Serving layout contract) and the
  off-contract layouts RFD 007 (Serving tiers) prices
  ([prototype findings, method](../analysis/2026-09-10-prototype-findings.md#fixtures)).
  An explicit setting locates them; their absence shows in the test
  output.
- Every refusal reason of RFD 007 and every doctor finding of RFD 016
  (Doctor) gets a fixture that triggers it: a delete file, an unsorted
  data file, a duplicate key across data files.
- Each residency tier of RFD 017 (Residency tiers) runs the same lookup
  suite with its own cost oracle (GETs and local reads); the disk tier's
  restart reuse and chunk-file cleanup are counted in a temporary
  directory.
- A gated test that cannot run reports as skipped.
- Tests pin the promises someone thought to count; the adversarial review
  looks for the rest
  ([prototype findings, finding 15](../analysis/2026-09-10-prototype-findings.md#15-what-reviews-found-that-tests-did-not)).

## Alternatives Considered

Hand-written object-store mocks. Rejected: a count taken against a mock
counts the mock.

Wall-clock latency assertions. Rejected: flaky where tight, empty where
loose, and tied to the machine. Round trips are deterministic
([prototype findings, finding 12](../analysis/2026-09-10-prototype-findings.md#12-deterministic-simulation)).

Containers for everything. Rejected: slow, and not always available. A
container gates only the catalog tests.

## Consequences

The catalog tests skip without a container; the skip count says how much a
green run proved. Golden tables are large and live outside the repository;
without them a contributor runs a smaller suite, and the output says so.

The prototype had no snapshot carrying a delete file
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified));
until one exists, the delete-file refusal of RFD 009 (Writes between
builds) is unproven.

Every cost-path change costs a second reader.

Open question: whether a golden table is the recipe that regenerates it or
a fixed set of bytes. A recipe follows the writer as it changes; fixed
bytes pin the exact layouts the findings were measured on, including ones
the writer produced for reasons not established
([prototype findings, finding 9](../analysis/2026-09-10-prototype-findings.md#9-spark-writes-several-sorted-files-per-bucket-sometimes-overlapping)).
