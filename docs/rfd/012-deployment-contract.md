# Deployment contract

## Problem

Every organization that runs icetray has its own way of handing a process
its configuration, credentials, logs, metrics, and readiness. A convention
written into the source has to be edited out by every other deployer.

The prototype showed two ways an operational surface can be wrong without
failing. A credential bridge that resolved credentials on every GET put a
helper process on the request path
([prototype findings, finding 11](../analysis/2026-09-10-prototype-findings.md#11-without-caller-side-caching-the-sdk-credential-chain-spawns-the-credential-helper-on-every-get)).
A snapshot id exported as a floating-point gauge lost its low digits
([prototype findings, finding 14](../analysis/2026-09-10-prototype-findings.md#14-metrics)).

## Decision

icetray will be operable through a twelve-factor surface alone
(configuration file path from a flag or environment variable, credentials
from the platform's default chain or mounted files, JSON logs with a
configurable field-name map and static fields, Prometheus metrics with
latency histograms, plaintext gRPC behind the platform's transport
security, gRPC health and a readiness endpoint), as one binary whose
subcommands are the server and the operator's tools, and will carry no
deployer-specific code.

The test for every addition is whether a second organization could deploy
the result without editing source. Upstream accepts shapes (a path, a
chain, a field-name map); a deployer supplies values in its own wrapper
image.

- Configuration is a file in either syntax RFD 011 (Configuration)
  accepts. A running server follows a change to its tables (RFD 018
  (Configuration reload)).
- `icetray serve`, `doctor` (RFD 016 (Doctor)), `get`, and `info` are
  subcommands of one binary with the same flags for where tables come
  from, so the serving image carries the tool that explains a refusal.
- Credentials are resolved and cached by the storage layer of RFD 003 (One
  storage stack). icetray has no secret format of its own.
- The listener speaks plaintext gRPC. TLS, authentication, and access
  policy are the platform's mesh or sidecar.
- A deployer may mount a local directory (RFD 017 (Residency tiers)). A
  replica without one serves the in-memory tiers only; a disk-residency
  table fails to load with a named reason, under the start-or-fail rule of
  RFD 005 (Generations and refresh).
- Logs are JSON events. The field-name map renames the standard fields
  (time, level, message, target); static fields are added to every event.
- Metrics are Prometheus: request counts by call; GET counts, local read
  counts, and latency histograms by kind (load, lookup), from the request
  counter of RFD 003; lookup latency by table and by result (cold hit, warm
  hit, miss); hedge counts; the resident index's live bytes per table
  beside process memory; and per table, the serving snapshot's sequence
  number and when it was swapped in.
- The snapshot id is in logs and the table-info reply. As a gauge value it
  loses digits
  ([prototype findings, finding 14](../analysis/2026-09-10-prototype-findings.md#14-metrics));
  as a label it adds a series with every swap.
- The gRPC listener and health service come up before any table has
  loaded. Readiness turns true once every configured table serves.
- The build carries its own protobuf compiler.
- A conformance run in CI starts the binary as an orchestrator would and
  checks that logs parse with the mapped names, metrics scrape, health
  answers before load completes, and readiness flips after.

## Alternatives Considered

Deployer-specific hooks upstream, such as a secret envelope format or a
house log schema. Rejected: each hook is one organization's convention
carried by every other deployer.

A fork per deployer. Rejected: a fork stops tracking upstream and dies.

Plain-text logs. Rejected: every platform ingests JSON, and a text format
needs a parser per deployer.

TLS and authentication inside the server. Rejected: every platform's mesh
or sidecar already does both, with its own certificate rotation and
identity.

Separate daemon and tool binaries. Rejected: two flag vocabularies and two
configuration paths drift apart, and the serving image would lack the tool
that diagnoses it.

## Consequences

A deployment without a mesh needs a sidecar or a private network; there is
no mode in which the port is safe to expose directly.

A replica runs under a memory limit set by its deployer: the doctor's
predicted resident bytes for each table's tier (doubled for a table a
refresh can rewrite wholesale), plus the page cache's memory budget, plus
a quarter of that total for the process, requests in flight, and a load's
transient chunks. The limit is the backstop for allocations the load
checks do not cover: RFD 004 (Resident key-chunk index) refuses a key page
header at load, and a value page header at lookup, that declares more than
its page can hold or more than a 64 MiB ceiling, and catches a panic
inside parquet's decoding as a refusal, but the lengths inside a
delta-encoded page still size buffers from the file. A file that trips
the limit takes down every replica that loads it; a refused file leaves
the last good generation serving.

Platform conveniences (metric naming, secret injection, log structure
beyond renamed fields) are the wrapper's job. The surface is a public
contract; a metric or log field renamed upstream breaks every wrapper.

A table that fails its first load exits the process nonzero, per the
start-or-fail rule of RFD 005 (Generations and refresh), after the reason
has been logged and exposed on the health service long enough to be
scraped. RFD 005 leaves open whether per-table lifecycle states with a
degraded state should replace start-or-fail, and readiness inherits the
question. For a degraded state: a replica serving nine tables of ten could
take traffic for the nine. Against: a ready replica that cannot answer for
one table moves the failure from the orchestrator, which can hold traffic,
to clients, which see errors.
