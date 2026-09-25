# Deployment contract

## Problem

icetray is open source and meant to be run by organizations other than
the one that wrote it. Each has its own way of handing a process its
configuration, injecting credentials, shipping logs, scraping metrics, and
deciding when a replica may take traffic. If any of those conventions is
written into the source, a second deployer has to edit it to run icetray,
and the project has one deployer for good.

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

The test for every proposed addition is whether a second organization
could deploy the result without editing source. Upstream accepts shapes
(a path, a chain, a field-name map); a deployer supplies values in its own
wrapper, a container image built from the published one or from source,
carrying its configuration and log field names.

Configuration is a file, located by a flag or an environment variable,
in either syntax RFD 011 (Configuration) accepts, so a deployer whose
pipeline emits JSON hands the file over as is.

`icetray serve` runs the server; `doctor` (RFD 016 (Doctor)), `get`, and
`info` are subcommands of the same binary with the same flags for where
tables come from. The image a deployer runs therefore carries the tool that
explains a refusal, and an operator can run it inside the pod against the
daemon's own configuration and credentials.

Credentials come from the platform's default chain or from mounted files,
resolved and cached by the storage layer of RFD 003 (One storage stack),
so no deployer supplies a secret format of its own.

The listener speaks plaintext gRPC and terminates no TLS. icetray expects
to run on a private network, or behind a service mesh or sidecar that
encrypts traffic and authenticates callers, and it carries no certificate,
credential store, or access policy of its own.

A deployer may mount a local directory; what lives in it, and why it needs
no backup, is RFD 017 (Residency tiers). A replica without one can serve
only the in-memory tiers; a table configured for disk residency fails to
load with a named reason, and the start-or-fail rule of RFD 005
(Generations and refresh) applies.

Logs are JSON events; a field-name map in configuration renames the
standard fields (time, level, message, target) to the deployer's schema, and
static fields from configuration are added to every event.

Metrics are Prometheus: request counts by call; GET counts, local read
counts, and latency histograms by kind (load, lookup), read from the
request counter of RFD 003 (One storage stack); lookup latency by table and
by result (cold hit, warm hit, miss); hedge counts; the resident index's
live bytes per table beside process memory, because a residency tier that
sheds bytes is not visible in process memory on every platform; and a
generation info metric with the snapshot id as a label
([prototype findings, finding 14](../analysis/2026-09-10-prototype-findings.md#14-metrics)).

The gRPC listener and its health service come up before any table has
loaded. Readiness turns true only once every configured table serves its
first generation, so an orchestrator can tell a replica that is still
starting from one that is broken and route traffic to neither.

Building from source does not require a protobuf compiler on the build
host; the build carries its own.

A conformance run in CI starts the binary the way an orchestrator would
and checks each element of the surface: logs parse with the mapped names,
metrics scrape, health answers before load completes, readiness flips
after.

## Alternatives Considered

Deployer-specific hooks upstream, such as a secret envelope format or a
house log schema. Rejected by the test above: each hook is one
organization's convention carried by every other deployer.

A fork per deployer. Rejected: a fork stops tracking upstream and dies.

Plain-text logs. Rejected: every platform ingests JSON, and a text format
needs a parser per deployer.

TLS and authentication inside the server. Rejected: every platform already
has a mesh or a sidecar that does both, with its own certificate rotation
and identity, and a second implementation inside the process is one more
thing to configure and get wrong.

Separate daemon and tool binaries. Rejected: two flag vocabularies and two
configuration paths that drift apart, and a serving image without the tool
that diagnoses it, for the saving of a few megabytes of linked code in a
tool that prints JSON.

## Consequences

A deployment without a mesh needs a sidecar or a private network; there is
no mode in which the port is safe to expose directly.

A replica runs under a memory limit set by its deployer, sized from the
doctor's predicted resident bytes for each table's tier (doubled for a table
a refresh can rewrite wholesale, since the new generation loads beside the
serving one), plus the page cache's memory budget, plus a margin of a
quarter of that total for the process, requests in flight, and a load's
transient chunks. The limit is the backstop for allocations the load checks
do not cover, not a substitute for them: RFD 004 (Resident key-chunk index)
refuses a key page header that declares more than its page can hold, but a
footer that claims billions of rows, a value page decoded at lookup, and
the lengths inside a delta-encoded page still size buffers from the file. A
file that trips the limit takes down every replica that loads it, because
all of them load the same snapshot and restart into the same file; a limit
alone would turn one bad file into a fleet-wide crash loop, which is why the
cases the checks know are refused instead and, on a refresh, the last good
generation keeps serving.

Conveniences a given platform expects (its metric naming, its secret
injection, any log structure beyond renamed fields) are the wrapper's
job, and the wrapper is not in this repository. The surface is therefore
a public contract; a metric or log field renamed upstream breaks every
wrapper.

Readiness covers the window while tables load. A table that fails its first
load makes the process exit with a nonzero status, per the start-or-fail
rule of RFD 005 (Generations and refresh), after the reason has been logged
and exposed on the health service long enough to be scraped; the
orchestrator sees a restart with a logged refusal, not a replica that is
alive and never ready. RFD 005 leaves open whether per-table lifecycle
states with a degraded state should replace start-or-fail, and readiness
inherits the question. For a degraded state:
a replica serving nine tables of ten could take traffic for the nine.
Against: a ready replica that cannot answer for one table moves the
failure from the orchestrator, which can hold traffic, to clients, which
see errors.
