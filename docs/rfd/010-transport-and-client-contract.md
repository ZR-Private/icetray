# Transport and client contract

> Updated 2026-09-24: a MultiGet carries at most `server.max_multi_get_keys`
> keys, 1000 by default. See RFD 013.

## Problem

icetray's clients are programs in more than one language. Values are
opaque bytes and keys are typed (RFD 008 (Keys)), so a text-only transport
puts an encoding layer in every client. The motivating workload reads many
keys per request, from a client that falls through to icetray after a
small online store or fans out to both
([workload archetypes, archetype A](../analysis/2026-09-09-workload-archetypes.md#archetype-a-batch-computed-per-user-vectors)),
so a multi-key read needs a defined shape, including what happens when one
key cannot be answered. Generations swap (RFD 005 (Generations and
refresh)), so a caller needs to know whether two answers share a snapshot.
The prototype had a gRPC service
([prototype findings, what was built](../analysis/2026-09-10-prototype-findings.md#what-was-built))
and no written contract.

## Decision

Clients will speak gRPC as defined in the checked-in protobuf service (Get,
MultiGet with results in request order all from one snapshot, and
TableInfo), every answer will carry the snapshot id it came from, and on the
client side JSON will exist only as the output of the operator
subcommands.

- A key is a oneof of long, string, and bytes. The bytes variant carries
  the binary, `fixed`, and uuid keys RFD 008 (Keys) admits; the table's
  key type says how its bytes are read.
- A variant that does not match the table's key type is an
  invalid-argument error. A table the server does not serve is a not-found
  error.
- A result is one of three outcomes, distinct on the wire: present with
  the value's bytes, present with a null value, or absent. A miss is a
  normal answer. What a null value means is the table owner's rule (RFD
  002 (Serving layout contract)).
- Get returns the outcome, the value, and the snapshot id.
- MultiGet answers every key from one generation and carries one snapshot
  id for the batch; each result carries the outcome and value, in request
  order. When any key cannot be answered, the whole call fails and the
  caller retries it.
- TableInfo reports the snapshot id, bucket count, file count, row count,
  key type (an enum), and resident index bytes.
- Field comments in the proto describe what a caller can rely on.

Configuration is RFD 011 (Configuration); logs, health, and readiness are
RFD 012 (Deployment contract).

## Alternatives Considered

HTTP with JSON bodies. Rejected: JSON carries neither bytes nor typed keys
natively, so every client would layer a second contract on the first.

Both gRPC and a JSON transport. Rejected: two contracts to keep equal.

Answering the rest of a MultiGet and marking the failed entry. Rejected for
the first version: an error check on every result, for a failure expected
to be rare.

Streaming MultiGet. Deferred: request-order results in one response serve
the fan-out pattern in archetype A. A stream can be added as a fourth call.

## Consequences

Clients in any language are generated from the proto; icetray ships no
hand-written client, and whether encoding helpers ship is RFD 008 (Keys).
Shell scripts and people use the operator subcommands of RFD 012
(Deployment contract) and read their JSON.

A caller can tell when two answers straddle a generation swap. A MultiGet
batch never does.

A MultiGet carries at most `server.max_multi_get_keys` keys, 1000 by
default; a larger batch is `INVALID_ARGUMENT` before any lookup (RFD 013
(Tail latency)). Concurrent load was not measured
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)),
so the 1000 is a single-pod guess.

A whole-batch failure discards the keys that were answered, costing
archetype A a second round of GETs. If failures are common, a later version
adds a separate call with per-entry outcomes.
