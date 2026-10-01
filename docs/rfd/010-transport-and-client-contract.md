# Transport and client contract

> Updated 2026-09-24: a MultiGet carries at most `server.max_multi_get_keys`
> keys, 1000 by default. See RFD 013.

## Problem

icetray answers get-by-key lookups for programs written in more than one
language. Values are opaque bytes and keys are typed (RFD 008 (Keys)), so a
transport that only speaks text puts an encoding layer in every client.

The workload that motivates the project reads many keys per request. Its
client consults a small online store and then falls through to the
lake-served tier, or fans out to that tier concurrently
([workload archetypes, archetype A](../analysis/2026-09-09-workload-archetypes.md#archetype-a-batch-computed-per-user-vectors)).
A multi-key read therefore needs a defined shape, including what happens
when one key in a batch cannot be answered. A table also advances by
swapping generations (RFD 005 (Generations and refresh)), so a caller
comparing two answers needs to know whether they came from the same
snapshot.

The contract has to be written down, because an unwritten contract grows a
second transport. The prototype carried a gRPC service definition beside a
command-line tool with nothing recording the contract between them
([prototype findings, what was built](../analysis/2026-09-10-prototype-findings.md#what-was-built)).

## Decision

Clients will speak gRPC as defined in the checked-in protobuf service (Get;
MultiGet, with results in request order and all from one snapshot; and
TableInfo), every answer will carry the snapshot id it came from, and on
the client side JSON will exist only as the output of the operator
subcommands.

A key is a oneof of long, string, and bytes. The bytes variant carries the
binary, `fixed`, and uuid keys RFD 008 (Keys) admits, and the table's key
type says how its bytes are read. The variant must match the table's key
type, and a key sent in the wrong one is an invalid-argument error. Naming
a table the server does not serve is a not-found error.

Every result names one of three outcomes, each distinct on the wire:
present with the value's bytes, present with a null value, or absent. A
miss is a normal answer rather than an error, and what a null value means
is the table owner's rule (RFD 002 (Serving layout contract)).

Get returns the outcome, the value, and the snapshot id. MultiGet answers
every key in the batch from one generation, so a batch is consistent with
itself. It carries one snapshot id for the whole batch, and its results
come back in the order the keys were sent, each with only the outcome and
the value. When any key in a MultiGet cannot be answered, the whole call
fails and the caller retries it.

TableInfo reports the snapshot id, bucket count, file count, row count, key
type, and resident index bytes. The key type is an enum, so clients switch
on it.

Field comments in the proto describe what a caller can rely on and leave
the server's internal structures out.

This decision covers the lookup transport. Configuration is RFD 011
(Configuration), and logs, health, and readiness are RFD 012 (Deployment
contract).

## Alternatives Considered

HTTP with JSON bodies. Rejected because values are bytes and keys are
typed, and JSON carries neither natively. Every client would base64 its
values and type its keys by convention, which is a second contract layered
on the first.

Both gRPC and a JSON transport. Rejected: two transports are two contracts,
and they would have to be kept equal.

Answering the rest of a MultiGet and marking the failed entry. Rejected for
the first version. It puts an error check on every result of every call, a
cost every caller pays for a failure expected to be rare, while a retry
only resends the batch the caller already built.

Streaming MultiGet. Deferred. The fan-out pattern in archetype A merges the
tier's results with the online store's, and request-order results in one
response serve that merge. A stream can be added later as a fourth call.

## Consequences

Clients in any language are generated from the proto. icetray ships no
hand-written client; whether encoding helpers ship is RFD 008 (Keys). A
shell script or a person at a terminal goes through the operator
subcommands of RFD 012 (Deployment contract) and reads their JSON.

Because every answer names its snapshot, a caller can tell when two answers
straddle a generation swap. A single MultiGet batch never straddles one.

A MultiGet carries at most `server.max_multi_get_keys` keys, 1000 by
default, and a larger batch is `INVALID_ARGUMENT` before any lookup runs,
which bounds the lookups one call can ask for (RFD 013 (Tail latency)). The
1000 is a single-pod guess. How lookups behave under concurrent load,
MultiGet fan-out included, is an open question
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)),
and its answer may move the limit.

A whole-batch failure discards the keys that were answered, which costs the
fan-out pattern in archetype A a second round of GETs for keys it already
had. If failures turn out to be common, a later version adds a separate
call with per-entry outcomes, and the shape of this one stays as it is.
