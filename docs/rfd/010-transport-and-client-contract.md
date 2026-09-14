# Transport and client contract

## Problem

icetray answers get-by-key lookups to programs, and those programs will be
written in more than one language. Values are opaque bytes and keys are
typed (RFD 008 (Keys)), so a transport that only speaks text puts an
encoding layer in every client. The workload that motivates the project
reads many keys per request from a client that consults a small online
store and falls through, or fans out concurrently, to the lake-served tier
([workload archetypes, archetype A](../analysis/2026-09-09-workload-archetypes.md#archetype-a-batch-computed-per-user-vectors));
a multi-key read therefore needs a defined shape, including what happens
when one key in a batch cannot be answered. A table advances by swapping
generations (RFD 005 (Generations and refresh)), so a caller comparing two
answers needs to know whether they came from the same snapshot. The
prototype carried a gRPC service definition beside a command-line tool
([prototype findings, what was built](../analysis/2026-09-10-prototype-findings.md#what-was-built))
without writing the contract down, and an unwritten contract grows a second
transport.

## Decision

Clients will speak gRPC as defined in the checked-in protobuf service (Get,
MultiGet with results in request order all from one snapshot, and
TableInfo), every answer will carry the snapshot id it came from, and on the
client side JSON will exist only as the output of the operator
subcommands.

A key is a oneof of long, string, and bytes; the bytes variant carries the
binary, `fixed`, and uuid keys RFD 008 (Keys) admits, and the table's key
type says how its bytes are read. The variant must match the table's key type; sending the
wrong one is an invalid-argument error. Naming a table the server does not
serve is a not-found error. Every result names one of three outcomes, distinct
on the wire: present, with the value's bytes; present with a null value; or
absent. A miss is a normal answer, not an error, and what a null value means
is the table owner's rule (RFD 002 (Serving layout contract)). Get returns
the outcome, the value, and the snapshot id. MultiGet answers every key in the batch from one
generation, so a batch is consistent with itself; it carries one snapshot
id for the whole batch, and each result carries the outcome and value only, in
the order the keys were sent. When any key in a MultiGet cannot be answered,
the whole call fails and the caller retries it. TableInfo reports the
snapshot id, bucket
count, file count, row count, key type, and resident index bytes; the key
type is an enum, so clients switch on it. Field
comments in the proto describe what a caller can rely on; the server's
internal structures stay out of them.

This decision covers the lookup transport; configuration is RFD 011
(Configuration), and logs, health, and readiness are RFD 012 (Deployment
contract).

## Alternatives Considered

HTTP with JSON bodies. Rejected: values are bytes and keys are typed; JSON
carries neither natively, so every client would base64 its values and type
its keys by convention, a second contract layered on the first.

Both gRPC and a JSON transport. Rejected: two transports are two contracts
that have to be kept equal.

Answering the rest of a MultiGet and marking the failed entry. Rejected for
the first version: it puts an error check on every result of every call, a
cost every caller pays for a failure expected to be rare, and the batch a
caller retries is the one it already built.

Streaming MultiGet. Deferred: the fan-out pattern in archetype A merges the
tier's results with the online store's, and request-order results in one
response serve that. A stream can be added later as a fourth call.

## Consequences

Clients in any language are generated from the proto. icetray ships no
hand-written client; whether encoding helpers ship is RFD 008 (Keys). A
shell script or a person at a terminal goes through the operator
subcommands of RFD 012 (Deployment contract) and reads their JSON.

Because every answer names its snapshot, a caller can tell when two answers
straddle a generation swap, and a MultiGet batch never does.

Behavior under concurrent load, MultiGet fan-out included, was not measured
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)),
so whether the contract needs a batch-size cap is not yet known.

A whole-batch failure discards the keys that were answered, which costs the
fan-out pattern in archetype A a second round of GETs for keys it already
had. If failures turn out to be common, a later version adds a separate call
with per-entry outcomes; the shape of this one does not change.
