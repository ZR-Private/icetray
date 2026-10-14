# Transport and client contract

> Updated 2026-09-24: a MultiGet carries at most `server.max_multi_get_keys`
> keys, 1000 by default. See RFD 013.

> Updated 2026-10-07: Get and MultiGet name the value columns to read,
> and a present result carries one value per named column, each its bytes
> or null, so presence and null are separate answers. TableInfo lists the
> table's value columns, and the MultiGet limit is
> `server.max_multi_get_values`, counting keys times columns. See RFD 022.

> Updated 2026-10-07: each column a Get or MultiGet reads carries a list
> of protobuf field numbers to trim its value to, empty for the whole
> value; TableInfo reports each column's format; a value that is not the
> protobuf its column declares fails the call as `FAILED_PRECONDITION`,
> a deterministic failure the caller does not retry. See RFD 024.

> Updated 2026-10-14: an ordered table is read through Scan and
> MultiScan, which take a direction, a limit, and optional exclusive
> bounds as an `OrderValue` and return rows with their order values and
> a `more` flag; TableInfo reports the order column, its type, and
> `max_scan_rows`. See RFD 025.

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

Clients will speak gRPC as defined in the checked-in protobuf service
(Get; MultiGet, with results in request order and all from one snapshot;
Scan and MultiScan, which read an ordered table's rows (RFD 025 (Ordered
rows per key)); and TableInfo), every answer will carry the snapshot id
it came from, and on the client side JSON will exist only as the output
of the operator subcommands.

A key is a oneof of long, string, and bytes. The bytes variant carries the
binary, `fixed`, and uuid keys RFD 008 (Keys) admits, and the table's key
type says how its bytes are read. The variant must match the table's key
type, and a key sent in the wrong one is an invalid-argument error. Naming
a table the server does not serve is a not-found error.

Get and MultiGet each carry one list of column reads (RFD 022 (Value
columns)), applied to every key of the call, with no default: an empty
list asks whether the key is present and nothing more. A name the table
does not serve, or a name given twice, is an invalid-argument error.
Each read names a column and a list of top-level protobuf field numbers
to trim its value to, empty for the whole value (RFD 024 (Field
projection)); a list for a column not declared protobuf, a number
outside protobuf's range, or a number given twice is an
invalid-argument error.

Every result says whether the key is present or absent. A present result
carries one value per requested column, in the order requested, and each
value is either its bytes or null, in a message of its own, since a
proto3 list of bytes cannot hold a null. A null value, a zero-length
value, and an absent key are distinct on the wire. An absent result
carries no values, and neither does a present result of a presence-only
request. A miss is a normal answer rather than an error, and what a null
value means is the table owner's rule (RFD 002 (Serving layout)).

Get returns the outcome, the values, and the snapshot id. MultiGet answers
every key in the batch from one generation, so a batch is consistent with
itself. It carries one snapshot id for the whole batch, and its results
come back in the order the keys were sent, each with only the outcome and
the values. When any key in a MultiGet cannot be answered, the whole call
fails and the caller retries it, unless the failure is
`FAILED_PRECONDITION`: a value a read trims that is not the protobuf its
column declares fails the same way on every attempt, so the caller stops
naming fields of that column or waits for the table to be rewritten
rather than retrying.

TableInfo reports the snapshot id, bucket count, file count, row count, key
type, resident index bytes, and the table's value columns in name order,
each with its type, a `fixed` column's width, and its format, and for an
ordered table its order column, the column's type and a `fixed` width,
and `max_scan_rows`. The key type, each column's type, its format, and
the order type are enums, so clients switch on them.

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
only resends the batch the caller already built. A value that is not the
protobuf its column declares is the exception: its failure is
deterministic, so a retry fails again, and the caller drops the key or
the field list; the doctor and `icetray_projection_errors_total` surface
the value (RFD 024).

Streaming MultiGet. Deferred. The fan-out pattern in archetype A merges the
tier's results with the online store's, and request-order results in one
response serve that merge. A stream can be added later as a call of its
own.

## Consequences

Clients in any language are generated from the proto. icetray ships no
hand-written client; whether encoding helpers ship is RFD 008 (Keys). A
shell script or a person at a terminal goes through the operator
subcommands of RFD 012 (Deployment contract) and reads their JSON.

Because every answer names its snapshot, a caller can tell when two answers
straddle a generation swap. A single MultiGet batch never straddles one.

A MultiGet carries at most `server.max_multi_get_values` values, 1000 by
default, counted as its keys times the columns it names, a presence-only
call counting one per key. A larger batch is `INVALID_ARGUMENT` before any
lookup runs, which bounds the value reads one call can ask for (RFD 013
(Tail latency)). The
1000 is a single-pod guess. How lookups behave under concurrent load,
MultiGet fan-out included, is an open question
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)),
and its answer may move the limit.

A whole-batch failure discards the keys that were answered, which costs the
fan-out pattern in archetype A a second round of GETs for keys it already
had. If failures turn out to be common, a later version adds a separate
call with per-entry outcomes, and the shape of this one stays as it is.
