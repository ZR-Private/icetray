# Calling icetray

A service calls icetray over gRPC with a client generated from
[`icetray.proto`](../crates/icetray-proto/proto/icetray.proto), whose
comments are the reference this page summarizes. There are three calls:
`Get` looks up one key, `MultiGet` looks up many keys from one snapshot,
and `TableInfo` describes a table the server serves. The
[glossary](glossary.md) defines terms such as snapshot and residency.

The server rejects most malformed requests as `INVALID_ARGUMENT`, but a
few rules are the caller's to keep, and the server cannot detect a break
in them. Each column a request lists adds a GET to a cold hit, whether
the caller uses the value or discards it. A caller that reads an absent
key as a null value, or a null as a zero-length value, misreads the row.
A projection that lists only one member of a oneof can return a member
the whole value cleared, and a projected message without its required
fields fails a strict parse. A retry of an error that recurs fails again
and adds load. So a caller requests only the columns, and for a protobuf
column only the fields, that it uses; reads presence and null as separate
facts; lists every member of a oneof and parses a projected value
partially; and retries only the transient errors.

## Connecting

icetray serves gRPC on port 7433 by default, in plaintext, with the
standard `grpc.health.v1` health service for readiness. The port
authenticates no client, so a deployment provides encryption and access
control with a private network, a service mesh, or a sidecar.

Generate a client from `icetray.proto` with your language's usual
toolchain. The file declares the package `icetray.v1` and sets no
language package options, so a Go build maps it to an import path with
`protoc-gen-go`'s `M` option or a `buf.gen.yaml` override. The server
offers no reflection, so pass the file to a tool such as
[grpcurl](https://github.com/fullstorydev/grpcurl) with `-proto`.

## Looking up a key

A `Get` request carries the table, the key, and the value columns to
read:

```sh
grpcurl -plaintext -proto crates/icetray-proto/proto/icetray.proto \
  -d '{"table": "profiles", "key": {"long": "42"}, "columns": [{"name": "embedding"}]}' \
  127.0.0.1:7433 icetray.v1.Icetray/Get
```

```json
{
  "presence": "PRESENCE_PRESENT",
  "values": [
    {"value": "AAAAPwAAgD8…"}
  ],
  "snapshotId": "7128216514704790069"
}
```

The key has three variants, and the caller sets the one that matches the
table's key type, which `TableInfo` reports: `long` for a long key, `str` for a
string key, and `bin` for a binary, fixed, or uuid key, carrying its
bytes (a uuid as its sixteen bytes). The caller encodes a composite key
into `bin` itself.

A response states two separate facts: whether the key is present, and,
for a present key, whether each value is null. Presence is
`PRESENCE_PRESENT` or `PRESENCE_ABSENT`, and a miss is a successful call.
The response for a present key carries one value per column requested,
in the order requested, and the response for an absent key carries none.
Each value is either the column's bytes, in `value`, or a null, in
`null_value`. A zero-length value is a `value` holding no bytes, distinct
from a null. The table's owner defines what a null means.

The same call in Go, with the package generated from `icetray.proto`
imported as `icetrayv1`:

```go
resp, err := client.Get(ctx, &icetrayv1.GetRequest{
	Table:   "profiles",
	Key:     &icetrayv1.Key{Key: &icetrayv1.Key_Long{Long: 42}},
	Columns: []*icetrayv1.ColumnRead{{Name: "embedding"}},
})
if err != nil {
	return err
}
if resp.GetPresence() == icetrayv1.Presence_PRESENCE_PRESENT {
	embedding := resp.GetValues()[0]
	if embedding.GetNullValue() == nil {
		use(embedding.GetValue())
	}
}
```

## Choosing columns

`TableInfo` lists the value columns a table serves, in name order, each
with its type (`binary`, `fixed` with its width, or `string`) and its
format (`opaque`, or `protobuf` for a column whose values a request may
project to some of their fields). A request lists each column it reads,
and the server reads no column by default.

Request only the columns the caller uses. On a cold hit, each requested
column costs one GET to object storage (two for a column the writer
dictionary-encoded), and the server issues the GETs for one key
concurrently, so a hit waits on the slowest of them. A request that lists
no column returns only the key's presence and reads no value. The
table's owner decides how to split a row into columns, as
[Value columns](table-layout.md#value-columns) describes.

## Projecting protobuf fields

A column the table's owner declares `protobuf` holds serialized protobuf
messages, and a request may project its value to a list of top-level
field numbers, so the caller receives and decodes only those fields. The
server projects a value after its page cache, so projection saves bytes
on the wire and in the caller's decode, and costs the same GETs as
reading the whole value:

```json
{
  "table": "profiles",
  "key": {"long": "42"},
  "columns": [
    {"name": "properties", "fields": [1, 4, 9]},
    {"name": "embedding"}
  ]
}
```

An empty `fields` returns the whole value. An empty `columns`, one level
up, returns no value.

Two rules are the caller's to keep, since icetray holds no schema and
cannot check them:

- **List every member of a oneof, or none.** Setting one member of a
  oneof clears the others. When a value sets one member and later
  another, a projection listing only the first returns it set, with no
  error.
- **Parse partially when the message has required fields.** A projected
  value lacks every field the request did not list, including a proto2
  `required` field or a `LEGACY_REQUIRED` field under editions, so parse
  with `proto.UnmarshalOptions{AllowPartial: true}` in Go,
  `parser().parsePartialFrom` in Java, or `ParsePartialFromString` in C++.

The projected value keeps every occurrence of each listed field, in the
value's order, and parses as the caller's message type: each listed field
reads as it does in the whole value, and every other field reads as
unset, which is its zero value for a field without presence. The server
returns a value that holds none of the listed fields as a `value` of zero
bytes, and returns a null as a null.

The server accepts a number the message does not define and returns
whatever the value carries under it, which the caller's runtime keeps as
unknown fields. A value that does not parse as protobuf fails the call as
`FAILED_PRECONDITION`, as [Errors and retries](#errors-and-retries)
describes.

Take field numbers from the generated code. Java generates a constant
per field, such as `Profile.TITLE_FIELD_NUMBER`. In Go, the descriptor
reports each number:

```go
fields := (&profilepb.Profile{}).ProtoReflect().Descriptor().Fields()
title := uint32(fields.ByName("title").Number())
```

## Looking up many keys

A `MultiGet` request carries a list of keys and one list of columns,
applied to every key. The server reads every key from one snapshot and
returns the results in request order, with one snapshot id for the
batch, so the results are consistent with each other. When the lookup of
any key fails, the whole call fails, so every response covers every key.
The caller then retries the whole batch when the error's row in
[Errors and retries](#errors-and-retries) says to. On
`FAILED_PRECONDITION`, the caller drops the key the error names or reads
that column whole.

A batch may request up to `server.max_multi_get_values` values, 1,000 by
default, counting its keys times its columns, with a presence-only batch
counting one per key. The server rejects a larger batch as
`INVALID_ARGUMENT` before it looks up any key, so a caller with more
values splits them across batches. The server caps the GETs a batch holds
in flight, so a large batch takes longer and its load on object storage
stays within that cap.

## Snapshots and consistency

Every `Get` response carries the id of the snapshot the server read it
from, and every `MultiGet` response carries one for the batch. A table
advances when its writer publishes a new snapshot, and icetray swaps the
new one in at once, so two calls made a moment apart can read from
different snapshots. A caller that combines responses from several calls
and needs them consistent compares their ids, and repeats the call with
the older id when they differ. `TableInfo` reports the snapshot being
served.

## Errors and retries

Three codes are transient and worth a retry: `RESOURCE_EXHAUSTED` with
backoff, `UNAVAILABLE`, and `DEADLINE_EXCEEDED` when the caller's own
deadline allows. Every other code recurs until the request, the value,
or the snapshot changes.

| Code | Meaning | Retry |
|---|---|---|
| `NOT_FOUND` | The server does not serve a table of that name. | No |
| `INVALID_ARGUMENT` | The request is invalid: a missing key, a key variant that does not match the table's key type, a uuid key that is not sixteen bytes, a column the table does not serve or one listed twice, a field list for a column that is not protobuf, a field number outside 1 to 536,870,911 or one listed twice, or a batch over the values limit. | No; fix the request |
| `FAILED_PRECONDITION` | A request listed fields of a value that is not protobuf. The error names the table, column, and key. | No; it recurs until the writer rewrites the value. Read that column whole, or drop the key. |
| `RESOURCE_EXHAUSTED` | The server is at `server.max_in_flight` calls (1,024 by default) and refused this one at once. | Yes, with backoff |
| `UNAVAILABLE` | A read from object storage or local disk failed, the table is not ready yet, or a lookup was canceled. | Yes |
| `DEADLINE_EXCEEDED` | A `Get`, or a page fetch within any lookup, ran past `server.get_deadline` (5 seconds by default), or a `MultiGet` ran past `server.multi_get_deadline` (15 seconds by default). | Yes, if the caller's own deadline allows |
| `INTERNAL` | The table's data is faulty, such as a malformed file. | Only after the table advances to another snapshot |

The gRPC layer enforces a deadline the client sends and reports its
expiry to the client (a tonic client sees `CANCELLED`), and the server
drops the call at that moment.

By default the server hedges a slow GET to object storage with a second
one to cut tail latency, so a caller can leave hedging a `Get` to the
server.
