# Calling icetray

A service calls icetray over gRPC with a client generated from
[`icetray.proto`](../crates/icetray-proto/proto/icetray.proto), whose
comments are the reference this page summarizes. There are five calls.
`Get` looks up one key and `MultiGet` many keys from one snapshot, in a
key-value table. `Scan` reads one key's rows in order and `MultiScan`
many keys' rows from one snapshot, in an [ordered
table](#reading-an-ordered-table). `TableInfo` describes a table the
server serves, and a table whose `order_column` it reports set is an
ordered table. The [glossary](glossary.md) defines terms such as
snapshot and residency.

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
in flight at `server.batch_fan_out`, 64 by default, so a large batch
takes longer and its load on object storage stays within that cap.

## Reading an ordered table

An ordered table holds many rows per key, each with an order value, such
as a time series by key and timestamp or the activity of each user. The
table's owner configures its order column, and `TableInfo` reports it in
`order_column`, its type in `order_type`, the most rows one `Scan`
returns in `max_scan_rows`, and the most values one `MultiScan` may ask
for in `max_multi_scan_values`. A key-value table reports an empty
`order_column` and a `max_scan_rows` and `max_multi_scan_values` of 0. The
server rejects a `Get` or `MultiGet` of an ordered table, and a `Scan` or
`MultiScan` of a key-value table, as `INVALID_ARGUMENT`. It decides this
from the table's configuration before anything else about the call, so it
answers `INVALID_ARGUMENT` even while the table is still loading, where a
call of the right kind meets `UNAVAILABLE` until the load completes; a
caller does not retry it.

A `Scan` request carries the table, the key, the value columns to read
as `Get` reads them, a direction, a limit, and optional bounds:

```sh
grpcurl -plaintext -proto crates/icetray-proto/proto/icetray.proto \
  -d '{"table": "events", "key": {"long": "42"}, "columns": [{"name": "payload"}], "direction": "DIRECTION_NEWEST_FIRST", "limit": 2}' \
  127.0.0.1:7433 icetray.v1.Icetray/Scan
```

```json
{
  "rows": [
    {"order": {"long": "1760400060000000"}, "values": [{"value": "CgVjbGljaw…"}]},
    {"order": {"long": "1760400000000000"}, "values": [{"value": "CgR2aWV3…"}]}
  ],
  "more": true,
  "snapshotId": "7128216514704790069"
}
```

- **Direction.** `DIRECTION_NEWEST_FIRST` returns the highest order
  values first, `DIRECTION_OLDEST_FIRST` the lowest. The request must set
  one.
- **Limit.** The most rows to return, at most the table's
  `max_scan_rows`. A limit of 0 reads up to `max_scan_rows`. The response
  must also fit `server.max_scan_bytes` ([Response size](#response-size)),
  so a caller whose values are large sets a limit that fits.
- **Bounds.** `after` and `before` are exclusive: a row is returned only
  when its order value is above `after` and below `before`. Either may
  be left unset, and when both are set `after` must be below `before`.
- **Order values.** An order value is an `OrderValue` whose variant
  follows the order type: `long` for `int`, `long`, `date`, and the
  timestamp types, `str` for `string`, and `bin` for `binary` and
  `fixed`. A date travels as days since the epoch, a `timestamp` or
  `timestamptz` as microseconds, and a `timestamp_ns` or
  `timestamptz_ns` as nanoseconds. Integer and time values compare as
  signed 64-bit integers, and strings and bytes as their bytes. A bound
  outside an `int` or `date` column's range admits every row on its side.

The response carries the key's rows in the requested direction, each
with its order value and one value per requested column, in the order
requested, read as a `Get` response's values are. No two rows of one
response share an order value. `more` is set when the key has rows
within the bounds past the last row returned. For a key with no rows,
the server returns no rows and leaves `more` unset, and that is a
successful call. A request that lists no columns returns the order
values alone.

The same call in Go:

```go
resp, err := client.Scan(ctx, &icetrayv1.ScanRequest{
	Table:     "events",
	Key:       &icetrayv1.Key{Key: &icetrayv1.Key_Long{Long: 42}},
	Columns:   []*icetrayv1.ColumnRead{{Name: "payload"}},
	Direction: icetrayv1.Direction_DIRECTION_NEWEST_FIRST,
	Limit:     100,
})
```

### Response size

The server refuses to send a `Scan` or `MultiScan` response whose encoded
size is above `server.max_scan_bytes`, 4 MiB (4,194,304 bytes) by default,
a gRPC client's default limit on a message it receives. The call fails
as `FAILED_PRECONDITION`, with no rows, and its message names the
response's size, the bound, and how many rows of a `Scan`, or how many
leading keys' results of a `MultiScan`, would have fit. The size is the
exact encoded size of the response message, which is what a client's
message limit measures.

Nothing about the same request changes on a retry, so the caller lowers
its `limit` or requests fewer columns, and sets `limit` explicitly: a
limit of 0 reads up to `max_scan_rows`, which for large values may not
fit. A caller that sets a limit of 100 and pages by order value (below)
reads the same history in pages of at most 100 rows, each of which fits when 100 rows of the requested columns come to less than the bound. An owner whose clients
accept larger messages raises `server.max_scan_bytes`
([Configuration](configuration.md#server)) and the clients raise their
own receive limit to match, as grpc-go's `MaxCallRecvMsgSize` does; the
doctor's `large_runs` advice tells the owner which tables would be
refused at the full limit.

In each file that holds the key, a cold `Scan` costs a GET per
contiguous span of uncached pages of each requested column, and under
`full` and `bloom` of the order column, which `disk` reads locally;
under `bloom` the key-page GETs come first. A caller therefore requests
only the columns it uses. A compaction that writes one file per bucket
holds each key in one file; one that rolls files by size can split a key
at a file boundary (`shared_edge_keys`), and a `Scan` of that key reads
every file that holds it. A table that takes appends between compactions
holds a key in up to one file per append, and a `Scan` reads every one
of them ([Performance and tuning](performance.md#what-a-scan-costs)).

## Paging by order value

A caller reads a history longer than one response by paging on the order
value. Reading newest first, the next request sets `before` to the order
value of the last row of the previous response; reading oldest first, it
sets `after` to it. Every other field stays the same. The caller stops
when a response has `more` unset:

```go
req := &icetrayv1.ScanRequest{
	Table:     "events",
	Key:       &icetrayv1.Key{Key: &icetrayv1.Key_Long{Long: 42}},
	Columns:   []*icetrayv1.ColumnRead{{Name: "payload"}},
	Direction: icetrayv1.Direction_DIRECTION_NEWEST_FIRST,
	Limit:     100,
}
for {
	resp, err := client.Scan(ctx, req)
	if err != nil {
		return err
	}
	rows := resp.GetRows()
	for _, row := range rows {
		use(row.GetOrder().GetLong(), row.GetValues()[0])
	}
	if !resp.GetMore() {
		return nil
	}
	req.Before = rows[len(rows)-1].GetOrder()
}
```

A key and order value appear together at most once among a snapshot's
live rows, so within one snapshot the order value is an exact cursor: the
next page starts at the next row, with no row repeated or skipped, and
the server keeps no state between requests. A refresh between two
requests moves the next page to the new snapshot, whose id the response
carries. A row the writer committed on the side the caller has already
read appears only to a `Scan` that starts over, so a caller that needs
every row of one snapshot compares the ids and starts over when they
differ.

## Scanning many keys

A `MultiScan` request carries a list of keys and one set of columns,
direction, limit, and bounds, applied to every key. The server reads
every key from one snapshot and returns one result per key in request
order, each with its own rows and `more`, with one snapshot id for the
call. For a key listed twice, the server returns a result at each
position. When the read of any key fails, the whole call fails, as for
`MultiGet`.

A `MultiScan` may request up to `server.max_multi_scan_values` values,
4,000 by default, which `TableInfo` reports in `max_multi_scan_values`.
They are counted as its keys times its limit times one more
than its columns, the one being the order value; a limit of 0 counts as
the table's `max_scan_rows`, so a `MultiScan` that leaves `limit` unset
asks for the most each key can return and is refused for more than a few
keys. The server rejects a larger call as
`INVALID_ARGUMENT` before it reads any key. At the defaults, a
`MultiScan` at the full limit of 1,000 rows with one column covers two
keys, and one with a limit of 20 and one column covers 100. A caller
reading many keys requests a smaller limit and pages each key whose
result has `more` set, and a caller reading one key's full limit over
several columns uses `Scan`. The GETs one `Scan` or `MultiScan` holds in
flight are capped at `server.batch_fan_out`, 64 by default, as for
`MultiGet`.

The whole `MultiScan` response is held to `server.max_scan_bytes`
([Response size](#response-size)): one result of many keys that together
come to more is `FAILED_PRECONDITION` with no results, and the message
names how many leading keys' results would have fit, a key listed twice
counting at each position. The caller requests fewer keys or a smaller
limit.

## Snapshots and consistency

Every `Get` and `Scan` response carries the id of the snapshot the server
read it from, and every `MultiGet` and `MultiScan` response carries one
for the call. A table
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
or the snapshot changes. `RESOURCE_EXHAUSTED` is the server's overload
alone: a response too large is never sent, but refused as
`FAILED_PRECONDITION`. A `RESOURCE_EXHAUSTED` that a client library
raises on its own for a message above its receive limit is not the server's
and not worth a retry; the client raises its limit, or the caller lowers
its `limit`.

| Code | Meaning | Retry |
|---|---|---|
| `NOT_FOUND` | The server does not serve a table of that name. | No |
| `INVALID_ARGUMENT` | The request is invalid: a missing key, a key variant that does not match the table's key type, a uuid key that is not sixteen bytes, a column the table does not serve or one listed twice, a field list for a column that is not protobuf, a field number outside 1 to 536,870,911 or one listed twice, a `MultiGet` or `MultiScan` over its values limit, a `Scan` or `MultiScan` that sets no direction, asks for a limit above the table's `max_scan_rows`, or sets a bound of the wrong variant, a bound with no variant, or an `after` at or above `before`, or a call of the wrong kind for the table (`Get` or `MultiGet` of an ordered table, `Scan` or `MultiScan` of a key-value table). | No; fix the request |
| `FAILED_PRECONDITION` | A request listed fields of a value that is not protobuf. The error names the table, column, and key, and for a `Scan` the order value of the row that holds the value, which a caller paging the key's rows steps past. Or a `Scan` or `MultiScan` response would encode to more than `server.max_scan_bytes` (4 MiB by default); the error names the size, the bound, and the rows (`Scan`) or leading keys' results (`MultiScan`) that would fit, and the server sends no rows. | No; it recurs until the writer rewrites the value. Read that column whole, or drop the key. For a response size, lower `limit` or request fewer columns or keys. |
| `RESOURCE_EXHAUSTED` | The server is at `server.max_in_flight` calls (1,024 by default) and refused this one at once. The server sends this for no other reason. | Yes, with backoff |
| `UNAVAILABLE` | A read from object storage or local disk failed, the table is not ready yet, or a lookup was canceled. | Yes |
| `DEADLINE_EXCEEDED` | A `Get`, or a page fetch within any lookup, ran past `server.get_deadline` (5 seconds by default), a `MultiGet` ran past `server.multi_get_deadline` (15 seconds by default), a `Scan`, or a span GET within any `Scan` or `MultiScan`, ran past `server.scan_deadline` (5 seconds by default), or a `MultiScan` ran past `server.multi_scan_deadline` (15 seconds by default). | Yes, if the caller's own deadline allows |
| `INTERNAL` | The table's data is faulty, such as a malformed file. | Only after the table advances to another snapshot |

The gRPC layer enforces a deadline the client sends and reports its
expiry to the client (a tonic client sees `CANCELLED`), and the server
drops the call at that moment.

By default the server hedges a slow GET to object storage with a second
one to cut tail latency, so a caller can leave hedging a `Get` or a
`Scan` to the server.
