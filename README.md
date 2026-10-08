# icetray

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/images/icetray-dark.svg">
    <img src="docs/images/icetray.svg" width="480" alt="Cubes break off an iceberg and fall into an ice tray">
  </picture>
</p>

Batch-frozen data, served one cube at a time.

icetray is a Rust server for key lookups on Apache Iceberg tables in object
storage. A batch job writes a table and publishes it by moving an Iceberg
ref, and icetray serves `Get` and `MultiGet` over gRPC from the table's
Parquet files.

It is built for data that is computed in batch and read by key. Tables
with a hot working set and a long, rarely read tail suit it best: hot
pages are served from icetray's page cache, and a cold page costs one
range GET.

## Why icetray

### Tables written in batches and read by key are a common pattern

Many large datasets are read by key, at random, and change only in
batches: a job rebuilds them, or writes to them and compacts. The
properties a service computes for its users, such as model scores,
embeddings, and features, are a common case. A service with millions of
users sees a small, active head, but any user in the long tail can come
back at any moment. Teams usually compute those properties in batch,
write them to the data lake, and then load them into an online key-value
store to serve them. Those stores are built for writes, which makes them
expensive.

icetray serves those tables directly from the lake, and the residency
configured for each table sets how much memory it trades for GETs and
latency.

### Multiple modes support trading memory for GETs and latency

Each table is configured with a residency, which sets where its key
chunks are stored: `full` keeps them in memory, `disk` in local files, and
`bloom` keeps only the writer's Bloom filters in memory and reads key
pages from object storage. Keeping the key chunks in memory gives the
lowest latency; local disk adds a local read to each lookup, and `bloom`
a GET to each cold hit. Each table is also configured with a lookup
level, the cost per lookup its owner accepts, which icetray checks on
every snapshot. A team can then weigh memory against GETs and latency,
and all three against the effort of writing the table to the optimal
layout.

### Built for batch-generated Iceberg tables, including merge-on-read updates between rebuilds

A writer publishes a new version of a table by committing a snapshot and
moving the ref icetray follows, a branch or tag. icetray loads the
snapshot on its next poll. A table can also change between rebuilds: a
writer that updates rows by merge-on-read leaves deletion vectors or
Parquet position deletes beside the data files, and icetray applies them
on every lookup until a compaction rewrites them into the data files.

## How it works

### A table written to the optimal layout costs one GET per hit and none per miss

icetray's optimal layout is the Iceberg and Parquet layout that makes a
point lookup cheap: `bucket(N, key)` partitioning, files sorted by key,
1,000-row pages, dictionary encoding off on the value column, and a Bloom
filter on the key. A standard Iceberg writer produces it from table
properties and a local sort order, as in this Spark SQL excerpt:

```sql
CREATE TABLE db.kv (key BIGINT, value BINARY) USING iceberg
PARTITIONED BY (bucket(16, key))
TBLPROPERTIES (
  'parquet.enable.dictionary' = 'false',
  'write.parquet.page-row-limit' = '1000',
  'write.parquet.bloom-filter-enabled.column.key' = 'true',
  'write.target-file-size-bytes' = '67108864'
  -- and the rest of the recipe
);
ALTER TABLE db.kv WRITE LOCALLY ORDERED BY key;
```

A lookup finds its bucket with the Iceberg bucket transform, its file by
key range, and its row by binary search over the key chunks, so a miss
costs no GET under `full` and `disk`. icetray serves tables in other
layouts less efficiently, and refuses a layout it would serve incorrectly,
such as unsorted files. To check a table against the optimal layout, run `icetray doctor`. It reads
the table's metadata and data files and reports each deviation, its cost
per lookup, and the writer property that fixes it. The server runs the
same check on every load. [The optimal layout](docs/table-layout.md) has
the full recipe and every check.

### A lookup reads only the value columns it requests

A table can serve several value columns, and each request lists the
columns the caller reads, so icetray serves callers with different access
patterns from one wide table: one reads the embedding column, another the properties column.
On a cold hit, each requested column costs one range GET, issued
concurrently, and a lookup that requests no column returns only the key's
presence. A table read by one kind of caller is fastest with the whole
row serialized into one value column.
[Value columns](docs/table-layout.md#value-columns) covers when to split
a row into columns.

### A protobuf column can be projected to the requested fields

A `binary` column that holds serialized protobuf messages can be declared
`format = "protobuf"`. A lookup can then project its value to a list of
top-level field numbers, so the caller receives and decodes only those
fields. Projection runs after the page cache, so it reduces the bytes on
the wire and the caller's decode time, and costs the same GETs as reading
the whole value, as in this `Get` request:

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

[Calling icetray](docs/calling.md) covers requests, responses, and
errors, including the rules for projecting a oneof or a message with
required fields.

### Every response comes from one published snapshot

When a writer moves the ref, icetray loads the new snapshot alongside the
serving one, reuses the index of every unchanged data file, verifies the
new files, and swaps the snapshot in atomically. Every response carries
the id of the snapshot it was read from, and a `MultiGet` reads all of
its keys from one snapshot. icetray refuses a snapshot it would serve
incorrectly, such as one with unsorted files, duplicate keys, or equality
deletes, and keeps serving the last good snapshot.
[Refresh and reload](docs/operating.md#refresh-and-reload) covers the
snapshot lifecycle and the configuration changes a running server applies
without a restart.

### Residency and lookup level are configured per table

Where the key chunks are stored sets each lookup's memory and GETs:

| Residency | Resident memory per key | Cold hit | Miss |
|---|---|---|---|
| `full` | about a byte for sequential keys, 8 for random integers, 14 to 18 for UUIDs | 1 GET | none |
| `disk` | a fraction of a byte, with the key chunks on local disk | 1 GET and 1 local read | 1 local read |
| `bloom` | the writer's Bloom filter | 2 GETs | none, or 1 GET for a false positive |

A hit served from the page cache takes microseconds, and a cold hit's GET
to object storage takes tens of milliseconds. Resident memory is
proportional to the key count and independent of value size.

`lookups` takes one of three levels: `optimal`, one GET per requested
column on a hit and none on a miss; `balanced`, a fixed extra cost per
lookup, such as `bloom`'s second GET; or `permissive`, any layout icetray
serves correctly, including one where a lookup probes several files. When
a snapshot's layout falls below the table's level, the
`icetray_table_below_level` gauge reads 1, and the load logs each finding
that caps the level. With `strict = true`, icetray refuses such a
snapshot and keeps serving the last one that met the level.
`icetray doctor` predicts each residency's resident memory and level from
a table's metadata and file footers, before deployment.
[Choosing a residency](docs/configuration.md#choosing-a-residency),
[Choosing a level](docs/configuration.md#choosing-a-level), and
[Performance and tuning](docs/performance.md) go deeper.

## Limits and workarounds

icetray serves exact-key lookups on tables a batch job writes, so writes,
scans, and range queries need another tool. Each limit below has a
workaround at write time or in the platform:

- **Equality deletes.** icetray serves deletion vectors and Parquet
  position deletes, and refuses a snapshot with equality deletes. For a
  writer that uses them, serve a tag or branch that a compaction job
  moves after it rewrites the equality deletes into data files.
- **Typed values.** Each value column is served as bytes, of Iceberg type
  `binary`, `fixed`, or `string`. Serialize a number or a struct into a
  binary column at write time; a protobuf column can be projected to its
  top-level fields.
- **Encryption and access control.** The gRPC port is plaintext and
  authenticates no client. Run it on a private network, behind a service
  mesh, or with a sidecar that terminates TLS and enforces access policy.
- **REST catalogs that need credentials.** The `rest` source sends no
  credentials, so it reaches catalogs that need none, or a proxy that
  adds them. Tables in AWS Glue and S3 Tables have their own sources,
  signed with the S3 credentials.

## Try it locally

You need a Rust toolchain, and a container runtime or Spark to write the
test table. This example runs locally and reads the table from local
disk.

Build the binary. `rust-toolchain.toml` pins the toolchain, which rustup
installs on the first `cargo` run:

```sh
cargo build --release
export PATH="$PWD/target/release:$PATH"
```

Write a test table of 2 million long keys with the repository's
golden-table script, which runs Spark in the `apache/spark` container
image:

```sh
dev/golden-tables.sh --only long_2m
```

[Building and testing](docs/development.md#the-development-environment)
covers the container runtime and how to run the script with PySpark
instead.

The table's keys are multiples of three, so 42 is present and 43 is
absent. Look
up both keys. `--value-column` configures the column the table serves,
and `--column` requests it for each key. The values below are shortened,
and your snapshot ids and timings will differ:

```sh
icetray get --location target/golden-warehouse/db/long_2m --value-column value --column value 42 43
```

```json
{"kind":"lookup","table":"long_2m","key":"42","presence":"present","values":{"value":{"base64":"NzM0NzVj…","utf8":"73475cb4…","bytes":192}},"snapshot_id":"7128216514704790069","gets":1,"elapsed_micros":342}
{"kind":"lookup","table":"long_2m","key":"43","presence":"absent","snapshot_id":"7128216514704790069","gets":0,"elapsed_micros":8}
```

The hit cost one GET and the miss none. Running `icetray doctor` with the
same `--location` and `--value-column` reports the table at the `optimal`
level.

To serve the table, save this configuration as `icetray.toml`, replacing
`/path/to/icetray` with the absolute path of your checkout, since a
static location must be absolute:

```toml
[[tables]]
name = "long_2m"
ref = "main"
key_column = "key"
value_columns = ["value"]

[tables.source]
type = "static"
location = "/path/to/icetray/target/golden-warehouse/db/long_2m"
```

```sh
icetray serve --config icetray.toml
```

The server listens for gRPC on `127.0.0.1:7433` and serves `/ready` and
`/metrics` on `127.0.0.1:9433`. From another terminal in the repository,
look a key up with [grpcurl](https://github.com/fullstorydev/grpcurl):

```sh
grpcurl -plaintext -proto crates/icetray-proto/proto/icetray.proto \
  -d '{"table": "long_2m", "key": {"long": "42"}, "columns": [{"name": "value"}]}' \
  127.0.0.1:7433 icetray.v1.Icetray/Get
```

Ctrl-C drains in-flight requests and stops the server.

## Deploying

Each release publishes Linux binaries and a multi-arch container image to
the GitHub Container Registry. icetray reads its configuration from one
TOML or JSON file, takes S3 credentials from the AWS SDK's default chain,
logs JSON to stdout, and serves Prometheus metrics, `/ready`, and gRPC
health. Size a replica's memory limit from the doctor's predicted
resident bytes, and put the local directory for `disk` residency on
instance-store NVMe. Before 1.0.0, any release may change
configuration keys, metric and log field names, the doctor's output
contract, protobuf fields and their numbers, or on-disk formats ([Stability](CONTRIBUTING.md#stability)).
[Operating icetray](docs/operating.md) covers sizing, Kubernetes, and what
to alert on.

## Documentation

- **Calling icetray:** [requests, responses, and errors](docs/calling.md).
- **Writing tables:** [the optimal layout](docs/table-layout.md).
- **Running icetray:** [operating](docs/operating.md),
  [performance and tuning](docs/performance.md), and
  [configuration and the command line](docs/configuration.md).
- **Terms:** [the glossary](docs/glossary.md).
- **Contributing:** [building and testing](docs/development.md),
  [contributing](CONTRIBUTING.md), [design decisions](docs/rfd/README.md),
  and the [measurements](docs/analysis/) behind them.

## Prior art

[ElephantDB](https://github.com/nathanmarz/elephantdb) established building
a dataset in batch and serving it read-only. icetray applies that shape to
Iceberg tables.

## License

MIT or Apache-2.0, at your option.
