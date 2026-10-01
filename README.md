# icetray

Batch-frozen data, served one cube at a time.

icetray serves key lookups from Apache Iceberg tables on object storage. A
batch job writes a table and publishes it by moving an Iceberg ref; icetray
follows the ref and answers `Get` and `MultiGet` over gRPC from the Parquet
files where they sit.

It is built for data that is computed in batch and read by key, such as
embeddings and model features. Tables with a hot working set and a long,
rarely read tail suit it best: hot pages come from memory, and a cold page
costs one range GET.

## How it works

- On load, icetray reads each data file's footer and key column once and
  keeps the keys resident in memory, in a local file, or as Bloom filters,
  chosen per table.
- A lookup finds its bucket with the Iceberg bucket transform, its file by
  key range, and its page by binary search.
- A hit reads one value page, from the page cache or with one range GET.
- When the ref moves, icetray loads the new snapshot beside the current one,
  reuses the index of every unchanged file, verifies it, and swaps it in
  atomically. Each answer carries the id of the snapshot it came from.
- `icetray doctor` checks a table's files against the
  [table contract](docs/table-contract.md) and reports what each deviation
  costs per lookup and which writer setting fixes it. The server runs the
  same check on every load.

## Performance

| | |
|---|---|
| Warm hit, in-process | 12.8 µs |
| Warm hits, in-process, eleven tasks | 497,000 a second |
| Cold hit, S3 in-region, median | 81 ms, 0.5 ms of it in icetray |
| First load, 1.2 billion rows in 32,768 files, four cores | 79 s |

Methods and machines are in the
[performance sweep](docs/analysis/2026-09-26-performance-sweep.md), the
[cold range-GET analysis](docs/analysis/2026-09-29-cold-range-get-latency.md),
and [RFD 013](docs/rfd/013-tail-latency.md).

## Quick start

Build:

```sh
cargo build --release
```

Write a table to the contract. In Spark SQL:

```sql
CREATE TABLE db.kv (key BIGINT, value BINARY) USING iceberg
PARTITIONED BY (bucket(16, key))
TBLPROPERTIES (
  'parquet.enable.dictionary' = 'false',
  'parquet.columnindex.truncate.length' = '256',
  'write.parquet.page-row-limit' = '1000',
  'write.parquet.page-size-bytes' = '8388608',
  'write.parquet.row-group-size-bytes' = '134217728',
  'write.parquet.compression-codec' = 'zstd',
  'write.parquet.bloom-filter-enabled.column.key' = 'true',
  'write.parquet.bloom-filter-fpp.column.key' = '0.01',
  'write.metadata.metrics.column.key' = 'full',
  'write.distribution-mode' = 'hash',
  'write.target-file-size-bytes' = '67108864'
);
ALTER TABLE db.kv WRITE LOCALLY ORDERED BY key;
```

Check it. The doctor exits 0 for a table that serves at full speed, 2 for
one that serves at a penalty, and 1 for one it refuses:

```sh
icetray doctor --location s3://warehouse/db/kv
```

Serve it with a configuration file:

```toml
[server]
listen = "0.0.0.0:7433"
metrics_listen = "0.0.0.0:9433"

[[tables]]
name = "kv"
ref = "main"
key_column = "key"
value_column = "value"

[tables.source]
type = "static"
location = "s3://warehouse/db/kv"
```

```sh
icetray serve --config icetray.toml
```

Look a key up with any client generated from
[`icetray.proto`](crates/icetray-proto/proto/icetray.proto):

```sh
grpcurl -plaintext -proto crates/icetray-proto/proto/icetray.proto \
  -d '{"table": "kv", "key": {"long": "42"}}' \
  127.0.0.1:7433 icetray.v1.Icetray/Get
```

or from the command line, which loads the table itself:

```sh
icetray get --location s3://warehouse/db/kv 42
```

## Deploying

- Configuration is one TOML or JSON file, from `--config` or
  `ICETRAY_CONFIG`. Changes to its tables apply on the next poll or on
  SIGHUP.
- Credentials come from the AWS SDK's default chain.
- Logs are JSON on stdout; metrics are Prometheus on the metrics port, with
  `/ready` and gRPC health for the orchestrator.
- The gRPC port speaks plaintext; put it on a private network or behind a
  service mesh.
- Size the memory limit from the doctor's predicted bytes for each table's
  residency tier.
- Mount a local volume for the local-file tier and the page cache's disk
  level.

## Documentation

- [The table contract](docs/table-contract.md)
- [Configuration and the command line](docs/configuration.md)
- [Operating icetray](docs/operating.md)
- [Building and testing](docs/development.md)
- [Design decisions](docs/rfd/) and the [measurements](docs/analysis/)
  behind them
- [Contributing](CONTRIBUTING.md)

## Prior art

[ElephantDB](https://github.com/nathanmarz/elephantdb) established building
a dataset in batch and serving it read-only. icetray applies that shape to
Iceberg tables.

## License

MIT or Apache-2.0, at your option.
