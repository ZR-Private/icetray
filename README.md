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
ref; icetray follows the ref and answers `Get` and `MultiGet` over gRPC
from the Parquet files where they sit.

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

## Try it locally

This path needs a Rust toolchain and, to write the tables, either a
container runtime or Spark. It runs on a laptop and touches no cloud
account.

Build. `rust-toolchain.toml` pins the toolchain, which rustup installs on
the first `cargo` run, and the build carries its own protobuf compiler:

```sh
cargo build --release
export PATH="$PWD/target/release:$PATH"
```

Write a few small contract-shaped tables with the golden-table recipe,
[`dev/golden-tables.py`](dev/golden-tables.py). The wrapper script runs it
with Spark 4.1.3 and Java 21 in the `apache/spark` container image, so the
host needs only a Docker-compatible runtime and `python3`. On macOS,
`dev/runtime.sh up` starts one with colima; on Linux, start your
distribution's Docker daemon.

```sh
dev/runtime.sh up
dev/golden-tables.sh --only contract
```

Without a container runtime, run the recipe with PySpark 4.1 and Java 17
or later on the host:

```sh
pip install 'pyspark==4.1.*'
python3 dev/golden-tables.py --only contract \
  --warehouse "file://$PWD/target/golden-warehouse"
```

Either way the first run downloads the Iceberg Spark runtime from Maven
Central. `--only contract` writes five tables, about 2.2 GB in all, most of
it one table of 16 KiB values, under `target/golden-warehouse/db/`. Their
keys are multiples of three, so 42 is present and 43 is absent.

Look keys up. `get` opens the table at a path, loads it, and prints one
JSON line per key (the value fields are shortened here):

```sh
icetray get --location target/golden-warehouse/db/long_2m 42 43
```

```json
{"kind":"lookup","table":"long_2m","key":"42","presence":"present","value_base64":"NzM0NzVj…","value_utf8":"73475cb4…","value_bytes":192,"snapshot_id":"7128216514704790069","gets":1,"elapsed_micros":342}
{"kind":"lookup","table":"long_2m","key":"43","presence":"absent","value_bytes":0,"snapshot_id":"7128216514704790069","gets":0,"elapsed_micros":8}
```

The hit cost one GET and the miss none. Check the table against the
[table contract](docs/table-contract.md):

```sh
icetray doctor --location target/golden-warehouse/db/long_2m
```

```json
{"kind":"table","table":"long_2m","snapshot_id":"7128216514704790069","serviceability":"efficient","residency":"full","file_count":4,"buckets":4,"buckets_with_files":4,"max_files_per_bucket":1,"records":2000000,"index_bytes":2623999,"predicted":{"full":2623999,"disk_memory":239082,"disk_bytes":2405869,"bloom":4433458,"bloom_bits_per_key":16.777504,"key_chunk_bytes":2384917,"filter_bytes":4194376},"inefficient":0,"advice":0}
```

It exits 0, with no finding. `predicted` is the memory and disk each
[residency](docs/configuration.md#choosing-a-residency) would need.

Serve it. A configuration file's locations are absolute, so write the
file with the full path:

```sh
cat > target/icetray.toml <<EOF
[[tables]]
name = "long_2m"
ref = "main"
key_column = "key"
value_column = "value"

[tables.source]
type = "static"
location = "$PWD/target/golden-warehouse/db/long_2m"
EOF
icetray serve --config target/icetray.toml
```

The server logs JSON to stdout, listens for gRPC on `127.0.0.1:7433`, and
answers `/ready` and `/metrics` on `127.0.0.1:9433`. From another
terminal in the repository, with
[grpcurl](https://github.com/fullstorydev/grpcurl):

```sh
grpcurl -plaintext -proto crates/icetray-proto/proto/icetray.proto \
  -d '{"table": "long_2m", "key": {"long": "42"}}' \
  127.0.0.1:7433 icetray.v1.Icetray/Get
```

```json
{
  "presence": "PRESENCE_PRESENT",
  "value": "NzM0NzVj…",
  "snapshotId": "7128216514704790069"
}
```

Ctrl-C drains the server and exits 0.

## Serve your own table

Write the table to the contract. In Spark SQL:

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

Check it:

```sh
icetray doctor --location s3://warehouse/db/kv
```

The doctor exits 0 for a table that serves efficiently, 2 for one that
serves [inefficiently](docs/glossary.md), and 1 for one it refuses. A
table in a configuration file has `strict` on unless the file turns it
off, and `serve` refuses a strict table that is inefficient. `icetray
doctor --config` judges each table the same way, so it exits 1 for such a
table.

Serve it with a configuration file. The default listeners are loopback,
so a container's file sets them to all interfaces:

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
[`icetray.proto`](crates/icetray-proto/proto/icetray.proto), or from the
command line, which loads the table itself:

```sh
icetray get --location s3://warehouse/db/kv 42
```

## Not a fit if

- **Your tables are written merge-on-read.** A snapshot with delete files
  or deletion vectors is refused. Copy-on-write tables serve, and a
  merge-on-read table can be served from a tag or branch that a compaction
  moves.
- **A key needs more than one value column.** icetray serves one value
  column per table; several fields are packed into one binary value at
  write time.
- **You need writes, scans, or range queries.** icetray is read-only and
  answers lookups by exact key.
- **The server itself must speak TLS or authenticate callers.** The gRPC
  port is plaintext and unauthenticated; encryption and access control
  belong to a mesh, a sidecar, or a private network.
- **Your REST catalog needs credentials.** The `rest` source sends none,
  so it serves catalogs that need none or that sit behind a proxy that
  adds them. Catalog-vended storage credentials are not supported.
- **You need a release binary for macOS or Windows.** Releases are Linux
  binaries for x86-64 and arm64 and a container image; other systems build
  from source.

## Deploying

- Each release publishes Linux binaries for x86-64 and arm64 and a
  container image for both to the GitHub Container Registry.
- Configuration is one TOML or JSON file, from `--config` or
  `ICETRAY_CONFIG`. Changes to its tables apply on the next poll or on
  SIGHUP.
- S3 credentials come from the AWS SDK's default chain, or from static
  keys in the file for S3-compatible stores. A REST catalog is called
  without credentials.
- Logs are JSON on stdout; metrics are Prometheus on the metrics port, with
  `/ready` and gRPC health for the orchestrator.
- The gRPC port speaks plaintext; put it on a private network or behind a
  service mesh.
- Size the memory limit from the doctor's predicted bytes for each table's
  residency.
- Mount a local volume for `disk` residency and the page cache's disk
  level.

[Operating icetray](docs/operating.md) covers sizing, Kubernetes, and what
to alert on.

## Documentation

- [The table contract](docs/table-contract.md)
- [Configuration and the command line](docs/configuration.md)
- [Operating icetray](docs/operating.md)
- [Glossary](docs/glossary.md)
- [Building and testing](docs/development.md)
- [Design decisions](docs/rfd/README.md) and the
  [measurements](docs/analysis/) behind them
- [Contributing](CONTRIBUTING.md)

## Prior art

[ElephantDB](https://github.com/nathanmarz/elephantdb) established building
a dataset in batch and serving it read-only. icetray applies that shape to
Iceberg tables.

## License

MIT or Apache-2.0, at your option.
