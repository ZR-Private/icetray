# icetray

Batch-frozen data, served one cube at a time.

icetray is a read-only get-by-key server over Apache Iceberg tables on
object storage. A batch job writes a table in the layout described below
and publishes each build by moving an Iceberg ref; icetray follows the ref,
reads the Parquet data files where they are, and answers lookups over gRPC.
There is no bulk load into a database, no write path, and no compaction of
its own. The data files under a snapshot are immutable, so nothing icetray
caches ever has to be invalidated.

The cost of a lookup is fixed by the layout. Each data file's key column
stays resident (in memory, in a local file, or as bloom filters, a choice
the table owner makes per table), so a key is routed to its bucket by the
Iceberg bucket transform, to its file by exact key range, and to its page by
a binary search, without touching object storage. A cold hit then costs one
range GET, for the value page; a miss costs none. Every answer carries the
snapshot id it came from. When the ref moves, icetray builds the new
snapshot as a generation beside the one serving, reusing the index of every
data file whose path is unchanged, verifies it, and swaps atomically; a
snapshot that cannot be served leaves the last good generation in place.

Because the reader assumes nothing about how a table was written, the same
binary carries a doctor that reports whether a table can be served, what
each deviation from the layout costs per lookup, and which writer property
fixes it. The server runs the same check at every load, so the verdict a
build pipeline gates on is the verdict the server acts on.

## The table contract

The writer decides what a lookup costs, so the contract is a description of
written files, verified on the files and never inferred from the properties
that were set. A table that meets it has:

- One key column of Iceberg type `long`, `string`, `binary`, or `uuid`
  (`fixed` is served as binary). A composite key is the caller's encoding
  into that column; icetray moves the bytes unchanged. Keys over 4,096
  bytes are refused; keys over 128 bytes draw advice, because resident
  bytes per key grow with key length.
- One value column of type `binary`, `fixed`, or `string`, nulls allowed.
  The value's bytes are served unchanged, and a null value is answered as
  present with no value, distinct from an absent key. What a null means is
  the table owner's rule. A table with more than one column of interest
  packs them into one binary value at write time; a second column would be
  a second GET per hit.
- `bucket(N, key)` partitioning with hash distribution, so a lookup routes
  to one bucket by arithmetic.
- Each file sorted by key as a local write order. No global sort is needed,
  and an unsorted file cannot be served.
- Dictionary encoding off. A dictionary-encoded value column costs one
  extra GET per hit.
- Pages of 1,000 rows, so a lookup decodes one key page and fetches one
  value page.
- Value pages bounded by bytes (64 KiB) for large values, with the writer's
  page-size check interval shortened so the bound binds. A row limit alone
  leaves value pages of many megabytes.
- Full-length manifest metrics and column index on the key. Truncated
  bounds make every file in a bucket report the same key range.
- A bloom filter on the key, which the bloom residency tier serves from.
- zstd compression.
- Data files of 32 to 64 MiB. First-byte latency of a range GET rises with
  object size.

As an example, this is how the project's own golden tables are written in
Spark SQL (the full recipe is `dev/golden-tables.py`):

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

For values of kilobytes per key, the recipe lowers
`write.parquet.page-size-bytes` to `65536` and sets
`write.parquet.row-group-check-min-record-count` and
`write.parquet.row-group-check-max-record-count` to small values so the
byte bound is checked often enough to hold. Compaction has to carry the
same file-size target or it undoes the layout.

Deviations fall into three tiers, and the line between them is what a
client could observe. A table icetray cannot serve correctly is refused at
load with a named reason: unsorted files, null or duplicate keys, a key
column whose physical type contradicts the schema, an unsupported key or
value type, delete files, keys over the cap, and the residency refusals
below. A table that serves correctly but slowly is priced: a dictionary
encoded value column, key pages over 2,000 rows, no bucket partition, and
overlapping files within a bucket each carry a measured penalty in extra
GETs per hit, key bytes decoded per lookup, or files probed per lookup. A
priced table is refused when its `strict` setting is on, which is the
default, and served with its penalty reported when it is off. Everything
else (truncated bounds, oversized value pages, long keys, oversized files,
a truncated column index, too many files per bucket, a bloom filter that
is absent or badly sized) is advice, which names the writer property to
change and never affects whether a table serves.

`icetray doctor` is the check. The tiers are RFD 002 (the contract), RFD
007 (the tiers and their penalties), and RFD 016 (what the doctor checks
and reports), under `docs/rfd/`.

## Running against a table

Configuration is one file, TOML or JSON, read into one schema; the syntax is
decided by content (a document whose first non-whitespace character is `{`
is JSON). A file with one static table and one table from a REST catalog:

```toml
[server]
listen = "0.0.0.0:7433"
metrics_listen = "0.0.0.0:9433"
poll_interval = "10s"

[storage.local]
dir = "/var/lib/icetray"
page_cache_disk_bytes = 8589934592

[[tables]]
name = "kv"
ref = "main"
key_column = "key"
value_column = "value"

[tables.source]
type = "static"
location = "s3://warehouse/db/kv"

[[tables]]
name = "profiles"
ref = "serving"
key_column = "user_id"
value_column = "vector"
residency = "disk"

[tables.source]
type = "rest"
uri = "http://catalog.example:8181"
warehouse = "example"
namespace = "db"
table = "profiles"
```

A static source is a table root, followed through its
`metadata/version-hint.text`, or a metadata file location. A REST source is
an Iceberg REST catalog, a namespace, and a table name. Each table also
takes `strict` (default `true`), `residency` (`full`, `disk`, or `bloom`,
default `full`), and `resident_budget_bytes`, a budget a `full` load is
refused over, at the footer stage, before any key chunk is fetched.

Every section defaults when absent, and a key the schema does not know is
an error. The server defaults are `listen = "127.0.0.1:7433"`,
`metrics_listen = "127.0.0.1:9433"`, `poll_interval = "10s"` (at least
`"1s"`, since a table's poll calls its catalog or reads its version hint,
and at most `"24h"`), and `page_cache_bytes = 268435456` (256 MiB; a budget below 524800 bytes, one
64 KiB value page and its 64-byte entry overhead for each of the cache's
eight shards, is refused, since each shard would keep a page whatever its
share and the budget would bound nothing). Under `storage`, `s3` holds
explicit client settings (`endpoint`, `region`, `access_key_id` and
`secret_access_key` as a pair, `allow_http`, `path_style`) and is omitted to
use the AWS default credential chain; `rewrite` is a list of `{from, to}`
prefix rules applied to every location the metadata yields, for a mirrored
or relocated table; `hedge` controls the hedging of lookup GETs (on by
default, at the 95th percentile of a 256-request window, capped at five
percent of requests); `local` is the optional local directory described
under Serving. Locations may be `s3://`, `s3a://`, or `file://`.

The server and storage sections also bound how long work may run, so a
wedged object store or catalog fails requests fast instead of piling them
up. Under `server`: `get_deadline = "5s"` and `multi_get_deadline = "15s"`
cap a `Get` and a whole `MultiGet` (expiry is `DEADLINE_EXCEEDED`; a
client's own deadline is enforced by the gRPC layer), and `get_deadline` also caps
every page GET a lookup makes, in a `MultiGet` too, so a GET to a store
that has stopped answering is cancelled rather than left running; `max_in_flight = 1024` requests
may run at once, and one more is refused at once with `RESOURCE_EXHAUSTED`;
`max_multi_get_keys = 1000` keys may be in one `MultiGet`, and more is
`INVALID_ARGUMENT`; `refresh_deadline = "5m"` caps a table's first load or
one refresh, after which the table is degraded and the next poll tries
again; `drain_deadline = "20s"` is how long a shutdown waits for open
requests. Under `storage`: `max_retries = 2` retries of a failed S3 request
(zero tries once), `connect_timeout = "1s"` for an S3 connection, and
`catalog_timeout = "30s"` for one catalog call. Each value above is the
default; every duration and both limits must be above zero, and
`poll_interval` between one second and one day.

With the file in hand:

```sh
icetray doctor --config icetray.toml
icetray info --config icetray.toml
icetray get --config icetray.toml --table kv 42
```

`doctor` prints one `doctor` line per table with the summary, one `finding`
line per cost or advice finding, and with `--files` one `file` line per data
file; a table it could not load is one `refused` line. `info` prints one
`table` line per served table. `get` prints one `get` line per key; `--table`
is required when more than one table is configured. Keys are typed as
icetray prints them: a long as its digits, a string as its text, a binary
key as hex with or without `0x`, a uuid hyphenated or as 32 hex digits.
`--table` on `doctor` and `info` limits the run to one table. With neither
`--config` nor `--location`, the file comes from `ICETRAY_CONFIG`; a
`--location` run ignores the variable.

One table can be opened without a file. `--location` takes a table root or
a metadata file and opens it under the name `table`, with `--key-column`
(default `key`), `--value-column` (default `value`), `--ref` (default
`main`), and `--residency` (default `full`); `--rewrite FROM=TO` applies a
prefix rewrite under either target and may be repeated. A located table is
never strict, so the doctor reports what `strict` would refuse.

```sh
icetray doctor --location s3://warehouse/db/kv
icetray get --location s3://warehouse/db/kv/metadata/v3.metadata.json 42
```

Output is JSON, one object per line, with `kind` first and `table` second so
a reader can route a line before parsing the rest. A `get` line:

```json
{"kind":"get","table":"kv","key":"42","presence":"present","value_base64":"aGVsbG8=","value_utf8":"hello","value_bytes":5,"snapshot_id":8412603190021477089,"gets":1,"elapsed_micros":250}
```

`presence` is `present`, `null`, or `absent`; the value fields appear only
for a present value, and `value_utf8` only when the bytes are UTF-8. `gets`
is the number of GETs the lookup issued. The `table` and `doctor` lines
carry the table's `snapshot_id`, `serviceability` (`efficient` or
`inefficient`), `residency`, `file_count`, `buckets`, `records`,
`index_bytes`, the count of `costs` and `advice`, and under `predicted` the
resident bytes each tier would need, computed from footers alone, so an
owner chooses a tier from one run.

Exit codes: 0 when the run did what was asked (for the doctor, no cost
finding; advice does not change the code); 2 when the doctor found a cost
finding, which a build pipeline gates on; 1 for a load or read error, a
table the doctor could not load (which dominates 2), a configuration file
that cannot be read or parsed, or a listener that would not bind; 64 when
the command line could not be used: neither or both of `--config` and
`--location`, a location flag with `--config`, no table matched, `--table`
required, a key that does not parse, or a bad log field mapping. The
one-shots log to stderr at `warn`, since their stdout is data.

## Serving

```sh
icetray serve --config icetray.toml
ICETRAY_CONFIG=/etc/icetray/icetray.toml icetray serve
```

`serve` takes only the configuration path. It binds the gRPC listener and
the metrics listener, registers the gRPC health service as `NOT_SERVING`,
and then loads every table concurrently, so the listeners accept while
tables load. Once every table serves its first generation, health flips to
`SERVING` and `/ready` on the metrics listener answers 200; until then it
answers 503. A table that fails its first load is fatal: the reason is
logged, health stays `NOT_SERVING` for ten seconds so an orchestrator's
probe sees the refusal beside its logged reason, and the process exits 1.
A silently missing table is worse than a crash at boot. SIGTERM or SIGINT
drains in-flight requests, flushes the page cache's disk level, and exits 0.
The drain waits at most `server.drain_deadline` for open calls. Past it,
the drain stops waiting, the page cache is still flushed, and the process
exits 1; a call still open ends with the process.

The client contract is the checked-in protobuf,
`crates/icetray-proto/proto/icetray.proto`, from which clients in any
language are generated; icetray ships no hand-written client. The service
has three calls. `Get` looks one key up in one table and returns a presence
(`PRESENCE_PRESENT` with the value's bytes, `PRESENCE_NULL`, or
`PRESENCE_ABSENT`) and the snapshot id it was read from. `MultiGet` answers
many keys in request order, all from one snapshot named once on the
response; when any key cannot be answered the whole call fails and the
caller retries the batch it built, so a response never answers only some of
the keys. `TableInfo` reports the served snapshot id, bucket count, data
file count, row count, key type, and resident index bytes; clients switch
on the key type to choose the key variant. A key is a oneof of `long`,
`str`, and `bin`, where `bin` carries binary, fixed, and uuid keys; the
wrong variant is an `INVALID_ARGUMENT` error, a table the server does not
serve is `NOT_FOUND`, and a miss is a normal answer, never an error. A
`Get` or `MultiGet` still running at its deadline (`server.get_deadline` or
`server.multi_get_deadline`) is `DEADLINE_EXCEEDED`. A deadline the client
sends is enforced by the gRPC layer: the client's library ends the call at
it and reports the expiry as its own (a tonic client reports `CANCELLED`),
and the server drops the call then, freeing its slot. `Get` and `MultiGet`
share `server.max_in_flight` slots, each held until its call ends; a call
that finds none free is refused at once with `RESOURCE_EXHAUSTED` rather
than queued. `TableInfo` takes no slot. A `MultiGet` of more than
`server.max_multi_get_keys` keys is `INVALID_ARGUMENT` before any lookup.

The listener speaks plaintext gRPC and terminates no TLS. icetray expects a
private network or a service mesh that encrypts traffic and authenticates
callers, and carries no certificate, credential store, or access policy of
its own. The default listen addresses are loopback for the same reason:
exposing the port is a decision the deployer writes into the file.

The metrics listener serves `/metrics` in Prometheus format and `/ready`.
Counters: `icetray_requests_total{call}`, `icetray_object_gets_total{kind}`,
`icetray_object_bytes_total{kind}`, `icetray_local_reads_total`,
`icetray_local_bytes_total`, `icetray_hedges_total`,
`icetray_page_cache_hits_total`, `icetray_page_cache_fetches_total`,
`icetray_lookups_total{table,result}`,
`icetray_refresh_errors_total{table,reason}`, and
`icetray_generation_swaps_total{table}`. Histograms:
`icetray_lookup_seconds{table,result}` and
`icetray_object_get_seconds{kind}`. Gauges: `icetray_index_bytes{table}`
(the index's live bytes, exported beside `icetray_process_rss_bytes`
because a residency tier that sheds bytes is not visible in process memory
on every platform), `icetray_page_cache_bytes`,
`icetray_findings{table,code}`, `icetray_generation_age_seconds{table}`,
`icetray_generation_info{table,snapshot_id}` (1 for the serving snapshot,
with the id as a label so no digits are lost), and
`icetray_table_state{table,state}`. Every label value is the name of an
enum variant, so the set of values is closed.

Logs are JSON events on stdout at `info`, one object per line. The standard
fields are `time` (RFC 3339, UTC), `level`, `message`, and `target`;
`server.log.fields` maps each to the name a deployer's log schema expects,
and `server.log.static_fields` adds constant fields to every event:

```toml
[server.log.fields]
time = "ts"
level = "severity"

[server.log.static_fields]
service = "icetray"
```

A key of `fields` that names no standard field, or two standard fields
mapped to one name, is refused at startup with exit 64.

`storage.local.dir` names an optional local directory. It holds two things:
the chunk files of tables with `residency = "disk"`, which keep each data
file's key column on local disk with only the fence array in memory, and
the disk level of the value-page cache, bounded by
`storage.local.page_cache_disk_bytes` (zero turns the level off; any other
value is at least one 4 MiB block). `index_budget_bytes` bounds the chunk
files across every table, measured against the directory as it is when a
load begins: the serving generation's chunk files stay until the new one
has swapped in, so a refresh that rewrites every data file needs room for
both sets, and a table rewritten wholesale wants a budget of about twice
its steady footprint. The steady footprint is the doctor's
`predicted.disk_bytes`: each chunk file's header and key chunks, the bytes
the budget is charged. A disk-tier lookup costs one local read and, on a
hit, one GET; a `bloom` table keeps the key column's bloom filters in
memory instead, needs no disk, and pays two GETs per hit and a GET per miss
at the filter's false-positive rate. `disk` residency without a local
directory is a configuration error, and `bloom` on a table whose files
carry no key bloom filter is a refusal at load.

Each table's ref is re-resolved every `server.poll_interval`. A poll that
finds the metadata location unchanged costs one read. A poll that finds a
new snapshot builds the generation beside the current one, loading only the
data files the previous generation did not hold and checking key
uniqueness only across overlap groups that gained a file, then swaps it in;
`icetray_generation_swaps_total` counts the swap. A refresh that fails
leaves the last good generation serving, counts
`icetray_refresh_errors_total`, and moves the table's state to degraded
without affecting readiness. Snapshot retention on the served ref must
exceed the longest lag a replica can have, since an expired snapshot's data
files vanish under a replica still serving it; icetray exposes the lag as
`icetray_generation_age_seconds` and cannot enforce the retention.

## Deploying

icetray is operable through a twelve-factor surface and carries no
deployer-specific code. The test for anything added to it is whether a
second organization could deploy the result without editing source.

The deployer's wrapper is a container image, built from source (no image is
published yet), that carries the binary, its configuration file, and its
log field names. The configuration path comes from `ICETRAY_CONFIG`, so a
pod passes no flags; a pipeline that renders JSON hands the file over as is,
since both syntaxes read into the same schema. The same image carries
`doctor`, `get`, and `info`, so an operator can run them inside the pod
against the daemon's own configuration and credentials to explain a
refusal.

Credentials come from the AWS SDK's default chain: profiles, single
sign-on, external credential helpers, and pod identity, resolved once and
cached until shortly before expiry, so a credential helper is not spawned
per GET. Static keys in `storage.s3` win over the chain and are the path
for S3-compatible endpoints; `endpoint`, `allow_http`, and `path_style` are
there for the same stores. No secret format of icetray's own exists.

The gRPC port is plaintext and unauthenticated. Run it on a private network
or behind a mesh or sidecar; there is no mode in which the port is safe to
expose directly.

A local volume is optional. Mount one to run disk-residency tables or to
keep a disk level of the page cache across restarts. Everything in it is a
copy of bytes in object storage, named by the data file it came from and
checked against that file's identity, and is rebuilt on demand; a replica
that restarts with its volume reuses every chunk file whose data file is
still referenced and deletes the rest. The volume needs capacity and
nothing else: nobody backs it up or shares it between replicas, and losing
it costs one cold load.

## Building and testing

`rust-toolchain.toml` pins the stable toolchain and the `rustfmt` and
`clippy` components; rustup installs it on first use. Building from source
needs no protobuf compiler on the host, since the build carries its own.

```sh
cargo build --release
cargo test --workspace
dev/ci-checks.sh
```

`dev/ci-checks.sh` is the lint gate: `cargo fmt --all --check` and `cargo
clippy --workspace --all-targets --locked -- -D warnings`. CI runs that
script and `cargo test --workspace --locked`, the same two commands a
contributor runs before a push.

Two suites are gated behind fixtures and refuse to run rather than report a
pass they did not earn; under a plain `cargo test` they show as ignored.
`dev/test-int.sh` runs the REST catalog tests against a catalog at
`ICETRAY_REST_URI` (default `http://127.0.0.1:8181`); `dev/runtime.sh up`
followed by `dev/rest-fixture.sh up` starts the reference catalog in a
container. `dev/test-golden.sh` runs the layout tests against the golden
tables `dev/golden-tables.py` writes, one contract-shaped table per key
type, one more in table format version 3 (the rest are version 2, the
writer's default), and the off-contract layouts whose cost and advice
findings the doctor reports:

```sh
dev/golden-tables.sh
ICETRAY_GOLDEN_WAREHOUSE=file://$PWD/target/golden-warehouse dev/test-golden.sh
```

The testkit also carries a scale probe, a program and not a test, that
writes 1,500 files under a temporary directory, loads them, times lookups
and a refresh, and prints what it measured:

```sh
cargo run -p icetray-testkit --example scale --release
```

`dev/golden-tables.sh` runs the recipe with Spark 4.1.3 and Iceberg 1.11.0
(`iceberg-spark-runtime-4.1_2.13`) in a container, so the host needs neither
Spark nor a Java runtime; the recipe itself runs anywhere Spark 4.1 has the
Iceberg runtime on its classpath and can write to any object store or local
directory. Spark 4.1 is the newest line Iceberg 1.11 ships a runtime for.
The REST suite runs against the `apache/iceberg-rest-fixture:1.10.1` image.
The recipe is also the worked example of the contract: the properties it
sets are the ones a table owner sets.

## Design

The decisions are recorded in `docs/rfd/`, one per file, starting with
`docs/rfd/001-how-to-rfd.md`. The measurements behind them are in
`docs/analysis/`, and the build they describe is in `docs/plans/`.
Conventions for contributors are in `CONTRIBUTING.md`; agents read
`AGENTS.md` as well.

The lineage: [ElephantDB](https://github.com/nathanmarz/elephantdb) proved
the batch-build-then-serve shape, and at least one published design showed
it works directly against lake files. icetray is that idea on stock Iceberg
tables: the bucket transform routes a key to its partition by arithmetic,
the resident key column finds the row, and every byte below a snapshot is
immutable, so caches never invalidate.

## License

MIT or Apache-2.0, at your option.
