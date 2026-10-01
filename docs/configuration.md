# Configuration and the command line

## The file

One TOML or JSON file, read by `serve` and the one-shots; a document whose
first non-whitespace character is `{` is JSON. Every section defaults when
absent, and an unknown key is an error. Values shown below are the
defaults.

One static table and one table from a REST catalog:

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

### `[[tables]]`

- A static source is a table root, followed through its
  `metadata/version-hint.text`, or a metadata file location.
- A REST source is an Iceberg REST catalog, a namespace, and a table name.
- `strict = true`.
- `residency = "full"`; also `disk` or `bloom`.
- `resident_budget_bytes`: a `full` load over it is refused at the footer
  stage, before any key chunk is fetched.

### `[server]`

- `listen = "127.0.0.1:7433"`.
- `metrics_listen = "127.0.0.1:9433"`.
- `poll_interval = "10s"`, at least `"1s"` and at most `"24h"`.
- `page_cache_bytes = 268435456` (256 MiB). The cache has one shard per
  tokio worker thread (one per core unless `TOKIO_WORKER_THREADS` says
  otherwise), rounded up to a power of two. A budget below one 64 KiB
  value page plus its 64-byte entry overhead per shard is refused: parsing
  refuses under one shard's 65600 bytes, and opening the cache refuses
  under the shards' total (1049600 bytes for the 16 shards of an 11-core
  machine), with exit code 1, before any table loads.
- `get_deadline = "5s"` caps a `Get` and every page GET a lookup makes, in
  a `MultiGet` too.
- `multi_get_deadline = "15s"` caps a whole `MultiGet`.
- Expiry of either deadline is `DEADLINE_EXCEEDED`; a client's own
  deadline is the gRPC layer's.
- `max_in_flight = 1024`; one more request is `RESOURCE_EXHAUSTED`.
- `max_multi_get_keys = 1000`; more is `INVALID_ARGUMENT`.
- `load_deadline = "30m"` caps a first load, or any build while no
  generation serves.
- `refresh_deadline = "5m"` caps one refresh over a serving generation;
  past it the table is degraded and the next poll retries.
- `drain_deadline = "20s"`: how long a shutdown waits for open requests.

### `[storage]`

- `s3` holds explicit client settings: `endpoint`, `region`,
  `access_key_id` and `secret_access_key` as a pair, `allow_http`, and
  `path_style`. Omit it to use the AWS default credential chain.
- `rewrite` is a list of `{from, to}` prefix rules applied to every
  location the metadata yields.
- `hedge` controls the hedging of lookup GETs: on by default, at the 95th
  percentile of a 256-request window, capped at five percent of requests.
- `local` is the optional local directory described in
  [Operating icetray](operating.md#the-local-directory).
- `max_retries = 2` retries of a failed S3 request; zero tries once.
- `connect_timeout = "1s"` for an S3 connection.
- `catalog_timeout = "30s"` for one catalog call.
- Locations may be `s3://`, `s3a://`, or `file://`.

Every duration, `max_in_flight`, and `max_multi_get_keys` must be above
zero.

## The command line

```sh
icetray doctor --config icetray.toml
icetray info --config icetray.toml
icetray get --config icetray.toml --table kv 42
```

- `doctor` prints one `doctor` line per table, one `finding` line per cost
  or advice finding, and with `--files` one `file` line per data file. A
  table it could not load is one `refused` line, and so is one with keys
  outside their file's recorded bucket (the load hashes each file's
  page-boundary keys; the doctor hashes every key).
- `info` prints one `table` line per served table.
- `get` prints one `get` line per key. `--table` is required when more
  than one table is configured.
- Keys are typed as icetray prints them: a long as its digits, a string as
  its text, a binary key as hex with or without `0x`, a uuid hyphenated or
  as 32 hex digits.
- `--table` on `doctor` and `info` limits the run to one table.
- With neither `--config` nor `--location`, the file comes from
  `ICETRAY_CONFIG`. A `--location` run ignores the variable.

### One table without a file

`--location` takes a table root or a metadata file and opens it under the
name `table`.

- `--key-column`, default `key`.
- `--value-column`, default `value`.
- `--ref`, default `main`.
- `--residency`, `full` or `bloom`, default `full`; `disk` is a usage
  error.
- `--rewrite FROM=TO`, repeatable, under either target. A command-line
  rule overrides the file's for the same prefix and is validated with it;
  an empty `FROM` exits 1.
- A located table runs with `strict` off, so the doctor reports what
  `strict` would refuse.

```sh
icetray doctor --location s3://warehouse/db/kv
icetray get --location s3://warehouse/db/kv/metadata/v3.metadata.json 42
```

## Output

JSON, one object per line, `kind` first and `table` second. A `get` line:

```json
{"kind":"get","table":"kv","key":"42","presence":"present","value_base64":"aGVsbG8=","value_utf8":"hello","value_bytes":5,"snapshot_id":8412603190021477089,"gets":1,"elapsed_micros":250}
```

- `presence` is `present`, `null`, or `absent`.
- The value fields appear only for a present value, and `value_utf8` only
  when the bytes are UTF-8.
- `gets` is the number of GETs the lookup issued.
- The `table` and `doctor` lines carry `snapshot_id`, `serviceability`
  (`efficient` or `inefficient`), `residency`, `file_count`, `buckets`,
  `records`, `index_bytes`, the count of `costs` and `advice`, and under
  `predicted` the resident bytes each tier would need, computed from
  footers alone.
- The one-shots log to stderr at `warn`; their stdout is data.

## Exit codes

- 0: the run did what was asked. For the doctor, no cost finding; advice
  does not change the code.
- 2: the doctor found a cost finding.
- 1: a load or read error; a table the doctor could not load or refused
  (which dominates 2); a configuration that cannot be read or parsed or
  breaks one of its rules, a bad log field mapping among them; or a
  listener that would not bind.
- 64: the command line could not be used: neither or both of `--config`
  and `--location`, a location flag with `--config`, a `--residency` other
  than `full` or `bloom`, no table matched, `--table` required, or a key
  that does not parse.
