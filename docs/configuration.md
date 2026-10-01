# Configuration and the command line

The configuration file `serve` and the one-shot subcommands read, and what
`doctor`, `info`, and `get` print.

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
and at most `"24h"`), and `page_cache_bytes = 268435456` (256 MiB). The cache's memory has one shard
per tokio worker thread (one per core unless `TOKIO_WORKER_THREADS` says
otherwise), rounded up to a power of two, and a budget below one 64 KiB
value page and its 64-byte entry overhead per shard is refused, since each
shard would keep a page whatever its share and the budget would bound
nothing: parsing refuses less than one shard's 65600 bytes, and opening the
cache refuses less than the shards' total (1049600 bytes for the 16 shards
of an 11-core machine), before any table loads, with exit code 1. Under `storage`, `s3` holds
explicit client settings (`endpoint`, `region`, `access_key_id` and
`secret_access_key` as a pair, `allow_http`, `path_style`) and is omitted to
use the AWS default credential chain; `rewrite` is a list of `{from, to}`
prefix rules applied to every location the metadata yields, for a mirrored
or relocated table; `hedge` controls the hedging of lookup GETs (on by
default, at the 95th percentile of a 256-request window, capped at five
percent of requests); `local` is the optional local directory described
in [Operating icetray](operating.md#the-local-directory). Locations may be `s3://`, `s3a://`, or `file://`.

The server and storage sections also bound how long work may run, so a
wedged object store or catalog fails requests fast instead of piling them
up. Under `server`: `get_deadline = "5s"` and `multi_get_deadline = "15s"`
cap a `Get` and a whole `MultiGet` (expiry is `DEADLINE_EXCEEDED`; a
client's own deadline is enforced by the gRPC layer), and `get_deadline` also caps
every page GET a lookup makes, in a `MultiGet` too, so a GET to a store
that has stopped answering is canceled rather than left running; `max_in_flight = 1024` requests
may run at once, and one more is refused at once with `RESOURCE_EXHAUSTED`;
`max_multi_get_keys = 1000` keys may be in one `MultiGet`, and more is
`INVALID_ARGUMENT`; `load_deadline = "30m"` caps a table's first load, or
any build while no generation serves, and `refresh_deadline = "5m"` one
refresh over a serving generation, after which the table is degraded and
the next poll tries again; `drain_deadline = "20s"` is how long a shutdown waits for open
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
file; a table it could not load is one `refused` line, and so is one whose
keys it finds outside their file's recorded bucket (the load hashes each
file's page-boundary keys, the doctor every key). `info` prints one
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
`main`), and `--residency` (`full` or `bloom`, default `full`; `disk` is a
usage error, since a one-shot loads once and exits and the doctor under
`full` already reports every tier's predicted bytes); `--rewrite FROM=TO` applies a
prefix rewrite under either target and may be repeated. A command-line rule
takes precedence over the file's for the same prefix, and is checked with
the file's, so an empty `FROM` exits 1 as it does in a file. A located table is
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
table the doctor could not load or refused (which dominates 2), a
configuration that cannot be read or parsed or breaks one of its rules (a
bad log field mapping among them), or a listener that would not bind; 64
when the command line could not be used: neither or both of `--config` and
`--location`, a location flag with `--config`, a `--residency` other than
`full` or `bloom`, no table matched, `--table` required, or a key that does
not parse. The
one-shots log to stderr at `warn`, since their stdout is data.
