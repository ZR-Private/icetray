# Configuration and the command line

## The file

The configuration is one file, TOML or JSON, read by `serve` and by the
[one-shots](glossary.md) (`doctor`, `get`, and `info`). A document whose
first non-whitespace character is `{` is JSON; anything else is TOML. Both
read into the same schema.

- Every section may be left out, and then takes its defaults. Within a
  section, a key marked required below must be present.
- At least one table is required. A file that lists none is invalid.
- A key the schema does not know is an error, and the message lists the
  keys that section accepts.
- The whole file is validated before anything is opened. An error names
  the file, where its path came from, and the setting by its full path,
  such as `invalid configuration file icetray.toml (from --config):
  server.poll_interval 0s is below the minimum of 1s`, and the run exits 1.
  A file that is not valid TOML is reported in one line with the line and
  column, such as
  `` is not valid toml: line 1, column 10: unclosed array table, expected `]` ``.

### An example

This example serves one table from a table root and one from a REST
catalog. Its values are chosen for a container, not copied from the
defaults: the default listeners are loopback, and `storage.local` has no
default at all.

```toml
[server]
listen = "0.0.0.0:7433"
metrics_listen = "0.0.0.0:9433"
poll_interval = "30s"

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

### Value formats

- **Durations** are strings in a unit-suffixed form: `"10s"`, `"250ms"`,
  `"100us"`, `"1.5s"`, `"1m30s"`, `"1m 30s"`, `"2h 30m"`, and
  `"10 seconds"` are all accepted. The largest unit is the hour, so
  `"24h"` is accepted and `"1d"` is not. A bare number, such as `10` or
  `"10"`, a negative duration, and ISO 8601 text such as `"PT10S"` are
  refused.
- **Byte sizes and counts** are TOML or JSON integers, in bytes for a
  size. TOML's digit separators work (`268_435_456`). Suffixes such as
  `"256MiB"` and floating-point numbers are refused.
- **Addresses** are an IP address and a port: `"127.0.0.1:7433"`,
  `"0.0.0.0:7433"`, `"[::]:7433"`. A host name is refused.
- **Locations** are URIs with the scheme `s3://`, `s3a://`, or `file:`,
  or absolute local paths. A `file:` URI is `file:///data/kv`,
  `file://localhost/data/kv`, or `file:/data/kv`; one with a relative path
  (`file:data/kv`) or another host (`file://otherhost/data/kv`) is
  refused. A `static` table's location is checked when the file is
  validated, after the file's own rewrite rules: an empty location, a
  relative path, and a location no store serves, such as `gs://` with no
  rule rewriting it, are refused there as `tables[0].source.location`,
  with exit 1. A `--rewrite` on the command line does not rescue such a
  location. The file's locations are never resolved against a working
  directory. (A relative `--location` on the command line is; see
  [One table without a file](#one-table-without-a-file).) The locations a
  table's metadata or catalog names are known only as it loads, and one
  that no store serves fails the load with the error kind `location`.

### `[server]`

| Key | Type | Default | Meaning |
|---|---|---|---|
| `listen` | address | `"127.0.0.1:7433"` | The gRPC listener. |
| `metrics_listen` | address | `"127.0.0.1:9433"` | The HTTP listener for `/metrics` and `/ready`. |
| `poll_interval` | duration | `"10s"` | How often each table's ref is re-resolved and the configuration file re-read. At least `"1s"` and at most `"24h"`. |
| `page_cache_bytes` | bytes | `268435456` (256 MiB) | The memory budget of the value-page cache. See the note below. |
| `get_deadline` | duration | `"5s"` | The longest a `Get` may run, and the longest each page fetch of any lookup may run, in a `MultiGet` too. Past it the call is `DEADLINE_EXCEEDED`. |
| `multi_get_deadline` | duration | `"15s"` | The longest a whole `MultiGet` may run, whatever its size. |
| `max_in_flight` | count | `1024` | `Get` and `MultiGet` calls in flight at once. One more is refused at once as `RESOURCE_EXHAUSTED`, never queued. |
| `max_multi_get_keys` | count | `1000` | Keys one `MultiGet` may carry. More is `INVALID_ARGUMENT` before any lookup. |
| `load_deadline` | duration | `"30m"` | The longest a build may run while no generation of the table serves: a first load, or a retry after one failed. |
| `refresh_deadline` | duration | `"5m"` | The longest one refresh over a serving generation may run. Past it the table is [degraded](glossary.md) and the next poll retries. |
| `drain_deadline` | duration | `"20s"` | How long a shutdown waits for open calls before it stops waiting. |

Every duration and both counts must be above zero. A client's own
deadline is enforced by the gRPC layer and is the client's to report.

The page cache has one shard per tokio worker thread, rounded up to a
power of two, and each shard must hold one 64 KiB value page plus its
64-byte entry: 65,600 bytes. Parsing refuses a `page_cache_bytes` under
one shard's minimum, and opening the cache refuses one under the total
for the machine's shards (1,049,600 bytes for the 16 shards of an
11-core machine), with exit 1, before any table loads. The worker count
is the number of cores the process sees, unless `TOKIO_WORKER_THREADS`
sets it.

### `[server.log]`

Logs are JSON, one object per line, with the standard fields `time`,
`level`, `message`, and `target`
([Operating icetray](operating.md#logs)).

| Key | Type | Default | Meaning |
|---|---|---|---|
| `fields` | table of standard field to string | empty | Renames standard fields: `time = "ts"` emits the time as `ts`. A key other than the four standard fields is refused, and so are two fields mapped to one name. |
| `static_fields` | table of string to string | empty | Constant fields added to every event. |

```toml
[server.log.fields]
time = "ts"
level = "severity"

[server.log.static_fields]
service = "icetray"
```

### `[storage]`

| Key | Type | Default | Meaning |
|---|---|---|---|
| `max_retries` | count | `2` | Retries of a failed S3 request. Zero tries each request once. |
| `connect_timeout` | duration | `"1s"` | The longest an S3 connection may take to open. |
| `catalog_timeout` | duration | `"30s"` | The longest one catalog call may run before the poll or load that made it fails: a `rest` table load, a `glue` `GetTable`, or an `s3tables` `GetTableMetadataLocation`. |
| `s3` | table | absent | Explicit S3 client settings, below. |
| `rewrite` | array of tables | empty | Location prefix rewrites, below. |
| `hedge` | table | hedging on | Hedging of lookup GETs, below. |
| `local` | table | absent | The local directory, below. Required by `disk` residency. |

### `[storage.s3]`

Without this table, S3 requests are signed with credentials from the AWS
SDK's default chain: environment variables, profiles and single sign-on,
credential helpers, web identity, and container or instance roles. The
chain and the region are loaded only when some location, after rewriting,
is `s3://` or `s3a://`, or when a table has a `glue` or `s3tables`
source; a run over `file://` tables from `static` or `rest` sources reads
no AWS configuration and never contacts the instance metadata service.
Calls to Glue and S3 Tables are signed with the same credentials as S3
requests: the static keys here when they are set, otherwise the chain,
which is resolved once for both.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `endpoint` | string | AWS | The endpoint URL, for S3-compatible stores. |
| `region` | string | the SDK's | The region. A `glue` or `s3tables` source that names no region of its own uses it too. |
| `access_key_id` | string | none | A static access key id. Set with `secret_access_key` or not at all. Static keys win over the chain. |
| `secret_access_key` | string | none | The static secret key. It is never printed. |
| `allow_http` | boolean | `false` | Permit a plain HTTP endpoint. |
| `path_style` | boolean | `false` | Address buckets as `host/bucket/key` rather than by virtual host. |

### `[[storage.rewrite]]`

Each rule replaces a location prefix before any read, for tables whose
metadata records paths from another host or bucket. Rules apply in file
order to every location the metadata yields, and the first rule whose
`from` matches wins.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `from` | string | required | The prefix as the metadata records it. Must not be empty. |
| `to` | string | required | The prefix to read from instead. Must not be empty. |

### `[storage.hedge]`

A lookup GET still unanswered after the delay is sent a second time, and
the first answer wins. Load reads are never hedged.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `enabled` | boolean | `true` | Whether lookup GETs are hedged. |
| `percentile` | number | `0.95` | The delay is this percentile of the table's recent lookup GET latencies. In (0, 1]. |
| `window` | count | `256` | Lookup GETs in each table's latency window. At least 1. |
| `max_rate` | number | `0.05` | The share of requests that may hedge. In [0, 1]. |

The ranges are checked even when `enabled` is `false`.

### `[storage.local]`

The optional local directory: the chunk files of `disk` tables and the
page cache's disk level. Everything in it is rebuilt from object storage
([Operating icetray](operating.md#the-local-directory)).

| Key | Type | Default | Meaning |
|---|---|---|---|
| `dir` | path | required | The directory. A relative path is taken from the working directory. |
| `page_cache_disk_bytes` | bytes | required | The size bound of the page cache's disk level. `0` turns the level off; any other value is at least one 4 MiB block, 4194304. |
| `index_budget_bytes` | bytes | unbounded | The bytes chunk files may occupy across every table, checked when a load begins. A load that would pass it is refused as `over_budget`. |

The disk level holds one open file descriptor per 4 MiB block, so
`page_cache_disk_bytes` sets part of the descriptor limit a replica needs
([Operating icetray](operating.md#file-descriptors)).

### `[[tables]]`

| Key | Type | Default | Meaning |
|---|---|---|---|
| `name` | string | required | The name clients use. Unique and not empty. |
| `ref` | string | required | The Iceberg branch or tag to serve, such as `"main"`. |
| `key_column` | string | required | The key column. |
| `value_column` | string | required | The value column. |
| `source` | table | required | Where the table's metadata comes from, below. |
| `lookups` | string | the best level `residency` reaches | The lookup cost the table accepts: `"optimal"`, `"balanced"`, or `"permissive"`; see [Choosing a level](#choosing-a-level). `full` and `disk` default to `"optimal"` and `bloom` to `"balanced"`. A level the residency cannot reach makes the file invalid, as `tables[0].lookups optimal is above what tables[0].residency bloom reaches; bloom reaches balanced at best`. |
| `strict` | boolean | `false` | Refuse a snapshot whose layout puts the table below its `lookups` level. A strict table whose first snapshot is below its level fails the start; a later one is refused, and the table is [held](glossary.md): the last snapshot that met the level keeps serving. Off, the snapshot serves and the table is flagged below its level. |
| `residency` | string | `"full"` | `"full"`, `"disk"`, or `"bloom"`; see [Choosing a residency](#choosing-a-residency). `"disk"` needs `[storage.local]`; without it the file is invalid, as `tables[0].residency disk needs a local directory under storage.local`. |
| `resident_budget_bytes` | bytes | unbounded | For `full` residency only: a load whose resident bytes would pass it is refused as `over_budget` once the footers are read, before any key chunk is fetched. Set on a `disk` or `bloom` table, it makes the file invalid. |

### `[tables.source]`

The `type` key picks the source kind. On every poll, icetray finds the
table's current metadata, resolves the configured ref in it, and builds a
new [generation](glossary.md) when the ref names a new snapshot.

| `type` | Keys | What a poll follows |
|---|---|---|
| `"static"` | `location`, required: a table root or a metadata file | A table root is followed through its `metadata/version-hint.text`, as the Hadoop catalog writes it; an unchanged hint costs that one read. A location ending in `.metadata.json` names one metadata file, which never changes, so the table never advances. |
| `"rest"` | `uri`, required: the catalog's base URI; `warehouse`, optional; `namespace`, required, its levels separated by dots (`"db.schema"`); `table`, required | Each poll loads the table from the catalog, one call within `storage.catalog_timeout`, and follows the metadata location the catalog returns. |
| `"glue"` | `database` and `table`, required; `catalog_id`, `region`, and `endpoint`, optional; [below](#glue) | Each poll calls Glue's `GetTable`, one call within `storage.catalog_timeout`, and follows the table's `metadata_location` parameter. An unchanged location costs that call and no read. |
| `"s3tables"` | `table_bucket_arn`, `namespace`, and `table`, required; `region` and `endpoint`, optional; [below](#s3tables) | Each poll calls S3 Tables' `GetTableMetadataLocation`, one call within `storage.catalog_timeout`, and follows the location it returns. An unchanged location costs that call and no read. |

The `rest` source sends no credentials. Its requests carry no
`Authorization` header, so a catalog that requires a bearer token, OAuth2
client credentials, or SigV4 signing cannot be used directly. It serves
REST catalogs that need no credentials, and catalogs behind a proxy that
adds them. A catalog that answers an unauthenticated request with 401
fails the table's first load, and the server exits 1. Credentials a
catalog vends for storage are not used either: every object read goes
through `[storage]` and its credentials.

The `glue` and `s3tables` sources ask their catalog for the location of
the table's current metadata file and nothing else. The metadata file and
every file it names are read through `[storage]`, its rewrite rules
included, as a `static` table's are. Their calls are signed with the
credentials `[storage.s3]` describes, and a call the catalog refuses or
that fails fails the load or poll with the error kind `catalog`; one that
runs past `storage.catalog_timeout` fails it with `catalog_timeout`.

The region of either source is its own `region` when set, else
`storage.s3.region`, else, for `s3tables`, the region the table bucket's
ARN names. A `glue` source that none of these names follows the SDK's own
region resolution: `AWS_REGION`, the profile, or the instance metadata
service.

#### `glue`

| Key | Type | Default | Meaning |
|---|---|---|---|
| `database` | string | required | The Glue database. Not empty. |
| `table` | string | required | The table in the database. Not empty. |
| `catalog_id` | string | the caller's account | The id of the catalog to read, for a table in another account's catalog. Not empty when set. |
| `region` | string | as above | The region of the Glue endpoint. Not empty when set. |
| `endpoint` | string | AWS | An `http://` or `https://` endpoint URL, for a Glue-compatible service or a VPC endpoint. |

The table must be an Iceberg table: its `table_type` parameter is
`ICEBERG`, in any case, and its `metadata_location` parameter names the
current metadata file. A table of another type is refused with the error
kind `table`, as is one without a `metadata_location`.

```toml
[tables.source]
type = "glue"
database = "db"
table = "kv"
```

#### `s3tables`

| Key | Type | Default | Meaning |
|---|---|---|---|
| `table_bucket_arn` | string | required | The table bucket's ARN, `arn:aws:s3tables:<region>:<account>:bucket/<name>`. Any other ARN makes the file invalid, as `tables[0].source.table_bucket_arn ... is not a table bucket arn`. |
| `namespace` | string | required | The namespace in the bucket. Not empty. |
| `table` | string | required | The table in the namespace. Not empty. |
| `region` | string | as above | The region of the S3 Tables endpoint. Not empty when set. |
| `endpoint` | string | AWS | An `http://` or `https://` endpoint URL, for an S3 Tables-compatible service or a VPC endpoint. |

A table the service holds no metadata location for, such as one created
and never written, is refused with the error kind `table`.

```toml
[tables.source]
type = "s3tables"
table_bucket_arn = "arn:aws:s3tables:us-east-1:111122223333:bucket/warehouse"
namespace = "db"
table = "kv"
```

A required key left out, or a key the source kind does not take, is
refused at `tables[0].source`; an empty value is refused by the setting's
full path, as `tables[0].source.database is empty`.

## Choosing a residency

A table's [residency](glossary.md) decides where its key chunks live, and
with it the table's memory, its disk use, and its GETs per lookup. Every
residency gives the same answers. `icetray info` and `icetray doctor`
print each residency's resident bytes under `predicted`, computed from the
footers alone, so the choice can be made with the numbers in hand.

| | `full` | `disk` | `bloom` |
|---|---|---|---|
| Memory | `predicted.full`: footers, page indexes, fences, and key chunks | `predicted.disk_memory`: footers, page indexes, and fences | `predicted.bloom`: footers, page indexes, fences, and bloom filters |
| Local disk | none | `predicted.disk_bytes` of chunk files | none |
| Cold hit | 1 GET | 1 local read and 1 GET | 2 GETs: the key page, then the value page |
| Warm hit | none | 1 local read | none |
| Miss | none | 1 local read | none, or 1 GET when the filter passes a key that is absent |
| Needs | memory for the key chunks | `[storage.local]` on a mounted volume | a bloom filter on the key in every file |

- **`full`**, the default, is the fastest on a hit. Use it whenever
  `predicted.full` fits in the replica's memory beside the page cache.
  `resident_budget_bytes` turns a table that outgrows its share into a
  refusal before its key chunks are fetched.
- **`disk`** holds a fraction of a byte per key in memory and keeps the
  rest in a local file. Use it for tables whose key chunks do not fit in
  memory, on a replica with a local volume. A local read is a memory copy
  while the operating system's page cache holds the chunk file, so size
  the memory limit with room for `predicted.disk_bytes`
  ([Operating icetray](operating.md#memory)).
- **`bloom`** needs no disk, at the price of a second GET on every cold
  hit and a GET for each false positive. The miss rate follows the
  writer's filter size, which `predicted.bloom_bits_per_key` reports: at
  8 bits per key or more, few misses cost a GET. Writers often size the
  filter per row group rather than per key, which can make
  `predicted.bloom` larger than `predicted.full`; compare the two before
  choosing it. A `bloom` load fetches the key chunks to verify them and
  holds them while it does, so its memory at load is above what it keeps.
  Use it when the keys do not fit in memory and no local volume can be
  mounted.

The decision is recorded in [RFD 017](rfd/017-residency-tiers.md).

## Choosing a level

A table's `lookups` is the lookup cost its owner accepts. A level is
stated in what a lookup costs, so it means the same on every table:

- `optimal`: a hit costs one GET and a miss none, and a lookup probes one
  file and decodes one key page of ordinary size.
- `balanced`: a lookup still probes one file, with a fixed extra cost: an
  extra GET per hit, a GET for a miss the bloom filter passes, or larger
  key pages.
- `permissive`: any snapshot icetray serves correctly, including one where
  a lookup probes more than one file.

The level a snapshot achieves is the costliest level that its residency
or any of its findings caps it at. `bloom` residency caps it at
`balanced`; the findings that cap it, and at which level, are in
[the table contract](table-contract.md#requirements). Advice caps
nothing. The doctor prints the level as `level`, and under `level_by_residency` the
level each residency would reach with the same files, beside what each
costs under `predicted`.

The line between `balanced` and `permissive` is the one that matters for
operations. A cost within one file follows from how the files are written
and stays put. Extra files probed come from how the table is committed
to, and they grow with every append until a compaction removes them.

Choose the level by what the table's readers and its writer can live
with:

- Keep the default, the best level the residency reaches, when the
  table's lookups sit on a latency-sensitive path and a table that gets
  slower should alert its owner.
- Declare `balanced` for a table whose writer cannot drop the value
  dictionary or shrink its key pages. Left at the default, such a table
  reads below its level from its first load
  ([What to alert on](operating.md#what-to-alert-on)).
- Declare `permissive` for a table that takes appends with no compaction
  schedule, accepting lookups that slow down until a compaction runs.

`strict` decides what falling below the level does. Off, the default, the
snapshot serves and the table is flagged. On, a snapshot whose layout is
below the level is refused as `below_level`: at start `serve` exits 1, and
on a refresh the last snapshot that met the level keeps serving and the
table is held ([Refresh and reload](operating.md#refresh-and-reload)). A
strict table's level is judged before any key chunk is fetched, from the
manifests, the footers, and the first and last key page of each new file
that shares a bucket with another, with the key's dictionary page when it
has one, so a refused snapshot writes nothing to the local directory.

The decision is recorded in [RFD 020](rfd/020-lookup-levels.md).

## The command line

```
icetray <COMMAND>

Commands:
  serve   Run the gRPC server
  doctor  Check tables against the table contract
  get     Look up keys in one table
  info    Describe each table's current snapshot
```

`-h` prints a command's summary and `--help` its full help. `icetray
--help` ends with the environment variables, the exit statuses, and
examples. `icetray --version` prints the version.

```sh
icetray serve --config icetray.toml
icetray doctor --config icetray.toml
icetray info --config icetray.toml --table kv
icetray get --config icetray.toml --table kv 42 43
icetray get --location s3://warehouse/db/kv 42
```

- `serve` takes `-c, --config <PATH>` and nothing else. It is described
  in [Operating icetray](operating.md).
- `doctor` loads each table, checks it against the
  [table contract](table-contract.md), and prints a `table` line, a
  `finding` line per finding, and a `refusal` line for a table that cannot
  be served. `--files` adds a `file` line per data file. The doctor
  decodes and hashes every key to check that it sits in its file's bucket,
  which a load does not, so it takes longer than `info` on the same table.
- `info` loads each table and prints its `table` line.
- `get` loads one table and prints a `lookup` line per key, in the order
  given.

### Choosing the tables

The one-shots share one set of flags, grouped in their help as follows.

**Table selection**

| Flag | Meaning |
|---|---|
| `-c, --config <PATH>` | The configuration file. |
| `--location <URI>` | One table opened without a file; see below. Cannot be combined with `--config`. |
| `--rewrite <FROM=TO>` | A location prefix rewrite, repeatable, under either target. |
| `-t, --table <NAME>` | Selects a configured table, so it needs a configuration file. On `doctor` and `info` it limits the run to that table; on `get` it is required when the file lists more than one. |

**Located table (with --location)**

| Flag | Default | Meaning |
|---|---|---|
| `--key-column <COLUMN>` | `key` | The key column. |
| `--value-column <COLUMN>` | `value` | The value column. |
| `--ref <REF>` | `main` | The branch or tag to read. |
| `--residency <RESIDENCY>` | `full` | `full` or `bloom`. `disk` needs a local directory and is available through a configuration file. |

**Output** (`doctor` only)

| Flag | Meaning |
|---|---|
| `--files` | Also print one `file` line per data file. |

The configuration file comes from `--config`, or, with neither `--config`
nor `--location`, from the `ICETRAY_CONFIG` environment variable. An empty
`ICETRAY_CONFIG` counts as unset. A `--location` run reads no
configuration file, not even one the variable names. With no flag and no
variable, the run exits 64 with
`no configuration: pass --config or --location, or set ICETRAY_CONFIG`.
A file that cannot be read, or that does not parse or validate, is named
in the error with where its path came from, `--config` or
`ICETRAY_CONFIG`.

A `--table` name that no configured table has exits 64, and so does
`--table` given with `--location`. Both errors print a hint: the first
lists the configured names, and the second says to drop `--table` or to
pass `--config` instead of `--location`.

### One table without a file

`--location` takes a table root or a metadata file, as a URI or as a
local path. An empty `--location`, a `file:` URI with a relative path or
another host, and a location no store serves exit 64. A relative local
path is resolved against the working directory, so
`--location target/golden-warehouse/db/long_2m` works from the repository
root. The table is named after the table root's last directory: `kv` for
`s3://warehouse/db/kv` and for
`s3://warehouse/db/kv/metadata/v3.metadata.json`. A location with no
directory to name it after, such as a bare bucket, opens as `table`.

A located table runs with every `[server]` and `[storage]` default. Its
`lookups` is the best level its `--residency` reaches, `optimal` for
`full` and `balanced` for `bloom`, and it is never strict, so the doctor
reports a located table below that level with exit 2 rather than refusing
it.

```sh
icetray doctor --location s3://warehouse/db/kv
icetray get --location s3://warehouse/db/kv/metadata/v3.metadata.json 42
icetray info --location target/golden-warehouse/db/long_2m
```

### Rewrite rules on the command line

`--rewrite FROM=TO` splits at the first `=`, and both halves must be non
empty: `--rewrite =x` or `--rewrite x` exits 64, naming the flag and the
value. A `TO` that is a relative local path is resolved against the working
directory. Under `--config`, the command line's rules go ahead of the
file's, so they win for the same prefix. `serve` takes no `--rewrite`;
its rules come from the file.

### Keys

Keys are given in the form for the table's key type:

- a long as its digits, with a leading `-` when negative;
- a string as its text;
- a binary or fixed key as hex, two digits per byte in either case, with
  or without the `0x` a lookup line prints;
- a uuid hyphenated or as 32 hex digits.

A [`lookup` line](#lookup)'s `key` is in this form, so passed back to
`get` it looks up the same key.

A key that starts with `-` and is not a number goes after `--`:
`icetray get --location ... -- -abc`. `get` reads the key type from the
table's metadata and checks every key against it before it loads any data
file, so a key that does not parse exits 64 at once, with a hint giving
the form.

## Output

The one-shots print JSON on stdout, one object per line, and nothing
else. Each line's first key is `kind`, which names what the line
describes, and its second is `table`. Warnings and other log events go to
stderr as JSON lines. A run's final failure goes to stderr as plain text:

```
error: no table named nope is configured
  hint: pass --table with one of: long_2m, str_2m
```

The `hint` line appears when the fix is plain from the failure. When a
reader closes stdout early, as `head` does, the run stops printing and
exits 0.

### The output contract, version 1

The line kinds and keys below are version 1 of the output contract.
Within a version, new keys and new line kinds may be added, so a reader
should ignore what it does not recognize. Removing or renaming a key,
changing a value's type or meaning, or removing a line kind makes a new
version, which this page will name.

Two conventions hold for every line:

- A snapshot id is a decimal string, `"7128216514704790069"`, because an
  id above 2^53 loses digits as a JSON number in most readers.
- A key that does not apply is `null` rather than absent, except where a
  line kind below says otherwise.

#### `table`

One per table, from `info` and from `doctor`:

```json
{"kind":"table","table":"long_2m","snapshot_id":"7128216514704790069","level":"optimal","lookups":"optimal","residency":"full","file_count":4,"buckets":4,"buckets_with_files":4,"max_files_per_bucket":1,"records":2000000,"index_bytes":2623999,"predicted":{"full":2623999,"disk_memory":239082,"disk_bytes":2405869,"bloom":4433458,"bloom_bits_per_key":16.777504,"key_chunk_bytes":2384917,"filter_bytes":4194376},"level_by_residency":{"full":"optimal","disk":"optimal","bloom":"balanced"},"lookup_cost":{"gets_per_hit":1,"gets_per_miss":0,"files_probed":1},"capping":0,"advice":0}
```

| Key | Value |
|---|---|
| `snapshot_id` | The snapshot loaded. |
| `level` | The [level](#choosing-a-level) the snapshot achieves in the loaded residency: `optimal`, `balanced`, or `permissive`, the costliest level that the residency or any finding caps it at. |
| `lookups` | The level the table declares: its configured `lookups`, or for a located table the best level its residency reaches. |
| `residency` | The residency the table was loaded in. |
| `file_count` | Live data files in the snapshot. |
| `buckets` | The bucket count of the key's bucket transform, `null` for a table without one. |
| `buckets_with_files` | Buckets holding at least one file. |
| `max_files_per_bucket` | Files in the fullest bucket. |
| `records` | Rows across every file, from the manifests. |
| `index_bytes` | Resident bytes of the index in the loaded residency. |
| `predicted.full` | Memory the `full` residency would hold. |
| `predicted.disk_memory` | Memory the `disk` residency would hold. |
| `predicted.disk_bytes` | The chunk files the `disk` residency would write, as `index_budget_bytes` counts them. |
| `predicted.bloom` | Memory the `bloom` residency would hold; `null` when a file has no key bloom filter. |
| `predicted.bloom_bits_per_key` | Filter bits per row; `null` when `predicted.bloom` is. |
| `predicted.key_chunk_bytes` | The key column's compressed bytes. |
| `predicted.filter_bytes` | The key column's bloom filter bytes; `null` when `predicted.bloom` is. |
| `level_by_residency` | For each residency that can load the table, the level its files would reach there. A residency that cannot load the table is left out, as `bloom` is when `predicted.bloom` is `null`. |
| `lookup_cost.gets_per_hit` | GETs one cold hit makes at worst in the loaded residency, as `icetray_table_gets_per_hit` reports it while the snapshot serves. |
| `lookup_cost.gets_per_miss` | GETs one miss makes at worst, as `icetray_table_gets_per_miss` reports it. |
| `lookup_cost.files_probed` | Files one lookup probes at worst, as `icetray_table_files_probed_per_lookup` reports it. |
| `capping` | The number of findings that cap the level. |
| `advice` | The number of advice findings. |

#### `finding`

One per finding, from `doctor`, capping findings first:

| Key | Value |
|---|---|
| `caps` | The level the finding caps the table at, `balanced` or `permissive`; `null` for advice. |
| `code` | The finding's code, such as `large_key_pages`; the codes are in [the table contract](table-contract.md#requirements). |
| `message` | What was measured, for a person. |
| `file` | The data file's location, or `null` for a table-level finding. |
| `penalty` | `null` for advice. For a capping finding, an object of `extra_gets_per_hit`, `keys_decoded_per_lookup`, and `files_probed_per_lookup`, each a number or `null` when the finding does not affect it. |
| `property` | The writer property that fixes it, or `null`. A per-column property is printed up to the column name, such as `write.metadata.metrics.column.`. |

#### `file`

One per data file, from `doctor --files`:

| Key | Value |
|---|---|
| `path` | The file's location as the manifest records it. |
| `bucket` | The bucket the file holds. |
| `key_type` | `long`, `string`, `binary`, or `uuid`. |
| `rows`, `row_groups` | Rows and row groups in the file. |
| `key_pages`, `value_pages` | Data pages of each column across every row group. |
| `max_key_page_rows` | Rows in the largest key page: the keys a lookup there decodes. |
| `max_key_bytes` | The longest key, in bytes. |
| `avg_value_page_bytes` | Mean uncompressed bytes of a value page: what a cold hit decompresses. |
| `avg_value_bytes` | Mean uncompressed bytes of one value. |
| `pages_aligned` | Whether every value page starts on the same row as a key page. |
| `dictionary_columns` | The columns, key or value, that a row group dictionary-encodes. |
| `bloom_bytes`, `bloom_bits_per_key` | The key's bloom filter size and bits per row; `null` when a row group has none. |
| `key_chunk_bytes` | The key column's compressed bytes. |
| `distinct_page_mins_ratio` | The share of key pages whose column-index minimum is distinct; low when the column index is truncated. |
| `bounds_present` | Whether the manifest carries full-length key bounds. |
| `resident_bytes` | The bytes this file's index holds in the loaded residency. |

#### `refusal`

One per table the doctor refused, after any `table` and `finding` lines
for it:

```json
{"kind":"refusal","table":"unpart","reason":"below_level","message":"table unpart refused: below_level (lookups optimal, the layout achieves permissive: no_bucket_partition)"}
```

| Key | Value |
|---|---|
| `reason` | A refusal reason from [the table contract](table-contract.md#requirements) (`unsorted`, `null_keys`, `duplicate_keys`, `key_outside_bucket`, `key_type_mismatch`, `key_type_unsupported`, `value_type_unsupported`, `delete_files`, `key_too_long`, `over_budget`, `no_bloom_filter`); `below_level` for a strict table whose layout is below its `lookups`, with the codes that cap it in `message`; or `load_failed` when an error rather than a rule stopped the load. |
| `error_kind` | Present only with `load_failed`: what failed, one of `iceberg`, `parquet`, `object_store`, `object_store_path`, `json`, `config`, `location`, `cache`, `key_type`, `table`, `file`, `not_ready`, `local`, `catalog_timeout`, `catalog`, `load_deadline`, `refresh_deadline`, `verify_canceled`, `lookup_canceled`. |
| `message` | The error, for a person. |

`reason` and `error_kind` use the same names as the `reason` label of
`icetray_refresh_errors_total`.

#### `lookup`

One per key, from `get`:

```json
{"kind":"lookup","table":"long_2m","key":"43","presence":"absent","value_bytes":0,"snapshot_id":"7128216514704790069","gets":0,"elapsed_micros":0}
```

| Key | Value |
|---|---|
| `key` | A string holding the key in the form `get` takes for it ([Keys](#keys)): a long as its digits, with a leading `-` when negative; a string as its text, with no escapes but JSON's own (`"abc"`, so `jq -r .key` gives `abc`); a binary key as `0x` and lowercase hex; a uuid hyphenated. Passed back to `get`, it looks up the same key. |
| `presence` | `present`, `null` (the key is in the table with a null value), or `absent`. |
| `value_base64` | The value in standard base64 with padding. Present only when `presence` is `present`. |
| `value_utf8` | The value as text. Present only when the value is present and its bytes are UTF-8. |
| `value_bytes` | The value's length in bytes; `0` for a null or absent key. |
| `snapshot_id` | The snapshot the answer came from. |
| `gets` | The GETs this lookup issued. |
| `elapsed_micros` | The lookup's time, in microseconds. |

## Exit codes

The same codes apply to every command; the root help's footer lists them.

- **0**: the run did what was asked. An absent key is an answer, so a
  `get` of absent keys exits 0. The doctor exits 0 when every table meets
  its `lookups` level; advice does not change the code, and neither does
  a capping finding the level accepts. `serve` exits 0 after a drain that
  finished within `server.drain_deadline`.
- **1**: the run failed. A configuration file that cannot be read, is not
  TOML or JSON, does not fit the schema (a key missing, unknown, or of the
  wrong type), or breaks a rule (no table listed, a `lookups` level its
  residency cannot reach, a rewrite rule with an empty half, an empty or
  unservable location, a bad log field mapping, and the rest); a relative
  path on the command line with no readable working directory; a load or
  read error; a table refused, which for the doctor is a `refusal` line,
  `below_level` under `strict` among them, and takes precedence over 2;
  and for `serve`, a first load that failed, a listener that would not
  bind, or a drain that passed its deadline.
- **2**: the doctor found a table below its `lookups` level that it did
  not refuse: a configured table with `strict` off, the default, or a
  `--location` table, which is never strict. A strict table below its
  level is refused instead, exit 1, as `serve` would refuse it.
- **64**: the command line could not be used: no configuration; both
  `--config` and `--location`; a `--location` that is empty, a `file:` URI
  with a relative path or another host, or one no store serves; a
  located-table flag without `--location` or with `--config`; a
  `--residency` other than `full` or `bloom`; a malformed `--rewrite`;
  `--table` with `--location`; a `--table` that no configured table has;
  `get` without `--table` when several tables are configured; or a key
  that does not parse.