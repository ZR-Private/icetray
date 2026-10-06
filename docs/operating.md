# Operating icetray

Terms such as generation, residency, and degraded are defined in the
[glossary](glossary.md). The trade-offs behind the sizing on this page,
and the settings that move them, are in
[Performance and tuning](performance.md).

## Starting and stopping

```sh
icetray serve --config icetray.toml
ICETRAY_CONFIG=/etc/icetray/icetray.toml icetray serve
```

`serve` takes only the configuration path, from `-c, --config` or from
`ICETRAY_CONFIG`; an empty variable counts as unset. Every setting is in
the file ([Configuration](configuration.md)).

- With neither, `serve` exits 64 with
  `no configuration: pass --config or set ICETRAY_CONFIG`.
- A file that cannot be read, or that does not validate (as a file that
  lists no table does not), is reported on stderr as plain text, and
  `serve` exits 1 before anything is opened. Either message names the
  file and whether its path came from `--config` or `ICETRAY_CONFIG`, and
  an invalid file's also names the setting by its full path and the value
  given:

  ```
  error: invalid configuration file /etc/icetray/icetray.toml (from ICETRAY_CONFIG): server.poll_interval 0s is below the minimum of 1s
  ```
- Once the file is read, logging starts. Every later failure is logged as
  a JSON event on stdout with the message `run failed` and the error in
  `error`.

Start binds the gRPC and metrics listeners, registers gRPC health as
`NOT_SERVING`, and loads every table concurrently. The listeners accept
while tables load.

- Once every table serves, health flips to `SERVING` and `/ready` on the
  metrics listener answers 200. Until then it answers 503.
- A table that fails its first load is fatal: the reason is logged, health
  stays `NOT_SERVING` for ten seconds, and the process exits 1. A strict
  table whose first snapshot is below its `lookups` level fails its first
  load this way, refused as `below_level`, since there is no earlier
  snapshot to hold, and so does a load that runs past
  `server.load_deadline`.
- SIGTERM or SIGINT starts a drain. Health turns `NOT_SERVING`, the gRPC
  listener stops accepting, and the metrics listener closes. The drain
  waits up to `server.drain_deadline` for open calls, then flushes the
  page cache's disk level and exits 0. Past the deadline, the page cache
  is still flushed and the process exits 1.
- A signal during the first loads ends them and drains.
- SIGHUP reloads the configuration file
  ([Refresh and reload](#refresh-and-reload)).

`serve` exits with:

- 0 after a drain that finished within `server.drain_deadline`;
- 1 for a configuration that cannot be read or validated, a first load
  that failed, a listener that would not bind, a page cache that could not
  open (as when the [descriptor limit](#file-descriptors) is short), or a
  drain that passed its deadline;
- 64 for no configuration or a command line it cannot parse.

## The gRPC contract

The client contract is `crates/icetray-proto/proto/icetray.proto`.
Clients are generated from it; the server offers no reflection, so a tool
such as grpcurl is given the file with `-proto`. Three calls:

- `Get` looks one key up in one table and returns a presence
  (`PRESENCE_PRESENT` with the value's bytes, `PRESENCE_NULL`, or
  `PRESENCE_ABSENT`) and the snapshot id it was read from.
- `MultiGet` answers many keys in request order from one snapshot, named
  once on the response. If any key cannot be answered the whole call
  fails.
- `TableInfo` reports the served snapshot id, bucket count, data file
  count, row count, key type, and resident index bytes.

Keys, errors, and limits:

- A key is a oneof of `long`, `str`, and `bin`; `bin` carries binary,
  fixed, and uuid keys. The wrong variant is `INVALID_ARGUMENT`.
- An unserved table is `NOT_FOUND`. A miss is a normal answer.
- A `Get` or `MultiGet` still running at `server.get_deadline` or
  `server.multi_get_deadline` is `DEADLINE_EXCEEDED`. A client-sent
  deadline is enforced by the gRPC layer (a tonic client reports
  `CANCELLED`); the server drops the call then.
- `Get` and `MultiGet` share `server.max_in_flight` slots, each held until
  its call ends. A call that finds none free is `RESOURCE_EXHAUSTED` at
  once. `TableInfo` takes no slot.
- A `MultiGet` of more than `server.max_multi_get_keys` keys is
  `INVALID_ARGUMENT` before any lookup.
- `UNAVAILABLE` is a failure a retry can clear: an object-store or local
  read that failed, a table not yet ready, a canceled lookup.
- `INTERNAL` is a failure of the data itself, such as a malformed file or
  a value page over the 64 MiB ceiling. A retry against the same snapshot
  fails the same way.

The listener speaks plaintext gRPC and authenticates no one. TLS,
authentication, and access policy are the platform's: run it on a private
network or behind a mesh or sidecar, never exposed directly. The default
listen addresses are loopback.

## Metrics

The metrics listener serves `/metrics` in the Prometheus text format and
`/ready`. Every metric has a `# HELP` line. Every label value comes from a
closed set, listed below.

Counters and histograms:

| Metric | Labels | Exposed from start | What it counts |
|---|---|---|---|
| `icetray_requests_total` | `call`, `code` | every call and code | gRPC calls answered, each once, by the status its client received. |
| `icetray_object_gets_total` | `kind` | every kind | Object-store GETs, hedges included. |
| `icetray_object_bytes_total` | `kind` | every kind | Bytes those GETs requested. |
| `icetray_local_reads_total` | | yes | Reads from the local directory. |
| `icetray_local_bytes_total` | | yes | Bytes of those reads. |
| `icetray_hedges_total` | | yes | Second GETs sent because a lookup GET was slow. |
| `icetray_page_cache_hits_total` | | yes | Value pages served from the page cache. |
| `icetray_page_cache_fetches_total` | | yes | Value-page fetches the cache ran, after coalescing. |
| `icetray_lookups_total` | `table`, `result` | every table and result | Lookups answered. |
| `icetray_refresh_errors_total` | `table`, `reason` | no: from the first error | Refreshes that failed and left the previous generation serving. A failure the table remembers is counted once, when met ([Refresh and reload](#refresh-and-reload)). |
| `icetray_generation_swaps_total` | `table` | every table | Generations a refresh swapped in. The first load is not counted. |
| `icetray_config_reloads_total` | `result` | every result | Reloads that read a changed configuration file. |
| `icetray_lookup_seconds` (histogram) | `table`, `result` | every table and result | Lookup latency. |
| `icetray_object_get_seconds` (histogram) | `kind` | every kind | Object-store GET latency. |

The histograms' buckets run from 100 microseconds to 2 seconds.

`icetray_requests_total` counts every call to `Get`, `MultiGet`, and
`TableInfo`, including those the gRPC layer answers before icetray's
handler runs: a body that does not decode (`internal`), a message over the
4 MiB limit (`out_of_range`), and an encoding the server does not take
(`unimplemented`). A call dropped before it answers, as when its client
goes away or its `grpc-timeout` passes, counts as `cancelled`, and an
answer whose body ends without a status counts as `unknown`. A call to a
method outside the contract is not counted.

Gauges, written at each scrape:

| Metric | Labels | Value |
|---|---|---|
| `icetray_requests_in_flight` | | `Get` and `MultiGet` calls admitted and not yet ended. |
| `icetray_max_in_flight` | | `server.max_in_flight`. A call that finds every slot held is `RESOURCE_EXHAUSTED`. |
| `icetray_table_state` | `table`, `state` | 1 for the table's current state, 0 for the others. |
| `icetray_table_below_level` | `table` | 1 while the newest snapshot the table measured, the one serving or, while the table is held, the one it refused, is below the table's `lookups` level, else 0. |
| `icetray_table_level` | `table` | The [level](configuration.md#choosing-a-level) that snapshot's layout achieves: 0 for `optimal`, 1 for `balanced`, 2 for `permissive`. |
| `icetray_table_declared_level` | `table` | The table's `lookups`, in the same numbers. |
| `icetray_table_gets_per_hit` | `table` | GETs one cold hit makes at worst on the serving generation's layout and residency: the value page, a dictionary-encoded value column's dictionary page, and under `bloom` the key page of every file probed, with a dictionary-encoded key column's dictionary page. Counted before hedges, unlike `icetray_object_gets_total`. Observed traffic is in `icetray_lookups_total`, where warm hits read below the prediction. |
| `icetray_table_gets_per_miss` | `table` | GETs one miss makes at worst on the serving generation's layout and residency: 0 under `full` and `disk`, and under `bloom` the key page of every file probed, each filter answering yes, with a dictionary-encoded key column's dictionary page. Counted before hedges, unlike `icetray_object_gets_total`. |
| `icetray_table_files_probed_per_lookup` | `table` | Files one cold lookup probes at worst on the serving generation: the files its key routes to whose exact ranges cover it, 1 where no files overlap. On a table whose files were written under two or more bucketings it is an upper bound. |
| `icetray_generation_age_seconds` | `table` | Seconds since the serving snapshot was committed, by its timestamp. A table written once a day reads up to a day between writes; it is not a replication lag. |
| `icetray_generation_sequence_number` | `table` | The serving snapshot's sequence number. |
| `icetray_generation_swapped_at_seconds` | `table` | When the serving generation was swapped in, in Unix seconds. |
| `icetray_findings` | `table`, `code` | The serving generation's findings of each code, 0 for a code it does not raise. |
| `icetray_index_bytes` | `table` | The resident index's live bytes. |
| `icetray_page_cache_bytes` | | Bytes the page cache's memory level holds against `server.page_cache_bytes`, after evictions. |
| `icetray_config_pending` | | 1 while the configuration file differs from the configuration applied, else 0. |
| `icetray_process_rss_bytes` | | The process's resident set size. |
| `icetray_allocator_allocated_bytes`, `_active_bytes`, `_metadata_bytes`, `_resident_bytes`, `_retained_bytes` | | jemalloc's own counts, which split the resident set. `allocated` is live data; `active` minus `allocated` is free space inside pages in use; `resident` minus `active` and `metadata` is freed memory not yet returned to the system; `retained` is address space only. |

The three level gauges appear with the table's first swap, like the
generation gauges. A snapshot refused as `below_level` was measured, so
they take its level; a snapshot refused for any other reason leaves them
as they were. A reload that changes `lookups` sets
`icetray_table_declared_level` at once.

The label values:

- `call`: `get`, `multi_get`, `table_info`.
- `code`: the gRPC status in lower snake case, every one of the 17 gRPC
  codes. icetray's handlers answer `ok`, `invalid_argument`,
  `deadline_exceeded`, `not_found`, `resource_exhausted`, `unavailable`,
  and `internal`; the gRPC layer adds `cancelled`, `out_of_range`,
  `unimplemented`, and `unknown`, as above.
- `kind`: `load` (footers, key chunks, and metadata read while a
  generation is built) and `lookup` (value pages, and key pages under
  `bloom` residency).
- `result` of `icetray_lookups_total` and `icetray_lookup_seconds`:
  `cold_hit` (the value page was fetched), `warm_hit` (the page cache
  served it), `miss`.
- `result` of `icetray_config_reloads_total`: `applied`, `refused`.
- `state`: `loading`, `serving`, `degraded`, `held` ([Refresh and
  reload](#refresh-and-reload)).
- `code` of `icetray_findings`: `dictionary_encoded_value`,
  `large_key_pages`, `no_bucket_partition`, `overlapping_files`,
  `bounds_missing`, `column_index_truncated`, `large_value_pages`,
  `value_pages_over_ceiling`, `long_keys`, `large_file`,
  `many_files_per_bucket`, `no_bloom_filter`, `bloom_oversized`,
  `bloom_undersized`.
- `reason` of `icetray_refresh_errors_total`: a refusal reason
  (`unsorted`, `null_keys`, `duplicate_keys`, `key_outside_bucket`,
  `key_type_mismatch`, `key_type_unsupported`, `value_type_unsupported`,
  `delete_files`, `key_too_long`, `over_budget`, `no_bloom_filter`,
  `below_level`), or an error kind (`iceberg`, `parquet`, `object_store`,
  `object_store_path`, `json`, `config`, `location`, `cache`, `key_type`,
  `table`, `file`, `not_ready`, `local`, `catalog_timeout`, `catalog`,
  `load_deadline`, `refresh_deadline`, `verify_canceled`,
  `lookup_canceled`). There are too many to expose for every table at
  start, so a series appears with its first error; alert on
  `icetray_table_state` instead ([What to alert on](#what-to-alert-on)). A
  table stuck behind a refused snapshot counts the failure once, so its
  rate falls to zero while the table stays degraded or held.

A table a reload adds gets its series when it is added. A table a reload
removes loses its gauges at the next scrape; its counters and histograms
stay until the process restarts. The snapshot id appears in the swap's log
line and in `TableInfo` but in no metric, since as a gauge it would lose
digits and as a label it would add a series with every swap.

## Logs

`serve` logs JSON on stdout, one object per line. The standard fields are
`time` (RFC 3339, UTC), `level`, `message`, and `target`, and each event
adds its own, such as `table`, `snapshot_id`, or `error`.
`server.log.fields` renames the standard fields and
`server.log.static_fields` adds constant fields to every event
([Configuration](configuration.md#serverlog)). A bad mapping is refused
with the rest of the configuration, with exit 1.

With `RUST_LOG` unset, the filter passes icetray's own events at `info`
for `serve` and at `warn` for a one-shot, and every other crate's at
`warn`, except the AWS SDK's profile normalizer
(`aws_runtime::env_config::normalize`), held to `error` because it warns
about every profile it cannot use. The SDK's warning that a file named by
`AWS_CONFIG_FILE` or `AWS_SHARED_CREDENTIALS_FILE` does not exist still
shows. A valid `RUST_LOG`, such as `debug` or `icetray_core=debug`,
replaces the default filter whole; an empty, blank, or invalid one counts
as unset.

Events worth knowing:

- `open-file limit`, at start, with the `limit` in force, and a warning
  with `limit` and `needed` when that is short
  ([File descriptors](#file-descriptors)).
- `listening`, with both addresses and the table count; then
  `table serving` per table as its first load completes.
- `swapped in a generation`, with the snapshot id and the files reused,
  on every load and refresh.
- A finding on a load, with `code`, `caps` (absent for advice), and
  `file`: at `warn` for a finding whose cap is below the table's
  `lookups` level, and at `info` for the rest, advice included.
- `configuration reloaded`, at `info`, with the file's `digest`, the
  tables `added`, `changed`, `retuned`, and `removed` by name, and the
  count `kept`.
- `configuration reload refused`, at `warn`, with the `reason`, after a
  `table refused for a reload` for each table refused
  ([Refresh and reload](#refresh-and-reload)).
- `draining`, when a signal starts the drain.
- `run failed`, at `error`, with the `error` that ended the run.
- `jemalloc background purging is unavailable`, a warning on Linux only.

The one-shots log the same JSON to stderr at `warn`, so their stdout stays
data, and they report their final failure as plain text, `error:
<message>` with an optional `  hint: <fix>` line. A write to a closed
stderr is dropped, and the run's exit code stands. The doctor's findings
are its output and are not logged; `info` and `get` log at `warn` the
findings that put a table below its `lookups` level.

## The local directory

`storage.local.dir` names an optional local directory. It holds:

- The chunk files of tables with `residency = "disk"`: each data file's
  key chunks, while its footer, page indexes, and fence array stay in
  memory.
- The disk level of the value-page cache, bounded by
  `storage.local.page_cache_disk_bytes`. Zero turns the level off; any
  other value is at least one 4 MiB block.

`index_budget_bytes` bounds the chunk files across every table, measured
when a load begins. The serving generation's chunk files stay until the
new one has swapped in, so a table rewritten wholesale wants about twice
its steady footprint, the doctor's `predicted.disk_bytes`.

A lookup in a `disk` table costs one local read and, on a cold hit, one
GET. The local read is a memory copy while the kernel's page cache holds
the chunk file, and a device read when it does not, so a `disk` table
serves at memory speed only with room for its chunk files in that cache
([Memory](#memory)). On Linux, put the directory on a mounted volume with a
filesystem such as ext4; a container's overlay filesystem forces every
local read onto another thread.

Everything in the directory is a copy of bytes in object storage, checked
against its data file's identity and rebuilt on demand. A restarted
replica reuses every chunk file whose data file is still referenced and
deletes the rest. The directory needs only capacity: it is not backed up
or shared between replicas, and losing it costs one cold load.

The trade-offs between `full`, `disk`, and `bloom` are in
[Choosing a residency](configuration.md#choosing-a-residency).

## File descriptors

A replica holds these open at once:

- one chunk file per data file of each `disk` table, for the life of its
  generation, and twice that while a refresh that rewrote the table loads
  beside the serving generation;
- one descriptor per 4 MiB block of the page cache's disk level, for the
  life of the process: `page_cache_disk_bytes` / 4 MiB, 2,048 for 8 GiB;
- up to 128 reads per worker thread, at most 1,024, for each table that is
  loading, each read a local file or an S3 socket. Every configured table
  loads at once at start, so the start holds that many for each table;
- client connections, the object store's and the catalog's connection
  pools, the standard streams, and the one chunk file retention opens at a
  time.

Size the limit to at least twice the data files of `disk` tables, plus
`page_cache_disk_bytes` / 4 MiB, plus one load's reads (1,024 on a machine
of eight or more worker threads) for each configured table, plus headroom
for the connections. At start icetray raises its soft limit to the hard
limit, so the hard limit is the one to set: `ulimit -Hn`, systemd's
`LimitNOFILE`, or the container runtime's `nofile` limit. Kubernetes has no
per-pod setting, so the container runtime's or the node's limit applies.

`serve` logs the limit in force at start. It warns, with `limit` and
`needed`, when the limit is below what the disk level and a load of every
configured table need (those terms plus 256); the chunk files of `disk`
tables come on top and are not known until their tables load. The page
cache opens before any table loads, so a limit too low for the disk level
alone (its blocks plus 256) fails there, with exit 1:

```
page cache: open-file limit 1500 is below the 2304 descriptors the disk level needs: Too many open files (os error 24)
```

## Refresh and reload

Each table's ref is re-resolved every `server.poll_interval`; what a poll
reads depends on the table's source
([Configuration](configuration.md#tablessource)).

- A poll that finds the metadata unchanged costs one read or one catalog
  call.
- A poll that finds a new snapshot builds the generation beside the
  current one, loading only the data files the previous generation did
  not hold, then swaps it in. `icetray_generation_swaps_total` counts it.
- A refresh that fails leaves the last good generation serving, counts
  `icetray_refresh_errors_total`, logs `refresh failed` at `warn`, and
  moves the table to degraded, or to held for a `below_level` refusal.
  Readiness is unchanged.
- A strict table whose new snapshot is below its `lookups` level refuses
  it as `below_level`, and the last snapshot that met the level keeps
  serving. The table is held, not degraded, since a writer's layout put
  it there and no fault did: `icetray_table_state{state="held"}` reads 1,
  and so does `icetray_table_below_level`, which measures the refused
  snapshot. Held ends at the next swap, as when a compaction commits a
  snapshot that meets the level, or at a poll that finds the ref back on
  the serving snapshot. A reload that relaxes `lookups` to a level the
  refused snapshot meets, or turns `strict` off, ends it too, and the next
  poll swaps that snapshot in. A transient failure of a poll while held
  makes the table degraded until a poll finds the source unchanged, which
  makes it held again.
- A failure of the snapshot itself is remembered. That is any refusal
  reason, `below_level` included, except `over_budget` against
  `storage.local.index_budget_bytes`. It is a `table` fault of the
  snapshot's metadata: a ref, schema, partition spec, or column it lacks,
  zero buckets, or a data file with no usable bucket value or one out of
  range. It is a `file` fault that a data file's footer or bloom filters
  show. These depend only on the snapshot and the table's entry, so trying
  again cannot help. The failure is counted and logged once, when it is
  met, and the table does not build that snapshot again while its source
  names the same metadata location: each poll costs only the one read or
  catalog call above, counts nothing, logs nothing, and the table stays
  degraded, or held. When the source names another metadata location,
  its snapshot is built. Once the table serves it, or refuses it in turn,
  `source moved past a refused snapshot` is logged at `info` with the
  `reason` and, when the ref named one, the refused `snapshot_id`. A
  reload that changes a setting of the table's entry other than `lookups`
  and `strict` loads the table anew under the new entry, the refused
  snapshot included, so fixing `ref` or a column name or raising
  `resident_budget_bytes` takes effect without waiting for a new snapshot;
  if the snapshot is still refused, the reload is refused. A reload that
  changes only `lookups` or `strict` retunes the table in place and loads
  nothing. It forgets every remembered refusal, whatever its reason, and
  logs `retune forgot a refused snapshot` at `info`, so the next poll
  builds the snapshot the ref names under the new entry. A snapshot
  refused for another reason than its level is then refused once more,
  and remembered again.
- Every other failure is tried again on every poll and is counted and
  logged each time: a store, catalog, cache, or local-directory error, a
  timeout or deadline, `over_budget` against the shared
  `index_budget_bytes`, which other tables' chunk files also decide, a
  version hint or catalog record that names no usable metadata, a
  `parquet` or `iceberg` library error, and a `file` fault met decoding a
  data file's pages, which a `disk` table may read from its local chunk
  file rather than the data file.
- A `glue` or `s3tables` call that the catalog refuses or that fails, for
  a missing permission, a table that does not exist, throttling, or an
  endpoint that cannot be reached, is a `refresh failed` event at `warn`
  with `reason` `catalog` and an `error` that names the table as the
  catalog does (`db.kv`, or `ns.kv in <table bucket ARN>`) and carries the
  service's error code. A call that runs past `storage.catalog_timeout`
  has `reason` `catalog_timeout`. Each is counted under its reason in
  `icetray_refresh_errors_total`. A Glue table that is not Iceberg, or a
  table without a metadata location, has `reason` `table`. The same
  failure on a first load ends `serve` with `run failed` and exit 1, and
  refuses a reload.
- Snapshot expiry removes a snapshot by its age, and a replica still
  serving an expired snapshot reads data files that are gone. Keep the
  retention on the served ref longer than the largest
  `icetray_generation_age_seconds` a replica can reach while a newer
  snapshot exists, as when its table is degraded. The snapshot a ref
  currently names is never expired.

The configuration file is read on the same interval, and at once on
SIGHUP. A change to its tables applies without a restart
([RFD 018](rfd/018-configuration-reload.md)).

- A table whose entry is unchanged keeps serving untouched. An entry
  that spells out the default `lookups` is unchanged from one that leaves
  it out.
- A table whose entry changed only in `lookups` or `strict` is retuned in
  place: its generation keeps serving and nothing is loaded, since the
  level is judged from the layout the generation already holds. A strict
  table whose serving snapshot is below its new level refuses the reload,
  as starting with that file would.
- Added and changed tables load beside the tables serving. Once every one
  of them serves, the whole set swaps at once, and the retuned tables take
  their new entries with it. A removed table then answers `NOT_FOUND`.
  The outcome is logged as `configuration reloaded` ([Logs](#logs)).
- A reload is refused whole when any of those tables fails to load or a
  retuned table is refused, when the file does not parse or validate (a
  file that lists no table among them), or when any setting outside
  `[[tables]]` changed; those apply on restart only. The refusal is logged
  as `configuration reload refused` and counted, and
  `icetray_config_pending` reads 1 until the file and the server agree.
  Its `reason` names what to fix:
  - for a file that does not parse or validate, the error, naming the file
    and the setting by its full path;
  - for a restart-only change, the setting, as in
    `server.listen changed, which only a restart applies`;
  - for tables refused, their names, as in `tables refused: kv`. Each
    table's own error is logged just before, as `table refused for a
    reload` with `table`, `reason`, and `error`: a load's failure, or
    `below_level` for a retuned table.

  The server keeps serving. The poll retries once the file changes; SIGHUP
  retries at once.
- Readiness does not change on a reload.
- A single-file mount fixed at start, such as a Kubernetes `subPath`, is
  never seen to change.
- Replace the file whole, by a rename. A read during an in-place write can
  see a shorter file that still parses.

## Deploying

icetray's code is the same for every deployer: anything a deployment
needs is a setting, a mounted file, or an environment variable.

- Each release publishes Linux binaries for x86-64 and arm64 and a
  container image, `ghcr.io/<owner>/icetray:<version>`, for both: the
  release binary on Ubuntu 24.04, running as the image's `ubuntu` user,
  uid 1000, with `icetray serve` as its command. The image carries
  `doctor`, `get`, and `info` too. In the image's name, `<owner>` is the
  GitHub owner of the repository that publishes the release, in lower
  case, and `<version>` is the release's tag without its `v`, such as
  `0.1.0`.
- The configuration path comes from `ICETRAY_CONFIG`, so a pod passes no
  flags. TOML and JSON read into the same schema. The default listeners
  are loopback, so a container's file sets `listen` and `metrics_listen`.
- Credentials for S3 come from the AWS SDK's default chain (environment
  variables, profiles and single sign-on, credential helpers, web
  identity, and container or instance roles, pod identity among them),
  resolved once and cached until shortly before expiry. The chain is
  loaded only when a location is `s3://` or `s3a://`
  ([Configuration](configuration.md#storages3)). Static
  keys in `storage.s3` win over the chain and are the path for
  S3-compatible endpoints, with `endpoint`, `allow_http`, and
  `path_style`. icetray has no secret format of its own.
- Calls to Glue and S3 Tables are signed with the same credentials as S3
  requests, from the same cache, so the chain is not run again for them.
  A REST catalog is called without credentials
  ([Configuration](configuration.md#tablessource)).
- The credentials need `s3:GetObject` on every location a table reads:
  its metadata files, manifest lists, manifests, and data files. A `glue`
  source also needs `glue:GetTable` on the catalog, the database, and the
  table; for a table in another account's catalog, that catalog's resource
  policy must grant it as well. An `s3tables` source needs
  `s3tables:GetTableMetadataLocation` on the table, and its files are read
  under `s3tables:GetTableData` on the table, which S3 Tables checks for
  reads of a table bucket's objects. Credentials a catalog or Lake
  Formation vends are not used, so the role's own permissions must allow
  every read.

### Memory

- Run each replica under a container memory limit. Size it as the sum,
  over the tables the replica serves, of the doctor's `predicted` bytes
  for each table's residency (`predicted.full`, `predicted.disk_memory`,
  or `predicted.bloom`), doubled for a table a refresh can rewrite
  wholesale or a reload can change; plus `server.page_cache_bytes`; plus a
  third of that total for the process, requests in flight, a load's
  transient chunks, and the allocator's fragmentation and bookkeeping. The
  `icetray_allocator_*` gauges show how much of the resident set is the
  allocator's.
- For `disk` tables, raise the limit by their chunk files' size, the
  doctor's `predicted.disk_bytes`. On Linux the page cache of the files a
  container reads counts against its memory limit, and a limit sized for
  the process alone leaves the chunk files nowhere to stay cached, so
  their reads go to the device and each lookup waits on it. The extra is
  cache the kernel reclaims under pressure, not memory the process holds:
  a scheduler's reservation, such as a Kubernetes memory request, can stay
  at the sum above.
- Load refuses a key page header, and a lookup a value page header, that
  declares more than its page can hold or more than a 64 MiB ceiling, and
  a panic inside parquet's decoding refuses the file. The lengths inside a
  delta-encoded page still size buffers from the file; there the limit is
  the backstop, and a file that trips it takes down every replica.

### CPU

A load is CPU-bound: past enough reads in flight, its time goes to
decoding and verifying key chunks, and for `disk` tables to writing chunk
files. icetray runs one tokio worker thread per core the process sees,
which under a CPU limit follows the limit in whole cores, and
`TOKIO_WORKER_THREADS` overrides it. The worker count sets the page cache's shard count and a
load's concurrency (128 reads per worker, at most 1,024), so load time and
the lookup rate both scale with it.

- As a starting point, plan about one core per few thousand lookups a
  second, then measure under the real key distribution. Warm hits on
  small values cost far less; cold hits over TLS to S3 and large values to
  decompress cost more.
- Avoid a tight CPU limit. A first load or a wholesale refresh uses every
  core it is given, and throttling stretches it toward
  `server.load_deadline` or `server.refresh_deadline`. Set the CPU request
  to the serving load and the limit well above it, or leave the limit
  off.

### The local volume

- A local volume is optional. Mount one for `disk` tables or to keep the
  page cache's disk level across restarts.
- It must be writable by uid 1000.
- In Kubernetes an `emptyDir` on the node's disk is enough, since nothing
  in it needs to outlive the pod. Avoid `medium: Memory`, which counts
  against the memory limit.

## Kubernetes

A pod spec that follows the advice above. The image is the placeholder
[Deploying](#deploying) explains, and the resource sizes are placeholders
too; the sections below say how to choose them.

```yaml
spec:
  terminationGracePeriodSeconds: 45
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    runAsGroup: 1000
    fsGroup: 1000
  containers:
    - name: icetray
      image: ghcr.io/<owner>/icetray:<version>
      env:
        - name: ICETRAY_CONFIG
          value: /etc/icetray/icetray.toml
      ports:
        - name: grpc
          containerPort: 7433
        - name: metrics
          containerPort: 9433
      startupProbe:
        httpGet:
          path: /ready
          port: metrics
        periodSeconds: 10
        failureThreshold: 190
      readinessProbe:
        httpGet:
          path: /ready
          port: metrics
        periodSeconds: 5
      livenessProbe:
        tcpSocket:
          port: metrics
        periodSeconds: 10
        failureThreshold: 3
      resources:
        requests:
          cpu: "4"
          memory: 6Gi
        limits:
          memory: 8Gi
      volumeMounts:
        - name: config
          mountPath: /etc/icetray
        - name: local
          mountPath: /var/lib/icetray
  volumes:
    - name: config
      configMap:
        name: icetray
    - name: local
      emptyDir:
        sizeLimit: 20Gi
```

**Probes.** `/ready` answers 503 until every table serves its first
generation, and 200 from then on, through refresh failures and reloads,
until a drain closes the metrics listener.

- The startup probe gives the first load its budget. Make
  `failureThreshold` times `periodSeconds` longer than
  `server.load_deadline` plus the ten seconds a failed start stays
  visible, so that a load that cannot finish ends with icetray's own error
  in the log rather than with the kubelet's kill. The example allows 1,900
  seconds against the default 30 minutes.
- The readiness probe on `/ready` keeps traffic away until the tables
  serve. A Kubernetes gRPC probe against the health service answers the
  same question.
- The liveness probe asks only whether the process answers, so it is a
  TCP check on the metrics port, whose accept loop keeps running through
  descriptor shortages. gRPC health reports whether a replica should take
  traffic: it reads `NOT_SERVING` through a first load and through a
  drain, so as a liveness check it would restart a replica that is doing
  what it should.

**Termination.** On SIGTERM icetray drains for up to
`server.drain_deadline` (20 seconds by default) and then flushes the page
cache's disk level. `terminationGracePeriodSeconds` must cover both, or
the kill cuts the flush short; Kubernetes' default of 30 seconds leaves
ten seconds for the flush. The `draining` event and the process's exit
show how long a drain and flush take. A `preStop` delay, if one is used to
let endpoints update first, adds to the time needed.

**Security context.** The image runs as uid 1000. `fsGroup: 1000` makes
the volumes Kubernetes manages ownership for, such as an `emptyDir` or
most persistent volumes, writable by that user. Without a writable volume,
a local directory the user cannot write, such as a root-owned volume or
`/var/lib/icetray` in the image's own filesystem, fails with permission
denied.

**Configuration.** Mount the ConfigMap as a directory and point
`ICETRAY_CONFIG` at the file inside it. Kubernetes updates such a mount by
swapping a symlink, which the next poll or a SIGHUP picks up. A `subPath`
mount is never updated.

**Resources.** Size the memory limit as in [Memory](#memory), with the
chunk files' page-cache headroom for `disk` tables above the request. Size
CPU as in [CPU](#cpu), without a tight limit. The descriptor limit is the
node's ([File descriptors](#file-descriptors)).

## What to alert on

Each of the first three conditions is one series compared with a
constant, so each reaches its owner without a join.

- `icetray_table_below_level == 1`: the newest snapshot of a table costs
  more per lookup than its `lookups` accepts. This is the table owner's
  alert: a writer changed the layout, or a compaction is overdue. It reads
  the same with `strict` on or off, since it measures the newest snapshot
  whether it serves or was refused. `icetray_table_level` and the table's
  findings say what put it there.
- `icetray_table_state{state="degraded"} == 1` for longer than a few
  polls: a table's refreshes are failing, or its ref names a snapshot it
  refused for a reason other than its level, and it serves an old
  snapshot. This is the alert for whoever runs the server: it fires for
  faults in storage and catalogs and for snapshots that cannot be served
  correctly, while a layout that only costs more reads as below its
  level, and under `strict` as held.
- `icetray_table_state{state="held"} == 1`: a strict table refused its
  newest snapshot for its level and serves the last one that met it, so
  its data falls behind its writer until a snapshot that meets the level
  is committed. How far behind shows in `icetray_generation_age_seconds`,
  the next alert.
- `icetray_generation_age_seconds` above the table's publishing interval
  plus a margin: the served data is older than its writer's schedule
  allows.
- `icetray_config_pending == 1` for more than a few minutes: a reload was
  refused, or a restart-only setting changed.
- A rising share of `icetray_requests_total` with `code` `unavailable`,
  `internal`, or `deadline_exceeded`.
- Any `icetray_requests_total{code="resource_exhausted"}`, or
  `icetray_requests_in_flight` near `icetray_max_in_flight`: the replica
  is saturated.
- `icetray_process_rss_bytes` near the container's memory limit.
- Failing readiness or restarting pods, from the orchestrator.

## Running the doctor against a deployment

Every sizing figure above comes from the doctor's `predicted` bytes, and
the doctor is the tool that explains a refusal. It loads each table the
way `serve` does, at the table's configured residency, and then decodes
and hashes every key to check its bucket, so its CPU time grows with the
row count and its memory is a load's.

Run it as a separate Job or Pod from the same image, with the same
configuration and credentials, rather than inside a serving pod. Inside
the pod it competes with the server for CPU and memory, and it shares the
server's local directory: it opens the same page cache disk level and
writes chunk files beside the server's. Give the doctor's pod its own
`emptyDir` for `storage.local.dir`, or run it with `--location`, which
uses no local directory.
