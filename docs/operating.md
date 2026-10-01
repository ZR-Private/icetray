# Operating icetray

## Starting and stopping

```sh
icetray serve --config icetray.toml
ICETRAY_CONFIG=/etc/icetray/icetray.toml icetray serve
```

`serve` takes only the configuration path.

- Start binds the gRPC and metrics listeners, registers gRPC health as
  `NOT_SERVING`, and loads every table concurrently. The listeners accept
  while tables load.
- Once every table serves, health flips to `SERVING` and `/ready` on the
  metrics listener answers 200. Until then it answers 503.
- A table that fails its first load is fatal: the reason is logged, health
  stays `NOT_SERVING` for ten seconds, and the process exits 1.
- SIGTERM or SIGINT drains in-flight requests, flushes the page cache's
  disk level, and exits 0. A signal during the first loads ends them.
- The drain waits at most `server.drain_deadline` for open calls. Past it,
  the page cache is still flushed and the process exits 1.

## The gRPC contract

The client contract is `crates/icetray-proto/proto/icetray.proto`.
Clients are generated from it. Three calls:

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

The listener speaks plaintext gRPC; TLS, authentication, and access
policy are the platform's. The default listen addresses are loopback.

## Metrics and logs

The metrics listener serves `/metrics` in Prometheus format and `/ready`.

Counters:

- `icetray_requests_total{call}`
- `icetray_object_gets_total{kind}` and `icetray_object_bytes_total{kind}`
- `icetray_local_reads_total` and `icetray_local_bytes_total`
- `icetray_hedges_total`
- `icetray_page_cache_hits_total` and `icetray_page_cache_fetches_total`
- `icetray_lookups_total{table,result}`
- `icetray_refresh_errors_total{table,reason}`
- `icetray_generation_swaps_total{table}`
- `icetray_config_reloads_total{result}`

Histograms:

- `icetray_lookup_seconds{table,result}`
- `icetray_object_get_seconds{kind}`

Gauges:

- `icetray_index_bytes{table}`: the index's live bytes, beside
  `icetray_process_rss_bytes`.
- `icetray_page_cache_bytes`
- `icetray_findings{table,code}`
- `icetray_generation_age_seconds{table}`
- `icetray_generation_sequence_number{table}`: the serving snapshot's
  sequence number.
- `icetray_generation_swapped_at_seconds{table}`: Unix seconds.
- `icetray_table_state{table,state}`
- `icetray_config_pending`: 1 while the configuration file differs from
  the configuration applied.

The snapshot id appears in the swap's log line and in `TableInfo` but in
no metric. Every label value is an enum variant name.

Logs are JSON on stdout at `info`, one object per line. The standard
fields are `time` (RFC 3339, UTC), `level`, `message`, and `target`.
`server.log.fields` renames them; `server.log.static_fields` adds constant
fields to every event:

```toml
[server.log.fields]
time = "ts"
level = "severity"

[server.log.static_fields]
service = "icetray"
```

A key of `fields` that names no standard field, or two standard fields
emitted under one name, is refused with the rest of the configuration,
before anything is opened, with exit 1.

## The local directory

`storage.local.dir` names an optional local directory. It holds:

- The chunk files of tables with `residency = "disk"`: each data file's
  key column, with only the fence array in memory.
- The disk level of the value-page cache, bounded by
  `storage.local.page_cache_disk_bytes`. Zero turns the level off; any
  other value is at least one 4 MiB block.

`index_budget_bytes` bounds the chunk files across every table, measured
when a load begins. The serving generation's chunk files stay until the
new one has swapped in, so a table rewritten wholesale wants about twice
its steady footprint, the doctor's `predicted.disk_bytes`.

A disk-tier lookup costs one local read and, on a hit, one GET. On Linux,
put the directory on a mounted volume with a filesystem such as ext4; a
container's overlay filesystem forces every local read onto another
thread.

A `bloom` table keeps the key column's bloom filters in memory, needs no
disk, and pays two GETs per hit and a GET per miss at the filter's
false-positive rate. `disk` residency without a local directory is a
configuration error. `bloom` on a table whose files carry no key bloom
filter is refused at load.

## Refresh and reload

Each table's ref is re-resolved every `server.poll_interval`.

- A poll that finds the metadata location unchanged costs one read.
- A poll that finds a new snapshot builds the generation beside the
  current one, loading only the data files the previous generation did
  not hold, then swaps it in. `icetray_generation_swaps_total` counts it.
- A refresh that fails leaves the last good generation serving, counts
  `icetray_refresh_errors_total`, and moves the table's state to degraded.
  Readiness is unchanged.
- Snapshot retention on the served ref must exceed the longest lag a
  replica can have, `icetray_generation_age_seconds`: an expired
  snapshot's data files vanish under a replica still serving it.

The configuration file is read on the same interval, and at once on
SIGHUP. A change to its tables applies without a restart
([RFD 018](rfd/018-configuration-reload.md)).

- A table whose entry is unchanged keeps serving untouched.
- Added and changed tables load beside the tables serving. Once every one
  of them serves, the whole set swaps at once. A removed table then
  answers `NOT_FOUND`.
- A reload is refused whole when any of those tables fails to load, when
  the file does not parse or validate, or when any setting outside
  `[[tables]]` changed; those apply on restart only. The refusal is logged
  and counted, and `icetray_config_pending` reads 1 until the file and the
  server agree. The poll retries once the file changes; SIGHUP retries at
  once.
- Readiness does not change on a reload.
- A single-file mount fixed at start is never seen to change.
- Replace the file whole, by a rename. A read during an in-place write can
  see a shorter file that still parses.

## Deploying

icetray's code is the same for every deployer, and anything added to it
must be deployable by a second organization without editing source.

- The deployer's wrapper is a container image built from source,
  carrying the binary, its configuration file, and its log field names.
  The same image carries `doctor`, `get`, and `info`, for running inside
  the pod with the daemon's configuration and credentials.
- The configuration path comes from `ICETRAY_CONFIG`, so a pod passes no
  flags. TOML and JSON read into the same schema.
- Credentials come from the AWS SDK's default chain (profiles, single
  sign-on, credential helpers, pod identity), resolved once and cached
  until shortly before expiry. Static keys in `storage.s3` win over the
  chain and are the path for S3-compatible endpoints, with `endpoint`,
  `allow_http`, and `path_style`. icetray has no secret format of its own.
- The gRPC port is plaintext and unauthenticated. Run it on a private
  network or behind a mesh or sidecar; the port is never safe to expose
  directly.

Memory:

- Run each replica under a container memory limit. Size it as the sum,
  over the tables the replica serves, of the doctor's `predicted` bytes
  for each table's tier (`predicted.full`, `predicted.disk_memory`, or
  `predicted.bloom`), doubled for a table a refresh can rewrite wholesale;
  plus `server.page_cache_bytes`; plus a quarter of that total for the
  process, requests in flight, and a load's transient chunks.
- Load refuses a key page header, and a lookup a value page header, that
  declares more than its page can hold or more than a 64 MiB ceiling, and
  a panic inside parquet's decoding refuses the file. The lengths inside a
  delta-encoded page still size buffers from the file; there the limit is
  the backstop, and a file that trips it takes down every replica.

The local volume:

- A local volume is optional. Mount one for disk-residency tables or to
  keep the page cache's disk level across restarts.
- Everything in it is a copy of bytes in object storage, checked against
  its data file's identity and rebuilt on demand. A restarted replica
  reuses every chunk file whose data file is still referenced and deletes
  the rest.
- The volume needs only capacity. It is not backed up or shared between
  replicas, and losing it costs one cold load.
- Each disk-tier data file keeps one chunk file open for the life of its
  generation, and a refresh holds two generations' at once. Set the
  descriptor limit (`ulimit -n`, or the container runtime's `nofile`) to
  at least twice the number of disk-tier data files the replica serves,
  plus 1,024 for sockets, the page cache's disk level, and the one chunk
  file retention opens at a time.
