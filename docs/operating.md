# Operating icetray

How `icetray serve` starts, serves, refreshes, and stops, what it exposes,
and what a deployment has to provide.

## Starting and stopping

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
A signal that arrives while the first loads run ends them where they wait,
so a slow first load does not hold the drain past an orchestrator's grace.
The drain waits at most `server.drain_deadline` for open calls. Past it,
the drain stops waiting, the page cache is still flushed, and the process
exits 1; a call still open ends with the process.

## The gRPC contract

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

## Metrics and logs

The metrics listener serves `/metrics` in Prometheus format and `/ready`.
Counters: `icetray_requests_total{call}`, `icetray_object_gets_total{kind}`,
`icetray_object_bytes_total{kind}`, `icetray_local_reads_total`,
`icetray_local_bytes_total`, `icetray_hedges_total`,
`icetray_page_cache_hits_total`, `icetray_page_cache_fetches_total`,
`icetray_lookups_total{table,result}`,
`icetray_refresh_errors_total{table,reason}`,
`icetray_generation_swaps_total{table}`, and
`icetray_config_reloads_total{result}`. Histograms:
`icetray_lookup_seconds{table,result}` and
`icetray_object_get_seconds{kind}`. Gauges: `icetray_index_bytes{table}`
(the index's live bytes, exported beside `icetray_process_rss_bytes`
because a residency tier that sheds bytes is not visible in process memory
on every platform), `icetray_page_cache_bytes`,
`icetray_findings{table,code}`, `icetray_generation_age_seconds{table}`,
`icetray_generation_sequence_number{table}` (the serving snapshot's
sequence number), `icetray_generation_swapped_at_seconds{table}` (when it
was swapped in, in Unix seconds), `icetray_table_state{table,state}`, and
`icetray_config_pending` (1 while the configuration file differs from the
configuration applied).
No metric carries the snapshot id, so the series count does not grow with
swaps; the id is in the swap's log line and in `TableInfo`. Every label value is the name of an
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
emitted under one name, is refused with the rest of the configuration,
before anything is opened, with exit 1.

## The local directory

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
hit, one GET. On Linux, put the directory on a mounted volume with a
filesystem such as ext4, not in a container's overlay filesystem: a local
read the operating system's page cache holds then runs on the worker
thread answering the request instead of handing off to another thread,
which overlayfs does not allow.
A `bloom` table keeps the key column's bloom filters in memory instead, needs no disk, and pays two GETs per hit and a GET per miss
at the filter's false-positive rate. `disk` residency without a local
directory is a configuration error, and `bloom` on a table whose files
carry no key bloom filter is a refusal at load.

## Refresh and reload

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

The configuration file is read again on the same interval, and at once on
SIGHUP; a change to its tables applies without a restart ([RFD 018](rfd/018-configuration-reload.md)). A
table whose entry is unchanged keeps serving untouched. An added table,
and a changed one, load beside the tables serving, a changed one beside
the table serving under its name, and once every one of them serves, the
whole set swaps at once; a removed table then answers `NOT_FOUND`. A
reload changes nothing when any of those tables fails to load, when the
file does not parse or validate, or when any setting outside `[[tables]]`
changed, which only a restart applies; the refusal is logged with its
reason and counted, `icetray_config_pending` reads 1 until the file and
the server agree again, and the poll does not try the same file again
until it changes, though a SIGHUP does. Readiness does not change on a
reload. A file mounted where the orchestrator never updates it, such as a
single-file mount fixed at start, is never seen to change. Replace the file
whole, by a rename, rather than writing it in place: a read in the middle
of an in-place write can see a shorter file that still parses.

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

Run each replica under a container memory limit, the backstop for what
load's checks do not cover. Size it as the sum, over the tables the replica
serves, of the doctor's `predicted` bytes for each table's tier
(`predicted.full`, `predicted.disk_memory`, or `predicted.bloom`), doubled
for a table a refresh can rewrite wholesale, since the new generation loads
beside the serving one; plus `server.page_cache_bytes`; plus a margin of a
quarter of that total for the process itself, requests in flight, and a
load's transient chunks. Load refuses a key page header, and a lookup a
value page header, that declares more than its page can hold or than a 64
MiB ceiling that does not rest on the footer, before parquet allocates for
it, but the lengths inside a delta-encoded page still size buffers from the
file, and there the limit is what stops the process. A file that trips it takes down every
replica that loads it, since they all load the same snapshot and restart
into the same file; that is why the cases the checks know are refused
instead, which on a refresh keeps the last good generation serving. A panic
inside parquet's decoding of a file is caught and refused the same way.

A local volume is optional. Mount one to run disk-residency tables or to
keep a disk level of the page cache across restarts. Everything in it is a
copy of bytes in object storage, named by the data file it came from and
checked against that file's identity, and is rebuilt on demand; a replica
that restarts with its volume reuses every chunk file whose data file is
still referenced and deletes the rest. The volume needs capacity and
nothing else: nobody backs it up or shares it between replicas, and losing
it costs one cold load. Each disk-tier data file keeps one chunk file open
for the life of its generation, and a refresh that rewrites a table
holds the old and the new generation's at once, so set the descriptor limit
(`ulimit -n`, or the container runtime's `nofile`) to at least twice the
number of disk-tier data files the replica serves, plus a headroom of 1,024
for sockets, the page cache's disk level, and the one chunk file retention
opens at a time.
