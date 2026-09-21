# Worker briefs

One brief per worker in the build sequence
([the sequence](2026-09-10-clean-build.md), [the blueprint](2026-09-10-clean-build-design.md)).
A brief is what a worker needs and nothing else: the files it owns, the
signatures it implements, the invariants it must prove and where, what it may
not change, and how it knows it is finished. Consumed during the build.

## Rules every worker follows

Read `CONTRIBUTING.md` and `AGENTS.md` first; they apply in full. Read the
RFDs your brief cites, and the blueprint's sections 2, 3, and 6.

- The blueprint's signatures are frozen. If one does not fit, stop and say
  so with the reason; do not change it and do not work around it. A signature
  changes by editing the blueprint first, which is a decision for a person.
- One owner per concept. If the thing you need belongs to another module,
  call it; do not compute it again.
- Every closed set is an enum, every error a typed variant, nothing decides
  on message text, no numeric literal without a named constant, and a
  constant two crates need lives in `contract`.
- Every invariant in your brief has a test at the tier named, written with
  the code it pins, asserting on typed outcomes and exact counts.
- Every `pub async fn` the daemon spawns (`Table::load`, `refresh`, `get`,
  `Opened::retain`) returns a `Send` future. A closure that takes a reference
  and returns an `async` block, or an opaque `impl IntoIterator` fed to
  `stream::iter`, makes every future up the call chain unprovably `Send`
  without failing core's own tests; collect into a `Vec` and stream by value,
  and keep the spawnability tests in `table` and `open` green.
- Gate before reporting: `dev/ci-checks.sh` and `cargo test --workspace`, run
  to completion, output read, never piped through `head`. Report the exact
  result lines.
- Conventional Commits, no attribution trailers, commit locally, never push.

## Stage 1: foundations (one worker, in order)

Owns: the workspace, `error`, `approx`, `key`, `bucket`, `contract`,
`config`, `storage`, `local`, `parquet_io`, `index`, `cache`. Nothing else
may start until these signatures exist and compile.

1. Workspace: four crates per blueprint section 1, `[workspace.dependencies]`
   with versions from `cargo add` and never from memory, the lint table of
   RFD 015 plus `wildcard_enum_match_arm`, `error`, `approx`.
2. `key` and `bucket`. RFD 008. Four key types; `uuid` orders as the Iceberg
   specification defines and is conformance-tested against the reference
   implementation, not assumed bytewise. Oracle tests for all four plus
   `fixed`.
3. `contract`: the layout's constants, each with a doc comment saying what it
   bounds and why that value. RFD 002.
4. `config`: TOML and JSON into one structure, validated once, with the
   equivalence test. RFD 011.
5. `storage`: prefix stores, location rewriting, one counter for GETs, bytes,
   hedges, local reads and local bytes, the credential bridge with coalesced
   refresh, and hedging against a `HedgeWindow` the caller owns. RFD 003,
   RFD 013.
6. `parquet_io`, `index`, `cache`. RFD 004, RFD 006. `Segments` errors on a
   non-resident byte. `FileIndex::build` performs every refusal and drops the
   value column's column index. `find` chooses the row group from the exact
   fences and never from column statistics.

Invariants proved here: bucket routing equals the library's transform for
every admitted type; key ordering equals Parquet's per type, and the uuid
comparator matches the reference; a file without column statistics and
several row groups never yields a false miss; `Segments` errors rather than
fetches; a dictionary-encoded value column costs one extra GET and a
dictionary-encoded key column none; keys over the cap are refused; the
credential provider refreshes once under concurrent expiry; configuration
parses identically from TOML and JSON.

Done when those tests pass, the gate is green, and the signatures in
blueprint section 3 for these modules exist as written.

## Stage 2A: tables

Owns `catalog`, `manifest`, `generation`, `table`, `open`. RFD 005, RFD 009,
RFD 007's refusal list, RFD 017's budget check.

- `catalog` builds every source including REST, and an unchanged poll costs
  one read.
- `manifest` derives the layout from the snapshot's own schema and each
  manifest's partition spec, reads delete manifests so the delete-file
  refusal is reachable, and orders files deterministically.
- `generation` loads with reuse by path, groups overlaps, refuses duplicate
  keys, checks the resident-bytes budget at the footer stage before any chunk
  is fetched, and exposes `plan` as the cost oracle. `Generation::load` passes
  the table's name and hedge window to `FileIndex::build` and
  `FileHandle::open`, and exposes `identities` for `retain`'s live set, which
  `Opened::retain` unions over every table sharing the local directory.
- `table` owns the lifecycle, the serialized refresh, the atomic swap, and
  the `strict` gate, which calls `check::diagnose` on a built generation
  before swapping it in. It is a separate module so that gate does not invert
  the dependency direction.

Invariants: load costs two GETs per file plus metadata, and a load with a
previous generation reuses every unchanged file by pointer; an unchanged poll
costs one read; a refresh verifies only overlap groups that gained a file and
a disjoint append verifies none; a duplicate key is refused and the previous
generation keeps serving; a delete file is refused by reason; an evolved
partition spec still routes; `plan` predicts exactly the GETs a `multi_get`
issues; `strict` refuses a cost-tier generation and only that; an over-budget
full-tier load is refused with nothing fetched.

## Stage 2B: doctor

Owns `check`. RFD 007, RFD 016. Depends only on the frozen `Generation`
shape.

The finding table in blueprint section 3 is the specification: every code, its
tier, its trigger, its penalty, and the writer property it names. Thresholds
live in one module, each deriving from `contract` where one exists, each with
the analysis section that set it. `TableSummary` is what the subcommands
serialize, so a new measurement reaches them without a change there.

Invariants: every finding code is triggered by exactly one fixture designed
for it and the contract fixture triggers none; the predicted resident bytes
per tier are computed from footers alone and agree with each tier's measured
bytes.

## Stage 2C: contract and binary

Owns `icetray-proto` and the `icetray` crate. RFD 010, RFD 012, RFD 016's
exit codes.

The proto is blueprint section 4 verbatim, with a vendored protobuf compiler
so a source build needs none.
The binary is blueprint section 5 as built: `serve` binds both listeners and
registers health `NOT_SERVING` before opening any table, loads every table
concurrently, flips `SERVING` and `/ready` together when every table
serves, runs one refresh task per table and one sampler on the poll
interval, calls `Opened::retain` after the first loads and after every swap,
drains on SIGTERM or SIGINT, and refuses to start after the grace on a
first-load failure. `doctor`, `get`, and `info` share the `Target`
vocabulary and go through `open`; RFD 010's mapping is
`service::status_from`, exhaustive over `Error`, and `key_from_wire` reads
the wire key under the table's key type. Logs are JSON with the configured
field names; the metric and label names in blueprint section 7 are the
public surface.

What the binary proves and where. `tests/serve.rs` drives `Serving`
in-process over the in-process S3 server: health `NOT_SERVING` and `/ready`
503 with the listeners already accepting, then `SERVING` on both health
names and 200 once the table serves; `TableInfo` describing the served
snapshot; three cold `Get`s at exactly the oracle's GETs, warm and absent
at none, a null at one GET cold and distinct from absent; `MultiGet` in
order from one snapshot; a missing, unset, mismatched, or fifteen-byte uuid
key as `INVALID_ARGUMENT` with no GET; an unserved table as `NOT_FOUND`; a
deleted object as `UNAVAILABLE` for one `Get` and for a whole batch, the
other keys still answering; exact `requests_total` deltas per call and the
sampled gauges in the exposition; a refresh within the poll interval; a
`Get` before `shutdown` answered and `join` `Ok`; a first-load refusal
visible as `NOT_SERVING` through the grace, then `FirstLoad` naming the
table and `ErrorKind::Refused(NoLocalDir)`; and a shutdown during the grace
ending the run at once. `tests/cli.rs` runs the built binary over local
fixtures: `info`'s summary with `kind` and `table` first; the doctor at
exit 0 with 16 `file` lines, at exit 2 with `no_bucket_partition` at tier
`cost`, and at exit 1 with a `refused` line whose reason is `unsorted`;
`get` answering present, null, and absent with base64 values and GET
counts; every key type as `Display` prints it; a bad key at exit 64 with
nothing on stdout; a table name required under two tables; TOML and JSON
configurations printing identical lines; location-only flags under
`--config`, no target, and an unknown `--table` at exit 64; a reader
closing stdout ending the run at exit 0; and `serve` started from the
environment alone, logging under the renamed fields, ready, `SERVING`, and
draining on SIGTERM to exit 0. Unit tests cover the key parser, the output
lines' key order, the log formatter's order and uniqueness, the recorder,
the wire key and status mapping per variant, the command line, and the exit
code per `CliError` variant.

## Stage 2D: testkit

Owns `icetray-testkit`. RFD 014.

Carry from the prototype what the salvage note marks verbatim, reading it
before copying. Rewrite `verify` on `Generation::plan`, give each measured
batch a fresh page cache, keep one source of truth for written ids, take the
key type explicitly, locate golden tables through `ICETRAY_GOLDEN_WAREHOUSE`
and make a missing warehouse a visible skip. Add one fixture per refusal and
per finding that no contract fixture triggers, including a column of
Iceberg's own uuid type, which the golden script cannot write.

Invariants: a gated test that cannot run reports as skipped and never as
passed; every `RefusalReason` variant has exactly one fixture.

Invariants the tables stage proved at unit tier only, or not at all, and
deferred here; where the testkit proves each:

- A `TableSource::Catalog` built by `from_config` resolving through a real
  REST catalog (`load_table` over HTTP, metadata and location taken from the
  loaded table): the catalog path is proved through iceberg's in-process
  `MemoryCatalog` at zero object-storage GETs (every P-tier load over
  `Fixture::catalog_source`, and `tests/lookup_s3.rs::
  test_refresh_on_a_catalog_source_reads_no_object` for the unchanged
  poll); the REST client itself is proved by `tests/rest_catalog.rs::
  test_rest_catalog_source_loads_polls_and_advances`, container-gated, run
  by `dev/test-int.sh` with `-- --ignored` and reported as ignored
  otherwise (RFD 014).
- `TableFault::BucketValueMissing` for a non-null, non-integer bucket
  literal stays unproved beyond the null case: iceberg-rust's manifest
  writer serializes the partition tuple by the spec's type and cannot write
  one.
- `Generation::load` in the Bloom tier: `tests/residency_bloom.rs` counts a
  load as the Full tier's GETs plus one filter per row group
  (`test_bloom_load_reads_filters_and_keeps_less_resident`), and a refresh
  as one filter per row group of each new file plus one chunk per row group
  of every member of each overlap group a new file joined, new files
  included (`test_bloom_reverifies_grown_groups_by_refetching_chunks`).
- The retain live set as the union over every table sharing a local
  directory: `tests/residency_disk.rs::
  test_retain_frees_only_what_no_table_serves` opens two tables over one
  root-followed fixture through `open_with`, drops a file by hand, and
  counts `Opened::retain` freeing nothing while either table serves the
  file and its chunk file once neither does.
- Load GET count for files with several row groups through
  `Generation::load`: `tests/findings.rs::
  test_statistics_free_multi_row_group_file_loads_one_chunk_per_row_group`
  counts a footer per file and a chunk per row group.

## Stage 2E: local disk

Built in stage 1, after `storage`, because `index` and `cache` call into it.
Its invariants are proved by the disk-tier acceptance tests, which stage 2D
owns.

## Stage 3: integration (one worker)

The acceptance suite green over in-memory, in-process S3, and simulation; the
gRPC tests behind the server; the container-gated catalog test run through
`dev/test-int.sh` over `dev/rest-fixture.sh`; the golden tables written by
`dev/golden-tables.sh` (the recipe `dev/golden-tables.py` under Spark in a
container) and their tests run through `dev/test-golden.sh`; the conformance
run `dev/conformance.sh`, a CI job, which starts the binary the way an
orchestrator would and checks logs, metrics, health, and readiness. Then the
README rewritten for the working system, and the humanizer pass over it and
the RFD problem statements.

Invariants: a cold hit is one round trip and a load a fixed number of
dependent round trips, identical across seeds; hedging trims an injected
bimodal tail and stays under its ceiling when there is no tail; load reads
never hedge.
