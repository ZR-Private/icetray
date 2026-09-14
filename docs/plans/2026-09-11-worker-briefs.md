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
- Gate before reporting: `dev/ci-checks.sh` and `cargo test --workspace`, run
  to completion, output read, never piped through `head`. Report the exact
  result lines.
- Conventional Commits, no attribution trailers, commit locally, never push.

## Stage 1: foundations (one worker, in order)

Owns: the workspace, `error`, `approx`, `key`, `bucket`, `contract`,
`config`, `storage`, `parquet_io`, `index`, `cache`. Nothing else may start
until these signatures exist and compile.

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
  is fetched, and exposes `plan` as the cost oracle.
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

The proto is blueprint section 4 verbatim, including the chunk-file header
message and a vendored protobuf compiler so a source build needs none.
`serve` binds the gRPC and metrics listeners and registers health before
loading, flips readiness when every table serves, refreshes per table, and
drains on SIGTERM. The one-shot subcommands share one flag vocabulary with
it and go through `open`. Logs are JSON with the configured field names; the
metric and label names in blueprint section 7 are the public surface.

Invariants: the gRPC error mapping per RFD 010, `MultiGet` from one snapshot
failing as a whole when one read fails, a null value distinct from an absent
key, health `NOT_SERVING` until every table serves with the listener already
accepting, and the subcommands' JSON shape and exit codes.

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

## Stage 2E: local disk

Owns `local`, and the disk level inside `cache`. RFD 017, RFD 012.

Chunk files under `index/`, named by a digest of the data file's path, each
carrying a `ChunkFileHeader` inside the envelope of RFD 011. Identity is
path, size, and footer digest; a matching file is reused on restart and the
chunk fetch is skipped. A generation that drops a file deletes its chunk
file. Pages are read with positioned reads on a blocking pool, never mapped,
and every read is counted. The page cache's disk level takes a `DiskTier`
from here.

Invariants: a disk-tier cold hit is one GET and one local read, a miss one
local read and no GET; a restart with matching chunk files fetches footers
only; a dropped file's chunk file is deleted; no local directory is a typed
refusal; a page evicted from memory is served from disk with no GET and
survives a restart.

## Stage 3: integration (one worker)

The acceptance suite green over in-memory, in-process S3, and simulation; the
gRPC tests behind the server; the container-gated catalog test with a visible
skip; the golden tables regenerated by `dev/golden-tables.py` and their tests
run; the conformance run that starts the binary the way an orchestrator would
and checks logs, metrics, health, and readiness. Then the README rewritten
for the working system, and the humanizer pass over it.

Invariants: a cold hit is one round trip and a load a fixed number of
dependent round trips, identical across seeds; hedging trims an injected
bimodal tail and stays under its ceiling when there is no tail; load reads
never hedge.
