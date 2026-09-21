# Clean build: sequence

How the blueprint in `2026-09-10-clean-build-design.md` gets built, in an
order that lets independent parts proceed in parallel once the seams they
share are frozen. Each step ends with the gate green (`fmt`, `clippy`
pedantic at deny, all tests) and one conventional commit.

## Before the first commit

- The repository history starts fresh at the first commit below; the
  prototype branch is not merged and not pushed. The published repository
  carries the analysis documents, the RFDs, these plans, the template files
  (`CONTRIBUTING.md`, `AGENTS.md`, `clippy.toml`, the toolchain pin, CI), and
  the code built here. The workspace manifest and its lint table arrive with
  stage 1's first commit, because a workspace with no members does not
  build.
- Commit author for every commit: the maintainer's public identity with a
  no-reply address. A local pre-commit hook (not committed) rejects a fixed
  denylist of internal identifiers. The denylist itself is never committed,
  so CI cannot run it; the hook and the checks run before every commit are
  the gate.
- Nothing from the prototype is copied without being read; the salvage note
  in the maintainer's scratch directory lists what is carried and what is
  rewritten, module by module.

## Stage 1: foundations, single-threaded

One worker, in this order, because every later step builds on these
signatures.

1. Workspace: four crates, `[workspace.dependencies]`, lint table, `approx`,
   `error`. Commit: `chore: workspace and lint policy`.
2. `key` and `bucket` with the oracle tests for all three key types.
3. `config` with TOML and JSON parsing and the equivalence test.
4. `storage`: prefix stores, rewriting, request counting, credential bridge
   with coalesced refresh, hedging behind `HedgeConfig` (adaptive delay from
   a sliding window of lookup GET latencies, rate ceiling), load-path reads
   never hedged. Unit tests with a counting credential provider and an
   in-memory store.
5. `local`: the directory layout, chunk-file identity and reuse, deletion
   with the generation, disk budget accounting, local-read counting,
   `DiskChunks` with positioned reads on a blocking pool, and the `DiskTier`
   handle the page cache's disk level is built from. Built here rather than
   in stage 2 because `FileIndex::build` and `PageCache::new` call into it
   and nothing is stubbed.
6. `parquet_io` and `index`: footer load, `Segments`, `FileIndex::build`
   with the refusals and both residency tiers (carry the prototype's bloom
   path and its tests), `find` over exact fences, `read_value`, `page_of`,
   `predicted_resident_bytes`; the value column's column index dropped after
   its offset index is taken. Unit tests write Parquet with `arrow`: contract layout,
   misaligned pages, dictionary-encoded value column, dictionary-encoded key
   column, several row groups with statistics disabled, keys at and over the
   cap.
7. `cache` with hits and fetches counted after coalescing.

## Stage 2: fan-out on frozen seams

Four workers in parallel, each against the signatures in the blueprint. None
changes a signature without updating the blueprint first.

- **A. Tables.** `catalog` (static, hint, REST construction; one-read change
  detection), `manifest` (snapshot schema, per-manifest spec, delete-file
  refusal, deterministic ordering), `generation` (load with reuse, overlap
  groups, duplicate refusal, `plan`, `Table` lifecycle with serialized
  refresh, the residency budget check at the footer stage), `open`.
- **B. Doctor.** `check` with the finding table and thresholds module,
  `TableSummary`, `diagnose`. Depends on A's `Generation` shape only, which is
  frozen.
- **C. Contract and binary.** `icetray-proto` (proto, build script, scoped
  allows, round-trip tests), the `icetray` binary's `serve` subcommand
  (service, health, readiness, per-table refresh tasks, twelve-factor main,
  graceful shutdown).
- **D. Testkit.** Carry the fixture writer, in-process S3, simulation
  transport, and acceptance tests; rewrite `verify` on `Generation::plan`;
  add the new fixtures (delete file, evolved spec, permuted-key overlap,
  dictionary value column, statistics-free multi-row-group file); golden
  locator by environment variable with visible skip; dependency split.

Order of merges: D's fixtures and A's tables first (they test each other),
then B, then C.

## Stage 3: integration

One worker.

1. The one-shot subcommands (`doctor`, `get`, `info`) exist from stage 2C
   on `core::open` and `TableSummary`, with the binary test
   (`icetray/tests/cli.rs`) covering `--config` in TOML and JSON,
   `--location`, string, binary, and uuid keys, exit codes 0, 1, 2, and 64,
   the broken-pipe path, and `serve` started from the environment with a
   SIGTERM drain; stage 3 reruns it as part of the workspace gate and adds
   nothing here.
2. Acceptance suite green over in-process S3 and simulation; the gRPC tests
   behind the server including health and readiness; the REST container test
   (`icetray-testkit/tests/rest_catalog.rs`, an ignored test that a plain
   `cargo test` reports as ignored) run through `dev/test-int.sh`. The first
   run found that core built the REST client without a storage factory,
   which the library requires before it loads a table; the client is now
   built with the library's in-memory factory, a placeholder that is never
   asked to build because every byte goes through `Storage`.
3. Golden tables: `dev/golden-tables.sh` runs the recipe with Spark in a
   container against `target/golden-warehouse`, and `dev/test-golden.sh`
   runs the golden tests (`icetray-testkit/tests/golden.rs`, ignored tests)
   against it. The first run found that a Hadoop catalog records locations
   as `file:/path`, with no authority, which `Storage::locate` refused as
   relative; the form is accepted now. The recipe's contract tables draw
   `large_value_pages` advice: 192-byte values in 1,000-row pages decompress
   to about 192 KB, above the 128 KiB threshold, so the recipe's `CONTRACT`
   properties do not by themselves meet RFD 002's value-page bound. Whether
   the recipe lowers `write.parquet.page-size-bytes` for every table or the
   threshold moves is a stage 4 question, recorded in its analysis.
4. Conformance run: `dev/conformance.sh`, a CI job of its own, runs the
   binary's `test_serve_conformance_from_the_environment` by name: the
   server started the way a pod starts it (the path from `ICETRAY_CONFIG`, a
   JSON file, no flags), the JSON logs under the configured field names, the
   health service answering while the table loads, readiness after it
   serves, the metrics scrape, and the drain on SIGTERM to exit 0.
5. `README.md` rewritten for the working system: what it is, the contract in
   words with a pointer to the doctor, how to run against a table, how to
   deploy. Then the humanizer pass on the README and the RFD problem
   statements.

## Stage 4: against real S3

From a laptop with profile credentials against tables written by the
notebook recipe in the prototype findings: doctor, lookups with value
verification, load time after the credential fix (the number the analysis
lacks), snapshot advance, adaptive hedging behavior. Recorded in
[the clean build against real S3](../analysis/2026-09-21-real-s3-verification.md);
the findings document's "not verified" list points at it for the two items
it settles. The snapshot was appended by a Spark job run the way
`dev/golden-tables.sh` runs the recipe, in a container against the S3
warehouse with temporary credentials from the profile.

## Done when

Every invariant in the blueprint's section 6 has its test; the gate is
green; the doctor reports the contract fixture as efficient with no
findings; the conformance run passes; the README describes the system as
built; the denylist grep over the tree and the history is empty.
