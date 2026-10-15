# Order Fences Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Apply the RFD 025 amendment of f5f8b79 (order fences, the merge by pair ranges, `server.max_scan_bytes`) and the confirmed findings of the second whole-branch review, then measure performance and sizing against the design before it.

**Architecture:** The load records, per order page, the shortest (key, order value) pair separating it from the page before (order fences), stores them in the ordered chunk file, and stops reading the writer's order column index. A Scan places a bound by binary search over its run's fences; the uniqueness merge selects pages of every file in an overlap group by pair-range intersection and streams them a row group at a time. A Scan or MultiScan response above `server.max_scan_bytes` is refused.

**Tech Stack:** Rust per `rust-toolchain.toml`, parquet, iceberg 0.10, foyer, tonic and prost, tokio; `icetray-testkit` stores and fixtures; the bench program `crates/icetray-testkit/examples/bench.rs`.

**Spec:** `docs/rfd/025-ordered-rows.md` at f5f8b79 and later. Findings: `.superpowers/sdd/2026-10-13-ordered-rows/branch-review-2-{read,load,api}.md` and `probes-2-report.md` (git-ignored workspace; the controller hands each task its pointers).

## Global Constraints

- Read `CONTRIBUTING.md` and `AGENTS.md` first; they bind every task.
- Run cargo only through flox: `flox activate -- cargo ...`, `flox activate -- dev/ci-checks.sh`.
- Never add `#[allow(...)]`; no `#[expect(dead_code)]` crosses a task boundary.
- Gate per task: focused `cargo test -p <crate>` while working; `dev/ci-checks.sh` and `cargo test --workspace --locked` before the task's last commit, run to completion.
- Commits: Conventional Commits, no trailers of any kind, never bypass the pre-commit hook. Do not push.
- No sleeps in tests; force concurrency with gates (`S3Fixture::hold`, `Gauged::fill_to`, `reach`). Tests are named `test_<fn>_<scenario>`, assert typed variants and exact non-zero counts, never message text, no timing assertions.
- No magic numbers: import production constants; a test literal gets a named const.
- No backward compatibility before 1.0: change formats, config keys, metric names, and proto fields in place; bump `CHUNK_FILE_VERSION` and `VERIFIER_VERSION` where the chunk file's content or the verifier's rules change; a local-directory file that does not match the running build is treated as absent.
- Nothing names any company, internal system, or internal workload.
- A key-value table keeps every check, layout requirement, and cost per call; the bench's key-value scenarios must not regress beyond run-to-run spread.
- Disk is limited: check `df -h /Users/boris` before builds, keep 10 GiB free, remove `target/debug/incremental` if needed; never remove `target/golden-warehouse` or `target/bench`.
- Defaults: `server.max_scan_bytes` 4 MiB (4,194,304 bytes of encoded response); everything else unchanged.

## Review Focus

1. A string-ordered table whose writer truncated its order column index to 16 bytes, values sharing a 64-byte prefix: every bounded Scan is one round and reads at most the bound page's rows beyond the limit (Task 3).
2. A file with no order column index at all loads and serves bounded Scans in one round (Task 2, Task 3).
3. Fences of byte-array keys or orders where adjacent pages differ only after a long shared prefix, and where a page starts a new key: the fence sorts above the previous page's last pair and at or below the page's first pair (Task 2, property test).
4. A Disk restart over trusted chunk files restores fences without decoding and without a GET, and a chunk file of the previous format is treated as absent (Task 2).
5. A rolled compaction (files of one snapshot meeting at edge keys) first-loads, refreshes, and restarts reading only edge pages for the uniqueness merge (Task 4).

---

## Task 1: Bench scenarios and sizing for the measurement, and the baseline run

**Files:** `crates/icetray-testkit/examples/bench.rs` (and testkit helpers it needs).

The baseline is the library code of 39b8a7c (this task's commit changes only the bench), so the same bench binary shape measures before and after.

- [ ] Add ordered tables and scenarios (named consts for every size):
  - `ordered-long`: long keys with `LONG_RUN_ROWS` (10,000) rows per key, timestamp order. Scenarios: `scan-bounded-long` (limit 10, newest first, `before` at a random point in the middle half of the run, warm and cold), `scan-head-long` (limit 100 newest, no bound, warm).
  - `ordered-string`: a string order column whose values share a 64-byte prefix and whose order column index is truncated to 16 bytes by the writer settings. Scenario `scan-bounded-string` (limit 10, a bound inside the run, warm and cold).
  - Load, refresh, and restart scenarios over an ordered table rolled by size (several files of one snapshot per bucket meeting at edge keys) and an appended ordered table (several overlapping appends per bucket, short runs per file): first load, refresh after one more append, and a Disk restart over the same local directory. Report wall, CPU, GETs, bytes fetched, local reads and bytes, and peak RSS.
- [ ] Sizing: per table and tier, report the resident bytes of the index by component where the code exposes them (key fences, the order column's index or fences, per-file facts, offset indexes), and the ordered chunk files' total bytes under Disk. If no accessor exists, add a narrow one on the library (a test-visible or pub stats method) in this task.
- [ ] Every scenario reports, as the existing ones do: latency percentiles, allocations and bytes per op, fetches and local reads per op, bytes read and returned, and for Scans the number of rounds to the store (add a rounds counter if needed: GET waves observed by the counting store).
- [ ] Build with `cargo build --profile profiling -p icetray-testkit --example bench --locked` and run the baseline: every new scenario in every tier, plus the existing key-value set (warm-hit, cold-hit, miss, multi-get-100 in full/disk/bloom, single and multi) and the existing ordered set; three alternating runs where comparing. Save the JSON under `target/bench/baseline-39b8a7c/` and write the exact commands into the report.
- [ ] Commit `test(icetray-testkit): bench scenarios for long runs, truncated order indexes, and ordered loads`.

## Task 2: Order fences at load, in the chunk file, and in the budget

**Files:** `crates/icetray-core/src/index/ordered.rs`, `index/build.rs`, `index/mod.rs`, `index/record.rs`, `local/format.rs`, budget estimation, `check.rs` (removal of the index check), tests.

- [ ] Fences: during verification, for each order page record the shortest (key, order value) pair that sorts above the last row of the previous page and at or below the page's first row (RFD 025 Load). Integer and time values exact; string, binary, and fixed cut to the shortest prefix that keeps that order (for a page that starts a new key, the key part separates and the order part is the shortest that keeps the pair at or below the first row). Keep them per row group beside the offset index. Rows deleted by merge-on-read still count as rows for fences (fences bound positions, not liveness).
- [ ] Stop reading and keeping the writer's order column index for ordered tables; remove the load's check that order values lie within that index and its file fault; remove `order_index_missing` and `order_index_truncated` from findings, levels, output, metrics, and their tests (the per-code metric series list shrinks).
- [ ] Chunk file: the ordered chunk file stores the fences; bump `CHUNK_FILE_VERSION` and `VERIFIER_VERSION`; a trusted restart restores fences without decoding; tighten `record::most_bytes` for string and binary order values (load review M7) to the bound the fences and facts actually need.
- [ ] Budget: the chunk-file budget and the resident budget count fences, estimating byte-array fences as key fences are estimated.
- [ ] Verification decodes a column's dictionary once per row group, not once per decode window (load review M3), for the key and order columns of ordered tables (check key-value verify too; if shared, the change must not regress the key-value load bench).
- [ ] Tests: a property test over random runs (long and string keys, long and string orders with long shared prefixes, pages starting at new keys, deleted rows) that every fence sorts above the previous page's last pair and at or below its own first pair, and is the shortest such; a file without an order column index loads; a restart under Disk restores identical fences with zero GETs and zero order decodes; a chunk file of the previous version is treated as absent.
- [ ] Commit(s) `feat(icetray-core): order fences ...`.

## Task 3: Scans placed by fences

**Files:** `crates/icetray-core/src/index/run.rs`, `scan.rs`, `index/ordered.rs` (range logic), `generation/mod.rs` as needed.

- [ ] Replace the column-index placement (`pieces`, `PageFit`, the two-round path and its counting) with a binary search over the run's fences: a bound lands on exactly one order page per row group the run spans; the rows beyond the limit that a Scan reads are at most the rows of the order page holding the bound (RFD 025 Cost). The walk does work proportional to the pages it reads, not to the run's pages (read review I4); one live row past the limit decides `more` (read review M1).
- [ ] Exact values when the bound's order page is local (Disk) or already in the scan cache: decode it first and read only the value pages of the rows taken, with no added round to object storage (read review I1).
- [ ] Every Scan is one round to object storage under `full` and `bloom` (after the key-page round under `bloom`), for every order type and index state.
- [ ] Tests: tighten the run-level model and property tests' byte bound to "pages of the rows taken plus the bound's order page's rows" and assert one round for every shape, truncated and missing indexes included; Disk and cached-page exactness (value pages equal the taken rows' pages; the review's P1 numbers: 476 B, not 640 B; one page, not two, for integer bounds); a warm Scan's work on a 10,000-page run equals a 10-page run's within a named count (count pages visited, not time). Remove the tests of the deleted two-round path.
- [ ] Commit(s) `feat(icetray-core): place a Scan's bounds by order fences`.

## Task 4: The uniqueness merge by pair ranges, streamed

**Files:** `crates/icetray-core/src/generation/ordered.rs`, `index/pairs.rs`, `check.rs` (`probe_widths`), tests in `crates/icetray-testkit/tests/refresh.rs`, `refusals.rs`, `residency_disk.rs`.

- [ ] Select pages for every file of an overlap group, checked or not, by pair-range intersection from the fences: a page's pairs lie between its fence and the next page's (RFD 025 Refresh). A checked file is no longer read whole (load review I1). A tie refuses `duplicate_rows` only when a checked file's copy is in it, as today.
- [ ] Stream: read each file's chosen pages a row group at a time as the merge advances, holding at most one row group's chosen pages per file (load review I2); decode chosen pages per page run, not one order page per span (load review M4).
- [ ] Remove the quadratic corners: the `meetings` sweep on chained key ranges with one order range (load review M1) and `probe_widths` on an appended bucket (load review M2), each to O(n log n) plus output.
- [ ] Tests: the review's probes as regression tests with exact counts: two files meeting at one edge key read only the edge pages (P1: about 94 B, not 4,821 B, per file, every tier); a Disk restart of a size-rolled table reads only edge pages (P2); bytes held before and during the merge bounded by one row group's chosen pages per file (P4: 50 files x 4 row groups); a deleted copy on the checked side (load review M8, P5); the existing refresh and refusal tests unchanged.
- [ ] Commit(s) `perf(icetray-core): merge ordered pairs by fence ranges, a row group at a time`.

## Task 5: `server.max_scan_bytes`

**Files:** `crates/icetray-core/src/config.rs`, `crates/icetray/src/service.rs`, telemetry, `check.rs` (`large_runs`), proto comments, `docs/calling.md`, `docs/configuration.md`, tests in `crates/icetray/tests/serve.rs`.

- [ ] Config `server.max_scan_bytes`, default 4 MiB (4,194,304), validated non-zero at load, a restart setting like the other server limits.
- [ ] Before sending, measure the response's exact encoded size (prost `encoded_len`); above the bound, return `FAILED_PRECONDITION` with no rows, a message naming the size, the bound, and how many rows (Scan) or which keys' rows (MultiScan) would fit; count it as an error in `icetray_scans_total` (and the MultiScan counterpart).
- [ ] A response-bytes histogram for Scan and MultiScan (api review M8).
- [ ] `large_runs`: `max_scan_rows` rows of the configured value columns at the footer's uncompressed bytes per row above `server.max_scan_bytes`.
- [ ] Docs: calling.md's error table (`RESOURCE_EXHAUSTED` is overload only; a response-size row for `FAILED_PRECONDITION`), the paging example sets `limit`; configuration.md's server table.
- [ ] Tests: a Scan and a MultiScan just under and just over the bound (exact sizes from a fixture with known value sizes), the error code and that no rows are sent, the metric counts.
- [ ] Commit `feat(icetray): refuse a Scan response above server.max_scan_bytes`.

## Task 6: Span fetches: permit order, failures, and contention

**Files:** `crates/icetray-core/src/cache.rs`, `storage.rs` (test store), tests.

- [ ] `get_span` takes the fan-out permit before it claims pages, as the key-value path does, so a Scan never waits behind another call's queued permits (read review I3); add the regression test from the review (P3: a second read must not time out behind the first's queue).
- [ ] A failed span returns without waiting on its joined flights' completion beyond what correctness needs (read review M5).
- [ ] `SpanFlights` stops being one process-wide mutex held across per-page lookups: shard it or scope it per table or file so warm span reads do not serialize (read review M6); a concurrency test that two warm Scans of different tables do not contend (count, not time).
- [ ] Tests for `get_span` failure paths (read review I5): a joined waiter receives the claimer's failure and its deadline; a failed or short span caches nothing and the next read fetches again; the short-read error.
- [ ] The test store's `Gauged` decrements its in-flight count when a GET future is dropped (read review M2).
- [ ] Commit(s) `fix(icetray-core): take the fan-out permit before claiming span pages` and as needed.

## Task 7: Value decoding and per-row costs

**Files:** `crates/icetray-core/src/index/run.rs`, `index/build.rs` (the value decode path), `scan.rs`.

- [ ] A direct decoder for PLAIN-encoded byte-array value pages (as 11cbfb8 did for key pages): slice values out of the decompressed page without per-value reference-count clones, and skip rows by walking lengths; dictionary-encoded pages keep the parquet path. Apply it wherever Scan decodes value pages; if the key-value Get path shares the code, apply it there too, and the key-value bench must not regress.
- [ ] Remove the per-row allocations the read review names (M3).
- [ ] MultiScan decodes its keys' results off the calling task where that lowers per-key cost (read review M4); measure single-task and multi-task MultiScan and keep the change only if it does not raise per-key CPU.
- [ ] Tests: the decoder against parquet's for random values, empty values, and pages with skipped prefixes; the existing Scan model tests.
- [ ] Commit(s) `perf(icetray-core): decode plain byte-array value pages directly`.

## Task 8: Service and output minors

**Files:** `crates/icetray/src/service.rs`, `output.rs`, the proto, `crates/icetray-core/src/local/mod.rs`, tests.

- [ ] The table-kind check runs before readiness and wire parsing, so a wrong-kind call is always `INVALID_ARGUMENT` (api review M2).
- [ ] The proto says that Get and MultiGet of an ordered table, and Scan and MultiScan of a key-value table, are `INVALID_ARGUMENT` (api review M3).
- [ ] The doctor's `table` line on an ordered table reports the Scan prediction (`gets_per_scan`) in place of key-value lookup costs (api review M1).
- [ ] `TableInfo` reports `max_multi_scan_values` (api review M9, the reporting half only).
- [ ] Error messages name the bound, the table, the computed MultiScan value count, and a bad row's order value where the review lists them (api review M10).
- [ ] End-to-end tests for the six `INVALID_ARGUMENT` paths the review lists (api review M12).
- [ ] `cache.format` is written crash-safely: write a temporary file, fsync, rename, fsync the directory (load review M6).
- [ ] Commit(s) `fix(icetray): ...`.

## Task 9: Docs and RFD update notes

**Files:** `docs/rfd/002-*.md`, `004-*.md`, `007-*.md`, `013-*.md`, `016-*.md`, `017-*.md`, `020-*.md` (update notes), `docs/operating.md`, `docs/configuration.md`, `docs/table-layout.md`, `docs/performance.md`, `docs/glossary.md`, `docs/calling.md`, README.

- [ ] Every mention of `order_index_missing`, `order_index_truncated`, or the order column index as a requirement or a placement mechanism is removed or rewritten to the fences; update notes on the RFDs RFD 025's Consequences lists, in house style (the decision as it now stands).
- [ ] The stale docs the api review lists (M11).
- [ ] `docs/table-layout.md`'s writer recipe no longer asks for a full-length order column index.
- [ ] Commit `docs: order fences and the response bound`.

## Task 10: Measurement and analysis

**Files:** `docs/analysis/2026-10-14-order-fences.md`.

- [ ] Build the bench at the branch head with the profiling profile and run exactly the Task 1 set with the Task 1 commands; three alternating runs against the baseline binary where comparing.
- [ ] Re-run the review's probes' scenarios as measured numbers (edge-key merge bytes, held bytes, bounded Scan bytes and rounds, warm Scan work on long runs).
- [ ] Sizing: resident index bytes per component and chunk-file bytes, before and after, per table and tier; fences against the writer's column index they replace.
- [ ] Profile (samply) a warm bounded Scan on `ordered-long` and `ordered-string` and a first load of the rolled table; name the top costs.
- [ ] Write the analysis: what moved, by how much, whether each RFD claim holds (one round always; work follows the limit; edge-only merge reads; memory per fence), the key-value regression check, and anything that did not move as expected. House voice; host by specs only.
- [ ] Commit `docs(analysis): order fences measured`.

---

## Follow-up (maintainer, 2026-10-15)

Decisions after the measurement: the merge reads ahead under a byte budget; MultiScan spreads its keys across idle workers as MultiGet does; the JSON output warns when an order value exceeds what a double holds exactly.

## Task 11: Merge read-ahead under a byte budget

**Files:** `crates/icetray-core/src/generation/ordered.rs`, `index/pairs.rs`, `docs/rfd/025-ordered-rows.md` (Refresh), `docs/operating.md`, tests.

- [ ] Each file of a merge holds the chosen pages of its current row group, always; beyond that the merge issues the reads of later row groups ahead of need, across the group's files in the order the merge will consume them, while the bytes held and in flight stay within `MERGE_READ_AHEAD_BYTES` (64 MiB, 67,108,864) per merge. Pages are read once; GETs and bytes are unchanged; only their timing moves.
- [ ] RFD 025 Refresh: replace "holds at most one row group's chosen pages per file" with the read-ahead rule as it stands; operating.md's refresh notes match.
- [ ] Tests: with a gated store, the merge issues the next row group's reads before a cursor runs dry when the budget allows (count rounds, not time); with a budget smaller than one row group per file it degrades to one row group per file and still completes; GET and byte counts equal the no-read-ahead counts; the held-bytes bound holds; every refusal test unchanged.
- [ ] Commit `perf(icetray-core): read the merge's row groups ahead under a byte budget`.

## Task 12: MultiScan spreads its keys across idle workers

**Files:** `crates/icetray-core/src/table.rs` (`Lookups::runs`, multi_scan), docs/rfd/025-ordered-rows.md (Limits or Cost), docs/performance.md, tests.

- [ ] MultiScan splits its keys into runs of at least `MULTI_SCAN_TASK_KEYS_MIN` (32) keys spread across idle workers through `Lookups::runs`, the gate MultiGet uses (it declines to split once calls in flight reach the worker count); below 32 keys a MultiScan stays on its task. Results, order, the receipt's counts, the value bound, and the response bound unchanged.
- [ ] Tests: a MultiScan of more than 32 keys answers identically split and unsplit (every key, both directions, repeated keys, a failing key, a dropped call's counts); the split happens only while workers are idle (assert through the gate, not timing).
- [ ] RFD 025 states that a MultiScan spreads as a MultiGet does; performance.md notes the latency/CPU trade with the Task 7 numbers.
- [ ] Commit `perf(icetray-core): spread a MultiScan's keys across idle workers`.

## Task 13: Warn when JSON output loses an order value's precision

**Files:** `crates/icetray/src/output.rs`, `docs/configuration.md` (Output), tests.

- [ ] When the doctor's or info's JSON output carries a long order value whose magnitude exceeds 2^53 (9,007,199,254,740,992), the command writes one warning to stderr naming the table and saying such values are exact as JSON text but lose precision in readers that parse numbers as doubles. The JSON itself is unchanged; text output never warns.
- [ ] configuration.md's Output section names the warning.
- [ ] Tests: a value at 2^53 does not warn, 2^53 + 1 and its negative do; text output never warns; stdout bytes unchanged.
- [ ] Commit `feat(icetray): warn when JSON output carries an order value past 2^53`.
