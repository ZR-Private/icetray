# Ordered Rows Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve ordered tables (many rows per key, sorted by key and an order column) through Scan and MultiScan, leaving every key-value table's checks and costs unchanged.

**Architecture:** An ordered table is a table entry with `order_column`. Load verifies rows strictly increase by (key, order), records per-file order facts, and checks uniqueness across files only among files that can repeat a pair. A Scan locates the key's run per file from the resident fences, reads spans of order and value pages through a scan page cache of its own, and merges the files' candidate rows by order value. Levels follow RFD 020, with edge keys shared by files one snapshot added at `balanced`.

**Tech Stack:** Rust (edition and toolchain per `rust-toolchain.toml`), parquet and arrow crates, iceberg 0.10, foyer, tonic and prost, tokio. Tests use the in-process stores and S3 server of `icetray-testkit`, seeded `rand::rngs::StdRng`, and Spark 4.1 golden tables.

**Spec:** `docs/rfd/025-ordered-rows.md`. Read it before each task; the plan argues from it.

## Global Constraints

- Read `CONTRIBUTING.md` and `AGENTS.md` first; they bind every task.
- Run cargo only through flox: `flox activate -- cargo ...`, `flox activate -- dev/ci-checks.sh`.
- Never add `#[allow(...)]`. Staging uses `#[expect(dead_code, reason = "read by <task>")]`, removed in the task that reads it; `dev/ci-checks.sh` fails on any left.
- Gate per task: `cargo test -p <crate>` while working; `dev/ci-checks.sh` and `cargo test --workspace --locked` before the task's commit.
- Commits: Conventional Commits, no co-author or generated-by trailers, the repository's pinned identity.
- Do not push. A push is the maintainer's decision.
- No sleeps in tests. Force concurrency with gates (`S3Fixture::hold`, `Gauged::fill_to`, `reach`).
- Tests are named `test_<fn>_<scenario>`, assert typed variants and counts, never message text, and assert that a counted quantity is non-zero where they claim one.
- No magic numbers: import production constants; a test literal gets a named const unless the assertion names it.
- No backward compatibility before 1.0: change formats, config keys, metric names, and proto fields in place; a local-directory file that does not match the running build is treated as absent (RFD 017).
- The change set breaks on-disk formats and interfaces, so the next release raises the minor version (`CONTRIBUTING.md`, Stability).
- Nothing in code, tests, docs, or commits names any company, internal system, or internal workload. Examples are generic: time series by key and timestamp, activity per user.
- A key-value table keeps every check, layout requirement, and cost per call. Every ordered-only rule is gated on `TableLayout::order` being `Some`.
- Defaults from the spec, exactly: `max_scan_rows` 1,000; `server.max_multi_scan_values` 4,000; `server.scan_deadline` 5 s; `server.multi_scan_deadline` 15 s; `server.scan_page_cache_bytes` 256 MiB; `long_runs` above 1,000 rows; `large_runs` above 4 MiB.

## Review Focus

These are the inputs the spec implies and no single task's happy path exercises. Each has its test in the owning task.

1. A run that crosses a row-group boundary and starts at row 0 of the second group returns every row in order (Task 8, `test_locate_run_spans_row_groups_from_row_zero`).
2. Paging with a limit equal to a run's live length sets `more` unset, one less sets it, and deleted rows at the run's edge never count (Task 9, `test_scan_more_counts_live_rows_only`).
3. A key-value entry and an ordered entry over one table under `disk` in one process each load, each keep their own chunk files, and neither trusts the other's (Task 6, `test_disk_entries_of_both_kinds_keep_their_own_chunk_files`).
4. A ref moved backward that drops a deletion vector revives a pair a later file repeats, and the refresh refuses `duplicate_rows` (Task 7, `test_refresh_refuses_a_pair_revived_by_dropped_deletes`).
5. Bounds at the extremes: `after = i64::MIN`, `before = i64::MAX`, an `int` or `date` bound beyond the 32-bit range, and a string bound equal to a stored value, each exclusive (Task 9, `test_scan_bounds_are_exclusive_at_the_extremes`).

---

## File Structure

New files:

| File | Responsibility |
|---|---|
| `crates/icetray-core/src/order.rs` | `OrderType`, `OrderValue`, `Direction`, `ScanBounds`, `OrderColumnLayout`, `OrderFacts`: the order column's types and its comparison |
| `crates/icetray-core/src/index/run.rs` | Finding a key's run in one file per tier, and reading a run's rows (order values and column values) |
| `crates/icetray-core/src/generation/ordered.rs` | Uniqueness across files for ordered tables: which files to check, page pruning, the (key, order) merge |
| `crates/icetray-core/src/scan.rs` | `Generation::scan` merge across files, `Scanned`/`ScanRow`, scan metrics |
| `crates/icetray-testkit/src/ordered.rs` | Ordered fixtures: rows per key, the order column, the naive-sort model `expected_scan` |
| `crates/icetray-testkit/tests/ordered.rs` | Integration tests and the model-based property test |
| `docs/analysis/2026-10-xx-ordered-rows-costs.md` | Task 14's measurements: Scan costs and the key-value regression run |

If `generation.rs` is a single file today, create `generation/ordered.rs` as `#[path]`-free submodule by moving `generation.rs` to `generation/mod.rs` in Task 7's first commit, with no other change in that commit.

Modified files are listed per task.

---

## Task 1: Server settings and the batch fan-out rename

**Files:**
- Modify: `crates/icetray-core/src/config.rs` (`ServerConfig` ~1149, `Default` ~1208, `DEFAULT_*` consts ~65-161, `ConfigViolation` ~335-634, `validate_bounds` ~1046, `restart_settings` ~758, tests `toml_full` ~2145, `json_full` ~2257, `full_config` ~2382, defaults ~2544, restart test ~2806, zero-limit table ~3546, fan-out parse test ~3564)
- Modify: every `TableHandles {` literal and use of `multi_get_fan_out` (open.rs:441, crates/icetray/src/serve.rs:976, testkit verify.rs:100-106, bench.rs:574-2621, table.rs tests)
- Modify: `docs/configuration.md` `[server]` table (~95-110), `dev/bench-grpc.sh` flag and JSON key

**Interfaces:**
- Produces: `ServerConfig { .., batch_fan_out: usize, max_multi_scan_values: usize, scan_deadline: Duration, multi_scan_deadline: Duration, scan_page_cache_bytes: u64 }`; consts `DEFAULT_BATCH_FAN_OUT = 64`, `DEFAULT_MAX_MULTI_SCAN_VALUES = 4_000`, `DEFAULT_SCAN_DEADLINE = 5 s`, `DEFAULT_MULTI_SCAN_DEADLINE = 15 s`, `DEFAULT_SCAN_PAGE_CACHE_BYTES = 256 MiB`; `TableHandles.batch_fan_out`; `ConfigViolation::{BatchFanOutZero, MaxMultiScanValuesZero, ScanDeadlineZero, MultiScanDeadlineZero}` (and `MultiGetFanOutZero` removed).

- [ ] **Step 1: Write the failing tests** in `config.rs`'s test module:

```rust
#[test]
fn test_parse_reads_the_scan_settings_and_their_defaults() {
    let config = parse_toml(TABLE_ONLY).unwrap();
    assert_eq!(config.server.batch_fan_out, DEFAULT_BATCH_FAN_OUT);
    assert_eq!(config.server.max_multi_scan_values, DEFAULT_MAX_MULTI_SCAN_VALUES);
    assert_eq!(config.server.scan_deadline, DEFAULT_SCAN_DEADLINE);
    assert_eq!(config.server.multi_scan_deadline, DEFAULT_MULTI_SCAN_DEADLINE);
    assert_eq!(config.server.scan_page_cache_bytes, DEFAULT_SCAN_PAGE_CACHE_BYTES);
}

#[test]
fn test_parse_refuses_multi_get_fan_out_as_unknown() {
    let toml = format!("[server]\nmulti_get_fan_out = 8\n{TABLE_ONLY}");
    assert!(parse_toml(&toml).is_err());
}
```

Extend the zero-limit table (~3546) with `batch_fan_out`, `max_multi_scan_values`, `scan_deadline`, `multi_scan_deadline`, and the fixtures `toml_full`, `json_full`, `full_config` with non-default values of all five. Extend the restart test so a change to each is named as restart-only. Use the existing helper names in that module (`parse_toml` stands for whatever the module's TOML parse helper is called).

- [ ] **Step 2: Run them and see them fail.** `flox activate -- cargo test -p icetray-core --lib config::` fails to compile on the missing fields.
- [ ] **Step 3: Implement.** Rename the field and const (update the const's doc comment, which cites `docs/analysis/2026-10-08-multi-get-fan-out.md`). Add the four settings with `#[serde(with = ...)]` duration parsing matching `get_deadline`. Add each to `validate_bounds`'s zero arrays and to `restart_settings`'s destructure. Thread `batch_fan_out` through `TableHandles` and `Lookups` (rename `multi_get_fan_out` there too). The new settings are not read yet: put `#[expect(dead_code, reason = "read by Task 9")]` on none of them, since `ServerConfig` fields are read by `restart_settings`; if clippy reports a field unread, stage it that way.
- [ ] **Step 4: Run the tests and see them pass**, then the whole crate.
- [ ] **Step 5: Docs.** In `docs/configuration.md`, rename the `multi_get_fan_out` row and add rows for the four settings with their defaults and one sentence each, taken from the spec's Limits and Caching bullets. In `dev/bench-grpc.sh` and `bench.rs`, rename `--multi-get-fan-out` to `--batch-fan-out` and the JSON key. Run `cargo test -p icetray-core --test doc_examples`.
- [ ] **Step 6: Gate and commit.** `feat(icetray-core): rename the batch fan-out and add the scan settings`

---

## Task 2: The snapshot that added each data file

**Files:**
- Modify: `crates/icetray-core/src/manifest.rs` (`DataFileEntry` ~563, `list_files` loop ~747-792, `data_sequence`/`inherited_sequence` ~980-1010, `testing::write_snapshot` ~1438-1476)
- Modify: `crates/icetray-testkit/src/fixture.rs` (`add_existing_file` path ~1244)

**Interfaces:**
- Produces: `DataFileEntry.added_by: SnapshotId` where `pub struct SnapshotId(pub i64)` (in `manifest.rs`, `Copy, Eq, Ord, Hash, Debug`); `fn added_snapshot(entry: &ManifestEntry, manifest: &ManifestFile) -> Result<SnapshotId>` raising `TableFault::SnapshotIdMissing { path, manifest }` when both are absent.

- [ ] **Step 1: Write the failing tests** in `manifest.rs`:

```rust
#[test]
fn test_added_snapshot_inherits_the_manifest_s_when_the_entry_leaves_it_null() {
    assert_eq!(added_snapshot_of(None, MANIFEST_SNAPSHOT), Some(SnapshotId(MANIFEST_SNAPSHOT)));
    assert_eq!(added_snapshot_of(Some(ENTRY_SNAPSHOT), MANIFEST_SNAPSHOT), Some(SnapshotId(ENTRY_SNAPSHOT)));
}

#[tokio::test]
async fn test_list_files_records_the_snapshot_that_added_each_file() {
    // Two appends, then a third snapshot that lists both files as existing.
    // Each entry's added_by is the snapshot of its append, not the third one.
}
```

`added_snapshot_of(own: Option<i64>, manifest: i64) -> Option<SnapshotId>` is the pure rule `added_snapshot` applies; it mirrors `inherited_sequence`'s split.

- [ ] **Step 2: Run them and see them fail.**
- [ ] **Step 3: Implement.** Read `entry.snapshot_id()` and fall back to `manifest.added_snapshot_id` (iceberg's own `inherit_data` rule). Fix `testing::write_snapshot`'s `Existing` case and the testkit's `add_existing_file` to pass the snapshot that added the file, which each fixture already records in its history, instead of the current one. `added_by` is read by Task 11; stage it with `#[expect(dead_code, reason = "read by Task 11's shared_edge_keys")]` if clippy flags it.
- [ ] **Step 4: Run the tests and the crate, see them pass.**
- [ ] **Step 5: Gate and commit.** `feat(icetray-core): record the snapshot that added each data file`

---

## Task 3: Chunk files named by the rules they were verified under

**Files:**
- Modify: `crates/icetray-core/src/local/mod.rs` (`VerifiedUnder` ~101, `chunk_path` ~413, `lookup` ~264, `write_chunks` ~290, `retain`/`keeps` ~323/445)
- Modify: `crates/icetray-core/src/local/format.rs` (`chunk_name` ~623, `ChunkFileHeader` ~188, `CHUNK_FILE_VERSION` ~36, `header_for` ~482, `from_header` ~308, `chunk_file_bytes` ~554)
- Modify: `crates/icetray-core/src/index/build.rs` (`verified_under` ~562, `reusable_disk_chunks` ~486, `Stale` ~447), `crates/icetray-core/src/open.rs` (~391 live set), `crates/icetray-core/src/generation.rs` (`identities` ~659)

**Interfaces:**
- Produces: `pub(crate) enum ChunkRules { KeyValue, Ordered { order_field_id: i32 } }` (`Copy, Eq, Hash, Debug`) in `local/mod.rs`; `VerifiedUnder { key_type, verifier_version, rules: ChunkRules }`; `chunk_name(path: &str, rules: ChunkRules) -> String` (key-value: the path digest; ordered: the digest of the path and the field id); `LocalDir::chunk_path(&self, &DataFileIdentity, ChunkRules)`; `lookup(&self, &DataFileIdentity, ChunkRules)`; `retain(&self, live: &HashSet<(DataFileIdentity, ChunkRules)>)`; `Generation::identities() -> impl Iterator<Item = (DataFileIdentity, ChunkRules)>`; header field `rules` (tag 9: `StoredRules { key_value | ordered { order_field_id } }`); `CHUNK_FILE_VERSION = 3`.

- [ ] **Step 1: Write the failing tests** in `local/mod.rs`:

```rust
#[test]
fn test_chunk_path_differs_by_rules_for_one_data_file() {
    let dir = tempfile::tempdir().unwrap();
    let (local, _) = open(dir.path());
    let id = identity();
    let kv = local.chunk_path(&id, ChunkRules::KeyValue);
    let ordered = local.chunk_path(&id, ChunkRules::Ordered { order_field_id: ORDER_FIELD_ID });
    let other = local.chunk_path(&id, ChunkRules::Ordered { order_field_id: ORDER_FIELD_ID + 1 });
    assert_ne!(kv, ordered);
    assert_ne!(ordered, other);
}

#[test]
fn test_lookup_rejects_a_file_whose_header_names_other_rules() {
    // Write under Ordered{1}, then rename it to the KeyValue name; lookup is Rejected(NameMismatch).
}

#[test]
fn test_retain_keeps_both_kinds_of_one_data_file() {
    // Write both rules for one identity; retain with both in the live set keeps both,
    // with only KeyValue it deletes the ordered one.
}
```

- [ ] **Step 2: Run them and see them fail.**
- [ ] **Step 3: Implement.** Key-value callers pass `ChunkRules::KeyValue` everywhere today's code names a chunk file; ordered callers arrive in Task 6. Bump `CHUNK_FILE_VERSION` so every existing chunk file is stale and rebuilt. Fix the `CHUNK_FILE_VERSION` doc to say a new version is rebuilt and never converted (already true after commit `ca8a96a`; keep it).
- [ ] **Step 4: Run local, index, and generation tests; see them pass.** `residency_disk.rs` and `shared_source.rs` in the testkit must pass unchanged.
- [ ] **Step 5: Gate and commit.** `feat(icetray-core): name chunk files by the rules that verified them`

---

## Task 4: Ordered fixtures and the naive-sort model

**Files:**
- Create: `crates/icetray-testkit/src/ordered.rs`
- Modify: `crates/icetray-testkit/src/fixture.rs` (`FixtureSpec` ~262, `create_table` ~1664, `write_data_files` ~1738, `batch` ~1822, manifest entry helpers ~1929-2002), `layout.rs` (`Layout`, `properties`), `keys.rs` (values), `merge_oracle.rs` (key by (key, order)), `lib.rs` (re-exports)

**Interfaces:**
- Produces, in `ordered.rs`:
  - `pub enum OrderKind { Long, Int, Date, Timestamp, TimestampTz, TimestampNs, TimestampTzNs, Str, Bin, Fixed(u32) }`
  - `pub struct Ordered { pub kind: OrderKind, pub rows_per_key: fn(u64) -> u64, pub start: i64, pub step: i64 }`: id `i` gets `rows_per_key(i)` rows with order values `start + step * j`, written in the kind's encoding (strings zero-padded to sort bytewise as numbers).
  - `pub const ORDER_FIELD_ID: i32 = 4;` and `pub const ORDER_COLUMN: &str = "ts";`
  - `pub fn order_value(kind: OrderKind, raw: i64) -> OrderValue` (the core type from Task 5; until Task 5 lands, the testkit's own `TestOrder` enum stands in and Task 5 replaces it).
  - `pub fn row_value(key: &Key, order: i64, len: usize, kind: ValueKind, version: u64) -> Vec<u8>`: a function of key, order value, and version (RFD 014).
  - `impl Fixture { pub fn expected_scan(&self, key: &Key, columns: &[&str], bounds: &TestScan) -> (Vec<(i64, Vec<Option<Vec<u8>>>)>, bool) }`: every live row of the key, sorted, cut by the exclusive bounds, ordered by direction, truncated to the limit, and whether rows remained.
  - `impl Fixture { pub async fn append_ordered(&mut self, catalog: &dyn Catalog, rows: &[(u64, i64)], prefix: &str) -> Result<()> }` and `delete_rows(&[(u64, i64)], DeleteForm, name)`, `update_rows(...)`, addressing rows by (id, order).
  - `FixtureSpec.ordered: Option<Ordered>` and `FixtureSpec::ordered(name, KeyType, Ordered) -> Self`.
  - `Layout.order_dictionary: bool`, `Layout.order_statistics: Statistics`, `Layout.row_group_rows` (exists) used to split runs across row groups.
- Consumes: nothing new from core except types already public.

- [ ] **Step 1: Write the failing tests** in `ordered.rs`'s test module:

```rust
#[tokio::test]
async fn test_write_fixture_writes_each_key_s_rows_sorted_by_order() -> Result<()> {
    let (_dir, catalog) = local_catalog();
    let spec = FixtureSpec::ordered("events", KeyType::Long, three_rows_per_key());
    let fixture = write_fixture(&*catalog, NAMESPACE, spec).await?;
    let oracle = MergeOracle::read(&fixture).await?;
    let key = fixture.key(SAMPLE[0]);
    let copies = oracle.copies_of(&key);
    assert_eq!(copies.len(), THREE);
    assert!(copies.windows(2).all(|pair| pair[0].order < pair[1].order));
    Ok(())
}

#[test]
fn test_expected_scan_applies_direction_bounds_and_limit() {
    // A hand-built row set; newest first, before = 3rd value, limit 1 -> one row and more = true.
}
```

- [ ] **Step 2: Run them and see them fail.**
- [ ] **Step 3: Implement.** `create_table` adds the order column (`ORDER_FIELD_ID`, the kind's Iceberg type) and a second ascending `SortField`. `write_data_files` expands each id into its rows, sorted by (key, order), and `Layout::properties` adds the order column's `SortingColumn`. Without `ordered`, the writer is unchanged. `MergeOracle` keys copies by (key, order) and `LiveDuplicate` fires on a live (key, order) pair twice.
- [ ] **Step 4: Run testkit unit tests; see them pass**, and the whole testkit (key-value fixtures unchanged).
- [ ] **Step 5: Gate and commit.** `test(icetray-testkit): ordered fixtures and their scan model`

---

## Task 5: The order column: types, configuration, resolution, footer stage

**Files:**
- Create: `crates/icetray-core/src/order.rs`
- Modify: `crates/icetray-core/src/config.rs` (`TableConfig` ~1489, manual `PartialEq` ~1829, `same_but_tuning` ~1788, `check_columns` ~1743, `validate` ~852), `manifest.rs` (`TableLayout` ~72, `derive` ~255, `for_entry` ~330), `column.rs` (`FileColumns` ~213, `resolve` ~229), `index/inspect.rs` (`Layout` ~97, `Layout::inspect` ~130, `kept_footer` ~317), `index/mod.rs` (`FileIndex::check_columns` ~907), `error.rs` (`RefusalReason` ~570 with `refusal_detail` ~482, `Recurs` ~736, `Display` ~851, test tables ~896/1127), `table.rs` (`judge_retune`/`retune` ~1239/1264), `lib.rs`
- Test: `crates/icetray-testkit/tests/refusals.rs`, `column_resolution.rs`

**Interfaces:**
- Produces, in `order.rs`:

```rust
pub enum OrderType { Int, Long, Date, Timestamp, TimestampTz, TimestampNs, TimestampTzNs, String, Binary, Fixed(u32) }
impl OrderType {
    pub fn from_iceberg(t: &PrimitiveType) -> Option<Self>;
    /// The Parquet physical type and logical annotation Iceberg writes for it.
    pub(crate) fn admits(&self, column: &ColumnDescriptor) -> Admit; // Admit::{Exact, PromotedFromInt, Mismatch}
    pub fn variant(&self) -> OrderVariant; // Long | Str | Bin
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OrderValue { Long(i64), Str(String), Bytes(Vec<u8>) }
pub enum OrderVariant { Long, Str, Bin }
pub(crate) struct OrderColumnLayout { pub name: String, pub field_id: i32, pub order_type: OrderType }
pub enum Direction { NewestFirst, OldestFirst }
/// `limit` 0 means the table's `max_scan_rows`; `after` and `before` are exclusive.
pub struct ScanBounds { pub direction: Direction, pub limit: u32, pub after: Option<OrderValue>, pub before: Option<OrderValue> }
```

`Direction` and `ScanBounds` are read by Task 8; stage them with `#[expect(dead_code, reason = "read by Task 8")]` until then.

- `TableConfig.order_column: Option<String>`, `TableConfig.max_scan_rows: Option<u32>` (`None` on an ordered table means `DEFAULT_MAX_SCAN_ROWS = 1_000`); `TableConfig::scan_rows(&self) -> Option<u32>`.
- `TableLayout.order: Option<OrderColumnLayout>`.
- `FileColumns.order_leaf: Option<usize>`; `Layout.order: Option<OrderLeaf { leaf, shape: OrderShape }>` with `OrderShape { Int32, Int64, Bytes, Fixed(u32) }`.
- `RefusalReason::OrderTypeUnsupported { column: String, got: String }`, `RefusalReason::OrderTypeMismatch { path: String, expected: OrderType, got: String }`.
- `ConfigViolation::{OrderColumnIsKey { table }, OrderColumnIsValue { table, column }, MaxScanRowsOnKeyValueTable { table }, MaxScanRowsZero { table }, MaxMultiScanValuesBelowScanRows { table, max_scan_rows, max_multi_scan_values }}`.

- [ ] **Step 1: Write the failing tests.** In `order.rs`: `test_from_iceberg_admits_every_order_type_and_refuses_others` (decimal, uuid, float, double, boolean are `None`); `test_order_values_compare_signed_and_bytewise`. In `config.rs`: one test per new violation, and `test_same_but_tuning_admits_a_max_scan_rows_change`. In testkit `refusals.rs`:

```rust
#[tokio::test]
async fn test_order_type_unsupported_is_refused() -> Result<()> { /* order column of type double */ }
#[tokio::test]
async fn test_order_type_mismatch_is_refused_for_millisecond_timestamps() -> Result<()> { /* RawFile with TIMESTAMP(MILLIS) */ }
#[tokio::test]
async fn test_int_order_promoted_to_long_is_served() -> Result<()> { /* schema long, file INT32 */ }
#[tokio::test]
async fn test_file_written_before_the_order_column_is_refused_as_column_mismatch() -> Result<()> { /* evolve Add, older file */ }
```

In `column_resolution.rs`: the order column resolves by name and field id like a value column.
- [ ] **Step 2: Run them and see them fail.**
- [ ] **Step 3: Implement.** `Layout::inspect` resolves the order leaf, checks its type with `OrderType::admits`, requires its offset index (`FileFault::MissingPageIndex` as for the key), and `kept_footer` keeps the order leaf's offset index and column index. `validate` refuses `max_multi_scan_values` below twice any ordered table's `max_scan_rows`. A table with `order_column` set still verifies under key-value rules until Task 6, so an ordered fixture with one row per key loads and repeated keys are `duplicate_keys`; that is the expected state at this commit and Task 6's tests replace it.
- [ ] **Step 4: Run tests; see them pass.**
- [ ] **Step 5: Gate and commit.** `feat(icetray-core): configure and resolve an order column`

---

## Task 6: Verifying an ordered file

**Files:**
- Modify: `crates/icetray-core/src/index/build.rs` (`KeysToVerify` ~213, `fetch_verified` ~262, `Verified` ~572, `verify_with` ~608-675, `RunCheck` ~741-809, `not_above_previous` ~915, `disk_chunks` ~320, `verified_under` ~562), `index/record.rs` (`VerificationRecord` ~31, `RowGroupRecord` ~42, `encode` ~110, `decode` ~143-213), `index/mod.rs` (`FileIndex` ~661: new fields; `resident_bytes` ~958; `FileFault` ~69), `index/inspect.rs` (`ResidentSet` ~441, `tiers` ~510, a parallel of `key_chunk_ranges` ~374), `index/page_header.rs` (`Entry` ~107: add `Int32`; an order-page check beside `check_value_page_headers` ~1021), `error.rs`
- Test: testkit `refusals.rs`, `residency_disk.rs`

**Interfaces:**
- Produces:
  - `pub(crate) struct OrderFacts { pub min: OrderValue, pub max: OrderValue, pub distinct_keys: Vec<u64>, pub longest_run: u64 }` in `order.rs`, one per file, `distinct_keys` per row group.
  - `Verified.order: Option<OrderFacts>`; `FileIndex.order: Option<OrderIndex { leaf: usize, facts: OrderFacts }>` (the leaf's offset and column index stay in the kept footer); `FileIndex::order_facts(&self) -> Option<&OrderFacts>`.
  - `fn order_chunk_ranges(metadata, order_leaf, file_size) -> Vec<Range<u64>>`.
  - `RefusalReason::DuplicateRows { path: String, key: Key, order: OrderValue }`, `RefusalReason::NullOrder { path: String, row_group: usize }`.
  - `FileFault::OrderNotUtf8 { row_group }`, `FileFault::OrderIndexMismatch { row_group, page }`.
  - Record tags: `VerificationRecord.order` (tag 3, message `OrderRecord { min: bytes, max: bytes, distinct_keys: repeated uint64, longest_run: uint64 }`).
  - `VERIFIER_VERSION = 2`.

- [ ] **Step 1: Write the failing tests** in `build.rs` (unit, on hand-built Parquet from `generation::testing::parquet`, extended with an order column) and testkit `refusals.rs`:

```rust
#[test]
fn test_verify_ordered_admits_a_key_repeated_with_rising_order() { /* keys 1,1,1,2 orders 1,2,3,1 */ }
#[test]
fn test_verify_ordered_refuses_an_equal_pair_as_duplicate_rows() { /* (1,2),(1,2) -> DuplicateRows{key:1, order:2} */ }
#[test]
fn test_verify_ordered_refuses_a_falling_order_as_unsorted() { /* (1,3),(1,2) */ }
#[test]
fn test_verify_ordered_checks_pairs_across_row_groups() { /* group 1 ends (1,5), group 2 starts (1,5) -> DuplicateRows */ }
#[test]
fn test_verify_ordered_refuses_null_order() {}
#[test]
fn test_verify_ordered_refuses_a_column_index_that_fails_to_bound_its_page() { /* hand-edit min/max */ }
#[test]
fn test_verify_ordered_records_min_max_distinct_keys_and_longest_run() {}
```

In `residency_disk.rs`: `test_disk_entries_of_both_kinds_keep_their_own_chunk_files` (Review Focus 3): one table, a key-value entry with `key_column = "key"` over a one-row-per-key copy and an ordered entry over the same files, both under `disk` in one `open`; both load, `index/` holds two chunk files per data file, and a restart reuses each without fetching chunks.
- [ ] **Step 2: Run them and see them fail.**
- [ ] **Step 3: Implement.** For an ordered table, `fetch_verified` fetches key and order chunks in one `read_ranges` call (one load GET per row group per column). `verify_with` decodes the order chunk alongside the key chunk batch by batch and compares (key, order) pairs: equal is `DuplicateRows`, lower is `Unsorted`. Keys may be equal across pages and row groups. Each order page's decoded values must lie within its column index entry when the entry is present (`OrderIndexMismatch` otherwise); a truncated byte-array entry is still a valid bound. Order pages pass the value-page header checks with `Entry::Int32` for an INT32 column. `record::decode` relaxes its strict fence order for ordered rules: fences and row-group edges may be equal. Under `disk`, an ordered file's chunk file holds key and order chunks and is written and looked up under `ChunkRules::Ordered { order_field_id }`. `ResidentSet` counts the order offset and column index in the footer and the order chunks in the chunk file. The chunk-file budget reserves an ordered chunk file from the footer's sizes, estimating string and binary order values in the record as `estimated_fence_bytes` estimates fences, and the descriptor count (`descriptors.rs`) and `retain` cover ordered chunk files as their own files. `DiskChunks::resident` samples the whole payload, so the residency sample covers order chunks with key chunks; a test asserts it. Key-value verification is unchanged; a test asserts a key-value file with repeated keys is still `duplicate_keys`.
- [ ] **Step 4: Run tests; see them pass.**
- [ ] **Step 5: Gate and commit.** `feat(icetray-core): verify ordered files by key and order value`

---

## Task 7: Uniqueness across files, and a refresh that costs its delta

**Files:**
- Move: `crates/icetray-core/src/generation.rs` to `generation/mod.rs` (first commit of this task, no other change)
- Create: `crates/icetray-core/src/generation/ordered.rs`
- Modify: `generation/mod.rs` (`Generation::assemble` ~276-329: branch on `layout.order`), `Span` ~1064
- Test: testkit `refresh.rs`, `refusals.rs`, `ordered.rs`

**Interfaces:**
- Produces, in `generation/ordered.rs`:

```rust
/// The files a refresh checks: new ones, and reused ones whose deletes shrank.
pub(super) fn checked(members: &[Member]) -> Vec<usize>;
/// For one checked file, the files it is checked against: those that can share a key
/// (buckets_may_share_a_key) whose key range and order range both intersect its own,
/// ends included.
pub(super) fn counterparts(spans: &[OrderedSpan<'_>], at: usize) -> Vec<usize>;
pub(super) struct OrderedSpan<'a> { pub bucketing: Option<Bucketing>, pub bucket: u32, pub keys: (&'a Key, &'a Key), pub orders: (&'a OrderValue, &'a OrderValue) }
/// The (key, order) merge of one checked file against its counterparts, reading only the
/// counterparts' order pages that can hold the checked file's rows.
pub(super) async fn refuse_duplicate_rows(table: &str, checked: &GroupMember, counterparts: &[GroupMember], deleted: &DeletedRows) -> Result<()>;
```

- Consumes: `FileIndex::order_facts` (Task 6), `buckets_may_share_a_key`, `GroupMember`, `DeletedRows`, `key_chunks_for_verification`.

- [ ] **Step 1: Write the failing tests** in testkit `refresh.rs` and `refusals.rs`:

```rust
#[tokio::test]
async fn test_refresh_reads_only_new_files_for_an_append_of_later_rows() -> Result<()> {
    // Ordered fixture, then append rows whose order values all exceed the table's max.
    // The refresh issues no GET to any reused file (Gauged or the GET counter per path).
}
#[tokio::test]
async fn test_refresh_reads_only_the_order_pages_late_rows_fall_among() -> Result<()> {
    // Append one row inside an existing file's order range; GETs to that file are its
    // order pages that can hold the row, a non-zero count below its page count.
}
#[tokio::test]
async fn test_duplicate_rows_across_files_are_refused_naming_the_later_file() -> Result<()> {}
#[tokio::test]
async fn test_new_files_of_one_refresh_are_checked_against_each_other() -> Result<()> {}
#[tokio::test]
async fn test_a_pair_whose_older_copy_is_deleted_is_served() -> Result<()> { /* update_rows */ }
#[tokio::test]
async fn test_refresh_refuses_a_pair_revived_by_dropped_deletes() -> Result<()> {
    // Review Focus 4: update a row (older copy deleted by a vector), serve, then move the
    // ref back to a snapshot without the vector but with both files; duplicate_rows.
}
```

- [ ] **Step 2: Run them and see them fail.**
- [ ] **Step 3: Implement.** For an ordered table, `assemble` replaces the group re-merge with `checked` and `counterparts`; key-value tables keep `overlap_groups` and `refuse_duplicates` unchanged. Page pruning: an order page of a counterpart is read when its rows' key range (from the fences of the key pages those rows span) and its column-index range both intersect the checked file's key and order ranges; a file without an order column index reads every order page in the key range. Under `full` it fetches pages by GET, under `disk` reads them from the ordered chunk file, under `bloom` fetches them with the key pages they span. A reused file whose deletes shrank fetches its own order chunks (and under `bloom` its key chunks).
- [ ] **Step 4: Run tests; see them pass**, including every existing `refresh.rs` and `merge_on_read.rs` test.
- [ ] **Step 5: Gate and commit.** `feat(icetray-core): check ordered uniqueness only where a pair can repeat`

---

## Task 8: Finding and reading a key's run in one file

**Files:**
- Create: `crates/icetray-core/src/index/run.rs`
- Modify: `index/mod.rs` (expose fences, groups, `decode_keys`, `fetch_pages` internals to `run.rs`), `parquet_io.rs` (`FileHandle` ~46: a span read with an explicit hedge window), `cache.rs` (a span entry point that splits into per-page entries)

**Interfaces:**
- Produces, in `run.rs`:

```rust
pub(crate) struct RunPart { pub row_group: usize, pub rows: Range<i64> }
pub(crate) struct Run { pub parts: Vec<RunPart> }
impl FileIndex {
    /// The key's run: per row group it spans, its exact rows. Full and Disk decode at most
    /// two key pages per row group; Bloom screens with the filter, then fetches those pages
    /// concurrently.
    pub(crate) async fn locate_run(&self, key: &Key, source: RunSource<'_>) -> Result<Option<Run>>;
    /// The candidate rows of the run for one Scan: up to `limit` live rows from the
    /// direction's end, cut by the bounds, plus one more live row's order value to set
    /// `more`. Reads the order column and the requested columns in one round when the order
    /// column index is present, two otherwise.
    pub(crate) async fn read_run(&self, run: &Run, bounds: &ScanBounds, limit: u32, columns: &[usize], deleted: &dyn Fn(RowPosition) -> bool, source: RunSource<'_>) -> Result<FileRows>;
}
pub(crate) struct FileRows { pub rows: Vec<(OrderValue, Vec<Option<Bytes>>)>, pub next: Option<OrderValue>, pub from_store: bool }
pub(crate) enum RunSource<'a> { Store { cache: &'a PageCache, issue: Issue<'a>, window: &'a HedgeWindow }, Disk(&'a DiskChunks) }
impl FileHandle {
    pub(crate) async fn read_span(&self, range: Range<u64>, window: &HedgeWindow) -> Result<Bytes>;
}
impl PageCache {
    /// Serves each page of `pages` from the cache, fetching every maximal contiguous run
    /// of uncached pages with one call of `fetch` and inserting each page as its own entry.
    pub(crate) async fn get_span<F, Fut>(&self, file: &Arc<str>, pages: &[Range<u64>], issue: Issue<'_>, fetch: F) -> Result<(Vec<Bytes>, PageOrigin)>;
}
```

- Consumes: `FileIndex` fences and groups, `ScanBounds`/`OrderValue` (Task 5), deletes via the passed closure.

- [ ] **Step 1: Write the failing tests** in `run.rs` (unit, over `generation::testing` tables with an order column and small `page_rows` and `row_group_rows`):

```rust
#[tokio::test]
async fn test_locate_run_inside_one_page() {}
#[tokio::test]
async fn test_locate_run_spanning_pages_decodes_at_most_two() { /* count decodes via a test hook or key-page reads */ }
#[tokio::test]
async fn test_locate_run_spans_row_groups_from_row_zero() { /* Review Focus 1 */ }
#[tokio::test]
async fn test_locate_run_for_a_key_equal_to_a_fence() {}
#[tokio::test]
async fn test_locate_run_misses_without_a_get() {}
#[tokio::test]
async fn test_read_run_newest_first_takes_the_last_live_rows() {}
#[tokio::test]
async fn test_read_run_reports_the_next_live_row_for_more() {}
#[tokio::test]
async fn test_read_run_with_a_bound_reads_one_round_with_a_column_index() { /* GET count = columns + 1 */ }
#[tokio::test]
async fn test_read_run_without_an_order_column_index_reads_two_rounds() {}
#[tokio::test]
async fn test_get_span_fetches_contiguous_uncached_pages_once_and_caches_each() {}
```

- [ ] **Step 2: Run them and see them fail.**
- [ ] **Step 3: Implement.** Run location per row group uses the resident first and last key of each group to pick the groups, then `partition_point(fence < key)` and `partition_point(fence <= key)` to bound the pages; decoding the first and last of those gives the exact rows. Live rows are counted against the deleted-rows closure without I/O. With a bound and an order column index, the pages that can hold the bound are selected from the index (pages strictly inside the run carry exact ranges, the run's edge pages include neighbouring keys' values), and the order pages and value pages over the candidate rows are read in one round. Span GETs go through `FileHandle::read_span` with the scan hedge window.
- [ ] **Step 4: Run tests; see them pass.**
- [ ] **Step 5: Gate and commit.** `feat(icetray-core): find and read a key's run in one file`

---

## Task 9: Scan and MultiScan in core

**Files:**
- Create: `crates/icetray-core/src/scan.rs`
- Modify: `generation/mod.rs` (`Generation::scan`), `table.rs` (`Table::scan`, `Table::multi_scan`, `TableHandles.scan_cache`, `Lookups`, a second `HedgeWindow`, scan metrics, the kind checks in `get`/`multi_get`), `open.rs` (build the scan cache; `Deadlines.scan`), `cache.rs` (a `name` label on its metric series), `metric.rs` (`DESCRIPTIONS`), `error.rs` (`Error::ScanRequest`, `Error::WrongTableKind`), `lib.rs`
- Test: `crates/icetray-testkit/tests/ordered.rs`, `table.rs` tests (fan-out)

**Interfaces:**
- Produces:

```rust
// scan.rs (Direction and ScanBounds come from Task 5's order.rs)
pub struct ScanRow { pub order: OrderValue, pub values: Vec<Option<Bytes>> }
pub struct Scanned { pub rows: Vec<ScanRow>, pub more: bool }
pub struct ScanLookup { pub scanned: Scanned, pub snapshot_id: i64 }
pub struct MultiScanLookup { pub results: Vec<Scanned>, pub snapshot_id: i64 }
pub enum ScanRequestFault { LimitAboveMax { limit: u32, max: u32 }, BoundVariant { expected: OrderVariant }, BoundsInverted, TooManyValues { keys: usize, limit: u32, columns: usize, max: usize } }
// table.rs
impl Table {
    pub async fn scan(&self, key: &Key, columns: &[ColumnRead<'_>], bounds: &ScanBounds) -> Result<ScanLookup>;
    pub async fn multi_scan(&self, keys: &[Key], columns: &[ColumnRead<'_>], bounds: &ScanBounds) -> Result<MultiScanLookup>;
}
pub enum TableKind { KeyValue, Ordered }
// error.rs
Error::ScanRequest(ScanRequestFault), Error::WrongTableKind { table: String, kind: TableKind }
// metric.rs
SCANS_TOTAL = "icetray_scans_total" {table, result}, SCAN_ROWS = "icetray_scan_rows", SCAN_FILES = "icetray_scan_files", SCAN_SECONDS = "icetray_scan_seconds" {table, temperature}
```

- Consumes: Tasks 5, 6, 8.

- [ ] **Step 1: Write the failing tests** in testkit `tests/ordered.rs`:

```rust
const SCAN_SEED: u64 = 0x5ca9_0025;
const INSTANCES: usize = 40;

#[tokio::test]
async fn test_scan_matches_the_model_in_every_tier() -> Result<()> {
    let mut rng = StdRng::seed_from_u64(SCAN_SEED);
    for _ in 0..INSTANCES {
        let spec = random_ordered_spec(&mut rng); // key and order kinds, rows per key, page and row-group rows, buckets
        let (fixture, tiers) = serve_ordered_in_every_tier(spec, &mut rng).await?; // appends, deletes, updates drawn too
        for _ in 0..KEYS_PER_INSTANCE {
            let (key, columns, bounds) = random_scan(&fixture, &mut rng);
            let expected = fixture.expected_scan(&key, &columns, &bounds.to_test());
            for served in tiers.all() {
                let got = served.table.scan(&key, &reads(&columns), &bounds).await?;
                assert_eq!(rows_of(&got.scanned), expected.0);
                assert_eq!(got.scanned.more, expected.1);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_multi_scan_matches_scan_for_each_key_from_one_snapshot() -> Result<()> {}
#[tokio::test]
async fn test_paging_by_order_value_returns_the_whole_history_once() -> Result<()> {}
#[tokio::test]
async fn test_scan_more_counts_live_rows_only() -> Result<()> {} // Review Focus 2
#[tokio::test]
async fn test_scan_bounds_are_exclusive_at_the_extremes() -> Result<()> {} // Review Focus 5
#[tokio::test]
async fn test_cold_scan_of_a_compacted_table_costs_one_get_per_column_and_one_for_order() -> Result<()> {}
#[tokio::test]
async fn test_scan_reads_only_files_that_hold_the_key() -> Result<()> {}
#[tokio::test]
async fn test_get_on_an_ordered_table_is_wrong_table_kind() -> Result<()> {}
#[tokio::test]
async fn test_scan_pages_stay_out_of_the_key_value_cache() -> Result<()> {}
```

In `table.rs` tests, following `two_columns_one_row_per_page` and `reach`: `test_scan_holds_at_most_batch_fan_out_gets` and `test_multi_scan_holds_at_most_batch_fan_out_gets`.
- [ ] **Step 2: Run them and see them fail.**
- [ ] **Step 3: Implement.** `Generation::scan` takes the candidates that may contain the key, locates each file's run, reads every file's candidate rows in one round under one `Semaphore::new(batch_fan_out)`, merges by order value in the requested direction, takes `limit` (0 means `max_scan_rows`), and sets `more` from the merged next values. `Table::scan` checks the table kind, the key variant, the selection, the limit and bound variants and order, then runs under the scan cache (built in `open` with `scan_page_cache_bytes`, no disk level, `with_fetch_deadline(scan_deadline)`) and the table's span hedge window. `multi_scan` checks `keys × limit × (columns + 1)` against `max_multi_scan_values` before reading. Label the page cache's metric series with `cache = "lookup" | "scan"` and update `docs/operating.md`. Count the order column's GETs in `icetray_value_page_gets_total` under the order column's name, so a caller budgets a Scan's GETs from that series. Record scan metrics; register histogram buckets in the binary in Task 10.
- [ ] **Step 4: Run tests; see them pass.**
- [ ] **Step 5: Gate and commit.** `feat(icetray-core): Scan and MultiScan`

---

## Task 10: Scan and MultiScan on the wire

**Files:**
- Modify: `crates/icetray-proto/proto/icetray.proto`, `crates/icetray-proto/build.rs` (`BYTES_FIELDS`), `crates/icetray-proto/tests/contract.rs`
- Modify: `crates/icetray/src/service.rs` (handlers, `answer_*`, `status_from`, `table_info`), `error.rs` (`CallFault`, wire faults), `telemetry.rs` (`Call`, histogram buckets), `serve.rs`
- Test: `crates/icetray/tests/serve.rs`

**Interfaces:**
- Produces, in the proto:

```proto
rpc Scan(ScanRequest) returns (ScanResponse);
rpc MultiScan(MultiScanRequest) returns (MultiScanResponse);

message OrderValue { oneof value { int64 long = 1; string str = 2; bytes bin = 3; } }
enum Direction { DIRECTION_UNSPECIFIED = 0; DIRECTION_NEWEST_FIRST = 1; DIRECTION_OLDEST_FIRST = 2; }
message ScanRequest { string table = 1; Key key = 2; repeated ColumnRead columns = 3; Direction direction = 4; uint32 limit = 5; OrderValue after = 6; OrderValue before = 7; }
message ScanRow { OrderValue order = 1; repeated ColumnValue values = 2; }
message ScanResponse { repeated ScanRow rows = 1; bool more = 2; int64 snapshot_id = 3; }
message MultiScanRequest { string table = 1; repeated Key keys = 2; repeated ColumnRead columns = 3; Direction direction = 4; uint32 limit = 5; OrderValue after = 6; OrderValue before = 7; }
message ScanResult { repeated ScanRow rows = 1; bool more = 2; }
message MultiScanResponse { repeated ScanResult results = 1; int64 snapshot_id = 2; }
enum OrderType { ORDER_TYPE_UNSPECIFIED = 0; ORDER_TYPE_INT = 1; ORDER_TYPE_LONG = 2; ORDER_TYPE_DATE = 3; ORDER_TYPE_TIMESTAMP = 4; ORDER_TYPE_TIMESTAMPTZ = 5; ORDER_TYPE_TIMESTAMP_NS = 6; ORDER_TYPE_TIMESTAMPTZ_NS = 7; ORDER_TYPE_STRING = 8; ORDER_TYPE_BINARY = 9; ORDER_TYPE_FIXED = 10; }
// TableInfoResponse gains: string order_column = 9; OrderType order_type = 10; uint32 order_width = 11; uint32 max_scan_rows = 12;
```

Comments on each message follow the existing style: what the server returns, and which requests are `INVALID_ARGUMENT`, as the spec's Wire bullet states.

- [ ] **Step 1: Write the failing tests.** In `contract.rs`: round trips for every new message, the `Direction` and `OrderType` enum tables with unknown values, `SERVICE_FULL_NAME` unchanged. In `crates/icetray/tests/serve.rs`:

```rust
#[tokio::test]
async fn test_scan_returns_rows_more_and_the_snapshot_id() {}
#[tokio::test]
async fn test_scan_with_unspecified_direction_is_invalid_argument() {}
#[tokio::test]
async fn test_scan_with_a_limit_above_max_scan_rows_is_invalid_argument() {}
#[tokio::test]
async fn test_scan_with_inverted_bounds_is_invalid_argument() {}
#[tokio::test]
async fn test_scan_on_a_key_value_table_is_invalid_argument() {}
#[tokio::test]
async fn test_get_on_an_ordered_table_is_invalid_argument() {}
#[tokio::test]
async fn test_multi_scan_over_max_values_is_invalid_argument_before_any_read() {}
#[tokio::test]
async fn test_multi_scan_answers_a_repeated_key_at_each_position() {}
#[tokio::test]
async fn test_table_info_reports_the_order_column_and_max_scan_rows() {}
```

Extend `test_requests_total_counts_every_call` and `test_alerting_series_are_exposed_from_the_start` for the new calls and series.
- [ ] **Step 2: Run them and see them fail.**
- [ ] **Step 3: Implement** the handlers with `slots.admit()` and `within(scan_deadline | multi_scan_deadline)`, `order_value_from_wire` checked against the table's `OrderVariant`, the `status_from` arms (`ScanRequest` and `WrongTableKind` are `INVALID_ARGUMENT`), `Call::{Scan, MultiScan}` in `ALL`, `index`, `method`, `Display` (`scan`, `multi_scan`), and `set_buckets_for_metric` for `SCAN_SECONDS`, `SCAN_ROWS`, `SCAN_FILES`.
- [ ] **Step 4: Run tests; see them pass.**
- [ ] **Step 5: Gate and commit.** `feat(icetray): serve Scan and MultiScan`

---

## Task 11: Levels and findings for ordered tables

**Files:**
- Modify: `crates/icetray-core/src/check.rs` (`FindingCode` ~273, `ALL` ~337, `cap` ~360, `raised_at_load` ~384, `per_column` ~409, `Display` ~432, `FileView` ~1118, `layout_caps` ~1199, `file_capping_findings` ~1598, `table_capping_findings` ~1806, `file_layout_advice` ~1654, `bloom_extremes` ~1319, `bloom_findings` ~1759, `WriterProperty` ~469, tests ~2086-2402), `generation/mod.rs` (`widest_per_bucket` ~1617, `Planned::judge_level` ~753, `Planned::overlapping` ~799), `table.rs` (`write_generation` ~1752: `TABLE_GETS_PER_SCAN`), `metric.rs`
- Test: testkit `findings.rs`; `crates/icetray/tests/serve.rs` (gauge test ~1136)

**Interfaces:**
- Produces: `FindingCode::{SharedEdgeKeys, DictionaryEncodedOrder, OrderIndexMissing, OrderIndexTruncated, LongRuns, LargeRuns}` with caps `Balanced` for the first three; `metric::TABLE_GETS_PER_SCAN = "icetray_table_gets_per_scan"`; `fn shared_edges(spans: &[Span], added_by: &[SnapshotId]) -> BTreeMap<u32, usize>` returning, per bucket, the most files one edge key spans among files one snapshot added.
- Consumes: `DataFileEntry.added_by` (Task 2, remove its staging marker), `OrderFacts` (Task 6).

- [ ] **Step 1: Write the failing tests** in testkit `findings.rs`, one per code, each asserting the code at its cap and the exact capping and advice set:

```rust
#[tokio::test]
async fn test_shared_edge_keys_caps_a_compaction_rolled_by_size_at_balanced() -> Result<()> {}
#[tokio::test]
async fn test_a_later_file_touching_an_edge_key_is_overlapping_files() -> Result<()> {}
#[tokio::test]
async fn test_a_compacted_ordered_table_in_one_file_per_bucket_is_optimal() -> Result<()> {}
#[tokio::test]
async fn test_dictionary_encoded_order_caps_at_balanced_under_full() -> Result<()> {}
#[tokio::test]
async fn test_dictionary_encoded_order_leaves_disk_optimal() -> Result<()> {}
#[tokio::test]
async fn test_order_index_missing_caps_at_balanced() -> Result<()> {}
#[tokio::test]
async fn test_order_index_truncated_is_advice() -> Result<()> {}
#[tokio::test]
async fn test_long_runs_is_advice_above_one_thousand_rows() -> Result<()> {}
#[tokio::test]
async fn test_large_runs_is_advice_above_four_mib() -> Result<()> {}
#[tokio::test]
async fn test_bloom_findings_divide_by_distinct_keys_for_an_ordered_table() -> Result<()> {}
#[tokio::test]
async fn test_strict_gate_judges_shared_edges_before_any_chunk() -> Result<()> {}
```

In `serve.rs`, change the gauge scrape test to serve a key-value and an ordered table and assert each gauge in `DESCRIPTIONS` on the first scrape of the table kind that publishes it.
- [ ] **Step 2: Run them and see them fail.**
- [ ] **Step 3: Implement.** For an ordered table, a pair of files in one bucket that meet only where one's last key is the other's first and share `added_by` is a shared edge, not overlap, and `widest_per_bucket` counts the rest as before; the strict gate computes both from key ranges, edge keys, and `added_by`. `dictionary_encoded_order` and `order_index_missing` cap under `full` and `bloom` only. `long_runs` and `large_runs` use fixed thresholds in the `thresholds` module (1,000 rows; 4 MiB from the footer's uncompressed bytes per row over the configured value columns). Bloom findings divide a row group's filter bits by `OrderFacts.distinct_keys` for ordered tables and by rows otherwise.
- [ ] **Step 4: Run tests; see them pass.**
- [ ] **Step 5: Gate and commit.** `feat(icetray-core): levels and findings for ordered tables`

---

## Task 12: The doctor and info for ordered tables

**Files:**
- Modify: `crates/icetray-core/src/check.rs` (`FileReport` ~661, `TableSummary` ~755, `measure` ~1358), `crates/icetray/src/output.rs` (`SummaryLine` ~105, `FileLine` ~143), `crates/icetray/src/oneshot.rs` (`info` ~151)
- Test: `crates/icetray/tests/cli.rs`

**Interfaces:**
- Produces: `TableSummary.order: Option<OrderSummary { column, order_type, max_scan_rows, longest_run }>`; `FileReport.order: Option<FileOrderReport { min, max, longest_run, distinct_keys }>`.

- [ ] **Step 1: Write the failing tests** in `cli.rs`: `test_doctor_reports_an_ordered_table_s_order_facts`, `test_info_reports_the_order_column`, and `test_doctor_reports_duplicate_rows_with_exit_failure`.
- [ ] **Step 2: Run them and see them fail.**
- [ ] **Step 3: Implement** the report fields and their JSON lines (the doctor's output contract; update `docs/configuration.md` "Output" in Task 15).
- [ ] **Step 4: Run tests; see them pass.**
- [ ] **Step 5: Gate and commit.** `feat(icetray): report ordered tables in the doctor and info`

---

## Task 13: Golden ordered tables from Spark

**Files:**
- Modify: `dev/golden-tables.py`, `crates/icetray-testkit/src/golden.rs`, `crates/icetray-testkit/tests/golden.rs`, `docs/development.md`

**Interfaces:**
- Produces: recipe group `ordered` with tables `ordered_compacted_100k`, `ordered_appended_100k`, `ordered_overlap_100k`, `ordered_merge_v2_100k`, `ordered_merge_v3_100k`, `ordered_merge_v3_compacted_100k`; `GoldenTable` variants of the same names.

- [ ] **Step 1: Extend the recipe.** `create(...)` takes an optional order column (`ts BIGINT`) and writes `ALTER TABLE ... WRITE LOCALLY ORDERED BY key, ts`. Change functions: an append of later order values, an overlapping append with skew splitting on, a `MERGE INTO t USING s ON t.key = s.key AND t.ts = s.ts WHEN MATCHED THEN UPDATE SET value = s.value WHEN NOT MATCHED THEN INSERT *` under `MERGE_ON_READ` (position deletes in v2, deletion vectors in v3), and `rewrite_data_files` with `sort_order => 'key ASC, ts ASC'`.
- [ ] **Step 2: Mirror the tables in `golden.rs`** with their key, order, and value derivations.
- [ ] **Step 3: Write the ignored tests** (`#[ignore = "needs ICETRAY_GOLDEN_WAREHOUSE; run dev/test-golden.sh"]`): every table's Scans match the derived rows for sampled keys in both directions with bounds; levels are `optimal` for the compacted table when one file holds each bucket, `balanced` with `shared_edge_keys` when Spark rolled files, and `permissive` with `overlapping_files` after appends.
- [ ] **Step 4: Generate and run.** `dev/runtime.sh up`, `dev/golden-tables.sh --only ordered`, `ICETRAY_GOLDEN_WAREHOUSE=target/golden-warehouse dev/test-golden.sh`. Record the result lines in the commit message.
- [ ] **Step 5: Gate and commit.** `test(icetray-testkit): golden ordered tables from Spark`

---

## Task 14: Benchmarks and the key-value regression run

**Files:**
- Modify: `crates/icetray-testkit/examples/bench.rs` (`Scenario` ~191 and its classifiers, `TableShape`/`SHAPES` ~710-721, `Op` ~1495, `lookups` ~1840, `serve_together` ~1958), `dev/bench-grpc.sh`
- Create: `docs/analysis/2026-10-xx-ordered-rows-costs.md` (dated the day of the run)

**Interfaces:**
- Produces: `TableName::Ordered` with an ordered `TableShape`; scenarios `scan-warm`, `scan-cold`, `scan-bounded`, `multi-scan-10`, `multi-scan-10-cold`, and `kv-beside-ordered`, which runs `warm-hit`, `cold-hit`, `miss`, `multi-get-100` on the long key-value table in a process that also serves the ordered table under shared handles.

- [ ] **Step 1: Add the shape and scenarios**, each with its branch in every exhaustive classifier and in `ALL`/`LOOKUPS`, and `Op::Scan`/`Op::MultiScan` that fail the run on a response that disagrees with the model.
- [ ] **Step 2: Run the key-value regression.** Build `bench` at the baseline commit, the one before Task 1, in a separate worktree, and at HEAD; point both at one `--data-dir`; run `warm-hit,cold-hit,miss,multi-get-100,multi-get-1000` three times per binary, alternating, on `--table long --tier full,disk,bloom`; then `kv-beside-ordered` at HEAD. Compare medians as `docs/analysis/2026-09-26-performance-sweep.md` does.
- [ ] **Step 3: Run the scan scenarios** in every tier.
- [ ] **Step 4: Write the analysis** with the method, the medians, and the conclusion. A key-value regression beyond run-to-run noise is a finding to report to the maintainer before Task 15, not to tune around.
- [ ] **Step 5: Gate and commit.** `test(icetray-testkit): bench ordered scans and the key-value path beside them`

---

## Task 15: Documentation and RFD update notes

**Files:**
- Modify: `docs/table-layout.md` (requirements rows for ordered tables and a writer recipe: `WRITE LOCALLY ORDERED BY key, ts`, bucketing, `MERGE` on (key, ts), compaction with the sort order, that streaming writers leaving files unsorted are served after compaction), `docs/configuration.md` (table settings `order_column`, `max_scan_rows`; output lines), `docs/calling.md` (Scan and MultiScan, paging by order value, `more`, limits, errors), `docs/operating.md` (metrics, finding codes, refusal reasons, the scan page cache, `lookups = "permissive"` for appended tables), `docs/performance.md` (Scan costs from Task 14), `docs/glossary.md` (ordered table, run, order column, shared edge key), `README.md` if its feature list names the read calls
- Modify: RFD update notes, each one line in the form `> Updated 2026-MM-DD: ... See RFD 025.`, on RFDs 002, 004, 005, 006, 007, 010, 011, 013, 016, 017, 018, 020, 021, 022, 023, and in-place edits where their text describes key-value tables as the only kind
- Delete: this plan, `docs/plans/2026-10-13-ordered-rows.md`

- [ ] **Step 1: Write the user docs** by the "User documentation" rules in `CONTRIBUTING.md`, in the house voice: affirmative statements, the real actor, established terms. Every TOML example must parse (`cargo test -p icetray-core --test doc_examples`).
- [ ] **Step 2: Write the RFD update notes** and in-place edits.
- [ ] **Step 3: Delete this plan** in the same commit.
- [ ] **Step 4: Gate and commit.** `docs: ordered tables`

---

## Execution notes

- Tasks 1-3 change nothing a key-value caller sees beyond the renamed setting and rebuilt chunk files.
- Tasks 4-7 make ordered tables load; until Task 9, an ordered table loads and serves nothing.
- Task 8 and Task 9 share types through `order.rs`; Task 9 consumes `FileRows` exactly as Task 8 defines it.
- Each task ends with `dev/ci-checks.sh` and `cargo test --workspace --locked` green, and its commit SHA reported.
- After Task 9 and after Task 11, run an adversarial review of the cost paths, as RFD 014 requires for a cost-path change.
