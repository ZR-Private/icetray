# Clean build: design blueprint

The shape of the rebuilt icetray, fixed before any code is written so that
the crates can be built in parallel against agreed seams. This document
implements RFDs 002 through 017. It is consumed during the build and can be
deleted once the code and its tests embody it; the RFDs outlive it.

Rules that hold throughout: one owner per concept (the module named here is
the only place that concept is defined or computed); dependencies point in
one direction (listed per crate below); every invariant in section 6 has a
test at the tier named there, written before or with the code it pins; every
closed set is an enum and every error is a typed variant, per
`CONTRIBUTING.md` ("Types over strings", "Errors", "Protobuf"), so nothing
below carries a string code, and no test matches on message text.

## 1. Crates and dependency direction

| crate | kind | depends on (workspace) | purpose |
|---|---|---|---|
| `icetray-core` | library | none | everything from bytes to lookups: keys, storage, catalog, manifests, index, cache, generations, doctor, config |
| `icetray-proto` | library | none | the gRPC contract, generated at build time |
| `icetray` | binary + thin library | core, proto | one binary: `serve` (the daemon: service, table lifecycle, poll loop, twelve-factor surface) and the one-shot `doctor`, `get`, `info` as JSON lines |
| `icetray-testkit` | library | core (dev: the `icetray` crate depends on testkit) | in-process S3, fixture writer, simulation, shared assertions, golden-table locator |

`icetray-core` never depends on the testkit (its unit tests write Parquet
with `arrow` as a dev-dependency). Every subcommand of the binary opens
tables through one core function and shares one flag vocabulary. Pinned
dependency versions live once, in `[workspace.dependencies]`.

## 2. `icetray-core` module map and owners

Modules in dependency order. "Owns" is the concept no other module may
redefine.

| module | owns | RFD |
|---|---|---|
| `error` | the one `Error` enum and `Result` | |
| `approx` | integer-to-float conversion (`ratio`, `gauge`, `count`, `len`); the only `cast_precision_loss` allow | 015 |
| `key` | `Key`, `KeyType`, key ordering (numeric for long, bytewise for string and binary), key/statistics comparison | 008 |
| `bucket` | the Iceberg bucket transform (murmur3 over the key's bytes) | 008 |
| `contract` | the serving layout's numbers as named constants (page rows, value page byte bound, key length cap and advice, file size target); the one home for every constant another crate needs | 002, 008 |
| `config` | the configuration schema and its validation; TOML and JSON parsing; defaults | 011 |
| `storage` | every byte read from object storage: store per prefix, location rewriting, request counting, credential chain with cached refresh, hedging | 003, 013 |
| `local` | the optional local directory: chunk files for disk residency (naming by data-file identity, reuse on restart, deletion with the generation), a disk budget, local-read counting, and the `DiskTier` handle `cache` uses for its disk level | 017, 012 |
| `catalog` | where the current metadata comes from (static root with version hint, metadata file, any catalog incl. REST); one-read change detection | 005 |
| `manifest` | snapshot to live data files; table layout (key and value columns, bucket transform) derived from the snapshot's schema and each manifest's partition spec; delete-file refusal | 005, 009 |
| `parquet_io` | Parquet byte access: one-GET footer load, `Segments` (in-memory `ChunkReader` that never fetches) | 004, 006 |
| `index` | the per-file resident index: key chunks, fences, exact per-row-group ranges, `find` (no I/O), `read_value` (one page through the cache) | 004, 006 |
| `cache` | the shared value-page cache keyed by (file, page offset) with fetch coalescing | 004 |
| `generation` | `Generation` (one snapshot, fully indexed, deterministic file order), overlap grouping and duplicate refusal, the residency budget check at the footer stage | 005, 017 |
| `check` | the doctor: tiers, finding codes, penalties, thresholds table | 007, 016 |
| `table` | `Table`: lifecycle, serialized refresh, atomic swap, and the `strict` gate, which runs `check::diagnose` on a built generation before it is swapped in | 005, 007 |
| `open` | opening every configured table over one storage, one cache, and the local directory, for every subcommand | 012 |

What moves relative to the prototype: REST catalog construction moves from
the server into `catalog`; the version-hint writer moves to the testkit; the
CLI's and server's duplicate "open tables" code becomes `open`; `Table`
leaves `generation` for its own module, so the strict gate can call the
doctor without inverting the dependency direction.

## 3. Frozen public signatures (`icetray-core`)

These are the seams other crates and parallel workers build against. Bodies
and private items are free; changing a signature below means updating this
document first.

```rust
// error
pub enum Error {
    Iceberg(#[from] iceberg::Error),
    Parquet(#[from] parquet::errors::ParquetError),
    ObjectStore(#[from] object_store::Error),
    ObjectStorePath(#[from] object_store::path::Error),
    Json(#[from] serde_json::Error),
    Config(#[from] ConfigError),
    Cache(#[from] CacheError),
    /// The table cannot be served correctly.
    Refused { table: String, reason: RefusalReason },
    /// The table serves at a cost and `strict` is on.
    Inefficient { table: String, findings: Vec<FindingCode> },
    KeyType { table: String, expected: KeyType, got: KeyType },
    /// A data file is malformed for serving; the fault set is closed.
    File { path: String, fault: FileFault },
    /// The table has no serving generation yet.
    NotReady { table: String },
}
pub type Result<T> = std::result::Result<T, Error>;

/// Named refusals (RFD 007). Display gives the snake_case name used in logs.
pub enum RefusalReason { Unsorted, NullKeys, DuplicateKeys, KeyTypeMismatch, ValueTypeUnsupported, DeleteFiles, KeyTooLong, OverBudget { needed: u64, budget: u64 }, NoBloomFilter { row_group: usize }, NoLocalDir }
/// A snapshot id is not an integer; it is never compared with one.
pub struct SnapshotId(pub i64);
/// Metric label values for lookups.
pub enum LookupResult { ColdHit, WarmHit, Miss }

// key
pub enum KeyType { Long, String, Binary, Uuid }
impl KeyType {
    /// long -> Long; string -> String; binary and fixed -> Binary; uuid -> Uuid (RFD 008).
    pub fn from_iceberg(column: &str, ty: &iceberg::spec::Type) -> Result<Self>;
}
/// `Ord` is the served order per type: numeric, UTF-8 bytes, bytewise, and for
/// `Uuid` the order the Iceberg specification defines, conformance-tested
/// against the reference implementation rather than assumed (RFD 008).
pub enum Key { Long(i64), Str(String), Bytes(Vec<u8>), Uuid([u8; 16]) }
impl Key {
    pub fn key_type(&self) -> KeyType;
    pub fn from_datum(datum: &iceberg::spec::Datum) -> Option<Self>;
    /// The bytes the bucket transform and byte-wise comparisons see.
    pub fn as_bytes(&self) -> std::borrow::Cow<'_, [u8]>;
    pub fn within_stats(&self, min: &[u8], max: &[u8]) -> bool;
}

// bucket
pub fn bucket(key: &Key, buckets: u32) -> u32;

// config
pub struct Config { pub server: ServerConfig, pub storage: StorageConfig, pub tables: Vec<TableConfig> }
impl Config {
    /// TOML or JSON, decided by content (a leading `{` is JSON).
    pub fn parse(text: &str) -> Result<Self>;
    pub fn from_path(path: &std::path::Path) -> Result<Self>;
}
/// `metrics_listen` is required (default 127.0.0.1:9433) and serves `/metrics`
/// and `/ready`; RFD 012 makes both part of the surface.
pub struct ServerConfig { pub listen: SocketAddr, pub metrics_listen: SocketAddr, pub poll_interval: Duration /* "10s" in the file */, pub page_cache_bytes: usize, pub log: LogConfig }
/// Logs are JSON only (RFD 012 rejected a text format); `fields` renames the
/// standard fields and `static_fields` are added to every event.
pub struct LogConfig { pub fields: BTreeMap<String, String>, pub static_fields: BTreeMap<String, String> }
pub struct HedgeConfig { pub enabled: bool, pub percentile: f64, pub window: usize, pub max_rate: f64 }
pub struct S3Config { pub endpoint: Option<String>, pub region: Option<String>, pub access_key_id: Option<String>, pub secret_access_key: Option<String>, pub allow_http: bool, pub path_style: bool }
pub struct RewriteRule { pub from: String, pub to: String }
pub enum ConfigError { Read(std::io::Error), Toml(toml::de::Error), Json(serde_json::Error), Invalid(ConfigViolation) }
pub enum ConfigViolation { EmptyTableName, DuplicateTableName(String), IncompleteStaticKeys, BudgetWithoutResidency, DiskResidencyWithoutLocalDir }
pub struct TableConfig { pub name: String, pub source: SourceConfig, pub r#ref: String, pub key_column: String, pub value_column: String, pub strict: bool /* default true */, pub residency: Residency /* default Full */, pub resident_budget_bytes: Option<u64> }
/// Where a table keeps its key chunks (RFD 017), ranked by lookup speed. Never chosen automatically.
pub enum Residency { Full, Disk, Bloom }
pub struct LocalConfig { pub dir: PathBuf, pub index_budget_bytes: Option<u64>, pub page_cache_disk_bytes: u64 }
pub struct StorageConfig { pub s3: Option<S3Config>, pub rewrite: Vec<RewriteRule>, pub hedge: HedgeConfig, pub local: Option<LocalConfig> }
pub enum SourceConfig {
    Static { location: String },
    Rest { uri: String, warehouse: Option<String>, namespace: String, table: String },
}

// storage
/// One counter for a lookup's whole cost, whatever residency the table runs
/// (RFD 003): object-store GETs and local reads are counted in the same place.
pub struct IoStats { /* gets, bytes, hedges, local_reads, local_bytes: AtomicU64 */ }
impl IoStats { pub fn gets(&self) -> u64; pub fn bytes(&self) -> u64; pub fn hedges(&self) -> u64; pub fn local_reads(&self) -> u64; pub fn local_bytes(&self) -> u64; }
pub enum CacheError { Fetch(Arc<Error>), Backend(String) }
/// A sliding window of recent lookup-GET latencies, owned by one `Table`;
/// the hedge delay is its configured percentile, capped by the hedge rate.
pub struct HedgeWindow;
impl HedgeWindow { pub fn new(config: &HedgeConfig) -> Self; pub fn delay(&self) -> Option<Duration>; pub fn record(&self, elapsed: Duration, hedged: bool); }

// local
pub struct LocalDir;
impl LocalDir {
    pub fn open(config: &LocalConfig, stats: Arc<IoStats>) -> Result<Self>;
    /// The chunk file for a data file, identified by path, size, and footer digest; `None` if absent or stale.
    pub fn chunks_for(&self, identity: &DataFileIdentity) -> Result<Option<DiskChunks>>;
    /// Writes verified key chunks as they stream; returns the handle the index keeps.
    pub fn write_chunks(&self, identity: &DataFileIdentity, chunks: impl Iterator<Item = (Range<u64>, Bytes)>) -> Result<DiskChunks>;
    /// Deletes chunk files no current generation references.
    pub fn retain(&self, live: &HashSet<DataFileIdentity>) -> Result<u64>;
}
/// A key page read from local disk on demand; implements the same `ChunkReader` surface as `Segments`.
pub struct DiskChunks;
impl DiskChunks { pub async fn page(&self, range: Range<u64>) -> Result<Bytes>; pub fn bytes_on_disk(&self) -> u64; }
/// What a chunk file is named by and validated against; written into its
/// header inside the RFD 011 envelope as a `ChunkFileHeader` message.
pub struct DataFileIdentity { pub path: String, pub size: u64, pub footer_digest: [u8; 32] }
/// The page cache's disk level, handed to `PageCache` by `local` (RFD 017).
pub struct DiskTier;
impl LocalDir { pub fn page_tier(&self) -> Option<DiskTier>; }
pub struct Located { pub store: Arc<dyn ObjectStore>, pub path: object_store::path::Path }
pub struct Storage;
impl Storage {
    pub fn new(config: StorageConfig) -> Self;
    pub async fn with_sdk_defaults(config: StorageConfig) -> Self;
    pub fn register(&self, prefix: impl Into<String>, store: Arc<dyn ObjectStore>);
    pub fn stats(&self) -> Arc<IoStats>;
    pub fn locate(&self, location: &str) -> Result<Located>;
    pub async fn read_all(&self, location: &str) -> Result<Bytes>;
    /// Load path: never hedged.
    pub async fn read_range(&self, l: &Located, range: Range<u64>) -> Result<Bytes>;
    /// Lookup path: hedged per `HedgeConfig` against the table's own latency
    /// window, since file size sets first-byte latency per table (RFD 013).
    pub async fn read_range_hedged(&self, l: &Located, range: Range<u64>, window: &HedgeWindow) -> Result<Bytes>;
    pub async fn read_ranges(&self, l: &Located, ranges: &[Range<u64>]) -> Result<Vec<Bytes>>;
}

// catalog
pub struct Resolved { pub metadata: Arc<TableMetadata>, pub location: String }
impl Resolved { pub fn snapshot_for_ref(&self, table: &str, ref_name: &str) -> Result<Arc<Snapshot>>; }
pub enum TableSource { Static { location: String }, Catalog { catalog: Arc<dyn Catalog>, ident: TableIdent } }
impl TableSource {
    /// Builds REST clients too; the only place a catalog client is constructed.
    pub async fn from_config(cfg: &SourceConfig) -> Result<Self>;
    pub async fn resolve(&self, storage: &Storage) -> Result<Resolved>;
    /// One small read when nothing moved.
    pub async fn resolve_if_changed(&self, storage: &Storage, known: &str) -> Result<Option<Resolved>>;
}

// manifest
pub struct Bucketing { pub count: u32, pub source_field_id: i32 }
pub struct TableLayout { pub bucketing: Option<Bucketing>, pub key_column: String, pub key_field_id: i32, pub key_type: KeyType, pub value_column: String, pub value_field_id: i32 }
impl TableLayout {
    /// From the snapshot's own schema, never the table's current one.
    pub fn derive(table: &str, md: &TableMetadata, snapshot: &Snapshot, key_column: &str, value_column: &str) -> Result<Self>;
    pub fn bucket_of(&self, key: &Key) -> u32;
}
pub struct DataFileEntry { pub path: String, pub bucket: u32, pub records: u64, pub size: u64, pub key_min: Option<Key>, pub key_max: Option<Key> }
/// Reads every manifest of the snapshot, delete manifests included; a delete
/// file is `Error::Refused { reason: RefusalReason::DeleteFiles }`. Bucket
/// position comes from each manifest's spec.
pub async fn list_data_files(table: &str, md: &TableMetadata, snapshot: &Snapshot, layout: &TableLayout, storage: &Storage) -> Result<Vec<DataFileEntry>>;

// parquet_io
pub struct FileHandle { pub location: String }
impl FileHandle {
    pub fn open(storage: Arc<Storage>, location: &str, size: u64) -> Result<Self>;
    pub async fn load_metadata(self: &Arc<Self>) -> Result<Arc<ParquetMetaData>>;
    pub async fn read_page(self: &Arc<Self>, range: Range<u64>, cache: Option<&PageCache>) -> Result<Bytes>;
}
pub struct Segments;
impl Segments { pub fn new(parts: Vec<(Range<u64>, Bytes)>, file_size: u64) -> Self; pub fn resident_len(&self) -> usize; }
impl parquet::file::reader::ChunkReader for Segments { /* errors on non-resident bytes */ }

// index (MAX_KEY_BYTES comes from `contract`)
pub struct RowRef { pub row_group: usize, pub row: i64 }
/// What makes a data file unservable beyond the contract refusals; closed set.
pub enum FileFault { MissingColumn(String), UnexpectedPhysicalType { expected: &'static str, got: String }, MissingPageIndex, PageOutOfRange { row: i64, row_group: usize }, CorruptBloomFilter { row_group: usize } }
/// Every tier's resident bytes for one file, from the footer alone (RFD 017).
pub struct ResidentBytes { pub full: u64, pub disk_memory: u64, pub disk_bytes: u64, pub bloom: Option<u64> /* None when a row group lacks a filter */, pub bloom_bits_per_key: Option<f64> }
pub struct FileIndex;
impl FileIndex {
    /// Refuses (typed) an unsorted or null key column, a key physical type
    /// that does not match `key_type` (Binary accepts BYTE_ARRAY and
    /// FIXED_LEN_BYTE_ARRAY), a value column whose physical type is neither
    /// BYTE_ARRAY nor FIXED_LEN_BYTE_ARRAY, keys over the cap, in the
    /// Bloom tier a row group without a key bloom filter, and in the Disk
    /// tier the absence of a local directory. Every tier streams the key
    /// chunks once for verification; Full keeps them in memory, Disk writes
    /// them through `LocalDir` (or reuses a chunk file with matching
    /// identity and skips the chunk fetch), Bloom keeps filters. The value
    /// column's column index is dropped after the offset index is taken; it
    /// is never part of the resident set.
    pub async fn build(handle: Arc<FileHandle>, key_column: &str, value_column: &str, key_type: KeyType, residency: Residency, local: Option<&LocalDir>) -> Result<Self>;
    /// Every tier's resident bytes for this file, from the footer alone.
    pub fn predicted_resident_bytes(metadata: &ParquetMetaData, key_leaf: usize) -> ResidentBytes;
    pub fn key_range(&self) -> (&Key, &Key);
    pub fn may_contain(&self, key: &Key) -> bool;
    /// Exact position. Full: decodes one resident key page, no I/O. Disk:
    /// one local read of that page. Bloom: filter check in memory, then on
    /// a positive one key-page GET through the cache. Row-group choice uses
    /// the exact fences, never statistics.
    pub async fn find(&self, key: &Key, cache: Option<&PageCache>) -> Result<Option<RowRef>>;
    /// `None` is a null value, distinct from an absent key.
    pub async fn read_value(&self, row: RowRef, cache: Option<&PageCache>) -> Result<Option<Bytes>>;
    pub fn resident_bytes(&self) -> usize;
    pub fn max_key_bytes(&self) -> usize;
    pub fn metadata(&self) -> &Arc<ParquetMetaData>;
    /// The page a key would decode and the value page it would fetch, for tests and the doctor.
    pub fn page_of(&self, key: &Key) -> Option<PageRef>;
}
pub struct PageRef { pub row_group: usize, pub key_page: usize, pub value_page_offset: u64 }

// cache
pub struct PageCache;
impl PageCache {
    /// `disk` is the local level of RFD 017: a page evicted from memory is
    /// served from it with a local read and no GET, and it survives a restart.
    pub fn new(capacity_bytes: usize, disk: Option<DiskTier>) -> Self;
    pub async fn get_or_fetch<F, Fut>(&self, key: (Arc<str>, u64), fetch: F) -> Result<Bytes>
    where F: FnOnce() -> Fut, Fut: Future<Output = Result<Bytes>> + Send + 'static;
    /// Hits and fetches after coalescing.
    pub fn hits(&self) -> u64; pub fn fetches(&self) -> u64; pub fn usage(&self) -> usize;
}

// generation
pub struct LoadedFile { pub entry: DataFileEntry, pub index: FileIndex }
pub struct Generation {
    pub table: String, pub snapshot_id: SnapshotId, pub sequence_number: i64, pub metadata_location: String,
    pub layout: TableLayout, pub residency: Residency, pub files: Vec<Arc<LoadedFile>> /* sorted by (bucket, first key, path) */,
    pub records: u64, pub index_bytes: usize, pub loaded_at: Instant,
}
impl Generation {
    /// `config` carries the residency and the resident-bytes budget; the
    /// budget is checked at the footer stage, before any key chunk is fetched.
    pub async fn load(table: &str, config: &TableConfig, resolved: &Resolved, snapshot: &Snapshot, storage: &Arc<Storage>, local: Option<&LocalDir>, previous: Option<&Generation>) -> Result<Self>;
    pub fn candidates(&self, key: &Key) -> impl Iterator<Item = &Arc<LoadedFile>>;
    pub fn overlapping_buckets(&self) -> &BTreeMap<u32, usize>;
    pub fn overlap_groups_verified(&self) -> usize;
    pub async fn get(&self, key: &Key, cache: &PageCache) -> Result<Value>;
    /// Which (file, value page) each key would fetch; the oracle for cost tests.
    pub fn plan(&self, keys: &[Key]) -> Vec<Option<(Arc<LoadedFile>, PageRef)>>;
}
/// The three outcomes of a lookup; the wire carries the same three.
pub enum Value { Present(Bytes), Null, Absent }
pub struct Lookup { pub value: Value, pub snapshot_id: SnapshotId }
/// Fails as a whole if any key's read fails; partial results are a later, separate call.
pub struct MultiLookup { pub values: Vec<Value>, pub snapshot_id: SnapshotId }

// table (after `check`, because the strict gate runs `check::diagnose` on a
// built generation before it is swapped in)
pub enum TableState { Loading, Serving { snapshot_id: SnapshotId, since: Instant }, Degraded { serving: Option<SnapshotId>, error: Arc<Error>, since: Instant } }
pub struct Table;
impl Table {
    /// Constructs in `Loading`; nothing is read until `load`.
    pub fn new(config: TableConfig, source: TableSource, storage: Arc<Storage>, cache: PageCache, local: Option<Arc<LocalDir>>) -> Self;
    /// First load. `Err` leaves the table `Degraded` with no generation.
    pub async fn load(&self) -> Result<()>;
    /// Serialized per table. `Ok(true)` when a new generation was swapped in.
    pub async fn refresh(&self) -> Result<bool>;
    pub fn name(&self) -> &str;
    pub fn config(&self) -> &TableConfig;
    pub fn state(&self) -> TableState;
    pub fn current(&self) -> Option<Arc<Generation>>;
    pub async fn get(&self, key: &Key) -> Result<Lookup>;
    pub async fn multi_get(&self, keys: &[Key]) -> Result<MultiLookup>;
}

// check
pub enum Tier { Advice, Cost }
pub enum Serviceability { Efficient, Inefficient }
pub struct Penalty { pub extra_gets_per_hit: Option<u32>, pub keys_decoded_per_lookup: Option<i64>, pub files_probed_per_lookup: Option<usize> }
pub enum FindingCode { DictionaryEncodedValue, LargeKeyPages, NoBucketPartition, OverlappingFiles, BoundsMissing, ColumnIndexTruncated, LargeValuePages, LongKeys, LargeFile, ManyFilesPerBucket, NoBloomFilter, BloomOversized, BloomUndersized }
/// Writer properties the doctor names; Display gives the property string.
pub enum WriterProperty { EnableDictionary, ColumnIndexTruncateLength, PageRowLimit, PageSizeBytes, RowGroupCheckRecordCount, TargetFileSizeBytes, MetricsColumn, BloomFilterEnabled, BloomFilterMaxBytes, SkewSplitting }
pub struct Finding { pub tier: Tier, pub code: FindingCode, pub message: String, pub file: Option<String>, pub penalty: Option<Penalty>, pub property: Option<WriterProperty> }
pub struct FileReport {
    pub path: String, pub bucket: u32, pub key_type: KeyType, pub rows: i64, pub row_groups: usize,
    pub key_pages: usize, pub value_pages: usize, pub max_key_page_rows: i64, pub max_key_bytes: usize,
    pub avg_value_page_bytes: u64, pub pages_aligned: bool, pub dictionary_columns: Vec<String>,
    pub bloom_bytes: Option<u64>, pub bloom_bits_per_key: Option<f64>, pub key_chunk_bytes: u64,
    pub distinct_page_mins_ratio: f64, pub bounds_present: bool, pub resident_bytes: usize,
}
pub struct TableSummary { pub table: String, pub snapshot_id: SnapshotId, pub serviceability: Serviceability, pub residency: Residency, pub file_count: usize, pub buckets: Option<u32>, pub buckets_with_files: usize, pub max_files_per_bucket: usize, pub records: u64, pub index_bytes: usize, pub predicted: ResidentBytes, pub costs: usize, pub advice: usize }
pub struct TableReport { pub summary: TableSummary, pub findings: Vec<Finding>, pub files: Vec<FileReport> }
pub fn diagnose(g: &Generation) -> TableReport;
/// The thresholds, in one place, each with the analysis section that set it.
pub mod thresholds { pub const KEY_PAGE_ROWS_COST: i64 = 2 * contract::PAGE_ROWS; pub const VALUE_PAGE_BYTES_ADVICE: u64 = 128 * 1024; pub const FILE_BYTES_ADVICE: u64 = 2 * contract::TARGET_FILE_BYTES_MAX; pub const FILES_PER_BUCKET_FACTOR: u64 = 2; pub const BLOOM_BYTES_PER_KEY_ADVICE: f64 = 4.0; pub const BLOOM_BITS_PER_KEY_MIN: f64 = 8.0; pub const DISTINCT_MINS_RATIO_ADVICE: f64 = 0.5; }

// contract (the layout's numbers; every other crate imports these, none redeclares them)
pub const PAGE_ROWS: i64 = 1_000;
pub const VALUE_PAGE_BYTES: u64 = 64 * 1024;
pub const TARGET_FILE_BYTES_MIN: u64 = 32 * 1024 * 1024;
pub const TARGET_FILE_BYTES_MAX: u64 = 64 * 1024 * 1024;
pub const MAX_KEY_BYTES: usize = 4_096;
pub const KEY_BYTES_ADVICE: usize = 128;

// open
pub struct Opened { pub storage: Arc<Storage>, pub cache: PageCache, pub local: Option<Arc<LocalDir>>, pub tables: Vec<Arc<Table>> }
/// Builds storage from the SDK chain, one cache, and every configured table
/// in `Loading`; `only` narrows by name. Loading is the caller's step.
pub async fn open(config: &Config, only: Option<&str>) -> Result<Opened>;
```

Finding codes and tiers (RFD 007, RFD 016). The code column is the Display
form of the `FindingCode` variant; code never compares these strings:

| code | tier | trigger | penalty | property named in advice |
|---|---|---|---|---|
| `dictionary_encoded_value` | Cost | value column dictionary-encoded | one extra GET per hit | `parquet.enable.dictionary` |
| `large_key_pages` | Cost | key page rows above `KEY_PAGE_ROWS_COST` | rows decoded per lookup | `write.parquet.page-row-limit` |
| `no_bucket_partition` | Cost | no bucket transform on the key and more than one file | files probed per lookup | partition spec |
| `overlapping_files` | Cost | intersecting exact ranges within a bucket | files probed per lookup | `spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled` |
| `bounds_missing` | Advice | manifest lacks key bounds, or its bounds are shorter than the file's exact first or last key | | `write.metadata.metrics.column.<key>` |
| `column_index_truncated` | Advice | distinct page minimums ratio below threshold | | `parquet.columnindex.truncate.length` |
| `large_value_pages` | Advice | mean uncompressed value page above threshold | | `write.parquet.page-size-bytes` and the row-group-check properties |
| `long_keys` | Advice | key bytes above `KEY_BYTES_ADVICE` | | |
| `large_file` | Advice | data file above `FILE_BYTES_ADVICE` | | `write.target-file-size-bytes` |
| `many_files_per_bucket` | Advice | files in a bucket above twice what the bucket's bytes divided by `TARGET_FILE_BYTES_MAX` explain (a rolled sorted task is not many files) | | compaction |
| `no_bloom_filter` | Advice | no bloom filter on the key | | `write.parquet.bloom-filter-enabled.column.<key>` |
| `bloom_oversized` | Advice | bloom bytes per key above threshold | | `write.parquet.bloom-filter-max-bytes` (Iceberg 1.11+) |
| `bloom_undersized` | Advice | bloom bits per key below `BLOOM_BITS_PER_KEY_MIN` (the Bloom tier's miss cost rises) | | `write.parquet.bloom-filter-ndv.column.<key>` (Iceberg 1.11+) |

Refusals (never findings; `Error::Refused` at load) are the `RefusalReason`
variants: unsorted, null keys, duplicate keys, key type mismatch, value type
unsupported (the value column must be Iceberg binary, fixed, or string,
served as opaque bytes), delete files, key too long, and the three residency
refusals of RFD 017: over budget, no local directory, no bloom filter. The
prototype's per-table `key_chunk_cost` line becomes a `FileReport` field,
not a finding.

## 4. `icetray-proto`

```proto
service Icetray { rpc Get(GetRequest) returns (GetResponse); rpc MultiGet(MultiGetRequest) returns (MultiGetResponse); rpc TableInfo(TableInfoRequest) returns (TableInfoResponse); }
// `bin` carries binary, fixed, and uuid keys; the table's key type says how
// its bytes are read (RFD 008).
message Key { oneof key { int64 long = 1; string str = 2; bytes bin = 3; } }
message GetRequest { string table = 1; Key key = 2; }
enum Presence { PRESENCE_UNSPECIFIED = 0; PRESENT = 1; NULL = 2; ABSENT = 3; }
message GetResponse { Presence presence = 1; bytes value = 2; int64 snapshot_id = 3; }
message MultiGetRequest { string table = 1; repeated Key keys = 2; }
message Hit { Presence presence = 1; bytes value = 2; }
message MultiGetResponse { repeated Hit results = 1; int64 snapshot_id = 2; }
message TableInfoRequest { string table = 1; }
enum KeyType { KEY_TYPE_UNSPECIFIED = 0; KEY_TYPE_LONG = 1; KEY_TYPE_STRING = 2; KEY_TYPE_BINARY = 3; KEY_TYPE_UUID = 4; }
message TableInfoResponse { string table = 1; int64 snapshot_id = 2; uint32 buckets = 3; uint64 data_files = 4; uint64 records = 5; KeyType key_type = 6; uint64 index_bytes = 7; }
```

A second file carries the one message icetray persists for itself, the
header of a chunk file (RFD 011's envelope: a magic byte, a version, then
this message length-prefixed):

```proto
message ChunkFileHeader { string data_file_path = 1; uint64 data_file_size = 2; bytes footer_digest = 3; }
```

Comments describe the contract, not internals. Generated code is included
in one inline module carrying the scoped allows (RFD 015). The build script
uses a vendored protobuf compiler, so building from source needs none on the
build host (RFD 012). The crate gets round-trip tests per message and a wire
test for the bytes key variant.
Health is `grpc.health.v1` from the tonic health crate, not our proto.

## 5. The binary and the testkit

**Local directory.** Optional (`storage.local`). Chunk files under
`index/`, named by a digest of the data file's path, each carrying a
`ChunkFileHeader` inside the RFD 011 envelope; the page cache's disk level
under `cache/`, handed to `PageCache` as a `DiskTier`.
Disk residency reads pages with positioned reads on a blocking pool, never
mmap, so every local read is counted and no page fault stalls a worker.
Startup reuses chunk files whose identity matches; a generation swap
deletes chunk files nothing references; a disk budget refuses loads that
would exceed it, at the footer stage.

**`icetray serve`.** The binary uses jemalloc with background purging so
that memory freed after a load (shed key chunks, dropped generations)
returns to the operating system; memory metrics report the index's live
bytes beside process RSS. The daemon subcommand binds the gRPC listener and the
metrics listener first (the metrics listener serves `/metrics` and
`/ready`), registers the health service (all tables `NOT_SERVING`), then calls
`core::open` and loads every table concurrently. A table's first-load failure
is fatal to the process (RFD 005: refuse to start), after the reason is
logged and the health status reflects it for the shutdown grace period. Once
every table is `Serving`, health flips to `SERVING` and the readiness
endpoint answers 200. Refresh runs as one task per table with the configured
interval; a refresh failure keeps the last generation, sets `Degraded`, and
counts `icetray_refresh_errors_total{table}`. Configuration path from
`--config` or `ICETRAY_CONFIG`; TOML or JSON. Logs through `tracing` with the
JSON formatter, field names remapped through `LogConfig.fields` and
`static_fields` appended. Prometheus histograms use the prototype's buckets
(100 µs to 2 s). SIGTERM drains in-flight requests and exits 0.

**One-shot subcommands.** `doctor [table] [--files]` (exit 0 efficient, 2
inefficient, 1 refused or error), `get [table] <key>...` (table optional when
exactly one is open; binary keys as hex), `info [table]`. JSON lines with
`kind` and `table` first; `doctor` and `info` lines serialize `TableSummary`
so a new measurement in core reaches the CLI without a CLI change. Flags
that only apply to `--location` are rejected under `--config`. Returns
`ExitCode`.

**Testkit.** Carried: the key/value derivation (`3 * i`, values a function
of the key), `FixtureSpec`, the Iceberg writer plumbing, the in-process S3
server, the simulation transport (verbatim), the acceptance tests. Changed:
the fixture writer imports the layout constants from `contract` instead of
declaring its own;
`verify` uses `Generation::plan` as the cost oracle instead of re-deriving
probe order; a fresh `PageCache` per measured batch; the fixture keeps one
source of truth for written ids; `key_array` takes the `KeyType`; golden
tables are located by `ICETRAY_GOLDEN_WAREHOUSE` and a missing warehouse
marks the test skipped in output; new fixtures, one per refusal
and per finding that no contract fixture triggers: a delete file, an
unsorted file, a null key, a mismatched key physical type, an unsupported
value column type, a multi-row-group file without column statistics, an
evolved partition spec, a permuted-key overlapping table, a
dictionary-encoded value column, a uuid-keyed table, and a table whose
files carry no key bloom filter; everything test-only under
`[dev-dependencies]`.

## 6. Invariants and the tier that proves each

Tiers (RFD 014): U unit; P in-process (in-memory store or in-process S3, GET
counter as oracle); S deterministic simulation (round trips as oracle); C
container-gated (visible skip); G golden tables written by Spark (visible
skip).

| invariant | tier |
|---|---|
| Bucket routing equals the Iceberg library's transform for long, string, binary, uuid, and fixed keys | U (oracle test against the library, all five types) |
| Key ordering equals Parquet's for each type; keys of different types never compare | U |
| A cold hit costs exactly one GET; a warm hit none; a miss none, including misses inside a file's range | P, S |
| Load costs two GETs per file plus the metadata reads; a load with `previous` reuses every unchanged file by pointer | P |
| An unchanged poll costs one read (hint file or catalog record) | P, C |
| A refresh verifies only overlap groups that gained a file; a disjoint append verifies zero groups | P (counting) |
| A duplicate key across files is refused and the previous generation keeps serving | P |
| A delete file in the followed snapshot is refused with reason `delete_files` | P (fixture with a delete file) |
| A file without column statistics and several row groups never yields a false miss | U (writer with statistics disabled) |
| Layout follows the snapshot's schema and each manifest's spec; an evolved bucket count routes correctly | P (or U on hand-built metadata if the writer cannot evolve a spec) |
| Overlapping files without duplicates serve correctly and are priced as files probed | P |
| Unpartitioned tables serve correctly and are priced | P |
| A dictionary-encoded value column costs one extra GET; a dictionary-encoded key column costs none | U |
| Keys over `MAX_KEY_BYTES` are refused; keys over the advice length serve with advice | P |
| `Generation::plan` predicts exactly the GETs a `multi_get` issues | P |
| Concurrent lookups for one page coalesce to one fetch | P |
| `Segments` errors on any non-resident byte instead of fetching | U |
| A cold hit is exactly one round trip; load is a fixed number of dependent round trips; results identical across seeds | S |
| Hedging trims an injected bimodal tail; under uniform latency the hedge rate stays under the ceiling | S |
| Load reads never hedge | S (counter), U |
| The credential provider refreshes once under concurrent expiry | U (counting provider) |
| Every finding code is triggered by exactly one fixture designed for it, and the contract fixture triggers none | P, G |
| Every `RefusalReason` variant is triggered by exactly one fixture designed for it | P |
| `strict` refuses a cost-tier generation and only that | P |
| gRPC maps missing key and wrong key type to `InvalidArgument`, unknown table to `NotFound`, storage failure to `Unavailable`; `MultiGet` answers from one snapshot and fails as a whole when one read fails | P (in-process S3 behind the server) |
| A null value is served as present-with-null, distinct from absent, at the same GET count | P (nullable fixture) |
| Bloom tier: a cold hit is exactly two GETs, a warm hit none, a negative miss none; refusals identical to Full plus its own; chunks unreachable after load; a row group without a filter is refused | P (carried from the prototype's residency tests) |
| Disk tier: a cold hit is one GET and one local read, a miss one local read and no GET; refusals identical to Full plus its own; a restart with matching chunk files fetches footers only; a dropped file's chunk file is deleted; no local directory is a typed refusal | P (temporary directory, counting GETs and local reads) |
| A uuid key column routes and orders as the reference implementation does, and a uuid-keyed lookup costs the same as a long-keyed one | U (oracle), P |
| The page cache's disk tier serves a page evicted from memory with a local read and no GET, and survives a restart | P (temporary directory) |
| Row-group choice never consults column statistics: long string keys sharing a prefix beyond 64 bytes across several row groups all resolve | U (carried from the prototype) |
| Over-budget Full load is refused at the footer stage with no chunk fetched | P (counting) |
| The value column's column index is not resident; index bytes for a byte-bounded large-value table are within a small factor of its key bytes | P |
| Health is `NOT_SERVING` until every table serves; the listener accepts before load completes | P (server test) |
| A skipped gated test is reported as skipped, never as passed | C, G (harness) |
| CLI JSON lines: `kind` and `table` first, `gets` per lookup exact, exit codes as specified | P (binary test) |
| Config parses identically from TOML and JSON | U |

## 7. Metrics and log names (fixed)

Counters: `icetray_requests_total{call}`, `icetray_object_gets_total{kind}`,
`icetray_object_bytes_total{kind}`, `icetray_local_reads_total`,
`icetray_local_bytes_total`, `icetray_hedges_total`,
`icetray_page_cache_hits_total`, `icetray_page_cache_fetches_total`,
`icetray_lookups_total{table,result}`,
`icetray_refresh_errors_total{table,reason}`,
`icetray_generation_swaps_total{table}`.
Histograms: `icetray_lookup_seconds{table,result}`,
`icetray_object_get_seconds{kind}`. Gauges: `icetray_index_bytes{table}`
(the index's live bytes, which is what a residency tier sheds),
`icetray_process_rss_bytes`, `icetray_page_cache_bytes`,
`icetray_findings{table,code}`, `icetray_generation_age_seconds{table}`
(the served snapshot's timestamp to now, through the time-conversion
helpers), `icetray_generation_info{table,snapshot_id}` (1 for the serving
snapshot, 0 for the previous; series of older snapshots are removed on
swap), `icetray_table_state{table,state}`.

Every label value is the `Display` of an enum, never a literal string at the
call site: `kind` from a read-kind enum, `result` from `LookupResult`, `call`
from the RPC enum, `reason` from `RefusalReason`, `code` from `FindingCode`,
`state` from `TableState`.

## 8. Open questions carried from the RFDs

Kept open here so the build does not settle them silently, each with what
the build actually does:

- The `strict` default: built as on, one line per table to turn off.
- Decoded key arrays as an opt-in: not built.
- The fences-only remote tier and bucket sharding: not built; the bucket
  filter on the manifest walk is the seam for the second.
- The hedging percentile, window, and ceiling: built as the 95th percentile
  over the last 256 lookup GETs with a 5 percent ceiling, all configurable,
  and the window is per table.
- Per-table lifecycle states: built as start-or-fail at boot, plus a
  `Degraded` state after a failed refresh; readiness inherits start-or-fail.
- Golden tables: built as a checked-in recipe that regenerates them, not as
  fixed bytes.
- A metadata-only doctor mode: not built.
