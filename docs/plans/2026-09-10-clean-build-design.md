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
| `icetray` | binary + thin library | core, proto | one binary: `serve` (the daemon: service, table lifecycle, poll loop, twelve-factor surface) and the one-shot `doctor`, `get`, `info` as JSON lines; the library's modules are `cli`, `error`, `key_text`, `output`, `oneshot`, `logging`, `telemetry`, `service`, `listen`, and `serve` |
| `icetray-testkit` | library | core (dev: the `icetray` crate depends on testkit) | in-process S3, fixture writer, simulation, shared assertions, golden-table locator |

`icetray-core` never depends on the testkit (its unit tests write Parquet
with `arrow` as a dev-dependency). Every subcommand of the binary opens
tables through one core function and shares one flag vocabulary. The
binary's `main` parses the command line, builds the runtime, and maps the
library's typed outcomes to exit codes; everything else is in the library,
so the integration tests drive `Serving` in-process over the in-process S3
server (`tests/serve.rs`) and the built binary over local fixtures
(`tests/cli.rs`). Pinned dependency versions live once, in
`[workspace.dependencies]`.

## 2. `icetray-core` module map and owners

Modules in dependency order. "Owns" is the concept no other module may
redefine.

| module | owns | RFD |
|---|---|---|
| `error` | the one `Error` enum and `Result` | |
| `approx` | integer-to-float conversion (`ratio`, `gauge`, `gauge_len`); the only `cast_precision_loss` allow | 015 |
| `time` | typed time conversions: the one place a Duration or timestamp becomes a number or a string (`seconds`, `micros`, `age_seconds`, `log_timestamp`), the wall clock is read (`now`), and an epoch millisecond becomes a `jiff::Timestamp` (`timestamp_from_millis`, in-crate; out of jiff's range is `TableFault::SnapshotTimestampOutOfRange`); CONTRIBUTING Constants | |
| `metric` | the metric and label names of section 7 as constants, and the read-kind label enum; the one home for those names | 012 |
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
    /// The table's metadata makes it unusable before any data file is read;
    /// the fault set is closed (`TableFault`, under `catalog`). `table` is the
    /// configured name, or, for a fault raised while a `TableSource` resolves
    /// and no table has been named yet, the source itself: the static
    /// location or the catalog identifier.
    Table { table: String, fault: TableFault },
    /// A data file is malformed for serving; the fault set is closed.
    File { path: String, fault: FileFault },
    /// The table has no serving generation yet.
    NotReady { table: String },
    /// The local directory failed (RFD 017); the fault set is closed.
    Local { path: PathBuf, fault: LocalFault },
}
pub type Result<T> = std::result::Result<T, Error>;
impl Error {
    /// The variant, with the reason for a refusal: the `reason` label of `refresh_errors_total`.
    pub fn kind(&self) -> ErrorKind;
}
/// One variant per `Error` variant, `Refused` carrying its reason. Display gives the `reason`
/// label of `refresh_errors_total`: the refusal's own name for `Refused`, `inefficient` for
/// the strict gate, and the variant's snake_case name for the rest (`iceberg`, `parquet`,
/// `object_store`, `object_store_path`, `json`, `config`, `cache`, `key_type`, `table`,
/// `file`, `not_ready`, `local`). Also what `TableState::Degraded` carries as its reason:
/// `Error` is not `Clone`, so the failure goes to the caller and its kind to the state.
/// Re-exported from `lib.rs`.
pub enum ErrorKind { Iceberg, Parquet, ObjectStore, ObjectStorePath, Json, Config, Cache, Refused(RefusalReason), Inefficient, KeyType, Table, File, NotReady, Local }

/// Named refusals (RFD 007). Display gives the snake_case name used in logs;
/// the fields name the file, row group, type, key, or bytes a reader has to
/// act on, and `Error::Refused`'s message carries them. Clone, not Copy.
/// `DuplicateKeys` names the file the key appears in again: the same file
/// for a duplicate within one file, the later file of a bucket otherwise.
/// `ValueTypeUnsupported` is raised from the snapshot's schema by
/// `TableLayout::derive`, so it names no file and carries the Iceberg type's
/// `Display` (the type does not implement `Hash`); a data file whose physical
/// type contradicts an admitted schema type is `FileFault::ValuePhysicalType`.
pub enum RefusalReason { Unsorted { path: String, row_group: usize }, NullKeys { path: String, row_group: usize }, DuplicateKeys { path: String, key: Key }, KeyTypeMismatch { path: String, expected: KeyType, got: parquet::basic::Type }, KeyTypeUnsupported, ValueTypeUnsupported { got: String }, DeleteFiles, KeyTooLong { path: String, bytes: usize }, OverBudget { needed: u64, budget: u64 }, NoBloomFilter { path: String, row_group: usize }, NoLocalDir }
/// A snapshot id is not an integer; it is never compared with one.
pub struct SnapshotId(pub i64);
/// Metric label values for lookups.
pub enum LookupResult { ColdHit, WarmHit, Miss }

// key
/// The width of an Iceberg uuid and of the FIXED_LEN_BYTE_ARRAY Parquet stores it in.
pub const UUID_BYTES: usize = 16;
pub enum KeyType { Long, String, Binary, Uuid }
impl KeyType {
    /// long -> Long; string -> String; binary and fixed -> Binary; uuid -> Uuid.
    /// Every other type is `None`; the caller that owns the table name turns it
    /// into `Error::Refused { reason: RefusalReason::KeyTypeUnsupported }` (RFD 008).
    pub fn from_iceberg(ty: &iceberg::spec::Type) -> Option<Self>;
}
/// `Ord` is the served order per type: numeric, UTF-8 bytes, bytewise, and for
/// `Uuid` bytewise unsigned over its sixteen bytes, which is the order
/// iceberg-rust's `Datum` and Parquet's UUID column statistics use,
/// conformance-tested against both rather than assumed. It is not the order
/// Iceberg Java's `Comparators` gives the type, `java.util.UUID`'s signed
/// halves; a file sorted that way reads as unsorted (RFD 008).
pub enum Key { Long(i64), Str(String), Bytes(Vec<u8>), Uuid([u8; UUID_BYTES]) }
impl Key {
    pub fn key_type(&self) -> KeyType;
    pub fn from_datum(datum: &iceberg::spec::Datum) -> Option<Self>;
    /// The bytes the bucket transform and byte-wise comparisons see.
    pub fn as_bytes(&self) -> std::borrow::Cow<'_, [u8]>;
    /// A statistic that cannot be read never excludes.
    pub fn within_stats(&self, min: &[u8], max: &[u8]) -> bool;
}
impl std::fmt::Display for Key { /* the integer, a Debug-quoted string, 0x-prefixed hex, a hyphenated uuid */ }

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
/// The two location variants are raised by `Storage::locate`, after rewriting, not by `Config::parse`.
/// `UnknownResidency` is raised by `Residency`'s `FromStr`, the command line's parser for the setting.
pub enum ConfigViolation { EmptyTableName, DuplicateTableName(String), IncompleteStaticKeys, BudgetWithoutResidency, DiskResidencyWithoutLocalDir, PageCacheDiskTooSmall { bytes: u64, minimum: u64 }, HedgePercentileOutOfRange { percentile: f64 }, HedgeMaxRateOutOfRange { max_rate: f64 }, HedgeWindowEmpty, NoStoreForLocation { location: String }, LocationNotAbsolute { location: String }, UnknownResidency { text: String } }
pub struct TableConfig { pub name: String, pub source: SourceConfig, pub r#ref: String, pub key_column: String, pub value_column: String, pub strict: bool /* default true */, pub residency: Residency /* default Full */, pub resident_budget_bytes: Option<u64> }
/// Where a table keeps its key chunks (RFD 017), ranked by lookup speed. Never chosen automatically.
/// Display gives the file's lowercase name, for the doctor's output and metric labels.
pub enum Residency { Full, Disk, Bloom }
/// Exactly the lowercase names `Display` prints (`full`, `disk`, `bloom`); any other word is
/// `ConfigViolation::UnknownResidency`. The one boundary parser for the setting when it arrives
/// from the command line; the file reads the same names through serde.
impl std::str::FromStr for Residency { type Err = ConfigViolation; }
/// `page_cache_disk_bytes`: zero means no disk level, anything else at least
/// `contract::PAGE_CACHE_DISK_BLOCK_BYTES`, the level's one block, or the
/// level would hold nothing; `Config::parse` refuses less as `PageCacheDiskTooSmall`.
pub struct LocalConfig { pub dir: PathBuf, pub index_budget_bytes: Option<u64>, pub page_cache_disk_bytes: u64 }
pub struct StorageConfig { pub s3: Option<S3Config>, pub rewrite: Vec<RewriteRule>, pub hedge: HedgeConfig, pub local: Option<LocalConfig> }
pub enum SourceConfig {
    Static { location: String },
    Rest { uri: String, warehouse: Option<String>, namespace: String, table: String },
}

// storage
/// One counter for a lookup's whole cost, whatever residency the table runs
/// (RFD 003): object-store GETs and local reads are counted in the same place.
/// Every GET is counted whether or not it succeeds; a hedge is a GET and
/// counts as one, and once more under `hedges` (RFD 013).
pub struct IoStats { /* gets, bytes, hedges, local_reads, local_bytes: AtomicU64 */ }
impl IoStats { pub fn gets(&self) -> u64; pub fn bytes(&self) -> u64; pub fn hedges(&self) -> u64; pub fn local_reads(&self) -> u64; pub fn local_bytes(&self) -> u64; }
/// A sliding window of recent lookup-GET latencies, owned by one `Table`;
/// the hedge delay is its configured percentile, capped by the hedge rate.
pub struct HedgeWindow;
impl HedgeWindow { pub fn new(config: &HedgeConfig) -> Self; pub fn delay(&self) -> Option<Duration>; pub fn record(&self, elapsed: Duration, hedged: bool); }

// local
/// The width of the digest a chunk file is named by and a footer is
/// identified by: SHA-256, which `digest` computes; the one home of the algorithm.
pub const DIGEST_BYTES: usize = 32;
pub fn digest(bytes: &[u8]) -> [u8; DIGEST_BYTES];
/// Why the local directory failed; closed. `Io` is the operation's error, the
/// rest are the format's boundaries and the blocking pool's.
pub enum LocalFault { Io(std::io::Error), RangeNotResident { start: u64, end: u64 }, ChunkLengthMismatch { start: u64, end: u64, len: u64 }, ChunkOverlap { start: u64, end: u64 }, HeaderTooLarge { len: usize }, Join(tokio::task::JoinError) }
pub struct LocalDir;
impl LocalDir {
    pub fn open(config: &LocalConfig, stats: Arc<IoStats>) -> Result<Self>;
    /// The chunk file for a data file, identified by path, size, and footer digest; `None` if absent or stale.
    pub fn chunks_for(&self, identity: &DataFileIdentity) -> Result<Option<DiskChunks>>;
    /// Writes verified key chunks, collected and sorted by data offset because the
    /// header names every span before the first payload byte; returns the handle
    /// the index keeps.
    pub fn write_chunks(&self, identity: &DataFileIdentity, chunks: impl Iterator<Item = (Range<u64>, Bytes)>) -> Result<DiskChunks>;
    /// Deletes chunk files no current generation references. `live` is the union
    /// over every table sharing the directory.
    pub fn retain(&self, live: &HashSet<DataFileIdentity>) -> Result<u64>;
    /// The bytes every file under `index/` occupies, and the bound from
    /// `LocalConfig::index_budget_bytes`; `generation` compares them at the
    /// footer stage (RFD 017).
    pub fn index_bytes_in_use(&self) -> Result<u64>;
    pub fn index_budget(&self) -> Option<u64>;
    /// `None` when `page_cache_disk_bytes` is zero; otherwise a level of at
    /// least one block, which `Config::parse` has checked.
    pub fn page_tier(&self) -> Option<DiskTier>;
}
/// A key page read from local disk on demand. Not a `ChunkReader`: the index
/// wraps the bytes a page read returns in a `Segments`.
pub struct DiskChunks;
impl DiskChunks {
    pub async fn page(&self, range: Range<u64>) -> Result<Bytes>;
    /// Whether one stored chunk holds the whole range; checked before a reused file skips the chunk fetch.
    pub fn holds(&self, range: &Range<u64>) -> bool;
    pub fn bytes_on_disk(&self) -> u64;
}
/// What a chunk file is named by and validated against; written into its
/// header inside the RFD 011 envelope as a `ChunkFileHeader` message.
pub struct DataFileIdentity { pub path: String, pub size: u64, pub footer_digest: [u8; DIGEST_BYTES] }
/// The header, a `pub(crate)` prost message declared in `local` (section 4);
/// its spans are what lets a restart serve pages without the footer.
// message ChunkFileHeader { string data_file_path = 1; uint64 data_file_size = 2; bytes footer_digest = 3; repeated ChunkSpan chunks = 4; }
// message ChunkSpan { uint64 data_offset = 1; uint64 length = 2; }
/// The page cache's disk level, handed to `PageCache` by `local` (RFD 017):
/// the `cache/` directory, its size bound, and the `IoStats` a page served
/// from it is charged to as one local read, the same counter `LocalDir`
/// charges chunk reads to (RFD 003).
pub struct DiskTier;
impl DiskTier { pub fn dir(&self) -> &Path; pub fn capacity_bytes(&self) -> u64; }
pub struct Located { pub store: Arc<dyn ObjectStore>, pub path: object_store::path::Path }
pub struct Storage;
impl Storage {
    pub fn new(config: StorageConfig) -> Self;
    pub async fn with_sdk_defaults(config: StorageConfig) -> Self;
    pub fn register(&self, prefix: impl Into<String>, store: Arc<dyn ObjectStore>);
    pub fn stats(&self) -> Arc<IoStats>;
    /// The hedge settings this storage runs under; `Table::new` builds the table's
    /// `HedgeWindow` from it, so the frozen `Table::new` takes no `HedgeConfig`.
    pub fn hedge_config(&self) -> &HedgeConfig;
    /// Rewrite rules first, then the store for the `scheme://authority/` prefix;
    /// no store is `ConfigViolation::NoStoreForLocation`, a relative path
    /// `ConfigViolation::LocationNotAbsolute`. `file:///path`, the authority-less
    /// `file:/path` a Hadoop catalog records, and a bare absolute path all name the
    /// local filesystem store.
    pub fn locate(&self, location: &str) -> Result<Located>;
    pub async fn read_all(&self, location: &str) -> Result<Bytes>;
    /// Load path: never hedged.
    pub async fn read_range(&self, l: &Located, range: Range<u64>) -> Result<Bytes>;
    /// Lookup path: hedged per `HedgeConfig` against the table's own latency
    /// window, since file size sets first-byte latency per table (RFD 013).
    /// Reads are timed on tokio's clock (`tokio::time::Instant`), which a paused
    /// runtime such as the simulation advances virtually, so the hedge window and
    /// the timer that consults it agree; the wall clock would feed the window real
    /// durations under a virtual timer and make the hedge count in simulation an
    /// artifact of machine load. Outside a paused runtime it is the same monotonic clock.
    pub async fn read_range_hedged(&self, l: &Located, range: Range<u64>, window: &HedgeWindow) -> Result<Bytes>;
    pub async fn read_ranges(&self, l: &Located, ranges: &[Range<u64>]) -> Result<Vec<Bytes>>;
}

// catalog
/// What makes a table unusable before any data file is read (`Error::Table`);
/// closed. `RefNotFound` is the one ref fault: iceberg-rust validates every
/// ref against the snapshot list when metadata is parsed or built, so a ref
/// that is present names a snapshot the metadata holds, and metadata whose
/// ref names a missing snapshot is refused at the parse boundary as
/// `Error::Json`. `BucketValueMissing` covers a null, another type, or a
/// negative value where the spec puts the key's bucket. `SnapshotTimestampOutOfRange` is a
/// snapshot timestamp jiff cannot represent, raised by `Generation::load` before any read.
pub enum TableFault { RefNotFound { ref_name: String }, SchemaMissing { schema_id: i32 }, SpecMissing { spec_id: i32 }, ColumnMissing { column: String }, ZeroBuckets { spec_id: i32 }, BucketValueMissing { path: String, spec_id: i32 }, VersionHintEmpty { root: String }, VersionHintMalformed { root: String, hint: String }, MetadataLocationUnknown, SnapshotTimestampOutOfRange { snapshot_id: SnapshotId } }
pub struct Resolved { pub metadata: Arc<TableMetadata>, pub location: String }
impl Resolved {
    /// A ref the metadata lacks is `TableFault::RefNotFound` under `table`.
    pub fn snapshot_for_ref(&self, table: &str, ref_name: &str) -> Result<Arc<Snapshot>>;
}
pub enum TableSource { Static { location: String }, Catalog { catalog: Arc<dyn Catalog>, ident: TableIdent } }
impl TableSource {
    /// Builds REST clients too; the only place a catalog client is constructed.
    /// Async because the REST builder is; constructing a client sends nothing. The
    /// REST client is built with the library's in-memory storage factory: the
    /// library refuses to load a table without one and builds file I/O from it
    /// lazily, and icetray never reads through the library, so the factory is a
    /// placeholder that reaches an empty store, never the host, should it ever be
    /// asked to build.
    pub async fn from_config(cfg: &SourceConfig) -> Result<Self>;
    /// A static location ending in `.metadata.json` names the file itself and
    /// never advances; any other is a table root followed through
    /// `metadata/version-hint.text`, whose content is a bare version, a file
    /// name under `metadata/`, or a full location (nothing but whitespace is
    /// `VersionHintEmpty`, more than one token or not UTF-8 is
    /// `VersionHintMalformed`). A catalog record without a metadata location
    /// is `MetadataLocationUnknown`.
    pub async fn resolve(&self, storage: &Storage) -> Result<Resolved>;
    /// One small read when nothing moved: the version hint of a static root,
    /// parsing no metadata, or one table load from the catalog, whose metadata
    /// the client parses either way; a pinned metadata file reads nothing.
    pub async fn resolve_if_changed(&self, storage: &Storage, known: &str) -> Result<Option<Resolved>>;
}
/// The metadata file a bare `version` names under `root`: `metadata/v<version>.metadata.json`,
/// the Hadoop catalog's layout. A writer publishing a snapshot by hand (the testkit's
/// `Fixture::commit_by_hand`) names its metadata file through it.
pub fn version_file(root: &str, version: u64) -> String;
/// The location of the version hint under `root`: `metadata/version-hint.text`. Core's own
/// resolver reads the hint through it and the testkit's hint writer names the file through it,
/// so this module stays the one home of the layout under a table root; `VERSION_HINT`,
/// `METADATA_DIR`, and `under` stay `pub(crate)`.
pub fn hint_location(root: &str) -> String;

// manifest
/// The bucket transform on the key, `bucket[count]` over `source_field_id`.
/// Files of one snapshot carry different bucketings after a spec evolution,
/// and a key's bucket differs under each. Ord, so it keys routes and orders files.
pub struct Bucketing { pub count: u32, pub source_field_id: i32 }
impl Bucketing { pub fn bucket_of(&self, key: &Key) -> u32; }
/// `bucketing` is the default spec's, the one new files are written under,
/// which `TableInfo` and the doctor report; each data file routes by its own
/// manifest's spec (`DataFileEntry::bucketing`).
pub struct TableLayout { pub bucketing: Option<Bucketing>, pub key_column: String, pub key_field_id: i32, pub key_type: KeyType, pub value_column: String, pub value_field_id: i32 }
impl TableLayout {
    /// From the snapshot's own schema, never the table's current one, except
    /// for a snapshot recording no schema id (written before Iceberg recorded
    /// them), which is read under the current schema as Iceberg readers read
    /// it. A schema id the metadata lacks or a configured column absent from
    /// the schema is a `TableFault`; a key type `KeyType::from_iceberg` does
    /// not admit is `KeyTypeUnsupported`; a value type other than binary,
    /// fixed, or string is `ValueTypeUnsupported`. The bucketing is the first
    /// field of the default spec, in spec order, that buckets the key; a
    /// count of zero there is `TableFault::ZeroBuckets`.
    pub fn derive(table: &str, md: &TableMetadata, snapshot: &Snapshot, key_column: &str, value_column: &str) -> Result<Self>;
    /// Under the default spec's transform; `0` for a table without one.
    pub fn bucket_of(&self, key: &Key) -> u32;
}
/// `bucketing` is the transform of the file's own manifest's spec, `None`
/// when that spec has no bucket on the key, and `bucket` is then `0`, the one
/// bucket every unbucketed file is in.
pub struct DataFileEntry { pub path: String, pub bucket: u32, pub bucketing: Option<Bucketing>, pub records: u64, pub size: u64, pub key_min: Option<Key>, pub key_max: Option<Key> }
/// Reads every manifest of the snapshot, delete manifests included, and skips
/// entries of status `Deleted`; a live entry whose content is not data is
/// `Error::Refused { reason: RefusalReason::DeleteFiles }`, so a delete
/// manifest whose entries are all deleted serves. Bucket position comes from
/// each manifest's spec, looked up in the metadata by the id the manifest list
/// records: missing is `TableFault::SpecMissing`, a partition tuple without a
/// non-negative integer at the bucket field is `TableFault::BucketValueMissing`.
/// Ordered by (bucketing, bucket, `key_min` with `None` first, path), the same
/// for the same manifests in any order.
pub async fn list_data_files(table: &str, md: &TableMetadata, snapshot: &Snapshot, layout: &TableLayout, storage: &Storage) -> Result<Vec<DataFileEntry>>;

// parquet_io
pub struct FileHandle { pub location: String }
impl FileHandle {
    /// `size` is the manifest's, so opening issues no request. `hedge` is the
    /// owning table's window (RFD 013): lookup reads are hedged against it and
    /// recorded into it, load reads never touch it.
    pub fn open(storage: Arc<Storage>, location: &str, size: u64, hedge: Arc<HedgeWindow>) -> Result<Self>;
    pub fn size(&self) -> u64;
    pub fn storage(&self) -> &Arc<Storage>;
    /// `local::digest` of the footer (the thrift metadata and the eight
    /// trailing bytes), the `footer_digest` of a `DataFileIdentity`; `Some`
    /// once `load_metadata` has returned, computed from the bytes that load
    /// fetched, so it costs no request of its own.
    pub fn footer_digest(&self) -> Option<[u8; local::DIGEST_BYTES]>;
    /// Load path, never hedged: one load GET per range.
    pub async fn read_range(&self, range: Range<u64>) -> Result<Bytes>;
    pub async fn read_ranges(&self, ranges: &[Range<u64>]) -> Result<Vec<Bytes>>;
    /// Lookup path: one lookup GET, hedged against the table's window.
    pub async fn read_range_hedged(&self, range: Range<u64>) -> Result<Bytes>;
    /// The footer and both page indexes behind a tail prefetch: one load GET
    /// when the tail fits it, one more when the indexes start before the
    /// prefetched tail, two more when the thrift metadata itself does not fit.
    /// The page indexes are read when the file has them; a file without them
    /// loads with neither and `index` refuses it as `FileFault::MissingPageIndex`.
    /// A failed fetch comes back as the storage error it was, not as a parquet error.
    pub async fn load_metadata(self: &Arc<Self>) -> Result<Arc<ParquetMetaData>>;
    pub async fn read_page(self: &Arc<Self>, range: Range<u64>, cache: Option<&PageCache>) -> Result<Bytes>;
    /// `read_page` with where the page came from; a read without a cache is always
    /// `Fetched`. The frozen `read_page` wraps it.
    pub(crate) async fn read_page_with_origin(self: &Arc<Self>, range: Range<u64>, cache: Option<&PageCache>) -> Result<(Bytes, PageOrigin)>;
}
/// Why a `Segments` refused a read; carried inside `ParquetError::External`.
pub enum SegmentsFault { NotResident { offset: u64 }, CrossesResidentRange { start: u64, end: u64, held_start: u64, held_end: u64 } }
pub struct Segments;
impl Segments {
    pub fn new(parts: Vec<(Range<u64>, Bytes)>, file_size: u64) -> Self; pub fn resident_len(&self) -> usize;
    /// The `SegmentsFault` a parquet error carries, when it is one, so a caller matches the variant.
    pub fn fault(error: &ParquetError) -> Option<&SegmentsFault>;
}
impl parquet::file::reader::ChunkReader for Segments { /* errors, typed as SegmentsFault, on non-resident bytes */ }

// index (MAX_KEY_BYTES comes from `contract`)
pub struct RowRef { pub row_group: usize, pub row: i64 }
/// What makes a data file unservable beyond the contract refusals; closed set.
/// A key physical type that does not match is a refusal (`KeyTypeMismatch`),
/// never a fault. A value column stored as anything but a byte array is
/// `ValuePhysicalType`, a fault: `TableLayout::derive` admitted the column's
/// Iceberg type from the schema, and a conformant writer stores binary, fixed,
/// and string as byte arrays, so the file contradicts its schema.
/// `InconsistentRowCounts`, `InconsistentOffsetIndex`, and `PageRowsMismatch`
/// are a footer that the file contradicts: a negative count or row groups
/// that do not sum to the file's, an offset index whose pages are not where
/// or what it says, a key or value page holding other rows than its offset
/// index gives it.
pub enum FileFault { MissingColumn(String), ValuePhysicalType { got: parquet::basic::Type }, MissingPageIndex, NoRows, InconsistentRowCounts, InconsistentOffsetIndex { row_group: usize }, PageRowsMismatch { row_group: usize, page: usize }, PageOutOfRange { row: i64, row_group: usize }, CorruptBloomFilter { row_group: usize }, UnsizedBloomFilter { row_group: usize }, KeyNotUtf8 { row_group: usize } }
/// Every tier's resident bytes for one file, from the footer alone (RFD 017).
pub struct ResidentBytes { pub full: u64, pub disk_memory: u64, pub disk_bytes: u64, pub bloom: Option<u64> /* None when a row group lacks a filter */, pub bloom_bits_per_key: Option<f64> }
pub struct FileIndex;
impl FileIndex {
    /// Refuses (typed, under `table`) an unsorted key column, a null key, a
    /// key repeated within the file, a key physical type that does not match
    /// `key_type` (Binary accepts BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY), keys
    /// over the cap, in the Bloom tier a row group without a key bloom
    /// filter, and in the Disk tier the absence of a local directory; a value
    /// column whose physical type is neither BYTE_ARRAY nor
    /// FIXED_LEN_BYTE_ARRAY is `FileFault::ValuePhysicalType`. Every tier
    /// streams the key chunks once for verification, checking every key page
    /// against its offset index; Full keeps the chunks in memory, Disk writes
    /// them through `LocalDir` (or reuses a chunk file with matching identity
    /// and skips the chunk fetch), Bloom keeps filters. The column index of
    /// every column but the key is dropped after the offset indexes are
    /// taken; it is never part of the resident set. `build` is `inspect`
    /// followed by `Inspected::build`.
    pub async fn build(table: &str, handle: Arc<FileHandle>, key_column: &str, value_column: &str, key_type: KeyType, residency: Residency, local: Option<&LocalDir>) -> Result<Self>;
    /// The footer stage of `build` on its own: one GET, every footer check,
    /// the footer trimmed to what the index keeps, and the prediction, so
    /// `generation` checks every file against a budget before any key chunk
    /// is fetched (RFD 017).
    pub async fn inspect(table: &str, handle: Arc<FileHandle>, key_column: &str, value_column: &str, key_type: KeyType) -> Result<Inspected>;
    /// Every tier's resident bytes for this file, from the footer alone, the
    /// same from the footer as loaded and as kept: no other column's column
    /// index is counted either way, so the budget check at the footer stage
    /// predicts what `resident_bytes` reports.
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
    /// `read_value` with the value page's origin, through `fetch_pages`, which returns the
    /// page's own origin and not the dictionary page's; the frozen `read_value` wraps it, and
    /// `find` discards the key page's origin.
    pub(crate) async fn read_value_with_origin(&self, row: RowRef, cache: Option<&PageCache>) -> Result<(Option<Bytes>, PageOrigin)>;
    pub fn resident_bytes(&self) -> usize;
    pub fn max_key_bytes(&self) -> usize;
    pub fn metadata(&self) -> &Arc<ParquetMetaData>;
    /// The page a key would decode and the value page it would fetch, for
    /// tests and the doctor; `None` when `find` would decode nothing (a key
    /// outside the range, between two row groups, or ruled out by the Bloom
    /// tier's filter), so `Generation::plan` predicts the key-page GET from it.
    pub fn page_of(&self, key: &Key) -> Option<PageRef>;
    /// For `generation` and `check`, in-crate: the tier built, the file, its
    /// row count, the key and value leaves, the fences per row group, the key
    /// pages of a row group and the rows one of them holds.
    pub fn residency(&self) -> Residency; pub fn handle(&self) -> &Arc<FileHandle>; pub fn rows(&self) -> i64; pub fn key_leaf(&self) -> usize; pub fn value_leaf(&self) -> usize; pub fn fences(&self) -> &[Vec<Key>]; pub fn page_count(&self, row_group: usize) -> usize; pub fn page_rows(&self, row_group: usize, page: usize) -> Range<i64>;
    /// The key column of a row group, or one page of it, decoded from `chunks` with no I/O; for the overlap check.
    pub fn decode_keys(&self, row_group: usize, page: Option<usize>, chunks: &Segments) -> Result<Keys>;
    /// The key chunks for the overlap check: Full hands out the resident
    /// chunks at no cost, Disk reads them back as one counted local read per
    /// row group and no GET, Bloom fetches them again as one counted load GET
    /// per row group.
    pub async fn key_chunks_for_verification(&self) -> Result<Arc<Segments>>;
}
/// A data file past the footer stage: its footer checked and trimmed, its
/// resident bytes predicted, and no key chunk fetched yet.
pub struct Inspected;
impl Inspected {
    pub fn predicted(&self) -> &ResidentBytes;
    pub fn handle(&self) -> &Arc<FileHandle>;
    /// What a Disk-tier `build` would add to `local`'s `index/`: zero when a
    /// chunk file with the file's identity already holds every key chunk, so
    /// a restart is not charged for what is on disk, else
    /// `predicted().disk_bytes`. Reads the chunk file's header and no chunk.
    pub fn disk_bytes_needed(&self, local: &LocalDir) -> Result<u64>;
    /// The chunk stage of `FileIndex::build`.
    pub async fn build(self, residency: Residency, local: Option<&LocalDir>) -> Result<FileIndex>;
}
/// A decoded run of keys in file order, one arm per key type over the values
/// parquet decoded, searched and compared in the served order; an in-crate
/// helper, not re-exported from `lib.rs`.
pub enum Keys { Long(Vec<i64>), Str(Vec<ByteArray>), Bin(Vec<ByteArray>), Uuid(Vec<[u8; UUID_BYTES]>) }
impl Keys { pub fn key_type(&self) -> KeyType; pub fn len(&self) -> usize; pub fn is_empty(&self) -> bool; pub fn get(&self, at: usize) -> Key; pub fn binary_search(&self, key: &Key) -> Result<usize, usize>; pub fn cmp_at(&self, at: usize, other: &Self, other_at: usize) -> Ordering; pub fn max_len(&self) -> usize; pub fn is_strictly_increasing(&self) -> bool; pub fn first_not_increasing(&self) -> Option<(usize, Ordering)>; }
/// `value_page_offset` is `Some` in the Full tier for a present key, where
/// the key page is resident and decodes without I/O; `None` for an absent
/// key and in the Disk and Bloom tiers, where naming the value page would
/// take a read.
pub struct PageRef { pub row_group: usize, pub key_page: usize, pub value_page_offset: Option<u64> }

// cache
/// `Backend` keeps foyer's error whole: it is `std::error::Error + Send + Sync`
/// and carries its kind, so a caller matches on it rather than on text.
pub enum CacheError { Fetch(Arc<Error>), Backend(#[from] foyer::Error) }
/// Where one call's page came from, in-crate. `Fetched` for a fetch the caller ran or
/// joined, since it waited on the GET; `Cached` for memory or the disk level, which cost none.
pub(crate) enum PageOrigin { Fetched, Cached }
/// Clone: clones share one cache and one pair of counters, so every table
/// holds a clone of the process's one cache.
pub struct PageCache;
impl PageCache {
    /// `disk` is the local level of RFD 017: a page evicted from memory is
    /// served from it with a local read and no GET, and it survives a restart.
    /// Async and fallible because opening the level opens its files and
    /// recovers what they hold; a failure is `CacheError::Backend`. A disk
    /// level that fails to read a page falls through to the fetch.
    pub async fn new(capacity_bytes: usize, disk: Option<DiskTier>) -> Result<Self>;
    pub async fn get_or_fetch<F, Fut>(&self, key: (Arc<str>, u64), fetch: F) -> Result<Bytes>
    where F: FnOnce() -> Fut, Fut: Future<Output = Result<Bytes>> + Send + 'static;
    /// `get_or_fetch` with where the page came from, per call and never by differencing the
    /// process-wide counters; the frozen `get_or_fetch` wraps it and drops the origin.
    pub(crate) async fn fetch_with_origin<F, Fut>(&self, key: (Arc<str>, u64), fetch: F) -> Result<(Bytes, PageOrigin)>
    where F: FnOnce() -> Fut, Fut: Future<Output = Result<Bytes>> + Send + 'static;
    /// `fetches` counts fetch futures run, after coalescing: concurrent misses
    /// on one key run one and count one. `hits` counts lookups served without
    /// a fetch, from memory or the disk level, per lookup served: several
    /// lookups coalesced onto one disk read each count a hit and a local read,
    /// as each would under `DiskChunks::page`, since foyer names no leader
    /// among them; a lookup coalesced onto another's fetch counts under neither.
    pub fn hits(&self) -> u64; pub fn fetches(&self) -> u64; pub fn usage(&self) -> usize;
    /// Flushes the disk level and closes it; `serve` calls it while draining
    /// (RFD 012), so what memory still held reaches disk before the process
    /// exits. A memory-only cache returns `Ok` and does nothing.
    pub async fn close(&self) -> Result<()>;
}

// generation
pub struct LoadedFile { pub entry: DataFileEntry, pub index: FileIndex }
pub struct Generation {
    pub table: String, pub snapshot_id: SnapshotId, pub sequence_number: i64, pub metadata_location: String,
    pub layout: TableLayout, pub residency: Residency, pub files: Vec<Arc<LoadedFile>> /* sorted by (bucketing, bucket, exact first key, path) */,
    pub records: u64, pub index_bytes: usize, pub loaded_at: Instant,
    /// The snapshot's own timestamp, from `snapshot.timestamp_ms()` through
    /// `time::timestamp_from_millis`, set in `load` before any read; the binary samples
    /// `generation_age_seconds` from it against now.
    pub snapshot_timestamp: jiff::Timestamp,
}
impl Generation {
    /// `config` carries the residency and the resident-bytes budget; the
    /// budget is checked at the footer stage, before any key chunk is fetched:
    /// Full compares `config.resident_budget_bytes` with the reused files'
    /// resident bytes plus every new file's predicted full bytes; Disk
    /// compares `local.index_budget()` with the bytes `index/` holds plus every
    /// new file's `Inspected::disk_bytes_needed`; Bloom takes none; over is
    /// `RefusalReason::OverBudget`. A file is reused from `previous` when its
    /// path and size are unchanged. Overlap groups that gained a file are
    /// verified one at a time, so the load holds one group's key chunks at
    /// once and not the table's (RFD 017).
    /// `hedge` is the table's hedge window, handed to every FileHandle it opens.
    /// The table's name is `config.name`; `load` takes no second copy of it.
    pub async fn load(config: &TableConfig, resolved: &Resolved, snapshot: &Snapshot, storage: &Arc<Storage>, hedge: Arc<HedgeWindow>, local: Option<&LocalDir>, previous: Option<&Generation>) -> Result<Self>;
    /// The files that may hold `key`, in file order: under every bucketing
    /// the files carry, the files of the bucket the key falls in under it,
    /// and every file without a bucketing, narrowed to those whose exact key
    /// range covers the key.
    pub fn candidates(&self, key: &Key) -> impl Iterator<Item = &Arc<LoadedFile>>;
    /// Per bucket number an overlap group touches, the widest such group. Two
    /// files may share a key when their exact ranges intersect and one key can
    /// fall in both their buckets: the same bucketing and bucket, two
    /// bucketings with buckets congruent modulo the gcd of their counts, or no
    /// bucketing on either; groups are the connected components. Buckets of
    /// different bucketings that share a number share one entry.
    pub fn overlapping_buckets(&self) -> &BTreeMap<u32, usize>;
    pub fn overlap_groups_verified(&self) -> usize;
    pub async fn get(&self, key: &Key, cache: &PageCache) -> Result<Value>;
    /// `get` with the `result` label: `Miss` for `Absent`; for a present key `ColdHit` when
    /// its value page was `Fetched` and `WarmHit` when `Cached`. Only the value page decides
    /// it, never a key-page GET in the Bloom tier or a chunk-file read in the Disk tier. The
    /// frozen `get` wraps it and drops the label.
    pub(crate) async fn lookup(&self, key: &Key, cache: &PageCache) -> Result<(Value, LookupResult)>;
    /// Which (file, value page) each key would fetch; the oracle for cost
    /// tests. The first candidate whose `page_of` names a value page, which in
    /// the Full tier is the file holding the key wherever in an overlap group
    /// it sits; when none does, the first candidate that would decode a key
    /// page, the first probe; `None` when none would.
    pub fn plan(&self, keys: &[Key]) -> Vec<Option<(Arc<LoadedFile>, PageRef)>>;
    /// The chunk-file identity of every file, for `LocalDir::retain`, whose
    /// live set is the union over every table sharing the directory.
    pub fn identities(&self) -> HashSet<DataFileIdentity>;
}
/// The three outcomes of a lookup; the wire carries the same three.
pub enum Value { Present(Bytes), Null, Absent }
pub struct Lookup { pub value: Value, pub snapshot_id: SnapshotId }
/// Fails as a whole if any key's read fails; partial results are a later, separate call.
pub struct MultiLookup { pub values: Vec<Value>, pub snapshot_id: SnapshotId }

// table (after `check`, because the strict gate runs `check::diagnose` on a
// built generation before it is swapped in)
/// Clone and Debug; Display gives the `state` label (`loading`, `serving`, `degraded`).
/// `Serving` means a generation answers and the last load or refresh succeeded; `since` is
/// when the state began (the swap, or the poll that ended a `Degraded` state). `Degraded`
/// means the last load or refresh failed while the generation before it, if any, keeps
/// answering (RFD 005); it ends at the next swap or at the next poll that finds the table in
/// step with its ref (`Ok(false)`). `Degraded.reason` is the failure's `ErrorKind` (a
/// refusal with its reason, the strict gate, a table or file fault, or the library or
/// local-directory failure's variant); the failure itself went to the caller of `load` or
/// `refresh`, which logs it, since `Error` is not `Clone`.
pub enum TableState { Loading, Serving { snapshot_id: SnapshotId, since: Instant }, Degraded { serving: Option<SnapshotId>, reason: ErrorKind, since: Instant } }
/// Owns the table's `HedgeWindow`, built once in `new` from `Storage::hedge_config` and
/// handed to every `Generation::load`, so one table's delay is set from its own reads and
/// never another's (RFD 013). Send + Sync; Debug names the table and its state. Holds the
/// serving generation in an `ArcSwapOption`, the state in a std mutex never held across an
/// await, the metadata location it last admitted or found unchanged in snapshot (`known`),
/// and one tokio mutex that serializes `load` and `refresh`. `known` is what `refresh`
/// polls against; a failed build does not advance it, so every later poll reads the metadata
/// again and retries, a transient failure recovers on the next poll, and a persistent refusal
/// is counted under `refresh_errors_total` once per poll. `load`, `refresh`, `get`, and
/// `Opened::retain` return `Send` futures: the daemon spawns them onto a multi-thread
/// runtime, so `manifest::list_data_files` collects the manifest files into a `Vec` before
/// streaming them and `generation::gather_chunks` streams group positions by value, since an
/// opaque `impl IntoIterator` fed to `stream::iter` and a closure taking a reference and
/// returning an `async` block each make every future up the call chain unprovably `Send`;
/// `table::tests::test_load_refresh_and_get_are_spawnable` and
/// `open::tests::test_retain_is_spawnable` pin the property.
pub struct Table;
impl Table {
    /// Constructs in `Loading`; nothing is read until `load`. Sets `table_state` for the state.
    pub fn new(config: TableConfig, source: TableSource, storage: Arc<Storage>, cache: PageCache, local: Option<Arc<LocalDir>>) -> Self;
    /// First load. `Err` leaves the table `Degraded` with no generation and is not counted
    /// under `refresh_errors_total`: a first-load failure is the caller's to report (RFD 005).
    /// On a serving table, a full re-resolve and reload reusing every unchanged file.
    pub async fn load(&self) -> Result<()>;
    /// Serialized per table. `Ok(true)` when a new generation was swapped in, counting
    /// `generation_swaps_total`. With no current generation it does what `load` does (a table
    /// left `Degraded` with no generation recovers on the next poll). Otherwise
    /// `resolve_if_changed(&storage, &known)`: `None` is `Ok(false)`; a snapshot whose id equals
    /// the current generation's records the new location and is `Ok(false)`; else the build,
    /// the gate, and the swap. `Err` leaves the current generation serving, sets `Degraded`,
    /// and counts `refresh_errors_total{table, reason}` with `reason` the `ErrorKind`'s Display.
    pub async fn refresh(&self) -> Result<bool>;
    pub fn name(&self) -> &str;
    pub fn config(&self) -> &TableConfig;
    pub fn state(&self) -> TableState;
    pub fn current(&self) -> Option<Arc<Generation>>;
    /// `Error::NotReady` with no generation; `Error::KeyType` before any read. One
    /// `load_full`, one lookup through `Generation::lookup`, timed and counted under
    /// `lookups_total{table, result}` and `lookup_seconds{table, result}`.
    pub async fn get(&self, key: &Key) -> Result<Lookup>;
    /// One `load_full`; every key's type checked before any read (a mismatch anywhere fails
    /// the whole call and issues nothing); the lookups run concurrently against that one
    /// generation, each counted as `get` counts; any `Err` fails the whole call (RFD 010).
    pub async fn multi_get(&self, keys: &[Key]) -> Result<MultiLookup>;
    /// Holds the table's load and refresh lock, for `Opened::retain`, which takes every
    /// table's in `tables` order; nothing else ever holds two.
    pub(crate) async fn serialized(&self) -> tokio::sync::MutexGuard<'_, ()>;
}
// The gate: `check::diagnose` on the built generation; every finding logged (cost at warn,
// advice at info, with `table`, `%code`, `?tier`, `file`, and the message); under `strict`
// an `Inefficient` verdict drops the generation as `Error::Inefficient` with the distinct
// cost codes in report order; advice never affects admission. The swap: `current.store`,
// then `Serving`; `index_bytes{table}` through `approx::gauge_len`; `findings{table, code}`
// set for every `FindingCode::ALL` entry from the admitted generation's counts, zero for
// the absent ones, so the gauge always describes the serving generation and a refused
// generation's findings reach only the logs and the error; `generation_info` 0 for the
// replaced snapshot and 1 for the new; `table_state{table, state}` 1 for the current state
// and 0 for the other two on every state change; one info event with `files`, `reused`
// (by `Arc::ptr_eq`), `records`, `residency`, `index_bytes`.

// check
pub enum Tier { Advice, Cost }
pub enum Serviceability { Efficient, Inefficient }
pub struct Penalty { pub extra_gets_per_hit: Option<u32>, pub keys_decoded_per_lookup: Option<i64>, pub files_probed_per_lookup: Option<usize> }
pub enum FindingCode { DictionaryEncodedValue, LargeKeyPages, NoBucketPartition, OverlappingFiles, BoundsMissing, ColumnIndexTruncated, LargeValuePages, LongKeys, LargeFile, ManyFilesPerBucket, NoBloomFilter, BloomOversized, BloomUndersized }
impl FindingCode {
    /// Every code in finding-table order, for the `findings` gauge, which `table` sets for every
    /// code on every admitted generation, zero for the absent ones; a test proves it complete.
    pub const ALL: [FindingCode; 13];
}
/// Writer properties the doctor names; Display gives the property string.
/// The per-column ones (`MetricsColumn`, `BloomFilterEnabled`,
/// `BloomFilterNdvColumn`) render the property up to its trailing dot and
/// the finding's message appends the column.
pub enum WriterProperty { EnableDictionary, ColumnIndexTruncateLength, PageRowLimit, PageSizeBytes, RowGroupCheckRecordCount, TargetFileSizeBytes, MetricsColumn, BloomFilterEnabled, BloomFilterMaxBytes, BloomFilterNdvColumn, SkewSplitting }
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
pub mod thresholds { pub const KEY_PAGE_ROWS_COST: i64 = 2 * contract::PAGE_ROWS; pub const VALUE_PAGE_ADVICE_FACTOR: u64 = 16; pub const VALUE_PAGE_BYTES_ADVICE: u64 = VALUE_PAGE_ADVICE_FACTOR * contract::VALUE_PAGE_BYTES /* 1 MiB: where a page's transfer reaches the order of a round trip; the cost below it is linear and small */; pub const FILE_BYTES_ADVICE: u64 = 2 * contract::TARGET_FILE_BYTES_MAX; pub const FILES_PER_BUCKET_FACTOR: u64 = 2; pub const BLOOM_BYTES_PER_KEY_ADVICE: f64 = 4.0; pub const BLOOM_BITS_PER_KEY_MIN: f64 = 8.0; pub const DISTINCT_MINS_RATIO_ADVICE: f64 = 0.5; }

// contract (the layout's numbers; every other crate imports these, none redeclares them)
pub const PAGE_ROWS: i64 = 1_000;
pub const VALUE_PAGE_BYTES: u64 = 64 * 1024;
pub const TARGET_FILE_BYTES_MIN: u64 = 32 * 1024 * 1024;
pub const TARGET_FILE_BYTES_MAX: u64 = 64 * 1024 * 1024;
pub const MAX_KEY_BYTES: usize = 4_096;
pub const KEY_BYTES_ADVICE: usize = 128;
/// The page cache disk level's block: its unit of eviction and the smallest level that holds a page.
pub const PAGE_CACHE_DISK_BLOCK_BYTES: u64 = 64 * VALUE_PAGE_BYTES;

// time (the one place a Duration or a timestamp becomes a number or text, and the wall clock is read)
/// A duration in seconds: the histogram unit, and the unit the latency buckets are declared in.
pub fn seconds(duration: Duration) -> f64;
/// Whole microseconds, the `get` line's `elapsed_micros`; saturates at `u64::MAX`.
pub fn micros(elapsed: Duration) -> u64;
/// The wall clock now; a caller that needs a timestamp never names the clock (the binary has no `jiff`).
pub fn now() -> jiff::Timestamp;
/// Seconds from `since` to now, the `generation_age_seconds` gauge; a `since` in the future gives a
/// negative age, kept as measured.
pub fn age_seconds(since: jiff::Timestamp) -> f64;
/// RFC 3339 in UTC with a `Z` offset through jiff's printer, the fraction of a second only when it is
/// non-zero; the one place a timestamp becomes log text.
pub fn log_timestamp(at: jiff::Timestamp) -> String;
/// In-crate: an Iceberg snapshot's epoch millisecond as a timestamp; out of jiff's range is `Err`,
/// which `Generation::load` reports as `TableFault::SnapshotTimestampOutOfRange`.
pub(crate) fn timestamp_from_millis(millis: i64) -> Result<jiff::Timestamp, jiff::Error>;

// open
/// A private `complete` field records whether `tables` is every configured table.
pub struct Opened { pub storage: Arc<Storage>, pub cache: PageCache, pub local: Option<Arc<LocalDir>>, pub tables: Vec<Arc<Table>> }
impl Opened {
    /// Deletes the chunk files no table's serving generation references and returns the
    /// bytes freed; `Ok(0)` with no local directory, and `Ok(0)` for an `Opened` that `only`
    /// narrowed, since the tables left out share the directory and their chunk files are
    /// live to them. Holds every table's load lock in `tables` order while the live set (the
    /// union of every table's `Generation::identities`) is computed and applied. The one
    /// place chunk files are deleted: the serve loop calls it after every table's first
    /// `load` and after any `refresh` that returned `Ok(true)`; a one-shot subcommand may
    /// call it before exit.
    pub async fn retain(&self) -> Result<u64>;
}
/// Builds storage from the SDK chain, one cache, and every configured table
/// in `Loading`; `only` narrows by name, and a name matching no table is `Ok` with empty
/// `tables`, whose message and exit code are the caller's. Issues no GET. Loading is the
/// caller's step. `lib.rs` re-exports `Opened`, `open`, `Table`, `TableState`, `ErrorKind`.
pub async fn open(config: &Config, only: Option<&str>) -> Result<Opened>;
/// The body of `open` over a storage the caller built, for the testkit and embedders: a test
/// constructs one with `Storage::new`, registers its own stores on it, and never touches the
/// SDK chain. `LocalDir::open` when configured, one `PageCache` with the directory's
/// `page_tier`, one `Table` per configured name matching `only`. The binary uses `open`; the
/// disk-tier acceptance tests count `Opened::retain` over two tables through this.
pub async fn open_with(config: &Config, only: Option<&str>, storage: Arc<Storage>) -> Result<Opened>;
```

Finding codes and tiers (RFD 007, RFD 016). The code column is the Display
form of the `FindingCode` variant; code never compares these strings:

| code | tier | trigger | penalty | property named in advice |
|---|---|---|---|---|
| `dictionary_encoded_value` | Cost | value column dictionary-encoded | one extra GET per hit | `parquet.enable.dictionary` |
| `large_key_pages` | Cost | key page rows above `KEY_PAGE_ROWS_COST` | rows decoded per lookup | `write.parquet.page-row-limit` |
| `no_bucket_partition` | Cost | a file written under a spec with no bucket transform on the key, and more than one file (each file's own spec, so an evolved table is priced by the files every lookup probes) | files probed per lookup: the unbucketed files plus one when bucketed files exist | partition spec |
| `overlapping_files` | Cost | intersecting exact ranges within a bucket | files probed per lookup | `spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled` |
| `bounds_missing` | Advice | manifest lacks key bounds, or its bounds are shorter than the file's exact first or last key | | `write.metadata.metrics.column.<key>` |
| `column_index_truncated` | Advice | distinct page minimums ratio below threshold | | `parquet.columnindex.truncate.length` |
| `large_value_pages` | Advice | mean uncompressed value page above 1 MiB (`FileReport::avg_value_bytes` beside it says how many values a hit fetches for one; reported, not thresholded) | | `write.parquet.page-size-bytes` and the row-group-check properties |
| `long_keys` | Advice | key bytes above `KEY_BYTES_ADVICE` | | |
| `large_file` | Advice | data file above `FILE_BYTES_ADVICE` | | `write.target-file-size-bytes` |
| `many_files_per_bucket` | Advice | files in a bucket above twice what the bucket's bytes divided by `TARGET_FILE_BYTES_MAX` explain (a rolled sorted task is not many files) | | compaction |
| `no_bloom_filter` | Advice | no bloom filter on the key | | `write.parquet.bloom-filter-enabled.column.<key>` |
| `bloom_oversized` | Advice | bloom bytes per key above threshold in any row group (filters are per row group; the report keeps the file's figure) | | `write.parquet.bloom-filter-max-bytes` (Iceberg 1.11+) |
| `bloom_undersized` | Advice | bloom bits per key below `BLOOM_BITS_PER_KEY_MIN` in any row group (a miss consults one row group's filter, so its rate is what rises) | | `write.parquet.bloom-filter-ndv.column.<key>` (Iceberg 1.11+) |

Refusals (never findings; `Error::Refused` at load) are the `RefusalReason`
variants: unsorted (a key below the one before it, within a row group or
across two), null keys, duplicate keys (a key equal to the one before it
within a file, or shared by two files of a bucket; never reported as
unsorted), key type mismatch, key type unsupported (the key column's Iceberg
type is none of long, string, binary, fixed, or uuid), value type
unsupported (the value column must be Iceberg binary, fixed, or string,
served as opaque bytes), delete files, key too long, and the three residency
refusals of RFD 017: over budget, no local directory, no bloom filter. The
reasons raised for one file carry its path, so a log line names the file.
The prototype's per-table `key_chunk_cost` line becomes a `FileReport`
field, not a finding.

## 4. `icetray-proto`

```proto
service Icetray { rpc Get(GetRequest) returns (GetResponse); rpc MultiGet(MultiGetRequest) returns (MultiGetResponse); rpc TableInfo(TableInfoRequest) returns (TableInfoResponse); }
// `bin` carries binary, fixed, and uuid keys; the table's key type says how
// its bytes are read (RFD 008).
message Key { oneof key { int64 long = 1; string str = 2; bytes bin = 3; } }
message GetRequest { string table = 1; Key key = 2; }
enum Presence { PRESENCE_UNSPECIFIED = 0; PRESENCE_PRESENT = 1; PRESENCE_NULL = 2; PRESENCE_ABSENT = 3; }
message GetResponse { Presence presence = 1; bytes value = 2; int64 snapshot_id = 3; }
message MultiGetRequest { string table = 1; repeated Key keys = 2; }
message Hit { Presence presence = 1; bytes value = 2; }
message MultiGetResponse { repeated Hit results = 1; int64 snapshot_id = 2; }
message TableInfoRequest { string table = 1; }
enum KeyType { KEY_TYPE_UNSPECIFIED = 0; KEY_TYPE_LONG = 1; KEY_TYPE_STRING = 2; KEY_TYPE_BINARY = 3; KEY_TYPE_UUID = 4; }
message TableInfoResponse { string table = 1; int64 snapshot_id = 2; uint32 buckets = 3; uint64 data_files = 4; uint64 records = 5; KeyType key_type = 6; uint64 index_bytes = 7; }
```

The one message icetray persists for itself, the header of a chunk file, is
not in this crate. It is a prost message declared in `icetray-core::local`
beside its reader, inside RFD 011's envelope (a magic byte, a version, then
the message length-prefixed), with no `.proto` file: nothing outside icetray
reads a chunk file, and `icetray-core` depends on no workspace crate. As
built:

```proto
message ChunkFileHeader { string data_file_path = 1; uint64 data_file_size = 2; bytes footer_digest = 3; repeated ChunkSpan chunks = 4; }
message ChunkSpan { uint64 data_offset = 1; uint64 length = 2; }
```

Comments describe the contract, not internals. Enum values carry their
enum's name as a prefix (`PRESENCE_NULL`, never `NULL`): value names share
one scope per package, and a bare `NULL` is a macro in C and C++ clients. A
request that omits its key, or sends a `Key` with no variant set, is an
INVALID_ARGUMENT error. Generated code is included in one inline module
carrying the scoped allows (RFD 015). The build script uses a vendored
protobuf compiler, so building from source needs none on the build host
(RFD 012). The crate gets round-trip tests per message, a wire test for the
bytes key variant, and an integer test per enum value.
Health is `grpc.health.v1` from the tonic health crate, not our proto.

## 5. The binary and the testkit

**Local directory.** Optional (`storage.local`). Chunk files under
`index/`, named by a digest of the data file's path, each carrying a
`ChunkFileHeader` inside the RFD 011 envelope; the page cache's disk level
under `cache/`, handed to `PageCache` as a `DiskTier`, or none when
`page_cache_disk_bytes` is zero; any other size is at least one block,
`contract::PAGE_CACHE_DISK_BLOCK_BYTES`, which `Config::parse` checks.
Disk residency reads pages with positioned reads on a blocking pool, never
mmap, so every local read is counted and no page fault stalls a worker.
Startup reuses chunk files whose identity matches; a generation swap
deletes chunk files nothing references; a disk budget refuses loads that
would exceed it, at the footer stage.

**`icetray serve`.** `serve` takes only the configuration path, from
`--config` or `ICETRAY_CONFIG` (RFD 012); TOML or JSON. The binary uses
jemalloc, and `serve` turns on background purging at start so that memory
freed after a load (shed key chunks, dropped generations) returns to the
operating system, with a `warn` log where the platform refuses (purging is
jemalloc's Linux feature); memory metrics report the index's live bytes
beside process RSS. `Serving::start` binds the gRPC listener and the metrics
listener first (the metrics listener serves `/metrics` and `/ready` from
one HTTP/1 accept loop, which only the shutdown ends: a failed `accept` is
logged and retried), registers the gRPC health service with the overall
server (`""`) and the `Icetray` service both `NOT_SERVING`, opens every
table through `core::open` (nothing read yet), starts the gRPC and HTTP
servers, logs `listening`, and returns; the loads run behind it, every table
concurrently, so the listeners accept while tables load. There is no
per-table health entry: readiness is a fact about the first loads. Once
every table serves a generation, the daemon calls `Opened::retain` once,
samples its gauges, flips both health names to `SERVING` together, and
`/ready` answers 200; both stay so, since a later refresh failure keeps the
last generation serving (RFD 005), sets `Degraded`, and counts
`icetray_refresh_errors_total{table}`. A table's first-load failure is fatal
to the process (RFD 005: refuse to start): the reason is logged, health
stays `NOT_SERVING` for `START_FAILURE_GRACE` (10 seconds, long enough for
an orchestrator's probe to see the refusal beside its logged reason; tests
pass a shorter grace through `ServeOptions`), then the run ends and the
process exits 1. Refresh runs as one task per table on
`server.poll_interval`, the one period for everything periodic, with the
sampler for `generation_age_seconds` and `process_rss_bytes` as one more
task on the same period; the tickers start one interval after the first
loads rather than at once, so a load is not followed by an immediate,
redundant poll and sample, and a missed tick delays rather than bursts. A
refresh that swapped a generation in is followed by `Opened::retain`.
`serve` logs JSON to stdout at `info` (the twelve-factor log stream)
through the binary's own `tracing` event formatter: keys in the order
time, level, target, message, the event's other fields, the static fields;
the standard fields under the names `LogConfig.fields` maps them to, every
line carrying the message key (empty for an event without one), and unique
keys, an event's own field winning over a standard one and never displaced
by a static field; a `log.fields` key naming no standard field, or two
standard fields mapped to one emitted name, is refused at startup with exit
64. Prometheus histograms use the prototype's buckets (100 µs to 2 s); the
recorder installs once per process and every later caller gets the same
handle. SIGTERM or SIGINT drains in-flight requests, flushes the page
cache's disk level (RFD 017), and exits 0; a shutdown asked for while the
first loads run is honoured as soon as they end, cutting the grace short
and never flipping to `SERVING`.

**One-shot subcommands.** `doctor`, `get <key>...`, and `info` each take
the target vocabulary after the subcommand: `--config <file>` (also
`ICETRAY_CONFIG`) or `--location <root or metadata file>` (exactly one, a
clap group), `--rewrite FROM=TO` under either, and four location-only flags
(`--key-column`, `--value-column`, `--ref`, `--residency`) that conflict
with `--config`, since clap waives a `requires` whose argument conflicts
with a present one. A `--location` table is opened under the name `table`,
never strict (the one-shots exist to report what `strict` would refuse),
with defaults `key`, `value`, `main`, `full`. The table filter is a
`--table` flag on all three (a positional before `get`'s keys would make a
key and a table name indistinguishable); `get` needs it when more than one
table is configured. Keys are typed in the form `Key`'s `Display` prints
them (a long's digits, negative ones included; a string's text without
`Display`'s quotes; hex with or without `0x`, either case, even length; a
uuid hyphenated or simple), parsed under the served generation's key type
before any lookup. Every one-shot opens through `core::open` and loads
concurrently; none calls `Opened::retain`, which is the daemon's. JSON
lines with `kind` and `table` first: `table` (`info`) and `doctor` lines
flatten `TableSummary`, so a new measurement in core reaches the CLI
without a CLI change; `finding` and `file` lines flatten core's `Finding`
and `FileReport`; a `get` line carries the key, `presence`
(`present`/`null`/`absent`), the value as standard base64 and, when UTF-8,
as text, `value_bytes`, `snapshot_id`, the GETs the lookup issued, and
`elapsed_micros`; a table the doctor could not load is one `refused` line
(`table`, `reason` as the `ErrorKind`'s name, `message`). The doctor opens
every table with `strict` off. Exit codes: 0 for a run that did what was
asked; 1 for a load or read error, a table the doctor could not load (which
dominates 2), a configuration file that cannot be read or parsed, a
first-load failure, or a bind failure; 2 when the doctor found a cost
finding (RFD 016); 64 (`sysexits.h`'s `EX_USAGE`) when the command line or
the target could not be used, including clap's own errors, no table
matched, a table name required, a key that does not parse, and a bad log
field mapping, so clap's default exit code 2 is never used and 2 means one
thing. A reader that closes stdout ends the run with exit 0 and nothing
more printed. The one-shots log to stderr at `warn`, since their stdout is
data.

**Testkit.** `icetray-testkit` is `error` (`TestkitError`, one variant per
failure source; `SpecFault { PadOnFixedWidthKey, CatalogBehind }` for a
specification the writer refuses; `Precondition { OracleNotFull,
CacheNotCold, GoldenUnset, RestUriUnset }` for a helper called in a state
it cannot measure; `required_var`, the one place a gate's variable is
read), `keys` (`key_at`, `key_at_padded`, `absent_key_from`,
`absent_key_near`, `far_key`, `value_for`, `value_for_len`; `KEY_STEP`,
`VALUE_BYTES`, `STRING_KEY_FIXED_BYTES`; keys of all four types, uuid as
sixteen bytes with the big-endian value in the tail, and values a function
of the key), `layout` (`Layout` with `page_rows`, `page_bytes`,
`Dictionary`, `column_index_truncate`, `Statistics`, `Bloom`,
`row_group_rows`, `target_file_bytes`, and `Layout::contract()` built from
`contract`'s constants; `KEY_BLOOM_FPP`, `KEY_COLUMN`, `VALUE_COLUMN`),
`fixture` (`FixtureSpec` with `small` and `interleaved`, `Order`, `Nulls`,
`Fixture` with its accessors, `files`, `keys_at`, and the sources
`catalog_source`, `root_source`, `pinned_source`; `RawFile::from_batch`;
`HandSnapshot`; `write_fixture`, `write_table_of_types`,
`position_delete_file`, `memory_catalog_local`, `memory_catalog_s3`,
`local_catalog`, `residue_class`; the constants `SMALL_IDS`,
`SMALL_BUCKETS`, `SAMPLE`, `INTERLEAVED_IDS`, `INTERLEAVE_STRIDE`,
`INTERLEAVED_BUCKETS`, `DATA_DIR`), `s3` (`S3Fixture`, whose
`storage_config` is hedge-disabled and whose `file_io_props` is a
`HashMap`), `sim` (`serve_s3`, `serve_s3_with`, `SlowEvery`,
`sim_s3_store`, `SIM_S3_PORT`), `verify` (`Served` with `load` and
`current`; `build`, `serve_over`, `serve`, `serve_catalog`,
`serve_catalog_with`; `expected_cold_gets`, `expected_key_pages`,
`predicted_probe`; `verify_hits`, `verify_hit_cost`, `verify_misses`;
`storage_config_for_tests`, `count`, `TEST_PAGE_CACHE_BYTES`), and
`golden` (`WAREHOUSE_VAR`, `warehouse`, `GoldenTable`, `table_config`,
`config`, `long_key`, `string_key`, `permuted_key`, `value`,
`wide_value`). The constants `PAGE_ROWS`, `VALUE_PAGE_BYTES`,
`TARGET_FILE_BYTES_MAX`, `KEY_BYTES_ADVICE`, `MAX_KEY_BYTES`, and
`PAGE_CACHE_DISK_BLOCK_BYTES` are imported from `contract` and never
redeclared; the golden key derivations delegate to `keys`; `count` is the
crate's one `usize` to `u64` conversion. Everything is test-only, under
`[dev-dependencies]` of the crates that use it.

The fixture writer commits through the Iceberg library's `ClusteredWriter`
over the rolling data-file writer under `Layout::properties`, grouping ids
by the library's own bucket transform. Raw files (`RawFile`, committed
through `fast_append`, which admits any data file) produce what the writer
cannot: a key column whose Parquet type contradicts the schema, a null
key, and the two unsupported-type tables. By-hand snapshots
(`Fixture::commit_by_hand`) write a data manifest, a delete manifest when
there are delete files, the manifest list, and the metadata at
`catalog::version_file(root, n)` under `Operation::Overwrite`, then the
hint at `catalog::hint_location(root)`: a snapshot carrying a delete file
and a snapshot dropping a data file, neither of which a fast append can
commit. A table that has committed by hand is followed through its root
and refuses further catalog commits as `SpecFault::CatalogBehind`.
`Fixture::write_hint` writes the current metadata location as the hint's
whole content; the testkit owns hint writing, core owns the hint's
location. `Statistics::Truncated(n)` cuts the key column's row-group
statistics, and because iceberg-rust 0.10.1 records a manifest bound only
from an exact Parquet statistic, the manifest then carries no key bounds
at all rather than shortened ones; the shortened bounds Iceberg's Java
writer records under its default metrics mode are covered only by the
golden `default_layout_1m` table. The evolved partition spec is proved at
unit tier on core's hand-built metadata (`generation::testing::Table::
evolve`), since iceberg-rust's transaction API cannot evolve a spec; the
testkit writes no such fixture.

`Served` is one table over its own storage and cache: `build` leaves it
unloaded, for a test that asserts on a refused table's state or needs a
cache of another size; `serve_over` and `serve` load it; `serve_catalog`
opens a fixture through its catalog over hedge-free storage. The cost
oracle is `Generation::plan` through `verify::expected_cold_gets` (the
distinct value pages a cold Full-tier batch fetches; a key the plan names
no value page for costs nothing in the Full tier and is
`Precondition::OracleNotFull` in any other), `expected_key_pages` (the
distinct key pages a Bloom-tier cold batch adds), and `predicted_probe`
(whether a miss pays a key-page GET in the Bloom tier or a local read in
the Disk tier). `verify_hit_cost` requires a cold cache and enforces it
(`Precondition::CacheNotCold`), asserts the cold batch costs exactly the
oracle's count and the repeat nothing. `Generation::plan` names a
Bloom-tier key's first probe alone, so it cannot price a cold Bloom hit
across an overlap group; the Bloom acceptance tests walk `candidates` and
`page_of` up to the holder the Full plan names. In the Bloom tier a refresh
that widens an overlap group costs, beyond the Full tier's GETs, one filter
per row group of every new file and one chunk per row group of every member
of the widened group, new files included: the tier keeps no chunks, and
`Generation::assemble` gathers `key_chunks_for_verification` for every
member of a group with a non-reused file. Every P-tier test serves under
`storage_config_for_tests` or `S3Fixture::storage_config`, both
hedge-disabled; only `tests/sim.rs` enables hedging.

The acceptance suite is `tests/lookup.rs`, `lookup_s3.rs`, `refresh.rs`,
`refusals.rs`, `findings.rs`, `residency_bloom.rs`, `residency_disk.rs`
(P), `sim.rs` (S), `golden.rs` (G), `rest_catalog.rs` (C), and the
`scale.rs` probe. One fixture per `RefusalReason` variant: descending
order for `Unsorted`; a raw nullable key column for `NullKeys`; an
overlapping append for `DuplicateKeys`; a raw `Utf8` key column on a
long-keyed table for `KeyTypeMismatch`; `write_table_of_types` with an
`Int` key for `KeyTypeUnsupported` and a `Long` value for
`ValueTypeUnsupported`; a by-hand snapshot listing a position-delete file
for `DeleteFiles`; a `key_pad` over `MAX_KEY_BYTES` for `KeyTooLong`; a
one-byte budget for `OverBudget`; `Bloom::Off` under `Residency::Bloom`
for `NoBloomFilter`; `Residency::Disk` without a local directory for
`NoLocalDir`. One fixture per `FindingCode`: `Dictionary::Value`;
`page_rows` at twice `KEY_PAGE_ROWS_COST` with 24-byte values; `buckets:
0` rolled into twenty 64 KiB files; the interleaved fixture;
`Statistics::Truncated`; `column_index_truncate` under a shared 64-byte
prefix; 16 KiB values under the row bound; a padded string key; 9,000 rows
of 16 KiB in one file; 48,000 ids at a 40 KiB target; `Bloom::Off`;
`Bloom::Fpp(1e-12)`; `Bloom::Fpp(0.5)`. Each set is bound to its enum by
an exhaustive match, and the advice a layout cannot avoid is asserted
beside the designed code. The gated tests carry `#[ignore = "<reason>"]`
naming their variable and script, so a plain `cargo test` reports them as
ignored, never as passed; forced without the variable they fail on
`Precondition::GoldenUnset` or `Precondition::RestUriUnset`.
`dev/test-int.sh` runs `rest_catalog` with `-- --ignored` once the catalog
answers; `dev/test-golden.sh` refuses without `ICETRAY_GOLDEN_WAREHOUSE`,
or without a `file://` warehouse's `db/` directory, then runs `golden`
with `-- --ignored`.

## 6. Invariants and the tier that proves each

Tiers (RFD 014): U unit; P in-process (in-memory store or in-process S3, GET
counter as oracle); S deterministic simulation (round trips as oracle); C
container-gated (visible skip); G golden tables written by Spark (visible
skip).

| invariant | tier |
|---|---|
| Bucket routing equals the Iceberg library's transform for long, string, binary, uuid, and fixed keys | U (oracle test against the library, all five types) |
| Key ordering equals Parquet's for each type; a key of another type than the table's is refused at the Table boundary with Error::KeyType | U, P |
| A cold hit costs exactly one GET; a warm hit none; a miss none, including misses inside a file's range | P, S |
| Load costs two GETs per file plus the metadata reads; a load with `previous` reuses every unchanged file by pointer | P |
| An unchanged poll costs one read (hint file or catalog record) | P, C |
| A refresh verifies only overlap groups that gained a file; a disjoint append verifies zero groups | P (counting) |
| A duplicate key across files is refused and the previous generation keeps serving | P |
| A delete file in the followed snapshot is refused with reason `delete_files` | P (fixture with a delete file) |
| A file without column statistics and several row groups never yields a false miss | U (writer with statistics disabled), P (`findings.rs`: a footer per file and a chunk per row group on load, an absent key inside the range costs no GET) |
| Layout follows the snapshot's schema and each manifest's spec; an evolved bucket count routes correctly | P (or U on hand-built metadata if the writer cannot evolve a spec) |
| Overlapping files without duplicates serve correctly and are priced as files probed | P |
| Unpartitioned tables serve correctly and are priced | P |
| A dictionary-encoded value column costs one extra GET; a dictionary-encoded key column costs none in the Full tier, one more local read per find in the Disk tier, and in the Bloom tier one more GET on the first positive in a row group and none after | U, P for the value column (`findings.rs`: one dictionary page per file touched on a cold batch, none warm) |
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
snapshot, 0 for the previous; a replaced snapshot's series reads 0 until the
process restarts, since the exporter's only series removal is an idle
timeout, which would also drop the live gauges `table` sets only on change,
so the recorder installs none), `icetray_table_state{table,state}`.

Every label value is the `Display` of an enum, never a literal string at the
call site: `kind` from a read-kind enum, `result` from `LookupResult`, `call`
from the RPC enum, `reason` from `ErrorKind`, whose `Display` is the
`RefusalReason`'s name for `Error::Refused`, `inefficient` for the strict
gate, and the variant's snake_case name otherwise, `code` from `FindingCode`,
`state` from `TableState`.

Ownership: `table` emits `lookups_total`, `lookup_seconds`,
`refresh_errors_total`, `generation_swaps_total`, `index_bytes`, `findings`,
`generation_info`, and `table_state`; `cache` the two page-cache counters and
the page-cache bytes gauge; `storage` the GET, byte, hedge, and local-read
series; the binary samples `generation_age_seconds` (from
`Generation::snapshot_timestamp` against now) and `process_rss_bytes` once
after the first loads and then every `poll_interval`, running the
recorder's upkeep with each sample, and counts `requests_total`. `icetray_findings{table,code}` describes the serving
generation: it is set for every `FindingCode` on every admitted generation,
zero for the absent codes, and never from a generation the strict gate
refused. `refresh_errors_total` is counted only from `refresh`, once per
failed poll; a first-load failure is the caller's to report.

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
