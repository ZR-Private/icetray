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
| `snapshot` | `SnapshotId`, the newtype for an Iceberg snapshot id; it has no order and is a metric label, never a gauge value | 015 |
| `approx` | integer-to-float conversion (`ratio`, `gauge`, `gauge_len`, and `signed_gauge` for a sequence number), the conversions between an in-memory `usize` and a `u64` total (`count` widens, `length` narrows; both exact, since a compile-time assertion admits only 64-bit targets), and a footer number as a measurement (`measured`, in-crate); the only `cast_precision_loss` allow | 015 |
| `time` | typed time conversions: the one place a Duration or timestamp becomes a number or a string (`seconds`, `micros`, `age_seconds`, `log_timestamp`), the wall clock is read (`now`), an epoch millisecond becomes a `jiff::Timestamp` (`timestamp_from_millis`, in-crate; out of jiff's range is `TableFault::SnapshotTimestampOutOfRange`), and a `Duration` is added to an epoch millisecond (`millis_after`, for a fixture writer stamping a commit after the metadata's last update); CONTRIBUTING Constants | |
| `metric` | the metric and label names of section 7 as constants, and the label-value enums `ReadKind` and `LookupResult`; the one home for those names | 012 |
| `key` | `Key`, `KeyType`, key ordering (numeric for long, bytewise for string and binary), key/statistics comparison | 008 |
| `bucket` | the Iceberg bucket transform (murmur3 over the key's bytes) | 008 |
| `contract` | the serving layout's numbers as named constants (page rows, value page byte bound, the page cache's disk block, file size target, key length cap and advice); a shared constant that is not a layout number lives with what it bounds (`key`, `check::thresholds`, `config`) | 002, 008 |
| `config` | the configuration schema and its validation; TOML and JSON parsing; defaults | 011 |
| `storage` | every byte read from object storage: store per prefix, location rewriting, request counting, credential chain with cached refresh, hedging | 003, 013 |
| `local` | the optional local directory: chunk files for disk residency (naming by data-file identity, reuse on restart, deletion with the generation), a disk budget, local-read counting, and the `DiskTier` handle `cache` uses for its disk level. A directory module: `local/mod.rs` holds the public surface and `LocalDir`, `local/format.rs` the chunk file format (envelope, header, checks, writer), `local/disk.rs` the read side (`DiskChunks`, `DiskTier`), and `local/testing.rs` the fixtures the three files' tests share | 017, 012 |
| `catalog` | where the current metadata comes from (static root with version hint, metadata file, any catalog incl. REST); one-read change detection | 005 |
| `manifest` | snapshot to live data files; table layout (key and value columns, bucket transform) derived from the snapshot's schema and each manifest's partition spec; delete-file refusal | 005, 009 |
| `parquet_io` | Parquet byte access: one-GET footer load, `Segments` (in-memory `ChunkReader` that never fetches) | 004, 006 |
| `index` | the per-file resident index: key chunks, fences, exact per-row-group ranges, `find` (no I/O), `read_value` (one page through the cache). A directory module by pipeline stage: `index/mod.rs` holds the public surface and the lookup path, `index/inspect.rs` the footer stage, `index/build.rs` the chunk stage, and `index/testing.rs` the fixtures the three stages' tests share | 004, 006 |
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
and private items are free, and a `pub` item only its own crate calls is not
listed; changing a signature below means updating this document first.

```rust
// error
// Core's `Error` carries iceberg, parquet, and object_store errors as variants, and `Resolved`
// exposes iceberg's `TableMetadata`, because core has no consumer outside this workspace, which
// versions its crates together, and the binary matches only on the variants; a consumer of core
// outside the workspace is what changes that, and library types then come off the public surface.
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
    /// A catalog call ran past `storage.catalog_timeout` (RFD 013); `table` is the
    /// catalog identifier, as for a fault raised while the source resolves.
    CatalogTimeout { table: String, after: Duration },
    /// A first load or refresh ran past `server.refresh_deadline` and was dropped (RFD 013).
    RefreshDeadline { table: String, after: Duration },
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
/// `file`, `not_ready`, `local`, `catalog_timeout`, `refresh_deadline`). Also what `TableState::Degraded` carries as its reason:
/// `Error` is not `Clone`, so the failure goes to the caller and its kind to the state.
/// Re-exported from `lib.rs`.
pub enum ErrorKind { Iceberg, Parquet, ObjectStore, ObjectStorePath, Json, Config, Cache, Refused(RefusalReason), Inefficient, KeyType, Table, File, NotReady, Local, CatalogTimeout, RefreshDeadline }

/// Named refusals (RFD 007). Display gives the snake_case name used in logs;
/// the fields name the file, row group, type, key, or bytes a reader has to
/// act on, and `Error::Refused`'s message carries them. Clone, not Copy.
/// `DuplicateKeys` names the file the key appears in again: the same file
/// for a duplicate within one file, the later file of a bucket otherwise.
/// `ValueTypeUnsupported` is raised from the snapshot's schema by
/// `TableLayout::derive`, so it names no file and carries the Iceberg type's
/// `Display` (the type does not implement `Hash`); a data file whose physical
/// type contradicts an admitted schema type is `FileFault::ValuePhysicalType`.
pub enum RefusalReason { Unsorted { path: String, row_group: usize }, NullKeys { path: String, row_group: usize }, DuplicateKeys { path: String, key: Key }, KeyTypeMismatch { path: String, expected: KeyType, got: parquet::basic::Type }, KeyTypeUnsupported, ValueTypeUnsupported { got: String }, DeleteFiles, KeyTooLong { path: String, bytes: usize }, OverBudget { needed: u64, budget: u64 }, NoBloomFilter { path: String, row_group: usize } }

// snapshot
/// A snapshot id is not an integer; it is never compared with one. Not a gauge value (an `f64`
/// loses its low digits) nor a metric label (a series per swap); logs and `TableInfo` carry it.
pub struct SnapshotId(pub i64);
/// A snapshot's sequence number, Iceberg's count of the table's commits; a newtype beside the id,
/// and a gauge value (`generation_sequence_number`), exact in an `f64` up to 2^53 commits.
pub struct SequenceNumber(pub i64);

// metric
/// Metric label values for lookups.
pub enum LookupResult { ColdHit, WarmHit, Miss }

// key
/// The width of an Iceberg uuid and of the FIXED_LEN_BYTE_ARRAY Parquet stores it in.
pub const UUID_BYTES: usize = 16;
/// Hex digits per byte in every hex rendering of bytes (key text, chunk file names, the
/// binary's key parser); the one home for the width.
pub const HEX_DIGITS_PER_BYTE: usize = 2;
pub enum KeyType { Long, String, Binary, Uuid }
impl KeyType {
    /// Every key type in declaration order; a test compares it to an exhaustive successor match.
    pub const ALL: [Self; 4];
    /// long -> Long; string -> String; binary and fixed -> Binary; uuid -> Uuid.
    /// Every other type is `None`; the caller that owns the table name turns it
    /// into `Error::Refused { reason: RefusalReason::KeyTypeUnsupported }` (RFD 008).
    pub fn from_iceberg(ty: &iceberg::spec::Type) -> Option<Self>;
    /// Whether every key of the type has one width: `Long` and `Uuid`; exhaustive, so a
    /// new key type states its width here before it compiles. The fixture writer
    /// consults it to refuse padding a fixed-width key.
    pub fn is_fixed_width(self) -> bool;
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
/// Residency when a table says nothing, and the command line's `--residency` default; the
/// one home for it.
pub const DEFAULT_RESIDENCY: Residency = Residency::Full;
pub struct Config { pub server: ServerConfig, pub storage: StorageConfig, pub tables: Vec<TableConfig> }
impl Config {
    /// TOML or JSON, decided by content (a leading `{` is JSON).
    pub fn parse(text: &str) -> Result<Self>;
    pub fn from_path(path: &std::path::Path) -> Result<Self>;
    /// `from_path` without `validated`: the file must parse, and the rules wait for the caller,
    /// which adds settings of its own first (the command line's `--rewrite` rules, ahead of the file's).
    pub fn from_path_unvalidated(path: &std::path::Path) -> Result<Self>;
    /// Every rule `parse` checks, applied to a configuration built or completed in code (the
    /// command line's `--location` table, or a file with the `--rewrite` rules added); the first
    /// broken rule is `ConfigError::Invalid`.
    pub fn validated(self) -> Result<Self>;
}
/// `metrics_listen` is required (default 127.0.0.1:9433) and serves `/metrics`
/// and `/ready`; RFD 012 makes both part of the surface.
/// `page_cache_bytes`: at least `cache::min_memory_bytes()`, one `contract::VALUE_PAGE_BYTES`
/// page with its entry overhead for each of the cache's pinned `MEMORY_SHARDS` (foyer splits the
/// budget per shard and a shard keeps its latest page whatever its share, so less bounds nothing);
/// `Config::parse` refuses less as `PageCacheTooSmall`, as it refuses a disk level below one block.
/// The live-path bounds (RFD 013) the binary's service and shutdown read: `get_deadline`
/// (default 5 s) and `multi_get_deadline` (15 s, flat), a client's own deadline being the
/// gRPC layer's, `max_in_flight` (1024) requests at once with no queue, `max_multi_get_keys`
/// (1000), `drain_deadline` (20 s); and the background path's `refresh_deadline` (5 min) on a
/// first load or refresh. `open_with` hands `Table::new` both `refresh_deadline` and
/// `get_deadline`, the bound on every lookup's page fetch. Every duration is a friendly duration; each is above zero, as are
/// the two limits (`ConfigViolation::*Zero`).
pub struct ServerConfig { pub listen: SocketAddr, pub metrics_listen: SocketAddr, pub poll_interval: Duration /* "10s" in the file; from 1 s to 24 h */, pub page_cache_bytes: usize, pub log: LogConfig, pub get_deadline: Duration, pub multi_get_deadline: Duration, pub max_in_flight: usize, pub max_multi_get_keys: usize, pub refresh_deadline: Duration, pub drain_deadline: Duration }
/// Logs are JSON only (RFD 012 rejected a text format); `fields` renames the
/// standard fields and `static_fields` are added to every event. A `fields` key naming no
/// standard field fails the parse (serde's own error, `ConfigError::Toml` or `Json`);
/// `Config::validated` refuses two standard fields emitted under one name
/// (`ConfigViolation::LogFieldCollision`, the first pair in `StandardField::ALL` order).
pub struct LogConfig { pub fields: BTreeMap<StandardField, String>, pub static_fields: BTreeMap<String, String> }
/// The mapped name, or the field's own; the binary's formatter and the collision check both read it.
impl LogConfig { pub fn emitted_name(&self, field: StandardField) -> &str; }
/// The standard fields of every log event (RFD 012). Display and serde give the file's lowercase
/// name (`time`, `level`, `message`, `target`), which is also the default emitted name.
pub enum StandardField { Time, Level, Message, Target }
impl StandardField { pub(crate) const ALL: [Self; 4]; pub const fn name(self) -> &'static str; }
pub struct HedgeConfig { pub enabled: bool, pub percentile: f64, pub window: usize, pub max_rate: f64 }
pub struct S3Config { pub endpoint: Option<String>, pub region: Option<String>, pub access_key_id: Option<String>, pub secret_access_key: Option<String>, pub allow_http: bool, pub path_style: bool }
pub struct RewriteRule { pub from: String, pub to: String }
pub enum ConfigError { Read(std::io::Error), Toml(toml::de::Error), Json(serde_json::Error), Invalid(ConfigViolation) }
/// The two location variants are raised by `Storage::locate`, after rewriting, not by `Config::parse`.
/// `UnknownResidency` is raised by `Residency`'s `FromStr`, which the command line reads the names it
/// offers back through; clap refuses any other text before it gets there.
pub enum ConfigViolation { EmptyTableName, DuplicateTableName(String), IncompleteStaticKeys, BudgetWithoutResidency, DiskResidencyWithoutLocalDir, PageCacheTooSmall { bytes: u64, minimum: u64 }, PageCacheDiskTooSmall { bytes: u64, minimum: u64 }, HedgePercentileOutOfRange { percentile: f64 }, HedgeMaxRateOutOfRange { max_rate: f64 }, HedgeWindowEmpty, PollIntervalTooShort { interval: Duration, minimum: Duration }, PollIntervalTooLong { interval: Duration, maximum: Duration }, GetDeadlineZero, MultiGetDeadlineZero, MaxInFlightZero, MaxMultiGetKeysZero, RefreshDeadlineZero, DrainDeadlineZero, ConnectTimeoutZero, CatalogTimeoutZero, LogFieldCollision { name: String, first: StandardField, second: StandardField }, EmptyRewritePrefix, NoStoreForLocation { location: String }, LocationNotAbsolute { location: String }, UnknownResidency { text: String } }
pub struct TableConfig { pub name: String, pub source: SourceConfig, pub r#ref: String, pub key_column: String, pub value_column: String, pub strict: bool /* default true */, pub residency: Residency /* default Full */, pub resident_budget_bytes: Option<u64> }
/// Where a table keeps its key chunks (RFD 017), ranked by lookup speed. Never chosen automatically.
/// Display gives the file's lowercase name, for the doctor's output and metric labels.
/// `name` is the lowercase name `Display` prints and the file spells.
pub enum Residency { Full, Disk, Bloom }
impl Residency { pub const fn name(self) -> &'static str; }
/// Exactly the lowercase names `Display` prints (`full`, `disk`, `bloom`); any other word is
/// `ConfigViolation::UnknownResidency`. The one boundary parser for the setting when it arrives
/// from the command line; the file reads the same names through serde.
impl std::str::FromStr for Residency { type Err = ConfigViolation; }
/// `page_cache_disk_bytes`: zero means no disk level, anything else at least
/// `contract::PAGE_CACHE_DISK_BLOCK_BYTES`, the level's one block, or the
/// level would hold nothing; `Config::parse` refuses less as `PageCacheDiskTooSmall`.
pub struct LocalConfig { pub dir: PathBuf, pub index_budget_bytes: Option<u64>, pub page_cache_disk_bytes: u64 }
/// `max_retries` (default 2, zero allowed) and `connect_timeout` (1 s) for the S3 clients,
/// `catalog_timeout` (30 s) for each catalog call, read through `Storage::catalog_timeout`;
/// both durations are friendly durations above zero.
pub struct StorageConfig { pub s3: Option<S3Config>, pub rewrite: Vec<RewriteRule>, pub hedge: HedgeConfig, pub local: Option<LocalConfig>, pub max_retries: usize, pub connect_timeout: Duration, pub catalog_timeout: Duration }
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
    /// `LocalConfig::index_budget_bytes`; a crate-internal `reserve` checks
    /// them with every outstanding reservation and holds a load's bytes
    /// until it has written (RFD 017).
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
    /// `storage.catalog_timeout`, which bounds each catalog call `TableSource` makes; the
    /// catalog client never reads through `Storage`, but every resolve has one in hand.
    pub fn catalog_timeout(&self) -> Duration;
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
/// negative value where the spec puts the key's bucket; `BucketValueOutOfRange` a bucket at or
/// past the spec's count, which no key hashes to. `SnapshotTimestampOutOfRange` is a
/// snapshot timestamp jiff cannot represent, raised by `Generation::load` before any read.
pub enum TableFault { RefNotFound { ref_name: String }, SchemaMissing { schema_id: i32 }, SpecMissing { spec_id: i32 }, ColumnMissing { column: String }, ZeroBuckets { spec_id: i32 }, BucketValueMissing { path: String, spec_id: i32 }, BucketValueOutOfRange { path: String, spec_id: i32, bucket: u32, count: u32 }, VersionHintEmpty { root: String }, VersionHintMalformed { root: String, hint: String }, MetadataLocationUnknown, SnapshotTimestampOutOfRange { snapshot_id: SnapshotId } }
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
    /// the client parses either way; a pinned metadata file reads nothing. With
    /// no `known` location nothing is unchanged, which is how `resolve` reads.
    /// A catalog load runs under `tokio::time::timeout` of `storage.catalog_timeout()`
    /// and past it is `Error::CatalogTimeout`.
    pub async fn resolve_if_changed(&self, storage: &Storage, known: Option<&str>) -> Result<Option<Resolved>>;
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
/// non-negative integer at the bucket field is `TableFault::BucketValueMissing`,
/// one at or past the spec's count `TableFault::BucketValueOutOfRange`.
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
    /// One page on the lookup path and where it came from: through `cache` when given,
    /// keyed by file id and `range.start`, so concurrent lookups for one page share one
    /// fetch; that fetch, and every read without a cache, is one hedged lookup GET, so a
    /// read without a cache is always `Fetched`.
    pub async fn read_page(self: &Arc<Self>, range: Range<u64>, cache: Option<&PageCache>) -> Result<(Bytes, PageOrigin)>;
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
/// or what it says (a column chunk running past the file's last byte among
/// them), a key or value page holding other rows than its offset index gives
/// it. `UnplaceableBloomFilter` is a filter the footer places nowhere the
/// file holds, its end past the file's last byte included.
/// `MalformedPageHeader` and `PageHeaderOverBound` are a key page header,
/// read ahead of parquet with the index's own compact-thrift reader, that
/// parquet would not read to the same numbers or that declares more than
/// its page can hold: an uncompressed size above its rows times the key
/// cap and an encoding allowance, a dictionary of more values than its row
/// group's rows, a compressed size past its place in the offset index, v2
/// level lengths that are negative or whose sum, which parquet adds as
/// `i32`s, is above the page's uncompressed size.
pub enum FileFault { MissingColumn(String), ValuePhysicalType { got: parquet::basic::Type }, MissingPageIndex, NoRows, InconsistentRowCounts, InconsistentOffsetIndex { row_group: usize }, PageRowsMismatch { row_group: usize, page: usize }, MalformedPageHeader { row_group: usize, page: ChunkPage, fault: PageHeaderFault }, PageHeaderOverBound { row_group: usize, page: ChunkPage, field: HeaderField, declared: u64, allowed: u64 }, PageOutOfRange { row: i64, row_group: usize }, CorruptBloomFilter { row_group: usize }, UnplaceableBloomFilter { row_group: usize }, KeyNotUtf8 { row_group: usize } }
/// A page of a column chunk as a fault names it: the dictionary page, or a
/// data page by its position in the offset index.
pub enum ChunkPage { Dictionary, Data(usize) }
/// The `PageHeader` fields a page-header fault names; `LevelsByteLength` is
/// the sum of a v2 header's two level lengths.
pub enum HeaderField { PageType, UncompressedPageSize, CompressedPageSize, DataPageHeader, DataPageHeaderV2, DictionaryPageHeader, NumValues, DefinitionLevelsByteLength, RepetitionLevelsByteLength, LevelsByteLength }
/// Why a key page header is not one parquet reads to the pre-scan's numbers:
/// truncated, a varint past 64 bits or an integer past its field's width, a
/// field id overflow, an unknown wire type, a field parquet reads sent as
/// another wire type, a set, map, or boolean list parquet cannot skip in
/// step, nesting past parquet's skip depth, a missing or negative field, an
/// unknown page type, or a page type its place does not hold.
pub enum PageHeaderFault { Truncated, VarintTooLong, OutOfRange, FieldIdOverflow, UnknownWireType(u8), WireTypeMismatch { field: i16, wire: WireType }, Unskippable(WireType), TooDeep, Missing(HeaderField), Negative(HeaderField), UnknownPageType(i32), UnexpectedPageType(parquet::basic::PageType) }
/// The Thrift compact protocol's wire types, by their codes.
pub enum WireType { BoolTrue, BoolFalse, Byte, I16, I32, I64, Double, Binary, List, Set, Map, Struct }
/// Every tier's resident bytes for one file, from the footer alone (RFD 017).
/// `disk_bytes` is the whole chunk file the Disk tier writes (envelope, header, key chunks), the
/// figure `index_budget_bytes` is charged; `key_chunk_bytes` and `filter_bytes` are the parts the
/// Full and Bloom tiers hold beyond `disk_memory`.
pub struct ResidentBytes { pub full: u64, pub disk_memory: u64, pub disk_bytes: u64, pub bloom: Option<u64> /* None when a row group lacks a filter */, pub bloom_bits_per_key: Option<f64>, pub key_chunk_bytes: u64, pub filter_bytes: Option<u64> }
impl ResidentBytes {
    /// The memory a tier holds (`full`, `disk_memory`, `bloom`): the one mapping from a
    /// tier to its field. `None` only for `Bloom` when `bloom` is.
    pub fn in_tier(&self, residency: Residency) -> Option<u64>;
}
pub struct FileIndex;
impl FileIndex {
    /// Refuses (typed, under `table`) an unsorted key column, a null key, a
    /// key repeated within the file, a key physical type that does not match
    /// `key_type` (Binary accepts BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY), keys
    /// over the cap, and in the Bloom tier a row group without a key bloom
    /// filter; a value column whose physical type is neither BYTE_ARRAY nor
    /// FIXED_LEN_BYTE_ARRAY is `FileFault::ValuePhysicalType`. The Disk tier
    /// with no local directory is `Error::Config` with
    /// `ConfigViolation::DiskResidencyWithoutLocalDir`. Every tier
    /// streams the key chunks once for verification, checking every key page
    /// header against what its page can hold before parquet reads it, and
    /// every key page against its offset index; Full keeps the chunks in memory, Disk writes
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
    /// Every tier's resident bytes for the file `handle` opens, from the
    /// footer alone, the same from the footer as loaded and as kept: no other
    /// column's column index is counted either way, so the budget check at
    /// the footer stage predicts what `resident_bytes` reports. A key chunk
    /// past the file's size counts as zero (`inspect` refuses it) and a filter
    /// past it makes `bloom` `None` (a Bloom build refuses it); the totals
    /// saturate. `disk_bytes` is the chunk file for the handle's identity,
    /// header and key chunks, so the handle has loaded its footer.
    pub fn predicted_resident_bytes(metadata: &ParquetMetaData, key_leaf: usize, handle: &FileHandle) -> ResidentBytes;
    pub fn key_range(&self) -> (&Key, &Key);
    pub fn may_contain(&self, key: &Key) -> bool;
    /// Exact position. Full: decodes one resident key page, no I/O. Disk:
    /// one local read of that page. Bloom: filter check in memory, then on
    /// a positive one key-page GET through the cache. Row-group choice uses
    /// the exact fences, never statistics.
    pub async fn find(&self, key: &Key, cache: Option<&PageCache>) -> Result<Option<RowRef>>;
    /// The value at `row` and its value page's origin, through `fetch_pages`, which returns
    /// the page's own origin and not the dictionary page's: `Fetched` for a cold hit, `Cached`
    /// for a warm one. `None` is a null value, distinct from an absent key. `find` discards
    /// the key page's origin.
    pub async fn read_value(&self, row: RowRef, cache: Option<&PageCache>) -> Result<(Option<Bytes>, PageOrigin)>;
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
    /// a restart is not charged for what is on disk, else the chunk file it
    /// would write, header and key chunks (`predicted().disk_bytes`), the
    /// bytes `LocalDir::index_bytes_in_use` then counts for it. Reads the
    /// chunk file's header and no chunk.
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
/// `FetchDeadline` is a fetch that ran past its handle's fetch deadline and was dropped,
/// which every coalesced waiter receives, with nothing cached.
pub enum CacheError { Fetch(Arc<Error>), FetchDeadline { path: String, offset: u64, after: Duration }, Backend(#[from] foyer::Error) }
/// Where one call's page came from, returned beside the page by `PageCache::get_or_fetch`
/// and `FileHandle::read_page`. `Fetched` for a fetch the caller ran or joined, since it
/// waited on the GET; `Cached` for memory or the disk level, which cost none. Re-exported
/// from `lib.rs`.
pub enum PageOrigin { Fetched, Cached }
/// Clone: clones share one cache and one pair of counters, so every table
/// holds a clone of the process's one cache. A clone may carry a fetch deadline
/// (in-crate `with_fetch_deadline(self, Duration) -> Self`, which `Table::new` applies);
/// `new` builds one without.
pub struct PageCache;
impl PageCache {
    /// `disk` is the local level of RFD 017: a page evicted from memory is
    /// served from it with a local read and no GET, and it survives a restart.
    /// Async and fallible because opening the level opens its files and
    /// recovers what they hold; a failure is `CacheError::Backend`. A disk
    /// level that fails to read a page falls through to the fetch.
    pub async fn new(capacity_bytes: usize, disk: Option<DiskTier>) -> Result<Self>;
    /// The page at `key` and where it came from: memory, the disk level, or `fetch`.
    /// Concurrent callers for one key share one fetch and one disk read. The origin decides
    /// a lookup's `result` label per call, never by differencing the process-wide counters.
    /// foyer runs the fetch on its own task, so a caller that stops waiting does not end it;
    /// on a handle with a fetch deadline the fetch future is dropped at that deadline from its
    /// start, which cancels its request, and every waiter gets `CacheError::FetchDeadline`.
    pub async fn get_or_fetch<F, Fut>(&self, key: (Arc<str>, u64), fetch: F) -> Result<(Bytes, PageOrigin)>
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
    pub table: String, pub snapshot_id: SnapshotId, pub sequence_number: SequenceNumber, pub metadata_location: String,
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
    /// compares `local.index_budget()` with the bytes `index/` holds, every
    /// other load's outstanding reservation, and every new file's
    /// `Inspected::disk_bytes_needed`, reserving the last until the new
    /// indexes are built (the budget is shared by every table); Bloom takes
    /// none; over is
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
    /// One lookup and its `result` label: `Miss` for `Absent`; for a present key `ColdHit` when
    /// its value page was `Fetched` and `WarmHit` when `Cached`. Only the value page decides
    /// it, never a key-page GET in the Bloom tier or a chunk-file read in the Disk tier.
    /// `Table::get` is the public lookup.
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
/// is counted under `refresh_errors_total` once per poll. `load`, `refresh`, `get`,
/// `multi_get`, and `Opened::retain` return `Send` futures: the daemon runs them on a
/// multi-thread runtime, so `manifest::list_data_files` collects the manifest files into a
/// `Vec` before streaming them, `generation::gather_chunks` streams group positions by value,
/// and `multi_get` builds its lookups into a `Vec` before streaming them, since an
/// opaque `impl IntoIterator` fed to `stream::iter` and a closure taking a reference and
/// returning an `async` block each make every future up the call chain unprovably `Send`;
/// `table::tests::test_load_refresh_get_and_multi_get_are_spawnable` and
/// `open::tests::test_retain_is_spawnable` pin the property.
pub struct Table;
impl Table {
    /// Constructs in `Loading`; nothing is read until `load`. Sets `table_state` for the state.
    /// Every `load` and `refresh` runs under `refresh_deadline` once it holds the load lock;
    /// past it the work is dropped, which releases the lock, and the call is
    /// `Error::RefreshDeadline`, degrading the table as any failure does. The table holds
    /// `cache` bounded by `get_deadline`: every page fetch a lookup runs ends at it from the
    /// fetch's start (RFD 013), in a `multi_get` too; load reads bypass the cache.
    pub fn new(config: TableConfig, source: TableSource, storage: Arc<Storage>, cache: PageCache, local: Option<Arc<LocalDir>>, refresh_deadline: Duration, get_deadline: Duration) -> Self;
    /// First load. `Err` leaves the table `Degraded` with no generation and is not counted
    /// under `refresh_errors_total`: a first-load failure is the caller's to report (RFD 005).
    /// On a serving table, a full re-resolve and reload reusing every unchanged file.
    pub async fn load(&self) -> Result<()>;
    /// Serialized per table. `Ok(true)` when a new generation was swapped in, counting
    /// `generation_swaps_total`. With no current generation it does what `load` does (a table
    /// left `Degraded` with no generation recovers on the next poll). Otherwise
    /// `resolve_if_changed(&storage, Some(&known))`: `None` is `Ok(false)`; a snapshot whose id equals
    /// the current generation's records the new location and is `Ok(false)`; else the build,
    /// the gate, and the swap. `Err` leaves the current generation serving, sets `Degraded`,
    /// and counts `refresh_errors_total{table, reason}` with `reason` the `ErrorKind`'s Display.
    pub async fn refresh(&self) -> Result<bool>;
    pub fn name(&self) -> &str;
    pub fn config(&self) -> &TableConfig;
    pub fn state(&self) -> TableState;
    pub fn current(&self) -> Option<Arc<Generation>>;
    /// `current`, or `Error::NotReady`: the one refusal for a table with no generation to
    /// answer from, which `get`, the gRPC service, and the one-shot subcommands all raise.
    pub fn serving(&self) -> Result<Arc<Generation>>;
    /// `Error::NotReady` with no generation; `Error::KeyType` before any read. One
    /// `load_full`, one lookup through `Generation::lookup`, timed and counted under
    /// `lookups_total{table, result}` and `lookup_seconds{table, result}`.
    pub async fn get(&self, key: &Key) -> Result<Lookup>;
    /// One `load_full`; every key's type checked before any read (a mismatch anywhere fails
    /// the whole call and issues nothing); the lookups run concurrently against that one
    /// generation, a private `MULTI_GET_FAN_OUT` (32) at a time through `buffered`, so the
    /// values come back in key order and one batch runs at most that many lookups at once; each
    /// is counted as `get` counts; any `Err` fails the whole call (RFD 010).
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
// generation's findings reach only the logs and the error; `generation_sequence_number{table}`
// through `approx::signed_gauge` and `generation_swapped_at_seconds{table}` through
// `time::unix_seconds(time::now())`, one series each per table; `table_state{table, state}` 1 for the current state
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
/// Private fields: `cost` (crate-private) and `advice` (public, for a renderer's tests) are the only
/// constructors, so `penalty` is `Some` exactly for a `Cost`. Serializes under the getters' names.
pub struct Finding { tier: Tier, code: FindingCode, message: String, file: Option<String>, penalty: Option<Penalty>, property: Option<WriterProperty> }
impl Finding {
    pub fn advice(code: FindingCode, file: Option<&str>, message: String, property: Option<WriterProperty>) -> Self;
    pub fn tier(&self) -> Tier; pub fn code(&self) -> FindingCode; pub fn message(&self) -> &str;
    pub fn file(&self) -> Option<&str>; pub fn penalty(&self) -> Option<Penalty>; pub fn property(&self) -> Option<WriterProperty>;
}
pub struct FileReport {
    pub path: String, pub bucket: u32, pub key_type: KeyType, pub rows: i64, pub row_groups: usize,
    pub key_pages: usize, pub value_pages: usize, pub max_key_page_rows: i64, pub max_key_bytes: usize,
    pub avg_value_page_bytes: u64, pub avg_value_bytes: u64, pub pages_aligned: bool, pub dictionary_columns: Vec<String>,
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
/// The wall clock as a `SystemTime`, for the one boundary that speaks it: the AWS SDK's credential
/// expiry, which the credential cache compares with `SystemTime`'s own methods. Every other caller uses `now`.
pub fn system_now() -> SystemTime;
/// Seconds from `since` to now, the `generation_age_seconds` gauge; a `since` in the future gives a
/// negative age, kept as measured.
pub fn age_seconds(since: jiff::Timestamp) -> f64;
/// Seconds since the Unix epoch with the fraction, the `generation_swapped_at_seconds` gauge; the one
/// place a timestamp becomes epoch seconds.
pub fn unix_seconds(at: jiff::Timestamp) -> f64;
/// RFC 3339 in UTC with a `Z` offset through jiff's printer, the fraction of a second only when it is
/// non-zero; the one place a timestamp becomes log text.
pub fn log_timestamp(at: jiff::Timestamp) -> String;
/// The epoch-millisecond count `step` after the epoch-millisecond `base`: how a fixture writer (the
/// testkit's, core's tests') stamps a commit later than the metadata's last update, as Iceberg
/// requires. No serving path writes a commit; public because the testkit is another crate. Whole
/// milliseconds of `step`; saturates at `i64::MAX`, outside what `timestamp_from_millis` admits.
pub fn millis_after(base: i64, step: Duration) -> i64;
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
    /// call it before exit. Each lock is waited for at most one refresh deadline, since a
    /// load or refresh gives its lock up by then and the lock is fair, so one slow table
    /// holds the others' next refresh up at most that long; with several slow tables, an
    /// early table can wait one deadline per slow table after it (RFD 017, Consequences).
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
served as opaque bytes), delete files, key too long, and the two residency
refusals of RFD 017: over budget, no bloom filter. Disk residency with no
local directory is not a refusal but `ConfigViolation::DiskResidencyWithoutLocalDir`:
`Config::parse` refuses it, and the loader raises the same violation, before
any chunk is fetched, for a configuration built in code. The reasons raised
for one file carry its path, so a log line names the file.
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
refresh that swapped a generation in is followed by `Opened::retain`. A
periodic task that panics before the shutdown aborts it, the only way one
ends, ends the run, even when a shutdown was already asked for: the log
names the task (`refresh of table <name>` or `sampler`) with the panic,
the daemon drains, and the process exits 1,
since a daemon whose tables nothing refreshes would otherwise keep
answering `/ready` 200 from an ageing snapshot (`ServeError::Periodic`).
`serve` logs JSON to stdout at `info` (the twelve-factor log stream)
through the binary's own `tracing` event formatter: keys in the order
time, level, target, message, the event's other fields, the static fields;
the standard fields under the names `LogConfig.fields` maps them to, every
line carrying the message key (empty for an event without one), and unique
keys, an event's own field winning over a standard one and never displaced
by a static field; a `log.fields` key naming no standard field, or two
standard fields mapped to one emitted name, is refused with the rest of the
configuration (RFD 011), exit 1. Prometheus histograms use the prototype's buckets (100 µs to 2 s); the
recorder installs once per process and every later caller gets the same
handle. SIGTERM or SIGINT drains in-flight requests, flushes the page
cache's disk level (RFD 017), and exits 0. The drain waits at most
`server.drain_deadline` for the servers' connections; past it, it stops
waiting and aborts both server tasks (tonic serves each connection on a
task of its own, so a call still open ends with the process), closes the
page cache all the same, and ends the run with
`ServeError::DrainOverran { after }`, exit 1. A shutdown asked for while the
first loads run is honoured as soon as they end, cutting the grace short
and never flipping to `SERVING`. Every drain, after a shutdown or a failed
run, first sets both health names `NOT_SERVING` and then removes them,
which ends every `Health/Watch` stream: tonic's graceful shutdown waits for
every connection to close, and an open watch would hold its connection,
and the drain, without end.

**The service.** `Service::new(&Opened, &ServerConfig)` serves `Get`,
`MultiGet`, and `TableInfo` over the opened tables. A wire key is read under
the served generation's key type (`key_from_wire`, a fault is
`WireKeyFault`, `INVALID_ARGUMENT`); core's errors become status codes by
variant (`status_from`, where a page fetch past its deadline,
`CacheError::FetchDeadline`, is `DEADLINE_EXCEEDED` like the call's own
deadline); the service's own bounds are `CallFault`, one code
per variant. `Get` runs under `server.get_deadline` and `MultiGet` under
`server.multi_get_deadline`; past it the lookup future is dropped and the
call is `DEADLINE_EXCEEDED` (`CallFault::DeadlineExceeded`). A page
fetch the lookup started runs on its own task inside the page cache, and
ends at `server.get_deadline` from its own start, which the table's cache
handle carries: its GET, and a hedge beside it, is dropped, which cancels
the request, and nothing is cached. In
a `MultiGet` the page fetches have that deadline too, so over a store that
has stopped answering a batch ends at the `Get` deadline when that comes
before its own. A client's own deadline is the gRPC layer's and the service
reads no `grpc-timeout`: tonic's server wraps every service in a layer that
drops the call at the client's deadline, which frees its permit, and answers
`CANCELLED`, while the client's library ends the call on its own side at the
same deadline and reports the expiry as its own (a tonic client, `CANCELLED`).
`Get` and `MultiGet`
share one semaphore of `server.max_in_flight` permits (held to tokio's
`Semaphore::MAX_PERMITS`); each call takes one with `try_acquire` as it
starts and holds it until the call ends, so a call past its deadline
gives it back when it is dropped. A call that finds none free is
`RESOURCE_EXHAUSTED` at once (`CallFault::Overloaded`), never queued.
`TableInfo` takes no permit. A `MultiGet` of more than
`server.max_multi_get_keys` keys is `INVALID_ARGUMENT`
(`CallFault::TooManyKeys`) before it takes a permit or looks anything up.

**One-shot subcommands.** `doctor`, `get <key>...`, and `info` each take
the target vocabulary after the subcommand: `--config <file>` or
`--location <root or metadata file>` (at most one; with neither, the file
`ICETRAY_CONFIG` names, read by `Target::config` rather than by clap, since
clap cannot tell an environment value from a flag and would report every
`--location` run under an exported variable as a conflict; no flag and no
variable is a usage error), `--rewrite FROM=TO` under either (split at the
first `=`, a text with none being a usage error; the rules go ahead of the
file's, so storage's first match makes a command-line rule take precedence
for the same prefix, and are validated with them, so an empty `FROM` is
`ConfigViolation::EmptyRewritePrefix`, exit 1, as in a file), and four
location-only flags (`--key-column`, `--value-column`, `--ref`,
`--residency`) that require `--location` and conflict with `--config`,
both, since clap waives a `requires` whose argument conflicts with a
present one. A `--location` table is opened under the name `table`, never
strict (the one-shots exist to report what `strict` would refuse), with
defaults `key`, `value`, `main`, `full`, and held to every rule
`Config::parse` checks through `Config::validated`. `--residency` offers
`full` and `bloom` only, through a `PossibleValuesParser` over
`Residency::name` read back by `Residency`'s `FromStr`, so clap refuses
`disk` as a usage error naming the accepted values: a one-shot is one load
and exit, no flag supplies the `storage.local` a disk tier needs, and the
doctor under `full` already reports every tier's predicted bytes. The table filter is a
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
and `FileReport`, the `file` lines only under `doctor --files`; a `get`
line carries the key, `presence`
(`present`/`null`/`absent`), the value as standard base64 and, when UTF-8,
as text, `value_bytes`, `snapshot_id`, the GETs the lookup issued, and
`elapsed_micros`; a table the doctor could not load is one `refused` line
(`table`, `reason` as the `ErrorKind`'s name, `message`). The doctor opens
every table with `strict` off. Exit codes: 0 for a run that did what was
asked; 1 for a load or read error, a table the doctor could not load (which
dominates 2), a configuration file that cannot be read or parsed or breaks
one of its rules (a bad log field mapping among them), a
first-load failure, a bind failure, or a drain that passed its deadline;
2 when the doctor found a cost
finding (RFD 016); 64 (`sysexits.h`'s `EX_USAGE`) when the command line or
the target could not be used, including clap's own errors, no target, no
table matched, a table name required, and a key that does not parse, so clap's default exit code 2 is never used and 2 means one
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
`contract`'s constants; `KEY_BLOOM_FPP`, `OVERSIZED_FPP`, `UNDERSIZED_FPP`,
`KEY_COLUMN`, `VALUE_COLUMN`), `fixture` (`FixtureSpec` with `small` and
`interleaved`, `Order`, `Nulls`, `Fixture` with its accessors, `files`,
`keys_at`, and the sources `catalog_source`, `root_source`,
`pinned_source`; `RawFile::from_batch`; `HandSnapshot`; `write_fixture`,
`write_table_of_types`, `position_delete_file`, `memory_catalog_local`,
`memory_catalog_s3` (their `MemoryCatalog` re-exported at the crate root, so a
caller without `iceberg` can hold it), `local_catalog`, `residue_class`; the constants
`SMALL_IDS`, `SMALL_BUCKETS`, `SAMPLE`, `INTERLEAVED_IDS`,
`INTERLEAVE_STRIDE`, `INTERLEAVED_BUCKETS`, `DATA_DIR`, and the shapes the
acceptance tests share, each declared once: `NAMESPACE`, `IDS_4K`, `IDS_2K`,
`SAMPLE_4K`, `OVERLAPPING_APPEND`, `APPENDED_4K_6K`, `NEW_ID`, `OLD_ID`,
`ONE_BUCKET`, `TWO_BUCKETS`, `FOUR_BUCKETS`, `TINY_BUDGET`,
`UNPARTITIONED_FILE_BYTES`, `SMALL_FILE_BYTES`, `WIDE_VALUE_BYTES`,
`LONG_KEY_EXTRA`, `LONG_KEY_BYTES`, `LONG_KEY_PAD`, `SECOND_CLASS`,
`THIRD_CLASS`, `TWO_FILES`, `THREE_FILES`, `TAIL`, `INTERLEAVED_SAMPLE`,
`NEWEST_SAMPLE`), `s3` (`S3Fixture`, whose `storage_config` is
hedge-disabled, whose `file_io_props` is a `HashMap`, and whose `hold`
stalls every request until `release`, with `held` counting the requests
that arrived while held and `waiting` those still open, which a client
that gives one up and closes its connection leaves), `sim`
(`serve_s3`, `serve_s3_with`, `SlowEvery`, `sim_s3_store`, `SIM_S3_PORT`),
`verify` (`Served` with `load` and `current`; `build`, `serve_over`,
`serve`, `serve_catalog`, `serve_catalog_with`; `expected_cold_gets`,
`expected_key_pages`, `predicted_probe`; `verify_hits`, `verify_hit_cost`,
`verify_misses`; `storage_config_for_tests`, `series_value` (one series
of a Prometheus text exposition by name and labels, as the `f64` the
format carries, `None` only when the series is absent),
`TEST_PAGE_CACHE_BYTES`, and the GET oracle's fixed counts
`LOAD_METADATA_GETS`, `METADATA_FILE_GETS`, `LOAD_GETS_PER_FILE`,
`HINT_POLL_GETS`), and
`golden` (`WAREHOUSE_VAR`, `warehouse`, `GoldenTable`, `table_config`,
`config`, `long_key`, `string_key`, `permuted_key`, `value`,
`wide_value`). The constants `PAGE_ROWS`, `VALUE_PAGE_BYTES`,
`TARGET_FILE_BYTES_MAX`, `KEY_BYTES_ADVICE`, `MAX_KEY_BYTES`, and
`PAGE_CACHE_DISK_BLOCK_BYTES` are imported from `contract` and never
redeclared; the golden key derivations delegate to `keys`; a `usize`
widens to `u64` through core's `approx::count`, and a `u64` or `u32`
narrows to `usize` through `approx::length`. Everything is test-only,
under `[dev-dependencies]` of the crates that use it.

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
(P), `sim.rs` (S), `golden.rs` (G), and `rest_catalog.rs` (C); the scale
probe is `examples/scale.rs`, a program run by hand and not a test. One fixture per `RefusalReason` variant: descending
order for `Unsorted`; a raw nullable key column for `NullKeys`; an
overlapping append for `DuplicateKeys`; a raw `Utf8` key column on a
long-keyed table for `KeyTypeMismatch`; `write_table_of_types` with an
`Int` key for `KeyTypeUnsupported` and a `Long` value for
`ValueTypeUnsupported`; a by-hand snapshot listing a position-delete file
for `DeleteFiles`; a `key_pad` over `MAX_KEY_BYTES` for `KeyTooLong`; a
one-byte budget for `OverBudget`; `Bloom::Off` under `Residency::Bloom`
for `NoBloomFilter`. Beside them, `Residency::Disk` without a local
directory proves the loader raises `DiskResidencyWithoutLocalDir` for a
configuration built in code and leaves the table as a refusal does. One
fixture per `FindingCode`: `Dictionary::Value`;
`page_rows` at twice `KEY_PAGE_ROWS_COST` with 24-byte values; `buckets:
0` rolled into twenty 64 KiB files; the interleaved fixture;
`Statistics::Truncated`; `column_index_truncate` under a shared 64-byte
prefix; 16 KiB values under the row bound; a padded string key; 9,000 rows
of 16 KiB in one file; 48,000 ids at a 40 KiB target; `Bloom::Off`;
`Bloom::Fpp(1e-12)`; `Bloom::Fpp(0.5)`. The completeness of
`FindingCode::ALL` is proved once, by core's successor chain in `check.rs`
(a new variant fails that match to compile until it is placed, and the
chain is compared to the list); the fixture suite has one test per code by
convention. The refusal set, whose variants carry fields and have no `ALL`,
is bound by the two exhaustive matches in `error.rs` (the label and the
message detail), which a new variant fails to compile; its fixtures exist
by the same convention. The advice a layout cannot avoid is asserted
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
| Load costs one footer GET per file and one key-chunk GET per row group, plus the metadata reads; a load with `previous` reuses every unchanged file by pointer | P |
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
| Disk tier: a cold hit is one GET and one local read, a miss one local read and no GET; refusals identical to Full plus its own; a restart with matching chunk files fetches footers only; a dropped file's chunk file is deleted; no local directory is the configuration violation `Config::parse` refuses, raised by the loader for a configuration built in code | P (temporary directory, counting GETs and local reads) |
| A uuid key column routes and orders as the reference implementation does, and a uuid-keyed lookup costs the same as a long-keyed one | U (oracle), P |
| The page cache's disk tier serves a page evicted from memory with a local read and no GET, and survives a restart | P (temporary directory) |
| Row-group choice never consults column statistics: long string keys sharing a prefix beyond 64 bytes across several row groups all resolve | U (carried from the prototype) |
| Over-budget Full load is refused at the footer stage with no chunk fetched | P (counting) |
| The value column's column index is not resident, and a file's resident bytes are what its footer predicted | P (`lookup.rs`: no loaded footer keeps a value column index), U for the prediction (core's byte-bounded large-value fixture: the kept footer is smaller than the loaded one by at least the dropped index, and `predicted_resident_bytes` of the loaded footer equals the built index's resident bytes) |
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
helpers), `icetray_generation_sequence_number{table}` (the serving
snapshot's sequence number) and `icetray_generation_swapped_at_seconds{table}`
(when it was swapped in, in Unix seconds), `icetray_table_state{table,state}`.
No metric carries the snapshot id: a label per snapshot would add a series
with every swap for the life of the process, since the recorder installs
no idle timeout (it would drop the live gauges `table` sets only on
change). The id is in the swap's log event and in `TableInfo`.

Every label value is the `Display` of an enum, never a literal string at the
call site: `kind` from a read-kind enum, `result` from `LookupResult`, `call`
from the RPC enum, `reason` from `ErrorKind`, whose `Display` is the
`RefusalReason`'s name for `Error::Refused`, `inefficient` for the strict
gate, and the variant's snake_case name otherwise, `code` from `FindingCode`,
`state` from `TableState`.

Ownership: `table` emits `lookups_total`, `lookup_seconds`,
`refresh_errors_total`, `generation_swaps_total`, `index_bytes`, `findings`,
`generation_sequence_number`, `generation_swapped_at_seconds`, and
`table_state`; `cache` the two page-cache counters and
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
