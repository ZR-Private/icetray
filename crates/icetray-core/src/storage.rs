//! Every byte read from object storage (RFD 003): one store per
//! `scheme://authority/` prefix, location rewriting before any request
//! leaves the process, one counter for GETs, bytes, hedges, local reads and
//! local bytes, the AWS SDK's credential chain, loaded before the first S3
//! store or AWS catalog client is built and bridged with a cached and
//! coalesced refresh that every S3 store and catalog client of a `Storage`
//! shares, and the hedged lookup read of RFD 013.

use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::ops::Range;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime};

use aws_config::SdkConfig;
use aws_credential_types::Credentials;
use aws_credential_types::provider::error::CredentialsError;
use aws_credential_types::provider::{self, ProvideCredentials, SharedCredentialsProvider};
use bytes::Bytes;
use futures::future::{Either, select};
use metrics::{Counter, Histogram};
use object_store::aws::{AmazonS3Builder, AwsCredential};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    BackoffConfig, ClientOptions, CredentialProvider, ObjectStore, ObjectStoreExt, RetryConfig,
};
use tokio::sync::OnceCell;
// Reads are timed on tokio's clock: a paused runtime (the simulation) advances
// it virtually, so the hedge window and the timer that consults it agree; the
// wall clock would feed the window real durations under a virtual timer.
use tokio::time::Instant;

use crate::approx;
use crate::config::{HedgeConfig, S3Config, StorageConfig};
use crate::error::{Error, Result};
use crate::metric::{self, ReadKind};
use crate::time;

/// The prefix of the local filesystem store. A `file://` URI and a bare
/// absolute path both resolve to it, so a table written by a local job and
/// one whose metadata records URIs read the same way; a store registered
/// under it (`Storage::register`) serves every local location.
pub const FILE_PREFIX: &str = "file://";

/// The local filesystem scheme, which every `file:` URI starts with
/// (RFC 8089): `file:///path` with an empty authority, `file://localhost/path`,
/// and the authority-less `file:/path`, the form Hadoop's `Path` records
/// in the metadata of a table written to a local warehouse, all name the
/// same absolute path.
pub(crate) const FILE_SCHEME: &str = "file:";

/// What opens a URI's authority after its scheme's `:`.
const AUTHORITY_OPENING: &str = "//";

/// The one host a `file:` URI may name besides none: this one (RFC 8089).
const LOCALHOST: &str = "localhost";

/// What separates a URI's scheme from its authority.
pub(crate) const SCHEME_SEPARATOR: &str = "://";

/// What separates an authority from the key, and the segments of a key.
pub(crate) const PATH_SEPARATOR: char = '/';

/// The schemes that name an S3 bucket by authority. `s3a://` is what
/// Hadoop-based writers record and reads the same as `s3://`.
const S3_SCHEMES: [&str; 2] = ["s3://", "s3a://"];

/// The store name `object_store`'s own S3 errors carry, so an error from the
/// credential bridge reads like one from the client it serves.
const S3_STORE_NAME: &str = "S3";

/// The provider name the static keys of `[storage.s3]` carry when an AWS
/// catalog client signs with them; the SDK records it in its own logs.
const STATIC_KEYS_PROVIDER: &str = "storage.s3";

/// How long before its expiry a cached credential stops being handed out.
/// Two minutes is longer than any one S3 request with its retries can take,
/// `S3_RETRY_TIMEOUT` plus `S3_MAX_BACKOFF` plus `S3_REQUEST_TIMEOUT`, which
/// a compile-time assertion below holds it to, so a request signed with a
/// credential from the cache never reaches the store after that credential
/// expired. It is short next to the hour-long lifetime of role and SSO
/// credentials, so the chain is consulted about once per lifetime rather
/// than once per GET (RFD 003).
const CREDENTIAL_REFRESH_MARGIN: Duration = Duration::from_secs(120);

/// How long after an S3 request's first attempt the client may still start
/// another; `max_retries` bounds the count, this the time. A retry is sent
/// with the credential the first attempt was signed with, so the last one
/// must end inside `CREDENTIAL_REFRESH_MARGIN`: one minute, plus a backoff
/// and a whole attempt, does.
const S3_RETRY_TIMEOUT: Duration = Duration::from_secs(60);

/// The longest pause between two attempts of one S3 request. The first
/// pause and the growth between pauses are `object_store`'s defaults; the
/// cap is ours so that the bound on a request with its retries is stated in
/// this module's constants.
const S3_MAX_BACKOFF: Duration = Duration::from_secs(5);

/// How long one attempt of an S3 request may run, from connecting to the
/// last byte of the body: thirty seconds, `object_store`'s own default,
/// set here so the credential bound is stated in this module. A load reads
/// whole key column chunks, which take seconds on a slow link; a lookup
/// under the default `Get` deadline of five seconds is dropped, with its
/// read, long before an attempt runs this long.
const S3_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// An S3 request with its retries ends inside the credential margin: the
/// last attempt starts within `S3_RETRY_TIMEOUT` plus one backoff of the
/// first and runs at most `S3_REQUEST_TIMEOUT`.
const _: () = assert!(
    S3_RETRY_TIMEOUT
        .saturating_add(S3_MAX_BACKOFF)
        .saturating_add(S3_REQUEST_TIMEOUT)
        .as_nanos()
        < CREDENTIAL_REFRESH_MARGIN.as_nanos()
);

/// The least time a credential the chain has just resolved is handed out
/// before the chain is asked again, when its expiry is already inside
/// `CREDENTIAL_REFRESH_MARGIN`. Some chains answer with a credential near
/// its expiry (a helper that caches until expiry, instance metadata just
/// before rotation) and, asked again at once, answer with the same one;
/// without this floor the chain would run once per GET, serialized behind
/// the cache lock, for the rest of that credential's life (RFD 003). Thirty
/// seconds bounds that to a few runs per margin and still adopts a fresher
/// credential well inside the margin. No credential is handed out past its
/// expiry.
const CREDENTIAL_MIN_REUSE: Duration = Duration::from_secs(30);

/// One counter for a lookup's whole cost, whatever residency the table runs
/// (RFD 003): object-store GETs and local reads are counted in the same
/// place. Every GET a `Storage` issues is counted here whether or not it
/// succeeds, so the counts are the test oracle of RFD 014 and the source of
/// the request metrics of RFD 012. The metric handles are registered once,
/// when the stats are built, with the recorder current then, so a GET
/// formats no label and looks up no series; a process installs its
/// recorder before it builds a `Storage`, since stats built earlier record
/// to no recorder.
#[derive(Debug)]
pub struct IoStats {
    gets: AtomicU64,
    bytes: AtomicU64,
    hedges: AtomicU64,
    local_reads: AtomicU64,
    local_bytes: AtomicU64,
    load: KindMetrics,
    lookup: KindMetrics,
    hedges_total: Counter,
    local_reads_total: Counter,
    local_bytes_total: Counter,
}

/// The metric handles of one `ReadKind`, under its `kind` label.
#[derive(Debug)]
struct KindMetrics {
    gets: Counter,
    bytes: Counter,
    get_seconds: Histogram,
}

impl KindMetrics {
    fn new(kind: ReadKind) -> Self {
        Self {
            gets: metrics::counter!(metric::OBJECT_GETS_TOTAL, metric::LABEL_KIND => kind.to_string()),
            bytes: metrics::counter!(metric::OBJECT_BYTES_TOTAL, metric::LABEL_KIND => kind.to_string()),
            get_seconds: metrics::histogram!(metric::OBJECT_GET_SECONDS, metric::LABEL_KIND => kind.to_string()),
        }
    }
}

impl Default for IoStats {
    fn default() -> Self {
        Self {
            gets: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            hedges: AtomicU64::new(0),
            local_reads: AtomicU64::new(0),
            local_bytes: AtomicU64::new(0),
            load: KindMetrics::new(ReadKind::Load),
            lookup: KindMetrics::new(ReadKind::Lookup),
            hedges_total: metrics::counter!(metric::HEDGES_TOTAL),
            local_reads_total: metrics::counter!(metric::LOCAL_READS_TOTAL),
            local_bytes_total: metrics::counter!(metric::LOCAL_BYTES_TOTAL),
        }
    }
}

impl IoStats {
    /// GETs issued, whole-object and ranged, hedges included; a
    /// `read_ranges` call counts one per range.
    pub fn gets(&self) -> u64 {
        self.gets.load(Ordering::Relaxed)
    }

    /// Bytes requested across those GETs; for a whole-object GET, the size
    /// the store reported, which is zero when the request failed.
    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    /// GETs that were the second copy of a slow lookup read (RFD 013).
    pub fn hedges(&self) -> u64 {
        self.hedges.load(Ordering::Relaxed)
    }

    /// Reads served from the local directory (RFD 017).
    pub fn local_reads(&self) -> u64 {
        self.local_reads.load(Ordering::Relaxed)
    }

    /// Bytes of those local reads.
    pub fn local_bytes(&self) -> u64 {
        self.local_bytes.load(Ordering::Relaxed)
    }

    /// The metric handles of the `kind` path.
    fn of(&self, kind: ReadKind) -> &KindMetrics {
        match kind {
            ReadKind::Load => &self.load,
            ReadKind::Lookup => &self.lookup,
        }
    }

    /// One GET of `len` requested bytes on the `kind` path.
    pub(crate) fn record_get(&self, kind: ReadKind, len: u64) {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(len, Ordering::Relaxed);
        let metrics = self.of(kind);
        metrics.gets.increment(1);
        metrics.bytes.increment(len);
    }

    /// One observation of the GET latency histogram on the `kind` path.
    fn record_latency(&self, kind: ReadKind, elapsed: Duration) {
        self.of(kind).get_seconds.record(time::seconds(elapsed));
    }

    /// A hedge: one more lookup GET of the same `len`, issued because the
    /// first copy was slow. A hedge is a GET and counts as one, and it
    /// counts once more under its own name (RFD 013).
    pub(crate) fn record_hedge(&self, len: u64) {
        self.hedges.fetch_add(1, Ordering::Relaxed);
        self.hedges_total.increment(1);
        self.record_get(ReadKind::Lookup, len);
    }

    /// One read of `len` bytes from the local directory (RFD 017), counted
    /// beside GETs so a lookup's whole cost is one structure whatever tier
    /// the table runs (RFD 003). A local read is never a GET.
    pub(crate) fn record_local_read(&self, len: u64) {
        self.local_reads.fetch_add(1, Ordering::Relaxed);
        self.local_bytes.fetch_add(len, Ordering::Relaxed);
        self.local_reads_total.increment(1);
        self.local_bytes_total.increment(len);
    }
}

/// A location resolved to the store that holds the object and the object's
/// path within that store. The `scheme://authority/` prefix is not kept:
/// nothing downstream reads it, and a `Located` is only ever handed back to
/// the `Storage` that produced it.
#[derive(Clone, Debug)]
pub(crate) struct Located {
    /// The store that holds the object.
    pub store: Arc<dyn ObjectStore>,
    /// The object's path within that store.
    pub path: Path,
}

/// Where an S3 store's credentials come from. Exactly one source per store
/// and never a third (RFD 003): `object_store`'s own environment and
/// instance-metadata lookups are not consulted, so a store either signs
/// with what this enum names or does not sign at all.
enum CredentialSource<'a> {
    /// The static key pair from `[storage.s3]`: the path for S3-compatible
    /// stores and for tests.
    Static {
        key_id: &'a str,
        secret_access_key: &'a str,
    },
    /// The SDK's default chain, loaded once by a `Storage` built with
    /// `Storage::with_sdk_defaults`, before the first S3 store it builds,
    /// through the storage's one bridge (`Storage::sdk_credentials`).
    SdkChain(Arc<SdkCredentials>),
    /// No credentials: requests go unsigned. Correct for public buckets and
    /// for a process that never sends a request to a store it built, such
    /// as a test that registered its own.
    Unsigned,
}

impl fmt::Display for CredentialSource<'_> {
    /// Names the source and never its contents, so it is safe in a log line.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Static { .. } => "static",
            Self::SdkChain(_) => "sdk_chain",
            Self::Unsigned => "unsigned",
        };
        f.write_str(name)
    }
}

/// Why a location names no store this process reads (RFD 003). Display
/// reads after the location it is about. The remedy is a rewrite rule or
/// another location, so each is the configuration's fault, or the command
/// line's, wherever the location came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum LocationFault {
    /// Neither a URI nor an absolute path, so it names no store at all.
    #[error("is neither a uri nor an absolute path")]
    NotAbsolute,
    /// A `file:` URI whose path is relative, as in `file:db/kv`: a URI
    /// is never resolved against a working directory, so it names none.
    #[error(
        "is a file: uri with a relative path; a file: location takes an absolute path, as file:///data/kv does"
    )]
    FileRelative,
    /// A `file:` URI naming a host other than this one, whose files no
    /// store here reads.
    #[error(
        "is a file: uri naming another host; a file: location names this one, as file:///data/kv or file://localhost/data/kv"
    )]
    FileRemoteHost,
    /// A URI whose `scheme://authority/` prefix names no store: none is
    /// registered for it, and the process builds `s3://`, `s3a://`, and
    /// `file://` stores alone.
    #[error(
        "names no object store; icetray reads s3://, s3a://, and file:// locations, so add a rewrite rule to one of them"
    )]
    NoStore,
}

/// A location no store serves, as storage tried to resolve it: after
/// rewriting, which is the text a store would have been asked for.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("location {location} {fault}")]
pub struct LocationError {
    /// The location after the first matching rewrite rule.
    pub location: String,
    /// Why no store serves it.
    pub fault: LocationFault,
}

/// Resolves Iceberg locations to stores and reads bytes through them.
/// Every read the crate makes from object storage goes through one of the
/// `read_*` methods, so rewriting, counting, credentials, and hedging happen
/// in one place (RFD 003).
#[derive(Debug)]
pub struct Storage {
    config: StorageConfig,
    /// The AWS SDK's defaults, when the process asked for them: loaded
    /// the first time a location under an S3 prefix is located or an AWS
    /// catalog client is built (`aws_sdk_config`), and kept.
    /// `object_store` alone reads environment variables and instance
    /// metadata; the SDK adds profiles, SSO, `credential_process`, and web
    /// identity. Loading reads the profile files and may probe the
    /// instance metadata service for a region, which takes seconds off
    /// EC2, so a process that reads no S3 location never loads them.
    sdk: Option<OnceCell<LoadedSdk>>,
    /// One store per `scheme://authority/` prefix. Tests register stores
    /// under synthetic prefixes; `s3://`, `s3a://`, and `file://` stores are
    /// built on first use.
    stores: Mutex<HashMap<String, Arc<dyn ObjectStore>>>,
    stats: Arc<IoStats>,
}

impl Storage {
    /// Builds from configuration with no SDK chain: an S3 store signs with
    /// the configured static keys or not at all, in the configured region
    /// or the store's default. For tests; a process that reads S3 under
    /// the platform's credentials uses `with_sdk_defaults`.
    pub fn new(config: StorageConfig) -> Self {
        Self {
            config,
            sdk: None,
            stores: Mutex::new(HashMap::new()),
            stats: Arc::new(IoStats::default()),
        }
    }

    /// Builds from configuration with the AWS SDK's default region and
    /// credential chain: profiles, SSO, `credential_process`, web identity,
    /// and instance metadata, loaded once, when `locate` first meets a
    /// location under an S3 prefix, and never for a process that reads
    /// none. Configured static keys still win for every S3 store.
    pub fn with_sdk_defaults(config: StorageConfig) -> Self {
        Self {
            sdk: Some(OnceCell::new()),
            ..Self::new(config)
        }
    }

    /// Registers the store for a `scheme://authority/` prefix; a location
    /// under that prefix resolves to it and nothing is built for it. The
    /// prefix ends in `/`, as `locate` splits it.
    pub fn register(&self, prefix: impl Into<String>, store: Arc<dyn ObjectStore>) {
        self.stores
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(prefix.into(), store);
    }

    /// The counters every read through this storage updates.
    pub fn stats(&self) -> Arc<IoStats> {
        Arc::clone(&self.stats)
    }

    /// The hedge settings this storage runs under, so a `Table` builds its
    /// `HedgeWindow` from the same configuration its reads are hedged by
    /// (RFD 013).
    pub(crate) fn hedge_config(&self) -> &HedgeConfig {
        &self.config.hedge
    }

    /// How long one catalog call may run, from `storage.catalog_timeout`.
    /// A catalog client speaks to its catalog over HTTP and never through
    /// this storage, but its bound is storage configuration beside the S3
    /// client's, and every `TableSource` resolves with a `Storage` in hand
    /// (RFD 013).
    pub(crate) fn catalog_timeout(&self) -> Duration {
        self.config.catalog_timeout
    }

    /// Resolves a location to a store and a path, as `locate_loaded` does,
    /// once the SDK's defaults are loaded when the location, rewritten,
    /// is under an S3 prefix and this storage was built with them; so an
    /// S3 store is never built before the chain it signs with. Every
    /// location the crate reads is located here.
    pub(crate) async fn locate(&self, location: &str) -> Result<Located> {
        self.load_sdk_for(location).await;
        self.locate_loaded(location)
    }

    /// Resolves a location to a store and a path with the SDK's defaults
    /// as far as they are loaded: the configured rewrite
    /// rules are applied in order and the first match wins, then the
    /// location is split into its `scheme://authority/` prefix and the rest
    /// (`file://` and a bare absolute path both name the local filesystem),
    /// then the store is taken from the registry or built on first use. A
    /// location no store serves is `Error::Location`, naming no table:
    /// the table whose load read it names itself (`Error::for_table`).
    fn locate_loaded(&self, location: &str) -> Result<Located> {
        let location = self.rewrite(location);
        let unserved = |fault| Error::Location {
            table: None,
            source: LocationError {
                location: location.to_string(),
                fault,
            },
        };
        let (prefix, rest) = split_location(&location).map_err(unserved)?;
        let store = self
            .store_for(&prefix)?
            .ok_or_else(|| unserved(LocationFault::NoStore))?;
        let path = if prefix == FILE_PREFIX {
            Path::from_absolute_path(&rest)?
        } else {
            Path::parse(&rest)?
        };
        Ok(Located { store, path })
    }

    /// Reads a whole object. One load GET of the object's size, counted
    /// whatever the outcome.
    pub async fn read_all(&self, location: &str) -> Result<Bytes> {
        let l = self.locate(location).await?;
        observed(&self.stats, ReadKind::Load, self.get_all(&l)).await
    }

    /// Reads one byte range on the load path. One load GET, never hedged:
    /// footers and key chunks are bulk work nobody waits on (RFD 013).
    pub(crate) async fn read_range(&self, l: &Located, range: Range<u64>) -> Result<Bytes> {
        self.stats.record_get(ReadKind::Load, range_len(&range));
        observed(
            &self.stats,
            ReadKind::Load,
            l.store.get_range(&l.path, range),
        )
        .await
    }

    /// Reads one byte range on the lookup path, hedged against the table's
    /// own latency window (RFD 013): when `window.delay()` names a delay,
    /// the request races a timer of that length, and if the timer wins an
    /// identical second request is issued, counted as a hedge, and the first
    /// answer wins. Every attempt, hedged or not, is recorded into the
    /// window with its elapsed time, so the ceiling counts every hedge
    /// spent; that includes a read dropped after its delay passed
    /// (`WindowSample`).
    /// One lookup GET, plus one more when a hedge is issued. A plain `fn`
    /// returning `read_span_hedged`'s future: every lookup future lays out
    /// two of these, and an `async fn` around it would add its own state.
    pub(crate) fn read_range_hedged<'a>(
        &'a self,
        l: &'a Located,
        range: Range<u64>,
        window: &'a HedgeWindow,
    ) -> impl Future<Output = Result<Bytes>> + 'a {
        self.read_span_hedged(l, range, 1, window)
    }

    /// `read_range_hedged` of a range holding `pages` pages, a Scan's span
    /// GET (RFD 025, Caching and hedging): a span's latency grows with its
    /// pages, so the delay it races is `window`'s per-page delay times its
    /// pages (`HedgeWindow::delay_for`), and it enters the window as its
    /// elapsed time over its pages. A window that records only such reads
    /// holds per-page latencies; one page is a plain lookup GET.
    pub(crate) async fn read_span_hedged(
        &self,
        l: &Located,
        range: Range<u64>,
        pages: u32,
        window: &HedgeWindow,
    ) -> Result<Bytes> {
        let len = range_len(&range);
        self.stats.record_get(ReadKind::Lookup, len);
        let pages = pages.max(1);
        let delay = window.delay_for(pages);
        let mut sample = WindowSample {
            window,
            started: Instant::now(),
            delay,
            pages,
            hedged: false,
            recorded: false,
        };
        let answer = self.race(l, range, len, delay, &mut sample.hedged).await;
        let elapsed = sample.record();
        self.stats.record_latency(ReadKind::Lookup, elapsed);
        answer.map_err(Error::from)
    }

    /// Reads several byte ranges of one object in one call to the store.
    /// One load GET per range: the store may coalesce neighboring ranges on
    /// the wire, and RFD 003 states its promises in logical GETs, for which
    /// this count is exact.
    pub(crate) async fn read_ranges(
        &self,
        l: &Located,
        ranges: &[Range<u64>],
    ) -> Result<Vec<Bytes>> {
        for range in ranges {
            self.stats.record_get(ReadKind::Load, range_len(range));
        }
        observed(
            &self.stats,
            ReadKind::Load,
            l.store.get_ranges(&l.path, ranges),
        )
        .await
    }

    /// Loads the SDK's defaults, once, when `sdk_to_load_for` names them.
    async fn load_sdk_for(&self, location: &str) {
        if let Some(sdk) = self.sdk_to_load_for(location) {
            load_sdk(sdk).await;
        }
    }

    /// The SDK's defaults an AWS catalog client starts from (RFD 019),
    /// loaded first if this storage was built with them and has not loaded
    /// them yet, so the client is built after the chain it signs with, as
    /// an S3 store is; `None` for a storage built without them.
    pub(crate) async fn aws_sdk_config(&self) -> Option<&SdkConfig> {
        let sdk = self.sdk.as_ref()?;
        Some(&load_sdk(sdk).await.config)
    }

    /// The credentials an AWS catalog client signs with (RFD 019): the
    /// ones every S3 store of this storage signs with (`credential_source`),
    /// so a catalog call never consults a chain of its own. `None` when
    /// S3 requests go unsigned.
    pub(crate) fn aws_credentials(&self) -> Option<SharedCredentialsProvider> {
        match self.credential_source() {
            CredentialSource::Static {
                key_id,
                secret_access_key,
            } => Some(SharedCredentialsProvider::new(Credentials::new(
                key_id,
                secret_access_key,
                None,
                None,
                STATIC_KEYS_PROVIDER,
            ))),
            CredentialSource::SdkChain(bridge) => {
                let bridge: Arc<dyn ProvideCredentials> = bridge;
                Some(SharedCredentialsProvider::new(bridge))
            }
            CredentialSource::Unsigned => None,
        }
    }

    /// The region `[storage.s3]` names, if any.
    pub(crate) fn s3_region(&self) -> Option<&str> {
        self.config.s3.as_ref().and_then(|s3| s3.region.as_deref())
    }

    /// The SDK's defaults still to load before `location` is located: when
    /// this storage was built with them, they are not loaded yet, and the
    /// location, rewritten, is under an S3 prefix.
    fn sdk_to_load_for(&self, location: &str) -> Option<&OnceCell<LoadedSdk>> {
        let sdk = self.sdk.as_ref().filter(|sdk| !sdk.initialized())?;
        let location = self.rewrite(location);
        S3_SCHEMES
            .iter()
            .any(|scheme| location.starts_with(scheme))
            .then_some(sdk)
    }

    /// The location with the first matching rewrite rule applied, or the
    /// location itself (`StorageConfig::rewritten`).
    fn rewrite<'a>(&self, location: &'a str) -> Cow<'a, str> {
        self.config.rewritten(location)
    }

    /// The store registered for `prefix`, or the one built for it and kept
    /// for the next location under it; `None` for a prefix nothing is
    /// registered for and no store is built for.
    fn store_for(&self, prefix: &str) -> Result<Option<Arc<dyn ObjectStore>>> {
        let mut stores = self.stores.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(store) = stores.get(prefix) {
            return Ok(Some(Arc::clone(store)));
        }
        let Some(store) = self.build_store(prefix)? else {
            return Ok(None);
        };
        stores.insert(prefix.to_string(), Arc::clone(&store));
        Ok(Some(store))
    }

    /// The store for a prefix nothing registered: the local filesystem for
    /// `file://`, an S3 client for `s3://` and `s3a://`, and `None` for
    /// anything else.
    fn build_store(&self, prefix: &str) -> Result<Option<Arc<dyn ObjectStore>>> {
        if prefix == FILE_PREFIX {
            return Ok(Some(Arc::new(LocalFileSystem::new())));
        }
        s3_bucket(prefix)
            .map(|bucket| self.s3_store(bucket))
            .transpose()
    }

    /// An S3 client for one bucket. Endpoint, region, `allow_http`, and
    /// path style come from `[storage.s3]`, or from its defaults when the
    /// table is absent, so the addressing style does not depend on whether
    /// the table is written; the region falls back to the SDK's when the
    /// file names none. Credentials come from exactly one
    /// `CredentialSource`. Every request retries at most `max_retries`
    /// times and within `S3_RETRY_TIMEOUT`, each attempt connects within
    /// `connect_timeout` and ends within `S3_REQUEST_TIMEOUT`, so a wedged
    /// store fails a request instead of holding it.
    fn s3_store(&self, bucket: &str) -> Result<Arc<dyn ObjectStore>> {
        let default_s3 = S3Config::default();
        let s3 = self.config.s3.as_ref().unwrap_or(&default_s3);
        let source = self.credential_source();
        tracing::debug!(bucket, credentials = %source, "building s3 store");
        let client = ClientOptions::new()
            .with_allow_http(s3.allow_http)
            .with_connect_timeout(self.config.connect_timeout)
            .with_timeout(S3_REQUEST_TIMEOUT);
        let retry = RetryConfig {
            backoff: BackoffConfig {
                max_backoff: S3_MAX_BACKOFF,
                ..BackoffConfig::default()
            },
            max_retries: self.config.max_retries,
            retry_timeout: S3_RETRY_TIMEOUT,
        };
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_client_options(client)
            .with_retry(retry)
            .with_virtual_hosted_style_request(!s3.path_style);
        if let Some(endpoint) = s3.endpoint.as_deref() {
            builder = builder.with_endpoint(endpoint);
        }
        let sdk_region = self.loaded_sdk().and_then(|sdk| sdk.config.region());
        let region: Option<&str> = s3
            .region
            .as_deref()
            .or_else(|| sdk_region.map(AsRef::as_ref));
        if let Some(region) = region {
            builder = builder.with_region(region);
        }
        builder = match source {
            CredentialSource::Static {
                key_id,
                secret_access_key,
            } => builder
                .with_access_key_id(key_id)
                .with_secret_access_key(secret_access_key),
            CredentialSource::SdkChain(bridge) => builder.with_credentials(bridge),
            CredentialSource::Unsigned => builder.with_skip_signature(true),
        };
        Ok(Arc::new(builder.build()?))
    }

    /// The SDK's defaults, once loaded.
    fn loaded_sdk(&self) -> Option<&LoadedSdk> {
        self.sdk.as_ref().and_then(OnceCell::get)
    }

    /// The one bridge from the SDK's credential chain this storage signs
    /// through, once the SDK's defaults are loaded and their chain
    /// resolved a provider: every S3 store it builds holds this one, and
    /// so does every AWS catalog client (`aws_credentials`), so the chain
    /// is consulted once per credential lifetime for the process rather
    /// than once per bucket or catalog (RFD 003).
    pub(crate) fn sdk_credentials(&self) -> Option<&Arc<SdkCredentials>> {
        self.loaded_sdk().and_then(|sdk| sdk.credentials.as_ref())
    }

    /// The one credential source for every S3 store and AWS catalog client
    /// this storage builds: the configured static keys when both are
    /// present, otherwise the SDK chain when the storage was built with it,
    /// otherwise none.
    fn credential_source(&self) -> CredentialSource<'_> {
        if let Some(s3) = &self.config.s3
            && let (Some(key_id), Some(secret_access_key)) =
                (&s3.access_key_id, &s3.secret_access_key)
        {
            return CredentialSource::Static {
                key_id,
                secret_access_key,
            };
        }
        self.sdk_credentials()
            .map_or(CredentialSource::Unsigned, |bridge| {
                CredentialSource::SdkChain(Arc::clone(bridge))
            })
    }

    /// One whole-object GET, counted whatever its outcome: the bytes are the
    /// size the store reported, so a request that failed counts as a GET of
    /// zero bytes.
    async fn get_all(&self, l: &Located) -> object_store::Result<Bytes> {
        let result = l.store.get(&l.path).await;
        let size = result.as_ref().map_or(0, |result| result.meta.size);
        self.stats.record_get(ReadKind::Load, size);
        result?.bytes().await
    }

    /// The lookup read with its hedge. The first request races a timer of
    /// `delay`; when the timer wins, an identical second request is issued
    /// and counted, and whichever answers first wins. An answer that is an
    /// error does not win: the other request finishes and decides. Sets
    /// `hedged` before it issues a hedge, so a caller dropped while the
    /// pair is in flight still knows one was spent.
    async fn race(
        &self,
        l: &Located,
        range: Range<u64>,
        len: u64,
        delay: Option<Duration>,
        hedged: &mut bool,
    ) -> object_store::Result<Bytes> {
        let first = pin!(l.store.get_range(&l.path, range.clone()));
        let Some(delay) = delay else {
            return first.await;
        };
        let timer = pin!(tokio::time::sleep(delay));
        let first = match select(first, timer).await {
            Either::Left((answer, _timer)) => return answer,
            Either::Right(((), first)) => first,
        };
        self.stats.record_hedge(len);
        *hedged = true;
        let second = pin!(l.store.get_range(&l.path, range));
        match select(first, second).await {
            Either::Left((Ok(bytes), _)) | Either::Right((Ok(bytes), _)) => Ok(bytes),
            Either::Left((Err(error), second)) => {
                tracing::debug!(%error, "first lookup request failed; waiting for its hedge");
                second.await
            }
            Either::Right((Err(error), first)) => {
                tracing::debug!(%error, "hedge failed; waiting for the first lookup request");
                first.await
            }
        }
    }
}

/// One lookup GET's sample for its table's hedge window (RFD 013). A read
/// that answers records its elapsed time. A read dropped before it answers
/// (the page cache drops a fetch at its deadline, a canceled lookup drops
/// its reads) records only once its delay has passed or its hedge was
/// issued: such a sample is the slow one the delay most needs, and its
/// hedge would otherwise never count against the ceiling. A read dropped
/// sooner records nothing, since its elapsed time only bounds its latency
/// from below and would pull the percentile down.
struct WindowSample<'w> {
    /// The table's window.
    window: &'w HedgeWindow,
    /// When the first request was issued.
    started: Instant,
    /// The delay the read raced, `None` when it could not hedge.
    delay: Option<Duration>,
    /// The pages the read holds, at least one: the window records its
    /// elapsed time over them.
    pages: u32,
    /// Whether a hedge was issued; set by `race` before it issues one.
    hedged: bool,
    /// Whether the sample is in the window.
    recorded: bool,
}

impl WindowSample<'_> {
    /// Records the answered read and returns its elapsed time.
    fn record(&mut self) -> Duration {
        let elapsed = self.started.elapsed();
        self.window.record(elapsed / self.pages, self.hedged);
        self.recorded = true;
        elapsed
    }
}

impl Drop for WindowSample<'_> {
    fn drop(&mut self) {
        if self.recorded {
            return;
        }
        let elapsed = self.started.elapsed();
        if self.hedged || self.delay.is_some_and(|delay| elapsed >= delay) {
            self.window.record(elapsed / self.pages, self.hedged);
        }
    }
}

/// The SDK's defaults in `sdk`, loaded once. Concurrent callers share the
/// one load. The load's future is boxed: it is several times the size of
/// the rest of a read, and every future that locates a location would
/// carry it inline.
async fn load_sdk(sdk: &OnceCell<LoadedSdk>) -> &LoadedSdk {
    sdk.get_or_init(|| {
        Box::pin(async {
            LoadedSdk::new(
                aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .load()
                    .await,
            )
        })
    })
    .await
}

/// The bucket an `s3://` or `s3a://` prefix names, or `None` for a prefix
/// of another scheme.
fn s3_bucket(prefix: &str) -> Option<&str> {
    S3_SCHEMES
        .iter()
        .find_map(|scheme| prefix.strip_prefix(scheme))
        .map(|authority| authority.trim_end_matches(PATH_SEPARATOR))
}

/// Why `location`, as given, names no store this process builds, or
/// `None` when it names one: the check a configuration's locations get
/// before any is read. Stores registered on a `Storage` are not consulted,
/// since nothing in a configuration registers one.
pub(crate) fn unbuildable(location: &str) -> Option<LocationFault> {
    match split_location(location) {
        Err(fault) => Some(fault),
        Ok((prefix, _)) => (prefix != FILE_PREFIX && s3_bucket(&prefix).is_none())
            .then_some(LocationFault::NoStore),
    }
}

/// A location's `scheme://authority/` prefix and the rest. Every `file:`
/// URI that names a path on this host (`file_path`) and a bare absolute
/// path give the `file://` prefix with the absolute path as the rest; a
/// relative path is `LocationFault::NotAbsolute`.
pub(crate) fn split_location(
    location: &str,
) -> std::result::Result<(String, String), LocationFault> {
    if let Some(rest) = location.strip_prefix(FILE_SCHEME) {
        return file_path(rest).map(|path| (FILE_PREFIX.to_string(), path.to_string()));
    }
    if location.starts_with(PATH_SEPARATOR) {
        return Ok((FILE_PREFIX.to_string(), location.to_string()));
    }
    let Some((scheme, rest)) = location.split_once(SCHEME_SEPARATOR) else {
        return Err(LocationFault::NotAbsolute);
    };
    let (authority, key) = rest.split_once(PATH_SEPARATOR).unwrap_or((rest, ""));
    Ok((
        format!("{scheme}{SCHEME_SEPARATOR}{authority}{PATH_SEPARATOR}"),
        key.to_string(),
    ))
}

/// The absolute path a `file:` URI names on this host, from `rest`, the
/// text after its scheme: an authority of nothing or `localhost`, or none,
/// then an absolute path. Another host is `LocationFault::FileRemoteHost`,
/// a path that is not absolute `LocationFault::FileRelative`.
fn file_path(rest: &str) -> std::result::Result<&str, LocationFault> {
    let (host, path) = file_parts(rest);
    if host.is_some_and(|host| !host.is_empty() && host != LOCALHOST) {
        return Err(LocationFault::FileRemoteHost);
    }
    if path.starts_with(PATH_SEPARATOR) {
        Ok(path)
    } else {
        Err(LocationFault::FileRelative)
    }
}

/// The authority and the path of a `file:` URI, from `rest`, the text
/// after its scheme, neither checked: with `//`, the authority up to the
/// next `/` and the path from it; without, no authority and `rest` whole.
pub(crate) fn file_parts(rest: &str) -> (Option<&str>, &str) {
    let Some(after) = rest.strip_prefix(AUTHORITY_OPENING) else {
        return (None, rest);
    };
    match after.find(PATH_SEPARATOR) {
        Some(at) => (Some(&after[..at]), &after[at..]),
        None => (Some(after), ""),
    }
}

/// The bytes a range asks for. An inverted range asks for nothing; the store
/// refuses it on its own.
fn range_len(range: &Range<u64>) -> u64 {
    range.end.saturating_sub(range.start)
}

/// Runs one store request and records its latency under `kind` in `stats`
/// whatever the outcome: a request that failed took time too.
async fn observed<T>(
    stats: &IoStats,
    kind: ReadKind,
    request: impl Future<Output = object_store::Result<T>>,
) -> Result<T> {
    let started = Instant::now();
    let answer = request.await;
    stats.record_latency(kind, started.elapsed());
    answer.map_err(Error::from)
}

/// One lookup GET as the window remembers it.
#[derive(Clone, Copy, Debug)]
struct Sample {
    elapsed: Duration,
    hedged: bool,
}

/// How much of a full hedge window turns over between two computations of
/// its percentile: a sixteenth. Computing it copies and sorts the window,
/// so it runs once every `window / HEDGE_RECOMPUTE_SHARE` lookup GETs
/// (every sixteenth at the default window of 256, and every GET for a
/// window under thirty-two) rather than on every one. The delay a GET is
/// given then comes from a window that shares at least fifteen sixteenths
/// of its samples with the current one.
const HEDGE_RECOMPUTE_SHARE: usize = 16;

/// A sliding window of recent lookup-GET latencies, owned by one `Table`
/// because a table's data file size sets its first-byte latency (RFD 013).
/// The hedge delay is the configured percentile of the last `window`
/// samples, computed when the window fills and then again each time
/// `HEDGE_RECOMPUTE_SHARE`'s share of it has turned over, and no delay is
/// given while one more hedge would put the window's share of hedged
/// samples above the ceiling; that share is kept exact per sample. The
/// share counts recorded attempts: a hedge still in flight is not in the
/// window, so when a stall catches several lookups at once each may hedge
/// before any is recorded, and the share can overshoot the ceiling until
/// they are. `config` has passed `Config::parse`'s validation: `window` is
/// at least one, `percentile` is in `(0, 1]`, and `max_rate` in `[0, 1]`.
#[derive(Debug)]
pub(crate) struct HedgeWindow {
    config: HedgeConfig,
    /// Samples between two computations of the percentile: at least one.
    recompute_every: usize,
    samples: Mutex<Samples>,
}

/// What a `HedgeWindow` holds under its lock.
#[derive(Debug, Default)]
struct Samples {
    /// The last `window` samples, oldest first.
    ring: VecDeque<Sample>,
    /// How many of `ring` were hedged.
    hedged: usize,
    /// Samples recorded since `percentile` was computed.
    since_computed: usize,
    /// The percentile of `ring` when it was last computed; `None` until the
    /// window first fills.
    percentile: Option<Duration>,
    /// The elapsed times the percentile is computed over, kept between
    /// computations so each reuses its buffer.
    sorted: Vec<Duration>,
}

impl HedgeWindow {
    /// An empty window; `delay` gives nothing until `window` samples are in.
    /// The ring grows as samples arrive and never past `window`, so a large
    /// configured window costs nothing until it is used.
    pub(crate) fn new(config: &HedgeConfig) -> Self {
        Self {
            config: config.clone(),
            recompute_every: (config.window / HEDGE_RECOMPUTE_SHARE).max(1),
            samples: Mutex::new(Samples::default()),
        }
    }

    /// The delay after which a lookup GET is hedged, or `None` when no hedge
    /// may be issued: hedging is disabled, the window is not yet full (the
    /// configured window is the sample requirement), or one more hedged
    /// sample would put the share of hedged samples in the window above
    /// `max_rate` (RFD 013: while hedges would exceed the ceiling, none is
    /// issued). Otherwise the window's nearest-rank percentile as last
    /// computed (`record` says when): the value at 1-based rank
    /// `ceil(percentile * window)` in the sorted samples.
    pub(crate) fn delay(&self) -> Option<Duration> {
        if !self.config.enabled {
            return None;
        }
        let samples = self.samples.lock().unwrap_or_else(PoisonError::into_inner);
        if samples.ring.len() < self.config.window {
            return None;
        }
        let share_with_one_more = approx::ratio(
            approx::count(samples.hedged + 1),
            approx::count(samples.ring.len()),
        );
        if share_with_one_more > self.config.max_rate {
            return None;
        }
        samples.percentile
    }

    /// The delay after which a read of `pages` pages is hedged: `delay`
    /// times the pages, `None` where `delay` gives none or the product
    /// overflows. One page is `delay`.
    pub(crate) fn delay_for(&self, pages: u32) -> Option<Duration> {
        self.delay()
            .and_then(|delay| delay.checked_mul(pages.max(1)))
    }

    /// Records one lookup GET: how long it took and whether a hedge was
    /// issued for it. The oldest sample leaves once the window is full. The
    /// percentile is computed when the window fills and again once
    /// `recompute_every` samples have arrived since it last was.
    pub(crate) fn record(&self, elapsed: Duration, hedged: bool) {
        let mut guard = self.samples.lock().unwrap_or_else(PoisonError::into_inner);
        let samples = &mut *guard;
        if samples.ring.len() >= self.config.window
            && let Some(oldest) = samples.ring.pop_front()
            && oldest.hedged
        {
            samples.hedged -= 1;
        }
        samples.ring.push_back(Sample { elapsed, hedged });
        if hedged {
            samples.hedged += 1;
        }
        samples.since_computed += 1;
        let full = samples.ring.len() >= self.config.window;
        let due = samples.percentile.is_none() || samples.since_computed >= self.recompute_every;
        if full && due {
            samples.sorted.clear();
            samples
                .sorted
                .extend(samples.ring.iter().map(|sample| sample.elapsed));
            samples.sorted.sort_unstable();
            samples.percentile = nearest_rank(&samples.sorted, self.config.percentile);
            samples.since_computed = 0;
        }
    }
}

/// The nearest-rank percentile of `sorted`: the sample at 1-based rank
/// `ceil(percentile * n)` over `n` samples. The rank is found by walking
/// the sorted samples with their ranks and taking the first whose share of
/// the samples, `rank / n`, reaches the percentile, so no float is ever
/// cast to an integer. `percentile` is in `(0, 1]`, which `Config::parse`
/// holds `HedgeConfig::percentile` to, so the last sample's share of one
/// reaches it and the walk finds a sample whenever there is one; `None`
/// only when there are no samples.
fn nearest_rank(sorted: &[Duration], percentile: f64) -> Option<Duration> {
    let n = approx::count(sorted.len());
    sorted
        .iter()
        .zip(1u64..)
        .find(|(_, rank)| approx::ratio(*rank, n) >= percentile)
        .map(|(sample, _)| *sample)
}

/// The AWS SDK's defaults as a `Storage` loaded them, with the one
/// credential bridge over their chain.
#[derive(Debug)]
struct LoadedSdk {
    /// The defaults: the region and the credential chain.
    config: SdkConfig,
    /// The bridge over `config`'s chain, built once with it; `None` when
    /// the chain resolved no provider.
    credentials: Option<Arc<SdkCredentials>>,
}

impl LoadedSdk {
    /// `config`, with the bridge over its chain.
    fn new(config: SdkConfig) -> Self {
        let credentials = config
            .credentials_provider()
            .map(|provider| Arc::new(SdkCredentials::new(provider)));
        Self {
            config,
            credentials,
        }
    }
}

/// The last credential the SDK chain resolved, in the form `object_store`
/// signs with and as the chain answered it, which the SDK's own catalog
/// clients sign with, and when it stops being handed out.
#[derive(Debug)]
struct CachedCredential {
    credential: Arc<AwsCredential>,
    resolved: Credentials,
    /// `None` for a credential without an expiry, which is usable indefinitely.
    usable_until: Option<SystemTime>,
}

impl CachedCredential {
    /// The chain's answer `resolved` at `now`. It is handed out until
    /// `CREDENTIAL_REFRESH_MARGIN` before its expiry, or for
    /// `CREDENTIAL_MIN_REUSE` when the expiry is nearer than that, and never
    /// past the expiry itself.
    fn new(resolved: Credentials, now: SystemTime) -> Self {
        let credential = Arc::new(AwsCredential {
            key_id: resolved.access_key_id().to_string(),
            secret_key: resolved.secret_access_key().to_string(),
            token: resolved.session_token().map(str::to_string),
        });
        let usable_until = resolved.expiry().map(|expiry| {
            let floor = now
                .checked_add(CREDENTIAL_MIN_REUSE)
                .map_or(expiry, |floor| floor.min(expiry));
            expiry
                .checked_sub(CREDENTIAL_REFRESH_MARGIN)
                .map_or(floor, |before_margin| before_margin.max(floor))
        });
        Self {
            credential,
            resolved,
            usable_until,
        }
    }

    /// Usable while `now` is before `usable_until`.
    fn is_fresh(&self, now: SystemTime) -> bool {
        self.usable_until.is_none_or(|until| until > now)
    }
}

/// The AWS SDK's credential chain presented to `object_store` and to the
/// SDK's own catalog clients, with the resolved credential cached until
/// `CREDENTIAL_REFRESH_MARGIN` before its expiry, or for
/// `CREDENTIAL_MIN_REUSE` when the chain answered with one already inside
/// that margin. Without the cache every GET would run the chain, and a
/// chain backed by `credential_process` spawns a process per run (RFD 003).
/// The cache lives behind a tokio mutex held across the chain's own await,
/// so concurrent callers that all find it stale collapse into one refresh:
/// the first to take the lock refreshes, and each of the rest finds a fresh
/// credential when its turn comes.
#[derive(Debug)]
pub(crate) struct SdkCredentials {
    provider: SharedCredentialsProvider,
    cached: tokio::sync::Mutex<Option<CachedCredential>>,
}

impl SdkCredentials {
    fn new(provider: SharedCredentialsProvider) -> Self {
        Self {
            provider,
            cached: tokio::sync::Mutex::new(None),
        }
    }

    /// `pick` of the cached credential while it is fresh, otherwise of the
    /// chain's answer, cached. The lock is held across the chain's await on
    /// purpose.
    async fn fresh<T>(
        &self,
        pick: impl FnOnce(&CachedCredential) -> T,
    ) -> std::result::Result<T, CredentialsError> {
        let mut cached = self.cached.lock().await;
        if let Some(entry) = cached
            .as_ref()
            .filter(|entry| entry.is_fresh(time::system_now()))
        {
            return Ok(pick(entry));
        }
        let resolved = self.provider.provide_credentials().await?;
        Ok(pick(cached.insert(CachedCredential::new(
            resolved,
            time::system_now(),
        ))))
    }

    /// The credential an S3 store signs with.
    async fn current(&self) -> object_store::Result<Arc<AwsCredential>> {
        self.fresh(|entry| Arc::clone(&entry.credential))
            .await
            .map_err(|source| object_store::Error::Generic {
                store: S3_STORE_NAME,
                source: Box::new(source),
            })
    }
}

impl ProvideCredentials for SdkCredentials {
    /// The credential an AWS catalog client signs with, from the same
    /// cache as every S3 store's.
    fn provide_credentials<'a>(&'a self) -> provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        provider::future::ProvideCredentials::new(self.fresh(|entry| entry.resolved.clone()))
    }
}

impl CredentialProvider for SdkCredentials {
    type Credential = AwsCredential;

    fn get_credential<'life0, 'async_trait>(
        &'life0 self,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<Arc<AwsCredential>>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(self.current())
    }
}

/// The prefix the tests register in-memory stores under, and the in-memory
/// store the tests that read objects through a `Storage` build on, from a
/// single `FileHandle` to a whole generation.
#[cfg(test)]
pub(crate) mod testing {
    use std::fmt;
    use std::future::Future;
    use std::io::{Read as _, Write as _};
    use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
    use std::ops::Range;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use aws_config::SdkConfig;
    use aws_credential_types::Credentials;
    use aws_credential_types::provider::{ProvideCredentials, future};
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
        ObjectStore, ObjectStoreExt as _, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use tokio::sync::watch;

    use super::{HedgeWindow, LoadedSdk, Storage};
    use crate::config::StorageConfig;
    use crate::parquet_io::FileHandle;

    /// The synthetic prefix an in-memory store is registered under.
    pub(crate) const MEM_PREFIX: &str = "mem://w/";

    /// How long a stalled store or catalog takes to answer: an hour, longer
    /// than any test runs, so to a test it never answers.
    pub(crate) const STALL: Duration = Duration::from_secs(3600);

    /// How long a test waits for work that a deadline should end: far above
    /// any deadline a test sets, so only a deadline that is missing reaches
    /// it.
    pub(crate) const DEADLINE_GUARD: Duration = Duration::from_secs(10);

    /// An in-memory store registered under `MEM_PREFIX` on a fresh storage.
    pub(crate) struct MemStore {
        pub(crate) storage: Arc<Storage>,
        pub(crate) store: Arc<InMemory>,
    }

    impl MemStore {
        pub(crate) fn new() -> Self {
            let store = Arc::new(InMemory::new());
            let storage = Arc::new(Storage::new(StorageConfig::default()));
            let registered: Arc<dyn ObjectStore> = Arc::<InMemory>::clone(&store);
            storage.register(MEM_PREFIX, registered);
            Self { storage, store }
        }

        /// Writes `bytes` at `location`, which is under `MEM_PREFIX`.
        pub(crate) async fn put(&self, location: &str, bytes: impl Into<Bytes>) {
            let key = location
                .strip_prefix(MEM_PREFIX)
                .expect("a test location is under the in-memory prefix");
            self.store
                .put(&Path::from(key), PutPayload::from(bytes.into()))
                .await
                .unwrap();
        }

        /// Writes `bytes` at `location` and opens a handle over them that
        /// reads through `window`.
        pub(crate) async fn open(
            &self,
            location: &str,
            bytes: Bytes,
            window: Arc<HedgeWindow>,
        ) -> Arc<FileHandle> {
            let size = crate::approx::count(bytes.len());
            self.put(location, bytes).await;
            Arc::new(
                FileHandle::open(Arc::clone(&self.storage), location, size, window)
                    .await
                    .unwrap(),
            )
        }
    }

    /// A store over `inner` that counts the GETs in flight at once and the
    /// most there have been since the last `fill_to`, so a test can see how
    /// many reads a caller runs together. After `fill_to(n)` every GET
    /// waits until `n` GETs are in flight at once, and from then on every
    /// GET passes: a caller that runs `n` reads together is seen to on any
    /// machine, however slow, and one that runs fewer never gets past. It
    /// keeps the byte range of every GET, so a test sees what a read
    /// fetched.
    #[derive(Debug)]
    pub(crate) struct Gauged {
        inner: Arc<dyn ObjectStore>,
        in_flight: AtomicUsize,
        high_water: AtomicUsize,
        /// The byte range of every GET since the last `take_ranges`.
        ranges: Mutex<Vec<Range<u64>>>,
        /// How many GETs must be in flight at once before the gate opens.
        fill: AtomicUsize,
        /// Whether GETs pass: false from `fill_to` until `fill` GETs are in
        /// flight together.
        open: watch::Sender<bool>,
    }

    impl Gauged {
        /// A store over `inner` whose GETs pass until `fill_to` is called.
        pub(crate) fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                in_flight: AtomicUsize::new(0),
                high_water: AtomicUsize::new(0),
                ranges: Mutex::new(Vec::new()),
                fill: AtomicUsize::new(0),
                open: watch::Sender::new(true),
            }
        }

        /// The most GETs in flight at once since the last `fill_to`.
        pub(crate) fn high_water(&self) -> usize {
            self.high_water.load(Ordering::SeqCst)
        }

        /// Starts the high-water mark again from the GETs in flight now and
        /// holds every GET until `fill` of them are in flight at once.
        pub(crate) fn fill_to(&self, fill: usize) {
            self.high_water
                .store(self.in_flight.load(Ordering::SeqCst), Ordering::SeqCst);
            self.fill.store(fill, Ordering::SeqCst);
            self.open.send_replace(false);
        }

        /// Holds every GET from here until `release`, however many are in
        /// flight: a fetch a test starts cannot finish before it looks.
        pub(crate) fn hold(&self) {
            self.fill_to(usize::MAX);
        }

        /// Lets the GETs `hold` holds pass.
        pub(crate) fn release(&self) {
            self.open.send_replace(true);
        }

        /// The byte ranges the GETs since the last call asked for, in the
        /// order they were issued.
        pub(crate) fn take_ranges(&self) -> Vec<Range<u64>> {
            std::mem::take(&mut *self.ranges.lock().unwrap())
        }
    }

    /// One GET of a `Gauged` store counted in flight, taken out of the
    /// count when dropped: a GET that ends, and one dropped while it waits
    /// at the gate or on the inner store, as a timeout, a hedge's loser,
    /// or a dropped caller drops it.
    struct InFlight<'a>(&'a AtomicUsize);

    impl Drop for InFlight<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl fmt::Display for Gauged {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Gauged({})", self.inner)
        }
    }

    impl ObjectStore for Gauged {
        fn put_opts<'life0, 'life1, 'async_trait>(
            &'life0 self,
            location: &'life1 Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> Pin<Box<dyn Future<Output = object_store::Result<PutResult>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.put_opts(location, payload, opts)
        }

        fn put_multipart_opts<'life0, 'life1, 'async_trait>(
            &'life0 self,
            location: &'life1 Path,
            opts: PutMultipartOptions,
        ) -> Pin<
            Box<
                dyn Future<Output = object_store::Result<Box<dyn MultipartUpload>>>
                    + Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.put_multipart_opts(location, opts)
        }

        fn get_opts<'life0, 'life1, 'async_trait>(
            &'life0 self,
            location: &'life1 Path,
            options: GetOptions,
        ) -> Pin<Box<dyn Future<Output = object_store::Result<GetResult>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move {
                if let Some(GetRange::Bounded(range)) = &options.range {
                    self.ranges.lock().unwrap().push(range.clone());
                }
                let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                let _in_flight = InFlight(&self.in_flight);
                self.high_water.fetch_max(now, Ordering::SeqCst);
                if now >= self.fill.load(Ordering::SeqCst) {
                    self.open.send_replace(true);
                }
                self.open
                    .subscribe()
                    .wait_for(|open| *open)
                    .await
                    .expect("the store holds the sender");
                self.inner.get_opts(location, options).await
            })
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_delimiter<'life0, 'life1, 'async_trait>(
            &'life0 self,
            prefix: Option<&'life1 Path>,
        ) -> Pin<Box<dyn Future<Output = object_store::Result<ListResult>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.list_with_delimiter(prefix)
        }

        fn copy_opts<'life0, 'life1, 'life2, 'async_trait>(
            &'life0 self,
            from: &'life1 Path,
            to: &'life2 Path,
            options: CopyOptions,
        ) -> Pin<Box<dyn Future<Output = object_store::Result<()>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.copy_opts(from, to, options)
        }
    }

    /// What the local HTTP endpoint does with every request it reads.
    #[derive(Clone, Debug)]
    pub(crate) enum Reply {
        /// Answers with this status line and an empty body, then closes.
        Status(&'static str),
        /// Answers with this status line and this JSON body, then closes.
        Json {
            /// The status line.
            status: &'static str,
            /// The body.
            body: String,
        },
        /// Keeps the connection open and never answers, as a wedged store
        /// or catalog does.
        Never,
    }

    /// An HTTP endpoint on a loopback port, served by a thread of its own
    /// so it answers whatever runtime the test runs on, that counts the
    /// requests it reads and keeps each one's head. Every request is read
    /// whole, its body by its `content-length`, before it is answered, so
    /// the client never meets a connection closed under a body it is still
    /// sending. The thread outlives the test; the process ends it.
    pub(crate) struct LocalHttp {
        /// The endpoint's address.
        pub(crate) addr: SocketAddr,
        requests: Arc<AtomicUsize>,
        reply: Arc<Mutex<Reply>>,
        heads: Arc<Mutex<Vec<String>>>,
    }

    impl LocalHttp {
        /// Binds a loopback port and serves every connection with `reply`
        /// until `set_reply` names another.
        pub(crate) fn start(reply: Reply) -> Self {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
            let addr = listener.local_addr().unwrap();
            let requests = Arc::new(AtomicUsize::new(0));
            let reply = Arc::new(Mutex::new(reply));
            let heads = Arc::new(Mutex::new(Vec::new()));
            let (counted, replies, kept) = (
                Arc::clone(&requests),
                Arc::clone(&reply),
                Arc::clone(&heads),
            );
            std::thread::spawn(move || {
                let mut held = Vec::new();
                for stream in listener.incoming() {
                    let mut stream = stream.unwrap();
                    let request = read_request(&mut stream);
                    kept.lock().unwrap().push(request);
                    counted.fetch_add(1, Ordering::SeqCst);
                    let reply = replies.lock().unwrap().clone();
                    match reply {
                        Reply::Status(line) => write!(
                            stream,
                            "HTTP/1.1 {line}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                        )
                        .unwrap(),
                        Reply::Json { status, body } => write!(
                            stream,
                            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .unwrap(),
                        Reply::Never => held.push(stream),
                    }
                }
            });
            Self {
                addr,
                requests,
                reply,
                heads,
            }
        }

        /// `http://` and the endpoint's address.
        pub(crate) fn url(&self) -> String {
            format!("http://{}", self.addr)
        }

        /// The requests read so far.
        pub(crate) fn requests(&self) -> usize {
            self.requests.load(Ordering::SeqCst)
        }

        /// What every request read from now on is answered with.
        pub(crate) fn set_reply(&self, reply: Reply) {
            *self.reply.lock().unwrap() = reply;
        }

        /// The head of every request read so far, in order: the request
        /// line and the headers, as sent.
        pub(crate) fn heads(&self) -> Vec<String> {
            self.heads.lock().unwrap().clone()
        }
    }

    /// The header that gives a request body's length, in the lowercase
    /// HTTP/1.1 header names compare equal to.
    const CONTENT_LENGTH: &str = "content-length";

    /// Reads one request whole: its head, through the blank line that ends
    /// it, or to the end of the stream, then as many body bytes as its
    /// `content-length` names. Returns the head.
    fn read_request(stream: &mut TcpStream) -> String {
        let end_of_head = b"\r\n\r\n";
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(end_of_head) && stream.read(&mut byte).unwrap() == 1 {
            head.push(byte[0]);
        }
        let head = String::from_utf8_lossy(&head).into_owned();
        let body_len = head
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(name, _)| name.trim().eq_ignore_ascii_case(CONTENT_LENGTH))
            .map_or(0, |(_, value)| value.trim().parse::<usize>().unwrap());
        let mut body = vec![0u8; body_len];
        stream.read_exact(&mut body).unwrap();
        head
    }

    /// A credential provider that counts its calls and yields once before
    /// answering, so concurrent callers overlap and a refresh that is not
    /// coalesced shows up as more than one call. It answers from `answers`
    /// in call order and repeats the last.
    #[derive(Debug)]
    pub(crate) struct CountingProvider {
        pub(crate) answers: Vec<Credentials>,
        pub(crate) calls: Arc<AtomicUsize>,
    }

    impl ProvideCredentials for CountingProvider {
        fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
        where
            Self: 'a,
        {
            future::ProvideCredentials::new(async move {
                let call = self.calls.fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
                let answer = self
                    .answers
                    .get(call)
                    .or_else(|| self.answers.last())
                    .unwrap();
                Ok(answer.clone())
            })
        }
    }

    /// A storage built with the SDK's defaults, `sdk` set as though they
    /// had just loaded, so an S3 store or AWS catalog client it builds
    /// signs through the bridge over `sdk`'s chain without reading any AWS
    /// configuration from the host; with an in-memory store under
    /// `MEM_PREFIX`.
    pub(crate) fn with_loaded_sdk(config: StorageConfig, sdk: SdkConfig) -> MemStore {
        let storage = Storage::with_sdk_defaults(config);
        storage
            .sdk
            .as_ref()
            .expect("built with the SDK's defaults")
            .set(LoadedSdk::new(sdk))
            .expect("nothing has loaded them yet");
        let store = Arc::new(InMemory::new());
        let registered: Arc<dyn ObjectStore> = Arc::<InMemory>::clone(&store);
        storage.register(MEM_PREFIX, registered);
        MemStore {
            storage: Arc::new(storage),
            store,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use futures::future::join_all;
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::throttle::{ThrottleConfig, ThrottledStore};
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use tokio::sync::{Notify, watch};

    use super::testing::{CountingProvider, Gauged, LocalHttp, MEM_PREFIX, Reply};
    use super::*;
    use crate::config::RewriteRule;

    /// The key of the one test object under the prefix.
    const OBJECT_KEY: &str = "db/t/f.bin";
    /// The bucket the S3 client tests address.
    const BUCKET: &str = "bucket-a";
    /// The status line of a store that is up and refusing work, which the
    /// S3 client retries.
    const UNAVAILABLE: &str = "503 Service Unavailable";
    /// A key nothing was written under.
    const MISSING_KEY: &str = "db/t/absent.bin";
    /// The test object's bytes.
    const PAYLOAD: &[u8] = b"abcdef";
    /// A range inside the object, two bytes long.
    const INNER_RANGE: Range<u64> = 1..3;
    /// The bytes `INNER_RANGE` covers.
    const INNER_BYTES: &[u8] = b"bc";
    /// A one-byte range at the start of the object.
    const FIRST_BYTE: Range<u64> = 0..1;
    /// A one-byte range further in.
    const FOURTH_BYTE: Range<u64> = 3..4;
    /// A latency sample below any store delay a test uses, so a window full
    /// of them makes a slow read hedge.
    const TINY: Duration = Duration::from_millis(1);
    /// A latency sample above any store delay a test uses, so a window full
    /// of them never makes a read hedge.
    const HUGE: Duration = Duration::from_secs(10);
    /// The wait a throttled store adds to each GET: above `TINY`.
    const SLOW: Duration = Duration::from_millis(10);
    /// Latency samples for the percentile tests, out of order on purpose.
    const SAMPLE_10MS: Duration = Duration::from_millis(10);
    const SAMPLE_20MS: Duration = Duration::from_millis(20);
    const SAMPLE_30MS: Duration = Duration::from_millis(30);
    const SAMPLE_40MS: Duration = Duration::from_millis(40);
    /// Below one rank in four: the smallest sample.
    const TENTH: f64 = 0.1;
    /// The median: rank 2 of 4.
    const MEDIAN: f64 = 0.5;
    /// The third quartile: rank 3 of 4.
    const THIRD_QUARTILE: f64 = 0.75;
    /// The default percentile: rank `ceil(3.8)`, the fourth of 4.
    const P95: f64 = 0.95;
    /// The top of the range: the last sample.
    const TOP: f64 = 1.0;
    /// A window of four samples, so every percentile above has an exact rank.
    const FOUR: usize = 4;
    /// A window of two samples: one hedged sample in two reaches a ceiling
    /// of one half, and one read replaces half the window.
    const TWO: usize = 2;
    /// A ceiling one hedged sample in four reaches.
    const ONE_IN_FOUR: f64 = 0.25;
    /// A ceiling between one hedged sample in four and two: a second hedge
    /// would exceed it where the first did not.
    const THREE_IN_TEN: f64 = 0.3;
    /// A ceiling one hedged sample in two reaches.
    const HALF: f64 = 0.5;
    /// Callers racing for one refresh.
    const CONCURRENT_CALLERS: usize = 16;
    /// A credential lifetime well beyond the refresh margin.
    const ONE_HOUR: Duration = Duration::from_secs(3600);
    /// The step the freshness tests take across a boundary.
    const ONE_SECOND: Duration = Duration::from_secs(1);
    /// The access key id a test credential carries.
    const TEST_KEY_ID: &str = "AKIAEXAMPLE";
    /// The secret a test credential carries.
    const TEST_SECRET: &str = "secret-example";
    /// The session token a test credential carries.
    const TEST_TOKEN: &str = "token-example";
    /// The provider name a test credential carries.
    const TEST_PROVIDER: &str = "test";

    fn payload_len() -> u64 {
        approx::count(PAYLOAD.len())
    }

    fn range_bytes(range: &Range<u64>) -> u64 {
        range.end - range.start
    }

    fn hedge_config(percentile: f64, window: usize, max_rate: f64) -> HedgeConfig {
        HedgeConfig {
            enabled: true,
            percentile,
            window,
            max_rate,
        }
    }

    fn window_with(config: &HedgeConfig, samples: &[(Duration, bool)]) -> HedgeWindow {
        let window = HedgeWindow::new(config);
        for &(elapsed, hedged) in samples {
            window.record(elapsed, hedged);
        }
        window
    }

    /// A window that is full of `TINY` samples, none hedged, so a read of a
    /// store slower than `TINY` hedges at once.
    fn eager_window() -> HedgeWindow {
        window_with(
            &hedge_config(MEDIAN, TWO, HALF),
            &[(TINY, false), (TINY, false)],
        )
    }

    /// A storage with `store` registered under `MEM_PREFIX` and the test
    /// object written into it, plus the object's location.
    async fn storage_with(store: Arc<dyn ObjectStore>) -> (Storage, Located) {
        store
            .put(&Path::from(OBJECT_KEY), PutPayload::from_static(PAYLOAD))
            .await
            .unwrap();
        let storage = Storage::new(StorageConfig::default());
        storage.register(MEM_PREFIX, store);
        let located = storage
            .locate_loaded(&format!("{MEM_PREFIX}{OBJECT_KEY}"))
            .unwrap();
        (storage, located)
    }

    fn slow_store() -> Arc<dyn ObjectStore> {
        Arc::new(ThrottledStore::new(
            InMemory::new(),
            ThrottleConfig {
                wait_get_per_call: SLOW,
                ..ThrottleConfig::default()
            },
        ))
    }

    /// A hedged read of a scripted store, bounded so that a hedge that never
    /// comes fails the test instead of hanging it.
    async fn hedged_read(
        storage: &Storage,
        located: &Located,
        window: &HedgeWindow,
    ) -> Result<Bytes> {
        tokio::time::timeout(
            HUGE,
            storage.read_range_hedged(located, INNER_RANGE, window),
        )
        .await
        .expect("the hedge releases the stalled request")
    }

    /// The location error a `locate` refused with.
    fn locate_violation(result: Result<Located>) -> LocationError {
        let Err(Error::Location {
            table: None,
            source,
        }) = result
        else {
            panic!("expected a location error naming no table");
        };
        source
    }

    /// When one GET of a `Scripted` store answers, relative to the other GET
    /// of a hedged pair. The test decides which of the pair finishes first
    /// without depending on the clock.
    #[derive(Clone, Copy, Debug)]
    enum Gate {
        /// Answer at once.
        Pass,
        /// Wait until a later GET releases this one, then answer and signal
        /// that it has.
        Stall,
        /// Release the stalled GET and answer at once, before it does.
        Release,
        /// Release the stalled GET and answer only after it has.
        ReleaseAndWait,
    }

    /// Whether a GET of a `Scripted` store answers from its in-memory store
    /// or fails.
    #[derive(Clone, Copy, Debug)]
    enum Outcome {
        Answer,
        Fail,
    }

    /// One GET of a `Scripted` store, by call order.
    #[derive(Clone, Copy, Debug)]
    struct Step {
        gate: Gate,
        outcome: Outcome,
    }

    /// A GET the script does not cover: it answers at once.
    const PASS: Step = Step {
        gate: Gate::Pass,
        outcome: Outcome::Answer,
    };
    /// The first GET of a hedged pair, held until the hedge arrives.
    const STALL: Step = Step {
        gate: Gate::Stall,
        outcome: Outcome::Answer,
    };
    /// The first GET of a hedged pair, held until the hedge arrives and
    /// then failing.
    const STALL_AND_FAIL: Step = Step {
        gate: Gate::Stall,
        outcome: Outcome::Fail,
    };
    /// A hedge that answers before the request it released.
    const RELEASE: Step = Step {
        gate: Gate::Release,
        outcome: Outcome::Answer,
    };
    /// A hedge that fails before the request it released answers.
    const RELEASE_AND_FAIL: Step = Step {
        gate: Gate::Release,
        outcome: Outcome::Fail,
    };
    /// A hedge that answers after the request it released.
    const RELEASE_AND_WAIT: Step = Step {
        gate: Gate::ReleaseAndWait,
        outcome: Outcome::Answer,
    };

    /// The store name a scripted failure carries.
    const SCRIPTED_STORE_NAME: &str = "scripted";
    /// The message a scripted failure carries; nothing reads it.
    const SCRIPTED_FAILURE: &str = "scripted failure";

    /// Delegates to an in-memory store, with each GET's timing and outcome
    /// taken from a script by call order; GETs past the script's end answer
    /// at once.
    #[derive(Debug)]
    struct Scripted {
        inner: InMemory,
        script: Vec<Step>,
        /// GETs made so far, which `arrived` watches.
        calls: watch::Sender<usize>,
        release: Notify,
        done: Notify,
    }

    impl Scripted {
        fn new(script: &[Step]) -> Self {
            Self {
                inner: InMemory::new(),
                script: script.to_vec(),
                calls: watch::Sender::new(0),
                release: Notify::new(),
                done: Notify::new(),
            }
        }

        /// The step for the next GET.
        fn step(&self) -> Step {
            let mut call = 0;
            self.calls.send_modify(|calls| {
                call = *calls;
                *calls += 1;
            });
            self.script.get(call).copied().unwrap_or(PASS)
        }

        /// Resolves once `calls` GETs have been made, whether or not any
        /// has answered.
        async fn arrived(&self, calls: usize) {
            self.calls
                .subscribe()
                .wait_for(|made| *made >= calls)
                .await
                .expect("the store outlives the wait");
        }
    }

    fn scripted_failure() -> object_store::Error {
        object_store::Error::Generic {
            store: SCRIPTED_STORE_NAME,
            source: SCRIPTED_FAILURE.into(),
        }
    }

    impl fmt::Display for Scripted {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Scripted({})", self.inner)
        }
    }

    impl ObjectStore for Scripted {
        fn put_opts<'life0, 'life1, 'async_trait>(
            &'life0 self,
            location: &'life1 Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> Pin<Box<dyn Future<Output = object_store::Result<PutResult>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.put_opts(location, payload, opts)
        }

        fn put_multipart_opts<'life0, 'life1, 'async_trait>(
            &'life0 self,
            location: &'life1 Path,
            opts: PutMultipartOptions,
        ) -> Pin<
            Box<
                dyn Future<Output = object_store::Result<Box<dyn MultipartUpload>>>
                    + Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.put_multipart_opts(location, opts)
        }

        fn get_opts<'life0, 'life1, 'async_trait>(
            &'life0 self,
            location: &'life1 Path,
            options: GetOptions,
        ) -> Pin<Box<dyn Future<Output = object_store::Result<GetResult>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move {
                let step = self.step();
                match step.gate {
                    Gate::Pass => {}
                    Gate::Stall => self.release.notified().await,
                    Gate::Release => self.release.notify_one(),
                    Gate::ReleaseAndWait => {
                        self.release.notify_one();
                        self.done.notified().await;
                    }
                }
                let answer = match step.outcome {
                    Outcome::Answer => self.inner.get_opts(location, options).await,
                    Outcome::Fail => Err(scripted_failure()),
                };
                if matches!(step.gate, Gate::Stall) {
                    self.done.notify_one();
                }
                answer
            })
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_delimiter<'life0, 'life1, 'async_trait>(
            &'life0 self,
            prefix: Option<&'life1 Path>,
        ) -> Pin<Box<dyn Future<Output = object_store::Result<ListResult>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.list_with_delimiter(prefix)
        }

        fn copy_opts<'life0, 'life1, 'life2, 'async_trait>(
            &'life0 self,
            from: &'life1 Path,
            to: &'life2 Path,
            options: CopyOptions,
        ) -> Pin<Box<dyn Future<Output = object_store::Result<()>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            Self: 'async_trait,
        {
            self.inner.copy_opts(from, to, options)
        }
    }

    /// An `SdkCredentials` over a counting provider of `answers`, and the
    /// counter.
    fn counting_credentials(answers: Vec<Credentials>) -> (SdkCredentials, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = CountingProvider {
            answers,
            calls: Arc::clone(&calls),
        };
        (
            SdkCredentials::new(SharedCredentialsProvider::new(provider)),
            calls,
        )
    }

    fn test_credentials(expiry: Option<SystemTime>) -> Credentials {
        Credentials::new(
            TEST_KEY_ID,
            TEST_SECRET,
            Some(TEST_TOKEN.to_string()),
            expiry,
            TEST_PROVIDER,
        )
    }

    /// A cached credential resolved at `now` with `expiry`; its contents do
    /// not matter to the freshness rule.
    fn cached_at(now: SystemTime, expiry: Option<SystemTime>) -> CachedCredential {
        CachedCredential::new(test_credentials(expiry), now)
    }

    #[test]
    fn test_split_location_s3_uri() {
        assert_eq!(
            split_location("s3://bucket-a/x/y.parquet").unwrap(),
            ("s3://bucket-a/".to_string(), "x/y.parquet".to_string())
        );
    }

    #[test]
    fn test_split_location_s3a_uri() {
        assert_eq!(
            split_location("s3a://bucket-a/x/y.parquet").unwrap(),
            ("s3a://bucket-a/".to_string(), "x/y.parquet".to_string())
        );
    }

    #[test]
    fn test_split_location_bucket_root_has_empty_rest() {
        assert_eq!(
            split_location("s3://bucket-a").unwrap(),
            ("s3://bucket-a/".to_string(), String::new())
        );
    }

    #[test]
    fn test_split_location_file_uri() {
        assert_eq!(
            split_location("file:///tmp/w/t").unwrap(),
            (FILE_PREFIX.to_string(), "/tmp/w/t".to_string())
        );
    }

    #[test]
    fn test_split_location_hadoop_file_uri_without_authority() {
        assert_eq!(
            split_location("file:/tmp/w/t").unwrap(),
            (FILE_PREFIX.to_string(), "/tmp/w/t".to_string())
        );
    }

    /// `file://localhost/` names this host as an empty authority does
    /// (RFC 8089); another host, and a relative path after `file:`, name
    /// no path here.
    #[test]
    fn test_split_location_file_uri_forms() {
        for location in [
            "file:///tmp/w/t",
            "file://localhost/tmp/w/t",
            "file:/tmp/w/t",
        ] {
            assert_eq!(
                split_location(location),
                Ok((FILE_PREFIX.to_string(), "/tmp/w/t".to_string())),
                "{location}"
            );
        }
        let refused = [
            ("file://host-b/tmp/w/t", LocationFault::FileRemoteHost),
            ("file:tmp/w/t", LocationFault::FileRelative),
            ("file:", LocationFault::FileRelative),
            ("file://", LocationFault::FileRelative),
            ("file://localhost", LocationFault::FileRelative),
        ];
        for (location, fault) in refused {
            assert_eq!(split_location(location), Err(fault), "{location}");
        }
    }

    #[tokio::test]
    async fn test_read_all_reads_a_file_localhost_uri() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f.bin");
        std::fs::write(&file, PAYLOAD).unwrap();
        let storage = Storage::new(StorageConfig::default());
        let location = format!("file://localhost{}", file.to_str().unwrap());
        assert_eq!(&storage.read_all(&location).await.unwrap()[..], PAYLOAD);
    }

    #[test]
    fn test_split_location_bare_absolute_path() {
        assert_eq!(
            split_location("/tmp/w/t").unwrap(),
            (FILE_PREFIX.to_string(), "/tmp/w/t".to_string())
        );
    }

    #[test]
    fn test_split_location_relative_path_refused() {
        assert_eq!(
            split_location("relative/path"),
            Err(LocationFault::NotAbsolute)
        );
    }

    #[test]
    fn test_locate_unknown_scheme_refused() {
        let storage = Storage::new(StorageConfig::default());
        assert_eq!(
            locate_violation(storage.locate_loaded("gs://bucket-a/x")),
            LocationError {
                location: "gs://bucket-a/x".to_string(),
                fault: LocationFault::NoStore,
            }
        );
    }

    /// A location builds a store when it is `file://`, `s3://`, or
    /// `s3a://`, or a bare absolute path; any other URI names none and a
    /// relative path names nothing at all.
    #[test]
    fn test_unbuildable_names_what_no_built_store_serves() {
        for location in [
            "s3://bucket-a/x",
            "s3a://bucket-a/x",
            "file:///data/x",
            "file:/data/x",
            "file://localhost/data/x",
            "/data/x",
        ] {
            assert_eq!(unbuildable(location), None, "{location}");
        }
        assert_eq!(
            unbuildable("file:data/x"),
            Some(LocationFault::FileRelative)
        );
        assert_eq!(unbuildable("gs://bucket-a/x"), Some(LocationFault::NoStore));
        assert_eq!(unbuildable(MEM_PREFIX), Some(LocationFault::NoStore));
        assert_eq!(unbuildable("db/x"), Some(LocationFault::NotAbsolute));
    }

    #[test]
    fn test_locate_reports_the_rewritten_location() {
        let storage = Storage::new(StorageConfig {
            rewrite: vec![RewriteRule {
                from: "gs://bucket-a/".to_string(),
                to: "mem://none/".to_string(),
            }],
            ..StorageConfig::default()
        });
        assert_eq!(
            locate_violation(storage.locate_loaded("gs://bucket-a/x")),
            LocationError {
                location: "mem://none/x".to_string(),
                fault: LocationFault::NoStore,
            }
        );
        assert_eq!(
            locate_violation(storage.locate_loaded("relative/x")),
            LocationError {
                location: "relative/x".to_string(),
                fault: LocationFault::NotAbsolute,
            }
        );
    }

    #[test]
    fn test_locate_applies_first_matching_rewrite_only() {
        let storage = Storage::new(StorageConfig {
            rewrite: vec![
                RewriteRule {
                    from: "s3://old-warehouse/".to_string(),
                    to: "mem://a/".to_string(),
                },
                RewriteRule {
                    from: "s3://old-warehouse/deep/".to_string(),
                    to: "mem://b/".to_string(),
                },
                RewriteRule {
                    from: "mem://a/".to_string(),
                    to: "mem://c/".to_string(),
                },
            ],
            ..StorageConfig::default()
        });
        let a: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let b: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let c: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        storage.register("mem://a/", Arc::clone(&a));
        storage.register("mem://b/", Arc::clone(&b));
        storage.register("mem://c/", Arc::clone(&c));

        let located = storage.locate_loaded("s3://old-warehouse/deep/x").unwrap();
        assert!(Arc::ptr_eq(&located.store, &a), "the first rule wins");
        assert!(
            !Arc::ptr_eq(&located.store, &b),
            "a later, longer rule loses"
        );
        assert!(
            !Arc::ptr_eq(&located.store, &c),
            "the result is not rewritten again"
        );
        assert_eq!(located.path, Path::from("deep/x"));

        let untouched = storage.locate_loaded("mem://b/y").unwrap();
        assert!(Arc::ptr_eq(&untouched.store, &b));
        assert_eq!(untouched.path, Path::from("y"));
    }

    #[test]
    fn test_locate_builds_one_s3_store_per_prefix() {
        let storage = Storage::new(StorageConfig {
            s3: Some(S3Config {
                endpoint: Some("http://object-store.example:9000".to_string()),
                region: Some("us-example-1".to_string()),
                access_key_id: Some(TEST_KEY_ID.to_string()),
                secret_access_key: Some(TEST_SECRET.to_string()),
                allow_http: true,
                path_style: true,
            }),
            ..StorageConfig::default()
        });
        assert!(matches!(
            storage.credential_source(),
            CredentialSource::Static { .. }
        ));
        let first = storage.locate_loaded("s3://bucket-a/x/y").unwrap();
        let again = storage.locate_loaded("s3://bucket-a/z").unwrap();
        let hadoop = storage.locate_loaded("s3a://bucket-a/x/y").unwrap();
        assert!(Arc::ptr_eq(&first.store, &again.store));
        assert!(!Arc::ptr_eq(&first.store, &hadoop.store));
        assert_eq!(first.path, Path::from("x/y"));
        assert_eq!(hadoop.path, Path::from("x/y"));
    }

    #[test]
    fn test_locate_builds_unsigned_s3_store_without_config() {
        let storage = Storage::new(StorageConfig::default());
        assert!(matches!(
            storage.credential_source(),
            CredentialSource::Unsigned
        ));
        let located = storage.locate_loaded("s3://bucket-a/x").unwrap();
        assert_eq!(located.path, Path::from("x"));
    }

    #[test]
    fn test_sdk_to_load_for_s3_locations_as_rewritten() {
        let storage = Storage::with_sdk_defaults(StorageConfig {
            rewrite: vec![
                RewriteRule {
                    from: "file:///mirror/".to_string(),
                    to: "s3://bucket-a/".to_string(),
                },
                RewriteRule {
                    from: "s3://moved/".to_string(),
                    to: "file:///local/".to_string(),
                },
            ],
            ..StorageConfig::default()
        });
        assert!(storage.sdk_to_load_for("s3://bucket-a/x").is_some());
        assert!(storage.sdk_to_load_for("s3a://bucket-a/x").is_some());
        assert!(
            storage.sdk_to_load_for("file:///mirror/x").is_some(),
            "rewritten into S3"
        );
        assert!(storage.sdk_to_load_for("file:///warehouse/x").is_none());
        assert!(storage.sdk_to_load_for("/warehouse/x").is_none());
        assert!(
            storage.sdk_to_load_for("s3://moved/x").is_none(),
            "rewritten out of S3"
        );
    }

    #[test]
    fn test_sdk_to_load_for_nothing_without_the_sdk_or_once_loaded() {
        let without = Storage::new(StorageConfig::default());
        assert!(without.sdk_to_load_for("s3://bucket-a/x").is_none());
        let loaded = Storage::with_sdk_defaults(StorageConfig::default());
        loaded
            .sdk
            .as_ref()
            .unwrap()
            .set(LoadedSdk::new(SdkConfig::builder().build()))
            .unwrap();
        assert!(loaded.sdk_to_load_for("s3://bucket-a/x").is_none());
    }

    #[tokio::test]
    async fn test_locate_a_local_location_leaves_the_sdk_unloaded() {
        let storage = Storage::with_sdk_defaults(StorageConfig::default());
        storage.locate("/warehouse/db/t/x").await.unwrap();
        storage.locate("file:///warehouse/db/t/y").await.unwrap();
        assert!(!storage.sdk.as_ref().unwrap().initialized());
    }

    #[tokio::test]
    async fn test_locate_s3_signs_with_the_loaded_sdk_chain() {
        let storage = Storage::with_sdk_defaults(StorageConfig::default());
        assert!(matches!(
            storage.credential_source(),
            CredentialSource::Unsigned
        ));
        let sdk = SdkConfig::builder()
            .credentials_provider(SharedCredentialsProvider::new(test_credentials(None)))
            .build();
        storage
            .sdk
            .as_ref()
            .unwrap()
            .set(LoadedSdk::new(sdk))
            .unwrap();
        storage.locate("s3://bucket-a/x").await.unwrap();
        assert!(matches!(
            storage.credential_source(),
            CredentialSource::SdkChain(_)
        ));
    }

    /// Every S3 store a storage builds signs through its one bridge, so
    /// reads from two buckets run the chain once between them, and the
    /// bridge each store is built with is the storage's.
    #[tokio::test]
    async fn test_s3_stores_of_every_bucket_share_one_credential_bridge() {
        let endpoint = LocalHttp::start(Reply::Status(UNAVAILABLE));
        let storage = Storage::with_sdk_defaults(StorageConfig {
            s3: Some(S3Config {
                endpoint: Some(endpoint.url()),
                allow_http: true,
                path_style: true,
                ..S3Config::default()
            }),
            max_retries: 0,
            ..StorageConfig::default()
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = CountingProvider {
            answers: vec![test_credentials(None)],
            calls: Arc::clone(&calls),
        };
        let sdk = SdkConfig::builder()
            .credentials_provider(SharedCredentialsProvider::new(provider))
            .build();
        storage
            .sdk
            .as_ref()
            .unwrap()
            .set(LoadedSdk::new(sdk))
            .unwrap();
        let buckets = ["bucket-a", "bucket-b"];
        for bucket in buckets {
            let result = storage
                .read_all(&format!("s3://{bucket}/{OBJECT_KEY}"))
                .await;
            assert!(matches!(result, Err(Error::ObjectStore(_))), "{result:?}");
        }
        assert_eq!(endpoint.requests(), buckets.len(), "each read was sent");
        assert_eq!(calls.load(Ordering::Relaxed), 1, "one chain run for both");
        let CredentialSource::SdkChain(built_with) = storage.credential_source() else {
            panic!("the storage signs through the SDK chain");
        };
        assert!(Arc::ptr_eq(&built_with, storage.sdk_credentials().unwrap()));
    }

    /// A storage whose S3 stores reach `endpoint` over plain HTTP, path
    /// style and unsigned, retrying `max_retries` times.
    fn s3_storage_at(endpoint: &LocalHttp, max_retries: usize) -> Storage {
        Storage::new(StorageConfig {
            s3: Some(S3Config {
                endpoint: Some(endpoint.url()),
                allow_http: true,
                path_style: true,
                ..S3Config::default()
            }),
            max_retries,
            ..StorageConfig::default()
        })
    }

    #[tokio::test]
    async fn test_read_all_retries_an_unavailable_s3_store_max_retries_times() {
        let retry_counts = [StorageConfig::default().max_retries, 0];
        for max_retries in retry_counts {
            let endpoint = LocalHttp::start(Reply::Status(UNAVAILABLE));
            let storage = s3_storage_at(&endpoint, max_retries);
            let result = storage
                .read_all(&format!("s3://{BUCKET}/{OBJECT_KEY}"))
                .await;
            assert!(matches!(result, Err(Error::ObjectStore(_))), "{result:?}");
            assert_eq!(
                endpoint.requests(),
                1 + max_retries,
                "{max_retries} retries"
            );
        }
    }

    #[test]
    fn test_record_get_counts_one_get_and_its_bytes() {
        let stats = IoStats::default();
        stats.record_get(ReadKind::Load, payload_len());
        assert_eq!(stats.gets(), 1);
        assert_eq!(stats.bytes(), payload_len());
        assert_eq!(stats.hedges(), 0);
        assert_eq!(stats.local_reads(), 0);
        assert_eq!(stats.local_bytes(), 0);
    }

    #[test]
    fn test_record_hedge_counts_a_get_and_a_hedge() {
        let stats = IoStats::default();
        stats.record_hedge(payload_len());
        assert_eq!(stats.hedges(), 1);
        assert_eq!(stats.gets(), 1);
        assert_eq!(stats.bytes(), payload_len());
    }

    #[test]
    fn test_record_local_read_counts_one_read_and_its_bytes_and_no_get() {
        let stats = IoStats::default();
        stats.record_local_read(payload_len());
        assert_eq!(stats.local_reads(), 1);
        assert_eq!(stats.local_bytes(), payload_len());
        assert_eq!(stats.gets(), 0);
        assert_eq!(stats.bytes(), 0);
        assert_eq!(stats.hedges(), 0);
    }

    #[tokio::test]
    async fn test_read_all_counts_one_load_get_of_the_size() {
        let (storage, _) = storage_with(Arc::new(InMemory::new())).await;
        let bytes = storage
            .read_all(&format!("{MEM_PREFIX}{OBJECT_KEY}"))
            .await
            .unwrap();
        assert_eq!(&bytes[..], PAYLOAD);
        let stats = storage.stats();
        assert_eq!(stats.gets(), 1);
        assert_eq!(stats.bytes(), payload_len());
        assert_eq!(stats.hedges(), 0);
    }

    #[tokio::test]
    async fn test_read_all_counts_a_failed_get_as_zero_bytes() {
        let (storage, _) = storage_with(Arc::new(InMemory::new())).await;
        let result = storage
            .read_all(&format!("{MEM_PREFIX}{MISSING_KEY}"))
            .await;
        assert!(matches!(
            result,
            Err(Error::ObjectStore(object_store::Error::NotFound { .. }))
        ));
        let stats = storage.stats();
        assert_eq!(stats.gets(), 1);
        assert_eq!(stats.bytes(), 0);
        assert_eq!(stats.hedges(), 0);
    }

    #[tokio::test]
    async fn test_read_all_maps_file_uri_and_bare_path_to_local_store() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f.bin");
        std::fs::write(&file, PAYLOAD).unwrap();
        let bare = file.to_str().unwrap();
        let storage = Storage::new(StorageConfig::default());

        let from_bare = storage.read_all(bare).await.unwrap();
        assert_eq!(&from_bare[..], PAYLOAD);
        assert_eq!(storage.stats().gets(), 1);

        let from_uri = storage
            .read_all(&format!("{FILE_PREFIX}{bare}"))
            .await
            .unwrap();
        assert_eq!(&from_uri[..], PAYLOAD);
        assert_eq!(storage.stats().gets(), 2);
        assert_eq!(storage.stats().bytes(), payload_len() + payload_len());

        let by_uri = storage
            .locate_loaded(&format!("{FILE_PREFIX}{bare}"))
            .unwrap();
        let by_path = storage.locate_loaded(bare).unwrap();
        assert!(Arc::ptr_eq(&by_uri.store, &by_path.store));
    }

    #[tokio::test]
    async fn test_read_range_counts_one_load_get_of_the_range() {
        let (storage, located) = storage_with(Arc::new(InMemory::new())).await;
        let bytes = storage.read_range(&located, INNER_RANGE).await.unwrap();
        assert_eq!(&bytes[..], INNER_BYTES);
        let stats = storage.stats();
        assert_eq!(stats.gets(), 1);
        assert_eq!(stats.bytes(), range_bytes(&INNER_RANGE));
        assert_eq!(stats.hedges(), 0);
    }

    #[tokio::test]
    async fn test_read_ranges_counts_one_get_per_range() {
        let (storage, located) = storage_with(Arc::new(InMemory::new())).await;
        let parts = storage
            .read_ranges(&located, &[FIRST_BYTE, FOURTH_BYTE])
            .await
            .unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(&parts[0][..], &PAYLOAD[0..1]);
        assert_eq!(&parts[1][..], &PAYLOAD[3..4]);
        let stats = storage.stats();
        assert_eq!(stats.gets(), 2);
        assert_eq!(
            stats.bytes(),
            range_bytes(&FIRST_BYTE) + range_bytes(&FOURTH_BYTE)
        );
        assert_eq!(stats.hedges(), 0);
    }

    #[tokio::test]
    async fn test_read_range_never_hedges_on_a_slow_store() {
        let (storage, located) = storage_with(slow_store()).await;
        let window = eager_window();
        assert!(window.delay().is_some(), "the window would hedge a lookup");
        let bytes = storage.read_range(&located, INNER_RANGE).await.unwrap();
        assert_eq!(&bytes[..], INNER_BYTES);
        let stats = storage.stats();
        assert_eq!(stats.hedges(), 0);
        assert_eq!(stats.gets(), 1);
    }

    #[tokio::test]
    async fn test_read_range_hedged_issues_one_hedge_when_first_is_slow() {
        let (storage, located) = storage_with(Arc::new(Scripted::new(&[STALL, RELEASE]))).await;
        let window = eager_window();
        let bytes = hedged_read(&storage, &located, &window).await.unwrap();
        assert_eq!(&bytes[..], INNER_BYTES);
        let stats = storage.stats();
        assert_eq!(stats.hedges(), 1);
        assert_eq!(stats.gets(), 2);
        assert_eq!(
            stats.bytes(),
            range_bytes(&INNER_RANGE) + range_bytes(&INNER_RANGE)
        );
    }

    #[tokio::test]
    async fn test_read_range_hedged_first_answer_wins_after_the_hedge() {
        let (storage, located) =
            storage_with(Arc::new(Scripted::new(&[STALL, RELEASE_AND_WAIT]))).await;
        let window = eager_window();
        let bytes = hedged_read(&storage, &located, &window).await.unwrap();
        assert_eq!(&bytes[..], INNER_BYTES);
        let stats = storage.stats();
        assert_eq!(stats.hedges(), 1);
        assert_eq!(stats.gets(), 2);
        assert_eq!(
            stats.bytes(),
            range_bytes(&INNER_RANGE) + range_bytes(&INNER_RANGE)
        );
        assert!(
            window.delay().is_none(),
            "the attempt entered the window as hedged"
        );
    }

    #[tokio::test]
    async fn test_read_range_hedged_hedge_answers_when_first_fails() {
        let (storage, located) =
            storage_with(Arc::new(Scripted::new(&[STALL_AND_FAIL, RELEASE_AND_WAIT]))).await;
        let window = eager_window();
        let bytes = hedged_read(&storage, &located, &window).await.unwrap();
        assert_eq!(&bytes[..], INNER_BYTES);
        let stats = storage.stats();
        assert_eq!(stats.hedges(), 1);
        assert_eq!(stats.gets(), 2);
    }

    #[tokio::test]
    async fn test_read_range_hedged_first_answers_when_hedge_fails() {
        let (storage, located) =
            storage_with(Arc::new(Scripted::new(&[STALL, RELEASE_AND_FAIL]))).await;
        let window = eager_window();
        let bytes = hedged_read(&storage, &located, &window).await.unwrap();
        assert_eq!(&bytes[..], INNER_BYTES);
        let stats = storage.stats();
        assert_eq!(stats.hedges(), 1);
        assert_eq!(stats.gets(), 2);
    }

    #[tokio::test]
    async fn test_read_range_hedged_fails_when_both_fail() {
        let (storage, located) =
            storage_with(Arc::new(Scripted::new(&[STALL_AND_FAIL, RELEASE_AND_FAIL]))).await;
        let window = eager_window();
        let result = hedged_read(&storage, &located, &window).await;
        assert!(matches!(
            result,
            Err(Error::ObjectStore(object_store::Error::Generic { .. }))
        ));
        let stats = storage.stats();
        assert_eq!(stats.hedges(), 1);
        assert_eq!(stats.gets(), 2);
        assert!(
            window.delay().is_none(),
            "the failed attempt entered the window as hedged"
        );
    }

    /// The pages of a plain lookup GET, which `read_range_hedged` reads
    /// through `read_span_hedged`.
    const ONE_PAGE: u32 = 1;

    /// `read_range_hedged`'s future is no larger than `read_span_hedged`'s:
    /// every lookup future lays out two of them, warm hits included, and a
    /// batched key's future is stored three times over, so state a wrapper
    /// keeps of its own is paid on every key-value call.
    #[tokio::test]
    async fn test_read_range_hedged_future_is_no_larger_than_read_span_hedged() {
        let (storage, located) = storage_with(Arc::new(InMemory::new())).await;
        let window = HedgeWindow::new(&hedge_config(MEDIAN, TWO, HALF));
        let range = size_of_val(&storage.read_range_hedged(&located, INNER_RANGE, &window));
        let span = size_of_val(&storage.read_span_hedged(&located, INNER_RANGE, ONE_PAGE, &window));
        assert!(range <= span, "{range} bytes against {span}");
    }

    /// The pages of the span the span-window tests read.
    const SPAN_PAGES: u32 = 4;

    /// A span's delay is the window's per-page delay times its pages, and
    /// one page, or none, is the plain delay (RFD 025, Caching and hedging).
    #[test]
    fn test_delay_for_scales_the_window_delay_with_the_pages() {
        let window = window_with(
            &hedge_config(MEDIAN, TWO, HALF),
            &[(TINY, false), (TINY, false)],
        );
        assert_eq!(window.delay_for(1), Some(TINY));
        assert_eq!(window.delay_for(0), Some(TINY), "at least one page");
        assert_eq!(window.delay_for(SPAN_PAGES), Some(TINY * SPAN_PAGES));
        let empty = HedgeWindow::new(&hedge_config(MEDIAN, TWO, HALF));
        assert_eq!(
            empty.delay_for(SPAN_PAGES),
            None,
            "no delay before the window fills"
        );
    }

    /// A span read enters its window as its elapsed time over its pages,
    /// so the window holds per-page latencies: after one fast read of a
    /// span, the window's median is at most a page's share of the time the
    /// read took.
    #[tokio::test]
    async fn test_read_span_hedged_records_its_elapsed_time_per_page() {
        let (storage, located) = storage_with(Arc::new(InMemory::new())).await;
        let window = window_with(
            &hedge_config(MEDIAN, TWO, HALF),
            &[(HUGE, false), (HUGE, false)],
        );
        let started = Instant::now();
        let bytes = storage
            .read_span_hedged(&located, INNER_RANGE, SPAN_PAGES, &window)
            .await
            .unwrap();
        let took = started.elapsed();
        assert_eq!(&bytes[..], INNER_BYTES);
        assert_eq!(storage.stats().gets(), 1);
        assert_eq!(
            storage.stats().hedges(),
            0,
            "the delay was HUGE times the pages"
        );
        let per_page = window.delay().expect("the window is full");
        assert!(
            per_page <= took / SPAN_PAGES,
            "{per_page:?} recorded for a read of {took:?} over {SPAN_PAGES} pages"
        );
    }

    #[tokio::test]
    async fn test_read_range_hedged_issues_no_hedge_when_first_is_fast() {
        let (storage, located) = storage_with(Arc::new(InMemory::new())).await;
        let window = window_with(
            &hedge_config(MEDIAN, TWO, HALF),
            &[(HUGE, false), (HUGE, false)],
        );
        assert_eq!(window.delay(), Some(HUGE));
        let bytes = storage
            .read_range_hedged(&located, INNER_RANGE, &window)
            .await
            .unwrap();
        assert_eq!(&bytes[..], INNER_BYTES);
        let stats = storage.stats();
        assert_eq!(stats.hedges(), 0);
        assert_eq!(stats.gets(), 1);
        assert_eq!(stats.bytes(), range_bytes(&INNER_RANGE));
        assert!(
            window.delay().is_some_and(|delay| delay < HUGE),
            "the unhedged attempt entered the window and displaced a HUGE sample"
        );
    }

    #[tokio::test]
    async fn test_read_range_hedged_dropped_after_its_hedge_counts_against_the_ceiling() {
        let scripted = Arc::new(Scripted::new(&[STALL, STALL]));
        let (storage, located) = storage_with(Arc::<Scripted>::clone(&scripted)).await;
        let window = eager_window();
        assert!(window.delay().is_some(), "the window would hedge a lookup");
        let both_of_the_pair = 2;
        tokio::time::timeout(HUGE, async {
            let read = pin!(storage.read_range_hedged(&located, INNER_RANGE, &window));
            tokio::select! {
                biased;
                _ = read => panic!("neither request of the pair answers"),
                () = scripted.arrived(both_of_the_pair) => {}
            }
        })
        .await
        .expect("the hedge is issued once the window's delay passes");
        assert_eq!(storage.stats().hedges(), 1);
        assert!(
            window.delay().is_none(),
            "the dropped read entered the window as hedged, which reaches the ceiling"
        );
    }

    #[tokio::test]
    async fn test_read_range_hedged_dropped_before_its_delay_records_nothing() {
        let (storage, located) = storage_with(Arc::new(Scripted::new(&[STALL]))).await;
        let window = window_with(
            &hedge_config(MEDIAN, TWO, HALF),
            &[(HUGE, false), (HUGE, false)],
        );
        assert_eq!(window.delay(), Some(HUGE));
        let read = storage.read_range_hedged(&located, INNER_RANGE, &window);
        assert!(
            futures::poll!(pin!(read)).is_pending(),
            "the first request stalls"
        );
        assert_eq!(
            window.delay(),
            Some(HUGE),
            "a read dropped long before its delay left the window as it was"
        );
    }

    #[tokio::test]
    async fn test_read_range_hedged_stops_hedging_at_the_ceiling() {
        let (storage, located) = storage_with(Arc::new(Scripted::new(&[STALL, RELEASE]))).await;
        let window = eager_window();

        hedged_read(&storage, &located, &window).await.unwrap();
        assert_eq!(storage.stats().hedges(), 1);
        assert!(
            window.delay().is_none(),
            "one hedged sample in two reaches a ceiling of one half"
        );

        let bytes = storage
            .read_range_hedged(&located, INNER_RANGE, &window)
            .await
            .unwrap();
        assert_eq!(&bytes[..], INNER_BYTES);
        assert_eq!(storage.stats().hedges(), 1);
        assert_eq!(storage.stats().gets(), 3);
        assert!(
            window.delay().is_none(),
            "the hedged sample is still in the window"
        );

        storage
            .read_range_hedged(&located, INNER_RANGE, &window)
            .await
            .unwrap();
        assert_eq!(storage.stats().hedges(), 1);
        assert_eq!(storage.stats().gets(), 4);
        assert!(
            window.delay().is_some(),
            "two unhedged samples have displaced the hedged one"
        );
    }

    #[test]
    fn test_delay_none_when_disabled() {
        let config = HedgeConfig {
            enabled: false,
            ..hedge_config(MEDIAN, 1, TOP)
        };
        let window = window_with(&config, &[(SAMPLE_10MS, false)]);
        assert_eq!(window.delay(), None);
    }

    #[test]
    fn test_delay_none_until_window_full() {
        let config = hedge_config(MEDIAN, FOUR, TOP);
        let window = window_with(
            &config,
            &[
                (SAMPLE_10MS, false),
                (SAMPLE_20MS, false),
                (SAMPLE_30MS, false),
            ],
        );
        assert_eq!(window.delay(), None);
        window.record(SAMPLE_40MS, false);
        assert_eq!(window.delay(), Some(SAMPLE_20MS));
    }

    #[test]
    fn test_delay_is_the_nearest_rank_percentile() {
        let samples = [
            (SAMPLE_40MS, false),
            (SAMPLE_10MS, false),
            (SAMPLE_30MS, false),
            (SAMPLE_20MS, false),
        ];
        let cases = [
            (TENTH, SAMPLE_10MS),
            (MEDIAN, SAMPLE_20MS),
            (THIRD_QUARTILE, SAMPLE_30MS),
            (P95, SAMPLE_40MS),
            (TOP, SAMPLE_40MS),
        ];
        for (percentile, expected) in cases {
            let window = window_with(&hedge_config(percentile, FOUR, TOP), &samples);
            assert_eq!(window.delay(), Some(expected), "percentile {percentile}");
        }
    }

    #[test]
    fn test_delay_none_when_one_more_hedge_would_exceed_the_ceiling() {
        let one_hedged = [
            (SAMPLE_10MS, true),
            (SAMPLE_20MS, false),
            (SAMPLE_30MS, false),
            (SAMPLE_40MS, false),
        ];
        let at_ceiling = window_with(&hedge_config(MEDIAN, FOUR, ONE_IN_FOUR), &one_hedged);
        assert_eq!(
            at_ceiling.delay(),
            None,
            "a second hedge in four would exceed one in four"
        );
        let between = window_with(&hedge_config(MEDIAN, FOUR, THREE_IN_TEN), &one_hedged);
        assert_eq!(
            between.delay(),
            None,
            "one in four is under the ceiling; two in four would be over it"
        );
        let reaching = window_with(&hedge_config(MEDIAN, FOUR, HALF), &one_hedged);
        assert_eq!(
            reaching.delay(),
            Some(SAMPLE_20MS),
            "a second hedge in four reaches one half without exceeding it"
        );
        let none_hedged = one_hedged.map(|(elapsed, _)| (elapsed, false));
        let unhedged = window_with(&hedge_config(MEDIAN, FOUR, ONE_IN_FOUR), &none_hedged);
        assert_eq!(unhedged.delay(), Some(SAMPLE_20MS));
        let off = window_with(&hedge_config(MEDIAN, FOUR, 0.0), &none_hedged);
        assert_eq!(off.delay(), None, "a ceiling of zero issues no hedge");
    }

    #[test]
    fn test_record_keeps_only_the_last_window_samples() {
        let window = window_with(
            &hedge_config(MEDIAN, TWO, HALF),
            &[
                (SAMPLE_10MS, true),
                (SAMPLE_20MS, false),
                (SAMPLE_30MS, false),
            ],
        );
        assert_eq!(
            window.delay(),
            Some(SAMPLE_20MS),
            "the first sample, hedged and the smallest, has left the window"
        );
    }

    /// For a window of thirty-two or more, the percentile is computed again
    /// only once a sixteenth of the window has turned over: a slow sample moves
    /// the delay at the second sample after the last computation, not at
    /// the first. The ceiling stays exact on every sample.
    #[test]
    fn test_delay_is_recomputed_once_a_share_of_the_window_turns_over() {
        let turned_over_per_computation = 2;
        let window = HEDGE_RECOMPUTE_SHARE * turned_over_per_computation;
        let config = hedge_config(TOP, window, HALF);
        let full = vec![(SAMPLE_10MS, false); window];
        let hedges = window_with(&config, &full);
        assert_eq!(hedges.delay(), Some(SAMPLE_10MS));
        hedges.record(SAMPLE_40MS, false);
        assert_eq!(
            hedges.delay(),
            Some(SAMPLE_10MS),
            "one sample of two since the last computation"
        );
        hedges.record(SAMPLE_40MS, false);
        assert_eq!(hedges.delay(), Some(SAMPLE_40MS));
        for _ in 0..window / 2 {
            hedges.record(SAMPLE_40MS, true);
        }
        assert_eq!(
            hedges.delay(),
            None,
            "half the window hedged: one more would pass the ceiling at once"
        );
    }

    #[test]
    fn test_hedge_window_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<HedgeWindow>();
    }

    #[test]
    fn test_is_fresh_without_expiry_indefinitely() {
        let now = time::system_now();
        let cached = cached_at(now, None);
        assert!(cached.is_fresh(now));
        assert!(cached.is_fresh(now + ONE_HOUR));
    }

    #[test]
    fn test_is_fresh_until_the_margin_before_expiry() {
        let now = time::system_now();
        let cached = cached_at(now, Some(now + ONE_HOUR));
        let margin_starts = now + ONE_HOUR - CREDENTIAL_REFRESH_MARGIN;
        assert!(cached.is_fresh(margin_starts - ONE_SECOND));
        assert!(!cached.is_fresh(margin_starts));
    }

    #[test]
    fn test_is_fresh_for_the_minimum_reuse_inside_the_margin() {
        let now = time::system_now();
        let cached = cached_at(now, Some(now + CREDENTIAL_REFRESH_MARGIN - ONE_SECOND));
        assert!(cached.is_fresh(now));
        assert!(cached.is_fresh(now + CREDENTIAL_MIN_REUSE - ONE_SECOND));
        assert!(!cached.is_fresh(now + CREDENTIAL_MIN_REUSE));
    }

    #[test]
    fn test_is_fresh_never_past_expiry() {
        let now = time::system_now();
        let expiring = cached_at(now, Some(now + ONE_SECOND));
        assert!(expiring.is_fresh(now));
        assert!(!expiring.is_fresh(now + ONE_SECOND));
        let expired = cached_at(now, Some(now - ONE_SECOND));
        assert!(!expired.is_fresh(now));
    }

    #[tokio::test]
    async fn test_get_credential_maps_key_secret_and_token() {
        let (provider, calls) = counting_credentials(vec![test_credentials(None)]);
        let credential = provider.get_credential().await.unwrap();
        assert_eq!(credential.key_id, TEST_KEY_ID);
        assert_eq!(credential.secret_key, TEST_SECRET);
        assert_eq!(credential.token.as_deref(), Some(TEST_TOKEN));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_get_credential_caches_a_credential_without_expiry() {
        let (provider, calls) = counting_credentials(vec![test_credentials(None)]);
        let first = provider.get_credential().await.unwrap();
        let again = provider.get_credential().await.unwrap();
        assert!(Arc::ptr_eq(&first, &again));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_get_credential_caches_a_credential_beyond_the_margin() {
        let beyond_margin =
            time::system_now() + CREDENTIAL_REFRESH_MARGIN + CREDENTIAL_REFRESH_MARGIN;
        let (provider, calls) = counting_credentials(vec![test_credentials(Some(beyond_margin))]);
        let first = provider.get_credential().await.unwrap();
        let again = provider.get_credential().await.unwrap();
        assert!(Arc::ptr_eq(&first, &again));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_get_credential_reuses_a_credential_inside_the_margin() {
        let inside_margin = time::system_now() + CREDENTIAL_REFRESH_MARGIN - ONE_SECOND;
        let (provider, calls) = counting_credentials(vec![test_credentials(Some(inside_margin))]);
        let first = provider.get_credential().await.unwrap();
        let again = provider.get_credential().await.unwrap();
        assert!(Arc::ptr_eq(&first, &again));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_get_credential_refreshes_an_expired_credential() {
        let expired = time::system_now() - ONE_SECOND;
        let (provider, calls) = counting_credentials(vec![test_credentials(Some(expired))]);
        let first = provider.get_credential().await.unwrap();
        let again = provider.get_credential().await.unwrap();
        assert!(!Arc::ptr_eq(&first, &again));
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_get_credential_coalesces_concurrent_first_use() {
        let (provider, calls) = counting_credentials(vec![test_credentials(None)]);
        let answers = join_all((0..CONCURRENT_CALLERS).map(|_| provider.get_credential())).await;
        assert_eq!(answers.len(), CONCURRENT_CALLERS);
        let first = answers[0].as_ref().unwrap();
        for answer in &answers {
            assert!(Arc::ptr_eq(first, answer.as_ref().unwrap()));
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_provide_credentials_answers_from_the_cache_s3_stores_sign_from() {
        let (bridge, calls) = counting_credentials(vec![test_credentials(None)]);
        let signed = bridge.get_credential().await.unwrap();
        let catalog = bridge.provide_credentials().await.unwrap();
        assert_eq!(catalog.access_key_id(), signed.key_id);
        assert_eq!(catalog.session_token(), signed.token.as_deref());
        assert_eq!(calls.load(Ordering::Relaxed), 1, "one chain run for both");
    }

    /// A catalog client signs with what the storage's S3 stores sign with:
    /// the static keys when set, the one bridge otherwise, and nothing for
    /// a storage that signs nothing.
    #[tokio::test]
    async fn test_aws_credentials_are_the_ones_s3_stores_sign_with() {
        let unsigned = Storage::new(StorageConfig::default());
        assert!(unsigned.aws_credentials().is_none());

        let keyed = Storage::new(StorageConfig {
            s3: Some(S3Config {
                access_key_id: Some(TEST_KEY_ID.to_string()),
                secret_access_key: Some(TEST_SECRET.to_string()),
                ..S3Config::default()
            }),
            ..StorageConfig::default()
        });
        let keys = keyed
            .aws_credentials()
            .unwrap()
            .provide_credentials()
            .await
            .unwrap();
        assert_eq!(keys.access_key_id(), TEST_KEY_ID);
        assert_eq!(keys.secret_access_key(), TEST_SECRET);

        let chained = Storage::with_sdk_defaults(StorageConfig::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let sdk = SdkConfig::builder()
            .credentials_provider(SharedCredentialsProvider::new(CountingProvider {
                answers: vec![test_credentials(None)],
                calls: Arc::clone(&calls),
            }))
            .build();
        chained
            .sdk
            .as_ref()
            .unwrap()
            .set(LoadedSdk::new(sdk))
            .unwrap();
        let catalog = chained.aws_credentials().unwrap();
        catalog.provide_credentials().await.unwrap();
        catalog.provide_credentials().await.unwrap();
        chained
            .sdk_credentials()
            .unwrap()
            .get_credential()
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 1, "one chain run for all");
    }

    #[tokio::test]
    async fn test_aws_sdk_config_is_none_without_the_sdk() {
        let storage = Storage::new(StorageConfig::default());
        assert!(storage.aws_sdk_config().await.is_none());
    }

    #[tokio::test]
    async fn test_get_credential_refreshes_once_under_concurrent_expiry() {
        let expired = time::system_now() - ONE_SECOND;
        let (provider, calls) = counting_credentials(vec![
            test_credentials(Some(expired)),
            test_credentials(None),
        ]);
        let primed = provider.get_credential().await.unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        let answers = join_all((0..CONCURRENT_CALLERS).map(|_| provider.get_credential())).await;
        assert_eq!(answers.len(), CONCURRENT_CALLERS);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        let fresh = answers[0].as_ref().unwrap();
        assert!(!Arc::ptr_eq(&primed, fresh));
        for answer in &answers {
            assert!(Arc::ptr_eq(fresh, answer.as_ref().unwrap()));
        }
    }

    /// A GET dropped while `Gauged` holds it leaves the GETs in flight:
    /// after it, one GET in flight does not open a gate of two, so a later
    /// gate counts only the GETs that are running.
    #[tokio::test]
    async fn test_gauged_counts_a_dropped_get_out_of_flight() {
        let gate_of_two = 2;
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from(OBJECT_KEY);
        inner
            .put(&path, PutPayload::from_static(PAYLOAD))
            .await
            .unwrap();
        let gauged = Gauged::new(Arc::clone(&inner));
        gauged.hold();
        let mut held = Box::pin(gauged.get_range(&path, INNER_RANGE));
        assert!(
            futures::poll!(held.as_mut()).is_pending(),
            "the GET is held"
        );
        drop(held);
        gauged.fill_to(gate_of_two);
        assert_eq!(gauged.high_water(), 0, "no GET in flight after the drop");
        let mut lone = Box::pin(gauged.get_range(&path, INNER_RANGE));
        assert!(
            futures::poll!(lone.as_mut()).is_pending(),
            "one GET in flight does not open a gate of two"
        );
    }
}
