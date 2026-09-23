//! Every byte read from object storage (RFD 003): one store per
//! `scheme://authority/` prefix, location rewriting before any request
//! leaves the process, one counter for GETs, bytes, hedges, local reads and
//! local bytes, the AWS SDK's credential chain bridged with a cached and
//! coalesced refresh, and the hedged lookup read of RFD 013.

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
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use bytes::Bytes;
use futures::future::{Either, select};
use object_store::aws::{AmazonS3Builder, AwsCredential};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{CredentialProvider, ObjectStore, ObjectStoreExt};
// Reads are timed on tokio's clock: a paused runtime (the simulation) advances
// it virtually, so the hedge window and the timer that consults it agree; the
// wall clock would feed the window real durations under a virtual timer.
use tokio::time::Instant;

use crate::approx;
use crate::config::{ConfigError, ConfigViolation, HedgeConfig, S3Config, StorageConfig};
use crate::error::{Error, Result};
use crate::metric::{self, ReadKind};
use crate::time;

/// The prefix of the local filesystem store. A `file://` URI and a bare
/// absolute path both resolve to it, so a table written by a local job and
/// one whose metadata records URIs read the same way.
const FILE_PREFIX: &str = "file://";

/// The local filesystem scheme with no authority, `file:/path`, the form
/// Hadoop's `Path` records in the metadata of a table written to a local
/// warehouse; it names the same absolute path as `file:///path`.
const FILE_SCHEME: &str = "file:";

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

/// How long before its expiry a cached credential stops being handed out.
/// Two minutes is longer than any one request with its retries takes, so a
/// request signed with a credential from the cache never reaches the store
/// after that credential expired, and it is short next to the hour-long
/// lifetime of role and SSO credentials, so the chain is consulted about
/// once per lifetime rather than once per GET (RFD 003).
const CREDENTIAL_REFRESH_MARGIN: Duration = Duration::from_secs(120);

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
/// the request metrics of RFD 012.
#[derive(Debug, Default)]
pub struct IoStats {
    gets: AtomicU64,
    bytes: AtomicU64,
    hedges: AtomicU64,
    local_reads: AtomicU64,
    local_bytes: AtomicU64,
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

    /// One GET of `len` requested bytes on the `kind` path.
    pub(crate) fn record_get(&self, kind: ReadKind, len: u64) {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(len, Ordering::Relaxed);
        metrics::counter!(metric::OBJECT_GETS_TOTAL, metric::LABEL_KIND => kind.to_string())
            .increment(1);
        metrics::counter!(metric::OBJECT_BYTES_TOTAL, metric::LABEL_KIND => kind.to_string())
            .increment(len);
    }

    /// A hedge: one more lookup GET of the same `len`, issued because the
    /// first copy was slow. A hedge is a GET and counts as one, and it
    /// counts once more under its own name (RFD 013).
    pub(crate) fn record_hedge(&self, len: u64) {
        self.hedges.fetch_add(1, Ordering::Relaxed);
        metrics::counter!(metric::HEDGES_TOTAL).increment(1);
        self.record_get(ReadKind::Lookup, len);
    }

    /// One read of `len` bytes from the local directory (RFD 017), counted
    /// beside GETs so a lookup's whole cost is one structure whatever tier
    /// the table runs (RFD 003). A local read is never a GET.
    pub(crate) fn record_local_read(&self, len: u64) {
        self.local_reads.fetch_add(1, Ordering::Relaxed);
        self.local_bytes.fetch_add(len, Ordering::Relaxed);
        metrics::counter!(metric::LOCAL_READS_TOTAL).increment(1);
        metrics::counter!(metric::LOCAL_BYTES_TOTAL).increment(len);
    }
}

/// A location resolved to the store that holds the object and the object's
/// path within that store. The `scheme://authority/` prefix is not kept:
/// nothing downstream reads it, and a `Located` is only ever handed back to
/// the `Storage` that produced it.
#[derive(Clone, Debug)]
pub struct Located {
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
    /// The SDK's default chain, resolved once by `Storage::with_sdk_defaults`.
    SdkChain(SharedCredentialsProvider),
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

/// Resolves Iceberg locations to stores and reads bytes through them.
/// Every read the crate makes from object storage goes through one of the
/// `read_*` methods, so rewriting, counting, credentials, and hedging happen
/// in one place (RFD 003).
#[derive(Debug)]
pub struct Storage {
    config: StorageConfig,
    /// The AWS SDK's resolved defaults, when the process asked for them.
    /// `object_store` alone reads environment variables and instance
    /// metadata; the SDK adds profiles, SSO, `credential_process`, and web
    /// identity.
    sdk: Option<SdkConfig>,
    /// One store per `scheme://authority/` prefix. Tests register stores
    /// under synthetic prefixes; `s3://`, `s3a://`, and `file://` stores are
    /// built on first use.
    stores: Mutex<HashMap<String, Arc<dyn ObjectStore>>>,
    stats: Arc<IoStats>,
}

impl Storage {
    /// Builds from configuration with no SDK chain: an S3 store signs with
    /// the configured static keys or not at all. For tests and embedders;
    /// the binary uses `with_sdk_defaults`.
    pub fn new(config: StorageConfig) -> Self {
        Self {
            config,
            sdk: None,
            stores: Mutex::new(HashMap::new()),
            stats: Arc::new(IoStats::default()),
        }
    }

    /// Builds from configuration with the AWS SDK's default region and
    /// credential chain loaded once: profiles, SSO, `credential_process`,
    /// web identity, and instance metadata. Configured static keys still win
    /// for every S3 store.
    pub async fn with_sdk_defaults(config: StorageConfig) -> Self {
        let sdk = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .load()
            .await;
        Self {
            sdk: Some(sdk),
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
    pub fn hedge_config(&self) -> &HedgeConfig {
        &self.config.hedge
    }

    /// Resolves a location to a store and a path: the configured rewrite
    /// rules are applied in order and the first match wins, then the
    /// location is split into its `scheme://authority/` prefix and the rest
    /// (`file://` and a bare absolute path both name the local filesystem),
    /// then the store is taken from the registry or built on first use. A
    /// location no store serves is a configuration fault, since the remedy
    /// is a rewrite rule or a registered store:
    /// `ConfigViolation::NoStoreForLocation`, or
    /// `ConfigViolation::LocationNotAbsolute` for a relative path.
    pub fn locate(&self, location: &str) -> Result<Located> {
        let location = self.rewrite(location);
        let (prefix, rest) = split_location(&location)?;
        let store = self.store_for(&prefix, &location)?;
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
        let l = self.locate(location)?;
        observed(ReadKind::Load, self.get_all(&l)).await
    }

    /// Reads one byte range on the load path. One load GET, never hedged:
    /// footers and key chunks are bulk work nobody waits on (RFD 013).
    pub async fn read_range(&self, l: &Located, range: Range<u64>) -> Result<Bytes> {
        self.stats.record_get(ReadKind::Load, range_len(&range));
        observed(ReadKind::Load, l.store.get_range(&l.path, range)).await
    }

    /// Reads one byte range on the lookup path, hedged against the table's
    /// own latency window (RFD 013): when `window.delay()` names a delay,
    /// the request races a timer of that length, and if the timer wins an
    /// identical second request is issued, counted as a hedge, and the first
    /// answer wins. Every attempt, hedged or not, is recorded into the
    /// window with its elapsed time, so the ceiling counts every hedge
    /// spent. One lookup GET, plus one more when a hedge is issued.
    pub async fn read_range_hedged(
        &self,
        l: &Located,
        range: Range<u64>,
        window: &HedgeWindow,
    ) -> Result<Bytes> {
        let len = range_len(&range);
        self.stats.record_get(ReadKind::Lookup, len);
        let started = Instant::now();
        let (answer, hedged) = self.race(l, range, len, window.delay()).await;
        let elapsed = started.elapsed();
        window.record(elapsed, hedged);
        observe_latency(ReadKind::Lookup, elapsed);
        answer.map_err(Error::from)
    }

    /// Reads several byte ranges of one object in one call to the store.
    /// One load GET per range: the store may coalesce neighbouring ranges on
    /// the wire, and RFD 003 states its promises in logical GETs, for which
    /// this count is exact.
    pub async fn read_ranges(&self, l: &Located, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        for range in ranges {
            self.stats.record_get(ReadKind::Load, range_len(range));
        }
        observed(ReadKind::Load, l.store.get_ranges(&l.path, ranges)).await
    }

    /// The location with the first matching rewrite rule applied, or the
    /// location itself. A rewritten location is not rewritten again.
    fn rewrite<'a>(&self, location: &'a str) -> Cow<'a, str> {
        self.config
            .rewrite
            .iter()
            .find_map(|rule| {
                location
                    .strip_prefix(rule.from.as_str())
                    .map(|rest| Cow::Owned(format!("{}{rest}", rule.to)))
            })
            .unwrap_or(Cow::Borrowed(location))
    }

    /// The store registered for `prefix`, or the one built for it and kept
    /// for the next location under it. `location` is for the error.
    fn store_for(&self, prefix: &str, location: &str) -> Result<Arc<dyn ObjectStore>> {
        let mut stores = self.stores.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(store) = stores.get(prefix) {
            return Ok(Arc::clone(store));
        }
        let store = self.build_store(prefix, location)?;
        stores.insert(prefix.to_string(), Arc::clone(&store));
        Ok(store)
    }

    /// The store for a prefix nothing registered: the local filesystem for
    /// `file://`, an S3 client for `s3://` and `s3a://`, and a configuration
    /// fault for anything else.
    fn build_store(&self, prefix: &str, location: &str) -> Result<Arc<dyn ObjectStore>> {
        if prefix == FILE_PREFIX {
            return Ok(Arc::new(LocalFileSystem::new()));
        }
        let bucket = S3_SCHEMES
            .iter()
            .find_map(|scheme| prefix.strip_prefix(scheme))
            .map(|authority| authority.trim_end_matches(PATH_SEPARATOR));
        match bucket {
            Some(bucket) => self.s3_store(bucket),
            None => Err(violation(ConfigViolation::NoStoreForLocation {
                location: location.to_string(),
            })),
        }
    }

    /// An S3 client for one bucket. Endpoint, region, `allow_http`, and
    /// path style come from `[storage.s3]`, or from its defaults when the
    /// table is absent, so the addressing style does not depend on whether
    /// the table is written; the region falls back to the SDK's when the
    /// file names none. Credentials come from exactly one
    /// `CredentialSource`.
    fn s3_store(&self, bucket: &str) -> Result<Arc<dyn ObjectStore>> {
        let default_s3 = S3Config::default();
        let s3 = self.config.s3.as_ref().unwrap_or(&default_s3);
        let source = self.credential_source();
        tracing::debug!(bucket, credentials = %source, "building s3 store");
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_allow_http(s3.allow_http)
            .with_virtual_hosted_style_request(!s3.path_style);
        if let Some(endpoint) = s3.endpoint.as_deref() {
            builder = builder.with_endpoint(endpoint);
        }
        let sdk_region = self.sdk.as_ref().and_then(SdkConfig::region);
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
            CredentialSource::SdkChain(provider) => {
                builder.with_credentials(Arc::new(SdkCredentials::new(provider)))
            }
            CredentialSource::Unsigned => builder.with_skip_signature(true),
        };
        Ok(Arc::new(builder.build()?))
    }

    /// The one credential source for every S3 store this storage builds:
    /// the configured static keys when both are present, otherwise the SDK
    /// chain when the storage was built with it, otherwise none.
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
        self.sdk
            .as_ref()
            .and_then(SdkConfig::credentials_provider)
            .map_or(CredentialSource::Unsigned, CredentialSource::SdkChain)
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
    /// error does not win: the other request finishes and decides. Returns
    /// the answer and whether a hedge was issued.
    async fn race(
        &self,
        l: &Located,
        range: Range<u64>,
        len: u64,
        delay: Option<Duration>,
    ) -> (object_store::Result<Bytes>, bool) {
        let first = pin!(l.store.get_range(&l.path, range.clone()));
        let Some(delay) = delay else {
            return (first.await, false);
        };
        let timer = pin!(tokio::time::sleep(delay));
        let first = match select(first, timer).await {
            Either::Left((answer, _timer)) => return (answer, false),
            Either::Right(((), first)) => first,
        };
        self.stats.record_hedge(len);
        let second = pin!(l.store.get_range(&l.path, range));
        let answer = match select(first, second).await {
            Either::Left((Ok(bytes), _)) | Either::Right((Ok(bytes), _)) => Ok(bytes),
            Either::Left((Err(error), second)) => {
                tracing::debug!(%error, "first lookup request failed; waiting for its hedge");
                second.await
            }
            Either::Right((Err(error), first)) => {
                tracing::debug!(%error, "hedge failed; waiting for the first lookup request");
                first.await
            }
        };
        (answer, true)
    }
}

/// A location's `scheme://authority/` prefix and the rest. `file://`, the
/// authority-less `file:/path`, and a bare absolute path all give the
/// `file://` prefix with the absolute path as the rest; a relative path is a
/// configuration fault.
fn split_location(location: &str) -> Result<(String, String)> {
    if let Some(path) = location.strip_prefix(FILE_PREFIX) {
        return Ok((FILE_PREFIX.to_string(), path.to_string()));
    }
    if let Some(path) = location.strip_prefix(FILE_SCHEME)
        && path.starts_with(PATH_SEPARATOR)
    {
        return Ok((FILE_PREFIX.to_string(), path.to_string()));
    }
    if location.starts_with(PATH_SEPARATOR) {
        return Ok((FILE_PREFIX.to_string(), location.to_string()));
    }
    let Some((scheme, rest)) = location.split_once(SCHEME_SEPARATOR) else {
        return Err(violation(ConfigViolation::LocationNotAbsolute {
            location: location.to_string(),
        }));
    };
    let (authority, key) = rest.split_once(PATH_SEPARATOR).unwrap_or((rest, ""));
    Ok((
        format!("{scheme}{SCHEME_SEPARATOR}{authority}{PATH_SEPARATOR}"),
        key.to_string(),
    ))
}

/// A location fault as the crate's error.
fn violation(violation: ConfigViolation) -> Error {
    Error::Config(ConfigError::Invalid(violation))
}

/// The bytes a range asks for. An inverted range asks for nothing; the store
/// refuses it on its own.
fn range_len(range: &Range<u64>) -> u64 {
    range.end.saturating_sub(range.start)
}

/// Runs one store request and records its latency under `kind` whatever the
/// outcome: a request that failed took time too.
async fn observed<T>(
    kind: ReadKind,
    request: impl Future<Output = object_store::Result<T>>,
) -> Result<T> {
    let started = Instant::now();
    let answer = request.await;
    observe_latency(kind, started.elapsed());
    answer.map_err(Error::from)
}

/// One observation of the GET latency histogram.
fn observe_latency(kind: ReadKind, elapsed: Duration) {
    metrics::histogram!(metric::OBJECT_GET_SECONDS, metric::LABEL_KIND => kind.to_string())
        .record(time::seconds(elapsed));
}

/// One lookup GET as the window remembers it.
#[derive(Clone, Copy, Debug)]
struct Sample {
    elapsed: Duration,
    hedged: bool,
}

/// A sliding window of recent lookup-GET latencies, owned by one `Table`
/// because a table's data file size sets its first-byte latency (RFD 013).
/// The hedge delay is the configured percentile of the last `window`
/// samples, and no delay is given while one more hedge would put the
/// window's share of hedged samples above the ceiling. The share counts
/// recorded attempts: a hedge still in flight is not in the window, so when
/// a stall catches several lookups at once each may hedge before any is
/// recorded, and the share can overshoot the ceiling until they are.
/// `config` has passed `Config::parse`'s validation: `window` is at least
/// one, `percentile` is in `(0, 1]`, and `max_rate` in `[0, 1]`.
#[derive(Debug)]
pub struct HedgeWindow {
    config: HedgeConfig,
    samples: Mutex<VecDeque<Sample>>,
}

impl HedgeWindow {
    /// An empty window; `delay` gives nothing until `window` samples are in.
    /// The ring grows as samples arrive and never past `window`, so a large
    /// configured window costs nothing until it is used.
    pub fn new(config: &HedgeConfig) -> Self {
        Self {
            config: config.clone(),
            samples: Mutex::new(VecDeque::new()),
        }
    }

    /// The delay after which a lookup GET is hedged, or `None` when no hedge
    /// may be issued: hedging is disabled, the window is not yet full (the
    /// configured window is the sample requirement), or one more hedged
    /// sample would put the share of hedged samples in the window above
    /// `max_rate` (RFD 013: while hedges would exceed the ceiling, none is
    /// issued). Otherwise the nearest-rank percentile of the window's
    /// elapsed times: the value at 1-based rank `ceil(percentile * window)`
    /// in the sorted samples.
    pub fn delay(&self) -> Option<Duration> {
        if !self.config.enabled {
            return None;
        }
        let samples = self.samples.lock().unwrap_or_else(PoisonError::into_inner);
        if samples.len() < self.config.window {
            return None;
        }
        let hedged = approx::count(samples.iter().filter(|sample| sample.hedged).count());
        let share_with_one_more = approx::ratio(hedged + 1, approx::count(samples.len()));
        if share_with_one_more > self.config.max_rate {
            return None;
        }
        let mut sorted: Vec<Duration> = samples.iter().map(|sample| sample.elapsed).collect();
        drop(samples);
        sorted.sort_unstable();
        nearest_rank(&sorted, self.config.percentile)
    }

    /// Records one lookup GET: how long it took and whether a hedge was
    /// issued for it. The oldest sample leaves once the window is full.
    pub fn record(&self, elapsed: Duration, hedged: bool) {
        let mut samples = self.samples.lock().unwrap_or_else(PoisonError::into_inner);
        if samples.len() >= self.config.window {
            samples.pop_front();
        }
        samples.push_back(Sample { elapsed, hedged });
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

/// The last credential the SDK chain resolved and when it stops being handed
/// out.
#[derive(Debug)]
struct CachedCredential {
    credential: Arc<AwsCredential>,
    /// `None` for a credential without an expiry, which is usable indefinitely.
    usable_until: Option<SystemTime>,
}

impl CachedCredential {
    /// A credential resolved at `now`. It is handed out until
    /// `CREDENTIAL_REFRESH_MARGIN` before its expiry, or for
    /// `CREDENTIAL_MIN_REUSE` when the expiry is nearer than that, and never
    /// past the expiry itself.
    fn new(credential: Arc<AwsCredential>, expiry: Option<SystemTime>, now: SystemTime) -> Self {
        let usable_until = expiry.map(|expiry| {
            let floor = now
                .checked_add(CREDENTIAL_MIN_REUSE)
                .map_or(expiry, |floor| floor.min(expiry));
            expiry
                .checked_sub(CREDENTIAL_REFRESH_MARGIN)
                .map_or(floor, |before_margin| before_margin.max(floor))
        });
        Self {
            credential,
            usable_until,
        }
    }

    /// Usable while `now` is before `usable_until`.
    fn is_fresh(&self, now: SystemTime) -> bool {
        self.usable_until.is_none_or(|until| until > now)
    }
}

/// The AWS SDK's credential chain presented to `object_store`, with the
/// resolved credential cached until `CREDENTIAL_REFRESH_MARGIN` before its
/// expiry, or for `CREDENTIAL_MIN_REUSE` when the chain answered with one
/// already inside that margin. Without the cache every GET would run the
/// chain, and a chain backed by `credential_process` spawns a process per
/// run (RFD 003). The cache lives behind a tokio mutex held across the
/// chain's own await, so concurrent callers that all find it stale collapse
/// into one refresh: the first to take the lock refreshes, and each of the
/// rest finds a fresh credential when its turn comes.
#[derive(Debug)]
struct SdkCredentials {
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

    /// The cached credential while it is fresh, otherwise the chain's
    /// answer, cached. The lock is held across the chain's await on purpose.
    async fn current(&self) -> object_store::Result<Arc<AwsCredential>> {
        let mut cached = self.cached.lock().await;
        if let Some(entry) = cached
            .as_ref()
            .filter(|entry| entry.is_fresh(time::system_now()))
        {
            return Ok(Arc::clone(&entry.credential));
        }
        let resolved = self
            .provider
            .provide_credentials()
            .await
            .map_err(|source| object_store::Error::Generic {
                store: S3_STORE_NAME,
                source: Box::new(source),
            })?;
        let credential = Arc::new(AwsCredential {
            key_id: resolved.access_key_id().to_string(),
            secret_key: resolved.secret_access_key().to_string(),
            token: resolved.session_token().map(str::to_string),
        });
        *cached = Some(CachedCredential::new(
            Arc::clone(&credential),
            resolved.expiry(),
            time::system_now(),
        ));
        Ok(credential)
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use aws_credential_types::Credentials;
    use aws_credential_types::provider::future;
    use futures::future::join_all;
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::throttle::{ThrottleConfig, ThrottledStore};
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use tokio::sync::Notify;

    use super::*;
    use crate::config::RewriteRule;

    /// The synthetic prefix tests register in-memory stores under.
    const MEM_PREFIX: &str = "mem://w/";
    /// The key of the one test object under the prefix.
    const OBJECT_KEY: &str = "db/t/f.bin";
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
        u64::try_from(PAYLOAD.len()).unwrap()
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
            .locate(&format!("{MEM_PREFIX}{OBJECT_KEY}"))
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

    /// The configuration violation a `locate` refused with.
    fn locate_violation(result: Result<Located>) -> ConfigViolation {
        let Err(Error::Config(ConfigError::Invalid(violation))) = result else {
            panic!("expected a configuration violation");
        };
        violation
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
        calls: AtomicUsize,
        release: Notify,
        done: Notify,
    }

    impl Scripted {
        fn new(script: &[Step]) -> Self {
            Self {
                inner: InMemory::new(),
                script: script.to_vec(),
                calls: AtomicUsize::new(0),
                release: Notify::new(),
                done: Notify::new(),
            }
        }

        /// The step for the next GET.
        fn step(&self) -> Step {
            let call = self.calls.fetch_add(1, Ordering::Relaxed);
            self.script.get(call).copied().unwrap_or(PASS)
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

    /// A credential provider that counts its calls and yields once before
    /// answering, so concurrent callers overlap and a refresh that is not
    /// coalesced shows up as more than one call. It answers from `answers`
    /// in call order and repeats the last.
    #[derive(Debug)]
    struct CountingProvider {
        answers: Vec<Credentials>,
        calls: Arc<AtomicUsize>,
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
        let credential = Arc::new(AwsCredential {
            key_id: TEST_KEY_ID.to_string(),
            secret_key: TEST_SECRET.to_string(),
            token: None,
        });
        CachedCredential::new(credential, expiry, now)
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

    #[test]
    fn test_split_location_bare_absolute_path() {
        assert_eq!(
            split_location("/tmp/w/t").unwrap(),
            (FILE_PREFIX.to_string(), "/tmp/w/t".to_string())
        );
    }

    #[test]
    fn test_split_location_relative_path_refused() {
        let Err(Error::Config(ConfigError::Invalid(violation))) = split_location("relative/path")
        else {
            panic!("expected a configuration violation");
        };
        assert_eq!(
            violation,
            ConfigViolation::LocationNotAbsolute {
                location: "relative/path".to_string()
            }
        );
    }

    #[test]
    fn test_locate_unknown_scheme_refused() {
        let storage = Storage::new(StorageConfig::default());
        assert_eq!(
            locate_violation(storage.locate("gs://bucket-a/x")),
            ConfigViolation::NoStoreForLocation {
                location: "gs://bucket-a/x".to_string()
            }
        );
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
            locate_violation(storage.locate("gs://bucket-a/x")),
            ConfigViolation::NoStoreForLocation {
                location: "mem://none/x".to_string()
            }
        );
        assert_eq!(
            locate_violation(storage.locate("relative/x")),
            ConfigViolation::LocationNotAbsolute {
                location: "relative/x".to_string()
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

        let located = storage.locate("s3://old-warehouse/deep/x").unwrap();
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

        let untouched = storage.locate("mem://b/y").unwrap();
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
        let first = storage.locate("s3://bucket-a/x/y").unwrap();
        let again = storage.locate("s3://bucket-a/z").unwrap();
        let hadoop = storage.locate("s3a://bucket-a/x/y").unwrap();
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
        let located = storage.locate("s3://bucket-a/x").unwrap();
        assert_eq!(located.path, Path::from("x"));
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

        let by_uri = storage.locate(&format!("{FILE_PREFIX}{bare}")).unwrap();
        let by_path = storage.locate(bare).unwrap();
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
}
