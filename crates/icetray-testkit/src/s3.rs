//! An S3-protocol server in the test process (RFD 014): `s3s` speaking
//! HTTP/1 over loopback, `s3s-fs` storing objects in a temporary directory.
//! The real `object_store` client drives it, so a test exercises the request
//! path the server uses and counts every GET at the client. `hold` stalls
//! every request until `release`, for the tests of what a wedged store does
//! to a caller, and `waiting` counts the held requests still open, so a
//! test sees a client give one up.

use std::collections::HashMap;
use std::fmt;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use hyper::Request;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use iceberg::io::{
    S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use icetray_core::config::{S3Config, StorageConfig};
use s3s::auth::SimpleAuth;
use s3s::service::{S3Service, S3ServiceBuilder};
use s3s_fs::FileSystem;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};

use crate::error::Result;
use crate::verify::storage_config_for_tests;

/// The static access key the fixture server accepts; callers outside the
/// crate reach it through `storage_config` and `file_io_props`.
pub(crate) const ACCESS_KEY: &str = "icetray-test-access-key";

/// The static secret the fixture server accepts.
pub(crate) const SECRET_KEY: &str = "icetray-test-secret-key";

/// The region clients claim; `s3s` does not check it.
pub(crate) const REGION: &str = "us-east-1";

/// Loopback with an OS-assigned port: the address the server binds.
const LOOPBACK_ANY_PORT: (Ipv4Addr, u16) = (Ipv4Addr::LOCALHOST, 0);

/// The scheme every S3 location under the fixture bucket carries.
const S3_SCHEME: &str = "s3://";

/// A running in-process S3 server with one bucket. Dropping it aborts the
/// accept loop, whose `JoinSet` of connection tasks aborts with it.
pub struct S3Fixture {
    addr: SocketAddr,
    bucket: String,
    root: TempDir,
    gate: Arc<Gate>,
    accept_loop: JoinHandle<()>,
}

/// Whether the server holds requests, how many it has held, and how many
/// of those are waiting now. A request that arrives while `closed` is true
/// waits until it turns false, or until its connection is dropped: hyper
/// drops a request's service future when the client closes the connection
/// it came on, which ends the wait.
struct Gate {
    closed: watch::Sender<bool>,
    held: AtomicUsize,
    waiting: AtomicUsize,
}

impl Gate {
    /// Waits while the gate is closed, counting the request as held when it
    /// has to wait and as waiting for as long as it does.
    async fn pass(&self) {
        let mut closed = self.closed.subscribe();
        if !*closed.borrow_and_update() {
            return;
        }
        self.held.fetch_add(1, Ordering::SeqCst);
        let _waiting = Waiting::enter(&self.waiting);
        // intentional: the sender lives in the gate this borrows, so the
        // wait cannot see it closed.
        let _ = closed.wait_for(|closed| !*closed).await;
    }
}

/// One held request counted in `Gate::waiting` until it is dropped, which
/// is when it is released or its connection goes.
struct Waiting<'a>(&'a AtomicUsize);

impl<'a> Waiting<'a> {
    fn enter(count: &'a AtomicUsize) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        Self(count)
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl fmt::Debug for S3Fixture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Fixture")
            .field("addr", &self.addr)
            .field("bucket", &self.bucket)
            .field("root", &self.root.path())
            .finish_non_exhaustive()
    }
}

impl Drop for S3Fixture {
    fn drop(&mut self) {
        self.accept_loop.abort();
    }
}

impl S3Fixture {
    /// Starts a server on an OS-assigned loopback port with `bucket`
    /// pre-created as a directory under a fresh temporary root.
    pub async fn start(bucket: &str) -> Result<Self> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir_all(root.path().join(bucket))?;
        let filesystem = FileSystem::new(root.path())?;
        let service = {
            let mut builder = S3ServiceBuilder::new(filesystem);
            builder.set_auth(SimpleAuth::from_single(ACCESS_KEY, SECRET_KEY));
            builder.build()
        };
        let listener = TcpListener::bind(LOOPBACK_ANY_PORT).await?;
        let addr = listener.local_addr()?;
        let gate = Arc::new(Gate {
            closed: watch::Sender::new(false),
            held: AtomicUsize::new(0),
            waiting: AtomicUsize::new(0),
        });
        let accept_loop = tokio::spawn(accept(listener, service, Arc::clone(&gate)));
        Ok(Self {
            addr,
            bucket: bucket.to_string(),
            root,
            gate,
            accept_loop,
        })
    }

    /// Holds every request that arrives from now until `release`: its
    /// connection is accepted and the request is answered once released,
    /// so a client sees a store that has stopped answering.
    pub fn hold(&self) {
        self.gate.closed.send_replace(true);
    }

    /// Answers the held requests and every later one again.
    pub fn release(&self) {
        self.gate.closed.send_replace(false);
    }

    /// How many requests have arrived while the server was held, over its
    /// whole life: a test waits on it to know its calls reached the store.
    pub fn held(&self) -> usize {
        self.gate.held.load(Ordering::SeqCst)
    }

    /// How many held requests are open now: a request stops counting when
    /// it is released or when its client gives it up and closes the
    /// connection it was sent on.
    pub fn waiting(&self) -> usize {
        self.gate.waiting.load(Ordering::SeqCst)
    }

    /// `http://127.0.0.1:<port>`.
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// The bucket that exists.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// The directory holding the bucket's objects, for inspection.
    pub fn root(&self) -> &Path {
        self.root.path()
    }

    /// `s3://<bucket>/<suffix>`.
    pub fn uri(&self, suffix: &str) -> String {
        format!(
            "{S3_SCHEME}{}/{}",
            self.bucket,
            suffix.trim_start_matches('/')
        )
    }

    /// `storage_config_for_tests` (no rewrites, no local directory, hedging
    /// disabled, so a cost test counts exactly the GETs the oracle
    /// predicts) pointing icetray's `object_store` at this server with the
    /// static keys and path-style addressing over plain HTTP.
    pub fn storage_config(&self) -> StorageConfig {
        StorageConfig {
            s3: Some(S3Config {
                endpoint: Some(self.endpoint()),
                region: Some(REGION.to_string()),
                access_key_id: Some(ACCESS_KEY.to_string()),
                secret_access_key: Some(SECRET_KEY.to_string()),
                allow_http: true,
                path_style: true,
            }),
            ..storage_config_for_tests()
        }
    }

    /// The properties the Iceberg library's S3 `FileIO` needs to reach this
    /// server, for the fixture writer and the memory catalog.
    pub fn file_io_props(&self) -> HashMap<String, String> {
        HashMap::from([
            (S3_ENDPOINT.to_string(), self.endpoint()),
            (S3_ACCESS_KEY_ID.to_string(), ACCESS_KEY.to_string()),
            (S3_SECRET_ACCESS_KEY.to_string(), SECRET_KEY.to_string()),
            (S3_REGION.to_string(), REGION.to_string()),
            (S3_PATH_STYLE_ACCESS.to_string(), true.to_string()),
        ])
    }
}

/// The accept loop: every connection is served on its own task in one
/// `JoinSet`, which aborts them all when the loop is aborted and dropped.
/// Finished tasks are joined as connections arrive, so the set does not
/// grow with the connection count. Every request passes `gate` first.
async fn accept(listener: TcpListener, service: S3Service, gate: Arc<Gate>) {
    let mut connections = JoinSet::new();
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(error) => {
                tracing::warn!(%error, "s3 fixture stopped accepting connections");
                return;
            }
        };
        while let Some(joined) = connections.try_join_next() {
            if let Err(error) = joined {
                tracing::warn!(%error, "s3 fixture connection task failed");
            }
        }
        let service = service.clone();
        let gate = Arc::clone(&gate);
        connections.spawn(async move {
            let gated = service_fn(move |request: Request<Incoming>| {
                let service = service.clone();
                let gate = Arc::clone(&gate);
                async move {
                    gate.pass().await;
                    // `S3Service` also has an inherent `call` over another
                    // body type; the trait method is the one hyper's body fits.
                    hyper::service::Service::call(&service, request).await
                }
            });
            let connection = http1::Builder::new().serve_connection(TokioIo::new(stream), gated);
            if let Err(error) = connection.await {
                // A client closing its pooled connection ends it with an error
                // that is nobody's failure.
                tracing::debug!(%error, "s3 fixture connection ended");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use icetray_core::Storage;

    use super::*;

    /// The bucket the server is started with; any name the filesystem
    /// admits as a directory works.
    const BUCKET: &str = "icetray-test";

    /// The object the hold test reads.
    const OBJECT: &str = "held/object";

    /// The object's bytes.
    const BODY: &[u8] = b"held";

    /// How long the hold test waits to see a held read not answered.
    const HELD_WAIT: Duration = Duration::from_millis(300);

    /// How long a released read may take before the test fails.
    const RELEASED_WAIT: Duration = Duration::from_secs(10);

    #[tokio::test(flavor = "multi_thread")]
    async fn test_hold_stalls_reads_until_release() {
        let s3 = S3Fixture::start(BUCKET).await.unwrap();
        let path = s3.root().join(BUCKET).join(OBJECT);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, BODY).unwrap();
        let storage = Arc::new(Storage::new(s3.storage_config()));
        let uri = s3.uri(OBJECT);
        assert_eq!(storage.read_all(&uri).await.unwrap().as_ref(), BODY);
        assert_eq!(s3.held(), 0, "nothing is held before hold");

        s3.hold();
        let read = tokio::spawn({
            let storage = Arc::clone(&storage);
            let uri = uri.clone();
            async move { storage.read_all(&uri).await }
        });
        tokio::time::sleep(HELD_WAIT).await;
        assert!(!read.is_finished(), "a held read is not answered");
        assert_eq!(s3.held(), 1);
        assert_eq!(s3.waiting(), 1);

        s3.release();
        let body = tokio::time::timeout(RELEASED_WAIT, read)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(body.as_ref(), BODY);
        assert_eq!(storage.read_all(&uri).await.unwrap().as_ref(), BODY);
        assert_eq!(s3.held(), 1, "a read after release is not held");
        assert_eq!(s3.waiting(), 0);
    }

    /// A held read its client drops stops waiting: the client closes the
    /// connection, and hyper drops the request with it.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_waiting_ends_when_the_client_drops_a_held_read() {
        let s3 = S3Fixture::start(BUCKET).await.unwrap();
        let path = s3.root().join(BUCKET).join(OBJECT);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, BODY).unwrap();
        let storage = Arc::new(Storage::new(s3.storage_config()));
        let uri = s3.uri(OBJECT);

        s3.hold();
        let read = tokio::spawn({
            let storage = Arc::clone(&storage);
            let uri = uri.clone();
            async move { storage.read_all(&uri).await }
        });
        tokio::time::sleep(HELD_WAIT).await;
        assert_eq!(s3.waiting(), 1, "the read is held");

        read.abort();
        tokio::time::sleep(HELD_WAIT).await;
        assert_eq!(s3.waiting(), 0, "the dropped read no longer waits");
        assert_eq!(s3.held(), 1);
    }

    #[tokio::test]
    async fn test_storage_config_points_at_the_bound_endpoint_with_hedging_off() {
        let s3 = S3Fixture::start(BUCKET).await.unwrap();
        let config = s3.storage_config();
        let s3_config = config.s3.unwrap();
        assert_eq!(s3_config.endpoint, Some(s3.endpoint()));
        assert!(s3.endpoint().starts_with("http://127.0.0.1:"));
        assert_eq!(s3_config.access_key_id.as_deref(), Some(ACCESS_KEY));
        assert_eq!(s3_config.secret_access_key.as_deref(), Some(SECRET_KEY));
        assert_eq!(s3_config.region.as_deref(), Some(REGION));
        assert!(s3_config.allow_http && s3_config.path_style);
        assert!(!config.hedge.enabled);
        assert!(config.rewrite.is_empty() && config.local.is_none());
        assert_eq!(s3.bucket(), BUCKET);
        assert_eq!(s3.uri("/warehouse/t"), format!("s3://{BUCKET}/warehouse/t"));
        assert!(s3.root().join(BUCKET).is_dir());
        assert_eq!(
            s3.file_io_props().get(S3_ENDPOINT),
            Some(&s3.endpoint()),
            "the writer's FileIO reaches the same server"
        );
    }
}
