//! An S3-protocol server in the test process (RFD 014): `s3s` speaking
//! HTTP/1 over loopback, `s3s-fs` storing objects in a temporary directory.
//! The real `object_store` client drives it, so a test exercises the request
//! path the server uses and counts every GET at the client.

use std::collections::HashMap;
use std::fmt;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;

use hyper::server::conn::http1;
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
    accept_loop: JoinHandle<()>,
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
        let accept_loop = tokio::spawn(accept(listener, service));
        Ok(Self {
            addr,
            bucket: bucket.to_string(),
            root,
            accept_loop,
        })
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
/// grow with the connection count.
async fn accept(listener: TcpListener, service: S3Service) {
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
        connections.spawn(async move {
            let connection = http1::Builder::new().serve_connection(TokioIo::new(stream), service);
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
    use super::*;

    /// The bucket the server is started with; any name the filesystem
    /// admits as a directory works.
    const BUCKET: &str = "icetray-test";

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
