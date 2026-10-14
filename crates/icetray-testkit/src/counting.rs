//! A store that counts the GETs it answers per object, so a test can say
//! which data files a load read and how often (RFD 005, RFD 025). A GET is
//! counted as `Storage` counts one: a ranged read of several ranges is one
//! GET per range.

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use icetray_core::FILE_PREFIX;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

/// A store over `inner` that counts every GET by the object it reads.
#[derive(Debug)]
pub struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    gets: Mutex<HashMap<Path, u64>>,
}

impl CountingStore {
    /// A counting store over the local filesystem, for registering under
    /// `FILE_PREFIX` on a `Storage` that reads a fixture's files.
    pub fn local() -> Self {
        Self {
            inner: Arc::new(LocalFileSystem::new()),
            gets: Mutex::new(HashMap::new()),
        }
    }

    /// GETs of the object at `location`, a local location as a manifest
    /// records it: a `file://` URI or a bare absolute path.
    pub fn gets_to(&self, location: &str) -> u64 {
        let path = location.strip_prefix(FILE_PREFIX).unwrap_or(location);
        let path = Path::from_absolute_path(path)
            .expect("a fixture's data file is at an absolute local path");
        self.gets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&path)
            .copied()
            .unwrap_or(0)
    }

    /// Counts `gets` GETs of `location`.
    fn count(&self, location: &Path, gets: usize) {
        let gets = u64::try_from(gets).expect("a count of ranges fits u64");
        *self
            .gets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(location.clone())
            .or_insert(0) += gets;
    }
}

impl fmt::Display for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.count(location, 1);
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.count(location, ranges.len());
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
