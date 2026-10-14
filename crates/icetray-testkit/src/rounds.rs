//! A store that counts the rounds of GETs a read takes, so a test or the
//! benchmark program can say how many round trips to object storage a Scan
//! waited on (RFD 025, Cost: one round across all files). A GET belongs to
//! the round after the deepest one any GET answered before it was issued:
//! GETs issued together share a round, and a GET issued once another has
//! answered, as a read that decodes one page before it asks for the next,
//! starts the next round. The count follows the order of issues and answers
//! alone, never time, so it is the same on a fast store and a slow one.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

/// A store over `inner` that counts the rounds of the GETs it answers since
/// it last restarted. A read's count is its own only while it is the one
/// read running between `restart` and `rounds`.
#[derive(Debug)]
pub struct RoundsStore {
    inner: Arc<dyn ObjectStore>,
    /// The deepest round any GET answered since the last restart.
    answered: AtomicU64,
}

impl RoundsStore {
    /// A rounds store over `inner`.
    pub fn over(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            answered: AtomicU64::new(0),
        }
    }

    /// A rounds store over the local filesystem, for registering under
    /// `FILE_PREFIX` on a `Storage` that reads a fixture's files.
    pub fn local() -> Self {
        Self::over(Arc::new(LocalFileSystem::new()))
    }

    /// The rounds of the GETs answered since the last restart: the deepest
    /// round any of them took, zero when none was issued.
    pub fn rounds(&self) -> u64 {
        self.answered.load(Ordering::Relaxed)
    }

    /// Starts counting anew, for the next read.
    pub fn restart(&self) {
        self.answered.store(0, Ordering::Relaxed);
    }

    /// Runs one GET, `request`, in the round after the deepest answered so
    /// far. The answer counts only after the GET's caller has been polled
    /// once more: a store that answers inside the poll that issued the GET
    /// would otherwise put the GETs issued after it in the same pass, which
    /// were issued together, in later rounds.
    async fn counted<T>(&self, request: impl Future<Output = T>) -> T {
        let round = self.rounds() + 1;
        let answer = request.await;
        tokio::task::yield_now().await;
        self.answered.fetch_max(round, Ordering::Relaxed);
        answer
    }
}

impl fmt::Display for RoundsStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RoundsStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for RoundsStore {
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
        self.counted(self.inner.get_opts(location, options)).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.counted(self.inner.get_ranges(location, ranges)).await
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

#[cfg(test)]
mod tests {
    use object_store::ObjectStoreExt as _;
    use object_store::memory::InMemory;

    use super::*;

    /// The object every test reads.
    const OBJECT: &str = "data/f0.parquet";

    /// The bytes it holds.
    const CONTENT: &[u8] = b"0123456789";

    /// One byte of `CONTENT`, the range a test reads alone.
    const FIRST_BYTE: Range<u64> = 0..1;

    /// Another byte of `CONTENT`, apart from `FIRST_BYTE`, so a ranged
    /// read of both asks two ranges.
    const THIRD_BYTE: Range<u64> = 2..3;

    /// Bytes a one-byte range covers.
    const ONE_BYTE: u64 = 1;

    /// A rounds store over an in-memory store holding `OBJECT`, which
    /// answers every GET inside the poll that issues it.
    async fn store() -> (RoundsStore, Path) {
        let inner = InMemory::new();
        let path = Path::from(OBJECT);
        inner.put(&path, CONTENT.into()).await.unwrap();
        (RoundsStore::over(Arc::new(inner)), path)
    }

    /// GETs issued together share a round, however fast the store answers.
    #[tokio::test]
    async fn test_rounds_of_gets_issued_together_is_one() {
        let (store, path) = store().await;
        let reads = (0..CONTENT.len()).map(|at| {
            let at = u64::try_from(at).unwrap();
            store.get_range(&path, at..at + ONE_BYTE)
        });
        futures::future::try_join_all(reads).await.unwrap();
        assert_eq!(store.rounds(), 1);
    }

    /// A GET issued after another answered starts the next round, and a
    /// restart counts from zero.
    #[tokio::test]
    async fn test_rounds_of_gets_one_after_another_counts_each() {
        let (store, path) = store().await;
        assert_eq!(store.rounds(), 0);
        let one_after_another = 3;
        for _ in 0..one_after_another {
            store.get_range(&path, FIRST_BYTE).await.unwrap();
        }
        assert_eq!(store.rounds(), one_after_another);
        store.restart();
        assert_eq!(store.rounds(), 0);
        let (first, second) = futures::join!(
            store.get_ranges(&path, &[FIRST_BYTE, THIRD_BYTE]),
            store.get(&path)
        );
        first.unwrap();
        second.unwrap();
        assert_eq!(store.rounds(), 1, "a ranged read and a GET issued together");
    }
}
