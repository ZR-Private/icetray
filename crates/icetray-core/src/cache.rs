//! The shared value-page cache (RFD 004): pages keyed by file and page
//! offset, never invalidated because a data file never changes, evicted by
//! size alone, with concurrent misses on one page coalesced into one fetch.

use std::sync::Arc;

use crate::error::Error;

/// Why the page cache could not produce a page.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    /// The fetch the cache ran for the caller failed. Every waiter coalesced
    /// onto that fetch receives the same failure, so it is shared.
    #[error("fetch failed: {0}")]
    Fetch(Arc<Error>),
    /// The cache itself failed, in the backend's own words.
    #[error("backend: {0}")]
    Backend(String),
}
