//! Test infrastructure shared across the workspace: an in-process S3 server,
//! the fixture writer, the deterministic simulation transport, shared
//! assertions, the golden-table locator (RFD 014), and the truth file a
//! check replays against a running server.

pub mod error;
pub mod fixture;
pub mod golden;
pub mod keys;
pub mod layout;
pub mod s3;
pub mod sim;
pub mod truth;
pub mod verify;

pub use error::{Result, TestkitError};
pub use fixture::{
    Fixture, FixtureSpec, HandSnapshot, Nulls, Order, RawFile, memory_catalog_local,
    memory_catalog_s3, write_fixture,
};
/// The catalog `memory_catalog_local` and `memory_catalog_s3` return, so a
/// caller without an `iceberg` dependency can hold it.
pub use iceberg::memory::MemoryCatalog;
pub use keys::{
    KeyShape, ValueKind, absent_key_from, absent_key_near, far_key, key_at, key_at_padded,
    key_at_shaped, scattered_key_at, text_value_for_len, value_for, value_for_len, value_of_kind,
};
pub use layout::{Bloom, Dictionary, Layout, Statistics};
pub use s3::S3Fixture;
