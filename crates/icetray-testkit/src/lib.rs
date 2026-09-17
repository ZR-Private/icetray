//! Test infrastructure shared across the workspace: an in-process S3 server,
//! the fixture writer, the deterministic simulation transport, shared
//! assertions, and the golden-table locator (RFD 014).

pub mod error;
pub mod fixture;
pub mod golden;
pub mod keys;
pub mod layout;
pub mod s3;
pub mod sim;
pub mod verify;

pub use error::{Result, TestkitError};
pub use fixture::{
    Fixture, FixtureSpec, HandSnapshot, Nulls, Order, RawFile, memory_catalog_local,
    memory_catalog_s3, write_fixture,
};
pub use keys::{
    absent_key_from, absent_key_near, far_key, key_at, key_at_padded, value_for, value_for_len,
};
pub use layout::{Bloom, Dictionary, Layout, Statistics};
pub use s3::S3Fixture;
