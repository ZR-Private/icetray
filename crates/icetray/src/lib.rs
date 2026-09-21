//! The one binary as a thin library: `serve` is the daemon (the gRPC
//! service over the generated contract, gRPC health, the metrics and
//! readiness listener, one refresh task per table, the twelve-factor
//! surface of RFD 012, a graceful drain) and `doctor`, `get`, and `info` are
//! one-shot subcommands printing JSON lines. Every subcommand opens tables
//! through `icetray_core::open` and shares one flag vocabulary; this library
//! holds everything `main` dispatches to, so the integration tests drive the
//! daemon in-process and the binary from outside.

pub mod cli;
pub mod error;
pub mod key_text;
pub mod listen;
pub mod logging;
pub mod oneshot;
pub mod output;
pub mod serve;
pub mod service;
pub mod telemetry;

pub use error::{EXIT_FAILURE, EXIT_INEFFICIENT, EXIT_OK, EXIT_USAGE, ServeError};
pub use serve::{ServeOptions, Serving};
