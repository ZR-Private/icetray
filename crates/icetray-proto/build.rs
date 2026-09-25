//! Generates the tonic/prost code for `proto/icetray.proto` into `OUT_DIR`
//! with the protobuf compiler the build carries, so building from source
//! needs no `protoc` on the host (RFD 012).

use std::error::Error;

/// The contract this crate compiles. An edit to it reruns the generator.
const CONTRACT: &str = "proto/icetray.proto";

/// The directory the contract's imports resolve against.
const INCLUDE_DIR: &str = "proto";

/// The fields generated as `bytes::Bytes` rather than `Vec<u8>`: the
/// values a lookup answers with, so the server hands the bytes it read to
/// the response without copying them into a new vector.
const BYTES_FIELDS: [&str; 2] = [".icetray.v1.GetResponse.value", ".icetray.v1.Hit.value"];

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo::rerun-if-changed={CONTRACT}");
    let mut config = tonic_prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    config.bytes(BYTES_FIELDS);
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_with_config(config, &[CONTRACT], &[INCLUDE_DIR])?;
    Ok(())
}
