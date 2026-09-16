//! Generates the tonic/prost code for `proto/icetray.proto` into `OUT_DIR`
//! with the protobuf compiler the build carries, so building from source
//! needs no `protoc` on the host (RFD 012).

use std::error::Error;

/// The contract this crate compiles. An edit to it reruns the generator.
const CONTRACT: &str = "proto/icetray.proto";

/// The directory the contract's imports resolve against.
const INCLUDE_DIR: &str = "proto";

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo::rerun-if-changed={CONTRACT}");
    let mut config = tonic_prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_with_config(config, &[CONTRACT], &[INCLUDE_DIR])?;
    Ok(())
}
