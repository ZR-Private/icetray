//! The gRPC contract: the service and messages every client speaks,
//! generated at build time from the checked-in protobuf files (RFD 010).
//! Everything public here is generated; the contract itself is
//! `proto/icetray.proto`, whose comments say what a caller may rely on.

/// Version 1 of the `icetray.v1` service and its messages.
pub mod v1 {
    // The inner allows are scoped to the generated code (RFD 015): one lint
    // per line, each present only because the generator's output trips it.
    // Nothing hand-written goes inside this module.
    #![allow(clippy::doc_markdown)]
    #![allow(clippy::default_trait_access)]
    #![allow(clippy::too_many_lines)]
    tonic::include_proto!("icetray.v1");
}
