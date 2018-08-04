//! Actor-based gRPC client and server implementation.

#[macro_use]
extern crate failure;
extern crate hyper;

mod metadata;
mod status;

pub use metadata::*;
pub use status::*;
