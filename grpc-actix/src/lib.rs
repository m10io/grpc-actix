//! Actor-based gRPC client and server implementation.
#![feature(generic_associated_types)]

extern crate actix;
extern crate base64;
extern crate bytes;
#[macro_use]
extern crate failure;
extern crate futures;
extern crate http;
extern crate hyper;
#[macro_use]
extern crate log;
extern crate parking_lot;
extern crate prost;

#[cfg(test)]
#[macro_use]
extern crate prost_derive;

mod frame;
mod headers;

mod client;
mod future;
mod metadata;
mod request;
mod response;
mod status;
mod server;

pub use client::*;
pub use future::*;
pub use metadata::*;
pub use request::*;
pub use response::*;
pub use status::*;
pub use server::*;
