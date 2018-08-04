//! gRPC future types.

use futures::prelude::*;

use super::status::*;

/// Shorthand for general [`Future`] trait objects that provide a [`Status`] error.
///
/// [`Future`]: https://docs.rs/futures/0.1/futures/future/trait.Future.html
/// [`Status`]: struct.Status.html
pub type GrpcFuture<I> = Box<Future<Item = I, Error = Status>>;

/// Shorthand for general boxed [`Stream`] trait objects that provide a [`Status`] error.
///
/// [`Stream`]: https://docs.rs/futures/0.1/futures/stream/trait.Stream.html
/// [`Status`]: struct.Status.html
pub type GrpcStream<I> = Box<Stream<Item = I, Error = Status> + Send>;
