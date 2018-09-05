//! gRPC future types.

use hyper;
use prost;

use futures::prelude::*;

use frame;

use super::status::*;

/// Shorthand for general boxed [`Future`] trait objects that provide a [`Status`] error.
///
/// [`Future`]: https://docs.rs/futures/0.1/futures/future/trait.Future.html
/// [`Status`]: struct.Status.html
pub type GrpcFuture<I> = Box<Future<Item = I, Error = Status> + Send>;

/// Shorthand for general boxed [`Stream`] trait objects that provide a [`Status`] error.
///
/// [`Stream`]: https://docs.rs/futures/0.1/futures/stream/trait.Stream.html
/// [`Status`]: struct.Status.html
pub type GrpcStream<I> = Box<Stream<Item = I, Error = Status> + Send>;

/// [`Future`] that wraps a generic [`Stream`], ensuring it produces one and only one value.
///
/// [`Future`]: https://docs.rs/futures/0.1/futures/future/trait.Future.html
/// [`Stream`]: https://docs.rs/futures/0.1/futures/stream/trait.Stream.html
pub(crate) struct SingleItem<S>
where
    S: Stream,
    S::Error: Into<Status>,
{
    /// Wrapped `Stream`.
    stream: S,
    /// Polled item (cached until we can verify that there are no subsequent items).
    item: Option<S::Item>,
}

impl<S> SingleItem<S>
where
    S: Stream,
    S::Error: Into<Status>,
{
    /// Returns a new instance wrapping the given stream.
    pub fn new(stream: S) -> Self {
        Self { stream, item: None }
    }
}

impl<S> Future for SingleItem<S>
where
    S: Stream,
    S::Error: Into<Status>,
{
    type Item = Option<S::Item>;
    type Error = Status;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match self.stream.poll() {
                Ok(Async::NotReady) => {
                    return Ok(Async::NotReady);
                }

                Ok(Async::Ready(Some(item))) => {
                    if self.item.is_some() {
                        return Err(Status::new(
                            StatusCode::Unimplemented,
                            Some("expected single-message body, received more than one message"),
                        ));
                    }

                    self.item = Some(item);

                    // Don't return, but continue processing in case the stream is done or
                    // additional items are still pending.
                }

                Ok(Async::Ready(None)) => {
                    return Ok(Async::Ready(self.item.take()));
                }

                Err(e) => {
                    return Err(e.into());
                }
            }
        }
    }
}

/// Returns a [`Stream`] that generates messages from a [`Chunk`] stream.
///
/// [`Stream`]: https://docs.rs/futures/0.1/futures/stream/trait.Stream.html
/// [`Chunk`]: https://docs.rs/hyper/0.12/hyper/struct.Chunk.html
pub(crate) fn message_stream<M, S>(chunk_stream: S) -> impl Stream<Item = M, Error = Status>
where
    M: prost::Message + Default,
    S: Stream<Item = hyper::Chunk>,
    S::Error: Into<Status>,
{
    chunk_stream
        .map_err(S::Error::into)
        .filter(|chunk| !chunk.is_empty())
        .and_then(frame::decode)
}
