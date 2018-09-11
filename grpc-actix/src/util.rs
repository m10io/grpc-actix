//! Miscellaneous utility code.

use actix::prelude::*;
use futures::prelude::*;

use super::status::*;
use actix::dev::ToEnvelope;
use std::borrow::Cow;

/// Extension trait for [`actix::Addr`].
///
/// [`actix::Addr`]: https://docs.rs/actix/0.7/actix/struct.Addr.html
pub trait AddrExt<M>
where
    M: Message + Send,
    M::Result: Send,
{
    /// Future return type of [`status_send()`].
    ///
    /// [`Status`]: struct.Status.html
    /// [`status_send()`]: #fn.status_send.html
    type StatusErrorResult: Future<Item = M::Result, Error = Status>;

    /// Future return type of [`logged_send()`].
    ///
    /// [`logged_send()`]: #fn.logged_send.html
    type LoggedErrorResult: Future<Item = M::Result, Error = MailboxError>;

    /// Send an asynchronous message via [`send()`], mapping any mailbox error to a gRPC [`Status`].
    ///
    /// The `message_name` and `target_name` parameters are used to generate the gRPC status
    /// message.
    ///
    /// [`send()`]: https://docs.rs/actix/0.7/actix/struct.Addr.html#method.send
    /// [`Status`]: struct.Status.html
    fn status_send<T, U>(&self, msg: M, message_name: T, target_name: U) -> Self::StatusErrorResult
    where
        T: Into<Cow<'static, str>>,
        U: Into<Cow<'static, str>>;

    /// Send an asynchronous message via [`send()`], logging any mailbox error and passing through
    /// the mailbox error value.
    ///
    /// The `message_name` and `target_name` parameters are used to generate the error message.
    ///
    /// [`send()`]: https://docs.rs/actix/0.7/actix/struct.Addr.html#method.send
    fn logged_send<T, U>(&self, msg: M, message_name: T, target_name: U) -> Self::LoggedErrorResult
    where
        T: Into<Cow<'static, str>>,
        U: Into<Cow<'static, str>>;
}

impl<M, A> AddrExt<M> for Addr<A>
where
    M: Message + Send,
    M::Result: Send,
    A: Handler<M>,
    A::Context: ToEnvelope<A, M>,
{
    type StatusErrorResult = StatusRequest<A, M>;
    type LoggedErrorResult = LoggedRequest<A, M>;

    fn status_send<T, U>(&self, msg: M, message_name: T, target_name: U) -> Self::StatusErrorResult
    where
        T: Into<Cow<'static, str>>,
        U: Into<Cow<'static, str>>,
    {
        StatusRequest {
            request: self.send(msg),
            message_name: message_name.into(),
            target_name: target_name.into(),
        }
    }

    fn logged_send<T, U>(&self, msg: M, message_name: T, target_name: U) -> Self::LoggedErrorResult
    where
        T: Into<Cow<'static, str>>,
        U: Into<Cow<'static, str>>,
    {
        LoggedRequest {
            request: self.send(msg),
            message_name: message_name.into(),
            target_name: target_name.into(),
        }
    }
}

/// Future for mapping the mailbox error of an actix [`Request`] to a gRPC [`Status`].
///
/// [`Request`]: https://docs.rs/actix/0.7/actix/prelude/struct.Request.html
/// [`Status`]: struct.Status.html
pub struct StatusRequest<A, M>
where
    A: Handler<M>,
    A::Context: ToEnvelope<A, M>,
    M: Message + Send,
    M::Result: Send,
{
    /// [`Request`] future.
    ///
    /// [`Request`]: https://docs.rs/actix/0.7/actix/prelude/struct.Request.html
    request: Request<A, M>,

    /// Message type sent.
    message_name: Cow<'static, str>,
    /// Target name string.
    target_name: Cow<'static, str>,
}

impl<A, M> Future for StatusRequest<A, M>
where
    A: Handler<M>,
    A::Context: ToEnvelope<A, M>,
    M: Message + Send,
    M::Result: Send,
{
    type Item = M::Result;
    type Error = Status;

    #[inline]
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.request
            .poll()
            .map_err(|e| Status::from_mailbox_error(&e, &*self.message_name, &*self.target_name))
    }
}

/// Future for logging the mailbox error of an actix [`Request`].
///
/// [`Request`]: https://docs.rs/actix/0.7/actix/prelude/struct.Request.html
pub struct LoggedRequest<A, M>
where
    A: Handler<M>,
    A::Context: ToEnvelope<A, M>,
    M: Message + Send,
    M::Result: Send,
{
    /// [`Request`] future.
    ///
    /// [`Request`]: https://docs.rs/actix/0.7/actix/prelude/struct.Request.html
    request: Request<A, M>,

    /// Message type sent.
    message_name: Cow<'static, str>,
    /// Target name string.
    target_name: Cow<'static, str>,
}

impl<A, M> Future for LoggedRequest<A, M>
where
    A: Handler<M>,
    A::Context: ToEnvelope<A, M>,
    M: Message + Send,
    M::Result: Send,
{
    type Item = M::Result;
    type Error = MailboxError;

    #[inline]
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.request.poll().map_err(|e| {
            error!(
                "Failed to send '{}' message to '{}': {}",
                self.message_name, self.target_name, e
            );
            e
        })
    }
}
