//! Miscellaneous utility code.

use actix::prelude::*;
use futures::prelude::*;

use std::mem;

use super::status::*;
use actix::dev::ToEnvelope;
use std::borrow::Cow;
use std::time::{Duration, Instant};
use tokio::timer::Delay;

/// General extension trait for [`actix::Addr`].
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

/// General extension trait for [`actix::Addr`] for retriable [`send()`] support.
///
/// [`actix::Addr`]: https://docs.rs/actix/0.7/actix/struct.Addr.html
/// [`send()`]: https://docs.rs/actix/0.7/actix/struct.Addr.html#method.send
pub trait AddrRetry<M, F, T, E>
where
    M: Message + Clone + Send,
    M::Result: Send,
    F: FnMut(Result<M::Result, MailboxError>) -> Option<Result<T, E>>,
{
    /// Future return type of [`send_with_retry_until()`].
    ///
    /// [`send_with_retry_until()`]: #fn.send_with_retry_until.html
    type Result: Future<Item = T, Error = E>;

    /// Send an asynchronous message via [`send()`], calling a closure on the result and retrying at
    /// after a specified delay time until the closure returns a `Some` result.
    ///
    /// The resulting future will yield the unpacked result of the provided closure.
    ///
    /// [`send()`]: https://docs.rs/actix/0.7/actix/struct.Addr.html#method.send
    fn send_with_retry_until(&self, msg: M, retry_delay: Duration, f: F) -> Self::Result;
}

impl<M, F, T, E, A> AddrRetry<M, F, T, E> for Addr<A>
where
    M: Message + Clone + Send,
    M::Result: Send,
    F: FnMut(Result<M::Result, MailboxError>) -> Option<Result<T, E>>,
    A: Handler<M>,
    A::Context: ToEnvelope<A, M>,
{
    type Result = RetryRequest<A, M, F, T, E>;

    fn send_with_retry_until(&self, msg: M, retry_delay: Duration, f: F) -> Self::Result {
        RetryRequest::new(self.clone(), msg, retry_delay, f)
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

/// Current future being polled by [`RetryRequest`].
///
/// [`RetryRequest`]: struct.RetryRequest.html
enum RetryRequestState<A, M>
where
    A: Handler<M>,
    A::Context: ToEnvelope<A, M>,
    M: Message,
{
    /// Waiting on a [`Request`] result.
    ///
    /// [`Request`]: https://docs.rs/actix/0.7/actix/prelude/struct.Request.html
    Request(Request<A, M>),

    /// Waiting on a [`Delay`] to elapse.
    ///
    /// [`Delay`]: https://docs.rs/tokio/0.1/tokio/timer/struct.Delay.html
    Delay(Delay),

    /// Reserved state when taken for processing.
    Invalid,
}

/// Future for retriable actix [`Addr::send()`] calls.
///
/// [`Addr::send()`]: https://docs.rs/actix/0.7/actix/struct.Addr.html#method.send
pub struct RetryRequest<A, M, F, T, E>
where
    A: Handler<M>,
    A::Context: ToEnvelope<A, M>,
    M: Message + Clone + Send,
    M::Result: Send,
    F: FnMut(Result<M::Result, MailboxError>) -> Option<Result<T, E>>,
{
    /// Address of the actor to which the message should be sent.
    addr: Addr<A>,
    /// Message to send.
    msg: M,

    /// Delay until the next send attempt after a rejected response.
    retry_delay: Duration,
    /// Closure used to test message responses.
    f: F,

    /// Current future being processed.
    state: RetryRequestState<A, M>,
}

impl<A, M, F, T, E> RetryRequest<A, M, F, T, E>
where
    A: Handler<M>,
    A::Context: ToEnvelope<A, M>,
    M: Message + Clone + Send,
    M::Result: Send,
    F: FnMut(Result<M::Result, MailboxError>) -> Option<Result<T, E>>,
{
    /// Creates a new instance of this type.
    pub fn new(addr: Addr<A>, msg: M, retry_delay: Duration, f: F) -> Self {
        let state = RetryRequestState::Request(addr.send(msg.clone()));
        Self {
            addr,
            msg,
            retry_delay,
            f,
            state,
        }
    }
}

impl<A, M, F, T, E> Future for RetryRequest<A, M, F, T, E>
where
    A: Handler<M>,
    A::Context: ToEnvelope<A, M>,
    M: Message + Clone + Send,
    M::Result: Send,
    F: FnMut(Result<M::Result, MailboxError>) -> Option<Result<T, E>>,
{
    type Item = T;
    type Error = E;

    #[inline]
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match mem::replace(&mut self.state, RetryRequestState::Invalid) {
                RetryRequestState::Request(mut request) => {
                    let result = match request.poll() {
                        Ok(Async::Ready(result)) => Ok(result),
                        Err(e) => Err(e),
                        Ok(Async::NotReady) => {
                            self.state = RetryRequestState::Request(request);
                            return Ok(Async::NotReady);
                        }
                    };

                    if let Some(final_result) = (self.f)(result) {
                        return final_result.map(Async::Ready);
                    }

                    self.state =
                        RetryRequestState::Delay(Delay::new(Instant::now() + self.retry_delay));
                }

                RetryRequestState::Delay(mut delay) => {
                    if let Ok(Async::NotReady) = delay.poll() {
                        self.state = RetryRequestState::Delay(delay);
                        return Ok(Async::NotReady);
                    }

                    self.state = RetryRequestState::Request(self.addr.send(self.msg.clone()));
                }

                RetryRequestState::Invalid => {
                    panic!("'RetryRequest' in an invalid state while polled");
                }
            }
        }
    }
}
