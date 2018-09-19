//! Shared gRPC client code.

use actix;
use hyper;

use futures::prelude::*;

use futures::future;
use http::uri;

use super::future::*;
use super::request::Request;
use super::response::Response;
use super::status::*;
use bytes::Bytes;
use http::uri::Uri;
use hyper::client::HttpConnector;
use std::sync::Arc;

#[cfg(feature = "timing")]
use parking_lot::Mutex;
#[cfg(feature = "timing")]
use std::time::Instant;
#[cfg(feature = "timing")]
use tokio::prelude::task;

/// [`future::Executor`] for client background `Connection` tasks.
///
/// [`future::Executor`]: https://docs.rs/futures/0.1/futures/future/trait.Executor.html
struct ClientExecutor;

impl<F> future::Executor<F> for ClientExecutor
where
    F: Future<Item = (), Error = ()> + 'static,
{
    #[inline]
    fn execute(&self, future: F) -> Result<(), future::ExecuteError<F>> {
        actix::spawn(future);

        Ok(())
    }
}

/// Client RPC handler for sending messages to a gRPC server.
#[derive(Debug)]
pub struct Client {
    /// HTTP client handler.
    http_client: Arc<hyper::Client<HttpConnector>>,
    /// Scheme component of server URI.
    server_scheme: uri::Scheme,
    /// Authority component of server URI.
    server_authority: uri::Authority,
}

impl Client {
    /// Creates a new RPC client instance.
    pub fn new(server_scheme: uri::Scheme, server_authority: uri::Authority) -> Self {
        let connector = HttpConnector::new_with_executor(ClientExecutor, None);
        Self {
            http_client: Arc::new(
                hyper::Client::builder()
                    .http2_only(true)
                    .executor(ClientExecutor)
                    .build(connector),
            ),
            server_scheme,
            server_authority,
        }
    }

    /// Sends an RPC message to the service at the specified path.
    pub fn send<ResponseT, RequestT>(
        &self,
        service: &str,
        request: RequestT,
    ) -> GrpcFuture<ResponseT>
    where
        ResponseT: Response + Send + 'static,
        RequestT: Request + Send + 'static,
    {
        let scheme = self.server_scheme.clone();
        let authority = self.server_authority.clone();
        let path_bytes = Bytes::from(service);
        let http_client = Arc::clone(&self.http_client);

        let uri_future = future::lazy(move || {
            let mut uri_parts = uri::Parts::default();
            uri_parts.scheme = Some(scheme);
            uri_parts.authority = Some(authority);
            uri_parts.path_and_query =
                Some(uri::PathAndQuery::from_shared(path_bytes).map_err(|_| {
                    Status::new(
                        StatusCode::Internal,
                        Some("client received an invalid service path"),
                    )
                })?);

            let uri = Uri::from_parts(uri_parts).map_err(|_| {
                Status::new(
                    StatusCode::Internal,
                    Some("failed to create full URI for client RPC request"),
                )
            })?;

            Ok(uri)
        });

        Box::new(uri_future.and_then(move |uri| {
            // `result_future` is modified at various points in the `and_then()` closure below
            // depending on whether the "timing" feature is enabled, so it's cleaner for us to
            // re-"let" it and return the final variable instead of trying to accommodate the
            // `let_and_return` lint.
            #[allow(unknown_lints, let_and_return)]
            request
                .into_http_request(uri)
                .and_then(move |http_request| {
                    #[cfg(feature = "timing")]
                    let request_time = Instant::now();

                    let result_future = http_client.request(http_request);

                    // The response future is unparked multiple times before the final result is
                    // yielded, but the first occurs when the task scheduler receives (or begins
                    // receiving) the HTTP response over the connection socket. Subsequent unparks
                    // occur after future processing begins within support libraries, and as such
                    // can occur after arbitrary delays based on when the task scheduler gets around
                    // to continuing future processing, so the first unpark is going to give us the
                    // least possible variation in timing.
                    #[cfg(feature = "timing")]
                    let result_future = InitialUnparkTimeFuture::new(result_future);

                    let result_future =
                        result_future
                            .map_err(Status::from)
                            .and_then(|http_response| {
                                #[cfg(feature = "timing")]
                                let (http_response, unpark_times) = http_response;

                                let result = ResponseT::from_http_response(http_response);

                                #[cfg(feature = "timing")]
                                let result = result.map(move |response| (response, unpark_times));

                                result
                            });

                    #[cfg(feature = "timing")]
                    let result_future = result_future.map(move |(mut response, unpark_times)| {
                        response.set_timing(
                            request_time,
                            Arc::try_unwrap(unpark_times).unwrap().get().unwrap(),
                        );
                        response
                    });

                    result_future
                })
        }))
    }
}

/// Future that wraps polling of another future, tracking when the task is initially unparked.
#[cfg(feature = "timing")]
struct InitialUnparkTimeFuture<F>
where
    F: Future,
{
    /// Wrapped future.
    future: F,
    /// Unpark time tracking.
    time: Arc<InitialUnparkTime>,
}

#[cfg(feature = "timing")]
impl<F> InitialUnparkTimeFuture<F>
where
    F: Future,
{
    /// Creates a new instance.
    pub fn new(future: F) -> Self {
        Self {
            future,
            time: Arc::default(),
        }
    }
}

#[cfg(feature = "timing")]
impl<F> Future for InitialUnparkTimeFuture<F>
where
    F: Future,
{
    type Item = (F::Item, Arc<InitialUnparkTime>);
    type Error = F::Error;

    #[inline]
    #[allow(deprecated)]
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let result = task::with_unpark_event(
            task::UnparkEvent::new(Arc::clone(&self.time) as Arc<task::EventSet>, 1),
            || self.future.poll(),
        );

        result.map(|ok| ok.map(|item| (item, Arc::clone(&self.time))))
    }
}

/// [`EventSet`] implementation for tracking when a task is initially unparked.
///
/// [`EventSet`]: https://docs.rs/tokio/0.1/tokio/prelude/task/trait.EventSet.html
#[cfg(feature = "timing")]
#[derive(Debug, Default)]
pub struct InitialUnparkTime {
    /// Initial unpark time tracking.
    time: Mutex<Option<Instant>>,
}

#[cfg(feature = "timing")]
impl InitialUnparkTime {
    /// Returns the time when the first unpark occurred.
    pub fn get(&self) -> Option<Instant> {
        *self.time.lock()
    }
}

#[cfg(feature = "timing")]
#[allow(deprecated)]
impl task::EventSet for InitialUnparkTime {
    #[inline]
    fn insert(&self, _id: usize) {
        let mut time_ref = self.time.lock();
        if time_ref.is_none() {
            *time_ref = Some(Instant::now());
        }
    }
}
