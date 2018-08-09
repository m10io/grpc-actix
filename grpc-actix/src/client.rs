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
        // TODO: Set custom `Executor` when building the HTTP client instance.
        Self {
            http_client: Arc::new(
                hyper::Client::builder()
                    .http2_only(true)
                    .executor(ClientExecutor)
                    .build_http(),
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
            request
                .into_http_request(uri)
                .and_then(move |http_request| {
                    http_client
                        .request(http_request)
                        .map_err(Status::from)
                        .and_then(ResponseT::from_http_response)
                })
        }))
    }
}
