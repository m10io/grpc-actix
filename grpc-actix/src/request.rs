//! RPC request support.

use http;
use hyper;
use prost;

use futures::prelude::*;

use super::frame;
use futures::{future, stream};

use super::future::*;
use super::metadata::*;
use super::status::*;

/// Common trait for RPC request types.
pub trait Request {
    /// Converts an HTTP request into a future that produces an instance of this type.
    fn from_http_request(request: hyper::Request<hyper::Body>) -> GrpcFuture<Self>;

    /// Converts an instance of this type into a future that produces an HTTP request to be sent to
    /// the specified URI.
    fn into_http_request(self, uri: hyper::Uri) -> GrpcFuture<hyper::Request<hyper::Body>>;
}

/// RPC request containing a single message.
#[derive(Default)]
pub struct UnaryRequest<M>
where
    M: prost::Message + Default + 'static,
{
    pub metadata: Metadata,
    pub data: M,
}

impl<M> Request for UnaryRequest<M>
where
    M: prost::Message + Default + 'static,
{
    fn from_http_request(request: hyper::Request<hyper::Body>) -> GrpcFuture<Self> {
        Box::new(future::lazy(move || {
            let metadata = Metadata::from_header_map(request.headers());

            SingleItem::new(message_stream(request.into_body())).map(move |message| Self {
                metadata,
                data: message,
            })
        }))
    }

    fn into_http_request(self, uri: hyper::Uri) -> GrpcFuture<hyper::Request<hyper::Body>> {
        Box::new(future::lazy(move || {
            let mut data = Vec::new();
            frame::encode(&self.data, &mut data)?;

            request_builder(uri, self.metadata).and_then(|mut builder| {
                builder
                    .body(hyper::Body::from(data))
                    .map_err(|e| Status::from_display(StatusCode::Internal, e))
            })
        }))
    }
}

impl<M> From<M> for UnaryRequest<M>
where
    M: prost::Message + Default + 'static,
{
    fn from(data: M) -> Self {
        Self {
            metadata: Metadata::default(),
            data,
        }
    }
}

/// RPC request containing a message stream.
pub struct StreamingRequest<M>
where
    M: prost::Message + Default + 'static,
{
    pub metadata: Metadata,
    pub data: GrpcStream<M>,
}

impl<M> Default for StreamingRequest<M>
where
    M: prost::Message + Default + 'static,
{
    fn default() -> Self {
        Self {
            metadata: Metadata::default(),
            data: Box::new(stream::empty()),
        }
    }
}

impl<M> Request for StreamingRequest<M>
where
    M: prost::Message + Default + 'static,
{
    fn from_http_request(request: hyper::Request<hyper::Body>) -> GrpcFuture<Self> {
        Box::new(future::lazy(move || {
            Ok(Self {
                metadata: Metadata::from_header_map(request.headers()),
                data: Box::new(message_stream(request.into_body())),
            })
        }))
    }

    fn into_http_request(self, uri: hyper::Uri) -> GrpcFuture<hyper::Request<hyper::Body>> {
        Box::new(future::lazy(move || {
            let data_stream = self.data.and_then(|message| {
                let mut data = Vec::new();
                frame::encode(&message, &mut data)?;

                Ok(hyper::Chunk::from(data))
            });

            request_builder(uri, self.metadata).and_then(|mut builder| {
                builder
                    .body(hyper::Body::wrap_stream(data_stream))
                    .map_err(|e| Status::from_display(StatusCode::Internal, e))
            })
        }))
    }
}

impl<M> From<GrpcStream<M>> for StreamingRequest<M>
where
    M: prost::Message + Default + 'static,
{
    fn from(data: GrpcStream<M>) -> Self {
        Self {
            metadata: Metadata::default(),
            data,
        }
    }
}

/// Returns a builder for a [`hyper::Request`] for the specified URI and metadata, with standard
/// settings for gRPC use.
///
/// [`hyper::Request`]: https://docs.rs/hyper/0.12/hyper/struct.Request.html
fn request_builder(uri: hyper::Uri, metadata: Metadata) -> Result<http::request::Builder, Status> {
    let mut builder = hyper::Request::post(uri);
    builder
        .version(http::Version::HTTP_2)
        .header(http::header::TE, "trailers")
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .header(
            http::header::USER_AGENT,
            format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")).as_str(),
        );
    metadata.append_to_headers(&mut builder)?;

    Ok(builder)
}
