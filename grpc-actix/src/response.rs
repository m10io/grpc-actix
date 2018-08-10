//! RPC response support.

use http;
use hyper;
use prost;

use futures::prelude::*;

use super::frame;
use futures::{future, stream, task};

use super::future::*;
use super::metadata::*;
use super::status::*;
use futures::task::Task;
use hyper::body::Payload;
use parking_lot::Mutex;
use std::io::Cursor;
use std::sync::Arc;

/// Custom [`Payload`] type for generating body data with trailers.
///
/// [`Payload`]: https://docs.rs/hyper/0.12/hyper/body/trait.Payload.html
pub struct ResponsePayload {
    /// Data stream.
    data: GrpcStream<hyper::Chunk>,
    /// Trailers future.
    trailers: GrpcFuture<Option<hyper::HeaderMap>>,
}

impl ResponsePayload {
    /// Creates a new instance from a [`prost::Message`] stream and trailing [`Metadata`] future.
    ///
    /// [`prost::Message`]: https://docs.rs/prost/0.4/prost/trait.Message.html
    /// [`Metadata`]: struct.Metadata.html
    pub fn new<S, F>(messages: S, trailing_metadata: F) -> Self
    where
        S: Stream + Send + 'static,
        S::Item: prost::Message + 'static,
        S::Error: Into<Status>,
        F: Future<Item = Metadata> + Send + 'static,
        F::Error: Into<Status>,
    {
        let data = Box::new(messages.map_err(S::Error::into).and_then(|message| {
            let mut data = Vec::new();
            frame::encode(&message, &mut data)?;

            Ok(hyper::Chunk::from(data))
        }));

        let trailers = Box::new(
            trailing_metadata
                .map_err(F::Error::into)
                .and_then(|metadata| {
                    let mut trailers = hyper::HeaderMap::new();
                    trailers.append("grpc-status", http::header::HeaderValue::from_static("0"));
                    metadata.append_to_headers(&mut trailers)?;

                    Ok(Some(trailers))
                }),
        );

        Self { data, trailers }
    }

    /// Creates a payload for an error response that only contains trailing metadata.
    pub fn trailers_only<F>(status: Status, trailing_metadata: F) -> Self
    where
        F: Future<Item = Metadata> + Send + 'static,
        F::Error: Into<Status>,
    {
        let data = Box::new(stream::empty());

        let trailers = Box::new(trailing_metadata.map_err(F::Error::into).and_then(
            move |metadata| {
                let mut trailers = hyper::HeaderMap::new();

                let code_value = http::header::HeaderValue::from_bytes(
                    format!("{}", status.code).as_str().as_bytes(),
                ).map_err(|_| {
                    Status::new(
                        StatusCode::Internal,
                        Some("failed to parse status code as an HTTP header value"),
                    )
                })?;
                trailers.append("grpc-status", code_value);

                if let Some(message) = &status.message {
                    let mut encoded_message = Vec::new();
                    percent_encode(message, &mut encoded_message);

                    let message_value = http::header::HeaderValue::from_bytes(&encoded_message)
                        .map_err(|_| {
                            Status::new(
                                StatusCode::Internal,
                                Some("failed to parse status message as an HTTP header value"),
                            )
                        })?;
                    trailers.append("grpc-message", message_value);
                }

                metadata.append_to_headers(&mut trailers)?;

                Ok(Some(trailers))
            },
        ));

        Self { data, trailers }
    }
}

impl hyper::body::Payload for ResponsePayload {
    type Data = hyper::Chunk;
    type Error = Status;

    fn poll_data(&mut self) -> Poll<Option<Self::Data>, Self::Error> {
        self.data.poll()
    }

    fn poll_trailers(&mut self) -> Poll<Option<hyper::HeaderMap>, Self::Error> {
        self.trailers.poll()
    }
}

/// Common trait for RPC response types.
pub trait Response {
    /// Converts an HTTP response into a future that produces an instance of this type.
    fn from_http_response(response: hyper::Response<hyper::Body>) -> GrpcFuture<Self>;

    /// Converts an instance of this type into a future that produces an HTTP response.
    fn into_http_response(self) -> GrpcFuture<hyper::Response<ResponsePayload>>;
}

/// RPC response containing a single message.
pub struct UnaryResponse<M>
where
    M: prost::Message + Default + 'static,
{
    pub metadata: Metadata,
    pub data: M,
    pub trailing_metadata: Metadata,
}

impl<M> Response for UnaryResponse<M>
where
    M: prost::Message + Default + 'static,
{
    fn from_http_response(response: hyper::Response<hyper::Body>) -> GrpcFuture<Self> {
        Box::new(future::lazy(move || {
            let metadata = Metadata::from_header_map(response.headers());
            let (messages, trailing_metadata) =
                message_stream_and_trailing_metadata_future(response.into_body());

            SingleItem::new(messages).join(trailing_metadata).map(
                move |(message, trailing_metadata)| Self {
                    metadata,
                    data: message,
                    trailing_metadata,
                },
            )
        }))
    }

    fn into_http_response(self) -> GrpcFuture<hyper::Response<ResponsePayload>> {
        Box::new(future::lazy(move || {
            let payload = ResponsePayload::new(
                stream::once::<_, Status>(Ok(self.data)),
                future::ok::<_, Status>(self.trailing_metadata),
            );

            response_builder(self.metadata).and_then(move |mut builder| {
                builder
                    .body(payload)
                    .map_err(|e| Status::from_display(StatusCode::Internal, e))
            })
        }))
    }
}

/// RPC response containing a message stream.
pub struct StreamingResponse<M>
where
    M: prost::Message + Default + 'static,
{
    pub metadata: Metadata,
    pub data: GrpcStream<M>,
    pub trailing_metadata: GrpcFuture<Metadata>,
}

impl<M> Response for StreamingResponse<M>
where
    M: prost::Message + Default + 'static,
{
    fn from_http_response(response: hyper::Response<hyper::Body>) -> GrpcFuture<Self> {
        Box::new(future::lazy(move || {
            let metadata = Metadata::from_header_map(response.headers());
            let (messages, trailing_metadata) =
                message_stream_and_trailing_metadata_future(response.into_body());

            Ok(Self {
                metadata,
                data: Box::new(messages),
                trailing_metadata: Box::new(trailing_metadata),
            })
        }))
    }

    fn into_http_response(self) -> GrpcFuture<hyper::Response<ResponsePayload>> {
        Box::new(future::lazy(move || {
            let payload = ResponsePayload::new(self.data, self.trailing_metadata);

            response_builder(self.metadata).and_then(move |mut builder| {
                builder
                    .body(payload)
                    .map_err(|e| Status::from_display(StatusCode::Internal, e))
            })
        }))
    }
}

/// Creates a "trailers-only" RPC response for gRPC errors.
pub fn error_response(
    status: Status,
    trailing_metadata_opt: Option<Metadata>,
) -> GrpcFuture<hyper::Response<ResponsePayload>> {
    Box::new(future::lazy(move || {
        let payload = ResponsePayload::trailers_only(
            status,
            future::ok::<_, Status>(trailing_metadata_opt.unwrap_or_default()),
        );

        response_builder(Metadata::default()).and_then(move |mut builder| {
            builder
                .body(payload)
                .map_err(|e| Status::from_display(StatusCode::Internal, e))
        })
    }))
}

//Creates a new RPC response future with any errors as gRPC status trailers.
pub fn flatten_response(
    response: GrpcFuture<hyper::Response<ResponsePayload>>
) -> GrpcFuture<hyper::Response<ResponsePayload>> {
    Box::new(response.then(|result| match result {
        Ok(res) => Box::new(future::ok(res)),
        Err(err) => {
            error_response(err, None)
        }
    }))
}


/// Processing state of the `Stream` used to provide [`Body`] data.
///
/// [`Body`]: https://docs.rs/hyper/0.12/hyper/struct.Body.html
enum DataState {
    /// Data stream is still processing. If the trailers future has been polled, this will also
    /// contain the [`Task`] used to wake up the trailers future once the data stream has finished.
    ///
    /// [`Task`]: https://docs.rs/futures/0.1/futures/task/struct.Task.html
    Polling(Option<Task>),

    /// Data stream has finished (trailers can now be polled). The [`Body`] is also provided so that
    /// the trailers future can take ownership of it for its processing.
    ///
    /// [`Body`]: https://docs.rs/hyper/0.12/hyper/struct.Body.html
    Finished(Option<hyper::Body>),
}

/// Processing state of the `Future` used to provide [`Body`] trailers.
///
/// [`Body`]: https://docs.rs/hyper/0.12/hyper/struct.Body.html
enum TrailersState {
    /// Waiting for data stream to finish processing. The data stream will update this shared value
    /// once it has finished its work.
    Waiting(Arc<Mutex<DataState>>),
    /// Trailers are now being polled.
    Polling(hyper::Body),
}

/// Stream used to provide the data from a [`Body`] instance in association with a
/// [`TrailersFuture`].
///
/// [`Body`]: https://docs.rs/hyper/0.12/hyper/struct.Body.html
/// [`TrailersFuture`]: struct.TrailersFuture.html
struct DataStream {
    /// `Body` instance (passed to the `TrailersFuture` once data streaming has completed).
    body_opt: Option<hyper::Body>,
    /// Processing state.
    state: Arc<Mutex<DataState>>,
}

impl Stream for DataStream {
    type Item = hyper::Chunk;
    type Error = hyper::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self.body_opt.take() {
            Some(mut body) => {
                let poll = body.poll_data();
                match &poll {
                    Ok(Async::Ready(None)) => {
                        // Wake up the trailers future if it has already been polled.
                        let mut guard = self.state.lock();
                        if let DataState::Polling(Some(task)) = &*guard {
                            task.notify();
                        }

                        *guard = DataState::Finished(Some(body));
                    }

                    _ => {
                        self.body_opt = Some(body);
                    }
                }

                poll
            }

            None => Ok(Async::Ready(None)),
        }
    }
}

/// Future used to provide the trailers from a [`Body`] instance.
///
/// [`Body`]: https://docs.rs/hyper/0.12/hyper/struct.Body.html
struct TrailersFuture {
    /// Processing state.
    state: TrailersState,
}

impl Future for TrailersFuture {
    type Item = Option<hyper::HeaderMap>;
    type Error = hyper::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // Check for whether the data stream has finished, taking ownership of the `Body` instance
        // if so.
        let taken_body = if let TrailersState::Waiting(data_state) = &self.state {
            let mut guard = data_state.lock();
            match &mut *guard {
                DataState::Polling(task) => {
                    *task = Some(task::current());

                    return Ok(Async::NotReady);
                }

                DataState::Finished(body) => {
                    assert!(
                        body.is_some(),
                        "synchronization error between response data stream and trailers future"
                    );

                    body.take()
                }
            }
        } else {
            None
        };

        if let Some(body) = taken_body {
            self.state = TrailersState::Polling(body);
        }

        // Update trailers polling.
        if let TrailersState::Polling(body) = &mut self.state {
            body.poll_trailers()
        } else {
            unreachable!("response trailers future reached an invalid state");
        }
    }
}

/// Returns a future that parses trailing [`Metadata`] from a future returning HTTP trailers.
///
/// [`Metadata`]: struct.Metadata.html
/// [`TrailersFuture`]: struct.TrailersFuture.html
fn trailing_metadata_future<F>(trailers_future: F) -> impl Future<Item = Metadata, Error = Status>
where
    F: Future<Item = Option<hyper::HeaderMap>>,
    F::Error: Into<Status>,
{
    trailers_future
        .map_err(F::Error::into)
        .and_then(|trailers_opt| {
            match trailers_opt {
                None => Err(Status::new(
                    StatusCode::Unavailable,
                    Some("no trailers received"),
                )),
                Some(trailers) => {
                    // Check for "grpc-status" and "grpc-message" trailers.
                    let status_value = trailers.get_all("grpc-status").iter().last().ok_or_else(
                        || {
                            Status::new(
                                StatusCode::Unavailable,
                                Some("missing 'grpc-status' trailer"),
                            )
                        },
                    )?;
                    let status_str = status_value.to_str().map_err(|_| {
                        Status::new(
                            StatusCode::Unavailable,
                            Some("'grpc-status' trailer is not a valid string"),
                        )
                    })?;
                    let status: u32 = status_str.parse().map_err(|_| {
                        Status::new(
                            StatusCode::Unavailable,
                            Some("'grpc-status' trailer does not contain a valid integer value"),
                        )
                    })?;

                    if status != 0 {
                        let message = trailers
                            .get_all("grpc-message")
                            .iter()
                            .last()
                            .map(|value| percent_decode(Cursor::new(value.as_bytes())));
                        Err(Status::new(status, message))
                    } else {
                        Ok(Metadata::from_header_map(&trailers))
                    }
                }
            }
        })
}

/// Returns a stream providing [`prost::Message`] instances and a future providing the trailing
/// [`Metadata`] from a [`Body`] instance.
///
/// [`prost::Message`]: https://docs.rs/prost/0.4/prost/trait.Message.html
/// [`Metadata`]: struct.Metadata.html
/// [`Body`]: https://docs.rs/hyper/0.12/hyper/struct.Body.html
fn message_stream_and_trailing_metadata_future<M>(
    body: hyper::Body,
) -> (
    impl Stream<Item = M, Error = Status>,
    impl Future<Item = Metadata, Error = Status>,
)
where
    M: prost::Message + Default,
{
    let data_state = Arc::new(Mutex::new(DataState::Polling(None)));
    let data_stream = DataStream {
        body_opt: Some(body),
        state: Arc::clone(&data_state),
    };
    let trailers_future = TrailersFuture {
        state: TrailersState::Waiting(data_state),
    };

    (
        message_stream(data_stream),
        trailing_metadata_future(trailers_future),
    )
}

/// Returns a builder for a [`hyper::Response`] for the specified metadata, with standard settings
/// for gRPC use.
///
/// [`hyper::Response`]: https://docs.rs/hyper/0.12/hyper/struct.Response.html
fn response_builder(metadata: Metadata) -> Result<http::response::Builder, Status> {
    let mut builder = hyper::Response::builder();
    builder
        .version(http::Version::HTTP_2)
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .header(
            http::header::USER_AGENT,
            format!("{}/{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")).as_str(),
        );
    metadata.append_to_headers(&mut builder)?;

    Ok(builder)
}
