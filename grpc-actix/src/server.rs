use actix::Actor;
use future::GrpcFuture;
use hyper::service::Service;
use hyper::{Body, Request, Response};
use response;
use response::ResponsePayload;
use status::{Status, StatusCode};
use std::collections::HashMap;

//Core HyperService implementation used with the MethodDispatch trait
pub struct GrpcHyperService<A: Actor> {
    pub dispatchers: HashMap<String, Box<dyn MethodDispatch<A>>>,
}

impl<A: Actor> GrpcHyperService<A> {
    pub fn new() -> GrpcHyperService<A> {
        GrpcHyperService {
            dispatchers: HashMap::new(),
        }
    }
}

impl<A: Actor> GrpcHyperService<A> {
    pub fn add_dispatch(&mut self, path: String, dispatch: Box<MethodDispatch<A>>) {
        self.dispatchers.insert(path, dispatch);
    }
}

impl<A: Actor> Service for GrpcHyperService<A> {
    type ReqBody = Body;
    type ResBody = ResponsePayload;
    type Error = Status;
    type Future = GrpcFuture<Response<ResponsePayload>>;

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let uri = req.uri().clone();
        let path = uri.path();
        match self.dispatchers.get(path) {
            Some(dispatcher) => response::flatten_response(dispatcher.dispatch(req))
                as GrpcFuture<Response<ResponsePayload>>,
            None => response::error_response(
                Status::new(StatusCode::NotFound, Some("That method could not be found")),
                None,
            ),
        }
    }
}

pub trait MethodDispatch<A: Actor> {
    fn dispatch(&self, request: Request<Body>) -> GrpcFuture<Response<ResponsePayload>>;
}
#[cfg(test)]
mod tests {
    use actix::{Actor, Context};
    use future::GrpcFuture;
    use futures::{future, stream, Future, Stream};
    use hyper::service::Service;
    use hyper::{Body, HeaderMap, Request, Response};
    use metadata::Metadata;
    use response::ResponsePayload;
    use server::{GrpcHyperService, MethodDispatch};
    use status::{Status, StatusCode};
    use frame;
    use bytes::Buf;

    #[derive(Clone, PartialEq, Message)]
    pub struct TestProstMessage {
        #[prost(string, tag="1")]
        test: String
    }

    struct TestActor;
    impl Actor for TestActor {
        type Context = Context<Self>;
    }

    struct TestMethodDispatch;
    impl MethodDispatch<TestActor> for TestMethodDispatch {
        fn dispatch(&self, request: Request<Body>) -> GrpcFuture<Response<ResponsePayload>> {
            let status = Status::new(StatusCode::Ok, Some("Ok"));
            let header_value = status.to_header_value().unwrap();
            let mut headers = HeaderMap::new();
            headers.append("grpc-status", header_value);
            let payload = ResponsePayload::new(
                Box::new(stream::once(Ok(TestProstMessage {
                    test: "test".to_string()
                })))
                    as Box<Stream<Item = TestProstMessage, Error = Status> + Send>,
                Box::new(future::ok(Metadata::from_header_map(&headers)))
                    as Box<Future<Item = Metadata, Error = Status> + Send>,
            );
            Box::new(future::ok(Response::new(payload)))
        }
    }
    #[test]
    fn test_not_found() {
        let mut service: GrpcHyperService<TestActor> = GrpcHyperService::new();
        let mut request = Request::builder();
        request.uri("https://test.example.com");
        let result = service.call(request.body(Body::empty()).unwrap()).wait();
        match result {
            Ok(response) => {
                let (parts, body) = response.into_parts();
                let trailers = body.trailers.wait().unwrap().unwrap();
                let status =
                    Status::new(StatusCode::NotFound, Some("That method could not be found"));
                let header_value = status.to_header_value().unwrap();
                assert_eq!(trailers.get("grpc-status"), Some(&header_value));
            }
            _ => assert!(false),
        }
    }
    #[test]
    fn test_dispatch() {
        let mut service: GrpcHyperService<TestActor> = GrpcHyperService::new();
        service.add_dispatch("/test".to_string(), Box::new(TestMethodDispatch {}));
        let mut request = Request::builder();
        request.uri("https://test.example.com/test");
        let result = service.call(request.body(Body::empty()).unwrap()).wait();
        match result {
            Ok(response) => {
                let (parts, body) = response.into_parts();
                let data = body.data.wait().next().unwrap().unwrap();
                let message: TestProstMessage = frame::decode(data).unwrap();
                let trailers = body.trailers.wait().unwrap().unwrap();
                let status = Status::new(StatusCode::Ok, Some("Ok"));
                let header_value = status.to_header_value().unwrap();
                assert_eq!(trailers.get("grpc-status"), Some(&header_value));
                assert_eq!(message.test, "test".to_string())
            }
            _ => assert!(false),
        }
    }
}
