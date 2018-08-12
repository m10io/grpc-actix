use actix::msgs::StartActor;
use actix::{Actor, Addr, Context};
use future::GrpcFuture;
use futures::Future;
use hyper::service::Service;
use hyper::{Body, Request, Response};
use response;
use response::ResponsePayload;
use status::{Status, StatusCode};
use std::collections::HashMap;
use thread_pool;

pub struct GrpcHyperService<A: Actor> {
    pub addr: Addr<A>,
    pub dispatchers: HashMap<String, Box<dyn MethodDispatch<A> + Send>>,
}

pub struct ServiceGenerator<A: Actor> {
    addrs: Vec<Addr<A>>,
    thread_pool: thread_pool::Pool,
    current_addr: usize,
}

impl<A: Actor<Context = Context<A>> + Send> ServiceGenerator<A> {
    pub fn new(threads: usize) -> ServiceGenerator<A> {
        ServiceGenerator {
            addrs: vec![],
            current_addr: 0,
            thread_pool: thread_pool::Pool::new(threads),
        }
    }
    pub fn start(&mut self, actor_generator: Box<Fn() -> A + Send>) {
        let _ = self.thread_pool.start().wait();
        let arbiters = self.thread_pool.thread_arbiters.clone();
        for thread_arbiter in arbiters {
            let actor = (actor_generator)();
            match thread_arbiter
                .arbiter
                .send(StartActor::new(|_| actor))
                .wait()
            {
                Ok(addr) => self.addrs.push(addr),
                Err(_) => {}
            }
        }
    }
    pub fn service(&mut self) -> Option<GrpcHyperService<A>> {
        self.addrs
            .get(self.current_addr)
            .map(|addr| GrpcHyperService {
                addr: addr.clone(),
                dispatchers: HashMap::new(),
            })
    }
}

impl<A: Actor> GrpcHyperService<A> {
    pub fn add_dispatch(&mut self, path: String, dispatch: Box<MethodDispatch<A> + Send>) {
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
            Some(dispatcher) => {
                response::flatten_response(dispatcher.dispatch(self.addr.clone(), req))
                    as GrpcFuture<Response<ResponsePayload>>
            }
            None => response::error_response(
                Status::new(StatusCode::NotFound, Some("That method could not be found")),
                None,
            ),
        }
    }
}

pub trait MethodDispatch<A: Actor> {
    fn dispatch(
        &self,
        actor: Addr<A>,
        request: Request<Body>,
    ) -> GrpcFuture<Response<ResponsePayload>>;
}
#[cfg(test)]
mod tests {
    use actix::{Actor, Addr, Context, System};
    use frame;
    use future::GrpcFuture;
    use futures::{future, stream, Future, Stream};
    use hyper::service::Service;
    use hyper::{Body, HeaderMap, Request, Response};
    use metadata::Metadata;
    use response::ResponsePayload;
    use server::{MethodDispatch, ServiceGenerator};
    use status::{Status, StatusCode};

    #[derive(Clone, PartialEq, Message)]
    pub struct TestProstMessage {
        #[prost(string, tag = "1")]
        test: String,
    }
    #[derive(Clone)]
    struct TestActor;
    impl Actor for TestActor {
        type Context = Context<Self>;
    }

    struct TestMethodDispatch;
    impl MethodDispatch<TestActor> for TestMethodDispatch {
        fn dispatch(
            &self,
            _actor: Addr<TestActor>,
            _request: Request<Body>,
        ) -> GrpcFuture<Response<ResponsePayload>> {
            let status = Status::new(StatusCode::Ok, Some("Ok"));
            let header_value = status.to_header_value().unwrap();
            let mut headers = HeaderMap::new();
            headers.append("grpc-status", header_value);
            let payload = ResponsePayload::new(
                Box::new(stream::once(Ok(TestProstMessage {
                    test: "test".to_string(),
                }))) as Box<Stream<Item = TestProstMessage, Error = Status> + Send>,
                Box::new(future::ok(Metadata::from_header_map(&headers)))
                    as Box<Future<Item = Metadata, Error = Status> + Send>,
            );
            Box::new(future::ok(Response::new(payload)))
        }
    }
    #[test]
    fn test_not_found() {
        //let addr = TestActor.start();
        let mut generator: ServiceGenerator<TestActor> = ServiceGenerator::new(2);
        System::run(move || {
            generator.start(Box::new(|| TestActor {}));
            let mut service = generator.service().unwrap();
            let mut request = Request::builder();
            request.uri("https://test.example.com");
            let result = service.call(request.body(Body::empty()).unwrap()).wait();
            match result {
                Ok(response) => {
                    let (_, body) = response.into_parts();
                    let trailers = body.trailers.wait().unwrap().unwrap();
                    let status =
                        Status::new(StatusCode::NotFound, Some("That method could not be found"));
                    let header_value = status.to_header_value().unwrap();
                    assert_eq!(trailers.get("grpc-status"), Some(&header_value));
                }
                _ => assert!(false),
            }
            System::current().stop()
        });
    }
    #[test]
    fn test_dispatch() {
        let mut generator: ServiceGenerator<TestActor> = ServiceGenerator::new(2);
        System::run(move || {
            generator.start(Box::new(|| TestActor {}));
            let mut service = generator.service().unwrap();
            service.add_dispatch("/test".to_string(), Box::new(TestMethodDispatch {}));
            let mut request = Request::builder();
            request.uri("https://test.example.com/test");
            let result = service.call(request.body(Body::empty()).unwrap()).wait();
            match result {
                Ok(response) => {
                    let (_, body) = response.into_parts();
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
            System::current().stop()
        });
    }
}
