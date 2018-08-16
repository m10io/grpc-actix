use actix::msgs::StartActor;
use actix::{Actor, Addr, Arbiter, Context};
use future::GrpcFuture;
use futures::{future, Future};
use hyper::server::conn::{Http, SpawnAll};
use hyper::service::Service;
use hyper::{self, Body, Request, Response};
use response;
use response::ResponsePayload;
use status::{Status, StatusCode};
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use thread_pool::{ArbiterExecutor, CircularVector, Pool};

/// A gRPC server based on Hyper
///
/// ## Example
///
/// ```
/// let mut sys = System::new("test");
/// let mut server = Server::spawn(|| BeanServiceServer {}, 3).wait().unwrap();
/// sys.block_on(
/// server.bind(
///   "127.0.0.1:50051".parse().unwrap(),
///    |mut service| simple::BeanServiceDispatch::add_to_service(&mut service),
/// ).map_err(|err| { eprintln!("{}", err); }));
/// sys.run();
/// ```
pub struct Server<A: Actor> {
    pub workers: Arc<Mutex<ArbiterActorPool<A>>>,
    arbiter_executor: ArbiterExecutor<ArbiterActorPool<A>>,
}

impl<A: Actor<Context = Context<A>> + Send> Server<A> {
    /// Creates a new server with specified number of threads and the actor generator
    ///
    /// actor_generator returns an instanceo of the Actor that will handle the requests
    pub fn spawn<F: Fn() -> A + Send>(
        actor_generator: F,
        threads: usize,
    ) -> impl Future<Item = Server<A>, Error = ServerStartError> {
        let workers = (0..threads).map(move |i| {
            let arbiter = Arbiter::new(format!("arbiter_{}", i));
            let actor = (actor_generator)();
            arbiter
                .send(StartActor::new(|_| actor))
                .map_err(|_| ServerStartError::FailedToStartWorkers)
                .map(|addr| (arbiter, addr))
        });
        future::join_all(workers).and_then(move |addrs| {
            let workers = Arc::new(Mutex::new(ArbiterActorPool(CircularVector::new(addrs))));
            let arbiter_executor = ArbiterExecutor::new(Arc::clone(&workers));
            future::ok(Server {
                workers: Arc::clone(&workers),
                arbiter_executor,
            })
        })
    }
    /// Binds the server to the specified address, and uses the callback to bind the Server to Service
    pub fn bind<F: 'static + Fn(&mut GrpcHyperService<A>) -> () + Send>(
        self,
        addr: SocketAddr,
        dispatch_adder: F,
    ) -> Box<Future<Item = (), Error = hyper::Error> + Send + 'static> {
        Box::new(
            future::result(
                Http::new()
                    .executor(self.arbiter_executor.clone())
                    .http2_only(true)
                    .serve_addr(&addr, move || {
                        Box::new(future::result(
                            self.service()
                                .map(|mut service| {
                                    dispatch_adder(&mut service);
                                    service
                                }).ok_or_else(|| {
                                    io::Error::new(
                                        io::ErrorKind::Other,
                                        "Failed to find available worker",
                                    )
                                }),
                        ))
                    }),
            ).and_then(|serve| SpawnAll { serve }),
        )
    }
    fn service(&self) -> Option<GrpcHyperService<A>> {
        let worker: Option<(Addr<Arbiter>, Addr<A>)> = {
            let mut workers = Arc::clone(&self.workers);
            let mut workers = workers.lock().unwrap();
            workers.0.next()
        };
        worker.map(|worker| GrpcHyperService {
            addr: worker.1,
            dispatchers: HashMap::new(),
        })
    }
}

pub struct ArbiterActorPool<A: Actor>(CircularVector<(Addr<Arbiter>, Addr<A>)>);
impl<A: Actor> Pool<Addr<Arbiter>> for ArbiterActorPool<A> {
    fn next(&mut self) -> Option<Addr<Arbiter>> {
        self.0.next().map(|t| t.0)
    }
}
#[derive(Debug)]
pub enum ServerStartError {
    FailedToStartWorkers,
}

pub struct GrpcHyperService<A: Actor> {
    pub addr: Addr<A>,
    pub dispatchers: HashMap<String, Box<dyn MethodDispatch<A> + Send>>,
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
    use server::{MethodDispatch, Server};
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
        System::run(move || {
            let server = Server::spawn(|| TestActor {}, 3).wait().unwrap();
            let mut service = server.service().unwrap();
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
        System::run(move || {
            let server = Server::spawn(|| TestActor {}, 3).wait().unwrap();
            let mut service = server.service().unwrap();
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
