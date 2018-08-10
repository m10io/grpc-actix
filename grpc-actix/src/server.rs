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
