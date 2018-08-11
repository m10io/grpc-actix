extern crate actix;
extern crate futures;
extern crate tokio;

use actix::msgs::StartActor;
use actix::{Actor, Addr, Arbiter, Context, Handler, Message, Response};
use futures::{future, Future};

pub struct Pool {
    thread_arbiters: Vec<ThreadArbiter>,
    threads: usize,
}

impl Pool {
    pub fn new(num: usize) -> Pool {
        Pool {
            thread_arbiters: vec![],
            threads: num,
        }
    }
    pub fn start<'pool>(&'pool mut self) -> Box<Future<Item = (), Error = ()> + Send + 'pool> {
        Box::new(
            future::join_all((0..self.threads).map(|i| {
                let arbiter = Arbiter::new(format!("arbiter_{}", i));
                arbiter
                    .send(StartActor::new(|_| RuntimeActor {}))
                    .map(|addr| ThreadArbiter {
                        arbiter,
                        actor_address: addr,
                    })
            })).map(move |thread_arbiters| {
                self.thread_arbiters = thread_arbiters;
            }).map_err(|_| ()),
        )
    }
}

pub struct RoundRobinScheduler {
    pool: Pool,
    current_thread: usize,
}

impl RoundRobinScheduler {
    pub fn new(threads: usize) -> RoundRobinScheduler {
        RoundRobinScheduler {
            pool: Pool::new(threads),
            current_thread: 0,
        }
    }
}

impl Actor for RoundRobinScheduler {
    type Context = Context<Self>;
}

pub struct Start;
impl Message for Start {
    type Result = Result<(), ()>;
}

impl Handler<Start> for RoundRobinScheduler {
    type Result = Response<(), ()>;
    fn handle(&mut self, _msg: Start, _ctx: &mut Context<Self>) -> Response<(), ()> {
        Response::reply(self.pool.start().wait())
    }
}

pub struct NextThread;
impl Message for NextThread {
    type Result = Result<ThreadArbiter, ()>;
}

impl Handler<NextThread> for RoundRobinScheduler {
    type Result = Response<ThreadArbiter, ()>;
    fn handle(
        &mut self,
        _msg: NextThread,
        _ctx: &mut Context<Self>,
    ) -> Response<ThreadArbiter, ()> {
        let thread_arbiter = self.pool.thread_arbiters[self.current_thread].clone();
        self.current_thread = (self.current_thread + 1) % self.pool.threads;
        Response::reply(Ok(thread_arbiter))
    }
}

#[derive(Clone)]
pub struct ThreadArbiter {
    arbiter: Addr<Arbiter>,
    pub actor_address: Addr<RuntimeActor>,
}

pub struct RuntimeActor;
impl Actor for RuntimeActor {
    type Context = Context<Self>;
}

pub struct SpawnFuture<F: Future + Send>(pub F);

impl<F: Future + 'static + Send> Message for SpawnFuture<F> {
    type Result = Result<F::Item, F::Error>;
}

impl<F: Future + Send + 'static> Handler<SpawnFuture<F>> for RuntimeActor {
    type Result = Response<F::Item, F::Error>;
    fn handle(&mut self, msg: SpawnFuture<F>, _ctx: &mut Context<Self>) -> Self::Result {
        Response::async(msg.0)
    }
}
