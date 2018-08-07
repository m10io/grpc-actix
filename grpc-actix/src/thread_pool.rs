use actix::msgs::StartActor;
use actix::{Actor, Addr, Arbiter, Context, Handler, Message, Response};
use futures::{future, Future};

pub struct Pool {
    thread_arbiters: Vec<ThreadArbiter>,
    threads: usize,
    current_thread: usize,
}

impl Pool {
    pub fn new(num: usize) -> Pool {
        Pool {
            thread_arbiters: vec![],
            threads: num,
            current_thread: 0,
        }
    }
}

impl Actor for Pool {
    type Context = Context<Self>;
}

pub struct Start;
impl Message for Start {
    type Result = Result<(), ()>;
}

impl Handler<Start> for Pool {
    type Result = Response<(), ()>;
    fn handle(&mut self, _msg: Start, _ctx: &mut Context<Self>) -> Response<(), ()> {
        Response::reply(
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
            }).map_err(|_| ())
            .wait(),
        )
    }
}

pub struct NextThread;
impl Message for NextThread {
    type Result = Result<ThreadArbiter, ()>;
}

impl Handler<NextThread> for Pool {
    type Result = Response<ThreadArbiter, ()>;
    fn handle(&mut self, _msg: NextThread, _ctx: &mut Context<Self>) -> Response<ThreadArbiter, ()> {
        let thread_arbiter = self.thread_arbiters[self.current_thread].clone();
        self.current_thread = (self.current_thread + 1) % self.threads;
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

impl <F: Future + 'static + Send> Message for SpawnFuture<F> {
    type Result = Result<F::Item, F::Error>;
}

impl <F: Future + Send + 'static> Handler<SpawnFuture<F>> for RuntimeActor {
    type Result = Response<F::Item, F::Error>;
    fn handle(&mut self, msg: SpawnFuture<F>, _ctx: &mut Context<Self>) -> Self::Result {
        Response::async(msg.0)
    }
}
