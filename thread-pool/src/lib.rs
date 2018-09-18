extern crate actix;
extern crate futures;
extern crate tokio;

use actix::{Addr, Arbiter, Context, Handler, Message, Response};
use futures::future::{ExecuteError, ExecuteErrorKind, Executor};
use futures::Future;
use std::sync::{Arc, Mutex};

/// A basic circular vector data structure
pub struct CircularVector<T> {
    data: Vec<T>,
    current_index: usize,
}

impl<T> CircularVector<T> {
    /// Creates a new CircularVector using the vector passed in
    pub fn new(data: Vec<T>) -> CircularVector<T> {
        CircularVector {
            data,
            current_index: 0,
        }
    }
    pub fn push(&mut self, obj: T) {
        self.data.push(obj);
    }
}

impl<T: Send + Clone> Pool<T> for CircularVector<T> {
    /// Pulls the next element from the array. Be warned it returns a copy of whatever that element is.
    fn next(&mut self) -> Option<T> {
        let index = self.current_index;
        self.current_index = (self.current_index + 1) % self.data.len();
        self.data.get(index).cloned()
    }
}

pub trait Pool<T>: Send {
    /// Pulls the next element from the pool
    fn next(&mut self) -> Option<T>;
}

/// An Executor that uses Arbiters to run its proceses
pub struct ArbiterExecutor<P: Pool<Addr<Arbiter>>>
where
    P: Send,
{
    internal_pool: Arc<Mutex<P>>,
}

impl<P: Pool<Addr<Arbiter>>> Clone for ArbiterExecutor<P> {
    fn clone(&self) -> ArbiterExecutor<P> {
        ArbiterExecutor {
            internal_pool: Arc::clone(&self.internal_pool),
        }
    }
}

impl<P: Pool<Addr<Arbiter>>> ArbiterExecutor<P> {
    /// Creates a new ArbiterExecutor using an existing Pool
    pub fn new(pool: Arc<Mutex<P>>) -> ArbiterExecutor<P> {
        ArbiterExecutor {
            internal_pool: pool,
        }
    }
}
impl<P: Pool<Addr<Arbiter>>> Pool<Addr<Arbiter>> for ArbiterExecutor<P> {
    fn next(&mut self) -> Option<Addr<Arbiter>> {
        self.internal_pool.lock().ok()?.next()
    }
}

impl<P: Pool<Addr<Arbiter>>, F: 'static + Future<Item = (), Error = ()> + Send> Executor<F>
    for ArbiterExecutor<P>
{
    fn execute(&self, future: F) -> Result<(), ExecuteError<F>> {
        match self
            .internal_pool
            .lock()
            .map_err(|_| ())
            .and_then(|mut pool| (&mut pool).next().ok_or(()))
        {
            Ok(arbiter) => arbiter.try_send(ExecutorFuture(future)).map_err(|err| {
                eprintln!("Failed to send execute");
                ExecuteError::new(ExecuteErrorKind::Shutdown, err.into_inner().0)
            }),
            Err(_) => Err(ExecuteError::new(ExecuteErrorKind::Shutdown, future)),
        }
    }
}

struct ExecutorFuture<F: Future<Item = (), Error = ()>>(F);

impl<F: Future<Item = (), Error = ()>> Message for ExecutorFuture<F> {
    type Result = Result<(), ()>;
}

impl<F: 'static + Future<Item = (), Error = ()>> Handler<ExecutorFuture<F>> for Arbiter {
    type Result = Response<(), ()>;
    fn handle(&mut self, msg: ExecutorFuture<F>, _: &mut Context<Self>) -> Self::Result {
        Response::async(msg.0)
    }
}

#[cfg(test)]
mod tests {
    use actix::{Arbiter, System};
    use futures::future::Executor;
    use futures::{future, Future};
    use std::sync::{Arc, Mutex};
    use {ArbiterExecutor, CircularVector, Pool};

    #[test]
    fn test_circular_vector() {
        let mut circle = CircularVector::new(vec![1, 2, 3]);
        assert_eq!(circle.next(), Some(1));
        assert_eq!(circle.next(), Some(2));
        assert_eq!(circle.next(), Some(3));
    }

    #[test]

    fn test_executor_circular() {
        System::run(|| {
            let pool = Arc::new(Mutex::new(CircularVector::new(vec![
                Arbiter::new("1"),
                Arbiter::new("2"),
                Arbiter::new("3"),
            ])));
            let executor = ArbiterExecutor::new(pool);
            for i in 1..3 {
                assert!(
                    executor
                        .execute(future::ok(()).map(move |_| {
                            let name = Arbiter::name();
                            let parts: Vec<&str> = name.split(':').collect();
                            assert_eq!(parts[2], format!("{}", i));
                        })).is_ok()
                );
            }
            System::current().stop();
        });
    }
}
