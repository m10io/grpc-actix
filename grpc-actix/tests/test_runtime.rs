extern crate actix;
extern crate futures;
extern crate grpc_actix;
extern crate tokio;

use actix::{Actor, System};
use futures::{future, Future};
use grpc_actix::thread_pool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::timer::Delay;

#[test]
fn test_start_thread_pool() {
    let result = Arc::new(AtomicBool::new(true));
    let result1 = Arc::clone(&result);
    System::run(move || {
        let addr = thread_pool::Pool::new(2).start();
        tokio::spawn(addr.send(thread_pool::Start).then(move |result| {
            result1.store(result.is_ok(), Ordering::Relaxed);
            System::current().stop();
            Box::new(future::ok(())) as Box<Future<Item = (), Error = ()> + Send>
        }));
    });
    assert!(result.load(Ordering::Relaxed));
}

#[test]
fn test_spawn_future() {
    let result = Arc::new(AtomicBool::new(true));
    let result1 = Arc::clone(&result);
    System::run(move || {
        let addr = thread_pool::Pool::new(2).start();
        tokio::spawn(
            addr.send(thread_pool::Start)
                .and_then(move |_| addr.send(thread_pool::NextThread))
                .map_err(|_| ())
                .and_then(|thread| future::result(thread))
                .and_then(|thread| {
                    thread
                        .actor_address
                        .send(thread_pool::SpawnFuture(Box::new(future::ok("Test"))
                            as Box<Future<Item = &str, Error = ()> + Send>)).map_err(|_| ())
                }).then(move |result| {
                    match result {
                        Ok(s) => result1.store(s == Ok("Test"), Ordering::Relaxed),
                        _ => result1.store(false, Ordering::Relaxed),
                    };
                    System::current().stop();
                    Box::new(future::ok(())) as Box<Future<Item = (), Error = ()> + Send>
                }),
        );
    });
    assert!(result.load(Ordering::Relaxed));
}

#[test]
fn test_spawn_futures() {
    /*let result = Arc::new(AtomicBool::new(true));
    let result1 = Arc::clone(&result);
    System::run(move || {
        let addr = thread_pool::Pool::new(2).start();
        tokio::spawn(
            addr.send(thread_pool::Start)
                .and_then(move |_| addr.send(thread_pool::NextThread))
                .map_err(|_| ())
                .and_then(|thread| future::result(thread))
                .and_then(|thread| {
                    thread
                        .actor_address
                        .send(
                            thread_pool::SpawnFuture(Delay::new(Instant::now() + Duration::from_millis(500)))
                            thread_pool::SpawnFuture(Box::new(future::ok("Test"))
                            as Box<Future<Item = &str, Error = ()> + Send>)).map_err(|_| ())
                }).then(move |result| {
                    match result {
                        Ok(s) => result1.store(s == Ok("Test"), Ordering::Relaxed),
                        _ => result1.store(false, Ordering::Relaxed),
                    };
                    System::current().stop();
                    Box::new(future::ok(())) as Box<Future<Item = (), Error = ()> + Send>
                }),
        );
    });
    assert!(result.load(Ordering::Relaxed));*/
}
