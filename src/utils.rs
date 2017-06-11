use std::time::{Instant, Duration};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use futures::{Async, Future};
use futures::executor;
use futures::task::Unpark;

pub type WaitResult<I, E> = Result<I, WaitTimeoutErr<E>>;

#[derive(Debug)]
pub enum WaitTimeoutErr<E> {
    FutureErr(E),
    TimedOut,
}

pub fn wait_timeout<F>(f: F, dur: Duration) -> WaitResult<F::Item, F::Error>
    where F: Future
{
    let now = Instant::now();
    let mut task = executor::spawn(f);
    let thread = Arc::new(ThreadUnpark::new(thread::current()));

    loop {
        let cur = Instant::now();
        if cur >= now + dur {
            return Err(WaitTimeoutErr::TimedOut);
        }
        match task.poll_future(thread.clone()) {
            Ok(Async::Ready(e)) => return Ok(e),
            Ok(Async::NotReady) => {}
            Err(e) => return Err(WaitTimeoutErr::FutureErr(e)),
        }

        thread.park(now + dur - cur);
    }

    struct ThreadUnpark {
        thread: thread::Thread,
        ready: AtomicBool,
    }

    impl ThreadUnpark {
        fn new(thread: thread::Thread) -> ThreadUnpark {
            ThreadUnpark {
                thread: thread,
                ready: AtomicBool::new(false),
            }
        }

        fn park(&self, dur: Duration) {
            if !self.ready.swap(false, Ordering::SeqCst) {
                thread::park_timeout(dur);
            }
        }
    }

    impl Unpark for ThreadUnpark {
        fn unpark(&self) {
            self.ready.store(true, Ordering::SeqCst);
            self.thread.unpark()
        }
    }
}
