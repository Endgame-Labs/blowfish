//! Poll query plans on CPU workers; yielded plans retain their state and use async I/O.
use bf_types::ApiError;
use std::{
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::OnceLock,
    task::{Context, Poll},
};
use tokio::sync::oneshot;

enum Work<F: Future> {
    Ready(F::Output),
    Yielded(Pin<Box<F>>),
    Panicked,
}

/// A resident plan completes on its CPU worker without entering `block_on`. A yielded plan
/// keeps its partially polled future, including I/O and locks, rather than executing twice.
/// Its blocking-pool continuation cannot exhaust the fixed CPU workers while waiting for I/O.
pub(crate) async fn run<F>(future: F) -> Result<F::Output, ApiError>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    static WORKERS: OnceLock<rayon::ThreadPool> = OnceLock::new();
    let pool = WORKERS.get_or_init(|| {
        rayon::ThreadPoolBuilder::new()
            .num_threads(std::thread::available_parallelism().map_or(1, usize::from))
            .thread_name(|i| format!("bf-query-{i}"))
            .build()
            .expect("query worker pool")
    });
    let runtime = tokio::runtime::Handle::current();
    let worker_runtime = runtime.clone();
    let (send, receive) = oneshot::channel();
    pool.spawn_fifo(move || {
        if send.is_closed() {
            return;
        }
        let work = catch_unwind(AssertUnwindSafe(|| {
            let _runtime = worker_runtime.enter();
            let mut future = Box::pin(future);
            let mut context = Context::from_waker(futures::task::noop_waker_ref());
            match future.as_mut().poll(&mut context) {
                Poll::Ready(value) => Work::Ready(value),
                Poll::Pending => Work::Yielded(future),
            }
        }))
        .unwrap_or(Work::Panicked);
        let _ = send.send(work);
    });
    match receive
        .await
        .map_err(|_| ApiError::new(500, "query worker stopped"))?
    {
        Work::Ready(value) => {
            metrics::counter!("blowfish_query_cpu_dispatch_total", "path" => "ready").increment(1);
            Ok(value)
        }
        Work::Yielded(future) => {
            metrics::counter!("blowfish_query_cpu_dispatch_total", "path" => "yielded")
                .increment(1);
            run_io(runtime, future).await
        }
        Work::Panicked => Err(ApiError::new(500, "query worker panicked")),
    }
}

/// Cold snapshots retain the original dispatch; yielded warm plans continue from their
/// saved suspension point with the same blocking-pool I/O capacity.
pub(crate) async fn run_io<F>(
    runtime: tokio::runtime::Handle,
    future: F,
) -> Result<F::Output, ApiError>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::task::spawn_blocking(move || runtime.block_on(future))
        .await
        .map_err(crate::segment::index_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    tokio::task_local! { static VALUE: usize; }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ready_work_uses_a_cpu_worker_and_keeps_its_task_local_context() {
        let result = run(VALUE.scope(17, async {
            assert!(
                std::thread::current()
                    .name()
                    .unwrap()
                    .starts_with("bf-query-")
            );
            VALUE.get()
        }))
        .await
        .unwrap();
        assert_eq!(result, 17);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_yielded_future_is_resumed_without_restarting_or_losing_context() {
        let polls = Arc::new(AtomicUsize::new(0));
        let seen = polls.clone();
        let result = run(VALUE.scope(
            23,
            std::future::poll_fn(move |_| {
                assert_eq!(VALUE.get(), 23);
                if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                    Poll::Pending
                } else {
                    Poll::Ready(42)
                }
            }),
        ))
        .await
        .unwrap();
        assert_eq!(result, 42);
        assert_eq!(polls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pending_io_does_not_occupy_cpu_workers() {
        let count = std::thread::available_parallelism().map_or(1, usize::from) + 2;
        let started = Arc::new(tokio::sync::Barrier::new(count + 1));
        let finish = Arc::new(tokio::sync::Semaphore::new(0));
        let mut tasks = Vec::new();
        for _ in 0..count {
            let (started, finish) = (started.clone(), finish.clone());
            tasks.push(tokio::spawn(run(async move {
                started.wait().await;
                finish.acquire().await.unwrap().forget();
            })));
        }
        tokio::time::timeout(std::time::Duration::from_secs(10), started.wait())
            .await
            .unwrap();
        assert_eq!(run(async { 7 }).await.unwrap(), 7);
        for task in &tasks {
            assert!(!task.is_finished());
        }
        finish.add_permits(count);
        for task in tasks {
            task.await.unwrap().unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_panicking_plan_does_not_kill_the_pool() {
        assert_eq!(
            run(async { panic!("test query panic") })
                .await
                .unwrap_err()
                .status,
            500
        );
        assert_eq!(run(async { 7 }).await.unwrap(), 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timers_created_on_cpu_workers_resume_on_the_io_path() {
        assert_eq!(
            run(async {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                7
            })
            .await
            .unwrap(),
            7
        );
        assert_eq!(
            run_io(
                tokio::runtime::Handle::current(),
                VALUE.scope(19, async {
                    assert!(
                        !std::thread::current()
                            .name()
                            .unwrap()
                            .starts_with("bf-query-")
                    );
                    VALUE.get()
                })
            )
            .await
            .unwrap(),
            19
        );
    }
}
