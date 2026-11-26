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

thread_local! {
    static IO_RUNTIME: std::cell::RefCell<Option<tokio::runtime::Handle>> = const { std::cell::RefCell::new(None) };
}

/// Run an independent single-thread HTTP reactor, retaining a shared runtime for I/O
/// continuations. The future must occupy this thread for its entire lifetime.
pub async fn on_query_reactor<F: Future>(runtime: tokio::runtime::Handle, future: F) -> F::Output {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            IO_RUNTIME.with(|slot| *slot.borrow_mut() = None);
        }
    }
    assert_eq!(
        tokio::runtime::Handle::current().runtime_flavor(),
        tokio::runtime::RuntimeFlavor::CurrentThread
    );
    IO_RUNTIME.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some(runtime);
    });
    let _reset = Reset;
    future.await
}

/// Background work belongs to the shared runtime, including work created by a first query.
pub(crate) fn shared_runtime() -> tokio::runtime::Handle {
    IO_RUNTIME
        .with(|slot| slot.borrow().clone())
        .unwrap_or_else(tokio::runtime::Handle::current)
}

pub(crate) fn is_reactor() -> bool {
    IO_RUNTIME.with(|slot| slot.borrow().is_some())
}

enum Work<F: Future> {
    Ready(F::Output),
    Yielded(Pin<Box<F>>),
    Panicked,
}

/// Shared CPU pool for query execution and fan-out after an I/O continuation.
pub(crate) fn workers() -> &'static rayon::ThreadPool {
    static WORKERS: OnceLock<rayon::ThreadPool> = OnceLock::new();
    WORKERS.get_or_init(|| {
        let cpus = std::thread::available_parallelism().map_or(1, usize::from);
        // Leave capacity for WAL completions and response delivery under sustained CPU
        // load. Small hosts retain every compute worker; their spare-core cost is larger.
        let workers = if cpus >= 8 { cpus - 1 } else { cpus };
        rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .thread_name(|i| format!("bf-query-{i}"))
            .build()
            .expect("query worker pool")
    })
}

/// A resident plan completes on its CPU worker without entering `block_on`. A yielded plan
/// keeps its partially polled future, including I/O and locks, rather than executing twice.
/// Its blocking-pool continuation cannot exhaust the fixed CPU workers while waiting for I/O.
pub(crate) async fn run<F>(future: F, inline: bool) -> Result<F::Output, ApiError>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    if inline && let Some(runtime) = IO_RUNTIME.with(|slot| slot.borrow().clone()) {
        let mut future = Box::pin(future);
        let work = catch_unwind(AssertUnwindSafe(|| {
            let _inline = bf_text::InlineGuard::enter();
            let mut context = Context::from_waker(futures::task::noop_waker_ref());
            future.as_mut().poll(&mut context)
        }));
        return match work {
            Ok(Poll::Ready(value)) => {
                metrics::counter!("blowfish_query_cpu_dispatch_total", "path" => "inline")
                    .increment(1);
                Ok(value)
            }
            Ok(Poll::Pending) => {
                metrics::counter!("blowfish_query_cpu_dispatch_total", "path" => "inline_yielded")
                    .increment(1);
                run_io(runtime, future).await
            }
            Err(_) => Err(ApiError::new(500, "query worker panicked")),
        };
    }
    let pool = workers();
    let runtime = shared_runtime();
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
/// saved suspension point with the same blocking-pool I/O capacity. Dropping the caller
/// aborts the plan at its next await point, for every caller.
pub(crate) async fn run_io<F>(
    runtime: tokio::runtime::Handle,
    future: F,
) -> Result<F::Output, ApiError>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let runtime = IO_RUNTIME
        .with(|slot| slot.borrow().clone())
        .unwrap_or(runtime);
    let worker_runtime = runtime.clone();
    // Dropping a blocking JoinHandle detaches its task. Abort the async plan when the
    // caller stops awaiting, so it releases its permits and pending I/O at its next await.
    struct Cancel(futures::future::AbortHandle);
    impl Drop for Cancel {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let (abort, registration) = futures::future::AbortHandle::new_pair();
    let _cancel = Cancel(abort);
    let future = futures::future::Abortable::new(future, registration);
    runtime
        .spawn_blocking(move || worker_runtime.block_on(future))
        .await
        .map_err(crate::segment::index_error)?
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
    async fn run<F>(future: F) -> Result<F::Output, ApiError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        super::run(future, true).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn independent_reactor_polls_inline_and_preserves_a_yielded_plan() {
        let io = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            tokio::runtime::Builder::new_current_thread()
                .thread_name("reactor-blocking")
                .enable_all()
                .build()
                .unwrap()
                .block_on(on_query_reactor(io, async {
                    let thread = std::thread::current().id();
                    assert_eq!(
                        shared_runtime()
                            .spawn(async { tokio::runtime::Handle::current().runtime_flavor() })
                            .await
                            .unwrap(),
                        tokio::runtime::RuntimeFlavor::MultiThread
                    );
                    assert!(
                        super::run(
                            async {
                                assert_eq!(
                                    tokio::runtime::Handle::current().runtime_flavor(),
                                    tokio::runtime::RuntimeFlavor::MultiThread
                                );
                                std::thread::current()
                                    .name()
                                    .unwrap()
                                    .starts_with("bf-query-")
                            },
                            false
                        )
                        .await
                        .unwrap()
                    );
                    assert_eq!(
                        run(async move { std::thread::current().id() })
                            .await
                            .unwrap(),
                        thread
                    );
                    let polls = Arc::new(AtomicUsize::new(0));
                    let seen = polls.clone();
                    let result = run(VALUE.scope(
                        29,
                        std::future::poll_fn(move |_| {
                            assert_eq!(VALUE.get(), 29);
                            if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                                assert_eq!(std::thread::current().id(), thread);
                                Poll::Pending
                            } else {
                                assert_ne!(std::thread::current().id(), thread);
                                assert_ne!(std::thread::current().name(), Some("reactor-blocking"));
                                Poll::Ready(42)
                            }
                        }),
                    ))
                    .await
                    .unwrap();
                    assert_eq!(result, 42);
                    assert_eq!(polls.load(Ordering::SeqCst), 2);
                    assert_eq!(
                        run(async { panic!("reactor panic") })
                            .await
                            .unwrap_err()
                            .status,
                        500
                    );
                    assert_eq!(run(async { 7 }).await.unwrap(), 7);
                    assert_eq!(
                        run(async {
                            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                            8
                        })
                        .await
                        .unwrap(),
                        8
                    );
                }));
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cold_reactor_refresh_keeps_backend_accounting_and_a_writable_handle() {
        use crate::{Engine, EngineConfig};
        use bf_storage::{Storage, StorageConfig};
        use serde_json::json;
        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::open(&StorageConfig {
            url: format!("file://{}", directory.path().display()),
            ..Default::default()
        })
        .await
        .unwrap();
        let config = EngineConfig {
            indexing: false,
            recall_sample_rate: 0.,
            ..Default::default()
        };
        let writer = Engine::with_config(storage.clone(), config.clone());
        writer
            .write(
                "reactor",
                serde_json::from_value(json!({
                    "distance_metric":"cosine_distance", "upsert_rows":[{"id":1,"vector":[1.,0.]}]
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        let reader = Engine::with_config(storage, config);
        let io = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(on_query_reactor(io, async move {
                    let (response, diagnostics) = reader
                        .query_with_diagnostics(
                            "reactor",
                            serde_json::from_value(json!({
                                "rank_by":["vector","ANN",[1.,0.]], "top_k":1
                            }))
                            .unwrap(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(response.result.rows.as_ref().unwrap()[0]["id"], json!(1));
                    assert!(
                        diagnostics
                            .backend
                            .iter()
                            .any(|measurement| measurement.stage == "snapshot"
                                && measurement.requests > 0)
                    );
                    reader
                        .write(
                            "reactor",
                            serde_json::from_value(json!({
                                "upsert_rows":[{"id":2,"vector":[0.,1.]}]
                            }))
                            .unwrap(),
                        )
                        .await
                        .unwrap();
                    let response = reader
                        .query(
                            "reactor",
                            serde_json::from_value(json!({
                                "rank_by":["id","asc"], "top_k":10
                            }))
                            .unwrap(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(response.result.rows.as_ref().unwrap().len(), 2);
                    assert!(reader.shutdown(std::time::Duration::from_secs(5)).await);
                }));
        })
        .await
        .unwrap();
        assert!(writer.shutdown(std::time::Duration::from_secs(5)).await);
    }

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
    /// A dropped caller must not leave its blocked plan running with its permits held.
    #[test]
    fn dropping_the_caller_aborts_a_plan_waiting_on_io() {
        // A detached plan would block a normal runtime's shutdown, so fail cleanly instead.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            runtime.block_on(dropped_caller_releases_its_plan())
        }));
        runtime.shutdown_background();
        outcome.unwrap();
    }

    async fn dropped_caller_releases_its_plan() {
        struct Held(Arc<AtomicUsize>);
        impl Drop for Held {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let released = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let (p, r, s) = (permits.clone(), released.clone(), started.clone());
        let caller = tokio::spawn(run_io(tokio::runtime::Handle::current(), async move {
            let _permit = p.acquire_owned().await.unwrap();
            let _held = Held(r);
            s.notify_one();
            std::future::pending::<()>().await; // an I/O backend that never answers
        }));
        started.notified().await;
        assert_eq!(permits.available_permits(), 0);
        caller.abort();
        let _ = caller.await;
        let freed = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            permits.clone().acquire_owned(),
        )
        .await;
        assert!(
            freed.is_ok(),
            "the plan kept its permit after its caller was dropped"
        );
        assert_eq!(released.load(Ordering::SeqCst), 1);
    }
}
