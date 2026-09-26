//! Running work that must not hold an async worker on tokio's blocking
//! pool, and waiting for it. The small directory operations of retention
//! and the disk budget check still run inline, and so does, on Linux, a
//! disk-tier page read the operating system's page cache serves without
//! blocking (`DiskChunks::page`); everything that decodes, merges, writes a
//! chunk file or reads a disk-tier page that would block comes here.

use tokio::task::JoinError;

/// Runs `work` on the blocking pool and waits for it: CPU-bound or
/// blocking work (a load's verification, merge and chunk-file write, the
/// doctor's bucket check, a disk-tier page read that would block) that
/// inline would hold the async worker that also answers lookups.
/// The worker awaiting it goes on polling other tasks, and several awaited
/// at once, as the futures of one `buffered` stream are, run on several
/// threads. `work` owns what it reads, as the `'static` bound requires. A
/// panic in `work` is resumed here, as an inline call would raise it. A
/// caller that stops waiting does not stop `work`: it runs to its end and
/// its result is dropped. `Err` is the task the pool dropped unstarted
/// because the runtime shut down; the caller names it for the work it was.
pub(crate) async fn on_blocking_pool<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
) -> std::result::Result<T, JoinError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| match error.try_into_panic() {
            Ok(payload) => std::panic::resume_unwind(payload),
            Err(canceled) => canceled,
        })
}

#[cfg(test)]
mod tests {
    use std::panic::AssertUnwindSafe;
    use std::time::Duration;

    use futures::FutureExt as _;

    use super::*;

    /// A marker a test panics with, so the panic's payload is known by type.
    struct Marker;

    /// Work on the blocking pool leaves the runtime's thread polling other
    /// tasks: on the current-thread runtime, whose one thread inline work
    /// would hold, the work waits for a message that a task spawned on the
    /// runtime sends, the stand-in for a lookup answered while a file
    /// verifies, and receives it.
    #[tokio::test]
    async fn test_on_blocking_pool_leaves_the_runtime_polling_other_tasks() {
        const HELD_AT_MOST: Duration = Duration::from_secs(30);
        let (sender, receiver) = std::sync::mpsc::channel();
        let lookup = tokio::spawn(async move { sender.send(()) });
        let held = on_blocking_pool(move || receiver.recv_timeout(HELD_AT_MOST))
            .await
            .unwrap();
        assert_eq!(
            held,
            Ok(()),
            "the held work never heard from the spawned task: the runtime did not poll it"
        );
        lookup.await.unwrap().unwrap();
    }

    /// A panic in work on the blocking pool is resumed on the caller, with
    /// its payload, as an inline call would raise it.
    #[tokio::test]
    async fn test_on_blocking_pool_resumes_a_panic_on_the_caller() {
        let outcome =
            AssertUnwindSafe(on_blocking_pool(|| std::panic::panic_any::<Marker>(Marker)))
                .catch_unwind()
                .await;
        let payload = outcome.expect_err("the work panicked");
        assert!(payload.downcast::<Marker>().is_ok());
    }
}
