//! Splitting state that concurrent lookups write into per-core shards: how
//! many shards, from the runtime's worker threads, and which shard a thread
//! writes, so lookups running on different workers do not contend for one
//! cache line or one lock.

use std::num::NonZero;
use std::sync::atomic::{AtomicUsize, Ordering};

/// The shard count for state that every worker thread writes: the worker
/// threads of the tokio runtime the caller runs on, or the machine's
/// available parallelism when called outside a runtime (or 1 when the
/// platform does not report it), rounded up to a power of two so that a
/// thread's `slot` masks into it.
pub(crate) fn count() -> usize {
    let threads = tokio::runtime::Handle::try_current().map_or_else(
        |_outside| std::thread::available_parallelism().map_or(1, NonZero::get),
        |runtime| runtime.metrics().num_workers(),
    );
    threads.max(1).next_power_of_two()
}

/// The next slot a thread draws on its first `slot` call.
static NEXT_SLOT: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    /// The calling thread's slot, drawn from `NEXT_SLOT` on first use.
    static SLOT: usize = NEXT_SLOT.fetch_add(1, Ordering::Relaxed);
}

/// The calling thread's slot: a number drawn once per thread, in the order
/// threads first ask. Masked by `count() - 1`, it gives the first threads
/// to ask distinct shards, so a runtime's workers write shards of their own
/// when they are the threads that draw.
pub(crate) fn slot() -> usize {
    SLOT.with(|slot| *slot)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_is_a_power_of_two_outside_a_runtime() {
        assert!(count().is_power_of_two());
    }

    #[test]
    fn test_count_follows_the_runtime_workers_rounded_up() {
        let workers = 3;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .build()
            .unwrap();
        assert_eq!(
            runtime.block_on(async { count() }),
            workers.next_power_of_two()
        );
        let single = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        assert_eq!(single.block_on(async { count() }), 1);
    }

    #[test]
    fn test_slot_is_stable_on_a_thread_and_distinct_across_threads() {
        let here = slot();
        assert_eq!(slot(), here);
        let there = std::thread::spawn(slot).join().unwrap();
        assert_ne!(there, here);
    }
}
