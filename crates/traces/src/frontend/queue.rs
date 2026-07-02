//! Bounded-concurrency fan-out of planned jobs across queriers. Results return
//! in completion order; callers must key results by job identity (`traceID` /
//! `(type,value)`), not position — the merge layer does exactly this.
//!
//! This is the `futures::buffer_unordered` re-expression of the legacy
//! `JoinSet` + `Semaphore` admission queue; both bound max-concurrency, this one
//! more declaratively.

use std::future::Future;

use futures::stream::{self, StreamExt};

/// Run `jobs` through `run` with at most `max_concurrency` in flight at once.
/// Returns every result (completion order, unordered). A zero concurrency clamps
/// to one.
pub async fn run_jobs<T, R, F, Fut>(jobs: Vec<T>, max_concurrency: usize, run: F) -> Vec<R>
where
    F: Fn(T) -> Fut,
    Fut: Future<Output = R>,
{
    let limit = max_concurrency.max(1);
    stream::iter(jobs)
        .map(run)
        .buffer_unordered(limit)
        .collect()
        .await
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use assert2::assert;

    use super::*;

    #[tokio::test]
    async fn runs_all_jobs_with_bounded_concurrency() {
        let inflight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let jobs: Vec<usize> = (0..20).collect();

        let inflight_c = inflight.clone();
        let max_seen_c = max_seen.clone();
        let results = run_jobs(jobs, 4, move |j| {
            let inflight = inflight_c.clone();
            let max_seen = max_seen_c.clone();
            async move {
                let now = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, Ordering::SeqCst);
                tokio::task::yield_now().await;
                inflight.fetch_sub(1, Ordering::SeqCst);
                j * 2
            }
        })
        .await;

        assert!(results.len() == 20);
        let sum: usize = results.iter().sum();
        let expected: usize = (0..20).map(|j| j * 2).sum();
        assert!(sum == expected);
        assert!(max_seen.load(Ordering::SeqCst) <= 4);
    }

    #[tokio::test]
    async fn zero_concurrency_clamps_to_one() {
        let results = run_jobs(vec![1, 2, 3], 0, |j| async move { j }).await;
        assert!(results.len() == 3);
    }

    #[tokio::test]
    async fn empty_jobs_returns_empty() {
        let results: Vec<usize> = run_jobs(Vec::new(), 4, |j: usize| async move { j }).await;
        assert!(results.is_empty());
    }
}
