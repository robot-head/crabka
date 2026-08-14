//! Shared container helpers for the Docker-backed suites in this crate.
//!
//! # Why this module exists
//!
//! `testcontainers` 0.28 bounds only the tail of `start()`. Its
//! `startup_timeout`, 60 s by default, wraps the daemon start call and the
//! wait conditions. Those steps run *after* the container exists. The image
//! pull runs before them and drains the daemon pull stream with no deadline
//! at all. A registry mirror that accepts the connection and then stops
//! sending bytes leaves `start()` pending for ever.
//!
//! That is not a slow test, it is a test that waits. On 2026-08-13 the
//! `loki_differential` suite held four such processes open and the CI job was
//! cancelled at its 90 minute limit with no diagnosis. A larger job timeout
//! only makes the wait longer.
//!
//! [`start_container`] puts a deadline around the whole of `start()`, the
//! pull included, and it names the image and the bound when the deadline
//! passes.

use std::{fmt::Display, future::Future, time::Duration};

use testcontainers::{ContainerAsync, GenericImage, runners::AsyncRunner};
use tokio::time::{Instant, sleep, timeout_at};

/// Budget for one call to [`start_container`], retries included.
///
/// Measured on 2026-08-12 against `mirror.gcr.io`: a cold pull of the pinned
/// Loki image took 6.1 s, and a warm container create plus start took 0.2 s
/// to 0.8 s, eight of them at once. A healthy first start therefore costs
/// about 9 s once the settle wait of the suites is added. This bound leaves
/// more than ten times that headroom for a slower runner.
///
/// The upper end matters as much as the lower end. The coverage job splits
/// the suite over two shards and runs four test processes at a time, so about
/// 15 waves of Loki tests reach one shard. A registry that is down therefore
/// costs about 30 minutes, and the job **fails** inside its own limit instead
/// of being cancelled.
pub const CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(2);

/// Start attempts inside [`CONTAINER_START_TIMEOUT`].
///
/// A stalled pull uses the whole budget on the first attempt. The retries
/// therefore only help against a fast and transient daemon error.
pub const CONTAINER_START_ATTEMPTS: usize = 3;

/// Delay between two start attempts.
pub const CONTAINER_START_RETRY_DELAY: Duration = Duration::from_secs(3);

/// Starts a container and gives the start a deadline.
///
/// `image_ref` names the image with its tag. The failure message quotes it,
/// so a stalled pull reads as a registry problem and not as a mystery hang.
/// `request` builds the container request, and it runs again for each retry.
///
/// # Panics
///
/// Panics when the container does not start inside
/// [`CONTAINER_START_TIMEOUT`], and when every attempt fails.
pub async fn start_container<F, R>(image_ref: &str, mut request: F) -> ContainerAsync<GenericImage>
where
    F: FnMut() -> R,
    R: AsyncRunner<GenericImage>,
{
    match start_within_budget(image_ref, || request().start()).await {
        Ok(container) => container,
        Err(report) => panic!("{report}"),
    }
}

/// Runs `attempt` until it succeeds, the attempts run out, or the budget ends.
///
/// The error is the message to show. It names the image and the bound.
///
/// This is the seam that the tests below drive. It takes a future instead of a
/// container request, because `AsyncRunner` has a blanket implementation and a
/// test cannot supply a stalled container start through it.
async fn start_within_budget<T, E, F, Fut>(image_ref: &str, mut attempt: F) -> Result<T, String>
where
    E: Display,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let bound = CONTAINER_START_TIMEOUT;
    let deadline = Instant::now() + bound;
    let mut last_error = String::from("no attempt ran");

    for number in 1..=CONTAINER_START_ATTEMPTS {
        match timeout_at(deadline, attempt()).await {
            Ok(Ok(started)) => return Ok(started),
            Ok(Err(error)) => {
                last_error = error.to_string();
                eprintln!(
                    "start of {image_ref} failed on attempt {number} of \
                     {CONTAINER_START_ATTEMPTS}: {last_error}"
                );
            }
            Err(_elapsed) => {
                return Err(format!(
                    "{image_ref} did not start within {bound:?}, on attempt {number} of \
                     {CONTAINER_START_ATTEMPTS}. The image pull or the container start is \
                     stuck. Check the registry mirror, or pull the image before the test."
                ));
            }
        }

        if number < CONTAINER_START_ATTEMPTS {
            let _ = timeout_at(deadline, sleep(CONTAINER_START_RETRY_DELAY)).await;
        }
    }

    Err(format!(
        "{image_ref} did not start after {CONTAINER_START_ATTEMPTS} attempts within {bound:?}. \
         Last error: {last_error}"
    ))
}

#[tokio::test(start_paused = true)]
async fn a_stalled_start_reports_the_image_and_the_bound() {
    let image_ref = "mirror.example/stalled/image:1.2.3";

    let report = start_within_budget(image_ref, || {
        std::future::pending::<Result<(), std::convert::Infallible>>()
    })
    .await
    .expect_err("a start that never finishes must not succeed");

    assert2::assert!(report.contains(image_ref));
    assert2::assert!(report.contains(&format!("{CONTAINER_START_TIMEOUT:?}")));
}

#[tokio::test(start_paused = true)]
async fn a_transient_failure_is_retried_inside_the_budget() {
    let attempts = std::cell::Cell::new(0_usize);

    let started = start_within_budget("mirror.example/flaky/image:1", || {
        let number = attempts.get() + 1;
        attempts.set(number);
        async move {
            if number < CONTAINER_START_ATTEMPTS {
                Err("daemon busy")
            } else {
                Ok("started")
            }
        }
    })
    .await
    .expect("the last attempt succeeds");

    assert2::assert!(started == "started");
    assert2::assert!(attempts.get() == CONTAINER_START_ATTEMPTS);
}

#[tokio::test(start_paused = true)]
async fn a_start_that_always_fails_reports_the_last_error() {
    let image_ref = "mirror.example/broken/image:9";

    let report = start_within_budget(image_ref, || async { Err::<(), _>("no such image") })
        .await
        .expect_err("every attempt fails");

    assert2::assert!(report.contains(image_ref));
    assert2::assert!(report.contains("no such image"));
    assert2::assert!(report.contains(&CONTAINER_START_ATTEMPTS.to_string()));
}
