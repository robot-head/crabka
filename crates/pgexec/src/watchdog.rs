//! The in-flight statement registry and the watchdog that reports the ones that
//! stop finishing.
//!
//! A wedged server looks like an idle one: every worker parked, every socket
//! still `ESTAB`, and nothing in the log to say which backend stopped making
//! progress or what it was running. The registry closes that gap. Each session
//! records the statement it is about to run and clears it on the way out; a
//! background loop periodically reports whatever has been sitting there too
//! long.
//!
//! Two properties are load bearing:
//!
//! * **Observation only.** Nothing here cancels, interrupts, or times a
//!   statement out. A statement that is legitimately slow is reported once and
//!   then left alone to finish, so turning the threshold down cannot change a
//!   result.
//! * **Nothing per row.** A statement costs one map insert on entry and one
//!   removal on exit, plus a bounded copy of its leading text. The scan runs on
//!   the watchdog's own schedule, never on the statement's.

use std::{
    collections::{HashMap, hash_map::Entry},
    fmt,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use crabka_units::{Time, convert::TimeExt as _};

/// How much of a statement's text the registry keeps.
///
/// The copy is what makes registration cost a bounded amount regardless of the
/// statement: a megabyte-long `INSERT` costs the same as `SELECT 1`. A kibibyte
/// is far more than enough to recognize a statement in a report.
const STATEMENT_TEXT_CAP: usize = 1024;

/// The shortest poll interval the watchdog will honor. A policy asking for less
/// (or for zero) gets this instead, so a misconfiguration cannot turn the
/// watchdog into a spin loop.
const MIN_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Where a session stood in its transaction when the statement started.
///
/// A wedged autocommit statement and a wedged statement inside an open block
/// are different problems — the second is holding a transaction's locks open —
/// so the report says which it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionActivity {
    /// No explicit transaction block open.
    Idle,
    /// Inside an open transaction block.
    InTransaction,
    /// Inside a block already `PREPARE TRANSACTION`d.
    Prepared,
    /// Inside a block that has failed and awaits `ROLLBACK`.
    Failed,
}

impl fmt::Display for TransactionActivity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Idle => "idle",
            Self::InTransaction => "in transaction",
            Self::Prepared => "prepared",
            Self::Failed => "failed transaction",
        })
    }
}

/// A statement the registry currently believes to be running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InFlightStatement {
    /// The backend id `pg_backend_pid()` answers with for the running session.
    pub backend_pid: i32,
    /// How long the statement has been running.
    pub elapsed: Duration,
    /// The session's transaction state when the statement started.
    pub transaction: TransactionActivity,
    /// The statement's leading text, truncated to [`STATEMENT_TEXT_CAP`].
    pub statement: String,
}

/// A statement that has outlived the configured threshold and is due to be
/// reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StuckStatement {
    /// The backend id `pg_backend_pid()` answers with for the running session.
    pub backend_pid: i32,
    /// How long the statement has been running.
    pub elapsed: Duration,
    /// The session's transaction state when the statement started.
    pub transaction: TransactionActivity,
    /// The statement's leading text, truncated to [`STATEMENT_TEXT_CAP`].
    pub statement: String,
    /// False for a statement's first report, true for every later one. A hang
    /// that outlives its first report is still visible without every poll
    /// re-logging it.
    pub repeated: bool,
}

/// How long a statement may run before the watchdog reports it, and how often
/// the watchdog looks.
///
/// The threshold only has to sit above the slowest statement a healthy server
/// runs; it is a diagnostic bound, not an execution limit, so overshooting it
/// costs a log line and nothing else.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StuckStatementPolicy {
    /// A statement running longer than this is reported.
    pub threshold: Time,
    /// How often the watchdog scans the registry.
    pub poll_interval: Time,
    /// How long after reporting a statement the watchdog will report it again,
    /// if it is still running.
    pub repeat_interval: Time,
}

impl Default for StuckStatementPolicy {
    fn default() -> Self {
        Self {
            threshold: crabka_units::secs(120),
            poll_interval: crabka_units::secs(5),
            repeat_interval: crabka_units::secs(600),
        }
    }
}

impl StuckStatementPolicy {
    /// Whether every interval is a finite, strictly positive extent.
    pub(crate) fn is_valid(self) -> bool {
        [self.threshold, self.poll_interval, self.repeat_interval]
            .into_iter()
            .all(|extent| extent.secs_f64().is_finite() && extent.secs_f64() > 0.0)
    }
}

/// One entry in the registry: what a backend is running, since when, and when
/// the watchdog last said so.
#[derive(Debug)]
struct Running {
    /// Distinguishes this registration from a later one on the same backend, so
    /// a guard can only ever remove the entry it created.
    token: u64,
    started: Instant,
    transaction: TransactionActivity,
    statement: Box<str>,
    reported: Option<Instant>,
}

/// Every statement currently executing on this server, keyed by backend id.
///
/// A session runs one statement at a time, so a backend has at most one entry.
#[derive(Debug, Default)]
pub struct StatementRegistry {
    running: Mutex<HashMap<i32, Running>>,
    next_token: AtomicU64,
}

impl StatementRegistry {
    /// Record `statement` as `backend_pid`'s in-flight statement until the
    /// returned guard drops.
    pub(crate) fn begin(
        self: &Arc<Self>,
        backend_pid: i32,
        statement: &str,
        transaction: TransactionActivity,
    ) -> StatementGuard {
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        self.lock().insert(
            backend_pid,
            Running {
                token,
                started: Instant::now(),
                transaction,
                statement: truncated(statement),
                reported: None,
            },
        );
        StatementGuard {
            registry: Arc::clone(self),
            backend_pid,
            token,
        }
    }

    /// Every statement currently in flight, longest-running first.
    ///
    /// This is the registry's read seam: it says what the watchdog would see
    /// without waiting for a poll or touching the log.
    pub fn in_flight(&self) -> Vec<InFlightStatement> {
        let now = Instant::now();
        let mut running = self
            .lock()
            .iter()
            .map(|(&backend_pid, entry)| InFlightStatement {
                backend_pid,
                elapsed: now.saturating_duration_since(entry.started),
                transaction: entry.transaction,
                statement: entry.statement.to_string(),
            })
            .collect::<Vec<_>>();
        running.sort_by(|a, b| {
            b.elapsed
                .cmp(&a.elapsed)
                .then(a.backend_pid.cmp(&b.backend_pid))
        });
        running
    }

    /// The statements due to be reported at `now`, longest-running first, each
    /// marked as reported so the next poll stays quiet.
    ///
    /// A statement is due when it has been running for at least `threshold` and
    /// either has never been reported or was last reported at least `repeat`
    /// ago.
    pub fn due_reports(
        &self,
        now: Instant,
        threshold: Duration,
        repeat: Duration,
    ) -> Vec<StuckStatement> {
        let mut due = Vec::new();
        for (&backend_pid, entry) in self.lock().iter_mut() {
            let elapsed = now.saturating_duration_since(entry.started);
            if elapsed < threshold {
                continue;
            }
            let repeated = match entry.reported {
                None => false,
                Some(reported) => {
                    if now.saturating_duration_since(reported) < repeat {
                        continue;
                    }
                    true
                }
            };
            entry.reported = Some(now);
            due.push(StuckStatement {
                backend_pid,
                elapsed,
                transaction: entry.transaction,
                statement: entry.statement.to_string(),
                repeated,
            });
        }
        due.sort_by(|a, b| {
            b.elapsed
                .cmp(&a.elapsed)
                .then(a.backend_pid.cmp(&b.backend_pid))
        });
        due
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<i32, Running>> {
        self.running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Holds a backend's registration for the length of one statement.
///
/// Deregistration is `Drop`'s job rather than a call at the end of the happy
/// path, because a statement has many ways out: it can return a row set, return
/// an error from any of a hundred places, or have its future dropped by
/// protocol cancellation. Every one of those runs `Drop`, and a registration
/// that survived any of them would be a stuck statement that never unsticks —
/// a permanent false alarm in every later report.
pub(crate) struct StatementGuard {
    registry: Arc<StatementRegistry>,
    backend_pid: i32,
    token: u64,
}

impl Drop for StatementGuard {
    fn drop(&mut self) {
        let mut running = self.registry.lock();
        if let Entry::Occupied(entry) = running.entry(self.backend_pid)
            && entry.get().token == self.token
        {
            entry.remove();
        }
    }
}

/// Keep `statement`'s leading [`STATEMENT_TEXT_CAP`] bytes, cut on a character
/// boundary.
fn truncated(statement: &str) -> Box<str> {
    if statement.len() <= STATEMENT_TEXT_CAP {
        return statement.into();
    }
    let mut end = STATEMENT_TEXT_CAP;
    while !statement.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &statement[..end]).into_boxed_str()
}

/// Poll `registry` on the policy's interval and log every statement that has
/// outstayed the policy's threshold.
///
/// Returns when the registry has no owner left, which is how the loop retires
/// with the engine it was started for.
pub(crate) async fn watch(registry: Weak<StatementRegistry>, policy: StuckStatementPolicy) {
    let poll = policy.poll_interval.to_std().max(MIN_POLL_INTERVAL);
    let threshold = policy.threshold.to_std();
    let repeat = policy.repeat_interval.to_std();
    loop {
        tokio::time::sleep(poll).await;
        let Some(registry) = registry.upgrade() else {
            return;
        };
        for stuck in registry.due_reports(Instant::now(), threshold, repeat) {
            tracing::warn!(
                backend_pid = stuck.backend_pid,
                elapsed_secs = stuck.elapsed.as_secs_f64(),
                transaction = %stuck.transaction,
                repeated = stuck.repeated,
                statement = %stuck.statement,
                "statement has not finished within the stuck-statement threshold"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn registry() -> Arc<StatementRegistry> {
        Arc::new(StatementRegistry::default())
    }

    #[test]
    fn a_guard_deregisters_however_its_statement_ends() {
        let registry = registry();
        {
            let _running = registry.begin(7, "SELECT 1", TransactionActivity::Idle);
            assert!(registry.in_flight().len() == 1);
        }
        assert!(registry.in_flight() == vec![]);
    }

    #[test]
    fn a_stale_guard_cannot_remove_a_newer_registration() {
        let registry = registry();
        let first = registry.begin(7, "SELECT 1", TransactionActivity::Idle);
        let second = registry.begin(7, "SELECT 2", TransactionActivity::Idle);
        drop(first);
        assert!(
            registry
                .in_flight()
                .into_iter()
                .map(|entry| entry.statement)
                .collect::<Vec<_>>()
                == vec!["SELECT 2".to_owned()]
        );
        drop(second);
        assert!(registry.in_flight() == vec![]);
    }

    #[test]
    fn a_report_repeats_only_after_the_repeat_interval() {
        let registry = registry();
        let _running = registry.begin(9, "SELECT 1", TransactionActivity::InTransaction);
        let start = Instant::now();
        let threshold = Duration::from_secs(10);
        let repeat = Duration::from_secs(100);

        let cases = [
            (Duration::from_secs(1), None),
            (Duration::from_secs(11), Some(false)),
            (Duration::from_secs(65), None),
            (Duration::from_secs(125), Some(true)),
            (Duration::from_secs(155), None),
            (Duration::from_secs(230), Some(true)),
        ];
        for (offset, expected) in cases {
            let due = registry.due_reports(start + offset, threshold, repeat);
            assert!(
                due.iter().map(|stuck| stuck.repeated).next() == expected,
                "at {offset:?}"
            );
        }
    }

    #[test]
    fn long_statement_text_is_truncated_to_the_cap() {
        let registry = registry();
        let _running = registry.begin(
            3,
            &format!("SELECT '{}'", "x".repeat(4096)),
            TransactionActivity::Idle,
        );
        let statement = registry.in_flight().remove(0).statement;
        assert!(statement.len() <= STATEMENT_TEXT_CAP + '…'.len_utf8());
        assert!(statement.ends_with('…'));
    }

    #[test]
    fn truncation_cuts_on_a_character_boundary() {
        let text = "é".repeat(STATEMENT_TEXT_CAP);
        let truncated = truncated(&text);
        assert!(truncated.ends_with('…'));
        assert!(truncated.trim_end_matches('…').chars().all(|c| c == 'é'));
    }

    #[test]
    fn the_default_policy_is_valid_and_zero_intervals_are_not() {
        assert!(StuckStatementPolicy::default().is_valid());
        let zeroed = [
            StuckStatementPolicy {
                threshold: crabka_units::secs(0),
                ..Default::default()
            },
            StuckStatementPolicy {
                poll_interval: crabka_units::secs(0),
                ..Default::default()
            },
            StuckStatementPolicy {
                repeat_interval: crabka_units::secs(0),
                ..Default::default()
            },
        ];
        for policy in zeroed {
            assert!(!policy.is_valid(), "{policy:?}");
        }
    }
}
