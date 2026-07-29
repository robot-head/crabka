//! SP37: the evaluation context (session timezone + the transaction/statement
//! clock) threaded through expression evaluation, and an injectable clock so
//! `now()`/`current_timestamp` are deterministic in tests.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use jiff::{Timestamp, tz::TimeZone};

/// Source of "current time". `SystemClock` in production; `FixedClock` in tests.
pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
}

#[derive(Debug, Default)]
pub struct SystemClock;
impl Clock for SystemClock {
    /// Quantized to microseconds, the resolution every `timestamp` encoding
    /// stores. A sub-microsecond value would compare unequal to its own stored
    /// form while encoding to identical bytes, which a unique index reads as a
    /// duplicate — the same hazard the text parsers guard against.
    fn now(&self) -> Timestamp {
        Timestamp::from_microsecond(Timestamp::now().as_microsecond())
            .unwrap_or_else(|_| Timestamp::now())
    }
}

/// A fixed clock for deterministic tests.
#[derive(Debug, Clone)]
pub struct FixedClock(pub Timestamp);
impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        self.0
    }
}

/// Per-statement evaluation context. `now`/`stmt_now` are the transaction- and
/// statement-start instants (PG transaction-stable semantics); `time_zone` is the
/// effective session zone; `clock` backs `clock_timestamp()`.
#[derive(Clone)]
pub struct EvalCtx {
    pub now: Timestamp,
    pub stmt_now: Timestamp,
    pub time_zone: TimeZone,
    pub current_user: String,
    pub session_user: String,
    pub clock: Arc<dyn Clock>,
    pub(crate) sequence: Option<Arc<SequenceRuntime>>,
    /// The session's queued `LISTEN`/`NOTIFY` work, so the side-effecting
    /// `pg_notify(channel, payload)` can enqueue from inside expression
    /// evaluation — the same seam `sequence` gives `nextval`. `None` outside a
    /// SQL session (planning contexts, unit tests), where `pg_notify` is an
    /// error rather than a silent no-op.
    pub(crate) notify: Option<Arc<Mutex<crate::session::NotifyPending>>>,
}

pub(crate) struct SequenceRuntime {
    pub(crate) kv: Arc<dyn crabka_pgkv::Kv>,
    pub(crate) manager: Arc<crate::seq::SequenceManager>,
    pub(crate) currvals: Arc<Mutex<HashMap<String, i64>>>,
}

impl EvalCtx {
    /// A UTC context anchored at the Unix epoch — for tests / non-temporal eval.
    pub fn test_default() -> Self {
        let epoch = Timestamp::UNIX_EPOCH;
        Self {
            now: epoch,
            stmt_now: epoch,
            time_zone: TimeZone::UTC,
            current_user: "public".into(),
            session_user: "public".into(),
            clock: Arc::new(SystemClock),
            sequence: None,
            notify: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{Clock, SystemClock};

    /// Every `timestamp` encoding stores microseconds. A clock reading with a
    /// finer tail would compare unequal to its own stored form while encoding to
    /// identical bytes — a unique index would read that as a duplicate.
    #[test]
    fn the_system_clock_reads_whole_microseconds() {
        for _ in 0..64 {
            let now = SystemClock.now();
            assert!(now.subsec_nanosecond() % 1_000 == 0);
        }
    }
}
