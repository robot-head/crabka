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
    fn now(&self) -> Timestamp {
        Timestamp::now()
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
        }
    }
}
