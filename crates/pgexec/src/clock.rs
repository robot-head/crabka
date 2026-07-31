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
    /// The `DateStyle` field order, which decides how an otherwise-ambiguous
    /// all-numeric date literal (`01/02/03`) is read.
    pub date_order: crabka_pgtypes::datetime::DateOrder,
    /// The `DateStyle` output format, which decides how a `date`, `timestamp`
    /// or `timestamptz` is spelled on the wire.
    pub date_style: crabka_pgtypes::datetime::DateStyle,
    /// The `IntervalStyle` GUC, which decides how an `interval` is spelled.
    pub interval_style: crabka_pgtypes::datetime::IntervalStyle,
    pub current_user: String,
    pub session_user: String,
    /// The session's backend process id — the value the wire layer announced in
    /// `BackendKeyData`, which `pg_backend_pid()` must agree with because that
    /// pairing is how a client correlates a cancel request with its session.
    /// 0 outside a SQL session (a planning context or a unit test), where no
    /// backend id was ever assigned.
    pub(crate) backend_pid: i32,
    pub clock: Arc<dyn Clock>,
    pub(crate) sequence: Option<Arc<SequenceRuntime>>,
    /// The catalog KV on its own, for a context that can read the catalog but
    /// has no sequence machinery — a DDL statement, which must resolve the
    /// relation a `'name'::regclass` default names while `nextval()` stays the
    /// 0A000 it is outside a session. A session leaves this `None` and reads
    /// through `sequence`, which carries the same handle.
    pub(crate) catalog: Option<Arc<dyn crabka_pgkv::Kv>>,
    /// The session's name-resolution scope — its `search_path`, the user
    /// `"$user"` expands to, and its backend id. `None` outside a SQL session
    /// (a planning context or a unit test), where
    /// [`crate::relname::ResolutionScope::default_scope`] stands in: a
    /// relation named there still has to resolve, and `PostgreSQL`'s own
    /// default path is the honest answer.
    pub(crate) resolution: Option<Arc<crate::relname::ResolutionScope>>,
    /// The session's queued `LISTEN`/`NOTIFY` work, so the side-effecting
    /// `pg_notify(channel, payload)` can enqueue from inside expression
    /// evaluation — the same seam `sequence` gives `nextval`. `None` outside a
    /// SQL session (planning contexts, unit tests), where `pg_notify` is an
    /// error rather than a silent no-op.
    pub(crate) notify: Option<Arc<Mutex<crate::session::NotifyPending>>>,
}

pub(crate) struct SequenceRuntime {
    /// The session's catalog KV. `nextval` reads sequence records through it,
    /// and it is the same handle the catalog-introspection functions read
    /// relations, views and comments through — a session has exactly one.
    pub(crate) kv: Arc<dyn crabka_pgkv::Kv>,
    pub(crate) manager: Arc<crate::seq::SequenceManager>,
    pub(crate) currvals: Arc<Mutex<HashMap<String, i64>>>,
}

impl EvalCtx {
    /// The session's text-output settings, in the shape the value layer's text
    /// encoder takes them.
    pub fn output_style(&self) -> crabka_pgtypes::encoding::OutputStyle<'_> {
        crabka_pgtypes::encoding::OutputStyle {
            time_zone: &self.time_zone,
            date_style: self.date_style,
            date_order: self.date_order,
            interval_style: self.interval_style,
        }
    }
}

impl EvalCtx {
    /// The catalog KV backing the `pg_catalog` functions, or `None` outside a
    /// SQL session (a planning context or a unit test), where those functions
    /// report 0A000 rather than inventing an answer.
    pub(crate) fn catalog(&self) -> Option<&dyn crabka_pgkv::Kv> {
        self.catalog
            .as_deref()
            .or_else(|| self.sequence.as_ref().map(|runtime| runtime.kv.as_ref()))
    }

    /// The scope an unqualified relation name resolves against.
    pub(crate) fn resolution(&self) -> &crate::relname::ResolutionScope {
        self.resolution
            .as_deref()
            .unwrap_or_else(|| crate::relname::ResolutionScope::default_scope())
    }
}

impl EvalCtx {
    /// A non-temporal context that still resolves names the session's way —
    /// what a DDL statement evaluates its `DEFAULT`s and partition bounds in.
    /// It has no clock of its own because a DDL statement has no row to stamp;
    /// it does need the search path, because the relation it names has to land
    /// in the right schema, and it needs the catalog, because a `DEFAULT
    /// 'name'::regclass` has a relation name to resolve.
    ///
    /// `catalog` is `None` where no session supplied one (a planning context),
    /// leaving the catalog-reading functions their 0A000.
    pub(crate) fn for_ddl(
        scope: &crate::relname::ResolutionScope,
        catalog: Option<&Arc<dyn crabka_pgkv::Kv>>,
    ) -> Self {
        Self {
            resolution: Some(Arc::new(scope.clone())),
            catalog: catalog.map(Arc::clone),
            ..Self::test_default()
        }
    }

    /// A UTC context anchored at the Unix epoch — for tests / non-temporal eval.
    pub fn test_default() -> Self {
        let epoch = Timestamp::UNIX_EPOCH;
        Self {
            now: epoch,
            stmt_now: epoch,
            time_zone: TimeZone::UTC,
            date_order: crabka_pgtypes::datetime::DateOrder::default(),
            date_style: crabka_pgtypes::datetime::DateStyle::default(),
            interval_style: crabka_pgtypes::datetime::IntervalStyle::default(),
            current_user: "public".into(),
            session_user: "public".into(),
            backend_pid: 0,
            clock: Arc::new(SystemClock),
            sequence: None,
            catalog: None,
            resolution: None,
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
