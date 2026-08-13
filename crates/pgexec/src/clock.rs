//! SP37: the evaluation context threaded through expression evaluation, and an
//! injectable clock.
//!
//! The context carries the session timezone and the transaction/statement
//! clock. The injectable clock makes `now()` and `current_timestamp`
//! deterministic in tests.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use jiff::{Timestamp, tz::TimeZone};

/// Source of "current time". Production uses `SystemClock` and tests use
/// `FixedClock`.
pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
}

#[derive(Debug, Default)]
pub struct SystemClock;
impl Clock for SystemClock {
    /// Quantized to microseconds, the resolution every `timestamp` encoding
    /// stores.
    ///
    /// A sub-microsecond value would compare unequal to its own stored form
    /// while it encodes to identical bytes, and a unique index reads that as a
    /// duplicate. The text parsers guard against the same hazard.
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

/// Per-statement evaluation context.
///
/// `now` and `stmt_now` are the transaction-start and statement-start instants,
/// with PG transaction-stable semantics. `time_zone` is the effective session
/// zone. `clock` backs `clock_timestamp()`.
#[derive(Clone)]
pub struct EvalCtx {
    pub now: Timestamp,
    pub stmt_now: Timestamp,
    pub time_zone: TimeZone,
    /// The `DateStyle` field order, which decides how an otherwise-ambiguous
    /// all-numeric date literal such as `01/02/03` is read.
    pub date_order: crabka_pgtypes::datetime::DateOrder,
    /// The `DateStyle` output format, which decides how a `date`, `timestamp`
    /// or `timestamptz` is spelled on the wire.
    pub date_style: crabka_pgtypes::datetime::DateStyle,
    /// The `IntervalStyle` GUC, which decides how an `interval` is spelled.
    pub interval_style: crabka_pgtypes::datetime::IntervalStyle,
    /// `extra_float_digits`, which decides how many significant digits a float
    /// renders with. PostgreSQL's default since v12 is 1 (shortest round trip).
    pub extra_float_digits: i32,
    /// `bytea_output`, which decides whether a `bytea` renders as `\x`-prefixed
    /// hex or in the older backslash-octal escape spelling.
    pub bytea_output: crabka_pgtypes::encoding::ByteaOutput,
    pub current_user: String,
    pub session_user: String,
    /// The session's backend process id.
    ///
    /// This is the value the wire layer announced in `BackendKeyData`.
    /// `pg_backend_pid()` must agree with it, because a client uses that
    /// pairing to match a cancel request with its session. The value is 0
    /// outside a SQL session, for example in a planning context or a unit test,
    /// where no backend id was ever assigned.
    pub(crate) backend_pid: i32,
    /// Nesting level of the ordinary trigger that executes in this session
    /// now.
    pub(crate) trigger_depth: u32,
    pub clock: Arc<dyn Clock>,
    /// The session's pseudo-random stream. Cloned statement contexts share the
    /// same locked generator so `setseed()` survives executor thread changes.
    pub(crate) random: Option<Arc<Mutex<crate::math_fn::Prng>>>,
    pub(crate) sequence: Option<Arc<SequenceRuntime>>,
    /// The catalog KV on its own, for a context that can read the catalog but
    /// has no sequence machinery.
    ///
    /// A DDL statement is such a context. It must resolve the relation a
    /// `'name'::regclass` default names, while `nextval()` stays the 0A000 it
    /// is outside a session. A session leaves this field `None` and reads
    /// through `sequence`, which carries the same handle.
    pub(crate) catalog: Option<Arc<dyn crabka_pgkv::Kv>>,
    /// The session's name-resolution scope: its `search_path`, the user
    /// `"$user"` expands to, and its backend id.
    ///
    /// The field is `None` outside a SQL session, for example in a planning
    /// context or a unit test. There
    /// [`crate::relname::ResolutionScope::default_scope`] stands in, because a
    /// relation named there still has to resolve and `PostgreSQL`'s own default
    /// path is the correct answer.
    pub(crate) resolution: Option<Arc<crate::relname::ResolutionScope>>,
    /// The session's queued `LISTEN`/`NOTIFY` work.
    ///
    /// The queue lets the side-effecting `pg_notify(channel, payload)` add work
    /// from inside expression evaluation. It is the same seam `sequence` gives
    /// `nextval`. The field is `None` outside a SQL session, for example in
    /// planning contexts and unit tests, where `pg_notify` is an error and not
    /// a silent no-op.
    pub(crate) notify: Option<Arc<Mutex<crate::session::NotifyPending>>>,
    pub(crate) transition_relations: Option<Arc<Mutex<HashMap<String, TransitionRelation>>>>,
    pub(crate) event_trigger: Option<Arc<EventTriggerContext>>,
    /// The session's transaction identity, for the functions that export it.
    ///
    /// `None` outside a SQL session — in a planning context or a unit test —
    /// where `pg_current_xact_id()` and its family report 0A000 rather than
    /// inventing a transaction that is not running.
    pub(crate) txn: Option<Arc<TxnRuntime>>,
}

/// The transaction state `pg_current_xact_id()`, `pg_current_snapshot()` and
/// `pg_xact_status()` read.
///
/// It is bundled behind one `Option<Arc<…>>` for the same reason
/// [`SequenceRuntime`] is: the four pieces are acquired together or not at all,
/// and every context that has none of them must refuse rather than guess.
pub(crate) struct TxnRuntime {
    /// The statement's read snapshot, exactly as the session took it — the
    /// analogue of `GetActiveSnapshot()`. Under REPEATABLE READ this is the
    /// snapshot fixed at `BEGIN`, which is what makes `pg_current_snapshot()`
    /// stable across the block.
    ///
    /// `None` where the session holds no transaction and so took no snapshot.
    /// A fresh one is then read from `procarray` on demand rather than eagerly,
    /// so building a context costs no lock on the registry every session
    /// shares.
    pub(crate) snapshot: Option<crabka_pgmvcc::visibility::Snapshot>,
    /// The transaction's own xid, if the transaction has already written and
    /// so already has one. `None` is exactly what
    /// `pg_current_xact_id_if_assigned()` reports as NULL.
    pub(crate) own_xid: Option<u64>,
    /// The running-transaction registry, so `pg_current_xact_id()` can assign
    /// an xid to a transaction that has not written, and `pg_xact_status()`
    /// can tell a running transaction from a lost one.
    pub(crate) procarray: Arc<crate::procarray::ProcArray>,
    /// An xid this statement's evaluation assigned, for the session to adopt.
    ///
    /// Expression evaluation holds the context by shared reference and cannot
    /// write the transaction's own xid slot, so it stages the assignment here
    /// and [`crate::session::SqlSession`] moves it across. This is the seam
    /// [`SequenceRuntime::pending`] gives `nextval`, and it exists for the
    /// same reason.
    pub(crate) assigned: Arc<Mutex<Option<u64>>>,
}

#[derive(Debug, Clone)]
pub(crate) struct TransitionRelation {
    pub columns: Vec<(String, crabka_pgtypes::ColumnType)>,
    pub rows: Vec<Vec<crabka_pgtypes::Datum>>,
}

#[derive(Debug, Clone)]
pub(crate) struct EventTriggerObject {
    pub class_id: i32,
    pub object_id: i32,
    pub object_sub_id: i32,
    pub object_type: String,
    pub schema_name: Option<String>,
    pub object_name: Option<String>,
    pub identity: String,
    /// Whether the object lived in a temporary namespace, which
    /// `pg_event_trigger_dropped_objects` reports as `is_temporary`.
    ///
    /// This is carried rather than derived. `schema_name` holds the *display*
    /// spelling, and [`crabka_pgcatalog::displayed_schema`] renders every
    /// temporary namespace as the bare alias `pg_temp`, which
    /// [`crabka_pgcatalog::is_temp_schema`] deliberately answers `false` for.
    /// The stored `pg_temp_<backend id>` is the only spelling the predicate
    /// recognises, and it is gone by the time a reader sees this struct — so
    /// the producer records the fact while it still holds the stored name.
    pub is_temporary: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct EventTriggerContext {
    pub event: crabka_pgcatalog::trigger::EventTriggerEvent,
    pub tag: String,
    pub commands: Vec<EventTriggerObject>,
    pub dropped: Vec<EventTriggerObject>,
    pub rewrite: Option<(i32, i32)>,
}

pub(crate) struct SequenceRuntime {
    /// The session's catalog KV.
    ///
    /// `nextval` reads sequence records through it. It is the same handle the
    /// catalog-introspection functions read relations, views and comments
    /// through. A session has exactly one.
    pub(crate) kv: Arc<dyn crabka_pgkv::Kv>,
    /// The session's row/index KV. This can differ from `kv` when catalog and
    /// data storage are split.
    pub(crate) data: Arc<dyn crabka_pgkv::Kv>,
    pub(crate) manager: Arc<crate::seq::SequenceManager>,
    pub(crate) currvals: Arc<Mutex<HashMap<String, i64>>>,
    /// The session's sequence advances that are not durable yet.
    ///
    /// A `Replicated` engine stages them here instead of a write through the
    /// store. The session folds them into the next batch it commits. This is
    /// the same seam [`EvalCtx::notify`] gives `pg_notify()`, and for the same
    /// reason: expression evaluation is synchronous and cannot await a commit.
    /// `Durable` mode persists as it goes and leaves this field empty.
    pub(crate) pending: Arc<Mutex<crate::seq::PendingSequences>>,
}

impl EvalCtx {
    /// The session's text-output settings, in the shape the value layer's text
    /// encoder takes.
    pub fn output_style(&self) -> crabka_pgtypes::encoding::OutputStyle<'_> {
        crabka_pgtypes::encoding::OutputStyle {
            time_zone: &self.time_zone,
            date_style: self.date_style,
            date_order: self.date_order,
            interval_style: self.interval_style,
            extra_float_digits: self.extra_float_digits,
            bytea_output: self.bytea_output,
        }
    }
}

impl EvalCtx {
    /// The catalog KV that backs the `pg_catalog` functions.
    ///
    /// The value is `None` outside a SQL session, for example in a planning
    /// context or a unit test. Those functions then report 0A000 and do not
    /// invent an answer.
    pub(crate) fn catalog(&self) -> Option<&dyn crabka_pgkv::Kv> {
        self.catalog
            .as_deref()
            .or_else(|| self.sequence.as_ref().map(|runtime| runtime.kv.as_ref()))
    }

    /// The data KV backing physical row and index entries.
    pub(crate) fn data(&self) -> Option<&dyn crabka_pgkv::Kv> {
        self.sequence.as_ref().map(|runtime| runtime.data.as_ref())
    }

    /// The session's transaction identity, or the 0A000 every member of the
    /// transaction-id family reports where there is no session behind the
    /// evaluation.
    ///
    /// # Errors
    ///
    /// 0A000 naming the function the caller was asked for.
    pub(crate) fn txn(&self, what: &str) -> Result<&TxnRuntime, crate::error::ExecError> {
        self.txn.as_deref().ok_or_else(|| {
            crate::error::ExecError::Unsupported(format!("{what} requires a SQL session"))
        })
    }

    /// The scope an unqualified relation name resolves against.
    pub(crate) fn resolution(&self) -> &crate::relname::ResolutionScope {
        self.resolution
            .as_deref()
            .unwrap_or_else(|| crate::relname::ResolutionScope::default_scope())
    }

    /// The database this session connected to.
    ///
    /// `current_database()`, the single `pg_database` row, `pg_stat_activity`
    /// and every `information_schema` `*_catalog` column answer with this, so a
    /// client that connected as `dbname=sales` is never told it is somewhere
    /// else. It rides on the resolution scope because the same name decides
    /// whether a three-part `sales.public.t` is a local reference or a
    /// cross-database one.
    pub(crate) fn database(&self) -> &str {
        &self.resolution().database
    }
}

impl EvalCtx {
    /// A non-temporal context that still resolves names the session's way.
    ///
    /// A DDL statement evaluates its `DEFAULT`s and partition bounds in this
    /// context. The context has no clock of its own, because a DDL statement
    /// has no row to stamp. It does need the search path, because the relation
    /// it names has to reach the right schema. It also needs the catalog,
    /// because a `DEFAULT 'name'::regclass` has a relation name to resolve.
    ///
    /// `catalog` is `None` where no session supplied one, for example in a
    /// planning context. The catalog-reading functions then keep their 0A000.
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

    /// A UTC context anchored at the Unix epoch, for tests and non-temporal
    /// evaluation.
    pub fn test_default() -> Self {
        let epoch = Timestamp::UNIX_EPOCH;
        Self {
            now: epoch,
            stmt_now: epoch,
            time_zone: TimeZone::UTC,
            date_order: crabka_pgtypes::datetime::DateOrder::default(),
            date_style: crabka_pgtypes::datetime::DateStyle::default(),
            interval_style: crabka_pgtypes::datetime::IntervalStyle::default(),
            extra_float_digits: 1,
            bytea_output: crabka_pgtypes::encoding::ByteaOutput::default(),
            current_user: "public".into(),
            session_user: "public".into(),
            backend_pid: 0,
            trigger_depth: 0,
            clock: Arc::new(SystemClock),
            random: None,
            sequence: None,
            catalog: None,
            resolution: None,
            notify: None,
            transition_relations: None,
            event_trigger: None,
            txn: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{Clock, SystemClock};

    /// Every `timestamp` encoding stores microseconds. A clock reading with a
    /// finer tail would compare unequal to its own stored form while it encodes
    /// to identical bytes, and a unique index would read that as a duplicate.
    #[test]
    fn the_system_clock_reads_whole_microseconds() {
        for _ in 0..64 {
            let now = SystemClock.now();
            assert!(now.subsec_nanosecond() % 1_000 == 0);
        }
    }
}
