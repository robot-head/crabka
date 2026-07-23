//! Per-connection session: runs SQL against the shared KV store. SP6 uses
//! PostgreSQL's xid/clog/snapshot MVCC with concurrent writers: writes go
//! through to disk tagged with the transaction's xid (read-your-writes via
//! `satisfies_mvcc` + own xid), commit/rollback record the outcome in the clog,
//! row-level conflicts serialize through the `RowLockManager` (held until
//! COMMIT/ROLLBACK and freed by `release_all`), and DDL serializes among DDLs
//! behind a small `catalog_lock`.

use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

use crabka_pgkv::Kv;
use crabka_pgmvcc::{clog::XidStatus, visibility::Snapshot};
use crabka_pgparser::ast::{
    BinaryOp, CopyFormat, CopyStmt, Expr, FuncArgs, IsolationLevel, JoinConstraint, QueryBody,
    QueryExpr, ResetTarget, RowLockStrength, SelectItem, SetExpr, Statement, TableExpr, UnaryOp,
};
use crabka_pgtypes::{ColumnType, Datum};
use crabka_pgwire::{
    engine::{
        BoundParam, CloseTarget, CopyInResponse, ExecuteOutcome, FieldDescription,
        PortalDescription, PreparedDescription, QueryResult, Session, TxStatus,
    },
    error::{PgError, sqlstate},
};
use tokio::sync::OwnedRwLockReadGuard;

use crate::{
    error::ExecError, exec::UniqueLocalSerialization, lockmgr::RowLockManager,
    procarray::ProcArray, seq::SequenceManager,
};

/// In-flight transaction context.
pub(crate) struct TxnCtx {
    /// Assigned lazily at the first write (None for a read-only transaction).
    pub(crate) xid: Option<u64>,
    /// The visibility snapshot: re-taken per statement under READ COMMITTED,
    /// fixed at BEGIN under REPEATABLE READ.
    pub(crate) snapshot: Snapshot,
    /// Holds the garbage horizon at or below this transaction's BEGIN-time
    /// snapshot xmin until COMMIT/ROLLBACK, so no version this transaction's
    /// snapshot(s) can see is pruned while the block is open. READ COMMITTED
    /// re-snapshots per statement, but the BEGIN-time xmin is `<=` every later
    /// snapshot's xmin, so one conservative pin covers the whole block.
    pub(crate) _snapshot_pin: crabka_pgmvcc::gc::SnapshotPin,
    pub(crate) repeatable_read: bool,
    /// The GLOBAL snapshot the cross-range resolver (`exec::global_status`) gates
    /// `Prepared(-> g)` rows against. Captured at BEGIN for REPEATABLE READ (fixed
    /// for the txn's life), re-captured per statement for READ COMMITTED. `None`
    /// on a non-GTM (single-range) engine — reads then use `NO_GLOBAL_SNAPSHOT()`
    /// and the `Prepared` branch is unreachable.
    pub(crate) global_snapshot: Option<Snapshot>,
    /// Finite range-0 read timestamp fixed at BEGIN for REPEATABLE READ.
    /// READ COMMITTED and autocommit allocate one per statement instead.
    pub(crate) timestamp_read: Option<crate::timestamp_txn::ReadTimestamp>,
    /// Holds the timestamp-domain reclaim floor at or below
    /// [`TxnCtx::timestamp_read`] while the block is open, so write-path
    /// timestamp pruning on this engine never reclaims a sharded-table
    /// version the fixed REPEATABLE READ timestamp can still see.
    pub(crate) _timestamp_read_pin: Option<crabka_pgmvcc::gc::SnapshotPin>,
    /// The `(table_id, rowid)` set this transaction's local xid has written, in
    /// write order (deduped is unnecessary — the abort-atomicity fence only scans
    /// these rows' versions, and a repeated entry just re-scans). Used by the
    /// cross-range re-stage fence (`effective_global_xid`): when a participant
    /// re-stage lands on a row that already carries an in-doubt `Prepared(-> g_old)`
    /// marker (a prior attempt staged it then its leader died), the `Prepared`
    /// marker this write/`join_global` stamps must ADOPT `g_old` rather than mint a
    /// SECOND version under a fresh `g'` that could commit independently — so each
    /// cross-range row resolves under EXACTLY ONE global decision (abort atomicity).
    pub(crate) written_rows: Vec<(u32, u64)>,
    /// SP37: the transaction-start instant (captured from the session clock at
    /// BEGIN). `now()`/`current_timestamp` are PG transaction-stable, so every
    /// statement in this block evaluates them against this single instant.
    pub(crate) txn_now: jiff::Timestamp,
    /// Held (SHARED) by explicit transactions that have written local tables,
    /// until COMMIT/ROLLBACK. Never blocks other DML — it lets unique-index
    /// DDL (which takes the same lock exclusively) wait out this transaction's
    /// writes before backfilling. Same-key unique conflicts serialize through
    /// per-key locks in the `RowLockManager` instead.
    pub(crate) unique_index_guard: Option<UniqueIndexGuard>,
    /// Held after the first ordinary write until COMMIT/ROLLBACK so conversion
    /// cannot rewrite an in-progress xid version out from under its commit.
    pub(crate) table_write_guard: Option<TableWriteGuard>,
    /// Remains held even when DDL releases `table_write_guard` before it waits
    /// for the catalog lock.
    pub(crate) writer_fence_guard: Option<crate::WriterFenceGuard>,
    /// Whether a query, DML, or DDL statement has established transaction semantics.
    /// PostgreSQL rejects SET TRANSACTION after this point.
    pub(crate) activity_started: bool,
}

/// Shared physical gate held while a transaction issues ordinary writes.
///
/// Its independent writer-fence lease remains live until the transaction's
/// terminal outcome, including while DDL releases this gate before catalog wait.
pub(crate) enum TableWriteGuard {
    Shared { _guard: OwnedRwLockReadGuard<()> },
}

/// A DML statement's SHARED hold on the engine's `unique_index_lock`. Unique
/// CREATE INDEX backfill (and CREATE TABLE with a unique constraint) takes the
/// same lock exclusively, so it waits for in-flight writers and blocks new
/// ones while it scans.
pub(crate) struct UniqueIndexGuard {
    _guard: OwnedRwLockReadGuard<()>,
}

/// Per-connection transaction state. `Failed` carries the aborted block's
/// context so its xid (and any row locks it holds) stay held until
/// COMMIT/ROLLBACK, which records the abort in the clog and releases them.
enum TxnState {
    Idle,
    InTransaction(TxnCtx),
    Prepared(TxnCtx),
    Failed(TxnCtx),
}

#[derive(Debug, Clone)]
struct GucSlot {
    source: GucValue,
    committed: GucValue,
    txn_current: Option<GucValue>,
    txn_session: Option<GucValue>,
}

impl GucSlot {
    fn new(source: GucValue) -> Self {
        Self {
            committed: source.clone(),
            source,
            txn_current: None,
            txn_session: None,
        }
    }

    fn effective(&self) -> &GucValue {
        self.txn_current.as_ref().unwrap_or(&self.committed)
    }

    fn set(&mut self, value: GucValue, local: bool) {
        self.txn_current = Some(value.clone());
        if !local {
            self.txn_session = Some(value);
        }
    }

    fn commit(&mut self) {
        if let Some(value) = self.txn_session.take() {
            self.committed = value;
        }
        self.txn_current = None;
    }

    fn rollback(&mut self) {
        self.txn_session = None;
        self.txn_current = None;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum GucValue {
    Bool(bool),
    Integer(i64),
    DurationMillis(u64),
    DateStyle(DateStyle),
    IntervalStyle(IntervalStyle),
    Text(String),
}

impl GucValue {
    fn render(&self) -> String {
        match self {
            Self::Bool(true) => "on".into(),
            Self::Bool(false) => "off".into(),
            Self::Integer(value) => value.to_string(),
            Self::DurationMillis(0) => "0".into(),
            Self::DurationMillis(value) if value % 86_400_000 == 0 => {
                format!("{}d", value / 86_400_000)
            }
            Self::DurationMillis(value) if value % 3_600_000 == 0 => {
                format!("{}h", value / 3_600_000)
            }
            Self::DurationMillis(value) if value % 60_000 == 0 => format!("{}min", value / 60_000),
            Self::DurationMillis(value) if value % 1_000 == 0 => format!("{}s", value / 1_000),
            Self::DurationMillis(value) => format!("{value}ms"),
            Self::DateStyle(value) => value.render(),
            Self::IntervalStyle(value) => value.render().into(),
            Self::Text(value) => value.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DateOutputStyle {
    Iso,
    Sql,
    Postgres,
    German,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DateOrder {
    Mdy,
    Dmy,
    Ymd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DateStyle {
    output: DateOutputStyle,
    order: DateOrder,
}

impl DateStyle {
    fn render(self) -> String {
        let output = match self.output {
            DateOutputStyle::Iso => "ISO",
            DateOutputStyle::Sql => "SQL",
            DateOutputStyle::Postgres => "Postgres",
            DateOutputStyle::German => "German",
        };
        let order = match self.order {
            DateOrder::Mdy => "MDY",
            DateOrder::Dmy => "DMY",
            DateOrder::Ymd => "YMD",
        };
        format!("{output}, {order}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IntervalStyle {
    Postgres,
    PostgresVerbose,
    SqlStandard,
    Iso8601,
}

impl IntervalStyle {
    fn render(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::PostgresVerbose => "postgres_verbose",
            Self::SqlStandard => "sql_standard",
            Self::Iso8601 => "iso_8601",
        }
    }
}

struct GucDefinition {
    name: &'static str,
    aliases: &'static [&'static str],
    vartype: &'static str,
    boot_default: &'static str,
    parse: fn(&str, Option<&GucValue>) -> Result<GucValue, ExecError>,
}

static GUC_DEFINITIONS: &[GucDefinition] = &[
    GucDefinition {
        name: "application_name",
        aliases: &[],
        vartype: "string",
        boot_default: "",
        parse: |value, _| Ok(GucValue::Text(value.to_string())),
    },
    GucDefinition {
        name: "client_encoding",
        aliases: &[],
        vartype: "string",
        boot_default: "UTF8",
        parse: parse_client_encoding,
    },
    GucDefinition {
        name: "datestyle",
        aliases: &["DateStyle"],
        vartype: "string",
        boot_default: "ISO, MDY",
        parse: parse_date_style,
    },
    GucDefinition {
        name: "extra_float_digits",
        aliases: &[],
        vartype: "integer",
        boot_default: "1",
        parse: parse_extra_float_digits,
    },
    GucDefinition {
        name: "intervalstyle",
        aliases: &["IntervalStyle"],
        vartype: "string",
        boot_default: "postgres",
        parse: parse_interval_style,
    },
    GucDefinition {
        name: "search_path",
        aliases: &[],
        vartype: "string",
        boot_default: "\"$user\", public",
        parse: |value, _| Ok(GucValue::Text(value.to_string())),
    },
    GucDefinition {
        name: "standard_conforming_strings",
        aliases: &[],
        vartype: "bool",
        boot_default: "on",
        parse: parse_bool,
    },
    GucDefinition {
        name: "statement_timeout",
        aliases: &[],
        vartype: "integer",
        boot_default: "0",
        parse: parse_statement_timeout,
    },
    GucDefinition {
        name: "timezone",
        aliases: &["TimeZone", "time zone"],
        vartype: "string",
        boot_default: "UTC",
        parse: parse_timezone,
    },
];

fn guc_definition(name: &str) -> Option<&'static GucDefinition> {
    GUC_DEFINITIONS.iter().find(|definition| {
        definition.name.eq_ignore_ascii_case(name)
            || definition
                .aliases
                .iter()
                .any(|alias| alias.eq_ignore_ascii_case(name))
    })
}

/// F-1: transactional session configuration registry for the practical GUC subset
/// clients send in startup/preamble traffic.
#[derive(Debug, Clone)]
pub(crate) struct GucState {
    slots: BTreeMap<String, GucSlot>,
}

/// One effective session parameter row exposed through `pg_catalog.pg_settings`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GucSettingRow {
    pub(crate) name: String,
    pub(crate) value: String,
    pub(crate) vartype: String,
    pub(crate) boot_val: String,
    pub(crate) reset_val: String,
}

impl Default for GucState {
    fn default() -> Self {
        Self::with_source_values(BTreeMap::new()).expect("compiled GUC defaults are valid")
    }
}

impl GucState {
    fn with_source_values(source_values: BTreeMap<String, String>) -> Result<Self, ExecError> {
        for name in source_values.keys() {
            if guc_definition(name).is_none() {
                return Err(ExecError::UnrecognizedParameter(name.clone()));
            }
        }
        let mut slots = BTreeMap::new();
        for definition in GUC_DEFINITIONS {
            let source = source_values
                .iter()
                .find(|(name, _)| {
                    guc_definition(name).is_some_and(|found| std::ptr::eq(found, definition))
                })
                .map_or(definition.boot_default, |(_, value)| value.as_str());
            slots.insert(
                definition.name.into(),
                GucSlot::new(parse_guc_value(definition, source, None)?),
            );
        }
        Ok(Self { slots })
    }

    pub(crate) fn effective(&self, name: &str) -> Result<String, ExecError> {
        let key = normalize_guc_name(name);
        self.slots
            .get(&key)
            .map(GucSlot::effective)
            .map(GucValue::render)
            .ok_or_else(|| ExecError::UnrecognizedParameter(name.to_string()))
    }

    fn effective_map(&self) -> BTreeMap<String, String> {
        self.slots
            .iter()
            .map(|(name, slot)| (name.clone(), slot.effective().render()))
            .collect()
    }

    pub(crate) fn set(&mut self, name: &str, value: &str, local: bool) -> Result<(), ExecError> {
        let key = normalize_guc_name(name);
        let definition = guc_definition(&key)
            .ok_or_else(|| ExecError::UnrecognizedParameter(name.to_string()))?;
        let current = self
            .slots
            .get(&key)
            .ok_or_else(|| ExecError::UnrecognizedParameter(name.to_string()))?
            .effective()
            .clone();
        let value = parse_guc_value(definition, value, Some(&current))?;
        let slot = self
            .slots
            .get_mut(&key)
            .ok_or_else(|| ExecError::UnrecognizedParameter(name.to_string()))?;
        slot.set(value, local);
        Ok(())
    }

    fn set_default(&mut self, name: &str, local: bool) -> Result<(), ExecError> {
        let key = normalize_guc_name(name);
        let slot = self
            .slots
            .get_mut(&key)
            .ok_or_else(|| ExecError::UnrecognizedParameter(name.to_string()))?;
        slot.set(slot.source.clone(), local);
        Ok(())
    }

    pub(crate) fn reset(&mut self, name: &str) -> Result<(), ExecError> {
        let key = normalize_guc_name(name);
        let slot = self
            .slots
            .get_mut(&key)
            .ok_or_else(|| ExecError::UnrecognizedParameter(name.to_string()))?;
        slot.set(slot.source.clone(), false);
        Ok(())
    }

    pub(crate) fn reset_all(&mut self) {
        for slot in self.slots.values_mut() {
            slot.set(slot.source.clone(), false);
        }
    }

    pub(crate) fn commit(&mut self) {
        for slot in self.slots.values_mut() {
            slot.commit();
        }
    }

    pub(crate) fn rollback(&mut self) {
        for slot in self.slots.values_mut() {
            slot.rollback();
        }
    }

    fn discard_all(&mut self) {
        for slot in self.slots.values_mut() {
            slot.committed.clone_from(&slot.source);
            slot.txn_current = None;
            slot.txn_session = None;
        }
    }

    fn settings(&self) -> Vec<GucSettingRow> {
        self.slots
            .iter()
            .map(|(name, slot)| {
                let definition = guc_definition(name).expect("registered GUC slot");
                GucSettingRow {
                    name: name.clone(),
                    value: slot.effective().render(),
                    vartype: definition.vartype.into(),
                    boot_val: definition.boot_default.into(),
                    reset_val: slot.source.render(),
                }
            })
            .collect()
    }
}

#[cfg(test)]
fn guc_default(name: &str) -> &'static str {
    guc_definition(name).map_or("", |definition| definition.boot_default)
}

pub(crate) fn guc_settings_runtime() -> Result<Vec<GucSettingRow>, ExecError> {
    GUC_RUNTIME.with(|cell| {
        let runtime = cell.borrow();
        let values = runtime
            .as_ref()
            .ok_or_else(|| ExecError::UnrecognizedParameter("pg_settings".into()))?;
        Ok(values.settings.clone())
    })
}

#[cfg(test)]
fn guc_vartype(name: &str) -> &'static str {
    guc_definition(name).map_or("string", |definition| definition.vartype)
}

fn normalize_guc_name(name: &str) -> String {
    guc_definition(name).map_or_else(
        || name.to_ascii_lowercase(),
        |definition| definition.name.into(),
    )
}

fn parse_guc_value(
    definition: &GucDefinition,
    value: &str,
    current: Option<&GucValue>,
) -> Result<GucValue, ExecError> {
    (definition.parse)(value, current)
}

#[cfg(test)]
fn canonical_guc_value(name: &str, value: &str) -> Result<String, ExecError> {
    let definition =
        guc_definition(name).ok_or_else(|| ExecError::UnrecognizedParameter(name.to_string()))?;
    parse_guc_value(definition, value, None).map(|parsed| parsed.render())
}

fn parse_timezone(value: &str, _: Option<&GucValue>) -> Result<GucValue, ExecError> {
    if value.eq_ignore_ascii_case("UTC") || jiff::tz::TimeZone::get(value).is_ok() {
        Ok(GucValue::Text(value.to_string()))
    } else {
        Err(ExecError::InvalidParameterValue(value.to_string()))
    }
}

fn parse_client_encoding(value: &str, _: Option<&GucValue>) -> Result<GucValue, ExecError> {
    if matches!(
        value.to_ascii_uppercase().as_str(),
        "UTF8" | "UTF-8" | "UNICODE"
    ) {
        Ok(GucValue::Text("UTF8".into()))
    } else {
        Err(ExecError::InvalidParameterValue(value.to_string()))
    }
}

fn parse_bool(value: &str, _: Option<&GucValue>) -> Result<GucValue, ExecError> {
    match value.to_ascii_lowercase().as_str() {
        "on" | "true" | "yes" | "1" => Ok(GucValue::Bool(true)),
        "off" | "false" | "no" | "0" => Ok(GucValue::Bool(false)),
        _ => Err(ExecError::InvalidParameterValue(value.to_string())),
    }
}

fn parse_extra_float_digits(value: &str, _: Option<&GucValue>) -> Result<GucValue, ExecError> {
    canonical_integer_guc(value, -15, 3)?
        .parse()
        .map(GucValue::Integer)
        .map_err(|_| ExecError::InvalidParameterValue(value.into()))
}

fn parse_date_style(value: &str, current: Option<&GucValue>) -> Result<GucValue, ExecError> {
    let mut output = None;
    let mut order = None;
    for part in value
        .split([',', ' '])
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        let lower = part.to_ascii_lowercase();
        match lower.as_str() {
            "iso" => output = Some(DateOutputStyle::Iso),
            "sql" => output = Some(DateOutputStyle::Sql),
            "postgres" | "postgresql" => output = Some(DateOutputStyle::Postgres),
            "german" => output = Some(DateOutputStyle::German),
            "mdy" | "us" | "noneuro" | "noneuropean" => order = Some(DateOrder::Mdy),
            "dmy" | "euro" | "european" => order = Some(DateOrder::Dmy),
            "ymd" => order = Some(DateOrder::Ymd),
            _ => return Err(ExecError::InvalidParameterValue(value.to_string())),
        }
    }
    let current = match current {
        Some(GucValue::DateStyle(current)) => *current,
        _ => DateStyle {
            output: DateOutputStyle::Iso,
            order: DateOrder::Mdy,
        },
    };
    Ok(GucValue::DateStyle(DateStyle {
        output: output.unwrap_or(current.output),
        order: order.unwrap_or(current.order),
    }))
}

fn parse_interval_style(value: &str, _: Option<&GucValue>) -> Result<GucValue, ExecError> {
    let style = match value.to_ascii_lowercase().as_str() {
        "postgres" => IntervalStyle::Postgres,
        "postgres_verbose" => IntervalStyle::PostgresVerbose,
        "sql_standard" => IntervalStyle::SqlStandard,
        "iso_8601" => IntervalStyle::Iso8601,
        _ => return Err(ExecError::InvalidParameterValue(value.to_string())),
    };
    Ok(GucValue::IntervalStyle(style))
}

fn parse_statement_timeout(value: &str, _: Option<&GucValue>) -> Result<GucValue, ExecError> {
    let trimmed = value.trim();
    let split = trimmed
        .find(|character: char| !(character.is_ascii_digit() || character == '.'))
        .unwrap_or(trimmed.len());
    let (number, unit) = trimmed.split_at(split);
    let number = number
        .parse::<f64>()
        .map_err(|_| ExecError::InvalidParameterValue(value.to_string()))?;
    let multiplier = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "ms" => 1.0,
        "s" => 1_000.0,
        "min" => 60_000.0,
        "h" => 3_600_000.0,
        "d" => 86_400_000.0,
        _ => return Err(ExecError::InvalidParameterValue(value.to_string())),
    };
    let millis = number * multiplier;
    if !millis.is_finite() || millis < 0.0 || millis > f64::from(i32::MAX) {
        return Err(ExecError::InvalidParameterValue(value.to_string()));
    }
    let millis = format!("{millis:.0}")
        .parse()
        .map_err(|_| ExecError::InvalidParameterValue(value.to_string()))?;
    Ok(GucValue::DurationMillis(millis))
}

/// Parse PostgreSQL integer-GUC input, including rounded decimal input and the
/// `0x`/`0o` forms accepted by PostgreSQL's configuration parser.
fn canonical_integer_guc(value: &str, min: i64, max: i64) -> Result<String, ExecError> {
    let trimmed = value.trim();
    let (negative, unsigned) = match trimmed.as_bytes().first() {
        Some(b'-') => (true, &trimmed[1..]),
        Some(b'+') => (false, &trimmed[1..]),
        _ => (false, trimmed),
    };
    let radix_value = unsigned
        .strip_prefix("0x")
        .or_else(|| unsigned.strip_prefix("0X"))
        .map(|digits| (16, digits))
        .or_else(|| {
            unsigned
                .strip_prefix("0o")
                .or_else(|| unsigned.strip_prefix("0O"))
                .map(|digits| (8, digits))
        });
    let parsed = if let Some((radix, digits)) = radix_value {
        i64::from_str_radix(digits, radix).ok().and_then(|number| {
            if negative {
                number.checked_neg()
            } else {
                Some(number)
            }
        })
    } else {
        trimmed.parse::<f64>().ok().and_then(|number| {
            let rounded = number.round();
            rounded
                .is_finite()
                .then(|| format!("{rounded:.0}").parse::<i64>().ok())
                .flatten()
        })
    };
    parsed
        .filter(|number| (min..=max).contains(number))
        .map(|number| number.to_string())
        .ok_or_else(|| ExecError::InvalidParameterValue(value.to_string()))
}

#[derive(Debug, Clone)]
struct GucMutation {
    name: String,
    value: String,
    local: bool,
}

#[derive(Debug, Clone)]
struct GucRuntime {
    values: BTreeMap<String, String>,
    settings: Vec<GucSettingRow>,
    mutations: Vec<GucMutation>,
}

thread_local! {
    static GUC_RUNTIME: RefCell<Option<GucRuntime>> = const { RefCell::new(None) };
}

pub(crate) fn current_setting_runtime(
    name: &str,
    missing_ok: bool,
) -> Result<Option<String>, ExecError> {
    let key = normalize_guc_name(name);
    GUC_RUNTIME.with(|cell| {
        let runtime = cell.borrow();
        let Some(runtime) = runtime.as_ref() else {
            return Err(ExecError::UnrecognizedParameter(name.to_string()));
        };
        match runtime.values.get(&key) {
            Some(value) => Ok(Some(value.clone())),
            None if missing_ok => Ok(None),
            None => Err(ExecError::UnrecognizedParameter(name.to_string())),
        }
    })
}

pub(crate) fn set_config_runtime(
    name: &str,
    value: &str,
    local: bool,
) -> Result<String, ExecError> {
    let key = normalize_guc_name(name);
    GUC_RUNTIME.with(|cell| {
        let mut runtime = cell.borrow_mut();
        let Some(runtime) = runtime.as_mut() else {
            return Err(ExecError::UnrecognizedParameter(name.to_string()));
        };
        let current = runtime
            .values
            .get(&key)
            .ok_or_else(|| ExecError::UnrecognizedParameter(name.to_string()))?;
        let definition = guc_definition(&key)
            .ok_or_else(|| ExecError::UnrecognizedParameter(name.to_string()))?;
        let current = parse_guc_value(definition, current, None)?;
        let value = parse_guc_value(definition, value, Some(&current))?.render();
        runtime.values.insert(key.clone(), value.clone());
        runtime.mutations.push(GucMutation {
            name: key,
            value: value.clone(),
            local,
        });
        Ok(value)
    })
}

fn with_guc_runtime<T>(
    values: BTreeMap<String, String>,
    settings: Vec<GucSettingRow>,
    f: impl FnOnce() -> T,
) -> (T, Vec<GucMutation>) {
    GUC_RUNTIME.with(|cell| {
        let previous = cell.replace(Some(GucRuntime {
            values,
            settings,
            mutations: Vec::new(),
        }));
        let result = f();
        let runtime = cell.replace(previous).expect("runtime installed");
        (result, runtime.mutations)
    })
}

/// PostgreSQL allows SET TRANSACTION after session/transaction controls, but
/// not after a successful data query, DML statement, or DDL statement. Keep the
/// classification exhaustive so a newly supported statement cannot silently
/// bypass the transaction-activity rule.
fn establishes_transaction_activity(stmt: &Statement) -> bool {
    match stmt {
        Statement::Query(_)
        | Statement::Insert { .. }
        | Statement::Update { .. }
        | Statement::Delete { .. }
        | Statement::Truncate { .. }
        | Statement::CreateTable { .. }
        | Statement::CreateIndex { .. }
        | Statement::DropIndex { .. }
        | Statement::DropTable { .. }
        | Statement::AlterTableRename { .. }
        | Statement::AlterTableAddPrimaryKey { .. }
        | Statement::CreateView { .. }
        | Statement::DropView { .. }
        | Statement::CreateFdw { .. }
        | Statement::DropFdw { .. }
        | Statement::CreateServer { .. }
        | Statement::AlterServer { .. }
        | Statement::DropServer { .. }
        | Statement::CreateUserMapping { .. }
        | Statement::AlterUserMapping { .. }
        | Statement::DropUserMapping { .. }
        | Statement::CreateForeignTable { .. }
        | Statement::DropForeignTable { .. }
        | Statement::CreateRole { .. }
        | Statement::DropRole { .. }
        | Statement::GrantTablePrivileges { .. }
        | Statement::RevokeTablePrivileges { .. }
        | Statement::ImportForeignSchema { .. } => true,
        Statement::Begin { .. }
        | Statement::Commit
        | Statement::Rollback
        | Statement::Set { .. }
        | Statement::Show { .. }
        | Statement::Reset { .. }
        | Statement::SetRole { .. }
        // VACUUM is refused inside a transaction block, so it never marks one
        // active.
        | Statement::Vacuum
        | Statement::CompatibilityRefusal(_) => false,
    }
}

/// Reconstruct the global visibility snapshot from range 0's DURABLE state (never
/// an in-memory running set — correction C2). xmax = next_global_xid; xip = [] (a
/// g < xmax is resolved by reading range 0's global clog directly). Caller must
/// have barriered range 0's replica current first.
pub(crate) fn durable_global_snapshot(range0: &dyn Kv) -> Result<Snapshot, ExecError> {
    use crabka_pgmvcc::xid::GLOBAL_XID_BASE;
    Ok(Snapshot {
        xmin: GLOBAL_XID_BASE,
        xmax: crate::gtm::read_next_global(range0)?,
        xip: vec![],
    })
}

/// One connection's view of the engine. Holds shared handles to the KV store,
/// the ProcArray, the SequenceManager, the RowLockManager, and the DDL catalog
/// lock, plus this connection's transaction state. Not shared between
/// connections.
pub struct SqlSession {
    pub(crate) kv: Arc<dyn Kv>,
    /// The store catalog (schema) lookups resolve through. Same as `kv` for the
    /// single-range engine; range 0's store for a multi-range data node.
    catalog_kv: Arc<dyn Kv>,
    procarray: Arc<ProcArray>,
    seq: Arc<SequenceManager>,
    lockmgr: Arc<RowLockManager>,
    catalog_lock: Arc<tokio::sync::Mutex<()>>,
    table_write_gate: Arc<tokio::sync::RwLock<()>>,
    writer_fence: Arc<crate::WriterFence>,
    /// Retains the registry entry while this session can hold a writer lease.
    _coordination: Arc<crate::EngineCoordination>,
    unique_index_lock: Arc<tokio::sync::RwLock<()>>,
    committer: Arc<dyn crate::commit::Committer>,
    linearizer: Arc<dyn crate::read_gate::Linearizer>,
    persist_mode: crate::PersistMode,
    /// Range 0's GTM, shared from the engine. `Some` on every range engine of a
    /// multi-range cluster (so any range can capture a global snapshot and
    /// resolve a `Prepared` row); `None` on a single-range engine.
    gtm: Option<Arc<crate::gtm::Gtm>>,
    /// A range-0 read barrier (data-range engines only). Before any read that
    /// consults range 0's global clog (the cross-range resolver), this catches the
    /// node's LOCAL range-0 replica up to range 0's linearizable applied index, so a
    /// `Committed(g)` is actually present when `global_status` reads it. `None` on
    /// range 0's own engine (it reads its own current store) and on a single-range
    /// engine.
    range0_barrier: Option<Arc<dyn crate::read_gate::Linearizer>>,
    /// Set when this session is enlisted as a participant in a cross-range global
    /// txn `g` (Task 4's coordinator calls `join_global`). While set, each local
    /// write also stamps a `Prepared(local_xid -> g)` clog marker and deregisters
    /// the local xid at prepare time. `None` for ordinary single-range txns.
    global_xid: Option<u64>,
    /// Bound on every lock wait this session performs (`None` waits
    /// indefinitely under the engine-local deadlock detector). Set by owners
    /// that can enlist this session in a cross-range transaction — a gateway
    /// escalating an explicit transaction past one range, or a range service
    /// hosting the session for a remote gateway — because a deadlock cycle
    /// spanning engines is invisible to any one engine's wait-for graph and
    /// only a capped wait resolves it.
    lock_wait_cap: Option<std::time::Duration>,
    /// SP37: the injectable clock (shared from the engine). Backs the per-statement
    /// `EvalCtx`'s `now`/`stmt_now` and `clock_timestamp()`. `SystemClock` in
    /// production; a `FixedClock` in tests for deterministic temporal evaluation.
    clock: Arc<dyn crate::clock::Clock>,
    /// SP37: the transactional `timezone` GUC. `effective()` feeds the per-statement
    /// `EvalCtx`'s `time_zone`; `SET`/`SHOW`/`RESET timezone` mutate/read it, and
    /// COMMIT/ROLLBACK promote/revert it in lockstep with the transaction outcome.
    guc: GucState,
    /// SP40: the foreign-table scanner (shared from the engine). `Some` when the
    /// binary registered a `kafka_fdw` via `SqlEngine::set_foreign_scanner`; a
    /// `SELECT` from a foreign table with this `None` returns `0A000`.
    foreign_scanner: Option<Arc<dyn crate::foreign::ForeignScanner>>,
    /// G-8 table scan seam. Defaults to local MVCC scanning; multi-range tenants
    /// can inject a scatter-gather scanner.
    range_scanner: Arc<dyn crate::scanner::RangeScanner>,
    join_stats: Arc<dyn crate::plan_dist::Stats>,
    join_strategy_config: crate::plan_dist::PlannerConfig,
    /// Timestamp oracle for sharded timestamp transactions.
    timestamp_oracle: Arc<dyn crate::timestamp_txn::TimestampSource>,
    /// Cached durable-timestamp horizon (shared from the engine): the floor a
    /// statement's read/transaction/commit timestamps must exceed, without
    /// rescanning the store per statement.
    timestamp_horizon: crate::timestamp_txn::TimestampHorizonSource,
    /// This range's local sequence (shared from the engine). A statement read
    /// timestamp falls back to it when the global source cannot grant above the
    /// durable horizon a single-shard bypass commit lifted; see
    /// [`SqlSession::allocate_statement_read_timestamp`].
    local_sequence: Arc<crate::local_sequence::LocalSequence>,
    /// Garbage-horizon pins + decided floor (shared from the engine). Every
    /// statement/transaction pins its snapshot xmin here so version pruning
    /// (write-path, `vacuum`, checkpoint compaction) never reclaims a version
    /// a live snapshot still sees.
    gc_horizon: Arc<crabka_pgmvcc::gc::GcHorizon>,
    /// Timestamp-version GC state (shared from the engine): statement read
    /// timestamps over sharded tables pin here so write-path timestamp
    /// pruning never reclaims a version an in-flight read may resolve to,
    /// and commit resolutions fold their opportunistic prune ops through it.
    ts_gc: Arc<crate::ts_gc::TsVersionGc>,
    timestamp_own_start_ts: Option<crate::timestamp_txn::TimestampTransactionId>,
    sequence_currvals: Arc<Mutex<HashMap<String, i64>>>,
    session_user: String,
    current_role: String,
    state: TxnState,
    prepared: HashMap<String, SqlPrepared>,
    portals: HashMap<String, SqlPortal>,
}

pub(crate) struct SqlSessionConfig {
    pub kv: Arc<dyn Kv>,
    pub catalog_kv: Arc<dyn Kv>,
    pub procarray: Arc<ProcArray>,
    pub seq: Arc<SequenceManager>,
    pub lockmgr: Arc<RowLockManager>,
    pub catalog_lock: Arc<tokio::sync::Mutex<()>>,
    pub table_write_gate: Arc<tokio::sync::RwLock<()>>,
    pub writer_fence: Arc<crate::WriterFence>,
    pub coordination: Arc<crate::EngineCoordination>,
    pub unique_index_lock: Arc<tokio::sync::RwLock<()>>,
    pub committer: Arc<dyn crate::commit::Committer>,
    pub linearizer: Arc<dyn crate::read_gate::Linearizer>,
    pub persist_mode: crate::PersistMode,
    pub gtm: Option<Arc<crate::gtm::Gtm>>,
    pub range0_barrier: Option<Arc<dyn crate::read_gate::Linearizer>>,
    pub clock: Arc<dyn crate::clock::Clock>,
    pub foreign_scanner: Option<Arc<dyn crate::foreign::ForeignScanner>>,
    pub range_scanner: Arc<dyn crate::scanner::RangeScanner>,
    pub join_stats: Arc<dyn crate::plan_dist::Stats>,
    pub join_strategy_config: crate::plan_dist::PlannerConfig,
    pub timestamp_oracle: Arc<dyn crate::timestamp_txn::TimestampSource>,
    pub timestamp_horizon: crate::timestamp_txn::TimestampHorizonSource,
    pub local_sequence: Arc<crate::local_sequence::LocalSequence>,
    pub gc_horizon: Arc<crabka_pgmvcc::gc::GcHorizon>,
    pub ts_gc: Arc<crate::ts_gc::TsVersionGc>,
}

#[derive(Clone)]
struct SqlPrepared {
    statement: Option<Statement>,
    description: PreparedDescription,
}

struct SqlPortal {
    statement: Option<Statement>,
    description: PortalDescription,
    formats: Vec<i16>,
    execution: SqlPortalExecution,
}

enum SqlPortalExecution {
    NotStarted,
    Rows {
        rows: Vec<Vec<Option<crabka_pgwire::engine::Cell>>>,
        tag: String,
        position: usize,
    },
    Command {
        tag: String,
    },
    Empty,
}

impl SqlSession {
    pub(crate) fn new(config: SqlSessionConfig) -> Self {
        let SqlSessionConfig {
            kv,
            catalog_kv,
            procarray,
            seq,
            lockmgr,
            catalog_lock,
            table_write_gate,
            writer_fence,
            coordination,
            unique_index_lock,
            committer,
            linearizer,
            persist_mode,
            gtm,
            range0_barrier,
            clock,
            foreign_scanner,
            range_scanner,
            join_stats,
            join_strategy_config,
            timestamp_oracle,
            timestamp_horizon,
            local_sequence,
            gc_horizon,
            ts_gc,
        } = config;
        Self {
            kv,
            catalog_kv,
            procarray,
            seq,
            lockmgr,
            catalog_lock,
            table_write_gate,
            writer_fence,
            _coordination: coordination,
            unique_index_lock,
            committer,
            linearizer,
            persist_mode,
            gtm,
            range0_barrier,
            global_xid: None,
            lock_wait_cap: None,
            clock,
            guc: GucState::default(),
            foreign_scanner,
            range_scanner,
            join_stats,
            join_strategy_config,
            timestamp_oracle,
            timestamp_horizon,
            local_sequence,
            gc_horizon,
            ts_gc,
            timestamp_own_start_ts: None,
            sequence_currvals: Arc::new(Mutex::new(HashMap::new())),
            session_user: "public".into(),
            current_role: "public".into(),
            state: TxnState::Idle,
            prepared: HashMap::new(),
            portals: HashMap::new(),
        }
    }

    /// Build the per-statement evaluation context. `now` is the transaction-start
    /// instant (PG transaction-stable) inside a txn, else this statement's instant.
    fn eval_ctx(&self) -> crate::clock::EvalCtx {
        let stmt_now = self.clock.now();
        let now = match &self.state {
            TxnState::InTransaction(c) | TxnState::Prepared(c) | TxnState::Failed(c) => c.txn_now,
            TxnState::Idle => stmt_now,
        };
        // SP37: the effective session zone (validated at SET time, so `get`
        // succeeds; `unwrap_or(UTC)` is a defensive fallback). `UTC` is
        // special-cased to the const so the common case never touches the tzdb.
        let tzname = self
            .guc
            .effective("timezone")
            .unwrap_or_else(|_| "UTC".into());
        let time_zone = if tzname.eq_ignore_ascii_case("UTC") {
            jiff::tz::TimeZone::UTC
        } else {
            jiff::tz::TimeZone::get(&tzname).unwrap_or(jiff::tz::TimeZone::UTC)
        };
        crate::clock::EvalCtx {
            now,
            stmt_now,
            time_zone,
            current_user: self.current_role.clone(),
            session_user: self.session_user.clone(),
            clock: Arc::clone(&self.clock),
            sequence: Some(Arc::new(crate::clock::SequenceRuntime {
                kv: Arc::clone(&self.catalog_kv),
                manager: Arc::clone(&self.seq),
                currvals: Arc::clone(&self.sequence_currvals),
            })),
        }
    }

    fn write_context<'a>(
        &'a self,
        global_snapshot: &'a Snapshot,
        snapshot: &'a Snapshot,
        xid: u64,
        repeatable_read: bool,
        eval_ctx: &'a crate::clock::EvalCtx,
        prune_horizon: Option<u64>,
    ) -> crate::exec::WriteContext<'a> {
        crate::exec::WriteContext {
            catalog_kv: self.catalog_kv.as_ref(),
            kv: self.kv.as_ref(),
            global: self.catalog_kv.as_ref(),
            global_snapshot,
            procarray: self.procarray.as_ref(),
            lockmgr: self.lockmgr.as_ref(),
            seq: self.seq.as_ref(),
            snapshot,
            xid,
            repeatable_read,
            eval_ctx,
            prune_horizon,
            lock_wait_cap: self.lock_wait_cap,
        }
    }

    /// The garbage horizon UPDATE/DELETE may prune dead row versions at.
    ///
    /// Sound on every engine kind — unlike [`SqlEngine::vacuum`], whose local
    /// sweeps are why it stays confined to single-range local engines:
    ///
    /// - The prune deletes ride the statement's own commit batch through the
    ///   engine committer, so on replicated engines they replicate through
    ///   the WAL and replay deterministically on followers, in recovery, and
    ///   in committed folds — never a local delete outside batch ordering.
    /// - The horizon is capped by this engine's oldest running writer and
    ///   lowest registered snapshot pin, which covers every reader of this
    ///   store: plain-table reads are always served by owner-local sessions
    ///   (forwarded DML and queries open sessions here; cross-range scatter
    ///   scans exist only for sharded tables, whose timestamp tuples this
    ///   pruning never touches), and a snapshot allocated after a prune
    ///   commits sees the pruned version's committed deleter, so the version
    ///   was already invisible to it.
    /// - Global 2PC writes stay untouched: an undecided enlisted xid reads as
    ///   `Prepared` (never dead), and global xids sit numerically above every
    ///   local horizon.
    ///
    /// [`SqlEngine::vacuum`]: crate::SqlEngine::vacuum
    fn local_prune_horizon(&self) -> Result<u64, ExecError> {
        crate::checkpoint_garbage_horizon(
            self.procarray.as_ref(),
            self.kv.as_ref(),
            self.gc_horizon.as_ref(),
        )
    }

    /// Set the timestamp transaction whose pending intents are owned by this SQL session.
    pub fn set_timestamp_own_start_ts(
        &mut self,
        start_ts: Option<crate::timestamp_txn::TimestampTransactionId>,
    ) {
        self.timestamp_own_start_ts = start_ts;
    }

    /// Bound (or unbound, with `None`) every lock wait this session performs.
    ///
    /// Owners set a cap when this session can be enlisted in a cross-range
    /// transaction: a deadlock cycle spanning engines never appears in any one
    /// engine's wait-for graph, so an expired cap — surfaced as a 40P01 the
    /// client retries — is the only detector such a cycle has. Purely local
    /// waits should keep the default `None` and rely on the exact
    /// engine-local cycle check.
    pub fn set_lock_wait_cap(&mut self, cap: Option<std::time::Duration>) {
        self.lock_wait_cap = cap;
    }

    /// Apply a typed practical-subset GUC mutation and return the `SET` command
    /// tag. Names outside the registry are unrecognized (42704).
    ///
    /// Transactional application mirrors PostgreSQL: inside an open block a `SET`
    /// stages a session override and `SET LOCAL` stages a local override (promoted/
    /// reverted by COMMIT/ROLLBACK); in autocommit the change is its own
    /// transaction, so it is applied then immediately committed (a bare `SET LOCAL`
    /// in autocommit is therefore dropped, matching PG).
    fn set_guc(
        &mut self,
        local: bool,
        name: &str,
        value: &crabka_pgparser::ast::SetValue,
    ) -> Result<QueryResult, ExecError> {
        let zone = match value {
            crabka_pgparser::ast::SetValue::Default => String::new(),
            crabka_pgparser::ast::SetValue::Value(v) => v.clone(),
        };
        // Apply with the right transactional scope.
        let in_txn = matches!(self.state, TxnState::InTransaction(_));
        if in_txn {
            match value {
                crabka_pgparser::ast::SetValue::Default => self.guc.set_default(name, local)?,
                crabka_pgparser::ast::SetValue::Value(_) => self.guc.set(name, &zone, local)?,
            }
        } else {
            // Autocommit: this SET is its own transaction. A plain SET persists
            // (set_session + commit); a SET LOCAL is committed too, which drops it.
            match value {
                crabka_pgparser::ast::SetValue::Default => self.guc.set_default(name, local)?,
                crabka_pgparser::ast::SetValue::Value(_) => self.guc.set(name, &zone, local)?,
            }
            self.guc.commit();
        }
        Ok(QueryResult::Command { tag: "SET".into() })
    }

    /// Reset a registered parameter to its independent source value. Transactional
    /// like SET; names outside the registry are unrecognized (42704).
    fn reset_guc(&mut self, target: &ResetTarget) -> Result<QueryResult, ExecError> {
        match target {
            ResetTarget::Name(name) => self.guc.reset(name)?,
            ResetTarget::All => self.guc.reset_all(),
        }
        if matches!(self.state, TxnState::Idle) {
            self.guc.commit(); // autocommit: persist the reset immediately
        }
        Ok(QueryResult::Command {
            tag: "RESET".into(),
        })
    }

    fn set_role(&mut self, role: Option<&str>) -> Result<QueryResult, ExecError> {
        let next_role = role.unwrap_or(&self.session_user);
        if !crabka_pgcatalog::role_exists(&*self.catalog_kv, next_role)? {
            return Err(
                crabka_pgcatalog::CatalogError::UndefinedObject(next_role.to_string()).into(),
            );
        }
        self.current_role = next_role.to_string();
        Ok(QueryResult::Command { tag: "SET".into() })
    }

    /// Return a registered parameter's effective value as one text row. Names
    /// outside the registry are unrecognized (42704); SHOW does not mutate state.
    fn show_guc(&self, name: &str) -> Result<QueryResult, ExecError> {
        use bytes::Bytes;
        use crabka_pgwire::engine::Cell;
        if name.eq_ignore_ascii_case("all") {
            return Ok(self.show_all_gucs());
        }
        let shown_name = normalize_guc_name(name);
        let value = self.guc.effective(name)?.as_bytes().to_vec();
        let field = FieldDescription {
            name: shown_name,
            table_oid: 0,
            column_id: 0,
            type_oid: crabka_pgtypes::ColumnType::Text.oid(),
            type_size: crabka_pgtypes::ColumnType::Text.type_size(),
            type_modifier: -1,
            format: 0,
        };
        Ok(QueryResult::Rows {
            fields: vec![field],
            rows: vec![vec![Some(Cell {
                text: Bytes::from(value.clone()),
                binary: Bytes::from(value),
            })]],
            tag: "SHOW".into(),
        })
    }

    fn show_all_gucs(&self) -> QueryResult {
        use bytes::Bytes;
        use crabka_pgwire::engine::Cell;
        let fields = ["name", "setting", "description"]
            .into_iter()
            .map(|name| FieldDescription {
                name: name.into(),
                table_oid: 0,
                column_id: 0,
                type_oid: ColumnType::Text.oid(),
                type_size: ColumnType::Text.type_size(),
                type_modifier: -1,
                format: 0,
            })
            .collect();
        let rows = self
            .guc
            .effective_map()
            .into_iter()
            .map(|(name, setting)| {
                [name, setting, String::new()]
                    .into_iter()
                    .map(|value| {
                        Some(Cell {
                            text: Bytes::from(value.clone()),
                            binary: Bytes::from(value),
                        })
                    })
                    .collect()
            })
            .collect();
        QueryResult::Rows {
            fields,
            rows,
            tag: "SHOW".into(),
        }
    }

    async fn set_transaction(
        &mut self,
        value: &crabka_pgparser::ast::SetValue,
    ) -> Result<QueryResult, ExecError> {
        let isolation = match value {
            crabka_pgparser::ast::SetValue::Value(v) if v == "repeatable read" => {
                Some(IsolationLevel::RepeatableRead)
            }
            crabka_pgparser::ast::SetValue::Value(v) if v == "read committed" => {
                Some(IsolationLevel::ReadCommitted)
            }
            _ => None,
        };
        let Some(level) = isolation else {
            return Ok(QueryResult::Command { tag: "SET".into() });
        };
        if matches!(
            &self.state,
            TxnState::InTransaction(TxnCtx {
                activity_started: true,
                ..
            })
        ) {
            return Err(ExecError::ActiveSqlTransaction(
                "SET TRANSACTION ISOLATION LEVEL must be called before any query".into(),
            ));
        }
        if matches!(level, IsolationLevel::RepeatableRead) {
            self.linearizer.ensure_readable().await?;
            self.ensure_global_readable().await?;
            let snapshot = self.procarray.snapshot();
            let global_snapshot = self.global_read_snapshot(None)?;
            let timestamp_read = self.allocate_statement_read_timestamp().await?;
            let timestamp_read_pin = self.ts_gc.pin_read(self.kv.as_ref(), timestamp_read)?;
            if let TxnState::InTransaction(ctx) = &mut self.state {
                ctx.repeatable_read = true;
                ctx.snapshot = snapshot;
                ctx.global_snapshot = Some(global_snapshot);
                ctx.timestamp_read = Some(timestamp_read);
                ctx._timestamp_read_pin = Some(timestamp_read_pin);
            }
        } else if let TxnState::InTransaction(ctx) = &mut self.state {
            ctx.repeatable_read = false;
            ctx.global_snapshot = None;
            ctx.timestamp_read = None;
            ctx._timestamp_read_pin = None;
        }
        Ok(QueryResult::Command { tag: "SET".into() })
    }

    fn discard_all(&mut self) -> Result<QueryResult, ExecError> {
        if !matches!(self.state, TxnState::Idle) {
            return Err(ExecError::ActiveSqlTransaction(
                "DISCARD ALL cannot run inside a transaction block".into(),
            ));
        }
        self.guc.discard_all();
        self.current_role.clone_from(&self.session_user);
        self.prepared.clear();
        self.portals.clear();
        Ok(QueryResult::Command {
            tag: "DISCARD ALL".into(),
        })
    }

    fn apply_guc_mutations(&mut self, mutations: Vec<GucMutation>) -> Result<(), ExecError> {
        let in_txn = matches!(self.state, TxnState::InTransaction(_));
        for mutation in mutations {
            self.guc
                .set(&mutation.name, &mutation.value, mutation.local)?;
            if !in_txn {
                self.guc.commit();
            }
        }
        Ok(())
    }

    /// Catch range 0's LOCAL replica up to its leader's linearizable applied index
    /// (data-range engines only; a no-op on range 0's own engine and single-range
    /// engines). Run AFTER the own-range `linearizer.ensure_readable()` and BEFORE
    /// any read that consults range 0's global clog (the cross-range resolver).
    async fn ensure_global_readable(&self) -> Result<(), ExecError> {
        if let Some(b) = &self.range0_barrier {
            b.ensure_readable().await?;
        }
        Ok(())
    }

    /// Execute one already-parsed statement (the router parses once, then routes).
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn run(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        self.run_one(stmt).await
    }

    fn mark_transaction_failed(&mut self) {
        match std::mem::replace(&mut self.state, TxnState::Idle) {
            TxnState::InTransaction(ctx) => self.state = TxnState::Failed(ctx),
            state => self.state = state,
        }
    }

    fn bind_extended_statement_params(
        &mut self,
        stmt: &mut Statement,
        params: &[BoundParam],
    ) -> Result<(), PgError> {
        let timezone_name = self.guc.effective("timezone").map_err(ExecError::into_pg)?;
        let time_zone = if timezone_name.eq_ignore_ascii_case("UTC") {
            jiff::tz::TimeZone::UTC
        } else {
            jiff::tz::TimeZone::get(&timezone_name).map_err(|_| {
                PgError::error(
                    "22023",
                    format!("invalid value for parameter: \"{timezone_name}\""),
                )
            })?
        };
        let bind_result = ParamBinder {
            catalog_kv: &*self.catalog_kv,
            params,
            time_zone: &time_zone,
            inferred_param_types: RefCell::new(vec![None; params.len()]),
        }
        .bind_statement_params(stmt);
        if bind_result.is_err() {
            self.mark_transaction_failed();
        }
        bind_result
    }

    fn reject_prepared_participant(&self) -> Result<(), ExecError> {
        let TxnState::Prepared(_) = self.state else {
            return Ok(());
        };
        let global_xid = self.global_xid.ok_or_else(|| {
            ExecError::ObjectNotInPrerequisiteState(
                "prepared global participant is missing its global xid".into(),
            )
        })?;
        Err(ExecError::ObjectNotInPrerequisiteState(format!(
            "global participant xid {global_xid} is externally prepared; release it through the global participant API"
        )))
    }

    async fn run_one(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        // Pin the garbage horizon for this autocommit statement's duration.
        // The pin (the ProcArray xmin now) is <= the xmin of any snapshot the
        // statement takes below, so a concurrent pruner/vacuum can never
        // reclaim a version this statement's snapshot(s) can still see —
        // including between the multiple eager KV scans of a join, subquery,
        // or index probe. Inside a block the TxnCtx pin (taken at BEGIN)
        // already covers every statement snapshot.
        let _statement_pin = matches!(self.state, TxnState::Idle)
            .then(|| self.gc_horizon.pin(self.procarray.snapshot().xmin));
        self.reject_prepared_participant()?;
        if matches!(self.state, TxnState::Failed(_))
            && !matches!(stmt, Statement::Commit | Statement::Rollback)
        {
            return Err(ExecError::InFailedTransaction);
        }
        let result = match stmt {
            Statement::CompatibilityRefusal(command) => {
                Err(ExecError::CompatibilityRefusal(*command))
            }
            Statement::Begin { isolation } => self.begin(*isolation).await,
            Statement::Commit => self.commit_cmd().await,
            Statement::Rollback => self.rollback_cmd().await,
            Statement::CreateTable { .. }
            | Statement::CreateIndex { .. }
            | Statement::DropIndex { .. }
            | Statement::DropTable { .. }
            | Statement::AlterTableRename { .. }
            | Statement::AlterTableAddPrimaryKey { .. }
            | Statement::CreateView { .. }
            | Statement::DropView { .. }
            // SP40: FDW DDL funnels through the same catalog-lock + execute_ddl + commit
            // path as ordinary DDL. ALTER/IMPORT variants resolve to a clear 0A000 inside
            // execute_ddl (not yet supported / deferred to a later task).
            | Statement::CreateFdw { .. }
            | Statement::DropFdw { .. }
            | Statement::CreateServer { .. }
            | Statement::AlterServer { .. }
            | Statement::DropServer { .. }
            | Statement::CreateUserMapping { .. }
            | Statement::AlterUserMapping { .. }
            | Statement::DropUserMapping { .. }
            | Statement::CreateForeignTable { .. }
            | Statement::DropForeignTable { .. }
            | Statement::CreateRole { .. }
            | Statement::DropRole { .. }
            | Statement::GrantTablePrivileges { .. }
            | Statement::RevokeTablePrivileges { .. }
            | Statement::ImportForeignSchema { .. } => self.run_ddl(stmt).await,
            Statement::Insert { .. }
            | Statement::Update { .. }
            | Statement::Delete { .. }
            | Statement::Truncate { .. } => self.run_write(stmt).await,
            Statement::Vacuum => {
                // PostgreSQL refuses VACUUM inside a transaction block. The
                // reclamation itself is autonomous here (adaptive background
                // vacuum with idle drain), so outside a block the accepted
                // hint returns immediately.
                if matches!(self.state, TxnState::InTransaction(_)) {
                    return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
                        "25001",
                        "VACUUM cannot run inside a transaction block",
                    )));
                }
                Ok(QueryResult::Command {
                    tag: "VACUUM".into(),
                })
            }
            Statement::Set { name, .. } if name == crabka_pgparser::ast::COPY_FROM_STDIN_SENTINEL => Err(ExecError::Unsupported(
                "COPY FROM STDIN requires pgwire CopyData messages".into(),
            )),
            Statement::Query(q) if q.locking.is_some() => self.run_query_locking(q).await,
            Statement::Query(_) => self.run_select(stmt).await,
            // SP37: GUC control. These are NOT exempt from the failed-txn guard
            // above (only COMMIT/ROLLBACK are), so a SET in an aborted block is
            // rejected — matching PostgreSQL.
            Statement::Set { local, name, value } if name == "__set_transaction" => {
                self.set_transaction(value).await
            }
            Statement::Set { name, .. } if name == "__discard_all" => self.discard_all(),
            Statement::Set { local, name, value } => self.set_guc(*local, name, value),
            Statement::Reset { target } => self.reset_guc(target),
            Statement::SetRole { role } => self.set_role(role.as_deref()),
            Statement::Show { name } => self.show_guc(name),
        };
        // Any error inside a transaction block aborts it (PostgreSQL 25P02): the
        // block stays Failed (carrying its ctx, so the xid and any row locks it
        // holds stay held) until COMMIT/ROLLBACK releases them. Autocommit errors
        // leave us Idle (the statement was its own transaction).
        if result.is_err() {
            self.mark_transaction_failed();
        } else if establishes_transaction_activity(stmt)
            && let TxnState::InTransaction(ctx) = &mut self.state
        {
            ctx.activity_started = true;
        }
        result
    }

    /// Record an aborted transaction's outcome (clog Aborted + deregister) and
    /// release its row locks. Shared by ROLLBACK and COMMIT-of-failed.
    async fn abort_ctx(&self, ctx: TxnCtx) -> Result<(), ExecError> {
        if let Some(xid) = ctx.xid {
            // Best-effort abort record; the versions are already invisible
            // (in-progress in no future snapshot once deregistered), so even if
            // this write is lost the rows never become visible.
            let r = self
                .committer
                .commit(vec![crabka_pgmvcc::clog::put_op(xid, XidStatus::Aborted)])
                .await;
            // Deregister even if the abort record failed to write: restart
            // re-seeds the ProcArray empty and the rows stay invisible (no clog
            // Committed), so a phantom running xid must not be stranded here.
            self.procarray.finish(xid);
            // Free every row this transaction locked, waking any blocked writers.
            self.lockmgr.release_all(xid);
            r?;
        }
        Ok(())
    }

    async fn abort_current_global(&mut self) -> Result<(), ExecError> {
        let Some(g) = self.global_xid.take() else {
            return Ok(());
        };
        self.commit_global_decision(g, XidStatus::Aborted).await?;
        if let Some(gtm) = &self.gtm {
            gtm.finish_global(g);
        }
        Ok(())
    }

    async fn begin(&mut self, isolation: Option<IsolationLevel>) -> Result<QueryResult, ExecError> {
        if matches!(self.state, TxnState::InTransaction(_)) {
            // BEGIN inside a block is a no-op (PostgreSQL warns and keeps going).
            return Ok(QueryResult::Command {
                tag: "BEGIN".into(),
            });
        }
        let rr = matches!(isolation, Some(IsolationLevel::RepeatableRead));
        // RR reuses this snapshot for the whole txn, so confirm a linearizable read
        // point BEFORE taking it. RC re-snapshots (and re-gates) per statement, so
        // it leaves a placeholder here and is not gated at BEGIN.
        if rr {
            self.linearizer.ensure_readable().await?;
            self.ensure_global_readable().await?; // range 0 caught up before the gsnap
        }
        let snapshot = self.procarray.snapshot();
        // RR fixes its GLOBAL snapshot at BEGIN too (so a Prepared(-> g) row's
        // in-doubt-ness is stable for the whole txn); RC re-captures per statement,
        // so leave it None here. Reconstructed from range 0's DURABLE state (after
        // the barrier above); NO_GLOBAL_SNAPSHOT() on a single-range engine.
        let global_snapshot = if rr {
            Some(self.global_read_snapshot(None)?)
        } else {
            None
        };
        // Pin the garbage horizon at this snapshot's xmin for the block's
        // lifetime: an open REPEATABLE READ transaction must keep every version
        // its fixed snapshot can see; a READ COMMITTED block's later statement
        // snapshots all have xmin >= this pin, so it covers them conservatively.
        let snapshot_pin = self.gc_horizon.pin(snapshot.xmin);
        let timestamp_read = if rr {
            Some(self.allocate_statement_read_timestamp().await?)
        } else {
            None
        };
        // Mirror pin in the timestamp domain: a fixed RR read timestamp must
        // hold the reclaim floor so sharded-table pruning keeps its history.
        let timestamp_read_pin = match timestamp_read {
            Some(read_ts) => Some(self.ts_gc.pin_read(self.kv.as_ref(), read_ts)?),
            None => None,
        };
        self.state = TxnState::InTransaction(TxnCtx {
            xid: None,
            snapshot,
            _snapshot_pin: snapshot_pin,
            repeatable_read: rr,
            global_snapshot,
            timestamp_read,
            _timestamp_read_pin: timestamp_read_pin,
            written_rows: Vec::new(),
            // PG transaction-stable `now()`/`current_timestamp`: fix it once at BEGIN.
            txn_now: self.clock.now(),
            unique_index_guard: None,
            table_write_guard: None,
            writer_fence_guard: None,
            activity_started: false,
        });
        Ok(QueryResult::Command {
            tag: "BEGIN".into(),
        })
    }

    async fn commit_cmd(&mut self) -> Result<QueryResult, ExecError> {
        match std::mem::replace(&mut self.state, TxnState::Idle) {
            TxnState::InTransaction(ctx) => {
                if let Some(xid) = ctx.xid {
                    if let Some(g) = self.global_xid.take() {
                        let status = self.commit_global_decision(g, XidStatus::Committed).await?;
                        self.procarray.finish(xid);
                        self.lockmgr.release_all(xid);
                        if let Some(gtm) = &self.gtm {
                            gtm.finish_global(g);
                        }
                        if !matches!(status, XidStatus::Committed) {
                            return Err(ExecError::SerializationFailure);
                        }
                        self.guc.commit();
                        return Ok(QueryResult::Command {
                            tag: "COMMIT".into(),
                        });
                    }
                    // Record the commit. Deregister xid BEFORE propagating any
                    // write error so the xid never stays stuck in the running set.
                    let mut ops = vec![crabka_pgmvcc::clog::put_op(xid, XidStatus::Committed)];
                    // In Replicated mode, fold the next_xid advance into the
                    // committed batch (the state machine max-merges it). A txn
                    // that allocated its xid only via a locking SELECT (FOR
                    // UPDATE / FOR SHARE) wrote no rows, so without this its
                    // next_xid bump would never reach the replicated state
                    // machine — after failover the new leader would reseed from a
                    // stale next_xid and re-hand-out this xid, whose clog entry is
                    // durably Committed (dirty reads). Redundant-but-harmless for
                    // data-writing txns: their write entry already folded
                    // next_xid and this COMMIT entry is ordered after it.
                    if self.persist_mode == crate::PersistMode::Replicated {
                        ops.push(self.procarray.next_xid_op());
                    }
                    let r = self.committer.commit(ops).await;
                    self.procarray.finish(xid);
                    // Free every row this transaction locked, waking waiters.
                    self.lockmgr.release_all(xid);
                    r?;
                }
                // SP37: a real COMMIT of an open block promotes any staged session
                // GUC override and drops any LOCAL override.
                self.guc.commit();
                Ok(QueryResult::Command {
                    tag: "COMMIT".into(),
                })
            }
            // COMMIT of a failed transaction behaves as a ROLLBACK.
            TxnState::Failed(ctx) => {
                self.abort_current_global().await?;
                self.abort_ctx(ctx).await?;
                // SP37: a failed block discards every staged GUC override.
                self.guc.rollback();
                Ok(QueryResult::Command {
                    tag: "ROLLBACK".into(),
                })
            }
            TxnState::Idle => Ok(QueryResult::Command {
                tag: "COMMIT".into(),
            }),
            TxnState::Prepared(ctx) => {
                self.state = TxnState::Prepared(ctx);
                self.reject_prepared_participant()?;
                unreachable!("reject_prepared_participant always errors for Prepared")
            }
        }
    }

    async fn rollback_cmd(&mut self) -> Result<QueryResult, ExecError> {
        match std::mem::replace(&mut self.state, TxnState::Idle) {
            TxnState::InTransaction(ctx) | TxnState::Failed(ctx) => {
                self.abort_current_global().await?;
                self.abort_ctx(ctx).await?;
            }
            TxnState::Prepared(ctx) => {
                self.state = TxnState::Prepared(ctx);
                self.reject_prepared_participant()?;
            }
            TxnState::Idle => {}
        }
        // SP37: ROLLBACK discards every staged GUC override (session and LOCAL).
        self.guc.rollback();
        Ok(QueryResult::Command {
            tag: "ROLLBACK".into(),
        })
    }

    async fn begin_sharded_global(&mut self) -> Result<u64, ExecError> {
        if let Some(g) = self.global_xid {
            return Ok(g);
        }
        let gtm = self.gtm.as_ref().ok_or_else(|| {
            ExecError::Unsupported(
                "sharded table writes require a global transaction manager".into(),
            )
        })?;
        let g = gtm.begin_global();
        self.committer
            .commit(vec![gtm.next_global_xid_op()])
            .await?;
        self.global_xid = Some(g);
        Ok(g)
    }

    async fn commit_global_decision(
        &self,
        g: u64,
        status: XidStatus,
    ) -> Result<XidStatus, ExecError> {
        let gtm = self
            .gtm
            .as_ref()
            .expect("global decision on a non-GTM session");
        self.committer
            .commit(vec![
                crabka_pgmvcc::clog::put_op(g, status),
                gtm.next_global_xid_op(),
            ])
            .await?;
        Ok(crabka_pgmvcc::clog::get(self.catalog_kv.as_ref(), g)?)
    }

    fn statement_targets_sharded_table(&self, stmt: &Statement) -> Result<bool, ExecError> {
        let table = match stmt {
            Statement::Insert { table, .. }
            | Statement::Update { table, .. }
            | Statement::Delete { table, .. } => table,
            _ => return Ok(false),
        };
        let table = crabka_pgcatalog::get_table(self.catalog_kv.as_ref(), table)?;
        Ok(crate::exec::table_uses_global_visibility(&table))
    }

    fn statement_has_returning(stmt: &Statement) -> bool {
        match stmt {
            Statement::Insert { returning, .. }
            | Statement::Update { returning, .. }
            | Statement::Delete { returning, .. } => returning.is_some(),
            _ => false,
        }
    }

    fn locking_select_targets_sharded_table(
        &self,
        s: &crabka_pgparser::ast::SelectStmt,
    ) -> Result<bool, ExecError> {
        let [TableExpr::Table { name, .. }] = s.from.as_slice() else {
            return Ok(false);
        };
        let table = crabka_pgcatalog::get_table(self.catalog_kv.as_ref(), name)?;
        Ok(crate::exec::table_uses_global_visibility(&table))
    }

    /// The GLOBAL snapshot a read should resolve `Prepared(-> g)` rows against.
    /// RR reuses the one captured at BEGIN (`stored`); RC / autocommit capture a
    /// fresh one from the GTM. A non-GTM (single-range) engine has no GTM, so this
    /// is `NO_GLOBAL_SNAPSHOT()` and the resolver's `Prepared` branch is
    /// unreachable (no `Prepared` tuple ever exists there).
    fn global_read_snapshot(&self, stored: Option<&Snapshot>) -> Result<Snapshot, ExecError> {
        if let Some(s) = stored {
            return Ok(s.clone()); // RR reuses the durable snapshot taken at BEGIN
        }
        // Any engine that can see cross-range Prepared rows reconstructs gsnap from
        // range 0's DURABLE state. The in-memory GTM running set is NEVER consulted
        // (correction C2): a network commit prunes g on one node only, so a range-0
        // running-set read would hide its own just-committed row cluster-wide.
        if self.gtm.is_some() || self.range0_barrier.is_some() {
            return durable_global_snapshot(&*self.catalog_kv);
        }
        Ok(crate::NO_GLOBAL_SNAPSHOT()) // single-range engine: no global xids exist
    }

    /// Allocate a statement read timestamp strictly above this range's durable
    /// horizon.
    ///
    /// The global timestamp source grants it in the common case, unchanged. When
    /// a single-shard bypass commit has lifted the durable horizon past the
    /// global source's position — so the source can no longer grant above the
    /// floor — the read falls back to this range's local sequence, which folds
    /// the horizon in and sits strictly above it. Genuine source unavailability
    /// (not a below-horizon grant) still propagates as an error.
    async fn allocate_statement_read_timestamp(
        &self,
    ) -> Result<crate::timestamp_txn::ReadTimestamp, ExecError> {
        let horizon = self.timestamp_horizon.current()?;
        let granted = self
            .timestamp_oracle
            .allocate_read_timestamp()
            .await
            .map_err(|error| ExecError::Unsupported(error.to_string()))?;
        if granted.get() > horizon {
            return Ok(granted);
        }
        self.local_sequence.observe(horizon);
        self.local_sequence
            .allocate_read_timestamp()
            .map_err(|error| ExecError::Unsupported(error.to_string()))
    }

    async fn run_select(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        let (snapshot, own, gsnap) = self.read_context().await?;
        let read_ts = match &self.state {
            TxnState::InTransaction(context) if context.repeatable_read => {
                context.timestamp_read.ok_or_else(|| {
                    ExecError::Unsupported("repeatable-read timestamp is missing".into())
                })?
            }
            TxnState::InTransaction(_) | TxnState::Idle => {
                self.allocate_statement_read_timestamp().await?
            }
            TxnState::Prepared(_) | TxnState::Failed(_) => {
                return Err(ExecError::InFailedTransaction);
            }
        };
        // Hold the timestamp-domain reclaim floor at or below this statement's
        // read timestamp for its duration, so concurrent write-path timestamp
        // pruning on this engine cannot reclaim a version it may resolve to.
        let _ts_read_pin = self.ts_gc.pin_read(self.kv.as_ref(), read_ts)?;
        let statement_scanner = if let Some(own_start_ts) = self.timestamp_own_start_ts {
            crate::scanner::TimestampedRangeScanner::with_own_transaction(
                Arc::clone(&self.range_scanner),
                read_ts,
                own_start_ts,
            )
        } else {
            crate::scanner::TimestampedRangeScanner::new(Arc::clone(&self.range_scanner), read_ts)
        };
        let statement_scanner = statement_scanner
            .with_join_planner(Arc::clone(&self.join_stats), self.join_strategy_config);
        let ctx = self.eval_ctx();
        // SP40: the session does not track an authenticated SQL user, so foreign-table
        // user-mapping lookups resolve against the conventional `"public"` mapping.
        let fctx = crate::exec::ForeignCtx {
            scanner: self.foreign_scanner.as_ref(),
            current_user: &self.current_role,
        };
        let ctes = crate::cte::CteContext::empty();
        let read_ctx = crate::subquery::SubCtx {
            catalog_kv: &*self.catalog_kv,
            kv: &*self.kv,
            global: &*self.catalog_kv,
            gsnap: &gsnap,
            snapshot: &snapshot,
            own,
            ctes: &ctes,
            eval_ctx: &ctx,
            fctx,
            range_scanner: &statement_scanner,
        };
        let (result, mutations) =
            with_guc_runtime(self.guc.effective_map(), self.guc.settings(), || {
                crate::exec::execute_read(&read_ctx, stmt)
            });
        if result.is_ok() {
            self.apply_guc_mutations(mutations)?;
        }
        result
    }

    async fn run_query_locking(&mut self, q: &QueryExpr) -> Result<QueryResult, ExecError> {
        if let Some(with) = &q.with {
            crate::cte::reject_recursive(with)?;
            return Err(ExecError::Unsupported(
                "FOR UPDATE/SHARE with CTEs is not supported".into(),
            ));
        }
        let SetExpr::Query(QueryBody::Select(s)) = &q.body else {
            return Err(ExecError::Unsupported(
                "locking is only supported on SELECT".into(),
            ));
        };
        let mut s = (**s).clone();
        s.order_by = q.order_by.clone();
        s.limit = q.limit;
        s.offset = q.offset;
        s.locking = q.locking;
        self.run_select_locking(&s).await
    }

    /// Locking SELECT (FOR UPDATE / FOR SHARE). Allocates an xid if none is
    /// active, takes row locks, EvalPlanQual-rechecks each row, and returns
    /// the surviving rows. Autocommit: finish + release_all at statement end
    /// (success and error). In-txn: locks persist until COMMIT/ROLLBACK.
    async fn run_select_locking(
        &mut self,
        s: &crabka_pgparser::ast::SelectStmt,
    ) -> Result<QueryResult, ExecError> {
        let mode = match s.locking {
            Some(RowLockStrength::ForUpdate) => crate::lockmgr::LockMode::Exclusive,
            Some(RowLockStrength::ForShare) => crate::lockmgr::LockMode::Shared,
            None => unreachable!("run_one only routes here when locking.is_some()"),
        };

        match &self.state {
            TxnState::InTransaction(_) => {
                // RC re-snapshots (and re-gates) per statement; RR reuses the
                // snapshot fixed and gated at BEGIN. Gate iff we re-snapshot.
                let refresh =
                    matches!(&self.state, TxnState::InTransaction(c) if !c.repeatable_read);
                if refresh {
                    // Gate before any local work (xid allocation, snapshot).
                    self.linearizer.ensure_readable().await?;
                    self.ensure_global_readable().await?; // range 0 caught up too
                }
                // Allocate an xid if the txn has not done a write yet (a FOR
                // UPDATE in a read-only txn still needs one, like PG).
                self.ensure_write_xid()?;
                if self.locking_select_targets_sharded_table(s)? {
                    self.begin_sharded_global().await?;
                }
                if refresh {
                    let snap = self.procarray.snapshot();
                    if let TxnState::InTransaction(c) = &mut self.state {
                        c.snapshot = snap;
                    }
                }
                // RC re-captures the global snapshot per statement; RR reuses the
                // one fixed at BEGIN. NO_GLOBAL_SNAPSHOT() on a non-GTM engine.
                let gsnap = match &self.state {
                    TxnState::InTransaction(c) if c.repeatable_read => {
                        self.global_read_snapshot(c.global_snapshot.as_ref())?
                    }
                    _ => self.global_read_snapshot(None)?,
                };
                let (snapshot, xid, repeatable_read) = match &self.state {
                    TxnState::InTransaction(c) => (
                        c.snapshot.clone(),
                        c.xid.expect("xid set by ensure_write_xid"),
                        c.repeatable_read,
                    ),
                    _ => unreachable!(),
                };
                let ctx = self.eval_ctx();
                let ctes = crate::cte::CteContext::empty();
                let read_ctx = crate::subquery::SubCtx {
                    catalog_kv: &*self.catalog_kv,
                    kv: self.kv.as_ref(),
                    global: &*self.catalog_kv,
                    gsnap: &gsnap,
                    snapshot: &snapshot,
                    own: Some(xid),
                    ctes: &ctes,
                    eval_ctx: &ctx,
                    fctx: crate::exec::ForeignCtx::none(),
                    range_scanner: self.range_scanner.as_ref(),
                };
                // Errors propagate to run_one which transitions to Failed,
                // keeping the xid + locks until COMMIT/ROLLBACK.
                crate::exec::execute_read_locking(
                    &read_ctx,
                    &self.procarray,
                    &self.lockmgr,
                    repeatable_read,
                    mode,
                    self.lock_wait_cap,
                    s,
                )
                .await
            }
            TxnState::Idle => {
                // Autocommit read takes a fresh snapshot → gate before any local
                // work (xid allocation, snapshot).
                self.linearizer.ensure_readable().await?;
                self.ensure_global_readable().await?; // range 0 caught up too
                // Autocommit: allocate an xid, run the locking SELECT, then
                // immediately release the locks (implicit txn ends at statement
                // end — there is no open block to hold them).
                let xid = self.procarray.begin_write()?;
                let sharded_lock = self.locking_select_targets_sharded_table(s)?;
                let sharded_global = if sharded_lock {
                    Some(self.begin_sharded_global().await?)
                } else {
                    None
                };
                let snapshot = self.procarray.snapshot();
                let gsnap = self.global_read_snapshot(None)?;
                let ctx = self.eval_ctx();
                let ctes = crate::cte::CteContext::empty();
                let read_ctx = crate::subquery::SubCtx {
                    catalog_kv: &*self.catalog_kv,
                    kv: self.kv.as_ref(),
                    global: &*self.catalog_kv,
                    gsnap: &gsnap,
                    snapshot: &snapshot,
                    own: Some(xid),
                    ctes: &ctes,
                    eval_ctx: &ctx,
                    fctx: crate::exec::ForeignCtx::none(),
                    range_scanner: self.range_scanner.as_ref(),
                };
                let result = crate::exec::execute_read_locking(
                    &read_ctx,
                    &self.procarray,
                    &self.lockmgr,
                    false, // autocommit is always READ COMMITTED
                    mode,
                    self.lock_wait_cap,
                    s,
                )
                .await;
                // Release regardless of success or error.
                self.procarray.finish(xid);
                self.lockmgr.release_all(xid);
                if let Some(g) = sharded_global {
                    if result.is_ok() {
                        self.commit_global_decision(g, XidStatus::Committed).await?;
                    } else {
                        let _ = self.abort_current_global().await;
                    }
                    self.global_xid = None;
                    if let Some(gtm) = &self.gtm {
                        gtm.finish_global(g);
                    }
                }
                result
            }
            TxnState::Prepared(_) => unreachable!("guarded in run_one"),
            TxnState::Failed(_) => unreachable!("guarded in run_one"),
        }
    }

    /// The (local snapshot, own-xid, global snapshot) a read should use.
    /// Autocommit: a fresh local + global snapshot, no own xid. In a txn: RC
    /// re-snapshots both per statement, RR reuses the local + global snapshots
    /// fixed at BEGIN; own xid is the txn's (Some after its first write). Gates
    /// before establishing a fresh snapshot (autocommit + RC); RR was gated at
    /// BEGIN. The global snapshot is `NO_GLOBAL_SNAPSHOT()` on a non-GTM engine.
    async fn read_context(&mut self) -> Result<(Snapshot, Option<u64>, Snapshot), ExecError> {
        enum Plan {
            Auto,
            RcRefresh,
            RrReuse,
        }
        // Decide the plan under a short borrow, then release it before awaiting
        // the gate (no `self` borrow held across the await).
        let plan = match &self.state {
            TxnState::Idle => Plan::Auto,
            TxnState::InTransaction(c) => {
                if c.repeatable_read {
                    Plan::RrReuse
                } else {
                    Plan::RcRefresh
                }
            }
            TxnState::Prepared(_) => unreachable!("guarded in run_one"),
            TxnState::Failed(_) => unreachable!("guarded in run_one"),
        };
        match plan {
            Plan::Auto => {
                self.linearizer.ensure_readable().await?;
                self.ensure_global_readable().await?; // range 0 caught up before the gsnap
                let gsnap = self.global_read_snapshot(None)?;
                Ok((self.procarray.snapshot(), None, gsnap))
            }
            Plan::RcRefresh => {
                self.linearizer.ensure_readable().await?;
                self.ensure_global_readable().await?; // range 0 caught up before the gsnap
                let snap = self.procarray.snapshot();
                // RC re-captures the global snapshot per statement too.
                let gsnap = self.global_read_snapshot(None)?;
                match &mut self.state {
                    TxnState::InTransaction(c) => {
                        c.snapshot = snap.clone();
                        Ok((snap, c.xid, gsnap))
                    }
                    _ => unreachable!(),
                }
            }
            Plan::RrReuse => match &self.state {
                TxnState::InTransaction(c) => {
                    let gsnap = self.global_read_snapshot(c.global_snapshot.as_ref())?;
                    Ok((c.snapshot.clone(), c.xid, gsnap))
                }
                _ => unreachable!(),
            },
        }
    }

    /// DDL is non-transactional and writes through immediately. All DDL funnels
    /// through the leader's catalog_lock held ACROSS the Raft commit, so DDL is
    /// globally serialized (next_table_id read+bump+commit is atomic; low
    /// throughput, fine for D1 — concurrent-DDL optimization is a later slice).
    /// The tokio Mutex is intentionally held across .await (allowed: it is an
    /// async mutex).
    async fn run_ddl(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        // An explicit writer retains its writer-fence lease, but releases its
        // shared physical gate before waiting for the catalog lock. Conversion
        // waits for that lease before it can acquire the physical gate.
        let transaction_holds_shared_fence = matches!(
            &self.state,
            TxnState::InTransaction(context)
                if matches!(context.table_write_guard, Some(TableWriteGuard::Shared { .. }))
        );
        if transaction_holds_shared_fence && let TxnState::InTransaction(context) = &mut self.state
        {
            context.table_write_guard = None;
        }
        let _g = self.catalog_lock.lock().await;
        let _unique_guard = if crate::exec::ddl_requires_unique_local_serialization(stmt) {
            Some(Arc::clone(&self.unique_index_lock).write_owned().await)
        } else {
            None
        };
        // SP40: IMPORT FOREIGN SCHEMA needs the registered scanner + current user
        // to discover foreign tables; the rest of DDL ignores the ForeignCtx.
        let fctx = crate::exec::ForeignCtx {
            scanner: self.foreign_scanner.as_ref(),
            current_user: &self.current_role,
        };
        let (result, ops) = crate::exec::execute_ddl(&*self.catalog_kv, stmt, fctx)?;
        // A data-range session reads schema metadata from range 0. Its committer
        // targets the local data range, so applying a catalog batch through it
        // would create metadata that is neither authoritative nor visible to
        // subsequent catalog lookups. The single-store path retains the commit
        // seam; a distinct catalog store owns its own atomic catalog batch.
        if Arc::ptr_eq(&self.kv, &self.catalog_kv) {
            self.committer.commit(ops).await?;
        } else {
            self.catalog_kv.write_batch(&ops)?;
        }
        Ok(result)
    }

    async fn ensure_unique_index_guard(&mut self, mode: UniqueLocalSerialization) {
        if matches!(mode, UniqueLocalSerialization::None) {
            return;
        }
        match &self.state {
            TxnState::InTransaction(ctx) if ctx.unique_index_guard.is_none() => {}
            _ => return,
        }
        let guard = UniqueIndexGuard {
            _guard: Arc::clone(&self.unique_index_lock).read_owned().await,
        };
        if let TxnState::InTransaction(ctx) = &mut self.state {
            ctx.unique_index_guard = Some(guard);
        }
    }

    async fn ensure_table_write_guard(&mut self) {
        let needs_guard = matches!(
            &self.state,
            TxnState::InTransaction(ctx) if ctx.table_write_guard.is_none()
        );
        if !needs_guard {
            return;
        }
        let writer_fence_guard = Arc::clone(&self.writer_fence).writer().await;
        let guard = Arc::clone(&self.table_write_gate).read_owned().await;
        if let TxnState::InTransaction(ctx) = &mut self.state {
            ctx.table_write_guard = Some(TableWriteGuard::Shared { _guard: guard });
            ctx.writer_fence_guard = Some(writer_fence_guard);
        }
    }

    async fn run_write(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        match &self.state {
            TxnState::InTransaction(_) => {
                self.ensure_table_write_guard().await;
                let targets_sharded_table = self.statement_targets_sharded_table(stmt)?;
                if targets_sharded_table && Self::statement_has_returning(stmt) {
                    return Err(ExecError::Unsupported(
                        "RETURNING on sharded timestamp writes is not supported".into(),
                    ));
                }
                if targets_sharded_table {
                    return Err(ExecError::Unsupported(
                        "sharded table writes inside explicit transactions are not supported"
                            .into(),
                    ));
                }
                self.ensure_write_xid()?;
                let unique_serialization = crate::exec::write_requires_unique_local_serialization(
                    self.catalog_kv.as_ref(),
                    stmt,
                )?;
                self.ensure_unique_index_guard(unique_serialization).await;
                // UPDATE/DELETE's eval_plan_qual re-check reads range 0's global clog
                // to resolve a cross-range supersede, so catch range 0's replica up
                // before the gsnap capture. (RR already barriered at BEGIN; the
                // barrier is idempotent.)
                self.ensure_global_readable().await?;
                // RC refreshes the read snapshot used by UPDATE/DELETE's scan.
                let refresh =
                    matches!(&self.state, TxnState::InTransaction(c) if !c.repeatable_read);
                if refresh {
                    let s = self.procarray.snapshot();
                    if let TxnState::InTransaction(c) = &mut self.state {
                        c.snapshot = s;
                    }
                }
                // RC re-captures the global snapshot per statement; RR reuses the
                // one fixed at BEGIN. NO_GLOBAL_SNAPSHOT() on a non-GTM engine. The
                // UPDATE/DELETE re-check resolves a cross-range supersede through it.
                let gsnap = match &self.state {
                    TxnState::InTransaction(c) if c.repeatable_read => {
                        self.global_read_snapshot(c.global_snapshot.as_ref())?
                    }
                    _ => self.global_read_snapshot(None)?,
                };
                let (snapshot, xid, repeatable_read) = match &self.state {
                    TxnState::InTransaction(c) => (
                        c.snapshot.clone(),
                        c.xid.expect("xid set"),
                        c.repeatable_read,
                    ),
                    _ => unreachable!(),
                };
                let ctx = self.eval_ctx();
                // An error here propagates to run_one, which transitions the
                // block to Failed (keeping the xid + row locks until
                // COMMIT/ROLLBACK, which calls release_all). In Durable mode
                // ProcArray's block-ahead reservation already durably covers
                // this xid, so no next_xid op; the
                // txn commits later, so no clog op. In Replicated mode we fold the
                // next_xid op into this batch (the state machine max-merges it;
                // re-folding on a later write in the same txn is harmless).
                // Cache the garbage horizon once per statement: UPDATE/DELETE
                // prune the chains of the rows they write against it, in the
                // same commit batch as the write itself.
                let prune_horizon = Some(self.local_prune_horizon()?);
                let write_ctx = self.write_context(
                    &gsnap,
                    &snapshot,
                    xid,
                    repeatable_read,
                    &ctx,
                    prune_horizon,
                );
                let (result, mut ops) = crate::exec::execute_write(&write_ctx, stmt).await?;
                // Record the (table_id, rowid)s this write touched (from the version
                // Puts it built) so the abort-atomicity fence (`effective_global_xid`)
                // can scan them for an inherited in-doubt `Prepared(-> g_old)` marker.
                // Read BEFORE the marker push below so the fence sees only pre-existing
                // versions (the new `xmin` version is not committed to `self.kv` yet).
                let touched: Vec<(u32, u64)> = ops
                    .iter()
                    .filter_map(|op| match op {
                        crabka_pgkv::WriteOp::Put { key, .. } => {
                            crabka_pgkv::key::table_rowid_of(key)
                        }
                        _ => None,
                    })
                    .collect();
                if let TxnState::InTransaction(c) = &mut self.state {
                    c.written_rows.extend(touched);
                }
                // A participant in a cross-range global txn `g` stamps a
                // Prepared(xid -> g) marker into the SAME durable batch so the row
                // carries it from the start, and deregisters `xid` from the
                // ProcArray running-set at prepare time (the atomicity linchpin):
                // the local snapshot then no longer gates the row, deferring
                // visibility entirely to range 0's global clog. This also covers
                // the case where the escalation trigger IS this range's first
                // write, so `join_global` had no local xid to backfill. Idempotent
                // on later writes of the same txn (the marker key/value is stable
                // and `finish` is a set-remove). The stamped global xid is FENCED to
                // any in-doubt decision already governing a touched row
                // (`effective_global_xid` — SP24 abort atomicity): a failover re-stage
                // adopts the original `g_old` instead of this attempt's fresh `g`, so a
                // row never carries two competing global decisions.
                if let Some(g) = self.global_xid {
                    let eff = self.effective_global_xid(g)?;
                    self.global_xid = Some(eff);
                    ops.push(crabka_pgmvcc::clog::put_op(xid, XidStatus::Prepared(eff)));
                }
                if self.persist_mode == crate::PersistMode::Replicated {
                    ops.push(self.procarray.next_xid_op());
                }
                self.committer.commit(ops).await?;
                if self.global_xid.is_some() {
                    self.procarray.finish(xid); // deregister-at-prepare
                }
                Ok(result)
            }
            TxnState::Idle => {
                let _writer_fence_guard = Arc::clone(&self.writer_fence).writer().await;
                let _table_write_guard = Arc::clone(&self.table_write_gate).read_owned().await;
                let targets_sharded_table = self.statement_targets_sharded_table(stmt)?;
                if targets_sharded_table && Self::statement_has_returning(stmt) {
                    return Err(ExecError::Unsupported(
                        "RETURNING on sharded timestamp writes is not supported".into(),
                    ));
                }
                if targets_sharded_table {
                    return self.run_sharded_timestamp_autocommit(stmt).await;
                }
                let _unique_guard = match crate::exec::write_requires_unique_local_serialization(
                    self.catalog_kv.as_ref(),
                    stmt,
                )? {
                    UniqueLocalSerialization::None => None,
                    UniqueLocalSerialization::Shared => Some(UniqueIndexGuard {
                        _guard: Arc::clone(&self.unique_index_lock).read_owned().await,
                    }),
                };
                // Autocommit UPDATE/DELETE's eval_plan_qual re-check reads range 0's
                // global clog, so catch range 0's replica up before the gsnap capture.
                self.ensure_global_readable().await?;
                // Autocommit: allocate an xid, execute (taking row locks), and
                // commit in one atomic batch (versions + clog). No global writer
                // lock; begin_write's durable block reservation covers the xid.
                let xid = self.procarray.begin_write()?;
                let sharded_write = targets_sharded_table;
                let sharded_global = if sharded_write {
                    Some(self.begin_sharded_global().await?)
                } else {
                    None
                };
                let snapshot = self.procarray.snapshot();
                let gsnap = self.global_read_snapshot(None)?;
                let ctx = self.eval_ctx();
                // Cache the garbage horizon once per statement (see the
                // in-transaction branch above).
                let prune_horizon = Some(self.local_prune_horizon()?);
                let write_ctx =
                    self.write_context(&gsnap, &snapshot, xid, false, &ctx, prune_horizon);
                let outcome = crate::exec::execute_write(&write_ctx, stmt).await;
                let (result, mut ops) = match outcome {
                    Ok(v) => v,
                    Err(e) => {
                        if sharded_global.is_some() {
                            let _ = self.abort_current_global().await;
                        }
                        // Autocommit error: abort and stay Idle. Record the abort
                        // (best-effort), deregister, and free this xid's row locks.
                        let _ = self
                            .committer
                            .commit(vec![crabka_pgmvcc::clog::put_op(xid, XidStatus::Aborted)])
                            .await;
                        self.procarray.finish(xid);
                        self.lockmgr.release_all(xid);
                        return Err(e);
                    }
                };
                if let Some(g) = sharded_global {
                    ops.push(crabka_pgmvcc::clog::put_op(xid, XidStatus::Prepared(g)));
                } else {
                    ops.push(crabka_pgmvcc::clog::put_op(xid, XidStatus::Committed));
                }
                // In Replicated mode, fold the next_xid advance into the same
                // batch as the rows + clog (the state machine max-merges it); in
                // Durable mode begin_write's block reservation already covers it.
                if self.persist_mode == crate::PersistMode::Replicated {
                    ops.push(self.procarray.next_xid_op());
                }
                // Deregister xid and free its row locks BEFORE propagating any
                // write error so neither the running set nor the lock table is
                // left holding a finished xid on a commit-batch failure.
                let r = self.committer.commit(ops).await;
                self.procarray.finish(xid);
                self.lockmgr.release_all(xid);
                r?;
                if let Some(g) = sharded_global {
                    let status = self.commit_global_decision(g, XidStatus::Committed).await?;
                    self.global_xid = None;
                    if let Some(gtm) = &self.gtm {
                        gtm.finish_global(g);
                    }
                    if !matches!(status, XidStatus::Committed) {
                        return Err(ExecError::SerializationFailure);
                    }
                }
                Ok(result)
            }
            TxnState::Prepared(_) => unreachable!("guarded in run_one"),
            TxnState::Failed(_) => unreachable!("guarded in run_one"),
        }
    }

    async fn run_copy_in(
        &mut self,
        copy: &CopyStmt,
        chunks: Vec<bytes::Bytes>,
    ) -> Result<QueryResult, ExecError> {
        // Statement-duration garbage-horizon pin — see `run_one`.
        let _statement_pin = matches!(self.state, TxnState::Idle)
            .then(|| self.gc_horizon.pin(self.procarray.snapshot().xmin));
        if matches!(copy.format, CopyFormat::Csv) {
            return Err(ExecError::Unsupported("COPY CSV is not supported".into()));
        }
        let data_len = chunks.iter().map(bytes::Bytes::len).sum();
        let mut data = Vec::with_capacity(data_len);
        for chunk in chunks {
            data.extend_from_slice(&chunk);
        }
        let rows = crate::exec::decode_copy_text(&data)?;
        match &self.state {
            TxnState::InTransaction(_) => {
                self.ensure_table_write_guard().await;
                if crate::exec::table_uses_global_visibility(&crabka_pgcatalog::get_table(
                    self.catalog_kv.as_ref(),
                    &copy.table,
                )?) {
                    return Err(ExecError::Unsupported(
                        "COPY into sharded tables is not supported".into(),
                    ));
                }
                let unique_serialization = crate::exec::copy_requires_unique_local_serialization(
                    self.catalog_kv.as_ref(),
                    copy,
                )?;
                self.ensure_unique_index_guard(unique_serialization).await;
                self.ensure_write_xid()?;
                let xid = match &self.state {
                    TxnState::InTransaction(ctx) => ctx.xid.expect("xid set"),
                    _ => unreachable!(),
                };
                let ctx = self.eval_ctx();
                let gsnap = self.global_read_snapshot(None)?;
                let snapshot = self.procarray.snapshot();
                // COPY is insert-only: no chains are re-read, nothing to prune.
                let write_ctx = self.write_context(&gsnap, &snapshot, xid, false, &ctx, None);
                let (result, mut ops) =
                    crate::exec::execute_copy_write(&write_ctx, copy, &rows).await?;
                if self.persist_mode == crate::PersistMode::Replicated {
                    ops.push(self.procarray.next_xid_op());
                }
                self.committer.commit(ops).await?;
                Ok(result)
            }
            TxnState::Idle => {
                let _writer_fence_guard = Arc::clone(&self.writer_fence).writer().await;
                let _table_write_guard = Arc::clone(&self.table_write_gate).read_owned().await;
                let copy_table =
                    crabka_pgcatalog::get_table(self.catalog_kv.as_ref(), &copy.table)?;
                if crate::exec::table_uses_global_visibility(&copy_table) {
                    let ctx = self.eval_ctx();
                    let plan = crate::exec::execute_timestamp_copy_write(
                        self.catalog_kv.as_ref(),
                        self.kv.as_ref(),
                        self.seq.as_ref(),
                        copy,
                        &rows,
                        &ctx,
                    )?;
                    return self.commit_timestamp_write_plan(plan).await;
                }
                let _unique_guard = match crate::exec::copy_requires_unique_local_serialization(
                    self.catalog_kv.as_ref(),
                    copy,
                )? {
                    UniqueLocalSerialization::None => None,
                    UniqueLocalSerialization::Shared => Some(UniqueIndexGuard {
                        _guard: Arc::clone(&self.unique_index_lock).read_owned().await,
                    }),
                };
                let xid = self.procarray.begin_write()?;
                let ctx = self.eval_ctx();
                let gsnap = self.global_read_snapshot(None)?;
                let snapshot = self.procarray.snapshot();
                // COPY is insert-only: no chains are re-read, nothing to prune.
                let write_ctx = self.write_context(&gsnap, &snapshot, xid, false, &ctx, None);
                let outcome = crate::exec::execute_copy_write(&write_ctx, copy, &rows).await;
                let (result, mut ops) = match outcome {
                    Ok(value) => value,
                    Err(error) => {
                        let _ = self
                            .committer
                            .commit(vec![crabka_pgmvcc::clog::put_op(xid, XidStatus::Aborted)])
                            .await;
                        self.procarray.finish(xid);
                        // Free the unique-key locks the failed COPY acquired.
                        self.lockmgr.release_all(xid);
                        return Err(error);
                    }
                };
                ops.push(crabka_pgmvcc::clog::put_op(xid, XidStatus::Committed));
                if self.persist_mode == crate::PersistMode::Replicated {
                    ops.push(self.procarray.next_xid_op());
                }
                let commit = self.committer.commit(ops).await;
                self.procarray.finish(xid);
                // Free the unique-key locks this COPY acquired, waking waiters.
                self.lockmgr.release_all(xid);
                commit?;
                Ok(result)
            }
            TxnState::Prepared(_) => Err(ExecError::ObjectNotInPrerequisiteState(
                "global participant is externally prepared".into(),
            )),
            TxnState::Failed(_) => Err(ExecError::InFailedTransaction),
        }
    }

    async fn run_sharded_timestamp_autocommit(
        &mut self,
        stmt: &Statement,
    ) -> Result<QueryResult, ExecError> {
        let ctx = self.eval_ctx();
        let plan = crate::exec::execute_timestamp_write(
            self.catalog_kv.as_ref(),
            self.kv.as_ref(),
            self.seq.as_ref(),
            stmt,
            &ctx,
        )?;
        self.commit_timestamp_write_plan(plan).await
    }

    async fn commit_timestamp_write_plan(
        &self,
        plan: crate::exec::TimestampWritePlan,
    ) -> Result<QueryResult, ExecError> {
        if plan.writes.is_empty() {
            return Ok(plan.result);
        }
        let start_ts = self.allocate_timestamp_transaction_id().await?;
        let participant = crate::timestamp_txn::TimestampTxnParticipant::new(
            Arc::clone(&self.kv),
            Arc::clone(&self.catalog_kv),
            Arc::clone(&self.committer),
            0,
        )
        .with_ts_gc(Arc::clone(&self.ts_gc));
        let commit_ts = self.allocate_commit_timestamp_after(start_ts).await?;
        participant.prewrite(start_ts, &plan.writes).await?;
        match participant
            .commit_with_ops(start_ts, commit_ts, &plan.writes, plan.commit_ops)
            .await
        {
            // The participant publishes closure for the committed timestamp
            // itself (`observe_committed_decision`), feeding the reclaim
            // floor that prunes superseded versions.
            Ok(()) => Ok(plan.result),
            Err(error) => {
                let _ = participant.abort(start_ts, &plan.writes).await;
                Err(error)
            }
        }
    }

    async fn allocate_timestamp_transaction_id(
        &self,
    ) -> Result<crate::timestamp_txn::TimestampTransactionId, ExecError> {
        self.timestamp_oracle
            .allocate_transaction_id_after(self.timestamp_horizon.current()?)
            .await
            .map_err(|error| ExecError::Unsupported(error.to_string()))
    }

    async fn allocate_commit_timestamp_after(
        &self,
        start_ts: crate::timestamp_txn::TimestampTransactionId,
    ) -> Result<crate::timestamp_txn::CommitTimestamp, ExecError> {
        self.timestamp_oracle
            .allocate_commit_after_durable(start_ts, self.timestamp_horizon.current()?)
            .await
            .map_err(|error| ExecError::Unsupported(error.to_string()))
    }

    /// On a transaction's first write: allocate the xid (idempotent on later
    /// writes). No lock — concurrency is row-level via the RowLockManager.
    fn ensure_write_xid(&mut self) -> Result<(), ExecError> {
        let needs = matches!(&self.state, TxnState::InTransaction(c) if c.xid.is_none());
        if !needs {
            return Ok(());
        }
        let xid = self.procarray.begin_write()?;
        if let TxnState::InTransaction(c) = &mut self.state {
            c.xid = Some(xid);
        }
        Ok(())
    }

    /// The current transaction's local xid, if one has been allocated (`None` for
    /// an idle session or a read-only txn that has not yet written). For a
    /// participant in a global txn this is the per-range local `Li` the
    /// `Prepared(Li -> g)` marker ties to the global `g`.
    pub fn local_xid(&self) -> Option<u64> {
        match &self.state {
            TxnState::InTransaction(c) | TxnState::Prepared(c) | TxnState::Failed(c) => c.xid,
            TxnState::Idle => None,
        }
    }

    /// Begin a held txn on this session if it is Idle, so a participant's first
    /// DML is HELD (never autocommitted): the coordinator can then drive its
    /// COMMIT/ROLLBACK and the `Prepared` marker is written before any of its rows
    /// become eligible to commit on their own. Idempotent (no-op if already in a
    /// txn). Reuses `begin`.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn ensure_began(&mut self) -> Result<(), ExecError> {
        if matches!(self.state, TxnState::Idle) {
            self.begin(None).await?;
        }
        Ok(())
    }

    /// The global xid the `Prepared(Li -> ·)` marker for THIS participant write must
    /// carry — the **abort-atomicity fence**. Normally `g` (the txn's own global xid),
    /// but if any row this txn's local xid `Li` has written already carries a SIBLING
    /// version under an in-doubt `Prepared(-> g_old)` marker for a DIFFERENT global
    /// txn `g_old`, the marker ADOPTS `g_old` instead.
    ///
    /// Why: a participant whose leader is killed mid-cross-range-txn loses its in-memory
    /// held session; the coordinator/worker retries the WHOLE transfer under a FRESH
    /// global `g'`, re-staging the same row on the NEW leader. Without this fence the
    /// re-stage mints a SECOND live version of the row stamped `Prepared(-> g')`, so the
    /// row is governed by TWO independent global decisions (`g_old` and `g'`); if
    /// `g_old` aborts but `g'` commits the `g'`-version stays visible — money created or
    /// destroyed (the SP24 abort-atomicity half-leak). Adopting `g_old` keeps the row
    /// under EXACTLY ONE global decision: when `g_old` is later aborted by the recovery
    /// abort-race, every version of the row resolves invisible (the pre-txn value
    /// re-surfaces); when `g_old` commits, exactly one version is live. The retry's `g'`
    /// then governs no version of this row — which is correct, since the row was already
    /// enlisted in `g_old`.
    ///
    /// Only an IN-DOUBT `g_old` is adopted (read range 0's global clog via `catalog_kv`,
    /// which the caller has already barriered current): a `g_old` that is already
    /// terminally decided imposes no surviving enlistment, so the write proceeds under
    /// its own `g`. Bank txns touch one row per range, so at most one `g_old` is found;
    /// if several rows disagree the smallest in-doubt `g_old` is adopted deterministically
    /// (canonical, fingerprint-stable). A non-GTM (single-range) engine has no global
    /// clog and no `Prepared` rows, so this returns `g` unchanged.
    fn effective_global_xid(&self, g: u64) -> Result<u64, ExecError> {
        let li = match self.local_xid() {
            Some(li) => li,
            None => return Ok(g), // no local write yet → nothing to fence
        };
        let written = match &self.state {
            TxnState::InTransaction(c) | TxnState::Prepared(c) | TxnState::Failed(c) => {
                &c.written_rows
            }
            TxnState::Idle => return Ok(g),
        };
        let mut adopted: Option<u64> = None;
        for &(table_id, rowid) in written {
            let prefix = crabka_pgkv::key::row_key(table_id, rowid);
            for (_k, v) in self.kv.scan_prefix(&prefix)? {
                let (xmin, _xmax, _row) = crabka_pgmvcc::version::decode_tuple(&v)?;
                if xmin == li {
                    continue; // this txn's OWN version — never fences itself
                }
                // A sibling version under an in-doubt `Prepared(-> g_old != g)` marker
                // means `g_old` still governs this row; adopt it.
                if let XidStatus::Prepared(g_old) =
                    crabka_pgmvcc::clog::get(self.kv.as_ref(), xmin)?
                    && g_old != g
                    && !matches!(
                        crabka_pgmvcc::clog::get(self.catalog_kv.as_ref(), g_old)?,
                        XidStatus::Committed | XidStatus::Aborted
                    )
                {
                    adopted = Some(adopted.map_or(g_old, |a| a.min(g_old)));
                }
            }
        }
        Ok(adopted.unwrap_or(g))
    }

    /// Prepare this held transaction as a participant of the caller-provided
    /// durable global xid. Returns the effective global xid stamped into the
    /// local `Prepared(Li -> g)` marker; this is normally `global_xid`, but the
    /// abort-atomicity fence may adopt an older in-doubt global xid already
    /// governing the same row.
    ///
    /// # Errors
    ///
    /// Returns `ObjectNotInPrerequisiteState` if no transaction is open,
    /// `InFailedTransaction` if the transaction block is failed, and an error if
    /// the durable prepare marker cannot be written.
    pub async fn prepare_global_participant(&mut self, global_xid: u64) -> Result<u64, ExecError> {
        use crabka_pgmvcc::xid::GLOBAL_XID_BASE;

        if global_xid < GLOBAL_XID_BASE {
            return Err(ExecError::ObjectNotInPrerequisiteState(format!(
                "global participant xid {global_xid} is below the global xid range"
            )));
        }
        match &self.state {
            TxnState::InTransaction(_) => {}
            TxnState::Prepared(_) => {
                return Err(ExecError::ObjectNotInPrerequisiteState(
                    "global participant is already externally prepared".into(),
                ));
            }
            TxnState::Failed(_) => return Err(ExecError::InFailedTransaction),
            TxnState::Idle => {
                return Err(ExecError::ObjectNotInPrerequisiteState(
                    "global participant prepare requires an open transaction".into(),
                ));
            }
        }

        let effective_global_xid = self.join_global(global_xid).await?;
        let TxnState::InTransaction(ctx) = std::mem::replace(&mut self.state, TxnState::Idle)
        else {
            self.state = TxnState::Idle;
            return Err(ExecError::ObjectNotInPrerequisiteState(
                "global participant prepare lost its open transaction".into(),
            ));
        };
        self.state = TxnState::Prepared(ctx);
        self.global_xid.ok_or_else(|| {
            ExecError::ObjectNotInPrerequisiteState(
                "global participant prepare did not retain a global xid".into(),
            )
        })?;
        Ok(effective_global_xid)
    }

    /// Release this participant after the coordinator has durably committed the
    /// supplied global xid. This only releases local locks and session state; it
    /// does not write a local commit or the global decision.
    ///
    /// # Errors
    ///
    /// Returns `ObjectNotInPrerequisiteState` if this session is not prepared for
    /// `global_xid` or if the global decision is not durably committed.
    pub async fn release_global_participant_commit(
        &mut self,
        global_xid: u64,
    ) -> Result<(), ExecError> {
        self.ensure_prepared_for_global_xid(global_xid)?;
        self.ensure_global_decision_is(global_xid, XidStatus::Committed)
            .await?;
        self.commit_release();
        Ok(())
    }

    /// Global xid currently owned by this prepared participant session.
    #[must_use]
    pub const fn prepared_global_xid(&self) -> Option<u64> {
        self.global_xid
    }

    /// Release this participant after the coordinator has durably aborted the
    /// supplied global xid. This only releases local locks and session state; it
    /// does not write a local abort or the global decision.
    ///
    /// # Errors
    ///
    /// Returns `ObjectNotInPrerequisiteState` if this session is not prepared for
    /// `global_xid` or if the global decision is not durably aborted.
    pub async fn release_global_participant_abort(
        &mut self,
        global_xid: u64,
    ) -> Result<(), ExecError> {
        self.ensure_prepared_for_global_xid(global_xid)?;
        self.ensure_global_decision_is(global_xid, XidStatus::Aborted)
            .await?;
        self.abort_release();
        Ok(())
    }

    fn ensure_prepared_for_global_xid(&self, global_xid: u64) -> Result<(), ExecError> {
        let Some(prepared_global_xid) = self.global_xid else {
            return Err(ExecError::ObjectNotInPrerequisiteState(
                "global participant release requires a prepared transaction".into(),
            ));
        };
        if prepared_global_xid != global_xid {
            return Err(ExecError::ObjectNotInPrerequisiteState(format!(
                "global participant prepared for xid {prepared_global_xid}, not {global_xid}"
            )));
        }
        if !matches!(self.state, TxnState::Prepared(_)) {
            return Err(ExecError::ObjectNotInPrerequisiteState(
                "global participant release requires an externally prepared transaction".into(),
            ));
        }
        Ok(())
    }

    async fn ensure_global_decision_is(
        &self,
        global_xid: u64,
        expected: XidStatus,
    ) -> Result<(), ExecError> {
        self.ensure_global_readable().await?;
        let actual = crabka_pgmvcc::clog::get(self.catalog_kv.as_ref(), global_xid)?;
        if actual == expected {
            return Ok(());
        }
        Err(ExecError::ObjectNotInPrerequisiteState(format!(
            "global participant xid {global_xid} decision is {actual:?}, not {expected:?}"
        )))
    }

    /// Enlist this session as a participant of global txn `g`. If it has already
    /// done a write (local xid `Li` allocated), write the `Prepared(Li -> g)`
    /// marker durably AND deregister `Li` from the ProcArray running-set so the
    /// local snapshot no longer gates its rows — range 0's global clog becomes the
    /// sole arbiter, which is what makes both ranges flip visible atomically at
    /// the single `Committed(g)` instant (the deregister-at-PREPARE linchpin). If
    /// no write has happened yet there is nothing to backfill: the first write's
    /// commit batch (see `run_write`) carries the marker and deregisters then.
    /// Idempotent. The stamped marker is FENCED to any in-doubt global decision
    /// already governing this txn's written rows (`effective_global_xid` — SP24
    /// abort atomicity), so a failover re-stage never mints a second version under a
    /// competing decision.
    pub(crate) async fn join_global(&mut self, g: u64) -> Result<u64, ExecError> {
        self.global_xid = Some(g);
        if let Some(local) = self.local_xid() {
            let eff = self.effective_global_xid(g)?;
            self.global_xid = Some(eff);
            self.committer
                .commit(vec![crabka_pgmvcc::clog::put_op(
                    local,
                    XidStatus::Prepared(eff),
                )])
                .await?;
            self.procarray.finish(local); // deregister-at-PREPARE (the atomicity linchpin)
        }
        Ok(self.global_xid.unwrap_or(g))
    }

    /// Release this participant's resources after the coordinator's global COMMIT.
    /// The rows are already `Prepared` + durable and their local xid is already
    /// deregistered, so the single `Committed(g)` write makes them visible; here
    /// we only free row locks and reset to Idle (NO per-participant clog write).
    fn commit_release(&mut self) {
        self.guc.commit();
        self.finish_current_txn();
    }

    /// Release this participant's resources after the coordinator's global ABORT.
    /// The rows stay invisible (range 0's global clog is absent/`Aborted(g)`); we
    /// only free row locks and reset to Idle (NO per-participant clog write).
    fn abort_release(&mut self) {
        self.guc.rollback();
        self.finish_current_txn();
    }

    /// Deregister the current txn's xid from the ProcArray and free its row locks,
    /// then reset to Idle. Writes NO clog entry — used by `Drop` (presumed-abort
    /// on disconnect) and by the global participant `commit_release`/`abort_release`
    /// (the decision was recorded once, globally, by the coordinator).
    fn finish_current_txn(&mut self) {
        if let Some(xid) = self.local_xid() {
            self.procarray.finish(xid);
            self.lockmgr.release_all(xid);
        }
        self.global_xid = None;
        self.state = TxnState::Idle;
    }
}

impl Drop for SqlSession {
    /// A connection dropped mid-transaction (client disconnect) must not leak
    /// its xid in the ProcArray, nor leave its row locks held forever (which
    /// would hang any writer blocked on them). Deregister the xid so it stops
    /// pinning snapshots' xmin, and free its row locks. The uncommitted versions
    /// stay invisible (no clog Committed entry). This is presumed-abort: a global
    /// participant dropped before the coordinator's decision releases its locks
    /// and its rows never become visible (range 0's global clog has no
    /// `Committed(g)`).
    fn drop(&mut self) {
        self.finish_current_txn();
    }
}

fn parse_single_extended_statement(sql: &str) -> Result<Statement, PgError> {
    let statements = crabka_pgparser::parse(sql).map_err(|e| ExecError::from(e).into_pg())?;
    match statements.as_slice() {
        [] => Err(PgError::error(
            sqlstate::SYNTAX_ERROR,
            "empty prepared statement",
        )),
        [stmt] => Ok(stmt.clone()),
        _ => Err(PgError::error(
            sqlstate::SYNTAX_ERROR,
            "cannot insert multiple commands into a prepared statement",
        )),
    }
}

fn parse_single_copy_statement(sql: &str) -> Result<Option<CopyStmt>, PgError> {
    let statements = crabka_pgparser::parse(sql).map_err(|e| ExecError::from(e).into_pg())?;
    match statements.as_slice() {
        [Statement::Set { name, value, .. }]
            if name == crabka_pgparser::ast::COPY_FROM_STDIN_SENTINEL =>
        {
            decode_copy_stmt(value).map(Some)
        }
        [_] => Ok(None),
        [] => Ok(None),
        statements if statements.iter().any(is_copy_sentinel) => Err(PgError::error(
            sqlstate::SYNTAX_ERROR,
            "COPY FROM STDIN must be the only statement in a simple query",
        )),
        _ => Ok(None),
    }
}

fn is_copy_sentinel(stmt: &Statement) -> bool {
    matches!(stmt, Statement::Set { name, .. } if name == crabka_pgparser::ast::COPY_FROM_STDIN_SENTINEL)
}

/// Decode the COPY statement carried by a parsed COPY FROM STDIN sentinel;
/// `None` for any other statement.
fn copy_sentinel_stmt(stmt: &Statement) -> Result<Option<CopyStmt>, PgError> {
    match stmt {
        Statement::Set { name, value, .. }
            if name == crabka_pgparser::ast::COPY_FROM_STDIN_SENTINEL =>
        {
            decode_copy_stmt(value).map(Some)
        }
        _ => Ok(None),
    }
}

fn decode_copy_stmt(value: &crabka_pgparser::ast::SetValue) -> Result<CopyStmt, PgError> {
    let crabka_pgparser::ast::SetValue::Value(encoded) = value else {
        return Err(PgError::error(
            sqlstate::SYNTAX_ERROR,
            "invalid COPY statement",
        ));
    };
    let mut parts = encoded.split('\t');
    let format = match parts.next() {
        Some("text") => CopyFormat::Text,
        Some("csv") => CopyFormat::Csv,
        _ => {
            return Err(PgError::error(
                sqlstate::SYNTAX_ERROR,
                "invalid COPY format",
            ));
        }
    };
    let table = parts
        .next()
        .map(decode_copy_part)
        .transpose()?
        .ok_or_else(|| PgError::error(sqlstate::SYNTAX_ERROR, "invalid COPY table"))?;
    let columns = parts
        .next()
        .filter(|columns| !columns.is_empty())
        .map(|columns| {
            columns
                .split(',')
                .map(decode_copy_part)
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;
    if parts.next().is_some() {
        return Err(PgError::error(
            sqlstate::SYNTAX_ERROR,
            "invalid COPY statement",
        ));
    }
    Ok(CopyStmt {
        table,
        columns,
        format,
    })
}

fn decode_copy_part(value: &str) -> Result<String, PgError> {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        let Some(escaped) = chars.next() else {
            return Err(PgError::error(
                sqlstate::SYNTAX_ERROR,
                "invalid COPY escape",
            ));
        };
        out.push(match escaped {
            't' => '\t',
            ',' => ',',
            '\\' => '\\',
            other => other,
        });
    }
    Ok(out)
}

struct ParamBinder<'a> {
    catalog_kv: &'a dyn Kv,
    params: &'a [BoundParam],
    time_zone: &'a jiff::tz::TimeZone,
    inferred_param_types: RefCell<Vec<Option<ColumnType>>>,
}

impl ParamBinder<'_> {
    fn bind_statement_params(&self, stmt: &mut Statement) -> Result<(), PgError> {
        let required = max_statement_param(stmt);
        if self.params.len() != required {
            return Err(PgError::protocol(format!(
                "bind message supplies {} parameters, but prepared statement requires {required}",
                self.params.len()
            )));
        }

        match stmt {
            Statement::Insert {
                table,
                columns,
                rows,
                returning,
            } => {
                let target_types = self.insert_target_types(table, columns.as_ref())?;
                for row in rows {
                    for (idx, expr) in row.iter_mut().enumerate() {
                        self.bind_expr(expr, target_types.get(idx).copied())?;
                    }
                }
                if let Some(returning) = returning {
                    let table = crabka_pgcatalog::get_table(self.catalog_kv, table)
                        .map_err(ExecError::from)
                        .map_err(ExecError::into_pg)?;
                    let scope = crate::scope::Scope::single(&table, &table.name);
                    self.bind_returning(returning, &scope)?;
                }
            }
            Statement::Query(q) => self.bind_query_expr(q)?,
            Statement::Update {
                table,
                assignments,
                filter,
                returning,
            } => {
                let table = crabka_pgcatalog::get_table(self.catalog_kv, table)
                    .map_err(ExecError::from)
                    .map_err(ExecError::into_pg)?;
                let scope = crate::scope::Scope::single(&table, &table.name);
                for (column, expr) in assignments {
                    let Some(idx) = table.column_index(column) else {
                        return Err(ExecError::UndefinedColumn(column.clone()).into_pg());
                    };
                    // Bind with the table scope so a parameter next to a
                    // column reference (`balance = balance + $1`) infers its
                    // type from that column.
                    self.bind_expr_with_scope(expr, Some(table.columns[idx].ty), &scope)?;
                }
                if let Some(expr) = filter {
                    self.bind_expr_with_scope(expr, Some(ColumnType::Bool), &scope)?;
                }
                if let Some(returning) = returning {
                    self.bind_returning(returning, &scope)?;
                }
            }
            Statement::Delete {
                table,
                filter,
                returning,
            } => {
                let table = crabka_pgcatalog::get_table(self.catalog_kv, table)
                    .map_err(ExecError::from)
                    .map_err(ExecError::into_pg)?;
                let scope = crate::scope::Scope::single(&table, &table.name);
                if let Some(expr) = filter {
                    self.bind_expr_with_scope(expr, Some(ColumnType::Bool), &scope)?;
                }
                if let Some(returning) = returning {
                    self.bind_returning(returning, &scope)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn resolved_param_types(&self) -> Result<Vec<u32>, PgError> {
        self.params
            .iter()
            .zip(self.inferred_param_types.borrow().iter())
            .map(|(param, inferred)| match param.type_oid {
                // PostgreSQL coerces a parameter whose type is still unknown
                // after Parse to text; ParameterDescription never reports
                // OID 0. The resolved type also feeds Bind (`typed_params`),
                // so execution decodes the value as text too.
                Some(0) | None => Ok(inferred.map_or(crabka_pgtypes::oids::TEXT, ColumnType::oid)),
                Some(oid) => {
                    param_column_type(param)?;
                    Ok(oid)
                }
            })
            .collect()
    }

    fn bind_returning(
        &self,
        returning: &mut [SelectItem],
        scope: &crate::scope::Scope,
    ) -> Result<(), PgError> {
        for item in returning {
            if let SelectItem::Expr { expr, .. } = item {
                self.bind_expr_with_scope(expr, None, scope)?;
            }
        }
        Ok(())
    }

    fn insert_target_types(
        &self,
        table: &str,
        columns: Option<&Vec<String>>,
    ) -> Result<Vec<ColumnType>, PgError> {
        let table = crabka_pgcatalog::get_table(self.catalog_kv, table)
            .map_err(ExecError::from)
            .map_err(ExecError::into_pg)?;
        let target_idx = match columns {
            None => (0..table.columns.len()).collect::<Vec<_>>(),
            Some(columns) => columns
                .iter()
                .map(|column| {
                    table
                        .column_index(column)
                        .ok_or_else(|| ExecError::UndefinedColumn(column.clone()).into_pg())
                })
                .collect::<Result<Vec<_>, _>>()?,
        };
        Ok(target_idx
            .into_iter()
            .map(|idx| table.columns[idx].ty)
            .collect())
    }

    fn bind_query_expr(&self, q: &mut QueryExpr) -> Result<(), PgError> {
        let ctes = crate::cte::CteContext::empty();
        self.bind_query_expr_with_ctes(q, &ctes)
    }

    fn bind_query_expr_with_ctes(
        &self,
        q: &mut QueryExpr,
        parent_ctes: &crate::cte::CteContext,
    ) -> Result<(), PgError> {
        let query_ctes = self.bind_with_clause(q.with.as_mut(), parent_ctes)?;
        self.bind_set_expr_with_ctes(&mut q.body, &query_ctes)?;
        for item in &mut q.order_by {
            self.bind_expr_with_scope_and_ctes(
                &mut item.expr,
                None,
                &crate::scope::Scope::empty(),
                &query_ctes,
            )?;
        }
        Ok(())
    }

    fn bind_with_clause(
        &self,
        with: Option<&mut crabka_pgparser::ast::WithClause>,
        parent_ctes: &crate::cte::CteContext,
    ) -> Result<crate::cte::CteContext, PgError> {
        let Some(with) = with else {
            return Ok(parent_ctes.child());
        };
        crate::cte::reject_recursive(with).map_err(ExecError::into_pg)?;

        let mut ctes = parent_ctes.child();
        for cte in &mut with.ctes {
            self.bind_query_expr_with_ctes(&mut cte.query, &ctes)?;
            let relation = crate::cte::describe_cte_relation(self.catalog_kv, cte, &ctes)
                .map_err(ExecError::into_pg)?;
            ctes.insert(cte.name.clone(), relation);
        }
        Ok(ctes)
    }

    fn bind_set_expr_with_ctes(
        &self,
        set: &mut SetExpr,
        ctes: &crate::cte::CteContext,
    ) -> Result<(), PgError> {
        match set {
            SetExpr::Query(QueryBody::Select(select)) => {
                let scope = if select.from.is_empty() {
                    crate::scope::Scope::empty()
                } else {
                    crate::exec::build_from_schema_with_ctes(self.catalog_kv, &select.from, ctes)
                        .map_err(ExecError::into_pg)?
                        .scope
                };
                for item in &mut select.projection {
                    if let SelectItem::Expr { expr, .. } = item {
                        self.bind_expr_with_scope_and_ctes(expr, None, &scope, ctes)?;
                    }
                }
                for table in &mut select.from {
                    self.bind_table_expr_with_ctes(table, ctes)?;
                }
                if let Some(expr) = &mut select.filter {
                    self.bind_expr_with_scope_and_ctes(expr, Some(ColumnType::Bool), &scope, ctes)?;
                }
                for expr in &mut select.group_by {
                    self.bind_expr_with_scope_and_ctes(expr, None, &scope, ctes)?;
                }
                if let Some(expr) = &mut select.having {
                    self.bind_expr_with_scope_and_ctes(expr, Some(ColumnType::Bool), &scope, ctes)?;
                }
                for item in &mut select.order_by {
                    self.bind_expr_with_scope_and_ctes(&mut item.expr, None, &scope, ctes)?;
                }
            }
            SetExpr::Query(QueryBody::Values(values)) => {
                for row in &mut values.rows {
                    for expr in row {
                        self.bind_expr_with_scope_and_ctes(
                            expr,
                            None,
                            &crate::scope::Scope::empty(),
                            ctes,
                        )?;
                    }
                }
            }
            SetExpr::Query(QueryBody::Nested(query)) => {
                self.bind_query_expr_with_ctes(query, ctes)?;
            }
            SetExpr::SetOp { left, right, .. } => {
                self.bind_set_expr_with_ctes(left, ctes)?;
                self.bind_set_expr_with_ctes(right, ctes)?;
            }
        }
        Ok(())
    }

    fn bind_table_expr_with_ctes(
        &self,
        table: &mut TableExpr,
        ctes: &crate::cte::CteContext,
    ) -> Result<(), PgError> {
        match table {
            TableExpr::Table { .. } => Ok(()),
            TableExpr::Derived { subquery, .. } => self.bind_query_expr_with_ctes(subquery, ctes),
            TableExpr::Join {
                left,
                right,
                constraint,
                ..
            } => {
                self.bind_table_expr_with_ctes(left, ctes)?;
                self.bind_table_expr_with_ctes(right, ctes)?;
                if let JoinConstraint::On(expr) = constraint {
                    self.bind_expr_with_scope_and_ctes(
                        expr,
                        Some(ColumnType::Bool),
                        &crate::scope::Scope::empty(),
                        ctes,
                    )?;
                }
                Ok(())
            }
        }
    }

    fn bind_expr(&self, expr: &mut Expr, expected: Option<ColumnType>) -> Result<(), PgError> {
        self.bind_expr_with_scope_and_ctes(
            expr,
            expected,
            &crate::scope::Scope::empty(),
            &crate::cte::CteContext::empty(),
        )
    }

    /// A `regclass` parameter whose text value is a relation name resolves via
    /// the catalog at bind time (PostgreSQL's `regclassin`); numeric, binary,
    /// and NULL values return `None` and take the ordinary decode path.
    fn regclass_param_expr(
        &self,
        param: &BoundParam,
        expected: Option<ColumnType>,
    ) -> Result<Option<Expr>, PgError> {
        let ty = param_column_type(param)?
            .or(expected)
            .unwrap_or(ColumnType::Text);
        if ty != ColumnType::Regclass || param.format != 0 {
            return Ok(None);
        }
        let Some(value) = &param.value else {
            return Ok(None);
        };
        let text = std::str::from_utf8(value).map_err(invalid_parameter_encoding)?;
        if text.trim().parse::<i32>().is_ok() {
            return Ok(None);
        }
        let oid =
            crate::exec::resolve_regclass(self.catalog_kv, text).map_err(ExecError::into_pg)?;
        Ok(Some(Expr::Const {
            value: Datum::Int4(oid),
            ty: ColumnType::Regclass,
        }))
    }

    fn bind_expr_with_scope(
        &self,
        expr: &mut Expr,
        expected: Option<ColumnType>,
        scope: &crate::scope::Scope,
    ) -> Result<(), PgError> {
        self.bind_expr_with_scope_and_ctes(expr, expected, scope, &crate::cte::CteContext::empty())
    }

    fn bind_expr_with_scope_and_ctes(
        &self,
        expr: &mut Expr,
        expected: Option<ColumnType>,
        scope: &crate::scope::Scope,
        ctes: &crate::cte::CteContext,
    ) -> Result<(), PgError> {
        match expr {
            Expr::Param(number) => {
                let index = param_index(*number)?;
                let param = self.params.get(index).ok_or_else(|| {
                    PgError::error(
                        sqlstate::UNDEFINED_PARAMETER,
                        format!("there is no parameter ${number}"),
                    )
                })?;
                if param.type_oid.is_none() {
                    self.inferred_param_types.borrow_mut()[index] = expected;
                }
                *expr = self
                    .regclass_param_expr(param, expected)?
                    .map_or_else(|| bound_param_expr(param, expected, self.time_zone), Ok)?;
            }
            Expr::Unary { op, expr } => {
                let child_expected = match op {
                    UnaryOp::Not => Some(ColumnType::Bool),
                    UnaryOp::Neg => expected,
                };
                self.bind_expr_with_scope_and_ctes(expr, child_expected, scope, ctes)?;
            }
            Expr::Cast { expr, ty } => {
                self.bind_expr_with_scope_and_ctes(expr, Some(*ty), scope, ctes)?;
            }
            Expr::IsNull { expr, .. } => {
                self.bind_expr_with_scope_and_ctes(expr, None, scope, ctes)?;
            }
            Expr::Binary { op, left, right } => {
                let left_expected =
                    binary_param_type(*op, right, scope).or(expected_for_binary(*op));
                let right_expected =
                    binary_param_type(*op, left, scope).or(expected_for_binary(*op));
                self.bind_expr_with_scope_and_ctes(left, left_expected, scope, ctes)?;
                self.bind_expr_with_scope_and_ctes(right, right_expected, scope, ctes)?;
            }
            Expr::Func(func) => {
                if let FuncArgs::Exprs(args) = &mut func.args {
                    for arg in args {
                        self.bind_expr_with_scope_and_ctes(arg, None, scope, ctes)?;
                    }
                }
            }
            Expr::InList { expr, list, .. } => {
                let item_type = infer_param_context_type(expr, scope);
                self.bind_expr_with_scope_and_ctes(expr, None, scope, ctes)?;
                for item in list {
                    self.bind_expr_with_scope_and_ctes(item, item_type, scope, ctes)?;
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                let bound_type = infer_param_context_type(expr, scope);
                self.bind_expr_with_scope_and_ctes(expr, None, scope, ctes)?;
                self.bind_expr_with_scope_and_ctes(low, bound_type, scope, ctes)?;
                self.bind_expr_with_scope_and_ctes(high, bound_type, scope, ctes)?;
            }
            Expr::Like { expr, pattern, .. } => {
                self.bind_expr_with_scope_and_ctes(expr, Some(ColumnType::Text), scope, ctes)?;
                self.bind_expr_with_scope_and_ctes(pattern, Some(ColumnType::Text), scope, ctes)?;
            }
            Expr::Case {
                operand,
                whens,
                else_result,
            } => {
                if let Some(expr) = operand {
                    self.bind_expr_with_scope_and_ctes(expr, None, scope, ctes)?;
                }
                for (when, then) in whens {
                    self.bind_expr_with_scope_and_ctes(when, None, scope, ctes)?;
                    self.bind_expr_with_scope_and_ctes(then, None, scope, ctes)?;
                }
                if let Some(expr) = else_result {
                    self.bind_expr_with_scope_and_ctes(expr, expected, scope, ctes)?;
                }
            }
            Expr::ScalarSubquery(query) | Expr::Exists(query) => {
                self.bind_query_expr_with_ctes(query, ctes)?;
            }
            Expr::InSubquery { expr, subquery, .. } | Expr::Quantified { expr, subquery, .. } => {
                self.bind_expr_with_scope_and_ctes(expr, None, scope, ctes)?;
                self.bind_query_expr_with_ctes(subquery, ctes)?;
            }
            Expr::IntLiteral(_)
            | Expr::NumericLiteral(_)
            | Expr::StringLiteral(_)
            | Expr::BoolLiteral(_)
            | Expr::NullLiteral
            | Expr::Default
            | Expr::Column { .. }
            | Expr::Const { .. } => {}
        }
        Ok(())
    }
}

fn param_index(number: u32) -> Result<usize, PgError> {
    if number == 0 {
        return Err(PgError::error(
            sqlstate::UNDEFINED_PARAMETER,
            "parameter numbers start at $1",
        ));
    }
    usize::try_from(number - 1).map_err(|_| {
        PgError::error(
            sqlstate::UNDEFINED_PARAMETER,
            format!("parameter number ${number} is too large"),
        )
    })
}

fn expected_for_binary(op: BinaryOp) -> Option<ColumnType> {
    match op {
        BinaryOp::And | BinaryOp::Or => Some(ColumnType::Bool),
        _ => None,
    }
}

fn binary_param_type(
    op: BinaryOp,
    other: &Expr,
    scope: &crate::scope::Scope,
) -> Option<ColumnType> {
    match op {
        // Comparisons and arithmetic take same-family operands, so a
        // parameter adopts its sibling's type — matching PostgreSQL's
        // operator resolution for `int8 + $1` and friends.
        BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Le
        | BinaryOp::Gt
        | BinaryOp::Ge
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div => infer_param_context_type(other, scope),
        BinaryOp::Concat => Some(ColumnType::Text),
        BinaryOp::And | BinaryOp::Or => None,
    }
}

fn infer_param_context_type(expr: &Expr, scope: &crate::scope::Scope) -> Option<ColumnType> {
    match expr {
        Expr::Column { table, name } => scope
            .resolve(table.as_deref(), name)
            .ok()
            .map(|idx| scope.ty_at(idx)),
        Expr::IntLiteral(_) => Some(ColumnType::Int4),
        Expr::StringLiteral(_) => Some(ColumnType::Text),
        Expr::BoolLiteral(_) => Some(ColumnType::Bool),
        Expr::Default => None,
        Expr::Const { ty, .. } | Expr::Cast { ty, .. } => Some(*ty),
        Expr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => infer_param_context_type(expr, scope),
        _ => None,
    }
}

fn max_statement_param(stmt: &Statement) -> usize {
    let mut max = 0;
    match stmt {
        Statement::Insert {
            rows, returning, ..
        } => {
            for row in rows {
                for expr in row {
                    collect_expr_param(expr, &mut max);
                }
            }
            collect_returning_param(returning.as_deref(), &mut max);
        }
        Statement::Query(q) => collect_query_param(q, &mut max),
        Statement::Update {
            assignments,
            filter,
            returning,
            ..
        } => {
            for (_, expr) in assignments {
                collect_expr_param(expr, &mut max);
            }
            if let Some(expr) = filter {
                collect_expr_param(expr, &mut max);
            }
            collect_returning_param(returning.as_deref(), &mut max);
        }
        Statement::Delete {
            filter, returning, ..
        } => {
            if let Some(expr) = filter {
                collect_expr_param(expr, &mut max);
            }
            collect_returning_param(returning.as_deref(), &mut max);
        }
        _ => {}
    }
    max
}

fn collect_returning_param(returning: Option<&[SelectItem]>, max: &mut usize) {
    let Some(returning) = returning else {
        return;
    };

    for item in returning {
        if let SelectItem::Expr { expr, .. } = item {
            collect_expr_param(expr, max);
        }
    }
}

fn collect_query_param(q: &QueryExpr, max: &mut usize) {
    if let Some(with) = &q.with {
        for cte in &with.ctes {
            collect_query_param(&cte.query, max);
        }
    }
    collect_set_param(&q.body, max);
    for item in &q.order_by {
        collect_expr_param(&item.expr, max);
    }
}

fn collect_set_param(set: &SetExpr, max: &mut usize) {
    match set {
        SetExpr::Query(QueryBody::Select(select)) => {
            for item in &select.projection {
                if let SelectItem::Expr { expr, .. } = item {
                    collect_expr_param(expr, max);
                }
            }
            for table in &select.from {
                collect_table_param(table, max);
            }
            if let Some(expr) = &select.filter {
                collect_expr_param(expr, max);
            }
            for expr in &select.group_by {
                collect_expr_param(expr, max);
            }
            if let Some(expr) = &select.having {
                collect_expr_param(expr, max);
            }
            for item in &select.order_by {
                collect_expr_param(&item.expr, max);
            }
        }
        SetExpr::Query(QueryBody::Values(values)) => {
            for row in &values.rows {
                for expr in row {
                    collect_expr_param(expr, max);
                }
            }
        }
        SetExpr::Query(QueryBody::Nested(query)) => collect_query_param(query, max),
        SetExpr::SetOp { left, right, .. } => {
            collect_set_param(left, max);
            collect_set_param(right, max);
        }
    }
}

fn collect_table_param(table: &TableExpr, max: &mut usize) {
    match table {
        TableExpr::Table { .. } => {}
        TableExpr::Derived { subquery, .. } => collect_query_param(subquery, max),
        TableExpr::Join {
            left,
            right,
            constraint,
            ..
        } => {
            collect_table_param(left, max);
            collect_table_param(right, max);
            if let JoinConstraint::On(expr) = constraint {
                collect_expr_param(expr, max);
            }
        }
    }
}

fn collect_expr_param(expr: &Expr, max: &mut usize) {
    match expr {
        Expr::Param(number) => {
            if let Ok(index) = param_index(*number) {
                *max = (*max).max(index + 1);
            }
        }
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } | Expr::IsNull { expr, .. } => {
            collect_expr_param(expr, max);
        }
        Expr::Binary { left, right, .. } => {
            collect_expr_param(left, max);
            collect_expr_param(right, max);
        }
        Expr::Func(func) => {
            if let FuncArgs::Exprs(args) = &func.args {
                for arg in args {
                    collect_expr_param(arg, max);
                }
            }
        }
        Expr::InList { expr, list, .. } => {
            collect_expr_param(expr, max);
            for item in list {
                collect_expr_param(item, max);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_expr_param(expr, max);
            collect_expr_param(low, max);
            collect_expr_param(high, max);
        }
        Expr::Like { expr, pattern, .. } => {
            collect_expr_param(expr, max);
            collect_expr_param(pattern, max);
        }
        Expr::Case {
            operand,
            whens,
            else_result,
        } => {
            if let Some(expr) = operand {
                collect_expr_param(expr, max);
            }
            for (when, then) in whens {
                collect_expr_param(when, max);
                collect_expr_param(then, max);
            }
            if let Some(expr) = else_result {
                collect_expr_param(expr, max);
            }
        }
        Expr::ScalarSubquery(query) | Expr::Exists(query) => collect_query_param(query, max),
        Expr::InSubquery { expr, subquery, .. } | Expr::Quantified { expr, subquery, .. } => {
            collect_expr_param(expr, max);
            collect_query_param(subquery, max);
        }
        Expr::IntLiteral(_)
        | Expr::NumericLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BoolLiteral(_)
        | Expr::NullLiteral
        | Expr::Default
        | Expr::Column { .. }
        | Expr::Const { .. } => {}
    }
}

fn bound_param_expr(
    param: &BoundParam,
    expected: Option<ColumnType>,
    time_zone: &jiff::tz::TimeZone,
) -> Result<Expr, PgError> {
    let ty = param_column_type(param)?
        .or(expected)
        .unwrap_or(ColumnType::Text);
    let Some(value) = &param.value else {
        return Ok(Expr::Const {
            value: Datum::Null,
            ty,
        });
    };
    let value = decode_bound_param(value, param, ty, time_zone)?;
    Ok(Expr::Const { value, ty })
}

fn param_column_type(param: &BoundParam) -> Result<Option<ColumnType>, PgError> {
    match param.type_oid {
        // The executor stores integral values as Int4/Int8 and floating-point values
        // as Float8. Decode the narrower wire representations at the Bind boundary.
        Some(crabka_pgtypes::oids::INT2) => Ok(Some(ColumnType::Int4)),
        Some(crabka_pgtypes::oids::INT4) => Ok(Some(ColumnType::Int4)),
        // `oid` aliases Int4 (see ColumnType::from_sql_name): drivers declare
        // OID parameters in their pg_catalog typeinfo lookups.
        Some(crabka_pgtypes::oids::OID) => Ok(Some(ColumnType::Int4)),
        Some(crabka_pgtypes::oids::REGCLASS) => Ok(Some(ColumnType::Regclass)),
        Some(crabka_pgtypes::oids::INT8) => Ok(Some(ColumnType::Int8)),
        Some(crabka_pgtypes::oids::TEXT) => Ok(Some(ColumnType::Text)),
        Some(crabka_pgtypes::oids::VARCHAR) => Ok(Some(ColumnType::Varchar(None))),
        Some(crabka_pgtypes::oids::BPCHAR) => Ok(Some(ColumnType::Char(None))),
        Some(crabka_pgtypes::oids::BOOL) => Ok(Some(ColumnType::Bool)),
        Some(crabka_pgtypes::oids::FLOAT4) => Ok(Some(ColumnType::Float8)),
        Some(crabka_pgtypes::oids::FLOAT8) => Ok(Some(ColumnType::Float8)),
        Some(crabka_pgtypes::oids::NUMERIC) => Ok(Some(ColumnType::Numeric(None))),
        Some(crabka_pgtypes::oids::BYTEA) => Ok(Some(ColumnType::Bytea)),
        Some(crabka_pgtypes::oids::UUID) => Ok(Some(ColumnType::Uuid)),
        Some(crabka_pgtypes::oids::DATE) => Ok(Some(ColumnType::Date)),
        Some(crabka_pgtypes::oids::TIME) => Ok(Some(ColumnType::Time)),
        Some(crabka_pgtypes::oids::TIMESTAMP) => Ok(Some(ColumnType::Timestamp)),
        Some(crabka_pgtypes::oids::TIMESTAMPTZ) => Ok(Some(ColumnType::Timestamptz)),
        Some(crabka_pgtypes::oids::INTERVAL) => Ok(Some(ColumnType::Interval)),
        Some(0) | None => Ok(None),
        Some(oid) => Err(PgError::error(
            "42P18",
            format!("could not determine data type of parameter with oid {oid}"),
        )),
    }
}

fn decode_bound_param(
    value: &[u8],
    param: &BoundParam,
    ty: ColumnType,
    time_zone: &jiff::tz::TimeZone,
) -> Result<Datum, PgError> {
    match param.type_oid {
        Some(crabka_pgtypes::oids::INT2) => return decode_int2_bound_param(value, param.format),
        Some(crabka_pgtypes::oids::FLOAT4) => {
            return decode_float4_bound_param(value, param.format);
        }
        _ => {}
    }

    match (param.format, ty) {
        (0, ty) => {
            let text = std::str::from_utf8(value).map_err(invalid_parameter_encoding)?;
            decode_text_bound_param(text, ty, time_zone)
                .map_err(ExecError::from)
                .map_err(ExecError::into_pg)
        }
        (1, ColumnType::Int4 | ColumnType::Regclass) => {
            let bytes = binary_array(value)?;
            Ok(Datum::Int4(i32::from_be_bytes(bytes)))
        }
        (1, ColumnType::Int8) => Ok(Datum::Int8(i64::from_be_bytes(binary_array(value)?))),
        (1, ColumnType::Bool) => match value {
            [0] => Ok(Datum::Bool(false)),
            [1] => Ok(Datum::Bool(true)),
            _ => Err(PgError::error(
                "22P03",
                "incorrect binary data format in bind parameter",
            )),
        },
        (1, ColumnType::Text) => {
            let text = std::str::from_utf8(value).map_err(invalid_parameter_encoding)?;
            Ok(Datum::Text(text.to_string()))
        }
        (1, ColumnType::Varchar(_) | ColumnType::Char(_)) => {
            let text = std::str::from_utf8(value).map_err(invalid_parameter_encoding)?;
            decode_text_bound_param(text, ty, time_zone)
                .map_err(ExecError::from)
                .map_err(ExecError::into_pg)
        }
        (1, ColumnType::Float8) => Ok(Datum::Float8(f64::from_be_bytes(binary_array(value)?))),
        (1, ColumnType::Numeric(_)) => crabka_pgtypes::numeric::from_binary(value)
            .map(Datum::Numeric)
            .ok_or_else(malformed_binary_parameter),
        (1, ColumnType::Bytea) => Ok(Datum::Bytea(value.to_vec())),
        (1, ColumnType::Uuid) => {
            let bytes: [u8; 16] = binary_array(value)?;
            Ok(Datum::Text(
                crabka_pgtypes::uuid::UuidBytes(bytes).to_canonical_text(),
            ))
        }
        (1, ColumnType::Date) => {
            let _: [u8; 4] = binary_array(value)?;
            crabka_pgtypes::datetime::date_from_binary(value)
                .map(Datum::Date)
                .map_err(ExecError::from)
                .map_err(ExecError::into_pg)
        }
        (1, ColumnType::Time) => {
            let _: [u8; 8] = binary_array(value)?;
            crabka_pgtypes::datetime::time_from_binary(value)
                .map(Datum::Time)
                .map_err(ExecError::from)
                .map_err(ExecError::into_pg)
        }
        (1, ColumnType::Timestamp) => {
            let _: [u8; 8] = binary_array(value)?;
            crabka_pgtypes::datetime::timestamp_from_binary(value)
                .map(Datum::Timestamp)
                .map_err(ExecError::from)
                .map_err(ExecError::into_pg)
        }
        (1, ColumnType::Timestamptz) => {
            let _: [u8; 8] = binary_array(value)?;
            crabka_pgtypes::datetime::timestamptz_from_binary(value)
                .map(Datum::Timestamptz)
                .map_err(ExecError::from)
                .map_err(ExecError::into_pg)
        }
        (1, ColumnType::Interval) => {
            let _: [u8; 16] = binary_array(value)?;
            crabka_pgtypes::datetime::interval_from_binary(value)
                .map(Datum::Interval)
                .map_err(ExecError::from)
                .map_err(ExecError::into_pg)
        }
        (format, _) => Err(PgError::protocol(format!(
            "invalid parameter format code {format}"
        ))),
    }
}

fn decode_int2_bound_param(value: &[u8], format: i16) -> Result<Datum, PgError> {
    match format {
        0 => {
            let text = std::str::from_utf8(value).map_err(invalid_parameter_encoding)?;
            if !has_integer_syntax(text) {
                return Err(PgError::error(
                    "22P02",
                    format!("invalid input syntax for type smallint: \"{text}\""),
                ));
            }
            text.trim()
                .parse::<i16>()
                .map(i32::from)
                .map(Datum::Int4)
                .map_err(|_| PgError::error("22003", "smallint out of range in bind parameter"))
        }
        1 => Ok(Datum::Int4(i32::from(i16::from_be_bytes(binary_array(
            value,
        )?)))),
        _ => Err(PgError::protocol(format!(
            "invalid parameter format code {format}"
        ))),
    }
}

fn decode_float4_bound_param(value: &[u8], format: i16) -> Result<Datum, PgError> {
    match format {
        0 => {
            let text = std::str::from_utf8(value).map_err(invalid_parameter_encoding)?;
            let text = text.trim();
            match text.parse::<f32>() {
                Ok(number) if number.is_infinite() && !is_infinity_spelling(text) => Err(
                    PgError::error("22003", "real out of range in bind parameter"),
                ),
                Ok(number) => Ok(Datum::Float8(f64::from(number))),
                Err(_) => Err(PgError::error(
                    "22P02",
                    format!("invalid input syntax for type real: \"{text}\""),
                )),
            }
        }
        1 => Ok(Datum::Float8(f64::from(f32::from_be_bytes(binary_array(
            value,
        )?)))),
        _ => Err(PgError::protocol(format!(
            "invalid parameter format code {format}"
        ))),
    }
}

fn is_infinity_spelling(value: &str) -> bool {
    let value = value.strip_prefix(['+', '-']).unwrap_or(value);
    value.eq_ignore_ascii_case("inf") || value.eq_ignore_ascii_case("infinity")
}

fn has_integer_syntax(value: &str) -> bool {
    let value = value.trim();
    let digits = value.strip_prefix(['+', '-']).unwrap_or(value);
    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

fn decode_text_bound_param(
    text: &str,
    ty: ColumnType,
    time_zone: &jiff::tz::TimeZone,
) -> Result<Datum, crabka_pgtypes::TypeError> {
    if ty == ColumnType::Bytea {
        return decode_bytea_text(text).map(Datum::Bytea);
    }
    crabka_pgtypes::cast::cast(&Datum::Text(text.to_string()), ty, time_zone)
}

pub(crate) fn decode_bytea_text(text: &str) -> Result<Vec<u8>, crabka_pgtypes::TypeError> {
    if let Some(hex) = text.strip_prefix("\\x") {
        return decode_bytea_hex(hex, text);
    }
    decode_bytea_escape(text)
}

fn decode_bytea_hex(hex: &str, original: &str) -> Result<Vec<u8>, crabka_pgtypes::TypeError> {
    if !hex.len().is_multiple_of(2) {
        return Err(crabka_pgtypes::TypeError::InvalidText {
            type_name: "bytea",
            value: original.to_string(),
        });
    }
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_digit(pair[0])?;
            let low = hex_digit(pair[1])?;
            Ok(high << 4 | low)
        })
        .collect()
}

fn decode_bytea_escape(text: &str) -> Result<Vec<u8>, crabka_pgtypes::TypeError> {
    let bytes = text.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while let Some(byte) = bytes.get(index) {
        if *byte != b'\\' {
            decoded.push(*byte);
            index += 1;
            continue;
        }
        let Some(escaped) = bytes.get(index + 1) else {
            return Err(invalid_bytea_text(text));
        };
        if *escaped == b'\\' {
            decoded.push(b'\\');
            index += 2;
            continue;
        }
        let [
            first @ b'0'..=b'7',
            second @ b'0'..=b'7',
            third @ b'0'..=b'7',
        ] = bytes.get(index + 1..index + 4).unwrap_or_default()
        else {
            return Err(invalid_bytea_text(text));
        };
        decoded.push((first - b'0') * 64 + (second - b'0') * 8 + (third - b'0'));
        index += 4;
    }
    Ok(decoded)
}

fn invalid_bytea_text(text: &str) -> crabka_pgtypes::TypeError {
    crabka_pgtypes::TypeError::InvalidText {
        type_name: "bytea",
        value: text.to_string(),
    }
}

fn hex_digit(byte: u8) -> Result<u8, crabka_pgtypes::TypeError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(crabka_pgtypes::TypeError::InvalidText {
            type_name: "bytea",
            value: char::from(byte).to_string(),
        }),
    }
}

fn binary_array<const N: usize>(value: &[u8]) -> Result<[u8; N], PgError> {
    value.try_into().map_err(|_| malformed_binary_parameter())
}

fn malformed_binary_parameter() -> PgError {
    PgError::error("22P03", "incorrect binary data format in bind parameter")
}

fn resolve_result_formats(requested: &[i16], count: usize) -> Result<Vec<i16>, PgError> {
    let validate = |value| match value {
        0 | 1 => Ok(value),
        _ => Err(PgError::protocol(format!("invalid format code {value}"))),
    };
    match requested.len() {
        0 => Ok(vec![0; count]),
        1 => Ok(vec![validate(requested[0])?; count]),
        n if n == count => requested.iter().copied().map(validate).collect(),
        n => Err(PgError::protocol(format!(
            "bind message has {n} result formats but query has {count} columns"
        ))),
    }
}

impl SqlSession {
    /// Validate a decoded COPY FROM STDIN statement and build the
    /// `CopyInResponse` the wire layer answers with before entering copy-in
    /// mode. Shared by the simple-protocol (`begin_copy_in`) and
    /// extended-protocol (`execute` on a COPY portal) start paths.
    fn copy_in_start(&mut self, copy: &CopyStmt) -> Result<CopyInResponse, PgError> {
        if matches!(self.state, TxnState::Failed(_)) {
            return Err(ExecError::InFailedTransaction.into_pg());
        }
        self.reject_prepared_participant()
            .map_err(ExecError::into_pg)?;
        if matches!(copy.format, CopyFormat::Csv) {
            return Err(ExecError::Unsupported("COPY CSV is not supported".into()).into_pg());
        }
        let table = crabka_pgcatalog::get_table(self.catalog_kv.as_ref(), &copy.table)
            .map_err(ExecError::from)
            .map_err(ExecError::into_pg)?;
        let target_count = match &copy.columns {
            Some(columns) => columns.len(),
            None => table.columns.len(),
        };
        if let Some(columns) = &copy.columns {
            for column in columns {
                if table.column_index(column).is_none() {
                    return Err(ExecError::UndefinedColumn(column.clone()).into_pg());
                }
            }
        }
        Ok(CopyInResponse {
            overall_format: 0,
            column_formats: vec![0; target_count],
        })
    }

    #[cfg(test)]
    async fn test_extended_query(
        &mut self,
        sql: &str,
        params: &[BoundParam],
    ) -> Result<Vec<QueryResult>, PgError> {
        let parameter_types = params
            .iter()
            .map(|p| p.type_oid.unwrap_or(0))
            .collect::<Vec<_>>();
        let description = self.parse("", sql, &parameter_types).await?;
        self.bind("", "", params, &[]).await?;
        match self.execute("", 0).await? {
            ExecuteOutcome::Rows { rows, completion } => Ok(vec![QueryResult::Rows {
                fields: description.fields,
                rows: rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|value| {
                                value.map(|bytes| crabka_pgwire::engine::Cell {
                                    text: bytes.clone(),
                                    binary: bytes,
                                })
                            })
                            .collect()
                    })
                    .collect(),
                tag: completion.unwrap_or_default(),
            }]),
            ExecuteOutcome::CommandComplete { tag } => Ok(vec![QueryResult::Command { tag }]),
            ExecuteOutcome::EmptyQuery => Ok(vec![QueryResult::Empty]),
            _ => unreachable!("reserved outcomes are not returned by SqlSession"),
        }
    }

    #[cfg(test)]
    pub(crate) async fn test_describe(
        &mut self,
        sql: &str,
    ) -> Result<Vec<FieldDescription>, PgError> {
        self.parse("", sql, &[]).await.map(|d| d.fields)
    }

    async fn stream_eligible_select<S: crabka_pgwire::engine::ResultSink>(
        &mut self,
        stmt: &Statement,
        result_index: usize,
        page_rows: usize,
        sink: &mut S,
    ) -> Option<Result<(), ExecError>> {
        let Statement::Query(query) = stmt else {
            return None;
        };
        let SetExpr::Query(QueryBody::Select(select)) = &query.body else {
            return None;
        };
        if query.with.is_some() || query.locking.is_some() {
            return None;
        }
        let mut select = (**select).clone();
        select.order_by = query.order_by.clone();
        select.limit = query.limit;
        select.offset = query.offset;
        select.locking = query.locking;
        let [TableExpr::Table { name, alias }] = select.from.as_slice() else {
            return None;
        };
        if select.distinct
            || !select.group_by.is_empty()
            || select.having.is_some()
            || !select.order_by.is_empty()
            || crate::agg::is_aggregate_query(&select)
        {
            return None;
        }
        let table = match crabka_pgcatalog::get_table(self.catalog_kv.as_ref(), name) {
            Ok(table) if table.foreign.is_none() => table,
            Ok(_) => return None,
            Err(error) => return Some(Err(error.into())),
        };

        Some(
            async {
                self.reject_prepared_participant()?;
                if matches!(self.state, TxnState::Failed(_)) {
                    return Err(ExecError::InFailedTransaction);
                }
                let (snapshot, own, global_snapshot) = self.read_context().await?;
                let read_ts = match &self.state {
                    TxnState::InTransaction(context) if context.repeatable_read => {
                        context.timestamp_read.ok_or_else(|| {
                            ExecError::Unsupported("repeatable-read timestamp is missing".into())
                        })?
                    }
                    TxnState::InTransaction(_) | TxnState::Idle => {
                        self.allocate_statement_read_timestamp().await?
                    }
                    TxnState::Prepared(_) | TxnState::Failed(_) => {
                        return Err(ExecError::InFailedTransaction);
                    }
                };
                // Statement-duration timestamp-domain pin — see `run_select`.
                let _ts_read_pin = self.ts_gc.pin_read(self.kv.as_ref(), read_ts)?;
                let scanner = crate::scanner::TimestampedRangeScanner::new(
                    Arc::clone(&self.range_scanner),
                    read_ts,
                );
                let qualifier = alias.as_deref().unwrap_or(&table.name);
                let scope = crate::scope::Scope::single(&table, qualifier);
                let (fields, expressions, _) =
                    crate::exec::resolve_projection(&select.projection, &scope)?;
                let mut plan =
                    crate::plan_dist::plan_scan(&table, select.filter.as_ref(), &select.projection);
                plan.projection = crate::ProjectionPushdown::All;
                plan.partial_aggregate = None;
                plan.top_k = None;
                let mut cursor = crate::scanner::RangeScanner::scan_cursor(
                    &scanner,
                    crate::scanner::ScanRequest {
                        local: self.kv.as_ref(),
                        global: self.catalog_kv.as_ref(),
                        global_snapshot: &global_snapshot,
                        snapshot: &snapshot,
                        own_xid: own,
                        read_ts: None,
                        own_start_ts: None,
                        table: &table,
                        interval: crate::scanner::RowInterval::ALL,
                        predicate: plan.predicate,
                        projection: plan.projection,
                        partial_aggregate: None,
                        top_k: None,
                    },
                )?;
                let ctx = self.eval_ctx();
                let mut offset =
                    usize::try_from(select.offset.unwrap_or(0).max(0)).unwrap_or(usize::MAX);
                let mut remaining = select
                    .limit
                    .map(|limit| usize::try_from(limit.max(0)).unwrap_or(usize::MAX));
                let mut fields = Some(fields);
                let mut emitted = 0usize;

                loop {
                    let page = cursor.next_page(page_rows).await?;
                    let mut source_rows = Vec::with_capacity(page.rows.len());
                    for scanned in page.rows {
                        if !crate::exec::row_matches(
                            select.filter.as_ref(),
                            &scope,
                            &scanned.row,
                            &ctx,
                        )? {
                            continue;
                        }
                        if offset > 0 {
                            offset -= 1;
                            continue;
                        }
                        if remaining == Some(0) {
                            break;
                        }
                        source_rows.push(scanned.row);
                        if let Some(remaining) = &mut remaining {
                            *remaining -= 1;
                        }
                    }
                    let projected =
                        crate::exec::project_rows(&expressions, &scope, &source_rows, &ctx)?;
                    let encoded =
                        match crate::exec::rows_result(Vec::new(), &projected, &ctx.time_zone) {
                            QueryResult::Rows { rows, .. } => rows,
                            _ => unreachable!("rows_result always returns rows"),
                        };
                    emitted = emitted.saturating_add(encoded.len());
                    let stopped = remaining == Some(0);
                    let is_last = page.is_last || stopped;
                    let mut chunks =
                        into_bounded_row_pages(encoded, page_rows, RESULT_PAGE_MAX_BYTES)
                            .peekable();
                    if chunks.peek().is_none() && is_last {
                        sink.send(crabka_pgwire::engine::ResultPage::Rows {
                            result_index,
                            fields: fields.take(),
                            rows: Vec::new(),
                            tag: Some(format!("SELECT {emitted}")),
                        })
                        .await
                        .map_err(ExecError::Remote)?;
                    }
                    while let Some(rows) = chunks.next() {
                        let rows = rows.map_err(ExecError::Remote)?;
                        let final_chunk = is_last && chunks.peek().is_none();
                        sink.send(crabka_pgwire::engine::ResultPage::Rows {
                            result_index,
                            fields: fields.take(),
                            rows,
                            tag: final_chunk.then(|| format!("SELECT {emitted}")),
                        })
                        .await
                        .map_err(ExecError::Remote)?;
                    }
                    if is_last {
                        break;
                    }
                }
                Ok(())
            }
            .await,
        )
    }

    #[cfg(test)]
    async fn test_describe_prepared(
        &mut self,
        sql: &str,
        params: &[u32],
    ) -> Result<(Vec<FieldDescription>, Vec<u32>), PgError> {
        self.parse("", sql, params)
            .await
            .map(|d| (d.fields, d.parameter_types))
    }
}

fn invalid_parameter_encoding(_: std::str::Utf8Error) -> PgError {
    PgError::error("22021", "invalid byte sequence for encoding \"UTF8\"")
}

impl Session for SqlSession {
    async fn simple_query(&mut self, sql: &str) -> Result<Vec<QueryResult>, PgError> {
        if sql.trim().is_empty() {
            return Ok(vec![QueryResult::Empty]);
        }
        let statements = crabka_pgparser::parse(sql).map_err(|e| ExecError::from(e).into_pg())?;
        if statements.is_empty() {
            return Ok(vec![QueryResult::Empty]);
        }
        let mut results = Vec::with_capacity(statements.len());
        for stmt in statements {
            results.push(self.run_one(&stmt).await.map_err(ExecError::into_pg)?);
        }
        Ok(results)
    }

    async fn simple_query_into<S: crabka_pgwire::engine::ResultSink>(
        &mut self,
        sql: &str,
        page_rows: usize,
        sink: &mut S,
    ) -> Result<(), PgError> {
        use crabka_pgwire::engine::ResultPage;

        if page_rows == 0 {
            return Err(PgError::protocol(
                "result page size must be greater than zero",
            ));
        }
        if sql.trim().is_empty() {
            return sink.send(ResultPage::Empty { result_index: 0 }).await;
        }
        let statements = crabka_pgparser::parse(sql).map_err(|e| ExecError::from(e).into_pg())?;
        if statements.is_empty() {
            return sink.send(ResultPage::Empty { result_index: 0 }).await;
        }
        for (result_index, stmt) in statements.iter().enumerate() {
            if let Some(result) = self
                .stream_eligible_select(stmt, result_index, page_rows, sink)
                .await
            {
                match result {
                    Ok(()) => {
                        if let TxnState::InTransaction(ctx) = &mut self.state {
                            ctx.activity_started = true;
                        }
                        continue;
                    }
                    Err(error) => {
                        self.mark_transaction_failed();
                        return Err(error.into_pg());
                    }
                }
            }
            match self.run_one(stmt).await.map_err(ExecError::into_pg)? {
                QueryResult::Rows { fields, rows, tag } => {
                    let mut fields = Some(fields);
                    if rows.is_empty() {
                        sink.send(ResultPage::Rows {
                            result_index,
                            fields,
                            rows,
                            tag: Some(tag),
                        })
                        .await?;
                        continue;
                    }
                    let mut pages =
                        into_bounded_row_pages(rows, page_rows, RESULT_PAGE_MAX_BYTES).peekable();
                    while let Some(rows) = pages.next() {
                        let rows = rows?;
                        let final_page = pages.peek().is_none();
                        sink.send(ResultPage::Rows {
                            result_index,
                            fields: fields.take(),
                            rows,
                            tag: final_page.then(|| tag.clone()),
                        })
                        .await?;
                    }
                }
                QueryResult::Command { tag } => {
                    sink.send(ResultPage::Command { result_index, tag }).await?;
                }
                QueryResult::Empty => {
                    sink.send(ResultPage::Empty { result_index }).await?;
                }
            }
        }
        Ok(())
    }

    async fn parse(
        &mut self,
        name: &str,
        sql: &str,
        param_types: &[u32],
    ) -> Result<PreparedDescription, PgError> {
        let result = async {
            if matches!(self.state, TxnState::Failed(_)) {
                return Err(ExecError::InFailedTransaction.into_pg());
            }
            if !name.is_empty() && self.prepared.contains_key(name) {
                return Err(PgError::error(
                    sqlstate::DUPLICATE_PREPARED_STATEMENT,
                    format!("prepared statement \"{name}\" already exists"),
                ));
            }
            if sql.trim().is_empty() {
                let description = PreparedDescription {
                    parameter_types: param_types.to_vec(),
                    fields: vec![],
                };
                self.prepared.insert(
                    name.to_owned(),
                    SqlPrepared {
                        statement: None,
                        description: description.clone(),
                    },
                );
                return Ok(description);
            }
            self.reject_prepared_participant()
                .map_err(ExecError::into_pg)?;
            let result = (|| {
                let statement = parse_single_extended_statement(sql)?;
                let mut inferred_statement = statement.clone();
                let parameter_count = max_statement_param(&statement).max(param_types.len());
                let params = (0..parameter_count)
                    .map(|index| BoundParam {
                        type_oid: match param_types.get(index).copied().unwrap_or(0) {
                            0 => None,
                            type_oid => Some(type_oid),
                        },
                        format: 0,
                        value: None,
                    })
                    .collect::<Vec<_>>();
                let timezone_name = self.guc.effective("timezone").map_err(ExecError::into_pg)?;
                let time_zone = if timezone_name.eq_ignore_ascii_case("UTC") {
                    jiff::tz::TimeZone::UTC
                } else {
                    jiff::tz::TimeZone::get(&timezone_name).map_err(|_| {
                        PgError::error(
                            "22023",
                            format!("invalid value for parameter: \"{timezone_name}\""),
                        )
                    })?
                };
                let binder = ParamBinder {
                    catalog_kv: &*self.catalog_kv,
                    params: &params,
                    time_zone: &time_zone,
                    inferred_param_types: RefCell::new(vec![None; params.len()]),
                };
                binder.bind_statement_params(&mut inferred_statement)?;
                let fields =
                    crate::exec::describe_statement(&*self.catalog_kv, &inferred_statement)
                        .map_err(ExecError::into_pg)?;
                let description = PreparedDescription {
                    fields,
                    parameter_types: binder.resolved_param_types()?,
                };
                Ok((statement, description))
            })();
            let (statement, description) = result?;
            self.prepared.insert(
                name.to_owned(),
                SqlPrepared {
                    statement: Some(statement),
                    description: description.clone(),
                },
            );
            Ok(description)
        }
        .await;
        if result.is_err() {
            self.mark_transaction_failed();
        }
        result
    }

    async fn bind(
        &mut self,
        portal: &str,
        statement: &str,
        params: &[BoundParam],
        result_formats: &[i16],
    ) -> Result<PortalDescription, PgError> {
        let result = async {
            if matches!(self.state, TxnState::Failed(_)) {
                return Err(ExecError::InFailedTransaction.into_pg());
            }
            if !portal.is_empty() && self.portals.contains_key(portal) {
                return Err(PgError::error(
                    sqlstate::DUPLICATE_CURSOR,
                    format!("cursor \"{portal}\" already exists"),
                ));
            }
            let prepared = self.prepared.get(statement).cloned().ok_or_else(|| {
                PgError::error(
                    sqlstate::INVALID_SQL_STATEMENT_NAME,
                    format!("prepared statement \"{statement}\" does not exist"),
                )
            })?;
            if params.len() != prepared.description.parameter_types.len() {
                return Err(PgError::protocol(format!(
                    "bind message supplies {} parameters, but prepared statement requires {}",
                    params.len(),
                    prepared.description.parameter_types.len()
                )));
            }
            let formats =
                resolve_result_formats(result_formats, prepared.description.fields.len())?;
            let mut bound = prepared.statement;
            let typed_params = params
                .iter()
                .zip(&prepared.description.parameter_types)
                .map(|(p, oid)| BoundParam {
                    type_oid: Some(*oid).filter(|v| *v != 0).or(p.type_oid),
                    ..p.clone()
                })
                .collect::<Vec<_>>();
            if let Some(stmt) = &mut bound {
                self.bind_extended_statement_params(stmt, &typed_params)?;
            }
            let description = PortalDescription {
                fields: prepared
                    .description
                    .fields
                    .iter()
                    .zip(&formats)
                    .map(|(f, &format)| FieldDescription {
                        format,
                        ..f.clone()
                    })
                    .collect(),
            };
            self.portals.insert(
                portal.to_owned(),
                SqlPortal {
                    statement: bound,
                    description: description.clone(),
                    formats,
                    execution: SqlPortalExecution::NotStarted,
                },
            );
            Ok(description)
        }
        .await;
        if result.is_err() {
            self.mark_transaction_failed();
        }
        result
    }

    async fn describe_statement(&mut self, name: &str) -> Result<PreparedDescription, PgError> {
        self.prepared
            .get(name)
            .map(|p| p.description.clone())
            .ok_or_else(|| {
                PgError::error(
                    sqlstate::INVALID_SQL_STATEMENT_NAME,
                    format!("prepared statement \"{name}\" does not exist"),
                )
            })
    }

    async fn describe_portal(&mut self, name: &str) -> Result<PortalDescription, PgError> {
        self.portals
            .get(name)
            .map(|p| p.description.clone())
            .ok_or_else(|| {
                PgError::error(
                    sqlstate::INVALID_CURSOR_NAME,
                    format!("portal \"{name}\" does not exist"),
                )
            })
    }

    async fn execute(&mut self, portal: &str, max_rows: u32) -> Result<ExecuteOutcome, PgError> {
        let needs_run = matches!(
            self.portals
                .get(portal)
                .ok_or_else(|| PgError::error(
                    sqlstate::INVALID_CURSOR_NAME,
                    format!("portal \"{portal}\" does not exist")
                ))?
                .execution,
            SqlPortalExecution::NotStarted
        );
        if needs_run {
            let statement = self.portals.get(portal).and_then(|p| p.statement.clone());
            if let Some(stmt) = &statement
                && let Some(copy) = copy_sentinel_stmt(stmt)?
            {
                // Extended-protocol COPY FROM STDIN: answer with a
                // CopyInResponse; the buffered rows arrive via
                // `copy_in_portal` after CopyDone.
                let response = self.copy_in_start(&copy)?;
                return Ok(ExecuteOutcome::CopyIn { response });
            }
            let execution = match statement {
                None => SqlPortalExecution::Empty,
                Some(stmt) => match self.run_one(&stmt).await.map_err(ExecError::into_pg)? {
                    QueryResult::Rows { rows, tag, .. } => SqlPortalExecution::Rows {
                        rows,
                        tag,
                        position: 0,
                    },
                    QueryResult::Command { tag } => SqlPortalExecution::Command { tag },
                    QueryResult::Empty => SqlPortalExecution::Empty,
                },
            };
            self.portals
                .get_mut(portal)
                .expect("portal exists throughout execute")
                .execution = execution;
        }
        let p = self
            .portals
            .get_mut(portal)
            .expect("portal exists throughout execute");
        match &mut p.execution {
            SqlPortalExecution::Rows {
                rows,
                tag,
                position,
            } => {
                let remaining = rows.len() - *position;
                let take = if max_rows == 0 {
                    remaining
                } else {
                    remaining.min(max_rows as usize)
                };
                let encoded = rows[*position..*position + take]
                    .iter()
                    .map(|row| {
                        row.iter()
                            .zip(&p.formats)
                            .map(|(cell, format)| {
                                cell.as_ref().map(|cell| {
                                    if *format == 1 {
                                        cell.binary.clone()
                                    } else {
                                        cell.text.clone()
                                    }
                                })
                            })
                            .collect()
                    })
                    .collect();
                *position += take;
                Ok(ExecuteOutcome::Rows {
                    rows: encoded,
                    completion: (*position == rows.len()).then(|| tag.clone()),
                })
            }
            SqlPortalExecution::Command { tag } => {
                Ok(ExecuteOutcome::CommandComplete { tag: tag.clone() })
            }
            SqlPortalExecution::Empty => Ok(ExecuteOutcome::EmptyQuery),
            SqlPortalExecution::NotStarted => unreachable!(),
        }
    }

    async fn close(&mut self, target: CloseTarget<'_>) -> Result<(), PgError> {
        match target {
            CloseTarget::Statement(name) => {
                self.prepared.remove(name);
            }
            CloseTarget::Portal(name) => {
                self.portals.remove(name);
            }
        }
        Ok(())
    }

    async fn sync(&mut self) -> Result<(), PgError> {
        self.portals.clear();
        Ok(())
    }

    async fn begin_copy_in(&mut self, sql: &str) -> Result<Option<CopyInResponse>, PgError> {
        let Some(copy) = parse_single_copy_statement(sql)? else {
            return Ok(None);
        };
        self.copy_in_start(&copy).map(Some)
    }

    async fn copy_in(
        &mut self,
        sql: &str,
        data: Vec<bytes::Bytes>,
    ) -> Result<QueryResult, PgError> {
        let Some(copy) = parse_single_copy_statement(sql)? else {
            return Err(PgError::error(
                sqlstate::SYNTAX_ERROR,
                "COPY data received for a non-COPY statement",
            ));
        };
        self.reject_prepared_participant()
            .map_err(ExecError::into_pg)?;
        let result = self.run_copy_in(&copy, data).await;
        if result.is_err() {
            self.mark_transaction_failed();
        }
        result.map_err(ExecError::into_pg)
    }

    async fn copy_in_portal(
        &mut self,
        portal: &str,
        data: Vec<bytes::Bytes>,
    ) -> Result<QueryResult, PgError> {
        let statement = self
            .portals
            .get(portal)
            .ok_or_else(|| {
                PgError::error(
                    sqlstate::INVALID_CURSOR_NAME,
                    format!("portal \"{portal}\" does not exist"),
                )
            })?
            .statement
            .clone();
        let copy = statement
            .as_ref()
            .map(copy_sentinel_stmt)
            .transpose()?
            .flatten()
            .ok_or_else(|| {
                PgError::error(
                    sqlstate::PROTOCOL_VIOLATION,
                    "COPY data received for a non-COPY portal",
                )
            })?;
        self.reject_prepared_participant()
            .map_err(ExecError::into_pg)?;
        let result = self.run_copy_in(&copy, data).await;
        if result.is_err() {
            self.mark_transaction_failed();
        }
        let result = result.map_err(ExecError::into_pg)?;
        // Mark the portal completed so a re-Execute reports the command tag
        // instead of restarting the copy.
        if let QueryResult::Command { tag } = &result
            && let Some(p) = self.portals.get_mut(portal)
        {
            p.execution = SqlPortalExecution::Command { tag: tag.clone() };
        }
        Ok(result)
    }

    fn mark_statement_failed(&mut self) {
        self.mark_transaction_failed();
    }

    fn tx_status(&self) -> TxStatus {
        match self.state {
            TxnState::Idle => TxStatus::Idle,
            TxnState::InTransaction(_) | TxnState::Prepared(_) => TxStatus::InTransaction,
            TxnState::Failed(_) => TxStatus::Failed,
        }
    }
}

const RESULT_PAGE_MAX_BYTES: usize = 1 << 20;

fn into_bounded_row_pages(
    rows: Vec<Vec<Option<crabka_pgwire::engine::Cell>>>,
    page_rows: usize,
    page_bytes: usize,
) -> impl Iterator<Item = Result<Vec<Vec<Option<crabka_pgwire::engine::Cell>>>, PgError>> {
    debug_assert!(page_rows > 0);
    debug_assert!(page_bytes > 0);
    let mut rows = rows.into_iter().peekable();
    std::iter::from_fn(move || {
        let first_bytes = match row_result_bytes(rows.peek()?) {
            Ok(bytes) => bytes,
            Err(error) => return Some(Err(error)),
        };
        if first_bytes > page_bytes {
            rows.next();
            return Some(Err(PgError::error(
                "54000",
                format!(
                    "one result row requires {first_bytes} bytes, exceeding the {page_bytes}-byte page limit"
                ),
            )));
        }
        let mut bytes = 0usize;
        let mut page = Vec::with_capacity(page_rows);
        while page.len() < page_rows {
            let Some(row) = rows.peek() else { break };
            let row_bytes = match row_result_bytes(row) {
                Ok(row_bytes) => row_bytes,
                Err(error) => return Some(Err(error)),
            };
            if !page.is_empty() && bytes.saturating_add(row_bytes) > page_bytes {
                break;
            }
            bytes = bytes.saturating_add(row_bytes);
            page.push(rows.next().expect("peeked row exists"));
        }
        Some(Ok(page))
    })
}

fn row_result_bytes(row: &[Option<crabka_pgwire::engine::Cell>]) -> Result<usize, PgError> {
    row.iter().try_fold(0usize, |bytes, cell| {
        let cell_bytes = cell
            .as_ref()
            .map_or(0, |cell| cell.text.len().saturating_add(cell.binary.len()));
        bytes.checked_add(cell_bytes).ok_or_else(|| {
            PgError::error("54000", "result row byte size exceeds addressable memory")
        })
    })
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
    };

    use crabka_pgkv::{Kv, MemKv};
    use crabka_pgwire::engine::{Engine, QueryResult, Session, TxStatus};

    use super::{
        ColumnType, GucState, SqlSession, canonical_guc_value, decode_bound_param, guc_default,
        guc_vartype,
    };
    use crate::{ExecError, SqlEngine};

    struct FailOnCommitter {
        kv: Arc<dyn Kv>,
        calls: AtomicU64,
        fail_on: u64,
    }

    impl FailOnCommitter {
        fn new(kv: Arc<dyn Kv>, fail_on: u64) -> Self {
            Self {
                kv,
                calls: AtomicU64::new(0),
                fail_on,
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::commit::Committer for FailOnCommitter {
        async fn commit(&self, ops: Vec<crabka_pgkv::WriteOp>) -> Result<(), ExecError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == self.fail_on {
                return Err(ExecError::Unsupported(format!(
                    "injected commit failure on call {call}"
                )));
            }
            self.kv.write_batch(&ops)?;
            Ok(())
        }
    }

    fn replicated_engine() -> SqlEngine {
        let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
        let committer: Arc<dyn crate::commit::Committer> =
            Arc::new(crate::commit::LocalCommitter {
                kv: Arc::clone(&kv),
            });
        SqlEngine::replicated(
            Arc::clone(&kv),
            kv,
            committer,
            Arc::new(crate::read_gate::LocalLinearizer),
        )
        .expect("replicated engine")
    }

    fn replicated_engine_failing_on_commit(fail_on: u64) -> SqlEngine {
        let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
        let committer: Arc<dyn crate::commit::Committer> =
            Arc::new(FailOnCommitter::new(Arc::clone(&kv), fail_on));
        SqlEngine::replicated(
            Arc::clone(&kv),
            kv,
            committer,
            Arc::new(crate::read_gate::LocalLinearizer),
        )
        .expect("replicated engine")
    }

    #[tokio::test]
    async fn bounded_result_sink_matches_collecting_simple_query() {
        use crabka_pgwire::engine::{CollectingResultSink, ResultPage};

        let engine = SqlEngine::new();
        let mut setup = engine.connect();
        setup
            .simple_query(
                "CREATE TABLE streamed (id int4, value text); \
                 INSERT INTO streamed VALUES (1, 'one'), (2, NULL), (3, 'three')",
            )
            .await
            .expect("setup");

        let mut collecting_session = engine.connect();
        let expected = collecting_session
            .simple_query("SELECT id, value FROM streamed ORDER BY id; SELECT 7")
            .await
            .expect("collecting query");

        let mut streamed_session = engine.connect();
        let mut sink = CollectingResultSink::default();
        streamed_session
            .simple_query_into(
                "SELECT id, value FROM streamed ORDER BY id; SELECT 7",
                2,
                &mut sink,
            )
            .await
            .expect("streamed query");

        assert!(sink.pages().iter().any(|page| matches!(
            page,
            ResultPage::Rows { rows, .. } if rows.len() == 2
        )));
        assert_eq!(sink.finish().expect("valid page stream"), expected);
    }

    #[test]
    fn row_pages_consume_many_page_input_once_in_order() {
        let rows: Vec<_> = (0..10_003).collect();
        let rows = rows
            .into_iter()
            .map(|value| {
                vec![Some(crabka_pgwire::engine::Cell {
                    text: bytes::Bytes::from(value.to_string()),
                    binary: bytes::Bytes::copy_from_slice(&i32::to_be_bytes(value)),
                })]
            })
            .collect();
        let pages: Vec<_> = super::into_bounded_row_pages(rows, 17, usize::MAX)
            .collect::<Result<_, _>>()
            .expect("bounded pages");

        assert_eq!(pages.len(), 589);
        assert!(pages.iter().all(|page| page.len() <= 17));
        let values: Vec<_> = pages
            .into_iter()
            .flatten()
            .map(|row| row[0].as_ref().expect("cell").text.clone())
            .collect();
        assert_eq!(values.first().expect("first"), "0");
        assert_eq!(values.last().expect("last"), "10002");
    }

    #[test]
    fn row_pages_reject_a_single_oversized_row() {
        use bytes::Bytes;
        use crabka_pgwire::engine::Cell;

        let rows = vec![vec![Some(Cell {
            text: Bytes::from_static(b"12345"),
            binary: Bytes::new(),
        })]];
        let error = super::into_bounded_row_pages(rows, 10, 4)
            .next()
            .expect("one result")
            .expect_err("row exceeds byte limit");

        assert_eq!(error.code, "54000");
    }

    #[tokio::test]
    async fn bounded_result_sink_propagates_backpressure_failure_before_next_statement() {
        use crabka_pgwire::{
            engine::{ResultPage, ResultSink},
            error::PgError,
        };

        struct RejectingSink {
            pages: usize,
        }

        #[async_trait::async_trait]
        impl ResultSink for RejectingSink {
            async fn send(&mut self, _page: ResultPage) -> Result<(), PgError> {
                self.pages += 1;
                Err(PgError::error("57014", "result consumer disconnected"))
            }
        }

        let engine = SqlEngine::new();
        let mut session = engine.connect();
        let mut sink = RejectingSink { pages: 0 };
        let error = session
            .simple_query_into(
                "SELECT 1; CREATE TABLE must_not_run (id int4)",
                1,
                &mut sink,
            )
            .await
            .expect_err("sink refusal must cancel production");
        assert_eq!(error.code, "57014");
        assert_eq!(sink.pages, 1);

        let missing = session
            .simple_query("SELECT id FROM must_not_run")
            .await
            .expect_err("second statement was not executed");
        assert_eq!(missing.code, "42P01");
    }

    #[tokio::test]
    async fn table_rename_updates_sharded_acl_metadata_in_the_authoritative_catalog() {
        let data_kv: Arc<dyn Kv> = Arc::new(MemKv::new());
        let catalog_kv: Arc<dyn Kv> = Arc::new(MemKv::new());
        let mut engine = SqlEngine::with_kv(Arc::clone(&data_kv)).expect("data-range engine");
        engine.set_catalog_kv(Arc::clone(&catalog_kv));
        let mut session = engine.connect();

        session
            .simple_query(
                "CREATE TABLE orders (id int4) SHARDED; \
                 CREATE ROLE reader; \
                 GRANT SELECT ON TABLE orders TO reader; \
                 ALTER TABLE orders RENAME TO archived_orders",
            )
            .await
            .expect("rename through catalog authority");

        let table = crabka_pgcatalog::get_table(catalog_kv.as_ref(), "archived_orders")
            .expect("renamed catalog table");
        let privileges = crabka_pgcatalog::list_table_privileges(catalog_kv.as_ref())
            .expect("renamed table privileges");
        assert!(table.sharded);
        assert_eq!(
            privileges,
            vec![crabka_pgcatalog::TablePrivilege {
                table: "archived_orders".into(),
                grantee: "reader".into(),
                privilege: "SELECT".into(),
            }]
        );
        assert!(
            data_kv
                .get(&crabka_pgkv::key::catalog_key("orders"))
                .expect("read local data range")
                .is_none()
        );
        assert!(
            data_kv
                .get(&crabka_pgkv::key::catalog_key("archived_orders"))
                .expect("read local data range")
                .is_none()
        );
    }

    struct FailFirstCommitOracle {
        next_start_ts: AtomicU64,
        should_fail_commit: AtomicBool,
    }

    impl FailFirstCommitOracle {
        const fn new() -> Self {
            Self {
                next_start_ts: AtomicU64::new(1),
                should_fail_commit: AtomicBool::new(true),
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::timestamp_txn::TimestampSource for FailFirstCommitOracle {
        async fn allocate_read_timestamp(
            &self,
        ) -> Result<crate::timestamp_txn::ReadTimestamp, crate::timestamp_txn::TimestampSourceError>
        {
            let timestamp = self.next_start_ts.fetch_add(1, Ordering::SeqCst);
            crate::timestamp_txn::ReadTimestamp::new(timestamp).map_err(Into::into)
        }

        async fn allocate_transaction_id(
            &self,
        ) -> Result<
            crate::timestamp_txn::TimestampTransactionId,
            crate::timestamp_txn::TimestampSourceError,
        > {
            let timestamp = self.next_start_ts.fetch_add(1, Ordering::SeqCst);
            crate::timestamp_txn::TimestampTransactionId::new(timestamp).map_err(Into::into)
        }

        async fn allocate_commit_after(
            &self,
            start_ts: crate::timestamp_txn::TimestampTransactionId,
        ) -> Result<crate::timestamp_txn::CommitTimestamp, crate::timestamp_txn::TimestampSourceError>
        {
            if self.should_fail_commit.swap(false, Ordering::SeqCst) {
                return Err(crate::timestamp_txn::TimestampSourceError::Unavailable(
                    "injected commit timestamp failure".into(),
                ));
            }
            let commit_ts = start_ts.get() + 1;
            self.next_start_ts
                .fetch_max(commit_ts + 1, Ordering::SeqCst);
            crate::timestamp_txn::CommitTimestamp::after_start(start_ts, commit_ts)
                .map_err(Into::into)
        }
    }

    /// SP37: the GUC transactional state machine — PostgreSQL's commit-keeps,
    /// rollback-reverts, and SET-LOCAL-always-reverts semantics for `timezone`.
    #[test]
    fn guc_timezone_transactional_semantics() {
        use crate::session::GucState;
        let mut g = GucState::default();
        assert_eq!(g.effective("timezone").expect("timezone"), "UTC");
        g.set("timezone", "America/New_York", false).expect("set");
        g.commit();
        assert_eq!(
            g.effective("timezone").expect("timezone"),
            "America/New_York"
        );
        g.set("timezone", "UTC", false).expect("set");
        assert_eq!(g.effective("timezone").expect("timezone"), "UTC");
        g.rollback();
        assert_eq!(
            g.effective("timezone").expect("timezone"),
            "America/New_York"
        );
        g.set("timezone", "UTC", false).expect("set");
        g.commit();
        assert_eq!(g.effective("timezone").expect("timezone"), "UTC");
        g.set("timezone", "America/New_York", true).expect("set");
        assert_eq!(
            g.effective("timezone").expect("timezone"),
            "America/New_York"
        );
        g.commit();
        assert_eq!(g.effective("timezone").expect("timezone"), "UTC");
        g.set("timezone", "America/New_York", false).expect("set");
        g.commit();
        g.reset("timezone").expect("reset");
        g.commit();
        assert_eq!(g.effective("timezone").expect("timezone"), "UTC");
    }

    /// Extract the single text cell of a one-row, one-column result.
    fn single_text(results: &[crabka_pgwire::engine::QueryResult]) -> String {
        use crabka_pgwire::engine::QueryResult;
        match results {
            [QueryResult::Rows { rows, .. }] => {
                let cell = rows[0][0].as_ref().expect("non-null cell");
                String::from_utf8(cell.text.to_vec()).expect("utf8")
            }
            other => panic!("expected one Rows result, got {other:?}"),
        }
    }

    fn text_param(value: Option<&str>, type_oid: Option<u32>) -> crabka_pgwire::engine::BoundParam {
        crabka_pgwire::engine::BoundParam {
            type_oid,
            format: 0,
            value: value.map(|v| bytes::Bytes::copy_from_slice(v.as_bytes())),
        }
    }

    fn binary_int4_param(value: i32) -> crabka_pgwire::engine::BoundParam {
        crabka_pgwire::engine::BoundParam {
            type_oid: Some(crabka_pgtypes::oids::INT4),
            format: 1,
            value: Some(bytes::Bytes::copy_from_slice(&value.to_be_bytes())),
        }
    }

    fn binary_bool_param(value: bool) -> crabka_pgwire::engine::BoundParam {
        crabka_pgwire::engine::BoundParam {
            type_oid: Some(crabka_pgtypes::oids::BOOL),
            format: 1,
            value: Some(bytes::Bytes::copy_from_slice(&[u8::from(value)])),
        }
    }

    fn binary_param(value: &[u8], type_oid: u32) -> crabka_pgwire::engine::BoundParam {
        crabka_pgwire::engine::BoundParam {
            type_oid: Some(type_oid),
            format: 1,
            value: Some(bytes::Bytes::copy_from_slice(value)),
        }
    }

    #[tokio::test]
    async fn extended_query_binds_text_parameter_for_select_cast() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        let params = [text_param(Some("42"), None)];

        let results = session
            .test_extended_query("SELECT $1::int4", &params)
            .await
            .expect("extended select");

        assert_eq!(single_text(&results), "42");
    }

    #[tokio::test]
    async fn sharded_autocommit_commit_timestamp_failure_leaves_no_intents() {
        let mut engine = SqlEngine::new();
        engine.set_timestamp_oracle(Arc::new(FailFirstCommitOracle::new()));
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE t (id int4) SHARDED")
            .await
            .expect("create sharded table");

        let error = session
            .simple_query("INSERT INTO t VALUES (1)")
            .await
            .expect_err("commit timestamp allocation fails");

        assert!(error.message.contains("injected commit timestamp failure"));
        assert_eq!(unresolved_timestamp_intents(&engine, "t"), 0);

        session
            .simple_query("INSERT INTO t VALUES (2)")
            .await
            .expect("future write can proceed");
        let selected = session
            .simple_query("SELECT id FROM t")
            .await
            .expect("select clean state");

        assert_eq!(single_text(&selected), "2");
        assert_eq!(unresolved_timestamp_intents(&engine, "t"), 0);
    }

    #[tokio::test]
    async fn replicated_sharded_timestamp_insert_folds_sequence_and_commits_rows() {
        let engine = replicated_engine();
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE t (id int4) SHARDED")
            .await
            .expect("create sharded table");

        session
            .simple_query("INSERT INTO t VALUES (1), (2)")
            .await
            .expect("replicated timestamp insert");
        let selected = session
            .simple_query("SELECT id FROM t ORDER BY id")
            .await
            .expect("select committed rows");
        let row_count = session
            .simple_query("SELECT count(*) FROM t")
            .await
            .expect("count committed rows");

        assert_eq!(single_text(&selected), "1");
        assert_eq!(single_text(&row_count), "2");
        assert_eq!(sequence_next_rowid(&engine, "t"), Some(3));
    }

    #[tokio::test]
    async fn replicated_sharded_timestamp_update_and_delete_remain_supported() {
        let engine = replicated_engine();
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE t (id int4, name text) SHARDED")
            .await
            .expect("create sharded table");
        session
            .simple_query("INSERT INTO t VALUES (1, 'alpha'), (2, 'beta')")
            .await
            .expect("insert rows");

        session
            .simple_query("UPDATE t SET name = 'gamma' WHERE id = 1")
            .await
            .expect("update row");
        session
            .simple_query("DELETE FROM t WHERE id = 2")
            .await
            .expect("delete row");
        let selected = session
            .simple_query("SELECT name FROM t WHERE id = 1")
            .await
            .expect("select remaining row");

        assert_eq!(single_text(&selected), "gamma");
    }

    #[tokio::test]
    async fn replicated_sequence_folding_preserves_non_unique_global_index_maintenance() {
        let engine = replicated_engine();
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE t (id int4, name text) SHARDED")
            .await
            .expect("create sharded table");
        session
            .simple_query("CREATE GLOBAL INDEX t_name_idx ON t (name)")
            .await
            .expect("create global index");

        session
            .simple_query("INSERT INTO t VALUES (1, 'alpha'), (2, 'beta')")
            .await
            .expect("insert rows");

        let index_id = index_id(&engine, "t", "t_name_idx");
        assert_eq!(visible_global_index_names(&engine, index_id, "alpha"), 1);
        assert_eq!(visible_global_index_names(&engine, index_id, "beta"), 1);
        assert_eq!(sequence_next_rowid(&engine, "t"), Some(3));
    }

    #[tokio::test]
    async fn replicated_timestamp_commit_failure_aborts_intents_without_sequence_fold() {
        let engine = replicated_engine_failing_on_commit(4);
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE t (id int4, name text) SHARDED")
            .await
            .expect("create sharded table");
        session
            .simple_query("CREATE GLOBAL INDEX t_name_idx ON t (name)")
            .await
            .expect("create global index");

        let error = session
            .simple_query("INSERT INTO t VALUES (1, 'alpha')")
            .await
            .expect_err("commit fails after prewrite");

        assert!(error.message.contains("injected commit failure on call 4"));
        let index_id = index_id(&engine, "t", "t_name_idx");
        assert_eq!(unresolved_timestamp_intents(&engine, "t"), 0);
        assert_eq!(visible_global_index_names(&engine, index_id, "alpha"), 0);
        assert_eq!(sequence_next_rowid(&engine, "t"), Some(1));
    }

    #[tokio::test]
    async fn sharded_timestamp_dml_maintains_non_unique_global_index_entries() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE t (id int4, name text) SHARDED")
            .await
            .expect("create sharded table");
        session
            .simple_query("CREATE GLOBAL INDEX t_name_idx ON t (name)")
            .await
            .expect("create global index");
        session
            .simple_query("INSERT INTO t VALUES (1, 'alpha'), (2, 'beta')")
            .await
            .expect("insert rows");

        let index_id = index_id(&engine, "t", "t_name_idx");
        assert_eq!(visible_global_index_names(&engine, index_id, "alpha"), 1);
        assert_eq!(visible_global_index_names(&engine, index_id, "beta"), 1);

        session
            .simple_query("UPDATE t SET name = 'gamma' WHERE id = 1")
            .await
            .expect("update indexed value");

        assert_eq!(visible_global_index_names(&engine, index_id, "alpha"), 0);
        assert_eq!(visible_global_index_names(&engine, index_id, "gamma"), 1);
        assert_eq!(visible_global_index_names(&engine, index_id, "beta"), 1);

        session
            .simple_query("DELETE FROM t WHERE id = 1")
            .await
            .expect("delete indexed row");

        assert_eq!(visible_global_index_names(&engine, index_id, "gamma"), 0);
        assert_eq!(visible_global_index_names(&engine, index_id, "beta"), 1);
    }

    #[tokio::test]
    async fn unique_global_index_on_sharded_table_still_fails_clear() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE t (id int4, name text) SHARDED")
            .await
            .expect("create sharded table");

        let error = session
            .simple_query("CREATE UNIQUE GLOBAL INDEX t_name_idx ON t (name)")
            .await
            .expect_err("unique global index is unsupported");

        assert_eq!(error.code, "0A000");
        assert!(
            error
                .message
                .contains("unique global indexes are not supported")
        );
    }

    fn unresolved_timestamp_intents(engine: &SqlEngine, table_name: &str) -> usize {
        let table = crabka_pgcatalog::get_table(engine.kv_handle().as_ref(), table_name)
            .expect("table exists");
        engine
            .kv_handle()
            .scan_prefix(&crabka_pgkv::key::table_prefix(table.id))
            .expect("scan table")
            .into_iter()
            .filter(|(_key, value)| {
                crabka_pgmvcc::version::decode_ts_tuple(value).is_ok_and(|version| {
                    version.state == crabka_pgmvcc::version::TsVersionState::Intent
                })
            })
            .count()
    }

    fn index_id(engine: &SqlEngine, table_name: &str, index_name: &str) -> u32 {
        crabka_pgcatalog::list_table_indexes(engine.kv_handle().as_ref(), table_name)
            .expect("list indexes")
            .into_iter()
            .find(|index| index.name == index_name)
            .expect("index exists")
            .id
    }

    fn visible_global_index_names(engine: &SqlEngine, index_id: u32, name: &str) -> usize {
        crate::timestamp_txn::read_visible_global_index_entries(
            engine.kv_handle().as_ref(),
            index_id,
            &[crabka_pgtypes::Datum::Text(name.into())],
            crate::timestamp_txn::ReadTimestamp::MAX,
        )
        .expect("read visible global index entries")
        .len()
    }

    fn sequence_next_rowid(engine: &SqlEngine, table_name: &str) -> Option<u64> {
        let table = crabka_pgcatalog::get_table(engine.kv_handle().as_ref(), table_name)
            .expect("table exists");
        let bytes = engine
            .kv_handle()
            .get(&crabka_pgkv::key::seq_key(table.id))
            .expect("read sequence key")?;
        let value = <[u8; 8]>::try_from(bytes.as_slice()).expect("sequence value is u64");
        Some(u64::from_be_bytes(value))
    }

    #[tokio::test]
    async fn extended_query_decodes_binary_int4_parameter() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        let params = [binary_int4_param(314)];

        let results = session
            .test_extended_query("SELECT $1::int4", &params)
            .await
            .expect("extended binary select");

        assert_eq!(single_text(&results), "314");
    }

    #[tokio::test]
    async fn extended_query_defaults_untyped_select_parameter_to_text() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        let params = [text_param(Some("hello"), None)];

        let results = session
            .test_extended_query("SELECT $1", &params)
            .await
            .expect("extended select");

        assert_eq!(single_text(&results), "hello");
    }

    #[tokio::test]
    async fn copy_from_stdin_inserts_text_rows_with_defaults_and_nulls() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE t (id int4, name text DEFAULT 'anon', note text)")
            .await
            .expect("create");

        let response = session
            .begin_copy_in("COPY t (id, note) FROM STDIN")
            .await
            .expect("begin copy")
            .expect("copy response");
        assert_eq!(response.overall_format, 0);
        assert_eq!(response.column_formats, vec![0, 0]);

        session
            .copy_in(
                "COPY t (id, note) FROM STDIN",
                vec![bytes::Bytes::from_static(b"1\thello\\nworld\n2\t\\N\n")],
            )
            .await
            .expect("copy done");

        let rows = session
            .simple_query("SELECT id, name, note FROM t ORDER BY id")
            .await
            .expect("select");
        let crabka_pgwire::engine::QueryResult::Rows { rows, .. } = &rows[0] else {
            panic!("expected rows");
        };
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0].as_ref().expect("id").text, "1");
        assert_eq!(rows[0][1].as_ref().expect("default").text, "anon");
        assert_eq!(rows[0][2].as_ref().expect("note").text, "hello\nworld");
        assert_eq!(rows[1][0].as_ref().expect("id").text, "2");
        assert!(rows[1][2].is_none());
    }

    #[tokio::test]
    async fn copy_from_stdin_not_null_failure_inserts_no_rows() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE t (id int4 NOT NULL, name text)")
            .await
            .expect("create");

        let err = session
            .copy_in(
                "COPY t (id, name) FROM STDIN",
                vec![bytes::Bytes::from_static(b"1\tok\n\\N\tbad\n")],
            )
            .await
            .expect_err("not null violation");
        assert_eq!(err.code, "23502");

        let rows = session
            .simple_query("SELECT id FROM t")
            .await
            .expect("select");
        let crabka_pgwire::engine::QueryResult::Rows { rows, .. } = &rows[0] else {
            panic!("expected rows");
        };
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn copy_from_stdin_hash_table_uses_timestamp_bucket_keys_atomically() {
        let kv = Arc::new(crabka_pgkv::MemKv::new());
        let mut engine =
            SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn crabka_pgkv::Kv>).expect("engine");
        engine.init_gtm_coordinator().expect("gtm");
        let mut session = engine.connect();
        session
            .simple_query(
                "CREATE TABLE hc (id int4 NOT NULL, value text) SHARDED BY HASH (id) BUCKETS 16",
            )
            .await
            .expect("create");

        let copy_rows = (0..16).fold(String::new(), |mut rows, id| {
            use std::fmt::Write as _;
            writeln!(&mut rows, "{id}\tv{id}").expect("writing to String cannot fail");
            rows
        });
        session
            .copy_in(
                "COPY hc (id, value) FROM STDIN",
                vec![bytes::Bytes::from(copy_rows)],
            )
            .await
            .expect("copy hash rows");

        let table = crabka_pgcatalog::get_table(kv.as_ref(), "hc").expect("table");
        let physical = kv
            .scan_prefix(&crabka_pgkv::key::table_prefix(table.id))
            .expect("physical rows");
        assert_eq!(physical.len(), 16);
        assert!(physical.iter().all(|(key, _)| matches!(
            crabka_pgkv::key::classify_key(key),
            crabka_pgkv::key::KeyClass::HashPrimaryVersion { .. }
        )));
        let results = session
            .simple_query("SELECT id FROM hc ORDER BY id")
            .await
            .expect("read copied rows");
        let QueryResult::Rows { rows, .. } = &results[0] else {
            panic!("expected rows");
        };
        assert_eq!(rows.len(), 16);

        let error = session
            .copy_in(
                "COPY hc (id, value) FROM STDIN",
                vec![bytes::Bytes::from_static(b"100\tok\n\\N\tbad\n")],
            )
            .await
            .expect_err("malformed batch aborts");
        assert_eq!(error.code, "23502");
        assert_eq!(
            kv.scan_prefix(&crabka_pgkv::key::table_prefix(table.id))
                .expect("physical rows after abort")
                .len(),
            16
        );
    }

    #[tokio::test]
    async fn copy_from_stdin_unique_failure_inserts_no_rows() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query(
                "CREATE TABLE t (id int4, name text);
                 CREATE UNIQUE INDEX t_name_idx ON t (name);
                 INSERT INTO t VALUES (1, 'existing')",
            )
            .await
            .expect("seed unique table");

        let existing_err = session
            .copy_in(
                "COPY t (id, name) FROM STDIN",
                vec![bytes::Bytes::from_static(b"2\tnew\n3\texisting\n")],
            )
            .await
            .expect_err("copy duplicates existing row");
        assert_eq!(existing_err.code, "23505");
        let input_err = session
            .copy_in(
                "COPY t (id, name) FROM STDIN",
                vec![bytes::Bytes::from_static(b"4\tdup\n5\tdup\n")],
            )
            .await
            .expect_err("copy duplicates input row");
        assert_eq!(input_err.code, "23505");

        let rows = session
            .simple_query("SELECT id, name FROM t ORDER BY id")
            .await
            .expect("select");
        let crabka_pgwire::engine::QueryResult::Rows { rows, .. } = &rows[0] else {
            panic!("expected rows");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_ref().expect("id").text, "1");
        assert_eq!(rows[0][1].as_ref().expect("name").text, "existing");
    }

    #[tokio::test]
    async fn extended_query_infers_where_parameter_from_compared_column() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE t (id int4, name text)")
            .await
            .expect("create table");
        session
            .simple_query("INSERT INTO t VALUES (1, 'one'), (2, 'two')")
            .await
            .expect("seed rows");
        let params = [text_param(Some("2"), None)];

        let results = session
            .test_extended_query("SELECT name FROM t WHERE id = $1", &params)
            .await
            .expect("parameterized where");

        assert_eq!(single_text(&results), "two");
    }

    #[tokio::test]
    async fn extended_query_infers_insert_parameter_types_from_target_columns() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE t (id int4, enabled bool, name text)")
            .await
            .expect("create table");
        let params = [
            text_param(Some("9"), None),
            text_param(Some("on"), None),
            text_param(Some("nine"), None),
        ];

        session
            .test_extended_query("INSERT INTO t VALUES ($1, $2, $3)", &params)
            .await
            .expect("insert with inferred params");
        let selected = session
            .simple_query("SELECT name FROM t WHERE enabled AND id = 9")
            .await
            .expect("select inserted row");

        assert_eq!(single_text(&selected), "nine");
    }

    async fn accounts_session(engine: &SqlEngine) -> SqlSession {
        let mut session = engine.connect();
        session
            .simple_query(
                "CREATE TABLE accounts (id BIGINT PRIMARY KEY, balance BIGINT, updates BIGINT)",
            )
            .await
            .expect("create accounts table");
        session
    }

    #[tokio::test]
    async fn extended_describe_infers_arithmetic_update_parameter_types_from_columns() {
        use assert2::assert;
        let engine = SqlEngine::new();
        let mut session = accounts_session(&engine).await;

        for sql in [
            "UPDATE accounts SET balance = balance + $1, updates = updates + 1 WHERE id = $2",
            "UPDATE accounts SET balance = balance - $1, updates = updates + 1 WHERE id = $2",
            "UPDATE accounts SET balance = balance * $1, updates = updates + 1 WHERE id = $2",
            "UPDATE accounts SET balance = balance / $1, updates = updates + 1 WHERE id = $2",
        ] {
            let (_, parameter_types) = session
                .test_describe_prepared(sql, &[])
                .await
                .expect("describe arithmetic update");
            assert!(
                parameter_types == vec![crabka_pgtypes::oids::INT8, crabka_pgtypes::oids::INT8],
                "sql: {sql}"
            );
        }
    }

    #[tokio::test]
    async fn extended_describe_defaults_unknown_parameter_to_text() {
        use assert2::assert;
        let engine = SqlEngine::new();
        let mut session = engine.connect();

        let (_, parameter_types) = session
            .test_describe_prepared("SELECT $1", &[])
            .await
            .expect("describe bare parameter select");

        assert!(parameter_types == vec![crabka_pgtypes::oids::TEXT]);
    }

    #[tokio::test]
    async fn extended_describe_accepts_declared_oid_parameter_type() {
        use assert2::assert;
        let engine = SqlEngine::new();
        let mut session = engine.connect();

        // tokio-postgres declares its pg_catalog typeinfo query's $1 as type
        // OID (26) in the Parse message; the declared oid must round-trip
        // through ParameterDescription instead of erroring 42P18.
        let (_, parameter_types) = session
            .test_describe_prepared(
                "SELECT t.typname FROM pg_catalog.pg_type t WHERE t.oid = $1",
                &[crabka_pgtypes::oids::OID],
            )
            .await
            .expect("describe with declared oid parameter");

        assert!(parameter_types == vec![crabka_pgtypes::oids::OID]);
    }

    #[tokio::test]
    async fn extended_regclass_parameter_resolves_relation_name() {
        use assert2::assert;
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE pgbench_accounts (aid BIGINT PRIMARY KEY)")
            .await
            .expect("create table");

        // pgbench -i's relkind probe, verbatim: $1 inside a regclass cast is
        // described as regclass (2205)...
        let probe = "SELECT relkind FROM pg_catalog.pg_class WHERE oid=$1::pg_catalog.regclass";
        let (_, parameter_types) = session
            .test_describe_prepared(probe, &[])
            .await
            .expect("describe relkind probe");
        assert!(parameter_types == vec![crabka_pgtypes::oids::REGCLASS]);

        // ...and executing with the table name bound as untyped text resolves
        // the name through the catalog.
        let mut session = engine.connect();
        let results = session
            .test_extended_query(
                probe,
                &[crabka_pgwire::engine::BoundParam {
                    type_oid: None,
                    format: 0,
                    value: Some(bytes::Bytes::from_static(b"pgbench_accounts")),
                }],
            )
            .await
            .expect("execute relkind probe");
        let [QueryResult::Rows { rows, .. }] = results.as_slice() else {
            panic!("expected rows, got {results:?}");
        };
        let text = rows[0][0]
            .as_ref()
            .map(|cell| String::from_utf8(cell.text.to_vec()).expect("utf8"));
        assert!(text == Some("r".into()));

        // An unknown relation name errors 42P01 like PostgreSQL.
        let mut session = engine.connect();
        let error = session
            .test_extended_query(
                probe,
                &[crabka_pgwire::engine::BoundParam {
                    type_oid: None,
                    format: 0,
                    value: Some(bytes::Bytes::from_static(b"no_such_relation")),
                }],
            )
            .await
            .expect_err("unknown relation name");
        assert!(error.code == "42P01");
    }

    #[tokio::test]
    async fn extended_query_executes_arithmetic_update_with_text_format_params() {
        use assert2::assert;
        let engine = SqlEngine::new();
        let mut session = accounts_session(&engine).await;
        session
            .simple_query("INSERT INTO accounts VALUES (42, 100, 0)")
            .await
            .expect("seed account row");
        let params = [text_param(Some("5"), None), text_param(Some("42"), None)];

        let results = session
            .test_extended_query(
                "UPDATE accounts SET balance = balance + $1, updates = updates + 1 WHERE id = $2",
                &params,
            )
            .await
            .expect("arithmetic update with untyped text params");
        assert!(
            results
                == vec![QueryResult::Command {
                    tag: "UPDATE 1".into()
                }]
        );

        let balance = session
            .simple_query("SELECT balance FROM accounts WHERE id = 42")
            .await
            .expect("read back balance");
        assert!(single_text(&balance) == "105");
        let updates = session
            .simple_query("SELECT updates FROM accounts WHERE id = 42")
            .await
            .expect("read back updates");
        assert!(single_text(&updates) == "1");
    }

    #[tokio::test]
    async fn extended_describe_never_reports_parameter_oid_zero() {
        use assert2::assert;
        let engine = SqlEngine::new();
        let mut session = accounts_session(&engine).await;

        for sql in [
            "UPDATE accounts SET balance = balance + $1, updates = updates + 1 WHERE id = $2",
            "UPDATE accounts SET balance = balance - $1 WHERE id = $2",
            "SELECT $1",
            "SELECT id FROM accounts WHERE balance * $1 > $2",
            "INSERT INTO accounts VALUES ($1, $2, $3)",
        ] {
            let (_, parameter_types) = session
                .test_describe_prepared(sql, &[])
                .await
                .expect("describe parameterized statement");
            assert!(
                !parameter_types.contains(&0),
                "sql: {sql}, types: {parameter_types:?}"
            );
        }
    }

    #[tokio::test]
    async fn extended_query_decodes_binary_bool_parameter() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        let params = [binary_bool_param(true)];

        let results = session
            .test_extended_query("SELECT $1", &params)
            .await
            .expect("extended binary bool select");

        assert_eq!(single_text(&results), "t");
    }

    #[tokio::test]
    async fn extended_query_binds_int8_and_float8_binary_values_in_dml_and_predicates() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE t (id int8, score float8)")
            .await
            .expect("create table");
        let params = [
            binary_param(&9_000_000_000_i64.to_be_bytes(), crabka_pgtypes::oids::INT8),
            binary_param(&1.5_f64.to_be_bytes(), crabka_pgtypes::oids::FLOAT8),
        ];
        session
            .test_extended_query("INSERT INTO t VALUES ($1, $2)", &params)
            .await
            .expect("insert binary scalar values");

        let results = session
            .test_extended_query("SELECT id FROM t WHERE id = $1 AND score = $2", &params)
            .await
            .expect("predicate with binary scalar values");
        assert_eq!(single_text(&results), "9000000000");
    }

    #[tokio::test]
    async fn extended_query_binds_bytea_uuid_and_date_text_and_binary_values() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE t (occurred date)")
            .await
            .expect("create table");
        let uuid = [
            0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12, 0x34,
            0x56, 0x78,
        ];
        let params = [
            text_param(Some("\\xdeadbeef"), Some(crabka_pgtypes::oids::BYTEA)),
            binary_param(&uuid, crabka_pgtypes::oids::UUID),
            binary_param(&8_767_i32.to_be_bytes(), crabka_pgtypes::oids::DATE),
        ];
        session
            .test_extended_query("SELECT $1", &params[..1])
            .await
            .map(|results| assert_eq!(single_text(&results), "\\xdeadbeef"))
            .expect("decode text bytea");
        session
            .test_extended_query("SELECT $1", &params[1..2])
            .await
            .map(|results| {
                assert_eq!(
                    single_text(&results),
                    "12345678-1234-5678-90ab-cdef12345678"
                );
            })
            .expect("decode binary uuid");
        session
            .test_extended_query("INSERT INTO t VALUES ($1)", &params[2..])
            .await
            .expect("insert binary date");

        let results = session
            .test_extended_query("SELECT occurred FROM t WHERE occurred = $1", &params[2..])
            .await
            .expect("predicate with binary date");
        assert_eq!(single_text(&results), "2024-01-02");
    }

    #[test]
    fn bind_decoder_rejects_malformed_binary_and_invalid_utf8() {
        let time_zone = jiff::tz::TimeZone::UTC;
        let malformed = binary_param(&[0, 1], crabka_pgtypes::oids::INT8);
        let error = decode_bound_param(
            malformed.value.as_deref().expect("binary value"),
            &malformed,
            ColumnType::Int8,
            &time_zone,
        )
        .expect_err("short int8 must be malformed binary");
        assert_eq!(error.code, "22P03");

        let invalid_utf8 = crabka_pgwire::engine::BoundParam {
            type_oid: Some(crabka_pgtypes::oids::TEXT),
            format: 0,
            value: Some(bytes::Bytes::from_static(&[0xff])),
        };
        let error = decode_bound_param(
            invalid_utf8.value.as_deref().expect("text value"),
            &invalid_utf8,
            ColumnType::Text,
            &time_zone,
        )
        .expect_err("invalid UTF-8 must fail");
        assert_eq!(error.code, "22021");
    }

    #[tokio::test]
    async fn extended_query_preserves_null_parameter() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        let params = [text_param(None, None)];

        let results = session
            .test_extended_query("SELECT $1::int4", &params)
            .await
            .expect("extended null select");

        let [crabka_pgwire::engine::QueryResult::Rows { rows, .. }] = &results[..] else {
            panic!("expected rows, got {results:?}");
        };
        assert!(rows[0][0].is_none());
    }

    #[tokio::test]
    async fn extended_query_parameterized_insert_round_trips() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE t (id int4, name text)")
            .await
            .expect("create table");
        let params = [
            text_param(Some("7"), Some(crabka_pgtypes::oids::INT4)),
            text_param(Some("seven"), Some(crabka_pgtypes::oids::TEXT)),
        ];

        session
            .test_extended_query("INSERT INTO t (id, name) VALUES ($1, $2)", &params)
            .await
            .expect("insert with params");
        let selected = session
            .simple_query("SELECT name FROM t WHERE id = 7")
            .await
            .expect("select inserted row");

        assert_eq!(single_text(&selected), "seven");
    }

    #[tokio::test]
    async fn extended_query_rejects_wrong_parameter_count() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        let params = [text_param(Some("1"), None), text_param(Some("extra"), None)];

        let err = session
            .test_extended_query("SELECT $1", &params)
            .await
            .expect_err("extra param rejected");

        assert_eq!(err.code, crabka_pgwire::error::sqlstate::PROTOCOL_VIOLATION);
        assert!(err.message.contains("supplies 2 parameters"));
    }

    #[tokio::test]
    async fn extended_registry_errors_fail_explicit_transaction_and_preserve_resources() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .parse("kept", "SELECT 1", &[])
            .await
            .expect("parse kept");
        session
            .bind("kept_portal", "kept", &[], &[])
            .await
            .expect("bind kept");

        async fn begin(session: &mut SqlSession) {
            session.simple_query("BEGIN").await.expect("begin");
            assert_eq!(session.tx_status(), TxStatus::InTransaction);
        }
        async fn recover_and_assert_resources(session: &mut SqlSession) {
            assert_eq!(session.tx_status(), TxStatus::Failed);
            session.simple_query("ROLLBACK").await.expect("rollback");
            session
                .describe_statement("kept")
                .await
                .expect("prepared preserved");
            session
                .describe_portal("kept_portal")
                .await
                .expect("portal preserved");
        }

        begin(&mut session).await;
        session
            .parse("kept", "SELECT 2", &[])
            .await
            .expect_err("duplicate statement");
        recover_and_assert_resources(&mut session).await;

        begin(&mut session).await;
        session
            .bind("new", "missing", &[], &[])
            .await
            .expect_err("missing statement");
        recover_and_assert_resources(&mut session).await;

        begin(&mut session).await;
        session
            .bind("kept_portal", "kept", &[], &[])
            .await
            .expect_err("duplicate portal");
        recover_and_assert_resources(&mut session).await;

        begin(&mut session).await;
        session
            .bind("new", "kept", &[text_param(Some("extra"), None)], &[])
            .await
            .expect_err("wrong count");
        recover_and_assert_resources(&mut session).await;

        begin(&mut session).await;
        session
            .bind("new", "kept", &[], &[7])
            .await
            .expect_err("invalid result format");
        recover_and_assert_resources(&mut session).await;

        begin(&mut session).await;
        session
            .parse("bad", "SELECT (", &[])
            .await
            .expect_err("parse error");
        recover_and_assert_resources(&mut session).await;
    }

    #[tokio::test]
    async fn extended_query_rejects_wrong_inferred_type() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE t (id int4)")
            .await
            .expect("create table");
        let params = [text_param(Some("not-an-int"), None)];

        let err = session
            .test_extended_query("INSERT INTO t VALUES ($1)", &params)
            .await
            .expect_err("bad int rejected");

        assert_eq!(err.code, "22P02");
        assert!(err.message.contains("integer"));
    }

    #[tokio::test]
    async fn extended_prepare_rejects_multiple_statements() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();

        let err = session
            .test_describe_prepared("SELECT 1; SELECT 2", &[])
            .await
            .expect_err("extended parse rejects multi-statement prepare");

        assert_eq!(err.code, crabka_pgwire::error::sqlstate::SYNTAX_ERROR);
        assert!(err.message.contains("multiple commands"));
    }

    #[tokio::test]
    async fn empty_extended_sql_succeeds_outside_failed_transaction() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();

        let results = session
            .test_extended_query(" \t\n", &[])
            .await
            .expect("empty extended query succeeds");
        assert!(matches!(
            results.as_slice(),
            [crabka_pgwire::engine::QueryResult::Empty]
        ));

        let (fields, param_types) = session
            .test_describe_prepared(" \t\n", &[])
            .await
            .expect("empty extended describe succeeds");
        assert!(fields.is_empty());
        assert!(param_types.is_empty());
    }

    #[tokio::test]
    async fn empty_extended_sql_is_rejected_inside_failed_transaction_until_rollback() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();

        session.simple_query("BEGIN").await.expect("begin");
        let original_error = session
            .simple_query("SELECT * FROM missing_table")
            .await
            .expect_err("undefined table aborts explicit transaction");
        assert_eq!(original_error.code, "42P01");

        let execute_error = session
            .test_extended_query("", &[])
            .await
            .expect_err("empty extended execute is rejected in failed transaction");
        assert_eq!(execute_error.code, "25P02");

        let describe_error = session
            .test_describe_prepared("", &[])
            .await
            .expect_err("empty extended prepare/describe is rejected in failed transaction");
        assert_eq!(describe_error.code, "25P02");

        session.simple_query("ROLLBACK").await.expect("rollback");
        session
            .test_extended_query("", &[])
            .await
            .expect("rollback restores empty extended execute behavior");
        session
            .test_describe_prepared("", &[])
            .await
            .expect("rollback restores empty extended describe behavior");
        let selected = session
            .simple_query("SELECT 1")
            .await
            .expect("rollback restores session usability");
        assert_eq!(single_text(&selected), "1");
    }

    /// SP37: `SET TIME ZONE` flows through the GUC into `eval_ctx()`, so a
    /// `timestamptz` renders in the session zone; `SHOW timezone` reads it back;
    /// and a ROLLBACK reverts a `SET` made inside a transaction.
    #[tokio::test]
    async fn set_timezone_flows_into_rendering_and_show() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();

        // Default zone is UTC.
        let show = s.simple_query("SHOW timezone").await.expect("show");
        assert_eq!(single_text(&show), "UTC");
        let utc = s
            .simple_query("SELECT TIMESTAMPTZ '2024-01-15 12:00:00+00'")
            .await
            .expect("select utc");
        assert_eq!(single_text(&utc), "2024-01-15 12:00:00+00");

        // SET TIME ZONE (autocommit) persists and feeds eval_ctx().
        s.simple_query("SET TIME ZONE 'America/New_York'")
            .await
            .expect("set tz");
        let show_ny = s.simple_query("SHOW timezone").await.expect("show ny");
        assert_eq!(single_text(&show_ny), "America/New_York");
        let ny = s
            .simple_query("SELECT TIMESTAMPTZ '2024-01-15 12:00:00+00'")
            .await
            .expect("select ny");
        assert_eq!(single_text(&ny), "2024-01-15 07:00:00-05");

        // A SET inside a transaction reverts on ROLLBACK.
        s.simple_query("BEGIN").await.expect("begin");
        s.simple_query("SET TIME ZONE 'UTC'")
            .await
            .expect("set utc");
        let inside = s.simple_query("SHOW timezone").await.expect("show inside");
        assert_eq!(single_text(&inside), "UTC");
        s.simple_query("ROLLBACK").await.expect("rollback");
        let after = s.simple_query("SHOW timezone").await.expect("show after");
        assert_eq!(single_text(&after), "America/New_York");

        // A SET inside a transaction persists on COMMIT.
        s.simple_query("BEGIN").await.expect("begin2");
        s.simple_query("SET TIME ZONE 'UTC'")
            .await
            .expect("set utc2");
        s.simple_query("COMMIT").await.expect("commit");
        let committed = s
            .simple_query("SHOW timezone")
            .await
            .expect("show committed");
        assert_eq!(single_text(&committed), "UTC");
    }

    /// SP37: SET LOCAL is always reverted at end-of-transaction; an unknown
    /// parameter is 42704; a bad zone is 22023.
    #[tokio::test]
    async fn set_local_reverts_and_errors_have_right_sqlstate() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();

        s.simple_query("BEGIN").await.expect("begin");
        s.simple_query("SET LOCAL TIME ZONE 'America/New_York'")
            .await
            .expect("set local");
        let inside = s.simple_query("SHOW timezone").await.expect("show local");
        assert_eq!(single_text(&inside), "America/New_York");
        // COMMIT drops a LOCAL override (never promoted).
        s.simple_query("COMMIT").await.expect("commit");
        let after = s.simple_query("SHOW timezone").await.expect("show after");
        assert_eq!(single_text(&after), "UTC");

        // Unknown parameter → 42704.
        let unknown = s
            .simple_query("SET nonexistent_param = 'x'")
            .await
            .expect_err("unknown param");
        assert_eq!(unknown.code, "42704");
        let unknown_show = s
            .simple_query("SHOW nonexistent_param")
            .await
            .expect_err("unknown show");
        assert_eq!(unknown_show.code, "42704");

        // Invalid zone → 22023.
        let bad = s
            .simple_query("SET timezone = 'Not/AZone'")
            .await
            .expect_err("bad zone");
        assert_eq!(bad.code, "22023");
    }

    #[tokio::test]
    async fn common_gucs_support_preamble_show_reset_and_discard() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();

        s.simple_query("SET application_name = 'sqlx'")
            .await
            .expect("application_name");
        s.simple_query("SET client_encoding = 'UTF8'")
            .await
            .expect("client_encoding");
        s.simple_query("SET standard_conforming_strings = on")
            .await
            .expect("standard_conforming_strings");
        s.simple_query("SET search_path = public")
            .await
            .expect("search_path");
        s.simple_query("SET statement_timeout = 0")
            .await
            .expect("statement_timeout");

        assert_eq!(
            single_text(&s.simple_query("SHOW application_name").await.expect("show")),
            "sqlx"
        );
        assert_eq!(
            single_text(&s.simple_query("SHOW client_encoding").await.expect("show")),
            "UTF8"
        );

        s.simple_query("RESET ALL").await.expect("reset all");
        assert_eq!(
            single_text(&s.simple_query("SHOW application_name").await.expect("show")),
            ""
        );

        s.simple_query("SET application_name = 'psql' ")
            .await
            .expect("set again");
        s.simple_query("DISCARD ALL").await.expect("discard all");
        assert_eq!(
            single_text(&s.simple_query("SHOW application_name").await.expect("show")),
            ""
        );
    }

    #[tokio::test]
    async fn sqlx_extra_float_digits_preamble_is_accepted() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();

        session
            .simple_query("SET extra_float_digits = 2")
            .await
            .expect("sqlx startup setting");
        assert_eq!(
            single_text(
                &session
                    .simple_query("SHOW extra_float_digits")
                    .await
                    .expect("show")
            ),
            "2"
        );
    }

    #[test]
    fn extra_float_digits_matches_postgres_integer_guc_input() {
        assert_eq!(guc_default("extra_float_digits"), "1");
        assert_eq!(guc_vartype("extra_float_digits"), "integer");

        for (input, expected) in [
            ("-15", "-15"),
            ("3", "3"),
            ("  +2  ", "2"),
            ("1.4", "1"),
            ("1.6", "2"),
            ("1.5", "2"),
            ("-1.5", "-2"),
            ("0x2", "2"),
            ("+0x2", "2"),
            ("0o2", "2"),
        ] {
            assert_eq!(
                canonical_guc_value("extra_float_digits", input)
                    .expect("accepted by PostgreSQL 18"),
                expected
            );
        }
        for input in ["-16", "4", "010", "nope"] {
            assert!(matches!(
                canonical_guc_value("extra_float_digits", input),
                Err(ExecError::InvalidParameterValue(_))
            ));
        }
    }

    #[tokio::test]
    async fn extra_float_digits_obeys_set_and_set_local_transactions() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();

        session.simple_query("BEGIN").await.expect("begin");
        session
            .simple_query("SET extra_float_digits = 2")
            .await
            .expect("set");
        session.simple_query("COMMIT").await.expect("commit");
        assert_eq!(
            single_text(
                &session
                    .simple_query("SHOW extra_float_digits")
                    .await
                    .expect("show")
            ),
            "2"
        );

        session.simple_query("BEGIN").await.expect("begin");
        session
            .simple_query("SET extra_float_digits = 3")
            .await
            .expect("set");
        session.simple_query("ROLLBACK").await.expect("rollback");
        assert_eq!(
            single_text(
                &session
                    .simple_query("SHOW extra_float_digits")
                    .await
                    .expect("show")
            ),
            "2"
        );

        session.simple_query("BEGIN").await.expect("begin");
        session
            .simple_query("SET LOCAL extra_float_digits = '-15'")
            .await
            .expect("set local");
        assert_eq!(
            single_text(
                &session
                    .simple_query("SHOW extra_float_digits")
                    .await
                    .expect("show")
            ),
            "-15"
        );
        session.simple_query("COMMIT").await.expect("commit");
        assert_eq!(
            single_text(
                &session
                    .simple_query("SHOW extra_float_digits")
                    .await
                    .expect("show")
            ),
            "2"
        );

        for input in ["-16", "4"] {
            let error = session
                .simple_query(&format!("SET extra_float_digits = '{input}'"))
                .await
                .expect_err("out of range");
            assert_eq!(error.code, "22023");
        }
    }

    #[test]
    fn guc_mutations_follow_statement_order_and_reset_to_source() {
        let mut gucs = GucState::default();
        gucs.set("application_name", "session-one", false).unwrap();
        gucs.set("application_name", "local-one", true).unwrap();
        assert_eq!(gucs.effective("application_name").unwrap(), "local-one");
        gucs.set("application_name", "session-two", false).unwrap();
        assert_eq!(gucs.effective("application_name").unwrap(), "session-two");
        gucs.commit();
        assert_eq!(gucs.effective("application_name").unwrap(), "session-two");

        let mut source = BTreeMap::new();
        source.insert("application_name".to_string(), "from-source".to_string());
        let mut gucs = GucState::with_source_values(source).unwrap();
        gucs.set("application_name", "changed", false).unwrap();
        gucs.commit();
        gucs.reset("application_name").unwrap();
        gucs.commit();
        assert_eq!(gucs.effective("application_name").unwrap(), "from-source");
    }

    #[test]
    fn typed_guc_parsers_match_postgres_18_canonical_forms() {
        let mut gucs = GucState::default();
        gucs.set("DateStyle", "SQL, European", false).unwrap();
        assert_eq!(gucs.effective("DateStyle").unwrap(), "SQL, DMY");
        gucs.set("IntervalStyle", "POSTGRES_VERBOSE", false)
            .unwrap();
        assert_eq!(gucs.effective("IntervalStyle").unwrap(), "postgres_verbose");
        gucs.set("statement_timeout", "1.5s", false).unwrap();
        assert_eq!(gucs.effective("statement_timeout").unwrap(), "1500ms");
        gucs.set("statement_timeout", "1 min", false).unwrap();
        assert_eq!(gucs.effective("statement_timeout").unwrap(), "1min");
        assert!(gucs.set("DateStyle", "nonsense", false).is_err());
        assert!(gucs.set("IntervalStyle", "nonsense", false).is_err());
        assert!(gucs.set("statement_timeout", "-1", false).is_err());
    }

    #[test]
    fn datestyle_partial_assignment_inherits_effective_components() {
        let mut gucs = GucState::default();
        gucs.set("DateStyle", "SQL, DMY", false).unwrap();
        gucs.set("DateStyle", "MDY", false).unwrap();
        assert_eq!(gucs.effective("DateStyle").unwrap(), "SQL, MDY");
        gucs.set("DateStyle", "German", false).unwrap();
        assert_eq!(gucs.effective("DateStyle").unwrap(), "German, MDY");
    }

    #[tokio::test]
    async fn datestyle_partial_sql_assignment_matches_postgres_18() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query("SET DateStyle TO SQL DMY; SET DateStyle TO MDY")
            .await
            .unwrap();
        assert_eq!(
            single_text(&session.simple_query("SHOW DateStyle").await.unwrap()),
            "SQL, MDY"
        );
        session
            .simple_query("SELECT set_config('DateStyle', 'YMD', false)")
            .await
            .unwrap();
        assert_eq!(
            single_text(&session.simple_query("SHOW DateStyle").await.unwrap()),
            "SQL, YMD"
        );
    }

    #[test]
    fn statement_timeout_extended_units_match_postgres_18() {
        let mut gucs = GucState::default();
        for (input, expected) in [(".5s", "500ms"), ("1h", "1h"), ("1d", "1d")] {
            gucs.set("statement_timeout", input, false).unwrap();
            assert_eq!(gucs.effective("statement_timeout").unwrap(), expected);
        }
        for invalid in ["-0.5s", "25d", "NaN", "1fortnight"] {
            assert!(gucs.set("statement_timeout", invalid, false).is_err());
        }
    }

    #[tokio::test]
    async fn statement_timeout_extended_units_work_through_sql() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        for (input, expected) in [(".5s", "500ms"), ("1h", "1h"), ("1d", "1d")] {
            session
                .simple_query(&format!("SET statement_timeout TO {input}"))
                .await
                .unwrap();
            assert_eq!(
                single_text(
                    &session
                        .simple_query("SHOW statement_timeout")
                        .await
                        .unwrap()
                ),
                expected
            );
        }
    }

    #[tokio::test]
    async fn set_transaction_must_precede_first_query() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session.simple_query("BEGIN").await.unwrap();
        session
            .simple_query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await
            .unwrap();
        session.simple_query("ROLLBACK").await.unwrap();

        session.simple_query("BEGIN; SELECT 1").await.unwrap();
        let error = session
            .simple_query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await
            .unwrap_err();
        assert_eq!(error.code, "25001");
        assert_eq!(session.tx_status(), TxStatus::Failed);
        session.simple_query("ROLLBACK").await.unwrap();
    }

    #[tokio::test]
    async fn set_transaction_rejects_after_successful_dml() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE f1_activity_dml (id int4)")
            .await
            .unwrap();
        session
            .simple_query("INSERT INTO f1_activity_dml VALUES (1)")
            .await
            .unwrap();

        for sql in [
            "INSERT INTO f1_activity_dml VALUES (2)",
            "UPDATE f1_activity_dml SET id = 3 WHERE id = 1",
            "DELETE FROM f1_activity_dml WHERE id = 1",
        ] {
            session.simple_query("BEGIN").await.unwrap();
            session.simple_query(sql).await.unwrap();
            let error = session
                .simple_query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                .await
                .unwrap_err();
            assert_eq!(error.code, "25001", "late SET after {sql}");
            assert_eq!(session.tx_status(), TxStatus::Failed);
            session.simple_query("ROLLBACK").await.unwrap();
        }
    }

    #[tokio::test]
    async fn set_transaction_rejects_after_successful_ddl() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session.simple_query("BEGIN").await.unwrap();
        session
            .simple_query("CREATE TABLE f1_activity_ddl (id int4)")
            .await
            .unwrap();
        let error = session
            .simple_query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await
            .unwrap_err();
        assert_eq!(error.code, "25001");
        assert_eq!(session.tx_status(), TxStatus::Failed);
        session.simple_query("ROLLBACK").await.unwrap();
    }

    #[tokio::test]
    async fn set_transaction_remains_allowed_after_session_controls() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session.simple_query("BEGIN").await.unwrap();
        session.simple_query("SHOW application_name").await.unwrap();
        session
            .simple_query("SET application_name = 'control-only'")
            .await
            .unwrap();
        session
            .simple_query("RESET application_name")
            .await
            .unwrap();
        session
            .simple_query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await
            .unwrap();
        session.simple_query("ROLLBACK").await.unwrap();
    }

    #[tokio::test]
    async fn current_setting_and_set_config_follow_transaction_scope() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();

        let initial = s
            .simple_query("SELECT current_setting('application_name')")
            .await
            .expect("current_setting");
        assert_eq!(single_text(&initial), "");

        let missing = s
            .simple_query("SELECT current_setting('no_such_setting', true)")
            .await
            .expect("missing ok");
        let [crabka_pgwire::engine::QueryResult::Rows { rows, .. }] = &missing[..] else {
            panic!("expected rows, got {missing:?}");
        };
        assert!(rows[0][0].is_none());

        let set = s
            .simple_query("SELECT set_config('application_name', 'from-func', false)")
            .await
            .expect("set_config");
        assert_eq!(single_text(&set), "from-func");
        assert_eq!(
            single_text(&s.simple_query("SHOW application_name").await.expect("show")),
            "from-func"
        );

        s.simple_query("BEGIN").await.expect("begin");
        s.simple_query("SELECT set_config('application_name', 'local-only', true)")
            .await
            .expect("set local");
        assert_eq!(
            single_text(&s.simple_query("SHOW application_name").await.expect("show")),
            "local-only"
        );
        s.simple_query("COMMIT").await.expect("commit");
        assert_eq!(
            single_text(&s.simple_query("SHOW application_name").await.expect("show")),
            "from-func"
        );
    }

    #[tokio::test]
    async fn guc_sql_interleavings_match_postgres_18() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();

        session.simple_query("BEGIN").await.unwrap();
        session
            .simple_query("SET application_name = 's1'")
            .await
            .unwrap();
        session
            .simple_query("SET LOCAL application_name = 'l1'")
            .await
            .unwrap();
        session
            .simple_query("SET application_name = 's2'")
            .await
            .unwrap();
        assert_eq!(
            single_text(&session.simple_query("SHOW application_name").await.unwrap()),
            "s2"
        );
        session.simple_query("COMMIT").await.unwrap();
        assert_eq!(
            single_text(&session.simple_query("SHOW application_name").await.unwrap()),
            "s2"
        );

        session.simple_query("BEGIN").await.unwrap();
        session
            .simple_query("SET LOCAL application_name = 'l2'")
            .await
            .unwrap();
        session
            .simple_query("SET application_name = 's3'")
            .await
            .unwrap();
        session
            .simple_query("SET LOCAL application_name = 'l3'")
            .await
            .unwrap();
        assert_eq!(
            single_text(&session.simple_query("SHOW application_name").await.unwrap()),
            "l3"
        );
        session.simple_query("COMMIT").await.unwrap();
        assert_eq!(
            single_text(&session.simple_query("SHOW application_name").await.unwrap()),
            "s3"
        );

        session.simple_query("BEGIN").await.unwrap();
        session
            .simple_query("SET statement_timeout = 17")
            .await
            .unwrap();
        assert_eq!(
            single_text(
                &session
                    .simple_query("SHOW statement_timeout")
                    .await
                    .unwrap()
            ),
            "17ms"
        );
        let error = session
            .simple_query("DISCARD ALL")
            .await
            .expect_err("transaction block");
        assert_eq!(error.code, "25001");
        session.simple_query("ROLLBACK").await.unwrap();
    }

    #[tokio::test]
    async fn discard_all_clears_extended_resources_and_keeps_session_usable() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session.parse("gone", "SELECT 1", &[]).await.unwrap();
        session.bind("gone_portal", "gone", &[], &[]).await.unwrap();

        session.simple_query("DISCARD ALL").await.unwrap();

        assert!(session.describe_statement("gone").await.is_err());
        assert!(session.describe_portal("gone_portal").await.is_err());
        assert_eq!(
            single_text(&session.simple_query("SELECT 1").await.unwrap()),
            "1"
        );
    }

    #[tokio::test]
    async fn source_values_drive_default_reset_all_discard_and_pg_settings() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        let mut source = BTreeMap::new();
        source.insert(
            "application_name".to_string(),
            "configured-source".to_string(),
        );
        session.guc = GucState::with_source_values(source).unwrap();

        session
            .simple_query("SET application_name = 'changed'; SET application_name = DEFAULT")
            .await
            .unwrap();
        assert_eq!(
            single_text(&session.simple_query("SHOW application_name").await.unwrap()),
            "configured-source"
        );
        session.simple_query("BEGIN").await.unwrap();
        session
            .simple_query("SET application_name = 'transaction-session'")
            .await
            .unwrap();
        session
            .simple_query("SET LOCAL application_name = DEFAULT")
            .await
            .unwrap();
        assert_eq!(
            single_text(&session.simple_query("SHOW application_name").await.unwrap()),
            "configured-source"
        );
        session.simple_query("COMMIT").await.unwrap();
        assert_eq!(
            single_text(&session.simple_query("SHOW application_name").await.unwrap()),
            "transaction-session"
        );
        session
            .simple_query("SELECT set_config('application_name', 'function-change', false)")
            .await
            .unwrap();
        session.simple_query("RESET ALL").await.unwrap();
        assert_eq!(
            single_text(&session.simple_query("SHOW application_name").await.unwrap()),
            "configured-source"
        );
        let settings = session
            .simple_query(
                "SELECT boot_val, reset_val FROM pg_catalog.pg_settings \
                 WHERE name = 'application_name'",
            )
            .await
            .unwrap();
        let [QueryResult::Rows { rows, .. }] = settings.as_slice() else {
            panic!("expected pg_settings row: {settings:?}");
        };
        assert_eq!(rows[0][0].as_ref().unwrap().text.as_ref(), b"");
        assert_eq!(
            rows[0][1].as_ref().unwrap().text.as_ref(),
            b"configured-source"
        );

        session
            .simple_query("SET application_name = 'before-discard'")
            .await
            .unwrap();
        session.simple_query("DISCARD ALL").await.unwrap();
        assert_eq!(
            single_text(&session.simple_query("SHOW application_name").await.unwrap()),
            "configured-source"
        );
    }

    #[tokio::test]
    async fn rejected_discard_preserves_role_gucs_and_extended_resources() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session.parse("kept", "SELECT 1", &[]).await.unwrap();
        session.bind("kept_portal", "kept", &[], &[]).await.unwrap();
        session
            .simple_query("SET application_name = 'kept'; BEGIN")
            .await
            .unwrap();
        session.current_role = "role-before-discard".into();

        let error = session.simple_query("DISCARD ALL").await.unwrap_err();
        assert_eq!(error.code, "25001");
        assert_eq!(session.current_role, "role-before-discard");
        assert_eq!(session.guc.effective("application_name").unwrap(), "kept");
        assert!(session.describe_statement("kept").await.is_ok());
        assert!(session.describe_portal("kept_portal").await.is_ok());

        session.simple_query("ROLLBACK; DISCARD ALL").await.unwrap();
        assert_eq!(session.current_role, session.session_user);
        assert_eq!(
            single_text(&session.simple_query("SELECT 1").await.unwrap()),
            "1"
        );
    }

    /// A session dropped while a write transaction is open (client disconnect)
    /// must deregister its xid from the ProcArray so it no longer pins
    /// `snapshot().xmin`.
    #[tokio::test]
    async fn dropping_a_session_mid_txn_deregisters_its_xid() {
        let engine = SqlEngine::new();

        {
            let mut s = engine.connect();
            s.simple_query("CREATE TABLE t (id int4)")
                .await
                .expect("create");
            s.simple_query("BEGIN").await.expect("begin");
            s.simple_query("INSERT INTO t VALUES (1)")
                .await
                .expect("insert");
            assert_eq!(
                engine.procarray.running_len(),
                1,
                "xid must be registered while the transaction is open"
            );
            // s is dropped here, mid-transaction (no COMMIT/ROLLBACK)
        }

        assert_eq!(
            engine.procarray.running_len(),
            0,
            "xid must be deregistered when the session is dropped mid-transaction"
        );
    }

    /// SP40: `IMPORT FOREIGN SCHEMA` materializes one foreign table per
    /// `(name, value_columns)` the registered scanner returns, with the envelope
    /// columns prepended and `OPTIONS (topic '<name>')` recorded. Driven by a fake
    /// scanner (no Kafka), so it proves the `execute_ddl` wiring in isolation.
    #[tokio::test]
    async fn import_foreign_schema_creates_tables_from_scanner() {
        use std::sync::Arc;

        use crabka_pgcatalog::{Column, ForeignServer, Table, UserMapping};
        use crabka_pgtypes::{ColumnType, Datum};

        use crate::{
            clock::EvalCtx,
            error::ExecError,
            foreign::{ForeignScanner, ImportFilter, ImportedTable, ScanBounds},
        };

        /// Returns two canned tables; records the filter it was handed.
        struct FakeImporter;
        impl ForeignScanner for FakeImporter {
            fn scan(
                &self,
                _table: &Table,
                _server: &ForeignServer,
                _mapping: Option<&UserMapping>,
                _bounds: &ScanBounds,
                _ctx: &EvalCtx,
            ) -> Result<Vec<Vec<Datum>>, ExecError> {
                Ok(Vec::new())
            }

            fn import_schema(
                &self,
                _server: &ForeignServer,
                _mapping: Option<&UserMapping>,
                filter: &ImportFilter,
            ) -> Result<Vec<ImportedTable>, ExecError> {
                // Honor the filter so the test can assert LIMIT TO works end-to-end.
                let all = vec![
                    ImportedTable {
                        name: "orders".to_string(),
                        columns: vec![Column::new("id", ColumnType::Int8)],
                        options: vec![
                            ("topic".to_string(), "orders".to_string()),
                            ("value_format".to_string(), "raw".to_string()),
                        ],
                    },
                    ImportedTable {
                        name: "payments".to_string(),
                        columns: vec![Column::new("amount", ColumnType::Float8)],
                        options: vec![
                            ("topic".to_string(), "payments".to_string()),
                            ("value_format".to_string(), "raw".to_string()),
                        ],
                    },
                ];
                Ok(all
                    .into_iter()
                    .filter(|t| filter.retains(&t.name))
                    .collect())
            }
        }

        let mut engine = SqlEngine::new();
        engine.set_foreign_scanner(Arc::new(FakeImporter));
        let mut s = engine.connect();

        s.simple_query(
            "CREATE SERVER k FOREIGN DATA WRAPPER kafka_fdw OPTIONS (bootstrap 'b:9092')",
        )
        .await
        .expect("create server");

        // LIMIT TO (orders) — only `orders` should be imported.
        let res = s
            .simple_query("IMPORT FOREIGN SCHEMA kafka LIMIT TO (orders) FROM SERVER k")
            .await
            .expect("import");
        assert!(
            matches!(&res[..], [crabka_pgwire::engine::QueryResult::Command { tag }] if tag == "IMPORT FOREIGN SCHEMA"),
            "expected IMPORT FOREIGN SCHEMA command tag, got {res:?}"
        );

        // `orders` exists as a foreign table; envelope columns are prepended, then
        // the value column `id`; OPTIONS carries the topic name.
        let orders =
            crabka_pgcatalog::get_table(&*engine.kv, "orders").expect("orders table exists");
        let meta = orders.foreign.expect("orders is a foreign table");
        assert_eq!(meta.server, "k");
        assert!(
            meta.options
                .contains(&("topic".to_string(), "orders".to_string()))
        );
        let last = orders.columns.last().expect("at least one value column");
        assert_eq!(last.name, "id");
        assert!(
            orders.columns.len() > 1,
            "envelope columns must be prepended before the value column"
        );

        // `payments` was excluded by LIMIT TO and must not exist.
        assert!(
            crabka_pgcatalog::get_table(&*engine.kv, "payments").is_err(),
            "payments was not in LIMIT TO and must not be imported"
        );
    }
}
#[cfg(test)]
mod compatibility_refusal_tests {
    use crabka_pgwire::engine::{Engine, Session};

    use crate::SqlEngine;

    #[tokio::test]
    async fn compatibility_refusals_execute_with_centralized_error_contracts() {
        let engine = SqlEngine::new();
        let cases = [
            (
                "ALTER DATABASE postgres RENAME TO other",
                "0A000",
                "database lifecycle",
            ),
            ("CREATE DATABASE other", "0A000", "database lifecycle"),
            ("DROP DATABASE other", "0A000", "database lifecycle"),
            (
                "ALTER EXTENSION plpgsql UPDATE",
                "0A000",
                "extension lifecycle",
            ),
            ("DROP EXTENSION plpgsql", "0A000", "extension lifecycle"),
            (
                "PREPARE TRANSACTION 'xid-1'",
                "55000",
                "SQL-level prepared transactions",
            ),
            (
                "COMMIT PREPARED 'xid-1'",
                "55000",
                "SQL-level prepared transactions",
            ),
            (
                "ROLLBACK PREPARED 'xid-1'",
                "55000",
                "SQL-level prepared transactions",
            ),
        ];

        for (sql, sqlstate, message) in cases {
            let mut session = engine.connect();
            let error = session.simple_query(sql).await.expect_err(sql);
            assert_eq!(error.code, sqlstate, "{sql}");
            assert!(error.message.contains(message), "{sql}: {error:?}");
        }
    }

    #[tokio::test]
    async fn every_non_goal_executes_through_session_as_exact_refusal() {
        let engine = SqlEngine::new();
        for spec in crabka_pgparser::ast::NON_GOAL_REFUSALS {
            let mut session = engine.connect();
            let error = session
                .simple_query(spec.representative_sql)
                .await
                .expect_err(spec.representative_sql);
            assert_eq!(error.code, "0A000", "{}", spec.command.command_name());
            assert_eq!(error.message, spec.command.message());
        }
    }
}
