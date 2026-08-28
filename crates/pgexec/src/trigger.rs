//! Trigger catalog DDL and PostgreSQL parse-analysis rules.

use crabka_pgcatalog::{
    RelationName, Table,
    routine::{Routine, RoutineResult, routines_named},
    trigger::{
        EventTrigger, EventTriggerEvent, EventTriggerFilter, Trigger, TriggerEnabled,
        TriggerEvents, TriggerLevel, TriggerTiming, drop_event_trigger_ops, drop_trigger_ops,
        get_event_trigger, get_trigger, put_event_trigger_ops, put_trigger_ops,
    },
};
use crabka_pgkv::{Kv, WriteOp};
use crabka_pgparser::ast as parsed;
use crabka_pgwire::engine::QueryResult;

use crate::error::ExecError;

#[derive(Debug, Clone)]
pub(crate) struct TriggerInvocation {
    pub name: String,
    pub when: String,
    pub level: String,
    pub operation: String,
    pub event: Option<String>,
    pub tag: Option<String>,
    pub relation_oid: u32,
    pub table_schema: String,
    pub table_name: String,
    pub arguments: Vec<String>,
    pub column_names: Vec<String>,
    pub column_types: Vec<crabka_pgtypes::ColumnType>,
    pub transitions: Vec<(String, Vec<Vec<crabka_pgtypes::Datum>>)>,
    pub old: crabka_pgtypes::Datum,
    pub new: crabka_pgtypes::Datum,
}

thread_local! {
    static TRIGGER_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    /// Triggers fired on this thread since it started, so a write can report
    /// how many its own statement fired as a difference. See [`fired_count`].
    static TRIGGERS_FIRED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static AFTER_TRIGGER_QUEUE: std::cell::RefCell<Option<Vec<PendingTrigger>>> = const { std::cell::RefCell::new(None) };
    static TRANSITION_CHANGES: std::cell::RefCell<Option<Vec<TransitionChange>>> = const { std::cell::RefCell::new(None) };
}

#[derive(Clone)]
struct TransitionChange {
    table_id: u32,
    operation: String,
    old: Option<Vec<crabka_pgtypes::Datum>>,
    new: Option<Vec<crabka_pgtypes::Datum>>,
}

#[derive(Debug, Clone)]
pub(crate) struct PendingTrigger {
    pub function_oid: u32,
    pub function: String,
    pub invocation: TriggerInvocation,
    pub table_id: u32,
    pub name: String,
    pub constraint: bool,
    pub deferrable: bool,
    pub initially_deferred: bool,
    pub old_transition: Option<String>,
    pub new_transition: Option<String>,
}

/// Monotonic count of the triggers this thread has fired.
///
/// A statement's own count is the difference between two readings, which is
/// what `pg.execute_write` records as `pg.triggers_fired`. This is a running
/// total and not a per-statement counter, because triggers fire in a nested
/// way. A trigger's own DML fires more triggers, and there is no single place
/// to reset the counter that a nested write would not clobber.
///
/// The counter is thread-local because the executor's whole write path runs on
/// one blocking worker thread under a current-thread runtime. The after-trigger
/// queue above already makes the same assumption.
pub(crate) fn fired_count() -> u64 {
    TRIGGERS_FIRED.with(std::cell::Cell::get)
}

/// Count one trigger as fired.
///
/// The trigger is either invoked now or queued to run at the end of the
/// statement. The statement caused both cases.
fn note_fired() {
    TRIGGERS_FIRED.with(|fired| fired.set(fired.get().saturating_add(1)));
}

pub(crate) fn with_after_trigger_queue<T>(f: impl FnOnce() -> T) -> (T, Vec<PendingTrigger>) {
    AFTER_TRIGGER_QUEUE.with(|cell| {
        let previous = cell.replace(Some(Vec::new()));
        let previous_changes = TRANSITION_CHANGES.with(|changes| changes.replace(Some(Vec::new())));
        let result = f();
        let mut queued = cell.replace(previous).unwrap_or_default();
        let changes = TRANSITION_CHANGES
            .with(|cell| cell.replace(previous_changes))
            .unwrap_or_default();
        for pending in &mut queued {
            for (alias, old) in [
                (pending.old_transition.as_ref(), true),
                (pending.new_transition.as_ref(), false),
            ] {
                let Some(alias) = alias else { continue };
                let rows = changes
                    .iter()
                    .filter(|candidate| {
                        candidate.table_id == pending.table_id
                            && candidate.operation == pending.invocation.operation
                    })
                    .filter_map(|candidate| {
                        let value = if old { &candidate.old } else { &candidate.new };
                        value.clone()
                    })
                    .collect();
                pending.invocation.transitions.push((alias.clone(), rows));
            }
        }
        (result, queued)
    })
}

struct TriggerDepthGuard;

impl Drop for TriggerDepthGuard {
    fn drop(&mut self) {
        TRIGGER_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

pub(crate) fn invoke(
    routine: Routine,
    invocation: TriggerInvocation,
) -> Result<crabka_pgtypes::Datum, ExecError> {
    let runtime = crate::routine::scalar_runtime_request_sender().ok_or_else(|| {
        ExecError::Unsupported("trigger function requires a session executor".into())
    })?;
    TRIGGER_DEPTH.with(|depth| depth.set(depth.get() + 1));
    let _guard = TriggerDepthGuard;
    let (reply, response) = std::sync::mpsc::channel();
    runtime
        .try_send(crate::routine::ScalarFunctionRequest {
            routine: Some(routine),
            values: Vec::new(),
            kind: crate::routine::FunctionRequestKind::Trigger(Box::new(invocation)),
            command_row_claims: crate::routine::scalar_runtime_command_row_claims(),
            reply,
        })
        .map_err(|_| {
            ExecError::ObjectNotInPrerequisiteState("trigger function executor stopped".into())
        })?;
    let (result, mutations) = response.recv().map_err(|_| {
        ExecError::ObjectNotInPrerequisiteState("trigger function executor stopped".into())
    })??;
    crate::session::apply_guc_runtime_mutations(mutations)?;
    match result {
        crate::routine::FunctionRequestResult::Scalar(value) => Ok(value),
        crate::routine::FunctionRequestResult::Table(_) => Err(
            ExecError::ObjectNotInPrerequisiteState("trigger function returned a table".into()),
        ),
    }
}

fn command(tag: &str, ops: Vec<WriteOp>) -> (QueryResult, Vec<WriteOp>) {
    (
        QueryResult::Command {
            tag: tag.to_string(),
        },
        ops,
    )
}

fn trigger_error(sqlstate: &'static str, message: impl Into<String>) -> ExecError {
    ExecError::FunctionError {
        sqlstate,
        message: message.into(),
    }
}

fn trigger_error_detail(
    sqlstate: &'static str,
    message: impl Into<String>,
    detail: impl Into<String>,
) -> ExecError {
    ExecError::Remote(
        crabka_pgwire::error::PgError::error(sqlstate, message.into()).with_detail(detail.into()),
    )
}

fn trigger_relation_kind_error(relation: &RelationName, kind: &str, detail: &str) -> ExecError {
    ExecError::Remote(
        crabka_pgwire::error::PgError::error("42809", format!("\"{}\" is a {kind}", relation.name))
            .with_detail(detail),
    )
}

fn relation_target(kv: &dyn Kv, name: &RelationName) -> Result<(u32, Option<Table>), ExecError> {
    if let Ok(table) = crabka_pgcatalog::get_table(kv, name) {
        return Ok((table.id, Some(table)));
    }
    let view_oids = crate::catalog_rel::view_oids(kv)?;
    if let Some(oid) = view_oids.get(name) {
        return Ok((u32::try_from(*oid).unwrap_or(0), None));
    }
    Err(ExecError::Catalog(
        crabka_pgcatalog::CatalogError::UndefinedTable(name.to_string()),
    ))
}

pub(crate) fn relation_trigger_table(kv: &dyn Kv, name: &RelationName) -> Result<Table, ExecError> {
    if let Ok(table) = crabka_pgcatalog::get_table(kv, name) {
        return Ok(table);
    }
    let view = crabka_pgcatalog::get_view(kv, name)?;
    let id = crate::catalog_rel::view_oids(kv)?
        .get(name)
        .copied()
        .and_then(|oid| u32::try_from(oid).ok())
        .unwrap_or(0);
    Ok(Table {
        owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
        id,
        name: view.name,
        columns: view.columns,
        sharded: false,
        row_security: false,
        force_row_security: false,
        sharding: None,
        foreign: None,
        materialized: None,
        checks: Vec::new(),
    })
}

fn trigger_routine(kv: &dyn Kv, written: &str, event: bool) -> Result<(u32, String), ExecError> {
    let name = written.strip_prefix("public.").unwrap_or(written);
    let builtin_oid = match (event, written) {
        (
            false,
            "pg_catalog.suppress_redundant_updates_trigger" | "suppress_redundant_updates_trigger",
        ) => Some(2022),
        (false, "pg_catalog.tsvector_update_trigger" | "tsvector_update_trigger") => Some(3743),
        (false, "pg_catalog.tsvector_update_trigger_column" | "tsvector_update_trigger_column") => {
            Some(3751)
        }
        _ => None,
    };
    if let Some(oid) = builtin_oid {
        return Ok((oid, written.to_string()));
    }
    let expected = if event { "event_trigger" } else { "trigger" };
    let mut candidates: Vec<Routine> = routines_named(kv, name)?
        .into_iter()
        .filter(|routine| routine.input_params().next().is_none())
        .collect();
    if candidates.is_empty() {
        return Err(ExecError::UndefinedFunction(format!(
            "function {written}() does not exist"
        )));
    }
    let routine = candidates.remove(0);
    let returns_expected = matches!(
        &routine.result,
        RoutineResult::Type { ty, setof: false } if ty.name == expected
    );
    if !returns_expected {
        return Err(trigger_error(
            "42P17",
            format!("function {written} must return type {expected}"),
        ));
    }
    Ok((routine.oid, routine.name))
}

fn map_timing(value: parsed::TriggerTiming) -> TriggerTiming {
    match value {
        parsed::TriggerTiming::Before => TriggerTiming::Before,
        parsed::TriggerTiming::After => TriggerTiming::After,
        parsed::TriggerTiming::InsteadOf => TriggerTiming::InsteadOf,
    }
}

fn map_level(value: parsed::TriggerLevel) -> TriggerLevel {
    match value {
        parsed::TriggerLevel::Row => TriggerLevel::Row,
        parsed::TriggerLevel::Statement => TriggerLevel::Statement,
    }
}

fn map_enabled(value: parsed::TriggerEnableMode) -> TriggerEnabled {
    match value {
        parsed::TriggerEnableMode::Origin => TriggerEnabled::Origin,
        parsed::TriggerEnableMode::Replica => TriggerEnabled::Replica,
        parsed::TriggerEnableMode::Always => TriggerEnabled::Always,
        parsed::TriggerEnableMode::Disabled => TriggerEnabled::Disabled,
    }
}

fn map_events(
    events: &[parsed::TriggerEvent],
    table: Option<&Table>,
) -> Result<TriggerEvents, ExecError> {
    let mut mapped = TriggerEvents::default();
    for event in events {
        let already = match event {
            parsed::TriggerEvent::Insert => std::mem::replace(&mut mapped.insert, true),
            parsed::TriggerEvent::Update { columns } => {
                let already = std::mem::replace(&mut mapped.update, true);
                for column in columns {
                    if let Some(table) = table {
                        if !table.columns.iter().any(|item| item.name == *column) {
                            return Err(ExecError::UndefinedTableColumn {
                                column: column.clone(),
                                table: table.name.to_string(),
                            });
                        }
                    }
                    if mapped.update_columns.contains(column) {
                        return Err(trigger_error(
                            "42701",
                            format!("column \"{column}\" specified more than once"),
                        ));
                    }
                    mapped.update_columns.push(column.clone());
                }
                already
            }
            parsed::TriggerEvent::Delete => std::mem::replace(&mut mapped.delete, true),
            parsed::TriggerEvent::Truncate => std::mem::replace(&mut mapped.truncate, true),
        };
        if already {
            return Err(trigger_error(
                "42601",
                "trigger event specified more than once",
            ));
        }
    }
    Ok(mapped)
}

pub(crate) fn create(
    kv: &dyn Kv,
    stmt: &parsed::CreateTrigger,
    table_name: RelationName,
    referenced_name: Option<RelationName>,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    let (table_id, table) = relation_target(kv, &table_name)?;
    let is_view = table.is_none();
    let timing = map_timing(stmt.timing);
    let level = map_level(stmt.level);
    let events = map_events(&stmt.events, table.as_ref())?;
    validate_when_references(stmt, table.as_ref(), timing, level, &events)?;

    if stmt.constraint && (timing != TriggerTiming::After || level != TriggerLevel::Row || is_view)
    {
        return Err(trigger_error(
            "42P17",
            "constraint triggers must be AFTER ROW triggers",
        ));
    }
    if stmt.constraint && stmt.or_replace {
        return Err(trigger_error(
            "0A000",
            "CREATE OR REPLACE CONSTRAINT TRIGGER is not supported",
        ));
    }
    if !stmt.constraint
        && (stmt.referenced_table.is_some() || stmt.deferrable || stmt.initially_deferred)
    {
        return Err(trigger_error(
            "42601",
            "FROM and deferrability clauses are only valid for constraint triggers",
        ));
    }
    if timing == TriggerTiming::InsteadOf && !is_view {
        return Err(trigger_relation_kind_error(
            &table_name,
            "table",
            "Tables cannot have INSTEAD OF triggers.",
        ));
    }
    if timing != TriggerTiming::InsteadOf && is_view && level != TriggerLevel::Statement {
        return Err(trigger_relation_kind_error(
            &table_name,
            "view",
            "Views cannot have row-level BEFORE or AFTER triggers.",
        ));
    }
    if is_view && events.truncate {
        return Err(trigger_relation_kind_error(
            &table_name,
            "view",
            "Views cannot have TRUNCATE triggers.",
        ));
    }
    if timing == TriggerTiming::InsteadOf && level != TriggerLevel::Row {
        return Err(trigger_error(
            "42P17",
            "INSTEAD OF triggers must be FOR EACH ROW",
        ));
    }
    if level == TriggerLevel::Row && events.truncate {
        return Err(trigger_error(
            "42P17",
            "TRUNCATE FOR EACH ROW triggers are not supported",
        ));
    }
    if timing == TriggerTiming::InsteadOf && stmt.when.is_some() {
        return Err(trigger_error(
            "42P17",
            "INSTEAD OF triggers cannot have WHEN conditions",
        ));
    }
    if timing == TriggerTiming::InsteadOf && events.truncate {
        return Err(trigger_error(
            "42P17",
            "INSTEAD OF triggers cannot have TRUNCATE events",
        ));
    }
    if timing == TriggerTiming::InsteadOf && !events.update_columns.is_empty() {
        return Err(trigger_error(
            "42P17",
            "INSTEAD OF triggers cannot have column lists",
        ));
    }
    if !stmt.transitions.is_empty() {
        if timing != TriggerTiming::After || stmt.constraint || is_view {
            return Err(trigger_error(
                "42P17",
                "transition tables can only be specified for AFTER non-constraint triggers",
            ));
        }
        // TRUNCATE removes every row at once and has no per-row images to
        // collect, so PostgreSQL rejects the clause outright rather than
        // reaching the OLD/NEW event rules below — which would otherwise report
        // "OLD TABLE can only be specified for …" for a TRUNCATE trigger.
        // TRUNCATE removes every row at once and has no per-row images to
        // collect, so PostgreSQL rejects the clause outright rather than
        // reaching the OLD/NEW event rules below — which would otherwise report
        // "OLD TABLE can only be specified for …" for a TRUNCATE trigger.
        if events.truncate {
            return Err(trigger_error(
                "0A000",
                "TRUNCATE triggers with transition tables are not supported",
            ));
        }
        if !events.update_columns.is_empty() {
            return Err(trigger_error(
                "42P17",
                "transition tables cannot be specified for triggers with column lists",
            ));
        }
        if stmt.events.len() != 1 {
            return Err(trigger_error(
                "42P17",
                "transition tables cannot be specified for triggers with more than one event",
            ));
        }
        if level == TriggerLevel::Row
            && table.is_some()
            && crate::partition::parent_of(kv, &table_name)?.is_some()
        {
            return Err(trigger_error(
                "42P17",
                "ROW triggers with transition tables cannot be defined on partitions",
            ));
        }
    }
    let mut old_transition = None;
    let mut new_transition = None;
    for transition in &stmt.transitions {
        if transition.old && !(events.update || events.delete) {
            return Err(trigger_error(
                "42P17",
                "OLD TABLE can only be specified for UPDATE or DELETE triggers",
            ));
        }
        if !(transition.old || events.update || events.insert) {
            return Err(trigger_error(
                "42P17",
                "NEW TABLE can only be specified for UPDATE or INSERT triggers",
            ));
        }
        let slot = if transition.old {
            &mut old_transition
        } else {
            &mut new_transition
        };
        if slot.replace(transition.name.clone()).is_some() {
            return Err(trigger_error(
                "42601",
                "OLD TABLE and NEW TABLE can each be specified only once",
            ));
        }
    }
    if old_transition == new_transition && old_transition.is_some() {
        return Err(trigger_error(
            "42601",
            "OLD TABLE and NEW TABLE aliases must be different",
        ));
    }
    let referenced_table_id = referenced_name
        .as_ref()
        .map(|name| relation_target(kv, name).map(|(id, _)| id))
        .transpose()?;
    let (function_oid, function) = trigger_routine(kv, &stmt.function, false)?;
    let existing = get_trigger(kv, table_id, &stmt.name)?;
    if existing.is_some() && !stmt.or_replace {
        return Err(ExecError::DuplicateObject(format!(
            "trigger \"{}\" for relation \"{}\" already exists",
            stmt.name, table_name
        )));
    }
    let mut trigger = Trigger {
        oid: existing.as_ref().map_or(0, |trigger| trigger.oid),
        name: stmt.name.clone(),
        table_id,
        table: table_name.clone(),
        parent_oid: existing.as_ref().map_or(0, |trigger| trigger.parent_oid),
        function_oid,
        function,
        timing,
        events,
        level,
        enabled: existing
            .as_ref()
            .map_or(TriggerEnabled::Origin, |t| t.enabled),
        is_internal: false,
        constraint: stmt.constraint,
        constraint_oid: existing
            .as_ref()
            .map_or(0, |trigger| trigger.constraint_oid),
        referenced_table_id,
        deferrable: stmt.deferrable,
        initially_deferred: stmt.initially_deferred,
        old_transition,
        new_transition,
        when: stmt.when_source.clone(),
        arguments: stmt.arguments.clone(),
    };
    let descendants = if table.is_some()
        && level == TriggerLevel::Row
        && crate::partition::is_partitioned(kv, &table_name)?
    {
        crate::partition::descendants(kv, &table_name)?
    } else {
        Vec::new()
    };
    let mut next_oid = crabka_pgcatalog::trigger::next_trigger_oid(kv)?;
    let mut allocated = false;
    if trigger.oid == 0 {
        trigger.oid = next_oid;
        next_oid += 1;
        allocated = true;
    }
    let mut ops = put_trigger_ops(kv, &trigger)?;
    for descendant in descendants {
        let child = crabka_pgcatalog::get_table(kv, &descendant)?;
        let child_existing = get_trigger(kv, child.id, &trigger.name)?;
        if child_existing
            .as_ref()
            .is_some_and(|child| child.parent_oid != trigger.oid)
            && !stmt.or_replace
        {
            return Err(ExecError::DuplicateObject(format!(
                "trigger \"{}\" for relation \"{}\" already exists",
                trigger.name, descendant
            )));
        }
        let mut clone = trigger.clone();
        clone.table_id = child.id;
        clone.table = child.name;
        clone.parent_oid = trigger.oid;
        clone.oid = child_existing.as_ref().map_or_else(
            || {
                allocated = true;
                let oid = next_oid;
                next_oid += 1;
                oid
            },
            |existing| existing.oid,
        );
        ops.extend(put_trigger_ops(kv, &clone)?);
    }
    if allocated {
        ops.insert(
            0,
            crabka_pgcatalog::trigger::set_next_trigger_oid_op(next_oid),
        );
    }
    Ok(command("CREATE TRIGGER", ops))
}

fn validate_when_references(
    stmt: &parsed::CreateTrigger,
    table: Option<&Table>,
    timing: TriggerTiming,
    level: TriggerLevel,
    events: &TriggerEvents,
) -> Result<(), ExecError> {
    let Some(condition) = &stmt.when else {
        return Ok(());
    };
    let old = when_references(condition, "old", None);
    let new = when_references(condition, "new", None);
    if level == TriggerLevel::Statement && (old || new) {
        return Err(trigger_error(
            "42P17",
            "statement trigger's WHEN condition cannot reference column values",
        ));
    }
    if events.insert && old {
        return Err(trigger_error(
            "42P17",
            "INSERT trigger's WHEN condition cannot reference OLD values",
        ));
    }
    if events.delete && new {
        return Err(trigger_error(
            "42P17",
            "DELETE trigger's WHEN condition cannot reference NEW values",
        ));
    }
    if timing == TriggerTiming::Before
        && ["tableoid", "ctid", "xmin", "xmax", "cmin", "cmax"]
            .iter()
            .any(|column| when_references(condition, "new", Some(column)))
    {
        return Err(trigger_error(
            "42P17",
            "BEFORE trigger's WHEN condition cannot reference NEW system columns",
        ));
    }
    if timing == TriggerTiming::Before
        && let Some(detail) =
            table.and_then(|table| when_references_new_generated(condition, table))
    {
        return Err(trigger_error_detail(
            "42P17",
            "BEFORE trigger's WHEN condition cannot reference NEW generated columns",
            detail,
        ));
    }
    Ok(())
}

fn when_references(expr: &parsed::Expr, image: &str, column: Option<&str>) -> bool {
    matches!(expr, parsed::Expr::Column { table: Some(table), name }
        if table.eq_ignore_ascii_case(image)
            && column.is_none_or(|column| name.eq_ignore_ascii_case(column)))
        || crate::exec::expr_children(expr)
            .into_iter()
            .any(|child| when_references(child, image, column))
}

fn when_references_new_generated(expr: &parsed::Expr, table: &Table) -> Option<String> {
    if let parsed::Expr::Column {
        table: Some(image),
        name,
    } = expr
        && image.eq_ignore_ascii_case("new")
    {
        if name == "*"
            && table
                .columns
                .iter()
                .any(|column| column.generated.is_some())
        {
            return Some(
                "A whole-row reference is used and the table contains generated columns.".into(),
            );
        }
        if table
            .columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(name) && column.generated.is_some())
        {
            return Some(format!("Column \"{name}\" is a generated column."));
        }
    }
    crate::exec::expr_children(expr)
        .into_iter()
        .find_map(|child| when_references_new_generated(child, table))
}

pub(crate) fn alter(
    kv: &dyn Kv,
    name: &str,
    table_name: RelationName,
    action: &parsed::AlterTriggerAction,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    let (table_id, _) = relation_target(kv, &table_name)?;
    let mut trigger = get_trigger(kv, table_id, name)?.ok_or_else(|| {
        ExecError::UndefinedObject(format!(
            "trigger \"{name}\" for table \"{table_name}\" does not exist"
        ))
    })?;
    let mut ops = Vec::new();
    match action {
        parsed::AlterTriggerAction::RenameTo(new_name) => {
            if get_trigger(kv, table_id, new_name)?.is_some() {
                return Err(ExecError::DuplicateObject(format!(
                    "trigger \"{new_name}\" for relation \"{table_name}\" already exists"
                )));
            }
            ops.extend(drop_trigger_ops(table_id, name));
            trigger.name = new_name.clone();
            ops.extend(put_trigger_ops(kv, &trigger)?);
            let roots = std::collections::HashSet::from([trigger.oid]);
            for mut clone in trigger_descendants(kv, &roots)? {
                if get_trigger(kv, clone.table_id, new_name)?.is_some() {
                    return Err(ExecError::DuplicateObject(format!(
                        "trigger \"{new_name}\" for relation \"{}\" already exists",
                        clone.table
                    )));
                }
                ops.extend(drop_trigger_ops(clone.table_id, &clone.name));
                clone.name = new_name.clone();
                ops.extend(put_trigger_ops(kv, &clone)?);
            }
        }
        parsed::AlterTriggerAction::DependsOnExtension { .. } => {}
    }
    Ok(command("ALTER TRIGGER", ops))
}

pub(crate) fn drop(
    kv: &dyn Kv,
    name: &str,
    table_name: RelationName,
    if_exists: bool,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    let (table_id, _) = relation_target(kv, &table_name)?;
    let Some(trigger) = get_trigger(kv, table_id, name)? else {
        if if_exists {
            return Ok(command("DROP TRIGGER", Vec::new()));
        }
        return Err(ExecError::UndefinedObject(format!(
            "trigger \"{name}\" for table \"{table_name}\" does not exist"
        )));
    };
    let mut ops = drop_trigger_ops(table_id, name);
    let roots = std::collections::HashSet::from([trigger.oid]);
    for clone in trigger_descendants(kv, &roots)? {
        ops.extend(drop_trigger_ops(clone.table_id, &clone.name));
    }
    Ok(command("DROP TRIGGER", ops))
}

fn trigger_descendants(
    kv: &dyn Kv,
    roots: &std::collections::HashSet<u32>,
) -> Result<Vec<Trigger>, ExecError> {
    let mut parent_oids = roots.clone();
    let mut remaining = crabka_pgcatalog::trigger::list_triggers(kv)?;
    let mut descendants = Vec::new();
    loop {
        let mut found = false;
        remaining.retain(|trigger| {
            if parent_oids.contains(&trigger.parent_oid) {
                parent_oids.insert(trigger.oid);
                descendants.push(trigger.clone());
                found = true;
                false
            } else {
                true
            }
        });
        if !found {
            return Ok(descendants);
        }
    }
}

fn map_event(value: parsed::EventTriggerEvent) -> EventTriggerEvent {
    match value {
        parsed::EventTriggerEvent::Login => EventTriggerEvent::Login,
        parsed::EventTriggerEvent::DdlCommandStart => EventTriggerEvent::DdlCommandStart,
        parsed::EventTriggerEvent::DdlCommandEnd => EventTriggerEvent::DdlCommandEnd,
        parsed::EventTriggerEvent::SqlDrop => EventTriggerEvent::SqlDrop,
        parsed::EventTriggerEvent::TableRewrite => EventTriggerEvent::TableRewrite,
    }
}

/// The role a privilege check should be made against, for a session that may
/// have authenticated as nobody.
///
/// Such a session carries `PUBLIC` and acts as the bootstrap superuser, the
/// same rule `ForeignCtx::effective_role` applies. It is spelled out here
/// because `create_event` is handed the role name rather than the context that
/// method hangs off.
fn acting_role(role: &str) -> &str {
    if role == crabka_pgcatalog::PUBLIC_ROLE {
        crabka_pgcatalog::BOOTSTRAP_ROLE
    } else {
        role
    }
}

/// One row of `PostgreSQL`'s command-tag table.
///
/// The rows come from `cmdtaglist.h` unedited, so the flags are the ones the
/// server itself consults rather than a reading of them: `event_trigger_ok` is
/// `command_tag_event_trigger_ok`, and `table_rewrite_ok` is
/// `command_tag_table_rewrite_ok`.
struct CommandTag {
    /// The tag as `PostgreSQL` spells it, which is upper case throughout.
    name: &'static str,
    /// Whether a `ddl_command_start`/`ddl_command_end`/`sql_drop` trigger may
    /// name the tag, and whether such a trigger fires for the command at all.
    event_trigger_ok: bool,
    /// Whether a `table_rewrite` trigger may name the tag.
    table_rewrite_ok: bool,
}

include!("event_command_tags.rs");

/// `PostgreSQL`'s `GetCommandTagEnum`: the table row for a tag a user wrote, or
/// `None` for `CMDTAG_UNKNOWN`.
///
/// The comparison ignores case because `GetCommandTagEnum` compares with
/// `pg_strcasecmp`, which is why `when tag in ('create table')` is a valid
/// filter. The scan is linear rather than the server's binary search: it runs
/// once per tag named in a `CREATE EVENT TRIGGER`, and a linear scan needs no
/// argument about whether case-insensitive comparison preserves the header's
/// ordering.
fn lookup_command_tag(name: &str) -> Option<&'static CommandTag> {
    COMMAND_TAGS
        .iter()
        .find(|tag| tag.name.eq_ignore_ascii_case(name))
}

/// The 0A000 `PostgreSQL` raises for a tag that exists but is closed to event
/// triggers.
///
/// The message quotes the user's spelling, not the canonical tag, which is what
/// `validate_ddl_tags` does with its `%s`.
fn unsupported_event_trigger_tag(written: &str) -> ExecError {
    trigger_error(
        "0A000",
        format!("event triggers are not supported for {written}"),
    )
}

/// Check a `CREATE EVENT TRIGGER`'s `WHEN` clause, in the order
/// `CreateEventTrigger` checks it.
///
/// The order is load-bearing: every filter variable is named and counted before
/// any tag value is looked at, so `when food in (…) and tag in ('bogus')`
/// complains about `food` and `when tag in (…) and tag in (…)` complains about
/// the repeat rather than about a tag.
///
/// # Errors
///
/// 42601 for a filter variable that is not `tag`, for the same variable twice,
/// and for a value that is no command tag at all; 0A000 for tag filtering on a
/// `login` trigger and for a real tag this event may not filter on.
fn validate_event_trigger_filters(
    event: parsed::EventTriggerEvent,
    filters: &[parsed::EventTriggerFilter],
) -> Result<(), ExecError> {
    let mut seen_tag = false;
    for filter in filters {
        if !filter.variable.eq_ignore_ascii_case("tag") {
            return Err(trigger_error(
                "42601",
                format!("unrecognized filter variable \"{}\"", filter.variable),
            ));
        }
        if seen_tag {
            return Err(trigger_error(
                "42601",
                format!(
                    "filter variable \"{}\" specified more than once",
                    filter.variable
                ),
            ));
        }
        seen_tag = true;
    }
    if !seen_tag {
        return Ok(());
    }
    if event == parsed::EventTriggerEvent::Login {
        return Err(trigger_error(
            "0A000",
            "tag filtering is not supported for login event triggers",
        ));
    }
    for value in filters.iter().flat_map(|filter| &filter.values) {
        let tag = lookup_command_tag(value);
        // A `table_rewrite` trigger takes the other flag, and takes it without
        // the "not recognized" arm: `validate_table_rewrite_tags` asks
        // `command_tag_table_rewrite_ok(CMDTAG_UNKNOWN)`, which is false, so an
        // invented tag comes back as "not supported" there rather than as the
        // 42601 a DDL trigger would raise.
        if event == parsed::EventTriggerEvent::TableRewrite {
            if !tag.is_some_and(|tag| tag.table_rewrite_ok) {
                return Err(unsupported_event_trigger_tag(value));
            }
            continue;
        }
        let Some(tag) = tag else {
            return Err(trigger_error(
                "42601",
                format!("filter value \"{value}\" not recognized for filter variable \"tag\""),
            ));
        };
        if !tag.event_trigger_ok {
            return Err(unsupported_event_trigger_tag(value));
        }
    }
    Ok(())
}

pub(crate) fn create_event(
    kv: &dyn Kv,
    stmt: &parsed::CreateEventTrigger,
    owner: &str,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    if !crate::rls::role_is_superuser(kv, acting_role(owner))? {
        return Err(ExecError::EventTriggerPrivilege {
            message: format!(
                "permission denied to create event trigger \"{}\"",
                stmt.name
            ),
            hint: "Must be superuser to create an event trigger.",
        });
    }
    validate_event_trigger_filters(stmt.event, &stmt.filters)?;
    if get_event_trigger(kv, &stmt.name)?.is_some() {
        return Err(ExecError::DuplicateObject(format!(
            "event trigger \"{}\" already exists",
            stmt.name
        )));
    }
    let (function_oid, function) = trigger_routine(kv, &stmt.function, true)?;
    let trigger = EventTrigger {
        oid: 0,
        name: stmt.name.clone(),
        event: map_event(stmt.event),
        owner: owner.to_string(),
        function_oid,
        function,
        enabled: TriggerEnabled::Origin,
        filters: stmt
            .filters
            .iter()
            .map(|filter| EventTriggerFilter {
                variable: filter.variable.clone(),
                values: filter.values.clone(),
            })
            .collect(),
    };
    Ok(command(
        "CREATE EVENT TRIGGER",
        put_event_trigger_ops(kv, &trigger)?,
    ))
}

pub(crate) fn alter_event(
    kv: &dyn Kv,
    name: &str,
    action: &parsed::AlterEventTriggerAction,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    let mut trigger = get_event_trigger(kv, name)?.ok_or_else(|| {
        ExecError::UndefinedObject(format!("event trigger \"{name}\" does not exist"))
    })?;
    let mut ops = Vec::new();
    match action {
        parsed::AlterEventTriggerAction::Enable(mode) => trigger.enabled = map_enabled(*mode),
        parsed::AlterEventTriggerAction::OwnerTo(owner) => {
            // The same rule `CREATE EVENT TRIGGER` enforces, applied to the
            // incoming owner: an event trigger runs its function for every DDL
            // command in the database, so handing one to a non-superuser is the
            // privilege escalation the create-time check exists to prevent.
            if !crate::rls::role_is_superuser(kv, owner)? {
                return Err(ExecError::EventTriggerPrivilege {
                    message: format!(
                        "permission denied to change owner of event trigger \"{name}\""
                    ),
                    hint: "The owner of an event trigger must be a superuser.",
                });
            }
            trigger.owner = owner.clone();
        }
        parsed::AlterEventTriggerAction::RenameTo(new_name) => {
            if get_event_trigger(kv, new_name)?.is_some() {
                return Err(ExecError::DuplicateObject(format!(
                    "event trigger \"{new_name}\" already exists"
                )));
            }
            ops.extend(drop_event_trigger_ops(name));
            trigger.name = new_name.clone();
        }
    }
    ops.extend(put_event_trigger_ops(kv, &trigger)?);
    Ok(command("ALTER EVENT TRIGGER", ops))
}

pub(crate) fn drop_event(
    kv: &dyn Kv,
    name: &str,
    if_exists: bool,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    if get_event_trigger(kv, name)?.is_none() {
        if if_exists {
            return Ok(command("DROP EVENT TRIGGER", Vec::new()));
        }
        return Err(ExecError::UndefinedObject(format!(
            "event trigger \"{name}\" does not exist"
        )));
    }
    Ok(command("DROP EVENT TRIGGER", drop_event_trigger_ops(name)))
}

/// Whether event triggers stay out of a statement's way.
///
/// The answer is the tag table's, not a list of its own: a command fires event
/// triggers exactly when `command_tag_event_trigger_ok` says its tag may be
/// filtered on. That covers the global objects `PostgreSQL` keeps event
/// triggers away from — roles, databases, tablespaces, event triggers
/// themselves — without naming them twice, and it makes an unmapped statement
/// silent rather than firing under the tag `UNKNOWN`, which is a tag no
/// `PostgreSQL` client would ever see.
pub(crate) fn event_trigger_ddl_is_excluded(stmt: &parsed::Statement) -> bool {
    !lookup_command_tag(event_command_tag(stmt)).is_some_and(|tag| tag.event_trigger_ok)
}

/// The command tag a statement reports to an event trigger.
///
/// `UNKNOWN` is the fallthrough for a statement with no tag of its own. It is
/// never a tag a trigger sees, because [`event_trigger_ddl_is_excluded`] finds
/// no table row for it and keeps the triggers from firing at all.
pub(crate) fn event_command_tag(stmt: &parsed::Statement) -> &'static str {
    use parsed::Statement;
    match stmt {
        Statement::CreateTable { .. } | Statement::CreateTableAs { .. } => "CREATE TABLE",
        Statement::AlterTable { .. } => "ALTER TABLE",
        Statement::DropTable { names, .. }
            if names
                .first()
                .is_some_and(|name| name.name.starts_with("__crabka_sequence__:")) =>
        {
            "DROP SEQUENCE"
        }
        Statement::DropTable { .. } => "DROP TABLE",
        Statement::CreateIndex { table, .. } if table.name == "__crabka_sequence__" => {
            "CREATE SEQUENCE"
        }
        Statement::CreateIndex { .. } => "CREATE INDEX",
        Statement::AlterIndex { .. } => "ALTER INDEX",
        Statement::DropIndex { .. } => "DROP INDEX",
        Statement::CreateView { .. } => "CREATE VIEW",
        Statement::AlterView { .. } => "ALTER VIEW",
        Statement::DropView { .. } => "DROP VIEW",
        Statement::CreateMaterializedView { .. } => "CREATE MATERIALIZED VIEW",
        Statement::RefreshMaterializedView { .. } => "REFRESH MATERIALIZED VIEW",
        Statement::DropMaterializedView { .. } => "DROP MATERIALIZED VIEW",
        Statement::CreatePolicy(_) => "CREATE POLICY",
        Statement::AlterPolicy { .. } => "ALTER POLICY",
        Statement::DropPolicy { .. } => "DROP POLICY",
        Statement::CreateAggregate(_) => "CREATE AGGREGATE",
        Statement::AlterAggregate { .. } => "ALTER AGGREGATE",
        Statement::DropAggregate { .. } => "DROP AGGREGATE",
        // The tags below are all `event_trigger_ok = false`, so naming them
        // here is what keeps the triggers quiet for those commands rather than
        // what makes them fire. They are global objects, or the event-trigger
        // DDL an event trigger must not be able to interfere with.
        Statement::CreateRole { .. } => "CREATE ROLE",
        Statement::AlterRole { .. } => "ALTER ROLE",
        Statement::DropRole { .. } => "DROP ROLE",
        Statement::GrantRoles { .. } => "GRANT ROLE",
        Statement::RevokeRoles { .. } => "REVOKE ROLE",
        Statement::CreateEventTrigger(_) => "CREATE EVENT TRIGGER",
        Statement::AlterEventTrigger { .. } => "ALTER EVENT TRIGGER",
        Statement::DropEventTrigger { .. } => "DROP EVENT TRIGGER",
        Statement::CreateSchema { .. } => "CREATE SCHEMA",
        Statement::AlterSchema { .. } => "ALTER SCHEMA",
        Statement::DropSchema { .. } => "DROP SCHEMA",
        Statement::CreateTrigger(_) => "CREATE TRIGGER",
        Statement::AlterTrigger { .. } => "ALTER TRIGGER",
        Statement::DropTrigger { .. } => "DROP TRIGGER",
        Statement::CreateRoutine(routine) => match routine.object {
            parsed::RoutineObject::Function => "CREATE FUNCTION",
            parsed::RoutineObject::Procedure => "CREATE PROCEDURE",
            parsed::RoutineObject::Routine => "CREATE ROUTINE",
        },
        Statement::AlterRoutine { object, .. } => match object {
            parsed::RoutineObject::Function => "ALTER FUNCTION",
            parsed::RoutineObject::Procedure => "ALTER PROCEDURE",
            parsed::RoutineObject::Routine => "ALTER ROUTINE",
        },
        Statement::DropRoutine { object, .. } => match object {
            parsed::RoutineObject::Function => "DROP FUNCTION",
            parsed::RoutineObject::Procedure => "DROP PROCEDURE",
            parsed::RoutineObject::Routine => "DROP ROUTINE",
        },
        Statement::CreateType { .. } => "CREATE TYPE",
        Statement::AlterType { .. } => "ALTER TYPE",
        Statement::DropType { .. } => "DROP TYPE",
        Statement::CreateDomain { .. } => "CREATE DOMAIN",
        Statement::AlterDomain { .. } => "ALTER DOMAIN",
        Statement::DropDomain { .. } => "DROP DOMAIN",
        Statement::Comment { .. } => "COMMENT",
        Statement::CreateFdw { .. } => "CREATE FOREIGN DATA WRAPPER",
        Statement::DropFdw { .. } => "DROP FOREIGN DATA WRAPPER",
        Statement::CreateServer { .. } => "CREATE SERVER",
        Statement::AlterServer { .. } => "ALTER SERVER",
        Statement::DropServer { .. } => "DROP SERVER",
        Statement::CreateUserMapping { .. } => "CREATE USER MAPPING",
        Statement::AlterUserMapping { .. } => "ALTER USER MAPPING",
        Statement::DropUserMapping { .. } => "DROP USER MAPPING",
        Statement::CreateForeignTable { .. } => "CREATE FOREIGN TABLE",
        Statement::DropForeignTable { .. } => "DROP FOREIGN TABLE",
        Statement::GrantTablePrivileges { .. } => "GRANT",
        Statement::GrantSchemaPrivileges { .. } => "GRANT",
        Statement::RevokeTablePrivileges { .. } => "REVOKE",
        Statement::RevokeSchemaPrivileges { .. } => "REVOKE",
        Statement::AlterDefaultTablePrivileges { .. } => "ALTER DEFAULT PRIVILEGES",
        Statement::ImportForeignSchema { .. } => "IMPORT FOREIGN SCHEMA",
        Statement::Utility(parsed::UtilityStatement::CreateOperator(_)) => "CREATE OPERATOR",
        Statement::Utility(parsed::UtilityStatement::DropOperator { .. }) => "DROP OPERATOR",
        Statement::Utility(parsed::UtilityStatement::TextSearch(ddl)) => match ddl {
            parsed::TextSearchDdl::Create {
                kind: parsed::TextSearchObjectKind::Configuration,
                ..
            } => "CREATE TEXT SEARCH CONFIGURATION",
            parsed::TextSearchDdl::Create {
                kind: parsed::TextSearchObjectKind::Dictionary,
                ..
            } => "CREATE TEXT SEARCH DICTIONARY",
            parsed::TextSearchDdl::Alter {
                kind: parsed::TextSearchObjectKind::Configuration,
                ..
            } => "ALTER TEXT SEARCH CONFIGURATION",
            parsed::TextSearchDdl::Alter {
                kind: parsed::TextSearchObjectKind::Dictionary,
                ..
            } => "ALTER TEXT SEARCH DICTIONARY",
            parsed::TextSearchDdl::Drop {
                kind: parsed::TextSearchObjectKind::Configuration,
                ..
            } => "DROP TEXT SEARCH CONFIGURATION",
            parsed::TextSearchDdl::Drop {
                kind: parsed::TextSearchObjectKind::Dictionary,
                ..
            } => "DROP TEXT SEARCH DICTIONARY",
        },
        _ => "UNKNOWN",
    }
}

pub(crate) fn is_drop_ddl(stmt: &parsed::Statement) -> bool {
    matches!(
        stmt,
        parsed::Statement::DropTable { .. }
            | parsed::Statement::DropIndex { .. }
            | parsed::Statement::DropView { .. }
            | parsed::Statement::DropSchema { .. }
            | parsed::Statement::DropTrigger { .. }
            | parsed::Statement::DropRoutine { .. }
            | parsed::Statement::DropType { .. }
            | parsed::Statement::DropDomain { .. }
            | parsed::Statement::DropFdw { .. }
            | parsed::Statement::DropServer { .. }
            | parsed::Statement::DropUserMapping { .. }
            | parsed::Statement::DropForeignTable { .. }
            | parsed::Statement::DropRole { .. }
    )
}

pub(crate) fn is_table_rewrite_ddl(stmt: &parsed::Statement) -> bool {
    matches!(
        stmt,
        parsed::Statement::AlterTable {
            actions,
            ..
        } if actions
            .iter()
            .any(|action| matches!(action, parsed::AlterTableAction::SetType { .. }))
    )
}

pub(crate) fn event_trigger_context(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    stmt: &parsed::Statement,
    event: EventTriggerEvent,
    tag: &str,
) -> Result<std::sync::Arc<crate::clock::EventTriggerContext>, ExecError> {
    let references: Vec<(&parsed::RelationRef, &str)> = match stmt {
        parsed::Statement::CreateTable { name, .. }
        | parsed::Statement::AlterTable { table: name, .. } => vec![(name, "table")],
        parsed::Statement::DropTable { names, .. } => {
            names.iter().map(|name| (name, "table")).collect()
        }
        parsed::Statement::CreateView { name, .. } | parsed::Statement::DropView { name, .. } => {
            vec![(name, "view")]
        }
        parsed::Statement::CreateForeignTable { name, .. }
        | parsed::Statement::DropForeignTable { name, .. } => vec![(name, "foreign table")],
        _ => Vec::new(),
    };
    let mut objects = Vec::new();
    for (reference, object_type) in references {
        let sequence = reference.name.strip_prefix("__crabka_sequence__:");
        let lookup = sequence.map(|name| parsed::RelationRef {
            schema: reference.schema.clone(),
            name: name.to_string(),
        });
        let Ok(name) = crate::relname::resolve_relation(
            kv,
            resolution,
            lookup.as_ref().unwrap_or(reference),
            crate::relname::SchemaDisposition::Reference,
        ) else {
            continue;
        };
        let object_id = if sequence.is_some() {
            let Some(oid) = crate::catalog_rel::sequence_oids(kv)?.get(&name).copied() else {
                continue;
            };
            oid
        } else if object_type == "view" {
            crate::catalog_rel::view_oids(kv)?
                .get(&name)
                .copied()
                .unwrap_or_default()
        } else {
            let Ok(table) = crabka_pgcatalog::get_table(kv, &name) else {
                continue;
            };
            crate::catalog_rel::table_relation_oid(table.id).unwrap_or_default()
        };
        let (object_type, object_name) = if sequence.is_some() {
            ("sequence", name.name.as_str())
        } else {
            (object_type, name.name.as_str())
        };
        objects.push(crate::clock::EventTriggerObject {
            class_id: crate::catalog_fn::PG_CLASS_OID,
            object_id,
            object_sub_id: 0,
            object_type: object_type.to_string(),
            schema_name: Some(crabka_pgcatalog::displayed_schema(&name.schema).to_string()),
            object_name: Some(object_name.to_string()),
            identity: format!(
                "{}.{}",
                crate::catalog_fn::quote_identifier(crabka_pgcatalog::displayed_schema(
                    &name.schema
                )),
                crate::catalog_fn::quote_identifier(object_name)
            ),
            is_temporary: crabka_pgcatalog::is_temp_schema(&name.schema),
        });
    }
    if let parsed::Statement::DropTable { names, cascade, .. } = stmt {
        for reference in names {
            let Ok(name) = crate::relname::resolve_relation(
                kv,
                resolution,
                reference,
                crate::relname::SchemaDisposition::Reference,
            ) else {
                continue;
            };
            let Ok(table) = crabka_pgcatalog::get_table(kv, &name) else {
                continue;
            };
            append_table_drop_objects(kv, &table, &mut objects)?;
            for descendant in crate::partition::descendants(kv, &name)? {
                if let Ok(table) = crabka_pgcatalog::get_table(kv, &descendant) {
                    append_relation_object(&table.name, table.id, "table", &mut objects);
                    append_table_drop_objects(kv, &table, &mut objects)?;
                }
            }
            if *cascade {
                for view in crate::exec::dependent_view_names(kv, &name, None)? {
                    if let Some(oid) = crate::catalog_rel::view_oids(kv)?.get(&view).copied() {
                        append_relation_object(
                            &view,
                            u32::try_from(oid).unwrap_or(0),
                            "view",
                            &mut objects,
                        );
                        append_trigger_objects(kv, u32::try_from(oid).unwrap_or(0), &mut objects)?;
                    }
                }
                for foreign_key in crabka_pgcatalog::list_referencing_foreign_keys(kv, table.id)? {
                    append_foreign_key_object(&foreign_key, &mut objects)?;
                }
            }
        }
        objects.sort_by_key(|object| (object.class_id, object.object_id, object.object_sub_id));
        objects.dedup_by_key(|object| (object.class_id, object.object_id, object.object_sub_id));
    }
    let rewrite = if matches!(event, EventTriggerEvent::TableRewrite) {
        objects.first().map(|object| (object.object_id, 4))
    } else {
        None
    };
    let (commands, dropped) = if matches!(event, EventTriggerEvent::SqlDrop) {
        (Vec::new(), objects)
    } else {
        (objects, Vec::new())
    };
    Ok(std::sync::Arc::new(crate::clock::EventTriggerContext {
        event,
        tag: tag.to_string(),
        commands,
        dropped,
        rewrite,
    }))
}

fn append_relation_object(
    name: &RelationName,
    oid: u32,
    object_type: &str,
    objects: &mut Vec<crate::clock::EventTriggerObject>,
) {
    objects.push(crate::clock::EventTriggerObject {
        class_id: crate::catalog_fn::PG_CLASS_OID,
        object_id: i32::try_from(oid).unwrap_or(0),
        object_sub_id: 0,
        object_type: object_type.into(),
        schema_name: Some(crabka_pgcatalog::displayed_schema(&name.schema).to_string()),
        object_name: Some(name.name.clone()),
        identity: format!(
            "{}.{}",
            crate::catalog_fn::quote_identifier(crabka_pgcatalog::displayed_schema(&name.schema)),
            crate::catalog_fn::quote_identifier(&name.name)
        ),
        is_temporary: crabka_pgcatalog::is_temp_schema(&name.schema),
    });
}

fn append_trigger_objects(
    kv: &dyn Kv,
    table_id: u32,
    objects: &mut Vec<crate::clock::EventTriggerObject>,
) -> Result<(), ExecError> {
    for trigger in crabka_pgcatalog::trigger::triggers_for_table(kv, table_id)? {
        objects.push(crate::clock::EventTriggerObject {
            class_id: crate::catalog_fn::PG_TRIGGER_OID,
            object_id: i32::try_from(trigger.oid).unwrap_or(0),
            object_sub_id: 0,
            object_type: "trigger".into(),
            schema_name: Some(
                crabka_pgcatalog::displayed_schema(&trigger.table.schema).to_string(),
            ),
            object_name: Some(trigger.name.clone()),
            identity: format!(
                "{} on {}.{}",
                crate::catalog_fn::quote_identifier(&trigger.name),
                crate::catalog_fn::quote_identifier(crabka_pgcatalog::displayed_schema(
                    &trigger.table.schema
                )),
                crate::catalog_fn::quote_identifier(&trigger.table.name)
            ),
            is_temporary: crabka_pgcatalog::is_temp_schema(&trigger.table.schema),
        });
    }
    Ok(())
}

fn append_foreign_key_object(
    foreign_key: &crabka_pgcatalog::ForeignKey,
    objects: &mut Vec<crate::clock::EventTriggerObject>,
) -> Result<(), ExecError> {
    objects.push(crate::clock::EventTriggerObject {
        class_id: crate::catalog_fn::PG_CONSTRAINT_OID,
        object_id: crate::catalog_rel::foreign_key_oid(foreign_key.id)?,
        object_sub_id: 0,
        object_type: "table constraint".into(),
        schema_name: Some(
            crabka_pgcatalog::displayed_schema(&foreign_key.table.schema).to_string(),
        ),
        object_name: Some(foreign_key.name.clone()),
        identity: format!(
            "{} on {}.{}",
            crate::catalog_fn::quote_identifier(&foreign_key.name),
            crate::catalog_fn::quote_identifier(crabka_pgcatalog::displayed_schema(
                &foreign_key.table.schema
            )),
            crate::catalog_fn::quote_identifier(&foreign_key.table.name)
        ),
        is_temporary: crabka_pgcatalog::is_temp_schema(&foreign_key.table.schema),
    });
    Ok(())
}

fn append_table_drop_objects(
    kv: &dyn Kv,
    table: &Table,
    objects: &mut Vec<crate::clock::EventTriggerObject>,
) -> Result<(), ExecError> {
    append_trigger_objects(kv, table.id, objects)?;
    for foreign_key in crabka_pgcatalog::list_table_foreign_keys(kv, table.id)? {
        append_foreign_key_object(&foreign_key, objects)?;
    }
    Ok(())
}

pub(crate) fn matching_event_triggers(
    kv: &dyn Kv,
    event: EventTriggerEvent,
    tag: &str,
    replication_role: &str,
) -> Result<Vec<EventTrigger>, ExecError> {
    let mut triggers = crabka_pgcatalog::trigger::list_event_triggers(kv)?;
    triggers.retain(|trigger| {
        if trigger.event != event {
            return false;
        }
        let enabled = match trigger.enabled {
            TriggerEnabled::Disabled => false,
            TriggerEnabled::Always => true,
            TriggerEnabled::Origin => replication_role != "replica",
            TriggerEnabled::Replica => replication_role == "replica",
        };
        enabled
            && trigger.filters.iter().all(|filter| {
                filter.variable.eq_ignore_ascii_case("tag")
                    && filter
                        .values
                        .iter()
                        .any(|value| value.eq_ignore_ascii_case(tag))
            })
    });
    triggers.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(triggers)
}

pub(crate) fn event_invocation(trigger: &EventTrigger, tag: &str) -> TriggerInvocation {
    TriggerInvocation {
        name: trigger.name.clone(),
        when: String::new(),
        level: String::new(),
        operation: String::new(),
        event: Some(
            match trigger.event {
                EventTriggerEvent::Login => "login",
                EventTriggerEvent::DdlCommandStart => "ddl_command_start",
                EventTriggerEvent::DdlCommandEnd => "ddl_command_end",
                EventTriggerEvent::SqlDrop => "sql_drop",
                EventTriggerEvent::TableRewrite => "table_rewrite",
            }
            .into(),
        ),
        tag: Some(tag.into()),
        relation_oid: 0,
        table_schema: String::new(),
        table_name: String::new(),
        arguments: Vec::new(),
        column_names: Vec::new(),
        column_types: Vec::new(),
        transitions: Vec::new(),
        old: crabka_pgtypes::Datum::Null,
        new: crabka_pgtypes::Datum::Null,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DmlEvent {
    Insert,
    Update,
    Delete,
    Truncate,
}

fn trigger_matches_event(
    trigger: &Trigger,
    table: Option<&Table>,
    event: DmlEvent,
    updated: &[String],
) -> bool {
    match event {
        DmlEvent::Insert => trigger.events.insert,
        DmlEvent::Delete => trigger.events.delete,
        DmlEvent::Truncate => trigger.events.truncate,
        DmlEvent::Update => {
            trigger.events.update
                && (trigger.events.update_columns.is_empty()
                    || trigger.events.update_columns.iter().any(|column| {
                        updated.contains(column)
                            || table.is_some_and(|table| {
                                generated_column_depends_on(table, column, updated)
                            })
                    }))
        }
    }
}

fn generated_column_depends_on(table: &Table, column: &str, updated: &[String]) -> bool {
    let Some(expression) = table
        .columns
        .iter()
        .find(|candidate| candidate.name == column)
        .and_then(|column| column.generated.as_ref())
        .and_then(|generated| crabka_pgparser::parser::parse_expression(&generated.expr).ok())
    else {
        return false;
    };
    updated
        .iter()
        .any(|column| expression_references_column(&expression, column))
}

fn expression_references_column(expr: &parsed::Expr, column: &str) -> bool {
    matches!(expr, parsed::Expr::Column { table: None, name } if name == column)
        || crate::exec::expr_children(expr)
            .into_iter()
            .any(|child| expression_references_column(child, column))
}

pub(crate) fn has_instead_row_trigger(
    kv: &dyn Kv,
    relation_id: u32,
    event: DmlEvent,
    updated: &[String],
) -> Result<bool, ExecError> {
    Ok(
        crabka_pgcatalog::trigger::triggers_for_table(kv, relation_id)?
            .into_iter()
            .any(|trigger| {
                trigger.timing == TriggerTiming::InsteadOf
                    && trigger.level == TriggerLevel::Row
                    && trigger_matches_event(&trigger, None, event, updated)
                    && trigger_is_enabled(&trigger)
            }),
    )
}

fn trigger_is_enabled(trigger: &Trigger) -> bool {
    let role = crate::session::current_setting_runtime("session_replication_role", false)
        .ok()
        .flatten()
        .unwrap_or_else(|| "origin".into());
    match trigger.enabled {
        TriggerEnabled::Disabled => false,
        TriggerEnabled::Always => true,
        TriggerEnabled::Origin => role != "replica",
        TriggerEnabled::Replica => role == "replica",
    }
}

fn record(table: &Table, row: Option<&[crabka_pgtypes::Datum]>) -> crabka_pgtypes::Datum {
    let Some(row) = row else {
        return crabka_pgtypes::Datum::Null;
    };
    crabka_pgtypes::Datum::Record(crabka_pgtypes::RecordValue::named(
        None,
        std::sync::Arc::from(
            table
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>(),
        ),
        row.to_vec(),
    ))
}

fn when_matches(
    trigger: &Trigger,
    table: &Table,
    old: Option<&[crabka_pgtypes::Datum]>,
    new: Option<&[crabka_pgtypes::Datum]>,
    ctx: &crate::clock::EvalCtx,
) -> Result<bool, ExecError> {
    let Some(source) = &trigger.when else {
        return Ok(true);
    };
    let expr = crabka_pgparser::parser::parse_expression(source)?;
    let mut scope = crate::scope::Scope::single(table, "old");
    scope.push_tableoid("old");
    let mut new_scope = crate::scope::Scope::single(table, "new");
    new_scope.push_tableoid("new");
    scope.columns.extend(new_scope.columns);
    let tableoid = crabka_pgtypes::Datum::Int4(crate::catalog_rel::table_relation_oid(table.id)?);
    let nulls = vec![crabka_pgtypes::Datum::Null; table.columns.len()];
    let mut values = old.unwrap_or(&nulls).to_vec();
    values.push(tableoid.clone());
    values.extend_from_slice(new.unwrap_or(&nulls));
    values.push(tableoid);
    Ok(matches!(
        crate::eval::eval(&expr, &scope, &values, ctx)?,
        crabka_pgtypes::Datum::Bool(true)
    ))
}

fn operation_name(event: DmlEvent) -> &'static str {
    match event {
        DmlEvent::Insert => "INSERT",
        DmlEvent::Update => "UPDATE",
        DmlEvent::Delete => "DELETE",
        DmlEvent::Truncate => "TRUNCATE",
    }
}

fn invoke_catalog_trigger(
    kv: &dyn Kv,
    trigger: &Trigger,
    table: &Table,
    event: DmlEvent,
    old: Option<&[crabka_pgtypes::Datum]>,
    new: Option<&[crabka_pgtypes::Datum]>,
) -> Result<crabka_pgtypes::Datum, ExecError> {
    note_fired();
    if trigger
        .function
        .ends_with("suppress_redundant_updates_trigger")
    {
        return Ok(if old == new {
            crabka_pgtypes::Datum::Null
        } else {
            record(table, new)
        });
    }
    if trigger.function.ends_with("tsvector_update_trigger")
        || trigger.function.ends_with("tsvector_update_trigger_column")
    {
        return invoke_tsvector_update_trigger(kv, trigger, table, event, new);
    }
    let routine = crate::routine::routine_by_oid(kv, trigger.function_oid as i32)?
        .or_else(|| {
            routines_named(kv, &trigger.function)
                .ok()?
                .into_iter()
                .next()
        })
        .ok_or_else(|| {
            ExecError::UndefinedFunction(format!("function {}() does not exist", trigger.function))
        })?;
    if routine.language == "c" && routine.body == "trigger_return_old" {
        // The regression module's trigger returns PostgreSQL's `tg_trigtuple`:
        // OLD for UPDATE/DELETE and NEW for INSERT.
        return Ok(record(table, old.or(new)));
    }
    invoke(
        routine,
        TriggerInvocation {
            name: trigger.name.clone(),
            when: match trigger.timing {
                TriggerTiming::Before => "BEFORE",
                TriggerTiming::After => "AFTER",
                TriggerTiming::InsteadOf => "INSTEAD OF",
            }
            .into(),
            level: match trigger.level {
                TriggerLevel::Row => "ROW",
                TriggerLevel::Statement => "STATEMENT",
            }
            .into(),
            operation: operation_name(event).into(),
            event: None,
            tag: None,
            relation_oid: u32::try_from(crate::catalog_rel::trigger_relation_oid(
                trigger.table_id,
            )?)
            .unwrap_or_default(),
            table_schema: table.name.schema.clone(),
            table_name: table.name.name.clone(),
            arguments: trigger.arguments.clone(),
            column_names: table
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect(),
            column_types: table.columns.iter().map(|column| column.ty).collect(),
            transitions: Vec::new(),
            old: record(table, old),
            new: record(table, new),
        },
    )
}

fn invoke_tsvector_update_trigger(
    kv: &dyn Kv,
    trigger: &Trigger,
    table: &Table,
    event: DmlEvent,
    new: Option<&[crabka_pgtypes::Datum]>,
) -> Result<crabka_pgtypes::Datum, ExecError> {
    use crabka_pgtypes::Datum;

    if trigger.timing != TriggerTiming::Before
        || trigger.level != TriggerLevel::Row
        || !matches!(event, DmlEvent::Insert | DmlEvent::Update)
    {
        return Err(trigger_error(
            "0A000",
            "tsvector_update_trigger must be fired BEFORE INSERT or UPDATE for each row",
        ));
    }
    if trigger.arguments.len() < 3 {
        return Err(trigger_error(
            "22023",
            "tsvector_update_trigger requires at least three arguments",
        ));
    }
    let mut row = new
        .ok_or_else(|| trigger_error("55000", "NEW row is not available"))?
        .to_vec();
    let column_index = |name: &str| {
        table
            .columns
            .iter()
            .position(|column| column.name == name)
            .ok_or_else(|| trigger_error("42703", format!("column \"{name}\" does not exist")))
    };
    let target = column_index(&trigger.arguments[0])?;
    if table.columns[target].ty != crabka_pgtypes::ColumnType::TsVector {
        return Err(trigger_error(
            "42804",
            format!(
                "column \"{}\" is not of tsvector type",
                trigger.arguments[0]
            ),
        ));
    }
    let config = if trigger.function.ends_with("tsvector_update_trigger_column") {
        let index = column_index(&trigger.arguments[1])?;
        match &row[index] {
            Datum::Text(value) => value.clone(),
            Datum::Null => {
                return Err(trigger_error(
                    "22023",
                    "text search configuration column must not be null",
                ));
            }
            _ => {
                return Err(trigger_error(
                    "42804",
                    "text search configuration column must be of regconfig type",
                ));
            }
        }
    } else {
        trigger.arguments[1].clone()
    };
    let mut document = String::new();
    for source in &trigger.arguments[2..] {
        let index = column_index(source)?;
        match &row[index] {
            Datum::Text(value) => {
                if !document.is_empty() {
                    document.push(' ');
                }
                document.push_str(value);
            }
            Datum::Null => {}
            _ => {
                return Err(trigger_error(
                    "42804",
                    format!("column \"{source}\" is not of a character type"),
                ));
            }
        }
    }
    row[target] = Datum::TsVector(crate::text_search_fn::to_tsvector(
        &config,
        &document,
        Some(kv),
    )?);
    Ok(record(table, Some(&row)))
}

fn queue_catalog_trigger(
    kv: &dyn Kv,
    trigger: &Trigger,
    table: &Table,
    event: DmlEvent,
    old: Option<&[crabka_pgtypes::Datum]>,
    new: Option<&[crabka_pgtypes::Datum]>,
) -> Result<(), ExecError> {
    note_fired();
    let invocation = TriggerInvocation {
        name: trigger.name.clone(),
        when: match trigger.timing {
            TriggerTiming::Before => "BEFORE",
            TriggerTiming::After => "AFTER",
            TriggerTiming::InsteadOf => "INSTEAD OF",
        }
        .into(),
        level: match trigger.level {
            TriggerLevel::Row => "ROW",
            TriggerLevel::Statement => "STATEMENT",
        }
        .into(),
        operation: operation_name(event).into(),
        event: None,
        tag: None,
        relation_oid: u32::try_from(crate::catalog_rel::trigger_relation_oid(trigger.table_id)?)
            .unwrap_or_default(),
        table_schema: table.name.schema.clone(),
        table_name: table.name.name.clone(),
        arguments: trigger.arguments.clone(),
        column_names: table
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect(),
        column_types: table.columns.iter().map(|column| column.ty).collect(),
        transitions: Vec::new(),
        old: record(table, old),
        new: record(table, new),
    };
    let pending = PendingTrigger {
        function_oid: trigger.function_oid,
        function: trigger.function.clone(),
        invocation,
        table_id: trigger.table_id,
        name: trigger.name.clone(),
        constraint: trigger.constraint,
        deferrable: trigger.deferrable,
        initially_deferred: trigger.initially_deferred,
        old_transition: trigger.old_transition.clone(),
        new_transition: trigger.new_transition.clone(),
    };
    let queued = AFTER_TRIGGER_QUEUE.with(|cell| {
        let mut queue = cell.borrow_mut();
        if let Some(queue) = queue.as_mut() {
            queue.push(pending.clone());
            true
        } else {
            false
        }
    });
    if queued {
        return Ok(());
    }
    let routine =
        crate::routine::routine_by_oid(kv, i32::try_from(pending.function_oid).unwrap_or(0))?
            .ok_or_else(|| {
                ExecError::UndefinedFunction(format!(
                    "function {}() does not exist",
                    pending.function
                ))
            })?;
    let _ = invoke(routine, pending.invocation)?;
    Ok(())
}

/// The relation a `BEFORE ROW` trigger fires for, together with the
/// row-security check the row it leaves behind must satisfy.
///
/// One value rather than two arguments because the pairing is a decision each
/// write path makes and can get wrong: a row routed into a partition leaf is
/// judged by the policies of the *parent* the statement named, not the leaf
/// whose triggers run. Naming the pair makes that choice visible at every call
/// site instead of hiding it between two adjacent arguments.
#[derive(Clone, Copy)]
pub(crate) struct WriteTarget<'a> {
    pub table: &'a Table,
    pub check: &'a crate::rls::WriteChecks,
}

/// Blank every `VIRTUAL` generated column of one trigger image, in place.
///
/// `PostgreSQL` does not let a trigger read a generated column — "it is not
/// allowed to access generated columns in `BEFORE` triggers", and its `AFTER`
/// images carry the same NULL — because the value is conceptually settled once
/// the triggers have run and before then there is nothing to report. Blanking
/// here rather than relying on the write path never to have computed the value
/// makes the rule hold for a statement that DOES name the column in its own
/// `WHERE`, which materializes it into the very row `OLD` is taken from.
fn blank_virtual_generated(table: &Table, image: &mut [crabka_pgtypes::Datum]) {
    for (index, _) in table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.is_virtual_generated())
    {
        if let Some(slot) = image.get_mut(index) {
            *slot = crabka_pgtypes::Datum::Null;
        }
    }
}

/// Blank every generated column of the `NEW` image, whichever kind it is.
///
/// The rule is the one `ddl.sgml` states: a generated column settles after the
/// `BEFORE` triggers, so before them there is no value and `NEW.b` is NULL. An
/// `UPDATE` is where the kinds part company from [`blank_virtual_generated`]:
/// its proposed row is built from the stored row, which carries the `STORED`
/// column's *old* value, and a trigger that read it would be reading a number
/// about to be replaced.
///
/// Only on the way in. A value a trigger then assigns to a `STORED` column
/// survives to the next trigger — `PostgreSQL` prints it — and is discarded by
/// the settle rather than between triggers. A `VIRTUAL` column is the one
/// upstream re-blanks after every trigger; see [`blank_virtual_generated`].
fn blank_generated(table: &Table, image: &mut [crabka_pgtypes::Datum]) {
    for (index, _) in table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.generated.is_some())
    {
        if let Some(slot) = image.get_mut(index) {
            *slot = crabka_pgtypes::Datum::Null;
        }
    }
}

/// [`blank_virtual_generated`] over a borrowed image, which is copied only when
/// the relation has a virtual generated column to blank.
fn trigger_image<'a>(
    table: &Table,
    image: Option<&'a [crabka_pgtypes::Datum]>,
) -> Option<std::borrow::Cow<'a, [crabka_pgtypes::Datum]>> {
    let image = image?;
    if !crate::exec::has_virtual_generated(table) {
        return Some(std::borrow::Cow::Borrowed(image));
    }
    let mut owned = image.to_vec();
    blank_virtual_generated(table, &mut owned);
    Some(std::borrow::Cow::Owned(owned))
}

/// Fire the relation's `BEFORE ROW` triggers, settle the row they leave behind,
/// and judge it against the target's check.
///
/// Both the settling ([`crate::exec::finish_written_row`]: the `STORED`
/// generated columns, then the domain, `NOT NULL` and `CHECK` constraints) and
/// the row-security check live *here* rather than in the row builders, because
/// a `BEFORE ROW` trigger returns a *replacement* row and the replacement is
/// what actually gets written. Anything judged before the trigger judges a row
/// nobody stores, and lets the trigger launder its replacement past both.
/// `PostgreSQL` orders it the same way, for the same reason: `ExecInsert` runs
/// `ExecBRInsertTriggers`, then `ExecComputeStoredGenerated`, then
/// `ExecWithCheckOptions` and `ExecConstraints`, and `ddl.sgml` states the rule
/// outright — "Generated columns are, conceptually, updated after BEFORE
/// triggers have run".
///
/// Every write path in the executor that fires row triggers passes through this
/// one function, so putting both here covers all of them at once and makes a
/// new write path that skips either fail to compile rather than fail to check.
/// The three that fire no row trigger at all — a view's `INSTEAD OF` insert and
/// update, and a sharded `COPY` — settle themselves, and are named in
/// [`crate::exec::finish_written_row`].
///
/// A `DELETE` writes no row: it settles nothing, and carries
/// [`crate::rls::CheckExemption::RemovesRows`] because its rows were already
/// filtered by the `USING` qual at `write_candidate_rows`.
pub(crate) fn fire_before_row(
    kv: &dyn Kv,
    target: WriteTarget<'_>,
    event: DmlEvent,
    updated: &[String],
    old: Option<&[crabka_pgtypes::Datum]>,
    mut new: Option<Vec<crabka_pgtypes::Datum>>,
    ctx: &crate::clock::EvalCtx,
) -> Result<Option<Vec<crabka_pgtypes::Datum>>, ExecError> {
    let WriteTarget { table, check } = target;
    let old_image = trigger_image(table, old);
    let old = old_image.as_deref();
    if let Some(image) = new.as_mut() {
        blank_generated(table, image);
    }
    for trigger in crabka_pgcatalog::trigger::triggers_for_table(kv, table.id)? {
        if trigger.timing != TriggerTiming::Before
            || trigger.level != TriggerLevel::Row
            || !trigger_matches_event(&trigger, Some(table), event, updated)
            || !trigger_is_enabled(&trigger)
            || !when_matches(&trigger, table, old, new.as_deref(), ctx)?
        {
            continue;
        }
        let result = invoke_catalog_trigger(kv, &trigger, table, event, old, new.as_deref())?;
        match result {
            crabka_pgtypes::Datum::Null => return Ok(None),
            crabka_pgtypes::Datum::Record(record) => {
                if record.values.len() != table.columns.len() {
                    return Err(trigger_error(
                        "42804",
                        "returned row structure does not match the structure of the triggering table",
                    ));
                }
                let values = record
                    .values
                    .into_iter()
                    .zip(&table.columns)
                    .map(|(value, column)| crate::exec::coerce(value, column.ty, ctx))
                    .collect::<Result<Vec<_>, _>>()?;
                if matches!(event, DmlEvent::Insert | DmlEvent::Update) {
                    // `check_modified_virtual_generated`: a value the trigger
                    // assigned to a virtual generated column is dropped before
                    // the next trigger — or the write — can see it.
                    let mut values = values;
                    blank_virtual_generated(table, &mut values);
                    new = Some(values);
                }
            }
            _ => {
                return Err(trigger_error(
                    "42804",
                    "trigger function returned a value that is not a row",
                ));
            }
        }
    }
    if event == DmlEvent::Delete {
        return Ok(old.map(|row| row.to_vec()));
    }
    // The row every trigger has had its say about is the one that settles, and
    // the settled row is the one the policy judges.
    if let Some(row) = &mut new {
        crate::exec::finish_written_row(table, row, ctx)?;
        check.permit_row(kv, table, row, ctx)?;
    }
    Ok(new)
}

pub(crate) fn fire_instead_row(
    kv: &dyn Kv,
    view: &Table,
    event: DmlEvent,
    updated: &[String],
    old: Option<&[crabka_pgtypes::Datum]>,
    new: Option<Vec<crabka_pgtypes::Datum>>,
    _ctx: &crate::clock::EvalCtx,
) -> Result<Option<Vec<crabka_pgtypes::Datum>>, ExecError> {
    if !has_instead_row_trigger(kv, view.id, event, updated)? {
        return Err(ExecError::ObjectNotInPrerequisiteState(format!(
            "cannot {} view \"{}\" because it has no INSTEAD OF trigger",
            operation_name(event).to_ascii_lowercase(),
            view.name
        )));
    }
    let mut result = if event == DmlEvent::Delete {
        old.map(|row| row.to_vec())
    } else {
        new
    };
    for trigger in crabka_pgcatalog::trigger::triggers_for_table(kv, view.id)? {
        if trigger.timing == TriggerTiming::InsteadOf
            && trigger.level == TriggerLevel::Row
            && trigger_matches_event(&trigger, None, event, updated)
            && trigger_is_enabled(&trigger)
        {
            let invocation_new = (event != DmlEvent::Delete)
                .then_some(result.as_deref())
                .flatten();
            let datum = invoke_catalog_trigger(kv, &trigger, view, event, old, invocation_new)?;
            result = match datum {
                crabka_pgtypes::Datum::Null => None,
                crabka_pgtypes::Datum::Record(record)
                    if record.values.len() == view.columns.len() =>
                {
                    Some(record.values)
                }
                _ => {
                    return Err(trigger_error(
                        "42804",
                        "returned row structure does not match the structure of the triggering view",
                    ));
                }
            };
            if result.is_none() {
                break;
            }
        }
    }
    Ok(result)
}

pub(crate) fn fire_after_row(
    kv: &dyn Kv,
    table: &Table,
    event: DmlEvent,
    updated: &[String],
    old: Option<&[crabka_pgtypes::Datum]>,
    new: Option<&[crabka_pgtypes::Datum]>,
    ctx: &crate::clock::EvalCtx,
) -> Result<(), ExecError> {
    let old_image = trigger_image(table, old);
    let new_image = trigger_image(table, new);
    let old = old_image.as_deref();
    let new = new_image.as_deref();
    let mut recorded = Vec::new();
    // A relation may sit under a partitioned parent, under one or more
    // inheritance parents, or under a mixture, so the ancestry is a DAG and not
    // the single chain a partition alone forms: `d INHERITS (b, c)` reaches a
    // shared grandparent by two routes. Recording an image once per *relation*
    // rather than once per route is what stops the grandparent's transition
    // table showing that row twice. Both routes reshape by column name, so
    // whichever arrives first produces the same image.
    let mut seen = std::collections::HashSet::new();
    let mut pending = vec![(
        table.clone(),
        old.map(<[crabka_pgtypes::Datum]>::to_vec),
        new.map(<[crabka_pgtypes::Datum]>::to_vec),
    )];
    while let Some((relation, transition_old, transition_new)) = pending.pop() {
        if !seen.insert(relation.id) {
            continue;
        }
        recorded.push(TransitionChange {
            table_id: relation.id,
            operation: operation_name(event).into(),
            old: transition_old.clone(),
            new: transition_new.clone(),
        });
        let mut ancestors = Vec::new();
        if let Some((parent_name, _)) = crate::partition::parent_of(kv, &relation.name)? {
            ancestors.push(parent_name);
        }
        ancestors.extend(crate::inheritance::parents_of(kv, &relation.name)?);
        for parent_name in ancestors {
            let parent = crabka_pgcatalog::get_table(kv, &parent_name)?;
            // The ancestor's columns, read out of this relation's row by name:
            // a child may store them in a different order and may add its own,
            // and the ancestor's transition table shows neither.
            let ordinals = crate::exec::column_mapping(&parent, &relation)?;
            let reshape = |row: &Vec<crabka_pgtypes::Datum>| {
                ordinals
                    .iter()
                    .map(|ordinal| {
                        row.get(*ordinal)
                            .cloned()
                            .unwrap_or(crabka_pgtypes::Datum::Null)
                    })
                    .collect()
            };
            pending.push((
                parent,
                transition_old.as_ref().map(&reshape),
                transition_new.as_ref().map(&reshape),
            ));
        }
    }
    TRANSITION_CHANGES.with(|changes| {
        if let Some(changes) = changes.borrow_mut().as_mut() {
            changes.extend(recorded);
        }
    });
    for trigger in crabka_pgcatalog::trigger::triggers_for_table(kv, table.id)? {
        if trigger.timing == TriggerTiming::After
            && trigger.level == TriggerLevel::Row
            && trigger_matches_event(&trigger, Some(table), event, updated)
            && trigger_is_enabled(&trigger)
            && when_matches(&trigger, table, old, new, ctx)?
        {
            queue_catalog_trigger(kv, &trigger, table, event, old, new)?;
        }
    }
    Ok(())
}

pub(crate) fn fire_statement(
    kv: &dyn Kv,
    table: &Table,
    event: DmlEvent,
    timing: TriggerTiming,
    updated: &[String],
    ctx: &crate::clock::EvalCtx,
) -> Result<(), ExecError> {
    // Opened on the first trigger that actually matches, so the common case —
    // a write against a table with no statement triggers — costs nothing. Every
    // write calls this up to four times (BEFORE and AFTER, per part), so a span
    // per call would outnumber the statements it describes.
    let mut span = tracing::Span::none();
    let mut fired = 0usize;
    for trigger in crabka_pgcatalog::trigger::triggers_for_table(kv, table.id)? {
        if trigger.timing == timing
            && trigger.level == TriggerLevel::Statement
            && trigger_matches_event(&trigger, Some(table), event, updated)
            && trigger_is_enabled(&trigger)
            && when_matches(&trigger, table, None, None, ctx)?
        {
            if span.is_none() {
                span = statement_trigger_span(table, event, timing);
            }
            fired += 1;
            let _guard = span.enter();
            if timing == TriggerTiming::After {
                queue_catalog_trigger(kv, &trigger, table, event, None, None)?;
            } else {
                let _ = invoke_catalog_trigger(kv, &trigger, table, event, None, None)?;
            }
        }
    }
    span.record("pg.triggers_fired", crate::telemetry::integer(fired));
    Ok(())
}

/// Build the span that covers the statement-level triggers one write fires for
/// one `(event, timing)` pair.
///
/// There is one span for the batch. Row-level triggers deliberately get no span
/// of their own. They fire once per row, so a span for each one would be a span
/// per row touched. `pg.lock.row` exists only for a contended acquire for the
/// same reason. The cost of row-level triggers shows up instead as
/// `pg.triggers_fired` on `pg.execute_write`, which [`fired_count`] counts.
fn statement_trigger_span(table: &Table, event: DmlEvent, timing: TriggerTiming) -> tracing::Span {
    tracing::debug_span!(
        target: crate::telemetry::EXEC_TARGET,
        "pg.triggers",
        otel.kind = "internal",
        pg.table_id = crate::telemetry::integer(table.id),
        db.collection.name = table.name.name.as_str(),
        pg.trigger.event = operation_name(event),
        pg.trigger.timing = match timing {
            TriggerTiming::Before => "BEFORE",
            TriggerTiming::After => "AFTER",
            TriggerTiming::InsteadOf => "INSTEAD OF",
        },
        pg.trigger.level = "STATEMENT",
        pg.triggers_fired = tracing::field::Empty,
    )
}

pub(crate) fn clone_partition_triggers(
    kv: &dyn Kv,
    parent: &Table,
    child: &RelationName,
) -> Result<Vec<WriteOp>, ExecError> {
    let sources = crabka_pgcatalog::trigger::triggers_for_table(kv, parent.id)?;
    let sources = sources
        .into_iter()
        .filter(|trigger| trigger.level == TriggerLevel::Row)
        .collect::<Vec<_>>();
    if sources.is_empty() {
        return Ok(Vec::new());
    }
    let mut relations = vec![child.clone()];
    relations.extend(crate::partition::descendants(kv, child)?);
    let mut next_oid = crabka_pgcatalog::trigger::next_trigger_oid(kv)?;
    let mut ops = Vec::new();
    for relation in relations {
        let table = crabka_pgcatalog::get_table(kv, &relation)?;
        for source in &sources {
            if let Some(existing) = get_trigger(kv, table.id, &source.name)? {
                if existing.parent_oid != source.oid {
                    return Err(ExecError::DuplicateObject(format!(
                        "trigger \"{}\" for relation \"{}\" already exists",
                        source.name, relation
                    )));
                }
                continue;
            }
            let mut clone = source.clone();
            clone.oid = next_oid;
            next_oid += 1;
            clone.table_id = table.id;
            clone.table = table.name.clone();
            clone.parent_oid = source.oid;
            ops.extend(put_trigger_ops(kv, &clone)?);
        }
    }
    if !ops.is_empty() {
        ops.insert(
            0,
            crabka_pgcatalog::trigger::set_next_trigger_oid_op(next_oid),
        );
    }
    Ok(ops)
}

pub(crate) fn clone_new_partition_triggers(
    kv: &dyn Kv,
    parent: &Table,
    child: &Table,
) -> Result<Vec<WriteOp>, ExecError> {
    let sources = crabka_pgcatalog::trigger::triggers_for_table(kv, parent.id)?;
    let mut next_oid = crabka_pgcatalog::trigger::next_trigger_oid(kv)?;
    let mut ops = Vec::new();
    for source in sources
        .into_iter()
        .filter(|trigger| trigger.level == TriggerLevel::Row)
    {
        let mut clone = source.clone();
        clone.oid = next_oid;
        next_oid += 1;
        clone.table_id = child.id;
        clone.table = child.name.clone();
        clone.parent_oid = source.oid;
        ops.extend(put_trigger_ops(kv, &clone)?);
    }
    if !ops.is_empty() {
        ops.insert(
            0,
            crabka_pgcatalog::trigger::set_next_trigger_oid_op(next_oid),
        );
    }
    Ok(ops)
}

pub(crate) fn drop_partition_trigger_clones(
    kv: &dyn Kv,
    parent: &Table,
    child: &RelationName,
) -> Result<Vec<WriteOp>, ExecError> {
    let parent_oids: std::collections::HashSet<u32> =
        crabka_pgcatalog::trigger::triggers_for_table(kv, parent.id)?
            .into_iter()
            .map(|trigger| trigger.oid)
            .collect();
    let mut relations = vec![child.clone()];
    relations.extend(crate::partition::descendants(kv, child)?);
    let mut ops = Vec::new();
    for relation in relations {
        let table = crabka_pgcatalog::get_table(kv, &relation)?;
        for trigger in crabka_pgcatalog::trigger::triggers_for_table(kv, table.id)? {
            if parent_oids.contains(&trigger.parent_oid) {
                ops.extend(drop_trigger_ops(table.id, &trigger.name));
            }
        }
    }
    Ok(ops)
}

pub(crate) fn set_table_trigger_mode(
    kv: &dyn Kv,
    table: &Table,
    selector: &parsed::TriggerSelector,
    mode: parsed::TriggerEnableMode,
) -> Result<Vec<WriteOp>, ExecError> {
    if matches!(selector, parsed::TriggerSelector::All)
        && mode == parsed::TriggerEnableMode::Disabled
        && (!crabka_pgcatalog::list_table_foreign_keys(kv, table.id)?.is_empty()
            || !crabka_pgcatalog::list_referencing_foreign_keys(kv, table.id)?.is_empty())
    {
        return Err(ExecError::Unsupported(
            "DISABLE TRIGGER ALL is not supported on tables with foreign keys".into(),
        ));
    }
    let mut triggers = crabka_pgcatalog::trigger::triggers_for_table(kv, table.id)?;
    let matched = triggers.iter().any(|trigger| match selector {
        parsed::TriggerSelector::Named(name) => trigger.name == *name,
        parsed::TriggerSelector::All => true,
        parsed::TriggerSelector::User => !trigger.is_internal,
    });
    if !matched && let parsed::TriggerSelector::Named(name) = selector {
        return Err(ExecError::UndefinedObject(format!(
            "trigger \"{name}\" for table \"{}\" does not exist",
            table.name
        )));
    }
    let mut ops = Vec::new();
    let mut parent_oids = std::collections::HashSet::new();
    for trigger in &mut triggers {
        let selected = match selector {
            parsed::TriggerSelector::Named(name) => trigger.name == *name,
            parsed::TriggerSelector::All => true,
            parsed::TriggerSelector::User => !trigger.is_internal,
        };
        if selected {
            trigger.enabled = map_enabled(mode);
            parent_oids.insert(trigger.oid);
            ops.extend(put_trigger_ops(kv, trigger)?);
        }
    }
    for mut clone in trigger_descendants(kv, &parent_oids)? {
        clone.enabled = map_enabled(mode);
        ops.extend(put_trigger_ops(kv, &clone)?);
    }
    Ok(ops)
}
