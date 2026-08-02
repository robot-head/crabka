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
            routine,
            values: Vec::new(),
            kind: crate::routine::FunctionRequestKind::Trigger(Box::new(invocation)),
            reply,
        })
        .map_err(|_| {
            ExecError::ObjectNotInPrerequisiteState("trigger function executor stopped".into())
        })?;
    match response.recv().map_err(|_| {
        ExecError::ObjectNotInPrerequisiteState("trigger function executor stopped".into())
    })?? {
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
        id,
        name: view.name,
        columns: view.columns,
        sharded: false,
        sharding: None,
        foreign: None,
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
                        let Some(definition) =
                            table.columns.iter().find(|item| item.name == *column)
                        else {
                            return Err(ExecError::UndefinedTableColumn {
                                column: column.clone(),
                                table: table.name.to_string(),
                            });
                        };
                        if definition.generated.is_some() {
                            return Err(trigger_error(
                                "42P17",
                                "trigger on column that is a generated column is not supported",
                            ));
                        }
                    }
                    if !mapped.update_columns.contains(column) {
                        mapped.update_columns.push(column.clone());
                    }
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
    if timing == TriggerTiming::InsteadOf && (!is_view || level != TriggerLevel::Row) {
        return Err(trigger_error(
            "42P17",
            "INSTEAD OF triggers must be FOR EACH ROW on views",
        ));
    }
    if timing != TriggerTiming::InsteadOf && is_view && level != TriggerLevel::Statement {
        return Err(trigger_error(
            "42P17",
            "BEFORE and AFTER triggers on views must be FOR EACH STATEMENT",
        ));
    }
    if level == TriggerLevel::Row && events.truncate {
        return Err(trigger_error(
            "42P17",
            "TRUNCATE FOR EACH ROW triggers are not supported",
        ));
    }
    if timing == TriggerTiming::InsteadOf && (events.truncate || stmt.when.is_some()) {
        return Err(trigger_error(
            "42P17",
            "INSTEAD OF triggers cannot have WHEN conditions or TRUNCATE events",
        ));
    }
    if timing == TriggerTiming::InsteadOf && !events.update_columns.is_empty() {
        return Err(trigger_error(
            "42P17",
            "INSTEAD OF UPDATE triggers cannot specify a column list",
        ));
    }
    if !stmt.transitions.is_empty() {
        if timing != TriggerTiming::After || stmt.constraint || is_view {
            return Err(trigger_error(
                "42P17",
                "transition tables can only be specified for AFTER non-constraint triggers",
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

pub(crate) fn create_event(
    kv: &dyn Kv,
    stmt: &parsed::CreateEventTrigger,
    owner: &str,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    if get_event_trigger(kv, &stmt.name)?.is_some() {
        return Err(ExecError::DuplicateObject(format!(
            "event trigger \"{}\" already exists",
            stmt.name
        )));
    }
    for filter in &stmt.filters {
        if filter.variable != "tag" {
            return Err(trigger_error(
                "22023",
                format!("filter variable \"{}\" not recognized", filter.variable),
            ));
        }
        if stmt.event == parsed::EventTriggerEvent::Login {
            return Err(trigger_error(
                "22023",
                "filter variable TAG is not supported for event LOGIN",
            ));
        }
        if let Some(value) = filter
            .values
            .iter()
            .find(|value| !EVENT_COMMAND_TAGS.contains(&value.as_str()))
        {
            return Err(trigger_error(
                "22023",
                format!("filter value \"{value}\" not recognized for filter variable \"tag\""),
            ));
        }
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
        parsed::AlterEventTriggerAction::OwnerTo(owner) => trigger.owner = owner.clone(),
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

pub(crate) fn event_trigger_ddl_is_excluded(stmt: &parsed::Statement) -> bool {
    matches!(
        stmt,
        parsed::Statement::CreateEventTrigger(_)
            | parsed::Statement::AlterEventTrigger { .. }
            | parsed::Statement::DropEventTrigger { .. }
            | parsed::Statement::CreateRole { .. }
            | parsed::Statement::DropRole { .. }
    )
}

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
        Statement::DropIndex { .. } => "DROP INDEX",
        Statement::CreateView { .. } => "CREATE VIEW",
        Statement::DropView { .. } => "DROP VIEW",
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
        Statement::RevokeTablePrivileges { .. } => "REVOKE",
        Statement::ImportForeignSchema { .. } => "IMPORT FOREIGN SCHEMA",
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

const EVENT_COMMAND_TAGS: &[&str] = &[
    "ALTER DOMAIN",
    "ALTER FOREIGN DATA WRAPPER",
    "ALTER FUNCTION",
    "ALTER PROCEDURE",
    "ALTER ROUTINE",
    "ALTER SCHEMA",
    "ALTER SERVER",
    "ALTER TABLE",
    "ALTER TEXT SEARCH CONFIGURATION",
    "ALTER TEXT SEARCH DICTIONARY",
    "ALTER TRIGGER",
    "ALTER TYPE",
    "ALTER USER MAPPING",
    "COMMENT",
    "CREATE DOMAIN",
    "CREATE FOREIGN DATA WRAPPER",
    "CREATE FOREIGN TABLE",
    "CREATE FUNCTION",
    "CREATE INDEX",
    "CREATE PROCEDURE",
    "CREATE ROUTINE",
    "CREATE SCHEMA",
    "CREATE SEQUENCE",
    "CREATE SERVER",
    "CREATE TABLE",
    "CREATE TEXT SEARCH CONFIGURATION",
    "CREATE TEXT SEARCH DICTIONARY",
    "CREATE TRIGGER",
    "CREATE TYPE",
    "CREATE USER MAPPING",
    "CREATE VIEW",
    "DROP DOMAIN",
    "DROP FOREIGN DATA WRAPPER",
    "DROP FOREIGN TABLE",
    "DROP FUNCTION",
    "DROP INDEX",
    "DROP PROCEDURE",
    "DROP ROUTINE",
    "DROP SCHEMA",
    "DROP SEQUENCE",
    "DROP SERVER",
    "DROP TABLE",
    "DROP TEXT SEARCH CONFIGURATION",
    "DROP TEXT SEARCH DICTIONARY",
    "DROP TRIGGER",
    "DROP TYPE",
    "DROP USER MAPPING",
    "DROP VIEW",
    "GRANT",
    "IMPORT FOREIGN SCHEMA",
    "REVOKE",
];

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
            i32::try_from(table.id).unwrap_or_default()
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
            schema_name: Some(name.schema.clone()),
            object_name: Some(object_name.to_string()),
            identity: format!(
                "{}.{}",
                crate::catalog_fn::quote_identifier(&name.schema),
                crate::catalog_fn::quote_identifier(object_name)
            ),
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
        schema_name: Some(name.schema.clone()),
        object_name: Some(name.name.clone()),
        identity: format!(
            "{}.{}",
            crate::catalog_fn::quote_identifier(&name.schema),
            crate::catalog_fn::quote_identifier(&name.name)
        ),
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
            schema_name: Some(trigger.table.schema.clone()),
            object_name: Some(trigger.name.clone()),
            identity: format!(
                "{} on {}.{}",
                crate::catalog_fn::quote_identifier(&trigger.name),
                crate::catalog_fn::quote_identifier(&trigger.table.schema),
                crate::catalog_fn::quote_identifier(&trigger.table.name)
            ),
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
        schema_name: Some(foreign_key.table.schema.clone()),
        object_name: Some(foreign_key.name.clone()),
        identity: format!(
            "{} on {}.{}",
            crate::catalog_fn::quote_identifier(&foreign_key.name),
            crate::catalog_fn::quote_identifier(&foreign_key.table.schema),
            crate::catalog_fn::quote_identifier(&foreign_key.table.name)
        ),
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

fn trigger_matches_event(trigger: &Trigger, event: DmlEvent, updated: &[String]) -> bool {
    match event {
        DmlEvent::Insert => trigger.events.insert,
        DmlEvent::Delete => trigger.events.delete,
        DmlEvent::Truncate => trigger.events.truncate,
        DmlEvent::Update => {
            trigger.events.update
                && (trigger.events.update_columns.is_empty()
                    || trigger
                        .events
                        .update_columns
                        .iter()
                        .any(|column| updated.contains(column)))
        }
    }
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
                    && trigger_matches_event(&trigger, event, updated)
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
    scope
        .columns
        .extend(crate::scope::Scope::single(table, "new").columns);
    let nulls = vec![crabka_pgtypes::Datum::Null; table.columns.len()];
    let mut values = old.unwrap_or(&nulls).to_vec();
    values.extend_from_slice(new.unwrap_or(&nulls));
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
            relation_oid: trigger.table_id,
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
        relation_oid: trigger.table_id,
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

pub(crate) fn fire_before_row(
    kv: &dyn Kv,
    table: &Table,
    event: DmlEvent,
    updated: &[String],
    old: Option<&[crabka_pgtypes::Datum]>,
    mut new: Option<Vec<crabka_pgtypes::Datum>>,
    ctx: &crate::clock::EvalCtx,
) -> Result<Option<Vec<crabka_pgtypes::Datum>>, ExecError> {
    for trigger in crabka_pgcatalog::trigger::triggers_for_table(kv, table.id)? {
        if trigger.timing != TriggerTiming::Before
            || trigger.level != TriggerLevel::Row
            || !trigger_matches_event(&trigger, event, updated)
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
        Ok(old.map(|row| row.to_vec()))
    } else {
        Ok(new)
    }
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
            && trigger_matches_event(&trigger, event, updated)
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
    let mut recorded = Vec::new();
    let mut relation = table.clone();
    let mut transition_old = old.map(|row| row.to_vec());
    let mut transition_new = new.map(|row| row.to_vec());
    loop {
        recorded.push(TransitionChange {
            table_id: relation.id,
            operation: operation_name(event).into(),
            old: transition_old.clone(),
            new: transition_new.clone(),
        });
        let Some((parent_name, _)) = crate::partition::parent_of(kv, &relation.name)? else {
            break;
        };
        let parent = crabka_pgcatalog::get_table(kv, &parent_name)?;
        let ordinals = crate::exec::column_mapping(&parent, &relation)?;
        let reshape = |row: Vec<crabka_pgtypes::Datum>| {
            ordinals
                .iter()
                .map(|ordinal| {
                    row.get(*ordinal)
                        .cloned()
                        .unwrap_or(crabka_pgtypes::Datum::Null)
                })
                .collect()
        };
        transition_old = transition_old.map(&reshape);
        transition_new = transition_new.map(reshape);
        relation = parent;
    }
    TRANSITION_CHANGES.with(|changes| {
        if let Some(changes) = changes.borrow_mut().as_mut() {
            changes.extend(recorded);
        }
    });
    for trigger in crabka_pgcatalog::trigger::triggers_for_table(kv, table.id)? {
        if trigger.timing == TriggerTiming::After
            && trigger.level == TriggerLevel::Row
            && trigger_matches_event(&trigger, event, updated)
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
    for trigger in crabka_pgcatalog::trigger::triggers_for_table(kv, table.id)? {
        if trigger.timing == timing
            && trigger.level == TriggerLevel::Statement
            && trigger_matches_event(&trigger, event, updated)
            && trigger_is_enabled(&trigger)
            && when_matches(&trigger, table, None, None, ctx)?
        {
            if timing == TriggerTiming::After {
                queue_catalog_trigger(kv, &trigger, table, event, None, None)?;
            } else {
                let _ = invoke_catalog_trigger(kv, &trigger, table, event, None, None)?;
            }
        }
    }
    Ok(())
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
