//! Small PL/pgSQL interpreter layered on the ordinary SQL executor.
//!
//! The procedural layer owns only control flow and lexical variables. Embedded
//! SQL stays ordinary parsed SQL, so it keeps the session's MVCC, locking,
//! constraint, savepoint, and command-tag behaviour.

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::Arc,
};

use crabka_pgcatalog::routine::{Routine, RoutineKind, RoutineResult};
use crabka_pgparser::ast::{
    ArraySubscript, AssignmentValue, BinaryOp, CteBody, CursorTarget, Expr, FetchCount,
    FetchDirection, FuncArgs, JoinConstraint, PlPgSqlBlock, PlPgSqlDeclaration, PlPgSqlInto,
    PlPgSqlLoop, PlPgSqlRaise, PlPgSqlRaiseLevel, PlPgSqlStatement, PlPgSqlTarget,
    PlPgSqlVariableConflict, QueryBody, QueryExpr, RoutineType, SelectItem, SetExpr, Statement,
    TableExpr,
};
use crabka_pgtypes::{ArrayValue, ColumnType, Datum, ElemType, RecordValue};
use crabka_pgwire::{
    engine::{FieldDescription, QueryResult},
    error::PgError,
};

use crate::{error::ExecError, session::SqlSession};

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone)]
struct Slot {
    value: Datum,
    ty: ColumnType,
    record_types: Option<Arc<[ColumnType]>>,
    constant: bool,
    not_null: bool,
}

fn declaration_type(
    ty: &RoutineType,
    lookup: impl FnOnce(&str) -> Result<Option<ColumnType>, ExecError>,
) -> Result<ColumnType, ExecError> {
    if let Some(ty) = ty.resolved {
        return Ok(ty);
    }
    if let Some(reference) = ty.name.strip_suffix("%type") {
        return lookup(reference)?.ok_or_else(|| ExecError::UndefinedColumn(reference.into()));
    }
    if ty.name.ends_with("%rowtype") {
        return Ok(ColumnType::Record(None));
    }
    Ok(if ty.name.eq_ignore_ascii_case("record") {
        ColumnType::Record(None)
    } else {
        ColumnType::Text
    })
}

pub(crate) fn cast_value(
    value: &Datum,
    ty: ColumnType,
    ctx: &crate::clock::EvalCtx,
) -> Result<Datum, ExecError> {
    let value = crate::eval::cast_value(value, ty, &ctx.time_zone)?;
    crate::usertype::check_domain(ty, &value, ctx)?;
    Ok(value)
}

#[derive(Default)]
struct Frame {
    label: Option<String>,
    slots: HashMap<String, Slot>,
    aliases: HashMap<String, String>,
}

fn inferred_record_types(value: &Datum) -> Option<Arc<[ColumnType]>> {
    let Datum::Record(record) = value else {
        return None;
    };
    record
        .values
        .iter()
        .map(Datum::column_type)
        .collect::<Option<Vec<_>>>()
        .map(Arc::from)
}

enum Flow {
    Next,
    Return(Datum),
    LoopControl {
        continuing: bool,
        label: Option<String>,
    },
}

enum ScalarFlow {
    Next,
    Return(Datum),
    LoopControl {
        continuing: bool,
        label: Option<String>,
    },
}

struct ScalarInterpreter<'a> {
    frames: Vec<Frame>,
    output_slot: Option<String>,
    ctx: &'a crate::clock::EvalCtx,
    steps: usize,
    active_error: Option<PgError>,
    context: String,
}

const MAX_SCALAR_STEPS: usize = 100_000;

struct Interpreter<'a> {
    session: &'a mut SqlSession,
    frames: Vec<Frame>,
    allow_transaction_control: bool,
    savepoint_serial: u64,
    active_error: Option<PgError>,
    exception_depth: u32,
    cursor_declarations: HashMap<String, CursorDeclaration>,
    last_row_count: usize,
    output_slot: Option<String>,
    set_results: Option<SetResultCollector>,
    returns_set: bool,
    context: String,
    variable_conflict: PlPgSqlVariableConflict,
    routine_oid: u32,
}

#[derive(Clone)]
struct CursorDeclaration {
    scroll: Option<bool>,
    arguments: Vec<(String, RoutineType)>,
    statement: Statement,
}

struct SetResultCollector {
    columns: Vec<(String, ColumnType)>,
    rows: Vec<Vec<Datum>>,
}

struct ProcedureCallOutput {
    fields: Vec<FieldDescription>,
    values: Vec<Datum>,
}

pub(crate) async fn execute_do(
    session: &mut SqlSession,
    language: &str,
    body: &str,
) -> Result<QueryResult, ExecError> {
    if language != "plpgsql" {
        return Err(crate::routine::do_block(language));
    }
    let block =
        crabka_pgparser::parse_plpgsql(body).map_err(|error| ExecError::Syntax(error.message))?;
    execute_invocation(session, block, root_frame(), "DO").await
}

pub(crate) async fn execute_call(
    session: &mut SqlSession,
    name: &str,
    args: &[Expr],
    named_args: &[(String, Expr)],
    variadic: Option<&Expr>,
) -> Result<QueryResult, ExecError> {
    let allow_transaction_control = session.plpgsql_is_idle();
    execute_call_with_transaction_control(
        session,
        name,
        args,
        named_args,
        variadic,
        allow_transaction_control,
    )
    .await
}

async fn execute_call_with_transaction_control(
    session: &mut SqlSession,
    name: &str,
    args: &[Expr],
    named_args: &[(String, Expr)],
    variadic: Option<&Expr>,
    allow_transaction_control: bool,
) -> Result<QueryResult, ExecError> {
    let output = execute_call_with_output(
        session,
        name,
        args,
        named_args,
        variadic,
        allow_transaction_control,
    )
    .await?;
    Ok(procedure_call_result(session, output))
}

async fn execute_call_with_output(
    session: &mut SqlSession,
    name: &str,
    args: &[Expr],
    named_args: &[(String, Expr)],
    variadic: Option<&Expr>,
    allow_transaction_control: bool,
) -> Result<Option<ProcedureCallOutput>, ExecError> {
    let owns_transaction = session.plpgsql_is_idle();
    if owns_transaction {
        session.plpgsql_begin_implicit_transaction().await?;
    }
    let result = execute_call_body(
        session,
        name,
        args,
        named_args,
        variadic,
        allow_transaction_control,
    )
    .await;
    if owns_transaction {
        session.plpgsql_finish_implicit_transaction(result).await
    } else {
        result
    }
}

async fn execute_call_body(
    session: &mut SqlSession,
    name: &str,
    args: &[Expr],
    named_args: &[(String, Expr)],
    variadic: Option<&Expr>,
    allow_transaction_control: bool,
) -> Result<Option<ProcedureCallOutput>, ExecError> {
    let bound = crate::routine::bind_procedure_call(
        session.plpgsql_catalog(),
        name,
        args,
        named_args,
        variadic,
    )?
    .ok_or_else(|| ExecError::UndefinedFunction(format!("procedure {name} does not exist")))?;
    let routine = bound.routine;
    if routine.kind != RoutineKind::Procedure {
        return Err(ExecError::WrongObjectType(format!(
            "{} is not a procedure\nHINT:  To call a function, use SELECT.",
            routine.identity()
        )));
    }
    if routine.language == "sql" {
        let allow_transaction_control =
            allow_transaction_control && !routine.security_definer && routine.config.is_empty();
        let frame = bind_parameters(session, &routine, &bound.args).await?;
        let interpreter = Interpreter {
            session,
            frames: vec![frame],
            allow_transaction_control,
            savepoint_serial: 0,
            active_error: None,
            exception_depth: 0,
            cursor_declarations: HashMap::new(),
            last_row_count: 0,
            output_slot: None,
            set_results: None,
            returns_set: false,
            context: format!("SQL procedure {}", routine.identity()),
            variable_conflict: PlPgSqlVariableConflict::UseVariable,
            routine_oid: routine.oid,
        };
        let statements = crate::routine::parse_body(&routine)?;
        for (index, statement) in statements.into_iter().enumerate() {
            let statement = interpreter
                .bind_statement(&statement)
                .map_err(|error| sql_statement_error(error, &routine, index + 1))?;
            match &statement {
                Statement::Commit { .. } | Statement::Rollback { .. }
                    if !allow_transaction_control =>
                {
                    return Err(ExecError::ActiveSqlTransaction(
                        "invalid transaction termination".into(),
                    ));
                }
                _ => {
                    Box::pin(interpreter.session.run_one(&statement))
                        .await
                        .map_err(|error| sql_statement_error(error, &routine, index + 1))?;
                }
            }
        }
        return Ok(None);
    }
    if routine.language != "plpgsql" {
        return Err(ExecError::Unsupported(format!(
            "cannot execute procedure {}: language \"{}\" has no interpreter",
            routine.identity(),
            routine.language
        )));
    }
    let frame = bind_parameters(session, &routine, &bound.args).await?;
    let block = crabka_pgparser::parse_plpgsql(&routine.body)
        .map_err(|error| ExecError::Syntax(error.message))?;
    execute_procedure_invocation(session, &routine, block, frame, allow_transaction_control).await
}

fn procedure_call_result(session: &SqlSession, output: Option<ProcedureCallOutput>) -> QueryResult {
    let Some(output) = output else {
        return QueryResult::Command { tag: "CALL".into() };
    };
    crate::exec::rows_result_with_tag(
        output.fields,
        &[output.values],
        session.plpgsql_eval_context().output_style(),
        "CALL".into(),
    )
}

pub(crate) async fn execute_scalar_function(
    session: &mut SqlSession,
    routine: &Routine,
    values: &[Datum],
) -> Result<Datum, ExecError> {
    let block = crate::routine::parse_plpgsql_body(routine)?;
    let ctx = session.plpgsql_eval_context();
    let (frame, output_slot) = bind_scalar_parameters(routine, values, &ctx, true)?;
    let mut interpreter = Interpreter {
        session,
        frames: vec![frame],
        allow_transaction_control: false,
        savepoint_serial: 0,
        active_error: None,
        exception_depth: 0,
        cursor_declarations: HashMap::new(),
        last_row_count: 0,
        output_slot,
        set_results: None,
        returns_set: false,
        context: format!("PL/pgSQL function {}", routine.identity()),
        variable_conflict: block.variable_conflict,
        routine_oid: routine.oid,
    };
    match interpreter.exec_block(&block).await? {
        Flow::Return(value) => scalar_function_result(routine, Some(value)),
        Flow::Next if interpreter.output_slot.is_some() => {
            scalar_function_result(routine, Some(interpreter.output_value()))
        }
        Flow::Next => scalar_function_result(routine, None),
        Flow::LoopControl { .. } => Err(ExecError::Syntax(
            "EXIT or CONTINUE cannot be used outside a loop".into(),
        )),
    }
}

pub(crate) async fn execute_sql_scalar_function(
    session: &mut SqlSession,
    routine: &Routine,
    values: &[Datum],
) -> Result<Datum, ExecError> {
    let ctx = session.plpgsql_eval_context();
    let (frame, output_slot) = bind_scalar_parameters(routine, values, &ctx, false)?;
    let interpreter = Interpreter {
        session,
        frames: vec![frame],
        allow_transaction_control: false,
        savepoint_serial: 0,
        active_error: None,
        exception_depth: 0,
        cursor_declarations: HashMap::new(),
        last_row_count: 0,
        output_slot,
        set_results: None,
        returns_set: false,
        context: format!("SQL function {}", routine.identity()),
        variable_conflict: PlPgSqlVariableConflict::UseVariable,
        routine_oid: routine.oid,
    };
    let statements = crate::routine::parse_body(routine)?;
    let mut final_result = None;
    for (index, statement) in statements.into_iter().enumerate() {
        let statement = interpreter
            .bind_statement(&statement)
            .map_err(|error| sql_statement_error(error, routine, index + 1))?;
        final_result = Some(
            Box::pin(interpreter.session.run_one(&statement))
                .await
                .map_err(|error| sql_statement_error(error, routine, index + 1))?,
        );
    }
    if crate::routine::declared_returns_void(routine) {
        return Ok(crate::routine::void_result_value());
    }
    let QueryResult::Rows { fields, rows, .. } =
        final_result.ok_or_else(|| crate::routine::sql_empty_body_error(routine, values))?
    else {
        return Err(crate::routine::sql_empty_body_error(routine, values));
    };
    let Some(row) = rows.first() else {
        return Ok(Datum::Null);
    };
    let rowtype =
        crate::routine::declared_relation_rowtype(interpreter.session.plpgsql_catalog(), routine)?;
    let returns_record = crate::routine::declared_output_parameter_count(routine) > 1
        || matches!(
            &routine.result,
            RoutineResult::Type { ty, setof: false }
                if ty.is_record() || matches!(ty.column, Some(ColumnType::Record(_)))
        );
    // A SQL function returning a relation rowtype may return that composite as
    // its sole result column. It is already the function result; wrapping it
    // as a row again would make its first field a composite instead of `id`.
    if let (Some(rowtype), [field], [value]) = (rowtype, fields.as_slice(), row.as_slice())
        && field.type_oid == rowtype.oid
    {
        return interpreter
            .session
            .plpgsql_decode_cell(field, value.as_ref());
    }
    if rowtype.is_some() || returns_record {
        if row.len() != fields.len() {
            return Err(ExecError::ObjectNotInPrerequisiteState(
                "SQL function executor returned the wrong table width".into(),
            ));
        }
        let values = fields
            .iter()
            .zip(row)
            .map(|(field, value)| {
                interpreter
                    .session
                    .plpgsql_decode_cell(field, value.as_ref())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let names: Vec<_> = fields.iter().map(|field| field.name.clone()).collect();
        return Ok(Datum::Record(RecordValue::named(
            rowtype,
            Arc::from(names),
            values,
        )));
    }
    match (fields.as_slice(), row.as_slice()) {
        ([field], [value]) => interpreter
            .session
            .plpgsql_decode_cell(field, value.as_ref()),
        _ => Err(ExecError::TypeMismatch(
            "SQL function result must have exactly one column".into(),
        )),
    }
}

pub(crate) async fn execute_trigger_function(
    session: &mut SqlSession,
    routine: &Routine,
    invocation: crate::trigger::TriggerInvocation,
) -> Result<Datum, ExecError> {
    let block = crate::routine::parse_plpgsql_body(routine)?;
    let is_event_trigger = invocation.event.is_some();
    // An event trigger's body can raise the event that fired it -- a DROP inside
    // an `ON sql_drop` body is enough -- and nothing on this path counts the
    // nesting. `trigger::invoke` guards the row-trigger path, but
    // `fire_event_triggers` reaches this function directly, so an unguarded
    // event trigger recurses until the stack is gone and the process aborts,
    // taking every other connection with it.
    let _event_call_depth = is_event_trigger
        .then(|| session.plpgsql_enter_call())
        .transpose()?;
    let mut frame = root_frame();
    frame.label = Some(routine.name.clone());
    let record_types: Arc<[ColumnType]> = Arc::from(invocation.column_types);
    let mut insert = |name: &str, value: Datum, ty: ColumnType, constant: bool| {
        frame.slots.insert(
            name.into(),
            Slot {
                record_types: matches!(ty, ColumnType::Record(_))
                    .then(|| Arc::clone(&record_types)),
                value,
                ty,
                constant,
                not_null: false,
            },
        );
    };
    insert("new", invocation.new, ColumnType::Record(None), false);
    insert("old", invocation.old, ColumnType::Record(None), false);
    for (name, value) in [
        ("tg_name", Datum::Text(invocation.name)),
        ("tg_when", Datum::Text(invocation.when)),
        ("tg_level", Datum::Text(invocation.level)),
        ("tg_op", Datum::Text(invocation.operation)),
        ("tg_table_name", Datum::Text(invocation.table_name.clone())),
        ("tg_relname", Datum::Text(invocation.table_name)),
        ("tg_table_schema", Datum::Text(invocation.table_schema)),
    ] {
        insert(name, value, ColumnType::Text, true);
    }
    insert(
        "tg_relid",
        Datum::Int4(i32::try_from(invocation.relation_oid).unwrap_or(0)),
        ColumnType::Int4,
        true,
    );
    let nargs = i32::try_from(invocation.arguments.len()).unwrap_or(i32::MAX);
    let arguments = invocation.arguments.into_iter().map(Datum::Text).collect();
    insert("tg_nargs", Datum::Int4(nargs), ColumnType::Int4, true);
    insert(
        "tg_argv",
        Datum::Array(ArrayValue::with_dims(
            ElemType::Text,
            arguments,
            vec![crabka_pgtypes::ArrayDim::new(0, nargs)],
        )),
        ColumnType::Array(ElemType::Text),
        true,
    );
    if let Some(event) = invocation.event {
        insert("tg_event", Datum::Text(event), ColumnType::Text, true);
    }
    if let Some(tag) = invocation.tag {
        insert("tg_tag", Datum::Text(tag), ColumnType::Text, true);
    }
    let mut interpreter = Interpreter {
        session,
        frames: vec![frame],
        allow_transaction_control: false,
        savepoint_serial: 0,
        active_error: None,
        exception_depth: 0,
        cursor_declarations: HashMap::new(),
        last_row_count: 0,
        output_slot: None,
        set_results: None,
        returns_set: false,
        context: format!("PL/pgSQL function {}", routine.identity()),
        variable_conflict: block.variable_conflict,
        routine_oid: routine.oid,
    };
    match interpreter.exec_block(&block).await? {
        Flow::Return(value) => Ok(value),
        Flow::Next if is_event_trigger => Ok(Datum::Null),
        Flow::Next => Err(ExecError::FunctionError {
            sqlstate: "2F005",
            message: "control reached end of trigger procedure without RETURN".to_string(),
        }),
        Flow::LoopControl { .. } => Err(ExecError::Syntax(
            "EXIT or CONTINUE cannot be used outside a loop".into(),
        )),
    }
}

pub(crate) async fn execute_table_function(
    session: &mut SqlSession,
    routine: &Routine,
    values: &[Datum],
    columns: Vec<(String, ColumnType)>,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let block = crate::routine::parse_plpgsql_body(routine)?;
    let ctx = session.plpgsql_eval_context();
    let (frame, output_slot) = bind_scalar_parameters(routine, values, &ctx, true)?;
    let mut interpreter = Interpreter {
        session,
        frames: vec![frame],
        allow_transaction_control: false,
        savepoint_serial: 0,
        active_error: None,
        exception_depth: 0,
        cursor_declarations: HashMap::new(),
        last_row_count: 0,
        output_slot,
        set_results: Some(SetResultCollector {
            columns: columns.clone(),
            rows: Vec::new(),
        }),
        returns_set: routine.returns_set(),
        context: format!("PL/pgSQL function {}", routine.identity()),
        variable_conflict: block.variable_conflict,
        routine_oid: routine.oid,
    };
    let flow = interpreter.exec_block(&block).await?;
    if !routine.returns_set() {
        match flow {
            Flow::Return(Datum::Record(record)) if columns.len() > 1 => {
                interpreter.push_set_row(record.values)?;
            }
            Flow::Return(value) if columns.len() == 1 => {
                interpreter.push_set_row(vec![value])?;
            }
            Flow::Return(_) | Flow::Next => {
                interpreter.push_current_output_row()?;
            }
            Flow::LoopControl { .. } => {
                return Err(ExecError::Syntax(
                    "EXIT or CONTINUE cannot be used outside a loop".into(),
                ));
            }
        }
    } else if matches!(flow, Flow::LoopControl { .. }) {
        return Err(ExecError::Syntax(
            "EXIT or CONTINUE cannot be used outside a loop".into(),
        ));
    }
    let collector = interpreter
        .set_results
        .take()
        .expect("set result collector");
    Ok(collector.rows)
}

pub(crate) async fn execute_sql_table_function(
    session: &mut SqlSession,
    routine: &Routine,
    values: &[Datum],
    columns: Vec<(String, ColumnType)>,
) -> Result<Vec<Vec<Datum>>, ExecError> {
    let ctx = session.plpgsql_eval_context();
    let (frame, output_slot) = bind_scalar_parameters(routine, values, &ctx, false)?;
    let interpreter = Interpreter {
        session,
        frames: vec![frame],
        allow_transaction_control: false,
        savepoint_serial: 0,
        active_error: None,
        exception_depth: 0,
        cursor_declarations: HashMap::new(),
        last_row_count: 0,
        output_slot,
        set_results: None,
        returns_set: false,
        context: format!("SQL function {}", routine.identity()),
        variable_conflict: PlPgSqlVariableConflict::UseVariable,
        routine_oid: routine.oid,
    };
    let mut final_result = None;
    let mut final_row_types = None;
    let mut final_statement = 0;
    for (index, statement) in crate::routine::parse_body(routine)?.into_iter().enumerate() {
        let statement = interpreter
            .bind_statement(&statement)
            .map_err(|error| sql_statement_error(error, routine, index + 1))?;
        final_row_types = simple_row_result_types(&statement)
            .map_err(|error| sql_statement_error(error, routine, index + 1))?;
        final_statement = index + 1;
        final_result = Some(
            Box::pin(interpreter.session.run_one(&statement))
                .await
                .map_err(|error| sql_statement_error(error, routine, index + 1))?,
        );
    }
    if crate::routine::declared_returns_setof_void(routine) {
        return Ok(Vec::new());
    }
    let QueryResult::Rows { fields, rows, .. } = final_result.ok_or_else(|| {
        ExecError::Syntax(
            "SQL function body must contain a final query or DML RETURNING statement".into(),
        )
    })?
    else {
        return Err(ExecError::Syntax(
            "SQL function body must contain a final query or DML RETURNING statement".into(),
        ));
    };
    validate_record_column_definitions(routine, final_statement, &fields, &columns)?;
    let named_rowtype =
        crate::routine::declared_relation_rowtype(interpreter.session.plpgsql_catalog(), routine)?;
    let packed_composite = fields.len() == 1
        && (columns.len() > 1
            || named_rowtype.is_some_and(|rowtype| fields[0].type_oid == rowtype.oid));
    let composite_result = match columns.as_slice() {
        [(_, ColumnType::Record(Some(rowtype)))] => Some(*rowtype),
        _ => None,
    };
    let unpacked_composite = composite_result.is_some() && fields.len() != 1;
    if !columns.is_empty()
        && fields.len() != columns.len()
        && !packed_composite
        && !unpacked_composite
    {
        return Err(ExecError::TypeMismatch(
            "SQL function result has the wrong number of columns".into(),
        ));
    }
    let rows = rows
        .into_iter()
        .map(|row| {
            if row.len() != fields.len() {
                return Err(ExecError::ObjectNotInPrerequisiteState(
                    "SQL function executor returned the wrong table width".into(),
                ));
            }
            if columns.is_empty() {
                let values = fields
                    .iter()
                    .zip(row)
                    .map(|(field, value)| {
                        interpreter
                            .session
                            .plpgsql_decode_cell(field, value.as_ref())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let names = Arc::from(
                    fields
                        .iter()
                        .map(|field| field.name.clone())
                        .collect::<Vec<_>>(),
                );
                return Ok(vec![Datum::Record(RecordValue::named(None, names, values))]);
            }
            if let Some(rowtype) = composite_result {
                let values = fields
                    .iter()
                    .zip(row)
                    .map(|(field, value)| {
                        interpreter
                            .session
                            .plpgsql_decode_cell(field, value.as_ref())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let names = Arc::from(
                    fields
                        .iter()
                        .map(|field| field.name.clone())
                        .collect::<Vec<_>>(),
                );
                return Ok(vec![Datum::Record(RecordValue::named(
                    Some(rowtype),
                    names,
                    values,
                ))]);
            }
            if packed_composite {
                if let Some(actual_types) = &final_row_types {
                    check_packed_result_types(actual_types, &columns)?;
                }
                let Some(cell) = row[0].as_ref() else {
                    return Ok(vec![Datum::Null; columns.len()]);
                };
                let text = std::str::from_utf8(&cell.text).map_err(|error| {
                    ExecError::Syntax(format!("invalid UTF-8 query result: {error}"))
                })?;
                let fields =
                    crabka_pgtypes::composite::record_fields(text).map_err(ExecError::from)?;
                if fields.len() != columns.len() {
                    return Err(ExecError::TypeMismatch(
                        "SQL function result has the wrong number of columns".into(),
                    ));
                }
                return fields
                    .into_iter()
                    .zip(&columns)
                    .map(|(field, (_, ty))| match field {
                        Some(text) => interpreter.session.plpgsql_decode_text(&text, *ty),
                        None => Ok(Datum::Null),
                    })
                    .collect();
            }
            fields
                .iter()
                .zip(row)
                .map(|(field, value)| {
                    interpreter
                        .session
                        .plpgsql_decode_cell(field, value.as_ref())
                })
                .collect()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(if routine.returns_set() {
        rows
    } else {
        rows.into_iter().take(1).collect()
    })
}

fn validate_record_column_definitions(
    routine: &Routine,
    statement: usize,
    fields: &[FieldDescription],
    columns: &[(String, ColumnType)],
) -> Result<(), ExecError> {
    let RoutineResult::Type { ty, .. } = &routine.result else {
        return Ok(());
    };
    if !(ty.is_record() || matches!(ty.column, Some(ColumnType::Record(None))))
        || routine.output_params().next().is_some()
    {
        return Ok(());
    }
    for (index, (field, (_, expected))) in fields.iter().zip(columns).enumerate() {
        let actual = crate::exec::column_type_from_oid(field.type_oid)?;
        if crabka_pgtypes::cast::assignment_cast_allowed(actual, *expected) {
            continue;
        }
        return Err(sql_statement_error(
            ExecError::Remote(
                PgError::error(
                    "42P13",
                    "return type mismatch in function declared to return record",
                )
                .with_detail(format!(
                    "Final statement returns {} instead of {} at column {}.",
                    actual.name(),
                    expected.name(),
                    index + 1
                )),
            ),
            routine,
            statement,
        ));
    }
    Ok(())
}

/// Infer the fields of a bound `SELECT ROW(...)` body before the wire result
/// turns that anonymous record into text. Other SQL bodies keep runtime-only
/// validation because their projected types can depend on relations or CTEs.
fn simple_row_result_types(statement: &Statement) -> Result<Option<Vec<ColumnType>>, ExecError> {
    let Statement::Query(query) = statement else {
        return Ok(None);
    };
    let SetExpr::Query(QueryBody::Select(select)) = &query.body else {
        return Ok(None);
    };
    if query.with.is_some()
        || !query.order_by.is_empty()
        || query.limit.is_some()
        || query.offset.is_some()
        || !select.from.is_empty()
    {
        return Ok(None);
    }
    let [
        SelectItem::Expr {
            expr: Expr::Row(items),
            ..
        },
    ] = select.projection.as_slice()
    else {
        return Ok(None);
    };
    items
        .iter()
        .map(|item| crate::eval::infer_type(item, &crate::scope::Scope::empty()))
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn check_packed_result_types(
    actual: &[ColumnType],
    expected: &[(String, ColumnType)],
) -> Result<(), ExecError> {
    if actual.len() != expected.len() {
        return Err(packed_result_mismatch(format!(
            "Returned row contains {} attributes, but query expects {}.",
            actual.len(),
            expected.len()
        )));
    }
    for (index, (actual, (_, expected))) in actual.iter().zip(expected).enumerate() {
        if !packed_result_type_matches(*actual, *expected) {
            return Err(packed_result_mismatch(format!(
                "Returned type {} at ordinal position {}, but query expects {}.",
                actual.name(),
                index + 1,
                expected.name()
            )));
        }
    }
    Ok(())
}

fn packed_result_type_matches(actual: ColumnType, expected: ColumnType) -> bool {
    actual == expected
        || (actual.is_string() && expected.is_string())
        || matches!(
            (actual, expected),
            (ColumnType::Numeric(_), ColumnType::Numeric(_))
        )
}

fn packed_result_mismatch(detail: String) -> ExecError {
    ExecError::Remote(
        PgError::error(
            "42P13",
            "function return row and query-specified return row do not match",
        )
        .with_detail(detail),
    )
}

/// Execute the expression and control subset of a scalar PL/pgSQL function from
/// the synchronous row evaluator. SQL-bearing bodies keep using the session
/// interpreter, because they cannot borrow an async session from scalar eval.
pub(crate) fn eval_scalar_function(
    routine: &Routine,
    values: &[Datum],
    ctx: &crate::clock::EvalCtx,
) -> Result<Datum, ExecError> {
    let block = crate::routine::parse_plpgsql_body(routine)?;
    let (frame, output_slot) = bind_scalar_parameters(routine, values, ctx, true)?;
    let mut interpreter = ScalarInterpreter {
        frames: vec![frame],
        output_slot,
        ctx,
        steps: 0,
        active_error: None,
        context: format!("PL/pgSQL function {}", routine.identity()),
    };
    match interpreter.exec_block(&block)? {
        ScalarFlow::Return(value) => scalar_function_result(routine, Some(value)),
        ScalarFlow::Next if interpreter.output_slot.is_some() => {
            scalar_function_result(routine, Some(interpreter.output_value()))
        }
        ScalarFlow::Next => scalar_function_result(routine, None),
        ScalarFlow::LoopControl { .. } => Err(ExecError::Syntax(
            "EXIT or CONTINUE cannot be used outside a loop".into(),
        )),
    }
}

/// Fold a scalar body's exit into the function's answer. `returned` is `None`
/// when control fell off the end of the body.
///
/// A `RETURNS void` function is allowed to fall off the end: PostgreSQL's
/// PL/pgSQL compiler appends the missing `RETURN` to a void body rather than
/// leaving the runtime to complain. Its answer is the void value however the
/// body left, because a bare `RETURN;` carries nothing to answer with.
fn scalar_function_result(routine: &Routine, returned: Option<Datum>) -> Result<Datum, ExecError> {
    if crate::routine::declared_returns_void(routine) {
        return Ok(crate::routine::void_result_value());
    }
    returned.ok_or_else(|| ExecError::FunctionError {
        sqlstate: "2F005",
        message: format!(
            "control reached end of function {} without RETURN",
            routine.identity()
        ),
    })
}

/// Whether a scalar body needs the owning SQL session rather than the pure
/// expression and control interpreter.
pub(crate) fn scalar_function_requires_session(
    catalog: &dyn crabka_pgkv::Kv,
    routine: &Routine,
) -> Result<bool, ExecError> {
    struct Scanner<'a> {
        catalog: &'a dyn crabka_pgkv::Kv,
        visiting: HashSet<String>,
    }

    impl Scanner<'_> {
        fn routine(&mut self, routine: &Routine) -> Result<bool, ExecError> {
            let identity = routine.identity();
            if !self.visiting.insert(identity.clone()) {
                return Ok(false);
            }
            let result = self.block(&crate::routine::parse_plpgsql_body(routine)?);
            self.visiting.remove(&identity);
            result
        }

        fn expressions<'a>(
            &mut self,
            expressions: impl IntoIterator<Item = &'a Expr>,
        ) -> Result<bool, ExecError> {
            for expression in expressions {
                if self.expression(expression)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }

        fn statements<'a>(
            &mut self,
            statements: impl IntoIterator<Item = &'a PlPgSqlStatement>,
        ) -> Result<bool, ExecError> {
            for statement in statements {
                if self.statement(statement)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }

        fn expression(&mut self, expression: &Expr) -> Result<bool, ExecError> {
            let mut direct = false;
            let mut calls = Vec::new();
            crate::grouping::visit_expr(expression, &mut |node| match node {
                Expr::ScalarSubquery(_)
                | Expr::Exists(_)
                | Expr::InSubquery { .. }
                | Expr::Quantified { .. } => direct = true,
                Expr::Func(call) => calls.push(call.clone()),
                _ => {}
            });
            if direct {
                return Ok(true);
            }
            for call in calls {
                let FuncArgs::Exprs(args) = &call.args else {
                    continue;
                };
                if !crate::routine::is_user_routine(self.catalog, &call.name) {
                    continue;
                }
                let given = crate::eval::static_arg_types(args, &crate::scope::Scope::empty())
                    .unwrap_or_else(|_| vec![crate::eval::ArgType::Opaque; args.len()]);
                match crate::routine::resolve_call(self.catalog, &call.name, &given) {
                    Ok(Some(callee)) if callee.language == "plpgsql" => {
                        if self.routine(&callee)? {
                            return Ok(true);
                        }
                    }
                    Ok(Some(_)) | Err(_) => return Ok(true),
                    Ok(None) => {}
                }
            }
            Ok(false)
        }

        fn block(&mut self, block: &PlPgSqlBlock) -> Result<bool, ExecError> {
            for declaration in &block.declarations {
                match declaration {
                    PlPgSqlDeclaration::Variable { ty, .. }
                        if ty.name.ends_with("%type") && ty.name.contains('.') =>
                    {
                        return Ok(true);
                    }
                    PlPgSqlDeclaration::Variable {
                        default: Some(default),
                        ..
                    } if self.expression(default)? => return Ok(true),
                    PlPgSqlDeclaration::Cursor { .. } => return Ok(true),
                    PlPgSqlDeclaration::Variable { .. } | PlPgSqlDeclaration::Alias { .. } => {}
                }
            }
            if self.statements(&block.statements)? {
                return Ok(true);
            }
            for handler in &block.exceptions {
                if self.statements(&handler.statements)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }

        fn statement(&mut self, statement: &PlPgSqlStatement) -> Result<bool, ExecError> {
            match statement {
                PlPgSqlStatement::Block(block) => self.block(block),
                PlPgSqlStatement::If {
                    branches,
                    else_body,
                } => {
                    for (condition, body) in branches {
                        if self.expression(condition)? || self.statements(body)? {
                            return Ok(true);
                        }
                    }
                    self.statements(else_body)
                }
                PlPgSqlStatement::Case {
                    operand,
                    arms,
                    else_body,
                } => {
                    if let Some(operand) = operand
                        && self.expression(operand)?
                    {
                        return Ok(true);
                    }
                    for (conditions, body) in arms {
                        if self.expressions(conditions)? || self.statements(body)? {
                            return Ok(true);
                        }
                    }
                    match else_body {
                        Some(body) => self.statements(body),
                        None => Ok(false),
                    }
                }
                PlPgSqlStatement::Loop { kind, body, .. } => {
                    let source = match kind.as_ref() {
                        PlPgSqlLoop::Unconditional => false,
                        PlPgSqlLoop::While(condition) => self.expression(condition)?,
                        PlPgSqlLoop::Integer {
                            lower, upper, step, ..
                        } => {
                            self.expression(lower)?
                                || self.expression(upper)?
                                || match step {
                                    Some(step) => self.expression(step)?,
                                    None => false,
                                }
                        }
                        PlPgSqlLoop::Query { .. }
                        | PlPgSqlLoop::Dynamic { .. }
                        | PlPgSqlLoop::Foreach { .. } => true,
                    };
                    if source {
                        Ok(true)
                    } else {
                        self.statements(body)
                    }
                }
                PlPgSqlStatement::Raise(raise) => Ok(raise.level != PlPgSqlRaiseLevel::Exception
                    || self.expressions(&raise.parameters)?
                    || self.expressions(raise.options.iter().map(|(_, value)| value))?),
                PlPgSqlStatement::Sql { .. }
                | PlPgSqlStatement::Perform { .. }
                | PlPgSqlStatement::Execute { .. }
                | PlPgSqlStatement::Open { .. }
                | PlPgSqlStatement::Fetch { .. }
                | PlPgSqlStatement::Close(_)
                | PlPgSqlStatement::GetDiagnostics { .. }
                | PlPgSqlStatement::Transaction { .. }
                | PlPgSqlStatement::ReturnNext(_)
                | PlPgSqlStatement::ReturnQuery(_)
                | PlPgSqlStatement::ReturnQueryExecute { .. } => Ok(true),
                PlPgSqlStatement::Assign { target, value } => {
                    Ok(self.expressions(&target.subscripts)? || self.expression(value)?)
                }
                PlPgSqlStatement::Exit { when, .. } => match when {
                    Some(when) => self.expression(when),
                    None => Ok(false),
                },
                PlPgSqlStatement::Return(value) => match value {
                    Some(value) => self.expression(value),
                    None => Ok(false),
                },
                PlPgSqlStatement::Assert { condition, message } => Ok(self
                    .expression(condition)?
                    || match message {
                        Some(message) => self.expression(message)?,
                        None => false,
                    }),
                PlPgSqlStatement::Null => Ok(false),
            }
        }
    }

    Scanner {
        catalog,
        visiting: HashSet::new(),
    }
    .routine(routine)
}

fn bind_scalar_parameters(
    routine: &Routine,
    values: &[Datum],
    ctx: &crate::clock::EvalCtx,
    include_outputs: bool,
) -> Result<(Frame, Option<String>), ExecError> {
    let inputs = routine.input_params().collect::<Vec<_>>();
    if inputs.len() != values.len() {
        return Err(ExecError::UndefinedFunction(format!(
            "function {} does not exist",
            routine.identity()
        )));
    }
    let mut frame = Frame {
        label: Some(routine.name.clone()),
        ..Frame::default()
    };
    for (index, (param, value)) in inputs.iter().zip(values).enumerate() {
        let ty = param
            .ty
            .column
            .or_else(|| value.column_type())
            .unwrap_or(ColumnType::Text);
        let value = cast_value(value, ty, ctx)?;
        let positional = format!("${}", index + 1);
        frame.slots.insert(
            positional.clone(),
            Slot {
                value,
                ty,
                record_types: None,
                constant: false,
                not_null: false,
            },
        );
        if let Some(name) = &param.name {
            frame.aliases.insert(name.clone(), positional);
        }
    }
    let mut output_slot = None;
    if include_outputs {
        for param in routine.output_params() {
            let Some(name) = &param.name else {
                continue;
            };
            output_slot = Some(name.clone());
            if param.mode == crabka_pgcatalog::routine::ParamMode::InOut {
                continue;
            }
            frame.slots.insert(
                name.clone(),
                Slot {
                    value: Datum::Null,
                    ty: param.ty.column.unwrap_or(ColumnType::Text),
                    record_types: None,
                    constant: false,
                    not_null: false,
                },
            );
        }
        if let crabka_pgcatalog::routine::RoutineResult::Table(columns) = &routine.result {
            for (name, ty) in columns {
                let ty = ty.column.unwrap_or(ColumnType::Text);
                frame.slots.insert(
                    name.clone(),
                    Slot {
                        value: Datum::Null,
                        ty,
                        record_types: None,
                        constant: false,
                        not_null: false,
                    },
                );
            }
        }
    }
    add_special_slots(&mut frame);
    Ok((frame, output_slot))
}

async fn bind_parameters(
    session: &mut SqlSession,
    routine: &Routine,
    args: &[Expr],
) -> Result<Frame, ExecError> {
    let mut frame = Frame {
        label: Some(routine.name.clone()),
        ..Frame::default()
    };
    for (index, (param, expr)) in routine.params.iter().zip(args).enumerate() {
        let ty = param.ty.column.unwrap_or(ColumnType::Text);
        let value = if param.mode.is_input() {
            session
                .plpgsql_eval_async(Expr::Cast {
                    expr: Box::new(expr.clone()),
                    ty,
                })
                .await?
                .0
        } else {
            Datum::Null
        };
        let slot = Slot {
            value,
            ty,
            record_types: None,
            constant: false,
            not_null: false,
        };
        let positional = format!("${}", index + 1);
        frame.slots.insert(positional.clone(), slot);
        if let Some(name) = &param.name {
            frame.aliases.insert(name.clone(), positional);
        }
    }
    add_special_slots(&mut frame);
    Ok(frame)
}

fn root_frame() -> Frame {
    let mut frame = Frame::default();
    add_special_slots(&mut frame);
    frame
}

fn add_special_slots(frame: &mut Frame) {
    frame.slots.insert(
        "found".into(),
        Slot {
            value: Datum::Bool(false),
            ty: ColumnType::Bool,
            record_types: None,
            constant: false,
            not_null: true,
        },
    );
    frame.slots.insert(
        "sqlstate".into(),
        Slot {
            value: Datum::Text("00000".into()),
            ty: ColumnType::Text,
            record_types: None,
            constant: false,
            not_null: true,
        },
    );
    frame.slots.insert(
        "sqlerrm".into(),
        Slot {
            value: Datum::Text(String::new()),
            ty: ColumnType::Text,
            record_types: None,
            constant: false,
            not_null: true,
        },
    );
}

async fn execute_invocation(
    session: &mut SqlSession,
    block: PlPgSqlBlock,
    root: Frame,
    tag: &str,
) -> Result<QueryResult, ExecError> {
    let owns_transaction = session.plpgsql_is_idle();
    if owns_transaction {
        session.plpgsql_begin_implicit_transaction().await?;
    }
    let result = {
        let mut interpreter = Interpreter {
            session,
            frames: vec![root],
            allow_transaction_control: owns_transaction,
            savepoint_serial: 0,
            active_error: None,
            exception_depth: 0,
            cursor_declarations: HashMap::new(),
            last_row_count: 0,
            output_slot: None,
            set_results: None,
            returns_set: false,
            context: format!("PL/pgSQL {tag}"),
            variable_conflict: block.variable_conflict,
            routine_oid: 0,
        };
        interpreter.exec_block(&block).await
    };
    let result = if owns_transaction {
        session.plpgsql_finish_implicit_transaction(result).await
    } else {
        result
    };
    result.map(|_| QueryResult::Command { tag: tag.into() })
}

async fn execute_procedure_invocation(
    session: &mut SqlSession,
    routine: &Routine,
    block: PlPgSqlBlock,
    root: Frame,
    allow_transaction_control: bool,
) -> Result<Option<ProcedureCallOutput>, ExecError> {
    let mut interpreter = Interpreter {
        session,
        frames: vec![root],
        allow_transaction_control: allow_transaction_control
            && !routine.security_definer
            && routine.config.is_empty(),
        savepoint_serial: 0,
        active_error: None,
        exception_depth: 0,
        cursor_declarations: HashMap::new(),
        last_row_count: 0,
        output_slot: None,
        set_results: None,
        returns_set: false,
        context: format!("PL/pgSQL procedure {}", routine.identity()),
        variable_conflict: block.variable_conflict,
        routine_oid: routine.oid,
    };
    interpreter.exec_block(&block).await?;
    Ok(procedure_output(routine, &interpreter))
}

fn procedure_output(
    routine: &Routine,
    interpreter: &Interpreter<'_>,
) -> Option<ProcedureCallOutput> {
    let outputs = routine
        .params
        .iter()
        .enumerate()
        .filter(|(_, param)| param.mode.is_output())
        .collect::<Vec<_>>();
    if outputs.is_empty() {
        return None;
    }
    let fields = outputs
        .iter()
        .enumerate()
        .map(|(output_index, (_, param))| {
            let name = param
                .name
                .clone()
                .unwrap_or_else(|| format!("column{}", output_index + 1));
            crate::exec::field(&name, param.ty.column.unwrap_or(ColumnType::Text))
        })
        .collect();
    let values = outputs
        .iter()
        .map(|(index, _)| {
            interpreter
                .lookup_slot(&format!("${}", index + 1))
                .map_or(Datum::Null, |slot| slot.value.clone())
        })
        .collect();
    Some(ProcedureCallOutput { fields, values })
}

impl ScalarInterpreter<'_> {
    fn exec_block(&mut self, block: &PlPgSqlBlock) -> Result<ScalarFlow, ExecError> {
        self.frames.push(Frame {
            label: block.label.clone(),
            ..Frame::default()
        });
        for declaration in &block.declarations {
            self.declare(declaration)?;
        }
        let result = self.exec_statements(&block.statements);
        let result = match result {
            Ok(flow) => Ok(flow),
            Err(error) if !block.exceptions.is_empty() => {
                let pg = ensure_error_context(error.clone().into_pg(), &self.context);
                self.set_special_error(&pg);
                let handler = block.exceptions.iter().find(|handler| {
                    handler.conditions.iter().any(|condition| {
                        crate::plpgsql_sqlstate::condition_matches(condition, &pg.code)
                    })
                });
                if let Some(handler) = handler {
                    let previous = self.active_error.replace(pg);
                    let outcome = self.exec_statements(&handler.statements);
                    self.active_error = previous;
                    outcome
                } else {
                    Err(error)
                }
            }
            Err(error) => Err(error),
        };
        self.frames.pop();
        match result? {
            ScalarFlow::LoopControl {
                continuing: false,
                label: Some(label),
            } if block.label.as_deref() == Some(label.as_str()) => Ok(ScalarFlow::Next),
            flow => Ok(flow),
        }
    }

    fn exec_statements(
        &mut self,
        statements: &[PlPgSqlStatement],
    ) -> Result<ScalarFlow, ExecError> {
        for statement in statements {
            self.steps = self.steps.saturating_add(1);
            if self.steps > MAX_SCALAR_STEPS {
                return Err(ExecError::StackDepthExceeded);
            }
            let flow = self.exec_statement(statement)?;
            if !matches!(flow, ScalarFlow::Next) {
                return Ok(flow);
            }
        }
        Ok(ScalarFlow::Next)
    }

    fn exec_statement(&mut self, statement: &PlPgSqlStatement) -> Result<ScalarFlow, ExecError> {
        match statement {
            PlPgSqlStatement::Block(block) => self.exec_block(block),
            PlPgSqlStatement::Assign { target, value } => {
                let value = self.eval(value)?;
                self.assign_target(target, value)?;
                Ok(ScalarFlow::Next)
            }
            PlPgSqlStatement::If {
                branches,
                else_body,
            } => {
                for (condition, body) in branches {
                    if self.truth(condition)? {
                        return self.exec_statements(body);
                    }
                }
                self.exec_statements(else_body)
            }
            PlPgSqlStatement::Case {
                operand,
                arms,
                else_body,
            } => {
                let operand = operand.as_ref().map(|expr| self.eval(expr)).transpose()?;
                for (conditions, body) in arms {
                    for condition in conditions {
                        let matched = if let Some(value) = &operand {
                            let right = self.eval(condition)?;
                            crabka_pgtypes::ops::compare(value, &right)?.is_some_and(|ordering| {
                                ordering == std::cmp::Ordering::Equal
                            })
                        } else {
                            self.truth(condition)?
                        };
                        if matched {
                            return self.exec_statements(body);
                        }
                    }
                }
                if let Some(body) = else_body {
                    self.exec_statements(body)
                } else {
                    Err(ExecError::FunctionError {
                        sqlstate: "20000",
                        message: "case not found".into(),
                    })
                }
            }
            PlPgSqlStatement::Loop {
                label, kind, body, ..
            } => self.exec_loop(label.as_deref(), kind, body),
            PlPgSqlStatement::Exit {
                continuing,
                label,
                when,
            } => {
                if when.as_ref().map(|expr| self.truth(expr)).transpose()?.unwrap_or(true) {
                    Ok(ScalarFlow::LoopControl {
                        continuing: *continuing,
                        label: label.clone(),
                    })
                } else {
                    Ok(ScalarFlow::Next)
                }
            }
            PlPgSqlStatement::Return(value) => {
                let value = value.as_ref().map(|expr| self.eval(expr)).transpose()?;
                Ok(ScalarFlow::Return(match value {
                    Some(value) => value,
                    None => self.output_value(),
                }))
            }
            PlPgSqlStatement::Raise(raise) => self.raise(raise),
            PlPgSqlStatement::Assert { condition, message } => {
                if self.truth(condition)? {
                    Ok(ScalarFlow::Next)
                } else {
                    let message = message
                        .as_ref()
                        .map(|expr| self.eval(expr))
                        .transpose()?
                        .filter(|value| !value.is_null())
                        .map_or_else(
                            || DEFAULT_ASSERT_MESSAGE.to_string(),
                            |value| {
                                String::from_utf8_lossy(
                                    &crabka_pgtypes::encoding::encode_text(
                                        &value,
                                        &self.ctx.time_zone,
                                    ),
                                )
                                .into_owned()
                            },
                        );
                    Err(ExecError::FunctionError {
                        sqlstate: "P0004",
                        message,
                    })
                }
            }
            PlPgSqlStatement::Null => Ok(ScalarFlow::Next),
            PlPgSqlStatement::Sql { .. }
            | PlPgSqlStatement::Perform { .. }
            | PlPgSqlStatement::Execute { .. }
            | PlPgSqlStatement::Open { .. }
            | PlPgSqlStatement::Fetch { .. }
            | PlPgSqlStatement::Close(_)
            | PlPgSqlStatement::GetDiagnostics { .. }
            | PlPgSqlStatement::Transaction { .. }
            | PlPgSqlStatement::ReturnNext(_)
            | PlPgSqlStatement::ReturnQuery(_)
            | PlPgSqlStatement::ReturnQueryExecute { .. } => Err(ExecError::Unsupported(
                "SQL-bearing or set-returning PL/pgSQL functions require the async session executor"
                    .into(),
            )),
        }
    }

    fn exec_loop(
        &mut self,
        label: Option<&str>,
        kind: &PlPgSqlLoop,
        body: &[PlPgSqlStatement],
    ) -> Result<ScalarFlow, ExecError> {
        match kind {
            PlPgSqlLoop::Unconditional => loop {
                if let Some(flow) = self.loop_iteration(label, body)? {
                    return Ok(flow);
                }
            },
            PlPgSqlLoop::While(condition) => {
                while self.truth(condition)? {
                    if let Some(flow) = self.loop_iteration(label, body)? {
                        return Ok(flow);
                    }
                }
                Ok(ScalarFlow::Next)
            }
            PlPgSqlLoop::Integer {
                variable,
                reverse,
                lower,
                upper,
                step,
            } => {
                let lower = self.integer(lower)?;
                let upper = self.integer(upper)?;
                let step = step
                    .as_ref()
                    .map(|expr| self.integer(expr))
                    .transpose()?
                    .unwrap_or(1);
                if step <= 0 {
                    return Err(ExecError::FunctionError {
                        sqlstate: "22023",
                        message: "BY value of FOR loop must be greater than zero".into(),
                    });
                }
                self.frames.push(Frame::default());
                self.frames.last_mut().expect("loop frame").slots.insert(
                    variable.clone(),
                    Slot {
                        value: Datum::Int4(lower),
                        ty: ColumnType::Int4,
                        record_types: None,
                        constant: false,
                        not_null: true,
                    },
                );
                let mut current = lower;
                while if *reverse {
                    current >= upper
                } else {
                    current <= upper
                } {
                    self.assign_name(variable, Datum::Int4(current))?;
                    if let Some(flow) = self.loop_iteration(label, body)? {
                        self.frames.pop();
                        return Ok(flow);
                    }
                    if current == upper {
                        break;
                    }
                    let Some(next) = (if *reverse {
                        current.checked_sub(step)
                    } else {
                        current.checked_add(step)
                    }) else {
                        break;
                    };
                    current = next;
                }
                self.frames.pop();
                Ok(ScalarFlow::Next)
            }
            PlPgSqlLoop::Query { .. }
            | PlPgSqlLoop::Dynamic { .. }
            | PlPgSqlLoop::Foreach { .. } => Err(ExecError::Unsupported(
                "SQL or array PL/pgSQL loops require the async session executor".into(),
            )),
        }
    }

    fn loop_iteration(
        &mut self,
        loop_label: Option<&str>,
        body: &[PlPgSqlStatement],
    ) -> Result<Option<ScalarFlow>, ExecError> {
        match self.exec_statements(body)? {
            ScalarFlow::Next => Ok(None),
            ScalarFlow::Return(value) => Ok(Some(ScalarFlow::Return(value))),
            ScalarFlow::LoopControl { continuing, label }
                if label.is_none() || label.as_deref() == loop_label =>
            {
                if continuing {
                    Ok(None)
                } else {
                    Ok(Some(ScalarFlow::Next))
                }
            }
            flow => Ok(Some(flow)),
        }
    }

    fn declare(&mut self, declaration: &PlPgSqlDeclaration) -> Result<(), ExecError> {
        match declaration {
            PlPgSqlDeclaration::Variable {
                name,
                ty,
                constant,
                not_null,
                default,
            } => {
                let ty =
                    declaration_type(ty, |name| Ok(self.lookup_slot(name).map(|slot| slot.ty)))?;
                let value = default
                    .as_ref()
                    .map(|expr| self.eval(expr))
                    .transpose()?
                    .unwrap_or(Datum::Null);
                let value = cast_value(&value, ty, self.ctx)?;
                if *not_null && value.is_null() {
                    return Err(ExecError::FunctionError {
                        sqlstate: "23502",
                        message: format!(
                            "variable \"{name}\" declared NOT NULL cannot default to NULL"
                        ),
                    });
                }
                self.frames.last_mut().expect("block frame").slots.insert(
                    name.clone(),
                    Slot {
                        value,
                        ty,
                        record_types: None,
                        constant: *constant,
                        not_null: *not_null,
                    },
                );
                Ok(())
            }
            PlPgSqlDeclaration::Alias { name, target } => {
                if self.lookup_slot(target).is_none() {
                    return Err(ExecError::Syntax(format!(
                        "alias target \"{target}\" does not exist"
                    )));
                }
                self.frames
                    .last_mut()
                    .expect("block frame")
                    .aliases
                    .insert(name.clone(), target.clone());
                Ok(())
            }
            PlPgSqlDeclaration::Cursor { .. } => Err(ExecError::Unsupported(
                "cursors require the async session executor".into(),
            )),
        }
    }

    fn eval(&self, expr: &Expr) -> Result<Datum, ExecError> {
        crate::eval::eval(
            &rewrite_expr_with(
                expr,
                &|table, name| {
                    if let Some(label) = table
                        && let Some(slot) = labeled_slot(&self.frames, label, name)
                    {
                        return Ok(Some(SqlBinder::slot_expr(slot)));
                    }
                    match table {
                        None => Ok(self.lookup_slot(name).map(SqlBinder::slot_expr)),
                        Some(record) => rewrite_record_field(
                            record,
                            name,
                            self.lookup_slot(record),
                            self.lookup_slot("tg_relid"),
                        ),
                    }
                },
                &|_| {
                    Err(ExecError::Unsupported(
                        "subqueries require the async session executor".into(),
                    ))
                },
            )?,
            &crate::scope::Scope::empty(),
            &[],
            self.ctx,
        )
    }

    fn truth(&self, expr: &Expr) -> Result<bool, ExecError> {
        match self.eval(expr)? {
            Datum::Bool(value) => Ok(value),
            Datum::Null => Ok(false),
            _ => Err(ExecError::TypeMismatch(
                "condition must be type boolean".into(),
            )),
        }
    }

    fn integer(&self, expr: &Expr) -> Result<i32, ExecError> {
        let value = self.eval(expr)?;
        let Datum::Int4(value) = cast_value(&value, ColumnType::Int4, self.ctx)? else {
            unreachable!("int4 cast returned another datum type");
        };
        Ok(value)
    }

    fn lookup_slot(&self, name: &str) -> Option<&Slot> {
        let mut current = name;
        for frame in self.frames.iter().rev() {
            if let Some(target) = frame.aliases.get(current) {
                current = target;
            }
            if let Some(slot) = frame.slots.get(current) {
                return Some(slot);
            }
        }
        None
    }

    fn resolve_alias(&self, name: &str) -> String {
        let mut current = name.to_string();
        for frame in self.frames.iter().rev() {
            if let Some(target) = frame.aliases.get(&current) {
                current = target.clone();
            }
        }
        current
    }

    fn assign_target(&mut self, target: &PlPgSqlTarget, value: Datum) -> Result<(), ExecError> {
        if target.subscripts.is_empty() && target.path.len() == 1 {
            return self.assign_name(&target.path[0], value);
        }
        let subscripts = target
            .subscripts
            .iter()
            .map(|expr| self.eval(expr))
            .collect::<Result<Vec<_>, _>>()?;
        if target.path.len() == 1 && !subscripts.is_empty() {
            let name = &target.path[0];
            let slot = self
                .lookup_slot(name)
                .cloned()
                .ok_or_else(|| ExecError::Syntax(format!("\"{name}\" is not a known variable")))?;
            let assigned =
                assign_subscripted(&slot.value, Some(slot.ty), &subscripts, &value, self.ctx)?;
            return self.assign_name(name, assigned);
        }
        if target.path.len() == 2 {
            let name = self.resolve_alias(&target.path[0]);
            for frame in self.frames.iter_mut().rev() {
                let Some(slot) = frame.slots.get_mut(&name) else {
                    continue;
                };
                let Datum::Record(record) = &mut slot.value else {
                    return Err(ExecError::ObjectNotInPrerequisiteState(format!(
                        "record \"{name}\" is not assigned yet"
                    )));
                };
                let Some(index) = record
                    .names
                    .iter()
                    .position(|field| field == &target.path[1])
                else {
                    return Err(ExecError::UndefinedColumn(target.path[1].clone()));
                };
                let field_type = slot
                    .record_types
                    .as_deref()
                    .and_then(|types| types.get(index))
                    .copied()
                    .or_else(|| record.values[index].column_type());
                record.values[index] = if subscripts.is_empty() {
                    match field_type {
                        Some(ty) => cast_value(&value, ty, self.ctx)?,
                        None => value,
                    }
                } else {
                    assign_subscripted(
                        &record.values[index],
                        field_type,
                        &subscripts,
                        &value,
                        self.ctx,
                    )?
                };
                return Ok(());
            }
        }
        Err(ExecError::Syntax(
            "invalid PL/pgSQL assignment target".into(),
        ))
    }

    fn assign_name(&mut self, name: &str, value: Datum) -> Result<(), ExecError> {
        let name = self.resolve_alias(name);
        for frame in self.frames.iter_mut().rev() {
            if let Some(slot) = frame.slots.get_mut(&name) {
                if slot.constant {
                    return Err(ExecError::FunctionError {
                        sqlstate: "22005",
                        message: format!("variable \"{name}\" is declared CONSTANT"),
                    });
                }
                let value = cast_value(&value, slot.ty, self.ctx)?;
                if slot.not_null && value.is_null() {
                    return Err(ExecError::FunctionError {
                        sqlstate: "23502",
                        message: format!(
                            "null value cannot be assigned to variable \"{name}\" declared NOT NULL"
                        ),
                    });
                }
                slot.record_types = inferred_record_types(&value);
                slot.value = value;
                return Ok(());
            }
        }
        Err(ExecError::Syntax(format!(
            "\"{name}\" is not a known variable"
        )))
    }

    fn output_value(&self) -> Datum {
        self.output_slot
            .as_deref()
            .and_then(|name| self.lookup_slot(name))
            .map_or(Datum::Null, |slot| slot.value.clone())
    }

    fn set_special_error(&mut self, error: &PgError) {
        let _ = self.assign_name("sqlstate", Datum::Text(error.code.clone()));
        let _ = self.assign_name("sqlerrm", Datum::Text(error.message.clone()));
    }

    fn raise(&mut self, raise: &PlPgSqlRaise) -> Result<ScalarFlow, ExecError> {
        if raise.message.is_none() && raise.condition.is_none() && raise.options.is_empty() {
            let error = self
                .active_error
                .clone()
                .ok_or_else(|| ExecError::FunctionError {
                    sqlstate: "0Z002",
                    message: "RAISE without parameters cannot be used outside an exception handler"
                        .into(),
                })?;
            return Err(ExecError::Remote(error));
        }
        if raise.level != PlPgSqlRaiseLevel::Exception {
            return Err(ExecError::Unsupported(
                "non-error RAISE from a scalar function requires the async session executor".into(),
            ));
        }
        let values = raise
            .parameters
            .iter()
            .map(|expr| {
                self.eval(expr).map(|value| {
                    if value.is_null() {
                        return NULL_RAISE_PARAMETER.to_string();
                    }
                    String::from_utf8_lossy(&crabka_pgtypes::encoding::encode_text(
                        &value,
                        &self.ctx.time_zone,
                    ))
                    .into_owned()
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let options = raise
            .options
            .iter()
            .map(|(name, expr)| {
                let value = self.eval(expr)?;
                reject_null_raise_option(&value)?;
                Ok((
                    name.as_str(),
                    String::from_utf8_lossy(&crabka_pgtypes::encoding::encode_text(
                        &value,
                        &self.ctx.time_zone,
                    ))
                    .into_owned(),
                ))
            })
            .collect::<Result<Vec<_>, ExecError>>()?;
        let diagnostic =
            build_raise_diagnostic(raise, &values, options)?.with_context(self.context.clone());
        Err(ExecError::Remote(diagnostic))
    }
}

fn labeled_slot<'a>(frames: &'a [Frame], label: &str, name: &str) -> Option<&'a Slot> {
    frames
        .iter()
        .rev()
        .find(|frame| frame.label.as_deref() == Some(label))
        .and_then(|frame| {
            let target = frame.aliases.get(name).map_or(name, String::as_str);
            frame.slots.get(target)
        })
}

impl Interpreter<'_> {
    fn exec_block<'a>(
        &'a mut self,
        block: &'a PlPgSqlBlock,
    ) -> BoxFuture<'a, Result<Flow, ExecError>> {
        Box::pin(async move {
            self.frames.push(Frame {
                label: block.label.clone(),
                ..Frame::default()
            });
            let exception_depth = self.exception_depth;
            let cursor_declarations = self.cursor_declarations.clone();
            let result = self.exec_block_inner(block).await;
            self.exception_depth = exception_depth;
            self.cursor_declarations = cursor_declarations;
            self.frames.pop();
            result
        })
    }

    fn exec_block_inner<'a>(
        &'a mut self,
        block: &'a PlPgSqlBlock,
    ) -> BoxFuture<'a, Result<Flow, ExecError>> {
        Box::pin(async move {
            for declaration in &block.declarations {
                self.declare(declaration).await?;
            }
            let savepoint = if block.exceptions.is_empty() {
                None
            } else {
                self.savepoint_serial += 1;
                let name = format!("__plpgsql_{}", self.savepoint_serial);
                self.session.savepoint(&name)?;
                Some(name)
            };
            if savepoint.is_some() {
                self.exception_depth += 1;
            }
            let result = self.exec_statements(&block.statements).await;
            let result = match (result, savepoint) {
                (Ok(flow), Some(name)) => {
                    self.session.release_savepoint(&name)?;
                    Ok(flow)
                }
                (Err(error), Some(name)) => {
                    let pg = ensure_error_context(error.clone().into_pg(), &self.context);
                    self.session.rollback_to_savepoint(&name).await?;
                    self.session.release_savepoint(&name)?;
                    let handler = block.exceptions.iter().find(|handler| {
                        handler.conditions.iter().any(|condition| {
                            crate::plpgsql_sqlstate::condition_matches(condition, &pg.code)
                        })
                    });
                    if let Some(handler) = handler {
                        let previous_state =
                            self.lookup_slot("sqlstate").map(|slot| slot.value.clone());
                        let previous_message =
                            self.lookup_slot("sqlerrm").map(|slot| slot.value.clone());
                        self.set_special_error(&pg);
                        let previous_error = self.active_error.replace(pg);
                        let outcome = self.exec_statements(&handler.statements).await;
                        self.active_error = previous_error;
                        if let Some(value) = previous_state {
                            let _ = self.assign_name("sqlstate", value);
                        }
                        if let Some(value) = previous_message {
                            let _ = self.assign_name("sqlerrm", value);
                        }
                        outcome
                    } else {
                        Err(error)
                    }
                }
                (result, None) => result,
            };
            if !block.exceptions.is_empty() {
                self.exception_depth -= 1;
            }
            match result? {
                Flow::LoopControl {
                    continuing: false,
                    label: Some(label),
                } if block.label.as_deref() == Some(label.as_str()) => Ok(Flow::Next),
                other => Ok(other),
            }
        })
    }

    fn exec_statements<'a>(
        &'a mut self,
        statements: &'a [PlPgSqlStatement],
    ) -> BoxFuture<'a, Result<Flow, ExecError>> {
        Box::pin(async move {
            for statement in statements {
                let flow = self.exec_statement(statement).await?;
                if !matches!(flow, Flow::Next) {
                    return Ok(flow);
                }
            }
            Ok(Flow::Next)
        })
    }

    fn exec_statement<'a>(
        &'a mut self,
        statement: &'a PlPgSqlStatement,
    ) -> BoxFuture<'a, Result<Flow, ExecError>> {
        Box::pin(async move {
            match statement {
                PlPgSqlStatement::Block(block) => self.exec_block(block).await,
                PlPgSqlStatement::Assign { target, value } => {
                    let (value, _) = self.eval_async(value).await?;
                    self.assign_target(target, value).await?;
                    Ok(Flow::Next)
                }
                PlPgSqlStatement::Sql {
                    statement, into, ..
                } => {
                    if let Statement::Call {
                        name,
                        args,
                        named_args,
                        variadic,
                    } = statement.as_ref()
                    {
                        self.execute_nested_call(name, args, named_args, variadic.as_deref())
                            .await?;
                    } else {
                        let bound = self.bind_statement(statement)?;
                        let dml_returning = statement_has_dml_returning(&bound);
                        let result = Box::pin(self.session.run_one(&bound)).await?;
                        self.consume_sql_result(result, into.as_ref(), true, false, dml_returning)
                            .await?;
                    }
                    Ok(Flow::Next)
                }
                PlPgSqlStatement::Perform {
                    query,
                    source,
                    line,
                } => {
                    let context = format!(
                        "SQL statement \"{source}\"\n{} line {line} at PERFORM",
                        self.context
                    );
                    let bound = self.bind_statement(query).map_err(|error| {
                        ExecError::Remote(ensure_error_context(error.into_pg(), &context))
                    })?;
                    let result = Box::pin(self.session.run_one(&bound))
                        .await
                        .map_err(|error| {
                            ExecError::Remote(ensure_error_context(error.into_pg(), &context))
                        })?;
                    self.last_row_count = result_row_count(&result);
                    self.set_found(self.last_row_count > 0);
                    Ok(Flow::Next)
                }
                PlPgSqlStatement::If {
                    branches,
                    else_body,
                } => {
                    for (condition, body) in branches {
                        if self.truth_async(condition).await? {
                            return self.exec_statements(body).await;
                        }
                    }
                    self.exec_statements(else_body).await
                }
                PlPgSqlStatement::Case {
                    operand,
                    arms,
                    else_body,
                } => {
                    let operand = match operand {
                        Some(expr) => Some(self.eval_async(expr).await?),
                        None => None,
                    };
                    for (conditions, body) in arms {
                        for condition in conditions {
                            let matched = if let Some((value, ty)) = &operand {
                                let (right, right_ty) = self.eval_async(condition).await?;
                                let expr = Expr::Binary {
                                    op: BinaryOp::Eq,
                                    left: Box::new(Expr::Const {
                                        value: value.clone(),
                                        ty: *ty,
                                    }),
                                    right: Box::new(Expr::Const {
                                        value: right,
                                        ty: right_ty,
                                    }),
                                };
                                matches!(self.session.plpgsql_eval(&expr)?.0, Datum::Bool(true))
                            } else {
                                self.truth_async(condition).await?
                            };
                            if matched {
                                return self.exec_statements(body).await;
                            }
                        }
                    }
                    if let Some(body) = else_body {
                        self.exec_statements(body).await
                    } else {
                        Err(ExecError::FunctionError {
                            sqlstate: "20000",
                            message: "case not found".into(),
                        })
                    }
                }
                PlPgSqlStatement::Loop {
                    label, kind, body, ..
                } => self.exec_loop(label.as_deref(), kind, body).await,
                PlPgSqlStatement::Exit {
                    continuing,
                    label,
                    when,
                } => {
                    let applies = match when {
                        Some(condition) => self.truth_async(condition).await?,
                        None => true,
                    };
                    if applies {
                        Ok(Flow::LoopControl {
                            continuing: *continuing,
                            label: label.clone(),
                        })
                    } else {
                        Ok(Flow::Next)
                    }
                }
                PlPgSqlStatement::Return(value) => {
                    if self.returns_set && value.is_some() {
                        return Err(ExecError::Syntax(
                            "RETURN cannot have a parameter in a set-returning function".into(),
                        ));
                    }
                    let value = match value {
                        Some(expr) => self.eval_async(expr).await?.0,
                        None => self.output_value(),
                    };
                    Ok(Flow::Return(value))
                }
                PlPgSqlStatement::Raise(raise) => self.raise(raise).await,
                PlPgSqlStatement::Assert { condition, message } => {
                    if self.truth_async(condition).await? {
                        Ok(Flow::Next)
                    } else {
                        let message = match message {
                            Some(expr) => match self.eval_async(expr).await?.0 {
                                Datum::Null => DEFAULT_ASSERT_MESSAGE.to_string(),
                                value => self.session.plpgsql_render(&value),
                            },
                            None => DEFAULT_ASSERT_MESSAGE.to_string(),
                        };
                        Err(ExecError::FunctionError {
                            sqlstate: "P0004",
                            message,
                        })
                    }
                }
                PlPgSqlStatement::Transaction { commit, chain } => {
                    if !self.allow_transaction_control || self.exception_depth > 0 {
                        return Err(ExecError::ActiveSqlTransaction(
                            "invalid transaction termination".into(),
                        ));
                    }
                    if *commit {
                        self.session.commit_cmd(*chain).await?;
                    } else {
                        self.session.rollback_cmd(*chain).await?;
                    }
                    if !*chain {
                        self.session.begin(None, None).await?;
                    }
                    Ok(Flow::Next)
                }
                PlPgSqlStatement::Null => Ok(Flow::Next),
                PlPgSqlStatement::ReturnNext(value) => {
                    if !self.returns_set {
                        return Err(ExecError::Syntax(
                            "RETURN NEXT cannot be used in a non-SETOF function".into(),
                        ));
                    }
                    match value {
                        Some(expr) => {
                            let value = self.eval_async(expr).await?.0;
                            self.push_set_value(value)?;
                        }
                        None => self.push_current_output_row()?,
                    }
                    Ok(Flow::Next)
                }
                PlPgSqlStatement::ReturnQuery(query) => {
                    if !self.returns_set {
                        return Err(ExecError::Syntax(
                            "RETURN QUERY cannot be used in a non-SETOF function".into(),
                        ));
                    }
                    let statement = self.bind_statement(query)?;
                    let result = Box::pin(self.session.run_one(&statement)).await?;
                    let count = self.push_query_result(result)?;
                    self.set_found(count > 0);
                    Ok(Flow::Next)
                }
                PlPgSqlStatement::ReturnQueryExecute { query, using } => {
                    if !self.returns_set {
                        return Err(ExecError::Syntax(
                            "RETURN QUERY EXECUTE cannot be used in a non-SETOF function".into(),
                        ));
                    }
                    let statement = self.dynamic_statement(query, using).await?;
                    let result = Box::pin(self.session.run_one(&statement)).await?;
                    let count = self.push_query_result(result)?;
                    self.set_found(count > 0);
                    Ok(Flow::Next)
                }
                PlPgSqlStatement::Execute { query, into, using } => {
                    let statement = self.dynamic_statement(query, using).await?;
                    let dml_returning = statement_has_dml_returning(&statement);
                    let result = Box::pin(self.session.run_one(&statement)).await?;
                    self.consume_sql_result(result, into.as_ref(), false, true, dml_returning)
                        .await?;
                    Ok(Flow::Next)
                }
                PlPgSqlStatement::Open {
                    cursor,
                    scroll,
                    arguments,
                    query,
                    dynamic_query,
                    using,
                } => {
                    let (declared_scroll, statement) = if let Some(dynamic_query) = dynamic_query {
                        if !arguments.is_empty() {
                            return Err(ExecError::Syntax(
                                "arguments cannot be used with OPEN FOR EXECUTE".into(),
                            ));
                        }
                        (*scroll, self.dynamic_statement(dynamic_query, using).await?)
                    } else if let Some(query) = query {
                        if !arguments.is_empty() || !using.is_empty() {
                            return Err(ExecError::Syntax(
                                "arguments cannot be used with OPEN FOR query".into(),
                            ));
                        }
                        (*scroll, self.bind_statement(query)?)
                    } else {
                        if !using.is_empty() {
                            return Err(ExecError::Syntax(
                                "USING requires OPEN FOR EXECUTE".into(),
                            ));
                        }
                        let declaration = self
                            .cursor_declarations
                            .get(cursor)
                            .cloned()
                            .ok_or_else(|| ExecError::UndefinedCursor(cursor.clone()))?;
                        if arguments.len() != declaration.arguments.len() {
                            return Err(ExecError::Syntax(format!(
                                "cursor \"{cursor}\" has {} arguments, but {} were supplied",
                                declaration.arguments.len(),
                                arguments.len()
                            )));
                        }
                        let mut frame = Frame::default();
                        for ((name, ty), expression) in declaration.arguments.iter().zip(arguments)
                        {
                            let ty = ty.resolved.unwrap_or(ColumnType::Text);
                            let value = self.eval_async(expression).await?.0;
                            let ctx = self.session.plpgsql_eval_context();
                            let value = cast_value(&value, ty, &ctx)?;
                            frame.slots.insert(
                                name.clone(),
                                Slot {
                                    value,
                                    ty,
                                    record_types: None,
                                    constant: true,
                                    not_null: false,
                                },
                            );
                        }
                        self.frames.push(frame);
                        let statement = self.bind_statement(&declaration.statement);
                        self.frames.pop();
                        (declaration.scroll, statement?)
                    };
                    let Statement::Query(query) = statement else {
                        return Err(ExecError::Syntax("cursor query must be a SELECT".into()));
                    };
                    self.session
                        .declare_cursor(
                            cursor,
                            false,
                            scroll.or(declared_scroll),
                            false,
                            "<plpgsql cursor query>",
                            &query,
                        )
                        .await?;
                    Ok(Flow::Next)
                }
                PlPgSqlStatement::Fetch {
                    cursor,
                    direction,
                    into,
                    move_only,
                } => {
                    let direction = parse_cursor_direction(direction)?;
                    let result = self
                        .session
                        .fetch_cursor(cursor, direction, *move_only)
                        .await?;
                    self.consume_sql_result(result, into.as_ref(), true, false, false)
                        .await?;
                    Ok(Flow::Next)
                }
                PlPgSqlStatement::Close(cursor) => {
                    self.session
                        .close_cursor(&CursorTarget::Name(cursor.clone()))?;
                    Ok(Flow::Next)
                }
                PlPgSqlStatement::GetDiagnostics { stacked, items } => {
                    if *stacked && self.active_error.is_none() {
                        return Err(ExecError::FunctionError {
                            sqlstate: "0Z002",
                            message: "GET STACKED DIAGNOSTICS cannot be used outside an exception handler"
                                .into(),
                        });
                    }
                    for (target, item) in items {
                        let error = self.active_error.as_ref();
                        let fields = error.and_then(|error| error.diagnostics.as_deref());
                        let value = match item.as_str() {
                            "row_count" if !stacked => Datum::Int8(self.last_row_count as i64),
                            "pg_context" if !stacked => Datum::Text(self.context.clone()),
                            "pg_routine_oid" if !stacked => {
                                Datum::Int4(i32::try_from(self.routine_oid).map_err(|_| {
                                    ExecError::Unsupported(
                                        "PL/pgSQL routine oid exceeds int4 range".into(),
                                    )
                                })?)
                            }
                            "returned_sqlstate" if *stacked => Datum::Text(
                                error.map_or_else(|| "00000".into(), |error| error.code.clone()),
                            ),
                            "message_text" if *stacked => Datum::Text(
                                error.map_or_else(String::new, |error| error.message.clone()),
                            ),
                            "column_name" if *stacked => Datum::Text(
                                fields
                                    .and_then(|fields| fields.column.clone())
                                    .unwrap_or_default(),
                            ),
                            "constraint_name" if *stacked => Datum::Text(
                                fields
                                    .and_then(|fields| fields.constraint.clone())
                                    .unwrap_or_default(),
                            ),
                            "pg_datatype_name" if *stacked => Datum::Text(
                                fields
                                    .and_then(|fields| fields.datatype.clone())
                                    .unwrap_or_default(),
                            ),
                            "table_name" if *stacked => Datum::Text(
                                fields
                                    .and_then(|fields| fields.table.clone())
                                    .unwrap_or_default(),
                            ),
                            "schema_name" if *stacked => Datum::Text(
                                fields
                                    .and_then(|fields| fields.schema.clone())
                                    .unwrap_or_default(),
                            ),
                            "pg_exception_detail" if *stacked => Datum::Text(
                                fields
                                    .and_then(|fields| fields.detail.clone())
                                    .unwrap_or_default(),
                            ),
                            "pg_exception_hint" if *stacked => Datum::Text(
                                fields
                                    .and_then(|fields| fields.hint.clone())
                                    .unwrap_or_default(),
                            ),
                            "pg_exception_context" if *stacked => Datum::Text(
                                fields
                                    .and_then(|fields| fields.context.clone())
                                    .unwrap_or_default(),
                            ),
                            _ => {
                                return Err(ExecError::Unsupported(format!(
                                    "PL/pgSQL diagnostic item {item} is not supported"
                                )));
                            }
                        };
                        self.assign_target(target, value).await?;
                    }
                    Ok(Flow::Next)
                }
            }
        })
    }

    fn exec_loop<'a>(
        &'a mut self,
        label: Option<&'a str>,
        kind: &'a PlPgSqlLoop,
        body: &'a [PlPgSqlStatement],
    ) -> BoxFuture<'a, Result<Flow, ExecError>> {
        Box::pin(async move {
            match kind {
                PlPgSqlLoop::Unconditional => loop {
                    if let Some(flow) = self.loop_iteration(label, body).await? {
                        return Ok(flow);
                    }
                },
                PlPgSqlLoop::While(condition) => {
                    while self.truth_async(condition).await? {
                        if let Some(flow) = self.loop_iteration(label, body).await? {
                            return Ok(flow);
                        }
                    }
                    Ok(Flow::Next)
                }
                PlPgSqlLoop::Integer {
                    variable,
                    reverse,
                    lower,
                    upper,
                    step,
                } => {
                    let lower = self.integer_async(lower).await?;
                    let upper = self.integer_async(upper).await?;
                    let step = match step {
                        Some(value) => self.integer_async(value).await?,
                        None => 1,
                    };
                    if step <= 0 {
                        return Err(ExecError::FunctionError {
                            sqlstate: "22023",
                            message: "BY value of FOR loop must be greater than zero".into(),
                        });
                    }
                    self.frames.push(Frame::default());
                    self.frames.last_mut().unwrap().slots.insert(
                        variable.clone(),
                        Slot {
                            value: Datum::Int4(lower),
                            ty: ColumnType::Int4,
                            record_types: None,
                            constant: false,
                            not_null: true,
                        },
                    );
                    let mut current = lower;
                    let end = upper;
                    let mut found = false;
                    while if *reverse {
                        current >= end
                    } else {
                        current <= end
                    } {
                        found = true;
                        self.assign_name(variable, Datum::Int4(current))?;
                        if let Some(flow) = self.loop_iteration(label, body).await? {
                            self.frames.pop();
                            self.set_found(found);
                            return Ok(flow);
                        }
                        if current == end {
                            break;
                        }
                        let next = if *reverse {
                            current.checked_sub(step)
                        } else {
                            current.checked_add(step)
                        };
                        let Some(next) = next else { break };
                        current = next;
                    }
                    self.frames.pop();
                    self.set_found(found);
                    Ok(Flow::Next)
                }
                PlPgSqlLoop::Query { targets, query } => {
                    let statement = self.bind_statement(query)?;
                    let QueryResult::Rows { fields, rows, .. } =
                        Box::pin(self.session.run_one(&statement)).await?
                    else {
                        return Err(ExecError::Syntax("FOR query did not return rows".into()));
                    };
                    let found = !rows.is_empty();
                    for row in rows {
                        self.assign_row(targets, &fields, Some(&row)).await?;
                        if let Some(flow) = self.loop_iteration(label, body).await? {
                            self.set_found(found);
                            return Ok(flow);
                        }
                    }
                    self.set_found(found);
                    Ok(Flow::Next)
                }
                PlPgSqlLoop::Dynamic {
                    targets,
                    query,
                    using,
                } => {
                    let statement = self.dynamic_statement(query, using).await?;
                    let QueryResult::Rows { fields, rows, .. } =
                        Box::pin(self.session.run_one(&statement)).await?
                    else {
                        return Err(ExecError::Syntax("FOR query did not return rows".into()));
                    };
                    let found = !rows.is_empty();
                    for row in rows {
                        self.assign_row(targets, &fields, Some(&row)).await?;
                        if let Some(flow) = self.loop_iteration(label, body).await? {
                            self.set_found(found);
                            return Ok(flow);
                        }
                    }
                    self.set_found(found);
                    Ok(Flow::Next)
                }
                PlPgSqlLoop::Foreach {
                    target,
                    slice,
                    array,
                } => {
                    let Datum::Array(array) = self.eval_async(array).await?.0 else {
                        return Err(ExecError::TypeMismatch(
                            "FOREACH expression must yield an array".into(),
                        ));
                    };
                    let slice = usize::try_from(slice.unwrap_or(0)).unwrap_or(usize::MAX);
                    if slice > array.ndims() {
                        return Err(ExecError::FunctionError {
                            sqlstate: "2202E",
                            message: format!(
                                "slice dimension ({slice}) is out of the valid range 0..{}",
                                array.ndims()
                            ),
                        });
                    }
                    let mut found = false;
                    if slice == 0 {
                        for value in array.elems {
                            found = true;
                            self.assign_target(target, value).await?;
                            if let Some(flow) = self.loop_iteration(label, body).await? {
                                self.set_found(found);
                                return Ok(flow);
                            }
                        }
                    } else {
                        let dimensions = array.dims[array.ndims() - slice..].to_vec();
                        let chunk_size = dimensions
                            .iter()
                            .map(|dimension| usize::try_from(dimension.len).unwrap_or(0))
                            .product::<usize>();
                        for values in array.elems.chunks(chunk_size.max(1)) {
                            found = true;
                            self.assign_target(
                                target,
                                Datum::Array(ArrayValue::with_dims(
                                    array.elem,
                                    values.to_vec(),
                                    dimensions.clone(),
                                )),
                            )
                            .await?;
                            if let Some(flow) = self.loop_iteration(label, body).await? {
                                self.set_found(found);
                                return Ok(flow);
                            }
                        }
                    }
                    self.set_found(found);
                    Ok(Flow::Next)
                }
            }
        })
    }

    async fn loop_iteration(
        &mut self,
        loop_label: Option<&str>,
        body: &[PlPgSqlStatement],
    ) -> Result<Option<Flow>, ExecError> {
        match self.exec_statements(body).await? {
            Flow::Next => Ok(None),
            Flow::Return(value) => Ok(Some(Flow::Return(value))),
            Flow::LoopControl { continuing, label }
                if label.is_none() || label.as_deref() == loop_label =>
            {
                if continuing {
                    Ok(None)
                } else {
                    Ok(Some(Flow::Next))
                }
            }
            flow => Ok(Some(flow)),
        }
    }

    async fn declare(&mut self, declaration: &PlPgSqlDeclaration) -> Result<(), ExecError> {
        match declaration {
            PlPgSqlDeclaration::Variable {
                name,
                ty,
                constant,
                not_null,
                default,
            } => {
                let ty = declaration_type(ty, |name| self.declaration_reference_type(name))?;
                let value = match default {
                    Some(expr) => {
                        self.session
                            .plpgsql_eval_async(Expr::Cast {
                                expr: Box::new(self.bind_expr(expr)?),
                                ty,
                            })
                            .await?
                            .0
                    }
                    None => Datum::Null,
                };
                let ctx = self.session.plpgsql_eval_context();
                let value = cast_value(&value, ty, &ctx)?;
                if *not_null && matches!(value, Datum::Null) {
                    return Err(ExecError::FunctionError {
                        sqlstate: "23502",
                        message: format!(
                            "variable \"{name}\" declared NOT NULL cannot default to NULL"
                        ),
                    });
                }
                self.frames.last_mut().unwrap().slots.insert(
                    name.clone(),
                    Slot {
                        value,
                        ty,
                        record_types: None,
                        constant: *constant,
                        not_null: *not_null,
                    },
                );
                Ok(())
            }
            PlPgSqlDeclaration::Alias { name, target } => {
                if self.lookup_slot(target).is_none() {
                    return Err(ExecError::Syntax(format!(
                        "alias target \"{target}\" does not exist"
                    )));
                }
                self.frames
                    .last_mut()
                    .unwrap()
                    .aliases
                    .insert(name.clone(), target.clone());
                Ok(())
            }
            PlPgSqlDeclaration::Cursor {
                name,
                scroll,
                arguments,
                query,
            } => {
                self.cursor_declarations.insert(
                    name.clone(),
                    CursorDeclaration {
                        scroll: *scroll,
                        arguments: arguments.clone(),
                        statement: query.as_ref().clone(),
                    },
                );
                self.frames.last_mut().unwrap().slots.insert(
                    name.clone(),
                    Slot {
                        value: Datum::Text(name.clone()),
                        ty: ColumnType::Text,
                        record_types: None,
                        constant: false,
                        not_null: true,
                    },
                );
                Ok(())
            }
        }
    }

    async fn eval_async(&mut self, expr: &Expr) -> Result<(Datum, ColumnType), ExecError> {
        let expr = self.bind_expr(expr)?;
        self.session.plpgsql_eval_async(expr).await
    }

    async fn dynamic_statement(
        &mut self,
        query: &Expr,
        using: &[Expr],
    ) -> Result<Statement, ExecError> {
        let Datum::Text(source) = self.eval_async(query).await?.0 else {
            return Err(ExecError::TypeMismatch(
                "EXECUTE query string must be text".into(),
            ));
        };
        let mut values = Vec::with_capacity(using.len());
        for expr in using {
            values.push(self.eval_async(expr).await?);
        }
        let mut statements =
            crabka_pgparser::parse(&source).map_err(|error| ExecError::Syntax(error.message))?;
        if statements.len() != 1 {
            return Err(ExecError::Syntax(
                "EXECUTE query string must contain one statement".into(),
            ));
        }
        let mut statement = statements.pop().expect("one dynamic statement");
        self.session.plpgsql_bind_params(&mut statement, &values)?;
        Ok(statement)
    }

    async fn truth_async(&mut self, expr: &Expr) -> Result<bool, ExecError> {
        match self.eval_async(expr).await?.0 {
            Datum::Bool(value) => Ok(value),
            Datum::Null => Ok(false),
            _ => Err(ExecError::TypeMismatch(
                "condition must be type boolean".into(),
            )),
        }
    }

    async fn integer_async(&mut self, expr: &Expr) -> Result<i32, ExecError> {
        let value = self.eval_async(expr).await?.0;
        let ctx = self.session.plpgsql_eval_context();
        let Datum::Int4(value) = cast_value(&value, ColumnType::Int4, &ctx)? else {
            unreachable!("int4 cast returned another datum type");
        };
        Ok(value)
    }

    fn lookup_slot(&self, name: &str) -> Option<&Slot> {
        let mut current = name;
        for frame in self.frames.iter().rev() {
            if let Some(target) = frame.aliases.get(current) {
                current = target;
            }
            if let Some(slot) = frame.slots.get(current) {
                return Some(slot);
            }
        }
        None
    }

    fn resolve_alias(&self, name: &str) -> String {
        let mut current = name.to_string();
        for frame in self.frames.iter().rev() {
            if let Some(target) = frame.aliases.get(&current) {
                current = target.clone();
            }
        }
        current
    }

    async fn assign_target(
        &mut self,
        target: &PlPgSqlTarget,
        value: Datum,
    ) -> Result<(), ExecError> {
        if target.subscripts.is_empty() && target.path.len() == 1 {
            return self.assign_name(&target.path[0], value);
        }
        let mut subscripts = Vec::with_capacity(target.subscripts.len());
        for expression in &target.subscripts {
            subscripts.push(self.eval_async(expression).await?.0);
        }
        if target.path.len() == 1 && !subscripts.is_empty() {
            let name = &target.path[0];
            let slot = self
                .lookup_slot(name)
                .cloned()
                .ok_or_else(|| ExecError::Syntax(format!("\"{name}\" is not a known variable")))?;
            let ctx = self.session.plpgsql_eval_context();
            let assigned =
                assign_subscripted(&slot.value, Some(slot.ty), &subscripts, &value, &ctx)?;
            return self.assign_name(name, assigned);
        }
        if target.path.len() == 2 {
            let name = self.resolve_alias(&target.path[0]);
            let ctx = self.session.plpgsql_eval_context();
            for frame in self.frames.iter_mut().rev() {
                let Some(slot) = frame.slots.get_mut(&name) else {
                    continue;
                };
                let Datum::Record(record) = &mut slot.value else {
                    return Err(ExecError::ObjectNotInPrerequisiteState(format!(
                        "record \"{name}\" is not assigned yet"
                    )));
                };
                let Some(index) = record
                    .names
                    .iter()
                    .position(|field| field == &target.path[1])
                else {
                    return Err(ExecError::UndefinedColumn(target.path[1].clone()));
                };
                let field_type = slot
                    .record_types
                    .as_deref()
                    .and_then(|types| types.get(index))
                    .copied()
                    .or_else(|| record.values[index].column_type());
                record.values[index] = if subscripts.is_empty() {
                    match field_type {
                        Some(ty) => cast_value(&value, ty, &ctx)?,
                        None => value,
                    }
                } else {
                    assign_subscripted(
                        &record.values[index],
                        field_type,
                        &subscripts,
                        &value,
                        &ctx,
                    )?
                };
                return Ok(());
            }
        }
        Err(ExecError::Syntax(
            "invalid PL/pgSQL assignment target".into(),
        ))
    }

    fn assign_name(&mut self, name: &str, value: Datum) -> Result<(), ExecError> {
        let name = self.resolve_alias(name);
        let ctx = self.session.plpgsql_eval_context();
        for frame in self.frames.iter_mut().rev() {
            if let Some(slot) = frame.slots.get_mut(&name) {
                if slot.constant {
                    return Err(ExecError::FunctionError {
                        sqlstate: "22005",
                        message: format!("variable \"{name}\" is declared CONSTANT"),
                    });
                }
                let value = cast_value(&value, slot.ty, &ctx)?;
                if slot.not_null && matches!(value, Datum::Null) {
                    return Err(ExecError::FunctionError {
                        sqlstate: "23502",
                        message: format!(
                            "null value cannot be assigned to variable \"{name}\" declared NOT NULL"
                        ),
                    });
                }
                slot.record_types = inferred_record_types(&value);
                slot.value = value;
                return Ok(());
            }
        }
        Err(ExecError::Syntax(format!(
            "\"{name}\" is not a known variable"
        )))
    }

    fn set_record_types(
        &mut self,
        name: &str,
        record_types: Arc<[ColumnType]>,
    ) -> Result<(), ExecError> {
        let name = self.resolve_alias(name);
        for frame in self.frames.iter_mut().rev() {
            if let Some(slot) = frame.slots.get_mut(&name) {
                slot.record_types = Some(record_types);
                return Ok(());
            }
        }
        Err(ExecError::Syntax(format!(
            "\"{name}\" is not a known variable"
        )))
    }

    fn output_value(&self) -> Datum {
        self.output_slot
            .as_deref()
            .and_then(|name| self.lookup_slot(name))
            .map_or(Datum::Null, |slot| slot.value.clone())
    }

    fn set_found(&mut self, found: bool) {
        let _ = self.assign_name("found", Datum::Bool(found));
    }

    fn set_special_error(&mut self, error: &PgError) {
        let _ = self.assign_name("sqlstate", Datum::Text(error.code.clone()));
        let _ = self.assign_name("sqlerrm", Datum::Text(error.message.clone()));
    }

    fn push_set_value(&mut self, value: Datum) -> Result<(), ExecError> {
        let row = match value {
            Datum::Record(record) => record.values,
            value => vec![value],
        };
        self.push_set_row(row)
    }

    fn push_current_output_row(&mut self) -> Result<(), ExecError> {
        let names = self
            .set_results
            .as_ref()
            .ok_or_else(|| ExecError::Syntax("not a set-returning function".into()))?
            .columns
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        let row = names
            .iter()
            .map(|name| {
                self.lookup_slot(name)
                    .map_or(Datum::Null, |slot| slot.value.clone())
            })
            .collect();
        self.push_set_row(row)
    }

    fn push_set_row(&mut self, row: Vec<Datum>) -> Result<(), ExecError> {
        let ctx = self.session.plpgsql_eval_context();
        let collector = self
            .set_results
            .as_mut()
            .ok_or_else(|| ExecError::Syntax("not a set-returning function".into()))?;
        if row.len() != collector.columns.len() {
            return Err(ExecError::FunctionError {
                sqlstate: "42804",
                message: format!(
                    "returned row contains {} columns but function expects {}",
                    row.len(),
                    collector.columns.len()
                ),
            });
        }
        let row = row
            .into_iter()
            .zip(&collector.columns)
            .map(|(value, (_, ty))| cast_value(&value, *ty, &ctx))
            .collect::<Result<Vec<_>, _>>()?;
        collector.rows.push(row);
        Ok(())
    }

    fn push_query_result(&mut self, result: QueryResult) -> Result<usize, ExecError> {
        let QueryResult::Rows { fields, rows, .. } = result else {
            return Err(ExecError::Syntax(
                "RETURN QUERY must execute a query that returns rows".into(),
            ));
        };
        let count = rows.len();
        for row in rows {
            let values = fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    self.session
                        .plpgsql_decode_cell(field, row.get(index).and_then(Option::as_ref))
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.push_set_row(values)?;
        }
        self.last_row_count = count;
        Ok(count)
    }

    async fn consume_sql_result(
        &mut self,
        result: QueryResult,
        into: Option<&PlPgSqlInto>,
        update_found: bool,
        discard_rows: bool,
        dml_returning: bool,
    ) -> Result<(), ExecError> {
        self.last_row_count = result_row_count(&result);
        let found = self.last_row_count > 0;
        match (result, into) {
            (QueryResult::Rows { fields, rows, .. }, Some(into)) => {
                if (into.strict && rows.len() != 1) || (dml_returning && rows.len() > 1) {
                    let (sqlstate, message) = if rows.is_empty() {
                        ("P0002", "query returned no rows")
                    } else {
                        ("P0003", "query returned more than one row")
                    };
                    return Err(ExecError::FunctionError {
                        sqlstate,
                        message: message.into(),
                    });
                }
                self.assign_row(&into.targets, &fields, rows.first())
                    .await?;
            }
            (QueryResult::Rows { .. }, None) if !discard_rows => {
                return Err(ExecError::Syntax(
                    "query has no destination for result data; use PERFORM instead".into(),
                ));
            }
            (_, Some(_)) => {
                return Err(ExecError::Syntax(
                    "INTO used with a statement that returns no rows".into(),
                ));
            }
            _ => {}
        }
        if update_found {
            self.set_found(found);
        }
        Ok(())
    }

    async fn assign_row(
        &mut self,
        targets: &[PlPgSqlTarget],
        fields: &[FieldDescription],
        row: Option<&Vec<Option<crabka_pgwire::engine::Cell>>>,
    ) -> Result<(), ExecError> {
        if let [target] = targets
            && target.path.len() == 1
            && target.subscripts.is_empty()
            && self
                .lookup_slot(&target.path[0])
                .is_some_and(|slot| matches!(slot.ty, ColumnType::Record(_)))
        {
            let values = fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    self.session.plpgsql_decode_cell(
                        field,
                        row.and_then(|row| row.get(index)).and_then(Option::as_ref),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            let names: Vec<String> = fields.iter().map(|field| field.name.clone()).collect();
            let record_types = fields
                .iter()
                .map(|field| crate::exec::column_type_from_oid(field.type_oid))
                .collect::<Result<Vec<_>, _>>()?;
            self.assign_target(
                target,
                Datum::Record(RecordValue::named(None, Arc::from(names), values)),
            )
            .await?;
            self.set_record_types(&target.path[0], Arc::from(record_types))?;
            return Ok(());
        }
        if targets.len() != row.map_or(targets.len(), Vec::len) {
            return Err(ExecError::Syntax(
                "number of PL/pgSQL targets does not match result columns".into(),
            ));
        }
        for (index, target) in targets.iter().enumerate() {
            let value = self.session.plpgsql_decode_cell(
                &fields[index],
                row.and_then(|row| row.get(index)).and_then(Option::as_ref),
            )?;
            self.assign_target(target, value).await?;
        }
        Ok(())
    }

    async fn raise(&mut self, raise: &PlPgSqlRaise) -> Result<Flow, ExecError> {
        if raise.message.is_none() && raise.condition.is_none() && raise.options.is_empty() {
            let error = self
                .active_error
                .clone()
                .ok_or_else(|| ExecError::FunctionError {
                    sqlstate: "0Z002",
                    message: "RAISE without parameters cannot be used outside an exception handler"
                        .into(),
                })?;
            return Err(ExecError::Remote(error));
        }
        let mut values = Vec::with_capacity(raise.parameters.len());
        for expr in &raise.parameters {
            let value = self.eval_async(expr).await?.0;
            values.push(self.session.plpgsql_render(&value));
        }
        let mut options = Vec::with_capacity(raise.options.len());
        for (name, value) in &raise.options {
            let value = self.eval_async(value).await?.0;
            reject_null_raise_option(&value)?;
            let value = self.session.plpgsql_render(&value);
            options.push((name.as_str(), value));
        }
        let diagnostic =
            build_raise_diagnostic(raise, &values, options)?.with_context(self.context.clone());
        if raise.level == PlPgSqlRaiseLevel::Exception {
            return Err(ExecError::Remote(diagnostic));
        }
        self.session.plpgsql_notice(diagnostic)?;
        Ok(Flow::Next)
    }

    fn bind_expr(&self, expr: &Expr) -> Result<Expr, ExecError> {
        SqlBinder {
            interpreter: self,
            resolution: self.session.plpgsql_resolution_scope(),
        }
        .rewrite_procedural_expr(expr)
    }

    fn declaration_reference_type(&self, reference: &str) -> Result<Option<ColumnType>, ExecError> {
        if let Some(slot) = self.lookup_slot(reference) {
            return Ok(Some(slot.ty));
        }
        let resolution = self.session.plpgsql_resolution_scope();
        crate::routine::relation_column_type(self.session.plpgsql_catalog(), &resolution, reference)
            .map(Some)
    }

    fn bind_statement(&self, statement: &Statement) -> Result<Statement, ExecError> {
        rewrite_statement(
            statement,
            &SqlBinder {
                interpreter: self,
                resolution: self.session.plpgsql_resolution_scope(),
            },
        )
    }

    async fn execute_nested_call(
        &mut self,
        name: &str,
        args: &[Expr],
        named_args: &[(String, Expr)],
        variadic: Option<&Expr>,
    ) -> Result<(), ExecError> {
        let resolution_args = args
            .iter()
            .map(|arg| self.bind_expr(arg))
            .collect::<Result<Vec<_>, _>>()?;
        let resolution_named = named_args
            .iter()
            .map(|(label, arg)| Ok((label.clone(), self.bind_expr(arg)?)))
            .collect::<Result<Vec<_>, ExecError>>()?;
        let resolution_variadic = variadic.map(|arg| self.bind_expr(arg)).transpose()?;
        let bound = crate::routine::bind_procedure_call(
            self.session.plpgsql_catalog(),
            name,
            &resolution_args,
            &resolution_named,
            resolution_variadic.as_ref(),
        )?
        .ok_or_else(|| ExecError::UndefinedFunction(format!("procedure {name} does not exist")))?;
        let mut target_args = args.iter().map(Some).collect::<Vec<_>>();
        target_args.resize(bound.routine.params.len(), None);
        for (name, arg) in named_args {
            if let Some(index) = bound
                .routine
                .params
                .iter()
                .position(|param| param.name.as_deref() == Some(name))
            {
                target_args[index] = Some(arg);
            }
        }
        let mut targets = Vec::new();
        let mut call_args = Vec::with_capacity(bound.args.len());
        for (index, (param, arg)) in bound.routine.params.iter().zip(&bound.args).enumerate() {
            if param.mode.is_output() {
                let target = target_args
                    .get(index)
                    .and_then(|arg| *arg)
                    .and_then(call_output_target)
                    .ok_or_else(|| {
                    ExecError::Syntax(format!(
                        "procedure parameter {} is an output parameter but corresponding argument is not writable",
                        param.name.as_deref().unwrap_or("<unnamed>")
                    ))
                })?;
                if target.path.is_empty()
                    || self
                        .lookup_slot(&target.path[0])
                        .is_none_or(|slot| slot.constant)
                {
                    return Err(ExecError::Syntax(format!(
                        "procedure parameter {} is an output parameter but corresponding argument is not writable",
                        param.name.as_deref().unwrap_or("<unnamed>")
                    )));
                }
                targets.push(target);
            }
            call_args.push(arg.clone());
        }
        let _guard = self.session.plpgsql_enter_call()?;
        let output = execute_call_with_output(
            self.session,
            name,
            &call_args,
            &[],
            None,
            self.allow_transaction_control,
        )
        .await?;
        if targets.is_empty() {
            return Ok(());
        }
        let output = output.ok_or_else(|| {
            ExecError::ObjectNotInPrerequisiteState(
                "procedure with output parameters returned no output row".into(),
            )
        })?;
        for (target, value) in targets.iter().zip(output.values) {
            self.assign_target(target, value).await?;
        }
        Ok(())
    }
}

fn call_output_target(expr: &Expr) -> Option<PlPgSqlTarget> {
    match expr {
        Expr::Column { table: None, name } => Some(PlPgSqlTarget {
            path: vec![name.clone()],
            subscripts: Vec::new(),
        }),
        Expr::Column {
            table: Some(record),
            name,
        } => Some(PlPgSqlTarget {
            path: vec![record.clone(), name.clone()],
            subscripts: Vec::new(),
        }),
        _ => None,
    }
}

/// The spelling `PostgreSQL` substitutes for a NULL `RAISE` format parameter.
///
/// `RAISE NOTICE '%', NULL` prints `<NULL>` there. A NULL is not a value the
/// text output functions can render — it travels out of band on the wire — so
/// every rendering path in a `RAISE` has to name it explicitly rather than hand
/// it to `encode_text`, which panics on one.
pub(crate) const NULL_RAISE_PARAMETER: &str = "<NULL>";

/// A NULL `USING` option value is an error, not a `<NULL>`: `PostgreSQL` reports
/// 22004 rather than putting the word into the DETAIL or HINT.
fn reject_null_raise_option(value: &Datum) -> Result<(), ExecError> {
    if value.is_null() {
        return Err(ExecError::FunctionError {
            sqlstate: "22004",
            message: "RAISE statement option cannot be null".into(),
        });
    }
    Ok(())
}

/// `ASSERT cond, message` with a NULL message falls back to the default text,
/// exactly as `PostgreSQL` does — the NULL is not rendered at all.
const DEFAULT_ASSERT_MESSAGE: &str = "assertion failed";

fn build_raise_diagnostic<'a>(
    raise: &PlPgSqlRaise,
    values: &[String],
    option_values: impl IntoIterator<Item = (&'a str, String)>,
) -> Result<PgError, ExecError> {
    let template = raise.message.clone().unwrap_or_else(|| {
        raise
            .condition
            .clone()
            .unwrap_or_else(|| "RAISE EXCEPTION".into())
    });
    let mut message = format_raise_message(&template, values)?;
    let mut options = HashMap::new();
    for (name, value) in option_values {
        if options.insert(name, value).is_some() {
            return Err(ExecError::Syntax(format!(
                "RAISE option {name} specified more than once"
            )));
        }
    }
    if let Some(value) = options.get("message") {
        if raise.message.is_some() {
            return Err(ExecError::Syntax(
                "RAISE option MESSAGE cannot be used with a message string".into(),
            ));
        }
        message.clone_from(value);
    }
    let mut code = match raise.condition.as_deref() {
        Some(condition) => condition_sqlstate(condition)?,
        None => match raise.level {
            PlPgSqlRaiseLevel::Warning => "01000".into(),
            PlPgSqlRaiseLevel::Exception => "P0001".into(),
            _ => "00000".into(),
        },
    };
    if let Some(value) = options.get("errcode") {
        if !crate::plpgsql_sqlstate::is_valid_sqlstate(value) || value == "00000" {
            return Err(ExecError::Syntax(format!(
                "invalid SQLSTATE code \"{value}\""
            )));
        }
        code.clone_from(value);
    }
    let mut diagnostic = match raise.level {
        PlPgSqlRaiseLevel::Debug => PgError::debug(message),
        PlPgSqlRaiseLevel::Log => PgError::log(message),
        PlPgSqlRaiseLevel::Info => PgError::info(message),
        PlPgSqlRaiseLevel::Notice => PgError::notice(message),
        PlPgSqlRaiseLevel::Warning => PgError::warning(message),
        PlPgSqlRaiseLevel::Exception => PgError::error(&code, message),
    }
    .with_code(code);
    for (name, value) in options {
        diagnostic = match name {
            "message" | "errcode" => diagnostic,
            "detail" => diagnostic.with_detail(value),
            "hint" => diagnostic.with_hint(value),
            "column" => diagnostic.with_column(value),
            "constraint" => diagnostic.with_constraint(value),
            "datatype" => diagnostic.with_datatype(value),
            "table" => diagnostic.with_table(value),
            "schema" => diagnostic.with_schema(value),
            _ => {
                return Err(ExecError::Syntax(format!(
                    "unrecognized RAISE option {name}"
                )));
            }
        };
    }
    Ok(diagnostic)
}

fn ensure_error_context(error: PgError, context: &str) -> PgError {
    if error
        .diagnostics
        .as_deref()
        .and_then(|diagnostics| diagnostics.context.as_ref())
        .is_some()
    {
        error
    } else {
        error.with_context(context)
    }
}

fn sql_statement_error(error: ExecError, routine: &Routine, statement: usize) -> ExecError {
    ExecError::Remote(ensure_error_context(
        error.into_pg(),
        &format!("SQL function \"{}\" statement {statement}", routine.name),
    ))
}

fn assign_subscripted(
    current: &Datum,
    declared_ty: Option<ColumnType>,
    subscripts: &[Datum],
    value: &Datum,
    ctx: &crate::clock::EvalCtx,
) -> Result<Datum, ExecError> {
    let array_elem = match declared_ty {
        Some(ColumnType::Array(elem)) => Some(elem),
        _ => match current {
            Datum::Array(array) => Some(array.elem),
            _ => None,
        },
    };
    if let Some(elem) = array_elem {
        let subscripts = subscripts
            .iter()
            .cloned()
            .map(crate::array_fn::SubscriptArg::Index)
            .collect::<Vec<_>>();
        return crate::array_fn::array_assign(current, &subscripts, value, elem, ctx);
    }
    if matches!(declared_ty, Some(ColumnType::Jsonb)) || matches!(current, Datum::Jsonb(_)) {
        return crate::json_fn::jsonb_subscript_assign(current, subscripts, value);
    }
    Err(ExecError::TypeMismatch(
        "PL/pgSQL subscripted assignment requires an array or jsonb value".into(),
    ))
}

fn format_raise_message(template: &str, values: &[String]) -> Result<String, ExecError> {
    let mut output = String::with_capacity(template.len());
    let mut values = values.iter();
    let mut chars = template.chars();
    while let Some(character) = chars.next() {
        if character != '%' {
            output.push(character);
            continue;
        }
        if matches!(chars.clone().next(), Some('%')) {
            chars.next();
            output.push('%');
            continue;
        }
        let value = values
            .next()
            .ok_or_else(|| ExecError::Syntax("too few parameters specified for RAISE".into()))?;
        output.push_str(value);
    }
    if values.next().is_some() {
        return Err(ExecError::Syntax(
            "too many parameters specified for RAISE".into(),
        ));
    }
    Ok(output)
}

fn result_row_count(result: &QueryResult) -> usize {
    match result {
        QueryResult::Rows { rows, .. } => rows.len(),
        QueryResult::Command { tag } => tag
            .split_whitespace()
            .next_back()
            .and_then(|n| n.parse().ok())
            .unwrap_or(0),
        QueryResult::Empty => 0,
    }
}

fn statement_has_dml_returning(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::Insert {
            returning: Some(_),
            ..
        } | Statement::Update {
            returning: Some(_),
            ..
        } | Statement::Delete {
            returning: Some(_),
            ..
        } | Statement::Merge {
            returning: Some(_),
            ..
        }
    )
}

fn parse_cursor_direction(direction: &str) -> Result<FetchDirection, ExecError> {
    let normalized = direction.to_ascii_lowercase();
    let words = normalized.split_whitespace().collect::<Vec<_>>();
    let signed = |word: &str| {
        word.parse::<i64>()
            .map_err(|_| ExecError::Syntax(format!("invalid cursor direction \"{direction}\"")))
    };
    Ok(match words.as_slice() {
        [] | ["next"] => FetchDirection::Relative(FetchCount::Rows(1)),
        ["prior"] => FetchDirection::Relative(FetchCount::Rows(-1)),
        ["first"] => FetchDirection::Absolute(1),
        ["last"] => FetchDirection::Absolute(-1),
        ["all"] | ["forward", "all"] => FetchDirection::Relative(FetchCount::AllForward),
        ["backward", "all"] => FetchDirection::Relative(FetchCount::AllBackward),
        [count] => FetchDirection::Relative(FetchCount::Rows(signed(count)?)),
        ["forward", count] => FetchDirection::Relative(FetchCount::Rows(signed(count)?)),
        ["backward", count] => FetchDirection::Relative(FetchCount::Rows(-signed(count)?)),
        ["absolute", count] => FetchDirection::Absolute(signed(count)?),
        ["relative", count] => FetchDirection::RelativeOne(signed(count)?),
        _ => {
            return Err(ExecError::Syntax(format!(
                "invalid cursor direction \"{direction}\""
            )));
        }
    })
}

fn condition_sqlstate(condition: &str) -> Result<String, ExecError> {
    if crate::plpgsql_sqlstate::is_valid_sqlstate(condition) {
        return Ok(condition.to_string());
    }
    crate::plpgsql_sqlstate::resolve_condition(condition)
        .next()
        .map(str::to_string)
        .ok_or_else(|| {
            ExecError::Syntax(format!("unrecognized exception condition \"{condition}\""))
        })
}

fn rewrite_record_field(
    record: &str,
    name: &str,
    slot: Option<&Slot>,
    tableoid: Option<&Slot>,
) -> Result<Option<Expr>, ExecError> {
    if name == "*" {
        return Ok(slot.map(SqlBinder::slot_expr));
    }
    if matches!(record, "old" | "new") && name == "tableoid" {
        return Ok(tableoid.map(SqlBinder::slot_expr));
    }
    Ok(match slot {
        Some(Slot {
            value: Datum::Record(record),
            record_types,
            ..
        }) => {
            let index = record
                .names
                .iter()
                .position(|field| field == name)
                .ok_or_else(|| ExecError::UndefinedColumn(name.to_string()))?;
            let value = record
                .values
                .get(index)
                .cloned()
                .ok_or_else(|| ExecError::UndefinedColumn(name.to_string()))?;
            let ty = record_types
                .as_deref()
                .and_then(|types| types.get(index))
                .copied()
                .or_else(|| value.column_type())
                .unwrap_or(ColumnType::Text);
            Some(Expr::Const { value, ty })
        }
        Some(slot) if matches!(slot.ty, ColumnType::Record(_)) => Some(Expr::FieldSelect {
            base: Box::new(SqlBinder::slot_expr(slot)),
            field: name.to_string(),
        }),
        _ => None,
    })
}

fn rewrite_expr_with(
    expr: &Expr,
    column: &impl Fn(Option<&str>, &str) -> Result<Option<Expr>, ExecError>,
    subquery: &impl Fn(&QueryExpr) -> Result<QueryExpr, ExecError>,
) -> Result<Expr, ExecError> {
    let one = |expr: &Expr| rewrite_expr_with(expr, column, subquery);
    let boxed = |expr: &Expr| -> Result<Box<Expr>, ExecError> { Ok(Box::new(one(expr)?)) };
    let list = |items: &[Expr]| items.iter().map(one).collect::<Result<Vec<_>, _>>();
    Ok(match expr {
        Expr::Column { table, name } => {
            column(table.as_deref(), name)?.unwrap_or_else(|| expr.clone())
        }
        Expr::Param(index) => column(None, &format!("${index}"))?.unwrap_or_else(|| expr.clone()),
        Expr::IntLiteral(_)
        | Expr::NumericLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BitStringLiteral(_)
        | Expr::BoolLiteral(_)
        | Expr::NullLiteral
        | Expr::Default
        | Expr::Const { .. } => expr.clone(),
        Expr::Unary { op, expr } => Expr::Unary {
            op: *op,
            expr: boxed(expr)?,
        },
        Expr::Binary { op, left, right } => Expr::Binary {
            op: *op,
            left: boxed(left)?,
            right: boxed(right)?,
        },
        Expr::Func(call) => Expr::Func(crabka_pgparser::ast::FuncCall {
            sql_syntax: call.sql_syntax,
            name: call.name.clone(),
            distinct: call.distinct,
            args: match &call.args {
                FuncArgs::Star => FuncArgs::Star,
                FuncArgs::Exprs(args) => FuncArgs::Exprs(list(args)?),
                FuncArgs::Named { positional, named } => FuncArgs::Named {
                    positional: list(positional)?,
                    named: named
                        .iter()
                        .map(|(label, arg)| Ok((label.clone(), one(arg)?)))
                        .collect::<Result<_, ExecError>>()?,
                },
                FuncArgs::Variadic { positional, array } => FuncArgs::Variadic {
                    positional: list(positional)?,
                    array: boxed(array)?,
                },
            },
            // An aggregate's sort keys may name the very variables this rewrite
            // substitutes, so they travel through it like the arguments.
            order_by: call
                .order_by
                .iter()
                .map(|item| {
                    Ok(crabka_pgparser::ast::OrderItem {
                        expr: one(&item.expr)?,
                        asc: item.asc,
                        nulls_first: item.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>, ExecError>>()?,
            within_group: call.within_group,
            filter: call.filter.as_deref().map(boxed).transpose()?,
        }),
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: boxed(expr)?,
            negated: *negated,
        },
        Expr::InList {
            expr,
            list: values,
            negated,
        } => Expr::InList {
            expr: boxed(expr)?,
            list: list(values)?,
            negated: *negated,
        },
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: boxed(expr)?,
            low: boxed(low)?,
            high: boxed(high)?,
            negated: *negated,
        },
        Expr::Like {
            expr,
            pattern,
            negated,
            kind,
            escape,
        } => Expr::Like {
            expr: boxed(expr)?,
            pattern: boxed(pattern)?,
            negated: *negated,
            kind: *kind,
            escape: escape.as_deref().map(boxed).transpose()?,
        },
        Expr::Case {
            operand,
            whens,
            else_result,
        } => Expr::Case {
            operand: operand.as_deref().map(boxed).transpose()?,
            whens: whens
                .iter()
                .map(|(a, b)| Ok((one(a)?, one(b)?)))
                .collect::<Result<_, ExecError>>()?,
            else_result: else_result.as_deref().map(boxed).transpose()?,
        },
        Expr::Cast { expr, ty } => Expr::Cast {
            expr: boxed(expr)?,
            ty: *ty,
        },
        Expr::FieldSelect { base, field } => Expr::FieldSelect {
            base: boxed(base)?,
            field: field.clone(),
        },
        Expr::FieldSelectAll(base) => Expr::FieldSelectAll(boxed(base)?),
        Expr::Collate { expr, collation } => Expr::Collate {
            expr: boxed(expr)?,
            collation: collation.clone(),
        },
        Expr::ScalarSubquery(query) => Expr::ScalarSubquery(Box::new(subquery(query)?)),
        Expr::Exists(query) => Expr::Exists(Box::new(subquery(query)?)),
        Expr::InSubquery {
            expr,
            subquery: query,
            negated,
        } => Expr::InSubquery {
            expr: boxed(expr)?,
            subquery: Box::new(subquery(query)?),
            negated: *negated,
        },
        Expr::Quantified {
            expr,
            op,
            all,
            subquery: query,
        } => Expr::Quantified {
            expr: boxed(expr)?,
            op: *op,
            all: *all,
            subquery: Box::new(subquery(query)?),
        },
        Expr::QuantifiedArray {
            expr,
            op,
            all,
            array,
        } => Expr::QuantifiedArray {
            expr: boxed(expr)?,
            op: *op,
            all: *all,
            array: boxed(array)?,
        },
        Expr::ArrayLiteral(items) => Expr::ArrayLiteral(list(items)?),
        Expr::ArraySubquery(query) => Expr::ArraySubquery(Box::new(subquery(query)?)),
        Expr::Row(items) => Expr::Row(list(items)?),
        Expr::Subscript { base, index } => Expr::Subscript {
            base: boxed(base)?,
            index: boxed(index)?,
        },
        Expr::ArrayRef { base, subscripts } => Expr::ArrayRef {
            base: boxed(base)?,
            subscripts: subscripts
                .iter()
                .map(|subscript| {
                    Ok(match subscript {
                        ArraySubscript::Index(value) => ArraySubscript::Index(one(value)?),
                        ArraySubscript::Slice { lower, upper } => ArraySubscript::Slice {
                            lower: lower.as_ref().map(one).transpose()?,
                            upper: upper.as_ref().map(one).transpose()?,
                        },
                    })
                })
                .collect::<Result<_, ExecError>>()?,
        },
        Expr::SqlJson(node) => Expr::SqlJson(Box::new(node.map_children(one)?)),
    })
}

fn rewrite_statement(
    statement: &Statement,
    binder: &SqlBinder<'_, '_>,
) -> Result<Statement, ExecError> {
    rewrite_statement_with_ctes(statement, binder, &crate::cte::CteContext::empty())
}

fn rewrite_statement_with_ctes(
    statement: &Statement,
    binder: &SqlBinder<'_, '_>,
    parent_ctes: &crate::cte::CteContext,
) -> Result<Statement, ExecError> {
    let mut out = statement.clone();
    match &mut out {
        Statement::Query(query) => {
            *query = binder.rewrite_query(query, parent_ctes)?;
        }
        Statement::Insert {
            table,
            source,
            with,
            on_conflict,
            returning,
            ..
        } => {
            let ctes = binder.rewrite_with(with, parent_ctes)?;
            let empty = crate::scope::Scope::empty();
            match source {
                crabka_pgparser::ast::InsertSource::Values(rows) => {
                    for row in rows {
                        for expr in row {
                            *expr = binder.rewrite_expr(expr, &empty, &ctes)?;
                        }
                    }
                }
                crabka_pgparser::ast::InsertSource::Query(query) => {
                    **query = binder.rewrite_query(query, &ctes)?;
                }
                crabka_pgparser::ast::InsertSource::DefaultValues => {}
            }
            let target = binder.table(table)?;
            let scope = crate::scope::Scope::single(&target, &target.name.name);
            if let Some(on_conflict) = on_conflict {
                let conflict_scope = crate::scope::Scope::insert_conflict(&target);
                if let crabka_pgparser::ast::OnConflictTarget::Columns {
                    index_predicate: Some(expr),
                    ..
                } = &mut on_conflict.target
                {
                    *expr = binder.rewrite_expr(expr, &scope, &ctes)?;
                }
                if let crabka_pgparser::ast::OnConflictAction::DoUpdate {
                    assignments,
                    filter,
                } = &mut on_conflict.action
                {
                    for (_, expr) in assignments {
                        *expr = binder.rewrite_expr(expr, &conflict_scope, &ctes)?;
                    }
                    if let Some(expr) = filter {
                        *expr = binder.rewrite_expr(expr, &conflict_scope, &ctes)?;
                    }
                }
            }
            binder.rewrite_returning(returning, &scope, &ctes)?;
        }
        Statement::Update {
            table,
            with,
            alias,
            assignments,
            from,
            filter,
            returning,
            ..
        } => {
            let ctes = binder.rewrite_with(with, parent_ctes)?;
            binder.rewrite_tables(from, &ctes, &[])?;
            let target = binder.table(table)?;
            let qualifier = alias.as_deref().unwrap_or(&target.name.name);
            let mut scope = crate::scope::Scope::single(&target, qualifier);
            if !from.is_empty() {
                scope.columns.extend(
                    crate::exec::build_from_schema_with_ctes_and_context(
                        binder.catalog(),
                        binder.resolution(),
                        from,
                        &ctes,
                        Some(&binder.interpreter.session.plpgsql_eval_context()),
                    )?
                    .scope
                    .columns,
                );
            }
            for assignment in assignments {
                for indirection in &mut assignment.indirections {
                    if let crabka_pgparser::ast::TargetIndirection::Subscript(subscript) =
                        indirection
                    {
                        for bound in subscript.bounds_mut() {
                            *bound = binder.rewrite_expr(bound, &scope, &ctes)?;
                        }
                    }
                }
                match &mut assignment.value {
                    AssignmentValue::Expr(expr) => {
                        *expr = binder.rewrite_expr(expr, &scope, &ctes)?;
                    }
                    AssignmentValue::Row(row) => {
                        for expr in row {
                            *expr = binder.rewrite_expr(expr, &scope, &ctes)?;
                        }
                    }
                    AssignmentValue::Subquery(query) => {
                        **query = binder.rewrite_query_outer(
                            query,
                            &ctes,
                            std::slice::from_ref(&scope),
                        )?;
                    }
                }
            }
            if let Some(expr) = filter {
                *expr = binder.rewrite_expr(expr, &scope, &ctes)?;
            }
            binder.rewrite_returning(returning, &scope, &ctes)?;
        }
        Statement::Delete {
            table,
            with,
            alias,
            using,
            filter,
            returning,
            ..
        } => {
            let ctes = binder.rewrite_with(with, parent_ctes)?;
            binder.rewrite_tables(using, &ctes, &[])?;
            let target = binder.table(table)?;
            let qualifier = alias.as_deref().unwrap_or(&target.name.name);
            let mut scope = crate::scope::Scope::single(&target, qualifier);
            if !using.is_empty() {
                scope.columns.extend(
                    crate::exec::build_from_schema_with_ctes_and_context(
                        binder.catalog(),
                        binder.resolution(),
                        using,
                        &ctes,
                        Some(&binder.interpreter.session.plpgsql_eval_context()),
                    )?
                    .scope
                    .columns,
                );
            }
            if let Some(expr) = filter {
                *expr = binder.rewrite_expr(expr, &scope, &ctes)?;
            }
            binder.rewrite_returning(returning, &scope, &ctes)?;
        }
        Statement::Merge {
            table,
            with,
            alias,
            source,
            on,
            clauses,
            returning,
        } => {
            let ctes = binder.rewrite_with(with, parent_ctes)?;
            let target = binder.table(table)?;
            let qualifier = alias.as_deref().unwrap_or(&target.name.name);
            let mut scope = crate::scope::Scope::single(&target, qualifier);
            let source_table = match source {
                crabka_pgparser::ast::MergeSource::Table { name, alias } => TableExpr::Table {
                    name: name.clone(),
                    only: false,
                    alias: alias.clone(),
                    columns: None,
                    sample: None,
                },
                crabka_pgparser::ast::MergeSource::Query {
                    query,
                    alias,
                    columns,
                } => {
                    **query = binder.rewrite_query(query, &ctes)?;
                    TableExpr::Derived {
                        subquery: (**query).clone(),
                        alias: alias.clone(),
                        columns: columns.clone(),
                        lateral: false,
                    }
                }
            };
            scope.columns.extend(
                crate::exec::build_from_schema_with_ctes_and_context(
                    binder.catalog(),
                    binder.resolution(),
                    std::slice::from_ref(&source_table),
                    &ctes,
                    Some(&binder.interpreter.session.plpgsql_eval_context()),
                )?
                .scope
                .columns,
            );
            *on = binder.rewrite_expr(on, &scope, &ctes)?;
            for clause in clauses {
                if let Some(condition) = &mut clause.condition {
                    *condition = binder.rewrite_expr(condition, &scope, &ctes)?;
                }
                match &mut clause.action {
                    crabka_pgparser::ast::MergeAction::Update(assignments) => {
                        for assignment in assignments {
                            for indirection in &mut assignment.indirections {
                                if let crabka_pgparser::ast::TargetIndirection::Subscript(
                                    subscript,
                                ) = indirection
                                {
                                    for bound in subscript.bounds_mut() {
                                        *bound = binder.rewrite_expr(bound, &scope, &ctes)?;
                                    }
                                }
                            }
                            match &mut assignment.value {
                                AssignmentValue::Expr(expr) => {
                                    *expr = binder.rewrite_expr(expr, &scope, &ctes)?;
                                }
                                AssignmentValue::Row(row) => {
                                    for expr in row {
                                        *expr = binder.rewrite_expr(expr, &scope, &ctes)?;
                                    }
                                }
                                AssignmentValue::Subquery(query) => {
                                    **query = binder.rewrite_query_outer(
                                        query,
                                        &ctes,
                                        std::slice::from_ref(&scope),
                                    )?;
                                }
                            }
                        }
                    }
                    crabka_pgparser::ast::MergeAction::Insert {
                        values: Some(values),
                        ..
                    } => {
                        for expr in values {
                            *expr = binder.rewrite_expr(expr, &scope, &ctes)?;
                        }
                    }
                    crabka_pgparser::ast::MergeAction::Delete
                    | crabka_pgparser::ast::MergeAction::DoNothing
                    | crabka_pgparser::ast::MergeAction::Insert { values: None, .. } => {}
                }
            }
            binder.rewrite_returning(returning, &scope, &ctes)?;
        }
        Statement::Call {
            args,
            named_args,
            variadic,
            ..
        } => {
            let empty = crate::scope::Scope::empty();
            for expr in args {
                *expr = binder.rewrite_expr(expr, &empty, parent_ctes)?;
            }
            for (_, expr) in named_args {
                *expr = binder.rewrite_expr(expr, &empty, parent_ctes)?;
            }
            if let Some(expr) = variadic {
                **expr = binder.rewrite_expr(expr, &empty, parent_ctes)?;
            }
        }
        _ => {}
    }
    Ok(out)
}

struct SqlBinder<'i, 's> {
    interpreter: &'i Interpreter<'s>,
    resolution: crate::relname::ResolutionScope,
}

impl SqlBinder<'_, '_> {
    fn catalog(&self) -> &dyn crabka_pgkv::Kv {
        self.interpreter.session.plpgsql_catalog()
    }

    fn resolution(&self) -> &crate::relname::ResolutionScope {
        &self.resolution
    }

    fn table(
        &self,
        reference: &crabka_pgparser::ast::RelationRef,
    ) -> Result<crabka_pgcatalog::Table, ExecError> {
        let name = crate::relname::resolve_relation(
            self.catalog(),
            self.resolution(),
            reference,
            crate::relname::SchemaDisposition::Reference,
        )?;
        Ok(crabka_pgcatalog::get_table(self.catalog(), &name)?)
    }

    fn slot_expr(slot: &Slot) -> Expr {
        Expr::Const {
            value: slot.value.clone(),
            ty: slot.ty,
        }
    }

    fn labeled_variable(&self, label: &str, name: &str) -> Option<Expr> {
        self.interpreter
            .frames
            .iter()
            .rev()
            .find(|frame| frame.label.as_deref() == Some(label))
            .and_then(|frame| {
                let target = frame.aliases.get(name).map_or(name, String::as_str);
                frame.slots.get(target)
            })
            .map(Self::slot_expr)
    }

    fn variable(&self, name: &str) -> Option<Expr> {
        self.interpreter.lookup_slot(name).map(Self::slot_expr)
    }

    fn rewrite_column(
        &self,
        scope: &crate::scope::Scope,
        outers: &[crate::scope::Scope],
        table: Option<&str>,
        name: &str,
    ) -> Result<Option<Expr>, ExecError> {
        if let Some(label) = table
            && let Some(value) = self.labeled_variable(label, name)
        {
            return Ok(Some(value));
        }
        if let Some(qualifier) = table {
            if scope.resolve(Some(qualifier), name).is_ok() {
                return Ok(None);
            }
            if scope
                .columns
                .iter()
                .any(|column| column.qualifier.as_deref() == Some(qualifier))
            {
                return Ok(None);
            }
            for outer in outers {
                if outer.resolve(Some(qualifier), name).is_ok()
                    || outer
                        .columns
                        .iter()
                        .any(|column| column.qualifier.as_deref() == Some(qualifier))
                {
                    return Ok(None);
                }
            }
            return rewrite_record_field(
                qualifier,
                name,
                self.interpreter.lookup_slot(qualifier),
                self.interpreter.lookup_slot("tg_relid"),
            );
        }

        let variable = self.variable(name);
        if variable.is_none() {
            return Ok(None);
        }
        let mut sql_resolution = scope.resolve(None, name);
        for outer in outers {
            if matches!(sql_resolution, Err(ExecError::UndefinedColumn(_))) {
                sql_resolution = outer.resolve(None, name);
            } else {
                break;
            }
        }
        match sql_resolution {
            Ok(_) => match self.interpreter.variable_conflict {
                PlPgSqlVariableConflict::Error => Err(ExecError::AmbiguousColumn(name.to_string())),
                PlPgSqlVariableConflict::UseVariable => Ok(variable),
                PlPgSqlVariableConflict::UseColumn => Ok(None),
            },
            Err(ExecError::AmbiguousColumn(_)) => match self.interpreter.variable_conflict {
                PlPgSqlVariableConflict::UseVariable => Ok(variable),
                PlPgSqlVariableConflict::Error | PlPgSqlVariableConflict::UseColumn => Ok(None),
            },
            Err(_) => Ok(variable),
        }
    }

    fn rewrite_expr(
        &self,
        expr: &Expr,
        scope: &crate::scope::Scope,
        ctes: &crate::cte::CteContext,
    ) -> Result<Expr, ExecError> {
        self.rewrite_expr_outer(expr, scope, &[], ctes)
    }

    fn rewrite_expr_outer(
        &self,
        expr: &Expr,
        scope: &crate::scope::Scope,
        outers: &[crate::scope::Scope],
        ctes: &crate::cte::CteContext,
    ) -> Result<Expr, ExecError> {
        rewrite_expr_with(
            expr,
            &|table, name| self.rewrite_column(scope, outers, table, name),
            &|query| {
                let nested_outers = std::iter::once(scope.clone())
                    .chain(outers.iter().cloned())
                    .collect::<Vec<_>>();
                self.rewrite_query_outer(query, ctes, &nested_outers)
            },
        )
    }

    fn rewrite_procedural_expr(&self, expr: &Expr) -> Result<Expr, ExecError> {
        rewrite_expr_with(
            expr,
            &|table, name| {
                if let Some(label) = table
                    && let Some(value) = self.labeled_variable(label, name)
                {
                    return Ok(Some(value));
                }
                match table {
                    None => Ok(self.variable(name)),
                    Some(record) => rewrite_record_field(
                        record,
                        name,
                        self.interpreter.lookup_slot(record),
                        self.interpreter.lookup_slot("tg_relid"),
                    ),
                }
            },
            &|query| self.rewrite_query(query, &crate::cte::CteContext::empty()),
        )
    }

    fn rewrite_query(
        &self,
        query: &QueryExpr,
        parent_ctes: &crate::cte::CteContext,
    ) -> Result<QueryExpr, ExecError> {
        self.rewrite_query_outer(query, parent_ctes, &[])
    }

    fn rewrite_query_outer(
        &self,
        query: &QueryExpr,
        parent_ctes: &crate::cte::CteContext,
        outers: &[crate::scope::Scope],
    ) -> Result<QueryExpr, ExecError> {
        let mut out = query.clone();
        let ctes = self.rewrite_with(&mut out.with, parent_ctes)?;
        let scope = self.rewrite_set(&mut out.body, &ctes, outers)?;
        let order_scope = match &out.body {
            SetExpr::Query(QueryBody::Select(select)) => {
                Self::projection_name_scope(&select.projection, &scope)
            }
            SetExpr::Query(QueryBody::Values(_))
            | SetExpr::Query(QueryBody::Nested(_))
            | SetExpr::SetOp { .. } => scope.clone(),
        };
        let order_outers = std::iter::once(scope.clone())
            .chain(outers.iter().cloned())
            .collect::<Vec<_>>();
        for order in &mut out.order_by {
            order.expr =
                self.rewrite_expr_outer(&order.expr, &order_scope, &order_outers, &ctes)?;
        }
        if let Some(expr) = &mut out.limit {
            *expr = self.rewrite_expr_outer(expr, &scope, outers, &ctes)?;
        }
        if let Some(expr) = &mut out.offset {
            *expr = self.rewrite_expr_outer(expr, &scope, outers, &ctes)?;
        }
        Ok(out)
    }

    fn rewrite_set(
        &self,
        set: &mut SetExpr,
        ctes: &crate::cte::CteContext,
        outers: &[crate::scope::Scope],
    ) -> Result<crate::scope::Scope, ExecError> {
        match set {
            SetExpr::SetOp { left, right, .. } => {
                self.rewrite_set(left, ctes, outers)?;
                self.rewrite_set(right, ctes, outers)?;
                let query = QueryExpr {
                    with: None,
                    body: set.clone(),
                    order_by: Vec::new(),
                    limit: None,
                    offset: None,
                    with_ties: false,
                    locking: None,
                };
                self.output_scope(&query, ctes)
            }
            SetExpr::Query(QueryBody::Values(values)) => {
                let scope = crate::scope::Scope::empty();
                for row in &mut values.rows {
                    for expr in row {
                        *expr = self.rewrite_expr_outer(expr, &scope, outers, ctes)?;
                    }
                }
                let query = QueryExpr {
                    with: None,
                    body: set.clone(),
                    order_by: Vec::new(),
                    limit: None,
                    offset: None,
                    with_ties: false,
                    locking: None,
                };
                self.output_scope(&query, ctes)
            }
            SetExpr::Query(QueryBody::Nested(query)) => {
                **query = self.rewrite_query_outer(query, ctes, outers)?;
                self.output_scope(query, ctes)
            }
            SetExpr::Query(QueryBody::Select(select)) => {
                self.rewrite_tables(&mut select.from, ctes, outers)?;
                let scope = if select.from.is_empty() {
                    crate::scope::Scope::empty()
                } else {
                    crate::exec::build_from_schema_with_ctes_and_context(
                        self.catalog(),
                        self.resolution(),
                        &select.from,
                        ctes,
                        Some(&self.interpreter.session.plpgsql_eval_context()),
                    )?
                    .scope
                };
                for item in &mut select.projection {
                    if let SelectItem::Expr { expr, .. } = item {
                        *expr = self.rewrite_expr_outer(expr, &scope, outers, ctes)?;
                    }
                }
                if let Some(expr) = &mut select.filter {
                    *expr = self.rewrite_expr_outer(expr, &scope, outers, ctes)?;
                }
                for expr in &mut select.group_by {
                    *expr = self.rewrite_expr_outer(expr, &scope, outers, ctes)?;
                }
                if let Some(expr) = &mut select.having {
                    *expr = self.rewrite_expr_outer(expr, &scope, outers, ctes)?;
                }
                for window in &mut select.windows {
                    self.rewrite_window_spec(&mut window.spec, &scope, outers, ctes)?;
                }
                for call in &mut select.window_calls {
                    if let FuncArgs::Exprs(args) = &mut call.args {
                        for expr in args {
                            *expr = self.rewrite_expr_outer(expr, &scope, outers, ctes)?;
                        }
                    }
                    if let Some(expr) = &mut call.filter {
                        *expr = self.rewrite_expr_outer(expr, &scope, outers, ctes)?;
                    }
                    if let crabka_pgparser::ast::WindowRef::Spec(spec) = &mut call.over {
                        self.rewrite_window_spec(spec, &scope, outers, ctes)?;
                    }
                }
                if let crabka_pgparser::ast::DistinctClause::On(exprs) = &mut select.distinct {
                    for expr in exprs {
                        *expr = self.rewrite_expr_outer(expr, &scope, outers, ctes)?;
                    }
                }
                let order_scope = Self::projection_name_scope(&select.projection, &scope);
                let order_outers = std::iter::once(scope.clone())
                    .chain(outers.iter().cloned())
                    .collect::<Vec<_>>();
                for order in &mut select.order_by {
                    order.expr =
                        self.rewrite_expr_outer(&order.expr, &order_scope, &order_outers, ctes)?;
                }
                if let Some(expr) = &mut select.limit {
                    *expr = self.rewrite_expr_outer(expr, &scope, outers, ctes)?;
                }
                if let Some(expr) = &mut select.offset {
                    *expr = self.rewrite_expr_outer(expr, &scope, outers, ctes)?;
                }
                Ok(scope)
            }
        }
    }

    fn output_scope(
        &self,
        query: &QueryExpr,
        ctes: &crate::cte::CteContext,
    ) -> Result<crate::scope::Scope, ExecError> {
        let fields = crate::query::describe_query_expr_with_ctes(
            self.catalog(),
            self.resolution(),
            query,
            ctes,
        )?;
        Ok(crate::scope::Scope {
            columns: fields
                .into_iter()
                .map(|field| {
                    Ok(crate::scope::ColumnBinding {
                        exposure: crate::scope::Exposure::Output,
                        qualifier: None,
                        name: field.name,
                        ty: crate::exec::column_type_from_oid(field.type_oid)?,
                    })
                })
                .collect::<Result<_, ExecError>>()?,
            ..Default::default()
        })
    }

    fn projection_name_scope(
        projection: &[SelectItem],
        input: &crate::scope::Scope,
    ) -> crate::scope::Scope {
        let mut columns = Vec::new();
        for item in projection {
            match item {
                SelectItem::Wildcard => {
                    // Skips a USING/NATURAL join's retained input columns for
                    // the same reason the real projection does, so this name
                    // scope keeps agreeing with the projection's width.
                    columns.extend(
                        input
                            .columns
                            .iter()
                            .filter(|column| !column.is_join_input())
                            .map(|column| crate::scope::ColumnBinding {
                                exposure: crate::scope::Exposure::Output,
                                qualifier: None,
                                name: column.name.clone(),
                                ty: column.ty,
                            }),
                    );
                }
                SelectItem::QualifiedWildcard(qualifier) => columns.extend(
                    input
                        .columns
                        .iter()
                        .filter(|column| column.qualifier.as_deref() == Some(qualifier))
                        .map(|column| crate::scope::ColumnBinding {
                            exposure: crate::scope::Exposure::Output,
                            qualifier: None,
                            name: column.name.clone(),
                            ty: column.ty,
                        }),
                ),
                SelectItem::Expr { expr, alias } => {
                    columns.push(crate::scope::ColumnBinding {
                        exposure: crate::scope::Exposure::Output,
                        qualifier: None,
                        name: alias
                            .clone()
                            .unwrap_or_else(|| crate::exec::derived_name(expr)),
                        ty: ColumnType::Text,
                    });
                }
            }
        }
        crate::scope::Scope {
            columns,
            ..Default::default()
        }
    }

    fn rewrite_window_spec(
        &self,
        spec: &mut crabka_pgparser::ast::WindowSpec,
        scope: &crate::scope::Scope,
        outers: &[crate::scope::Scope],
        ctes: &crate::cte::CteContext,
    ) -> Result<(), ExecError> {
        for expr in &mut spec.partition_by {
            *expr = self.rewrite_expr_outer(expr, scope, outers, ctes)?;
        }
        for order in &mut spec.order_by {
            order.expr = self.rewrite_expr_outer(&order.expr, scope, outers, ctes)?;
        }
        if let Some(frame) = &mut spec.frame {
            for bound in [&mut frame.start, &mut frame.end] {
                match bound {
                    crabka_pgparser::ast::FrameBound::Preceding(expr)
                    | crabka_pgparser::ast::FrameBound::Following(expr) => {
                        *expr = self.rewrite_expr_outer(expr, scope, outers, ctes)?;
                    }
                    crabka_pgparser::ast::FrameBound::UnboundedPreceding
                    | crabka_pgparser::ast::FrameBound::CurrentRow
                    | crabka_pgparser::ast::FrameBound::UnboundedFollowing => {}
                }
            }
        }
        Ok(())
    }

    fn rewrite_tables(
        &self,
        tables: &mut [TableExpr],
        ctes: &crate::cte::CteContext,
        query_outers: &[crate::scope::Scope],
    ) -> Result<(), ExecError> {
        let mut outer = crate::scope::Scope::empty();
        for table in tables {
            self.rewrite_table(table, &outer, query_outers, ctes)?;
            let relation = crate::exec::build_from_schema_with_ctes_and_context(
                self.catalog(),
                self.resolution(),
                std::slice::from_ref(table),
                ctes,
                Some(&self.interpreter.session.plpgsql_eval_context()),
            )?;
            outer.columns.extend(relation.scope.columns);
        }
        Ok(())
    }

    fn rewrite_table(
        &self,
        table: &mut TableExpr,
        outer: &crate::scope::Scope,
        query_outers: &[crate::scope::Scope],
        ctes: &crate::cte::CteContext,
    ) -> Result<(), ExecError> {
        match table {
            TableExpr::Table { sample, .. } => {
                if let Some(sample) = sample {
                    sample.percent =
                        self.rewrite_expr_outer(&sample.percent, outer, query_outers, ctes)?;
                    if let Some(expr) = &mut sample.repeatable {
                        *expr = self.rewrite_expr_outer(expr, outer, query_outers, ctes)?;
                    }
                }
            }
            TableExpr::Derived {
                subquery, lateral, ..
            } => {
                *subquery = if *lateral {
                    let lateral_outers = std::iter::once(outer.clone())
                        .chain(query_outers.iter().cloned())
                        .collect::<Vec<_>>();
                    self.rewrite_query_outer(subquery, ctes, &lateral_outers)?
                } else {
                    self.rewrite_query(subquery, ctes)?
                };
            }
            // A `JSON_TABLE` item is implicitly lateral, so its context and
            // `PASSING` expressions are rewritten against the outer scope
            // exactly as a function item's arguments are.
            TableExpr::JsonTable(table) => {
                for expr in table.exprs_mut() {
                    *expr = self.rewrite_expr_outer(expr, outer, query_outers, ctes)?;
                }
            }
            TableExpr::XmlTable(table) => {
                for expr in table.exprs_mut() {
                    *expr = self.rewrite_expr_outer(expr, outer, query_outers, ctes)?;
                }
            }
            TableExpr::Join {
                left,
                right,
                kind,
                constraint,
            } => {
                self.rewrite_table(left, outer, query_outers, ctes)?;
                let left_scope = crate::exec::build_from_schema_with_ctes_and_context(
                    self.catalog(),
                    self.resolution(),
                    std::slice::from_ref(left.as_ref()),
                    ctes,
                    Some(&self.interpreter.session.plpgsql_eval_context()),
                )?
                .scope;
                let mut right_outer = outer.clone();
                right_outer.columns.extend(left_scope.columns);
                self.rewrite_table(right, &right_outer, query_outers, ctes)?;
                if let JoinConstraint::On(expr) = constraint {
                    let join = TableExpr::Join {
                        left: left.clone(),
                        right: right.clone(),
                        kind: *kind,
                        constraint: JoinConstraint::None,
                    };
                    let join_scope = crate::exec::build_from_schema_with_ctes_and_context(
                        self.catalog(),
                        self.resolution(),
                        std::slice::from_ref(&join),
                        ctes,
                        Some(&self.interpreter.session.plpgsql_eval_context()),
                    )?
                    .scope;
                    *expr = self.rewrite_expr_outer(expr, &join_scope, query_outers, ctes)?;
                }
            }
            TableExpr::Function { functions, .. } => {
                for function in functions {
                    for arg in function.arguments_mut() {
                        *arg = self.rewrite_expr_outer(arg, outer, query_outers, ctes)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn rewrite_with(
        &self,
        with: &mut Option<crabka_pgparser::ast::WithClause>,
        parent: &crate::cte::CteContext,
    ) -> Result<crate::cte::CteContext, ExecError> {
        let Some(with) = with else {
            return Ok(parent.child());
        };
        let original = with.clone();
        let mut ctes = if with.recursive {
            crate::cte::describe_with_clause(
                self.catalog(),
                self.resolution(),
                Some(&original),
                parent,
            )?
        } else {
            parent.child()
        };
        for cte in &mut with.ctes {
            match &mut cte.body {
                CteBody::Query(query) => **query = self.rewrite_query(query, &ctes)?,
                CteBody::Dml(statement) => {
                    **statement = rewrite_statement_with_ctes(statement, self, &ctes)?;
                }
            }
            if let Some(cycle) = &mut cte.cycle
                && let Some((marked, unmarked)) = &mut cycle.mark_values
            {
                let empty = crate::scope::Scope::empty();
                *marked = self.rewrite_expr(marked, &empty, &ctes)?;
                *unmarked = self.rewrite_expr(unmarked, &empty, &ctes)?;
            }
            if !with.recursive {
                let relation = crate::cte::describe_cte_relation(
                    self.catalog(),
                    self.resolution(),
                    cte,
                    false,
                    &ctes,
                )?;
                ctes.insert(cte.name.clone(), relation);
            }
        }
        if with.recursive {
            crate::cte::describe_with_clause(self.catalog(), self.resolution(), Some(with), parent)
        } else {
            Ok(ctes)
        }
    }

    fn rewrite_returning(
        &self,
        returning: &mut Option<crabka_pgparser::ast::Returning>,
        scope: &crate::scope::Scope,
        ctes: &crate::cte::CteContext,
    ) -> Result<(), ExecError> {
        if let Some(returning) = returning {
            for item in &mut returning.items {
                if let SelectItem::Expr { expr, .. } = item {
                    *expr = self.rewrite_expr(expr, scope, ctes)?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgtypes::{ColumnType, Datum};
    use crabka_pgwire::engine::{Engine, QueryResult, Session};

    use super::execute_sql_table_function;
    use crate::{SqlEngine, eval::ArgType};

    fn first_text(results: &[QueryResult]) -> &str {
        let QueryResult::Rows { rows, .. } = &results[0] else {
            panic!("expected rows");
        };
        std::str::from_utf8(&rows[0][0].as_ref().expect("value").text).expect("utf8")
    }

    #[tokio::test]
    async fn do_executes_declarations_control_flow_and_dml() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE pl_do (id int4)")
            .await
            .expect("table");
        session
            .simple_query(
                "DO $$
                 DECLARE n int4 := 0;
                 BEGIN
                   FOR i IN 1..4 LOOP
                     n := n + i;
                   END LOOP;
                   IF n = 10 THEN
                     INSERT INTO pl_do VALUES (n);
                   END IF;
                 END
                 $$",
            )
            .await
            .expect("do block");
        let rows = session
            .simple_query("SELECT id FROM pl_do")
            .await
            .expect("select");
        assert_eq!(first_text(&rows), "10");
    }

    #[tokio::test]
    async fn sql_table_executor_keeps_body_columns_out_of_output_slots() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query(
                "CREATE FUNCTION table_range(upper_bound int) RETURNS TABLE(a int) LANGUAGE sql \
                 AS 'SELECT a FROM generate_series(1, upper_bound) AS a(a)'",
            )
            .await
            .expect("definition");
        let routine = crate::routine::resolve_call(
            session.plpgsql_catalog(),
            "table_range",
            &[ArgType::Known(ColumnType::Int4)],
        )
        .expect("lookup")
        .expect("routine");

        let rows = execute_sql_table_function(
            &mut session,
            &routine,
            &[Datum::Int4(3)],
            vec![("a".into(), ColumnType::Int4)],
        )
        .await
        .expect("table result");
        assert!(
            rows == vec![
                vec![Datum::Int4(1)],
                vec![Datum::Int4(2)],
                vec![Datum::Int4(3)]
            ]
        );
    }

    #[tokio::test]
    async fn plpgsql_procedure_binds_parameters_and_select_into() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query(
                "CREATE TABLE pl_call (id int4); \
                 CREATE PROCEDURE add_pl(x int4) LANGUAGE plpgsql AS $$
                 DECLARE doubled int4;
                 BEGIN
                   SELECT x * 2 INTO STRICT doubled;
                   INSERT INTO pl_call VALUES (doubled);
                 END
                 $$",
            )
            .await
            .expect("setup");
        session.simple_query("CALL add_pl(7)").await.expect("call");
        let rows = session
            .simple_query("SELECT id FROM pl_call")
            .await
            .expect("select");
        assert_eq!(first_text(&rows), "14");
    }

    #[tokio::test]
    async fn caught_exception_rolls_back_only_its_block() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query(
                "CREATE TABLE pl_exc (id int4 PRIMARY KEY); \
                 DO $$
                 BEGIN
                   INSERT INTO pl_exc VALUES (1);
                   BEGIN
                     INSERT INTO pl_exc VALUES (2);
                     INSERT INTO pl_exc VALUES (1);
                   EXCEPTION WHEN unique_violation THEN
                     INSERT INTO pl_exc VALUES (3);
                   END;
                 END
                 $$",
            )
            .await
            .expect("caught exception");
        let rows = session
            .simple_query("SELECT id FROM pl_exc ORDER BY id")
            .await
            .expect("select");
        let QueryResult::Rows { rows, .. } = &rows[0] else {
            panic!("expected rows");
        };
        let values = rows
            .iter()
            .map(|row| std::str::from_utf8(&row[0].as_ref().unwrap().text).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(values, ["1", "3"]);
    }

    #[tokio::test]
    async fn raise_notice_is_queued_on_the_session_receiver() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        let mut notices = session.take_notices().expect("notice receiver");
        session
            .simple_query("DO $$ BEGIN RAISE NOTICE 'value %', 42; END $$")
            .await
            .expect("notice block");
        let notice = notices.try_recv().expect("notice");
        assert_eq!(notice.code, "00000");
        assert_eq!(notice.message, "value 42");
    }

    #[tokio::test]
    async fn procedure_transaction_control_commits_and_starts_a_new_block() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query(
                "CREATE TABLE pl_tx (id int4); \
                 CREATE PROCEDURE tx_pl() LANGUAGE plpgsql AS $$
                 BEGIN
                   INSERT INTO pl_tx VALUES (1);
                   COMMIT;
                   INSERT INTO pl_tx VALUES (2);
                 END
                 $$; \
                 CALL tx_pl()",
            )
            .await
            .expect("transaction procedure");
        let rows = session
            .simple_query("SELECT count(*) FROM pl_tx")
            .await
            .expect("count");
        assert_eq!(first_text(&rows), "2");
    }

    #[tokio::test]
    async fn scalar_function_runs_once_per_input_row() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        let rows = session
            .simple_query(
                "CREATE TABLE pl_values (n int4); \
                 INSERT INTO pl_values VALUES (1), (2), (3); \
                 CREATE FUNCTION pl_double(x int4) RETURNS int4 LANGUAGE plpgsql AS $$
                 DECLARE result int4;
                 BEGIN
                   result := x * 2;
                   RETURN result;
                 END
                 $$; \
                 SELECT pl_double(n) FROM pl_values ORDER BY n",
            )
            .await
            .expect("row-dependent function");
        let QueryResult::Rows { rows, .. } = rows.last().expect("result") else {
            panic!("expected rows");
        };
        let values = rows
            .iter()
            .map(|row| std::str::from_utf8(&row[0].as_ref().expect("cell").text).expect("utf8"))
            .collect::<Vec<_>>();
        assert!(values == ["2", "4", "6"]);
    }

    #[tokio::test]
    async fn scalar_function_recursion_and_lazy_case_are_bounded_and_correct() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        let rows = session
            .simple_query(
                "CREATE FUNCTION pl_factorial(n int4) RETURNS int4 LANGUAGE plpgsql AS $$
                 BEGIN
                   IF n <= 1 THEN
                     RETURN 1;
                   END IF;
                   RETURN n * pl_factorial(n - 1);
                 END
                 $$; \
                 CREATE FUNCTION pl_boom() RETURNS int4 LANGUAGE plpgsql AS $$
                 BEGIN
                   RETURN 1 / 0;
                 END
                 $$; \
                 SELECT pl_factorial(5), CASE WHEN true THEN 7 ELSE pl_boom() END",
            )
            .await
            .expect("recursive and lazy function");
        let QueryResult::Rows { rows, .. } = rows.last().expect("result") else {
            panic!("expected rows");
        };
        assert!(std::str::from_utf8(&rows[0][0].as_ref().expect("factorial").text) == Ok("120"));
        assert!(std::str::from_utf8(&rows[0][1].as_ref().expect("case").text) == Ok("7"));
    }

    #[tokio::test]
    async fn scalar_function_returns_its_single_out_parameter() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        let rows = session
            .simple_query(
                "CREATE FUNCTION pl_out(x int4, OUT doubled int4) LANGUAGE plpgsql AS $$
                 BEGIN
                   doubled := x * 2;
                 END
                 $$; \
                 SELECT pl_out(9)",
            )
            .await
            .expect("OUT function");
        assert!(first_text(&rows[1..]) == "18");
    }

    #[tokio::test]
    async fn scalar_function_binds_positional_parameters() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        let rows = session
            .simple_query(
                "CREATE FUNCTION pl_multirange(i anyrange) RETURNS anymultirange LANGUAGE plpgsql AS $$
                 BEGIN
                   RETURN multirange($1);
                 END
                 $$; \
                 SELECT pl_multirange(int4range(1, 4))",
            )
            .await
            .expect("positional parameter");
        assert!(first_text(&rows[1..]) == "{[1,4)}");
    }

    #[tokio::test]
    async fn sql_bearing_scalar_functions_share_the_outer_statement_transaction() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query(
                "CREATE TABLE pl_source (id int4 PRIMARY KEY, value int4); \
                 CREATE TABLE pl_log (value int4); \
                 INSERT INTO pl_source VALUES (1, 10), (2, 20); \
                 CREATE FUNCTION pl_lookup(p int4) RETURNS int4 LANGUAGE plpgsql AS $$ \
                 DECLARE answer int4; BEGIN \
                   SELECT value INTO answer FROM pl_source WHERE id = p; \
                   INSERT INTO pl_log VALUES (p); \
                   RETURN answer; \
                 END $$",
            )
            .await
            .expect("setup");

        let rows = session
            .simple_query("SELECT pl_lookup(id) FROM pl_source ORDER BY id")
            .await
            .expect("function query");
        let QueryResult::Rows { rows, .. } = rows.last().expect("result") else {
            panic!("expected rows");
        };
        let values = rows
            .iter()
            .map(|row| std::str::from_utf8(&row[0].as_ref().expect("cell").text).expect("utf8"))
            .collect::<Vec<_>>();
        assert!(values == ["10", "20"]);
        let count = session
            .simple_query("SELECT count(*) FROM pl_log")
            .await
            .expect("count");
        assert!(first_text(&count) == "2");

        let error = session
            .simple_query(
                "SELECT CASE WHEN id = 2 THEN 1 / 0 ELSE pl_lookup(id) END \
                 FROM pl_source ORDER BY id",
            )
            .await
            .expect_err("later row fails");
        assert!(error.code == "22012");
        let count = session
            .simple_query("SELECT count(*) FROM pl_log")
            .await
            .expect("count after rollback");
        assert!(first_text(&count) == "2");
    }

    #[tokio::test]
    async fn sql_bearing_scalar_calls_are_lazy_nested_and_group_aware() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query(
                "CREATE TABLE pl_group_values (id int4); \
                 INSERT INTO pl_group_values VALUES (1), (2), (3); \
                 CREATE FUNCTION pl_max_plus(p int4) RETURNS int4 LANGUAGE plpgsql AS $$ \
                 DECLARE answer int4; BEGIN \
                   SELECT max(id) + p INTO answer FROM pl_group_values; RETURN answer; \
                 END $$; \
                 CREATE FUNCTION pl_nested(p int4) RETURNS int4 LANGUAGE plpgsql AS $$ \
                 BEGIN RETURN pl_max_plus(p) + 1; END $$; \
                 CREATE FUNCTION pl_control(p int4) RETURNS int4 LANGUAGE plpgsql AS $$ \
                 DECLARE answer int4; BEGIN \
                   answer := pl_max_plus(p); \
                   IF pl_max_plus(0) > 0 THEN RETURN answer; END IF; \
                   RETURN 0; \
                 END $$",
            )
            .await
            .expect("setup");
        let nested = session
            .simple_query("SELECT pl_nested(3)")
            .await
            .expect("nested query");
        assert!(first_text(&nested) == "7");
        let control = session
            .simple_query("SELECT pl_control(2)")
            .await
            .expect("assignment and IF query");
        assert!(first_text(&control) == "5");
        let rows = session
            .simple_query(
                "SELECT CASE WHEN false THEN pl_max_plus(max(id)) \
                             ELSE pl_nested(max(id)) END FROM pl_group_values",
            )
            .await
            .expect("nested grouped query");
        assert!(first_text(&rows) == "7");
    }

    #[tokio::test]
    async fn scalar_functions_in_dml_share_its_implicit_transaction() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query(
                "CREATE TABLE pl_dml_target (value int4); \
                 CREATE TABLE pl_dml_log (value int4); \
                 CREATE FUNCTION pl_dml_value(p int4) RETURNS int4 LANGUAGE plpgsql AS $$ \
                 BEGIN INSERT INTO pl_dml_log VALUES (p); RETURN p; END $$; \
                 INSERT INTO pl_dml_target VALUES (pl_dml_value(1))",
            )
            .await
            .expect("successful DML call");
        let count = session
            .simple_query("SELECT count(*) FROM pl_dml_log")
            .await
            .expect("committed side effect");
        assert!(first_text(&count) == "1");

        let error = session
            .simple_query("INSERT INTO pl_dml_target VALUES (pl_dml_value(2)), (1 / 0)")
            .await
            .expect_err("outer insert fails");
        assert!(error.code == "22012");
        let count = session
            .simple_query("SELECT count(*) FROM pl_dml_log")
            .await
            .expect("rolled back side effect");
        assert!(first_text(&count) == "1");
    }

    #[tokio::test]
    async fn integer_for_variables_are_int4_and_bounds_are_range_checked() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        let rows = session
            .simple_query(
                "CREATE FUNCTION loop_pick(x int4) RETURNS text LANGUAGE plpgsql AS $$ \
                 BEGIN RETURN 'int4'; END $$; \
                 CREATE FUNCTION loop_pick(x int8) RETURNS text LANGUAGE plpgsql AS $$ \
                 BEGIN RETURN 'int8'; END $$; \
                 CREATE FUNCTION loop_type() RETURNS text LANGUAGE plpgsql AS $$ \
                 BEGIN FOR i IN 1..1 LOOP RETURN loop_pick(i); END LOOP; END $$; \
                 SELECT loop_type()",
            )
            .await
            .expect("integer loop overload");
        assert!(first_text(&rows[3..]) == "int4");

        let error = session
            .simple_query(
                "DO $$ BEGIN FOR i IN 2147483648::int8..2147483648::int8 LOOP NULL; END LOOP; END $$",
            )
            .await
            .expect_err("int4 loop bound overflow");
        assert!(error.code == "22003");
    }

    #[tokio::test]
    async fn nested_procedure_recursion_is_bounded() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query(
                "CREATE PROCEDURE recursive_call() LANGUAGE plpgsql AS $$ \
                 BEGIN CALL recursive_call(); END $$",
            )
            .await
            .expect("procedure");
        let error = session
            .simple_query("CALL recursive_call()")
            .await
            .expect_err("bounded recursion");
        assert!(error.code == "54001");
    }

    #[tokio::test]
    async fn nested_sql_bearing_function_uses_session_exception_rollback() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        session
            .simple_query(
                "CREATE TABLE nested_sql_guard (id int4 PRIMARY KEY); \
                 INSERT INTO nested_sql_guard VALUES (1); \
                 CREATE FUNCTION nested_sql_inner() RETURNS int4 LANGUAGE plpgsql AS $$ \
                 BEGIN INSERT INTO nested_sql_guard VALUES (1); RETURN 0; END $$; \
                 CREATE FUNCTION nested_sql_outer() RETURNS int4 LANGUAGE plpgsql AS $$ \
                 BEGIN \
                   BEGIN RETURN nested_sql_inner(); \
                   EXCEPTION WHEN unique_violation THEN RETURN 7; END; \
                 END $$",
            )
            .await
            .expect("setup");
        let rows = session
            .simple_query("BEGIN; SELECT nested_sql_outer(); SELECT 1; ROLLBACK")
            .await
            .expect("caught inner error keeps transaction usable");
        assert!(first_text(&rows[1..]) == "7");
        assert!(first_text(&rows[2..]) == "1");
    }

    #[tokio::test]
    async fn record_field_assignment_coerces_to_the_selected_column_type() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        for select in ["1::int4", "NULL::int4"] {
            let error = session
                .simple_query(&format!(
                    "DO $$ DECLARE r record; BEGIN SELECT {select} AS x INTO r; r.x := 'oops'; END $$"
                ))
                .await
                .expect_err("record field cast");
            assert!(error.code == "22P02", "{select}: {error:?}");
        }
    }

    #[tokio::test]
    async fn percent_type_uses_parameter_type() {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        let rows = session
            .simple_query(
                "CREATE FUNCTION array_copy(x anyarray) RETURNS anyarray LANGUAGE plpgsql AS $$ \
                 DECLARE res x%TYPE; BEGIN res := array_fill(x[1], ARRAY[4]); RETURN res; END $$; \
                 CREATE FUNCTION copy_type(value int4) RETURNS int4 LANGUAGE plpgsql AS $$ \
                 DECLARE copy value%TYPE; BEGIN copy := value; RETURN copy; END $$; \
                 SELECT copy_type(7)",
            )
            .await
            .expect("%TYPE function");
        assert!(first_text(&rows[2..]) == "7");
    }
}
