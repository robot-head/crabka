//! F-2: the `pg_catalog` functions `psql`'s `\d` family, ORM preambles and
//! migration tools call.
//!
//! The module covers object definition reconstruction (`pg_get_viewdef`,
//! `pg_get_indexdef`, `pg_get_constraintdef`), identity (`pg_get_userbyid`,
//! `current_schemas`, `pg_backend_pid`), comments (`obj_description` and
//! related functions), sizes, and the `has_*_privilege` family.
//!
//! [`eval`](crate::eval) dispatches the family ahead of the older scalar
//! families, exactly like `json_fn`/`array_fn`. What separates it from those is
//! that most of these functions read the catalog. They reach it through
//! [`EvalCtx::catalog`], which is `None` outside a SQL session, for example in
//! a planning context or a unit test. There they report 0A000 and do not
//! silently answer NULL.

use std::fmt::Write;

use crabka_pgcatalog::{
    CommentObject, ForeignKey, Index, MatchType, ReferentialAction, RelationName, Table, View,
};
use crabka_pgkv::Kv;
use crabka_pgparser::ast::{Expr, FuncCall};
use crabka_pgtypes::{ArrayValue, ColumnType, Datum, ElemType};

use crate::{
    clock::EvalCtx,
    error::ExecError,
    func::{int_arg, require_arity, undefined_function},
    relname::{ResolutionScope, parse_written_relation},
    scope::Scope,
};

/// `pg_class`'s own oid, the `classoid` every relation comment carries.
pub(crate) const PG_CLASS_OID: i32 = 1259;
pub(crate) const PG_CONSTRAINT_OID: i32 = 2606;
pub(crate) const PG_TRIGGER_OID: i32 = 2620;
/// First oid of the band reserved for roles.
pub(crate) const ROLE_OID_BASE: i32 = 100_000;
/// The oid of the bootstrap superuser, which owns every object.
///
/// This is PostgreSQL's own oid for that role, so `pg_get_userbyid(relowner)`
/// needs no special case on the caller's side.
pub(crate) const BOOTSTRAP_ROLE_OID: i32 = 10;
/// The oid of `pg_database_owner`, the implicit role `public` belongs to.
///
/// This is again PostgreSQL's own oid, so a client that compares against 6171
/// matches.
pub(crate) const DATABASE_OWNER_ROLE_OID: i32 = 6171;
/// PostgreSQL's encoding number for UTF-8.
pub(crate) const UTF8_ENCODING: i32 = 6;
/// PostgreSQL's encoding number for EUC-KR.
pub(crate) const EUC_KR_ENCODING: i32 = 3;
/// The role every crabka object is owned by. crabka has no per-object owner
/// column, and the bootstrap superuser is the only role that can create one.
pub(crate) const OBJECT_OWNER: &str = "postgres";

/// Which member of the `pg_get_function*` family a call names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoutineDefKind {
    Definition,
    Arguments,
    IdentityArguments,
    Result,
}

/// The catalog functions, and how their result type is decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CatalogFunc {
    ViewDef,
    IndexDef,
    ConstraintDef,
    TriggerDef,
    Expr,
    UserById,
    SerialSequence,
    /// A definition-reconstruction function for an object kind crabka has none
    /// of. It is always NULL, like PostgreSQL's answer for a missing oid.
    NullDef,
    /// `pg_get_partkeydef(oid)` — the `PARTITION BY` clause that rebuilds a
    /// partitioned relation's key, and NULL for a relation without one.
    PartKeyDef,
    /// P2: the `pg_get_function*` family over the routine catalog.
    RoutineDef(RoutineDefKind),
    IsVisible,
    RelationSize,
    TableSize,
    IndexesSize,
    TotalRelationSize,
    /// A size over a whole database or tablespace, whose argument is a name or
    /// oid of that object and not a relation.
    ClusterSize,
    SizePretty,
    SizeBytes,
    ObjDescription,
    ColDescription,
    ShobjDescription,
    CurrentSchemas,
    BackendPid,
    StartTime,
    HasPrivilege,
    HasRole,
    EncodingToChar,
    CharToEncoding,
    IsPublishable,
    /// `pg_relation_is_updatable` and `pg_column_is_updatable`, the pair
    /// `information_schema` derives its updatability columns from.
    RelationIsUpdatable,
    ColumnIsUpdatable,
    InRecovery,
    TablespaceLocation,
    TriggerDepth,
    EventRewriteOid,
    EventRewriteReason,
    NotificationQueueUsage,
}

/// Classify a lowercased function name.
fn catalog_func(name: &str) -> Option<CatalogFunc> {
    use CatalogFunc::{
        BackendPid, CharToEncoding, ClusterSize, ColDescription, ConstraintDef, CurrentSchemas,
        EncodingToChar, Expr as ExprDef, HasPrivilege, HasRole, InRecovery, IndexDef, IndexesSize,
        IsPublishable, IsVisible, NullDef, ObjDescription, RelationSize, RoutineDef,
        SerialSequence, ShobjDescription, SizeBytes, SizePretty, StartTime, TableSize,
        TablespaceLocation, TotalRelationSize, TriggerDef, TriggerDepth, UserById, ViewDef,
    };
    Some(match name {
        "pg_get_viewdef" => ViewDef,
        "pg_get_indexdef" => IndexDef,
        "pg_get_constraintdef" => ConstraintDef,
        "pg_get_expr" => ExprDef,
        "pg_get_userbyid" => UserById,
        "pg_get_serial_sequence" => SerialSequence,
        "pg_get_functiondef" => RoutineDef(RoutineDefKind::Definition),
        "pg_get_function_arguments" => RoutineDef(RoutineDefKind::Arguments),
        "pg_get_function_identity_arguments" => RoutineDef(RoutineDefKind::IdentityArguments),
        "pg_get_function_result" => RoutineDef(RoutineDefKind::Result),
        "pg_get_triggerdef" => TriggerDef,
        "pg_get_partkeydef" => CatalogFunc::PartKeyDef,
        "pg_get_ruledef" | "pg_get_statisticsobjdef" | "pg_get_statisticsobjdef_columns" => NullDef,
        // `pg_table_is_visible` belongs to the same family but is dispatched
        // through [`crate::func`]; every other member lands here. Both routes
        // evaluate through [`crate::visibility`].
        "pg_type_is_visible"
        | "pg_function_is_visible"
        | "pg_opclass_is_visible"
        | "pg_opfamily_is_visible"
        | "pg_operator_is_visible"
        | "pg_collation_is_visible"
        | "pg_conversion_is_visible"
        | "pg_statistics_obj_is_visible"
        | "pg_ts_config_is_visible"
        | "pg_ts_dict_is_visible"
        | "pg_ts_parser_is_visible"
        | "pg_ts_template_is_visible" => IsVisible,
        "pg_relation_size" => RelationSize,
        "pg_table_size" => TableSize,
        "pg_indexes_size" => IndexesSize,
        "pg_total_relation_size" => TotalRelationSize,
        "pg_database_size" | "pg_tablespace_size" => ClusterSize,
        "pg_size_pretty" => SizePretty,
        "pg_size_bytes" => SizeBytes,
        "obj_description" => ObjDescription,
        "col_description" => ColDescription,
        "shobj_description" => ShobjDescription,
        "current_schemas" => CurrentSchemas,
        "pg_backend_pid" => BackendPid,
        "pg_postmaster_start_time" | "pg_conf_load_time" => StartTime,
        "pg_encoding_to_char" => EncodingToChar,
        "pg_char_to_encoding" => CharToEncoding,
        "pg_relation_is_publishable" => IsPublishable,
        "pg_relation_is_updatable" => CatalogFunc::RelationIsUpdatable,
        "pg_column_is_updatable" => CatalogFunc::ColumnIsUpdatable,
        "pg_is_in_recovery" => InRecovery,
        "pg_tablespace_location" => TablespaceLocation,
        "pg_trigger_depth" => TriggerDepth,
        "pg_event_trigger_table_rewrite_oid" => CatalogFunc::EventRewriteOid,
        "pg_event_trigger_table_rewrite_reason" => CatalogFunc::EventRewriteReason,
        "pg_notification_queue_usage" => CatalogFunc::NotificationQueueUsage,
        _ if is_privilege_func(name) => HasPrivilege,
        "pg_has_role" => HasRole,
        _ => return None,
    })
}

/// The `has_<objectkind>_privilege` family, which shares one entry point.
///
/// Three of them answer for real: `has_table_privilege`,
/// `has_any_column_privilege` and `has_column_privilege` resolve the relation
/// and consult its ACL, because those are the shapes
/// [`RelationPrivilegeCall::of`] recognises. **The other eleven still return
/// `true` unconditionally** — no privilege is stored for a database, schema,
/// sequence, function, language, server, foreign-data wrapper, tablespace,
/// type or parameter, so there is nothing to consult.
///
/// Row-level security refuses a policy qual naming *any* of the fourteen. For
/// the eleven that is the original reason: the qual would admit every row to
/// every role instead of the subset it appears to describe. For the three it
/// is no longer that — they answer correctly — but the refusal is kept because
/// a qual whose meaning turns on an ACL is a second security surface behind
/// the policy, and admitting it needs its own argument rather than this one.
pub(crate) const PRIVILEGE_FUNCTIONS: [&str; 14] = [
    "has_table_privilege",
    "has_column_privilege",
    "has_any_column_privilege",
    "has_database_privilege",
    "has_schema_privilege",
    "has_sequence_privilege",
    "has_function_privilege",
    "has_language_privilege",
    "has_server_privilege",
    "has_foreign_data_wrapper_privilege",
    "has_tablespace_privilege",
    "has_type_privilege",
    "has_parameter_privilege",
    "has_largeobject_privilege",
];

fn is_privilege_func(name: &str) -> bool {
    PRIVILEGE_FUNCTIONS.contains(&name)
}

/// Is `name` one of this family's functions?
///
/// This is the dispatch point in [`eval`](crate::eval).
pub(crate) fn is_catalog_func(name: &str) -> bool {
    catalog_func(name).is_some()
}

/// Statically infer a catalog call's result type, and validate the arity.
///
/// # Errors
///
/// 42883 for an unknown name or a bad arity.
pub(crate) fn catalog_func_result_type(
    fc: &FuncCall,
    scope: &Scope,
) -> Result<ColumnType, ExecError> {
    use CatalogFunc::{
        BackendPid, CharToEncoding, ClusterSize, CurrentSchemas, HasPrivilege, HasRole, InRecovery,
        IndexesSize, IsPublishable, IsVisible, RelationSize, SizeBytes, SizePretty, StartTime,
        TableSize, TotalRelationSize,
    };
    let f = catalog_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = crate::func::checked_args(fc)?;
    require_arity(fc, arity_ok(f, args.len()))?;
    if f == SizeBytes
        && !crate::func::is_unknown_arg(&args[0])
        && !crate::eval::infer_type(&args[0], scope)?.is_string()
    {
        return Err(crate::func::undefined_function_spelled(
            &fc.name, args, scope,
        ));
    }
    if f == SizePretty {
        if crate::func::is_unknown_arg(&args[0]) {
            return Err(crate::func::ambiguous_function(&fc.name, 1));
        }
        let ty = crate::eval::infer_type(&args[0], scope)?;
        let resolved = ty.storage_type();
        if matches!(resolved, ColumnType::Int2 | ColumnType::Int4) {
            return Err(ExecError::FunctionError {
                sqlstate: "42725",
                message: format!("function {}({}) is not unique", fc.name, ty.name()),
            });
        }
        if !matches!(resolved, ColumnType::Int8 | ColumnType::Numeric(_)) {
            return Err(crate::func::undefined_function_spelled(
                &fc.name, args, scope,
            ));
        }
    }
    Ok(match f {
        IsVisible
        | HasPrivilege
        | HasRole
        | IsPublishable
        | InRecovery
        | CatalogFunc::ColumnIsUpdatable => ColumnType::Bool,
        RelationSize | TableSize | IndexesSize | TotalRelationSize | ClusterSize | SizeBytes => {
            ColumnType::Int8
        }
        BackendPid
        | CharToEncoding
        | CatalogFunc::TriggerDepth
        | CatalogFunc::EventRewriteOid
        | CatalogFunc::EventRewriteReason
        | CatalogFunc::RelationIsUpdatable => ColumnType::Int4,
        CatalogFunc::NotificationQueueUsage => ColumnType::Float8,
        StartTime => ColumnType::Timestamptz,
        CurrentSchemas => ColumnType::Array(ElemType::Text),
        _ => ColumnType::Text,
    })
}

/// The accepted argument counts, per function.
fn arity_ok(f: CatalogFunc, n: usize) -> bool {
    use CatalogFunc::{
        BackendPid, CharToEncoding, ClusterSize, ColDescription, ConstraintDef, CurrentSchemas,
        EncodingToChar, Expr as ExprDef, HasPrivilege, HasRole, InRecovery, IndexDef, IndexesSize,
        IsPublishable, IsVisible, NullDef, ObjDescription, RelationSize, SerialSequence,
        ShobjDescription, SizeBytes, SizePretty, StartTime, TableSize, TablespaceLocation,
        TotalRelationSize, UserById, ViewDef,
    };
    match f {
        ViewDef | ConstraintDef | CatalogFunc::TriggerDef | NullDef => n == 1 || n == 2,
        CatalogFunc::RoutineDef(_) => n == 1,
        IndexDef => n == 1 || n == 3,
        ExprDef => n == 2 || n == 3,
        UserById
        | IsVisible
        | SizePretty
        | SizeBytes
        | CurrentSchemas
        | EncodingToChar
        | CharToEncoding
        | IsPublishable
        | TablespaceLocation
        | TableSize
        | IndexesSize
        | TotalRelationSize
        | CatalogFunc::PartKeyDef => n == 1,
        SerialSequence | ColDescription | ShobjDescription | HasRole => n == 2 || n == 3,
        CatalogFunc::RelationIsUpdatable => n == 2,
        CatalogFunc::ColumnIsUpdatable => n == 3,
        ObjDescription | RelationSize => n == 1 || n == 2,
        ClusterSize => n == 1,
        BackendPid
        | StartTime
        | InRecovery
        | CatalogFunc::TriggerDepth
        | CatalogFunc::EventRewriteOid
        | CatalogFunc::EventRewriteReason => n == 0,
        CatalogFunc::NotificationQueueUsage => n == 0,
        HasPrivilege => (1..=4).contains(&n),
    }
}

/// Evaluate a catalog call.
///
/// # Errors
///
/// 42883 for an unknown name or bad arity, 42P01 for a missing relation, 22023
/// for an unrecognized privilege name, and 0A000 when no catalog is reachable.
pub(crate) fn eval_catalog(
    fc: &FuncCall,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let f = catalog_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = crate::func::checked_args(fc)?;
    require_arity(fc, arity_ok(f, args.len()))?;
    let vals: Vec<Datum> = args.iter().map(&mut eval_child).collect::<Result<_, _>>()?;
    eval_resolved(f, &fc.name, &vals, ctx)
}

/// Evaluate a catalog call whose arguments are already values.
fn eval_resolved(
    f: CatalogFunc,
    name: &str,
    vals: &[Datum],
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    use CatalogFunc::{
        BackendPid, CharToEncoding, ClusterSize, CurrentSchemas, EncodingToChar, HasPrivilege,
        HasRole, InRecovery, IsPublishable, IsVisible, NullDef, SizeBytes, SizePretty, StartTime,
        TablespaceLocation,
    };
    match f {
        NullDef => Ok(Datum::Null),
        TablespaceLocation => tablespace_location(&vals[0], ctx),
        IsVisible => crate::visibility::is_visible(
            crate::visibility::Catalog::for_function(name)
                .ok_or_else(|| undefined_function(name))?,
            &vals[0],
            ctx,
        ),
        IsPublishable => Ok(publishable_answer(&vals[0])),
        InRecovery => Ok(Datum::Bool(false)),
        // A whole-database or whole-tablespace size names something that is not
        // a relation, so its argument is taken as given rather than resolved.
        ClusterSize => Ok(cluster_size(&vals[0])),
        BackendPid => Ok(Datum::Int4(ctx.backend_pid)),
        CatalogFunc::TriggerDepth => Ok(Datum::Int4(
            i32::try_from(ctx.trigger_depth).unwrap_or(i32::MAX),
        )),
        CatalogFunc::EventRewriteOid | CatalogFunc::EventRewriteReason => {
            let Some((oid, reason)) = ctx.event_trigger.as_ref().and_then(|event| event.rewrite)
            else {
                return Err(ExecError::FunctionError {
                    sqlstate: "39P03",
                    message: format!(
                        "{name}() can only be called in a table_rewrite event trigger"
                    ),
                });
            };
            Ok(Datum::Int4(if matches!(f, CatalogFunc::EventRewriteOid) {
                oid
            } else {
                reason
            }))
        }
        // Notification queues are not implemented, so their occupied fraction
        // is exactly zero. Keep the function volatile: that remains correct if
        // queue state is added later.
        CatalogFunc::NotificationQueueUsage => Ok(Datum::Float8(0.0)),
        StartTime => Ok(Datum::Timestamptz(process_start_time())),
        CurrentSchemas => current_schemas(&vals[0], ctx),
        EncodingToChar => Ok(encoding_to_char(&vals[0])),
        CharToEncoding => Ok(char_to_encoding(&vals[0])),
        SizePretty => size_pretty(&vals[0]),
        SizeBytes => size_bytes(&vals[0]),
        HasPrivilege => has_privilege(name, vals, ctx),
        HasRole => Ok(Datum::Bool(true)),
        _ => eval_catalog_reading(f, vals, ctx),
    }
}

fn tablespace_location(value: &Datum, ctx: &EvalCtx) -> Result<Datum, ExecError> {
    let oid = match value {
        Datum::Int4(oid) => *oid as u32,
        Datum::Int8(oid) => u32::try_from(*oid).map_err(|_| {
            ExecError::TypeMismatch("pg_tablespace_location expects a tablespace oid".into())
        })?,
        Datum::Null => return Ok(Datum::Null),
        _ => {
            return Err(ExecError::TypeMismatch(
                "pg_tablespace_location expects a tablespace oid".into(),
            ));
        }
    };
    if oid == crate::catalog_rel::DEFAULT_TABLESPACE_OID as u32 || oid == 1664 {
        return Ok(Datum::Text(String::new()));
    }
    let kv = ctx.catalog().ok_or_else(|| {
        ExecError::Unsupported("pg_tablespace_location requires a catalog".into())
    })?;
    Ok(crabka_pgcatalog::list_tablespaces(kv)?
        .into_iter()
        .find(|tablespace| tablespace.oid == oid)
        .map_or(Datum::Null, |tablespace| {
            Datum::Text(if tablespace.location.is_empty() {
                format!("pg_tblspc/{oid}")
            } else {
                tablespace.location
            })
        }))
}

/// The half of the family that reads the catalog.
fn eval_catalog_reading(f: CatalogFunc, vals: &[Datum], ctx: &EvalCtx) -> Result<Datum, ExecError> {
    use CatalogFunc::{
        ColDescription, ConstraintDef, Expr as ExprDef, IndexDef, IndexesSize, ObjDescription,
        RelationSize, SerialSequence, ShobjDescription, TableSize, TotalRelationSize, UserById,
        ViewDef,
    };
    let kv = ctx.catalog().ok_or_else(catalog_unavailable)?;
    let data_kv = ctx.data().unwrap_or(kv);
    // Every function here that takes a relation *name* resolves it the way the
    // session resolves one, so `pg_get_viewdef('v')` sees what `SELECT … FROM v`
    // sees.
    let scope = ctx.resolution();
    match f {
        ViewDef => view_def(kv, scope, vals, ctx.output_style()),
        // `pg_get_indexdef(oid, colno, pretty)` — the column number selects one
        // key expression and is not supported, but `pretty` changes the
        // relation's spelling and psql's `\d` passes it.
        IndexDef => index_def(
            kv,
            scope,
            &vals[0],
            matches!(vals.get(2), Some(Datum::Bool(true))),
        ),
        ConstraintDef => constraint_def(kv, scope, &vals[0]),
        CatalogFunc::PartKeyDef => part_key_def(kv, scope, &vals[0]),
        CatalogFunc::TriggerDef => trigger_def(kv, &vals[0]),
        // Every catalog column this reaches already holds the text that column
        // is supposed to report — `polqual` deparsed by its projection, a
        // default or `CHECK` predicate as stored — so "decompiling" is the
        // identity here. A column that starts needing deparsing must do it in
        // its own projection, not here: this call has only the value, and the
        // relation argument it would need is nothing but an oid.
        ExprDef => Ok(vals[0].clone()),
        UserById => user_by_id(kv, &vals[0]),
        SerialSequence => serial_sequence(kv, scope, vals),
        RelationSize => relation_size_with_fork(kv, data_kv, scope, vals),
        TableSize => table_size(kv, data_kv, scope, &vals[0]),
        IndexesSize => indexes_size(kv, data_kv, scope, &vals[0]),
        TotalRelationSize => total_relation_size(kv, data_kv, scope, &vals[0]),
        ObjDescription => description(kv, scope, &vals[0], 0),
        ColDescription => {
            let subid = i32::try_from(int_arg(&vals[1])?)
                .map_err(|_| ExecError::Unsupported("column number out of range".into()))?;
            description(kv, scope, &vals[0], subid)
        }
        // `shobj_description` covers shared objects (roles, databases,
        // tablespaces); crabka carries no comments on those.
        ShobjDescription => Ok(Datum::Null),
        CatalogFunc::RoutineDef(kind) => routine_def(kv, kind, &vals[0]),
        CatalogFunc::RelationIsUpdatable => relation_is_updatable(kv, scope, vals),
        CatalogFunc::ColumnIsUpdatable => column_is_updatable(kv, scope, vals),
        _ => Ok(Datum::Null),
    }
}

// ------------------------------------------------------------- updatability

/// `pg_relation_is_updatable(regclass, boolean)` — the bitmask of write commands
/// a relation admits, as [`crate::viewwrite`] computes it.
///
/// The function is strict, and an oid no relation answers to reports 0 rather
/// than failing: `999999::regclass` is a legal `regclass` value, so PostgreSQL
/// reaches the function body and finds nothing to open. A name, by contrast,
/// goes through the `regclass` input function and raises 42P01 for a relation
/// the search path cannot reach — which is what [`resolve_relation_oid`] does.
fn relation_is_updatable(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    vals: &[Datum],
) -> Result<Datum, ExecError> {
    if vals.iter().any(|value| matches!(value, Datum::Null)) {
        return Ok(Datum::Null);
    }
    let include_triggers = flag_arg(&vals[1])?;
    let oid = resolve_relation_oid(kv, scope, &vals[0])?;
    let Some(name) = updatable_relation_name(kv, oid)? else {
        return Ok(Datum::Int4(0));
    };
    Ok(Datum::Int4(crate::viewwrite::relation_updatable_events(
        kv,
        &name,
        include_triggers,
        None,
        0,
    )))
}

/// `pg_column_is_updatable(regclass, smallint, boolean)` — whether one column
/// can be assigned to through the relation.
///
/// A system column is never updatable, which is the `attnum <= 0` guard; a
/// column past the end of a *table* still answers true, because PostgreSQL
/// settles a table before it ever looks at the column number.
///
/// `include_triggers` does change the answer, and is honoured: an `INSTEAD OF`
/// trigger can supply the `UPDATE` half of the `UPDATE | DELETE` this predicate
/// requires, which an unassignable column cannot supply for itself.
fn column_is_updatable(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    vals: &[Datum],
) -> Result<Datum, ExecError> {
    if vals.iter().any(|value| matches!(value, Datum::Null)) {
        return Ok(Datum::Null);
    }
    let include_triggers = flag_arg(&vals[2])?;
    let attnum = int_arg(&vals[1])?;
    let Ok(attnum) = i32::try_from(attnum) else {
        return Ok(Datum::Bool(false));
    };
    let oid = resolve_relation_oid(kv, scope, &vals[0])?;
    let Some(name) = updatable_relation_name(kv, oid)? else {
        return Ok(Datum::Bool(false));
    };
    Ok(Datum::Bool(crate::viewwrite::column_is_updatable(
        kv,
        &name,
        attnum,
        include_triggers,
    )))
}

/// The `include_triggers` flag both predicates take.
fn flag_arg(value: &Datum) -> Result<bool, ExecError> {
    match value {
        Datum::Bool(flag) => Ok(*flag),
        other => Err(ExecError::TypeMismatch(format!(
            "updatability test expects a boolean, not {}",
            other.column_type().map_or("unknown", ColumnType::name)
        ))),
    }
}

/// The catalog name behind a relation oid, for the two kinds the updatability
/// analysis can say anything about. A sequence, an index, or a materialized
/// view resolves to `None`, which both predicates report as "not updatable"
/// exactly as PostgreSQL does for a relkind it has no rewrite rules for — and a
/// materialized view genuinely has none, which is why `INSERT` into one is
/// 42809 rather than a rewrite.
fn updatable_relation_name(kv: &dyn Kv, oid: i32) -> Result<Option<RelationName>, ExecError> {
    for table in crabka_pgcatalog::list_tables(kv)? {
        if crate::catalog_rel::table_relation_oid(table.id)? == oid {
            if table.materialized.is_some() {
                return Ok(None);
            }
            return Ok(Some(table.name));
        }
    }
    Ok(crate::catalog_rel::view_oids(kv)?
        .into_iter()
        .find_map(|(name, view_oid)| (view_oid == oid).then_some(name)))
}

fn trigger_def(kv: &dyn Kv, reference: &Datum) -> Result<Datum, ExecError> {
    let Ok(oid) = u32::try_from(int_arg(reference)?) else {
        return Ok(Datum::Null);
    };
    let Some(trigger) = crabka_pgcatalog::trigger::list_triggers(kv)?
        .into_iter()
        .find(|trigger| trigger.oid == oid)
    else {
        return Ok(Datum::Null);
    };
    use crabka_pgcatalog::trigger::{TriggerLevel, TriggerTiming};
    let mut sql = if trigger.constraint {
        format!(
            "CREATE CONSTRAINT TRIGGER {}",
            quote_identifier(&trigger.name)
        )
    } else {
        format!("CREATE TRIGGER {}", quote_identifier(&trigger.name))
    };
    sql.push(' ');
    sql.push_str(match trigger.timing {
        TriggerTiming::Before => "BEFORE",
        TriggerTiming::After => "AFTER",
        TriggerTiming::InsteadOf => "INSTEAD OF",
    });
    let mut events = Vec::new();
    if trigger.events.insert {
        events.push("INSERT".into());
    }
    if trigger.events.delete {
        events.push("DELETE".into());
    }
    if trigger.events.update {
        let columns = trigger
            .events
            .update_columns
            .iter()
            .map(|column| quote_identifier(column))
            .collect::<Vec<_>>();
        events.push(if columns.is_empty() {
            "UPDATE".into()
        } else {
            format!("UPDATE OF {}", columns.join(", "))
        });
    }
    if trigger.events.truncate {
        events.push("TRUNCATE".into());
    }
    sql.push(' ');
    sql.push_str(&events.join(" OR "));
    let _ = write!(
        sql,
        " ON {}.{}",
        quote_identifier(crabka_pgcatalog::displayed_schema(&trigger.table.schema)),
        quote_identifier(&trigger.table.name)
    );
    if let Some(referenced) = trigger.referenced_table_id
        && let Ok(table) = crabka_pgcatalog::table_by_id(kv, referenced)
    {
        let _ = write!(
            sql,
            " FROM {}.{}",
            quote_identifier(crabka_pgcatalog::displayed_schema(&table.name.schema)),
            quote_identifier(&table.name.name)
        );
    }
    if trigger.deferrable {
        sql.push_str(" DEFERRABLE");
    } else if trigger.constraint {
        sql.push_str(" NOT DEFERRABLE");
    }
    if trigger.initially_deferred {
        sql.push_str(" INITIALLY DEFERRED");
    } else if trigger.constraint {
        sql.push_str(" INITIALLY IMMEDIATE");
    }
    if trigger.old_transition.is_some() || trigger.new_transition.is_some() {
        sql.push_str(" REFERENCING");
        if let Some(name) = &trigger.old_transition {
            let _ = write!(sql, " OLD TABLE AS {}", quote_identifier(name));
        }
        if let Some(name) = &trigger.new_transition {
            let _ = write!(sql, " NEW TABLE AS {}", quote_identifier(name));
        }
    }
    sql.push_str(match trigger.level {
        TriggerLevel::Row => " FOR EACH ROW",
        TriggerLevel::Statement => " FOR EACH STATEMENT",
    });
    if let Some(predicate) = &trigger.when {
        let _ = write!(sql, " WHEN ({predicate})");
    }
    let args = trigger
        .arguments
        .iter()
        .map(|arg| format!("'{}'", arg.replace(char::from(39), "''")))
        .collect::<Vec<_>>()
        .join(", ");
    let function =
        crate::routine::routine_by_oid(kv, i32::try_from(trigger.function_oid).unwrap_or(0))?
            .map_or(trigger.function, |routine| routine.name);
    let _ = write!(sql, " EXECUTE FUNCTION {function}({args})");
    Ok(Datum::Text(sql))
}

/// The `pg_get_function*` family.
///
/// A reference that resolves to no routine is NULL, exactly as `PostgreSQL`
/// answers for an oid that no longer exists.
fn routine_def(kv: &dyn Kv, kind: RoutineDefKind, reference: &Datum) -> Result<Datum, ExecError> {
    let Some(routine) = crate::routine::routine_by_reference(kv, reference)? else {
        return Ok(Datum::Null);
    };
    Ok(match kind {
        RoutineDefKind::Definition => Datum::Text(crate::routine::render_functiondef(&routine)),
        RoutineDefKind::Arguments => Datum::Text(crate::routine::render_arguments(&routine)),
        RoutineDefKind::IdentityArguments => {
            Datum::Text(crate::routine::render_identity_arguments(&routine))
        }
        RoutineDefKind::Result => {
            crate::routine::render_result(&routine).map_or(Datum::Null, Datum::Text)
        }
    })
}

fn catalog_unavailable() -> ExecError {
    ExecError::Unsupported("catalog functions require a SQL session".into())
}

/// `pg_relation_is_publishable(oid)`. crabka publishes every relation, so the
/// answer is true for any non-NULL oid and NULL for a NULL one.
fn publishable_answer(oid: &Datum) -> Datum {
    match oid {
        Datum::Null => Datum::Null,
        _ => Datum::Bool(true),
    }
}

/// When this process started, as `pg_postmaster_start_time()` reports it.
///
/// This function reads the kernel instead of a value captured on the first
/// call. A lazily captured instant is LATER than the statement's `now()` and
/// would make every uptime query report a negative age. On a host where the
/// kernel cannot answer, for example a non-Linux host, the first call's instant
/// is the fallback.
fn process_start_time() -> jiff::Timestamp {
    static START: std::sync::OnceLock<jiff::Timestamp> = std::sync::OnceLock::new();
    *START.get_or_init(|| {
        boot_relative_start_time().unwrap_or_else(|| {
            jiff::Timestamp::from_microsecond(jiff::Timestamp::now().as_microsecond())
                .unwrap_or(jiff::Timestamp::UNIX_EPOCH)
        })
    })
}

/// This process's start instant from `/proc`.
///
/// The instant is the boot time in `/proc/stat` plus this process's start
/// offset, which is field 22 of `/proc/self/stat`, in clock ticks. The result
/// is `None` on any host that does not publish both.
fn boot_relative_start_time() -> Option<jiff::Timestamp> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // The second field is the comm name in parentheses and may contain spaces,
    // so the fields are counted from after the closing parenthesis.
    let tail = stat.rsplit_once(')')?.1;
    let ticks: i64 = tail.split_whitespace().nth(19)?.parse().ok()?;
    let boot = std::fs::read_to_string("/proc/stat").ok()?;
    let btime: i64 = boot
        .lines()
        .find_map(|line| line.strip_prefix("btime "))?
        .trim()
        .parse()
        .ok()?;
    // The user-space clock tick is 100 Hz on every Linux crabka targets.
    jiff::Timestamp::from_second(btime + ticks / 100).ok()
}

/// `current_schemas(include_implicit)`, the session's search path as an array.
///
/// The array is in path order and holds only the entries that name an existing
/// schema.
///
/// `include_implicit` adds the two entries PostgreSQL prepends when the path
/// does not already name them: the session's temporary namespace, and then
/// `pg_catalog`. The behavior below was verified against PostgreSQL 18.4. Under
/// the default path `current_schemas(false)` is `{public}` and
/// `current_schemas(true)` is `{pg_catalog,public}`. After a
/// `CREATE TEMP TABLE` the second one becomes `{pg_temp_1,pg_catalog,public}`.
/// After `SET search_path = pg_catalog, public` both are `{pg_catalog,public}`.
/// An entry that names no existing schema is skipped and not reported. The
/// function is strict, so a NULL argument answers NULL.
fn current_schemas(include_implicit: &Datum, ctx: &EvalCtx) -> Result<Datum, ExecError> {
    if matches!(include_implicit, Datum::Null) {
        return Ok(Datum::Null);
    }
    let kv = ctx.catalog().ok_or_else(catalog_unavailable)?;
    let schemas = if matches!(include_implicit, Datum::Bool(true)) {
        ctx.resolution().visible_schemas(kv)?
    } else {
        ctx.resolution().explicit_schemas(kv)?
    };
    Ok(Datum::Array(ArrayValue::new(
        ElemType::Text,
        schemas.into_iter().map(Datum::Text).collect(),
    )))
}

/// PostgreSQL 18.4's stable encoding-id table. The engine stores text as UTF-8,
/// but catalog inspection still needs every PostgreSQL encoding identity.
const ENCODING_NAMES: &[&str] = &[
    "SQL_ASCII",
    "EUC_JP",
    "EUC_CN",
    "EUC_KR",
    "EUC_TW",
    "EUC_JIS_2004",
    "UTF8",
    "MULE_INTERNAL",
    "LATIN1",
    "LATIN2",
    "LATIN3",
    "LATIN4",
    "LATIN5",
    "LATIN6",
    "LATIN7",
    "LATIN8",
    "LATIN9",
    "LATIN10",
    "WIN1256",
    "WIN1258",
    "WIN866",
    "WIN874",
    "KOI8R",
    "WIN1251",
    "WIN1252",
    "ISO_8859_5",
    "ISO_8859_6",
    "ISO_8859_7",
    "ISO_8859_8",
    "WIN1250",
    "WIN1253",
    "WIN1254",
    "WIN1255",
    "WIN1257",
    "KOI8U",
    "SJIS",
    "BIG5",
    "GBK",
    "UHC",
    "GB18030",
    "JOHAB",
    "SHIFT_JIS_2004",
];

fn encoding_to_char(encoding: &Datum) -> Datum {
    match int_arg(encoding) {
        Ok(n) => usize::try_from(n)
            .ok()
            .and_then(|index| ENCODING_NAMES.get(index))
            .map_or_else(
                || Datum::Text(String::new()),
                |name| Datum::Text((*name).to_string()),
            ),
        // PostgreSQL answers the empty string for an out-of-range encoding id.
        Err(_) => Datum::Null,
    }
}

fn char_to_encoding(name: &Datum) -> Datum {
    match name {
        Datum::Text(text) => encoding_id(text).map_or(Datum::Int4(-1), Datum::Int4),
        _ => Datum::Null,
    }
}

pub(crate) fn encoding_id(name: &str) -> Option<i32> {
    let mut normalized = name
        .bytes()
        .filter(u8::is_ascii_alphanumeric)
        .map(|byte| byte.to_ascii_lowercase())
        .collect::<Vec<_>>();
    if normalized == b"unicode" {
        return Some(UTF8_ENCODING);
    }
    if normalized.starts_with(b"windows") {
        normalized.splice(..7, b"win".iter().copied());
    } else if normalized.starts_with(b"cp") {
        normalized.splice(..2, b"win".iter().copied());
    } else if normalized == b"shiftjis" {
        normalized = b"sjis".to_vec();
    }
    ENCODING_NAMES
        .iter()
        .position(|candidate| {
            candidate
                .bytes()
                .filter(u8::is_ascii_alphanumeric)
                .map(|byte| byte.to_ascii_lowercase())
                .eq(normalized.iter().copied())
        })
        .and_then(|index| i32::try_from(index).ok())
}

/// `pg_size_pretty(bigint)`, byte for byte as PostgreSQL formats it: the value
/// is divided one unit at a time, keeping one extra bit so the final halving
/// rounds rather than truncates.
fn size_pretty(size: &Datum) -> Result<Datum, ExecError> {
    let mut value = match size {
        Datum::Null => return Ok(Datum::Null),
        Datum::Numeric(value) => return size_pretty_numeric(value),
        Datum::Int8(value) => *value,
        Datum::Int2(_) | Datum::Int4(_) => {
            return Err(crate::func::ambiguous_function("pg_size_pretty", 1));
        }
        _ => {
            return Err(ExecError::UndefinedFunction(format!(
                "function pg_size_pretty({}) does not exist",
                size.column_type().map_or("unknown", ColumnType::name)
            )));
        }
    };
    let limit = 10 * 1024;
    let limit2 = limit * 2 - 1;
    if value.unsigned_abs() < limit {
        return Ok(Datum::Text(format!("{value} bytes")));
    }
    for unit in ["kB", "MB", "GB", "TB", "PB"] {
        value /= if unit == "kB" { 1 << 9 } else { 1 << 10 };
        if value.unsigned_abs() < limit2 || unit == "PB" {
            // PostgreSQL's `half_rounded`: the extra bit is halved away with
            // ties rounded AWAY from zero, so -20 half-units is -10, not -9.
            let rounded = i64::midpoint(value, if value < 0 { -1 } else { 1 });
            return Ok(Datum::Text(format!("{rounded} {unit}")));
        }
    }
    Ok(Datum::Text(format!("{value} bytes")))
}

fn size_pretty_numeric(value: &crabka_pgtypes::numeric::NumericValue) -> Result<Datum, ExecError> {
    use crabka_pgtypes::numeric;

    let mut value = value.clone();
    let zero = numeric::from_i64(0);
    let one = numeric::from_i64(1);
    let two = numeric::from_i64(2);
    let units = [
        ("bytes", 10 * 1024_i64, false, 512_i64),
        ("kB", 20 * 1024 - 1, true, 1024),
        ("MB", 20 * 1024 - 1, true, 1024),
        ("GB", 20 * 1024 - 1, true, 1024),
        ("TB", 20 * 1024 - 1, true, 1024),
        ("PB", 20 * 1024 - 1, true, 1),
    ];
    for (index, (unit, limit, round, divisor)) in units.into_iter().enumerate() {
        if index + 1 == units.len() || numeric::abs(&value) < numeric::from_i64(limit) {
            if round {
                value = if value >= zero {
                    numeric::add(&value, &one)
                } else {
                    numeric::sub(&value, &one)
                };
                value = numeric::div_trunc(&value, &two)?;
            }
            return Ok(Datum::Text(format!("{} {unit}", numeric::to_text(&value))));
        }
        value = numeric::div_trunc(&value, &numeric::from_i64(divisor))?;
    }
    unreachable!("the final size unit always returns")
}

/// `pg_size_bytes(text)`: scan the same decimal grammar as PostgreSQL, apply
/// its case-insensitive binary unit, then use the numeric layer's half-away-
/// from-zero bigint conversion.
fn size_bytes(size: &Datum) -> Result<Datum, ExecError> {
    let input = match size {
        Datum::Null => return Ok(Datum::Null),
        Datum::Text(input) => input,
        other => {
            let ty = other.column_type().map_or("unknown", ColumnType::name);
            return Err(ExecError::UndefinedFunction(format!(
                "function pg_size_bytes({ty}) does not exist"
            )));
        }
    };
    let bytes = input.as_bytes();
    let mut number_start = 0;
    while bytes.get(number_start).is_some_and(u8::is_ascii_whitespace) {
        number_start += 1;
    }

    let mut number_end = number_start;
    if matches!(bytes.get(number_end), Some(b'+' | b'-')) {
        number_end += 1;
    }
    let mut have_digits = false;
    while bytes.get(number_end).is_some_and(u8::is_ascii_digit) {
        have_digits = true;
        number_end += 1;
    }
    if bytes.get(number_end) == Some(&b'.') {
        number_end += 1;
        while bytes.get(number_end).is_some_and(u8::is_ascii_digit) {
            have_digits = true;
            number_end += 1;
        }
    }
    if !have_digits {
        return Err(invalid_size(input));
    }

    // PostgreSQL treats `E` as the start of an exponent only when at least one
    // exponent digit follows; otherwise it remains part of the unit text.
    if matches!(bytes.get(number_end), Some(b'e' | b'E')) {
        let mut exponent_end = number_end + 1;
        if matches!(bytes.get(exponent_end), Some(b'+' | b'-')) {
            exponent_end += 1;
        }
        let exponent_start = exponent_end;
        while bytes.get(exponent_end).is_some_and(u8::is_ascii_digit) {
            exponent_end += 1;
        }
        if exponent_end > exponent_start {
            number_end = exponent_end;
        }
    }

    let number = &input[number_start..number_end];
    let Some(mut value) = crabka_pgtypes::numeric::parse(number) else {
        return Err(ExecError::Type(crabka_pgtypes::TypeError::OutOfRange {
            message: "value overflows numeric format".into(),
        }));
    };

    let mut unit_start = number_end;
    while bytes.get(unit_start).is_some_and(u8::is_ascii_whitespace) {
        unit_start += 1;
    }
    let mut unit_end = bytes.len();
    while unit_end > unit_start && bytes[unit_end - 1].is_ascii_whitespace() {
        unit_end -= 1;
    }
    let unit = &input[unit_start..unit_end];
    let unit_bits = if unit.is_empty()
        || unit.eq_ignore_ascii_case("bytes")
        || unit.eq_ignore_ascii_case("b")
    {
        0
    } else if unit.eq_ignore_ascii_case("kb") {
        10
    } else if unit.eq_ignore_ascii_case("mb") {
        20
    } else if unit.eq_ignore_ascii_case("gb") {
        30
    } else if unit.eq_ignore_ascii_case("tb") {
        40
    } else if unit.eq_ignore_ascii_case("pb") {
        50
    } else {
        return Err(invalid_size_unit(input, unit));
    };
    if unit_bits > 0 {
        let multiplier = crabka_pgtypes::numeric::from_i64(1_i64 << unit_bits);
        value = crabka_pgtypes::numeric::mul(&value, &multiplier);
    }
    Ok(Datum::Int8(crabka_pgtypes::numeric::to_i64(&value)?))
}

fn invalid_size(input: &str) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "22023",
        message: format!("invalid size: \"{input}\""),
    }
}

fn invalid_size_unit(input: &str, unit: &str) -> ExecError {
    ExecError::Remote(
        crabka_pgwire::error::PgError::error("22023", format!("invalid size: \"{input}\""))
            .with_detail(format!("Invalid size unit: \"{unit}\"."))
            .with_hint(
                "Valid units are \"bytes\", \"B\", \"kB\", \"MB\", \"GB\", \"TB\", and \"PB\".",
            ),
    )
}

/// The `has_*_privilege` family.
///
/// The relation-scoped members answer from the grants `GRANT`/`REVOKE` actually
/// wrote — see [`crate::privilege`], which is the same decision every `SELECT`,
/// `INSERT`, `UPDATE`, `DELETE` and `TRUNCATE` is gated on, so the answer and
/// the enforcement cannot drift apart.
///
/// The rest still answer `true` unconditionally, and that is a statement about
/// what this catalog stores rather than an oversight: there is no ACL for a
/// database, a language, a tablespace, a type, a foreign server, a large object
/// or a routine, so there is nothing for a grant to have written and nothing an
/// enforcement path could read. [`crate::rls::validate_policy_qual`] still
/// refuses a policy written around one of those, for exactly the reason it once
/// refused all of them.
///
/// An unrecognized privilege name is `PostgreSQL`'s 22023, which is what callers
/// actually depend on catching.
fn has_privilege(name: &str, vals: &[Datum], ctx: &EvalCtx) -> Result<Datum, ExecError> {
    let Some(Datum::Text(privilege)) = vals.last() else {
        return Ok(Datum::Null);
    };
    if vals.iter().any(|value| matches!(value, Datum::Null)) {
        return Ok(Datum::Null);
    }
    let bare = privilege
        .trim()
        .strip_suffix(" WITH GRANT OPTION")
        .unwrap_or(privilege.trim());
    if !recognized_privilege(bare) {
        return Err(ExecError::FunctionError {
            sqlstate: "22023",
            message: format!("unrecognized privilege type: \"{privilege}\""),
        });
    }
    let Some(shape) = RelationPrivilegeCall::of(name, vals.len()) else {
        return Ok(Datum::Bool(true));
    };
    let Some(kv) = ctx.catalog() else {
        return Ok(Datum::Bool(true));
    };
    // A relation-scoped test must still fail on a missing relation, before any
    // question about grants is asked.
    let oid = resolve_relation_oid(kv, ctx.resolution(), &vals[shape.relation])?;
    let Some((relation, owner, kind)) = relation_acl_target(kv, oid)? else {
        // A relation with no catalog record of its own — a virtual catalog
        // relation, an index, a sequence. PostgreSQL grants `SELECT` on the
        // system catalogs to `PUBLIC`, and none of the others carry a table ACL
        // here, so the honest answer for all of them is the one that says yes.
        return Ok(Datum::Bool(true));
    };
    let role = match shape.role.map(|at| &vals[at]) {
        Some(argument) => role_argument_name(kv, argument)?,
        None => ctx.current_user.clone(),
    };
    let role = effective_privilege_role(&role);
    let privilege_ctx = crate::privilege::PrivilegeCtx::new(kv, &role);
    let wanted = bare.to_ascii_uppercase();
    if !crabka_pgcatalog::TABLE_PRIVILEGES.contains(&wanted.as_str()) {
        // A privilege that is recognized somewhere but cannot be granted on a
        // relation (`CONNECT`, `USAGE`, `EXECUTE`, …). PostgreSQL raises 22023
        // for these in a relation position; answering `true` is what this
        // function did before privileges were enforced, and keeping it there
        // confines this change to the answers that were actually wrong.
        return Ok(Datum::Bool(true));
    }
    // A column privilege is a table privilege here: no column-level grant can be
    // stored, so `GRANT SELECT (a) ON t` cannot have narrowed anything and the
    // table-level answer is the whole answer. `kind` likewise does not change
    // the decision — it only changes how a denial would be spelled, and this
    // function returns a boolean rather than raising one.
    let _ = kind;
    crate::privilege::holds_named(&privilege_ctx, &relation, &owner, &wanted).map(Datum::Bool)
}

/// Where a relation-scoped `has_*_privilege` call keeps its arguments.
///
/// Each of the three has an optional leading role argument, so the positions
/// are counted from the end: the privilege name is last, and the relation sits
/// a fixed distance in front of it.
struct RelationPrivilegeCall {
    /// The index of the relation argument.
    relation: usize,
    /// The index of the role argument, when the call names one.
    role: Option<usize>,
}

impl RelationPrivilegeCall {
    fn of(name: &str, arity: usize) -> Option<Self> {
        // (minimum arity without a role, arguments between relation and privilege)
        let (minimum, trailing) = match name {
            "has_table_privilege" | "has_any_column_privilege" => (2, 0),
            "has_column_privilege" => (3, 1),
            _ => return None,
        };
        if arity < minimum {
            return None;
        }
        Some(Self {
            relation: arity.checked_sub(trailing + 2)?,
            role: (arity > minimum).then_some(0),
        })
    }
}

/// The relation an ACL question is about: its name, its owner, and whether
/// `PostgreSQL` would call it a table or a view.
///
/// `None` for an oid that names no ACL-bearing relation.
fn relation_acl_target(
    kv: &dyn Kv,
    oid: i32,
) -> Result<Option<(RelationName, String, crate::privilege::RelationKind)>, ExecError> {
    for table in crabka_pgcatalog::list_tables(kv)? {
        if crate::catalog_rel::table_relation_oid(table.id)? == oid {
            return Ok(Some((
                table.name,
                table.owner,
                crate::privilege::RelationKind::Table,
            )));
        }
    }
    let view_oids = crate::catalog_rel::view_oids(kv)?;
    for view in crabka_pgcatalog::list_views(kv)? {
        if view_oids.get(&view.name) == Some(&oid) {
            return Ok(Some((
                view.name,
                view.owner,
                crate::privilege::RelationKind::View,
            )));
        }
    }
    Ok(None)
}

/// The role a `has_*_privilege` role argument names, given as a name or an oid.
fn role_argument_name(kv: &dyn Kv, argument: &Datum) -> Result<String, ExecError> {
    match argument {
        Datum::Text(name) => Ok(name.clone()),
        other => match user_by_id(kv, other)? {
            Datum::Text(name) => Ok(name),
            _ => Ok(String::new()),
        },
    }
}

/// The role a privilege question is really about.
///
/// `PUBLIC` in the `current_user` position means the session authenticated as
/// nobody, and every other decision in the engine reads that as the bootstrap
/// superuser — see `ForeignCtx::effective_role`. Answering these functions under
/// a different role than the enforcement path uses would let a session be told
/// it may not read a relation it can read.
pub(crate) fn effective_privilege_role(role: &str) -> String {
    if role == crabka_pgcatalog::PUBLIC_ROLE {
        crabka_pgcatalog::BOOTSTRAP_ROLE.to_string()
    } else {
        role.to_string()
    }
}

fn recognized_privilege(privilege: &str) -> bool {
    const NAMES: &[&str] = &[
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "TRUNCATE",
        "REFERENCES",
        "TRIGGER",
        "MAINTAIN",
        "CREATE",
        "CONNECT",
        "TEMPORARY",
        "TEMP",
        "EXECUTE",
        "USAGE",
        "SET",
        "ALTER SYSTEM",
    ];
    NAMES
        .iter()
        .any(|known| known.eq_ignore_ascii_case(privilege))
}

/// Report the bytes physically stored for a secondary index. Other relation
/// kinds still report zero rather than a fabricated PostgreSQL page count.
fn relation_size_with_fork(
    catalog_kv: &dyn Kv,
    data_kv: &dyn Kv,
    scope: &ResolutionScope,
    vals: &[Datum],
) -> Result<Datum, ExecError> {
    let object = &vals[0];
    if matches!(object, Datum::Null) {
        return Ok(Datum::Null);
    }
    let oid = resolve_relation_oid(catalog_kv, scope, object)?;
    if crate::catalog_rel::relation_for_oid(catalog_kv, oid)?.is_none() {
        return Ok(Datum::Null);
    }
    let fork = match vals.get(1) {
        None => "main",
        Some(Datum::Null) => return Ok(Datum::Null),
        Some(Datum::Text(fork)) if fork == "main" => "main",
        Some(Datum::Text(fork)) if matches!(fork.as_str(), "fsm" | "vm" | "init") => fork.as_str(),
        Some(Datum::Text(_)) => {
            return Err(ExecError::Remote(
                crabka_pgwire::error::PgError::error("22023", "invalid fork name")
                    .with_hint("Valid fork names are \"main\", \"fsm\", \"vm\", and \"init\"."),
            ));
        }
        Some(_) => {
            return Err(ExecError::TypeMismatch(
                "pg_relation_size fork name must be text".into(),
            ));
        }
    };
    Ok(Datum::Int8(if fork == "main" {
        relation_size_bytes(catalog_kv, data_kv, oid)?
    } else {
        0
    }))
}

fn relation_size(
    catalog_kv: &dyn Kv,
    data_kv: &dyn Kv,
    scope: &ResolutionScope,
    object: &Datum,
) -> Result<Datum, ExecError> {
    relation_size_with_fork(catalog_kv, data_kv, scope, std::slice::from_ref(object))
}

fn table_size(
    catalog_kv: &dyn Kv,
    data_kv: &dyn Kv,
    scope: &ResolutionScope,
    object: &Datum,
) -> Result<Datum, ExecError> {
    relation_size(catalog_kv, data_kv, scope, object)
}

fn indexes_size(
    catalog_kv: &dyn Kv,
    data_kv: &dyn Kv,
    scope: &ResolutionScope,
    object: &Datum,
) -> Result<Datum, ExecError> {
    if matches!(object, Datum::Null) {
        return Ok(Datum::Null);
    }
    let oid = resolve_relation_oid(catalog_kv, scope, object)?;
    if crate::catalog_rel::relation_for_oid(catalog_kv, oid)?.is_none() {
        return Ok(Datum::Null);
    }
    Ok(Datum::Int8(indexes_size_bytes(catalog_kv, data_kv, oid)?))
}

fn total_relation_size(
    catalog_kv: &dyn Kv,
    data_kv: &dyn Kv,
    scope: &ResolutionScope,
    object: &Datum,
) -> Result<Datum, ExecError> {
    if matches!(object, Datum::Null) {
        return Ok(Datum::Null);
    }
    let oid = resolve_relation_oid(catalog_kv, scope, object)?;
    if crate::catalog_rel::relation_for_oid(catalog_kv, oid)?.is_none() {
        return Ok(Datum::Null);
    }
    let bytes = relation_size_bytes(catalog_kv, data_kv, oid)?
        .checked_add(indexes_size_bytes(catalog_kv, data_kv, oid)?)
        .ok_or_else(|| ExecError::Unsupported("relation size exceeds int8".into()))?;
    Ok(Datum::Int8(bytes))
}

fn relation_size_bytes(catalog_kv: &dyn Kv, data_kv: &dyn Kv, oid: i32) -> Result<i64, ExecError> {
    let indexes = crabka_pgcatalog::list_indexes(catalog_kv)?;
    let Some(index) = indexes.iter().find(|index| {
        crate::catalog_rel::index_relation_oid(index.id).is_ok_and(|index_oid| index_oid == oid)
    }) else {
        return Ok(0);
    };
    secondary_index_size(data_kv, index)
}

fn indexes_size_bytes(catalog_kv: &dyn Kv, data_kv: &dyn Kv, oid: i32) -> Result<i64, ExecError> {
    let Some(table_id) = crabka_pgcatalog::list_tables(catalog_kv)?
        .into_iter()
        .find_map(|table| {
            (crate::catalog_rel::table_relation_oid(table.id).ok() == Some(oid)).then_some(table.id)
        })
    else {
        return Ok(0);
    };
    crabka_pgcatalog::list_indexes(catalog_kv)?
        .iter()
        .filter(|index| index.table_id == table_id)
        .try_fold(0_i64, |total, index| {
            total
                .checked_add(secondary_index_size(data_kv, index)?)
                .ok_or_else(|| ExecError::Unsupported("relation size exceeds int8".into()))
        })
}

fn secondary_index_size(kv: &dyn Kv, index: &crabka_pgcatalog::Index) -> Result<i64, ExecError> {
    let prefix = crabka_pgkv::key::secondary_index_prefix(index.table_id, index.id);
    kv.scan_prefix(&prefix)?
        .into_iter()
        .try_fold(0_i64, |total, (key, value)| {
            let entry = key
                .len()
                .checked_add(value.len())
                .and_then(|bytes| i64::try_from(bytes).ok())
                .ok_or_else(|| ExecError::Unsupported("relation size exceeds int8".into()))?;
            total
                .checked_add(entry)
                .ok_or_else(|| ExecError::Unsupported("relation size exceeds int8".into()))
        })
}

/// `pg_database_size`/`pg_tablespace_size`.
///
/// Crabka keeps no storage accounting, so the answer is zero bytes for any
/// non-NULL argument.
fn cluster_size(object: &Datum) -> Datum {
    match object {
        Datum::Null => Datum::Null,
        _ => Datum::Int8(0),
    }
}

/// `pg_table_is_visible(oid)` — would an unqualified reference to this
/// relation's name resolve to *this* relation?
///
/// [`crate::visibility`] holds the rule, which is the same one every other
/// member of the family follows. This is the entry point [`crate::func`]
/// dispatches through, because `pg_table_is_visible` is classified there rather
/// than in this module's family.
///
/// # Errors
///
/// Propagates storage/corruption errors from the catalog KV seam.
pub(crate) fn relation_is_visible(oid: &Datum, ctx: &EvalCtx) -> Result<Datum, ExecError> {
    crate::visibility::is_visible(crate::visibility::Catalog::Relation, oid, ctx)
}

/// Resolve a `regclass`-shaped argument — an oid, or a relation name — to its
/// `pg_class` oid.
fn resolve_relation_oid(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    object: &Datum,
) -> Result<i32, ExecError> {
    match object {
        Datum::Text(name) => resolve_relation_in_scope(kv, scope, name),
        Datum::Regclass(value) => Ok(value.oid),
        other => i32::try_from(int_arg(other)?)
            .map_err(|_| ExecError::Unsupported("oid exceeds int4 range".into())),
    }
}

/// Resolve a written relation name across every relation kind, not just base
/// tables.
///
/// The text is a `regclass` input, read by [`parse_written_relation`]. A quoted
/// part keeps its case and may hold a dot, and an unquoted part is downcased.
/// The text is then resolved the way `scope` resolves any written name. A
/// qualifier names the schema outright, and this function looks for a bare name
/// in each visible schema in path order.
pub(crate) fn resolve_relation_in_scope(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    name: &str,
) -> Result<i32, ExecError> {
    let written = parse_written_relation(scope, name)?;
    let schemas = match &written.reference.schema {
        Some(schema) => vec![schema.clone()],
        None => scope.visible_schemas(kv)?,
    };
    for schema in schemas {
        let candidate = RelationName::new(schema, written.reference.name.clone());
        if let Some(oid) = relation_oid(kv, &candidate)? {
            return Ok(oid);
        }
    }
    Err(written.undefined_table())
}

/// The catalog-aware half of a `… :: regclass` cast whose operand spells a
/// relation *name*.
///
/// A name is the only shape a search path applies to. `t` under
/// `search_path = s1` is `s1.t`, exactly as `SELECT … FROM t` reads it. Every
/// other operand names its relation outright and takes
/// [`crate::exec::regclass_cast`]. Those operands are an oid in any of its
/// spellings, `-`, and a value that is already a `regclass`.
pub(crate) fn regclass_cast(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    value: &Datum,
) -> Result<Option<Datum>, ExecError> {
    let Some(name) = relation_name_operand(value) else {
        return crate::exec::regclass_cast(kv, scope, value);
    };
    let oid = resolve_relation_in_scope(kv, scope, name)?;
    crate::exec::regclass_by_oid(kv, scope, oid)
        .map(Datum::Regclass)
        .map(Some)
}

/// The operand text that spells a relation name instead of an oid.
///
/// This is `regclassin`'s own test. It takes `-` and an all-digit string as
/// oids before it reads anything as a name.
fn relation_name_operand(value: &Datum) -> Option<&str> {
    let Datum::Text(text) = value else {
        return None;
    };
    let trimmed = text.trim();
    (trimmed != "-" && trimmed.parse::<i32>().is_err()).then_some(text.as_str())
}

/// The `pg_class` oid of the relation stored under exactly this catalog name,
/// or `None` when no relation of any kind answers to it.
///
/// The relation may be a virtual catalog relation, a table, a view, a sequence
/// or an index.
///
/// [`crate::relname::resolve_relation`] resolves the three kinds the catalog
/// keys by name. `regclass` also accepts an index and a virtual catalog
/// relation, so [`resolve_relation_in_scope`] holds the search-path walk, and
/// this function is the per-schema probe that walk repeats.
fn relation_oid(kv: &dyn Kv, name: &RelationName) -> Result<Option<i32>, ExecError> {
    match crate::exec::resolve_base_relation(kv, name) {
        Ok(oid) => return Ok(Some(oid)),
        // Not a virtual relation and not a table; the other three `pg_class`
        // kinds are this module's to check.
        Err(ExecError::Catalog(crabka_pgcatalog::CatalogError::UndefinedTable(_))) => {}
        Err(other) => return Err(other),
    }
    if let Some(oid) = crate::catalog_rel::view_oids(kv)?.get(name) {
        return Ok(Some(*oid));
    }
    if let Some(oid) = crate::catalog_rel::sequence_oids(kv)?.get(name) {
        return Ok(Some(*oid));
    }
    for index in crabka_pgcatalog::list_indexes(kv)? {
        if index.qualified_name() == *name {
            return crate::catalog_rel::index_relation_oid(index.id).map(Some);
        }
    }
    Ok(None)
}

/// The catalog name a written relation reference denotes, resolved through the
/// one resolver.
///
/// Callers use this function where the answer is a *name* and not an oid, and
/// where only the catalog-keyed relation kinds can be meant.
fn resolve_relation_name(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    name: &str,
) -> Result<RelationName, ExecError> {
    crate::relname::resolve_relation(
        kv,
        scope,
        &parse_written_relation(scope, name)?.reference,
        crate::relname::SchemaDisposition::Reference,
    )
}

/// The inverse of [`resolve_relation_in_scope`].
///
/// The result is the name `regclassout` prints for a `pg_class` oid, or `None`
/// when no relation has that oid.
///
/// The name is spelled as PostgreSQL spells it. Each identifier is quoted only
/// when `quote_ident` would quote it, and the name is schema-qualified exactly
/// when an unqualified reference would not reach this relation — the test
/// `scope` decides, in [`quote_relation_name`]. Every spelling this function
/// produces has to read back through
/// [`crate::relname::parse_written_relation`] as the same relation. That is the
/// round trip `regclass_input.rs` pins.
pub(crate) fn relation_name_by_oid(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    oid: i32,
) -> Result<Option<String>, ExecError> {
    let Some(name) = crate::catalog_rel::relation_for_oid(kv, oid)? else {
        return Ok(None);
    };
    quote_relation_name(kv, scope, &name).map(Some)
}

/// Spell a catalog relation name the way `regclassout` would: `schema.relation`
/// with each half quoted as needed, and the schema dropped exactly when writing
/// the bare name would reach this very relation.
///
/// `regclassout` asks `RelationIsVisible` and qualifies only when the answer is
/// no. [`crate::visibility::relation_name_is_visible`] is that walk. It depends
/// on the session, so this function takes the session's `scope`; the rule it
/// replaced — drop the schema for `public` and `pg_catalog`, keep it otherwise
/// — agrees with `PostgreSQL` only under the default `search_path` and is wrong
/// in both directions elsewhere. Verified against `postgres:18.4`:
///
/// ```text
/// SET search_path = app;
/// 'app.ap'::regclass::text     -- PostgreSQL: ap          old rule: app.ap
/// 'public.pp'::regclass::text  -- PostgreSQL: public.pp   old rule: pp
/// ```
///
/// A temporary relation is visible under its own session's scope, because
/// [`crate::relname::ResolutionScope::visible_schemas`] puts the temporary
/// namespace first exactly as `recomputeNamespacePath` does — so `'tt'::regclass`
/// prints `tt`, not `pg_temp_7.tt`. Another session's temporary relation is not
/// visible and keeps its real `pg_temp_<id>` qualifier, which is the spelling
/// `get_namespace_name` gives `regclassout` there too.
///
/// [`foreign_key_definition`]'s referent is *not* routed through here — it
/// deparses with `generate_relation_name`, a different `PostgreSQL` function
/// that this one used to be conflated with, and it takes the visibility test
/// directly.
///
/// # Errors
///
/// Propagates storage/corruption errors from the catalog KV seam, which the
/// visibility walk reads to find what each schema on the path already holds.
fn quote_relation_name(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    name: &RelationName,
) -> Result<String, ExecError> {
    if crate::visibility::relation_name_is_visible(kv, scope, name)? {
        return Ok(crate::string_fn::quote_ident(&name.name));
    }
    Ok(format!(
        "{}.{}",
        crate::string_fn::quote_ident(&name.schema),
        crate::string_fn::quote_ident(&name.name)
    ))
}

/// `pg_get_userbyid(oid)`, the role name for a role oid.
///
/// PostgreSQL answers `unknown (OID=n)` for an oid no role has, and `\dt`'s
/// Owner column shows that text verbatim, so this function reproduces the
/// fallback exactly.
fn user_by_id(kv: &dyn Kv, oid: &Datum) -> Result<Datum, ExecError> {
    if matches!(oid, Datum::Null) {
        return Ok(Datum::Null);
    }
    let wanted = int_arg(oid)?;
    if i64::from(BOOTSTRAP_ROLE_OID) == wanted {
        return Ok(Datum::Text(OBJECT_OWNER.into()));
    }
    if i64::from(DATABASE_OWNER_ROLE_OID) == wanted {
        return Ok(Datum::Text(crabka_pgcatalog::PUBLIC_SCHEMA_OWNER.into()));
    }
    for (name, role_oid) in crate::catalog_rel::role_oids(kv)? {
        if i64::from(role_oid) == wanted {
            return Ok(Datum::Text(name));
        }
    }
    Ok(Datum::Text(format!("unknown (OID={wanted})")))
}

/// `pg_get_serial_sequence(table, column)`, the sequence a serial or identity
/// column draws from, schema-qualified, or NULL when the column has none.
fn serial_sequence(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    vals: &[Datum],
) -> Result<Datum, ExecError> {
    let (Datum::Text(relation), Datum::Text(column)) = (&vals[0], &vals[1]) else {
        return Ok(Datum::Null);
    };
    let table = crabka_pgcatalog::get_table(kv, &resolve_relation_name(kv, scope, relation)?)?;
    let found = table
        .columns
        .iter()
        .find(|candidate| candidate.name == *column)
        .ok_or_else(|| ExecError::UndefinedColumn(column.clone()))?;
    match &found.default {
        Some(crabka_pgcatalog::ColumnDefault::NextVal(sequence)) => Ok(Datum::Text(
            qualified_sequence_name(kv, scope, &table.name, sequence)?,
        )),
        _ => Ok(Datum::Null),
    }
}

/// `pg_get_serial_sequence`'s answer for a stored `nextval` default.
///
/// The answer is the sequence's name, always schema-qualified. PostgreSQL
/// qualifies it even in `public`, because a caller passes the text directly
/// back to `nextval` or `setval`.
///
/// A default stores the sequence as it is *spelled* for `nextval`, which is
/// bare in `public` and dotted elsewhere, so the text alone cannot say which
/// schema it names. The sequence's own catalog record can. For a sequence the
/// catalog no longer holds, this function falls back to its table's schema,
/// which is where `PostgreSQL` puts the sequence a serial column owns.
///
/// A spelling is not an identity. Every session's temporary namespace spells
/// `pg_temp`, so the spelling a temporary sequence carries is one that every
/// other session's sequence of that name carries too, and a match on the text
/// alone can pick a sequence this session cannot even reach. Only this
/// session's temporary namespace can hold the sequence its own default names,
/// so the other sessions' namespaces are dropped before the match is made.
///
/// The schema is spelled as [`crabka_pgcatalog::displayed_schema`] spells it —
/// measured on `postgres:18.4`, where a temporary table's
/// `pg_get_serial_sequence` answers `pg_temp.probe_t_s_seq`.
fn qualified_sequence_name(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    table: &RelationName,
    spelled: &str,
) -> Result<String, ExecError> {
    let temp = scope.temp_schema();
    let sequence = crabka_pgcatalog::list_sequences(kv)?
        .into_iter()
        .map(|(name, _)| name)
        .filter(|name| !crabka_pgcatalog::is_temp_schema(&name.schema) || name.schema == temp)
        .find(|name| name.to_string() == spelled)
        .unwrap_or_else(|| table.sibling(spelled));
    Ok(format!(
        "{}.{}",
        quote_identifier(crabka_pgcatalog::displayed_schema(&sequence.schema)),
        quote_identifier(&sequence.name)
    ))
}

/// `obj_description`/`col_description`, the comment on an object, or on one of
/// its columns when `subid` is non-zero.
fn description(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    object: &Datum,
    subid: i32,
) -> Result<Datum, ExecError> {
    if matches!(object, Datum::Null) {
        return Ok(Datum::Null);
    }
    let oid = resolve_relation_oid(kv, scope, object)?;
    for table in crabka_pgcatalog::list_tables(kv)? {
        if crate::catalog_rel::table_relation_oid(table.id)? != oid {
            continue;
        }
        if subid == 0 {
            // Comments are stored under the kind word the `COMMENT ON` wrote,
            // and `COMMENT ON MATERIALIZED VIEW` and `COMMENT ON FOREIGN TABLE`
            // are both stored relations here — so the lookup has to ask for the
            // kind this relation actually is, not the one every `Table` used to
            // be.
            return comment_datum(
                kv,
                crate::exec::stored_relation_kind(&table),
                CommentObject::Relation(&table.name),
            );
        }
        let index = usize::try_from(subid.saturating_sub(1)).unwrap_or(usize::MAX);
        let Some(column) = table.columns.get(index) else {
            return Ok(Datum::Null);
        };
        return comment_datum(
            kv,
            "column",
            CommentObject::Column(&table.name, &column.name),
        );
    }
    for (name, view_oid) in crate::catalog_rel::view_oids(kv)? {
        if view_oid == oid && subid == 0 {
            return comment_datum(kv, "view", CommentObject::Relation(&name));
        }
    }
    Ok(Datum::Null)
}

fn comment_datum(kv: &dyn Kv, kind: &str, object: CommentObject<'_>) -> Result<Datum, ExecError> {
    Ok(crabka_pgcatalog::get_comment(kv, kind, object)?.map_or(Datum::Null, Datum::Text))
}

// ------------------------------------------------------ definition rebuilding

/// `pg_get_viewdef` in each of its overloads.
///
/// The second argument is either the pretty-print flag or a wrap column. A wrap
/// column implies pretty-printing, exactly as PostgreSQL's
/// `pg_get_viewdef(oid, integer)` does.
fn view_def(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    vals: &[Datum],
    style: crabka_pgtypes::encoding::OutputStyle<'_>,
) -> Result<Datum, ExecError> {
    let (pretty, wrap) = match vals.get(1) {
        None => (false, None),
        Some(Datum::Bool(flag)) => (*flag, None),
        Some(Datum::Null) => return Ok(Datum::Null),
        // The wrap-column overload implies pretty-printing, and a non-positive
        // column means "never pack", which is one output column per line.
        Some(other) => {
            let column = int_arg(other)?;
            (true, usize::try_from(column).ok().filter(|n| *n > 0))
        }
    };
    // A materialized view answers `pg_get_viewdef` too — it is what `\d+` prints
    // under "View definition:" for one — so it is tried before the literal
    // refusal, which is reserved for a relation kind that carries no query.
    if let Some(table) = lookup_materialized(kv, scope, &vals[0])? {
        return Ok(Datum::Text(materialized_definition(
            &table, pretty, wrap, style,
        )));
    }
    let Some(view) = lookup_view(kv, scope, &vals[0])? else {
        return Ok(Datum::Text("Not a view".into()));
    };
    Ok(Datum::Text(view_definition(&view, pretty, wrap, style)))
}

/// Find the materialized view an oid or a name refers to, resolved exactly as
/// [`lookup_view`] resolves one, or `None` when the argument names anything
/// else.
fn lookup_materialized(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    object: &Datum,
) -> Result<Option<crabka_pgcatalog::Table>, ExecError> {
    let wanted = match object {
        Datum::Null => return Ok(None),
        Datum::Text(name) => match resolve_relation_in_scope(kv, scope, name) {
            Ok(oid) => oid,
            Err(_) => return Ok(None),
        },
        other => i32::try_from(int_arg(other)?)
            .map_err(|_| ExecError::Unsupported("oid exceeds int4 range".into()))?,
    };
    for table in crabka_pgcatalog::list_tables(kv)? {
        if table.materialized.is_some()
            && crate::catalog_rel::table_relation_oid(table.id)? == wanted
        {
            return Ok(Some(table));
        }
    }
    Ok(None)
}

/// Find the view an oid or a name refers to, or `None` when the argument names
/// an object that is not a view. PostgreSQL answers the literal `Not a view`.
///
/// This function resolves a name through the session's search path exactly as a
/// `regclass` cast resolves one, so it finds a view the path reaches under that
/// view's own schema. A name no relation answers to is not a view either.
fn lookup_view(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    object: &Datum,
) -> Result<Option<View>, ExecError> {
    let wanted = match object {
        Datum::Null => return Ok(None),
        Datum::Text(name) => match resolve_relation_in_scope(kv, scope, name) {
            Ok(oid) => oid,
            Err(_) => return Ok(None),
        },
        other => i32::try_from(int_arg(other)?)
            .map_err(|_| ExecError::Unsupported("oid exceeds int4 range".into()))?,
    };
    let oids = crate::catalog_rel::view_oids(kv)?;
    Ok(crabka_pgcatalog::list_views(kv)?
        .into_iter()
        .find(|view| oids.get(&view.name) == Some(&wanted)))
}

/// Render a stored view definition the way PostgreSQL's rule deparser does.
///
/// This function re-parses the stored text and prints it from the tree, so the
/// answer is normalized and not echoed. Keyword lines carry PostgreSQL's
/// indentation, and output columns are named from the view's catalog column
/// list. When `pretty` is false, operator expressions are fully parenthesized.
/// For a stored definition that no longer parses, this function falls back to
/// the source text, which is still a valid view definition.
pub(crate) fn view_definition_text(
    view: &View,
    pretty: bool,
    style: crabka_pgtypes::encoding::OutputStyle<'_>,
) -> String {
    view_definition(view, pretty, None, style)
}

/// The same rendering for a materialized view, whose definition and output
/// column list live on its [`crabka_pgcatalog::Table`] record rather than on a
/// [`View`].
///
/// It borrows the view renderer rather than duplicating it because
/// `pg_get_viewdef` answers for both relation kinds and PostgreSQL prints the
/// same text for each: the deparser only ever needs the stored query and the
/// names its output columns currently carry, and a materialized view has both.
/// Naming the columns from the catalog rather than the stored text is what makes
/// `ALTER MATERIALIZED VIEW … RENAME COLUMN` show up in the definition, as it
/// does in PostgreSQL.
pub(crate) fn materialized_definition_text(
    table: &crabka_pgcatalog::Table,
    pretty: bool,
    style: crabka_pgtypes::encoding::OutputStyle<'_>,
) -> String {
    materialized_definition(table, pretty, None, style)
}

/// [`materialized_definition_text`] with an explicit select-list wrap column.
fn materialized_definition(
    table: &crabka_pgcatalog::Table,
    pretty: bool,
    wrap: Option<usize>,
    style: crabka_pgtypes::encoding::OutputStyle<'_>,
) -> String {
    let Some(matview) = &table.materialized else {
        return String::new();
    };
    view_definition(
        &View {
            name: table.name.clone(),
            definition: matview.definition.clone(),
            owner: table.owner.clone(),
            columns: table.columns.clone(),
            options: crabka_pgcatalog::ViewOptions::default(),
        },
        pretty,
        wrap,
        style,
    )
}

/// [`view_definition_text`] with an explicit select-list wrap column.
fn view_definition(
    view: &View,
    pretty: bool,
    wrap: Option<usize>,
    style: crabka_pgtypes::encoding::OutputStyle<'_>,
) -> String {
    let Ok(statements) = crabka_pgparser::parse(&view.definition) else {
        return format!("{};", view.definition.trim_end_matches(';'));
    };
    let [crabka_pgparser::ast::Statement::Query(query)] = statements.as_slice() else {
        return format!("{};", view.definition.trim_end_matches(';'));
    };
    let names = view
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let mut out = String::new();
    crate::viewdef::write_query(&mut out, query, &names, pretty, wrap, style);
    out.push(';');
    out
}

/// `pg_get_indexdef(oid)` — the `CREATE INDEX` statement that rebuilds an index.
fn index_def(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    object: &Datum,
    pretty: bool,
) -> Result<Datum, ExecError> {
    if matches!(object, Datum::Null) {
        return Ok(Datum::Null);
    }
    let oid = resolve_relation_oid(kv, scope, object)?;
    for index in crabka_pgcatalog::list_indexes(kv)? {
        if crate::catalog_rel::index_relation_oid(index.id)? != oid {
            continue;
        }
        let table = crabka_pgcatalog::get_table(kv, &index.table)?;
        let qualify = !pretty || !scope.visible_schemas(kv)?.contains(&table.name.schema);
        return Ok(Datum::Text(index_definition_as(&index, &table, qualify)));
    }
    Ok(Datum::Null)
}

/// The `CREATE INDEX` text for one index, schema-qualified like PostgreSQL's.
///
/// `pg_get_indexdef` genuinely qualifies the table it indexes, even in
/// `public`. That is why this function always spells out the schema.
/// [`foreign_key_definition`] deliberately leaves its referent unqualified when
/// it is visible. Read the note there before you make the two agree.
pub(crate) fn index_definition(index: &Index, table: &Table) -> String {
    index_definition_as(index, table, true)
}

/// The same text, with the choice of qualification made by the caller.
///
/// `pg_get_indexdef`'s three-argument `pretty` form drops the schema when the
/// table is on the search path — psql's `\d` calls it that way, and so does
/// the `without_overlaps` regression test.
fn index_definition_as(index: &Index, table: &Table, qualify: bool) -> String {
    let relation = if qualify {
        format!(
            "{}.{}",
            quote_identifier(crabka_pgcatalog::displayed_schema(&table.name.schema)),
            quote_identifier(&table.name.name)
        )
    } else {
        quote_identifier(&table.name.name)
    };
    format!(
        "CREATE {}INDEX {} ON {relation} USING {} ({})",
        if index.unique { "UNIQUE " } else { "" },
        quote_identifier(&index.name),
        match index.method {
            crabka_pgcatalog::IndexMethod::Btree => "btree",
            crabka_pgcatalog::IndexMethod::Hash => "hash",
            crabka_pgcatalog::IndexMethod::Gist => "gist",
            crabka_pgcatalog::IndexMethod::Gin => "gin",
            crabka_pgcatalog::IndexMethod::Spgist => "spgist",
        },
        index_key_list(&index.columns),
    )
}

/// `pg_get_partkeydef(oid)` — the `PARTITION BY` body a partitioned relation
/// was declared with, which is what psql's `\d` prints as `Partition key:`.
///
/// The answer is NULL for anything that is not a partitioned parent: an
/// ordinary table, an index, an oid no relation carries. PostgreSQL answers the
/// same way rather than raising, because the function is called unconditionally
/// on every relation `\d` describes.
///
/// Only column keys can appear. An expression partition key is refused at
/// `CREATE TABLE` time (see [`crate::partition`]), so the `partexprs` half of
/// PostgreSQL's rendering has nothing to print here.
fn part_key_def(kv: &dyn Kv, scope: &ResolutionScope, object: &Datum) -> Result<Datum, ExecError> {
    if matches!(object, Datum::Null) {
        return Ok(Datum::Null);
    }
    let oid = resolve_relation_oid(kv, scope, object)?;
    let Some(name) = crate::catalog_rel::relation_for_oid(kv, oid)? else {
        return Ok(Datum::Null);
    };
    let Some(scheme) = crate::partition::scheme_of(kv, &name)? else {
        return Ok(Datum::Null);
    };
    let keys = scheme
        .keys
        .iter()
        .map(|key| quote_identifier(key))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(Datum::Text(format!(
        "{} ({keys})",
        scheme.strategy.name().to_uppercase()
    )))
}

/// `pg_get_constraintdef(oid)` — the constraint clause that rebuilds a
/// constraint, in the spelling `ALTER TABLE … ADD CONSTRAINT` takes.
fn constraint_def(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    object: &Datum,
) -> Result<Datum, ExecError> {
    if matches!(object, Datum::Null) {
        return Ok(Datum::Null);
    }
    let wanted = i32::try_from(int_arg(object)?)
        .map_err(|_| ExecError::Unsupported("oid exceeds int4 range".into()))?;
    for index in crabka_pgcatalog::list_indexes(kv)? {
        let Some(kind) = index.constraint else {
            continue;
        };
        if crate::catalog_rel::index_constraint_oid(index.id)? != wanted {
            continue;
        }
        let keyword = match kind {
            crabka_pgcatalog::IndexConstraint::PrimaryKey => "PRIMARY KEY",
            crabka_pgcatalog::IndexConstraint::Unique => "UNIQUE",
            crabka_pgcatalog::IndexConstraint::Exclusion(_) => "EXCLUDE",
        };
        // `WITHOUT OVERLAPS` rides on the last key column, and this rendering
        // is what psql echoes verbatim for such a constraint instead of the
        // `PRIMARY KEY, btree (…)` line it synthesizes for an ordinary one.
        let temporal = if index.without_overlaps {
            " WITHOUT OVERLAPS"
        } else {
            ""
        };
        let mut definition = format!(
            "{keyword} ({}{temporal})",
            quoted_column_list(&index.columns)
        );
        let (deferrable, initially_deferred) = index.deferral.columns();
        if deferrable {
            definition.push_str(" DEFERRABLE");
        }
        if initially_deferred {
            definition.push_str(" INITIALLY DEFERRED");
        }
        return Ok(Datum::Text(definition));
    }
    if let Some(definition) = foreign_key_constraint_def(kv, scope, wanted)? {
        return Ok(Datum::Text(definition));
    }
    check_constraint_def(kv, wanted)
}

/// The `FOREIGN KEY …` clause for the foreign key an oid names, or `None` when
/// the oid is outside the foreign-key band.
fn foreign_key_constraint_def(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    wanted: i32,
) -> Result<Option<String>, ExecError> {
    let oids = crate::catalog_rel::foreign_key_constraint_oids(kv)?;
    for foreign_key in crabka_pgcatalog::list_foreign_keys(kv)? {
        let key = (foreign_key.table.clone(), foreign_key.name.clone());
        if oids.get(&key) == Some(&wanted) {
            return Ok(Some(foreign_key_definition(kv, scope, &foreign_key)?));
        }
    }
    Ok(None)
}

/// The `FOREIGN KEY …` clause for one foreign key, in PostgreSQL's clause
/// order.
///
/// The order is the referencing columns, the referent, `MATCH FULL`,
/// `ON UPDATE` before `ON DELETE`, the deferral, and `NOT VALID` last.
/// `MATCH SIMPLE` and `NO ACTION` are the defaults and print nothing. Only
/// `ON DELETE` can carry a `SET NULL`/`SET DEFAULT` column list.
///
/// `psql`'s `\d` builds both its `Foreign-key constraints:` block and the
/// parent's `Referenced by:` block out of this text verbatim. It adds only the
/// indent, the quoted constraint name and the `TABLE … CONSTRAINT …` prefix.
///
/// This function emits the referent **unqualified** when it is visible, so
/// `REFERENCES pp(id)` and never `public.pp(id)`. PostgreSQL renders it with
/// `generate_relation_name`, which omits the schema of a relation the search
/// path reaches and spells out the schema of one it does not, so the answer
/// depends on the session's `search_path` and changes as that changes:
/// `REFERENCES pp(id)` under the default path becomes `REFERENCES public.pp(id)`
/// once `public` is off it. That is a real asymmetry with [`index_definition`],
/// whose `ON public.t` is qualified because `pg_get_indexdef` genuinely
/// qualifies unconditionally; neither should be "fixed" to match the other.
///
/// # Errors
///
/// Propagates the catalog reads the visibility test needs.
pub(crate) fn foreign_key_definition(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    foreign_key: &ForeignKey,
) -> Result<String, ExecError> {
    let mut out = format!(
        "FOREIGN KEY ({}) REFERENCES {}({})",
        quoted_column_list(&foreign_key.columns),
        qualified_unless_visible(kv, scope, &foreign_key.referenced_table)?,
        quoted_column_list(&foreign_key.referenced_columns),
    );
    if foreign_key.match_type == MatchType::Full {
        out.push_str(" MATCH FULL");
    }
    if let Some(action) = referential_action_clause(foreign_key.on_update) {
        out.push_str(" ON UPDATE ");
        out.push_str(action);
    }
    if let Some(action) = referential_action_clause(foreign_key.on_delete) {
        out.push_str(" ON DELETE ");
        out.push_str(action);
        if !foreign_key.set_columns.is_empty() {
            out.push_str(" (");
            out.push_str(&quoted_column_list(&foreign_key.set_columns));
            out.push(')');
        }
    }
    if foreign_key.deferrable {
        out.push_str(" DEFERRABLE");
    }
    if foreign_key.initially_deferred {
        out.push_str(" INITIALLY DEFERRED");
    }
    if !foreign_key.validated {
        out.push_str(" NOT VALID");
    }
    Ok(out)
}

/// `generate_relation_name`: the name a *deparsed* relation reference is spelled
/// with — bare when an unqualified reference would reach this relation, and
/// `schema.relation` when it would not, because a qualifier is then the only
/// spelling that reads back as the same relation.
///
/// The distinction from [`quote_relation_name`] is the whole point: that one
/// answers `regclassout`'s question with a fixed rule about which schemas are
/// "always" reachable, and this one asks the search path what it actually
/// reaches right now. `public` is on the default path but is not on every path,
/// and a schema that is on the path can still be shadowed by an earlier one
/// holding the same relation name.
///
/// The qualifier a temporary namespace takes here is `pg_temp`, not the stored
/// `pg_temp_<backend id>`. Measured on `postgres:18.4` with a temporary
/// `shadow_t` shadowed by a `public.shadow_t` and `pg_temp` listed last on the
/// search path, where `pg_get_constraintdef` answers
/// `FOREIGN KEY (a) REFERENCES pg_temp.shadow_t(a)`.
fn qualified_unless_visible(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    name: &RelationName,
) -> Result<String, ExecError> {
    if crate::visibility::relation_name_is_visible(kv, scope, name)? {
        return Ok(quote_identifier(&name.name));
    }
    Ok(format!(
        "{}.{}",
        quote_identifier(crabka_pgcatalog::displayed_schema(&name.schema)),
        quote_identifier(&name.name)
    ))
}

/// How a referential action spells itself in a constraint definition.
///
/// `NO ACTION` is the default, and PostgreSQL leaves the clause out entirely.
fn referential_action_clause(action: ReferentialAction) -> Option<&'static str> {
    match action {
        ReferentialAction::NoAction => None,
        ReferentialAction::Restrict => Some("RESTRICT"),
        ReferentialAction::Cascade => Some("CASCADE"),
        ReferentialAction::SetNull => Some("SET NULL"),
        ReferentialAction::SetDefault => Some("SET DEFAULT"),
    }
}

/// A parenthesized-list body.
///
/// Each name is quoted as needed, the names are comma-separated, and they stay
/// in the order given. They are never sorted, because a foreign key's columns
/// pair positionally with the referenced ones.
fn quoted_column_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| quote_identifier(column))
        .collect::<Vec<_>>()
        .join(", ")
}

fn index_key_list(keys: &[String]) -> String {
    keys.iter()
        .map(|key| {
            crabka_pgcatalog::index_key_expression(key).map_or_else(
                || quote_identifier(key),
                |expression| format!("({expression})"),
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn check_constraint_def(kv: &dyn Kv, wanted: i32) -> Result<Datum, ExecError> {
    let check_oids = crate::catalog_rel::check_constraint_oids(kv)?;
    let not_null_oids = crate::catalog_rel::not_null_constraint_oids(kv)?;
    for table in crabka_pgcatalog::list_tables(kv)? {
        for check in &table.checks {
            let key = crate::catalog_rel::ConstraintKey::new(&table.name, &check.name);
            if check_oids.get(&key) == Some(&wanted) {
                let suffix = if check.validated { "" } else { " NOT VALID" };
                return Ok(Datum::Text(format!("CHECK (({})){suffix}", check.expr)));
            }
        }
        for column in &table.columns {
            let key = crate::catalog_rel::ConstraintKey::new(&table.name, &column.name);
            if column.not_null && not_null_oids.get(&key) == Some(&wanted) {
                return Ok(Datum::Text(format!(
                    "NOT NULL {}",
                    quote_identifier(&column.name)
                )));
            }
        }
    }
    Ok(Datum::Null)
}

/// The source text of a stored column default, as `pg_attrdef.adbin` holds it.
pub(crate) fn default_source_text(
    kv: &dyn Kv,
    default: &crabka_pgcatalog::ColumnDefault,
    ty: ColumnType,
    style: crabka_pgtypes::encoding::OutputStyle<'_>,
) -> String {
    match default {
        crabka_pgcatalog::ColumnDefault::NextVal(sequence) => {
            format!("nextval('{}'::regclass)", sequence.replace('\'', "''"))
        }
        // A `regclass` default stores only the oid, so the name it deparses to
        // is read from the catalog now: a `RENAME` of the relation changes what
        // `\d` and `pg_get_expr` print, as it does in PostgreSQL.
        //
        // The default scope, not the reader's, for the reason
        // `crate::exec::format_column_default` gives: `pg_attrdef.adbin` is one
        // stored text per column and cannot vary by who reads it.
        crabka_pgcatalog::ColumnDefault::Value(Datum::Regclass(value)) => {
            let resolved = crate::exec::regclass_by_oid(
                kv,
                crate::relname::ResolutionScope::default_scope(),
                value.oid,
            )
            .unwrap_or_else(|_| crabka_pgtypes::RegclassValue::unresolved(value.oid));
            crate::viewdef::const_text(&Datum::Regclass(resolved), ty, style)
        }
        // A bit-string default deparses with the *literal's* type, not the
        // column's. PostgreSQL wraps the assignment coercion around the Const
        // and `pg_get_expr` hides implicit casts, so what prints is the type
        // the literal already had: `B'0101'` is a `bit` wherever it lands, and
        // in a `bit varying(5)` column PostgreSQL still prints
        // `'0101'::"bit"`. A bare `'1001'` has no type of its own, so it is
        // read by the column's own input function and prints the column's base
        // type. The stored datum's `varying` flag is what records which of the
        // two happened, so this reads the flag rather than the column.
        crabka_pgcatalog::ColumnDefault::Value(value @ Datum::BitString(bits)) => {
            let literal = if bits.varying {
                ColumnType::VarBit(None)
            } else {
                ColumnType::Bit(None)
            };
            crate::viewdef::const_text(value, literal, style)
        }
        crabka_pgcatalog::ColumnDefault::Value(value) => {
            crate::viewdef::const_text(value, ty, style)
        }
    }
}

/// Quote an identifier the way `PostgreSQL`'s `quote_identifier` does — the one
/// renderer, reached by the whole `pg_get_*def` family and by `pg_indexes`.
///
/// This used to test the identifier's *character shape* alone, and its comment
/// claimed that was what `quote_ident` did. It was not. `PostgreSQL` bares an
/// identifier only when the character shape is safe **and** the word is not a
/// keyword outside the `UNRESERVED` category, so a column named `values` prints
/// `"values"` while one named `set` prints bare. Deparsing without the keyword
/// test emits SQL that does not read back.
///
/// The predicate lives in [`crate::string_fn::quote_ident`], which the SQL
/// function `quote_ident` already answers with, so this delegates rather than
/// keeping a second copy that can drift from the first.
///
/// Delegating changes the keyword behaviour and nothing else. The two character
/// rules are the same rule written twice: "non-empty, no leading digit, every
/// character in `[a-z0-9_]`" and "leading character in `[a-z_]`, every character
/// in `[a-z0-9_]`" admit exactly the same strings, since a string whose
/// characters are all `[a-z0-9_]` and whose first is not a digit begins with
/// `[a-z_]` by construction.
pub(crate) fn quote_identifier(name: &str) -> String {
    crate::string_fn::quote_ident(name)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgcatalog::{
        Column, ForeignKey, IndexPlacement, MatchType, ReferentialAction, RelationName, Table,
    };
    use crabka_pgkv::{Kv, MemKv};
    use crabka_pgparser::parser::parse_expr_for_test as pexpr;
    use crabka_pgtypes::{ColumnType, Datum};

    use super::{
        catalog_func, char_to_encoding, constraint_def, encoding_to_char, foreign_key_definition,
        indexes_size, is_catalog_func, quote_identifier, relation_size, relation_size_with_fork,
        size_bytes, size_pretty, table_size, total_relation_size,
    };
    use crate::error::ExecError;

    // One case: how it differs from `sample_foreign_key`, and the definition
    // PostgreSQL prints for the result.
    type DefinitionCase = (fn(&mut ForeignKey), &'static str);

    // The child of the oracle's `cc` / `pp` pair: one column, every optional
    // clause at its default, so a case only sets what it exercises.
    fn sample_foreign_key() -> ForeignKey {
        ForeignKey {
            id: 1,
            name: "cc_a_fkey".into(),
            table: RelationName::public("cc"),
            table_id: 2,
            columns: vec!["a".into()],
            referenced_table: RelationName::public("pp"),
            referenced_table_id: 1,
            referenced_columns: vec!["id".into()],
            referenced_index_id: 1,
            referenced_index: "pp_pkey".into(),
            match_type: MatchType::Simple,
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
            set_columns: Vec::new(),
            deferrable: false,
            initially_deferred: false,
            validated: true,
        }
    }

    fn rendered(configure: fn(&mut ForeignKey)) -> String {
        rendered_under(&crate::relname::ResolutionScope::default(), configure)
    }

    /// The same rendering, with the session's `search_path` chosen by the
    /// caller — the input the referent's spelling actually depends on.
    fn rendered_under(
        scope: &crate::relname::ResolutionScope,
        configure: fn(&mut ForeignKey),
    ) -> String {
        let mut foreign_key = sample_foreign_key();
        configure(&mut foreign_key);
        let kv = MemKv::new();
        for schema in ["app", "shadow"] {
            kv.write_batch(
                &crabka_pgcatalog::create_schema_ops(&kv, schema, "postgres").expect("schema ops"),
            )
            .expect("seed schema");
        }
        foreign_key_definition(&kv, scope, &foreign_key).expect("foreign key definition")
    }

    /// Make `name` exist as a table, so that it occupies its bare name in its
    /// schema and can shadow a relation of the same name further down the path.
    fn seed_relation(kv: &MemKv, name: &RelationName) {
        let (_, ops) = crabka_pgcatalog::create_table_ops(
            kv,
            name,
            vec![crabka_pgcatalog::Column::new("x", ColumnType::Int4)],
        )
        .expect("create table");
        kv.write_batch(&ops).expect("apply");
    }

    /// A session whose `search_path` is exactly `entries`.
    fn path(entries: &[&str]) -> crate::relname::ResolutionScope {
        crate::relname::ResolutionScope {
            search_path: crate::search_path::SearchPath::from_items(
                &entries
                    .iter()
                    .map(|name| (*name).to_string())
                    .collect::<Vec<_>>(),
            ),
            ..crate::relname::ResolutionScope::default()
        }
    }

    #[test]
    fn the_family_claims_the_functions_psql_calls() {
        for name in [
            "pg_get_viewdef",
            "pg_get_indexdef",
            "pg_get_constraintdef",
            "pg_get_expr",
            "pg_get_userbyid",
            "pg_get_serial_sequence",
            "pg_table_is_visible",
            "pg_type_is_visible",
            "pg_function_is_visible",
            "pg_relation_size",
            "pg_total_relation_size",
            "pg_size_pretty",
            "pg_size_bytes",
            "obj_description",
            "col_description",
            "shobj_description",
            "current_schemas",
            "pg_backend_pid",
            "pg_notification_queue_usage",
            "pg_postmaster_start_time",
            "has_table_privilege",
            "pg_encoding_to_char",
        ] {
            // `pg_table_is_visible` is the one name the older scalar family
            // already owns; every other name must land here.
            if name == "pg_table_is_visible" {
                assert!(crate::func::is_scalar(name));
            } else {
                assert!(is_catalog_func(name), "{name} is not dispatched");
            }
        }
    }

    #[test]
    fn unknown_names_are_not_claimed() {
        assert!(catalog_func("pg_get_nonesuch").is_none());
        assert!(!is_catalog_func("upper"));
    }

    #[test]
    fn encoding_identity_matches_postgresql() {
        assert_eq!(
            encoding_to_char(&Datum::Int4(22)),
            Datum::Text("KOI8R".into())
        );
        assert_eq!(
            encoding_to_char(&Datum::Int4(41)),
            Datum::Text("SHIFT_JIS_2004".into())
        );
        assert_eq!(
            encoding_to_char(&Datum::Int4(42)),
            Datum::Text(String::new())
        );
        assert_eq!(
            char_to_encoding(&Datum::Text("utf8".into())),
            Datum::Int4(6)
        );
        assert_eq!(
            char_to_encoding(&Datum::Text("UNICODE".into())),
            Datum::Int4(6)
        );
        assert_eq!(
            char_to_encoding(&Datum::Text("UTF-8".into())),
            Datum::Int4(6)
        );
        assert_eq!(
            char_to_encoding(&Datum::Text("WINDOWS1252".into())),
            Datum::Int4(24)
        );
    }

    /// PostgreSQL's own `pg_size_pretty` boundaries, measured on 18.4.
    #[test]
    fn size_pretty_matches_postgres_boundaries() {
        let cases = [
            (0_i64, "0 bytes"),
            (10, "10 bytes"),
            (10_239, "10239 bytes"),
            (10_240, "10 kB"),
            (1_048_576, "1024 kB"),
            (20_971_520, "20 MB"),
            (21_474_836_480, "20 GB"),
            (21_990_232_555_520, "20 TB"),
            (-10_240, "-10 kB"),
            (i64::MIN, "-8192 PB"),
            (i64::MAX, "8192 PB"),
        ];
        for (input, expected) in cases {
            let got = size_pretty(&Datum::Int8(input)).expect("size_pretty");
            assert!(got == Datum::Text(expected.into()), "{input}");
        }

        for (input, expected) in [
            ("10.5", "10.5 bytes"),
            ("1000000.5", "977 kB"),
            ("-1000000000000.5", "-931 GB"),
            ("11528652096115048447", "10239 PB"),
            ("11528652096115048448", "10240 PB"),
            ("-11528652096115048448", "-10240 PB"),
            ("NaN", "NaN PB"),
            ("Infinity", "Infinity PB"),
            ("-Infinity", "-Infinity PB"),
        ] {
            let value = crabka_pgtypes::numeric::parse(input).expect("numeric size");
            let got = size_pretty(&Datum::Numeric(value)).expect("numeric size_pretty");
            assert!(got == Datum::Text(expected.into()), "{input}");
        }
    }

    #[test]
    fn size_pretty_resolves_domains_through_their_base_type() {
        let domain = |oid, name, base| {
            ColumnType::Domain(crabka_pgtypes::usertype::DomainRef {
                oid,
                name,
                base: Box::leak(Box::new(base)),
            })
        };
        for ty in [
            domain(900_101, "size_int8_domain", ColumnType::Int8),
            domain(900_102, "size_numeric_domain", ColumnType::Numeric(None)),
        ] {
            let table = Table {
                id: 1,
                owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
                name: RelationName::public("size_domains"),
                columns: vec![Column::new("v", ty)],
                sharded: false,
                row_security: false,
                force_row_security: false,
                sharding: None,
                foreign: None,
                materialized: None,
                checks: Vec::new(),
            };
            let expression = pexpr("pg_size_pretty(v)").expect("parse size call");
            assert!(
                crate::eval::infer_type(
                    &expression,
                    &crate::scope::Scope::single(&table, "size_domains"),
                )
                .expect("domain overload")
                    == ColumnType::Text
            );
        }

        let int4_domain = domain(900_103, "size_int4_domain", ColumnType::Int4);
        let table = Table {
            id: 1,
            owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
            name: RelationName::public("size_domains"),
            columns: vec![Column::new("v", int4_domain)],
            sharded: false,
            row_security: false,
            force_row_security: false,
            sharding: None,
            foreign: None,
            materialized: None,
            checks: Vec::new(),
        };
        let error = crate::eval::infer_type(
            &pexpr("pg_size_pretty(v)").expect("parse size call"),
            &crate::scope::Scope::single(&table, "size_domains"),
        )
        .expect_err("int4 domain is ambiguous")
        .into_pg();
        assert!(error.code == "42725");
        assert!(error.message == "function pg_size_pretty(size_int4_domain) is not unique");
    }

    #[test]
    fn size_bytes_matches_postgres_inputs() {
        let cases = [
            ("1", 1_i64),
            ("123bytes", 123),
            ("256 B", 256),
            ("128kB", 131_072),
            ("1MB", 1_048_576),
            (" 1 GB", 1_073_741_824),
            ("1.5 gB ", 1_610_612_736),
            ("1TB", 1_099_511_627_776),
            ("3000 tb", 3_298_534_883_328_000),
            ("1e6 MB", 1_048_576_000_000),
            ("99 PB", 111_464_090_777_419_776),
            ("-10e-1 MB", -1_048_576),
            ("-1. kb", -1_024),
            ("-.1kb", -102),
        ];
        for (input, expected) in cases {
            let got = size_bytes(&Datum::Text(input.into())).expect("size_bytes");
            assert!(got == Datum::Int8(expected), "{input}");
        }
        assert!(size_bytes(&Datum::Null).expect("NULL") == Datum::Null);
        assert!(
            size_bytes(&Datum::Int4(1))
                .expect_err("wrong overload")
                .into_pg()
                .code
                == "42883"
        );
    }

    #[test]
    fn size_bytes_reports_postgres_errors() {
        let cases = [
            ("", "22023", "invalid size: \"\""),
            ("kb", "22023", "invalid size: \"kb\""),
            ("-.", "22023", "invalid size: \"-.\""),
            ("9223372036854775807.9", "22003", "bigint out of range"),
            ("1e100", "22003", "bigint out of range"),
            (
                "1e1000000000000000000",
                "22003",
                "value overflows numeric format",
            ),
        ];
        for (input, expected_sqlstate, expected_message) in cases {
            let error = size_bytes(&Datum::Text(input.into())).expect_err("invalid size");
            let (sqlstate, message) = match error {
                ExecError::FunctionError { sqlstate, message } => (sqlstate, message),
                ExecError::Type(error) => (error.sqlstate(), error.to_string()),
                other => panic!("unexpected error for {input}: {other:?}"),
            };
            assert!(sqlstate == expected_sqlstate, "{input}");
            assert!(message == expected_message, "{input}");
        }

        let error = size_bytes(&Datum::Text("1 AB A    ".into()))
            .expect_err("invalid size unit")
            .into_pg();
        assert!(error.code == "22023");
        assert!(error.message == "invalid size: \"1 AB A    \"");
        let diagnostics = error.diagnostics.expect("unit diagnostics");
        assert!(diagnostics.detail.as_deref() == Some("Invalid size unit: \"AB A\"."));
        assert!(
            diagnostics.hint.as_deref()
                == Some(
                    "Valid units are \"bytes\", \"B\", \"kB\", \"MB\", \"GB\", \"TB\", and \"PB\"."
                )
        );
    }

    #[test]
    fn relation_size_counts_physical_index_entries_only() {
        let catalog = MemKv::new();
        let data = MemKv::new();
        let table = RelationName::public("size_probe");
        let table_id = crabka_pgcatalog::create_table(
            &catalog,
            &table,
            vec![Column::new("a", ColumnType::Int4)],
        )
        .expect("table");
        let (ordinary_id, ops) = crabka_pgcatalog::create_index_ops(
            &catalog,
            "size_probe_a_idx",
            &table,
            vec!["a".into()],
            false,
            IndexPlacement::Local,
        )
        .expect("ordinary index");
        catalog.write_batch(&ops).expect("write ordinary index");
        let (expression_id, ops) = crabka_pgcatalog::create_index_ops(
            &catalog,
            "size_probe_expr_idx",
            &table,
            vec![crabka_pgcatalog::expression_index_key("(1)")],
            false,
            IndexPlacement::Local,
        )
        .expect("expression index");
        catalog.write_batch(&ops).expect("write expression index");

        let key = crabka_pgkv::key::secondary_index_entry_key(
            table_id,
            ordinary_id,
            &[Datum::Int4(7)],
            1,
        );
        let value = vec![1, 2, 3];
        let expected = key
            .len()
            .checked_add(value.len())
            .and_then(|bytes| i64::try_from(bytes).ok())
            .expect("entry size");
        data.write_batch(&[crabka_pgkv::WriteOp::Put { key, value }])
            .expect("write index entry");

        let scope = crate::relname::ResolutionScope::default();
        let ordinary_relation_oid =
            crate::catalog_rel::index_relation_oid(ordinary_id).expect("index oid");
        let expression_relation_oid =
            crate::catalog_rel::index_relation_oid(expression_id).expect("expression index oid");
        let table_relation_oid =
            crate::catalog_rel::table_relation_oid(table_id).expect("table oid");
        assert!(
            relation_size(&catalog, &data, &scope, &Datum::Int4(ordinary_relation_oid),)
                .expect("ordinary size")
                == Datum::Int8(expected)
        );
        assert!(
            relation_size(
                &catalog,
                &data,
                &scope,
                &Datum::Int4(expression_relation_oid),
            )
            .expect("expression size")
                == Datum::Int8(0)
        );
        assert!(
            relation_size(&catalog, &data, &scope, &Datum::Int4(table_relation_oid),)
                .expect("table size")
                == Datum::Int8(0)
        );
        assert!(
            table_size(&catalog, &data, &scope, &Datum::Int4(ordinary_relation_oid),)
                .expect("index table size")
                == Datum::Int8(expected)
        );
        assert!(
            indexes_size(&catalog, &data, &scope, &Datum::Int4(table_relation_oid))
                .expect("indexes size")
                == Datum::Int8(expected)
        );
        assert!(
            indexes_size(&catalog, &data, &scope, &Datum::Int4(ordinary_relation_oid),)
                .expect("index indexes size")
                == Datum::Int8(0)
        );
        assert!(
            total_relation_size(&catalog, &data, &scope, &Datum::Int4(table_relation_oid),)
                .expect("table total size")
                == Datum::Int8(expected)
        );
        assert!(
            total_relation_size(&catalog, &data, &scope, &Datum::Int4(ordinary_relation_oid),)
                .expect("index total size")
                == Datum::Int8(expected)
        );
        assert!(
            relation_size_with_fork(
                &catalog,
                &data,
                &scope,
                &[
                    Datum::Int4(ordinary_relation_oid),
                    Datum::Text("fsm".into()),
                ],
            )
            .expect("index fsm size")
                == Datum::Int8(0)
        );
        assert!(
            relation_size_with_fork(
                &catalog,
                &data,
                &scope,
                &[Datum::Int4(ordinary_relation_oid), Datum::Null],
            )
            .expect("null fork")
                == Datum::Null
        );
        let invalid_fork = relation_size_with_fork(
            &catalog,
            &data,
            &scope,
            &[
                Datum::Int4(ordinary_relation_oid),
                Datum::Text("toast".into()),
            ],
        )
        .expect_err("invalid fork")
        .into_pg();
        assert!(invalid_fork.code == "22023");
        assert!(invalid_fork.message == "invalid fork name");
        assert!(
            invalid_fork
                .diagnostics
                .and_then(|diagnostics| diagnostics.hint)
                .as_deref()
                == Some("Valid fork names are \"main\", \"fsm\", \"vm\", and \"init\".")
        );
        for size in [
            relation_size(&catalog, &data, &scope, &Datum::Int4(i32::MAX)),
            indexes_size(&catalog, &data, &scope, &Datum::Int4(i32::MAX)),
            total_relation_size(&catalog, &data, &scope, &Datum::Int4(i32::MAX)),
        ] {
            assert!(size.expect("missing oid") == Datum::Null);
        }
    }

    // The spellings captured from a live PostgreSQL 18.4, verbatim — the text
    // `psql`'s `\d` prints after the constraint name.
    #[test]
    fn foreign_key_definitions_match_the_postgres_oracle() {
        let cases: &[DefinitionCase] = &[
            (|_| {}, "FOREIGN KEY (a) REFERENCES pp(id)"),
            (
                |fk| {
                    fk.columns = vec!["c".into()];
                    fk.referenced_columns = vec!["k".into()];
                    fk.on_delete = ReferentialAction::SetDefault;
                    fk.deferrable = true;
                    fk.initially_deferred = true;
                },
                "FOREIGN KEY (c) REFERENCES pp(k) ON DELETE SET DEFAULT DEFERRABLE INITIALLY DEFERRED",
            ),
            (
                |fk| {
                    fk.columns = vec!["b".into()];
                    fk.match_type = MatchType::Full;
                    fk.on_update = ReferentialAction::Cascade;
                    fk.on_delete = ReferentialAction::SetNull;
                },
                "FOREIGN KEY (b) REFERENCES pp(id) MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL",
            ),
            (
                |fk| {
                    fk.on_delete = ReferentialAction::Restrict;
                    fk.validated = false;
                },
                "FOREIGN KEY (a) REFERENCES pp(id) ON DELETE RESTRICT NOT VALID",
            ),
            // The referenced table's key is `(x, y)`; both lists print in the
            // order the FK clause wrote them, never the index's order.
            (
                |fk| {
                    fk.columns = vec!["b".into(), "a".into()];
                    fk.referenced_table = RelationName::public("pperm");
                    fk.referenced_columns = vec!["y".into(), "x".into()];
                },
                "FOREIGN KEY (b, a) REFERENCES pperm(y, x)",
            ),
        ];
        for (configure, expected) in cases {
            assert!(rendered(*configure) == *expected, "{expected}");
        }
    }

    #[test]
    fn foreign_key_clauses_render_in_postgres_order() {
        const BARE: &str = "FOREIGN KEY (a) REFERENCES pp(id)";
        let cases: &[DefinitionCase] = &[
            // Every referential action, on each side. NO ACTION is the default
            // and prints nothing at all.
            (|fk| fk.on_update = ReferentialAction::NoAction, BARE),
            (
                |fk| fk.on_update = ReferentialAction::Restrict,
                "FOREIGN KEY (a) REFERENCES pp(id) ON UPDATE RESTRICT",
            ),
            (
                |fk| fk.on_update = ReferentialAction::Cascade,
                "FOREIGN KEY (a) REFERENCES pp(id) ON UPDATE CASCADE",
            ),
            (
                |fk| fk.on_update = ReferentialAction::SetNull,
                "FOREIGN KEY (a) REFERENCES pp(id) ON UPDATE SET NULL",
            ),
            (
                |fk| fk.on_update = ReferentialAction::SetDefault,
                "FOREIGN KEY (a) REFERENCES pp(id) ON UPDATE SET DEFAULT",
            ),
            (|fk| fk.on_delete = ReferentialAction::NoAction, BARE),
            (
                |fk| fk.on_delete = ReferentialAction::Restrict,
                "FOREIGN KEY (a) REFERENCES pp(id) ON DELETE RESTRICT",
            ),
            (
                |fk| fk.on_delete = ReferentialAction::Cascade,
                "FOREIGN KEY (a) REFERENCES pp(id) ON DELETE CASCADE",
            ),
            (
                |fk| fk.on_delete = ReferentialAction::SetNull,
                "FOREIGN KEY (a) REFERENCES pp(id) ON DELETE SET NULL",
            ),
            (
                |fk| fk.on_delete = ReferentialAction::SetDefault,
                "FOREIGN KEY (a) REFERENCES pp(id) ON DELETE SET DEFAULT",
            ),
            // ON UPDATE comes first even when only ON DELETE is the exotic one.
            (
                |fk| {
                    fk.on_update = ReferentialAction::SetDefault;
                    fk.on_delete = ReferentialAction::Cascade;
                },
                "FOREIGN KEY (a) REFERENCES pp(id) ON UPDATE SET DEFAULT ON DELETE CASCADE",
            ),
            // MATCH SIMPLE is the default and silent; MATCH FULL precedes the
            // actions.
            (|fk| fk.match_type = MatchType::Simple, BARE),
            (
                |fk| fk.match_type = MatchType::Full,
                "FOREIGN KEY (a) REFERENCES pp(id) MATCH FULL",
            ),
            // A SET column list belongs to ON DELETE, and only when stored.
            (
                |fk| {
                    fk.columns = vec!["a".into(), "b".into()];
                    fk.referenced_columns = vec!["id".into(), "k".into()];
                    fk.on_delete = ReferentialAction::SetNull;
                    fk.set_columns = vec!["b".into()];
                },
                "FOREIGN KEY (a, b) REFERENCES pp(id, k) ON DELETE SET NULL (b)",
            ),
            (
                |fk| {
                    fk.on_delete = ReferentialAction::SetDefault;
                    fk.set_columns = vec!["a".into()];
                },
                "FOREIGN KEY (a) REFERENCES pp(id) ON DELETE SET DEFAULT (a)",
            ),
            (
                |fk| {
                    fk.on_delete = ReferentialAction::SetNull;
                    fk.set_columns = Vec::new();
                },
                "FOREIGN KEY (a) REFERENCES pp(id) ON DELETE SET NULL",
            ),
            // A deferrable constraint that is not initially deferred says so
            // with the one word.
            (
                |fk| fk.deferrable = true,
                "FOREIGN KEY (a) REFERENCES pp(id) DEFERRABLE",
            ),
            (
                |fk| {
                    fk.deferrable = true;
                    fk.initially_deferred = true;
                },
                "FOREIGN KEY (a) REFERENCES pp(id) DEFERRABLE INITIALLY DEFERRED",
            ),
            (
                |fk| fk.validated = false,
                "FOREIGN KEY (a) REFERENCES pp(id) NOT VALID",
            ),
            // Every clause at once, which is the whole order in one string.
            (
                |fk| {
                    fk.match_type = MatchType::Full;
                    fk.on_update = ReferentialAction::Cascade;
                    fk.on_delete = ReferentialAction::SetNull;
                    fk.set_columns = vec!["a".into()];
                    fk.deferrable = true;
                    fk.initially_deferred = true;
                    fk.validated = false;
                },
                "FOREIGN KEY (a) REFERENCES pp(id) MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL (a) DEFERRABLE INITIALLY DEFERRED NOT VALID",
            ),
            // Identifiers are quoted on both sides, the referent included.
            (
                |fk| {
                    fk.columns = vec!["Order".into()];
                    fk.referenced_table = RelationName::public("Par Ent");
                    fk.referenced_columns = vec!["user id".into()];
                    fk.on_delete = ReferentialAction::SetNull;
                    fk.set_columns = vec!["Order".into()];
                },
                "FOREIGN KEY (\"Order\") REFERENCES \"Par Ent\"(\"user id\") ON DELETE SET NULL (\"Order\")",
            ),
        ];
        for (configure, expected) in cases {
            assert!(rendered(*configure) == *expected, "{expected}");
        }
    }

    // The referent is unqualified, unlike `pg_get_indexdef`'s `ON public.t`.
    #[test]
    fn the_referenced_relation_is_not_schema_qualified() {
        assert!(!rendered(|_| {}).contains("public."));
    }

    /// A referent the search path does not reach is spelled out, which is what
    /// `generate_relation_name` does once a relation has a schema of its own.
    #[test]
    fn a_referent_outside_the_search_path_is_schema_qualified() {
        assert!(
            rendered(|fk| fk.referenced_table = RelationName::new("app", "pp"))
                == "FOREIGN KEY (a) REFERENCES app.pp(id)"
        );
    }

    /// The referent's spelling is decided by the session's `search_path`, not by
    /// which schema the relation happens to sit in. Each row pairs a path with
    /// the clause PostgreSQL 18.4 renders under it.
    #[test]
    fn a_referent_is_qualified_exactly_when_the_search_path_misses_it() {
        /// A `search_path`, a tweak to the sample key, and the referent the
        /// oracle spells under that path.
        type Case = (&'static [&'static str], fn(&mut ForeignKey), &'static str);

        let cases: [Case; 6] = [
            // `public` is on the default path, so its relations print bare …
            (&["public"], |_| (), "pp"),
            // … and off it, the very same relation has to be spelled out.
            (&["app"], |_| (), "public.pp"),
            (&[], |_| (), "public.pp"),
            // A user schema on the path prints bare, which the fixed
            // "everything but public and pg_catalog is qualified" rule cannot.
            (
                &["app"],
                |fk| fk.referenced_table = RelationName::new("app", "pp"),
                "pp",
            ),
            (
                &["public", "app"],
                |fk| fk.referenced_table = RelationName::new("app", "pp"),
                "pp",
            ),
            // Being on the path is not enough: an earlier entry holding the same
            // relation name shadows this one, so a qualifier is the only
            // spelling that reads back as the same relation.
            (
                &["shadow", "app"],
                |fk| fk.referenced_table = RelationName::new("app", "pp"),
                "app.pp",
            ),
        ];
        for (entries, configure, referent) in cases {
            let mut scope = path(entries);
            let shadowing = referent == "app.pp";
            let kv = MemKv::new();
            for schema in ["app", "shadow"] {
                kv.write_batch(
                    &crabka_pgcatalog::create_schema_ops(&kv, schema, "postgres")
                        .expect("schema ops"),
                )
                .expect("seed schema");
            }
            if shadowing {
                seed_relation(&kv, &RelationName::new("shadow", "pp"));
            }
            scope.user = "postgres".into();
            let mut foreign_key = sample_foreign_key();
            configure(&mut foreign_key);
            let expected = format!("FOREIGN KEY (a) REFERENCES {referent}(id)");
            assert!(
                foreign_key_definition(&kv, &scope, &foreign_key).expect("definition") == expected,
                "search_path = {entries:?}"
            );
        }
    }

    #[test]
    fn a_foreign_key_oid_resolves_to_its_definition() {
        let kv = MemKv::new();
        let scope = crate::relname::ResolutionScope::default();
        let foreign_key = sample_foreign_key();
        kv.write_batch(&crabka_pgcatalog::put_foreign_key_ops(&foreign_key))
            .expect("seed the catalog");
        let oids = crate::catalog_rel::foreign_key_constraint_oids(&kv).expect("foreign key oids");
        let oid = oids[&(
            crabka_pgcatalog::RelationName::public("cc"),
            "cc_a_fkey".to_string(),
        )];
        assert!(
            constraint_def(&kv, &scope, &Datum::Int4(oid)).expect("constraint definition")
                == Datum::Text("FOREIGN KEY (a) REFERENCES pp(id)".into())
        );
        // An oid no constraint holds is NULL, as PostgreSQL answers.
        assert!(
            constraint_def(&kv, &scope, &Datum::Int4(oid + 1)).expect("constraint definition")
                == Datum::Null
        );
    }

    #[test]
    fn identifiers_are_quoted_only_when_they_need_it() {
        assert!(quote_identifier("plain") == "plain");
        assert!(quote_identifier("with_1") == "with_1");
        assert!(quote_identifier("Mixed") == "\"Mixed\"");
        assert!(quote_identifier("has space") == "\"has space\"");
        assert!(quote_identifier("quote\"d") == "\"quote\"\"d\"");
    }
}
