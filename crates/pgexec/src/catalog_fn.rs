//! F-2: the `pg_catalog` functions `psql`'s `\d` family, ORM preambles and
//! migration tools call — object definition reconstruction (`pg_get_viewdef`,
//! `pg_get_indexdef`, `pg_get_constraintdef`), identity (`pg_get_userbyid`,
//! `current_schemas`, `pg_backend_pid`), comments (`obj_description` and
//! friends), sizes, and the `has_*_privilege` family.
//!
//! The family is dispatched from [`eval`](crate::eval) ahead of the older
//! scalar families, exactly like `json_fn`/`array_fn`. What separates it from
//! those is that most of these functions read the catalog: they reach it
//! through [`EvalCtx::catalog`], which is `None` outside a SQL session (a
//! planning context or a unit test), where they report 0A000 rather than
//! silently answering NULL.

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

/// `pg_class`'s own oid — the `classoid` every relation comment carries.
pub(crate) const PG_CLASS_OID: i32 = 1259;
/// First oid of the band reserved for roles.
pub(crate) const ROLE_OID_BASE: i32 = 100_000;
/// The oid of the bootstrap superuser, which owns every object — PostgreSQL's
/// own oid for it, so `pg_get_userbyid(relowner)` needs no special casing on
/// the caller's side.
pub(crate) const BOOTSTRAP_ROLE_OID: i32 = 10;
/// The oid of `pg_database_owner`, the implicit role `public` belongs to —
/// again PostgreSQL's own oid, so a client comparing against 6171 matches.
pub(crate) const DATABASE_OWNER_ROLE_OID: i32 = 6171;
/// PostgreSQL's encoding number for UTF-8.
pub(crate) const UTF8_ENCODING: i32 = 6;
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
    Expr,
    UserById,
    SerialSequence,
    /// A definition-reconstruction function for an object kind crabka has none
    /// of; always NULL, like PostgreSQL's answer for a missing oid.
    NullDef,
    /// P2: the `pg_get_function*` family over the routine catalog.
    RoutineDef(RoutineDefKind),
    IsVisible,
    RelationSize,
    /// A size over a whole database or tablespace, whose argument is a name or
    /// oid of that object rather than a relation.
    ClusterSize,
    SizePretty,
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
    InRecovery,
    TablespaceLocation,
}

/// Classify a (lowercased) function name.
fn catalog_func(name: &str) -> Option<CatalogFunc> {
    use CatalogFunc::{
        BackendPid, CharToEncoding, ClusterSize, ColDescription, ConstraintDef, CurrentSchemas,
        EncodingToChar, Expr as ExprDef, HasPrivilege, HasRole, InRecovery, IndexDef,
        IsPublishable, IsVisible, NullDef, ObjDescription, RelationSize, RoutineDef,
        SerialSequence, ShobjDescription, SizePretty, StartTime, TablespaceLocation, UserById,
        ViewDef,
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
        "pg_get_ruledef"
        | "pg_get_triggerdef"
        | "pg_get_partkeydef"
        | "pg_get_statisticsobjdef" => NullDef,
        "pg_type_is_visible"
        | "pg_function_is_visible"
        | "pg_opclass_is_visible"
        | "pg_operator_is_visible"
        | "pg_collation_is_visible"
        | "pg_conversion_is_visible"
        | "pg_statistics_obj_is_visible"
        | "pg_ts_config_is_visible" => IsVisible,
        "pg_relation_size" | "pg_total_relation_size" | "pg_table_size" | "pg_indexes_size" => {
            RelationSize
        }
        "pg_database_size" | "pg_tablespace_size" => ClusterSize,
        "pg_size_pretty" => SizePretty,
        "obj_description" => ObjDescription,
        "col_description" => ColDescription,
        "shobj_description" => ShobjDescription,
        "current_schemas" => CurrentSchemas,
        "pg_backend_pid" => BackendPid,
        "pg_postmaster_start_time" | "pg_conf_load_time" => StartTime,
        "pg_encoding_to_char" => EncodingToChar,
        "pg_char_to_encoding" => CharToEncoding,
        "pg_relation_is_publishable" => IsPublishable,
        "pg_is_in_recovery" => InRecovery,
        "pg_tablespace_location" => TablespaceLocation,
        _ if is_privilege_func(name) => HasPrivilege,
        "pg_has_role" => HasRole,
        _ => return None,
    })
}

/// The `has_<objectkind>_privilege` family, which shares one implementation.
fn is_privilege_func(name: &str) -> bool {
    matches!(
        name,
        "has_table_privilege"
            | "has_column_privilege"
            | "has_any_column_privilege"
            | "has_database_privilege"
            | "has_schema_privilege"
            | "has_sequence_privilege"
            | "has_function_privilege"
            | "has_language_privilege"
            | "has_server_privilege"
            | "has_foreign_data_wrapper_privilege"
            | "has_tablespace_privilege"
            | "has_type_privilege"
            | "has_parameter_privilege"
            | "has_largeobject_privilege"
    )
}

/// Is `name` one of this family's functions? The dispatch point in
/// [`eval`](crate::eval).
pub(crate) fn is_catalog_func(name: &str) -> bool {
    catalog_func(name).is_some()
}

/// Statically infer a catalog call's result type, validating arity.
///
/// # Errors
///
/// 42883 for an unknown name or a bad arity.
pub(crate) fn catalog_func_result_type(
    fc: &FuncCall,
    _scope: &Scope,
) -> Result<ColumnType, ExecError> {
    use CatalogFunc::{
        BackendPid, CharToEncoding, ClusterSize, CurrentSchemas, HasPrivilege, HasRole, InRecovery,
        IsPublishable, IsVisible, RelationSize, StartTime,
    };
    let f = catalog_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let n = crate::func::checked_args(fc)?.len();
    require_arity(fc, arity_ok(f, n))?;
    Ok(match f {
        IsVisible | HasPrivilege | HasRole | IsPublishable | InRecovery => ColumnType::Bool,
        RelationSize | ClusterSize => ColumnType::Int8,
        BackendPid | CharToEncoding => ColumnType::Int4,
        StartTime => ColumnType::Timestamptz,
        CurrentSchemas => ColumnType::Array(ElemType::Text),
        _ => ColumnType::Text,
    })
}

/// The accepted argument counts, per function.
fn arity_ok(f: CatalogFunc, n: usize) -> bool {
    use CatalogFunc::{
        BackendPid, CharToEncoding, ClusterSize, ColDescription, ConstraintDef, CurrentSchemas,
        EncodingToChar, Expr as ExprDef, HasPrivilege, HasRole, InRecovery, IndexDef,
        IsPublishable, IsVisible, NullDef, ObjDescription, RelationSize, SerialSequence,
        ShobjDescription, SizePretty, StartTime, TablespaceLocation, UserById, ViewDef,
    };
    match f {
        ViewDef | ConstraintDef | NullDef => n == 1 || n == 2,
        CatalogFunc::RoutineDef(_) => n == 1,
        IndexDef => n == 1 || n == 3,
        ExprDef => n == 2 || n == 3,
        UserById | IsVisible | SizePretty | CurrentSchemas | EncodingToChar | CharToEncoding
        | IsPublishable | TablespaceLocation => n == 1,
        SerialSequence | ColDescription | ShobjDescription | HasRole => n == 2 || n == 3,
        ObjDescription | RelationSize => n == 1 || n == 2,
        ClusterSize => n == 1,
        BackendPid | StartTime | InRecovery => n == 0,
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
        HasRole, InRecovery, IsPublishable, IsVisible, NullDef, SizePretty, StartTime,
        TablespaceLocation,
    };
    match f {
        NullDef | TablespaceLocation => Ok(Datum::Null),
        IsVisible | IsPublishable => Ok(visibility_answer(&vals[0])),
        InRecovery => Ok(Datum::Bool(false)),
        // A whole-database or whole-tablespace size names something that is not
        // a relation, so its argument is taken as given rather than resolved.
        ClusterSize => Ok(cluster_size(&vals[0])),
        BackendPid => Ok(Datum::Int4(ctx.backend_pid)),
        StartTime => Ok(Datum::Timestamptz(process_start_time())),
        CurrentSchemas => current_schemas(&vals[0], ctx),
        EncodingToChar => Ok(encoding_to_char(&vals[0])),
        CharToEncoding => Ok(char_to_encoding(&vals[0])),
        SizePretty => size_pretty(&vals[0]),
        HasPrivilege => has_privilege(name, vals, ctx),
        HasRole => Ok(Datum::Bool(true)),
        _ => eval_catalog_reading(f, vals, ctx),
    }
}

/// The half of the family that reads the catalog.
fn eval_catalog_reading(f: CatalogFunc, vals: &[Datum], ctx: &EvalCtx) -> Result<Datum, ExecError> {
    use CatalogFunc::{
        ColDescription, ConstraintDef, Expr as ExprDef, IndexDef, ObjDescription, RelationSize,
        SerialSequence, ShobjDescription, UserById, ViewDef,
    };
    let kv = ctx.catalog().ok_or_else(catalog_unavailable)?;
    // Every function here that takes a relation *name* resolves it the way the
    // session resolves one, so `pg_get_viewdef('v')` sees what `SELECT … FROM v`
    // sees.
    let scope = ctx.resolution();
    match f {
        ViewDef => view_def(kv, scope, vals),
        IndexDef => index_def(kv, scope, &vals[0]),
        ConstraintDef => constraint_def(kv, &vals[0]),
        // crabka stores a default/`CHECK` predicate as source text, so
        // "decompiling" it is the identity on the stored text.
        ExprDef => Ok(vals[0].clone()),
        UserById => user_by_id(kv, &vals[0]),
        SerialSequence => serial_sequence(kv, scope, vals),
        RelationSize => relation_size(kv, scope, &vals[0]),
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
        _ => Ok(Datum::Null),
    }
}

/// The `pg_get_function*` family. A reference that resolves to no routine is
/// NULL, exactly as `PostgreSQL` answers for an oid that no longer exists.
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

/// Everything crabka exposes lives in a schema on the search path, so a
/// visibility test is true for any non-NULL oid and NULL for a NULL one.
fn visibility_answer(oid: &Datum) -> Datum {
    match oid {
        Datum::Null => Datum::Null,
        _ => Datum::Bool(true),
    }
}

/// When this process started, as `pg_postmaster_start_time()` reports it.
///
/// Read from the kernel rather than captured on first call, because a lazily
/// captured instant is LATER than the statement's `now()` and would make every
/// uptime query report a negative age. Where the kernel cannot say (a non-Linux
/// host), the first call's instant is the honest fallback.
fn process_start_time() -> jiff::Timestamp {
    static START: std::sync::OnceLock<jiff::Timestamp> = std::sync::OnceLock::new();
    *START.get_or_init(|| {
        boot_relative_start_time().unwrap_or_else(|| {
            jiff::Timestamp::from_microsecond(jiff::Timestamp::now().as_microsecond())
                .unwrap_or(jiff::Timestamp::UNIX_EPOCH)
        })
    })
}

/// This process's start instant from `/proc`: the boot time in `/proc/stat`
/// plus this process's start offset (field 22 of `/proc/self/stat`, in clock
/// ticks). `None` on any host that does not publish both.
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

/// `current_schemas(include_implicit)` — the session's search path as an array,
/// in path order and filtered to the entries that name an existing schema.
///
/// `include_implicit` adds the two entries PostgreSQL prepends when the path
/// does not already name them: the session's temporary namespace, and then
/// `pg_catalog`. Verified against PostgreSQL 18.4: under the default path
/// `current_schemas(false)` is `{public}` while `current_schemas(true)` is
/// `{pg_catalog,public}`; after a `CREATE TEMP TABLE` the latter becomes
/// `{pg_temp_1,pg_catalog,public}`; after `SET search_path = pg_catalog, public`
/// both are `{pg_catalog,public}`; and an entry naming no existing schema is
/// silently skipped rather than reported. The function is strict, so a NULL
/// argument answers NULL.
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

/// crabka is UTF-8 only, so only PostgreSQL's UTF8 encoding number has a name.
fn encoding_to_char(encoding: &Datum) -> Datum {
    match int_arg(encoding) {
        Ok(n) if i32::try_from(n) == Ok(UTF8_ENCODING) => Datum::Text("UTF8".into()),
        // PostgreSQL answers the empty string for an out-of-range encoding id.
        Ok(_) => Datum::Text(String::new()),
        Err(_) => Datum::Null,
    }
}

fn char_to_encoding(name: &Datum) -> Datum {
    match name {
        Datum::Text(text) if text.eq_ignore_ascii_case("UTF8") => Datum::Int4(UTF8_ENCODING),
        Datum::Text(_) => Datum::Int4(-1),
        _ => Datum::Null,
    }
}

/// `pg_size_pretty(bigint)`, byte for byte as PostgreSQL formats it: the value
/// is shifted one unit at a time, keeping one extra bit so the final halving
/// rounds rather than truncates.
fn size_pretty(size: &Datum) -> Result<Datum, ExecError> {
    if matches!(size, Datum::Null) {
        return Ok(Datum::Null);
    }
    let mut value = int_arg(size)?;
    let limit = 10 * 1024;
    let limit2 = limit * 2 - 1;
    if value.abs() < limit {
        return Ok(Datum::Text(format!("{value} bytes")));
    }
    for unit in ["kB", "MB", "GB", "TB", "PB"] {
        value >>= if unit == "kB" { 9 } else { 10 };
        if value.abs() < limit2 || unit == "PB" {
            // PostgreSQL's `half_rounded`: the extra bit is halved away with
            // ties rounded AWAY from zero, so -20 half-units is -10, not -9.
            let rounded = i64::midpoint(value, if value < 0 { -1 } else { 1 });
            return Ok(Datum::Text(format!("{rounded} {unit}")));
        }
    }
    Ok(Datum::Text(format!("{value} bytes")))
}

/// The `has_*_privilege` family. crabka's single bootstrap role owns every
/// object, so every recognized privilege on an existing object is held; an
/// unrecognized privilege name is PostgreSQL's 22023, which is what callers
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
    // A relation-scoped test must still fail on a missing relation; the object
    // argument is the one before the privilege name.
    if (name == "has_table_privilege" || name == "has_any_column_privilege")
        && let (Some(object), Some(kv)) = (vals.get(vals.len() - 2), ctx.catalog())
    {
        resolve_relation_oid(kv, ctx.resolution(), object)?;
    }
    Ok(Datum::Bool(true))
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

/// crabka has no physical storage accounting: every relation reports zero
/// bytes rather than a fabricated page count. The functions still resolve their
/// relation argument, so a missing relation is 42P01 as in PostgreSQL.
fn relation_size(kv: &dyn Kv, scope: &ResolutionScope, object: &Datum) -> Result<Datum, ExecError> {
    if matches!(object, Datum::Null) {
        return Ok(Datum::Null);
    }
    resolve_relation_oid(kv, scope, object)?;
    Ok(Datum::Int8(0))
}

/// `pg_database_size`/`pg_tablespace_size`. crabka keeps no storage accounting,
/// so the answer is zero bytes for any non-NULL argument.
fn cluster_size(object: &Datum) -> Datum {
    match object {
        Datum::Null => Datum::Null,
        _ => Datum::Int8(0),
    }
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
        other => i32::try_from(int_arg(other)?)
            .map_err(|_| ExecError::Unsupported("oid exceeds int4 range".into())),
    }
}

/// Resolve a written relation name across every relation kind, not just base
/// tables.
///
/// The text is a `regclass` input, read by [`parse_written_relation`] — so a
/// quoted part keeps its case and may hold a dot, and an unquoted one downcases
/// — and then resolved the way `scope` resolves any written name: a qualifier
/// names the schema outright, and a bare name is looked for in each visible
/// schema in path order.
pub(crate) fn resolve_relation_in_scope(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    name: &str,
) -> Result<i32, ExecError> {
    let written = parse_written_relation(name)?;
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
/// relation *name*, which is the only shape a search path applies to: `t` under
/// `search_path = s1` is `s1.t`, exactly as `SELECT … FROM t` reads it. Every
/// other operand — an oid in any of its spellings, `-`, a `regclass` already —
/// names its relation outright and takes [`crate::exec::regclass_cast`].
pub(crate) fn regclass_cast(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    value: &Datum,
) -> Result<Option<Datum>, ExecError> {
    let Some(name) = relation_name_operand(value) else {
        return crate::exec::regclass_cast(kv, scope, value);
    };
    let oid = resolve_relation_in_scope(kv, scope, name)?;
    crate::exec::regclass_by_oid(kv, oid)
        .map(Datum::Regclass)
        .map(Some)
}

/// The operand text that spells a relation name rather than an oid —
/// `regclassin`'s own test, which takes `-` and an all-digit string as oids
/// before it reads anything as a name.
fn relation_name_operand(value: &Datum) -> Option<&str> {
    let Datum::Text(text) = value else {
        return None;
    };
    let trimmed = text.trim();
    (trimmed != "-" && trimmed.parse::<i32>().is_err()).then_some(text.as_str())
}

/// The `pg_class` oid of the relation stored under exactly this catalog name —
/// a virtual catalog relation, a table, a view, a sequence or an index — or
/// `None` when no relation of any kind answers to it.
///
/// [`crate::relname::resolve_relation`] resolves the three kinds the catalog
/// keys by name; `regclass` also accepts an index and a virtual catalog
/// relation, so the search-path walk lives in [`resolve_relation_in_scope`] and
/// this is the per-schema probe it repeats.
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

/// The catalog name of a virtual relation, which [`crate::exec`] lists under a
/// flat spelling: an `information_schema.` qualifier names that schema, and
/// every unqualified spelling is a `pg_catalog` relation.
fn virtual_relation_name(spelled: &str) -> RelationName {
    match spelled.split_once('.') {
        Some((schema, name)) => RelationName::new(schema, name),
        None => RelationName::new(crate::search_path::PG_CATALOG, spelled),
    }
}

/// The catalog name a written relation reference denotes, resolved through the
/// one resolver. Used where the answer is a *name* rather than an oid, and where
/// only the catalog-keyed relation kinds can be meant.
fn resolve_relation_name(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    name: &str,
) -> Result<RelationName, ExecError> {
    crate::relname::resolve_relation(
        kv,
        scope,
        &parse_written_relation(name)?.reference,
        crate::relname::SchemaDisposition::Reference,
    )
}

/// The inverse of [`resolve_relation_in_scope`]: the name `regclassout` prints
/// for a `pg_class` oid, or `None` when no relation has that oid.
///
/// The name is spelled as PostgreSQL spells it — each identifier quoted only
/// when `quote_ident` would, and schema-qualified only when the schema is
/// outside the search path (`public` and `pg_catalog` print bare, so a catalog
/// name carrying no schema prefix already has the right shape). Every spelling
/// this produces has to read back through
/// [`crate::relname::parse_written_relation`] as the same relation, which is
/// the round trip `regclass_input.rs` pins.
pub(crate) fn relation_name_by_oid(kv: &dyn Kv, oid: i32) -> Result<Option<String>, ExecError> {
    for virtual_table in crate::exec::virtual_table_names() {
        if crate::exec::virtual_relation_oid(virtual_table) == oid {
            return Ok(Some(quote_relation_name(&virtual_relation_name(
                virtual_table,
            ))));
        }
    }
    for table in crabka_pgcatalog::list_tables(kv)? {
        if i64::from(oid) == i64::from(table.id) {
            return Ok(Some(quote_relation_name(&table.name)));
        }
    }
    for map in [
        crate::catalog_rel::view_oids(kv)?,
        crate::catalog_rel::sequence_oids(kv)?,
    ] {
        if let Some((name, _)) = map.iter().find(|(_, candidate)| **candidate == oid) {
            return Ok(Some(quote_relation_name(name)));
        }
    }
    for index in crabka_pgcatalog::list_indexes(kv)? {
        if crate::catalog_rel::index_relation_oid(index.id)? == oid {
            return Ok(Some(quote_relation_name(&index.qualified_name())));
        }
    }
    Ok(None)
}

/// Spell a catalog relation name the way `regclassout` would: `schema.relation`
/// with each half quoted as needed, dropping the schema for the two that are
/// always on the search path.
fn quote_relation_name(name: &RelationName) -> String {
    if name.schema == crabka_pgcatalog::PUBLIC_SCHEMA
        || name.schema == crate::search_path::PG_CATALOG
    {
        crate::string_fn::quote_ident(&name.name)
    } else {
        format!(
            "{}.{}",
            crate::string_fn::quote_ident(&name.schema),
            crate::string_fn::quote_ident(&name.name)
        )
    }
}

/// `pg_get_userbyid(oid)` — the role name for a role oid. PostgreSQL answers
/// `unknown (OID=n)` for an oid no role has, which `\dt`'s Owner column shows
/// verbatim, so the fallback is reproduced exactly.
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

/// `pg_get_serial_sequence(table, column)` — the sequence a serial/identity
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
            qualified_sequence_name(kv, &table.name, sequence)?,
        )),
        _ => Ok(Datum::Null),
    }
}

/// `pg_get_serial_sequence`'s answer for a stored `nextval` default: the
/// sequence's name, always schema-qualified — PostgreSQL qualifies it even in
/// `public` because the text is meant to be handed straight back to
/// `nextval`/`setval`.
///
/// A default stores the sequence as it is *spelled* for `nextval`, which is bare
/// in `public` and dotted elsewhere, so the text alone cannot say which schema it
/// names. The sequence's own catalog record can, and a sequence the catalog no
/// longer holds falls back to its table's schema — where `PostgreSQL` puts the
/// sequence a serial column owns.
fn qualified_sequence_name(
    kv: &dyn Kv,
    table: &RelationName,
    spelled: &str,
) -> Result<String, ExecError> {
    let sequence = crabka_pgcatalog::list_sequences(kv)?
        .into_iter()
        .map(|(name, _)| name)
        .find(|name| name.to_string() == spelled)
        .unwrap_or_else(|| table.sibling(spelled));
    Ok(format!(
        "{}.{}",
        quote_identifier(&sequence.schema),
        quote_identifier(&sequence.name)
    ))
}

/// `obj_description`/`col_description` — the comment on an object, or on one of
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
        if i64::from(oid) != i64::from(table.id) {
            continue;
        }
        if subid == 0 {
            return comment_datum(kv, "table", CommentObject::Relation(&table.name));
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

/// `pg_get_viewdef` in each of its overloads. The second argument is either the
/// pretty-print flag or a wrap column; a wrap column implies pretty-printing,
/// exactly as PostgreSQL's `pg_get_viewdef(oid, integer)` does.
fn view_def(kv: &dyn Kv, scope: &ResolutionScope, vals: &[Datum]) -> Result<Datum, ExecError> {
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
    let Some(view) = lookup_view(kv, scope, &vals[0])? else {
        return Ok(Datum::Text("Not a view".into()));
    };
    Ok(Datum::Text(view_definition(&view, pretty, wrap)))
}

/// Find the view an oid or a name refers to, or `None` when the argument names
/// something that is not a view (PostgreSQL answers the literal `Not a view`).
///
/// A name is resolved through the session's search path exactly as a `regclass`
/// cast resolves one, so a view the path reaches is found under its own schema;
/// a name no relation answers to is not a view either.
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
/// The stored text is re-parsed and printed from the tree, so the answer is
/// normalized rather than echoed: keyword lines carry PostgreSQL's indentation,
/// output columns are named from the view's catalog column list, and — when
/// `pretty` is false — operator expressions are fully parenthesized. A stored
/// definition that no longer parses falls back to the source text, which is
/// still a valid view definition.
pub(crate) fn view_definition_text(view: &View, pretty: bool) -> String {
    view_definition(view, pretty, None)
}

/// Is a view auto-updatable — PostgreSQL's `is_updatable`/`is_insertable_into`?
///
/// A view is auto-updatable when its body is a plain `SELECT` over exactly one
/// relation with no `DISTINCT`, grouping, set operation, `LIMIT`/`OFFSET` or
/// window function, which is PostgreSQL's own `view_query_is_auto_updatable`
/// test minus the checks for object kinds crabka has none of.
pub(crate) fn view_is_auto_updatable(view: &View) -> bool {
    use crabka_pgparser::ast::{DistinctClause, QueryBody, SetExpr, Statement, TableExpr};
    let Ok(statements) = crabka_pgparser::parse(&view.definition) else {
        return false;
    };
    let [Statement::Query(query)] = statements.as_slice() else {
        return false;
    };
    let SetExpr::Query(QueryBody::Select(select)) = &query.body else {
        return false;
    };
    matches!(select.from.as_slice(), [TableExpr::Table { .. }])
        && matches!(select.distinct, DistinctClause::All)
        && select.group_by.is_empty()
        && select.having.is_none()
        && select.window_calls.is_empty()
        && select.limit.is_none()
        && select.offset.is_none()
        && query.limit.is_none()
        && query.offset.is_none()
        && query.with.is_none()
}

/// [`view_definition_text`] with an explicit select-list wrap column.
fn view_definition(view: &View, pretty: bool, wrap: Option<usize>) -> String {
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
    crate::viewdef::write_query(&mut out, query, &names, pretty, wrap);
    out.push(';');
    out
}

/// `pg_get_indexdef(oid)` — the `CREATE INDEX` statement that rebuilds an index.
fn index_def(kv: &dyn Kv, scope: &ResolutionScope, object: &Datum) -> Result<Datum, ExecError> {
    if matches!(object, Datum::Null) {
        return Ok(Datum::Null);
    }
    let oid = resolve_relation_oid(kv, scope, object)?;
    for index in crabka_pgcatalog::list_indexes(kv)? {
        if crate::catalog_rel::index_relation_oid(index.id)? != oid {
            continue;
        }
        let table = crabka_pgcatalog::get_table(kv, &index.table)?;
        return Ok(Datum::Text(index_definition(&index, &table)));
    }
    Ok(Datum::Null)
}

/// The `CREATE INDEX` text for one index, schema-qualified like PostgreSQL's.
///
/// `pg_get_indexdef` genuinely qualifies the table it indexes — even in
/// `public`, which is why the schema is spelled out unconditionally.
/// [`foreign_key_definition`] deliberately leaves its referent unqualified when
/// it is visible — read the note there before making the two agree.
pub(crate) fn index_definition(index: &Index, table: &Table) -> String {
    format!(
        "CREATE {}INDEX {} ON {}.{} USING btree ({})",
        if index.unique { "UNIQUE " } else { "" },
        quote_identifier(&index.name),
        quote_identifier(&table.name.schema),
        quote_identifier(&table.name.name),
        quoted_column_list(&index.columns),
    )
}

/// `pg_get_constraintdef(oid)` — the constraint clause that rebuilds a
/// constraint, in the spelling `ALTER TABLE … ADD CONSTRAINT` takes.
fn constraint_def(kv: &dyn Kv, object: &Datum) -> Result<Datum, ExecError> {
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
        };
        return Ok(Datum::Text(format!(
            "{keyword} ({})",
            quoted_column_list(&index.columns)
        )));
    }
    if let Some(definition) = foreign_key_constraint_def(kv, wanted)? {
        return Ok(Datum::Text(definition));
    }
    check_constraint_def(kv, wanted)
}

/// The `FOREIGN KEY …` clause for the foreign key an oid names, or `None` when
/// the oid is outside the foreign-key band.
fn foreign_key_constraint_def(kv: &dyn Kv, wanted: i32) -> Result<Option<String>, ExecError> {
    let oids = crate::catalog_rel::foreign_key_constraint_oids(kv)?;
    for foreign_key in crabka_pgcatalog::list_foreign_keys(kv)? {
        let key = format!("{}.{}", foreign_key.table, foreign_key.name);
        if oids.get(&key) == Some(&wanted) {
            return Ok(Some(foreign_key_definition(&foreign_key)));
        }
    }
    Ok(None)
}

/// The `FOREIGN KEY …` clause for one foreign key, in PostgreSQL's clause
/// order: referencing columns, the referent, `MATCH FULL`, `ON UPDATE` before
/// `ON DELETE`, the deferral, and `NOT VALID` last. `MATCH SIMPLE` and
/// `NO ACTION` are the defaults and print nothing, and only `ON DELETE` can
/// carry a `SET NULL`/`SET DEFAULT` column list.
///
/// `psql`'s `\d` builds both its `Foreign-key constraints:` block and the
/// parent's `Referenced by:` block out of this text verbatim, adding only the
/// indent, the quoted constraint name and the `TABLE … CONSTRAINT …` prefix.
///
/// The referent is emitted **unqualified** when it is visible —
/// `REFERENCES pp(id)`, never `public.pp(id)`. PostgreSQL renders it with
/// `generate_relation_name`, which omits the schema of a relation the search
/// path reaches and spells out the schema of one it does not. That is a real
/// asymmetry with [`index_definition`], whose `ON public.t` is qualified because
/// `pg_get_indexdef` genuinely qualifies; neither should be "fixed" to match the
/// other.
pub(crate) fn foreign_key_definition(foreign_key: &ForeignKey) -> String {
    let mut out = format!(
        "FOREIGN KEY ({}) REFERENCES {}({})",
        quoted_column_list(&foreign_key.columns),
        quote_relation_name(&foreign_key.referenced_table),
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
    out
}

/// How a referential action spells itself in a constraint definition. `NO
/// ACTION` is the default and PostgreSQL leaves the clause out entirely.
fn referential_action_clause(action: ReferentialAction) -> Option<&'static str> {
    match action {
        ReferentialAction::NoAction => None,
        ReferentialAction::Restrict => Some("RESTRICT"),
        ReferentialAction::Cascade => Some("CASCADE"),
        ReferentialAction::SetNull => Some("SET NULL"),
        ReferentialAction::SetDefault => Some("SET DEFAULT"),
    }
}

/// A parenthesized-list body: each name quoted as needed, comma-separated, in
/// the order given — never sorted, because a foreign key's columns pair
/// positionally with the referenced ones.
fn quoted_column_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| quote_identifier(column))
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
) -> String {
    match default {
        crabka_pgcatalog::ColumnDefault::NextVal(sequence) => {
            format!("nextval('{}'::regclass)", sequence.replace('\'', "''"))
        }
        // A `regclass` default stores only the oid, so the name it deparses to
        // is read from the catalog now: a `RENAME` of the relation changes what
        // `\d` and `pg_get_expr` print, as it does in PostgreSQL.
        crabka_pgcatalog::ColumnDefault::Value(Datum::Regclass(value)) => {
            let resolved = crate::exec::regclass_by_oid(kv, value.oid)
                .unwrap_or_else(|_| crabka_pgtypes::RegclassValue::unresolved(value.oid));
            crate::viewdef::const_text(&Datum::Regclass(resolved), ty)
        }
        crabka_pgcatalog::ColumnDefault::Value(value) => crate::viewdef::const_text(value, ty),
    }
}

/// Quote an identifier the way PostgreSQL's `quote_ident` does: bare when it is
/// a lowercase-safe identifier, double-quoted otherwise.
pub(crate) fn quote_identifier(name: &str) -> String {
    let safe = !name.is_empty()
        && !name.starts_with(|c: char| c.is_ascii_digit())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if safe {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgcatalog::{ForeignKey, MatchType, ReferentialAction, RelationName};
    use crabka_pgkv::{Kv, MemKv};
    use crabka_pgtypes::Datum;

    use super::{
        catalog_func, constraint_def, foreign_key_definition, is_catalog_func, quote_identifier,
        size_pretty,
    };

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
        let mut foreign_key = sample_foreign_key();
        configure(&mut foreign_key);
        foreign_key_definition(&foreign_key)
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
            "obj_description",
            "col_description",
            "shobj_description",
            "current_schemas",
            "pg_backend_pid",
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
        ];
        for (input, expected) in cases {
            let got = size_pretty(&Datum::Int8(input)).expect("size_pretty");
            assert!(got == Datum::Text(expected.into()), "{input}");
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

    #[test]
    fn a_foreign_key_oid_resolves_to_its_definition() {
        let kv = MemKv::new();
        let foreign_key = sample_foreign_key();
        kv.write_batch(&crabka_pgcatalog::put_foreign_key_ops(&foreign_key))
            .expect("seed the catalog");
        let oids = crate::catalog_rel::foreign_key_constraint_oids(&kv).expect("foreign key oids");
        let oid = oids["cc.cc_a_fkey"];
        assert!(
            constraint_def(&kv, &Datum::Int4(oid)).expect("constraint definition")
                == Datum::Text("FOREIGN KEY (a) REFERENCES pp(id)".into())
        );
        // An oid no constraint holds is NULL, as PostgreSQL answers.
        assert!(
            constraint_def(&kv, &Datum::Int4(oid + 1)).expect("constraint definition")
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
