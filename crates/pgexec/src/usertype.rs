//! User-defined types: `CREATE`/`ALTER`/`DROP` of `TYPE` and `DOMAIN`, and the
//! value-level operations over the types they create.
//!
//! The durable definition lives in the catalog (`crabka_pgcatalog`). The
//! process-wide registry in `crabka_pgtypes::usertype` is what makes a type
//! *name* resolvable from the parser, which holds no catalog handle. Every DDL
//! successful DDL publishes the catalog delta through the session boundary, and
//! [`hydrate`] restores the registry from the catalog when a session opens so a
//! restart or a second node still resolves the names.

use std::collections::HashSet;

use crabka_pgcatalog::RelationName;
use crabka_pgkv::{Kv, WriteOp};
use crabka_pgparser::ast::{
    AlterDomainAction, AlterTypeAction, BaseTypeOption, BaseTypeOptionValue, CompositeFieldDef,
    CreateTypeDefinition, DomainConstraint, EnumValuePosition,
};
use crabka_pgtypes::{
    ColumnType, Datum,
    usertype::{
        self, BaseBody, CompositeField, DomainBody, DomainCheck, RangeBody, UserType, UserTypeBody,
    },
};
use crabka_pgwire::engine::QueryResult;

use crate::{
    error::ExecError,
    relname::{SchemaDisposition, resolve_relation},
};

/// Load every catalog-stored type into the process registry.
///
/// Idempotent: a second registration of a type keeps its oid, so a call on
/// every session open costs a catalog scan and changes nothing else.
///
/// # Errors
///
/// Propagates catalog read errors.
pub fn hydrate(kv: &dyn Kv) -> Result<(), ExecError> {
    crabka_pgcatalog::hydrate_user_types(kv)?;
    Ok(())
}

/// `CREATE TYPE name AS { (…) | ENUM (…) | RANGE (…) }`.
///
/// # Errors
///
/// 42710 when the name is taken, 0A000 for a base type gres cannot represent,
/// and 42P16/42701 for a malformed composite.
pub fn create_type(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    name: &RelationName,
    definition: &CreateTypeDefinition,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    let lookup_name = name.to_string();
    let body = match definition {
        CreateTypeDefinition::Composite(fields) => {
            UserTypeBody::Composite(composite_fields(fields)?)
        }
        CreateTypeDefinition::Enum(labels) => {
            UserTypeBody::Enum(enum_labels(&lookup_name, labels)?)
        }
        CreateTypeDefinition::Range {
            subtype,
            collation,
            multirange_type_name,
        } => {
            let (schema, companion) = match multirange_type_name {
                Some(companion) => companion_identity(kv, resolution, companion)?,
                None => (
                    name.schema.clone(),
                    usertype::default_multirange_name(&name.name),
                ),
            };
            UserTypeBody::Range(RangeBody {
                subtype: *subtype,
                collation: collation.clone(),
                multirange_schema: Some(schema),
                multirange_name: Some(companion),
            })
        }
        CreateTypeDefinition::Shell => UserTypeBody::Shell,
        CreateTypeDefinition::Base(options) => {
            return create_base_type(kv, name, options);
        }
    };
    register(kv, name, body, "CREATE TYPE")
}

/// `CREATE DOMAIN name [AS] base [constraint …]`.
///
/// # Errors
///
/// 42710 when the name is taken, and 42601 when the constraint list is
/// contradictory.
pub fn create_domain(
    kv: &dyn Kv,
    name: &RelationName,
    base: ColumnType,
    constraints: &[DomainConstraint],
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    if matches!(base, ColumnType::Record(_)) {
        return Err(ExecError::Unsupported(
            "domains over composite types are not supported".into(),
        ));
    }
    let mut body = DomainBody {
        base,
        not_null: false,
        default: None,
        checks: Vec::new(),
    };
    let mut unnamed = 0usize;
    for constraint in constraints {
        match constraint {
            DomainConstraint::Default(expr) => body.default = Some(expr.clone()),
            DomainConstraint::NotNull => body.not_null = true,
            DomainConstraint::Null => body.not_null = false,
            DomainConstraint::Check {
                name: check_name,
                text,
            } => {
                unnamed += 1;
                let check_name = check_name
                    .clone()
                    .unwrap_or_else(|| default_check_name(&name.name, unnamed));
                body.checks.push(DomainCheck {
                    name: check_name,
                    expr: text.clone(),
                });
            }
        }
    }
    register(kv, name, UserTypeBody::Domain(body), "CREATE DOMAIN")
}

/// `CREATE TYPE name (INPUT = …, OUTPUT = …, LIKE = …)` — a user-defined base
/// type, completing a shell of the same name when one is already there.
///
/// gres represents a base type's values in the `Datum` of the type `LIKE`
/// names, so `LIKE` is *required* here even though PostgreSQL treats it as one
/// way of several to describe the layout. `INTERNALLENGTH = 24` with no `LIKE`
/// describes a byte image gres has no value for, and inventing an opaque one
/// would buy a `CREATE TYPE` that succeeds and a first `SELECT` that cannot.
///
/// # Errors
///
/// 42P17 when `INPUT` or `OUTPUT` is missing, 42710 when the name is taken by
/// a defined type, and 0A000 for an option gres cannot honour.
fn create_base_type(
    kv: &dyn Kv,
    name: &RelationName,
    options: &[BaseTypeOption],
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    let mut input = None;
    let mut output = None;
    let mut representation = None;
    let mut category = None;
    let mut preferred = false;
    let mut delimiter = ",".to_string();
    for option in options {
        match option.name.as_str() {
            "input" => input = Some(option_name(option)?),
            "output" => output = Some(option_name(option)?),
            "like" => representation = Some(option_type(option)?),
            "category" => category = Some(option_char(option)?),
            "preferred" => preferred = option_bool(option)?,
            "delimiter" => delimiter = option_string(option)?,
            // Every remaining option describes a layout, a companion routine or
            // an element type that gres has nowhere to put. Refusing is the
            // whole difference between "not supported" and a type that exists
            // but misbehaves.
            other => {
                return Err(ExecError::Unsupported(format!(
                    "type attribute \"{other}\" is not supported: gres builds a base type only \
                     from LIKE, INPUT, OUTPUT, CATEGORY, PREFERRED and DELIMITER"
                )));
            }
        }
    }
    let input = input.ok_or_else(|| {
        ExecError::InvalidObjectDefinition("type input function must be specified".into())
    })?;
    let output = output.ok_or_else(|| {
        ExecError::InvalidObjectDefinition("type output function must be specified".into())
    })?;
    let Some(representation) = representation else {
        return Err(ExecError::Unsupported(
            "CREATE TYPE needs LIKE = <type>: gres holds a base type's values in the \
             representation type's form, and has no opaque byte image to fall back on"
                .into(),
        ));
    };
    require_routine(kv, &input)?;
    require_routine(kv, &output)?;
    // `DefineType` starts every base type at `TYPCATEGORY_USER` and only the
    // `CATEGORY` option moves it. `LIKE` copies the layout, never the category.
    let category = category.unwrap_or_else(|| "U".to_string());
    let body = UserTypeBody::Base(BaseBody {
        representation,
        input,
        output,
        category,
        preferred,
        delimiter,
    });
    // A shell of this name is the two-phase definition closing: keep the oid,
    // because the I/O functions created in between already name it.
    let shell = crabka_pgcatalog::list_user_types(kv)?
        .into_iter()
        .find(|ty| ty.schema == name.schema && ty.name == name.name && ty.is_shell());
    let Some(shell) = shell else {
        return register(kv, name, body, "CREATE TYPE");
    };
    let completed = UserType { body, ..shell };
    Ok((
        QueryResult::Command {
            tag: "CREATE TYPE".to_string(),
        },
        crabka_pgcatalog::put_user_type_ops(kv, &completed)?,
    ))
}

/// The routine a `INPUT =` / `OUTPUT =` option names must already exist.
fn require_routine(kv: &dyn Kv, name: &str) -> Result<(), ExecError> {
    if crate::routine::is_user_routine(kv, name) {
        return Ok(());
    }
    Err(ExecError::UndefinedFunction(format!(
        "function {name} does not exist"
    )))
}

fn option_name(option: &BaseTypeOption) -> Result<String, ExecError> {
    match &option.value {
        BaseTypeOptionValue::Name(name) => Ok(name.clone()),
        BaseTypeOptionValue::Str(text) => Ok(text.clone()),
        _ => Err(malformed_option(option, "a function name")),
    }
}

fn option_type(option: &BaseTypeOption) -> Result<ColumnType, ExecError> {
    let BaseTypeOptionValue::Name(name) = &option.value else {
        return Err(malformed_option(option, "a type name"));
    };
    ColumnType::from_sql_name(name)
        .ok_or_else(|| ExecError::UndefinedObject(format!("type \"{name}\" does not exist")))
}

fn option_string(option: &BaseTypeOption) -> Result<String, ExecError> {
    match &option.value {
        BaseTypeOptionValue::Str(text) => Ok(text.clone()),
        BaseTypeOptionValue::Name(name) => Ok(name.clone()),
        _ => Err(malformed_option(option, "a string")),
    }
}

fn option_char(option: &BaseTypeOption) -> Result<String, ExecError> {
    let text = option_string(option)?;
    if text.chars().count() == 1 {
        Ok(text)
    } else {
        Err(malformed_option(option, "a single character"))
    }
}

fn option_bool(option: &BaseTypeOption) -> Result<bool, ExecError> {
    match &option.value {
        BaseTypeOptionValue::Bool(value) => Ok(*value),
        // `PASSEDBYVALUE` and friends are written bare, which reads as true.
        BaseTypeOptionValue::Omitted => Ok(true),
        _ => Err(malformed_option(option, "a boolean")),
    }
}

fn malformed_option(option: &BaseTypeOption, wanted: &str) -> ExecError {
    ExecError::InvalidObjectDefinition(format!("type attribute \"{}\" needs {wanted}", option.name))
}

/// `PostgreSQL` names an unnamed domain constraint `<domain>_check`, then
/// `<domain>_check1`, `<domain>_check2`, … for the ones after it.
fn default_check_name(domain: &str, ordinal: usize) -> String {
    if ordinal <= 1 {
        format!("{domain}_check")
    } else {
        format!("{domain}_check{}", ordinal - 1)
    }
}

fn composite_fields(fields: &[CompositeFieldDef]) -> Result<Vec<CompositeField>, ExecError> {
    let mut seen: Vec<&str> = Vec::with_capacity(fields.len());
    let mut out = Vec::with_capacity(fields.len());
    for field in fields {
        if seen.contains(&field.name.as_str()) {
            return Err(ExecError::DuplicateObject(format!(
                "column \"{}\" specified more than once",
                field.name
            )));
        }
        if matches!(field.ty, ColumnType::Record(None)) {
            return Err(ExecError::Unsupported(format!(
                "column \"{}\" has pseudo-type record",
                field.name
            )));
        }
        seen.push(&field.name);
        out.push(CompositeField {
            name: field.name.clone(),
            ty: field.ty,
        });
    }
    Ok(out)
}

fn enum_labels(name: &str, labels: &[String]) -> Result<Vec<String>, ExecError> {
    let mut seen: Vec<&str> = Vec::with_capacity(labels.len());
    for label in labels {
        if seen.contains(&label.as_str()) {
            return Err(ExecError::DuplicateObject(format!(
                "enum label \"{label}\" already exists"
            )));
        }
        check_label_length(name, label)?;
        seen.push(label);
    }
    Ok(labels.to_vec())
}

/// `PostgreSQL`'s `NAMEDATALEN - 1` limit on an enum label (22023).
fn check_label_length(type_name: &str, label: &str) -> Result<(), ExecError> {
    const NAMEDATALEN: usize = 63;
    if label.len() > NAMEDATALEN {
        return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
            "22023",
            format!("invalid enum label \"{label}\" for type {type_name}"),
        )));
    }
    Ok(())
}

/// Allocate the oid and build the catalog record. The session publishes the
/// committed definition to the parser registry only after event triggers pass.
fn register(
    kv: &dyn Kv,
    name: &RelationName,
    body: UserTypeBody,
    tag: &str,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    if matches!(&body, UserTypeBody::Composite(_)) && crabka_pgcatalog::relation_exists(kv, name)? {
        return Err(crabka_pgcatalog::CatalogError::DuplicateTable(name.to_string()).into());
    }
    ensure_type_name_available(kv, name, None)?;
    if let UserTypeBody::Range(range) = &body {
        let (schema, companion) = match (&range.multirange_schema, &range.multirange_name) {
            (Some(schema), Some(companion)) => (schema.clone(), companion.clone()),
            _ => (
                name.schema.clone(),
                usertype::default_multirange_name(&name.name),
            ),
        };
        let companion = RelationName::new(schema, companion);
        if companion == *name {
            return Err(ExecError::DuplicateObject(format!(
                "type \"{companion}\" already exists"
            )));
        }
        ensure_type_name_available(kv, &companion, None)?;
    }
    let (_, ops) = crabka_pgcatalog::create_user_type_ops(kv, name, body)?;
    Ok((
        QueryResult::Command {
            tag: tag.to_string(),
        },
        ops,
    ))
}

#[derive(Clone, Copy)]
enum TypeNameExclusion {
    Primary(u32),
    Multirange(u32),
}

/// Enforce PostgreSQL's shared type namespace: base types, multirange
/// companions, and relation row types cannot occupy the same exact identity.
fn ensure_type_name_available(
    kv: &dyn Kv,
    name: &RelationName,
    exclude: Option<TypeNameExclusion>,
) -> Result<(), ExecError> {
    let types = crabka_pgcatalog::list_user_types(kv)?;
    let primary_taken = types.iter().any(|ty| {
        ty.schema == name.schema
            && ty.name == name.name
            && !matches!(exclude, Some(TypeNameExclusion::Primary(oid)) if oid == ty.oid)
    });
    let identity = (name.schema.clone(), name.name.clone());
    let companion_taken = types.iter().any(|ty| {
        ty.multirange_identity() == Some(identity.clone())
            && !matches!(exclude, Some(TypeNameExclusion::Multirange(oid)) if oid == ty.oid)
    });
    let row_type_taken = crabka_pgcatalog::list_tables(kv)?
        .iter()
        .any(|table| table.name == *name)
        || crabka_pgcatalog::list_views(kv)?
            .iter()
            .any(|view| view.name == *name);
    let builtin_taken =
        name.schema == "pg_catalog" && crate::exec::is_builtin_catalog_type_name(&name.name);
    if primary_taken || companion_taken || row_type_taken || builtin_taken {
        return Err(ExecError::DuplicateObject(format!(
            "type \"{name}\" already exists"
        )));
    }
    Ok(())
}

/// Refuse a rowtype-producing relation whose exact schema/name identity is
/// already occupied by a user type or a range's multirange companion.
pub(crate) fn ensure_relation_type_name_available(
    kv: &dyn Kv,
    name: &RelationName,
) -> Result<(), ExecError> {
    let identity = (name.schema.clone(), name.name.clone());
    let types = crabka_pgcatalog::list_user_types(kv)?;
    if let Some(ty) = types
        .iter()
        .find(|ty| ty.schema == name.schema && ty.name == name.name)
    {
        if ty.fields().is_some() {
            return Err(crabka_pgcatalog::CatalogError::DuplicateTable(name.to_string()).into());
        }
        return Err(ExecError::DuplicateObject(format!(
            "type \"{name}\" already exists"
        )));
    }
    if types
        .iter()
        .any(|ty| ty.multirange_identity() == Some(identity.clone()))
    {
        return Err(ExecError::DuplicateObject(format!(
            "type \"{name}\" already exists"
        )));
    }
    Ok(())
}

/// An index may share a name with scalar user types, but not with a composite:
/// the composite's backing `pg_class` row already occupies that relation name.
pub(crate) fn ensure_index_name_available(
    kv: &dyn Kv,
    name: &RelationName,
) -> Result<(), ExecError> {
    if crabka_pgcatalog::list_user_types(kv)?
        .iter()
        .any(|ty| ty.schema == name.schema && ty.name == name.name && ty.fields().is_some())
    {
        return Err(crabka_pgcatalog::CatalogError::DuplicateTable(name.to_string()).into());
    }
    Ok(())
}

fn companion_identity(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    companion: &crabka_pgparser::ast::RelationRef,
) -> Result<(String, String), ExecError> {
    let companion = resolve_relation(kv, resolution, companion, SchemaDisposition::Creation)?;
    Ok((companion.schema, companion.name))
}

/// `ALTER TYPE name <action>`.
///
/// # Errors
///
/// 42704 when the type does not exist, 42809 when the action does not apply to
/// its kind, 42710 on a duplicate label, and 0A000 for the actions the engine
/// does not implement.
pub fn alter_type(
    kv: &dyn Kv,
    name: &RelationName,
    action: &AlterTypeAction,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    let lookup_name = name.to_string();
    let (mut ty, is_multirange) = require_type_or_multirange(kv, name)?;
    if is_multirange {
        return match action {
            AlterTypeAction::RenameTo(new_name) => rename_multirange(kv, ty, name, new_name),
            AlterTypeAction::OwnerTo(_) => Ok((command("ALTER TYPE"), Vec::new())),
            AlterTypeAction::AddAttribute(_) => Err(wrong_kind(name, "a composite type")),
            AlterTypeAction::AddValue { .. } | AlterTypeAction::RenameValue { .. } => {
                Err(wrong_kind(name, "an enum"))
            }
        };
    }
    match action {
        AlterTypeAction::AddAttribute(field) => {
            if column_type_contains_oid(field.ty, ty.oid, &mut HashSet::new()) {
                return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
                    "42P16",
                    format!("composite type {lookup_name} cannot be made a member of itself"),
                )));
            }
            let UserTypeBody::Composite(fields) = &mut ty.body else {
                return Err(wrong_kind(name, "a composite type"));
            };
            if fields.iter().any(|existing| existing.name == field.name) {
                return Err(ExecError::DuplicateObject(format!(
                    "column \"{}\" of relation \"{lookup_name}\" already exists",
                    field.name
                )));
            }
            fields.push(
                composite_fields(std::slice::from_ref(field))?
                    .pop()
                    .expect("one field produces one field"),
            );
        }
        AlterTypeAction::AddValue {
            label,
            if_not_exists,
            position,
        } => {
            let UserTypeBody::Enum(labels) = &mut ty.body else {
                return Err(wrong_kind(name, "an enum"));
            };
            if labels.iter().any(|existing| existing == label) {
                if *if_not_exists {
                    return Ok((command("ALTER TYPE"), Vec::new()));
                }
                return Err(ExecError::DuplicateObject(format!(
                    "enum label \"{label}\" already exists"
                )));
            }
            check_label_length(&lookup_name, label)?;
            let index = match position {
                None => labels.len(),
                Some(EnumValuePosition::Before(neighbour)) => {
                    label_position(&lookup_name, labels, neighbour)?
                }
                Some(EnumValuePosition::After(neighbour)) => {
                    label_position(&lookup_name, labels, neighbour)? + 1
                }
            };
            labels.insert(index, label.clone());
        }
        AlterTypeAction::RenameValue { from, to } => {
            let UserTypeBody::Enum(labels) = &mut ty.body else {
                return Err(wrong_kind(name, "an enum"));
            };
            if labels.iter().any(|existing| existing == to) {
                return Err(ExecError::DuplicateObject(format!(
                    "enum label \"{to}\" already exists"
                )));
            }
            let index = label_position(&lookup_name, labels, from)?;
            check_label_length(&lookup_name, to)?;
            labels[index] = to.clone();
        }
        AlterTypeAction::RenameTo(new_name) => return rename(kv, ty, new_name, "ALTER TYPE"),
        // The engine has a single type owner, so an ownership change is a
        // no-op rather than a refusal — matching how the rest of the engine
        // treats `OWNER TO`.
        AlterTypeAction::OwnerTo(_) => return Ok((command("ALTER TYPE"), Vec::new())),
    }
    Ok((
        command("ALTER TYPE"),
        crabka_pgcatalog::put_user_type_ops(kv, &ty)?,
    ))
}

fn rename_multirange(
    kv: &dyn Kv,
    mut range_type: UserType,
    old_name: &RelationName,
    new_name: &str,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    let renamed = RelationName::new(old_name.schema.clone(), new_name);
    ensure_type_name_available(
        kv,
        &renamed,
        Some(TypeNameExclusion::Multirange(range_type.oid)),
    )?;
    let UserTypeBody::Range(range) = &mut range_type.body else {
        unreachable!("a multirange companion belongs to a range type")
    };
    range.multirange_schema = Some(renamed.schema);
    range.multirange_name = Some(renamed.name);
    Ok((
        command("ALTER TYPE"),
        crabka_pgcatalog::put_user_type_ops(kv, &range_type)?,
    ))
}

fn column_type_contains_oid(ty: ColumnType, target: u32, seen: &mut HashSet<u32>) -> bool {
    if ty.oid() == target {
        return true;
    }
    match ty {
        ColumnType::Array(elem) => column_type_contains_oid(elem.column_type(), target, seen),
        ColumnType::Domain(domain) => column_type_contains_oid(*domain.base, target, seen),
        ColumnType::Range(range) => column_type_contains_oid(*range.subtype, target, seen),
        ColumnType::Multirange(multirange) => {
            column_type_contains_oid(*multirange.range.subtype, target, seen)
        }
        ColumnType::Record(Some(record)) if seen.insert(record.oid) => {
            usertype::lookup_oid(record.oid).is_some_and(|ty| {
                ty.fields().is_some_and(|fields| {
                    fields
                        .iter()
                        .any(|field| column_type_contains_oid(field.ty, target, seen))
                })
            })
        }
        _ => false,
    }
}

/// `ALTER DOMAIN name <action>`.
///
/// # Errors
///
/// 42704 when the domain does not exist, 42809 when the name is not a domain,
/// and 42710/42704 for constraint-name conflicts.
pub fn alter_domain(
    kv: &dyn Kv,
    name: &RelationName,
    action: &AlterDomainAction,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    let lookup_name = name.to_string();
    let mut ty = require_type(kv, name)?;
    if let AlterDomainAction::RenameTo(new_name) = action {
        if ty.domain().is_none() {
            return Err(wrong_kind(name, "a domain"));
        }
        return rename(kv, ty, new_name, "ALTER DOMAIN");
    }
    if matches!(action, AlterDomainAction::OwnerTo(_)) {
        return Ok((command("ALTER DOMAIN"), Vec::new()));
    }
    let UserTypeBody::Domain(domain) = &mut ty.body else {
        return Err(wrong_kind(name, "a domain"));
    };
    match action {
        AlterDomainAction::SetDefault(expr) => domain.default = Some(expr.clone()),
        AlterDomainAction::DropDefault => domain.default = None,
        AlterDomainAction::SetNotNull(not_null) => domain.not_null = *not_null,
        AlterDomainAction::AddConstraint {
            name: constraint_name,
            text,
            not_valid,
        } => {
            if *not_valid {
                return Err(ExecError::Unsupported(
                    "ALTER DOMAIN … ADD CONSTRAINT … NOT VALID is not supported: an unvalidated \
                     domain constraint would silently accept existing values that violate it"
                        .into(),
                ));
            }
            let ordinal = domain.checks.len() + 1;
            let constraint_name = constraint_name
                .clone()
                .unwrap_or_else(|| default_check_name(&name.name, ordinal));
            if domain
                .checks
                .iter()
                .any(|check| check.name == constraint_name)
            {
                return Err(ExecError::DuplicateObject(format!(
                    "constraint \"{constraint_name}\" for domain \"{lookup_name}\" already exists"
                )));
            }
            domain.checks.push(DomainCheck {
                name: constraint_name,
                expr: text.clone(),
            });
        }
        AlterDomainAction::DropConstraint {
            name: constraint_name,
            if_exists,
        } => {
            let before = domain.checks.len();
            domain.checks.retain(|check| check.name != *constraint_name);
            if domain.checks.len() == before && !*if_exists {
                return Err(ExecError::UndefinedObject(format!(
                    "constraint \"{constraint_name}\" of domain \"{lookup_name}\" does not exist"
                )));
            }
        }
        // Every constraint the engine stores has already been validated
        // against nothing (a domain has no existing rows of its own to scan),
        // so validating one is a successful no-op, as in PostgreSQL when the
        // constraint is already valid.
        AlterDomainAction::ValidateConstraint(constraint_name) => {
            if !domain
                .checks
                .iter()
                .any(|check| check.name == *constraint_name)
            {
                return Err(ExecError::UndefinedObject(format!(
                    "constraint \"{constraint_name}\" of domain \"{lookup_name}\" does not exist"
                )));
            }
        }
        AlterDomainAction::RenameConstraint { from, to } => {
            if domain.checks.iter().any(|check| check.name == *to) {
                return Err(ExecError::DuplicateObject(format!(
                    "constraint \"{to}\" for domain \"{lookup_name}\" already exists"
                )));
            }
            let Some(check) = domain.checks.iter_mut().find(|check| check.name == *from) else {
                return Err(ExecError::UndefinedObject(format!(
                    "constraint \"{from}\" of domain \"{lookup_name}\" does not exist"
                )));
            };
            check.name = to.clone();
        }
        AlterDomainAction::RenameTo(_) | AlterDomainAction::OwnerTo(_) => {
            unreachable!("handled before the domain body is borrowed")
        }
    }
    Ok((
        command("ALTER DOMAIN"),
        crabka_pgcatalog::put_user_type_ops(kv, &ty)?,
    ))
}

fn rename(
    kv: &dyn Kv,
    mut ty: UserType,
    new_name: &str,
    tag: &str,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    let renamed_identity = RelationName::new(ty.schema.clone(), new_name);
    if ty.fields().is_some() && crabka_pgcatalog::relation_exists(kv, &renamed_identity)? {
        return Err(
            crabka_pgcatalog::CatalogError::DuplicateTable(renamed_identity.to_string()).into(),
        );
    }
    ensure_type_name_available(
        kv,
        &renamed_identity,
        Some(TypeNameExclusion::Primary(ty.oid)),
    )?;
    let old = ty.clone();
    if let Some((schema, name)) = ty.multirange_identity()
        && let UserTypeBody::Range(range) = &mut ty.body
        && range.multirange_schema.is_none()
    {
        range.multirange_schema = Some(schema);
        range.multirange_name = Some(name);
    }
    ty.name = new_name.to_string();
    Ok((
        QueryResult::Command {
            tag: tag.to_string(),
        },
        crabka_pgcatalog::rename_user_type_ops(kv, &old, &ty)?,
    ))
}

/// `DROP TYPE`/`DROP DOMAIN`.
///
/// `kind` selects which of the two to drop, so `DROP DOMAIN` refuses a
/// composite (42809) exactly as `PostgreSQL` does.
///
/// # Errors
///
/// 42704 when a name does not exist and `IF EXISTS` was not given, 42809 on a
/// kind mismatch, and 2BP01 when a table column still uses the type.
pub fn drop_types(
    kv: &dyn Kv,
    names: &[RelationName],
    if_exists: bool,
    cascade: bool,
    domain_only: bool,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    let tag = if domain_only {
        "DROP DOMAIN"
    } else {
        "DROP TYPE"
    };
    let mut ops = Vec::new();
    for name in names {
        let Some(ty) = crabka_pgcatalog::get_user_type(kv, name)? else {
            if name.schema == "pg_catalog" && crate::exec::is_builtin_catalog_type_name(&name.name)
            {
                return Err(ExecError::DependentObjectsStillExist(format!(
                    "cannot drop type {name} because it is required by the database system"
                )));
            }
            if crabka_pgcatalog::list_user_types(kv)?.iter().any(|ty| {
                ty.multirange_identity() == Some((name.schema.clone(), name.name.clone()))
            }) {
                return Err(ExecError::DependentObjectsStillExist(format!(
                    "cannot drop type {name} because its range type requires it"
                )));
            }
            if if_exists {
                continue;
            }
            return Err(ExecError::UndefinedObject(format!(
                "type \"{name}\" does not exist"
            )));
        };
        if domain_only && ty.domain().is_none() {
            return Err(wrong_kind(name, "a domain"));
        }
        if !domain_only && ty.domain().is_some() {
            return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
                "42809",
                format!("\"{name}\" is a domain\nHINT:  Use DROP DOMAIN to remove a domain."),
            )));
        }
        let dependents = dependent_user_types(kv, ty.oid)?;
        if !cascade && let Some(dependent) = dependents.first() {
            return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
                "2BP01",
                format!(
                    "cannot drop type {name} because other objects depend on it\nDETAIL:  \
                     type {} depends on type {name}",
                    dependent.qualified_name()
                ),
            )));
        }
        // A routine that names the type in its signature, and a cast that names
        // it at either end, both depend on it. Leaving either behind would be
        // worse than refusing the drop: a routine whose argument type no longer
        // resolves, or a `pg_cast` row pointing at a vanished oid.
        let routines = dependent_routines(kv, &ty)?;
        let casts = dependent_casts(kv, ty.oid)?;
        if !cascade && let Some(routine) = routines.first() {
            return Err(dependency_refusal(
                name,
                &format!("function {} depends on type {name}", routine.identity()),
            ));
        }
        if !cascade && let Some(cast) = casts.first() {
            return Err(dependency_refusal(
                name,
                &format!("{} depends on type {name}", cast_dependency_line(cast)),
            ));
        }
        if !cascade && let Some(user) = column_using_type(kv, &ty)? {
            return Err(dependency_refusal(
                name,
                &format!("column {user} depends on type {name}"),
            ));
        }
        for dependent in dependents.into_iter().rev() {
            ops.extend(crabka_pgcatalog::drop_user_type_ops(kv, &dependent)?);
        }
        for routine in &routines {
            ops.extend(crabka_pgcatalog::routine::drop_routine_ops(
                &routine.identity(),
            ));
        }
        for cast in &casts {
            ops.extend(crabka_pgcatalog::drop_user_cast_ops(
                cast.source,
                cast.target,
            ));
        }
        ops.extend(crabka_pgcatalog::drop_user_type_ops(kv, &ty)?);
    }
    Ok((
        QueryResult::Command {
            tag: tag.to_string(),
        },
        ops,
    ))
}

/// The `drop cascades to …` lines a `DROP TYPE … CASCADE` of `reference`
/// reports: its dependent types, then the routines that name it, then the
/// casts that have it at either end.
///
/// Computed before the drop runs, while the dependents are still there.
///
/// # Errors
///
/// Propagates catalog read errors. A name that resolves to no type reports
/// nothing — the drop itself raises the 42704.
pub(crate) fn type_cascade_lines(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    reference: &crabka_pgparser::ast::RelationRef,
) -> Result<Vec<String>, ExecError> {
    let Ok(name) = resolve_relation(kv, resolution, reference, SchemaDisposition::Reference) else {
        return Ok(Vec::new());
    };
    let Some(ty) = crabka_pgcatalog::get_user_type(kv, &name)? else {
        return Ok(Vec::new());
    };
    let mut lines: Vec<String> = dependent_user_types(kv, ty.oid)?
        .iter()
        .map(|dependent| format!("drop cascades to type {}", dependent.qualified_name()))
        .collect();
    lines.extend(
        dependent_routines(kv, &ty)?
            .iter()
            .map(|routine| format!("drop cascades to function {}", routine.identity())),
    );
    lines.extend(
        dependent_casts(kv, ty.oid)?
            .iter()
            .map(|cast| format!("drop cascades to {}", cast_dependency_line(cast))),
    );
    Ok(lines)
}

/// PostgreSQL's 2BP01 for a drop that would orphan something, with the one
/// dependency it names first.
fn dependency_refusal(name: &RelationName, detail: &str) -> ExecError {
    ExecError::Remote(crabka_pgwire::error::PgError::error(
        "2BP01",
        format!("cannot drop type {name} because other objects depend on it\nDETAIL:  {detail}"),
    ))
}

/// Every routine whose signature names `ty`, in oid order — which is creation
/// order, and so the order PostgreSQL reports the cascade in.
///
/// A routine names a type by *name*, not by oid, because a shell type has no
/// [`ColumnType`] to compare and its I/O pair is exactly the dependency that
/// matters here.
pub(crate) fn dependent_routines(
    kv: &dyn Kv,
    ty: &UserType,
) -> Result<Vec<crabka_pgcatalog::routine::Routine>, ExecError> {
    let wanted = ty.qualified_name();
    let mut found: Vec<crabka_pgcatalog::routine::Routine> =
        crabka_pgcatalog::routine::list_routines(kv)?
            .into_iter()
            .filter(|routine| routine_names_type(routine, &wanted))
            .collect();
    found.sort_by_key(|routine| routine.oid);
    Ok(found)
}

fn routine_names_type(routine: &crabka_pgcatalog::routine::Routine, wanted: &str) -> bool {
    let names = |ty: &crabka_pgcatalog::routine::RoutineType| ty.name == wanted;
    routine.params.iter().any(|param| names(&param.ty))
        || match &routine.result {
            crabka_pgcatalog::routine::RoutineResult::Type { ty, .. } => names(ty),
            crabka_pgcatalog::routine::RoutineResult::Table(columns) => {
                columns.iter().any(|(_, ty)| names(ty))
            }
            crabka_pgcatalog::routine::RoutineResult::Unspecified => false,
        }
}

/// Every declared cast with `oid` at either end, in the order they were
/// recorded — which the `(source, target)` key gives for free only by pair, so
/// the catalog scan order is what PostgreSQL's oid order approximates.
fn dependent_casts(kv: &dyn Kv, oid: u32) -> Result<Vec<crabka_pgcatalog::UserCast>, ExecError> {
    Ok(crabka_pgcatalog::list_user_casts(kv)?
        .into_iter()
        .filter(|cast| cast.source == oid || cast.target == oid)
        .collect())
}

/// `cast from xfloat4 to real`, the way `getObjectDescription` spells a cast.
pub(crate) fn cast_dependency_line(cast: &crabka_pgcatalog::UserCast) -> String {
    format!(
        "cast from {} to {}",
        type_name_for_oid(cast.source),
        type_name_for_oid(cast.target)
    )
}

/// The name `format_type` gives an oid: `real` for `float4`, and the type's own
/// name for a user type.
fn type_name_for_oid(oid: u32) -> String {
    if let Some(ty) = usertype::column_type_for_oid(oid) {
        return ty.name().to_string();
    }
    crate::exec::builtin_type_name(oid)
        .and_then(ColumnType::from_sql_name)
        .map_or_else(|| format!("type {oid}"), |ty| ty.name().to_string())
}

fn dependent_user_types(kv: &dyn Kv, root_oid: u32) -> Result<Vec<UserType>, ExecError> {
    let types = crabka_pgcatalog::list_user_types(kv)?;
    let mut dropped = HashSet::from([root_oid]);
    let mut dependents = Vec::new();
    while let Some(dependent) = types
        .iter()
        .find(|ty| !dropped.contains(&ty.oid) && user_type_references_any(ty, &dropped))
    {
        dropped.insert(dependent.oid);
        dependents.push(dependent.clone());
    }
    Ok(dependents)
}

fn user_type_references_any(ty: &UserType, oids: &HashSet<u32>) -> bool {
    match &ty.body {
        UserTypeBody::Composite(fields) => fields
            .iter()
            .any(|field| column_type_references_any(field.ty, oids)),
        UserTypeBody::Range(range) => column_type_references_any(range.subtype, oids),
        UserTypeBody::Domain(domain) => column_type_references_any(domain.base, oids),
        UserTypeBody::Base(base) => column_type_references_any(base.representation, oids),
        // A shell names nothing, which is the point of it.
        UserTypeBody::Enum(_) | UserTypeBody::Shell => false,
    }
}

fn column_type_references_any(ty: ColumnType, oids: &HashSet<u32>) -> bool {
    if oids.contains(&ty.oid()) {
        return true;
    }
    match ty {
        ColumnType::Array(element) => column_type_references_any(element.column_type(), oids),
        ColumnType::Domain(domain) => column_type_references_any(*domain.base, oids),
        ColumnType::Range(range) => column_type_references_any(*range.subtype, oids),
        ColumnType::Multirange(multirange) => {
            oids.contains(&multirange.range.oid)
                || column_type_references_any(*multirange.range.subtype, oids)
        }
        _ => false,
    }
}

/// Drop every user type owned by `schema`, including a range whose multirange
/// companion lives there, and every user type that depends on one of them.
///
/// The returned catalog writes are ordered dependents first. The caller
/// publishes their committed catalog delta at its transaction boundary.
///
/// # Errors
///
/// Returns catalog storage/corruption errors, or an invalid-state error for a
/// cyclic user-type dependency graph.
pub(crate) fn drop_schema_types_ops(kv: &dyn Kv, schema: &str) -> Result<Vec<WriteOp>, ExecError> {
    let types = crabka_pgcatalog::list_user_types(kv)?;
    let mut dropping: HashSet<u32> = types
        .iter()
        .filter(|ty| {
            ty.schema == schema
                || ty
                    .multirange_identity()
                    .is_some_and(|(companion_schema, _)| companion_schema == schema)
        })
        .map(|ty| ty.oid)
        .collect();
    while let Some(dependent) = types
        .iter()
        .find(|ty| !dropping.contains(&ty.oid) && user_type_references_any(ty, &dropping))
    {
        dropping.insert(dependent.oid);
    }

    let mut remaining: Vec<UserType> = types
        .into_iter()
        .filter(|ty| dropping.contains(&ty.oid))
        .collect();
    let mut ordered = Vec::with_capacity(remaining.len());
    while !remaining.is_empty() {
        let Some(index) = remaining.iter().position(|candidate| {
            let candidate_oid = HashSet::from([candidate.oid]);
            !remaining.iter().any(|other| {
                other.oid != candidate.oid && user_type_references_any(other, &candidate_oid)
            })
        }) else {
            return Err(ExecError::ObjectNotInPrerequisiteState(
                "cyclic user-type dependency graph".into(),
            ));
        };
        ordered.push(remaining.remove(index));
    }

    let mut ops = Vec::new();
    for ty in &ordered {
        ops.extend(crabka_pgcatalog::drop_user_type_ops(kv, ty)?);
    }
    Ok(ops)
}

/// `table.column` of the first table column declared with `ty`, if any.
fn column_using_type(kv: &dyn Kv, ty: &UserType) -> Result<Option<String>, ExecError> {
    for table in crabka_pgcatalog::list_tables(kv)? {
        for column in &table.columns {
            if column.ty.oid() == ty.oid {
                return Ok(Some(format!("{}.{}", table.name, column.name)));
            }
        }
    }
    Ok(None)
}

fn label_position(type_name: &str, labels: &[String], label: &str) -> Result<usize, ExecError> {
    labels
        .iter()
        .position(|existing| existing == label)
        .ok_or_else(|| {
            ExecError::Remote(crabka_pgwire::error::PgError::error(
                "22P02",
                format!("\"{label}\" is not an existing enum label of type {type_name}"),
            ))
        })
}

fn require_type(kv: &dyn Kv, name: &RelationName) -> Result<UserType, ExecError> {
    crabka_pgcatalog::get_user_type(kv, name)?
        .ok_or_else(|| ExecError::UndefinedObject(format!("type \"{name}\" does not exist")))
}

fn require_type_or_multirange(
    kv: &dyn Kv,
    name: &RelationName,
) -> Result<(UserType, bool), ExecError> {
    if let Some(ty) = crabka_pgcatalog::get_user_type(kv, name)? {
        return Ok((ty, false));
    }
    if name.schema == "pg_catalog" && crate::exec::is_builtin_catalog_type_name(&name.name) {
        return Err(ExecError::Unsupported(format!(
            "cannot alter system type {name}"
        )));
    }
    let identity = (name.schema.clone(), name.name.clone());
    crabka_pgcatalog::list_user_types(kv)?
        .into_iter()
        .find(|ty| ty.multirange_identity() == Some(identity.clone()))
        .map(|ty| (ty, true))
        .ok_or_else(|| ExecError::UndefinedObject(format!("type \"{name}\" does not exist")))
}

fn wrong_kind(name: &RelationName, wanted: &str) -> ExecError {
    ExecError::Remote(crabka_pgwire::error::PgError::error(
        "42809",
        format!("\"{name}\" is not {wanted}"),
    ))
}

fn command(tag: &str) -> QueryResult {
    QueryResult::Command {
        tag: tag.to_string(),
    }
}

// ── Value-level operations ────────────────────────────────────────────────────

/// Enforce a domain's `NOT NULL` and `CHECK` constraints on `value`.
///
/// A no-op for every type that is not a domain, so callers can hand it
/// whatever target type they have. `CHECK` follows `PostgreSQL`'s three-valued
/// rule: only an explicit `false` violates it, and a NULL result passes.
///
/// # Errors
///
/// 23502 when a `NOT NULL` domain is given NULL, and 23514 when a `CHECK` is
/// false.
/// Does the domain with this oid carry any `CHECK` constraint?
///
/// `PostgreSQL` routes SQL/JSON output coercion through the JSON populate path
/// rather than the type's input function precisely for constrained domains, so
/// this distinction is observable.
#[must_use]
pub fn domain_has_checks(oid: u32) -> bool {
    usertype::lookup_oid(oid)
        .and_then(|registered| registered.domain().map(|d| !d.checks.is_empty()))
        .unwrap_or(false)
}

pub fn check_domain(
    ty: ColumnType,
    value: &Datum,
    ctx: &crate::clock::EvalCtx,
) -> Result<(), ExecError> {
    let ColumnType::Domain(domain_ref) = ty else {
        return Ok(());
    };
    let Some(registered) = usertype::lookup_oid(domain_ref.oid) else {
        return Err(ExecError::UndefinedObject(format!(
            "type \"{}\" does not exist",
            domain_ref.name
        )));
    };
    let Some(domain) = registered.domain() else {
        return Ok(());
    };
    if value.is_null() && domain.not_null {
        return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
            "23502",
            format!("domain {} does not allow null values", registered.name),
        )));
    }
    // A NULL never violates a CHECK: PostgreSQL evaluates the predicate and
    // a NULL result passes, and `VALUE IS NULL` predicates are the reason
    // the evaluation still happens below for non-NULL values only when the
    // domain has checks.
    if domain.checks.is_empty() {
        return Ok(());
    }
    // The predicate names the tested value `VALUE`, which the lexer folds to
    // the ordinary identifier `value`.
    let scope = value_scope(*domain_ref.base);
    let row = [value.clone()];
    for check in &domain.checks {
        let expr = crabka_pgparser::parser::parse_expression(&check.expr)?;
        let result = crate::eval::eval(&expr, &scope, &row, ctx)?;
        if matches!(result, Datum::Bool(false)) {
            return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
                "23514",
                format!(
                    "value for domain {} violates check constraint \"{}\"",
                    registered.name, check.name
                ),
            )));
        }
    }
    Ok(())
}

/// The one-column scope a domain `CHECK (VALUE …)` predicate resolves against.
fn value_scope(base: ColumnType) -> crate::scope::Scope {
    crate::scope::Scope {
        columns: vec![crate::scope::ColumnBinding {
            exposure: crate::scope::Exposure::Output,
            qualifier: None,
            name: "value".to_string(),
            ty: base,
        }],
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgkv::MemKv;
    use crabka_pgparser::ast::RelationRef;

    use super::*;

    #[test]
    fn unnamed_domain_checks_get_postgres_default_names() {
        assert!(default_check_name("d", 1) == "d_check");
        assert!(default_check_name("d", 2) == "d_check1");
        assert!(default_check_name("d", 3) == "d_check2");
    }

    #[test]
    fn a_composite_refuses_duplicate_and_pseudo_type_fields() {
        let dup = [
            CompositeFieldDef {
                name: "x".into(),
                ty: ColumnType::Int4,
                collation: None,
            },
            CompositeFieldDef {
                name: "x".into(),
                ty: ColumnType::Int4,
                collation: None,
            },
        ];
        assert!(composite_fields(&dup).is_err());
        let pseudo = [CompositeFieldDef {
            name: "x".into(),
            ty: ColumnType::Record(None),
            collation: None,
        }];
        assert!(composite_fields(&pseudo).is_err());
        let ok = [CompositeFieldDef {
            name: "x".into(),
            ty: ColumnType::Numeric(None),
            collation: None,
        }];
        assert!(composite_fields(&ok).expect("valid").len() == 1);
    }

    #[test]
    fn duplicate_enum_labels_are_rejected_and_long_ones_are_22023() {
        assert!(enum_labels("e", &["a".into(), "a".into()]).is_err());
        assert!(
            enum_labels("e", &["a".into(), "b".into()])
                .expect("valid")
                .len()
                == 2
        );
        let long = "x".repeat(64);
        let err = check_label_length("e", &long).expect_err("too long");
        assert!(err.into_pg().code == "22023");
    }

    #[test]
    fn range_companion_names_are_preserved_and_collisions_rejected() {
        let kv = MemKv::default();
        let (_, ops) = create_type(
            &kv,
            crate::relname::ResolutionScope::default_scope(),
            &RelationName::public("named_range_test"),
            &CreateTypeDefinition::Range {
                subtype: ColumnType::Text,
                collation: None,
                multirange_type_name: Some(RelationRef::bare("named_multirange_test")),
            },
        )
        .expect("explicit companion");
        kv.write_batch(&ops).expect("store explicit companion");
        let stored =
            crabka_pgcatalog::get_user_type(&kv, &RelationName::public("named_range_test"))
                .expect("read explicit companion")
                .expect("stored range");
        assert!(
            stored.multirange_identity() == Some(("public".into(), "named_multirange_test".into()))
        );

        let error = create_type(
            &kv,
            crate::relname::ResolutionScope::default_scope(),
            &RelationName::public("invalid_companion_range_test"),
            &CreateTypeDefinition::Range {
                subtype: ColumnType::Text,
                collation: None,
                multirange_type_name: Some(RelationRef::bare("named_multirange_test")),
            },
        )
        .expect_err("existing companion collision");
        assert!(error.into_pg().code == "42710");

        let error = create_type(
            &kv,
            crate::relname::ResolutionScope::default_scope(),
            &RelationName::public("system_companion_range_test"),
            &CreateTypeDefinition::Range {
                subtype: ColumnType::Text,
                collation: None,
                multirange_type_name: Some(RelationRef {
                    schema: Some("pg_catalog".into()),
                    name: "int4".into(),
                }),
            },
        )
        .expect_err("exact pg_catalog type collision");
        assert!(error.into_pg().code == "42710");
    }

    #[test]
    fn drop_composite_cascade_removes_dependent_range() {
        let kv = MemKv::default();
        let (composite, composite_ops) = crabka_pgcatalog::create_user_type_ops(
            &kv,
            &RelationName::public("cascade_composite_test"),
            UserTypeBody::Composite(vec![CompositeField {
                name: "value".into(),
                ty: ColumnType::Int4,
            }]),
        )
        .expect("composite");
        kv.write_batch(&composite_ops).expect("store composite");
        hydrate(&kv).expect("publish composite");
        let (_range, range_ops) = crabka_pgcatalog::create_user_type_ops(
            &kv,
            &RelationName::public("cascade_composite_range_test"),
            UserTypeBody::Range(RangeBody {
                subtype: composite
                    .column_type()
                    .expect("a composite always has a column type"),
                collation: None,
                multirange_schema: None,
                multirange_name: None,
            }),
        )
        .expect("range");
        kv.write_batch(&range_ops).expect("store range");
        hydrate(&kv).expect("publish range");

        let composite_name = RelationName::new(&composite.schema, &composite.name);
        let before = crabka_pgcatalog::list_user_types(&kv).expect("types before drop");
        let (_, drop_ops) = drop_types(
            &kv,
            std::slice::from_ref(&composite_name),
            false,
            true,
            false,
        )
        .expect("cascade");
        kv.write_batch(&drop_ops).expect("drop types");
        let after = crabka_pgcatalog::list_user_types(&kv).expect("types after drop");
        usertype::publish_catalog_delta(&before, &after);

        assert!(
            crabka_pgcatalog::list_user_types(&kv)
                .expect("types")
                .is_empty()
        );
    }

    #[test]
    fn composite_attribute_rejects_indirect_self_inclusion() {
        let kv = MemKv::default();
        let (composite, composite_ops) = crabka_pgcatalog::create_user_type_ops(
            &kv,
            &RelationName::public("recursive_composite_test"),
            UserTypeBody::Composite(vec![]),
        )
        .expect("composite");
        kv.write_batch(&composite_ops).expect("store composite");
        let (range, range_ops) = crabka_pgcatalog::create_user_type_ops(
            &kv,
            &RelationName::public("recursive_composite_range_test"),
            UserTypeBody::Range(RangeBody {
                subtype: composite
                    .column_type()
                    .expect("a composite always has a column type"),
                collation: None,
                multirange_schema: None,
                multirange_name: None,
            }),
        )
        .expect("range");
        kv.write_batch(&range_ops).expect("store range");

        let error = alter_type(
            &kv,
            &RelationName::new(&composite.schema, &composite.name),
            &AlterTypeAction::AddAttribute(CompositeFieldDef {
                name: "recursive".into(),
                ty: range
                    .column_type()
                    .expect("a range always has a column type"),
                collation: None,
            }),
        )
        .expect_err("recursive member");
        assert!(error.into_pg().code == "42P16");
    }
}
