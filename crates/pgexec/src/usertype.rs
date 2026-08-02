//! User-defined types: `CREATE`/`ALTER`/`DROP` of `TYPE` and `DOMAIN`, and the
//! value-level operations over the types they create.
//!
//! The durable definition lives in the catalog (`crabka_pgcatalog`); the
//! process-wide registry in `crabka_pgtypes::usertype` is what makes a type
//! *name* resolvable from the parser, which holds no catalog handle. Every DDL
//! path here writes both, and [`hydrate`] restores the registry from the catalog
//! when a session opens so a restart or a second node still resolves the names.

use crabka_pgkv::{Kv, WriteOp};
use crabka_pgparser::ast::{
    AlterDomainAction, AlterTypeAction, CompositeFieldDef, CreateTypeDefinition, DomainConstraint,
    EnumValuePosition,
};
use crabka_pgtypes::{
    ColumnType, Datum,
    usertype::{self, CompositeField, DomainBody, DomainCheck, RangeBody, UserType, UserTypeBody},
};
use crabka_pgwire::engine::QueryResult;

use crate::error::ExecError;

/// Load every catalog-stored type into the process registry.
///
/// Idempotent: re-registering a type keeps its oid, so calling this on every
/// session open costs a catalog scan and changes nothing else.
///
/// # Errors
///
/// Propagates catalog read errors.
pub fn hydrate(kv: &dyn Kv) -> Result<(), ExecError> {
    for ty in crabka_pgcatalog::list_user_types(kv)? {
        usertype::replace(&ty);
    }
    Ok(())
}

/// `CREATE TYPE name AS { (…) | ENUM (…) | RANGE (…) }`.
///
/// # Errors
///
/// 42710 when the name is taken, 0A000 for the shell and range forms, and
/// 42P16/42701 for a malformed composite.
pub fn create_type(
    kv: &dyn Kv,
    name: &str,
    definition: &CreateTypeDefinition,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    let body = match definition {
        CreateTypeDefinition::Composite(fields) => {
            UserTypeBody::Composite(composite_fields(fields)?)
        }
        CreateTypeDefinition::Enum(labels) => UserTypeBody::Enum(enum_labels(name, labels)?),
        CreateTypeDefinition::Range { subtype, collation } => UserTypeBody::Range(RangeBody {
            subtype: *subtype,
            collation: collation.clone(),
        }),
        CreateTypeDefinition::Shell => {
            return Err(ExecError::Unsupported(
                "CREATE TYPE without a definition (a shell type) is not supported: shell types \
                 exist only to be completed by a C-language base type"
                    .into(),
            ));
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
    name: &str,
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
                    .unwrap_or_else(|| default_check_name(name, unnamed));
                body.checks.push(DomainCheck {
                    name: check_name,
                    expr: text.clone(),
                });
            }
        }
    }
    register(kv, name, UserTypeBody::Domain(body), "CREATE DOMAIN")
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

/// Allocate the oid, write the catalog record, and register the type so the
/// parser resolves its name from the next statement onward.
fn register(
    kv: &dyn Kv,
    name: &str,
    body: UserTypeBody,
    tag: &str,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    // A type and a relation share one namespace in `PostgreSQL`, so a name a
    // relation already holds is 42P07 rather than a duplicate type. A user type
    // carries no schema of its own here, so `public` is the only namespace it
    // can collide in.
    if crabka_pgcatalog::get_table(kv, &crabka_pgcatalog::RelationName::public(name)).is_ok() {
        return Err(ExecError::DuplicateObject(format!(
            "relation \"{name}\" already exists"
        )));
    }
    let (ty, ops) = crabka_pgcatalog::create_user_type_ops(kv, name, body)?;
    usertype::replace(&ty);
    Ok((
        QueryResult::Command {
            tag: tag.to_string(),
        },
        ops,
    ))
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
    name: &str,
    action: &AlterTypeAction,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    let mut ty = require_type(kv, name)?;
    match action {
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
            check_label_length(name, label)?;
            let index = match position {
                None => labels.len(),
                Some(EnumValuePosition::Before(neighbour)) => {
                    label_position(name, labels, neighbour)?
                }
                Some(EnumValuePosition::After(neighbour)) => {
                    label_position(name, labels, neighbour)? + 1
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
            let index = label_position(name, labels, from)?;
            check_label_length(name, to)?;
            labels[index] = to.clone();
        }
        AlterTypeAction::RenameTo(new_name) => return rename(kv, ty, new_name, "ALTER TYPE"),
        // The engine has a single type owner, so an ownership change is a
        // no-op rather than a refusal — matching how the rest of the engine
        // treats `OWNER TO`.
        AlterTypeAction::OwnerTo(_) => return Ok((command("ALTER TYPE"), Vec::new())),
    }
    usertype::replace(&ty);
    Ok((
        command("ALTER TYPE"),
        vec![crabka_pgcatalog::put_user_type_op(&ty)],
    ))
}

/// `ALTER DOMAIN name <action>`.
///
/// # Errors
///
/// 42704 when the domain does not exist, 42809 when the name is not a domain,
/// and 42710/42704 for constraint-name conflicts.
pub fn alter_domain(
    kv: &dyn Kv,
    name: &str,
    action: &AlterDomainAction,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
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
                .unwrap_or_else(|| default_check_name(name, ordinal));
            if domain
                .checks
                .iter()
                .any(|check| check.name == constraint_name)
            {
                return Err(ExecError::DuplicateObject(format!(
                    "constraint \"{constraint_name}\" for domain \"{name}\" already exists"
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
                    "constraint \"{constraint_name}\" of domain \"{name}\" does not exist"
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
                    "constraint \"{constraint_name}\" of domain \"{name}\" does not exist"
                )));
            }
        }
        AlterDomainAction::RenameConstraint { from, to } => {
            if domain.checks.iter().any(|check| check.name == *to) {
                return Err(ExecError::DuplicateObject(format!(
                    "constraint \"{to}\" for domain \"{name}\" already exists"
                )));
            }
            let Some(check) = domain.checks.iter_mut().find(|check| check.name == *from) else {
                return Err(ExecError::UndefinedObject(format!(
                    "constraint \"{from}\" of domain \"{name}\" does not exist"
                )));
            };
            check.name = to.clone();
        }
        AlterDomainAction::RenameTo(_) | AlterDomainAction::OwnerTo(_) => {
            unreachable!("handled before the domain body is borrowed")
        }
    }
    usertype::replace(&ty);
    Ok((
        command("ALTER DOMAIN"),
        vec![crabka_pgcatalog::put_user_type_op(&ty)],
    ))
}

fn rename(
    kv: &dyn Kv,
    mut ty: UserType,
    new_name: &str,
    tag: &str,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    if crabka_pgcatalog::get_user_type(kv, new_name)?.is_some() {
        return Err(ExecError::DuplicateObject(format!(
            "type \"{new_name}\" already exists"
        )));
    }
    let old_name = std::mem::replace(&mut ty.name, new_name.to_string());
    usertype::unregister(&old_name);
    usertype::replace(&ty);
    Ok((
        QueryResult::Command {
            tag: tag.to_string(),
        },
        crabka_pgcatalog::rename_user_type_ops(&old_name, &ty),
    ))
}

/// `DROP TYPE`/`DROP DOMAIN`. `kind` selects which of the two is being dropped,
/// so `DROP DOMAIN` refuses a composite (42809) exactly as `PostgreSQL` does.
///
/// # Errors
///
/// 42704 when a name does not exist and `IF EXISTS` was not given, 42809 on a
/// kind mismatch, and 2BP01 when a table column still uses the type.
pub fn drop_types(
    kv: &dyn Kv,
    names: &[String],
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
        if !cascade && let Some(user) = column_using_type(kv, &ty)? {
            return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
                "2BP01",
                format!(
                    "cannot drop type {name} because other objects depend on it\nDETAIL:  \
                     column {user} depends on type {name}"
                ),
            )));
        }
        usertype::unregister(name);
        ops.extend(crabka_pgcatalog::drop_user_type_ops(name));
    }
    Ok((
        QueryResult::Command {
            tag: tag.to_string(),
        },
        ops,
    ))
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

fn require_type(kv: &dyn Kv, name: &str) -> Result<UserType, ExecError> {
    crabka_pgcatalog::get_user_type(kv, name)?
        .ok_or_else(|| ExecError::UndefinedObject(format!("type \"{name}\" does not exist")))
}

fn wrong_kind(name: &str, wanted: &str) -> ExecError {
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
/// A no-op for every type that is not a domain, so callers can hand it whatever
/// target type they have. `CHECK` follows `PostgreSQL`'s three-valued rule: only
/// an explicit `false` violates, a NULL result passes.
///
/// # Errors
///
/// 23502 when a `NOT NULL` domain is given NULL, and 23514 when a `CHECK` is
/// false.
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
            qualifier: None,
            name: "value".to_string(),
            ty: base,
        }],
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

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
}
