//! The `pg_*_is_visible` family: would an unqualified reference to this
//! object's name resolve to *this* object?
//!
//! It is not "does the object exist", and it is not "is the object's schema on
//! the `search_path`" either. `PostgreSQL` answers it in `namespace.c` by
//! walking the effective path in order and stopping at the first schema that
//! holds something of the same kind under the same name:
//!
//! ```text
//! for schema in current_schemas(true):
//!     if schema is the object's schema:      visible
//!     if schema already holds that name:     shadowed, not visible
//! not on the path at all:                    not visible
//! ```
//!
//! Two consequences drive everything here. Visibility depends on *other*
//! objects, so creating an unrelated relation in an earlier schema hides a
//! later one; and it is per-catalog, so a type named `x` shadows a later type
//! named `x` and has nothing to say about a relation named `x`.
//!
//! What counts as "the same name" also varies by catalog. A relation, a type,
//! a conversion, a statistics object and a text-search object each occupy their
//! bare name; a routine occupies its name *and* argument types; an operator its
//! name and operand types; an operator class or family its name and index
//! access method. Verified against `postgres:18.4`:
//!
//! ```text
//! CREATE SCHEMA s1; CREATE SCHEMA s2;
//! CREATE FUNCTION s1.f(int)  …;  CREATE FUNCTION s2.f(int) …;
//! CREATE FUNCTION s2.f(text) …;
//! SET search_path = s1, s2;
//! pg_function_is_visible(s1.f(int))  -> true
//! pg_function_is_visible(s2.f(int))  -> false   -- shadowed by s1.f(int)
//! pg_function_is_visible(s2.f(text)) -> true    -- different arguments
//! ```
//!
//! An oid no object carries answers NULL rather than false — the family is
//! strict on its argument and reports "no such object" the same way.

use crabka_pgcatalog::RelationName;
use crabka_pgkv::Kv;
use crabka_pgtypes::Datum;

use crate::{clock::EvalCtx, error::ExecError};

/// The catalog a `pg_*_is_visible` call interrogates. Each one shadows only
/// within itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Catalog {
    /// `pg_table_is_visible` — `pg_class`, every relation kind.
    Relation,
    /// `pg_type_is_visible` — `pg_type`.
    Type,
    /// `pg_function_is_visible` — `pg_proc`, shadowed per signature.
    Function,
    /// `pg_operator_is_visible` — `pg_operator`, shadowed per operand pair.
    Operator,
    /// `pg_opclass_is_visible` — `pg_opclass`, shadowed per access method.
    OperatorClass,
    /// `pg_opfamily_is_visible` — `pg_opfamily`, shadowed per access method.
    OperatorFamily,
    /// `pg_collation_is_visible` — `pg_collation`.
    Collation,
    /// `pg_conversion_is_visible` — `pg_conversion`.
    Conversion,
    /// `pg_statistics_obj_is_visible` — `pg_statistic_ext`.
    StatisticsObject,
    /// `pg_ts_config_is_visible` — `pg_ts_config`.
    TsConfig,
    /// `pg_ts_dict_is_visible` — `pg_ts_dict`.
    TsDictionary,
    /// `pg_ts_parser_is_visible` — `pg_ts_parser`.
    TsParser,
    /// `pg_ts_template_is_visible` — `pg_ts_template`.
    TsTemplate,
}

impl Catalog {
    /// The catalog a function name interrogates, or `None` when the name is not
    /// one of the family. The list is `PostgreSQL` 18.4's, checked against the
    /// oracle rather than remembered:
    ///
    /// ```sql
    /// SELECT proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    ///  WHERE n.nspname = 'pg_catalog' AND proname LIKE '%is\_visible';
    /// ```
    pub(crate) fn for_function(name: &str) -> Option<Self> {
        Some(match name {
            "pg_table_is_visible" => Self::Relation,
            "pg_type_is_visible" => Self::Type,
            "pg_function_is_visible" => Self::Function,
            "pg_operator_is_visible" => Self::Operator,
            "pg_opclass_is_visible" => Self::OperatorClass,
            "pg_opfamily_is_visible" => Self::OperatorFamily,
            "pg_collation_is_visible" => Self::Collation,
            "pg_conversion_is_visible" => Self::Conversion,
            "pg_statistics_obj_is_visible" => Self::StatisticsObject,
            "pg_ts_config_is_visible" => Self::TsConfig,
            "pg_ts_dict_is_visible" => Self::TsDictionary,
            "pg_ts_parser_is_visible" => Self::TsParser,
            "pg_ts_template_is_visible" => Self::TsTemplate,
            _ => return None,
        })
    }

    /// Whether the path walk skips temporary namespaces.
    ///
    /// `PostgreSQL` never looks for a statistics object or a text-search object
    /// in a temporary namespace — `StatisticsObjIsVisibleExt` and the four
    /// `TS*IsVisibleExt` carry an explicit `continue` for it, because neither
    /// object kind can be created temporary. Every other member searches the
    /// temporary namespace first, exactly as name resolution does.
    fn skips_temp_schemas(self) -> bool {
        matches!(
            self,
            Self::StatisticsObject
                | Self::TsConfig
                | Self::TsDictionary
                | Self::TsParser
                | Self::TsTemplate
        )
    }
}

/// What an object has to share with another for one to hide the other.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ShadowKey {
    /// A bare name, unique within its schema: a relation, a type, a
    /// conversion, a collation, a statistics object, a text-search object.
    Name(String),
    /// A routine name together with its input type oids: an overload on
    /// different arguments does not hide anything.
    Signature { name: String, args: Vec<i32> },
    /// An operator name together with its operand type oids. A prefix operator
    /// carries 0 for the absent left operand, as `pg_operator.oprleft` does.
    Operands { name: String, left: i32, right: i32 },
    /// A name that only clashes within one index access method: an operator
    /// class or an operator family.
    Method { name: String, method: i32 },
}

impl ShadowKey {
    /// The bare name, for the catalogs whose probe only needs one.
    fn name(&self) -> &str {
        match self {
            Self::Name(name)
            | Self::Signature { name, .. }
            | Self::Operands { name, .. }
            | Self::Method { name, .. } => name,
        }
    }
}

/// Where an object lives and what would hide it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Located {
    schema: String,
    key: ShadowKey,
}

/// Evaluate one member of the family.
///
/// A NULL oid answers NULL because every one of these functions is strict; an
/// oid no object of that catalog carries answers NULL too, which is what
/// `PostgreSQL` reports through the `is_missing` path its `*IsVisibleExt`
/// helpers take.
///
/// # Errors
///
/// Propagates storage/corruption errors from the catalog KV seam.
pub(crate) fn is_visible(catalog: Catalog, oid: &Datum, ctx: &EvalCtx) -> Result<Datum, ExecError> {
    let Some(oid) = oid_arg(oid)? else {
        return Ok(Datum::Null);
    };
    let Some(kv) = ctx.catalog() else {
        // No catalog is reachable, so no object exists to be visible. The
        // family stays total here rather than raising, because it is a filter
        // in every query that uses it and an error would lose the whole row set.
        return Ok(Datum::Null);
    };
    let Some(found) = locate(catalog, kv, oid)? else {
        return Ok(Datum::Null);
    };
    let scope = ctx.resolution();
    for schema in scope.visible_schemas(kv)? {
        if catalog.skips_temp_schemas() && crabka_pgcatalog::is_temp_schema(&schema) {
            continue;
        }
        if schema == found.schema {
            return Ok(Datum::Bool(true));
        }
        if occupied(catalog, kv, &schema, &found.key)? {
            return Ok(Datum::Bool(false));
        }
    }
    Ok(Datum::Bool(false))
}

/// `RelationIsVisible` asked about a relation the caller already has the *name*
/// of rather than an oid: would writing `name.name` unqualified reach this very
/// relation?
///
/// This is the test PostgreSQL's `generate_relation_name` makes before it
/// decides whether to spell a schema out, and it is the same path walk
/// [`is_visible`] makes — the only difference is that the relation is given
/// rather than looked up, which saves the whole-catalog scan an oid lookup
/// costs. Callers that render a name they already hold should use this.
///
/// # Errors
///
/// Propagates storage/corruption errors from the catalog KV seam.
pub(crate) fn relation_name_is_visible(
    kv: &dyn Kv,
    scope: &crate::relname::ResolutionScope,
    name: &RelationName,
) -> Result<bool, ExecError> {
    let key = ShadowKey::Name(name.name.clone());
    for schema in scope.visible_schemas(kv)? {
        if schema == name.schema {
            return Ok(true);
        }
        if occupied(Catalog::Relation, kv, &schema, &key)? {
            return Ok(false);
        }
    }
    Ok(false)
}

/// The argument as a `pg_class`-width oid. A value outside `int4` names no
/// object, so it reads as absent rather than as an error.
fn oid_arg(oid: &Datum) -> Result<Option<i32>, ExecError> {
    match oid {
        Datum::Null => Ok(None),
        other => Ok(i32::try_from(crate::func::int_arg(other)?).ok()),
    }
}

/// Where the object `oid` names lives, or `None` when this catalog has no such
/// object.
fn locate(catalog: Catalog, kv: &dyn Kv, oid: i32) -> Result<Option<Located>, ExecError> {
    Ok(match catalog {
        Catalog::Relation => crate::catalog_rel::relation_for_oid(kv, oid)?.map(|name| Located {
            schema: name.schema,
            key: ShadowKey::Name(name.name),
        }),
        Catalog::Type => locate_type(kv, oid)?,
        Catalog::Function => locate_function(kv, oid)?,
        Catalog::Operator => crate::builtin_operators::BUILTIN_OPERATORS
            .iter()
            .find(|operator| operator.0 == oid)
            .map(|operator| Located {
                schema: crate::search_path::PG_CATALOG.to_string(),
                key: ShadowKey::Operands {
                    name: operator.1.to_string(),
                    left: operator.5,
                    right: operator.6,
                },
            }),
        Catalog::OperatorClass => locate_operator_class(kv, oid)?,
        Catalog::OperatorFamily => locate_operator_family(kv, oid)?,
        Catalog::Collation => crate::catalog_rel::BUILTIN_COLLATIONS
            .iter()
            .find(|collation| collation.0 == oid)
            .map(|collation| catalog_object(collation.1)),
        Catalog::Conversion => crate::builtin_conversions::BUILTIN_CONVERSIONS
            .iter()
            .find(|conversion| conversion.0 == oid)
            .map(|conversion| catalog_object(conversion.1)),
        // crabka records no extended statistics and no text-search parsers or
        // templates, so no oid names one and the answer is always NULL.
        Catalog::StatisticsObject | Catalog::TsParser | Catalog::TsTemplate => None,
        Catalog::TsConfig => locate_text_search(
            kv,
            oid,
            crabka_pgparser::ast::TextSearchObjectKind::Configuration,
        )?,
        Catalog::TsDictionary => locate_text_search(
            kv,
            oid,
            crabka_pgparser::ast::TextSearchObjectKind::Dictionary,
        )?,
    })
}

/// A `pg_catalog` object identified by its bare name — the shape every fixture
/// crabka ships takes, since none of them can be redeclared elsewhere.
fn catalog_object(name: &str) -> Located {
    Located {
        schema: crate::search_path::PG_CATALOG.to_string(),
        key: ShadowKey::Name(name.to_string()),
    }
}

/// Whether `schema` already holds an object of `catalog` under `key`.
fn occupied(
    catalog: Catalog,
    kv: &dyn Kv,
    schema: &str,
    key: &ShadowKey,
) -> Result<bool, ExecError> {
    match catalog {
        Catalog::Relation => {
            // Four keyed `get`s and a static lookup rather than a scan of the
            // relation catalogs: this probe runs once per path entry per row of
            // a `\d` listing, and a scan here is what makes a catalog query
            // quadratic in the number of relations.
            let name = RelationName::new(schema, key.name());
            Ok(crate::catalog_rel::virtual_relation_named(&name)
                || crabka_pgcatalog::relation_exists(kv, &name)?)
        }
        Catalog::Type => type_occupied(kv, schema, key.name()),
        Catalog::Function => function_occupied(kv, schema, key),
        Catalog::Operator => Ok(schema == crate::search_path::PG_CATALOG
            && matches!(key, ShadowKey::Operands { name, left, right }
                if crate::builtin_operators::BUILTIN_OPERATORS.iter().any(|operator|
                    operator.1 == name && operator.5 == *left && operator.6 == *right))),
        Catalog::OperatorClass => operator_class_occupied(kv, schema, key),
        Catalog::OperatorFamily => operator_family_occupied(kv, schema, key),
        Catalog::Collation => Ok(schema == crate::search_path::PG_CATALOG
            && crate::catalog_rel::BUILTIN_COLLATIONS
                .iter()
                .any(|collation| collation.1 == key.name())),
        Catalog::Conversion => Ok(schema == crate::search_path::PG_CATALOG
            && crate::builtin_conversions::BUILTIN_CONVERSIONS
                .iter()
                .any(|conversion| conversion.1 == key.name())),
        // Nothing crabka stores can occupy these names outside `pg_catalog`,
        // and an object already located there is answered before the walk
        // reaches this probe.
        Catalog::StatisticsObject | Catalog::TsParser | Catalog::TsTemplate => Ok(false),
        Catalog::TsConfig => text_search_occupied(
            kv,
            schema,
            key.name(),
            crabka_pgparser::ast::TextSearchObjectKind::Configuration,
        ),
        Catalog::TsDictionary => text_search_occupied(
            kv,
            schema,
            key.name(),
            crabka_pgparser::ast::TextSearchObjectKind::Dictionary,
        ),
    }
}

/// `pg_type`: crabka's built-in types are all `pg_catalog`; a user type carries
/// the schema it was created in, and owns its array companion (and, for a range
/// type, its multirange companion and that companion's array) under the derived
/// oids `pg_type` reports them by.
fn locate_type(kv: &dyn Kv, oid: i32) -> Result<Option<Located>, ExecError> {
    if let Ok(wanted) = u32::try_from(oid) {
        for ty in crabka_pgcatalog::list_user_types(kv)? {
            if let Some(located) = user_type_at(&ty, wanted) {
                return Ok(Some(located));
            }
        }
    }
    let name = crate::exec::regtype_name(oid);
    Ok((crate::exec::regtype_oid(&name) == Some(oid)).then(|| catalog_object(&name)))
}

/// Where a user type answers to `oid`, across the four oids one `CREATE TYPE`
/// occupies: the type itself, its array companion, and — for a range type — its
/// multirange companion and that companion's array.
///
/// The multirange companion is looked up by its own identity because
/// `CREATE TYPE … AS RANGE (multirange_type_name = other.name)` can put it in a
/// different schema from the range type.
fn user_type_at(ty: &crabka_pgtypes::usertype::UserType, oid: u32) -> Option<Located> {
    let here = |name: String| Located {
        schema: ty.schema.clone(),
        key: ShadowKey::Name(name),
    };
    if ty.oid == oid {
        return Some(here(ty.name.clone()));
    }
    if crabka_pgtypes::usertype::user_array_oid(ty.oid) == oid {
        return Some(here(format!("_{}", ty.name)));
    }
    let multirange_oid = ty.multirange_type()?.oid();
    let (schema, name) = ty.multirange_identity()?;
    if multirange_oid == oid {
        return Some(Located {
            schema,
            key: ShadowKey::Name(name),
        });
    }
    (crabka_pgtypes::usertype::user_multirange_array_oid(multirange_oid) == oid).then(|| Located {
        schema,
        key: ShadowKey::Name(format!("_{name}")),
    })
}

/// Is a type of this name declared in `schema`?
///
/// An array companion is not stored in its own right — `_dom` in schema `s` is
/// occupied by whatever `CREATE DOMAIN s.dom` created — so an underscored name
/// is looked for both ways.
fn type_occupied(kv: &dyn Kv, schema: &str, name: &str) -> Result<bool, ExecError> {
    if schema == crate::search_path::PG_CATALOG {
        return Ok(crate::exec::regtype_oid(name).is_some());
    }
    if crabka_pgcatalog::get_user_type(kv, &RelationName::new(schema, name))?.is_some() {
        return Ok(true);
    }
    let Some(element) = name.strip_prefix('_') else {
        return Ok(false);
    };
    Ok(crabka_pgcatalog::get_user_type(kv, &RelationName::new(schema, element))?.is_some())
}

/// `pg_proc`: the built-in fixture is `pg_catalog`; crabka's user routines have
/// no schema of their own and report `public`, which is the namespace
/// `pg_proc.pronamespace` gives them.
fn locate_function(kv: &dyn Kv, oid: i32) -> Result<Option<Located>, ExecError> {
    if let Some((name, args)) = crate::reg_fn::builtin_proc_signature(oid) {
        return Ok(Some(Located {
            schema: crate::search_path::PG_CATALOG.to_string(),
            key: ShadowKey::Signature {
                name: name.to_string(),
                args: args.to_vec(),
            },
        }));
    }
    let routine = crabka_pgcatalog::routine::list_routines(kv)?
        .into_iter()
        .find(|routine| i32::try_from(routine.oid) == Ok(oid));
    routine.map(|routine| {
        Ok::<_, ExecError>(Located {
            schema: crabka_pgcatalog::PUBLIC_SCHEMA.to_string(),
            key: ShadowKey::Signature {
                name: routine.name.clone(),
                args: crate::routine::routine_arg_type_oids(kv, &routine)?,
            },
        })
    }).transpose()
}

/// Is a routine of this exact signature declared in `schema`?
fn function_occupied(kv: &dyn Kv, schema: &str, key: &ShadowKey) -> Result<bool, ExecError> {
    let ShadowKey::Signature { name, args } = key else {
        return Ok(false);
    };
    if schema == crate::search_path::PG_CATALOG {
        return Ok(crate::reg_fn::builtin_proc_declared(name, args));
    }
    if schema != crabka_pgcatalog::PUBLIC_SCHEMA {
        return Ok(false);
    }
    for routine in crabka_pgcatalog::routine::routines_named(kv, name)? {
        if crate::routine::routine_arg_type_oids(kv, &routine)? == *args {
            return Ok(true);
        }
    }
    Ok(false)
}

/// `pg_opclass`: the built-in fixture is `pg_catalog`; a `CREATE OPERATOR
/// CLASS` carries its own schema.
fn locate_operator_class(kv: &dyn Kv, oid: i32) -> Result<Option<Located>, ExecError> {
    if let Some(class) = crate::builtin_opclasses::BUILTIN_OPERATOR_CLASSES
        .iter()
        .find(|class| class.0 == oid)
    {
        return Ok(Some(Located {
            schema: crate::search_path::PG_CATALOG.to_string(),
            key: ShadowKey::Method {
                name: class.2.to_string(),
                method: class.1,
            },
        }));
    }
    Ok(crabka_pgcatalog::list_operator_classes(kv)?
        .into_iter()
        .find(|class| i32::try_from(class.oid) == Ok(oid))
        .map(|class| Located {
            schema: class.name.schema,
            key: ShadowKey::Method {
                name: class.name.name,
                method: crate::catalog_rel::access_method_oid(&class.method).unwrap_or_default(),
            },
        }))
}

fn operator_class_occupied(kv: &dyn Kv, schema: &str, key: &ShadowKey) -> Result<bool, ExecError> {
    let ShadowKey::Method { name, method } = key else {
        return Ok(false);
    };
    if schema == crate::search_path::PG_CATALOG
        && crate::builtin_opclasses::BUILTIN_OPERATOR_CLASSES
            .iter()
            .any(|class| class.2 == name && class.1 == *method)
    {
        return Ok(true);
    }
    Ok(crabka_pgcatalog::list_operator_classes(kv)?
        .iter()
        .any(|class| {
            class.name.schema == schema
                && class.name.name == *name
                && crate::catalog_rel::access_method_oid(&class.method).unwrap_or_default()
                    == *method
        }))
}

/// `pg_opfamily`, the same shape as [`locate_operator_class`].
fn locate_operator_family(kv: &dyn Kv, oid: i32) -> Result<Option<Located>, ExecError> {
    if let Some(family) = crate::builtin_opfamilies::BUILTIN_OPERATOR_FAMILIES
        .iter()
        .find(|family| family.0 == oid)
    {
        return Ok(Some(Located {
            schema: crate::search_path::PG_CATALOG.to_string(),
            key: ShadowKey::Method {
                name: family.2.to_string(),
                method: crate::catalog_rel::access_method_oid(family.1).unwrap_or_default(),
            },
        }));
    }
    Ok(crabka_pgcatalog::list_operator_families(kv)?
        .into_iter()
        .find(|family| i32::try_from(family.oid) == Ok(oid))
        .map(|family| Located {
            schema: family.name.schema,
            key: ShadowKey::Method {
                name: family.name.name,
                method: crate::catalog_rel::access_method_oid(&family.method).unwrap_or_default(),
            },
        }))
}

fn operator_family_occupied(kv: &dyn Kv, schema: &str, key: &ShadowKey) -> Result<bool, ExecError> {
    let ShadowKey::Method { name, method } = key else {
        return Ok(false);
    };
    if schema == crate::search_path::PG_CATALOG
        && crate::builtin_opfamilies::BUILTIN_OPERATOR_FAMILIES
            .iter()
            .any(|family| {
                family.2 == name
                    && crate::catalog_rel::access_method_oid(family.1).unwrap_or_default()
                        == *method
            })
    {
        return Ok(true);
    }
    Ok(crabka_pgcatalog::list_operator_families(kv)?
        .iter()
        .any(|family| {
            family.name.schema == schema
                && family.name.name == *name
                && crate::catalog_rel::access_method_oid(&family.method).unwrap_or_default()
                    == *method
        }))
}

/// `pg_ts_config`/`pg_ts_dict`: crabka declares every text-search object in
/// `pg_catalog`, so the oid only has to name one that exists.
fn locate_text_search(
    kv: &dyn Kv,
    oid: i32,
    kind: crabka_pgparser::ast::TextSearchObjectKind,
) -> Result<Option<Located>, ExecError> {
    Ok(crate::text_search_catalog::catalog_rows(kv, kind)?
        .into_iter()
        .find(|(name, _)| crate::text_search_catalog::object_oid(name) == oid)
        .map(|(name, _)| catalog_object(&name)))
}

fn text_search_occupied(
    kv: &dyn Kv,
    schema: &str,
    name: &str,
    kind: crabka_pgparser::ast::TextSearchObjectKind,
) -> Result<bool, ExecError> {
    if schema != crate::search_path::PG_CATALOG {
        return Ok(false);
    }
    Ok(crate::text_search_catalog::catalog_rows(kv, kind)?
        .iter()
        .any(|(candidate, _)| candidate == name))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgtypes::{ColumnType, Datum};

    use super::{Catalog, is_visible};
    use crate::{clock::EvalCtx, relname::ResolutionScope, search_path::SearchPath};

    /// A session context over `kv` whose `search_path` is `path`, spelled the
    /// way `SET search_path = …` spells it.
    fn ctx(kv: &std::sync::Arc<dyn crabka_pgkv::Kv>, path: &[&str]) -> EvalCtx {
        ctx_as(kv, crate::catalog_fn::OBJECT_OWNER, path)
    }

    /// [`ctx`] under `user` rather than the bootstrap role, which is what the
    /// schema `USAGE` test the path walk makes is answered against.
    fn ctx_as(kv: &std::sync::Arc<dyn crabka_pgkv::Kv>, user: &str, path: &[&str]) -> EvalCtx {
        let scope = ResolutionScope {
            search_path: SearchPath::from_items(
                &path.iter().map(|part| (*part).into()).collect::<Vec<_>>(),
            ),
            user: user.to_string(),
            backend_id: 1,
            ..ResolutionScope::default()
        };
        EvalCtx {
            catalog: Some(std::sync::Arc::clone(kv)),
            resolution: Some(std::sync::Arc::new(scope)),
            ..EvalCtx::test_default()
        }
    }

    /// A catalog holding `CREATE SCHEMA s1; CREATE SCHEMA s2;` and one table in
    /// each of the schemas named in `tables`, as `schema.name` pairs.
    fn catalog(tables: &[(&str, &str)]) -> std::sync::Arc<dyn crabka_pgkv::Kv> {
        let kv: std::sync::Arc<dyn crabka_pgkv::Kv> =
            std::sync::Arc::new(crabka_pgkv::MemKv::default());
        for schema in ["s1", "s2", "s3"] {
            let ops = crabka_pgcatalog::create_schema_ops(
                kv.as_ref(),
                schema,
                crate::catalog_fn::OBJECT_OWNER,
            )
            .expect("create schema");
            kv.write_batch(&ops).expect("apply");
        }
        for (schema, name) in tables {
            create_table(kv.as_ref(), schema, name);
        }
        kv
    }

    fn create_table(kv: &dyn crabka_pgkv::Kv, schema: &str, name: &str) {
        let (_, ops) = crabka_pgcatalog::create_table_ops(
            kv,
            &crabka_pgcatalog::RelationName::new(schema, name),
            vec![crabka_pgcatalog::Column::new("x", ColumnType::Int4)],
        )
        .expect("create table");
        kv.write_batch(&ops).expect("apply");
    }

    fn oid_of(kv: &dyn crabka_pgkv::Kv, schema: &str, name: &str) -> Datum {
        let table =
            crabka_pgcatalog::get_table(kv, &crabka_pgcatalog::RelationName::new(schema, name))
                .expect("table");
        Datum::Int4(crate::catalog_rel::table_relation_oid(table.id).expect("oid"))
    }

    #[test]
    fn a_relation_answers_visible_only_from_the_first_path_entry_that_holds_its_name() {
        let kv = catalog(&[("s1", "t"), ("s2", "t"), ("s3", "only_s3")]);
        for (path, schema, expected) in [
            (vec!["s1", "s2"], "s1", true),
            (vec!["s1", "s2"], "s2", false),
            (vec!["s2", "s1"], "s2", true),
            (vec!["s2", "s1"], "s1", false),
            (vec!["s2"], "s1", false),
            (vec![], "s1", false),
        ] {
            let answer = is_visible(
                Catalog::Relation,
                &oid_of(kv.as_ref(), schema, "t"),
                &ctx(&kv, &path),
            )
            .expect("visible");
            assert!(
                answer == Datum::Bool(expected),
                "search_path = {path:?}, {schema}.t"
            );
        }
    }

    /// A schema the role holds no `USAGE` on is not on the path at all, so
    /// nothing in it is visible and nothing in it shadows. `postgres:18.4`
    /// answers the same three ways for the same setup:
    ///
    /// ```text
    /// SET ROLE lowly; SET search_path = s1, s2;   -- USAGE on s2 only
    /// pg_table_is_visible(s1.t)  -> false
    /// pg_table_is_visible(s2.t)  -> true
    /// pg_table_is_visible(s1.only_s1) -> false
    /// ```
    #[test]
    fn a_relation_in_a_schema_the_role_cannot_search_is_neither_visible_nor_shadowing() {
        let kv = catalog(&[("s1", "t"), ("s2", "t"), ("s1", "only_s1")]);
        crabka_pgcatalog::create_role(kv.as_ref(), "lowly", true).expect("role");
        let ops = crabka_pgcatalog::grant_schema_privileges_ops(
            kv.as_ref(),
            &["s2".to_string()],
            &["lowly".to_string()],
            &["USAGE".to_string()],
        )
        .expect("grant");
        kv.write_batch(&ops).expect("apply");
        let lowly = ctx_as(&kv, "lowly", &["s1", "s2"]);
        let root = ctx(&kv, &["s1", "s2"]);
        for (schema, name, under_lowly, under_root) in [
            ("s1", "t", false, true),
            ("s2", "t", true, false),
            ("s1", "only_s1", false, true),
        ] {
            let oid = oid_of(kv.as_ref(), schema, name);
            assert!(
                is_visible(Catalog::Relation, &oid, &lowly).expect("visible")
                    == Datum::Bool(under_lowly),
                "{schema}.{name} as lowly"
            );
            assert!(
                is_visible(Catalog::Relation, &oid, &root).expect("visible")
                    == Datum::Bool(under_root),
                "{schema}.{name} as the bootstrap role"
            );
        }
    }

    #[test]
    fn an_unrelated_relation_created_in_an_earlier_schema_hides_a_later_one() {
        let kv = catalog(&[("s2", "only_s3")]);
        let oid = oid_of(kv.as_ref(), "s2", "only_s3");
        let ctx = ctx(&kv, &["s1", "s2"]);
        assert!(is_visible(Catalog::Relation, &oid, &ctx).expect("before") == Datum::Bool(true));

        create_table(kv.as_ref(), "s1", "only_s3");

        assert!(is_visible(Catalog::Relation, &oid, &ctx).expect("after") == Datum::Bool(false));
    }

    #[test]
    fn a_catalog_relation_is_visible_even_under_an_empty_search_path() {
        let kv = catalog(&[]);
        let empty = ctx(&kv, &[]);
        for oid in [crate::exec::virtual_relation_oid("pg_class")]
            .into_iter()
            // A catalog's own oid index is a `pg_class` row too, and has to
            // answer for its pinned oid rather than report no such relation.
            .chain(
                crate::exec::BUILTIN_CATALOG_OID_INDEXES
                    .iter()
                    .map(|index| index.oid),
            )
        {
            let answer = is_visible(Catalog::Relation, &Datum::Int4(oid), &empty).expect("answer");
            assert!(answer == Datum::Bool(true), "oid {oid}");
        }
    }

    #[test]
    fn an_oid_no_object_carries_answers_null() {
        let kv = catalog(&[]);
        let session = ctx(&kv, &["s1"]);
        for catalog in [
            Catalog::Relation,
            Catalog::Type,
            Catalog::Function,
            Catalog::Operator,
            Catalog::OperatorClass,
            Catalog::OperatorFamily,
            Catalog::Collation,
            Catalog::Conversion,
            Catalog::StatisticsObject,
            Catalog::TsConfig,
            Catalog::TsDictionary,
            Catalog::TsParser,
            Catalog::TsTemplate,
        ] {
            let answer = is_visible(catalog, &Datum::Int4(999_999), &session).expect("answer");
            assert!(answer == Datum::Null, "{catalog:?} for a missing oid");
            let null = is_visible(catalog, &Datum::Null, &session).expect("answer");
            assert!(null == Datum::Null, "{catalog:?} for NULL");
        }
    }

    #[test]
    fn every_family_member_the_oracle_declares_is_dispatched() {
        for name in [
            "pg_table_is_visible",
            "pg_type_is_visible",
            "pg_function_is_visible",
            "pg_operator_is_visible",
            "pg_opclass_is_visible",
            "pg_opfamily_is_visible",
            "pg_collation_is_visible",
            "pg_conversion_is_visible",
            "pg_statistics_obj_is_visible",
            "pg_ts_config_is_visible",
            "pg_ts_dict_is_visible",
            "pg_ts_parser_is_visible",
            "pg_ts_template_is_visible",
        ] {
            assert!(Catalog::for_function(name).is_some(), "{name}");
            // Exactly one of the two dispatch routes has to claim the name, or
            // the call reports `42883 does not exist` at the wire.
            assert!(
                crate::catalog_fn::is_catalog_func(name) || crate::func::is_scalar(name),
                "{name} is not dispatched"
            );
        }
        assert!(Catalog::for_function("pg_relation_is_publishable").is_none());
        assert!(Catalog::for_function("pg_table_is_visible_not_really").is_none());
    }

    /// `pg_relation_is_publishable` shared the visibility stub, and it is a
    /// different question: crabka publishes every relation whatever the search
    /// path reaches.
    #[test]
    fn publishability_does_not_follow_the_search_path() {
        let kv = catalog(&[("s1", "t")]);
        let session = ctx(&kv, &["s2"]);
        let oid = oid_of(kv.as_ref(), "s1", "t");
        assert!(
            is_visible(Catalog::Relation, &oid, &session).expect("visible") == Datum::Bool(false)
        );
        let call = crabka_pgparser::ast::FuncCall {
            sql_syntax: false,
            name: "pg_relation_is_publishable".into(),
            distinct: false,
            args: crabka_pgparser::ast::FuncArgs::Exprs(vec![
                crabka_pgparser::ast::Expr::IntLiteral("0".into()),
           ]),
           order_by: Vec::new(),
            within_group: false,
           filter: None,
        };
        let publishable =
            crate::catalog_fn::eval_catalog(&call, &session, |_| Ok(oid.clone())).expect("answer");
        assert!(publishable == Datum::Bool(true));
    }

    #[test]
    fn a_builtin_type_operator_collation_and_text_search_object_stay_visible() {
        let kv = catalog(&[]);
        let session = ctx(&kv, &[]);
        for (catalog, oid) in [
            (Catalog::Type, 23),
            (Catalog::Operator, 551),
            (Catalog::Collation, 950),
            (
                Catalog::TsConfig,
                crate::text_search_catalog::object_oid("simple"),
            ),
        ] {
            let answer = is_visible(catalog, &Datum::Int4(oid), &session).expect("answer");
            assert!(answer == Datum::Bool(true), "{catalog:?} {oid}");
        }
    }
}
