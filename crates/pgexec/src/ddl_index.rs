//! DDL and catalog code carved out of `exec`.

use super::{
    ColumnType, ExecError, HashSet, Kv, Scope, Table, available_index_name, is_immutable_function,
};

pub(crate) fn index_name_or_default(
    kv: &dyn Kv,
    explicit: Option<&str>,
    table: &crabka_pgcatalog::RelationName,
    keys: &[crabka_pgparser::ast::IndexKey],
) -> String {
    if let Some(name) = explicit {
        return name.to_string();
    }
    let parts: Vec<&str> = keys
        .iter()
        .map(|key| key.column.as_deref().unwrap_or("expr"))
        .collect();
    let base = format!("{}_{}_idx", table.name, parts.join("_"));
    available_index_name(kv, table, &base, &HashSet::new())
}

/// The durable key list an index can be built from. Expression source uses the
/// catalog's NUL-prefixed encoding, which cannot collide with a SQL identifier.
pub(crate) fn index_key_columns(
    keys: &[crabka_pgparser::ast::IndexKey],
    _predicate: Option<&str>,
) -> Result<Vec<String>, ExecError> {
    keys.iter()
        .map(|key| {
            Ok(key
                .column
                .clone()
                .unwrap_or_else(|| crabka_pgcatalog::expression_index_key(&key.text)))
        })
        .collect()
}

/// Catalog metadata that stays aligned with [`index_key_columns`].
#[must_use]
pub(crate) fn index_key_options(
    keys: &[crabka_pgparser::ast::IndexKey],
) -> Vec<crabka_pgcatalog::IndexKeyOptions> {
    keys.iter()
        .map(|key| crabka_pgcatalog::IndexKeyOptions {
            descending: key.descending,
            nulls_first: key.nulls_first.unwrap_or(key.descending),
            opclass: key.opclass.clone(),
            opclass_options: key.opclass_options.clone(),
            collation: key.collation.clone(),
        })
        .collect()
}

pub(crate) fn validate_index_predicate(
    table: &Table,
    predicate: Option<&str>,
) -> Result<(), ExecError> {
    let Some(predicate) = predicate else {
        return Ok(());
    };
    let expression = crabka_pgparser::parser::parse_expression(predicate)?;
    let scope = Scope::single(table, &table.name.name);
    crate::eval::check_predicate_resolves(&expression, &scope)?;
    let mut invalid = crate::agg::contains_aggregate(&expression);
    crate::grouping::visit_expr(&expression, &mut |node| {
        invalid |= matches!(
            node,
            crabka_pgparser::ast::Expr::ScalarSubquery(_)
                | crabka_pgparser::ast::Expr::Exists(_)
                | crabka_pgparser::ast::Expr::InSubquery { .. }
                | crabka_pgparser::ast::Expr::Quantified { .. }
        ) || matches!(node, crabka_pgparser::ast::Expr::Func(call) if !is_immutable_function(&call.name));
    });
    if invalid {
        return Err(ExecError::InvalidObjectDefinition(
            "functions in index predicate must be marked IMMUTABLE".into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_index_expressions(
    table: &Table,
    keys: &[crabka_pgparser::ast::IndexKey],
    unique: bool,
    placement: crabka_pgcatalog::IndexPlacement,
    method: crabka_pgcatalog::IndexMethod,
) -> Result<(), ExecError> {
    use crabka_pgparser::ast::Expr;

    let expressions: Vec<&str> = keys
        .iter()
        .filter_map(|key| key.column.is_none().then_some(key.text.as_str()))
        .collect();
    if expressions.is_empty() {
        return Ok(());
    }
    // B-tree expression keys use the same immutable evaluator as ordinary
    // keys. The remaining access methods are catalog-only for expressions.
    if (unique
        && (placement != crabka_pgcatalog::IndexPlacement::Local
            || method != crabka_pgcatalog::IndexMethod::Btree))
        || (!unique
            && (placement != crabka_pgcatalog::IndexPlacement::Local
                || !matches!(
                    method,
                    crabka_pgcatalog::IndexMethod::Btree
                        | crabka_pgcatalog::IndexMethod::Gist
                        | crabka_pgcatalog::IndexMethod::Spgist
                )))
    {
        return Err(ExecError::Unsupported(
            "expression indexes currently require a local B-tree, GiST, or SP-GiST index".into(),
        ));
    }
    let scope = Scope::single(table, &table.name.name);
    for source in expressions {
        let expr = crabka_pgparser::parser::parse_expression(source)?;
        crate::eval::infer_type(&expr, &scope)?;
        let mut invalid = false;
        crate::grouping::visit_expr(&expr, &mut |node| {
            invalid |= matches!(
                node,
                Expr::ScalarSubquery(_)
                    | Expr::Exists(_)
                    | Expr::InSubquery { .. }
                    | Expr::Quantified { .. }
            ) || matches!(node, Expr::Func(call) if !is_immutable_function(&call.name));
        });
        if invalid || crate::agg::contains_aggregate(&expr) {
            return Err(ExecError::InvalidObjectDefinition(
                "functions in index expression must be marked IMMUTABLE".into(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_index_opclasses(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    table: &Table,
    keys: &[crabka_pgparser::ast::IndexKey],
    method: crabka_pgcatalog::IndexMethod,
) -> Result<(), ExecError> {
    let method_name = index_method_name(method);
    let method_oid = match method {
        crabka_pgcatalog::IndexMethod::Btree => crate::catalog_rel::BTREE_AM_OID,
        crabka_pgcatalog::IndexMethod::Hash => crate::catalog_rel::HASH_AM_OID,
        crabka_pgcatalog::IndexMethod::Gist => crate::catalog_rel::GIST_AM_OID,
        crabka_pgcatalog::IndexMethod::Gin => crate::catalog_rel::GIN_AM_OID,
        crabka_pgcatalog::IndexMethod::Spgist => crate::catalog_rel::SPGIST_AM_OID,
    };
    let user_classes = crabka_pgcatalog::list_operator_classes(kv)?;
    let visible_schemas = resolution.visible_schemas(kv)?;
    for key in keys {
        let Some(written) = key.opclass.as_deref() else {
            let ty = match key.column.as_deref() {
                Some(column) => {
                    table
                        .columns
                        .iter()
                        .find(|candidate| candidate.name == column)
                        .ok_or_else(|| ExecError::UndefinedColumn(column.into()))?
                        .ty
                }
                None => crate::eval::infer_type(
                    &crabka_pgparser::parser::parse_expression(&key.text)?,
                    &Scope::single(table, &table.name.name),
                )?,
            };
            validate_default_index_opclass(ty, method)?;
            continue;
        };
        let (schema, name) = written
            .split_once('.')
            .map_or((None, written), |(schema, name)| (Some(schema), name));
        let builtin_input_oid = || {
            crate::builtin_opclasses::BUILTIN_OPERATOR_CLASSES
                .iter()
                .find(|(_, am_oid, class_name, _, _, _, _)| {
                    *am_oid == method_oid && *class_name == name
                })
                .map(|(_, _, _, _, input_oid, _, _)| *input_oid as u32)
        };
        let user_input_oid = |schema: &str| {
            user_classes
                .iter()
                .find(|class| {
                    class.method == method_name
                        && class.name.name == name
                        && class.name.schema == schema
                })
                .map(|class| class.input_type_oid)
        };
        let input_oid = match schema {
            Some("pg_catalog") => builtin_input_oid(),
            Some(schema) => user_input_oid(schema),
            None => visible_schemas.iter().find_map(|schema| {
                if schema == "pg_catalog" {
                    builtin_input_oid()
                } else {
                    user_classes
                        .iter()
                        .find(|class| {
                            class.method == method_name
                                && class.name.name == name
                                && class.name.schema == *schema
                        })
                        .map(|class| class.input_type_oid)
                }
            }),
        }
        .ok_or_else(|| {
            ExecError::UndefinedObject(format!(
                "operator class \"{written}\" does not exist for access method \"{method_name}\""
            ))
        })?;
        let ty = match key.column.as_deref() {
            Some(column) => {
                table
                    .columns
                    .iter()
                    .find(|candidate| candidate.name == column)
                    .ok_or_else(|| ExecError::UndefinedColumn(column.into()))?
                    .ty
            }
            None => crate::eval::infer_type(
                &crabka_pgparser::parser::parse_expression(&key.text)?,
                &Scope::single(table, &table.name.name),
            )?,
        };
        let column_oid = ty.oid();
        let binary_text_compatible = input_oid == crabka_pgtypes::oids::TEXT
            && matches!(ty, ColumnType::Text | ColumnType::Varchar(_));
        if input_oid != column_oid && input_oid != 2277 && !binary_text_compatible {
            return Err(ExecError::TypeMismatch(format!(
                "operator class \"{written}\" does not accept data type {}",
                crate::func::format_type(i64::from(column_oid), -1)
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_default_index_opclass(
    ty: ColumnType,
    method: crabka_pgcatalog::IndexMethod,
) -> Result<(), ExecError> {
    // `json` and `xml` have no operator class for ANY access method — neither
    // has an equality operator to build one on — so unlike `jsonpath` they are
    // not limited to btree and hash. This is also what makes `j json UNIQUE`
    // and `j json PRIMARY KEY` fail, since both build a btree index.
    if matches!(ty.storage_type(), ColumnType::Json | ColumnType::Xml) {
        return Err(ExecError::UndefinedObject(format!(
            "data type {} has no default operator class for access method \"{}\"",
            ty.name(),
            index_method_name(method)
        )));
    }
    if ty.storage_type() == ColumnType::JsonPath
        && matches!(
            method,
            crabka_pgcatalog::IndexMethod::Btree | crabka_pgcatalog::IndexMethod::Hash
        )
    {
        return Err(ExecError::UndefinedObject(format!(
            "data type {} has no default operator class for access method \"{}\"",
            ty.name(),
            index_method_name(method)
        )));
    }
    // `xid` and `cid` are the reverse of `jsonpath`: they have a HASH opclass
    // but no btree one, because transaction ids compare with modular
    // arithmetic and so have no sort order for a btree to hold. `x xid UNIQUE`
    // and `x xid PRIMARY KEY` fail for the same reason, since both build one.
    if matches!(ty.storage_type(), ColumnType::Xid | ColumnType::Cid)
        && method == crabka_pgcatalog::IndexMethod::Btree
    {
        return Err(ExecError::UndefinedObject(format!(
            "data type {} has no default operator class for access method \"{}\"",
            ty.name(),
            index_method_name(method)
        )));
    }
    Ok(())
}

pub(crate) fn validate_index_method(
    table: &Table,
    columns: &[String],
    unique: bool,
    placement: crabka_pgcatalog::IndexPlacement,
    method: crabka_pgcatalog::IndexMethod,
) -> Result<(), ExecError> {
    if method == crabka_pgcatalog::IndexMethod::Btree {
        return Ok(());
    }
    if method != crabka_pgcatalog::IndexMethod::Gin {
        if unique {
            return Err(ExecError::Unsupported(format!(
                "access method {} does not support unique indexes",
                index_method_name(method)
            )));
        }
        return Ok(());
    }
    // ponytail: one stored tsvector column keeps maintenance on the existing
    // row path; add expression/multicolumn GIN only when queries require it.
    if unique {
        return Err(ExecError::Unsupported(
            "access method gin does not support unique indexes".into(),
        ));
    }
    if placement != crabka_pgcatalog::IndexPlacement::Local {
        return Err(ExecError::Unsupported(
            "global GIN indexes are not supported".into(),
        ));
    }
    let [column] = columns else {
        return Err(ExecError::Unsupported(
            "GIN indexes currently require exactly one tsvector column".into(),
        ));
    };
    let column = table
        .column_index(column)
        .ok_or_else(|| ExecError::UndefinedColumn(column.clone()))?;
    if table.columns[column].ty != ColumnType::TsVector {
        return Err(ExecError::Unsupported(
            "GIN indexes currently support only tsvector columns".into(),
        ));
    }
    Ok(())
}

pub(crate) fn index_method_name(method: crabka_pgcatalog::IndexMethod) -> &'static str {
    match method {
        crabka_pgcatalog::IndexMethod::Btree => "btree",
        crabka_pgcatalog::IndexMethod::Hash => "hash",
        crabka_pgcatalog::IndexMethod::Gist => "gist",
        crabka_pgcatalog::IndexMethod::Gin => "gin",
        crabka_pgcatalog::IndexMethod::Spgist => "spgist",
    }
}
