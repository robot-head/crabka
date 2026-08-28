use super::*;

pub(super) fn requalify_view_relation(
    mut relation: Relation,
    view: &crabka_pgcatalog::View,
    qualifier: &str,
    row_type: Option<crabka_pgtypes::usertype::UserTypeRef>,
) -> Result<Relation, ExecError> {
    if relation.scope.width() != view.columns.len() {
        return Err(ExecError::Unsupported(
            "stored view definition does not match its catalog schema".into(),
        ));
    }
    for (binding, column) in relation.scope.columns.iter_mut().zip(&view.columns) {
        binding.qualifier = Some(qualifier.to_string());
        binding.name.clone_from(&column.name);
        binding.ty = column.ty;
    }
    relation.scope.replace_row_type(qualifier, row_type);
    Ok(relation)
}

/// Resolve an existing user type through the type catalog, not relation
/// existence.
pub(crate) fn resolve_user_type(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    reference: &crabka_pgparser::ast::RelationRef,
) -> Result<crabka_pgcatalog::RelationName, ExecError> {
    if reference.schema.is_some() {
        return resolve_relation(kv, resolution, reference, SchemaDisposition::Utility);
    }
    let user_types = crabka_pgcatalog::list_user_types(kv)?;
    for schema in resolution.visible_schemas(kv)? {
        let candidate = crabka_pgcatalog::RelationName::new(schema, reference.name.clone());
        if candidate.schema == "pg_catalog" && is_builtin_catalog_type_name(&candidate.name) {
            return Ok(candidate);
        }
        let identity = (candidate.schema.clone(), candidate.name.clone());
        if crabka_pgcatalog::get_user_type(kv, &candidate)?.is_some()
            || user_types
                .iter()
                .any(|ty| ty.multirange_identity() == Some(identity.clone()))
        {
            return Ok(candidate);
        }
    }
    resolve_relation(kv, resolution, reference, SchemaDisposition::Utility)
}

pub(super) fn resolve_user_types(
    kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    references: &[crabka_pgparser::ast::RelationRef],
) -> Result<Vec<crabka_pgcatalog::RelationName>, ExecError> {
    references
        .iter()
        .map(|reference| resolve_user_type(kv, resolution, reference))
        .collect()
}

pub(super) fn check_view_columns_replaceable(
    existing: &[Column],
    replacement: &[Column],
    view: &crabka_pgcatalog::RelationName,
) -> Result<(), ExecError> {
    if replacement.len() < existing.len() {
        return Err(ExecError::InvalidTableDefinition(
            "cannot drop columns from view".into(),
        ));
    }
    for (old, new) in existing.iter().zip(replacement) {
        if old.name != new.name {
            return Err(ExecError::Remote(
                crabka_pgwire::error::PgError::error(
                    "42P16",
                    format!(
                        "cannot change name of view column \"{}\" to \"{}\"",
                        old.name, new.name
                    ),
                )
                .with_hint(
                    "Use ALTER VIEW ... RENAME COLUMN ... to change name of view column instead.",
                ),
            ));
        }
        if old.ty != new.ty {
            return Err(ExecError::InvalidTableDefinition(format!(
                "cannot change data type of view column \"{}\" from {} to {}",
                old.name,
                old.ty.name(),
                new.ty.name()
            )));
        }
    }
    for appended in &replacement[existing.len()..] {
        if existing.iter().any(|old| old.name == appended.name) {
            return Err(ExecError::DuplicateColumn {
                column: appended.name.clone(),
                table: view.to_string(),
            });
        }
    }
    Ok(())
}

pub(super) fn validate_view_definition(
    catalog_kv: &dyn Kv,
    resolution: &crate::relname::ResolutionScope,
    query: &crabka_pgparser::ast::QueryExpr,
) -> Result<Vec<crabka_pgcatalog::RelationName>, ExecError> {
    if query.locking.is_some() {
        return Err(ExecError::Unsupported(
            "CREATE VIEW does not support locking SELECT".into(),
        ));
    }
    let mut references = Vec::new();
    let mut refusal = None;
    crate::viewdeps::walk_query(query, &mut |node| {
        let found = match node {
            crate::viewdeps::Node::Relation(source) => {
                references.push(source.reference);
                return;
            }
            crate::viewdeps::Node::Expr(Expr::Param(number)) => {
                ExecError::Remote(crabka_pgwire::error::PgError::error(
                    "42P02",
                    format!("there is no parameter ${number}"),
                ))
            }
            crate::viewdeps::Node::DataModifyingCte => ExecError::Unsupported(
                "views must not contain data-modifying statements in WITH".into(),
            ),
            crate::viewdeps::Node::Expr(_) | crate::viewdeps::Node::ComputedFrom => return,
        };
        if refusal.is_none() {
            refusal = Some(found);
        }
    });
    if let Some(refusal) = refusal {
        return Err(refusal);
    }
    let mut sources = Vec::new();
    for reference in references {
        let name = resolve_relation(
            catalog_kv,
            resolution,
            reference,
            SchemaDisposition::Reference,
        )?;
        if !sources.contains(&name) {
            sources.push(name);
        }
    }
    Ok(sources)
}
