//! Materialized common table expression scope for SELECT execution.

use std::collections::HashMap;

use crabka_pgparser::ast::WithClause;

use crate::{clock::EvalCtx, error::ExecError, join::Relation};

#[derive(Debug, Clone, Default)]
pub(crate) struct CteContext {
    relations: HashMap<String, Relation>,
}

impl CteContext {
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    pub(crate) fn child(&self) -> Self {
        self.clone()
    }

    pub(crate) fn lookup(&self, name: &str) -> Option<&Relation> {
        self.relations.get(name)
    }

    pub(crate) fn insert(&mut self, name: String, rel: Relation) {
        self.relations.insert(name, rel);
    }
}

pub(crate) fn reject_recursive(with: &WithClause) -> Result<(), ExecError> {
    if with.recursive {
        return Err(ExecError::Unsupported(
            "recursive CTEs are not supported yet".into(),
        ));
    }
    Ok(())
}

pub(crate) fn requalify_cte(rel: &Relation, alias: &str) -> Relation {
    let mut out = rel.clone();
    for col in &mut out.scope.columns {
        col.qualifier = Some(alias.to_string());
    }
    out
}

pub(crate) fn apply_cte_column_aliases(
    rel: Relation,
    name: &str,
    columns: &Option<Vec<String>>,
) -> Result<Relation, ExecError> {
    crate::values::requalify_derived(rel, name, columns)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn evaluate_with_clause(
    catalog_kv: &dyn crabka_pgkv::Kv,
    kv: &dyn crabka_pgkv::Kv,
    global: &dyn crabka_pgkv::Kv,
    gsnap: &crabka_pgmvcc::visibility::Snapshot,
    snapshot: &crabka_pgmvcc::visibility::Snapshot,
    own: Option<u64>,
    with: Option<&WithClause>,
    parent: &CteContext,
    ctx: &EvalCtx,
    fctx: crate::exec::ForeignCtx,
    range_scanner: &dyn crate::scanner::RangeScanner,
) -> Result<CteContext, ExecError> {
    let Some(with) = with else {
        return Ok(parent.child());
    };
    reject_recursive(with)?;

    let mut out = parent.child();
    for cte in &with.ctes {
        if cte.query.locking.is_some() {
            return Err(ExecError::Unsupported(
                "FOR UPDATE/SHARE is not supported in CTEs".into(),
            ));
        }
        let rel = crate::query::query_to_relation_with_ctes(
            catalog_kv,
            kv,
            global,
            gsnap,
            snapshot,
            own,
            &cte.query,
            &out,
            ctx,
            fctx,
            range_scanner,
        )?;
        let rel = apply_cte_column_aliases(rel, &cte.name, &cte.columns)?;
        out.insert(cte.name.clone(), rel);
    }
    Ok(out)
}

pub(crate) fn describe_with_clause(
    catalog_kv: &dyn crabka_pgkv::Kv,
    with: Option<&WithClause>,
    parent: &CteContext,
) -> Result<CteContext, ExecError> {
    let Some(with) = with else {
        return Ok(parent.child());
    };
    reject_recursive(with)?;

    let mut out = parent.child();
    for cte in &with.ctes {
        let rel = describe_cte_relation(catalog_kv, cte, &out)?;
        out.insert(cte.name.clone(), rel);
    }
    Ok(out)
}

pub(crate) fn describe_cte_relation(
    catalog_kv: &dyn crabka_pgkv::Kv,
    cte: &crabka_pgparser::ast::Cte,
    ctes: &CteContext,
) -> Result<Relation, ExecError> {
    let fields = crate::query::describe_query_expr_with_ctes(catalog_kv, &cte.query, ctes)?;
    let columns = fields
        .iter()
        .map(|f| {
            Ok(crate::scope::ColumnBinding {
                qualifier: None,
                name: f.name.clone(),
                ty: crate::exec::column_type_from_oid(f.type_oid)?,
            })
        })
        .collect::<Result<Vec<_>, ExecError>>()?;
    let rel = Relation {
        scope: crate::scope::Scope { columns },
        rows: Vec::new(),
    };
    apply_cte_column_aliases(rel, &cte.name, &cte.columns)
}
