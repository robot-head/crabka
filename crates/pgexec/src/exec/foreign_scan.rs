//! Foreign-table scan eligibility.

use super::*;

pub(crate) fn is_single_foreign_table(
    catalog_kv: &dyn Kv,
    from: &[crabka_pgparser::ast::TableExpr],
    ctes: &crate::cte::CteContext,
    fctx: ForeignCtx,
) -> bool {
    if fctx.scanner.is_none() {
        return false;
    }
    let resolution = fctx.resolution;
    let [
        crabka_pgparser::ast::TableExpr::Table {
            name,
            columns: None,
            sample: None,
            ..
        },
    ] = from
    else {
        return false;
    };
    if name.schema.is_none() && ctes.lookup(&name.name).is_some() {
        return false;
    }
    resolve_relation(catalog_kv, resolution, name, SchemaDisposition::Reference).is_ok_and(|name| {
        crabka_pgcatalog::get_table(catalog_kv, &name).is_ok_and(|t| t.foreign.is_some())
    })
}
