//! Foreign-table scan eligibility.

use super::*;

pub(super) fn require_handler(catalog_kv: &dyn Kv, table: &Table) -> Result<(), ExecError> {
    let Some(foreign) = &table.foreign else {
        return Ok(());
    };
    let server = crabka_pgcatalog::get_server(catalog_kv, &foreign.server)?;
    require_server_handler(catalog_kv, &server)
}

pub(super) fn require_server_handler(
    catalog_kv: &dyn Kv,
    server: &crabka_pgcatalog::ForeignServer,
) -> Result<(), ExecError> {
    let fdw = crabka_pgcatalog::get_fdw(catalog_kv, &server.wrapper)?;
    if fdw.handler.is_none() {
        return Err(ExecError::Unsupported(format!(
            "foreign-data wrapper \"{}\" has no handler",
            fdw.name
        )));
    }
    Ok(())
}

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
