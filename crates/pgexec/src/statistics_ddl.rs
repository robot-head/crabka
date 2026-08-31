//! Extended-statistics DDL over the durable catalog record.

use std::collections::BTreeSet;

use crabka_pgcatalog::{RelationName, statistics::Statistics};
use crabka_pgkv::{Kv, WriteOp};
use crabka_pgparser::ast::{self, Expr, TableExpr};
use crabka_pgwire::engine::QueryResult;

use crate::{
    error::ExecError,
    exec::ForeignCtx,
    privilege::{RelationKind, require_ownership},
    relname::{SchemaDisposition, resolve_relation},
};

type DdlResult = Result<(QueryResult, Vec<WriteOp>), ExecError>;

const MAX_KEYS: usize = 8;

fn command(tag: &str) -> QueryResult {
    QueryResult::Command { tag: tag.into() }
}

/// `CREATE STATISTICS` is intentionally limited to a plain relation here.
/// PostgreSQL parses the larger FROM grammar and rejects every other shape
/// semantically, so the parser retains it and this is the single shared gate.
fn source_table(stats: &ast::CreateStatistics) -> Result<&ast::RelationRef, ExecError> {
    match &stats.from {
        TableExpr::Table {
            name,
            only: false,
            alias: None,
            columns: None,
            sample: None,
        } => Ok(name),
        _ => Err(ExecError::Unsupported(
            "CREATE STATISTICS only supports relation names in the FROM clause".into(),
        )),
    }
}

fn kinds(kinds: &[String], keys: &[i16]) -> Result<Vec<String>, ExecError> {
    let kinds = if kinds.is_empty() {
        if keys == [0] {
            return Ok(vec!["e".into()]);
        }
        let mut defaults = vec!["d".into(), "f".into(), "m".into()];
        if keys.contains(&0) {
            defaults.push("e".into());
        }
        defaults
    } else {
        kinds
            .iter()
            .map(|kind| match kind.as_str() {
                "ndistinct" => Ok("d".into()),
                "dependencies" => Ok("f".into()),
                "mcv" => Ok("m".into()),
                "expressions" => Ok("e".into()),
                _ => Err(ExecError::Unsupported(format!(
                    "unrecognized statistics kind \"{kind}\""
                ))),
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    let mut kinds = kinds;
    if keys.contains(&0) && !kinds.iter().any(|kind| kind == "e") {
        kinds.push("e".into());
    }
    let mut seen = BTreeSet::new();
    if kinds.iter().any(|kind| !seen.insert(kind)) {
        return Err(ExecError::Unsupported(
            "duplicate statistics kind in definition".into(),
        ));
    }
    Ok(kinds)
}

fn expression_text(expr: &Expr) -> String {
    crate::viewdef::expression_text(
        expr,
        crabka_pgtypes::encoding::OutputStyle::with_zone(&jiff::tz::TimeZone::UTC),
    )
}

fn statistic_column_ordinal(
    table: &crabka_pgcatalog::Table,
    name: &str,
) -> Result<usize, ExecError> {
    if matches!(
        name,
        "ctid" | "oid" | "tableoid" | "xmin" | "cmin" | "xmax" | "cmax"
    ) {
        return Err(ExecError::Unsupported(
            "statistics creation on system columns is not supported".into(),
        ));
    }
    let ordinal = table
        .column_index(name)
        .ok_or_else(|| ExecError::UndefinedTableColumn {
            column: name.into(),
            table: table.name.name.clone(),
        })?;
    if table.columns[ordinal]
        .generated
        .as_ref()
        .is_some_and(|generated| generated.kind == crabka_pgcatalog::GeneratedKind::Virtual)
    {
        return Err(ExecError::Unsupported(
            "statistics creation on virtual generated columns is not supported".into(),
        ));
    }
    if crate::eval::has_no_btree_opclass(table.columns[ordinal].ty) {
        return Err(ExecError::Unsupported(format!(
            "column \"{name}\" cannot be used in statistics because its type {} has no default btree operator class",
            table.columns[ordinal].ty.name()
        )));
    }
    Ok(ordinal)
}

fn validate_expression_columns(
    expr: &Expr,
    table: &crabka_pgcatalog::Table,
) -> Result<(), ExecError> {
    let mut error = None;
    crate::grouping::visit_expr(expr, &mut |node| {
        if error.is_none()
            && let Expr::Column { table: None, name } = node
            && let Err(problem) = statistic_column_ordinal(table, name)
        {
            error = Some(problem);
        }
    });
    error.map_or(Ok(()), Err)
}

fn definition(
    stats: &ast::CreateStatistics,
    table: &crabka_pgcatalog::Table,
) -> Result<(Vec<i16>, Vec<String>), ExecError> {
    if stats.expressions.len() > MAX_KEYS {
        return Err(ExecError::Unsupported(format!(
            "cannot have more than {MAX_KEYS} columns in statistics"
        )));
    }
    let mut seen = BTreeSet::new();
    let mut keys = Vec::with_capacity(stats.expressions.len());
    let mut expressions = Vec::new();
    for expr in &stats.expressions {
        validate_expression_columns(expr, table)?;
        let text = expression_text(expr);
        if !seen.insert(text.clone()) {
            return Err(ExecError::Unsupported(
                "duplicate expression in statistics definition".into(),
            ));
        }
        match expr {
            Expr::Column { table: None, name } => {
                let ordinal = statistic_column_ordinal(table, name)?;
                keys.push(i16::try_from(ordinal + 1).map_err(|_| {
                    ExecError::Unsupported("statistics column number exceeds int2".into())
                })?);
            }
            _ => {
                keys.push(0);
                expressions.push(text);
            }
        }
    }
    if keys.len() < 2 && keys.first().copied() != Some(0) {
        return Err(ExecError::Unsupported(
            "extended statistics require at least 2 columns".into(),
        ));
    }
    Ok((keys, expressions))
}

pub(crate) fn create(
    kv: &dyn Kv,
    stats: &ast::CreateStatistics,
    fctx: ForeignCtx<'_>,
) -> DdlResult {
    let source = resolve_relation(
        kv,
        fctx.resolution,
        source_table(stats)?,
        SchemaDisposition::Utility,
    )?;
    let table = crabka_pgcatalog::get_table(kv, &source)?;
    require_ownership(
        kv,
        &source,
        &table.owner,
        RelationKind::Table,
        fctx.effective_role(),
    )?;
    let name = resolve_relation(
        kv,
        fctx.resolution,
        &stats.name,
        SchemaDisposition::Creation,
    )?;
    if stats.if_not_exists && crabka_pgcatalog::statistics::get(kv, &name)?.is_some() {
        return Ok((command("CREATE STATISTICS"), Vec::new()));
    }
    let (keys, expressions) = definition(stats, &table)?;
    let kinds = kinds(&stats.kinds, &keys)?;
    let record = Statistics {
        oid: 0,
        name,
        table_id: table.id,
        owner: fctx.effective_role().to_string(),
        target: -1,
        keys,
        kinds,
        expressions,
        data: None,
    };
    Ok((
        command("CREATE STATISTICS"),
        crabka_pgcatalog::statistics::create_ops(kv, &record)?,
    ))
}

pub(crate) fn require_statistics_owner(
    kv: &dyn Kv,
    object: &Statistics,
    role: &str,
) -> Result<(), ExecError> {
    if crabka_pgcatalog::role_has_privs_of(kv, role, &object.owner)?
        || crate::rls::role_is_superuser(kv, role)?
    {
        return Ok(());
    }
    Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
        "42501",
        format!("must be owner of statistics object {}", object.name.name),
    )))
}

pub(crate) fn alter(
    kv: &dyn Kv,
    name: RelationName,
    if_exists: bool,
    action: &ast::AlterStatisticsAction,
    fctx: ForeignCtx<'_>,
) -> DdlResult {
    let Some(mut object) = crabka_pgcatalog::statistics::get(kv, &name)? else {
        if if_exists {
            return Ok((command("ALTER STATISTICS"), Vec::new()));
        }
        return Err(crabka_pgcatalog::CatalogError::UndefinedObject(name.to_string()).into());
    };
    require_statistics_owner(kv, &object, fctx.effective_role())?;
    let ops = match action {
        ast::AlterStatisticsAction::RenameTo(new_name) => {
            crabka_pgcatalog::statistics::rename_ops(kv, &name, &name.sibling(new_name))?
        }
        ast::AlterStatisticsAction::SetSchema(schema) => crabka_pgcatalog::statistics::rename_ops(
            kv,
            &name,
            &RelationName::new(schema, name.name.clone()),
        )?,
        ast::AlterStatisticsAction::OwnerTo(owner) => {
            if !crabka_pgcatalog::role_exists(kv, owner)? {
                return Err(ExecError::UndefinedObject(format!(
                    "role \"{owner}\" does not exist"
                )));
            }
            object.owner.clone_from(owner);
            vec![crabka_pgcatalog::statistics::put_op(&object)]
        }
        ast::AlterStatisticsAction::SetStatistics(target) => {
            let target = target.unwrap_or(-1);
            object.target = i16::try_from(target).map_err(|_| {
                ExecError::Unsupported("statistics target must be between -1 and 10000".into())
            })?;
            if !(-1..=10_000).contains(&target) {
                return Err(ExecError::Unsupported(
                    "statistics target must be between -1 and 10000".into(),
                ));
            }
            vec![crabka_pgcatalog::statistics::put_op(&object)]
        }
    };
    Ok((command("ALTER STATISTICS"), ops))
}

pub(crate) fn drop(
    kv: &dyn Kv,
    names: &[ast::RelationRef],
    if_exists: bool,
    fctx: ForeignCtx<'_>,
) -> DdlResult {
    let mut ops = Vec::new();
    for written in names {
        let name = resolve_relation(kv, fctx.resolution, written, SchemaDisposition::Utility)?;
        match crabka_pgcatalog::statistics::get(kv, &name)? {
            Some(object) => {
                require_statistics_owner(kv, &object, fctx.effective_role())?;
                ops.extend(crabka_pgcatalog::statistics::drop_ops(kv, &name)?);
            }
            None if if_exists => {}
            None => {
                return Err(
                    crabka_pgcatalog::CatalogError::UndefinedObject(name.to_string()).into(),
                );
            }
        }
    }
    Ok((command("DROP STATISTICS"), ops))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgcatalog::{Column, GeneratedColumn, GeneratedKind, RelationName, Table};
    use crabka_pgparser::ast::{CreateStatistics, Expr, RelationRef, TableExpr};
    use crabka_pgtypes::ColumnType;

    use super::{definition, kinds};

    fn table() -> Table {
        Table {
            id: 42,
            owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
            name: RelationName::public("t"),
            columns: vec![
                Column::new("a", ColumnType::Int4),
                Column::new("b", ColumnType::Int4),
            ],
            sharded: false,
            row_security: false,
            force_row_security: false,
            sharding: None,
            foreign: None,
            materialized: None,
            checks: Vec::new(),
        }
    }

    fn stats(expressions: Vec<Expr>) -> CreateStatistics {
        CreateStatistics {
            name: RelationRef::bare("s"),
            if_not_exists: false,
            kinds: Vec::new(),
            expressions,
            from: TableExpr::Table {
                name: RelationRef::bare("t"),
                only: false,
                alias: None,
                columns: None,
                sample: None,
            },
        }
    }

    #[test]
    fn definition_keeps_attribute_numbers_and_expression_slots() {
        let parsed =
            crabka_pgparser::parse("CREATE STATISTICS s ON a, (b + 1) FROM t").expect("parse");
        let [crabka_pgparser::ast::Statement::CreateStatistics(parsed_stats)] = parsed.as_slice()
        else {
            panic!("stats");
        };
        assert!(definition(parsed_stats, &table()) == Ok((vec![1, 0], vec!["(b + 1)".into()])));
        assert!(kinds(&[], &[1, 2]).expect("default kinds") == ["d", "f", "m"]);
        assert!(kinds(&[], &[0]).expect("one expression kind") == ["e"]);
        assert!(kinds(&[], &[1, 0]).expect("mixed expression kinds") == ["d", "f", "m", "e"]);
        assert!(kinds(&["ndistinct".into()], &[0, 0]).expect("expression kind") == ["d", "e"]);
        assert!(
            definition(
                &stats(vec![Expr::Column {
                    table: None,
                    name: "a".into()
                }]),
                &table()
            )
            .is_err()
        );
    }

    #[test]
    fn virtual_columns_cannot_back_statistics() {
        let mut table = table();
        table.columns[1].generated = Some(GeneratedColumn {
            expr: "a + 1".into(),
            kind: GeneratedKind::Virtual,
        });
        assert!(
            definition(
                &stats(vec![
                    Expr::Column {
                        table: None,
                        name: "a".into()
                    },
                    Expr::Column {
                        table: None,
                        name: "b".into()
                    },
                ]),
                &table,
            )
            .is_err()
        );
        let parsed = crabka_pgparser::parse("CREATE STATISTICS s ON a, (b + 1) FROM t")
            .expect("parse expression statistics");
        let [crabka_pgparser::ast::Statement::CreateStatistics(parsed_stats)] = parsed.as_slice()
        else {
            panic!("stats");
        };
        assert!(definition(parsed_stats, &table).is_err());
    }

    #[test]
    fn columns_without_btree_opclasses_cannot_back_statistics() {
        let mut table = table();
        table.columns[1].ty = ColumnType::Xid;
        let Err(crate::error::ExecError::Unsupported(error)) = definition(
            &stats(vec![Expr::Column {
                table: None,
                name: "b".into(),
            }]),
            &table,
        ) else {
            panic!("xid statistics should be rejected");
        };
        assert!(
            error
                == "column \"b\" cannot be used in statistics because its type xid has no default btree operator class"
        );
    }
}
