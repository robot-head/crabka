use super::*;

pub(super) fn stored_default_value(written: Datum, coerced: Datum) -> Datum {
    match (&written, &coerced) {
        (Datum::BitString(_), Datum::BitString(_)) if written == coerced => written,
        _ => coerced,
    }
}

pub(super) fn column_from_ast(
    table_name: &crabka_pgcatalog::RelationName,
    column: &crabka_pgparser::ast::ColumnDef,
    ctx: &crate::clock::EvalCtx,
    serial_sequences: &mut Vec<(crabka_pgcatalog::RelationName, Sequence)>,
    primary_key_columns: &HashSet<&str>,
) -> Result<Column, ExecError> {
    let mut catalog_column = Column::new(column.name.clone(), column.ty);
    if let Some(collation) = &column.collation {
        crate::eval::require_collatable(column.ty)?;
        catalog_column.collation = Some(collation.clone());
    }
    if primary_key_columns.contains(column.name.as_str()) {
        catalog_column.not_null = true;
    }
    if column.serial.is_some() {
        let sequence_name = table_name.sibling(format!("{}_{}_seq", table_name.name, column.name));
        catalog_column.not_null = true;
        catalog_column.default = Some(ColumnDefault::NextVal(sequence_name.to_string()));
        serial_sequences.push((
            sequence_name,
            Sequence::new(1, 1, None, None, Some(1), false),
        ));
    }
    for constraint in &column.constraints {
        match &constraint.kind {
            crabka_pgparser::ast::ColumnConstraintKind::NotNull => catalog_column.not_null = true,
            crabka_pgparser::ast::ColumnConstraintKind::Null => catalog_column.not_null = false,
            crabka_pgparser::ast::ColumnConstraintKind::Default(expr) => {
                catalog_column.default = Some(default_from_expr(expr, column.ty, ctx)?);
            }
            crabka_pgparser::ast::ColumnConstraintKind::PrimaryKey => {
                catalog_column.not_null = true;
            }
            crabka_pgparser::ast::ColumnConstraintKind::Unique { nulls_not_distinct } => {
                if *nulls_not_distinct {
                    return Err(ExecError::Unsupported(
                        "UNIQUE NULLS NOT DISTINCT is not supported: unique indexes use \
                         PostgreSQL's default NULLS DISTINCT semantics"
                            .into(),
                    ));
                }
            }
            crabka_pgparser::ast::ColumnConstraintKind::Identity(spec) => {
                let sequence_name =
                    table_name.sibling(format!("{}_{}_seq", table_name.name, column.name));
                catalog_column.not_null = true;
                catalog_column.identity = Some(if spec.always {
                    crabka_pgcatalog::IdentityKind::Always
                } else {
                    crabka_pgcatalog::IdentityKind::ByDefault
                });
                catalog_column.default = Some(ColumnDefault::NextVal(sequence_name.to_string()));
                serial_sequences.push((sequence_name, sequence_from_options(&spec.options)));
            }
            crabka_pgparser::ast::ColumnConstraintKind::Generated(spec) => {
                if catalog_column.generated.is_some() {
                    return Err(ExecError::Syntax(format!(
                        "multiple generation clauses specified for column \"{}\" of table \"{}\"",
                        column.name, table_name.name
                    )));
                }
                catalog_column.generated = Some(crabka_pgcatalog::GeneratedColumn {
                    expr: spec.predicate.text.clone(),
                    kind: match spec.kind {
                        crabka_pgparser::ast::GeneratedKind::Stored => {
                            crabka_pgcatalog::GeneratedKind::Stored
                        }
                        crabka_pgparser::ast::GeneratedKind::Virtual => {
                            crabka_pgcatalog::GeneratedKind::Virtual
                        }
                    },
                });
            }
            crabka_pgparser::ast::ColumnConstraintKind::Check(_)
            | crabka_pgparser::ast::ColumnConstraintKind::References(_) => {}
        }
    }
    if catalog_column.generated.is_some() {
        if catalog_column.identity.is_some() {
            return Err(ExecError::Syntax(format!(
                "both identity and generation expression specified for column \"{}\" of table \
                 \"{}\"",
                column.name, table_name.name
            )));
        }
        if catalog_column.default.is_some() {
            return Err(ExecError::Syntax(format!(
                "both default and generation expression specified for column \"{}\" of table \
                 \"{}\"",
                column.name, table_name.name
            )));
        }
    }
    Ok(catalog_column)
}
