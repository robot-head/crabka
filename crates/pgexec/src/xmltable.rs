//! SQL/XML `XMLTABLE` FROM-item support.

use crabka_pgparser::ast::{XmlTable, XmlTableColumn, XmlTableValueColumn};
use crabka_pgtypes::{ColumnType, Datum, TypeError, xml};

use crate::{
    clock::EvalCtx,
    error::ExecError,
    join::Relation,
    scope::{ColumnBinding, Exposure, Scope},
};

/// The `Describe`-time shape of an `XMLTABLE` item.
pub(crate) fn from_item_schema(table: &XmlTable) -> Result<Relation, ExecError> {
    validate_column_aliases(table)?;
    relation(table, Vec::new())
}

/// Run an `XMLTABLE` item.
pub(crate) fn from_item(table: &XmlTable, ctx: &EvalCtx) -> Result<Relation, ExecError> {
    validate_column_aliases(table)?;
    if table.namespaces.iter().any(|(prefix, _)| prefix.is_none()) {
        return Err(ExecError::Unsupported(
            "DEFAULT namespace is not supported".into(),
        ));
    }
    let document = crate::xml_fn::as_xml(
        &crate::eval::eval(&table.document, &Scope::empty(), &[], ctx)?,
        "XMLTABLE",
    )?;
    let row_path = expression_text(&table.row_path, ctx)?;
    let namespaces = table
        .namespaces
        .iter()
        .map(|(prefix, uri)| {
            Ok((
                prefix.clone().unwrap_or_default(),
                expression_text(uri, ctx)?,
            ))
        })
        .collect::<Result<Vec<_>, ExecError>>()?;
    let bindings = namespaces
        .iter()
        .map(|(prefix, uri)| (prefix.as_str(), uri.as_str()))
        .collect::<Vec<_>>();
    let rows = xpath(&document, &row_path, &bindings)?
        .into_iter()
        .enumerate()
        .map(|(ordinal, item)| row(table, ctx, &item, ordinal, &bindings))
        .collect::<Result<Vec<_>, _>>()?;
    relation(table, rows)
}

/// `XMLTABLE` is implicitly lateral when one of its expressions reads a prior
/// FROM item, like `JSON_TABLE` and ordinary table functions.
pub(crate) fn references_scope(table: &XmlTable, outer: &Scope) -> bool {
    table.lateral
        || table
            .exprs()
            .into_iter()
            .any(|expr| crate::exec::expr_references_scope(expr, outer))
}

fn relation(
    table: &XmlTable,
    rows: Vec<Vec<crabka_pgtypes::Datum>>,
) -> Result<Relation, ExecError> {
    let mut columns = table
        .columns
        .iter()
        .map(|column| match column {
            XmlTableColumn::Ordinality { name } => ColumnBinding {
                exposure: Exposure::Output,
                qualifier: None,
                name: name.clone(),
                ty: crabka_pgtypes::ColumnType::Int4,
            },
            XmlTableColumn::Value(column) => ColumnBinding {
                exposure: Exposure::Output,
                qualifier: None,
                name: column.name.clone(),
                ty: column.ty,
            },
        })
        .collect::<Vec<_>>();
    if let Some(names) = &table.column_aliases {
        for (column, name) in columns.iter_mut().zip(names) {
            column.name.clone_from(name);
        }
    }
    let qualifier = table.alias.clone().unwrap_or_else(|| "xmltable".into());
    for column in &mut columns {
        column.qualifier = Some(qualifier.clone());
    }
    Ok(Relation {
        scope: Scope {
            columns,
            ..Default::default()
        },
        rows,
    })
}

fn validate_column_aliases(table: &XmlTable) -> Result<(), ExecError> {
    if let Some(names) = &table.column_aliases
        && names.len() > table.columns.len()
    {
        return Err(TypeError::Coded {
            sqlstate: "42601",
            message: format!(
                "XMLTABLE function has {} columns available but {} columns specified",
                table.columns.len(),
                names.len()
            ),
        }
        .into());
    }
    Ok(())
}

fn row(
    table: &XmlTable,
    ctx: &EvalCtx,
    item: &str,
    ordinal: usize,
    namespaces: &[(&str, &str)],
) -> Result<Vec<Datum>, ExecError> {
    table
        .columns
        .iter()
        .map(|column| match column {
            XmlTableColumn::Ordinality { .. } => Ok(Datum::Int4(
                i32::try_from(ordinal.saturating_add(1)).unwrap_or(i32::MAX),
            )),
            XmlTableColumn::Value(column) => value_column(column, ctx, item, namespaces),
        })
        .collect()
}

fn value_column(
    column: &XmlTableValueColumn,
    ctx: &EvalCtx,
    item: &str,
    namespaces: &[(&str, &str)],
) -> Result<Datum, ExecError> {
    let path = match &column.path {
        Some(path) => expression_text(path, ctx)?,
        None => column.name.clone(),
    };
    let path = relative_path(&path);
    let selected = xpath(item, &path, namespaces)?;
    let value = match selected.as_slice() {
        [] => match &column.default {
            Some(default) => crate::eval::eval(default, &Scope::empty(), &[], ctx)?,
            None => Datum::Null,
        },
        _ if column.ty.storage_type() == ColumnType::Xml => Datum::Xml(selected.concat()),
        [selected] => crate::eval::cast_value_in_at(
            &Datum::Text(xpath_text(selected)?),
            column.ty,
            ctx.output_style(),
            ctx.now,
        )?,
        _ => {
            return Err(ExecError::Unsupported(
                "more than one value returned by column XPath expression".into(),
            ));
        }
    };
    if column.not_null && value.is_null() {
        return Err(TypeError::Coded {
            sqlstate: "22004",
            message: format!("null is not allowed in column \"{}\"", column.name),
        }
        .into());
    }
    Ok(value)
}

fn xpath(
    document: &str,
    path: &str,
    namespaces: &[(&str, &str)],
) -> Result<Vec<String>, ExecError> {
    let values = if namespaces.is_empty() {
        xml::xpath(document, path)
    } else {
        xml::xpath_with_namespaces(document, path, namespaces)
    };
    values.map_err(ExecError::from)
}

fn expression_text(expr: &crabka_pgparser::ast::Expr, ctx: &EvalCtx) -> Result<String, ExecError> {
    Ok(crate::xml_fn::text_of(
        &crate::eval::eval(expr, &Scope::empty(), &[], ctx)?,
        ctx,
    ))
}

fn xpath_text(value: &str) -> Result<String, ExecError> {
    let document = if value.starts_with('<') {
        value.to_string()
    } else {
        format!("<xmltable>{value}</xmltable>")
    };
    xml::text_value(&document).map_err(ExecError::from)
}

/// XMLTABLE column paths are relative to the row item, while `xpath` paths are
/// rooted in its document. Prefix ordinary relative paths with that root.
fn relative_path(path: &str) -> String {
    if path == "." {
        "/*".into()
    } else if let Some(path) = path.strip_prefix("./") {
        format!("/*/{path}")
    } else if path.starts_with('/')
        || path.contains("namespace::")
        || path.starts_with(['\'', '"'])
        || path.starts_with(". = ")
        || path.starts_with("string-length(")
    {
        path.into()
    } else {
        format!("/*/{path}")
    }
}
