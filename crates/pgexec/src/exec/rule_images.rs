//! OLD/NEW image substitution for rewrite-rule actions.

use super::*;

pub(super) fn bind_rule_expr(
    expr: &Expr,
    table: &Table,
    old: Option<&[Datum]>,
    new: Option<&[Datum]>,
) -> Result<Expr, ExecError> {
    let mut error = None;
    let bound = crate::viewwrite::map_expr(expr, false, &mut |node, _| match rule_image_expr(
        node, table, old, new,
    ) {
        Ok(replacement) => replacement,
        Err(err) => {
            error = Some(err);
            None
        }
    });
    match error {
        Some(error) => Err(error),
        None => Ok(bound),
    }
}

pub(super) fn rule_image_expr(
    node: &Expr,
    table: &Table,
    old: Option<&[Datum]>,
    new: Option<&[Datum]>,
) -> Result<Option<Expr>, ExecError> {
    let Expr::Column {
        table: Some(qualifier),
        name,
    } = node
    else {
        return Ok(None);
    };
    let values = match qualifier.as_str() {
        "old" => old,
        "new" => new,
        _ => return Ok(None),
    };
    let values = values.ok_or_else(|| {
        ExecError::Unsupported(format!(
            "{qualifier} is not available for this rewrite rule event"
        ))
    })?;
    if name == "*" {
        return Err(ExecError::Unsupported(
            "OLD.* and NEW.* are only supported as INSERT VALUES items".into(),
        ));
    }
    let index = table
        .column_index(name)
        .ok_or_else(|| ExecError::UndefinedTableColumn {
            column: name.clone(),
            table: table.name.to_string(),
        })?;
    Ok(Some(Expr::Const {
        value: values[index].clone(),
        ty: table.columns[index].ty,
    }))
}
