//! DDL support for durable rewrite rules.

use crabka_pgcatalog::{RelationName, rule};
use crabka_pgkv::{Kv, WriteOp};
use crabka_pgparser::ast as parsed;
use crabka_pgwire::engine::QueryResult;

use crate::error::ExecError;

fn command(tag: &str) -> QueryResult {
    QueryResult::Command { tag: tag.into() }
}

fn event(event: parsed::RuleEvent) -> rule::RuleEvent {
    match event {
        parsed::RuleEvent::Select => rule::RuleEvent::Select,
        parsed::RuleEvent::Insert => rule::RuleEvent::Insert,
        parsed::RuleEvent::Update => rule::RuleEvent::Update,
        parsed::RuleEvent::Delete => rule::RuleEvent::Delete,
    }
}

fn relation_id(kv: &dyn Kv, name: &RelationName) -> Result<crabka_pgcatalog::TableId, ExecError> {
    if let Ok(table) = crabka_pgcatalog::get_table(kv, name) {
        return Ok(table.id);
    }
    crabka_pgcatalog::get_view(kv, name)?;
    crate::catalog_rel::view_oids(kv)?
        .get(name)
        .copied()
        .and_then(|oid| u32::try_from(oid).ok())
        .ok_or_else(|| ExecError::UndefinedObject(format!("relation \"{name}\" does not exist")))
}

fn reject_rule_images_in_ctes(action: &parsed::RuleAction) -> Result<(), ExecError> {
    let parsed::RuleAction::Statements(actions) = action else {
        return Ok(());
    };
    for action in actions {
        let Some(with) = crate::exec::statement_with_clause(action) else {
            continue;
        };
        for cte in &with.ctes {
            let mut image = None;
            match &cte.body {
                parsed::CteBody::Query(query) => rule_image_in_query(query, &mut image),
                parsed::CteBody::Dml(statement) => rule_image_in_statement(statement, &mut image),
            }
            if let Some(image) = image {
                return Err(ExecError::Syntax(format!(
                    "cannot refer to {image} within WITH query"
                )));
            }
        }
    }
    Ok(())
}

fn reject_rule_images_in_locking_clause(action: &parsed::RuleAction) -> Result<(), ExecError> {
    let parsed::RuleAction::Statements(actions) = action else {
        return Ok(());
    };
    for action in actions {
        let parsed::Statement::Query(query) = action else {
            continue;
        };
        let Some(locking) = &query.locking else {
            continue;
        };
        if let Some(image) = locking
            .of
            .iter()
            .find(|name| matches!(name.to_ascii_lowercase().as_str(), "old" | "new"))
        {
            return Err(ExecError::Remote(crabka_pgwire::error::PgError::error(
                "42P01",
                format!("relation \"{image}\" in FOR UPDATE clause not found in FROM clause"),
            )));
        }
    }
    Ok(())
}

/// `VALUES` has no range table, so an unqualified name there cannot name a
/// rule's OLD or NEW row image. PostgreSQL rejects it while creating the rule.
fn reject_unqualified_columns_in_rule_values(
    action: &parsed::RuleAction,
    _source: &RelationName,
) -> Result<(), ExecError> {
    let parsed::RuleAction::Statements(actions) = action else {
        return Ok(());
    };
    for action in actions {
        let parsed::Statement::Insert {
            source: parsed::InsertSource::Values(rows),
            ..
        } = action
        else {
            continue;
        };
        for expr in rows.iter().flatten() {
            let mut column = None;
            crate::viewwrite::map_expr(expr, false, &mut |node, in_subquery| {
                if !in_subquery && let parsed::Expr::Column { table: None, name } = node {
                    column = Some(name.clone());
                }
                None
            });
            if let Some(column) = column {
                return Err(ExecError::Remote(
                    crabka_pgwire::error::PgError::error(
                        "42703",
                        format!("column \"{column}\" does not exist"),
                    )
                    .with_detail(format!(
                        "There are columns named \"{column}\", but they are in tables that cannot be referenced from this part of the query."
                    ))
                    .with_hint("Try using a table-qualified name."),
                ));
            }
        }
    }
    Ok(())
}

fn rule_image_in_expr(expr: &parsed::Expr, image: &mut Option<String>) {
    crate::viewwrite::map_expr(expr, false, &mut |expr, _| {
        if let parsed::Expr::Column {
            table: Some(qualifier),
            ..
        } = expr
            && matches!(qualifier.as_str(), "old" | "new")
        {
            *image = Some(qualifier.to_ascii_uppercase());
        }
        None
    });
}

fn rule_image_in_query(query: &parsed::QueryExpr, image: &mut Option<String>) {
    let mut query = query.clone();
    crate::viewwrite::map_query(&mut query, &mut |expr, _| {
        if let parsed::Expr::Column {
            table: Some(qualifier),
            ..
        } = expr
            && matches!(qualifier.as_str(), "old" | "new")
        {
            *image = Some(qualifier.to_ascii_uppercase());
        }
        None
    });
    rule_image_wildcard_in_query(&query, image);
}

fn rule_image_in_statement(statement: &parsed::Statement, image: &mut Option<String>) {
    match statement {
        parsed::Statement::Insert {
            source,
            on_conflict,
            returning,
            ..
        } => {
            match source {
                parsed::InsertSource::Query(query) => rule_image_in_query(query, image),
                parsed::InsertSource::Values(rows) => {
                    for expr in rows.iter().flatten() {
                        rule_image_in_expr(expr, image);
                    }
                }
                parsed::InsertSource::DefaultValues => {}
            }
            if let Some(conflict) = on_conflict {
                if let parsed::OnConflictTarget::Columns {
                    index_predicate: Some(predicate),
                    ..
                } = &conflict.target
                {
                    rule_image_in_expr(predicate, image);
                }
                if let parsed::OnConflictAction::DoUpdate {
                    assignments,
                    filter,
                } = &conflict.action
                {
                    for (_, expr) in assignments {
                        rule_image_in_expr(expr, image);
                    }
                    if let Some(filter) = filter {
                        rule_image_in_expr(filter, image);
                    }
                }
            }
            rule_image_in_returning(returning.as_ref(), image);
        }
        parsed::Statement::Update {
            assignments,
            from,
            filter,
            returning,
            ..
        } => {
            for item in from {
                rule_image_in_table_expr(item, image);
            }
            for assignment in assignments {
                match &assignment.value {
                    parsed::AssignmentValue::Expr(expr) => rule_image_in_expr(expr, image),
                    parsed::AssignmentValue::Row(exprs) => {
                        for expr in exprs {
                            rule_image_in_expr(expr, image);
                        }
                    }
                    parsed::AssignmentValue::Subquery(query) => rule_image_in_query(query, image),
                }
            }
            if let Some(filter) = filter {
                rule_image_in_expr(filter, image);
            }
            rule_image_in_returning(returning.as_ref(), image);
        }
        parsed::Statement::Delete {
            using,
            filter,
            returning,
            ..
        } => {
            for item in using {
                rule_image_in_table_expr(item, image);
            }
            if let Some(filter) = filter {
                rule_image_in_expr(filter, image);
            }
            rule_image_in_returning(returning.as_ref(), image);
        }
        _ => {}
    }
}

fn rule_image_in_table_expr(item: &parsed::TableExpr, image: &mut Option<String>) {
    match item {
        parsed::TableExpr::Derived { subquery, .. } => rule_image_in_query(subquery, image),
        parsed::TableExpr::Join {
            left,
            right,
            constraint,
            ..
        } => {
            rule_image_in_table_expr(left, image);
            rule_image_in_table_expr(right, image);
            if let parsed::JoinConstraint::On(expr) = constraint {
                rule_image_in_expr(expr, image);
            }
        }
        parsed::TableExpr::Function { functions, .. } => {
            for function in functions {
                for argument in function.arguments() {
                    rule_image_in_expr(argument, image);
                }
            }
        }
        parsed::TableExpr::JsonTable(table) => {
            for expr in table.exprs() {
                rule_image_in_expr(expr, image);
            }
        }
        parsed::TableExpr::XmlTable(table) => {
            for expr in table.exprs() {
                rule_image_in_expr(expr, image);
            }
        }
        parsed::TableExpr::Table { .. } => {}
    }
}

fn rule_image_in_returning(returning: Option<&parsed::Returning>, image: &mut Option<String>) {
    if let Some(returning) = returning {
        for item in &returning.items {
            if let parsed::SelectItem::Expr { expr, .. } = item {
                rule_image_in_expr(expr, image);
            }
            if let parsed::SelectItem::QualifiedWildcard(qualifier) = item
                && matches!(qualifier.as_str(), "old" | "new")
            {
                *image = Some(qualifier.to_ascii_uppercase());
            }
        }
    }
}

fn rule_image_wildcard_in_query(query: &parsed::QueryExpr, image: &mut Option<String>) {
    if let Some(with) = &query.with {
        for cte in &with.ctes {
            if let parsed::CteBody::Query(query) = &cte.body {
                rule_image_wildcard_in_query(query, image);
            }
        }
    }
    rule_image_wildcard_in_set_expr(&query.body, image);
}

fn rule_image_wildcard_in_set_expr(body: &parsed::SetExpr, image: &mut Option<String>) {
    match body {
        parsed::SetExpr::Query(parsed::QueryBody::Select(select)) => {
            for item in &select.projection {
                if let parsed::SelectItem::QualifiedWildcard(qualifier) = item
                    && matches!(qualifier.as_str(), "old" | "new")
                {
                    *image = Some(qualifier.to_ascii_uppercase());
                }
            }
        }
        parsed::SetExpr::Query(parsed::QueryBody::Nested(query)) => {
            rule_image_wildcard_in_query(query, image);
        }
        parsed::SetExpr::SetOp { left, right, .. } => {
            rule_image_wildcard_in_set_expr(left, image);
            rule_image_wildcard_in_set_expr(right, image);
        }
        parsed::SetExpr::Query(parsed::QueryBody::Values(_)) => {}
    }
}

pub(crate) fn create(
    kv: &dyn Kv,
    stmt: &parsed::CreateRule,
    table_name: RelationName,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    reject_rule_images_in_ctes(&stmt.action)?;
    reject_rule_images_in_locking_clause(&stmt.action)?;
    reject_unqualified_columns_in_rule_values(&stmt.action, &table_name)?;
    if stmt.event == parsed::RuleEvent::Select {
        if crabka_pgcatalog::get_view(kv, &table_name).is_ok() {
            return Err(ExecError::ObjectNotInPrerequisiteState(format!(
                "\"{}\" is already a view",
                table_name.name
            )));
        }
        if crabka_pgcatalog::get_table(kv, &table_name).is_ok() {
            let kind = if crate::partition::is_partitioned(kv, &table_name)? {
                "partitioned tables"
            } else {
                "tables"
            };
            return Err(ExecError::Remote(
                crabka_pgwire::error::PgError::error(
                    "42809",
                    format!(
                        "relation \"{}\" cannot have ON SELECT rules",
                        table_name.name
                    ),
                )
                .with_detail(format!("This operation is not supported for {kind}.")),
            ));
        }
    }
    let table_id = relation_id(kv, &table_name)?;
    let existing = rule::get_rule(kv, table_id, &stmt.name)?;
    if existing.is_some() && !stmt.or_replace {
        return Err(ExecError::DuplicateObject(format!(
            "rule \"{}\" for relation \"{}\" already exists",
            stmt.name, table_name
        )));
    }
    let stored = rule::Rule {
        oid: existing.as_ref().map_or(0, |rule| rule.oid),
        name: stmt.name.clone(),
        table_id,
        table: table_name,
        event: event(stmt.event),
        condition: stmt
            .condition
            .as_ref()
            .map(|condition| condition.source.clone()),
        instead: stmt.instead,
        enabled: existing
            .as_ref()
            .map_or(crabka_pgcatalog::trigger::TriggerEnabled::Origin, |rule| {
                rule.enabled
            }),
        action: stmt.action_source.clone(),
    };
    Ok((command("CREATE RULE"), rule::put_rule_ops(kv, &stored)?))
}

pub(crate) fn set_enabled(
    kv: &dyn Kv,
    table_name: RelationName,
    name: &str,
    mode: parsed::TriggerEnableMode,
) -> Result<Vec<WriteOp>, ExecError> {
    let table_id = relation_id(kv, &table_name)?;
    let mut stored = rule::get_rule(kv, table_id, name)?.ok_or_else(|| {
        ExecError::UndefinedObject(format!(
            "rule \"{name}\" for relation \"{table_name}\" does not exist"
        ))
    })?;
    stored.enabled = match mode {
        parsed::TriggerEnableMode::Origin => crabka_pgcatalog::trigger::TriggerEnabled::Origin,
        parsed::TriggerEnableMode::Replica => crabka_pgcatalog::trigger::TriggerEnabled::Replica,
        parsed::TriggerEnableMode::Always => crabka_pgcatalog::trigger::TriggerEnabled::Always,
        parsed::TriggerEnableMode::Disabled => crabka_pgcatalog::trigger::TriggerEnabled::Disabled,
    };
    Ok(rule::put_rule_ops(kv, &stored)?)
}

pub(crate) fn alter(
    kv: &dyn Kv,
    name: &str,
    table_name: RelationName,
    action: &parsed::AlterRuleAction,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    if name == "_RETURN" && crabka_pgcatalog::get_view(kv, &table_name).is_ok() {
        return Err(ExecError::InvalidObjectDefinition(
            "renaming an ON SELECT rule is not allowed".into(),
        ));
    }
    let table_id = relation_id(kv, &table_name)?;
    let mut stored = rule::get_rule(kv, table_id, name)?.ok_or_else(|| {
        ExecError::UndefinedObject(format!(
            "rule \"{name}\" for relation \"{table_name}\" does not exist"
        ))
    })?;
    match action {
        parsed::AlterRuleAction::RenameTo(new_name) => {
            let return_rule = new_name.eq_ignore_ascii_case("_RETURN")
                && crabka_pgcatalog::get_view(kv, &table_name).is_ok();
            if return_rule || rule::get_rule(kv, table_id, new_name)?.is_some() {
                let new_name = return_rule.then_some("_RETURN").unwrap_or(new_name);
                return Err(ExecError::DuplicateObject(format!(
                    "rule \"{new_name}\" for relation \"{table_name}\" already exists"
                )));
            }
            let mut ops = rule::drop_rule_ops(table_id, name);
            stored.name = new_name.clone();
            ops.extend(rule::put_rule_ops(kv, &stored)?);
            Ok((command("ALTER RULE"), ops))
        }
    }
}

pub(crate) fn drop(
    kv: &dyn Kv,
    name: &str,
    table_name: RelationName,
    if_exists: bool,
) -> Result<(QueryResult, Vec<WriteOp>), ExecError> {
    if name == "_RETURN" && crabka_pgcatalog::get_view(kv, &table_name).is_ok() {
        return Err(ExecError::Remote(
            crabka_pgwire::error::PgError::error(
                "2BP01",
                format!(
                    "cannot drop rule _RETURN on view {} because view {} requires it",
                    table_name.name, table_name.name
                ),
            )
            .with_hint(format!("You can drop view {} instead.", table_name.name)),
        ));
    }
    let table_id = relation_id(kv, &table_name)?;
    let existing = rule::get_rule(kv, table_id, name)?;
    if existing.is_none() {
        if if_exists {
            return Ok((command("DROP RULE"), Vec::new()));
        }
        return Err(ExecError::UndefinedObject(format!(
            "rule \"{name}\" for relation \"{table_name}\" does not exist"
        )));
    }
    let oid = existing.expect("checked above").oid.to_string();
    let mut ops = rule::drop_rule_ops(table_id, name);
    ops.push(crabka_pgcatalog::set_comment_op(
        "rule",
        crabka_pgcatalog::CommentObject::Named(&oid),
        None,
    ));
    Ok((command("DROP RULE"), ops))
}
