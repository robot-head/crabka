use super::*;

pub(super) fn view_write_qualifier<'a>(
    name: &'a crabka_pgcatalog::RelationName,
    alias: Option<&'a str>,
) -> &'a str {
    alias.unwrap_or(&name.name)
}

pub(super) fn view_write_alias(stmt: &Statement) -> Option<&str> {
    match stmt {
        Statement::Insert { alias, .. }
        | Statement::Update { alias, .. }
        | Statement::Delete { alias, .. }
        | Statement::Merge { alias, .. } => alias.as_deref(),
        _ => None,
    }
}

pub(super) fn view_writes(stmt: &Statement) -> Vec<crate::viewwrite::ViewWrite> {
    use crabka_pgparser::ast::MergeAction;

    use crate::viewwrite::{ViewCommand, ViewWrite};

    match stmt {
        Statement::Insert { .. } => vec![ViewWrite::direct(ViewCommand::Insert)],
        Statement::Update { .. } => vec![ViewWrite::direct(ViewCommand::Update)],
        Statement::Delete { .. } => vec![ViewWrite::direct(ViewCommand::Delete)],
        Statement::Merge { clauses, .. } => {
            let mut writes = Vec::new();
            for clause in clauses {
                let command = match clause.action {
                    MergeAction::Insert { .. } => ViewCommand::Insert,
                    MergeAction::Update(_) => ViewCommand::Update,
                    MergeAction::Delete => ViewCommand::Delete,
                    MergeAction::DoNothing => continue,
                };
                let write = ViewWrite::merged(command);
                if !writes.contains(&write) {
                    writes.push(write);
                }
            }
            writes
        }
        _ => Vec::new(),
    }
}
