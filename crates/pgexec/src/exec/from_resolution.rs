//! FROM-clause lateral and inaccessible-reference diagnostics.

use super::*;

#[derive(Clone, Copy)]
pub(crate) enum OuterReference {
    /// A sibling FROM item of the same query level. `LATERAL` would bring it
    /// into view, so `PostgreSQL` offers that as the remedy.
    Sibling,
    /// The relation an `UPDATE`/`DELETE` is targeting, named from a `FROM` or
    /// `USING` item that was not written `LATERAL`. No remedy exists.
    Target,
    /// The same target, named from an item that *was* written `LATERAL`. The
    /// name is then found and rejected rather than never found, which in
    /// `PostgreSQL` is a different check and a differently shaped message.
    LateralTarget,
}

/// Was `LATERAL` written on this FROM item?
///
/// Only the keyword counts, not the implicit laterality of a function item: it
/// is what `PostgreSQL` uses to decide whether the target relation's name is
/// looked up at all, and so which of the two prohibitions reports it.
pub(crate) fn item_is_lateral(te: &crabka_pgparser::ast::TableExpr) -> bool {
    use crabka_pgparser::ast::TableExpr;
    match te {
        TableExpr::Derived { lateral, .. } | TableExpr::Function { lateral, .. } => *lateral,
        TableExpr::JsonTable(table) => table.lateral,
        TableExpr::XmlTable(table) => table.lateral,
        TableExpr::Table { .. } => false,
        TableExpr::Join { left, right, .. } => item_is_lateral(left) || item_is_lateral(right),
    }
}

impl OuterReference {
    fn note(self) -> crate::error::FromEntryNote {
        use crate::error::FromEntryNote;
        match self {
            Self::Sibling => FromEntryNote::MarkSubqueryLateral,
            Self::Target => FromEntryNote::TargetRelation,
            Self::LateralTarget => FromEntryNote::LateralTargetRelation,
        }
    }
}

/// The qualifier of the one visible column in `scope` named `name`.
///
/// `None` when no column or more than one carries the name: `PostgreSQL` names a
/// single inaccessible match and says "there are columns named …" for several,
/// and the several case is not one this engine reaches yet.
pub(crate) fn sole_owner(scope: &Scope, name: &str) -> Option<String> {
    let mut owner = None;
    for column in &scope.columns {
        if column.name != name {
            continue;
        }
        // A USING/NATURAL-coalesced column belongs to the join rather than to
        // one table, so there is no name to put in the explanation.
        let qualifier = column.qualifier.as_deref()?;
        if qualifier.starts_with('$') {
            continue;
        }
        if owner.is_some() {
            return None;
        }
        owner = Some(qualifier.to_owned());
    }
    owner
}

/// Re-word a resolution failure that `outer` explains.
///
/// gres resolves a FROM item against its own scope alone, so a reference to an
/// entry one query level out fails with the bare "missing FROM-clause entry" or
/// "column does not exist". `PostgreSQL` searches the whole range table before
/// reporting and says so when it finds the name somewhere the reference cannot
/// see. Only the error already on its way out is rewritten — a name `outer` does
/// not supply passes through untouched.
pub(crate) fn explain_outer_reference(
    error: ExecError,
    outer: &Scope,
    kind: OuterReference,
) -> ExecError {
    match error {
        ExecError::MissingFromEntry(table)
            if outer
                .columns
                .iter()
                .any(|column| column.qualifier.as_deref() == Some(table.as_str())) =>
        {
            ExecError::InvalidFromEntry {
                table,
                note: kind.note(),
            }
        }
        ExecError::UndefinedColumn(column) => match sole_owner(outer, &column) {
            // Written `LATERAL`, the target relation's name does resolve, and
            // what rejects it names the relation rather than the column.
            Some(table) if matches!(kind, OuterReference::LateralTarget) => {
                ExecError::InvalidFromEntry {
                    table,
                    note: kind.note(),
                }
            }
            Some(table) => ExecError::InaccessibleColumn {
                column,
                table,
                lateral_would_help: matches!(kind, OuterReference::Sibling),
            },
            None => ExecError::UndefinedColumn(column),
        },
        other => other,
    }
}
