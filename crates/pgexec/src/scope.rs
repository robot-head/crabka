//! SP33: the resolution scope for a (possibly joined) relation. A `Scope` is the
//! ordered schema of a relation's combined row; `resolve` maps a (qualified or
//! bare) column reference to its flat index into that row. Replaces the
//! single-`crabka_pgcatalog::Table` column lookup that every prior slice used.

use crabka_pgcatalog::Table;
use crabka_pgtypes::ColumnType;

use crate::error::ExecError;

/// One column visible in a scope: its source qualifier (table name or alias;
/// `None` for a USING/NATURAL-coalesced column), its name, and its type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnBinding {
    pub(crate) qualifier: Option<String>,
    pub(crate) name: String,
    pub(crate) ty: ColumnType,
}

/// The ordered schema of a relation. Flat indices line up with the combined row.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Scope {
    pub(crate) columns: Vec<ColumnBinding>,
}

impl Scope {
    /// The empty scope (FROM-less SELECT): only constant expressions resolve.
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    /// A base table's scope: every column qualified by `qualifier` (the alias if
    /// present, else the table name).
    pub fn single(table: &Table, qualifier: &str) -> Self {
        Self {
            columns: table
                .columns
                .iter()
                .map(|c| ColumnBinding {
                    qualifier: Some(qualifier.to_string()),
                    name: c.name.clone(),
                    ty: c.ty,
                })
                .collect(),
        }
    }

    /// The combined row width; used by the join layer to size NULL-padded rows.
    pub fn width(&self) -> usize {
        self.columns.len()
    }

    /// The type of the column at `idx` (caller ensures `idx < width()`).
    pub fn ty_at(&self, idx: usize) -> ColumnType {
        self.columns[idx].ty
    }

    /// Resolve a column reference to its flat index. Unqualified: unique match by
    /// name (0 -> 42703, >1 -> 42702). Qualified `t.name`: `t` must be a qualifier in
    /// scope (else 42P01), then unique match by name under it (0 -> 42703, >1 -> 42702).
    pub fn resolve(&self, qualifier: Option<&str>, name: &str) -> Result<usize, ExecError> {
        if let Some(q) = qualifier
            && !self
                .columns
                .iter()
                .any(|c| c.qualifier.as_deref() == Some(q))
        {
            return Err(ExecError::MissingFromEntry(q.to_string()));
        }
        let mut found: Option<usize> = None;
        for (i, c) in self.columns.iter().enumerate() {
            let q_ok = qualifier.is_none_or(|q| c.qualifier.as_deref() == Some(q));
            if q_ok && c.name == name {
                if found.is_some() {
                    return Err(ExecError::AmbiguousColumn(name.to_string()));
                }
                found = Some(i);
            }
        }
        found.ok_or_else(|| ExecError::UndefinedColumn(name.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use crabka_pgcatalog::{Column, Table};

    use super::*;

    fn tbl(name: &str, cols: &[(&str, ColumnType)]) -> Table {
        Table {
            id: 1,
            name: name.to_string(),
            columns: cols.iter().map(|(n, t)| Column::new(*n, *t)).collect(),
            sharded: false,
            sharding: None,
            foreign: None,
        }
    }

    #[test]
    fn single_table_resolves_bare_and_qualified() {
        let t = tbl("t", &[("id", ColumnType::Int4), ("name", ColumnType::Text)]);
        let s = Scope::single(&t, "t");
        assert_eq!(s.resolve(None, "id"), Ok(0));
        assert_eq!(s.resolve(Some("t"), "name"), Ok(1));
    }

    #[test]
    #[allow(non_snake_case)]
    fn unknown_column_is_42703_and_unknown_qualifier_is_42P01() {
        let t = tbl("t", &[("id", ColumnType::Int4)]);
        let s = Scope::single(&t, "t");
        assert_eq!(
            s.resolve(None, "nope"),
            Err(ExecError::UndefinedColumn("nope".into()))
        );
        assert_eq!(
            s.resolve(Some("x"), "id"),
            Err(ExecError::MissingFromEntry("x".into()))
        );
    }

    #[test]
    fn duplicate_bare_name_across_tables_is_ambiguous_42702() {
        // Two tables each with `id`; a bare `id` is ambiguous, a qualified one is not.
        let a = tbl("a", &[("id", ColumnType::Int4)]);
        let b = tbl("b", &[("id", ColumnType::Int4)]);
        let mut s = Scope::single(&a, "a");
        s.columns.extend(Scope::single(&b, "b").columns);
        assert_eq!(
            s.resolve(None, "id"),
            Err(ExecError::AmbiguousColumn("id".into()))
        );
        assert_eq!(s.resolve(Some("a"), "id"), Ok(0));
        assert_eq!(s.resolve(Some("b"), "id"), Ok(1));
    }
}
