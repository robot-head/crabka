//! SP33: the resolution scope for a relation, which can be a joined relation.
//!
//! A `Scope` is the ordered schema of a relation's combined row. `resolve` maps
//! a column reference, qualified or bare, to its flat index into that row. This
//! replaces the single-`crabka_pgcatalog::Table` column lookup that every prior
//! slice used.

use crabka_pgcatalog::Table;
use crabka_pgtypes::ColumnType;

use crate::error::ExecError;

/// One column visible in a scope: its source qualifier, its name, and its type.
///
/// The qualifier is a table name or an alias. It is `None` for a
/// USING/NATURAL-coalesced column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnBinding {
    pub(crate) qualifier: Option<String>,
    pub(crate) name: String,
    pub(crate) ty: ColumnType,
}

/// The qualifier of a positional column reference, where `$pos.3` is "the
/// column at index 3", whatever its name is.
///
/// `PostgreSQL` expands `*` into positional `Var` nodes, so `SELECT *` works
/// over a relation whose column names repeat. For example,
/// `ROWS FROM (generate_series(1,3), generate_series(1,2))` has two columns
/// named `generate_series`. A bare reference to one of those names is still
/// `42702`. A `$` cannot begin an unquoted identifier, so no user relation can
/// collide with this qualifier.
pub(crate) const POSITION_QUALIFIER: &str = "$pos";

/// The ordered schema of a relation. Flat indices match the combined row.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Scope {
    pub(crate) columns: Vec<ColumnBinding>,
}

impl Scope {
    /// The empty scope, for a FROM-less SELECT. Only constant expressions
    /// resolve.
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    /// A base table's scope, where `qualifier` qualifies every column.
    ///
    /// `qualifier` is the alias when there is one, and the table name when
    /// there is not.
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

    /// The scope for an `INSERT … ON CONFLICT DO UPDATE` assignment or filter.
    ///
    /// The scope is the target table's columns qualified by the table name,
    /// followed by the same columns qualified as `excluded`. Expressions
    /// evaluate against the concatenation of the conflicting stored row and the
    /// proposed row, so the order matters. Target columns occupy `0..width`,
    /// and `excluded` columns occupy the rest.
    ///
    /// Every column name appears twice, so a bare unqualified reference is
    /// ambiguous, which is 42702. That is PostgreSQL's behavior.
    /// `DO UPDATE SET v = v + 1` is an error there too, and the reference must
    /// be written `t.v` or `excluded.v`.
    pub fn insert_conflict(table: &Table) -> Self {
        // The qualifier is the relation's own name, never its schema-qualified
        // spelling: `INSERT INTO s.t … ON CONFLICT DO UPDATE SET v = t.v` is how
        // PostgreSQL binds the target, because the range table entry an INSERT
        // adds is aliased to the bare relation name.
        let mut scope = Self::single(table, &table.name.name);
        scope
            .columns
            .extend(Self::single(table, "excluded").columns);
        scope
    }

    /// The combined row width. The join layer uses it to size NULL-padded
    /// rows.
    pub fn width(&self) -> usize {
        self.columns.len()
    }

    /// The type of the column at `idx`. The caller must make sure that
    /// `idx < width()`.
    pub fn ty_at(&self, idx: usize) -> ColumnType {
        self.columns[idx].ty
    }

    /// Resolve a column reference to its flat index.
    ///
    /// An unqualified reference needs a unique match by name. 0 matches is
    /// 42703 and more than 1 match is 42702. For a qualified `t.name`, `t` must
    /// be a qualifier in scope, and 42P01 if it is not. The name must then have
    /// a unique match under that qualifier, where 0 matches is 42703 and more
    /// than 1 match is 42702.
    ///
    /// [`POSITION_QUALIFIER`] is the one exception. It names a column by index
    /// instead of by name, which is how a `*` expansion refers to a relation
    /// whose column names repeat.
    pub fn resolve(&self, qualifier: Option<&str>, name: &str) -> Result<usize, ExecError> {
        if qualifier == Some(POSITION_QUALIFIER) {
            return name
                .parse::<usize>()
                .ok()
                .filter(|index| *index < self.columns.len())
                .ok_or_else(|| ExecError::UndefinedColumn(name.to_string()));
        }
        // ONE pass, testing the name before the qualifier. Both tests are pure,
        // so the order does not change the outcome, and the name rejects almost
        // every column without touching the qualifier at all.
        let mut found: Option<usize> = None;
        for (i, c) in self.columns.iter().enumerate() {
            if c.name == name && qualifier.is_none_or(|q| c.qualifier.as_deref() == Some(q)) {
                if found.is_some() {
                    return Err(ExecError::AmbiguousColumn(name.to_string()));
                }
                found = Some(i);
            }
        }
        if let Some(index) = found {
            return Ok(index);
        }
        // Nothing matched, so the qualifier still has to be checked to tell
        // 42P01 from 42703. It cannot be reached with a match in hand: a match
        // required a column carrying that very qualifier.
        if let Some(q) = qualifier
            && !self
                .columns
                .iter()
                .any(|c| c.qualifier.as_deref() == Some(q))
        {
            return Err(ExecError::MissingFromEntry(q.to_string()));
        }
        Err(ExecError::UndefinedColumn(name.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgcatalog::{Column, RelationName, Table};

    use super::*;

    fn tbl(name: &str, cols: &[(&str, ColumnType)]) -> Table {
        Table {
            id: 1,
            owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
            name: RelationName::public(name),
            columns: cols.iter().map(|(n, t)| Column::new(*n, *t)).collect(),
            sharded: false,
            row_security: false,
            force_row_security: false,
            sharding: None,
            foreign: None,
            checks: Vec::new(),
        }
    }

    fn binding(qualifier: &str, name: &str, ty: ColumnType) -> ColumnBinding {
        ColumnBinding {
            qualifier: Some(qualifier.to_string()),
            name: name.to_string(),
            ty,
        }
    }

    #[test]
    fn single_table_resolves_bare_and_qualified() {
        let t = tbl("t", &[("id", ColumnType::Int4), ("name", ColumnType::Text)]);
        let s = Scope::single(&t, "t");
        assert!(s.resolve(None, "id") == Ok(0));
        assert!(s.resolve(Some("t"), "name") == Ok(1));
    }

    #[test]
    #[allow(non_snake_case)]
    fn unknown_column_is_42703_and_unknown_qualifier_is_42P01() {
        let t = tbl("t", &[("id", ColumnType::Int4)]);
        let s = Scope::single(&t, "t");
        assert!(s.resolve(None, "nope") == Err(ExecError::UndefinedColumn("nope".into())));
        assert!(s.resolve(Some("x"), "id") == Err(ExecError::MissingFromEntry("x".into())));
    }

    #[test]
    fn duplicate_bare_name_across_tables_is_ambiguous_42702() {
        // Two tables each with `id`; a bare `id` is ambiguous, a qualified one is not.
        let a = tbl("a", &[("id", ColumnType::Int4)]);
        let b = tbl("b", &[("id", ColumnType::Int4)]);
        let mut s = Scope::single(&a, "a");
        s.columns.extend(Scope::single(&b, "b").columns);
        assert!(s.resolve(None, "id") == Err(ExecError::AmbiguousColumn("id".into())));
        assert!(s.resolve(Some("a"), "id") == Ok(0));
        assert!(s.resolve(Some("b"), "id") == Ok(1));
    }

    #[test]
    fn insert_conflict_binds_target_then_excluded() {
        let t = tbl("t", &[("k", ColumnType::Int4), ("v", ColumnType::Text)]);
        let expected = Scope {
            columns: vec![
                binding("t", "k", ColumnType::Int4),
                binding("t", "v", ColumnType::Text),
                binding("excluded", "k", ColumnType::Int4),
                binding("excluded", "v", ColumnType::Text),
            ],
        };
        assert!(Scope::insert_conflict(&t) == expected);
    }

    #[test]
    fn insert_conflict_resolution() {
        let t = tbl(
            "t",
            &[
                ("k", ColumnType::Int4),
                ("v", ColumnType::Text),
                ("n", ColumnType::Int8),
            ],
        );
        let width = t.columns.len();
        let s = Scope::insert_conflict(&t);
        // Target columns first at 0..width, `excluded` mirroring them at width+i;
        // every bare name is ambiguous (PG-correct — see `insert_conflict`).
        let cases: Vec<(Option<&str>, &str, Result<usize, ExecError>)> = vec![
            (Some("t"), "k", Ok(0)),
            (Some("t"), "v", Ok(1)),
            (Some("t"), "n", Ok(2)),
            (Some("excluded"), "k", Ok(width)),
            (Some("excluded"), "v", Ok(width + 1)),
            (Some("excluded"), "n", Ok(width + 2)),
            (None, "k", Err(ExecError::AmbiguousColumn("k".into()))),
            (None, "v", Err(ExecError::AmbiguousColumn("v".into()))),
            (
                Some("t"),
                "nope",
                Err(ExecError::UndefinedColumn("nope".into())),
            ),
            (
                Some("other"),
                "k",
                Err(ExecError::MissingFromEntry("other".into())),
            ),
            // An absent qualifier is 42P01 even when the name is ambiguous under
            // the qualifiers that ARE in scope, and even when the name is absent
            // too: the missing FROM entry outranks both.
            (
                Some("other"),
                "nope",
                Err(ExecError::MissingFromEntry("other".into())),
            ),
        ];

        for (qualifier, name, expected) in cases {
            let got = s.resolve(qualifier, name);
            assert!(got == expected, "resolving {qualifier:?}.{name}");
        }
    }
}
