//! SP33: the resolution scope for a relation, which can be a joined relation.
//!
//! A `Scope` is the ordered schema of a relation's combined row. `resolve` maps
//! a column reference, qualified or bare, to its flat index into that row. This
//! replaces the single-`crabka_pgcatalog::Table` column lookup that every prior
//! slice used.

use crabka_pgcatalog::Table;
use crabka_pgtypes::{ColumnType, Datum, RecordValue};

use crate::error::ExecError;

/// One column visible in a scope: its source qualifier (table name or alias;
/// `None` for a USING/NATURAL-coalesced column), its name, its type, and how a
/// reference can reach it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnBinding {
    pub(crate) qualifier: Option<String>,
    pub(crate) name: String,
    pub(crate) ty: ColumnType,
    pub(crate) exposure: Exposure,
}

/// How a reference can reach a column.
///
/// `PostgreSQL` keeps a joined query's base range-table entries whole and adds
/// the join's own merged column list on top, so `ja.x` and the merged `x` are
/// two different things that a flat, one-qualifier-per-column list cannot both
/// hold. [`Exposure::JoinInput`] is the second of them: the side's raw column,
/// kept in the row so `ja.x`, `ja.*` and `SELECT ja` still see the side's own
/// value, but reachable only when a reference names its qualifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Exposure {
    /// An ordinary column of the relation: reachable bare or qualified, and
    /// expanded by `*`.
    #[default]
    Output,
    /// A `USING`/`NATURAL` join's raw input column from one side. A bare name
    /// and `*` see the join's merged column instead; only `ja.x`, `ja.*` and a
    /// whole-row `ja` reach this one.
    ///
    /// `merged` is the flat index of the merged column when `PostgreSQL`'s
    /// merged variable *is* this very column, and `None` when it is not.
    /// `PostgreSQL` builds that variable in `buildMergedJoinVar`: an INNER or
    /// LEFT join takes the left input's column, a RIGHT join the right input's,
    /// and only a FULL join needs a real `COALESCE` of the two. That is why
    /// `SELECT x … GROUP BY ja.x` is grouped-valid over a LEFT join but not over
    /// a FULL one, and why `pg_get_viewdef` prints the merged column of a RIGHT
    /// join as `jb.x`.
    JoinInput { merged: Option<usize> },
    /// The hidden liveness marker an outer join adds for one qualifier on the
    /// side it can null-extend. Its qualifier is [`LIVE_QUALIFIER`] and its name
    /// is the qualifier it marks; see [`Scope::live_marker`].
    LiveMarker,
}

impl ColumnBinding {
    /// Is this column hidden from a bare name and from `*`?
    ///
    /// Two kinds are: a `USING`/`NATURAL` join's raw input column, which a bare
    /// name and `*` skip in favour of the merged column, and an outer join's
    /// liveness marker, which is not a column of the relation at all. Both stay
    /// in the row — the first so `ja.x` and `SELECT ja` still see the side's own
    /// value, the second so a whole-row reference can tell a null-extended row
    /// from a stored one.
    ///
    /// The name predates the marker; every caller wants "skip what `*` skips",
    /// which is what this answers.
    pub(crate) fn is_join_input(&self) -> bool {
        !matches!(self.exposure, Exposure::Output)
    }
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

/// The qualifier of the hidden columns a correlated select-list, `ORDER BY` or
/// `DISTINCT ON` expression is materialized into: `$corr.0` is "the value the
/// first such expression took for this source row".
///
/// A correlated subquery reads the source row, so it cannot be folded once
/// before the row loop the way an uncorrelated one is. Evaluating it per row
/// and parking the value in a hidden column lets the ordinary projection,
/// sort, and dedup machinery keep treating the select list as a set of
/// row-local expressions. As with [`POSITION_QUALIFIER`], a `$` cannot begin an
/// unquoted identifier, so no user relation can collide with this qualifier,
/// and `*` skips the columns carrying it.
pub(crate) const CORRELATED_QUALIFIER: &str = "$corr";

/// The qualifier of the liveness markers an outer join adds: `$live.jb` is "was
/// `jb`'s side of the join a real row for this output row, or was it invented by
/// null-extension?".
///
/// A whole-row reference to the null-extended side of an outer join is NULL *as
/// a whole* — `count(jb)` skips it and `jb::text` renders nothing — while a
/// stored row whose every column happens to be NULL is an ordinary composite
/// that renders `(,)` and is counted. `IS NULL` is true for both (it is
/// field-wise, as PostgreSQL's `argisrow` test is), so nothing about the values
/// tells them apart; only where the row came from does. `ja LEFT JOIN jb ON
/// true` puts both in one result, so the distinction cannot be a property of the
/// query either — it is per row.
///
/// `PostgreSQL` carries it the same way, one level lower: `EXPLAIN VERBOSE` of
/// `SELECT jb FROM ja LEFT JOIN jb ON …` shows the scan of `jb` emitting `jb.*`
/// as a real output column, which the join then null-extends along with the
/// rest. The marker is that column with the payload dropped — the composite is
/// cheap to rebuild from the columns already in the row, and its liveness is
/// not.
///
/// As with [`POSITION_QUALIFIER`], a `$` cannot begin an unquoted identifier, so
/// no user relation can collide with this qualifier, and both `*` and a bare
/// name skip the columns carrying it.
pub(crate) const LIVE_QUALIFIER: &str = "$live";

/// The ordered schema of a relation. Flat indices line up with the combined row.
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
                    exposure: Exposure::Output,
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
        //
        // A qualified reference reaches every column carrying that qualifier,
        // join inputs included — that is the only way to `ja.x`. A bare one
        // skips them, so the merged `x` of a USING/NATURAL join is the single
        // match rather than one of three.
        let mut found: Option<usize> = None;
        for (i, c) in self.columns.iter().enumerate() {
            if c.name == name
                && match qualifier {
                    Some(q) => c.qualifier.as_deref() == Some(q),
                    None => !c.is_join_input(),
                }
            {
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

    /// The index that names the same *variable* as the column at `index`.
    ///
    /// A `USING`/`NATURAL` join input whose value `PostgreSQL` reuses as the
    /// merged column is not a separate variable there: over `ja LEFT JOIN jb
    /// USING (x)` the merged `x` and `ja.x` are one and the same `Var`, which is
    /// why `SELECT x … GROUP BY ja.x` is grouped-valid and why `pg_get_viewdef`
    /// prints that merged column as `ja.x`. Following the link collapses the two
    /// spellings onto one index so `GROUP BY` matching agrees. A FULL join's
    /// merged column is a real `COALESCE` of both sides and links to neither, so
    /// grouping by one side alone stays 42803, exactly as `PostgreSQL` has it.
    ///
    /// Chained joins nest the links (`ja JOIN jb USING (x) JOIN jc USING (x)`
    /// merges a merged column again), so this follows them to the end. The walk
    /// is bounded by the scope width, because every step moves to a strictly
    /// earlier column.
    pub fn canonical(&self, index: usize) -> usize {
        let mut index = index;
        for _ in 0..self.columns.len() {
            match self.columns[index].exposure {
                Exposure::JoinInput {
                    merged: Some(merged),
                } => index = merged,
                _ => break,
            }
        }
        index
    }

    /// Resolve a *whole-row* reference: the flat indices, in row order, of every
    /// column carrying `qualifier`.
    ///
    /// A `USING`/`NATURAL` join input carries its side's qualifier and belongs
    /// to that side's row, so `SELECT ja` over such a join still yields every
    /// column of `ja` in declaration order.
    ///
    /// `SELECT t FROM t` does not name a column at all. `PostgreSQL` resolves a
    /// bare name that matches no column against the range table, and a match
    /// there becomes a `Var` with `varattno` 0 — the entire row as one composite
    /// value, whose fields are that relation's columns in declaration order.
    ///
    /// A column always wins: `SELECT shadow FROM shadow` reads the column, so
    /// this is only ever consulted after [`Scope::resolve`] has reported 42703.
    /// Only an UNQUALIFIED name can be a whole-row reference — `PostgreSQL`
    /// reads `s.t` as `table s, column t` and reports a missing FROM entry for
    /// `s`, never as the whole row of `s.t`.
    ///
    /// `None` when nothing in scope carries that qualifier, which keeps the
    /// caller's 42703 exactly as it was.
    pub fn whole_row(&self, qualifier: &str) -> Option<Vec<usize>> {
        // The internal qualifiers name a *column* by position or by ordinal, so
        // they are not relations and have no whole row. Neither can be spelled
        // by a user reference (a `$` cannot begin an unquoted identifier), but a
        // caller holding a name from elsewhere must not reach them either.
        if qualifier == POSITION_QUALIFIER
            || qualifier == CORRELATED_QUALIFIER
            || qualifier == LIVE_QUALIFIER
        {
            return None;
        }
        let indices: Vec<usize> = self
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.qualifier.as_deref() == Some(qualifier))
            .map(|(i, _)| i)
            .collect();
        (!indices.is_empty()).then_some(indices)
    }

    /// The flat index of `qualifier`'s liveness marker, when an outer join above
    /// it added one. See [`LIVE_QUALIFIER`].
    ///
    /// Absent for every relation no outer join can null-extend, which is why a
    /// query without one pays nothing for this.
    pub fn live_marker(&self, qualifier: &str) -> Option<usize> {
        self.columns.iter().position(|c| {
            c.exposure == Exposure::LiveMarker
                && c.qualifier.as_deref() == Some(LIVE_QUALIFIER)
                && c.name == qualifier
        })
    }

    /// The composite value of a whole-row reference over one row of this scope.
    ///
    /// The field names are the relation's column names, which is what
    /// `row_to_json(t)` and `(t).c` read; the type is the anonymous `record`,
    /// because a relation's composite type is not registered here.
    ///
    /// A row an outer join invented for this side has no whole row to speak of,
    /// so the reference is NULL rather than a composite of NULLs — see
    /// [`LIVE_QUALIFIER`]. Every consumer then gets that for free: `count(jb)`
    /// skips it, `row_to_json(jb)` is NULL, `jb::text` renders nothing,
    /// `COALESCE(jb::text, 'none')` takes the fallback, and the wire encoder
    /// sends NULL.
    ///
    /// One pass over the scope collects the qualifier's columns and reads its
    /// marker together, because this runs per row: it is the same single scan
    /// [`Scope::whole_row`] alone used to cost, and an invented row now leaves
    /// before any name or field is cloned.
    pub fn whole_row_value(&self, qualifier: &str, values: &[Datum]) -> Option<Datum> {
        if qualifier == POSITION_QUALIFIER
            || qualifier == CORRELATED_QUALIFIER
            || qualifier == LIVE_QUALIFIER
        {
            return None;
        }
        let mut indices: Vec<usize> = Vec::new();
        let mut invented = false;
        for (i, c) in self.columns.iter().enumerate() {
            if c.exposure == Exposure::LiveMarker {
                invented |= c.name == qualifier && values[i].is_null();
            } else if c.qualifier.as_deref() == Some(qualifier) {
                indices.push(i);
            }
        }
        if indices.is_empty() {
            return None;
        }
        if invented {
            return Some(Datum::Null);
        }
        let names: std::sync::Arc<[String]> = indices
            .iter()
            .map(|i| self.columns[*i].name.clone())
            .collect();
        let fields = indices
            .iter()
            .map(|i| values[*i].clone())
            .collect::<Vec<_>>();
        Some(Datum::Record(RecordValue::named(None, names, fields)))
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
            materialized: None,
            checks: Vec::new(),
        }
    }

    fn binding(qualifier: &str, name: &str, ty: ColumnType) -> ColumnBinding {
        ColumnBinding {
            exposure: Exposure::Output,
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
