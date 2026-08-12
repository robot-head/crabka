//! SP33: the resolution scope for a relation, which can be a joined relation.
//!
//! A `Scope` is the ordered schema of a relation's combined row. `resolve` maps
//! a column reference, qualified or bare, to its flat index into that row. This
//! replaces the single-`crabka_pgcatalog::Table` column lookup that every prior
//! slice used.

use crabka_pgcatalog::Table;
use crabka_pgparser::ast::{
    DistinctClause, Expr, FrameBound, FuncArgs, JoinConstraint, QueryBody, QueryExpr, SelectItem,
    SelectStmt, SetExpr, TableExpr, WindowCall, WindowRef, WindowSpec,
};
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
    /// One of the system columns the engine answers — [`TABLEOID_COLUMN`] or
    /// [`CTID_COLUMN`].
    ///
    /// It carries the relation's own qualifier, so `a.ctid` and — when no other
    /// relation in scope offers the name — a bare `ctid` both reach it.
    /// Everything that enumerates a relation's columns skips it, which is how
    /// `PostgreSQL` hides a system column: `SELECT *`, `SELECT a.*`, a
    /// whole-row `SELECT a`, `pg_attribute`-driven `\d`, and
    /// `information_schema.columns` all show only the user columns.
    SystemColumn,
}

/// The oid of the relation the row itself came from, which over an inheritance
/// or partition tree is the LEAF's and not the parent's.
///
/// `PostgreSQL` has six system columns (`tableoid`, `cmax`, `xmax`, `cmin`,
/// `xmin`, `ctid`, at `attnum` -6 through -1). This is the one whose answer is
/// a catalog fact the engine already holds.
pub(crate) const TABLEOID_COLUMN: &str = "tableoid";

/// An identifier for where the row is stored, which is what `ctid` *means*.
///
/// The four system columns still missing (`cmin`, `cmax`, `xmin`, `xmax`)
/// report a heap tuple's MVCC header, which a KV row space keeps in a shape
/// nothing here can render as one. `ctid` looked like the same kind of gap —
/// `PostgreSQL` answers it with a block number and a slot inside that block,
/// and there is no heap here to have either. What makes it answerable anyway is
/// that a `ctid` is not a promise about a layout: an `UPDATE` moves the row and
/// `CLUSTER` renumbers every row in the relation, so no portable statement can
/// depend on a particular value. What is left of the contract is "an identifier
/// for this row's storage", and the engine does hold one — see [`row_ctid`].
pub(crate) const CTID_COLUMN: &str = "ctid";

/// The `ctid` of the row whose storage identity is `identity`.
///
/// The identity is the row's rowid for a stored relation, and the row's
/// one-based ordinal in the projection for a relation the engine synthesises,
/// which has no other storage to name.
///
/// A rowid is the key every version of one row hangs under, so it outlives an
/// `UPDATE`: the new version is written beside the old one under the same key,
/// and the row keeps its `ctid`. `PostgreSQL`'s moves, because there the update
/// writes a new tuple somewhere else in the heap. `CLUSTER` reassigns rowids in
/// index order, which is the one event that moves a `ctid` in both. Nothing may
/// depend on either behaviour — `PostgreSQL` documents a `ctid` as valid only
/// until the row is updated or the table is rewritten, so a statement that
/// survives one of those was already outside the contract.
///
/// The 64-bit identity is split across the two fields a `tid` has, high half
/// into the block and low half into the offset. Two rows of one relation
/// therefore differ, up to 2^48 rows, which is past the point `PostgreSQL`'s
/// own `ItemPointer` stops being able to address them: a 4-billion-block heap
/// of 8 kB pages holds about 1.2e12 tuples. Identity 0 is never handed out —
/// rowids and ordinals both start at 1 — so no row is ever stamped `(0,0)`, the
/// value `PostgreSQL` reserves for an invalid item pointer.
pub(crate) fn row_ctid(identity: u64) -> Datum {
    Datum::Tid(crabka_pgtypes::Tid {
        // Saturating rather than wrapping: past 2^48 rows in one relation the
        // low half no longer separates them whatever the high half does, and a
        // pinned block is at least monotone with the identity.
        block: u32::try_from(identity >> 16).unwrap_or(u32::MAX),
        offset: u16::try_from(identity & u64::from(u16::MAX)).unwrap_or(u16::MAX),
    })
}

impl ColumnBinding {
    /// Is this column hidden from `*`?
    ///
    /// Three kinds are: a `USING`/`NATURAL` join's raw input column, which a
    /// bare name and `*` skip in favour of the merged column; an outer join's
    /// liveness marker, which is not a column of the relation at all; and a
    /// system column, which `PostgreSQL` keeps out of every expansion of a
    /// relation's columns. All three stay in the row — the first so `ja.x` and
    /// `SELECT ja` still see the side's own value, the second so a whole-row
    /// reference can tell a null-extended row from a stored one, the third so
    /// `a.tableoid` has something to read.
    ///
    /// The name predates the other two; every caller wants "skip what `*`
    /// skips", which is what this answers. A bare name skips the same set with
    /// one exception, spelled out in [`Scope::resolve`]: a system column is
    /// reachable bare, because `SELECT tableoid FROM t` is valid.
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

/// What one statement's text asks the read paths below it to carry.
///
/// Two hidden columns are conditional on this, for one reason: each is a column
/// on every row of something, so adding one nothing reads makes the whole result
/// wider for nothing — enough, over a ten-thousand-row join, to push a query
/// past the blocking-query memory budget it used to fit inside.
///
/// * `names` is every UNQUALIFIED name written anywhere in the statement, which
///   decides the liveness markers ([`LIVE_QUALIFIER`]) an outer join carries. It
///   is a superset of the whole-row references in the statement:
///   [`Scope::whole_row_value`] is reached from exactly one place, an
///   `Expr::Column` whose `table` is `None` that no column of the scope answers,
///   so a marker can only ever be read through a bare name the statement spells.
///   Names that turn out to be ordinary columns (the overwhelming majority) cost
///   a marker only when a relation of the same query happens to share the name,
///   and a marker nothing reads is merely the width this type exists to avoid —
///   never a wrong answer.
/// * `tableoid` and `ctid` are whether the statement spells either name at all,
///   qualified or not, which decides whether a scan stamps each row with the
///   relation it came from and with its own storage identity. Unlike a marker,
///   these are read through `a.tableoid` as often as bare, so neither can be
///   narrowed to a qualifier.
///
/// Deliberately not narrowed to "names that fail to resolve": which names those
/// are depends on the scope, and the scope is what the read path is in the
/// middle of building.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StatementRefs {
    names: std::collections::HashSet<String>,
    tableoid: bool,
    ctid: bool,
}

/// Must a join carry a liveness marker for `qualifier`?
///
/// `None` is "the statement is not known here", which marks every qualifier —
/// the behaviour every path had before markers became conditional. Only a caller
/// holding the statement whose FROM clause it is building can narrow it, and a
/// caller that cannot must not guess.
pub(crate) fn wants_whole_row(refs: Option<&StatementRefs>, qualifier: &str) -> bool {
    refs.is_none_or(|refs| refs.names.contains(qualifier))
}

/// Must a stored-relation scan stamp each row with its relation's oid?
///
/// `None` is "the statement is not known here", and stamps nothing: every path
/// that reaches a scan without a statement in hand — the schema-description
/// walk, a DML target read — has no `tableoid` reference to answer, because
/// only a `SELECT` establishes the refs and only an expression bound against a
/// scan's own scope can read the column.
pub(crate) fn wants_tableoid(refs: Option<&StatementRefs>) -> bool {
    refs.is_some_and(StatementRefs::reads_tableoid)
}

/// Must a scan stamp each row with its own storage identity? The same rule
/// [`wants_tableoid`] states, for [`CTID_COLUMN`].
pub(crate) fn wants_ctid(refs: Option<&StatementRefs>) -> bool {
    refs.is_some_and(StatementRefs::reads_ctid)
}

/// Must a read path that builds no system column at all decline the statement?
///
/// The fast paths resolve a select list against a bare [`Scope::single`], which
/// carries neither system column, so a statement that spells either has to take
/// the ordinary read path rather than report the 42703 that path is about to
/// answer. They ask this one question instead of both, so a third system column
/// cannot be added without every one of them getting it.
pub(crate) fn wants_system_column(refs: Option<&StatementRefs>) -> bool {
    refs.is_some_and(StatementRefs::reads_system_column)
}

/// The hidden system columns one scan of one relation appends to every row it
/// yields, in the order [`Scope::push_tableoid`] and [`Scope::push_ctid`] put
/// them in.
///
/// One value decides the scope and the row together. A scan that pushed the
/// column but stamped no value, or the reverse, would hand the layers above it
/// a row whose width disagrees with its schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct SystemColumns {
    pub(crate) tableoid: bool,
    pub(crate) ctid: bool,
}

impl SystemColumns {
    /// What a scan of `table` carries for the statement `refs` describes.
    ///
    /// Two relations get less than the statement asked for.
    ///
    /// A relation that declares a column of its own by either name gets neither
    /// of them. `PostgreSQL` rejects `CREATE TABLE t (ctid int)` outright —
    /// "column name \"ctid\" conflicts with a system column name" — so the
    /// clash cannot arise there; this engine accepts the declaration, and a
    /// scope holding both bindings would answer 42702 for a name that reads the
    /// user's own column today.
    ///
    /// A foreign table gets no `ctid`. Its rows come back from a remote system
    /// through [`crate::foreign::ForeignScanner::scan`], which hands over bare
    /// `Datum`s and no storage identity to derive one from, so the name stays
    /// the 42703 it is now rather than becoming a counter over whatever order
    /// that scan happened to return.
    pub(crate) fn of(refs: Option<&StatementRefs>, table: &Table) -> Self {
        Self {
            tableoid: wants_tableoid(refs) && table.column_index(TABLEOID_COLUMN).is_none(),
            ctid: wants_ctid(refs)
                && table.column_index(CTID_COLUMN).is_none()
                && table.foreign.is_none(),
        }
    }

    /// Append these columns to `scope`, qualified by `qualifier`.
    pub(crate) fn extend_scope(self, scope: &mut Scope, qualifier: &str) {
        if self.tableoid {
            scope.push_tableoid(qualifier);
        }
        if self.ctid {
            scope.push_ctid(qualifier);
        }
    }
}

impl StatementRefs {
    /// Every unqualified name `select` spells, in it and in every query nested
    /// inside it.
    ///
    /// A nested query re-derives its own set when it executes, so descending is
    /// not what makes an inner reference safe; it covers the reverse case, an
    /// inner expression that is evaluated against THIS statement's scope — a
    /// correlated select list, a lateral item's `ON` — whose text belongs to the
    /// inner query but whose reference is resolved out here.
    pub(crate) fn of_select(select: &SelectStmt) -> Self {
        let mut refs = Self::default();
        refs.add_select(select);
        refs
    }

    /// The references of a statement that reads every system column.
    ///
    /// For the one caller that needs to know which names a FROM *can* supply
    /// rather than which ones a particular statement asked of it: whether a
    /// scan builds the column is a width optimisation, and a stored relation
    /// offers both names whatever the statement spells. See
    /// [`crate::exec::from_column_names`].
    pub(crate) fn every_system_column() -> Self {
        Self {
            names: std::collections::HashSet::new(),
            tableoid: true,
            ctid: true,
        }
    }

    /// Does the statement spell [`TABLEOID_COLUMN`], qualified or bare?
    ///
    /// A read path that cannot build the column has to decline the whole
    /// statement rather than answer 42703 for it, so this is asked in two
    /// places: here, through [`wants_tableoid`], by the scan that stamps the
    /// column, and directly by the fast paths that resolve a select list
    /// against a scope with no system column in it.
    pub(crate) const fn reads_tableoid(&self) -> bool {
        self.tableoid
    }

    /// Does the statement spell [`CTID_COLUMN`], qualified or bare? Asked for
    /// the reason [`Self::reads_tableoid`] is, at the scan that stamps it.
    pub(crate) const fn reads_ctid(&self) -> bool {
        self.ctid
    }

    /// Does the statement spell either system column? See
    /// [`wants_system_column`], the one thing that asks.
    pub(crate) const fn reads_system_column(&self) -> bool {
        self.tableoid || self.ctid
    }

    fn add_select(&mut self, select: &SelectStmt) {
        // Destructured without `..` on purpose: a clause added to `SelectStmt`
        // later has to be considered here rather than silently skipped, and the
        // cost of overlooking one is a marker missing where something reads it.
        let SelectStmt {
            projection,
            from,
            filter,
            distinct,
            group_by,
            grouping: _, // indices into `group_by`, which is walked in full
            having,
            windows,
            window_calls,
            order_by,
            limit,
            offset,
            with_ties: _,
            locking: _, // relation names, never expressions
        } = select;
        for item in projection {
            match item {
                SelectItem::Expr { expr, alias: _ } => self.add_expr(expr),
                // `*` and `a.*` expand to the columns themselves, never to a
                // whole row.
                SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {}
            }
        }
        for item in from {
            self.add_table_expr(item);
        }
        for expr in filter.iter().chain(having).chain(limit).chain(offset) {
            self.add_expr(expr);
        }
        if let DistinctClause::On(keys) = distinct {
            for key in keys {
                self.add_expr(key);
            }
        }
        for expr in group_by {
            self.add_expr(expr);
        }
        for item in order_by {
            self.add_expr(&item.expr);
        }
        for window in windows {
            self.add_window_spec(&window.spec);
        }
        for WindowCall {
            name: _,
            distinct: _,
            args,
            filter,
            over,
        } in window_calls
        {
            match args {
                FuncArgs::Exprs(args) => {
                    for arg in args {
                        self.add_expr(arg);
                    }
                }
                // `count(*)` reads no expression at all.
                FuncArgs::Star => {}
            }
            if let Some(expr) = filter {
                self.add_expr(expr);
            }
            match over {
                WindowRef::Spec(spec) => self.add_window_spec(spec),
                // A `WINDOW` name, whose spec is in `windows` and walked there.
                WindowRef::Named(_) => {}
            }
        }
    }

    fn add_window_spec(&mut self, spec: &WindowSpec) {
        let WindowSpec {
            base: _, // a `WINDOW` name, whose own spec is walked where it is defined
            partition_by,
            order_by,
            frame,
        } = spec;
        for expr in partition_by {
            self.add_expr(expr);
        }
        for item in order_by {
            self.add_expr(&item.expr);
        }
        if let Some(frame) = frame {
            for bound in [&frame.start, &frame.end] {
                match bound {
                    FrameBound::Preceding(expr) | FrameBound::Following(expr) => {
                        self.add_expr(expr);
                    }
                    FrameBound::UnboundedPreceding
                    | FrameBound::CurrentRow
                    | FrameBound::UnboundedFollowing => {}
                }
            }
        }
    }

    /// Every FROM item is destructured without `..`, for the reason
    /// [`Self::add_select`] is: an expression-bearing field added later must
    /// stop the build rather than be skipped.
    fn add_table_expr(&mut self, table: &TableExpr) {
        match table {
            TableExpr::Table {
                name: _,
                only: _,
                alias: _,
                columns: _,
                sample,
            } => {
                for expr in sample
                    .iter()
                    .flat_map(|s| std::iter::once(&s.percent).chain(s.repeatable.iter()))
                {
                    self.add_expr(expr);
                }
            }
            TableExpr::Derived {
                subquery,
                alias: _,
                columns: _,
                lateral: _,
            } => self.add_query(subquery),
            TableExpr::Join {
                left,
                right,
                kind: _,
                constraint,
            } => {
                self.add_table_expr(left);
                self.add_table_expr(right);
                if let JoinConstraint::On(expr) = constraint {
                    self.add_expr(expr);
                }
            }
            TableExpr::Function {
                functions,
                rows_from: _,
                with_ordinality: _,
                lateral: _,
                alias: _,
                column_aliases: _,
            } => {
                for call in functions {
                    for arg in &call.args {
                        self.add_expr(arg);
                    }
                }
            }
            TableExpr::JsonTable(table) => {
                for expr in table.exprs() {
                    self.add_expr(expr);
                }
            }
        }
    }

    fn add_query(&mut self, query: &QueryExpr) {
        if let Some(with) = &query.with {
            for cte in &with.ctes {
                if let Some(body) = cte.body.as_query() {
                    self.add_query(body);
                }
            }
        }
        self.add_set_expr(&query.body);
        for item in &query.order_by {
            self.add_expr(&item.expr);
        }
        for expr in query.limit.iter().chain(&query.offset) {
            self.add_expr(expr);
        }
    }

    fn add_set_expr(&mut self, body: &SetExpr) {
        match body {
            SetExpr::Query(QueryBody::Select(select)) => self.add_select(select),
            SetExpr::Query(QueryBody::Values(values)) => {
                for row in &values.rows {
                    for expr in row {
                        self.add_expr(expr);
                    }
                }
            }
            SetExpr::Query(QueryBody::Nested(nested)) => self.add_query(nested),
            SetExpr::SetOp { left, right, .. } => {
                self.add_set_expr(left);
                self.add_set_expr(right);
            }
        }
    }

    fn add_expr(&mut self, expr: &Expr) {
        if let Expr::Column { table, name } = expr {
            if table.is_none() {
                self.names.insert(name.clone());
            }
            // Qualified or not: `a.tableoid` is the spelling an inheritance
            // query uses most, and it reaches the same hidden column.
            self.tableoid |= name == TABLEOID_COLUMN;
            self.ctid |= name == CTID_COLUMN;
        }
        for child in crate::exec::expr_children(expr) {
            self.add_expr(child);
        }
        for query in crate::exec::query_children(expr) {
            self.add_query(query);
        }
    }
}

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

    /// Append the hidden [`TABLEOID_COLUMN`] for `qualifier`, at the end of the
    /// row.
    ///
    /// At the end, and never among the relation's own columns, because every
    /// index into a stored row is an index into `Table::columns`: a scan's
    /// pushed-down projection, a partition's `column_mapping` permutation, and
    /// the storage encoding all count from zero and stop at the declared width.
    /// A system column past that width is invisible to all three, which is what
    /// lets one appended `Datum` carry it.
    ///
    /// [`ColumnType::Int4`] and not [`ColumnType::Oid`], which is what
    /// `pg_typeof` answers in `PostgreSQL`, because `Int4` is how this engine
    /// spells every relation oid it produces: `pg_class.oid`, `pg_type.oid` and
    /// the `regclass` datum all carry one. The two spellings that matter both
    /// depend on it — `tableoid::regclass` resolves a NAME only for an `Int4`,
    /// `Int8` or text operand (see [`crate::exec::regclass_cast`]), and
    /// `t.tableoid = pg_class.oid` is a comparison of two `Int4`s rather than of
    /// two types the engine has no operator for.
    pub fn push_tableoid(&mut self, qualifier: &str) {
        self.push_system_column(qualifier, TABLEOID_COLUMN, ColumnType::Int4);
    }

    /// Append the hidden [`CTID_COLUMN`] for `qualifier`, after any
    /// [`Scope::push_tableoid`] and at the end of the row.
    ///
    /// The order of the two is the order every scan appends their values in,
    /// and it is fixed here so a scan cannot pick a different one.
    /// [`ColumnType::Tid`] is what `PostgreSQL` gives the column, and the type
    /// the `tid` literals a statement compares it against already parse as.
    pub fn push_ctid(&mut self, qualifier: &str) {
        self.push_system_column(qualifier, CTID_COLUMN, ColumnType::Tid);
    }

    fn push_system_column(&mut self, qualifier: &str, name: &str, ty: ColumnType) {
        self.columns.push(ColumnBinding {
            qualifier: Some(qualifier.to_string()),
            name: name.to_string(),
            ty,
            exposure: Exposure::SystemColumn,
        });
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
        //
        // A system column is the exception a bare reference still reaches:
        // `SELECT tableoid FROM t` is valid, and over two relations that both
        // offer the name it is 42702, exactly as an ambiguous user column is.
        let mut found: Option<usize> = None;
        for (i, c) in self.columns.iter().enumerate() {
            if c.name == name
                && match qualifier {
                    Some(q) => c.qualifier.as_deref() == Some(q),
                    None => !c.is_join_input() || c.exposure == Exposure::SystemColumn,
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
        // A system column carries the relation's own qualifier but is not one of
        // its columns: `SELECT t FROM t` over a two-column table is `(1,x)` in
        // PostgreSQL, never `(1,x,16385)`.
        let indices: Vec<usize> = self
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                c.qualifier.as_deref() == Some(qualifier) && c.exposure != Exposure::SystemColumn
            })
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
    pub fn refs_value(&self, qualifier: &str, values: &[Datum]) -> Option<Datum> {
        if qualifier == POSITION_QUALIFIER
            || qualifier == CORRELATED_QUALIFIER
            || qualifier == LIVE_QUALIFIER
        {
            return None;
        }
        let mut indices: Vec<usize> = Vec::new();
        let mut invented = false;
        for (i, c) in self.columns.iter().enumerate() {
            match c.exposure {
                Exposure::LiveMarker => invented |= c.name == qualifier && values[i].is_null(),
                // The same exclusion [`Scope::whole_row`] makes, and it has to be
                // made twice because this is the per-row path and that one is the
                // per-statement one. `SELECT t, tableoid FROM t` is `(1,x)` and an
                // oid beside it in `PostgreSQL`, never `(1,x,20001)`.
                Exposure::SystemColumn => {}
                Exposure::Output | Exposure::JoinInput { .. } => {
                    if c.qualifier.as_deref() == Some(qualifier) {
                        indices.push(i);
                    }
                }
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

    /// The scope a stored-relation scan builds for a statement that reads
    /// `tableoid`: the relation's own columns, then the system column.
    fn scope_with_tableoid(name: &str, cols: &[(&str, ColumnType)]) -> Scope {
        let mut scope = Scope::single(&tbl(name, cols), name);
        scope.push_tableoid(name);
        scope
    }

    #[test]
    fn tableoid_goes_last_and_is_a_system_column() {
        let expected = Scope {
            columns: vec![
                binding("t", "a", ColumnType::Int4),
                ColumnBinding {
                    qualifier: Some("t".to_string()),
                    name: "tableoid".to_string(),
                    ty: ColumnType::Int4,
                    exposure: Exposure::SystemColumn,
                },
            ],
        };
        assert!(scope_with_tableoid("t", &[("a", ColumnType::Int4)]) == expected);
    }

    #[test]
    fn tableoid_resolves_bare_and_qualified() {
        let s = scope_with_tableoid("t", &[("a", ColumnType::Int4)]);
        let cases: Vec<(Option<&str>, &str, Result<usize, ExecError>)> = vec![
            // Both spellings reach it: `SELECT tableoid FROM t` and
            // `SELECT t.tableoid FROM t` are each valid PostgreSQL.
            (None, "tableoid", Ok(1)),
            (Some("t"), "tableoid", Ok(1)),
            // The user columns are untouched by its presence.
            (None, "a", Ok(0)),
            (Some("t"), "a", Ok(0)),
        ];
        for (qualifier, name, expected) in cases {
            assert!(
                s.resolve(qualifier, name) == expected,
                "{qualifier:?}.{name}"
            );
        }
    }

    #[test]
    fn tableoid_is_hidden_from_star_and_from_a_whole_row_reference() {
        let s = scope_with_tableoid("t", &[("a", ColumnType::Int4)]);
        // `is_join_input` is what `SELECT *` filters on, and `whole_row` is what
        // `SELECT t` and `SELECT t.*` expand: `PostgreSQL` shows a system column
        // in none of the three.
        assert!(s.columns[0].is_join_input() == false);
        assert!(s.columns[1].is_join_input() == true);
        assert!(s.whole_row("t") == Some(vec![0]));

        let row = [Datum::Int4(7), Datum::Int4(20_001)];
        let expected = Datum::Record(RecordValue::named(
            None,
            ["a".to_string()].into_iter().collect(),
            vec![Datum::Int4(7)],
        ));
        assert!(s.refs_value("t", &row) == Some(expected));
    }

    #[test]
    fn tableoid_of_two_relations_is_ambiguous_bare_and_reachable_qualified() {
        let mut s = scope_with_tableoid("a", &[("x", ColumnType::Int4)]);
        s.columns
            .extend(scope_with_tableoid("b", &[("y", ColumnType::Int4)]).columns);
        // The shape `inherit.sql` writes is the qualified one, and a comma-FROM
        // over two relations makes the bare one 42702 — which is what
        // `PostgreSQL` answers for `SELECT tableoid FROM t, p`.
        assert!(s.resolve(None, "tableoid") == Err(ExecError::AmbiguousColumn("tableoid".into())));
        assert!(s.resolve(Some("a"), "tableoid") == Ok(1));
        assert!(s.resolve(Some("b"), "tableoid") == Ok(3));
    }

    #[test]
    fn statement_refs_notice_every_spelling_of_tableoid() {
        let cases = [
            ("SELECT 1 FROM t", false),
            ("SELECT a FROM t", false),
            // Bare, qualified, and buried in a filter, a function argument, a
            // cast and a nested query — each is a read the scan must answer.
            ("SELECT tableoid FROM t", true),
            ("SELECT t.tableoid FROM t", true),
            ("SELECT tableoid::regclass FROM t", true),
            ("SELECT 1 FROM t WHERE t.tableoid = 3", true),
            ("SELECT count(tableoid) FROM t", true),
            ("SELECT 1 FROM t WHERE a IN (SELECT tableoid FROM p)", true),
            ("SELECT 1 FROM t ORDER BY tableoid", true),
            // A column merely NAMED like it in another position is not a read of
            // the system column, and the walk must not confuse the two.
            ("SELECT tableoids FROM t", false),
        ];
        for (sql, expected) in cases {
            let parsed = crabka_pgparser::parse(sql).expect("statement parses");
            let [crabka_pgparser::ast::Statement::Query(query)] = parsed.as_slice() else {
                panic!("{sql} is one query");
            };
            let SetExpr::Query(QueryBody::Select(select)) = &query.body else {
                panic!("{sql} is a plain select");
            };
            // `ORDER BY`, `LIMIT` and `OFFSET` are parsed onto the enclosing
            // `QueryExpr`, and every executing path folds them into the
            // `SelectStmt` before it reaches `of_select`. Folding them here too
            // is what makes this measure the statement the engine measures.
            let mut select = (**select).clone();
            select.order_by.clone_from(&query.order_by);
            select.limit.clone_from(&query.limit);
            select.offset.clone_from(&query.offset);
            let refs = StatementRefs::of_select(&select);
            assert!(refs.reads_tableoid() == expected, "{sql}");
        }
    }

    #[test]
    fn statement_refs_notice_every_spelling_of_ctid() {
        let cases = [
            ("SELECT 1 FROM t", (false, false)),
            ("SELECT ctid FROM t", (true, true)),
            ("SELECT t.ctid FROM t", (true, true)),
            ("SELECT min(ctid) FROM t", (true, true)),
            ("SELECT 1 FROM t WHERE ctid = '(0,1)'", (true, true)),
            (
                "SELECT 1 FROM t WHERE a IN (SELECT ctid FROM p)",
                (true, true),
            ),
            ("SELECT 1 FROM t ORDER BY ctid", (true, true)),
            // Only the other one, and a name merely containing it.
            ("SELECT tableoid FROM t", (false, true)),
            ("SELECT ctids FROM t", (false, false)),
        ];
        for (sql, (ctid, system)) in cases {
            let parsed = crabka_pgparser::parse(sql).expect("statement parses");
            let [crabka_pgparser::ast::Statement::Query(query)] = parsed.as_slice() else {
                panic!("{sql} is one query");
            };
            let SetExpr::Query(QueryBody::Select(select)) = &query.body else {
                panic!("{sql} is a plain select");
            };
            let mut select = (**select).clone();
            select.order_by.clone_from(&query.order_by);
            let refs = StatementRefs::of_select(&select);
            assert!(refs.reads_ctid() == ctid, "{sql}");
            assert!(refs.reads_system_column() == system, "{sql}");
        }
    }

    #[test]
    fn ctid_goes_after_tableoid_and_is_a_system_column() {
        let t = tbl("t", &[("a", ColumnType::Int4)]);
        let mut s = Scope::single(&t, "t");
        SystemColumns {
            tableoid: true,
            ctid: true,
        }
        .extend_scope(&mut s, "t");
        let expected = Scope {
            columns: vec![
                binding("t", "a", ColumnType::Int4),
                ColumnBinding {
                    qualifier: Some("t".to_string()),
                    name: "tableoid".to_string(),
                    ty: ColumnType::Int4,
                    exposure: Exposure::SystemColumn,
                },
                ColumnBinding {
                    qualifier: Some("t".to_string()),
                    name: "ctid".to_string(),
                    ty: ColumnType::Tid,
                    exposure: Exposure::SystemColumn,
                },
            ],
        };
        assert!(s == expected);
        // Reachable both ways, and hidden from every expansion of the relation.
        assert!(s.resolve(None, "ctid") == Ok(2));
        assert!(s.resolve(Some("t"), "ctid") == Ok(2));
        assert!(s.whole_row("t") == Some(vec![0]));
        assert!(s.columns[2].is_join_input() == true);
    }

    /// `PostgreSQL` refuses `CREATE TABLE t (ctid int)`; this engine accepts
    /// it, and a scope carrying the user's column and the system one would
    /// answer 42702 for a name that reads the user's column today.
    #[test]
    fn a_relation_declaring_the_name_itself_gets_no_system_column() {
        let refs = StatementRefs::every_system_column();
        let cases = [
            (
                tbl("plain", &[("a", ColumnType::Int4)]),
                SystemColumns {
                    tableoid: true,
                    ctid: true,
                },
            ),
            (
                tbl("shadowed", &[("ctid", ColumnType::Int4)]),
                SystemColumns {
                    tableoid: true,
                    ctid: false,
                },
            ),
            (
                tbl(
                    "both",
                    &[("ctid", ColumnType::Int4), ("tableoid", ColumnType::Int4)],
                ),
                SystemColumns {
                    tableoid: false,
                    ctid: false,
                },
            ),
        ];
        for (table, expected) in cases {
            assert!(
                SystemColumns::of(Some(&refs), &table) == expected,
                "{}",
                table.name
            );
        }
        // Nothing is carried for a statement that spells neither.
        assert!(
            SystemColumns::of(None, &tbl("plain", &[("a", ColumnType::Int4)]))
                == SystemColumns::default()
        );
    }

    /// The mapping is the row's storage identity split across the two fields a
    /// `tid` has. Nothing outside this test may depend on the values.
    #[test]
    fn a_row_ctid_is_its_identity_split_across_the_two_fields() {
        let cases = [
            (1, (0, 1)),
            (2, (0, 2)),
            (65_535, (0, 65_535)),
            (65_536, (1, 0)),
            (65_537, (1, 1)),
            (u64::from(u32::MAX) + 1, (65_536, 0)),
        ];
        for (identity, (block, offset)) in cases {
            let expected = Datum::Tid(crabka_pgtypes::Tid { block, offset });
            assert!(row_ctid(identity) == expected, "{identity}");
        }
        // Distinct up to the point the two fields stop separating identities,
        // which is past every row count a heap can address.
        assert!(row_ctid(1) != row_ctid(2));
        assert!(row_ctid(1 << 47) != row_ctid((1 << 47) + 1));
    }
}
