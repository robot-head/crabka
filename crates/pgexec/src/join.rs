//! SP33: joins over `Relation`s. A `Relation` is a `Scope` (ordered schema) plus
//! its materialized rows. Base tables, joins, and derived subqueries all produce
//! one. This module is pure relational algebra over already-fetched rows, with
//! no kv or catalog access, so hand-built relations can unit-test it. See the
//! SP33 design doc for why this single-range pure fold warrants no model.
//!
//! An equality-constrained join (`USING`/`NATURAL`, an `ON` whose top-level
//! conjuncts include `left.col = right.col`, or an `OR` whose every branch has
//! such a key) probes hash indexes over the right relation instead of walking it
//! per left row, so a 10k-row self-join costs thousands of predicate evaluations
//! rather than 100M. Unusable equality keys are left to the full predicate; a
//! join with no hash-comparable necessary key folds as a nested loop.

use std::collections::HashMap;

use crabka_pgparser::ast::{BinaryOp, Expr, JoinConstraint, JoinKind};
use crabka_pgtypes::{ColumnType, Datum};

use crate::{
    bind::BoundExpr,
    error::ExecError,
    scope::{ColumnBinding, Exposure, LIVE_QUALIFIER, Scope, StatementRefs, wants_whole_row},
};

/// A materialized relation: an ordered `Scope` (the schema) plus its rows, each
/// row positionally aligned to `scope.columns`. Base tables, joins, and, later,
/// derived subqueries all produce one.
#[derive(Debug, Clone)]
pub(crate) struct Relation {
    pub scope: Scope,
    pub rows: Vec<Vec<Datum>>,
}

/// What the statement, rather than the two relations, decides about a join.
///
/// Both fields are the enclosing statement's business and neither is derivable
/// from the rows being joined, which is why they travel together.
#[derive(Clone, Copy)]
pub(crate) struct JoinPolicy<'a> {
    /// Memory one blocking operator may retain before it reports 53200.
    pub(crate) memory: crabka_units::ByteSize,
    /// The statement's whole-row references, which decide the hidden liveness
    /// markers an outer join carries. `None` marks every qualifier; see
    /// [`missing_live_markers`].
    pub(crate) refs: Option<&'a StatementRefs>,
}

impl Default for JoinPolicy<'_> {
    /// The engine's own budget, and no statement to narrow markers by, so every
    /// qualifier an outer join can null-extend is marked.
    ///
    /// This is what a join built with no statement in hand gets: the
    /// schema-description path, whose relations have no rows for either field to
    /// bear on, and the unit tests below, which join hand-built relations.
    fn default() -> Self {
        Self {
            memory: crate::scanner::BLOCKING_QUERY_MEMORY,
            refs: None,
        }
    }
}

/// Join two relations under `kind` + `constraint`, returning the combined
/// relation. `ctx` carries the session zone + clock used to evaluate an `ON`
/// predicate that contains temporal expressions (a USING/NATURAL/CROSS join, or a
/// rows-free schema join, never touches it).
///
/// `policy` is what the enclosing statement decides: its memory budget and its
/// whole-row references, the latter deciding which hidden liveness markers an
/// outer join carries. See [`JoinPolicy`].
pub(crate) fn join_relations(
    left: Relation,
    right: Relation,
    kind: JoinKind,
    constraint: &JoinConstraint,
    ctx: &crate::clock::EvalCtx,
    policy: JoinPolicy<'_>,
) -> Result<Relation, ExecError> {
    join_relations_impl(left, &right, kind, constraint, ctx, policy, None)
}

pub(crate) struct PreparedJoinIndex {
    index: Option<JoinIndex>,
    estimated_bytes: usize,
}

pub(crate) fn prepare_join_index(
    left: &Relation,
    right: &Relation,
    constraint: &JoinConstraint,
    ctx: &crate::clock::EvalCtx,
    blocking_query_memory: crabka_units::ByteSize,
) -> Result<PreparedJoinIndex, ExecError> {
    let index = JoinIndex::build(left, right, constraint, ctx, blocking_query_memory)?;
    let estimated_bytes = index.as_ref().map_or(0, JoinIndex::estimated_bytes);
    Ok(PreparedJoinIndex {
        index,
        estimated_bytes,
    })
}

impl PreparedJoinIndex {
    /// A prepared join that carries no index, so probing falls back to scanning
    /// the right relation. Costs nothing to hold, which is what lets a lateral
    /// item memoize its inner relation under a memory budget too small for the
    /// index over the outer one.
    pub(crate) fn none() -> Self {
        Self {
            index: None,
            estimated_bytes: 0,
        }
    }

    pub(crate) fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }

    pub(crate) fn discard_index(&mut self) {
        self.index = None;
        self.estimated_bytes = 0;
    }
}

pub(crate) fn join_relations_prepared(
    left: Relation,
    right: &Relation,
    kind: JoinKind,
    constraint: &JoinConstraint,
    ctx: &crate::clock::EvalCtx,
    policy: JoinPolicy<'_>,
    prepared: &PreparedJoinIndex,
) -> Result<Relation, ExecError> {
    join_relations_impl(left, right, kind, constraint, ctx, policy, Some(prepared))
}

/// Count a join's output rows without materializing them. This is the same
/// candidate probe and full predicate recheck as [`join_relations`], including
/// duplicate suppression across indexed `OR` branches and outer-join padding.
pub(crate) fn count_join_rows(
    left: &Relation,
    right: &Relation,
    kind: JoinKind,
    constraint: &JoinConstraint,
    ctx: &crate::clock::EvalCtx,
    blocking_query_memory: crabka_units::ByteSize,
) -> Result<i64, ExecError> {
    let condition = JoinCondition::new(left, right, constraint)?;
    let track_right = matches!(kind, JoinKind::Right | JoinKind::Full);
    let right_match_bytes = if track_right { right.rows.len() } else { 0 };
    if crate::scanner::exceeds_query_memory(right_match_bytes, blocking_query_memory) {
        return Err(crate::scanner::memory_budget_exceeded());
    }

    let mut index = JoinIndex::build(left, right, constraint, ctx, blocking_query_memory)?;
    if index.as_ref().is_some_and(|index| {
        crate::scanner::exceeds_query_memory(
            right_match_bytes.saturating_add(index.estimated_bytes()),
            blocking_query_memory,
        )
    }) {
        index = None;
    }
    let mut key_buf = Vec::new();
    let mut candidate_buf =
        Vec::with_capacity(index.as_ref().map_or(0, JoinIndex::candidate_capacity));
    let mut right_matched = track_right.then(|| vec![false; right.rows.len()]);
    let keep_left = matches!(kind, JoinKind::Left | JoinKind::Full);
    let mut count = 0_i64;

    for left_row in &left.rows {
        let mut any = false;
        for right_index in candidate_rows(
            index.as_ref(),
            right.rows.len(),
            left_row,
            &mut key_buf,
            &mut candidate_buf,
            ctx,
        )? {
            if condition.matches(left_row, &right.rows[right_index], ctx)? {
                any = true;
                if let Some(right_matched) = &mut right_matched {
                    right_matched[right_index] = true;
                }
                increment_join_count(&mut count)?;
            }
        }
        if !any && keep_left {
            increment_join_count(&mut count)?;
        }
    }
    if let Some(right_matched) = right_matched {
        for matched in right_matched {
            if !matched {
                increment_join_count(&mut count)?;
            }
        }
    }
    Ok(count)
}

fn increment_join_count(count: &mut i64) -> Result<(), ExecError> {
    *count = count
        .checked_add(1)
        .ok_or(ExecError::Type(crabka_pgtypes::TypeError::Overflow))?;
    Ok(())
}

struct JoinCondition {
    combined_scope: Scope,
    join_cols: Vec<String>,
    pairs: Vec<(usize, usize)>,
    /// Bound against `combined_scope` ONCE here rather than resolved by name for
    /// every candidate pair — a nested loop evaluates this predicate
    /// `|left| × |right|` times.
    on_predicate: Option<BoundExpr>,
}

impl JoinCondition {
    fn new(
        left: &Relation,
        right: &Relation,
        constraint: &JoinConstraint,
    ) -> Result<Self, ExecError> {
        for column in &right.scope.columns {
            // A liveness marker's qualifier is the internal `$live`, which both
            // sides of a nested outer join carry and which names no relation, so
            // it is not the duplicate alias this rejects.
            if let Some(qualifier) = &column.qualifier
                && column.exposure != Exposure::LiveMarker
                && left
                    .scope
                    .columns
                    .iter()
                    .any(|left| left.qualifier.as_ref() == Some(qualifier))
            {
                return Err(ExecError::DuplicateAlias(qualifier.clone()));
            }
        }
        let mut combined_scope = left.scope.clone();
        combined_scope
            .columns
            .extend(right.scope.columns.iter().cloned());
        let join_cols = match constraint {
            JoinConstraint::Using(columns) => columns.clone(),
            JoinConstraint::Natural => natural_common_columns(&left.scope, &right.scope),
            JoinConstraint::On(_) | JoinConstraint::None => Vec::new(),
        };
        let mut pairs = Vec::with_capacity(join_cols.len());
        for column in &join_cols {
            pairs.push((
                left.scope.resolve(None, column)?,
                right.scope.resolve(None, column)?,
            ));
        }
        let on_predicate = match constraint {
            JoinConstraint::On(predicate) => Some(BoundExpr::new(predicate, &combined_scope)?),
            _ => None,
        };
        Ok(Self {
            combined_scope,
            join_cols,
            pairs,
            on_predicate,
        })
    }

    fn matches(
        &self,
        left: &[Datum],
        right: &[Datum],
        ctx: &crate::clock::EvalCtx,
    ) -> Result<bool, ExecError> {
        use std::cmp::Ordering;

        if !self.pairs.is_empty() {
            for (left_index, right_index) in &self.pairs {
                if crabka_pgtypes::ops::compare(&left[*left_index], &right[*right_index])?
                    != Some(Ordering::Equal)
                {
                    return Ok(false);
                }
            }
            return Ok(true);
        }
        let Some(predicate) = &self.on_predicate else {
            return Ok(true);
        };
        let mut combined = left.to_vec();
        combined.extend_from_slice(right);
        match crate::eval::eval(predicate.expr(), &self.combined_scope, &combined, ctx)? {
            Datum::Bool(value) => Ok(value),
            Datum::Null => Ok(false),
            _ => Err(ExecError::TypeMismatch(
                "JOIN/ON condition must be boolean".into(),
            )),
        }
    }
}

/// The qualifiers on a side an outer join can null-extend that a whole-row
/// reference can read and that carry no liveness marker yet, in row order.
///
/// A side that is itself an outer join already marks the qualifiers *it* can
/// invent, and null-extending the side nulls those markers along with everything
/// else in it, so a nested join adds only what is missing. A `USING`/`NATURAL`
/// join's merged columns belong to no relation and have no whole row, so they
/// need none.
///
/// A marker is only ever read by [`Scope::whole_row_value`], through a bare name
/// the statement spells, so a qualifier no name of the statement can reach needs
/// none either — which is what keeps an outer join in a query with no whole-row
/// reference exactly as wide as it was before markers existed. `refs` is
/// `None` wherever the statement is not known, and then every qualifier is
/// marked; see [`wants_whole_row`].
fn missing_live_markers(side: &Scope, refs: Option<&StatementRefs>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for column in &side.columns {
        if let Some(qualifier) = &column.qualifier
            && column.exposure != Exposure::LiveMarker
            && wants_whole_row(refs, qualifier)
            && side.live_marker(qualifier).is_none()
            && !out.contains(qualifier)
        {
            out.push(qualifier.clone());
        }
    }
    out
}

/// The hidden column carrying one qualifier's liveness. See [`LIVE_QUALIFIER`].
fn live_marker_binding(qualifier: &str) -> ColumnBinding {
    ColumnBinding {
        qualifier: Some(LIVE_QUALIFIER.to_string()),
        name: qualifier.to_string(),
        ty: ColumnType::Bool,
        exposure: Exposure::LiveMarker,
    }
}

/// A live side's marker value; a null-extended side's is NULL, like the rest of
/// the row the join invented for it.
const LIVE: Datum = Datum::Bool(true);

fn join_relations_impl(
    left: Relation,
    right: &Relation,
    kind: JoinKind,
    constraint: &JoinConstraint,
    ctx: &crate::clock::EvalCtx,
    policy: JoinPolicy<'_>,
    prepared: Option<&PreparedJoinIndex>,
) -> Result<Relation, ExecError> {
    let JoinPolicy {
        memory: blocking_query_memory,
        refs,
    } = policy;
    let condition = JoinCondition::new(&left, right, constraint)?;
    let lw = left.scope.width();

    // Only an outer join invents rows, so only an outer join adds markers: an
    // inner or cross join's output rows are exactly as wide as before. `Left`
    // keeps unmatched LEFT rows, which means it is the RIGHT side it invents.
    let (left_markers, right_markers) = match kind {
        JoinKind::Inner | JoinKind::Cross => (Vec::new(), Vec::new()),
        JoinKind::Left => (Vec::new(), missing_live_markers(&right.scope, refs)),
        JoinKind::Right => (missing_live_markers(&left.scope, refs), Vec::new()),
        JoinKind::Full => (
            missing_live_markers(&left.scope, refs),
            missing_live_markers(&right.scope, refs),
        ),
    };
    let (nl, nr) = (left_markers.len(), right_markers.len());
    // Appended left-side markers first, then right-side, after every real column
    // of the combined row.
    let mark = |row: &mut Vec<Datum>, left_live: &Datum, right_live: &Datum| {
        row.resize(row.len() + nl, left_live.clone());
        row.resize(row.len() + nr, right_live.clone());
    };

    let built_index = prepared
        .is_none()
        .then(|| JoinIndex::build(&left, right, constraint, ctx, blocking_query_memory))
        .transpose()?
        .flatten();
    let index = prepared.map_or(built_index.as_ref(), |prepared| prepared.index.as_ref());
    let mut key_buf: Vec<Datum> = Vec::new();
    let mut candidate_buf = Vec::with_capacity(index.map_or(0, JoinIndex::candidate_capacity));

    let mut rows = Vec::new();
    let mut result_bytes = 0usize;
    match kind {
        JoinKind::Inner | JoinKind::Cross => {
            for l in &left.rows {
                for ri in candidate_rows(
                    index,
                    right.rows.len(),
                    l,
                    &mut key_buf,
                    &mut candidate_buf,
                    ctx,
                )? {
                    let r = &right.rows[ri];
                    if condition.matches(l, r, ctx)? {
                        let mut row = l.clone();
                        row.extend(r.iter().cloned());
                        push_bounded_join_row(
                            &mut rows,
                            &mut result_bytes,
                            row,
                            blocking_query_memory,
                        )?;
                    }
                }
            }
        }
        JoinKind::Left | JoinKind::Right | JoinKind::Full => {
            let rw = right.scope.width();
            let want_left = matches!(kind, JoinKind::Left | JoinKind::Full);
            let want_right = matches!(kind, JoinKind::Right | JoinKind::Full);
            let mut right_matched = vec![false; right.rows.len()];
            for l in &left.rows {
                let mut any = false;
                for ri in candidate_rows(
                    index,
                    right.rows.len(),
                    l,
                    &mut key_buf,
                    &mut candidate_buf,
                    ctx,
                )? {
                    let r = &right.rows[ri];
                    if condition.matches(l, r, ctx)? {
                        any = true;
                        right_matched[ri] = true;
                        let mut row = l.clone();
                        row.extend(r.iter().cloned());
                        mark(&mut row, &LIVE, &LIVE);
                        push_bounded_join_row(
                            &mut rows,
                            &mut result_bytes,
                            row,
                            blocking_query_memory,
                        )?;
                    }
                }
                if !any && want_left {
                    let mut row = l.clone();
                    row.extend(vec![Datum::Null; rw]);
                    mark(&mut row, &LIVE, &Datum::Null);
                    push_bounded_join_row(
                        &mut rows,
                        &mut result_bytes,
                        row,
                        blocking_query_memory,
                    )?;
                }
            }
            if want_right {
                for (ri, r) in right.rows.iter().enumerate() {
                    if !right_matched[ri] {
                        let mut row = vec![Datum::Null; lw];
                        row.extend(r.iter().cloned());
                        mark(&mut row, &Datum::Null, &LIVE);
                        push_bounded_join_row(
                            &mut rows,
                            &mut result_bytes,
                            row,
                            blocking_query_memory,
                        )?;
                    }
                }
            }
        }
    }

    // The markers sit after every real column, in the order the rows carry them,
    // so binding the `ON` predicate against the marker-free combined scope above
    // resolved to the same indices it does here.
    let markers: Vec<ColumnBinding> = left_markers
        .iter()
        .chain(&right_markers)
        .map(|qualifier| live_marker_binding(qualifier))
        .collect();

    // USING/NATURAL: coalesce + reorder the join columns. Otherwise the combined
    // left ++ right schema is the result.
    if condition.pairs.is_empty() {
        let mut scope = condition.combined_scope;
        scope.columns.extend(markers);
        Ok(Relation { scope, rows })
    } else {
        Ok(coalesce_join_columns(
            &left.scope,
            &right.scope,
            kind,
            &condition.pairs,
            &condition.join_cols,
            rows,
            markers,
        ))
    }
}

/// The right rows a left row could possibly join with: the index's bucket for
/// its key, or — with no usable index — every right row.
enum CandidateRows<'a> {
    Indexed(std::slice::Iter<'a, usize>),
    All(std::ops::Range<usize>),
}

impl Iterator for CandidateRows<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Indexed(rows) => rows.next().copied(),
            Self::All(rows) => rows.next(),
        }
    }
}

fn candidate_rows<'a>(
    index: Option<&'a JoinIndex>,
    right_len: usize,
    lrow: &[Datum],
    key_buf: &mut Vec<Datum>,
    candidate_buf: &'a mut Vec<usize>,
    ctx: &crate::clock::EvalCtx,
) -> Result<CandidateRows<'a>, ExecError> {
    Ok(match index {
        Some(index) => {
            CandidateRows::Indexed(index.candidates(lrow, key_buf, candidate_buf, ctx)?.iter())
        }
        None => CandidateRows::All(0..right_len),
    })
}

enum JoinIndex {
    Conjunctive(EquiIndex),
    Disjunctive {
        indexes: Vec<EquiIndex>,
        right_len: usize,
    },
}

impl JoinIndex {
    const MAX_OR_DISJUNCTS: usize = 16;

    fn build(
        left: &Relation,
        right: &Relation,
        constraint: &JoinConstraint,
        ctx: &crate::clock::EvalCtx,
        blocking_query_memory: crabka_units::ByteSize,
    ) -> Result<Option<Self>, ExecError> {
        let mut combined = left.scope.clone();
        combined.columns.extend(right.scope.columns.iter().cloned());
        if let JoinConstraint::On(predicate) = constraint
            && matches!(
                predicate,
                Expr::Binary {
                    op: BinaryOp::Or,
                    ..
                }
            )
        {
            let mut disjuncts = Vec::new();
            collect_or_disjuncts(predicate, &mut disjuncts);
            if disjuncts.len() > Self::MAX_OR_DISJUNCTS {
                return Ok(None);
            }
            let mut indexes = Vec::with_capacity(disjuncts.len());
            let mut index_bytes = right
                .rows
                .len()
                .saturating_mul(std::mem::size_of::<usize>());
            for disjunct in disjuncts {
                let keys = equi_key_columns(disjunct, &combined, left.scope.width());
                let Some(plan) = EquiIndex::plan(left, right, &keys, ctx) else {
                    // A branch without a usable necessary equality can match any
                    // right row, so narrowing the union would be unsound.
                    return Ok(None);
                };
                index_bytes = index_bytes.saturating_add(plan.estimated_bytes);
                if crate::scanner::exceeds_query_memory(index_bytes, blocking_query_memory) {
                    return Ok(None);
                }
                indexes.push(EquiIndex::build_planned(right, plan));
            }
            let index = Self::Disjunctive {
                indexes,
                right_len: right.rows.len(),
            };
            return Ok((!crate::scanner::exceeds_query_memory(
                index.estimated_bytes(),
                blocking_query_memory,
            ))
            .then_some(index));
        }

        let keys = equi_keys_for(left, right, constraint)?;
        let Some(plan) = EquiIndex::plan(left, right, &keys, ctx) else {
            return Ok(None);
        };
        if crate::scanner::exceeds_query_memory(plan.estimated_bytes, blocking_query_memory) {
            return Ok(None);
        }
        let index = Self::Conjunctive(EquiIndex::build_planned(right, plan));
        Ok(
            (!crate::scanner::exceeds_query_memory(index.estimated_bytes(), blocking_query_memory))
                .then_some(index),
        )
    }

    fn candidate_capacity(&self) -> usize {
        match self {
            Self::Conjunctive(_) => 0,
            Self::Disjunctive { right_len, .. } => *right_len,
        }
    }

    fn estimated_bytes(&self) -> usize {
        match self {
            Self::Conjunctive(index) => index.estimated_bytes(),
            Self::Disjunctive { indexes, right_len } => indexes.iter().fold(
                right_len
                    .saturating_mul(std::mem::size_of::<usize>())
                    .saturating_add(
                        indexes
                            .capacity()
                            .saturating_mul(std::mem::size_of::<EquiIndex>()),
                    ),
                |bytes, index| bytes.saturating_add(index.estimated_bytes()),
            ),
        }
    }

    fn candidates<'a>(
        &'a self,
        lrow: &[Datum],
        key_buf: &mut Vec<Datum>,
        union: &'a mut Vec<usize>,
        ctx: &crate::clock::EvalCtx,
    ) -> Result<&'a [usize], ExecError> {
        match self {
            Self::Conjunctive(index) => index.candidates(lrow, key_buf, ctx),
            Self::Disjunctive { indexes, .. } => {
                union.clear();
                let mut lists = Vec::with_capacity(indexes.len());
                for index in indexes {
                    lists.push(index.candidates(lrow, key_buf, ctx)?);
                }
                let mut positions = [0usize; Self::MAX_OR_DISJUNCTS];
                while let Some(next) = lists
                    .iter()
                    .enumerate()
                    .filter_map(|(index, rows)| rows.get(positions[index]).copied())
                    .min()
                {
                    union.push(next);
                    for (index, rows) in lists.iter().enumerate() {
                        while rows.get(positions[index]) == Some(&next) {
                            positions[index] += 1;
                        }
                    }
                }
                Ok(union)
            }
        }
    }
}

fn equi_keys_for(
    left: &Relation,
    right: &Relation,
    constraint: &JoinConstraint,
) -> Result<Vec<(LeftKey, usize)>, ExecError> {
    let mut combined = left.scope.clone();
    combined.columns.extend(right.scope.columns.iter().cloned());
    match constraint {
        JoinConstraint::Using(columns) => columns
            .iter()
            .map(|column| {
                Ok((
                    LeftKey::Column(left.scope.resolve(None, column)?),
                    right.scope.resolve(None, column)?,
                ))
            })
            .collect(),
        JoinConstraint::Natural => natural_common_columns(&left.scope, &right.scope)
            .iter()
            .map(|column| {
                Ok((
                    LeftKey::Column(left.scope.resolve(None, column)?),
                    right.scope.resolve(None, column)?,
                ))
            })
            .collect(),
        JoinConstraint::On(predicate) => {
            Ok(equi_key_columns(predicate, &combined, left.scope.width()))
        }
        JoinConstraint::None => Ok(Vec::new()),
    }
}

fn collect_or_disjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    if let Expr::Binary {
        op: BinaryOp::Or,
        left,
        right,
    } = expr
    {
        collect_or_disjuncts(left, out);
        collect_or_disjuncts(right, out);
    } else {
        out.push(expr);
    }
}

/// Right-relation row indices grouped by their equality key, ascending within
/// each bucket so a probe visits candidates in the order the nested loop would.
///
/// `Datum`'s hash equality agrees with `ops::compare` only for values of the
/// SAME variant — `Int2(1)` and `Int4(1)` compare Equal but hash apart, and so
/// do `Int4(1)` and `Numeric(1)`. The index uses the subset of required equality
/// keys whose values are exactly hash-comparable; the full predicate rechecks
/// every candidate, including equality keys omitted here.
struct EquiIndex {
    /// How each key's value is read from a left row.
    left_key: Vec<LeftKey>,
    /// The left relation's schema, so an expression key can be evaluated
    /// against a left row during the probe.
    left_scope: Scope,
    buckets: HashMap<Vec<Datum>, Vec<usize>>,
}

struct EquiIndexPlan {
    keys: Vec<(LeftKey, usize)>,
    left_scope: Scope,
    estimated_bytes: usize,
}

/// What a key column's values look like on one side of the join.
enum KeyVariant<'a> {
    /// Every non-NULL value carries the variant of this sample.
    Uniform(&'a Datum),
    /// No non-NULL value at all. Nothing can match through this column, and the
    /// index represents that faithfully, because a NULL key is never bucketed.
    AllNull,
    /// Values of differing variants, whose hash equality would not agree with
    /// `ops::compare`.
    Mixed,
}

impl EquiIndex {
    /// Below this many left×right pairs the nested loop is already cheap enough
    /// that the buckets would cost more to build than the probes save.
    const MIN_PAIRS: usize = 4096;

    /// Plan an index without allocating its buckets. The estimate assumes every
    /// right row has a distinct key, which bounds the bucket/table overhead, and
    /// includes heap-backed scalar key payloads.
    fn plan(
        left: &Relation,
        right: &Relation,
        keys: &[(LeftKey, usize)],
        ctx: &crate::clock::EvalCtx,
    ) -> Option<EquiIndexPlan> {
        if keys.is_empty() || left.rows.len().saturating_mul(right.rows.len()) < Self::MIN_PAIRS {
            return None;
        }
        let keys: Vec<(LeftKey, usize)> = keys
            .iter()
            .filter(|(li, ri)| {
                // An expression key is materialized once here purely to check
                // that its values hash the way they compare; the probe
                // re-evaluates it per left row.
                let left_values = match li {
                    LeftKey::Column(lc) => LeftKeyValues::Column(*lc),
                    LeftKey::Expr(expr) => {
                        let mut values = Vec::with_capacity(left.rows.len());
                        for row in &left.rows {
                            match crate::eval::eval(expr, &left.scope, row, ctx) {
                                Ok(value) => values.push(value),
                                // An expression that cannot be evaluated off a
                                // left row alone is left to the full predicate.
                                Err(_) => return false,
                            }
                        }
                        LeftKeyValues::Materialized(values)
                    }
                };
                match (
                    left_values.variant(&left.rows),
                    key_variant(&right.rows, *ri),
                ) {
                    (KeyVariant::Mixed, _) | (_, KeyVariant::Mixed) => false,
                    (KeyVariant::Uniform(l), KeyVariant::Uniform(r))
                        if std::mem::discriminant(l) != std::mem::discriminant(r)
                            || !hashes_like_it_compares(l) =>
                    {
                        false
                    }
                    (KeyVariant::Uniform(_), KeyVariant::Uniform(_)) => true,
                    // With no non-NULL value on one side, no probe can hit a bucket
                    // whatever the other side holds — which is the right answer.
                    _ => true,
                }
            })
            .cloned()
            .collect();
        if keys.is_empty() {
            return None;
        }
        let mut estimated_bytes = keys.len().saturating_mul(std::mem::size_of::<usize>());
        for row in &right.rows {
            if keys.iter().any(|(_, rc)| row[*rc].is_null()) {
                continue;
            }
            let key_bytes = keys.iter().fold(0usize, |bytes, (_, rc)| {
                bytes.saturating_add(crate::scanner::datum_row_bytes(std::slice::from_ref(
                    &row[*rc],
                )))
            });
            estimated_bytes = estimated_bytes
                .saturating_add(key_bytes)
                // Worst-case one HashMap entry and one bucket allocation per row,
                // with spare vector/table capacity rather than logical lengths.
                .saturating_add(16 * std::mem::size_of::<usize>());
        }
        Some(EquiIndexPlan {
            keys,
            left_scope: left.scope.clone(),
            estimated_bytes,
        })
    }

    fn build_planned(right: &Relation, plan: EquiIndexPlan) -> Self {
        let keys = plan.keys;
        let mut buckets: HashMap<Vec<Datum>, Vec<usize>> = HashMap::new();
        for (ri, row) in right.rows.iter().enumerate() {
            // A NULL in the key never compares Equal, so the row is not indexed
            // and simply falls out as unmatched.
            if keys.iter().any(|(_, rc)| row[*rc].is_null()) {
                continue;
            }
            let key: Vec<Datum> = keys.iter().map(|(_, rc)| row[*rc].clone()).collect();
            buckets.entry(key).or_default().push(ri);
        }
        Self {
            left_key: keys.into_iter().map(|(lc, _)| lc).collect(),
            left_scope: plan.left_scope,
            buckets,
        }
    }

    /// `keys` are `(left_column, right_column)` pairs the predicate requires to
    /// compare Equal. Returns `None` when no index applies.
    #[cfg(test)]
    fn build(left: &Relation, right: &Relation, keys: &[(usize, usize)]) -> Option<Self> {
        let keys: Vec<(LeftKey, usize)> = keys
            .iter()
            .map(|(lc, rc)| (LeftKey::Column(*lc), *rc))
            .collect();
        let ctx = crate::clock::EvalCtx::test_default();
        Self::plan(left, right, &keys, &ctx).map(|plan| Self::build_planned(right, plan))
    }

    fn estimated_bytes(&self) -> usize {
        let table_bytes = self
            .buckets
            .capacity()
            .saturating_mul(4 * std::mem::size_of::<usize>());
        self.buckets.iter().fold(
            table_bytes.saturating_add(
                self.left_key
                    .capacity()
                    .saturating_mul(std::mem::size_of::<usize>()),
            ),
            |bytes, (key, rows)| {
                bytes
                    .saturating_add(crate::scanner::datum_row_bytes(key))
                    .saturating_add(
                        key.capacity()
                            .saturating_sub(key.len())
                            .saturating_mul(std::mem::size_of::<Datum>()),
                    )
                    .saturating_add(rows.capacity().saturating_mul(std::mem::size_of::<usize>()))
                    .saturating_add(10 * std::mem::size_of::<usize>())
            },
        )
    }

    fn candidates<'a>(
        &'a self,
        lrow: &[Datum],
        key_buf: &mut Vec<Datum>,
        ctx: &crate::clock::EvalCtx,
    ) -> Result<&'a [usize], ExecError> {
        key_buf.clear();
        for key in &self.left_key {
            let value = match key {
                LeftKey::Column(lc) => lrow[*lc].clone(),
                LeftKey::Expr(expr) => crate::eval::eval(expr, &self.left_scope, lrow, ctx)?,
            };
            // A NULL key never compares Equal, so no bucket can hold a match.
            if value.is_null() {
                return Ok(&[]);
            }
            key_buf.push(value);
        }
        Ok(match self.buckets.get(key_buf.as_slice()) {
            Some(rows) => rows,
            None => &[],
        })
    }
}

fn key_variant(rows: &[Vec<Datum>], column: usize) -> KeyVariant<'_> {
    key_variant_of(rows.iter().map(|row| &row[column]))
}

fn key_variant_of<'a>(values: impl Iterator<Item = &'a Datum>) -> KeyVariant<'a> {
    let mut seen: Option<&Datum> = None;
    for value in values {
        if value.is_null() {
            continue;
        }
        match seen {
            None => seen = Some(value),
            Some(sample) if std::mem::discriminant(sample) == std::mem::discriminant(value) => {}
            Some(_) => return KeyVariant::Mixed,
        }
    }
    seen.map_or(KeyVariant::AllNull, KeyVariant::Uniform)
}

/// A key's left-side values during planning: read straight from a column, or
/// materialized once from an expression so its variant can be checked.
enum LeftKeyValues {
    Column(usize),
    Materialized(Vec<Datum>),
}

impl LeftKeyValues {
    fn variant<'a>(&'a self, rows: &'a [Vec<Datum>]) -> KeyVariant<'a> {
        match self {
            Self::Column(column) => key_variant(rows, *column),
            Self::Materialized(values) => key_variant_of(values.iter()),
        }
    }
}

/// Whether `Datum`'s `Eq`/`Hash` decide this variant exactly as `ops::compare`
/// does, which is what lets a hash bucket stand in for the comparison.
///
/// The scalar types agree by construction (`Eq` and `Hash` both canonicalize
/// NaN, signed zero, and numeric scale the way `compare` orders them). The
/// composite types do not: `array_cmp` ignores the element type, so `int4[]`
/// `{1}` and `int8[]` `{1}` compare Equal while `Eq` calls them different — and
/// `interval` compares by a canonical estimate. Those keys keep the nested loop.
pub(crate) fn hashes_like_it_compares(sample: &Datum) -> bool {
    matches!(
        sample,
        Datum::Bool(_)
            | Datum::Int2(_)
            | Datum::Int4(_)
            | Datum::Int8(_)
            | Datum::Float4(_)
            | Datum::Float8(_)
            | Datum::Numeric(_)
            | Datum::Text(_)
            | Datum::Bytea(_)
            | Datum::Date(_)
            | Datum::Time(_)
            | Datum::Timestamp(_)
            | Datum::Timestamptz(_)
    )
}

/// The `(left_column, right_column)` pairs an `ON` predicate requires to compare
/// Equal: its top-level `AND` conjuncts of the form `l.col = r.col`, with one
/// side resolving into the left relation and the other into the right.
///
/// These are necessary conditions for the predicate to hold, which is what makes
/// them safe as a pre-filter — the full predicate still decides every candidate.
/// Conjuncts of any other shape (including `OR`, which is not a necessary
/// condition) contribute no key.
fn equi_key_columns(pred: &Expr, combined: &Scope, lw: usize) -> Vec<(LeftKey, usize)> {
    let mut keys = Vec::new();
    collect_equi_key_columns(pred, combined, lw, &mut keys);
    keys
}

fn collect_equi_key_columns(
    pred: &Expr,
    combined: &Scope,
    lw: usize,
    out: &mut Vec<(LeftKey, usize)>,
) {
    match pred {
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            collect_equi_key_columns(left, combined, lw, out);
            collect_equi_key_columns(right, combined, lw, out);
        }
        Expr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } => {
            match (
                combined_column_index(left, combined),
                combined_column_index(right, combined),
            ) {
                (Some(a), Some(b)) => match (a < lw, b < lw) {
                    (true, false) => out.push((LeftKey::Column(a), b - lw)),
                    (false, true) => out.push((LeftKey::Column(b), a - lw)),
                    _ => {}
                },
                // One side is a bare right column and the other an expression
                // over left columns only: `c.unique2 = coalesce(a.x, b.y)` keys
                // the index on the expression's value, evaluated per left row.
                (None, Some(b)) if b >= lw && reads_only_left(left, combined, lw) => {
                    out.push((LeftKey::Expr(left.as_ref().clone()), b - lw));
                }
                (Some(a), None) if a >= lw && reads_only_left(right, combined, lw) => {
                    out.push((LeftKey::Expr(right.as_ref().clone()), a - lw));
                }
                _ => {}
            }
        }
        _ => {}
    }
}

/// How a hash key's value is obtained from a left row: a column read, or an
/// expression over left columns evaluated per row.
#[derive(Clone)]
enum LeftKey {
    Column(usize),
    Expr(Expr),
}

/// The combined-scope position of a bare column reference, or `None` for any
/// other expression. That includes an expression that does not resolve, or that
/// resolves ambiguously. `matches` reports that error when it evaluates the
/// predicate.
fn combined_column_index(expr: &Expr, combined: &Scope) -> Option<usize> {
    let Expr::Column { table, name } = expr else {
        return None;
    };
    combined.resolve(table.as_deref(), name).ok()
}

/// Whether `expr` reads only left-relation columns, so its value is fixed by a
/// left row alone and can key the hash index. Every column must resolve to the
/// left half of the combined scope; an unresolvable or ambiguous reference
/// disqualifies it, and so does a subquery, whose evaluation needs a planner
/// context the probe does not carry.
fn reads_only_left(expr: &Expr, combined: &Scope, lw: usize) -> bool {
    let mut only_left = true;
    crate::grouping::visit_expr(expr, &mut |node: &Expr| match node {
        Expr::Column { table, name } => match combined.resolve(table.as_deref(), name) {
            Ok(index) if index < lw => {}
            _ => only_left = false,
        },
        Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery { .. } => only_left = false,
        _ => {}
    });
    only_left
}

fn push_bounded_join_row(
    rows: &mut Vec<Vec<Datum>>,
    used: &mut usize,
    row: Vec<Datum>,
    blocking_query_memory: crabka_units::ByteSize,
) -> Result<(), ExecError> {
    let bytes = crate::scanner::datum_row_bytes(&row);
    if crate::scanner::exceeds_query_memory(used.saturating_add(bytes), blocking_query_memory) {
        return Err(crate::scanner::memory_budget_exceeded());
    }
    *used += bytes;
    rows.push(row);
    Ok(())
}

/// The column names common to both scopes (matched by name), in left order,
/// deduplicated. Drives `NATURAL JOIN`'s join-column set (empty => degenerates to
/// a cross join, per PostgreSQL).
/// A join input of an earlier `USING`/`NATURAL` join is not a column of the
/// relation — a bare reference to its name reaches the merged column instead —
/// so commonality is computed over the visible columns only. Otherwise `a JOIN b
/// USING (x) NATURAL JOIN c` would match `c.x` against the retained `a.x` and
/// `b.x` as well as the merged `x`.
fn natural_common_columns(left: &Scope, right: &Scope) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for c in left.columns.iter().filter(|c| !c.is_join_input()) {
        if right
            .columns
            .iter()
            .any(|rc| !rc.is_join_input() && rc.name == c.name)
            && !out.contains(&c.name)
        {
            out.push(c.name.clone());
        }
    }
    out
}

/// Which side's column `PostgreSQL` reuses as a `USING`/`NATURAL` join's merged
/// column, per `buildMergedJoinVar`.
///
/// An INNER or LEFT join takes the left input's column and a RIGHT join the
/// right input's — the merged column *is* that side's variable, not a copy of
/// it. Only a FULL join needs a real `COALESCE`, because there either side can
/// be the null-extended one. The values agree whichever rule is applied, since a
/// `USING` key joins on `=` and a NULL never matches; what the identity decides
/// is whether `SELECT x … GROUP BY ja.x` names one variable or two.
#[derive(Clone, Copy, PartialEq, Eq)]
enum MergedFrom {
    Left,
    Right,
    Coalesce,
}

impl MergedFrom {
    fn of(kind: JoinKind) -> Self {
        match kind {
            JoinKind::Inner | JoinKind::Cross | JoinKind::Left => Self::Left,
            JoinKind::Right => Self::Right,
            JoinKind::Full => Self::Coalesce,
        }
    }
}

/// Reshape a `left ++ right` combined relation into PostgreSQL's USING/NATURAL
/// output.
///
/// The merged join columns come FIRST, in `USING` order and unqualified: that is
/// what a bare `x` and a `SELECT *` see. Both sides' raw columns then follow
/// whole, each in its own declaration order, marked [`Exposure::JoinInput`] so
/// that only a qualified reference reaches them.
///
/// Keeping them is the point. `PostgreSQL` leaves the base range-table entries
/// intact and adds the merged list on top, so `ja.x` still names the left side's
/// own column. Dropping them made `SELECT ja.*` return a row one column short
/// with no error at all; and on the nullable side of an outer join the raw
/// column genuinely differs from the merged one — over `ja LEFT JOIN jb USING
/// (x)` a null-extended row has `x` = 2 while `jb.x` is NULL, which is what
/// makes the `WHERE jb.x IS NULL` anti-join idiom select anything.
///
/// One flat list cannot serve both orders, which is why the merged columns are
/// separate entries rather than the raw ones renamed: `SELECT *` over `m1 JOIN
/// m2 USING (b, a)` yields `b, a, c, d`, while `SELECT m1.*` over that same join
/// yields `a, b, c`. The cost is one extra datum per row per join column.
fn coalesce_join_columns(
    left_scope: &Scope,
    right_scope: &Scope,
    kind: JoinKind,
    pairs: &[(usize, usize)], // (left_idx, right_idx) per join column, in join order
    join_names: &[String],
    rows: Vec<Vec<Datum>>, // combined left ++ right ++ liveness-marker rows
    markers: Vec<ColumnBinding>,
) -> Relation {
    let lw = left_scope.width();
    let merged_width = pairs.len();
    let merged_from = MergedFrom::of(kind);
    // Where each input column lands once the merged ones are in front.
    let left_at = |i: usize| merged_width + i;
    let right_at = |i: usize| merged_width + lw + i;

    // The merged column takes its identity side's type. USING/NATURAL keys are
    // the same type on both sides in this slice's tested surface; PG unifies
    // left/right types for a mixed-width key (e.g. `int4` USING `int8`) — that
    // unification is deferred.
    let mut columns: Vec<ColumnBinding> =
        Vec::with_capacity(merged_width + lw + right_scope.width());
    for ((li, ri), name) in pairs.iter().zip(join_names) {
        columns.push(ColumnBinding {
            qualifier: None,
            name: name.clone(),
            ty: match merged_from {
                MergedFrom::Right => right_scope.ty_at(*ri),
                MergedFrom::Left | MergedFrom::Coalesce => left_scope.ty_at(*li),
            },
            exposure: Exposure::Output,
        });
    }
    // Both sides whole. An input a side inherited from an earlier USING join
    // keeps its own link, rebased onto the new indices, so a chained `ja JOIN jb
    // USING (x) JOIN jc USING (x)` still collapses `ja.x` onto the outermost
    // merged column.
    let rebase = |c: &ColumnBinding, offset: usize| match c.exposure {
        Exposure::JoinInput {
            merged: Some(merged),
        } => Exposure::JoinInput {
            merged: Some(offset + merged),
        },
        other => other,
    };
    for c in &left_scope.columns {
        columns.push(ColumnBinding {
            exposure: rebase(c, merged_width),
            ..c.clone()
        });
    }
    for c in &right_scope.columns {
        columns.push(ColumnBinding {
            exposure: rebase(c, merged_width + lw),
            ..c.clone()
        });
    }
    // Demote each side's join column: a bare name and `*` must see the merged
    // one instead. Its `merged` link records whether PostgreSQL treats the two
    // as one variable (see [`MergedFrom`]).
    for (join_index, (li, ri)) in pairs.iter().enumerate() {
        columns[left_at(*li)].exposure = Exposure::JoinInput {
            merged: (merged_from == MergedFrom::Left).then_some(join_index),
        };
        columns[right_at(*ri)].exposure = Exposure::JoinInput {
            merged: (merged_from == MergedFrom::Right).then_some(join_index),
        };
    }
    // Last, as in the rows: prepending the merged columns shifts every real
    // column by `merged_width` and leaves the markers where they were, at the
    // end of both the scope and the row.
    columns.extend(markers);
    let scope = Scope { columns };

    let new_rows = rows
        .into_iter()
        .map(|mut row| {
            let mut out: Vec<Datum> = Vec::with_capacity(scope.width());
            for (li, ri) in pairs {
                let left = &row[*li];
                let right = &row[lw + *ri];
                out.push(match merged_from {
                    MergedFrom::Left => left.clone(),
                    MergedFrom::Right => right.clone(),
                    MergedFrom::Coalesce if left.is_null() => right.clone(),
                    MergedFrom::Coalesce => left.clone(),
                });
            }
            out.append(&mut row);
            out
        })
        .collect();
    Relation {
        scope,
        rows: new_rows,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgtypes::{ArrayValue, ColumnType, ElemType};
    use crabka_units::prelude::ByteSizeExt as _;

    use super::*;

    /// A default (UTC/epoch) eval context. These pure relational-algebra tests
    /// use no temporal ON predicate, so the zone never affects the result.
    fn tctx() -> crate::clock::EvalCtx {
        crate::clock::EvalCtx::test_default()
    }

    fn join_relations(
        left: Relation,
        right: Relation,
        kind: JoinKind,
        constraint: &JoinConstraint,
        ctx: &crate::clock::EvalCtx,
    ) -> Result<Relation, ExecError> {
        super::join_relations(left, right, kind, constraint, ctx, JoinPolicy::default())
    }

    fn rel(qual: &str, cols: &[&str], rows: Vec<Vec<i32>>) -> Relation {
        let scope = Scope {
            columns: cols
                .iter()
                .map(|n| crate::scope::ColumnBinding {
                    exposure: crate::scope::Exposure::Output,
                    qualifier: Some(qual.into()),
                    name: (*n).into(),
                    ty: ColumnType::Int4,
                })
                .collect(),
        };
        Relation {
            scope,
            rows: rows
                .into_iter()
                .map(|r| r.into_iter().map(Datum::Int4).collect())
                .collect(),
        }
    }

    fn on_eq(lq: &str, lc: &str, rq: &str, rc: &str) -> JoinConstraint {
        JoinConstraint::On(Expr::Binary {
            op: crabka_pgparser::ast::BinaryOp::Eq,
            left: Box::new(Expr::Column {
                table: Some(lq.into()),
                name: lc.into(),
            }),
            right: Box::new(Expr::Column {
                table: Some(rq.into()),
                name: rc.into(),
            }),
        })
    }

    /// `ON right.k = <expression over left columns>` keys the hash index on the
    /// expression's value. PostgreSQL resolves such a join the same way, and the
    /// upstream `join` test's bug #8591 query is unusable without it: three
    /// 10k-row scans is 10^12 pairs as a nested loop.
    #[test]
    fn an_expression_over_left_columns_keys_the_index() {
        // 80x80 clears MIN_PAIRS, so the index is planned rather than skipped.
        let left = rel(
            "a",
            &["x", "y"],
            (0..80).map(|i| vec![i % 13, i % 7]).collect(),
        );
        let right = rel("b", &["k"], (0..80).map(|i| vec![i % 19]).collect());
        // ON b.k = a.x + a.y
        let constraint = JoinConstraint::On(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Column {
                table: Some("b".into()),
                name: "k".into(),
            }),
            right: Box::new(Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Column {
                    table: Some("a".into()),
                    name: "x".into(),
                }),
                right: Box::new(Expr::Column {
                    table: Some("a".into()),
                    name: "y".into(),
                }),
            }),
        });

        // The index must actually be built on the expression, not skipped.
        let index = JoinIndex::build(
            &left,
            &right,
            &constraint,
            &tctx(),
            crate::scanner::BLOCKING_QUERY_MEMORY,
        )
        .expect("valid join constraint")
        .expect("an expression key is indexable");
        assert2::assert!(matches!(
            &index,
            JoinIndex::Conjunctive(equi) if matches!(equi.left_key.as_slice(), [LeftKey::Expr(_)])
        ));

        // And it must agree with the nested loop it replaces, for every kind.
        for kind in [JoinKind::Inner, JoinKind::Left] {
            let expected: Vec<Vec<Datum>> =
                brute_force_join(&left, &right, kind, |l, r| match (&l[0], &l[1], &r[0]) {
                    (Datum::Int4(x), Datum::Int4(y), Datum::Int4(k)) => *k == x + y,
                    _ => false,
                });
            let actual = visible(
                &join_relations(left.clone(), right.clone(), kind, &constraint, &tctx())
                    .expect("indexed join"),
            );
            assert2::assert!(actual == expected, "{kind:?}");
        }
    }

    /// The rows as a query sees them.
    ///
    /// An outer join's liveness markers are internal bookkeeping, not columns of
    /// the relation — a bare name and `*` both skip them — so what a join
    /// produces is compared without them.
    fn visible(relation: &Relation) -> Vec<Vec<Datum>> {
        let keep: Vec<usize> = relation
            .scope
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.exposure != Exposure::LiveMarker)
            .map(|(i, _)| i)
            .collect();
        relation
            .rows
            .iter()
            .map(|row| keep.iter().map(|i| row[*i].clone()).collect())
            .collect()
    }

    /// The nested loop the index is supposed to be indistinguishable from.
    fn brute_force_join(
        left: &Relation,
        right: &Relation,
        kind: JoinKind,
        matches: impl Fn(&[Datum], &[Datum]) -> bool,
    ) -> Vec<Vec<Datum>> {
        let mut rows = Vec::new();
        for l in &left.rows {
            let mut any = false;
            for r in &right.rows {
                if matches(l, r) {
                    any = true;
                    let mut row = l.clone();
                    row.extend(r.iter().cloned());
                    rows.push(row);
                }
            }
            if !any && kind == JoinKind::Left {
                let mut row = l.clone();
                row.extend(right.scope.columns.iter().map(|_| Datum::Null));
                rows.push(row);
            }
        }
        rows
    }

    fn on_or_eq(lq: &str, left_cols: &[&str], rq: &str, rc: &str) -> JoinConstraint {
        let mut branches = left_cols.iter().map(|lc| Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Column {
                table: Some(lq.into()),
                name: (*lc).into(),
            }),
            right: Box::new(Expr::Column {
                table: Some(rq.into()),
                name: rc.into(),
            }),
        });
        let first = branches.next().expect("at least one OR branch");
        JoinConstraint::On(branches.fold(first, |left, right| Expr::Binary {
            op: BinaryOp::Or,
            left: Box::new(left),
            right: Box::new(right),
        }))
    }

    #[test]
    fn inner_join_keeps_only_matches() {
        let a = rel("a", &["id"], vec![vec![1], vec![2], vec![3]]);
        let b = rel("b", &["id"], vec![vec![2], vec![3], vec![4]]);
        let j = join_relations(a, b, JoinKind::Inner, &on_eq("a", "id", "b", "id"), &tctx())
            .expect("join");
        assert_eq!(
            j.rows,
            vec![
                vec![Datum::Int4(2), Datum::Int4(2)],
                vec![Datum::Int4(3), Datum::Int4(3)]
            ]
        );
    }

    #[test]
    fn cross_join_is_the_product() {
        let a = rel("a", &["x"], vec![vec![1], vec![2]]);
        let b = rel("b", &["y"], vec![vec![9]]);
        let j = join_relations(a, b, JoinKind::Cross, &JoinConstraint::None, &tctx())
            .expect("cross join");
        assert_eq!(j.rows.len(), 2);
        assert_eq!(j.scope.width(), 2);
    }

    #[test]
    fn cross_join_rejects_result_before_memory_budget_is_crossed() {
        let scope = |qualifier: &str| Scope {
            columns: vec![crate::scope::ColumnBinding {
                exposure: crate::scope::Exposure::Output,
                qualifier: Some(qualifier.into()),
                name: "value".into(),
                ty: ColumnType::Text,
            }],
        };
        let wide = "x".repeat(5 * 1024 * 1024);
        let left = Relation {
            scope: scope("l"),
            rows: vec![vec![Datum::Text(wide.clone())], vec![Datum::Text(wide)]],
        };
        let right = Relation {
            scope: scope("r"),
            rows: vec![vec![Datum::Text("a".into())], vec![Datum::Text("b".into())]],
        };

        let error = join_relations(left, right, JoinKind::Cross, &JoinConstraint::None, &tctx())
            .expect_err("join output must respect blocking memory budget");

        assert_eq!(error.into_pg().code, "53200");
    }

    #[test]
    fn left_join_null_extends_unmatched_left_rows() {
        let a = rel("a", &["id"], vec![vec![1], vec![2], vec![3]]);
        let b = rel("b", &["id"], vec![vec![2], vec![3]]);
        let j = join_relations(a, b, JoinKind::Left, &on_eq("a", "id", "b", "id"), &tctx())
            .expect("left join");
        // id=1 has no match -> (1, NULL); 2,3 match.
        assert!(visible(&j).contains(&vec![Datum::Int4(1), Datum::Null]));
        assert!(visible(&j).len() == 3);
    }

    #[test]
    fn right_join_null_extends_unmatched_right_rows() {
        let a = rel("a", &["id"], vec![vec![2]]);
        let b = rel("b", &["id"], vec![vec![1], vec![2]]);
        let j = join_relations(a, b, JoinKind::Right, &on_eq("a", "id", "b", "id"), &tctx())
            .expect("right join");
        assert!(visible(&j).contains(&vec![Datum::Null, Datum::Int4(1)]));
        assert!(visible(&j).len() == 2);
    }

    #[test]
    fn full_join_keeps_unmatched_from_both_sides() {
        let a = rel("a", &["id"], vec![vec![1], vec![2]]);
        let b = rel("b", &["id"], vec![vec![2], vec![3]]);
        let j = join_relations(a, b, JoinKind::Full, &on_eq("a", "id", "b", "id"), &tctx())
            .expect("full join");
        assert!(visible(&j).contains(&vec![Datum::Int4(1), Datum::Null])); // unmatched left
        assert!(visible(&j).contains(&vec![Datum::Null, Datum::Int4(3)])); // unmatched right
        assert!(visible(&j).contains(&vec![Datum::Int4(2), Datum::Int4(2)])); // matched
        assert!(visible(&j).len() == 3);
    }

    #[test]
    fn using_join_coalesces_the_column_first_and_unqualified() {
        let a = rel("a", &["id", "av"], vec![vec![1, 10], vec![2, 20]]);
        let b = rel("b", &["id", "bv"], vec![vec![2, 200], vec![3, 300]]);
        let j = join_relations(
            a,
            b,
            JoinKind::Inner,
            &JoinConstraint::Using(vec!["id".into()]),
            &tctx(),
        )
        .expect("using");
        // What `SELECT *` sees: the merged unqualified `id` first, then a.av,
        // then b.bv. Both sides' own `id` is kept but skipped here.
        let visible: Vec<&str> = j
            .scope
            .columns
            .iter()
            .filter(|c| !c.is_join_input())
            .map(|c| c.name.as_str())
            .collect();
        assert!(visible == vec!["id", "av", "bv"]);
        assert!(j.scope.columns[0].qualifier.is_none());
        // A bare name reaches the merged column; each side's own `id` is still
        // reachable under its qualifier, which is what `a.*` and `SELECT a` read.
        assert!(j.scope.resolve(None, "id") == Ok(0));
        assert!(j.scope.resolve(Some("a"), "id") == Ok(1));
        assert!(j.scope.resolve(Some("b"), "id") == Ok(3));
        assert!(j.scope.whole_row("a") == Some(vec![1, 2]));
        assert!(j.scope.whole_row("b") == Some(vec![3, 4]));
        assert!(
            j.rows
                == vec![vec![
                    Datum::Int4(2),
                    Datum::Int4(2),
                    Datum::Int4(20),
                    Datum::Int4(2),
                    Datum::Int4(200),
                ]]
        );
    }

    /// Which side's column `PostgreSQL` reuses as the merged one, per join kind.
    ///
    /// The values agree whichever side is picked — a `USING` key joins on `=`,
    /// so a matched row has both sides equal and an unmatched one has the other
    /// side NULL. What the identity decides is whether the merged column and the
    /// side's own column are ONE variable, which is what makes `SELECT x … GROUP
    /// BY ja.x` grouped-valid over a LEFT join and 42803 over a FULL one.
    #[test]
    fn merged_column_takes_its_side_from_the_join_kind() {
        // (kind, index the LEFT input's `id` collapses onto, and the RIGHT's)
        let cases: Vec<(JoinKind, Option<usize>, Option<usize>)> = vec![
            (JoinKind::Inner, Some(0), None),
            (JoinKind::Left, Some(0), None),
            (JoinKind::Right, None, Some(0)),
            (JoinKind::Full, None, None),
        ];
        for (kind, left_merged, right_merged) in cases {
            let a = rel("a", &["id", "av"], vec![vec![1, 10], vec![2, 20]]);
            let b = rel("b", &["id", "bv"], vec![vec![2, 200], vec![3, 300]]);
            let j = join_relations(
                a,
                b,
                kind,
                &JoinConstraint::Using(vec!["id".into()]),
                &tctx(),
            )
            .expect("using join");
            let left = j.scope.resolve(Some("a"), "id").expect("a.id");
            let right = j.scope.resolve(Some("b"), "id").expect("b.id");
            assert!(
                j.scope.canonical(left) == left_merged.unwrap_or(left),
                "{kind:?}: a.id"
            );
            assert!(
                j.scope.canonical(right) == right_merged.unwrap_or(right),
                "{kind:?}: b.id"
            );
        }
    }

    /// A FULL join really does need the `COALESCE`: neither side's raw column
    /// carries the merged value on every row.
    #[test]
    fn full_join_using_merges_a_value_neither_side_holds_alone() {
        let a = rel("a", &["id"], vec![vec![1], vec![2]]);
        let b = rel("b", &["id"], vec![vec![2], vec![3]]);
        let j = join_relations(
            a,
            b,
            JoinKind::Full,
            &JoinConstraint::Using(vec!["id".into()]),
            &tctx(),
        )
        .expect("full using");
        // [merged id, a.id, b.id] — the merged column is 1 where b.id is NULL
        // and 3 where a.id is NULL, so it equals neither raw column throughout.
        let mut rows = visible(&j);
        rows.sort_by_key(|r| format!("{:?}", r[0]));
        assert!(
            rows == vec![
                vec![Datum::Int4(1), Datum::Int4(1), Datum::Null],
                vec![Datum::Int4(2), Datum::Int4(2), Datum::Int4(2)],
                vec![Datum::Int4(3), Datum::Null, Datum::Int4(3)],
            ]
        );
    }

    #[test]
    fn natural_join_uses_all_common_columns() {
        let a = rel("a", &["id"], vec![vec![1], vec![2]]);
        let b = rel("b", &["id"], vec![vec![2], vec![3]]);
        let j = join_relations(a, b, JoinKind::Inner, &JoinConstraint::Natural, &tctx())
            .expect("natural");
        // One merged `id` that a bare name and `*` see, and both sides' own `id`
        // kept behind their qualifiers so `a.id`, `a.*` and `SELECT a` still work.
        assert!(j.scope.columns.len() == 3);
        assert!(j.scope.resolve(None, "id") == Ok(0));
        assert!(j.scope.resolve(Some("a"), "id") == Ok(1));
        assert!(j.scope.resolve(Some("b"), "id") == Ok(2));
        assert!(j.rows == vec![vec![Datum::Int4(2), Datum::Int4(2), Datum::Int4(2)]]);
    }

    #[test]
    fn left_join_using_coalesces_unmatched_to_left_value() {
        // LEFT JOIN USING: an unmatched left row keeps its own join-key value (the
        // right side is NULL, so COALESCE picks the left).
        let a = rel("a", &["id", "av"], vec![vec![1, 10], vec![2, 20]]);
        let b = rel("b", &["id", "bv"], vec![vec![2, 200]]);
        let j = join_relations(
            a,
            b,
            JoinKind::Left,
            &JoinConstraint::Using(vec!["id".into()]),
            &tctx(),
        )
        .expect("left using");
        // Row shape is [merged id, a.id, a.av, b.id, b.bv]. The unmatched left
        // row is the case that separates the merged column from the raw ones:
        // `id` is 1 because LEFT takes the left input's value, while `b.id` is
        // NULL because that side really was null-extended. Pointing `b.id` at
        // the merged column would pass every inner-join test and break the
        // `WHERE b.id IS NULL` anti-join idiom.
        assert!(
            visible(&j)
                == vec![
                    vec![
                        Datum::Int4(1),
                        Datum::Int4(1),
                        Datum::Int4(10),
                        Datum::Null,
                        Datum::Null,
                    ],
                    vec![
                        Datum::Int4(2),
                        Datum::Int4(2),
                        Datum::Int4(20),
                        Datum::Int4(2),
                        Datum::Int4(200),
                    ],
                ]
        );
    }

    /// A relation whose single key column holds `keys`, with `None` for NULL.
    fn keyed(qualifier: &str, keys: &[Option<i32>]) -> Relation {
        Relation {
            scope: Scope {
                columns: vec![crate::scope::ColumnBinding {
                    exposure: crate::scope::Exposure::Output,
                    qualifier: Some(qualifier.into()),
                    name: "k".into(),
                    ty: ColumnType::Int4,
                }],
            },
            rows: keys
                .iter()
                .map(|k| vec![k.map_or(Datum::Null, Datum::Int4)])
                .collect(),
        }
    }

    /// An independent double loop over the same inputs. This is the answer the
    /// indexed probe has to reproduce exactly, in rows and in order.
    fn reference_join(left: &Relation, right: &Relation, kind: JoinKind) -> Vec<Vec<Datum>> {
        let matches = |l: &Datum, r: &Datum| !l.is_null() && !r.is_null() && l == r;
        let mut rows: Vec<Vec<Datum>> = Vec::new();
        let mut right_matched = vec![false; right.rows.len()];
        for l in &left.rows {
            let mut any = false;
            for (ri, r) in right.rows.iter().enumerate() {
                if matches(&l[0], &r[0]) {
                    any = true;
                    right_matched[ri] = true;
                    rows.push(vec![l[0].clone(), r[0].clone()]);
                }
            }
            if !any && matches!(kind, JoinKind::Left | JoinKind::Full) {
                rows.push(vec![l[0].clone(), Datum::Null]);
            }
        }
        if matches!(kind, JoinKind::Right | JoinKind::Full) {
            for (ri, r) in right.rows.iter().enumerate() {
                if !right_matched[ri] {
                    rows.push(vec![Datum::Null, r[0].clone()]);
                }
            }
        }
        rows
    }

    /// The indexed probe is an optimization, not a semantic change. Over a
    /// relation pair big enough to take it, with duplicate keys, NULLs, and
    /// unmatched rows on both sides, every join kind returns exactly what the
    /// double loop returns, in the same order.
    #[test]
    fn indexed_equi_join_agrees_with_the_nested_loop() {
        let left_keys: Vec<Option<i32>> =
            (0..120i32).map(|i| (i % 7 != 0).then_some(i % 5)).collect();
        let right_keys: Vec<Option<i32>> = (0..120i32)
            .map(|i| (i % 11 != 0).then_some(i % 6))
            .collect();
        // Big enough that `EquiIndex::build` engages rather than declining.
        assert2::assert!(left_keys.len() * right_keys.len() >= EquiIndex::MIN_PAIRS);

        for kind in [
            JoinKind::Inner,
            JoinKind::Left,
            JoinKind::Right,
            JoinKind::Full,
        ] {
            let left = keyed("a", &left_keys);
            let right = keyed("b", &right_keys);
            let expected = reference_join(&left, &right, kind);
            let actual = visible(
                &join_relations(left, right, kind, &on_eq("a", "k", "b", "k"), &tctx())
                    .expect("join"),
            );
            assert2::assert!(actual == expected, "{kind:?}");
        }
    }

    #[test]
    fn indexed_or_equi_join_agrees_with_the_nested_loop_and_order() {
        let scope = |qualifier: &str, names: &[&str]| Scope {
            columns: names
                .iter()
                .map(|name| ColumnBinding {
                    exposure: Exposure::Output,
                    qualifier: Some(qualifier.into()),
                    name: (*name).into(),
                    ty: ColumnType::Int4,
                })
                .collect(),
        };
        let left = Relation {
            scope: scope("a", &["x", "y"]),
            rows: (0..80)
                .map(|i| {
                    let x = (i % 11 != 0).then_some(Datum::Int4(i % 6));
                    let y = if i % 13 == 0 {
                        None
                    } else {
                        Some({
                            if i % 5 == 0 {
                                x.clone().unwrap_or(Datum::Int4(i % 6))
                            } else {
                                Datum::Int4((i + 1) % 6)
                            }
                        })
                    };
                    vec![x.unwrap_or(Datum::Null), y.unwrap_or(Datum::Null)]
                })
                .collect(),
        };
        let right = Relation {
            scope: scope("b", &["k"]),
            rows: (0..80)
                .map(|i| {
                    vec![if i % 17 == 0 {
                        Datum::Null
                    } else {
                        Datum::Int4(i % 7)
                    }]
                })
                .collect(),
        };
        let constraint = on_or_eq("a", &["x", "y"], "b", "k");
        let reference = |kind: JoinKind| {
            let mut rows = Vec::new();
            let mut right_matched = vec![false; right.rows.len()];
            for l in &left.rows {
                let mut any = false;
                for (ri, r) in right.rows.iter().enumerate() {
                    let matched = !r[0].is_null()
                        && ((!l[0].is_null() && l[0] == r[0]) || (!l[1].is_null() && l[1] == r[0]));
                    if matched {
                        any = true;
                        right_matched[ri] = true;
                        let mut row = l.clone();
                        row.extend(r.iter().cloned());
                        rows.push(row);
                    }
                }
                if !any && matches!(kind, JoinKind::Left | JoinKind::Full) {
                    let mut row = l.clone();
                    row.push(Datum::Null);
                    rows.push(row);
                }
            }
            if matches!(kind, JoinKind::Right | JoinKind::Full) {
                for (ri, r) in right.rows.iter().enumerate() {
                    if !right_matched[ri] {
                        rows.push(vec![Datum::Null, Datum::Null, r[0].clone()]);
                    }
                }
            }
            rows
        };

        for kind in [
            JoinKind::Inner,
            JoinKind::Left,
            JoinKind::Right,
            JoinKind::Full,
        ] {
            let actual = visible(
                &join_relations(left.clone(), right.clone(), kind, &constraint, &tctx())
                    .expect("OR equijoin"),
            );
            let expected = reference(kind);
            assert2::assert!(actual == expected, "{kind:?}");
            assert2::assert!(
                count_join_rows(
                    &left,
                    &right,
                    kind,
                    &constraint,
                    &tctx(),
                    crate::scanner::BLOCKING_QUERY_MEMORY,
                )
                .expect("count OR equijoin")
                    == i64::try_from(expected.len()).expect("test cardinality fits int8"),
                "{kind:?}",
            );
        }
    }

    #[test]
    fn counted_or_join_matches_the_pg_regress_cardinality() {
        let left = rel(
            "t1",
            &["tenthous", "thousand"],
            (0..1_000)
                .map(|row| {
                    let key = row % 100;
                    vec![key, if row < 100 { key } else { (key + 1) % 100 }]
                })
                .collect(),
        );
        let right = rel(
            "t2",
            &["thousand"],
            (0..1_000).map(|row| vec![row % 100]).collect(),
        );
        let constraint = on_or_eq("t1", &["tenthous", "thousand"], "t2", "thousand");

        let count = count_join_rows(
            &left,
            &right,
            JoinKind::Left,
            &constraint,
            &tctx(),
            crate::scanner::BLOCKING_QUERY_MEMORY,
        )
        .expect("count pg_regress OR join");

        assert2::assert!(count == 19_000);
    }

    #[test]
    fn counted_join_does_not_retain_wide_output_rows() {
        let scope = |qualifier: &str| Scope {
            columns: vec![
                ColumnBinding {
                    exposure: Exposure::Output,
                    qualifier: Some(qualifier.into()),
                    name: "k".into(),
                    ty: ColumnType::Int4,
                },
                ColumnBinding {
                    exposure: Exposure::Output,
                    qualifier: Some(qualifier.into()),
                    name: "payload".into(),
                    ty: ColumnType::Text,
                },
            ],
        };
        let rows = || {
            (0..80)
                .map(|key| vec![Datum::Int4(key), Datum::Text("x".repeat(1_024))])
                .collect()
        };
        let left = Relation {
            scope: scope("a"),
            rows: rows(),
        };
        let right = Relation {
            scope: scope("b"),
            rows: rows(),
        };
        let constraint = on_eq("a", "k", "b", "k");
        let budget = crabka_units::ByteSize::from_bytes(16 * 1_024);

        let count = count_join_rows(&left, &right, JoinKind::Left, &constraint, &tctx(), budget)
            .expect("count without joined-row materialization");
        let error = super::join_relations(
            left,
            right,
            JoinKind::Left,
            &constraint,
            &tctx(),
            JoinPolicy {
                memory: budget,
                refs: None,
            },
        )
        .expect_err("wide materialized join must exceed the same budget");

        assert2::assert!(count == 80);
        assert2::assert!(error.into_pg().code == "53200");
    }

    #[test]
    fn prepared_or_index_matches_one_shot_inner_and_left_joins() {
        let left = rel(
            "a",
            &["x", "y"],
            (0..80)
                .map(|i| vec![i % 9, if i % 5 == 0 { i % 9 } else { (i + 1) % 9 }])
                .collect(),
        );
        let right = rel("b", &["k"], (0..80).map(|i| vec![i % 7]).collect());
        let constraint = on_or_eq("a", &["x", "y"], "b", "k");
        let prepared = prepare_join_index(
            &left,
            &right,
            &constraint,
            &tctx(),
            crate::scanner::BLOCKING_QUERY_MEMORY,
        )
        .expect("prepare OR index");

        for kind in [JoinKind::Inner, JoinKind::Left] {
            let expected = join_relations(left.clone(), right.clone(), kind, &constraint, &tctx())
                .expect("one-shot join")
                .rows;
            let mut actual = Vec::new();
            for row in &left.rows {
                actual.extend(
                    join_relations_prepared(
                        Relation {
                            scope: left.scope.clone(),
                            rows: vec![row.clone()],
                        },
                        &right,
                        kind,
                        &constraint,
                        &tctx(),
                        JoinPolicy::default(),
                        &prepared,
                    )
                    .expect("prepared join")
                    .rows,
                );
            }
            assert2::assert!(actual == expected, "{kind:?}");
        }
    }

    #[test]
    fn indexed_or_equi_join_visits_only_the_union_of_matching_buckets() {
        let left = rel(
            "a",
            &["x", "y"],
            (0..10_000).map(|i| vec![i, (i + 1) % 10_000]).collect(),
        );
        let right = rel("b", &["k"], (0..10_000).map(|i| vec![i]).collect());
        let constraint = on_or_eq("a", &["x", "y"], "b", "k");
        let index = JoinIndex::build(
            &left,
            &right,
            &constraint,
            &tctx(),
            crate::scanner::BLOCKING_QUERY_MEMORY,
        )
        .expect("valid join constraint")
        .expect("each OR branch has a hashable equality key");
        assert2::assert!(matches!(index, JoinIndex::Disjunctive { .. }));

        let mut key_buf = Vec::new();
        let mut union = Vec::new();
        let visited: usize = left
            .rows
            .iter()
            .map(|row| {
                index
                    .candidates(row, &mut key_buf, &mut union, &tctx())
                    .expect("probe")
                    .len()
            })
            .sum();

        assert2::assert!(visited == 2 * left.rows.len());
    }

    #[test]
    fn or_index_declines_when_any_branch_has_no_hashable_equality() {
        let left = rel("a", &["x", "y"], (0..80).map(|i| vec![i, i]).collect());
        let right = rel("b", &["k"], (0..80).map(|i| vec![i]).collect());
        let constraint = JoinConstraint::On(Expr::Binary {
            op: BinaryOp::Or,
            left: Box::new(match on_eq("a", "x", "b", "k") {
                JoinConstraint::On(expr) => expr,
                _ => unreachable!(),
            }),
            right: Box::new(Expr::Binary {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Column {
                    table: Some("a".into()),
                    name: "y".into(),
                }),
                right: Box::new(Expr::Column {
                    table: Some("b".into()),
                    name: "k".into(),
                }),
            }),
        });

        assert2::assert!(
            JoinIndex::build(
                &left,
                &right,
                &constraint,
                &tctx(),
                crate::scanner::BLOCKING_QUERY_MEMORY,
            )
            .expect("valid join constraint")
            .is_none()
        );
    }

    #[test]
    fn or_index_declines_when_any_equality_branch_is_hash_unsafe() {
        let left = Relation {
            scope: Scope {
                columns: ["x", "y"]
                    .into_iter()
                    .map(|name| ColumnBinding {
                        exposure: Exposure::Output,
                        qualifier: Some("a".into()),
                        name: name.into(),
                        ty: ColumnType::Int4,
                    })
                    .collect(),
            },
            rows: (0..80)
                .map(|i| vec![Datum::Int4(i), Datum::Int4(i)])
                .collect(),
        };
        let right = Relation {
            scope: Scope {
                columns: vec![
                    ColumnBinding {
                        exposure: Exposure::Output,
                        qualifier: Some("b".into()),
                        name: "k".into(),
                        ty: ColumnType::Int4,
                    },
                    ColumnBinding {
                        exposure: Exposure::Output,
                        qualifier: Some("b".into()),
                        name: "wide".into(),
                        ty: ColumnType::Int8,
                    },
                ],
            },
            rows: (0..80)
                .map(|i| vec![Datum::Int4(i), Datum::Int8(i64::from(i))])
                .collect(),
        };
        let constraint = JoinConstraint::On(Expr::Binary {
            op: BinaryOp::Or,
            left: Box::new(match on_eq("a", "x", "b", "k") {
                JoinConstraint::On(expr) => expr,
                _ => unreachable!(),
            }),
            right: Box::new(match on_eq("a", "y", "b", "wide") {
                JoinConstraint::On(expr) => expr,
                _ => unreachable!(),
            }),
        });

        assert2::assert!(
            JoinIndex::build(
                &left,
                &right,
                &constraint,
                &tctx(),
                crate::scanner::BLOCKING_QUERY_MEMORY,
            )
            .expect("valid join constraint")
            .is_none()
        );
    }

    #[test]
    fn or_index_respects_branch_and_memory_limits() {
        let names: Vec<String> = (0..=JoinIndex::MAX_OR_DISJUNCTS)
            .map(|index| format!("k{index}"))
            .collect();
        let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let left = rel(
            "a",
            &name_refs,
            (0..80).map(|value| vec![value; name_refs.len()]).collect(),
        );
        let right = rel("b", &["k"], (0..80).map(|i| vec![i]).collect());
        let too_many = on_or_eq("a", &name_refs, "b", "k");
        assert2::assert!(
            JoinIndex::build(
                &left,
                &right,
                &too_many,
                &tctx(),
                crate::scanner::BLOCKING_QUERY_MEMORY,
            )
            .expect("valid join constraint")
            .is_none()
        );

        let bounded = on_or_eq("a", &[name_refs[0], name_refs[1]], "b", "k");
        assert2::assert!(
            JoinIndex::build(
                &left,
                &right,
                &bounded,
                &tctx(),
                crabka_units::ByteSize::from_bytes(1),
            )
            .expect("valid join constraint")
            .is_none()
        );
    }

    #[test]
    fn index_discards_an_actual_allocation_over_the_budget() {
        let left = rel("a", &["k"], (0..80).map(|i| vec![i]).collect());
        let right = rel("b", &["k"], (0..80).map(|i| vec![i]).collect());
        let keys = [(LeftKey::Column(0), 0)];
        let plan = EquiIndex::plan(&left, &right, &keys, &tctx()).expect("index plan");
        let planned_bytes = plan.estimated_bytes;
        let actual_bytes = EquiIndex::build_planned(&right, plan).estimated_bytes();
        assert2::assert!(actual_bytes > planned_bytes);

        let budget = crabka_units::ByteSize::from_bytes(
            u64::try_from(actual_bytes - 1).expect("test allocation fits u64"),
        );
        let prepared =
            prepare_join_index(&left, &right, &on_eq("a", "k", "b", "k"), &tctx(), budget)
                .expect("prepare index");
        assert2::assert!(prepared.index.is_none());
        assert2::assert!(prepared.estimated_bytes() == 0);

        let actual = join_relations_prepared(
            left.clone(),
            &right,
            JoinKind::Inner,
            &on_eq("a", "k", "b", "k"),
            &tctx(),
            JoinPolicy::default(),
            &prepared,
        )
        .expect("nested-loop fallback");
        assert2::assert!(actual.rows == reference_join(&left, &right, JoinKind::Inner));
    }

    #[test]
    fn or_index_preallocates_the_charged_union_scratch() {
        let left = rel("a", &["x", "y"], vec![vec![0, 0]; 4]);
        let right = rel("b", &["k"], vec![vec![0]; 1_025]);
        let constraint = on_or_eq("a", &["x", "y"], "b", "k");
        let index = JoinIndex::build(
            &left,
            &right,
            &constraint,
            &tctx(),
            crate::scanner::BLOCKING_QUERY_MEMORY,
        )
        .expect("valid join constraint")
        .expect("OR index");

        let mut key_buf = Vec::new();
        let mut union = Vec::with_capacity(index.candidate_capacity());
        let candidates = index
            .candidates(&left.rows[0], &mut key_buf, &mut union, &tctx())
            .expect("probe")
            .len();
        let retained_without_union = match &index {
            JoinIndex::Disjunctive { indexes, .. } => indexes.iter().fold(
                indexes
                    .capacity()
                    .saturating_mul(std::mem::size_of::<EquiIndex>()),
                |bytes, index| bytes.saturating_add(index.estimated_bytes()),
            ),
            JoinIndex::Conjunctive(_) => unreachable!("expected OR index"),
        };

        assert2::assert!(candidates == right.rows.len());
        assert2::assert!(union.capacity() == right.rows.len());
        assert2::assert!(
            index.estimated_bytes()
                == retained_without_union
                    .saturating_add(union.capacity() * std::mem::size_of::<usize>())
        );
    }

    /// Key columns of different `Datum` variants must NOT be indexed: `Int4(1)`
    /// and `Int8(1)` compare Equal but hash apart, so bucketing them would lose
    /// matches. The nested loop still finds them.
    #[test]
    fn mixed_key_variants_still_join_through_the_nested_loop() {
        let left = keyed("a", &(0..80i32).map(Some).collect::<Vec<_>>());
        let right = Relation {
            scope: Scope {
                columns: vec![crate::scope::ColumnBinding {
                    exposure: crate::scope::Exposure::Output,
                    qualifier: Some("b".into()),
                    name: "k".into(),
                    ty: ColumnType::Int8,
                }],
            },
            rows: (0..80i64).map(|i| vec![Datum::Int8(i)]).collect(),
        };
        assert2::assert!(EquiIndex::build(&left, &right, &[(0, 0)]).is_none());

        let joined = join_relations(
            left,
            right,
            JoinKind::Inner,
            &on_eq("a", "k", "b", "k"),
            &tctx(),
        )
        .expect("join")
        .rows;
        assert2::assert!(joined.len() == 80);
    }

    #[test]
    fn unsupported_equality_key_does_not_disable_a_supported_key() {
        let relation = |qualifier: &str| Relation {
            scope: Scope {
                columns: vec![
                    ColumnBinding {
                        exposure: Exposure::Output,
                        qualifier: Some(qualifier.into()),
                        name: "name".into(),
                        ty: ColumnType::Text,
                    },
                    ColumnBinding {
                        exposure: Exposure::Output,
                        qualifier: Some(qualifier.into()),
                        name: "args".into(),
                        ty: ColumnType::OidVector,
                    },
                ],
            },
            rows: (0..80)
                .map(|i| {
                    vec![
                        Datum::Text(format!("name{}", i % 8)),
                        Datum::OidVector(ArrayValue::new(ElemType::Int4, vec![Datum::Int4(i)])),
                    ]
                })
                .collect(),
        };
        let left = relation("a");
        let right = relation("b");

        let index =
            EquiIndex::build(&left, &right, &[(0, 0), (1, 1)]).expect("text key remains indexable");

        assert2::assert!(matches!(index.left_key.as_slice(), [LeftKey::Column(0)]));
        let mut key_buf = Vec::new();
        assert2::assert!(
            index
                .candidates(&left.rows[0], &mut key_buf, &tctx())
                .expect("probe")
                .len()
                == 10
        );
    }

    /// The point of the index: an equi-join on a unique key visits ONE right row
    /// per left row instead of the whole right relation. Without it a 10k-row
    /// self-join evaluates the ON predicate 100 million times and never answers.
    /// `pg_regress`'s `join` corpus does exactly that self-join.
    #[test]
    fn indexed_equi_join_visits_one_candidate_per_left_row() {
        let keys: Vec<Option<i32>> = (0..10_000i32).map(Some).collect();
        let left = keyed("a", &keys);
        let right = keyed("b", &keys);
        let index = EquiIndex::build(&left, &right, &[(0, 0)]).expect("unique int4 key is indexed");

        let mut key_buf = Vec::new();
        let visited: usize = left
            .rows
            .iter()
            .map(|row| {
                index
                    .candidates(row, &mut key_buf, &tctx())
                    .expect("probe")
                    .len()
            })
            .sum();
        assert2::assert!(visited == left.rows.len());
    }
}
