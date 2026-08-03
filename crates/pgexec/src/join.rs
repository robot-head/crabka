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
use crabka_pgtypes::Datum;

use crate::{
    error::ExecError,
    scope::{ColumnBinding, Scope},
};

/// A materialized relation: an ordered `Scope` (the schema) plus its rows, each
/// row positionally aligned to `scope.columns`. Base tables, joins, and, later,
/// derived subqueries all produce one.
#[derive(Debug, Clone)]
pub(crate) struct Relation {
    pub scope: Scope,
    pub rows: Vec<Vec<Datum>>,
}

/// Join two relations under `kind` and `constraint`, and return the combined
/// relation. `ctx` carries the session zone and clock that evaluate an `ON`
/// predicate with temporal expressions in it. A USING/NATURAL/CROSS join, or a
/// rows-free schema join, never touches `ctx`.
pub(crate) fn join_relations(
    left: Relation,
    right: Relation,
    kind: JoinKind,
    constraint: &JoinConstraint,
    ctx: &crate::clock::EvalCtx,
    blocking_query_memory: crabka_units::ByteSize,
) -> Result<Relation, ExecError> {
    join_relations_impl(
        left,
        &right,
        kind,
        constraint,
        ctx,
        blocking_query_memory,
        None,
    )
}

pub(crate) struct PreparedJoinIndex {
    index: Option<JoinIndex>,
    estimated_bytes: usize,
}

pub(crate) fn prepare_join_index(
    left: &Relation,
    right: &Relation,
    constraint: &JoinConstraint,
    blocking_query_memory: crabka_units::ByteSize,
) -> Result<PreparedJoinIndex, ExecError> {
    let index = JoinIndex::build(left, right, constraint, blocking_query_memory)?;
    let estimated_bytes = index.as_ref().map_or(0, JoinIndex::estimated_bytes);
    Ok(PreparedJoinIndex {
        index,
        estimated_bytes,
    })
}

impl PreparedJoinIndex {
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
    blocking_query_memory: crabka_units::ByteSize,
    prepared: &PreparedJoinIndex,
) -> Result<Relation, ExecError> {
    join_relations_impl(
        left,
        right,
        kind,
        constraint,
        ctx,
        blocking_query_memory,
        Some(prepared),
    )
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

    let mut index = JoinIndex::build(left, right, constraint, blocking_query_memory)?;
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
        ) {
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

struct JoinCondition<'a> {
    combined_scope: Scope,
    join_cols: Vec<String>,
    pairs: Vec<(usize, usize)>,
    on_predicate: Option<&'a Expr>,
}

impl<'a> JoinCondition<'a> {
    fn new(
        left: &Relation,
        right: &Relation,
        constraint: &'a JoinConstraint,
    ) -> Result<Self, ExecError> {
        for column in &right.scope.columns {
            if let Some(qualifier) = &column.qualifier
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
        Ok(Self {
            combined_scope,
            join_cols,
            pairs,
            on_predicate: match constraint {
                JoinConstraint::On(predicate) => Some(predicate),
                _ => None,
            },
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
        let Some(predicate) = self.on_predicate else {
            return Ok(true);
        };
        let mut combined = left.to_vec();
        combined.extend_from_slice(right);
        match crate::eval::eval(predicate, &self.combined_scope, &combined, ctx)? {
            Datum::Bool(value) => Ok(value),
            Datum::Null => Ok(false),
            _ => Err(ExecError::TypeMismatch(
                "JOIN/ON condition must be boolean".into(),
            )),
        }
    }
}

fn join_relations_impl(
    left: Relation,
    right: &Relation,
    kind: JoinKind,
    constraint: &JoinConstraint,
    ctx: &crate::clock::EvalCtx,
    blocking_query_memory: crabka_units::ByteSize,
    prepared: Option<&PreparedJoinIndex>,
) -> Result<Relation, ExecError> {
    let condition = JoinCondition::new(&left, right, constraint)?;
    let lw = left.scope.width();

    let built_index = prepared
        .is_none()
        .then(|| JoinIndex::build(&left, right, constraint, blocking_query_memory))
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
                for ri in
                    candidate_rows(index, right.rows.len(), l, &mut key_buf, &mut candidate_buf)
                {
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
                for ri in
                    candidate_rows(index, right.rows.len(), l, &mut key_buf, &mut candidate_buf)
                {
                    let r = &right.rows[ri];
                    if condition.matches(l, r, ctx)? {
                        any = true;
                        right_matched[ri] = true;
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
                if !any && want_left {
                    let mut row = l.clone();
                    row.extend(vec![Datum::Null; rw]);
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

    // USING/NATURAL: coalesce + reorder the join columns. Otherwise the combined
    // left ++ right schema is the result.
    if condition.pairs.is_empty() {
        Ok(Relation {
            scope: condition.combined_scope,
            rows,
        })
    } else {
        Ok(coalesce_join_columns(
            &left.scope,
            &right.scope,
            &condition.pairs,
            &condition.join_cols,
            rows,
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
) -> CandidateRows<'a> {
    match index {
        Some(index) => {
            CandidateRows::Indexed(index.candidates(lrow, key_buf, candidate_buf).iter())
        }
        None => CandidateRows::All(0..right_len),
    }
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
                let Some(plan) = EquiIndex::plan(left, right, &keys) else {
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
        let Some(plan) = EquiIndex::plan(left, right, &keys) else {
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
    ) -> &'a [usize] {
        match self {
            Self::Conjunctive(index) => index.candidates(lrow, key_buf),
            Self::Disjunctive { indexes, .. } => {
                union.clear();
                let mut lists = Vec::with_capacity(indexes.len());
                for index in indexes {
                    lists.push(index.candidates(lrow, key_buf));
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
                union
            }
        }
    }
}

fn equi_keys_for(
    left: &Relation,
    right: &Relation,
    constraint: &JoinConstraint,
) -> Result<Vec<(usize, usize)>, ExecError> {
    let mut combined = left.scope.clone();
    combined.columns.extend(right.scope.columns.iter().cloned());
    match constraint {
        JoinConstraint::Using(columns) => columns
            .iter()
            .map(|column| {
                Ok((
                    left.scope.resolve(None, column)?,
                    right.scope.resolve(None, column)?,
                ))
            })
            .collect(),
        JoinConstraint::Natural => natural_common_columns(&left.scope, &right.scope)
            .iter()
            .map(|column| {
                Ok((
                    left.scope.resolve(None, column)?,
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
    /// Key columns, as indices into a left row.
    left_key: Vec<usize>,
    buckets: HashMap<Vec<Datum>, Vec<usize>>,
}

struct EquiIndexPlan {
    keys: Vec<(usize, usize)>,
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
    fn plan(left: &Relation, right: &Relation, keys: &[(usize, usize)]) -> Option<EquiIndexPlan> {
        if keys.is_empty() || left.rows.len().saturating_mul(right.rows.len()) < Self::MIN_PAIRS {
            return None;
        }
        let keys: Vec<(usize, usize)> = keys
            .iter()
            .copied()
            .filter(|(li, ri)| {
                match (key_variant(&left.rows, *li), key_variant(&right.rows, *ri)) {
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
            left_key: keys.iter().map(|(lc, _)| *lc).collect(),
            buckets,
        }
    }

    /// `keys` are `(left_column, right_column)` pairs the predicate requires to
    /// compare Equal. Returns `None` when no index applies.
    #[cfg(test)]
    fn build(left: &Relation, right: &Relation, keys: &[(usize, usize)]) -> Option<Self> {
        Self::plan(left, right, keys).map(|plan| Self::build_planned(right, plan))
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

    fn candidates<'a>(&'a self, lrow: &[Datum], key_buf: &mut Vec<Datum>) -> &'a [usize] {
        key_buf.clear();
        for &lc in &self.left_key {
            if lrow[lc].is_null() {
                return &[];
            }
            key_buf.push(lrow[lc].clone());
        }
        match self.buckets.get(key_buf.as_slice()) {
            Some(rows) => rows,
            None => &[],
        }
    }
}

fn key_variant(rows: &[Vec<Datum>], column: usize) -> KeyVariant<'_> {
    let mut seen: Option<&Datum> = None;
    for row in rows {
        let value = &row[column];
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
/// them safe as a pre-filter. The full predicate still decides every candidate.
/// Conjuncts of any other shape contribute no key, including `OR`, which is not
/// a necessary condition.
fn equi_key_columns(pred: &Expr, combined: &Scope, lw: usize) -> Vec<(usize, usize)> {
    let mut keys = Vec::new();
    collect_equi_key_columns(pred, combined, lw, &mut keys);
    keys
}

fn collect_equi_key_columns(
    pred: &Expr,
    combined: &Scope,
    lw: usize,
    out: &mut Vec<(usize, usize)>,
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
            let (Some(a), Some(b)) = (
                combined_column_index(left, combined),
                combined_column_index(right, combined),
            ) else {
                return;
            };
            match (a < lw, b < lw) {
                (true, false) => out.push((a, b - lw)),
                (false, true) => out.push((b, a - lw)),
                _ => {}
            }
        }
        _ => {}
    }
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

/// The column names common to both scopes, matched by name, in left order, and
/// deduplicated. This drives `NATURAL JOIN`'s join-column set. An empty set
/// degenerates to a cross join, as PostgreSQL does.
fn natural_common_columns(left: &Scope, right: &Scope) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for c in &left.columns {
        if right.columns.iter().any(|rc| rc.name == c.name) && !out.contains(&c.name) {
            out.push(c.name.clone());
        }
    }
    out
}

/// Reshape a `left ++ right` combined relation into PostgreSQL's USING/NATURAL
/// output. Each join column appears ONCE, coalesced so that the present side
/// wins, which matters for outer joins. Each join column is unqualified and sits
/// FIRST in `join` order. The remaining left columns follow, and then the
/// remaining right columns.
fn coalesce_join_columns(
    left_scope: &Scope,
    right_scope: &Scope,
    pairs: &[(usize, usize)], // (left_idx, right_idx) per join column, in join order
    join_names: &[String],
    rows: Vec<Vec<Datum>>, // combined left ++ right rows
) -> Relation {
    let lw = left_scope.width();
    let left_join: Vec<usize> = pairs.iter().map(|(li, _)| *li).collect();
    let right_join: Vec<usize> = pairs.iter().map(|(_, ri)| *ri).collect();

    // New schema: merged join cols (unqualified), then non-join left, then non-join right.
    // The merged column takes the LEFT side's type. USING/NATURAL keys are the same
    // type on both sides in this slice's tested surface; PG unifies left/right types
    // for a mixed-width key (e.g. `int4` USING `int8`) — that unification is deferred.
    let mut columns: Vec<ColumnBinding> = Vec::new();
    for ((li, _ri), name) in pairs.iter().zip(join_names) {
        columns.push(ColumnBinding {
            qualifier: None,
            name: name.clone(),
            ty: left_scope.ty_at(*li),
        });
    }
    for (i, c) in left_scope.columns.iter().enumerate() {
        if !left_join.contains(&i) {
            columns.push(c.clone());
        }
    }
    for (i, c) in right_scope.columns.iter().enumerate() {
        if !right_join.contains(&i) {
            columns.push(c.clone());
        }
    }
    let scope = Scope { columns };

    let new_rows = rows
        .into_iter()
        .map(|row| {
            let mut out: Vec<Datum> = Vec::with_capacity(scope.width());
            // Coalesced join columns (left value unless NULL, else right value).
            for (li, ri) in pairs {
                let lv = &row[*li];
                out.push(if lv.is_null() {
                    row[lw + *ri].clone()
                } else {
                    lv.clone()
                });
            }
            // Remaining left columns.
            for (i, val) in row[..lw].iter().enumerate() {
                if !left_join.contains(&i) {
                    out.push(val.clone());
                }
            }
            // Remaining right columns.
            for (i, val) in row[lw..].iter().enumerate() {
                if !right_join.contains(&i) {
                    out.push(val.clone());
                }
            }
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
        super::join_relations(
            left,
            right,
            kind,
            constraint,
            ctx,
            crate::scanner::BLOCKING_QUERY_MEMORY,
        )
    }

    fn rel(qual: &str, cols: &[&str], rows: Vec<Vec<i32>>) -> Relation {
        let scope = Scope {
            columns: cols
                .iter()
                .map(|n| crate::scope::ColumnBinding {
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
        assert!(j.rows.contains(&vec![Datum::Int4(1), Datum::Null]));
        assert_eq!(j.rows.len(), 3);
    }

    #[test]
    fn right_join_null_extends_unmatched_right_rows() {
        let a = rel("a", &["id"], vec![vec![2]]);
        let b = rel("b", &["id"], vec![vec![1], vec![2]]);
        let j = join_relations(a, b, JoinKind::Right, &on_eq("a", "id", "b", "id"), &tctx())
            .expect("right join");
        assert!(j.rows.contains(&vec![Datum::Null, Datum::Int4(1)]));
        assert_eq!(j.rows.len(), 2);
    }

    #[test]
    fn full_join_keeps_unmatched_from_both_sides() {
        let a = rel("a", &["id"], vec![vec![1], vec![2]]);
        let b = rel("b", &["id"], vec![vec![2], vec![3]]);
        let j = join_relations(a, b, JoinKind::Full, &on_eq("a", "id", "b", "id"), &tctx())
            .expect("full join");
        assert!(j.rows.contains(&vec![Datum::Int4(1), Datum::Null])); // unmatched left
        assert!(j.rows.contains(&vec![Datum::Null, Datum::Int4(3)])); // unmatched right
        assert!(j.rows.contains(&vec![Datum::Int4(2), Datum::Int4(2)])); // matched
        assert_eq!(j.rows.len(), 3);
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
        // Output schema: merged unqualified `id` first, then a.av, then b.bv.
        assert_eq!(j.scope.columns[0].qualifier, None);
        assert_eq!(j.scope.columns[0].name, "id");
        assert_eq!(
            j.scope
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>(),
            vec!["id", "av", "bv"]
        );
        assert_eq!(
            j.rows,
            vec![vec![Datum::Int4(2), Datum::Int4(20), Datum::Int4(200)]]
        );
    }

    #[test]
    fn natural_join_uses_all_common_columns() {
        let a = rel("a", &["id"], vec![vec![1], vec![2]]);
        let b = rel("b", &["id"], vec![vec![2], vec![3]]);
        let j = join_relations(a, b, JoinKind::Inner, &JoinConstraint::Natural, &tctx())
            .expect("natural");
        assert_eq!(j.scope.columns.len(), 1); // single merged `id`
        assert_eq!(j.rows, vec![vec![Datum::Int4(2)]]);
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
        // rows: id=1 unmatched -> (1, 10, NULL); id=2 matched -> (2, 20, 200).
        assert!(
            j.rows
                .contains(&vec![Datum::Int4(1), Datum::Int4(10), Datum::Null])
        );
        assert!(
            j.rows
                .contains(&vec![Datum::Int4(2), Datum::Int4(20), Datum::Int4(200)])
        );
        assert_eq!(j.rows.len(), 2);
    }

    /// A relation whose single key column holds `keys`, with `None` for NULL.
    fn keyed(qualifier: &str, keys: &[Option<i32>]) -> Relation {
        Relation {
            scope: Scope {
                columns: vec![crate::scope::ColumnBinding {
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
            let actual = join_relations(left, right, kind, &on_eq("a", "k", "b", "k"), &tctx())
                .expect("join")
                .rows;
            assert2::assert!(actual == expected, "{kind:?}");
        }
    }

    #[test]
    fn indexed_or_equi_join_agrees_with_the_nested_loop_and_order() {
        let scope = |qualifier: &str, names: &[&str]| Scope {
            columns: names
                .iter()
                .map(|name| ColumnBinding {
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
            let actual = join_relations(left.clone(), right.clone(), kind, &constraint, &tctx())
                .expect("OR equijoin")
                .rows;
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
                    qualifier: Some(qualifier.into()),
                    name: "k".into(),
                    ty: ColumnType::Int4,
                },
                ColumnBinding {
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
        let error =
            super::join_relations(left, right, JoinKind::Left, &constraint, &tctx(), budget)
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
                        crate::scanner::BLOCKING_QUERY_MEMORY,
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
            .map(|row| index.candidates(row, &mut key_buf, &mut union).len())
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
                        qualifier: Some("b".into()),
                        name: "k".into(),
                        ty: ColumnType::Int4,
                    },
                    ColumnBinding {
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
        let keys = [(0, 0)];
        let plan = EquiIndex::plan(&left, &right, &keys).expect("index plan");
        let planned_bytes = plan.estimated_bytes;
        let actual_bytes = EquiIndex::build_planned(&right, plan).estimated_bytes();
        assert2::assert!(actual_bytes > planned_bytes);

        let budget = crabka_units::ByteSize::from_bytes(
            u64::try_from(actual_bytes - 1).expect("test allocation fits u64"),
        );
        let prepared = prepare_join_index(&left, &right, &on_eq("a", "k", "b", "k"), budget)
            .expect("prepare index");
        assert2::assert!(prepared.index.is_none());
        assert2::assert!(prepared.estimated_bytes() == 0);

        let actual = join_relations_prepared(
            left.clone(),
            &right,
            JoinKind::Inner,
            &on_eq("a", "k", "b", "k"),
            &tctx(),
            crate::scanner::BLOCKING_QUERY_MEMORY,
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
            crate::scanner::BLOCKING_QUERY_MEMORY,
        )
        .expect("valid join constraint")
        .expect("OR index");

        let mut key_buf = Vec::new();
        let mut union = Vec::with_capacity(index.candidate_capacity());
        let candidates = index
            .candidates(&left.rows[0], &mut key_buf, &mut union)
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
                        qualifier: Some(qualifier.into()),
                        name: "name".into(),
                        ty: ColumnType::Text,
                    },
                    ColumnBinding {
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

        assert2::assert!(index.left_key == vec![0]);
        let mut key_buf = Vec::new();
        assert2::assert!(index.candidates(&left.rows[0], &mut key_buf).len() == 10);
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
            .map(|row| index.candidates(row, &mut key_buf).len())
            .sum();
        assert2::assert!(visited == left.rows.len());
    }
}
