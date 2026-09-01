//! PostgreSQL-compatible `tsvector` and `tsquery` values.

use std::{cmp::Ordering, collections::BTreeMap, fmt, str::FromStr};

use crate::TypeError;

/// `PostgreSQL`'s largest stored lexeme position.
pub const MAX_POSITION: u16 = 16_383;
/// `PostgreSQL`'s largest phrase distance.
pub const MAX_PHRASE_DISTANCE: u16 = 16_384;

/// A text-search weight. `PostgreSQL` orders these D (default) through A.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Weight {
    D,
    C,
    B,
    A,
}

impl Weight {
    #[must_use]
    pub fn parse(c: char) -> Option<Self> {
        Some(match c.to_ascii_uppercase() {
            'A' => Self::A,
            'B' => Self::B,
            'C' => Self::C,
            'D' => Self::D,
            _ => return None,
        })
    }

    const fn suffix(self) -> &'static str {
        match self {
            Self::A => "A",
            Self::B => "B",
            Self::C => "C",
            Self::D => "",
        }
    }
}

/// One occurrence of a lexeme in a document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position {
    pub position: u16,
    pub weight: Weight,
}

/// One normalized document lexeme and its optional occurrences.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lexeme {
    pub text: String,
    pub positions: Vec<Position>,
}

/// A normalized document. Entries are always sorted and de-duplicated.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TsVector(pub Vec<Lexeme>);

impl TsVector {
    #[must_use]
    pub fn new(entries: impl IntoIterator<Item = Lexeme>) -> Self {
        let mut merged: BTreeMap<String, Vec<Position>> = BTreeMap::new();
        for entry in entries {
            merged
                .entry(entry.text)
                .or_default()
                .extend(entry.positions);
        }
        Self(
            merged
                .into_iter()
                .map(|(text, mut positions)| {
                    positions.sort_unstable();
                    positions.dedup();
                    Lexeme { text, positions }
                })
                .collect(),
        )
    }

    #[must_use]
    pub fn strip(&self) -> Self {
        Self(
            self.0
                .iter()
                .map(|entry| Lexeme {
                    text: entry.text.clone(),
                    positions: Vec::new(),
                })
                .collect(),
        )
    }

    /// Concatenate two vectors.
    ///
    /// This function shifts the right-hand positions to after the last position
    /// in the left-hand document.
    #[must_use]
    pub fn concat(&self, right: &Self) -> Self {
        let offset = self
            .0
            .iter()
            .flat_map(|entry| &entry.positions)
            .map(|position| position.position)
            .max()
            .unwrap_or(0);
        Self::new(
            self.0
                .iter()
                .cloned()
                .chain(right.0.iter().cloned().map(|mut entry| {
                    for position in &mut entry.positions {
                        position.position =
                            position.position.saturating_add(offset).min(MAX_POSITION);
                    }
                    entry
                })),
        )
    }

    #[must_use]
    pub fn matches(&self, query: &TsQuery) -> bool {
        query_matches(query, self).0
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[must_use]
    pub fn set_weight(&self, weight: Weight, selected: Option<&[String]>) -> Self {
        let mut vector = self.clone();
        for entry in &mut vector.0 {
            if selected.is_none_or(|words| words.contains(&entry.text)) {
                for position in &mut entry.positions {
                    position.weight = weight;
                }
            }
        }
        vector
    }

    #[must_use]
    pub fn delete(&self, words: &[String]) -> Self {
        Self(
            self.0
                .iter()
                .filter(|entry| !words.contains(&entry.text))
                .cloned()
                .collect(),
        )
    }

    #[must_use]
    pub fn filter_weights(&self, weights: &[Weight]) -> Self {
        Self::new(self.0.iter().filter_map(|entry| {
            let positions = entry
                .positions
                .iter()
                .filter(|position| weights.contains(&position.weight))
                .copied()
                .collect::<Vec<_>>();
            (!positions.is_empty()).then(|| Lexeme {
                text: entry.text.clone(),
                positions,
            })
        }))
    }

    #[must_use]
    pub fn rank(&self, query: &TsQuery) -> f32 {
        if !self.matches(query) {
            return 0.0;
        }
        let mut terms = Vec::new();
        collect_terms(query, &mut terms);
        let score = terms
            .iter()
            .flat_map(|term| self.0.iter().filter(move |entry| entry.text == term.text))
            .flat_map(|entry| &entry.positions)
            .map(|position| match position.weight {
                Weight::D => 0.1,
                Weight::C => 0.2,
                Weight::B => 0.4,
                Weight::A => 1.0,
            })
            .sum::<f32>();
        let document_len = u16::try_from(self.0.len()).unwrap_or(u16::MAX);
        score / (1.0 + f32::from(document_len))
    }

    #[must_use]
    pub fn rank_cd(&self, query: &TsQuery) -> f32 {
        let mut terms = Vec::new();
        collect_terms(query, &mut terms);
        let terms = &terms;
        let mut document = self
            .0
            .iter()
            .enumerate()
            .flat_map(|(entry, lexeme)| {
                lexeme
                    .positions
                    .iter()
                    .copied()
                    .filter_map(move |position| {
                        terms
                            .iter()
                            .any(|term| {
                                (if term.prefix {
                                    lexeme.text.starts_with(&term.text)
                                } else {
                                    lexeme.text == term.text
                                }) && (term.weights.is_empty()
                                    || term.weights.contains(&position.weight))
                            })
                            .then_some((entry, position))
                    })
            })
            .collect::<Vec<_>>();
        document
            .sort_unstable_by_key(|(entry, position)| (position.position, position.weight, *entry));

        let mut rank = 0.0;
        let mut start = 0;
        while let Some(end) =
            (start..document.len()).find(|&end| self.cover_matches(&document[start..=end], query))
        {
            let begin = (start..=end)
                .rev()
                .find(|&begin| self.cover_matches(&document[begin..=end], query))
                .expect("a matching cover has a lower bound");
            let cover = &document[begin..=end];
            let inverse_weights = cover
                .iter()
                .map(|(_, position)| match position.weight {
                    Weight::D => 1.0 / f64::from(0.1_f32),
                    Weight::C => 1.0 / f64::from(0.2_f32),
                    Weight::B => 1.0 / f64::from(0.4_f32),
                    Weight::A => 1.0,
                })
                .sum::<f64>();
            let density =
                f64::from(u32::try_from(cover.len()).unwrap_or(u32::MAX)) / inverse_weights;
            let positions = i32::from(cover.last().expect("nonempty cover").1.position)
                - i32::from(cover.first().expect("nonempty cover").1.position);
            let noise = positions - i32::try_from(cover.len() - 1).unwrap_or(i32::MAX);
            let noise = if noise < 0 {
                i32::try_from(cover.len() - 1).unwrap_or(i32::MAX) / 2
            } else {
                noise
            };
            rank += density / f64::from(1 + noise);
            start = begin + 1;
        }
        rank as f32
    }

    fn cover_matches(&self, cover: &[(usize, Position)], query: &TsQuery) -> bool {
        Self::new(cover.iter().map(|(entry, position)| Lexeme {
            text: self.0[*entry].text.clone(),
            positions: vec![*position],
        }))
        .matches(query)
    }

    fn positions(&self, term: &QueryTerm) -> Vec<u16> {
        self.0
            .iter()
            .filter(|entry| {
                if term.prefix {
                    entry.text.starts_with(&term.text)
                } else {
                    entry.text == term.text
                }
            })
            .flat_map(|entry| entry.positions.iter())
            .filter(|position| term.weights.is_empty() || term.weights.contains(&position.weight))
            .map(|position| position.position)
            .collect()
    }
}

/// A query lexeme plus optional weight and prefix restrictions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueryTerm {
    pub text: String,
    pub weights: Vec<Weight>,
    pub prefix: bool,
}

/// A normalized text-search query.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TsQuery {
    Empty,
    Term(QueryTerm),
    Not(Box<Self>),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Phrase(Box<Self>, Box<Self>, u16),
}

impl TsQuery {
    #[must_use]
    pub fn node_count(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Term(_) => 1,
            Self::Not(query) => 1 + query.node_count(),
            Self::And(left, right) | Self::Or(left, right) | Self::Phrase(left, right, _) => {
                1 + left.node_count() + right.node_count()
            }
        }
    }

    /// `PostgreSQL`'s query containment operators compare lexemes and ignore the
    /// boolean structure around them.
    #[must_use]
    pub fn contains(&self, other: &Self) -> bool {
        let mut mine = Vec::new();
        let mut theirs = Vec::new();
        collect_terms(self, &mut mine);
        collect_terms(other, &mut theirs);
        theirs.iter().all(|term| mine.contains(term))
    }

    #[must_use]
    pub fn terms(&self) -> Vec<&str> {
        let mut terms = Vec::new();
        collect_terms(self, &mut terms);
        terms.into_iter().map(|term| term.text.as_str()).collect()
    }

    /// Substitute every structural occurrence of `target` with `replacement`.
    #[must_use]
    pub fn rewrite(&self, target: &Self, replacement: &Self) -> Self {
        if target == &Self::Empty {
            return self.clone();
        }
        let Some(mut query) = Qtn::from_query(self) else {
            return Self::Empty;
        };
        let Some(mut target) = Qtn::from_query(target) else {
            return self.clone();
        };
        query.ternary_sort();
        target.ternary_sort();
        query
            .rewrite(&target, Qtn::from_query(replacement).as_ref())
            .map_or(Self::Empty, |mut query| {
                query.binary();
                query.into_query()
            })
    }

    /// Compare queries using PostgreSQL's on-disk `tsquery` order.
    #[must_use]
    pub fn postgres_cmp(&self, other: &Self) -> Ordering {
        let node_count = self.node_count().cmp(&other.node_count());
        if !node_count.is_eq() {
            return node_count;
        }
        let operand_bytes = self
            .operand_storage_bytes()
            .cmp(&other.operand_storage_bytes());
        if !operand_bytes.is_eq() {
            return operand_bytes;
        }
        match (Qtn::from_query(self), Qtn::from_query(other)) {
            (Some(left), Some(right)) => Qtn::compare(&left, &right),
            _ => Ordering::Equal,
        }
    }

    fn operand_storage_bytes(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Term(term) => term.text.len() + 1,
            Self::Not(query) => query.operand_storage_bytes(),
            Self::And(left, right) | Self::Or(left, right) | Self::Phrase(left, right, _) => {
                left.operand_storage_bytes() + right.operand_storage_bytes()
            }
        }
    }
}

#[derive(Clone)]
struct Qtn {
    kind: QtnKind,
    nochange: bool,
}

#[derive(Clone)]
enum QtnKind {
    Term(QueryTerm),
    Not(Box<Qtn>),
    And(Vec<Qtn>),
    Or(Vec<Qtn>),
    Phrase(Box<Qtn>, Box<Qtn>, u16),
}

impl Qtn {
    fn from_query(query: &TsQuery) -> Option<Self> {
        Some(Self {
            kind: match query {
                TsQuery::Empty => return None,
                TsQuery::Term(term) => QtnKind::Term(term.clone()),
                TsQuery::Not(inner) => QtnKind::Not(Box::new(Self::from_query(inner)?)),
                TsQuery::And(left, right) => {
                    QtnKind::And(vec![Self::from_query(right)?, Self::from_query(left)?])
                }
                TsQuery::Or(left, right) => {
                    QtnKind::Or(vec![Self::from_query(right)?, Self::from_query(left)?])
                }
                TsQuery::Phrase(left, right, distance) => QtnKind::Phrase(
                    Box::new(Self::from_query(right)?),
                    Box::new(Self::from_query(left)?),
                    *distance,
                ),
            },
            nochange: false,
        })
    }

    fn ternary_sort(&mut self) {
        match &mut self.kind {
            QtnKind::And(children) => {
                for child in &mut *children {
                    child.ternary_sort();
                }
                let mut flattened = Vec::new();
                for child in std::mem::take(children) {
                    match child.kind {
                        QtnKind::And(children) => flattened.extend(children),
                        _ => flattened.push(child),
                    }
                }
                flattened.sort_by(Self::compare);
                *children = flattened;
            }
            QtnKind::Or(children) => {
                for child in &mut *children {
                    child.ternary_sort();
                }
                let mut flattened = Vec::new();
                for child in std::mem::take(children) {
                    match child.kind {
                        QtnKind::Or(children) => flattened.extend(children),
                        _ => flattened.push(child),
                    }
                }
                flattened.sort_by(Self::compare);
                *children = flattened;
            }
            QtnKind::Not(child) => child.ternary_sort(),
            QtnKind::Phrase(first, second, _) => {
                first.ternary_sort();
                second.ternary_sort();
            }
            QtnKind::Term(_) => {}
        }
    }

    fn sort(&mut self) {
        match &mut self.kind {
            QtnKind::And(children) | QtnKind::Or(children) => {
                for child in &mut *children {
                    child.sort();
                }
                children.sort_by(Self::compare);
            }
            QtnKind::Not(child) => child.sort(),
            QtnKind::Phrase(first, second, _) => {
                first.sort();
                second.sort();
            }
            QtnKind::Term(_) => {}
        }
    }

    fn rewrite(mut self, target: &Self, replacement: Option<&Self>) -> Option<Self> {
        if Self::compare(&self, target).is_eq() {
            return replacement.cloned().map(|mut replacement| {
                replacement.nochange = true;
                replacement
            });
        }
        if self.nochange {
            return Some(self);
        }
        let matched_subset = match (&mut self.kind, &target.kind) {
            (QtnKind::And(children), QtnKind::And(targets))
            | (QtnKind::Or(children), QtnKind::Or(targets))
                if children.len() > targets.len() && !targets.is_empty() =>
            {
                let mut matched = vec![false; children.len()];
                let mut index = 0;
                for target in targets {
                    while index < children.len() && Self::compare(&children[index], target).is_lt()
                    {
                        index += 1;
                    }
                    if index == children.len() || !Self::compare(&children[index], target).is_eq() {
                        return self.rewrite_children(target, replacement);
                    }
                    matched[index] = true;
                    index += 1;
                }
                let mut retained = children
                    .drain(..)
                    .enumerate()
                    .filter_map(|(index, child)| (!matched[index]).then_some(child))
                    .collect::<Vec<_>>();
                if let Some(mut replacement) = replacement.cloned() {
                    replacement.nochange = true;
                    retained.push(replacement);
                }
                *children = retained;
                true
            }
            _ => false,
        };
        if matched_subset {
            self.sort();
            return Some(self);
        }
        self.rewrite_children(target, replacement)
    }

    fn rewrite_children(self, target: &Self, replacement: Option<&Self>) -> Option<Self> {
        match self.kind {
            QtnKind::Term(_) => Some(self),
            QtnKind::Not(child) => (*child).rewrite(target, replacement).map(|child| Self {
                kind: QtnKind::Not(Box::new(child)),
                nochange: false,
            }),
            QtnKind::And(children) => {
                Self::rewrite_associative(children, target, replacement, true)
            }
            QtnKind::Or(children) => {
                Self::rewrite_associative(children, target, replacement, false)
            }
            QtnKind::Phrase(first, second, distance) => {
                let first = (*first).rewrite(target, replacement)?;
                let second = (*second).rewrite(target, replacement)?;
                Some(Self {
                    kind: QtnKind::Phrase(Box::new(first), Box::new(second), distance),
                    nochange: false,
                })
            }
        }
    }

    fn rewrite_associative(
        children: Vec<Self>,
        target: &Self,
        replacement: Option<&Self>,
        and: bool,
    ) -> Option<Self> {
        let mut children = children
            .into_iter()
            .filter_map(|child| child.rewrite(target, replacement))
            .collect::<Vec<_>>();
        if children.is_empty() {
            None
        } else if children.len() == 1 {
            children.pop()
        } else {
            Some(Self {
                kind: if and {
                    QtnKind::And(children)
                } else {
                    QtnKind::Or(children)
                },
                nochange: false,
            })
        }
    }

    fn binary(&mut self) {
        match &mut self.kind {
            QtnKind::And(children) => Self::binary_children(children, true),
            QtnKind::Or(children) => Self::binary_children(children, false),
            QtnKind::Not(child) => child.binary(),
            QtnKind::Phrase(first, second, _) => {
                first.binary();
                second.binary();
            }
            QtnKind::Term(_) => {}
        }
    }

    fn binary_children(children: &mut Vec<Self>, and: bool) {
        for child in &mut *children {
            child.binary();
        }
        while children.len() > 2 {
            let first = children.remove(0);
            let second = children.remove(0);
            let last = children.pop().expect("at least three children");
            children.insert(
                0,
                Self {
                    kind: if and {
                        QtnKind::And(vec![first, second])
                    } else {
                        QtnKind::Or(vec![first, second])
                    },
                    nochange: false,
                },
            );
            children.insert(1, last);
        }
    }

    fn into_query(self) -> TsQuery {
        match self.kind {
            QtnKind::Term(term) => TsQuery::Term(term),
            QtnKind::Not(child) => TsQuery::Not(Box::new(child.into_query())),
            QtnKind::And(mut children) => {
                let first = children.remove(0).into_query();
                let second = children.remove(0).into_query();
                combine_associative(vec![second, first], true)
            }
            QtnKind::Or(mut children) => {
                let first = children.remove(0).into_query();
                let second = children.remove(0).into_query();
                combine_associative(vec![second, first], false)
            }
            QtnKind::Phrase(first, second, distance) => TsQuery::Phrase(
                Box::new(second.into_query()),
                Box::new(first.into_query()),
                distance,
            ),
        }
    }

    fn compare(left: &Self, right: &Self) -> Ordering {
        let rank = Self::rank(right).cmp(&Self::rank(left));
        if !rank.is_eq() {
            return rank;
        }
        match (&left.kind, &right.kind) {
            (QtnKind::Term(left), QtnKind::Term(right)) => {
                let checksum =
                    legacy_crc32(right.text.as_bytes()).cmp(&legacy_crc32(left.text.as_bytes()));
                if checksum.is_eq() {
                    left.text.as_bytes().cmp(right.text.as_bytes())
                } else {
                    checksum
                }
            }
            (QtnKind::Not(left), QtnKind::Not(right)) => Self::compare(left, right),
            (QtnKind::And(left), QtnKind::And(right)) | (QtnKind::Or(left), QtnKind::Or(right)) => {
                let child_count = right.len().cmp(&left.len());
                if !child_count.is_eq() {
                    return child_count;
                }
                for (left, right) in left.iter().zip(right) {
                    let comparison = Self::compare(left, right);
                    if !comparison.is_eq() {
                        return comparison;
                    }
                }
                Ordering::Equal
            }
            (
                QtnKind::Phrase(left_first, left_second, left_distance),
                QtnKind::Phrase(right_first, right_second, right_distance),
            ) => Self::compare(left_first, right_first)
                .then_with(|| Self::compare(left_second, right_second))
                .then_with(|| right_distance.cmp(left_distance)),
            _ => unreachable!("equal query ranks have the same variant"),
        }
    }

    const fn rank(query: &Self) -> u8 {
        match query.kind {
            QtnKind::Term(_) => 1,
            QtnKind::Not(_) => 2,
            QtnKind::And(_) => 3,
            QtnKind::Or(_) => 4,
            QtnKind::Phrase(_, _, _) => 5,
        }
    }
}

fn combine_associative(parts: Vec<TsQuery>, and: bool) -> TsQuery {
    let mut flattened = Vec::new();
    for part in parts {
        flatten_associative(part, and, &mut flattened);
    }
    flattened
        .into_iter()
        .reduce(|left, right| {
            if and {
                TsQuery::And(Box::new(left), Box::new(right))
            } else {
                TsQuery::Or(Box::new(left), Box::new(right))
            }
        })
        .unwrap_or(TsQuery::Empty)
}

fn flatten_associative(query: TsQuery, and: bool, flattened: &mut Vec<TsQuery>) {
    match (and, query) {
        (true, TsQuery::And(left, right)) | (false, TsQuery::Or(left, right)) => {
            flatten_associative(*left, and, flattened);
            flatten_associative(*right, and, flattened);
        }
        (_, query) => flattened.push(query),
    }
}

fn legacy_crc32(bytes: &[u8]) -> i32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        let mut table = (crc >> 24) ^ u32::from(*byte);
        for _ in 0..8 {
            table = if table & 1 == 0 {
                table >> 1
            } else {
                (table >> 1) ^ 0xedb8_8320
            };
        }
        crc = table ^ (crc << 8);
    }
    i32::from_ne_bytes((crc ^ u32::MAX).to_ne_bytes())
}

fn collect_terms<'a>(query: &'a TsQuery, terms: &mut Vec<&'a QueryTerm>) {
    match query {
        TsQuery::Empty => {}
        TsQuery::Term(term) => terms.push(term),
        TsQuery::Not(inner) => collect_terms(inner, terms),
        TsQuery::And(left, right) | TsQuery::Or(left, right) | TsQuery::Phrase(left, right, _) => {
            collect_terms(left, terms);
            collect_terms(right, terms);
        }
    }
}

fn query_matches(query: &TsQuery, vector: &TsVector) -> (bool, Vec<u16>) {
    match query {
        TsQuery::Empty => (false, Vec::new()),
        TsQuery::Term(term) => {
            let result = phrase_term_matches(term, vector);
            (result.matched, result.positions)
        }
        TsQuery::Not(inner) => (!query_matches(inner, vector).0, Vec::new()),
        TsQuery::And(left, right) => {
            let (left_match, mut positions) = query_matches(left, vector);
            let (right_match, right_positions) = query_matches(right, vector);
            if left_match && right_match {
                positions.extend(right_positions);
                (true, positions)
            } else {
                (false, Vec::new())
            }
        }
        TsQuery::Or(left, right) => {
            let (left_match, left_positions) = query_matches(left, vector);
            let (right_match, right_positions) = query_matches(right, vector);
            let mut positions = if left_match {
                left_positions
            } else {
                Vec::new()
            };
            if right_match {
                positions.extend(right_positions);
            }
            (left_match || right_match, positions)
        }
        TsQuery::Phrase(left, right, distance) => {
            let result = phrase_and(
                phrase_matches(left, vector),
                phrase_matches(right, vector),
                Some(*distance),
            );
            (result.matched, result.positions)
        }
    }
}

#[derive(Default)]
struct PhraseMatch {
    matched: bool,
    positions: Vec<u16>,
    negated: bool,
    width: u32,
}

fn phrase_term_matches(term: &QueryTerm, vector: &TsVector) -> PhraseMatch {
    let positions = vector.positions(term);
    let matched = !positions.is_empty()
        || vector.0.iter().any(|entry| {
            (if term.prefix {
                entry.text.starts_with(&term.text)
            } else {
                entry.text == term.text
            }) && entry.positions.is_empty()
        });
    PhraseMatch {
        matched,
        positions,
        ..PhraseMatch::default()
    }
}

fn phrase_matches(query: &TsQuery, vector: &TsVector) -> PhraseMatch {
    match query {
        TsQuery::Empty => PhraseMatch::default(),
        TsQuery::Term(term) => phrase_term_matches(term, vector),
        TsQuery::Not(inner) => {
            let mut result = phrase_matches(inner, vector);
            if !result.matched {
                return PhraseMatch {
                    matched: true,
                    negated: true,
                    ..PhraseMatch::default()
                };
            }
            if result.positions.is_empty() {
                return PhraseMatch::default();
            }
            result.negated = !result.negated;
            result
        }
        TsQuery::And(left, right) => phrase_and(
            phrase_matches(left, vector),
            phrase_matches(right, vector),
            None,
        ),
        TsQuery::Or(left, right) => {
            phrase_or(phrase_matches(left, vector), phrase_matches(right, vector))
        }
        TsQuery::Phrase(left, right, distance) => phrase_and(
            phrase_matches(left, vector),
            phrase_matches(right, vector),
            Some(*distance),
        ),
    }
}

fn phrase_and(left: PhraseMatch, right: PhraseMatch, distance: Option<u16>) -> PhraseMatch {
    if !left.matched || !right.matched {
        return PhraseMatch::default();
    }

    let (left_offset, right_offset, width) = if let Some(distance) = distance {
        (
            u32::from(distance) + right.width,
            0,
            u32::from(distance) + left.width + right.width,
        )
    } else {
        let width = left.width.max(right.width);
        (width - left.width, width - right.width, width)
    };
    let (positions, negated) = match (left.negated, right.negated) {
        (true, true) => (
            phrase_output(
                &left.positions,
                &right.positions,
                left_offset,
                right_offset,
                true,
                true,
                true,
            ),
            true,
        ),
        (true, false) => (
            phrase_output(
                &left.positions,
                &right.positions,
                left_offset,
                right_offset,
                false,
                true,
                false,
            ),
            false,
        ),
        (false, true) => (
            phrase_output(
                &left.positions,
                &right.positions,
                left_offset,
                right_offset,
                true,
                false,
                false,
            ),
            false,
        ),
        (false, false) => (
            phrase_output(
                &left.positions,
                &right.positions,
                left_offset,
                right_offset,
                false,
                false,
                true,
            ),
            false,
        ),
    };
    PhraseMatch {
        matched: negated || !positions.is_empty(),
        positions,
        negated,
        width,
    }
}

fn phrase_or(mut left: PhraseMatch, mut right: PhraseMatch) -> PhraseMatch {
    if !left.matched && !right.matched {
        return PhraseMatch::default();
    }
    if !left.matched {
        left.width = 0;
    }
    if !right.matched {
        right.width = 0;
    }

    let width = left.width.max(right.width);
    let left_offset = width - left.width;
    let right_offset = width - right.width;
    let (positions, negated) = match (left.negated, right.negated) {
        (true, true) => (
            phrase_output(
                &left.positions,
                &right.positions,
                left_offset,
                right_offset,
                false,
                false,
                true,
            ),
            true,
        ),
        (true, false) => (
            phrase_output(
                &left.positions,
                &right.positions,
                left_offset,
                right_offset,
                true,
                false,
                false,
            ),
            true,
        ),
        (false, true) => (
            phrase_output(
                &left.positions,
                &right.positions,
                left_offset,
                right_offset,
                false,
                true,
                false,
            ),
            true,
        ),
        (false, false) => (
            phrase_output(
                &left.positions,
                &right.positions,
                left_offset,
                right_offset,
                true,
                true,
                true,
            ),
            false,
        ),
    };
    PhraseMatch {
        matched: negated || !positions.is_empty(),
        positions,
        negated,
        width,
    }
}

fn phrase_output(
    left: &[u16],
    right: &[u16],
    left_offset: u32,
    right_offset: u32,
    emit_left_only: bool,
    emit_right_only: bool,
    emit_both: bool,
) -> Vec<u16> {
    let mut left_index = 0;
    let mut right_index = 0;
    let mut output = Vec::with_capacity(left.len() + right.len());
    while left_index < left.len() || right_index < right.len() {
        let left_position = left
            .get(left_index)
            .map_or(u32::MAX, |position| u32::from(*position) + left_offset);
        let right_position = right
            .get(right_index)
            .map_or(u32::MAX, |position| u32::from(*position) + right_offset);
        let position = if left_position < right_position {
            left_index += 1;
            emit_left_only.then_some(left_position)
        } else if left_position == right_position {
            left_index += 1;
            right_index += 1;
            emit_both.then_some(left_position)
        } else {
            right_index += 1;
            emit_right_only.then_some(right_position)
        };
        if let Some(position) = position
            && let Ok(position) = u16::try_from(position)
            && position <= MAX_POSITION
        {
            output.push(position);
        }
    }
    output
}

fn invalid(kind: &'static str, input: &str) -> TypeError {
    TypeError::InvalidText {
        type_name: kind,
        value: input.to_string(),
    }
}

impl FromStr for TsVector {
    type Err = TypeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut cursor = Cursor::new(input);
        let mut entries = Vec::new();
        while cursor.skip_space() {
            let text = cursor.lexeme().ok_or_else(|| invalid("tsvector", input))?;
            let mut positions = Vec::new();
            if cursor.eat(':') {
                loop {
                    let position = cursor.number().ok_or_else(|| invalid("tsvector", input))?;
                    let position = u16::try_from(position)
                        .ok()
                        .filter(|position| (1..=MAX_POSITION).contains(position))
                        .ok_or_else(|| invalid("tsvector", input))?;
                    let weight = cursor
                        .peek()
                        .and_then(Weight::parse)
                        .inspect(|_| {
                            cursor.bump();
                        })
                        .unwrap_or(Weight::D);
                    positions.push(Position { position, weight });
                    if !cursor.eat(',') {
                        break;
                    }
                }
            }
            entries.push(Lexeme { text, positions });
        }
        Ok(Self::new(entries))
    }
}

impl FromStr for TsQuery {
    type Err = TypeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.trim().is_empty() {
            return Ok(Self::Empty);
        }
        let mut parser = QueryParser::new(input);
        let query = parser.or().ok_or_else(|| invalid("tsquery", input))?;
        parser.cursor.skip_space();
        if parser.cursor.peek().is_some() {
            return Err(invalid("tsquery", input));
        }
        Ok(query)
    }
}

impl fmt::Display for TsVector {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, entry) in self.0.iter().enumerate() {
            if index != 0 {
                out.write_str(" ")?;
            }
            quoted(out, &entry.text)?;
            if !entry.positions.is_empty() {
                out.write_str(":")?;
                for (position_index, position) in entry.positions.iter().enumerate() {
                    if position_index != 0 {
                        out.write_str(",")?;
                    }
                    write!(out, "{}{}", position.position, position.weight.suffix())?;
                }
            }
        }
        Ok(())
    }
}

impl fmt::Display for TsQuery {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        display_query(self, out, 0)
    }
}

fn display_query(query: &TsQuery, out: &mut fmt::Formatter<'_>, parent: u8) -> fmt::Result {
    let precedence = match query {
        TsQuery::Empty | TsQuery::Term(_) => 5,
        TsQuery::Not(_) => 4,
        TsQuery::Phrase(_, _, _) => 3,
        TsQuery::And(_, _) => 2,
        TsQuery::Or(_, _) => 1,
    };
    let needs_parentheses = precedence < parent;
    if needs_parentheses {
        out.write_str("( ")?;
    }
    match query {
        TsQuery::Empty => {}
        TsQuery::Term(term) => {
            quoted(out, &term.text)?;
            if !term.weights.is_empty() || term.prefix {
                out.write_str(":")?;
                for weight in [Weight::A, Weight::B, Weight::C, Weight::D] {
                    if term.weights.contains(&weight) {
                        out.write_str(match weight {
                            Weight::A => "A",
                            Weight::B => "B",
                            Weight::C => "C",
                            Weight::D => "D",
                        })?;
                    }
                }
                if term.prefix {
                    out.write_str("*")?;
                }
            }
        }
        TsQuery::Not(query) => {
            out.write_str("!")?;
            display_query(query, out, precedence)?;
        }
        TsQuery::And(left, right) => binary_query(left, right, "&", precedence, out)?,
        TsQuery::Or(left, right) => binary_query(left, right, "|", precedence, out)?,
        TsQuery::Phrase(left, right, distance) => {
            let operator = if *distance == 1 {
                "<->".to_string()
            } else {
                format!("<{distance}>")
            };
            binary_query(left, right, &operator, precedence, out)?;
        }
    }
    if needs_parentheses {
        out.write_str(" )")?;
    }
    Ok(())
}

fn binary_query(
    left: &TsQuery,
    right: &TsQuery,
    operator: &str,
    precedence: u8,
    out: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    display_query(left, out, precedence)?;
    write!(out, " {operator} ")?;
    display_query(right, out, precedence + 1)
}

fn quoted(out: &mut fmt::Formatter<'_>, text: &str) -> fmt::Result {
    out.write_str("'")?;
    for character in text.chars() {
        match character {
            '\'' => out.write_str("'")?,
            '\\' => out.write_str("\\")?,
            _ => {}
        }
        out.write_str(character.encode_utf8(&mut [0; 4]))?;
    }
    out.write_str("'")
}

struct QueryParser<'a> {
    input: &'a str,
    cursor: Cursor<'a>,
}

impl<'a> QueryParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input,
            cursor: Cursor::new(input),
        }
    }

    fn or(&mut self) -> Option<TsQuery> {
        let mut query = self.and()?;
        while self.cursor.eat_space('|') {
            query = TsQuery::Or(Box::new(query), Box::new(self.and()?));
        }
        Some(query)
    }

    fn and(&mut self) -> Option<TsQuery> {
        let mut query = self.phrase()?;
        while self.cursor.eat_space('&') {
            query = TsQuery::And(Box::new(query), Box::new(self.phrase()?));
        }
        Some(query)
    }

    fn phrase(&mut self) -> Option<TsQuery> {
        let mut query = self.not()?;
        loop {
            self.cursor.skip_space();
            let checkpoint = self.cursor.offset;
            if !self.cursor.eat('<') {
                break;
            }
            let distance = if self.cursor.eat('-') {
                if !self.cursor.eat('>') {
                    self.cursor.offset = checkpoint;
                    break;
                }
                1
            } else {
                let distance = self.cursor.number()?;
                if !self.cursor.eat('>') || distance > u32::from(MAX_PHRASE_DISTANCE) {
                    return None;
                }
                u16::try_from(distance).ok()?
            };
            query = TsQuery::Phrase(Box::new(query), Box::new(self.not()?), distance);
        }
        Some(query)
    }

    fn not(&mut self) -> Option<TsQuery> {
        self.cursor.skip_space();
        if self.cursor.eat('!') {
            return Some(TsQuery::Not(Box::new(self.not()?)));
        }
        self.primary()
    }

    fn primary(&mut self) -> Option<TsQuery> {
        self.cursor.skip_space();
        if self.cursor.eat('(') {
            let query = self.or()?;
            self.cursor.skip_space();
            return self.cursor.eat(')').then_some(query);
        }
        let text = self.cursor.lexeme()?;
        let mut weights = Vec::new();
        let mut prefix = false;
        if self.cursor.eat(':') {
            while let Some(character) = self.cursor.peek() {
                if character == '*' {
                    prefix = true;
                    self.cursor.bump();
                } else if let Some(weight) = Weight::parse(character) {
                    if !weights.contains(&weight) {
                        weights.push(weight);
                    }
                    self.cursor.bump();
                } else {
                    break;
                }
            }
        }
        if text.is_empty() {
            return None;
        }
        let _ = self.input;
        Some(TsQuery::Term(QueryTerm {
            text,
            weights,
            prefix,
        }))
    }
}

struct Cursor<'a> {
    input: &'a str,
    offset: usize,
}

impl<'a> Cursor<'a> {
    const fn new(input: &'a str) -> Self {
        Self { input, offset: 0 }
    }

    fn peek(&self) -> Option<char> {
        self.input[self.offset..].chars().next()
    }

    fn bump(&mut self) -> Option<char> {
        let character = self.peek()?;
        self.offset += character.len_utf8();
        Some(character)
    }

    fn eat(&mut self, expected: char) -> bool {
        if self.peek() == Some(expected) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn skip_space(&mut self) -> bool {
        while self.peek().is_some_and(char::is_whitespace) {
            self.bump();
        }
        self.peek().is_some()
    }

    fn eat_space(&mut self, expected: char) -> bool {
        self.skip_space();
        self.eat(expected)
    }

    fn number(&mut self) -> Option<u32> {
        let start = self.offset;
        while self
            .peek()
            .is_some_and(|character| character.is_ascii_digit())
        {
            self.bump();
        }
        (start != self.offset)
            .then(|| self.input[start..self.offset].parse().ok())
            .flatten()
    }

    fn lexeme(&mut self) -> Option<String> {
        self.skip_space();
        if self.eat('\'') {
            let mut text = String::new();
            loop {
                let character = self.bump()?;
                if character == '\'' {
                    if self.eat('\'') {
                        text.push('\'');
                    } else {
                        break;
                    }
                } else if character == '\\' {
                    text.push(self.bump()?);
                } else {
                    text.push(character);
                }
            }
            return Some(text);
        }
        let start = self.offset;
        while self
            .peek()
            .is_some_and(|character| !character.is_whitespace() && !"()!&|:<>".contains(character))
        {
            self.bump();
        }
        (start != self.offset).then(|| self.input[start..self.offset].to_string())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn vector_normalizes_and_round_trips() {
        let vector: TsVector = "'fat':4,2A 'cat':3 'fat':2A".parse().expect("vector");
        assert_eq!(vector.to_string(), "'cat':3 'fat':2A,4");
        assert_eq!(
            vector.to_string().parse::<TsVector>().expect("round trip"),
            vector
        );
    }

    #[test]
    fn query_precedence_and_phrase_match() {
        let query: TsQuery = "fat & (cat | rat)".parse().expect("query");
        assert_eq!(query.to_string(), "'fat' & ( 'cat' | 'rat' )");
        let vector: TsVector = "'fat':1 'cat':2".parse().expect("vector");
        assert!(vector.matches(&"fat <-> cat".parse().expect("phrase")));
        assert!(!vector.matches(&"cat <-> fat".parse().expect("reverse")));
    }

    #[test]
    fn query_supports_weights_prefix_and_negation() {
        let vector: TsVector = "'cat':1A 'cater':2B 'rat':3".parse().expect("vector");
        assert!(vector.matches(&"cat:A".parse().expect("weight")));
        assert!(vector.matches(&"cat:*B".parse().expect("prefix")));
        assert!(vector.matches(&"cat & !dog".parse().expect("not")));
    }

    #[test]
    fn unpositioned_lexemes_match_every_weight() {
        let vector: TsVector = "'wd'".parse().expect("vector");

        assert!(vector.matches(&"wd:A".parse().expect("A query")));
        assert!(vector.matches(&"wd:D".parse().expect("D query")));
    }

    #[test]
    fn failed_or_branches_do_not_leak_phrase_positions() {
        let vector: TsVector = "'a':1 'd':2 'c':10".parse().expect("vector");
        let query = "((a & b) | c) <-> d".parse().expect("query");
        assert!(!vector.matches(&query));
    }

    #[test]
    fn phrase_matching_tracks_negated_positions() {
        let missing_left: TsVector = "'yh':2".parse().expect("vector");
        assert!(missing_left.matches(&"!pl <-> yh".parse().expect("query")));

        let adjacent: TsVector = "'pl':1 'yh':2".parse().expect("vector");
        assert!(!adjacent.matches(&"!pl <-> yh".parse().expect("query")));
        assert!(adjacent.matches(&"!pl <-> !yh".parse().expect("query")));
        assert!(adjacent.matches(&"!yh <-> pl".parse().expect("query")));

        let distant_right: TsVector = "'qt':3".parse().expect("vector");
        assert!(distant_right.matches(&"!qe <2> qt".parse().expect("query")));
    }

    #[test]
    fn cover_density_rank_uses_minimal_positioned_covers() {
        let query: TsQuery = "a & b".parse().expect("query");
        let adjacent: TsVector = "'a':1 'b':2".parse().expect("vector");
        let gapped: TsVector = "'a':1 'b':3".parse().expect("vector");
        let repeated: TsVector = "'a':1,5 'b':2".parse().expect("vector");
        let stripped: TsVector = "'a' 'b':2".parse().expect("vector");
        let same_position: TsVector = "'a':1 'sa':2A 'sb':2D".parse().expect("vector");

        assert!((adjacent.rank_cd(&query) - 0.1).abs() < f32::EPSILON);
        assert!((gapped.rank_cd(&query) - 0.05).abs() < f32::EPSILON);
        assert!((repeated.rank_cd(&query) - 0.133_333_34).abs() < f32::EPSILON);
        assert!(stripped.rank_cd(&query) == 0.0);
        assert!(
            (same_position.rank_cd(&"a <-> s:*".parse().expect("prefix phrase")) - 0.1).abs()
                < f32::EPSILON
        );
    }

    #[test]
    fn query_canonical_text_round_trips_escapes_and_default_weight() {
        let query: TsQuery = r"'can''t\\stop':D".parse().expect("query");
        assert_eq!(query.to_string(), r"'can''t\\stop':D");
        assert_eq!(
            query.to_string().parse::<TsQuery>().expect("round trip"),
            query
        );
    }

    #[test]
    fn query_rewrite_matches_associative_subtrees_and_removes_empty_replacements() {
        let rewritten = "foo & bar & qq & new & york"
            .parse::<TsQuery>()
            .expect("query")
            .rewrite(
                &"york & new".parse().expect("target"),
                &"big & apple | nyc | new & york & city"
                    .parse()
                    .expect("replacement"),
            );
        assert_eq!(
            rewritten.to_string(),
            "'foo' & 'bar' & 'qq' & ( 'city' & 'new' & 'york' | 'nyc' | 'big' & 'apple' )"
        );

        let removed = "5 & (6 | 5)"
            .parse::<TsQuery>()
            .expect("query")
            .rewrite(&"5".parse().expect("target"), &TsQuery::Empty);
        assert_eq!(removed.to_string(), "'6'");
        assert_eq!(
            "!5".parse::<TsQuery>()
                .expect("query")
                .rewrite(&"5".parse().expect("target"), &TsQuery::Empty),
            TsQuery::Empty
        );
    }
}
