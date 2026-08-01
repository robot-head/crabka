//! PostgreSQL-compatible `tsvector` and `tsquery` values.

use std::{collections::BTreeMap, fmt, str::FromStr};

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

    /// Concatenate vectors, shifting the right-hand positions after the last
    /// position in the left-hand document.
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
            let positions = vector.positions(term);
            let exists = if positions.is_empty() {
                vector.0.iter().any(|entry| {
                    (if term.prefix {
                        entry.text.starts_with(&term.text)
                    } else {
                        entry.text == term.text
                    }) && term.weights.is_empty()
                        && entry.positions.is_empty()
                })
            } else {
                true
            };
            (exists, positions)
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
            let (left_match, left_positions) = query_matches(left, vector);
            let (right_match, right_positions) = query_matches(right, vector);
            let positions = right_positions
                .into_iter()
                .filter(|right| {
                    left_positions
                        .iter()
                        .any(|left| right.checked_sub(*left) == Some(*distance))
                })
                .collect::<Vec<_>>();
            (
                left_match && right_match && !positions.is_empty(),
                positions,
            )
        }
    }
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
    fn failed_or_branches_do_not_leak_phrase_positions() {
        let vector: TsVector = "'a':1 'd':2 'c':10".parse().expect("vector");
        let query = "((a & b) | c) <-> d".parse().expect("query");
        assert!(!vector.matches(&query));
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
}
