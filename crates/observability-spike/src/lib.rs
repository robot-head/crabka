//! Throwaway Loki storage/query spike.
//!
//! This crate is intentionally small and empirical. It captures the parts of the
//! Loki replacement design that need proof before production crates exist:
//! canonical stream fingerprints, an inverted label index, and Loki-shaped
//! stream results.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use regex::Regex;
use serde_json::{Value, json};
use xxhash_rust::xxh3::xxh3_64;

pub type Labels = BTreeMap<String, String>;
pub type SeriesFingerprint = u64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogEntry {
    pub timestamp_ns: i64,
    pub labels: Labels,
    pub line: String,
}

impl LogEntry {
    #[must_use]
    pub fn new(timestamp_ns: i64, labels: Labels, line: impl Into<String>) -> Self {
        Self {
            timestamp_ns,
            labels,
            line: line.into(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct LogSelector {
    matchers: Vec<LabelMatcher>,
    line_filter: Option<LineFilter>,
}

impl LogSelector {
    #[must_use]
    pub fn new(labels: Labels) -> Self {
        Self {
            matchers: labels
                .into_iter()
                .map(|(name, value)| LabelMatcher::new(name, MatcherOp::Equal, value))
                .collect(),
            line_filter: None,
        }
    }

    #[must_use]
    pub fn contains(mut self, needle: impl Into<String>) -> Self {
        self.line_filter = Some(LineFilter::Contains(needle.into()));
        self
    }

    #[must_use]
    pub fn matches(&self, entry: &LogEntry, start_ns: i64, end_ns: i64) -> bool {
        entry.timestamp_ns >= start_ns
            && entry.timestamp_ns <= end_ns
            && self
                .matchers
                .iter()
                .all(|matcher| matcher.matches(&entry.labels))
            && self
                .line_filter
                .as_ref()
                .is_none_or(|filter| filter.matches(&entry.line))
    }
}

#[derive(Clone, Debug)]
pub struct LabelMatcher {
    name: String,
    op: MatcherOp,
    value: String,
    regex: Option<Regex>,
}

impl LabelMatcher {
    fn new(name: impl Into<String>, op: MatcherOp, value: impl Into<String>) -> Self {
        let value = value.into();
        let regex = match op {
            MatcherOp::RegexEqual | MatcherOp::RegexNotEqual => Regex::new(&value).ok(),
            MatcherOp::Equal | MatcherOp::NotEqual => None,
        };
        Self {
            name: name.into(),
            op,
            value,
            regex,
        }
    }

    fn matches(&self, labels: &Labels) -> bool {
        let candidate = labels.get(&self.name);
        match self.op {
            MatcherOp::Equal => candidate == Some(&self.value),
            MatcherOp::NotEqual => candidate != Some(&self.value),
            MatcherOp::RegexEqual => candidate.is_some_and(|value| {
                self.regex
                    .as_ref()
                    .is_some_and(|regex| regex.is_match(value))
            }),
            MatcherOp::RegexNotEqual => candidate.is_none_or(|value| {
                self.regex
                    .as_ref()
                    .is_none_or(|regex| !regex.is_match(value))
            }),
        }
    }

    fn exact_posting_key(&self) -> Option<(String, String)> {
        (self.op == MatcherOp::Equal).then(|| (self.name.clone(), self.value.clone()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MatcherOp {
    Equal,
    NotEqual,
    RegexEqual,
    RegexNotEqual,
}

#[derive(Clone, Debug)]
enum LineFilter {
    Contains(String),
    NotContains(String),
    Regex(Regex),
    NotRegex(Regex),
}

impl LineFilter {
    fn matches(&self, line: &str) -> bool {
        match self {
            Self::Contains(needle) => line.contains(needle),
            Self::NotContains(needle) => !line.contains(needle),
            Self::Regex(regex) => regex.is_match(line),
            Self::NotRegex(regex) => !regex.is_match(line),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LabelIndex {
    series: BTreeMap<SeriesFingerprint, Labels>,
    postings: BTreeMap<(String, String), BTreeSet<SeriesFingerprint>>,
}

impl LabelIndex {
    pub fn insert_series(&mut self, labels: Labels) -> SeriesFingerprint {
        let fingerprint = series_fingerprint(&labels);
        for (name, value) in &labels {
            self.postings
                .entry((name.clone(), value.clone()))
                .or_default()
                .insert(fingerprint);
        }
        self.series.insert(fingerprint, labels);
        fingerprint
    }

    #[must_use]
    pub fn match_series(&self, selector: &LogSelector) -> BTreeSet<SeriesFingerprint> {
        let mut matched: Option<BTreeSet<SeriesFingerprint>> = None;
        for matcher in &selector.matchers {
            let Some(key) = matcher.exact_posting_key() else {
                continue;
            };
            let Some(next) = self.postings.get(&key) else {
                return BTreeSet::new();
            };
            matched = Some(match matched {
                Some(current) => current.intersection(next).copied().collect(),
                None => next.clone(),
            });
        }
        matched.unwrap_or_else(|| self.series.keys().copied().collect())
    }

    #[must_use]
    pub fn label_names(&self) -> BTreeSet<String> {
        self.postings.keys().map(|(name, _)| name.clone()).collect()
    }

    #[must_use]
    pub fn label_values(&self, label_name: &str) -> BTreeSet<String> {
        self.postings
            .keys()
            .filter(|(name, _)| name == label_name)
            .map(|(_, value)| value.clone())
            .collect()
    }
}

#[must_use]
pub fn labels<const N: usize>(items: [(&str, &str); N]) -> Labels {
    items
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
}

#[must_use]
pub fn series_fingerprint(labels: &Labels) -> SeriesFingerprint {
    let mut canonical = Vec::new();
    for (name, value) in labels {
        append_len_prefixed(&mut canonical, name);
        append_len_prefixed(&mut canonical, value);
    }
    xxh3_64(&canonical)
}

#[must_use]
pub fn loki_streams_response(
    entries: &[LogEntry],
    selector: &LogSelector,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let mut streams: BTreeMap<Labels, Vec<[String; 2]>> = BTreeMap::new();
    for entry in entries
        .iter()
        .filter(|entry| selector.matches(entry, start_ns, end_ns))
    {
        streams
            .entry(entry.labels.clone())
            .or_default()
            .push([entry.timestamp_ns.to_string(), entry.line.clone()]);
    }

    let result: Vec<Value> = streams
        .into_iter()
        .map(|(stream, values)| {
            json!({
                "stream": stream,
                "values": values,
            })
        })
        .collect();

    json!({
        "status": "success",
        "data": {
            "resultType": "streams",
            "result": result,
        }
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParseLogQlError {
    message: String,
}

impl fmt::Display for ParseLogQlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ParseLogQlError {}

pub fn parse_logql(input: &str) -> Result<LogSelector, ParseLogQlError> {
    let mut parser = Parser::new(input);
    parser.parse()
}

struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn parse(&mut self) -> Result<LogSelector, ParseLogQlError> {
        self.skip_ws();
        self.expect('{')?;
        let matchers = self.parse_matchers()?;
        self.expect('}')?;
        self.skip_ws();
        let line_filter = if self.pos == self.input.len() {
            None
        } else {
            Some(self.parse_line_filter()?)
        };
        self.skip_ws();
        if self.pos != self.input.len() {
            return Err(self.error("expected end of query"));
        }
        Ok(LogSelector {
            matchers,
            line_filter,
        })
    }

    fn parse_matchers(&mut self) -> Result<Vec<LabelMatcher>, ParseLogQlError> {
        let mut matchers = Vec::new();
        loop {
            self.skip_ws();
            if self.peek() == Some('}') {
                return Ok(matchers);
            }
            let name = self.parse_ident()?;
            self.skip_ws();
            let op = self.parse_matcher_op()?;
            self.skip_ws();
            let value = self.parse_quoted()?;
            let matcher = LabelMatcher::new(name, op, value);
            if matches!(op, MatcherOp::RegexEqual | MatcherOp::RegexNotEqual)
                && matcher.regex.is_none()
            {
                return Err(self.error("expected valid regex matcher"));
            }
            matchers.push(matcher);
            self.skip_ws();
            match self.peek() {
                Some(',') => {
                    self.pos += 1;
                }
                Some('}') => return Ok(matchers),
                _ => return Err(self.error("expected ',' or '}'")),
            }
        }
    }

    fn parse_matcher_op(&mut self) -> Result<MatcherOp, ParseLogQlError> {
        for (text, op) in [
            ("=~", MatcherOp::RegexEqual),
            ("!~", MatcherOp::RegexNotEqual),
            ("!=", MatcherOp::NotEqual),
            ("=", MatcherOp::Equal),
        ] {
            if self.consume(text) {
                return Ok(op);
            }
        }
        Err(self.error("expected label matcher operator"))
    }

    fn parse_line_filter(&mut self) -> Result<LineFilter, ParseLogQlError> {
        let op = if self.consume("|=") {
            LineFilterOp::Contains
        } else if self.consume("!=") {
            LineFilterOp::NotContains
        } else if self.consume("|~") {
            LineFilterOp::Regex
        } else if self.consume("!~") {
            LineFilterOp::NotRegex
        } else {
            return Err(self.error("expected line filter operator"));
        };
        self.skip_ws();
        let value = self.parse_quoted()?;
        match op {
            LineFilterOp::Contains => Ok(LineFilter::Contains(value)),
            LineFilterOp::NotContains => Ok(LineFilter::NotContains(value)),
            LineFilterOp::Regex => Regex::new(&value)
                .map(LineFilter::Regex)
                .map_err(|_| self.error("expected valid line regex")),
            LineFilterOp::NotRegex => Regex::new(&value)
                .map(LineFilter::NotRegex)
                .map_err(|_| self.error("expected valid line regex")),
        }
    }

    fn parse_ident(&mut self) -> Result<String, ParseLogQlError> {
        let start = self.pos;
        while let Some(ch) = self.peek() {
            if ch == '_' || ch == ':' || ch == '.' || ch.is_ascii_alphanumeric() {
                self.pos += ch.len_utf8();
            } else {
                break;
            }
        }
        if self.pos == start {
            return Err(self.error("expected label name"));
        }
        Ok(self.input[start..self.pos].to_string())
    }

    fn parse_quoted(&mut self) -> Result<String, ParseLogQlError> {
        self.expect('"')?;
        let mut out = String::new();
        while let Some(ch) = self.peek() {
            self.pos += ch.len_utf8();
            match ch {
                '"' => return Ok(out),
                '\\' => {
                    let Some(escaped) = self.peek() else {
                        return Err(self.error("expected escaped character"));
                    };
                    self.pos += escaped.len_utf8();
                    out.push(escaped);
                }
                _ => out.push(ch),
            }
        }
        Err(self.error("expected closing quote"))
    }

    fn expect(&mut self, expected: char) -> Result<(), ParseLogQlError> {
        self.skip_ws();
        if self.peek() == Some(expected) {
            self.pos += expected.len_utf8();
            Ok(())
        } else {
            Err(self.error(&format!("expected '{expected}'")))
        }
    }

    fn consume(&mut self, text: &str) -> bool {
        self.skip_ws();
        if self.input[self.pos..].starts_with(text) {
            self.pos += text.len();
            true
        } else {
            false
        }
    }

    fn skip_ws(&mut self) {
        while let Some(ch) = self.peek() {
            if ch.is_whitespace() {
                self.pos += ch.len_utf8();
            } else {
                break;
            }
        }
    }

    fn peek(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn error(&self, message: &str) -> ParseLogQlError {
        ParseLogQlError {
            message: format!("{message} at byte {}", self.pos),
        }
    }
}

enum LineFilterOp {
    Contains,
    NotContains,
    Regex,
    NotRegex,
}

fn append_len_prefixed(out: &mut Vec<u8>, value: &str) {
    let len = u32::try_from(value.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value.as_bytes());
}
