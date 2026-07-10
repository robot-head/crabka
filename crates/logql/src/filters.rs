use std::{cmp::Ordering, net::IpAddr};

use regex::Regex;

use crate::{
    Labels, ParseError,
    stream::{PipelineStage, insert_extracted_field},
    util::{parse_bytes_literal, parse_prometheus_duration_literal},
};

#[derive(Clone, Debug, PartialEq)]
pub struct FieldFilter {
    pub name: String,
    pub op: ComparisonOp,
    pub value: FieldValue,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FieldFilterChain {
    first: FieldFilter,
    rest: Vec<(FieldFilterLogicOp, FieldFilter)>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum FieldFilterExpression {
    Filter(FieldFilter),
    Group(Box<FieldFilterExpression>),
    Chain {
        first: Box<FieldFilterExpression>,
        rest: Vec<(FieldFilterLogicOp, FieldFilterExpression)>,
    },
}

impl FieldFilterExpression {
    #[must_use]
    pub fn apply(&self, fields: &mut Labels) -> bool {
        match self {
            Self::Filter(filter) => filter.apply(fields),
            Self::Group(expression) => expression.apply(fields),
            Self::Chain { first, rest } => {
                let mut result = first.apply(fields);
                for (op, expression) in rest {
                    match op {
                        FieldFilterLogicOp::And => result = result && expression.apply(fields),
                        FieldFilterLogicOp::Or => result = result || expression.apply(fields),
                    }
                }
                result
            }
        }
    }

    #[must_use]
    pub fn matches(&self, fields: &Labels) -> bool {
        let mut fields = fields.clone();
        self.apply(&mut fields)
    }
}

pub(crate) fn field_filter_expression_to_pipeline_stage(
    expression: FieldFilterExpression,
) -> PipelineStage {
    match expression {
        FieldFilterExpression::Filter(filter) => PipelineStage::FieldFilter(filter),
        FieldFilterExpression::Chain { first, rest } => {
            let first = match *first {
                FieldFilterExpression::Filter(filter) => filter,
                first => {
                    return PipelineStage::FieldFilterExpression(FieldFilterExpression::Chain {
                        first: Box::new(first),
                        rest,
                    });
                }
            };

            let mut flat_rest = Vec::new();
            for (op, expression) in rest {
                let filter = match expression {
                    FieldFilterExpression::Filter(filter) => filter,
                    expression => {
                        let first = Box::new(FieldFilterExpression::Filter(first));
                        let rest = flat_rest
                            .into_iter()
                            .map(|(op, filter)| (op, FieldFilterExpression::Filter(filter)))
                            .chain(std::iter::once((op, expression)))
                            .collect();
                        return PipelineStage::FieldFilterExpression(
                            FieldFilterExpression::Chain { first, rest },
                        );
                    }
                };
                flat_rest.push((op, filter));
            }

            PipelineStage::FieldFilterChain(FieldFilterChain::new(first, flat_rest))
        }
        expression => PipelineStage::FieldFilterExpression(expression),
    }
}

impl FieldFilterChain {
    #[must_use]
    pub fn new(first: FieldFilter, rest: Vec<(FieldFilterLogicOp, FieldFilter)>) -> Self {
        Self { first, rest }
    }

    #[must_use]
    pub fn matches(&self, fields: &Labels) -> bool {
        let mut fields = fields.clone();
        self.apply(&mut fields)
    }

    pub fn apply(&self, fields: &mut Labels) -> bool {
        let mut result = self.first.apply(fields);
        for (op, filter) in &self.rest {
            match op {
                FieldFilterLogicOp::And => result = result && filter.apply(fields),
                FieldFilterLogicOp::Or => result = result || filter.apply(fields),
            }
        }
        result
    }

    #[must_use]
    pub fn first(&self) -> &FieldFilter {
        &self.first
    }

    #[must_use]
    pub fn rest(&self) -> &[(FieldFilterLogicOp, FieldFilter)] {
        &self.rest
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FieldFilterLogicOp {
    And,
    Or,
}

impl FieldFilter {
    #[must_use]
    pub fn new(name: impl Into<String>, op: ComparisonOp, value: FieldValue) -> Self {
        Self {
            name: name.into(),
            op,
            value,
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(op = ?op), err)]
    pub fn try_new(
        name: impl Into<String>,
        op: ComparisonOp,
        value: FieldValue,
    ) -> Result<Self, ParseError> {
        let filter = Self::new(name, op, value);
        filter.validate()?;
        Ok(filter)
    }

    #[must_use]
    pub fn matches(&self, fields: &Labels) -> bool {
        let mut fields = fields.clone();
        self.apply(&mut fields)
    }

    pub fn apply(&self, fields: &mut Labels) -> bool {
        let candidate = fields
            .get(&self.name)
            .map_or("", String::as_str)
            .to_string();

        match &self.value {
            FieldValue::Number(expected) => {
                if !fields.contains_key(&self.name) {
                    return false;
                }
                match candidate.parse::<f64>() {
                    Ok(candidate) => self.op.compare_numbers(candidate, *expected),
                    Err(_) => {
                        insert_extracted_field(fields, "__error__", "LabelFilterErr".to_string());
                        insert_extracted_field(
                            fields,
                            "__error_details__",
                            format!(r#"strconv.ParseFloat: parsing "{candidate}": invalid syntax"#),
                        );
                        true
                    }
                }
            }
            FieldValue::Duration(expected) => parse_prometheus_duration_literal(&candidate)
                .is_some_and(|candidate| {
                    self.op.compare_numbers(candidate as f64, *expected as f64)
                }),
            FieldValue::Bytes(expected) => parse_bytes_literal(&candidate)
                .is_some_and(|candidate| self.op.compare_numbers(candidate, *expected)),
            FieldValue::String(expected) => self.op.compare_strings(&candidate, expected),
            FieldValue::Ip(expected) => match self.op {
                ComparisonOp::Equal => expected.matches_ip_text(&candidate),
                ComparisonOp::NotEqual => !expected.matches_ip_text(&candidate),
                _ => false,
            },
        }
    }

    fn validate(&self) -> Result<(), ParseError> {
        if matches!(
            self.op,
            ComparisonOp::RegexEqual | ComparisonOp::RegexNotEqual
        ) {
            let FieldValue::String(pattern) = &self.value else {
                return Err(ParseError::Syntax {
                    message: "expected string regex field comparison value".to_string(),
                    position: 0,
                });
            };
            Regex::new(pattern).map_err(|source| ParseError::InvalidRegex {
                pattern: pattern.clone(),
                source,
            })?;
        }
        if matches!(self.value, FieldValue::Ip(_))
            && !matches!(self.op, ComparisonOp::Equal | ComparisonOp::NotEqual)
        {
            return Err(ParseError::Syntax {
                message: "ip field comparisons only support = and !=".to_string(),
                position: 0,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComparisonOp {
    Equal,
    NotEqual,
    RegexEqual,
    RegexNotEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
}

impl ComparisonOp {
    fn compare_numbers(self, candidate: f64, expected: f64) -> bool {
        candidate
            .partial_cmp(&expected)
            .is_some_and(|ordering| match self {
                Self::Equal => ordering == Ordering::Equal,
                Self::NotEqual => ordering != Ordering::Equal,
                Self::RegexEqual | Self::RegexNotEqual => false,
                Self::Greater => ordering == Ordering::Greater,
                Self::GreaterEqual => matches!(ordering, Ordering::Greater | Ordering::Equal),
                Self::Less => ordering == Ordering::Less,
                Self::LessEqual => matches!(ordering, Ordering::Less | Ordering::Equal),
            })
    }

    fn compare_strings(self, candidate: &str, expected: &str) -> bool {
        match self {
            Self::Equal => candidate == expected,
            Self::NotEqual => candidate != expected,
            Self::RegexEqual => Regex::new(expected).is_ok_and(|regex| regex.is_match(candidate)),
            Self::RegexNotEqual => {
                Regex::new(expected).is_ok_and(|regex| !regex.is_match(candidate))
            }
            Self::Greater => candidate > expected,
            Self::GreaterEqual => candidate >= expected,
            Self::Less => candidate < expected,
            Self::LessEqual => candidate <= expected,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum FieldValue {
    Number(f64),
    Duration(i64),
    Bytes(f64),
    String(String),
    Ip(IpMatcher),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LineFilter {
    pub op: LineFilterOp,
    pub pattern: String,
    ip_matcher: Option<IpMatcher>,
}

impl LineFilter {
    #[tracing::instrument(level = "debug", skip_all, fields(op = ?op), err)]
    pub fn new(op: LineFilterOp, pattern: impl Into<String>) -> Result<Self, ParseError> {
        let filter = Self {
            op,
            pattern: pattern.into(),
            ip_matcher: None,
        };
        filter.validate()?;
        Ok(filter)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(op = ?op), err)]
    pub fn ip(op: LineFilterOp, pattern: impl Into<String>) -> Result<Self, ParseError> {
        let pattern = pattern.into();
        let filter = Self {
            op,
            ip_matcher: Some(IpMatcher::parse(&pattern)?),
            pattern,
        };
        filter.validate()?;
        Ok(filter)
    }

    #[must_use]
    pub fn is_ip_matcher(&self) -> bool {
        self.ip_matcher.is_some()
    }

    #[must_use]
    pub fn matches(&self, line: &str) -> bool {
        if let Some(matcher) = &self.ip_matcher {
            return match self.op {
                LineFilterOp::Contains => matcher.matches_line(line),
                LineFilterOp::NotContains => !matcher.matches_line(line),
                _ => false,
            };
        }
        match self.op {
            LineFilterOp::Contains => line.contains(&self.pattern),
            LineFilterOp::NotContains => !line.contains(&self.pattern),
            LineFilterOp::Regex => self.regex().is_match(line),
            LineFilterOp::NotRegex => !self.regex().is_match(line),
            LineFilterOp::Pattern => line_matches_pattern(line, &self.pattern),
            LineFilterOp::NotPattern => !line_matches_pattern(line, &self.pattern),
        }
    }

    fn validate(&self) -> Result<(), ParseError> {
        if self.ip_matcher.is_some()
            && !matches!(self.op, LineFilterOp::Contains | LineFilterOp::NotContains)
        {
            return Err(ParseError::Syntax {
                message: "ip line filters only support |= and !=".to_string(),
                position: 0,
            });
        }
        if matches!(self.op, LineFilterOp::Regex | LineFilterOp::NotRegex) {
            Regex::new(&self.pattern).map_err(|source| ParseError::InvalidRegex {
                pattern: self.pattern.clone(),
                source,
            })?;
        }
        Ok(())
    }

    fn regex(&self) -> Regex {
        Regex::new(&self.pattern).expect("line regex filter validated at construction")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IpMatcher {
    pattern: String,
    range: IpRange,
}

impl IpMatcher {
    #[tracing::instrument(level = "debug", skip_all, fields(pattern = %pattern), err)]
    pub fn parse(pattern: &str) -> Result<Self, ParseError> {
        let range = if let Some((start, end)) = pattern.split_once('-') {
            IpRange::range(parse_ip_addr(start)?, parse_ip_addr(end)?)?
        } else if let Some((base, prefix)) = pattern.split_once('/') {
            let base = parse_ip_addr(base)?;
            let prefix = prefix.parse::<u8>().map_err(|_| ParseError::Syntax {
                message: "invalid ip CIDR prefix".to_string(),
                position: 0,
            })?;
            IpRange::cidr(base, prefix)?
        } else {
            IpRange::single(parse_ip_addr(pattern)?)
        };

        Ok(Self {
            pattern: pattern.to_string(),
            range,
        })
    }

    #[must_use]
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    #[must_use]
    fn matches_ip_text(&self, value: &str) -> bool {
        value
            .parse::<IpAddr>()
            .is_ok_and(|addr| self.range.contains(addr))
    }

    #[must_use]
    fn matches_line(&self, line: &str) -> bool {
        ip_candidate_tokens(line).any(|candidate| self.matches_ip_text(candidate))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct IpRange {
    family: IpFamily,
    start: u128,
    end: u128,
}

impl IpRange {
    fn single(addr: IpAddr) -> Self {
        let (family, value) = ip_to_value(addr);
        Self {
            family,
            start: value,
            end: value,
        }
    }

    fn range(start: IpAddr, end: IpAddr) -> Result<Self, ParseError> {
        let (start_family, start) = ip_to_value(start);
        let (end_family, end) = ip_to_value(end);
        if start_family != end_family || start > end {
            return Err(ParseError::Syntax {
                message: "invalid ip range".to_string(),
                position: 0,
            });
        }
        Ok(Self {
            family: start_family,
            start,
            end,
        })
    }

    fn cidr(base: IpAddr, prefix: u8) -> Result<Self, ParseError> {
        let (family, value) = ip_to_value(base);
        let bits = family.bits();
        if prefix > bits {
            return Err(ParseError::Syntax {
                message: "invalid ip CIDR prefix".to_string(),
                position: 0,
            });
        }
        let host_bits = bits - prefix;
        let mask = if prefix == 0 {
            0
        } else {
            (!0_u128) << u32::from(host_bits)
        } & family.mask();
        let start = value & mask;
        let host_mask = !mask & family.mask();
        let end = start.saturating_add(host_mask);
        Ok(Self { family, start, end })
    }

    fn contains(&self, addr: IpAddr) -> bool {
        let (family, value) = ip_to_value(addr);
        family == self.family && value >= self.start && value <= self.end
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IpFamily {
    V4,
    V6,
}

impl IpFamily {
    fn bits(self) -> u8 {
        match self {
            Self::V4 => 32,
            Self::V6 => 128,
        }
    }

    fn mask(self) -> u128 {
        match self {
            Self::V4 => u128::from(u32::MAX),
            Self::V6 => u128::MAX,
        }
    }
}

fn parse_ip_addr(value: &str) -> Result<IpAddr, ParseError> {
    value.parse().map_err(|_| ParseError::Syntax {
        message: "invalid ip pattern".to_string(),
        position: 0,
    })
}

fn ip_to_value(addr: IpAddr) -> (IpFamily, u128) {
    match addr {
        IpAddr::V4(addr) => (IpFamily::V4, u128::from(u32::from(addr))),
        IpAddr::V6(addr) => (IpFamily::V6, u128::from(addr)),
    }
}

fn ip_candidate_tokens(line: &str) -> impl Iterator<Item = &str> {
    line.split(|ch: char| !(ch.is_ascii_hexdigit() || ch == '.' || ch == ':'))
        .filter(|candidate| !candidate.is_empty())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LineFilterOp {
    Contains,
    NotContains,
    Regex,
    NotRegex,
    Pattern,
    NotPattern,
}

fn line_matches_pattern(line: &str, pattern: &str) -> bool {
    let mut remaining = line;
    let mut matched_any_literal = false;
    for literal in pattern.split("<_>").filter(|literal| !literal.is_empty()) {
        let Some(offset) = remaining.find(literal) else {
            return false;
        };
        matched_any_literal = true;
        remaining = &remaining[offset + literal.len()..];
    }
    matched_any_literal || pattern.contains("<_>")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Labels;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    fn number_filter(name: &str, op: ComparisonOp, expected: f64) -> FieldFilter {
        FieldFilter::new(name, op, FieldValue::Number(expected))
    }

    fn string_filter(name: &str, op: ComparisonOp, expected: &str) -> FieldFilter {
        FieldFilter::new(name, op, FieldValue::String(expected.to_string()))
    }

    #[test]
    fn field_filter_matches_returns_candidate_result() {
        let filter = number_filter("status", ComparisonOp::GreaterEqual, 500.0);

        for (name, status, expected) in [
            ("boundary matches", "500", true),
            ("below boundary does not match", "499", false),
        ] {
            assert_eq!(
                filter.matches(&labels(&[("status", status)])),
                expected,
                "case {name}"
            );
        }
    }

    #[test]
    fn field_filter_expression_matches_honors_logic() {
        let expression = FieldFilterExpression::Chain {
            first: Box::new(FieldFilterExpression::Group(Box::new(
                FieldFilterExpression::Filter(number_filter(
                    "status",
                    ComparisonOp::GreaterEqual,
                    500.0,
                )),
            ))),
            rest: vec![(
                FieldFilterLogicOp::Or,
                FieldFilterExpression::Filter(string_filter("level", ComparisonOp::Equal, "warn")),
            )],
        };

        for (pairs, expected) in [
            ([("status", "500"), ("level", "info")], true),
            ([("status", "200"), ("level", "warn")], true),
            ([("status", "200"), ("level", "info")], false),
        ] {
            assert_eq!(expression.matches(&labels(&pairs)), expected, "{pairs:?}");
        }
    }

    #[test]
    fn field_filter_chain_matches_and_exposes_rest_filters() {
        let rest_filter = string_filter("path", ComparisonOp::NotEqual, "/health");
        let chain = FieldFilterChain::new(
            number_filter("status", ComparisonOp::GreaterEqual, 500.0),
            vec![(FieldFilterLogicOp::And, rest_filter.clone())],
        );

        for (name, path, expected) in [
            ("non-health path", "/checkout", true),
            ("excluded health path", "/health", false),
        ] {
            assert_eq!(
                chain.matches(&labels(&[("status", "500"), ("path", path)])),
                expected,
                "case {name}"
            );
        }
        assert_eq!(chain.rest(), &[(FieldFilterLogicOp::And, rest_filter)]);
    }

    #[test]
    fn field_filter_validation_rejects_invalid_combinations() {
        for (name, op, value) in [
            (
                "path",
                ComparisonOp::RegexEqual,
                FieldValue::String("[".to_string()),
            ),
            ("path", ComparisonOp::RegexEqual, FieldValue::Number(1.0)),
            (
                "remote_addr",
                ComparisonOp::Greater,
                FieldValue::Ip(IpMatcher::parse("192.168.1.1").unwrap()),
            ),
        ] {
            assert!(
                FieldFilter::try_new(name, op, value.clone()).is_err(),
                "{name} {op:?} {value:?}"
            );
        }
    }

    #[test]
    fn comparison_ops_compare_number_boundaries() {
        for (op, candidate, expected, result) in [
            (ComparisonOp::Equal, 500.0, 500.0, true),
            (ComparisonOp::Equal, 499.0, 500.0, false),
            (ComparisonOp::NotEqual, 499.0, 500.0, true),
            (ComparisonOp::NotEqual, 500.0, 500.0, false),
            (ComparisonOp::Greater, 501.0, 500.0, true),
            (ComparisonOp::Greater, 500.0, 500.0, false),
            (ComparisonOp::GreaterEqual, 501.0, 500.0, true),
            (ComparisonOp::GreaterEqual, 500.0, 500.0, true),
            (ComparisonOp::GreaterEqual, 499.0, 500.0, false),
            (ComparisonOp::Less, 499.0, 500.0, true),
            (ComparisonOp::Less, 500.0, 500.0, false),
            (ComparisonOp::LessEqual, 499.0, 500.0, true),
            (ComparisonOp::LessEqual, 500.0, 500.0, true),
            (ComparisonOp::LessEqual, 501.0, 500.0, false),
        ] {
            assert_eq!(
                op.compare_numbers(candidate, expected),
                result,
                "{candidate} {op:?} {expected}"
            );
        }
    }

    #[test]
    fn comparison_ops_compare_string_boundaries() {
        for (op, candidate, expected, result) in [
            (ComparisonOp::Greater, "n", "m", true),
            (ComparisonOp::Greater, "m", "m", false),
            (ComparisonOp::GreaterEqual, "n", "m", true),
            (ComparisonOp::GreaterEqual, "m", "m", true),
            (ComparisonOp::GreaterEqual, "l", "m", false),
            (ComparisonOp::Less, "l", "m", true),
            (ComparisonOp::Less, "m", "m", false),
            (ComparisonOp::Less, "n", "m", false),
            (ComparisonOp::LessEqual, "l", "m", true),
            (ComparisonOp::LessEqual, "m", "m", true),
            (ComparisonOp::LessEqual, "n", "m", false),
        ] {
            assert_eq!(
                op.compare_strings(candidate, expected),
                result,
                "{candidate} {op:?} {expected}"
            );
        }
    }

    #[test]
    fn line_filter_reports_ip_matcher_mode() {
        assert!(
            !LineFilter::new(LineFilterOp::Contains, "error")
                .unwrap()
                .is_ip_matcher()
        );
        assert!(
            LineFilter::ip(LineFilterOp::Contains, "192.168.1.0/24")
                .unwrap()
                .is_ip_matcher()
        );
    }

    #[test]
    fn line_filter_validation_rejects_invalid_regex_and_ip_ops() {
        assert!(LineFilter::new(LineFilterOp::Regex, "[").is_err());
        assert!(LineFilter::ip(LineFilterOp::Regex, "192.168.1.1").is_err());
    }

    #[test]
    fn ip_matcher_returns_original_pattern() {
        let matcher = IpMatcher::parse("192.168.1.0/24").unwrap();

        assert_eq!(matcher.pattern(), "192.168.1.0/24");
    }

    #[test]
    fn ip_matcher_rejects_invalid_ranges_and_prefixes() {
        for pattern in [
            "192.168.1.10-192.168.1.1",
            "192.168.1.1-2001:db8::1",
            "192.168.1.1/33",
        ] {
            assert!(IpMatcher::parse(pattern).is_err(), "{pattern}");
        }
    }

    #[test]
    fn ip_matcher_accepts_single_address_ranges_and_host_prefixes() {
        for (name, matcher) in [
            (
                "single-address range",
                IpMatcher::parse("192.168.1.1-192.168.1.1").unwrap(),
            ),
            ("host prefix", IpMatcher::parse("192.168.1.1/32").unwrap()),
        ] {
            assert_eq!(matcher.matches_ip_text("192.168.1.1"), true, "case {name}");
            assert_eq!(matcher.matches_ip_text("192.168.1.2"), false, "case {name}");
        }
    }

    #[test]
    fn line_pattern_matches_wildcard_only_pattern() {
        assert!(line_matches_pattern("anything", "<_>"));
        assert!(!line_matches_pattern("anything", ""));
    }
}
