//! `LogQL` parser front-end for Crabka's Loki-compatible logs path.
//!
//! This slice covers stream selectors, line filters, `json` / `logfmt` /
//! `pattern` / `regexp` parser stages, `line_format`, field filters, range
//! aggregations, vector aggregations, and unwrapped range aggregation samples.
//! Binary operations and the wider PromQL expression surface stay out until the
//! querier has the basic Loki path wired.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::net::IpAddr;

use base64::Engine as _;
use base64::prelude::BASE64_STANDARD;
use chrono::{FixedOffset, LocalResult, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use chrono_tz::Tz;
use crabka_blockstore::{
    BlockDescriptor, BlockIndex, BlockStoreError, LabelIndex, LabelPredicate,
    MatchOp as BlockMatchOp, SeriesFingerprint, TimeRange,
};
use regex::{NoExpand, Regex};
use thiserror::Error;
use time::OffsetDateTime;

pub type Labels = BTreeMap<String, String>;
pub const UNWRAP_SAMPLE_VALUE_LABEL: &str = "__crabka_unwrap_sample_value__";

#[derive(Clone, Debug, PartialEq)]
pub struct StreamQuery {
    pub matchers: Vec<LabelMatcher>,
    pub pipeline: Vec<PipelineStage>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PipelineEvaluation {
    pub fields: Labels,
    pub line: String,
}

impl StreamQuery {
    #[must_use]
    pub fn matches(&self, labels: &Labels, line: &str) -> bool {
        self.matches_with_fields(labels, line, &Labels::new())
    }

    #[must_use]
    pub fn matches_with_fields(
        &self,
        labels: &Labels,
        line: &str,
        initial_fields: &Labels,
    ) -> bool {
        self.evaluate_with_fields(labels, line, initial_fields)
            .is_some()
    }

    #[must_use]
    pub fn matches_with_fields_at(
        &self,
        labels: &Labels,
        line: &str,
        initial_fields: &Labels,
        timestamp_ns: i64,
    ) -> bool {
        self.evaluate_with_fields_at(labels, line, initial_fields, timestamp_ns)
            .is_some()
    }

    #[must_use]
    pub fn evaluate_with_fields(
        &self,
        labels: &Labels,
        line: &str,
        initial_fields: &Labels,
    ) -> Option<PipelineEvaluation> {
        self.evaluate_with_fields_and_timestamp(labels, line, initial_fields, None)
    }

    #[must_use]
    pub fn evaluate_with_fields_at(
        &self,
        labels: &Labels,
        line: &str,
        initial_fields: &Labels,
        timestamp_ns: i64,
    ) -> Option<PipelineEvaluation> {
        self.evaluate_with_fields_and_timestamp(labels, line, initial_fields, Some(timestamp_ns))
    }

    fn evaluate_with_fields_and_timestamp(
        &self,
        labels: &Labels,
        line: &str,
        initial_fields: &Labels,
        timestamp_ns: Option<i64>,
    ) -> Option<PipelineEvaluation> {
        let mut fields = labels.clone();
        fields.extend(initial_fields.clone());
        if !self.matchers.iter().all(|matcher| matcher.matches(labels)) {
            return None;
        }

        let mut line = line.to_string();
        for stage in &self.pipeline {
            if !stage.apply_with_timestamp(&mut line, &mut fields, timestamp_ns) {
                return None;
            }
        }

        Some(PipelineEvaluation { fields, line })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LabelMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
}

impl LabelMatcher {
    pub fn new(
        name: impl Into<String>,
        op: MatchOp,
        value: impl Into<String>,
    ) -> Result<Self, ParseError> {
        let matcher = Self {
            name: name.into(),
            op,
            value: value.into(),
        };
        matcher.validate()?;
        Ok(matcher)
    }

    #[must_use]
    pub fn matches(&self, labels: &Labels) -> bool {
        let candidate = labels.get(&self.name);
        match self.op {
            MatchOp::Equal => candidate == Some(&self.value),
            MatchOp::NotEqual => candidate != Some(&self.value),
            MatchOp::RegexEqual => self.regex().is_match(candidate.map_or("", String::as_str)),
            MatchOp::RegexNotEqual => candidate.is_none_or(|value| !self.regex().is_match(value)),
        }
    }

    #[must_use]
    pub fn matches_empty_value(&self) -> bool {
        match self.op {
            MatchOp::Equal => self.value.is_empty(),
            MatchOp::NotEqual => !self.value.is_empty(),
            MatchOp::RegexEqual => self.regex().is_match(""),
            MatchOp::RegexNotEqual => !self.regex().is_match(""),
        }
    }

    fn validate(&self) -> Result<(), ParseError> {
        if matches!(self.op, MatchOp::RegexEqual | MatchOp::RegexNotEqual) {
            Regex::new(&anchored_regex_pattern(&self.value)).map_err(|source| {
                ParseError::InvalidRegex {
                    pattern: self.value.clone(),
                    source,
                }
            })?;
        }
        Ok(())
    }

    fn regex(&self) -> Regex {
        Regex::new(&anchored_regex_pattern(&self.value))
            .expect("regex matcher validated at construction")
    }
}

fn anchored_regex_pattern(pattern: &str) -> String {
    format!("^(?:{pattern})$")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MatchOp {
    Equal,
    NotEqual,
    RegexEqual,
    RegexNotEqual,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PipelineStage {
    LineFilter(LineFilter),
    Decolorize,
    Parser(ParserStage),
    LineFormat(LineFormat),
    LabelFormat(LabelFormat),
    DropLabels(LabelSelectionSet),
    KeepLabels(LabelSelectionSet),
    Unwrap(UnwrapExpression),
    FieldFilter(FieldFilter),
    FieldFilterChain(FieldFilterChain),
    FieldFilterExpression(FieldFilterExpression),
}

impl PipelineStage {
    #[must_use]
    pub fn matches(&self, line: &str) -> bool {
        self.apply(&mut line.to_string(), &mut Labels::new())
    }

    #[must_use]
    pub fn apply(&self, line: &mut String, fields: &mut Labels) -> bool {
        self.apply_with_timestamp(line, fields, None)
    }

    fn apply_with_timestamp(
        &self,
        line: &mut String,
        fields: &mut Labels,
        timestamp_ns: Option<i64>,
    ) -> bool {
        match self {
            Self::LineFilter(filter) => filter.matches(line),
            Self::Decolorize => {
                *line = decolorize_line(line);
                true
            }
            Self::Parser(parser) => {
                parser.apply(line, fields);
                true
            }
            Self::LineFormat(format) => {
                *line = format.render_with_timestamp(line, fields, timestamp_ns);
                true
            }
            Self::LabelFormat(format) => {
                format.apply_with_timestamp(line, fields, timestamp_ns);
                true
            }
            Self::DropLabels(labels) => {
                labels.apply_drop(fields);
                true
            }
            Self::KeepLabels(labels) => {
                labels.apply_keep(fields);
                true
            }
            Self::Unwrap(unwrap) => {
                unwrap.apply(fields);
                true
            }
            Self::FieldFilter(filter) => filter.apply(fields),
            Self::FieldFilterChain(chain) => chain.apply(fields),
            Self::FieldFilterExpression(expression) => expression.apply(fields),
        }
    }

    #[must_use]
    pub fn mutates_line(&self) -> bool {
        matches!(
            self,
            Self::Decolorize | Self::Parser(ParserStage::Unpack) | Self::LineFormat(_)
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParserStage {
    Json,
    JsonSelected(JsonParserConfig),
    Logfmt,
    LogfmtConfigured(LogfmtParserConfig),
    LogfmtSelected(LogfmtParserConfig),
    Unpack,
    Pattern(PatternParser),
    Regexp(RegexpParser),
}

impl ParserStage {
    fn apply(&self, line: &mut String, fields: &mut Labels) {
        match self {
            Self::Json => parse_json_fields(line, fields),
            Self::JsonSelected(config) => parse_selected_json_fields(line, fields, config),
            Self::Logfmt => parse_logfmt_fields(line, fields),
            Self::LogfmtConfigured(config) => parse_configured_logfmt_fields(line, fields, config),
            Self::LogfmtSelected(config) => parse_selected_logfmt_fields(line, fields, config),
            Self::Unpack => unpack_json_line(line, fields),
            Self::Pattern(parser) => parser.apply(line, fields),
            Self::Regexp(parser) => parser.apply(line, fields),
        }
    }
}

fn parse_json_fields(line: &str, fields: &mut Labels) {
    let Ok(serde_json::Value::Object(object)) = serde_json::from_str(line) else {
        insert_json_parser_error(fields);
        return;
    };

    for (name, value) in object {
        flatten_json_field(&sanitize_json_field_name(&name), &value, fields);
    }
}

fn parse_selected_json_fields(line: &str, fields: &mut Labels, config: &JsonParserConfig) {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
        insert_json_parser_error(fields);
        return;
    };

    for extraction in config.extractions() {
        if let Some(value) = extraction.path().evaluate(&value) {
            insert_extracted_field(
                fields,
                extraction.destination(),
                selected_json_value_to_string(value),
            );
        }
    }
}

fn insert_json_parser_error(fields: &mut Labels) {
    insert_extracted_field(fields, "__error__", "JSONParserErr".to_string());
    insert_extracted_field(
        fields,
        "__error_details__",
        "Value looks like object, but can't find closing '}' symbol".to_string(),
    );
}

fn flatten_json_field(name: &str, value: &serde_json::Value, fields: &mut Labels) {
    match value {
        serde_json::Value::Object(object) => {
            for (child_name, child_value) in object {
                let child_name = sanitize_json_field_name(child_name);
                let flattened_name = if name.is_empty() {
                    child_name
                } else {
                    format!("{name}_{child_name}")
                };
                flatten_json_field(&flattened_name, child_value, fields);
            }
        }
        serde_json::Value::Array(_) => {}
        _ => {
            insert_extracted_field(fields, name, field_value_to_string(value));
        }
    }
}

fn sanitize_json_field_name(name: &str) -> String {
    let mut sanitized = name
        .chars()
        .map(|ch| {
            if ch == '_' || ch == ':' || ch.is_ascii_alphanumeric() {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        sanitized.push('_');
    }
    if sanitized
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_digit())
    {
        sanitized.insert(0, '_');
    }
    sanitized
}

fn field_value_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Null | serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            String::new()
        }
    }
}

fn selected_json_value_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            serde_json::to_string(value).unwrap_or_default()
        }
        _ => field_value_to_string(value),
    }
}

fn parse_logfmt_fields(line: &str, fields: &mut Labels) {
    let mut parser = LogfmtParser::new(line);
    while let Some((key, value)) = parser.next_pair() {
        insert_extracted_field(fields, &sanitize_logfmt_field_name(&key), value);
    }
}

fn parse_configured_logfmt_fields(line: &str, fields: &mut Labels, config: &LogfmtParserConfig) {
    let mut parser = LogfmtParser::new(line);
    loop {
        match parser.next_pair_with_options(config.keep_empty(), config.strict()) {
            Ok(Some((key, value))) => {
                insert_extracted_field(fields, &sanitize_logfmt_field_name(&key), value);
            }
            Ok(None) => break,
            Err(details) => {
                insert_logfmt_parser_error(fields, details);
                break;
            }
        }
    }
}

fn parse_selected_logfmt_fields(line: &str, fields: &mut Labels, config: &LogfmtParserConfig) {
    let mut parsed = Labels::new();
    let mut parser = LogfmtParser::new(line);
    loop {
        match parser.next_pair_with_options(true, config.strict()) {
            Ok(Some((key, value))) => {
                parsed.entry(key).or_insert(value);
            }
            Ok(None) => break,
            Err(details) => {
                insert_logfmt_parser_error(fields, details);
                break;
            }
        }
    }

    for extraction in config.extractions() {
        let value = parsed.get(extraction.source()).cloned().unwrap_or_default();
        insert_extracted_field(fields, extraction.destination(), value);
    }
}

fn insert_logfmt_parser_error(fields: &mut Labels, details: String) {
    insert_extracted_field(fields, "__error__", "LogfmtParserErr".to_string());
    insert_extracted_field(fields, "__error_details__", details);
}

fn sanitize_logfmt_field_name(name: &str) -> String {
    let mut sanitized = String::new();
    let mut last_was_underscore = false;
    for ch in name.chars() {
        if ch == '_' || ch == ':' || ch.is_ascii_alphanumeric() {
            sanitized.push(ch);
            last_was_underscore = false;
        } else if !last_was_underscore {
            sanitized.push('_');
            last_was_underscore = true;
        }
    }
    if sanitized.is_empty() {
        sanitized.push('_');
    }
    if sanitized
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_digit())
    {
        sanitized.insert(0, '_');
    }
    sanitized
}

fn unpack_json_line(line: &mut String, fields: &mut Labels) {
    let Ok(serde_json::Value::Object(object)) = serde_json::from_str(line) else {
        insert_json_parser_error(fields);
        return;
    };

    let mut replacement = None;
    for (name, value) in object {
        if name == "_entry" {
            if let serde_json::Value::String(entry) = value {
                replacement = Some(entry);
            }
            continue;
        }

        flatten_json_field(&sanitize_json_field_name(&name), &value, fields);
    }

    if let Some(entry) = replacement {
        *line = entry;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PatternParser {
    pattern: String,
    parts: Vec<PatternPart>,
}

impl PatternParser {
    pub fn new(pattern: impl Into<String>) -> Result<Self, ParseError> {
        let pattern = pattern.into();
        let parts = parse_pattern_parts(&pattern)?;
        Ok(Self { pattern, parts })
    }

    #[must_use]
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    fn apply(&self, line: &str, fields: &mut Labels) {
        let Some(captures) = self.captures(line) else {
            insert_pattern_parser_error(fields);
            return;
        };

        for (name, value) in captures {
            if name != "_" {
                insert_extracted_field(fields, &name, value);
            }
        }
    }

    fn captures(&self, line: &str) -> Option<Vec<(String, String)>> {
        let mut pos = 0;
        let mut captures = Vec::new();
        for (index, part) in self.parts.iter().enumerate() {
            match part {
                PatternPart::Literal(literal) => {
                    if !line[pos..].starts_with(literal) {
                        return None;
                    }
                    pos += literal.len();
                }
                PatternPart::Capture(name) => {
                    let next_literal = self.parts[index + 1..].iter().find_map(|part| {
                        if let PatternPart::Literal(literal) = part {
                            Some(literal.as_str())
                        } else {
                            None
                        }
                    });
                    let value_end = if let Some(next_literal) = next_literal {
                        pos + line[pos..].find(next_literal)?
                    } else {
                        line.len()
                    };
                    captures.push((name.clone(), line[pos..value_end].to_string()));
                    pos = value_end;
                }
            }
        }
        Some(captures)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PatternPart {
    Capture(String),
    Literal(String),
}

fn parse_pattern_parts(pattern: &str) -> Result<Vec<PatternPart>, ParseError> {
    let mut pos = 0;
    let mut parts = Vec::new();
    let mut named_captures = 0;
    let mut previous_capture = false;
    let mut separator_since_capture = String::new();

    while pos < pattern.len() {
        let Some(open_offset) = pattern[pos..].find('<') else {
            let literal = &pattern[pos..];
            if !literal.is_empty() {
                separator_since_capture.push_str(literal);
                parts.push(PatternPart::Literal(literal.to_string()));
            }
            break;
        };

        let literal_start = pos;
        let open = pos + open_offset;
        let literal = &pattern[literal_start..open];
        if !literal.is_empty() {
            separator_since_capture.push_str(literal);
            parts.push(PatternPart::Literal(literal.to_string()));
        }

        let capture_start = open + 1;
        let close_offset = pattern[capture_start..]
            .find('>')
            .ok_or_else(|| pattern_parse_error("expected closing pattern capture"))?;
        let close = capture_start + close_offset;
        let name = &pattern[capture_start..close];
        if name.is_empty() {
            return Err(pattern_parse_error("expected pattern capture name"));
        }
        if previous_capture && !separator_since_capture.chars().any(char::is_whitespace) {
            return Err(pattern_parse_error(
                "expected whitespace between pattern captures",
            ));
        }
        if name != "_" {
            named_captures += 1;
        }
        parts.push(PatternPart::Capture(name.to_string()));
        previous_capture = true;
        separator_since_capture.clear();
        pos = close + 1;
    }

    if named_captures == 0 {
        return Err(pattern_parse_error(
            "pattern parser requires at least one named capture",
        ));
    }
    Ok(parts)
}

fn pattern_parse_error(message: &str) -> ParseError {
    ParseError::Syntax {
        message: message.to_string(),
        position: 0,
    }
}

fn insert_pattern_parser_error(fields: &mut Labels) {
    insert_extracted_field(fields, "__error__", "PatternParserErr".to_string());
    insert_extracted_field(
        fields,
        "__error_details__",
        "pattern parser failed to match log line".to_string(),
    );
}

#[derive(Clone, Debug)]
pub struct RegexpParser {
    pattern: String,
    regex: Regex,
    capture_names: Vec<String>,
}

impl RegexpParser {
    pub fn new(pattern: impl Into<String>) -> Result<Self, ParseError> {
        let pattern = pattern.into();
        let regex = Regex::new(&pattern).map_err(|error| regexp_parse_error(&error.to_string()))?;
        let capture_names = regex
            .capture_names()
            .flatten()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        if capture_names.is_empty() {
            return Err(regexp_parse_error(
                "regexp parser requires at least one named capture",
            ));
        }

        Ok(Self {
            pattern,
            regex,
            capture_names,
        })
    }

    #[must_use]
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    fn apply(&self, line: &str, fields: &mut Labels) {
        let Some(captures) = self.regex.captures(line) else {
            insert_regexp_parser_error(fields);
            return;
        };

        for name in &self.capture_names {
            if let Some(value) = captures.name(name) {
                insert_extracted_field(fields, name, value.as_str().to_string());
            }
        }
    }
}

impl PartialEq for RegexpParser {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern && self.capture_names == other.capture_names
    }
}

impl Eq for RegexpParser {}

fn regexp_parse_error(message: &str) -> ParseError {
    ParseError::Syntax {
        message: message.to_string(),
        position: 0,
    }
}

fn insert_regexp_parser_error(fields: &mut Labels) {
    insert_extracted_field(fields, "__error__", "RegexpParserErr".to_string());
    insert_extracted_field(
        fields,
        "__error_details__",
        "regexp parser failed to match log line".to_string(),
    );
}

fn insert_extracted_field(fields: &mut Labels, name: &str, value: String) {
    if fields.contains_key(name) {
        fields.entry(format!("{name}_extracted")).or_insert(value);
    } else {
        fields.insert(name.to_string(), value);
    }
}

fn decolorize_line(line: &str) -> String {
    Regex::new(r"\x1b\[[0-?]*[ -/]*[@-~]")
        .expect("ANSI CSI regex is valid")
        .replace_all(line, "")
        .into_owned()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LineFormat {
    template: String,
    parts: Vec<TemplatePart>,
}

impl LineFormat {
    pub fn new(template: impl Into<String>) -> Result<Self, ParseError> {
        let template = template.into();
        let parts = parse_template_parts(&template)?;
        Ok(Self { template, parts })
    }

    #[must_use]
    pub fn template(&self) -> &str {
        &self.template
    }

    #[must_use]
    pub fn render(&self, line: &str, fields: &Labels) -> String {
        self.render_with_timestamp(line, fields, None)
    }

    fn render_with_timestamp(
        &self,
        line: &str,
        fields: &Labels,
        timestamp_ns: Option<i64>,
    ) -> String {
        let context = TemplateRenderContext::new(line, fields, timestamp_ns);
        render_template_parts(&self.parts, &context)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TemplatePart {
    Literal(String),
    Comment,
    Expression(TemplateExpression),
    Conditional(TemplateConditional),
    Range(TemplateRange),
    With(TemplateWith),
    Assignment(TemplateAssignment),
}

#[derive(Clone, Debug, PartialEq)]
enum TemplateRuntimeValue {
    String(String),
    Integer(i64),
    Json(serde_json::Value),
}

impl TemplateRuntimeValue {
    fn into_rendered_string(self) -> String {
        match self {
            Self::String(value) => value,
            Self::Integer(value) => value.to_string(),
            Self::Json(value) => template_json_value_to_string(&value),
        }
    }

    fn as_rendered_string(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::Integer(value) => value.to_string(),
            Self::Json(value) => template_json_value_to_string(value),
        }
    }

    fn is_template_string(&self) -> bool {
        matches!(
            self,
            Self::String(_) | Self::Json(serde_json::Value::String(_))
        )
    }
}

#[derive(Clone, Debug)]
struct TemplateRenderContext<'a> {
    line: &'a str,
    fields: &'a Labels,
    timestamp_ns: Option<i64>,
    variables: BTreeMap<String, TemplateRuntimeValue>,
    current_dot: Option<TemplateRuntimeValue>,
}

impl<'a> TemplateRenderContext<'a> {
    fn new(line: &'a str, fields: &'a Labels, timestamp_ns: Option<i64>) -> Self {
        Self {
            line,
            fields,
            timestamp_ns,
            variables: BTreeMap::new(),
            current_dot: None,
        }
    }

    fn with_variable(&self, name: String, value: TemplateRuntimeValue) -> Self {
        let mut variables = self.variables.clone();
        variables.insert(name, value);
        Self {
            line: self.line,
            fields: self.fields,
            timestamp_ns: self.timestamp_ns,
            variables,
            current_dot: self.current_dot.clone(),
        }
    }

    fn with_current_dot(&self, value: TemplateRuntimeValue) -> Self {
        Self {
            line: self.line,
            fields: self.fields,
            timestamp_ns: self.timestamp_ns,
            variables: self.variables.clone(),
            current_dot: Some(value),
        }
    }
}

fn template_json_value_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => String::new(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            serde_json::to_string(value).unwrap_or_default()
        }
    }
}

fn template_variable_path_value(
    value: &TemplateRuntimeValue,
    path: &[String],
) -> Option<TemplateRuntimeValue> {
    if path.is_empty() {
        return Some(value.clone());
    }
    let TemplateRuntimeValue::Json(mut current) = value.clone() else {
        return None;
    };
    for part in path {
        match current {
            serde_json::Value::Object(mut object) => {
                current = object.remove(part)?;
            }
            _ => return None,
        }
    }
    Some(TemplateRuntimeValue::Json(current))
}

fn template_current_dot_field_value(
    value: &TemplateRuntimeValue,
    field: &str,
) -> Option<TemplateRuntimeValue> {
    let path = field
        .split('.')
        .filter(|part| !part.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    template_variable_path_value(value, &path)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TemplateConditional {
    branches: Vec<(TemplateExpression, Vec<TemplatePart>)>,
    else_parts: Vec<TemplatePart>,
}

impl TemplateConditional {
    fn render(&self, context: &TemplateRenderContext<'_>) -> String {
        for (condition, parts) in &self.branches {
            if template_truthy(&condition.render(context)) {
                return render_template_parts(parts, context);
            }
        }
        render_template_parts(&self.else_parts, context)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TemplateAssignment {
    variable: String,
    expression: TemplateExpression,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TemplateRangeBinding {
    Dot,
    Value(String),
    IndexValue { index: String, value: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TemplateRange {
    binding: TemplateRangeBinding,
    expression: TemplateExpression,
    parts: Vec<TemplatePart>,
    else_parts: Vec<TemplatePart>,
}

impl TemplateRange {
    fn render(&self, context: &TemplateRenderContext<'_>) -> String {
        let value = self.expression.evaluate(context);
        match value {
            TemplateRuntimeValue::Json(serde_json::Value::Array(values)) => {
                self.render_array(context, values)
            }
            TemplateRuntimeValue::Json(serde_json::Value::Object(object)) => {
                self.render_object(context, object)
            }
            _ => render_template_parts(&self.else_parts, context),
        }
    }

    fn render_array(
        &self,
        context: &TemplateRenderContext<'_>,
        values: Vec<serde_json::Value>,
    ) -> String {
        if values.is_empty() {
            return render_template_parts(&self.else_parts, context);
        }
        let mut rendered = String::new();
        for (index, value) in values.into_iter().enumerate() {
            let key = TemplateRuntimeValue::Integer(index as i64);
            let value = TemplateRuntimeValue::Json(value);
            rendered.push_str(&self.render_iteration(context, key, value));
        }
        rendered
    }

    fn render_object(
        &self,
        context: &TemplateRenderContext<'_>,
        object: serde_json::Map<String, serde_json::Value>,
    ) -> String {
        if object.is_empty() {
            return render_template_parts(&self.else_parts, context);
        }
        let mut entries = object.into_iter().collect::<Vec<_>>();
        entries.sort_by(|(left, _), (right, _)| left.cmp(right));

        let mut rendered = String::new();
        for (key, value) in entries {
            let key = TemplateRuntimeValue::String(key);
            let value = TemplateRuntimeValue::Json(value);
            rendered.push_str(&self.render_iteration(context, key, value));
        }
        rendered
    }

    fn render_iteration(
        &self,
        context: &TemplateRenderContext<'_>,
        key: TemplateRuntimeValue,
        value: TemplateRuntimeValue,
    ) -> String {
        let child_context = match &self.binding {
            TemplateRangeBinding::Dot => context.with_current_dot(value),
            TemplateRangeBinding::Value(variable) => context.with_variable(variable.clone(), value),
            TemplateRangeBinding::IndexValue {
                index: index_variable,
                value: value_variable,
            } => context
                .with_variable(index_variable.clone(), key)
                .with_variable(value_variable.clone(), value),
        };
        render_template_parts(&self.parts, &child_context)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TemplateWith {
    expression: TemplateExpression,
    parts: Vec<TemplatePart>,
    else_parts: Vec<TemplatePart>,
}

impl TemplateWith {
    fn render(&self, context: &TemplateRenderContext<'_>) -> String {
        let value = self.expression.evaluate(context);
        if !template_truthy(&value.as_rendered_string()) {
            return render_template_parts(&self.else_parts, context);
        }
        let child_context = context.with_current_dot(value);
        render_template_parts(&self.parts, &child_context)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TemplateExpression {
    commands: Vec<TemplateCommand>,
}

impl TemplateExpression {
    fn parse(expression: &str) -> Result<Self, ParseError> {
        let mut commands = Vec::new();
        for command in split_template_pipeline(expression)? {
            commands.push(TemplateCommand::parse(command.trim())?);
        }
        if commands.is_empty() {
            return Err(template_parse_error("expected template action"));
        }
        Ok(Self { commands })
    }

    fn render(&self, context: &TemplateRenderContext<'_>) -> String {
        self.evaluate(context).into_rendered_string()
    }

    fn evaluate(&self, context: &TemplateRenderContext<'_>) -> TemplateRuntimeValue {
        let mut input = None;
        for command in &self.commands {
            input = Some(command.evaluate(context, input));
        }
        input.unwrap_or_else(|| TemplateRuntimeValue::String(String::new()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TemplateCommand {
    Value(TemplateValue),
    Function {
        name: String,
        args: Vec<TemplateValue>,
    },
}

impl TemplateCommand {
    fn parse(command: &str) -> Result<Self, ParseError> {
        let tokens = tokenize_template_command(command)?;
        let Some((head, tail)) = tokens.split_first() else {
            return Err(template_parse_error("expected template command"));
        };
        if tail.is_empty() && !is_template_function_name(head) {
            return Ok(Self::Value(TemplateValue::parse(head)?));
        }
        if !is_template_function_name(head) {
            return Err(template_parse_error("unsupported template action"));
        }
        Ok(Self::Function {
            name: head.to_string(),
            args: tail
                .iter()
                .map(|token| TemplateValue::parse(token))
                .collect::<Result<Vec<_>, _>>()?,
        })
    }

    fn evaluate(
        &self,
        context: &TemplateRenderContext<'_>,
        input: Option<TemplateRuntimeValue>,
    ) -> TemplateRuntimeValue {
        match self {
            Self::Value(value) => value.evaluate(context),
            Self::Function { name, args } => {
                let mut values = args
                    .iter()
                    .map(|arg| arg.evaluate(context))
                    .collect::<Vec<_>>();
                if let Some(input) = input {
                    values.push(input);
                }
                evaluate_template_function(name, &values)
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TemplateValue {
    Current,
    Field(String),
    Variable { name: String, path: Vec<String> },
    Line,
    Timestamp,
    String(String),
    Integer(i64),
    Expression(Box<TemplateExpression>),
    Bare(String),
}

impl TemplateValue {
    fn parse(token: &str) -> Result<Self, ParseError> {
        if token.starts_with('(') && token.ends_with(')') && token.len() >= 2 {
            return Ok(Self::Expression(Box::new(TemplateExpression::parse(
                token[1..token.len() - 1].trim(),
            )?)));
        }
        if token == "." {
            return Ok(Self::Current);
        }
        if let Some(field) = token.strip_prefix('.') {
            if field.is_empty() {
                return Err(template_parse_error("expected template field name"));
            }
            return Ok(Self::Field(field.to_string()));
        }
        if let Some(variable) = token.strip_prefix('$') {
            if variable.is_empty() {
                return Err(template_parse_error("expected template variable name"));
            }
            let mut parts = variable.split('.');
            let Some(name) = parts.next() else {
                return Err(template_parse_error("expected template variable name"));
            };
            if name.is_empty() {
                return Err(template_parse_error("expected template variable name"));
            }
            return Ok(Self::Variable {
                name: name.to_string(),
                path: parts
                    .filter(|part| !part.is_empty())
                    .map(ToString::to_string)
                    .collect(),
            });
        }
        if matches!(token, "__line__" | "line") {
            return Ok(Self::Line);
        }
        if matches!(token, "__timestamp__" | "timestamp") {
            return Ok(Self::Timestamp);
        }
        if let Some(value) = quoted_template_token_value(token)? {
            return Ok(Self::String(value));
        }
        if let Ok(value) = token.parse::<i64>() {
            return Ok(Self::Integer(value));
        }
        Ok(Self::Bare(token.to_string()))
    }

    fn evaluate(&self, context: &TemplateRenderContext<'_>) -> TemplateRuntimeValue {
        match self {
            Self::Current => context
                .current_dot
                .clone()
                .unwrap_or_else(|| TemplateRuntimeValue::String(String::new())),
            Self::Field(name) => context
                .current_dot
                .as_ref()
                .and_then(|value| template_current_dot_field_value(value, name))
                .unwrap_or_else(|| {
                    TemplateRuntimeValue::String(
                        context.fields.get(name).cloned().unwrap_or_default(),
                    )
                }),
            Self::Variable { name, path } => context
                .variables
                .get(name)
                .and_then(|value| template_variable_path_value(value, path))
                .unwrap_or_else(|| TemplateRuntimeValue::String(String::new())),
            Self::Line => TemplateRuntimeValue::String(context.line.to_string()),
            Self::Timestamp => TemplateRuntimeValue::String(
                context
                    .timestamp_ns
                    .map_or_else(String::new, |value| value.to_string()),
            ),
            Self::String(value) | Self::Bare(value) => TemplateRuntimeValue::String(value.clone()),
            Self::Integer(value) => TemplateRuntimeValue::Integer(*value),
            Self::Expression(expression) => expression.evaluate(context),
        }
    }
}

struct ParsedTemplateAction<'a> {
    expression: &'a str,
    next_pos: usize,
    trim_left: bool,
}

fn parse_template_action(
    template: &str,
    open: usize,
) -> Result<ParsedTemplateAction<'_>, ParseError> {
    let mut expression_start = open + 2;
    let trim_left = template_action_trim_left(template, open)?;
    if trim_left {
        expression_start += 1;
    }
    let close_offset = template[expression_start..]
        .find("}}")
        .ok_or_else(|| template_parse_error("expected closing template action"))?;
    let close = expression_start + close_offset;
    let trim_right = template_action_trim_right(template, expression_start, close);
    let expression_end = if trim_right { close - 1 } else { close };
    let mut next_pos = close + 2;
    if trim_right {
        next_pos = skip_leading_template_whitespace(template, next_pos);
    }
    Ok(ParsedTemplateAction {
        expression: template[expression_start..expression_end].trim(),
        next_pos,
        trim_left,
    })
}

fn is_template_comment_action(expression: &str) -> bool {
    expression.starts_with("/*") && expression.ends_with("*/")
}

fn template_action_trim_left(template: &str, open: usize) -> Result<bool, ParseError> {
    let expression_start = open + 2;
    if !template[expression_start..].starts_with('-') {
        return Ok(false);
    }
    let Some(next) = template[expression_start + 1..].chars().next() else {
        return Err(template_parse_error("expected closing template action"));
    };
    Ok(next.is_whitespace())
}

fn template_action_trim_right(template: &str, expression_start: usize, close: usize) -> bool {
    if close <= expression_start || !template[..close].ends_with('-') {
        return false;
    }
    template[expression_start..close - 1]
        .chars()
        .next_back()
        .is_some_and(char::is_whitespace)
}

fn skip_leading_template_whitespace(template: &str, mut pos: usize) -> usize {
    while pos < template.len() {
        let Some(ch) = template[pos..].chars().next() else {
            break;
        };
        if !ch.is_whitespace() {
            break;
        }
        pos += ch.len_utf8();
    }
    pos
}

fn trim_last_template_literal(parts: &mut Vec<TemplatePart>) {
    let Some(TemplatePart::Literal(literal)) = parts.last_mut() else {
        return;
    };
    let trimmed = literal.trim_end_matches(char::is_whitespace).len();
    literal.truncate(trimmed);
    if literal.is_empty() {
        parts.pop();
    }
}

fn trim_template_body_end(template: &str, start: usize, end: usize) -> usize {
    let mut body_end = end;
    for (offset, ch) in template[start..end].char_indices().rev() {
        if !ch.is_whitespace() {
            break;
        }
        body_end = start + offset;
    }
    body_end
}

fn parse_template_parts(template: &str) -> Result<Vec<TemplatePart>, ParseError> {
    let mut parts = Vec::new();
    let mut pos = 0;
    while pos < template.len() {
        let Some(open_offset) = template[pos..].find("{{") else {
            if pos < template.len() {
                parts.push(TemplatePart::Literal(template[pos..].to_string()));
            }
            break;
        };
        let open = pos + open_offset;
        if open > pos {
            let literal = if template_action_trim_left(template, open)? {
                template[pos..open]
                    .trim_end_matches(char::is_whitespace)
                    .to_string()
            } else {
                template[pos..open].to_string()
            };
            if !literal.is_empty() {
                parts.push(TemplatePart::Literal(literal));
            }
        } else if template_action_trim_left(template, open)? {
            trim_last_template_literal(&mut parts);
        }

        let action = parse_template_action(template, open)?;
        let expression = action.expression;
        if let Some(condition) = expression.strip_prefix("if ") {
            let (conditional, next_pos) =
                parse_template_conditional(template, action.next_pos, condition.trim())?;
            parts.push(TemplatePart::Conditional(conditional));
            pos = next_pos;
            continue;
        }
        if let Some(range_expression) = expression.strip_prefix("range ") {
            let (range, next_pos) =
                parse_template_range(template, action.next_pos, range_expression)?;
            parts.push(TemplatePart::Range(range));
            pos = next_pos;
            continue;
        }
        if let Some(with_expression) = expression.strip_prefix("with ") {
            let (with, next_pos) = parse_template_with(template, action.next_pos, with_expression)?;
            parts.push(TemplatePart::With(with));
            pos = next_pos;
            continue;
        }
        if is_template_comment_action(expression) {
            parts.push(TemplatePart::Comment);
            pos = action.next_pos;
            continue;
        }
        if let Some(assignment) = parse_template_assignment(expression)? {
            parts.push(TemplatePart::Assignment(assignment));
            pos = action.next_pos;
            continue;
        }
        if expression == "else"
            || expression.starts_with("else if ")
            || expression == "end"
            || expression.starts_with("range ")
            || expression.starts_with("with ")
        {
            return Err(template_parse_error("unexpected template control action"));
        }
        parts.push(TemplatePart::Expression(TemplateExpression::parse(
            expression,
        )?));
        pos = action.next_pos;
    }
    Ok(parts)
}

fn parse_template_assignment(expression: &str) -> Result<Option<TemplateAssignment>, ParseError> {
    if !expression.trim_start().starts_with('$') {
        return Ok(None);
    }
    let Some((variable, expression)) = expression.split_once(":=") else {
        return Ok(None);
    };
    let variable = parse_template_variable_name(variable.trim(), "expected template variable")?;
    Ok(Some(TemplateAssignment {
        variable,
        expression: TemplateExpression::parse(expression.trim())?,
    }))
}

fn parse_template_conditional(
    template: &str,
    mut branch_start: usize,
    first_condition: &str,
) -> Result<(TemplateConditional, usize), ParseError> {
    let mut branches = Vec::new();
    let mut condition = TemplateExpression::parse(first_condition)?;
    loop {
        let Some((body_end, expression, next_pos)) =
            find_template_control_action(template, branch_start)?
        else {
            return Err(template_parse_error("expected template end action"));
        };
        let branch_parts = parse_template_parts(&template[branch_start..body_end])?;
        if let Some(next_condition) = expression.strip_prefix("else if ") {
            branches.push((condition, branch_parts));
            condition = TemplateExpression::parse(next_condition.trim())?;
            branch_start = next_pos;
            continue;
        }
        if expression == "else" {
            branches.push((condition, branch_parts));
            let Some((else_end_body, end_expression, else_end_next)) =
                find_template_control_action(template, next_pos)?
            else {
                return Err(template_parse_error("expected template end action"));
            };
            if end_expression != "end" {
                return Err(template_parse_error("unexpected template control action"));
            }
            let else_parts = parse_template_parts(&template[next_pos..else_end_body])?;
            return Ok((
                TemplateConditional {
                    branches,
                    else_parts,
                },
                else_end_next,
            ));
        }
        branches.push((condition, branch_parts));
        return Ok((
            TemplateConditional {
                branches,
                else_parts: Vec::new(),
            },
            next_pos,
        ));
    }
}

fn parse_template_range(
    template: &str,
    body_start: usize,
    range_expression: &str,
) -> Result<(TemplateRange, usize), ParseError> {
    let (binding, expression) = parse_template_range_expression(range_expression)?;
    let Some((control_body, control_expression, control_next)) =
        find_template_control_action(template, body_start)?
    else {
        return Err(template_parse_error("expected template end action"));
    };
    let parts = parse_template_parts(&template[body_start..control_body])?;
    if control_expression == "end" {
        return Ok((
            TemplateRange {
                binding,
                expression,
                parts,
                else_parts: Vec::new(),
            },
            control_next,
        ));
    }
    if control_expression != "else" {
        return Err(template_parse_error("unexpected template control action"));
    }

    let Some((end_body, end_expression, end_next)) =
        find_template_control_action(template, control_next)?
    else {
        return Err(template_parse_error("expected template end action"));
    };
    if end_expression != "end" {
        return Err(template_parse_error("unexpected template control action"));
    }
    let else_parts = parse_template_parts(&template[control_next..end_body])?;
    Ok((
        TemplateRange {
            binding,
            expression,
            parts,
            else_parts,
        },
        end_next,
    ))
}

fn parse_template_with(
    template: &str,
    body_start: usize,
    with_expression: &str,
) -> Result<(TemplateWith, usize), ParseError> {
    let expression = TemplateExpression::parse(with_expression.trim())?;
    let Some((control_body, control_expression, control_next)) =
        find_template_control_action(template, body_start)?
    else {
        return Err(template_parse_error("expected template end action"));
    };
    let parts = parse_template_parts(&template[body_start..control_body])?;
    if control_expression == "end" {
        return Ok((
            TemplateWith {
                expression,
                parts,
                else_parts: Vec::new(),
            },
            control_next,
        ));
    }
    if control_expression != "else" {
        return Err(template_parse_error("unexpected template control action"));
    }

    let Some((end_body, end_expression, end_next)) =
        find_template_control_action(template, control_next)?
    else {
        return Err(template_parse_error("expected template end action"));
    };
    if end_expression != "end" {
        return Err(template_parse_error("unexpected template control action"));
    }
    let else_parts = parse_template_parts(&template[control_next..end_body])?;
    Ok((
        TemplateWith {
            expression,
            parts,
            else_parts,
        },
        end_next,
    ))
}

fn parse_template_range_expression(
    range_expression: &str,
) -> Result<(TemplateRangeBinding, TemplateExpression), ParseError> {
    let Some((variables, expression)) = range_expression.split_once(":=") else {
        return Ok((
            TemplateRangeBinding::Dot,
            TemplateExpression::parse(range_expression.trim())?,
        ));
    };
    let variables = variables.split(',').map(str::trim).collect::<Vec<_>>();
    let binding = match variables.as_slice() {
        [variable] => TemplateRangeBinding::Value(parse_template_variable_name(
            variable,
            "expected template range variable",
        )?),
        [index, value] => TemplateRangeBinding::IndexValue {
            index: parse_template_variable_name(index, "expected template range variable")?,
            value: parse_template_variable_name(value, "expected template range variable")?,
        },
        _ => return Err(template_parse_error("expected template range variable")),
    };
    Ok((binding, TemplateExpression::parse(expression.trim())?))
}

fn parse_template_variable_name(
    variable: &str,
    message: &'static str,
) -> Result<String, ParseError> {
    let Some(variable) = variable.strip_prefix('$') else {
        return Err(template_parse_error(message));
    };
    if variable.is_empty() || variable.contains(|ch: char| ch.is_whitespace() || ch == '.') {
        return Err(template_parse_error(message));
    }
    Ok(variable.to_string())
}

fn find_template_control_action(
    template: &str,
    mut pos: usize,
) -> Result<Option<(usize, &str, usize)>, ParseError> {
    let body_start = pos;
    let mut depth = 0usize;
    while pos < template.len() {
        let Some(open_offset) = template[pos..].find("{{") else {
            return Ok(None);
        };
        let open = pos + open_offset;
        let action = parse_template_action(template, open)?;
        let expression = action.expression;
        if is_template_comment_action(expression) {
            pos = action.next_pos;
            continue;
        }
        if expression.starts_with("if ")
            || expression.starts_with("range ")
            || expression.starts_with("with ")
        {
            depth += 1;
        } else if expression == "end" {
            if depth == 0 {
                let body_end = if action.trim_left {
                    trim_template_body_end(template, body_start, open)
                } else {
                    open
                };
                return Ok(Some((body_end, expression, action.next_pos)));
            }
            depth -= 1;
        } else if depth == 0 && (expression == "else" || expression.starts_with("else if ")) {
            let body_end = if action.trim_left {
                trim_template_body_end(template, body_start, open)
            } else {
                open
            };
            return Ok(Some((body_end, expression, action.next_pos)));
        }
        pos = action.next_pos;
    }
    Ok(None)
}

fn render_template_parts(parts: &[TemplatePart], context: &TemplateRenderContext<'_>) -> String {
    let mut context = context.clone();
    let mut rendered = String::new();
    for part in parts {
        match part {
            TemplatePart::Literal(literal) => rendered.push_str(literal),
            TemplatePart::Comment => {}
            TemplatePart::Expression(expression) => {
                rendered.push_str(&expression.render(&context));
            }
            TemplatePart::Conditional(conditional) => {
                rendered.push_str(&conditional.render(&context));
            }
            TemplatePart::Range(range) => {
                rendered.push_str(&range.render(&context));
            }
            TemplatePart::With(with) => {
                rendered.push_str(&with.render(&context));
            }
            TemplatePart::Assignment(assignment) => {
                let value = assignment.expression.evaluate(&context);
                context = context.with_variable(assignment.variable.clone(), value);
            }
        }
    }
    rendered
}

fn template_truthy(value: &str) -> bool {
    !matches!(value, "" | "false" | "0")
}

fn split_template_pipeline(expression: &str) -> Result<Vec<&str>, ParseError> {
    let mut commands = Vec::new();
    let mut start = 0;
    let mut quote = None;
    let mut escaped = false;
    for (index, ch) in expression.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if quote == Some('"') && ch == '\\' {
            escaped = true;
            continue;
        }
        if let Some(quote_ch) = quote {
            if ch == quote_ch {
                quote = None;
            }
            continue;
        }
        if ch == '"' || ch == '`' {
            quote = Some(ch);
        } else if ch == '|' {
            let command = expression[start..index].trim();
            if command.is_empty() {
                return Err(template_parse_error("expected template command"));
            }
            commands.push(command);
            start = index + ch.len_utf8();
        }
    }
    if quote.is_some() {
        return Err(template_parse_error("unterminated template string"));
    }
    let command = expression[start..].trim();
    if !command.is_empty() {
        commands.push(command);
    }
    Ok(commands)
}

fn tokenize_template_command(command: &str) -> Result<Vec<String>, ParseError> {
    let mut tokens = Vec::new();
    let mut pos = 0;
    while pos < command.len() {
        let Some((offset, ch)) = command[pos..]
            .char_indices()
            .find(|(_, ch)| !ch.is_whitespace())
        else {
            break;
        };
        pos += offset;
        if ch == '"' || ch == '`' {
            let (token, next) = parse_template_quoted_token(command, pos, ch)?;
            tokens.push(token);
            pos = next;
        } else if ch == '(' {
            let (token, next) = parse_template_parenthesized_token(command, pos)?;
            tokens.push(token);
            pos = next;
        } else {
            let end = command[pos..]
                .char_indices()
                .find_map(|(offset, ch)| ch.is_whitespace().then_some(pos + offset))
                .unwrap_or(command.len());
            tokens.push(command[pos..end].to_string());
            pos = end;
        }
    }
    Ok(tokens)
}

fn parse_template_parenthesized_token(
    command: &str,
    start: usize,
) -> Result<(String, usize), ParseError> {
    let mut depth = 0usize;
    let mut quote = None;
    let mut escaped = false;
    for (offset, ch) in command[start..].char_indices() {
        let index = start + offset;
        if escaped {
            escaped = false;
            continue;
        }
        if quote == Some('"') && ch == '\\' {
            escaped = true;
            continue;
        }
        if let Some(quote_ch) = quote {
            if ch == quote_ch {
                quote = None;
            }
            continue;
        }
        if ch == '"' || ch == '`' {
            quote = Some(ch);
            continue;
        }
        match ch {
            '(' => depth += 1,
            ')' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| template_parse_error("unexpected template parenthesis"))?;
                if depth == 0 {
                    return Ok((command[start..=index].to_string(), index + ch.len_utf8()));
                }
            }
            _ => {}
        }
    }
    Err(template_parse_error("unterminated template parenthesis"))
}

fn parse_template_quoted_token(
    command: &str,
    start: usize,
    quote: char,
) -> Result<(String, usize), ParseError> {
    let mut escaped = false;
    for (offset, ch) in command[start + quote.len_utf8()..].char_indices() {
        let index = start + quote.len_utf8() + offset;
        if escaped {
            escaped = false;
            continue;
        }
        if quote == '"' && ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == quote {
            return Ok((command[start..=index].to_string(), index + quote.len_utf8()));
        }
    }
    Err(template_parse_error("unterminated template string"))
}

fn quoted_template_token_value(token: &str) -> Result<Option<String>, ParseError> {
    if token.starts_with('`') && token.ends_with('`') && token.len() >= 2 {
        return Ok(Some(token[1..token.len() - 1].to_string()));
    }
    if token.starts_with('"') && token.ends_with('"') && token.len() >= 2 {
        return Ok(Some(decode_quoted_fragment(&token[1..token.len() - 1])?));
    }
    Ok(None)
}

fn decode_quoted_fragment(fragment: &str) -> Result<String, ParseError> {
    let mut decoded = String::new();
    let mut chars = fragment.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            decoded.push(ch);
            continue;
        }
        let Some(escaped) = chars.next() else {
            return Err(template_parse_error("unterminated template escape"));
        };
        match escaped {
            '\\' => decoded.push('\\'),
            '"' => decoded.push('"'),
            'n' => decoded.push('\n'),
            'r' => decoded.push('\r'),
            't' => decoded.push('\t'),
            other => decoded.push(other),
        }
    }
    Ok(decoded)
}

fn is_template_function_name(name: &str) -> bool {
    matches!(
        name,
        "alignLeft"
            | "alignRight"
            | "add"
            | "addf"
            | "and"
            | "b64dec"
            | "b64enc"
            | "lower"
            | "upper"
            | "replace"
            | "default"
            | "contains"
            | "bytes"
            | "date"
            | "eq"
            | "ge"
            | "gt"
            | "hasPrefix"
            | "hasSuffix"
            | "html"
            | "index"
            | "js"
            | "le"
            | "duration"
            | "duration_seconds"
            | "div"
            | "divf"
            | "ceil"
            | "float64"
            | "floor"
            | "fromJson"
            | "indent"
            | "int"
            | "len"
            | "lt"
            | "max"
            | "maxf"
            | "min"
            | "minf"
            | "mod"
            | "mul"
            | "mulf"
            | "ne"
            | "nindent"
            | "now"
            | "not"
            | "or"
            | "print"
            | "printf"
            | "println"
            | "repeat"
            | "count"
            | "regexReplaceAll"
            | "regexReplaceAllLiteral"
            | "substr"
            | "title"
            | "toDate"
            | "toDateInZone"
            | "trim"
            | "trimAll"
            | "trimPrefix"
            | "trimSuffix"
            | "trunc"
            | "sub"
            | "subf"
            | "round"
            | "slice"
            | "unixEpoch"
            | "unixEpochMillis"
            | "unixEpochNanos"
            | "unixToTime"
            | "urlquery"
            | "urlencode"
            | "urldecode"
    )
}

fn evaluate_template_function(name: &str, args: &[TemplateRuntimeValue]) -> TemplateRuntimeValue {
    if name == "fromJson" {
        let Some(value) = args.first() else {
            return TemplateRuntimeValue::String(String::new());
        };
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&value.as_rendered_string())
        else {
            return TemplateRuntimeValue::String(String::new());
        };
        return TemplateRuntimeValue::Json(value);
    }

    if name == "index" {
        return evaluate_template_index(args);
    }
    if name == "slice" {
        return evaluate_template_slice(args);
    }
    if name == "print" {
        return TemplateRuntimeValue::String(format_template_print(args, false));
    }
    if name == "println" {
        return TemplateRuntimeValue::String(format_template_print(args, true));
    }
    if name == "html" {
        return TemplateRuntimeValue::String(html_escape_template_string(&format_template_print(
            args, false,
        )));
    }
    if name == "js" {
        return TemplateRuntimeValue::String(js_escape_template_string(&format_template_print(
            args, false,
        )));
    }

    let args = args
        .iter()
        .map(TemplateRuntimeValue::as_rendered_string)
        .collect::<Vec<_>>();
    let rendered = (|| -> String {
        match name {
            "add" => format_template_integer_sum(&args),
            "addf" => format_template_float_sum(&args),
            "alignLeft" => {
                if args.len() < 2 {
                    return String::new();
                }
                let Ok(width) = args[0].parse::<usize>() else {
                    return String::new();
                };
                align_left_template_string(width, &args[1])
            }
            "alignRight" => {
                if args.len() < 2 {
                    return String::new();
                }
                let Ok(width) = args[0].parse::<usize>() else {
                    return String::new();
                };
                align_right_template_string(width, &args[1])
            }
            "and" => args.iter().all(|value| template_truthy(value)).to_string(),
            "b64enc" => args
                .first()
                .map_or_else(String::new, |value| BASE64_STANDARD.encode(value)),
            "b64dec" => {
                let Some(value) = args.first() else {
                    return String::new();
                };
                let Ok(decoded) = BASE64_STANDARD.decode(value) else {
                    return String::new();
                };
                String::from_utf8(decoded).unwrap_or_default()
            }
            "lower" => args
                .first()
                .map_or_else(String::new, |value| value.to_lowercase()),
            "upper" => args
                .first()
                .map_or_else(String::new, |value| value.to_uppercase()),
            "replace" => {
                if args.len() < 3 {
                    return String::new();
                }
                args[2].replace(&args[0], &args[1])
            }
            "default" => {
                if args.len() < 2 || args[1].is_empty() {
                    return args.first().cloned().unwrap_or_default();
                }
                args[1].clone()
            }
            "contains" => {
                if args.len() < 2 {
                    return "false".to_string();
                }
                args[1].contains(&args[0]).to_string()
            }
            "ceil" => args.first().map_or_else(String::new, |value| {
                format_template_float_unary(value, f64::ceil)
            }),
            "bytes" => {
                let Some(value) = args.first() else {
                    return String::new();
                };
                format_template_bytes(value)
            }
            "date" => format_template_date(&args),
            "duration" | "duration_seconds" => {
                let Some(value) = args.first() else {
                    return String::new();
                };
                format_template_duration_seconds(value)
            }
            "div" => format_template_integer_binary(&args, |left, right| {
                (right != 0).then_some(left / right)
            }),
            "divf" => format_template_float_fold(&args, |left, right| {
                (right != 0.0).then_some(left / right)
            }),
            "eq" => {
                if args.len() < 2 {
                    return "false".to_string();
                }
                (args[1] == args[0]).to_string()
            }
            "ne" => {
                if args.len() < 2 {
                    return "false".to_string();
                }
                (args[1] != args[0]).to_string()
            }
            "lt" => format_template_ordering(&args, |ordering| ordering.is_lt()),
            "le" => format_template_ordering(&args, |ordering| ordering.is_le()),
            "gt" => format_template_ordering(&args, |ordering| ordering.is_gt()),
            "ge" => format_template_ordering(&args, |ordering| ordering.is_ge()),
            "float64" => args
                .first()
                .map_or_else(String::new, |value| parse_template_float(value)),
            "floor" => args.first().map_or_else(String::new, |value| {
                format_template_float_unary(value, f64::floor)
            }),
            "hasPrefix" => {
                if args.len() < 2 {
                    return "false".to_string();
                }
                args[1].starts_with(&args[0]).to_string()
            }
            "hasSuffix" => {
                if args.len() < 2 {
                    return "false".to_string();
                }
                args[1].ends_with(&args[0]).to_string()
            }
            "indent" => {
                if args.len() < 2 {
                    return String::new();
                }
                let Ok(spaces) = args[0].parse::<usize>() else {
                    return String::new();
                };
                indent_template_string(spaces, &args[1])
            }
            "int" => args
                .first()
                .map_or_else(String::new, |value| parse_template_integer(value)),
            "len" => args
                .first()
                .map_or_else(String::new, |value| value.len().to_string()),
            "max" => format_template_integer_min_max(&args, Ord::max),
            "maxf" => format_template_float_min_max(&args, f64::max),
            "min" => format_template_integer_min_max(&args, Ord::min),
            "minf" => format_template_float_min_max(&args, f64::min),
            "mod" => format_template_integer_binary(&args, |left, right| {
                (right != 0).then_some(left % right)
            }),
            "mul" => format_template_integer_product(&args),
            "mulf" => format_template_float_product(&args),
            "nindent" => {
                if args.len() < 2 {
                    return String::new();
                }
                let Ok(spaces) = args[0].parse::<usize>() else {
                    return String::new();
                };
                format!("\n{}", indent_template_string(spaces, &args[1]))
            }
            "now" => current_template_timestamp(),
            "not" => args
                .first()
                .is_none_or(|value| !template_truthy(value))
                .to_string(),
            "or" => args.iter().any(|value| template_truthy(value)).to_string(),
            "printf" => format_template_printf(&args),
            "repeat" => {
                if args.len() < 2 {
                    return String::new();
                }
                let Ok(count) = args[0].parse::<usize>() else {
                    return String::new();
                };
                args[1].repeat(count)
            }
            "count" => {
                if args.len() < 2 {
                    return String::new();
                }
                let Ok(regex) = Regex::new(&args[0]) else {
                    return String::new();
                };
                regex.find_iter(&args[1]).count().to_string()
            }
            "regexReplaceAll" => {
                if args.len() < 3 {
                    return String::new();
                }
                let Ok(regex) = Regex::new(&args[0]) else {
                    return String::new();
                };
                regex.replace_all(&args[1], args[2].as_str()).into_owned()
            }
            "regexReplaceAllLiteral" => {
                if args.len() < 3 {
                    return String::new();
                }
                let Ok(regex) = Regex::new(&args[0]) else {
                    return String::new();
                };
                regex
                    .replace_all(&args[1], NoExpand(args[2].as_str()))
                    .into_owned()
            }
            "round" => format_template_float_round(&args),
            "trunc" => {
                if args.len() < 2 {
                    return String::new();
                }
                let Ok(count) = args[0].parse::<i64>() else {
                    return String::new();
                };
                truncate_template_string(&args[1], count)
            }
            "substr" => {
                if args.len() < 3 {
                    return String::new();
                }
                let (Ok(start), Ok(end)) = (args[0].parse::<i64>(), args[1].parse::<i64>()) else {
                    return String::new();
                };
                substring_template_string(&args[2], start, end)
            }
            "title" => args
                .first()
                .map_or_else(String::new, |value| title_template_string(value)),
            "toDate" => format_template_to_date(&args),
            "toDateInZone" => format_template_to_date_in_zone(&args),
            "trim" => args
                .first()
                .map_or_else(String::new, |value| value.trim().to_string()),
            "trimAll" => {
                if args.len() < 2 {
                    return String::new();
                }
                args[1].trim_matches(|ch| args[0].contains(ch)).to_string()
            }
            "trimPrefix" => {
                if args.len() < 2 {
                    return String::new();
                }
                args[1]
                    .strip_prefix(&args[0])
                    .unwrap_or(&args[1])
                    .to_string()
            }
            "trimSuffix" => {
                if args.len() < 2 {
                    return String::new();
                }
                args[1]
                    .strip_suffix(&args[0])
                    .unwrap_or(&args[1])
                    .to_string()
            }
            "sub" => format_template_integer_binary(&args, |left, right| Some(left - right)),
            "subf" => format_template_float_fold(&args, |left, right| Some(left - right)),
            "unixEpoch" => epoch_template_timestamp(&args, 1_000_000_000),
            "unixEpochMillis" => epoch_template_timestamp(&args, 1_000_000),
            "unixEpochNanos" => epoch_template_timestamp(&args, 1),
            "unixToTime" => args
                .first()
                .map_or_else(String::new, |value| unix_to_template_timestamp(value)),
            "urlquery" => args
                .first()
                .map_or_else(String::new, |value| urlquery_template_string(value)),
            "urlencode" => args
                .first()
                .map_or_else(String::new, |value| urlencode_template_string(value)),
            "urldecode" => args
                .first()
                .map_or_else(String::new, |value| urldecode_template_string(value)),
            _ => String::new(),
        }
    })();
    TemplateRuntimeValue::String(rendered)
}

fn parse_template_integer(value: &str) -> String {
    value
        .parse::<i64>()
        .map_or_else(|_| String::new(), |value| value.to_string())
}

fn template_integer_args(args: &[String]) -> Option<Vec<i64>> {
    args.iter().map(|value| value.parse::<i64>().ok()).collect()
}

fn format_template_integer_sum(args: &[String]) -> String {
    let Some(values) = template_integer_args(args) else {
        return String::new();
    };
    values
        .into_iter()
        .try_fold(0i64, i64::checked_add)
        .map_or_else(String::new, |value| value.to_string())
}

fn format_template_integer_product(args: &[String]) -> String {
    let Some(values) = template_integer_args(args) else {
        return String::new();
    };
    values
        .into_iter()
        .try_fold(1i64, i64::checked_mul)
        .map_or_else(String::new, |value| value.to_string())
}

fn format_template_integer_binary(
    args: &[String],
    op: impl FnOnce(i64, i64) -> Option<i64>,
) -> String {
    if args.len() < 2 {
        return String::new();
    }
    let (Ok(left), Ok(right)) = (args[0].parse::<i64>(), args[1].parse::<i64>()) else {
        return String::new();
    };
    op(left, right).map_or_else(String::new, |value| value.to_string())
}

fn format_template_integer_min_max(args: &[String], op: impl Fn(i64, i64) -> i64) -> String {
    let Some(values) = template_integer_args(args) else {
        return String::new();
    };
    values
        .into_iter()
        .reduce(op)
        .map_or_else(String::new, |value| value.to_string())
}

fn parse_template_float(value: &str) -> String {
    value
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
        .map_or_else(String::new, format_template_float)
}

fn format_template_ordering(args: &[String], predicate: impl FnOnce(Ordering) -> bool) -> String {
    if args.len() < 2 {
        return "false".to_string();
    }
    template_compare_values(&args[0], &args[1])
        .is_some_and(predicate)
        .to_string()
}

fn template_compare_values(left: &str, right: &str) -> Option<Ordering> {
    match (left.parse::<f64>(), right.parse::<f64>()) {
        (Ok(left), Ok(right)) if left.is_finite() && right.is_finite() => left.partial_cmp(&right),
        _ => Some(left.cmp(right)),
    }
}

fn format_template_print(args: &[TemplateRuntimeValue], newline: bool) -> String {
    let mut rendered = String::new();
    let mut previous_was_string = false;
    for (index, arg) in args.iter().enumerate() {
        let current_is_string = arg.is_template_string();
        if index > 0 && (newline || (!previous_was_string && !current_is_string)) {
            rendered.push(' ');
        }
        rendered.push_str(&arg.as_rendered_string());
        previous_was_string = current_is_string;
    }
    if newline {
        rendered.push('\n');
    }
    rendered
}

fn evaluate_template_index(args: &[TemplateRuntimeValue]) -> TemplateRuntimeValue {
    let Some((value, indexes)) = template_collection_first_args(args) else {
        return TemplateRuntimeValue::String(String::new());
    };
    let mut current = value.clone();
    for index in indexes {
        let Some(indexed) = template_index_value(&current, &index.as_rendered_string()) else {
            return TemplateRuntimeValue::String(String::new());
        };
        current = indexed;
    }
    current
}

fn evaluate_template_slice(args: &[TemplateRuntimeValue]) -> TemplateRuntimeValue {
    let Some((value, bounds)) = template_collection_first_args(args) else {
        return TemplateRuntimeValue::String(String::new());
    };
    match value {
        TemplateRuntimeValue::String(value) => template_slice_string(value, bounds),
        TemplateRuntimeValue::Json(serde_json::Value::String(value)) => {
            template_slice_string(value, bounds)
        }
        TemplateRuntimeValue::Json(serde_json::Value::Array(values)) => {
            template_slice_array(values, bounds)
        }
        _ => TemplateRuntimeValue::String(String::new()),
    }
}

fn template_collection_first_args(
    args: &[TemplateRuntimeValue],
) -> Option<(&TemplateRuntimeValue, &[TemplateRuntimeValue])> {
    let (first, rest) = args.split_first()?;
    if template_value_is_collection(first) {
        return Some((first, rest));
    }
    let (last, rest) = args.split_last()?;
    template_value_is_collection(last).then_some((last, rest))
}

fn template_value_is_collection(value: &TemplateRuntimeValue) -> bool {
    matches!(
        value,
        TemplateRuntimeValue::String(_)
            | TemplateRuntimeValue::Json(serde_json::Value::String(_))
            | TemplateRuntimeValue::Json(serde_json::Value::Array(_))
            | TemplateRuntimeValue::Json(serde_json::Value::Object(_))
    )
}

fn template_index_value(value: &TemplateRuntimeValue, index: &str) -> Option<TemplateRuntimeValue> {
    match value {
        TemplateRuntimeValue::Json(serde_json::Value::Object(object)) => {
            object.get(index).cloned().map(TemplateRuntimeValue::Json)
        }
        TemplateRuntimeValue::Json(serde_json::Value::Array(values)) => index
            .parse::<usize>()
            .ok()
            .and_then(|index| values.get(index).cloned())
            .map(TemplateRuntimeValue::Json),
        TemplateRuntimeValue::String(value) => index
            .parse::<usize>()
            .ok()
            .and_then(|index| value.as_bytes().get(index).copied())
            .map(|byte| TemplateRuntimeValue::Integer(i64::from(byte))),
        TemplateRuntimeValue::Json(serde_json::Value::String(value)) => index
            .parse::<usize>()
            .ok()
            .and_then(|index| value.as_bytes().get(index).copied())
            .map(|byte| TemplateRuntimeValue::Integer(i64::from(byte))),
        _ => None,
    }
}

fn template_slice_string(value: &str, bounds: &[TemplateRuntimeValue]) -> TemplateRuntimeValue {
    let Some((start, end)) = template_slice_bounds(value.len(), bounds) else {
        return TemplateRuntimeValue::String(String::new());
    };
    TemplateRuntimeValue::String(
        value
            .get(start..end)
            .map_or_else(String::new, ToString::to_string),
    )
}

fn template_slice_array(
    values: &[serde_json::Value],
    bounds: &[TemplateRuntimeValue],
) -> TemplateRuntimeValue {
    let Some((start, end)) = template_slice_bounds(values.len(), bounds) else {
        return TemplateRuntimeValue::String(String::new());
    };
    TemplateRuntimeValue::Json(serde_json::Value::Array(values[start..end].to_vec()))
}

fn template_slice_bounds(len: usize, bounds: &[TemplateRuntimeValue]) -> Option<(usize, usize)> {
    if bounds.len() > 3 {
        return None;
    }
    let start = bounds.first().map_or(Some(0), parse_template_bound)?;
    let end = bounds.get(1).map_or(Some(len), parse_template_bound)?;
    if let Some(capacity) = bounds.get(2) {
        let capacity = parse_template_bound(capacity)?;
        if end > capacity || capacity > len {
            return None;
        }
    }
    (start <= end && end <= len).then_some((start, end))
}

fn parse_template_bound(value: &TemplateRuntimeValue) -> Option<usize> {
    value.as_rendered_string().parse::<usize>().ok()
}

fn template_float_args(args: &[String]) -> Option<Vec<f64>> {
    args.iter()
        .map(|value| value.parse::<f64>().ok().filter(|value| value.is_finite()))
        .collect()
}

fn format_template_float(value: f64) -> String {
    if value == -0.0 {
        "0".to_string()
    } else {
        value.to_string()
    }
}

fn format_template_float_sum(args: &[String]) -> String {
    let Some(values) = template_float_args(args) else {
        return String::new();
    };
    format_template_float(values.into_iter().sum())
}

fn format_template_float_product(args: &[String]) -> String {
    let Some(values) = template_float_args(args) else {
        return String::new();
    };
    format_template_float(values.into_iter().product())
}

fn format_template_float_fold(args: &[String], op: impl Fn(f64, f64) -> Option<f64>) -> String {
    let Some(values) = template_float_args(args) else {
        return String::new();
    };
    let mut values = values.into_iter();
    let Some(first) = values.next() else {
        return String::new();
    };
    values
        .try_fold(first, op)
        .filter(|value| value.is_finite())
        .map_or_else(String::new, format_template_float)
}

fn format_template_float_min_max(args: &[String], op: impl Fn(f64, f64) -> f64) -> String {
    let Some(values) = template_float_args(args) else {
        return String::new();
    };
    values
        .into_iter()
        .reduce(op)
        .map_or_else(String::new, format_template_float)
}

fn format_template_float_unary(value: &str, op: impl FnOnce(f64) -> f64) -> String {
    value
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
        .map(op)
        .filter(|value| value.is_finite())
        .map_or_else(String::new, format_template_float)
}

fn format_template_float_round(args: &[String]) -> String {
    if args.len() < 2 {
        return String::new();
    }
    let (Ok(value), Ok(precision)) = (args[0].parse::<f64>(), args[1].parse::<i32>()) else {
        return String::new();
    };
    if !value.is_finite() {
        return String::new();
    }
    let round_on = args
        .get(2)
        .map_or(Some(0.5), |value| value.parse::<f64>().ok());
    let Some(round_on) = round_on.filter(|value| value.is_finite()) else {
        return String::new();
    };
    let factor = 10f64.powi(precision);
    if !factor.is_finite() || factor == 0.0 {
        return String::new();
    }
    let shifted = value * factor;
    if !shifted.is_finite() {
        return String::new();
    }
    let rounded = if shifted.is_sign_negative() {
        (shifted - round_on).ceil()
    } else {
        (shifted + round_on).floor()
    } / factor;
    if rounded.is_finite() {
        format_template_float(rounded)
    } else {
        String::new()
    }
}

fn format_template_bytes(value: &str) -> String {
    let Some(bytes) = parse_bytes_literal(value) else {
        return String::new();
    };
    if bytes.fract() == 0.0 && bytes <= u64::MAX as f64 {
        (bytes as u64).to_string()
    } else {
        bytes.to_string()
    }
}

fn format_template_duration_seconds(value: &str) -> String {
    let Some(duration_ns) = parse_prometheus_duration_literal(value) else {
        return String::new();
    };
    let Ok(duration_ns) = u128::try_from(duration_ns) else {
        return String::new();
    };
    format_decimal_ratio(duration_ns, 1_000_000_000)
}

fn current_template_timestamp() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_nanos().to_string())
        .unwrap_or_default()
}

fn format_template_date(args: &[String]) -> String {
    if args.len() < 2 {
        return String::new();
    }
    let Ok(timestamp_ns) = args[1].parse::<i128>() else {
        return String::new();
    };
    let Ok(timestamp) = OffsetDateTime::from_unix_timestamp_nanos(timestamp_ns) else {
        return String::new();
    };
    format_go_time_layout(&args[0], timestamp)
}

fn format_template_to_date(args: &[String]) -> String {
    if args.len() < 2 {
        return String::new();
    }
    parse_go_time_layout_to_unix_nanos(&args[0], "Local", &args[1])
}

fn format_template_to_date_in_zone(args: &[String]) -> String {
    if args.len() < 3 {
        return String::new();
    }
    parse_go_time_layout_to_unix_nanos(&args[0], &args[1], &args[2])
}

fn format_template_printf(args: &[String]) -> String {
    let Some(format) = args.first() else {
        return String::new();
    };

    let mut formatted = String::new();
    let mut chars = format.chars().peekable();
    let mut values = args.iter().skip(1);
    while let Some(ch) = chars.next() {
        if ch != '%' {
            formatted.push(ch);
            continue;
        }
        if chars.peek() == Some(&'%') {
            chars.next();
            formatted.push('%');
            continue;
        }

        let left_align = if chars.peek() == Some(&'-') {
            chars.next();
            true
        } else {
            false
        };
        let width = consume_template_printf_number(&mut chars);
        let precision = if chars.peek() == Some(&'.') {
            chars.next();
            Some(consume_template_printf_number(&mut chars).unwrap_or(0))
        } else {
            None
        };

        let Some(verb) = chars.next() else {
            break;
        };
        if verb != 's' {
            formatted.push('%');
            if left_align {
                formatted.push('-');
            }
            if let Some(width) = width {
                formatted.push_str(&width.to_string());
            }
            if let Some(precision) = precision {
                formatted.push('.');
                formatted.push_str(&precision.to_string());
            }
            formatted.push(verb);
            continue;
        }

        let value = values.next().map(String::as_str).unwrap_or_default();
        formatted.push_str(&format_template_printf_string(
            value, width, precision, left_align,
        ));
    }
    formatted
}

fn consume_template_printf_number<I>(chars: &mut std::iter::Peekable<I>) -> Option<usize>
where
    I: Iterator<Item = char>,
{
    let mut value = 0usize;
    let mut consumed = false;
    while let Some(ch) = chars.peek().copied() {
        let Some(digit) = ch.to_digit(10) else {
            break;
        };
        chars.next();
        value = value
            .saturating_mul(10)
            .saturating_add(usize::try_from(digit).unwrap_or(0));
        consumed = true;
    }
    consumed.then_some(value)
}

fn format_template_printf_string(
    value: &str,
    width: Option<usize>,
    precision: Option<usize>,
    left_align: bool,
) -> String {
    let mut rendered = precision.map_or_else(
        || value.to_string(),
        |precision| value.chars().take(precision).collect(),
    );
    let Some(width) = width else {
        return rendered;
    };

    let len = rendered.chars().count();
    if len >= width {
        return rendered;
    }
    let padding = " ".repeat(width - len);
    if left_align {
        rendered.push_str(&padding);
        rendered
    } else {
        format!("{padding}{rendered}")
    }
}

fn format_go_time_layout(layout: &str, timestamp: OffsetDateTime) -> String {
    let mut formatted = String::new();
    let mut index = 0;
    while index < layout.len() {
        let rest = &layout[index..];
        if rest.starts_with("2006") {
            formatted.push_str(&format!("{:04}", timestamp.year()));
            index += 4;
        } else if rest.starts_with("06") {
            formatted.push_str(&format!("{:02}", timestamp.year().rem_euclid(100)));
            index += 2;
        } else if rest.starts_with("15") {
            formatted.push_str(&format!("{:02}", timestamp.hour()));
            index += 2;
        } else if rest.starts_with("04") {
            formatted.push_str(&format!("{:02}", timestamp.minute()));
            index += 2;
        } else if rest.starts_with("05") {
            formatted.push_str(&format!("{:02}", timestamp.second()));
            index += 2;
        } else if rest.starts_with("01") {
            formatted.push_str(&format!("{:02}", u8::from(timestamp.month())));
            index += 2;
        } else if rest.starts_with('1') {
            formatted.push_str(&u8::from(timestamp.month()).to_string());
            index += 1;
        } else if rest.starts_with("02") {
            formatted.push_str(&format!("{:02}", timestamp.day()));
            index += 2;
        } else if rest.starts_with('2') {
            formatted.push_str(&timestamp.day().to_string());
            index += 1;
        } else if rest.starts_with("Z07:00") {
            formatted.push('Z');
            index += 6;
        } else if rest.starts_with("-07:00") {
            formatted.push_str("+00:00");
            index += 6;
        } else if rest.starts_with('.') {
            let digits = rest[1..]
                .chars()
                .take_while(|ch| *ch == '0' || *ch == '9')
                .count();
            if digits == 0 {
                formatted.push('.');
                index += 1;
                continue;
            }
            let fraction = format!("{:09}", timestamp.nanosecond());
            formatted.push('.');
            formatted.push_str(&fraction[..digits.min(fraction.len())]);
            index += 1 + digits;
        } else {
            let Some(ch) = rest.chars().next() else {
                break;
            };
            formatted.push(ch);
            index += ch.len_utf8();
        }
    }
    formatted
}

#[derive(Clone, Copy, Debug)]
struct ParsedTemplateDate {
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    nanosecond: u32,
    offset_seconds: Option<i32>,
}

fn parse_go_time_layout_to_unix_nanos(layout: &str, zone: &str, value: &str) -> String {
    let Some(parsed) = parse_go_time_layout_value(layout, value) else {
        return String::new();
    };
    let Some(date) = NaiveDate::from_ymd_opt(parsed.year, parsed.month, parsed.day) else {
        return String::new();
    };
    let Some(time) =
        NaiveTime::from_hms_nano_opt(parsed.hour, parsed.minute, parsed.second, parsed.nanosecond)
    else {
        return String::new();
    };
    let datetime = NaiveDateTime::new(date, time);
    let Some(utc_datetime) = resolve_template_datetime(datetime, zone, parsed.offset_seconds)
    else {
        return String::new();
    };
    utc_datetime
        .timestamp_nanos_opt()
        .map_or_else(String::new, |value| value.to_string())
}

fn resolve_template_datetime(
    datetime: NaiveDateTime,
    zone: &str,
    offset_seconds: Option<i32>,
) -> Option<chrono::DateTime<Utc>> {
    if let Some(offset_seconds) = offset_seconds {
        let offset = FixedOffset::east_opt(offset_seconds)?;
        return offset
            .from_local_datetime(&datetime)
            .single()
            .map(|datetime| datetime.with_timezone(&Utc));
    }
    if zone == "UTC" || zone == "Local" {
        return Some(Utc.from_utc_datetime(&datetime));
    }
    let zone = zone.parse::<Tz>().ok()?;
    match zone.from_local_datetime(&datetime) {
        LocalResult::Single(datetime) => Some(datetime.with_timezone(&Utc)),
        LocalResult::Ambiguous(earliest, _) => Some(earliest.with_timezone(&Utc)),
        LocalResult::None => None,
    }
}

fn parse_go_time_layout_value(layout: &str, value: &str) -> Option<ParsedTemplateDate> {
    let mut parsed = ParsedTemplateDate {
        year: 0,
        month: 1,
        day: 1,
        hour: 0,
        minute: 0,
        second: 0,
        nanosecond: 0,
        offset_seconds: None,
    };
    let mut layout_pos = 0usize;
    let mut value_pos = 0usize;
    while layout_pos < layout.len() {
        let rest = &layout[layout_pos..];
        if rest.starts_with("2006") {
            parsed.year = parse_fixed_template_digits(value, &mut value_pos, 4)? as i32;
            layout_pos += 4;
        } else if rest.starts_with("06") {
            parsed.year = 2000 + parse_fixed_template_digits(value, &mut value_pos, 2)? as i32;
            layout_pos += 2;
        } else if rest.starts_with("15") {
            parsed.hour = parse_fixed_template_digits(value, &mut value_pos, 2)?;
            layout_pos += 2;
        } else if rest.starts_with("04") {
            parsed.minute = parse_fixed_template_digits(value, &mut value_pos, 2)?;
            layout_pos += 2;
        } else if rest.starts_with("05") {
            parsed.second = parse_fixed_template_digits(value, &mut value_pos, 2)?;
            layout_pos += 2;
        } else if rest.starts_with("01") {
            parsed.month = parse_fixed_template_digits(value, &mut value_pos, 2)?;
            layout_pos += 2;
        } else if rest.starts_with('1') {
            parsed.month = parse_variable_template_digits(value, &mut value_pos, 2)?;
            layout_pos += 1;
        } else if rest.starts_with("02") {
            parsed.day = parse_fixed_template_digits(value, &mut value_pos, 2)?;
            layout_pos += 2;
        } else if rest.starts_with('2') {
            parsed.day = parse_variable_template_digits(value, &mut value_pos, 2)?;
            layout_pos += 1;
        } else if rest.starts_with("Z07:00") {
            parsed.offset_seconds = Some(parse_template_timezone_offset(value, &mut value_pos)?);
            layout_pos += 6;
        } else if rest.starts_with("-07:00") {
            parsed.offset_seconds = Some(parse_template_timezone_offset(value, &mut value_pos)?);
            layout_pos += 6;
        } else if rest.starts_with('.') {
            let digits = rest[1..]
                .chars()
                .take_while(|ch| *ch == '0' || *ch == '9')
                .count();
            if digits == 0 {
                match_template_literal(value, &mut value_pos, '.')?;
                layout_pos += 1;
            } else {
                parsed.nanosecond =
                    parse_template_fractional_nanoseconds(value, &mut value_pos, digits)?;
                layout_pos += 1 + digits;
            }
        } else {
            let ch = rest.chars().next()?;
            match_template_literal(value, &mut value_pos, ch)?;
            layout_pos += ch.len_utf8();
        }
    }
    (value_pos == value.len()).then_some(parsed)
}

fn parse_fixed_template_digits(value: &str, pos: &mut usize, count: usize) -> Option<u32> {
    if *pos + count > value.len() {
        return None;
    }
    let digits = &value[*pos..*pos + count];
    digits
        .bytes()
        .all(|byte| byte.is_ascii_digit())
        .then_some(())?;
    *pos += count;
    digits.parse::<u32>().ok()
}

fn parse_variable_template_digits(value: &str, pos: &mut usize, max_count: usize) -> Option<u32> {
    let start = *pos;
    let mut count = 0usize;
    while *pos < value.len()
        && count < max_count
        && value.as_bytes().get(*pos).is_some_and(u8::is_ascii_digit)
    {
        *pos += 1;
        count += 1;
    }
    (count > 0).then_some(())?;
    value[start..*pos].parse::<u32>().ok()
}

fn parse_template_fractional_nanoseconds(
    value: &str,
    pos: &mut usize,
    max_digits: usize,
) -> Option<u32> {
    match_template_literal(value, pos, '.')?;
    let start = *pos;
    let mut count = 0usize;
    while *pos < value.len()
        && count < max_digits
        && value.as_bytes().get(*pos).is_some_and(u8::is_ascii_digit)
    {
        *pos += 1;
        count += 1;
    }
    (count > 0).then_some(())?;
    let mut fraction = value[start..*pos].parse::<u32>().ok()?;
    for _ in count..9 {
        fraction = fraction.checked_mul(10)?;
    }
    Some(fraction)
}

fn parse_template_timezone_offset(value: &str, pos: &mut usize) -> Option<i32> {
    if value[*pos..].starts_with('Z') {
        *pos += 1;
        return Some(0);
    }
    let sign = if value[*pos..].starts_with('+') {
        *pos += 1;
        1
    } else if value[*pos..].starts_with('-') {
        *pos += 1;
        -1
    } else {
        return None;
    };
    let hours = i32::try_from(parse_fixed_template_digits(value, pos, 2)?).ok()?;
    match_template_literal(value, pos, ':')?;
    let minutes = i32::try_from(parse_fixed_template_digits(value, pos, 2)?).ok()?;
    Some(sign * (hours * 3600 + minutes * 60))
}

fn match_template_literal(value: &str, pos: &mut usize, expected: char) -> Option<()> {
    let ch = value[*pos..].chars().next()?;
    if ch != expected {
        return None;
    }
    *pos += ch.len_utf8();
    Some(())
}

fn unix_to_template_timestamp(epoch: &str) -> String {
    let Ok(value) = epoch.parse::<i128>() else {
        return String::new();
    };
    let nanos = match epoch.len() {
        5 => value.checked_mul(86_400_000_000_000),
        10 => value.checked_mul(1_000_000_000),
        13 => value.checked_mul(1_000_000),
        16 => value.checked_mul(1_000),
        19 => Some(value),
        _ => None,
    };
    nanos.map_or_else(String::new, |value| value.to_string())
}

fn epoch_template_timestamp(args: &[String], divisor: i64) -> String {
    let Some(timestamp) = args.first() else {
        return String::new();
    };
    let Ok(timestamp_ns) = timestamp.parse::<i64>() else {
        return String::new();
    };
    timestamp_ns.div_euclid(divisor).to_string()
}

fn align_left_template_string(width: usize, value: &str) -> String {
    let mut chars = value.chars().take(width).collect::<String>();
    let padding = width.saturating_sub(chars.chars().count());
    chars.extend(std::iter::repeat_n(' ', padding));
    chars
}

fn align_right_template_string(width: usize, value: &str) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    if chars.len() >= width {
        return chars[chars.len() - width..].iter().collect();
    }
    let mut aligned = " ".repeat(width - chars.len());
    aligned.extend(chars);
    aligned
}

fn indent_template_string(spaces: usize, value: &str) -> String {
    let prefix = " ".repeat(spaces);
    value
        .split('\n')
        .map(|line| format!("{prefix}{line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn truncate_template_string(value: &str, count: i64) -> String {
    if count >= 0 {
        return value.chars().take(count as usize).collect();
    }
    let count = count.unsigned_abs() as usize;
    let len = value.chars().count();
    value.chars().skip(len.saturating_sub(count)).collect()
}

fn substring_template_string(value: &str, start: i64, end: i64) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    let len = chars.len();
    let start = usize::try_from(start.max(0)).unwrap_or(usize::MAX).min(len);
    let end = if end < 0 {
        len
    } else {
        usize::try_from(end).unwrap_or(usize::MAX).min(len)
    };
    if end <= start {
        return String::new();
    }
    chars[start..end].iter().collect()
}

fn title_template_string(value: &str) -> String {
    let mut titled = String::new();
    let mut capitalize_next = true;
    for ch in value.chars() {
        if ch.is_alphanumeric() {
            if capitalize_next {
                for upper in ch.to_uppercase() {
                    titled.push(upper);
                }
            } else {
                for lower in ch.to_lowercase() {
                    titled.push(lower);
                }
            }
            capitalize_next = false;
        } else {
            titled.push(ch);
            capitalize_next = true;
        }
    }
    titled
}

fn html_escape_template_string(value: &str) -> String {
    let mut escaped = String::new();
    for ch in value.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&#34;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn js_escape_template_string(value: &str) -> String {
    let mut escaped = String::new();
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '\'' => escaped.push_str("\\'"),
            '"' => escaped.push_str("\\\""),
            '<' => escaped.push_str("\\u003C"),
            '>' => escaped.push_str("\\u003E"),
            '&' => escaped.push_str("\\u0026"),
            '=' => escaped.push_str("\\u003D"),
            '\n' => escaped.push_str("\\u000A"),
            '\r' => escaped.push_str("\\u000D"),
            '\t' => escaped.push_str("\\u0009"),
            '\u{2028}' => escaped.push_str("\\u2028"),
            '\u{2029}' => escaped.push_str("\\u2029"),
            ch if ch.is_control() => push_template_unicode_escape(&mut escaped, u32::from(ch)),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn urlencode_template_string(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(char::from(byte));
            }
            _ => {
                encoded.push('%');
                encoded.push(hex_digit(byte >> 4));
                encoded.push(hex_digit(byte & 0x0f));
            }
        }
    }
    encoded
}

fn urlquery_template_string(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(char::from(byte));
            }
            b' ' => encoded.push('+'),
            _ => {
                encoded.push('%');
                encoded.push(hex_digit(byte >> 4));
                encoded.push(hex_digit(byte & 0x0f));
            }
        }
    }
    encoded
}

fn push_template_unicode_escape(output: &mut String, value: u32) {
    output.push_str("\\u");
    for shift in [12, 8, 4, 0] {
        output.push(hex_digit(((value >> shift) & 0x0f) as u8));
    }
}

fn urldecode_template_string(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && index + 2 < bytes.len()
            && let (Some(high), Some(low)) =
                (hex_value(bytes[index + 1]), hex_value(bytes[index + 2]))
        {
            decoded.push((high << 4) | low);
            index += 3;
            continue;
        }
        decoded.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

fn hex_digit(value: u8) -> char {
    match value {
        0..=9 => char::from(b'0' + value),
        10..=15 => char::from(b'A' + value - 10),
        _ => '0',
    }
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn template_parse_error(message: &str) -> ParseError {
    ParseError::Syntax {
        message: message.to_string(),
        position: 0,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LabelFormat {
    assignments: Vec<LabelFormatAssignment>,
}

impl LabelFormat {
    pub fn new(assignments: Vec<LabelFormatAssignment>) -> Result<Self, ParseError> {
        let mut destinations = BTreeSet::new();
        for assignment in &assignments {
            if !destinations.insert(assignment.destination.clone()) {
                return Err(template_parse_error(
                    "label_format destination appears more than once",
                ));
            }
        }
        Ok(Self { assignments })
    }

    #[must_use]
    pub fn assignments(&self) -> &[LabelFormatAssignment] {
        &self.assignments
    }

    fn apply_with_timestamp(&self, line: &str, fields: &mut Labels, timestamp_ns: Option<i64>) {
        for assignment in &self.assignments {
            assignment.apply_with_timestamp(line, fields, timestamp_ns);
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LabelFormatAssignment {
    destination: String,
    value: LabelFormatValue,
}

impl LabelFormatAssignment {
    pub fn rename(
        destination: impl Into<String>,
        source: impl Into<String>,
    ) -> Result<Self, ParseError> {
        Ok(Self {
            destination: destination.into(),
            value: LabelFormatValue::Rename(source.into()),
        })
    }

    pub fn template(
        destination: impl Into<String>,
        template: impl Into<String>,
    ) -> Result<Self, ParseError> {
        Ok(Self {
            destination: destination.into(),
            value: LabelFormatValue::Template(LineFormat::new(template)?),
        })
    }

    #[must_use]
    pub fn destination(&self) -> &str {
        &self.destination
    }

    #[must_use]
    pub fn value(&self) -> &LabelFormatValue {
        &self.value
    }

    fn apply_with_timestamp(&self, line: &str, fields: &mut Labels, timestamp_ns: Option<i64>) {
        match &self.value {
            LabelFormatValue::Rename(source) => {
                if let Some(value) = fields.remove(source) {
                    fields.insert(self.destination.clone(), value);
                } else {
                    fields.remove(&self.destination);
                }
            }
            LabelFormatValue::Template(template) => {
                fields.insert(
                    self.destination.clone(),
                    template.render_with_timestamp(line, fields, timestamp_ns),
                );
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LabelFormatValue {
    Rename(String),
    Template(LineFormat),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnwrapExpression {
    label: String,
    conversion: UnwrapConversion,
}

impl UnwrapExpression {
    pub fn new(label: impl Into<String>) -> Result<Self, ParseError> {
        let expression = Self {
            label: label.into(),
            conversion: UnwrapConversion::Raw,
        };
        expression.validate()?;
        Ok(expression)
    }

    pub fn bytes(label: impl Into<String>) -> Result<Self, ParseError> {
        let expression = Self {
            label: label.into(),
            conversion: UnwrapConversion::Bytes,
        };
        expression.validate()?;
        Ok(expression)
    }

    pub fn duration(label: impl Into<String>) -> Result<Self, ParseError> {
        let expression = Self {
            label: label.into(),
            conversion: UnwrapConversion::Duration,
        };
        expression.validate()?;
        Ok(expression)
    }

    #[must_use]
    pub fn label(&self) -> &str {
        &self.label
    }

    #[must_use]
    pub fn conversion(&self) -> UnwrapConversion {
        self.conversion
    }

    fn apply(&self, fields: &mut Labels) {
        fields.remove(UNWRAP_SAMPLE_VALUE_LABEL);
        let Some(value) = fields.get(&self.label) else {
            fields.insert("__error__".to_string(), "SampleExtractionErr".to_string());
            fields.insert(
                "__error_details__".to_string(),
                format!("unwrap label `{}` is missing", self.label),
            );
            return;
        };
        match self.convert_sample_value(value) {
            Some(value) => {
                fields.insert(UNWRAP_SAMPLE_VALUE_LABEL.to_string(), value.to_string());
            }
            None => {
                fields.insert("__error__".to_string(), "SampleExtractionErr".to_string());
                fields.insert(
                    "__error_details__".to_string(),
                    format!("unwrap label `{}` cannot be converted", self.label),
                );
            }
        }
    }

    fn convert_sample_value(&self, value: &str) -> Option<String> {
        match self.conversion {
            UnwrapConversion::Raw => parse_raw_sample_literal(value),
            UnwrapConversion::Bytes => {
                let bytes = parse_bytes_literal(value)?;
                if bytes.fract() == 0.0 && bytes <= u64::MAX as f64 {
                    Some((bytes as u64).to_string())
                } else {
                    None
                }
            }
            UnwrapConversion::Duration => {
                let duration_ns = parse_prometheus_duration_literal(value)?;
                Some(format_decimal_ratio(
                    u128::try_from(duration_ns).ok()?,
                    1_000_000_000,
                ))
            }
        }
    }

    fn validate(&self) -> Result<(), ParseError> {
        if self.label.is_empty() {
            return Err(template_parse_error("expected unwrap label name"));
        }
        Ok(())
    }
}

fn parse_raw_sample_literal(value: &str) -> Option<String> {
    let (numerator, denominator) = parse_decimal_sample_literal(value)?;
    let negative = numerator < 0;
    let formatted = format_decimal_ratio(numerator.unsigned_abs(), denominator);
    Some(if negative {
        format!("-{formatted}")
    } else {
        formatted
    })
}

fn parse_decimal_sample_literal(value: &str) -> Option<(i128, u128)> {
    if value.is_empty() {
        return None;
    }
    let (negative, value) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    if value.is_empty() {
        return None;
    }

    let (mantissa, exponent) = match value.find(|ch| matches!(ch, 'e' | 'E')) {
        Some(index) => {
            let exponent_text = &value[index + 1..];
            if exponent_text.find(|ch| matches!(ch, 'e' | 'E')).is_some() {
                return None;
            }
            (&value[..index], parse_decimal_exponent(exponent_text)?)
        }
        None => (value, 0),
    };
    if mantissa.is_empty() {
        return None;
    }

    let (whole, fractional) = match mantissa.split_once('.') {
        Some((whole, fractional)) => (whole, fractional),
        None => (mantissa, ""),
    };
    if whole.is_empty() && fractional.is_empty() {
        return None;
    }
    if !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fractional.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }

    let mut digits = String::with_capacity(whole.len() + fractional.len());
    digits.push_str(whole);
    digits.push_str(fractional);
    if digits.is_empty() {
        return None;
    }
    let mut numerator = digits.parse::<u128>().ok()?;

    let decimal_places = i64::try_from(fractional.len())
        .ok()?
        .checked_sub(i64::from(exponent))?;
    let denominator = if decimal_places >= 0 {
        10_u128.checked_pow(u32::try_from(decimal_places).ok()?)?
    } else {
        numerator =
            numerator.checked_mul(10_u128.checked_pow(u32::try_from(-decimal_places).ok()?)?)?;
        1
    };
    let numerator = i128::try_from(numerator).ok()?;
    Some((if negative { -numerator } else { numerator }, denominator))
}

fn parse_decimal_exponent(value: &str) -> Option<i32> {
    if value.is_empty() {
        return None;
    }
    let value = value.strip_prefix('+').unwrap_or(value);
    if value.is_empty() {
        return None;
    }
    value.parse::<i32>().ok()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnwrapConversion {
    Raw,
    Bytes,
    Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LabelSelectionSet {
    selections: Vec<LabelSelection>,
}

impl LabelSelectionSet {
    pub fn new(selections: Vec<LabelSelection>) -> Result<Self, ParseError> {
        if selections.is_empty() {
            return Err(template_parse_error("expected label selection"));
        }
        Ok(Self { selections })
    }

    #[must_use]
    pub fn selections(&self) -> &[LabelSelection] {
        &self.selections
    }

    fn apply_drop(&self, fields: &mut Labels) {
        for selection in &self.selections {
            if selection.matches(fields) {
                fields.remove(selection.name_str());
            }
        }
    }

    fn apply_keep(&self, fields: &mut Labels) {
        let mut kept = Labels::new();
        for selection in &self.selections {
            if selection.matches(fields)
                && let Some(value) = fields.get(selection.name_str()).cloned()
            {
                kept.insert(selection.name_str().to_string(), value);
            }
        }

        for reserved in ["__error__", "__error_details__"] {
            if let Some(value) = fields.get(reserved).cloned() {
                kept.insert(reserved.to_string(), value);
            }
        }

        *fields = kept;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LabelSelection {
    name: String,
    matcher: Option<LabelSelectionMatcher>,
}

impl LabelSelection {
    pub fn name(name: impl Into<String>) -> Result<Self, ParseError> {
        let selection = Self {
            name: name.into(),
            matcher: None,
        };
        selection.validate()?;
        Ok(selection)
    }

    pub fn equal(name: impl Into<String>, value: impl Into<String>) -> Result<Self, ParseError> {
        let selection = Self {
            name: name.into(),
            matcher: Some(LabelSelectionMatcher::Equal(value.into())),
        };
        selection.validate()?;
        Ok(selection)
    }

    pub fn regex(name: impl Into<String>, pattern: impl Into<String>) -> Result<Self, ParseError> {
        let selection = Self {
            name: name.into(),
            matcher: Some(LabelSelectionMatcher::Regex(pattern.into())),
        };
        selection.validate()?;
        Ok(selection)
    }

    #[must_use]
    pub fn name_str(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn matcher(&self) -> Option<&LabelSelectionMatcher> {
        self.matcher.as_ref()
    }

    #[must_use]
    fn matches(&self, fields: &Labels) -> bool {
        let Some(value) = fields.get(&self.name) else {
            return false;
        };
        match &self.matcher {
            None => true,
            Some(LabelSelectionMatcher::Equal(expected)) => value == expected,
            Some(LabelSelectionMatcher::Regex(pattern)) => {
                Regex::new(&anchored_regex_pattern(pattern))
                    .expect("label selection regex validated at construction")
                    .is_match(value)
            }
        }
    }

    fn validate(&self) -> Result<(), ParseError> {
        if self.name.is_empty() {
            return Err(template_parse_error("expected label name"));
        }
        if let Some(LabelSelectionMatcher::Regex(pattern)) = &self.matcher {
            Regex::new(&anchored_regex_pattern(pattern)).map_err(|source| {
                ParseError::InvalidRegex {
                    pattern: pattern.clone(),
                    source,
                }
            })?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LabelSelectionMatcher {
    Equal(String),
    Regex(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonParserConfig {
    extractions: Vec<JsonExtraction>,
}

impl JsonParserConfig {
    pub fn new(extractions: Vec<JsonExtraction>) -> Result<Self, ParseError> {
        if extractions.is_empty() {
            return Err(template_parse_error("expected json extraction"));
        }
        let mut destinations = BTreeSet::new();
        for extraction in &extractions {
            if !destinations.insert(extraction.destination.clone()) {
                return Err(template_parse_error(
                    "json extraction destination appears more than once",
                ));
            }
        }
        Ok(Self { extractions })
    }

    #[must_use]
    pub fn extractions(&self) -> &[JsonExtraction] {
        &self.extractions
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonExtraction {
    destination: String,
    expression: String,
    path: JsonPath,
}

impl JsonExtraction {
    pub fn new(
        destination: impl Into<String>,
        expression: impl Into<String>,
    ) -> Result<Self, ParseError> {
        let destination = destination.into();
        let expression = expression.into();
        if destination.is_empty() {
            return Err(template_parse_error("expected json label name"));
        }
        let path = JsonPath::parse(&expression)?;
        Ok(Self {
            destination,
            expression,
            path,
        })
    }

    #[must_use]
    pub fn destination(&self) -> &str {
        &self.destination
    }

    #[must_use]
    pub fn expression(&self) -> &str {
        &self.expression
    }

    fn path(&self) -> &JsonPath {
        &self.path
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct JsonPath {
    parts: Vec<JsonPathPart>,
}

impl JsonPath {
    fn parse(expression: &str) -> Result<Self, ParseError> {
        let mut parser = JsonPathParser::new(expression);
        parser.parse()
    }

    fn evaluate<'a>(&self, value: &'a serde_json::Value) -> Option<&'a serde_json::Value> {
        let mut current = value;
        for part in &self.parts {
            match part {
                JsonPathPart::Field(name) => {
                    current = current.as_object()?.get(name)?;
                }
                JsonPathPart::Index(index) => {
                    current = current.as_array()?.get(*index)?;
                }
            }
        }
        Some(current)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum JsonPathPart {
    Field(String),
    Index(usize),
}

struct JsonPathParser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> JsonPathParser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn parse(&mut self) -> Result<JsonPath, ParseError> {
        let mut parts = Vec::new();
        if let Some(field) = self.parse_field_name() {
            parts.push(JsonPathPart::Field(field));
        }

        while self.pos < self.input.len() {
            match self.peek() {
                Some('.') => {
                    self.pos += 1;
                    let field = self.parse_field_name().ok_or_else(|| {
                        template_parse_error("expected json field name after '.'")
                    })?;
                    parts.push(JsonPathPart::Field(field));
                }
                Some('[') => {
                    self.pos += 1;
                    parts.push(self.parse_bracket_part()?);
                    if self.peek() != Some(']') {
                        return Err(template_parse_error("expected closing json path bracket"));
                    }
                    self.pos += 1;
                }
                _ => return Err(template_parse_error("expected json path component")),
            }
        }

        if parts.is_empty() {
            return Err(template_parse_error("expected json path expression"));
        }
        Ok(JsonPath { parts })
    }

    fn parse_field_name(&mut self) -> Option<String> {
        let start = self.pos;
        while let Some(ch) = self.peek() {
            if ch == '_' || ch == ':' || ch == '-' || ch.is_ascii_alphanumeric() {
                self.pos += ch.len_utf8();
            } else {
                break;
            }
        }
        (self.pos > start).then(|| self.input[start..self.pos].to_string())
    }

    fn parse_bracket_part(&mut self) -> Result<JsonPathPart, ParseError> {
        if self.peek() == Some('"') {
            return self.parse_bracket_string().map(JsonPathPart::Field);
        }

        let start = self.pos;
        while self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
            self.pos += 1;
        }
        if self.pos == start {
            return Err(template_parse_error("expected json path array index"));
        }
        let index = self.input[start..self.pos]
            .parse::<usize>()
            .map_err(|_| template_parse_error("expected json path array index"))?;
        Ok(JsonPathPart::Index(index))
    }

    fn parse_bracket_string(&mut self) -> Result<String, ParseError> {
        self.pos += 1;
        let mut out = String::new();
        while let Some(ch) = self.peek() {
            self.pos += ch.len_utf8();
            match ch {
                '"' => return Ok(out),
                '\\' => {
                    let Some(escaped) = self.peek() else {
                        return Err(template_parse_error("expected escaped json path character"));
                    };
                    self.pos += escaped.len_utf8();
                    out.push(decode_quoted_escape(escaped));
                }
                _ => out.push(ch),
            }
        }
        Err(template_parse_error("expected closing json path string"))
    }

    fn peek(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogfmtParserConfig {
    extractions: Vec<LogfmtExtraction>,
    strict: bool,
    keep_empty: bool,
}

impl LogfmtParserConfig {
    pub fn new(extractions: Vec<LogfmtExtraction>) -> Result<Self, ParseError> {
        Self::with_options(extractions, false, false)
    }

    pub fn flags(strict: bool, keep_empty: bool) -> Result<Self, ParseError> {
        Self::with_options(Vec::new(), strict, keep_empty)
    }

    pub fn with_options(
        extractions: Vec<LogfmtExtraction>,
        strict: bool,
        keep_empty: bool,
    ) -> Result<Self, ParseError> {
        if extractions.is_empty() {
            if strict || keep_empty {
                return Ok(Self {
                    extractions,
                    strict,
                    keep_empty,
                });
            }
            return Err(template_parse_error("expected logfmt extraction"));
        }
        let mut destinations = BTreeSet::new();
        for extraction in &extractions {
            if !destinations.insert(extraction.destination.clone()) {
                return Err(template_parse_error(
                    "logfmt extraction destination appears more than once",
                ));
            }
        }
        Ok(Self {
            extractions,
            strict,
            keep_empty,
        })
    }

    #[must_use]
    pub fn extractions(&self) -> &[LogfmtExtraction] {
        &self.extractions
    }

    #[must_use]
    pub fn strict(&self) -> bool {
        self.strict
    }

    #[must_use]
    pub fn keep_empty(&self) -> bool {
        self.keep_empty
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogfmtExtraction {
    destination: String,
    source: String,
}

impl LogfmtExtraction {
    pub fn same(name: impl Into<String>) -> Result<Self, ParseError> {
        let name = name.into();
        Self::rename(name.clone(), name)
    }

    pub fn rename(
        destination: impl Into<String>,
        source: impl Into<String>,
    ) -> Result<Self, ParseError> {
        let extraction = Self {
            destination: destination.into(),
            source: source.into(),
        };
        extraction.validate()?;
        Ok(extraction)
    }

    #[must_use]
    pub fn destination(&self) -> &str {
        &self.destination
    }

    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }

    fn validate(&self) -> Result<(), ParseError> {
        if self.destination.is_empty() || self.source.is_empty() {
            return Err(template_parse_error("expected logfmt label name"));
        }
        Ok(())
    }
}

struct LogfmtParser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> LogfmtParser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn next_pair(&mut self) -> Option<(String, String)> {
        self.next_pair_with_standalone(false)
    }

    fn next_pair_with_standalone(&mut self, keep_standalone: bool) -> Option<(String, String)> {
        self.next_pair_with_options(keep_standalone, false)
            .unwrap_or(None)
    }

    fn next_pair_with_options(
        &mut self,
        keep_standalone: bool,
        strict: bool,
    ) -> Result<Option<(String, String)>, String> {
        loop {
            self.skip_ws();
            if self.pos == self.input.len() {
                return Ok(None);
            }

            let token_start = self.pos;
            let key = self.parse_key();
            if key.is_empty() || self.peek() != Some('=') {
                if keep_standalone && !key.is_empty() {
                    return Ok(Some((key, String::new())));
                }
                if strict && key.is_empty() {
                    return Err(format!("invalid logfmt token at byte {token_start}"));
                }
                self.skip_token();
                continue;
            }
            self.pos += 1;
            match self.parse_value(strict) {
                Ok(value) => return Ok(Some((key, value))),
                Err(details) if strict => return Err(details),
                Err(_) => continue,
            }
        }
    }

    fn parse_key(&mut self) -> String {
        let start = self.pos;
        while let Some(ch) = self.peek() {
            if ch.is_whitespace() || ch == '=' {
                break;
            }
            self.pos += ch.len_utf8();
        }
        self.input[start..self.pos].to_string()
    }

    fn parse_value(&mut self, strict: bool) -> Result<String, String> {
        if self.peek() == Some('"') {
            self.pos += 1;
            return self.parse_quoted_value().ok_or_else(|| {
                format!(
                    "logfmt syntax error at pos {} : unterminated quoted value",
                    self.pos + 1
                )
            });
        }

        let start = self.pos;
        while let Some(ch) = self.peek() {
            if ch.is_whitespace() {
                break;
            }
            self.pos += ch.len_utf8();
        }
        let value = &self.input[start..self.pos];
        if strict && value.contains('=') {
            return Err(format!("invalid logfmt value at byte {start}"));
        }
        Ok(value.to_string())
    }

    fn parse_quoted_value(&mut self) -> Option<String> {
        let mut out = String::new();
        while let Some(ch) = self.peek() {
            self.pos += ch.len_utf8();
            match ch {
                '"' => return Some(out),
                '\\' => {
                    if let Some(escaped) = self.peek() {
                        self.pos += escaped.len_utf8();
                        out.push(decode_quoted_escape(escaped));
                    }
                }
                _ => out.push(ch),
            }
        }
        None
    }

    fn skip_token(&mut self) {
        while let Some(ch) = self.peek() {
            if ch.is_whitespace() {
                break;
            }
            self.pos += ch.len_utf8();
        }
    }

    fn skip_ws(&mut self) {
        while let Some(ch) = self.peek() {
            if !ch.is_whitespace() {
                break;
            }
            self.pos += ch.len_utf8();
        }
    }

    fn peek(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }
}

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

fn field_filter_expression_to_pipeline_stage(expression: FieldFilterExpression) -> PipelineStage {
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
    pub fn new(op: LineFilterOp, pattern: impl Into<String>) -> Result<Self, ParseError> {
        let filter = Self {
            op,
            pattern: pattern.into(),
            ip_matcher: None,
        };
        filter.validate()?;
        Ok(filter)
    }

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
        let end = start | (!mask & family.mask());
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

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("invalid regex `{pattern}`: {source}")]
    InvalidRegex {
        pattern: String,
        source: regex::Error,
    },
    #[error("{message} at byte {position}")]
    Syntax { message: String, position: usize },
}

pub fn parse_query(input: &str) -> Result<StreamQuery, ParseError> {
    Parser::new(input).parse()
}

pub fn parse_metric_query(input: &str) -> Result<MetricQuery, ParseError> {
    Parser::new(input).parse_metric()
}

pub fn parse_metric_label_replace_query(input: &str) -> Result<MetricLabelReplace, ParseError> {
    Parser::new(input).parse_metric_label_replace()
}

pub fn parse_metric_label_join_query(input: &str) -> Result<MetricLabelJoin, ParseError> {
    Parser::new(input).parse_metric_label_join()
}

pub fn parse_metric_scalar_comparison_query(
    input: &str,
) -> Result<MetricScalarComparison, ParseError> {
    Parser::new(input).parse_metric_scalar_comparison()
}

pub fn parse_metric_scalar_arithmetic_query(
    input: &str,
) -> Result<MetricScalarArithmetic, ParseError> {
    Parser::new(input).parse_metric_scalar_arithmetic()
}

pub fn parse_metric_binary_arithmetic_query(
    input: &str,
) -> Result<MetricBinaryArithmetic, ParseError> {
    Parser::new(input).parse_metric_binary_arithmetic()
}

pub fn parse_metric_binary_comparison_query(
    input: &str,
) -> Result<MetricBinaryComparison, ParseError> {
    Parser::new(input).parse_metric_binary_comparison()
}

pub fn parse_metric_binary_set_query(input: &str) -> Result<MetricBinarySet, ParseError> {
    Parser::new(input).parse_metric_binary_set()
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricQuery {
    pub aggregation: RangeAggregation,
    pub vector_aggregation: Option<VectorAggregation>,
    pub range_grouping: Option<VectorGrouping>,
    pub stream: StreamQuery,
    pub range_ns: i64,
    pub offset_ns: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricLabelReplace {
    pub query: MetricQuery,
    pub destination_label: String,
    pub replacement: String,
    pub source_label: String,
    pub pattern: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricLabelJoin {
    pub query: MetricQuery,
    pub destination_label: String,
    pub separator: String,
    pub source_labels: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricScalarComparison {
    pub query: MetricQuery,
    pub op: ComparisonOp,
    pub bool_modifier: bool,
    pub scalar: String,
    pub scalar_on_left: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricScalarArithmetic {
    pub query: MetricQuery,
    pub op: MetricScalarArithmeticOp,
    pub scalar: String,
    pub scalar_on_left: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricBinaryArithmetic {
    pub left: MetricQuery,
    pub op: MetricScalarArithmeticOp,
    pub matching: Option<MetricVectorMatching>,
    pub right: MetricQuery,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricBinaryComparison {
    pub left: MetricQuery,
    pub op: ComparisonOp,
    pub bool_modifier: bool,
    pub matching: Option<MetricVectorMatching>,
    pub right: MetricQuery,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricBinarySet {
    pub left: MetricQuery,
    pub op: MetricBinarySetOp,
    pub matching: Option<MetricVectorMatching>,
    pub right: MetricQuery,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetricVectorMatching {
    On {
        labels: Vec<String>,
        group: Option<MetricVectorGroupModifier>,
    },
    Ignoring {
        labels: Vec<String>,
        group: Option<MetricVectorGroupModifier>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetricVectorGroupModifier {
    Left(Vec<String>),
    Right(Vec<String>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricBinarySetOp {
    And,
    Or,
    Unless,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricScalarArithmeticOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Power,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RangeAggregation {
    CountOverTime,
    Rate,
    RateCounter,
    BytesRate,
    BytesOverTime,
    AbsentOverTime,
    PresentOverTime,
    SumOverTime,
    AvgOverTime,
    StdvarOverTime,
    StddevOverTime,
    QuantileOverTime(Quantile),
    MinOverTime,
    MaxOverTime,
    FirstOverTime,
    LastOverTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Quantile {
    pub numerator: u64,
    pub denominator: u64,
}

enum RangeAggregationKind {
    Standard(RangeAggregation),
    QuantileOverTime,
}

fn range_aggregation_supports_grouping(aggregation: &RangeAggregation) -> bool {
    matches!(
        aggregation,
        RangeAggregation::AvgOverTime
            | RangeAggregation::StdvarOverTime
            | RangeAggregation::StddevOverTime
            | RangeAggregation::QuantileOverTime(_)
            | RangeAggregation::MinOverTime
            | RangeAggregation::MaxOverTime
            | RangeAggregation::FirstOverTime
            | RangeAggregation::LastOverTime
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorAggregation {
    pub op: VectorAggregationOp,
    pub grouping: Option<VectorGrouping>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VectorAggregationOp {
    Sum,
    Count,
    Min,
    Max,
    Avg,
    Stddev,
    Stdvar,
    CountValues(String),
    TopK(u64),
    BottomK(u64),
    ApproxTopK(u64),
    Sort,
    SortDesc,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VectorGrouping {
    By(Vec<String>),
    Without(Vec<String>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct StreamPlan {
    pub tenant: String,
    pub time_range: TimeRange,
    pub query: StreamQuery,
    pub fingerprints: BTreeSet<SeriesFingerprint>,
    pub blocks: Vec<BlockDescriptor>,
}

#[derive(Debug, Error)]
pub enum PlanError {
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
}

pub fn plan_stream_query(
    tenant: impl Into<String>,
    time_range: TimeRange,
    query: StreamQuery,
    label_index: &LabelIndex,
    block_index: &BlockIndex,
) -> Result<StreamPlan, PlanError> {
    let tenant = tenant.into();
    let predicates = query
        .matchers
        .iter()
        .map(label_predicate)
        .collect::<Result<Vec<_>, _>>()?;
    let fingerprints = label_index.match_series(&tenant, &predicates);
    let fingerprint_list = fingerprints.iter().copied().collect::<Vec<_>>();
    let blocks = if fingerprint_list.is_empty() {
        Vec::new()
    } else {
        block_index.match_blocks(&tenant, time_range, &fingerprint_list)
    };

    Ok(StreamPlan {
        tenant,
        time_range,
        query,
        fingerprints,
        blocks,
    })
}

fn label_predicate(matcher: &LabelMatcher) -> Result<LabelPredicate, BlockStoreError> {
    LabelPredicate::new(
        matcher.name.clone(),
        match matcher.op {
            MatchOp::Equal => BlockMatchOp::Equal,
            MatchOp::NotEqual => BlockMatchOp::NotEqual,
            MatchOp::RegexEqual => BlockMatchOp::RegexEqual,
            MatchOp::RegexNotEqual => BlockMatchOp::RegexNotEqual,
        },
        matcher.value.clone(),
    )
}

struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn parse(mut self) -> Result<StreamQuery, ParseError> {
        let query = self.parse_stream_query(false)?;
        self.skip_ws();
        if self.pos == self.input.len() {
            Ok(query)
        } else {
            Err(self.error("expected end of query"))
        }
    }

    fn parse_metric(mut self) -> Result<MetricQuery, ParseError> {
        self.skip_ws();
        let mut vector_aggregation = self.try_parse_vector_aggregation()?;
        let prefix_grouping = vector_aggregation
            .as_ref()
            .and_then(|aggregation| aggregation.grouping.clone());
        if vector_aggregation.is_some() {
            self.expect('(')?;
            if let Some(vector_aggregation) = &mut vector_aggregation {
                self.parse_vector_aggregation_parameter(&mut vector_aggregation.op)?;
            }
        }
        let aggregation_kind = self.parse_range_aggregation()?;
        self.expect('(')?;
        let aggregation = match aggregation_kind {
            RangeAggregationKind::Standard(aggregation) => aggregation,
            RangeAggregationKind::QuantileOverTime => {
                let quantile = self.parse_quantile()?;
                self.skip_ws();
                self.expect(',')?;
                RangeAggregation::QuantileOverTime(quantile)
            }
        };
        let (stream, range_ns, offset_ns) = self.parse_metric_range_stream_query()?;
        self.expect(')')?;
        let vector_aggregation = if let Some(mut vector_aggregation) = vector_aggregation {
            self.expect(')')?;
            let suffix_grouping = self.try_parse_vector_grouping()?;
            if prefix_grouping.is_some() && suffix_grouping.is_some() {
                return Err(self.error("expected only one vector grouping clause"));
            }
            vector_aggregation.grouping = suffix_grouping.or(prefix_grouping);
            if matches!(vector_aggregation.op, VectorAggregationOp::ApproxTopK(_))
                && vector_aggregation.grouping.is_some()
            {
                return Err(self.error("approx_topk does not support grouping"));
            }
            Some(vector_aggregation)
        } else {
            None
        };
        let range_grouping = if vector_aggregation.is_none() {
            let grouping = self.try_parse_vector_grouping()?;
            if grouping.is_some() && !range_aggregation_supports_grouping(&aggregation) {
                return Err(self.error("range aggregation does not support grouping"));
            }
            grouping
        } else {
            None
        };
        self.skip_ws();
        if self.pos != self.input.len() {
            return Err(self.error("expected end of query"));
        }

        Ok(MetricQuery {
            aggregation,
            vector_aggregation,
            range_grouping,
            stream,
            range_ns,
            offset_ns,
        })
    }

    fn parse_metric_range_stream_query(&mut self) -> Result<(StreamQuery, i64, i64), ParseError> {
        self.skip_ws();
        self.expect('{')?;
        let matchers = self.parse_matchers()?;
        self.expect('}')?;
        self.validate_stream_selector(&matchers)?;

        let mut pipeline = Vec::new();
        let mut range_ns = None;
        let mut offset_ns = 0;
        let mut range_allows_following_pipeline = false;
        loop {
            self.skip_ws();
            if self.pos == self.input.len() {
                break;
            }

            if range_ns.is_none() && self.peek() == Some('[') {
                range_allows_following_pipeline = pipeline.is_empty();
                range_ns = Some(self.parse_range_selector()?);
                offset_ns = self.parse_range_offset()?;
                continue;
            }

            if range_ns.is_some() {
                if self.peek() == Some(')') {
                    break;
                }
                if !range_allows_following_pipeline {
                    return Err(self.error("expected ')'"));
                }
            }

            pipeline.push(self.parse_pipeline_stage()?);
        }

        let Some(range_ns) = range_ns else {
            return Err(self.error("expected range selector"));
        };

        Ok((StreamQuery { matchers, pipeline }, range_ns, offset_ns))
    }

    fn parse_metric_label_replace(mut self) -> Result<MetricLabelReplace, ParseError> {
        self.skip_ws();
        if !self.consume_keyword("label_replace") {
            return Err(self.error("expected label_replace"));
        }
        self.expect('(')?;
        let metric_query = self.parse_metric_function_argument("label_replace")?;
        self.expect(',')?;
        let destination_label = self.parse_quoted()?;
        self.expect(',')?;
        let replacement = self.parse_quoted()?;
        self.expect(',')?;
        let source_label = self.parse_quoted()?;
        self.expect(',')?;
        let pattern = self.parse_quoted()?;
        self.expect(')')?;
        self.skip_ws();
        if self.pos != self.input.len() {
            return Err(self.error("expected end of query"));
        }

        Ok(MetricLabelReplace {
            query: Parser::new(&metric_query).parse_metric()?,
            destination_label,
            replacement,
            source_label,
            pattern,
        })
    }

    fn parse_metric_label_join(mut self) -> Result<MetricLabelJoin, ParseError> {
        self.skip_ws();
        if !self.consume_keyword("label_join") {
            return Err(self.error("expected label_join"));
        }
        self.expect('(')?;
        let metric_query = self.parse_metric_function_argument("label_join")?;
        self.expect(',')?;
        let destination_label = self.parse_quoted()?;
        self.expect(',')?;
        let separator = self.parse_quoted()?;
        self.expect(',')?;
        let mut source_labels = vec![self.parse_quoted()?];
        loop {
            self.skip_ws();
            if self.peek() == Some(')') {
                break;
            }
            self.expect(',')?;
            source_labels.push(self.parse_quoted()?);
        }
        self.expect(')')?;
        self.skip_ws();
        if self.pos != self.input.len() {
            return Err(self.error("expected end of query"));
        }

        Ok(MetricLabelJoin {
            query: Parser::new(&metric_query).parse_metric()?,
            destination_label,
            separator,
            source_labels,
        })
    }

    fn parse_metric_function_argument(
        &mut self,
        function_name: &str,
    ) -> Result<String, ParseError> {
        self.skip_ws();
        let start = self.pos;
        let mut depth = 0usize;
        let mut quote_delimiter = None;
        let mut escaped = false;
        while let Some(ch) = self.peek() {
            if let Some(delimiter) = quote_delimiter {
                self.pos += ch.len_utf8();
                if escaped {
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if ch == delimiter {
                    quote_delimiter = None;
                }
                continue;
            }

            match ch {
                '"' | '`' => {
                    quote_delimiter = Some(ch);
                    self.pos += ch.len_utf8();
                }
                '(' => {
                    depth += 1;
                    self.pos += ch.len_utf8();
                }
                ')' => {
                    if depth == 0 {
                        let message = format!("expected {function_name} metric query argument");
                        return Err(self.error(&message));
                    }
                    depth -= 1;
                    self.pos += ch.len_utf8();
                }
                ',' if depth == 0 => {
                    let metric_query = self.input[start..self.pos].trim();
                    if metric_query.is_empty() {
                        let message = format!("expected {function_name} metric query argument");
                        return Err(self.error(&message));
                    }
                    return Ok(metric_query.to_string());
                }
                _ => self.pos += ch.len_utf8(),
            }
        }
        let message = format!("expected {function_name} metric query argument");
        Err(self.error(&message))
    }

    fn parse_metric_scalar_comparison(mut self) -> Result<MetricScalarComparison, ParseError> {
        self.skip_ws();
        let start = self.pos;
        let comparison = match self.parse_scalar_literal_text() {
            Ok(scalar) => {
                self.skip_ws();
                let op = self.parse_comparison_op()?;
                self.skip_ws();
                let bool_modifier = self.consume_keyword("bool");
                let metric_query_text = self.input[self.pos..].trim().to_string();
                if metric_query_text.is_empty() {
                    return Err(self.error("expected metric expression"));
                }
                self.pos = self.input.len();
                MetricScalarComparison {
                    query: Parser::new(&metric_query_text).parse_metric()?,
                    op,
                    bool_modifier,
                    scalar,
                    scalar_on_left: true,
                }
            }
            Err(_) => {
                self.pos = start;
                let metric_query_text = self.parse_metric_expression_argument()?;
                let query = Parser::new(&metric_query_text).parse_metric()?;
                let op = self.parse_comparison_op()?;
                self.skip_ws();
                let bool_modifier = self.consume_keyword("bool");
                let scalar = self.parse_scalar_literal_text()?;
                MetricScalarComparison {
                    query,
                    op,
                    bool_modifier,
                    scalar,
                    scalar_on_left: false,
                }
            }
        };
        self.skip_ws();
        if self.pos != self.input.len() {
            return Err(self.error("expected end of query"));
        }

        Ok(comparison)
    }

    fn parse_metric_scalar_arithmetic(mut self) -> Result<MetricScalarArithmetic, ParseError> {
        self.skip_ws();
        let start = self.pos;
        let arithmetic = match self.parse_scalar_literal_text() {
            Ok(scalar) => {
                self.skip_ws();
                let Some(op) = self.parse_arithmetic_op() else {
                    return Err(self.error("expected metric arithmetic operator"));
                };
                let metric_query_text = self.input[self.pos..].trim().to_string();
                if metric_query_text.is_empty() {
                    return Err(self.error("expected metric expression"));
                }
                self.pos = self.input.len();
                MetricScalarArithmetic {
                    query: Parser::new(&metric_query_text).parse_metric()?,
                    op,
                    scalar,
                    scalar_on_left: true,
                }
            }
            Err(_) => {
                self.pos = start;
                let (metric_query_text, op) = self.parse_metric_arithmetic_argument()?;
                let query = Parser::new(&metric_query_text).parse_metric()?;
                let scalar = self.parse_scalar_literal_text()?;
                MetricScalarArithmetic {
                    query,
                    op,
                    scalar,
                    scalar_on_left: false,
                }
            }
        };
        self.skip_ws();
        if self.pos != self.input.len() {
            return Err(self.error("expected end of query"));
        }

        Ok(arithmetic)
    }

    fn parse_metric_binary_arithmetic(mut self) -> Result<MetricBinaryArithmetic, ParseError> {
        self.skip_ws();
        let (left_text, op) = self.parse_metric_arithmetic_argument()?;
        let matching = self.parse_metric_vector_matching_modifier(true)?;
        let right_text = self.input[self.pos..].trim().to_string();
        if right_text.is_empty() {
            return Err(self.error("expected metric expression"));
        }
        self.pos = self.input.len();
        let arithmetic = MetricBinaryArithmetic {
            left: Parser::new(&left_text).parse_metric()?,
            op,
            matching,
            right: Parser::new(&right_text).parse_metric()?,
        };
        self.skip_ws();
        if self.pos != self.input.len() {
            return Err(self.error("expected end of query"));
        }

        Ok(arithmetic)
    }

    fn parse_metric_binary_comparison(mut self) -> Result<MetricBinaryComparison, ParseError> {
        self.skip_ws();
        let left_text = self.parse_metric_expression_argument()?;
        let op = self.parse_comparison_op()?;
        self.skip_ws();
        let bool_modifier = self.consume_keyword("bool");
        let matching = self.parse_metric_vector_matching_modifier(true)?;
        let right_text = self.input[self.pos..].trim().to_string();
        if right_text.is_empty() {
            return Err(self.error("expected metric expression"));
        }
        self.pos = self.input.len();
        let comparison = MetricBinaryComparison {
            left: Parser::new(&left_text).parse_metric()?,
            op,
            bool_modifier,
            matching,
            right: Parser::new(&right_text).parse_metric()?,
        };
        self.skip_ws();
        if self.pos != self.input.len() {
            return Err(self.error("expected end of query"));
        }

        Ok(comparison)
    }

    fn parse_metric_binary_set(mut self) -> Result<MetricBinarySet, ParseError> {
        self.skip_ws();
        let (left_text, op) = self.parse_metric_set_argument()?;
        let matching = self.parse_metric_vector_matching_modifier(false)?;
        let right_text = self.input[self.pos..].trim().to_string();
        if right_text.is_empty() {
            return Err(self.error("expected metric expression"));
        }
        self.pos = self.input.len();
        let set = MetricBinarySet {
            left: Parser::new(&left_text).parse_metric()?,
            op,
            matching,
            right: Parser::new(&right_text).parse_metric()?,
        };
        self.skip_ws();
        if self.pos != self.input.len() {
            return Err(self.error("expected end of query"));
        }

        Ok(set)
    }

    fn parse_metric_expression_argument(&mut self) -> Result<String, ParseError> {
        let start = self.pos;
        let mut depth = 0usize;
        let mut quote_delimiter = None;
        let mut escaped = false;
        while let Some(ch) = self.peek() {
            if let Some(delimiter) = quote_delimiter {
                self.pos += ch.len_utf8();
                if escaped {
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if ch == delimiter {
                    quote_delimiter = None;
                }
                continue;
            }

            match ch {
                '"' | '`' => {
                    quote_delimiter = Some(ch);
                    self.pos += ch.len_utf8();
                }
                '(' => {
                    depth += 1;
                    self.pos += ch.len_utf8();
                }
                ')' => {
                    if depth == 0 {
                        return Err(self.error("expected metric expression"));
                    }
                    depth -= 1;
                    self.pos += ch.len_utf8();
                }
                '>' | '<' | '=' | '!' if depth == 0 => {
                    let metric_query = self.input[start..self.pos].trim();
                    if metric_query.is_empty() {
                        return Err(self.error("expected metric expression"));
                    }
                    return Ok(metric_query.to_string());
                }
                _ => self.pos += ch.len_utf8(),
            }
        }
        Err(self.error("expected metric comparison operator"))
    }

    fn parse_metric_arithmetic_argument(
        &mut self,
    ) -> Result<(String, MetricScalarArithmeticOp), ParseError> {
        let start = self.pos;
        let mut depth = 0usize;
        let mut quote_delimiter = None;
        let mut escaped = false;
        while let Some(ch) = self.peek() {
            if let Some(delimiter) = quote_delimiter {
                self.pos += ch.len_utf8();
                if escaped {
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if ch == delimiter {
                    quote_delimiter = None;
                }
                continue;
            }

            match ch {
                '"' | '`' => {
                    quote_delimiter = Some(ch);
                    self.pos += ch.len_utf8();
                }
                '(' => {
                    depth += 1;
                    self.pos += ch.len_utf8();
                }
                ')' => {
                    if depth == 0 {
                        return Err(self.error("expected metric expression"));
                    }
                    depth -= 1;
                    self.pos += ch.len_utf8();
                }
                '+' | '-' | '*' | '/' | '%' | '^' if depth == 0 => {
                    let metric_query = self.input[start..self.pos].trim();
                    if metric_query.is_empty() {
                        return Err(self.error("expected metric expression"));
                    }
                    let op = self.parse_arithmetic_op().expect("operator matched above");
                    return Ok((metric_query.to_string(), op));
                }
                _ => self.pos += ch.len_utf8(),
            }
        }
        Err(self.error("expected metric arithmetic operator"))
    }

    fn parse_metric_set_argument(&mut self) -> Result<(String, MetricBinarySetOp), ParseError> {
        let start = self.pos;
        let mut depth = 0usize;
        let mut quote_delimiter = None;
        let mut escaped = false;
        while let Some(ch) = self.peek() {
            if let Some(delimiter) = quote_delimiter {
                self.pos += ch.len_utf8();
                if escaped {
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if ch == delimiter {
                    quote_delimiter = None;
                }
                continue;
            }

            match ch {
                '"' | '`' => {
                    quote_delimiter = Some(ch);
                    self.pos += ch.len_utf8();
                }
                '(' => {
                    depth += 1;
                    self.pos += ch.len_utf8();
                }
                ')' => {
                    if depth == 0 {
                        return Err(self.error("expected metric expression"));
                    }
                    depth -= 1;
                    self.pos += ch.len_utf8();
                }
                _ if depth == 0 => {
                    if let Some((keyword_len, op)) = self.match_metric_set_op_at(self.pos) {
                        let metric_query = self.input[start..self.pos].trim();
                        if metric_query.is_empty() {
                            return Err(self.error("expected metric expression"));
                        }
                        self.pos += keyword_len;
                        return Ok((metric_query.to_string(), op));
                    }
                    self.pos += ch.len_utf8();
                }
                _ => self.pos += ch.len_utf8(),
            }
        }
        Err(self.error("expected metric set operator"))
    }

    fn match_metric_set_op_at(&self, position: usize) -> Option<(usize, MetricBinarySetOp)> {
        for (keyword, op) in [
            ("unless", MetricBinarySetOp::Unless),
            ("and", MetricBinarySetOp::And),
            ("or", MetricBinarySetOp::Or),
        ] {
            if !self.input[position..].starts_with(keyword) {
                continue;
            }
            let previous = self.input[..position].chars().next_back();
            if previous.is_some_and(is_ident_char) {
                continue;
            }
            let end = position + keyword.len();
            let next = self.input[end..].chars().next();
            if next.is_some_and(is_ident_char) {
                continue;
            }
            return Some((keyword.len(), op));
        }
        None
    }

    fn parse_metric_vector_matching_modifier(
        &mut self,
        allow_group_modifier: bool,
    ) -> Result<Option<MetricVectorMatching>, ParseError> {
        if self.consume_keyword("on") {
            let labels = self.parse_grouping_labels()?;
            let group = self.parse_metric_vector_group_modifier()?;
            if group.is_some() && !allow_group_modifier {
                return Err(self.error("group modifiers are not supported for set operators"));
            }
            return Ok(Some(MetricVectorMatching::On { labels, group }));
        }
        if self.consume_keyword("ignoring") {
            let labels = self.parse_grouping_labels()?;
            let group = self.parse_metric_vector_group_modifier()?;
            if group.is_some() && !allow_group_modifier {
                return Err(self.error("group modifiers are not supported for set operators"));
            }
            return Ok(Some(MetricVectorMatching::Ignoring { labels, group }));
        }
        Ok(None)
    }

    fn parse_metric_vector_group_modifier(
        &mut self,
    ) -> Result<Option<MetricVectorGroupModifier>, ParseError> {
        if self.consume_keyword("group_left") {
            return Ok(Some(MetricVectorGroupModifier::Left(
                self.parse_optional_grouping_labels()?,
            )));
        }
        if self.consume_keyword("group_right") {
            return Ok(Some(MetricVectorGroupModifier::Right(
                self.parse_optional_grouping_labels()?,
            )));
        }
        Ok(None)
    }

    fn parse_optional_grouping_labels(&mut self) -> Result<Vec<String>, ParseError> {
        self.skip_ws();
        if self.peek() == Some('(') {
            self.parse_grouping_labels()
        } else {
            Ok(Vec::new())
        }
    }

    fn parse_arithmetic_op(&mut self) -> Option<MetricScalarArithmeticOp> {
        let op = match self.peek()? {
            '+' => MetricScalarArithmeticOp::Add,
            '-' => MetricScalarArithmeticOp::Subtract,
            '*' => MetricScalarArithmeticOp::Multiply,
            '/' => MetricScalarArithmeticOp::Divide,
            '%' => MetricScalarArithmeticOp::Modulo,
            '^' => MetricScalarArithmeticOp::Power,
            _ => return None,
        };
        self.pos += 1;
        Some(op)
    }

    fn parse_scalar_literal_text(&mut self) -> Result<String, ParseError> {
        self.skip_ws();
        let start = self.pos;
        if matches!(self.peek(), Some('+') | Some('-')) {
            self.pos += 1;
        }

        let whole_start = self.pos;
        while self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
            self.pos += 1;
        }
        let whole_digits = self.pos > whole_start;

        let mut fractional_digits = false;
        if self.peek() == Some('.') {
            self.pos += 1;
            let fractional_start = self.pos;
            while self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
                self.pos += 1;
            }
            fractional_digits = self.pos > fractional_start;
        }

        if !whole_digits && !fractional_digits {
            return Err(self.error("expected scalar literal"));
        }

        if matches!(self.peek(), Some('e') | Some('E')) {
            self.pos += 1;
            if matches!(self.peek(), Some('+') | Some('-')) {
                self.pos += 1;
            }
            let exponent_start = self.pos;
            while self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
                self.pos += 1;
            }
            if self.pos == exponent_start {
                return Err(self.error("expected scalar exponent"));
            }
        }

        Ok(self.input[start..self.pos].to_string())
    }

    fn try_parse_vector_aggregation(&mut self) -> Result<Option<VectorAggregation>, ParseError> {
        let Some(op) = self.try_parse_vector_aggregation_op() else {
            return Ok(None);
        };
        let grouping = self.try_parse_vector_grouping()?;
        Ok(Some(VectorAggregation { op, grouping }))
    }

    fn try_parse_vector_aggregation_op(&mut self) -> Option<VectorAggregationOp> {
        if self.consume_keyword("sum") {
            Some(VectorAggregationOp::Sum)
        } else if self.consume_keyword("count") {
            Some(VectorAggregationOp::Count)
        } else if self.consume_keyword("min") {
            Some(VectorAggregationOp::Min)
        } else if self.consume_keyword("max") {
            Some(VectorAggregationOp::Max)
        } else if self.consume_keyword("avg") {
            Some(VectorAggregationOp::Avg)
        } else if self.consume_keyword("stddev") {
            Some(VectorAggregationOp::Stddev)
        } else if self.consume_keyword("stdvar") {
            Some(VectorAggregationOp::Stdvar)
        } else if self.consume_keyword("count_values") {
            Some(VectorAggregationOp::CountValues(String::new()))
        } else if self.consume_keyword("approx_topk") {
            Some(VectorAggregationOp::ApproxTopK(0))
        } else if self.consume_keyword("topk") {
            Some(VectorAggregationOp::TopK(0))
        } else if self.consume_keyword("bottomk") {
            Some(VectorAggregationOp::BottomK(0))
        } else if self.consume_keyword("sort_desc") {
            Some(VectorAggregationOp::SortDesc)
        } else if self.consume_keyword("sort") {
            Some(VectorAggregationOp::Sort)
        } else {
            None
        }
    }

    fn parse_vector_aggregation_parameter(
        &mut self,
        op: &mut VectorAggregationOp,
    ) -> Result<(), ParseError> {
        match op {
            VectorAggregationOp::TopK(parameter)
            | VectorAggregationOp::BottomK(parameter)
            | VectorAggregationOp::ApproxTopK(parameter) => {
                self.skip_ws();
                *parameter = self.parse_u64_scalar()?;
                self.skip_ws();
                self.expect(',')?;
                Ok(())
            }
            VectorAggregationOp::CountValues(label) => {
                self.skip_ws();
                *label = self.parse_quoted()?;
                self.skip_ws();
                self.expect(',')?;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn parse_u64_scalar(&mut self) -> Result<u64, ParseError> {
        let start = self.pos;
        while self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
            self.pos += 1;
        }
        if self.pos == start {
            return Err(self.error("expected scalar parameter"));
        }
        self.input[start..self.pos]
            .parse::<u64>()
            .map_err(|_| self.error("expected scalar parameter"))
    }

    fn try_parse_vector_grouping(&mut self) -> Result<Option<VectorGrouping>, ParseError> {
        if self.consume_keyword("by") {
            Ok(Some(VectorGrouping::By(self.parse_grouping_labels()?)))
        } else if self.consume_keyword("without") {
            Ok(Some(VectorGrouping::Without(self.parse_grouping_labels()?)))
        } else {
            Ok(None)
        }
    }

    fn parse_grouping_labels(&mut self) -> Result<Vec<String>, ParseError> {
        self.expect('(')?;
        let mut labels = Vec::new();
        loop {
            self.skip_ws();
            if self.peek() == Some(')') {
                self.pos += 1;
                return Ok(labels);
            }
            labels.push(self.parse_ident()?);
            self.skip_ws();
            match self.peek() {
                Some(',') => {
                    self.pos += 1;
                }
                Some(')') => {
                    self.pos += 1;
                    return Ok(labels);
                }
                _ => return Err(self.error("expected ',' or ')'")),
            }
        }
    }

    fn parse_range_aggregation(&mut self) -> Result<RangeAggregationKind, ParseError> {
        if self.consume_keyword("count_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::CountOverTime,
            ))
        } else if self.consume_keyword("rate_counter") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::RateCounter,
            ))
        } else if self.consume_keyword("rate") {
            Ok(RangeAggregationKind::Standard(RangeAggregation::Rate))
        } else if self.consume_keyword("bytes_rate") {
            Ok(RangeAggregationKind::Standard(RangeAggregation::BytesRate))
        } else if self.consume_keyword("bytes_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::BytesOverTime,
            ))
        } else if self.consume_keyword("absent_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::AbsentOverTime,
            ))
        } else if self.consume_keyword("present_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::PresentOverTime,
            ))
        } else if self.consume_keyword("sum_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::SumOverTime,
            ))
        } else if self.consume_keyword("avg_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::AvgOverTime,
            ))
        } else if self.consume_keyword("stdvar_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::StdvarOverTime,
            ))
        } else if self.consume_keyword("stddev_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::StddevOverTime,
            ))
        } else if self.consume_keyword("quantile_over_time") {
            Ok(RangeAggregationKind::QuantileOverTime)
        } else if self.consume_keyword("min_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::MinOverTime,
            ))
        } else if self.consume_keyword("max_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::MaxOverTime,
            ))
        } else if self.consume_keyword("first_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::FirstOverTime,
            ))
        } else if self.consume_keyword("last_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::LastOverTime,
            ))
        } else {
            Err(self.error("expected range aggregation"))
        }
    }

    fn parse_quantile(&mut self) -> Result<Quantile, ParseError> {
        self.skip_ws();
        let whole_start = self.pos;
        while self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
            self.pos += 1;
        }
        if self.pos == whole_start {
            return Err(self.error("expected quantile scalar"));
        }
        let whole = self.input[whole_start..self.pos]
            .parse::<u64>()
            .map_err(|_| self.error("expected quantile scalar"))?;

        let mut denominator = 1_u64;
        let mut fraction = 0_u64;
        if self.peek() == Some('.') {
            self.pos += 1;
            let fraction_start = self.pos;
            while self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
                self.pos += 1;
            }
            if self.pos == fraction_start {
                return Err(self.error("expected quantile scalar"));
            }
            let fraction_text = &self.input[fraction_start..self.pos];
            denominator = 10_u64
                .checked_pow(u32::try_from(fraction_text.len()).unwrap_or(u32::MAX))
                .ok_or_else(|| self.error("expected quantile scalar"))?;
            fraction = fraction_text
                .parse::<u64>()
                .map_err(|_| self.error("expected quantile scalar"))?;
        }

        let numerator = whole
            .checked_mul(denominator)
            .and_then(|value| value.checked_add(fraction))
            .ok_or_else(|| self.error("expected quantile scalar"))?;
        if numerator > denominator {
            return Err(self.error("quantile scalar must be between 0 and 1"));
        }

        let divisor = gcd_u64(numerator, denominator);
        Ok(Quantile {
            numerator: numerator / divisor,
            denominator: denominator / divisor,
        })
    }

    fn parse_stream_query(&mut self, stop_before_range: bool) -> Result<StreamQuery, ParseError> {
        self.skip_ws();
        self.expect('{')?;
        let matchers = self.parse_matchers()?;
        self.expect('}')?;
        self.validate_stream_selector(&matchers)?;

        let mut pipeline = Vec::new();
        loop {
            self.skip_ws();
            if stop_before_range && self.peek() == Some('[') {
                break;
            }
            if self.pos == self.input.len() {
                break;
            }
            pipeline.push(self.parse_pipeline_stage()?);
        }

        Ok(StreamQuery { matchers, pipeline })
    }

    fn validate_stream_selector(&self, matchers: &[LabelMatcher]) -> Result<(), ParseError> {
        if matchers
            .iter()
            .any(|matcher| !matcher.matches_empty_value())
        {
            return Ok(());
        }

        Err(self.error(
            "selector must contain at least one label matcher that does not match the empty string",
        ))
    }

    fn parse_range_selector(&mut self) -> Result<i64, ParseError> {
        self.expect('[')?;
        self.skip_ws();
        let range_ns = self.parse_prometheus_duration()?;
        self.expect(']')?;
        Ok(range_ns)
    }

    fn parse_range_offset(&mut self) -> Result<i64, ParseError> {
        if self.consume_keyword("offset") {
            self.skip_ws();
            let negative = self.consume("-");
            let duration_ns = self.parse_prometheus_duration()?;
            if negative {
                duration_ns
                    .checked_neg()
                    .ok_or_else(|| self.error("range duration overflow"))
            } else {
                Ok(duration_ns)
            }
        } else {
            Ok(0)
        }
    }

    fn parse_prometheus_duration(&mut self) -> Result<i64, ParseError> {
        let mut parsed_chunk = false;
        let mut previous_unit_order = None;
        let mut seen_units = 0_u16;
        let mut total_ns = 0_i128;

        loop {
            let value_start = self.pos;
            while self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
                self.pos += 1;
            }
            if self.pos == value_start {
                if parsed_chunk {
                    break;
                }
                return Err(self.error("expected range duration"));
            }
            let value = self.input[value_start..self.pos]
                .parse::<i128>()
                .map_err(|_| self.error("expected range duration"))?;

            let unit_start = self.pos;
            while self.peek().is_some_and(|ch| ch.is_ascii_alphabetic()) {
                self.pos += 1;
            }
            let unit = &self.input[unit_start..self.pos];
            let Some((unit_order, unit_bit, multiplier)) = duration_unit(unit) else {
                return Err(self.error("expected range duration unit"));
            };
            if seen_units & unit_bit != 0 {
                return Err(self.error("repeated range duration unit"));
            }
            if previous_unit_order.is_some_and(|previous| unit_order <= previous) {
                return Err(self.error("range duration units must be longest to shortest"));
            }

            let chunk_ns = value
                .checked_mul(multiplier)
                .ok_or_else(|| self.error("range duration overflow"))?;
            total_ns = total_ns
                .checked_add(chunk_ns)
                .ok_or_else(|| self.error("range duration overflow"))?;
            seen_units |= unit_bit;
            previous_unit_order = Some(unit_order);
            parsed_chunk = true;

            if self.peek() == Some(']') {
                break;
            }
        }

        i64::try_from(total_ns).map_err(|_| self.error("range duration overflow"))
    }

    fn parse_matchers(&mut self) -> Result<Vec<LabelMatcher>, ParseError> {
        let mut matchers = Vec::new();
        loop {
            self.skip_ws();
            if self.peek() == Some('}') {
                return Ok(matchers);
            }

            let name = self.parse_ident()?;
            self.skip_ws();
            let op = self.parse_match_op()?;
            self.skip_ws();
            let value = self.parse_quoted()?;
            matchers.push(LabelMatcher::new(name, op, value)?);

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

    fn parse_match_op(&mut self) -> Result<MatchOp, ParseError> {
        for (text, op) in [
            ("=~", MatchOp::RegexEqual),
            ("!~", MatchOp::RegexNotEqual),
            ("!=", MatchOp::NotEqual),
            ("=", MatchOp::Equal),
        ] {
            if self.consume(text) {
                return Ok(op);
            }
        }
        Err(self.error("expected label matcher operator"))
    }

    fn parse_pipeline_stage(&mut self) -> Result<PipelineStage, ParseError> {
        if let Some(filter) = self.try_parse_line_filter()? {
            return Ok(PipelineStage::LineFilter(filter));
        }

        if !self.consume("|") {
            return Err(self.error("expected pipeline stage"));
        }
        self.skip_ws();
        if self.consume_keyword("decolorize") {
            return Ok(PipelineStage::Decolorize);
        }
        if self.consume_keyword("json") {
            self.skip_ws();
            if self.pos < self.input.len() && !matches!(self.peek(), Some('|') | Some('[')) {
                return Ok(PipelineStage::Parser(ParserStage::JsonSelected(
                    self.parse_json_parser_config()?,
                )));
            }
            return Ok(PipelineStage::Parser(ParserStage::Json));
        }
        if self.consume_keyword("logfmt") {
            self.skip_ws();
            let (strict, keep_empty) = self.parse_logfmt_parser_flags();
            self.skip_ws();
            if self.pos < self.input.len() && !matches!(self.peek(), Some('|') | Some('[')) {
                return Ok(PipelineStage::Parser(ParserStage::LogfmtSelected(
                    self.parse_logfmt_parser_config(strict, keep_empty)?,
                )));
            }
            if strict || keep_empty {
                return Ok(PipelineStage::Parser(ParserStage::LogfmtConfigured(
                    LogfmtParserConfig::flags(strict, keep_empty)?,
                )));
            }
            return Ok(PipelineStage::Parser(ParserStage::Logfmt));
        }
        if self.consume_keyword("unpack") {
            return Ok(PipelineStage::Parser(ParserStage::Unpack));
        }
        if self.consume_keyword("pattern") {
            let pattern = self.parse_quoted()?;
            return Ok(PipelineStage::Parser(ParserStage::Pattern(
                PatternParser::new(pattern)?,
            )));
        }
        if self.consume_keyword("regexp") {
            let pattern = self.parse_quoted()?;
            return Ok(PipelineStage::Parser(ParserStage::Regexp(
                RegexpParser::new(pattern)?,
            )));
        }
        if self.consume_keyword("line_format") {
            let template = self.parse_quoted()?;
            return Ok(PipelineStage::LineFormat(LineFormat::new(template)?));
        }
        if self.consume_keyword("label_format") {
            return Ok(PipelineStage::LabelFormat(self.parse_label_format()?));
        }
        if self.consume_keyword("drop") {
            return Ok(PipelineStage::DropLabels(self.parse_label_selection_set()?));
        }
        if self.consume_keyword("keep") {
            return Ok(PipelineStage::KeepLabels(self.parse_label_selection_set()?));
        }
        if self.consume_keyword("unwrap") {
            self.skip_ws();
            return Ok(PipelineStage::Unwrap(self.parse_unwrap_expression()?));
        }

        let expression = self.parse_field_filter_expression()?;
        Ok(field_filter_expression_to_pipeline_stage(expression))
    }

    fn try_parse_line_filter(&mut self) -> Result<Option<LineFilter>, ParseError> {
        let op = if self.consume("|=") {
            LineFilterOp::Contains
        } else if self.consume("!=") {
            LineFilterOp::NotContains
        } else if self.consume("|~") {
            LineFilterOp::Regex
        } else if self.consume("!~") {
            LineFilterOp::NotRegex
        } else if self.consume("|>") {
            LineFilterOp::Pattern
        } else if self.consume("!>") {
            LineFilterOp::NotPattern
        } else {
            return Ok(None);
        };
        self.skip_ws();
        if self.consume_keyword("ip") {
            self.expect('(')?;
            let pattern = self.parse_quoted()?;
            self.expect(')')?;
            return LineFilter::ip(op, pattern).map(Some);
        }
        let pattern = self.parse_quoted()?;
        LineFilter::new(op, pattern).map(Some)
    }

    fn parse_label_format(&mut self) -> Result<LabelFormat, ParseError> {
        let mut assignments = Vec::new();
        loop {
            self.skip_ws();
            let destination = self.parse_ident()?;
            self.expect('=')?;
            self.skip_ws();
            let assignment = if matches!(self.peek(), Some('"') | Some('`')) {
                LabelFormatAssignment::template(destination, self.parse_quoted()?)?
            } else {
                LabelFormatAssignment::rename(destination, self.parse_ident()?)?
            };
            assignments.push(assignment);
            if !self.consume(",") {
                break;
            }
        }
        LabelFormat::new(assignments)
    }

    fn parse_unwrap_expression(&mut self) -> Result<UnwrapExpression, ParseError> {
        if self.consume_keyword("bytes") {
            self.expect('(')?;
            let label = self.parse_ident()?;
            self.expect(')')?;
            return UnwrapExpression::bytes(label);
        }
        if self.consume_keyword("duration") || self.consume_keyword("duration_seconds") {
            self.expect('(')?;
            let label = self.parse_ident()?;
            self.expect(')')?;
            return UnwrapExpression::duration(label);
        }

        UnwrapExpression::new(self.parse_ident()?)
    }

    fn parse_json_parser_config(&mut self) -> Result<JsonParserConfig, ParseError> {
        let mut extractions = Vec::new();
        loop {
            self.skip_ws();
            let destination = self.parse_ident()?;
            self.expect('=')?;
            self.skip_ws();
            extractions.push(JsonExtraction::new(destination, self.parse_quoted()?)?);
            if !self.consume(",") {
                break;
            }
        }
        JsonParserConfig::new(extractions)
    }

    fn parse_logfmt_parser_flags(&mut self) -> (bool, bool) {
        let mut strict = false;
        let mut keep_empty = false;
        loop {
            self.skip_ws();
            if self.consume("--strict") {
                strict = true;
            } else if self.consume("--keep-empty") {
                keep_empty = true;
            } else {
                break;
            }
        }
        (strict, keep_empty)
    }

    fn parse_logfmt_parser_config(
        &mut self,
        strict: bool,
        keep_empty: bool,
    ) -> Result<LogfmtParserConfig, ParseError> {
        let mut extractions = Vec::new();
        loop {
            self.skip_ws();
            let destination = self.parse_ident()?;
            self.skip_ws();
            let extraction = if self.consume("=") {
                LogfmtExtraction::rename(destination, self.parse_quoted()?)?
            } else {
                LogfmtExtraction::same(destination)?
            };
            extractions.push(extraction);
            if !self.consume(",") {
                break;
            }
        }
        LogfmtParserConfig::with_options(extractions, strict, keep_empty)
    }

    fn parse_label_selection_set(&mut self) -> Result<LabelSelectionSet, ParseError> {
        let mut selections = Vec::new();
        loop {
            self.skip_ws();
            let name = self.parse_ident()?;
            self.skip_ws();
            let selection = if self.consume("=~") {
                LabelSelection::regex(name, self.parse_quoted()?)?
            } else if self.consume("=") {
                LabelSelection::equal(name, self.parse_quoted()?)?
            } else {
                LabelSelection::name(name)?
            };
            selections.push(selection);
            if !self.consume(",") {
                break;
            }
        }
        LabelSelectionSet::new(selections)
    }

    fn parse_field_filter(&mut self) -> Result<FieldFilter, ParseError> {
        self.skip_ws();
        let name = self.parse_ident()?;
        self.skip_ws();
        let op = self.parse_comparison_op()?;
        self.skip_ws();
        let value = self.parse_field_value()?;
        FieldFilter::try_new(name, op, value)
    }

    fn parse_field_filter_primary(&mut self) -> Result<FieldFilterExpression, ParseError> {
        self.skip_ws();
        if self.consume("(") {
            let expression = self.parse_field_filter_expression()?;
            self.skip_ws();
            self.expect(')')?;
            return Ok(FieldFilterExpression::Group(Box::new(expression)));
        }
        self.parse_field_filter().map(FieldFilterExpression::Filter)
    }

    fn parse_field_filter_expression(&mut self) -> Result<FieldFilterExpression, ParseError> {
        let first = self.parse_field_filter_primary()?;
        let mut rest = Vec::new();
        loop {
            self.skip_ws();
            let op = if self.consume_keyword("and") {
                FieldFilterLogicOp::And
            } else if self.consume_keyword("or") {
                FieldFilterLogicOp::Or
            } else if self.consume(",") {
                FieldFilterLogicOp::And
            } else if self
                .peek()
                .is_some_and(|ch| is_ident_start(ch) || ch == '(')
            {
                FieldFilterLogicOp::And
            } else {
                break;
            };
            rest.push((op, self.parse_field_filter_primary()?));
        }

        if rest.is_empty() {
            Ok(first)
        } else {
            Ok(FieldFilterExpression::Chain {
                first: Box::new(first),
                rest,
            })
        }
    }

    fn parse_comparison_op(&mut self) -> Result<ComparisonOp, ParseError> {
        for (text, op) in [
            (">=", ComparisonOp::GreaterEqual),
            ("<=", ComparisonOp::LessEqual),
            ("=~", ComparisonOp::RegexEqual),
            ("!~", ComparisonOp::RegexNotEqual),
            ("==", ComparisonOp::Equal),
            ("!=", ComparisonOp::NotEqual),
            ("=", ComparisonOp::Equal),
            (">", ComparisonOp::Greater),
            ("<", ComparisonOp::Less),
        ] {
            if self.consume(text) {
                return Ok(op);
            }
        }
        Err(self.error("expected field comparison operator"))
    }

    fn parse_field_value(&mut self) -> Result<FieldValue, ParseError> {
        if self.consume_keyword("ip") {
            self.expect('(')?;
            let pattern = self.parse_quoted()?;
            self.expect(')')?;
            return Ok(FieldValue::Ip(IpMatcher::parse(&pattern)?));
        }
        if matches!(self.peek(), Some('"') | Some('`')) {
            return Ok(FieldValue::String(self.parse_quoted()?));
        }

        let start = self.pos;
        if self.peek() == Some('-') {
            self.pos += 1;
        }
        while let Some(ch) = self.peek() {
            if ch.is_ascii_alphanumeric() || ch == '.' {
                self.pos += ch.len_utf8();
            } else {
                break;
            }
        }
        if self.pos == start || &self.input[start..self.pos] == "-" {
            return Err(self.error("expected field comparison value"));
        }
        let literal = &self.input[start..self.pos];
        if literal.bytes().any(|byte| byte.is_ascii_alphabetic()) {
            if let Some(duration_ns) = parse_prometheus_duration_literal(literal) {
                return Ok(FieldValue::Duration(duration_ns));
            }
            if let Some(bytes) = parse_bytes_literal(literal) {
                return Ok(FieldValue::Bytes(bytes));
            }
            return Err(self.error("expected duration or bytes field comparison value"));
        }
        literal
            .parse()
            .map(FieldValue::Number)
            .map_err(|_| self.error("expected numeric field comparison value"))
    }

    fn parse_ident(&mut self) -> Result<String, ParseError> {
        let start = self.pos;
        while let Some(ch) = self.peek() {
            if is_ident_char(ch) {
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

    fn parse_quoted(&mut self) -> Result<String, ParseError> {
        self.skip_ws();
        if self.peek() == Some('`') {
            self.pos += 1;
            let start = self.pos;
            while let Some(ch) = self.peek() {
                if ch == '`' {
                    let out = self.input[start..self.pos].to_string();
                    self.pos += 1;
                    return Ok(out);
                }
                self.pos += ch.len_utf8();
            }
            return Err(self.error("expected closing backtick"));
        }

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
                    out.push(decode_quoted_escape(escaped));
                }
                _ => out.push(ch),
            }
        }
        Err(self.error("expected closing quote"))
    }

    fn expect(&mut self, expected: char) -> Result<(), ParseError> {
        self.skip_ws();
        if self.peek() == Some(expected) {
            self.pos += expected.len_utf8();
            Ok(())
        } else {
            Err(self.error(&format!("expected {}", QuotedChar(expected))))
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

    fn consume_keyword(&mut self, keyword: &str) -> bool {
        self.skip_ws();
        if !self.input[self.pos..].starts_with(keyword) {
            return false;
        }
        let end = self.pos + keyword.len();
        let next = self.input[end..].chars().next();
        if next.is_some_and(is_ident_char) {
            return false;
        }
        self.pos = end;
        true
    }

    fn skip_ws(&mut self) {
        loop {
            let start = self.pos;
            while let Some(ch) = self.peek() {
                if ch.is_whitespace() {
                    self.pos += ch.len_utf8();
                } else {
                    break;
                }
            }
            if self.peek() == Some('#') {
                while let Some(ch) = self.peek() {
                    self.pos += ch.len_utf8();
                    if ch == '\n' {
                        break;
                    }
                }
            }
            if self.pos == start {
                break;
            }
        }
    }

    fn peek(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn error(&self, message: &str) -> ParseError {
        ParseError::Syntax {
            message: message.to_string(),
            position: self.pos,
        }
    }
}

fn is_ident_start(ch: char) -> bool {
    ch == '_' || ch == ':' || ch == '.' || ch.is_ascii_alphabetic()
}

fn is_ident_char(ch: char) -> bool {
    is_ident_start(ch) || ch.is_ascii_digit()
}

fn gcd_u64(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

struct QuotedChar(char);

fn decode_quoted_escape(escaped: char) -> char {
    match escaped {
        'n' => '\n',
        'r' => '\r',
        't' => '\t',
        '"' => '"',
        '\\' => '\\',
        other => other,
    }
}

fn duration_unit(unit: &str) -> Option<(u8, u16, i128)> {
    match unit {
        "y" => Some((0, 1 << 0, 31_536_000_000_000_000)),
        "w" => Some((1, 1 << 1, 604_800_000_000_000)),
        "d" => Some((2, 1 << 2, 86_400_000_000_000)),
        "h" => Some((3, 1 << 3, 3_600_000_000_000)),
        "m" => Some((4, 1 << 4, 60_000_000_000)),
        "s" => Some((5, 1 << 5, 1_000_000_000)),
        "ms" => Some((6, 1 << 6, 1_000_000)),
        "us" => Some((7, 1 << 7, 1_000)),
        "ns" => Some((8, 1 << 8, 1)),
        _ => None,
    }
}

fn parse_prometheus_duration_literal(value: &str) -> Option<i64> {
    let mut pos = 0;
    let mut parsed_chunk = false;
    let mut previous_unit_order = None;
    let mut seen_units = 0_u16;
    let mut total_ns = 0_i128;

    while pos < value.len() {
        let value_start = pos;
        while value.as_bytes().get(pos).is_some_and(u8::is_ascii_digit) {
            pos += 1;
        }
        if pos == value_start {
            return None;
        }
        let amount = value[value_start..pos].parse::<i128>().ok()?;

        let unit_start = pos;
        while value
            .as_bytes()
            .get(pos)
            .is_some_and(u8::is_ascii_alphabetic)
        {
            pos += 1;
        }
        let (unit_order, unit_bit, multiplier) = duration_unit(&value[unit_start..pos])?;
        if seen_units & unit_bit != 0 {
            return None;
        }
        if previous_unit_order.is_some_and(|previous| unit_order <= previous) {
            return None;
        }

        let chunk_ns = amount.checked_mul(multiplier)?;
        total_ns = total_ns.checked_add(chunk_ns)?;
        seen_units |= unit_bit;
        previous_unit_order = Some(unit_order);
        parsed_chunk = true;
    }

    if !parsed_chunk {
        return None;
    }
    i64::try_from(total_ns).ok()
}

fn format_decimal_ratio(numerator: u128, denominator: u128) -> String {
    let whole = numerator / denominator;
    let mut remainder = numerator % denominator;
    if remainder == 0 {
        return whole.to_string();
    }

    let mut decimals = String::new();
    while remainder != 0 && decimals.len() < 9 {
        remainder *= 10;
        let digit = u8::try_from(remainder / denominator).expect("decimal digit is less than 10");
        decimals.push(char::from(b'0' + digit));
        remainder %= denominator;
    }
    while decimals.ends_with('0') {
        decimals.pop();
    }
    format!("{whole}.{decimals}")
}

fn parse_bytes_literal(value: &str) -> Option<f64> {
    let unit_start = value
        .find(|ch: char| ch.is_ascii_alphabetic())
        .unwrap_or(value.len());
    let amount = value[..unit_start].parse::<f64>().ok()?;
    if !amount.is_finite() || amount < 0.0 {
        return None;
    }
    let multiplier = bytes_unit_multiplier(&value[unit_start..])?;
    Some(amount * multiplier)
}

fn bytes_unit_multiplier(unit: &str) -> Option<f64> {
    match unit {
        "" | "B" => Some(1.0),
        "kB" | "KB" => Some(1_000.0),
        "MB" => Some(1_000_000.0),
        "GB" => Some(1_000_000_000.0),
        "TB" => Some(1_000_000_000_000.0),
        "KiB" => Some(1024.0),
        "MiB" => Some(1024.0 * 1024.0),
        "GiB" => Some(1024.0 * 1024.0 * 1024.0),
        "TiB" => Some(1024.0 * 1024.0 * 1024.0 * 1024.0),
        _ => None,
    }
}

impl fmt::Display for QuotedChar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "'{}'", self.0)
    }
}
