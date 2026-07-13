use regex::Regex;

use crate::{
    FieldFilter, FieldFilterChain, FieldFilterExpression, JsonParserConfig, LabelFormat,
    LabelSelectionSet, Labels, LineFilter, LineFormat, LogfmtParserConfig, ParseError,
    UnwrapExpression, extract::LogfmtParser,
};

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

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(matchers = self.matchers.len(), stages = self.pipeline.len())
    )]
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
    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
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

pub(crate) fn anchored_regex_pattern(pattern: &str) -> String {
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
        if let Some(value) = extraction.evaluate(&value) {
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
    loop {
        let previous_pos = parser.pos;
        match parser.next_pair_with_options(false, false) {
            Ok(Some((key, value))) => {
                if parser.pos <= previous_pos {
                    break;
                }
                insert_extracted_field(fields, &sanitize_logfmt_field_name(&key), value);
            }
            Ok(None) | Err(_) => break,
        }
    }
}

fn parse_configured_logfmt_fields(line: &str, fields: &mut Labels, config: &LogfmtParserConfig) {
    let mut parser = LogfmtParser::new(line);
    loop {
        let previous_pos = parser.pos;
        match parser.next_pair_with_options(config.keep_empty(), config.strict()) {
            Ok(Some((key, value))) => {
                if parser.pos <= previous_pos {
                    break;
                }
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
        let previous_pos = parser.pos;
        match parser.next_pair_with_options(true, config.strict()) {
            Ok(Some((key, value))) => {
                if parser.pos <= previous_pos {
                    break;
                }
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
    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
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
                    pos = pos.saturating_add(literal.len());
                }
                PatternPart::Capture(name) => {
                    let next_literal =
                        self.parts
                            .iter()
                            .skip(index.saturating_add(1))
                            .find_map(|part| {
                                if let PatternPart::Literal(literal) = part {
                                    Some(literal.as_str())
                                } else {
                                    None
                                }
                            });
                    let value_end = if let Some(next_literal) = next_literal {
                        pos.saturating_add(line[pos..].find(next_literal)?)
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
    let mut has_named_capture = false;
    let mut previous_capture = false;
    let mut separator_since_capture = String::new();

    while let Some(open_offset) = pattern[pos..].find('<') {
        let literal_start = pos;
        let open = pos.saturating_add(open_offset);
        let literal = &pattern[literal_start..open];
        if !literal.is_empty() {
            separator_since_capture.push_str(literal);
            parts.push(PatternPart::Literal(literal.to_string()));
        }

        let capture_start = open.saturating_add(1);
        let close_offset = pattern[capture_start..]
            .find('>')
            .ok_or_else(|| pattern_parse_error("expected closing pattern capture"))?;
        let close = capture_start.saturating_add(close_offset);
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
            has_named_capture = true;
        }
        parts.push(PatternPart::Capture(name.to_string()));
        previous_capture = true;
        separator_since_capture.clear();
        pos = close.saturating_add(1);
    }

    let literal = &pattern[pos..];
    if !literal.is_empty() {
        separator_since_capture.push_str(literal);
        parts.push(PatternPart::Literal(literal.to_string()));
    }

    if !has_named_capture {
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

#[cfg(test)]
mod tests {
    use super::{PatternParser, PatternPart, parse_pattern_parts};

    #[test]
    fn parse_pattern_parts_omits_empty_literals_around_captures() {
        assert_eq!(
            parse_pattern_parts("<method>").unwrap(),
            vec![PatternPart::Capture("method".to_string())]
        );
        assert_eq!(
            parse_pattern_parts("prefix <value>").unwrap(),
            vec![
                PatternPart::Literal("prefix ".to_string()),
                PatternPart::Capture("value".to_string()),
            ]
        );
        assert_eq!(
            parse_pattern_parts("<method> <path>").unwrap(),
            vec![
                PatternPart::Capture("method".to_string()),
                PatternPart::Literal(" ".to_string()),
                PatternPart::Capture("path".to_string()),
            ]
        );
    }

    #[test]
    fn pattern_parser_captures_after_nonzero_prefix() {
        let parser = PatternParser::new("prefix <method> suffix <status>").unwrap();

        assert_eq!(
            parser.captures("prefix GET suffix 500").unwrap(),
            vec![
                ("method".to_string(), "GET".to_string()),
                ("status".to_string(), "500".to_string()),
            ]
        );
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
    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
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

pub(crate) fn insert_extracted_field(fields: &mut Labels, name: &str, value: String) {
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
