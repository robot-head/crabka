use std::collections::BTreeSet;

use crate::ParseError;
use crate::template::template_parse_error;
use crate::util::decode_quoted_escape;

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

    pub(crate) fn evaluate<'a>(
        &self,
        value: &'a serde_json::Value,
    ) -> Option<&'a serde_json::Value> {
        self.path.evaluate(value)
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
        if self.input.starts_with('.') {
            return Err(template_parse_error(
                "expected json field name before dot separator",
            ));
        }
        if let Some(field) = self.parse_field_name() {
            parts.push(JsonPathPart::Field(field));
        }

        while self.pos < self.input.len() {
            match self.peek() {
                Some('.') => {
                    self.pos = self.pos.saturating_add('.'.len_utf8());
                    let field = self.parse_field_name().ok_or_else(|| {
                        template_parse_error("expected json field name after '.'")
                    })?;
                    parts.push(JsonPathPart::Field(field));
                }
                Some('[') => {
                    self.pos = self.pos.saturating_add('['.len_utf8());
                    parts.push(self.parse_bracket_part()?);
                    if self.peek() != Some(']') {
                        return Err(template_parse_error("expected closing json path bracket"));
                    }
                    self.pos = self.pos.saturating_add(']'.len_utf8());
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
            if is_json_path_field_name_char(ch) {
                self.pos = self.pos.saturating_add(ch.len_utf8());
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
            self.pos = self.pos.saturating_add('0'.len_utf8());
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
        self.pos = self.pos.saturating_add('"'.len_utf8());
        let mut out = String::new();
        while let Some(ch) = self.peek() {
            self.pos = self.pos.saturating_add(ch.len_utf8());
            match ch {
                '"' => return Ok(out),
                '\\' => {
                    let Some(escaped) = self.peek() else {
                        return Err(template_parse_error("expected escaped json path character"));
                    };
                    self.pos = self.pos.saturating_add(escaped.len_utf8());
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

fn is_json_path_field_name_char(ch: char) -> bool {
    matches!(ch, '_' | ':' | '-') || ch.is_ascii_alphanumeric()
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::{JsonExtraction, JsonPath, JsonPathPart, LogfmtExtraction, LogfmtParserConfig};

    #[test]
    fn json_extraction_expression_returns_source_text() {
        let extraction = JsonExtraction::new("value", "trace:id.request-id").unwrap();

        assert_eq!(extraction.expression(), "trace:id.request-id");
    }

    #[test]
    fn json_path_parse_advances_over_dot_separators() {
        assert_eq!(
            JsonPath::parse("request.headers").unwrap(),
            JsonPath {
                parts: vec![
                    JsonPathPart::Field("request".to_string()),
                    JsonPathPart::Field("headers".to_string()),
                ],
            }
        );
    }

    #[test]
    fn json_path_parse_rejects_empty_dot_field_segments() {
        for path in [".request", "request.", "request..headers"] {
            assert!(JsonPath::parse(path).is_err(), "{path}");
        }
    }

    #[test]
    fn json_path_parse_advances_over_array_indexes() {
        assert_eq!(
            JsonPath::parse("servers[10]").unwrap(),
            JsonPath {
                parts: vec![
                    JsonPathPart::Field("servers".to_string()),
                    JsonPathPart::Index(10),
                ],
            }
        );
    }

    #[test]
    fn json_path_parse_accepts_bracket_start_field() {
        assert_eq!(
            JsonPath::parse(r#"["request"].headers"#).unwrap(),
            JsonPath {
                parts: vec![
                    JsonPathPart::Field("request".to_string()),
                    JsonPathPart::Field("headers".to_string()),
                ],
            }
        );
    }

    #[test]
    fn json_path_bracket_strings_decode_escaped_characters() {
        assert_eq!(
            JsonPath::parse(r#"headers["quoted\"name"]"#).unwrap(),
            JsonPath {
                parts: vec![
                    JsonPathPart::Field("headers".to_string()),
                    JsonPathPart::Field("quoted\"name".to_string()),
                ],
            }
        );
    }

    #[test]
    fn json_path_field_names_accept_identifier_punctuation() {
        assert_eq!(
            JsonPath::parse("trace:id.request-id._meta").unwrap(),
            JsonPath {
                parts: vec![
                    JsonPathPart::Field("trace:id".to_string()),
                    JsonPathPart::Field("request-id".to_string()),
                    JsonPathPart::Field("_meta".to_string()),
                ],
            }
        );
    }

    #[test]
    fn logfmt_flags_preserve_non_strict_keep_empty_config() {
        let config = LogfmtParserConfig::flags(false, true).unwrap();

        assert!(!config.strict());
        assert!(config.keep_empty());
    }

    #[test]
    fn logfmt_extractions_reject_empty_destination_or_source() {
        check!(LogfmtExtraction::same("").is_err());
        check!(LogfmtExtraction::rename("", "source").is_err());
        check!(LogfmtExtraction::rename("destination", "").is_err());
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

pub(crate) struct LogfmtParser<'a> {
    input: &'a str,
    pub(crate) pos: usize,
}

impl<'a> LogfmtParser<'a> {
    pub(crate) fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    pub(crate) fn next_pair_with_options(
        &mut self,
        keep_standalone: bool,
        strict: bool,
    ) -> Result<Option<(String, String)>, String> {
        loop {
            let remaining = &self.input[self.pos..];
            let trimmed = remaining.trim_start_matches(char::is_whitespace);
            self.pos = self.input.len().saturating_sub(trimmed.len());
            if self.pos == self.input.len() {
                return Ok(None);
            }

            let token_start = self.pos;
            let key = self.parse_key();
            if key.is_empty() || !self.input[self.pos..].starts_with('=') {
                if keep_standalone && !key.is_empty() {
                    return Ok(Some((key, String::new())));
                }
                if strict && key.is_empty() {
                    return Err(format!("invalid logfmt token at byte {token_start}"));
                }
                let remaining = &self.input[self.pos..];
                let token_end = remaining
                    .find(char::is_whitespace)
                    .map_or(self.input.len(), |offset| self.pos.saturating_add(offset));
                self.pos = token_end;
                continue;
            }
            self.pos = self.pos.saturating_add('='.len_utf8());
            match self.parse_value(strict) {
                Ok(value) => return Ok(Some((key, value))),
                Err(details) if strict => return Err(details),
                Err(_) => continue,
            }
        }
    }

    fn parse_key(&mut self) -> String {
        let start = self.pos;
        let remaining = &self.input[self.pos..];
        let key_end = remaining
            .find(|ch: char| ch.is_whitespace() || ch == '=')
            .map_or(self.input.len(), |offset| self.pos.saturating_add(offset));
        self.pos = key_end;
        self.input[start..self.pos].to_string()
    }

    fn parse_value(&mut self, strict: bool) -> Result<String, String> {
        if self.input[self.pos..].starts_with('"') {
            self.pos = self.pos.saturating_add('"'.len_utf8());
            return self.parse_quoted_value().ok_or_else(|| {
                format!(
                    "logfmt syntax error at pos {} : unterminated quoted value",
                    self.pos.saturating_add(1)
                )
            });
        }

        let start = self.pos;
        let remaining = &self.input[self.pos..];
        let value_end = remaining
            .find(char::is_whitespace)
            .map_or(self.input.len(), |offset| self.pos.saturating_add(offset));
        self.pos = value_end;
        let value = &self.input[start..self.pos];
        if strict && value.contains('=') {
            return Err(format!("invalid logfmt value at byte {start}"));
        }
        Ok(value.to_string())
    }

    fn parse_quoted_value(&mut self) -> Option<String> {
        let mut out = String::new();
        let start = self.pos;
        let mut chars = self.input[start..].char_indices();
        while let Some((offset, ch)) = chars.next() {
            let ch_end = start.saturating_add(offset).saturating_add(ch.len_utf8());
            match ch {
                '"' => {
                    self.pos = ch_end;
                    return Some(out);
                }
                '\\' => {
                    if let Some((escaped_offset, escaped)) = chars.next() {
                        self.pos = start
                            .saturating_add(escaped_offset)
                            .saturating_add(escaped.len_utf8());
                        out.push(decode_quoted_escape(escaped));
                    }
                }
                _ => out.push(ch),
            }
        }
        self.pos = self.input.len();
        None
    }
}
