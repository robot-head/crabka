use std::collections::BTreeSet;

use regex::Regex;

use crate::{
    Labels, ParseError, UNWRAP_SAMPLE_VALUE_LABEL,
    stream::anchored_regex_pattern,
    template::{LineFormat, template_parse_error},
    util::{format_decimal_ratio, parse_bytes_literal, parse_prometheus_duration_literal},
};

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

    pub(crate) fn apply_with_timestamp(
        &self,
        line: &str,
        fields: &mut Labels,
        timestamp_ns: Option<i64>,
    ) {
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

    pub(crate) fn apply(&self, fields: &mut Labels) {
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
    if value.starts_with('+') || value.starts_with('-') {
        return None;
    }
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

    pub(crate) fn apply_drop(&self, fields: &mut Labels) {
        for selection in &self.selections {
            if selection.matches(fields) {
                fields.remove(selection.name_str());
            }
        }
    }

    pub(crate) fn apply_keep(&self, fields: &mut Labels) {
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

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    #[test]
    fn label_format_accessors_return_configured_assignments() {
        let route = LabelFormatAssignment::rename("route", "path").unwrap();
        let summary =
            LabelFormatAssignment::template("summary", "{{.method}} {{.status}}").unwrap();
        let format = LabelFormat::new(vec![route.clone(), summary.clone()]).unwrap();

        assert_eq!(format.assignments(), &[route.clone(), summary]);
        assert_eq!(route.destination(), "route");
        assert!(matches!(route.value(), LabelFormatValue::Rename(source) if source == "path"));
    }

    #[test]
    fn unwrap_expression_accessors_and_validation_use_label() {
        let expression = UnwrapExpression::bytes("size").unwrap();

        assert_eq!(expression.label(), "size");
        assert_eq!(expression.conversion(), UnwrapConversion::Bytes);
        check!(UnwrapExpression::new("").is_err());
        check!(UnwrapExpression::bytes("").is_err());
        check!(UnwrapExpression::duration("").is_err());
    }

    #[test]
    fn unwrap_bytes_conversion_accepts_only_integer_bytes_in_range() {
        let expression = UnwrapExpression::bytes("size").unwrap();

        assert_eq!(expression.convert_sample_value("1B"), Some("1".to_string()));
        assert_eq!(expression.convert_sample_value("1.5B"), None);
    }

    #[test]
    fn raw_sample_literals_preserve_zero_and_signs() {
        assert_eq!(parse_raw_sample_literal("0"), Some("0".to_string()));
        assert_eq!(parse_raw_sample_literal("+12.5"), Some("12.5".to_string()));
        assert_eq!(parse_raw_sample_literal("-12.5"), Some("-12.5".to_string()));
    }

    #[test]
    fn raw_sample_literals_accept_fractional_boundary_forms() {
        assert_eq!(parse_raw_sample_literal(".5"), Some("0.5".to_string()));
        assert_eq!(parse_raw_sample_literal("1."), Some("1".to_string()));
        assert_eq!(parse_raw_sample_literal("1e2"), Some("100".to_string()));
        assert_eq!(parse_raw_sample_literal("1e-2"), Some("0.01".to_string()));
    }

    #[test]
    fn raw_sample_literals_reject_invalid_digits() {
        assert_eq!(parse_raw_sample_literal("12a.3"), None);
        assert_eq!(parse_raw_sample_literal("12.3a"), None);
        assert_eq!(parse_raw_sample_literal("1e2e3"), None);
    }

    #[test]
    fn label_selection_set_accessors_and_drop_apply_selection() {
        let drop_level = LabelSelection::name("level").unwrap();
        let drop_debug_app = LabelSelection::regex("app", "debug-.*").unwrap();
        let selections =
            LabelSelectionSet::new(vec![drop_level.clone(), drop_debug_app.clone()]).unwrap();

        assert_eq!(selections.selections(), &[drop_level, drop_debug_app]);

        let mut fields = labels(&[("app", "debug-api"), ("level", "warn"), ("status", "500")]);
        selections.apply_drop(&mut fields);

        assert_eq!(fields.get("status"), Some(&"500".to_string()));
        assert!(!fields.contains_key("app"));
        assert!(!fields.contains_key("level"));
    }

    #[test]
    fn label_selection_matcher_accessor_returns_matcher() {
        let exact = LabelSelection::equal("status", "500").unwrap();
        let regex = LabelSelection::regex("app", "api|worker").unwrap();
        let bare = LabelSelection::name("level").unwrap();

        assert_eq!(
            exact.matcher(),
            Some(&LabelSelectionMatcher::Equal("500".to_string()))
        );
        assert_eq!(
            regex.matcher(),
            Some(&LabelSelectionMatcher::Regex("api|worker".to_string()))
        );
        assert_eq!(bare.matcher(), None);
    }

    #[test]
    fn label_selection_matches_requires_present_matching_value() {
        let bare = LabelSelection::name("level").unwrap();
        let exact = LabelSelection::equal("status", "500").unwrap();
        let regex = LabelSelection::regex("app", "api|worker").unwrap();
        let fields = labels(&[("app", "api"), ("status", "500")]);
        let wrong_status = labels(&[("status", "200")]);
        let frontend = labels(&[("app", "frontend")]);

        for (selection, candidate, expected) in [
            (&bare, &fields, false),
            (&exact, &fields, true),
            (&exact, &wrong_status, false),
            (&regex, &fields, true),
            (&regex, &frontend, false),
        ] {
            assert_eq!(
                selection.matches(candidate),
                expected,
                "{selection:?} against {candidate:?}"
            );
        }
    }

    #[test]
    fn label_selection_validation_rejects_empty_names_and_invalid_regex() {
        check!(LabelSelection::name("").is_err());
        check!(LabelSelection::equal("", "value").is_err());
        check!(LabelSelection::regex("app", "[").is_err());
    }
}
