use std::{cmp::Ordering, collections::BTreeMap};

use base64::{Engine as _, prelude::BASE64_STANDARD};
use chrono::{FixedOffset, LocalResult, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use chrono_tz::Tz;
use regex::{NoExpand, Regex};
use time::OffsetDateTime;

use crate::{
    Labels, ParseError,
    util::{format_decimal_ratio, parse_bytes_literal, parse_prometheus_duration_literal},
};

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

    pub(crate) fn render_with_timestamp(
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

    fn is_truthy(&self) -> bool {
        match self {
            Self::String(value) => template_string_truthy(value),
            Self::Integer(value) => *value != 0,
            Self::Json(value) => template_json_value_truthy(value),
        }
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

fn template_json_value_truthy(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => false,
        serde_json::Value::Bool(value) => *value,
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                value != 0
            } else if let Some(value) = value.as_u64() {
                value != 0
            } else {
                value.as_f64().is_some_and(|value| value != 0.0)
            }
        }
        serde_json::Value::String(value) => !value.is_empty(),
        serde_json::Value::Array(value) => !value.is_empty(),
        serde_json::Value::Object(value) => !value.is_empty(),
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

fn template_root_field_value(fields: &Labels, path: &[String]) -> TemplateRuntimeValue {
    let Some((first, rest)) = path.split_first() else {
        let object = fields
            .iter()
            .map(|(key, value)| (key.clone(), serde_json::Value::String(value.clone())))
            .collect();
        return TemplateRuntimeValue::Json(serde_json::Value::Object(object));
    };

    let Some(value) = fields.get(first) else {
        return TemplateRuntimeValue::String(String::new());
    };
    if rest.is_empty() {
        return TemplateRuntimeValue::String(value.clone());
    }

    serde_json::from_str::<serde_json::Value>(value)
        .ok()
        .and_then(|json| template_variable_path_value(&TemplateRuntimeValue::Json(json), rest))
        .unwrap_or_else(|| TemplateRuntimeValue::String(String::new()))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TemplateConditional {
    branches: Vec<(TemplateControlExpression, Vec<TemplatePart>)>,
    else_parts: Vec<TemplatePart>,
}

impl TemplateConditional {
    fn render(&self, context: &TemplateRenderContext<'_>) -> String {
        let mut context = context.clone();
        for (condition, parts) in &self.branches {
            let (value, branch_context) = condition.evaluate(&context);
            if value.is_truthy() {
                return render_template_parts(parts, &branch_context);
            }
            context = branch_context;
        }
        render_template_parts(&self.else_parts, &context)
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
    expression: TemplateControlExpression,
    parts: Vec<TemplatePart>,
    else_parts: Vec<TemplatePart>,
}

impl TemplateWith {
    fn render(&self, context: &TemplateRenderContext<'_>) -> String {
        let (value, context) = self.expression.evaluate(context);
        if !value.is_truthy() {
            return render_template_parts(&self.else_parts, &context);
        }
        let child_context = context.with_current_dot(value);
        render_template_parts(&self.parts, &child_context)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TemplateControlExpression {
    variable: Option<String>,
    expression: TemplateExpression,
}

impl TemplateControlExpression {
    fn parse(expression: &str) -> Result<Self, ParseError> {
        let (variable, expression) = parse_template_control_assignment(expression)?
            .map_or((None, expression.trim()), |(variable, expression)| {
                (Some(variable), expression)
            });
        Ok(Self {
            variable,
            expression: TemplateExpression::parse(expression)?,
        })
    }

    fn evaluate<'a>(
        &self,
        context: &TemplateRenderContext<'a>,
    ) -> (TemplateRuntimeValue, TemplateRenderContext<'a>) {
        let value = self.expression.evaluate(context);
        let context = self.variable.as_ref().map_or_else(
            || context.clone(),
            |variable| context.with_variable(variable.clone(), value.clone()),
        );
        (value, context)
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
    Root { path: Vec<String> },
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
        if token == "$" {
            return Ok(Self::Root { path: Vec::new() });
        }
        if let Some(path) = token.strip_prefix("$.") {
            return Ok(Self::Root {
                path: path
                    .split('.')
                    .filter(|part| !part.is_empty())
                    .map(ToString::to_string)
                    .collect(),
            });
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
            Self::Root { path } => template_root_field_value(context.fields, path),
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
    let untrimmed_next_pos = close + 2;
    let mut next_pos = untrimmed_next_pos;
    if trim_right {
        next_pos = skip_leading_template_whitespace(template, next_pos);
        if next_pos < untrimmed_next_pos {
            return Err(template_parse_error(
                "template action parser did not advance",
            ));
        }
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
    let Some(rest) = template.get(pos..) else {
        return template.len();
    };
    let trimmed = rest.trim_start_matches(char::is_whitespace);
    pos = template
        .len()
        .checked_sub(trimmed.len())
        .expect("trimmed suffix cannot be longer than template");
    pos
}

fn trim_template_body_end(template: &str, start: usize, end: usize) -> usize {
    template[..end]
        .trim_end_matches(char::is_whitespace)
        .len()
        .max(start)
}

fn parse_template_parts(template: &str) -> Result<Vec<TemplatePart>, ParseError> {
    let mut parts = Vec::new();
    let mut pos = 0;
    while let Some(rest) = template.get(pos..) {
        if rest.is_empty() {
            break;
        }
        let Some(open_offset) = rest.find("{{") else {
            parts.push(TemplatePart::Literal(rest.to_string()));
            break;
        };
        let open = pos
            .checked_add(open_offset)
            .expect("template action offset cannot overflow");
        let literal = &template[pos..open];
        if !literal.is_empty() {
            let literal = if template_action_trim_left(template, open)? {
                literal.trim_end_matches(char::is_whitespace).to_string()
            } else {
                literal.to_string()
            };
            parts.push(TemplatePart::Literal(literal));
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
        if is_unexpected_template_control_action(expression) {
            return Err(template_parse_error("unexpected template control action"));
        }
        parts.push(TemplatePart::Expression(TemplateExpression::parse(
            expression,
        )?));
        pos = action.next_pos;
    }
    Ok(parts)
}

fn is_unexpected_template_control_action(expression: &str) -> bool {
    matches!(
        template_control_action(expression),
        TemplateControlAction::Else
            | TemplateControlAction::ElseIf
            | TemplateControlAction::ElseWith
            | TemplateControlAction::End
    )
}

fn parse_template_assignment(expression: &str) -> Result<Option<TemplateAssignment>, ParseError> {
    if !expression.trim_start().starts_with('$') {
        return Ok(None);
    }
    let (variable, expression) = if let Some((variable, expression)) = expression.split_once(":=") {
        (variable, expression)
    } else if let Some((variable, expression)) = expression.split_once('=') {
        if variable
            .trim()
            .contains(is_template_control_assignment_variable_char)
        {
            return Ok(None);
        }
        (variable, expression)
    } else {
        return Ok(None);
    };
    let variable = parse_template_variable_name(variable.trim(), "expected template variable")?;
    Ok(Some(TemplateAssignment {
        variable,
        expression: TemplateExpression::parse(expression.trim())?,
    }))
}

fn parse_template_control_assignment(
    expression: &str,
) -> Result<Option<(String, &str)>, ParseError> {
    if !expression.trim_start().starts_with('$') {
        return Ok(None);
    }
    let Some((variable, expression)) = expression.split_once(":=") else {
        return Ok(None);
    };
    if variable
        .trim()
        .contains(is_template_control_assignment_variable_char)
    {
        return Ok(None);
    }
    Ok(Some((
        parse_template_variable_name(variable.trim(), "expected template variable")?,
        expression.trim(),
    )))
}

fn parse_template_conditional(
    template: &str,
    mut branch_start: usize,
    first_condition: &str,
) -> Result<(TemplateConditional, usize), ParseError> {
    let mut branches = Vec::new();
    let mut condition = TemplateControlExpression::parse(first_condition)?;
    loop {
        let Some((body_end, expression, next_pos)) =
            find_template_control_action(template, branch_start)?
        else {
            return Err(template_parse_error("expected template end action"));
        };
        let branch_parts = parse_template_parts(&template[branch_start..body_end])?;
        if let Some(next_condition) = expression.strip_prefix("else if ") {
            branches.push((condition, branch_parts));
            condition = TemplateControlExpression::parse(next_condition.trim())?;
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
    let expression = TemplateControlExpression::parse(with_expression.trim())?;
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
        if let Some(with_expression) = control_expression.strip_prefix("else with ") {
            let (with, next_pos) = parse_template_with(template, control_next, with_expression)?;
            return Ok((
                TemplateWith {
                    expression,
                    parts,
                    else_parts: vec![TemplatePart::With(with)],
                },
                next_pos,
            ));
        }
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
    if variable.is_empty() {
        return Err(template_parse_error(message));
    }
    if variable.contains(is_template_variable_name_char_invalid) {
        return Err(template_parse_error(message));
    }
    Ok(variable.to_string())
}

fn is_template_control_assignment_variable_char(ch: char) -> bool {
    match ch {
        '|' => true,
        _ => ch.is_whitespace(),
    }
}

fn is_template_variable_name_char_invalid(ch: char) -> bool {
    match ch {
        '.' => true,
        _ => ch.is_whitespace(),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TemplateControlAction {
    If,
    Range,
    With,
    Else,
    ElseIf,
    ElseWith,
    End,
    Other,
}

fn template_control_action(expression: &str) -> TemplateControlAction {
    match expression {
        "else" => TemplateControlAction::Else,
        "end" => TemplateControlAction::End,
        _ if expression.starts_with("if ") => TemplateControlAction::If,
        _ if expression.starts_with("range ") => TemplateControlAction::Range,
        _ if expression.starts_with("with ") => TemplateControlAction::With,
        _ if expression.starts_with("else if ") => TemplateControlAction::ElseIf,
        _ if expression.starts_with("else with ") => TemplateControlAction::ElseWith,
        _ => TemplateControlAction::Other,
    }
}

fn find_template_control_action(
    template: &str,
    mut pos: usize,
) -> Result<Option<(usize, &str, usize)>, ParseError> {
    let body_start = pos;
    let mut nested_controls = Vec::new();
    loop {
        let Some(rest) = template.get(pos..) else {
            return Ok(None);
        };
        if rest.is_empty() {
            return Ok(None);
        }
        let Some(open_offset) = rest.find("{{") else {
            return Ok(None);
        };
        let open = pos
            .checked_add(open_offset)
            .expect("template action offset cannot overflow");
        let action = parse_template_action(template, open)?;
        let expression = action.expression;
        if is_template_comment_action(expression) {
            pos = action.next_pos;
            continue;
        }
        match template_control_action(expression) {
            TemplateControlAction::If
            | TemplateControlAction::Range
            | TemplateControlAction::With => {
                nested_controls.push(());
            }
            TemplateControlAction::End => {
                if nested_controls.is_empty() {
                    let body_end = if action.trim_left {
                        trim_template_body_end(template, body_start, open)
                    } else {
                        open
                    };
                    return Ok(Some((body_end, expression, action.next_pos)));
                }
                nested_controls.pop();
            }
            TemplateControlAction::Else
            | TemplateControlAction::ElseIf
            | TemplateControlAction::ElseWith
                if nested_controls.is_empty() =>
            {
                let body_end = if action.trim_left {
                    trim_template_body_end(template, body_start, open)
                } else {
                    open
                };
                return Ok(Some((body_end, expression, action.next_pos)));
            }
            _ => {}
        }
        pos = action.next_pos;
    }
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

fn template_string_truthy(value: &str) -> bool {
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
        if matches!(ch, '"' | '`') {
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
    while let Some(rest) = command.get(pos..) {
        let Some((offset, ch)) = rest.char_indices().find(|(_, ch)| !ch.is_whitespace()) else {
            break;
        };
        pos = pos
            .checked_add(offset)
            .expect("template token offset cannot overflow");
        if matches!(ch, '"' | '`') {
            let (token, next) = parse_template_quoted_token(command, pos, ch)?;
            ensure_template_quoted_token(command, pos, &token, next, ch)?;
            tokens.push(token);
            pos = next;
        } else if ch == '(' {
            let (token, next) = parse_template_parenthesized_token(command, pos)?;
            ensure_template_parenthesized_token(command, pos, &token, next)?;
            tokens.push(token);
            pos = next;
        } else {
            let end = command
                .get(pos..)
                .and_then(|rest| rest.find(char::is_whitespace))
                .map(|offset| {
                    pos.checked_add(offset)
                        .expect("template token end offset cannot overflow")
                })
                .unwrap_or(command.len());
            tokens.push(command[pos..end].to_string());
            pos = end;
        }
    }
    Ok(tokens)
}

fn ensure_template_quoted_token(
    command: &str,
    pos: usize,
    token: &str,
    next: usize,
    quote: char,
) -> Result<(), ParseError> {
    if next <= pos {
        return Err(template_parse_error(
            "template token parser did not advance",
        ));
    }
    if next > command.len() {
        return Err(template_parse_error(
            "template token parser advanced past command",
        ));
    }
    if !is_wrapped_template_token(token, quote) {
        return Err(template_parse_error(
            "template token parser returned unwrapped quoted token",
        ));
    }
    Ok(())
}

fn ensure_template_parenthesized_token(
    command: &str,
    pos: usize,
    token: &str,
    next: usize,
) -> Result<(), ParseError> {
    if next <= pos {
        return Err(template_parse_error(
            "template token parser did not advance",
        ));
    }
    if next > command.len() {
        return Err(template_parse_error(
            "template token parser advanced past command",
        ));
    }
    if !token.starts_with('(') {
        return Err(template_parse_error(
            "template token parser returned token without opening parenthesis",
        ));
    }
    if !token.ends_with(')') {
        return Err(template_parse_error(
            "template token parser returned token without closing parenthesis",
        ));
    }
    Ok(())
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
        if matches!(ch, '"' | '`') {
            quote = Some(ch);
            continue;
        }
        match ch {
            '(' => depth = depth.saturating_add(1),
            ')' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| template_parse_error("unexpected template parenthesis"))?;
                if depth == 0 {
                    return Ok((
                        command[start..=index].to_string(),
                        index.saturating_add(ch.len_utf8()),
                    ));
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
    let value_start = start.saturating_add(quote.len_utf8());
    for (offset, ch) in command[value_start..].char_indices() {
        let index = value_start.saturating_add(offset);
        if escaped {
            escaped = false;
            continue;
        }
        if quote == '"' && ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == quote {
            return Ok((
                command[start..=index].to_string(),
                index.saturating_add(quote.len_utf8()),
            ));
        }
    }
    Err(template_parse_error("unterminated template string"))
}

fn quoted_template_token_value(token: &str) -> Result<Option<String>, ParseError> {
    if is_wrapped_template_token(token, '`') {
        return Ok(Some(token[1..token.len() - 1].to_string()));
    }
    if is_wrapped_template_token(token, '"') {
        return Ok(Some(decode_quoted_fragment(&token[1..token.len() - 1])?));
    }
    Ok(None)
}

fn is_wrapped_template_token(token: &str, quote: char) -> bool {
    if token.len() < quote.len_utf8().saturating_mul(2) {
        return false;
    }
    token.starts_with(quote) && token.ends_with(quote)
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
    if name == "and" {
        return TemplateRuntimeValue::String(
            args.iter().all(TemplateRuntimeValue::is_truthy).to_string(),
        );
    }
    if name == "not" {
        return TemplateRuntimeValue::String(
            args.first()
                .is_none_or(|value| !value.is_truthy())
                .to_string(),
        );
    }
    if name == "or" {
        return TemplateRuntimeValue::String(
            args.iter().any(TemplateRuntimeValue::is_truthy).to_string(),
        );
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
    if value == 0.0 {
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
    if !factor.is_finite() {
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
    let mut rest = layout;
    while !rest.is_empty() {
        if let Some(next) = rest.strip_prefix("2006") {
            formatted.push_str(&format!("{:04}", timestamp.year()));
            rest = next;
        } else if let Some(next) = rest.strip_prefix("06") {
            formatted.push_str(&format!("{:02}", timestamp.year().rem_euclid(100)));
            rest = next;
        } else if let Some(next) = rest.strip_prefix("15") {
            formatted.push_str(&format!("{:02}", timestamp.hour()));
            rest = next;
        } else if let Some(next) = rest.strip_prefix("04") {
            formatted.push_str(&format!("{:02}", timestamp.minute()));
            rest = next;
        } else if let Some(next) = rest.strip_prefix("05") {
            formatted.push_str(&format!("{:02}", timestamp.second()));
            rest = next;
        } else if let Some(next) = rest.strip_prefix("01") {
            formatted.push_str(&format!("{:02}", u8::from(timestamp.month())));
            rest = next;
        } else if let Some(next) = rest.strip_prefix('1') {
            formatted.push_str(&u8::from(timestamp.month()).to_string());
            rest = next;
        } else if let Some(next) = rest.strip_prefix("02") {
            formatted.push_str(&format!("{:02}", timestamp.day()));
            rest = next;
        } else if let Some(next) = rest.strip_prefix('2') {
            formatted.push_str(&timestamp.day().to_string());
            rest = next;
        } else if let Some(next) = rest.strip_prefix("Z07:00") {
            formatted.push('Z');
            rest = next;
        } else if let Some(next) = rest.strip_prefix("-07:00") {
            formatted.push_str("+00:00");
            rest = next;
        } else if let Some(fraction_rest) = rest.strip_prefix('.') {
            let digits = fraction_rest
                .chars()
                .take_while(|ch| *ch == '0' || *ch == '9')
                .count();
            if digits == 0 {
                formatted.push('.');
                rest = fraction_rest;
                continue;
            }
            let fraction = format!("{:09}", timestamp.nanosecond());
            formatted.push('.');
            formatted.push_str(&fraction[..digits.min(fraction.len())]);
            rest = &fraction_rest[digits..];
        } else {
            let ch = rest.chars().next().expect("layout rest is not empty");
            formatted.push(ch);
            rest = &rest[ch.len_utf8()..];
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
    let mut value_pos = 0usize;
    let mut rest = layout;
    while !rest.is_empty() {
        if let Some(next) = rest.strip_prefix("2006") {
            parsed.year = parse_fixed_template_digits(value, &mut value_pos, 4)? as i32;
            rest = next;
        } else if let Some(next) = rest.strip_prefix("06") {
            parsed.year = 2000 + parse_fixed_template_digits(value, &mut value_pos, 2)? as i32;
            rest = next;
        } else if let Some(next) = rest.strip_prefix("15") {
            parsed.hour = parse_fixed_template_digits(value, &mut value_pos, 2)?;
            rest = next;
        } else if let Some(next) = rest.strip_prefix("04") {
            parsed.minute = parse_fixed_template_digits(value, &mut value_pos, 2)?;
            rest = next;
        } else if let Some(next) = rest.strip_prefix("05") {
            parsed.second = parse_fixed_template_digits(value, &mut value_pos, 2)?;
            rest = next;
        } else if let Some(next) = rest.strip_prefix("01") {
            parsed.month = parse_fixed_template_digits(value, &mut value_pos, 2)?;
            rest = next;
        } else if let Some(next) = rest.strip_prefix('1') {
            parsed.month = parse_variable_template_digits(value, &mut value_pos, 2)?;
            rest = next;
        } else if let Some(next) = rest.strip_prefix("02") {
            parsed.day = parse_fixed_template_digits(value, &mut value_pos, 2)?;
            rest = next;
        } else if let Some(next) = rest.strip_prefix('2') {
            parsed.day = parse_variable_template_digits(value, &mut value_pos, 2)?;
            rest = next;
        } else if let Some(next) = rest.strip_prefix("Z07:00") {
            parsed.offset_seconds = Some(parse_template_timezone_offset(value, &mut value_pos)?);
            rest = next;
        } else if let Some(next) = rest.strip_prefix("-07:00") {
            parsed.offset_seconds = Some(parse_template_timezone_offset(value, &mut value_pos)?);
            rest = next;
        } else if let Some(fraction_rest) = rest.strip_prefix('.') {
            let digits = fraction_rest
                .chars()
                .take_while(|ch| *ch == '0' || *ch == '9')
                .count();
            if digits == 0 {
                match_template_literal(value, &mut value_pos, '.')?;
                rest = fraction_rest;
            } else {
                parsed.nanosecond =
                    parse_template_fractional_nanoseconds(value, &mut value_pos, digits)?;
                rest = &fraction_rest[digits..];
            }
        } else {
            let ch = rest.chars().next()?;
            match_template_literal(value, &mut value_pos, ch)?;
            rest = &rest[ch.len_utf8()..];
        }
    }
    (value_pos == value.len()).then_some(parsed)
}

fn parse_fixed_template_digits(value: &str, pos: &mut usize, count: usize) -> Option<u32> {
    let start = *pos;
    let digits = value.get(start..)?.get(..count)?;
    digits
        .bytes()
        .all(|byte| byte.is_ascii_digit())
        .then_some(())?;
    advance_template_pos(pos, count)?;
    digits.parse::<u32>().ok()
}

fn parse_variable_template_digits(value: &str, pos: &mut usize, max_count: usize) -> Option<u32> {
    let start = *pos;
    let rest = value.get(start..)?;
    let digits_len = rest
        .bytes()
        .take(max_count)
        .take_while(u8::is_ascii_digit)
        .count();
    let digits = rest.get(..digits_len).filter(|digits| !digits.is_empty())?;
    advance_template_pos(pos, digits_len)?;
    digits.parse::<u32>().ok()
}

fn parse_template_fractional_nanoseconds(
    value: &str,
    pos: &mut usize,
    max_digits: usize,
) -> Option<u32> {
    match_template_literal(value, pos, '.')?;
    let start = *pos;
    let rest = value.get(start..)?;
    let digits_len = rest
        .bytes()
        .take(max_digits)
        .take_while(u8::is_ascii_digit)
        .count();
    let digits = rest.get(..digits_len).filter(|digits| !digits.is_empty())?;
    advance_template_pos(pos, digits_len)?;
    let mut fraction = digits.parse::<u32>().ok()?;
    for _ in digits_len..9 {
        fraction = fraction.checked_mul(10)?;
    }
    Some(fraction)
}

fn parse_template_timezone_offset(value: &str, pos: &mut usize) -> Option<i32> {
    let rest = value.get(*pos..)?;
    if rest.starts_with('Z') {
        advance_template_pos(pos, 1)?;
        return Some(0);
    }
    let sign: i32 = if rest.starts_with('+') {
        advance_template_pos(pos, 1)?;
        1
    } else if rest.starts_with('-') {
        advance_template_pos(pos, 1)?;
        -1
    } else {
        return None;
    };
    let hours = i32::try_from(parse_fixed_template_digits(value, pos, 2)?).ok()?;
    match_template_literal(value, pos, ':')?;
    let minutes = i32::try_from(parse_fixed_template_digits(value, pos, 2)?).ok()?;
    let total_minutes = hours.checked_mul(60)?.checked_add(minutes)?;
    sign.checked_mul(total_minutes.checked_mul(60)?)
}

fn match_template_literal(value: &str, pos: &mut usize, expected: char) -> Option<()> {
    let ch = value.get(*pos..)?.chars().next()?;
    if ch != expected {
        return None;
    }
    advance_template_pos(pos, ch.len_utf8())?;
    Some(())
}

fn advance_template_pos(pos: &mut usize, amount: usize) -> Option<()> {
    *pos = pos.checked_add(amount)?;
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
    let end = usize::try_from(end).ok().map_or(len, |end| end.min(len));
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
    output.push_str(&format!("\\u{value:04X}"));
}

fn urldecode_template_string(value: &str) -> String {
    let mut bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    while let Some((&byte, rest)) = bytes.split_first() {
        if byte == b'%'
            && let Some(hex_bytes) = rest.get(..2)
            && let Ok(hex) = std::str::from_utf8(hex_bytes)
            && let Ok(decoded_byte) = u8::from_str_radix(hex, 16)
        {
            decoded.push(decoded_byte);
            bytes = &rest[2..];
            continue;
        }
        decoded.push(byte);
        bytes = rest;
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

pub(crate) fn template_parse_error(message: &str) -> ParseError {
    ParseError::Syntax {
        message: message.to_string(),
        position: 0,
    }
}

#[cfg(test)]
mod tests {
    use std::{cmp::Ordering, collections::BTreeMap};

    use super::{
        LineFormat, TemplatePart, TemplateRuntimeValue, ensure_template_parenthesized_token,
        ensure_template_quoted_token, evaluate_template_index, evaluate_template_slice,
        format_go_time_layout, format_template_bytes, format_template_date, format_template_float,
        format_template_float_round, format_template_integer_binary, format_template_ordering,
        format_template_to_date, format_template_to_date_in_zone,
        is_template_control_assignment_variable_char, is_template_variable_name_char_invalid,
        js_escape_template_string, parse_go_time_layout_value,
        parse_template_fractional_nanoseconds, parse_template_parenthesized_token,
        parse_template_parts, parse_template_quoted_token, parse_template_timezone_offset,
        parse_variable_template_digits, push_template_unicode_escape, quoted_template_token_value,
        skip_leading_template_whitespace, substring_template_string, template_compare_values,
        template_index_value, template_slice_bounds, template_value_is_collection,
        tokenize_template_command, trim_template_body_end, urldecode_template_string,
    };

    #[test]
    fn template_helpers_trim_body_suffixes_and_literal_boundaries() {
        assert_eq!(trim_template_body_end("prefix body \n\t", 7, 14), 11);
        assert_eq!(trim_template_body_end("prefix \n\t", 7, 9), 7);

        assert!(parse_template_parts("").unwrap().is_empty());
        assert_eq!(
            parse_template_parts("literal").unwrap(),
            vec![TemplatePart::Literal("literal".to_string())]
        );
    }

    #[test]
    fn template_helpers_skip_leading_whitespace_from_current_position() {
        assert_eq!(skip_leading_template_whitespace("abc \n\tdef", 3), 6);
        assert_eq!(skip_leading_template_whitespace("abc", 1), 1);
        assert_eq!(skip_leading_template_whitespace("abc", 10), 3);
    }

    #[test]
    fn template_helpers_classify_invalid_variable_boundaries() {
        for (ch, expected) in [('|', true), (' ', true), ('\n', true), ('_', false)] {
            assert_eq!(
                is_template_control_assignment_variable_char(ch),
                expected,
                "control-assignment boundary: {ch:?}"
            );
        }

        for (ch, expected) in [('.', true), (' ', true), ('\t', true), ('_', false)] {
            assert_eq!(
                is_template_variable_name_char_invalid(ch),
                expected,
                "variable-name boundary: {ch:?}"
            );
        }
    }

    #[test]
    fn template_token_guards_reject_non_advancing_or_unwrapped_results() {
        for (token, end, expected_ok) in [
            ("`ok`", 4, true),
            ("`ok`", 0, false),
            ("`ok`", 5, false),
            ("`ok", 3, false),
        ] {
            assert_eq!(
                ensure_template_quoted_token("`ok`", 0, token, end, '`').is_ok(),
                expected_ok,
                "quoted token {token:?} ending at {end}"
            );
        }

        for (token, end, expected_ok) in [
            ("(ok)", 4, true),
            ("(ok)", 0, false),
            ("(ok)", 5, false),
            ("ok)", 3, false),
            ("(ok", 3, false),
        ] {
            assert_eq!(
                ensure_template_parenthesized_token("(ok)", 0, token, end).is_ok(),
                expected_ok,
                "parenthesized token {token:?} ending at {end}"
            );
        }
    }

    #[test]
    fn template_parenthesized_tokens_ignore_parentheses_inside_strings() {
        assert_eq!(
            parse_template_parenthesized_token(r#"(printf "a)b") tail"#, 0).unwrap(),
            (r#"(printf "a)b")"#.to_string(), 14)
        );
        assert_eq!(
            parse_template_parenthesized_token(r#"(printf `c)d`) tail"#, 0).unwrap(),
            ("(printf `c)d`)".to_string(), 14)
        );
        assert_eq!(
            tokenize_template_command(r#"print (printf "a)b") (printf `c)d`)"#).unwrap(),
            vec![
                "print".to_string(),
                r#"(printf "a)b")"#.to_string(),
                "(printf `c)d`)".to_string(),
            ]
        );
    }

    #[test]
    fn template_quoted_tokens_advance_from_the_opening_quote() {
        assert_eq!(
            parse_template_quoted_token(r#"x "a\"b" tail"#, 2, '"').unwrap(),
            (r#""a\"b""#.to_string(), 8)
        );
        assert_eq!(
            parse_template_quoted_token("x `a b` tail", 2, '`').unwrap(),
            ("`a b`".to_string(), 7)
        );
        assert_eq!(
            tokenize_template_command(r#"print "a b" `c d`"#).unwrap(),
            vec![
                "print".to_string(),
                r#""a b""#.to_string(),
                "`c d`".to_string(),
            ]
        );
    }

    #[test]
    fn quoted_template_token_values_require_matching_wrappers() {
        for (name, input, expected) in [
            ("unquoted", "abc", None),
            ("single backtick", "`", None),
            ("single quote", "\"", None),
            ("empty backticks", "``", Some(String::new())),
            ("empty quotes", "\"\"", Some(String::new())),
            ("missing closing backtick", "`unterminated", None),
            ("missing opening backtick", "unterminated`", None),
            ("missing closing quote", "\"unterminated", None),
            ("missing opening quote", "unterminated\"", None),
        ] {
            assert_eq!(
                quoted_template_token_value(input).unwrap(),
                expected,
                "case {name}"
            );
        }
    }

    #[test]
    fn template_trim_right_allows_adjacent_literal() {
        let format = LineFormat::new(r#"{{ "ok" -}}tail"#).unwrap();
        assert_eq!(format.render("line", &BTreeMap::new()), "oktail");
    }

    #[test]
    fn template_helpers_tolerate_missing_arguments() {
        for (template, expected) in [
            ("{{ alignLeft 5 }}", ""),
            ("{{ alignRight 5 }}", ""),
            ("{{ replace \"a\" \"b\" }}", ""),
            ("{{ default \"fallback\" }}", "fallback"),
            ("{{ contains \"needle\" }}", "false"),
            ("{{ eq \"x\" }}", "false"),
            ("{{ ne \"x\" }}", "false"),
            ("{{ hasPrefix \"api\" }}", "false"),
            ("{{ hasSuffix \"api\" }}", "false"),
            ("{{ indent 2 }}", ""),
            ("{{ nindent 2 }}", ""),
            ("{{ repeat 3 }}", ""),
            ("{{ count \"o\" }}", ""),
            ("{{ regexReplaceAll \"o\" \"foo\" }}", ""),
            ("{{ regexReplaceAllLiteral \"o\" \"foo\" }}", ""),
            ("{{ trunc 3 }}", ""),
            ("{{ substr 1 3 }}", ""),
            ("{{ trimAll \"/\" }}", ""),
            ("{{ trimPrefix \"/\" }}", ""),
            ("{{ trimSuffix \"/\" }}", ""),
        ] {
            let format = LineFormat::new(template).unwrap();
            assert_eq!(
                format.render("raw", &BTreeMap::new()),
                expected,
                "template should tolerate missing args: {template}"
            );
        }
    }

    #[test]
    fn template_numeric_helpers_cover_missing_and_non_finite_inputs() {
        let one = vec!["9".to_string()];
        let two = vec!["9".to_string(), "4".to_string()];

        assert_eq!(
            format_template_integer_binary(&one, |left, right| Some(left - right)),
            ""
        );
        assert_eq!(
            format_template_integer_binary(&two, |left, right| Some(left - right)),
            "5"
        );
        assert_eq!(format_template_ordering(&one, Ordering::is_lt), "false");
        assert_eq!(format_template_ordering(&two, Ordering::is_gt), "true");

        assert_eq!(template_compare_values("NaN", "2"), Some(Ordering::Greater));
        assert_eq!(template_compare_values("1", "inf"), Some(Ordering::Less));
    }

    #[test]
    fn template_collection_helpers_index_and_slice_strings() {
        let plain = TemplateRuntimeValue::String("abc".to_string());
        let json_string = TemplateRuntimeValue::Json(serde_json::Value::String("xyz".to_string()));
        let scalar = TemplateRuntimeValue::Integer(7);

        for (value, expected) in [(&plain, true), (&json_string, true), (&scalar, false)] {
            assert_eq!(
                template_value_is_collection(value),
                expected,
                "collection check: {value:?}"
            );
        }

        assert_eq!(
            template_index_value(&plain, "1"),
            Some(TemplateRuntimeValue::Integer(i64::from(b'b')))
        );
        assert_eq!(
            template_index_value(&json_string, "1"),
            Some(TemplateRuntimeValue::Integer(i64::from(b'y')))
        );
        assert_eq!(template_index_value(&scalar, "0"), None);

        assert_eq!(
            evaluate_template_index(&[
                plain.clone(),
                TemplateRuntimeValue::String("2".to_string())
            ]),
            TemplateRuntimeValue::Integer(i64::from(b'c'))
        );
        assert_eq!(
            evaluate_template_index(&[
                json_string.clone(),
                TemplateRuntimeValue::String("0".to_string())
            ]),
            TemplateRuntimeValue::Integer(i64::from(b'x'))
        );
        assert_eq!(
            evaluate_template_slice(&[
                json_string,
                TemplateRuntimeValue::String("1".to_string()),
                TemplateRuntimeValue::String("3".to_string()),
            ]),
            TemplateRuntimeValue::String("yz".to_string())
        );
    }

    #[test]
    fn template_slice_bounds_validate_length_capacity_and_order() {
        let bounds = |values: &[usize]| {
            values
                .iter()
                .map(|value| TemplateRuntimeValue::String(value.to_string()))
                .collect::<Vec<_>>()
        };

        assert_eq!(template_slice_bounds(5, &bounds(&[0, 2, 5])), Some((0, 2)));
        assert_eq!(template_slice_bounds(5, &bounds(&[0, 2, 5, 5])), None);

        assert_eq!(template_slice_bounds(5, &bounds(&[0, 3, 3])), Some((0, 3)));
        assert_eq!(template_slice_bounds(5, &bounds(&[0, 5, 5])), Some((0, 5)));
        assert_eq!(template_slice_bounds(5, &bounds(&[0, 4, 3])), None);
        assert_eq!(template_slice_bounds(5, &bounds(&[0, 3, 6])), None);

        assert_eq!(template_slice_bounds(5, &bounds(&[4, 2])), None);
        assert_eq!(template_slice_bounds(5, &bounds(&[1, 6])), None);
    }

    #[test]
    fn template_float_and_bytes_formatting_preserves_edge_cases() {
        let args = |values: &[&str]| {
            values
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
        };

        assert_eq!(format_template_float(-0.0), "0");
        assert_eq!(format_template_float_round(&args(&["1"])), "");
        assert_eq!(format_template_float_round(&args(&["-1.6", "0"])), "-2");
        assert_eq!(
            format_template_float_round(&args(&["1.24", "1", "0.5"])),
            "1.2"
        );
        assert_eq!(
            format_template_float_round(&args(&["1.24", "1", "NaN"])),
            ""
        );
        assert_eq!(format_template_float_round(&args(&["1.2", "400"])), "");

        assert_eq!(format_template_bytes("1.5"), "1.5");
        assert_eq!(format_template_bytes("1kB"), "1000");
        assert_eq!(
            format_template_bytes("100000000000000000000"),
            "100000000000000000000"
        );
    }

    #[test]
    fn template_date_helpers_accept_extra_args_and_cover_layout_tokens() {
        let args = |values: &[&str]| {
            values
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
        };
        let timestamp_ns = "1704197045123456789";

        assert_eq!(
            format_template_date(&args(&[
                "2006-01-02T15:04:05.000000000Z07:00",
                timestamp_ns,
                "ignored",
            ])),
            "2024-01-02T12:04:05.123456789Z"
        );
        assert_eq!(
            format_template_to_date(&args(&[
                "2006-01-02T15:04:05.999999999 -07:00",
                "2024-01-02T12:04:05.123456789 +00:00",
                "ignored",
            ])),
            timestamp_ns
        );
        assert_eq!(
            format_template_to_date_in_zone(&args(&[
                "2006-01-02 15:04:05",
                "America/New_York",
                "2024-01-02 07:04:05",
                "ignored",
            ])),
            "1704197045000000000"
        );
        assert_eq!(format_template_date(&args(&["2006"])), "");
        assert_eq!(format_template_to_date(&args(&["2006"])), "");
        assert_eq!(format_template_to_date_in_zone(&args(&["2006", "UTC"])), "");
    }

    #[test]
    fn go_time_layout_helpers_format_and_parse_each_supported_token() {
        let timestamp = time::OffsetDateTime::from_unix_timestamp_nanos(1704197045123456789)
            .expect("test timestamp should be valid");

        assert_eq!(
            format_go_time_layout(
                "2006|06|15|04|05|01|1|02|2|Z07:00|-07:00|.000000000|.",
                timestamp,
            ),
            "2024|24|12|04|05|01|1|02|2|Z|+00:00|.123456789|."
        );

        let parsed = parse_go_time_layout_value(
            "06|1|2|15|04|05|.999999999|Z07:00|-07:00|.",
            "24|7|8|09|10|11|.123456789|+02:30|-03:15|.",
        )
        .expect("layout value should parse");
        assert_eq!(
            (
                parsed.year,
                parsed.month,
                parsed.day,
                parsed.hour,
                parsed.minute,
                parsed.second,
                parsed.nanosecond,
                parsed.offset_seconds,
            ),
            (2024, 7, 8, 9, 10, 11, 123_456_789, Some(-11_700))
        );
    }

    #[test]
    fn go_time_low_level_parsers_consume_expected_widths() {
        let mut pos = 0;
        assert_eq!(
            parse_variable_template_digits("123x", &mut pos, 2),
            Some(12)
        );
        assert_eq!(pos, 2);

        pos = 0;
        assert_eq!(parse_variable_template_digits("x12", &mut pos, 2), None);
        assert_eq!(pos, 0);

        pos = 0;
        assert_eq!(
            parse_template_fractional_nanoseconds(".1234x", &mut pos, 3),
            Some(123_000_000)
        );
        assert_eq!(pos, 4);

        pos = 0;
        assert_eq!(
            parse_template_fractional_nanoseconds(".x", &mut pos, 3),
            None
        );
        assert_eq!(pos, 1);

        pos = 0;
        assert_eq!(parse_template_timezone_offset("Z!", &mut pos), Some(0));
        assert_eq!(pos, 1);

        pos = 0;
        assert_eq!(
            parse_template_timezone_offset("+02:30", &mut pos),
            Some(9_000)
        );
        assert_eq!(pos, 6);

        pos = 0;
        assert_eq!(
            parse_template_timezone_offset("+12:34", &mut pos),
            Some(45_240)
        );
        assert_eq!(pos, 6);

        pos = 1;
        assert_eq!(
            parse_template_timezone_offset("x-03:15", &mut pos),
            Some(-11_700)
        );
        assert_eq!(pos, 7);

        pos = 0;
        assert_eq!(parse_template_timezone_offset("UTC", &mut pos), None);
        assert_eq!(pos, 0);
    }

    #[test]
    fn template_string_escape_helpers_cover_special_bytes() {
        assert_eq!(substring_template_string("abcdef", 2, 0), "");
        assert_eq!(substring_template_string("abcdef", 2, -1), "cdef");

        assert_eq!(
            js_escape_template_string("\\'\"\n\r\t\u{2028}\u{2029}\u{0001}"),
            r#"\\\'\"\u000A\u000D\u0009\u2028\u2029\u0001"#
        );

        let mut escaped = "prefix".to_string();
        push_template_unicode_escape(&mut escaped, 0x1f);
        assert_eq!(escaped, r"prefix\u001F");

        assert_eq!(urldecode_template_string("%7a%2F%3f%zz%A"), "z/?%zz%A");
    }
}
