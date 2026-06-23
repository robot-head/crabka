//! `PromQL` parser/planner entry points.

pub mod aggregate;
pub mod label_ops;
pub mod leaf;
pub mod over_time_range;
pub mod rate_range;
pub mod scalar_math;

use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use promql_parser::parser::ast::ExtensionExpr;
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{Call, Function, FunctionArgs};
use promql_parser::parser::{Expr, Extension, parse};
use promql_parser::util::display_duration;

use crate::PromqlError;
use crate::error::Result;

/// Query-range values available to Prometheus duration expressions.
#[allow(clippy::struct_field_names)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurationExprContext {
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
}

impl DurationExprContext {
    #[must_use]
    pub fn instant(time_ms: i64) -> Self {
        Self {
            start_ms: time_ms,
            end_ms: time_ms,
            step_ms: 0,
        }
    }

    #[must_use]
    pub fn range(start_ms: i64, end_ms: i64, step_ms: i64) -> Self {
        Self {
            start_ms,
            end_ms,
            step_ms,
        }
    }
}

/// Parse a `PromQL` expression into the upstream parser AST.
///
/// # Errors
///
/// Returns [`PromqlError::Parse`] when the upstream parser rejects the query.
pub fn parse_promql(query: &str) -> Result<Expr> {
    parse_promql_with_duration_context(query, DurationExprContext::instant(0))
}

/// Parse `PromQL`, first folding Prometheus duration expressions to fixed durations.
///
/// The parser crate stores selector ranges, subquery resolutions, and offsets as
/// concrete [`Duration`] values. Prometheus 3.x accepts scalar expressions in
/// those positions, so Crabka normalizes them before handing the query to the
/// parser.
///
/// # Errors
///
/// Returns [`PromqlError::Parse`] when normalization or the upstream parser
/// rejects the query.
pub fn parse_promql_with_duration_context(
    query: &str,
    context: DurationExprContext,
) -> Result<Expr> {
    let (query, selector_modifier) = strip_extended_selector_modifiers(query)?;
    let normalized = normalize_duration_expressions(&query, context)?;
    match parse(&normalized) {
        Ok(expr) => Ok(selector_modifier.map_or(expr.clone(), |modifier| {
            wrap_extended_selectors(expr, modifier)
        })),
        Err(error) => parse_experimental_zero_arg_helper(&query).ok_or(PromqlError::Parse(error)),
    }
}

/// Prometheus 3.x range/vector selector modifier accepted after selectors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExtendedSelectorModifier {
    Anchored,
    Smoothed,
}

impl ExtendedSelectorModifier {
    #[must_use]
    pub fn keyword(self) -> &'static str {
        match self {
            Self::Anchored => "anchored",
            Self::Smoothed => "smoothed",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExtendedSelectorExpr {
    modifier: ExtendedSelectorModifier,
    children: Vec<Expr>,
}

impl ExtendedSelectorExpr {
    #[must_use]
    pub fn modifier(&self) -> ExtendedSelectorModifier {
        self.modifier
    }

    #[must_use]
    pub fn child(&self) -> Option<&Expr> {
        self.children.first()
    }
}

impl ExtensionExpr for ExtendedSelectorExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.modifier.keyword()
    }

    fn value_type(&self) -> ValueType {
        self.child().map_or(ValueType::Vector, Expr::value_type)
    }

    fn children(&self) -> &[Expr] {
        &self.children
    }

    fn with_new_children(&self, children: Vec<Expr>) -> Arc<dyn ExtensionExpr> {
        Arc::new(Self {
            modifier: self.modifier,
            children,
        })
    }
}

fn wrap_extended_selectors(expr: Expr, modifier: ExtendedSelectorModifier) -> Expr {
    match expr {
        Expr::MatrixSelector(_) | Expr::VectorSelector(_) => Expr::Extension(Extension {
            expr: Arc::new(ExtendedSelectorExpr {
                modifier,
                children: vec![expr],
            }),
        }),
        Expr::Call(mut call) => {
            call.args.args = call
                .args
                .args
                .into_iter()
                .map(|arg| Box::new(wrap_extended_selectors(*arg, modifier)))
                .collect();
            Expr::Call(call)
        }
        Expr::Aggregate(mut aggregate) => {
            aggregate.expr = Box::new(wrap_extended_selectors(*aggregate.expr, modifier));
            Expr::Aggregate(aggregate)
        }
        Expr::Unary(mut unary) => {
            unary.expr = Box::new(wrap_extended_selectors(*unary.expr, modifier));
            Expr::Unary(unary)
        }
        Expr::Binary(mut binary) => {
            binary.lhs = Box::new(wrap_extended_selectors(*binary.lhs, modifier));
            binary.rhs = Box::new(wrap_extended_selectors(*binary.rhs, modifier));
            Expr::Binary(binary)
        }
        Expr::Paren(mut paren) => {
            paren.expr = Box::new(wrap_extended_selectors(*paren.expr, modifier));
            Expr::Paren(paren)
        }
        Expr::Subquery(mut subquery) => {
            subquery.expr = Box::new(wrap_extended_selectors(*subquery.expr, modifier));
            Expr::Subquery(subquery)
        }
        other => other,
    }
}

fn strip_extended_selector_modifiers(
    query: &str,
) -> Result<(String, Option<ExtendedSelectorModifier>)> {
    let chars = query.chars().collect::<Vec<_>>();
    let mut out = String::with_capacity(query.len());
    let mut index = 0;
    let mut quote = None;
    let mut modifier = None;

    while index < chars.len() {
        let ch = chars[index];
        if let Some(quote_ch) = quote {
            out.push(ch);
            if ch == '\\' {
                if let Some(next) = chars.get(index + 1) {
                    out.push(*next);
                    index += 2;
                    continue;
                }
            } else if ch == quote_ch {
                quote = None;
            }
            index += 1;
            continue;
        }

        if ch == '"' || ch == '\'' || ch == '`' {
            quote = Some(ch);
            out.push(ch);
            index += 1;
            continue;
        }

        if let Some((found, end)) = extended_modifier_at(&chars, index) {
            if let Some(previous) = modifier
                && previous != found
            {
                return Err(PromqlError::Parse(
                    "cannot mix anchored and smoothed selector modifiers".to_string(),
                ));
            }
            modifier = Some(found);
            while out.ends_with(char::is_whitespace) {
                out.pop();
            }
            index = end;
            while chars.get(index).is_some_and(|ch| ch.is_whitespace()) {
                index += 1;
            }
            if chars.get(index).is_some_and(|ch| *ch != ')' && *ch != ',') {
                out.push(' ');
            }
            continue;
        }

        out.push(ch);
        index += 1;
    }

    Ok((out, modifier))
}

fn extended_modifier_at(chars: &[char], index: usize) -> Option<(ExtendedSelectorModifier, usize)> {
    for (keyword, modifier) in [
        ("anchored", ExtendedSelectorModifier::Anchored),
        ("smoothed", ExtendedSelectorModifier::Smoothed),
    ] {
        let end = index.checked_add(keyword.len())?;
        if end > chars.len() {
            continue;
        }
        if chars[index..end].iter().collect::<String>() != keyword {
            continue;
        }
        let before_ok = index == 0 || !is_ident_char(chars[index - 1]);
        let after_ok = chars.get(end).is_none_or(|ch| !is_ident_char(*ch));
        if before_ok && after_ok {
            return Some((modifier, end));
        }
    }
    None
}

fn parse_experimental_zero_arg_helper(query: &str) -> Option<Expr> {
    let name = match query.trim() {
        "start()" => "start",
        "end()" => "end",
        _ => return None,
    };

    Some(Expr::Call(Call {
        func: Function::new(name, vec![], 0, ValueType::Scalar, true),
        args: FunctionArgs::empty_args(),
    }))
}

fn normalize_duration_expressions(query: &str, context: DurationExprContext) -> Result<String> {
    let chars = query.chars().collect::<Vec<_>>();
    let mut out = String::with_capacity(query.len());
    let mut index = 0;
    let mut quote = None;

    while index < chars.len() {
        let ch = chars[index];
        if let Some(quote_ch) = quote {
            out.push(ch);
            if ch == '\\' {
                if let Some(next) = chars.get(index + 1) {
                    out.push(*next);
                    index += 2;
                    continue;
                }
            } else if ch == quote_ch {
                quote = None;
            }
            index += 1;
            continue;
        }

        if ch == '"' || ch == '\'' || ch == '`' {
            quote = Some(ch);
            out.push(ch);
            index += 1;
            continue;
        }

        if ch == '[' {
            let end = matching_delimiter(&chars, index, '[', ']')?;
            let bracket_content = chars[index + 1..end].iter().collect::<String>();
            out.push('[');
            out.push_str(&normalize_range_duration_content(
                &bracket_content,
                context,
            )?);
            out.push(']');
            index = end + 1;
            continue;
        }

        if starts_offset_keyword(&chars, index) {
            let after_keyword = index + "offset".len();
            if let Some((operand, end)) = offset_operand(&chars, after_keyword) {
                let seconds = DurationExprParser::new(&operand, context).parse()?;
                if !is_zero(seconds) {
                    out.push_str(&chars[index..after_keyword].iter().collect::<String>());
                    out.push(' ');
                    if seconds < 0.0 {
                        out.push('-');
                        out.push_str(&seconds_to_duration_literal(-seconds)?);
                    } else {
                        out.push_str(&seconds_to_duration_literal(seconds)?);
                    }
                }
                index = end;
                continue;
            }
        }

        out.push(ch);
        index += 1;
    }

    Ok(out)
}

fn normalize_range_duration_content(
    content: &str,
    duration_ctx: DurationExprContext,
) -> Result<String> {
    let Some(colon) = top_level_colon(content)? else {
        return seconds_to_duration_literal(
            DurationExprParser::new(content, duration_ctx).parse()?,
        );
    };
    let range = content[..colon].trim();
    let step = content[colon + 1..].trim();
    let range = seconds_to_duration_literal(DurationExprParser::new(range, duration_ctx).parse()?)?;
    if step.is_empty() {
        Ok(format!("{range}:"))
    } else {
        Ok(format!(
            "{range}:{}",
            seconds_to_duration_literal(DurationExprParser::new(step, duration_ctx).parse()?)?
        ))
    }
}

fn top_level_colon(content: &str) -> Result<Option<usize>> {
    let chars = content.chars().collect::<Vec<_>>();
    let mut parens = 0_i32;
    let mut quote = None;
    for (index, ch) in chars.iter().enumerate() {
        if let Some(quote_ch) = quote {
            if *ch == quote_ch {
                quote = None;
            }
            continue;
        }
        match *ch {
            '"' | '\'' | '`' => quote = Some(*ch),
            '(' => parens += 1,
            ')' => parens -= 1,
            ':' if parens == 0 => return Ok(Some(index)),
            _ => {}
        }
        if parens < 0 {
            return Err(PromqlError::Parse(format!(
                "unbalanced duration expression `{content}`"
            )));
        }
    }
    Ok(None)
}

fn matching_delimiter(chars: &[char], start: usize, open: char, close: char) -> Result<usize> {
    let mut depth = 0_i32;
    let mut quote = None;
    let mut index = start;
    while index < chars.len() {
        let ch = chars[index];
        if let Some(quote_ch) = quote {
            if ch == '\\' {
                index += 2;
                continue;
            }
            if ch == quote_ch {
                quote = None;
            }
            index += 1;
            continue;
        }
        match ch {
            '"' | '\'' | '`' => quote = Some(ch),
            _ if ch == open => depth += 1,
            _ if ch == close => {
                depth -= 1;
                if depth == 0 {
                    return Ok(index);
                }
            }
            _ => {}
        }
        index += 1;
    }
    Err(PromqlError::Parse(format!("unclosed `{open}` in query")))
}

fn starts_offset_keyword(chars: &[char], index: usize) -> bool {
    const OFFSET: &str = "offset";
    if index + OFFSET.len() > chars.len() {
        return false;
    }
    let word = chars[index..index + OFFSET.len()]
        .iter()
        .collect::<String>();
    if !word.eq_ignore_ascii_case(OFFSET) {
        return false;
    }
    let before_ok = index == 0 || !is_ident_char(chars[index - 1]);
    let after = index + OFFSET.len();
    let after_ok = after == chars.len() || !is_ident_char(chars[after]);
    before_ok && after_ok
}

fn offset_operand(chars: &[char], after_keyword: usize) -> Option<(String, usize)> {
    let mut start = skip_ws(chars, after_keyword);
    if start >= chars.len() {
        return None;
    }

    let mut sign_start = None;
    if chars[start] == '+' || chars[start] == '-' {
        sign_start = Some(start);
        start = skip_ws(chars, start + 1);
    }
    if start >= chars.len() {
        return None;
    }

    if chars[start] == '(' {
        let end = matching_delimiter(chars, start, '(', ')').ok()? + 1;
        let mut operand = chars[start..end].iter().collect::<String>();
        if let Some(sign_start) = sign_start {
            operand.insert(0, chars[sign_start]);
        }
        return Some((operand, end));
    }

    let end = if is_ident_start(chars[start]) {
        let ident_end = consume_ident(chars, start);
        let call_start = skip_ws(chars, ident_end);
        if chars.get(call_start) == Some(&'(') {
            matching_delimiter(chars, call_start, '(', ')').ok()? + 1
        } else {
            ident_end
        }
    } else if chars[start].is_ascii_digit() || chars[start] == '.' {
        consume_number_duration(chars, start)
    } else {
        return None;
    };

    let operand_start = sign_start.unwrap_or(start);
    Some((chars[operand_start..end].iter().collect::<String>(), end))
}

fn skip_ws(chars: &[char], mut index: usize) -> usize {
    while chars.get(index).is_some_and(|ch| ch.is_whitespace()) {
        index += 1;
    }
    index
}

fn consume_ident(chars: &[char], mut index: usize) -> usize {
    while chars.get(index).is_some_and(|ch| is_ident_char(*ch)) {
        index += 1;
    }
    index
}

fn consume_number_duration(chars: &[char], mut index: usize) -> usize {
    while index < chars.len() && (chars[index].is_ascii_digit() || chars[index] == '.') {
        index += 1;
    }
    while index < chars.len() && chars[index].is_ascii_alphabetic() {
        index += 1;
        while index < chars.len() && (chars[index].is_ascii_digit() || chars[index] == '.') {
            index += 1;
        }
    }
    index
}

fn is_ident_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_ident_char(ch: char) -> bool {
    is_ident_start(ch) || ch.is_ascii_digit() || ch == ':'
}

/// Largest duration the engine represents: `i64::MAX` milliseconds expressed in
/// seconds. `Duration::from_secs_f64` panics for finite values beyond `u64`
/// seconds (~1.8e19), so we reject anything past the engine ceiling first.
#[allow(clippy::cast_precision_loss)]
const MAX_DURATION_SECONDS: f64 = (i64::MAX as f64) / 1000.0;

fn seconds_to_duration_literal(seconds: f64) -> Result<String> {
    if !seconds.is_finite() || !(0.0..=MAX_DURATION_SECONDS).contains(&seconds) {
        return Err(PromqlError::Parse(format!(
            "duration expression evaluated to invalid duration `{seconds}`"
        )));
    }
    let duration = Duration::from_secs_f64(seconds);
    Ok(display_duration(&duration))
}

fn is_zero(value: f64) -> bool {
    value.abs() <= f64::EPSILON
}

struct DurationExprParser<'a> {
    chars: Vec<char>,
    index: usize,
    src: &'a str,
    context: DurationExprContext,
}

impl<'a> DurationExprParser<'a> {
    fn new(src: &'a str, context: DurationExprContext) -> Self {
        Self {
            chars: src.chars().collect(),
            index: 0,
            src,
            context,
        }
    }

    fn parse(mut self) -> Result<f64> {
        let value = self.parse_add_sub()?;
        self.skip_ws();
        if self.index != self.chars.len() {
            return Err(PromqlError::Parse(format!(
                "unexpected duration expression input `{}` in `{}`",
                self.chars[self.index], self.src
            )));
        }
        Ok(value)
    }

    fn parse_add_sub(&mut self) -> Result<f64> {
        let mut value = self.parse_mul_div_mod()?;
        loop {
            self.skip_ws();
            if self.eat('+') {
                value += self.parse_mul_div_mod()?;
            } else if self.eat('-') {
                value -= self.parse_mul_div_mod()?;
            } else {
                return Ok(value);
            }
        }
    }

    fn parse_mul_div_mod(&mut self) -> Result<f64> {
        let mut value = self.parse_unary()?;
        loop {
            self.skip_ws();
            if self.eat('*') {
                value *= self.parse_unary()?;
            } else if self.eat('/') {
                value /= self.parse_unary()?;
            } else if self.eat('%') {
                value %= self.parse_unary()?;
            } else {
                return Ok(value);
            }
        }
    }

    fn parse_unary(&mut self) -> Result<f64> {
        self.skip_ws();
        if self.eat('+') {
            return self.parse_unary();
        }
        if self.eat('-') {
            return Ok(-self.parse_power()?);
        }
        self.parse_power()
    }

    fn parse_power(&mut self) -> Result<f64> {
        let base = self.parse_primary()?;
        self.skip_ws();
        if self.eat('^') {
            Ok(base.powf(self.parse_unary()?))
        } else {
            Ok(base)
        }
    }

    fn parse_primary(&mut self) -> Result<f64> {
        self.skip_ws();
        if self.eat('(') {
            let value = self.parse_add_sub()?;
            self.skip_ws();
            if !self.eat(')') {
                return Err(PromqlError::Parse(format!(
                    "unclosed duration expression `{}`",
                    self.src
                )));
            }
            return Ok(value);
        }
        if self.peek().is_some_and(is_ident_start) {
            return self.parse_call();
        }
        if self
            .peek()
            .is_some_and(|ch| ch.is_ascii_digit() || ch == '.')
        {
            return self.parse_number_or_duration();
        }
        Err(PromqlError::Parse(format!(
            "expected duration expression in `{}`",
            self.src
        )))
    }

    fn parse_call(&mut self) -> Result<f64> {
        let start = self.index;
        self.index = consume_ident(&self.chars, self.index);
        let name = self.chars[start..self.index].iter().collect::<String>();
        self.skip_ws();
        if !self.eat('(') {
            return Err(PromqlError::Parse(format!(
                "expected function call in duration expression `{}`",
                self.src
            )));
        }
        let mut args = Vec::new();
        self.skip_ws();
        if !self.eat(')') {
            loop {
                args.push(self.parse_add_sub()?);
                self.skip_ws();
                if self.eat(')') {
                    break;
                }
                if !self.eat(',') {
                    return Err(PromqlError::Parse(format!(
                        "expected `,` or `)` in duration expression `{}`",
                        self.src
                    )));
                }
            }
        }

        match name.to_ascii_lowercase().as_str() {
            "step" if args.is_empty() => Ok(ms_to_seconds(self.context.step_ms)),
            "range" if args.is_empty() => Ok(ms_to_seconds(
                self.context.end_ms.saturating_sub(self.context.start_ms),
            )),
            "start" if args.is_empty() => Ok(ms_to_seconds(self.context.start_ms)),
            "end" if args.is_empty() => Ok(ms_to_seconds(self.context.end_ms)),
            "min" if !args.is_empty() => Ok(args.into_iter().fold(f64::INFINITY, f64::min)),
            "max" if !args.is_empty() => Ok(args.into_iter().fold(f64::NEG_INFINITY, f64::max)),
            _ => Err(PromqlError::Parse(format!(
                "unsupported duration expression function `{name}`"
            ))),
        }
    }

    fn parse_number_or_duration(&mut self) -> Result<f64> {
        let start = self.index;
        let mut total = 0.0;
        let mut saw_unit = false;

        loop {
            let number_start = self.index;
            while self
                .peek()
                .is_some_and(|ch| ch.is_ascii_digit() || ch == '.')
            {
                self.index += 1;
            }
            if number_start == self.index {
                break;
            }
            let number = self.chars[number_start..self.index]
                .iter()
                .collect::<String>()
                .parse::<f64>()
                .map_err(|error| {
                    PromqlError::Parse(format!(
                        "invalid duration expression number `{}`: {error}",
                        self.chars[number_start..self.index]
                            .iter()
                            .collect::<String>()
                    ))
                })?;
            let unit_start = self.index;
            while self.peek().is_some_and(|ch| ch.is_ascii_alphabetic()) {
                self.index += 1;
            }
            if unit_start == self.index {
                if saw_unit {
                    self.index = number_start;
                    break;
                }
                return Ok(number);
            }
            saw_unit = true;
            total += number
                * duration_unit_seconds(
                    &self.chars[unit_start..self.index]
                        .iter()
                        .collect::<String>(),
                )?;
        }

        if saw_unit {
            Ok(total)
        } else {
            Err(PromqlError::Parse(format!(
                "expected number or duration in `{}`",
                &self.src[start..]
            )))
        }
    }

    fn skip_ws(&mut self) {
        self.index = skip_ws(&self.chars, self.index);
    }

    fn eat(&mut self, ch: char) -> bool {
        if self.peek() == Some(ch) {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.index).copied()
    }
}

#[allow(clippy::cast_precision_loss)]
fn ms_to_seconds(ms: i64) -> f64 {
    ms as f64 / 1000.0
}

fn duration_unit_seconds(unit: &str) -> Result<f64> {
    match unit {
        "ms" => Ok(0.001),
        "s" => Ok(1.0),
        "m" => Ok(60.0),
        "h" => Ok(60.0 * 60.0),
        "d" => Ok(60.0 * 60.0 * 24.0),
        "w" => Ok(60.0 * 60.0 * 24.0 * 7.0),
        "y" => Ok(60.0 * 60.0 * 24.0 * 365.0),
        _ => Err(PromqlError::Parse(format!(
            "invalid duration expression unit `{unit}`"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use promql_parser::parser::Expr;

    use crate::PromqlError;

    use super::*;

    #[test]
    fn parse_promql_wraps_parser_success() {
        let expr = parse_promql("up").unwrap();

        assert!(matches!(expr, Expr::VectorSelector(_)));
    }

    #[test]
    fn parse_promql_maps_parser_errors() {
        let err = parse_promql("up {{{").unwrap_err();

        assert!(matches!(err, PromqlError::Parse(_)));
    }

    #[test]
    fn parse_promql_folds_range_duration_expressions() {
        let expr = parse_promql_with_duration_context(
            "metric[step()+1ms]",
            DurationExprContext::range(50_000, 60_000, 5_000),
        )
        .unwrap();

        assert!(expr.to_string() == "metric[5s1ms]");
    }

    #[test]
    fn parse_promql_folds_parenthesized_offset_expression() {
        let expr = parse_promql_with_duration_context(
            "metric offset (-2 * 2)",
            DurationExprContext::instant(1_000_000),
        )
        .unwrap();

        assert!(expr.to_string() == "metric offset -4s");
    }

    #[test]
    fn parse_promql_rejects_huge_finite_duration_expression() {
        // `10s ^ 22` folds to a finite ~1e22 seconds, which overflows the
        // `Duration::from_secs_f64` representable range. It must surface a
        // parse error rather than panicking.
        let err = parse_promql_with_duration_context(
            "metric[10s ^ 22]",
            DurationExprContext::instant(1_000_000),
        )
        .unwrap_err();

        assert!(matches!(err, PromqlError::Parse(_)));
    }

    #[test]
    fn parse_promql_preserves_unparenthesized_offset_precedence() {
        let expr = parse_promql_with_duration_context(
            "metric offset step()*0",
            DurationExprContext::range(50_000, 60_000, 5_000),
        )
        .unwrap();

        assert!(expr.to_string() == "metric offset 5s * 0");
    }
}
