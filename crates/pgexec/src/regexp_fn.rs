//! The scalar `regexp_*` function family: `regexp_replace`, `regexp_count`,
//! `regexp_instr`, `regexp_like`, `regexp_substr`, `regexp_match` and
//! `regexp_split_to_array`. `regexp_split_to_table` and `regexp_matches` are
//! set-returning and live in `srf`.
//!
//! ## Regular-expression dialect
//!
//! PostgreSQL implements POSIX Advanced Regular Expressions with Spencer's
//! engine. Crabka uses the `regex` crate's RE2-family engine. The two agree on
//! the operators SQL patterns actually use: literals, classes and POSIX class
//! names, alternation, grouping, the quantifiers and bounds, anchors, and the
//! `i`/`n`/`s`/`x`/`q` flags. They diverge on back-references, which are `\1`
//! inside a *pattern*, and on look-around. The `regex` crate rejects both at
//! compile time with 2201B and does not match them. PostgreSQL counts positions
//! and lengths in characters, not bytes, and crabka counts them the same way.

use crabka_pgparser::ast::{Expr, FuncCall};
use crabka_pgtypes::{ArrayValue, ColumnType, Datum, ElemType};
use regex::{Captures, Regex, RegexBuilder};

use crate::{
    clock::EvalCtx,
    error::ExecError,
    func::{checked_args, int_arg, require_arity, type_error, undefined_function},
    scope::Scope,
};

/// The scalar regular-expression functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RegexpFunc {
    Replace,
    Count,
    Instr,
    Like,
    Substr,
    Match,
    SplitToArray,
}

impl RegexpFunc {
    /// The name PostgreSQL prints in its "does not support the global option"
    /// message. This is the SQL spelling, with parentheses.
    fn sql_name(self) -> &'static str {
        match self {
            RegexpFunc::Replace => "regexp_replace()",
            RegexpFunc::Count => "regexp_count()",
            RegexpFunc::Instr => "regexp_instr()",
            RegexpFunc::Like => "regexp_like()",
            RegexpFunc::Substr => "regexp_substr()",
            RegexpFunc::Match => "regexp_match()",
            RegexpFunc::SplitToArray => "regexp_split_to_array()",
        }
    }

    /// Only `regexp_replace` reads `g`. Every other function here rejects it.
    fn allows_global(self) -> bool {
        self == RegexpFunc::Replace
    }
}

fn regexp_func(name: &str) -> Option<RegexpFunc> {
    Some(match name {
        "regexp_replace" => RegexpFunc::Replace,
        "regexp_count" => RegexpFunc::Count,
        "regexp_instr" => RegexpFunc::Instr,
        "regexp_like" => RegexpFunc::Like,
        "regexp_substr" => RegexpFunc::Substr,
        "regexp_match" => RegexpFunc::Match,
        "regexp_split_to_array" => RegexpFunc::SplitToArray,
        _ => return None,
    })
}

/// Is `name` one of this module's functions? `func::is_scalar` folds this in.
pub(crate) fn is_regexp_func(name: &str) -> bool {
    regexp_func(name).is_some()
}

/// Statically infer a regexp call's result type, and check name and arity.
pub(crate) fn regexp_func_result_type(
    fc: &FuncCall,
    _scope: &Scope,
) -> Result<ColumnType, ExecError> {
    let f = regexp_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let n = checked_args(fc)?.len();
    match f {
        RegexpFunc::Replace => {
            require_arity(fc, (3..=6).contains(&n))?;
            Ok(ColumnType::Text)
        }
        RegexpFunc::Count => {
            require_arity(fc, (2..=4).contains(&n))?;
            Ok(ColumnType::Int4)
        }
        RegexpFunc::Instr => {
            require_arity(fc, (2..=7).contains(&n))?;
            Ok(ColumnType::Int4)
        }
        RegexpFunc::Like => {
            require_arity(fc, n == 2 || n == 3)?;
            Ok(ColumnType::Bool)
        }
        RegexpFunc::Substr => {
            require_arity(fc, (2..=6).contains(&n))?;
            Ok(ColumnType::Text)
        }
        RegexpFunc::Match | RegexpFunc::SplitToArray => {
            require_arity(fc, n == 2 || n == 3)?;
            Ok(ColumnType::Array(ElemType::Text))
        }
    }
}

/// Evaluate a regexp call. Every function in the family is strict.
pub(crate) fn eval_regexp(
    fc: &FuncCall,
    _ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let f = regexp_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = checked_args(fc)?;
    let vals = args
        .iter()
        .map(&mut eval_child)
        .collect::<Result<Vec<_>, _>>()?;
    if vals.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    eval_strict(f, fc, &vals)
}

fn eval_strict(f: RegexpFunc, fc: &FuncCall, vals: &[Datum]) -> Result<Datum, ExecError> {
    let [source, pattern, rest @ ..] = vals else {
        return Err(undefined_function(&fc.name));
    };
    let source = text_arg(source)?;
    let pattern = text_arg(pattern)?;
    match f {
        RegexpFunc::Replace => {
            let (replacement, tail) = rest
                .split_first()
                .ok_or_else(|| undefined_function(&fc.name))?;
            replace(fc, source, pattern, text_arg(replacement)?, tail)
        }
        RegexpFunc::Like => {
            let flags = optional_flags(rest)?;
            let re = compile(f, pattern, flags)?;
            Ok(Datum::Bool(re.is_match(source)))
        }
        RegexpFunc::Match => {
            let flags = optional_flags(rest)?;
            let re = compile(f, pattern, flags)?;
            Ok(match re.captures(source) {
                None => Datum::Null,
                Some(caps) => {
                    Datum::Array(ArrayValue::new(ElemType::Text, group_datums(&re, &caps)))
                }
            })
        }
        RegexpFunc::SplitToArray => {
            let flags = optional_flags(rest)?;
            let re = compile(f, pattern, flags)?;
            let parts = split_pieces(&re, source)
                .into_iter()
                .map(Datum::Text)
                .collect();
            Ok(Datum::Array(ArrayValue::new(ElemType::Text, parts)))
        }
        RegexpFunc::Count => {
            let start = optional_position(rest.first(), "start")?.unwrap_or(1);
            let flags = rest.get(1).map(text_arg).transpose()?.unwrap_or("");
            let re = compile(f, pattern, flags)?;
            let tail = char_suffix(source, start);
            Ok(Datum::Int4(
                i32::try_from(re.find_iter(tail).count()).unwrap_or(i32::MAX),
            ))
        }
        RegexpFunc::Substr => substr(fc, source, pattern, rest),
        RegexpFunc::Instr => instr(fc, source, pattern, rest),
    }
}

// ---- individual functions ----

/// `regexp_replace(source, pattern, replacement [, start [, N]] [, flags])`.
/// A text argument in the fourth position is the flag string. An integer there
/// is the 1-based character position at which the search starts.
fn replace(
    fc: &FuncCall,
    source: &str,
    pattern: &str,
    replacement: &str,
    tail: &[Datum],
) -> Result<Datum, ExecError> {
    let (start, nth, flags) = match tail {
        [] => (1, None, ""),
        [Datum::Text(flags)] => (1, None, flags.as_str()),
        [start] => (position(start, "start")?, None, ""),
        [start, n] => (position(start, "start")?, Some(nth_arg(n)?), ""),
        [start, n, flags] => (
            position(start, "start")?,
            Some(nth_arg(n)?),
            text_arg(flags)?,
        ),
        _ => return Err(undefined_function(&fc.name)),
    };
    let global = flags.contains('g');
    let re = compile(RegexpFunc::Replace, pattern, flags)?;
    let (head, tail_text) = char_split(source, start);
    // `N = 0` replaces every match from `start`; `N = k` replaces only the k'th;
    // with neither, the `g` flag decides between all matches and just the first.
    let target: Option<usize> = match nth {
        Some(0) => None,
        Some(k) => Some(k),
        None if global => None,
        None => Some(1),
    };
    let mut out = String::with_capacity(source.len());
    out.push_str(head);
    let mut last = 0usize;
    for (seen, caps) in re.captures_iter(tail_text).enumerate() {
        let whole = caps.get(0).expect("group 0 always participates");
        if target.is_some_and(|k| k != seen + 1) {
            continue;
        }
        out.push_str(&tail_text[last..whole.start()]);
        expand_replacement(&mut out, replacement, &caps);
        last = whole.end();
        if target.is_some() {
            break;
        }
    }
    out.push_str(&tail_text[last..]);
    Ok(Datum::Text(out))
}

/// `regexp_substr(source, pattern [, start [, N [, flags [, subexpr]]]])`.
fn substr(fc: &FuncCall, source: &str, pattern: &str, tail: &[Datum]) -> Result<Datum, ExecError> {
    let start = optional_position(tail.first(), "start")?.unwrap_or(1);
    let nth = tail.get(1).map(nth_arg).transpose()?.unwrap_or(1);
    let flags = tail.get(2).map(text_arg).transpose()?.unwrap_or("");
    let subexpr = tail.get(3).map(subexpr_arg).transpose()?.unwrap_or(0);
    require_arity(fc, tail.len() <= 4)?;
    let re = compile(RegexpFunc::Substr, pattern, flags)?;
    let Some(caps) = nth_match(&re, char_suffix(source, start), nth) else {
        return Ok(Datum::Null);
    };
    Ok(match caps.get(subexpr) {
        None => Datum::Null,
        Some(m) => Datum::Text(m.as_str().to_string()),
    })
}

/// `regexp_instr(source, pattern [, start [, N [, endoption [, flags [, subexpr]]]]])`.
fn instr(fc: &FuncCall, source: &str, pattern: &str, tail: &[Datum]) -> Result<Datum, ExecError> {
    let start = optional_position(tail.first(), "start")?.unwrap_or(1);
    let nth = tail.get(1).map(nth_arg).transpose()?.unwrap_or(1);
    let endoption = match tail.get(2) {
        None => 0,
        Some(d) => match int_arg(d)? {
            v @ (0 | 1) => v,
            other => return Err(invalid_parameter("endoption", other)),
        },
    };
    let flags = tail.get(3).map(text_arg).transpose()?.unwrap_or("");
    let subexpr = tail.get(4).map(subexpr_arg).transpose()?.unwrap_or(0);
    require_arity(fc, tail.len() <= 5)?;
    let re = compile(RegexpFunc::Instr, pattern, flags)?;
    let suffix = char_suffix(source, start);
    let Some(caps) = nth_match(&re, suffix, nth) else {
        return Ok(Datum::Int4(0));
    };
    let Some(m) = caps.get(subexpr) else {
        return Ok(Datum::Int4(0));
    };
    let byte = if endoption == 0 { m.start() } else { m.end() };
    let offset = suffix[..byte].chars().count() as i64;
    Ok(Datum::Int4(
        i32::try_from(start + offset).unwrap_or(i32::MAX),
    ))
}

/// Split `input` on `re`, and apply PostgreSQL's zero-length-match rule.
///
/// The rule ignores an empty match at the start of the string, at its end, or
/// immediately after a previous match. `regexp_split_to_array('abc', '')` is
/// therefore `{a,b,c}` and not a run of empty strings.
fn split_pieces(re: &Regex, input: &str) -> Vec<String> {
    let mut pieces = Vec::new();
    let mut piece_start = 0usize;
    let mut search = 0usize;
    let mut previous_end: Option<usize> = None;
    while search <= input.len() {
        let Some(m) = re.find_at(input, search) else {
            break;
        };
        let (start, end) = (m.start(), m.end());
        if start == end {
            let ignored = start == 0 || start == input.len() || previous_end == Some(start);
            if !ignored {
                pieces.push(input[piece_start..start].to_string());
                piece_start = end;
                previous_end = Some(end);
            }
            let next = next_boundary(input, start);
            if next <= start {
                break;
            }
            search = next;
        } else {
            pieces.push(input[piece_start..start].to_string());
            piece_start = end;
            previous_end = Some(end);
            search = end;
        }
    }
    pieces.push(input[piece_start..].to_string());
    pieces
}

/// The next UTF-8 character boundary after `at`, or one past the end. The
/// zero-length-match scan therefore always terminates.
fn next_boundary(input: &str, at: usize) -> usize {
    input[at..]
        .chars()
        .next()
        .map_or(at + 1, |c| at + c.len_utf8())
}

/// The `nth` (1-based) match of `re` in `haystack`.
fn nth_match<'h>(re: &Regex, haystack: &'h str, nth: usize) -> Option<Captures<'h>> {
    re.captures_iter(haystack).nth(nth.saturating_sub(1))
}

/// Every capture group's text, as PostgreSQL's `text[]` result.
///
/// The result is the whole match when the pattern has no groups. If the pattern
/// has groups, the result has one element per group, and a non-participating
/// group is a NULL element.
fn group_datums(re: &Regex, caps: &Captures<'_>) -> Vec<Datum> {
    if re.captures_len() == 1 {
        return vec![Datum::Text(
            caps.get(0)
                .expect("group 0 always participates")
                .as_str()
                .to_string(),
        )];
    }
    (1..re.captures_len())
        .map(|i| match caps.get(i) {
            None => Datum::Null,
            Some(m) => Datum::Text(m.as_str().to_string()),
        })
        .collect()
}

/// Expand a PostgreSQL replacement string: `\1`…`\9` are capture groups, `\&`
/// is the whole match, `\\` is a literal backslash, and any other escaped
/// character is itself.
fn expand_replacement(out: &mut String, replacement: &str, caps: &Captures<'_>) {
    let mut chars = replacement.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            None => out.push('\\'),
            Some('&') => out.push_str(caps.get(0).map_or("", |m| m.as_str())),
            Some(d) if d.is_ascii_digit() => {
                let group = d as usize - '0' as usize;
                out.push_str(caps.get(group).map_or("", |m| m.as_str()));
            }
            Some(other) => out.push(other),
        }
    }
}

// ---- argument helpers ----

fn text_arg(d: &Datum) -> Result<&str, ExecError> {
    match d {
        Datum::Text(s) => Ok(s),
        other => Err(type_error("function", other)),
    }
}

/// A trailing optional flag string, for the two-or-three-argument functions.
fn optional_flags(rest: &[Datum]) -> Result<&str, ExecError> {
    match rest {
        [] => Ok(""),
        [flags] => text_arg(flags),
        _ => Ok(""),
    }
}

fn invalid_parameter(name: &str, value: i64) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "22023",
        message: format!("invalid value for parameter \"{name}\": {value}"),
    }
}

/// A 1-based character position argument. `0` and below are 22023.
fn position(d: &Datum, name: &str) -> Result<i64, ExecError> {
    let v = int_arg(d)?;
    if v < 1 {
        return Err(invalid_parameter(name, v));
    }
    Ok(v)
}

fn optional_position(d: Option<&Datum>, name: &str) -> Result<Option<i64>, ExecError> {
    match d {
        // The optional argument slot may hold the flag string instead.
        None | Some(Datum::Text(_)) => Ok(None),
        Some(d) => position(d, name).map(Some),
    }
}

/// The `N` argument, which selects the match. It is 1-based.
///
/// `0` means "every match" for `regexp_replace` only. The other callers reject
/// `0` through [`position`].
fn nth_arg(d: &Datum) -> Result<usize, ExecError> {
    let v = int_arg(d)?;
    if v < 0 {
        return Err(invalid_parameter("n", v));
    }
    usize::try_from(v).map_err(|_| invalid_parameter("n", v))
}

fn subexpr_arg(d: &Datum) -> Result<usize, ExecError> {
    let v = int_arg(d)?;
    if v < 0 {
        return Err(invalid_parameter("subexpr", v));
    }
    usize::try_from(v).map_err(|_| invalid_parameter("subexpr", v))
}

/// Split `s` at 1-based character position `start`, then return the untouched
/// prefix and the region the match applies to.
fn char_split(s: &str, start: i64) -> (&str, &str) {
    let skip = usize::try_from(start - 1).unwrap_or(0);
    let at = s.char_indices().nth(skip).map_or(s.len(), |(byte, _)| byte);
    s.split_at(at)
}

fn char_suffix(s: &str, start: i64) -> &str {
    char_split(s, start).1
}

// ---- pattern compilation ----

/// Compile a PostgreSQL pattern with its flag string.
///
/// PostgreSQL's default is "non-newline-sensitive": `.` matches a newline and
/// `^`/`$` anchor only at the ends of the string. `n`/`m`/`p`/`w` make that
/// newline-sensitive, `s`/`e`/`b`/`t` restore the default, `i`/`c` set case
/// folding, `x` enables expanded syntax and `q` makes the pattern a literal.
fn compile(f: RegexpFunc, pattern: &str, flags: &str) -> Result<Regex, ExecError> {
    let mut case_insensitive = false;
    let mut newline_sensitive = false;
    let mut expanded = false;
    let mut literal = false;
    for flag in flags.chars() {
        match flag {
            'i' => case_insensitive = true,
            'c' => case_insensitive = false,
            'n' | 'm' | 'p' | 'w' => newline_sensitive = true,
            's' | 'e' | 'b' | 't' => newline_sensitive = false,
            'x' => expanded = true,
            'q' => literal = true,
            'g' if f.allows_global() => {}
            'g' => {
                return Err(ExecError::FunctionError {
                    sqlstate: "22023",
                    message: format!("{} does not support the \"global\" option", f.sql_name()),
                });
            }
            other => {
                return Err(ExecError::FunctionError {
                    sqlstate: "22023",
                    message: format!("invalid regular expression option: \"{other}\""),
                });
            }
        }
    }
    let source = if literal {
        regex::escape(pattern)
    } else {
        pattern.to_string()
    };
    RegexBuilder::new(&source)
        .case_insensitive(case_insensitive)
        .multi_line(newline_sensitive)
        .dot_matches_new_line(!newline_sensitive)
        .ignore_whitespace(expanded)
        .build()
        .map_err(|error| ExecError::FunctionError {
            sqlstate: "2201B",
            message: format!("invalid regular expression: {error}"),
        })
}

#[cfg(test)]
mod tests;
