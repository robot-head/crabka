//! Recursive-descent `TraceQL` parser.

use crate::ast::{
    Aggregate, ComparisonOp, Field, FieldExpr, Intrinsic, Pipeline, Query, QueryHints, Scope,
    SpansetExpr, StructuralOp, Value, WithBinding,
};
use crate::error::{Result, TraceqlError};
use crate::lexer::{Token, lex};

pub fn parse(query: &str) -> Result<Query> {
    Parser {
        tokens: lex(query)?,
        pos: 0,
    }
    .parse_query()
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn parse_query(&mut self) -> Result<Query> {
        let root = self.parse_spanset_or()?;
        let pipeline = self.parse_pipeline()?;
        let hints = self.parse_query_hints()?;
        self.expect(&Token::Eof)?;
        Ok(Query {
            root,
            pipeline,
            hints,
        })
    }

    fn parse_query_hints(&mut self) -> Result<QueryHints> {
        if !matches!(self.peek(), Token::Ident(name) if name == "with") {
            return Ok(QueryHints::default());
        }
        self.pos += 1;
        self.expect(&Token::LParen)?;
        let mut hints = QueryHints::default();
        loop {
            let name = self.expect_ident()?;
            self.expect(&Token::Eq)?;
            let value = match self.advance() {
                Token::Bool(value) => value,
                other => {
                    return Err(Self::err(format!(
                        "expected boolean query hint value, got {other:?}"
                    )));
                }
            };
            match name.as_str() {
                "most_recent" => hints.most_recent = value,
                "exemplars" => hints.exemplars = Some(value),
                other => return Err(Self::err(format!("unsupported query hint {other:?}"))),
            }
            if !self.eat(&Token::Comma) {
                break;
            }
        }
        self.expect(&Token::RParen)?;
        Ok(hints)
    }

    fn parse_pipeline(&mut self) -> Result<Vec<Pipeline>> {
        let mut out = Vec::new();
        while self.eat(&Token::Pipe) {
            out.push(self.parse_pipeline_stage()?);
            if let Some(by) = self.parse_adjacent_by()? {
                out.push(by);
            }
            if let Some((op, value)) = self.parse_numeric_filter()? {
                out.push(Pipeline::Filter { op, value });
            }
        }
        Ok(out)
    }

    fn parse_pipeline_stage(&mut self) -> Result<Pipeline> {
        let name = self.expect_ident()?;
        match name.as_str() {
            "count" => {
                self.expect(&Token::LParen)?;
                self.expect(&Token::RParen)?;
                Ok(Pipeline::Aggregate(Aggregate::Count))
            }
            "rate" => {
                self.expect(&Token::LParen)?;
                self.expect(&Token::RParen)?;
                Ok(Pipeline::Aggregate(Aggregate::Rate))
            }
            "count_over_time" => {
                self.expect(&Token::LParen)?;
                self.expect(&Token::RParen)?;
                Ok(Pipeline::Aggregate(Aggregate::CountOverTime))
            }
            "sum_over_time" | "avg_over_time" | "min_over_time" | "max_over_time" => {
                self.parse_field_over_time(&name)
            }
            "histogram_over_time" => self.parse_histogram_over_time(),
            "quantile_over_time" => {
                self.expect(&Token::LParen)?;
                let field = self.parse_field()?;
                let mut quantiles = Vec::new();
                while self.eat(&Token::Comma) {
                    quantiles.push(self.parse_quantile()?);
                }
                self.expect(&Token::RParen)?;
                Ok(Pipeline::Aggregate(Aggregate::QuantileOverTime {
                    field,
                    quantiles,
                }))
            }
            "sum" | "avg" | "max" | "min" => {
                self.expect(&Token::LParen)?;
                let field = self.parse_field()?;
                self.expect(&Token::RParen)?;
                let agg = match name.as_str() {
                    "sum" => Aggregate::Sum(field),
                    "avg" => Aggregate::Avg(field),
                    "max" => Aggregate::Max(field),
                    _ => Aggregate::Min(field),
                };
                Ok(Pipeline::Aggregate(agg))
            }
            "by" => {
                self.expect(&Token::LParen)?;
                let fields = self.parse_field_list()?;
                self.expect(&Token::RParen)?;
                Ok(Pipeline::By(fields))
            }
            "topk" => {
                self.expect(&Token::LParen)?;
                let k = self.parse_rank_limit()?;
                self.expect(&Token::RParen)?;
                Ok(Pipeline::TopK(k))
            }
            "bottomk" => {
                self.expect(&Token::LParen)?;
                let k = self.parse_rank_limit()?;
                self.expect(&Token::RParen)?;
                Ok(Pipeline::BottomK(k))
            }
            "compare" => {
                self.expect(&Token::LParen)?;
                self.expect(&Token::RParen)?;
                Ok(Pipeline::Compare)
            }
            "coalesce" => {
                self.expect(&Token::LParen)?;
                self.expect(&Token::RParen)?;
                Ok(Pipeline::Coalesce)
            }
            "select" => {
                self.expect(&Token::LParen)?;
                let fields = self.parse_field_list()?;
                self.expect(&Token::RParen)?;
                Ok(Pipeline::Select(fields))
            }
            "with" => self.parse_with_pipeline(),
            other => Err(Self::err(format!("unsupported pipeline stage {other:?}"))),
        }
    }

    fn parse_with_pipeline(&mut self) -> Result<Pipeline> {
        self.expect(&Token::LParen)?;
        let mut bindings = Vec::new();
        loop {
            let name = self.expect_ident()?;
            self.expect(&Token::Eq)?;
            let expr = self.parse_field_or()?;
            bindings.push(WithBinding { name, expr });
            if !self.eat(&Token::Comma) {
                break;
            }
        }
        self.expect(&Token::RParen)?;
        Ok(Pipeline::With(bindings))
    }

    fn parse_field_over_time(&mut self, name: &str) -> Result<Pipeline> {
        self.expect(&Token::LParen)?;
        let field = self.parse_field()?;
        self.expect(&Token::RParen)?;
        let aggregate = match name {
            "sum_over_time" => Aggregate::SumOverTime(field),
            "avg_over_time" => Aggregate::AvgOverTime(field),
            "min_over_time" => Aggregate::MinOverTime(field),
            "max_over_time" => Aggregate::MaxOverTime(field),
            _ => unreachable!("matched aggregate is exhaustive"),
        };
        Ok(Pipeline::Aggregate(aggregate))
    }

    fn parse_histogram_over_time(&mut self) -> Result<Pipeline> {
        self.expect(&Token::LParen)?;
        let field = if self.peek() == &Token::RParen {
            Field {
                scope: Scope::Intrinsic(Intrinsic::Duration),
                key: "duration".into(),
            }
        } else {
            self.parse_field()?
        };
        self.expect(&Token::RParen)?;
        Ok(Pipeline::Aggregate(Aggregate::HistogramOverTime(field)))
    }

    fn parse_adjacent_by(&mut self) -> Result<Option<Pipeline>> {
        if !matches!(self.peek(), Token::Ident(name) if name == "by") {
            return Ok(None);
        }
        self.pos += 1;
        self.expect(&Token::LParen)?;
        let fields = self.parse_field_list()?;
        self.expect(&Token::RParen)?;
        Ok(Some(Pipeline::By(fields)))
    }

    fn parse_rank_limit(&mut self) -> Result<usize> {
        let Token::Int(value) = self.advance() else {
            return Err(Self::err("expected integer rank limit"));
        };
        if value < 0 {
            return Err(Self::err("rank limit must be non-negative"));
        }
        usize::try_from(value).map_err(|e| TraceqlError::Parse(e.to_string()))
    }

    fn parse_numeric_filter(&mut self) -> Result<Option<(ComparisonOp, f64)>> {
        let Some(op) = self.parse_comparison_op() else {
            return Ok(None);
        };
        let value = numeric_filter_value(self.parse_additive_value(&numeric_filter_field())?)?;
        Ok(Some((op, value)))
    }

    fn parse_field_list(&mut self) -> Result<Vec<Field>> {
        let mut fields = vec![self.parse_field()?];
        while self.eat(&Token::Comma) {
            fields.push(self.parse_field()?);
        }
        Ok(fields)
    }

    fn parse_quantile(&mut self) -> Result<f64> {
        let value = if self.eat(&Token::Dot) {
            let digits = match self.advance() {
                Token::Int(v) => v.to_string(),
                other => {
                    return Err(Self::err(format!(
                        "expected quantile digits, got {other:?}"
                    )));
                }
            };
            format!("0.{digits}")
                .parse()
                .map_err(|e: std::num::ParseFloatError| TraceqlError::Parse(e.to_string()))?
        } else {
            match self.advance() {
                Token::Float(v) => v,
                Token::Int(v) => v
                    .to_string()
                    .parse()
                    .map_err(|e: std::num::ParseFloatError| TraceqlError::Parse(e.to_string()))?,
                other => return Err(Self::err(format!("expected quantile, got {other:?}"))),
            }
        };
        if !(0.0..=1.0).contains(&value) {
            return Err(Self::err(format!("quantile out of range: {value}")));
        }
        Ok(value)
    }

    fn parse_spanset_or(&mut self) -> Result<SpansetExpr> {
        let mut expr = self.parse_spanset_and()?;
        while self.eat(&Token::Or) {
            let rhs = self.parse_spanset_and()?;
            expr = SpansetExpr::Or(Box::new(expr), Box::new(rhs));
        }
        Ok(expr)
    }

    fn parse_spanset_and(&mut self) -> Result<SpansetExpr> {
        let mut expr = self.parse_structural()?;
        while self.eat(&Token::And) {
            let rhs = self.parse_structural()?;
            expr = SpansetExpr::And(Box::new(expr), Box::new(rhs));
        }
        Ok(expr)
    }

    fn parse_structural(&mut self) -> Result<SpansetExpr> {
        let mut expr = self.parse_spanset_primary()?;
        while let Some(op) = self.parse_structural_op() {
            let rhs = self.parse_spanset_primary()?;
            expr = SpansetExpr::Structural {
                op,
                lhs: Box::new(expr),
                rhs: Box::new(rhs),
            };
        }
        Ok(expr)
    }

    fn parse_spanset_primary(&mut self) -> Result<SpansetExpr> {
        if self.eat(&Token::LBrace) {
            let fe = self.parse_field_or()?;
            self.expect(&Token::RBrace)?;
            return Ok(SpansetExpr::Selector(Box::new(fe)));
        }
        if self.eat(&Token::LParen) {
            let expr = self.parse_spanset_or()?;
            self.expect(&Token::RParen)?;
            return Ok(expr);
        }
        Err(Self::err(format!(
            "expected spanset, got {:?}",
            self.peek()
        )))
    }

    fn parse_field_or(&mut self) -> Result<FieldExpr> {
        let mut expr = self.parse_field_and()?;
        while self.eat(&Token::Or) {
            let rhs = self.parse_field_and()?;
            expr = FieldExpr::Or(Box::new(expr), Box::new(rhs));
        }
        Ok(expr)
    }

    fn parse_field_and(&mut self) -> Result<FieldExpr> {
        let mut expr = self.parse_field_not()?;
        while self.eat(&Token::And) {
            let rhs = self.parse_field_not()?;
            expr = FieldExpr::And(Box::new(expr), Box::new(rhs));
        }
        Ok(expr)
    }

    fn parse_field_not(&mut self) -> Result<FieldExpr> {
        if self.eat(&Token::Not) {
            return Ok(FieldExpr::Not(Box::new(self.parse_field_not()?)));
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<FieldExpr> {
        if self.eat(&Token::LParen) {
            let expr = self.parse_field_or()?;
            self.expect(&Token::RParen)?;
            return Ok(expr);
        }
        let lhs = self.parse_field()?;
        let Some(op) = self.parse_comparison_op() else {
            return Ok(FieldExpr::Field(lhs));
        };
        if op == ComparisonOp::Eq && self.peek() == &Token::Eq {
            return Err(Self::err("use single = for equality; == is not TraceQL"));
        }
        let rhs = self.parse_value(&lhs)?;
        Ok(FieldExpr::Comparison { lhs, op, rhs })
    }

    fn parse_field(&mut self) -> Result<Field> {
        if self.eat(&Token::Dot) {
            return Ok(Field {
                scope: Scope::Both,
                key: self.expect_ident()?,
            });
        }

        let first = self.expect_ident()?;
        if self.eat(&Token::Colon) {
            let key = self.expect_ident()?;
            return Ok(Field {
                scope: Scope::Intrinsic(intrinsic(&first, &key)?),
                key,
            });
        }
        if self.eat(&Token::Dot) {
            return Ok(Field {
                scope: scope(&first)?,
                key: self.expect_ident()?,
            });
        }
        Ok(Field {
            scope: Scope::Both,
            key: first,
        })
    }

    fn parse_value(&mut self, lhs: &Field) -> Result<Value> {
        self.parse_additive_value(lhs)
    }

    fn parse_additive_value(&mut self, lhs: &Field) -> Result<Value> {
        let mut value = self.parse_multiplicative_value(lhs)?;
        loop {
            if self.eat(&Token::Plus) {
                value = value_add(value, self.parse_multiplicative_value(lhs)?)?;
            } else if self.eat(&Token::Minus) {
                value = value_sub(value, self.parse_multiplicative_value(lhs)?)?;
            } else {
                return Ok(value);
            }
        }
    }

    fn parse_multiplicative_value(&mut self, lhs: &Field) -> Result<Value> {
        let mut value = self.parse_power_value(lhs)?;
        loop {
            if self.eat(&Token::Star) {
                value = value_mul(value, self.parse_power_value(lhs)?)?;
            } else if self.eat(&Token::Slash) {
                value = value_div(value, self.parse_power_value(lhs)?)?;
            } else if self.eat(&Token::Mod) {
                value = value_mod(value, self.parse_power_value(lhs)?)?;
            } else {
                return Ok(value);
            }
        }
    }

    fn parse_power_value(&mut self, lhs: &Field) -> Result<Value> {
        let mut value = self.parse_unary_value(lhs)?;
        while self.eat(&Token::Caret) {
            value = value_pow(value, self.parse_unary_value(lhs)?)?;
        }
        Ok(value)
    }

    fn parse_unary_value(&mut self, lhs: &Field) -> Result<Value> {
        if self.eat(&Token::Minus) {
            return value_neg(self.parse_unary_value(lhs)?);
        }
        self.parse_primary_value(lhs)
    }

    fn parse_primary_value(&mut self, lhs: &Field) -> Result<Value> {
        match self.advance() {
            Token::Ident(v) if is_duration_field(lhs) => {
                parse_duration_nanos(&v).map(Value::Duration)
            }
            Token::Str(v) | Token::Ident(v) => Ok(Value::Str(v)),
            Token::Int(v) => Ok(Value::Int(v)),
            Token::Float(v) => Ok(Value::Float(v)),
            Token::Bool(v) => Ok(Value::Bool(v)),
            Token::Nil => Ok(Value::Nil),
            Token::LParen => {
                let value = self.parse_additive_value(lhs)?;
                self.expect(&Token::RParen)?;
                Ok(value)
            }
            other => Err(Self::err(format!("expected value, got {other:?}"))),
        }
    }

    fn parse_comparison_op(&mut self) -> Option<ComparisonOp> {
        let op = match self.peek() {
            Token::Eq => ComparisonOp::Eq,
            Token::Neq => ComparisonOp::Neq,
            Token::Parent => ComparisonOp::Lt,
            Token::Lte => ComparisonOp::Lte,
            Token::Child => ComparisonOp::Gt,
            Token::Gte => ComparisonOp::Gte,
            Token::Re => ComparisonOp::Re,
            Token::Nre => ComparisonOp::Nre,
            _ => return None,
        };
        self.pos += 1;
        Some(op)
    }

    fn parse_structural_op(&mut self) -> Option<StructuralOp> {
        let op = match self.peek() {
            Token::Desc => StructuralOp::Descendant,
            Token::Anc => StructuralOp::Ancestor,
            Token::Child => StructuralOp::Child,
            Token::Parent => StructuralOp::Parent,
            Token::Sibling => StructuralOp::Sibling,
            Token::NegDesc => StructuralOp::NegDescendant,
            Token::NegAnc => StructuralOp::NegAncestor,
            Token::NegChild => StructuralOp::NegChild,
            Token::NegParent => StructuralOp::NegParent,
            Token::UnionDesc => StructuralOp::UnionDescendant,
            Token::UnionAnc => StructuralOp::UnionAncestor,
            Token::UnionChild => StructuralOp::UnionChild,
            Token::UnionParent => StructuralOp::UnionParent,
            Token::UnionSibling => StructuralOp::UnionSibling,
            _ => return None,
        };
        self.pos += 1;
        Some(op)
    }

    fn expect_ident(&mut self) -> Result<String> {
        match self.advance() {
            Token::Ident(s) => Ok(s),
            other => Err(Self::err(format!("expected identifier, got {other:?}"))),
        }
    }

    fn expect(&mut self, expected: &Token) -> Result<()> {
        let got = self.advance();
        if &got == expected {
            Ok(())
        } else {
            Err(Self::err(format!("expected {expected:?}, got {got:?}")))
        }
    }

    fn eat(&mut self, expected: &Token) -> bool {
        if self.peek() == expected {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn advance(&mut self) -> Token {
        let t = self.peek().clone();
        self.pos += 1;
        t
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn err(msg: impl Into<String>) -> TraceqlError {
        TraceqlError::Parse(msg.into())
    }
}

fn scope(s: &str) -> Result<Scope> {
    match s {
        "span" => Ok(Scope::Span),
        "resource" => Ok(Scope::Resource),
        "parent" => Ok(Scope::Parent),
        "event" => Ok(Scope::Event),
        "link" => Ok(Scope::Link),
        "instrumentation" => Ok(Scope::Instrumentation),
        _ => Err(TraceqlError::Parse(format!("unknown scope {s:?}"))),
    }
}

fn intrinsic(scope: &str, key: &str) -> Result<Intrinsic> {
    match (scope, key) {
        ("span", "name") => Ok(Intrinsic::Name),
        ("span", "duration") => Ok(Intrinsic::Duration),
        ("span", "kind") => Ok(Intrinsic::Kind),
        ("span", "status") => Ok(Intrinsic::Status),
        ("span", "statusMessage") => Ok(Intrinsic::StatusMessage),
        ("span", "id") => Ok(Intrinsic::Id),
        ("span", "parentID" | "parentId") => Ok(Intrinsic::ParentId),
        ("span", "childCount") => Ok(Intrinsic::ChildCount),
        ("trace", "duration") => Ok(Intrinsic::TraceDuration),
        ("trace", "rootName") => Ok(Intrinsic::TraceRootName),
        ("trace", "rootService") => Ok(Intrinsic::TraceRootService),
        ("trace", "id") => Ok(Intrinsic::TraceId),
        ("event", "name") => Ok(Intrinsic::EventName),
        ("event", "timeSinceStart") => Ok(Intrinsic::EventTimeSinceStart),
        ("link", "traceID" | "traceId") => Ok(Intrinsic::LinkTraceId),
        ("link", "spanID" | "spanId") => Ok(Intrinsic::LinkSpanId),
        ("instrumentation", "name") => Ok(Intrinsic::InstrumentationName),
        ("instrumentation", "version") => Ok(Intrinsic::InstrumentationVersion),
        ("span", "nestedSetLeft") => Ok(Intrinsic::NestedSetLeft),
        ("span", "nestedSetRight") => Ok(Intrinsic::NestedSetRight),
        ("span", "nestedSetParent" | "Parent") => Ok(Intrinsic::NestedSetParent),
        _ => Err(TraceqlError::Parse(format!(
            "unknown intrinsic {scope}:{key}"
        ))),
    }
}

fn is_duration_field(field: &Field) -> bool {
    matches!(
        field.scope,
        Scope::Intrinsic(
            Intrinsic::Duration | Intrinsic::TraceDuration | Intrinsic::EventTimeSinceStart
        )
    )
}

fn value_add(lhs: Value, rhs: Value) -> Result<Value> {
    match (lhs, rhs) {
        (Value::Int(lhs), Value::Int(rhs)) => lhs
            .checked_add(rhs)
            .map(Value::Int)
            .ok_or_else(|| TraceqlError::Parse("integer addition out of range".into())),
        (Value::Float(lhs), Value::Float(rhs)) => Ok(Value::Float(lhs + rhs)),
        (Value::Int(lhs), Value::Float(rhs)) => Ok(Value::Float(i64_to_f64(lhs)? + rhs)),
        (Value::Float(lhs), Value::Int(rhs)) => Ok(Value::Float(lhs + i64_to_f64(rhs)?)),
        (Value::Duration(lhs), Value::Duration(rhs)) => lhs
            .checked_add(rhs)
            .map(Value::Duration)
            .ok_or_else(|| TraceqlError::Parse("duration addition out of range".into())),
        (lhs, rhs) => arithmetic_type_error("+", &lhs, &rhs),
    }
}

fn value_sub(lhs: Value, rhs: Value) -> Result<Value> {
    match (lhs, rhs) {
        (Value::Int(lhs), Value::Int(rhs)) => lhs
            .checked_sub(rhs)
            .map(Value::Int)
            .ok_or_else(|| TraceqlError::Parse("integer subtraction out of range".into())),
        (Value::Float(lhs), Value::Float(rhs)) => Ok(Value::Float(lhs - rhs)),
        (Value::Int(lhs), Value::Float(rhs)) => Ok(Value::Float(i64_to_f64(lhs)? - rhs)),
        (Value::Float(lhs), Value::Int(rhs)) => Ok(Value::Float(lhs - i64_to_f64(rhs)?)),
        (Value::Duration(lhs), Value::Duration(rhs)) => lhs
            .checked_sub(rhs)
            .map(Value::Duration)
            .ok_or_else(|| TraceqlError::Parse("duration subtraction out of range".into())),
        (lhs, rhs) => arithmetic_type_error("-", &lhs, &rhs),
    }
}

fn value_mul(lhs: Value, rhs: Value) -> Result<Value> {
    match (lhs, rhs) {
        (Value::Int(lhs), Value::Int(rhs)) => lhs
            .checked_mul(rhs)
            .map(Value::Int)
            .ok_or_else(|| TraceqlError::Parse("integer multiplication out of range".into())),
        (Value::Float(lhs), Value::Float(rhs)) => Ok(Value::Float(lhs * rhs)),
        (Value::Int(lhs), Value::Float(rhs)) => Ok(Value::Float(i64_to_f64(lhs)? * rhs)),
        (Value::Float(lhs), Value::Int(rhs)) => Ok(Value::Float(lhs * i64_to_f64(rhs)?)),
        (Value::Duration(lhs), Value::Int(rhs)) | (Value::Int(rhs), Value::Duration(lhs)) => lhs
            .checked_mul(rhs)
            .map(Value::Duration)
            .ok_or_else(|| TraceqlError::Parse("duration multiplication out of range".into())),
        (lhs, rhs) => arithmetic_type_error("*", &lhs, &rhs),
    }
}

fn value_div(lhs: Value, rhs: Value) -> Result<Value> {
    match (lhs, rhs) {
        (_, Value::Int(0) | Value::Float(0.0)) => {
            Err(TraceqlError::Parse("division by zero".into()))
        }
        (Value::Int(lhs), Value::Int(rhs)) => {
            let rem = lhs
                .checked_rem(rhs)
                .ok_or_else(|| TraceqlError::Parse("integer division out of range".into()))?;
            if rem == 0 {
                let quot = lhs
                    .checked_div(rhs)
                    .ok_or_else(|| TraceqlError::Parse("integer division out of range".into()))?;
                Ok(Value::Int(quot))
            } else {
                Ok(Value::Float(i64_to_f64(lhs)? / i64_to_f64(rhs)?))
            }
        }
        (Value::Float(lhs), Value::Float(rhs)) => Ok(Value::Float(lhs / rhs)),
        (Value::Int(lhs), Value::Float(rhs)) => Ok(Value::Float(i64_to_f64(lhs)? / rhs)),
        (Value::Float(lhs), Value::Int(rhs)) => Ok(Value::Float(lhs / i64_to_f64(rhs)?)),
        (Value::Duration(lhs), Value::Int(rhs)) => lhs
            .checked_div(rhs)
            .map(Value::Duration)
            .ok_or_else(|| TraceqlError::Parse("duration division out of range".into())),
        (lhs, rhs) => arithmetic_type_error("/", &lhs, &rhs),
    }
}

fn value_mod(lhs: Value, rhs: Value) -> Result<Value> {
    match (lhs, rhs) {
        (_, Value::Int(0) | Value::Duration(0)) => {
            Err(TraceqlError::Parse("modulo by zero".into()))
        }
        (Value::Int(lhs), Value::Int(rhs)) => lhs
            .checked_rem(rhs)
            .map(Value::Int)
            .ok_or_else(|| TraceqlError::Parse("integer modulo out of range".into())),
        (Value::Duration(lhs), Value::Duration(rhs)) => lhs
            .checked_rem(rhs)
            .map(Value::Duration)
            .ok_or_else(|| TraceqlError::Parse("duration modulo out of range".into())),
        (lhs, rhs) => arithmetic_type_error("%", &lhs, &rhs),
    }
}

fn value_pow(lhs: Value, rhs: Value) -> Result<Value> {
    match (lhs, rhs) {
        (Value::Int(lhs), Value::Int(rhs)) if rhs >= 0 => u32::try_from(rhs)
            .ok()
            .and_then(|rhs| lhs.checked_pow(rhs))
            .map(Value::Int)
            .ok_or_else(|| TraceqlError::Parse("integer exponentiation out of range".into())),
        (Value::Int(lhs), Value::Int(rhs)) => {
            Ok(Value::Float(i64_to_f64(lhs)?.powf(i64_to_f64(rhs)?)))
        }
        (Value::Float(lhs), Value::Float(rhs)) => Ok(Value::Float(lhs.powf(rhs))),
        (Value::Int(lhs), Value::Float(rhs)) => Ok(Value::Float(i64_to_f64(lhs)?.powf(rhs))),
        (Value::Float(lhs), Value::Int(rhs)) => Ok(Value::Float(lhs.powf(i64_to_f64(rhs)?))),
        (lhs, rhs) => arithmetic_type_error("^", &lhs, &rhs),
    }
}

fn value_neg(value: Value) -> Result<Value> {
    match value {
        Value::Int(value) => value
            .checked_neg()
            .map(Value::Int)
            .ok_or_else(|| TraceqlError::Parse("integer negation out of range".into())),
        Value::Float(value) => Ok(Value::Float(-value)),
        Value::Duration(value) => value
            .checked_neg()
            .map(Value::Duration)
            .ok_or_else(|| TraceqlError::Parse("duration negation out of range".into())),
        other => Err(TraceqlError::Parse(format!(
            "unary - is not supported for {other:?}"
        ))),
    }
}

fn arithmetic_type_error(op: &str, lhs: &Value, rhs: &Value) -> Result<Value> {
    Err(TraceqlError::Parse(format!(
        "operator {op} is not supported for {lhs:?} and {rhs:?}"
    )))
}

fn i64_to_f64(value: i64) -> Result<f64> {
    value
        .to_string()
        .parse()
        .map_err(|e: std::num::ParseFloatError| TraceqlError::Parse(e.to_string()))
}

fn numeric_filter_field() -> Field {
    Field {
        scope: Scope::Both,
        key: String::new(),
    }
}

fn numeric_filter_value(value: Value) -> Result<f64> {
    match value {
        Value::Int(value) => i64_to_f64(value),
        Value::Float(value) => Ok(value),
        other => Err(TraceqlError::Parse(format!(
            "expected numeric filter value, got {other:?}"
        ))),
    }
}

fn parse_duration_nanos(s: &str) -> Result<i64> {
    if s.is_empty() {
        return Err(TraceqlError::Parse("empty duration".into()));
    }

    let mut total = 0_i128;
    let mut rest = s;
    while !rest.is_empty() {
        let number_len = rest
            .char_indices()
            .take_while(|(_, ch)| ch.is_ascii_digit() || *ch == '.')
            .map(|(idx, ch)| idx + ch.len_utf8())
            .last()
            .ok_or_else(|| TraceqlError::Parse(format!("expected duration number in {s:?}")))?;
        let (number, tail) = rest.split_at(number_len);
        let unit_len = tail
            .char_indices()
            .take_while(|(_, ch)| ch.is_ascii_alphabetic() || *ch == 'µ')
            .map(|(idx, ch)| idx + ch.len_utf8())
            .last()
            .ok_or_else(|| {
                TraceqlError::Parse(format!("missing duration unit after {number:?}"))
            })?;
        let (unit, next) = tail.split_at(unit_len);
        let multiplier = match unit {
            "ns" => 1_i128,
            "us" | "µs" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            other => {
                return Err(TraceqlError::Parse(format!(
                    "unknown duration unit {other:?}"
                )));
            }
        };
        let component = parse_duration_component_nanos(number, multiplier, s)?;
        total = total
            .checked_add(component)
            .ok_or_else(|| TraceqlError::Parse(format!("duration out of range: {s:?}")))?;
        rest = next;
    }

    i64::try_from(total).map_err(|e| TraceqlError::Parse(e.to_string()))
}

fn parse_duration_component_nanos(number: &str, multiplier: i128, original: &str) -> Result<i128> {
    let (whole, fraction) = number.split_once('.').map_or((number, ""), |parts| parts);
    if whole.is_empty() && fraction.is_empty() {
        return Err(TraceqlError::Parse(format!(
            "invalid duration number {number:?}"
        )));
    }
    if fraction.contains('.') {
        return Err(TraceqlError::Parse(format!(
            "invalid duration number {number:?}"
        )));
    }

    let whole = if whole.is_empty() {
        0
    } else {
        whole
            .parse::<i128>()
            .map_err(|e| TraceqlError::Parse(e.to_string()))?
    };
    let whole_ns = whole
        .checked_mul(multiplier)
        .ok_or_else(|| TraceqlError::Parse(format!("duration out of range: {original:?}")))?;
    if fraction.is_empty() {
        return Ok(whole_ns);
    }

    let fraction_digits = fraction
        .parse::<i128>()
        .map_err(|e| TraceqlError::Parse(e.to_string()))?;
    let scale = 10_i128
        .checked_pow(u32::try_from(fraction.len()).map_err(|e| TraceqlError::Parse(e.to_string()))?)
        .ok_or_else(|| {
            TraceqlError::Parse(format!("duration precision too large: {original:?}"))
        })?;
    let fraction_ns = fraction_digits
        .checked_mul(multiplier)
        .ok_or_else(|| TraceqlError::Parse(format!("duration out of range: {original:?}")))?
        / scale;

    whole_ns
        .checked_add(fraction_ns)
        .ok_or_else(|| TraceqlError::Parse(format!("duration out of range: {original:?}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::*;
    use assert2::assert;

    #[test]
    fn bare_dot_is_both_scope() {
        let q = parse("{ .service = \"checkout\" }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!("selector")
        };
        let FieldExpr::Comparison { lhs, op, rhs } = fe.as_ref() else {
            panic!("cmp")
        };
        assert!(lhs.scope == Scope::Both && lhs.key == "service");
        assert!(*op == ComparisonOp::Eq);
        assert!(*rhs == Value::Str("checkout".into()));
    }

    #[test]
    fn span_colon_intrinsic_duration() {
        let q = parse("{ span:duration > 100ms }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!()
        };
        let FieldExpr::Comparison { lhs, op, rhs } = fe.as_ref() else {
            panic!()
        };
        assert!(lhs.scope == Scope::Intrinsic(Intrinsic::Duration));
        assert!(*op == ComparisonOp::Gt);
        assert!(*rhs == Value::Duration(100_000_000));
    }

    #[test]
    fn duration_literals_accept_compound_go_durations() {
        let q = parse("{ span:duration > 1m30s }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!()
        };
        let FieldExpr::Comparison { rhs, .. } = fe.as_ref() else {
            panic!()
        };
        assert!(*rhs == Value::Duration(90_000_000_000));
    }

    #[test]
    fn duration_literal_arithmetic_obeys_precedence() {
        let q = parse("{ span:duration > 100ms + 2 * 50ms }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!()
        };
        let FieldExpr::Comparison { rhs, .. } = fe.as_ref() else {
            panic!()
        };
        assert!(*rhs == Value::Duration(200_000_000));
    }

    #[test]
    fn numeric_literal_arithmetic_obeys_precedence() {
        let q = parse("{ .retries = 1 + 2 * 3 }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!()
        };
        let FieldExpr::Comparison { rhs, .. } = fe.as_ref() else {
            panic!()
        };
        assert!(*rhs == Value::Int(7));
    }

    #[test]
    fn single_span_rule_intra_brace_is_and() {
        let q = parse("{ .a = 1 && .b = 2 }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!()
        };
        assert!(matches!(fe.as_ref(), FieldExpr::And(_, _)));
    }

    #[test]
    fn grouped_field_boolean_parses_inside_selector() {
        let q = parse("{ !(.a = 1 || .b = 2) }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!()
        };
        assert!(
            matches!(fe.as_ref(), FieldExpr::Not(inner) if matches!(inner.as_ref(), FieldExpr::Or(_, _)))
        );
    }

    #[test]
    fn inter_brace_and_is_spanset_level() {
        let q = parse("{ .a = 1 } && { .b = 2 }").unwrap();
        assert!(matches!(q.root, SpansetExpr::And(_, _)));
    }

    #[test]
    fn structural_operators_parse() {
        for (query, expected) in [
            ("{ .a = 1 } >> { .b = 2 }", StructuralOp::Descendant),
            ("{ .a = 1 } << { .b = 2 }", StructuralOp::Ancestor),
            ("{ .a = 1 } > { .b = 2 }", StructuralOp::Child),
            ("{ .a = 1 } < { .b = 2 }", StructuralOp::Parent),
            ("{ .a = 1 } ~ { .b = 2 }", StructuralOp::Sibling),
            ("{ .a = 1 } !>> { .b = 2 }", StructuralOp::NegDescendant),
            ("{ .a = 1 } !<< { .b = 2 }", StructuralOp::NegAncestor),
            ("{ .a = 1 } !> { .b = 2 }", StructuralOp::NegChild),
            ("{ .a = 1 } !< { .b = 2 }", StructuralOp::NegParent),
            ("{ .a = 1 } &>> { .b = 2 }", StructuralOp::UnionDescendant),
            ("{ .a = 1 } &<< { .b = 2 }", StructuralOp::UnionAncestor),
            ("{ .a = 1 } &> { .b = 2 }", StructuralOp::UnionChild),
            ("{ .a = 1 } &< { .b = 2 }", StructuralOp::UnionParent),
            ("{ .a = 1 } &~ { .b = 2 }", StructuralOp::UnionSibling),
        ] {
            let q = parse(query).unwrap();
            let SpansetExpr::Structural { op, .. } = &q.root else {
                panic!("expected structural expression for {query}")
            };
            assert!(*op == expected);
        }
    }

    #[test]
    fn pipeline_count_with_filter() {
        let q = parse("{ .a = 1 } | count() > 2").unwrap();
        assert!(q.pipeline.len() == 2);
        assert!(q.pipeline[0] == Pipeline::Aggregate(Aggregate::Count));
        assert!(
            q.pipeline[1]
                == Pipeline::Filter {
                    op: ComparisonOp::Gt,
                    value: 2.0,
                }
        );
    }

    #[test]
    fn pipeline_scalar_filter_accepts_literal_arithmetic() {
        let q = parse("{ .a = 1 } | count() > 1 + 2 * 3").unwrap();
        assert!(q.pipeline.len() == 2);
        assert!(q.pipeline[0] == Pipeline::Aggregate(Aggregate::Count));
        assert!(
            q.pipeline[1]
                == Pipeline::Filter {
                    op: ComparisonOp::Gt,
                    value: 7.0,
                }
        );
    }

    #[test]
    fn pipeline_adjacent_by_parses_before_filter() {
        let q = parse("{ .a = 1 } | count() by(span.svc) > 2").unwrap();
        assert!(q.pipeline.len() == 3);
        assert!(q.pipeline[0] == Pipeline::Aggregate(Aggregate::Count));
        assert!(matches!(q.pipeline[1], Pipeline::By(_)));
        assert!(
            q.pipeline[2]
                == Pipeline::Filter {
                    op: ComparisonOp::Gt,
                    value: 2.0,
                }
        );
    }

    #[test]
    fn traceql_metrics_pipeline_functions_parse() {
        let q = parse("{ .a = 1 } | rate()").unwrap();
        assert!(q.pipeline == vec![Pipeline::Aggregate(Aggregate::Rate)]);

        let q = parse("{ .a = 1 } | count_over_time()").unwrap();
        assert!(q.pipeline == vec![Pipeline::Aggregate(Aggregate::CountOverTime)]);

        let q = parse("{ .a = 1 } | avg_over_time(span:duration)").unwrap();
        assert!(matches!(
            q.pipeline.as_slice(),
            [Pipeline::Aggregate(Aggregate::AvgOverTime(_))]
        ));

        let q =
            parse("{ .a = 1 } | quantile_over_time(span:duration, .5, 0.9) by(span.svc)").unwrap();
        let [
            Pipeline::Aggregate(Aggregate::QuantileOverTime { quantiles, .. }),
            Pipeline::By(by),
        ] = q.pipeline.as_slice()
        else {
            panic!("quantile pipeline")
        };
        assert!(*quantiles == vec![0.5, 0.9]);
        assert!(by[0].key == "svc");

        let q = parse("{ .a = 1 } | histogram_over_time(span:duration) by(span.svc)").unwrap();
        assert!(matches!(
            q.pipeline.as_slice(),
            [
                Pipeline::Aggregate(Aggregate::HistogramOverTime(_)),
                Pipeline::By(_)
            ]
        ));

        let q = parse("{ .a = 1 } | count_over_time() | by(span.svc) | topk(2)").unwrap();
        assert!(matches!(
            q.pipeline.as_slice(),
            [
                Pipeline::Aggregate(Aggregate::CountOverTime),
                Pipeline::By(_),
                Pipeline::TopK(2)
            ]
        ));

        let q = parse("{ .a = 1 } | count_over_time() | by(span.svc) | bottomk(1)").unwrap();
        assert!(matches!(
            q.pipeline.as_slice(),
            [
                Pipeline::Aggregate(Aggregate::CountOverTime),
                Pipeline::By(_),
                Pipeline::BottomK(1)
            ]
        ));

        let q = parse("{ .a = 1 } | count_over_time() | by(span.svc) | compare()").unwrap();
        assert!(matches!(
            q.pipeline.as_slice(),
            [
                Pipeline::Aggregate(Aggregate::CountOverTime),
                Pipeline::By(_),
                Pipeline::Compare
            ]
        ));
    }

    #[test]
    fn most_recent_query_hint_parses() {
        let q = parse("{ .a = 1 } with (most_recent=true)").unwrap();
        assert!(q.hints.most_recent);
        assert!(parse("{ .a = 1 } with (unknown=true)").is_err());
    }

    #[test]
    fn exemplars_query_hint_parses() {
        let q = parse("{ .a = 1 } | count_over_time() with (exemplars=false)").unwrap();
        assert!(q.hints.exemplars == Some(false));
    }

    #[test]
    fn pipeline_with_bindings_parse() {
        let q = parse("{ .a = 1 } | with(error = span:status = error)").unwrap();
        let [Pipeline::With(bindings)] = q.pipeline.as_slice() else {
            panic!("with pipeline")
        };
        assert!(bindings.len() == 1);
        assert!(bindings[0].name == "error");
        assert!(matches!(
            bindings[0].expr,
            FieldExpr::Comparison {
                lhs: Field {
                    scope: Scope::Intrinsic(Intrinsic::Status),
                    ..
                },
                op: ComparisonOp::Eq,
                rhs: Value::Str(ref value),
            } if value == "error"
        ));
    }

    #[test]
    fn double_equals_is_rejected() {
        assert!(parse("{ .a == 1 }").is_err());
    }

    #[test]
    fn value_fold_min_div_neg_one_errors_not_panics() {
        // (0 - 9223372036854775807 - 1) folds to i64::MIN; (0 - 1) folds to -1.
        // i64::MIN / -1 and i64::MIN % -1 overflow and must surface as a Parse
        // error rather than panicking the parser (DoS via crafted query).
        let div = parse("{ .x = (0 - 9223372036854775807 - 1) / (0 - 1) }");
        assert!(matches!(div, Err(TraceqlError::Parse(_))));

        let rem = parse("{ .x = (0 - 9223372036854775807 - 1) % (0 - 1) }");
        assert!(matches!(rem, Err(TraceqlError::Parse(_))));
    }

    #[test]
    fn value_fold_div_and_mod_still_work() {
        let q = parse("{ .x = 6 / 2 }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!("selector")
        };
        let FieldExpr::Comparison { rhs, .. } = fe.as_ref() else {
            panic!("cmp")
        };
        assert!(*rhs == Value::Int(3));

        let q = parse("{ .x = 7 % 3 }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!("selector")
        };
        let FieldExpr::Comparison { rhs, .. } = fe.as_ref() else {
            panic!("cmp")
        };
        assert!(*rhs == Value::Int(1));
    }

    #[test]
    fn quantile_leading_zero_fraction_preserved() {
        for (query, expected) in [
            ("{ .a = 1 } | quantile_over_time(span:duration, .05)", 0.05),
            ("{ .a = 1 } | quantile_over_time(span:duration, .99)", 0.99),
            ("{ .a = 1 } | quantile_over_time(span:duration, .5)", 0.5),
            ("{ .a = 1 } | quantile_over_time(span:duration, .9)", 0.9),
        ] {
            let q = parse(query).unwrap();
            let [Pipeline::Aggregate(Aggregate::QuantileOverTime { quantiles, .. })] =
                q.pipeline.as_slice()
            else {
                panic!("quantile pipeline for {query}")
            };
            assert!(*quantiles == vec![expected]);
        }
    }
}
