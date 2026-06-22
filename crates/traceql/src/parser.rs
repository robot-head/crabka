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
        ("span", "nestedSetParent") => Ok(Intrinsic::NestedSetParent),
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
    fn span_nested_set_parent_intrinsic_resolves() {
        let q = parse("{ span:nestedSetParent > 0 }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!()
        };
        let FieldExpr::Comparison { lhs, .. } = fe.as_ref() else {
            panic!()
        };
        assert!(lhs.scope == Scope::Intrinsic(Intrinsic::NestedSetParent));
    }

    #[test]
    fn span_parent_alias_is_not_a_valid_intrinsic() {
        // `span:Parent` was a bogus alias for nestedSetParent inconsistent with
        // Tempo's naming and with the other nested-set intrinsics; it must not
        // resolve.
        let err = parse("{ span:Parent > 0 }");
        assert!(matches!(err, Err(TraceqlError::Parse(_))));
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

    fn selector_rhs(query: &str) -> Value {
        let q = parse(query).unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!("selector for {query}")
        };
        let FieldExpr::Comparison { rhs, .. } = fe.as_ref() else {
            panic!("comparison for {query}")
        };
        rhs.clone()
    }

    fn parse_err(query: &str) -> String {
        match parse(query) {
            Err(TraceqlError::Parse(msg)) => msg,
            other => panic!("expected parse error for {query}, got {other:?}"),
        }
    }

    // ---- query hints ----

    #[test]
    fn query_hint_non_boolean_value_errors() {
        let msg = parse_err("{ .a = 1 } with (most_recent=5)");
        assert!(msg.contains("expected boolean query hint value"));
    }

    #[test]
    fn query_hint_multiple_entries_parse() {
        let q = parse("{ .a = 1 } | count_over_time() with (most_recent=true, exemplars=true)")
            .unwrap();
        assert!(q.hints.most_recent);
        assert!(q.hints.exemplars == Some(true));
    }

    #[test]
    fn query_hint_missing_equals_errors() {
        // `with (` followed by an identifier then a non-`=` token.
        let msg = parse_err("{ .a = 1 } with (most_recent true)");
        assert!(msg.contains("expected Eq"));
    }

    // ---- pipeline stages ----

    #[test]
    fn unsupported_pipeline_stage_errors() {
        let msg = parse_err("{ .a = 1 } | bogus()");
        assert!(msg.contains("unsupported pipeline stage"));
        assert!(msg.contains("bogus"));
    }

    #[test]
    fn sum_avg_max_min_aggregates_parse() {
        let q = parse("{ .a = 1 } | sum(.x)").unwrap();
        assert!(matches!(
            q.pipeline.as_slice(),
            [Pipeline::Aggregate(Aggregate::Sum(_))]
        ));
        let q = parse("{ .a = 1 } | avg(.x)").unwrap();
        assert!(matches!(
            q.pipeline.as_slice(),
            [Pipeline::Aggregate(Aggregate::Avg(_))]
        ));
        let q = parse("{ .a = 1 } | max(.x)").unwrap();
        assert!(matches!(
            q.pipeline.as_slice(),
            [Pipeline::Aggregate(Aggregate::Max(_))]
        ));
        let q = parse("{ .a = 1 } | min(.x)").unwrap();
        assert!(matches!(
            q.pipeline.as_slice(),
            [Pipeline::Aggregate(Aggregate::Min(_))]
        ));
    }

    #[test]
    fn over_time_aggregates_parse() {
        for (query, ok) in [
            (
                "{ .a = 1 } | sum_over_time(span:duration)",
                matches!(
                    parse("{ .a = 1 } | sum_over_time(span:duration)")
                        .unwrap()
                        .pipeline
                        .as_slice(),
                    [Pipeline::Aggregate(Aggregate::SumOverTime(_))]
                ),
            ),
            (
                "{ .a = 1 } | min_over_time(span:duration)",
                matches!(
                    parse("{ .a = 1 } | min_over_time(span:duration)")
                        .unwrap()
                        .pipeline
                        .as_slice(),
                    [Pipeline::Aggregate(Aggregate::MinOverTime(_))]
                ),
            ),
            (
                "{ .a = 1 } | max_over_time(span:duration)",
                matches!(
                    parse("{ .a = 1 } | max_over_time(span:duration)")
                        .unwrap()
                        .pipeline
                        .as_slice(),
                    [Pipeline::Aggregate(Aggregate::MaxOverTime(_))]
                ),
            ),
        ] {
            assert!(ok, "over-time aggregate failed for {query}");
        }
    }

    #[test]
    fn histogram_over_time_defaults_to_duration_when_empty() {
        let q = parse("{ .a = 1 } | histogram_over_time()").unwrap();
        let [Pipeline::Aggregate(Aggregate::HistogramOverTime(field))] = q.pipeline.as_slice()
        else {
            panic!("histogram pipeline")
        };
        assert!(field.scope == Scope::Intrinsic(Intrinsic::Duration));
        assert!(field.key == "duration");
    }

    #[test]
    fn select_and_coalesce_pipeline_stages_parse() {
        let q = parse("{ .a = 1 } | select(.x, .y)").unwrap();
        let [Pipeline::Select(fields)] = q.pipeline.as_slice() else {
            panic!("select pipeline")
        };
        assert!(fields.len() == 2);
        assert!(fields[0].key == "x");
        assert!(fields[1].key == "y");

        let q = parse("{ .a = 1 } | coalesce()").unwrap();
        assert!(q.pipeline == vec![Pipeline::Coalesce]);
    }

    #[test]
    fn with_pipeline_supports_multiple_bindings() {
        let q = parse("{ .a = 1 } | with(x = .foo, y = .bar)").unwrap();
        let [Pipeline::With(bindings)] = q.pipeline.as_slice() else {
            panic!("with pipeline")
        };
        assert!(bindings.len() == 2);
        assert!(bindings[0].name == "x");
        assert!(bindings[1].name == "y");
    }

    // ---- rank limits ----

    #[test]
    fn rank_limit_requires_integer() {
        let msg = parse_err("{ .a = 1 } | topk(.5)");
        assert!(msg.contains("expected integer rank limit"));
    }

    #[test]
    fn rank_limit_rejects_negative() {
        let msg = parse_err("{ .a = 1 } | bottomk(0 - 1)");
        // `0 - 1` is two int tokens; the rank parser only reads the first Int so
        // it sees `0` then a stray token. Use a directly negative literal path.
        // A bare negative integer is lexed as Minus Int, so topk reads Minus.
        assert!(!msg.is_empty());
        let msg = parse_err("{ .a = 1 } | topk(-2)");
        assert!(msg.contains("expected integer rank limit"));
    }

    // ---- quantile edge cases ----

    #[test]
    fn quantile_accepts_integer_zero_and_one() {
        let q = parse("{ .a = 1 } | quantile_over_time(span:duration, 0, 1)").unwrap();
        let [Pipeline::Aggregate(Aggregate::QuantileOverTime { quantiles, .. })] =
            q.pipeline.as_slice()
        else {
            panic!("quantile pipeline")
        };
        assert!(*quantiles == vec![0.0, 1.0]);
    }

    #[test]
    fn quantile_out_of_range_errors() {
        let msg = parse_err("{ .a = 1 } | quantile_over_time(span:duration, 2)");
        assert!(msg.contains("quantile out of range"));
    }

    #[test]
    fn quantile_non_numeric_token_errors() {
        let msg = parse_err("{ .a = 1 } | quantile_over_time(span:duration, foo)");
        assert!(msg.contains("expected quantile"));
    }

    #[test]
    fn quantile_leading_dot_non_digit_errors() {
        let msg = parse_err("{ .a = 1 } | quantile_over_time(span:duration, .foo)");
        assert!(msg.contains("expected quantile digits"));
    }

    // ---- spanset / structural parsing ----

    #[test]
    fn parenthesized_spanset_groups_or() {
        let q = parse("({ .a = 1 } || { .b = 2 }) && { .c = 3 }").unwrap();
        let SpansetExpr::And(lhs, _) = &q.root else {
            panic!("and at top")
        };
        assert!(matches!(lhs.as_ref(), SpansetExpr::Or(_, _)));
    }

    #[test]
    fn spanset_primary_requires_brace_or_paren() {
        let msg = parse_err(".a = 1");
        assert!(msg.contains("expected spanset"));
    }

    #[test]
    fn inter_brace_or_is_spanset_level() {
        let q = parse("{ .a = 1 } || { .b = 2 }").unwrap();
        assert!(matches!(q.root, SpansetExpr::Or(_, _)));
    }

    // ---- field parsing ----

    #[test]
    fn explicit_scope_dot_key_resolves() {
        for (query, scope) in [
            ("{ resource.region = \"us\" }", Scope::Resource),
            ("{ event.foo = 1 }", Scope::Event),
            ("{ link.foo = 1 }", Scope::Link),
            ("{ instrumentation.foo = 1 }", Scope::Instrumentation),
            ("{ parent.foo = 1 }", Scope::Parent),
        ] {
            let q = parse(query).unwrap();
            let SpansetExpr::Selector(fe) = &q.root else {
                panic!("selector for {query}")
            };
            let FieldExpr::Comparison { lhs, .. } = fe.as_ref() else {
                panic!("comparison for {query}")
            };
            assert!(lhs.scope == scope, "scope mismatch for {query}");
        }
    }

    #[test]
    fn unknown_scope_prefix_errors() {
        let msg = parse_err("{ bogus.foo = 1 }");
        assert!(msg.contains("unknown scope"));
    }

    #[test]
    fn unknown_intrinsic_errors() {
        let msg = parse_err("{ span:bogus = 1 }");
        assert!(msg.contains("unknown intrinsic"));
    }

    #[test]
    fn bare_ident_is_both_scope() {
        let q = parse("{ foo = 1 }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!("selector")
        };
        let FieldExpr::Comparison { lhs, .. } = fe.as_ref() else {
            panic!("comparison")
        };
        assert!(lhs.scope == Scope::Both);
        assert!(lhs.key == "foo");
    }

    #[test]
    fn bare_field_without_comparison_is_existence() {
        let q = parse("{ .foo }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!("selector")
        };
        let FieldExpr::Field(field) = fe.as_ref() else {
            panic!("bare field")
        };
        assert!(field.key == "foo");
    }

    #[test]
    fn trace_and_event_intrinsics_resolve() {
        for (query, intrinsic) in [
            ("{ trace:rootName = \"x\" }", Intrinsic::TraceRootName),
            ("{ trace:rootService = \"x\" }", Intrinsic::TraceRootService),
            ("{ trace:id = \"x\" }", Intrinsic::TraceId),
            ("{ event:name = \"x\" }", Intrinsic::EventName),
            ("{ link:traceID = \"x\" }", Intrinsic::LinkTraceId),
            ("{ link:spanID = \"x\" }", Intrinsic::LinkSpanId),
            (
                "{ instrumentation:version = \"x\" }",
                Intrinsic::InstrumentationVersion,
            ),
            ("{ span:nestedSetLeft > 0 }", Intrinsic::NestedSetLeft),
            ("{ span:nestedSetRight > 0 }", Intrinsic::NestedSetRight),
            ("{ span:parentId = \"x\" }", Intrinsic::ParentId),
        ] {
            let q = parse(query).unwrap();
            let SpansetExpr::Selector(fe) = &q.root else {
                panic!("selector for {query}")
            };
            let FieldExpr::Comparison { lhs, .. } = fe.as_ref() else {
                panic!("comparison for {query}")
            };
            assert!(
                lhs.scope == Scope::Intrinsic(intrinsic),
                "intrinsic mismatch for {query}"
            );
        }
    }

    // ---- value parsing ----

    #[test]
    fn primary_values_cover_all_literal_kinds() {
        assert!(selector_rhs("{ .a = \"s\" }") == Value::Str("s".into()));
        assert!(selector_rhs("{ .a = 42 }") == Value::Int(42));
        assert!(selector_rhs("{ .a = 1.5 }") == Value::Float(1.5));
        assert!(selector_rhs("{ .a = true }") == Value::Bool(true));
        assert!(selector_rhs("{ .a = nil }") == Value::Nil);
        // bare identifier on a non-duration field folds to a string value.
        assert!(selector_rhs("{ .a = ident }") == Value::Str("ident".into()));
    }

    #[test]
    fn parenthesized_value_groups_arithmetic() {
        assert!(selector_rhs("{ .a = (1 + 2) * 3 }") == Value::Int(9));
    }

    #[test]
    fn missing_value_errors() {
        let msg = parse_err("{ .a = }");
        assert!(msg.contains("expected value"));
    }

    #[test]
    fn unary_negation_folds() {
        assert!(selector_rhs("{ .a = -5 }") == Value::Int(-5));
        assert!(selector_rhs("{ .a = - -5 }") == Value::Int(5));
    }

    #[test]
    fn power_operator_folds() {
        assert!(selector_rhs("{ .a = 2 ^ 3 }") == Value::Int(8));
        // negative exponent falls back to float.
        assert!(selector_rhs("{ .a = 2 ^ (0 - 1) }") == Value::Float(0.5));
    }

    #[test]
    fn modulo_operator_folds() {
        assert!(selector_rhs("{ .a = 10 % 3 }") == Value::Int(1));
    }

    // ---- value arithmetic helpers: mixed int/float ----

    #[test]
    fn mixed_int_float_arithmetic_promotes_to_float() {
        assert!(selector_rhs("{ .a = 1 + 2.0 }") == Value::Float(3.0));
        assert!(selector_rhs("{ .a = 2.0 + 1 }") == Value::Float(3.0));
        assert!(selector_rhs("{ .a = 5 - 1.5 }") == Value::Float(3.5));
        assert!(selector_rhs("{ .a = 5.5 - 1 }") == Value::Float(4.5));
        assert!(selector_rhs("{ .a = 2 * 1.5 }") == Value::Float(3.0));
        assert!(selector_rhs("{ .a = 1.5 * 2 }") == Value::Float(3.0));
        assert!(selector_rhs("{ .a = 3 / 1.5 }") == Value::Float(2.0));
        assert!(selector_rhs("{ .a = 3.0 / 2 }") == Value::Float(1.5));
        assert!(selector_rhs("{ .a = 1.0 + 2.0 }") == Value::Float(3.0));
        assert!(selector_rhs("{ .a = 6.0 / 2.0 }") == Value::Float(3.0));
        assert!(selector_rhs("{ .a = 1.0 - 0.5 }") == Value::Float(0.5));
        assert!(selector_rhs("{ .a = 2.0 * 2.0 }") == Value::Float(4.0));
    }

    #[test]
    fn float_power_variants_fold() {
        assert!(selector_rhs("{ .a = 2.0 ^ 2.0 }") == Value::Float(4.0));
        assert!(selector_rhs("{ .a = 2 ^ 2.0 }") == Value::Float(4.0));
        assert!(selector_rhs("{ .a = 2.0 ^ 2 }") == Value::Float(4.0));
    }

    // ---- duration arithmetic ----

    #[test]
    fn duration_subtraction_and_modulo_fold() {
        assert!(selector_rhs("{ span:duration = 100ms - 40ms }") == Value::Duration(60_000_000));
        assert!(selector_rhs("{ span:duration = 100ms % 30ms }") == Value::Duration(10_000_000));
    }

    #[test]
    fn duration_scalar_division_folds() {
        assert!(selector_rhs("{ span:duration = 100ms / 4 }") == Value::Duration(25_000_000));
    }

    #[test]
    fn duration_negation_folds() {
        assert!(selector_rhs("{ span:duration = 0ms - 5ms }") == Value::Duration(-5_000_000));
    }

    #[test]
    fn float_negation_folds() {
        assert!(selector_rhs("{ .a = 0.0 - 2.5 }") == Value::Float(-2.5));
        assert!(selector_rhs("{ .a = -2.5 }") == Value::Float(-2.5));
    }

    // ---- arithmetic error / overflow paths ----

    #[test]
    fn division_by_zero_errors() {
        assert!(parse_err("{ .a = 1 / 0 }").contains("division by zero"));
        assert!(parse_err("{ .a = 1.0 / 0.0 }").contains("division by zero"));
    }

    #[test]
    fn modulo_by_zero_errors() {
        assert!(parse_err("{ .a = 1 % 0 }").contains("modulo by zero"));
    }

    #[test]
    fn integer_addition_overflow_errors() {
        let msg = parse_err("{ .a = 9223372036854775807 + 1 }");
        assert!(msg.contains("integer addition out of range"));
    }

    #[test]
    fn integer_multiplication_overflow_errors() {
        let msg = parse_err("{ .a = 9223372036854775807 * 2 }");
        assert!(msg.contains("integer multiplication out of range"));
    }

    #[test]
    fn integer_exponentiation_overflow_errors() {
        let msg = parse_err("{ .a = 9223372036854775807 ^ 2 }");
        assert!(msg.contains("integer exponentiation out of range"));
    }

    #[test]
    fn type_mismatched_arithmetic_errors() {
        // adding a string to an int is unsupported.
        let msg = parse_err("{ .a = 1 + \"x\" }");
        assert!(msg.contains("is not supported"));
    }

    #[test]
    fn unary_negation_of_string_errors() {
        let msg = parse_err("{ .a = -\"x\" }");
        assert!(msg.contains("unary - is not supported"));
    }

    #[test]
    fn duration_plus_int_is_type_error() {
        // duration + bare int is not a supported combination.
        let msg = parse_err("{ span:duration = 100ms + 5 }");
        assert!(msg.contains("is not supported"));
    }

    // ---- numeric pipeline filter value validation ----

    #[test]
    fn pipeline_filter_rejects_non_numeric_value() {
        let msg = parse_err("{ .a = 1 } | count() > \"x\"");
        assert!(msg.contains("expected numeric filter value"));
    }

    #[test]
    fn pipeline_filter_accepts_float_value() {
        let q = parse("{ .a = 1 } | count() > 1.5").unwrap();
        assert!(
            q.pipeline[1]
                == Pipeline::Filter {
                    op: ComparisonOp::Gt,
                    value: 1.5,
                }
        );
    }

    // ---- duration literal parsing edge cases ----

    #[test]
    fn duration_units_all_resolve() {
        assert!(selector_rhs("{ span:duration = 5ns }") == Value::Duration(5));
        assert!(selector_rhs("{ span:duration = 5us }") == Value::Duration(5_000));
        assert!(selector_rhs("{ span:duration = 5ms }") == Value::Duration(5_000_000));
        assert!(selector_rhs("{ span:duration = 5s }") == Value::Duration(5_000_000_000));
        assert!(selector_rhs("{ span:duration = 2m }") == Value::Duration(120_000_000_000));
        assert!(selector_rhs("{ span:duration = 1h }") == Value::Duration(3_600_000_000_000));
    }

    #[test]
    fn duration_fractional_component_folds() {
        assert!(selector_rhs("{ span:duration = 1.5s }") == Value::Duration(1_500_000_000));
    }

    #[test]
    fn duration_unknown_unit_errors() {
        let msg = parse_err("{ span:duration = 5zz }");
        assert!(msg.contains("unknown duration unit"));
    }

    #[test]
    fn non_numeric_duration_ident_errors() {
        // A bare identifier against a duration field is routed to the duration
        // parser, which fails because it has no leading number.
        let msg = parse_err("{ span:duration = abc }");
        assert!(msg.contains("duration number") || msg.contains("duration"));
    }

    #[test]
    fn duration_number_without_unit_errors() {
        // `5x` lexes as one duration ident: number `5` then unit `x`, which is
        // an unknown unit. `12` followed by a non-unit char trips the missing
        // unit path; emulate via a digits-only ident such as `5k`.
        let msg = parse_err("{ span:duration = 5k }");
        assert!(msg.contains("unknown duration unit"));
    }

    #[test]
    fn bare_int_against_duration_field_folds_as_int() {
        // A plain integer literal against a duration field is lexed as an Int
        // token (not a duration ident), so it folds to an Int value.
        assert!(selector_rhs("{ span:duration = 5 }") == Value::Int(5));
    }

    #[test]
    fn all_comparison_operators_parse() {
        for (query, expected) in [
            ("{ .a < 5 }", ComparisonOp::Lt),
            ("{ .a <= 5 }", ComparisonOp::Lte),
            ("{ .a > 5 }", ComparisonOp::Gt),
            ("{ .a >= 5 }", ComparisonOp::Gte),
            ("{ .a != 5 }", ComparisonOp::Neq),
            ("{ .a =~ \"x\" }", ComparisonOp::Re),
            ("{ .a !~ \"x\" }", ComparisonOp::Nre),
            ("{ .a = 5 }", ComparisonOp::Eq),
        ] {
            let q = parse(query).unwrap();
            let SpansetExpr::Selector(fe) = &q.root else {
                panic!("selector for {query}")
            };
            let FieldExpr::Comparison { op, .. } = fe.as_ref() else {
                panic!("comparison for {query}")
            };
            assert!(*op == expected, "op mismatch for {query}");
        }
    }

    #[test]
    fn leading_dot_duration_fraction_folds() {
        // `.5s` lexes as one duration ident with an empty whole part and a
        // fractional part, exercising the fraction-scaling branch.
        assert!(selector_rhs("{ span:duration = .5s }") == Value::Duration(500_000_000));
    }

    #[test]
    fn duration_overflow_errors() {
        // A duration literal far beyond i64 nanoseconds must surface as a parse
        // error rather than overflowing (the i64::try_from at the end fails).
        let msg = parse_err("{ span:duration = 100000000000h }");
        assert!(msg.contains("range"));
    }

    #[test]
    fn duration_i128_multiply_overflow_errors() {
        // A whole-number part that parses as i128 but overflows when scaled by
        // the hour multiplier must surface "duration out of range". 30 digits
        // (~1e29) is well within i128, but ×3.6e12 exceeds i128::MAX.
        let big = "1".to_string() + &"0".repeat(29);
        let msg = parse_err(&format!("{{ span:duration = {big}h }}"));
        assert!(msg.contains("duration out of range"));
    }
}
