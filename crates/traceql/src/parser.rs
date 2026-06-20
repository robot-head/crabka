//! Recursive-descent `TraceQL` parser.

use crate::ast::{
    Aggregate, ComparisonOp, Field, FieldExpr, Intrinsic, Pipeline, Query, QueryHints, Scope,
    SpansetExpr, StructuralOp, Value,
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
            "sum_over_time"
            | "avg_over_time"
            | "min_over_time"
            | "max_over_time"
            | "histogram_over_time" => {
                self.expect(&Token::LParen)?;
                let field = self.parse_field()?;
                self.expect(&Token::RParen)?;
                let agg = match name.as_str() {
                    "sum_over_time" => Aggregate::SumOverTime(field),
                    "avg_over_time" => Aggregate::AvgOverTime(field),
                    "min_over_time" => Aggregate::MinOverTime(field),
                    "max_over_time" => Aggregate::MaxOverTime(field),
                    _ => Aggregate::HistogramOverTime(field),
                };
                Ok(Pipeline::Aggregate(agg))
            }
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
            other => Err(Self::err(format!("unsupported pipeline stage {other:?}"))),
        }
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
        let value = match self.advance() {
            Token::Int(v) => v
                .to_string()
                .parse::<f64>()
                .map_err(|e| TraceqlError::Parse(e.to_string()))?,
            Token::Float(v) => v,
            other => {
                return Err(Self::err(format!(
                    "expected numeric filter value, got {other:?}"
                )));
            }
        };
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
        match self.advance() {
            Token::Ident(v) if is_duration_field(lhs) => {
                parse_duration_nanos(&v).map(Value::Duration)
            }
            Token::Str(v) | Token::Ident(v) => Ok(Value::Str(v)),
            Token::Int(v) => Ok(Value::Int(v)),
            Token::Float(v) => Ok(Value::Float(v)),
            Token::Bool(v) => Ok(Value::Bool(v)),
            Token::Nil => Ok(Value::Nil),
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

fn parse_duration_nanos(s: &str) -> Result<i64> {
    let unit_start = s
        .find(|ch: char| !(ch.is_ascii_digit() || ch == '.'))
        .ok_or_else(|| TraceqlError::Parse(format!("missing duration unit in {s:?}")))?;
    let multiplier = match &s[unit_start..] {
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
    let number = &s[..unit_start];
    let nanos = if let Some((whole, frac)) = number.split_once('.') {
        let whole = whole
            .parse::<i128>()
            .map_err(|e| TraceqlError::Parse(e.to_string()))?;
        let frac_digits = frac
            .parse::<i128>()
            .map_err(|e| TraceqlError::Parse(e.to_string()))?;
        let scale = 10_i128
            .checked_pow(u32::try_from(frac.len()).map_err(|e| TraceqlError::Parse(e.to_string()))?)
            .ok_or_else(|| TraceqlError::Parse(format!("duration precision too large: {s:?}")))?;
        whole * multiplier + (frac_digits * multiplier / scale)
    } else {
        number
            .parse::<i128>()
            .map_err(|e| TraceqlError::Parse(e.to_string()))?
            * multiplier
    };
    i64::try_from(nanos).map_err(|e| TraceqlError::Parse(e.to_string()))
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
    fn single_span_rule_intra_brace_is_and() {
        let q = parse("{ .a = 1 && .b = 2 }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else {
            panic!()
        };
        assert!(matches!(fe.as_ref(), FieldExpr::And(_, _)));
    }

    #[test]
    fn inter_brace_and_is_spanset_level() {
        let q = parse("{ .a = 1 } && { .b = 2 }").unwrap();
        assert!(matches!(q.root, SpansetExpr::And(_, _)));
    }

    #[test]
    fn structural_descendant_parses() {
        let q = parse("{ .a = 1 } >> { .b = 2 }").unwrap();
        let SpansetExpr::Structural { op, .. } = &q.root else {
            panic!()
        };
        assert!(*op == StructuralOp::Descendant);
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
    fn double_equals_is_rejected() {
        assert!(parse("{ .a == 1 }").is_err());
    }
}
