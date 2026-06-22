//! `TraceQL` abstract syntax tree.

#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    pub root: SpansetExpr,
    pub pipeline: Vec<Pipeline>,
    pub hints: QueryHints,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueryHints {
    pub most_recent: bool,
    pub exemplars: Option<bool>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SpansetExpr {
    Selector(Box<FieldExpr>),
    And(Box<SpansetExpr>, Box<SpansetExpr>),
    Or(Box<SpansetExpr>, Box<SpansetExpr>),
    Structural {
        op: StructuralOp,
        lhs: Box<SpansetExpr>,
        rhs: Box<SpansetExpr>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum FieldExpr {
    Comparison {
        lhs: Field,
        op: ComparisonOp,
        rhs: Value,
    },
    And(Box<FieldExpr>, Box<FieldExpr>),
    Or(Box<FieldExpr>, Box<FieldExpr>),
    Not(Box<FieldExpr>),
    Field(Field),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    pub scope: Scope,
    pub key: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Scope {
    Both,
    Span,
    Resource,
    Parent,
    Event,
    Link,
    Instrumentation,
    Intrinsic(Intrinsic),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Intrinsic {
    Name,
    Duration,
    Kind,
    Status,
    StatusMessage,
    Id,
    ParentId,
    ChildCount,
    TraceDuration,
    TraceRootName,
    TraceRootService,
    TraceId,
    EventName,
    EventTimeSinceStart,
    LinkTraceId,
    LinkSpanId,
    InstrumentationName,
    InstrumentationVersion,
    NestedSetLeft,
    NestedSetRight,
    NestedSetParent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComparisonOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    Re,
    Nre,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Str(String),
    Int(i64),
    Float(f64),
    Duration(i64),
    Bool(bool),
    Nil,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StructuralOp {
    Descendant,
    Ancestor,
    Child,
    Parent,
    Sibling,
    NegDescendant,
    NegAncestor,
    NegChild,
    NegParent,
    UnionDescendant,
    UnionAncestor,
    UnionChild,
    UnionParent,
    UnionSibling,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Pipeline {
    Aggregate(Aggregate),
    Filter { op: ComparisonOp, value: f64 },
    By(Vec<Field>),
    TopK(usize),
    BottomK(usize),
    Compare,
    Select(Vec<Field>),
    Coalesce,
    With(Vec<WithBinding>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct WithBinding {
    pub name: String,
    pub expr: FieldExpr,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Aggregate {
    Count,
    Rate,
    CountOverTime,
    SumOverTime(Field),
    AvgOverTime(Field),
    MinOverTime(Field),
    MaxOverTime(Field),
    HistogramOverTime(Field),
    QuantileOverTime { field: Field, quantiles: Vec<f64> },
    Sum(Field),
    Avg(Field),
    Max(Field),
    Min(Field),
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn ast_constructs_selector_and_pipeline() {
        let q = Query {
            root: SpansetExpr::Selector(Box::new(FieldExpr::Comparison {
                lhs: Field {
                    scope: Scope::Both,
                    key: "foo".into(),
                },
                op: ComparisonOp::Eq,
                rhs: Value::Int(1),
            })),
            pipeline: vec![Pipeline::Aggregate(Aggregate::Count)],
            hints: QueryHints::default(),
        };
        assert!(matches!(q.root, SpansetExpr::Selector(_)));
        assert!(q.pipeline == vec![Pipeline::Aggregate(Aggregate::Count)]);
    }
}
