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
    /// `with(sample=...)` — Tempo's probabilistic metrics-sampling hint. Grafana's
    /// Traces Drilldown sends `sample=true`. Accepted and recorded; Crabka computes
    /// exact metrics (sampling is a performance hint, so ignoring it is sound).
    pub sample: Option<bool>,
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
    /// A constant boolean filter. The empty spanset `{}` and the scalar-boolean
    /// spanset `{ true }` lower to `Const(true)` (match every span); `{ false }`
    /// lowers to `Const(false)` (match no span). Mirrors Grafana Tempo, whose
    /// Explore "Search" tab and TraceQL-metrics default to `{}`.
    Const(bool),
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
    Filter {
        op: ComparisonOp,
        value: f64,
    },
    By(Vec<Field>),
    TopK(usize),
    BottomK(usize),
    /// Tempo attribute-comparison metric: `compare({selection}, topN [, start_ns,
    /// end_ns])`. Partitions the spans matching the outer spanset into a
    /// `selection` group (also matching `selection`) and a `baseline` group (the
    /// rest), then emits per-attribute value-distribution series for each group.
    /// `top_n` keeps the most frequent values per attribute (default 10). The
    /// optional `start`/`end` nanosecond bounds narrow the selection sub-window.
    Compare {
        selection: Box<SpansetExpr>,
        top_n: usize,
        start: Option<i64>,
        end: Option<i64>,
    },
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
        assert_eq!(
            (
                matches!(q.root, SpansetExpr::Selector(_)),
                &q.pipeline,
                &q.hints,
            ),
            (
                true,
                &vec![Pipeline::Aggregate(Aggregate::Count)],
                &QueryHints::default(),
            )
        );
    }
}
