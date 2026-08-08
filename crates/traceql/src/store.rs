//! Storage boundary for `TraceQL` planning and execution.

use crabka_units::ByteSize;
use datafusion::prelude::SessionContext;

use crate::{
    error::Result,
    result::{ScopedTag, TagScope, TraceSpans, TypedValue},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatchScope {
    Both,
    Span,
    Resource,
    Intrinsic,
    Parent,
    Event,
    Link,
    Instrumentation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatchCmp {
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
pub enum MatchValue {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Nil,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SpanMatcher {
    pub scope: MatchScope,
    pub key: String,
    pub op: MatchCmp,
    pub value: MatchValue,
    pub negated: bool,
}

pub struct ScanResult {
    pub ctx: SessionContext,
    pub span_table: String,
    /// Approximate decoded size of the scanned data that the store registers
    /// into `ctx`. This is the data the scan inspected, before the query
    /// filters it. The engine passes the value up to
    /// `SearchResponse::inspected`.
    pub inspected: ByteSize,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ScanOptions {
    pub job: Option<ScanJob>,
    pub projection_matchers: Vec<SpanMatcher>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanJob {
    pub object_key: String,
    pub row_group_start: usize,
    pub row_group_end: usize,
}

#[async_trait::async_trait]
pub trait SpanStore: Send + Sync {
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[SpanMatcher],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<ScanResult>;

    async fn scan_with_options(
        &self,
        tenant: &str,
        matchers: &[SpanMatcher],
        start_ns: i64,
        end_ns: i64,
        options: &ScanOptions,
    ) -> Result<ScanResult> {
        let _ = options;
        self.scan(tenant, matchers, start_ns, end_ns).await
    }

    async fn trace_by_id(&self, tenant: &str, trace_id: &[u8; 16]) -> Result<Option<TraceSpans>>;

    async fn trace_by_id_within(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Option<TraceSpans>> {
        Ok(self
            .trace_by_id(tenant, trace_id)
            .await?
            .map(|trace| filter_trace_spans_by_time(trace, start_ns, end_ns)))
    }

    async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>>;

    async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>>;
}

#[must_use]
pub fn filter_trace_spans_by_time(mut trace: TraceSpans, start_ns: i64, end_ns: i64) -> TraceSpans {
    trace.spans.retain(|span| {
        let Ok(start) = i64::try_from(span.start_time_unix_nano) else {
            return false;
        };
        start >= start_ns && start <= end_ns
    });
    trace
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_units::bytes;
    use datafusion::prelude::SessionContext;

    use super::*;

    struct Empty;

    #[async_trait::async_trait]
    impl SpanStore for Empty {
        async fn scan(
            &self,
            _tenant: &str,
            _matchers: &[SpanMatcher],
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<ScanResult> {
            Ok(ScanResult {
                ctx: SessionContext::new(),
                span_table: "spans".into(),
                inspected: bytes(0),
            })
        }

        async fn trace_by_id(
            &self,
            _tenant: &str,
            _trace_id: &[u8; 16],
        ) -> Result<Option<crate::result::TraceSpans>> {
            Ok(None)
        }

        async fn tag_names(
            &self,
            _tenant: &str,
            _scope: Option<crate::result::TagScope>,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<crate::result::ScopedTag>> {
            Ok(vec![])
        }

        async fn tag_values(
            &self,
            _tenant: &str,
            _tag: &str,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<crate::result::TypedValue>> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn trait_is_object_safe() {
        let s: std::sync::Arc<dyn SpanStore> = std::sync::Arc::new(Empty);
        let r = s.scan("t", &[], 0, 1).await.unwrap();
        assert!(r.span_table == "spans");
        assert!(s.trace_by_id("t", &[0; 16]).await.unwrap().is_none());
    }
}
