//! Storage boundary for `TraceQL` planning and execution.

use datafusion::prelude::SessionContext;

use crate::error::Result;
use crate::result::{ScopedTag, TagScope, TraceSpans, TypedValue};

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
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScanOptions {
    pub job: Option<ScanJob>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use datafusion::prelude::SessionContext;

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
