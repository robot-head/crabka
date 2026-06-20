//! Read-side wrapper over the traces hot tier.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use crabka_traceql::{ScopedTag, TagScope, TraceSpans, TraceqlError, TypedValue};

pub type Result<T> = std::result::Result<T, TraceqlError>;

#[async_trait::async_trait]
pub trait LiveSource: Send + Sync {
    async fn span_batches(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<RecordBatch>>;

    async fn trace_spans(&self, tenant: &str, trace_id: &[u8; 16]) -> Result<Option<TraceSpans>>;

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

    fn block_builder_frontier_ns(&self, tenant: &str) -> i64;
}

pub struct LiveTier {
    source: Arc<dyn LiveSource>,
}

impl LiveTier {
    #[must_use]
    pub fn new(source: Arc<dyn LiveSource>) -> Self {
        Self { source }
    }

    pub async fn span_batches(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<RecordBatch>> {
        self.source.span_batches(tenant, start_ns, end_ns).await
    }

    pub async fn trace_spans(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
    ) -> Result<Option<TraceSpans>> {
        self.source.trace_spans(tenant, trace_id).await
    }

    pub async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>> {
        self.source.tag_names(tenant, scope, start_ns, end_ns).await
    }

    pub async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>> {
        self.source.tag_values(tenant, tag, start_ns, end_ns).await
    }

    #[must_use]
    pub fn block_builder_frontier_ns(&self, tenant: &str) -> i64 {
        self.source.block_builder_frontier_ns(tenant)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::record_batch::RecordBatch;
    use assert2::assert;
    use crabka_traceql::{AttrValue, ScopedTag, SpanRef, TagScope, TraceSpans, TypedValue};

    use super::*;

    #[derive(Default)]
    struct FakeLiveSource {
        batches: Vec<RecordBatch>,
        trace: Option<TraceSpans>,
        tags: Vec<ScopedTag>,
        values: Vec<TypedValue>,
        frontiers: BTreeMap<String, i64>,
    }

    #[async_trait::async_trait]
    impl LiveSource for FakeLiveSource {
        async fn span_batches(
            &self,
            _tenant: &str,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<RecordBatch>> {
            Ok(self.batches.clone())
        }

        async fn trace_spans(
            &self,
            _tenant: &str,
            _trace_id: &[u8; 16],
        ) -> Result<Option<TraceSpans>> {
            Ok(self.trace.clone())
        }

        async fn tag_names(
            &self,
            _tenant: &str,
            _scope: Option<TagScope>,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<ScopedTag>> {
            Ok(self.tags.clone())
        }

        async fn tag_values(
            &self,
            _tenant: &str,
            _tag: &str,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<TypedValue>> {
            Ok(self.values.clone())
        }

        fn block_builder_frontier_ns(&self, tenant: &str) -> i64 {
            self.frontiers.get(tenant).copied().unwrap_or_default()
        }
    }

    fn trace() -> TraceSpans {
        TraceSpans {
            trace_id: [1; 16],
            root_service_name: "api".into(),
            root_trace_name: "GET /".into(),
            spans: vec![SpanRef {
                span_id: [2; 8],
                parent_span_id: None,
                name: "GET /".into(),
                kind: 0,
                nested_set_left: 1,
                nested_set_right: 2,
                nested_set_parent: 0,
                start_time_unix_nano: 2_000,
                duration_nanos: 50,
                status_code: 0,
                status_message: String::new(),
                instrumentation_name: String::new(),
                instrumentation_version: String::new(),
                attributes: vec![("svc".into(), AttrValue::Str("api".into()))],
            }],
        }
    }

    #[tokio::test]
    async fn live_tier_delegates_reads_to_source() {
        let mut source = FakeLiveSource {
            trace: Some(trace()),
            tags: vec![ScopedTag {
                scope: TagScope::Span,
                tags: vec!["svc".into()],
            }],
            values: vec![TypedValue {
                type_: "string".into(),
                value: "api".into(),
            }],
            ..FakeLiveSource::default()
        };
        source.frontiers.insert("tenant-a".into(), 1_500);
        let live = LiveTier::new(Arc::new(source));

        assert!(live.block_builder_frontier_ns("tenant-a") == 1_500);
        assert!(
            live.span_batches("tenant-a", 0, 5_000)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            live.trace_spans("tenant-a", &[1; 16])
                .await
                .unwrap()
                .unwrap()
                .spans
                .len()
                == 1
        );
        assert!(
            live.tag_names("tenant-a", Some(TagScope::Span), 0, 5_000)
                .await
                .unwrap()[0]
                .tags
                == vec!["svc"]
        );
        assert!(live.tag_values("tenant-a", ".svc", 0, 5_000).await.unwrap()[0].value == "api");
    }
}
