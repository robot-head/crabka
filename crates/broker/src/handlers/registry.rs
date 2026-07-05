//! Broker API dispatch registry.

use bytes::Bytes;
use crabka_protocol::api_key::ApiKey;
use futures_util::future::BoxFuture;

use crate::{
    broker::Broker,
    error::BrokerError,
    handlers::{ApiKeyCode, ApiVersion, CorrelationId},
};

pub(crate) type PlainHandler =
    fn(&Broker, ApiVersion, CorrelationId, &[u8]) -> BoxFuture<'static, Result<Bytes, BrokerError>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RequestQuotaPolicy {
    ApplyFallbackAccounting,
    InlineExempt,
    SelfAccounted,
}

#[derive(Clone, Copy)]
pub(crate) enum DispatchKind {
    Plain(PlainHandler),
}

#[derive(Clone, Copy)]
pub(crate) struct DispatchEntry {
    api_key: ApiKey,
    flexible_min: ApiVersion,
    quota_policy: RequestQuotaPolicy,
    kind: DispatchKind,
}

#[derive(Default)]
pub(crate) struct DispatchRegistry {
    table: std::collections::HashMap<ApiKeyCode, DispatchEntry>,
}

impl DispatchEntry {
    pub(crate) fn plain(api_key: ApiKey, flexible_min: ApiVersion, handler: PlainHandler) -> Self {
        Self {
            api_key,
            flexible_min,
            quota_policy: RequestQuotaPolicy::ApplyFallbackAccounting,
            kind: DispatchKind::Plain(handler),
        }
    }

    pub(crate) fn api_key(self) -> ApiKey {
        self.api_key
    }

    pub(crate) fn kind(self) -> DispatchKind {
        self.kind
    }

    pub(crate) fn quota_policy(self) -> RequestQuotaPolicy {
        self.quota_policy
    }

    pub(crate) fn body_flexible(self, version: ApiVersion) -> bool {
        self.flexible_min != i16::MAX && version >= self.flexible_min
    }

    pub(crate) fn is_plain(self) -> bool {
        matches!(self.kind, DispatchKind::Plain(_))
    }
}

impl DispatchRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn register(&mut self, entry: DispatchEntry) -> bool {
        self.table.insert(entry.api_key as i16, entry).is_none()
    }

    pub(crate) fn get(&self, api_key: ApiKeyCode) -> Option<DispatchEntry> {
        self.table.get(&api_key).copied()
    }

    pub(crate) fn get_plain(&self, api_key: ApiKeyCode) -> Option<PlainHandler> {
        match self.get(api_key)?.kind {
            DispatchKind::Plain(handler) => Some(handler),
        }
    }
}

macro_rules! register_plain {
    ($registry:ident, $api:ident, $request:ident, $handler:ident) => {{
        $registry.register(DispatchEntry::plain(
            ApiKey::$api,
            crabka_protocol::owned::$request::FLEXIBLE_MIN,
            crate::handlers::$handler::handle,
        ));
    }};
}

pub(crate) fn build_registry() -> DispatchRegistry {
    let mut registry = DispatchRegistry::new();

    register_plain!(registry, ApiVersions, api_versions_request, api_versions);
    registry.register(DispatchEntry::plain(
        ApiKey::AddOffsetsToTxn,
        crabka_protocol::owned::add_offsets_to_txn_request::FLEXIBLE_MIN,
        crate::txn::handlers::add_offset_commits_to_txn::handle,
    ));
    registry.register(DispatchEntry::plain(
        ApiKey::WriteTxnMarkers,
        crabka_protocol::owned::write_txn_markers_request::FLEXIBLE_MIN,
        crate::txn::handlers::write_txn_markers::handle,
    ));
    register_plain!(
        registry,
        FetchSnapshot,
        fetch_snapshot_request,
        fetch_snapshot
    );
    register_plain!(
        registry,
        ConsumerGroupDescribe,
        consumer_group_describe_request,
        consumer_group_describe
    );
    register_plain!(
        registry,
        AssignReplicasToDirs,
        assign_replicas_to_dirs_request,
        assign_replicas_to_dirs
    );
    registry.register(DispatchEntry::plain(
        ApiKey::InitializeShareGroupState,
        crabka_protocol::owned::initialize_share_group_state_request::FLEXIBLE_MIN,
        crate::share_coordinator::handlers::initialize::handle,
    ));
    registry.register(DispatchEntry::plain(
        ApiKey::ReadShareGroupState,
        crabka_protocol::owned::read_share_group_state_request::FLEXIBLE_MIN,
        crate::share_coordinator::handlers::read::handle,
    ));
    registry.register(DispatchEntry::plain(
        ApiKey::WriteShareGroupState,
        crabka_protocol::owned::write_share_group_state_request::FLEXIBLE_MIN,
        crate::share_coordinator::handlers::write::handle,
    ));
    registry.register(DispatchEntry::plain(
        ApiKey::DeleteShareGroupState,
        crabka_protocol::owned::delete_share_group_state_request::FLEXIBLE_MIN,
        crate::share_coordinator::handlers::delete::handle,
    ));
    registry.register(DispatchEntry::plain(
        ApiKey::ReadShareGroupStateSummary,
        crabka_protocol::owned::read_share_group_state_summary_request::FLEXIBLE_MIN,
        crate::share_coordinator::handlers::read_summary::handle,
    ));
    register_plain!(
        registry,
        StreamsGroupDescribe,
        streams_group_describe_request,
        streams_group_describe
    );

    registry
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::handlers;

    #[test]
    fn registry_registers_plain_handlers() {
        let registry = build_registry();

        let api_versions = registry
            .get(ApiKey::ApiVersions as i16)
            .expect("ApiVersions");
        assert!(api_versions.is_plain());
        assert!(api_versions.quota_policy() == RequestQuotaPolicy::ApplyFallbackAccounting);
        assert!(api_versions.body_flexible(3));
        assert!(!api_versions.body_flexible(2));

        for key in [25, 27, 59, 69, 73, 83, 84, 85, 86, 87, 89] {
            let entry = registry
                .get(key)
                .unwrap_or_else(|| panic!("registered api_key {key}"));
            assert!(entry.is_plain(), "api_key {key}");
        }
    }

    #[test]
    fn registry_reports_missing_keys() {
        let registry = build_registry();

        assert!(registry.get(9999).is_none());
    }

    #[test]
    fn plain_handler_pointer_matches_existing_api_versions_handler() {
        let registry = build_registry();
        let handler = registry
            .get_plain(ApiKey::ApiVersions as i16)
            .expect("plain ApiVersions handler");

        assert!(std::ptr::fn_addr_eq(
            handler,
            handlers::api_versions::handle as PlainHandler
        ));
    }
}
