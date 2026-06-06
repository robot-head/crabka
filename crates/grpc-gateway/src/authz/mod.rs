//! Gateway trusted-proxy authorization: holds the `crabka_authz::Authorizer`
//! and an `ArcSwap`'d `AclCache` refreshed by polling the broker's `DescribeAcls`.

pub mod auth_layer;
pub use auth_layer::{BearerValidator, anonymous, resolve_principal};

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use crabka_authz::{AclCache, Authorizer};
use crabka_client_admin::AdminClient;
use tokio_util::sync::CancellationToken;

pub struct GatewayAuthz {
    authorizer: Arc<dyn Authorizer>,
    cache: ArcSwap<AclCache>,
}

impl GatewayAuthz {
    #[must_use]
    pub fn new(authorizer: Arc<dyn Authorizer>) -> Self {
        Self {
            authorizer,
            cache: ArcSwap::from_pointee(AclCache::default()),
        }
    }

    #[must_use]
    pub fn authorizer(&self) -> &Arc<dyn Authorizer> {
        &self.authorizer
    }

    #[must_use]
    pub fn cache(&self) -> arc_swap::Guard<Arc<AclCache>> {
        self.cache.load()
    }

    /// Poll `DescribeAcls` into the cache until `shutdown`. Logs + keeps the prior
    /// snapshot on error.
    pub async fn run_acl_refresh(
        self: Arc<Self>,
        bootstrap: String,
        refresh: Duration,
        shutdown: CancellationToken,
    ) {
        let addrs: Vec<String> = bootstrap.split(',').map(|s| s.trim().to_string()).collect();
        loop {
            match Self::fetch(&addrs).await {
                Ok(entries) => self.cache.store(Arc::new(AclCache::new(entries))),
                Err(e) => {
                    tracing::warn!(error = %e, "ACL refresh failed; keeping prior snapshot");
                }
            }
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(refresh) => {}
            }
        }
    }

    async fn fetch(
        addrs: &[String],
    ) -> Result<Vec<crabka_metadata::AclEntry>, crate::error::GatewayError> {
        let mut admin = AdminClient::connect(addrs)
            .await
            .map_err(|e| crate::error::GatewayError::Other(format!("acl admin connect: {e}")))?;
        let entries = admin
            .describe_acls(&crabka_client_admin::AclEntryFilter::default())
            .await
            .map_err(|e| crate::error::GatewayError::Other(format!("describe_acls: {e}")))?;
        Ok(entries.into_iter().map(acl_entry_from_admin).collect())
    }
}

/// Convert a `crabka_client_admin::AclEntry` into a `crabka_metadata::AclEntry`.
/// The two types have structurally identical field names and enum variant sets —
/// the admin crate keeps local copies to avoid a broker dependency.
fn acl_entry_from_admin(e: crabka_client_admin::AclEntry) -> crabka_metadata::AclEntry {
    use crabka_client_admin::{
        AclOperation as AO, PatternType as PT, PermissionType as Perm, ResourceType as RT,
    };
    use crabka_metadata::{
        AclEntry as ME, AclOperation as MAO, PatternType as MPT, PermissionType as MPerm,
        ResourceType as MRT,
    };

    let resource_type = match e.resource_type {
        RT::Topic => MRT::Topic,
        RT::Group => MRT::Group,
        RT::Cluster => MRT::Cluster,
        RT::TransactionalId => MRT::TransactionalId,
    };
    let pattern_type = match e.pattern_type {
        PT::Literal => MPT::Literal,
        PT::Prefixed => MPT::Prefixed,
    };
    let operation = match e.operation {
        AO::All => MAO::All,
        AO::Read => MAO::Read,
        AO::Write => MAO::Write,
        AO::Create => MAO::Create,
        AO::Delete => MAO::Delete,
        AO::Alter => MAO::Alter,
        AO::Describe => MAO::Describe,
        AO::ClusterAction => MAO::ClusterAction,
        AO::DescribeConfigs => MAO::DescribeConfigs,
        AO::AlterConfigs => MAO::AlterConfigs,
        AO::IdempotentWrite => MAO::IdempotentWrite,
    };
    let permission_type = match e.permission_type {
        Perm::Allow => MPerm::Allow,
        Perm::Deny => MPerm::Deny,
    };

    ME {
        resource_type,
        resource_name: e.resource_name,
        pattern_type,
        principal: e.principal,
        host: e.host,
        operation,
        permission_type,
    }
}
