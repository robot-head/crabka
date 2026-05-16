use std::time::Duration;

use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use k8s_openapi::chrono::Utc;
use kube::Client;
use kube::api::{Api, Patch, PatchParams, PostParams};

const LEASE_DURATION_SECS: i32 = 15;
const RETRY: Duration = Duration::from_secs(2);

/// Block until this process holds the Lease.
///
/// Simplistic implementation: poll every 2s, try to take the Lease if it
/// is unowned or expired, otherwise wait. Sufficient for slice 17's
/// scaffold; KIP-style precise election can be a follow-up if needed.
///
/// # Errors
///
/// Returns an error if the Kubernetes API call fails for a reason other
/// than a 409 create-race (which is handled internally by retrying).
pub async fn acquire(
    client: Client,
    namespace: &str,
    name: &str,
    identity: &str,
) -> anyhow::Result<()> {
    let api: Api<Lease> = Api::namespaced(client, namespace);
    loop {
        match api.get_opt(name).await? {
            None => {
                // Create the Lease with us as holder.
                let lease = Lease {
                    metadata: ObjectMeta {
                        name: Some(name.into()),
                        ..Default::default()
                    },
                    spec: Some(LeaseSpec {
                        holder_identity: Some(identity.into()),
                        lease_duration_seconds: Some(LEASE_DURATION_SECS),
                        acquire_time: Some(MicroTime(Utc::now())),
                        renew_time: Some(MicroTime(Utc::now())),
                        lease_transitions: Some(1),
                    }),
                };
                match api.create(&PostParams::default(), &lease).await {
                    Ok(_) => {
                        tracing::info!(%name, %identity, "acquired lease (created)");
                        return Ok(());
                    }
                    Err(kube::Error::Api(e)) if e.code == 409 => {
                        // Race; another replica created it. Retry.
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Some(existing) => {
                if held_by_us(&existing, identity) {
                    tracing::info!(%name, %identity, "re-confirmed lease ownership");
                    return Ok(());
                }
                if is_expired(&existing) {
                    // Try to take over.
                    let patch = serde_json::json!({
                        "spec": {
                            "holderIdentity": identity,
                            "leaseDurationSeconds": LEASE_DURATION_SECS,
                            "acquireTime": MicroTime(Utc::now()),
                            "renewTime": MicroTime(Utc::now()),
                        }
                    });
                    if api
                        .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
                        .await
                        .is_ok()
                    {
                        tracing::info!(%name, %identity, "acquired expired lease");
                        return Ok(());
                    }
                }
                tracing::debug!(%name, "lease held by another replica, waiting");
                tokio::time::sleep(RETRY).await;
            }
        }
    }
}

fn held_by_us(lease: &Lease, identity: &str) -> bool {
    lease
        .spec
        .as_ref()
        .and_then(|s| s.holder_identity.as_deref())
        == Some(identity)
}

fn is_expired(lease: &Lease) -> bool {
    let Some(spec) = lease.spec.as_ref() else {
        return true;
    };
    let Some(renew) = spec.renew_time.as_ref() else {
        return true;
    };
    let dur = i64::from(spec.lease_duration_seconds.unwrap_or(LEASE_DURATION_SECS));
    Utc::now().signed_duration_since(renew.0).num_seconds() > dur
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::chrono;

    fn lease_with(holder: &str, renew: chrono::DateTime<chrono::Utc>) -> Lease {
        Lease {
            metadata: ObjectMeta::default(),
            spec: Some(LeaseSpec {
                holder_identity: Some(holder.into()),
                lease_duration_seconds: Some(15),
                acquire_time: Some(MicroTime(renew)),
                renew_time: Some(MicroTime(renew)),
                lease_transitions: Some(1),
            }),
        }
    }

    #[test]
    fn held_by_us_matches_identity() {
        let l = lease_with("me", Utc::now());
        assert!(held_by_us(&l, "me"));
        assert!(!held_by_us(&l, "someone-else"));
    }

    #[test]
    fn expiry_uses_renew_time() {
        let stale = Utc::now() - chrono::Duration::seconds(60);
        let fresh = Utc::now();
        assert!(is_expired(&lease_with("x", stale)));
        assert!(!is_expired(&lease_with("x", fresh)));
    }
}
