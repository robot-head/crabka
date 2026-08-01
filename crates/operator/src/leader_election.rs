use crabka_units::{Time, convert::TimeExt as _};
use k8s_openapi::{
    api::coordination::v1::{Lease, LeaseSpec},
    apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta},
    jiff,
};
use kube::{
    Client,
    api::{Api, Patch, PatchParams, PostParams},
};

fn now() -> jiff::Timestamp {
    jiff::Timestamp::now()
}

/// The Lease duration as k8s's `leaseDurationSeconds` (`Option<i32>`). The
/// field belongs to `k8s_openapi`'s generated `LeaseSpec`, so the validated
/// extent narrows to whole seconds here.
fn lease_duration_seconds(extent: Time) -> anyhow::Result<i32> {
    i32::try_from(extent.secs_i64()).map_err(Into::into)
}

/// Block until this process holds the Lease.
///
/// Simplistic implementation: poll every 2s, try to take the Lease if it
/// is unowned or expired, otherwise wait. KIP-style precise election
/// can be a follow-up if needed.
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
    lease_duration: Time,
    retry_interval: Time,
) -> anyhow::Result<()> {
    let api: Api<Lease> = Api::namespaced(client, namespace);
    let lease_duration_seconds = lease_duration_seconds(lease_duration)?;
    loop {
        match api.get_opt(name).await? {
            None => {
                let lease = Lease {
                    metadata: ObjectMeta {
                        name: Some(name.into()),
                        ..Default::default()
                    },
                    spec: Some(LeaseSpec {
                        holder_identity: Some(identity.into()),
                        lease_duration_seconds: Some(lease_duration_seconds),
                        acquire_time: Some(MicroTime(now())),
                        renew_time: Some(MicroTime(now())),
                        lease_transitions: Some(1),
                        ..Default::default()
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
                if is_expired(&existing, lease_duration) {
                    let patch = serde_json::json!({
                        "spec": {
                            "holderIdentity": identity,
                            "leaseDurationSeconds": lease_duration_seconds,
                            "acquireTime": MicroTime(now()),
                            "renewTime": MicroTime(now()),
                        }
                    });
                    match api
                        .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
                        .await
                    {
                        Ok(_) => {
                            tracing::info!(%name, %identity, "acquired expired lease");
                            return Ok(());
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, %name, "takeover patch failed; will retry");
                        }
                    }
                }
                tracing::debug!(%name, "lease held by another replica, waiting");
                tokio::time::sleep(retry_interval.to_std()).await;
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

fn is_expired(lease: &Lease, fallback_duration: Time) -> bool {
    let Some(spec) = lease.spec.as_ref() else {
        return true;
    };
    let Some(renew) = spec.renew_time.as_ref() else {
        return true;
    };
    let lease_duration = spec
        .lease_duration_seconds
        .map_or(fallback_duration, |raw| Time::from_secs(i64::from(raw)));
    let elapsed = Time::from_secs(now().as_second() - renew.0.as_second());
    elapsed > lease_duration
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_units::secs;

    use super::*;

    fn lease_with(holder: &str, renew: jiff::Timestamp) -> Lease {
        Lease {
            metadata: ObjectMeta::default(),
            spec: Some(LeaseSpec {
                holder_identity: Some(holder.into()),
                lease_duration_seconds: Some(15),
                acquire_time: Some(MicroTime(renew)),
                renew_time: Some(MicroTime(renew)),
                lease_transitions: Some(1),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn held_by_us_matches_identity() {
        let l = lease_with("me", now());
        assert!(held_by_us(&l, "me"));
        assert!(!held_by_us(&l, "someone-else"));
    }

    #[test]
    fn expiry_uses_renew_time() {
        let stale = jiff::Timestamp::from_second(now().as_second() - 60).unwrap();
        let fresh = now();
        assert!(is_expired(&lease_with("x", stale), secs(15)));
        assert!(!is_expired(&lease_with("x", fresh), secs(15)));
    }

    #[test]
    fn expiry_uses_configured_fallback_when_lease_omits_duration() {
        let renew = jiff::Timestamp::from_second(now().as_second() - 20).unwrap();
        let mut lease = lease_with("x", renew);
        lease.spec.as_mut().unwrap().lease_duration_seconds = None;
        assert!(!is_expired(&lease, secs(30)));
        assert!(is_expired(&lease, secs(10)));
    }

    #[test]
    fn lease_duration_renders_as_whole_seconds() {
        // The k8s field is `Option<i32>` seconds; the extent narrows there and
        // nowhere else.
        assert!(lease_duration_seconds(secs(15)).unwrap() == 15);
        assert!(lease_duration_seconds(crabka_units::minutes(2)).unwrap() == 120);
        assert!(lease_duration_seconds(crabka_units::days(365 * 100)).is_err());
    }
}
