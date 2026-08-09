use crabka_units::{Time, convert::TimeExt as _};
use k8s_openapi::{
    api::coordination::v1::{Lease, LeaseSpec},
    apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta},
    jiff,
};
use kube::{
    Client,
    api::{Api, PostParams},
};

fn now() -> jiff::Timestamp {
    jiff::Timestamp::now()
}

/// The Lease duration as the Kubernetes `leaseDurationSeconds` field, of type
/// `Option<i32>`. The field belongs to the `LeaseSpec` that `k8s_openapi`
/// generates, so the validated extent narrows to whole seconds here.
fn lease_duration_seconds(extent: Time) -> anyhow::Result<i32> {
    i32::try_from(extent.secs_i64()).map_err(Into::into)
}

/// A held Kubernetes lease. Its background task renews the lease until
/// leadership is lost or the guard is dropped.
pub struct Leadership {
    renewer: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Leadership {
    /// Wait until the lease can no longer be renewed safely.
    ///
    /// # Errors
    ///
    /// Returns the reason leadership was lost or the renewal task failed.
    pub async fn wait(&mut self) -> anyhow::Result<()> {
        (&mut self.renewer)
            .await
            .map_err(|error| anyhow::anyhow!("leader-election renewal task failed: {error}"))?
    }
}

impl Drop for Leadership {
    fn drop(&mut self) {
        self.renewer.abort();
    }
}

/// Block until this process holds the Lease, then keep it renewed.
///
/// # Errors
///
/// Returns an error if the Kubernetes API call fails for a reason other
/// than a 409 create-race. The function retries internally on a 409
/// create-race.
pub async fn acquire(
    client: Client,
    namespace: &str,
    name: &str,
    identity: &str,
    lease_duration: Time,
    retry_interval: Time,
) -> anyhow::Result<Leadership> {
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
                        break;
                    }
                    Err(kube::Error::Api(e)) if e.code == 409 => {
                        // Race; another replica created it. Retry.
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Some(mut existing) => {
                if held_by_us(&existing, identity) {
                    tracing::info!(%name, %identity, "re-confirmed lease ownership");
                    break;
                }
                if is_expired(&existing, lease_duration) {
                    claim(&mut existing, identity, lease_duration_seconds);
                    match api.replace(name, &PostParams::default(), &existing).await {
                        Ok(_) => {
                            tracing::info!(%name, %identity, "acquired expired lease");
                            break;
                        }
                        Err(kube::Error::Api(error)) if error.code == 409 => {
                            tracing::debug!(%name, "lease takeover raced; retrying");
                        }
                        Err(error) => {
                            tracing::warn!(%error, %name, "lease takeover failed; will retry");
                        }
                    }
                }
                tracing::debug!(%name, "lease held by another replica, waiting");
                tokio::time::sleep(retry_interval.to_std()).await;
            }
        }
    }

    let renew_name = name.to_owned();
    let renew_identity = identity.to_owned();
    let renewer = tokio::spawn(async move {
        renew_loop(
            api,
            renew_name,
            renew_identity,
            lease_duration,
            retry_interval,
            lease_duration_seconds,
        )
        .await
    });
    Ok(Leadership { renewer })
}

fn claim(lease: &mut Lease, identity: &str, lease_duration_seconds: i32) {
    let spec = lease.spec.get_or_insert_with(LeaseSpec::default);
    let changed_holder = spec.holder_identity.as_deref() != Some(identity);
    let timestamp = MicroTime(now());
    spec.holder_identity = Some(identity.to_owned());
    spec.lease_duration_seconds = Some(lease_duration_seconds);
    spec.acquire_time = Some(timestamp.clone());
    spec.renew_time = Some(timestamp);
    if changed_holder {
        spec.lease_transitions = Some(spec.lease_transitions.unwrap_or(0).saturating_add(1));
    }
}

async fn renew_loop(
    api: Api<Lease>,
    name: String,
    identity: String,
    lease_duration: Time,
    retry_interval: Time,
    lease_duration_seconds: i32,
) -> anyhow::Result<()> {
    let mut last_success = tokio::time::Instant::now();
    loop {
        tokio::time::sleep(retry_interval.to_std()).await;
        match renew_once(&api, &name, &identity, lease_duration_seconds).await {
            Ok(()) => last_success = tokio::time::Instant::now(),
            Err(RenewError::Lost(reason)) => {
                anyhow::bail!("lost leader-election lease {name}: {reason}");
            }
            Err(RenewError::Retry(error)) => {
                if last_success.elapsed() >= lease_duration.to_std() {
                    anyhow::bail!(
                        "could not renew leader-election lease {name} before its deadline: {error}"
                    );
                }
                tracing::warn!(%error, %name, "lease renewal failed; retrying before deadline");
            }
        }
    }
}

enum RenewError {
    Lost(String),
    Retry(kube::Error),
}

async fn renew_once(
    api: &Api<Lease>,
    name: &str,
    identity: &str,
    lease_duration_seconds: i32,
) -> Result<(), RenewError> {
    let Some(mut lease) = api.get_opt(name).await.map_err(RenewError::Retry)? else {
        return Err(RenewError::Lost("lease was deleted".to_owned()));
    };
    if !held_by_us(&lease, identity) {
        let holder = lease
            .spec
            .as_ref()
            .and_then(|spec| spec.holder_identity.as_deref())
            .unwrap_or("<none>");
        return Err(RenewError::Lost(format!("holder changed to {holder}")));
    }
    let spec = lease.spec.get_or_insert_with(LeaseSpec::default);
    spec.lease_duration_seconds = Some(lease_duration_seconds);
    spec.renew_time = Some(MicroTime(now()));
    api.replace(name, &PostParams::default(), &lease)
        .await
        .map(|_| ())
        .map_err(RenewError::Retry)
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

    #[test]
    fn claim_updates_owner_timestamps_and_transition_count() {
        let old = jiff::Timestamp::from_second(now().as_second() - 60).unwrap();
        let mut lease = lease_with("old", old);
        claim(&mut lease, "new", 30);
        let spec = lease.spec.unwrap();
        assert!(spec.holder_identity.as_deref() == Some("new"));
        assert!(spec.lease_duration_seconds == Some(30));
        assert!(spec.lease_transitions == Some(2));
        assert!(spec.acquire_time == spec.renew_time);
        assert!(spec.renew_time.unwrap().0 > old);
    }
}
