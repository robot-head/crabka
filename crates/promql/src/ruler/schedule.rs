use std::collections::BTreeMap;

use super::{
    RulerGroupState,
    config::{stable_hash_parts, yaml_duration_ms},
};
use crate::PromqlError;

/// One ruler shard for deterministic rule-group ownership.
///
/// Shards are one-based to match Mimir's shard notation: `1_of_3`, `2_of_3`, ...
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RulerShard {
    pub index: usize,
    pub total: usize,
}

impl RulerShard {
    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn new(index: usize, total: usize) -> Result<Self, PromqlError> {
        if total == 0 {
            return Err(PromqlError::Plan(
                "ruler shard total must be positive".into(),
            ));
        }
        if index == 0 || index > total {
            return Err(PromqlError::Plan(format!(
                "ruler shard index must be between 1 and {total}"
            )));
        }
        Ok(Self { index, total })
    }

    #[must_use]
    pub fn owns_group(self, tenant: &str, namespace: &str, group_name: &str) -> bool {
        let buckets = self.total as u64;
        let shard_index =
            usize::try_from(stable_hash_parts(&[tenant, namespace, group_name]) % buckets)
                .unwrap_or(0);
        shard_index == self.index - 1
    }
}

/// Return the rule groups owned by one ruler shard for a tenant.
#[must_use]
pub fn filter_ruler_rule_set_for_shard(
    tenant: &str,
    rules: &BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    shard: RulerShard,
) -> BTreeMap<String, BTreeMap<String, serde_yaml::Value>> {
    let mut filtered = BTreeMap::new();
    for (namespace, namespace_groups) in rules {
        let groups = namespace_groups
            .iter()
            .filter(|(group_name, _)| shard.owns_group(tenant, namespace, group_name))
            .map(|(group_name, group)| (group_name.clone(), group.clone()))
            .collect::<BTreeMap<_, _>>();
        if !groups.is_empty() {
            filtered.insert(namespace.clone(), groups);
        }
    }
    filtered
}

/// Return rule groups whose configured interval has elapsed.
#[must_use]
pub fn filter_ruler_rule_set_due_for_eval(
    tenant: &str,
    rules: &BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    group_state: &RulerGroupState,
    eval_time_ms: i64,
) -> BTreeMap<String, BTreeMap<String, serde_yaml::Value>> {
    let mut filtered = BTreeMap::new();
    for (namespace, namespace_groups) in rules {
        let groups = namespace_groups
            .iter()
            .filter(|(group_name, group)| {
                ruler_group_due_for_eval(
                    tenant,
                    namespace,
                    group_name,
                    group,
                    group_state,
                    eval_time_ms,
                )
            })
            .map(|(group_name, group)| (group_name.clone(), group.clone()))
            .collect::<BTreeMap<_, _>>();
        if !groups.is_empty() {
            filtered.insert(namespace.clone(), groups);
        }
    }
    filtered
}

/// Return rule groups owned by one shard whose configured interval has elapsed.
#[must_use]
pub fn filter_ruler_rule_set_for_shard_due_for_eval(
    tenant: &str,
    rules: &BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    group_state: &RulerGroupState,
    shard: RulerShard,
    eval_time_ms: i64,
) -> BTreeMap<String, BTreeMap<String, serde_yaml::Value>> {
    let sharded = filter_ruler_rule_set_for_shard(tenant, rules, shard);
    filter_ruler_rule_set_due_for_eval(tenant, &sharded, group_state, eval_time_ms)
}

fn ruler_group_due_for_eval(
    tenant: &str,
    namespace: &str,
    group_name: &str,
    group: &serde_yaml::Value,
    group_state: &RulerGroupState,
    eval_time_ms: i64,
) -> bool {
    let Some(last_eval_ms) = group_state.last_eval_ms(tenant, namespace, group_name) else {
        return true;
    };
    // A malformed `interval` is a config error; skip the group rather than
    // treating an unparseable value as `0` and re-evaluating every tick. The
    // `for`/`expr` paths surface the same parse error as a hard failure.
    let Ok(interval_ms) = yaml_duration_ms(group, "interval") else {
        return false;
    };
    eval_time_ms.saturating_sub(last_eval_ms) >= interval_ms
}
