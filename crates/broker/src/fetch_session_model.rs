//! Exhaustive stateright enumeration of the KIP-227 forget+merge composition
//! (`super::apply_incremental`).
//!
//! The session-cache partition map is **stateful** across incremental fetches,
//! so a real state machine fits (unlike the stateless quota lookup, which is
//! exhaustive-enum + proptest only). The model drives the REAL forget+merge over
//! sequences of incremental fetches whose topic references carry varying
//! identity halves — name-only (Fetch v ≤ 12), id-only (v ≥ 13), or both — over a
//! tiny topic/id/partition universe, starting from a fully-resolved session, and
//! asserts:
//!
//! - `no_shadow` (headline): no two cached keys ever refer to one logical
//!   partition (same partition AND a shared non-trivial identity half). A shadow
//!   would silently corrupt subsequent reads — the merge-only shadow is already
//!   fixed+tested; this targets the *composed* forget-then-merge path.
//! - **subscription fidelity** (per-transition): a subscribed partition is
//!   reflected with the requested `max_bytes`.
//! - `no_orphan_default`: no key carries default state (`max_bytes == 0`); a
//!   merge-created entry always takes the request's value.
//!
//! Note on determinism: the real merge resolves a double-match (a request whose
//! name matches one cached key and whose id matches another) via `HashMap`
//! iteration order, so `next_state` is not a pure function of `(state, action)`.
//! That is faithful to production and sound here: every possible resolution
//! satisfies the asserted invariants (which are choice-independent), and the
//! sorted projection gives identical fingerprints for identical resulting
//! content, so the explored graph is well-defined.

use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    time::Duration,
};

use crabka_protocol::{
    owned::fetch_request::{FetchPartition, FetchTopic, ForgottenTopic},
    primitives::uuid::Uuid as WireUuid,
};
use stateright::{Checker, Model, Property};

use super::{CachedPartitionState, FetchSessionKey, apply_incremental};

// Exhaustiveness is bounded on UNIQUE states (memory-proportional). The combined
// forget×sub action cross-product makes the *generated* (visited-edge) count
// ~100× the unique-state count — path convergence, not real growth — so
// `target_state_count` is only a truncation backstop set far above the natural
// generated total, and the real bound is `MAX_UNIQUE_STATES`.
const TARGET_STATE_COUNT: usize = 30_000_000;
const MAX_UNIQUE_STATES: usize = 500_000;
const MAX_DEPTH: usize = 30;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

// Tiny symbolic universe. Names {A,B}, ids {U,V} (a rename = same id, new name).
const NAME_A: &str = "A";
const NAME_B: &str = "B";
fn id_of(tag: u8) -> WireUuid {
    WireUuid([tag; 16])
}

/// A client topic reference and which identity halves it carries. `tag` 1 = id U,
/// 2 = id V; the name is one of {A, B}. `Both(B, 1)` models a rename of topic A
/// (id U) to name B; `IdOnly(1)` / `NameOnly(A)` model the half-identity wire
/// forms.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Ref {
    Both(&'static str, u8),
    NameOnly(&'static str),
    IdOnly(u8),
}
fn ref_name(r: Ref) -> String {
    match r {
        Ref::Both(n, _) | Ref::NameOnly(n) => n.to_string(),
        Ref::IdOnly(_) => String::new(),
    }
}
fn ref_id(r: Ref) -> WireUuid {
    match r {
        Ref::Both(_, t) | Ref::IdOnly(t) => id_of(t),
        Ref::NameOnly(_) => WireUuid::ZERO,
    }
}
/// Mirror of the merge find-predicate: does cached key `k` match reference `r`
/// for partition `p` (either non-empty name equal, or non-zero id equal)?
fn ref_matches(k: &FetchSessionKey, r: Ref, p: i32) -> bool {
    let name = ref_name(r);
    let id = ref_id(r);
    k.partition == p
        && ((!name.is_empty() && k.topic_name == name)
            || (id != WireUuid::ZERO && k.topic_id == id))
}

#[derive(Clone, Debug)]
struct CacheState {
    partitions: HashMap<FetchSessionKey, CachedPartitionState>,
}
impl CacheState {
    /// Canonical, hashable projection: (name, id-bytes, partition, `max_bytes`),
    /// sorted. `max_bytes` distinguishes a real entry from a default-state
    /// shadow; the other `CachedPartitionState` fields are irrelevant here.
    fn proj(&self) -> Vec<(String, [u8; 16], i32, i32)> {
        let mut v: Vec<_> = self
            .partitions
            .iter()
            .map(|(k, s)| (k.topic_name.clone(), k.topic_id.0, k.partition, s.max_bytes))
            .collect();
        v.sort();
        v
    }
}
impl PartialEq for CacheState {
    fn eq(&self, o: &Self) -> bool {
        self.proj() == o.proj()
    }
}
impl Eq for CacheState {}
impl Hash for CacheState {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.proj().hash(h);
    }
}

/// One incremental fetch: optionally forget one (ref, partition), optionally
/// subscribe one (ref, partition, `max_bytes`). A real fetch can carry both.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Fetch {
    forget: Option<(Ref, i32)>,
    sub: Option<(Ref, i32, i32)>,
}

struct FsModel {
    refs: Vec<Ref>,
    partitions: Vec<i32>,
}

fn forgotten_topic(r: Ref, p: i32) -> ForgottenTopic {
    ForgottenTopic {
        topic: ref_name(r),
        topic_id: ref_id(r),
        partitions: vec![p],
        ..Default::default()
    }
}
fn fetch_topic(r: Ref, p: i32, mb: i32) -> FetchTopic {
    FetchTopic {
        topic: ref_name(r),
        topic_id: ref_id(r),
        partitions: vec![FetchPartition {
            partition: p,
            partition_max_bytes: mb,
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// No two distinct keys refer to one logical partition: same partition AND
/// (both names non-empty & equal) OR (both ids non-zero & equal).
fn no_shadow(partitions: &HashMap<FetchSessionKey, CachedPartitionState>) -> bool {
    let keys: Vec<&FetchSessionKey> = partitions.keys().collect();
    for (i, a) in keys.iter().enumerate() {
        for b in &keys[i + 1..] {
            if a.partition == b.partition
                && ((!a.topic_name.is_empty() && a.topic_name == b.topic_name)
                    || (a.topic_id != WireUuid::ZERO && a.topic_id == b.topic_id))
            {
                return false;
            }
        }
    }
    true
}

impl Model for FsModel {
    type State = CacheState;
    type Action = Fetch;

    fn init_states(&self) -> Vec<Self::State> {
        // Seed: a fully-resolved session — one key with BOTH halves (name A,
        // id U) on the first partition, max_bytes 9 (a non-default sentinel).
        let mut partitions = HashMap::new();
        partitions.insert(
            FetchSessionKey {
                topic_name: NAME_A.to_string(),
                topic_id: id_of(1),
                partition: self.partitions[0],
            },
            CachedPartitionState {
                max_bytes: 9,
                ..Default::default()
            },
        );
        vec![CacheState { partitions }]
    }

    fn actions(&self, _s: &Self::State, actions: &mut Vec<Self::Action>) {
        let mut forgets: Vec<Option<(Ref, i32)>> = vec![None];
        for &r in &self.refs {
            for &p in &self.partitions {
                forgets.push(Some((r, p)));
            }
        }
        let mut subs: Vec<Option<(Ref, i32, i32)>> = vec![None];
        for &r in &self.refs {
            for &p in &self.partitions {
                for mb in [1, 2] {
                    subs.push(Some((r, p, mb)));
                }
            }
        }
        for &f in &forgets {
            for &s in &subs {
                if f.is_none() && s.is_none() {
                    continue; // empty fetch is a no-op; skip
                }
                actions.push(Fetch { forget: f, sub: s });
            }
        }
    }

    fn next_state(&self, last: &Self::State, a: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        let forgotten: Vec<ForgottenTopic> = a
            .forget
            .into_iter()
            .map(|(r, p)| forgotten_topic(r, p))
            .collect();
        let topics: Vec<FetchTopic> = a
            .sub
            .into_iter()
            .map(|(r, p, mb)| fetch_topic(r, p, mb))
            .collect();

        apply_incremental(&mut s.partitions, &forgotten, &topics);

        // Headline safety, per transition (fires the moment a shadow appears).
        assert!(
            no_shadow(&s.partitions),
            "shadow entry after {a:?}: {:?}",
            s.proj()
        );

        // Subscription fidelity: a subscribed partition is reflected with the
        // requested max_bytes by some key matching the request's identity.
        if let Some((r, p, mb)) = a.sub {
            let present = s
                .partitions
                .iter()
                .any(|(k, st)| ref_matches(k, r, p) && st.max_bytes == mb);
            assert!(
                present,
                "subscription not reflected after {a:?}: {:?}",
                s.proj()
            );
        }

        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("no_shadow", |_, s: &CacheState| no_shadow(&s.partitions)),
            Property::always("no_orphan_default", |_, s: &CacheState| {
                s.partitions.values().all(|v| v.max_bytes != 0)
            }),
            // Non-vacuity witnesses.
            Property::sometimes("renamed", |_, s: &CacheState| {
                s.partitions.keys().any(|k| k.topic_name == NAME_B)
            }),
            Property::sometimes("emptied", |_, s: &CacheState| s.partitions.is_empty()),
            Property::sometimes("id_only_key", |_, s: &CacheState| {
                s.partitions.keys().any(|k| k.topic_name.is_empty())
            }),
            Property::sometimes("name_only_key", |_, s: &CacheState| {
                s.partitions.keys().any(|k| k.topic_id == WireUuid::ZERO)
            }),
            Property::sometimes("two_keys", |_, s: &CacheState| s.partitions.len() >= 2),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.partitions.len() <= 8
    }
}

fn run(model: FsModel, label: &str) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(TARGET_STATE_COUNT)
        .timeout(CHECK_TIMEOUT)
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert!(checker.max_depth() < MAX_DEPTH, "[{label}] depth cap hit");
    assert!(
        checker.state_count() < TARGET_STATE_COUNT,
        "[{label}] truncated — not exhaustive"
    );
    assert!(
        checker.unique_state_count() < MAX_UNIQUE_STATES,
        "[{label}] unique-state bound exceeded ({})",
        checker.unique_state_count()
    );
    checker.assert_properties();
}

#[test]
fn fetch_session_basic() {
    // One partition; the core rename/half-identity churn alphabet.
    run(
        FsModel {
            refs: vec![
                Ref::Both(NAME_A, 1),
                Ref::NameOnly(NAME_A),
                Ref::IdOnly(1),
                Ref::Both(NAME_B, 1),
            ],
            partitions: vec![0],
        },
        "fetch_session_basic",
    );
}

#[test]
fn fetch_session_wide() {
    // One partition, the full identity-churn alphabet: both halves of topic A
    // (id U), its rename to B (same id U), a name-only and id-only form of each,
    // and a second id V (a stale/conflicting identity). forget and merge are both
    // partition-scoped (`k.partition == p`), so a single partition exercises all
    // the shadow logic; the `two_keys` witness still fires via two distinct-
    // identity topics coexisting on the one partition.
    run(
        FsModel {
            refs: vec![
                Ref::Both(NAME_A, 1),
                Ref::NameOnly(NAME_A),
                Ref::IdOnly(1),
                Ref::Both(NAME_B, 1),
                Ref::NameOnly(NAME_B),
                Ref::Both(NAME_A, 2),
                Ref::IdOnly(2),
            ],
            partitions: vec![0],
        },
        "fetch_session_wide",
    );
}
