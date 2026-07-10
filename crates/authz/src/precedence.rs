//! Exhaustive-enumeration + proptest verification of `SimpleAclAuthorizer`
//! precedence against an INDEPENDENT oracle. The authorizer is a sequential pure
//! decision function (super-user bypass > deny-wins > allow > default-deny,
//! composed with Literal / Literal-`*` / Prefixed resource matching,
//! principal/host wildcards, and the one-way operation-implication table), so
//! exhaustive enumeration + proptest is the honest fit — not stateright (there
//! are no transitions). Mirrors the quota-precedence slice.
//!
//! The oracle re-derives the decision from first principles (its own matching
//! predicates + its own implication arrows), never calling the production
//! `matches_*`/`implies`, so a production regression — a dropped/flipped
//! implication arrow, broken deny-wins, or a matching bug — is caught rather
//! than spot-checked. Every case also asserts the broker (`MetadataImage`) and
//! gateway (`AclCache`) decision paths agree (no drift).

use std::{collections::HashSet, net::SocketAddr};

use crabka_metadata::{
    AclEntry, AclOperation, MetadataImage, MetadataRecord, PatternType, PermissionType,
    ResourceType,
};
use crabka_security::{AuthMethod, Principal};
use uuid::Uuid;

use crate::{AclCache, AuthorizationRequest, AuthorizationResult, Authorizer, SimpleAclAuthorizer};

// ----- independent oracle (separate source of truth) -----

/// Independent re-derivation of the ACL decision. Uses its OWN matching
/// predicates and its OWN implication table — never calls the production
/// `matches_*`/`implies`. Production and oracle must agree on every input.
fn oracle_decision(
    super_users: &HashSet<String>,
    entries: &[AclEntry],
    req: &AuthorizationRequest<'_>,
) -> AuthorizationResult {
    if super_users.contains(&req.principal.name) {
        return AuthorizationResult::Allow;
    }
    let mut saw_allow = false;
    let mut saw_deny = false;
    for e in entries {
        if oracle_resource_match(e, req.resource_type, req.resource_name)
            && oracle_principal_match(e, &req.principal.name)
            && oracle_host_match(e, req.host)
            && oracle_op_match(e.operation, req.operation)
        {
            match e.permission_type {
                PermissionType::Deny => saw_deny = true,
                PermissionType::Allow => saw_allow = true,
            }
        }
    }
    if saw_deny {
        AuthorizationResult::Deny
    } else if saw_allow {
        AuthorizationResult::Allow
    } else {
        AuthorizationResult::Deny
    }
}

fn oracle_resource_match(e: &AclEntry, rt: ResourceType, name: &str) -> bool {
    if e.resource_type != rt {
        return false;
    }
    match e.pattern_type {
        PatternType::Literal => e.resource_name == name || e.resource_name == "*",
        PatternType::Prefixed => name.starts_with(e.resource_name.as_str()),
    }
}

fn oracle_principal_match(e: &AclEntry, name: &str) -> bool {
    e.principal == "User:*" || e.principal == format!("User:{name}")
}

fn oracle_host_match(e: &AclEntry, host: &SocketAddr) -> bool {
    e.host == "*" || e.host == host.ip().to_string()
}

/// The one-way operation-implication table, declared independently of
/// production: exact match, `All` implies everything, and the explicit arrows
/// `{Read,Write,Delete,Alter}` -> `Describe`, `AlterConfigs` ->
/// `DescribeConfigs`.
fn oracle_op_match(stored: AclOperation, requested: AclOperation) -> bool {
    use AclOperation::{All, Alter, AlterConfigs, Delete, Describe, DescribeConfigs, Read, Write};
    // Implication arrows as an explicit data table — deliberately a different
    // structure from production's `matches!`-based `implies`, so the cross-check
    // catches a regression in either form.
    const ARROWS: &[(AclOperation, AclOperation)] = &[
        (Read, Describe),
        (Write, Describe),
        (Delete, Describe),
        (Alter, Describe),
        (AlterConfigs, DescribeConfigs),
    ];
    if stored == requested || stored == All {
        return true;
    }
    ARROWS.contains(&(stored, requested))
}

// ----- builders -----

#[allow(clippy::too_many_arguments)]
fn entry(
    rt: ResourceType,
    pattern: PatternType,
    name: &str,
    principal: &str,
    host: &str,
    op: AclOperation,
    perm: PermissionType,
) -> AclEntry {
    AclEntry {
        resource_type: rt,
        resource_name: name.into(),
        pattern_type: pattern,
        principal: principal.into(),
        host: host.into(),
        operation: op,
        permission_type: perm,
    }
}

fn principal(name: &str) -> Principal {
    Principal {
        name: name.into(),
        auth_method: AuthMethod::SaslPlain,
        groups: vec![],
    }
}

fn image_of(entries: &[AclEntry]) -> MetadataImage {
    let mut img = MetadataImage::new(Uuid::nil());
    for e in entries {
        img.apply(&MetadataRecord::V1AccessControlEntry(e.clone()));
    }
    img
}

/// Assert the real authorizer (driven through BOTH the broker `MetadataImage`
/// and the gateway `AclCache`) agrees with the oracle, and that the two sources
/// agree with each other (no broker-vs-gateway drift).
fn check(super_users: &HashSet<String>, entries: &[AclEntry], req: &AuthorizationRequest<'_>) {
    use assert2::assert;
    let auth = SimpleAclAuthorizer::new(super_users.clone());
    let image = image_of(entries);
    let cache = AclCache::new(entries.to_vec());
    let want = oracle_decision(super_users, entries, req);
    let got_image = auth.authorize(&image, req);
    let got_cache = auth.authorize(&cache, req);
    assert!(
        (got_image, got_cache) == (want, want),
        "image/cache decisions ({got_image:?}, {got_cache:?}) != oracle {want:?} for {req:?} with {} entries",
        entries.len()
    );
}

// ----- exhaustive enumeration -----

const ALICE: &str = "alice";

/// Fixed candidate pool spanning every decision dimension: Allow & Deny on the
/// same (resource, op) for deny-wins; ops that imply (Read/Write/`AlterConfigs`),
/// a leaf op (Describe), and `All`; Literal-exact / Literal-`*` / Prefixed
/// patterns; principal `User:alice` and the `User:*` wildcard; `*` and a
/// specific host; plus a non-matching decoy.
fn candidate_pool() -> Vec<AclEntry> {
    use AclOperation::{All, AlterConfigs, Describe, Read, Write};
    use PatternType::{Literal, Prefixed};
    use PermissionType::{Allow, Deny};
    use ResourceType::Topic;
    vec![
        entry(Topic, Literal, "foo", "User:alice", "*", Read, Allow), // E0
        entry(Topic, Literal, "foo", "User:alice", "*", Read, Deny),  // E1 deny-wins vs E0
        entry(Topic, Literal, "*", "User:alice", "*", Write, Allow), // E2 literal-* wildcard, Write->Describe
        entry(Topic, Prefixed, "te", "User:alice", "*", Describe, Allow), // E3 prefix, leaf op
        entry(Topic, Literal, "foo", "User:*", "*", All, Allow),     // E4 principal wildcard, All
        entry(Topic, Literal, "foo", "User:*", "*", All, Deny),      // E5 broad deny-wins
        entry(Topic, Literal, "foo", "User:alice", "10.0.0.1", Read, Allow), // E6 host-specific
        entry(
            Topic,
            Literal,
            "foo",
            "User:alice",
            "*",
            AlterConfigs,
            Allow,
        ), // E7 ->DescribeConfigs
        entry(Topic, Literal, "bar", "User:bob", "*", Read, Allow), // E8 decoy (other principal/resource)
        entry(Topic, Prefixed, "te", "User:alice", "*", Describe, Deny), // E9 prefix deny on Describe
    ]
}

/// Representative requests covering operation implication, both wildcards, both
/// patterns, principal/host filtering, and default-deny.
fn requests<'a>(
    alice: &'a Principal,
    bob: &'a Principal,
    h1: &'a SocketAddr,
    h2: &'a SocketAddr,
) -> Vec<AuthorizationRequest<'a>> {
    use AclOperation::{Create, Describe, DescribeConfigs, Read, Write};
    let r = |p: &'a Principal, h: &'a SocketAddr, name: &'a str, op| AuthorizationRequest {
        principal: p,
        host: h,
        resource_type: ResourceType::Topic,
        resource_name: name,
        operation: op,
    };
    vec![
        r(alice, h1, "foo", Read),
        r(alice, h1, "foo", Describe),
        r(alice, h1, "foo", Write),
        r(alice, h1, "team-x", Read),
        r(alice, h1, "tea", Describe),
        r(alice, h1, "foo", DescribeConfigs),
        r(alice, h1, "other", Read),
        r(alice, h2, "foo", Read),
        r(bob, h1, "foo", Read),
        r(bob, h1, "bar", Read),
        r(alice, h1, "foo", Create),
        r(alice, h1, "*", Read),
    ]
}

#[test]
fn acl_precedence_exhaustive() {
    let pool = candidate_pool();
    let k = pool.len();
    assert_eq!(k, 10, "candidate pool size drives the 2^k enumeration");

    let alice = principal(ALICE);
    let bob = principal("bob");
    let h1: SocketAddr = "10.0.0.1:9092".parse().unwrap();
    let h2: SocketAddr = "10.0.0.2:9092".parse().unwrap();
    let reqs = requests(&alice, &bob, &h1, &h2);

    let no_super: HashSet<String> = HashSet::new();
    let super_alice: HashSet<String> = std::iter::once(ALICE.to_string()).collect();

    for mask in 0u32..(1u32 << k) {
        let entries: Vec<AclEntry> = (0..k)
            .filter(|i| mask & (1 << i) != 0)
            .map(|i| pool[i].clone())
            .collect();
        for req in &reqs {
            check(&no_super, &entries, req);
            check(&super_alice, &entries, req);
        }
    }
}

#[cfg(test)]
mod fuzz {
    use std::{collections::HashSet, net::SocketAddr};

    use crabka_metadata::{AclEntry, AclOperation, PatternType, PermissionType, ResourceType};
    use proptest::prelude::*;

    use super::{check, principal};
    use crate::AuthorizationRequest;

    fn op_of(i: u8) -> AclOperation {
        use AclOperation::{
            All, Alter, AlterConfigs, ClusterAction, Create, Delete, Describe, DescribeConfigs,
            IdempotentWrite, Read, Write,
        };
        [
            All,
            Read,
            Write,
            Create,
            Delete,
            Alter,
            Describe,
            ClusterAction,
            DescribeConfigs,
            AlterConfigs,
            IdempotentWrite,
        ][i as usize % 11]
    }
    fn rt_of(i: u8) -> ResourceType {
        use ResourceType::{Cluster, DelegationToken, Group, Topic, TransactionalId};
        [Topic, Group, Cluster, TransactionalId, DelegationToken][i as usize % 5]
    }
    fn name_of(i: u8) -> &'static str {
        ["foo", "bar", "team-x", "te", "*", "other"][i as usize % 6]
    }
    fn princ_of(i: u8) -> &'static str {
        ["User:alice", "User:bob", "User:*"][i as usize % 3]
    }
    fn host_of(i: u8) -> &'static str {
        ["*", "10.0.0.1", "10.0.0.2"][i as usize % 3]
    }

    prop_compose! {
        fn arb_entry()(
            perm in any::<bool>(), op in 0u8..11, rt in 0u8..5, name in 0u8..6,
            princ in 0u8..3, host in 0u8..3, pat in any::<bool>(),
        ) -> AclEntry {
            AclEntry {
                resource_type: rt_of(rt),
                resource_name: name_of(name).into(),
                pattern_type: if pat { PatternType::Prefixed } else { PatternType::Literal },
                principal: princ_of(princ).into(),
                host: host_of(host).into(),
                operation: op_of(op),
                permission_type: if perm { PermissionType::Deny } else { PermissionType::Allow },
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(3000))]
        #[test]
        fn authorize_matches_oracle_and_sources_agree(
            entries in proptest::collection::vec(arb_entry(), 0..12),
            req_princ in 0u8..3, req_host in 0u8..3, req_rt in 0u8..5,
            req_name in 0u8..6, req_op in 0u8..11,
            super_alice in any::<bool>(), super_bob in any::<bool>(),
        ) {
            // Request principal name drawn from {alice, bob, carol}; carol is in
            // no ACL, exercising default-deny + a non-matching principal.
            let pname = ["alice", "bob", "carol"][req_princ as usize % 3];
            let p = principal(pname);
            let host: SocketAddr = format!(
                "{}:9092",
                ["10.0.0.1", "10.0.0.2", "10.0.0.9"][req_host as usize % 3]
            )
            .parse()
            .unwrap();
            let req = AuthorizationRequest {
                principal: &p,
                host: &host,
                resource_type: rt_of(req_rt),
                resource_name: name_of(req_name),
                operation: op_of(req_op),
            };
            let mut su: HashSet<String> = HashSet::new();
            if super_alice {
                su.insert("alice".into());
            }
            if super_bob {
                su.insert("bob".into());
            }
            // `check` asserts real(image) == oracle AND real(cache) == oracle (so
            // image == cache too), via assert2 panics that proptest shrinks on.
            check(&su, &entries, &req);
        }
    }
}
