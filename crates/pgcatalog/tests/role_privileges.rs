//! `role_has_privs_of` against `role_can_set`. The two answer different
//! questions and a later slice matches a row-level-security policy's `TO` list
//! with the former, so the cases where they disagree are the point of the test.

use assert2::assert;
use crabka_pgcatalog::{
    RoleAttribute, RoleAttributes, create_role_with_memberships_ops, grant_role_memberships_ops,
    revoke_role_memberships_ops, role_can_set, role_has_privs_of,
};
use crabka_pgkv::{Kv, MemKv};

/// A role's login flag, inheritance, and the roles it is granted membership of.
struct RoleSpec {
    name: &'static str,
    inherits: bool,
    member_of: &'static [&'static str],
}

const fn role(name: &'static str, member_of: &'static [&'static str]) -> RoleSpec {
    RoleSpec {
        name,
        inherits: true,
        member_of,
    }
}

const fn noinherit(name: &'static str, member_of: &'static [&'static str]) -> RoleSpec {
    RoleSpec {
        name,
        inherits: false,
        member_of,
    }
}

fn catalog(roles: &[RoleSpec]) -> MemKv {
    let kv = MemKv::new();
    for spec in roles {
        let mut attributes = RoleAttributes::default();
        attributes.set(RoleAttribute::Inherit, spec.inherits);
        let members = spec
            .member_of
            .iter()
            .map(|name| (*name).to_string())
            .collect::<Vec<_>>();
        let ops = create_role_with_memberships_ops(&kv, spec.name, true, attributes, &members)
            .expect("create role ops");
        kv.write_batch(&ops).expect("catalog batch");
    }
    kv
}

/// What both predicates say about one `(member, role)` pair, so a case states
/// its whole expectation and the disagreements are visible side by side.
#[derive(Debug, PartialEq, Eq)]
struct Verdicts {
    can_set: bool,
    has_privs: bool,
}

fn verdicts(kv: &dyn Kv, member: &str, role: &str) -> Verdicts {
    Verdicts {
        can_set: role_can_set(kv, member, role).expect("role_can_set"),
        has_privs: role_has_privs_of(kv, member, role).expect("role_has_privs_of"),
    }
}

#[test]
fn privilege_inheritance_follows_membership_transitively() {
    let kv = catalog(&[
        role("grandparent", &[]),
        role("parent", &["grandparent"]),
        role("child", &["parent"]),
        role("stranger", &[]),
    ]);

    let cases = [
        // A role always has its own privileges and may always assume itself.
        (
            "child",
            "child",
            Verdicts {
                can_set: true,
                has_privs: true,
            },
        ),
        (
            "child",
            "parent",
            Verdicts {
                can_set: true,
                has_privs: true,
            },
        ),
        // Two hops: membership is transitive on both sides.
        (
            "child",
            "grandparent",
            Verdicts {
                can_set: true,
                has_privs: true,
            },
        ),
        // Membership does not run the other way.
        (
            "parent",
            "child",
            Verdicts {
                can_set: false,
                has_privs: false,
            },
        ),
        (
            "child",
            "stranger",
            Verdicts {
                can_set: false,
                has_privs: false,
            },
        ),
    ];

    for (member, role, expected) in cases {
        assert!(verdicts(&kv, member, role) == expected);
    }
}

/// The first place the two predicates part company: a `NOINHERIT` role may
/// still `SET ROLE` to what it is a member of, but holds none of its
/// privileges without doing so.
#[test]
fn a_noinherit_role_can_assume_a_role_whose_privileges_it_does_not_hold() {
    let kv = catalog(&[
        role("grandparent", &[]),
        role("parent", &["grandparent"]),
        noinherit("detached", &["parent"]),
        role("via_detached", &["detached"]),
    ]);

    let cases = [
        (
            "detached",
            "parent",
            Verdicts {
                can_set: true,
                has_privs: false,
            },
        ),
        (
            "detached",
            "grandparent",
            Verdicts {
                can_set: true,
                has_privs: false,
            },
        ),
        // The break is at the non-inheriting role, not below it: a member of
        // `detached` reaches `detached` but nothing beyond.
        (
            "via_detached",
            "detached",
            Verdicts {
                can_set: true,
                has_privs: true,
            },
        ),
        (
            "via_detached",
            "parent",
            Verdicts {
                can_set: true,
                has_privs: false,
            },
        ),
    ];

    for (member, role, expected) in cases {
        assert!(verdicts(&kv, member, role) == expected);
    }
}

/// The second place they part company: the bootstrap superuser may assume any
/// role, but holds only the privileges of the roles it is actually a member of.
/// A policy `TO some_role` must not match a superuser session for free.
#[test]
fn the_bootstrap_superuser_gets_no_inheritance_shortcut() {
    let kv = catalog(&[role("unrelated", &[])]);

    assert!(
        verdicts(&kv, "postgres", "unrelated")
            == Verdicts {
                can_set: true,
                has_privs: false,
            }
    );
}

/// A role reachable by two paths is reached once. The traversal visits each
/// role at most once, so a diamond terminates and still answers.
#[test]
fn a_role_reachable_by_two_paths_is_still_reached() {
    let kv = catalog(&[
        role("apex", &[]),
        role("left", &["apex"]),
        noinherit("right", &["apex"]),
        role("base", &["left", "right"]),
    ]);

    // `left` inherits, so `apex` is reached through it even though the `right`
    // branch stops at a non-inheriting role.
    assert!(
        verdicts(&kv, "base", "apex")
            == Verdicts {
                can_set: true,
                has_privs: true,
            }
    );
}

/// `GRANT <role> TO <member>` writes the record `CREATE ROLE … IN ROLE` writes,
/// so a membership made either way reaches both predicates — including through
/// a role that was itself admitted by the other spelling.
#[test]
fn granted_membership_is_the_same_record_as_in_role() {
    let kv = catalog(&[
        role("apex", &[]),
        role("middle", &["apex"]),
        role("base", &[]),
    ]);
    assert!(
        verdicts(&kv, "base", "apex")
            == Verdicts {
                can_set: false,
                has_privs: false,
            }
    );

    let ops = grant_role_memberships_ops(&kv, &["middle".into()], &["base".into()])
        .expect("grant role ops");
    kv.write_batch(&ops).expect("catalog batch");
    // The new edge is traversed exactly like an `IN ROLE` one, so `apex` is
    // reached transitively through the `IN ROLE` edge above it.
    assert!(
        verdicts(&kv, "base", "apex")
            == Verdicts {
                can_set: true,
                has_privs: true,
            }
    );

    let ops = revoke_role_memberships_ops(&kv, &["middle".into()], &["base".into()])
        .expect("revoke role ops");
    kv.write_batch(&ops).expect("catalog batch");
    assert!(
        verdicts(&kv, "base", "apex")
            == Verdicts {
                can_set: false,
                has_privs: false,
            }
    );
}

/// Re-granting an existing membership and revoking one that was never granted
/// are both no-ops, as they are in `PostgreSQL`; only an unknown role is an
/// error, on either side of the `TO`.
#[test]
fn repeat_grants_are_idempotent_and_unknown_roles_are_refused() {
    let kv = catalog(&[role("apex", &[]), role("base", &["apex"])]);
    let roles = ["apex".to_string()];
    let members = ["base".to_string()];

    let ops = grant_role_memberships_ops(&kv, &roles, &members).expect("regrant");
    kv.write_batch(&ops).expect("catalog batch");
    assert!(role_has_privs_of(&kv, "base", "apex").expect("has privs"));

    let ops = revoke_role_memberships_ops(&kv, &roles, &members).expect("revoke");
    kv.write_batch(&ops).expect("catalog batch");
    let ops = revoke_role_memberships_ops(&kv, &roles, &members).expect("revoke again");
    kv.write_batch(&ops).expect("catalog batch");
    assert!(!role_has_privs_of(&kv, "base", "apex").expect("has privs"));

    for (roles, members) in [
        (["ghost".to_string()], ["base".to_string()]),
        (["apex".to_string()], ["ghost".to_string()]),
    ] {
        assert!(
            grant_role_memberships_ops(&kv, &roles, &members)
                .expect_err("unknown role")
                .sqlstate()
                == "42704"
        );
        assert!(
            revoke_role_memberships_ops(&kv, &roles, &members)
                .expect_err("unknown role")
                .sqlstate()
                == "42704"
        );
    }
}
