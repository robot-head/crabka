//! Durable row-level-security policy records — the storage behind `pg_policy`.
//!
//! Nothing here is reachable from SQL. No parser production and no executor DDL
//! arm writes a policy, so the SQL surface still rejects `CREATE POLICY` and
//! `ALTER TABLE … ENABLE ROW LEVEL SECURITY` exactly as it did before this
//! module existed. That is deliberate: a policy that can be *created* but is
//! not yet *enforced* returns the rows it is supposed to hide, which is
//! strictly worse than a syntax error. Enforcement is a later slice; this
//! module is storage and its own tests.
//!
//! A policy's `USING` and `WITH CHECK` quals are stored as **source text**, not
//! as a parsed expression — the same choice [`crate::CheckConstraint::expr`]
//! and [`crate::trigger::Trigger::when`] make. Two reasons: the catalog does
//! not depend on the parser, and `pg_get_expr` has to hand the text back
//! anyway. The enforcement path re-parses it per statement, so a policy never
//! carries a stale plan.
//!
//! Keys are `table_id`-scoped, never name-scoped:
//! `catalog_policy/<table_id BE>/<policy name>`. Every policy of one relation
//! is therefore one prefix scan, and — the property that matters — a policy
//! cannot be stranded by `ALTER TABLE … RENAME`, because a table id survives a
//! rename and the key holds no relation name to go stale. A stranded policy
//! silently stops protecting its relation, so the fix is to make the situation
//! unrepresentable rather than to remember to move keys.

use crabka_pgkv::{Kv, KvError, WriteOp};
use zerocopy::{FromBytes, IntoBytes, byteorder::big_endian::U32};

use crate::{
    CatalogError, TableId,
    serde::{read_string, take_n, take_u8, write_str},
};

/// The first OID handed to a row-security policy. Sits one band above the
/// event-trigger band ([`crate::trigger::EVENT_TRIGGER_OID_BASE`]) so a
/// `pg_policy.oid` never collides with another catalog object's.
pub const POLICY_OID_BASE: u32 = 170_000;

/// The command a policy applies to — `pg_policy.polcmd`.
///
/// [`Self::All`] is not a shorthand for the other four: `PostgreSQL` applies an
/// `ALL` policy to every command *in addition to* any command-specific policy,
/// so the two are separate rows that both take part in the fold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyCommand {
    All,
    Select,
    Insert,
    Update,
    Delete,
}

impl PolicyCommand {
    /// The `pg_policy.polcmd` letter.
    #[must_use]
    pub const fn catalog_code(self) -> char {
        match self {
            Self::All => '*',
            Self::Select => 'r',
            Self::Insert => 'a',
            Self::Update => 'w',
            Self::Delete => 'd',
        }
    }

    /// The command word as `CREATE POLICY … FOR <cmd>` spells it.
    #[must_use]
    pub const fn keyword(self) -> &'static str {
        match self {
            Self::All => "ALL",
            Self::Select => "SELECT",
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }
}

/// A stored row-security policy: one `pg_policy` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Policy {
    /// `pg_policy.oid`, allocated from [`POLICY_OID_BASE`]. Zero on a record
    /// handed to [`create_policy_ops`], which allocates one regardless of what
    /// it was given.
    pub oid: u32,
    /// `pg_policy.polname`. Unique per relation, not per database.
    pub name: String,
    /// The relation the policy governs — `pg_policy.polrelid`.
    pub table_id: TableId,
    /// `pg_policy.polcmd`.
    pub command: PolicyCommand,
    /// `pg_policy.polpermissive`: true for `AS PERMISSIVE` (the default), which
    /// ORs into the row's visibility, false for `AS RESTRICTIVE`, which ANDs
    /// onto it. A restrictive policy can only ever remove rows.
    pub permissive: bool,
    /// `pg_policy.polroles`: the roles named by `TO`.
    ///
    /// **An empty list means `PUBLIC`** — the policy applies to every role.
    /// That is `PostgreSQL`'s encoding too (`polroles` holds the zero OID), and
    /// it is why the default-deny fold must never treat "no roles listed" as
    /// "no role matches".
    pub roles: Vec<String>,
    /// Source text of the `USING` qual, without its enclosing parentheses.
    /// `None` when the policy has none, which for a permissive policy means it
    /// contributes nothing to the read fold.
    pub using: Option<String>,
    /// Source text of the `WITH CHECK` qual, without its enclosing parentheses.
    /// `None` when the policy has none; `PostgreSQL` then checks written rows
    /// against [`Self::using`] instead, for the commands that write.
    pub with_check: Option<String>,
}

impl Policy {
    /// True when the policy applies to every role, i.e. its role list is empty.
    ///
    /// The convention is easy to invert by accident, so it is spelled out here
    /// rather than re-derived at each use site.
    #[must_use]
    pub fn applies_to_public(&self) -> bool {
        self.roles.is_empty()
    }
}

/// The fields `ALTER POLICY` can rewrite. `None` leaves the stored value alone.
///
/// The struct is deliberately narrower than [`Policy`]: `PostgreSQL` has no
/// syntax for changing a policy's command or its permissive/restrictive kind
/// after creation, and it has no syntax for *removing* a qual either — only for
/// replacing one. Leaving those fields out means a mis-built `ALTER` cannot
/// widen a restrictive policy into a permissive one, rather than that being a
/// validation the catalog has to remember to run.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PolicyChange {
    /// A new `TO` list. An empty vector is a meaningful value — `TO PUBLIC`.
    pub roles: Option<Vec<String>>,
    /// New `USING` qual source.
    pub using: Option<String>,
    /// New `WITH CHECK` qual source.
    pub with_check: Option<String>,
}

fn policy_prefix() -> Vec<u8> {
    b"\0\0\0\0catalog_policy/".to_vec()
}

fn policy_table_prefix(table_id: TableId) -> Vec<u8> {
    let mut prefix = policy_prefix();
    prefix.extend_from_slice(&table_id.to_be_bytes());
    prefix.push(b'/');
    prefix
}

fn policy_key(table_id: TableId, name: &str) -> Vec<u8> {
    let mut key = policy_table_prefix(table_id);
    key.extend_from_slice(name.as_bytes());
    key
}

fn next_policy_oid_key() -> Vec<u8> {
    b"\0\0\0\0meta/next_policy_oid".to_vec()
}

fn read_oid_counter(kv: &dyn Kv) -> Result<u32, CatalogError> {
    match kv.get(&next_policy_oid_key())? {
        Some(bytes) => {
            let (value, _) = U32::read_from_prefix(bytes.as_slice())
                .map_err(|_| KvError::CorruptRow("policy oid counter is not u32".into()))?;
            Ok(value.get())
        }
        None => Ok(POLICY_OID_BASE),
    }
}

/// The OID the next policy created will be given.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn next_policy_oid(kv: &dyn Kv) -> Result<u32, CatalogError> {
    read_oid_counter(kv)
}

/// The relation name a policy error names. `PostgreSQL` reports the bare
/// relation name here, not a schema-qualified one.
fn relation_label(kv: &dyn Kv, table_id: TableId) -> Result<String, CatalogError> {
    Ok(crate::relation_name_of(kv, table_id)?
        .map_or_else(|| format!("table id {table_id}"), |name| name.name))
}

/// Look up one policy by relation and name.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn get_policy(
    kv: &dyn Kv,
    table_id: TableId,
    name: &str,
) -> Result<Option<Policy>, CatalogError> {
    kv.get(&policy_key(table_id, name))?
        .map(|bytes| deserialize_policy(&bytes).map_err(CatalogError::from))
        .transpose()
}

/// Every policy attached to one relation, in name order.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn policies_for_table(kv: &dyn Kv, table_id: TableId) -> Result<Vec<Policy>, CatalogError> {
    let mut policies: Vec<_> = kv
        .scan_prefix(&policy_table_prefix(table_id))?
        .into_iter()
        .map(|(_, bytes)| deserialize_policy(&bytes).map_err(CatalogError::from))
        .collect::<Result<_, _>>()?;
    policies.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(policies)
}

/// Every policy in the database, for the `pg_policy` projection.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn list_policies(kv: &dyn Kv) -> Result<Vec<Policy>, CatalogError> {
    kv.scan_prefix(&policy_prefix())?
        .into_iter()
        .map(|(_, bytes)| deserialize_policy(&bytes).map_err(CatalogError::from))
        .collect()
}

/// Build the write batch for `CREATE POLICY`, allocating the OID.
///
/// The record's `oid` is ignored and overwritten with a freshly allocated one:
/// a policy's identity comes from the counter, never from the caller.
///
/// # Errors
///
/// Returns [`CatalogError::DuplicatePolicy`] when the relation already carries
/// a policy of that name, or storage/corruption errors from the catalog KV
/// seam.
pub fn create_policy_ops(kv: &dyn Kv, policy: &Policy) -> Result<Vec<WriteOp>, CatalogError> {
    if get_policy(kv, policy.table_id, &policy.name)?.is_some() {
        return Err(CatalogError::DuplicatePolicy {
            name: policy.name.clone(),
            relation: relation_label(kv, policy.table_id)?,
        });
    }
    let oid = read_oid_counter(kv)?;
    let stored = Policy {
        oid,
        ..policy.clone()
    };
    Ok(vec![
        WriteOp::Put {
            key: next_policy_oid_key(),
            value: U32::new(oid + 1).as_bytes().to_vec(),
        },
        WriteOp::Put {
            key: policy_key(stored.table_id, &stored.name),
            value: serialize_policy(&stored),
        },
    ])
}

/// Build the write batch for `ALTER POLICY … TO/USING/WITH CHECK`.
///
/// The stored OID, command and permissive flag are carried over untouched.
///
/// # Errors
///
/// Returns [`CatalogError::UndefinedPolicy`] when the relation carries no
/// policy of that name, or storage/corruption errors from the catalog KV seam.
pub fn alter_policy_ops(
    kv: &dyn Kv,
    table_id: TableId,
    name: &str,
    change: &PolicyChange,
) -> Result<Vec<WriteOp>, CatalogError> {
    let mut stored = require_policy(kv, table_id, name)?;
    if let Some(roles) = &change.roles {
        stored.roles.clone_from(roles);
    }
    if let Some(using) = &change.using {
        stored.using = Some(using.clone());
    }
    if let Some(with_check) = &change.with_check {
        stored.with_check = Some(with_check.clone());
    }
    Ok(vec![WriteOp::Put {
        key: policy_key(table_id, name),
        value: serialize_policy(&stored),
    }])
}

/// Build the write batch for `ALTER POLICY … RENAME TO`.
///
/// # Errors
///
/// Returns [`CatalogError::UndefinedPolicy`] when the policy does not exist,
/// [`CatalogError::DuplicatePolicy`] when the relation already carries a policy
/// under the new name, or storage/corruption errors from the catalog KV seam.
pub fn rename_policy_ops(
    kv: &dyn Kv,
    table_id: TableId,
    name: &str,
    new_name: &str,
) -> Result<Vec<WriteOp>, CatalogError> {
    let mut stored = require_policy(kv, table_id, name)?;
    if name != new_name && get_policy(kv, table_id, new_name)?.is_some() {
        return Err(CatalogError::DuplicatePolicy {
            name: new_name.to_string(),
            relation: relation_label(kv, table_id)?,
        });
    }
    stored.name = new_name.to_string();
    Ok(vec![
        WriteOp::Delete {
            key: policy_key(table_id, name),
        },
        WriteOp::Put {
            key: policy_key(table_id, new_name),
            value: serialize_policy(&stored),
        },
    ])
}

fn require_policy(kv: &dyn Kv, table_id: TableId, name: &str) -> Result<Policy, CatalogError> {
    match get_policy(kv, table_id, name)? {
        Some(policy) => Ok(policy),
        None => Err(CatalogError::UndefinedPolicy {
            name: name.to_string(),
            relation: relation_label(kv, table_id)?,
        }),
    }
}

/// Build the write batch for `DROP POLICY`.
///
/// # Errors
///
/// Returns [`CatalogError::UndefinedPolicy`] when the relation carries no
/// policy of that name, or storage/corruption errors from the catalog KV seam.
pub fn drop_policy_ops(
    kv: &dyn Kv,
    table_id: TableId,
    name: &str,
) -> Result<Vec<WriteOp>, CatalogError> {
    let _existing = require_policy(kv, table_id, name)?;
    Ok(vec![WriteOp::Delete {
        key: policy_key(table_id, name),
    }])
}

/// Delete every policy attached to a relation, for the batch that drops it.
///
/// Table ids are handed out from a counter that a restore or a reset can wind
/// back, so leaving these behind is not merely litter: a later relation on the
/// same id would inherit another relation's policies.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn drop_policies_for_table_ops(
    kv: &dyn Kv,
    table_id: TableId,
) -> Result<Vec<WriteOp>, CatalogError> {
    Ok(kv
        .scan_prefix(&policy_table_prefix(table_id))?
        .into_iter()
        .map(|(key, _)| WriteOp::Delete { key })
        .collect())
}

const POLICY_VERSION: u8 = 1;

fn command_code(command: PolicyCommand) -> u8 {
    match command {
        PolicyCommand::All => 0,
        PolicyCommand::Select => 1,
        PolicyCommand::Insert => 2,
        PolicyCommand::Update => 3,
        PolicyCommand::Delete => 4,
    }
}

fn read_command(value: u8) -> Result<PolicyCommand, KvError> {
    match value {
        0 => Ok(PolicyCommand::All),
        1 => Ok(PolicyCommand::Select),
        2 => Ok(PolicyCommand::Insert),
        3 => Ok(PolicyCommand::Update),
        4 => Ok(PolicyCommand::Delete),
        _ => Err(KvError::CorruptRow("unknown policy command".into())),
    }
}

fn write_count(out: &mut Vec<u8>, value: usize) {
    out.extend_from_slice(
        &u32::try_from(value)
            .expect("policy list length must fit in u32")
            .to_be_bytes(),
    );
}

fn read_u32(cur: &mut &[u8]) -> Result<u32, KvError> {
    let bytes = <[u8; 4]>::try_from(take_n(cur, 4)?)
        .map_err(|_| KvError::CorruptRow("invalid u32 width".into()))?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_bool(cur: &mut &[u8]) -> Result<bool, KvError> {
    match take_u8(cur)? {
        0 => Ok(false),
        1 => Ok(true),
        flag => Err(KvError::CorruptRow(format!("unknown policy flag {flag}"))),
    }
}

fn write_opt_string(out: &mut Vec<u8>, value: Option<&str>) {
    out.push(u8::from(value.is_some()));
    if let Some(value) = value {
        write_str(out, value);
    }
}

fn read_opt_string(cur: &mut &[u8]) -> Result<Option<String>, KvError> {
    if read_bool(cur)? {
        Ok(Some(read_string(cur)?))
    } else {
        Ok(None)
    }
}

/// Encode a policy record.
///
/// # Panics
///
/// Panics when the role list or a string exceeds its `u32` wire limit.
#[must_use]
pub fn serialize_policy(policy: &Policy) -> Vec<u8> {
    let mut out = vec![POLICY_VERSION];
    out.extend_from_slice(&policy.oid.to_be_bytes());
    out.extend_from_slice(&policy.table_id.to_be_bytes());
    write_str(&mut out, &policy.name);
    out.push(command_code(policy.command));
    out.push(u8::from(policy.permissive));
    write_count(&mut out, policy.roles.len());
    for role in &policy.roles {
        write_str(&mut out, role);
    }
    write_opt_string(&mut out, policy.using.as_deref());
    write_opt_string(&mut out, policy.with_check.as_deref());
    out
}

/// Decode a policy record.
///
/// # Errors
///
/// Returns [`KvError::CorruptRow`] for an unknown version, an unknown command
/// or flag byte, a truncated field, or trailing bytes.
pub fn deserialize_policy(bytes: &[u8]) -> Result<Policy, KvError> {
    let mut cur = bytes;
    if take_u8(&mut cur)? != POLICY_VERSION {
        return Err(KvError::CorruptRow("unknown policy version".into()));
    }
    let oid = read_u32(&mut cur)?;
    let table_id = read_u32(&mut cur)?;
    let name = read_string(&mut cur)?;
    let command = read_command(take_u8(&mut cur)?)?;
    let permissive = read_bool(&mut cur)?;
    let role_count = read_u32(&mut cur)? as usize;
    // The count is catalog-supplied rather than client-supplied, but sizing the
    // allocation from it directly would still turn one corrupt byte into a
    // multi-gigabyte reservation; the pushes grow it for a genuinely long list.
    let mut roles = Vec::with_capacity(role_count.min(16));
    for _ in 0..role_count {
        roles.push(read_string(&mut cur)?);
    }
    let policy = Policy {
        oid,
        name,
        table_id,
        command,
        permissive,
        roles,
        using: read_opt_string(&mut cur)?,
        with_check: read_opt_string(&mut cur)?,
    };
    if !cur.is_empty() {
        return Err(KvError::CorruptRow(
            "trailing bytes in policy record".into(),
        ));
    }
    Ok(policy)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgkv::MemKv;

    use super::*;

    fn policy(name: &str, table_id: TableId) -> Policy {
        Policy {
            oid: 0,
            name: name.into(),
            table_id,
            command: PolicyCommand::Select,
            permissive: true,
            roles: vec!["regress_alice".into()],
            using: Some("owner = current_user".into()),
            with_check: None,
        }
    }

    fn create(kv: &MemKv, policy: &Policy) {
        let ops = create_policy_ops(kv, policy).expect("create policy ops");
        kv.write_batch(&ops).expect("policy batch");
    }

    /// Every field shape the encoder has to carry, compared as one value.
    #[test]
    fn a_policy_record_round_trips_every_field() {
        let cases = [
            policy("p_select", 42),
            Policy {
                command: PolicyCommand::All,
                permissive: false,
                roles: Vec::new(),
                using: None,
                with_check: Some("length(title) < 10".into()),
                ..policy("p_all", 7)
            },
            Policy {
                command: PolicyCommand::Insert,
                roles: vec!["regress_alice".into(), "regress_bob".into()],
                with_check: Some("dlevel <= (SELECT seclv FROM uaccount)".into()),
                ..policy("p_insert", 9)
            },
            Policy {
                command: PolicyCommand::Update,
                ..policy("p_update", 9)
            },
            Policy {
                command: PolicyCommand::Delete,
                ..policy("p_delete", 9)
            },
        ];
        for case in cases {
            let stored = Policy {
                oid: 170_007,
                ..case
            };
            assert!(deserialize_policy(&serialize_policy(&stored)) == Ok(stored));
        }
    }

    #[test]
    fn a_command_carries_its_catalog_letter_and_keyword() {
        let cases = [
            (PolicyCommand::All, '*', "ALL"),
            (PolicyCommand::Select, 'r', "SELECT"),
            (PolicyCommand::Insert, 'a', "INSERT"),
            (PolicyCommand::Update, 'w', "UPDATE"),
            (PolicyCommand::Delete, 'd', "DELETE"),
        ];
        for (command, code, keyword) in cases {
            assert!(command.catalog_code() == code);
            assert!(command.keyword() == keyword);
        }
    }

    #[test]
    fn an_empty_role_list_means_public() {
        let public = Policy {
            roles: Vec::new(),
            ..policy("p", 1)
        };
        assert!(public.applies_to_public());
        assert!(!policy("p", 1).applies_to_public());
    }

    #[test]
    fn creating_a_policy_allocates_a_banded_oid_and_bumps_the_counter() {
        let kv = MemKv::new();
        assert!(next_policy_oid(&kv).expect("counter") == POLICY_OID_BASE);
        create(&kv, &policy("p1", 42));
        create(&kv, &policy("p2", 42));
        let stored = get_policy(&kv, 42, "p1").expect("get").expect("present");
        assert!(
            stored
                == Policy {
                    oid: POLICY_OID_BASE,
                    ..policy("p1", 42)
                }
        );
        assert!(
            get_policy(&kv, 42, "p2")
                .expect("get")
                .expect("present")
                .oid
                == POLICY_OID_BASE + 1
        );
        assert!(next_policy_oid(&kv).expect("counter") == POLICY_OID_BASE + 2);
    }

    #[test]
    fn a_caller_supplied_oid_is_replaced_by_the_allocated_one() {
        let kv = MemKv::new();
        create(
            &kv,
            &Policy {
                oid: 999_999,
                ..policy("p", 42)
            },
        );
        assert!(get_policy(&kv, 42, "p").expect("get").expect("present").oid == POLICY_OID_BASE);
    }

    #[test]
    fn a_duplicate_policy_name_on_one_relation_is_rejected() {
        let kv = MemKv::new();
        create(&kv, &policy("p", 42));
        let error = create_policy_ops(&kv, &policy("p", 42)).expect_err("duplicate");
        assert!(
            error
                == CatalogError::DuplicatePolicy {
                    name: "p".into(),
                    relation: "table id 42".into(),
                }
        );
        assert!(error.sqlstate() == "42710");
    }

    #[test]
    fn the_same_policy_name_on_two_relations_is_two_policies() {
        let kv = MemKv::new();
        create(&kv, &policy("p", 42));
        create(&kv, &policy("p", 43));
        assert!(policies_for_table(&kv, 42).expect("scan").len() == 1);
        assert!(policies_for_table(&kv, 43).expect("scan").len() == 1);
        assert!(list_policies(&kv).expect("list").len() == 2);
    }

    #[test]
    fn policies_for_a_relation_come_back_in_name_order() {
        let kv = MemKv::new();
        for name in ["p_c", "p_a", "p_b"] {
            create(&kv, &policy(name, 42));
        }
        create(&kv, &policy("other", 43));
        let names: Vec<_> = policies_for_table(&kv, 42)
            .expect("scan")
            .into_iter()
            .map(|policy| policy.name)
            .collect();
        assert!(names == vec!["p_a", "p_b", "p_c"]);
    }

    #[test]
    fn altering_a_policy_rewrites_only_the_fields_it_names() {
        let kv = MemKv::new();
        create(&kv, &policy("p", 42));
        let ops = alter_policy_ops(
            &kv,
            42,
            "p",
            &PolicyChange {
                roles: Some(vec!["regress_bob".into()]),
                with_check: Some("owner = current_user".into()),
                using: None,
            },
        )
        .expect("alter ops");
        kv.write_batch(&ops).expect("policy batch");
        assert!(
            get_policy(&kv, 42, "p").expect("get").expect("present")
                == Policy {
                    oid: POLICY_OID_BASE,
                    roles: vec!["regress_bob".into()],
                    with_check: Some("owner = current_user".into()),
                    ..policy("p", 42)
                }
        );
    }

    #[test]
    fn altering_a_policy_to_public_stores_an_empty_role_list() {
        let kv = MemKv::new();
        create(&kv, &policy("p", 42));
        let ops = alter_policy_ops(
            &kv,
            42,
            "p",
            &PolicyChange {
                roles: Some(Vec::new()),
                ..PolicyChange::default()
            },
        )
        .expect("alter ops");
        kv.write_batch(&ops).expect("policy batch");
        assert!(
            get_policy(&kv, 42, "p")
                .expect("get")
                .expect("present")
                .applies_to_public()
        );
    }

    #[test]
    fn altering_or_dropping_a_missing_policy_is_undefined_object() {
        let kv = MemKv::new();
        let expected = CatalogError::UndefinedPolicy {
            name: "nope".into(),
            relation: "table id 42".into(),
        };
        assert!(
            alter_policy_ops(&kv, 42, "nope", &PolicyChange::default()).expect_err("alter")
                == expected
        );
        assert!(drop_policy_ops(&kv, 42, "nope").expect_err("drop") == expected);
        assert!(rename_policy_ops(&kv, 42, "nope", "other").expect_err("rename") == expected);
        assert!(expected.sqlstate() == "42704");
    }

    #[test]
    fn renaming_a_policy_moves_its_key_and_keeps_its_oid() {
        let kv = MemKv::new();
        create(&kv, &policy("p", 42));
        let ops = rename_policy_ops(&kv, 42, "p", "p_renamed").expect("rename ops");
        kv.write_batch(&ops).expect("policy batch");
        assert!(get_policy(&kv, 42, "p").expect("get").is_none());
        assert!(
            get_policy(&kv, 42, "p_renamed").expect("get")
                == Some(Policy {
                    oid: POLICY_OID_BASE,
                    name: "p_renamed".into(),
                    ..policy("p", 42)
                })
        );
    }

    #[test]
    fn renaming_a_policy_onto_a_taken_name_is_rejected() {
        let kv = MemKv::new();
        create(&kv, &policy("p", 42));
        create(&kv, &policy("q", 42));
        assert!(
            rename_policy_ops(&kv, 42, "p", "q").expect_err("duplicate")
                == CatalogError::DuplicatePolicy {
                    name: "q".into(),
                    relation: "table id 42".into(),
                }
        );
    }

    #[test]
    fn dropping_one_policy_leaves_the_relations_others_alone() {
        let kv = MemKv::new();
        create(&kv, &policy("p", 42));
        create(&kv, &policy("q", 42));
        let ops = drop_policy_ops(&kv, 42, "p").expect("drop ops");
        kv.write_batch(&ops).expect("policy batch");
        let names: Vec<_> = policies_for_table(&kv, 42)
            .expect("scan")
            .into_iter()
            .map(|policy| policy.name)
            .collect();
        assert!(names == vec!["q"]);
    }

    #[test]
    fn dropping_a_relations_policies_leaves_every_other_relation_alone() {
        let kv = MemKv::new();
        create(&kv, &policy("p", 42));
        create(&kv, &policy("q", 42));
        create(&kv, &policy("p", 43));
        let ops = drop_policies_for_table_ops(&kv, 42).expect("drop-all ops");
        kv.write_batch(&ops).expect("policy batch");
        assert!(policies_for_table(&kv, 42).expect("scan").is_empty());
        assert!(policies_for_table(&kv, 43).expect("scan").len() == 1);
    }

    #[test]
    fn a_record_the_reader_does_not_fully_understand_is_refused() {
        let encoded = serialize_policy(&Policy {
            oid: POLICY_OID_BASE,
            ..policy("p", 42)
        });
        let mut wrong_version = encoded.clone();
        wrong_version[0] = POLICY_VERSION + 1;
        assert!(
            deserialize_policy(&wrong_version)
                == Err(KvError::CorruptRow("unknown policy version".into()))
        );

        let mut wrong_command = encoded.clone();
        // version, oid, table_id, then the length-prefixed name.
        let command_at = 1 + 4 + 4 + 4 + "p".len();
        wrong_command[command_at] = 9;
        assert!(
            deserialize_policy(&wrong_command)
                == Err(KvError::CorruptRow("unknown policy command".into()))
        );

        let mut wrong_permissive = encoded.clone();
        wrong_permissive[command_at + 1] = 2;
        assert!(
            deserialize_policy(&wrong_permissive)
                == Err(KvError::CorruptRow("unknown policy flag 2".into()))
        );

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(
            deserialize_policy(&trailing)
                == Err(KvError::CorruptRow(
                    "trailing bytes in policy record".into()
                ))
        );

        assert!(deserialize_policy(&encoded[..encoded.len() - 1]).is_err());
    }
}
