//! Durable rewrite-rule catalog records.

#![allow(clippy::missing_errors_doc)]

use crabka_pgkv::{Kv, KvError, WriteOp};

use crate::{
    CatalogError, RelationName, TableId,
    serde::{read_string, take_n, take_u8, write_str},
};

pub const RULE_OID_BASE: u32 = 170_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleEvent {
    Select,
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rule {
    pub oid: u32,
    pub name: String,
    pub table_id: TableId,
    pub table: RelationName,
    pub event: RuleEvent,
    pub condition: Option<String>,
    pub instead: bool,
    pub enabled: crate::trigger::TriggerEnabled,
    /// SQL after `DO ALSO` or `DO INSTEAD`, reparsed only when the rule fires.
    pub action: String,
}

fn prefix() -> Vec<u8> {
    b"\0\0\0\0catalog_rule/".to_vec()
}

fn key(table_id: TableId, name: &str) -> Vec<u8> {
    let mut key = prefix();
    key.extend_from_slice(&table_id.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(name.as_bytes());
    key
}

fn next_oid_key() -> Vec<u8> {
    b"\0\0\0\0meta/next_rule_oid".to_vec()
}

fn event_code(event: RuleEvent) -> u8 {
    match event {
        RuleEvent::Select => 0,
        RuleEvent::Insert => 1,
        RuleEvent::Update => 2,
        RuleEvent::Delete => 3,
    }
}

fn read_event(code: u8) -> Result<RuleEvent, KvError> {
    match code {
        0 => Ok(RuleEvent::Select),
        1 => Ok(RuleEvent::Insert),
        2 => Ok(RuleEvent::Update),
        3 => Ok(RuleEvent::Delete),
        _ => Err(KvError::CorruptRow("unknown rewrite rule event".into())),
    }
}

fn read_u32(cur: &mut &[u8]) -> Result<u32, KvError> {
    let bytes = take_n(cur, 4)?;
    Ok(u32::from_be_bytes(bytes.try_into().expect("four bytes")))
}

fn write_opt_string(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            out.push(1);
            write_str(out, value);
        }
        None => out.push(0),
    }
}

fn read_opt_string(cur: &mut &[u8]) -> Result<Option<String>, KvError> {
    match take_u8(cur)? {
        0 => Ok(None),
        1 => read_string(cur).map(Some),
        _ => Err(KvError::CorruptRow(
            "invalid optional rewrite rule string".into(),
        )),
    }
}

pub fn get_rule(kv: &dyn Kv, table_id: TableId, name: &str) -> Result<Option<Rule>, CatalogError> {
    kv.get(&key(table_id, name))?
        .map(|bytes| deserialize_rule(&bytes).map_err(CatalogError::from))
        .transpose()
}

pub fn rules_for_table(kv: &dyn Kv, table_id: TableId) -> Result<Vec<Rule>, CatalogError> {
    let mut key_prefix = prefix();
    key_prefix.extend_from_slice(&table_id.to_be_bytes());
    key_prefix.push(b'/');
    let mut rules: Vec<_> = kv
        .scan_prefix(&key_prefix)?
        .into_iter()
        .map(|(_, bytes)| deserialize_rule(&bytes).map_err(CatalogError::from))
        .collect::<Result<_, _>>()?;
    rules.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(rules)
}

pub fn list_rules(kv: &dyn Kv) -> Result<Vec<Rule>, CatalogError> {
    kv.scan_prefix(&prefix())?
        .into_iter()
        .map(|(_, bytes)| deserialize_rule(&bytes).map_err(CatalogError::from))
        .collect()
}

pub fn put_rule_ops(kv: &dyn Kv, rule: &Rule) -> Result<Vec<WriteOp>, CatalogError> {
    let mut stored = rule.clone();
    let mut ops = Vec::new();
    if stored.oid == 0 {
        if let Some(existing) = get_rule(kv, stored.table_id, &stored.name)? {
            stored.oid = existing.oid;
        } else {
            let oid = kv
                .get(&next_oid_key())?
                .map(|bytes| read_u32(&mut bytes.as_slice()))
                .transpose()?
                .unwrap_or(RULE_OID_BASE);
            stored.oid = oid;
            ops.push(WriteOp::Put {
                key: next_oid_key(),
                value: (oid + 1).to_be_bytes().to_vec(),
            });
        }
    }
    ops.push(WriteOp::Put {
        key: key(stored.table_id, &stored.name),
        value: serialize_rule(&stored),
    });
    Ok(ops)
}

#[must_use]
pub fn drop_rule_ops(table_id: TableId, name: &str) -> Vec<WriteOp> {
    vec![WriteOp::Delete {
        key: key(table_id, name),
    }]
}

pub fn drop_rules_for_table_ops(
    kv: &dyn Kv,
    table_id: TableId,
) -> Result<Vec<WriteOp>, CatalogError> {
    Ok(rules_for_table(kv, table_id)?
        .into_iter()
        .flat_map(|rule| {
            let oid = rule.oid.to_string();
            let mut ops = drop_rule_ops(table_id, &rule.name);
            ops.push(crate::set_comment_op(
                "rule",
                crate::CommentObject::Named(&oid),
                None,
            ));
            ops
        })
        .collect())
}

#[must_use]
pub fn serialize_rule(rule: &Rule) -> Vec<u8> {
    let mut out = vec![3];
    out.extend_from_slice(&rule.oid.to_be_bytes());
    out.extend_from_slice(&rule.table_id.to_be_bytes());
    out.push(event_code(rule.event));
    out.push(u8::from(rule.instead));
    out.push(match rule.enabled {
        crate::trigger::TriggerEnabled::Origin => 0,
        crate::trigger::TriggerEnabled::Disabled => 1,
        crate::trigger::TriggerEnabled::Replica => 2,
        crate::trigger::TriggerEnabled::Always => 3,
    });
    write_str(&mut out, &rule.name);
    write_str(&mut out, &rule.table.schema);
    write_str(&mut out, &rule.table.name);
    write_opt_string(&mut out, rule.condition.as_deref());
    write_str(&mut out, &rule.action);
    out
}

pub fn deserialize_rule(bytes: &[u8]) -> Result<Rule, KvError> {
    let mut cur = bytes;
    if take_u8(&mut cur)? != 3 {
        return Err(KvError::CorruptRow("unknown rewrite rule version".into()));
    }
    let oid = read_u32(&mut cur)?;
    let table_id = read_u32(&mut cur)?;
    let event = read_event(take_u8(&mut cur)?)?;
    let instead = match take_u8(&mut cur)? {
        0 => false,
        1 => true,
        _ => {
            return Err(KvError::CorruptRow(
                "invalid rewrite rule instead flag".into(),
            ));
        }
    };
    let enabled = match take_u8(&mut cur)? {
        0 => crate::trigger::TriggerEnabled::Origin,
        1 => crate::trigger::TriggerEnabled::Disabled,
        2 => crate::trigger::TriggerEnabled::Replica,
        3 => crate::trigger::TriggerEnabled::Always,
        _ => {
            return Err(KvError::CorruptRow(
                "invalid rewrite rule enabled flag".into(),
            ));
        }
    };
    let name = read_string(&mut cur)?;
    let table = RelationName::new(read_string(&mut cur)?, read_string(&mut cur)?);
    let condition = read_opt_string(&mut cur)?;
    let action = read_string(&mut cur)?;
    if !cur.is_empty() {
        return Err(KvError::CorruptRow("trailing rewrite rule bytes".into()));
    }
    Ok(Rule {
        oid,
        name,
        table_id,
        table,
        event,
        condition,
        instead,
        enabled,
        action,
    })
}

#[cfg(test)]
mod tests {
    use crabka_pgkv::{Kv, MemKv};

    use super::*;

    fn rule(name: &str) -> Rule {
        Rule {
            oid: 0,
            name: name.into(),
            table_id: 42,
            table: RelationName::public("items"),
            event: RuleEvent::Insert,
            condition: Some("new.id > 0".into()),
            instead: false,
            enabled: crate::trigger::TriggerEnabled::Origin,
            action: "INSERT INTO item_log VALUES (new.*)".into(),
        }
    }

    #[test]
    fn rule_round_trip_allocates_oids_and_drops_by_table() {
        let kv = MemKv::new();
        kv.write_batch(&put_rule_ops(&kv, &rule("audit")).expect("put audit"))
            .expect("write audit");
        kv.write_batch(&put_rule_ops(&kv, &rule("mirror")).expect("put mirror"))
            .expect("write mirror");

        let stored = rules_for_table(&kv, 42).expect("list rules");
        assert_eq!(stored.len(), 2);
        assert_eq!(stored[0].oid, RULE_OID_BASE);
        assert_eq!(stored[1].oid, RULE_OID_BASE + 1);
        assert_eq!(stored[0].action, "INSERT INTO item_log VALUES (new.*)");

        kv.write_batch(&drop_rules_for_table_ops(&kv, 42).expect("drop rules"))
            .expect("write drops");
        assert!(
            rules_for_table(&kv, 42)
                .expect("list after drop")
                .is_empty()
        );
    }
}
