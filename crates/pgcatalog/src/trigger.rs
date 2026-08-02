//! Durable ordinary-trigger and event-trigger catalog records.

#![allow(clippy::missing_errors_doc)]

use crabka_pgkv::{Kv, KvError, WriteOp};
use zerocopy::{FromBytes, IntoBytes, byteorder::big_endian::U32};

use crate::{
    CatalogError, RelationName, TableId,
    serde::{read_string, take_n, take_u8, write_str},
};

pub const TRIGGER_OID_BASE: u32 = 150_000;
pub const EVENT_TRIGGER_OID_BASE: u32 = 160_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerLevel {
    Row,
    Statement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerEnabled {
    Origin,
    Disabled,
    Replica,
    Always,
}

impl TriggerEnabled {
    #[must_use]
    pub const fn catalog_code(self) -> char {
        match self {
            Self::Origin => 'O',
            Self::Disabled => 'D',
            Self::Replica => 'R',
            Self::Always => 'A',
        }
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TriggerEvents {
    pub insert: bool,
    pub update: bool,
    pub delete: bool,
    pub truncate: bool,
    pub update_columns: Vec<String>,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trigger {
    pub oid: u32,
    pub name: String,
    pub table_id: TableId,
    pub table: RelationName,
    /// OID of the partition-parent trigger this row clones, or zero.
    pub parent_oid: u32,
    pub function_oid: u32,
    pub function: String,
    pub timing: TriggerTiming,
    pub events: TriggerEvents,
    pub level: TriggerLevel,
    pub enabled: TriggerEnabled,
    pub is_internal: bool,
    pub constraint: bool,
    pub constraint_oid: u32,
    pub referenced_table_id: Option<TableId>,
    pub deferrable: bool,
    pub initially_deferred: bool,
    pub old_transition: Option<String>,
    pub new_transition: Option<String>,
    /// Source text of the `WHEN` condition, without its enclosing parentheses.
    pub when: Option<String>,
    pub arguments: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventTriggerEvent {
    Login,
    DdlCommandStart,
    DdlCommandEnd,
    SqlDrop,
    TableRewrite,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventTriggerFilter {
    pub variable: String,
    pub values: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventTrigger {
    pub oid: u32,
    pub name: String,
    pub event: EventTriggerEvent,
    pub owner: String,
    pub function_oid: u32,
    pub function: String,
    pub enabled: TriggerEnabled,
    pub filters: Vec<EventTriggerFilter>,
}

fn trigger_prefix() -> Vec<u8> {
    b"\0\0\0\0catalog_trigger/".to_vec()
}

fn trigger_key(table_id: TableId, name: &str) -> Vec<u8> {
    let mut key = trigger_prefix();
    key.extend_from_slice(&table_id.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(name.as_bytes());
    key
}

fn event_trigger_prefix() -> Vec<u8> {
    b"\0\0\0\0catalog_event_trigger/".to_vec()
}

fn event_trigger_key(name: &str) -> Vec<u8> {
    let mut key = event_trigger_prefix();
    key.extend_from_slice(name.as_bytes());
    key
}

fn next_trigger_oid_key() -> Vec<u8> {
    b"\0\0\0\0meta/next_trigger_oid".to_vec()
}

fn next_event_trigger_oid_key() -> Vec<u8> {
    b"\0\0\0\0meta/next_event_trigger_oid".to_vec()
}

fn read_oid_counter(kv: &dyn Kv, key: &[u8], base: u32) -> Result<u32, CatalogError> {
    match kv.get(key)? {
        Some(bytes) => {
            let (value, _) = U32::read_from_prefix(bytes.as_slice())
                .map_err(|_| KvError::CorruptRow("trigger oid counter is not u32".into()))?;
            Ok(value.get())
        }
        None => Ok(base),
    }
}

pub fn next_trigger_oid(kv: &dyn Kv) -> Result<u32, CatalogError> {
    read_oid_counter(kv, &next_trigger_oid_key(), TRIGGER_OID_BASE)
}

#[must_use]
pub fn set_next_trigger_oid_op(next: u32) -> WriteOp {
    WriteOp::Put {
        key: next_trigger_oid_key(),
        value: U32::new(next).as_bytes().to_vec(),
    }
}

pub fn get_trigger(
    kv: &dyn Kv,
    table_id: TableId,
    name: &str,
) -> Result<Option<Trigger>, CatalogError> {
    kv.get(&trigger_key(table_id, name))?
        .map(|bytes| deserialize_trigger(&bytes).map_err(CatalogError::from))
        .transpose()
}

pub fn triggers_for_table(kv: &dyn Kv, table_id: TableId) -> Result<Vec<Trigger>, CatalogError> {
    let mut prefix = trigger_prefix();
    prefix.extend_from_slice(&table_id.to_be_bytes());
    prefix.push(b'/');
    let mut triggers: Vec<_> = kv
        .scan_prefix(&prefix)?
        .into_iter()
        .map(|(_, bytes)| deserialize_trigger(&bytes).map_err(CatalogError::from))
        .collect::<Result<_, _>>()?;
    triggers.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(triggers)
}

pub fn list_triggers(kv: &dyn Kv) -> Result<Vec<Trigger>, CatalogError> {
    kv.scan_prefix(&trigger_prefix())?
        .into_iter()
        .map(|(_, bytes)| deserialize_trigger(&bytes).map_err(CatalogError::from))
        .collect()
}

pub fn put_trigger_ops(kv: &dyn Kv, trigger: &Trigger) -> Result<Vec<WriteOp>, CatalogError> {
    let mut stored = trigger.clone();
    let mut ops = Vec::new();
    if stored.oid == 0 {
        if let Some(existing) = get_trigger(kv, stored.table_id, &stored.name)? {
            stored.oid = existing.oid;
        } else {
            stored.oid = read_oid_counter(kv, &next_trigger_oid_key(), TRIGGER_OID_BASE)?;
            ops.push(WriteOp::Put {
                key: next_trigger_oid_key(),
                value: U32::new(stored.oid + 1).as_bytes().to_vec(),
            });
        }
    }
    ops.push(WriteOp::Put {
        key: trigger_key(stored.table_id, &stored.name),
        value: serialize_trigger(&stored),
    });
    Ok(ops)
}

#[must_use]
pub fn drop_trigger_ops(table_id: TableId, name: &str) -> Vec<WriteOp> {
    vec![WriteOp::Delete {
        key: trigger_key(table_id, name),
    }]
}

pub fn drop_triggers_for_table_ops(
    kv: &dyn Kv,
    table_id: TableId,
) -> Result<Vec<WriteOp>, CatalogError> {
    Ok(triggers_for_table(kv, table_id)?
        .into_iter()
        .flat_map(|trigger| drop_trigger_ops(table_id, &trigger.name))
        .collect())
}

pub fn get_event_trigger(kv: &dyn Kv, name: &str) -> Result<Option<EventTrigger>, CatalogError> {
    kv.get(&event_trigger_key(name))?
        .map(|bytes| deserialize_event_trigger(&bytes).map_err(CatalogError::from))
        .transpose()
}

pub fn list_event_triggers(kv: &dyn Kv) -> Result<Vec<EventTrigger>, CatalogError> {
    kv.scan_prefix(&event_trigger_prefix())?
        .into_iter()
        .map(|(_, bytes)| deserialize_event_trigger(&bytes).map_err(CatalogError::from))
        .collect()
}

pub fn put_event_trigger_ops(
    kv: &dyn Kv,
    trigger: &EventTrigger,
) -> Result<Vec<WriteOp>, CatalogError> {
    let mut stored = trigger.clone();
    let mut ops = Vec::new();
    if stored.oid == 0 {
        if let Some(existing) = get_event_trigger(kv, &stored.name)? {
            stored.oid = existing.oid;
        } else {
            stored.oid =
                read_oid_counter(kv, &next_event_trigger_oid_key(), EVENT_TRIGGER_OID_BASE)?;
            ops.push(WriteOp::Put {
                key: next_event_trigger_oid_key(),
                value: U32::new(stored.oid + 1).as_bytes().to_vec(),
            });
        }
    }
    ops.push(WriteOp::Put {
        key: event_trigger_key(&stored.name),
        value: serialize_event_trigger(&stored),
    });
    Ok(ops)
}

#[must_use]
pub fn drop_event_trigger_ops(name: &str) -> Vec<WriteOp> {
    vec![WriteOp::Delete {
        key: event_trigger_key(name),
    }]
}

fn write_bool(out: &mut Vec<u8>, value: bool) {
    out.push(u8::from(value));
}

fn write_count(out: &mut Vec<u8>, value: usize) {
    out.extend_from_slice(
        &u32::try_from(value)
            .expect("trigger list length must fit in u32")
            .to_be_bytes(),
    );
}

fn read_count(cur: &mut &[u8]) -> Result<usize, KvError> {
    Ok(read_u32(cur)? as usize)
}

fn read_u32(cur: &mut &[u8]) -> Result<u32, KvError> {
    let bytes = <[u8; 4]>::try_from(take_n(cur, 4)?)
        .map_err(|_| KvError::CorruptRow("invalid u32 width".into()))?;
    Ok(u32::from_be_bytes(bytes))
}

fn write_opt_string(out: &mut Vec<u8>, value: Option<&str>) {
    write_bool(out, value.is_some());
    if let Some(value) = value {
        write_str(out, value);
    }
}

fn read_opt_string(cur: &mut &[u8]) -> Result<Option<String>, KvError> {
    if take_u8(cur)? == 0 {
        Ok(None)
    } else {
        Ok(Some(read_string(cur)?))
    }
}

fn timing_code(value: TriggerTiming) -> u8 {
    match value {
        TriggerTiming::Before => 0,
        TriggerTiming::After => 1,
        TriggerTiming::InsteadOf => 2,
    }
}

fn read_timing(value: u8) -> Result<TriggerTiming, KvError> {
    match value {
        0 => Ok(TriggerTiming::Before),
        1 => Ok(TriggerTiming::After),
        2 => Ok(TriggerTiming::InsteadOf),
        _ => Err(KvError::CorruptRow("unknown trigger timing".into())),
    }
}

fn enabled_code(value: TriggerEnabled) -> u8 {
    match value {
        TriggerEnabled::Origin => 0,
        TriggerEnabled::Disabled => 1,
        TriggerEnabled::Replica => 2,
        TriggerEnabled::Always => 3,
    }
}

fn read_enabled(value: u8) -> Result<TriggerEnabled, KvError> {
    match value {
        0 => Ok(TriggerEnabled::Origin),
        1 => Ok(TriggerEnabled::Disabled),
        2 => Ok(TriggerEnabled::Replica),
        3 => Ok(TriggerEnabled::Always),
        _ => Err(KvError::CorruptRow("unknown trigger enabled mode".into())),
    }
}

const TRIGGER_VERSION: u8 = 2;

#[must_use]
pub fn serialize_trigger(trigger: &Trigger) -> Vec<u8> {
    let mut out = vec![TRIGGER_VERSION];
    for value in [
        trigger.oid,
        trigger.table_id,
        trigger.parent_oid,
        trigger.function_oid,
    ] {
        out.extend_from_slice(&value.to_be_bytes());
    }
    write_str(&mut out, &trigger.name);
    write_str(&mut out, &trigger.table.schema);
    write_str(&mut out, &trigger.table.name);
    write_str(&mut out, &trigger.function);
    out.push(timing_code(trigger.timing));
    let event_bits = u8::from(trigger.events.insert)
        | (u8::from(trigger.events.update) << 1)
        | (u8::from(trigger.events.delete) << 2)
        | (u8::from(trigger.events.truncate) << 3);
    out.push(event_bits);
    write_count(&mut out, trigger.events.update_columns.len());
    for column in &trigger.events.update_columns {
        write_str(&mut out, column);
    }
    out.push(match trigger.level {
        TriggerLevel::Row => 0,
        TriggerLevel::Statement => 1,
    });
    out.push(enabled_code(trigger.enabled));
    write_bool(&mut out, trigger.is_internal);
    write_bool(&mut out, trigger.constraint);
    out.extend_from_slice(&trigger.constraint_oid.to_be_bytes());
    out.extend_from_slice(&trigger.referenced_table_id.unwrap_or(0).to_be_bytes());
    write_bool(&mut out, trigger.deferrable);
    write_bool(&mut out, trigger.initially_deferred);
    write_opt_string(&mut out, trigger.old_transition.as_deref());
    write_opt_string(&mut out, trigger.new_transition.as_deref());
    write_opt_string(&mut out, trigger.when.as_deref());
    write_count(&mut out, trigger.arguments.len());
    for argument in &trigger.arguments {
        write_str(&mut out, argument);
    }
    out
}

pub fn deserialize_trigger(bytes: &[u8]) -> Result<Trigger, KvError> {
    let mut cur = bytes;
    let version = take_u8(&mut cur)?;
    if !matches!(version, 1 | TRIGGER_VERSION) {
        return Err(KvError::CorruptRow("unknown trigger version".into()));
    }
    let oid = read_u32(&mut cur)?;
    let table_id = read_u32(&mut cur)?;
    let parent_oid = read_u32(&mut cur)?;
    let function_oid = read_u32(&mut cur)?;
    let name = read_string(&mut cur)?;
    let table = RelationName::new(read_string(&mut cur)?, read_string(&mut cur)?);
    let function = read_string(&mut cur)?;
    let timing = read_timing(take_u8(&mut cur)?)?;
    let bits = take_u8(&mut cur)?;
    let column_count = read_count(&mut cur)?;
    let mut update_columns = Vec::with_capacity(column_count.min(1024));
    for _ in 0..column_count {
        update_columns.push(read_string(&mut cur)?);
    }
    let level = match take_u8(&mut cur)? {
        0 => TriggerLevel::Row,
        1 => TriggerLevel::Statement,
        _ => return Err(KvError::CorruptRow("unknown trigger level".into())),
    };
    let enabled = read_enabled(take_u8(&mut cur)?)?;
    let is_internal = take_u8(&mut cur)? != 0;
    let constraint = version >= 2 && take_u8(&mut cur)? != 0;
    let constraint_oid = read_u32(&mut cur)?;
    let referenced = read_u32(&mut cur)?;
    let trigger = Trigger {
        oid,
        name,
        table_id,
        table,
        parent_oid,
        function_oid,
        function,
        timing,
        events: TriggerEvents {
            insert: bits & 1 != 0,
            update: bits & 2 != 0,
            delete: bits & 4 != 0,
            truncate: bits & 8 != 0,
            update_columns,
        },
        level,
        enabled,
        is_internal,
        constraint,
        constraint_oid,
        referenced_table_id: (referenced != 0).then_some(referenced),
        deferrable: take_u8(&mut cur)? != 0,
        initially_deferred: take_u8(&mut cur)? != 0,
        old_transition: read_opt_string(&mut cur)?,
        new_transition: read_opt_string(&mut cur)?,
        when: read_opt_string(&mut cur)?,
        arguments: {
            let count = read_count(&mut cur)?;
            let mut values = Vec::with_capacity(count.min(1024));
            for _ in 0..count {
                values.push(read_string(&mut cur)?);
            }
            values
        },
    };
    if !cur.is_empty() {
        return Err(KvError::CorruptRow(
            "trailing bytes in trigger record".into(),
        ));
    }
    Ok(trigger)
}

fn event_code(value: EventTriggerEvent) -> u8 {
    match value {
        EventTriggerEvent::Login => 0,
        EventTriggerEvent::DdlCommandStart => 1,
        EventTriggerEvent::DdlCommandEnd => 2,
        EventTriggerEvent::SqlDrop => 3,
        EventTriggerEvent::TableRewrite => 4,
    }
}

fn read_event(value: u8) -> Result<EventTriggerEvent, KvError> {
    match value {
        0 => Ok(EventTriggerEvent::Login),
        1 => Ok(EventTriggerEvent::DdlCommandStart),
        2 => Ok(EventTriggerEvent::DdlCommandEnd),
        3 => Ok(EventTriggerEvent::SqlDrop),
        4 => Ok(EventTriggerEvent::TableRewrite),
        _ => Err(KvError::CorruptRow("unknown event trigger event".into())),
    }
}

const EVENT_TRIGGER_VERSION: u8 = 1;

#[must_use]
pub fn serialize_event_trigger(trigger: &EventTrigger) -> Vec<u8> {
    let mut out = vec![EVENT_TRIGGER_VERSION];
    out.extend_from_slice(&trigger.oid.to_be_bytes());
    write_str(&mut out, &trigger.name);
    out.push(event_code(trigger.event));
    write_str(&mut out, &trigger.owner);
    out.extend_from_slice(&trigger.function_oid.to_be_bytes());
    write_str(&mut out, &trigger.function);
    out.push(enabled_code(trigger.enabled));
    write_count(&mut out, trigger.filters.len());
    for filter in &trigger.filters {
        write_str(&mut out, &filter.variable);
        write_count(&mut out, filter.values.len());
        for value in &filter.values {
            write_str(&mut out, value);
        }
    }
    out
}

pub fn deserialize_event_trigger(bytes: &[u8]) -> Result<EventTrigger, KvError> {
    let mut cur = bytes;
    if take_u8(&mut cur)? != EVENT_TRIGGER_VERSION {
        return Err(KvError::CorruptRow("unknown event trigger version".into()));
    }
    let oid = read_u32(&mut cur)?;
    let name = read_string(&mut cur)?;
    let event = read_event(take_u8(&mut cur)?)?;
    let owner = read_string(&mut cur)?;
    let function_oid = read_u32(&mut cur)?;
    let function = read_string(&mut cur)?;
    let enabled = read_enabled(take_u8(&mut cur)?)?;
    let count = read_count(&mut cur)?;
    let mut filters = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let variable = read_string(&mut cur)?;
        let value_count = read_count(&mut cur)?;
        let mut values = Vec::with_capacity(value_count.min(1024));
        for _ in 0..value_count {
            values.push(read_string(&mut cur)?);
        }
        filters.push(EventTriggerFilter { variable, values });
    }
    if !cur.is_empty() {
        return Err(KvError::CorruptRow(
            "trailing bytes in event trigger record".into(),
        ));
    }
    Ok(EventTrigger {
        oid,
        name,
        event,
        owner,
        function_oid,
        function,
        enabled,
        filters,
    })
}

#[cfg(test)]
mod tests {
    use crabka_pgkv::{Kv, MemKv};

    use super::*;

    fn ordinary() -> Trigger {
        Trigger {
            oid: 0,
            name: "audit".into(),
            table_id: 42,
            table: RelationName::public("items"),
            parent_oid: 0,
            function_oid: 140_001,
            function: "audit_row".into(),
            timing: TriggerTiming::After,
            events: TriggerEvents {
                insert: true,
                update: true,
                update_columns: vec!["value".into()],
                ..TriggerEvents::default()
            },
            level: TriggerLevel::Row,
            enabled: TriggerEnabled::Origin,
            is_internal: false,
            constraint: false,
            constraint_oid: 0,
            referenced_table_id: None,
            deferrable: false,
            initially_deferred: false,
            old_transition: Some("old_rows".into()),
            new_transition: Some("new_rows".into()),
            when: Some("NEW.value IS DISTINCT FROM OLD.value".into()),
            arguments: vec!["audit_log".into()],
        }
    }

    #[test]
    fn ordinary_trigger_round_trip_and_oid_allocation() {
        let kv = MemKv::new();
        kv.write_batch(&put_trigger_ops(&kv, &ordinary()).unwrap())
            .unwrap();
        let stored = get_trigger(&kv, 42, "audit").unwrap().unwrap();
        assert_eq!(stored.oid, TRIGGER_OID_BASE);
        let mut expected = ordinary();
        expected.oid = TRIGGER_OID_BASE;
        assert_eq!(stored, expected);
    }

    #[test]
    fn event_trigger_round_trip_and_oid_allocation() {
        let kv = MemKv::new();
        let trigger = EventTrigger {
            oid: 0,
            name: "ddl_audit".into(),
            event: EventTriggerEvent::DdlCommandEnd,
            owner: "crab".into(),
            function_oid: 140_002,
            function: "audit_ddl".into(),
            enabled: TriggerEnabled::Always,
            filters: vec![EventTriggerFilter {
                variable: "tag".into(),
                values: vec!["CREATE TABLE".into()],
            }],
        };
        kv.write_batch(&put_event_trigger_ops(&kv, &trigger).unwrap())
            .unwrap();
        let stored = get_event_trigger(&kv, "ddl_audit").unwrap().unwrap();
        assert_eq!(stored.oid, EVENT_TRIGGER_OID_BASE);
        assert_eq!(stored.name, trigger.name);
        assert_eq!(stored.filters, trigger.filters);
    }
}
