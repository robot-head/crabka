//! Stored per-attribute statistics backing `pg_stats`.

use std::collections::BTreeMap;

use crabka_pgcatalog::RelationName;
use crabka_pgkv::{Kv, WriteOp, key::push_key_part};

use crate::error::ExecError;

const PREFIX: &[u8] = b"\0\0\0\0catalog_attrstats/";

/// The fixed fields PostgreSQL exposes for one `(relation, attribute, inherit)` key.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct AttributeStats {
    pub(crate) null_frac: Option<f32>,
    pub(crate) avg_width: Option<i32>,
    pub(crate) n_distinct: Option<f32>,
    pub(crate) most_common_vals: Option<String>,
    pub(crate) most_common_freqs: Option<String>,
    pub(crate) histogram_bounds: Option<String>,
    pub(crate) correlation: Option<f32>,
    pub(crate) most_common_elems: Option<String>,
    pub(crate) most_common_elem_freqs: Option<String>,
    pub(crate) elem_count_histogram: Option<String>,
    pub(crate) range_length_histogram: Option<String>,
    pub(crate) range_empty_frac: Option<f32>,
    pub(crate) range_bounds_histogram: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct AttributeStatsKey {
    pub(crate) relation: RelationName,
    pub(crate) attnum: i16,
    pub(crate) inherited: bool,
}

pub(crate) fn set_op(key: &AttributeStatsKey, stats: AttributeStats) -> WriteOp {
    WriteOp::Put {
        key: key_bytes(key),
        value: encode(stats),
    }
}

pub(crate) fn clear_op(key: &AttributeStatsKey) -> WriteOp {
    WriteOp::Delete {
        key: key_bytes(key),
    }
}

pub(crate) fn get(
    kv: &dyn Kv,
    key: &AttributeStatsKey,
) -> Result<Option<AttributeStats>, ExecError> {
    kv.get(&key_bytes(key))
        .map_err(ExecError::Kv)?
        .map(|value| decode(&value))
        .transpose()
}

pub(crate) fn all(kv: &dyn Kv) -> Result<BTreeMap<AttributeStatsKey, AttributeStats>, ExecError> {
    kv.scan_prefix(PREFIX)
        .map_err(ExecError::Kv)?
        .into_iter()
        .map(|(key, value)| Ok((key_from_bytes(&key)?, decode(&value)?)))
        .collect()
}

/// Move every per-attribute statistic to a relation's new name.
pub(crate) fn rename_ops(
    kv: &dyn Kv,
    from: &RelationName,
    to: &RelationName,
) -> Result<Vec<WriteOp>, ExecError> {
    let mut ops = Vec::new();
    for (mut key, stats) in all(kv)? {
        if key.relation != *from {
            continue;
        }
        ops.push(clear_op(&key));
        key.relation = to.clone();
        ops.push(set_op(&key, stats));
    }
    Ok(ops)
}

fn key_bytes(key: &AttributeStatsKey) -> Vec<u8> {
    let mut bytes = PREFIX.to_vec();
    push_key_part(&mut bytes, &key.relation.schema);
    push_key_part(&mut bytes, &key.relation.name);
    bytes.extend_from_slice(&key.attnum.to_be_bytes());
    bytes.push(u8::from(key.inherited));
    bytes
}

fn key_from_bytes(key: &[u8]) -> Result<AttributeStatsKey, ExecError> {
    let mut rest = key
        .get(PREFIX.len()..)
        .ok_or_else(|| corrupt("attribute statistics key is shorter than its prefix"))?;
    let schema = key_part(&mut rest)?;
    let name = key_part(&mut rest)?;
    let (attnum, rest_after_attnum) = rest
        .split_first_chunk::<2>()
        .ok_or_else(|| corrupt("truncated attribute statistics key attnum"))?;
    let (&inherited, tail) = rest_after_attnum
        .split_first()
        .ok_or_else(|| corrupt("truncated attribute statistics key inherited flag"))?;
    if !tail.is_empty() || inherited > 1 {
        return Err(corrupt("invalid attribute statistics key"));
    }
    Ok(AttributeStatsKey {
        relation: RelationName::new(schema, name),
        attnum: i16::from_be_bytes(*attnum),
        inherited: inherited == 1,
    })
}

fn encode(stats: AttributeStats) -> Vec<u8> {
    let mut value = Vec::with_capacity(64);
    push_f32(&mut value, stats.null_frac);
    push_i32(&mut value, stats.avg_width);
    push_f32(&mut value, stats.n_distinct);
    push_text(&mut value, stats.most_common_vals.as_deref());
    push_text(&mut value, stats.most_common_freqs.as_deref());
    push_text(&mut value, stats.histogram_bounds.as_deref());
    push_f32(&mut value, stats.correlation);
    push_text(&mut value, stats.most_common_elems.as_deref());
    push_text(&mut value, stats.most_common_elem_freqs.as_deref());
    push_text(&mut value, stats.elem_count_histogram.as_deref());
    push_text(&mut value, stats.range_length_histogram.as_deref());
    push_f32(&mut value, stats.range_empty_frac);
    push_text(&mut value, stats.range_bounds_histogram.as_deref());
    value
}

fn decode(value: &[u8]) -> Result<AttributeStats, ExecError> {
    let mut rest = value;
    let null_frac = take_f32(&mut rest)?;
    let avg_width = take_i32(&mut rest)?;
    let n_distinct = take_f32(&mut rest)?;
    let most_common_vals = take_text(&mut rest)?;
    let most_common_freqs = take_text(&mut rest)?;
    let histogram_bounds = take_text(&mut rest)?;
    let correlation = take_f32(&mut rest)?;
    let most_common_elems = take_text(&mut rest)?;
    let most_common_elem_freqs = take_text(&mut rest)?;
    let elem_count_histogram = take_text(&mut rest)?;
    let range_length_histogram = take_text(&mut rest)?;
    let range_empty_frac = take_f32(&mut rest)?;
    let range_bounds_histogram = take_text(&mut rest)?;
    if !rest.is_empty() {
        return Err(corrupt("trailing attribute statistics bytes"));
    }
    Ok(AttributeStats {
        null_frac,
        avg_width,
        n_distinct,
        most_common_vals,
        most_common_freqs,
        histogram_bounds,
        correlation,
        most_common_elems,
        most_common_elem_freqs,
        elem_count_histogram,
        range_length_histogram,
        range_empty_frac,
        range_bounds_histogram,
    })
}

fn push_f32(value: &mut Vec<u8>, field: Option<f32>) {
    value.push(u8::from(field.is_some()));
    value.extend_from_slice(&field.unwrap_or_default().to_be_bytes());
}

fn push_i32(value: &mut Vec<u8>, field: Option<i32>) {
    value.push(u8::from(field.is_some()));
    value.extend_from_slice(&field.unwrap_or_default().to_be_bytes());
}

fn push_text(value: &mut Vec<u8>, field: Option<&str>) {
    let Some(field) = field else {
        value.push(0);
        return;
    };
    value.push(1);
    value.extend_from_slice(
        &u32::try_from(field.len())
            .expect("attribute statistics text length exceeds u32")
            .to_be_bytes(),
    );
    value.extend_from_slice(field.as_bytes());
}

fn take_f32(value: &mut &[u8]) -> Result<Option<f32>, ExecError> {
    let (&present, rest) = value
        .split_first()
        .ok_or_else(|| corrupt("truncated attribute statistics field"))?;
    let (bytes, tail) = rest
        .split_first_chunk::<4>()
        .ok_or_else(|| corrupt("truncated attribute statistics field"))?;
    *value = tail;
    match present {
        0 => Ok(None),
        1 => Ok(Some(f32::from_be_bytes(*bytes))),
        _ => Err(corrupt("invalid attribute statistics field presence")),
    }
}

fn take_i32(value: &mut &[u8]) -> Result<Option<i32>, ExecError> {
    let (&present, rest) = value
        .split_first()
        .ok_or_else(|| corrupt("truncated attribute statistics field"))?;
    let (bytes, tail) = rest
        .split_first_chunk::<4>()
        .ok_or_else(|| corrupt("truncated attribute statistics field"))?;
    *value = tail;
    match present {
        0 => Ok(None),
        1 => Ok(Some(i32::from_be_bytes(*bytes))),
        _ => Err(corrupt("invalid attribute statistics field presence")),
    }
}

fn take_text(value: &mut &[u8]) -> Result<Option<String>, ExecError> {
    let (&present, rest) = value
        .split_first()
        .ok_or_else(|| corrupt("truncated attribute statistics field"))?;
    match present {
        0 => {
            *value = rest;
            Ok(None)
        }
        1 => {
            let (length, text) = rest
                .split_first_chunk::<4>()
                .ok_or_else(|| corrupt("truncated attribute statistics text length"))?;
            let length = usize::try_from(u32::from_be_bytes(*length))
                .map_err(|_| corrupt("attribute statistics text length exceeds usize"))?;
            if text.len() < length {
                return Err(corrupt("truncated attribute statistics text"));
            }
            let (text, tail) = text.split_at(length);
            *value = tail;
            String::from_utf8(text.to_vec())
                .map(Some)
                .map_err(|_| corrupt("attribute statistics text is not UTF-8"))
        }
        _ => Err(corrupt("invalid attribute statistics field presence")),
    }
}

fn key_part(cur: &mut &[u8]) -> Result<String, ExecError> {
    let (length, rest) = cur
        .split_first_chunk::<4>()
        .ok_or_else(|| corrupt("truncated attribute statistics key"))?;
    let len = usize::try_from(u32::from_be_bytes(*length))
        .map_err(|_| corrupt("attribute statistics key name length exceeds usize"))?;
    if rest.len() < len {
        return Err(corrupt("truncated attribute statistics key name"));
    }
    let (name, tail) = rest.split_at(len);
    *cur = tail;
    String::from_utf8(name.to_vec())
        .map_err(|_| corrupt("attribute statistics key name is not UTF-8"))
}

fn corrupt(message: &str) -> ExecError {
    ExecError::Kv(crabka_pgkv::KvError::CorruptRow(message.into()))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgkv::{Kv, MemKv};

    use super::{
        AttributeStats, AttributeStatsKey, PREFIX, RelationName, all, clear_op, decode, encode,
        get, key_bytes, key_from_bytes, key_part, rename_ops, set_op, take_text,
    };

    #[test]
    fn stored_statistics_round_trip_and_clear_by_full_key() {
        let kv = MemKv::new();
        let key = AttributeStatsKey {
            relation: RelationName::new("s", "t"),
            attnum: 2,
            inherited: true,
        };
        let stats = AttributeStats {
            null_frac: Some(0.25),
            avg_width: Some(17),
            n_distinct: Some(-0.5),
            most_common_vals: Some("{1,2}".into()),
            most_common_freqs: Some("{0.2,0.8}".into()),
            histogram_bounds: Some("{1,2,3}".into()),
            correlation: Some(0.75),
            most_common_elems: Some("{a,b}".into()),
            most_common_elem_freqs: Some("{0.2,0.8,0.2,0.8}".into()),
            elem_count_histogram: Some("{1,2,3}".into()),
            range_length_histogram: Some("{1,2,3}".into()),
            range_empty_frac: Some(0.5),
            range_bounds_histogram: Some("{\"[1,2)\"}".into()),
        };
        kv.write_batch(&[set_op(&key, stats.clone())])
            .expect("write");
        assert!(get(&kv, &key).expect("read") == Some(stats.clone()));
        assert!(all(&kv).expect("all").get(&key) == Some(&stats));
        kv.write_batch(&[clear_op(&key)]).expect("clear");
        assert!(get(&kv, &key).expect("read") == None);
    }

    #[test]
    fn decoder_rejects_invalid_keys_and_preserves_absent_fields() {
        let key = AttributeStatsKey {
            relation: RelationName::new("s", "t"),
            attnum: 2,
            inherited: false,
        };
        assert!(key_from_bytes(&key_bytes(&key)).expect("valid key") == key);

        let mut invalid_flag = key_bytes(&key);
        *invalid_flag.last_mut().expect("inherited flag") = 2;
        assert!(key_from_bytes(&invalid_flag).is_err());
        let mut trailing = key_bytes(&key);
        trailing.push(0);
        assert!(key_from_bytes(&trailing).is_err());

        let mut truncated = PREFIX.to_vec();
        truncated.extend_from_slice(&1_u32.to_be_bytes());
        truncated.push(b's');
        truncated.extend_from_slice(&2_u32.to_be_bytes());
        truncated.push(b't');
        assert!(key_from_bytes(&truncated).is_err());

        let mut exact_name = [0, 0, 0, 1, b's'].as_slice();
        assert!(key_part(&mut exact_name).expect("exact name") == "s");
        assert!(exact_name.is_empty());

        let mut exact_text = [1, 0, 0, 0, 1, b'x'].as_slice();
        assert!(take_text(&mut exact_text).expect("exact text") == Some("x".into()));
        assert!(exact_text.is_empty());
        let mut truncated_text = [1, 0, 0, 0, 2, b'x'].as_slice();
        assert!(take_text(&mut truncated_text).is_err());

        let absent = AttributeStats::default();
        assert!(decode(&encode(absent.clone())).expect("absent fields") == absent);
    }

    #[test]
    fn rename_moves_every_attribute_statistic() {
        let kv = MemKv::new();
        let before = RelationName::new("s", "before");
        let after = RelationName::new("s", "after");
        for attnum in [1, 2] {
            kv.write_batch(&[set_op(
                &AttributeStatsKey {
                    relation: before.clone(),
                    attnum,
                    inherited: attnum == 2,
                },
                AttributeStats::default(),
            )])
            .expect("write");
        }

        kv.write_batch(&rename_ops(&kv, &before, &after).expect("rename"))
            .expect("apply rename");
        assert!(
            all(&kv)
                .expect("all")
                .keys()
                .all(|key| key.relation == after)
        );
    }
}
