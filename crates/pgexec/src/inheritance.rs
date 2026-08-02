//! PostgreSQL table-inheritance catalog links.

use crabka_pgcatalog::RelationName;
use crabka_pgkv::{Kv, WriteOp, key::push_key_part};

use crate::error::ExecError;

const PARENTS_PREFIX: &[u8] = b"\0\0\0\0catalog_inheritance/parents/";
const CHILDREN_PREFIX: &[u8] = b"\0\0\0\0catalog_inheritance/children/";
const VERSION: u8 = 1;

fn relation_key(prefix: &[u8], relation: &RelationName) -> Vec<u8> {
    let mut key = prefix.to_vec();
    push_key_part(&mut key, &relation.schema);
    push_key_part(&mut key, &relation.name);
    key
}

fn child_index_key(parent: &RelationName, child: &RelationName) -> Vec<u8> {
    let mut key = relation_key(CHILDREN_PREFIX, parent);
    push_key_part(&mut key, &child.schema);
    push_key_part(&mut key, &child.name);
    key
}

pub(crate) fn attach_ops(child: &RelationName, parents: &[RelationName]) -> Vec<WriteOp> {
    let mut value = vec![VERSION];
    value.extend_from_slice(
        &u32::try_from(parents.len())
            .expect("inheritance parent count exceeds u32")
            .to_be_bytes(),
    );
    for parent in parents {
        write_string(&mut value, &parent.schema);
        write_string(&mut value, &parent.name);
    }
    let mut ops = vec![WriteOp::Put {
        key: relation_key(PARENTS_PREFIX, child),
        value,
    }];
    ops.extend(parents.iter().map(|parent| WriteOp::Put {
        key: child_index_key(parent, child),
        value: Vec::new(),
    }));
    ops
}

pub(crate) fn parents_of(
    kv: &dyn Kv,
    child: &RelationName,
) -> Result<Vec<RelationName>, ExecError> {
    let Some(value) = kv
        .get(&relation_key(PARENTS_PREFIX, child))
        .map_err(ExecError::Kv)?
    else {
        return Ok(Vec::new());
    };
    let mut cur = value.as_slice();
    if take(&mut cur, 1)?[0] != VERSION {
        return Err(corrupt("unknown inheritance record version"));
    }
    let count = u32::from_be_bytes(take(&mut cur, 4)?.try_into().expect("4"));
    let mut parents = Vec::with_capacity(usize::try_from(count).expect("u32 fits usize"));
    for _ in 0..count {
        parents.push(RelationName::new(
            read_string(&mut cur)?,
            read_string(&mut cur)?,
        ));
    }
    if !cur.is_empty() {
        return Err(corrupt("trailing inheritance record bytes"));
    }
    Ok(parents)
}

pub(crate) fn children_of(
    kv: &dyn Kv,
    parent: &RelationName,
) -> Result<Vec<RelationName>, ExecError> {
    let prefix = relation_key(CHILDREN_PREFIX, parent);
    kv.scan_prefix(&prefix)
        .map_err(ExecError::Kv)?
        .into_iter()
        .map(|(key, _)| {
            let mut suffix = &key[prefix.len()..];
            let schema = key_part(&mut suffix)?;
            let name = key_part(&mut suffix)?;
            if !suffix.is_empty() {
                return Err(corrupt("trailing inheritance child key bytes"));
            }
            Ok(RelationName::new(schema, name))
        })
        .collect()
}

pub(crate) fn descendants(
    kv: &dyn Kv,
    parent: &RelationName,
) -> Result<Vec<RelationName>, ExecError> {
    let mut out = Vec::new();
    let mut pending = children_of(kv, parent)?;
    while let Some(child) = pending.pop() {
        pending.extend(children_of(kv, &child)?);
        out.push(child);
    }
    Ok(out)
}

fn write_string(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(
        &u32::try_from(value.len())
            .expect("catalog name exceeds u32")
            .to_be_bytes(),
    );
    out.extend_from_slice(value.as_bytes());
}

fn read_string(cur: &mut &[u8]) -> Result<String, ExecError> {
    let len = u32::from_be_bytes(take(cur, 4)?.try_into().expect("4"));
    String::from_utf8(take(cur, usize::try_from(len).expect("u32 fits usize"))?.to_vec())
        .map_err(|_| corrupt("inheritance name is not UTF-8"))
}

fn key_part(cur: &mut &[u8]) -> Result<String, ExecError> {
    let len = u32::from_be_bytes(take(cur, 4)?.try_into().expect("4"));
    String::from_utf8(take(cur, usize::try_from(len).expect("u32 fits usize"))?.to_vec())
        .map_err(|_| corrupt("inheritance key name is not UTF-8"))
}

fn take<'a>(cur: &mut &'a [u8], len: usize) -> Result<&'a [u8], ExecError> {
    if cur.len() < len {
        return Err(corrupt("truncated inheritance record"));
    }
    let (head, tail) = cur.split_at(len);
    *cur = tail;
    Ok(head)
}

fn corrupt(message: &str) -> ExecError {
    ExecError::Kv(crabka_pgkv::KvError::CorruptRow(message.into()))
}
