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

fn encode_parents(parents: &[RelationName]) -> Vec<u8> {
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
    value
}

/// Write ops that record `child`'s parents and each parent's back-link.
///
/// Every parent also gets its `pg_class.relhassubclass` latched, which is what
/// `PostgreSQL` does when a child appears. Nothing here ever clears it —
/// dropping the last child leaves the flag set until an `ANALYZE` looks, and
/// `expected/vacuum.out` reads a parent inside that window.
pub(crate) fn attach_ops(child: &RelationName, parents: &[RelationName]) -> Vec<WriteOp> {
    let mut ops = vec![WriteOp::Put {
        key: relation_key(PARENTS_PREFIX, child),
        value: encode_parents(parents),
    }];
    for parent in parents {
        ops.push(WriteOp::Put {
            key: child_index_key(parent, child),
            value: Vec::new(),
        });
        ops.push(crate::relstats::set_has_subclass_op(parent));
    }
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

/// Whether `parent` has any direct inheritance child.
///
/// Every `UPDATE` and `DELETE` in the engine asks this to decide whether it is
/// a tree write, and almost every one of them is answered "no" — so it stops at
/// the first key and reads none of the values, rather than building the child
/// list through [`children_of`] and throwing it away. The names are read later,
/// once, by the writes that turn out to need them.
pub(crate) fn has_children(kv: &dyn Kv, parent: &RelationName) -> Result<bool, ExecError> {
    let start = relation_key(CHILDREN_PREFIX, parent);
    let mut end = start.clone();
    // The exclusive bound above every key starting with `start`. The prefix ends
    // in a length-prefixed name, so it can never be all-`0xff` and the increment
    // always lands.
    for byte in end.iter_mut().rev() {
        if *byte == u8::MAX {
            *byte = 0;
        } else {
            *byte += 1;
            break;
        }
    }
    let seen = kv
        .for_each_key(&start, &end, 1, &mut |_| ())
        .map_err(ExecError::Kv)?;
    Ok(seen > 0)
}

/// Every relation below `parent`, each named once.
///
/// The visited set is load-bearing rather than defensive: multiple inheritance
/// makes the graph a DAG, so `d INHERITS (b, c)` under `b, c INHERITS a` is
/// reachable from `a` by two paths. Yielding `d` twice makes
/// [`inherited_scan`](crate::exec) read its rows twice, and `SELECT * FROM a`
/// silently returns duplicates.
pub(crate) fn descendants(
    kv: &dyn Kv,
    parent: &RelationName,
) -> Result<Vec<RelationName>, ExecError> {
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut pending = children_of(kv, parent)?;
    while let Some(child) = pending.pop() {
        if !seen.insert(child.clone()) {
            continue;
        }
        pending.extend(children_of(kv, &child)?);
        out.push(child);
    }
    Ok(out)
}

/// Remove every inheritance link that mentions `name`, so dropping a relation
/// cannot leave a dangling one behind.
///
/// Three link kinds have to go, and missing any one breaks a *different* read:
/// the relation's own parent list, the index entry each parent holds pointing
/// at it (leaving that behind makes the surviving parent unreadable, because
/// scanning it resolves a child that no longer exists), and the parent list of
/// every child, which names the relation being dropped and would otherwise make
/// `pg_inherits` — and so psql's `\d` — fail for the whole database.
pub(crate) fn drop_metadata_ops(
    kv: &dyn Kv,
    name: &RelationName,
) -> Result<Vec<WriteOp>, ExecError> {
    let mut ops = vec![WriteOp::Delete {
        key: relation_key(PARENTS_PREFIX, name),
    }];
    for parent in parents_of(kv, name)? {
        ops.push(WriteOp::Delete {
            key: child_index_key(&parent, name),
        });
    }
    for child in children_of(kv, name)? {
        ops.push(WriteOp::Delete {
            key: child_index_key(name, &child),
        });
        let remaining: Vec<RelationName> = parents_of(kv, &child)?
            .into_iter()
            .filter(|parent| parent != name)
            .collect();
        let key = relation_key(PARENTS_PREFIX, &child);
        ops.push(if remaining.is_empty() {
            WriteOp::Delete { key }
        } else {
            WriteOp::Put {
                key,
                value: encode_parents(&remaining),
            }
        });
    }
    Ok(ops)
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
