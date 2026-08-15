//! PostgreSQL table-inheritance catalog links.

use std::collections::{BTreeMap, BTreeSet, HashSet};

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
    decode_parents(&value)
}

fn decode_parents(value: &[u8]) -> Result<Vec<RelationName>, ExecError> {
    let mut cur = value;
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

/// Every relation below `parent`, each named once, in the order `PostgreSQL`
/// appends them to a read of the tree.
///
/// The visited set is load-bearing rather than defensive: multiple inheritance
/// makes the graph a DAG, so `d INHERITS (b, c)` under `b, c INHERITS a` is
/// reachable from `a` by two paths. Yielding `d` twice makes
/// [`inherited_scan`](crate::exec) read its rows twice, and `SELECT * FROM a`
/// silently returns duplicates.
///
/// # Why the queue is a queue
///
/// `PostgreSQL`'s `find_all_inheritors` walks the tree breadth-first, and the
/// order is observable: an unordered `SELECT * FROM parent` returns each
/// relation's rows in exactly the order the walk names them. Measured on 18.4
/// over `a` with children `b, c` and grandchildren `d, e` below `b`, the rows
/// arrive `a, b, c, d, e` — a whole level before the next one, not `b`'s
/// subtree before `c`. A stack yields `a, c, b, e, d` instead, which is two
/// wrong answers rather than one: the wrong level order and the siblings
/// reversed.
pub(crate) fn descendants(
    kv: &dyn Kv,
    parent: &RelationName,
) -> Result<Vec<RelationName>, ExecError> {
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut pending = std::collections::VecDeque::from(children_of(kv, parent)?);
    while let Some(child) = pending.pop_front() {
        if !seen.insert(child.clone()) {
            continue;
        }
        pending.extend(children_of(kv, &child)?);
        out.push(child);
    }
    Ok(out)
}

/// Remove every inheritance link that `dropping` takes part in, so a drop
/// cannot leave a dangling one behind.
///
/// `dropping` is every relation the statement removes, not one of them. Three
/// link kinds have to go, and missing any one breaks a *different* read: each
/// departing relation's own parent list, the index entry each parent holds
/// pointing at it (leaving that behind makes the surviving parent unreadable,
/// because scanning it resolves a child that no longer exists), and the parent
/// list of every child, which names a departing relation and would otherwise
/// make `pg_inherits` — and so psql's `\d` — fail for the whole database.
///
/// # Why the whole set at once
///
/// A child's new parent list is read back from the *committed* store, which
/// cannot see the rest of the batch. One relation at a time therefore wrote one
/// list per departing parent and let the last write stand: `DROP TABLE a, b`
/// over a child of both emitted `child -> [b]` for `a` and `child -> [a]` for
/// `b`, and the child was left naming a relation the same statement had just
/// removed. Worse, a child that is itself in the batch had its own `Delete`
/// overwritten by a parent's `Put`, which resurrected a parent list for a
/// relation that no longer existed — a key no read reaches until some later
/// `CREATE TABLE` takes the same name and silently adopts it.
///
/// Taking the set first fixes both. Each surviving child's list is a function
/// of the committed store and the set, never of the order the names were
/// written in.
///
/// The ops are collected by key rather than pushed, because the two ends of one
/// link both reach the same index entry: a departing child deletes the entry its
/// departing parent holds, and that parent deletes it again. See [`KeyedOps`].
pub(crate) fn drop_metadata_ops(
    kv: &dyn Kv,
    dropping: &HashSet<RelationName>,
) -> Result<Vec<WriteOp>, ExecError> {
    let mut ops = KeyedOps::default();
    let mut bereaved = BTreeSet::new();
    for name in dropping {
        ops.delete(relation_key(PARENTS_PREFIX, name));
        for parent in parents_of(kv, name)? {
            ops.delete(child_index_key(&parent, name));
        }
        for child in children_of(kv, name)? {
            ops.delete(child_index_key(name, &child));
            bereaved.insert(child);
        }
    }
    for child in bereaved {
        // A child inside the set is already gone, and the loop above deleted its
        // list. Rewriting that key would put back what the batch removes.
        if dropping.contains(&child) {
            continue;
        }
        let remaining: Vec<RelationName> = parents_of(kv, &child)?
            .into_iter()
            .filter(|parent| !dropping.contains(parent))
            .collect();
        let key = relation_key(PARENTS_PREFIX, &child);
        if remaining.is_empty() {
            ops.delete(key);
        } else {
            ops.put(key, encode_parents(&remaining));
        }
    }
    Ok(ops.into_batch())
}

/// Move every inheritance link that names `from` onto `to`, so a rename is
/// invisible to the tree the way it is in `PostgreSQL`.
///
/// Three link kinds carry the name, and each one lost breaks something
/// different. The relation's own parent list is keyed by it, so leaving it
/// behind detaches the renamed relation from its parents. Each parent holds an
/// index entry naming it, so leaving that behind makes the parent unreadable —
/// a tree scan resolves a child that no longer answers, and the whole `SELECT`
/// fails. Each child's parent list *contains* it, so leaving that behind
/// detaches every child and strands a name that a later `CREATE TABLE` of the
/// old name silently adopts, along with the rows underneath it.
///
/// The record the renamed relation keeps is moved byte for byte rather than
/// re-encoded: it names that relation's *parents*, and none of them changed.
///
/// The ops are collected by key ([`KeyedOps`]) because a relation that is both
/// a parent and a child of the same relation reaches one index entry from both
/// loops. The acyclic check refuses that shape, and this batch does not depend
/// on it having done so.
pub(crate) fn rename_ops(
    kv: &dyn Kv,
    from: &RelationName,
    to: &RelationName,
) -> Result<Vec<WriteOp>, ExecError> {
    let mut ops = KeyedOps::default();
    if let Some(record) = kv
        .get(&relation_key(PARENTS_PREFIX, from))
        .map_err(ExecError::Kv)?
    {
        let parents = decode_parents(&record)?;
        ops.delete(relation_key(PARENTS_PREFIX, from));
        ops.put(relation_key(PARENTS_PREFIX, to), record);
        for parent in parents {
            ops.delete(child_index_key(&parent, from));
            ops.put(child_index_key(&parent, to), Vec::new());
        }
    }
    for child in children_of(kv, from)? {
        ops.delete(child_index_key(from, &child));
        ops.put(child_index_key(to, &child), Vec::new());
        let parents: Vec<RelationName> = parents_of(kv, &child)?
            .into_iter()
            .map(|parent| if parent == *from { to.clone() } else { parent })
            .collect();
        ops.put(
            relation_key(PARENTS_PREFIX, &child),
            encode_parents(&parents),
        );
    }
    Ok(ops.into_batch())
}

/// A write batch that holds at most one op per key, whatever order the ops
/// arrive in.
///
/// Both [`drop_metadata_ops`] and [`rename_ops`] reach one key from two
/// directions, and the reason is the same each time: the two ends of an
/// inheritance link are two relations, and a statement that touches both ends
/// addresses the entry between them twice. Keyed collection makes "one op per
/// key" a property of the type rather than a rule to keep, and it puts the
/// batch in key order whatever order the caller built it in.
#[derive(Default)]
struct KeyedOps(BTreeMap<Vec<u8>, WriteOp>);

impl KeyedOps {
    fn delete(&mut self, key: Vec<u8>) {
        self.0.insert(key.clone(), WriteOp::Delete { key });
    }

    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.0.insert(key.clone(), WriteOp::Put { key, value });
    }

    fn into_batch(self) -> Vec<WriteOp> {
        self.0.into_values().collect()
    }
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

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgcatalog::RelationName;
    use crabka_pgkv::{Kv, MemKv, WriteOp, key::push_key_part};

    use super::{
        CHILDREN_PREFIX, PARENTS_PREFIX, attach_ops, children_of, descendants, drop_metadata_ops,
        parents_of, rename_ops,
    };

    fn relation(name: &str) -> RelationName {
        RelationName::new("public".to_string(), name.to_string())
    }

    fn dropping(names: &[&str]) -> std::collections::HashSet<RelationName> {
        names.iter().map(|name| relation(name)).collect()
    }

    /// A store holding `child INHERITS (parents…)` for each pair given.
    fn linked(links: &[(&str, &[&str])]) -> MemKv {
        let kv = MemKv::new();
        for (child, parents) in links {
            let parents: Vec<RelationName> = parents.iter().map(|name| relation(name)).collect();
            kv.write_batch(&attach_ops(&relation(child), &parents))
                .expect("attach");
        }
        kv
    }

    fn apply(kv: &MemKv, ops: Vec<WriteOp>) {
        kv.write_batch(&ops).expect("write batch");
    }

    /// Every inheritance key the store still holds, both directions.
    fn remaining_keys(kv: &MemKv) -> Vec<Vec<u8>> {
        let mut keys: Vec<Vec<u8>> = [PARENTS_PREFIX, CHILDREN_PREFIX]
            .into_iter()
            .flat_map(|prefix| {
                kv.scan_prefix(prefix)
                    .expect("scan")
                    .into_iter()
                    .map(|(key, _)| key)
            })
            .collect();
        keys.sort();
        keys
    }

    /// The key each op addresses, so a batch can be checked for two ops that
    /// disagree about one key — the shape whose outcome depends on apply order.
    fn op_keys(ops: &[WriteOp]) -> Vec<&[u8]> {
        ops.iter()
            .map(|op| match op {
                WriteOp::Put { key, .. } | WriteOp::Delete { key } => key.as_slice(),
                other => panic!("unexpected inheritance op {other:?}"),
            })
            .collect()
    }

    /// Whether any inheritance key or value still spells `name` — the question
    /// a rename has to answer "no" to, because a name left behind here is one a
    /// later `CREATE TABLE` of it silently adopts.
    fn anything_still_names(kv: &MemKv, name: &str) -> bool {
        let needle = {
            let mut bytes = Vec::new();
            push_key_part(&mut bytes, name);
            bytes
        };
        let window = |bytes: &[u8]| {
            bytes
                .windows(needle.len())
                .any(|window| window == needle.as_slice())
        };
        [PARENTS_PREFIX, CHILDREN_PREFIX].into_iter().any(|prefix| {
            kv.scan_prefix(prefix)
                .expect("scan")
                .into_iter()
                .any(|(key, value)| window(&key) || window(&value))
        })
    }

    #[test]
    fn a_renamed_relation_keeps_the_parents_it_had() {
        let kv = linked(&[("c", &["a", "b"])]);
        apply(
            &kv,
            rename_ops(&kv, &relation("c"), &relation("c2")).expect("ops"),
        );
        assert!(
            parents_of(&kv, &relation("c2")).expect("read") == vec![relation("a"), relation("b")]
        );
        assert!(children_of(&kv, &relation("a")).expect("read") == vec![relation("c2")]);
        assert!(children_of(&kv, &relation("b")).expect("read") == vec![relation("c2")]);
    }

    #[test]
    fn a_renamed_parent_is_named_by_the_children_it_had() {
        // Renaming the parent left the child naming a relation nothing carried:
        // `SELECT` over the parent lost the child's rows, and an `INSERT` into
        // the child failed on the ancestor walk.
        let kv = linked(&[("c", &["p"]), ("d", &["p", "q"])]);
        apply(
            &kv,
            rename_ops(&kv, &relation("p"), &relation("p2")).expect("ops"),
        );
        assert!(parents_of(&kv, &relation("c")).expect("read") == vec![relation("p2")]);
        assert!(
            parents_of(&kv, &relation("d")).expect("read") == vec![relation("p2"), relation("q")]
        );
        assert!(
            children_of(&kv, &relation("p2")).expect("read") == vec![relation("c"), relation("d")]
        );
    }

    #[test]
    fn the_old_name_is_left_holding_nothing() {
        // The consequence of leaving one behind is not a dangling key but a
        // silent adoption: `CREATE TABLE p` after `ALTER TABLE p RENAME TO p2`
        // took over p2's children, and reading the new empty table returned
        // their rows.
        let kv = linked(&[("mid", &["top"]), ("leaf", &["mid"])]);
        apply(
            &kv,
            rename_ops(&kv, &relation("mid"), &relation("middle")).expect("ops"),
        );
        assert!(!anything_still_names(&kv, "mid"));
        assert!(parents_of(&kv, &relation("leaf")).expect("read") == vec![relation("middle")]);
        assert!(children_of(&kv, &relation("top")).expect("read") == vec![relation("middle")]);
        assert!(
            descendants(&kv, &relation("top")).expect("walk")
                == vec![relation("middle"), relation("leaf")]
        );
    }

    #[test]
    fn the_rename_batch_addresses_each_key_once() {
        let kv = linked(&[("mid", &["a", "b"]), ("c", &["mid"]), ("d", &["mid"])]);
        let ops = rename_ops(&kv, &relation("mid"), &relation("middle")).expect("ops");
        let mut keys = op_keys(&ops);
        let total = keys.len();
        keys.sort_unstable();
        keys.dedup();
        assert!(keys.len() == total);
    }

    #[test]
    fn renaming_a_relation_with_no_links_writes_nothing() {
        let kv = linked(&[("c", &["p"])]);
        assert!(rename_ops(&kv, &relation("lonely"), &relation("lonely2")).expect("ops") == vec![]);
    }

    #[test]
    fn a_child_of_two_departing_parents_is_left_naming_neither() {
        // The defect: one read-modify-write per parent, each blind to the other,
        // left `c -> [a]` or `c -> [b]` depending on which Put the batch applied
        // last — a live child naming a relation the same statement removed.
        let kv = linked(&[("c", &["a", "b"])]);
        apply(
            &kv,
            drop_metadata_ops(&kv, &dropping(&["a", "b"])).expect("ops"),
        );
        assert!(parents_of(&kv, &relation("c")).expect("read") == Vec::new());
        assert!(remaining_keys(&kv).is_empty());
    }

    #[test]
    fn a_child_keeps_exactly_the_parents_the_batch_leaves_behind() {
        let kv = linked(&[("c", &["a", "b", "d"])]);
        apply(
            &kv,
            drop_metadata_ops(&kv, &dropping(&["a", "d"])).expect("ops"),
        );
        assert!(parents_of(&kv, &relation("c")).expect("read") == vec![relation("b")]);
        assert!(children_of(&kv, &relation("b")).expect("read") == vec![relation("c")]);
        assert!(children_of(&kv, &relation("a")).expect("read") == Vec::new());
        assert!(children_of(&kv, &relation("d")).expect("read") == Vec::new());
    }

    #[test]
    fn a_departing_child_is_not_resurrected_by_a_departing_parent() {
        // `DROP TABLE c, a, b` names everything and is what `PostgreSQL`
        // requires, yet the per-parent rewrite put `c`'s list back after `c`'s
        // own Delete. Nothing reads that key until a later `CREATE TABLE c`
        // adopts it, at which point `pg_inherits` fails for the whole database.
        let kv = linked(&[("c", &["a", "b"])]);
        apply(
            &kv,
            drop_metadata_ops(&kv, &dropping(&["a", "b", "c"])).expect("ops"),
        );
        assert!(remaining_keys(&kv).is_empty());
    }

    #[test]
    fn the_batch_addresses_each_key_once() {
        // Two ops on one key make the result depend on the order the batch is
        // applied in. This is the property that makes the rewrite order-free,
        // and it is stronger than checking the store after one apply.
        let kv = linked(&[
            ("c", &["a", "b"]),
            ("d", &["a", "b"]),
            ("e", &["a"]),
            ("g", &["c"]),
        ]);
        let ops = drop_metadata_ops(&kv, &dropping(&["a", "b", "c"])).expect("ops");
        let mut keys = op_keys(&ops);
        let total = keys.len();
        keys.sort_unstable();
        keys.dedup();
        assert!(keys.len() == total);
    }

    #[test]
    fn the_batch_does_not_depend_on_the_order_the_set_iterates_in() {
        let ops = |names: &[&str]| {
            let kv = linked(&[("c", &["a", "b"]), ("d", &["b", "a"])]);
            drop_metadata_ops(&kv, &dropping(names)).expect("ops")
        };
        assert!(ops(&["a", "b"]) == ops(&["b", "a"]));
    }

    #[test]
    fn a_grandchild_keeps_its_link_to_a_child_that_stays() {
        // Dropping the top of a three-level tree must not touch the link
        // between the two levels below it.
        let kv = linked(&[("mid", &["top"]), ("leaf", &["mid"])]);
        apply(
            &kv,
            drop_metadata_ops(&kv, &dropping(&["top"])).expect("ops"),
        );
        assert!(parents_of(&kv, &relation("mid")).expect("read") == Vec::new());
        assert!(parents_of(&kv, &relation("leaf")).expect("read") == vec![relation("mid")]);
        assert!(children_of(&kv, &relation("mid")).expect("read") == vec![relation("leaf")]);
    }

    #[test]
    fn the_walk_names_a_whole_level_before_the_next_one() {
        // The order is observable: an unordered `SELECT * FROM a` returns each
        // relation's rows in the order this walk names them, and PostgreSQL
        // 18.4 answers `a, b, c, d, e` for this tree. A stack answers
        // `c, b, e, d`.
        let kv = linked(&[("b", &["a"]), ("c", &["a"]), ("d", &["b"]), ("e", &["b"])]);
        assert!(
            descendants(&kv, &relation("a")).expect("walk")
                == vec![relation("b"), relation("c"), relation("d"), relation("e")]
        );
    }

    #[test]
    fn the_walk_names_a_relation_reachable_by_two_paths_once() {
        let kv = linked(&[("b", &["a"]), ("c", &["a"]), ("d", &["b", "c"])]);
        assert!(
            descendants(&kv, &relation("a")).expect("walk")
                == vec![relation("b"), relation("c"), relation("d")]
        );
    }

    #[test]
    fn a_relation_with_no_links_contributes_only_its_own_erasure() {
        let kv = linked(&[]);
        let ops = drop_metadata_ops(&kv, &dropping(&["lonely"])).expect("ops");
        assert!(ops.len() == 1);
        apply(&kv, ops);
        assert!(remaining_keys(&kv).is_empty());
    }
}
