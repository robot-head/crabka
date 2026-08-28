//! Durable large-object metadata and pages.
//!
//! The executor owns descriptors and permissions; this module owns the
//! transactionally-written object bytes and the ACL metadata they address.

use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU32, Ordering},
};

use crabka_pgkv::{Kv, KvError, WriteOp};

use crate::CatalogError;

/// PostgreSQL's `LOBLKSIZE`: the `pg_largeobject` page payload size.
pub const PAGE_SIZE: usize = 2_048;
const FIRST_OID: u32 = 16_384;
const METADATA_PREFIX: &[u8] = b"\0\0\0\0catalog_largeobject/metadata/";
const PAGE_PREFIX: &[u8] = b"\0\0\0\0catalog_largeobject/page/";
const METADATA_VERSION: u8 = 1;
static PROCESS_NEXT_OID: AtomicU32 = AtomicU32::new(FIRST_OID);

/// One explicit `SELECT`/`UPDATE` ACL entry from `pg_largeobject_metadata`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclEntry {
    pub grantee: String,
    pub grantor: String,
    pub select: bool,
    pub update: bool,
    pub grant_select: bool,
    pub grant_update: bool,
}

/// The durable half of one large object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metadata {
    pub oid: u32,
    pub owner: String,
    pub acl: Vec<AclEntry>,
}

/// One `pg_largeobject` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page {
    pub oid: u32,
    pub page_no: u32,
    pub data: Vec<u8>,
}

/// Allocate a free large-object OID and build its metadata write.
///
/// The caller must serialize this with other catalog creation, as it does for
/// relation OID allocation. `0` asks for the next OID; a nonzero OID is used
/// unchanged.
///
/// # Errors
///
/// Returns duplicate-object or storage/corruption errors from the catalog KV
/// seam.
pub fn create_ops(
    kv: &dyn Kv,
    requested_oid: u32,
    owner: &str,
) -> Result<(u32, Vec<WriteOp>), CatalogError> {
    let oid = if requested_oid == 0 {
        next_oid(kv)?
    } else {
        requested_oid
    };
    if kv.get(&metadata_key(oid))?.is_some() {
        return Err(CatalogError::DuplicateLargeObject(oid));
    }
    let metadata = Metadata {
        oid,
        owner: owner.to_string(),
        acl: Vec::new(),
    };
    Ok((oid, vec![put_metadata_op(&metadata)]))
}

/// Read one large object's durable metadata.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV
/// seam.
pub fn get_metadata(kv: &dyn Kv, oid: u32) -> Result<Metadata, CatalogError> {
    let bytes = kv
        .get(&metadata_key(oid))?
        .ok_or(CatalogError::UndefinedLargeObject(oid))?;
    deserialize_metadata(oid, &bytes).map_err(CatalogError::from)
}

/// List large objects in OID order.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn list_metadata(kv: &dyn Kv) -> Result<Vec<Metadata>, CatalogError> {
    kv.scan_prefix(METADATA_PREFIX)?
        .into_iter()
        .map(|(key, bytes)| {
            let oid = oid_from_metadata_key(&key)?;
            deserialize_metadata(oid, &bytes).map_err(CatalogError::from)
        })
        .collect()
}

/// Replace metadata after an ACL or ownership change.
#[must_use]
pub fn put_metadata_op(metadata: &Metadata) -> WriteOp {
    WriteOp::Put {
        key: metadata_key(metadata.oid),
        value: serialize_metadata(metadata),
    }
}

/// Read all bytes, materialising sparse pages as zeroes.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV
/// seam.
pub fn read(kv: &dyn Kv, oid: u32) -> Result<Vec<u8>, CatalogError> {
    let _ = get_metadata(kv, oid)?;
    let pages = list_pages(kv, oid)?;
    let Some(last) = pages.last() else {
        return Ok(Vec::new());
    };
    let last_end = usize::try_from(last.page_no)
        .ok()
        .and_then(|page| page.checked_mul(PAGE_SIZE))
        .and_then(|offset| offset.checked_add(last.data.len()))
        .ok_or_else(too_large)?;
    let mut bytes = vec![0; last_end];
    for page in pages {
        let offset = usize::try_from(page.page_no)
            .ok()
            .and_then(|page| page.checked_mul(PAGE_SIZE))
            .ok_or_else(too_large)?;
        bytes[offset..offset + page.data.len()].copy_from_slice(&page.data);
    }
    Ok(bytes)
}

/// Build atomic page rewrites for a whole object.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV
/// seam.
pub fn replace_ops(kv: &dyn Kv, oid: u32, bytes: &[u8]) -> Result<Vec<WriteOp>, CatalogError> {
    let _ = get_metadata(kv, oid)?;
    replace_page_ops(kv, oid, bytes)
}

/// Build page rewrites without checking metadata.
///
/// A caller creating an object includes [`put_metadata_op`] in the same atomic
/// batch, so its metadata is intentionally not visible while these page ops
/// are built.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn replace_page_ops(kv: &dyn Kv, oid: u32, bytes: &[u8]) -> Result<Vec<WriteOp>, CatalogError> {
    let pages = bytes
        .chunks(PAGE_SIZE)
        .enumerate()
        .map(|(page_no, data)| {
            Ok((
                u32::try_from(page_no).map_err(|_| too_large())?,
                data.to_vec(),
            ))
        })
        .collect::<Result<BTreeMap<_, _>, CatalogError>>()?;
    replace_sparse_page_ops(kv, oid, &pages)
}

/// Replace an object with the provided sparse page map.
///
/// Missing pages represent zero-filled holes; callers keep the final page so
/// its payload length records the logical object length.
pub fn replace_sparse_page_ops(
    kv: &dyn Kv,
    oid: u32,
    pages: &BTreeMap<u32, Vec<u8>>,
) -> Result<Vec<WriteOp>, CatalogError> {
    let mut ops = list_pages(kv, oid)?
        .into_iter()
        .map(|page| WriteOp::Delete {
            key: page_key(oid, page.page_no),
        })
        .collect::<Vec<_>>();
    ops.extend(
        pages
            .iter()
            .map(|(page_no, data)| {
                if data.is_empty() || data.len() > PAGE_SIZE {
                    return Err(too_large());
                }
                Ok(WriteOp::Put {
                    key: page_key(oid, *page_no),
                    value: data.clone(),
                })
            })
            .collect::<Result<Vec<_>, CatalogError>>()?,
    );
    Ok(ops)
}

/// Build atomic deletion of an object and every page it owns.
///
/// # Errors
///
/// Returns undefined-object or storage/corruption errors from the catalog KV
/// seam.
pub fn unlink_ops(kv: &dyn Kv, oid: u32) -> Result<Vec<WriteOp>, CatalogError> {
    let _ = get_metadata(kv, oid)?;
    let mut ops = vec![WriteOp::Delete {
        key: metadata_key(oid),
    }];
    ops.extend(
        list_pages(kv, oid)?
            .into_iter()
            .map(|page| WriteOp::Delete {
                key: page_key(oid, page.page_no),
            }),
    );
    Ok(ops)
}

/// List one object's pages in page-number order.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn list_pages(kv: &dyn Kv, oid: u32) -> Result<Vec<Page>, CatalogError> {
    let prefix = page_prefix(oid);
    kv.scan_prefix(&prefix)?
        .into_iter()
        .map(|(key, data)| {
            let page_no = page_no_from_key(&key, &prefix)?;
            if data.len() > PAGE_SIZE {
                return Err(
                    KvError::CorruptRow("large object page exceeds LOBLKSIZE".into()).into(),
                );
            }
            Ok(Page { oid, page_no, data })
        })
        .collect()
}

fn next_oid(kv: &dyn Kv) -> Result<u32, CatalogError> {
    let floor = list_metadata(kv)?
        .into_iter()
        .map(|metadata| metadata.oid)
        .max();
    let floor = floor.map_or(Ok(FIRST_OID), |oid| {
        oid.checked_add(1).ok_or_else(too_large)
    })?;
    loop {
        let observed = PROCESS_NEXT_OID.load(Ordering::Relaxed);
        let oid = observed.max(floor);
        let next = oid.checked_add(1).ok_or_else(too_large)?;
        if PROCESS_NEXT_OID
            .compare_exchange_weak(observed, next, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return Ok(oid);
        }
    }
}

fn metadata_key(oid: u32) -> Vec<u8> {
    let mut key = METADATA_PREFIX.to_vec();
    key.extend_from_slice(&oid.to_be_bytes());
    key
}

fn page_prefix(oid: u32) -> Vec<u8> {
    let mut key = PAGE_PREFIX.to_vec();
    key.extend_from_slice(&oid.to_be_bytes());
    key
}

fn page_key(oid: u32, page_no: u32) -> Vec<u8> {
    let mut key = page_prefix(oid);
    key.extend_from_slice(&page_no.to_be_bytes());
    key
}

fn oid_from_metadata_key(key: &[u8]) -> Result<u32, KvError> {
    let Some(bytes) = key.strip_prefix(METADATA_PREFIX) else {
        return Err(KvError::CorruptRow(
            "large object metadata key has wrong prefix".into(),
        ));
    };
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| KvError::CorruptRow("large object metadata key has wrong length".into()))?;
    Ok(u32::from_be_bytes(bytes))
}

fn page_no_from_key(key: &[u8], prefix: &[u8]) -> Result<u32, KvError> {
    let Some(bytes) = key.strip_prefix(prefix) else {
        return Err(KvError::CorruptRow(
            "large object page key has wrong prefix".into(),
        ));
    };
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| KvError::CorruptRow("large object page key has wrong length".into()))?;
    Ok(u32::from_be_bytes(bytes))
}

fn serialize_metadata(metadata: &Metadata) -> Vec<u8> {
    let mut out = vec![METADATA_VERSION];
    put_string(&mut out, &metadata.owner);
    out.extend_from_slice(
        &u32::try_from(metadata.acl.len())
            .expect("ACL count cannot exceed u32")
            .to_be_bytes(),
    );
    for acl in &metadata.acl {
        put_string(&mut out, &acl.grantee);
        put_string(&mut out, &acl.grantor);
        out.push(u8::from(acl.select));
        out.push(u8::from(acl.update));
        out.push(u8::from(acl.grant_select));
        out.push(u8::from(acl.grant_update));
    }
    out
}

fn deserialize_metadata(oid: u32, bytes: &[u8]) -> Result<Metadata, KvError> {
    let mut cur = bytes;
    if take_u8(&mut cur)? != METADATA_VERSION {
        return Err(KvError::CorruptRow(
            "unknown large object metadata version".into(),
        ));
    }
    let owner = take_string(&mut cur)?;
    let count = u32::from_be_bytes(take_array(&mut cur)?);
    let mut acl = Vec::with_capacity(usize::try_from(count).expect("u32 fits in usize"));
    for _ in 0..count {
        let grantee = take_string(&mut cur)?;
        let grantor = take_string(&mut cur)?;
        let flags = [
            take_bool(&mut cur)?,
            take_bool(&mut cur)?,
            take_bool(&mut cur)?,
            take_bool(&mut cur)?,
        ];
        acl.push(AclEntry {
            grantee,
            grantor,
            select: flags[0],
            update: flags[1],
            grant_select: flags[2],
            grant_update: flags[3],
        });
    }
    if !cur.is_empty() {
        return Err(KvError::CorruptRow(
            "large object metadata has trailing bytes".into(),
        ));
    }
    Ok(Metadata { oid, owner, acl })
}

fn put_string(out: &mut Vec<u8>, value: &str) {
    let len = u32::try_from(value.len()).expect("catalog string length fits u32");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value.as_bytes());
}

fn take_u8(cur: &mut &[u8]) -> Result<u8, KvError> {
    let Some((&value, rest)) = cur.split_first() else {
        return Err(KvError::CorruptRow(
            "truncated large object metadata".into(),
        ));
    };
    *cur = rest;
    Ok(value)
}

fn take_array<const N: usize>(cur: &mut &[u8]) -> Result<[u8; N], KvError> {
    if cur.len() < N {
        return Err(KvError::CorruptRow(
            "truncated large object metadata".into(),
        ));
    }
    let (head, tail) = cur.split_at(N);
    *cur = tail;
    head.try_into()
        .map_err(|_| KvError::CorruptRow("invalid large object metadata".into()))
}

fn take_string(cur: &mut &[u8]) -> Result<String, KvError> {
    let len = usize::try_from(u32::from_be_bytes(take_array(cur)?)).expect("u32 fits in usize");
    if cur.len() < len {
        return Err(KvError::CorruptRow(
            "truncated large object metadata".into(),
        ));
    }
    let (bytes, tail) = cur.split_at(len);
    *cur = tail;
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| KvError::CorruptRow("large object metadata is not UTF-8".into()))
}

fn take_bool(cur: &mut &[u8]) -> Result<bool, KvError> {
    match take_u8(cur)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(KvError::CorruptRow("invalid large object ACL flag".into())),
    }
}

fn too_large() -> CatalogError {
    CatalogError::Storage(KvError::CorruptRow(
        "large object exceeds address space".into(),
    ))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgkv::{Kv as _, MemKv};

    use super::*;

    #[test]
    fn stores_pages_and_sparse_bytes() {
        let kv = MemKv::default();
        let (oid, create) = create_ops(&kv, 0, "public").expect("create ops");
        assert!(oid >= FIRST_OID);
        kv.write_batch(&create).expect("create");
        let mut bytes = vec![0; PAGE_SIZE + 3];
        bytes[0] = 1;
        bytes[PAGE_SIZE] = 2;
        kv.write_batch(&replace_ops(&kv, oid, &bytes).expect("replace"))
            .expect("write pages");

        assert!(read(&kv, oid).expect("read") == bytes);
        assert!(list_pages(&kv, oid).expect("pages").len() == 2);
    }

    #[test]
    fn automatic_oids_do_not_collide_before_metadata_commits() {
        let kv = MemKv::default();
        let (first, _) = create_ops(&kv, 0, "owner").expect("first OID");
        let (second, _) = create_ops(&kv, 0, "owner").expect("second OID");
        assert!(first != second);
    }

    #[test]
    fn keeps_acl_and_reclaims_all_pages_on_unlink() {
        let kv = MemKv::default();
        let (oid, create) = create_ops(&kv, 42, "owner").expect("create ops");
        kv.write_batch(&create).expect("create");
        let mut metadata = get_metadata(&kv, oid).expect("metadata");
        metadata.acl.push(AclEntry {
            grantee: "reader".into(),
            grantor: "owner".into(),
            select: true,
            update: false,
            grant_select: false,
            grant_update: false,
        });
        let mut ops = replace_ops(&kv, oid, &[7; PAGE_SIZE + 1]).expect("replace");
        ops.push(put_metadata_op(&metadata));
        kv.write_batch(&ops).expect("write");

        assert!(get_metadata(&kv, oid).expect("metadata") == metadata);
        kv.write_batch(&unlink_ops(&kv, oid).expect("unlink ops"))
            .expect("unlink");
        assert!(matches!(
            get_metadata(&kv, oid),
            Err(CatalogError::UndefinedLargeObject(42))
        ));
        assert!(list_pages(&kv, oid).expect("pages").is_empty());
    }
}
