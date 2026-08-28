//! Transaction-local large-object changes.
//!
//! `pgcatalog::largeobject` owns durable rows; this layer provides read-your-
//! writes and turns a session's edits into one commit batch.

use std::collections::BTreeMap;

use crabka_pgcatalog::largeobject::{self, AclEntry, Metadata};
use crabka_pgkv::{Kv, WriteOp};

use crate::error::ExecError;

const INV_WRITE: i32 = 0x20_000;
const INV_READ: i32 = 0x40_000;
const MAX_LO_READ_BYTES: usize = 0x3fff_fffb;

#[derive(Debug, Clone, Copy)]
pub(crate) enum LoPrivilege {
    Select,
    Update,
}

#[derive(Debug)]
struct Descriptor {
    oid: u32,
    position: usize,
    readable: bool,
    writable: bool,
}

/// Session-local descriptor table for `lo_open`/`lo_close` and the `lo*`
/// streaming functions.
#[derive(Debug, Default)]
pub(crate) struct Descriptors {
    open: BTreeMap<i32, Descriptor>,
}

impl Descriptors {
    pub(crate) fn open(&mut self, oid: u32, mode: i32) -> Result<i32, ExecError> {
        let readable = mode & INV_READ != 0;
        let writable = mode & INV_WRITE != 0;
        if !readable && !writable {
            return Err(ExecError::FunctionError {
                sqlstate: "22023",
                message: "invalid flags for large object open".into(),
            });
        }
        let mut descriptor = 0;
        while self.open.contains_key(&descriptor) {
            descriptor = descriptor.checked_add(1).ok_or_else(descriptor_exhausted)?;
        }
        self.open.insert(
            descriptor,
            Descriptor {
                oid,
                position: 0,
                readable,
                writable,
            },
        );
        Ok(descriptor)
    }

    pub(crate) fn close(&mut self, descriptor: i32) -> Result<(), ExecError> {
        self.open
            .remove(&descriptor)
            .map(|_| ())
            .ok_or_else(|| invalid_descriptor(descriptor))
    }

    pub(crate) fn clear(&mut self) {
        self.open.clear();
    }

    fn get(&self, descriptor: i32) -> Result<&Descriptor, ExecError> {
        self.open
            .get(&descriptor)
            .ok_or_else(|| invalid_descriptor(descriptor))
    }

    fn get_mut(&mut self, descriptor: i32) -> Result<&mut Descriptor, ExecError> {
        self.open
            .get_mut(&descriptor)
            .ok_or_else(|| invalid_descriptor(descriptor))
    }
}

#[derive(Debug, Clone)]
enum Change {
    Present {
        metadata: Metadata,
        bytes: SparseBytes,
        created: bool,
        comment: Option<Option<String>>,
    },
    Deleted,
}

/// Sparse transaction-local contents. PostgreSQL records large objects in
/// pages, so a seek to 5GiB must not allocate 5GiB before a single byte is
/// written.
#[derive(Debug, Clone, Default)]
struct SparseBytes {
    len: usize,
    pages: BTreeMap<u32, Vec<u8>>,
}

impl SparseBytes {
    fn from_dense(bytes: Vec<u8>) -> Self {
        let len = bytes.len();
        let pages = bytes
            .chunks(largeobject::PAGE_SIZE)
            .enumerate()
            .map(|(page, data)| {
                (
                    u32::try_from(page).expect("large object page fits u32"),
                    data.to_vec(),
                )
            })
            .collect();
        Self { len, pages }
    }

    fn from_pages(pages: Vec<largeobject::Page>) -> Result<Self, ExecError> {
        let mut bytes = Self::default();
        for page in pages {
            let start = usize::try_from(page.page_no)
                .ok()
                .and_then(|page| page.checked_mul(largeobject::PAGE_SIZE))
                .ok_or_else(oid_exhausted)?;
            bytes.len = bytes.len.max(
                start
                    .checked_add(page.data.len())
                    .ok_or_else(oid_exhausted)?,
            );
            bytes.pages.insert(page.page_no, page.data);
        }
        Ok(bytes)
    }

    fn read_all(&self) -> Result<Vec<u8>, ExecError> {
        if self.len > MAX_LO_READ_BYTES {
            return Err(large_object_read_too_large());
        }
        self.read_range(0, self.len)
    }

    fn read_range(&self, offset: usize, len: usize) -> Result<Vec<u8>, ExecError> {
        let end = offset
            .checked_add(len)
            .ok_or_else(oid_exhausted)?
            .min(self.len);
        if offset >= end {
            return Ok(Vec::new());
        }
        let mut out = vec![0; end - offset];
        let first = u32::try_from(offset / largeobject::PAGE_SIZE).map_err(|_| oid_exhausted())?;
        let last =
            u32::try_from((end - 1) / largeobject::PAGE_SIZE).map_err(|_| oid_exhausted())?;
        for (page_no, page) in self.pages.range(first..=last) {
            let page_start = usize::try_from(*page_no)
                .ok()
                .and_then(|page| page.checked_mul(largeobject::PAGE_SIZE))
                .ok_or_else(oid_exhausted)?;
            let copy_start = offset.max(page_start);
            let copy_end = end.min(page_start.saturating_add(page.len()));
            if copy_start < copy_end {
                out[copy_start - offset..copy_end - offset]
                    .copy_from_slice(&page[copy_start - page_start..copy_end - page_start]);
            }
        }
        Ok(out)
    }

    fn write_at(&mut self, offset: usize, mut source: &[u8]) -> Result<(), ExecError> {
        let end = offset.checked_add(source.len()).ok_or_else(oid_exhausted)?;
        let mut cursor = offset;
        while !source.is_empty() {
            let page_no =
                u32::try_from(cursor / largeobject::PAGE_SIZE).map_err(|_| oid_exhausted())?;
            let page_offset = cursor % largeobject::PAGE_SIZE;
            let count = source.len().min(largeobject::PAGE_SIZE - page_offset);
            let page = self
                .pages
                .entry(page_no)
                .or_insert_with(|| vec![0; largeobject::PAGE_SIZE]);
            page.resize(largeobject::PAGE_SIZE, 0);
            page[page_offset..page_offset + count].copy_from_slice(&source[..count]);
            source = &source[count..];
            cursor += count;
        }
        self.len = self.len.max(end);
        self.normalize_tail()?;
        Ok(())
    }

    fn truncate(&mut self, len: usize) -> Result<(), ExecError> {
        self.len = len;
        self.normalize_tail()
    }

    fn normalize_tail(&mut self) -> Result<(), ExecError> {
        if self.len == 0 {
            self.pages.clear();
            return Ok(());
        }
        let last =
            u32::try_from((self.len - 1) / largeobject::PAGE_SIZE).map_err(|_| oid_exhausted())?;
        let last_len = (self.len - 1) % largeobject::PAGE_SIZE + 1;
        self.pages.retain(|page, _| *page <= last);
        self.pages
            .entry(last)
            .or_insert_with(|| vec![0; last_len])
            .resize(last_len, 0);
        Ok(())
    }
}

/// Every large-object mutation the current SQL transaction has staged.
#[derive(Debug, Clone, Default)]
pub(crate) struct PendingLargeObjects {
    changes: BTreeMap<u32, Change>,
}

impl PendingLargeObjects {
    /// Create a new object and return its OID without making it externally
    /// visible before the surrounding transaction commits.
    pub(crate) fn create(
        &mut self,
        kv: &dyn Kv,
        requested_oid: u32,
        owner: &str,
        compat_privileges: bool,
    ) -> Result<u32, ExecError> {
        let (mut oid, _) = largeobject::create_ops(kv, requested_oid, owner)?;
        while self.changes.contains_key(&oid) {
            oid = oid.checked_add(1).ok_or_else(oid_exhausted)?;
        }
        if requested_oid != 0 && !matches!(self.changes.get(&oid), Some(Change::Deleted) | None) {
            return Err(duplicate_oid(oid));
        }
        self.changes.insert(
            oid,
            Change::Present {
                metadata: Metadata {
                    oid,
                    owner: owner.to_string(),
                    acl: compat_privileges
                        .then(|| AclEntry {
                            grantee: crabka_pgcatalog::PUBLIC_ROLE.into(),
                            grantor: owner.to_string(),
                            select: true,
                            update: true,
                            grant_select: false,
                            grant_update: false,
                        })
                        .into_iter()
                        .collect(),
                },
                bytes: SparseBytes::default(),
                created: true,
                comment: None,
            },
        );
        Ok(oid)
    }

    /// Return the current transaction's contents, including uncommitted edits.
    pub(crate) fn read(&mut self, kv: &dyn Kv, oid: u32) -> Result<Vec<u8>, ExecError> {
        self.materialize(kv, oid)?.read_all()
    }

    /// Read a bounded range without materialising sparse holes outside it.
    pub(crate) fn read_range(
        &mut self,
        kv: &dyn Kv,
        oid: u32,
        offset: usize,
        len: usize,
    ) -> Result<Vec<u8>, ExecError> {
        self.materialize(kv, oid)?.read_range(offset, len)
    }

    fn len(&mut self, kv: &dyn Kv, oid: u32) -> Result<usize, ExecError> {
        Ok(self.materialize(kv, oid)?.len)
    }

    /// Overwrite all bytes in the current transaction's visible version.
    pub(crate) fn replace(
        &mut self,
        kv: &dyn Kv,
        oid: u32,
        bytes: Vec<u8>,
    ) -> Result<(), ExecError> {
        *self.materialize(kv, oid)? = SparseBytes::from_dense(bytes);
        Ok(())
    }

    /// Write bytes at `offset`, extending sparse space with zeroes.
    pub(crate) fn write_at(
        &mut self,
        kv: &dyn Kv,
        oid: u32,
        offset: usize,
        bytes: &[u8],
    ) -> Result<(), ExecError> {
        self.materialize(kv, oid)?.write_at(offset, bytes)
    }

    /// Change the visible length, zero-filling an extension.
    pub(crate) fn truncate(&mut self, kv: &dyn Kv, oid: u32, len: usize) -> Result<(), ExecError> {
        self.materialize(kv, oid)?.truncate(len)
    }

    /// Change an object's owner in the transaction-local version.
    pub(crate) fn set_owner(
        &mut self,
        kv: &dyn Kv,
        oid: u32,
        actor: &str,
        owner: String,
    ) -> Result<(), ExecError> {
        let metadata = self.metadata(kv, oid)?;
        if !crabka_pgcatalog::role_has_privs_of(kv, actor, &metadata.owner)?
            && !crate::rls::role_is_superuser(kv, actor)?
        {
            return Err(ExecError::FunctionError {
                sqlstate: "42501",
                message: format!("must be owner of large object {oid}"),
            });
        }
        self.materialize(kv, oid)?;
        let Some(Change::Present { metadata, .. }) = self.changes.get_mut(&oid) else {
            return Err(undefined_oid(oid));
        };
        metadata.owner = owner.clone();
        if !metadata
            .acl
            .iter()
            .any(|entry| entry.grantee == owner && entry.grantor == owner)
        {
            metadata.acl.push(AclEntry {
                grantee: owner.clone(),
                grantor: owner,
                select: true,
                update: true,
                grant_select: false,
                grant_update: false,
            });
        }
        Ok(())
    }

    /// Stage a comment owned by the large object's owner or a superuser.
    pub(crate) fn set_comment(
        &mut self,
        kv: &dyn Kv,
        oid: u32,
        actor: &str,
        comment: Option<String>,
    ) -> Result<(), ExecError> {
        let metadata = self.metadata(kv, oid)?;
        if !crabka_pgcatalog::role_has_privs_of(kv, actor, &metadata.owner)?
            && !crate::rls::role_is_superuser(kv, actor)?
        {
            return Err(ExecError::FunctionError {
                sqlstate: "42501",
                message: format!("must be owner of large object {oid}"),
            });
        }
        self.materialize(kv, oid)?;
        let Some(Change::Present {
            comment: staged, ..
        }) = self.changes.get_mut(&oid)
        else {
            return Err(undefined_oid(oid));
        };
        *staged = Some(comment);
        Ok(())
    }

    /// Grant `SELECT` and/or `UPDATE` on one object in its local version.
    pub(crate) fn grant(
        &mut self,
        kv: &dyn Kv,
        oid: u32,
        actor: &str,
        grantees: &[String],
        privileges: &[crabka_pgparser::ast::PrivilegeSpec],
        grant_option: bool,
    ) -> Result<(), ExecError> {
        let requested = acl_privileges(privileges)?;
        let metadata = self.metadata(kv, oid)?;
        let rights = acl_rights(kv, &metadata, actor)?;
        if (requested[0] && !rights[2]) || (requested[1] && !rights[3]) {
            return Err(acl_denied(oid));
        }
        self.materialize(kv, oid)?;
        let Some(Change::Present { metadata, .. }) = self.changes.get_mut(&oid) else {
            return Err(undefined_oid(oid));
        };
        for grantee in grantees {
            let entry = metadata
                .acl
                .iter()
                .position(|entry| entry.grantee == *grantee && entry.grantor == actor);
            let entry = if let Some(entry) = entry {
                &mut metadata.acl[entry]
            } else {
                metadata.acl.push(AclEntry {
                    grantee: grantee.clone(),
                    grantor: actor.to_string(),
                    select: false,
                    update: false,
                    grant_select: false,
                    grant_update: false,
                });
                metadata.acl.last_mut().expect("new large-object ACL entry")
            };
            entry.select |= requested[0];
            entry.update |= requested[1];
            if grant_option {
                entry.grant_select |= requested[0];
                entry.grant_update |= requested[1];
            }
        }
        Ok(())
    }

    /// Revoke privileges (or only their grant option) from one object.
    pub(crate) fn revoke(
        &mut self,
        kv: &dyn Kv,
        oid: u32,
        actor: &str,
        grantees: &[String],
        privileges: &[crabka_pgparser::ast::PrivilegeSpec],
        grant_option_only: bool,
    ) -> Result<(), ExecError> {
        let requested = acl_privileges(privileges)?;
        let metadata = self.metadata(kv, oid)?;
        let owner_or_superuser = crabka_pgcatalog::role_has_privs_of(kv, actor, &metadata.owner)?
            || crate::rls::role_is_superuser(kv, actor)?;
        if !owner_or_superuser
            && !metadata
                .acl
                .iter()
                .any(|entry| entry.grantor == actor && grantees.contains(&entry.grantee))
        {
            return Err(acl_denied(oid));
        }
        self.materialize(kv, oid)?;
        let Some(Change::Present { metadata, .. }) = self.changes.get_mut(&oid) else {
            return Err(undefined_oid(oid));
        };
        for entry in &mut metadata.acl {
            if !grantees.contains(&entry.grantee) || (!owner_or_superuser && entry.grantor != actor)
            {
                continue;
            }
            if requested[0] {
                entry.grant_select = false;
                if !grant_option_only {
                    entry.select = false;
                }
            }
            if requested[1] {
                entry.grant_update = false;
                if !grant_option_only {
                    entry.update = false;
                }
            }
        }
        // ponytail: this does not cascade grants made through a revoked option;
        // add the PostgreSQL grant-dependency walk with RESTRICT/CASCADE syntax together.
        metadata.acl.retain(|entry| entry.select || entry.update);
        Ok(())
    }

    /// Remove an object, unless this transaction created it (when there is no
    /// durable work to emit at all).
    pub(crate) fn unlink(&mut self, kv: &dyn Kv, oid: u32) -> Result<(), ExecError> {
        match self.changes.get(&oid) {
            Some(Change::Present { created: true, .. }) => {
                self.changes.remove(&oid);
                Ok(())
            }
            Some(Change::Present { .. }) => {
                self.changes.insert(oid, Change::Deleted);
                Ok(())
            }
            Some(Change::Deleted) => Err(undefined_oid(oid)),
            None => {
                let _ = largeobject::get_metadata(kv, oid)?;
                self.changes.insert(oid, Change::Deleted);
                Ok(())
            }
        }
    }

    /// Remove every staged change, for rollback and failed autocommit.
    pub(crate) fn clear(&mut self) {
        self.changes.clear();
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    fn metadata(&self, kv: &dyn Kv, oid: u32) -> Result<Metadata, crabka_pgcatalog::CatalogError> {
        match self.changes.get(&oid) {
            Some(Change::Present { metadata, .. }) => Ok(metadata.clone()),
            Some(Change::Deleted) => Err(crabka_pgcatalog::CatalogError::UndefinedLargeObject(oid)),
            None => largeobject::get_metadata(kv, oid),
        }
    }

    /// Turn every current change into one deterministic durable batch.
    pub(crate) fn take_ops(&mut self, kv: &dyn Kv) -> Result<Vec<WriteOp>, ExecError> {
        let changes = std::mem::take(&mut self.changes);
        let mut ops = Vec::new();
        for (oid, change) in changes {
            match change {
                Change::Present {
                    metadata,
                    bytes,
                    created,
                    comment,
                } => {
                    ops.push(largeobject::put_metadata_op(&metadata));
                    let _ = created;
                    ops.extend(largeobject::replace_sparse_page_ops(kv, oid, &bytes.pages)?);
                    if let Some(comment) = comment {
                        let oid = oid.to_string();
                        ops.push(crabka_pgcatalog::set_comment_op(
                            "large object",
                            crabka_pgcatalog::CommentObject::Named(&oid),
                            comment.as_deref(),
                        ));
                    }
                }
                Change::Deleted => {
                    ops.extend(largeobject::unlink_ops(kv, oid)?);
                    let oid = oid.to_string();
                    ops.push(crabka_pgcatalog::set_comment_op(
                        "large object",
                        crabka_pgcatalog::CommentObject::Named(&oid),
                        None,
                    ));
                }
            }
        }
        Ok(ops)
    }

    fn materialize(&mut self, kv: &dyn Kv, oid: u32) -> Result<&mut SparseBytes, ExecError> {
        if !self.changes.contains_key(&oid) {
            let metadata = largeobject::get_metadata(kv, oid)?;
            let bytes = SparseBytes::from_pages(largeobject::list_pages(kv, oid)?)?;
            self.changes.insert(
                oid,
                Change::Present {
                    metadata,
                    bytes,
                    created: false,
                    comment: None,
                },
            );
        }
        match self.changes.get_mut(&oid) {
            Some(Change::Present { bytes, .. }) => Ok(bytes),
            Some(Change::Deleted) | None => Err(undefined_oid(oid)),
        }
    }
}

fn acl_privileges(
    privileges: &[crabka_pgparser::ast::PrivilegeSpec],
) -> Result<[bool; 2], ExecError> {
    let mut requested = [false; 2];
    for privilege in privileges {
        if !privilege.columns.is_empty() {
            return Err(ExecError::FunctionError {
                sqlstate: "0LP01",
                message: "column privileges are only valid for relations".into(),
            });
        }
        match privilege.name.as_str() {
            "SELECT" => requested[0] = true,
            "UPDATE" => requested[1] = true,
            "ALL" | "ALL PRIVILEGES" => requested = [true; 2],
            name => {
                return Err(ExecError::FunctionError {
                    sqlstate: "22023",
                    message: format!("invalid privilege type {name} for large object"),
                });
            }
        }
    }
    Ok(requested)
}

fn acl_rights(kv: &dyn Kv, metadata: &Metadata, role: &str) -> Result<[bool; 4], ExecError> {
    if crabka_pgcatalog::role_has_privs_of(kv, role, &metadata.owner)?
        || crate::rls::role_is_superuser(kv, role)?
    {
        return Ok([true; 4]);
    }
    let mut rights = [false; 4];
    for entry in &metadata.acl {
        if entry.grantee == crabka_pgcatalog::PUBLIC_ROLE
            || crabka_pgcatalog::role_has_privs_of(kv, role, &entry.grantee)?
        {
            rights[0] |= entry.select;
            rights[1] |= entry.update;
            rights[2] |= entry.grant_select;
            rights[3] |= entry.grant_update;
        }
    }
    Ok(rights)
}

fn acl_denied(oid: u32) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42501",
        message: format!("permission denied for large object {oid}"),
    }
}

pub(crate) fn open(
    runtime: &crate::clock::LargeObjectRuntime,
    role: &str,
    oid: u32,
    mode: i32,
) -> Result<i32, ExecError> {
    if mode & INV_WRITE != 0 {
        require_writable(runtime, "lo_open(INV_WRITE)")?;
    }
    if mode & INV_READ != 0 {
        require_privilege(runtime, role, oid, LoPrivilege::Select)?;
    }
    if mode & INV_WRITE != 0 {
        require_privilege(runtime, role, oid, LoPrivilege::Update)?;
    }
    runtime
        .pending
        .lock()
        .expect("pending large objects")
        .materialize(runtime.kv.as_ref(), oid)?;
    runtime
        .descriptors
        .lock()
        .expect("large object descriptors")
        .open(oid, mode)
}

pub(crate) fn require_writable(
    runtime: &crate::clock::LargeObjectRuntime,
    function: &str,
) -> Result<(), ExecError> {
    if runtime.read_only {
        return Err(ExecError::FunctionError {
            sqlstate: "25006",
            message: format!("cannot execute {function} in a read-only transaction"),
        });
    }
    Ok(())
}

pub(crate) fn require_privilege(
    runtime: &crate::clock::LargeObjectRuntime,
    role: &str,
    oid: u32,
    wanted: LoPrivilege,
) -> Result<(), ExecError> {
    match has_privilege(runtime, role, oid, wanted)? {
        Some(true) => Ok(()),
        Some(false) => Err(ExecError::FunctionError {
            sqlstate: "42501",
            message: format!("permission denied for large object {oid}"),
        }),
        None => Err(undefined_oid(oid)),
    }
}

/// Whether `role` currently has `wanted` on `oid`; `None` is an absent object.
pub(crate) fn has_privilege(
    runtime: &crate::clock::LargeObjectRuntime,
    role: &str,
    oid: u32,
    wanted: LoPrivilege,
) -> Result<Option<bool>, ExecError> {
    has_privilege_inner(runtime, role, oid, wanted, false)
}

/// Whether `role` may pass `wanted` on to another role; `None` is absent.
pub(crate) fn has_grant_option(
    runtime: &crate::clock::LargeObjectRuntime,
    role: &str,
    oid: u32,
    wanted: LoPrivilege,
) -> Result<Option<bool>, ExecError> {
    has_privilege_inner(runtime, role, oid, wanted, true)
}

fn has_privilege_inner(
    runtime: &crate::clock::LargeObjectRuntime,
    role: &str,
    oid: u32,
    wanted: LoPrivilege,
    grant_option: bool,
) -> Result<Option<bool>, ExecError> {
    let role = if role == crabka_pgcatalog::PUBLIC_ROLE {
        crabka_pgcatalog::BOOTSTRAP_ROLE
    } else {
        role
    };
    let metadata = match runtime
        .pending
        .lock()
        .expect("pending large objects")
        .metadata(runtime.kv.as_ref(), oid)
    {
        Ok(metadata) => metadata,
        Err(crabka_pgcatalog::CatalogError::UndefinedLargeObject(_)) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if crate::rls::role_is_superuser(runtime.kv.as_ref(), role)?
        || crabka_pgcatalog::role_has_privs_of(runtime.kv.as_ref(), role, &metadata.owner)?
    {
        return Ok(Some(true));
    }
    for entry in &metadata.acl {
        let grantee_matches = entry.grantee == crabka_pgcatalog::PUBLIC_ROLE
            || crabka_pgcatalog::role_has_privs_of(runtime.kv.as_ref(), role, &entry.grantee)?;
        let granted = match (wanted, grant_option) {
            (LoPrivilege::Select, false) => entry.select,
            (LoPrivilege::Update, false) => entry.update,
            (LoPrivilege::Select, true) => entry.grant_select,
            (LoPrivilege::Update, true) => entry.grant_update,
        };
        if grantee_matches && granted {
            return Ok(Some(true));
        }
    }
    Ok(Some(false))
}

pub(crate) fn close(
    runtime: &crate::clock::LargeObjectRuntime,
    descriptor: i32,
) -> Result<(), ExecError> {
    runtime
        .descriptors
        .lock()
        .expect("large object descriptors")
        .close(descriptor)
}

pub(crate) fn read_descriptor(
    runtime: &crate::clock::LargeObjectRuntime,
    descriptor: i32,
    len: usize,
) -> Result<Vec<u8>, ExecError> {
    let (oid, position) = {
        let descriptors = runtime
            .descriptors
            .lock()
            .expect("large object descriptors");
        let descriptor = descriptors.get(descriptor)?;
        if !descriptor.readable {
            return Err(not_open_for_reading(descriptor.oid));
        }
        (descriptor.oid, descriptor.position)
    };
    let result = runtime
        .pending
        .lock()
        .expect("pending large objects")
        .read_range(runtime.kv.as_ref(), oid, position, len)?;
    runtime
        .descriptors
        .lock()
        .expect("large object descriptors")
        .get_mut(descriptor)?
        .position = position + result.len();
    Ok(result)
}

pub(crate) fn write_descriptor(
    runtime: &crate::clock::LargeObjectRuntime,
    descriptor: i32,
    bytes: &[u8],
) -> Result<(), ExecError> {
    require_writable(runtime, "lowrite()")?;
    let (oid, position) = {
        let descriptors = runtime
            .descriptors
            .lock()
            .expect("large object descriptors");
        let descriptor = descriptors.get(descriptor)?;
        if !descriptor.writable {
            return Err(not_open_for_writing(descriptor.oid));
        }
        (descriptor.oid, descriptor.position)
    };
    runtime
        .pending
        .lock()
        .expect("pending large objects")
        .write_at(runtime.kv.as_ref(), oid, position, bytes)?;
    runtime
        .descriptors
        .lock()
        .expect("large object descriptors")
        .get_mut(descriptor)?
        .position = position
        .checked_add(bytes.len())
        .ok_or_else(lo_offset_error)?;
    Ok(())
}

/// Resize the object a writable descriptor references.
pub(crate) fn truncate_descriptor(
    runtime: &crate::clock::LargeObjectRuntime,
    descriptor: i32,
    len: usize,
    function: &str,
) -> Result<(), ExecError> {
    require_writable(runtime, function)?;
    let oid = {
        let descriptors = runtime
            .descriptors
            .lock()
            .expect("large object descriptors");
        let descriptor = descriptors.get(descriptor)?;
        if !descriptor.writable {
            return Err(not_open_for_writing(descriptor.oid));
        }
        descriptor.oid
    };
    runtime
        .pending
        .lock()
        .expect("pending large objects")
        .truncate(runtime.kv.as_ref(), oid, len)
}

pub(crate) fn seek_descriptor(
    runtime: &crate::clock::LargeObjectRuntime,
    descriptor: i32,
    offset: i64,
    whence: i32,
) -> Result<usize, ExecError> {
    let (oid, position) = {
        let descriptors = runtime
            .descriptors
            .lock()
            .expect("large object descriptors");
        let descriptor = descriptors.get(descriptor)?;
        (descriptor.oid, descriptor.position)
    };
    let base = match whence {
        0 => 0_i64,
        1 => i64::try_from(position).map_err(|_| lo_offset_error())?,
        2 => i64::try_from(
            runtime
                .pending
                .lock()
                .expect("pending large objects")
                .len(runtime.kv.as_ref(), oid)?,
        )
        .map_err(|_| lo_offset_error())?,
        _ => {
            return Err(ExecError::FunctionError {
                sqlstate: "22023",
                message: "invalid whence for large object seek".into(),
            });
        }
    };
    let target = usize::try_from(base.checked_add(offset).ok_or_else(lo_offset_error)?)
        .map_err(|_| lo_offset_error())?;
    runtime
        .descriptors
        .lock()
        .expect("large object descriptors")
        .get_mut(descriptor)?
        .position = target;
    Ok(target)
}

pub(crate) fn tell_descriptor(
    runtime: &crate::clock::LargeObjectRuntime,
    descriptor: i32,
) -> Result<usize, ExecError> {
    Ok(runtime
        .descriptors
        .lock()
        .expect("large object descriptors")
        .get(descriptor)?
        .position)
}

fn undefined_oid(oid: u32) -> ExecError {
    crabka_pgcatalog::CatalogError::UndefinedLargeObject(oid).into()
}

fn duplicate_oid(oid: u32) -> ExecError {
    crabka_pgcatalog::CatalogError::DuplicateLargeObject(oid).into()
}

fn oid_exhausted() -> ExecError {
    ExecError::FunctionError {
        sqlstate: "54000",
        message: "large object exceeds address space".into(),
    }
}

fn invalid_descriptor(descriptor: i32) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42704",
        message: format!("invalid large-object descriptor: {descriptor}"),
    }
}

fn descriptor_exhausted() -> ExecError {
    ExecError::FunctionError {
        sqlstate: "54000",
        message: "too many large-object descriptors".into(),
    }
}

fn not_open_for_reading(oid: u32) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "55000",
        message: format!("large object {oid} was not opened for reading"),
    }
}

fn not_open_for_writing(oid: u32) -> ExecError {
    ExecError::FunctionError {
        sqlstate: "55000",
        message: format!("large object {oid} was not opened for writing"),
    }
}

fn lo_offset_error() -> ExecError {
    ExecError::FunctionError {
        sqlstate: "22003",
        message: "large object seek target out of range".into(),
    }
}

fn large_object_read_too_large() -> ExecError {
    ExecError::FunctionError {
        sqlstate: "54000",
        message: "large object read request is too large".into(),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgkv::{Kv as _, MemKv};

    use super::*;

    #[test]
    fn stages_reads_writes_and_rollback_without_leaking() {
        let kv = MemKv::default();
        let mut pending = PendingLargeObjects::default();
        let oid = pending.create(&kv, 0, "public", false).expect("create");
        pending.write_at(&kv, oid, 3, b"cat").expect("write");
        assert!(pending.read(&kv, oid).expect("read") == b"\0\0\0cat");
        pending.clear();
        assert!(matches!(largeobject::get_metadata(&kv, oid), Err(_)));
    }

    #[test]
    fn commits_new_object_in_one_batch() {
        let kv = MemKv::default();
        let mut pending = PendingLargeObjects::default();
        let oid = pending.create(&kv, 0, "public", false).expect("create");
        pending
            .replace(&kv, oid, b"crab".to_vec())
            .expect("replace");
        kv.write_batch(&pending.take_ops(&kv).expect("ops"))
            .expect("commit");

        assert!(largeobject::read(&kv, oid).expect("read") == b"crab");
    }

    #[test]
    fn keeps_multi_gibibyte_holes_sparse() {
        let kv = MemKv::default();
        let mut pending = PendingLargeObjects::default();
        let oid = pending.create(&kv, 0, "public", false).expect("create");
        let offset = usize::try_from(5_000_000_000_u64).expect("64-bit address space");
        pending
            .write_at(&kv, oid, offset, b"x")
            .expect("sparse write");

        let Change::Present { bytes, .. } = pending.changes.get(&oid).expect("change") else {
            panic!("created object remains present");
        };
        assert!(bytes.pages.len() == 1);
        assert!(bytes.len == offset + 1);
        assert!(
            pending
                .read_range(&kv, oid, offset - 2, 4)
                .expect("bounded sparse read")
                == b"\0\0x"
        );
        assert!(matches!(
            pending.read(&kv, oid),
            Err(ExecError::FunctionError {
                sqlstate: "54000",
                message,
            }) if message == "large object read request is too large"
        ));
    }

    #[test]
    fn reuses_closed_descriptor_numbers() {
        let mut descriptors = Descriptors::default();
        assert!(descriptors.open(1, INV_READ).expect("open") == 0);
        descriptors.close(0).expect("close");
        assert!(descriptors.open(1, INV_READ).expect("reopen") == 0);
    }
}
