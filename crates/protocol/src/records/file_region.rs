//! A descriptor for a contiguous run of records bytes that live in a segment
//! `.log` file. The zero-copy fetch path (Increment D) uses it.
//!
//! The fetch read returns the `(file, offset, len)` triple instead of a `pread`
//! of the records into an owned `Bytes`, which would copy them out of the page
//! cache into userspace. The broker then `sendfile(2)`s that range straight
//! from the page cache to a plaintext socket and never brings the bytes into
//! userspace. The `Arc<File>` pins the inode through the async send, even if
//! compaction or truncation removes the segment from the log in the meantime.
//! On Unix the open fd keeps the inode alive.
//!
//! `FileRegion` is intentionally pure-`std`: an `Arc<std::fs::File>` and
//! integers. The protocol crate therefore needs no new dependency, and the same
//! type can flow from the log crate's `read_raw_desc` through
//! `RecordsPayload::FileRegions` to the broker's sendfile drainer.

use std::{fs::File, sync::Arc};

/// A `[offset, offset+len)` byte range within a segment `.log` file.
///
/// The range holds one or more complete v2 record batches, and it can end with
/// a clipped trailing batch. Byte for byte, these are the records-field bytes
/// that a fetch response would otherwise materialize.
#[derive(Debug, Clone)]
pub struct FileRegion {
    /// The segment `.log` file. Shared (`Arc`) so the inode stays open for the
    /// duration of the async send regardless of concurrent retention.
    pub file: Arc<File>,
    /// Start byte position of the records run within `file`.
    pub offset: u64,
    /// Number of bytes in the run.
    pub len: usize,
}

impl PartialEq for FileRegion {
    /// Two regions are equal when they describe the same byte range of the
    /// same underlying file. `Arc<File>` has no `PartialEq`, so this impl
    /// compares `Arc` pointer identity, which means the same open handle.
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.file, &other.file) && self.offset == other.offset && self.len == other.len
    }
}

impl Eq for FileRegion {}
