//! A descriptor for a contiguous run of records bytes that live in a segment
//! `.log` file, used by the zero-copy fetch path (Increment D).
//!
//! Instead of `pread`ing the records into an owned `Bytes` (the userspace copy
//! out of the page cache), the fetch read returns the `(file, offset, len)`
//! triple. The broker then `sendfile(2)`s that range straight from the page
//! cache to a plaintext socket — never bringing the bytes into userspace. The
//! `Arc<File>` pins the inode through the async send even if compaction or
//! truncation removes the segment from the log in the meantime (the open fd
//! keeps the inode alive on Unix).
//!
//! `FileRegion` is intentionally pure-`std` (`Arc<std::fs::File>` + integers) so
//! the protocol crate needs no new dependency and the same type can flow from
//! the log crate's `read_raw_desc` through `RecordsPayload::FileRegions` to the
//! broker's sendfile drainer.

use std::{fs::File, sync::Arc};

/// A `[offset, offset+len)` byte range within a segment `.log` file that holds
/// one or more complete (or a clipped trailing) v2 record batches — byte-for-
/// byte the records-field bytes a fetch response would otherwise materialize.
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
    /// same underlying file. `Arc<File>` has no `PartialEq`; comparing the
    /// `Arc` pointer identity is the meaningful notion here (same open handle).
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.file, &other.file) && self.offset == other.offset && self.len == other.len
    }
}

impl Eq for FileRegion {}
