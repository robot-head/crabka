//! Page image primitives for the redo seam.

use bytes::{Bytes, BytesMut};
use crabka_page_store::{PAGE_SIZE, PageKey};
use crabka_postgres_wal::Lsn;
use thiserror::Error;

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// A full `PostgreSQL` page plus the relation key and LSN known to the redo seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageImage {
    key: PageKey,
    lsn: Lsn,
    bytes: Bytes,
}

impl PageImage {
    /// Builds a page image after checking the bytes are exactly one page.
    pub fn new(key: PageKey, lsn: Lsn, bytes: Bytes) -> Result<Self, PageImageError> {
        if bytes.len() != PAGE_SIZE {
            return Err(PageImageError::WrongSize {
                expected: PAGE_SIZE,
                actual: bytes.len(),
            });
        }

        Ok(Self { key, lsn, bytes })
    }

    /// Builds a zeroed page image.
    #[must_use]
    pub fn zeroed(key: PageKey, lsn: Lsn) -> Self {
        let bytes = Bytes::from(vec![0_u8; PAGE_SIZE]);
        Self { key, lsn, bytes }
    }

    /// Returns the key for the page image.
    #[must_use]
    pub const fn key(&self) -> PageKey {
        self.key
    }

    /// Returns the page image LSN.
    #[must_use]
    pub const fn lsn(&self) -> Lsn {
        self.lsn
    }

    /// Returns the raw page bytes.
    #[must_use]
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Replaces the bytes and LSN, preserving the page key.
    pub(crate) fn replace(self, lsn: Lsn, bytes: Bytes) -> Result<Self, PageImageError> {
        Self::new(self.key, lsn, bytes)
    }

    /// Applies a byte-range patch after bounds have been checked by the caller.
    pub(crate) fn patch_unchecked(&self, lsn: Lsn, offset: usize, patch: &[u8]) -> Self {
        let mut bytes = BytesMut::from(self.bytes.as_ref());
        bytes[offset..offset + patch.len()].copy_from_slice(patch);

        Self {
            key: self.key,
            lsn,
            bytes: bytes.freeze(),
        }
    }

    /// Consumes the page image and returns its raw bytes.
    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }
}

pub(crate) fn bytes_with_page_lsn(mut bytes: Vec<u8>, lsn: Lsn) -> Result<Bytes, PageImageError> {
    if bytes.len() != PAGE_SIZE {
        return Err(PageImageError::WrongSize {
            expected: PAGE_SIZE,
            actual: bytes.len(),
        });
    }

    bytes[0..8].copy_from_slice(&lsn.value().to_le_bytes());
    Ok(Bytes::from(bytes))
}

/// Errors returned while building page images.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum PageImageError {
    /// Page images must be exactly one `PostgreSQL` page.
    #[error("page image must be exactly {expected} bytes, got {actual}")]
    WrongSize {
        /// Required size.
        expected: usize,
        /// Actual size.
        actual: usize,
    },
}

/// Returns a deterministic FNV-1a hash of the page bytes.
#[must_use]
pub fn deterministic_page_hash(page: &PageImage) -> u64 {
    page.bytes().iter().fold(FNV_OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn key() -> PageKey {
        PageKey::new(1663, 5, 16_384, 0, 0)
    }

    #[test]
    fn page_image_requires_exactly_one_page() {
        let image = PageImage::new(key(), Lsn(1), Bytes::from_static(b"too-short"));

        assert!(
            image
                == Err(PageImageError::WrongSize {
                    expected: PAGE_SIZE,
                    actual: 9
                })
        );
    }

    #[test]
    fn deterministic_page_hash_is_stable_for_equal_bytes() {
        let first = PageImage::zeroed(key(), Lsn(1));
        let second = PageImage::zeroed(key(), Lsn(99));

        assert!(deterministic_page_hash(&first) == deterministic_page_hash(&second));
    }
}
