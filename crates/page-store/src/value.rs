//! Stored page-layer values.

use bytes::Bytes;
use thiserror::Error;

/// `PostgreSQL` page size in bytes.
pub const PAGE_SIZE: usize = 8 * 1024;

/// A value stored for a page key at an LSN.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// A full `PostgreSQL` page image.
    Image(Bytes),
    /// WAL bytes that redo can apply later.
    Wal {
        /// Whether this WAL record initializes the page from zeroes.
        will_init: bool,
        /// Raw WAL record bytes retained for future redo.
        rec: Bytes,
    },
}

impl Value {
    /// Builds a full-page image value.
    pub fn image(page: Bytes) -> Result<Self, ValueError> {
        if page.len() != PAGE_SIZE {
            return Err(ValueError::WrongImageSize {
                expected: PAGE_SIZE,
                actual: page.len(),
            });
        }

        Ok(Self::Image(page))
    }
}

/// Errors returned while building page values.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ValueError {
    /// Full-page images must be exactly one `PostgreSQL` page.
    #[error("image value must be exactly {expected} bytes, got {actual}")]
    WrongImageSize {
        /// Required image size.
        expected: usize,
        /// Observed image size.
        actual: usize,
    },
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn image_values_are_exactly_one_page() {
        assert!(Value::image(Bytes::from(vec![0_u8; 100])).is_err());
        assert!(Value::image(Bytes::from(vec![0_u8; PAGE_SIZE])).is_ok());
    }
}
