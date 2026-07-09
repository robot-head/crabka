//! Typed `PostgreSQL` storage keys.

use std::fmt;

use serde::{Deserialize, Serialize};

/// `PostgreSQL` relation fork number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ForkNumber(pub u8);

/// `PostgreSQL` relation block number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BlockNumber(pub u32);

/// Identifies a `PostgreSQL` relation fork.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RelTag {
    /// Tablespace OID.
    pub spc_node: u32,
    /// Database OID.
    pub db_node: u32,
    /// Relation filenode OID.
    pub rel_node: u32,
    /// Relation fork number.
    pub fork_number: ForkNumber,
}

impl RelTag {
    /// Builds a typed relation tag.
    #[must_use]
    pub const fn new(spc_node: u32, db_node: u32, rel_node: u32, fork_number: u8) -> Self {
        Self {
            spc_node,
            db_node,
            rel_node,
            fork_number: ForkNumber(fork_number),
        }
    }
}

/// Identifies a single `PostgreSQL` heap/index page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PageKey {
    /// Relation fork containing the block.
    pub rel: RelTag,
    /// Block number within the fork.
    pub block_number: BlockNumber,
}

/// Identifies a page addressed by the page-store keyspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum StorageKey {
    /// A relation heap/index/fork page stored by existing [`PageKey`] APIs.
    Relation(PageKey),
    /// An SLRU page reserved for PG-4b materialization.
    Slru(SlruPageKey),
    /// Relation metadata reserved for PG-4b materialization.
    RelMeta(RelMetaKey),
}

impl From<PageKey> for StorageKey {
    fn from(key: PageKey) -> Self {
        Self::Relation(key)
    }
}

impl StorageKey {
    /// Encodes the tagged keyspace as fixed-width lowercase hex that sorts like [`StorageKey`].
    #[must_use]
    pub fn to_fixed_hex(self) -> String {
        match self {
            Self::Relation(key) => format!("00{}", key.to_fixed_hex()),
            Self::Slru(key) => format!(
                "01{:02x}{:08x}{:08x}",
                key.kind.ordering_tag(),
                key.segment_number,
                key.block_number
            ),
            Self::RelMeta(key) => format!(
                "02{:08x}{:08x}{:08x}{:02x}{:02x}",
                key.rel.spc_node,
                key.rel.db_node,
                key.rel.rel_node,
                key.rel.fork_number.0,
                key.kind.ordering_tag()
            ),
        }
    }
}

/// Identifies one SLRU page reserved for future storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SlruPageKey {
    /// SLRU family.
    pub kind: SlruKind,
    /// Segment number.
    pub segment_number: u32,
    /// Block number within the segment.
    pub block_number: u32,
}

impl SlruPageKey {
    /// Builds a typed SLRU page key.
    #[must_use]
    pub const fn new(kind: SlruKind, segment_number: u32, block_number: u32) -> Self {
        Self {
            kind,
            segment_number,
            block_number,
        }
    }
}

/// SLRU families called out by the PG-4b scaffold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum SlruKind {
    /// `pg_xact`/CLOG status pages.
    Clog,
    /// Multixact offset pages.
    MultiXactOffset,
    /// Multixact member pages.
    MultiXactMember,
    /// Commit timestamp pages.
    CommitTs,
}

impl SlruKind {
    const fn ordering_tag(self) -> u8 {
        match self {
            Self::Clog => 0,
            Self::MultiXactOffset => 1,
            Self::MultiXactMember => 2,
            Self::CommitTs => 3,
        }
    }
}

/// Identifies relation metadata reserved for future storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RelMetaKey {
    /// Relation whose metadata is requested.
    pub rel: RelTag,
    /// Metadata family.
    pub kind: RelMetaKind,
}

impl RelMetaKey {
    /// Builds a typed relation metadata key.
    #[must_use]
    pub const fn new(rel: RelTag, kind: RelMetaKind) -> Self {
        Self { rel, kind }
    }
}

/// Relation metadata families called out by the PG-4b scaffold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum RelMetaKind {
    /// Relation fork size metadata.
    Size,
    /// Relation map metadata.
    RelMap,
    /// Storage manager create/drop/truncate metadata.
    StorageManager,
}

impl RelMetaKind {
    const fn ordering_tag(self) -> u8 {
        match self {
            Self::Size => 0,
            Self::RelMap => 1,
            Self::StorageManager => 2,
        }
    }
}

impl PageKey {
    /// Builds a typed page key.
    #[must_use]
    pub const fn new(
        spc_node: u32,
        db_node: u32,
        rel_node: u32,
        fork_number: u8,
        block_number: u32,
    ) -> Self {
        Self {
            rel: RelTag::new(spc_node, db_node, rel_node, fork_number),
            block_number: BlockNumber(block_number),
        }
    }

    /// Encodes the key as fixed-width lowercase hex that sorts like [`PageKey`].
    #[must_use]
    pub fn to_fixed_hex(self) -> String {
        format!(
            "{:08x}{:08x}{:08x}{:02x}{:08x}",
            self.rel.spc_node,
            self.rel.db_node,
            self.rel.rel_node,
            self.rel.fork_number.0,
            self.block_number.0
        )
    }
}

impl fmt::Display for PageKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_fixed_hex())
    }
}

impl fmt::Display for SlruPageKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "slru/{:?}/{:08x}/{:08x}",
            self.kind, self.segment_number, self.block_number
        )
    }
}

impl fmt::Display for RelMetaKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "relmeta/{:?}/{:?}", self.kind, self.rel)
    }
}

impl fmt::Display for StorageKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Relation(key) => key.fmt(formatter),
            Self::Slru(key) => key.fmt(formatter),
            Self::RelMeta(key) => key.fmt(formatter),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn fixed_hex_encoding_sorts_like_page_keys() {
        let smaller = PageKey::new(1663, 5, 16_384, 0, 7);
        let larger = PageKey::new(1663, 5, 16_385, 0, 0);

        assert!((smaller < larger) == (smaller.to_fixed_hex() < larger.to_fixed_hex()));
    }

    #[test]
    fn storage_key_preserves_existing_page_key_ordering_adapter() {
        let key = PageKey::new(1663, 5, 16_384, 0, 7);

        assert!(StorageKey::from(key) == StorageKey::Relation(key));
        assert!(StorageKey::from(key).to_string() == key.to_string());
    }

    #[test]
    fn storage_key_distinguishes_long_tail_families() {
        let slru = StorageKey::Slru(SlruPageKey::new(SlruKind::Clog, 0, 1));
        let relmeta = StorageKey::RelMeta(RelMetaKey::new(
            RelTag::new(1663, 5, 16_384, 0),
            RelMetaKind::Size,
        ));

        assert!(slru != relmeta);
        assert!(slru.to_string().starts_with("slru/"));
        assert!(relmeta.to_string().starts_with("relmeta/"));
    }

    #[test]
    fn storage_key_fixed_hex_sorts_like_tagged_key_order() {
        let keys = vec![
            StorageKey::RelMeta(RelMetaKey::new(
                RelTag::new(1663, 5, 16_384, 0),
                RelMetaKind::Size,
            )),
            StorageKey::Slru(SlruPageKey::new(SlruKind::MultiXactOffset, 1, 0)),
            StorageKey::Relation(PageKey::new(1663, 5, 16_384, 0, 7)),
            StorageKey::Slru(SlruPageKey::new(SlruKind::Clog, 0, 3)),
            StorageKey::Relation(PageKey::new(1663, 5, 16_384, 0, 0)),
        ];

        let mut sorted = keys.clone();
        sorted.sort();
        let mut by_encoding = keys;
        by_encoding.sort_by_key(|key| key.to_fixed_hex());

        assert!(sorted == by_encoding);
        assert!(matches!(sorted[0], StorageKey::Relation(_)));
        assert!(matches!(sorted[2], StorageKey::Slru(_)));
        assert!(matches!(sorted[4], StorageKey::RelMeta(_)));
    }
}
