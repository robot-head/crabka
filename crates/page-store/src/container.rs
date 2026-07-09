//! Immutable layer container writer and ranged point reader.

use std::{cmp::Ordering, io::Write as _, ops::Range};

use bytes::{BufMut as _, Bytes, BytesMut};
use crabka_object_store::{
    DEFAULT_MULTIPART_CHUNK_SIZE, DEFAULT_MULTIPART_THRESHOLD, ObjectOps, ObjectStoreError,
};
use crabka_postgres_wal::Lsn;
use object_store::{GetRange, path::Path as ObjectPath};
use thiserror::Error;

use crate::{LayerDesc, LayerKind, PAGE_SIZE, PageKey, TimelinePath, Value};

const HEADER_MAGIC: &[u8; 8] = b"CRPG3L\0\0";
const FOOTER_MAGIC: &[u8; 8] = b"CRPG3F\0\0";
const VERSION: u16 = 1;
const FOOTER_LEN: u64 = 36;
const VALUE_TAG_IMAGE: u8 = 0;
const VALUE_TAG_WAL: u8 = 1;
const ENTRY_PREFIX_LEN: u64 = 17 + 8 + 1 + 1 + 4;
const INDEX_ENTRY_LEN: usize = 17 + 8 + 8 + 8;

/// A single `(key, lsn, value)` tuple written to an immutable layer.
pub type LayerWriteEntry = (PageKey, Lsn, Value);

/// Writes sorted entries as one immutable object and returns its descriptor.
pub async fn write_layer(
    ops: &dyn ObjectOps,
    timeline: &TimelinePath,
    kind: LayerKind,
    entries: &[LayerWriteEntry],
) -> Result<LayerDesc, ContainerError> {
    let first = entries.first().ok_or(ContainerError::EmptyLayer)?;
    let last = entries.last().ok_or(ContainerError::EmptyLayer)?;
    ensure_entries_are_sorted(entries)?;

    let (lsn_start, lsn_end) = entry_lsn_range(entries);
    let desc = LayerDesc::new(timeline.clone(), kind, first.0, last.0, lsn_start, lsn_end)?;
    let bytes = encode_container(&desc, entries)?;
    let mut file = tempfile::NamedTempFile::new()?;
    file.write_all(&bytes)?;
    file.flush()?;

    ops.put_from_path(
        &ObjectPath::from(desc.object_name()),
        file.path(),
        DEFAULT_MULTIPART_THRESHOLD,
        DEFAULT_MULTIPART_CHUNK_SIZE,
    )
    .await?;

    Ok(desc)
}

/// Ranged reader for one immutable layer object.
#[derive(Debug, Clone)]
pub struct LayerReader {
    object_name: ObjectPath,
    object_size: u64,
    index: Vec<KeyIndexEntry>,
}

impl LayerReader {
    /// Opens a layer by reading and validating the footer and key index.
    pub async fn open(ops: &dyn ObjectOps, desc: &LayerDesc) -> Result<Self, ContainerError> {
        let object_name = ObjectPath::from(desc.object_name());
        let object_size = ops.head(&object_name).await?.size;
        if object_size < FOOTER_LEN {
            return Err(ContainerError::Truncated {
                object: object_name.to_string(),
                needed: FOOTER_LEN,
                actual: object_size,
            });
        }

        let footer_start = object_size - FOOTER_LEN;
        let footer = ops
            .get_range(&object_name, GetRange::Bounded(footer_start..object_size))
            .await?;
        let footer = Footer::parse(&footer, object_name.as_ref())?;
        footer.ensure_index_bounds(object_name.as_ref(), footer_start)?;

        let index_end = checked_add(footer.index_offset, footer.index_len, "index end")?;
        let index_bytes = ops
            .get_range(
                &object_name,
                GetRange::Bounded(footer.index_offset..index_end),
            )
            .await?;
        footer.ensure_index_crc(object_name.as_ref(), &index_bytes)?;
        let index = parse_index(&index_bytes, object_name.as_ref())?;
        ensure_index_matches_entry_count(&index, footer.entry_count, object_name.as_ref())?;

        Ok(Self {
            object_name,
            object_size,
            index,
        })
    }

    /// Reads all entries for `key` with one byte-range request over its entry block.
    pub async fn entries_for_key(
        &self,
        ops: &dyn ObjectOps,
        key: PageKey,
    ) -> Result<Vec<LayerWriteEntry>, ContainerError> {
        let Some(index_entry) = self.index_entry_for_key(key) else {
            return Ok(Vec::new());
        };
        let range = index_entry.entry_range(self.object_name.as_ref(), self.object_size)?;
        let bytes = ops
            .get_range(&self.object_name, GetRange::Bounded(range))
            .await?;
        parse_entries_for_key(&bytes, key, self.object_name.as_ref())
    }

    /// Reads every entry in this layer in deterministic `(key, lsn)` order.
    pub async fn entries(
        &self,
        ops: &dyn ObjectOps,
    ) -> Result<Vec<LayerWriteEntry>, ContainerError> {
        let mut entries = Vec::new();
        for index_entry in &self.index {
            entries.extend(self.entries_for_key(ops, index_entry.key).await?);
        }
        Ok(entries)
    }

    fn index_entry_for_key(&self, key: PageKey) -> Option<&KeyIndexEntry> {
        self.index
            .binary_search_by(|entry| entry.key.cmp(&key))
            .ok()
            .map(|index| &self.index[index])
    }
}

/// Errors returned while writing or reading layer containers.
#[derive(Debug, Error)]
pub enum ContainerError {
    /// Layer containers must contain at least one entry.
    #[error("layer container cannot be empty")]
    EmptyLayer,
    /// Entries must arrive in deterministic `(key, lsn)` order.
    #[error(
        "layer entries are not sorted at entry {index}: previous ({previous_key}, {previous_lsn}) > current ({current_key}, {current_lsn})"
    )]
    UnsortedEntries {
        /// Index of the out-of-order entry.
        index: usize,
        /// Previous key.
        previous_key: PageKey,
        /// Previous LSN.
        previous_lsn: Lsn,
        /// Current key.
        current_key: PageKey,
        /// Current LSN.
        current_lsn: Lsn,
    },
    /// A container object is too short for the requested structure.
    #[error("container `{object}` is truncated: need {needed} bytes, got {actual}")]
    Truncated {
        /// Object name.
        object: String,
        /// Bytes needed.
        needed: u64,
        /// Bytes available.
        actual: u64,
    },
    /// Container bytes failed deterministic validation.
    #[error("container `{object}` is corrupt: {reason}")]
    Corrupt {
        /// Object name.
        object: String,
        /// Human-readable reason.
        reason: &'static str,
    },
    /// Layer descriptor invariants failed.
    #[error(transparent)]
    Layer(#[from] crate::LayerError),
    /// Object store operation failed.
    #[error(transparent)]
    ObjectStore(#[from] ObjectStoreError),
    /// Local temporary-file I/O failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A length or offset exceeded the container format bounds.
    #[error("container field `{field}` is too large: {value}")]
    TooLarge {
        /// Field name.
        field: &'static str,
        /// Rejected value.
        value: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KeyIndexEntry {
    key: PageKey,
    first_lsn: Lsn,
    offset: u64,
    len: u64,
}

impl KeyIndexEntry {
    fn entry_range(&self, object: &str, object_size: u64) -> Result<Range<u64>, ContainerError> {
        let end = checked_add(self.offset, self.len, "entry range end")?;
        if end > object_size.saturating_sub(FOOTER_LEN) {
            return Err(ContainerError::Truncated {
                object: object.to_owned(),
                needed: end,
                actual: object_size,
            });
        }

        Ok(self.offset..end)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Footer {
    index_offset: u64,
    index_len: u64,
    entry_count: u64,
    index_crc32c: u32,
}

impl Footer {
    fn parse(bytes: &[u8], object: &str) -> Result<Self, ContainerError> {
        let mut reader = ByteReader::new(bytes, object);
        let index_offset = reader.read_u64()?;
        let index_len = reader.read_u64()?;
        let entry_count = reader.read_u64()?;
        let index_crc32c = reader.read_u32()?;
        let magic = reader.read_array::<8>()?;
        if magic != *FOOTER_MAGIC {
            return Err(corrupt(object, "footer magic mismatch"));
        }

        Ok(Self {
            index_offset,
            index_len,
            entry_count,
            index_crc32c,
        })
    }

    fn ensure_index_bounds(&self, object: &str, footer_start: u64) -> Result<(), ContainerError> {
        let index_end = checked_add(self.index_offset, self.index_len, "index end")?;
        if index_end != footer_start {
            return Err(corrupt(object, "index does not end at footer"));
        }
        if usize::try_from(self.index_len).is_err() {
            return Err(ContainerError::TooLarge {
                field: "index_len",
                value: self.index_len,
            });
        }

        Ok(())
    }

    fn ensure_index_crc(&self, object: &str, index_bytes: &[u8]) -> Result<(), ContainerError> {
        if crc32c::crc32c(index_bytes) != self.index_crc32c {
            return Err(corrupt(object, "index crc32c mismatch"));
        }

        Ok(())
    }
}

struct ByteReader<'a> {
    bytes: &'a [u8],
    object: &'a str,
    offset: usize,
}

impl<'a> ByteReader<'a> {
    const fn new(bytes: &'a [u8], object: &'a str) -> Self {
        Self {
            bytes,
            object,
            offset: 0,
        }
    }

    fn read_u8(&mut self) -> Result<u8, ContainerError> {
        let [byte] = self.read_array::<1>()?;
        Ok(byte)
    }

    fn read_u32(&mut self) -> Result<u32, ContainerError> {
        Ok(u32::from_le_bytes(self.read_array::<4>()?))
    }

    fn read_u64(&mut self) -> Result<u64, ContainerError> {
        Ok(u64::from_le_bytes(self.read_array::<8>()?))
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], ContainerError> {
        let end = self.offset.checked_add(N).ok_or(ContainerError::TooLarge {
            field: "reader offset",
            value: u64::MAX,
        })?;
        let Some(slice) = self.bytes.get(self.offset..end) else {
            return Err(ContainerError::Truncated {
                object: self.object.to_owned(),
                needed: u64::try_from(end).unwrap_or(u64::MAX),
                actual: u64::try_from(self.bytes.len()).unwrap_or(u64::MAX),
            });
        };
        self.offset = end;
        let mut array = [0_u8; N];
        array.copy_from_slice(slice);
        Ok(array)
    }

    fn read_bytes(&mut self, len: usize) -> Result<Bytes, ContainerError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(ContainerError::TooLarge {
                field: "value length",
                value: u64::try_from(len).unwrap_or(u64::MAX),
            })?;
        let Some(slice) = self.bytes.get(self.offset..end) else {
            return Err(ContainerError::Truncated {
                object: self.object.to_owned(),
                needed: u64::try_from(end).unwrap_or(u64::MAX),
                actual: u64::try_from(self.bytes.len()).unwrap_or(u64::MAX),
            });
        };
        self.offset = end;
        Ok(Bytes::copy_from_slice(slice))
    }

    const fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }
}

fn encode_container(
    desc: &LayerDesc,
    entries: &[LayerWriteEntry],
) -> Result<Vec<u8>, ContainerError> {
    let mut bytes = BytesMut::new();
    encode_header(&mut bytes, desc)?;

    let mut index = Vec::new();
    let mut entry_count = 0_u64;
    let mut entries_for_key = entries.iter().peekable();
    while let Some((key, lsn, value)) = entries_for_key.next() {
        let start = u64::try_from(bytes.len()).map_err(|_| ContainerError::TooLarge {
            field: "entry offset",
            value: u64::MAX,
        })?;
        encode_entry(&mut bytes, *key, *lsn, value)?;
        entry_count += 1;

        while let Some((next_key, next_lsn, next_value)) = entries_for_key.peek() {
            if next_key != key {
                break;
            }
            encode_entry(&mut bytes, *next_key, *next_lsn, next_value)?;
            entry_count += 1;
            let _ = entries_for_key.next();
        }

        let end = u64::try_from(bytes.len()).map_err(|_| ContainerError::TooLarge {
            field: "entry offset",
            value: u64::MAX,
        })?;
        index.push(KeyIndexEntry {
            key: *key,
            first_lsn: *lsn,
            offset: start,
            len: end - start,
        });
    }

    let index_offset = u64::try_from(bytes.len()).map_err(|_| ContainerError::TooLarge {
        field: "index offset",
        value: u64::MAX,
    })?;
    let index_bytes = encode_index(&index);
    let index_len = u64::try_from(index_bytes.len()).map_err(|_| ContainerError::TooLarge {
        field: "index_len",
        value: u64::MAX,
    })?;
    let index_crc32c = crc32c::crc32c(&index_bytes);
    bytes.extend_from_slice(&index_bytes);
    encode_footer(
        &mut bytes,
        index_offset,
        index_len,
        entry_count,
        index_crc32c,
    );
    Ok(bytes.to_vec())
}

fn encode_header(bytes: &mut BytesMut, desc: &LayerDesc) -> Result<(), ContainerError> {
    let timeline_prefix = desc.timeline.prefix();
    let timeline_len =
        u16::try_from(timeline_prefix.len()).map_err(|_| ContainerError::TooLarge {
            field: "timeline prefix length",
            value: u64::try_from(timeline_prefix.len()).unwrap_or(u64::MAX),
        })?;
    bytes.extend_from_slice(HEADER_MAGIC);
    bytes.put_u16_le(VERSION);
    bytes.put_u8(kind_tag(desc.kind));
    bytes.put_u8(0);
    bytes.put_u16_le(timeline_len);
    bytes.extend_from_slice(timeline_prefix.as_bytes());
    encode_key(bytes, desc.key_start);
    encode_key(bytes, desc.key_end);
    bytes.put_u64_le(desc.lsn_start.value());
    bytes.put_u64_le(desc.lsn_end.value());
    Ok(())
}

fn encode_entry(
    bytes: &mut BytesMut,
    key: PageKey,
    lsn: Lsn,
    value: &Value,
) -> Result<(), ContainerError> {
    encode_key(bytes, key);
    bytes.put_u64_le(lsn.value());
    match value {
        Value::Image(page) => {
            bytes.put_u8(VALUE_TAG_IMAGE);
            bytes.put_u8(0);
            bytes.put_u32_le(
                u32::try_from(page.len()).map_err(|_| ContainerError::TooLarge {
                    field: "image length",
                    value: u64::try_from(page.len()).unwrap_or(u64::MAX),
                })?,
            );
            bytes.extend_from_slice(page);
        }
        Value::Wal { will_init, rec } => {
            bytes.put_u8(VALUE_TAG_WAL);
            bytes.put_u8(u8::from(*will_init));
            bytes.put_u32_le(
                u32::try_from(rec.len()).map_err(|_| ContainerError::TooLarge {
                    field: "wal length",
                    value: u64::try_from(rec.len()).unwrap_or(u64::MAX),
                })?,
            );
            bytes.extend_from_slice(rec);
        }
    }
    Ok(())
}

fn encode_index(index: &[KeyIndexEntry]) -> Bytes {
    let mut bytes = BytesMut::with_capacity(index.len() * INDEX_ENTRY_LEN);
    for entry in index {
        encode_key(&mut bytes, entry.key);
        bytes.put_u64_le(entry.first_lsn.value());
        bytes.put_u64_le(entry.offset);
        bytes.put_u64_le(entry.len);
    }
    bytes.freeze()
}

fn encode_footer(
    bytes: &mut BytesMut,
    index_offset: u64,
    index_len: u64,
    entry_count: u64,
    index_crc32c: u32,
) {
    bytes.put_u64_le(index_offset);
    bytes.put_u64_le(index_len);
    bytes.put_u64_le(entry_count);
    bytes.put_u32_le(index_crc32c);
    bytes.extend_from_slice(FOOTER_MAGIC);
}

fn parse_index(bytes: &[u8], object: &str) -> Result<Vec<KeyIndexEntry>, ContainerError> {
    if !bytes.len().is_multiple_of(INDEX_ENTRY_LEN) {
        return Err(corrupt(object, "index length is not entry-aligned"));
    }

    let mut reader = ByteReader::new(bytes, object);
    let mut index = Vec::with_capacity(bytes.len() / INDEX_ENTRY_LEN);
    while reader.remaining() > 0 {
        index.push(KeyIndexEntry {
            key: decode_key(&mut reader)?,
            first_lsn: Lsn(reader.read_u64()?),
            offset: reader.read_u64()?,
            len: reader.read_u64()?,
        });
    }
    ensure_index_is_sorted(&index, object)?;
    Ok(index)
}

fn parse_entries_for_key(
    bytes: &[u8],
    expected_key: PageKey,
    object: &str,
) -> Result<Vec<LayerWriteEntry>, ContainerError> {
    let mut reader = ByteReader::new(bytes, object);
    let mut entries = Vec::new();
    while reader.remaining() > 0 {
        let key = decode_key(&mut reader)?;
        if key != expected_key {
            return Err(corrupt(object, "entry block contains a different key"));
        }
        let lsn = Lsn(reader.read_u64()?);
        let tag = reader.read_u8()?;
        let will_init = reader.read_u8()? != 0;
        let len = usize::try_from(reader.read_u32()?).map_err(|_| ContainerError::TooLarge {
            field: "value length",
            value: u64::MAX,
        })?;
        let value_bytes = reader.read_bytes(len)?;
        let value = parse_value(tag, will_init, value_bytes, object)?;
        entries.push((key, lsn, value));
    }

    Ok(entries)
}

fn parse_value(
    tag: u8,
    will_init: bool,
    bytes: Bytes,
    object: &str,
) -> Result<Value, ContainerError> {
    match tag {
        VALUE_TAG_IMAGE => {
            if will_init {
                return Err(corrupt(object, "image value has will_init flag"));
            }
            if bytes.len() != PAGE_SIZE {
                return Err(corrupt(object, "image value length is not one page"));
            }
            Ok(Value::Image(bytes))
        }
        VALUE_TAG_WAL => Ok(Value::Wal {
            will_init,
            rec: bytes,
        }),
        _ => Err(corrupt(object, "unknown value tag")),
    }
}

fn encode_key(bytes: &mut BytesMut, key: PageKey) {
    bytes.put_u32_le(key.rel.spc_node);
    bytes.put_u32_le(key.rel.db_node);
    bytes.put_u32_le(key.rel.rel_node);
    bytes.put_u8(key.rel.fork_number.0);
    bytes.put_u32_le(key.block_number.0);
}

fn decode_key(reader: &mut ByteReader<'_>) -> Result<PageKey, ContainerError> {
    let spc_node = reader.read_u32()?;
    let db_node = reader.read_u32()?;
    let rel_node = reader.read_u32()?;
    let fork_number = reader.read_u8()?;
    let block_number = reader.read_u32()?;
    Ok(PageKey::new(
        spc_node,
        db_node,
        rel_node,
        fork_number,
        block_number,
    ))
}

const fn kind_tag(kind: LayerKind) -> u8 {
    match kind {
        LayerKind::Image => 0,
        LayerKind::Delta => 1,
    }
}

fn ensure_entries_are_sorted(entries: &[LayerWriteEntry]) -> Result<(), ContainerError> {
    for (index, pair) in entries.windows(2).enumerate() {
        let [previous, current] = pair else {
            return Ok(());
        };
        if (previous.0, previous.1) <= (current.0, current.1) {
            continue;
        }

        return Err(ContainerError::UnsortedEntries {
            index: index + 1,
            previous_key: previous.0,
            previous_lsn: previous.1,
            current_key: current.0,
            current_lsn: current.1,
        });
    }

    Ok(())
}

fn entry_lsn_range(entries: &[LayerWriteEntry]) -> (Lsn, Lsn) {
    let (_, first_lsn, _) = entries
        .first()
        .expect("non-empty entries are required for a layer descriptor");
    let mut lsn_start = *first_lsn;
    let mut lsn_end = *first_lsn;

    for (_, lsn, _) in entries.iter().skip(1) {
        lsn_start = lsn_start.min(*lsn);
        lsn_end = lsn_end.max(*lsn);
    }

    (lsn_start, lsn_end)
}

fn ensure_index_is_sorted(index: &[KeyIndexEntry], object: &str) -> Result<(), ContainerError> {
    for pair in index.windows(2) {
        let [previous, current] = pair else {
            return Ok(());
        };
        if previous.key.cmp(&current.key) == Ordering::Less {
            continue;
        }

        return Err(corrupt(object, "index keys are not strictly sorted"));
    }

    Ok(())
}

fn ensure_index_matches_entry_count(
    index: &[KeyIndexEntry],
    entry_count: u64,
    object: &str,
) -> Result<(), ContainerError> {
    let mut indexed_entries = 0_u64;
    for entry in index {
        if entry.len < ENTRY_PREFIX_LEN {
            return Err(corrupt(object, "indexed entry range is too short"));
        }
        indexed_entries = indexed_entries.saturating_add(1);
    }
    if indexed_entries > entry_count {
        return Err(corrupt(object, "index has more keys than entries"));
    }

    Ok(())
}

fn checked_add(left: u64, right: u64, field: &'static str) -> Result<u64, ContainerError> {
    left.checked_add(right)
        .ok_or(ContainerError::TooLarge { field, value: left })
}

fn corrupt(object: &str, reason: &'static str) -> ContainerError {
    ContainerError::Corrupt {
        object: object.to_owned(),
        reason,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use crabka_object_store::{ObjectOps as _, ObjectStoreClient};
    use object_store::memory::InMemory;

    use super::*;
    use crate::{LayerIndex, TenantId, TimelineId};

    fn ops() -> ObjectStoreClient {
        ObjectStoreClient::new(Arc::new(InMemory::new()))
    }

    fn timeline() -> TimelinePath {
        TimelinePath::new(
            TenantId::parse("tenant").expect("tenant id is valid"),
            TimelineId::parse("timeline").expect("timeline id is valid"),
        )
    }

    fn key(block_number: u32) -> PageKey {
        PageKey::new(1663, 5, 16_384, 0, block_number)
    }

    fn page(byte: u8) -> Bytes {
        Bytes::from(vec![byte; PAGE_SIZE])
    }

    #[tokio::test]
    async fn container_round_trips_and_point_reads() {
        let ops = ops();
        let entries = vec![
            (
                key(0),
                Lsn(10),
                Value::image(page(b'a')).expect("test image is one page"),
            ),
            (
                key(0),
                Lsn(20),
                Value::Wal {
                    will_init: false,
                    rec: Bytes::from_static(b"r1"),
                },
            ),
            (
                key(7),
                Lsn(15),
                Value::Wal {
                    will_init: true,
                    rec: Bytes::from_static(b"r2"),
                },
            ),
        ];

        let desc = write_layer(&ops, &timeline(), LayerKind::Delta, &entries)
            .await
            .expect("layer writes");
        let reader = LayerReader::open(&ops, &desc).await.expect("layer opens");
        let got = reader
            .entries_for_key(&ops, key(0))
            .await
            .expect("point read succeeds");

        assert!(got.len() == 2);
        assert!(got[0].1 == Lsn(10));
        assert!(got[1].1 == Lsn(20));
        assert!(matches!(&got[0].2, Value::Image(img) if img.as_ref() == page(b'a').as_ref()));
    }

    #[tokio::test]
    async fn missing_key_returns_empty_point_read() {
        let ops = ops();
        let entries = vec![(
            key(0),
            Lsn(10),
            Value::Wal {
                will_init: false,
                rec: Bytes::from_static(b"r1"),
            },
        )];
        let desc = write_layer(&ops, &timeline(), LayerKind::Delta, &entries)
            .await
            .expect("layer writes");
        let reader = LayerReader::open(&ops, &desc).await.expect("layer opens");

        assert!(
            reader
                .entries_for_key(&ops, key(1))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn descriptor_lsn_range_covers_all_key_sorted_entries() {
        let ops = ops();
        let entries = vec![
            (
                key(0),
                Lsn(30),
                Value::Wal {
                    will_init: false,
                    rec: Bytes::from_static(b"highest-on-first-key"),
                },
            ),
            (
                key(1),
                Lsn(10),
                Value::Wal {
                    will_init: false,
                    rec: Bytes::from_static(b"lowest-on-last-key"),
                },
            ),
        ];

        let desc = write_layer(&ops, &timeline(), LayerKind::Delta, &entries)
            .await
            .expect("layer writes");
        let mut index = LayerIndex::new();
        index.insert(desc.clone());

        assert!(desc.key_start == key(0));
        assert!(desc.key_end == key(1));
        assert!(desc.lsn_start == Lsn(10));
        assert!(desc.lsn_end == Lsn(30));
        assert!(desc.contains_lsn(Lsn(30)));
        assert!(index.best_layer(key(0), Lsn(30)) == Some(&desc));
    }

    #[tokio::test]
    async fn corrupt_footer_is_a_loud_error() {
        let ops = ops();
        let entries = vec![(
            key(0),
            Lsn(10),
            Value::Wal {
                will_init: false,
                rec: Bytes::from_static(b"r1"),
            },
        )];
        let desc = write_layer(&ops, &timeline(), LayerKind::Delta, &entries)
            .await
            .expect("layer writes");
        let object_name = ObjectPath::from(desc.object_name());
        let mut bytes = ops.get(&object_name).await.expect("object exists").to_vec();
        let last = bytes.last_mut().expect("container is non-empty");
        *last ^= 0xff;
        ops.put(&object_name, Bytes::from(bytes))
            .await
            .expect("corrupted object overwrites in memory");

        let err = LayerReader::open(&ops, &desc).await.unwrap_err();

        assert!(matches!(err, ContainerError::Corrupt { .. }));
    }

    #[tokio::test]
    async fn truncated_container_is_a_loud_error() {
        let ops = ops();
        let desc = LayerDesc::new(
            timeline(),
            LayerKind::Delta,
            key(0),
            key(0),
            Lsn(10),
            Lsn(10),
        )
        .expect("descriptor ranges are valid");
        ops.put(
            &ObjectPath::from(desc.object_name()),
            Bytes::from_static(b"short"),
        )
        .await
        .expect("short object writes");

        let err = LayerReader::open(&ops, &desc).await.unwrap_err();

        assert!(matches!(err, ContainerError::Truncated { .. }));
    }

    #[tokio::test]
    async fn writer_rejects_unsorted_entries() {
        let ops = ops();
        let entries = vec![
            (
                key(1),
                Lsn(10),
                Value::Wal {
                    will_init: false,
                    rec: Bytes::from_static(b"r1"),
                },
            ),
            (
                key(0),
                Lsn(20),
                Value::Wal {
                    will_init: false,
                    rec: Bytes::from_static(b"r2"),
                },
            ),
        ];

        let err = write_layer(&ops, &timeline(), LayerKind::Delta, &entries)
            .await
            .unwrap_err();

        assert!(matches!(err, ContainerError::UnsortedEntries { .. }));
    }
}
