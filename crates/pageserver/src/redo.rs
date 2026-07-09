//! Redo adapters used by the page service.

use bytes::{BufMut as _, Bytes, BytesMut};
use crabka_page_store::{PAGE_SIZE, PageKey, ReconstructData};
use crabka_postgres_redo::{PageImage, RedoAction, RedoRecord, apply_redo_records};
use crabka_postgres_wal::Lsn;

use crate::{RedoDecodeError, RedoReconstructionError};

const TAG_BYTE_RANGE_PATCH: u8 = 1;
const TAG_INITIALIZE_PAGE: u8 = 2;
const TAG_FULL_PAGE_IMAGE: u8 = 3;
const U64_FIELD_LEN: usize = 8;

/// Reconstructs a page from page-store reconstruction data.
pub trait PageRedo: Send + Sync {
    /// Materializes `key` at `target_lsn` from page-store base/delta data.
    fn reconstruct_page(
        &self,
        key: PageKey,
        data: ReconstructData,
        target_lsn: Lsn,
    ) -> Result<Bytes, RedoReconstructionError>;
}

/// Decodes opaque WAL bytes retained by page-store into a typed redo record.
pub trait RedoRecordDecoder: Send + Sync {
    /// Decodes one delta record for `key` at `lsn`.
    fn decode_record(
        &self,
        key: PageKey,
        lsn: Lsn,
        bytes: Bytes,
    ) -> Result<RedoRecord, RedoDecodeError>;
}

/// `crabka-postgres-redo` adapter parameterized by an opaque-delta decoder.
#[derive(Debug, Clone)]
pub struct PostgresRedo<D> {
    decoder: D,
}

impl<D> PostgresRedo<D> {
    /// Builds a postgres-redo adapter using `decoder` for stored WAL bytes.
    #[must_use]
    pub const fn new(decoder: D) -> Self {
        Self { decoder }
    }
}

impl<D> PageRedo for PostgresRedo<D>
where
    D: RedoRecordDecoder,
{
    fn reconstruct_page(
        &self,
        key: PageKey,
        data: ReconstructData,
        target_lsn: Lsn,
    ) -> Result<Bytes, RedoReconstructionError> {
        if let Some((_, image)) = &data.base
            && data.deltas.is_empty()
        {
            return Ok(image.clone());
        }

        let base = base_page_image(key, data.base)?;
        let records = decode_records(&self.decoder, key, data.deltas)?;
        if base.is_none() && records.is_empty() {
            return Err(RedoReconstructionError::EmptyChain {
                key,
                lsn: target_lsn,
            });
        }

        Ok(apply_redo_records(base, &records)?.into_bytes())
    }
}

fn base_page_image(
    key: PageKey,
    base: Option<(Lsn, Bytes)>,
) -> Result<Option<PageImage>, RedoReconstructionError> {
    let Some((lsn, image)) = base else {
        return Ok(None);
    };

    PageImage::new(key, lsn, image)
        .map(Some)
        .map_err(|source| RedoReconstructionError::InvalidBaseImage { key, lsn, source })
}

fn decode_records(
    decoder: &dyn RedoRecordDecoder,
    key: PageKey,
    deltas: Vec<(Lsn, Bytes)>,
) -> Result<Vec<RedoRecord>, RedoReconstructionError> {
    deltas
        .into_iter()
        .map(|(lsn, bytes)| decoder.decode_record(key, lsn, bytes).map_err(Into::into))
        .collect()
}

/// Tiny deterministic codec for tests and pre-wire-format synthetic redo records.
#[derive(Debug, Default, Clone, Copy)]
pub struct SyntheticRedoCodec;

impl SyntheticRedoCodec {
    /// Encodes one supported synthetic [`RedoRecord`] action into page-store WAL bytes.
    #[must_use]
    pub fn encode_record(record: &RedoRecord) -> Bytes {
        match &record.action {
            RedoAction::ByteRangePatch(patch) => {
                let mut bytes = BytesMut::with_capacity(1 + U64_FIELD_LEN + patch.bytes.len());
                bytes.put_u8(TAG_BYTE_RANGE_PATCH);
                bytes.put_u64_le(u64::try_from(patch.offset).unwrap_or(u64::MAX));
                bytes.extend_from_slice(&patch.bytes);
                bytes.freeze()
            }
            RedoAction::InitializePage { image } => encode_page_image(TAG_INITIALIZE_PAGE, image),
            RedoAction::FullPageImage { image } => encode_page_image(TAG_FULL_PAGE_IMAGE, image),
        }
    }
}

impl RedoRecordDecoder for SyntheticRedoCodec {
    fn decode_record(
        &self,
        key: PageKey,
        lsn: Lsn,
        bytes: Bytes,
    ) -> Result<RedoRecord, RedoDecodeError> {
        let mut reader = SyntheticRecordReader::new(key, lsn, bytes);
        let tag = reader.read_u8("tag")?;
        let record = match tag {
            TAG_BYTE_RANGE_PATCH => decode_byte_range_patch(&mut reader)?,
            TAG_INITIALIZE_PAGE => RedoRecord::initialize_page(key, lsn, reader.read_page_image()?),
            TAG_FULL_PAGE_IMAGE => RedoRecord::full_page_image(key, lsn, reader.read_page_image()?),
            tag => return Err(RedoDecodeError::UnsupportedTag { key, lsn, tag }),
        };
        reader.finish()?;
        Ok(record)
    }
}

fn encode_page_image(tag: u8, image: &Bytes) -> Bytes {
    let mut bytes = BytesMut::with_capacity(1 + PAGE_SIZE);
    bytes.put_u8(tag);
    bytes.extend_from_slice(image);
    bytes.freeze()
}

fn decode_byte_range_patch(
    reader: &mut SyntheticRecordReader,
) -> Result<RedoRecord, RedoDecodeError> {
    let offset = reader.read_usize("offset")?;
    let bytes = reader.read_remaining();
    Ok(RedoRecord::byte_range_patch(
        reader.key, reader.lsn, offset, bytes,
    ))
}

struct SyntheticRecordReader {
    key: PageKey,
    lsn: Lsn,
    bytes: Bytes,
    offset: usize,
}

impl SyntheticRecordReader {
    const fn new(key: PageKey, lsn: Lsn, bytes: Bytes) -> Self {
        Self {
            key,
            lsn,
            bytes,
            offset: 0,
        }
    }

    fn read_u8(&mut self, field: &'static str) -> Result<u8, RedoDecodeError> {
        let Some(byte) = self.bytes.get(self.offset).copied() else {
            return Err(self.truncated(field));
        };
        self.offset += 1;
        Ok(byte)
    }

    fn read_usize(&mut self, field: &'static str) -> Result<usize, RedoDecodeError> {
        let raw = self.read_array::<U64_FIELD_LEN>(field)?;
        let value = u64::from_le_bytes(raw);
        usize::try_from(value).map_err(|_| RedoDecodeError::FieldTooLarge {
            key: self.key,
            lsn: self.lsn,
            field,
            value,
        })
    }

    fn read_page_image(&mut self) -> Result<Bytes, RedoDecodeError> {
        let image = self.read_bytes(PAGE_SIZE, "page image")?;
        Ok(image)
    }

    fn read_array<const N: usize>(
        &mut self,
        field: &'static str,
    ) -> Result<[u8; N], RedoDecodeError> {
        let bytes = self.read_bytes(N, field)?;
        let mut array = [0_u8; N];
        array.copy_from_slice(&bytes);
        Ok(array)
    }

    fn read_bytes(&mut self, len: usize, field: &'static str) -> Result<Bytes, RedoDecodeError> {
        let Some(end) = self.offset.checked_add(len) else {
            return Err(self.truncated(field));
        };
        let Some(slice) = self.bytes.get(self.offset..end) else {
            return Err(self.truncated(field));
        };
        self.offset = end;
        Ok(Bytes::copy_from_slice(slice))
    }

    fn read_remaining(&mut self) -> Bytes {
        let bytes = self.bytes.slice(self.offset..);
        self.offset = self.bytes.len();
        bytes
    }

    fn finish(&self) -> Result<(), RedoDecodeError> {
        let extra = self.bytes.len().saturating_sub(self.offset);
        if extra == 0 {
            return Ok(());
        }

        Err(RedoDecodeError::TrailingBytes {
            key: self.key,
            lsn: self.lsn,
            extra,
        })
    }

    const fn truncated(&self, field: &'static str) -> RedoDecodeError {
        RedoDecodeError::Truncated {
            key: self.key,
            lsn: self.lsn,
            field,
        }
    }
}
