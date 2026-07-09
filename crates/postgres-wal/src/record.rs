//! WAL record headers and CRC validation.

use crate::{Lsn, framing::WalDecodeError};

/// Size in bytes of a `PostgreSQL` `XLogRecord` header.
pub const XLOG_RECORD_HEADER_SIZE: usize = 24;

const XLOG_RECORD_HEADER_SIZE_U32: u32 = 24;
const CRC_OFFSET: usize = 20;

const XLR_MAX_BLOCK_ID: u8 = 32;
const XLR_BLOCK_ID_DATA_SHORT: u8 = 255;
const XLR_BLOCK_ID_DATA_LONG: u8 = 254;
const XLR_BLOCK_ID_ORIGIN: u8 = 253;
const XLR_BLOCK_ID_TOPLEVEL_XID: u8 = 252;

const BKPBLOCK_HAS_IMAGE: u8 = 0x10;
const BKPBLOCK_HAS_DATA: u8 = 0x20;
const BKPBLOCK_WILL_INIT: u8 = 0x40;
const BKPBLOCK_SAME_REL: u8 = 0x80;
const BKPBLOCK_FORK_MASK: u8 = 0x0F;

const BKPIMAGE_HAS_HOLE: u8 = 0x01;
const BKPIMAGE_APPLY: u8 = 0x02;
const BKPIMAGE_COMPRESSED_BITS: u8 = 0x04 | 0x08 | 0x10;

const BLOCK_HEADER_SIZE: usize = 4;
const BLOCK_IMAGE_HEADER_SIZE: usize = 5;
const REL_FILE_LOCATOR_SIZE: usize = 12;
const BLOCK_NUMBER_SIZE: usize = 4;
const SHORT_MAIN_DATA_HEADER_SIZE: usize = 2;
const LONG_MAIN_DATA_HEADER_SIZE: usize = 5;
const ORIGIN_HEADER_SIZE: usize = 5;
const TOPLEVEL_XID_HEADER_SIZE: usize = 5;

/// The fixed `PostgreSQL` `XLogRecord` header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct XLogRecordHeader {
    /// Total record length, including this 24-byte header.
    pub total_len: u32,
    /// Transaction ID associated with the record.
    pub xid: u32,
    /// Previous record LSN.
    pub prev_lsn: Lsn,
    /// Resource-manager-specific info bits.
    pub info: u8,
    /// Resource manager identifier.
    pub rmid: u8,
    /// Stored CRC-32C over the record body and header prefix.
    pub crc: u32,
}

impl XLogRecordHeader {
    /// Parses a complete fixed WAL record header from `bytes`.
    pub fn parse(bytes: &[u8], start_lsn: Lsn) -> Result<Self, WalDecodeError> {
        if bytes.len() < XLOG_RECORD_HEADER_SIZE {
            return Err(WalDecodeError::TruncatedRecordHeader {
                lsn: start_lsn,
                needed: XLOG_RECORD_HEADER_SIZE,
                got: bytes.len(),
            });
        }

        let total_len = read_u32(bytes, 0);
        if total_len < XLOG_RECORD_HEADER_SIZE_U32 {
            return Err(WalDecodeError::RecordTooShort {
                lsn: start_lsn,
                total_len,
                minimum: XLOG_RECORD_HEADER_SIZE_U32,
            });
        }

        Ok(Self {
            total_len,
            xid: read_u32(bytes, 4),
            prev_lsn: Lsn(read_u64(bytes, 8)),
            info: bytes[16],
            rmid: bytes[17],
            crc: read_u32(bytes, CRC_OFFSET),
        })
    }
}

/// A reassembled WAL record with its framing metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XLogRecord {
    /// LSN of the first byte of the record header.
    pub start_lsn: Lsn,
    /// Record length in bytes, excluding MAXALIGN padding.
    pub total_len: u32,
    /// Parsed fixed record header.
    pub header: XLogRecordHeader,
    /// Exact record bytes, including the 24-byte header and excluding padding.
    pub bytes: Box<[u8]>,
}

impl XLogRecord {
    /// Parses `bytes` as one complete WAL record and validates its CRC-32C.
    pub fn parse(bytes: Vec<u8>, start_lsn: Lsn) -> Result<Self, WalDecodeError> {
        let header = XLogRecordHeader::parse(&bytes, start_lsn)?;
        let total_len = header.total_len as usize;

        if bytes.len() != total_len {
            return Err(WalDecodeError::TruncatedRecord {
                lsn: start_lsn,
                needed: total_len,
                got: bytes.len(),
            });
        }

        validate_record_crc(&bytes, header, start_lsn)?;

        Ok(Self {
            start_lsn,
            total_len: header.total_len,
            header,
            bytes: bytes.into_boxed_slice(),
        })
    }

    /// Decodes this record's resource-manager-agnostic PG 17 body grammar.
    pub fn decode(&self) -> Result<DecodedRecord, WalDecodeError> {
        DecodedRecord::parse(self)
    }
}

/// A WAL record decoded to the bounded, resource-manager-agnostic PG 17 body grammar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedRecord {
    /// LSN of the fixed record header.
    pub start_lsn: Lsn,
    /// Record length in bytes, excluding MAXALIGN padding.
    pub total_len: u32,
    /// Parsed fixed record header.
    pub header: XLogRecordHeader,
    /// Decoded block references in record-header order.
    pub blocks: Vec<BlockRef>,
    /// Main-data bytes following block image/data payloads.
    pub main_data: Box<[u8]>,
    /// Optional origin value when the origin header is present.
    pub origin: Option<u32>,
    /// Optional top-level transaction ID when the header is present.
    pub toplevel_xid: Option<u32>,
}

impl DecodedRecord {
    /// Parses a decoded body from a CRC-validated WAL record.
    pub fn parse(record: &XLogRecord) -> Result<Self, WalDecodeError> {
        let body = &record.bytes[XLOG_RECORD_HEADER_SIZE..];
        let mut parser = RecordBodyParser::new(record.start_lsn, body);
        let parsed_body = parser.parse()?;

        Ok(Self {
            start_lsn: record.start_lsn,
            total_len: record.total_len,
            header: record.header,
            blocks: parsed_body.blocks,
            main_data: parsed_body.main_data.into_boxed_slice(),
            origin: parsed_body.origin,
            toplevel_xid: parsed_body.toplevel_xid,
        })
    }
}

/// A decoded block reference from a WAL record body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRef {
    /// Block reference ID from the WAL body.
    pub id: u8,
    /// Fork number encoded in the block flags.
    pub fork: u8,
    /// Raw and typed block flags.
    pub flags: BlockFlags,
    /// Relation locator, encoded directly or inherited through `SAME_REL`.
    pub rel: RelFileLocator,
    /// Block number within the fork.
    pub blkno: u32,
    /// Reconstructed 8 KiB full-page image, when present.
    pub image: Option<BlockImage>,
    /// Block data bytes, when present.
    pub data: Box<[u8]>,
}

impl BlockRef {
    /// Returns true when this block reference includes an FPI.
    #[must_use]
    pub const fn has_image(&self) -> bool {
        self.flags.has_image()
    }

    /// Returns true when this block reference includes block data bytes.
    #[must_use]
    pub const fn has_data(&self) -> bool {
        self.flags.has_data()
    }

    /// Returns true when redo will initialize this block.
    #[must_use]
    pub const fn will_init(&self) -> bool {
        self.flags.will_init()
    }

    /// Returns true when this block reused the previous relation locator.
    #[must_use]
    pub const fn same_rel(&self) -> bool {
        self.flags.same_rel()
    }
}

/// Raw block-reference flags with typed accessors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockFlags {
    /// Raw `fork_flags` byte from the block header.
    pub raw: u8,
}

impl BlockFlags {
    /// Returns true when the block reference includes an FPI.
    #[must_use]
    pub const fn has_image(self) -> bool {
        self.raw & BKPBLOCK_HAS_IMAGE != 0
    }

    /// Returns true when the block reference includes block data bytes.
    #[must_use]
    pub const fn has_data(self) -> bool {
        self.raw & BKPBLOCK_HAS_DATA != 0
    }

    /// Returns true when redo will initialize the block.
    #[must_use]
    pub const fn will_init(self) -> bool {
        self.raw & BKPBLOCK_WILL_INIT != 0
    }

    /// Returns true when the relation locator is inherited from the prior block.
    #[must_use]
    pub const fn same_rel(self) -> bool {
        self.raw & BKPBLOCK_SAME_REL != 0
    }
}

/// Relation locator encoded in block references.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelFileLocator {
    /// Tablespace OID.
    pub spc_oid: u32,
    /// Database OID.
    pub db_oid: u32,
    /// Relation filenode number.
    pub rel_number: u32,
}

/// A reconstructed full-page image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockImage {
    /// Full page bytes after hole reconstruction.
    pub bytes: Box<[u8]>,
    /// Offset of the hole in the page image, when a hole was encoded.
    pub hole_offset: Option<u16>,
    /// Whether the image has the `BKPIMAGE_APPLY` bit set.
    pub apply: bool,
}

impl BlockImage {
    /// Returns the reconstructed page image length.
    #[must_use]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns true when the reconstructed page image is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl AsRef<[u8]> for BlockImage {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedBody {
    blocks: Vec<BlockRef>,
    main_data: Vec<u8>,
    origin: Option<u32>,
    toplevel_xid: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PendingBlock {
    id: u8,
    fork: u8,
    flags: u8,
    data_length: usize,
    image_header: Option<BlockImageHeader>,
    rel: RelFileLocator,
    blkno: u32,
}

impl PendingBlock {
    const fn payload_length(self) -> usize {
        let image_length = match self.image_header {
            Some(image_header) => image_header.length,
            None => 0,
        };
        image_length + self.data_length
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BlockImageHeader {
    length: usize,
    hole_offset: u16,
    info: u8,
}

impl BlockImageHeader {
    const fn has_hole(self) -> bool {
        self.info & BKPIMAGE_HAS_HOLE != 0
    }

    const fn apply(self) -> bool {
        self.info & BKPIMAGE_APPLY != 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordBodyParser<'a> {
    start_lsn: Lsn,
    body: &'a [u8],
    cursor: usize,
    last_rel: Option<RelFileLocator>,
}

impl<'a> RecordBodyParser<'a> {
    const fn new(start_lsn: Lsn, body: &'a [u8]) -> Self {
        Self {
            start_lsn,
            body,
            cursor: 0,
            last_rel: None,
        }
    }

    fn parse(&mut self) -> Result<ParsedBody, WalDecodeError> {
        let mut pending_blocks = Vec::new();
        let mut pending_payload_length = 0_usize;
        let mut main_data_length = 0;
        let mut origin = None;
        let mut toplevel_xid = None;

        while let Some(header_id) = self.peek_u8() {
            if header_id <= XLR_MAX_BLOCK_ID {
                let pending_block = self.parse_block_header()?;
                pending_payload_length = pending_payload_length
                    .checked_add(pending_block.payload_length())
                    .ok_or(WalDecodeError::RecordBodyGrammar {
                        lsn: self.start_lsn,
                        context: "block payload length overflow",
                    })?;
                pending_blocks.push(pending_block);
                if self.has_reached_payload_boundary(pending_payload_length)? {
                    break;
                }
                continue;
            }

            self.cursor += 1;
            match header_id {
                XLR_BLOCK_ID_DATA_SHORT => {
                    self.require_available(
                        SHORT_MAIN_DATA_HEADER_SIZE - 1,
                        "short main-data header",
                    )?;
                    main_data_length = usize::from(self.body[self.cursor]);
                    self.cursor += 1;
                    break;
                }
                XLR_BLOCK_ID_DATA_LONG => {
                    self.require_available(
                        LONG_MAIN_DATA_HEADER_SIZE - 1,
                        "long main-data header",
                    )?;
                    main_data_length = read_u32(self.body, self.cursor) as usize;
                    self.cursor += 4;
                    break;
                }
                XLR_BLOCK_ID_ORIGIN => {
                    self.require_available(ORIGIN_HEADER_SIZE - 1, "origin header")?;
                    origin = Some(read_u32(self.body, self.cursor));
                    self.cursor += 4;
                    if self.has_reached_payload_boundary(pending_payload_length)? {
                        break;
                    }
                }
                XLR_BLOCK_ID_TOPLEVEL_XID => {
                    self.require_available(TOPLEVEL_XID_HEADER_SIZE - 1, "top-level xid header")?;
                    toplevel_xid = Some(read_u32(self.body, self.cursor));
                    self.cursor += 4;
                    if self.has_reached_payload_boundary(pending_payload_length)? {
                        break;
                    }
                }
                _ => {
                    return Err(WalDecodeError::RecordBodyGrammar {
                        lsn: self.start_lsn,
                        context: "unknown record body header id",
                    });
                }
            }
        }

        let blocks = self.parse_payload_blocks(&pending_blocks)?;
        let main_data = self.consume_main_data(main_data_length)?;

        if self.cursor == self.body.len() {
            return Ok(ParsedBody {
                blocks,
                main_data,
                origin,
                toplevel_xid,
            });
        }

        Err(WalDecodeError::RecordBodyLengthMismatch {
            lsn: self.start_lsn,
            expected: self.cursor,
            got: self.body.len(),
        })
    }

    fn parse_block_header(&mut self) -> Result<PendingBlock, WalDecodeError> {
        self.require_available(BLOCK_HEADER_SIZE, "block header")?;
        let id = self.body[self.cursor];
        let flags = self.body[self.cursor + 1];
        let data_length = usize::from(read_u16(self.body, self.cursor + 2));
        self.cursor += BLOCK_HEADER_SIZE;

        if flags & BKPBLOCK_HAS_DATA == 0 && data_length != 0 {
            return Err(WalDecodeError::RecordBodyGrammar {
                lsn: self.start_lsn,
                context: "block data length without HAS_DATA flag",
            });
        }

        let image_header = if flags & BKPBLOCK_HAS_IMAGE == 0 {
            None
        } else {
            Some(self.parse_block_image_header(id)?)
        };

        let same_rel = flags & BKPBLOCK_SAME_REL != 0;
        let rel = if same_rel {
            let Some(rel) = self.last_rel else {
                return Err(WalDecodeError::RecordBodyGrammar {
                    lsn: self.start_lsn,
                    context: "SAME_REL without a prior relation locator",
                });
            };
            rel
        } else {
            let rel = self.parse_rel_file_locator()?;
            self.last_rel = Some(rel);
            rel
        };

        self.require_available(BLOCK_NUMBER_SIZE, "block number")?;
        let blkno = read_u32(self.body, self.cursor);
        self.cursor += BLOCK_NUMBER_SIZE;

        Ok(PendingBlock {
            id,
            fork: flags & BKPBLOCK_FORK_MASK,
            flags,
            data_length,
            image_header,
            rel,
            blkno,
        })
    }

    fn parse_block_image_header(
        &mut self,
        block_id: u8,
    ) -> Result<BlockImageHeader, WalDecodeError> {
        self.require_available(BLOCK_IMAGE_HEADER_SIZE, "block image header")?;
        let length = usize::from(read_u16(self.body, self.cursor));
        let hole_offset = read_u16(self.body, self.cursor + 2);
        let info = self.body[self.cursor + 4];
        self.cursor += BLOCK_IMAGE_HEADER_SIZE;

        if info & BKPIMAGE_COMPRESSED_BITS == 0 {
            return Ok(BlockImageHeader {
                length,
                hole_offset,
                info,
            });
        }

        Err(WalDecodeError::CompressedFpiUnsupported {
            lsn: self.start_lsn,
            block_id,
            bimg_info: info,
        })
    }

    fn parse_rel_file_locator(&mut self) -> Result<RelFileLocator, WalDecodeError> {
        self.require_available(REL_FILE_LOCATOR_SIZE, "relation locator")?;
        let rel = RelFileLocator {
            spc_oid: read_u32(self.body, self.cursor),
            db_oid: read_u32(self.body, self.cursor + 4),
            rel_number: read_u32(self.body, self.cursor + 8),
        };
        self.cursor += REL_FILE_LOCATOR_SIZE;
        Ok(rel)
    }

    fn parse_payload_blocks(
        &mut self,
        pending_blocks: &[PendingBlock],
    ) -> Result<Vec<BlockRef>, WalDecodeError> {
        let mut blocks = Vec::with_capacity(pending_blocks.len());
        for pending_block in pending_blocks {
            let image = match pending_block.image_header {
                Some(image_header) => {
                    Some(self.consume_block_image(pending_block.id, image_header)?)
                }
                None => None,
            };
            let data = self.consume_block_data(pending_block.data_length)?;
            blocks.push(BlockRef {
                id: pending_block.id,
                fork: pending_block.fork,
                flags: BlockFlags {
                    raw: pending_block.flags,
                },
                rel: pending_block.rel,
                blkno: pending_block.blkno,
                image,
                data: data.into_boxed_slice(),
            });
        }
        Ok(blocks)
    }

    fn consume_block_image(
        &mut self,
        block_id: u8,
        header: BlockImageHeader,
    ) -> Result<BlockImage, WalDecodeError> {
        self.require_available(header.length, "block image payload")?;
        let image_bytes = &self.body[self.cursor..self.cursor + header.length];
        self.cursor += header.length;
        reconstruct_block_image(self.start_lsn, block_id, header, image_bytes)
    }

    fn consume_block_data(&mut self, length: usize) -> Result<Vec<u8>, WalDecodeError> {
        self.require_available(length, "block data payload")?;
        let data = self.body[self.cursor..self.cursor + length].to_vec();
        self.cursor += length;
        Ok(data)
    }

    fn consume_main_data(&mut self, length: usize) -> Result<Vec<u8>, WalDecodeError> {
        self.require_available(length, "main data payload")?;
        let data = self.body[self.cursor..self.cursor + length].to_vec();
        self.cursor += length;
        Ok(data)
    }

    fn peek_u8(&self) -> Option<u8> {
        self.body.get(self.cursor).copied()
    }

    fn has_reached_payload_boundary(
        &self,
        pending_payload_length: usize,
    ) -> Result<bool, WalDecodeError> {
        let remaining = self.body.len().saturating_sub(self.cursor);
        if remaining >= pending_payload_length {
            return Ok(remaining == pending_payload_length);
        }

        Err(WalDecodeError::RecordBodyTruncated {
            lsn: self.start_lsn,
            context: "block payload section",
            needed: pending_payload_length,
            got: remaining,
        })
    }

    fn require_available(
        &self,
        needed: usize,
        context: &'static str,
    ) -> Result<(), WalDecodeError> {
        let available = self.body.len().saturating_sub(self.cursor);
        if available >= needed {
            return Ok(());
        }

        Err(WalDecodeError::RecordBodyTruncated {
            lsn: self.start_lsn,
            context,
            needed,
            got: available,
        })
    }
}

fn reconstruct_block_image(
    start_lsn: Lsn,
    block_id: u8,
    header: BlockImageHeader,
    image_bytes: &[u8],
) -> Result<BlockImage, WalDecodeError> {
    if !header.has_hole() {
        if image_bytes.len() == crate::consts_v17::XLOG_BLCKSZ {
            return Ok(BlockImage {
                bytes: image_bytes.to_vec().into_boxed_slice(),
                hole_offset: None,
                apply: header.apply(),
            });
        }

        return Err(WalDecodeError::RecordBodyLengthMismatch {
            lsn: start_lsn,
            expected: crate::consts_v17::XLOG_BLCKSZ,
            got: image_bytes.len(),
        });
    }

    let hole_offset = usize::from(header.hole_offset);
    if hole_offset > image_bytes.len() || image_bytes.len() > crate::consts_v17::XLOG_BLCKSZ {
        return Err(WalDecodeError::InvalidFpiHole {
            lsn: start_lsn,
            block_id,
            hole_offset: header.hole_offset,
            image_length: image_bytes.len(),
        });
    }

    let hole_length = crate::consts_v17::XLOG_BLCKSZ - image_bytes.len();
    let mut reconstructed = Vec::with_capacity(crate::consts_v17::XLOG_BLCKSZ);
    reconstructed.extend_from_slice(&image_bytes[..hole_offset]);
    reconstructed.resize(hole_offset + hole_length, 0);
    reconstructed.extend_from_slice(&image_bytes[hole_offset..]);

    if reconstructed.len() == crate::consts_v17::XLOG_BLCKSZ {
        return Ok(BlockImage {
            bytes: reconstructed.into_boxed_slice(),
            hole_offset: Some(header.hole_offset),
            apply: header.apply(),
        });
    }

    Err(WalDecodeError::InvalidFpiHole {
        lsn: start_lsn,
        block_id,
        hole_offset: header.hole_offset,
        image_length: image_bytes.len(),
    })
}

/// Validates `PostgreSQL`'s WAL record CRC-32C recipe.
pub fn validate_record_crc(
    record: &[u8],
    header: XLogRecordHeader,
    start_lsn: Lsn,
) -> Result<(), WalDecodeError> {
    if record.len() < header.total_len as usize {
        return Err(WalDecodeError::TruncatedRecord {
            lsn: start_lsn,
            needed: header.total_len as usize,
            got: record.len(),
        });
    }

    let total_len = header.total_len as usize;
    let mut crc = crc32c::crc32c(&record[XLOG_RECORD_HEADER_SIZE..total_len]);
    crc = crc32c::crc32c_append(crc, &record[..CRC_OFFSET]);

    if crc == header.crc {
        return Ok(());
    }

    Err(WalDecodeError::BadCrc {
        lsn: start_lsn,
        expected: header.crc,
        got: crc,
    })
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ])
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    const TEST_LSN: Lsn = Lsn(0x0700_0000);

    #[test]
    fn multiblock_record_yields_all_block_refs() {
        let rel = RelFileLocator {
            spc_oid: 1663,
            db_oid: 5,
            rel_number: 12_345,
        };
        let mut body = Vec::new();
        push_block_header(&mut body, 0, 1 | BKPBLOCK_HAS_DATA, 3);
        push_rel(&mut body, rel);
        body.extend_from_slice(&7_u32.to_le_bytes());
        push_block_header(&mut body, 1, 2 | BKPBLOCK_HAS_DATA | BKPBLOCK_SAME_REL, 2);
        body.extend_from_slice(&8_u32.to_le_bytes());
        body.extend_from_slice(&[XLR_BLOCK_ID_DATA_SHORT, 4]);
        body.extend_from_slice(&[1, 2, 3]);
        body.extend_from_slice(&[4, 5]);
        body.extend_from_slice(&[9, 8, 7, 6]);

        let decoded = parse_decoded_record(&body).unwrap();

        assert!(decoded.blocks.len() == 2);
        assert!(decoded.blocks[0].id == 0);
        assert!(decoded.blocks[0].rel == rel);
        assert!(decoded.blocks[0].fork == 1);
        assert!(decoded.blocks[0].blkno == 7);
        assert!(decoded.blocks[0].data.as_ref() == [1, 2, 3]);
        assert!(decoded.blocks[1].id == 1);
        assert!(decoded.blocks[1].same_rel());
        assert!(decoded.blocks[1].rel == rel);
        assert!(decoded.blocks[1].fork == 2);
        assert!(decoded.blocks[1].blkno == 8);
        assert!(decoded.blocks[1].data.as_ref() == [4, 5]);
        assert!(decoded.main_data.as_ref() == [9, 8, 7, 6]);
    }

    #[test]
    fn fpi_hole_reconstructs_to_full_page() {
        let rel = RelFileLocator {
            spc_oid: 1663,
            db_oid: 5,
            rel_number: 99,
        };
        let hole_offset = 100_usize;
        let image_len = crate::consts_v17::XLOG_BLCKSZ - 16;
        let image_payload = vec![0xA5; image_len];
        let mut body = Vec::new();
        push_block_header(&mut body, 0, BKPBLOCK_HAS_IMAGE, 0);
        body.extend_from_slice(&test_u16(image_len).to_le_bytes());
        body.extend_from_slice(&test_u16(hole_offset).to_le_bytes());
        body.push(BKPIMAGE_HAS_HOLE | BKPIMAGE_APPLY);
        push_rel(&mut body, rel);
        body.extend_from_slice(&3_u32.to_le_bytes());
        body.extend_from_slice(&image_payload);

        let decoded = parse_decoded_record(&body).unwrap();
        let image = decoded.blocks[0].image.as_ref().unwrap();

        assert!(image.len() == crate::consts_v17::XLOG_BLCKSZ);
        assert!(image.apply);
        assert!(image.hole_offset == Some(test_u16(hole_offset)));
        assert!(
            image.as_ref()[hole_offset..hole_offset + 16]
                .iter()
                .all(|byte| *byte == 0)
        );
        assert!(image.as_ref()[hole_offset - 1] == 0xA5);
        assert!(image.as_ref()[hole_offset + 16] == 0xA5);
    }

    #[test]
    fn compressed_fpi_is_rejected_loudly() {
        let mut body = Vec::new();
        push_block_header(&mut body, 0, BKPBLOCK_HAS_IMAGE, 0);
        body.extend_from_slice(&16_u16.to_le_bytes());
        body.extend_from_slice(&0_u16.to_le_bytes());
        body.push(0x04);

        let decoded = parse_decoded_record(&body);

        assert!(let Err(WalDecodeError::CompressedFpiUnsupported { block_id, bimg_info, .. }) = decoded);
        assert!(block_id == 0);
        assert!(bimg_info == 0x04);
    }

    #[test]
    fn same_rel_folding_and_short_long_data_headers() {
        let rel = RelFileLocator {
            spc_oid: 1663,
            db_oid: 5,
            rel_number: 42,
        };

        for (data_header, expected_main_data) in [
            (vec![XLR_BLOCK_ID_DATA_SHORT, 3], vec![1, 2, 3]),
            (
                {
                    let mut header = vec![XLR_BLOCK_ID_DATA_LONG];
                    header.extend_from_slice(&3_u32.to_le_bytes());
                    header
                },
                vec![4, 5, 6],
            ),
        ] {
            let mut body = Vec::new();
            push_block_header(&mut body, 0, 0, 0);
            push_rel(&mut body, rel);
            body.extend_from_slice(&11_u32.to_le_bytes());
            push_block_header(&mut body, 1, BKPBLOCK_SAME_REL, 0);
            body.extend_from_slice(&12_u32.to_le_bytes());
            body.extend_from_slice(&data_header);
            body.extend_from_slice(&expected_main_data);

            let decoded = parse_decoded_record(&body).unwrap();

            assert!(decoded.blocks.len() == 2);
            assert!(decoded.blocks[1].same_rel());
            assert!(decoded.blocks[1].rel == rel);
            assert!(decoded.main_data.as_ref() == expected_main_data.as_slice());
        }
    }

    #[test]
    fn main_data_only_record_decodes_without_blocks() {
        let body = [XLR_BLOCK_ID_DATA_SHORT, 3, 7, 8, 9];

        let decoded = parse_decoded_record(&body).unwrap();

        assert!(decoded.blocks.is_empty());
        assert!(decoded.main_data.as_ref() == [7, 8, 9]);
    }

    fn parse_decoded_record(body: &[u8]) -> Result<DecodedRecord, WalDecodeError> {
        let record = XLogRecord::parse(build_record(body), TEST_LSN)?;
        record.decode()
    }

    fn build_record(body: &[u8]) -> Vec<u8> {
        let total_len = XLOG_RECORD_HEADER_SIZE + body.len();
        let mut record = vec![0; total_len];
        record[..4].copy_from_slice(&test_u32(total_len).to_le_bytes());
        record[4..8].copy_from_slice(&123_u32.to_le_bytes());
        record[16] = 0x20;
        record[17] = 0x00;
        record[XLOG_RECORD_HEADER_SIZE..].copy_from_slice(body);

        let mut crc = crc32c::crc32c(&record[XLOG_RECORD_HEADER_SIZE..]);
        crc = crc32c::crc32c_append(crc, &record[..CRC_OFFSET]);
        record[CRC_OFFSET..XLOG_RECORD_HEADER_SIZE].copy_from_slice(&crc.to_le_bytes());
        record
    }

    fn push_block_header(body: &mut Vec<u8>, id: u8, flags: u8, data_length: u16) {
        body.push(id);
        body.push(flags);
        body.extend_from_slice(&data_length.to_le_bytes());
    }

    fn push_rel(body: &mut Vec<u8>, rel: RelFileLocator) {
        body.extend_from_slice(&rel.spc_oid.to_le_bytes());
        body.extend_from_slice(&rel.db_oid.to_le_bytes());
        body.extend_from_slice(&rel.rel_number.to_le_bytes());
    }

    fn test_u16(value: usize) -> u16 {
        u16::try_from(value).expect("test value fits in u16")
    }

    fn test_u32(value: usize) -> u32 {
        u32::try_from(value).expect("test value fits in u32")
    }
}
