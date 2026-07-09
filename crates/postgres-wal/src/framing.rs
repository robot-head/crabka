//! WAL page framing and header parsing.

use std::num::NonZeroU64;

use thiserror::Error;

use crate::{
    Lsn, consts_v17,
    record::{XLOG_RECORD_HEADER_SIZE, XLogRecord, XLogRecordHeader},
};

const RM_XLOG_ID: u8 = 0;
const XLR_INFO_MASK: u8 = 0x0F;
const XLOG_SWITCH: u8 = 0x40;

/// Error returned when WAL framing bytes cannot be parsed as `PostgreSQL` 17 WAL.
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum WalDecodeError {
    /// Bytes were fed out of LSN order.
    #[error("WAL feed LSN mismatch: got {got}, expected {expected}")]
    FeedLsnMismatch {
        /// LSN supplied to `feed`.
        got: Lsn,
        /// Next LSN expected by the decoder.
        expected: Lsn,
    },

    /// The page did not contain enough bytes for the advertised header.
    #[error("WAL page at {lsn} is truncated: needed {needed} bytes, got {got}")]
    TruncatedPage {
        /// LSN of the page being parsed.
        lsn: Lsn,
        /// Number of bytes needed to finish parsing the header.
        needed: usize,
        /// Number of bytes available.
        got: usize,
    },

    /// The page magic did not match `PostgreSQL` 17.
    #[error("bad PostgreSQL WAL page magic at {lsn}: got 0x{got:04X}, expected 0x{expected:04X}")]
    BadMagic {
        /// LSN of the page being parsed.
        lsn: Lsn,
        /// Magic read from the input bytes.
        got: u16,
        /// Expected `PostgreSQL` 17 magic.
        expected: u16,
    },

    /// The page header address did not match the LSN supplied by the caller.
    #[error("WAL page address mismatch: header says {got}, caller supplied {expected}")]
    PageAddressMismatch {
        /// Page address read from the header.
        got: Lsn,
        /// LSN supplied by the caller.
        expected: Lsn,
    },

    /// A page advertised a continuation record when no previous fragment exists.
    #[error("unexpected WAL continuation record at {lsn}")]
    UnexpectedContinuation {
        /// LSN of the page with the unexpected continuation flag.
        lsn: Lsn,
    },

    /// A previous page ended with a partial record, but the next page did not continue it.
    #[error("missing WAL continuation record at {lsn}")]
    MissingContinuation {
        /// LSN of the page that should have continued the previous record.
        lsn: Lsn,
    },

    /// The fixed record header is truncated.
    #[error("WAL record header at {lsn} is truncated: needed {needed} bytes, got {got}")]
    TruncatedRecordHeader {
        /// LSN of the record being parsed.
        lsn: Lsn,
        /// Number of bytes needed to parse the header.
        needed: usize,
        /// Number of bytes available.
        got: usize,
    },

    /// A record length did not include the fixed header.
    #[error("WAL record at {lsn} is too short: total length {total_len}, minimum {minimum}")]
    RecordTooShort {
        /// LSN of the record being parsed.
        lsn: Lsn,
        /// Length advertised in the record header.
        total_len: u32,
        /// Minimum valid WAL record length.
        minimum: u32,
    },

    /// A record body ended before the advertised total length.
    #[error("WAL record at {lsn} is truncated: needed {needed} bytes, got {got}")]
    TruncatedRecord {
        /// LSN of the record being parsed.
        lsn: Lsn,
        /// Number of bytes needed to finish the record.
        needed: usize,
        /// Number of bytes available.
        got: usize,
    },

    /// A record CRC-32C did not match `PostgreSQL`'s WAL checksum recipe.
    #[error("bad WAL record CRC at {lsn}: got 0x{got:08X}, expected 0x{expected:08X}")]
    BadCrc {
        /// LSN of the record being parsed.
        lsn: Lsn,
        /// CRC stored in the record header.
        expected: u32,
        /// CRC computed by the decoder.
        got: u32,
    },

    /// A continuation page advertised a remaining length that disagreed with the buffered record.
    #[error(
        "WAL continuation length mismatch at {lsn}: got {got} remaining bytes, expected {expected}"
    )]
    ContinuationLengthMismatch {
        /// LSN of the continuation page.
        lsn: Lsn,
        /// Remaining length advertised by the page header.
        got: usize,
        /// Remaining record bytes expected by the decoder.
        expected: usize,
    },

    /// A record body header or payload ended before the bounded grammar could be parsed.
    #[error(
        "WAL record body at {lsn} is truncated while parsing {context}: needed {needed} bytes, got {got}"
    )]
    RecordBodyTruncated {
        /// LSN of the record being parsed.
        lsn: Lsn,
        /// Grammar element being parsed.
        context: &'static str,
        /// Number of bytes needed for this element.
        needed: usize,
        /// Number of bytes available at the current cursor.
        got: usize,
    },

    /// A record body did not match the bounded PG 17 grammar.
    #[error("WAL record body grammar error at {lsn}: {context}")]
    RecordBodyGrammar {
        /// LSN of the record being parsed.
        lsn: Lsn,
        /// Description of the grammar violation.
        context: &'static str,
    },

    /// A record body parser did not consume exactly the advertised body length.
    #[error("WAL record body at {lsn} consumed {expected} bytes, but record body has {got} bytes")]
    RecordBodyLengthMismatch {
        /// LSN of the record being parsed.
        lsn: Lsn,
        /// Expected or consumed byte count.
        expected: usize,
        /// Actual byte count.
        got: usize,
    },

    /// Compressed full-page images are intentionally not decoded yet.
    #[error(
        "compressed WAL full-page image is unsupported at {lsn}, block {block_id}, bimg_info 0x{bimg_info:02X}"
    )]
    CompressedFpiUnsupported {
        /// LSN of the record being parsed.
        lsn: Lsn,
        /// Block reference ID with the compressed image.
        block_id: u8,
        /// Raw block image info byte.
        bimg_info: u8,
    },

    /// An uncompressed full-page image hole descriptor could not reconstruct an 8 KiB page.
    #[error(
        "invalid WAL full-page image hole at {lsn}, block {block_id}: hole offset {hole_offset}, stored image length {image_length}"
    )]
    InvalidFpiHole {
        /// LSN of the record being parsed.
        lsn: Lsn,
        /// Block reference ID with the invalid image.
        block_id: u8,
        /// Hole offset encoded in the block image header.
        hole_offset: u16,
        /// Stored image byte length.
        image_length: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PartialRecord {
    start_lsn: Lsn,
    total_len: Option<usize>,
    bytes: Vec<u8>,
}

/// Pull-based, sans-IO WAL stream decoder.
#[derive(Debug, Default)]
pub struct WalStreamDecoder {
    buffer_lsn: Option<Lsn>,
    bytes: Vec<u8>,
    cursor: usize,
    partial_record: Option<PartialRecord>,
    wal_segsize: Option<NonZeroU64>,
}

impl WalStreamDecoder {
    /// Creates an empty decoder whose first feed is expected at `base_lsn`.
    #[must_use]
    pub fn new(base_lsn: Lsn) -> Self {
        Self {
            buffer_lsn: Some(base_lsn),
            bytes: Vec::new(),
            cursor: 0,
            partial_record: None,
            wal_segsize: None,
        }
    }

    /// Appends consecutive WAL bytes at `lsn`.
    pub fn feed(&mut self, lsn: Lsn, bytes: &[u8]) -> Result<(), WalDecodeError> {
        if bytes.is_empty() {
            return Ok(());
        }

        let expected = self.next_feed_lsn();
        if lsn != expected {
            return Err(WalDecodeError::FeedLsnMismatch { got: lsn, expected });
        }

        let base_lsn = self.buffer_lsn.unwrap_or(Lsn(0));
        let expected_offset = usize::try_from(expected.0 - base_lsn.0)
            .expect("expected WAL feed offset fits in usize");
        if expected_offset > self.bytes.len() {
            self.bytes.resize(expected_offset, 0);
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    /// Returns the next complete WAL record, or `Ok(None)` for an incomplete tail.
    pub fn poll_record(&mut self) -> Result<Option<XLogRecord>, WalDecodeError> {
        loop {
            if !self.has_complete_page_at_cursor() {
                return Ok(None);
            }

            let page_start = self.current_page_start();
            let page_lsn = self.lsn_at(page_start);
            let page_end = page_start + consts_v17::XLOG_BLCKSZ;
            let header = parse_page_header(&self.bytes[page_start..page_end], page_lsn)?;
            self.observe_page_header(header);
            let standard_header = header.standard();
            let page_contains_continuation =
                standard_header.info & consts_v17::XLP_FIRST_IS_CONTRECORD != 0;
            let cursor_is_at_first_page_record = self.cursor <= page_start + header.size();

            if page_contains_continuation && cursor_is_at_first_page_record {
                let record = self.consume_continuation_page(header, page_start, page_end)?;
                if record.is_some() {
                    return Ok(record);
                }
                continue;
            }

            if self.partial_record.is_some() {
                return Err(WalDecodeError::MissingContinuation { lsn: page_lsn });
            }

            let record = self.consume_regular_page(header, page_start, page_end)?;
            if record.is_some() {
                return Ok(record);
            }
        }
    }

    fn consume_continuation_page(
        &mut self,
        header: PageHeader,
        page_start: usize,
        page_end: usize,
    ) -> Result<Option<XLogRecord>, WalDecodeError> {
        let page_lsn = self.lsn_at(page_start);
        let fragment_start = page_start + header.size();
        let advertised_remaining = header.standard().rem_len as usize;
        let Some(mut partial_record) = self.partial_record.take() else {
            if page_start == 0 && matches!(header, PageHeader::Long { .. }) {
                self.cursor = maxalign(fragment_start + advertised_remaining).min(page_end);
                return Ok(None);
            }

            return Err(WalDecodeError::UnexpectedContinuation { lsn: page_lsn });
        };

        let remaining_record_bytes = if let Some(total_len) = partial_record.total_len {
            let remaining_record_bytes = total_len.saturating_sub(partial_record.bytes.len());
            if advertised_remaining != remaining_record_bytes {
                return Err(WalDecodeError::ContinuationLengthMismatch {
                    lsn: page_lsn,
                    got: advertised_remaining,
                    expected: remaining_record_bytes,
                });
            }

            remaining_record_bytes
        } else {
            let total_len = partial_record
                .bytes
                .len()
                .checked_add(advertised_remaining)
                .expect("partial WAL record length must fit in usize");
            partial_record.total_len = Some(total_len);
            advertised_remaining
        };

        let bytes_to_copy = remaining_record_bytes
            .min(advertised_remaining)
            .min(page_end - fragment_start);
        partial_record
            .bytes
            .extend_from_slice(&self.bytes[fragment_start..fragment_start + bytes_to_copy]);
        self.cursor = fragment_start + bytes_to_copy;

        let total_len = partial_record
            .total_len
            .expect("continued WAL record total length is known");
        if partial_record.bytes.len() < total_len {
            self.cursor = page_end;
            self.partial_record = Some(partial_record);
            return Ok(None);
        }

        let start_lsn = partial_record.start_lsn;
        let aligned_cursor = maxalign(self.cursor);
        let record = XLogRecord::parse(partial_record.bytes, start_lsn)?;
        self.cursor = aligned_cursor.min(page_end);
        self.skip_rest_of_segment_after_switch(&record);
        Ok(Some(record))
    }

    fn consume_regular_page(
        &mut self,
        header: PageHeader,
        page_start: usize,
        page_end: usize,
    ) -> Result<Option<XLogRecord>, WalDecodeError> {
        let record_start = self.cursor.max(page_start + header.size());

        if record_start >= page_end {
            self.cursor = page_end;
            return Ok(None);
        }

        let remaining_page = page_end - record_start;
        if remaining_page < XLOG_RECORD_HEADER_SIZE {
            if self.bytes[record_start..page_end]
                .iter()
                .all(|byte| *byte == 0)
            {
                self.cursor = page_end;
                return Ok(None);
            }

            self.partial_record = Some(PartialRecord {
                start_lsn: self.lsn_at(record_start),
                total_len: None,
                bytes: self.bytes[record_start..page_end].to_vec(),
            });
            self.cursor = page_end;
            return Ok(None);
        }

        if self.bytes[record_start..record_start + XLOG_RECORD_HEADER_SIZE]
            .iter()
            .all(|byte| *byte == 0)
        {
            self.cursor = page_end;
            return Ok(None);
        }

        let start_lsn = self.lsn_at(record_start);
        let header = XLogRecordHeader::parse(
            &self.bytes[record_start..record_start + XLOG_RECORD_HEADER_SIZE],
            start_lsn,
        )?;
        let total_len = header.total_len as usize;

        if total_len <= remaining_page {
            let record_end = record_start + total_len;
            let record_bytes = self.bytes[record_start..record_end].to_vec();
            self.cursor = maxalign(record_end).min(page_end);
            let record = XLogRecord::parse(record_bytes, start_lsn)?;
            self.skip_rest_of_segment_after_switch(&record);
            return Ok(Some(record));
        }

        let mut bytes = Vec::with_capacity(total_len);
        bytes.extend_from_slice(&self.bytes[record_start..page_end]);
        self.partial_record = Some(PartialRecord {
            start_lsn,
            total_len: Some(total_len),
            bytes,
        });
        self.cursor = page_end;
        Ok(None)
    }

    fn has_complete_page_at_cursor(&self) -> bool {
        self.current_page_start() + consts_v17::XLOG_BLCKSZ <= self.bytes.len()
    }

    fn current_page_start(&self) -> usize {
        self.cursor - (self.cursor % consts_v17::XLOG_BLCKSZ)
    }

    fn lsn_at(&self, offset: usize) -> Lsn {
        let base_lsn = self.buffer_lsn.unwrap_or(Lsn(0));
        Lsn(base_lsn.0 + offset as u64)
    }

    fn next_feed_lsn(&self) -> Lsn {
        let base_lsn = self.buffer_lsn.unwrap_or(Lsn(0));
        Lsn(base_lsn.0 + self.bytes.len().max(self.cursor) as u64)
    }

    fn observe_page_header(&mut self, header: PageHeader) {
        let PageHeader::Long { seg_size, .. } = header else {
            return;
        };

        self.wal_segsize = NonZeroU64::new(u64::from(seg_size));
    }

    fn skip_rest_of_segment_after_switch(&mut self, record: &XLogRecord) {
        if !is_xlog_switch_record(record) {
            return;
        }

        let Some(wal_segsize) = self.wal_segsize else {
            return;
        };
        let base_lsn = self.buffer_lsn.unwrap_or(Lsn(0)).0;
        let next_segment_lsn = next_segment_start_lsn(record.start_lsn.0, wal_segsize);
        if next_segment_lsn <= base_lsn {
            return;
        }

        let next_segment_offset = usize::try_from(next_segment_lsn - base_lsn)
            .expect("next WAL segment offset fits in usize");
        self.cursor = self.cursor.max(next_segment_offset);
    }
}

fn is_xlog_switch_record(record: &XLogRecord) -> bool {
    record.header.rmid == RM_XLOG_ID && record.header.info & !XLR_INFO_MASK == XLOG_SWITCH
}

fn next_segment_start_lsn(lsn: u64, wal_segsize: NonZeroU64) -> u64 {
    let wal_segsize = wal_segsize.get();
    let current_segment = lsn / wal_segsize;
    current_segment
        .checked_add(1)
        .and_then(|segment| segment.checked_mul(wal_segsize))
        .expect("next WAL segment LSN fits in u64")
}

/// The fixed portion shared by short and long `PostgreSQL` WAL page headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StandardPageHeader {
    /// Page magic number.
    pub magic: u16,
    /// Page flags.
    pub info: u16,
    /// Timeline ID.
    pub timeline_id: u32,
    /// Address of this WAL page.
    pub pageaddr: Lsn,
    /// Remaining bytes for a continuation record that starts on this page.
    pub rem_len: u32,
}

/// A parsed `PostgreSQL` WAL page header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageHeader {
    /// Short page header used by regular pages after the first page in a
    /// segment.
    Short {
        /// Shared header fields.
        std: StandardPageHeader,
    },
    /// Long page header used at segment starts.
    Long {
        /// Shared header fields.
        std: StandardPageHeader,
        /// System identifier written into the WAL segment.
        sysid: u64,
        /// WAL segment size in bytes.
        seg_size: u32,
        /// WAL page size in bytes.
        xlog_blcksz: u32,
    },
}

impl PageHeader {
    /// Returns the number of bytes occupied by this page header.
    #[must_use]
    pub const fn size(self) -> usize {
        match self {
            Self::Short { .. } => consts_v17::SIZE_OF_SHORT_PHD,
            Self::Long { .. } => consts_v17::SIZE_OF_LONG_PHD,
        }
    }

    /// Returns the fixed fields shared by long and short headers.
    #[must_use]
    pub const fn standard(self) -> StandardPageHeader {
        match self {
            Self::Short { std } | Self::Long { std, .. } => std,
        }
    }
}

/// Parses the WAL page header at `page_lsn`.
///
/// `PostgreSQL` WAL is native-endian. Crabka currently accepts the little-endian
/// `PostgreSQL` 17 fixture corpus used by its tests.
pub fn parse_page_header(page: &[u8], page_lsn: Lsn) -> Result<PageHeader, WalDecodeError> {
    require_len(page, consts_v17::SIZE_OF_SHORT_PHD, page_lsn)?;

    let std = StandardPageHeader {
        magic: read_u16(page, 0),
        info: read_u16(page, 2),
        timeline_id: read_u32(page, 4),
        pageaddr: Lsn(read_u64(page, 8)),
        rem_len: read_u32(page, 16),
    };

    if std.magic != consts_v17::XLOG_PAGE_MAGIC {
        return Err(WalDecodeError::BadMagic {
            lsn: page_lsn,
            got: std.magic,
            expected: consts_v17::XLOG_PAGE_MAGIC,
        });
    }

    if std.pageaddr != page_lsn {
        return Err(WalDecodeError::PageAddressMismatch {
            got: std.pageaddr,
            expected: page_lsn,
        });
    }

    if std.info & consts_v17::XLP_LONG_HEADER == 0 {
        return Ok(PageHeader::Short { std });
    }

    require_len(page, consts_v17::SIZE_OF_LONG_PHD, page_lsn)?;

    Ok(PageHeader::Long {
        std,
        sysid: read_u64(page, 24),
        seg_size: read_u32(page, 32),
        xlog_blcksz: read_u32(page, 36),
    })
}

/// Parses every complete WAL page header in `bytes`, starting at `base_lsn`.
pub fn parse_page_headers(bytes: &[u8], base_lsn: Lsn) -> Result<Vec<PageHeader>, WalDecodeError> {
    let mut headers = Vec::with_capacity(bytes.len() / consts_v17::XLOG_BLCKSZ);
    for (page_index, page) in bytes.chunks(consts_v17::XLOG_BLCKSZ).enumerate() {
        let page_lsn = Lsn(base_lsn.0 + page_index as u64 * consts_v17::XLOG_BLCKSZ as u64);
        if page.len() != consts_v17::XLOG_BLCKSZ {
            return Err(WalDecodeError::TruncatedPage {
                lsn: page_lsn,
                needed: consts_v17::XLOG_BLCKSZ,
                got: page.len(),
            });
        }

        let header = parse_page_header(page, page_lsn)?;
        headers.push(header);
    }
    Ok(headers)
}

fn require_len(page: &[u8], needed: usize, lsn: Lsn) -> Result<(), WalDecodeError> {
    if page.len() >= needed {
        return Ok(());
    }

    Err(WalDecodeError::TruncatedPage {
        lsn,
        needed,
        got: page.len(),
    })
}

fn maxalign(offset: usize) -> usize {
    const ALIGNMENT: usize = 8;
    (offset + ALIGNMENT - 1) & !(ALIGNMENT - 1)
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ])
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
    use std::path::{Path, PathBuf};

    use assert2::assert;

    use super::*;
    use crate::consts_v17::{XLP_FIRST_IS_CONTRECORD, XLP_LONG_HEADER};

    #[test]
    fn long_header_parses_at_segment_start() {
        let (fixture_base_lsn, fixture_segment) = fixture_segment();
        let header = parse_page_header(&fixture_segment, fixture_base_lsn);

        assert!(let Ok(PageHeader::Long {
            std,
            sysid: _,
            seg_size,
            xlog_blcksz,
        }) = header);
        assert!(std.info & XLP_LONG_HEADER == XLP_LONG_HEADER);
        assert!(std.rem_len == 0 || std.info & XLP_FIRST_IS_CONTRECORD != 0);
        assert!(seg_size == 1024 * 1024);
        assert!(xlog_blcksz == 8192);
    }

    #[test]
    fn short_header_on_second_page_matches_page_address() {
        let (fixture_base_lsn, fixture_segment) = fixture_segment();
        let second_page_lsn = Lsn(fixture_base_lsn.0 + consts_v17::XLOG_BLCKSZ as u64);
        let second_page = &fixture_segment[consts_v17::XLOG_BLCKSZ..];
        let header = parse_page_header(second_page, second_page_lsn);

        assert!(let Ok(PageHeader::Short { std }) = header);
        assert!(std.pageaddr == second_page_lsn);
        assert!(std.info & XLP_LONG_HEADER == 0);
    }

    #[test]
    fn wrong_magic_is_versioned_error() {
        let (fixture_base_lsn, mut segment) = fixture_segment();
        segment[0] ^= 0xFF;

        let parsed = parse_page_header(&segment, fixture_base_lsn);

        assert!(let Err(WalDecodeError::BadMagic { got: _, expected, .. }) = parsed);
        assert!(expected == consts_v17::XLOG_PAGE_MAGIC);
    }

    #[test]
    fn truncated_page_is_reported_without_panic() {
        let (fixture_base_lsn, fixture_segment) = fixture_segment();
        let parsed = parse_page_header(&fixture_segment[..8], fixture_base_lsn);

        assert!(let Err(WalDecodeError::TruncatedPage { needed, got, .. }) = parsed);
        assert!(needed == consts_v17::SIZE_OF_SHORT_PHD);
        assert!(got == 8);
    }

    #[test]
    fn mismatched_page_address_is_reported() {
        let (fixture_base_lsn, fixture_segment) = fixture_segment();
        let parsed = parse_page_header(&fixture_segment, Lsn(fixture_base_lsn.0 + 1));

        assert!(let Err(WalDecodeError::PageAddressMismatch { got, expected }) = parsed);
        assert!(got == fixture_base_lsn);
        assert!(expected == Lsn(fixture_base_lsn.0 + 1));
    }

    #[test]
    fn fixture_segment_frames_as_pages() {
        let (fixture_base_lsn, fixture_segment) = fully_framed_fixture_segment();
        let headers = parse_page_headers(&fixture_segment, fixture_base_lsn);

        assert!(let Ok(headers) = headers);
        assert!(headers.len() == 128);
        assert!(let Some(PageHeader::Long { .. }) = headers.first());
        assert!(
            headers
                .iter()
                .skip(1)
                .all(|header| matches!(header, PageHeader::Short { .. }))
        );
    }

    #[test]
    fn xlog_switch_skips_zero_filled_segment_tail() {
        let first_segment_lsn = Lsn(0x0600_0000);
        let next_segment_lsn = Lsn(first_segment_lsn.0 + 1024 * 1024);
        let switch_record = build_record(0, XLOG_SWITCH, RM_XLOG_ID, &[]);
        let next_record = build_record(13, 0x10, 0x11, &[1, 2, 3, 4]);
        let mut bytes = build_page(
            first_segment_lsn,
            consts_v17::XLP_LONG_HEADER,
            0,
            &switch_record,
        );
        bytes.resize(1024 * 1024, 0);
        bytes.extend_from_slice(&build_page(
            next_segment_lsn,
            consts_v17::XLP_LONG_HEADER,
            0,
            &next_record,
        ));
        let mut decoder = WalStreamDecoder::new(first_segment_lsn);

        decoder.feed(first_segment_lsn, &bytes).unwrap();
        let decoded_switch = decoder.poll_record().unwrap().unwrap();
        let decoded_next_record = decoder.poll_record().unwrap().unwrap();

        assert!(
            decoded_switch.start_lsn
                == Lsn(first_segment_lsn.0 + consts_v17::SIZE_OF_LONG_PHD as u64)
        );
        assert!(decoded_switch.header.rmid == RM_XLOG_ID);
        assert!(decoded_switch.header.info == XLOG_SWITCH);
        assert!(
            decoded_next_record.start_lsn
                == Lsn(next_segment_lsn.0 + consts_v17::SIZE_OF_LONG_PHD as u64)
        );
        assert!(decoded_next_record.header.xid == 13);
    }

    #[test]
    fn xlog_switch_accepts_next_segment_without_padding_bytes() {
        let first_segment_lsn = Lsn(0x0600_0000);
        let next_segment_lsn = Lsn(first_segment_lsn.0 + 1024 * 1024);
        let switch_record = build_record(0, XLOG_SWITCH, RM_XLOG_ID, &[]);
        let next_record = build_record(14, 0x10, 0x11, &[5, 6, 7, 8]);
        let first_page = build_page(
            first_segment_lsn,
            consts_v17::XLP_LONG_HEADER,
            0,
            &switch_record,
        );
        let next_page = build_page(
            next_segment_lsn,
            consts_v17::XLP_LONG_HEADER,
            0,
            &next_record,
        );
        let mut decoder = WalStreamDecoder::new(first_segment_lsn);

        decoder.feed(first_segment_lsn, &first_page).unwrap();
        let decoded_switch = decoder.poll_record().unwrap().unwrap();
        decoder.feed(next_segment_lsn, &next_page).unwrap();
        let decoded_next_record = decoder.poll_record().unwrap().unwrap();

        assert!(decoded_switch.header.rmid == RM_XLOG_ID);
        assert!(decoded_switch.header.info == XLOG_SWITCH);
        assert!(
            decoded_next_record.start_lsn
                == Lsn(next_segment_lsn.0 + consts_v17::SIZE_OF_LONG_PHD as u64)
        );
        assert!(decoded_next_record.header.xid == 14);
    }

    #[test]
    fn fixture_segment_contains_nonempty_real_record_corpus() {
        let (fixture_base_lsn, fixture_segment) = fixture_segment();
        let mut decoder = WalStreamDecoder::new(fixture_base_lsn);

        decoder.feed(fixture_base_lsn, &fixture_segment).unwrap();
        let first = decoder.poll_record().unwrap().unwrap().decode().unwrap();
        let second = decoder.poll_record().unwrap().unwrap().decode().unwrap();

        assert!(first.start_lsn >= fixture_base_lsn);
        assert!(first.total_len >= test_u32(XLOG_RECORD_HEADER_SIZE));
        assert!(second.start_lsn > first.start_lsn);
        assert!(second.total_len >= test_u32(XLOG_RECORD_HEADER_SIZE));
        assert!(!first.blocks.is_empty() || !second.blocks.is_empty());
    }

    #[test]
    fn truncated_segment_tail_is_reported() {
        let (fixture_base_lsn, fixture_segment) = fixture_segment();
        let truncated_len = consts_v17::XLOG_BLCKSZ + consts_v17::SIZE_OF_SHORT_PHD;
        let headers = parse_page_headers(&fixture_segment[..truncated_len], fixture_base_lsn);

        assert!(let Err(WalDecodeError::TruncatedPage { needed, got, .. }) = headers);
        assert!(needed == consts_v17::XLOG_BLCKSZ);
        assert!(got == consts_v17::SIZE_OF_SHORT_PHD);
    }

    #[test]
    fn first_record_decodes_with_typed_header_and_len() {
        let base_lsn = Lsn(0x0200_0000);
        let record = build_record(7, 0x10, 0x11, &[1, 2, 3, 4, 5]);
        let record_start_lsn = Lsn(base_lsn.0 + consts_v17::SIZE_OF_LONG_PHD as u64);
        let page = build_page(base_lsn, consts_v17::XLP_LONG_HEADER, 0, &record);
        let mut decoder = WalStreamDecoder::new(base_lsn);

        decoder.feed(base_lsn, &page).unwrap();
        let decoded_record = decoder.poll_record().unwrap().unwrap();

        assert!(decoded_record.start_lsn == record_start_lsn);
        assert!(decoded_record.total_len == test_u32(record.len()));
        assert!(decoded_record.header.xid == 7);
        assert!(decoded_record.header.info == 0x10);
        assert!(decoded_record.header.rmid == 0x11);
        assert!(decoded_record.bytes.as_ref() == record.as_slice());
    }

    #[test]
    fn contrecord_across_pages_reassembles() {
        let base_lsn = Lsn(0x0300_0000);
        let payload = vec![0xAB; consts_v17::XLOG_BLCKSZ + 128];
        let record = build_record(8, 0x20, 0x12, &payload);
        let first_fragment_len = consts_v17::XLOG_BLCKSZ - consts_v17::SIZE_OF_LONG_PHD;
        let second_lsn = Lsn(base_lsn.0 + consts_v17::XLOG_BLCKSZ as u64);
        let first_page = build_page(
            base_lsn,
            consts_v17::XLP_LONG_HEADER,
            0,
            &record[..first_fragment_len],
        );
        let second_page = build_page(
            second_lsn,
            consts_v17::XLP_FIRST_IS_CONTRECORD,
            test_u32(record.len() - first_fragment_len),
            &record[first_fragment_len..],
        );
        let mut decoder = WalStreamDecoder::new(base_lsn);

        decoder.feed(base_lsn, &first_page).unwrap();
        assert!(decoder.poll_record().unwrap().is_none());
        decoder.feed(second_lsn, &second_page).unwrap();
        let decoded_record = decoder.poll_record().unwrap().unwrap();

        assert!(decoded_record.start_lsn == Lsn(base_lsn.0 + consts_v17::SIZE_OF_LONG_PHD as u64));
        assert!(decoded_record.total_len == test_u32(record.len()));
        assert!(decoded_record.bytes.as_ref() == record.as_slice());
    }

    #[test]
    fn corrupt_byte_fails_crc_with_lsn() {
        let base_lsn = Lsn(0x0400_0000);
        let mut record = build_record(9, 0x30, 0x13, &[9, 8, 7, 6]);
        let last_byte = record.len() - 1;
        record[last_byte] ^= 0xFF;
        let page = build_page(base_lsn, consts_v17::XLP_LONG_HEADER, 0, &record);
        let mut decoder = WalStreamDecoder::new(base_lsn);

        decoder.feed(base_lsn, &page).unwrap();
        let decode_result = decoder.poll_record();

        assert!(let Err(WalDecodeError::BadCrc { lsn, .. }) = decode_result);
        assert!(lsn == Lsn(base_lsn.0 + consts_v17::SIZE_OF_LONG_PHD as u64));
    }

    #[test]
    fn multiple_records_on_one_page_are_polled_in_order() {
        let base_lsn = Lsn(0x0450_0000);
        let first_record = build_record(11, 0x31, 0x15, &[1, 2, 3]);
        let second_record = build_record(12, 0x32, 0x16, &[4, 5, 6]);
        let mut page_payload = first_record.clone();
        page_payload.resize(maxalign(page_payload.len()), 0);
        let second_record_start = consts_v17::SIZE_OF_LONG_PHD + page_payload.len();
        page_payload.extend_from_slice(&second_record);
        let page = build_page(base_lsn, consts_v17::XLP_LONG_HEADER, 0, &page_payload);
        let mut decoder = WalStreamDecoder::new(base_lsn);

        decoder.feed(base_lsn, &page).unwrap();
        let decoded_first_record = decoder.poll_record().unwrap().unwrap();
        let decoded_second_record = decoder.poll_record().unwrap().unwrap();

        assert!(decoded_first_record.header.xid == 11);
        assert!(decoded_second_record.header.xid == 12);
        assert!(decoded_second_record.start_lsn == Lsn(base_lsn.0 + second_record_start as u64));
    }

    #[test]
    fn truncated_tail_is_incomplete_not_panic() {
        let base_lsn = Lsn(0x0500_0000);
        let record = build_record(10, 0x40, 0x14, &[1; 128]);
        let page = build_page(base_lsn, consts_v17::XLP_LONG_HEADER, 0, &record);
        let tail_len = consts_v17::SIZE_OF_LONG_PHD + record.len() / 2;
        let mut decoder = WalStreamDecoder::new(base_lsn);

        decoder.feed(base_lsn, &page[..tail_len]).unwrap();

        assert!(decoder.poll_record().unwrap().is_none());
    }

    #[test]
    fn unexpected_contrecord_is_a_framing_error() {
        let base_lsn = Lsn(0x0600_0000);
        let page = build_page(base_lsn, consts_v17::XLP_FIRST_IS_CONTRECORD, 16, &[1; 16]);
        let mut decoder = WalStreamDecoder::new(base_lsn);

        decoder.feed(base_lsn, &page).unwrap();
        let decode_result = decoder.poll_record();

        assert!(let Err(WalDecodeError::UnexpectedContinuation { lsn }) = decode_result);
        assert!(lsn == base_lsn);
    }

    #[derive(Debug, Eq, PartialEq)]
    struct FixtureSegment {
        path: String,
        base_lsn: Lsn,
    }

    fn fixture_segment() -> (Lsn, Vec<u8>) {
        let segment = first_fixture_segment();
        let bytes = std::fs::read(fixture_path(&segment.path)).expect("fixture segment reads");

        (segment.base_lsn, bytes)
    }

    fn fully_framed_fixture_segment() -> (Lsn, Vec<u8>) {
        for segment in fixture_segments() {
            let bytes = std::fs::read(fixture_path(&segment.path)).expect("fixture segment reads");
            if parse_page_headers(&bytes, segment.base_lsn).is_ok() {
                return (segment.base_lsn, bytes);
            }
        }

        panic!("fixture manifest must list at least one fully framed WAL segment");
    }

    fn first_fixture_segment() -> FixtureSegment {
        fixture_segments()
            .into_iter()
            .next()
            .expect("fixture manifest lists at least one WAL segment")
    }

    fn fixture_segments() -> Vec<FixtureSegment> {
        let table = include_str!("../tests/fixtures/manifest.toml")
            .parse::<toml::Table>()
            .expect("fixture manifest is valid TOML");
        let wal_segsize = parse_wal_segsize(toml_string(&table, "wal_segsize"));
        let Some(files) = table.get("files").and_then(toml::Value::as_array) else {
            panic!("fixture manifest lists files");
        };
        let mut segments = files
            .iter()
            .filter_map(|entry| {
                let table = entry.as_table()?;
                let path = toml_string(table, "path").to_owned();
                is_wal_manifest_path(&path).then(|| FixtureSegment {
                    base_lsn: segment_base_lsn_from_filename(&path, wal_segsize),
                    path,
                })
            })
            .collect::<Vec<_>>();
        segments.sort_by_key(|segment| segment.base_lsn);
        segments
    }

    fn fixture_path(relative_path: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(relative_path)
    }

    fn toml_string<'a>(table: &'a toml::Table, key: &str) -> &'a str {
        table
            .get(key)
            .and_then(toml::Value::as_str)
            .unwrap_or_else(|| panic!("manifest key {key} is present as a string"))
    }

    fn is_wal_manifest_path(path: &str) -> bool {
        Path::new(path)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("wal"))
    }

    fn parse_wal_segsize(raw_size: &str) -> u64 {
        let Some(megabytes) = raw_size.strip_suffix("MB") else {
            panic!("unsupported wal_segsize {raw_size}");
        };
        let megabytes = megabytes
            .parse::<u64>()
            .unwrap_or_else(|source| panic!("wal_segsize must start with a number: {source}"));

        megabytes
            .checked_mul(1024 * 1024)
            .expect("wal_segsize fits in u64")
    }

    fn segment_base_lsn_from_filename(path: &str, wal_segsize: u64) -> Lsn {
        let Some(filename) = Path::new(path).file_name().and_then(|name| name.to_str()) else {
            panic!("WAL path must have a UTF-8 file name: {path}");
        };
        let Some(wal_name) = filename.strip_suffix(".wal") else {
            panic!("WAL file must use .wal extension: {path}");
        };
        assert!(wal_name.len() == 24);

        let log = parse_hex_u64(&wal_name[8..16], "WAL log number");
        let segment = parse_hex_u64(&wal_name[16..24], "WAL segment number");
        let lsn_space_per_log = u64::from(u32::MAX) + 1;
        assert!(lsn_space_per_log % wal_segsize == 0);
        let segments_per_log = lsn_space_per_log / wal_segsize;
        let segment_number = log
            .checked_mul(segments_per_log)
            .and_then(|value| value.checked_add(segment))
            .expect("WAL segment number fits in u64");
        let base_lsn = segment_number
            .checked_mul(wal_segsize)
            .expect("WAL segment base LSN fits in u64");

        Lsn(base_lsn)
    }

    fn parse_hex_u64(raw: &str, label: &str) -> u64 {
        u64::from_str_radix(raw, 16)
            .unwrap_or_else(|source| panic!("invalid {label} {raw}: {source}"))
    }

    fn build_record(xid: u32, info: u8, rmid: u8, body: &[u8]) -> Vec<u8> {
        let total_len = XLOG_RECORD_HEADER_SIZE + body.len();
        let mut record = vec![0; total_len];
        record[..4].copy_from_slice(&test_u32(total_len).to_le_bytes());
        record[4..8].copy_from_slice(&xid.to_le_bytes());
        record[16] = info;
        record[17] = rmid;
        record[XLOG_RECORD_HEADER_SIZE..].copy_from_slice(body);

        let mut crc = crc32c::crc32c(&record[XLOG_RECORD_HEADER_SIZE..]);
        crc = crc32c::crc32c_append(crc, &record[..20]);
        record[20..24].copy_from_slice(&crc.to_le_bytes());
        record
    }

    fn build_page(page_lsn: Lsn, info: u16, rem_len: u32, payload: &[u8]) -> Vec<u8> {
        let header_size = if info & consts_v17::XLP_LONG_HEADER == 0 {
            consts_v17::SIZE_OF_SHORT_PHD
        } else {
            consts_v17::SIZE_OF_LONG_PHD
        };
        let mut page = vec![0; consts_v17::XLOG_BLCKSZ];
        page[0..2].copy_from_slice(&consts_v17::XLOG_PAGE_MAGIC.to_le_bytes());
        page[2..4].copy_from_slice(&info.to_le_bytes());
        page[4..8].copy_from_slice(&1_u32.to_le_bytes());
        page[8..16].copy_from_slice(&page_lsn.0.to_le_bytes());
        page[16..20].copy_from_slice(&rem_len.to_le_bytes());

        if info & consts_v17::XLP_LONG_HEADER != 0 {
            page[24..32].copy_from_slice(&0x0123_4567_89AB_CDEF_u64.to_le_bytes());
            page[32..36].copy_from_slice(&(1024_u32 * 1024).to_le_bytes());
            page[36..40].copy_from_slice(&test_u32(consts_v17::XLOG_BLCKSZ).to_le_bytes());
        }

        let end = header_size + payload.len();
        page[header_size..end].copy_from_slice(payload);
        page
    }

    fn test_u32(value: usize) -> u32 {
        u32::try_from(value).expect("test value fits in u32")
    }
}
