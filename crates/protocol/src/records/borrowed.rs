//! Borrowed `RecordBatch<'a>`, `Record<'a>`, and `RecordHeader<'a>`.

use bytes::Bytes;

use crate::records::header::{Attributes, RecordBatchHeader};

pub struct RecordBatch<'a> {
    pub(crate) header: &'a RecordBatchHeader,
    pub(crate) body: RecordBody<'a>,
}

pub(crate) enum RecordBody<'a> {
    Borrowed(&'a [u8]),
    Owned(Bytes),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record<'a> {
    pub attributes: i8,
    pub timestamp_delta: i64,
    pub offset_delta: i32,
    pub key: Option<&'a [u8]>,
    pub value: Option<&'a [u8]>,
    pub headers: Vec<RecordHeader<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordHeader<'a> {
    pub key: &'a str,
    pub value: Option<&'a [u8]>,
}

impl<'a> RecordBatch<'a> {
    #[must_use]
    pub fn header(&self) -> &RecordBatchHeader {
        self.header
    }

    #[must_use]
    pub fn attributes(&self) -> Attributes {
        Attributes(self.header.attributes.get())
    }
}
