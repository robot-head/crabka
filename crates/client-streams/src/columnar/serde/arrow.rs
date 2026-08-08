//! `ArrowIpcSerde`, the Arrow-IPC stream encoding of an arrow-rs
//! `RecordBatch`.

use ::arrow::{
    array::RecordBatch,
    ipc::{reader::StreamReader, writer::StreamWriter},
};
use bytes::Bytes;

use crate::processor::serde::{DefaultSerde, Serde, SerdeAssociate, SerdeError};

/// `Serde<RecordBatch>` that uses the Arrow IPC stream format, which embeds the
/// schema in each message.
#[derive(Debug, Clone, Copy, Default)]
pub struct ArrowIpcSerde;

impl Serde<RecordBatch> for ArrowIpcSerde {
    fn serialize(&self, _topic: &str, value: &RecordBatch) -> Bytes {
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &value.schema())
                .expect("arrow IPC StreamWriter init on in-memory buffer");
            writer.write(value).expect("arrow IPC write batch");
            writer.finish().expect("arrow IPC finish");
        }
        Bytes::from(buf)
    }

    fn deserialize(&self, _topic: &str, bytes: &[u8]) -> Result<RecordBatch, SerdeError> {
        let mut reader = StreamReader::try_new(bytes, None)
            .map_err(|e| SerdeError(format!("arrow IPC read: {e}")))?;
        match reader.next() {
            Some(Ok(batch)) => Ok(batch),
            Some(Err(e)) => Err(SerdeError(format!("arrow IPC decode: {e}"))),
            None => Err(SerdeError(
                "arrow IPC stream contained no record batch".into(),
            )),
        }
    }
}

impl SerdeAssociate for ArrowIpcSerde {
    type Target = RecordBatch;
}
impl DefaultSerde for RecordBatch {
    type Serde = ArrowIpcSerde;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ::arrow::{
        array::{Float64Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use assert2::check;

    use super::*;

    fn sample() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("total", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Float64Array::from(vec![1.0_f64, 2.5])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn arrow_ipc_round_trips() {
        let s = ArrowIpcSerde;
        let batch = sample();
        let bytes = s.serialize("t", &batch);
        let back = s.deserialize("t", &bytes).unwrap();
        check!(back.num_rows() == batch.num_rows());
        check!(back.num_columns() == batch.num_columns());
        check!(back.schema() == batch.schema());
        check!(back == batch);
    }

    #[test]
    fn arrow_ipc_is_readable_by_standalone_stream_reader() {
        // Cross-reader portability: the bytes parse as a standalone Arrow IPC stream.
        let s = ArrowIpcSerde;
        let bytes = s.serialize("t", &sample());
        let reader = StreamReader::try_new(&bytes[..], None).unwrap();
        let batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
        check!(batches.len() == 1);
        check!(batches[0].num_rows() == 2);
    }

    #[test]
    fn arrow_ipc_rejects_garbage() {
        let s = ArrowIpcSerde;
        check!(s.deserialize("t", b"not-ipc").is_err());
    }
}
