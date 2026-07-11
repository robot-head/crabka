//! `PolarsIpcSerde`: Arrow-IPC encoding of a polars `DataFrame` at the `Serde<T>` boundary.

use std::io::Cursor;

use ::polars::prelude::*;
use bytes::Bytes;

use crate::processor::serde::{DefaultSerde, Serde, SerdeAssociate, SerdeError};

/// `Serde<DataFrame>` using the Arrow IPC stream format (schema embedded per message).
#[derive(Debug, Clone, Copy, Default)]
pub struct PolarsIpcSerde;

impl Serde<DataFrame> for PolarsIpcSerde {
    fn serialize(&self, _topic: &str, value: &DataFrame) -> Bytes {
        let mut buf = Vec::new();
        let mut df = value.clone();
        IpcWriter::new(&mut buf)
            .finish(&mut df)
            .expect("polars IPC write to in-memory buffer is infallible");
        Bytes::from(buf)
    }

    fn deserialize(&self, _topic: &str, bytes: &[u8]) -> Result<DataFrame, SerdeError> {
        IpcReader::new(Cursor::new(bytes.to_vec()))
            .finish()
            .map_err(|e| SerdeError(format!("polars IPC read: {e}")))
    }
}

impl SerdeAssociate for PolarsIpcSerde {
    type Target = DataFrame;
}

impl DefaultSerde for DataFrame {
    type Serde = PolarsIpcSerde;
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    fn sample() -> DataFrame {
        df!("id" => ["a", "b"], "total" => [1.0_f64, 2.5]).unwrap()
    }

    #[test]
    fn polars_ipc_round_trips() {
        let s = PolarsIpcSerde;
        let df = sample();
        let bytes = s.serialize("t", &df);
        let back = s.deserialize("t", &bytes).unwrap();
        check!(back.equals(&df));
    }

    #[test]
    fn polars_ipc_is_readable_by_arrow_ipc_reader() {
        let s = PolarsIpcSerde;
        let bytes = s.serialize("t", &sample());
        let reader = IpcReader::new(Cursor::new(bytes.to_vec()));
        let df = reader.finish().unwrap();
        let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
        check!((df.height(), names) == (2, vec!["id", "total"]));
    }

    #[test]
    fn polars_ipc_rejects_garbage() {
        let s = PolarsIpcSerde;
        check!(s.deserialize("t", b"not-ipc").is_err());
    }
}
