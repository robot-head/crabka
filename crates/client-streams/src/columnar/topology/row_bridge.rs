//! `RowBridge`: convert `Vec<row>` ↔ a polars payload `DataFrame`. The default
//! impl goes through `serde_json` so it works for any `Serialize + DeserializeOwned`
//! row type.

use ::polars::prelude::*;
use serde::{Serialize, de::DeserializeOwned};

use super::codec::BatchError;

/// Convert decoded rows ↔ a payload `DataFrame` (no reserved columns).
pub trait RowBridge<R>: Send + Sync + 'static {
    /// Assemble `rows` into a payload `DataFrame`.
    ///
    /// # Errors
    /// Returns [`BatchError`] if the rows cannot be assembled into a frame.
    fn rows_to_frame(&self, rows: &[R]) -> Result<DataFrame, BatchError>;
    /// Convert a payload `DataFrame` back into rows.
    ///
    /// # Errors
    /// Returns [`BatchError`] if the frame cannot be converted back to rows.
    fn frame_to_rows(&self, df: &DataFrame) -> Result<Vec<R>, BatchError>;
}

/// JSON-value-backed bridge: works for any `R: Serialize + DeserializeOwned`.
///
/// Convenience over fidelity: column dtypes are inferred by polars from the JSON,
/// so numeric types can shift (e.g. an all-integer column round-trips fine, but a
/// column mixing integers and nulls/floats may be widened to `f64`). For
/// strongly-typed, lossless batches use a custom [`RowBridge`] over a binary
/// encoding instead.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonRowBridge;

impl<R: Serialize + DeserializeOwned + Send + Sync + 'static> RowBridge<R> for JsonRowBridge {
    fn rows_to_frame(&self, rows: &[R]) -> Result<DataFrame, BatchError> {
        let json = serde_json::to_vec(rows).map_err(|e| BatchError(e.to_string()))?;
        let cursor = std::io::Cursor::new(json);
        JsonReader::new(cursor)
            .with_json_format(JsonFormat::Json)
            .finish()
            .map_err(|e| BatchError(format!("rows_to_frame: {e}")))
    }

    fn frame_to_rows(&self, df: &DataFrame) -> Result<Vec<R>, BatchError> {
        let mut buf = Vec::new();
        let mut df = df.clone();
        JsonWriter::new(&mut buf)
            .with_json_format(JsonFormat::Json)
            .finish(&mut df)
            .map_err(|e| BatchError(format!("frame_to_rows write: {e}")))?;
        serde_json::from_slice(&buf).map_err(|e| BatchError(format!("frame_to_rows parse: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use serde::Deserialize;

    use super::*;

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    struct Txn {
        user: String,
        amount: i64,
    }

    #[test]
    fn json_bridge_round_trips_rows() {
        let bridge = JsonRowBridge;
        let rows = vec![
            Txn {
                user: "a".into(),
                amount: 5,
            },
            Txn {
                user: "b".into(),
                amount: 7,
            },
        ];
        let df = bridge.rows_to_frame(&rows).unwrap();
        check!(df.height() == 2);
        let back: Vec<Txn> = bridge.frame_to_rows(&df).unwrap();
        check!(back == rows);
    }
}
