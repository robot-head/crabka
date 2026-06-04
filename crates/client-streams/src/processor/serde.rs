//! `Serde<T>`: typed (de)serialization at source/sink boundaries.

use bytes::Bytes;

/// Failure to deserialize bytes into `T`.
#[derive(Debug, thiserror::Error)]
#[error("deserialization error: {0}")]
pub struct SerdeError(pub String);

/// Serialize a `T` to bytes and back. Used by source nodes (deserialize) and
/// sink/repartition nodes (serialize).
pub trait Serde<T>: Send + Sync + 'static {
    fn serialize(&self, value: &T) -> Bytes;
    fn deserialize(&self, bytes: &[u8]) -> Result<T, SerdeError>;
}

/// Identity serde for raw `Bytes`.
#[derive(Debug, Clone, Copy, Default)]
pub struct BytesSerde;
impl Serde<Bytes> for BytesSerde {
    fn serialize(&self, value: &Bytes) -> Bytes {
        value.clone()
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<Bytes, SerdeError> {
        Ok(Bytes::copy_from_slice(bytes))
    }
}

/// UTF-8 `String` serde.
#[derive(Debug, Clone, Copy, Default)]
pub struct StringSerde;
impl Serde<String> for StringSerde {
    #[allow(clippy::ptr_arg)]
    fn serialize(&self, value: &String) -> Bytes {
        Bytes::copy_from_slice(value.as_bytes())
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<String, SerdeError> {
        String::from_utf8(bytes.to_vec()).map_err(|e| SerdeError(e.to_string()))
    }
}

/// Big-endian 8-byte `i64` serde (matches the JVM `Serdes.Long()`).
#[derive(Debug, Clone, Copy, Default)]
pub struct I64Serde;
impl Serde<i64> for I64Serde {
    fn serialize(&self, value: &i64) -> Bytes {
        Bytes::copy_from_slice(&value.to_be_bytes())
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<i64, SerdeError> {
        let arr: [u8; 8] = bytes
            .try_into()
            .map_err(|_| SerdeError(format!("expected 8 bytes, got {}", bytes.len())))?;
        Ok(i64::from_be_bytes(arr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::check;

    #[test]
    fn string_serde_round_trips() {
        let s = StringSerde;
        let b = s.serialize(&"héllo".to_string());
        check!(s.deserialize(&b).unwrap() == "héllo");
    }

    #[test]
    fn i64_serde_is_big_endian_8_bytes() {
        let s = I64Serde;
        let b = s.serialize(&1i64);
        check!(b.as_ref() == [0, 0, 0, 0, 0, 0, 0, 1]);
        check!(s.deserialize(&b).unwrap() == 1);
        check!(s.deserialize(&[0, 1]).is_err());
    }

    #[test]
    fn bytes_serde_is_identity() {
        let s = BytesSerde;
        let b = s.serialize(&bytes::Bytes::from_static(b"xy"));
        check!(s.deserialize(&b).unwrap() == bytes::Bytes::from_static(b"xy"));
    }
}
