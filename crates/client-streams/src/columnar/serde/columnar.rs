//! `ColumnarSerde<T>`, the native zero-copy `columnar` encoding at the
//! `Serde<T>` boundary.

use std::marker::PhantomData;

use ::columnar::{
    Borrow, Columnar, ContainerOf,
    bytes::stash::Stash,
    common::{Index, Len, Push},
};
use bytes::Bytes;

use crate::processor::serde::{Serde, SerdeAssociate, SerdeError};

/// `Serde<T>` for any `T: columnar::Columnar`, with columnar's native byte
/// layout.
///
/// You opt in per type. There is no `DefaultSerde` blanket impl, because Rust
/// coherence forbids one.
///
/// # Wire format
///
/// The bytes are exactly columnar's own `indexed` encoding of a one-element
/// container. [`Stash::write_bytes`] produces them and
/// [`Stash::try_from_bytes`] rebuilds them. That encoding is a `u64`-aligned
/// offset index followed by the column byte regions, so the buffer is
/// self-describing and `8`-byte framed. It needs no hand-rolled length prefix.
///
/// Reconstruction goes through [`Stash::try_from_bytes`]. That function
/// validates the framing header, the slice count, and the `u64` alignment before
/// any indexing, and it relocates the data to an aligned buffer when the incoming
/// slice is misaligned. Malformed or truncated input therefore returns a
/// [`SerdeError`] and does not panic.
pub struct ColumnarSerde<T>(PhantomData<fn() -> T>);

impl<T> Clone for ColumnarSerde<T> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}
impl<T> Default for ColumnarSerde<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T: Columnar + Send + Sync + 'static> Serde<T> for ColumnarSerde<T> {
    fn serialize(&self, _topic: &str, value: &T) -> Bytes {
        // A `Stash` in its `Typed` variant is just the native container; push the
        // single value and serialize with columnar's own indexed writer.
        let mut stash: Stash<ContainerOf<T>, Vec<u8>> = Stash::default();
        Push::push(&mut stash, value);
        let mut buf = Vec::with_capacity(stash.length_in_bytes());
        // `write_bytes` over a `Vec<u8>` is infallible (its `WriteBytes::Error`
        // is `Infallible`), so this never errors.
        stash
            .write_bytes(&mut buf)
            .expect("writing columnar bytes to a Vec is infallible");
        Bytes::from(buf)
    }

    fn deserialize(&self, _topic: &str, bytes: &[u8]) -> Result<T, SerdeError> {
        // `try_from_bytes` validates the framing header, the slice count for
        // `T`'s container, and `u64` alignment (relocating if misaligned). It
        // returns `Err` — never panics — on structurally invalid/truncated data.
        let stash: Stash<ContainerOf<T>, Vec<u8>> = Stash::try_from_bytes(bytes.to_vec())
            .map_err(|e| SerdeError(format!("invalid columnar bytes: {e}")))?;
        let borrowed = <Stash<ContainerOf<T>, Vec<u8>> as Borrow>::borrow(&stash);
        if Len::len(&borrowed) != 1 {
            return Err(SerdeError(format!(
                "expected exactly one columnar element, found {}",
                Len::len(&borrowed)
            )));
        }
        let reference = Index::get(&borrowed, 0);
        Ok(<T as Columnar>::into_owned(reference))
    }
}

impl<T: Send + Sync + 'static> SerdeAssociate for ColumnarSerde<T> {
    type Target = T;
}

#[cfg(test)]
mod tests {
    use ::columnar::Columnar;
    use assert2::check;

    use super::*;

    #[derive(Columnar, Clone, Debug, PartialEq)]
    struct Point {
        x: i64,
        y: i64,
    }

    #[test]
    fn columnar_serde_round_trips() {
        let s = ColumnarSerde::<Point>::default();
        let p = Point { x: 3, y: -7 };
        let bytes = s.serialize("t", &p);
        check!(s.deserialize("t", &bytes).unwrap() == p);
    }

    #[test]
    fn columnar_serde_rejects_truncated_bytes() {
        let s = ColumnarSerde::<Point>::default();
        check!(s.deserialize("t", &[0u8, 1, 2]).is_err());
    }
}
