//! Helpers shared across generated message modules. Not public API.

#[doc(hidden)]
#[inline]
pub fn is_default<T: Default + PartialEq>(v: &T) -> bool {
    v == &T::default()
}
