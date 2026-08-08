//! KIP-213 foreign-key join internals: the byte codecs and the five join
//! processors.
//!
//! The byte codecs are `CombinedKey`, `SubscriptionWrapper`,
//! `SubscriptionResponseWrapper`, and Murmur3-128. All byte formats are
//! JVM-exact. The `--fkjoin` capture in `tests/testdata/fk_join/behavior.json`
//! pins them.
pub(crate) mod combined_key;
pub(crate) mod murmur3;
pub(crate) mod processors;
pub(crate) mod subscription;
pub(crate) mod wrapper_serde;
