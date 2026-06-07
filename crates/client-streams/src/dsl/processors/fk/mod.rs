//! KIP-213 foreign-key join internals: byte codecs (`CombinedKey`,
//! `SubscriptionWrapper`, `SubscriptionResponseWrapper`, Murmur3-128) + the five
//! join processors. All byte formats are JVM-exact (pinned by the `--fkjoin`
//! capture in `tests/testdata/fk_join/behavior.json`).
pub(crate) mod combined_key;
pub(crate) mod murmur3;
pub(crate) mod subscription;
// `processors` is added in Task 5.
