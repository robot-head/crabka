//! Abstraction over "where ACL entries come from", so one evaluator serves both
//! the broker (a `MetadataImage` snapshot) and the gateway (a `Vec<AclEntry>`
//! cache fetched via `DescribeAcls`).

use crabka_metadata::{AclEntry, ResourceType};

/// A source of ACL entries the authorizer can match against. `matching_acls`
/// MUST return every entry whose resource pattern matches `(rt, name)`:
/// LITERAL entries equal to `name`, LITERAL `*` (wildcard), and PREFIXED
/// entries where `name.starts_with(entry.resource_name)`. (Mirror
/// [`crabka_metadata::MetadataImage::matching_acls`] —
/// `crates/metadata/src/image.rs`.)
pub trait AclSource {
    fn matching_acls<'a>(
        &'a self,
        rt: ResourceType,
        name: &'a str,
    ) -> Box<dyn Iterator<Item = &'a AclEntry> + 'a>;
}

// The broker's MetadataImage already implements the exact matching semantics;
// adapt its iterator. (Trait is local ⇒ orphan rule satisfied for the foreign
// MetadataImage type.)
impl AclSource for crabka_metadata::MetadataImage {
    fn matching_acls<'a>(
        &'a self,
        rt: ResourceType,
        name: &'a str,
    ) -> Box<dyn Iterator<Item = &'a AclEntry> + 'a> {
        Box::new(crabka_metadata::MetadataImage::matching_acls(
            self, rt, name,
        ))
    }
}
