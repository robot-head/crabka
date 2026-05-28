//! `auth_to_local` principal-to-local-name rules. Minimal stub; Task 3
//! replaces this with the full DSL parser/applier.

/// A single `auth_to_local` rule. Placeholder definition (Task 3 fills it).
#[derive(Debug, Clone)]
pub enum Rule {
    /// The `DEFAULT` rule: strip the realm if it matches the default realm.
    Default,
}
