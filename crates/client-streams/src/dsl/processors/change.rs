//! `Change<V>` — the (old, new) value a `KTable` propagates internally.
//! `new == None` is a tombstone (the key was deleted / stopped matching).
//! State stores hold `V`; only the inter-node forwarded value is `Change<V>`.

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Change<V> {
    pub old: Option<V>,
    pub new: Option<V>,
}

#[allow(dead_code)]
impl<V> Change<V> {
    pub fn update(old: Option<V>, new: V) -> Self {
        Self {
            old,
            new: Some(new),
        }
    }
    pub fn tombstone(old: Option<V>) -> Self {
        Self { old, new: None }
    }
    pub fn is_tombstone(&self) -> bool {
        self.new.is_none()
    }
    /// Map both sides through `f` (used by `KTable` `map_values`).
    pub fn map<V2>(self, f: impl Fn(&V) -> V2) -> Change<V2> {
        Change {
            old: self.old.as_ref().map(&f),
            new: self.new.as_ref().map(&f),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn change_update_and_tombstone() {
        let upd = Change::update(Some(1), 2);
        check!(upd.old == Some(1));
        check!(upd.new == Some(2));
        check!(!upd.is_tombstone());
        let tomb: Change<i64> = Change::tombstone(Some(5));
        check!(tomb.new.is_none());
        check!(tomb.is_tombstone());
        let mapped = Change::update(Some(1), 2).map(ToString::to_string);
        check!(mapped.old == Some("1".to_string()));
        check!(mapped.new == Some("2".to_string()));
    }
}
