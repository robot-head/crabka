//! `KeyValueBytesStore<K,V>`: the single typed store the registry holds and
//! downcasts to. Serde + changelog-buffer logic over a pluggable `ByteKeyValueStore`.
use std::any::Any;

use bytes::Bytes;

use crate::processor::serde::Serde;
use crate::store::api::{KeyValueStore, StateStore};
use crate::store::byte::{ByteKeyValueStore, InMemoryBytes};

pub struct KeyValueBytesStore<K, V> {
    name: String,
    changelog_topic: String,
    backend: Box<dyn ByteKeyValueStore>,
    key_serde: Box<dyn Serde<K>>,
    value_serde: Box<dyn Serde<V>>,
    changelog: Vec<(Bytes, Option<Bytes>)>,
    logging: bool,
}

impl<K: 'static, V: 'static> KeyValueBytesStore<K, V> {
    #[must_use]
    pub(crate) fn new(
        name: String,
        backend: Box<dyn ByteKeyValueStore>,
        key_serde: Box<dyn Serde<K>>,
        value_serde: Box<dyn Serde<V>>,
        changelog_topic: String,
    ) -> Self {
        Self {
            name,
            changelog_topic,
            backend,
            key_serde,
            value_serde,
            changelog: Vec::new(),
            logging: true,
        }
    }

    /// Convenience constructor for tests: an in-memory-backed store.
    #[must_use]
    pub fn in_memory(
        name: String,
        key_serde: Box<dyn Serde<K>>,
        value_serde: Box<dyn Serde<V>>,
        changelog_topic: String,
    ) -> Self {
        Self::new(
            name,
            Box::new(InMemoryBytes::default()),
            key_serde,
            value_serde,
            changelog_topic,
        )
    }
}

impl<K: 'static, V: 'static> StateStore for KeyValueBytesStore<K, V> {
    fn name(&self) -> &str {
        &self.name
    }
    fn flush(&mut self) {}
    fn close(&mut self) {}
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn changelog_topic(&self) -> &str {
        &self.changelog_topic
    }
    fn take_changelog(&mut self) -> Vec<(Bytes, Option<Bytes>)> {
        std::mem::take(&mut self.changelog)
    }
    fn apply_changelog(&mut self, key: Bytes, value: Option<Bytes>) {
        match value {
            Some(v) => self.backend.put(key, v),
            None => {
                self.backend.delete(&key);
            }
        }
    }
    fn set_logging(&mut self, on: bool) {
        self.logging = on;
    }
}

impl<K: 'static, V: 'static> KeyValueStore<K, V> for KeyValueBytesStore<K, V> {
    fn get(&self, key: &K) -> Option<V> {
        let kb = self.key_serde.serialize(key);
        self.backend.get(&kb).map(|vb| {
            self.value_serde
                .deserialize(&vb)
                .expect("store value deserialize")
        })
    }
    fn put(&mut self, key: K, value: V) {
        let kb = self.key_serde.serialize(&key);
        let vb = self.value_serde.serialize(&value);
        self.backend.put(kb.clone(), vb.clone());
        if self.logging {
            self.changelog.push((kb, Some(vb)));
        }
    }
    fn delete(&mut self, key: &K) -> Option<V> {
        let kb = self.key_serde.serialize(key);
        let prev = self.backend.delete(&kb).map(|vb| {
            self.value_serde
                .deserialize(&vb)
                .expect("store value deserialize")
        });
        if self.logging {
            self.changelog.push((kb, None));
        }
        prev
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::serde::{I64Serde, StringSerde};
    use assert2::check;

    fn store() -> KeyValueBytesStore<String, i64> {
        KeyValueBytesStore::in_memory(
            "s".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "s-changelog".into(),
        )
    }

    #[test]
    fn put_get_delete_and_changelog_buffer() {
        let mut s = store();
        s.put("a".into(), 1);
        s.put("a".into(), 2);
        check!(s.get(&"a".to_string()) == Some(2));
        check!(s.delete(&"a".to_string()) == Some(2));
        check!(s.get(&"a".to_string()) == None);
        let cl = s.take_changelog();
        check!(cl.len() == 3);
        check!(cl[2].1.is_none());
        check!(s.take_changelog().is_empty());
    }

    #[test]
    fn apply_changelog_restores_without_re_logging() {
        let mut s = store();
        s.apply_changelog(
            b"k".to_vec().into(),
            Some(bytes::Bytes::from_static(&[0, 0, 0, 0, 0, 0, 0, 7])),
        );
        check!(s.get(&"k".to_string()) == Some(7));
        check!(s.take_changelog().is_empty());
        s.apply_changelog(b"k".to_vec().into(), None);
        check!(s.get(&"k".to_string()) == None);
    }
}
