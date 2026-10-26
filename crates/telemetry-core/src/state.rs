//! Keyed in-memory state backed by a compacted Kafka changelog topic.
//!
//! Writes are staged in an overlay for the open transaction. The processor calls
//! [`StateStore::commit`] only after `commit_transaction` succeeds and [`StateStore::abort`] after
//! an abort, so the committed map never holds a write whose transaction did not commit.

use std::collections::HashMap;
use std::hash::Hash;

use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;

/// One changelog write. `value: None` is a tombstone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangelogRecord {
    pub partition: i32,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

/// Type-erased view of a store, used by the processor.
pub trait StateStore: Send {
    fn name(&self) -> &str;
    /// The input partition of the record being processed; changelog writes go to this partition.
    fn set_partition(&mut self, partition: i32);
    /// Changelog records staged since the last call, in write order.
    fn take_changelog(&mut self) -> Vec<ChangelogRecord>;
    fn commit(&mut self);
    fn abort(&mut self);
    /// Apply one changelog record read during restore.
    fn restore(&mut self, partition: i32, key: &[u8], value: Option<&[u8]>) -> Result<()>;
    fn drop_partitions(&mut self, partitions: &[i32]);
    /// Committed keys.
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Keys and values are stored in the changelog as JSON.
pub struct KeyedState<K, V> {
    name: String,
    committed: HashMap<K, (i32, V)>,
    staged: HashMap<K, (i32, Option<V>)>,
    log: Vec<ChangelogRecord>,
    partition: Option<i32>,
}

impl<K, V> KeyedState<K, V>
where
    K: Eq + Hash + Clone + Serialize + DeserializeOwned + Send,
    V: Clone + Serialize + DeserializeOwned + Send,
{
    /// `name` becomes the changelog topic suffix: `<STATE_TOPIC_PREFIX>.<name>`.
    pub fn new(name: &str) -> Self {
        KeyedState {
            name: name.to_string(),
            committed: HashMap::new(),
            staged: HashMap::new(),
            log: Vec::new(),
            partition: None,
        }
    }

    /// Reads staged writes first, so later records in a transaction see earlier ones.
    pub fn get(&self, key: &K) -> Option<&V> {
        match self.staged.get(key) {
            Some((_, v)) => v.as_ref(),
            None => self.committed.get(key).map(|(_, v)| v),
        }
    }

    pub fn put(&mut self, key: K, value: V) -> Result<()> {
        let p = self.current_partition()?;
        self.log.push(ChangelogRecord {
            partition: p,
            key: serde_json::to_vec(&key).context("serialize state key")?,
            value: Some(serde_json::to_vec(&value).context("serialize state value")?),
        });
        self.staged.insert(key, (p, Some(value)));
        Ok(())
    }

    pub fn delete(&mut self, key: &K) -> Result<()> {
        let p = self.current_partition()?;
        self.log.push(ChangelogRecord {
            partition: p,
            key: serde_json::to_vec(key).context("serialize state key")?,
            value: None,
        });
        self.staged.insert(key.clone(), (p, None));
        Ok(())
    }

    fn current_partition(&self) -> Result<i32> {
        self.partition.context("state written outside Job::process")
    }
}

impl<K, V> StateStore for KeyedState<K, V>
where
    K: Eq + Hash + Clone + Serialize + DeserializeOwned + Send,
    V: Clone + Serialize + DeserializeOwned + Send,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn set_partition(&mut self, partition: i32) {
        self.partition = Some(partition);
    }

    fn take_changelog(&mut self) -> Vec<ChangelogRecord> {
        std::mem::take(&mut self.log)
    }

    fn commit(&mut self) {
        for (k, (p, v)) in self.staged.drain() {
            match v {
                Some(v) => {
                    self.committed.insert(k, (p, v));
                }
                None => {
                    self.committed.remove(&k);
                }
            }
        }
        self.log.clear();
    }

    fn abort(&mut self) {
        self.staged.clear();
        self.log.clear();
    }

    fn restore(&mut self, partition: i32, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        let k: K = serde_json::from_slice(key).context("decode changelog key")?;
        match value {
            Some(v) => {
                let v: V = serde_json::from_slice(v).context("decode changelog value")?;
                self.committed.insert(k, (partition, v));
            }
            None => {
                self.committed.remove(&k);
            }
        }
        Ok(())
    }

    fn drop_partitions(&mut self, partitions: &[i32]) {
        self.committed.retain(|_, (p, _)| !partitions.contains(p));
        self.staged.retain(|_, (p, _)| !partitions.contains(p));
    }

    fn len(&self) -> usize {
        self.committed.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> KeyedState<String, u32> {
        let mut s = KeyedState::new("t");
        s.set_partition(3);
        s
    }

    #[test]
    fn staged_writes_are_visible_before_commit_and_gone_after_abort() {
        let mut s = store();
        s.put("a".into(), 1).unwrap();
        s.commit();
        s.put("a".into(), 2).unwrap();
        s.put("b".into(), 7).unwrap();
        s.delete(&"a".to_string()).unwrap();
        assert_eq!((s.get(&"a".into()), s.get(&"b".into())), (None, Some(&7)));
        s.abort();
        assert_eq!((s.get(&"a".into()), s.get(&"b".into())), (Some(&1), None));
        assert!(s.take_changelog().is_empty());
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn commit_applies_puts_and_deletes() {
        let mut s = store();
        s.put("a".into(), 1).unwrap();
        s.put("b".into(), 2).unwrap();
        s.commit();
        s.delete(&"a".to_string()).unwrap();
        s.put("b".into(), 3).unwrap();
        assert_eq!(s.len(), 2);
        s.commit();
        assert_eq!(
            (s.get(&"a".into()), s.get(&"b".into()), s.len()),
            (None, Some(&3), 1)
        );
    }

    #[test]
    fn changelog_goes_to_the_input_partition_and_uses_tombstones() {
        let mut s = store();
        s.put("a".into(), 1).unwrap();
        s.set_partition(5);
        s.delete(&"b".to_string()).unwrap();
        let log = s.take_changelog();
        assert_eq!(log.len(), 2);
        assert_eq!(
            (
                log[0].partition,
                log[0].key.as_slice(),
                log[0].value.as_deref()
            ),
            (3, &b"\"a\""[..], Some(&b"1"[..]))
        );
        assert_eq!((log[1].partition, log[1].value.clone()), (5, None));
        assert!(s.take_changelog().is_empty());
    }

    #[test]
    fn restore_replays_the_changelog_to_the_same_state() {
        let mut s = store();
        s.put("a".into(), 1).unwrap();
        s.put("b".into(), 2).unwrap();
        s.put("a".into(), 4).unwrap();
        s.delete(&"b".to_string()).unwrap();
        s.put("c".into(), 9).unwrap();
        let log = s.take_changelog();
        s.commit();

        let mut r: KeyedState<String, u32> = KeyedState::new("t");
        for rec in &log {
            r.restore(rec.partition, &rec.key, rec.value.as_deref())
                .unwrap();
        }
        for k in ["a", "b", "c"] {
            assert_eq!(r.get(&k.into()), s.get(&k.into()), "key {k}");
        }
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn drop_partitions_removes_only_those_partitions() {
        let mut s = store();
        s.put("a".into(), 1).unwrap();
        s.set_partition(4);
        s.put("b".into(), 2).unwrap();
        s.commit();
        s.drop_partitions(&[3]);
        assert_eq!((s.get(&"a".into()), s.get(&"b".into())), (None, Some(&2)));
    }

    #[test]
    fn write_outside_process_is_an_error() {
        let mut s: KeyedState<String, u32> = KeyedState::new("t");
        assert!(s.put("a".into(), 1).is_err());
    }
}
