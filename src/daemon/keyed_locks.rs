//! Per-key async mutual exclusion for daemon-owned idempotency sections

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

type Entries<K> = Mutex<HashMap<K, Weak<AsyncMutex<()>>>>;

/// A set of async locks, one per key, created on demand
///
/// Two holders of the same key run one at a time, and holders of different
/// keys never wait on each other. A key's lock lives only while a guard or a
/// waiter references it, so the map holds only keys that are in use
///
/// Guards are owned and `'static`, so callers can hold them across `.await`
/// points and move them between tasks. Locks are not reentrant: taking the
/// same key twice in one task deadlocks
///
/// Each instance is independent. Store one per daemon, not in a process-wide
/// static, so that in-process test daemons do not share lock state
pub(crate) struct KeyedLocks<K> {
    entries: Arc<Entries<K>>,
}

impl<K> Clone for KeyedLocks<K> {
    fn clone(&self) -> Self {
        Self {
            entries: Arc::clone(&self.entries),
        }
    }
}

impl<K> Default for KeyedLocks<K> {
    fn default() -> Self {
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<K: Eq + Hash + Clone> KeyedLocks<K> {
    /// Wait until no other guard holds `key`, then hold it
    pub(crate) async fn lock(&self, key: K) -> KeyedGuard<K> {
        let (lock, entry) = self.entry(key);
        let permit = lock.lock_owned().await;
        KeyedGuard {
            _permit: permit,
            _entry: entry,
        }
    }

    /// Hold `key` only when no other guard holds it now
    pub(crate) fn try_lock(&self, key: K) -> Option<KeyedGuard<K>> {
        let (lock, entry) = self.entry(key);
        let permit = lock.try_lock_owned().ok()?;
        Some(KeyedGuard {
            _permit: permit,
            _entry: entry,
        })
    }

    fn entry(&self, key: K) -> (Arc<AsyncMutex<()>>, EntryRef<K>) {
        let mut entries = lock_entries(&self.entries);
        let lock = match entries.get(&key).and_then(Weak::upgrade) {
            Some(lock) => lock,
            None => {
                let lock = Arc::new(AsyncMutex::new(()));
                entries.insert(key.clone(), Arc::downgrade(&lock));
                lock
            }
        };
        let entry = EntryRef {
            lock: Some(Arc::clone(&lock)),
            key,
            entries: Arc::clone(&self.entries),
        };
        (lock, entry)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        lock_entries(&self.entries).len()
    }
}

/// Exclusive hold on one key of a [`KeyedLocks`], released on drop
#[must_use = "the key is released as soon as the guard is dropped"]
pub(crate) struct KeyedGuard<K: Eq + Hash> {
    // field order matters: the permit must release its reference before the
    // entry checks whether the key is still in use
    _permit: OwnedMutexGuard<()>,
    _entry: EntryRef<K>,
}

/// One strong reference to a key's lock that removes the map entry when it
/// is the last reference
///
/// Waiters hold this too, so a cancelled `lock` future also cleans up
struct EntryRef<K: Eq + Hash> {
    lock: Option<Arc<AsyncMutex<()>>>,
    key: K,
    entries: Arc<Entries<K>>,
}

impl<K: Eq + Hash> Drop for EntryRef<K> {
    fn drop(&mut self) {
        drop(self.lock.take());
        // upgrades happen only under the map lock, so a zero count seen here
        // cannot become nonzero before the entry is removed
        let mut entries = lock_entries(&self.entries);
        if entries
            .get(&self.key)
            .is_some_and(|lock| lock.strong_count() == 0)
        {
            entries.remove(&self.key);
        }
    }
}

fn lock_entries<K>(entries: &Entries<K>) -> MutexGuard<'_, HashMap<K, Weak<AsyncMutex<()>>>> {
    // no code panics while holding the map, so a poisoned map is still consistent
    entries.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use super::KeyedLocks;

    #[tokio::test]
    async fn released_and_cancelled_keys_leave_no_entries() {
        let locks = KeyedLocks::<u32>::default();
        let held = locks.lock(1).await;
        let other = locks.lock(2).await;
        assert!(locks.try_lock(1).is_none());
        assert_eq!(locks.len(), 2);
        drop(other);
        assert_eq!(locks.len(), 1);

        // a waiter that is woken but dropped before it runs is the last reference
        {
            let mut waiter = pin!(locks.lock(1));
            tokio::select! {
                biased;
                _ = &mut waiter => panic!("key 1 is still held"),
                () = std::future::ready(()) => {}
            }
            drop(held);
            assert_eq!(locks.len(), 1);
        }
        assert_eq!(locks.len(), 0);

        assert!(locks.try_lock(1).is_some());
        assert_eq!(locks.len(), 0);
    }
}
