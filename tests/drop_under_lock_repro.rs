use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;

/// A value whose `Drop` accesses the map, mimicking a connection handle
/// whose destructor performs cleanup that touches another key.
struct Handle {
    map: Arc<DashMap<u64, Handle>>,
    cleanup_key: u64,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // Cleanup logic that accesses the map (possibly the same shard).
        let _ = self.map.remove(&self.cleanup_key);
    }
}

fn run_with_timeout(name: &str, f: impl FnOnce() + Send + 'static) {
    let handle = std::thread::spawn(f);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = handle.join();
        let _ = tx.send(());
    });
    if rx.recv_timeout(Duration::from_secs(10)).is_err() {
        panic!("DEADLOCK detected in case: {name}");
    }
}

#[test]
fn entry_insert_replace() {
    run_with_timeout("entry_insert_replace", || {
        let map = Arc::new(DashMap::new());
        map.insert(
            1,
            Handle {
                map: map.clone(),
                cleanup_key: 1,
            },
        );
        // Replace via Entry::insert -> old value dropped while shard lock held?
        map.entry(1).insert(Handle {
            map: map.clone(),
            cleanup_key: 1,
        });
    });
}

#[test]
fn entry_insert_entry_replace() {
    run_with_timeout("entry_insert_entry_replace", || {
        let map = Arc::new(DashMap::new());
        map.insert(
            1,
            Handle {
                map: map.clone(),
                cleanup_key: 1,
            },
        );
        map.entry(1).insert_entry(Handle {
            map: map.clone(),
            cleanup_key: 1,
        });
    });
}

#[test]
fn entry_ref_insert_replace() {
    run_with_timeout("entry_ref_insert_replace", || {
        let map: Arc<DashMap<K2, Handle2>> = Arc::new(DashMap::new());
        map.insert(K2, Handle2 { map: map.clone() });
        map.entry_ref(&K2).insert(Handle2 { map: map.clone() });
    });
}

#[derive(Hash, PartialEq, Eq)]
struct K2;

impl From<&K2> for K2 {
    fn from(_: &K2) -> Self {
        K2
    }
}

struct Handle2 {
    map: Arc<DashMap<K2, Handle2>>,
}

impl Drop for Handle2 {
    fn drop(&mut self) {
        let _ = self.map.remove(&K2);
    }
}

#[test]
fn clear_drops() {
    run_with_timeout("clear", || {
        let map = Arc::new(DashMap::new());
        for i in 0..64 {
            map.insert(
                i,
                Handle {
                    map: map.clone(),
                    cleanup_key: i,
                },
            );
        }
        map.clear();
    });
}

#[test]
fn retain_false_drops() {
    run_with_timeout("retain_false", || {
        let map = Arc::new(DashMap::new());
        for i in 0..64 {
            map.insert(
                i,
                Handle {
                    map: map.clone(),
                    cleanup_key: i,
                },
            );
        }
        map.retain(|_, _| false);
    });
}

#[test]
fn remove_control() {
    run_with_timeout("remove_control", || {
        let map = Arc::new(DashMap::new());
        map.insert(
            1,
            Handle {
                map: map.clone(),
                cleanup_key: 1,
            },
        );
        // remove returns the value; dropping it here must not hold the lock.
        let _ = map.remove(&1);
    });
}

#[test]
fn insert_replace_control() {
    run_with_timeout("insert_replace_control", || {
        let map = Arc::new(DashMap::new());
        map.insert(
            1,
            Handle {
                map: map.clone(),
                cleanup_key: 1,
            },
        );
        // insert returns the old value; dropping it here must not hold the lock.
        let _ = map.insert(
            1,
            Handle {
                map: map.clone(),
                cleanup_key: 1,
            },
        );
    });
}

#[test]
fn occupied_entry_remove_drops_key() {
    run_with_timeout("occupied_entry_remove_drops_key", || {
        let map: Arc<DashMap<DropKey, u64>> = Arc::new(DashMap::new());
        map.insert(
            DropKey {
                map: map.clone(),
                id: 1,
            },
            10,
        );
        match map.entry(DropKey {
            map: map.clone(),
            id: 1,
        }) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                entry.remove();
            }
            dashmap::mapref::entry::Entry::Vacant(_) => unreachable!(),
        };
    });
}

#[test]
fn entry_insert_then_downgrade() {
    run_with_timeout("entry_insert_then_downgrade", || {
        let map = Arc::new(DashMap::new());
        map.insert(
            1,
            Handle {
                map: map.clone(),
                cleanup_key: 1,
            },
        );
        let reference = map
            .entry(1)
            .insert(Handle {
                map: map.clone(),
                cleanup_key: 1,
            })
            .downgrade();
        drop(reference);
    });
}

/// A key whose `Drop` accesses the map.
struct DropKey {
    map: Arc<DashMap<DropKey, u64>>,
    id: u64,
}

impl PartialEq for DropKey {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for DropKey {}

impl std::hash::Hash for DropKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Drop for DropKey {
    fn drop(&mut self) {
        let query = DropKey {
            map: self.map.clone(),
            id: self.id,
        };
        let _ = self.map.remove(&query);
        // Avoid recursive drops from the lookup key itself.
        std::mem::forget(query);
    }
}
