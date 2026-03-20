// O(1) fixed-size object pool with generational keys for use-after-free
// detection. Used for in-memory topology structures (active transactions,
// per-shard indices, etc.).

// ---------------------------------------------------------------------------
// SlabKey
// ---------------------------------------------------------------------------

/// Handle to an entry in a [`Slab`]. Combines an index with a generation
/// counter so that stale keys are detected rather than silently aliasing a
/// recycled slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SlabKey {
    index: u32,
    generation: u32,
}

// ---------------------------------------------------------------------------
// Slab<T>
// ---------------------------------------------------------------------------

enum Entry<T> {
    Occupied { value: T, generation: u32 },
    Free { next: Option<u32>, generation: u32 },
}

/// A slab allocator: O(1) insert, remove, and lookup by key.
pub struct Slab<T> {
    entries: Vec<Entry<T>>,
    free_head: Option<u32>,
    len: usize,
}

impl<T> Slab<T> {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            free_head: None,
            len: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
            free_head: None,
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Insert a value, returning a key that can be used to access it later.
    pub fn insert(&mut self, value: T) -> SlabKey {
        self.len += 1;

        if let Some(index) = self.free_head {
            // Reuse a free slot
            let entry = &mut self.entries[index as usize];
            let generation = match entry {
                Entry::Free { next, generation } => {
                    self.free_head = *next;
                    *generation
                }
                Entry::Occupied { .. } => unreachable!("free_head pointed to occupied slot"),
            };
            let new_gen = generation + 1;
            *entry = Entry::Occupied {
                value,
                generation: new_gen,
            };
            SlabKey {
                index,
                generation: new_gen,
            }
        } else {
            // Grow
            let index = self.entries.len() as u32;
            let generation = 0;
            self.entries.push(Entry::Occupied { value, generation });
            SlabKey { index, generation }
        }
    }

    /// Remove an entry by key. Returns `None` if the key is stale or invalid.
    pub fn remove(&mut self, key: SlabKey) -> Option<T> {
        let entry = self.entries.get_mut(key.index as usize)?;
        match entry {
            Entry::Occupied { generation, .. } if *generation == key.generation => {
                let old = std::mem::replace(
                    entry,
                    Entry::Free {
                        next: self.free_head,
                        generation: key.generation,
                    },
                );
                self.free_head = Some(key.index);
                self.len -= 1;
                match old {
                    Entry::Occupied { value, .. } => Some(value),
                    Entry::Free { .. } => unreachable!(),
                }
            }
            _ => None,
        }
    }

    /// Get a shared reference to the value at `key`.
    pub fn get(&self, key: SlabKey) -> Option<&T> {
        match self.entries.get(key.index as usize)? {
            Entry::Occupied { value, generation } if *generation == key.generation => Some(value),
            _ => None,
        }
    }

    /// Get a mutable reference to the value at `key`.
    pub fn get_mut(&mut self, key: SlabKey) -> Option<&mut T> {
        match self.entries.get_mut(key.index as usize)? {
            Entry::Occupied {
                value, generation, ..
            } if *generation == key.generation => Some(value),
            _ => None,
        }
    }

    /// Iterate over all occupied entries as `(SlabKey, &T)`.
    pub fn iter(&self) -> impl Iterator<Item = (SlabKey, &T)> {
        self.entries
            .iter()
            .enumerate()
            .filter_map(|(i, entry)| match entry {
                Entry::Occupied { value, generation } => Some((
                    SlabKey {
                        index: i as u32,
                        generation: *generation,
                    },
                    value,
                )),
                Entry::Free { .. } => None,
            })
    }
}

impl<T> Default for Slab<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_get() {
        let mut slab = Slab::new();
        let k = slab.insert(42);
        assert_eq!(slab.get(k), Some(&42));
        assert_eq!(slab.len(), 1);
    }

    #[test]
    fn remove_and_reinsert() {
        let mut slab = Slab::new();
        let k1 = slab.insert("hello");
        slab.remove(k1);
        assert_eq!(slab.len(), 0);
        assert_eq!(slab.get(k1), None); // stale key

        let k2 = slab.insert("world");
        assert_eq!(k2.index, k1.index); // slot reused
        assert_ne!(k2.generation, k1.generation); // generation bumped
        assert_eq!(slab.get(k2), Some(&"world"));
    }

    #[test]
    fn stale_key_returns_none() {
        let mut slab = Slab::new();
        let k1 = slab.insert(1);
        slab.remove(k1);
        slab.insert(2); // reuses slot

        // Old key is stale — different generation
        assert_eq!(slab.get(k1), None);
        assert_eq!(slab.remove(k1), None);
    }

    #[test]
    fn get_mut() {
        let mut slab = Slab::new();
        let k = slab.insert(10);
        *slab.get_mut(k).unwrap() = 20;
        assert_eq!(slab.get(k), Some(&20));
    }

    #[test]
    fn iter_skips_free() {
        let mut slab = Slab::new();
        let k0 = slab.insert("a");
        let _k1 = slab.insert("b");
        let _k2 = slab.insert("c");
        slab.remove(k0);

        let values: Vec<_> = slab.iter().map(|(_, v)| *v).collect();
        assert_eq!(values, vec!["b", "c"]);
    }

    #[test]
    fn many_insert_remove_cycles() {
        let mut slab = Slab::new();
        let mut keys = Vec::new();

        for i in 0..100 {
            keys.push(slab.insert(i));
        }
        assert_eq!(slab.len(), 100);

        // Remove even indices
        for i in (0..100).step_by(2) {
            slab.remove(keys[i]);
        }
        assert_eq!(slab.len(), 50);

        // Reinsert — should reuse freed slots
        for i in 0..50 {
            slab.insert(1000 + i);
        }
        assert_eq!(slab.len(), 100);
    }

    #[test]
    fn invalid_key() {
        let slab = Slab::<i32>::new();
        let bad = SlabKey {
            index: 999,
            generation: 0,
        };
        assert_eq!(slab.get(bad), None);
    }
}
