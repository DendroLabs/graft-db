use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// Thread-safe queue for passing messages between shard event loops and
/// network I/O threads. Wraps `Arc<Mutex<VecDeque<T>>>`.
pub struct SharedQueue<T> {
    inner: Arc<Mutex<VecDeque<T>>>,
}

impl<T> SharedQueue<T> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub fn push(&self, item: T) {
        self.inner.lock().unwrap().push_back(item);
    }

    pub fn push_batch(&self, items: Vec<T>) {
        let mut q = self.inner.lock().unwrap();
        q.extend(items);
    }

    pub fn drain(&self) -> Vec<T> {
        let mut q = self.inner.lock().unwrap();
        q.drain(..).collect()
    }
}

impl<T> Clone for SharedQueue<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Default for SharedQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Control message for dynamic replica registration on the primary.
#[derive(Debug)]
pub enum ReplControl {
    Register {
        id: String,
        shard_id: u8,
        last_lsn: u64,
    },
    Unregister {
        id: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_and_drain() {
        let q = SharedQueue::new();
        q.push(1);
        q.push(2);
        q.push(3);
        let items = q.drain();
        assert_eq!(items, vec![1, 2, 3]);
        assert!(q.drain().is_empty());
    }

    #[test]
    fn push_batch() {
        let q = SharedQueue::new();
        q.push_batch(vec![10, 20, 30]);
        q.push(40);
        let items = q.drain();
        assert_eq!(items, vec![10, 20, 30, 40]);
    }

    #[test]
    fn clone_shares_state() {
        let q1 = SharedQueue::new();
        let q2 = q1.clone();
        q1.push(42);
        let items = q2.drain();
        assert_eq!(items, vec![42]);
    }

    #[test]
    fn concurrent_access() {
        let q = SharedQueue::new();
        let q2 = q.clone();
        let handle = std::thread::spawn(move || {
            for i in 0..100 {
                q2.push(i);
            }
        });
        handle.join().unwrap();
        let items = q.drain();
        assert_eq!(items.len(), 100);
    }
}
