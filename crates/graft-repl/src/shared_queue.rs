use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::protocol::WalBatchMsg;

/// Thread-safe queue for passing messages between shard event loops and
/// network I/O threads. Wraps `Arc<Mutex<VecDeque<T>>>`.
#[derive(Debug)]
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

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clone the front item without removing it. Used by writer threads that
    /// must not lose an item if the send fails partway — the item stays
    /// queued until `pop_front` confirms it was actually delivered.
    pub fn peek_front(&self) -> Option<T>
    where
        T: Clone,
    {
        self.inner.lock().unwrap().front().cloned()
    }

    /// Remove and return the front item. Pair with `peek_front` for
    /// send-then-confirm delivery (see above).
    pub fn pop_front(&self) -> Option<T> {
        self.inner.lock().unwrap().pop_front()
    }

    /// Sum of `f` applied to every queued item. Used to enforce a bounded
    /// byte budget per queue without a separate atomic counter to keep in
    /// sync — O(n) in queue length, which is self-limiting since callers
    /// use this to enforce the very cap that bounds n.
    pub fn total_bytes_by<F: Fn(&T) -> usize>(&self, f: F) -> usize {
        self.inner.lock().unwrap().iter().map(f).sum()
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
        /// The replica's outbound batch queue, resolved through
        /// `ReplicaOutboxRegistry` so a reconnecting replica is handed the
        /// exact same queue (and any backlog buffered while it was
        /// disconnected) rather than an empty one.
        outbox: SharedQueue<WalBatchMsg>,
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
