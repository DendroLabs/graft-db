use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::protocol::WalBatchMsg;
use crate::shared_queue::SharedQueue;

/// Per-shard directory mapping stable replica IDs to their outbound WAL
/// batch queue.
///
/// Both the network transport (on connect, looking up/creating a replica's
/// queue) and the shard's `ReplicationSender` (on `ReplControl::Register`)
/// resolve through this registry. That guarantees a reconnecting replica is
/// handed the *same* queue object it had before the disconnect — so any
/// batches buffered while it was offline are not lost — and that two
/// different replicas never end up draining the same queue.
#[derive(Clone, Default)]
pub struct ReplicaOutboxRegistry {
    inner: Arc<Mutex<HashMap<String, SharedQueue<WalBatchMsg>>>>,
}

impl ReplicaOutboxRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the existing queue for `replica_id`, creating a fresh empty one
    /// if this is the first time this ID has been seen.
    pub fn get_or_create(&self, replica_id: &str) -> SharedQueue<WalBatchMsg> {
        let mut map = self.inner.lock().unwrap();
        map.entry(replica_id.to_string()).or_default().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_id_returns_same_queue() {
        let registry = ReplicaOutboxRegistry::new();
        let q1 = registry.get_or_create("replica-a");
        q1.push(WalBatchMsg {
            shard_id: 0,
            first_lsn: 1,
            last_lsn: 2,
            records: vec![1, 2, 3],
        });

        // Simulate a reconnect: look the same ID up again.
        let q2 = registry.get_or_create("replica-a");
        assert_eq!(q2.len(), 1);
    }

    #[test]
    fn different_ids_get_different_queues() {
        let registry = ReplicaOutboxRegistry::new();
        let a = registry.get_or_create("replica-a");
        let b = registry.get_or_create("replica-b");

        a.push(WalBatchMsg {
            shard_id: 0,
            first_lsn: 1,
            last_lsn: 2,
            records: vec![1],
        });

        assert_eq!(a.len(), 1);
        assert_eq!(b.len(), 0);
    }
}
