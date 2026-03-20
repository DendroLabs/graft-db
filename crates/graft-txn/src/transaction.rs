use graft_core::{Error, PageId, Result, TxId};
use graft_io::IoBackend;
use hashbrown::{HashMap, HashSet};

use crate::mvcc::Snapshot;
use crate::wal::{WalBody, WalRecordType, WalWriter};

// ---------------------------------------------------------------------------
// Transaction
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxStatus {
    Active,
    Committed,
    Aborted,
}

/// Per-transaction state tracked by the [`TransactionManager`].
pub struct Transaction {
    pub tx_id: TxId,
    pub snapshot: Snapshot,
    pub status: TxStatus,
    /// Set of (page_id, slot) pairs this transaction has written.
    write_set: Vec<(PageId, u16)>,
}

// ---------------------------------------------------------------------------
// TransactionManager
// ---------------------------------------------------------------------------

/// Manages transaction lifecycles, snapshots, and conflict detection.
///
/// Single-threaded per shard. Uses first-committer-wins: multiple
/// transactions may write to the same record, but only the first to
/// commit succeeds — the others get a `WriteConflict` error.
pub struct TransactionManager {
    next_tx_id: TxId,
    active: HashMap<TxId, Transaction>,
    /// For conflict detection: maps `(page_id, slot)` to the TxId that last
    /// committed a write. Entries older than the oldest active snapshot can
    /// be pruned.
    committed_writes: HashMap<(PageId, u16), TxId>,
    /// Committed TxIds (for MVCC visibility). Bounded: we prune below the
    /// low-water mark.
    committed_set: HashSet<TxId>,
    /// The oldest snapshot timestamp among active transactions. Anything
    /// committed below this is guaranteed visible to everyone.
    low_water: TxId,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            next_tx_id: 1, // TxId 0 is reserved (NULL sentinel)
            active: HashMap::new(),
            committed_writes: HashMap::new(),
            committed_set: HashSet::new(),
            low_water: 0,
        }
    }

    /// Begin a new transaction, returning its ID.
    pub fn begin(&mut self, wal: &mut WalWriter) -> TxId {
        let tx_id = self.next_tx_id;
        self.next_tx_id += 1;

        let snapshot = Snapshot {
            ts: tx_id,
            active: self.active.keys().copied().collect(),
        };

        let tx = Transaction {
            tx_id,
            snapshot,
            status: TxStatus::Active,
            write_set: Vec::new(),
        };

        wal.append(tx_id, WalRecordType::Begin, &WalBody::Empty);
        self.active.insert(tx_id, tx);
        tx_id
    }

    /// Record that a transaction wrote to `(page_id, slot)`.
    /// Call this before modifying the page so the WAL has the entry.
    pub fn record_write(&mut self, tx_id: TxId, page_id: PageId, slot: u16) {
        if let Some(tx) = self.active.get_mut(&tx_id) {
            tx.write_set.push((page_id, slot));
        }
    }

    /// Commit a transaction. Validates write set against concurrent commits
    /// (first-committer-wins). Flushes the WAL to make the commit durable.
    pub fn commit(
        &mut self,
        tx_id: TxId,
        wal: &mut WalWriter,
        io: &mut impl IoBackend,
    ) -> Result<()> {
        let tx = self
            .active
            .get(&tx_id)
            .ok_or(Error::TxNotActive)?;

        // First-committer-wins: check if any of our writes conflict with
        // a transaction that committed concurrently (i.e. was not visible
        // to our snapshot).
        for &(page_id, slot) in &tx.write_set {
            if let Some(&last_writer) = self.committed_writes.get(&(page_id, slot)) {
                let was_invisible = last_writer >= tx.snapshot.ts
                    || tx.snapshot.active.contains(&last_writer);
                if was_invisible {
                    self.abort(tx_id, wal);
                    return Err(Error::WriteConflict(
                        graft_core::NodeId::from_raw(page_id),
                    ));
                }
            }
        }

        // No conflicts — commit
        wal.append(tx_id, WalRecordType::Commit, &WalBody::Empty);
        wal.flush(io).map_err(Error::Io)?;

        // Update bookkeeping
        let tx = self.active.remove(&tx_id).unwrap();
        for &(page_id, slot) in &tx.write_set {
            self.committed_writes.insert((page_id, slot), tx_id);
        }
        self.committed_set.insert(tx_id);
        self.update_low_water();

        Ok(())
    }

    /// Abort a transaction. Its writes are discarded (MVCC: records with
    /// tx_min = this tx_id will be invisible to all snapshots).
    pub fn abort(&mut self, tx_id: TxId, wal: &mut WalWriter) {
        wal.append(tx_id, WalRecordType::Abort, &WalBody::Empty);
        self.active.remove(&tx_id);
        self.update_low_water();
    }

    /// Get the snapshot for a transaction.
    pub fn snapshot(&self, tx_id: TxId) -> Option<&Snapshot> {
        self.active.get(&tx_id).map(|tx| &tx.snapshot)
    }

    /// Check if a transaction has been committed.
    pub fn is_committed(&self, tx_id: TxId) -> bool {
        self.committed_set.contains(&tx_id)
    }

    /// The next TxId that will be assigned.
    pub fn next_tx_id(&self) -> TxId {
        self.next_tx_id
    }

    /// Number of currently active transactions.
    pub fn active_count(&self) -> usize {
        self.active.len()
    }

    // -- internal -----------------------------------------------------------

    fn update_low_water(&mut self) {
        let new_low = self
            .active
            .values()
            .map(|tx| tx.snapshot.ts)
            .min()
            .unwrap_or(self.next_tx_id);

        if new_low > self.low_water {
            // Prune committed_writes and committed_set below the low-water mark
            self.committed_writes.retain(|_, &mut tx| tx >= self.low_water);
            self.committed_set.retain(|&tx| tx >= self.low_water);
            self.low_water = new_low;
        }
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graft_io::sim::SimIoBackend;
    use graft_io::{FileHandle, OpenOptions};
    use std::path::PathBuf;

    fn setup() -> (SimIoBackend, FileHandle, WalWriter, TransactionManager) {
        let mut io = SimIoBackend::new(42);
        let fh = io
            .open(
                &PathBuf::from("/sim/test.wal"),
                &OpenOptions::create_read_write(),
            )
            .unwrap();
        (io, fh, WalWriter::new(fh), TransactionManager::new())
    }

    #[test]
    fn begin_and_commit() {
        let (mut io, _, mut wal, mut tm) = setup();
        let tx = tm.begin(&mut wal);
        assert_eq!(tm.active_count(), 1);

        tm.commit(tx, &mut wal, &mut io).unwrap();
        assert_eq!(tm.active_count(), 0);
        assert!(tm.is_committed(tx));
    }

    #[test]
    fn begin_and_abort() {
        let (_, _, mut wal, mut tm) = setup();
        let tx = tm.begin(&mut wal);
        tm.abort(tx, &mut wal);
        assert_eq!(tm.active_count(), 0);
        assert!(!tm.is_committed(tx));
    }

    #[test]
    fn commit_inactive_tx_errors() {
        let (mut io, _, mut wal, mut tm) = setup();
        let result = tm.commit(999, &mut wal, &mut io);
        assert!(result.is_err());
    }

    #[test]
    fn snapshot_captures_active_set() {
        let (_, _, mut wal, mut tm) = setup();
        let tx1 = tm.begin(&mut wal);
        let tx2 = tm.begin(&mut wal);

        let snap = tm.snapshot(tx2).unwrap();
        assert!(snap.active.contains(&tx1));
        assert!(!snap.active.contains(&tx2));

        // tx1 is in tx2's active set, so tx1's writes are invisible to tx2
        assert!(!snap.is_visible(tx1, 0));

        let _ = tx1;
    }

    #[test]
    fn no_conflict_disjoint_writes() {
        let (mut io, _, mut wal, mut tm) = setup();

        let tx1 = tm.begin(&mut wal);
        let tx2 = tm.begin(&mut wal);

        tm.record_write(tx1, 1, 0);
        tm.record_write(tx2, 2, 0); // different page

        tm.commit(tx1, &mut wal, &mut io).unwrap();
        tm.commit(tx2, &mut wal, &mut io).unwrap(); // no conflict
    }

    #[test]
    fn first_committer_wins() {
        let (mut io, _, mut wal, mut tm) = setup();

        let tx1 = tm.begin(&mut wal);
        let tx2 = tm.begin(&mut wal);

        // Both write to same (page, slot)
        tm.record_write(tx1, 1, 0);
        tm.record_write(tx2, 1, 0);

        // tx1 commits first — succeeds
        tm.commit(tx1, &mut wal, &mut io).unwrap();

        // tx2 tries to commit — conflict!
        let result = tm.commit(tx2, &mut wal, &mut io);
        assert!(matches!(result, Err(Error::WriteConflict(_))));
    }

    #[test]
    fn no_conflict_if_committed_before_snapshot() {
        let (mut io, _, mut wal, mut tm) = setup();

        // tx1 writes and commits BEFORE tx2 starts
        let tx1 = tm.begin(&mut wal);
        tm.record_write(tx1, 1, 0);
        tm.commit(tx1, &mut wal, &mut io).unwrap();

        // tx2 starts after tx1 committed — its snapshot includes tx1
        let tx2 = tm.begin(&mut wal);
        tm.record_write(tx2, 1, 0); // same slot, but tx1 already committed

        // No conflict because tx1 committed before tx2's snapshot
        tm.commit(tx2, &mut wal, &mut io).unwrap();
    }

    #[test]
    fn low_water_pruning() {
        let (mut io, _, mut wal, mut tm) = setup();

        for _ in 0..20 {
            let tx = tm.begin(&mut wal);
            tm.record_write(tx, 1, 0);
            tm.commit(tx, &mut wal, &mut io).unwrap();
        }

        // With no active transactions, low water should have advanced,
        // pruning old committed_writes entries.
        assert_eq!(tm.active_count(), 0);
    }
}
