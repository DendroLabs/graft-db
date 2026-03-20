use graft_core::TxId;

/// A point-in-time view of the database for snapshot isolation.
///
/// Captured at transaction begin. Records the transaction timestamp and which
/// transactions were still active (uncommitted) at that moment. Visibility
/// checks use this to determine which record versions a transaction can see.
#[derive(Clone, Debug)]
pub struct Snapshot {
    /// Our transaction's ID.
    pub ts: TxId,
    /// TxIds that were active (not yet committed or aborted) when this
    /// snapshot was taken. Records written by these transactions are
    /// invisible, even if their TxId < `ts`.
    pub active: Vec<TxId>,
}

impl Snapshot {
    /// Is a record version visible under this snapshot?
    ///
    /// A record has `tx_min` (creating transaction) and `tx_max` (deleting
    /// transaction, 0 if alive). The record is visible iff:
    ///
    /// 1. `tx_min` committed before our snapshot (tx_min < ts AND tx_min
    ///    was not in the active set), AND
    /// 2. The record has not been deleted by a committed transaction visible
    ///    to us (tx_max == 0 OR tx_max >= ts OR tx_max was in the active set).
    #[inline]
    pub fn is_visible(&self, tx_min: TxId, tx_max: TxId) -> bool {
        let creator_visible = tx_min < self.ts && !self.active.contains(&tx_min);
        if !creator_visible {
            return false;
        }
        // Not deleted, or deleted by a transaction we can't see
        tx_max == 0 || tx_max >= self.ts || self.active.contains(&tx_max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(ts: TxId, active: &[TxId]) -> Snapshot {
        Snapshot {
            ts,
            active: active.to_vec(),
        }
    }

    #[test]
    fn visible_committed_record() {
        let s = snap(10, &[]);
        // Created by tx 5 (committed), not deleted
        assert!(s.is_visible(5, 0));
    }

    #[test]
    fn invisible_future_record() {
        let s = snap(10, &[]);
        // Created by tx 15 — after our snapshot
        assert!(!s.is_visible(15, 0));
    }

    #[test]
    fn invisible_active_creator() {
        let s = snap(10, &[7]);
        // Created by tx 7 which was active at snapshot time
        assert!(!s.is_visible(7, 0));
    }

    #[test]
    fn deleted_before_snapshot() {
        let s = snap(10, &[]);
        // Created by tx 3, deleted by tx 8 (both committed before snapshot)
        assert!(!s.is_visible(3, 8));
    }

    #[test]
    fn deleted_after_snapshot() {
        let s = snap(10, &[]);
        // Created by tx 3, deleted by tx 12 (after snapshot — not visible to us)
        assert!(s.is_visible(3, 12));
    }

    #[test]
    fn deleted_by_active_tx() {
        let s = snap(10, &[8]);
        // Created by tx 3, deleted by tx 8 which is still active
        assert!(s.is_visible(3, 8));
    }

    #[test]
    fn own_writes_invisible() {
        // Our own tx_id is ts=10. Records we create have tx_min=10.
        // Since tx_min(10) is NOT < ts(10), they are invisible via snapshot.
        // This is correct — own writes need special handling in the executor.
        let s = snap(10, &[]);
        assert!(!s.is_visible(10, 0));
    }
}
