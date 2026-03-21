use graft_core::TxId;
use graft_txn::wal::{WalRecord, WalRecordType};

/// Buffers WAL records per-transaction.
///
/// The primary uses this to collect WAL records as they're written and only
/// release them for replication after the transaction commits. Aborted
/// transactions are discarded — replicas never see uncommitted records.
pub struct CommitBuffer {
    /// Records buffered per transaction, keyed by tx_id.
    pending: Vec<(TxId, Vec<WalRecord>)>,
    /// Committed records ready to be shipped, in commit order.
    ready: Vec<WalRecord>,
}

impl CommitBuffer {
    pub fn new() -> Self {
        Self {
            pending: Vec::new(),
            ready: Vec::new(),
        }
    }

    /// Record a WAL record for a transaction. Called as the primary writes
    /// WAL records during query execution.
    pub fn append(&mut self, record: WalRecord) {
        let tx_id = record.tx_id;

        match record.record_type {
            WalRecordType::Begin => {
                self.pending.push((tx_id, vec![record]));
            }
            WalRecordType::Commit => {
                // Find the pending tx and move all its records to ready
                if let Some(pos) = self.pending.iter().position(|(id, _)| *id == tx_id) {
                    let (_, mut records) = self.pending.swap_remove(pos);
                    records.push(record);
                    self.ready.append(&mut records);
                }
            }
            WalRecordType::Abort => {
                // Discard the entire transaction
                if let Some(pos) = self.pending.iter().position(|(id, _)| *id == tx_id) {
                    self.pending.swap_remove(pos);
                }
            }
            _ => {
                // Data record (PageWrite, PageClear, etc.) — append to pending tx
                if let Some(entry) = self.pending.iter_mut().find(|(id, _)| *id == tx_id) {
                    entry.1.push(record);
                }
            }
        }
    }

    /// Drain all committed records that are ready for shipping.
    /// Returns them in commit order.
    pub fn drain_ready(&mut self) -> Vec<WalRecord> {
        std::mem::take(&mut self.ready)
    }

    /// Returns true if there are committed records ready to ship.
    pub fn has_ready(&self) -> bool {
        !self.ready.is_empty()
    }

    /// Number of transactions currently buffered (not yet committed or aborted).
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Number of committed records ready to ship.
    pub fn ready_count(&self) -> usize {
        self.ready.len()
    }
}

impl Default for CommitBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graft_txn::wal::WalBody;

    fn make_record(tx_id: TxId, record_type: WalRecordType, lsn: u64) -> WalRecord {
        WalRecord {
            lsn,
            tx_id,
            record_type,
            body: WalBody::Empty,
        }
    }

    fn make_page_write(tx_id: TxId, lsn: u64, page_id: u64) -> WalRecord {
        WalRecord {
            lsn,
            tx_id,
            record_type: WalRecordType::PageWrite,
            body: WalBody::PageWrite {
                page_id,
                slot: 0,
                page_type: 1,
                data: [0u8; graft_core::constants::RECORD_SIZE],
            },
        }
    }

    #[test]
    fn commit_releases_records() {
        let mut buf = CommitBuffer::new();

        buf.append(make_record(1, WalRecordType::Begin, 0));
        buf.append(make_page_write(1, 24, 1));
        buf.append(make_page_write(1, 128, 2));
        assert!(!buf.has_ready());
        assert_eq!(buf.pending_count(), 1);

        buf.append(make_record(1, WalRecordType::Commit, 232));
        assert!(buf.has_ready());
        assert_eq!(buf.pending_count(), 0);

        let ready = buf.drain_ready();
        assert_eq!(ready.len(), 4); // Begin + 2 PageWrite + Commit
        assert_eq!(ready[0].record_type, WalRecordType::Begin);
        assert_eq!(ready[1].record_type, WalRecordType::PageWrite);
        assert_eq!(ready[2].record_type, WalRecordType::PageWrite);
        assert_eq!(ready[3].record_type, WalRecordType::Commit);
    }

    #[test]
    fn abort_discards_records() {
        let mut buf = CommitBuffer::new();

        buf.append(make_record(1, WalRecordType::Begin, 0));
        buf.append(make_page_write(1, 24, 1));
        buf.append(make_record(1, WalRecordType::Abort, 128));

        assert!(!buf.has_ready());
        assert_eq!(buf.pending_count(), 0);
        assert_eq!(buf.ready_count(), 0);
    }

    #[test]
    fn interleaved_transactions() {
        let mut buf = CommitBuffer::new();

        // tx 1 begins
        buf.append(make_record(1, WalRecordType::Begin, 0));
        buf.append(make_page_write(1, 24, 10));

        // tx 2 begins
        buf.append(make_record(2, WalRecordType::Begin, 128));
        buf.append(make_page_write(2, 152, 20));

        // tx 1 commits
        buf.append(make_record(1, WalRecordType::Commit, 256));

        // tx 2 aborts
        buf.append(make_record(2, WalRecordType::Abort, 280));

        // Only tx 1 records should be ready
        assert!(buf.has_ready());
        let ready = buf.drain_ready();
        assert_eq!(ready.len(), 3); // Begin + PageWrite + Commit
        assert!(ready.iter().all(|r| r.tx_id == 1));
        assert_eq!(buf.pending_count(), 0);
    }

    #[test]
    fn multiple_commits_accumulate() {
        let mut buf = CommitBuffer::new();

        // tx 1
        buf.append(make_record(1, WalRecordType::Begin, 0));
        buf.append(make_record(1, WalRecordType::Commit, 24));

        // tx 2
        buf.append(make_record(2, WalRecordType::Begin, 48));
        buf.append(make_page_write(2, 72, 5));
        buf.append(make_record(2, WalRecordType::Commit, 176));

        let ready = buf.drain_ready();
        assert_eq!(ready.len(), 5); // tx1(2) + tx2(3)

        // Draining clears the buffer
        assert!(!buf.has_ready());
        assert_eq!(buf.drain_ready().len(), 0);
    }

    #[test]
    fn drain_is_idempotent() {
        let mut buf = CommitBuffer::new();
        buf.append(make_record(1, WalRecordType::Begin, 0));
        buf.append(make_record(1, WalRecordType::Commit, 24));

        let r1 = buf.drain_ready();
        assert_eq!(r1.len(), 2);

        let r2 = buf.drain_ready();
        assert_eq!(r2.len(), 0);
    }
}
