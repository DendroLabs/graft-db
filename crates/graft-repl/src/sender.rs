use graft_txn::wal::WalRecord;

use crate::commit_buffer::CommitBuffer;
use crate::protocol::WalBatchMsg;

/// Identifies a connected replica for tracking ACK state.
#[derive(Clone, Debug)]
pub struct ReplicaState {
    pub id: String,
    pub shard_id: u8,
    /// The last LSN acknowledged by this replica.
    pub acked_lsn: u64,
    /// Whether this replica is currently connected.
    pub connected: bool,
}

/// Ships committed WAL records to replicas.
///
/// The sender lives on the primary shard thread. It collects committed WAL
/// records from the `CommitBuffer` and produces `WalBatchMsg` messages to
/// be sent to replicas over the network.
pub struct ReplicationSender {
    shard_id: u8,
    commit_buffer: CommitBuffer,
    /// Outbound queue of batches ready to send.
    outbound: Vec<WalBatchMsg>,
    /// Per-replica ACK tracking.
    replicas: Vec<ReplicaState>,
    /// The LSN of the last record we shipped.
    last_shipped_lsn: u64,
}

impl ReplicationSender {
    pub fn new(shard_id: u8) -> Self {
        Self {
            shard_id,
            commit_buffer: CommitBuffer::new(),
            outbound: Vec::new(),
            replicas: Vec::new(),
            last_shipped_lsn: 0,
        }
    }

    /// Feed a WAL record into the commit buffer.
    pub fn on_wal_record(&mut self, record: WalRecord) {
        self.commit_buffer.append(record);
    }

    /// Process committed records into outbound batches.
    /// Call this periodically (e.g., in the event loop's poll_replication phase).
    pub fn poll(&mut self) {
        if !self.commit_buffer.has_ready() {
            return;
        }

        let records = self.commit_buffer.drain_ready();
        if records.is_empty() {
            return;
        }

        let batch = Self::records_to_batch(self.shard_id, &records);
        if batch.last_lsn > self.last_shipped_lsn {
            self.last_shipped_lsn = batch.last_lsn;
        }
        self.outbound.push(batch);
    }

    /// Drain outbound batches ready for network transmission.
    pub fn drain_outbound(&mut self) -> Vec<WalBatchMsg> {
        std::mem::take(&mut self.outbound)
    }

    /// Record an ACK from a replica.
    pub fn on_ack(&mut self, replica_id: &str, acked_lsn: u64) {
        if let Some(r) = self.replicas.iter_mut().find(|r| r.id == replica_id) {
            if acked_lsn > r.acked_lsn {
                r.acked_lsn = acked_lsn;
            }
        }
    }

    /// Register a new replica connection.
    pub fn add_replica(&mut self, id: String, shard_id: u8, last_lsn: u64) {
        self.replicas.push(ReplicaState {
            id,
            shard_id,
            acked_lsn: last_lsn,
            connected: true,
        });
    }

    /// Mark a replica as disconnected.
    pub fn remove_replica(&mut self, id: &str) {
        if let Some(r) = self.replicas.iter_mut().find(|r| r.id == id) {
            r.connected = false;
        }
    }

    /// The minimum ACK'd LSN across all connected replicas.
    /// Used for WAL retention decisions.
    pub fn min_acked_lsn(&self) -> u64 {
        self.replicas
            .iter()
            .filter(|r| r.connected)
            .map(|r| r.acked_lsn)
            .min()
            .unwrap_or(self.last_shipped_lsn)
    }

    /// Reference to replica states.
    pub fn replicas(&self) -> &[ReplicaState] {
        &self.replicas
    }

    pub fn last_shipped_lsn(&self) -> u64 {
        self.last_shipped_lsn
    }

    pub fn commit_buffer(&self) -> &CommitBuffer {
        &self.commit_buffer
    }

    /// Serialize WAL records into a WalBatchMsg with raw bytes.
    fn records_to_batch(shard_id: u8, records: &[WalRecord]) -> WalBatchMsg {
        let first_lsn = records.first().map(|r| r.lsn).unwrap_or(0);
        let last_lsn = records.last().map(|r| r.lsn).unwrap_or(0);

        // Serialize records into raw bytes using the same WAL format.
        // Each record: header(20) + body(variable) + crc(4)
        let mut raw = Vec::new();
        for record in records {
            serialize_wal_record(record, &mut raw);
        }

        WalBatchMsg {
            shard_id,
            first_lsn,
            last_lsn,
            records: raw,
        }
    }
}

/// Serialize a WalRecord to raw bytes in the same format as the WAL file.
/// This allows replicas to apply records using the same WalReader logic.
fn serialize_wal_record(record: &WalRecord, buf: &mut Vec<u8>) {
    let body_len = record.body.serialized_len() as u32;

    // Header: tx_id(8) + body_len(4) + record_type(1) + pad(3) + reserved(4) = 20
    let start = buf.len();
    buf.extend_from_slice(&record.tx_id.to_le_bytes());
    buf.extend_from_slice(&body_len.to_le_bytes());
    buf.push(record.record_type as u8);
    buf.extend_from_slice(&[0u8; 3]); // pad
    buf.extend_from_slice(&[0u8; 4]); // reserved

    record.body.write_to(buf);

    // CRC over header + body
    let crc = crc32c::crc32c(&buf[start..]);
    buf.extend_from_slice(&crc.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use graft_core::constants::RECORD_SIZE;
    use graft_txn::wal::{WalBody, WalRecordType};

    fn make_record(tx_id: u64, record_type: WalRecordType, lsn: u64) -> WalRecord {
        WalRecord {
            lsn,
            tx_id,
            record_type,
            body: WalBody::Empty,
        }
    }

    fn make_page_write(tx_id: u64, lsn: u64, page_id: u64) -> WalRecord {
        WalRecord {
            lsn,
            tx_id,
            record_type: WalRecordType::PageWrite,
            body: WalBody::PageWrite {
                page_id,
                slot: 0,
                page_type: 1,
                data: [0xAA; RECORD_SIZE],
            },
        }
    }

    #[test]
    fn sender_produces_batch_after_commit() {
        let mut sender = ReplicationSender::new(0);

        sender.on_wal_record(make_record(1, WalRecordType::Begin, 0));
        sender.on_wal_record(make_page_write(1, 24, 1));
        sender.on_wal_record(make_record(1, WalRecordType::Commit, 128));

        sender.poll();

        let batches = sender.drain_outbound();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].shard_id, 0);
        assert_eq!(batches[0].first_lsn, 0);
        assert_eq!(batches[0].last_lsn, 128);
        assert!(!batches[0].records.is_empty());
    }

    #[test]
    fn sender_ignores_aborted_tx() {
        let mut sender = ReplicationSender::new(0);

        sender.on_wal_record(make_record(1, WalRecordType::Begin, 0));
        sender.on_wal_record(make_page_write(1, 24, 1));
        sender.on_wal_record(make_record(1, WalRecordType::Abort, 128));

        sender.poll();

        let batches = sender.drain_outbound();
        assert_eq!(batches.len(), 0);
    }

    #[test]
    fn sender_tracks_replica_acks() {
        let mut sender = ReplicationSender::new(0);
        sender.add_replica("replica-1".into(), 0, 0);
        sender.add_replica("replica-2".into(), 0, 0);

        sender.on_ack("replica-1", 100);
        sender.on_ack("replica-2", 50);

        assert_eq!(sender.min_acked_lsn(), 50);

        sender.on_ack("replica-2", 150);
        assert_eq!(sender.min_acked_lsn(), 100);
    }

    #[test]
    fn sender_disconnected_replica_excluded_from_min() {
        let mut sender = ReplicationSender::new(0);
        sender.add_replica("replica-1".into(), 0, 0);
        sender.add_replica("replica-2".into(), 0, 0);

        sender.on_ack("replica-1", 100);
        sender.on_ack("replica-2", 50);

        // Disconnect the lagging replica
        sender.remove_replica("replica-2");
        assert_eq!(sender.min_acked_lsn(), 100);
    }

    #[test]
    fn serialized_records_have_valid_crc() {
        let record = make_page_write(1, 0, 42);
        let mut buf = Vec::new();
        serialize_wal_record(&record, &mut buf);

        // Verify CRC: header(20) + body(80) + crc(4) = 104
        assert_eq!(buf.len(), 104);

        let crc_offset = buf.len() - 4;
        let stored_crc = u32::from_le_bytes(buf[crc_offset..].try_into().unwrap());
        let computed_crc = crc32c::crc32c(&buf[..crc_offset]);
        assert_eq!(stored_crc, computed_crc);
    }
}
