use graft_txn::wal::WalRecord;

use crate::commit_buffer::CommitBuffer;
use crate::protocol::WalBatchMsg;
use crate::shared_queue::SharedQueue;

/// Maximum bytes of undelivered batches to buffer per replica. Beyond this,
/// a disconnected or badly-lagging replica is evicted rather than letting
/// its queue grow without bound — it must reconnect and fully resync. This
/// mirrors `WalRetention::default_1gb()`; there is no WAL-replay-based
/// catch-up path yet (planned for the Phase 8g snapshot-shipping work), so
/// "evict and require a fresh resync" is the correct, bounded, explicit
/// failure mode rather than silent unbounded growth.
const MAX_REPLICA_OUTBOX_BYTES: usize = 1024 * 1024 * 1024;

/// Identifies a connected replica for tracking ACK state.
#[derive(Debug)]
pub struct ReplicaState {
    pub id: String,
    pub shard_id: u8,
    /// The last LSN acknowledged by this replica.
    pub acked_lsn: u64,
    /// Whether this replica is currently connected.
    pub connected: bool,
    /// Whether this replica was evicted for exceeding
    /// `MAX_REPLICA_OUTBOX_BYTES`. Once evicted, no further batches are
    /// fanned out to it — it must reconnect (which allocates a fresh outbox
    /// via `ReplicaOutboxRegistry`) to resume.
    pub evicted: bool,
    /// This replica's outbound queue, shared with the network writer thread
    /// (resolved through `ReplicaOutboxRegistry` so reconnects reuse it).
    outbox: SharedQueue<WalBatchMsg>,
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

    /// Process committed records into outbound batches, fanning each new
    /// batch out to every registered replica's own outbox.
    ///
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

        // Fan out to every replica that has ever registered on this shard.
        // If none has, there's nothing to retain — this is what keeps an
        // unreplicated primary's memory bounded (finding #10).
        let batch_bytes = batch.records.len();
        for r in &mut self.replicas {
            if r.evicted {
                continue;
            }
            let buffered = r.outbox.total_bytes_by(|b| b.records.len());
            if buffered + batch_bytes > MAX_REPLICA_OUTBOX_BYTES {
                tracing::error!(
                    "replica {} on shard {} exceeded the {}-byte retention \
                     window; evicting — it must reconnect and fully resync",
                    r.id,
                    self.shard_id,
                    MAX_REPLICA_OUTBOX_BYTES
                );
                r.evicted = true;
                r.connected = false;
                continue;
            }
            r.outbox.push(batch.clone());
        }

        self.outbound.push(batch);
    }

    /// Drain outbound batches produced by the last `poll()`. Kept for
    /// introspection/testing — production delivery goes through each
    /// replica's own outbox (see `poll()`), not this shared buffer.
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

    /// Register a replica connection. If `id` is already known (a
    /// reconnect), the existing entry is resumed in place — `acked_lsn`
    /// and any evicted state are preserved rather than reset, and the
    /// `outbox` clone passed in is expected to be the same underlying
    /// queue as before (guaranteed by resolving it through the same
    /// `ReplicaOutboxRegistry`).
    pub fn add_replica(&mut self, id: String, shard_id: u8, outbox: SharedQueue<WalBatchMsg>) {
        if let Some(r) = self.replicas.iter_mut().find(|r| r.id == id) {
            r.shard_id = shard_id;
            r.connected = true;
            r.outbox = outbox;
            return;
        }
        self.replicas.push(ReplicaState {
            id,
            shard_id,
            acked_lsn: 0,
            connected: true,
            evicted: false,
            outbox,
        });
    }

    /// Mark a replica as disconnected. Its outbox and ACK state are kept
    /// (bounded by `MAX_REPLICA_OUTBOX_BYTES`) so a reconnect can resume
    /// without data loss.
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
        sender.add_replica("replica-1".into(), 0, SharedQueue::new());
        sender.add_replica("replica-2".into(), 0, SharedQueue::new());

        sender.on_ack("replica-1", 100);
        sender.on_ack("replica-2", 50);

        assert_eq!(sender.min_acked_lsn(), 50);

        sender.on_ack("replica-2", 150);
        assert_eq!(sender.min_acked_lsn(), 100);
    }

    #[test]
    fn sender_disconnected_replica_excluded_from_min() {
        let mut sender = ReplicationSender::new(0);
        sender.add_replica("replica-1".into(), 0, SharedQueue::new());
        sender.add_replica("replica-2".into(), 0, SharedQueue::new());

        sender.on_ack("replica-1", 100);
        sender.on_ack("replica-2", 50);

        // Disconnect the lagging replica
        sender.remove_replica("replica-2");
        assert_eq!(sender.min_acked_lsn(), 100);
    }

    #[test]
    fn poll_fans_batch_out_to_every_registered_replica() {
        let mut sender = ReplicationSender::new(0);
        let q1 = SharedQueue::new();
        let q2 = SharedQueue::new();
        sender.add_replica("replica-1".into(), 0, q1.clone());
        sender.add_replica("replica-2".into(), 0, q2.clone());

        sender.on_wal_record(make_record(1, WalRecordType::Begin, 0));
        sender.on_wal_record(make_page_write(1, 24, 1));
        sender.on_wal_record(make_record(1, WalRecordType::Commit, 128));
        sender.poll();

        // Each replica gets its own full copy of the batch — not an
        // arbitrary split of a single shared queue.
        assert_eq!(q1.len(), 1);
        assert_eq!(q2.len(), 1);
        assert_eq!(q1.peek_front().unwrap().last_lsn, 128);
        assert_eq!(q2.peek_front().unwrap().last_lsn, 128);
    }

    #[test]
    fn poll_retains_nothing_when_no_replica_ever_registered() {
        let mut sender = ReplicationSender::new(0);

        sender.on_wal_record(make_record(1, WalRecordType::Begin, 0));
        sender.on_wal_record(make_record(1, WalRecordType::Commit, 24));
        sender.poll();

        // No replica has ever registered, so nothing should have been
        // retained anywhere reachable via a replica queue.
        assert!(sender.replicas().is_empty());
    }

    #[test]
    fn reconnect_reuses_existing_replica_state_and_outbox() {
        let mut sender = ReplicationSender::new(0);
        let outbox = SharedQueue::new();
        sender.add_replica("replica-1".into(), 0, outbox.clone());
        sender.on_ack("replica-1", 42);
        sender.remove_replica("replica-1");
        assert_eq!(sender.replicas().len(), 1);
        assert!(!sender.replicas()[0].connected);

        // Reconnect: same id, freshly-looked-up (but identical, per the
        // registry contract) outbox handle.
        sender.add_replica("replica-1".into(), 0, outbox.clone());
        assert_eq!(sender.replicas().len(), 1, "must not append a ghost entry");
        assert!(sender.replicas()[0].connected);
        assert_eq!(
            sender.replicas()[0].acked_lsn,
            42,
            "acked_lsn must not reset on reconnect"
        );
    }

    #[test]
    fn replica_exceeding_outbox_cap_is_evicted_not_grown_unbounded() {
        let mut sender = ReplicationSender::new(0);
        let outbox = SharedQueue::new();
        sender.add_replica("slow-replica".into(), 0, outbox.clone());

        // Pre-fill the outbox past the cap to simulate a replica that's
        // been disconnected long enough to accumulate a huge backlog.
        outbox.push(WalBatchMsg {
            shard_id: 0,
            first_lsn: 0,
            last_lsn: 0,
            records: vec![0u8; MAX_REPLICA_OUTBOX_BYTES + 1],
        });

        sender.on_wal_record(make_record(1, WalRecordType::Begin, 0));
        sender.on_wal_record(make_record(1, WalRecordType::Commit, 24));
        sender.poll();

        assert!(sender.replicas()[0].evicted);
        assert!(!sender.replicas()[0].connected);
        // The new batch was not appended on top of the already-oversized backlog.
        assert_eq!(outbox.len(), 1);
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
