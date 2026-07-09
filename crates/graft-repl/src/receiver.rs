use graft_txn::wal::{WalBody, WalRecord, WalRecordType};

use crate::protocol::{WalAckMsg, WalBatchMsg};

/// Receives WAL batches and prepares records for application to a shard.
///
/// The receiver lives on the replica shard thread. It deserializes incoming
/// `WalBatchMsg` messages into `WalRecord`s and produces ACKs.
pub struct ReplicationReceiver {
    shard_id: u8,
    /// The last LSN we successfully applied.
    last_applied_lsn: u64,
    /// Records deserialized from batches, ready to apply.
    pending_records: Vec<WalRecord>,
    /// ACKs to send back to the primary.
    pending_acks: Vec<WalAckMsg>,
    /// Set once a batch fails CRC verification or is truncated. A corrupted
    /// batch can leave a gap in the replicated WAL stream (e.g. a lost
    /// PageWrite whose Commit still arrives), and applying anything *after*
    /// the gap — or worse, ACKing it — would let the primary believe this
    /// replica is fully caught up when it is permanently missing data
    /// (silent divergence). Once poisoned, every subsequent batch is
    /// rejected without being applied or ACKed: the replica's replicated
    /// state is stuck exactly where it was at the moment of corruption,
    /// which is observable (ACK'd LSN stops advancing) rather than silent.
    /// There is no in-place repair; recovery requires an operator-driven
    /// full resync (Phase 8g, not yet built).
    poisoned: Option<String>,
}

impl ReplicationReceiver {
    pub fn new(shard_id: u8) -> Self {
        Self {
            shard_id,
            last_applied_lsn: 0,
            pending_records: Vec::new(),
            pending_acks: Vec::new(),
            poisoned: None,
        }
    }

    pub fn new_at(shard_id: u8, last_applied_lsn: u64) -> Self {
        Self {
            shard_id,
            last_applied_lsn,
            pending_records: Vec::new(),
            pending_acks: Vec::new(),
            poisoned: None,
        }
    }

    /// Process an incoming WAL batch from the primary.
    /// Deserializes the raw bytes into WalRecords with CRC verification.
    /// Returns Ok(count) with the number of records deserialized, or Err on
    /// CRC failure/truncation. Once poisoned by a prior corrupted batch,
    /// every call returns Err without applying or ACKing anything (see
    /// `poisoned` field doc and `is_poisoned`).
    pub fn on_batch(&mut self, batch: &WalBatchMsg) -> Result<usize, String> {
        if let Some(ref reason) = self.poisoned {
            return Err(format!(
                "replication receiver poisoned by an earlier corrupted batch ({reason}); \
                 refusing further batches until a manual resync"
            ));
        }

        // Skip batches we've already applied
        if batch.last_lsn <= self.last_applied_lsn && self.last_applied_lsn > 0 {
            return Ok(0);
        }

        let records = match deserialize_wal_records(&batch.records) {
            Ok(r) => r,
            Err(e) => {
                self.poisoned = Some(e.clone());
                return Err(e);
            }
        };
        let count = records.len();

        self.pending_records.extend(records);

        // Track the batch LSN for ACK
        if batch.last_lsn > self.last_applied_lsn {
            self.last_applied_lsn = batch.last_lsn;
        }

        // Queue an ACK
        self.pending_acks.push(WalAckMsg {
            shard_id: self.shard_id,
            acked_lsn: self.last_applied_lsn,
        });

        Ok(count)
    }

    /// True once a corrupted/truncated batch has permanently halted this
    /// receiver. The replica will not apply or ACK anything further; an
    /// operator must resync it from scratch.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.is_some()
    }

    /// Why this receiver stopped, if it did.
    pub fn poison_reason(&self) -> Option<&str> {
        self.poisoned.as_deref()
    }

    /// Drain records that are ready to be applied to the shard.
    pub fn drain_records(&mut self) -> Vec<WalRecord> {
        std::mem::take(&mut self.pending_records)
    }

    /// Drain ACKs ready to be sent to the primary.
    pub fn drain_acks(&mut self) -> Vec<WalAckMsg> {
        std::mem::take(&mut self.pending_acks)
    }

    /// The last LSN we applied.
    pub fn last_applied_lsn(&self) -> u64 {
        self.last_applied_lsn
    }

    pub fn shard_id(&self) -> u8 {
        self.shard_id
    }

    /// Returns true if there are records waiting to be applied.
    pub fn has_pending(&self) -> bool {
        !self.pending_records.is_empty()
    }
}

// ---------------------------------------------------------------------------
// WAL record deserialization (from raw bytes)
// ---------------------------------------------------------------------------

const WAL_HEADER_SIZE: usize = 20;
const WAL_CRC_SIZE: usize = 4;

/// Deserialize raw WAL bytes (as shipped in WalBatchMsg) into WalRecords.
/// Verifies CRC on each record.
pub fn deserialize_wal_records(data: &[u8]) -> Result<Vec<WalRecord>, String> {
    let mut records = Vec::new();
    let mut offset = 0;

    while offset + WAL_HEADER_SIZE + WAL_CRC_SIZE <= data.len() {
        // Read header
        let header = &data[offset..offset + WAL_HEADER_SIZE];
        let tx_id = u64::from_le_bytes(header[0..8].try_into().unwrap());
        let body_len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
        let record_type = WalRecordType::from_u8(header[12])
            .ok_or_else(|| format!("unknown WAL record type: {}", header[12]))?;

        let total_len = WAL_HEADER_SIZE + body_len + WAL_CRC_SIZE;
        if offset + total_len > data.len() {
            return Err("truncated WAL record in batch".into());
        }

        // Verify CRC
        let record_bytes = &data[offset..offset + total_len];
        let stored_crc = u32::from_le_bytes(
            record_bytes[total_len - WAL_CRC_SIZE..total_len]
                .try_into()
                .unwrap(),
        );
        let computed_crc = crc32c::crc32c(&record_bytes[..total_len - WAL_CRC_SIZE]);
        if stored_crc != computed_crc {
            return Err(format!(
                "CRC mismatch at offset {offset}: stored={stored_crc:#x}, computed={computed_crc:#x}"
            ));
        }

        // Parse body
        let body_bytes = &data[offset + WAL_HEADER_SIZE..offset + WAL_HEADER_SIZE + body_len];
        let body = WalBody::read_from(record_type, body_bytes)
            .ok_or_else(|| format!("failed to parse WAL body at offset {offset}"))?;

        records.push(WalRecord {
            lsn: 0, // LSN is relative to the batch; the receiver tracks position separately
            tx_id,
            record_type,
            body,
        });

        offset += total_len;
    }

    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sender::ReplicationSender;
    use graft_core::constants::RECORD_SIZE;

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
                slot: 7,
                page_type: 1,
                data: [0xBB; RECORD_SIZE],
            },
        }
    }

    #[test]
    fn receiver_deserializes_batch() {
        // Create a batch via sender
        let mut sender = ReplicationSender::new(0);
        sender.on_wal_record(make_record(1, WalRecordType::Begin, 0));
        sender.on_wal_record(make_page_write(1, 24, 42));
        sender.on_wal_record(make_record(1, WalRecordType::Commit, 128));
        sender.poll();

        let batches = sender.drain_outbound();
        assert_eq!(batches.len(), 1);

        // Receiver processes the batch
        let mut receiver = ReplicationReceiver::new(0);
        let count = receiver.on_batch(&batches[0]).unwrap();
        assert_eq!(count, 3);

        let records = receiver.drain_records();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].record_type, WalRecordType::Begin);
        assert_eq!(records[0].tx_id, 1);
        assert_eq!(records[1].record_type, WalRecordType::PageWrite);
        assert_eq!(records[2].record_type, WalRecordType::Commit);

        // Verify the page write data survived serialization round-trip
        match &records[1].body {
            WalBody::PageWrite {
                page_id,
                slot,
                page_type,
                data,
            } => {
                assert_eq!(*page_id, 42);
                assert_eq!(*slot, 7);
                assert_eq!(*page_type, 1);
                assert_eq!(data[0], 0xBB);
            }
            _ => panic!("expected PageWrite"),
        }
    }

    #[test]
    fn receiver_produces_ack() {
        let mut sender = ReplicationSender::new(0);
        sender.on_wal_record(make_record(1, WalRecordType::Begin, 0));
        sender.on_wal_record(make_record(1, WalRecordType::Commit, 24));
        sender.poll();

        let batches = sender.drain_outbound();
        let mut receiver = ReplicationReceiver::new(0);
        receiver.on_batch(&batches[0]).unwrap();

        let acks = receiver.drain_acks();
        assert_eq!(acks.len(), 1);
        assert_eq!(acks[0].shard_id, 0);
        assert_eq!(acks[0].acked_lsn, 24);
    }

    #[test]
    fn receiver_detects_crc_corruption() {
        let mut sender = ReplicationSender::new(0);
        sender.on_wal_record(make_record(1, WalRecordType::Begin, 0));
        sender.on_wal_record(make_record(1, WalRecordType::Commit, 24));
        sender.poll();

        let mut batches = sender.drain_outbound();
        // Corrupt a byte
        if let Some(batch) = batches.first_mut() {
            if !batch.records.is_empty() {
                batch.records[5] ^= 0xFF;
            }
        }

        let mut receiver = ReplicationReceiver::new(0);
        let result = receiver.on_batch(&batches[0]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("CRC mismatch"));
        assert!(
            receiver.is_poisoned(),
            "a corrupted batch must poison the receiver"
        );
    }

    /// Regression test for finding #6: a corrupted batch must not be a
    /// silent, self-healing gap. Before the fix, `on_batch`'s error was
    /// discarded and the *next* good batch's higher LSN (and its ACK) would
    /// make the primary believe the replica had everything, even though a
    /// transaction was permanently missing. After the fix, the receiver
    /// must refuse every subsequent batch — even perfectly valid ones —
    /// so neither `drain_records()` nor the ACK stream ever move past the
    /// point of corruption.
    #[test]
    fn corrupted_batch_poisons_receiver_and_blocks_all_later_batches() {
        let mut sender = ReplicationSender::new(0);

        // tx 1 — will arrive corrupted.
        sender.on_wal_record(make_record(1, WalRecordType::Begin, 0));
        sender.on_wal_record(make_record(1, WalRecordType::Commit, 24));
        sender.poll();

        // tx 2 — perfectly valid, higher LSN, arrives after the corrupted batch.
        sender.on_wal_record(make_record(2, WalRecordType::Begin, 48));
        sender.on_wal_record(make_page_write(2, 72, 10));
        sender.on_wal_record(make_record(2, WalRecordType::Commit, 176));
        sender.poll();

        let mut batches = sender.drain_outbound();
        assert_eq!(batches.len(), 2);
        // Corrupt the first (lower-LSN) batch only.
        batches[0].records[5] ^= 0xFF;

        let mut receiver = ReplicationReceiver::new(0);

        let first = receiver.on_batch(&batches[0]);
        assert!(first.is_err(), "corrupted batch must be rejected");
        assert!(receiver.is_poisoned());
        assert!(receiver.poison_reason().unwrap().contains("CRC mismatch"));

        // The later, uncorrupted batch must ALSO be rejected — this is the
        // crux of the fix. Silently accepting it would bridge the gap.
        let second = receiver.on_batch(&batches[1]);
        assert!(
            second.is_err(),
            "once poisoned, even a valid later batch must be rejected, \
             not silently applied over the gap"
        );

        // Nothing from either batch was applied or ACKed — the primary
        // must see this replica stalled, not falsely caught up.
        assert!(receiver.drain_records().is_empty());
        assert!(receiver.drain_acks().is_empty());
        assert_eq!(receiver.last_applied_lsn(), 0);
    }

    #[test]
    fn receiver_multiple_batches() {
        let mut sender = ReplicationSender::new(0);

        // tx 1
        sender.on_wal_record(make_record(1, WalRecordType::Begin, 0));
        sender.on_wal_record(make_record(1, WalRecordType::Commit, 24));
        sender.poll();

        // tx 2
        sender.on_wal_record(make_record(2, WalRecordType::Begin, 48));
        sender.on_wal_record(make_page_write(2, 72, 10));
        sender.on_wal_record(make_record(2, WalRecordType::Commit, 176));
        sender.poll();

        let batches = sender.drain_outbound();
        assert_eq!(batches.len(), 2);

        let mut receiver = ReplicationReceiver::new(0);
        receiver.on_batch(&batches[0]).unwrap();
        receiver.on_batch(&batches[1]).unwrap();

        let records = receiver.drain_records();
        assert_eq!(records.len(), 5); // tx1(2) + tx2(3)

        let acks = receiver.drain_acks();
        assert_eq!(acks.len(), 2);
        assert_eq!(acks[1].acked_lsn, 176);
    }

    #[test]
    fn end_to_end_sender_receiver() {
        // Full pipeline: sender buffers, commits, ships, receiver applies
        let mut sender = ReplicationSender::new(0);
        sender.add_replica("r1".into(), 0, crate::SharedQueue::new());

        // Write a committed transaction
        sender.on_wal_record(make_record(1, WalRecordType::Begin, 0));
        sender.on_wal_record(make_page_write(1, 24, 1));
        sender.on_wal_record(make_page_write(1, 128, 2));
        sender.on_wal_record(make_record(1, WalRecordType::Commit, 232));

        // Write an aborted transaction
        sender.on_wal_record(make_record(2, WalRecordType::Begin, 256));
        sender.on_wal_record(make_page_write(2, 280, 3));
        sender.on_wal_record(make_record(2, WalRecordType::Abort, 384));

        sender.poll();

        let batches = sender.drain_outbound();
        // Only one batch (the committed tx)
        assert_eq!(batches.len(), 1);

        let mut receiver = ReplicationReceiver::new(0);
        let count = receiver.on_batch(&batches[0]).unwrap();
        assert_eq!(count, 4); // Begin + 2 PageWrite + Commit

        // Verify receiver state
        let records = receiver.drain_records();
        assert!(records.iter().all(|r| r.tx_id == 1));
        assert_eq!(records.len(), 4);

        // Send ACK back to sender
        let acks = receiver.drain_acks();
        for ack in &acks {
            sender.on_ack("r1", ack.acked_lsn);
        }
        assert_eq!(sender.min_acked_lsn(), 232);
    }
}
