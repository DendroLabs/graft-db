use graft_core::constants::{RECORD_SIZE, WAL_BUFFER_SIZE};
use graft_core::{PageId, TxId};
use graft_io::{FileHandle, IoBackend};
use std::io;

// ---------------------------------------------------------------------------
// WAL record format (on disk)
// ---------------------------------------------------------------------------
//
// [header: 20 bytes] [body: variable] [crc32c: 4 bytes]
//
// Header:
//   tx_id:        u64 (8)
//   body_len:     u32 (4)
//   record_type:  u8  (1)
//   _pad:         [u8; 3] (3)
//   header_reserved: u32 (4)
//
// CRC covers header + body.

const WAL_HEADER_SIZE: usize = 20;
const WAL_CRC_SIZE: usize = 4;

// ---------------------------------------------------------------------------
// WalRecordType
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum WalRecordType {
    Begin = 1,
    Commit = 2,
    Abort = 3,
    /// Redo record: write `data` into `(page_id, slot)`.
    PageWrite = 10,
    /// Redo record: free slot `(page_id, slot)`.
    PageClear = 11,
    Checkpoint = 20,
}

impl WalRecordType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Begin),
            2 => Some(Self::Commit),
            3 => Some(Self::Abort),
            10 => Some(Self::PageWrite),
            11 => Some(Self::PageClear),
            20 => Some(Self::Checkpoint),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// WalRecord (in memory)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct WalRecord {
    /// Byte offset of this record in the WAL file (set during read/write).
    pub lsn: u64,
    pub tx_id: TxId,
    pub record_type: WalRecordType,
    pub body: WalBody,
}

#[derive(Clone, Debug)]
pub enum WalBody {
    Empty,
    PageWrite {
        page_id: PageId,
        slot: u16,
        page_type: u8,
        data: [u8; RECORD_SIZE],
    },
    PageClear {
        page_id: PageId,
        slot: u16,
        page_type: u8,
    },
}

impl WalBody {
    pub fn serialized_len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::PageWrite { .. } => 8 + 2 + 6 + RECORD_SIZE, // 80
            Self::PageClear { .. } => 8 + 2 + 6,               // 16
        }
    }

    pub fn write_to(&self, buf: &mut Vec<u8>) {
        match self {
            Self::Empty => {}
            Self::PageWrite {
                page_id,
                slot,
                page_type,
                data,
            } => {
                buf.extend_from_slice(&page_id.to_le_bytes()); // 8
                buf.extend_from_slice(&slot.to_le_bytes()); // 2
                buf.push(*page_type); // 1
                buf.extend_from_slice(&[0u8; 5]); // 5 padding
                buf.extend_from_slice(data); // 64
            }
            Self::PageClear {
                page_id,
                slot,
                page_type,
            } => {
                buf.extend_from_slice(&page_id.to_le_bytes());
                buf.extend_from_slice(&slot.to_le_bytes());
                buf.push(*page_type);
                buf.extend_from_slice(&[0u8; 5]);
            }
        }
    }

    pub fn read_from(record_type: WalRecordType, data: &[u8]) -> Option<Self> {
        match record_type {
            WalRecordType::Begin
            | WalRecordType::Commit
            | WalRecordType::Abort
            | WalRecordType::Checkpoint => Some(Self::Empty),
            WalRecordType::PageWrite => {
                if data.len() < 16 + RECORD_SIZE {
                    return None;
                }
                let page_id = u64::from_le_bytes(data[0..8].try_into().unwrap());
                let slot = u16::from_le_bytes(data[8..10].try_into().unwrap());
                let page_type = data[10];
                let mut rec_data = [0u8; RECORD_SIZE];
                rec_data.copy_from_slice(&data[16..16 + RECORD_SIZE]);
                Some(Self::PageWrite {
                    page_id,
                    slot,
                    page_type,
                    data: rec_data,
                })
            }
            WalRecordType::PageClear => {
                if data.len() < 11 {
                    return None;
                }
                let page_id = u64::from_le_bytes(data[0..8].try_into().unwrap());
                let slot = u16::from_le_bytes(data[8..10].try_into().unwrap());
                let page_type = data[10];
                Some(Self::PageClear {
                    page_id,
                    slot,
                    page_type,
                })
            }
        }
    }
}

// ---------------------------------------------------------------------------
// WalWriter
// ---------------------------------------------------------------------------

/// Buffered write-ahead log writer with group commit support.
///
/// Records accumulate in a 64 KB buffer. `flush()` writes the buffer to disk
/// and calls `sync()`. The caller decides when to flush — the transaction
/// manager flushes at commit time or after the group commit window expires.
pub struct WalWriter {
    handle: FileHandle,
    buffer: Vec<u8>,
    /// Current write position in the WAL file.
    file_offset: u64,
    /// File offset after the last successful sync.
    flushed_offset: u64,
}

impl WalWriter {
    pub fn new(handle: FileHandle) -> Self {
        Self {
            handle,
            buffer: Vec::with_capacity(WAL_BUFFER_SIZE),
            file_offset: 0,
            flushed_offset: 0,
        }
    }

    /// Resume writing at the given offset (for recovery).
    pub fn new_at(handle: FileHandle, offset: u64) -> Self {
        Self {
            handle,
            buffer: Vec::with_capacity(WAL_BUFFER_SIZE),
            file_offset: offset,
            flushed_offset: offset,
        }
    }

    /// Append a WAL record to the buffer. Returns the record's LSN (byte
    /// offset in the WAL file).
    pub fn append(&mut self, tx_id: TxId, record_type: WalRecordType, body: &WalBody) -> u64 {
        let lsn = self.file_offset + self.buffer.len() as u64;
        let body_len = body.serialized_len() as u32;

        // Header: tx_id(8) + body_len(4) + record_type(1) + pad(3) + reserved(4) = 20
        self.buffer.extend_from_slice(&tx_id.to_le_bytes());
        self.buffer.extend_from_slice(&body_len.to_le_bytes());
        self.buffer.push(record_type as u8);
        self.buffer.extend_from_slice(&[0u8; 3]); // pad
        self.buffer.extend_from_slice(&[0u8; 4]); // reserved

        let crc_start = self.buffer.len() - WAL_HEADER_SIZE;
        body.write_to(&mut self.buffer);

        // CRC over header + body
        let crc = crc32c::crc32c(&self.buffer[crc_start..]);
        self.buffer.extend_from_slice(&crc.to_le_bytes());

        lsn
    }

    /// Write the buffer to the OS page cache (no fsync).
    ///
    /// Data is durable against process crashes (the OS has it) but NOT
    /// against power failure until `sync()` is called. This is the
    /// group-commit fast path: multiple transactions can `write()`,
    /// then a single `sync()` makes them all durable at once.
    pub fn write(&mut self, io: &mut dyn IoBackend) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let written = io.write_at(self.handle, self.file_offset, &self.buffer)?;
        debug_assert_eq!(written, self.buffer.len());
        self.file_offset += self.buffer.len() as u64;
        self.buffer.clear();
        Ok(())
    }

    /// Fsync the WAL file, making all previously written data durable
    /// against power failure.
    pub fn sync(&mut self, io: &mut dyn IoBackend) -> io::Result<()> {
        io.sync(self.handle)?;
        self.flushed_offset = self.file_offset;
        Ok(())
    }

    /// Write the buffer to disk AND fsync (equivalent to `write()` + `sync()`).
    ///
    /// Use this when you need immediate durability (e.g., full checkpoint).
    /// For normal transaction commits, prefer `write()` with periodic `sync()`
    /// for group-commit throughput.
    pub fn flush(&mut self, io: &mut dyn IoBackend) -> io::Result<()> {
        self.write(io)?;
        self.sync(io)?;
        Ok(())
    }

    /// Returns true if the buffer should be flushed (exceeds threshold).
    pub fn should_flush(&self) -> bool {
        self.buffer.len() >= WAL_BUFFER_SIZE
    }

    /// Byte offset of the last durably flushed position.
    pub fn flushed_offset(&self) -> u64 {
        self.flushed_offset
    }

    /// Total bytes written (including buffered, unflushed data).
    pub fn current_offset(&self) -> u64 {
        self.file_offset + self.buffer.len() as u64
    }
}

// ---------------------------------------------------------------------------
// WalReader
// ---------------------------------------------------------------------------

/// Reads WAL records sequentially for crash recovery.
pub struct WalReader {
    handle: FileHandle,
    offset: u64,
    file_size: u64,
}

impl WalReader {
    pub fn new(handle: FileHandle, file_size: u64) -> Self {
        Self {
            handle,
            offset: 0,
            file_size,
        }
    }

    /// Read the next WAL record. Returns `None` at EOF or on a torn/partial
    /// record (which marks the end of the valid WAL).
    pub fn next(&mut self, io: &mut dyn IoBackend) -> Option<WalRecord> {
        // Need at least header + crc
        if self.offset + (WAL_HEADER_SIZE + WAL_CRC_SIZE) as u64 > self.file_size {
            return None;
        }

        // Read header
        let mut header = [0u8; WAL_HEADER_SIZE];
        if io.read_at(self.handle, self.offset, &mut header).ok()? < WAL_HEADER_SIZE {
            return None;
        }

        let tx_id = u64::from_le_bytes(header[0..8].try_into().unwrap());
        let body_len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
        let record_type = WalRecordType::from_u8(header[12])?;

        let total_len = WAL_HEADER_SIZE + body_len + WAL_CRC_SIZE;
        if self.offset + total_len as u64 > self.file_size {
            return None; // torn record
        }

        // Read body + crc
        let mut payload = vec![0u8; body_len + WAL_CRC_SIZE];
        if io
            .read_at(
                self.handle,
                self.offset + WAL_HEADER_SIZE as u64,
                &mut payload,
            )
            .ok()?
            < payload.len()
        {
            return None;
        }

        // Verify CRC
        let stored_crc = u32::from_le_bytes(payload[body_len..body_len + 4].try_into().unwrap());
        let mut computed = crc32c::crc32c(&header);
        computed = crc32c::crc32c_append(computed, &payload[..body_len]);
        if stored_crc != computed {
            return None; // corrupt record — treat as end of valid WAL
        }

        let body = WalBody::read_from(record_type, &payload[..body_len])?;

        let lsn = self.offset;
        self.offset += total_len as u64;

        Some(WalRecord {
            lsn,
            tx_id,
            record_type,
            body,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graft_io::sim::SimIoBackend;
    use graft_io::OpenOptions;
    use std::path::PathBuf;

    fn wal_path() -> PathBuf {
        PathBuf::from("/sim/shard0.wal")
    }

    fn setup() -> (SimIoBackend, FileHandle) {
        let mut io = SimIoBackend::new(42);
        let fh = io
            .open(&wal_path(), &OpenOptions::create_read_write())
            .unwrap();
        (io, fh)
    }

    #[test]
    fn write_and_read_begin_commit() {
        let (mut io, fh) = setup();
        let mut writer = WalWriter::new(fh);

        writer.append(1, WalRecordType::Begin, &WalBody::Empty);
        writer.append(1, WalRecordType::Commit, &WalBody::Empty);
        writer.flush(&mut io).unwrap();

        let file_size = io.file_size(fh).unwrap();
        let mut reader = WalReader::new(fh, file_size);

        let r1 = reader.next(&mut io).unwrap();
        assert_eq!(r1.tx_id, 1);
        assert_eq!(r1.record_type, WalRecordType::Begin);
        assert_eq!(r1.lsn, 0);

        let r2 = reader.next(&mut io).unwrap();
        assert_eq!(r2.tx_id, 1);
        assert_eq!(r2.record_type, WalRecordType::Commit);

        assert!(reader.next(&mut io).is_none()); // EOF
    }

    #[test]
    fn write_and_read_page_write() {
        let (mut io, fh) = setup();
        let mut writer = WalWriter::new(fh);

        let mut data = [0u8; RECORD_SIZE];
        data[0] = 0xAA;
        data[63] = 0xBB;

        writer.append(
            5,
            WalRecordType::PageWrite,
            &WalBody::PageWrite {
                page_id: 42,
                slot: 7,
                page_type: 1, // Node
                data,
            },
        );
        writer.flush(&mut io).unwrap();

        let file_size = io.file_size(fh).unwrap();
        let mut reader = WalReader::new(fh, file_size);
        let rec = reader.next(&mut io).unwrap();

        assert_eq!(rec.tx_id, 5);
        assert_eq!(rec.record_type, WalRecordType::PageWrite);
        match rec.body {
            WalBody::PageWrite {
                page_id,
                slot,
                page_type,
                data: d,
            } => {
                assert_eq!(page_id, 42);
                assert_eq!(slot, 7);
                assert_eq!(page_type, 1);
                assert_eq!(d[0], 0xAA);
                assert_eq!(d[63], 0xBB);
            }
            _ => panic!("wrong body type"),
        }
    }

    #[test]
    fn write_and_read_page_clear() {
        let (mut io, fh) = setup();
        let mut writer = WalWriter::new(fh);

        writer.append(
            3,
            WalRecordType::PageClear,
            &WalBody::PageClear {
                page_id: 10,
                slot: 99,
                page_type: 2, // Edge
            },
        );
        writer.flush(&mut io).unwrap();

        let file_size = io.file_size(fh).unwrap();
        let mut reader = WalReader::new(fh, file_size);
        let rec = reader.next(&mut io).unwrap();

        assert_eq!(rec.record_type, WalRecordType::PageClear);
        match rec.body {
            WalBody::PageClear {
                page_id,
                slot,
                page_type,
            } => {
                assert_eq!(page_id, 10);
                assert_eq!(slot, 99);
                assert_eq!(page_type, 2);
            }
            _ => panic!("wrong body type"),
        }
    }

    #[test]
    fn corrupt_crc_stops_reader() {
        let (mut io, fh) = setup();
        let mut writer = WalWriter::new(fh);

        writer.append(1, WalRecordType::Begin, &WalBody::Empty);
        writer.append(1, WalRecordType::Commit, &WalBody::Empty);
        writer.flush(&mut io).unwrap();

        // Corrupt a byte in the second record
        let file_size = io.file_size(fh).unwrap();
        let first_rec_size = WAL_HEADER_SIZE + WAL_CRC_SIZE; // Begin has empty body
        let corrupt_offset = first_rec_size as u64 + 2;
        io.write_at(fh, corrupt_offset, &[0xFF]).unwrap();

        let mut reader = WalReader::new(fh, file_size);
        assert!(reader.next(&mut io).is_some()); // first record OK
        assert!(reader.next(&mut io).is_none()); // second record corrupt → stops
    }

    #[test]
    fn lsn_is_byte_offset() {
        let (mut io, fh) = setup();
        let mut writer = WalWriter::new(fh);

        let lsn0 = writer.append(1, WalRecordType::Begin, &WalBody::Empty);
        assert_eq!(lsn0, 0);

        let lsn1 = writer.append(1, WalRecordType::Commit, &WalBody::Empty);
        // lsn1 = header(20) + body(0) + crc(4) = 24
        assert_eq!(lsn1, (WAL_HEADER_SIZE + WAL_CRC_SIZE) as u64);

        writer.flush(&mut io).unwrap();
    }

    #[test]
    fn multiple_records_round_trip() {
        let (mut io, fh) = setup();
        let mut writer = WalWriter::new(fh);

        for i in 0..50u64 {
            writer.append(i, WalRecordType::Begin, &WalBody::Empty);
            writer.append(
                i,
                WalRecordType::PageWrite,
                &WalBody::PageWrite {
                    page_id: i,
                    slot: i as u16,
                    page_type: 1,
                    data: [i as u8; RECORD_SIZE],
                },
            );
            writer.append(i, WalRecordType::Commit, &WalBody::Empty);
        }
        writer.flush(&mut io).unwrap();

        let file_size = io.file_size(fh).unwrap();
        let mut reader = WalReader::new(fh, file_size);
        let mut count = 0;
        while reader.next(&mut io).is_some() {
            count += 1;
        }
        assert_eq!(count, 150); // 50 * (Begin + PageWrite + Commit)
    }
}
