use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Replication message types (10-13, continuing from wire protocol 1-9)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ReplMessageType {
    ReplHello = 10,
    WalBatch = 11,
    WalAck = 12,
    ReplStatus = 13,
}

impl ReplMessageType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            10 => Some(Self::ReplHello),
            11 => Some(Self::WalBatch),
            12 => Some(Self::WalAck),
            13 => Some(Self::ReplStatus),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Replication role
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationRole {
    Standalone,
    Primary,
    Replica,
}

impl std::fmt::Display for ReplicationRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Standalone => write!(f, "standalone"),
            Self::Primary => write!(f, "primary"),
            Self::Replica => write!(f, "replica"),
        }
    }
}

impl std::str::FromStr for ReplicationRole {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "standalone" => Ok(Self::Standalone),
            "primary" => Ok(Self::Primary),
            "replica" => Ok(Self::Replica),
            _ => Err(format!("unknown role: {s}")),
        }
    }
}

// ---------------------------------------------------------------------------
// Replication wire messages (MessagePack payloads)
// ---------------------------------------------------------------------------

/// Sent by replica to primary on connection to identify itself.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplHelloMsg {
    pub shard_id: u8,
    pub role: u8, // 1=primary, 2=replica
    pub last_lsn: u64,
    pub cluster_id: u64,
}

/// Batch of committed WAL records shipped from primary to replica.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalBatchMsg {
    pub shard_id: u8,
    pub first_lsn: u64,
    pub last_lsn: u64,
    /// Raw WAL bytes — a sequence of complete WAL records (header + body + CRC).
    /// CRC-protected at the WAL record level; each record is independently verifiable.
    pub records: Vec<u8>,
}

/// ACK from replica: confirms all records up to `acked_lsn` have been applied.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalAckMsg {
    pub shard_id: u8,
    pub acked_lsn: u64,
}

/// Status message for heartbeats and diagnostics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplStatusMsg {
    pub shard_id: u8,
    pub role: u8,
    pub current_lsn: u64,
}

// ---------------------------------------------------------------------------
// Replication header encode/decode
// ---------------------------------------------------------------------------

/// Replication messages use a simple 8-byte header:
/// [magic: 2 bytes "RF"] [version: 1] [msg_type: 1] [payload_len: 4 (u32 BE)]
pub const REPL_MAGIC: [u8; 2] = *b"RF";
pub const REPL_VERSION: u8 = 1;
pub const REPL_HEADER_SIZE: usize = 8;

pub fn encode_repl_header(msg_type: ReplMessageType, payload_len: u32) -> [u8; REPL_HEADER_SIZE] {
    let mut buf = [0u8; REPL_HEADER_SIZE];
    buf[0..2].copy_from_slice(&REPL_MAGIC);
    buf[2] = REPL_VERSION;
    buf[3] = msg_type as u8;
    buf[4..8].copy_from_slice(&payload_len.to_be_bytes());
    buf
}

pub fn decode_repl_header(
    buf: &[u8; REPL_HEADER_SIZE],
) -> Result<(ReplMessageType, u32), &'static str> {
    if buf[0..2] != REPL_MAGIC {
        return Err("invalid replication magic bytes");
    }
    if buf[2] != REPL_VERSION {
        return Err("unsupported replication protocol version");
    }
    let msg_type = ReplMessageType::from_u8(buf[3]).ok_or("unknown replication message type")?;
    let len = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    Ok((msg_type, len))
}

// ---------------------------------------------------------------------------
// Send/receive helpers for replication messages
// ---------------------------------------------------------------------------

use std::io::{self, Read, Write};

pub fn send_repl_message<W: Write>(
    writer: &mut W,
    msg_type: ReplMessageType,
    payload: &[u8],
) -> io::Result<()> {
    let header = encode_repl_header(msg_type, payload.len() as u32);
    writer.write_all(&header)?;
    writer.write_all(payload)?;
    writer.flush()
}

pub fn recv_repl_message<R: Read>(reader: &mut R) -> io::Result<(ReplMessageType, Vec<u8>)> {
    let mut header = [0u8; REPL_HEADER_SIZE];
    reader.read_exact(&mut header)?;
    let (msg_type, len) =
        decode_repl_header(&header).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let mut payload = vec![0u8; len as usize];
    if len > 0 {
        reader.read_exact(&mut payload)?;
    }
    Ok((msg_type, payload))
}

pub fn send_repl_hello<W: Write>(w: &mut W, msg: &ReplHelloMsg) -> io::Result<()> {
    let payload =
        rmp_serde::to_vec(msg).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    send_repl_message(w, ReplMessageType::ReplHello, &payload)
}

pub fn send_wal_batch<W: Write>(w: &mut W, msg: &WalBatchMsg) -> io::Result<()> {
    let payload =
        rmp_serde::to_vec(msg).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    send_repl_message(w, ReplMessageType::WalBatch, &payload)
}

pub fn send_wal_ack<W: Write>(w: &mut W, msg: &WalAckMsg) -> io::Result<()> {
    let payload =
        rmp_serde::to_vec(msg).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    send_repl_message(w, ReplMessageType::WalAck, &payload)
}

pub fn send_repl_status<W: Write>(w: &mut W, msg: &ReplStatusMsg) -> io::Result<()> {
    let payload =
        rmp_serde::to_vec(msg).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    send_repl_message(w, ReplMessageType::ReplStatus, &payload)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repl_header_round_trip() {
        let header = encode_repl_header(ReplMessageType::WalBatch, 1024);
        let (msg_type, len) = decode_repl_header(&header).unwrap();
        assert_eq!(msg_type, ReplMessageType::WalBatch);
        assert_eq!(len, 1024);
    }

    #[test]
    fn repl_header_invalid_magic() {
        let mut header = encode_repl_header(ReplMessageType::ReplHello, 0);
        header[0] = b'X';
        assert!(decode_repl_header(&header).is_err());
    }

    #[test]
    fn repl_hello_round_trip() {
        let mut buf = Vec::new();
        let msg = ReplHelloMsg {
            shard_id: 3,
            role: 2,
            last_lsn: 12345,
            cluster_id: 42,
        };
        send_repl_hello(&mut buf, &msg).unwrap();

        let mut reader = &buf[..];
        let (msg_type, payload) = recv_repl_message(&mut reader).unwrap();
        assert_eq!(msg_type, ReplMessageType::ReplHello);
        let decoded: ReplHelloMsg = rmp_serde::from_slice(&payload).unwrap();
        assert_eq!(decoded.shard_id, 3);
        assert_eq!(decoded.last_lsn, 12345);
        assert_eq!(decoded.cluster_id, 42);
    }

    #[test]
    fn wal_batch_round_trip() {
        let mut buf = Vec::new();
        let records = vec![1, 2, 3, 4, 5];
        let msg = WalBatchMsg {
            shard_id: 0,
            first_lsn: 100,
            last_lsn: 200,
            records: records.clone(),
        };
        send_wal_batch(&mut buf, &msg).unwrap();

        let mut reader = &buf[..];
        let (msg_type, payload) = recv_repl_message(&mut reader).unwrap();
        assert_eq!(msg_type, ReplMessageType::WalBatch);
        let decoded: WalBatchMsg = rmp_serde::from_slice(&payload).unwrap();
        assert_eq!(decoded.shard_id, 0);
        assert_eq!(decoded.first_lsn, 100);
        assert_eq!(decoded.last_lsn, 200);
        assert_eq!(decoded.records, records);
    }

    #[test]
    fn wal_ack_round_trip() {
        let mut buf = Vec::new();
        let msg = WalAckMsg {
            shard_id: 1,
            acked_lsn: 500,
        };
        send_wal_ack(&mut buf, &msg).unwrap();

        let mut reader = &buf[..];
        let (msg_type, payload) = recv_repl_message(&mut reader).unwrap();
        assert_eq!(msg_type, ReplMessageType::WalAck);
        let decoded: WalAckMsg = rmp_serde::from_slice(&payload).unwrap();
        assert_eq!(decoded.shard_id, 1);
        assert_eq!(decoded.acked_lsn, 500);
    }

    #[test]
    fn all_repl_message_types() {
        for i in 10..=13u8 {
            assert!(ReplMessageType::from_u8(i).is_some());
        }
        assert!(ReplMessageType::from_u8(9).is_none());
        assert!(ReplMessageType::from_u8(14).is_none());
    }

    #[test]
    fn role_display_and_parse() {
        assert_eq!(ReplicationRole::Standalone.to_string(), "standalone");
        assert_eq!(ReplicationRole::Primary.to_string(), "primary");
        assert_eq!(ReplicationRole::Replica.to_string(), "replica");

        assert_eq!(
            "primary".parse::<ReplicationRole>().unwrap(),
            ReplicationRole::Primary
        );
        assert_eq!(
            "REPLICA".parse::<ReplicationRole>().unwrap(),
            ReplicationRole::Replica
        );
        assert!("unknown".parse::<ReplicationRole>().is_err());
    }
}
