use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const MAGIC: [u8; 4] = *b"GF01";
pub const VERSION: u8 = 1;
pub const DEFAULT_PORT: u16 = 7687;
pub const HEADER_SIZE: usize = 8;
pub const MAX_PAYLOAD_SIZE: usize = u16::MAX as usize;

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    Hello = 1,
    Query = 2,
    Result = 3,
    Row = 4,
    Summary = 5,
    Error = 6,
    BeginTx = 7,
    CommitTx = 8,
    RollbackTx = 9,
}

impl MessageType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Hello),
            2 => Some(Self::Query),
            3 => Some(Self::Result),
            4 => Some(Self::Row),
            5 => Some(Self::Summary),
            6 => Some(Self::Error),
            7 => Some(Self::BeginTx),
            8 => Some(Self::CommitTx),
            9 => Some(Self::RollbackTx),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Wire messages — MessagePack payloads
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HelloMsg {
    pub client: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryMsg {
    pub text: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResultMsg {
    pub columns: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RowMsg {
    pub values: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SummaryMsg {
    pub rows_affected: u64,
    pub elapsed_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ErrorMsg {
    pub message: String,
}

// ---------------------------------------------------------------------------
// Header encode/decode
// ---------------------------------------------------------------------------

pub fn encode_header(msg_type: MessageType, payload_len: u16) -> [u8; HEADER_SIZE] {
    let mut buf = [0u8; HEADER_SIZE];
    buf[0..4].copy_from_slice(&MAGIC);
    buf[4] = VERSION;
    buf[5] = msg_type as u8;
    buf[6..8].copy_from_slice(&payload_len.to_be_bytes());
    buf
}

pub fn decode_header(buf: &[u8; HEADER_SIZE]) -> io::Result<(MessageType, u16)> {
    if buf[0..4] != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid magic bytes",
        ));
    }
    if buf[4] != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported protocol version: {}", buf[4]),
        ));
    }
    let msg_type = MessageType::from_u8(buf[5]).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown message type: {}", buf[5]),
        )
    })?;
    let len = u16::from_be_bytes([buf[6], buf[7]]);
    Ok((msg_type, len))
}

// ---------------------------------------------------------------------------
// Send/receive helpers
// ---------------------------------------------------------------------------

pub fn send_message<W: Write>(
    writer: &mut W,
    msg_type: MessageType,
    payload: &[u8],
) -> io::Result<()> {
    if payload.len() > MAX_PAYLOAD_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "payload too large",
        ));
    }
    let header = encode_header(msg_type, payload.len() as u16);
    writer.write_all(&header)?;
    writer.write_all(payload)?;
    writer.flush()
}

pub fn recv_message<R: Read>(reader: &mut R) -> io::Result<(MessageType, Vec<u8>)> {
    let mut header = [0u8; HEADER_SIZE];
    reader.read_exact(&mut header)?;
    let (msg_type, len) = decode_header(&header)?;
    let mut payload = vec![0u8; len as usize];
    if len > 0 {
        reader.read_exact(&mut payload)?;
    }
    Ok((msg_type, payload))
}

// ---------------------------------------------------------------------------
// Typed send helpers
// ---------------------------------------------------------------------------

pub fn send_hello<W: Write>(w: &mut W, msg: &HelloMsg) -> io::Result<()> {
    let payload = rmp_serde::to_vec(msg)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    send_message(w, MessageType::Hello, &payload)
}

pub fn send_query<W: Write>(w: &mut W, msg: &QueryMsg) -> io::Result<()> {
    let payload = rmp_serde::to_vec(msg)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    send_message(w, MessageType::Query, &payload)
}

pub fn send_result<W: Write>(w: &mut W, msg: &ResultMsg) -> io::Result<()> {
    let payload = rmp_serde::to_vec(msg)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    send_message(w, MessageType::Result, &payload)
}

pub fn send_row<W: Write>(w: &mut W, msg: &RowMsg) -> io::Result<()> {
    let payload = rmp_serde::to_vec(msg)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    send_message(w, MessageType::Row, &payload)
}

pub fn send_summary<W: Write>(w: &mut W, msg: &SummaryMsg) -> io::Result<()> {
    let payload = rmp_serde::to_vec(msg)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    send_message(w, MessageType::Summary, &payload)
}

pub fn send_error<W: Write>(w: &mut W, msg: &ErrorMsg) -> io::Result<()> {
    let payload = rmp_serde::to_vec(msg)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    send_message(w, MessageType::Error, &payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trip() {
        let header = encode_header(MessageType::Query, 42);
        let (msg_type, len) = decode_header(&header).unwrap();
        assert_eq!(msg_type, MessageType::Query);
        assert_eq!(len, 42);
    }

    #[test]
    fn invalid_magic() {
        let mut header = encode_header(MessageType::Hello, 0);
        header[0] = b'X';
        assert!(decode_header(&header).is_err());
    }

    #[test]
    fn message_round_trip() {
        let mut buf = Vec::new();
        let payload = b"hello world";
        send_message(&mut buf, MessageType::Query, payload).unwrap();

        let mut reader = &buf[..];
        let (msg_type, data) = recv_message(&mut reader).unwrap();
        assert_eq!(msg_type, MessageType::Query);
        assert_eq!(data, payload);
    }

    #[test]
    fn typed_hello_round_trip() {
        let mut buf = Vec::new();
        send_hello(&mut buf, &HelloMsg { client: "test".into() }).unwrap();

        let mut reader = &buf[..];
        let (msg_type, payload) = recv_message(&mut reader).unwrap();
        assert_eq!(msg_type, MessageType::Hello);
        let msg: HelloMsg = rmp_serde::from_slice(&payload).unwrap();
        assert_eq!(msg.client, "test");
    }

    #[test]
    fn all_message_types_from_u8() {
        for i in 1..=9u8 {
            assert!(MessageType::from_u8(i).is_some());
        }
        assert!(MessageType::from_u8(0).is_none());
        assert!(MessageType::from_u8(10).is_none());
    }
}
