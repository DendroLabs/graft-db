use thiserror::Error;

use crate::id::{NodeId, EdgeId, PageId};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    // Storage
    #[error("page {0} not found")]
    PageNotFound(PageId),

    #[error("page {0} is full")]
    PageFull(PageId),

    #[error("node {0} not found")]
    NodeNotFound(NodeId),

    #[error("edge {0} not found")]
    EdgeNotFound(EdgeId),

    #[error("record slot {slot} out of range for page {page}")]
    SlotOutOfRange { page: PageId, slot: u16 },

    #[error("page {page} corrupt: {detail}")]
    CorruptPage { page: PageId, detail: String },

    // Transactions
    #[error("transaction conflict: write-write on node {0}")]
    WriteConflict(NodeId),

    #[error("transaction not active")]
    TxNotActive,

    // I/O
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    // Wire protocol
    #[error("invalid wire magic")]
    InvalidMagic,

    #[error("unsupported protocol version {0}")]
    UnsupportedVersion(u8),

    // Query
    #[error("parse error: {0}")]
    Parse(String),

    #[error("unknown label: {0}")]
    UnknownLabel(String),

    #[error("type error: {0}")]
    TypeError(String),
}
