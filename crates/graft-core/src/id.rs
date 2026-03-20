use serde::{Deserialize, Serialize};
use std::fmt;

use crate::constants::SHARD_BITS;

/// Identifies the shard (CPU core) that owns a piece of data.
pub type ShardId = u8;

/// Monotonically increasing transaction identifier.
pub type TxId = u64;

/// Page number within a shard's storage file.
pub type PageId = u64;

// ---------------------------------------------------------------------------
// NodeId
// ---------------------------------------------------------------------------

/// 64-bit node identifier.
///
/// Layout: `[shard: 8 bits | local_id: 56 bits]`
///
/// The upper 8 bits encode the owning shard so that any component can route a
/// node operation to the correct core without a lookup.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(u64);

impl NodeId {
    pub const NULL: Self = Self(0);

    const LOCAL_MASK: u64 = (1u64 << (64 - SHARD_BITS)) - 1;

    #[inline]
    pub fn is_null(self) -> bool {
        self.0 == 0
    }

    /// Reconstruct from a raw u64 (e.g. read from disk).
    #[inline]
    pub fn from_raw(v: u64) -> Self {
        Self(v)
    }

    #[inline]
    pub fn new(shard: ShardId, local_id: u64) -> Self {
        debug_assert!(
            local_id <= Self::LOCAL_MASK,
            "local_id overflows 56-bit space"
        );
        Self((shard as u64) << (64 - SHARD_BITS) | (local_id & Self::LOCAL_MASK))
    }

    #[inline]
    pub fn shard(self) -> ShardId {
        (self.0 >> (64 - SHARD_BITS)) as ShardId
    }

    #[inline]
    pub fn local_id(self) -> u64 {
        self.0 & Self::LOCAL_MASK
    }

    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId(s{}:{})", self.shard(), self.local_id())
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "n{}:{}", self.shard(), self.local_id())
    }
}

// ---------------------------------------------------------------------------
// EdgeId
// ---------------------------------------------------------------------------

/// 64-bit edge identifier.
///
/// Same layout as [`NodeId`]: upper 8 bits = shard of the **source** node.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EdgeId(u64);

impl EdgeId {
    pub const NULL: Self = Self(0);

    const LOCAL_MASK: u64 = (1u64 << (64 - SHARD_BITS)) - 1;

    #[inline]
    pub fn is_null(self) -> bool {
        self.0 == 0
    }

    #[inline]
    pub fn from_raw(v: u64) -> Self {
        Self(v)
    }

    #[inline]
    pub fn new(shard: ShardId, local_id: u64) -> Self {
        debug_assert!(
            local_id <= Self::LOCAL_MASK,
            "local_id overflows 56-bit space"
        );
        Self((shard as u64) << (64 - SHARD_BITS) | (local_id & Self::LOCAL_MASK))
    }

    #[inline]
    pub fn shard(self) -> ShardId {
        (self.0 >> (64 - SHARD_BITS)) as ShardId
    }

    #[inline]
    pub fn local_id(self) -> u64 {
        self.0 & Self::LOCAL_MASK
    }

    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Debug for EdgeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EdgeId(s{}:{})", self.shard(), self.local_id())
    }
}

impl fmt::Display for EdgeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "e{}:{}", self.shard(), self.local_id())
    }
}

// ---------------------------------------------------------------------------
// LabelId
// ---------------------------------------------------------------------------

/// Compact label identifier (mapped from label strings via a dictionary).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LabelId(u32);

impl LabelId {
    pub const NONE: Self = Self(0);

    #[inline]
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    #[inline]
    pub fn as_u32(self) -> u32 {
        self.0
    }
}

impl fmt::Debug for LabelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LabelId({})", self.0)
    }
}

impl fmt::Display for LabelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "L{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_round_trip() {
        let id = NodeId::new(42, 12345);
        assert_eq!(id.shard(), 42);
        assert_eq!(id.local_id(), 12345);
    }

    #[test]
    fn node_id_shard_zero() {
        let id = NodeId::new(0, 1);
        assert_eq!(id.shard(), 0);
        assert_eq!(id.local_id(), 1);
    }

    #[test]
    fn node_id_max_shard() {
        let id = NodeId::new(255, 0);
        assert_eq!(id.shard(), 255);
        assert_eq!(id.local_id(), 0);
    }

    #[test]
    fn node_id_max_local() {
        let max_local = (1u64 << 56) - 1;
        let id = NodeId::new(1, max_local);
        assert_eq!(id.shard(), 1);
        assert_eq!(id.local_id(), max_local);
    }

    #[test]
    fn edge_id_round_trip() {
        let id = EdgeId::new(7, 999);
        assert_eq!(id.shard(), 7);
        assert_eq!(id.local_id(), 999);
    }

    #[test]
    fn label_id() {
        let l = LabelId::new(100);
        assert_eq!(l.as_u32(), 100);
    }

    #[test]
    fn display_formats() {
        assert_eq!(format!("{}", NodeId::new(3, 10)), "n3:10");
        assert_eq!(format!("{}", EdgeId::new(3, 10)), "e3:10");
        assert_eq!(format!("{}", LabelId::new(5)), "L5");
    }
}
