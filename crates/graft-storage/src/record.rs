use graft_core::constants::RECORD_SIZE;
use graft_core::{EdgeId, LabelId, NodeId, TxId};

// Nodes get 16 bytes of inline property space. Edges have none — five u64
// pointer fields plus MVCC timestamps consume the full 64-byte budget.
// Edge properties always go to overflow pages.
pub const INLINE_NODE_PROP_BYTES: usize = 16;

// ---------------------------------------------------------------------------
// NodeRecord — 64 bytes
// ---------------------------------------------------------------------------
//
// Offset  Size  Field
// 0       8     node_id
// 8       4     label_id
// 12      2     flags
// 14      2     (reserved)
// 16      8     first_out_edge
// 24      8     first_in_edge
// 32      8     tx_min
// 40      8     tx_max (0 = alive)
// 48      16    inline_props

#[derive(Clone, Copy, Debug)]
pub struct NodeRecord {
    pub node_id: NodeId,
    pub label_id: LabelId,
    pub flags: u16,
    pub first_out_edge: EdgeId,
    pub first_in_edge: EdgeId,
    pub tx_min: TxId,
    pub tx_max: TxId,
    pub inline_props: [u8; INLINE_NODE_PROP_BYTES],
}

impl NodeRecord {
    pub fn new(node_id: NodeId, label_id: LabelId, tx_min: TxId) -> Self {
        Self {
            node_id,
            label_id,
            flags: 0,
            first_out_edge: EdgeId::NULL,
            first_in_edge: EdgeId::NULL,
            tx_min,
            tx_max: 0,
            inline_props: [0; INLINE_NODE_PROP_BYTES],
        }
    }

    pub fn is_deleted(&self) -> bool {
        self.tx_max != 0
    }

    pub fn read_from(buf: &[u8]) -> Self {
        debug_assert!(buf.len() >= RECORD_SIZE);
        Self {
            node_id: NodeId::from_raw(r_u64(buf, 0)),
            label_id: LabelId::new(r_u32(buf, 8)),
            flags: r_u16(buf, 12),
            first_out_edge: EdgeId::from_raw(r_u64(buf, 16)),
            first_in_edge: EdgeId::from_raw(r_u64(buf, 24)),
            tx_min: r_u64(buf, 32),
            tx_max: r_u64(buf, 40),
            inline_props: {
                let mut p = [0u8; INLINE_NODE_PROP_BYTES];
                p.copy_from_slice(&buf[48..64]);
                p
            },
        }
    }

    pub fn write_to(&self, buf: &mut [u8]) {
        debug_assert!(buf.len() >= RECORD_SIZE);
        w_u64(buf, 0, self.node_id.as_u64());
        w_u32(buf, 8, self.label_id.as_u32());
        w_u16(buf, 12, self.flags);
        w_u16(buf, 14, 0); // reserved
        w_u64(buf, 16, self.first_out_edge.as_u64());
        w_u64(buf, 24, self.first_in_edge.as_u64());
        w_u64(buf, 32, self.tx_min);
        w_u64(buf, 40, self.tx_max);
        buf[48..64].copy_from_slice(&self.inline_props);
    }

    pub fn to_bytes(&self) -> [u8; RECORD_SIZE] {
        let mut buf = [0u8; RECORD_SIZE];
        self.write_to(&mut buf);
        buf
    }
}

// ---------------------------------------------------------------------------
// EdgeRecord — 64 bytes
// ---------------------------------------------------------------------------
//
// Offset  Size  Field
// 0       8     edge_id
// 8       8     source (NodeId)
// 16      8     target (NodeId)
// 24      8     next_out_edge
// 32      8     next_in_edge
// 40      4     label_id
// 44      2     flags
// 46      2     (reserved)
// 48      8     tx_min
// 56      8     tx_max (0 = alive)

#[derive(Clone, Copy, Debug)]
pub struct EdgeRecord {
    pub edge_id: EdgeId,
    pub source: NodeId,
    pub target: NodeId,
    pub next_out_edge: EdgeId,
    pub next_in_edge: EdgeId,
    pub label_id: LabelId,
    pub flags: u16,
    pub tx_min: TxId,
    pub tx_max: TxId,
}

impl EdgeRecord {
    pub fn new(
        edge_id: EdgeId,
        source: NodeId,
        target: NodeId,
        label_id: LabelId,
        tx_min: TxId,
    ) -> Self {
        Self {
            edge_id,
            source,
            target,
            next_out_edge: EdgeId::NULL,
            next_in_edge: EdgeId::NULL,
            label_id,
            flags: 0,
            tx_min,
            tx_max: 0,
        }
    }

    pub fn is_deleted(&self) -> bool {
        self.tx_max != 0
    }

    pub fn read_from(buf: &[u8]) -> Self {
        debug_assert!(buf.len() >= RECORD_SIZE);
        Self {
            edge_id: EdgeId::from_raw(r_u64(buf, 0)),
            source: NodeId::from_raw(r_u64(buf, 8)),
            target: NodeId::from_raw(r_u64(buf, 16)),
            next_out_edge: EdgeId::from_raw(r_u64(buf, 24)),
            next_in_edge: EdgeId::from_raw(r_u64(buf, 32)),
            label_id: LabelId::new(r_u32(buf, 40)),
            flags: r_u16(buf, 44),
            tx_min: r_u64(buf, 48),
            tx_max: r_u64(buf, 56),
        }
    }

    pub fn write_to(&self, buf: &mut [u8]) {
        debug_assert!(buf.len() >= RECORD_SIZE);
        w_u64(buf, 0, self.edge_id.as_u64());
        w_u64(buf, 8, self.source.as_u64());
        w_u64(buf, 16, self.target.as_u64());
        w_u64(buf, 24, self.next_out_edge.as_u64());
        w_u64(buf, 32, self.next_in_edge.as_u64());
        w_u32(buf, 40, self.label_id.as_u32());
        w_u16(buf, 44, self.flags);
        w_u16(buf, 46, 0); // reserved
        w_u64(buf, 48, self.tx_min);
        w_u64(buf, 56, self.tx_max);
    }

    pub fn to_bytes(&self) -> [u8; RECORD_SIZE] {
        let mut buf = [0u8; RECORD_SIZE];
        self.write_to(&mut buf);
        buf
    }
}

// ---------------------------------------------------------------------------
// Little-endian helpers
// ---------------------------------------------------------------------------

#[inline]
fn r_u16(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes(buf[off..off + 2].try_into().unwrap())
}

#[inline]
fn r_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(buf[off..off + 4].try_into().unwrap())
}

#[inline]
fn r_u64(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

#[inline]
fn w_u16(buf: &mut [u8], off: usize, v: u16) {
    buf[off..off + 2].copy_from_slice(&v.to_le_bytes());
}

#[inline]
fn w_u32(buf: &mut [u8], off: usize, v: u32) {
    buf[off..off + 4].copy_from_slice(&v.to_le_bytes());
}

#[inline]
fn w_u64(buf: &mut [u8], off: usize, v: u64) {
    buf[off..off + 8].copy_from_slice(&v.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_record_round_trip() {
        let node_id = NodeId::new(5, 100);
        let label = LabelId::new(7);
        let mut rec = NodeRecord::new(node_id, label, 1);
        rec.first_out_edge = EdgeId::new(5, 200);
        rec.inline_props[0] = 0xAA;
        rec.inline_props[15] = 0xBB;

        let bytes = rec.to_bytes();
        assert_eq!(bytes.len(), RECORD_SIZE);

        let rec2 = NodeRecord::read_from(&bytes);
        assert_eq!(rec2.node_id.as_u64(), node_id.as_u64());
        assert_eq!(rec2.label_id.as_u32(), 7);
        assert_eq!(rec2.first_out_edge.as_u64(), EdgeId::new(5, 200).as_u64());
        assert!(rec2.first_in_edge.is_null());
        assert_eq!(rec2.tx_min, 1);
        assert_eq!(rec2.tx_max, 0);
        assert!(!rec2.is_deleted());
        assert_eq!(rec2.inline_props[0], 0xAA);
        assert_eq!(rec2.inline_props[15], 0xBB);
    }

    #[test]
    fn edge_record_round_trip() {
        let eid = EdgeId::new(2, 50);
        let src = NodeId::new(2, 10);
        let tgt = NodeId::new(3, 20);
        let label = LabelId::new(99);
        let mut rec = EdgeRecord::new(eid, src, tgt, label, 5);
        rec.next_out_edge = EdgeId::new(2, 51);

        let bytes = rec.to_bytes();
        let rec2 = EdgeRecord::read_from(&bytes);
        assert_eq!(rec2.edge_id.as_u64(), eid.as_u64());
        assert_eq!(rec2.source.as_u64(), src.as_u64());
        assert_eq!(rec2.target.as_u64(), tgt.as_u64());
        assert_eq!(rec2.next_out_edge.as_u64(), EdgeId::new(2, 51).as_u64());
        assert!(rec2.next_in_edge.is_null());
        assert_eq!(rec2.label_id.as_u32(), 99);
        assert_eq!(rec2.tx_min, 5);
        assert_eq!(rec2.tx_max, 0);
        assert!(!rec2.is_deleted());
    }

    #[test]
    fn edge_record_deleted() {
        let mut rec = EdgeRecord::new(
            EdgeId::new(0, 1), NodeId::new(0, 1), NodeId::new(0, 2),
            LabelId::new(1), 1,
        );
        assert!(!rec.is_deleted());
        rec.tx_max = 5;
        assert!(rec.is_deleted());
    }

    #[test]
    fn node_record_deleted() {
        let mut rec = NodeRecord::new(NodeId::new(0, 1), LabelId::new(1), 1);
        assert!(!rec.is_deleted());
        rec.tx_max = 5;
        assert!(rec.is_deleted());
    }
}
