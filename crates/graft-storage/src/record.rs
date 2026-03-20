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
// PropertyRecord — 64 bytes
// ---------------------------------------------------------------------------
//
// Offset  Size  Field
// 0       8     entity_id (u64)
// 8       1     entity_type (0=node, 1=edge)
// 9       1     key_len (max 23)
// 10      23    key (UTF-8)
// 33      1     value_type (0=Null, 1=Int, 2=Float, 3=String, 4=Bool)
// 34      30    value_data
//
// For String values: value_data[0] = strlen, value_data[1..1+strlen] = UTF-8

pub const PROP_KEY_MAX: usize = 23;
pub const PROP_VALUE_DATA_SIZE: usize = 30;

pub const VALUE_TYPE_NULL: u8 = 0;
pub const VALUE_TYPE_INT: u8 = 1;
pub const VALUE_TYPE_FLOAT: u8 = 2;
pub const VALUE_TYPE_STRING: u8 = 3;
pub const VALUE_TYPE_BOOL: u8 = 4;

pub const ENTITY_TYPE_NODE: u8 = 0;
pub const ENTITY_TYPE_EDGE: u8 = 1;

#[derive(Clone, Debug)]
pub struct PropertyRecord {
    pub entity_id: u64,
    pub entity_type: u8,
    pub key: String,
    pub value_type: u8,
    pub value_data: [u8; PROP_VALUE_DATA_SIZE],
}

impl PropertyRecord {
    pub fn new_null(entity_id: u64, entity_type: u8, key: &str) -> Self {
        assert!(
            key.len() <= PROP_KEY_MAX,
            "property key too long: {} bytes (max {PROP_KEY_MAX})",
            key.len()
        );
        Self {
            entity_id,
            entity_type,
            key: key.to_owned(),
            value_type: VALUE_TYPE_NULL,
            value_data: [0; PROP_VALUE_DATA_SIZE],
        }
    }

    pub fn new_int(entity_id: u64, entity_type: u8, key: &str, value: i64) -> Self {
        assert!(key.len() <= PROP_KEY_MAX);
        let mut data = [0u8; PROP_VALUE_DATA_SIZE];
        data[0..8].copy_from_slice(&value.to_le_bytes());
        Self {
            entity_id,
            entity_type,
            key: key.to_owned(),
            value_type: VALUE_TYPE_INT,
            value_data: data,
        }
    }

    pub fn new_float(entity_id: u64, entity_type: u8, key: &str, value: f64) -> Self {
        assert!(key.len() <= PROP_KEY_MAX);
        let mut data = [0u8; PROP_VALUE_DATA_SIZE];
        data[0..8].copy_from_slice(&value.to_le_bytes());
        Self {
            entity_id,
            entity_type,
            key: key.to_owned(),
            value_type: VALUE_TYPE_FLOAT,
            value_data: data,
        }
    }

    pub fn new_string(entity_id: u64, entity_type: u8, key: &str, value: &str) -> Self {
        assert!(key.len() <= PROP_KEY_MAX);
        let bytes = value.as_bytes();
        assert!(
            bytes.len() < PROP_VALUE_DATA_SIZE,
            "property string value too long: {} bytes (max {})",
            bytes.len(),
            PROP_VALUE_DATA_SIZE - 1
        );
        let mut data = [0u8; PROP_VALUE_DATA_SIZE];
        data[0] = bytes.len() as u8;
        data[1..1 + bytes.len()].copy_from_slice(bytes);
        Self {
            entity_id,
            entity_type,
            key: key.to_owned(),
            value_type: VALUE_TYPE_STRING,
            value_data: data,
        }
    }

    pub fn new_bool(entity_id: u64, entity_type: u8, key: &str, value: bool) -> Self {
        assert!(key.len() <= PROP_KEY_MAX);
        let mut data = [0u8; PROP_VALUE_DATA_SIZE];
        data[0] = u8::from(value);
        Self {
            entity_id,
            entity_type,
            key: key.to_owned(),
            value_type: VALUE_TYPE_BOOL,
            value_data: data,
        }
    }

    pub fn int_value(&self) -> Option<i64> {
        if self.value_type == VALUE_TYPE_INT {
            Some(i64::from_le_bytes(self.value_data[0..8].try_into().unwrap()))
        } else {
            None
        }
    }

    pub fn float_value(&self) -> Option<f64> {
        if self.value_type == VALUE_TYPE_FLOAT {
            Some(f64::from_le_bytes(self.value_data[0..8].try_into().unwrap()))
        } else {
            None
        }
    }

    pub fn string_value(&self) -> Option<String> {
        if self.value_type == VALUE_TYPE_STRING {
            let len = self.value_data[0] as usize;
            Some(
                String::from_utf8_lossy(&self.value_data[1..1 + len]).into_owned(),
            )
        } else {
            None
        }
    }

    pub fn bool_value(&self) -> Option<bool> {
        if self.value_type == VALUE_TYPE_BOOL {
            Some(self.value_data[0] != 0)
        } else {
            None
        }
    }

    pub fn is_null(&self) -> bool {
        self.value_type == VALUE_TYPE_NULL
    }

    pub fn read_from(buf: &[u8]) -> Self {
        debug_assert!(buf.len() >= RECORD_SIZE);
        let entity_id = r_u64(buf, 0);
        let entity_type = buf[8];
        let key_len = buf[9] as usize;
        let key_len = key_len.min(PROP_KEY_MAX);
        let key = std::str::from_utf8(&buf[10..10 + key_len])
            .unwrap_or("")
            .to_owned();
        let value_type = buf[33];
        let mut value_data = [0u8; PROP_VALUE_DATA_SIZE];
        value_data.copy_from_slice(&buf[34..64]);
        Self {
            entity_id,
            entity_type,
            key,
            value_type,
            value_data,
        }
    }

    pub fn write_to(&self, buf: &mut [u8]) {
        debug_assert!(buf.len() >= RECORD_SIZE);
        buf[..RECORD_SIZE].fill(0);
        w_u64(buf, 0, self.entity_id);
        buf[8] = self.entity_type;
        let key_bytes = self.key.as_bytes();
        let key_len = key_bytes.len().min(PROP_KEY_MAX);
        buf[9] = key_len as u8;
        buf[10..10 + key_len].copy_from_slice(&key_bytes[..key_len]);
        buf[33] = self.value_type;
        buf[34..64].copy_from_slice(&self.value_data);
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

    #[test]
    fn property_record_int_round_trip() {
        let rec = PropertyRecord::new_int(42, ENTITY_TYPE_NODE, "age", 30);
        let bytes = rec.to_bytes();
        let rec2 = PropertyRecord::read_from(&bytes);
        assert_eq!(rec2.entity_id, 42);
        assert_eq!(rec2.entity_type, ENTITY_TYPE_NODE);
        assert_eq!(rec2.key, "age");
        assert_eq!(rec2.int_value(), Some(30));
    }

    #[test]
    fn property_record_string_round_trip() {
        let rec = PropertyRecord::new_string(7, ENTITY_TYPE_EDGE, "name", "Alice");
        let bytes = rec.to_bytes();
        let rec2 = PropertyRecord::read_from(&bytes);
        assert_eq!(rec2.entity_id, 7);
        assert_eq!(rec2.entity_type, ENTITY_TYPE_EDGE);
        assert_eq!(rec2.key, "name");
        assert_eq!(rec2.string_value(), Some("Alice".into()));
    }

    #[test]
    fn property_record_bool_round_trip() {
        let rec = PropertyRecord::new_bool(1, ENTITY_TYPE_NODE, "active", true);
        let bytes = rec.to_bytes();
        let rec2 = PropertyRecord::read_from(&bytes);
        assert_eq!(rec2.bool_value(), Some(true));
    }

    #[test]
    fn property_record_float_round_trip() {
        let rec = PropertyRecord::new_float(1, ENTITY_TYPE_NODE, "score", 3.14);
        let bytes = rec.to_bytes();
        let rec2 = PropertyRecord::read_from(&bytes);
        assert!((rec2.float_value().unwrap() - 3.14).abs() < f64::EPSILON);
    }

    #[test]
    fn property_record_null_round_trip() {
        let rec = PropertyRecord::new_null(1, ENTITY_TYPE_NODE, "missing");
        let bytes = rec.to_bytes();
        let rec2 = PropertyRecord::read_from(&bytes);
        assert!(rec2.is_null());
    }
}
