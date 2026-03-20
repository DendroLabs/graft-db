use graft_core::constants::RECORD_SIZE;
use graft_core::LabelId;

/// Maximum label name length that fits in a 64-byte record.
pub const LABEL_NAME_MAX: usize = 58;

/// Bidirectional mapping between label strings and compact `LabelId` values.
///
/// LabelId(0) is reserved as `NONE` (unlabeled), so real labels start at 1.
pub struct LabelDictionary {
    /// label string → LabelId
    to_id: Vec<(String, LabelId)>,
    next_id: u32,
}

impl Default for LabelDictionary {
    fn default() -> Self {
        Self::new()
    }
}

impl LabelDictionary {
    pub fn new() -> Self {
        Self {
            to_id: Vec::new(),
            next_id: 1, // 0 is LabelId::NONE
        }
    }

    /// Get or create a LabelId for the given label string.
    pub fn get_or_insert(&mut self, label: &str) -> LabelId {
        for (name, id) in &self.to_id {
            if name == label {
                return *id;
            }
        }
        let id = LabelId::new(self.next_id);
        self.next_id += 1;
        self.to_id.push((label.to_owned(), id));
        id
    }

    /// Look up the LabelId for a label string, if it exists.
    pub fn get(&self, label: &str) -> Option<LabelId> {
        self.to_id
            .iter()
            .find_map(|(name, id)| if name == label { Some(*id) } else { None })
    }

    /// Look up the label string for a LabelId.
    pub fn name(&self, id: LabelId) -> Option<&str> {
        if id == LabelId::NONE {
            return None;
        }
        self.to_id
            .iter()
            .find_map(|(name, lid)| if *lid == id { Some(name.as_str()) } else { None })
    }

    /// Insert a label with a specific ID (for recovery).
    pub fn insert_label(&mut self, id: LabelId, name: &str) {
        if self.get(name).is_some() {
            return;
        }
        self.to_id.push((name.to_owned(), id));
        if id.as_u32() >= self.next_id {
            self.next_id = id.as_u32() + 1;
        }
    }

    /// Return all labels as (id, name) pairs for serialization.
    pub fn to_records(&self) -> &[(String, LabelId)] {
        &self.to_id
    }

    /// Serialize a label into a 64-byte record.
    ///
    /// Layout: label_id(4) + name_len(2) + name(up to 58 bytes)
    pub fn write_label_record(id: LabelId, name: &str, buf: &mut [u8]) {
        debug_assert!(buf.len() >= RECORD_SIZE);
        buf[..RECORD_SIZE].fill(0);
        buf[0..4].copy_from_slice(&id.as_u32().to_le_bytes());
        let name_bytes = name.as_bytes();
        let len = name_bytes.len().min(LABEL_NAME_MAX);
        buf[4..6].copy_from_slice(&(len as u16).to_le_bytes());
        buf[6..6 + len].copy_from_slice(&name_bytes[..len]);
    }

    /// Deserialize a label from a 64-byte record.
    pub fn read_label_record(buf: &[u8]) -> (LabelId, String) {
        debug_assert!(buf.len() >= RECORD_SIZE);
        let id = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let len = u16::from_le_bytes(buf[4..6].try_into().unwrap()) as usize;
        let len = len.min(LABEL_NAME_MAX);
        let name = std::str::from_utf8(&buf[6..6 + len])
            .unwrap_or("")
            .to_owned();
        (LabelId::new(id), name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_or_insert_returns_same_id() {
        let mut dict = LabelDictionary::new();
        let id1 = dict.get_or_insert("Person");
        let id2 = dict.get_or_insert("Person");
        assert_eq!(id1, id2);
    }

    #[test]
    fn different_labels_get_different_ids() {
        let mut dict = LabelDictionary::new();
        let id1 = dict.get_or_insert("Person");
        let id2 = dict.get_or_insert("Company");
        assert_ne!(id1, id2);
    }

    #[test]
    fn name_round_trip() {
        let mut dict = LabelDictionary::new();
        let id = dict.get_or_insert("Person");
        assert_eq!(dict.name(id), Some("Person"));
    }

    #[test]
    fn none_label_returns_none() {
        let dict = LabelDictionary::new();
        assert_eq!(dict.name(LabelId::NONE), None);
    }

    #[test]
    fn get_missing_returns_none() {
        let dict = LabelDictionary::new();
        assert_eq!(dict.get("Missing"), None);
    }
}
