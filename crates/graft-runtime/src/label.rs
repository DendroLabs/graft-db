use graft_core::LabelId;

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
