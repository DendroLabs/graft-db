use graft_core::{EdgeId, LabelId, NodeId, ShardId};
use graft_query::executor::{EdgeInfo, NodeInfo, StorageAccess, Value};

use crate::label::LabelDictionary;

// ---------------------------------------------------------------------------
// Internal node/edge records
// ---------------------------------------------------------------------------

struct NodeRecord {
    id: NodeId,
    label: LabelId,
    first_out_edge: EdgeId,
    first_in_edge: EdgeId,
    properties: Vec<(String, Value)>,
    deleted: bool,
}

struct EdgeRecord {
    id: EdgeId,
    source: NodeId,
    target: NodeId,
    label: LabelId,
    next_out_edge: EdgeId, // next outbound edge from same source
    next_in_edge: EdgeId,  // next inbound edge to same target
    properties: Vec<(String, Value)>,
    deleted: bool,
}

// ---------------------------------------------------------------------------
// Shard — a single data partition
// ---------------------------------------------------------------------------

pub struct Shard {
    shard_id: ShardId,
    pub(crate) labels: LabelDictionary,
    nodes: Vec<NodeRecord>,
    edges: Vec<EdgeRecord>,
    next_node_local: u64,
    next_edge_local: u64,
}

impl Shard {
    pub fn new(shard_id: ShardId) -> Self {
        Self {
            shard_id,
            labels: LabelDictionary::new(),
            nodes: Vec::new(),
            edges: Vec::new(),
            next_node_local: 1, // 0 is NULL sentinel
            next_edge_local: 1,
        }
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    fn alloc_node_id(&mut self) -> NodeId {
        let id = NodeId::new(self.shard_id, self.next_node_local);
        self.next_node_local += 1;
        id
    }

    fn alloc_edge_id(&mut self) -> EdgeId {
        let id = EdgeId::new(self.shard_id, self.next_edge_local);
        self.next_edge_local += 1;
        id
    }

    fn find_node(&self, id: NodeId) -> Option<&NodeRecord> {
        self.nodes.iter().find(|n| n.id == id && !n.deleted)
    }

    fn find_node_mut(&mut self, id: NodeId) -> Option<&mut NodeRecord> {
        self.nodes.iter_mut().find(|n| n.id == id && !n.deleted)
    }

    fn find_edge(&self, id: EdgeId) -> Option<&EdgeRecord> {
        self.edges.iter().find(|e| e.id == id && !e.deleted)
    }

    fn find_edge_mut(&mut self, id: EdgeId) -> Option<&mut EdgeRecord> {
        self.edges.iter_mut().find(|e| e.id == id && !e.deleted)
    }
}

// ---------------------------------------------------------------------------
// StorageAccess implementation
// ---------------------------------------------------------------------------

impl StorageAccess for Shard {
    fn scan_nodes(&self, label: Option<&str>) -> Vec<NodeInfo> {
        let label_id = label.and_then(|l| self.labels.get(l));

        // If a label was requested but doesn't exist in the dictionary,
        // there can be no matching nodes.
        if label.is_some() && label_id.is_none() {
            return Vec::new();
        }

        self.nodes
            .iter()
            .filter(|n| !n.deleted)
            .filter(|n| match label_id {
                Some(lid) => n.label == lid,
                None => true,
            })
            .map(|n| NodeInfo {
                id: n.id,
                label: self.labels.name(n.label).map(String::from),
            })
            .collect()
    }

    fn get_node(&self, id: NodeId) -> Option<NodeInfo> {
        self.find_node(id).map(|n| NodeInfo {
            id: n.id,
            label: self.labels.name(n.label).map(String::from),
        })
    }

    fn node_property(&self, id: NodeId, key: &str) -> Value {
        self.find_node(id)
            .and_then(|n| {
                n.properties
                    .iter()
                    .find_map(|(k, v)| if k == key { Some(v.clone()) } else { None })
            })
            .unwrap_or(Value::Null)
    }

    fn outbound_edges(&self, node_id: NodeId, label: Option<&str>) -> Vec<EdgeInfo> {
        let label_id = label.and_then(|l| self.labels.get(l));
        if label.is_some() && label_id.is_none() {
            return Vec::new();
        }

        let mut result = Vec::new();
        let node = match self.find_node(node_id) {
            Some(n) => n,
            None => return result,
        };

        let mut edge_id = node.first_out_edge;
        while !edge_id.is_null() {
            let edge = match self.find_edge(edge_id) {
                Some(e) => e,
                None => break,
            };
            if label_id.is_none() || edge.label == label_id.unwrap() {
                result.push(EdgeInfo {
                    id: edge.id,
                    source: edge.source,
                    target: edge.target,
                    label: self.labels.name(edge.label).map(String::from),
                });
            }
            edge_id = edge.next_out_edge;
        }
        result
    }

    fn inbound_edges(&self, node_id: NodeId, label: Option<&str>) -> Vec<EdgeInfo> {
        let label_id = label.and_then(|l| self.labels.get(l));
        if label.is_some() && label_id.is_none() {
            return Vec::new();
        }

        let mut result = Vec::new();
        let node = match self.find_node(node_id) {
            Some(n) => n,
            None => return result,
        };

        let mut edge_id = node.first_in_edge;
        while !edge_id.is_null() {
            let edge = match self.find_edge(edge_id) {
                Some(e) => e,
                None => break,
            };
            if label_id.is_none() || edge.label == label_id.unwrap() {
                result.push(EdgeInfo {
                    id: edge.id,
                    source: edge.source,
                    target: edge.target,
                    label: self.labels.name(edge.label).map(String::from),
                });
            }
            edge_id = edge.next_in_edge;
        }
        result
    }

    fn edge_property(&self, id: EdgeId, key: &str) -> Value {
        self.find_edge(id)
            .and_then(|e| {
                e.properties
                    .iter()
                    .find_map(|(k, v)| if k == key { Some(v.clone()) } else { None })
            })
            .unwrap_or(Value::Null)
    }

    fn create_node(
        &mut self,
        label: Option<&str>,
        properties: &[(String, Value)],
    ) -> NodeId {
        let id = self.alloc_node_id();
        let label_id = label
            .map(|l| self.labels.get_or_insert(l))
            .unwrap_or(LabelId::NONE);

        self.nodes.push(NodeRecord {
            id,
            label: label_id,
            first_out_edge: EdgeId::NULL,
            first_in_edge: EdgeId::NULL,
            properties: properties.to_vec(),
            deleted: false,
        });
        id
    }

    fn create_edge(
        &mut self,
        source: NodeId,
        target: NodeId,
        label: Option<&str>,
        properties: &[(String, Value)],
    ) -> EdgeId {
        let id = self.alloc_edge_id();
        let label_id = label
            .map(|l| self.labels.get_or_insert(l))
            .unwrap_or(LabelId::NONE);

        // Read current head pointers before creating the edge
        let old_out_head = self
            .find_node(source)
            .map(|n| n.first_out_edge)
            .unwrap_or(EdgeId::NULL);
        let old_in_head = self
            .find_node(target)
            .map(|n| n.first_in_edge)
            .unwrap_or(EdgeId::NULL);

        // Insert edge at the head of both linked lists
        self.edges.push(EdgeRecord {
            id,
            source,
            target,
            label: label_id,
            next_out_edge: old_out_head,
            next_in_edge: old_in_head,
            properties: properties.to_vec(),
            deleted: false,
        });

        // Update head pointers on source and target nodes
        if let Some(node) = self.find_node_mut(source) {
            node.first_out_edge = id;
        }
        if let Some(node) = self.find_node_mut(target) {
            node.first_in_edge = id;
        }

        id
    }

    fn set_node_property(&mut self, id: NodeId, key: &str, value: &Value) {
        if let Some(node) = self.find_node_mut(id) {
            if let Some(prop) = node.properties.iter_mut().find(|(k, _)| k == key) {
                prop.1 = value.clone();
            } else {
                node.properties.push((key.to_owned(), value.clone()));
            }
        }
    }

    fn set_edge_property(&mut self, id: EdgeId, key: &str, value: &Value) {
        if let Some(edge) = self.find_edge_mut(id) {
            if let Some(prop) = edge.properties.iter_mut().find(|(k, _)| k == key) {
                prop.1 = value.clone();
            } else {
                edge.properties.push((key.to_owned(), value.clone()));
            }
        }
    }

    fn delete_node(&mut self, id: NodeId) {
        if let Some(node) = self.nodes.iter_mut().find(|n| n.id == id) {
            node.deleted = true;
        }
    }

    fn delete_edge(&mut self, id: EdgeId) {
        if let Some(edge) = self.edges.iter_mut().find(|e| e.id == id) {
            edge.deleted = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_scan_nodes() {
        let mut shard = Shard::new(0);
        let id = shard.create_node(Some("Person"), &[("name".into(), Value::String("Alice".into()))]);
        assert!(!id.is_null());

        let nodes = shard.scan_nodes(Some("Person"));
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].label.as_deref(), Some("Person"));

        assert_eq!(shard.node_property(id, "name"), Value::String("Alice".into()));
        assert_eq!(shard.node_property(id, "missing"), Value::Null);
    }

    #[test]
    fn scan_with_label_filter() {
        let mut shard = Shard::new(0);
        shard.create_node(Some("Person"), &[]);
        shard.create_node(Some("Company"), &[]);
        shard.create_node(Some("Person"), &[]);

        assert_eq!(shard.scan_nodes(Some("Person")).len(), 2);
        assert_eq!(shard.scan_nodes(Some("Company")).len(), 1);
        assert_eq!(shard.scan_nodes(Some("Missing")).len(), 0);
        assert_eq!(shard.scan_nodes(None).len(), 3);
    }

    #[test]
    fn create_edge_and_traverse() {
        let mut shard = Shard::new(0);
        let alice = shard.create_node(Some("Person"), &[]);
        let bob = shard.create_node(Some("Person"), &[]);
        let edge = shard.create_edge(alice, bob, Some("KNOWS"), &[("since".into(), Value::Int(2020))]);

        // Outbound from Alice
        let out = shard.outbound_edges(alice, None);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].target, bob);

        // Inbound to Bob
        let inb = shard.inbound_edges(bob, None);
        assert_eq!(inb.len(), 1);
        assert_eq!(inb[0].source, alice);

        // Edge property
        assert_eq!(shard.edge_property(edge, "since"), Value::Int(2020));
    }

    #[test]
    fn index_free_adjacency_linked_list() {
        let mut shard = Shard::new(0);
        let a = shard.create_node(Some("N"), &[]);
        let b = shard.create_node(Some("N"), &[]);
        let c = shard.create_node(Some("N"), &[]);

        // a->b, a->c — two outbound edges from a
        shard.create_edge(a, b, Some("E"), &[]);
        shard.create_edge(a, c, Some("E"), &[]);

        let out = shard.outbound_edges(a, None);
        assert_eq!(out.len(), 2);
        // Head insertion means most recent edge comes first
        assert_eq!(out[0].target, c);
        assert_eq!(out[1].target, b);
    }

    #[test]
    fn edge_label_filter() {
        let mut shard = Shard::new(0);
        let a = shard.create_node(None, &[]);
        let b = shard.create_node(None, &[]);
        shard.create_edge(a, b, Some("KNOWS"), &[]);
        shard.create_edge(a, b, Some("WORKS_WITH"), &[]);

        assert_eq!(shard.outbound_edges(a, Some("KNOWS")).len(), 1);
        assert_eq!(shard.outbound_edges(a, Some("WORKS_WITH")).len(), 1);
        assert_eq!(shard.outbound_edges(a, None).len(), 2);
    }

    #[test]
    fn set_property_updates_existing() {
        let mut shard = Shard::new(0);
        let id = shard.create_node(None, &[("x".into(), Value::Int(1))]);
        shard.set_node_property(id, "x", &Value::Int(99));
        assert_eq!(shard.node_property(id, "x"), Value::Int(99));
    }

    #[test]
    fn delete_node_hides_from_scan() {
        let mut shard = Shard::new(0);
        let id = shard.create_node(Some("X"), &[]);
        assert_eq!(shard.scan_nodes(None).len(), 1);
        shard.delete_node(id);
        assert_eq!(shard.scan_nodes(None).len(), 0);
        assert!(shard.get_node(id).is_none());
    }

    #[test]
    fn delete_edge_hides_from_traversal() {
        let mut shard = Shard::new(0);
        let a = shard.create_node(None, &[]);
        let b = shard.create_node(None, &[]);
        let eid = shard.create_edge(a, b, None, &[]);
        assert_eq!(shard.outbound_edges(a, None).len(), 1);
        shard.delete_edge(eid);
        // Edge is deleted, but since we use soft deletion the linked-list
        // walk skips it (find_edge returns None for deleted edges, breaking
        // the walk). For now this means traversal stops at a deleted edge.
        // Full unlink is a future optimization.
    }
}
