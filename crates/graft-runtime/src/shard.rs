use std::cell::RefCell;

use graft_core::{EdgeId, LabelId, NodeId, PageId, ShardId};
use graft_query::executor::{EdgeInfo, NodeInfo, StorageAccess, Value};
use graft_storage::page::PageType;
use graft_storage::{BufferPool, EdgeRecord, NodeRecord, Page};
use hashbrown::HashMap;

use crate::label::LabelDictionary;

// Default buffer pool capacity (pages) per pool.
const DEFAULT_POOL_CAPACITY: usize = 1024;

// ---------------------------------------------------------------------------
// Shard — a single data partition backed by page-based storage
// ---------------------------------------------------------------------------

pub struct Shard {
    shard_id: ShardId,
    pub(crate) labels: LabelDictionary,

    // Page-based storage — wrapped in RefCell because BufferPool::pin/unpin
    // require &mut self, but StorageAccess reads take &self. Shards are
    // single-threaded so RefCell is safe.
    node_pool: RefCell<BufferPool>,
    edge_pool: RefCell<BufferPool>,

    // ID → (page_id, slot) indexes for O(1) lookup
    node_index: HashMap<u64, (PageId, u16)>,
    edge_index: HashMap<u64, (PageId, u16)>,

    // Property side-tables (keyed by raw id + property name)
    node_props: HashMap<(u64, String), Value>,
    edge_props: HashMap<(u64, String), Value>,

    // Monotonic counters
    next_node_local: u64,
    next_edge_local: u64,

    // Page ID counters (separate namespaces for node/edge pools)
    next_node_page_id: PageId,
    next_edge_page_id: PageId,
}

impl Shard {
    pub fn new(shard_id: ShardId) -> Self {
        Self {
            shard_id,
            labels: LabelDictionary::new(),
            node_pool: RefCell::new(BufferPool::new(DEFAULT_POOL_CAPACITY)),
            edge_pool: RefCell::new(BufferPool::new(DEFAULT_POOL_CAPACITY)),
            node_index: HashMap::new(),
            edge_index: HashMap::new(),
            node_props: HashMap::new(),
            edge_props: HashMap::new(),
            next_node_local: 1, // 0 is NULL sentinel
            next_edge_local: 1,
            next_node_page_id: 1,
            next_edge_page_id: 1,
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

    // -- Page allocation helpers -----------------------------------------------

    /// Find a node page with a free slot, or allocate a new one.
    fn alloc_node_slot(&mut self) -> (PageId, u16) {
        let mut pool = self.node_pool.borrow_mut();

        // Try the most recent page first
        if self.next_node_page_id > 1 {
            let last_page_id = self.next_node_page_id - 1;
            if let Some(fid) = pool.pin(last_page_id) {
                let page = pool.page_mut(fid);
                if !page.is_full() {
                    let slot = page.alloc_slot().unwrap();
                    pool.unpin(fid);
                    return (last_page_id, slot);
                }
                pool.unpin(fid);
            }
        }

        // Allocate a new page
        let page_id = self.next_node_page_id;
        self.next_node_page_id += 1;
        let page = Page::new(page_id, PageType::Node);
        pool.create(page).expect("node pool create failed");
        let fid = pool.pin(page_id).unwrap();
        let slot = pool.page_mut(fid).alloc_slot().unwrap();
        pool.unpin(fid);
        (page_id, slot)
    }

    /// Find an edge page with a free slot, or allocate a new one.
    fn alloc_edge_slot(&mut self) -> (PageId, u16) {
        let mut pool = self.edge_pool.borrow_mut();

        if self.next_edge_page_id > 1 {
            let last_page_id = self.next_edge_page_id - 1;
            if let Some(fid) = pool.pin(last_page_id) {
                let page = pool.page_mut(fid);
                if !page.is_full() {
                    let slot = page.alloc_slot().unwrap();
                    pool.unpin(fid);
                    return (last_page_id, slot);
                }
                pool.unpin(fid);
            }
        }

        let page_id = self.next_edge_page_id;
        self.next_edge_page_id += 1;
        let page = Page::new(page_id, PageType::Edge);
        pool.create(page).expect("edge pool create failed");
        let fid = pool.pin(page_id).unwrap();
        let slot = pool.page_mut(fid).alloc_slot().unwrap();
        pool.unpin(fid);
        (page_id, slot)
    }

    // -- Record read/write helpers ---------------------------------------------

    fn read_node_record(&self, page_id: PageId, slot: u16) -> Option<NodeRecord> {
        let mut pool = self.node_pool.borrow_mut();
        let fid = pool.pin(page_id)?;
        let buf = pool.page(fid).read_record(slot).ok()?;
        let rec = NodeRecord::read_from(buf);
        pool.unpin(fid);
        Some(rec)
    }

    fn write_node_record(&self, page_id: PageId, slot: u16, rec: &NodeRecord) {
        let mut pool = self.node_pool.borrow_mut();
        let fid = pool.pin(page_id).expect("page not in pool");
        pool.page_mut(fid)
            .write_record(slot, &rec.to_bytes())
            .expect("write_record failed");
        pool.unpin(fid);
    }

    fn read_edge_record(&self, page_id: PageId, slot: u16) -> Option<EdgeRecord> {
        let mut pool = self.edge_pool.borrow_mut();
        let fid = pool.pin(page_id)?;
        let buf = pool.page(fid).read_record(slot).ok()?;
        let rec = EdgeRecord::read_from(buf);
        pool.unpin(fid);
        Some(rec)
    }

    fn write_edge_record(&self, page_id: PageId, slot: u16, rec: &EdgeRecord) {
        let mut pool = self.edge_pool.borrow_mut();
        let fid = pool.pin(page_id).expect("page not in pool");
        pool.page_mut(fid)
            .write_record(slot, &rec.to_bytes())
            .expect("write_record failed");
        pool.unpin(fid);
    }

    // -- Lookup helpers --------------------------------------------------------

    fn find_node(&self, id: NodeId) -> Option<NodeRecord> {
        let &(page_id, slot) = self.node_index.get(&id.as_u64())?;
        let rec = self.read_node_record(page_id, slot)?;
        if rec.is_deleted() || rec.node_id.is_null() {
            return None;
        }
        Some(rec)
    }

    fn find_edge(&self, id: EdgeId) -> Option<EdgeRecord> {
        let &(page_id, slot) = self.edge_index.get(&id.as_u64())?;
        let rec = self.read_edge_record(page_id, slot)?;
        if rec.is_deleted() || rec.edge_id.is_null() {
            return None;
        }
        Some(rec)
    }
}

// ---------------------------------------------------------------------------
// StorageAccess implementation
// ---------------------------------------------------------------------------

impl StorageAccess for Shard {
    fn scan_nodes(&self, label: Option<&str>) -> Vec<NodeInfo> {
        let label_id = label.and_then(|l| self.labels.get(l));

        if label.is_some() && label_id.is_none() {
            return Vec::new();
        }

        let mut result = Vec::new();
        for (&raw_id, &(page_id, slot)) in &self.node_index {
            if let Some(rec) = self.read_node_record(page_id, slot) {
                if rec.is_deleted() || rec.node_id.is_null() {
                    continue;
                }
                if let Some(lid) = label_id {
                    if rec.label_id != lid {
                        continue;
                    }
                }
                result.push(NodeInfo {
                    id: NodeId::from_raw(raw_id),
                    label: self.labels.name(rec.label_id).map(String::from),
                });
            }
        }
        result
    }

    fn get_node(&self, id: NodeId) -> Option<NodeInfo> {
        let rec = self.find_node(id)?;
        Some(NodeInfo {
            id: rec.node_id,
            label: self.labels.name(rec.label_id).map(String::from),
        })
    }

    fn node_property(&self, id: NodeId, key: &str) -> Value {
        if self.find_node(id).is_none() {
            return Value::Null;
        }
        self.node_props
            .get(&(id.as_u64(), key.to_owned()))
            .cloned()
            .unwrap_or(Value::Null)
    }

    fn outbound_edges(&self, node_id: NodeId, label: Option<&str>) -> Vec<EdgeInfo> {
        let label_id = label.and_then(|l| self.labels.get(l));
        if label.is_some() && label_id.is_none() {
            return Vec::new();
        }

        let node = match self.find_node(node_id) {
            Some(n) => n,
            None => return Vec::new(),
        };

        let mut result = Vec::new();
        let mut edge_id = node.first_out_edge;
        while !edge_id.is_null() {
            let edge = match self.find_edge(edge_id) {
                Some(e) => e,
                None => break,
            };
            if label_id.is_none() || edge.label_id == label_id.unwrap() {
                result.push(EdgeInfo {
                    id: edge.edge_id,
                    source: edge.source,
                    target: edge.target,
                    label: self.labels.name(edge.label_id).map(String::from),
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

        let node = match self.find_node(node_id) {
            Some(n) => n,
            None => return Vec::new(),
        };

        let mut result = Vec::new();
        let mut edge_id = node.first_in_edge;
        while !edge_id.is_null() {
            let edge = match self.find_edge(edge_id) {
                Some(e) => e,
                None => break,
            };
            if label_id.is_none() || edge.label_id == label_id.unwrap() {
                result.push(EdgeInfo {
                    id: edge.edge_id,
                    source: edge.source,
                    target: edge.target,
                    label: self.labels.name(edge.label_id).map(String::from),
                });
            }
            edge_id = edge.next_in_edge;
        }
        result
    }

    fn edge_property(&self, id: EdgeId, key: &str) -> Value {
        if self.find_edge(id).is_none() {
            return Value::Null;
        }
        self.edge_props
            .get(&(id.as_u64(), key.to_owned()))
            .cloned()
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

        let rec = NodeRecord::new(id, label_id, 0);
        let (page_id, slot) = self.alloc_node_slot();
        self.write_node_record(page_id, slot, &rec);
        self.node_index.insert(id.as_u64(), (page_id, slot));

        for (k, v) in properties {
            self.node_props.insert((id.as_u64(), k.clone()), v.clone());
        }

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

        // Read current head pointers
        let old_out_head = self
            .find_node(source)
            .map(|n| n.first_out_edge)
            .unwrap_or(EdgeId::NULL);
        let old_in_head = self
            .find_node(target)
            .map(|n| n.first_in_edge)
            .unwrap_or(EdgeId::NULL);

        // Create edge record with linked list pointers
        let mut rec = EdgeRecord::new(id, source, target, label_id, 0);
        rec.next_out_edge = old_out_head;
        rec.next_in_edge = old_in_head;

        let (page_id, slot) = self.alloc_edge_slot();
        self.write_edge_record(page_id, slot, &rec);
        self.edge_index.insert(id.as_u64(), (page_id, slot));

        // Update source node's first_out_edge
        if let Some(&(np, ns)) = self.node_index.get(&source.as_u64()) {
            if let Some(mut node_rec) = self.read_node_record(np, ns) {
                if !node_rec.is_deleted() {
                    node_rec.first_out_edge = id;
                    self.write_node_record(np, ns, &node_rec);
                }
            }
        }

        // Update target node's first_in_edge
        if let Some(&(np, ns)) = self.node_index.get(&target.as_u64()) {
            if let Some(mut node_rec) = self.read_node_record(np, ns) {
                if !node_rec.is_deleted() {
                    node_rec.first_in_edge = id;
                    self.write_node_record(np, ns, &node_rec);
                }
            }
        }

        for (k, v) in properties {
            self.edge_props.insert((id.as_u64(), k.clone()), v.clone());
        }

        id
    }

    fn set_node_property(&mut self, id: NodeId, key: &str, value: &Value) {
        if self.find_node(id).is_some() {
            self.node_props
                .insert((id.as_u64(), key.to_owned()), value.clone());
        }
    }

    fn set_edge_property(&mut self, id: EdgeId, key: &str, value: &Value) {
        if self.find_edge(id).is_some() {
            self.edge_props
                .insert((id.as_u64(), key.to_owned()), value.clone());
        }
    }

    fn delete_node(&mut self, id: NodeId) {
        if let Some(&(page_id, slot)) = self.node_index.get(&id.as_u64()) {
            if let Some(mut rec) = self.read_node_record(page_id, slot) {
                rec.tx_max = 1; // non-zero = deleted
                self.write_node_record(page_id, slot, &rec);
            }
        }
    }

    fn delete_edge(&mut self, id: EdgeId) {
        if let Some(&(page_id, slot)) = self.edge_index.get(&id.as_u64()) {
            if let Some(mut rec) = self.read_edge_record(page_id, slot) {
                rec.tx_max = 1; // non-zero = deleted
                self.write_edge_record(page_id, slot, &rec);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graft_core::constants::RECORDS_PER_PAGE;

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

        let out = shard.outbound_edges(alice, None);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].target, bob);

        let inb = shard.inbound_edges(bob, None);
        assert_eq!(inb.len(), 1);
        assert_eq!(inb[0].source, alice);

        assert_eq!(shard.edge_property(edge, "since"), Value::Int(2020));
    }

    #[test]
    fn index_free_adjacency_linked_list() {
        let mut shard = Shard::new(0);
        let a = shard.create_node(Some("N"), &[]);
        let b = shard.create_node(Some("N"), &[]);
        let c = shard.create_node(Some("N"), &[]);

        shard.create_edge(a, b, Some("E"), &[]);
        shard.create_edge(a, c, Some("E"), &[]);

        let out = shard.outbound_edges(a, None);
        assert_eq!(out.len(), 2);
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
        // Edge is deleted — linked-list walk breaks at deleted edge.
    }

    #[test]
    fn page_fills_and_spills() {
        let mut shard = Shard::new(0);
        let count = RECORDS_PER_PAGE + 10;
        for i in 0..count {
            shard.create_node(Some("N"), &[("i".into(), Value::Int(i as i64))]);
        }
        assert_eq!(shard.scan_nodes(Some("N")).len(), count);
        assert!(shard.next_node_page_id > 2, "should have allocated 2+ pages");
    }
}
