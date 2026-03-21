use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::thread::{self, JoinHandle};

use graft_core::{EdgeId, NodeId, ShardId, TxId};
use graft_query::executor::{EdgeInfo, NodeInfo, StorageAccess, Value};
use graft_query::{self, QueryResult};

use crate::affinity;
use crate::event_loop::ShardEventLoop;
use crate::shard::{Shard, ShardConfig};
use crate::spsc;

// ---------------------------------------------------------------------------
// Request / Response — typed messages between coordinator and shard workers
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) enum Request {
    ScanNodes {
        label: Option<String>,
    },
    GetNode {
        node_id: NodeId,
    },
    NodeProperty {
        node_id: NodeId,
        key: String,
    },
    OutboundEdges {
        node_id: NodeId,
        label: Option<String>,
    },
    InboundEdges {
        node_id: NodeId,
        label: Option<String>,
    },
    EdgeProperty {
        edge_id: EdgeId,
        key: String,
    },
    CreateNode {
        label: Option<String>,
        properties: Vec<(String, Value)>,
    },
    CreateEdge {
        source: NodeId,
        target: NodeId,
        label: Option<String>,
        properties: Vec<(String, Value)>,
    },
    SetNodeProperty {
        node_id: NodeId,
        key: String,
        value: Value,
    },
    SetEdgeProperty {
        edge_id: EdgeId,
        key: String,
        value: Value,
    },
    DeleteNode {
        node_id: NodeId,
    },
    DeleteEdge {
        edge_id: EdgeId,
    },
    BeginTx {
        tx_id: TxId,
    },
    SetActiveTx {
        tx_id: TxId,
    },
    CommitTx {
        tx_id: TxId,
    },
    AbortTx {
        tx_id: TxId,
    },
    Flush,
    Shutdown,
}

#[derive(Debug)]
pub(crate) enum Response {
    Nodes(Vec<NodeInfo>),
    Node(Option<NodeInfo>),
    Value(Value),
    Edges(Vec<EdgeInfo>),
    NodeId(NodeId),
    EdgeId(EdgeId),
    Done,
}

// ---------------------------------------------------------------------------
// ShardHandle — coordinator's handle to one shard worker thread
// ---------------------------------------------------------------------------

struct ShardHandle {
    request_tx: spsc::Producer<Request>,
    response_rx: spsc::Consumer<Response>,
    thread: Option<JoinHandle<()>>,
}

impl ShardHandle {
    /// Send a request and block until the response arrives.
    fn call(&self, req: Request) -> Response {
        // Push request (spin if full)
        let mut req = req;
        loop {
            match self.request_tx.push(req) {
                Ok(()) => break,
                Err(returned) => {
                    req = returned;
                    std::hint::spin_loop();
                }
            }
        }
        // Wait for response
        loop {
            if let Some(resp) = self.response_rx.pop() {
                return resp;
            }
            std::hint::spin_loop();
        }
    }
}

impl Drop for ShardHandle {
    fn drop(&mut self) {
        // Send shutdown — the event loop flushes before exiting.
        // It also sends a Done response so we can drain the queue.
        let _ = self.request_tx.push(Request::Shutdown);
        // Wait for the Done response to ensure flush completed
        loop {
            match self.response_rx.pop() {
                Some(_) => break,
                None => std::hint::spin_loop(),
            }
        }
        // Wait for thread to exit
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

// ---------------------------------------------------------------------------
// ShardCluster — multi-threaded shard-per-core database
// ---------------------------------------------------------------------------

/// A database that runs each shard on its own OS thread.
///
/// This is the production-path architecture: one thread per core, each owning
/// its shard's data. The coordinator routes operations to the correct shard
/// thread via lock-free SPSC queues.
///
/// ```text
///   Client query
///       |
///   ShardCluster (coordinator)
///       |--- SPSC ---> [Thread 0: Shard 0]
///       |--- SPSC ---> [Thread 1: Shard 1]
///       |--- SPSC ---> [Thread 2: Shard 2]
///       \--- SPSC ---> [Thread N: Shard N]
/// ```
pub struct ShardCluster {
    handles: Vec<ShardHandle>,
    next_shard: AtomicU8,
    next_tx_id: AtomicU64,
    current_tx: TxId,
    /// When true, auto-commit begin/commit/abort in the executor are suppressed.
    in_explicit_tx: bool,
    pin_cores: bool,
}

impl ShardCluster {
    /// Spawn a cluster with `n` shard worker threads (pinning disabled).
    pub fn new(n: usize) -> Self {
        Self::new_with_options(n, false)
    }

    /// Spawn a cluster with `n` shard worker threads.
    /// If `pin_cores` is true, each worker thread is pinned to its shard's core.
    pub fn new_with_options(n: usize, pin_cores: bool) -> Self {
        assert!((1..=256).contains(&n), "shard count must be 1..=256");

        let mut handles = Vec::with_capacity(n);

        for i in 0..n {
            let shard_id = i as ShardId;
            let shard = Shard::new(shard_id);
            let handle = Self::spawn_worker(shard, shard_id, pin_cores);
            handles.push(handle);
        }

        Self {
            handles,
            next_shard: AtomicU8::new(0),
            next_tx_id: AtomicU64::new(1),
            current_tx: 0,
            in_explicit_tx: false,
            pin_cores,
        }
    }

    /// Spawn a durable cluster (pinning disabled).
    pub fn open(n: usize, data_dir: &Path) -> std::io::Result<Self> {
        Self::open_with_options(n, data_dir, false)
    }

    /// Spawn a durable cluster with `n` shard worker threads backed by files
    /// in `data_dir`. Creates or recovers shards from disk.
    /// If `pin_cores` is true, each worker thread is pinned to its shard's core.
    pub fn open_with_options(n: usize, data_dir: &Path, pin_cores: bool) -> std::io::Result<Self> {
        assert!((1..=256).contains(&n), "shard count must be 1..=256");

        // First, open all shards and find the max tx_id for global counter
        let mut shards = Vec::with_capacity(n);
        let mut max_tx: TxId = 0;
        for i in 0..n {
            let shard_id = i as ShardId;
            let config = ShardConfig {
                data_dir: data_dir.to_owned(),
                pool_capacity: 1024,
            };
            let shard = Shard::open(shard_id, &config)?;
            let shard_tx = shard.next_local_tx();
            if shard_tx > max_tx {
                max_tx = shard_tx;
            }
            shards.push(shard);
        }

        // Now spawn worker threads
        let mut handles = Vec::with_capacity(n);
        for shard in shards {
            let shard_id = shard.shard_id();
            let handle = Self::spawn_worker(shard, shard_id, pin_cores);
            handles.push(handle);
        }

        Ok(Self {
            handles,
            next_shard: AtomicU8::new(0),
            next_tx_id: AtomicU64::new(max_tx),
            current_tx: 0,
            in_explicit_tx: false,
            pin_cores,
        })
    }

    fn spawn_worker(shard: Shard, shard_id: ShardId, pin_cores: bool) -> ShardHandle {
        let (req_tx, req_rx) = spsc::channel::<Request>(4096);
        let (resp_tx, resp_rx) = spsc::channel::<Response>(4096);

        let event_loop = ShardEventLoop::with_coordinator(shard, req_rx, resp_tx);
        let pin_core = if pin_cores {
            Some(shard_id as usize)
        } else {
            None
        };

        let thread = thread::Builder::new()
            .name(format!("graft-shard-{shard_id}"))
            .spawn(move || {
                if let Some(core_id) = pin_core {
                    affinity::pin_to_core(core_id);
                }
                event_loop.run();
            })
            .expect("failed to spawn shard thread");

        ShardHandle {
            request_tx: req_tx,
            response_rx: resp_rx,
            thread: Some(thread),
        }
    }

    /// Flush all shards to disk. Each shard flushes its WAL and dirty pages.
    pub fn flush(&self) {
        for handle in &self.handles {
            handle.call(Request::Flush);
        }
    }

    /// Number of shard threads.
    pub fn shard_count(&self) -> usize {
        self.handles.len()
    }

    /// Whether core pinning is enabled.
    pub fn pin_cores(&self) -> bool {
        self.pin_cores
    }

    /// Execute a GQL query across the cluster.
    pub fn query(&mut self, gql: &str) -> Result<QueryResult, String> {
        let ast = graft_query::parse(gql)?;
        let plan = graft_query::planner::plan(&ast).map_err(|e| format!("{e}"))?;
        graft_query::executor::execute(&plan, self).map_err(|e| format!("{e}"))
    }

    /// Begin an explicit multi-statement transaction. Returns the tx_id.
    /// Subsequent calls to `query()` within this tx will suppress auto-commit.
    pub fn begin_explicit_tx(&mut self) -> TxId {
        assert!(!self.in_explicit_tx, "already in an explicit transaction");
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::Relaxed);
        for handle in &self.handles {
            handle.call(Request::BeginTx { tx_id });
        }
        self.current_tx = tx_id;
        self.in_explicit_tx = true;
        tx_id
    }

    /// Commit the current explicit transaction.
    pub fn commit_explicit_tx(&mut self) {
        assert!(self.in_explicit_tx, "no explicit transaction active");
        let tx_id = self.current_tx;
        for handle in &self.handles {
            handle.call(Request::CommitTx { tx_id });
        }
        self.current_tx = 0;
        self.in_explicit_tx = false;
    }

    /// Abort the current explicit transaction.
    pub fn abort_explicit_tx(&mut self) {
        assert!(self.in_explicit_tx, "no explicit transaction active");
        let tx_id = self.current_tx;
        for handle in &self.handles {
            handle.call(Request::AbortTx { tx_id });
        }
        self.current_tx = 0;
        self.in_explicit_tx = false;
    }

    /// Execute a query within an explicit transaction. Restores tx context
    /// on the shard workers before executing.
    pub fn query_in_tx(&mut self, gql: &str) -> Result<QueryResult, String> {
        assert!(self.in_explicit_tx, "no explicit transaction active");
        let tx_id = self.current_tx;
        // Restore tx context on all shards (may have been changed by
        // other connections' auto-commit queries since last access)
        for handle in &self.handles {
            handle.call(Request::SetActiveTx { tx_id });
        }
        // Execute — begin_tx/commit_tx will be no-ops due to in_explicit_tx
        let ast = graft_query::parse(gql)?;
        let plan = graft_query::planner::plan(&ast).map_err(|e| format!("{e}"))?;
        graft_query::executor::execute(&plan, self).map_err(|e| format!("{e}"))
    }

    /// Pick next shard for round-robin node creation.
    fn pick_shard(&self) -> ShardId {
        let n = self.handles.len() as u8;
        let id = self.next_shard.fetch_add(1, Ordering::Relaxed);
        id % n
    }

    fn shard_for_node(&self, id: NodeId) -> &ShardHandle {
        let idx = id.shard() as usize % self.handles.len();
        &self.handles[idx]
    }

    fn shard_for_edge(&self, id: EdgeId) -> &ShardHandle {
        let idx = id.shard() as usize % self.handles.len();
        &self.handles[idx]
    }
}

// ShardHandle::drop sends Shutdown and joins threads automatically.

// ---------------------------------------------------------------------------
// StorageAccess — routes through SPSC to worker threads
// ---------------------------------------------------------------------------

impl StorageAccess for ShardCluster {
    fn scan_nodes(&self, label: Option<&str>) -> Vec<NodeInfo> {
        // Fan out to ALL shards, merge results
        let label_owned = label.map(String::from);

        // Send requests to all shards
        for handle in &self.handles {
            let mut req = Request::ScanNodes {
                label: label_owned.clone(),
            };
            loop {
                match handle.request_tx.push(req) {
                    Ok(()) => break,
                    Err(returned) => {
                        req = returned;
                        std::hint::spin_loop();
                    }
                }
            }
        }

        // Collect responses from all shards
        let mut result = Vec::new();
        for handle in &self.handles {
            loop {
                if let Some(Response::Nodes(nodes)) = handle.response_rx.pop() {
                    result.extend(nodes);
                    break;
                }
                std::hint::spin_loop();
            }
        }
        result
    }

    fn get_node(&self, id: NodeId) -> Option<NodeInfo> {
        match self
            .shard_for_node(id)
            .call(Request::GetNode { node_id: id })
        {
            Response::Node(n) => n,
            _ => unreachable!(),
        }
    }

    fn node_property(&self, id: NodeId, key: &str) -> Value {
        match self.shard_for_node(id).call(Request::NodeProperty {
            node_id: id,
            key: key.to_owned(),
        }) {
            Response::Value(v) => v,
            _ => unreachable!(),
        }
    }

    fn outbound_edges(&self, node_id: NodeId, label: Option<&str>) -> Vec<EdgeInfo> {
        match self.shard_for_node(node_id).call(Request::OutboundEdges {
            node_id,
            label: label.map(String::from),
        }) {
            Response::Edges(e) => e,
            _ => unreachable!(),
        }
    }

    fn inbound_edges(&self, node_id: NodeId, label: Option<&str>) -> Vec<EdgeInfo> {
        // Inbound edges could be on any shard — fan out to all
        let label_owned = label.map(String::from);

        for handle in &self.handles {
            let mut req = Request::InboundEdges {
                node_id,
                label: label_owned.clone(),
            };
            loop {
                match handle.request_tx.push(req) {
                    Ok(()) => break,
                    Err(returned) => {
                        req = returned;
                        std::hint::spin_loop();
                    }
                }
            }
        }

        let mut result = Vec::new();
        for handle in &self.handles {
            loop {
                if let Some(Response::Edges(edges)) = handle.response_rx.pop() {
                    result.extend(edges);
                    break;
                }
                std::hint::spin_loop();
            }
        }
        result
    }

    fn edge_property(&self, id: EdgeId, key: &str) -> Value {
        match self.shard_for_edge(id).call(Request::EdgeProperty {
            edge_id: id,
            key: key.to_owned(),
        }) {
            Response::Value(v) => v,
            _ => unreachable!(),
        }
    }

    fn create_node(&mut self, label: Option<&str>, properties: &[(String, Value)]) -> NodeId {
        let shard_id = self.pick_shard();
        match self.handles[shard_id as usize].call(Request::CreateNode {
            label: label.map(String::from),
            properties: properties.to_vec(),
        }) {
            Response::NodeId(id) => id,
            _ => unreachable!(),
        }
    }

    fn create_edge(
        &mut self,
        source: NodeId,
        target: NodeId,
        label: Option<&str>,
        properties: &[(String, Value)],
    ) -> EdgeId {
        match self.shard_for_node(source).call(Request::CreateEdge {
            source,
            target,
            label: label.map(String::from),
            properties: properties.to_vec(),
        }) {
            Response::EdgeId(id) => id,
            _ => unreachable!(),
        }
    }

    fn set_node_property(&mut self, id: NodeId, key: &str, value: &Value) {
        self.shard_for_node(id).call(Request::SetNodeProperty {
            node_id: id,
            key: key.to_owned(),
            value: value.clone(),
        });
    }

    fn set_edge_property(&mut self, id: EdgeId, key: &str, value: &Value) {
        self.shard_for_edge(id).call(Request::SetEdgeProperty {
            edge_id: id,
            key: key.to_owned(),
            value: value.clone(),
        });
    }

    fn delete_node(&mut self, id: NodeId) {
        self.shard_for_node(id)
            .call(Request::DeleteNode { node_id: id });
    }

    fn delete_edge(&mut self, id: EdgeId) {
        self.shard_for_edge(id)
            .call(Request::DeleteEdge { edge_id: id });
    }

    fn begin_tx(&mut self) -> u64 {
        if self.in_explicit_tx {
            return self.current_tx;
        }
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::Relaxed);
        self.current_tx = tx_id;
        for handle in &self.handles {
            handle.call(Request::BeginTx { tx_id });
        }
        tx_id
    }

    fn commit_tx(&mut self) {
        if self.in_explicit_tx {
            return;
        }
        let tx_id = self.current_tx;
        for handle in &self.handles {
            handle.call(Request::CommitTx { tx_id });
        }
        self.current_tx = 0;
    }

    fn abort_tx(&mut self) {
        if self.in_explicit_tx {
            return;
        }
        let tx_id = self.current_tx;
        for handle in &self.handles {
            handle.call(Request::AbortTx { tx_id });
        }
        self.current_tx = 0;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use graft_query::executor::Value;

    #[test]
    fn single_shard_cluster_basic() {
        let mut cluster = ShardCluster::new(1);
        cluster
            .query("CREATE (:Person {name: 'Alice', age: 30})")
            .unwrap();
        cluster
            .query("CREATE (:Person {name: 'Bob', age: 25})")
            .unwrap();

        let result = cluster
            .query("MATCH (p:Person) RETURN p.name ORDER BY p.name")
            .unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::String("Alice".into()));
        assert_eq!(result.rows[1][0], Value::String("Bob".into()));
    }

    #[test]
    fn multi_shard_cluster_distributes_nodes() {
        let mut cluster = ShardCluster::new(4);

        for i in 0..8 {
            cluster.query(&format!("CREATE (:N {{v: {i}}})")).unwrap();
        }

        let result = cluster
            .query("MATCH (n:N) RETURN n.v ORDER BY n.v")
            .unwrap();
        assert_eq!(result.rows.len(), 8);

        // Verify nodes are distributed across shards
        let result = cluster.query("MATCH (n:N) RETURN id(n)").unwrap();
        let mut shards: Vec<u8> = result
            .rows
            .iter()
            .map(|r| match &r[0] {
                Value::Int(id) => NodeId::from_raw(*id as u64).shard(),
                _ => panic!("expected int"),
            })
            .collect();
        shards.sort();
        shards.dedup();
        assert_eq!(shards.len(), 4, "8 nodes should span 4 shards");
    }

    #[test]
    fn multi_shard_cluster_cross_shard_traversal() {
        let mut cluster = ShardCluster::new(2);

        cluster.query("CREATE (:Person {name: 'Alice'})").unwrap();
        cluster.query("CREATE (:Person {name: 'Bob'})").unwrap();
        cluster
            .query(
                "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
                 CREATE (a)-[:KNOWS {since: 2024}]->(b)",
            )
            .unwrap();

        let result = cluster
            .query(
                "MATCH (a:Person)-[e:KNOWS]->(b:Person) \
                 RETURN a.name, b.name, e.since",
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::String("Alice".into()));
        assert_eq!(result.rows[0][1], Value::String("Bob".into()));
        assert_eq!(result.rows[0][2], Value::Int(2024));
    }

    #[test]
    fn multi_shard_cluster_aggregate() {
        let mut cluster = ShardCluster::new(4);

        for i in 0..12 {
            cluster.query(&format!("CREATE (:N {{v: {i}}})")).unwrap();
        }

        let result = cluster.query("MATCH (n:N) RETURN COUNT(*)").unwrap();
        assert_eq!(result.rows[0][0], Value::Int(12));
    }

    #[test]
    fn multi_shard_cluster_multi_hop() {
        let mut cluster = ShardCluster::new(3);

        cluster.query("CREATE (:N {name: 'A'})").unwrap();
        cluster.query("CREATE (:N {name: 'B'})").unwrap();
        cluster.query("CREATE (:N {name: 'C'})").unwrap();

        cluster
            .query("MATCH (a:N {name: 'A'}), (b:N {name: 'B'}) CREATE (a)-[:E]->(b)")
            .unwrap();
        cluster
            .query("MATCH (b:N {name: 'B'}), (c:N {name: 'C'}) CREATE (b)-[:E]->(c)")
            .unwrap();

        let result = cluster
            .query(
                "MATCH (a:N {name: 'A'})-[:E]->(b:N)-[:E]->(c:N) \
                 RETURN c.name",
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::String("C".into()));
    }

    #[test]
    fn multi_shard_cluster_set_and_delete() {
        let mut cluster = ShardCluster::new(2);

        cluster.query("CREATE (:N {name: 'a', v: 1})").unwrap();
        cluster.query("CREATE (:N {name: 'b', v: 2})").unwrap();

        // SET across shards
        cluster
            .query("MATCH (n:N {name: 'b'}) SET n.v = 99")
            .unwrap();

        let result = cluster
            .query("MATCH (n:N) RETURN n.name, n.v ORDER BY n.name")
            .unwrap();
        assert_eq!(result.rows[1][1], Value::Int(99));

        // DELETE across shards
        cluster.query("MATCH (n:N {name: 'a'}) DELETE n").unwrap();

        let result = cluster.query("MATCH (n:N) RETURN n.name").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::String("b".into()));
    }

    #[test]
    fn multi_shard_cluster_where_filter() {
        let mut cluster = ShardCluster::new(3);

        for i in 0..9 {
            cluster.query(&format!("CREATE (:N {{v: {i}}})")).unwrap();
        }

        let result = cluster
            .query("MATCH (n:N) WHERE n.v > 5 RETURN n.v ORDER BY n.v")
            .unwrap();
        assert_eq!(result.rows.len(), 3); // 6, 7, 8
        assert_eq!(result.rows[0][0], Value::Int(6));
    }

    #[test]
    fn multi_shard_cluster_string_predicates() {
        let mut cluster = ShardCluster::new(2);

        cluster.query("CREATE (:N {name: 'Alice'})").unwrap();
        cluster.query("CREATE (:N {name: 'Bob'})").unwrap();
        cluster.query("CREATE (:N {name: 'Alicia'})").unwrap();

        let result = cluster
            .query("MATCH (n:N) WHERE n.name STARTS WITH 'Al' RETURN n.name ORDER BY n.name")
            .unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::String("Alice".into()));
        assert_eq!(result.rows[1][0], Value::String("Alicia".into()));
    }

    #[test]
    fn cluster_auto_commit() {
        // Queries through ShardCluster get proper tx wrapping
        let mut cluster = ShardCluster::new(2);

        // Each query should auto-commit
        cluster.query("CREATE (:Person {name: 'Alice'})").unwrap();
        cluster.query("CREATE (:Person {name: 'Bob'})").unwrap();

        // Read should see both committed records
        let result = cluster
            .query("MATCH (p:Person) RETURN p.name ORDER BY p.name")
            .unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::String("Alice".into()));
        assert_eq!(result.rows[1][0], Value::String("Bob".into()));

        // Delete one and verify
        cluster
            .query("MATCH (p:Person {name: 'Alice'}) DELETE p")
            .unwrap();

        let result = cluster.query("MATCH (p:Person) RETURN p.name").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::String("Bob".into()));
    }

    #[test]
    fn cluster_graceful_shutdown() {
        // Verify cluster shuts down cleanly when dropped
        let mut cluster = ShardCluster::new(4);
        cluster.query("CREATE (:N {v: 1})").unwrap();
        drop(cluster);
        // If we get here without hanging, shutdown works
    }

    #[test]
    fn cluster_with_pin_cores() {
        let mut cluster = ShardCluster::new_with_options(2, true);
        assert!(cluster.pin_cores());
        cluster.query("CREATE (:N {v: 1})").unwrap();
        cluster.query("CREATE (:N {v: 2})").unwrap();

        let result = cluster
            .query("MATCH (n:N) RETURN n.v ORDER BY n.v")
            .unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::Int(1));
        assert_eq!(result.rows[1][0], Value::Int(2));
    }

    #[test]
    fn explicit_tx_commit() {
        let mut cluster = ShardCluster::new(2);

        let tx_id = cluster.begin_explicit_tx();
        assert!(tx_id > 0);

        // Create within explicit tx
        cluster
            .query_in_tx("CREATE (:Person {name: 'Alice'})")
            .unwrap();

        // Should be visible within the tx
        let result = cluster
            .query_in_tx("MATCH (p:Person) RETURN p.name")
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::String("Alice".into()));

        // Commit
        cluster.commit_explicit_tx();

        // Should be visible after commit via auto-commit query
        let result = cluster.query("MATCH (p:Person) RETURN p.name").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::String("Alice".into()));
    }

    #[test]
    fn explicit_tx_rollback() {
        let mut cluster = ShardCluster::new(2);

        cluster.begin_explicit_tx();
        cluster
            .query_in_tx("CREATE (:Person {name: 'Alice'})")
            .unwrap();

        // Visible within tx
        let result = cluster
            .query_in_tx("MATCH (p:Person) RETURN p.name")
            .unwrap();
        assert_eq!(result.rows.len(), 1);

        // Abort
        cluster.abort_explicit_tx();

        // Should NOT be visible after abort
        let result = cluster.query("MATCH (p:Person) RETURN p.name").unwrap();
        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn explicit_tx_isolation_from_auto_commit() {
        let mut cluster = ShardCluster::new(2);

        // Create a node via auto-commit
        cluster.query("CREATE (:Person {name: 'Bob'})").unwrap();

        // Start explicit tx
        cluster.begin_explicit_tx();
        cluster
            .query_in_tx("CREATE (:Person {name: 'Alice'})")
            .unwrap();

        // Both should be visible within the tx
        let result = cluster
            .query_in_tx("MATCH (p:Person) RETURN p.name ORDER BY p.name")
            .unwrap();
        assert_eq!(result.rows.len(), 2);

        cluster.commit_explicit_tx();

        let result = cluster
            .query("MATCH (p:Person) RETURN p.name ORDER BY p.name")
            .unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn cluster_open_and_recover() {
        let dir = tempfile::tempdir().unwrap();

        // Create data in a durable cluster
        {
            let mut cluster = ShardCluster::open(2, dir.path()).unwrap();
            cluster
                .query("CREATE (:Person {name: 'Alice', age: 30})")
                .unwrap();
            cluster
                .query("CREATE (:Person {name: 'Bob', age: 25})")
                .unwrap();
            cluster.query("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS {since: 2024}]->(b)").unwrap();
            // Drop triggers flush via ShardHandle::drop
        }

        // Re-open and verify all data survived
        {
            let mut cluster = ShardCluster::open(2, dir.path()).unwrap();

            let result = cluster
                .query("MATCH (n:Person) RETURN n.name ORDER BY n.name")
                .unwrap();
            assert_eq!(result.rows.len(), 2);
            assert_eq!(result.rows[0][0], Value::String("Alice".into()));
            assert_eq!(result.rows[1][0], Value::String("Bob".into()));

            let result = cluster
                .query("MATCH (a)-[e:KNOWS]->(b) RETURN a.name, e.since, b.name")
                .unwrap();
            assert_eq!(result.rows.len(), 1);
            assert_eq!(result.rows[0][0], Value::String("Alice".into()));
            assert_eq!(result.rows[0][1], Value::Int(2024));
            assert_eq!(result.rows[0][2], Value::String("Bob".into()));
        }
    }

    #[test]
    fn explicit_tx_commit_durable() {
        let dir = tempfile::tempdir().unwrap();

        // Create data via explicit tx in a durable cluster
        {
            let mut cluster = ShardCluster::open(2, dir.path()).unwrap();
            cluster.begin_explicit_tx();
            cluster
                .query_in_tx("CREATE (:Person {name: 'Alice'})")
                .unwrap();
            cluster
                .query_in_tx("CREATE (:Person {name: 'Bob'})")
                .unwrap();
            cluster.commit_explicit_tx();
            // Drop triggers flush
        }

        // Re-open and verify data survived
        {
            let mut cluster = ShardCluster::open(2, dir.path()).unwrap();
            let result = cluster
                .query("MATCH (p:Person) RETURN p.name ORDER BY p.name")
                .unwrap();
            assert_eq!(result.rows.len(), 2);
            assert_eq!(result.rows[0][0], Value::String("Alice".into()));
            assert_eq!(result.rows[1][0], Value::String("Bob".into()));
        }
    }

    #[test]
    fn explicit_tx_rollback_durable() {
        let dir = tempfile::tempdir().unwrap();

        // Create committed data, then rollback additional data
        {
            let mut cluster = ShardCluster::open(2, dir.path()).unwrap();

            // Commit one node via auto-commit
            cluster.query("CREATE (:Person {name: 'Alice'})").unwrap();

            // Begin explicit tx, create another, then rollback
            cluster.begin_explicit_tx();
            cluster
                .query_in_tx("CREATE (:Person {name: 'Ghost'})")
                .unwrap();
            cluster.abort_explicit_tx();
        }

        // Re-open — only Alice should survive
        {
            let mut cluster = ShardCluster::open(2, dir.path()).unwrap();
            let result = cluster
                .query("MATCH (p:Person) RETURN p.name")
                .unwrap();
            assert_eq!(result.rows.len(), 1);
            assert_eq!(result.rows[0][0], Value::String("Alice".into()));
        }
    }

    #[test]
    fn explicit_tx_multi_statement_interleaved() {
        let mut cluster = ShardCluster::new(2);

        cluster.begin_explicit_tx();

        // Create, read, create more, read again
        cluster
            .query_in_tx("CREATE (:N {name: 'a', v: 1})")
            .unwrap();
        let result = cluster
            .query_in_tx("MATCH (n:N) RETURN COUNT(*)")
            .unwrap();
        assert_eq!(result.rows[0][0], Value::Int(1));

        cluster
            .query_in_tx("CREATE (:N {name: 'b', v: 2})")
            .unwrap();
        cluster
            .query_in_tx("CREATE (:N {name: 'c', v: 3})")
            .unwrap();

        // SET within the tx
        cluster
            .query_in_tx("MATCH (n:N {name: 'a'}) SET n.v = 100")
            .unwrap();

        let result = cluster
            .query_in_tx("MATCH (n:N) RETURN n.name, n.v ORDER BY n.name")
            .unwrap();
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0][0], Value::String("a".into()));
        assert_eq!(result.rows[0][1], Value::Int(100)); // updated
        assert_eq!(result.rows[1][0], Value::String("b".into()));
        assert_eq!(result.rows[2][0], Value::String("c".into()));

        // DELETE within the tx
        cluster
            .query_in_tx("MATCH (n:N {name: 'b'}) DELETE n")
            .unwrap();
        let result = cluster
            .query_in_tx("MATCH (n:N) RETURN COUNT(*)")
            .unwrap();
        assert_eq!(result.rows[0][0], Value::Int(2));

        cluster.commit_explicit_tx();

        // Verify post-commit
        let result = cluster
            .query("MATCH (n:N) RETURN n.name, n.v ORDER BY n.name")
            .unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::String("a".into()));
        assert_eq!(result.rows[0][1], Value::Int(100));
        assert_eq!(result.rows[1][0], Value::String("c".into()));
    }

    #[test]
    fn batched_flush_many_pages() {
        // Create enough data to span multiple pages, flush, recover
        let dir = tempfile::tempdir().unwrap();

        {
            let mut cluster = ShardCluster::open(1, dir.path()).unwrap();
            // 127 records per page, so 200 nodes should span 2+ pages
            for i in 0..200 {
                cluster
                    .query(&format!("CREATE (:N {{v: {i}}})"))
                    .unwrap();
            }
        }

        {
            let mut cluster = ShardCluster::open(1, dir.path()).unwrap();
            let result = cluster.query("MATCH (n:N) RETURN COUNT(*)").unwrap();
            assert_eq!(result.rows[0][0], Value::Int(200));

            // Verify first and last values survived
            let result = cluster
                .query("MATCH (n:N) RETURN n.v ORDER BY n.v LIMIT 1")
                .unwrap();
            assert_eq!(result.rows[0][0], Value::Int(0));

            let result = cluster
                .query("MATCH (n:N) RETURN n.v ORDER BY n.v SKIP 199 LIMIT 1")
                .unwrap();
            assert_eq!(result.rows[0][0], Value::Int(199));
        }
    }
}
