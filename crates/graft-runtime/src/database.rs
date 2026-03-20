use graft_core::{EdgeId, NodeId, ShardId, TxId};
use graft_query::executor::{EdgeInfo, NodeInfo, StorageAccess, Value};
use graft_query::{self, QueryResult};

use crate::shard::Shard;

/// A graft database instance with multi-shard support.
///
/// Holds N shards (default 1, configurable). Operations are routed to the
/// correct shard based on NodeId upper 8 bits. Cross-shard queries fan out
/// to all shards and merge results.
pub struct Database {
    shards: Vec<Shard>,
    next_shard: ShardId,
    next_tx_id: TxId,
    current_tx: TxId,
}

impl Database {
    /// Create a database with a single shard (backward compatible).
    pub fn new() -> Self {
        Self::with_shards(1)
    }

    /// Create a database with `n` shards (1..=256).
    pub fn with_shards(n: usize) -> Self {
        assert!((1..=256).contains(&n), "shard count must be 1..=256");
        let shards = (0..n).map(|i| Shard::new(i as ShardId)).collect();
        Self {
            shards,
            next_shard: 0,
            next_tx_id: 1,
            current_tx: 0,
        }
    }

    /// Number of shards.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Execute a GQL query string and return the result.
    pub fn query(&mut self, gql: &str) -> Result<QueryResult, String> {
        let ast = graft_query::parse(gql)?;
        let plan = graft_query::planner::plan(&ast).map_err(|e| format!("{e}"))?;
        graft_query::executor::execute(&plan, self)
            .map_err(|e| format!("{e}"))
    }

    /// Pick the next shard for new node creation (round-robin).
    fn pick_shard(&mut self) -> ShardId {
        let id = self.next_shard;
        self.next_shard = ((id as usize + 1) % self.shards.len()) as ShardId;
        id
    }

    /// Get the shard that owns a given node.
    fn shard_for_node(&self, id: NodeId) -> &Shard {
        let idx = id.shard() as usize % self.shards.len();
        &self.shards[idx]
    }

    /// Get the shard that owns a given node (mutable).
    fn shard_for_node_mut(&mut self, id: NodeId) -> &mut Shard {
        let idx = id.shard() as usize % self.shards.len();
        &mut self.shards[idx]
    }

    /// Get the shard that owns a given edge (edges owned by source shard).
    fn shard_for_edge(&self, id: EdgeId) -> &Shard {
        let idx = id.shard() as usize % self.shards.len();
        &self.shards[idx]
    }

    fn shard_for_edge_mut(&mut self, id: EdgeId) -> &mut Shard {
        let idx = id.shard() as usize % self.shards.len();
        &mut self.shards[idx]
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// StorageAccess — routes operations across shards
// ---------------------------------------------------------------------------

impl StorageAccess for Database {
    fn scan_nodes(&self, label: Option<&str>) -> Vec<NodeInfo> {
        let mut result = Vec::new();
        for shard in &self.shards {
            result.extend(shard.scan_nodes(label));
        }
        result
    }

    fn get_node(&self, id: NodeId) -> Option<NodeInfo> {
        self.shard_for_node(id).get_node(id)
    }

    fn node_property(&self, id: NodeId, key: &str) -> Value {
        self.shard_for_node(id).node_property(id, key)
    }

    fn outbound_edges(&self, node_id: NodeId, label: Option<&str>) -> Vec<EdgeInfo> {
        // Outbound edges are stored on the source node's shard
        self.shard_for_node(node_id).outbound_edges(node_id, label)
    }

    fn inbound_edges(&self, node_id: NodeId, label: Option<&str>) -> Vec<EdgeInfo> {
        // Inbound edges: the edge is on the *source* node's shard, so we
        // need to check all shards for edges targeting this node.
        let mut result = Vec::new();
        for shard in &self.shards {
            result.extend(shard.inbound_edges(node_id, label));
        }
        result
    }

    fn edge_property(&self, id: EdgeId, key: &str) -> Value {
        self.shard_for_edge(id).edge_property(id, key)
    }

    fn create_node(
        &mut self,
        label: Option<&str>,
        properties: &[(String, Value)],
    ) -> NodeId {
        let shard_id = self.pick_shard();
        let idx = shard_id as usize;
        self.shards[idx].create_node(label, properties)
    }

    fn create_edge(
        &mut self,
        source: NodeId,
        target: NodeId,
        label: Option<&str>,
        properties: &[(String, Value)],
    ) -> EdgeId {
        // Edge is owned by the source node's shard
        self.shard_for_node_mut(source)
            .create_edge(source, target, label, properties)
    }

    fn set_node_property(&mut self, id: NodeId, key: &str, value: &Value) {
        self.shard_for_node_mut(id).set_node_property(id, key, value);
    }

    fn set_edge_property(&mut self, id: EdgeId, key: &str, value: &Value) {
        self.shard_for_edge_mut(id).set_edge_property(id, key, value);
    }

    fn delete_node(&mut self, id: NodeId) {
        self.shard_for_node_mut(id).delete_node(id);
    }

    fn delete_edge(&mut self, id: EdgeId) {
        self.shard_for_edge_mut(id).delete_edge(id);
    }

    fn begin_tx(&mut self) -> u64 {
        let tx_id = self.next_tx_id;
        self.next_tx_id += 1;
        self.current_tx = tx_id;
        for shard in &mut self.shards {
            shard.begin_tx_with_id(tx_id);
        }
        tx_id
    }

    fn commit_tx(&mut self) {
        for shard in &mut self.shards {
            shard.commit_current_tx();
        }
        self.current_tx = 0;
    }

    fn abort_tx(&mut self) {
        for shard in &mut self.shards {
            shard.abort_current_tx();
        }
        self.current_tx = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graft_query::Value;

    #[test]
    fn create_and_match() {
        let mut db = Database::new();
        db.query("CREATE (a:Person {name: 'Alice', age: 30})").unwrap();
        db.query("CREATE (b:Person {name: 'Bob', age: 25})").unwrap();

        let result = db.query("MATCH (p:Person) RETURN p.name ORDER BY p.name").unwrap();
        assert_eq!(result.columns, vec!["p.name"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::String("Alice".into()));
        assert_eq!(result.rows[1][0], Value::String("Bob".into()));
    }

    #[test]
    fn create_edge_and_traverse() {
        let mut db = Database::new();
        db.query("CREATE (a:Person {name: 'Alice'})").unwrap();
        db.query("CREATE (b:Person {name: 'Bob'})").unwrap();
        db.query(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
             CREATE (a)-[e:KNOWS {since: 2020}]->(b)",
        )
        .unwrap();

        let result = db
            .query(
                "MATCH (a:Person)-[e:KNOWS]->(b:Person) \
                 RETURN a.name, b.name, e.since",
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::String("Alice".into()));
        assert_eq!(result.rows[0][1], Value::String("Bob".into()));
        assert_eq!(result.rows[0][2], Value::Int(2020));
    }

    #[test]
    fn where_filter() {
        let mut db = Database::new();
        db.query("CREATE (:Person {name: 'Alice', age: 30})").unwrap();
        db.query("CREATE (:Person {name: 'Bob', age: 25})").unwrap();
        db.query("CREATE (:Person {name: 'Charlie', age: 35})").unwrap();

        let result = db.query("MATCH (p:Person) WHERE p.age > 28 RETURN p.name ORDER BY p.name").unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::String("Alice".into()));
        assert_eq!(result.rows[1][0], Value::String("Charlie".into()));
    }

    #[test]
    fn set_property() {
        let mut db = Database::new();
        db.query("CREATE (:Person {name: 'Alice', age: 30})").unwrap();
        db.query("MATCH (p:Person {name: 'Alice'}) SET p.age = 31").unwrap();

        let result = db.query("MATCH (p:Person {name: 'Alice'}) RETURN p.age").unwrap();
        assert_eq!(result.rows[0][0], Value::Int(31));
    }

    #[test]
    fn delete_node() {
        let mut db = Database::new();
        db.query("CREATE (:Person {name: 'Alice'})").unwrap();
        db.query("MATCH (p:Person {name: 'Alice'}) DELETE p").unwrap();

        let result = db.query("MATCH (p:Person) RETURN p.name").unwrap();
        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn aggregation_count() {
        let mut db = Database::new();
        db.query("CREATE (:Person {name: 'Alice'})").unwrap();
        db.query("CREATE (:Person {name: 'Bob'})").unwrap();
        db.query("CREATE (:Person {name: 'Charlie'})").unwrap();

        let result = db.query("MATCH (p:Person) RETURN COUNT(p)").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Int(3));
    }

    #[test]
    fn limit_and_skip() {
        let mut db = Database::new();
        db.query("CREATE (:N {v: 1})").unwrap();
        db.query("CREATE (:N {v: 2})").unwrap();
        db.query("CREATE (:N {v: 3})").unwrap();
        db.query("CREATE (:N {v: 4})").unwrap();

        let result = db.query("MATCH (n:N) RETURN n.v ORDER BY n.v SKIP 1 LIMIT 2").unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::Int(2));
        assert_eq!(result.rows[1][0], Value::Int(3));
    }

    #[test]
    fn multi_hop_traversal() {
        let mut db = Database::new();
        db.query("CREATE (:Person {name: 'Alice'})").unwrap();
        db.query("CREATE (:Person {name: 'Bob'})").unwrap();
        db.query("CREATE (:Person {name: 'Charlie'})").unwrap();

        db.query(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
             CREATE (a)-[:KNOWS]->(b)",
        )
        .unwrap();
        db.query(
            "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) \
             CREATE (b)-[:KNOWS]->(c)",
        )
        .unwrap();

        // Two-hop: Alice -> Bob -> Charlie
        let result = db
            .query(
                "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
                 RETURN a.name, b.name, c.name",
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::String("Alice".into()));
        assert_eq!(result.rows[0][1], Value::String("Bob".into()));
        assert_eq!(result.rows[0][2], Value::String("Charlie".into()));
    }

    #[test]
    fn parse_error_reported() {
        let mut db = Database::new();
        let result = db.query("INVALID QUERY");
        assert!(result.is_err());
    }

    // -- Variable-length path tests ------------------------------------------

    fn setup_chain(db: &mut Database) {
        // A -> B -> C -> D
        db.query("CREATE (:N {name: 'A'})").unwrap();
        db.query("CREATE (:N {name: 'B'})").unwrap();
        db.query("CREATE (:N {name: 'C'})").unwrap();
        db.query("CREATE (:N {name: 'D'})").unwrap();
        db.query("MATCH (a:N {name: 'A'}), (b:N {name: 'B'}) CREATE (a)-[:E]->(b)").unwrap();
        db.query("MATCH (b:N {name: 'B'}), (c:N {name: 'C'}) CREATE (b)-[:E]->(c)").unwrap();
        db.query("MATCH (c:N {name: 'C'}), (d:N {name: 'D'}) CREATE (c)-[:E]->(d)").unwrap();
    }

    #[test]
    fn var_length_star() {
        // *  = 1..unbounded — from A should reach B, C, D
        let mut db = Database::new();
        setup_chain(&mut db);
        let result = db.query(
            "MATCH (a:N {name: 'A'})-[:E*]->(b:N) RETURN b.name ORDER BY b.name",
        ).unwrap();
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0][0], Value::String("B".into()));
        assert_eq!(result.rows[1][0], Value::String("C".into()));
        assert_eq!(result.rows[2][0], Value::String("D".into()));
    }

    #[test]
    fn var_length_exact() {
        // *2 = exactly 2 hops — from A should reach C only
        let mut db = Database::new();
        setup_chain(&mut db);
        let result = db.query(
            "MATCH (a:N {name: 'A'})-[:E*2]->(b:N) RETURN b.name",
        ).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::String("C".into()));
    }

    #[test]
    fn var_length_range() {
        // *1..2 = 1 to 2 hops — from A should reach B and C
        let mut db = Database::new();
        setup_chain(&mut db);
        let result = db.query(
            "MATCH (a:N {name: 'A'})-[:E*1..2]->(b:N) RETURN b.name ORDER BY b.name",
        ).unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::String("B".into()));
        assert_eq!(result.rows[1][0], Value::String("C".into()));
    }

    #[test]
    fn var_length_min_only() {
        // *2.. = 2+ hops — from A should reach C and D
        let mut db = Database::new();
        setup_chain(&mut db);
        let result = db.query(
            "MATCH (a:N {name: 'A'})-[:E*2..]->(b:N) RETURN b.name ORDER BY b.name",
        ).unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::String("C".into()));
        assert_eq!(result.rows[1][0], Value::String("D".into()));
    }

    #[test]
    fn var_length_max_only() {
        // *..2 = 1 to 2 hops — from A should reach B and C
        let mut db = Database::new();
        setup_chain(&mut db);
        let result = db.query(
            "MATCH (a:N {name: 'A'})-[:E*..2]->(b:N) RETURN b.name ORDER BY b.name",
        ).unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::String("B".into()));
        assert_eq!(result.rows[1][0], Value::String("C".into()));
    }

    // -- Built-in scalar functions -------------------------------------------

    #[test]
    fn function_id() {
        let mut db = Database::new();
        db.query("CREATE (:Person {name: 'Alice'})").unwrap();
        let result = db.query("MATCH (p:Person) RETURN id(p)").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert!(matches!(&result.rows[0][0], Value::Int(n) if *n > 0));
    }

    #[test]
    fn function_type() {
        let mut db = Database::new();
        db.query("CREATE (:Person {name: 'Alice'})").unwrap();
        db.query("CREATE (:Person {name: 'Bob'})").unwrap();
        db.query(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
             CREATE (a)-[:KNOWS]->(b)",
        )
        .unwrap();

        let result = db
            .query("MATCH (a:Person)-[e:KNOWS]->(b:Person) RETURN type(e)")
            .unwrap();
        assert_eq!(result.rows[0][0], Value::String("KNOWS".into()));
    }

    #[test]
    fn function_labels() {
        let mut db = Database::new();
        db.query("CREATE (:Person {name: 'Alice'})").unwrap();
        let result = db.query("MATCH (p:Person) RETURN labels(p)").unwrap();
        assert_eq!(result.rows[0][0], Value::String("Person".into()));
    }

    #[test]
    fn function_to_string() {
        let mut db = Database::new();
        db.query("CREATE (:N {v: 42})").unwrap();
        let result = db.query("MATCH (n:N) RETURN toString(n.v)").unwrap();
        assert_eq!(result.rows[0][0], Value::String("42".into()));
    }

    #[test]
    fn function_to_integer() {
        let mut db = Database::new();
        db.query("CREATE (:N {v: '123'})").unwrap();
        let result = db.query("MATCH (n:N) RETURN toInteger(n.v)").unwrap();
        assert_eq!(result.rows[0][0], Value::Int(123));
    }

    #[test]
    fn function_to_float() {
        let mut db = Database::new();
        db.query("CREATE (:N {v: '3.14'})").unwrap();
        let result = db.query("MATCH (n:N) RETURN toFloat(n.v)").unwrap();
        assert!(matches!(&result.rows[0][0], Value::Float(f) if (*f - 3.14).abs() < 0.001));
    }

    // -- IS NULL / IS NOT NULL ------------------------------------------------

    #[test]
    fn is_null() {
        let mut db = Database::new();
        db.query("CREATE (:N {name: 'a'})").unwrap();
        db.query("CREATE (:N {name: 'b', email: 'b@test'})").unwrap();

        let result = db
            .query("MATCH (n:N) WHERE n.email IS NULL RETURN n.name")
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::String("a".into()));
    }

    #[test]
    fn is_not_null() {
        let mut db = Database::new();
        db.query("CREATE (:N {name: 'a'})").unwrap();
        db.query("CREATE (:N {name: 'b', email: 'b@test'})").unwrap();

        let result = db
            .query("MATCH (n:N) WHERE n.email IS NOT NULL RETURN n.name")
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::String("b".into()));
    }

    // -- COUNT(*) -------------------------------------------------------------

    #[test]
    fn count_star() {
        let mut db = Database::new();
        db.query("CREATE (:N {v: 1})").unwrap();
        db.query("CREATE (:N {v: 2})").unwrap();
        db.query("CREATE (:N {v: 3})").unwrap();

        let result = db.query("MATCH (n:N) RETURN COUNT(*)").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Int(3));
    }

    // -- String predicates ----------------------------------------------------

    #[test]
    fn string_contains() {
        let mut db = Database::new();
        db.query("CREATE (:N {name: 'Alice'})").unwrap();
        db.query("CREATE (:N {name: 'Bob'})").unwrap();
        db.query("CREATE (:N {name: 'Alicia'})").unwrap();

        let result = db
            .query("MATCH (n:N) WHERE n.name CONTAINS 'lic' RETURN n.name ORDER BY n.name")
            .unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::String("Alice".into()));
        assert_eq!(result.rows[1][0], Value::String("Alicia".into()));
    }

    #[test]
    fn string_starts_with() {
        let mut db = Database::new();
        db.query("CREATE (:N {name: 'Alice'})").unwrap();
        db.query("CREATE (:N {name: 'Bob'})").unwrap();
        db.query("CREATE (:N {name: 'Alicia'})").unwrap();

        let result = db
            .query("MATCH (n:N) WHERE n.name STARTS WITH 'Al' RETURN n.name ORDER BY n.name")
            .unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::String("Alice".into()));
        assert_eq!(result.rows[1][0], Value::String("Alicia".into()));
    }

    #[test]
    fn string_ends_with() {
        let mut db = Database::new();
        db.query("CREATE (:N {name: 'Alice'})").unwrap();
        db.query("CREATE (:N {name: 'Bob'})").unwrap();
        db.query("CREATE (:N {name: 'Alicia'})").unwrap();

        let result = db
            .query("MATCH (n:N) WHERE n.name ENDS WITH 'ce' RETURN n.name")
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::String("Alice".into()));
    }

    #[test]
    fn return_distinct() {
        let mut db = Database::new();
        // Two paths to same destination: A->B->D and A->C->D
        db.query("CREATE (:N {name: 'A'})").unwrap();
        db.query("CREATE (:N {name: 'B'})").unwrap();
        db.query("CREATE (:N {name: 'C'})").unwrap();
        db.query("CREATE (:N {name: 'D'})").unwrap();
        db.query("MATCH (a:N {name: 'A'}), (b:N {name: 'B'}) CREATE (a)-[:E]->(b)").unwrap();
        db.query("MATCH (a:N {name: 'A'}), (c:N {name: 'C'}) CREATE (a)-[:E]->(c)").unwrap();
        db.query("MATCH (b:N {name: 'B'}), (d:N {name: 'D'}) CREATE (b)-[:E]->(d)").unwrap();
        db.query("MATCH (c:N {name: 'C'}), (d:N {name: 'D'}) CREATE (c)-[:E]->(d)").unwrap();

        // Without DISTINCT: two-hop from A reaches D twice (via B and via C)
        let result = db.query(
            "MATCH (a:N {name: 'A'})-[:E]->(m:N)-[:E]->(d:N) RETURN d.name",
        ).unwrap();
        assert_eq!(result.rows.len(), 2);

        // With DISTINCT: D appears only once
        let result = db.query(
            "MATCH (a:N {name: 'A'})-[:E]->(m:N)-[:E]->(d:N) RETURN DISTINCT d.name",
        ).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::String("D".into()));
    }

    // -- Multi-shard tests ----------------------------------------------------

    #[test]
    fn multi_shard_nodes_distributed() {
        let mut db = Database::with_shards(4);
        // Create 4 nodes — round-robin should place one per shard
        db.query("CREATE (:N {v: 1})").unwrap();
        db.query("CREATE (:N {v: 2})").unwrap();
        db.query("CREATE (:N {v: 3})").unwrap();
        db.query("CREATE (:N {v: 4})").unwrap();

        // All 4 should be visible via scan
        let result = db
            .query("MATCH (n:N) RETURN n.v ORDER BY n.v")
            .unwrap();
        assert_eq!(result.rows.len(), 4);
        assert_eq!(result.rows[0][0], Value::Int(1));
        assert_eq!(result.rows[3][0], Value::Int(4));

        // Verify they live on different shards
        let result = db.query("MATCH (n:N) RETURN id(n)").unwrap();
        let mut shards: Vec<u8> = result
            .rows
            .iter()
            .map(|r| match &r[0] {
                Value::Int(id) => graft_core::NodeId::from_raw(*id as u64).shard(),
                _ => panic!("expected int"),
            })
            .collect();
        shards.sort();
        shards.dedup();
        assert_eq!(shards.len(), 4, "nodes should be on 4 different shards");
    }

    #[test]
    fn multi_shard_cross_shard_traversal() {
        let mut db = Database::with_shards(2);
        // With 2 shards and round-robin, Alice goes to shard 0, Bob to shard 1
        db.query("CREATE (:Person {name: 'Alice'})").unwrap();
        db.query("CREATE (:Person {name: 'Bob'})").unwrap();
        db.query(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
             CREATE (a)-[:KNOWS {since: 2024}]->(b)",
        )
        .unwrap();

        // Traverse across shards
        let result = db
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
    fn multi_shard_aggregate_across_shards() {
        let mut db = Database::with_shards(4);
        for i in 0..8 {
            db.query(&format!("CREATE (:N {{v: {i}}})")).unwrap();
        }

        let result = db.query("MATCH (n:N) RETURN COUNT(*)").unwrap();
        assert_eq!(result.rows[0][0], Value::Int(8));

        let result = db.query("MATCH (n:N) RETURN SUM(n.v)").unwrap();
        // 0+1+2+3+4+5+6+7 = 28
        assert!(matches!(&result.rows[0][0], Value::Float(f) if (*f - 28.0).abs() < 0.01));
    }

    #[test]
    fn multi_shard_delete_cross_shard() {
        let mut db = Database::with_shards(2);
        db.query("CREATE (:N {name: 'a'})").unwrap();
        db.query("CREATE (:N {name: 'b'})").unwrap();

        let result = db.query("MATCH (n:N) RETURN n.name").unwrap();
        assert_eq!(result.rows.len(), 2);

        db.query("MATCH (n:N {name: 'a'}) DELETE n").unwrap();

        let result = db.query("MATCH (n:N) RETURN n.name").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::String("b".into()));
    }

    #[test]
    fn multi_shard_set_property_cross_shard() {
        let mut db = Database::with_shards(2);
        db.query("CREATE (:N {name: 'a', v: 1})").unwrap();
        db.query("CREATE (:N {name: 'b', v: 2})").unwrap();

        db.query("MATCH (n:N {name: 'b'}) SET n.v = 99").unwrap();

        let result = db
            .query("MATCH (n:N) RETURN n.name, n.v ORDER BY n.name")
            .unwrap();
        assert_eq!(result.rows[0][0], Value::String("a".into()));
        assert_eq!(result.rows[0][1], Value::Int(1));
        assert_eq!(result.rows[1][0], Value::String("b".into()));
        assert_eq!(result.rows[1][1], Value::Int(99));
    }

    #[test]
    fn multi_shard_multi_hop() {
        let mut db = Database::with_shards(3);
        db.query("CREATE (:N {name: 'A'})").unwrap();
        db.query("CREATE (:N {name: 'B'})").unwrap();
        db.query("CREATE (:N {name: 'C'})").unwrap();

        db.query(
            "MATCH (a:N {name: 'A'}), (b:N {name: 'B'}) CREATE (a)-[:E]->(b)",
        )
        .unwrap();
        db.query(
            "MATCH (b:N {name: 'B'}), (c:N {name: 'C'}) CREATE (b)-[:E]->(c)",
        )
        .unwrap();

        let result = db
            .query(
                "MATCH (a:N {name: 'A'})-[:E]->(b:N)-[:E]->(c:N) \
                 RETURN c.name",
            )
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::String("C".into()));
    }
}
