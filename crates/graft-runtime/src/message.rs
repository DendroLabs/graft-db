use graft_core::{NodeId, ShardId};
use graft_query::executor::{EdgeInfo, NodeInfo, Value};

/// Messages sent between shards via SPSC ring buffers.
///
/// In the shard-per-core model, each shard owns its data partition and
/// communicates with other shards exclusively through these messages.
/// No shared mutable state in the hot path.
#[derive(Debug)]
pub enum ShardMessage {
    // -- Requests (shard A asks shard B) -------------------------------------

    /// Request: scan nodes with optional label filter on the target shard.
    ScanNodes {
        request_id: u64,
        reply_to: ShardId,
        label: Option<String>,
    },

    /// Request: get a specific node by ID.
    GetNode {
        request_id: u64,
        reply_to: ShardId,
        node_id: NodeId,
    },

    /// Request: get a node property.
    GetNodeProperty {
        request_id: u64,
        reply_to: ShardId,
        node_id: NodeId,
        key: String,
    },

    /// Request: get outbound edges from a node.
    GetOutboundEdges {
        request_id: u64,
        reply_to: ShardId,
        node_id: NodeId,
        label: Option<String>,
    },

    /// Request: get inbound edges to a node.
    GetInboundEdges {
        request_id: u64,
        reply_to: ShardId,
        node_id: NodeId,
        label: Option<String>,
    },

    // -- Responses -----------------------------------------------------------

    /// Response: here are the nodes from a scan.
    ScanNodesResult {
        request_id: u64,
        nodes: Vec<NodeInfo>,
    },

    /// Response: here is a node (or None).
    GetNodeResult {
        request_id: u64,
        node: Option<NodeInfo>,
    },

    /// Response: here is a property value.
    GetNodePropertyResult {
        request_id: u64,
        value: Value,
    },

    /// Response: here are the edges.
    EdgesResult {
        request_id: u64,
        edges: Vec<EdgeInfo>,
    },
}

impl ShardMessage {
    pub fn request_id(&self) -> u64 {
        match self {
            Self::ScanNodes { request_id, .. }
            | Self::GetNode { request_id, .. }
            | Self::GetNodeProperty { request_id, .. }
            | Self::GetOutboundEdges { request_id, .. }
            | Self::GetInboundEdges { request_id, .. }
            | Self::ScanNodesResult { request_id, .. }
            | Self::GetNodeResult { request_id, .. }
            | Self::GetNodePropertyResult { request_id, .. }
            | Self::EdgesResult { request_id, .. } => *request_id,
        }
    }
}
