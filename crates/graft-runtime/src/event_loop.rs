use std::thread;

use graft_core::ShardId;
use graft_query::executor::StorageAccess;
use graft_repl::{ReplicationReceiver, ReplicationSender};

use crate::cluster::{Request, Response};
use crate::message::ShardMessage;
use crate::shard::Shard;
use crate::spsc::{Consumer, Producer};

const SPIN_LIMIT: u32 = 64;

/// Per-shard event loop context.
///
/// Each shard runs a cooperative event loop on its pinned OS thread:
///   1. poll_coordinator — drain coordinator requests, produce responses
///   2. poll_io          — check for completed I/O operations
///   3. poll_messages    — drain inbound messages from other shards
///   4. advance_queries  — make progress on active queries
///   5. submit_io        — enqueue new I/O operations
pub struct ShardEventLoop {
    pub shard: Shard,
    /// One consumer per other shard (messages arriving here).
    inbound: Vec<(ShardId, Consumer<ShardMessage>)>,
    /// One producer per other shard (messages going out).
    outbound: Vec<(ShardId, Producer<ShardMessage>)>,
    /// Coordinator channel — requests from the ShardCluster coordinator.
    coordinator_rx: Option<Consumer<Request>>,
    /// Coordinator channel — responses back to the ShardCluster coordinator.
    coordinator_tx: Option<Producer<Response>>,
    /// Set to true when a Shutdown request is received.
    should_shutdown: bool,
    /// Statistics
    messages_received: u64,
    messages_sent: u64,
    ticks: u64,
    /// Replication sender (primary mode) — ships committed WAL records to replicas.
    repl_sender: Option<ReplicationSender>,
    /// Replication receiver (replica mode) — receives and applies WAL records from primary.
    repl_receiver: Option<ReplicationReceiver>,
}

impl ShardEventLoop {
    pub fn new(
        shard: Shard,
        inbound: Vec<(ShardId, Consumer<ShardMessage>)>,
        outbound: Vec<(ShardId, Producer<ShardMessage>)>,
    ) -> Self {
        Self {
            shard,
            inbound,
            outbound,
            coordinator_rx: None,
            coordinator_tx: None,
            should_shutdown: false,
            messages_received: 0,
            messages_sent: 0,
            ticks: 0,
            repl_sender: None,
            repl_receiver: None,
        }
    }

    /// Create an event loop with coordinator channels for use in ShardCluster.
    pub(crate) fn with_coordinator(
        shard: Shard,
        coordinator_rx: Consumer<Request>,
        coordinator_tx: Producer<Response>,
    ) -> Self {
        Self {
            shard,
            inbound: Vec::new(),
            outbound: Vec::new(),
            coordinator_rx: Some(coordinator_rx),
            coordinator_tx: Some(coordinator_tx),
            should_shutdown: false,
            messages_received: 0,
            messages_sent: 0,
            ticks: 0,
            repl_sender: None,
            repl_receiver: None,
        }
    }

    /// Set the replication sender (primary mode).
    pub fn set_repl_sender(&mut self, sender: ReplicationSender) {
        self.repl_sender = Some(sender);
    }

    /// Set the replication receiver (replica mode).
    pub fn set_repl_receiver(&mut self, receiver: ReplicationReceiver) {
        self.shard.set_read_only(true);
        self.repl_receiver = Some(receiver);
    }

    /// Access the replication sender (if in primary mode).
    pub fn repl_sender(&self) -> Option<&ReplicationSender> {
        self.repl_sender.as_ref()
    }

    /// Mutable access to the replication sender.
    pub fn repl_sender_mut(&mut self) -> Option<&mut ReplicationSender> {
        self.repl_sender.as_mut()
    }

    /// Access the replication receiver (if in replica mode).
    pub fn repl_receiver(&self) -> Option<&ReplicationReceiver> {
        self.repl_receiver.as_ref()
    }

    /// Mutable access to the replication receiver.
    pub fn repl_receiver_mut(&mut self) -> Option<&mut ReplicationReceiver> {
        self.repl_receiver.as_mut()
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard.shard_id()
    }

    pub fn ticks(&self) -> u64 {
        self.ticks
    }

    pub fn messages_received(&self) -> u64 {
        self.messages_received
    }

    pub fn messages_sent(&self) -> u64 {
        self.messages_sent
    }

    /// Run the event loop until shutdown. Used when the event loop is
    /// spawned as a ShardCluster worker thread.
    pub(crate) fn run(mut self) {
        let mut idle_count: u32 = 0;
        loop {
            let work = self.tick();
            if self.should_shutdown {
                return;
            }
            if work > 0 {
                idle_count = 0;
            } else {
                idle_count += 1;
                if idle_count > SPIN_LIMIT {
                    thread::yield_now();
                } else {
                    std::hint::spin_loop();
                }
            }
        }
    }

    /// Run one tick of the event loop. Returns the number of items processed.
    pub fn tick(&mut self) -> usize {
        self.ticks += 1;
        let mut work_done = 0;

        // Phase 1: poll_coordinator
        work_done += self.poll_coordinator();

        // Phase 2: poll_io (placeholder for io_uring/posix integration)
        // self.poll_io();

        // Phase 3: poll_messages
        work_done += self.poll_messages();

        // Phase 4: advance_queries (placeholder for query state machines)
        // self.advance_queries();

        // Phase 5: poll_replication
        work_done += self.poll_replication();

        // Phase 6: submit_io (placeholder)
        // self.submit_io();

        work_done
    }

    /// Poll replication: on primary, check for committed WAL records to ship;
    /// on replica, apply received records to the shard.
    fn poll_replication(&mut self) -> usize {
        let mut work = 0;

        // Primary: poll sender for outbound batches
        if let Some(ref mut sender) = self.repl_sender {
            sender.poll();
            let batches = sender.drain_outbound();
            work += batches.len();
            // Batches are available for the network layer to send.
            // In Phase 8b, these will be written to TCP streams.
            // For now, they are produced and dropped (network not yet wired).
            let _ = batches;
        }

        // Replica: apply pending records from receiver
        if let Some(ref mut receiver) = self.repl_receiver {
            let records = receiver.drain_records();
            work += records.len();
            for record in &records {
                self.shard.apply_wal_record(record);
            }
        }

        work
    }

    /// Drain coordinator requests and produce responses.
    fn poll_coordinator(&mut self) -> usize {
        if self.coordinator_rx.is_none() {
            return 0;
        }

        // Drain all requests first to release the borrow on self
        let mut batch = Vec::new();
        while let Some(req) = self.coordinator_rx.as_ref().unwrap().pop() {
            batch.push(req);
        }

        let count = batch.len();
        for req in batch {
            if matches!(req, Request::Shutdown) {
                let _ = self.shard.flush();
                self.should_shutdown = true;
                self.send_response(Response::Done);
                return count;
            }
            let resp = self.handle_request(req);
            self.send_response(resp);
        }
        count
    }

    fn send_response(&self, resp: Response) {
        if let Some(ref tx) = self.coordinator_tx {
            let mut r = resp;
            loop {
                match tx.push(r) {
                    Ok(()) => break,
                    Err(returned) => {
                        r = returned;
                        std::hint::spin_loop();
                    }
                }
            }
        }
    }

    fn handle_request(&mut self, req: Request) -> Response {
        match req {
            Request::Flush => {
                let _ = self.shard.flush();
                Response::Done
            }
            Request::ScanNodes { label } => {
                Response::Nodes(self.shard.scan_nodes(label.as_deref()))
            }
            Request::GetNode { node_id } => Response::Node(self.shard.get_node(node_id)),
            Request::NodeProperty { node_id, key } => {
                Response::Value(self.shard.node_property(node_id, &key))
            }
            Request::OutboundEdges { node_id, label } => {
                Response::Edges(self.shard.outbound_edges(node_id, label.as_deref()))
            }
            Request::InboundEdges { node_id, label } => {
                Response::Edges(self.shard.inbound_edges(node_id, label.as_deref()))
            }
            Request::EdgeProperty { edge_id, key } => {
                Response::Value(self.shard.edge_property(edge_id, &key))
            }
            Request::CreateNode { label, properties } => {
                let id = self.shard.create_node(label.as_deref(), &properties);
                Response::NodeId(id)
            }
            Request::CreateEdge {
                source,
                target,
                label,
                properties,
            } => {
                let id = self
                    .shard
                    .create_edge(source, target, label.as_deref(), &properties);
                Response::EdgeId(id)
            }
            Request::SetNodeProperty {
                node_id,
                key,
                value,
            } => {
                self.shard.set_node_property(node_id, &key, &value);
                Response::Done
            }
            Request::SetEdgeProperty {
                edge_id,
                key,
                value,
            } => {
                self.shard.set_edge_property(edge_id, &key, &value);
                Response::Done
            }
            Request::DeleteNode { node_id } => {
                self.shard.delete_node(node_id);
                Response::Done
            }
            Request::DeleteEdge { edge_id } => {
                self.shard.delete_edge(edge_id);
                Response::Done
            }
            Request::BeginTx { tx_id } => {
                self.shard.begin_tx_with_id(tx_id);
                Response::Done
            }
            Request::SetActiveTx { tx_id } => {
                self.shard.set_active_tx(tx_id);
                Response::Done
            }
            Request::CommitTx { tx_id } => {
                self.shard.set_active_tx(tx_id);
                self.shard.commit_current_tx();
                Response::Done
            }
            Request::AbortTx { tx_id } => {
                self.shard.set_active_tx(tx_id);
                self.shard.abort_current_tx();
                Response::Done
            }
            Request::Shutdown => unreachable!("handled in poll_coordinator"),
        }
    }

    /// Drain all inbound message queues and handle each message.
    fn poll_messages(&mut self) -> usize {
        // Drain all messages first to avoid borrow conflict
        let mut batch = Vec::new();
        for (_from_shard, consumer) in &self.inbound {
            while let Some(msg) = consumer.pop() {
                batch.push(msg);
            }
        }
        let count = batch.len();
        for msg in batch {
            self.handle_message(msg);
        }
        self.messages_received += count as u64;
        count
    }

    /// Handle a single inbound message.
    fn handle_message(&mut self, msg: ShardMessage) {
        match msg {
            ShardMessage::ScanNodes {
                request_id,
                reply_to,
                label,
            } => {
                let nodes = self.shard.scan_nodes(label.as_deref());
                self.send_to(
                    reply_to,
                    ShardMessage::ScanNodesResult { request_id, nodes },
                );
            }
            ShardMessage::GetNode {
                request_id,
                reply_to,
                node_id,
            } => {
                let node = self.shard.get_node(node_id);
                self.send_to(reply_to, ShardMessage::GetNodeResult { request_id, node });
            }
            ShardMessage::GetNodeProperty {
                request_id,
                reply_to,
                node_id,
                key,
            } => {
                let value = self.shard.node_property(node_id, &key);
                self.send_to(
                    reply_to,
                    ShardMessage::GetNodePropertyResult { request_id, value },
                );
            }
            ShardMessage::GetOutboundEdges {
                request_id,
                reply_to,
                node_id,
                label,
            } => {
                let edges = self.shard.outbound_edges(node_id, label.as_deref());
                self.send_to(reply_to, ShardMessage::EdgesResult { request_id, edges });
            }
            ShardMessage::GetInboundEdges {
                request_id,
                reply_to,
                node_id,
                label,
            } => {
                let edges = self.shard.inbound_edges(node_id, label.as_deref());
                self.send_to(reply_to, ShardMessage::EdgesResult { request_id, edges });
            }
            // Responses are collected by whoever is waiting
            ShardMessage::ScanNodesResult { .. }
            | ShardMessage::GetNodeResult { .. }
            | ShardMessage::GetNodePropertyResult { .. }
            | ShardMessage::EdgesResult { .. } => {
                // In the future, these feed into the query state machine.
                // For now, they are handled in-line by tests.
            }
        }
    }

    /// Send a message to another shard.
    fn send_to(&self, target: ShardId, msg: ShardMessage) {
        for (id, producer) in &self.outbound {
            if *id == target {
                // In a real system, a full queue triggers backpressure.
                // For now, panic on full queue (indicates a bug in tests).
                producer
                    .push(msg)
                    .unwrap_or_else(|_| panic!("SPSC queue to shard {target} is full"));
                return;
            }
        }
    }
}

/// Build a mesh of event loops connected by SPSC channels.
///
/// Returns one `ShardEventLoop` per shard, fully interconnected.
/// Each shard has an inbound queue from every other shard, and an
/// outbound queue to every other shard.
pub fn build_shard_mesh(shard_count: usize, queue_capacity: usize) -> Vec<ShardEventLoop> {
    use crate::spsc;

    let n = shard_count;

    // Create all channels: channels[from][to] = (producer, consumer)
    // We only need channels where from != to.
    let mut producers: Vec<Vec<Option<Producer<ShardMessage>>>> =
        (0..n).map(|_| (0..n).map(|_| None).collect()).collect();
    let mut consumers: Vec<Vec<Option<Consumer<ShardMessage>>>> =
        (0..n).map(|_| (0..n).map(|_| None).collect()).collect();

    for from in 0..n {
        for to in 0..n {
            if from != to {
                let (tx, rx) = spsc::channel(queue_capacity);
                producers[from][to] = Some(tx);
                consumers[from][to] = Some(rx);
            }
        }
    }

    let mut loops = Vec::with_capacity(n);
    for shard_id in 0..n {
        let shard = Shard::new(shard_id as ShardId);

        // Inbound: messages arriving at this shard (consumer end)
        // channels[from][shard_id] — consumer for messages from `from`
        let inbound: Vec<(ShardId, Consumer<ShardMessage>)> = (0..n)
            .filter(|&from| from != shard_id)
            .map(|from| {
                let consumer = consumers[from][shard_id].take().unwrap();
                (from as ShardId, consumer)
            })
            .collect();

        // Outbound: messages leaving this shard (producer end)
        // channels[shard_id][to] — producer for messages to `to`
        let outbound: Vec<(ShardId, Producer<ShardMessage>)> = (0..n)
            .filter(|&to| to != shard_id)
            .map(|to| {
                let producer = producers[shard_id][to].take().unwrap();
                (to as ShardId, producer)
            })
            .collect();

        loops.push(ShardEventLoop::new(shard, inbound, outbound));
    }

    loops
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::ShardMessage;
    use graft_query::executor::{StorageAccess, Value};

    #[test]
    fn build_mesh_creates_connected_shards() {
        let loops = build_shard_mesh(3, 64);
        assert_eq!(loops.len(), 3);
        assert_eq!(loops[0].shard_id(), 0);
        assert_eq!(loops[1].shard_id(), 1);
        assert_eq!(loops[2].shard_id(), 2);
        // Each shard should have inbound from 2 others
        assert_eq!(loops[0].inbound.len(), 2);
        assert_eq!(loops[0].outbound.len(), 2);
    }

    #[test]
    fn message_passing_between_shards() {
        let mut loops = build_shard_mesh(2, 64);

        // Add data to shard 0
        loops[0].shard.create_node(
            Some("Person"),
            &[("name".into(), Value::String("Alice".into()))],
        );

        // Shard 1 requests a scan from shard 0
        // We manually push a message from shard 1's outbound to shard 0's inbound
        loops[1].outbound[0]
            .1
            .push(ShardMessage::ScanNodes {
                request_id: 1,
                reply_to: 1,
                label: Some("Person".into()),
            })
            .unwrap();

        // Shard 0 processes the message
        let processed = loops[0].tick();
        assert_eq!(processed, 1);

        // Shard 1 should have a response waiting
        let processed = loops[1].tick();
        // The response is a ScanNodesResult — currently just consumed by handle_message
        // In real system it would feed the query state machine
        assert_eq!(processed, 1);
    }

    #[test]
    fn event_loop_tick_is_cooperative() {
        let mut loops = build_shard_mesh(2, 64);

        // Multiple ticks with no messages = no work
        assert_eq!(loops[0].tick(), 0);
        assert_eq!(loops[0].tick(), 0);
        assert_eq!(loops[0].ticks(), 2);
        assert_eq!(loops[0].messages_received(), 0);
    }

    #[test]
    fn event_loop_handles_coordinator_requests() {
        use crate::cluster::{Request, Response};
        use crate::spsc;

        let shard = Shard::new(0);
        let (req_tx, req_rx) = spsc::channel::<Request>(64);
        let (resp_tx, resp_rx) = spsc::channel::<Response>(64);

        let mut el = ShardEventLoop::with_coordinator(shard, req_rx, resp_tx);

        // Create a node via coordinator request
        req_tx
            .push(Request::CreateNode {
                label: Some("Person".into()),
                properties: vec![("name".into(), Value::String("Alice".into()))],
            })
            .unwrap();

        let work = el.tick();
        assert_eq!(work, 1);

        // Should have a NodeId response
        let resp = resp_rx.pop().unwrap();
        match resp {
            Response::NodeId(id) => assert_eq!(id.shard(), 0),
            other => panic!("expected NodeId, got {:?}", other),
        }

        // Scan nodes via coordinator
        req_tx
            .push(Request::ScanNodes { label: None })
            .unwrap();
        el.tick();
        let resp = resp_rx.pop().unwrap();
        match resp {
            Response::Nodes(nodes) => {
                assert_eq!(nodes.len(), 1);
                assert_eq!(nodes[0].label.as_deref(), Some("Person"));
            }
            other => panic!("expected Nodes, got {:?}", other),
        }
    }

    #[test]
    fn event_loop_coordinator_tx_lifecycle() {
        use crate::cluster::{Request, Response};
        use crate::spsc;

        let shard = Shard::new(0);
        let (req_tx, req_rx) = spsc::channel::<Request>(64);
        let (resp_tx, resp_rx) = spsc::channel::<Response>(64);

        let mut el = ShardEventLoop::with_coordinator(shard, req_rx, resp_tx);

        // Begin tx
        req_tx.push(Request::BeginTx { tx_id: 1 }).unwrap();
        el.tick();
        assert!(matches!(resp_rx.pop().unwrap(), Response::Done));

        // Create node in tx
        req_tx
            .push(Request::CreateNode {
                label: Some("N".into()),
                properties: vec![],
            })
            .unwrap();
        el.tick();
        let _node_id = match resp_rx.pop().unwrap() {
            Response::NodeId(id) => id,
            other => panic!("expected NodeId, got {:?}", other),
        };

        // Commit
        req_tx.push(Request::CommitTx { tx_id: 1 }).unwrap();
        el.tick();
        assert!(matches!(resp_rx.pop().unwrap(), Response::Done));

        // Verify node visible after commit
        req_tx
            .push(Request::BeginTx { tx_id: 2 })
            .unwrap();
        el.tick();
        resp_rx.pop(); // consume Done

        req_tx.push(Request::ScanNodes { label: None }).unwrap();
        el.tick();
        match resp_rx.pop().unwrap() {
            Response::Nodes(nodes) => assert_eq!(nodes.len(), 1),
            other => panic!("expected Nodes, got {:?}", other),
        }
    }

    #[test]
    fn event_loop_shutdown() {
        use crate::cluster::{Request, Response};
        use crate::spsc;

        let shard = Shard::new(0);
        let (req_tx, req_rx) = spsc::channel::<Request>(64);
        let (resp_tx, resp_rx) = spsc::channel::<Response>(64);

        let mut el = ShardEventLoop::with_coordinator(shard, req_rx, resp_tx);

        req_tx.push(Request::Shutdown).unwrap();
        el.tick();

        assert!(el.should_shutdown);
        // Should have sent a Done response
        assert!(matches!(resp_rx.pop().unwrap(), Response::Done));
    }

    #[test]
    fn event_loop_mixed_coordinator_and_mesh() {
        use crate::cluster::{Request, Response};
        use crate::spsc;

        // Create a single-shard event loop with both coordinator and mesh channels
        let shard = Shard::new(0);
        let (req_tx, req_rx) = spsc::channel::<Request>(64);
        let (resp_tx, resp_rx) = spsc::channel::<Response>(64);

        let mut el = ShardEventLoop::with_coordinator(shard, req_rx, resp_tx);

        // Coordinator request
        req_tx
            .push(Request::CreateNode {
                label: Some("N".into()),
                properties: vec![("v".into(), Value::Int(42))],
            })
            .unwrap();

        let work = el.tick();
        assert_eq!(work, 1);
        assert!(matches!(resp_rx.pop().unwrap(), Response::NodeId(_)));

        // No work on idle tick
        let work = el.tick();
        assert_eq!(work, 0);
    }

    #[test]
    fn three_shard_fan_out() {
        let mut loops = build_shard_mesh(3, 64);

        // Add a node to each shard
        loops[0]
            .shard
            .create_node(Some("N"), &[("v".into(), Value::Int(1))]);
        loops[1]
            .shard
            .create_node(Some("N"), &[("v".into(), Value::Int(2))]);
        loops[2]
            .shard
            .create_node(Some("N"), &[("v".into(), Value::Int(3))]);

        // Shard 0 asks shard 1 and shard 2 for their nodes
        for (_, producer) in &loops[0].outbound {
            producer
                .push(ShardMessage::ScanNodes {
                    request_id: 42,
                    reply_to: 0,
                    label: Some("N".into()),
                })
                .unwrap();
        }

        // Shard 1 and 2 process and respond
        loops[1].tick();
        loops[2].tick();

        // Shard 0 should now have 2 responses
        let processed = loops[0].tick();
        assert_eq!(processed, 2);
        assert_eq!(loops[0].messages_received(), 2);
    }
}
