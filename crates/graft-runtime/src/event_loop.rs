use std::thread;

use graft_core::ShardId;
use graft_query::executor::StorageAccess;
use graft_repl::{
    ReplControl, ReplicationReceiver, ReplicationSender, SharedQueue, WalAckMsg, WalBatchMsg,
};

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
    /// Primary: ACKs from replicas via network reader thread.
    repl_ack_inbox: Option<SharedQueue<(String, u64)>>,
    /// Primary: dynamic replica registration from network acceptor.
    repl_control: Option<SharedQueue<ReplControl>>,
    /// Replica: inbound batches from network reader thread.
    repl_inbox: Option<SharedQueue<WalBatchMsg>>,
    /// Replica: outbound ACKs for network writer thread.
    repl_ack_outbox: Option<SharedQueue<WalAckMsg>>,
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
            repl_ack_inbox: None,
            repl_control: None,
            repl_inbox: None,
            repl_ack_outbox: None,
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
            repl_ack_inbox: None,
            repl_control: None,
            repl_inbox: None,
            repl_ack_outbox: None,
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

    /// Set primary-mode replication queues.
    pub fn set_repl_primary_queues(
        &mut self,
        ack_inbox: SharedQueue<(String, u64)>,
        control: SharedQueue<ReplControl>,
    ) {
        self.repl_ack_inbox = Some(ack_inbox);
        self.repl_control = Some(control);
    }

    /// Set replica-mode replication queues.
    pub fn set_repl_replica_queues(
        &mut self,
        inbox: SharedQueue<WalBatchMsg>,
        ack_outbox: SharedQueue<WalAckMsg>,
    ) {
        self.repl_inbox = Some(inbox);
        self.repl_ack_outbox = Some(ack_outbox);
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

    /// Poll replication: on primary, drain WAL records from shard into sender,
    /// which fans new batches out to each registered replica's own outbox.
    /// On replica, drain inbox, apply records, push ACKs.
    fn poll_replication(&mut self) -> usize {
        let mut work = 0;

        // Primary path
        if self.repl_sender.is_some() {
            // 1. Process control messages (register/unregister replicas)
            if let Some(ref control) = self.repl_control {
                let msgs = control.drain();
                work += msgs.len();
                let sender = self.repl_sender.as_mut().unwrap();
                for msg in msgs {
                    match msg {
                        ReplControl::Register {
                            id,
                            shard_id,
                            outbox,
                        } => sender.add_replica(id, shard_id, outbox),
                        ReplControl::Unregister { id } => sender.remove_replica(&id),
                    }
                }
            }

            // 2. Drain repl_log from shard into sender, then poll it — poll()
            // fans any newly-ready batch out to every registered replica's
            // own outbox directly (see ReplicationSender::poll).
            let records = self.shard.drain_repl_log();
            work += records.len();
            {
                let sender = self.repl_sender.as_mut().unwrap();
                for record in records {
                    sender.on_wal_record(record);
                }
                sender.poll();
            }

            // 3. Drain ACK inbox → feed to sender
            if let Some(ref ack_inbox) = self.repl_ack_inbox {
                let acks = ack_inbox.drain();
                work += acks.len();
                let sender = self.repl_sender.as_mut().unwrap();
                for (replica_id, lsn) in acks {
                    sender.on_ack(&replica_id, lsn);
                }
            }
        }

        // Replica path
        if self.repl_receiver.is_some() {
            // 1. Drain inbox → feed to receiver
            if let Some(ref inbox) = self.repl_inbox {
                let batches = inbox.drain();
                work += batches.len();
                let shard_id = self.shard.shard_id();
                let receiver = self.repl_receiver.as_mut().unwrap();
                // Once poisoned by a corrupted/truncated batch, stop calling
                // on_batch entirely: it would just keep returning the same
                // poison error for every remaining batch in this drain, and
                // logging that on every tick forever would be pure spam
                // (the condition was already logged once, below, the tick
                // it happened).
                if !receiver.is_poisoned() {
                    for batch in &batches {
                        if let Err(e) = receiver.on_batch(batch) {
                            // A corrupted/truncated batch can't be locally
                            // repaired and must never be silently skipped
                            // over (finding #6): skipping it would let a
                            // later batch's higher LSN — and its ACK — make
                            // the primary believe this replica has
                            // everything, when it is permanently missing a
                            // transaction. Surface it loudly; the receiver
                            // is now poisoned and stops applying/ACKing
                            // anything further until an operator resyncs it.
                            tracing::error!(
                                shard_id,
                                batch_last_lsn = batch.last_lsn,
                                error = %e,
                                "replication batch rejected; this replica will not apply or ACK \
                                 any further WAL records until a manual resync"
                            );
                            break;
                        }
                    }
                }
            }

            // 2. Drain records → apply to shard
            let records = self.repl_receiver.as_mut().unwrap().drain_records();
            work += records.len();
            for record in &records {
                self.shard.apply_wal_record(record);
            }

            // 3. Drain ACKs → push to outbox
            let acks = self.repl_receiver.as_mut().unwrap().drain_acks();
            work += acks.len();
            if let Some(ref ack_outbox) = self.repl_ack_outbox {
                ack_outbox.push_batch(acks);
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
        req_tx.push(Request::ScanNodes { label: None }).unwrap();
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
        req_tx.push(Request::BeginTx { tx_id: 2 }).unwrap();
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
    fn poll_replication_primary_feeds_outbox() {
        use crate::cluster::{Request, Response};
        use crate::spsc;

        let mut shard = Shard::new(0);
        shard.enable_replication();

        let (req_tx, req_rx) = spsc::channel::<Request>(64);
        let (resp_tx, resp_rx) = spsc::channel::<Response>(64);

        let ack_inbox = SharedQueue::new();
        let control = SharedQueue::new();
        // A registered replica's own outbox — this is what production code
        // creates via ReplicaOutboxRegistry and hands over in a Register
        // control message.
        let replica_outbox = SharedQueue::new();

        let mut el = ShardEventLoop::with_coordinator(shard, req_rx, resp_tx);
        el.set_repl_sender(graft_repl::ReplicationSender::new(0));
        el.set_repl_primary_queues(ack_inbox.clone(), control.clone());

        control.push(ReplControl::Register {
            id: "replica-1".into(),
            shard_id: 0,
            outbox: replica_outbox.clone(),
        });

        // Create data via coordinator
        req_tx.push(Request::BeginTx { tx_id: 1 }).unwrap();
        el.tick();
        resp_rx.pop(); // Done

        req_tx
            .push(Request::CreateNode {
                label: Some("N".into()),
                properties: vec![],
            })
            .unwrap();
        el.tick();
        resp_rx.pop(); // NodeId

        req_tx.push(Request::CommitTx { tx_id: 1 }).unwrap();
        el.tick();
        resp_rx.pop(); // Done

        // poll_replication should have drained repl_log into sender, which
        // fans the resulting batch out to the registered replica's outbox.
        // Might need a few extra ticks for the sender to poll.
        el.tick();
        el.tick();

        let batches = replica_outbox.drain();
        assert!(
            !batches.is_empty(),
            "expected replica outbox to have batches"
        );
        assert_eq!(batches[0].shard_id, 0);
    }

    #[test]
    fn poll_replication_replica_applies_from_inbox() {
        use crate::cluster::{Request, Response};
        use crate::spsc;
        use graft_repl::ReplicationSender;

        // Create a batch using sender (simulating primary)
        let mut sender = ReplicationSender::new(0);
        use graft_txn::wal::{WalBody, WalRecord, WalRecordType};
        sender.on_wal_record(WalRecord {
            lsn: 1,
            tx_id: 1,
            record_type: WalRecordType::Begin,
            body: WalBody::Empty,
        });
        // Create a node page write
        use graft_core::NodeId;
        use graft_storage::NodeRecord;
        let node_id = NodeId::new(0, 1);
        let node_rec = NodeRecord::new(node_id, graft_core::LabelId::NONE, 1);
        let data = node_rec.to_bytes();
        sender.on_wal_record(WalRecord {
            lsn: 2,
            tx_id: 1,
            record_type: WalRecordType::PageWrite,
            body: WalBody::PageWrite {
                page_id: 1,
                slot: 0,
                page_type: 1,
                data,
            },
        });
        sender.on_wal_record(WalRecord {
            lsn: 3,
            tx_id: 1,
            record_type: WalRecordType::Commit,
            body: WalBody::Empty,
        });
        sender.poll();
        let batches = sender.drain_outbound();
        assert_eq!(batches.len(), 1);

        // Set up replica event loop
        let shard = Shard::new(0);
        let (_req_tx, req_rx) = spsc::channel::<Request>(64);
        let (resp_tx, _resp_rx) = spsc::channel::<Response>(64);

        let inbox = SharedQueue::new();
        let ack_outbox = SharedQueue::new();

        let mut el = ShardEventLoop::with_coordinator(shard, req_rx, resp_tx);
        el.set_repl_receiver(graft_repl::ReplicationReceiver::new(0));
        el.set_repl_replica_queues(inbox.clone(), ack_outbox.clone());

        // Push batch to inbox
        inbox.push(batches[0].clone());

        // Tick to process
        el.tick();

        // Verify ACK was produced
        let acks = ack_outbox.drain();
        assert!(!acks.is_empty(), "expected ACK in ack_outbox");
        assert_eq!(acks[0].shard_id, 0);
    }

    /// Regression test for finding #6, exercised through the full replica
    /// event-loop path (not just the receiver in isolation): a batch that
    /// fails CRC verification must not be silently dropped while later,
    /// valid batches keep getting applied — that would leave the shard's
    /// graph permanently missing data with no observable sign of it. The
    /// event loop must stop applying/ACKing once the receiver is poisoned.
    #[test]
    fn poll_replication_replica_stops_applying_after_corrupted_batch() {
        use crate::cluster::{Request, Response};
        use crate::spsc;
        use graft_core::{LabelId, NodeId};
        use graft_repl::ReplicationSender;
        use graft_storage::NodeRecord;
        use graft_txn::wal::{WalBody, WalRecord, WalRecordType};

        fn node_batch(sender: &mut ReplicationSender, tx_id: u64, node_local: u64, base_lsn: u64) {
            sender.on_wal_record(WalRecord {
                lsn: base_lsn,
                tx_id,
                record_type: WalRecordType::Begin,
                body: WalBody::Empty,
            });
            let node_id = NodeId::new(0, node_local);
            let node_rec = NodeRecord::new(node_id, LabelId::NONE, tx_id);
            sender.on_wal_record(WalRecord {
                lsn: base_lsn + 1,
                tx_id,
                record_type: WalRecordType::PageWrite,
                body: WalBody::PageWrite {
                    page_id: node_local, // distinct page per tx to avoid overlap
                    slot: 0,
                    page_type: 1,
                    data: node_rec.to_bytes(),
                },
            });
            sender.on_wal_record(WalRecord {
                lsn: base_lsn + 2,
                tx_id,
                record_type: WalRecordType::Commit,
                body: WalBody::Empty,
            });
            sender.poll();
        }

        let mut sender = ReplicationSender::new(0);
        node_batch(&mut sender, 1, 1, 1); // tx 1: will arrive corrupted
        node_batch(&mut sender, 2, 2, 4); // tx 2: valid, higher LSN

        let mut batches = sender.drain_outbound();
        assert_eq!(batches.len(), 2);
        batches[0].records[5] ^= 0xFF; // corrupt tx 1's batch only

        let shard = Shard::new(0);
        let (_req_tx, req_rx) = spsc::channel::<Request>(64);
        let (resp_tx, _resp_rx) = spsc::channel::<Response>(64);

        let inbox = SharedQueue::new();
        let ack_outbox = SharedQueue::new();

        let mut el = ShardEventLoop::with_coordinator(shard, req_rx, resp_tx);
        el.set_repl_receiver(graft_repl::ReplicationReceiver::new(0));
        el.set_repl_replica_queues(inbox.clone(), ack_outbox.clone());

        inbox.push(batches[0].clone()); // corrupted
        inbox.push(batches[1].clone()); // valid, would bridge the gap if applied

        el.tick();

        assert!(
            el.repl_receiver().unwrap().is_poisoned(),
            "receiver must be poisoned after the corrupted batch"
        );
        // Neither transaction's node made it into the shard: tx 1 was
        // corrupted, and tx 2 must have been rejected too (not applied
        // "around" the gap).
        assert!(
            el.shard.get_node(NodeId::new(0, 1)).is_none(),
            "corrupted tx must not be applied"
        );
        assert!(
            el.shard.get_node(NodeId::new(0, 2)).is_none(),
            "a later valid batch must not be applied once poisoned — that \
             would silently bridge the gap left by the corrupted batch"
        );
        assert!(
            ack_outbox.drain().is_empty(),
            "no ACK must be sent for a poisoned replica's batches"
        );
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
