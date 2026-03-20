use graft_core::ShardId;

use crate::message::ShardMessage;
use crate::shard::Shard;
use crate::spsc::{Consumer, Producer};

/// Per-shard event loop context.
///
/// Each shard runs a cooperative event loop on its pinned OS thread:
///   1. poll_io     — check for completed I/O operations
///   2. poll_messages — drain inbound messages from other shards
///   3. advance_queries — make progress on active queries
///   4. submit_io   — enqueue new I/O operations
///
/// This is the skeleton that will integrate with IoBackend in a future phase.
pub struct ShardEventLoop {
    pub shard: Shard,
    /// One consumer per other shard (messages arriving here).
    inbound: Vec<(ShardId, Consumer<ShardMessage>)>,
    /// One producer per other shard (messages going out).
    outbound: Vec<(ShardId, Producer<ShardMessage>)>,
    /// Statistics
    messages_received: u64,
    messages_sent: u64,
    ticks: u64,
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
            messages_received: 0,
            messages_sent: 0,
            ticks: 0,
        }
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

    /// Run one tick of the event loop. Returns the number of messages processed.
    pub fn tick(&mut self) -> usize {
        self.ticks += 1;
        let mut work_done = 0;

        // Phase 1: poll_io (placeholder for io_uring/posix integration)
        // self.poll_io();

        // Phase 2: poll_messages
        work_done += self.poll_messages();

        // Phase 3: advance_queries (placeholder for query state machines)
        // self.advance_queries();

        // Phase 4: submit_io (placeholder)
        // self.submit_io();

        work_done
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
        use graft_query::executor::StorageAccess;

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
                self.send_to(
                    reply_to,
                    ShardMessage::GetNodeResult { request_id, node },
                );
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
                self.send_to(
                    reply_to,
                    ShardMessage::EdgesResult { request_id, edges },
                );
            }
            ShardMessage::GetInboundEdges {
                request_id,
                reply_to,
                node_id,
                label,
            } => {
                let edges = self.shard.inbound_edges(node_id, label.as_deref());
                self.send_to(
                    reply_to,
                    ShardMessage::EdgesResult { request_id, edges },
                );
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
        loops[0]
            .shard
            .create_node(Some("Person"), &[("name".into(), Value::String("Alice".into()))]);

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
