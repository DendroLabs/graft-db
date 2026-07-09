use std::io::BufWriter;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use graft_repl::protocol::{
    recv_repl_message, send_repl_hello, send_repl_status, send_wal_ack, send_wal_batch,
    ReplHelloMsg, ReplMessageType, ReplStatusMsg, WalAckMsg, WalBatchMsg,
};
use graft_repl::{ReplControl, SharedQueue};
use graft_runtime::ReplHandles;

use crate::identity::ReplicaIdentity;

/// A replica gets this long to send its ReplHello before the handshake
/// thread gives up on it. Keeps a client that connects but never speaks
/// from tying up a thread forever (part of the fix for a blocking
/// handshake: see `run_primary_listener` below for the accept-loop half).
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// The primary sends a heartbeat every second when idle (see
/// `primary_writer`). A replica that hears nothing for 5x that long
/// treats the primary as gone and reconnects instead of blocking in
/// `read_exact` forever on a half-open TCP connection.
const REPLICA_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Start the primary replication listener. Spawns a background thread that
/// accepts replica connections on `bind_addr` and ships WAL batches.
pub fn run_primary_listener(bind_addr: String, handles: ReplHandles, cluster_id: u64) {
    std::thread::Builder::new()
        .name("graft-repl-listener".into())
        .spawn(move || {
            let listener = match TcpListener::bind(&bind_addr) {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!("replication listener failed to bind to {bind_addr}: {e}");
                    return;
                }
            };
            tracing::info!("replication listener on {bind_addr}");
            serve_primary_listener(listener, handles, cluster_id);
        })
        .expect("failed to spawn replication listener");
}

/// Accept loop over an already-bound listener. Split out from
/// `run_primary_listener` so tests can bind an OS-assigned port (`:0`) and
/// exercise the real accept/handshake/registration path without a fixed,
/// collision-prone port number.
fn serve_primary_listener(listener: TcpListener, handles: ReplHandles, cluster_id: u64) {
    let handles = Arc::new(handles);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                // Handshake + registration happen on a dedicated thread per
                // connection so a client that connects but never speaks (or
                // is slow) can never block other replicas from registering
                // (finding #11: this used to run inline in the accept loop).
                let handles = Arc::clone(&handles);
                std::thread::Builder::new()
                    .name("graft-repl-handshake".into())
                    .spawn(move || handle_replica_connection(stream, &handles, cluster_id))
                    .expect("failed to spawn replication handshake thread");
            }
            Err(e) => {
                tracing::error!("replication accept error: {e}");
            }
        }
    }
}

fn handle_replica_connection(stream: TcpStream, handles: &ReplHandles, cluster_id: u64) {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".into());
    tracing::info!("replica connected from {peer}");

    let mut reader = match stream.try_clone() {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("failed to clone stream for {peer}: {e}");
            return;
        }
    };
    let mut writer = BufWriter::new(match stream.try_clone() {
        Ok(w) => w,
        Err(e) => {
            tracing::error!("failed to clone stream for {peer}: {e}");
            return;
        }
    });

    let _ = stream.set_read_timeout(Some(HANDSHAKE_TIMEOUT));

    let hello = match recv_repl_message(&mut reader) {
        Ok((ReplMessageType::ReplHello, payload)) => {
            match rmp_serde::from_slice::<ReplHelloMsg>(&payload) {
                Ok(h) => h,
                Err(e) => {
                    tracing::error!("bad hello from {peer}: {e}");
                    return;
                }
            }
        }
        Ok((other, _)) => {
            tracing::error!("expected ReplHello from {peer}, got {other:?}");
            return;
        }
        Err(e) => {
            tracing::error!("hello recv error from {peer}: {e}");
            return;
        }
    };

    // Reject a replica claiming a different cluster than ours — applying
    // its WAL over our pages would be silent cross-cluster corruption
    // (finding #15). cluster_id 0 means "I don't have one yet" and is only
    // valid on a replica's very first connection.
    if hello.cluster_id != 0 && hello.cluster_id != cluster_id {
        tracing::error!(
            "rejecting replica {peer}: cluster_id mismatch (got {}, expected {})",
            hello.cluster_id,
            cluster_id
        );
        return;
    }

    // Reject a shard-count mismatch outright rather than silently
    // registering only the overlapping shard range (finding #4).
    if hello.shard_count as usize != handles.shards.len() {
        tracing::error!(
            "rejecting replica {peer}: shard_count mismatch (got {}, expected {})",
            hello.shard_count,
            handles.shards.len()
        );
        return;
    }

    let replica_id = hello.replica_id.clone();

    let reply = ReplHelloMsg {
        replica_id: String::new(),
        cluster_id,
        role: 1, // primary
        shard_count: handles.shards.len() as u8,
    };
    if let Err(e) = send_repl_hello(&mut writer, &reply) {
        tracing::error!("hello send error to {peer}: {e}");
        return;
    }

    // Handshake done — restore blocking reads for the steady-state ACK
    // loop, which can legitimately be idle for a long time.
    let _ = stream.set_read_timeout(None);

    // Register replica on all shards, resolving each shard's outbox through
    // that shard's ReplicaOutboxRegistry. A reconnecting replica (same
    // replica_id) gets back the exact queue it had before, so anything
    // buffered while it was disconnected is still there (fixes the
    // reconnect-data-loss half of finding #1 / finding #3).
    let mut outboxes: Vec<(u8, SharedQueue<WalBatchMsg>)> =
        Vec::with_capacity(handles.shards.len());
    for sq in &handles.shards {
        let Some(ref registry) = sq.outbox_registry else {
            continue;
        };
        let outbox = registry.get_or_create(&replica_id);
        outboxes.push((sq.shard_id, outbox.clone()));
        if let Some(ref control) = sq.control {
            control.push(ReplControl::Register {
                id: replica_id.clone(),
                shard_id: sq.shard_id,
                outbox,
            });
        }
    }

    let ack_inboxes: Vec<(u8, SharedQueue<(String, u64)>)> = handles
        .shards
        .iter()
        .filter_map(|sq| sq.ack_inbox.as_ref().map(|a| (sq.shard_id, a.clone())))
        .collect();

    let controls: Vec<SharedQueue<ReplControl>> = handles
        .shards
        .iter()
        .filter_map(|sq| sq.control.as_ref().cloned())
        .collect();

    let rid_w = replica_id.clone();
    let peer_w = peer.clone();
    let writer_handle = std::thread::Builder::new()
        .name(format!("graft-repl-writer-{peer}"))
        .spawn(move || {
            primary_writer(writer, outboxes);
            for ctrl in &controls {
                ctrl.push(ReplControl::Unregister { id: rid_w.clone() });
            }
            tracing::info!("replica writer disconnected: {rid_w} ({peer_w})");
        })
        .expect("failed to spawn repl writer thread");

    primary_reader(reader, ack_inboxes, &replica_id);
    tracing::info!("replica reader disconnected: {replica_id} ({peer})");
    let _ = writer_handle.join();
}

fn primary_writer(mut writer: BufWriter<TcpStream>, outboxes: Vec<(u8, SharedQueue<WalBatchMsg>)>) {
    let heartbeat_interval = Duration::from_secs(1);
    let mut last_send = Instant::now();

    loop {
        let mut sent = false;
        for (_shard_id, outbox) in &outboxes {
            // Peek-then-confirm (not a destructive drain): if the send
            // fails partway, the batch stays at the front of the queue and
            // is retried — by this connection if it recovers, or by the
            // next one to register under the same replica_id — instead of
            // being lost (finding #3).
            while let Some(batch) = outbox.peek_front() {
                if let Err(e) = send_wal_batch(&mut writer, &batch) {
                    tracing::debug!("repl writer send error: {e}");
                    return;
                }
                outbox.pop_front();
                sent = true;
            }
        }

        if sent {
            last_send = Instant::now();
        } else if last_send.elapsed() >= heartbeat_interval {
            let status = ReplStatusMsg {
                shard_id: 0,
                role: 1,
                current_lsn: 0,
            };
            if let Err(e) = send_repl_status(&mut writer, &status) {
                tracing::debug!("repl writer heartbeat error: {e}");
                return;
            }
            last_send = Instant::now();
        }

        if !sent {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

fn primary_reader(
    mut reader: TcpStream,
    ack_inboxes: Vec<(u8, SharedQueue<(String, u64)>)>,
    replica_id: &str,
) {
    loop {
        match recv_repl_message(&mut reader) {
            Ok((ReplMessageType::WalAck, payload)) => {
                match rmp_serde::from_slice::<WalAckMsg>(&payload) {
                    Ok(ack) => {
                        for (shard_id, inbox) in &ack_inboxes {
                            if *shard_id == ack.shard_id {
                                inbox.push((replica_id.to_owned(), ack.acked_lsn));
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("bad WalAck from replica: {e}");
                    }
                }
            }
            Ok((msg_type, _)) => {
                tracing::debug!("unexpected message from replica: {msg_type:?}");
            }
            Err(e) => {
                tracing::debug!("replica reader error: {e}");
                return;
            }
        }
    }
}

/// Connect to the primary as a replica. Spawns background threads for
/// reading WAL batches and sending ACKs.
pub fn run_replica_connector(
    primary_addr: String,
    handles: ReplHandles,
    identity: ReplicaIdentity,
) {
    std::thread::Builder::new()
        .name("graft-repl-connector".into())
        .spawn(move || replica_connect_loop(primary_addr, handles, identity))
        .expect("failed to spawn replication connector");
}

fn replica_connect_loop(primary_addr: String, handles: ReplHandles, identity: ReplicaIdentity) {
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(30);

    loop {
        tracing::info!("connecting to primary at {primary_addr}...");

        match TcpStream::connect(&primary_addr) {
            Ok(stream) => {
                backoff = Duration::from_secs(1);
                if let Err(e) = run_replica_session(&stream, &handles, &identity) {
                    tracing::warn!("replication session ended: {e}");
                }
            }
            Err(e) => {
                tracing::warn!("failed to connect to primary {primary_addr}: {e}");
            }
        }

        tracing::info!("reconnecting in {:?}...", backoff);
        std::thread::sleep(backoff);
        backoff = (backoff * 2).min(max_backoff);
    }
}

fn run_replica_session(
    stream: &TcpStream,
    handles: &ReplHandles,
    identity: &ReplicaIdentity,
) -> std::io::Result<()> {
    let mut reader = stream.try_clone()?;
    let mut writer = BufWriter::new(stream.try_clone()?);

    let _ = stream.set_nodelay(true);

    let hello = ReplHelloMsg {
        replica_id: identity.replica_id.clone(),
        cluster_id: identity.cluster_id(),
        role: 2, // replica
        shard_count: handles.shards.len() as u8,
    };
    send_repl_hello(&mut writer, &hello)?;

    let (msg_type, payload) = recv_repl_message(&mut reader)?;
    if msg_type != ReplMessageType::ReplHello {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "expected ReplHello from primary",
        ));
    }
    let reply: ReplHelloMsg = rmp_serde::from_slice(&payload)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    if reply.shard_count as usize != handles.shards.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "primary shard_count {} does not match ours ({})",
                reply.shard_count,
                handles.shards.len()
            ),
        ));
    }

    // Adopt the primary's cluster_id on our very first ever connection.
    if identity.cluster_id() == 0 {
        identity.adopt_cluster_id(reply.cluster_id);
    } else if identity.cluster_id() != reply.cluster_id {
        // The primary already validates this and would have rejected us,
        // so reaching here means something is inconsistent locally.
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "primary cluster_id does not match our previously-adopted cluster_id",
        ));
    }

    tracing::info!("replication session established with primary");

    let inboxes: Vec<(u8, SharedQueue<WalBatchMsg>)> = handles
        .shards
        .iter()
        .filter_map(|sq| sq.inbox.as_ref().map(|i| (sq.shard_id, i.clone())))
        .collect();

    let ack_outboxes: Vec<(u8, SharedQueue<WalAckMsg>)> = handles
        .shards
        .iter()
        .filter_map(|sq| sq.ack_outbox.as_ref().map(|a| (sq.shard_id, a.clone())))
        .collect();

    // Shared between this thread and the ack writer thread so that when
    // one notices the connection is dead, the other stops too instead of
    // sitting in its sleep loop forever with nothing to send (finding #5).
    let should_stop = Arc::new(AtomicBool::new(false));

    let writer_ack_outboxes = ack_outboxes;
    let writer_should_stop = Arc::clone(&should_stop);
    let writer_handle = std::thread::Builder::new()
        .name("graft-repl-ack-writer".into())
        .spawn(move || {
            replica_ack_writer(writer, writer_ack_outboxes, writer_should_stop);
        })
        .expect("failed to spawn replica ack writer");

    // A read timeout turns a half-open connection (primary host gone with
    // no RST) into a bounded wait instead of an indefinite block, so a dead
    // primary is actually detected and reconnected to (finding #12). The
    // primary heartbeats every 1s while idle, so 5s is a generous margin.
    let _ = stream.set_read_timeout(Some(REPLICA_READ_TIMEOUT));

    loop {
        match recv_repl_message(&mut reader) {
            Ok((ReplMessageType::WalBatch, payload)) => {
                match rmp_serde::from_slice::<WalBatchMsg>(&payload) {
                    Ok(batch) => {
                        for (shard_id, inbox) in &inboxes {
                            if *shard_id == batch.shard_id {
                                inbox.push(batch.clone());
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("bad WalBatch from primary: {e}");
                    }
                }
            }
            Ok((ReplMessageType::ReplStatus, _)) => {
                // Heartbeat — the successful read is itself the liveness signal.
            }
            Ok((msg_type, _)) => {
                tracing::debug!("unexpected message from primary: {msg_type:?}");
            }
            Err(e) => {
                should_stop.store(true, Ordering::Relaxed);
                let _ = writer_handle.join();
                return Err(e);
            }
        }
    }
}

fn replica_ack_writer(
    mut writer: BufWriter<TcpStream>,
    ack_outboxes: Vec<(u8, SharedQueue<WalAckMsg>)>,
    should_stop: Arc<AtomicBool>,
) {
    loop {
        if should_stop.load(Ordering::Relaxed) {
            return;
        }
        let mut sent = false;
        for (_shard_id, outbox) in &ack_outboxes {
            let acks = outbox.drain();
            for ack in &acks {
                if let Err(e) = send_wal_ack(&mut writer, ack) {
                    tracing::debug!("repl ack writer error: {e}");
                    return;
                }
                sent = true;
            }
        }
        if !sent {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graft_runtime::ShardCluster;
    use std::time::Duration as StdDuration;

    fn bind_ephemeral() -> TcpListener {
        TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port")
    }

    fn wait_until<F: Fn() -> bool>(cond: F, timeout: StdDuration) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if cond() {
                return true;
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }
        cond()
    }

    /// Real end-to-end test over actual TCP sockets and threads (not just
    /// the in-process SharedQueues): two replicas connect to one primary,
    /// and both must receive every write — the literal scenario from
    /// finding #1 (a single shared outbox used to hand each replica only
    /// an arbitrary slice of the stream).
    #[test]
    fn two_real_replicas_each_receive_the_full_stream() {
        let (mut primary, primary_handles) = ShardCluster::new_primary(1, false);
        let listener = bind_ephemeral();
        let addr = listener.local_addr().unwrap().to_string();
        let cluster_id = 12345;
        std::thread::spawn(move || serve_primary_listener(listener, primary_handles, cluster_id));

        let (replica_a, replica_a_handles) = ShardCluster::new_replica(1, false);
        let (replica_b, replica_b_handles) = ShardCluster::new_replica(1, false);
        let replica_a = Arc::new(std::sync::Mutex::new(replica_a));
        let replica_b = Arc::new(std::sync::Mutex::new(replica_b));

        let identity_a = ReplicaIdentity::load_or_create(None);
        let identity_b = ReplicaIdentity::load_or_create(None);
        run_replica_connector(addr.clone(), replica_a_handles, identity_a);
        run_replica_connector(addr, replica_b_handles, identity_b);

        // Give both replicas time to complete the handshake and register.
        std::thread::sleep(StdDuration::from_millis(150));

        primary.query("CREATE (:Person {name: 'Alice'})").unwrap();
        primary.query("CREATE (:Person {name: 'Bob'})").unwrap();

        let query_a = Arc::clone(&replica_a);
        let query_b = Arc::clone(&replica_b);
        let ok_a = wait_until(
            || {
                query_a
                    .lock()
                    .unwrap()
                    .query("MATCH (p:Person) RETURN p.name")
                    .map(|r| r.rows.len())
                    .unwrap_or(0)
                    == 2
            },
            StdDuration::from_secs(5),
        );
        let ok_b = wait_until(
            || {
                query_b
                    .lock()
                    .unwrap()
                    .query("MATCH (p:Person) RETURN p.name")
                    .map(|r| r.rows.len())
                    .unwrap_or(0)
                    == 2
            },
            StdDuration::from_secs(5),
        );

        assert!(ok_a, "replica A did not receive the full stream");
        assert!(ok_b, "replica B did not receive the full stream");
    }

    /// Regression test for finding #4: a replica whose shard_count doesn't
    /// match the primary's must be rejected outright, not silently
    /// registered against a subset of shards.
    #[test]
    fn shard_count_mismatch_is_rejected() {
        let (_primary, primary_handles) = ShardCluster::new_primary(2, false);
        let listener = bind_ephemeral();
        let addr = listener.local_addr().unwrap().to_string();
        std::thread::spawn(move || serve_primary_listener(listener, primary_handles, 999));

        std::thread::sleep(StdDuration::from_millis(50));
        let stream = TcpStream::connect(&addr).unwrap();
        let mut writer = BufWriter::new(stream.try_clone().unwrap());
        let mut reader = stream;

        send_repl_hello(
            &mut writer,
            &ReplHelloMsg {
                replica_id: "mismatched".into(),
                cluster_id: 0,
                role: 2,
                shard_count: 1, // primary has 2
            },
        )
        .unwrap();

        // The primary must close the connection instead of replying.
        let result = recv_repl_message(&mut reader);
        assert!(
            result.is_err(),
            "primary should close the connection on shard_count mismatch"
        );
    }

    /// Regression test for finding #15: a replica reporting a cluster_id
    /// that doesn't match the primary's must be rejected, not silently
    /// accepted (which would let WAL from an unrelated cluster be applied).
    #[test]
    fn cluster_id_mismatch_is_rejected() {
        let (_primary, primary_handles) = ShardCluster::new_primary(1, false);
        let listener = bind_ephemeral();
        let addr = listener.local_addr().unwrap().to_string();
        std::thread::spawn(move || serve_primary_listener(listener, primary_handles, 111));

        std::thread::sleep(StdDuration::from_millis(50));
        let stream = TcpStream::connect(&addr).unwrap();
        let mut writer = BufWriter::new(stream.try_clone().unwrap());
        let mut reader = stream;

        send_repl_hello(
            &mut writer,
            &ReplHelloMsg {
                replica_id: "foreign-cluster-replica".into(),
                cluster_id: 222, // does not match the primary's 111
                role: 2,
                shard_count: 1,
            },
        )
        .unwrap();

        let result = recv_repl_message(&mut reader);
        assert!(
            result.is_err(),
            "primary should close the connection on cluster_id mismatch"
        );
    }

    /// A fresh replica (cluster_id 0, meaning "I don't have one yet") must
    /// be accepted and told the primary's cluster_id.
    #[test]
    fn fresh_replica_with_zero_cluster_id_is_accepted_and_adopts_cluster_id() {
        let (_primary, primary_handles) = ShardCluster::new_primary(1, false);
        let listener = bind_ephemeral();
        let addr = listener.local_addr().unwrap().to_string();
        std::thread::spawn(move || serve_primary_listener(listener, primary_handles, 555));

        std::thread::sleep(StdDuration::from_millis(50));
        let stream = TcpStream::connect(&addr).unwrap();
        let mut writer = BufWriter::new(stream.try_clone().unwrap());
        let mut reader = stream;

        send_repl_hello(
            &mut writer,
            &ReplHelloMsg {
                replica_id: "fresh-replica".into(),
                cluster_id: 0,
                role: 2,
                shard_count: 1,
            },
        )
        .unwrap();

        let (msg_type, payload) = recv_repl_message(&mut reader).unwrap();
        assert_eq!(msg_type, ReplMessageType::ReplHello);
        let reply: ReplHelloMsg = rmp_serde::from_slice(&payload).unwrap();
        assert_eq!(reply.cluster_id, 555);
    }
}
