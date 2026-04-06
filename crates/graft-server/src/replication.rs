use std::io::BufWriter;
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

use graft_repl::protocol::{
    recv_repl_message, send_repl_hello, send_repl_status, send_wal_ack, send_wal_batch,
    ReplHelloMsg, ReplMessageType, ReplStatusMsg, WalAckMsg, WalBatchMsg,
};
use graft_repl::{ReplControl, SharedQueue};
use graft_runtime::ReplHandles;

/// Start the primary replication listener. Spawns a background thread that
/// accepts replica connections on `bind_addr` and ships WAL batches.
pub fn run_primary_listener(bind_addr: String, handles: ReplHandles) {
    std::thread::Builder::new()
        .name("graft-repl-listener".into())
        .spawn(move || primary_listener(bind_addr, handles))
        .expect("failed to spawn replication listener");
}

fn primary_listener(bind_addr: String, handles: ReplHandles) {
    let listener = match TcpListener::bind(&bind_addr) {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("replication listener failed to bind to {bind_addr}: {e}");
            return;
        }
    };
    tracing::info!("replication listener on {bind_addr}");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let peer = stream
                    .peer_addr()
                    .map(|a| a.to_string())
                    .unwrap_or_else(|_| "unknown".into());
                tracing::info!("replica connected from {peer}");

                // Handshake
                let mut reader = match stream.try_clone() {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::error!("failed to clone stream for {peer}: {e}");
                        continue;
                    }
                };
                let mut writer = BufWriter::new(stream);

                // Receive ReplHello from replica
                let hello = match recv_repl_message(&mut reader) {
                    Ok((ReplMessageType::ReplHello, payload)) => {
                        match rmp_serde::from_slice::<ReplHelloMsg>(&payload) {
                            Ok(h) => h,
                            Err(e) => {
                                tracing::error!("bad hello from {peer}: {e}");
                                continue;
                            }
                        }
                    }
                    Ok((other, _)) => {
                        tracing::error!("expected ReplHello from {peer}, got {other:?}");
                        continue;
                    }
                    Err(e) => {
                        tracing::error!("hello recv error from {peer}: {e}");
                        continue;
                    }
                };

                let replica_id = peer.to_string();

                // Send ReplHello back
                let reply = ReplHelloMsg {
                    shard_id: 0,
                    role: 1, // primary
                    last_lsn: 0,
                    cluster_id: hello.cluster_id,
                };
                if let Err(e) = send_repl_hello(&mut writer, &reply) {
                    tracing::error!("hello send error to {peer}: {e}");
                    continue;
                }

                // Register replica on all shards
                for sq in &handles.shards {
                    if let Some(ref control) = sq.control {
                        control.push(ReplControl::Register {
                            id: replica_id.clone(),
                            shard_id: sq.shard_id,
                            last_lsn: hello.last_lsn,
                        });
                    }
                }

                // Collect outbox/ack_inbox clones for this connection's threads
                let outboxes: Vec<(u8, SharedQueue<WalBatchMsg>)> = handles
                    .shards
                    .iter()
                    .filter_map(|sq| sq.outbox.as_ref().map(|o| (sq.shard_id, o.clone())))
                    .collect();

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

                // Writer thread: drain outboxes → send batches over TCP
                std::thread::Builder::new()
                    .name(format!("graft-repl-writer-{peer}"))
                    .spawn(move || {
                        primary_writer(writer, outboxes);
                        // On disconnect, unregister replica
                        for ctrl in &controls {
                            ctrl.push(ReplControl::Unregister { id: rid_w.clone() });
                        }
                        tracing::info!("replica writer disconnected: {rid_w}");
                    })
                    .expect("failed to spawn repl writer thread");

                // Reader thread: recv ACKs from replica → push to ack_inbox
                let rid_r = replica_id;
                std::thread::Builder::new()
                    .name(format!("graft-repl-reader-{peer}"))
                    .spawn(move || {
                        primary_reader(reader, ack_inboxes, &rid_r);
                        tracing::info!("replica reader disconnected: {rid_r}");
                    })
                    .expect("failed to spawn repl reader thread");
            }
            Err(e) => {
                tracing::error!("replication accept error: {e}");
            }
        }
    }
}

fn primary_writer(mut writer: BufWriter<TcpStream>, outboxes: Vec<(u8, SharedQueue<WalBatchMsg>)>) {
    let heartbeat_interval = Duration::from_secs(1);
    let mut last_send = Instant::now();

    loop {
        let mut sent = false;
        for (_shard_id, outbox) in &outboxes {
            let batches = outbox.drain();
            for batch in &batches {
                if let Err(e) = send_wal_batch(&mut writer, batch) {
                    tracing::debug!("repl writer send error: {e}");
                    return;
                }
                sent = true;
            }
        }

        if sent {
            last_send = Instant::now();
        } else if last_send.elapsed() >= heartbeat_interval {
            // Send heartbeat status
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
                        // Route ACK to the correct shard's ack_inbox
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
pub fn run_replica_connector(primary_addr: String, handles: ReplHandles) {
    std::thread::Builder::new()
        .name("graft-repl-connector".into())
        .spawn(move || replica_connect_loop(primary_addr, handles))
        .expect("failed to spawn replication connector");
}

fn replica_connect_loop(primary_addr: String, handles: ReplHandles) {
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(30);

    loop {
        tracing::info!("connecting to primary at {primary_addr}...");

        match TcpStream::connect(&primary_addr) {
            Ok(stream) => {
                backoff = Duration::from_secs(1);
                if let Err(e) = run_replica_session(&stream, &handles) {
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

fn run_replica_session(stream: &TcpStream, handles: &ReplHandles) -> std::io::Result<()> {
    let mut reader = stream.try_clone()?;
    let mut writer = BufWriter::new(stream.try_clone()?);

    let _ = stream.set_nodelay(true);

    // Send ReplHello
    let hello = ReplHelloMsg {
        shard_id: 0,
        role: 2, // replica
        last_lsn: 0,
        cluster_id: 0,
    };
    send_repl_hello(&mut writer, &hello)?;

    // Receive ReplHello back
    let (msg_type, _payload) = recv_repl_message(&mut reader)?;
    if msg_type != ReplMessageType::ReplHello {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "expected ReplHello from primary",
        ));
    }

    tracing::info!("replication session established with primary");

    // Collect inbox/ack_outbox clones
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

    // Writer thread: drain ack_outboxes → send ACKs to primary
    let writer_ack_outboxes = ack_outboxes;
    let writer_handle = std::thread::Builder::new()
        .name("graft-repl-ack-writer".into())
        .spawn(move || {
            replica_ack_writer(writer, writer_ack_outboxes);
        })
        .expect("failed to spawn replica ack writer");

    // Reader: recv batches from primary → push to inbox by shard_id
    // This runs on the connector thread directly.
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
                // Heartbeat, ignore
            }
            Ok((msg_type, _)) => {
                tracing::debug!("unexpected message from primary: {msg_type:?}");
            }
            Err(e) => {
                // The writer thread will also exit since the stream is dead
                let _ = writer_handle.join();
                return Err(e);
            }
        }
    }
}

fn replica_ack_writer(
    mut writer: BufWriter<TcpStream>,
    ack_outboxes: Vec<(u8, SharedQueue<WalAckMsg>)>,
) {
    loop {
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
