use std::thread;
use std::time::Duration;

use graft_repl::SharedQueue;
use graft_runtime::ShardCluster;

/// Test that primary cluster with replication enabled captures WAL records
/// and fans batches out to a registered replica's own outbox.
#[test]
fn primary_produces_repl_batches() {
    let (mut cluster, repl_handles) = ShardCluster::new_primary(1, false);

    // Register a fake replica so batches are produced. Production code
    // resolves the outbox through `outbox_registry`; a test can just make
    // one directly since it plays the role of the network layer here.
    let outbox = SharedQueue::new();
    if let Some(ref control) = repl_handles.shards[0].control {
        control.push(graft_repl::ReplControl::Register {
            id: "test-replica".into(),
            shard_id: 0,
            outbox: outbox.clone(),
        });
    }

    // Write data
    cluster.query("CREATE (:Person {name: 'Alice'})").unwrap();

    // Give the event loop time to process repl_log and fan out to the
    // replica's outbox.
    thread::sleep(Duration::from_millis(50));

    let batches = outbox.drain();
    assert!(
        !batches.is_empty(),
        "expected batches in replica outbox after write"
    );
    assert_eq!(batches[0].shard_id, 0);
    assert!(!batches[0].records.is_empty());
}

/// Test end-to-end: primary produces batches, we manually feed them to
/// a replica's inbox, and verify the replica applies the data.
#[test]
fn end_to_end_via_shared_queues() {
    let shard_count = 1;

    // Start primary
    let (mut primary, primary_handles) = ShardCluster::new_primary(shard_count, false);

    // Register fake replica
    let outbox = SharedQueue::new();
    if let Some(ref control) = primary_handles.shards[0].control {
        control.push(graft_repl::ReplControl::Register {
            id: "test-replica".into(),
            shard_id: 0,
            outbox: outbox.clone(),
        });
    }

    // Write data on primary
    primary.query("CREATE (:Person {name: 'Alice'})").unwrap();
    primary.query("CREATE (:Person {name: 'Bob'})").unwrap();

    // Drain all batches with retry (event loop may still be processing)
    let mut all_batches = Vec::new();
    for _ in 0..20 {
        thread::sleep(Duration::from_millis(10));
        all_batches.extend(outbox.drain());
    }
    assert!(!all_batches.is_empty(), "expected batches from primary");

    // Start replica
    let (mut replica, replica_handles) = ShardCluster::new_replica(shard_count, false);

    // Feed batches to replica's inbox
    let inbox = replica_handles.shards[0].inbox.as_ref().unwrap();
    for batch in &all_batches {
        inbox.push(batch.clone());
    }

    // Wait for replica to apply
    thread::sleep(Duration::from_millis(200));

    // Verify replica has the data
    let result = replica
        .query("MATCH (p:Person) RETURN p.name ORDER BY p.name")
        .unwrap();
    assert_eq!(result.rows.len(), 2, "replica should have 2 Person nodes");

    // Verify ACKs were produced
    let ack_outbox = replica_handles.shards[0].ack_outbox.as_ref().unwrap();
    let acks = ack_outbox.drain();
    assert!(!acks.is_empty(), "expected ACKs from replica");
}

/// Regression test for findings #7/#8: a replica's own read-only query
/// tx_ids must never collide with the tx_ids of transactions it applies via
/// replication. This test deliberately reproduces the exact adversarial
/// scenario the original bug describes: a primary's *first* transaction
/// always gets tx_id 1 (the lowest possible value, and exactly what a naive
/// replica-side counter could also hand out first) — replicate it, then
/// confirm the replica's own query tx_id (obtained via the public
/// `begin_explicit_tx` API, since `ShardCluster`'s internal counter field is
/// private to graft-runtime) is nowhere near that range.
#[test]
fn replica_query_tx_ids_never_collide_with_replicated_primary_tx_ids() {
    let shard_count = 1;
    let (mut primary, primary_handles) = ShardCluster::new_primary(shard_count, false);

    let outbox = SharedQueue::new();
    if let Some(ref control) = primary_handles.shards[0].control {
        control.push(graft_repl::ReplControl::Register {
            id: "test-replica".into(),
            shard_id: 0,
            outbox: outbox.clone(),
        });
    }

    // Primary's first-ever committed write always gets the lowest possible
    // real tx_id (1) -- exactly the value the pre-fix sync_tx_counter()
    // mechanism could also have handed to a concurrent replica query.
    primary.query("CREATE (:N {v: 1})").unwrap();

    let mut all_batches = Vec::new();
    for _ in 0..20 {
        thread::sleep(Duration::from_millis(10));
        all_batches.extend(outbox.drain());
        if !all_batches.is_empty() {
            break;
        }
    }
    assert!(!all_batches.is_empty(), "expected a batch from the primary");

    let (mut replica, replica_handles) = ShardCluster::new_replica(shard_count, false);
    let inbox = replica_handles.shards[0].inbox.as_ref().unwrap();
    for batch in &all_batches {
        inbox.push(batch.clone());
    }
    thread::sleep(Duration::from_millis(100));

    // The replicated node must be visible via the normal
    // was_committed()+snapshot path, not any own-write bypass.
    let result = replica.query("MATCH (n:N) RETURN n.v").unwrap();
    assert_eq!(
        result.rows.len(),
        1,
        "replica should see the replicated node"
    );

    // The replica's own query tx_id must be drawn from a disjoint range,
    // nowhere near the low range real (primary-issued/replicated) tx_ids
    // use. `graft_runtime::cluster::REPLICA_TX_ID_BASE` is `1 << 63`
    // (private to graft-runtime); this threshold is a generous, clearly
    // documented proxy for "not anywhere near the low range" usable from
    // this integration test.
    let replica_tx_id = replica.begin_explicit_tx();
    assert!(
        replica_tx_id > (1u64 << 62),
        "replica query tx_id {replica_tx_id} must be far outside the low \
         range real primary tx_ids (starting at 1) ever use"
    );
    replica.abort_explicit_tx();
}

/// Regression test for finding #1: a single shared outbox used to be
/// drained racily by every replica connection, so with N replicas each got
/// an arbitrary ~1/N slice of the stream instead of the full stream. Each
/// registered replica must now see every batch.
#[test]
fn multiple_replicas_each_get_the_full_stream() {
    let (mut cluster, repl_handles) = ShardCluster::new_primary(1, false);

    let outbox_a = SharedQueue::new();
    let outbox_b = SharedQueue::new();
    let outbox_c = SharedQueue::new();
    if let Some(ref control) = repl_handles.shards[0].control {
        for (id, outbox) in [
            ("replica-a", &outbox_a),
            ("replica-b", &outbox_b),
            ("replica-c", &outbox_c),
        ] {
            control.push(graft_repl::ReplControl::Register {
                id: id.into(),
                shard_id: 0,
                outbox: outbox.clone(),
            });
        }
    }

    cluster.query("CREATE (:Person {name: 'Alice'})").unwrap();
    cluster.query("CREATE (:Person {name: 'Bob'})").unwrap();
    cluster.query("CREATE (:Person {name: 'Carol'})").unwrap();

    thread::sleep(Duration::from_millis(100));

    let batches_a = outbox_a.drain();
    let batches_b = outbox_b.drain();
    let batches_c = outbox_c.drain();

    assert!(!batches_a.is_empty());
    assert_eq!(
        batches_a.len(),
        batches_b.len(),
        "every replica must see the same number of batches"
    );
    assert_eq!(batches_a.len(), batches_c.len());

    let total_records = |batches: &[graft_repl::WalBatchMsg]| -> usize {
        batches.iter().map(|b| b.records.len()).sum()
    };
    assert_eq!(total_records(&batches_a), total_records(&batches_b));
    assert_eq!(total_records(&batches_a), total_records(&batches_c));
}

/// Regression test for the reconnect half of finding #1 / finding #3: a
/// replica that goes through Unregister then Register again under the same
/// id (simulating a TCP reconnect) must not lose batches produced while it
/// was "disconnected" — the same underlying outbox is reused.
#[test]
fn reconnect_with_same_id_preserves_queued_batches() {
    let (mut cluster, repl_handles) = ShardCluster::new_primary(1, false);
    let control = repl_handles.shards[0].control.as_ref().unwrap();
    let outbox = SharedQueue::new();

    control.push(graft_repl::ReplControl::Register {
        id: "sticky-replica".into(),
        shard_id: 0,
        outbox: outbox.clone(),
    });

    cluster.query("CREATE (:Person {name: 'Alice'})").unwrap();
    thread::sleep(Duration::from_millis(50));

    // Simulate a disconnect: unregister, but don't drain the outbox (as a
    // real network writer thread wouldn't get to drain a batch it never
    // received before the socket died).
    control.push(graft_repl::ReplControl::Unregister {
        id: "sticky-replica".into(),
    });
    thread::sleep(Duration::from_millis(20));

    // More writes happen while "disconnected".
    cluster.query("CREATE (:Person {name: 'Bob'})").unwrap();
    thread::sleep(Duration::from_millis(50));

    // Reconnect under the same id with a freshly-looked-up outbox handle
    // (as the network layer would do via ReplicaOutboxRegistry — here we
    // just reuse the same SharedQueue clone, which is what the registry
    // guarantees in production).
    control.push(graft_repl::ReplControl::Register {
        id: "sticky-replica".into(),
        shard_id: 0,
        outbox: outbox.clone(),
    });
    thread::sleep(Duration::from_millis(50));

    let batches = outbox.drain();
    assert!(
        !batches.is_empty(),
        "batches produced while disconnected must not be lost"
    );

    // Both Alice and Bob's writes must be represented — not just the ones
    // before or after the simulated disconnect. Each committed CREATE
    // produces its own Begin/PageWrite/Commit WAL record run, so seeing
    // records from two separate batches/commits confirms nothing in
    // between was dropped.
    let mut commit_count = 0usize;
    for batch in &batches {
        let records = graft_repl::receiver::deserialize_wal_records(&batch.records).unwrap();
        commit_count += records
            .iter()
            .filter(|r| r.record_type == graft_txn::wal::WalRecordType::Commit)
            .count();
    }
    assert_eq!(commit_count, 2, "expected commits from both writes");
}
