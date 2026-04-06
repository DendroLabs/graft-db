use std::thread;
use std::time::Duration;

use graft_runtime::ShardCluster;

/// Test that primary cluster with replication enabled captures WAL records
/// and pushes batches to the outbox SharedQueues.
#[test]
fn primary_produces_repl_batches() {
    let (mut cluster, repl_handles) = ShardCluster::new_primary(1, false);

    // Register a fake replica so batches are produced
    if let Some(ref control) = repl_handles.shards[0].control {
        control.push(graft_repl::ReplControl::Register {
            id: "test-replica".into(),
            shard_id: 0,
            last_lsn: 0,
        });
    }

    // Write data
    cluster.query("CREATE (:Person {name: 'Alice'})").unwrap();

    // Give the event loop time to process repl_log and push to outbox
    thread::sleep(Duration::from_millis(50));

    // Check outbox has batches
    let outbox = repl_handles.shards[0].outbox.as_ref().unwrap();
    let batches = outbox.drain();
    assert!(
        !batches.is_empty(),
        "expected batches in outbox after write"
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
    if let Some(ref control) = primary_handles.shards[0].control {
        control.push(graft_repl::ReplControl::Register {
            id: "test-replica".into(),
            shard_id: 0,
            last_lsn: 0,
        });
    }

    // Write data on primary
    primary.query("CREATE (:Person {name: 'Alice'})").unwrap();
    primary.query("CREATE (:Person {name: 'Bob'})").unwrap();

    // Drain all batches with retry (event loop may still be processing)
    let outbox = primary_handles.shards[0].outbox.as_ref().unwrap();
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
