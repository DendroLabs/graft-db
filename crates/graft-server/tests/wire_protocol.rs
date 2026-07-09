use std::io::BufWriter;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;

use graft_core::protocol::*;
use graft_runtime::ShardCluster;

/// Start an in-process server on an OS-assigned port, return the address.
fn start_test_server() -> String {
    start_test_server_with_shards(1)
}

fn start_test_server_with_shards(shard_count: usize) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let db = Arc::new(Mutex::new(ShardCluster::new(shard_count)));

    thread::spawn(move || {
        for stream in listener.incoming() {
            let stream = match stream {
                Ok(s) => s,
                Err(_) => break,
            };
            let db = Arc::clone(&db);
            thread::spawn(move || {
                let mut reader = stream.try_clone().unwrap();
                let mut writer = BufWriter::new(stream);

                // HELLO handshake
                let (msg_type, _) = recv_message(&mut reader).unwrap();
                assert_eq!(msg_type, MessageType::Hello);
                send_hello(
                    &mut writer,
                    &HelloMsg {
                        client: "test-server".into(),
                        role: Some("standalone".into()),
                        read_only: Some(false),
                        shards: None,
                    },
                )
                .unwrap();

                let mut active_tx: Option<u64> = None;

                // Message loop
                while let Ok((msg_type, payload)) = recv_message(&mut reader) {
                    match msg_type {
                        MessageType::BeginTx => {
                            let tx_id = {
                                let mut db = db.lock().unwrap();
                                db.begin_explicit_tx()
                            };
                            active_tx = Some(tx_id);
                            send_begin_tx_response(&mut writer, &BeginTxResponseMsg { tx_id })
                                .unwrap();
                        }
                        MessageType::CommitTx => {
                            let tx_id = active_tx.take().expect("COMMIT without BEGIN in test");
                            let commit_result = {
                                let mut db = db.lock().unwrap();
                                db.commit_explicit_tx(tx_id)
                            };
                            match commit_result {
                                Ok(()) => send_summary(
                                    &mut writer,
                                    &SummaryMsg {
                                        rows_affected: 0,
                                        elapsed_ms: 0,
                                    },
                                )
                                .unwrap(),
                                Err(e) => send_error(
                                    &mut writer,
                                    &ErrorMsg {
                                        message: format!("commit failed: {e}"),
                                    },
                                )
                                .unwrap(),
                            }
                        }
                        MessageType::RollbackTx => {
                            let tx_id = active_tx.take().expect("ROLLBACK without BEGIN in test");
                            {
                                let mut db = db.lock().unwrap();
                                db.abort_explicit_tx(tx_id);
                            }
                            send_summary(
                                &mut writer,
                                &SummaryMsg {
                                    rows_affected: 0,
                                    elapsed_ms: 0,
                                },
                            )
                            .unwrap();
                        }
                        MessageType::Query => {
                            let query_msg: QueryMsg = rmp_serde::from_slice(&payload).unwrap();
                            let result = {
                                let mut db = db.lock().unwrap();
                                match active_tx {
                                    Some(tx_id) => db.query_in_tx(tx_id, &query_msg.text),
                                    None => db.query(&query_msg.text),
                                }
                            };
                            match result {
                                Ok(qr) => {
                                    send_result(
                                        &mut writer,
                                        &ResultMsg {
                                            columns: qr.columns,
                                        },
                                    )
                                    .unwrap();
                                    let row_count = qr.rows.len() as u64;
                                    for row in &qr.rows {
                                        send_row(
                                            &mut writer,
                                            &RowMsg {
                                                values: row
                                                    .iter()
                                                    .map(|v| format!("{v}"))
                                                    .collect(),
                                            },
                                        )
                                        .unwrap();
                                    }
                                    send_summary(
                                        &mut writer,
                                        &SummaryMsg {
                                            rows_affected: row_count,
                                            elapsed_ms: 0,
                                        },
                                    )
                                    .unwrap();
                                }
                                Err(e) => {
                                    send_error(&mut writer, &ErrorMsg { message: e }).unwrap();
                                }
                            }
                        }
                        _ => break,
                    }
                }

                // Abort orphaned tx on disconnect
                if let Some(tx_id) = active_tx {
                    let mut db = db.lock().unwrap();
                    db.abort_explicit_tx(tx_id);
                }
            });
        }
    });

    addr
}

#[test]
fn client_server_round_trip() {
    let addr = start_test_server();
    let mut client = graft_client::Client::connect(&addr).unwrap();

    // CREATE nodes
    client
        .query("CREATE (:Person {name: 'Alice', age: 30})")
        .unwrap();
    client
        .query("CREATE (:Person {name: 'Bob', age: 25})")
        .unwrap();

    // MATCH + RETURN
    let result = client
        .query("MATCH (p:Person) RETURN p.name ORDER BY p.name")
        .unwrap();
    assert_eq!(result.columns, vec!["p.name"]);
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0], vec!["Alice"]);
    assert_eq!(result.rows[1], vec!["Bob"]);
}

#[test]
fn create_edge_and_traverse() {
    let addr = start_test_server();
    let mut client = graft_client::Client::connect(&addr).unwrap();

    client.query("CREATE (:Person {name: 'Alice'})").unwrap();
    client.query("CREATE (:Person {name: 'Bob'})").unwrap();
    client
        .query(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
             CREATE (a)-[:KNOWS {since: 2020}]->(b)",
        )
        .unwrap();

    let result = client
        .query("MATCH (a:Person)-[e:KNOWS]->(b:Person) RETURN a.name, b.name, e.since")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], "Alice");
    assert_eq!(result.rows[0][1], "Bob");
    assert_eq!(result.rows[0][2], "2020");
}

#[test]
fn error_for_bad_query() {
    let addr = start_test_server();
    let mut client = graft_client::Client::connect(&addr).unwrap();

    let result = client.query("INVALID QUERY");
    assert!(result.is_err());
}

#[test]
fn aggregation_over_wire() {
    let addr = start_test_server();
    let mut client = graft_client::Client::connect(&addr).unwrap();

    client.query("CREATE (:X {v: 10})").unwrap();
    client.query("CREATE (:X {v: 20})").unwrap();
    client.query("CREATE (:X {v: 30})").unwrap();

    let result = client.query("MATCH (n:X) RETURN COUNT(n)").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], "3");
}

#[test]
fn multiple_queries_same_connection() {
    let addr = start_test_server();
    let mut client = graft_client::Client::connect(&addr).unwrap();

    for i in 0..10 {
        client.query(&format!("CREATE (:N {{v: {i}}})",)).unwrap();
    }

    let result = client.query("MATCH (n:N) RETURN COUNT(n)").unwrap();
    assert_eq!(result.rows[0][0], "10");
}

#[test]
fn multi_shard_over_wire() {
    let addr = start_test_server_with_shards(4);
    let mut client = graft_client::Client::connect(&addr).unwrap();

    for i in 0..8 {
        client.query(&format!("CREATE (:N {{v: {i}}})")).unwrap();
    }

    let result = client.query("MATCH (n:N) RETURN COUNT(*)").unwrap();
    assert_eq!(result.rows[0][0], "8");

    let result = client.query("MATCH (n:N) RETURN n.v ORDER BY n.v").unwrap();
    assert_eq!(result.rows.len(), 8);
    assert_eq!(result.rows[0][0], "0");
    assert_eq!(result.rows[7][0], "7");
}

#[test]
fn multi_shard_traversal_over_wire() {
    let addr = start_test_server_with_shards(2);
    let mut client = graft_client::Client::connect(&addr).unwrap();

    client.query("CREATE (:Person {name: 'Alice'})").unwrap();
    client.query("CREATE (:Person {name: 'Bob'})").unwrap();
    client
        .query(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
             CREATE (a)-[:KNOWS]->(b)",
        )
        .unwrap();

    let result = client
        .query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], "Alice");
    assert_eq!(result.rows[0][1], "Bob");
}

#[test]
fn explicit_tx_commit_over_wire() {
    let addr = start_test_server();
    let mut client = graft_client::Client::connect(&addr).unwrap();

    let tx_id = client.begin_tx().unwrap();
    assert!(tx_id > 0);

    client.query("CREATE (:Person {name: 'Alice'})").unwrap();
    let result = client.query("MATCH (p:Person) RETURN p.name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], "Alice");

    client.commit_tx().unwrap();

    let result = client.query("MATCH (p:Person) RETURN p.name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], "Alice");
}

#[test]
fn explicit_tx_rollback_over_wire() {
    let addr = start_test_server();
    let mut client = graft_client::Client::connect(&addr).unwrap();

    client.begin_tx().unwrap();
    client.query("CREATE (:Person {name: 'Alice'})").unwrap();
    client.rollback_tx().unwrap();

    let result = client.query("MATCH (p:Person) RETURN p.name").unwrap();
    assert_eq!(result.rows.len(), 0);
}

#[test]
fn connection_drop_aborts_orphaned_tx() {
    let addr = start_test_server();

    // Start a tx and create a node, then drop the connection without committing
    {
        let mut client = graft_client::Client::connect(&addr).unwrap();
        client.begin_tx().unwrap();
        client.query("CREATE (:Ghost {name: 'phantom'})").unwrap();
        // client drops here — server should abort the tx
    }

    // Give the server a moment to process the disconnect
    std::thread::sleep(std::time::Duration::from_millis(50));

    // New connection should not see the uncommitted data
    let mut client2 = graft_client::Client::connect(&addr).unwrap();
    let result = client2.query("MATCH (g:Ghost) RETURN g.name").unwrap();
    assert_eq!(result.rows.len(), 0);
}

#[test]
fn explicit_tx_multi_statement_over_wire() {
    let addr = start_test_server();
    let mut client = graft_client::Client::connect(&addr).unwrap();

    client.begin_tx().unwrap();

    // Multiple creates within the tx
    client.query("CREATE (:N {name: 'a', v: 1})").unwrap();
    client.query("CREATE (:N {name: 'b', v: 2})").unwrap();
    client.query("CREATE (:N {name: 'c', v: 3})").unwrap();

    // Read within tx
    let result = client.query("MATCH (n:N) RETURN COUNT(*)").unwrap();
    assert_eq!(result.rows[0][0], "3");

    // Modify within tx
    client
        .query("MATCH (n:N {name: 'b'}) SET n.v = 99")
        .unwrap();
    client.query("MATCH (n:N {name: 'c'}) DELETE n").unwrap();

    let result = client
        .query("MATCH (n:N) RETURN n.name, n.v ORDER BY n.name")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], "a");
    assert_eq!(result.rows[1][0], "b");
    assert_eq!(result.rows[1][1], "99");

    client.commit_tx().unwrap();

    // Verify post-commit
    let result = client.query("MATCH (n:N) RETURN COUNT(*)").unwrap();
    assert_eq!(result.rows[0][0], "2");
}

#[test]
fn auto_commit_queries_between_explicit_txs() {
    let addr = start_test_server();
    let mut client = graft_client::Client::connect(&addr).unwrap();

    // Auto-commit
    client.query("CREATE (:N {v: 1})").unwrap();

    // Explicit tx
    client.begin_tx().unwrap();
    client.query("CREATE (:N {v: 2})").unwrap();
    client.commit_tx().unwrap();

    // Auto-commit again
    client.query("CREATE (:N {v: 3})").unwrap();

    // Another explicit tx
    client.begin_tx().unwrap();
    client.query("CREATE (:N {v: 4})").unwrap();
    client.rollback_tx().unwrap();

    // Should see 3 nodes (v=1, v=2, v=3; v=4 was rolled back)
    let result = client.query("MATCH (n:N) RETURN n.v ORDER BY n.v").unwrap();
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][0], "1");
    assert_eq!(result.rows[1][0], "2");
    assert_eq!(result.rows[2][0], "3");
}

#[test]
fn multi_shard_explicit_tx_over_wire() {
    let addr = start_test_server_with_shards(4);
    let mut client = graft_client::Client::connect(&addr).unwrap();

    client.begin_tx().unwrap();
    for i in 0..8 {
        client.query(&format!("CREATE (:N {{v: {i}}})")).unwrap();
    }

    // All 8 should be visible within the tx across 4 shards
    let result = client.query("MATCH (n:N) RETURN COUNT(*)").unwrap();
    assert_eq!(result.rows[0][0], "8");

    client.commit_tx().unwrap();

    let result = client.query("MATCH (n:N) RETURN n.v ORDER BY n.v").unwrap();
    assert_eq!(result.rows.len(), 8);
}

// -- Milestone 3 regression tests: concurrent explicit transactions over the
// wire. The coordinator used to have ONE global tx context: a second
// connection's BEGIN panicked an assert (poisoning the server's cluster
// mutex, killing every connection), and an auto-commit query during another
// connection's open explicit tx silently joined it.

#[test]
fn two_connections_begin_concurrently_without_panic() {
    let addr = start_test_server();
    let mut conn_a = graft_client::Client::connect(&addr).unwrap();
    let mut conn_b = graft_client::Client::connect(&addr).unwrap();

    // Concurrent BEGINs — the second used to panic the coordinator.
    let tx_a = conn_a.begin_tx().unwrap();
    let tx_b = conn_b.begin_tx().unwrap();
    assert_ne!(tx_a, tx_b);

    // Disjoint writes in each tx; both commits succeed.
    conn_a.query("CREATE (:P {name: 'from_a'})").unwrap();
    conn_b.query("CREATE (:P {name: 'from_b'})").unwrap();
    conn_a.commit_tx().unwrap();
    conn_b.commit_tx().unwrap();

    let result = conn_a
        .query("MATCH (p:P) RETURN p.name ORDER BY p.name")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], "from_a");
    assert_eq!(result.rows[1][0], "from_b");
}

#[test]
fn conflicting_commits_loser_gets_error_winner_persists() {
    let addr = start_test_server();
    let mut conn_a = graft_client::Client::connect(&addr).unwrap();
    let mut conn_b = graft_client::Client::connect(&addr).unwrap();

    conn_a.query("CREATE (:N {name: 'x', v: 0})").unwrap();

    conn_a.begin_tx().unwrap();
    conn_b.begin_tx().unwrap();

    // Both transactions write the same property. Loser (B) writes FIRST,
    // winner (A) writes LAST: property records are overwritten in place
    // with no MVCC versioning (pre-existing gap, see Milestone 2's result
    // in tasks/todo.md), so this ordering keeps the final-value assertion
    // meaningful.
    conn_b.query("MATCH (n:N) SET n.v = 2").unwrap();
    conn_a.query("MATCH (n:N) SET n.v = 1").unwrap();

    // First committer wins; the loser's COMMIT must come back as a wire
    // ERROR (the client maps it to Err), not success.
    conn_a.commit_tx().unwrap();
    let err = conn_b.commit_tx().unwrap_err();
    assert!(
        err.to_string().contains("conflict"),
        "loser's commit error should mention the conflict, got: {err}"
    );

    // Winner's value persists.
    let result = conn_a.query("MATCH (n:N) RETURN n.v").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], "1");
}

#[test]
fn auto_commit_on_other_connection_does_not_join_explicit_tx() {
    let addr = start_test_server();
    let mut conn_a = graft_client::Client::connect(&addr).unwrap();
    let mut conn_b = graft_client::Client::connect(&addr).unwrap();

    conn_a.begin_tx().unwrap();
    conn_a.query("CREATE (:T {name: 'ghost'})").unwrap();

    // conn_b's auto-commit query used to silently join conn_a's open tx
    // (and would then vanish with conn_a's rollback). It must commit
    // independently.
    conn_b.query("CREATE (:T {name: 'real'})").unwrap();

    conn_a.rollback_tx().unwrap();

    // conn_b's write survives conn_a's rollback; conn_a's does not.
    let result = conn_b.query("MATCH (t:T) RETURN t.name").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], "real");
}
