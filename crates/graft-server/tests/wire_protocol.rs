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
                loop {
                    let (msg_type, payload) = match recv_message(&mut reader) {
                        Ok(m) => m,
                        Err(_) => break,
                    };
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
                            {
                                let mut db = db.lock().unwrap();
                                db.commit_explicit_tx();
                            }
                            active_tx = None;
                            send_summary(
                                &mut writer,
                                &SummaryMsg {
                                    rows_affected: 0,
                                    elapsed_ms: 0,
                                },
                            )
                            .unwrap();
                        }
                        MessageType::RollbackTx => {
                            {
                                let mut db = db.lock().unwrap();
                                db.abort_explicit_tx();
                            }
                            active_tx = None;
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
                                if active_tx.is_some() {
                                    db.query_in_tx(&query_msg.text)
                                } else {
                                    db.query(&query_msg.text)
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
                if active_tx.is_some() {
                    let mut db = db.lock().unwrap();
                    db.abort_explicit_tx();
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
    client.query("MATCH (n:N {name: 'b'}) SET n.v = 99").unwrap();
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
    let result = client
        .query("MATCH (n:N) RETURN n.v ORDER BY n.v")
        .unwrap();
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

    let result = client
        .query("MATCH (n:N) RETURN n.v ORDER BY n.v")
        .unwrap();
    assert_eq!(result.rows.len(), 8);
}
