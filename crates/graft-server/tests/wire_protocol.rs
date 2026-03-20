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
                    },
                )
                .unwrap();

                // Query loop
                loop {
                    let (msg_type, payload) = match recv_message(&mut reader) {
                        Ok(m) => m,
                        Err(_) => break,
                    };
                    if msg_type != MessageType::Query {
                        break;
                    }
                    let query_msg: QueryMsg = rmp_serde::from_slice(&payload).unwrap();
                    let result = {
                        let mut db = db.lock().unwrap();
                        db.query(&query_msg.text)
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
                                        values: row.iter().map(|v| format!("{v}")).collect(),
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

    client
        .query("CREATE (:Person {name: 'Alice'})")
        .unwrap();
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
        client
            .query(&format!("CREATE (:N {{v: {i}}})",))
            .unwrap();
    }

    let result = client.query("MATCH (n:N) RETURN COUNT(n)").unwrap();
    assert_eq!(result.rows[0][0], "10");
}

#[test]
fn multi_shard_over_wire() {
    let addr = start_test_server_with_shards(4);
    let mut client = graft_client::Client::connect(&addr).unwrap();

    for i in 0..8 {
        client
            .query(&format!("CREATE (:N {{v: {i}}})"))
            .unwrap();
    }

    let result = client.query("MATCH (n:N) RETURN COUNT(*)").unwrap();
    assert_eq!(result.rows[0][0], "8");

    let result = client
        .query("MATCH (n:N) RETURN n.v ORDER BY n.v")
        .unwrap();
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
