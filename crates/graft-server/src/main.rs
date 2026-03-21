mod admin;
mod metrics;

use std::io::{BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use clap::Parser;
use graft_core::protocol::*;
use graft_repl::protocol::ReplicationRole;
use graft_runtime::ShardCluster;

/// Default replication port.
pub const DEFAULT_REPLICATION_PORT: u16 = 7688;

#[derive(Parser)]
#[command(name = "graft-server", about = "graft graph database server")]
struct Args {
    /// Address to bind to
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Port to listen on
    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,

    /// Number of shard threads (default: number of CPU cores)
    #[arg(long, default_value_t = num_shards_default())]
    shards: usize,

    /// Data directory for persistent storage. If omitted, data is in-memory only.
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Pin each shard worker thread to its own CPU core.
    #[arg(long, default_value_t = false)]
    pin_cores: bool,

    /// Server role: standalone (default), primary, or replica.
    #[arg(long, default_value = "standalone")]
    role: String,

    /// Primary server address for replica mode (host:port).
    #[arg(long)]
    primary: Option<String>,

    /// Port for replication traffic (default: 7688).
    #[arg(long, default_value_t = DEFAULT_REPLICATION_PORT)]
    replication_port: u16,

    /// Port for Prometheus metrics endpoint (default: 9100). Set to 0 to disable.
    #[arg(long, default_value_t = 9100)]
    metrics_port: u16,
}

fn num_shards_default() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1)
}

fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let addr = format!("{}:{}", args.host, args.port);

    let role: ReplicationRole = args.role.parse().unwrap_or_else(|e| {
        eprintln!("invalid role: {e}");
        std::process::exit(1);
    });

    // Validate role-specific args
    if role == ReplicationRole::Replica && args.primary.is_none() {
        eprintln!("--primary is required when --role=replica");
        std::process::exit(1);
    }

    let db = if let Some(ref data_dir) = args.data_dir {
        let cluster = ShardCluster::open_with_options(args.shards, data_dir, args.pin_cores)
            .unwrap_or_else(|e| {
                eprintln!("failed to open data directory {}: {e}", data_dir.display());
                std::process::exit(1);
            });
        Arc::new(Mutex::new(cluster))
    } else {
        Arc::new(Mutex::new(ShardCluster::new_with_options(
            args.shards,
            args.pin_cores,
        )))
    };

    let listener = TcpListener::bind(&addr).unwrap_or_else(|e| {
        eprintln!("failed to bind to {addr}: {e}");
        std::process::exit(1);
    });

    let storage_mode = if args.data_dir.is_some() {
        "durable"
    } else {
        "ephemeral"
    };
    let is_read_only = role == ReplicationRole::Replica;
    eprintln!(
        "graft listening on {addr} ({}, {} shard{}, {storage_mode})",
        role,
        args.shards,
        if args.shards == 1 { "" } else { "s" }
    );

    if role == ReplicationRole::Primary {
        eprintln!("replication port: {}", args.replication_port);
    }

    // Start metrics endpoint
    if args.metrics_port > 0 {
        let metrics_addr = format!("{}:{}", args.host, args.metrics_port);
        metrics::start_metrics_server(metrics_addr.clone(), role, args.shards);
        eprintln!("metrics: http://{metrics_addr}/metrics");
    }

    let shard_count = args.shards;
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let db = Arc::clone(&db);
                std::thread::spawn(move || {
                    if let Err(e) =
                        handle_connection(stream, db, role, is_read_only, shard_count)
                    {
                        tracing::debug!("connection closed: {e}");
                    }
                });
            }
            Err(e) => {
                tracing::error!("accept error: {e}");
            }
        }
    }
}

fn handle_connection(
    stream: TcpStream,
    db: Arc<Mutex<ShardCluster>>,
    role: ReplicationRole,
    is_read_only: bool,
    shard_count: usize,
) -> std::io::Result<()> {
    let peer = stream.peer_addr()?;
    tracing::info!("new connection from {peer}");

    let mut reader = stream.try_clone()?;
    let mut writer = BufWriter::new(stream);

    // Read HELLO
    let (msg_type, payload) = recv_message(&mut reader)?;
    if msg_type != MessageType::Hello {
        send_error(
            &mut writer,
            &ErrorMsg {
                message: "expected HELLO".into(),
            },
        )?;
        return Ok(());
    }
    let hello: HelloMsg = rmp_serde::from_slice(&payload)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    tracing::info!("client: {}", hello.client);

    // Send HELLO back
    send_hello(
        &mut writer,
        &HelloMsg {
            client: format!("graft-server {}", env!("CARGO_PKG_VERSION")),
            role: Some(role.to_string()),
            read_only: Some(is_read_only),
            shards: Some(shard_count),
        },
    )?;
    writer.flush()?;

    // Disable Nagle's algorithm for low-latency responses
    if let Ok(ref sock) = writer.get_ref().try_clone() {
        let _ = sock.set_nodelay(true);
    }

    // Per-connection transaction state
    let mut active_tx: Option<u64> = None;

    // Query loop
    let result = (|| -> std::io::Result<()> {
        loop {
            let (msg_type, payload) = recv_message(&mut reader)?;

            match msg_type {
                MessageType::BeginTx => {
                    if active_tx.is_some() {
                        send_error(
                            &mut writer,
                            &ErrorMsg {
                                message: "transaction already active".into(),
                            },
                        )?;
                        writer.flush()?;
                        continue;
                    }
                    let tx_id = {
                        let mut db = db.lock().unwrap();
                        db.begin_explicit_tx()
                    };
                    active_tx = Some(tx_id);
                    send_begin_tx_response(&mut writer, &BeginTxResponseMsg { tx_id })?;
                    writer.flush()?;
                }
                MessageType::CommitTx => {
                    if active_tx.is_none() {
                        send_error(
                            &mut writer,
                            &ErrorMsg {
                                message: "no transaction active".into(),
                            },
                        )?;
                        writer.flush()?;
                        continue;
                    }
                    let start = Instant::now();
                    {
                        let mut db = db.lock().unwrap();
                        db.commit_explicit_tx();
                    }
                    active_tx = None;
                    send_summary(
                        &mut writer,
                        &SummaryMsg {
                            rows_affected: 0,
                            elapsed_ms: start.elapsed().as_millis() as u64,
                        },
                    )?;
                    writer.flush()?;
                }
                MessageType::RollbackTx => {
                    if active_tx.is_none() {
                        send_error(
                            &mut writer,
                            &ErrorMsg {
                                message: "no transaction active".into(),
                            },
                        )?;
                        writer.flush()?;
                        continue;
                    }
                    let start = Instant::now();
                    {
                        let mut db = db.lock().unwrap();
                        db.abort_explicit_tx();
                    }
                    active_tx = None;
                    send_summary(
                        &mut writer,
                        &SummaryMsg {
                            rows_affected: 0,
                            elapsed_ms: start.elapsed().as_millis() as u64,
                        },
                    )?;
                    writer.flush()?;
                }
                MessageType::Query => {
                    let query_msg: QueryMsg = rmp_serde::from_slice(&payload)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

                    let start = Instant::now();

                    // Try admin commands first (SHOW REPLICAS, etc.)
                    if let Some(admin_result) =
                        admin::try_execute(&query_msg.text, role, shard_count)
                    {
                        let elapsed = start.elapsed();
                        send_result(&mut writer, &admin::to_result_msg(&admin_result))?;
                        for row_msg in admin::to_row_msgs(&admin_result) {
                            send_row(&mut writer, &row_msg)?;
                        }
                        send_summary(
                            &mut writer,
                            &admin::to_summary_msg(
                                &admin_result,
                                elapsed.as_millis() as u64,
                            ),
                        )?;
                        writer.flush()?;
                        continue;
                    }

                    // Reject writes on read-only replicas
                    if is_read_only {
                        let upper = query_msg.text.trim().to_uppercase();
                        let is_write = upper.starts_with("CREATE")
                            || upper.starts_with("DELETE")
                            || upper.starts_with("SET")
                            || (upper.contains("SET ") && upper.starts_with("MATCH"));
                        if is_write {
                            send_error(
                                &mut writer,
                                &ErrorMsg {
                                    message: "cannot execute write queries on a read-only replica"
                                        .into(),
                                },
                            )?;
                            writer.flush()?;
                            continue;
                        }
                    }

                    let result = {
                        let mut db = db.lock().unwrap();
                        if active_tx.is_some() {
                            db.query_in_tx(&query_msg.text)
                        } else {
                            db.query(&query_msg.text)
                        }
                    };
                    let elapsed = start.elapsed();

                    match result {
                        Ok(qr) => {
                            send_result(
                                &mut writer,
                                &ResultMsg {
                                    columns: qr.columns.clone(),
                                },
                            )?;

                            let row_count = qr.rows.len() as u64;
                            for row in &qr.rows {
                                send_row(
                                    &mut writer,
                                    &RowMsg {
                                        values: row.iter().map(|v| format!("{v}")).collect(),
                                    },
                                )?;
                            }

                            send_summary(
                                &mut writer,
                                &SummaryMsg {
                                    rows_affected: row_count,
                                    elapsed_ms: elapsed.as_millis() as u64,
                                },
                            )?;
                            writer.flush()?;
                        }
                        Err(e) => {
                            send_error(&mut writer, &ErrorMsg { message: e })?;
                            writer.flush()?;
                        }
                    }
                }
                _ => {
                    send_error(
                        &mut writer,
                        &ErrorMsg {
                            message: format!("unexpected message type: {:?}", msg_type),
                        },
                    )?;
                    writer.flush()?;
                }
            }
        }
    })();

    // On disconnect, abort any orphaned transaction
    if active_tx.is_some() {
        tracing::info!("connection {peer} dropped with active tx, aborting");
        let mut db = db.lock().unwrap();
        db.abort_explicit_tx();
    }

    result
}
