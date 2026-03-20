use std::io::BufWriter;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use clap::Parser;
use graft_core::protocol::*;
use graft_runtime::ShardCluster;

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

    let db = Arc::new(Mutex::new(ShardCluster::new(args.shards)));

    let listener = TcpListener::bind(&addr).unwrap_or_else(|e| {
        eprintln!("failed to bind to {addr}: {e}");
        std::process::exit(1);
    });

    eprintln!(
        "graft listening on {addr} ({} shard{})",
        args.shards,
        if args.shards == 1 { "" } else { "s" }
    );

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let db = Arc::clone(&db);
                std::thread::spawn(move || {
                    if let Err(e) = handle_connection(stream, db) {
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

fn handle_connection(stream: TcpStream, db: Arc<Mutex<ShardCluster>>) -> std::io::Result<()> {
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
        },
    )?;

    // Query loop
    loop {
        let (msg_type, payload) = recv_message(&mut reader)?;

        match msg_type {
            MessageType::Query => {
                let query_msg: QueryMsg = rmp_serde::from_slice(&payload)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

                let start = Instant::now();
                let result = {
                    let mut db = db.lock().unwrap();
                    db.query(&query_msg.text)
                };
                let elapsed = start.elapsed();

                match result {
                    Ok(qr) => {
                        // Send RESULT (column headers)
                        send_result(
                            &mut writer,
                            &ResultMsg {
                                columns: qr.columns.clone(),
                            },
                        )?;

                        // Send ROWs
                        let row_count = qr.rows.len() as u64;
                        for row in &qr.rows {
                            send_row(
                                &mut writer,
                                &RowMsg {
                                    values: row.iter().map(|v| format!("{v}")).collect(),
                                },
                            )?;
                        }

                        // Send SUMMARY
                        send_summary(
                            &mut writer,
                            &SummaryMsg {
                                rows_affected: row_count,
                                elapsed_ms: elapsed.as_millis() as u64,
                            },
                        )?;
                    }
                    Err(e) => {
                        send_error(&mut writer, &ErrorMsg { message: e })?;
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
            }
        }
    }
}
