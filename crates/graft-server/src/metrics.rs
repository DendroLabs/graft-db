use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};

use graft_repl::protocol::ReplicationRole;

/// Metrics state snapshot (collected from the cluster periodically).
pub struct MetricsSnapshot {
    pub role: ReplicationRole,
    pub shard_count: usize,
}

/// Start a minimal HTTP metrics server on a dedicated thread.
/// Listens on `addr` (e.g. "127.0.0.1:9100") and responds to
/// `GET /metrics` with Prometheus text format.
pub fn start_metrics_server(addr: String, role: ReplicationRole, shard_count: usize) {
    std::thread::Builder::new()
        .name("graft-metrics".into())
        .spawn(move || {
            let listener = match TcpListener::bind(&addr) {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!("metrics server failed to bind to {addr}: {e}");
                    return;
                }
            };
            tracing::info!("metrics endpoint listening on http://{addr}/metrics");

            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        let snapshot = MetricsSnapshot { role, shard_count };
                        if let Err(e) = handle_http(stream, &snapshot) {
                            tracing::debug!("metrics connection error: {e}");
                        }
                    }
                    Err(e) => {
                        tracing::debug!("metrics accept error: {e}");
                    }
                }
            }
        })
        .expect("failed to spawn metrics thread");
}

fn handle_http(stream: TcpStream, snapshot: &MetricsSnapshot) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    // Read the HTTP request line
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;

    // Drain remaining headers (read until empty line)
    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        if line.trim().is_empty() {
            break;
        }
    }

    if request_line.starts_with("GET /metrics") {
        let body = render_metrics(snapshot);
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        writer.write_all(response.as_bytes())?;
    } else if request_line.starts_with("GET /health") {
        let body = "ok\n";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        writer.write_all(response.as_bytes())?;
    } else {
        let body = "404 Not Found\n";
        let response = format!(
            "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        writer.write_all(response.as_bytes())?;
    }

    writer.flush()
}

fn render_metrics(snapshot: &MetricsSnapshot) -> String {
    let role_value = match snapshot.role {
        ReplicationRole::Standalone => 0,
        ReplicationRole::Primary => 1,
        ReplicationRole::Replica => 2,
    };

    let mut out = String::new();

    out.push_str("# HELP graft_role Server role: 0=standalone, 1=primary, 2=replica\n");
    out.push_str("# TYPE graft_role gauge\n");
    out.push_str(&format!("graft_role {role_value}\n"));
    out.push('\n');

    out.push_str("# HELP graft_shard_count Number of shards\n");
    out.push_str("# TYPE graft_shard_count gauge\n");
    out.push_str(&format!("graft_shard_count {}\n", snapshot.shard_count));
    out.push('\n');

    // TODO: wire up per-shard metrics from ShardCluster
    // graft_shard_wal_lsn{shard="0"} 12345
    // graft_shard_node_count{shard="0"} 1000
    // graft_shard_edge_count{shard="0"} 5000
    // graft_replica_lag_bytes{replica="r1",shard="0"} 1024
    // graft_replica_lag_ms{replica="r1"} 50
    // graft_replica_state{replica="r1"} 1  (streaming)
    // graft_wal_retention_bytes 1073741824

    out
}
