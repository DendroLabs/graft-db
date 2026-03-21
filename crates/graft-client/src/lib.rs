use std::io::BufWriter;
use std::net::TcpStream;

use graft_core::protocol::*;

/// Result of executing a query against the server.
#[derive(Clone, Debug)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub rows_affected: u64,
    pub elapsed_ms: u64,
}

/// A synchronous client connection to a graft server.
pub struct Client {
    reader: TcpStream,
    writer: BufWriter<TcpStream>,
    server_role: Option<String>,
    server_read_only: bool,
    server_shards: Option<usize>,
}

impl Client {
    /// Connect to a graft server and perform the HELLO handshake.
    pub fn connect(addr: &str) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        let reader = stream.try_clone()?;
        let mut writer = BufWriter::new(stream);

        // Send HELLO
        send_hello(
            &mut writer,
            &HelloMsg {
                client: format!("graft-client {}", env!("CARGO_PKG_VERSION")),
                role: None,
                read_only: None,
                shards: None,
            },
        )?;

        // Read server HELLO
        let (msg_type, payload) = recv_message(&mut reader.try_clone()?)?;
        if msg_type != MessageType::Hello {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "expected HELLO from server",
            ));
        }
        let hello: HelloMsg = rmp_serde::from_slice(&payload)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        Ok(Self {
            reader,
            writer,
            server_role: hello.role,
            server_read_only: hello.read_only.unwrap_or(false),
            server_shards: hello.shards,
        })
    }

    /// The server's role (standalone, primary, replica) if reported.
    pub fn server_role(&self) -> Option<&str> {
        self.server_role.as_deref()
    }

    /// Whether the server is read-only (true for replicas).
    pub fn is_read_only(&self) -> bool {
        self.server_read_only
    }

    /// Number of shards the server is running, if reported.
    pub fn server_shards(&self) -> Option<usize> {
        self.server_shards
    }

    /// Begin an explicit transaction. Returns the server-assigned tx_id.
    pub fn begin_tx(&mut self) -> std::io::Result<u64> {
        send_begin_tx(&mut self.writer)?;

        let (msg_type, payload) = recv_message(&mut self.reader)?;
        match msg_type {
            MessageType::BeginTx => {
                let resp: BeginTxResponseMsg = rmp_serde::from_slice(&payload)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                Ok(resp.tx_id)
            }
            MessageType::Error => {
                let err: ErrorMsg = rmp_serde::from_slice(&payload)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                Err(std::io::Error::other(err.message))
            }
            other => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("expected BeginTx response, got {:?}", other),
            )),
        }
    }

    /// Commit the current explicit transaction.
    pub fn commit_tx(&mut self) -> std::io::Result<()> {
        send_commit_tx(&mut self.writer)?;

        let (msg_type, payload) = recv_message(&mut self.reader)?;
        match msg_type {
            MessageType::Summary => Ok(()),
            MessageType::Error => {
                let err: ErrorMsg = rmp_serde::from_slice(&payload)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                Err(std::io::Error::other(err.message))
            }
            other => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("expected Summary, got {:?}", other),
            )),
        }
    }

    /// Rollback the current explicit transaction.
    pub fn rollback_tx(&mut self) -> std::io::Result<()> {
        send_rollback_tx(&mut self.writer)?;

        let (msg_type, payload) = recv_message(&mut self.reader)?;
        match msg_type {
            MessageType::Summary => Ok(()),
            MessageType::Error => {
                let err: ErrorMsg = rmp_serde::from_slice(&payload)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                Err(std::io::Error::other(err.message))
            }
            other => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("expected Summary, got {:?}", other),
            )),
        }
    }

    /// Execute a GQL query and return the result.
    pub fn query(&mut self, gql: &str) -> std::io::Result<QueryResult> {
        send_query(&mut self.writer, &QueryMsg { text: gql.into() })?;

        // Read RESULT or ERROR
        let (msg_type, payload) = recv_message(&mut self.reader)?;

        match msg_type {
            MessageType::Error => {
                let err: ErrorMsg = rmp_serde::from_slice(&payload)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                return Err(std::io::Error::other(err.message));
            }
            MessageType::Result => {}
            other => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("expected RESULT or ERROR, got {:?}", other),
                ));
            }
        }

        let result_msg: ResultMsg = rmp_serde::from_slice(&payload)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        // Read ROWs until SUMMARY
        let mut rows = Vec::new();
        loop {
            let (msg_type, payload) = recv_message(&mut self.reader)?;
            match msg_type {
                MessageType::Row => {
                    let row_msg: RowMsg = rmp_serde::from_slice(&payload)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                    rows.push(row_msg.values);
                }
                MessageType::Summary => {
                    let summary: SummaryMsg = rmp_serde::from_slice(&payload)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                    return Ok(QueryResult {
                        columns: result_msg.columns,
                        rows,
                        rows_affected: summary.rows_affected,
                        elapsed_ms: summary.elapsed_ms,
                    });
                }
                MessageType::Error => {
                    let err: ErrorMsg = rmp_serde::from_slice(&payload)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                    return Err(std::io::Error::other(err.message));
                }
                other => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("expected ROW or SUMMARY, got {:?}", other),
                    ));
                }
            }
        }
    }
}
