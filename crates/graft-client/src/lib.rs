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
            },
        )?;

        // Read server HELLO
        let (msg_type, _payload) = recv_message(&mut reader.try_clone()?)?;
        if msg_type != MessageType::Hello {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "expected HELLO from server",
            ));
        }

        Ok(Self { reader, writer })
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
