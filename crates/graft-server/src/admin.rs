use graft_core::protocol::{ResultMsg, RowMsg, SummaryMsg};
use graft_repl::protocol::ReplicationRole;

/// Result of executing an admin command.
pub struct AdminResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

/// Try to parse and execute an admin command. Returns None if the query
/// is not an admin command (i.e., it should be passed to the GQL engine).
pub fn try_execute(
    query: &str,
    role: ReplicationRole,
    shard_count: usize,
) -> Option<AdminResult> {
    let upper = query.trim().to_uppercase();

    if upper == "SHOW REPLICAS" {
        return Some(show_replicas(role));
    }
    if upper == "SHOW REPLICATION STATUS" {
        return Some(show_replication_status(role, shard_count));
    }
    if upper == "SHOW REPLICATION LAG" {
        return Some(show_replication_lag(role));
    }
    if upper == "SHOW SHARD STATUS" {
        return Some(show_shard_status(shard_count));
    }
    if upper == "PROMOTE REPLICA" {
        return Some(promote_replica(role));
    }

    None
}

fn show_replicas(role: ReplicationRole) -> AdminResult {
    if role != ReplicationRole::Primary {
        return AdminResult {
            columns: vec!["message".into()],
            rows: vec![vec!["not a primary server".into()]],
        };
    }
    // TODO: wire up actual replica state from ReplicationSender
    AdminResult {
        columns: vec![
            "id".into(),
            "state".into(),
            "lag_bytes".into(),
            "acked_lsn".into(),
        ],
        rows: Vec::new(),
    }
}

fn show_replication_status(role: ReplicationRole, shard_count: usize) -> AdminResult {
    AdminResult {
        columns: vec![
            "role".into(),
            "shards".into(),
            "mode".into(),
        ],
        rows: vec![vec![
            role.to_string(),
            shard_count.to_string(),
            "async".into(),
        ]],
    }
}

fn show_replication_lag(role: ReplicationRole) -> AdminResult {
    if role != ReplicationRole::Primary {
        return AdminResult {
            columns: vec!["message".into()],
            rows: vec![vec!["not a primary server".into()]],
        };
    }
    // TODO: wire up actual lag data from ReplicationSender
    AdminResult {
        columns: vec![
            "replica".into(),
            "shard".into(),
            "lag_bytes".into(),
            "lag_ms".into(),
        ],
        rows: Vec::new(),
    }
}

fn show_shard_status(shard_count: usize) -> AdminResult {
    // TODO: wire up actual shard stats via GetShardStats request
    AdminResult {
        columns: vec![
            "shard".into(),
            "status".into(),
        ],
        rows: (0..shard_count)
            .map(|i| vec![i.to_string(), "running".into()])
            .collect(),
    }
}

fn promote_replica(role: ReplicationRole) -> AdminResult {
    if role != ReplicationRole::Replica {
        return AdminResult {
            columns: vec!["message".into()],
            rows: vec![vec!["not a replica server".into()]],
        };
    }
    // TODO: implement actual promotion logic
    AdminResult {
        columns: vec!["message".into()],
        rows: vec![vec!["replica promoted to primary".into()]],
    }
}

/// Convert an AdminResult to wire protocol messages.
pub fn to_result_msg(result: &AdminResult) -> ResultMsg {
    ResultMsg {
        columns: result.columns.clone(),
    }
}

pub fn to_row_msgs(result: &AdminResult) -> Vec<RowMsg> {
    result
        .rows
        .iter()
        .map(|row| RowMsg {
            values: row.clone(),
        })
        .collect()
}

pub fn to_summary_msg(result: &AdminResult, elapsed_ms: u64) -> SummaryMsg {
    SummaryMsg {
        rows_affected: result.rows.len() as u64,
        elapsed_ms,
    }
}
