/// WAL retention manager for replication.
///
/// Tracks how much WAL data must be retained for replica catchup.
/// In the current single-file WAL model, this manages a retention
/// window by tracking min ACK'd LSN across replicas. When WAL
/// segments are implemented (Phase 8d), this will manage segment
/// deletion.
pub struct WalRetention {
    /// Maximum bytes of WAL to retain per shard for replica catchup.
    max_bytes: u64,
    /// The minimum LSN that must be retained (based on replica ACKs).
    min_retained_lsn: u64,
    /// Current WAL size estimate.
    current_bytes: u64,
}

impl WalRetention {
    /// Create a new retention manager.
    /// `max_bytes` is the retention limit per shard (default 1GB).
    pub fn new(max_bytes: u64) -> Self {
        Self {
            max_bytes,
            min_retained_lsn: 0,
            current_bytes: 0,
        }
    }

    /// Default retention: 1GB per shard.
    pub fn default_1gb() -> Self {
        Self::new(1024 * 1024 * 1024)
    }

    /// Update the minimum retained LSN based on the minimum ACK'd LSN
    /// across all connected replicas.
    pub fn update_min_lsn(&mut self, min_acked_lsn: u64) {
        if min_acked_lsn > self.min_retained_lsn {
            self.min_retained_lsn = min_acked_lsn;
        }
    }

    /// Update current WAL byte count.
    pub fn update_size(&mut self, current_bytes: u64) {
        self.current_bytes = current_bytes;
    }

    /// Whether retention limit is exceeded (WAL data should be cleaned up).
    pub fn is_over_limit(&self) -> bool {
        self.current_bytes > self.max_bytes
    }

    /// The LSN below which WAL data can be safely truncated.
    pub fn truncation_lsn(&self) -> u64 {
        self.min_retained_lsn
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    pub fn current_bytes(&self) -> u64 {
        self.current_bytes
    }

    pub fn min_retained_lsn(&self) -> u64 {
        self.min_retained_lsn
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_tracks_lsn() {
        let mut ret = WalRetention::new(1024);
        assert_eq!(ret.min_retained_lsn(), 0);

        ret.update_min_lsn(100);
        assert_eq!(ret.min_retained_lsn(), 100);

        // Should not go backwards
        ret.update_min_lsn(50);
        assert_eq!(ret.min_retained_lsn(), 100);
    }

    #[test]
    fn retention_over_limit() {
        let mut ret = WalRetention::new(1024);
        assert!(!ret.is_over_limit());

        ret.update_size(2048);
        assert!(ret.is_over_limit());
    }

    #[test]
    fn default_1gb() {
        let ret = WalRetention::default_1gb();
        assert_eq!(ret.max_bytes(), 1024 * 1024 * 1024);
    }
}
