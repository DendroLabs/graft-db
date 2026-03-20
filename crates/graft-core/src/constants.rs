// Storage
pub const PAGE_SIZE: usize = 8192;
pub const PAGE_HEADER_SIZE: usize = 32;
pub const PAGE_USABLE_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE;
pub const RECORD_SIZE: usize = 64;
pub const RECORDS_PER_PAGE: usize = PAGE_USABLE_SIZE / RECORD_SIZE; // 127

// Inline property space on each node/edge record.
// 64-byte record minus fixed fields leaves room for a small inline payload.
pub const INLINE_PROPERTY_BYTES: usize = 16;

// Sharding
pub const SHARD_BITS: u32 = 8;
pub const MAX_SHARDS: usize = 1 << SHARD_BITS; // 256

// WAL
pub const WAL_BUFFER_SIZE: usize = 64 * 1024; // 64 KB
pub const GROUP_COMMIT_WINDOW_MS: u64 = 2;

// Checkpoints
pub const CHECKPOINT_INTERVAL_SECS: u64 = 5 * 60; // 5 minutes
pub const CHECKPOINT_WAL_THRESHOLD: u64 = 100 * 1024 * 1024; // 100 MB

// Wire protocol
pub const WIRE_MAGIC: [u8; 4] = *b"GF01";
pub const WIRE_VERSION: u8 = 1;
pub const DEFAULT_PORT: u16 = 7687;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_geometry() {
        assert_eq!(PAGE_SIZE, 8192);
        assert_eq!(PAGE_USABLE_SIZE, 8160);
        assert_eq!(RECORDS_PER_PAGE, 127);
        // Header + records + waste = PAGE_SIZE
        assert!(PAGE_HEADER_SIZE + RECORDS_PER_PAGE * RECORD_SIZE <= PAGE_SIZE);
        // Verify records fit within usable space
        assert!(RECORDS_PER_PAGE * RECORD_SIZE <= PAGE_USABLE_SIZE);
        // Verify we can't fit one more
        assert!((RECORDS_PER_PAGE + 1) * RECORD_SIZE > PAGE_USABLE_SIZE);
    }
}
