use std::cell::RefCell;
use std::path::PathBuf;

use graft_core::constants::{PAGE_SIZE, RECORD_SIZE};
use graft_core::{EdgeId, LabelId, NodeId, PageId, ShardId, TxId};
use graft_io::{FileHandle, IoBackend, OpenOptions};
use graft_query::executor::{EdgeInfo, NodeInfo, StorageAccess, Value};
use graft_storage::page::PageType;
use graft_storage::record::{
    ENTITY_TYPE_EDGE, ENTITY_TYPE_NODE, VALUE_TYPE_BOOL, VALUE_TYPE_FLOAT, VALUE_TYPE_INT,
    VALUE_TYPE_NULL, VALUE_TYPE_STRING,
};
use graft_storage::{BufferPool, EdgeRecord, NodeRecord, Page, PropertyRecord};
use graft_txn::mvcc::Snapshot;
use graft_txn::wal::{WalBody, WalReader, WalRecordType, WalWriter};
use graft_txn::TransactionManager;
use hashbrown::HashMap;

use crate::label::LabelDictionary;

// Default buffer pool capacity (pages) per pool.
const DEFAULT_POOL_CAPACITY: usize = 1024;

/// Group commit: sync the WAL at most every N milliseconds or N commits,
/// whichever comes first. Between syncs, committed data is in the OS page
/// cache (safe against process crashes) but not yet durable against power
/// failure. At 2ms / 64 commits, the worst-case data loss on power failure
/// is bounded to a few milliseconds of committed transactions.
const GROUP_COMMIT_INTERVAL_MS: u64 = 2;
const GROUP_COMMIT_MAX_PENDING: u32 = 64;

// ---------------------------------------------------------------------------
// ShardConfig
// ---------------------------------------------------------------------------

pub struct ShardConfig {
    pub data_dir: PathBuf,
    pub pool_capacity: usize,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("."),
            pool_capacity: DEFAULT_POOL_CAPACITY,
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn page_file_offset(page_id: PageId) -> u64 {
    debug_assert!(page_id > 0, "page_id 0 is reserved");
    (page_id - 1) * PAGE_SIZE as u64
}

fn value_to_prop_record(
    entity_id: u64,
    entity_type: u8,
    key: &str,
    value: &Value,
) -> PropertyRecord {
    match value {
        Value::Null => PropertyRecord::new_null(entity_id, entity_type, key),
        Value::Int(n) => PropertyRecord::new_int(entity_id, entity_type, key, *n),
        Value::Float(f) => PropertyRecord::new_float(entity_id, entity_type, key, *f),
        Value::String(s) => PropertyRecord::new_string(entity_id, entity_type, key, s),
        Value::Bool(b) => PropertyRecord::new_bool(entity_id, entity_type, key, *b),
        _ => PropertyRecord::new_null(entity_id, entity_type, key),
    }
}

fn prop_record_to_value(rec: &PropertyRecord) -> Value {
    match rec.value_type {
        VALUE_TYPE_NULL => Value::Null,
        VALUE_TYPE_INT => Value::Int(rec.int_value().unwrap()),
        VALUE_TYPE_FLOAT => Value::Float(rec.float_value().unwrap()),
        VALUE_TYPE_STRING => Value::String(rec.string_value().unwrap()),
        VALUE_TYPE_BOOL => Value::Bool(rec.bool_value().unwrap()),
        _ => Value::Null,
    }
}

// ---------------------------------------------------------------------------
// Shard — a single data partition backed by page-based storage
// ---------------------------------------------------------------------------

pub struct Shard {
    shard_id: ShardId,
    pub(crate) labels: LabelDictionary,

    // Page-based storage — wrapped in RefCell because BufferPool::pin/unpin
    // require &mut self, but StorageAccess reads take &self. Shards are
    // single-threaded so RefCell is safe.
    node_pool: RefCell<BufferPool>,
    edge_pool: RefCell<BufferPool>,
    prop_pool: RefCell<BufferPool>,

    // ID → (page_id, slot) indexes for O(1) lookup
    node_index: HashMap<u64, (PageId, u16)>,
    edge_index: HashMap<u64, (PageId, u16)>,

    // Property indexes: (entity_id, key) → (page_id, slot)
    node_prop_index: HashMap<(u64, String), (PageId, u16)>,
    edge_prop_index: HashMap<(u64, String), (PageId, u16)>,

    // Monotonic counters
    next_node_local: u64,
    next_edge_local: u64,

    // Page ID counters (separate namespaces for node/edge/prop pools)
    next_node_page_id: PageId,
    next_edge_page_id: PageId,
    next_prop_page_id: PageId,

    // MVCC transaction state
    tx_mgr: TransactionManager,
    current_tx: TxId,
    current_snapshot: Option<Snapshot>,
    next_local_tx: TxId,

    // Group commit: track time + count since last WAL sync for periodic fsync.
    last_sync_ms: u64,
    commits_since_sync: u32,

    // Replication state
    read_only: bool,
    last_applied_lsn: u64,

    // I/O + WAL — None for ephemeral shards
    io: Option<RefCell<Box<dyn IoBackend + Send>>>,
    wal: Option<RefCell<WalWriter>>,
    node_file: Option<FileHandle>,
    edge_file: Option<FileHandle>,
    prop_file: Option<FileHandle>,
    label_file: Option<FileHandle>,
    _wal_file: Option<FileHandle>,
}

impl Shard {
    /// Create an ephemeral (in-memory only) shard. No I/O, no WAL.
    pub fn new(shard_id: ShardId) -> Self {
        Self {
            shard_id,
            labels: LabelDictionary::new(),
            node_pool: RefCell::new(BufferPool::new(DEFAULT_POOL_CAPACITY)),
            edge_pool: RefCell::new(BufferPool::new(DEFAULT_POOL_CAPACITY)),
            prop_pool: RefCell::new(BufferPool::new(DEFAULT_POOL_CAPACITY)),
            node_index: HashMap::new(),
            edge_index: HashMap::new(),
            node_prop_index: HashMap::new(),
            edge_prop_index: HashMap::new(),
            next_node_local: 1,
            next_edge_local: 1,
            next_node_page_id: 1,
            next_edge_page_id: 1,
            next_prop_page_id: 1,
            tx_mgr: TransactionManager::new(),
            current_tx: 0,
            current_snapshot: None,
            next_local_tx: 1,
            last_sync_ms: 0,
            commits_since_sync: 0,
            read_only: false,
            last_applied_lsn: 0,
            io: None,
            wal: None,
            node_file: None,
            edge_file: None,
            prop_file: None,
            label_file: None,
            _wal_file: None,
        }
    }

    /// Open a durable shard backed by files on disk using the default
    /// PosixIoBackend. Creates files if they don't exist, otherwise
    /// recovers from existing data + WAL replay.
    pub fn open(shard_id: ShardId, config: &ShardConfig) -> std::io::Result<Self> {
        Self::open_with_io(shard_id, config, graft_io::default_backend())
    }

    /// Open a durable shard with a caller-provided I/O backend.
    /// On Linux, pass `Box::new(IoUringBackend::new()?)` for io_uring.
    pub fn open_with_io(
        shard_id: ShardId,
        config: &ShardConfig,
        mut io: Box<dyn IoBackend + Send>,
    ) -> std::io::Result<Self> {
        let shard_dir = config.data_dir.join(format!("shard-{shard_id}"));
        std::fs::create_dir_all(&shard_dir)?;
        let opts = OpenOptions::create_read_write();

        let node_fh = io.open(&shard_dir.join("nodes.dat"), &opts)?;
        let edge_fh = io.open(&shard_dir.join("edges.dat"), &opts)?;
        let prop_fh = io.open(&shard_dir.join("props.dat"), &opts)?;
        let label_fh = io.open(&shard_dir.join("labels.dat"), &opts)?;
        let wal_fh = io.open(&shard_dir.join("shard.wal"), &opts)?;

        let cap = config.pool_capacity;
        let mut node_pool = BufferPool::new(cap);
        let mut edge_pool = BufferPool::new(cap);
        let mut prop_pool = BufferPool::new(cap);

        let mut node_index = HashMap::new();
        let mut edge_index = HashMap::new();
        let mut node_prop_index = HashMap::new();
        let mut edge_prop_index = HashMap::new();
        let mut labels = LabelDictionary::new();

        let mut next_node_local: u64 = 1;
        let mut next_edge_local: u64 = 1;
        let mut next_node_page_id: PageId = 1;
        let mut next_edge_page_id: PageId = 1;
        let mut next_prop_page_id: PageId = 1;
        let mut max_tx_id: TxId = 0;

        // --- WAL pass 1: build committed/uncommitted tx sets ---
        // Must happen BEFORE data file scan so we can filter out records
        // from aborted transactions that were flushed to data files.
        let wal_size = io.file_size(wal_fh)?;
        let mut committed_txs = hashbrown::HashSet::new();
        let mut uncommitted_txs = hashbrown::HashSet::new();
        if wal_size > 0 {
            let mut begun_txs = hashbrown::HashSet::new();
            let mut reader = WalReader::new(wal_fh, wal_size);
            while let Some(record) = reader.next(&mut *io) {
                if record.tx_id > max_tx_id {
                    max_tx_id = record.tx_id;
                }
                match record.record_type {
                    WalRecordType::Begin => {
                        begun_txs.insert(record.tx_id);
                    }
                    WalRecordType::Commit => {
                        committed_txs.insert(record.tx_id);
                    }
                    _ => {}
                }
            }
            // Uncommitted = began but never committed (aborted or in-flight at crash)
            for tx_id in &begun_txs {
                if !committed_txs.contains(tx_id) {
                    uncommitted_txs.insert(*tx_id);
                }
            }
        }

        // --- Scan data files ---
        // Skip records whose tx_min belongs to an uncommitted transaction.

        scan_data_file(
            &mut *io,
            node_fh,
            &mut node_pool,
            &mut |_page, pid, slot, rec_bytes| {
                let rec = NodeRecord::read_from(rec_bytes);
                // Skip records from uncommitted transactions — their data was
                // flushed to data files during shutdown but should not survive.
                if uncommitted_txs.contains(&rec.tx_min) {
                    return;
                }
                if !rec.is_deleted() && !rec.node_id.is_null() {
                    node_index.insert(rec.node_id.as_u64(), (pid, slot));
                    let local = rec.node_id.local_id();
                    if local >= next_node_local {
                        next_node_local = local + 1;
                    }
                }
                if rec.tx_min > max_tx_id {
                    max_tx_id = rec.tx_min;
                }
                if rec.tx_max > max_tx_id {
                    max_tx_id = rec.tx_max;
                }
                if pid >= next_node_page_id {
                    next_node_page_id = pid + 1;
                }
            },
        )?;

        scan_data_file(
            &mut *io,
            edge_fh,
            &mut edge_pool,
            &mut |_page, pid, slot, rec_bytes| {
                let rec = EdgeRecord::read_from(rec_bytes);
                if uncommitted_txs.contains(&rec.tx_min) {
                    return;
                }
                if !rec.is_deleted() && !rec.edge_id.is_null() {
                    edge_index.insert(rec.edge_id.as_u64(), (pid, slot));
                    let local = rec.edge_id.local_id();
                    if local >= next_edge_local {
                        next_edge_local = local + 1;
                    }
                }
                if rec.tx_min > max_tx_id {
                    max_tx_id = rec.tx_min;
                }
                if rec.tx_max > max_tx_id {
                    max_tx_id = rec.tx_max;
                }
                if pid >= next_edge_page_id {
                    next_edge_page_id = pid + 1;
                }
            },
        )?;

        scan_data_file(
            &mut *io,
            prop_fh,
            &mut prop_pool,
            &mut |_page, pid, slot, rec_bytes| {
                let rec = PropertyRecord::read_from(rec_bytes);
                if rec.entity_id != 0 {
                    let idx = match rec.entity_type {
                        ENTITY_TYPE_NODE => &mut node_prop_index,
                        _ => &mut edge_prop_index,
                    };
                    idx.insert((rec.entity_id, rec.key.clone()), (pid, slot));
                }
                if pid >= next_prop_page_id {
                    next_prop_page_id = pid + 1;
                }
            },
        )?;

        // Scan label file (sequential 64-byte records, no page wrapper)
        {
            let label_size = io.file_size(label_fh)?;
            let num_records = label_size / RECORD_SIZE as u64;
            for i in 0..num_records {
                let offset = i * RECORD_SIZE as u64;
                let mut buf = [0u8; RECORD_SIZE];
                if io.read_at(label_fh, offset, &mut buf).unwrap_or(0) == RECORD_SIZE {
                    let (lid, name) = LabelDictionary::read_label_record(&buf);
                    if lid != LabelId::NONE && !name.is_empty() {
                        labels.insert_label(lid, &name);
                    }
                }
            }
        }

        // --- WAL pass 2: replay only committed records ---
        if wal_size > 0 {
            // Pass 2: replay only committed (or pre-MVCC tx_id==0) records
            let mut reader = WalReader::new(wal_fh, wal_size);
            while let Some(record) = reader.next(&mut *io) {
                // Skip records from uncommitted transactions
                if record.tx_id != 0 && !committed_txs.contains(&record.tx_id) {
                    continue;
                }
                match record.body {
                    WalBody::PageWrite {
                        page_id,
                        slot,
                        page_type,
                        data,
                    } => {
                        match page_type {
                            1 => {
                                // Node
                                ensure_pool_has_page(
                                    &mut *io,
                                    node_fh,
                                    &mut node_pool,
                                    page_id,
                                    PageType::Node,
                                );
                                write_record_to_pool(&mut node_pool, page_id, slot, &data);
                                let rec = NodeRecord::read_from(&data);
                                if !rec.is_deleted() && !rec.node_id.is_null() {
                                    node_index.insert(rec.node_id.as_u64(), (page_id, slot));
                                    let local = rec.node_id.local_id();
                                    if local >= next_node_local {
                                        next_node_local = local + 1;
                                    }
                                }
                                if rec.tx_min > max_tx_id {
                                    max_tx_id = rec.tx_min;
                                }
                                if rec.tx_max > max_tx_id {
                                    max_tx_id = rec.tx_max;
                                }
                                if page_id >= next_node_page_id {
                                    next_node_page_id = page_id + 1;
                                }
                            }
                            2 => {
                                // Edge
                                ensure_pool_has_page(
                                    &mut *io,
                                    edge_fh,
                                    &mut edge_pool,
                                    page_id,
                                    PageType::Edge,
                                );
                                write_record_to_pool(&mut edge_pool, page_id, slot, &data);
                                let rec = EdgeRecord::read_from(&data);
                                if !rec.is_deleted() && !rec.edge_id.is_null() {
                                    edge_index.insert(rec.edge_id.as_u64(), (page_id, slot));
                                    let local = rec.edge_id.local_id();
                                    if local >= next_edge_local {
                                        next_edge_local = local + 1;
                                    }
                                }
                                if rec.tx_min > max_tx_id {
                                    max_tx_id = rec.tx_min;
                                }
                                if rec.tx_max > max_tx_id {
                                    max_tx_id = rec.tx_max;
                                }
                                if page_id >= next_edge_page_id {
                                    next_edge_page_id = page_id + 1;
                                }
                            }
                            3 => {
                                // Property
                                ensure_pool_has_page(
                                    &mut *io,
                                    prop_fh,
                                    &mut prop_pool,
                                    page_id,
                                    PageType::PropertyOverflow,
                                );
                                write_record_to_pool(&mut prop_pool, page_id, slot, &data);
                                let rec = PropertyRecord::read_from(&data);
                                if rec.entity_id != 0 {
                                    let idx = match rec.entity_type {
                                        ENTITY_TYPE_NODE => &mut node_prop_index,
                                        _ => &mut edge_prop_index,
                                    };
                                    idx.insert((rec.entity_id, rec.key.clone()), (page_id, slot));
                                }
                                if page_id >= next_prop_page_id {
                                    next_prop_page_id = page_id + 1;
                                }
                            }
                            4 => {
                                // Label
                                let (lid, name) = LabelDictionary::read_label_record(&data);
                                if lid != LabelId::NONE && !name.is_empty() {
                                    labels.insert_label(lid, &name);
                                }
                            }
                            _ => {}
                        }
                    }
                    WalBody::PageClear {
                        page_id,
                        slot,
                        page_type,
                    } => match page_type {
                        1 => {
                            if let Some(fid) = node_pool.pin(page_id) {
                                node_pool.page_mut(fid).free_slot(slot);
                                node_pool.unpin(fid);
                            }
                        }
                        2 => {
                            if let Some(fid) = edge_pool.pin(page_id) {
                                edge_pool.page_mut(fid).free_slot(slot);
                                edge_pool.unpin(fid);
                            }
                        }
                        3 => {
                            if let Some(fid) = prop_pool.pin(page_id) {
                                prop_pool.page_mut(fid).free_slot(slot);
                                prop_pool.unpin(fid);
                            }
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }
        }

        // --- Post-recovery checkpoint: flush dirty pages, truncate WAL ---
        flush_pool_to_file(&mut *io, node_fh, &mut node_pool)?;
        flush_pool_to_file(&mut *io, edge_fh, &mut edge_pool)?;
        flush_pool_to_file(&mut *io, prop_fh, &mut prop_pool)?;

        // Write labels
        write_labels_to_file(&mut *io, label_fh, &labels)?;

        // Sync all files
        io.sync(node_fh)?;
        io.sync(edge_fh)?;
        io.sync(prop_fh)?;
        io.sync(label_fh)?;

        // Truncate WAL (fresh start after checkpoint)
        io.truncate(wal_fh, 0)?;
        io.sync(wal_fh)?;

        let wal = WalWriter::new(wal_fh);

        let mut tx_mgr = TransactionManager::new();
        tx_mgr.advance_past(max_tx_id);
        let next_local_tx = max_tx_id + 1;

        Ok(Self {
            shard_id,
            labels,
            node_pool: RefCell::new(node_pool),
            edge_pool: RefCell::new(edge_pool),
            prop_pool: RefCell::new(prop_pool),
            node_index,
            edge_index,
            node_prop_index,
            edge_prop_index,
            next_node_local,
            next_edge_local,
            next_node_page_id,
            next_edge_page_id,
            next_prop_page_id,
            tx_mgr,
            current_tx: 0,
            current_snapshot: None,
            next_local_tx,
            last_sync_ms: 0,
            commits_since_sync: 0,
            read_only: false,
            last_applied_lsn: 0,
            io: Some(RefCell::new(io)),
            wal: Some(RefCell::new(wal)),
            node_file: Some(node_fh),
            edge_file: Some(edge_fh),
            prop_file: Some(prop_fh),
            label_file: Some(label_fh),
            _wal_file: Some(wal_fh),
        })
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// The next local transaction ID this shard would assign.
    /// Used by ShardCluster to initialize its global counter after recovery.
    pub fn next_local_tx(&self) -> TxId {
        self.next_local_tx
    }

    /// Whether this shard is in read-only mode (replica).
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Set read-only mode. When true, mutations return without effect.
    pub fn set_read_only(&mut self, read_only: bool) {
        self.read_only = read_only;
    }

    /// The last LSN applied from replication.
    pub fn last_applied_lsn(&self) -> u64 {
        self.last_applied_lsn
    }

    /// Apply a single WAL record to this shard (used by replication receiver).
    ///
    /// This applies the record directly to the in-memory data structures
    /// (buffer pools, indexes) without writing to the local WAL — the
    /// replica's WAL is the record stream from the primary.
    pub fn apply_wal_record(
        &mut self,
        record: &graft_txn::wal::WalRecord,
    ) {
        use graft_storage::page::PageType;
        use graft_storage::record::{ENTITY_TYPE_NODE};
        use graft_storage::{EdgeRecord, NodeRecord, PropertyRecord};
        use graft_txn::wal::{WalBody, WalRecordType};

        match &record.body {
            WalBody::PageWrite {
                page_id,
                slot,
                page_type,
                data,
            } => {
                match *page_type {
                    1 => {
                        // Node
                        Self::ensure_pool_has_page_inline(
                            &mut self.node_pool,
                            *page_id,
                            PageType::Node,
                        );
                        Self::write_record_to_pool_inline(&mut self.node_pool, *page_id, *slot, data);
                        let rec = NodeRecord::read_from(data);
                        if !rec.is_deleted() && !rec.node_id.is_null() {
                            self.node_index.insert(rec.node_id.as_u64(), (*page_id, *slot));
                            let local = rec.node_id.local_id();
                            if local >= self.next_node_local {
                                self.next_node_local = local + 1;
                            }
                        }
                        if rec.tx_min > 0 {
                            self.tx_mgr.advance_past(rec.tx_min);
                        }
                        if *page_id >= self.next_node_page_id {
                            self.next_node_page_id = *page_id + 1;
                        }
                    }
                    2 => {
                        // Edge
                        Self::ensure_pool_has_page_inline(
                            &mut self.edge_pool,
                            *page_id,
                            PageType::Edge,
                        );
                        Self::write_record_to_pool_inline(&mut self.edge_pool, *page_id, *slot, data);
                        let rec = EdgeRecord::read_from(data);
                        if !rec.is_deleted() && !rec.edge_id.is_null() {
                            self.edge_index.insert(rec.edge_id.as_u64(), (*page_id, *slot));
                            let local = rec.edge_id.local_id();
                            if local >= self.next_edge_local {
                                self.next_edge_local = local + 1;
                            }
                        }
                        if rec.tx_min > 0 {
                            self.tx_mgr.advance_past(rec.tx_min);
                        }
                        if *page_id >= self.next_edge_page_id {
                            self.next_edge_page_id = *page_id + 1;
                        }
                    }
                    3 => {
                        // Property
                        Self::ensure_pool_has_page_inline(
                            &mut self.prop_pool,
                            *page_id,
                            PageType::PropertyOverflow,
                        );
                        Self::write_record_to_pool_inline(&mut self.prop_pool, *page_id, *slot, data);
                        let rec = PropertyRecord::read_from(data);
                        if rec.entity_id != 0 {
                            let idx = match rec.entity_type {
                                ENTITY_TYPE_NODE => &mut self.node_prop_index,
                                _ => &mut self.edge_prop_index,
                            };
                            idx.insert((rec.entity_id, rec.key.clone()), (*page_id, *slot));
                        }
                        if *page_id >= self.next_prop_page_id {
                            self.next_prop_page_id = *page_id + 1;
                        }
                    }
                    4 => {
                        // Label
                        let (lid, name) = LabelDictionary::read_label_record(data);
                        if lid != LabelId::NONE && !name.is_empty() {
                            self.labels.insert_label(lid, &name);
                        }
                    }
                    _ => {}
                }
            }
            WalBody::PageClear {
                page_id,
                slot,
                page_type,
            } => {
                let pool = match *page_type {
                    1 => &self.node_pool,
                    2 => &self.edge_pool,
                    3 => &self.prop_pool,
                    _ => return,
                };
                let mut p = pool.borrow_mut();
                if let Some(fid) = p.pin(*page_id) {
                    p.page_mut(fid).free_slot(*slot);
                    p.unpin(fid);
                }
            }
            WalBody::Empty => {
                // Begin/Commit/Abort — update tx manager state
                match record.record_type {
                    WalRecordType::Begin => {
                        self.tx_mgr.advance_past(record.tx_id);
                    }
                    WalRecordType::Commit => {
                        self.tx_mgr.mark_committed(record.tx_id);
                    }
                    _ => {}
                }
            }
        }

        self.last_applied_lsn = record.lsn;
    }

    /// Inline helper: ensure a page exists in a pool (for apply_wal_record).
    fn ensure_pool_has_page_inline(
        pool: &mut RefCell<BufferPool>,
        page_id: PageId,
        page_type: graft_storage::page::PageType,
    ) {
        let mut p = pool.borrow_mut();
        if !p.contains(page_id) {
            let page = Page::new(page_id, page_type);
            let _ = p.create(page);
        }
    }

    /// Inline helper: write record data to a pool page (for apply_wal_record).
    fn write_record_to_pool_inline(
        pool: &mut RefCell<BufferPool>,
        page_id: PageId,
        slot: u16,
        data: &[u8; RECORD_SIZE],
    ) {
        let mut p = pool.borrow_mut();
        if let Some(fid) = p.pin(page_id) {
            let _ = p.page_mut(fid).write_record(slot, data);
            p.unpin(fid);
        }
    }

    /// Flush WAL and all dirty pages to disk. No-op for ephemeral shards.
    pub fn flush(&self) -> std::io::Result<()> {
        let io_cell = match &self.io {
            Some(io) => io,
            None => return Ok(()),
        };

        // Flush WAL
        if let Some(ref wal) = self.wal {
            wal.borrow_mut().flush(&mut **io_cell.borrow_mut())?;
        }

        // Collect dirty pages from each pool
        let node_dirty = self.node_pool.borrow_mut().dirty_pages();
        let edge_dirty = self.edge_pool.borrow_mut().dirty_pages();
        let prop_dirty = self.prop_pool.borrow_mut().dirty_pages();

        {
            let mut io = io_cell.borrow_mut();

            // Batched writes per file
            if let Some(fh) = self.node_file {
                if !node_dirty.is_empty() {
                    let batch: Vec<_> = node_dirty
                        .iter()
                        .map(|(pid, data)| (page_file_offset(*pid), data))
                        .collect();
                    io.write_pages_batch(fh, &batch)?;
                }
            }
            if let Some(fh) = self.edge_file {
                if !edge_dirty.is_empty() {
                    let batch: Vec<_> = edge_dirty
                        .iter()
                        .map(|(pid, data)| (page_file_offset(*pid), data))
                        .collect();
                    io.write_pages_batch(fh, &batch)?;
                }
            }
            if let Some(fh) = self.prop_file {
                if !prop_dirty.is_empty() {
                    let batch: Vec<_> = prop_dirty
                        .iter()
                        .map(|(pid, data)| (page_file_offset(*pid), data))
                        .collect();
                    io.write_pages_batch(fh, &batch)?;
                }
            }

            // Batched sync for all files that had dirty pages
            let mut sync_handles = Vec::new();
            if self.node_file.is_some() && !node_dirty.is_empty() {
                sync_handles.push(self.node_file.unwrap());
            }
            if self.edge_file.is_some() && !edge_dirty.is_empty() {
                sync_handles.push(self.edge_file.unwrap());
            }
            if self.prop_file.is_some() && !prop_dirty.is_empty() {
                sync_handles.push(self.prop_file.unwrap());
            }
            if !sync_handles.is_empty() {
                io.sync_batch(&sync_handles)?;
            }
        }

        // Clear dirty flags
        {
            let mut pool = self.node_pool.borrow_mut();
            for (page_id, _) in &node_dirty {
                pool.clear_dirty_page(*page_id);
            }
        }
        {
            let mut pool = self.edge_pool.borrow_mut();
            for (page_id, _) in &edge_dirty {
                pool.clear_dirty_page(*page_id);
            }
        }
        {
            let mut pool = self.prop_pool.borrow_mut();
            for (page_id, _) in &prop_dirty {
                pool.clear_dirty_page(*page_id);
            }
        }

        // Write labels
        if let (Some(ref io_cell), Some(label_fh)) = (&self.io, self.label_file) {
            let mut io = io_cell.borrow_mut();
            write_labels_to_file(&mut **io, label_fh, &self.labels)?;
            io.sync(label_fh)?;
        }

        Ok(())
    }

    // -- ID allocation ---------------------------------------------------------

    fn alloc_node_id(&mut self) -> NodeId {
        let id = NodeId::new(self.shard_id, self.next_node_local);
        self.next_node_local += 1;
        id
    }

    fn alloc_edge_id(&mut self) -> EdgeId {
        let id = EdgeId::new(self.shard_id, self.next_edge_local);
        self.next_edge_local += 1;
        id
    }

    // -- Page allocation helpers -----------------------------------------------

    fn alloc_slot(
        pool: &RefCell<BufferPool>,
        next_page_id: &mut PageId,
        page_type: PageType,
    ) -> (PageId, u16, Option<graft_storage::buffer_pool::EvictedPage>) {
        let mut p = pool.borrow_mut();

        // Try the most recent page first
        if *next_page_id > 1 {
            let last_page_id = *next_page_id - 1;
            if let Some(fid) = p.pin(last_page_id) {
                let page = p.page_mut(fid);
                if !page.is_full() {
                    let slot = page.alloc_slot().unwrap();
                    p.unpin(fid);
                    return (last_page_id, slot, None);
                }
                p.unpin(fid);
            }
        }

        // Allocate a new page
        let page_id = *next_page_id;
        *next_page_id += 1;
        let page = Page::new(page_id, page_type);
        let evicted = p.create(page).expect("pool create failed").1;

        let fid = p.pin(page_id).unwrap();
        let slot = p.page_mut(fid).alloc_slot().unwrap();
        p.unpin(fid);
        (page_id, slot, evicted)
    }

    fn alloc_node_slot(&mut self) -> (PageId, u16) {
        let (pid, slot, evicted) =
            Self::alloc_slot(&self.node_pool, &mut self.next_node_page_id, PageType::Node);
        if let Some(ev) = evicted {
            self.handle_eviction(&ev);
        }
        (pid, slot)
    }

    fn alloc_edge_slot(&mut self) -> (PageId, u16) {
        let (pid, slot, evicted) =
            Self::alloc_slot(&self.edge_pool, &mut self.next_edge_page_id, PageType::Edge);
        if let Some(ev) = evicted {
            self.handle_eviction(&ev);
        }
        (pid, slot)
    }

    fn alloc_prop_slot(&mut self) -> (PageId, u16) {
        let (pid, slot, evicted) = Self::alloc_slot(
            &self.prop_pool,
            &mut self.next_prop_page_id,
            PageType::PropertyOverflow,
        );
        if let Some(ev) = evicted {
            self.handle_eviction(&ev);
        }
        (pid, slot)
    }

    // -- I/O helpers -----------------------------------------------------------

    /// Load a page from disk into the pool if not already present.
    fn ensure_page_loaded(
        &self,
        pool: &RefCell<BufferPool>,
        file: Option<FileHandle>,
        page_id: PageId,
    ) {
        if pool.borrow().contains(page_id) {
            return;
        }
        let (io_cell, fh) = match (&self.io, file) {
            (Some(io), Some(fh)) => (io, fh),
            _ => return,
        };
        let mut io = io_cell.borrow_mut();
        let mut buf = [0u8; PAGE_SIZE];
        if io
            .read_page(fh, page_file_offset(page_id), &mut buf)
            .is_err()
        {
            return;
        }
        drop(io);

        let mut pool_mut = pool.borrow_mut();
        if pool_mut.contains(page_id) {
            return;
        }
        if let Ok(Some(evicted)) = pool_mut.load(page_id, &buf) {
            drop(pool_mut);
            self.handle_eviction(&evicted);
        }
    }

    /// Write an evicted dirty page to the correct data file.
    fn handle_eviction(&self, evicted: &graft_storage::buffer_pool::EvictedPage) {
        if let Some(ref io_cell) = self.io {
            let page_type = PageType::from_page_bytes(&evicted.data);
            let fh = match page_type {
                Some(PageType::Node) => self.node_file,
                Some(PageType::Edge) => self.edge_file,
                Some(PageType::PropertyOverflow) => self.prop_file,
                Some(PageType::Label) => self.label_file,
                _ => None,
            };
            if let Some(fh) = fh {
                let mut io = io_cell.borrow_mut();
                let _ = io.write_page(fh, page_file_offset(evicted.page_id), &evicted.data);
            }
        }
    }

    /// Log a page write to the WAL.
    fn log_page_write(&self, page_id: PageId, slot: u16, data: &[u8; RECORD_SIZE], page_type: u8) {
        if let Some(ref wal_cell) = self.wal {
            let mut wal = wal_cell.borrow_mut();
            wal.append(
                self.current_tx,
                WalRecordType::PageWrite,
                &WalBody::PageWrite {
                    page_id,
                    slot,
                    page_type,
                    data: *data,
                },
            );
            if wal.should_flush() {
                if let Some(ref io_cell) = self.io {
                    let _ = wal.write(&mut **io_cell.borrow_mut());
                }
            }
        }
    }

    /// Log a label creation to the WAL.
    fn log_label_write(&self, id: LabelId, name: &str) {
        if let Some(ref wal_cell) = self.wal {
            let mut data = [0u8; RECORD_SIZE];
            LabelDictionary::write_label_record(id, name, &mut data);
            let mut wal = wal_cell.borrow_mut();
            wal.append(
                self.current_tx,
                WalRecordType::PageWrite,
                &WalBody::PageWrite {
                    page_id: 0,
                    slot: 0,
                    page_type: PageType::Label as u8,
                    data,
                },
            );
            if wal.should_flush() {
                if let Some(ref io_cell) = self.io {
                    let _ = wal.write(&mut **io_cell.borrow_mut());
                }
            }
        }
    }

    // -- MVCC transaction lifecycle --------------------------------------------

    /// Begin a transaction with an externally-assigned tx_id (used by
    /// ShardCluster which generates a global tx_id).
    pub fn begin_tx_with_id(&mut self, tx_id: TxId) {
        let mut wal_ref = self.wal.as_ref().map(|w| w.borrow_mut());
        let wal_opt = wal_ref.as_deref_mut();
        self.tx_mgr.begin_with_id(tx_id, wal_opt);
        self.current_tx = tx_id;
        self.current_snapshot = self.tx_mgr.snapshot(tx_id).cloned();
    }

    /// Commit the current transaction.
    ///
    /// Uses group commit: the WAL commit record is written to the OS page
    /// cache immediately (safe against process crashes) but fsync is deferred
    /// until the group commit interval (2ms) has elapsed. This bounds the
    /// worst-case data loss on power failure to ~2ms of committed transactions.
    pub fn commit_current_tx(&mut self) {
        if self.current_tx == 0 {
            return;
        }
        let tx_id = self.current_tx;

        // Scope the RefCell borrows so they're dropped before the group commit sync.
        {
            let mut wal_ref = self.wal.as_ref().map(|w| w.borrow_mut());
            let wal_opt = wal_ref.as_deref_mut();
            let mut io_ref = self.io.as_ref().map(|i| i.borrow_mut());
            let io_opt: Option<&mut dyn IoBackend> = match io_ref.as_mut() {
                Some(r) => Some(&mut ***r),
                None => None,
            };
            let _ = self.tx_mgr.commit(tx_id, wal_opt, io_opt);
        }

        self.current_tx = 0;
        self.current_snapshot = None;

        // Group commit: periodically fsync the WAL to bound the durability window.
        // Sync when either the time interval or commit count threshold is reached.
        self.commits_since_sync += 1;
        if let (Some(ref wal_cell), Some(ref io_cell)) = (&self.wal, &self.io) {
            let should_sync = self.commits_since_sync >= GROUP_COMMIT_MAX_PENDING || {
                let now = io_cell.borrow().now_millis();
                now >= self.last_sync_ms + GROUP_COMMIT_INTERVAL_MS
            };
            if should_sync {
                let mut wal = wal_cell.borrow_mut();
                let mut io = io_cell.borrow_mut();
                let _ = wal.sync(&mut **io);
                self.last_sync_ms = io.now_millis();
                self.commits_since_sync = 0;
            }
        }
    }

    /// Switch the shard's active transaction context to an already-begun tx.
    /// Used when the coordinator needs to restore tx context for an explicit
    /// transaction across serialized connection requests.
    pub fn set_active_tx(&mut self, tx_id: TxId) {
        self.current_tx = tx_id;
        if tx_id == 0 {
            self.current_snapshot = None;
        } else {
            self.current_snapshot = self.tx_mgr.snapshot(tx_id).cloned();
        }
    }

    /// Abort the current transaction.
    pub fn abort_current_tx(&mut self) {
        if self.current_tx == 0 {
            return;
        }
        let tx_id = self.current_tx;
        let mut wal_ref = self.wal.as_ref().map(|w| w.borrow_mut());
        let wal_opt = wal_ref.as_deref_mut();
        self.tx_mgr.abort(tx_id, wal_opt);
        self.current_tx = 0;
        self.current_snapshot = None;
    }

    /// Check if a record is visible to the current transaction.
    ///
    /// When no transaction is active (current_tx == 0), falls back to the
    /// legacy check: tx_max == 0 (not deleted).
    ///
    /// When a transaction is active:
    /// - Pre-MVCC records (tx_min == 0) are visible if not deleted (tx_max == 0).
    /// - Own writes (tx_min == current_tx) are visible unless also deleted
    ///   by current_tx.
    /// - Otherwise: tx_min must be a committed transaction visible to our
    ///   snapshot, and either not deleted or deleted by a tx not yet visible.
    fn is_record_visible(&self, tx_min: TxId, tx_max: TxId) -> bool {
        if self.current_tx == 0 {
            // Legacy path: no MVCC, just check deletion
            return tx_max == 0;
        }

        // Pre-MVCC records (tx_min == 0)
        if tx_min == 0 {
            return tx_max == 0;
        }

        // Own-writes visibility
        if tx_min == self.current_tx {
            return tx_max == 0 || tx_max != self.current_tx;
        }

        // tx_min must have been committed to be visible
        if !self.tx_mgr.was_committed(tx_min) {
            return false;
        }

        if let Some(ref snap) = self.current_snapshot {
            snap.is_visible(tx_min, tx_max)
        } else {
            tx_max == 0
        }
    }

    // -- Record read/write helpers ---------------------------------------------

    fn read_node_record(&self, page_id: PageId, slot: u16) -> Option<NodeRecord> {
        self.ensure_page_loaded(&self.node_pool, self.node_file, page_id);
        let mut pool = self.node_pool.borrow_mut();
        let fid = pool.pin(page_id)?;
        let buf = pool.page(fid).read_record(slot).ok()?;
        let rec = NodeRecord::read_from(buf);
        pool.unpin(fid);
        Some(rec)
    }

    fn write_node_record(&self, page_id: PageId, slot: u16, rec: &NodeRecord) {
        let bytes = rec.to_bytes();
        {
            let mut pool = self.node_pool.borrow_mut();
            let fid = pool.pin(page_id).expect("page not in pool");
            pool.page_mut(fid)
                .write_record(slot, &bytes)
                .expect("write_record failed");
            pool.unpin(fid);
        }
        self.log_page_write(page_id, slot, &bytes, PageType::Node as u8);
    }

    fn read_edge_record(&self, page_id: PageId, slot: u16) -> Option<EdgeRecord> {
        self.ensure_page_loaded(&self.edge_pool, self.edge_file, page_id);
        let mut pool = self.edge_pool.borrow_mut();
        let fid = pool.pin(page_id)?;
        let buf = pool.page(fid).read_record(slot).ok()?;
        let rec = EdgeRecord::read_from(buf);
        pool.unpin(fid);
        Some(rec)
    }

    fn write_edge_record(&self, page_id: PageId, slot: u16, rec: &EdgeRecord) {
        let bytes = rec.to_bytes();
        {
            let mut pool = self.edge_pool.borrow_mut();
            let fid = pool.pin(page_id).expect("page not in pool");
            pool.page_mut(fid)
                .write_record(slot, &bytes)
                .expect("write_record failed");
            pool.unpin(fid);
        }
        self.log_page_write(page_id, slot, &bytes, PageType::Edge as u8);
    }

    fn read_prop_record(&self, page_id: PageId, slot: u16) -> Option<PropertyRecord> {
        self.ensure_page_loaded(&self.prop_pool, self.prop_file, page_id);
        let mut pool = self.prop_pool.borrow_mut();
        let fid = pool.pin(page_id)?;
        let buf = pool.page(fid).read_record(slot).ok()?;
        let rec = PropertyRecord::read_from(buf);
        pool.unpin(fid);
        Some(rec)
    }

    fn write_prop_record(&self, page_id: PageId, slot: u16, rec: &PropertyRecord) {
        let bytes = rec.to_bytes();
        {
            let mut pool = self.prop_pool.borrow_mut();
            let fid = pool.pin(page_id).expect("page not in pool");
            pool.page_mut(fid)
                .write_record(slot, &bytes)
                .expect("write_record failed");
            pool.unpin(fid);
        }
        self.log_page_write(page_id, slot, &bytes, PageType::PropertyOverflow as u8);
    }

    // -- Lookup helpers --------------------------------------------------------

    fn find_node(&self, id: NodeId) -> Option<NodeRecord> {
        let &(page_id, slot) = self.node_index.get(&id.as_u64())?;
        let rec = self.read_node_record(page_id, slot)?;
        if rec.node_id.is_null() || !self.is_record_visible(rec.tx_min, rec.tx_max) {
            return None;
        }
        Some(rec)
    }

    fn find_edge(&self, id: EdgeId) -> Option<EdgeRecord> {
        let &(page_id, slot) = self.edge_index.get(&id.as_u64())?;
        let rec = self.read_edge_record(page_id, slot)?;
        if rec.edge_id.is_null() || !self.is_record_visible(rec.tx_min, rec.tx_max) {
            return None;
        }
        Some(rec)
    }

    /// Read an edge record without visibility filtering — used by edge chain
    /// traversal to get the next pointer even for invisible edges.
    fn read_edge_raw(&self, id: EdgeId) -> Option<EdgeRecord> {
        let &(page_id, slot) = self.edge_index.get(&id.as_u64())?;
        self.read_edge_record(page_id, slot)
    }
}

// ---------------------------------------------------------------------------
// Recovery helpers (standalone functions to avoid borrow issues)
// ---------------------------------------------------------------------------

#[allow(clippy::type_complexity)]
fn scan_data_file(
    io: &mut dyn IoBackend,
    file: FileHandle,
    pool: &mut BufferPool,
    visitor: &mut dyn FnMut(&Page, PageId, u16, &[u8]),
) -> std::io::Result<()> {
    let file_size = io.file_size(file)?;
    let num_pages = file_size / PAGE_SIZE as u64;
    for i in 0..num_pages {
        let offset = i * PAGE_SIZE as u64;
        let mut buf = [0u8; PAGE_SIZE];
        io.read_page(file, offset, &mut buf)?;
        let page = Page::from_bytes(buf);
        let pid = page.page_id();
        if pid == 0 {
            continue;
        }

        // Visit occupied slots
        for slot in page.occupied_slots() {
            if let Ok(rec_bytes) = page.read_record(slot) {
                let mut owned = [0u8; RECORD_SIZE];
                owned.copy_from_slice(rec_bytes);
                visitor(&page, pid, slot, &owned);
            }
        }

        // Load page into pool (handle eviction by writing back)
        if !pool.contains(pid) {
            match pool.load(pid, page.as_bytes()) {
                Ok(Some(evicted)) => {
                    // Write evicted page back to its file
                    let evicted_type = PageType::from_page_bytes(&evicted.data);
                    if evicted_type.is_some() {
                        let _ =
                            io.write_page(file, page_file_offset(evicted.page_id), &evicted.data);
                    }
                }
                Ok(None) => {}
                Err(_) => {} // AlreadyLoaded
            }
        }
    }
    Ok(())
}

fn ensure_pool_has_page(
    io: &mut dyn IoBackend,
    file: FileHandle,
    pool: &mut BufferPool,
    page_id: PageId,
    page_type: PageType,
) {
    if pool.contains(page_id) {
        return;
    }
    // Try to load from disk
    let mut buf = [0u8; PAGE_SIZE];
    if io
        .read_page(file, page_file_offset(page_id), &mut buf)
        .is_ok()
    {
        let _ = pool.load(page_id, &buf);
        return;
    }
    // Page doesn't exist on disk yet — create it
    let page = Page::new(page_id, page_type);
    let _ = pool.create(page);
}

fn write_record_to_pool(
    pool: &mut BufferPool,
    page_id: PageId,
    slot: u16,
    data: &[u8; RECORD_SIZE],
) {
    if let Some(fid) = pool.pin(page_id) {
        // Ensure the slot is allocated (during replay the page may be fresh)
        let page = pool.page_mut(fid);
        let _ = page.write_record(slot, data);
        pool.unpin(fid);
    }
}

fn flush_pool_to_file(
    io: &mut dyn IoBackend,
    file: FileHandle,
    pool: &mut BufferPool,
) -> std::io::Result<()> {
    let dirty = pool.dirty_pages();
    for (page_id, data) in &dirty {
        io.write_page(file, page_file_offset(*page_id), data)?;
    }
    for (page_id, _) in &dirty {
        pool.clear_dirty_page(*page_id);
    }
    Ok(())
}

fn write_labels_to_file(
    io: &mut dyn IoBackend,
    file: FileHandle,
    labels: &LabelDictionary,
) -> std::io::Result<()> {
    io.truncate(file, 0)?;
    let records = labels.to_records();
    for (i, (name, id)) in records.iter().enumerate() {
        let mut buf = [0u8; RECORD_SIZE];
        LabelDictionary::write_label_record(*id, name, &mut buf);
        io.write_at(file, (i * RECORD_SIZE) as u64, &buf)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// StorageAccess implementation
// ---------------------------------------------------------------------------

impl StorageAccess for Shard {
    fn scan_nodes(&self, label: Option<&str>) -> Vec<NodeInfo> {
        let label_id = label.and_then(|l| self.labels.get(l));

        if label.is_some() && label_id.is_none() {
            return Vec::new();
        }

        let mut result = Vec::new();
        for (&raw_id, &(page_id, slot)) in &self.node_index {
            if let Some(rec) = self.read_node_record(page_id, slot) {
                if rec.node_id.is_null() || !self.is_record_visible(rec.tx_min, rec.tx_max) {
                    continue;
                }
                if let Some(lid) = label_id {
                    if rec.label_id != lid {
                        continue;
                    }
                }
                result.push(NodeInfo {
                    id: NodeId::from_raw(raw_id),
                    label: self.labels.name(rec.label_id).map(String::from),
                });
            }
        }
        result
    }

    fn get_node(&self, id: NodeId) -> Option<NodeInfo> {
        let rec = self.find_node(id)?;
        Some(NodeInfo {
            id: rec.node_id,
            label: self.labels.name(rec.label_id).map(String::from),
        })
    }

    fn node_property(&self, id: NodeId, key: &str) -> Value {
        if self.find_node(id).is_none() {
            return Value::Null;
        }
        match self.node_prop_index.get(&(id.as_u64(), key.to_owned())) {
            Some(&(page_id, slot)) => self
                .read_prop_record(page_id, slot)
                .map(|r| prop_record_to_value(&r))
                .unwrap_or(Value::Null),
            None => Value::Null,
        }
    }

    fn outbound_edges(&self, node_id: NodeId, label: Option<&str>) -> Vec<EdgeInfo> {
        let label_id = label.and_then(|l| self.labels.get(l));
        if label.is_some() && label_id.is_none() {
            return Vec::new();
        }

        let node = match self.find_node(node_id) {
            Some(n) => n,
            None => return Vec::new(),
        };

        let mut result = Vec::new();
        let mut edge_id = node.first_out_edge;
        while !edge_id.is_null() {
            let edge = match self.read_edge_raw(edge_id) {
                Some(e) => e,
                None => break,
            };
            let next = edge.next_out_edge;
            if self.is_record_visible(edge.tx_min, edge.tx_max)
                && (label_id.is_none() || edge.label_id == label_id.unwrap())
            {
                result.push(EdgeInfo {
                    id: edge.edge_id,
                    source: edge.source,
                    target: edge.target,
                    label: self.labels.name(edge.label_id).map(String::from),
                });
            }
            edge_id = next;
        }
        result
    }

    fn inbound_edges(&self, node_id: NodeId, label: Option<&str>) -> Vec<EdgeInfo> {
        let label_id = label.and_then(|l| self.labels.get(l));
        if label.is_some() && label_id.is_none() {
            return Vec::new();
        }

        let node = match self.find_node(node_id) {
            Some(n) => n,
            None => return Vec::new(),
        };

        let mut result = Vec::new();
        let mut edge_id = node.first_in_edge;
        while !edge_id.is_null() {
            let edge = match self.read_edge_raw(edge_id) {
                Some(e) => e,
                None => break,
            };
            let next = edge.next_in_edge;
            if self.is_record_visible(edge.tx_min, edge.tx_max)
                && (label_id.is_none() || edge.label_id == label_id.unwrap())
            {
                result.push(EdgeInfo {
                    id: edge.edge_id,
                    source: edge.source,
                    target: edge.target,
                    label: self.labels.name(edge.label_id).map(String::from),
                });
            }
            edge_id = next;
        }
        result
    }

    fn edge_property(&self, id: EdgeId, key: &str) -> Value {
        if self.find_edge(id).is_none() {
            return Value::Null;
        }
        match self.edge_prop_index.get(&(id.as_u64(), key.to_owned())) {
            Some(&(page_id, slot)) => self
                .read_prop_record(page_id, slot)
                .map(|r| prop_record_to_value(&r))
                .unwrap_or(Value::Null),
            None => Value::Null,
        }
    }

    fn create_node(&mut self, label: Option<&str>, properties: &[(String, Value)]) -> NodeId {
        let id = self.alloc_node_id();

        let is_new_label = label.is_some() && self.labels.get(label.unwrap()).is_none();
        let label_id = label
            .map(|l| self.labels.get_or_insert(l))
            .unwrap_or(LabelId::NONE);
        if is_new_label {
            self.log_label_write(label_id, label.unwrap());
        }

        let rec = NodeRecord::new(id, label_id, self.current_tx);
        let (page_id, slot) = self.alloc_node_slot();
        self.write_node_record(page_id, slot, &rec);
        self.node_index.insert(id.as_u64(), (page_id, slot));

        for (k, v) in properties {
            let prop_rec = value_to_prop_record(id.as_u64(), ENTITY_TYPE_NODE, k, v);
            let (pp, ps) = self.alloc_prop_slot();
            self.write_prop_record(pp, ps, &prop_rec);
            self.node_prop_index
                .insert((id.as_u64(), k.clone()), (pp, ps));
        }

        id
    }

    fn create_edge(
        &mut self,
        source: NodeId,
        target: NodeId,
        label: Option<&str>,
        properties: &[(String, Value)],
    ) -> EdgeId {
        let id = self.alloc_edge_id();

        let is_new_label = label.is_some() && self.labels.get(label.unwrap()).is_none();
        let label_id = label
            .map(|l| self.labels.get_or_insert(l))
            .unwrap_or(LabelId::NONE);
        if is_new_label {
            self.log_label_write(label_id, label.unwrap());
        }

        // Read current head pointers
        let old_out_head = self
            .find_node(source)
            .map(|n| n.first_out_edge)
            .unwrap_or(EdgeId::NULL);
        let old_in_head = self
            .find_node(target)
            .map(|n| n.first_in_edge)
            .unwrap_or(EdgeId::NULL);

        // Create edge record with linked list pointers
        let mut rec = EdgeRecord::new(id, source, target, label_id, self.current_tx);
        rec.next_out_edge = old_out_head;
        rec.next_in_edge = old_in_head;

        let (page_id, slot) = self.alloc_edge_slot();
        self.write_edge_record(page_id, slot, &rec);
        self.edge_index.insert(id.as_u64(), (page_id, slot));

        // Update source node's first_out_edge
        if let Some(&(np, ns)) = self.node_index.get(&source.as_u64()) {
            if let Some(mut node_rec) = self.read_node_record(np, ns) {
                if !node_rec.is_deleted() {
                    node_rec.first_out_edge = id;
                    self.write_node_record(np, ns, &node_rec);
                }
            }
        }

        // Update target node's first_in_edge
        if let Some(&(np, ns)) = self.node_index.get(&target.as_u64()) {
            if let Some(mut node_rec) = self.read_node_record(np, ns) {
                if !node_rec.is_deleted() {
                    node_rec.first_in_edge = id;
                    self.write_node_record(np, ns, &node_rec);
                }
            }
        }

        for (k, v) in properties {
            let prop_rec = value_to_prop_record(id.as_u64(), ENTITY_TYPE_EDGE, k, v);
            let (pp, ps) = self.alloc_prop_slot();
            self.write_prop_record(pp, ps, &prop_rec);
            self.edge_prop_index
                .insert((id.as_u64(), k.clone()), (pp, ps));
        }

        id
    }

    fn set_node_property(&mut self, id: NodeId, key: &str, value: &Value) {
        if self.find_node(id).is_none() {
            return;
        }
        let prop_rec = value_to_prop_record(id.as_u64(), ENTITY_TYPE_NODE, key, value);
        if let Some(&(page_id, slot)) = self.node_prop_index.get(&(id.as_u64(), key.to_owned())) {
            self.write_prop_record(page_id, slot, &prop_rec);
        } else {
            let (pp, ps) = self.alloc_prop_slot();
            self.write_prop_record(pp, ps, &prop_rec);
            self.node_prop_index
                .insert((id.as_u64(), key.to_owned()), (pp, ps));
        }
    }

    fn set_edge_property(&mut self, id: EdgeId, key: &str, value: &Value) {
        if self.find_edge(id).is_none() {
            return;
        }
        let prop_rec = value_to_prop_record(id.as_u64(), ENTITY_TYPE_EDGE, key, value);
        if let Some(&(page_id, slot)) = self.edge_prop_index.get(&(id.as_u64(), key.to_owned())) {
            self.write_prop_record(page_id, slot, &prop_rec);
        } else {
            let (pp, ps) = self.alloc_prop_slot();
            self.write_prop_record(pp, ps, &prop_rec);
            self.edge_prop_index
                .insert((id.as_u64(), key.to_owned()), (pp, ps));
        }
    }

    fn delete_node(&mut self, id: NodeId) {
        let del_tx = if self.current_tx == 0 {
            1
        } else {
            self.current_tx
        };
        if let Some(&(page_id, slot)) = self.node_index.get(&id.as_u64()) {
            if let Some(mut rec) = self.read_node_record(page_id, slot) {
                rec.tx_max = del_tx;
                self.write_node_record(page_id, slot, &rec);
            }
        }
    }

    fn delete_edge(&mut self, id: EdgeId) {
        let del_tx = if self.current_tx == 0 {
            1
        } else {
            self.current_tx
        };
        if let Some(&(page_id, slot)) = self.edge_index.get(&id.as_u64()) {
            if let Some(mut rec) = self.read_edge_record(page_id, slot) {
                rec.tx_max = del_tx;
                self.write_edge_record(page_id, slot, &rec);
            }
        }
    }

    fn begin_tx(&mut self) -> u64 {
        let tx_id = self.next_local_tx;
        self.next_local_tx += 1;
        self.begin_tx_with_id(tx_id);
        tx_id
    }

    fn commit_tx(&mut self) {
        self.commit_current_tx();
    }

    fn abort_tx(&mut self) {
        self.abort_current_tx();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graft_core::constants::RECORDS_PER_PAGE;

    #[test]
    fn create_and_scan_nodes() {
        let mut shard = Shard::new(0);
        let id = shard.create_node(
            Some("Person"),
            &[("name".into(), Value::String("Alice".into()))],
        );
        assert!(!id.is_null());

        let nodes = shard.scan_nodes(Some("Person"));
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].label.as_deref(), Some("Person"));

        assert_eq!(
            shard.node_property(id, "name"),
            Value::String("Alice".into())
        );
        assert_eq!(shard.node_property(id, "missing"), Value::Null);
    }

    #[test]
    fn scan_with_label_filter() {
        let mut shard = Shard::new(0);
        shard.create_node(Some("Person"), &[]);
        shard.create_node(Some("Company"), &[]);
        shard.create_node(Some("Person"), &[]);

        assert_eq!(shard.scan_nodes(Some("Person")).len(), 2);
        assert_eq!(shard.scan_nodes(Some("Company")).len(), 1);
        assert_eq!(shard.scan_nodes(Some("Missing")).len(), 0);
        assert_eq!(shard.scan_nodes(None).len(), 3);
    }

    #[test]
    fn create_edge_and_traverse() {
        let mut shard = Shard::new(0);
        let alice = shard.create_node(Some("Person"), &[]);
        let bob = shard.create_node(Some("Person"), &[]);
        let edge = shard.create_edge(
            alice,
            bob,
            Some("KNOWS"),
            &[("since".into(), Value::Int(2020))],
        );

        let out = shard.outbound_edges(alice, None);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].target, bob);

        let inb = shard.inbound_edges(bob, None);
        assert_eq!(inb.len(), 1);
        assert_eq!(inb[0].source, alice);

        assert_eq!(shard.edge_property(edge, "since"), Value::Int(2020));
    }

    #[test]
    fn index_free_adjacency_linked_list() {
        let mut shard = Shard::new(0);
        let a = shard.create_node(Some("N"), &[]);
        let b = shard.create_node(Some("N"), &[]);
        let c = shard.create_node(Some("N"), &[]);

        shard.create_edge(a, b, Some("E"), &[]);
        shard.create_edge(a, c, Some("E"), &[]);

        let out = shard.outbound_edges(a, None);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].target, c);
        assert_eq!(out[1].target, b);
    }

    #[test]
    fn edge_label_filter() {
        let mut shard = Shard::new(0);
        let a = shard.create_node(None, &[]);
        let b = shard.create_node(None, &[]);
        shard.create_edge(a, b, Some("KNOWS"), &[]);
        shard.create_edge(a, b, Some("WORKS_WITH"), &[]);

        assert_eq!(shard.outbound_edges(a, Some("KNOWS")).len(), 1);
        assert_eq!(shard.outbound_edges(a, Some("WORKS_WITH")).len(), 1);
        assert_eq!(shard.outbound_edges(a, None).len(), 2);
    }

    #[test]
    fn set_property_updates_existing() {
        let mut shard = Shard::new(0);
        let id = shard.create_node(None, &[("x".into(), Value::Int(1))]);
        shard.set_node_property(id, "x", &Value::Int(99));
        assert_eq!(shard.node_property(id, "x"), Value::Int(99));
    }

    #[test]
    fn delete_node_hides_from_scan() {
        let mut shard = Shard::new(0);
        let id = shard.create_node(Some("X"), &[]);
        assert_eq!(shard.scan_nodes(None).len(), 1);
        shard.delete_node(id);
        assert_eq!(shard.scan_nodes(None).len(), 0);
        assert!(shard.get_node(id).is_none());
    }

    #[test]
    fn delete_edge_hides_from_traversal() {
        let mut shard = Shard::new(0);
        let a = shard.create_node(None, &[]);
        let b = shard.create_node(None, &[]);
        let eid = shard.create_edge(a, b, None, &[]);
        assert_eq!(shard.outbound_edges(a, None).len(), 1);
        shard.delete_edge(eid);
        // Edge is deleted — linked-list walk breaks at deleted edge.
    }

    #[test]
    fn page_fills_and_spills() {
        let mut shard = Shard::new(0);
        let count = RECORDS_PER_PAGE + 10;
        for i in 0..count {
            shard.create_node(Some("N"), &[("i".into(), Value::Int(i as i64))]);
        }
        assert_eq!(shard.scan_nodes(Some("N")).len(), count);
        assert!(
            shard.next_node_page_id > 2,
            "should have allocated 2+ pages"
        );
    }

    #[test]
    fn shard_open_and_recover() {
        let dir = tempfile::tempdir().unwrap();

        // Create data in a durable shard
        let id;
        let edge_id;
        {
            let mut shard = Shard::open(
                0,
                &ShardConfig {
                    data_dir: dir.path().to_owned(),
                    pool_capacity: 64,
                },
            )
            .unwrap();

            id = shard.create_node(
                Some("Person"),
                &[
                    ("name".into(), Value::String("Alice".into())),
                    ("age".into(), Value::Int(30)),
                ],
            );
            let bob = shard.create_node(
                Some("Person"),
                &[("name".into(), Value::String("Bob".into()))],
            );
            edge_id = shard.create_edge(
                id,
                bob,
                Some("KNOWS"),
                &[("since".into(), Value::Int(2024))],
            );

            shard.flush().unwrap();
        }

        // Re-open and verify data survives
        {
            let shard = Shard::open(
                0,
                &ShardConfig {
                    data_dir: dir.path().to_owned(),
                    pool_capacity: 64,
                },
            )
            .unwrap();

            let nodes = shard.scan_nodes(Some("Person"));
            assert_eq!(nodes.len(), 2);

            assert_eq!(
                shard.node_property(id, "name"),
                Value::String("Alice".into())
            );
            assert_eq!(shard.node_property(id, "age"), Value::Int(30));

            let out = shard.outbound_edges(id, Some("KNOWS"));
            assert_eq!(out.len(), 1);
            assert_eq!(shard.edge_property(edge_id, "since"), Value::Int(2024));
        }
    }

    #[test]
    fn shard_wal_replay() {
        let dir = tempfile::tempdir().unwrap();

        // Create data but DON'T flush — only WAL protects it
        let id;
        {
            let mut shard = Shard::open(
                0,
                &ShardConfig {
                    data_dir: dir.path().to_owned(),
                    pool_capacity: 64,
                },
            )
            .unwrap();

            id = shard.create_node(Some("X"), &[("v".into(), Value::Int(42))]);

            // Flush WAL only (not dirty pages) — simulates crash after WAL write
            if let Some(ref wal) = shard.wal {
                if let Some(ref io) = shard.io {
                    wal.borrow_mut().flush(&mut **io.borrow_mut()).unwrap();
                }
            }
            // Do NOT call shard.flush() — dirty pages stay in memory only
        }

        // Re-open — WAL replay should recover the data
        {
            let shard = Shard::open(
                0,
                &ShardConfig {
                    data_dir: dir.path().to_owned(),
                    pool_capacity: 64,
                },
            )
            .unwrap();

            let nodes = shard.scan_nodes(Some("X"));
            assert_eq!(nodes.len(), 1);
            assert_eq!(shard.node_property(id, "v"), Value::Int(42));
        }
    }

    #[test]
    fn shard_property_pages() {
        // Verify property page system works identically to old HashMap approach
        let mut shard = Shard::new(0);
        let a = shard.create_node(
            None,
            &[
                ("name".into(), Value::String("test".into())),
                ("x".into(), Value::Int(1)),
                ("f".into(), Value::Float(2.5)),
                ("b".into(), Value::Bool(true)),
            ],
        );
        assert_eq!(shard.node_property(a, "name"), Value::String("test".into()));
        assert_eq!(shard.node_property(a, "x"), Value::Int(1));
        assert_eq!(shard.node_property(a, "f"), Value::Float(2.5));
        assert_eq!(shard.node_property(a, "b"), Value::Bool(true));
        assert_eq!(shard.node_property(a, "nope"), Value::Null);

        // Update
        shard.set_node_property(a, "x", &Value::Int(99));
        assert_eq!(shard.node_property(a, "x"), Value::Int(99));
    }

    #[test]
    fn shard_eviction_writes_to_disk() {
        // Use a tiny pool (4 pages) so that creating enough nodes forces eviction.
        // After re-opening, all data should be recoverable from data files + WAL.
        let dir = tempfile::tempdir().unwrap();

        let node_count = RECORDS_PER_PAGE * 6; // 6 pages worth, but pool only holds 4
        let mut ids = Vec::new();

        {
            let mut shard = Shard::open(
                0,
                &ShardConfig {
                    data_dir: dir.path().to_owned(),
                    pool_capacity: 4,
                },
            )
            .unwrap();

            for i in 0..node_count {
                let id = shard.create_node(None, &[("idx".into(), Value::Int(i as i64))]);
                ids.push(id);
            }

            shard.flush().unwrap();
        }

        // Re-open with the same tiny pool — recovery must page-in from disk
        {
            let shard = Shard::open(
                0,
                &ShardConfig {
                    data_dir: dir.path().to_owned(),
                    pool_capacity: 4,
                },
            )
            .unwrap();

            // Verify every node is readable (will require page faults from disk)
            for (i, &id) in ids.iter().enumerate() {
                let v = shard.node_property(id, "idx");
                assert_eq!(v, Value::Int(i as i64), "node {i} property mismatch");
            }

            let all_nodes = shard.scan_nodes(None);
            assert_eq!(all_nodes.len(), node_count);
        }
    }

    // -- MVCC tests -----------------------------------------------------------

    #[test]
    fn mvcc_own_writes_visible() {
        // Within a tx, created records are immediately readable
        let mut shard = Shard::new(0);
        shard.begin_tx();
        let id = shard.create_node(Some("X"), &[("v".into(), Value::Int(1))]);
        // Should be visible within the same tx
        assert!(shard.get_node(id).is_some());
        assert_eq!(shard.scan_nodes(Some("X")).len(), 1);
        assert_eq!(shard.node_property(id, "v"), Value::Int(1));
        shard.commit_tx();
    }

    #[test]
    fn mvcc_abort_hides_writes() {
        // After abort, records created in that tx are invisible
        let mut shard = Shard::new(0);

        // Create a node in tx 1, commit it
        shard.begin_tx();
        shard.create_node(Some("A"), &[]);
        shard.commit_tx();

        // Create another node in tx 2, abort it
        shard.begin_tx();
        let aborted_id = shard.create_node(Some("B"), &[]);
        shard.abort_tx();

        // After abort, only the committed node should be visible
        shard.begin_tx();
        assert_eq!(shard.scan_nodes(None).len(), 1);
        assert_eq!(shard.scan_nodes(Some("A")).len(), 1);
        assert!(shard.get_node(aborted_id).is_none());
        shard.commit_tx();
    }

    #[test]
    fn mvcc_delete_visibility() {
        let mut shard = Shard::new(0);

        // Create and commit a node
        shard.begin_tx();
        let id = shard.create_node(Some("X"), &[]);
        shard.commit_tx();

        // Delete in a new tx, then commit
        shard.begin_tx();
        shard.delete_node(id);
        shard.commit_tx();

        // After commit, deleted node should be invisible
        shard.begin_tx();
        assert!(shard.get_node(id).is_none());
        assert_eq!(shard.scan_nodes(None).len(), 0);
        shard.commit_tx();
    }

    #[test]
    fn mvcc_edge_chain_with_invisible() {
        // Invisible edge mid-chain doesn't break traversal of subsequent edges
        let mut shard = Shard::new(0);

        // Create nodes and edges
        shard.begin_tx();
        let a = shard.create_node(Some("N"), &[]);
        let b = shard.create_node(Some("N"), &[]);
        let c = shard.create_node(Some("N"), &[]);
        let e1 = shard.create_edge(a, b, Some("E"), &[]);
        shard.create_edge(a, c, Some("E"), &[]);
        shard.commit_tx();

        // Delete e1 (the second edge in the chain, since e2 was prepended)
        shard.begin_tx();
        shard.delete_edge(e1);
        shard.commit_tx();

        // Traversal should still find the remaining edge
        shard.begin_tx();
        let out = shard.outbound_edges(a, None);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].target, c);
        shard.commit_tx();
    }

    #[test]
    fn mvcc_visibility_basic() {
        // Records created in tx N are visible to tx N+1
        let mut shard = Shard::new(0);

        shard.begin_tx();
        let id = shard.create_node(Some("X"), &[("v".into(), Value::Int(42))]);
        shard.commit_tx();

        shard.begin_tx();
        assert!(shard.get_node(id).is_some());
        assert_eq!(shard.node_property(id, "v"), Value::Int(42));
        shard.commit_tx();
    }

    #[test]
    fn wal_recovery_replays_committed() {
        let dir = tempfile::tempdir().unwrap();

        // Create data in committed transactions
        let id;
        {
            let mut shard = Shard::open(
                0,
                &ShardConfig {
                    data_dir: dir.path().to_owned(),
                    pool_capacity: 64,
                },
            )
            .unwrap();

            shard.begin_tx();
            id = shard.create_node(Some("X"), &[("v".into(), Value::Int(42))]);
            shard.commit_tx();

            // Flush WAL only, not dirty pages
            if let Some(ref wal) = shard.wal {
                if let Some(ref io) = shard.io {
                    wal.borrow_mut().flush(&mut **io.borrow_mut()).unwrap();
                }
            }
        }

        // Re-open — committed tx should be replayed
        {
            let mut shard = Shard::open(
                0,
                &ShardConfig {
                    data_dir: dir.path().to_owned(),
                    pool_capacity: 64,
                },
            )
            .unwrap();

            shard.begin_tx();
            let nodes = shard.scan_nodes(Some("X"));
            assert_eq!(nodes.len(), 1);
            assert_eq!(shard.node_property(id, "v"), Value::Int(42));
            shard.commit_tx();
        }
    }

    #[test]
    fn wal_recovery_ignores_uncommitted() {
        let dir = tempfile::tempdir().unwrap();

        // Create data in an uncommitted transaction (begin but no commit)
        {
            let mut shard = Shard::open(
                0,
                &ShardConfig {
                    data_dir: dir.path().to_owned(),
                    pool_capacity: 64,
                },
            )
            .unwrap();

            shard.begin_tx();
            shard.create_node(Some("X"), &[("v".into(), Value::Int(42))]);
            // Do NOT commit

            // Flush WAL so records are on disk
            if let Some(ref wal) = shard.wal {
                if let Some(ref io) = shard.io {
                    wal.borrow_mut().flush(&mut **io.borrow_mut()).unwrap();
                }
            }
        }

        // Re-open — uncommitted tx should NOT be replayed
        {
            let mut shard = Shard::open(
                0,
                &ShardConfig {
                    data_dir: dir.path().to_owned(),
                    pool_capacity: 64,
                },
            )
            .unwrap();

            shard.begin_tx();
            let nodes = shard.scan_nodes(Some("X"));
            assert_eq!(
                nodes.len(),
                0,
                "uncommitted writes should not survive recovery"
            );
            shard.commit_tx();
        }
    }
}
