# graft — High-Performance Open-Source Graph Database

## Project Overview

graft is an open-source graph database written in Rust, designed to be the world's best graph database. It uses a hardware-aware, OS-integrated architecture that no existing graph database employs.

## Key Differentiators

1. **Shard-per-core, shared-nothing concurrency** (Seastar/ScyllaDB model in Rust) — one OS thread per CPU core, each owning its own data partition. No locks, no shared mutable state in the hot path.
2. **Native graph storage with index-free adjacency** — 64-byte fixed-size node/edge records with direct pointers to neighbors. O(1) per traversal hop, independent of total graph size.
3. **GQL-native query language** (ISO/IEC 39075:2024) — the first ISO database language since SQL. Not Cypher, not Gremlin, not a proprietary language.
4. **Custom memory allocators** — slab allocators for topology (O(1), zero fragmentation), arena allocators for query execution (cache-friendly, O(1) bulk deallocation).
5. **Kernel-bypass I/O** — io_uring for storage, with path toward SPDK/DPDK for full OS bypass.
6. **AGPL v3 license, no feature gating** — one codebase, one license. No crippled community edition.

## Architecture

### Crate Structure
```
crates/
├── graft-core/         # Core types: NodeId, EdgeId, LabelId, constants, errors
├── graft-storage/      # Storage engine: 8KB pages, node/edge records, buffer pool
├── graft-alloc/        # Custom allocators: SlabAllocator<T>, Arena
├── graft-io/           # I/O abstraction: io_uring (Linux), posix (macOS), simulation
├── graft-txn/          # Transactions: MVCC, WAL, snapshot isolation, checkpoints
├── graft-query/        # Query engine: LALRPOP GQL parser, planner, push-based executor, scalar functions
├── graft-runtime/      # Shard-per-core runtime: ShardCluster (thread-per-shard), SPSC ring buffers, event loop, message passing
├── graft-server/       # TCP server: ShardCluster backend, binary wire protocol, --shards flag
├── graft-client/       # Sync Rust client library
├── graft-cli/          # Interactive REPL: rustyline + comfy-table, semicolon-terminated queries
└── graft-sim/          # Simulation testing: deterministic I/O, fault injection
```

### Storage Format
- **8KB pages** aligned to NVMe page size
- **Node records**: 64 bytes (one cache line) — node_id, label, first_out_edge, first_in_edge (linked list heads for index-free adjacency), inline properties, MVCC tx_min
- **Edge records**: 64 bytes — edge_id, source, target, next_out_edge, next_in_edge (doubly-linked for bidirectional traversal), inline properties
- **127 records per page** (8160 usable bytes / 64 bytes)
- **Properties**: inline-first (8-16 bytes on record), overflow to property pages for larger values
- **Buffer pool**: per-shard, CLOCK eviction, O_DIRECT

### Concurrency Model
- One OS thread pinned to one CPU core, owning one shard
- Node ID upper 8 bits = shard assignment (supports 256 cores)
- Edges owned by source node's shard
- Inter-shard communication via SPSC lock-free ring buffers (cache-line padded, power-of-two capacity)
- `ShardMessage` protocol for inter-shard request/response (ScanNodes, GetNode, GetNodeProperty, GetOutboundEdges, GetInboundEdges + their responses)
- Custom cooperative event loop (no tokio on server): `poll_io → poll_messages → advance_queries → submit_io`
- `build_shard_mesh()` creates a fully-interconnected topology of `ShardEventLoop` instances
- Multi-shard `Database` with `StorageAccess` routing: round-robin node creation, NodeId-based shard routing, cross-shard scan fan-out
- `ShardCluster`: production-path thread-per-shard executor — spawns N OS threads, each owning a `Shard`, coordinator routes queries via SPSC queues with fan-out for scans and targeted routing for point lookups
- Single shared atomic: global commit counter (AtomicU64)

### Transaction Model
- MVCC with per-shard `TransactionManager` tracking active/committed transactions
- Auto-commit: every query is implicitly wrapped in a transaction (begin before, commit/abort after)
- `Snapshot` visibility: checks `tx_min`/`tx_max` against snapshot timestamp and active set
- Own-writes visible within a transaction (special case in `is_record_visible`)
- `was_committed()` handles both current-session and post-recovery (pre-`base_tx_id`) visibility
- Per-shard WAL files with 64KB write buffer; WAL records stamped with `current_tx`
- Two-pass WAL recovery: pass 1 builds committed tx set, pass 2 replays only committed writes
- Snapshot isolation with first-committer-wins conflict detection
- Edge chain traversal reads raw records for next pointers, checks visibility separately (invisible edges skipped without breaking chain)
- `StorageAccess` trait has `begin_tx/commit_tx/abort_tx` (default no-ops for backward compat)
- `ShardCluster` generates global tx_id via `AtomicU64`, routes `BeginTx/CommitTx/AbortTx` to all shard workers
- Deferred: explicit `BEGIN`/`COMMIT`/`ROLLBACK` over wire protocol (requires per-connection tx state)

### Query Engine
- LALRPOP parser (LR(1), build-time codegen) with hand-written lexer
- Pipeline: GQL text → AST → Logical Plan → Physical Plan → Push-based execution
- Phase 1 GQL subset:
  - **Clauses**: MATCH, CREATE, WHERE, RETURN (with DISTINCT), ORDER BY, LIMIT, SKIP, SET, DELETE
  - **Expressions**: arithmetic (`+`, `-`, `*`, `/`, `%`), comparison (`=`, `<>`, `<`, `>`, `<=`, `>=`), logical (`AND`, `OR`, `NOT`)
  - **Null handling**: `IS NULL`, `IS NOT NULL`
  - **String predicates**: `CONTAINS`, `STARTS WITH`, `ENDS WITH`
  - **Aggregations**: `COUNT(*)`, `COUNT(expr)`, `SUM`, `AVG`, `MIN`, `MAX`
  - **Scalar functions**: `id()`, `type()`, `labels()`, `toString()`, `toInteger()`, `toFloat()`
  - **Variable-length paths**: `*`, `*N`, `*N..M`, `*N..`, `*..M` (BFS with cycle detection)

### I/O Layer
- `IoBackend: Send` trait with swappable implementations, `default_backend()` auto-selects best platform backend
- `IoUringBackend` (Linux production): O_DIRECT for data files (bypasses kernel page cache), fdatasync for direct files, pread/pwrite fallback for WAL (small sequential writes), EINVAL fallback for filesystems without O_DIRECT (e.g. tmpfs)
- `PosixIoBackend` (macOS dev), `SimIoBackend` (testing)
- `Shard::open_with_io()` accepts `Box<dyn IoBackend + Send>` for caller-provided backends
- All non-determinism behind injectable interfaces (I/O, time, RNG) for simulation testing

### Wire Protocol
- Custom binary: 8-byte header (magic 0xGF01, version, msg type, length) + MessagePack payload
- Port 7687
- Message types: HELLO, QUERY, RESULT, ROW, SUMMARY, ERROR, BEGIN_TX, COMMIT_TX, ROLLBACK_TX

## Build & Run

```bash
# Build everything
cargo build

# Run tests
cargo test

# Lint
cargo clippy -- -D warnings

# Format
cargo fmt

# Run server (once implemented)
cargo run --bin graft-server

# Run CLI (once implemented)
cargo run --bin graft-cli -- connect localhost:7687
```

## Implementation Order

1. ~~Skeleton + core types (graft-core)~~ — **done**: NodeId/EdgeId/LabelId with shard encoding, constants, errors, property types, wire protocol
2. ~~Storage engine (graft-storage)~~ — **done**: 8KB pages, node/edge records, buffer pool with CLOCK eviction
3. ~~I/O layer (graft-io)~~ — **done**: IoBackend trait (Send supertrait), SimIoBackend, PosixIoBackend, IoUringBackend (O_DIRECT, fdatasync), default_backend() auto-selection
4. ~~Custom allocators (graft-alloc)~~ — **done**: SlabAllocator<T>, Arena with chunk growth
5. ~~WAL + transactions (graft-txn)~~ — **done**: MVCC visibility, WAL with CRC, snapshot isolation, first-committer-wins
6. ~~Query engine (graft-query)~~ — **done**: LALRPOP parser, hand-written lexer, planner, executor with scalar functions, IS NULL, string predicates, variable-length paths, aggregations, DISTINCT
7. ~~Shard-per-core runtime (graft-runtime)~~ — **done**: multi-shard Database, ShardCluster (thread-per-shard with SPSC message passing), cooperative event loop skeleton, shard mesh builder. Queries execute across real OS threads with fan-out for scans and targeted routing for point lookups.
8. ~~Server + CLI~~ — **done**: graft-server uses ShardCluster (thread-per-shard, `--shards` flag, defaults to CPU count), graft-client is a sync TCP client, graft-cli is a rustyline REPL with comfy-table output. Wire protocol integration tests cover multi-shard queries.
9. ~~Persistence~~ — **done**: Shard::open() with page-based storage, WAL-logged mutations, dirty page flush, recovery via file scan + WAL replay, buffer pool eviction writes to disk
10. ~~MVCC transactions~~ — **done**: per-shard TransactionManager, auto-commit in executor, Snapshot visibility, own-writes, two-pass WAL recovery (committed-only replay), tx_min/tx_max stamping, edge chain MVCC traversal, 221 tests passing
11. ~~io_uring backend~~ — **done**: IoUringBackend with O_DIRECT for data files, fdatasync, EINVAL fallback, Shard generic over IoBackend via Box<dyn IoBackend + Send>, default_backend() auto-selects io_uring on Linux
12. **Next**: benchmarks (criterion), core pinning

## Key Dependencies

lalrpop, rmp-serde, ahash, hashbrown, crossbeam, io-uring, rustyline, comfy-table, crc32c, thiserror, tracing, proptest, criterion, clap

## Design Principles

- **No locks in the hot path** — shard-per-core eliminates contention
- **No GC** — Rust ownership + custom allocators give deterministic latency
- **Simulation-testable** — all non-determinism behind injectable interfaces from day one
- **Standards-first** — GQL, not a proprietary query language
- **Truly open source** — AGPL v3, every feature in one codebase
- **Reliability over convenience** — when choosing between implementation approaches, always choose the option that is more reliable under worst-case scenarios (crashes, corruption, partial writes), even if it requires more upfront work. No shortcuts that create separate failure modes or custom formats when a uniform mechanism exists.
- **Scale beyond competitors** — every design decision must consider what happens at 1B+ nodes and edges. If an approach requires all data of a certain type to fit in memory, or creates an O(n) bottleneck that competitors don't have, it's the wrong approach. Design for larger-than-competitor scale from the start.

## Research

Comprehensive research documents are in `/research/`:
- `01-architecture-landscape.md` — 11 competing GDB architectures analyzed
- `02-user-sentiment.md` — User pain points, wishlists, migration stories
- `03-ops-scalability-innovation.md` — Scale stories, emerging tech (CXL, FPGA, GraphRAG)
- `04-database-as-os.md` — OS-integration approach, kernel bypass, historical precedent
- `05-strategic-synthesis.md` — Strategic playbook, differentiators, blockers, wild ideas
