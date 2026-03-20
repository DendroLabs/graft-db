# Architecture Overview

This document describes graft's technical architecture — the data model, concurrency model, crate structure, and the design principles that tie them together.

## Design Philosophy

graft is built on three core beliefs:

1. **The database should own the hardware.** Modern databases delegate critical work to the OS (page cache, thread scheduling, I/O scheduling) and then fight the OS for control. graft manages its own memory, pins its own threads, and will eventually bypass the kernel for I/O.

2. **Graph traversal should be O(1) per hop.** Most graph databases store adjacency in indexed structures that degrade as the graph grows. graft uses index-free adjacency: each node carries direct pointers to its edges, making every hop a fixed-cost pointer chase regardless of total graph size.

3. **Non-determinism kills reliability.** Every source of non-determinism (I/O, time, entropy) is behind an injectable interface. The same binary that runs in production can run under a deterministic simulation that can reproduce any bug.

## Data Model

graft is a labeled property graph:

- **Nodes** have an ID, a label, and properties
- **Edges** have an ID, a label, source/target nodes, and properties
- **Labels** are compact integer IDs mapped from strings via a dictionary
- **Properties** are key-value pairs (Null, Bool, Int, Float, String, Bytes)

### Identity

All IDs embed their shard routing information:

```
NodeId / EdgeId: 64 bits
┌──────────┬─────────────────────────────┐
│ shard(8) │       local_id(56)          │
└──────────┴─────────────────────────────┘
```

- **Upper 8 bits**: shard ID (0–255), identifying the owning CPU core
- **Lower 56 bits**: local identifier within the shard
- **NULL sentinel**: 0 (shard 0, local 0)

Any component can route a request to the correct shard by inspecting the upper bits — no routing table needed. Edges are owned by the source node's shard.

## Crate Structure

```
crates/
├── graft-core/         Core types, constants, error definitions
├── graft-storage/      Pages, records, buffer pool
├── graft-alloc/        Slab and arena allocators
├── graft-io/           I/O backend trait + implementations
├── graft-txn/          MVCC transactions, WAL, snapshots
├── graft-query/        GQL parser, planner, executor
├── graft-runtime/      Database, Shard, LabelDictionary, StorageAccess impl
├── graft-server/       TCP server with binary wire protocol
├── graft-client/       Synchronous Rust client library
├── graft-cli/          Interactive REPL with table output
└── graft-sim/          Simulation testing framework (planned)
```

### Dependency Flow

```
graft-core          (no internal deps — types, constants, errors)
    ↑
graft-io            (depends on graft-core for PAGE_SIZE)
    ↑
graft-storage       (depends on graft-core for types)
    ↑
graft-alloc         (standalone — no graft dependencies)

graft-txn           (depends on graft-core, graft-io)
    ↑
graft-query         (depends on graft-core)
    ↑
graft-runtime       (depends on graft-core, graft-query)
    ↑
graft-server        (depends on graft-core, graft-runtime, graft-query)

graft-client        (depends on graft-core)
    ↑
graft-cli           (depends on graft-core, graft-client)
```

## Runtime

The runtime provides the execution environment for queries against sharded storage.

### Shard

A `Shard` is the fundamental unit of data ownership. Each shard:

- Implements `StorageAccess` for the query executor
- Owns three buffer pools (node, edge, property pages) and a label dictionary
- Maintains `HashMap` indexes for O(1) node/edge lookup by ID
- Uses **index-free adjacency**: each node carries `first_out_edge`/`first_in_edge` linked-list heads; each edge carries `next_out_edge`/`next_in_edge` pointers
- Tracks its own `TransactionManager` for MVCC (auto-commit, `is_record_visible()`)
- Supports both ephemeral (in-memory) and durable (file-backed with WAL) modes
- Accepts any `Box<dyn IoBackend + Send>` via `Shard::open_with_io()`

### Database (test path)

`Database` wraps multiple `Shard` instances in a single-threaded multi-shard setup:

- Round-robin node creation across shards
- NodeId/EdgeId shard routing via upper 8 bits
- Cross-shard scan fan-out
- `Database::query(gql)` — parses, plans, executes with auto-commit

### ShardCluster (production path)

`ShardCluster` is the thread-per-shard production executor:

- Spawns N OS threads, each owning a `Shard`
- Coordinator routes queries via SPSC lock-free ring buffers
- Fan-out for scans, targeted routing for point lookups
- Global tx_id via `AtomicU64`, routes `BeginTx/CommitTx/AbortTx` to all shard workers
- `--shards` flag on server (defaults to CPU count)

### Event Loop (planned)

Each shard will run a cooperative event loop:

```
loop {
    poll_io()          // check for completed I/O
    poll_messages()    // drain inbound ring buffers from other shards
    advance_queries()  // push-based query execution
    submit_io()        // batch new I/O requests
}
```

No `tokio`, no work stealing — each core does exactly the work that belongs to it.

## Storage Architecture

See [storage.md](storage.md) for full details. The key points:

- **8 KB pages** aligned to NVMe page size
- **64-byte records** (one cache line) for both nodes and edges
- **127 records per page** (8160 usable bytes / 64 bytes per record)
- **Free-list slot management** within each page (singly-linked list through unused slots)
- **Buffer pool** with CLOCK eviction and index-based API for multi-page access
- **CRC32C checksums** on every page, validated on read, updated before eviction

### Index-Free Adjacency

Each node record carries two edge-list heads:

```
NodeRecord:
  first_out_edge → EdgeRecord → EdgeRecord → ... (outbound linked list)
  first_in_edge  → EdgeRecord → EdgeRecord → ... (inbound linked list)
```

Each edge record carries next-pointers for both lists:

```
EdgeRecord:
  next_out_edge → next outbound edge from the same source
  next_in_edge  → next inbound edge to the same target
```

Traversing neighbors = following a linked list. No index lookup, no B-tree scan, no hash probe. The cost is constant per hop regardless of graph size.

## Transaction Model

See [transactions.md](transactions.md) for full details. Summary:

- **MVCC** with snapshot isolation
- **Write-ahead log** (WAL) with 64 KB buffering and group commit
- **First-committer-wins** conflict detection on `(page_id, slot)` pairs
- **Low-water mark** pruning of old transaction metadata
- Snapshots capture the active transaction set at begin time; visibility checks are O(active_count)

## I/O Layer

All I/O goes through the `IoBackend: Send` trait. `default_backend()` auto-selects the best backend for the platform.

| Implementation | Purpose |
|---|---|
| `PosixIoBackend` | macOS/Linux development — uses `pread`/`pwrite` via `FileExt` |
| `SimIoBackend` | Deterministic testing — in-memory filesystem with crash/fault injection |
| `IoUringBackend` | Linux production — kernel-bypass I/O via `io_uring` with O_DIRECT |

The trait covers:
- File lifecycle: `open`, `close`, `sync`, `truncate`, `remove`
- Page-aligned I/O: `read_page`, `write_page` (8 KB aligned)
- Arbitrary byte I/O: `read_at`, `write_at` (for WAL)
- Clock: `now_millis` (monotonic)
- Entropy: `random_u64`

### IoUringBackend

- **O_DIRECT** for data files (`.dat`) — bypasses the kernel page cache since graft manages its own buffer pool
- **fdatasync** for direct files, full fsync for buffered files (WAL)
- **pread/pwrite** for WAL I/O — small sequential writes don't benefit from io_uring overhead
- **EINVAL fallback** — if O_DIRECT fails (e.g. tmpfs), transparently falls back to buffered I/O
- `.wal` files never use O_DIRECT (small sequential writes)
- Ring size: 256 entries, synchronous submit-and-wait per operation (Phase 1)

### Simulation Testing

`SimIoBackend` provides:
- **Deterministic replay**: seeded PRNG, controlled time
- **Crash simulation**: `crash()` reverts all files to last synced state, invalidates file handles
- **Fault injection**: `inject_fault(kind)` fails the next I/O operation then auto-clears (transient fault model)
- **State inspection**: `file_data()` and `durable_data()` for test assertions

## Custom Allocators

### Slab Allocator (`Slab<T>`)

O(1) fixed-size object pool with generational keys:
- `insert(value) → SlabKey` (O(1), reuses freed slots)
- `get(key) → Option<&T>` (O(1), returns `None` for stale keys)
- `remove(key) → Option<T>` (O(1), bumps generation counter)

Generational keys (index + generation) detect use-after-free: if a slot is recycled, old keys return `None` instead of silently aliasing the new occupant.

### Arena Allocator (`Arena`)

Bump allocator for query execution temporaries:
- `alloc<T: Copy>(&self) → &T` (O(1), interior mutability allows concurrent refs)
- `alloc_slice_copy(&self, &[T]) → &[T]`
- `reset(&mut self)` (O(1) bulk deallocation, borrow checker enforces no live refs)

Grows in 8 KB chunks. Only supports `Copy` types (no `Drop`). The `&self` / `&mut self` split means the borrow checker statically prevents use-after-reset.

## Wire Protocol

Custom binary protocol on port 7687. Defined in `graft-core/src/protocol.rs`, shared by server and client.

```
Header: 8 bytes
  magic:    [u8; 4]  = "GF01"
  version:  u8       = 1
  msg_type: u8
  length:   u16      (big-endian, max 65535 bytes)

Payload: MessagePack encoded
```

Message types: HELLO, QUERY, RESULT, ROW, SUMMARY, ERROR, BEGIN_TX, COMMIT_TX, ROLLBACK_TX.

### Connection Flow

```
Client                    Server
  |--- HELLO ------------>|
  |<--- HELLO ------------|
  |                        |
  |--- QUERY ------------>|
  |<--- RESULT -----------|  (column headers)
  |<--- ROW --------------|  (repeated per row)
  |<--- SUMMARY ----------|  (rows_affected, elapsed_ms)
  |                        |
  |--- QUERY ------------>|
  |<--- ERROR ------------|  (on parse/execution failure)
```

### Server

- `graft-server`: TCP server using `std::net`, one thread per connection
- Shared `ShardCluster` behind `Arc<Mutex<ShardCluster>>` — thread-per-shard execution
- Configurable `--host`, `--port`, `--shards`, `--data-dir` via clap
- Ephemeral mode (no `--data-dir`) or durable mode (with `--data-dir`)

### Client

- `graft-client`: synchronous `Client` struct with `connect(addr)` and `query(gql)` methods
- Returns `QueryResult { columns, rows, rows_affected, elapsed_ms }`

### CLI

- `graft-cli`: interactive REPL using `rustyline`
- Semicolon-terminated queries, multi-line input support
- `comfy-table` formatted output
- Commands: `\quit`, `\help`

## Query Engine

graft implements a subset of GQL (ISO/IEC 39075:2024), the first ISO graph query language.

### GQL Subset

- `MATCH` — pattern matching (node patterns, edge traversals, property filters)
- `CREATE` — node/edge creation
- `WHERE` — filtering with boolean expressions
- `RETURN` / `RETURN DISTINCT` — projection with aliases, deduplication
- `ORDER BY`, `LIMIT`, `SKIP` — result control
- `SET`, `DELETE` — mutation
- Aggregations: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- Variable-length paths: `*`, `*n`, `*n..m`, `*n..`, `*..m` quantifiers on edge patterns

### Pipeline

```
GQL text → Lexer → Tokens → LALRPOP Parser → AST → Planner → Plan → Executor → Results
```

- **Lexer** (hand-written): tokenizes GQL with case-insensitive keywords, string escapes, edge arrows (`->`, `<-`)
- **Parser** (LALRPOP, LR(1)): build-time codegen from a `.lalrpop` grammar file, produces a typed AST
- **Planner**: converts AST to a `Plan` tree, reordering clauses into execution order (MATCH → WHERE → ORDER BY → SKIP → LIMIT → RETURN). Pattern properties become filter predicates. Aggregate functions in RETURN produce an `Aggregate` plan node.
- **Executor**: evaluates the plan against a `StorageAccess` trait. Materializes intermediate results at each step. Supports full expression evaluation (arithmetic, comparison, boolean logic, property access, NULL propagation).

### StorageAccess Trait

The executor is decoupled from storage via the `StorageAccess` trait:

```rust
pub trait StorageAccess {
    fn scan_nodes(&self, label: Option<&str>) -> Vec<NodeInfo>;
    fn get_node(&self, id: NodeId) -> Option<NodeInfo>;
    fn node_property(&self, id: NodeId, key: &str) -> Value;
    fn outbound_edges(&self, node_id: NodeId, label: Option<&str>) -> Vec<EdgeInfo>;
    fn inbound_edges(&self, node_id: NodeId, label: Option<&str>) -> Vec<EdgeInfo>;
    fn edge_property(&self, id: EdgeId, key: &str) -> Value;
    // + mutation methods: create_node, create_edge, set_*_property, delete_*
}
```

This allows testing the query engine with an in-memory graph implementation, independent of the page-based storage engine.

## io_uring Backend

`IoUringBackend` (`graft-io/src/uring.rs`) implements `IoBackend` using Linux's `io_uring` for kernel-bypass I/O. Gated behind `#[cfg(target_os = "linux")]`.

### Current Implementation

- Synchronous submit-and-wait pattern: one SQE submitted, one CQE waited per operation
- **O_DIRECT** for data files — bypasses kernel page cache (graft manages its own buffer pool)
- `.wal` files use buffered I/O with pread/pwrite (small sequential writes)
- **fdatasync** for O_DIRECT files (skips metadata update), full fsync for buffered files
- EINVAL fallback — gracefully handles filesystems that don't support O_DIRECT (e.g. tmpfs)
- Ring size: 256 entries
- `Shard::open()` auto-selects io_uring on Linux via `default_backend()`

### Future: Batched Async

- Submit multiple SQEs per event loop iteration
- Completion-driven execution: `poll_io()` drains CQEs, wakes waiting queries

## Benchmarks

Criterion micro-benchmarks provide baselines for the storage and query layers.

### Storage (`graft-storage/benches/page_ops.rs`)

| Benchmark | Description |
|---|---|
| `page_alloc_slot` | Allocate 127 slots from a fresh page |
| `page_alloc_free_cycle` | Allocate 127 slots then free all |
| `node_record_write_read` | Serialize + write + read + deserialize a NodeRecord |
| `edge_record_write_read` | Same for EdgeRecord |
| `page_checksum` | CRC32C compute + verify on an 8 KB page |

### Query (`graft-runtime/benches/query.rs`)

All query benchmarks run against a 100-node chain graph (person_0 → person_1 → ... → person_99).

| Benchmark | Description |
|---|---|
| `scan_100_nodes` | `MATCH (p:Person) RETURN p.name` |
| `filter_100_nodes` | `MATCH (p:Person) WHERE p.age > 40 RETURN p.name` |
| `single_hop_traversal` | Single-edge pattern match |
| `two_hop_traversal` | Two-edge pattern match |
| `var_length_1_to_5` | Variable-length path `*1..5` |
| `count_100_nodes` | `COUNT(p)` aggregation |
| `avg_100_nodes` | `AVG(p.age)` aggregation |
| `order_by_100_nodes` | `ORDER BY p.age` |
| `create_node` | Single node creation throughput |
| `parse_plan_execute_simple` | End-to-end: parse → plan → execute a simple match |

Run benchmarks:
```bash
cargo bench --bench page_ops    # storage layer
cargo bench --bench query       # query layer
cargo bench                     # all
```
