# graft

A high-performance, open-source graph database written in Rust.

graft uses a hardware-aware, OS-integrated architecture — shard-per-core concurrency, index-free adjacency, custom allocators, and kernel-bypass I/O — to deliver deterministic, low-latency graph operations at scale.

It speaks **GQL** (ISO/IEC 39075:2024), the first ISO-standard graph query language since SQL.

## Why graft?

Most graph databases treat the operating system as a black box: generic memory allocation, thread pools with lock contention, buffered I/O through the kernel page cache. graft takes the opposite approach — it co-designs with the hardware and OS to eliminate overhead at every layer.

- **Shard-per-core, shared-nothing concurrency.** One OS thread per CPU core, each owning its own data partition. No locks, no contention in the hot path.
- **Index-free adjacency.** Nodes carry direct pointers to their edges. Every traversal hop is O(1), regardless of total graph size.
- **64-byte cache-line-aligned records.** Node and edge records fit exactly in one CPU cache line. No partial loads, no false sharing.
- **Custom memory allocators.** Slab allocators for topology (O(1) alloc, zero fragmentation), arena allocators for query execution (O(1) bulk dealloc).
- **Kernel-bypass I/O.** io_uring on Linux, with a path toward SPDK/DPDK for full OS bypass.
- **Standards-first query language.** GQL (ISO/IEC 39075:2024) — not Cypher, not Gremlin, not a proprietary language.

## Performance

### In-process (single core, no network overhead)

Measured on AMD Ryzen 5 3600, 64 GB RAM, NVMe:

| Operation | Result |
|---|---|
| Point lookup (by node ID) | **49 ns** |
| Single traversal hop | **111 ns** (9M hops/sec/core) |
| Fan-out traversal (1000 edges) | **65 ns/edge** (15.4M edges/sec) |
| Node insert | **3.3M nodes/sec** |
| Edge insert | **2.8M edges/sec** |
| Label scan (10K nodes) | **16M nodes/sec** |
| End-to-end GQL `CREATE` | **2.2 us** |
| End-to-end GQL `COUNT(*)` (100 nodes) | **17 us** |

### Over TCP (localhost, 6 shards)

| Operation | Avg | p99 | Throughput |
|---|---|---|---|
| `CREATE` node | 29 us | 42 us | 34,721 qps |
| `COUNT(*)` | 173 us | 180 us | 5,794 qps |
| Point lookup | 1.24 ms | 1.27 ms | 806 qps |
| Single-hop traversal | 1.44 ms | 1.49 ms | 696 qps |

Run benchmarks yourself:

```bash
cargo bench --bench query
```

## Quick Start

```bash
# Build from source
git clone https://github.com/DendroLabs/graft-db.git
cd graft-db
cargo build --release

# Start the server (defaults to all CPU cores)
cargo run --release --bin graft-server

# Connect with the CLI
cargo run --release --bin graft-cli
```

```
graft> CREATE (a:Person {name: 'Alice', age: 30});
OK (0 rows affected, 0 ms)

graft> CREATE (b:Person {name: 'Bob', age: 25});
OK (0 rows affected, 0 ms)

graft> MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
       CREATE (a)-[:KNOWS {since: 2024}]->(b);
OK (0 rows affected, 0 ms)

graft> MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name;
+--------+--------+
| a.name | b.name |
+--------+--------+
| Alice  | Bob    |
+--------+--------+
1 row(s) (0 ms)
```

## GQL Query Language

graft implements a substantial subset of the GQL standard:

```sql
-- Create nodes and relationships
CREATE (:Person {name: 'Alice', age: 30})
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS]->(b)

-- Pattern matching with filters
MATCH (p:Person) WHERE p.age > 25 RETURN p.name ORDER BY p.age LIMIT 10

-- Multi-hop traversals
MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c)
RETURN c.name

-- Variable-length paths (BFS with cycle detection)
MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..5]->(b)
RETURN b.name

-- Aggregations
MATCH (p:Person) RETURN AVG(p.age), COUNT(*), MIN(p.age), MAX(p.age)

-- Updates and deletes
MATCH (p:Person {name: 'Alice'}) SET p.age = 31
MATCH (p:Person {name: 'Bob'}) DELETE p
```

**Supported clauses:** `MATCH`, `CREATE`, `WHERE`, `RETURN` (with `DISTINCT`), `ORDER BY`, `LIMIT`, `SKIP`, `SET`, `DELETE`

**Expressions:** arithmetic, comparison, logical (`AND`, `OR`, `NOT`), `IS NULL`, `IS NOT NULL`, `CONTAINS`, `STARTS WITH`, `ENDS WITH`

**Functions:** `COUNT(*)`, `COUNT(expr)`, `SUM`, `AVG`, `MIN`, `MAX`, `id()`, `type()`, `labels()`, `toString()`, `toInteger()`, `toFloat()`

See the full [User's Guide](docs/users-guide.md) for complete syntax reference.

## Architecture

```
crates/
  graft-core/      Core types, constants, wire protocol definitions
  graft-storage/   Page-based storage engine (8 KB pages, 64-byte records, buffer pool)
  graft-alloc/     Slab and arena allocators
  graft-io/        I/O backends (io_uring, posix, simulation)
  graft-txn/       MVCC transactions, WAL, snapshot isolation
  graft-query/     GQL parser (LALRPOP), query planner, push-based executor
  graft-runtime/   Shard-per-core runtime, thread-per-shard cluster, SPSC message passing
  graft-server/    TCP server with binary wire protocol
  graft-client/    Synchronous Rust client library
  graft-cli/       Interactive REPL with table-formatted output
  graft-sim/       Simulation testing framework
```

### Storage

8 KB pages aligned to NVMe page size. Each page holds 127 fixed-size 64-byte records. Node records store the node ID, label, MVCC timestamp, and linked-list heads pointing to first outbound and inbound edges. Edge records store source, target, edge type, and next-pointers for both the outbound and inbound linked lists — enabling bidirectional index-free adjacency traversal in O(1) per hop.

### Concurrency

Each CPU core runs one OS thread that owns one shard. Node IDs encode the owning shard in their upper bits. Edges are owned by the source node's shard. Cross-shard communication uses lock-free SPSC ring buffers with cache-line padding. The only shared mutable state is a single `AtomicU64` global commit counter.

### Transactions

MVCC with per-shard WAL files. Snapshot isolation with first-committer-wins conflict detection. Checkpoints every 5 minutes or 100 MB of WAL.

## Documentation

- [User's Guide](docs/users-guide.md) — full query reference, client library API, configuration
- [Architecture](docs/architecture.md) — technical deep dive
- [Storage Engine](docs/storage.md) — page layout, buffer pool, record format
- [Transaction Model](docs/transactions.md) — MVCC, WAL, isolation levels

## Building & Testing

```bash
cargo build               # debug build
cargo build --release      # optimized build
cargo test                 # run all tests (~200+)
cargo clippy -- -D warnings  # lint
cargo bench --bench query  # run benchmarks
```

## Status

graft is in active development. The core engine — storage, query execution, shard-per-core runtime, server, client, and CLI — is functional and tested. Current work is focused on wiring the page-based storage engine and WAL to the runtime for full durability.

Contributions, bug reports, and feedback are welcome.

## License

[GNU Affero General Public License v3.0](LICENSE) — one codebase, one license, every feature. No crippled community edition.
