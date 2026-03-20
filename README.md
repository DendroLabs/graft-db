# graft

A high-performance, open-source graph database written in Rust.

graft uses a hardware-aware, OS-integrated architecture — shard-per-core concurrency, index-free adjacency, custom memory allocators, and kernel-bypass I/O — to deliver deterministic latency at scale.

## Status

**Early development.** All 10 implementation steps complete: core types, storage engine, I/O layer (posix + io_uring + simulation), custom allocators, transactions (MVCC/WAL), query engine (GQL parser, planner, executor with variable-length paths, aggregations, DISTINCT), shard-per-core runtime, TCP server, client library, interactive CLI, and Criterion benchmarks.

## Quick start

```bash
# Build
cargo build

# Run tests
cargo test

# Lint
cargo clippy -- -D warnings

# Run benchmarks
cargo bench

# Start the server
cargo run --bin graft-server

# Connect with the CLI (in another terminal)
cargo run --bin graft-cli
```

Example session:
```
graft> CREATE (a:Person {name: 'Alice', age: 30});
OK (0 rows affected, 0 ms)

graft> CREATE (b:Person {name: 'Bob', age: 25});
OK (0 rows affected, 0 ms)

graft> MATCH (p:Person) RETURN p.name, p.age ORDER BY p.name;
+-------+------+
| p.name| p.age|
+-------+------+
| Alice | 30   |
| Bob   | 25   |
+-------+------+
2 row(s) (0 ms)
```

## Project layout

```
crates/
  graft-core/      Core types: NodeId, EdgeId, LabelId, constants, errors
  graft-storage/   Storage engine: 8 KB pages, 64-byte records, buffer pool
  graft-alloc/     Custom allocators: Slab<T>, Arena
  graft-io/        I/O backends: PosixIo (macOS/Linux), IoUring (Linux), SimIo (testing)
  graft-txn/       Transactions: MVCC, WAL, snapshot isolation
  graft-query/     Query engine: LALRPOP GQL parser, planner, executor
  graft-runtime/   Runtime: Database, Shard, LabelDictionary, StorageAccess impl
  graft-server/    TCP server with binary wire protocol (port 7687)
  graft-client/    Synchronous Rust client library
  graft-cli/       Interactive REPL with table-formatted output
  graft-sim/       Simulation testing framework (planned)
docs/
  architecture.md  Technical architecture reference
  storage.md       Storage engine internals
  transactions.md  Transaction model and WAL
research/          Background research and competitive analysis
```

## Key design choices

- **Shard-per-core** — one OS thread per CPU core, each owning its own data partition. No locks in the hot path.
- **Index-free adjacency** — nodes carry linked-list pointers to their edges. O(1) per traversal hop regardless of graph size.
- **GQL query language** — ISO/IEC 39075:2024, the first ISO database language since SQL.
- **Simulation-testable** — all non-determinism (I/O, time, RNG) behind injectable interfaces from day one.
- **AGPL v3** — one codebase, one license, every feature. Companies that modify and deploy must share their changes.

## Documentation

- [Architecture overview](docs/architecture.md)
- [Storage engine](docs/storage.md)
- [Transaction model](docs/transactions.md)

## License

GNU Affero General Public License v3.0 — see [LICENSE](LICENSE) for details.
