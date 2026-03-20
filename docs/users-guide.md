# graft User's Guide

**graft** is a high-performance open-source graph database written in Rust. It uses a shard-per-core architecture with index-free adjacency for O(1) traversals, and speaks GQL (ISO/IEC 39075:2024) — the first ISO-standard graph query language.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Running the Server](#running-the-server)
- [Using the CLI](#using-the-cli)
- [GQL Query Language](#gql-query-language)
  - [CREATE — Creating Data](#create--creating-data)
  - [MATCH — Reading Data](#match--reading-data)
  - [WHERE — Filtering](#where--filtering)
  - [RETURN — Projecting Results](#return--projecting-results)
  - [ORDER BY, LIMIT, SKIP — Sorting and Pagination](#order-by-limit-skip--sorting-and-pagination)
  - [SET — Updating Properties](#set--updating-properties)
  - [DELETE — Removing Data](#delete--removing-data)
  - [Variable-Length Paths](#variable-length-paths)
- [Expressions and Operators](#expressions-and-operators)
- [Functions](#functions)
- [Data Types](#data-types)
- [Using the Rust Client Library](#using-the-rust-client-library)
- [Architecture Overview](#architecture-overview)
- [Configuration Reference](#configuration-reference)
- [Benchmarks](#benchmarks)

---

## Installation

### Prerequisites

- Rust 1.75+ (install via [rustup](https://rustup.rs/))
- A C compiler (`build-essential` on Linux, Xcode CLI tools on macOS)

### Build from Source

```bash
git clone https://github.com/DendroLabs/graft-db.git
cd graft-db
cargo build --release
```

The binaries will be at:
- `target/release/graft-server` — the database server
- `target/release/graft-cli` — the interactive REPL

### Verify Installation

```bash
cargo test
```

---

## Quick Start

**1. Start the server:**

```bash
./target/release/graft-server
```

```
graft listening on 127.0.0.1:7687 (12 shards)
```

**2. Connect with the CLI:**

```bash
./target/release/graft-cli
```

```
connected to 127.0.0.1:7687
type \quit to exit, \help for help
```

**3. Create some data and query it:**

```sql
graft> CREATE (:Person {name: 'Alice', age: 30});
OK (0 rows affected, 0 ms)

graft> CREATE (:Person {name: 'Bob', age: 25});
OK (0 rows affected, 0 ms)

graft> MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
   ... CREATE (a)-[:KNOWS {since: 2020}]->(b);
OK (0 rows affected, 0 ms)

graft> MATCH (a:Person)-[e:KNOWS]->(b:Person) RETURN a.name, b.name, e.since;
+--------+--------+---------+
| a.name | b.name | e.since |
+--------+--------+---------+
| Alice  | Bob    | 2020    |
+--------+--------+---------+
1 row(s) (0 ms)
```

---

## Running the Server

```
graft-server [OPTIONS]
```

### Options

| Flag | Default | Description |
|---|---|---|
| `--host <HOST>` | `127.0.0.1` | Address to bind to |
| `--port <PORT>` | `7687` | Port to listen on |
| `--shards <N>` | number of CPU cores | Number of shard threads |

### Examples

```bash
# Listen on all interfaces, port 7687, auto-detect cores
graft-server --host 0.0.0.0

# Specific port and shard count
graft-server --port 9000 --shards 4

# Single-shard mode (useful for development)
graft-server --shards 1
```

### How Shards Work

graft uses a **shard-per-core** architecture. Each shard runs on its own OS thread and owns a partition of the data. Nodes are distributed across shards using round-robin assignment. Edges are stored on the source node's shard.

Queries that scan all nodes (e.g., `MATCH (n:Person)`) fan out to all shards and merge results. Point lookups (by node ID) route directly to the owning shard.

More shards = more parallelism for concurrent queries, but adds inter-shard communication overhead for fan-out operations. For most workloads, defaulting to the number of CPU cores is optimal.

---

## Using the CLI

```
graft-cli [ADDRESS]
```

Connect to a graft server. Default address is `127.0.0.1:7687`.

```bash
# Connect to local server
graft-cli

# Connect to remote server
graft-cli 192.168.1.100:7687
```

### REPL Commands

| Command | Description |
|---|---|
| `\quit`, `\q`, `\exit` | Exit the REPL |
| `\help`, `\h`, `\?` | Show help |

### Query Input

- Queries must be terminated with a **semicolon** (`;`)
- Multi-line input is supported — press Enter to continue on the next line
- The prompt changes from `graft>` to `   ...` when in multi-line mode
- All GQL keywords are case-insensitive (`MATCH`, `match`, `Match` all work)

```
graft> MATCH (p:Person)
   ... WHERE p.age > 25
   ... RETURN p.name, p.age
   ... ORDER BY p.age DESC;
```

---

## GQL Query Language

graft implements a subset of GQL (ISO/IEC 39075:2024). The supported clauses are:

| Clause | Purpose |
|---|---|
| `MATCH` | Find patterns in the graph |
| `CREATE` | Create nodes and edges |
| `WHERE` | Filter matched results |
| `RETURN` | Project columns from results |
| `ORDER BY` | Sort results |
| `LIMIT` | Limit number of results |
| `SKIP` | Skip leading results |
| `SET` | Update properties |
| `DELETE` | Remove nodes or edges |

### CREATE — Creating Data

#### Create a Node

```sql
CREATE (:Person {name: 'Alice', age: 30});
```

- `Person` is the **label** (optional)
- `{name: 'Alice', age: 30}` are **properties** (optional)
- A node can have zero or one label

```sql
-- Node with no label or properties
CREATE ();

-- Node with label only
CREATE (:City);

-- Node with properties only
CREATE ({temperature: 72.5});
```

#### Create an Edge

Edges are created between existing nodes. First MATCH the endpoints, then CREATE the edge:

```sql
-- Create an edge between two matched nodes
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS {since: 2020}]->(b);
```

- `KNOWS` is the edge **label** (optional)
- `->` indicates direction (from `a` to `b`)
- `{since: 2020}` are edge **properties** (optional)

#### Create Multiple Patterns

```sql
-- Create two nodes and an edge in one CREATE
CREATE (a:Person {name: 'Charlie'})-[:WORKS_AT]->(c:Company {name: 'Acme'});
```

### MATCH — Reading Data

#### Match All Nodes with a Label

```sql
MATCH (p:Person)
RETURN p.name, p.age;
```

#### Match by Property

```sql
MATCH (p:Person {name: 'Alice'})
RETURN p.age;
```

Property patterns in MATCH are shorthand for equality filters. The above is equivalent to:

```sql
MATCH (p:Person)
WHERE p.name = 'Alice'
RETURN p.age;
```

#### Match Edges (Traversal)

```sql
-- Outbound edges
MATCH (a:Person)-[:KNOWS]->(b:Person)
RETURN a.name, b.name;

-- Inbound edges
MATCH (a:Person)<-[:KNOWS]-(b:Person)
RETURN a.name, b.name;

-- Edge with variable (to access edge properties)
MATCH (a:Person)-[e:KNOWS]->(b:Person)
RETURN a.name, b.name, e.since;

-- Any edge label
MATCH (a:Person)-[e]->(b:Person)
RETURN a.name, type(e), b.name;
```

#### Multi-Hop Traversal

```sql
-- Two hops: Alice -> ? -> ?
MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
RETURN c.name;
```

#### Multiple MATCH Patterns

```sql
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
RETURN a.age, b.age;
```

### WHERE — Filtering

The WHERE clause filters results using boolean expressions.

```sql
MATCH (p:Person)
WHERE p.age > 25
RETURN p.name;
```

#### Comparison Operators

| Operator | Meaning |
|---|---|
| `=` | Equal |
| `<>` | Not equal |
| `<` | Less than |
| `>` | Greater than |
| `<=` | Less than or equal |
| `>=` | Greater than or equal |

#### Logical Operators

```sql
-- AND
MATCH (p:Person)
WHERE p.age > 20 AND p.age < 40
RETURN p.name;

-- OR
MATCH (p:Person)
WHERE p.name = 'Alice' OR p.name = 'Bob'
RETURN p.name;

-- NOT
MATCH (p:Person)
WHERE NOT p.age > 30
RETURN p.name;
```

#### Null Checks

```sql
-- Find nodes missing an email property
MATCH (p:Person)
WHERE p.email IS NULL
RETURN p.name;

-- Find nodes that have an email
MATCH (p:Person)
WHERE p.email IS NOT NULL
RETURN p.name;
```

#### String Predicates

```sql
-- Contains substring
MATCH (p:Person)
WHERE p.name CONTAINS 'lic'
RETURN p.name;

-- Starts with prefix
MATCH (p:Person)
WHERE p.name STARTS WITH 'Al'
RETURN p.name;

-- Ends with suffix
MATCH (p:Person)
WHERE p.name ENDS WITH 'ce'
RETURN p.name;
```

### RETURN — Projecting Results

```sql
-- Return specific properties
MATCH (p:Person) RETURN p.name, p.age;

-- Return with alias
MATCH (p:Person) RETURN p.name AS person_name;

-- Return expressions
MATCH (p:Person) RETURN p.name, p.age + 1;

-- Return DISTINCT values (deduplicate)
MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c)
RETURN DISTINCT c.name;
```

#### Aggregation Functions

Aggregation functions collapse multiple rows into a single result.

```sql
-- Count all matched nodes
MATCH (p:Person) RETURN COUNT(*);

-- Count non-null values
MATCH (p:Person) RETURN COUNT(p.email);

-- Sum
MATCH (p:Person) RETURN SUM(p.age);

-- Average
MATCH (p:Person) RETURN AVG(p.age);

-- Minimum and Maximum
MATCH (p:Person) RETURN MIN(p.age), MAX(p.age);
```

| Function | Description |
|---|---|
| `COUNT(*)` | Count all rows |
| `COUNT(expr)` | Count non-null values |
| `SUM(expr)` | Sum of numeric values (returns float) |
| `AVG(expr)` | Average of numeric values (returns float) |
| `MIN(expr)` | Minimum numeric value (returns float) |
| `MAX(expr)` | Maximum numeric value (returns float) |

### ORDER BY, LIMIT, SKIP — Sorting and Pagination

```sql
-- Sort ascending (default)
MATCH (p:Person) RETURN p.name ORDER BY p.age;

-- Sort descending
MATCH (p:Person) RETURN p.name ORDER BY p.age DESC;

-- Multiple sort keys
MATCH (p:Person) RETURN p.name ORDER BY p.age DESC, p.name ASC;

-- Limit results
MATCH (p:Person) RETURN p.name LIMIT 10;

-- Skip results (offset)
MATCH (p:Person) RETURN p.name SKIP 5;

-- Pagination: skip 20, take 10
MATCH (p:Person) RETURN p.name ORDER BY p.name SKIP 20 LIMIT 10;
```

### SET — Updating Properties

```sql
-- Update a property
MATCH (p:Person {name: 'Alice'})
SET p.age = 31;

-- Set multiple properties
MATCH (p:Person {name: 'Alice'})
SET p.age = 31, p.city = 'NYC';

-- Set edge properties
MATCH (a:Person)-[e:KNOWS]->(b:Person)
SET e.strength = 0.9;
```

### DELETE — Removing Data

```sql
-- Delete a node
MATCH (p:Person {name: 'Alice'})
DELETE p;

-- Delete an edge
MATCH (a:Person)-[e:KNOWS]->(b:Person {name: 'Bob'})
DELETE e;

-- Delete multiple
MATCH (p:Person)
WHERE p.age < 18
DELETE p;
```

> **Note:** Deleting a node does not automatically delete its edges. Delete edges first if needed.

### Variable-Length Paths

Variable-length paths allow traversal of edges with a flexible number of hops.

```sql
-- Any number of hops (1 or more)
MATCH (a:Person {name: 'Alice'})-[:KNOWS*]->(b:Person)
RETURN b.name;

-- Exactly N hops
MATCH (a:Person)-[:KNOWS*3]->(b:Person)
RETURN b.name;

-- Between N and M hops
MATCH (a:Person)-[:KNOWS*2..5]->(b:Person)
RETURN b.name;

-- N or more hops (no upper bound)
MATCH (a:Person)-[:KNOWS*2..]->(b:Person)
RETURN b.name;

-- Up to M hops
MATCH (a:Person)-[:KNOWS*..3]->(b:Person)
RETURN b.name;
```

| Syntax | Meaning |
|---|---|
| `*` | 1 or more hops (unbounded) |
| `*N` | Exactly N hops |
| `*N..M` | Between N and M hops (inclusive) |
| `*N..` | N or more hops |
| `*..M` | 1 to M hops |

Variable-length paths use BFS with cycle detection to avoid infinite loops in cyclic graphs.

---

## Expressions and Operators

Expressions can be used in `WHERE`, `RETURN`, `SET`, and `ORDER BY` clauses.

### Arithmetic

| Operator | Example | Description |
|---|---|---|
| `+` | `p.age + 1` | Addition |
| `-` | `p.age - 1` | Subtraction |
| `*` | `p.salary * 1.1` | Multiplication |
| `/` | `p.total / p.count` | Division |
| `%` | `p.value % 2` | Modulo |
| `-` (unary) | `-p.value` | Negation |

### Comparison

| Operator | Example |
|---|---|
| `=` | `p.name = 'Alice'` |
| `<>` | `p.name <> 'Bob'` |
| `<` | `p.age < 30` |
| `>` | `p.age > 20` |
| `<=` | `p.age <= 40` |
| `>=` | `p.age >= 18` |

### Logical

| Operator | Example |
|---|---|
| `AND` | `p.age > 20 AND p.age < 30` |
| `OR` | `p.x = 1 OR p.y = 2` |
| `NOT` | `NOT p.active` |

### Null and String

| Operator | Example |
|---|---|
| `IS NULL` | `p.email IS NULL` |
| `IS NOT NULL` | `p.email IS NOT NULL` |
| `CONTAINS` | `p.name CONTAINS 'li'` |
| `STARTS WITH` | `p.name STARTS WITH 'Al'` |
| `ENDS WITH` | `p.name ENDS WITH 'ce'` |

### Operator Precedence (highest to lowest)

1. Unary: `-`, `NOT`
2. Multiplicative: `*`, `/`, `%`
3. Additive: `+`, `-`
4. Comparison: `=`, `<>`, `<`, `>`, `<=`, `>=`
5. String predicates: `CONTAINS`, `STARTS WITH`, `ENDS WITH`
6. Null checks: `IS NULL`, `IS NOT NULL`
7. Logical: `AND`
8. Logical: `OR`

---

## Functions

### Scalar Functions

| Function | Description | Example |
|---|---|---|
| `id(node_or_edge)` | Returns the internal 64-bit ID | `RETURN id(p)` |
| `labels(node)` | Returns the node's label | `RETURN labels(p)` |
| `type(edge)` | Returns the edge's label | `RETURN type(e)` |
| `toString(value)` | Converts to string | `RETURN toString(42)` → `'42'` |
| `toInteger(value)` | Converts to integer | `RETURN toInteger('123')` → `123` |
| `toFloat(value)` | Converts to float | `RETURN toFloat('3.14')` → `3.14` |

### Aggregation Functions

| Function | Description | Example |
|---|---|---|
| `COUNT(*)` | Count all rows | `RETURN COUNT(*)` |
| `COUNT(expr)` | Count non-null values | `RETURN COUNT(p.email)` |
| `SUM(expr)` | Sum numeric values | `RETURN SUM(p.age)` |
| `AVG(expr)` | Average numeric values | `RETURN AVG(p.age)` |
| `MIN(expr)` | Minimum value | `RETURN MIN(p.age)` |
| `MAX(expr)` | Maximum value | `RETURN MAX(p.age)` |

---

## Data Types

| Type | Examples | Notes |
|---|---|---|
| Integer | `42`, `-1`, `0` | 64-bit signed |
| Float | `3.14`, `-0.5` | 64-bit IEEE 754 |
| String | `'hello'`, `'it''s'` | Single-quoted, `''` for escaped quote |
| Boolean | `TRUE`, `FALSE` | Case-insensitive |
| Null | `NULL` | Missing or undefined value |

---

## Using the Rust Client Library

The `graft-client` crate provides a synchronous TCP client.

### Add Dependency

```toml
[dependencies]
graft-client = { path = "crates/graft-client" }
```

### Usage

```rust
use graft_client::Client;

fn main() -> std::io::Result<()> {
    // Connect to the server
    let mut client = Client::connect("127.0.0.1:7687")?;

    // Create data
    client.query("CREATE (:Person {name: 'Alice', age: 30})")?;
    client.query("CREATE (:Person {name: 'Bob', age: 25})")?;

    // Query data
    let result = client.query("MATCH (p:Person) RETURN p.name, p.age")?;

    // Access results
    println!("Columns: {:?}", result.columns);   // ["p.name", "p.age"]
    for row in &result.rows {
        println!("{:?}", row);                     // ["Alice", "30"]
    }
    println!("{} rows in {} ms", result.rows.len(), result.elapsed_ms);

    Ok(())
}
```

### Client API

```rust
impl Client {
    /// Connect to a graft server and perform the handshake.
    pub fn connect(addr: &str) -> std::io::Result<Self>;

    /// Execute a GQL query. Returns columns, rows (as strings), and timing.
    pub fn query(&mut self, gql: &str) -> std::io::Result<QueryResult>;
}

pub struct QueryResult {
    pub columns: Vec<String>,       // Column names
    pub rows: Vec<Vec<String>>,     // Row values as strings
    pub rows_affected: u64,         // For mutations
    pub elapsed_ms: u64,            // Server-side execution time
}
```

---

## Architecture Overview

```
                    ┌─────────────┐
                    │  graft-cli  │ ← interactive REPL
                    └──────┬──────┘
                           │ TCP (port 7687)
                    ┌──────▼──────┐
                    │graft-server │ ← accepts connections, routes queries
                    └──────┬──────┘
                           │
              ┌────────────▼────────────┐
              │     ShardCluster        │ ← coordinator
              │  (SPSC ring buffers)    │
              └─┬──────┬──────┬──────┬──┘
                │      │      │      │
             ┌──▼─┐ ┌──▼─┐ ┌──▼─┐ ┌──▼─┐
             │ S0 │ │ S1 │ │ S2 │ │ S3 │  ← one OS thread per shard
             └────┘ └────┘ └────┘ └────┘
              Each shard owns:
              • BufferPool (8KB pages)
              • Node/Edge records (64 bytes each)
              • Property side-tables
              • Label dictionary
```

### Key Concepts

- **Shard**: A data partition that owns a subset of nodes and edges. Each shard runs on its own OS thread.
- **Node ID**: 64-bit identifier. Upper 8 bits encode the owning shard, lower 56 bits are the local ID.
- **Index-free adjacency**: Each node record contains pointers to its first outbound and inbound edge. Edges form linked lists. Traversing one hop is O(1) regardless of total graph size.
- **Pages**: Data is stored on 8KB pages (aligned to NVMe page size), each holding up to 127 records of 64 bytes.

### Wire Protocol

graft uses a custom binary protocol on port 7687:

- **Header**: 8 bytes — magic (`GF01`), version, message type, payload length
- **Payload**: MessagePack-encoded data
- **Message flow**: `HELLO` → `QUERY` → `RESULT` → `ROW`* → `SUMMARY`

---

## Configuration Reference

### Server Flags

| Flag | Default | Description |
|---|---|---|
| `--host` | `127.0.0.1` | Bind address |
| `--port` | `7687` | Listen port |
| `--shards` | CPU core count | Number of shard threads (1–256) |

### Environment Variables

| Variable | Description |
|---|---|
| `RUST_LOG` | Logging level (`info`, `debug`, `trace`) |

```bash
# Enable debug logging
RUST_LOG=debug graft-server
```

---

## Benchmarks

Run the built-in benchmark suite with:

```bash
cargo bench --bench query
```

This measures insert throughput, point lookups, scan throughput, traversal latency, end-to-end GQL queries, and multi-shard overhead.

### Reference Numbers

Measured on AMD Ryzen 5 3600 (6 cores), 64 GB RAM, NVMe:

| Operation | Latency | Throughput |
|---|---|---|
| Point lookup (by ID) | 49 ns | — |
| Single traversal hop | 111 ns | 9M hops/sec/core |
| Fan-out (1000 edges) | 65 ns/edge | 15.4M edges/sec |
| Node insert | — | 3.3M nodes/sec |
| Edge insert | — | 2.8M edges/sec |
| Label scan (10K nodes) | — | 16M nodes/sec |
| E2E GQL `COUNT(*)` (100 nodes) | 17 µs | — |
| E2E GQL `CREATE` node | 2.2 µs | — |

Over TCP (localhost, 6 shards, 1000 nodes):

| Operation | Avg Latency | p99 Latency | Throughput |
|---|---|---|---|
| `CREATE` node | 29 µs | 42 µs | 34,721 qps |
| `COUNT(*)` | 173 µs | 180 µs | 5,794 qps |
| Single-hop traversal | 1.44 ms | 1.49 ms | 696 qps |
| Point lookup (property match) | 1.24 ms | 1.27 ms | 806 qps |

---

## License

graft is licensed under the [GNU Affero General Public License v3.0](https://www.gnu.org/licenses/agpl-3.0.html) (AGPL-3.0). One codebase, one license, no feature gating.
