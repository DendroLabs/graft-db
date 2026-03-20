# Graph Database Architecture Landscape

## Summary Table

| Database | Storage | Data Model | Query Language | ACID | Clustering | Schema | License |
|----------|---------|-----------|---------------|------|-----------|--------|---------|
| **Neo4j** | Native graph (block store) | Property Graph | Cypher | Full | Raft (single writer + read replicas) | Optional | GPLv3 (CE) / Commercial (EE) |
| **Neptune** | Custom distributed (Aurora-like) | Property Graph + RDF | Gremlin, openCypher, SPARQL | Full (writer) | Single writer + 15 read replicas | Free | Proprietary (AWS managed) |
| **JanusGraph** | Pluggable (Cassandra/HBase/BDB) | Property Graph | Gremlin | Backend-dependent | Via storage backend | Optional | Apache 2.0 |
| **ArangoDB** | RocksDB | Multi-model (doc/graph/KV) | AQL | Full (single-node/OneShard) | Coordinator/DBServer/Agent | Optional | Community License / BSL 1.1 |
| **TigerGraph** | Native C++ (GSE/GPE) | Property Graph | GSQL | Full | MPP with partitioning + replication | Strict (schema-first) | Proprietary (free tier 50GB) |
| **Dgraph** | Badger (LSM/Go) | RDF-like / GraphQL-native | DQL / GraphQL | Snapshot Isolation | Raft groups (predicate sharding) | Semi-required | Apache 2.0 |
| **NebulaGraph** | RocksDB per storage node | Property Graph | nGQL | Limited cross-partition | Shared-nothing (compute/storage separated) | Strict | Apache 2.0 + Commons Clause |
| **SurrealDB** | RocksDB/TiKV/SurrealKV | Multi-model (doc/graph/rel/vector) | SurrealQL | Full | Compute/storage separation (TiKV) | Optional | BSL (-> Apache 2.0 after 4yr) |
| **Apache AGE** | PostgreSQL tables | Property Graph | openCypher + SQL | Full (PostgreSQL) | PostgreSQL replication | Optional | Apache 2.0 |
| **Memgraph** | In-memory (skip lists) + WAL/snapshots | Property Graph | Cypher | Full (transactional mode) | MAIN + REPLICAs | Optional | Open Source (CE) / Commercial (EE) |
| **TypeDB** | RocksDB | PERA (entity-relation-attribute) | TypeQL | Snapshot Isolation | Enterprise only | Strict (type-theoretic) | MPL 2.0 (CE) / Commercial (EE) |

---

## Individual Database Architectures

### Neo4j

**Storage Engine:** Native graph using fixed-size records with direct file offsets (pointers) to neighbors. Neo4j 5+ introduced a Block Store Format grouping related data into 8KB blocks (matched to NVMe page sizes), co-locating a node, its properties, and its most frequently accessed relationships for improved locality. Implements *index-free adjacency* at the byte level — traversing a relationship is a direct memory-address calculation, not a B-tree lookup.

**Query Language:** Cypher — declarative, pattern-matching with ASCII-art syntax for paths (e.g., `(a)-[:KNOWS]->(b)`). Primary input into the ISO GQL standard.

**Indexing:** Range indexes (replacing B-tree in v5+), Point indexes (spatial), Text indexes (prefix/suffix), Full-text (Lucene-backed), Composite (multi-property). Token lookup indexes for label/relationship-type scanning. Primary traversal is index-free adjacency — indexes used only for initial node lookups.

**Transactions:** Full ACID. Raft consensus for write durability in clusters (N/2+1 acknowledgment). Serializable isolation single-node. Causal consistency via bookmark mechanism in clusters.

**Clustering:** Core-Replica architecture. Core (Primary) servers form Raft consensus group for writes; Secondary (Read Replica) servers scale reads via async log shipping. **Single-writer bottleneck** — all writes through Raft leader. Entire graph must fit on one machine. New *InfiniGraph* (Sep 2025) introduces "property sharding" — graph structure stays in one shard, properties distributed across Autonomous Clusters, targeting 100TB+.

**Schema:** Schema-optional. Supports optional constraints (uniqueness, existence, node key) and indexes via Cypher DDL.

**Licensing:** Community Edition: GPLv3 (single-node only, no clustering, no hot backups, 4-CPU-core limit for GDS). Enterprise Edition: commercial license. Pricing reportedly $10,000-$50,000+/month.

**Limitations:** Single-writer bottleneck. No native horizontal sharding (until InfiniGraph). Full graph must fit on one machine's storage. Community Edition severely restricted.

---

### Amazon Neptune

**Storage:** Custom SSD-backed distributed storage (Aurora-inspired), replicated 6 ways across 3 AZs. Auto-scales 10 GiB to 128 TiB. Supports both property graph and RDF in different internal formats.

**Query Languages:** Gremlin (imperative traversal), openCypher (declarative pattern matching), SPARQL (RDF). Multi-language support is unique.

**Indexing:** Automatic — SPOG, POGS, GPSO indexes. Users cannot create custom indexes. Optional OSGP index via Lab Mode.

**Transactions:** ACID on writer with READ COMMITTED isolation (strengthened to prevent non-repeatable/phantom reads). Read replicas: SNAPSHOT isolation with eventual consistency.

**Clustering:** Single writer + up to 15 read replicas. Write scaling is vertical only. Neptune Serverless auto-scales compute.

**Limitations:** AWS vendor lock-in. No custom indexing. Single-writer constraint. Cannot mix property graph and RDF in same queries. Limited configuration.

---

### JanusGraph

**Storage:** Non-native — pluggable storage backend (Cassandra, HBase, Bigtable, BerkeleyDB, ScyllaDB). Graph stored as adjacency list serialized into KV rows. Does NOT implement index-free adjacency.

**Query Language:** Gremlin (TinkerPop). Also supports OLAP via Hadoop-Gremlin with Spark.

**Indexing:** Composite indexes (equality-only, stored in primary backend) and Mixed indexes (Elasticsearch/Solr/Lucene for range, full-text, geo, fuzzy). **Critical issue:** Updates to data and mixed indexes are NOT atomic.

**Transactions:** ACID depends entirely on backend. BerkeleyDB: full ACID. Cassandra/HBase: NOT fully ACID. JanusGraph adds optimistic locking and eventual consistency reconciliation.

**Clustering:** Inherited from storage backend. JanusGraph query servers are stateless and independently scalable. Must manage 3+ independent systems (storage cluster, index cluster, Gremlin server).

**Licensing:** Apache 2.0 (Linux Foundation).

**Limitations:** Non-atomic index updates. ACID is backend-dependent and often weaker than expected. High operational complexity. Higher query latency than native graph stores. Limited community momentum.

---

### ArangoDB

**Storage:** RocksDB (LSM-tree). Data stored as JSON documents; edges are special documents with `_from`/`_to` fields. Graph capabilities layered on document store — NOT native graph storage.

**Query Language:** AQL — unified declarative language for documents, KV, and graphs. Supports traversals, JOINs, geospatial, full-text in one language.

**Indexing:** Persistent (B-tree-like), hash, fulltext, geo (S2-based), TTL. LZ4 compression (~1:6 ratio).

**Transactions:** ACID for single-node and OneShard. Sharded cluster mode: ACID only if data not distributed across multiple DB-Servers. OneShard feature places all collections on single DB-Server.

**Clustering:** Coordinator-DBServer-Agent architecture. Configurable shard keys. Coordinators are stateless routers. Agents (Raft) manage cluster state. Synchronous replication.

**Licensing:** Changed: v3.12+ Community Edition has custom license restricting commercial use and imposing **100GB dataset limit**. Enterprise uses BSL 1.1 (converts to Apache 2.0 after 4 years).

**Limitations:** Not native graph — traversals slower for deep queries. Multi-shard ACID not fully supported. 100GB community limit. AQL unique to ArangoDB. SmartGraphs paywalled.

---

### TigerGraph

**Storage:** Native C++ engine. Graph Storage Engine (GSE) and Graph Processing Engine (GPE) co-located for data locality. Compressed proprietary format (~50% of input size).

**Query Language:** GSQL — SQL-compatible with BSP parallel computation. ACCUM clause for parallel node/edge compute. Declarative SQL-style plus iterative graph algorithms.

**Indexing:** Internal, automatic. Engine optimizes access patterns based on defined schema.

**Transactions:** Full ACID with strong consistency.

**Clustering:** True MPP. Data partitioned with configurable partitioning and replication factors. Queries execute in parallel across all partitions. Claims 100s of millions of vertices/edges per second per machine.

**Licensing:** Proprietary. Free Enterprise license for databases up to 50GB. Full pricing not public.

**Limitations:** Fully proprietary/closed-source. GSQL unique to TigerGraph. Schema-first makes it less flexible. High operational complexity. Substantial enterprise costs.

---

### Dgraph

**Storage:** Badger — Go-based LSM-tree KV store, SSD-optimized, based on WiscKey paper (separates keys from values). Data organized as predicates, not nodes.

**Query Language:** DQL (GraphQL variant with graph extensions). Native GraphQL integration — schema generates complete API without resolvers.

**Transactions:** Distributed ACID with Snapshot Isolation (not Serializable — write skew possible). MVCC, optimistic concurrency, linearizable reads, WAL.

**Clustering:** Zeros (cluster managers) + Alphas (data servers). Alphas grouped by predicates (predicate sharding). Raft consensus within each group. Automatic predicate rebalancing.

**Licensing:** Apache 2.0 (planned full open-source in v25).

**Limitations:** Predicate-based sharding means multi-predicate queries hit multiple groups. Snapshot Isolation allows write skew. Corporate turbulence (acquired twice). DQL not portable.

---

### NebulaGraph

**Storage:** RocksDB per storage node. Hash-partitioned by vertex_id. Strict compute/storage separation.

**Query Language:** nGQL — SQL-like declarative graph language.

**Transactions:** Raft consensus for storage-layer consistency. Single-partition operations atomic, but cross-partition transaction guarantees limited.

**Clustering:** Three-service: Meta Service (Raft-based metadata), Query Service (stateless, horizontally scalable), Storage Service (shared-nothing, Raft-replicated). Compute and storage scale independently.

**Licensing:** Apache 2.0 + Commons Clause 1.0.

**Limitations:** Strong schema requirement. Limited cross-partition transactions. nGQL not widely adopted. External Elasticsearch required for full-text. Smaller Western community.

---

### SurrealDB

**Storage:** RocksDB (embedded), TiKV (distributed), SurrealKV (custom), IndexedDB (browser/WASM). Graph relationships stored as embedded edges alongside documents — not native graph storage.

**Query Language:** SurrealQL — handles documents, graph traversals, JOINs, vector search, full-text in one language.

**Transactions:** Fully ACID. TiKV's Raft-based consensus for distributed fault tolerance.

**Licensing:** BSL (converts to Apache 2.0 after 4 years).

**Limitations:** Young project, not battle-tested. Graph is layered on document store. SurrealQL unique. TiKV adds operational complexity. Multi-model breadth may lag dedicated GDBs.

---

### Apache AGE

**Storage:** PostgreSQL tables. Each graph label gets a vertex or edge table. Properties stored as `agtype` (JSONB-like).

**Query Language:** openCypher embedded in SQL via `cypher()` function.

**Transactions:** Full ACID inherited from PostgreSQL.

**Clustering:** PostgreSQL's scaling model — vertical primary, horizontal read replicas. Can leverage Citus, Patroni. No graph-specific distributed execution.

**Licensing:** Apache 2.0.

**Limitations:** Graph traversals translate to table joins — degrade at depth. Performance limited by relational model. Scaling is PostgreSQL scaling. Smaller community.

---

### Memgraph

**Storage:** In-memory with concurrent skip lists. Durability via snapshots + WAL. Three modes: IN_MEMORY_TRANSACTIONAL (default), IN_MEMORY_ANALYTICAL (no durability), ON_DISK_TRANSACTIONAL (RocksDB).

**Query Language:** Cypher (openCypher-compatible) over Bolt protocol. MAGE extensions for graph algorithms.

**Transactions:** Full ACID with MVCC in transactional mode. Writes never block reads.

**Clustering:** MAIN-REPLICA. One MAIN handles reads/writes; REPLICAs provide read scaling. Enterprise adds automatic failover via Raft coordinator.

**Licensing:** Community (open source) / Enterprise (commercial for HA, RBAC, audit).

**Limitations:** Dataset limited by RAM. Single-writer MAIN. Community lacks auto-failover. ON_DISK mode slower. Smaller ecosystem. Benchmark claims questioned.

---

### TypeDB

**Storage:** RocksDB. Polymorphic Entity-Relation-Attribute (PERA) model grounded in type theory.

**Query Language:** TypeQL — type-theoretic, declarative. "Queries as Types." Published at ACM SIGMOD/PODS 2024 (Best Newcomer Award).

**Transactions:** ACID with snapshot isolation. Optimistic concurrency. Read/write/schema transaction flavors.

**Clustering:** Community is single-server only. TypeDB Cloud provides clustering with auto-failover.

**Licensing:** Community: MPL 2.0. Cloud/Enterprise: commercial.

**Limitations:** Community is single-server. PERA model requires significant learning. Small community. Type-theoretic overhead for simple use cases.

---

## Key Architectural Concepts

### GQL (ISO/IEC 39075:2024)

First new ISO database language since SQL in 1987. Published April 2024. Features graph pattern matching with ASCII-art syntax, quantified path patterns, regular path queries. Built primarily from Cypher with inputs from PGQL, GSQL, and G-CORE. Shares GPML sub-language with SQL/PGQ (read-only SQL extension). All major vendors moving toward compliance. SQL/PGQ means relational databases (PostgreSQL, Oracle, SQL Server) will gain graph query capabilities — increasing competitive pressure on standalone GDBs.

### Property Graph vs RDF

| Dimension | Property Graph | RDF |
|-----------|---------------|-----|
| Orientation | Node-centric | Triple-centric |
| Identity | Local database IDs | Globally unique URIs |
| Interoperability | Low | High (web-scale linking) |
| Schema | Typically optional | Ontology-based (RDFS, OWL) |
| Performance | Optimized for traversals | Slower for deep traversals |
| Best For | App backends, analytics | Knowledge graphs, semantic web |

### Native Graph Storage vs Graph Layer on KV/Relational

**Native** (Neo4j, TigerGraph, Memgraph): Data physically stored as graph structures with direct pointer-based adjacency. Traversal cost O(k) where k = relationships traversed, independent of total graph size. Performance advantage grows exponentially with traversal depth (2+ hops: 10x+; 4+ hops: dramatic).

**Non-native** (JanusGraph, Apache AGE, ArangoDB, SurrealDB): Graph operations translate to JOINs or KV lookups. Each hop requires index lookups — at minimum 2 per relationship. Performance degrades with graph size because indexes grow. N hops = 2N index lookups. Advantage: leverage existing mature infrastructure.

### Index-Free Adjacency vs Indexed Lookups

**Index-free adjacency** (Neo4j, Memgraph, TigerGraph): Each node stores direct physical references to adjacent nodes. Traversal = pointer dereference, O(1) per hop. Query time proportional to subgraph touched, not total graph size.

**Indexed lookups** (JanusGraph, AGE, ArangoDB): Relationships resolved by global index lookup — O(log N) per hop. Performance degrades as graph grows. Multi-hop penalty compounds.

For 1-hop: difference negligible. For 4+ hops: index-free adjacency completes in milliseconds where indexed approaches may time out.
