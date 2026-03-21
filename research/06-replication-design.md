# 06 — Replication Design Outline

## Goal

WAL-shipping primary-replica replication for durability and read scaling. A single primary accepts writes; replicas receive WAL records over TCP and replay them to maintain a consistent copy.

## Architecture

```
Primary                           Replica(s)
+-----------+                     +-------------+
| Shard 0   |---WAL stream-----→ | Shard 0     |
| Shard 1   |---WAL stream-----→ | Shard 1     |
| ...       |                     | ...         |
+-----------+                     +-------------+
     ↑                                   ↑
  writes                            reads only
```

Each shard on the primary ships its WAL independently to the corresponding shard on each replica. This preserves the shard-per-core model — no cross-shard coordination on the replication path.

## Phases

### 8a. Replication Protocol

New crate: `graft-repl`.

**ReplicationSender** (primary side):
- Attaches to a shard's WAL writer
- On each WAL write/commit, appends the record to a replication buffer
- Ships buffered records to connected replicas over TCP
- WAL records are already CRC-protected and self-describing — no additional framing needed beyond a length prefix
- Tracks the last-acknowledged LSN (log sequence number) per replica

**ReplicationReceiver** (replica side):
- TCP listener per shard, receives WAL records from primary
- Feeds records into a replay pipeline (reuses existing WAL recovery logic)
- Sends ACK with replayed LSN back to primary

**Wire format**: Reuse the existing binary wire protocol header (magic, version, msg type, length). New message types:
- `WAL_RECORD` — a single WAL record (already serialized)
- `WAL_ACK` — replica confirms replay up to LSN
- `REPL_HELLO` — handshake with shard_id, last known LSN for catchup

### 8b. Replica Shard

A read-only `Shard` variant that:
- Accepts WAL records via `apply_wal_record(record)` instead of direct mutations
- Adapts the existing two-pass WAL recovery for streaming replay:
  - Pass 1 is unnecessary for streaming (we know the record is committed because the primary only ships committed records)
  - Direct single-pass apply: decode WAL body, apply to storage
- Maintains its own buffer pool and page files (independent storage)
- Rejects write operations (CREATE, SET, DELETE) with an error

**Catchup**: On initial connect or reconnect, the replica sends its last-replayed LSN. The primary replays WAL records from that point forward. This requires either:
- WAL retention: keep WAL files until all replicas have acknowledged (configurable retention window)
- Or snapshot shipping: if a replica is too far behind, ship a full snapshot instead of WAL records

### 8c. Replication Manager

Lives in `graft-server` on the primary.

Responsibilities:
- Manages connected replicas (add/remove)
- Configurable `replication_mode`:
  - `async` (default): primary commits immediately, ships WAL asynchronously. Fastest writes, risk of data loss if primary crashes before replica receives records.
  - `sync(n)`: primary blocks on commit until `n` replicas acknowledge. Durable but higher latency.
  - `semi-sync`: primary blocks on commit until at least 1 replica acknowledges. Balance of durability and performance.
- Monitors replica lag (difference between primary LSN and replica's last-ack LSN)
- Handles replica disconnection gracefully (removes from ack set, continues accepting writes in async mode)

### 8d. Leader Election (Deferred)

Initially: static primary assignment via CLI flag (`--role primary` vs `--role replica --primary-addr host:port`).

Future: Raft-based leader election for automatic failover.
- Requires a minimum of 3 nodes for quorum
- The elected leader becomes the primary; others become replicas
- On leader failure, a new leader is elected and replicas re-attach
- Raft log can be a separate lightweight WAL, not the data WAL

### 8e. Read Routing

Clients connect to replicas for read-only queries:
- Replica advertises itself as read-only in the HELLO handshake
- Write queries on a replica return an error: "cannot write to replica"
- Optional: replica can forward writes to primary (adds complexity, defer initially)
- Staleness: reads on replicas may be slightly behind primary. Accept this for async mode. For sync mode, a replica that has ACK'd is guaranteed consistent up to the ACK'd LSN.

## Key Design Decisions

1. **Per-shard WAL shipping** (not per-cluster): preserves the shared-nothing model. Each shard's replication is independent — no global ordering or coordination.

2. **Reuse WAL format**: WAL records are already CRC-protected, typed, and self-describing. No need for a separate replication log format.

3. **Primary ships only committed records**: simplifies replica logic (no need for the two-pass committed-set check). The primary buffers records and only ships after commit.

4. **Async-first**: start with async replication for simplicity and performance. Add sync/semi-sync as configuration options.

5. **LSN-based catchup**: avoids the need for snapshot shipping in the common case. Only fall back to snapshot shipping when the replica is too far behind (WAL retention exceeded).

## Dependencies

- Phases 4-7 must be complete (core pinning, wire transactions, event loop, batched I/O)
- The event loop provides a natural integration point: `poll_replication()` phase in the tick loop
- Batched io_uring benefits replication: replica can batch WAL record writes

## Open Questions

1. **WAL retention policy**: time-based (keep N hours) vs size-based (keep N GB) vs replica-aware (keep until all replicas ACK)?
2. **Snapshot shipping format**: full page dump or logical dump?
3. **Multi-primary**: out of scope. Single-primary, multi-replica only.
4. **Cross-datacenter**: network partitions between primary and replica — how long to buffer before declaring replica dead?

## Implementation Order

1. `graft-repl` crate with `ReplicationSender` and `ReplicationReceiver`
2. Read-only replica shard with WAL apply
3. Integration into `graft-server` with `--role` flag
4. Async replication end-to-end tests
5. Sync/semi-sync mode
6. Catchup and WAL retention
7. Leader election (separate plan)
