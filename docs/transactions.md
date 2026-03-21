# Transaction Model

This document describes graft's transaction system: MVCC visibility, snapshot isolation, conflict detection, and the write-ahead log.

## Overview

graft uses **MVCC (Multi-Version Concurrency Control)** with **snapshot isolation**:

- Readers never block writers. Writers never block readers.
- Each transaction sees a consistent snapshot of the database as of its start time.
- Write conflicts are detected at commit time using **first-committer-wins**.
- Durability is provided by a **write-ahead log** (WAL) with buffered, group-commit writes.

## Transaction Lifecycle

```
begin()  →  read/write operations  →  commit() or abort()
```

1. **Begin**: assigns a monotonically increasing `TxId` and captures a snapshot of all currently active transactions.
2. **Read**: uses the snapshot to determine which record versions are visible.
3. **Write**: records the `(page_id, slot)` pair in the transaction's write set.
4. **Commit**: validates the write set against concurrent commits, writes a commit record to the WAL, and flushes.
5. **Abort**: writes an abort record to the WAL and discards the transaction state.

## MVCC Visibility

Every node and edge record carries two MVCC timestamps:

| Field | Meaning |
|---|---|
| `tx_min` | TxId of the transaction that created this record |
| `tx_max` | TxId of the transaction that deleted this record (0 = alive) |

### Snapshot

A snapshot is captured at transaction begin and contains:
- `ts`: the transaction's own TxId
- `active`: list of TxIds that were active (uncommitted) at snapshot time

### Visibility Rules

A record is visible to a snapshot if:

1. **The creator committed before our snapshot**: `tx_min < ts` AND `tx_min` is NOT in the active set
2. **The record has not been deleted by a visible transaction**: `tx_max == 0` (alive) OR `tx_max >= ts` (deleted after us) OR `tx_max` is in the active set (deleter hasn't committed)

In code:

```rust
fn is_visible(&self, tx_min: TxId, tx_max: TxId) -> bool {
    let creator_visible = tx_min < self.ts && !self.active.contains(&tx_min);
    if !creator_visible {
        return false;
    }
    tx_max == 0 || tx_max >= self.ts || self.active.contains(&tx_max)
}
```

### Own Writes

A transaction's own writes have `tx_min == current_tx`. Since the snapshot's `is_visible()` requires `tx_min < ts` (strict less-than), own writes are **not** visible through the snapshot alone.

The `Shard::is_record_visible()` method handles own-writes as a special case **before** consulting the snapshot:

```rust
// Own writes: tx_min == current_tx → visible (unless also deleted by current_tx)
if tx_min == self.current_tx {
    return tx_max == 0 || tx_max != self.current_tx;
}
```

This keeps the `Snapshot` logic simple while ensuring within-transaction reads see the transaction's own writes.

## Conflict Detection

graft uses **first-committer-wins** with write-set validation at commit time.

### How It Works

1. When a transaction writes to a record, it logs the `(page_id, slot)` pair in its write set.
2. At commit time, each entry in the write set is checked against `committed_writes` — a map from `(page_id, slot)` to the TxId that last committed a write.
3. A conflict exists if the last writer was **invisible to our snapshot** — meaning it committed concurrently:

```rust
let was_invisible = last_writer >= tx.snapshot.ts
    || tx.snapshot.active.contains(&last_writer);
```

4. If any conflict is found, the transaction is aborted and a `WriteConflict` error is returned.
5. If no conflicts, the transaction commits: its write set entries are added to `committed_writes`.

### Why Both Checks

The conflict check has two conditions because there are two ways a concurrent writer can be invisible:

- `last_writer >= snapshot.ts`: the writer started after us
- `active.contains(&last_writer)`: the writer was active (uncommitted) when we started, but committed while we were running

Both represent concurrent modifications that our snapshot didn't see.

### Example: No Conflict

```
tx1 begins (ts=1)
tx1 writes (page 5, slot 3)
tx1 commits
tx2 begins (ts=2)     ← snapshot sees tx1 as committed
tx2 writes (page 5, slot 3)
tx2 commits            ← OK: tx1 committed before tx2's snapshot
```

### Example: Conflict

```
tx1 begins (ts=1)
tx2 begins (ts=2)      ← snapshot: active = [tx1]
tx1 writes (page 5, slot 3)
tx2 writes (page 5, slot 3)
tx1 commits             ← succeeds (first committer)
tx2 commits             ← CONFLICT: tx1 was in tx2's active set
```

## Write-Ahead Log (WAL)

The WAL ensures durability: every state change is logged before it takes effect. On crash, the WAL is replayed to recover committed state.

### Record Format

```
[header: 20 bytes] [body: variable] [CRC32C: 4 bytes]
```

**Header (20 bytes):**
```
Offset  Size  Field
─────────────────────────────────
0       8     tx_id          (u64)
8       4     body_len       (u32)
12      1     record_type    (u8)
13      3     padding
16      4     reserved
```

**CRC32C** covers header + body.

### Record Types

| Value | Type | Body | Description |
|---|---|---|---|
| 1 | `Begin` | Empty | Transaction started |
| 2 | `Commit` | Empty | Transaction committed |
| 3 | `Abort` | Empty | Transaction aborted |
| 10 | `PageWrite` | 80 bytes | Redo: write data to (page, slot) |
| 11 | `PageClear` | 16 bytes | Redo: free slot (page, slot) |
| 20 | `Checkpoint` | Empty | Checkpoint marker |

**PageWrite body (80 bytes):**
```
0       8     page_id    (u64)
8       2     slot       (u16)
10      6     padding
16      64    data       (record bytes)
```

**PageClear body (16 bytes):**
```
0       8     page_id    (u64)
8       2     slot       (u16)
10      6     padding
```

### LSN (Log Sequence Number)

The LSN is the **byte offset** of a record within the WAL file. This gives a total ordering of all WAL records and allows pages to track which WAL entry they were last updated from (via the page header's `lsn` field).

### Buffered Writes and Group Commit

The WAL writer maintains a **64 KB in-memory buffer**. Records are appended to the buffer via `append()`, which returns the LSN immediately.

The WAL has two separate operations:

- **`write()`**: flushes the buffer to the OS page cache. Data is safe against process crashes (the OS has it) but NOT against power failure.
- **`sync()`**: calls `fsync`, making all written data durable against power failure.
- **`flush()`**: `write()` + `sync()` combined. Used for full checkpoints.

#### Group Commit

Transaction commits use **group commit** to amortize `fsync` cost:

1. On commit, the WAL Commit record is written to the OS page cache via `write()` (no fsync).
2. The shard periodically calls `sync()` when either threshold is reached:
   - **Time**: 2ms since last sync (`GROUP_COMMIT_INTERVAL_MS`)
   - **Count**: 64 commits since last sync (`GROUP_COMMIT_MAX_PENDING`)

This means:

- **Committed data is immediately safe against process crashes** (application crash, OOM kill, segfault). The OS page cache has the data and will write it to disk.
- **On power failure**, up to ~2ms or 64 transactions of committed data may be lost. This is the same tradeoff PostgreSQL makes with `synchronous_commit = off`.
- **Full checkpoints** (`Shard::flush()`) always use `flush()` (write + sync) for complete durability.

The group commit optimization yields a ~50x throughput improvement for durable writes on typical hardware.

### Recovery

WAL recovery uses a **two-pass** approach for transaction awareness:

**Pass 1 — Build committed transaction set:**
1. Scan the entire WAL sequentially
2. Collect all tx_ids that have a `Commit` record
3. Track the maximum tx_id seen (for `advance_past()`)

**Pass 2 — Replay committed writes only:**
1. Scan the WAL again from the beginning
2. For each `PageWrite`/`PageClear` record:
   - If `tx_id == 0` (pre-MVCC), replay unconditionally
   - If `tx_id` is in the committed set, replay
   - Otherwise, skip (transaction was active/aborted at crash time)

`WalReader` scans sequentially — reads the 20-byte header, body, and 4-byte CRC. A corrupt or truncated record is treated as the end of the valid log. All records before it are valid; everything after is discarded.

After replay, `TransactionManager::advance_past(max_tx_id)` ensures new transactions don't collide with recovered ones and sets `base_tx_id` for post-recovery visibility.

### Checkpoints (planned)

Checkpoints flush all dirty pages to the data file and write a checkpoint record to the WAL. After a checkpoint, WAL records before the checkpoint's LSN can be discarded. Target: every 5 minutes or 100 MB of WAL data.

## TransactionManager

The `TransactionManager` is the central coordinator for transaction lifecycle within a single shard. It is single-threaded (one per shard).

### State

| Field | Type | Purpose |
|---|---|---|
| `next_tx_id` | `TxId` | Next ID to assign (starts at 1; 0 is NULL) |
| `base_tx_id` | `TxId` | Starting tx_id for this session (tx_ids below this are from previous sessions, treated as committed) |
| `active` | `HashMap<TxId, Transaction>` | Currently active transactions |
| `committed_writes` | `HashMap<(PageId, u16), TxId>` | Last committer per (page, slot) — pruned below low-water |
| `committed_set` | `HashSet<TxId>` | Set of committed TxIds — NOT pruned (needed for MVCC visibility) |
| `low_water` | `TxId` | Oldest active snapshot timestamp |

### Low-Water Mark Pruning

When a transaction commits or aborts, the low-water mark is recomputed as the minimum `snapshot.ts` among all active transactions. `committed_writes` entries below the low-water mark are pruned — they are no longer needed for conflict detection.

**Note:** `committed_set` is intentionally **not** pruned. It is needed for MVCC visibility checks (`was_committed()`) — a record's `tx_min` may reference a committed transaction that is below the low-water mark, and `is_record_visible()` must still be able to confirm it committed (rather than was aborted). A future optimization can replace `committed_set` with a high-water mark once all transactions below it are guaranteed committed.

### Post-Recovery Visibility

After WAL recovery, the `TransactionManager` uses `base_tx_id` (set by `advance_past()`) to recognize transactions from previous sessions. `was_committed(tx_id)` returns `true` if `tx_id < base_tx_id`, since two-pass WAL recovery ensures only committed writes survive.

### Auto-Commit

Every query is implicitly wrapped in a transaction by the executor:

```
begin_tx() → execute query → commit_tx() (or abort_tx() on error)
```

`StorageAccess` provides `begin_tx/commit_tx/abort_tx` methods (default no-ops for backward compatibility). `ShardCluster` generates a global `tx_id` via `AtomicU64` and routes `BeginTx/CommitTx/AbortTx` to all shard workers.

### Edge Chain Traversal with MVCC

When traversing edge chains (outbound/inbound linked lists), invisible edges must be skipped without breaking the chain. The shard reads raw edge records to get next pointers (`next_out_edge`/`next_in_edge`), then checks visibility separately. This ensures that an invisible edge mid-chain still provides the pointer to the next edge.
