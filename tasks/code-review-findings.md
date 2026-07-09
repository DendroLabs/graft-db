# Code Review Findings — Phase 8f Replication Transport (2026-07-09)

Source: multi-agent code review (xhigh effort, 53 agents, 66 candidates verified, 0 refuted).
All 15 findings below are CONFIRMED by independent adversarial verification.
Ranked most-severe first. Fix grouped by root cause, in roughly this order.

## 1. Shared outbox delivers each WAL batch to exactly ONE replica
- `crates/graft-server/src/replication.rs:121` (also :97, :100, :154)
- Every replica connection spawns a `primary_writer` that drains the SAME shared
  per-shard outbox `SharedQueue`. With 2 replicas, each gets an arbitrary ~half of
  the WAL stream — both end up with missing nodes/edges and broken adjacency chains.
- Also loses data with a single replica across reconnect: the lingering old writer
  thread drains batches into the dead socket, permanently discarding them.

## 2. Replication LSN resets on primary restart — silently halts all shipping
- `crates/graft-runtime/src/shard.rs:537`
- `repl_next_lsn` is hardcoded to 1 in `Shard::open` and never persisted. After a
  primary restart, replicas at e.g. last_applied_lsn=5000 skip every incoming batch
  via the `batch.last_lsn <= self.last_applied_lsn` dedup check in
  `ReplicationReceiver::on_batch`. Replication halts forever, no error logged.

## 3. Send errors drop batches permanently — no retransmission path
- `crates/graft-server/src/replication.rs:156` (also :264, event_loop.rs:285)
- `primary_writer` drops all drained-but-unsent batches on a send error. The
  sender's CommitBuffer was already drained (`drain_ready`), reconnect hello sends
  hardcoded `last_lsn: 0`, and `ReplControl::Register` only sets ACK-tracking state.
  Replica permanently misses committed transactions.

## 4. Shard-count mismatch silently drops WAL batches
- `crates/graft-server/src/replication.rs:309`
- No shard-count negotiation/validation in the ReplHello handshake. Primary with 8
  shards + replica with 4: batches for shards 4-7 match no inbox and fall through
  without a log line. Replica silently holds half the graph.

## 5. Replica hangs forever instead of reconnecting
- `crates/graft-server/src/replication.rs:329`
- `run_replica_session` joins `writer_handle`, but `replica_ack_writer` only exits
  on a send error and never sends once the primary is gone (no batches -> no ACKs ->
  infinite 1ms-sleep loop). Reconnect/backoff code is never reached. Replica
  silently stops replicating until process restart.

## 6. on_batch errors swallowed — corrupted batch creates undetected gap
- `crates/graft-runtime/src/event_loop.rs:286`
- `let _ = receiver.on_batch(batch);` discards Err from CRC mismatch/truncation.
  Records discarded, but the NEXT good batch has higher last_lsn, applies, and its
  ACK covers the gap. Primary believes replica has everything; replica permanently
  missing a transaction. Silent divergence.

## 7. Replica query tx_ids collide with replicated primary tx_ids (MVCC unsound)
- `crates/graft-runtime/src/shard.rs:554` (also cluster.rs:828)
- `sync_tx_counter` gives a replica query a tx_id equal to the primary's NEXT tx id.
  Mid-scan, `poll_replication` applies that primary tx's PageWrites with
  tx_min == the query's own tx_id; MVCC own-write visibility makes the
  partially-applied, uncommitted-on-replica transaction visible. Violates snapshot
  isolation on replicas.

## 8. begin_explicit_tx skips sync_tx_counter — same collision for explicit BEGIN
- `crates/graft-runtime/src/cluster.rs:589` (also :587)
- Auto-commit begin_tx syncs the counter; `begin_explicit_tx` does not (plain
  `fetch_add` on next_tx_id). Explicit BEGIN on a replica hands out tx_id 1 while
  primary ships its own tx 1/2/... — torn intermediate state visible. Note:
  main.rs BeginTx handling also lacks a read-only guard on replicas.

## 9. Replicas commit transactions the primary aborted (WriteConflict)
- `crates/graft-runtime/src/shard.rs:1103`
- `commit_current_tx` discards `tx_mgr.commit()`'s Result (`let _ =`), then
  unconditionally pushes a replication Commit record. On WriteConflict the tx was
  internally aborted on the primary (local WAL gets Abort), but the CommitBuffer
  releases the records and the replica applies + mark_committed. Permanent
  primary/replica divergence.

## 10. Unbounded outbox growth when no replica is connected (OOM)
- `crates/graft-runtime/src/event_loop.rs:263`
- On a primary, `poll_replication` serializes every committed WAL record into
  batches and pushes to the outbox unconditionally; `ReplicationSender::poll()`
  never checks for registered replicas. Primary with no replica attached duplicates
  the entire write stream in RAM until OOM.

## 11. Blocking handshake on accept-loop thread — one stalled connection blocks all
- `crates/graft-server/src/replication.rs:51`
- `recv_repl_message` (blocking, no read timeout) runs inline in the accept loop.
  Any client opening port 7688 without sending data blocks all future replica
  registration cluster-wide.

## 12. No read timeout / heartbeat-loss detection on replica (half-open TCP)
- `crates/graft-server/src/replication.rs:305`
- Primary host dies without RST: replica blocks in read_exact indefinitely. The 1s
  primary heartbeat is sent but never checked for arrival. Reconnect never triggers
  even after primary returns.

## 13. sync_tx_counter runs on EVERY auto-commit query on standalone/primary (perf)
- `crates/graft-runtime/src/cluster.rs:828`
- Blocking spin-wait GetNextTxId round-trip to every shard before every query, even
  on standalone/primary clusters where shard counters can never outrun the cluster
  counter. 32-shard server pays 32 sequential cross-thread round-trips per query in
  the hottest path. Only needed on replica clusters.

## 14. Reconnects append ghost ReplicaState entries forever; acked_lsn resets to 0
- `crates/graft-server/src/replication.rs:88` (also :71, event_loop.rs:243)
- Replicas registered under peer ip:ephemeral-port (unique per connection) with
  last_lsn from hello (always 0). `add_replica` appends; `remove_replica` only marks
  disconnected. Flapping replica grows the Vec unboundedly, min_acked_lsn reports 0
  after every reconnect (blocks future WAL retention), SHOW REPLICAS lists ghosts.

## 15. cluster_id hardcoded to 0 and never verified
- `crates/graft-server/src/replication.rs:265`
- Replica sends cluster_id 0, primary echoes it, nothing checks membership.
  Pointing --primary at the wrong host applies foreign WAL page-writes over
  existing pages — silent cross-cluster corruption. The field exists precisely to
  prevent this.

## Verification requirements
- `cargo test` green (278 tests at baseline), `cargo clippy -- -D warnings`,
  `cargo fmt` clean.
- New regression tests for each fixed failure scenario where practical
  (multi-replica fan-out, primary-restart LSN continuity, corrupted-batch handling,
  WriteConflict non-replication, explicit-tx counter sync).
- Follow CLAUDE.md design principles: reliability over convenience (no shortcuts
  that create separate failure modes), design for 1B+ scale (no unbounded memory,
  no O(n) scans competitors don't have).
