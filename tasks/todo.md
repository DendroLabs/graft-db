# Write-Conflict Detection — Plan (current task)

Task: (1) Shard mutation methods must call `TransactionManager::record_write()`
so first-committer-wins conflict detection can actually trigger; (2) surface
`WriteConflict` from `commit_current_tx()` to wire-protocol clients so a losing
commit is reported as an error, not success. These are the two out-of-scope
gaps flagged in the previous task's Milestone 3 result (below).

Baseline: 306 tests passing on `main` at c1b91ee (re-verified at task start).

Milestones by root cause. Ordered so the dangerous switch (conflicts actually
firing) is flipped only after the error-reporting path is already in place.

## Milestone 1 — Surface commit results end-to-end (root cause: every layer discards `commit_current_tx()`'s Result)

The plumbing today: `Shard::commit_current_tx()` returns `()`;
`event_loop.rs`'s `Request::CommitTx` arm answers `Response::Done`
unconditionally; `cluster.rs`'s `commit_explicit_tx()` /
`StorageAccess::commit_tx` return `()`; `executor.rs`'s auto-commit ignores
`commit_tx()`; `main.rs`'s `CommitTx` handler always sends a success
`SummaryMsg`. A commit that loses first-committer-wins would be reported as
success at every one of those layers.

Design:
- `Shard::commit_current_tx() -> graft_core::Result<()>` — return the
  already-captured `commit_result` (the replication Commit/Abort branching on
  it stays exactly as is).
- `StorageAccess::commit_tx()` (graft-query) → `Result<(), String>`, default
  `Ok(())`. String keeps graft-query decoupled from graft-core's Error and
  matches the existing `Result<_, String>` convention in cluster::query and
  the wire ErrorMsg.
- `executor::execute()`: on plan success, a failing `commit_tx()` becomes a
  new `ExecutionError::CommitFailed(String)`. No `abort_tx()` on commit
  failure — `tx_mgr.commit()` already aborted internally on conflict.
- `Response::CommitResult(Result<(), String>)` in cluster.rs;
  `Request::CommitTx` arm in event_loop.rs returns it.
- `ShardCluster::commit_explicit_tx() -> Result<(), String>` and its
  `StorageAccess::commit_tx` — send CommitTx to ALL shards regardless of
  individual failures (every shard must still resolve its tx), return the
  first error. Same for `Database::commit_tx` (database.rs).
- `main.rs` CommitTx handler: `Err` → `ErrorMsg`; `active_tx = None` either
  way (a conflicted tx is already aborted server-side).
- graft-client needs NO change: `commit_tx()` already maps an ERROR response
  to `Err(io::Error)`.
- `Shard.tx_mgr` field → `pub(crate)` so event_loop.rs tests can inject write
  sets the same way shard.rs's own tests already do (there is still no real
  record_write path until Milestone 2).

- [x] shard.rs: `commit_current_tx()` returns Result; StorageAccess impl maps;
      `tx_mgr` pub(crate); update doc comment (result IS surfaced now)
- [x] executor.rs: trait signature + `ExecutionError::CommitFailed` + execute()
      propagation; new unit test (mock storage with failing commit_tx)
- [x] event_loop.rs: CommitTx arm → `Response::CommitResult`; new test that a
      conflict injected via `shard.tx_mgr.record_write()` comes back as
      `CommitResult(Err(_))` through the coordinator SPSC path
- [x] cluster.rs: Response variant; `commit_explicit_tx()`/`commit_tx()` collect
      per-shard results; existing tests get `.unwrap()`
- [x] database.rs: `commit_tx()` collects per-shard results
- [x] main.rs: CommitTx handler branches Ok/Err; wire_protocol.rs test-harness
      call site updated
- [x] benches (persistence.rs, query.rs): `shard.commit_tx()` now Result —
      `.unwrap()`
- [x] shard.rs: strengthen the 2 existing finding-#9 regression tests to also
      assert tx1's commit returns Ok and tx2's returns Err(WriteConflict)
- [x] `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`,
      `cargo fmt --check`

### Milestone 1 — result (done)

308 tests passing (306 baseline + 2 new: executor.rs
`commit_failure_surfaces_as_execution_error`, event_loop.rs
`event_loop_commit_tx_reports_write_conflict`), `cargo clippy --workspace
--all-targets -- -D warnings` clean, `cargo fmt --check` clean.

Implemented exactly per the design block above; no deviations. Notes for the
next coder:
- `Shard.tx_mgr` is now `pub(crate)` (with a doc comment saying why) so
  same-crate tests can inject write sets until Milestone 2 makes real ones.
- Existing tests calling `shard.commit_current_tx()` / `shard.commit_tx()` /
  `cluster.commit_explicit_tx()` got `.unwrap()` — Result is `#[must_use]`,
  so any new call site must handle it (clippy gate enforces this).
- `QueryResult` (graft-query executor.rs) gained `#[derive(Debug)]` — needed
  by a test's `unwrap_err()`; harmless public addition.
- On the auto-commit path a commit failure surfaces as
  `ExecutionError::CommitFailed` → cluster::query's `Result<_, String>` →
  wire ERROR ("commit failed: transaction conflict: ..."). On the explicit
  path, server sends ERROR for COMMIT_TX and clears its per-connection
  `active_tx` either way (the tx is already resolved shard-side).
- graft-client needed no change (`commit_tx()` already maps ERROR → `Err`);
  graft-cli needed no change (it only sends Query messages, and
  `execute_query` already prints server errors).
- The cross-shard partial-commit gap on `Err` is documented in
  `commit_explicit_tx()`'s doc comment and deferred to Milestone 4 (flagged,
  needs scope confirmation).

## Milestone 2 — Record write sets in Shard mutation paths (root cause: `record_write()` never called, so `tx.write_set` is always empty)

- All real record writes funnel through exactly three helpers
  (`write_node_record`, `write_edge_record`, `write_prop_record`), which all
  call `log_page_write(page_id, slot, data, page_type)` — wire
  `tx_mgr.record_write()` there (one choke point). `log_label_write` does NOT
  go through `log_page_write` and must NOT record (label dictionary writes use
  a synthetic `(0,0)` key and get_or_insert is idempotent — recording would
  make every label creation conflict with every other).
- KEY-SPACE BUG TO AVOID: node/edge/prop files have independent page-id
  spaces (separate `alloc_*_slot` counters), so a raw `(page_id, slot)`
  conflict key would false-conflict node page N with edge page N. The
  conflict key must include a record-space discriminant. Preferred: change
  `TransactionManager`'s write-set key to `(u8 /*page_type*/, PageId, u16)`
  (honest tuple; graft-txn's API is crate-internal). Alternative (bit-encode
  page_type into PageId's upper bits) rejected as less readable.
- `record_write` no-ops when `current_tx` isn't active (e.g. legacy tx-0
  paths) — already safe.
- Verify recovery/`apply_wal_record` paths don't route through
  `log_page_write` (they use `write_record_to_pool_inline`) so replica apply
  and WAL replay never record write sets.
- Tests at shard level now use REAL mutations (no more direct
  `tx_mgr.record_write` injection): two interleaved txs (via
  `begin_tx_with_id`/`set_active_tx`) doing `set_node_property` on the same
  node+key → second committer gets Err; disjoint writes (different nodes,
  and node-page-N vs edge-page-N) → both commit Ok (key-space regression).
- After this milestone conflicts are real at the Shard level, but still NOT
  triggerable through the cluster/wire (see Milestone 3's root cause).

- [x] graft-txn transaction.rs: write-set key `(PageId, u16)` →
      `(u8 /*page_type*/, PageId, u16)`; `record_write()` gains `page_type`
      param; `committed_writes` + `commit()` loop + doc comments updated;
      4 existing tests updated to new signature; new unit test: same
      (page,slot) under different page_type does NOT conflict
- [x] shard.rs: `log_page_write()` calls
      `self.tx_mgr.record_write(self.current_tx, page_type, page_id, slot)`
      (top of fn, before WAL append); `log_label_write` deliberately NOT
      recording (comment says why); `tx_mgr` field doc comment updated
- [x] Verified (by reading, not assuming): WAL replay pass 2 uses free fn
      `write_record_to_pool`, replica apply uses `write_record_to_pool_inline`
      — neither routes through `log_page_write`, so no write-set recording
      on replay/apply
- [x] shard.rs: convert the 2 finding-#9 tests from `tx_mgr.record_write`
      injection to REAL mutations (committed setup tx creates node+property;
      txs 2/3 both `set_node_property` same key); fix stale comment block
- [x] event_loop.rs: `event_loop_commit_tx_reports_write_conflict` updated to
      new record_write signature (kept as injection — it tests the
      CommitResult response plumbing, not the recording; comment updated)
- [x] shard.rs new tests: (a) two interleaved txs set same node property →
      second committer Err(WriteConflict); (b) disjoint property writes →
      both commit Ok; (c) node-page-N vs edge-page-N no false conflict
      (delete_node(n1) in tx1 vs create_edge(n2→n3) in tx2 — both hit
      (page 0, slot 0) in their own namespaces); (d) two txs create_edge from
      the same source node → head-pointer update conflicts, second loses
- [x] `cargo test --workspace` (308 baseline + new), `cargo clippy --workspace
      --all-targets -- -D warnings`, `cargo fmt --check`

### Milestone 2 — result (done)

313 tests passing (308 baseline + 5 new: graft-txn
`no_conflict_same_page_slot_different_page_type`, shard.rs
`real_mutation_write_conflict_second_committer_loses`,
`real_mutation_disjoint_writes_both_commit`,
`node_and_edge_page_namespaces_do_not_false_conflict`,
`concurrent_edges_from_same_source_conflict_on_head_pointer`),
`cargo clippy --workspace --all-targets -- -D warnings` clean,
`cargo fmt --check` clean.

Implemented exactly per the design block above; no deviations. Notes for the
next coder:
- Conflict key is `(u8 page_type, PageId, u16 slot)` end to end:
  `Transaction.write_set`, `TransactionManager.committed_writes`, and
  `record_write()`'s signature all changed together. `page_type` values are
  `graft_storage::PageType as u8` (Node=1, Edge=2, PropertyOverflow=3).
- The single recording site is the top of `Shard::log_page_write()` — every
  real record mutation (`write_node_record`/`write_edge_record`/
  `write_prop_record`) funnels through it. No borrow-order issue: the call
  happens before the WAL RefCell borrow is taken.
- `log_label_write` deliberately does NOT record (doc comment on it explains:
  synthetic (0,0) key + idempotent `get_or_insert` would false-conflict every
  concurrent label creation).
- Recovery (open pass 2) and replica apply verified by reading: they use the
  free fn `write_record_to_pool` / `Self::write_record_to_pool_inline`
  respectively — neither goes near `log_page_write`, so replay never records
  write sets. `record_write` also no-ops for the legacy tx-0 paths
  (`delete_node`'s del_tx=1 fallback, direct mutations outside a tx) because
  tx 0 is never in the active map.
- The 2 finding-#9 replication tests now provoke the conflict through real
  mutations (committed setup tx 1 creates node+property; concurrent txs 2/3
  both `set_node_property` the same key). The CommitBuffer-leak test's batch
  assertion now deserializes the shipped batch (`WalBatchMsg.records` is raw
  WAL bytes) and asserts no record of the losing tx ships.
- event_loop.rs's conflict test still injects via `tx_mgr.record_write`
  (updated to the 4-arg signature) — intentional: it tests the CommitResult
  response plumbing in isolation; real-mutation recording is covered by
  shard.rs's own tests. `Shard.tx_mgr` stays `pub(crate)` for exactly this
  kind of test (doc comment updated).
- Adjacent gap observed (pre-existing, NOT touched, may deserve a future
  task): property records are overwritten IN PLACE and carry no
  tx_min/tx_max, so an aborted tx's property bytes physically remain (the
  loser's value can be read back after its abort). Conflict detection now
  correctly reports the loser as failed, but property writes are not
  MVCC-versioned like node/edge records. Same class of pre-existing gap as
  index non-MVCC-awareness; out of this task's scope.
- Conflicts are now real at the Shard level, but still NOT triggerable
  through the cluster/wire — `ShardCluster`'s single global tx context
  serializes all transactions (Milestone 3's root cause, unchanged).

## Milestone 3 — Concurrent explicit transactions at the cluster (root cause: coordinator assumes one global tx context)

Discovered during task recon, required by the task's "conflict detection can
actually trigger" goal: `ShardCluster` serializes ALL transactions through one
`in_explicit_tx`/`current_tx` pair —
- a second concurrent `BEGIN` hits `assert!(!self.in_explicit_tx)` and panics
  the coordinator thread (poisoning the server's `Arc<Mutex<ShardCluster>>`,
  killing every connection);
- an auto-commit query arriving while another connection's explicit tx is
  open silently JOINS that tx (`begin_tx()` returns `current_tx` when
  `in_explicit_tx`), so its writes are attributed to (and can be rolled back
  with) a stranger's transaction;
- consequence: no two transactions are ever concurrently active, so a
  write-write conflict can never occur through the server today.

Design sketch (details for the implementing coder to finalize):
- `commit_explicit_tx(tx_id)` / `abort_explicit_tx(tx_id)` /
  `query_in_tx(tx_id, gql)` take the tx explicitly; server passes its
  per-connection `active_tx` (state it already tracks).
- `begin_explicit_tx()` drops the assert; shards' TransactionManager already
  supports N concurrently-active txs (`active: HashMap`), and the event loop
  already restores context via `SetActiveTx` — the coordinator API is the
  only blocker.
- Auto-commit queries always allocate their own tx (never join): the whole
  begin→execute→commit runs under one coordinator mutex hold, so a transient
  per-call tx context replaces the persistent global one.
- Server orphan-abort on disconnect passes the connection's tx_id.
- End-to-end regression tests over the wire protocol: (a) two connections
  BEGIN concurrently without panic; (b) conflicting SETs → loser's COMMIT
  returns ERROR, winner's value persists; (c) auto-commit query during
  another connection's explicit tx does not join it (its write commits
  independently and survives the other tx's ROLLBACK).

Implementation notes (finalized at implementation time):
- `current_tx`/`in_explicit_tx` stay as fields but become TRANSIENT per-call
  execution context for the executor's StorageAccess callbacks: `query_in_tx`
  sets them just before `executor::execute()` and clears them right after
  (both paths), so the existing begin/commit/abort no-op plumbing is reused
  unchanged. Between public calls both are always 0/false (`execute()` always
  ends in `commit_tx` or `abort_tx`, which reset `current_tx` on the
  auto-commit path).
- Conflict wire test must order writes so the WINNER writes last: property
  records are overwritten in place with no MVCC versioning (pre-existing gap,
  see M2 result), so the loser's physical bytes would otherwise be what a
  later read returns even though its commit correctly errored.

- [x] cluster.rs: `begin_explicit_tx()` drops the assert + persistent state;
      `commit_explicit_tx(tx_id)`, `abort_explicit_tx(tx_id)`,
      `query_in_tx(tx_id, gql)` take the tx explicitly; struct field doc
      comments updated to transient-context semantics
- [x] cluster.rs tests: update 7 existing call sites to new signatures; new
      tests: (a) two interleaved explicit txs, conflicting SETs → second
      committer Err + winner-writes-last value persists; (b) disjoint
      explicit txs both commit Ok; (c) auto-commit during an open explicit
      tx commits independently and survives that tx's abort
- [x] main.rs: CommitTx/RollbackTx/Query/orphan-abort handlers pass the
      connection's `active_tx` tx_id
- [x] wire_protocol.rs: test harness mirrors main.rs; new end-to-end tests
      (a)/(b)/(c) from the design sketch over two real client connections
- [x] replication.rs test: `abort_explicit_tx(tx_id)` call site updated
- [x] `cargo test --workspace` (313 baseline + new), `cargo clippy --workspace
      --all-targets -- -D warnings`, `cargo fmt --check`
- [x] CLAUDE.md Transaction Model bullet updated; todo.md M3 result written

### Milestone 3 — result (done)

319 tests passing (313 baseline + 6 new: cluster.rs
`concurrent_explicit_txs_conflicting_writes_second_committer_loses`,
`concurrent_explicit_txs_disjoint_writes_both_commit`,
`auto_commit_query_does_not_join_open_explicit_tx`; wire_protocol.rs
`two_connections_begin_concurrently_without_panic`,
`conflicting_commits_loser_gets_error_winner_persists`,
`auto_commit_on_other_connection_does_not_join_explicit_tx`),
`cargo clippy --workspace --all-targets -- -D warnings` clean,
`cargo fmt --check` clean. wire_protocol tests re-run 3x — stable.

Implemented per the design sketch + implementation notes above; no
deviations. Notes for the next coder:
- `ShardCluster`'s `current_tx`/`in_explicit_tx` fields were NOT removed:
  they became transient per-call execution context for the executor's
  `StorageAccess` callbacks (`query_in_tx` sets them immediately before
  `executor::execute()` and clears them right after, on both Ok and Err
  paths). Between public calls both are always 0/false — the auto-commit
  path's `commit_tx`/`abort_tx` reset `current_tx`, and `execute()` always
  ends in exactly one of them. This reuses the existing begin/commit/abort
  no-op plumbing unchanged rather than threading a tx parameter through
  graft-query's `StorageAccess` trait.
- `begin_explicit_tx()` still fans `BeginTx` out to ALL shards (every
  shard's TransactionManager needs the tx in its active map with a
  snapshot); `query_in_tx` still fans `SetActiveTx` out before parsing.
  Shard-side nothing changed — event_loop.rs's CommitTx/AbortTx arms were
  already tx_id-parameterized (they call `set_active_tx(tx_id)` first).
- Server (main.rs): CommitTx/RollbackTx handlers use
  `let Some(tx_id) = active_tx.take() else { ... }` — state cleared up
  front since the tx is over either way; Query passes `active_tx`'s tx_id;
  orphan-abort on disconnect passes the connection's tx_id. The
  "transaction already active" guard on BEGIN is per-connection wire
  protocol policy and stays.
- The conflict tests order writes so the WINNER writes last: property
  records are overwritten in place with no MVCC versioning (pre-existing
  gap, see M2 result), so with the loser writing last a read-back would
  return the loser's bytes even though its commit correctly errored. The
  commit Ok/Err assertions are ordering-independent; only the final-value
  assertion needs this.
- The M4 gap is unchanged and documented in `commit_explicit_tx`'s doc
  comment: no cross-shard prepare phase, so a multi-shard conflicting tx
  commits on non-conflicting shards while the client gets an error.
  M4 remains FLAGGED — needs orchestrator/human scope confirmation.

## Milestone 4 — Cross-shard commit atomicity (root cause: fan-out commit has no prepare phase) — DECLINED 2026-07-09, kept as known future work

Once Milestone 3 makes conflicts triggerable, a multi-shard tx that conflicts
on one shard aborts there but commits on the others — the client gets an
error while half its writes are visible. Fix: two-phase commit under the
coordinator mutex (which serializes all commits, making this sound):
`TransactionManager::validate(tx_id)` (extract the conflict-check loop),
`Request::PrepareCommit` fan-out first, then CommitTx-all or AbortTx-all.
This is adjacent scope beyond the literal task ask — get orchestrator/human
confirmation before implementing. Until then the partial-commit anomaly is a
documented, pre-announced gap (strictly better than today's silent lost
updates).

**Scope decision (2026-07-09)**: the user was asked and chose to STOP at the
delivered scope — M1-M3 were committed as `bc99b39` and pushed; M4 was
explicitly declined for now and remains future work with the fix sketch
above. The write-conflict task is COMPLETE as delivered.

## Verification (every milestone)

- `cargo test --workspace` green (306 at baseline)
- `cargo clippy --workspace --all-targets -- -D warnings` clean
- `cargo fmt --check` clean

---

# Phase 8f Replication — Code Review Fix Plan (PREVIOUS TASK — COMPLETE)

Source: `tasks/code-review-findings.md` (15 confirmed findings, ranked by severity).
Baseline: 278 tests passing on main.

Findings are grouped into 3 milestones by root cause. One milestone per coder rotation.

## Milestone 1 — Network transport & delivery reliability (findings #1, #3, #4, #5, #10, #11, #12, #14, #15)

Root cause: the primary/replica TCP transport layer (`graft-server/src/replication.rs`)
used a single shared per-shard outbox drained racily by all replica connections, an
unstable replica identity (peer ip:ephemeral-port), no shard-count/cluster_id
validation, a blocking handshake in the accept loop, and no liveness detection —
all in the same ~350-line file and tightly interdependent.

Design (see full rationale in session — persisted to continue.txt):
- **Stable replica identity**: replica generates/loads a persistent `replica_id`
  (random u64 hex, persisted to `<data_dir>/replica_id` for durable replicas,
  in-memory for ephemeral ones, stable across reconnects within a process).
- **cluster_id**: primary generates/loads a persistent `cluster_id`
  (`<data_dir>/cluster_id` or in-memory). Replica adopts it on first connect
  (cluster_id=0 sentinel), persists/remembers it, and the primary REJECTS any
  future connection whose cluster_id doesn't match (fixes #15).
- **shard_count negotiation**: hello carries `shard_count`; primary rejects a
  mismatched replica instead of silently dropping half the batches (fixes #4).
- **Per-replica outbox via `ReplicaOutboxRegistry`**: `Arc<Mutex<HashMap<replica_id,
  SharedQueue<WalBatchMsg>>>>` per shard, created once at cluster construction.
  Both the network thread (on connect, `get_or_create`) and the shard's
  `ReplicationSender` (via `ReplControl::Register{id, shard_id, outbox}`) resolve
  through it, so a reconnecting replica is handed the SAME queue it had before —
  no data loss across reconnects, and N replicas each get their own full stream
  (fixes #1's fan-out bug and the reconnect-data-loss sub-bug).
- **Bounded per-replica buffering**: `ReplicationSender::poll()` fans each new
  batch out to every registered replica's queue, capped at 1GB per replica
  (`MAX_REPLICA_OUTBOX_BYTES`); over the cap, the replica is evicted (logged) and
  must fully resync on reconnect — bounded memory, explicit failure mode instead
  of OOM (fixes #10). Zero-ever-registered replicas retain nothing (also #10).
- **Retransmission**: writer threads use peek-front/pop-front (not destructive
  drain) so a send error leaves the batch queued for retry after reconnect
  (fixes #3).
- **Threaded handshake**: accept loop spawns a thread immediately per connection
  instead of doing the blocking hello exchange inline (fixes #11).
- **Replica read timeout + stop signaling**: replica sets a 5s read timeout
  (primary heartbeats every 1s) on its primary connection; on timeout/error it
  sets a shared `should_stop` flag that `replica_ack_writer` polls, so both
  threads exit and the outer loop's reconnect/backoff actually runs (fixes #12
  and #5 together — #12 makes the reader notice a dead primary, #5's join-hang
  needs the writer to also notice and exit).

Touches: `graft-repl/src/{shared_queue,registry(new),sender,protocol,lib}.rs`,
`graft-runtime/src/{event_loop,cluster}.rs`, `graft-server/src/{replication,identity(new),main}.rs`.

- [x] `SharedQueue`: add `len`, `is_empty`, `peek_front`, `pop_front`, `total_bytes_by`
- [x] `ReplicaOutboxRegistry` (new `registry.rs`)
- [x] `ReplControl::Register` carries `outbox`, drops `last_lsn`
- [x] `ReplHelloMsg` restructured: `replica_id`, `cluster_id`, `role`, `shard_count`
- [x] `ReplicationSender`: per-replica outbox + eviction bound in `poll()`,
      `add_replica(id, shard_id, outbox)` reconnect-preserves state
- [x] `event_loop.rs`: drop shared `repl_outbox`, wire `Register`'s outbox through
- [x] `cluster.rs`: `ShardReplQueues.outbox_registry` created per shard in
      `new_primary`/`open_primary`
- [x] `graft-server/src/identity.rs`: `load_or_create_cluster_id`,
      `ReplicaIdentity::load_or_create` (+ `random_u64` via `RandomState`, zero new deps)
- [x] `replication.rs` rewrite: threaded handshake, cluster_id/shard_count
      validation + rejection, per-replica registry lookup, peek/pop-front writer,
      replica read timeout + should_stop signaling
- [x] `main.rs`: thread identity resolution into `run_primary_listener` /
      `run_replica_connector`
- [x] Regression tests: multi-replica fan-out (each gets full stream), reconnect
      preserves queued backlog, shard-count mismatch rejected, cluster_id
      mismatch rejected, unregistered-primary produces no unbounded growth
- [x] `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt`

## Milestone 2 — LSN persistence + corrupted-batch handling (findings #2, #6)

- [x] #2: persist `repl_next_lsn` durably (survives primary restart) so replicas
      don't silently stop receiving batches after a primary restart.
- [x] #6: `on_batch` CRC/truncation errors are surfaced (logged loudly) and the
      receiver is poisoned so no later batch can silently bridge the gap.

### Milestone 2 — result (done)

299 tests passing (294 baseline + 5 new regression tests), `cargo clippy
--workspace --all-targets -- -D warnings` clean, `cargo fmt --check` clean.
`graft-server` real-TCP tests re-run 3x — stable.

**#2 — design deviation from the continue.txt starting hypothesis, with
rationale**: the prior session's handoff suggested persisting
`repl_next_lsn`'s *current* value on the WAL group-commit fsync cadence.
Implemented instead: a **reservation-ceiling allocator**, because the
group-commit-cadence idea doesn't actually satisfy "never reuse an LSN
already shipped" — WAL records are shipped to replicas as soon as a
transaction commits (`poll_replication` drains `repl_log` every event-loop
tick), completely independent of the WAL's own fsync timer. A batch can
reach and be applied by a replica well before the corresponding fsync
happens, so persisting only at fsync time could still let a restarted
primary reissue (and thus have a replica silently skip) an LSN it already
shipped in the previous life.

Implemented in `crates/graft-runtime/src/shard.rs`:
- `REPL_LSN_RESERVE_BATCH = 10_000` (doc comment explains the design).
- New `Shard` fields: `repl_lsn_ceiling: u64` (in-memory), `repl_lsn_file:
  Option<FileHandle>` (handle to `<shard_dir>/repl_lsn.meta`).
- `Shard::next_repl_lsn(&mut self) -> u64`: when `repl_next_lsn` would
  exceed the current ceiling, persists (write + fsync) a new ceiling
  `REPL_LSN_RESERVE_BATCH` ahead *before* handing out the LSN, then hands
  out LSNs from the reserved range with no further syncs until the next
  crossing. Replaced the 5 duplicated `let lsn = self.repl_next_lsn; ...`
  call sites (log_page_write, log_label_write, begin_tx_with_id,
  commit_current_tx, abort_current_tx) with this one method.
- `Shard::open_with_io()`: opens `repl_lsn.meta`, reads the persisted
  ceiling (0 if never a primary before), and resumes `repl_next_lsn` from
  `ceiling + 1` instead of hardcoding 1.
- Cost: one extra fsync per 10,000 LSNs issued (negligible), and up to
  9,999 "wasted" (never-issued, harmless) LSN values on a crash — u64 LSN
  space makes that irrelevant at any realistic scale.
- **Platform gotcha found and fixed along the way**: `crates/graft-io/src/uring.rs`'s
  `is_data_file()` opens anything not literally ending in `.wal` with
  O_DIRECT on Linux. An 8-byte unaligned `write_at`/`read_at` on an
  O_DIRECT fd fails with EINVAL on real O_DIRECT-capable filesystems (only
  the EINVAL-at-open fallback for tmpfs-like filesystems would have saved
  it). Fixed by adding a `.meta` extension case to `is_data_file()`
  (alongside the existing `.wal` case) and naming the new file
  `repl_lsn.meta` rather than `repl_lsn.dat`. **Not locally verified** —
  `uring.rs` is `#[cfg(target_os = "linux")]`-gated and this dev machine is
  macOS; no Linux target/toolchain available to cross-compile-check it.
  `cargo fmt --check` did parse it successfully (rustfmt doesn't evaluate
  `cfg`), and the change is a single match-arm line identical in shape to
  the pre-existing `.wal` arm — low risk, but flagging since it's the one
  piece of this milestone that couldn't be built or tested on this machine.
- Regression tests (`crates/graft-runtime/src/shard.rs`, `shard::tests`):
  `repl_lsn_starts_at_one_for_a_never_replicated_durable_shard`,
  `repl_lsn_survives_reopen_and_never_goes_backward_or_is_reused` (opens a
  durable shard, issues LSNs, drops it, reopens at the same dir — asserts
  the next LSN is strictly greater, and specifically equals
  `REPL_LSN_RESERVE_BATCH + 1`, proving it resumed from the *reserved*
  ceiling, not the last-issued value), `repl_lsn_reservation_bounds_extra_fsyncs_across_many_records`
  (50 transactions in one run still only cross the reservation boundary
  once).

**#6 — design deviation from the continue.txt starting hypothesis, with
rationale**: the prior session's handoff suggested forcing a TCP reconnect
on a corrupted batch. Analyzed and decided against it: the primary's
per-replica outbox (`SharedQueue`) pops a batch on successful `send()`
*before* network delivery is confirmed, so in the most plausible corruption
scenarios (bit-rot in transit, a receiver-side deserialization bug) the
specific corrupted batch's bytes are already gone from the primary's queue
by the time any reconnect would happen — reconnecting buys no actual data
recovery. It would only add a new cross-thread signaling path (shard event
loop thread has no handle to the TCP socket, owned by
`graft-server/src/replication.rs`'s network threads) for a behavior
(connection churn) that doesn't fix anything a plain poisoned-and-halted
state doesn't already achieve more simply and more visibly.

Implemented instead — **poison the receiver, halt forward progress, log
loudly**:
- `crates/graft-repl/src/receiver.rs`: `ReplicationReceiver` gained a
  `poisoned: Option<String>` field. `on_batch()` returns an error
  immediately (without touching `pending_records`/`pending_acks`/
  `last_applied_lsn`) if already poisoned; on a CRC/truncation error it
  sets `poisoned` before returning `Err`. New `is_poisoned()` /
  `poison_reason()` accessors. This is the core fix: once poisoned, *no*
  later batch — even a perfectly valid, higher-LSN one — can be applied or
  ACKed, so the gap can never be silently bridged. `SHOW REPLICATION LAG`
  (existing, Phase 8f-transport tooling) naturally reflects the stall since
  `acked_lsn` stops advancing — no new admin surface needed.
- `crates/graft-runtime/src/event_loop.rs` (`poll_replication`, replica
  path): replaced `let _ = receiver.on_batch(batch);` with a loop that logs
  `tracing::error!` (shard_id, batch's last_lsn, the error) exactly once —
  on the tick the corruption is first detected — then breaks; on later
  ticks, `receiver.is_poisoned()` is checked first so an already-poisoned
  receiver skips `on_batch` entirely instead of logging the same rejection
  every tick forever.
- **Known limitation, explicitly accepted** (matches the "no resync
  mechanism yet" scope note from continue.txt / finding #6): poisoning is
  in-memory only, cleared by a process restart. A corrupted batch's data is
  NOT recoverable within this phase's scope either way (Phase 8g —
  snapshot shipping / full resync — doesn't exist yet), so a restarted
  replica would quietly resume from a higher LSN, permanently missing the
  gap, with no further warning. This is a real gap but out of Milestone 2's
  scope (LSN corruption is expected to be extremely rare — CRC catches
  transit/deserialization bugs, not routine failures — and persisting a
  poison marker durably plus a startup guard is a meaningfully bigger
  feature than "log loudly and stop"). Flagging for whoever builds Phase
  8g: the resync mechanism should also address this "poison must survive
  restart" gap.
- Regression tests: `crates/graft-repl/src/receiver.rs`
  `corrupted_batch_poisons_receiver_and_blocks_all_later_batches` (a valid
  higher-LSN batch sent after a corrupted one is also rejected; nothing
  applied or ACKed), plus an added assertion in the existing
  `receiver_detects_crc_corruption`. `crates/graft-runtime/src/event_loop.rs`
  `poll_replication_replica_stops_applying_after_corrupted_batch`
  (full event-loop-level check: pushes a corrupted batch then a valid one
  into the inbox, ticks once, asserts neither transaction's node exists in
  the shard and no ACK was sent).

## Milestone 3 — MVCC / tx-counter correctness on replicas (findings #7, #8, #9, #13)

**Design decision — deviates from continue.txt's two options (a)/(b), with
rationale.** Neither "reserve a gap" (probabilistic, not airtight — a long
enough replica query + fast enough primary could still close the gap) nor
"route through an explicit snapshot LSN boundary" (correct but a much bigger
change: new snapshot semantics, not just counter arithmetic) is necessary.
The actual root cause is that replica-local read-only snapshot tx_ids and
real primary-issued tx_ids share ONE numbering space. Splitting that space
into two **disjoint, non-overlapping ranges by construction** — the same
"reserve bits for a purpose" pattern this codebase already uses for NodeId's
shard encoding — removes the race entirely instead of bounding it:

- `crates/graft-runtime/src/cluster.rs`: new `const REPLICA_TX_ID_BASE: TxId
  = 1 << 63`. `new_replica()`/`open_replica()` initialize `next_tx_id:
  AtomicU64::new(REPLICA_TX_ID_BASE)` instead of `1`/`max_tx`. A primary
  would need to commit over 2^63 transactions to ever reach this range —
  physically impossible at any realistic scale (billions of years even at
  1B commits/sec) — so no replica-issued tx_id can *ever* equal a real
  primary/replicated tx_id, not "probably won't", structurally can't.
- Because the ranges never overlap, a replica's `next_tx_id` counter needs
  **no synchronization with shard-level state at all**. `sync_tx_counter()`
  (the TOCTOU-racy `GetNextTxId` round-trip) is deleted entirely — not
  gated behind a role check, just gone — along with its only caller
  (`begin_tx()`'s `self.sync_tx_counter();`) and its only request/response
  plumbing (`Request::GetNextTxId` / `Response::TxId` in cluster.rs,
  the matching arm in event_loop.rs). This fixes #7 (no collision is
  possible, so the own-write-visibility bypass in `Shard::is_record_visible`
  can never be tricked by in-flight replicated data) and #13 (zero per-query
  cost on every role, not just "skipped on non-replica" — strictly better
  than gating).
- #8 (`begin_explicit_tx` skipping the sync) needed **no code change at
  all**: it already just does `fetch_add(1)` on `next_tx_id`, so once
  construction seeds the right starting range, both the auto-commit and
  explicit-BEGIN paths are automatically correct with the same code.
  Decided **not** to add a read-only guard rejecting `BeginTx` on replicas
  (the secondary note in finding #8): multi-statement consistent reads via
  explicit BEGIN are a legitimate replica feature, and the collision risk
  that motivated the suggestion no longer exists. Write statements inside
  such a transaction are already rejected at the wire layer
  (`graft-server/src/main.rs`'s `is_read_only` check covers all `Query`
  messages, tx or not).
- No `is_replica` flag was added to `ShardCluster` — deliberately: nothing
  needs to branch on role anymore once construction seeds the right base,
  so an unused flag would be speculative state (CLAUDE.md: no
  "flexibility" that wasn't requested).
- **Known forward-looking gap, out of scope here**: if/when Phase 8h
  (automatic failover / `PROMOTE REPLICA`, currently a stub in
  `graft-server/src/admin.rs`) turns a replica into a primary in-process,
  its `next_tx_id` counter would still be in the high range and a
  *downstream* replica of *that* newly-promoted primary would also start
  its own counter at the same `REPLICA_TX_ID_BASE` — a real (if narrow)
  collision surface for chained replication. Not reachable today (no
  chaining, no real promotion logic exists yet). Flagging for whoever
  builds 8h.

- [x] #7/#8: disjoint reserved tx_id range for replica clusters (see above);
      `sync_tx_counter()` deleted.
- [x] #9: `commit_current_tx` (`crates/graft-runtime/src/shard.rs`) now
      captures `tx_mgr.commit()`'s `Result` and ships a replication `Abort`
      record (not `Commit`) when it's `Err`. **Important subtlety found
      while implementing**: shipping *nothing* (just skipping the Commit
      push, the other option `continue.txt` raised) would have been wrong —
      `CommitBuffer::append` (`crates/graft-repl/src/commit_buffer.rs`) only
      resolves a buffered tx_id on seeing an explicit `Commit` or `Abort`
      record; with neither, the tx's buffered records sit in
      `CommitBuffer.pending` forever — an unbounded leak per WriteConflict,
      which is exactly the class of bug (`no unbounded memory`) this
      review's own verification requirements call out. Shipping an explicit
      Abort is the only option that's both correct and bounded.
      **Also discovered, explicitly out of scope**: `Shard` never actually
      calls `TransactionManager::record_write()` anywhere in its mutation
      methods (`create_node`, `set_node_property`, etc.), so
      `tx_mgr.commit()` can never actually observe a write-write conflict
      through any real code path today — `tx.write_set` is always empty.
      Finding #9's bug is real and correctly fixed, but currently latent
      (unreachable in production) until conflict detection is actually
      wired into `Shard`'s mutation API — a separate, much larger gap this
      milestone does not touch. Tests below construct the conflict by
      calling `tx_mgr.record_write()` directly (legal — same-module private
      field access in `shard.rs`'s own test block) to exercise the fix.
- [x] #13: resolved as a side effect of the #7/#8 design — `sync_tx_counter`
      cost is zero on every role now, not merely gated off for non-replicas.

### Milestone 3 — result (done)

306 tests passing (299 baseline + 7 new regression tests), `cargo clippy
--workspace --all-targets -- -D warnings` clean, `cargo fmt --check` clean.
`graft-server` tests re-run 5x (3x full crate, 2x the new timing-sensitive
integration test in isolation) — stable, no flakiness observed.

**All 15 findings from `tasks/code-review-findings.md` are now fixed** —
this was the last milestone. Nothing has been committed to git yet across
all 3 milestones; everything remains uncommitted working-tree changes (the
user hasn't asked for a commit).

Changed in this milestone:
- `crates/graft-runtime/src/cluster.rs`: added `const REPLICA_TX_ID_BASE:
  TxId = 1 << 63` with full design-rationale doc comment. `new_replica()`
  and `open_replica()` now seed `next_tx_id: AtomicU64::new(REPLICA_TX_ID_BASE)`
  instead of `1`/a shard-derived `max_tx` — `open_replica()`'s per-shard
  `max_tx` tracking loop was removed entirely since nothing needs it anymore.
  Deleted `sync_tx_counter()` (dead after this change), its call site in
  `begin_tx()`, and its only request/response plumbing: `Request::GetNextTxId`
  / `Response::TxId` (also removed from `crates/graft-runtime/src/event_loop.rs`'s
  match arm). Added 4 regression tests to `cluster.rs`'s existing test module.
- `crates/graft-runtime/src/shard.rs`: `commit_current_tx()` now captures
  `tx_mgr.commit()`'s `Result` (was `let _ =`) and ships a replication
  `Abort` record instead of `Commit` when it's `Err` — fixes #9. Added 2
  regression tests that provoke a real `WriteConflict` by calling
  `tx_mgr.record_write()` directly from the test module (same-module private
  field access), since `Shard` itself never calls `record_write` from any
  mutation method today (see "discovered, out of scope" note below).
- `crates/graft-server/tests/replication.rs`: added 1 integration test
  reproducing the exact adversarial scenario finding #7 describes (primary's
  first tx always gets tx_id 1) end-to-end over the real
  primary-to-replica queue pipeline.

**Design decision, deviates from continue.txt's two suggested options —
full rationale in the Milestone 3 section above.** Neither "reserve a gap"
(bounded but not airtight) nor "route through a snapshot LSN boundary"
(correct but a much bigger change) was implemented. Instead: split the u64
TxId space into two ranges that can never overlap by construction (bit 63),
the same "reserve bits for a purpose" pattern this codebase already uses for
NodeId's shard encoding. This eliminates `sync_tx_counter()`'s TOCTOU race
entirely rather than narrowing it, fixes #8 with zero code change (the
existing `fetch_add(1)` in `begin_explicit_tx` was already correct once
construction seeds the right base), and fixes #13 more completely than
"gate to replica-only" would have (zero cost on *every* role, including
replicas, not just skipped on non-replicas).

**Two gaps discovered while implementing, both explicitly out of scope,
both documented in code comments at their exact location:**
1. `Shard` never calls `TransactionManager::record_write()` from any
   mutation method (`create_node`, `set_node_property`, etc.) — so
   `tx_mgr.commit()` can never actually observe a write-write conflict
   through any real code path today. Finding #9's fix is correct and real,
   but currently latent (unreachable in production) until conflict
   detection is wired into `Shard`'s mutation API. That wiring is a
   separate, much larger feature this milestone does not touch.
2. `commit_current_tx()`'s `WriteConflict` `Result` is still not surfaced
   to the wire-protocol client — `event_loop.rs`'s `Request::CommitTx`
   handler and `cluster.rs`'s commit paths all treat every commit as
   unconditionally successful. A client is never told its commit lost a
   conflict. Pre-existing, not part of any of the 15 findings, documented
   in `commit_current_tx()`'s doc comment.
3. (Forward-looking, not a bug today) If Phase 8h ever implements real
   `PROMOTE REPLICA` (currently a stub in `graft-server/src/admin.rs`), a
   promoted replica's `next_tx_id` counter would still be in the high
   range, and a downstream replica of *that* newly-promoted primary would
   independently start its own counter at the same `REPLICA_TX_ID_BASE` —
   a narrow collision surface for chained replication. Not reachable today
   (no chaining, no real promotion logic exists yet).

## Verification (every milestone)

- `cargo test` green (278 at baseline, expect growth from new regression tests)
- `cargo clippy -- -D warnings`
- `cargo fmt` clean

## Milestone 1 — result (done)

294 tests passing (278 baseline + 16 new regression tests), `cargo clippy
--workspace --all-targets -- -D warnings` clean, `cargo fmt --check` clean.

Note: the local toolchain's clippy is newer than whatever produced the 278-test
baseline, so `cargo clippy -- -D warnings` was *not* actually clean on `main`
before this milestone — 6 pre-existing, unrelated lint errors in
`graft-core`, `graft-storage`, `graft-query`, `graft-runtime/database.rs`, and
`graft-server/tests/wire_protocol.rs` (approx-PI-constant test literals,
`map_or(true, ..)` → `is_none_or`, a redundant `.map_err(|e| e)`, an
assert-on-constants test, an unused import/var, a while-let-loop suggestion).
Fixed all of them (mechanical, `#[allow]`-annotated where the literal value is
intentional test data, not an approximation of PI) since the task's
verification gate requires a clean clippy run — confirmed via `git stash` that
none of these were introduced by this milestone's changes.
