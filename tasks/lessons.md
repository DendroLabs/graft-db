# Lessons Learned

## `WalBatchMsg.records` is raw serialized WAL bytes, not structured records

When a test needs to assert on the *contents* of a shipped replication batch
(e.g. "no record of the losing tx may ship"), `batch.records` cannot be
iterated as `WalRecord`s — it's a `Vec<u8>` of WAL-format bytes
(header + body + CRC per record, built by `sender.rs`'s
`records_to_batch`). Deserialize first with
`graft_repl::receiver::deserialize_wal_records(&batch.records)`. Caught
before a build cycle this time only because the struct was checked before
writing the assertion — the field name reads like a `Vec<WalRecord>`.

## `cargo clippy -- -D warnings` was not actually clean on `main` before this work

**Problem**: Running the mandated verification command
(`cargo clippy --workspace --all-targets -- -D warnings`) failed with ~14
errors spread across `graft-core`, `graft-storage`, `graft-query`, and
`graft-runtime` — none of them related to the replication work in progress.

**Why it was confusing**: it looked at first like my own changes had broken
something in unrelated crates I hadn't touched (e.g. `graft-query/executor.rs`
`map_or(true, ..)`, `graft-core/constants.rs` "assertion on constants",
several `3.14`-is-close-to-PI lints).

**Root cause**: the installed clippy/rustc toolchain is newer than whatever
was used when the 278-test baseline was established, and newer clippy
versions added lints (`unnecessary_map_or` → suggests `is_none_or`,
`assertions_on_constants`, etc.) that the old baseline never had to satisfy.

**Fix**: verified via `git stash && cargo clippy ... && git stash pop` that
the exact same errors exist on `main` with zero changes applied — confirming
they were pre-existing and unrelated. Fixed them anyway (mechanical,
low-risk: `#[allow(clippy::approx_constant)]` on tests using `3.14` as
intentional test data, `map_or(true, ..)` → `is_none_or(..)`, removed a
redundant `.map_err(|e| e)`, `#[allow(clippy::assertions_on_constants)]` on a
compile-time-invariant test, prefixed an unused var with `_`, removed an
unused import, converted a manual loop to `while let`) because the task's
verification gate explicitly requires a clean clippy run — leaving it broken
would mean the gate can never pass for any future milestone either.

**How to avoid next time**: when `cargo clippy -- -D warnings` fails, always
check with `git stash` whether the failures pre-exist on `main` before
assuming your own change caused them. If they're unrelated but block the
required verification gate, fix them (small, mechanical fixes only) rather
than skipping the gate — but say so explicitly in the handoff/report so
reviewers know why "unrelated" files changed.

## Multiplexed single-connection replication needs registry-mediated queue handoff, not per-connection queue creation

**Problem**: the original bug (finding #1) was a single shared
`SharedQueue<WalBatchMsg>` per shard, created once at cluster-construction
time and drained racily by every replica connection's writer thread. The
naive fix — "just create a new queue per connection" — doesn't work, because
a reconnecting replica needs the *same* queue it had before (to not lose
batches queued while disconnected), but the network thread (which spawns the
writer and needs a queue handle immediately) and the shard's
`ReplicationSender` (which is the source of truth for "does this replica_id
already have a queue") live on different OS threads with no synchronous
request/response channel between them.

**Fix**: `ReplicaOutboxRegistry` (`crates/graft-repl/src/registry.rs`) is an
`Arc<Mutex<HashMap<replica_id, SharedQueue<WalBatchMsg>>>>` per shard, created
once at cluster-construction time (like the old single outbox was). Both
sides resolve through `get_or_create(id)`: the network thread on connect (to
get a handle for the writer thread), and the shard-side `ReplicationSender`
via the `ReplControl::Register{id, shard_id, outbox}` message (the network
thread passes the SAME resolved handle through). Because both sides go
through the same registry, they're automatically consistent — no
synchronous round-trip needed.

## Read-timeout-as-liveness-detection only works if the peer promises periodic traffic

Considered adding a read timeout to `primary_reader` (primary's ACK-reading
loop) symmetric to the replica's read timeout fix (finding #12), but did NOT
do it: the replica only sends WalAck messages when there's something to ack,
never a periodic heartbeat back to the primary. A read timeout there would
have falsely killed healthy-but-idle connections. The replica-side fix works
specifically because the *primary* already sends a `ReplStatusMsg` heartbeat
every 1s when idle (pre-existing code) — so the replica can safely treat "no
read in 5s" as "primary is gone." Don't add a read-timeout-based
liveness check on a leg of a connection where the peer has no periodic
traffic guarantee.

## `IoUringBackend` opens anything not named exactly `*.wal` with O_DIRECT — unaligned small metadata writes will EINVAL on real filesystems

**Problem**: Milestone 2 (finding #2) needed a small dedicated file to
persist a replication LSN counter (8 bytes, unaligned, written via a single
`write_at(fh, 0, &bytes)` + `sync(fh)`). The obvious name, `repl_lsn.dat`,
would have compiled and passed every test on this dev machine (macOS) but
been silently broken in production (Linux): `crates/graft-io/src/uring.rs`'s
`is_data_file()` opens anything whose extension isn't literally `"wal"` with
O_DIRECT, and O_DIRECT requires page-aligned offsets *and* lengths for every
read/write on that fd — an 8-byte write at offset 0 fails with EINVAL on any
real O_DIRECT-capable filesystem (ext4, xfs). The EINVAL-fallback-to-buffered
path in `uring.rs` only triggers on `open()` (for filesystems like tmpfs that
reject O_DIRECT outright), not on a per-write EINVAL — so this would not be
caught by the existing fallback, and would not surface on macOS at all since
`uring.rs` is `#[cfg(target_os = "linux")]`-gated and never compiles here.

**Why it was easy to miss**: nothing about the `IoBackend` trait signature or
the `PosixIoBackend`/`SimIoBackend` implementations hints at this — both are
totally alignment-agnostic. The constraint is invisible unless you
specifically go read `IoUringBackend::is_data_file()` and know that O_DIRECT
demands alignment. A dev-machine-only (macOS) session would never hit the
failure locally; it would only show up as a mysterious EINVAL on a Linux
production primary.

**Fix**: extended `is_data_file()` with a `.meta` extension case (alongside
the existing `.wal` case) that also skips O_DIRECT, and named the new file
`repl_lsn.meta` instead of `repl_lsn.dat`.

**How to avoid next time**: before choosing a filename/extension for any new
file opened through `Shard`'s `io: Box<dyn IoBackend + Send>`, check
`is_data_file()` in `crates/graft-io/src/uring.rs`. Anything that isn't a
128-record page-aligned bulk data file (i.e. anything that isn't exactly
like `nodes.dat`/`edges.dat`/`props.dat`/`labels.dat`) — small counters,
metadata, sequence files — needs a non-O_DIRECT extension (`.wal` or the new
`.meta`), or `is_data_file()` needs to grow another case. This can't be
caught by local tests on a non-Linux dev machine; it has to be checked by
reading the code, not by running it.

## RefCell field borrows don't compose across method-call boundaries even when the methods only touch disjoint fields

**Problem**: refactoring 5 duplicated `let lsn = self.repl_next_lsn; self.repl_next_lsn += 1;`
call sites in `crates/graft-runtime/src/shard.rs` into one
`self.next_repl_lsn(&mut self)` helper broke compilation in exactly the two
call sites (`begin_tx_with_id`, `abort_current_tx`) that hold a
function-scoped (not block-scoped) `let wal_ref = self.wal.as_ref().map(|w| w.borrow_mut());`
binding: `error[E0502]: cannot borrow *self as mutable because it is also
borrowed as immutable`.

**Why it was surprising**: `next_repl_lsn` only ever touches `self.io` /
`self.repl_lsn_file` / `self.repl_lsn_ceiling` / `self.repl_next_lsn` —
completely disjoint from `self.wal`, which is what `wal_ref` borrows. RefCell
borrow tracking is a *runtime* mechanism, so it's tempting to assume the
compiler will similarly "see through" to the fact that there's no real
conflict. It doesn't: `self.wal.as_ref()` is a plain compile-time immutable
borrow of the `self.wal` *field* (producing `&RefCell<WalWriter>`, stored
inside `wal_ref` for the rest of the function), and calling any `&mut self`
method requires the compiler to be able to prove no part of `self` is
borrowed — it can't see inside `next_repl_lsn` to know it only touches other
fields, so it conservatively rejects the whole call.

**Fix**: added an explicit `drop(wal_ref);` right before the `if
self.repl_enabled { ... }` block in both functions, once `wal_opt` (the
value actually needed by `tx_mgr.begin_with_id`/`abort`) had already been
consumed. The two call sites that scope the borrow inside an explicit `{ }`
block (`log_page_write`, `log_label_write`, `commit_current_tx`) never hit
this because the borrow's lifetime already ends before reaching the
`repl_enabled` check.

**How to avoid next time**: when introducing a new `&mut self` helper method
call inside a function that already holds a `RefCell::borrow_mut()` result in
a *function-scoped* (not block-scoped) local, either scope the existing
borrow in an explicit `{ }` block that ends before the new call, or
explicitly `drop()` it — don't assume disjoint-field reasoning will save you
once a method call is involved.

## A TOCTOU race can sometimes be eliminated by construction instead of narrowed by synchronization — check for that before adding a sync mechanism

**Problem (Milestone 3, finding #7)**: the review's own handoff (`continue.txt`)
proposed two options for fixing a replica-query-tx_id-collides-with-a-
replicated-primary-tx_id race: (a) "sync to max + a large gap" (bounds the
race but isn't airtight — a long enough query + fast enough primary could
still close the gap) or (b) "route reads through an explicit snapshot LSN
boundary" (correct but a much bigger change — new snapshot semantics, not
just counter arithmetic). Both assumed the fix had to live in *when*/*how
often* to synchronize two counters that share one numbering space.

**What actually worked**: neither. Since a replica's own read-only snapshot
tx_ids never need to be durable, never need to compare meaningfully against
real tx_ids except via strict `<`, and only exist to make MVCC's `is_visible`
math work — they didn't need to share a numbering space with real tx_ids at
all. Splitting the u64 `TxId` space into two ranges that can never overlap
*by construction* (top bit reserved, `1 << 63`) removed the race entirely: a
primary would need 2^63 real commits to ever reach that range, which is not
a "probably never" bound, it's a "cannot happen at any physically realizable
scale" bound. This is the same pattern the codebase already uses for
`NodeId`'s shard-id-in-upper-bits encoding — recognizing that made the
design obvious in retrospect. It also happened to make two *other* findings
(#8, #13) disappear for free: #8 (`begin_explicit_tx` missing a sync call)
needed no code change once construction seeded the right range; #13
(`sync_tx_counter`'s per-query cost even on primary/standalone) resolved
because the whole mechanism could be deleted, not just gated.

**How to avoid missing this next time**: when a proposed fix for a race is
"synchronize more carefully" or "narrow the window", first ask whether the
two things racing actually *need* to share the same value space at all. If
one side's values are synthetic/internal (never persisted, never compared
to the other side except by ordering) and the space is large enough,
partitioning into disjoint reserved ranges can turn a probabilistic
bound into a structural guarantee — and often deletes code (a whole sync
mechanism) rather than adding it. Don't reach for "add synchronization" as
the default response to "these two counters need to agree."

## Test coverage can prove a fix's *logic* is correct even when the bug's real-world trigger path doesn't exist yet — say so explicitly

**Problem (Milestone 3, finding #9)**: the finding was "on `WriteConflict`,
`commit_current_tx` discards the error and ships a false replication Commit."
Writing a regression test required provoking an actual `WriteConflict` from
`TransactionManager::commit()`, which only happens if two transactions wrote
to the same `(page_id, slot)` — tracked via `TransactionManager::record_write()`.
Grepping the codebase turned up zero call sites of `record_write` anywhere in
`Shard`'s mutation methods (`create_node`, `set_node_property`, etc.) — so
`tx.write_set` is *always* empty for every real transaction today, and
`tx_mgr.commit()` can never actually return `Err` through any code path a
user query can reach. The bug being fixed is real (correct logic, correctly
guards a real error type) but currently unreachable in production.

**Why this mattered**: it would have been easy to either (a) skip testing
this finding as "can't construct the scenario," silently under-delivering on
the task's "new regression tests for each fixed failure scenario" requirement,
or (b) go fix the missing `record_write` wiring too, silently expanding scope
far beyond the 4 assigned findings into a whole concurrency-conflict-detection
feature.

**What worked**: test the fix's *logic* directly by calling the
lower-level API (`shard.tx_mgr.record_write(...)`, a private field accessible
from the same-module `mod tests` block) to construct the exact state
`tx_mgr.commit()` needs to return `Err`, independent of how a real caller
would eventually reach that state. This validates the fix is correct without
either skipping coverage or expanding scope — and the discovery itself (the
missing wiring) is valuable enough to be worth flagging explicitly in the
handoff/task doc as a separate, real, out-of-scope gap, not silently ignored.

**How to avoid next time**: when a regression test for a specific bug fix
needs a precondition that the current public API can't produce, check
whether a lower-level/private entry point in the same module can construct
it directly. That's a legitimate way to test fix *logic* in isolation from a
*trigger path* that doesn't exist yet — just say so explicitly, both in the
test's comments and in the handoff, so nobody mistakes "the fix is tested"
for "the bug is currently reachable in production."

## A `replace_all` edit after targeted edits can silently clobber them when the pattern is a substring of the edited line

**Problem**: converting ~10 test call sites of `shard.commit_current_tx();` to
`.unwrap()` via replace_all ALSO matched inside a line a targeted edit had
just created (`let result = shard.commit_current_tx();` — the pattern is a
substring), turning it into `let result = ...unwrap();` and breaking the
test's type (`()` vs `Result`), caught only at the next build.

**How to avoid next time**: do the broad `replace_all` FIRST, then apply the
targeted exceptions on top — or make the replace pattern anchored enough
(include leading indentation/context) that it can't match inside lines other
edits produced.

## Conflict-test value assertions must account for non-MVCC-versioned records: order writes so the winner writes LAST

**Problem (Milestone 3, write-conflict task)**: the natural way to write "loser's
commit errors, winner's value persists" is winner-writes-then-loser-writes.
With graft's current storage that test would fail its final-value assertion:
property records are overwritten IN PLACE with no tx_min/tx_max (pre-existing
gap noted in M2's result), so whichever transaction physically wrote last is
what a later read returns — even if that transaction's commit correctly came
back as a WriteConflict error and it was aborted.

**Why it's easy to miss**: the commit Ok/Err assertions pass in either
ordering, and on a dev run the failure would look like conflict detection
returning the wrong winner, sending you into the (correct) TransactionManager
logic; the actual culprit is the unrelated, pre-existing property-versioning
gap.

**Fix**: in both the cluster-level and wire-level conflict tests, the LOSER
issues its SET first and the WINNER writes last, with a comment at each test
explaining exactly why the ordering matters. The commit-result assertions are
ordering-independent; only the final-value read-back needs this.

**How to avoid next time**: before asserting on post-conflict/post-abort
*values* (not just commit results), check whether the record type involved is
actually MVCC-versioned. In graft today: node/edge records are (tx_min/tx_max
stamped); property records are NOT. If it isn't versioned, structure the test
so physical write order matches the logical outcome being asserted, and
say so in a comment.

## Relay-build orchestration: the token watchdog can't fire mid-turn

**The problem**: the /relay-build skill has a 150k-token BUDGET watchdog that
tells a coder to WIND DOWN when its accumulated tokens cross the threshold.
In the Phase 8f review-fix relay (2026-07-09), all three coders ran an entire
milestone in a single background turn (~226k-323k tokens each), so the
orchestrator only saw the token count *after* each turn ended — the watchdog
never had a chance to fire.

**Why it's easy to miss**: the skill reads as if the orchestrator can
intervene continuously; in practice a background agent's usage is only
reported at turn end, so any coder that doesn't stop mid-milestone (QUESTION
or CHECKPOINT) is invisible until it's done.

**What actually controls context size**: milestone scoping in the coder
protocol, not the budget number. Milestones sized as "one root-cause cluster
of findings" kept every coder well under context limits even at 2x the
nominal budget, and each ended at a verified safe point.

**How to apply next time**: treat BUDGET as a between-turn check only. If a
milestone looks like it could exceed ~2x budget, split it in the task doc
before spawning the coder rather than counting on WIND DOWN. The durable
task doc (tasks/code-review-findings.md) + continue.txt handoff is what made
rotation safe regardless.

## Relay-build round 2 (write-conflict task): confirmations and two orchestration refinements

**Watchdog confirmation**: second relay run, same pattern as the first — all
three coders completed a full milestone in a single background turn (185k,
149k, 171k tokens), so the 150k BUDGET watchdog never had a chance to fire
mid-turn. Root-cause milestone scoping is, again, what actually controlled
context size. Treat this as settled: size milestones in the task doc, don't
count on WIND DOWN.

**TaskStop after a CHECKPOINT is a no-op that errors**: a background coder
that ends its turn with CHECKPOINT has already exited — `TaskStop` on its id
returns "No task found". Harmless, but skip it: rotation is just "spawn the
fresh coder"; there is nothing to stop.

**Scope flags embedded in CHECKPOINT beat QUESTION for end-of-milestone
decisions**: the M3 coder finished its milestone, then flagged the next one
(M4, cross-shard commit atomicity) as adjacent scope needing human
confirmation — inside its CHECKPOINT line rather than stopping mid-work with
QUESTION. That let the orchestrator surface the decision to the user at the
natural rotation boundary (with AskUserQuestion + a recommendation) instead
of either blindly rotating a coder into unconfirmed scope or interrupting
work. When a coder's plan contains a milestone beyond the literal task ask,
"flag it in the plan + checkpoint before it" is the right protocol; the
orchestrator should read checkpoints for such flags before auto-rotating.
