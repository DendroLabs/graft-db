# Storage Engine

This document describes graft's storage engine: page layout, record formats, free-list management, the buffer pool, and checksum integrity.

## Page Layout

All data lives in **8 KB (8192 byte) pages**, aligned to NVMe page size for O_DIRECT I/O. Each page has a 32-byte header followed by a data region that holds up to 127 fixed-size records.

```
┌─────────────────────────────────────┐  byte 0
│            Page Header (32B)        │
├─────────────────────────────────────┤  byte 32
│          Record 0 (64B)             │
├─────────────────────────────────────┤  byte 96
│          Record 1 (64B)             │
├─────────────────────────────────────┤
│              ...                    │
├─────────────────────────────────────┤  byte 8128
│         Record 126 (64B)           │
├─────────────────────────────────────┤  byte 8192
│    (unused: 32 bytes of waste)      │
└─────────────────────────────────────┘
```

### Constants

| Constant | Value | Notes |
|---|---|---|
| `PAGE_SIZE` | 8192 | NVMe-aligned |
| `PAGE_HEADER_SIZE` | 32 | Fixed header |
| `PAGE_USABLE_SIZE` | 8160 | 8192 - 32 |
| `RECORD_SIZE` | 64 | One cache line |
| `RECORDS_PER_PAGE` | 127 | 8160 / 64 |

The 32 bytes of waste (8192 - 32 - 127*64 = 32) are unused padding at the end of each page.

### Page Header

```
Offset  Size  Field
─────────────────────────────────────
0       8     page_id         (u64)
8       8     lsn             (u64, log sequence number)
16      2     record_count    (u16, active records)
18      2     free_head       (u16, first free slot, 0xFFFF = full)
20      1     page_type       (u8)
21      1     flags           (u8, reserved)
22      4     checksum        (u32, CRC32C)
26      6     reserved
```

### Page Types

| Value | Type | Description |
|---|---|---|
| 0 | `Free` | Unallocated page |
| 1 | `Node` | Contains node records |
| 2 | `Edge` | Contains edge records |
| 3 | `PropertyOverflow` | Large property values |

Unknown page type bytes are treated as corruption (`CorruptPage` error), never silently mapped to a default.

## Record Formats

Both node and edge records are exactly 64 bytes — one CPU cache line. This ensures that reading a single record never touches two cache lines.

### Node Record (64 bytes)

```
Offset  Size  Field
─────────────────────────────────────
0       8     node_id          (NodeId)
8       4     label_id         (LabelId)
12      2     flags            (u16)
14      2     (reserved)
16      8     first_out_edge   (EdgeId, head of outbound edge list)
24      8     first_in_edge    (EdgeId, head of inbound edge list)
32      8     tx_min           (TxId, creating transaction)
40      8     tx_max           (TxId, deleting transaction, 0 = alive)
48      16    inline_props     (inline property bytes)
```

- `first_out_edge` / `first_in_edge`: heads of the index-free adjacency linked lists
- `tx_min` / `tx_max`: MVCC version range (see [transactions.md](transactions.md))
- `inline_props`: 16 bytes of inline property storage; larger values overflow to `PropertyOverflow` pages

### Edge Record (64 bytes)

```
Offset  Size  Field
─────────────────────────────────────
0       8     edge_id          (EdgeId)
8       8     source           (NodeId)
16      8     target           (NodeId)
24      8     next_out_edge    (EdgeId, next edge in source's outbound list)
32      8     next_in_edge     (EdgeId, next edge in target's inbound list)
40      4     label_id         (LabelId)
44      2     flags            (u16)
46      2     (reserved)
48      8     tx_min           (TxId, creating transaction)
56      8     tx_max           (TxId, deleting transaction, 0 = alive)
```

Edge records have **no inline property space** — the five 8-byte pointer fields (source, target, next_out_edge, next_in_edge, edge_id) plus MVCC timestamps consume the full 64-byte budget. Edge properties always go to overflow pages.

### Index-Free Adjacency

Edges form two singly-linked lists per node:

```
Node A (first_out_edge → E1)
  E1 (next_out_edge → E2)
    E2 (next_out_edge → E3)
      E3 (next_out_edge → NULL)

Node B (first_in_edge → E1)
  E1 (next_in_edge → E4)
    E4 (next_in_edge → NULL)
```

To traverse all outbound edges of a node: follow `first_out_edge`, then repeatedly follow `next_out_edge` until NULL. Each hop is a single record read — O(1) per hop, O(degree) total.

### Byte Order

All multi-byte fields are stored in **little-endian** format. Serialization is manual (no serde overhead) via `to_le_bytes` / `from_le_bytes`.

## Free-List Slot Management

Within each page, free slots are managed as a **singly-linked list** threaded through the first two bytes of each unused record.

### Initialization

When a page is created, all 127 slots are linked:

```
free_head → slot 0 → slot 1 → slot 2 → ... → slot 126 → NONE (0xFFFF)
```

The "next" pointer is stored as a `u16` in bytes `[0..2]` of the slot's 64-byte region.

### Allocation

`alloc_slot()`:
1. Read `free_head` from the page header
2. If `SLOT_NONE` (0xFFFF), the page is full — return `None`
3. Read the next pointer from the free slot's first two bytes
4. Set `free_head` to that next pointer
5. Zero the slot's 64 bytes
6. Increment `record_count`
7. Return the slot index

### Deallocation

`free_slot(slot)`:
1. Write the current `free_head` into the slot's first two bytes
2. Zero the remaining 62 bytes
3. Set `free_head` to this slot
4. Decrement `record_count`

Both operations are O(1).

## Checksum Integrity

Every page carries a CRC32C checksum (bytes 22–25 of the header). The checksum covers all page bytes **except** the checksum field itself:

```
CRC32C(bytes[0..22] ++ bytes[26..8192])
```

### When checksums are computed

- **Page creation**: `Page::new()` computes the initial checksum
- **Buffer pool eviction**: dirty pages have their checksum updated before eviction data is returned
- **`dirty_pages()`**: all dirty pages get checksum updates before their data is copied out
- **Manual**: `update_checksum()` is available but not called after every `write_record()`

The checksum is **not** automatically updated on every write to the page — this is intentional. In-memory pages are trusted; checksums protect against disk corruption and torn writes. The buffer pool ensures checksums are fresh before any data leaves memory.

### Verification

`verify_checksum()` recomputes the CRC and compares it to the stored value. Call this after reading a page from disk to detect corruption.

## Buffer Pool

The buffer pool is a fixed-capacity page cache with **CLOCK eviction** and an **index-based API**.

### Why Index-Based

Graph traversals frequently need multiple pages pinned simultaneously (e.g., reading a node on one page and following an edge pointer to another page). A reference-based API (`pin(&mut self) → &Page`) prevents this because Rust's borrow checker won't allow multiple borrows of `&mut self`.

The solution: `pin()` returns an opaque `FrameId`, and separate `page(fid)` / `page_mut(fid)` methods access the data.

```rust
let f1 = pool.pin(page_1)?;
let f2 = pool.pin(page_2)?;
// Both pages accessible simultaneously:
let node = pool.page(f1);
let edge = pool.page(f2);
pool.unpin(f1);
pool.unpin(f2);
```

### CLOCK Eviction

When the pool is full and a new page needs to be loaded:

1. **First pass**: scan for an empty (invalid) frame
2. **CLOCK scan** (up to 2 full rotations):
   - Skip frames with `pin_count > 0` (in use)
   - If `ref_bit` is set, clear it and move on (second chance)
   - Otherwise, evict this frame

If a dirty page is evicted, its checksum is updated and the page data is returned as an `EvictedPage` for the caller to write to disk.

If all frames are pinned, the pool returns `BufferPoolError::AllPinned`.

### API

| Method | Description |
|---|---|
| `pin(page_id) → Option<FrameId>` | Pin a loaded page, returns `None` if not in pool |
| `unpin(fid)` | Release a pin |
| `page(fid) → &Page` | Read-only access |
| `page_mut(fid) → &mut Page` | Mutable access, auto-marks dirty |
| `load(page_id, data) → Result<Option<EvictedPage>>` | Load a page from disk into the pool |
| `create(page) → Result<(PageId, Option<EvictedPage>)>` | Add a newly created page (starts dirty) |
| `dirty_pages() → Vec<(PageId, data)>` | Get all dirty pages (updates checksums) |
| `clear_dirty_page(page_id)` | Clear dirty flag after successful flush |
