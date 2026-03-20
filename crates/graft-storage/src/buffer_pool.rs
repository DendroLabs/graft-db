use graft_core::constants::PAGE_SIZE;
use graft_core::PageId;

use crate::page::Page;

/// Opaque handle to a frame in the buffer pool. Returned by `pin` — the
/// caller uses it to access page data and must pass it back to `unpin`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameId(usize);

// ---------------------------------------------------------------------------
// PageFrame
// ---------------------------------------------------------------------------

struct PageFrame {
    page: Page,
    page_id: PageId,
    pin_count: u32,
    dirty: bool,
    ref_bit: bool,
    valid: bool,
}

impl PageFrame {
    fn empty() -> Self {
        Self {
            page: Page::from_bytes([0; PAGE_SIZE]),
            page_id: 0,
            pin_count: 0,
            dirty: false,
            ref_bit: false,
            valid: false,
        }
    }
}

// ---------------------------------------------------------------------------
// EvictedPage — returned when a dirty page is evicted
// ---------------------------------------------------------------------------

pub struct EvictedPage {
    pub page_id: PageId,
    pub data: [u8; PAGE_SIZE],
}

// ---------------------------------------------------------------------------
// BufferPool
// ---------------------------------------------------------------------------

/// Fixed-capacity page cache with CLOCK eviction.
///
/// The buffer pool manages page frames in memory. External code (I/O layer)
/// is responsible for reading pages from disk and writing dirty evictions.
///
/// The API is index-based: `pin` returns a [`FrameId`] that can be used to
/// access page data via `page` / `page_mut`. This allows multiple pages to
/// be pinned simultaneously (required for graph traversals that touch
/// multiple pages per hop).
pub struct BufferPool {
    frames: Vec<PageFrame>,
    /// Maps loaded PageId → frame index.
    page_table: hashbrown::HashMap<PageId, FrameId>,
    clock_hand: usize,
}

impl BufferPool {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        let mut frames = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            frames.push(PageFrame::empty());
        }
        Self {
            frames,
            page_table: hashbrown::HashMap::with_capacity(capacity),
            clock_hand: 0,
        }
    }

    pub fn capacity(&self) -> usize {
        self.frames.len()
    }

    /// Number of pages currently loaded in the pool.
    pub fn len(&self) -> usize {
        self.page_table.len()
    }

    pub fn is_empty(&self) -> bool {
        self.page_table.is_empty()
    }

    /// Check whether a page is loaded in the pool.
    pub fn contains(&self, page_id: PageId) -> bool {
        self.page_table.contains_key(&page_id)
    }

    // -- pin / unpin --------------------------------------------------------

    /// Pin a page, returning its frame handle. Returns `None` if the page is
    /// not in the pool. Use `page()` / `page_mut()` to access the data.
    pub fn pin(&mut self, page_id: PageId) -> Option<FrameId> {
        let &fid = self.page_table.get(&page_id)?;
        let frame = &mut self.frames[fid.0];
        frame.pin_count += 1;
        frame.ref_bit = true;
        Some(fid)
    }

    /// Release a pin on a frame.
    pub fn unpin(&mut self, fid: FrameId) {
        let frame = &mut self.frames[fid.0];
        debug_assert!(frame.pin_count > 0, "unpin called with pin_count 0");
        frame.pin_count -= 1;
    }

    /// Read-only access to a pinned page's data.
    pub fn page(&self, fid: FrameId) -> &Page {
        &self.frames[fid.0].page
    }

    /// Mutable access to a pinned page's data. Automatically marks dirty.
    pub fn page_mut(&mut self, fid: FrameId) -> &mut Page {
        let frame = &mut self.frames[fid.0];
        frame.dirty = true;
        &mut frame.page
    }

    /// Mark a frame as dirty without taking a mutable page reference.
    pub fn mark_dirty(&mut self, fid: FrameId) {
        self.frames[fid.0].dirty = true;
    }

    pub fn is_dirty(&self, fid: FrameId) -> bool {
        self.frames[fid.0].dirty
    }

    // -- load / create ------------------------------------------------------

    /// Load a page from raw bytes into the pool.
    ///
    /// If the pool is full, runs CLOCK eviction first. Returns `Some(EvictedPage)`
    /// if a dirty page was evicted (caller must write it to disk).
    pub fn load(
        &mut self,
        page_id: PageId,
        data: &[u8; PAGE_SIZE],
    ) -> Result<Option<EvictedPage>, BufferPoolError> {
        if self.page_table.contains_key(&page_id) {
            return Err(BufferPoolError::AlreadyLoaded(page_id));
        }
        let (idx, evicted) = self.find_or_evict()?;
        let frame = &mut self.frames[idx];
        frame.page = Page::from_bytes(*data);
        frame.page_id = page_id;
        frame.pin_count = 0;
        frame.dirty = false;
        frame.ref_bit = false;
        frame.valid = true;
        self.page_table.insert(page_id, FrameId(idx));
        Ok(evicted)
    }

    /// Create a fresh page in the pool (for newly allocated pages).
    pub fn create(
        &mut self,
        page: Page,
    ) -> Result<(PageId, Option<EvictedPage>), BufferPoolError> {
        let page_id = page.page_id();
        if self.page_table.contains_key(&page_id) {
            return Err(BufferPoolError::AlreadyLoaded(page_id));
        }
        let (idx, evicted) = self.find_or_evict()?;
        let frame = &mut self.frames[idx];
        frame.page = page;
        frame.page_id = page_id;
        frame.pin_count = 0;
        frame.dirty = true; // new page needs to be written
        frame.ref_bit = false;
        frame.valid = true;
        self.page_table.insert(page_id, FrameId(idx));
        Ok((page_id, evicted))
    }

    /// Flush preparation: update checksums on all dirty pages and return
    /// their (page_id, data) pairs. Does NOT clear dirty flags — call
    /// `clear_dirty` after the I/O layer confirms the write.
    pub fn dirty_pages(&mut self) -> Vec<(PageId, [u8; PAGE_SIZE])> {
        self.frames
            .iter_mut()
            .filter(|f| f.valid && f.dirty)
            .map(|f| {
                f.page.update_checksum();
                (f.page_id, *f.page.as_bytes())
            })
            .collect()
    }

    /// Clear the dirty flag on a page (after it has been flushed to disk).
    pub fn clear_dirty_page(&mut self, page_id: PageId) {
        if let Some(&fid) = self.page_table.get(&page_id) {
            self.frames[fid.0].dirty = false;
        }
    }

    // -- eviction -----------------------------------------------------------

    /// Find a free frame or evict one using the CLOCK algorithm.
    fn find_or_evict(&mut self) -> Result<(usize, Option<EvictedPage>), BufferPoolError> {
        let cap = self.frames.len();

        // First pass: look for an empty frame
        for i in 0..cap {
            if !self.frames[i].valid {
                return Ok((i, None));
            }
        }

        // CLOCK scan — at most 2 full rotations
        for _ in 0..2 * cap {
            let idx = self.clock_hand;
            self.clock_hand = (self.clock_hand + 1) % cap;

            let frame = &mut self.frames[idx];
            if frame.pin_count > 0 {
                continue;
            }
            if frame.ref_bit {
                frame.ref_bit = false;
                continue;
            }

            // Victim found — update checksum before evicting dirty data
            let evicted = if frame.dirty {
                frame.page.update_checksum();
                Some(EvictedPage {
                    page_id: frame.page_id,
                    data: *frame.page.as_bytes(),
                })
            } else {
                None
            };
            self.page_table.remove(&frame.page_id);
            frame.valid = false;
            return Ok((idx, evicted));
        }

        Err(BufferPoolError::AllPinned)
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum BufferPoolError {
    #[error("page {0} is already loaded in the buffer pool")]
    AlreadyLoaded(PageId),

    #[error("all buffer pool frames are pinned; cannot evict")]
    AllPinned,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::PageType;

    fn make_page(id: PageId) -> Page {
        Page::new(id, PageType::Node)
    }

    #[test]
    fn load_and_pin() {
        let mut pool = BufferPool::new(4);
        pool.load(10, make_page(10).as_bytes()).unwrap();

        assert!(pool.contains(10));
        assert_eq!(pool.len(), 1);

        let fid = pool.pin(10).unwrap();
        assert_eq!(pool.page(fid).page_id(), 10);
        pool.unpin(fid);
    }

    #[test]
    fn pin_mut_marks_dirty() {
        let mut pool = BufferPool::new(4);
        pool.load(1, make_page(1).as_bytes()).unwrap();

        let fid = pool.pin(1).unwrap();
        assert!(!pool.is_dirty(fid));
        pool.unpin(fid);

        let fid = pool.pin(1).unwrap();
        let _ = pool.page_mut(fid);
        assert!(pool.is_dirty(fid));
        pool.unpin(fid);
    }

    #[test]
    fn multi_page_pin() {
        let mut pool = BufferPool::new(4);
        pool.load(1, make_page(1).as_bytes()).unwrap();
        pool.load(2, make_page(2).as_bytes()).unwrap();

        // Pin two pages simultaneously — the whole point of the index-based API
        let f1 = pool.pin(1).unwrap();
        let f2 = pool.pin(2).unwrap();
        assert_eq!(pool.page(f1).page_id(), 1);
        assert_eq!(pool.page(f2).page_id(), 2);
        pool.unpin(f1);
        pool.unpin(f2);
    }

    #[test]
    fn pin_nonexistent_returns_none() {
        let mut pool = BufferPool::new(4);
        assert!(pool.pin(999).is_none());
    }

    #[test]
    fn eviction_when_full() {
        let mut pool = BufferPool::new(2);
        pool.load(1, make_page(1).as_bytes()).unwrap();
        pool.load(2, make_page(2).as_bytes()).unwrap();
        assert_eq!(pool.len(), 2);

        // Load a third page — should evict one
        let result = pool.load(3, make_page(3).as_bytes()).unwrap();
        assert!(result.is_none()); // evicted page was clean
        assert_eq!(pool.len(), 2);
        assert!(pool.contains(3));
    }

    #[test]
    fn dirty_eviction_returns_data() {
        let mut pool = BufferPool::new(1);
        pool.create(make_page(1)).unwrap(); // dirty because it's new

        // Loading another page should evict dirty page 1
        let evicted = pool.load(2, make_page(2).as_bytes()).unwrap();
        assert!(evicted.is_some());
        assert_eq!(evicted.unwrap().page_id, 1);
    }

    #[test]
    fn all_pinned_error() {
        let mut pool = BufferPool::new(1);
        pool.load(1, make_page(1).as_bytes()).unwrap();
        let fid = pool.pin(1).unwrap();

        let result = pool.load(2, make_page(2).as_bytes());
        assert!(matches!(result, Err(BufferPoolError::AllPinned)));

        pool.unpin(fid);
    }

    #[test]
    fn clock_ref_bit_second_chance() {
        let mut pool = BufferPool::new(2);
        pool.load(1, make_page(1).as_bytes()).unwrap();
        pool.load(2, make_page(2).as_bytes()).unwrap();

        // Access page 1 to set its ref_bit
        let fid = pool.pin(1).unwrap();
        pool.unpin(fid);

        // Load page 3 — page 1 gets a second chance, page 2 should be evicted
        pool.load(3, make_page(3).as_bytes()).unwrap();
        assert!(pool.contains(1)); // survived due to ref_bit
        assert!(!pool.contains(2)); // evicted
        assert!(pool.contains(3));
    }

    #[test]
    fn create_page() {
        let mut pool = BufferPool::new(4);
        let (pid, evicted) = pool.create(make_page(42)).unwrap();
        assert_eq!(pid, 42);
        assert!(evicted.is_none());

        let fid = pool.pin(42).unwrap();
        assert!(pool.is_dirty(fid)); // new pages are dirty
        pool.unpin(fid);
    }

    #[test]
    fn dirty_pages_list() {
        let mut pool = BufferPool::new(4);
        pool.create(make_page(1)).unwrap();
        pool.load(2, make_page(2).as_bytes()).unwrap();
        pool.create(make_page(3)).unwrap();

        let dirty = pool.dirty_pages();
        assert_eq!(dirty.len(), 2); // pages 1 and 3
        let ids: Vec<_> = dirty.iter().map(|(id, _)| *id).collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&3));
    }

    #[test]
    fn evicted_dirty_page_has_valid_checksum() {
        let mut pool = BufferPool::new(1);
        let (_, _) = pool.create(make_page(1)).unwrap();

        // Mutate the page so checksum is stale
        let fid = pool.pin(1).unwrap();
        pool.page_mut(fid).set_lsn(42);
        pool.unpin(fid);

        // Evict by loading another page
        let evicted = pool.load(2, make_page(2).as_bytes()).unwrap().unwrap();
        let recovered = Page::from_bytes(evicted.data);
        assert!(recovered.verify_checksum());
        assert_eq!(recovered.lsn(), 42);
    }
}
