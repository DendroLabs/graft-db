/// Bump allocator for query execution temporaries. O(1) allocation, O(1)
/// bulk deallocation via `reset()`. No per-object `Drop` — only `Copy` types.
///
/// `alloc` takes `&self` (interior mutability) so multiple arena-allocated
/// references can coexist. `reset` takes `&mut self`, which the borrow
/// checker enforces cannot be called while any `&T` references are live.
use std::cell::{Cell, RefCell};

const DEFAULT_CHUNK_SIZE: usize = 8 * 1024; // 8 KB

pub struct Arena {
    /// Heap-allocated chunks. `RefCell` because we only borrow mutably
    /// during `grow` (briefly) and during `reset` (`&mut self`).
    chunks: RefCell<Vec<Box<[u8]>>>,
    /// Pointer to the next free byte in the current chunk.
    ptr: Cell<*mut u8>,
    /// Pointer one past the end of the current chunk.
    end: Cell<*mut u8>,
    chunk_size: usize,
    bytes_used: Cell<usize>,
}

impl Arena {
    pub fn new() -> Self {
        Self::with_chunk_size(DEFAULT_CHUNK_SIZE)
    }

    pub fn with_chunk_size(chunk_size: usize) -> Self {
        assert!(chunk_size > 0);
        Self {
            chunks: RefCell::new(Vec::new()),
            ptr: Cell::new(std::ptr::null_mut()),
            end: Cell::new(std::ptr::null_mut()),
            chunk_size,
            bytes_used: Cell::new(0),
        }
    }

    /// Allocate and initialize a `Copy` value, returning a reference tied to
    /// the arena's lifetime.
    pub fn alloc<T: Copy>(&self, value: T) -> &T {
        let size = std::mem::size_of::<T>();
        let align = std::mem::align_of::<T>();

        // ZST
        if size == 0 {
            return unsafe { &*std::ptr::dangling::<T>() };
        }

        let raw = self.alloc_raw(size, align);
        unsafe {
            raw.cast::<T>().write(value);
            &*raw.cast::<T>()
        }
    }

    /// Allocate a copy of a slice.
    pub fn alloc_slice_copy<T: Copy>(&self, src: &[T]) -> &[T] {
        if src.is_empty() {
            return &[];
        }
        let size = std::mem::size_of_val(src);
        let align = std::mem::align_of::<T>();
        let raw = self.alloc_raw(size, align);
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr().cast::<u8>(), raw, size);
            std::slice::from_raw_parts(raw.cast::<T>(), src.len())
        }
    }

    /// Total bytes consumed by allocations (not counting chunk overhead).
    pub fn bytes_used(&self) -> usize {
        self.bytes_used.get()
    }

    /// Drop all allocations and reclaim memory. Keeps one chunk for reuse.
    ///
    /// `&mut self` guarantees no outstanding references exist.
    pub fn reset(&mut self) {
        let chunks = self.chunks.get_mut();
        if chunks.is_empty() {
            return;
        }
        chunks.truncate(1);
        let chunk = &mut chunks[0];
        self.ptr.set(chunk.as_mut_ptr());
        self.end.set(unsafe { chunk.as_mut_ptr().add(chunk.len()) });
        self.bytes_used.set(0);
    }

    // -- internals ----------------------------------------------------------

    fn alloc_raw(&self, size: usize, align: usize) -> *mut u8 {
        let ptr = self.ptr.get();
        let aligned = align_up(ptr as usize, align);
        let new_ptr = aligned + size;

        if new_ptr <= self.end.get() as usize {
            self.ptr.set(new_ptr as *mut u8);
            self.bytes_used.set(self.bytes_used.get() + size);
            aligned as *mut u8
        } else {
            self.grow_and_alloc(size, align)
        }
    }

    #[cold]
    fn grow_and_alloc(&self, size: usize, align: usize) -> *mut u8 {
        // New chunk must be large enough for this allocation + alignment
        let min_size = size + align;
        let chunk_size = self.chunk_size.max(min_size);
        let mut chunk = vec![0u8; chunk_size].into_boxed_slice();
        let chunk_ptr = chunk.as_mut_ptr();
        let chunk_end = unsafe { chunk_ptr.add(chunk_size) };

        self.chunks.borrow_mut().push(chunk);

        let aligned = align_up(chunk_ptr as usize, align);
        let new_ptr = aligned + size;
        self.ptr.set(new_ptr as *mut u8);
        self.end.set(chunk_end);
        self.bytes_used.set(self.bytes_used.get() + size);
        aligned as *mut u8
    }
}

impl Default for Arena {
    fn default() -> Self {
        Self::new()
    }
}

#[inline]
fn align_up(addr: usize, align: usize) -> usize {
    (addr + align - 1) & !(align - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_and_read() {
        let arena = Arena::new();
        let a = arena.alloc(42u64);
        let b = arena.alloc(99u32);
        // Both references live simultaneously
        assert_eq!(*a, 42);
        assert_eq!(*b, 99);
    }

    #[test]
    fn alloc_slice() {
        let arena = Arena::new();
        let src = [1u32, 2, 3, 4, 5];
        let s = arena.alloc_slice_copy(&src);
        assert_eq!(s, &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn alloc_empty_slice() {
        let arena = Arena::new();
        let s = arena.alloc_slice_copy::<u8>(&[]);
        assert!(s.is_empty());
    }

    #[test]
    fn reset_frees_memory() {
        let mut arena = Arena::new();
        for i in 0..1000u64 {
            arena.alloc(i);
        }
        let used = arena.bytes_used();
        assert!(used > 0);

        arena.reset();
        assert_eq!(arena.bytes_used(), 0);

        // Can allocate again after reset
        let v = arena.alloc(123u64);
        assert_eq!(*v, 123);
    }

    #[test]
    fn grows_across_chunks() {
        // Tiny chunks to force growth
        let arena = Arena::with_chunk_size(32);
        let mut refs = Vec::new();
        for i in 0..100u64 {
            refs.push(arena.alloc(i));
        }
        // All values survive across chunk boundaries
        for (i, r) in refs.iter().enumerate() {
            assert_eq!(**r, i as u64);
        }
        assert!(arena.bytes_used() >= 100 * 8);
    }

    #[test]
    fn alignment() {
        let arena = Arena::with_chunk_size(64);
        let _ = arena.alloc(1u8); // misalign the pointer
        let val = arena.alloc(42u64); // requires 8-byte alignment
        let ptr = val as *const u64 as usize;
        assert_eq!(ptr % std::mem::align_of::<u64>(), 0);
    }

    #[test]
    fn large_allocation() {
        let arena = Arena::with_chunk_size(32);
        // Allocation larger than chunk_size should still work
        let big = arena.alloc_slice_copy(&[0u8; 256]);
        assert_eq!(big.len(), 256);
    }

    #[test]
    fn zst() {
        let arena = Arena::new();
        let _ = arena.alloc(());
        assert_eq!(arena.bytes_used(), 0);
    }

    #[test]
    fn bytes_used_tracking() {
        let arena = Arena::new();
        arena.alloc(0u64);
        assert_eq!(arena.bytes_used(), 8);
        arena.alloc(0u32);
        assert_eq!(arena.bytes_used(), 12);
    }
}
