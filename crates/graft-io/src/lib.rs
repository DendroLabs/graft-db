pub mod posix;
pub mod sim;
#[cfg(target_os = "linux")]
pub mod uring;

use graft_core::constants::PAGE_SIZE;
use std::io;
use std::path::Path;

pub use posix::PosixIoBackend;
pub use sim::SimIoBackend;
#[cfg(target_os = "linux")]
pub use uring::IoUringBackend;

// ---------------------------------------------------------------------------
// FileHandle
// ---------------------------------------------------------------------------

/// Opaque handle to an open file, returned by `IoBackend::open`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FileHandle(pub(crate) u32);

// ---------------------------------------------------------------------------
// OpenOptions
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct OpenOptions {
    pub read: bool,
    pub write: bool,
    pub create: bool,
    pub truncate: bool,
}

impl OpenOptions {
    pub fn read() -> Self {
        Self { read: true, write: false, create: false, truncate: false }
    }

    pub fn read_write() -> Self {
        Self { read: true, write: true, create: false, truncate: false }
    }

    pub fn create_read_write() -> Self {
        Self { read: true, write: true, create: true, truncate: false }
    }
}

// ---------------------------------------------------------------------------
// IoBackend trait
// ---------------------------------------------------------------------------

/// Encapsulates all platform-dependent operations: file I/O, time, entropy.
///
/// In production this wraps real OS calls (`pread`/`pwrite`). In simulation
/// everything is deterministic and injectable, enabling reproducible tests
/// and fault injection.
/// Create the best available I/O backend for the current platform.
/// On Linux, returns `IoUringBackend`; elsewhere, returns `PosixIoBackend`.
pub fn default_backend() -> Box<dyn IoBackend + Send> {
    #[cfg(target_os = "linux")]
    {
        match IoUringBackend::new() {
            Ok(b) => return Box::new(b),
            Err(_) => {} // fall through to posix
        }
    }
    Box::new(PosixIoBackend::new())
}

pub trait IoBackend: Send {
    // -- file lifecycle -----------------------------------------------------

    fn open(&mut self, path: &Path, opts: &OpenOptions) -> io::Result<FileHandle>;
    fn close(&mut self, handle: FileHandle) -> io::Result<()>;
    fn sync(&mut self, handle: FileHandle) -> io::Result<()>;
    fn file_size(&self, handle: FileHandle) -> io::Result<u64>;
    fn truncate(&mut self, handle: FileHandle, size: u64) -> io::Result<()>;
    fn remove(&mut self, path: &Path) -> io::Result<()>;

    // -- aligned page I/O (data files) --------------------------------------

    fn read_page(
        &mut self,
        handle: FileHandle,
        offset: u64,
        buf: &mut [u8; PAGE_SIZE],
    ) -> io::Result<()>;

    fn write_page(
        &mut self,
        handle: FileHandle,
        offset: u64,
        buf: &[u8; PAGE_SIZE],
    ) -> io::Result<()>;

    // -- arbitrary byte I/O (WAL) -------------------------------------------

    fn read_at(
        &mut self,
        handle: FileHandle,
        offset: u64,
        buf: &mut [u8],
    ) -> io::Result<usize>;

    fn write_at(
        &mut self,
        handle: FileHandle,
        offset: u64,
        data: &[u8],
    ) -> io::Result<usize>;

    // -- clock (monotonic milliseconds) -------------------------------------

    fn now_millis(&self) -> u64;

    // -- entropy ------------------------------------------------------------

    fn random_u64(&mut self) -> u64;
}
