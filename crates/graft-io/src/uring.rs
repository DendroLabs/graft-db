//! io_uring-based I/O backend for Linux production use.
//!
//! This module is only compiled on Linux (`cfg(target_os = "linux")`).
//! It provides kernel-bypass async I/O through io_uring, submitting
//! page-aligned reads/writes as SQEs and reaping completions.
//!
//! Phase 1: synchronous wrappers around io_uring (submit + wait for each op).
//! Future: batched submission with the shard event loop.

use std::fs::{self, File, OpenOptions as StdOpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::Instant;

use graft_core::constants::PAGE_SIZE;
use io_uring::{opcode, types, IoUring};

use crate::{FileHandle, IoBackend, OpenOptions};

const RING_ENTRIES: u32 = 256;

/// Linux io_uring I/O backend.
///
/// Uses io_uring for page-aligned I/O (data files) and falls back to
/// pread/pwrite for arbitrary byte I/O (WAL), since WAL writes are
/// typically small and sequential.
///
/// Data files are opened with O_DIRECT to bypass the kernel page cache —
/// graft manages its own buffer pool, so double-caching wastes memory.
/// O_DIRECT requires page-aligned buffers; `read_page`/`write_page` use
/// the caller's `[u8; PAGE_SIZE]` which is stack- or slab-allocated at
/// 8KB alignment by the buffer pool.
pub struct IoUringBackend {
    ring: IoUring,
    files: Vec<Option<FileEntry>>,
    free_slots: Vec<u32>,
    epoch: Instant,
    rng_state: u64,
    /// Enable O_DIRECT for data file I/O (page reads/writes).
    /// Disabled in tests or when the filesystem doesn't support it.
    use_direct_io: bool,
}

struct FileEntry {
    file: File,
    #[allow(dead_code)]
    path: PathBuf,
    /// Whether this file was opened with O_DIRECT.
    direct: bool,
}

impl IoUringBackend {
    pub fn new() -> io::Result<Self> {
        Self::with_options(true)
    }

    /// Create a backend with configurable O_DIRECT.
    pub fn with_options(use_direct_io: bool) -> io::Result<Self> {
        let ring = IoUring::new(RING_ENTRIES)?;
        let epoch = Instant::now();
        let seed = epoch.elapsed().as_nanos() as u64 | 1;
        Ok(Self {
            ring,
            files: Vec::new(),
            free_slots: Vec::new(),
            epoch,
            rng_state: seed,
            use_direct_io,
        })
    }

    fn alloc_handle(&mut self) -> u32 {
        if let Some(slot) = self.free_slots.pop() {
            slot
        } else {
            let slot = self.files.len() as u32;
            self.files.push(None);
            slot
        }
    }

    fn get_file(&self, handle: FileHandle) -> io::Result<&File> {
        self.files
            .get(handle.0 as usize)
            .and_then(|slot| slot.as_ref())
            .map(|entry| &entry.file)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid file handle"))
    }

    fn get_entry(&self, handle: FileHandle) -> io::Result<&FileEntry> {
        self.files
            .get(handle.0 as usize)
            .and_then(|slot| slot.as_ref())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid file handle"))
    }

    fn get_fd(&self, handle: FileHandle) -> io::Result<i32> {
        Ok(self.get_file(handle)?.as_raw_fd())
    }

    /// Submit a single SQE and wait for its completion.
    fn submit_and_wait_one(&mut self, sqe: io_uring::squeue::Entry) -> io::Result<i32> {
        unsafe {
            self.ring.submission().push(&sqe).map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "io_uring submission queue full")
            })?;
        }
        self.ring.submit_and_wait(1)?;

        let cqe = self
            .ring
            .completion()
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "no completion event"))?;

        let result = cqe.result();
        if result < 0 {
            Err(io::Error::from_raw_os_error(-result))
        } else {
            Ok(result)
        }
    }

    fn xorshift64(&mut self) -> u64 {
        let mut x = self.rng_state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng_state = x;
        x
    }

    /// Determine if a path is a data file (not WAL) based on extension.
    /// Data files use O_DIRECT; WAL files don't (small sequential writes).
    fn is_data_file(path: &Path) -> bool {
        match path.extension().and_then(|e| e.to_str()) {
            Some("dat") => true,
            Some("wal") => false,
            _ => true,
        }
    }
}

impl Default for IoUringBackend {
    fn default() -> Self {
        Self::new().expect("failed to create io_uring")
    }
}

impl IoBackend for IoUringBackend {
    fn open(&mut self, path: &Path, opts: &OpenOptions) -> io::Result<FileHandle> {
        let want_direct = self.use_direct_io && Self::is_data_file(path);
        let mut o = StdOpenOptions::new();
        o.read(opts.read);
        o.write(opts.write);
        o.create(opts.create);
        o.truncate(opts.truncate);
        if want_direct {
            // O_DIRECT = 0x4000 on Linux (libc::O_DIRECT)
            o.custom_flags(0x4000);
        }

        let file = match o.open(path) {
            Ok(f) => f,
            Err(e) if want_direct && e.raw_os_error() == Some(22) => {
                // EINVAL — filesystem may not support O_DIRECT (e.g. tmpfs).
                // Fall back to buffered I/O.
                let mut o2 = StdOpenOptions::new();
                o2.read(opts.read);
                o2.write(opts.write);
                o2.create(opts.create);
                o2.truncate(opts.truncate);
                let file = o2.open(path)?;
                let slot = self.alloc_handle();
                self.files[slot as usize] = Some(FileEntry {
                    file,
                    path: path.to_owned(),
                    direct: false,
                });
                return Ok(FileHandle(slot));
            }
            Err(e) => return Err(e),
        };

        let slot = self.alloc_handle();
        self.files[slot as usize] = Some(FileEntry {
            file,
            path: path.to_owned(),
            direct: want_direct,
        });
        Ok(FileHandle(slot))
    }

    fn close(&mut self, handle: FileHandle) -> io::Result<()> {
        let slot = handle.0 as usize;
        if slot < self.files.len() && self.files[slot].is_some() {
            self.files[slot] = None;
            self.free_slots.push(handle.0);
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid file handle",
            ))
        }
    }

    fn sync(&mut self, handle: FileHandle) -> io::Result<()> {
        let entry = self.get_entry(handle)?;
        if entry.direct {
            // For O_DIRECT files, use fdatasync (skips metadata) for performance
            let fd = entry.file.as_raw_fd();
            let sqe = opcode::Fsync::new(types::Fd(fd))
                .flags(types::FsyncFlags::DATASYNC)
                .build();
            self.submit_and_wait_one(sqe)?;
        } else {
            let fd = entry.file.as_raw_fd();
            let sqe = opcode::Fsync::new(types::Fd(fd)).build();
            self.submit_and_wait_one(sqe)?;
        }
        Ok(())
    }

    fn file_size(&self, handle: FileHandle) -> io::Result<u64> {
        let file = self.get_file(handle)?;
        Ok(file.metadata()?.len())
    }

    fn truncate(&mut self, handle: FileHandle, size: u64) -> io::Result<()> {
        let file = self.get_file(handle)?;
        file.set_len(size)
    }

    fn remove(&mut self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn read_page(
        &mut self,
        handle: FileHandle,
        offset: u64,
        buf: &mut [u8; PAGE_SIZE],
    ) -> io::Result<()> {
        let fd = self.get_fd(handle)?;
        let sqe = opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), PAGE_SIZE as u32)
            .offset(offset)
            .build();
        let n = self.submit_and_wait_one(sqe)?;
        if (n as usize) < PAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("short read: {n} < {PAGE_SIZE}"),
            ));
        }
        Ok(())
    }

    fn write_page(
        &mut self,
        handle: FileHandle,
        offset: u64,
        buf: &[u8; PAGE_SIZE],
    ) -> io::Result<()> {
        let fd = self.get_fd(handle)?;
        let sqe = opcode::Write::new(types::Fd(fd), buf.as_ptr(), PAGE_SIZE as u32)
            .offset(offset)
            .build();
        let n = self.submit_and_wait_one(sqe)?;
        if (n as usize) < PAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                format!("short write: {n} < {PAGE_SIZE}"),
            ));
        }
        Ok(())
    }

    // WAL I/O: use pread/pwrite (small sequential writes, not worth io_uring overhead)
    fn read_at(
        &mut self,
        handle: FileHandle,
        offset: u64,
        buf: &mut [u8],
    ) -> io::Result<usize> {
        let file = self.get_file(handle)?;
        file.read_at(buf, offset)
    }

    fn write_at(
        &mut self,
        handle: FileHandle,
        offset: u64,
        data: &[u8],
    ) -> io::Result<usize> {
        let file = self.get_file(handle)?;
        file.write_at(data, offset)
    }

    fn now_millis(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    fn random_u64(&mut self) -> u64 {
        self.xorshift64()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");

        // Use non-direct I/O in tests (tmpfs may not support O_DIRECT)
        let mut io = IoUringBackend::with_options(false).unwrap();
        let fh = io
            .open(&path, &crate::OpenOptions::create_read_write())
            .unwrap();

        let mut page = [0u8; PAGE_SIZE];
        page[0] = 0xAB;
        page[PAGE_SIZE - 1] = 0xCD;
        io.write_page(fh, 0, &page).unwrap();

        let mut buf = [0u8; PAGE_SIZE];
        io.read_page(fh, 0, &mut buf).unwrap();
        assert_eq!(buf[0], 0xAB);
        assert_eq!(buf[PAGE_SIZE - 1], 0xCD);

        io.close(fh).unwrap();
    }

    #[test]
    fn arbitrary_io() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut io = IoUringBackend::with_options(false).unwrap();
        let fh = io
            .open(&path, &crate::OpenOptions::create_read_write())
            .unwrap();

        let data = b"hello graft";
        let n = io.write_at(fh, 0, data).unwrap();
        assert_eq!(n, data.len());

        let mut buf = [0u8; 11];
        let n = io.read_at(fh, 0, &mut buf).unwrap();
        assert_eq!(n, 11);
        assert_eq!(&buf, b"hello graft");

        io.close(fh).unwrap();
    }

    #[test]
    fn file_size_and_truncate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");

        let mut io = IoUringBackend::with_options(false).unwrap();
        let fh = io
            .open(&path, &crate::OpenOptions::create_read_write())
            .unwrap();
        assert_eq!(io.file_size(fh).unwrap(), 0);

        let page = [0u8; PAGE_SIZE];
        io.write_page(fh, 0, &page).unwrap();
        assert_eq!(io.file_size(fh).unwrap(), PAGE_SIZE as u64);

        io.truncate(fh, 0).unwrap();
        assert_eq!(io.file_size(fh).unwrap(), 0);

        io.close(fh).unwrap();
    }

    #[test]
    fn sync_via_uring() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");

        let mut io = IoUringBackend::with_options(false).unwrap();
        let fh = io
            .open(&path, &crate::OpenOptions::create_read_write())
            .unwrap();
        io.sync(fh).unwrap();
        io.close(fh).unwrap();
    }

    #[test]
    fn handle_reuse() {
        let dir = tempfile::tempdir().unwrap();
        let mut io = IoUringBackend::with_options(false).unwrap();

        let fh1 = io
            .open(
                &dir.path().join("a.dat"),
                &crate::OpenOptions::create_read_write(),
            )
            .unwrap();
        io.close(fh1).unwrap();

        let fh2 = io
            .open(
                &dir.path().join("b.dat"),
                &crate::OpenOptions::create_read_write(),
            )
            .unwrap();
        assert_eq!(fh1, fh2);
        io.close(fh2).unwrap();
    }

    #[test]
    fn clock_and_rng() {
        let mut io = IoUringBackend::with_options(false).unwrap();
        let t1 = io.now_millis();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let t2 = io.now_millis();
        assert!(t2 >= t1);

        let a = io.random_u64();
        let b = io.random_u64();
        assert_ne!(a, b);
    }

    #[test]
    fn wal_file_skips_direct() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shard.wal");

        // Even with direct_io enabled, .wal files should not use O_DIRECT
        let mut io = IoUringBackend::with_options(true).unwrap();
        let fh = io
            .open(&path, &crate::OpenOptions::create_read_write())
            .unwrap();

        let entry = io.get_entry(fh).unwrap();
        assert!(!entry.direct, ".wal files should not use O_DIRECT");

        io.close(fh).unwrap();
    }

    #[test]
    fn data_file_direct_fallback() {
        // On tmpfs (where tempdir usually lives), O_DIRECT may not work.
        // The backend should gracefully fall back.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nodes.dat");

        let mut io = IoUringBackend::with_options(true).unwrap();
        let fh = io
            .open(&path, &crate::OpenOptions::create_read_write())
            .unwrap();

        // Whether direct or not, basic I/O should work
        let page = [42u8; PAGE_SIZE];
        io.write_page(fh, 0, &page).unwrap();

        let mut buf = [0u8; PAGE_SIZE];
        io.read_page(fh, 0, &mut buf).unwrap();
        assert_eq!(buf[0], 42);

        io.close(fh).unwrap();
    }
}
