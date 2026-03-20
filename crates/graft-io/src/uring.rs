//! io_uring-based I/O backend for Linux production use.
//!
//! This module is only compiled on Linux (`cfg(target_os = "linux")`).
//! It provides kernel-bypass async I/O through io_uring, submitting
//! page-aligned reads/writes as SQEs and reaping completions.
//!
//! Phase 1: synchronous wrappers around io_uring (submit + wait for each op).
//! Future: batched submission with the shard event loop.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions as StdOpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
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
pub struct IoUringBackend {
    ring: IoUring,
    files: Vec<Option<FileEntry>>,
    free_slots: Vec<u32>,
    epoch: Instant,
    rng_state: u64,
}

struct FileEntry {
    file: File,
    path: PathBuf,
}

impl IoUringBackend {
    pub fn new() -> io::Result<Self> {
        let ring = IoUring::new(RING_ENTRIES)?;
        let epoch = Instant::now();
        let seed = epoch.elapsed().as_nanos() as u64 | 1;
        Ok(Self {
            ring,
            files: Vec::new(),
            free_slots: Vec::new(),
            epoch,
            rng_state: seed,
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
}

impl Default for IoUringBackend {
    fn default() -> Self {
        Self::new().expect("failed to create io_uring")
    }
}

impl IoBackend for IoUringBackend {
    fn open(&mut self, path: &Path, opts: &OpenOptions) -> io::Result<FileHandle> {
        let mut o = StdOpenOptions::new();
        o.read(opts.read);
        o.write(opts.write);
        o.create(opts.create);
        o.truncate(opts.truncate);
        let file = o.open(path)?;
        let slot = self.alloc_handle();
        self.files[slot as usize] = Some(FileEntry {
            file,
            path: path.to_owned(),
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
        let fd = self.get_fd(handle)?;
        let sqe = opcode::Fsync::new(types::Fd(fd)).build();
        self.submit_and_wait_one(sqe)?;
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
