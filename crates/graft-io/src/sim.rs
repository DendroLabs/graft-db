use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use graft_core::constants::PAGE_SIZE;

use crate::{FileHandle, IoBackend, OpenOptions};

// ---------------------------------------------------------------------------
// SimFile
// ---------------------------------------------------------------------------

struct SimFile {
    /// Current in-memory state (includes unsynced writes).
    data: Vec<u8>,
    /// Last synced state — a crash reverts to this.
    durable: Vec<u8>,
}

impl SimFile {
    fn new() -> Self {
        Self {
            data: Vec::new(),
            durable: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// SimHandle
// ---------------------------------------------------------------------------

struct SimHandle {
    path: PathBuf,
    readable: bool,
    writable: bool,
}

// ---------------------------------------------------------------------------
// SimIoBackend
// ---------------------------------------------------------------------------

/// Fully deterministic, in-memory I/O backend for simulation testing.
///
/// Key features:
/// - All files live in memory (no real filesystem access)
/// - Time is manually advanced
/// - RNG is seeded and reproducible
/// - Fault injection: make the next I/O operation fail
/// - Crash simulation: revert all files to their last synced state
pub struct SimIoBackend {
    files: HashMap<PathBuf, SimFile>,
    handles: Vec<Option<SimHandle>>,
    free_slots: Vec<u32>,
    time_millis: u64,
    rng_state: u64,
    fault: Option<io::ErrorKind>,
}

impl SimIoBackend {
    pub fn new(seed: u64) -> Self {
        Self {
            files: HashMap::new(),
            handles: Vec::new(),
            free_slots: Vec::new(),
            time_millis: 0,
            rng_state: seed | 1, // ensure non-zero for xorshift
            fault: None,
        }
    }

    /// Advance the simulated clock by `millis` milliseconds.
    pub fn advance_time(&mut self, millis: u64) {
        self.time_millis += millis;
    }

    /// Set the simulated clock to an exact value.
    pub fn set_time(&mut self, millis: u64) {
        self.time_millis = millis;
    }

    /// Inject a fault: the next I/O operation will fail with this error kind,
    /// then the fault is automatically cleared (transient error model).
    pub fn inject_fault(&mut self, kind: io::ErrorKind) {
        self.fault = Some(kind);
    }

    /// Clear any pending injected fault.
    pub fn clear_fault(&mut self) {
        self.fault = None;
    }

    /// Simulate a crash: revert all files to their last synced state and
    /// close all handles.
    pub fn crash(&mut self) {
        for file in self.files.values_mut() {
            file.data = file.durable.clone();
        }
        self.handles.clear();
        self.free_slots.clear();
    }

    /// Get the current durable (synced) data for a file, for test assertions.
    pub fn durable_data(&self, path: &Path) -> Option<&[u8]> {
        self.files.get(path).map(|f| f.durable.as_slice())
    }

    /// Get the current in-memory data for a file (including unsynced writes).
    pub fn file_data(&self, path: &Path) -> Option<&[u8]> {
        self.files.get(path).map(|f| f.data.as_slice())
    }

    // -- internal -----------------------------------------------------------

    fn check_fault(&mut self) -> io::Result<()> {
        if let Some(kind) = self.fault.take() {
            Err(io::Error::new(kind, "injected fault"))
        } else {
            Ok(())
        }
    }

    fn get_handle(&self, handle: FileHandle) -> io::Result<&SimHandle> {
        self.handles
            .get(handle.0 as usize)
            .and_then(|slot| slot.as_ref())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid file handle"))
    }

    fn get_file_data_mut(&mut self, path: &Path) -> io::Result<&mut Vec<u8>> {
        self.files
            .get_mut(path)
            .map(|f| &mut f.data)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))
    }

    fn alloc_handle(&mut self, sh: SimHandle) -> FileHandle {
        if let Some(idx) = self.free_slots.pop() {
            self.handles[idx as usize] = Some(sh);
            FileHandle(idx)
        } else {
            let idx = self.handles.len() as u32;
            self.handles.push(Some(sh));
            FileHandle(idx)
        }
    }
}

impl IoBackend for SimIoBackend {
    fn open(&mut self, path: &Path, opts: &OpenOptions) -> io::Result<FileHandle> {
        self.check_fault()?;

        let exists = self.files.contains_key(path);
        if !exists && !opts.create {
            return Err(io::Error::new(io::ErrorKind::NotFound, "file not found"));
        }
        if !exists {
            self.files.insert(path.to_path_buf(), SimFile::new());
        }
        if opts.truncate {
            if let Some(f) = self.files.get_mut(path) {
                f.data.clear();
            }
        }

        let handle = SimHandle {
            path: path.to_path_buf(),
            readable: opts.read,
            writable: opts.write,
        };
        Ok(self.alloc_handle(handle))
    }

    fn close(&mut self, handle: FileHandle) -> io::Result<()> {
        let idx = handle.0 as usize;
        if idx < self.handles.len() && self.handles[idx].is_some() {
            self.handles[idx] = None;
            self.free_slots.push(handle.0);
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid file handle"))
        }
    }

    fn sync(&mut self, handle: FileHandle) -> io::Result<()> {
        self.check_fault()?;
        let path = self.get_handle(handle)?.path.clone();
        if let Some(f) = self.files.get_mut(&path) {
            f.durable = f.data.clone();
        }
        Ok(())
    }

    fn file_size(&self, handle: FileHandle) -> io::Result<u64> {
        let path = &self.get_handle(handle)?.path;
        self.files
            .get(path)
            .map(|f| f.data.len() as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))
    }

    fn truncate(&mut self, handle: FileHandle, size: u64) -> io::Result<()> {
        self.check_fault()?;
        let path = self.get_handle(handle)?.path.clone();
        let data = self.get_file_data_mut(&path)?;
        data.resize(size as usize, 0);
        Ok(())
    }

    fn remove(&mut self, path: &Path) -> io::Result<()> {
        self.check_fault()?;
        self.files
            .remove(path)
            .map(|_| ())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))
    }

    fn read_page(
        &mut self,
        handle: FileHandle,
        offset: u64,
        buf: &mut [u8; PAGE_SIZE],
    ) -> io::Result<()> {
        self.check_fault()?;
        let sh = self.get_handle(handle)?;
        if !sh.readable {
            return Err(io::Error::new(io::ErrorKind::PermissionDenied, "not readable"));
        }
        let path = sh.path.clone();
        let data = self.files.get(&path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))?;
        let start = offset as usize;
        let end = start + PAGE_SIZE;
        if end > data.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("read_page at offset {} but file is {} bytes", offset, data.data.len()),
            ));
        }
        buf.copy_from_slice(&data.data[start..end]);
        Ok(())
    }

    fn write_page(
        &mut self,
        handle: FileHandle,
        offset: u64,
        buf: &[u8; PAGE_SIZE],
    ) -> io::Result<()> {
        self.check_fault()?;
        let sh = self.get_handle(handle)?;
        if !sh.writable {
            return Err(io::Error::new(io::ErrorKind::PermissionDenied, "not writable"));
        }
        let path = sh.path.clone();
        let data = self.get_file_data_mut(&path)?;
        let start = offset as usize;
        let end = start + PAGE_SIZE;
        if end > data.len() {
            data.resize(end, 0);
        }
        data[start..end].copy_from_slice(buf);
        Ok(())
    }

    fn read_at(
        &mut self,
        handle: FileHandle,
        offset: u64,
        buf: &mut [u8],
    ) -> io::Result<usize> {
        self.check_fault()?;
        let sh = self.get_handle(handle)?;
        if !sh.readable {
            return Err(io::Error::new(io::ErrorKind::PermissionDenied, "not readable"));
        }
        let path = sh.path.clone();
        let data = self.files.get(&path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))?;
        let start = offset as usize;
        if start >= data.data.len() {
            return Ok(0);
        }
        let available = data.data.len() - start;
        let n = buf.len().min(available);
        buf[..n].copy_from_slice(&data.data[start..start + n]);
        Ok(n)
    }

    fn write_at(
        &mut self,
        handle: FileHandle,
        offset: u64,
        data: &[u8],
    ) -> io::Result<usize> {
        self.check_fault()?;
        let sh = self.get_handle(handle)?;
        if !sh.writable {
            return Err(io::Error::new(io::ErrorKind::PermissionDenied, "not writable"));
        }
        let path = sh.path.clone();
        let file_data = self.get_file_data_mut(&path)?;
        let start = offset as usize;
        let end = start + data.len();
        if end > file_data.len() {
            file_data.resize(end, 0);
        }
        file_data[start..end].copy_from_slice(data);
        Ok(data.len())
    }

    fn now_millis(&self) -> u64 {
        self.time_millis
    }

    fn random_u64(&mut self) -> u64 {
        xorshift64(&mut self.rng_state)
    }
}

fn xorshift64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn test_path(name: &str) -> PathBuf {
        PathBuf::from(format!("/sim/{name}"))
    }

    #[test]
    fn create_write_read_page() {
        let mut io = SimIoBackend::new(42);
        let path = test_path("data.db");
        let fh = io.open(&path, &OpenOptions::create_read_write()).unwrap();

        let mut page = [0u8; PAGE_SIZE];
        page[0] = 0xDE;
        page[PAGE_SIZE - 1] = 0xAD;
        io.write_page(fh, 0, &page).unwrap();

        let mut buf = [0u8; PAGE_SIZE];
        io.read_page(fh, 0, &mut buf).unwrap();
        assert_eq!(buf[0], 0xDE);
        assert_eq!(buf[PAGE_SIZE - 1], 0xAD);

        io.close(fh).unwrap();
    }

    #[test]
    fn open_nonexistent_without_create_fails() {
        let mut io = SimIoBackend::new(42);
        let result = io.open(&test_path("nope"), &OpenOptions::read());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn read_beyond_eof() {
        let mut io = SimIoBackend::new(42);
        let fh = io.open(&test_path("small"), &OpenOptions::create_read_write()).unwrap();
        // File is empty, reading a page should fail
        let mut buf = [0u8; PAGE_SIZE];
        let result = io.read_page(fh, 0, &mut buf);
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn arbitrary_io() {
        let mut io = SimIoBackend::new(42);
        let path = test_path("wal.log");
        let fh = io.open(&path, &OpenOptions::create_read_write()).unwrap();

        io.write_at(fh, 0, b"hello").unwrap();
        io.write_at(fh, 5, b" world").unwrap();
        assert_eq!(io.file_size(fh).unwrap(), 11);

        let mut buf = [0u8; 11];
        let n = io.read_at(fh, 0, &mut buf).unwrap();
        assert_eq!(n, 11);
        assert_eq!(&buf, b"hello world");
    }

    #[test]
    fn sync_makes_durable() {
        let mut io = SimIoBackend::new(42);
        let path = test_path("data.db");
        let fh = io.open(&path, &OpenOptions::create_read_write()).unwrap();

        io.write_at(fh, 0, b"before sync").unwrap();
        assert_eq!(io.durable_data(&path).unwrap(), b"");

        io.sync(fh).unwrap();
        assert_eq!(io.durable_data(&path).unwrap(), b"before sync");
    }

    #[test]
    fn crash_reverts_to_durable() {
        let mut io = SimIoBackend::new(42);
        let path = test_path("data.db");
        let fh = io.open(&path, &OpenOptions::create_read_write()).unwrap();

        io.write_at(fh, 0, b"synced").unwrap();
        io.sync(fh).unwrap();

        io.write_at(fh, 0, b"UNSYNC").unwrap();
        assert_eq!(io.file_data(&path).unwrap(), b"UNSYNC");

        io.crash();
        assert_eq!(io.file_data(&path).unwrap(), b"synced");

        // Handles are invalidated after crash
        assert!(io.file_size(fh).is_err());
    }

    #[test]
    fn fault_injection() {
        let mut io = SimIoBackend::new(42);
        let path = test_path("data.db");
        let fh = io.open(&path, &OpenOptions::create_read_write()).unwrap();

        // Inject a fault
        io.inject_fault(io::ErrorKind::Other);
        let result = io.write_at(fh, 0, b"data");
        assert!(result.is_err());

        // Fault is transient — next operation succeeds
        io.write_at(fh, 0, b"data").unwrap();
    }

    #[test]
    fn truncate() {
        let mut io = SimIoBackend::new(42);
        let path = test_path("data.db");
        let fh = io.open(&path, &OpenOptions::create_read_write()).unwrap();

        io.write_at(fh, 0, b"some data here").unwrap();
        assert_eq!(io.file_size(fh).unwrap(), 14);

        io.truncate(fh, 4).unwrap();
        assert_eq!(io.file_size(fh).unwrap(), 4);
        assert_eq!(io.file_data(&path).unwrap(), b"some");
    }

    #[test]
    fn remove_file() {
        let mut io = SimIoBackend::new(42);
        let path = test_path("doomed");
        let fh = io.open(&path, &OpenOptions::create_read_write()).unwrap();
        io.close(fh).unwrap();

        io.remove(&path).unwrap();
        assert!(io.file_data(&path).is_none());
        assert!(io.remove(&path).is_err()); // already gone
    }

    #[test]
    fn deterministic_time() {
        let mut io = SimIoBackend::new(42);
        assert_eq!(io.now_millis(), 0);
        io.advance_time(100);
        assert_eq!(io.now_millis(), 100);
        io.advance_time(50);
        assert_eq!(io.now_millis(), 150);
    }

    #[test]
    fn deterministic_rng() {
        let mut a = SimIoBackend::new(42);
        let mut b = SimIoBackend::new(42);

        // Same seed produces same sequence
        for _ in 0..10 {
            assert_eq!(a.random_u64(), b.random_u64());
        }

        // Different seed produces different sequence
        let mut c = SimIoBackend::new(99);
        let vals_a: Vec<_> = (0..5).map(|_| a.random_u64()).collect();
        let vals_c: Vec<_> = (0..5).map(|_| c.random_u64()).collect();
        assert_ne!(vals_a, vals_c);
    }

    #[test]
    fn multiple_pages() {
        let mut io = SimIoBackend::new(42);
        let path = test_path("multi.db");
        let fh = io.open(&path, &OpenOptions::create_read_write()).unwrap();

        let mut p0 = [0u8; PAGE_SIZE];
        let mut p1 = [0u8; PAGE_SIZE];
        p0[0] = 0xAA;
        p1[0] = 0xBB;

        io.write_page(fh, 0, &p0).unwrap();
        io.write_page(fh, PAGE_SIZE as u64, &p1).unwrap();
        assert_eq!(io.file_size(fh).unwrap(), 2 * PAGE_SIZE as u64);

        let mut buf = [0u8; PAGE_SIZE];
        io.read_page(fh, PAGE_SIZE as u64, &mut buf).unwrap();
        assert_eq!(buf[0], 0xBB);

        io.read_page(fh, 0, &mut buf).unwrap();
        assert_eq!(buf[0], 0xAA);
    }

    #[test]
    fn write_only_handle_cannot_read() {
        let mut io = SimIoBackend::new(42);
        let path = test_path("wo");
        let opts = OpenOptions { read: false, write: true, create: true, truncate: false };
        let fh = io.open(&path, &opts).unwrap();
        io.write_at(fh, 0, b"data").unwrap();

        let mut buf = [0u8; 4];
        let result = io.read_at(fh, 0, &mut buf);
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
    }
}
