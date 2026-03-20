use std::fs::{self, File};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::time::Instant;

use graft_core::constants::PAGE_SIZE;

use crate::{FileHandle, IoBackend, OpenOptions};

// ---------------------------------------------------------------------------
// PosixIoBackend
// ---------------------------------------------------------------------------

/// Real filesystem I/O via `pread`/`pwrite` (macOS / Linux dev).
pub struct PosixIoBackend {
    files: Vec<Option<File>>,
    free_slots: Vec<u32>,
    epoch: Instant,
    rng_state: u64,
}

impl PosixIoBackend {
    pub fn new() -> Self {
        // Seed RNG from the monotonic clock
        let epoch = Instant::now();
        let seed = epoch.elapsed().as_nanos() as u64 | 1; // ensure non-zero
        Self {
            files: Vec::new(),
            free_slots: Vec::new(),
            epoch,
            rng_state: seed,
        }
    }

    fn get_file(&self, handle: FileHandle) -> io::Result<&File> {
        self.files
            .get(handle.0 as usize)
            .and_then(|slot| slot.as_ref())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid file handle"))
    }

    fn alloc_handle(&mut self, file: File) -> FileHandle {
        if let Some(idx) = self.free_slots.pop() {
            self.files[idx as usize] = Some(file);
            FileHandle(idx)
        } else {
            let idx = self.files.len() as u32;
            self.files.push(Some(file));
            FileHandle(idx)
        }
    }
}

impl Default for PosixIoBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl IoBackend for PosixIoBackend {
    fn open(&mut self, path: &Path, opts: &OpenOptions) -> io::Result<FileHandle> {
        let file = fs::OpenOptions::new()
            .read(opts.read)
            .write(opts.write)
            .create(opts.create)
            .truncate(opts.truncate)
            .open(path)?;
        Ok(self.alloc_handle(file))
    }

    fn close(&mut self, handle: FileHandle) -> io::Result<()> {
        let idx = handle.0 as usize;
        if idx < self.files.len() && self.files[idx].is_some() {
            self.files[idx] = None; // File::drop does the OS close
            self.free_slots.push(handle.0);
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid file handle"))
        }
    }

    fn sync(&mut self, handle: FileHandle) -> io::Result<()> {
        self.get_file(handle)?.sync_all()
    }

    fn file_size(&self, handle: FileHandle) -> io::Result<u64> {
        Ok(self.get_file(handle)?.metadata()?.len())
    }

    fn truncate(&mut self, handle: FileHandle, size: u64) -> io::Result<()> {
        self.get_file(handle)?.set_len(size)
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
        self.get_file(handle)?.read_exact_at(buf, offset)
    }

    fn write_page(
        &mut self,
        handle: FileHandle,
        offset: u64,
        buf: &[u8; PAGE_SIZE],
    ) -> io::Result<()> {
        self.get_file(handle)?.write_all_at(buf, offset)
    }

    fn read_at(
        &mut self,
        handle: FileHandle,
        offset: u64,
        buf: &mut [u8],
    ) -> io::Result<usize> {
        self.get_file(handle)?.read_at(buf, offset)
    }

    fn write_at(
        &mut self,
        handle: FileHandle,
        offset: u64,
        data: &[u8],
    ) -> io::Result<usize> {
        self.get_file(handle)?.write_at(data, offset)
    }

    fn now_millis(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
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

    #[test]
    fn page_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");

        let mut io = PosixIoBackend::new();
        let fh = io.open(&path, &OpenOptions::create_read_write()).unwrap();

        // Write a page
        let mut page = [0u8; PAGE_SIZE];
        page[0] = 0xAB;
        page[PAGE_SIZE - 1] = 0xCD;
        io.write_page(fh, 0, &page).unwrap();

        // Read it back
        let mut buf = [0u8; PAGE_SIZE];
        io.read_page(fh, 0, &mut buf).unwrap();
        assert_eq!(buf[0], 0xAB);
        assert_eq!(buf[PAGE_SIZE - 1], 0xCD);

        io.close(fh).unwrap();
    }

    #[test]
    fn arbitrary_io() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let mut io = PosixIoBackend::new();
        let fh = io.open(&path, &OpenOptions::create_read_write()).unwrap();

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

        let mut io = PosixIoBackend::new();
        let fh = io.open(&path, &OpenOptions::create_read_write()).unwrap();
        assert_eq!(io.file_size(fh).unwrap(), 0);

        let page = [0u8; PAGE_SIZE];
        io.write_page(fh, 0, &page).unwrap();
        assert_eq!(io.file_size(fh).unwrap(), PAGE_SIZE as u64);

        io.truncate(fh, 0).unwrap();
        assert_eq!(io.file_size(fh).unwrap(), 0);

        io.close(fh).unwrap();
    }

    #[test]
    fn sync_does_not_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");

        let mut io = PosixIoBackend::new();
        let fh = io.open(&path, &OpenOptions::create_read_write()).unwrap();
        io.sync(fh).unwrap();
        io.close(fh).unwrap();
    }

    #[test]
    fn remove_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("doomed.dat");

        let mut io = PosixIoBackend::new();
        let fh = io.open(&path, &OpenOptions::create_read_write()).unwrap();
        io.close(fh).unwrap();
        assert!(path.exists());

        io.remove(&path).unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn handle_reuse_after_close() {
        let dir = tempfile::tempdir().unwrap();
        let mut io = PosixIoBackend::new();

        let fh1 = io.open(&dir.path().join("a"), &OpenOptions::create_read_write()).unwrap();
        io.close(fh1).unwrap();

        // Next open should reuse the slot
        let fh2 = io.open(&dir.path().join("b"), &OpenOptions::create_read_write()).unwrap();
        assert_eq!(fh1, fh2);
        io.close(fh2).unwrap();
    }

    #[test]
    fn invalid_handle_errors() {
        let io = PosixIoBackend::new();
        assert!(io.file_size(FileHandle(999)).is_err());
    }

    #[test]
    fn clock_advances() {
        let io = PosixIoBackend::new();
        let t1 = io.now_millis();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let t2 = io.now_millis();
        assert!(t2 >= t1);
    }

    #[test]
    fn rng_produces_different_values() {
        let mut io = PosixIoBackend::new();
        let a = io.random_u64();
        let b = io.random_u64();
        assert_ne!(a, b);
    }
}
