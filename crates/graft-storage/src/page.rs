use graft_core::constants::{PAGE_HEADER_SIZE, PAGE_SIZE, RECORDS_PER_PAGE, RECORD_SIZE};
use graft_core::{PageId, Result};

/// Slot index sentinel: no more free slots.
const SLOT_NONE: u16 = 0xFFFF;

// ---------------------------------------------------------------------------
// PageType
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    Free = 0,
    Node = 1,
    Edge = 2,
    PropertyOverflow = 3,
    Label = 4,
}

impl PageType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Free),
            1 => Some(Self::Node),
            2 => Some(Self::Edge),
            3 => Some(Self::PropertyOverflow),
            4 => Some(Self::Label),
            _ => None,
        }
    }

    /// Read the page type directly from a raw page buffer without
    /// constructing a full `Page`.
    pub fn from_page_bytes(data: &[u8]) -> Option<Self> {
        if data.len() > OFF_PAGE_TYPE {
            Self::from_u8(data[OFF_PAGE_TYPE])
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Page header layout (32 bytes)
// ---------------------------------------------------------------------------
//
// Offset  Size  Field
// 0       8     page_id
// 8       8     lsn (log sequence number)
// 16      2     record_count (active records)
// 18      2     free_head (first free slot, SLOT_NONE = full)
// 20      1     page_type
// 21      1     flags (reserved)
// 22      4     checksum (CRC32C over bytes 0..22 ++ 26..PAGE_SIZE)
// 26      6     reserved
//
// Total: 32

const OFF_PAGE_ID: usize = 0;
const OFF_LSN: usize = 8;
const OFF_RECORD_COUNT: usize = 16;
const OFF_FREE_HEAD: usize = 18;
const OFF_PAGE_TYPE: usize = 20;
const OFF_CHECKSUM: usize = 22;

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

/// An 8 KB database page containing up to 127 fixed-size (64-byte) records.
///
/// Free slots are managed as a singly-linked list threaded through the first
/// two bytes of each unused record (a `u16` next-pointer).
pub struct Page {
    data: [u8; PAGE_SIZE],
}

impl Page {
    /// Create a new, empty page.
    pub fn new(page_id: PageId, page_type: PageType) -> Self {
        let mut page = Self {
            data: [0u8; PAGE_SIZE],
        };
        page.set_page_id(page_id);
        page.set_page_type(page_type);
        page.set_record_count(0);

        // Build the free list: slot 0 → 1 → 2 → … → 126 → NONE
        for slot in 0..RECORDS_PER_PAGE as u16 {
            let next = if slot + 1 < RECORDS_PER_PAGE as u16 {
                slot + 1
            } else {
                SLOT_NONE
            };
            let off = Self::slot_offset(slot);
            page.data[off..off + 2].copy_from_slice(&next.to_le_bytes());
        }
        page.set_free_head(0);
        page.update_checksum();
        page
    }

    /// Wrap an existing raw page buffer.
    pub fn from_bytes(data: [u8; PAGE_SIZE]) -> Self {
        Self { data }
    }

    // -- header accessors ---------------------------------------------------

    pub fn page_id(&self) -> PageId {
        u64::from_le_bytes(self.data[OFF_PAGE_ID..OFF_PAGE_ID + 8].try_into().unwrap())
    }

    pub fn lsn(&self) -> u64 {
        u64::from_le_bytes(self.data[OFF_LSN..OFF_LSN + 8].try_into().unwrap())
    }

    pub fn record_count(&self) -> u16 {
        u16::from_le_bytes(
            self.data[OFF_RECORD_COUNT..OFF_RECORD_COUNT + 2]
                .try_into()
                .unwrap(),
        )
    }

    pub fn page_type(&self) -> Result<PageType> {
        PageType::from_u8(self.data[OFF_PAGE_TYPE]).ok_or_else(|| graft_core::Error::CorruptPage {
            page: self.page_id(),
            detail: format!("invalid page type byte: {:#04x}", self.data[OFF_PAGE_TYPE]),
        })
    }

    pub fn is_full(&self) -> bool {
        self.free_head() == SLOT_NONE
    }

    pub fn set_lsn(&mut self, lsn: u64) {
        self.data[OFF_LSN..OFF_LSN + 8].copy_from_slice(&lsn.to_le_bytes());
    }

    // -- slot management ----------------------------------------------------

    /// Allocate a free slot, returning its index, or `None` if the page is full.
    pub fn alloc_slot(&mut self) -> Option<u16> {
        let head = self.free_head();
        if head == SLOT_NONE {
            return None;
        }
        // Read next pointer from the free slot
        let off = Self::slot_offset(head);
        let next = u16::from_le_bytes(self.data[off..off + 2].try_into().unwrap());
        self.set_free_head(next);
        // Zero the slot
        self.data[off..off + RECORD_SIZE].fill(0);
        self.set_record_count(self.record_count() + 1);
        Some(head)
    }

    /// Return a slot to the free list.
    pub fn free_slot(&mut self, slot: u16) {
        debug_assert!((slot as usize) < RECORDS_PER_PAGE);
        let off = Self::slot_offset(slot);
        // Thread this slot into the free list head
        let old_head = self.free_head();
        self.data[off..off + 2].copy_from_slice(&old_head.to_le_bytes());
        // Zero the rest of the slot
        self.data[off + 2..off + RECORD_SIZE].fill(0);
        self.set_free_head(slot);
        self.set_record_count(self.record_count() - 1);
    }

    // -- record access ------------------------------------------------------

    /// Read the raw 64-byte record at `slot`.
    pub fn read_record(&self, slot: u16) -> Result<&[u8]> {
        let off = Self::checked_slot_offset(self.page_id(), slot)?;
        Ok(&self.data[off..off + RECORD_SIZE])
    }

    /// Write a 64-byte record into `slot`.
    pub fn write_record(&mut self, slot: u16, record: &[u8]) -> Result<()> {
        debug_assert_eq!(record.len(), RECORD_SIZE);
        let off = Self::checked_slot_offset(self.page_id(), slot)?;
        self.data[off..off + RECORD_SIZE].copy_from_slice(record);
        Ok(())
    }

    // -- checksum -----------------------------------------------------------

    pub fn compute_checksum(&self) -> u32 {
        // Hash everything except the checksum field itself (bytes 22..26).
        let mut h = crc32c::crc32c(&self.data[..OFF_CHECKSUM]);
        h = crc32c::crc32c_append(h, &self.data[OFF_CHECKSUM + 4..]);
        h
    }

    pub fn verify_checksum(&self) -> bool {
        self.stored_checksum() == self.compute_checksum()
    }

    pub fn update_checksum(&mut self) {
        let crc = self.compute_checksum();
        self.data[OFF_CHECKSUM..OFF_CHECKSUM + 4].copy_from_slice(&crc.to_le_bytes());
    }

    /// Return the indices of all occupied (non-free) slots.
    pub fn occupied_slots(&self) -> Vec<u16> {
        let mut is_free = [false; RECORDS_PER_PAGE];
        let mut slot = self.free_head();
        while slot != SLOT_NONE && (slot as usize) < RECORDS_PER_PAGE {
            is_free[slot as usize] = true;
            let off = Self::slot_offset(slot);
            slot = u16::from_le_bytes(self.data[off..off + 2].try_into().unwrap());
        }
        (0..RECORDS_PER_PAGE as u16)
            .filter(|&s| !is_free[s as usize])
            .collect()
    }

    // -- raw access ---------------------------------------------------------

    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    pub fn as_bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.data
    }

    // -- internal -----------------------------------------------------------

    fn free_head(&self) -> u16 {
        u16::from_le_bytes(
            self.data[OFF_FREE_HEAD..OFF_FREE_HEAD + 2]
                .try_into()
                .unwrap(),
        )
    }

    fn set_free_head(&mut self, v: u16) {
        self.data[OFF_FREE_HEAD..OFF_FREE_HEAD + 2].copy_from_slice(&v.to_le_bytes());
    }

    fn set_page_id(&mut self, id: PageId) {
        self.data[OFF_PAGE_ID..OFF_PAGE_ID + 8].copy_from_slice(&id.to_le_bytes());
    }

    fn set_page_type(&mut self, t: PageType) {
        self.data[OFF_PAGE_TYPE] = t as u8;
    }

    fn set_record_count(&mut self, n: u16) {
        self.data[OFF_RECORD_COUNT..OFF_RECORD_COUNT + 2].copy_from_slice(&n.to_le_bytes());
    }

    fn stored_checksum(&self) -> u32 {
        u32::from_le_bytes(
            self.data[OFF_CHECKSUM..OFF_CHECKSUM + 4]
                .try_into()
                .unwrap(),
        )
    }

    fn slot_offset(slot: u16) -> usize {
        PAGE_HEADER_SIZE + (slot as usize) * RECORD_SIZE
    }

    fn checked_slot_offset(page_id: PageId, slot: u16) -> Result<usize> {
        if (slot as usize) >= RECORDS_PER_PAGE {
            return Err(graft_core::Error::SlotOutOfRange {
                page: page_id,
                slot,
            });
        }
        Ok(Self::slot_offset(slot))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_page_header() {
        let page = Page::new(42, PageType::Node);
        assert_eq!(page.page_id(), 42);
        assert_eq!(page.page_type().unwrap(), PageType::Node);
        assert_eq!(page.record_count(), 0);
        assert!(!page.is_full());
        assert!(page.verify_checksum());
    }

    #[test]
    fn alloc_and_free_slots() {
        let mut page = Page::new(1, PageType::Node);

        // Allocate all 127 slots
        let mut slots = Vec::new();
        for _ in 0..RECORDS_PER_PAGE {
            slots.push(page.alloc_slot().expect("should have free slot"));
        }
        assert_eq!(page.record_count(), RECORDS_PER_PAGE as u16);
        assert!(page.is_full());
        assert!(page.alloc_slot().is_none());

        // Free one and re-allocate
        page.free_slot(slots[50]);
        assert_eq!(page.record_count(), (RECORDS_PER_PAGE - 1) as u16);
        assert!(!page.is_full());
        let reused = page.alloc_slot().unwrap();
        assert_eq!(reused, slots[50]);
    }

    #[test]
    fn read_write_record() {
        let mut page = Page::new(1, PageType::Edge);
        let slot = page.alloc_slot().unwrap();

        let mut rec = [0u8; RECORD_SIZE];
        rec[0] = 0xAB;
        rec[63] = 0xCD;
        page.write_record(slot, &rec).unwrap();

        let read = page.read_record(slot).unwrap();
        assert_eq!(read[0], 0xAB);
        assert_eq!(read[63], 0xCD);
    }

    #[test]
    fn slot_out_of_range() {
        let page = Page::new(1, PageType::Node);
        assert!(page.read_record(RECORDS_PER_PAGE as u16).is_err());
    }

    #[test]
    fn checksum_detects_corruption() {
        let mut page = Page::new(1, PageType::Node);
        page.update_checksum();
        assert!(page.verify_checksum());

        // Corrupt a byte
        page.data[PAGE_HEADER_SIZE + 10] ^= 0xFF;
        assert!(!page.verify_checksum());
    }

    #[test]
    fn from_bytes_round_trip() {
        let page = Page::new(99, PageType::Edge);
        let bytes = *page.as_bytes();
        let page2 = Page::from_bytes(bytes);
        assert_eq!(page2.page_id(), 99);
        assert_eq!(page2.page_type().unwrap(), PageType::Edge);
    }

    #[test]
    fn corrupt_page_type_detected() {
        let mut page = Page::new(1, PageType::Node);
        page.data[OFF_PAGE_TYPE] = 0xFF;
        assert!(page.page_type().is_err());
    }
}
