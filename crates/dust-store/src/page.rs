//! Page layout codec for the dust storage engine.
//!
//! Each page is PAGE_SIZE bytes with a fixed header, cell pointer array,
//! and cell data growing from the end of the page backward.

pub const PAGE_SIZE: usize = 16384; // 16 KiB
pub const PAGE_HEADER_SIZE: usize = 40;
pub const MAGIC: u32 = 0x44555354; // "DUST"

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    Meta = 1,
    Internal = 2,
    Leaf = 3,
    Overflow = 4,
    FreeList = 5,
}

impl PageType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Meta),
            2 => Some(Self::Internal),
            3 => Some(Self::Leaf),
            4 => Some(Self::Overflow),
            5 => Some(Self::FreeList),
            _ => None,
        }
    }
}

/// Fixed header at the start of every page (40 bytes).
///
/// Layout:
///   [0..4]   magic: u32
///   [4..12]  page_id: u64
///   [12]     page_type: u8
///   [13]     flags: u8
///   [14..16] cell_count: u16
///   [16..18] free_start: u16  (end of cell pointer array = start of free space)
///   [18..20] free_end: u16    (start of cell data area = end of free space)
///   [20..28] right_ptr: u64   (right child or next-leaf pointer)
///   [28..36] parent_ptr: u64
///   [36..40] checksum: [u8; 4] (truncated BLAKE3)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageHeader {
    pub page_id: u64,
    pub page_type: PageType,
    pub flags: u8,
    pub cell_count: u16,
    pub free_start: u16,
    pub free_end: u16,
    pub right_ptr: u64,
    pub parent_ptr: u64,
    pub checksum: [u8; 4],
}

#[derive(Clone)]
pub struct Page {
    data: Box<[u8; PAGE_SIZE]>,
}

impl std::fmt::Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let hdr = self.header();
        f.debug_struct("Page")
            .field("page_id", &hdr.page_id)
            .field("page_type", &hdr.page_type)
            .field("cell_count", &hdr.cell_count)
            .finish()
    }
}

impl Page {
    /// Create a new empty page with given ID and type.
    pub fn new(page_id: u64, page_type: PageType) -> Self {
        let mut data = Box::new([0u8; PAGE_SIZE]);

        // Write header
        data[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        data[4..12].copy_from_slice(&page_id.to_le_bytes());
        data[12] = page_type as u8;
        data[13] = 0; // flags
        data[14..16].copy_from_slice(&0u16.to_le_bytes()); // cell_count
        let free_start = PAGE_HEADER_SIZE as u16;
        let free_end = PAGE_SIZE as u16;
        data[16..18].copy_from_slice(&free_start.to_le_bytes());
        data[18..20].copy_from_slice(&free_end.to_le_bytes());
        data[20..28].copy_from_slice(&0u64.to_le_bytes()); // right_ptr
        data[28..36].copy_from_slice(&0u64.to_le_bytes()); // parent_ptr
        data[36..40].copy_from_slice(&[0u8; 4]); // checksum

        Self { data }
    }

    /// Create page from raw bytes.
    pub fn from_bytes(data: [u8; PAGE_SIZE]) -> Self {
        Self {
            data: Box::new(data),
        }
    }

    /// Read the page header.
    pub fn header(&self) -> PageHeader {
        PageHeader {
            page_id: u64::from_le_bytes(
                self.data[4..12].try_into().expect("8-byte page_id field"),
            ),
            page_type: PageType::from_u8(self.data[12]).unwrap_or(PageType::Meta),
            flags: self.data[13],
            cell_count: u16::from_le_bytes(
                self.data[14..16].try_into().expect("2-byte cell_count field"),
            ),
            free_start: u16::from_le_bytes(
                self.data[16..18].try_into().expect("2-byte free_start field"),
            ),
            free_end: u16::from_le_bytes(
                self.data[18..20].try_into().expect("2-byte free_end field"),
            ),
            right_ptr: u64::from_le_bytes(
                self.data[20..28].try_into().expect("8-byte right_ptr field"),
            ),
            parent_ptr: u64::from_le_bytes(
                self.data[28..36].try_into().expect("8-byte parent_ptr field"),
            ),
            checksum: self.data[36..40].try_into().expect("4-byte checksum field"),
        }
    }

    pub fn page_id(&self) -> u64 {
        u64::from_le_bytes(self.data[4..12].try_into().expect("8-byte page_id field"))
    }

    pub fn page_type(&self) -> PageType {
        PageType::from_u8(self.data[12]).unwrap_or(PageType::Meta)
    }

    pub fn cell_count(&self) -> u16 {
        u16::from_le_bytes(self.data[14..16].try_into().expect("2-byte cell_count field"))
    }

    pub fn set_cell_count(&mut self, count: u16) {
        self.data[14..16].copy_from_slice(&count.to_le_bytes());
    }

    pub fn free_start(&self) -> u16 {
        u16::from_le_bytes(self.data[16..18].try_into().expect("2-byte free_start field"))
    }

    pub fn set_free_start(&mut self, offset: u16) {
        self.data[16..18].copy_from_slice(&offset.to_le_bytes());
    }

    pub fn free_end(&self) -> u16 {
        u16::from_le_bytes(self.data[18..20].try_into().expect("2-byte free_end field"))
    }

    pub fn set_free_end(&mut self, offset: u16) {
        self.data[18..20].copy_from_slice(&offset.to_le_bytes());
    }

    pub fn right_ptr(&self) -> u64 {
        u64::from_le_bytes(self.data[20..28].try_into().expect("8-byte right_ptr field"))
    }

    pub fn set_right_ptr(&mut self, ptr: u64) {
        self.data[20..28].copy_from_slice(&ptr.to_le_bytes());
    }

    pub fn parent_ptr(&self) -> u64 {
        u64::from_le_bytes(self.data[28..36].try_into().expect("8-byte parent_ptr field"))
    }

    pub fn set_parent_ptr(&mut self, ptr: u64) {
        self.data[28..36].copy_from_slice(&ptr.to_le_bytes());
    }

    /// Read the cell pointer at given index. Returns the offset within the page.
    pub fn cell_offset(&self, index: u16) -> u16 {
        let ptr_offset = PAGE_HEADER_SIZE + (index as usize) * 2;
        u16::from_le_bytes(
            self.data[ptr_offset..ptr_offset + 2]
                .try_into()
                .expect("2-byte cell pointer"),
        )
    }

    /// Set the cell pointer at given index.
    fn set_cell_offset(&mut self, index: u16, offset: u16) {
        let ptr_offset = PAGE_HEADER_SIZE + (index as usize) * 2;
        self.data[ptr_offset..ptr_offset + 2].copy_from_slice(&offset.to_le_bytes());
    }

    /// Get the raw cell data at given index.
    pub fn cell_data(&self, index: u16) -> &[u8] {
        let offset = self.cell_offset(index) as usize;
        // Cell format: cell_size(u16) + data
        let size = u16::from_le_bytes(
            self.data[offset..offset + 2]
                .try_into()
                .expect("2-byte cell size"),
        ) as usize;
        &self.data[offset + 2..offset + 2 + size]
    }

    /// Available free space in the page.
    pub fn usable_space(&self) -> usize {
        let fs = self.free_start() as usize;
        let fe = self.free_end() as usize;
        fe.saturating_sub(fs)
    }

    /// Insert a cell at the given logical index position.
    /// Returns false if there is not enough space.
    pub fn insert_cell(&mut self, index: u16, cell_data: &[u8]) -> bool {
        let cell_size = cell_data.len();
        // Need: 2 bytes for cell pointer + 2 bytes for cell size header + cell data
        let needed = 2 + 2 + cell_size;
        if self.usable_space() < needed {
            return false;
        }

        let count = self.cell_count();

        // Write cell data at the end of free space
        let new_free_end = self.free_end() as usize - 2 - cell_size;
        let cell_offset = new_free_end;
        // Write size prefix
        self.data[cell_offset..cell_offset + 2].copy_from_slice(&(cell_size as u16).to_le_bytes());
        // Write cell data
        self.data[cell_offset + 2..cell_offset + 2 + cell_size].copy_from_slice(cell_data);

        // Shift cell pointers to make room at index
        for i in (index..count).rev() {
            let ptr = self.cell_offset(i);
            self.set_cell_offset(i + 1, ptr);
        }

        // Write new cell pointer
        self.set_cell_offset(index, cell_offset as u16);

        // Update header
        self.set_cell_count(count + 1);
        self.set_free_start(self.free_start() + 2); // pointer array grew by 2 bytes
        self.set_free_end(new_free_end as u16);

        true
    }

    /// Remove the cell at given index.
    pub fn remove_cell(&mut self, index: u16) {
        let count = self.cell_count();
        if index >= count {
            return;
        }

        // Shift cell pointers left
        for i in index..count - 1 {
            let ptr = self.cell_offset(i + 1);
            self.set_cell_offset(i, ptr);
        }

        self.set_cell_count(count - 1);
        self.set_free_start(self.free_start() - 2);
        // Note: we don't compact the cell data area on remove (lazy compaction)
    }

    /// Compute BLAKE3 checksum of the page (excluding the checksum field).
    pub fn compute_checksum(&self) -> [u8; 4] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.data[0..36]); // everything before checksum
        hasher.update(&self.data[40..]); // everything after checksum
        let hash = hasher.finalize();
        let bytes = hash.as_bytes();
        [bytes[0], bytes[1], bytes[2], bytes[3]]
    }

    /// Check if the stored checksum matches the computed one.
    pub fn verify_checksum(&self) -> bool {
        self.header().checksum == self.compute_checksum()
    }

    /// Write the correct checksum into the page header.
    pub fn write_checksum(&mut self) {
        let checksum = self.compute_checksum();
        self.data[36..40].copy_from_slice(&checksum);
    }

    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    pub fn into_bytes(self) -> Box<[u8; PAGE_SIZE]> {
        self.data
    }

    pub fn data_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_page_has_correct_header() {
        let page = Page::new(42, PageType::Leaf);
        let hdr = page.header();
        assert_eq!(hdr.page_id, 42);
        assert_eq!(hdr.page_type, PageType::Leaf);
        assert_eq!(hdr.cell_count, 0);
        assert_eq!(hdr.free_start, PAGE_HEADER_SIZE as u16);
        assert_eq!(hdr.free_end, PAGE_SIZE as u16);
        assert_eq!(hdr.right_ptr, 0);
        assert_eq!(hdr.parent_ptr, 0);
    }

    #[test]
    fn insert_and_read_cells() {
        let mut page = Page::new(1, PageType::Leaf);

        assert!(page.insert_cell(0, b"hello"));
        assert!(page.insert_cell(1, b"world"));
        assert_eq!(page.cell_count(), 2);

        assert_eq!(page.cell_data(0), b"hello");
        assert_eq!(page.cell_data(1), b"world");
    }

    #[test]
    fn insert_at_middle() {
        let mut page = Page::new(1, PageType::Leaf);
        page.insert_cell(0, b"aaa");
        page.insert_cell(1, b"ccc");
        page.insert_cell(1, b"bbb"); // insert at index 1

        assert_eq!(page.cell_count(), 3);
        assert_eq!(page.cell_data(0), b"aaa");
        assert_eq!(page.cell_data(1), b"bbb");
        assert_eq!(page.cell_data(2), b"ccc");
    }

    #[test]
    fn remove_cell_shifts_pointers() {
        let mut page = Page::new(1, PageType::Leaf);
        page.insert_cell(0, b"a");
        page.insert_cell(1, b"b");
        page.insert_cell(2, b"c");

        page.remove_cell(1); // remove "b"
        assert_eq!(page.cell_count(), 2);
        assert_eq!(page.cell_data(0), b"a");
        assert_eq!(page.cell_data(1), b"c");
    }

    #[test]
    fn checksum_round_trip() {
        let mut page = Page::new(1, PageType::Leaf);
        page.insert_cell(0, b"test data");
        page.write_checksum();
        assert!(page.verify_checksum());

        // Corrupt the page
        page.data_mut()[100] ^= 0xFF;
        assert!(!page.verify_checksum());
    }

    #[test]
    fn usable_space_decreases_with_cells() {
        let mut page = Page::new(1, PageType::Leaf);
        let initial = page.usable_space();
        page.insert_cell(0, b"hello");
        assert!(page.usable_space() < initial);
    }

    #[test]
    fn page_type_round_trip() {
        for pt in [
            PageType::Meta,
            PageType::Internal,
            PageType::Leaf,
            PageType::Overflow,
            PageType::FreeList,
        ] {
            assert_eq!(PageType::from_u8(pt as u8), Some(pt));
        }
        assert_eq!(PageType::from_u8(255), None);
    }

    #[test]
    fn right_ptr_and_parent_ptr() {
        let mut page = Page::new(1, PageType::Internal);
        page.set_right_ptr(999);
        page.set_parent_ptr(42);
        assert_eq!(page.right_ptr(), 999);
        assert_eq!(page.parent_ptr(), 42);
    }
}
