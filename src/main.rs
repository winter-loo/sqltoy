use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
};

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let path = if args.len() == 2 {
        args[1].clone()
    } else {
        panic!("usage: {} <filename>", args[0]);
    };
    let mut table = Table::new(path);

    loop {
        print!("sqltoy> ");
        let _ = std::io::stdout().flush();

        let mut line = String::new();
        let _ = std::io::stdin().read_line(&mut line);
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        // handle meta-commands
        if line.starts_with('.') {
            let line = line.to_lowercase();
            match line.as_str() {
                ".quit" | ".q" => {
                    return;
                }
                _ => eprintln!("unknown command"),
            }

            continue;
        }

        match sqlite3_prepare(line) {
            Ok(stmt) => {
                sqlite3_step(stmt, &mut table);
            }
            Err(errmsg) => {
                println!("{errmsg}");
            }
        }
    }
}

fn sqlite3_prepare(line: &str) -> Result<Sqlite3Stmt, String> {
    let mut tokens = line.split_whitespace();
    match tokens.next() {
        Some(token) => {
            let stmt = match token.to_lowercase().as_str() {
                "select" => Statement::Select(SelectStatement {}),
                "create" => Statement::Create(CreateTableStmt {}),
                "insert" => Statement::Insert(InsertStatement {
                    row: Row::new(
                        tokens.next().map(|s| s.to_string()),
                        tokens.next().map(|s| s.to_string()),
                        tokens.next().map(|s| s.to_string()),
                    ),
                }),
                "delete" => Statement::Delete(DeleteStatement {}),
                "update" => Statement::Update(UpdateStatement {}),
                "commit" => Statement::Commit,
                _ => {
                    return Err("unknown sql statement".to_string());
                }
            };
            Ok(Sqlite3Stmt(Vdbe { stmt }))
        }
        None => unreachable!("empty line should already be filtered"),
    }
}

fn sqlite3_step(stmt: Sqlite3Stmt, table: &mut Table) {
    let vdbe = stmt.0;
    match vdbe.stmt {
        Statement::Select(_select_stmt) => {
            for page in table.pager.iter_mut() {
                for row in page.iter() {
                    println!("{row}");
                }
            }
        }
        Statement::Insert(insert_stmt) => {
            table.insert(insert_stmt.row);
        }
        Statement::Commit => {
            // persist table data into file
            table.pager.flush();
            println!("commit OK");
        }
        _ => {}
    }
}

pub struct Sqlite3Stmt(Vdbe);

/// Comment from sqlite3:
///
/// ```
/// The "sqlite3_stmt" structure pointer that is returned by sqlite3_prepare()
/// is really a pointer to an instance of this structure.
/// ```
struct Vdbe {
    stmt: Statement,
}

enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Create(CreateTableStmt),
    Delete(DeleteStatement),
    Update(UpdateStatement),
    Commit,
}

struct SelectStatement {}

struct InsertStatement {
    row: Row,
}

struct CreateTableStmt {}

struct DeleteStatement {}

struct UpdateStatement {}

#[derive(Debug)]
struct Row {
    f1: Option<String>,
    f2: Option<String>,
    f3: Option<String>,
    len: usize,
}

impl Row {
    fn new(f1: Option<String>, f2: Option<String>, f3: Option<String>) -> Self {
        let header_len = std::mem::size_of::<u32>();
        let len = 3 * header_len
            + f1.as_ref().map(|s| s.len()).unwrap_or(0)
            + f2.as_ref().map(|s| s.len()).unwrap_or(0)
            + f3.as_ref().map(|s| s.len()).unwrap_or(0);

        Self { f1, f2, f3, len }
    }

    /// write String to disk using '<length><value>' format
    /// if '<length>' is zero, then no '<value>' is written!
    /// The '<length>' occupies 4 bytes.
    fn serialize_to_slice(&self, out: &mut [u8]) -> usize {
        let mut nbytes = 0;
        match &self.f1 {
            Some(value) => {
                out[nbytes..nbytes + 4].copy_from_slice(&(value.len() as u32).to_be_bytes());
                out[nbytes + 4..nbytes + 4 + value.len()].copy_from_slice(value.as_bytes());
                nbytes += std::mem::size_of::<u32>() + value.len();
            }
            None => {
                out[nbytes..nbytes + 4].copy_from_slice(&0u32.to_be_bytes());
                nbytes += std::mem::size_of::<u32>();
            }
        }
        match &self.f2 {
            Some(value) => {
                out[nbytes..nbytes + 4].copy_from_slice(&(value.len() as u32).to_be_bytes());
                out[nbytes + 4..nbytes + 4 + value.len()].copy_from_slice(value.as_bytes());
                nbytes += std::mem::size_of::<u32>() + value.len();
            }
            None => {
                out[nbytes..nbytes + 4].copy_from_slice(&0u32.to_be_bytes());
                nbytes += std::mem::size_of::<u32>();
            }
        }
        match &self.f3 {
            Some(value) => {
                out[nbytes..nbytes + 4].copy_from_slice(&(value.len() as u32).to_be_bytes());
                out[nbytes + 4..nbytes + 4 + value.len()].copy_from_slice(value.as_bytes());
                nbytes += std::mem::size_of::<u32>() + value.len();
            }
            None => {
                out[nbytes..nbytes + 4].copy_from_slice(&0u32.to_be_bytes());
                nbytes += std::mem::size_of::<u32>();
            }
        }

        nbytes
    }

    /// write String to disk using '<length><value>' format
    /// if '<length>' is zero, then no '<value>' is written!
    /// The '<length>' occupies 4 bytes.
    fn serialize(&self, out: &mut Vec<u8>) -> usize {
        let mut nbytes = 0;
        match &self.f1 {
            Some(value) => {
                out.extend_from_slice(&(value.len() as u32).to_be_bytes());
                out.extend_from_slice(value.as_bytes());
                nbytes += std::mem::size_of::<u32>() + value.len();
            }
            None => {
                out.extend_from_slice(&0u32.to_be_bytes());
                nbytes += std::mem::size_of::<u32>();
            }
        }
        match &self.f2 {
            Some(value) => {
                out.extend_from_slice(&(value.len() as u32).to_be_bytes());
                out.extend_from_slice(value.as_bytes());
                nbytes += std::mem::size_of::<u32>() + value.len();
            }
            None => {
                out.extend_from_slice(&0u32.to_be_bytes());
                nbytes += std::mem::size_of::<u32>();
            }
        }
        match &self.f3 {
            Some(value) => {
                out.extend_from_slice(&(value.len() as u32).to_be_bytes());
                out.extend_from_slice(value.as_bytes());
                nbytes += std::mem::size_of::<u32>() + value.len();
            }
            None => {
                out.extend_from_slice(&0u32.to_be_bytes());
                nbytes += std::mem::size_of::<u32>();
            }
        }

        nbytes
    }

    fn deserialize(input: &[u8]) -> Option<Row> {
        let len_size = std::mem::size_of::<u32>();
        let mut tot_len: usize = len_size;
        assert!(input.len() >= tot_len);

        let len = u32::from_be_bytes([
            input[tot_len - 4],
            input[tot_len - 3],
            input[tot_len - 2],
            input[tot_len - 1],
        ]);
        let f1 = if len == 0 {
            None
        } else {
            let len = len as usize;
            tot_len += len;
            assert!(input.len() >= tot_len);
            Some(String::from_utf8(input[tot_len - len..tot_len].to_vec()).unwrap())
        };

        // deserialize f2
        tot_len += len_size;
        assert!(input.len() >= tot_len);
        let len = u32::from_be_bytes([
            input[tot_len - 4],
            input[tot_len - 3],
            input[tot_len - 2],
            input[tot_len - 1],
        ]);
        let f2 = if len == 0 {
            None
        } else {
            let len = len as usize;
            tot_len += len;
            assert!(input.len() >= tot_len);

            Some(String::from_utf8(input[tot_len - len..tot_len].to_vec()).unwrap())
        };

        // deserialize f3
        tot_len += len_size;
        assert!(input.len() >= tot_len);
        let len = u32::from_be_bytes([
            input[tot_len - 4],
            input[tot_len - 3],
            input[tot_len - 2],
            input[tot_len - 1],
        ]);
        let f3 = if len == 0 {
            None
        } else {
            let len = len as usize;
            tot_len += len;
            assert!(input.len() >= tot_len);

            Some(String::from_utf8(input[tot_len - len..tot_len].to_vec()).unwrap())
        };

        Some(Row::new(f1, f2, f3))
    }
}

impl std::fmt::Display for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(f1) = &self.f1 {
            write!(f, "{f1}")?;
        } else {
            write!(f, "null")?;
        }
        write!(f, "|")?;
        if let Some(f2) = &self.f2 {
            write!(f, "{f2}")?;
        } else {
            write!(f, "null")?;
        }
        write!(f, "|")?;
        if let Some(f3) = &self.f3 {
            write!(f, "{f3}")?;
        } else {
            write!(f, "null")?;
        }
        write!(f, "")
    }
}

#[cfg(feature = "debug_page_layout")]
const PAGE_SIZE: usize = 32;
#[cfg(not(feature = "debug_page_layout"))]
const PAGE_SIZE: usize = 4096;

/// page structure:
///
/// page type + num cells
#[derive(Debug, Clone, Copy)]
struct Page {
    data: [u8; PAGE_SIZE],
    dirty: bool,
}

enum PageType {
    IndexInterior = 2,
    TableInterior = 5,
    IndexLeaf = 10,
    TableLeaf = 13,
}

impl From<u8> for PageType {
    fn from(value: u8) -> Self {
        if value == PageType::IndexInterior as u8 {
            PageType::IndexInterior
        } else if value == PageType::TableInterior as u8 {
            PageType::TableInterior
        } else if value == PageType::IndexLeaf as u8 {
            PageType::IndexLeaf
        } else if value == PageType::TableLeaf as u8 {
            PageType::TableLeaf
        } else {
            panic!("invalid page type value");
        }
    }
}

impl From<PageType> for u8 {
    fn from(value: PageType) -> Self {
        match value {
            PageType::IndexInterior => PageType::IndexInterior as u8,
            PageType::TableInterior => PageType::TableInterior as u8,
            PageType::IndexLeaf => PageType::IndexLeaf as u8,
            PageType::TableLeaf => PageType::TableLeaf as u8,
        }
    }
}

struct PageNo(u32);

/// utilities for accessing page structure
/// https://devnotes.ldd.cool/notes-sqlite/update_flow#database-file-format
impl Page {
    const PAGE_TYPE_OFFSET: usize = 0;
    const PAGE_TYPE_SIZE: usize = 1;
    const FIRST_FREE_BLOCK_OFFSET: usize = 1;
    const FIRST_FREE_BLOCK_SIZE: usize = 2;
    const NUM_CELLS_OFFSET: usize = 3;
    const NUM_CELLS_SIZE: usize = 2;
    const CELL_START_OFFSET: usize = 5;
    const CELL_START_SIZE: usize = 2;
    const NUM_FRAGMENTED_OFFSET: usize = 7;
    const NUM_FRAGMENTED_SIZE: usize = 1;
    const RIGHT_CHILD_OFFSET: usize = 8;
    const RIGHT_CHILD_SIZE: usize = 4;
    const CELL_POINTER_SIZE: usize = 2;

    fn page_type(&self) -> PageType {
        u8::from_be_bytes([self.data[Page::PAGE_TYPE_OFFSET]]).into()
    }

    fn set_page_type(&mut self, pt: PageType) {
        self.data[Page::PAGE_TYPE_OFFSET] = pt.into();
    }

    fn num_cells(&self) -> u16 {
        u16::from_be_bytes([
            self.data[Page::NUM_CELLS_OFFSET],
            self.data[Page::NUM_CELLS_OFFSET + 1],
        ])
    }

    fn set_num_cells(&mut self, n: u16) {
        self.data[Page::NUM_CELLS_OFFSET..Page::NUM_CELLS_OFFSET + Page::NUM_CELLS_SIZE]
            .copy_from_slice(&n.to_be_bytes());
    }

    fn cell_start(&self) -> u16 {
        u16::from_be_bytes([
            self.data[Page::CELL_START_OFFSET],
            self.data[Page::CELL_START_OFFSET + 1],
        ])
    }

    fn set_cell_start(&mut self, offset: u16) {
        // why use this cubersome style? `copy_from_slice` will check source and
        // destination length. If they do not match, panic!
        self.data[Page::CELL_START_OFFSET..Page::CELL_START_OFFSET + Page::CELL_START_SIZE]
            .copy_from_slice(&offset.to_be_bytes());
    }

    /// the length of un-allocated page space
    fn unused_len(&self) -> usize {
        let cell_pointers_len = (self.num_cells() as usize) * Page::CELL_POINTER_SIZE;
        let cell_pointers_tail = self.cell_pointer_offset() + cell_pointers_len;
        self.cell_start() as usize - cell_pointers_tail
    }

    fn cell_pointer_offset(&self) -> usize {
        match self.page_type() {
            PageType::IndexInterior | PageType::TableInterior => {
                Page::RIGHT_CHILD_OFFSET + Page::RIGHT_CHILD_SIZE
            }
            PageType::IndexLeaf | PageType::TableLeaf => {
                Page::NUM_FRAGMENTED_OFFSET + Page::NUM_FRAGMENTED_SIZE
            }
        }
    }

    fn cell_offset(&self, n: u16) -> usize {
        let cell_pointer_start = self.cell_pointer_offset();
        let cell_pointer_offset = cell_pointer_start
            + if n == 0 {
                0
            } else {
                (n - 1) as usize * Page::CELL_POINTER_SIZE
            };
        u16::from_be_bytes([
            self.data[cell_pointer_offset],
            self.data[cell_pointer_offset + 1],
        ]) as usize
    }

    fn set_cell_offset(&mut self, n: u16, offset: u16) {
        let cell_pointer_start = self.cell_pointer_offset();
        let off = cell_pointer_start
            + if n == 0 {
                0
            } else {
                (n - 1) as usize * Page::CELL_POINTER_SIZE
            };
        self.data[off..off + Page::CELL_POINTER_SIZE].copy_from_slice(&offset.to_be_bytes());
    }

    fn get_cell(&self, n: u16) -> Cell<'_> {
        let cell = &self.data[dbg!(self.cell_offset(n))..];
        match self.page_type() {
            PageType::TableLeaf => Cell::TableLeaf(TableLeafCell::deserialize(cell)),
            PageType::TableInterior => Cell::TableInterior(TableInteriorCell::parse(cell)),
            PageType::IndexLeaf => Cell::IndexLeaf(IndexLeafCell::parse(cell)),
            PageType::IndexInterior => Cell::IndexInterior(IndexInteriorCell::parse(cell)),
        }
    }
}

enum Cell<'a> {
    TableLeaf(TableLeafCell<'a>),
    TableInterior(TableInteriorCell),
    IndexLeaf(IndexLeafCell<'a>),
    IndexInterior(IndexInteriorCell<'a>),
}

struct TableLeafCell<'a> {
    rowid: u64,
    payload: &'a [u8],
}

impl<'a> TableLeafCell<'a> {
    fn deserialize(data: &'a [u8]) -> Self {
        // first varint: number of bytes of payload
        let Varint {
            value: payload_len,
            len: offset1,
        } = Varint::read_from_slice(data).unwrap();

        // second varint: rowid
        let Varint {
            value: rowid,
            len: offset2,
        } = Varint::read_from_slice(&data[offset1..]).unwrap();
        println!("payload_len {payload_len}");

        // deserialize payload and the page number of the first overflow page
        let payload = if payload_len as usize > PAGE_SIZE - offset1 - offset2 {
            let off = PAGE_SIZE - std::mem::size_of::<PageNo>();
            let _first_overflow_page = Some(PageNo(u32::from_be_bytes([
                data[off],
                data[off + 1],
                data[off + 2],
                data[off + 3],
            ])));

            let _payload = todo!("read from next page");
        } else {
            let off = offset1 + offset2;
            &data[off..off + payload_len as usize]
        };
        Self { rowid, payload }
    }

    fn serialize(row: &Row, cell_content: &mut [u8]) {
        // 1. write the first varint
        let mut off = Varint::into_bytes(row.len as u64, cell_content);

        // 2. write the second varint
        off += Varint::into_bytes(0, cell_content);

        // 3. write payload
        // Assume no large data written into a row
        // TODO: handle overflow page
        assert!(row.len < PAGE_SIZE - off);
        row.serialize_to_slice(&mut cell_content[off..off + row.len]);
    }

    fn get_row(&self) -> Row {
        Row::deserialize(self.payload).unwrap()
    }
}

struct TableInteriorCell {
    left_child: PageNo,
    rowid: u64,
}

impl TableInteriorCell {
    fn parse(data: &[u8]) -> Self {
        let left_child = PageNo(u32::from_be_bytes([data[0], data[1], data[2], data[3]]));
        let rowid = Varint::read_from_slice(&data[4..]).unwrap().value;
        Self { left_child, rowid }
    }
}

struct IndexLeafCell<'a> {
    payload: &'a [u8],
}

impl<'a> IndexLeafCell<'a> {
    fn parse(data: &'a [u8]) -> Self {
        let Varint {
            value: payload_len,
            len: offset1,
        } = Varint::read_from_slice(data).unwrap();
        let payload = if payload_len as usize > PAGE_SIZE - offset1 {
            let off = PAGE_SIZE - std::mem::size_of::<PageNo>();
            let _first_overflow_page = Some(PageNo(u32::from_be_bytes([
                data[off],
                data[off + 1],
                data[off + 2],
                data[off + 3],
            ])));
            let _payload = todo!("read from next page");
        } else {
            &data[offset1..offset1 + payload_len as usize]
        };
        Self { payload }
    }
}

struct IndexInteriorCell<'a> {
    left_child: PageNo,
    payload: &'a [u8],
}

impl<'a> IndexInteriorCell<'a> {
    fn parse(data: &'a [u8]) -> Self {
        let left_child = PageNo(u32::from_be_bytes([data[0], data[1], data[2], data[3]]));
        let offset1 = std::mem::size_of::<u32>();
        let Varint {
            value: payload_len,
            len: offset2,
        } = Varint::read_from_slice(&data[offset1..]).unwrap();
        let off = offset1 + offset2;
        let payload = if payload_len as usize > PAGE_SIZE - off {
            todo!("read from next page")
        } else {
            &data[off..off + payload_len as usize]
        };
        Self {
            left_child,
            payload,
        }
    }
}

struct Varint {
    value: u64,
    len: usize,
}

impl Varint {
    fn read_from_slice(buf: &[u8]) -> Option<Varint> {
        let mut sum = 0u64;
        let mut i = 0;
        while i < buf.len() {
            if i == 9 {
                return None;
            }
            let n = buf[i];
            i += 1;
            if n & 0x80 == 0 {
                sum += n as u64;
                break;
            }
            sum += (n & 0x7f) as u64;
        }
        assert!(i <= buf.len());
        Some(Varint { value: sum, len: i })
    }

    fn into_bytes(value: u64, buf: &mut [u8]) -> usize {
        todo!()
    }
}

impl Page {
    fn create_table_leaf_page() -> Self {
        let mut page = Page {
            data: [0u8; PAGE_SIZE],
            dirty: false,
        };
        page.set_page_type(PageType::TableLeaf);
        page.set_cell_start(PAGE_SIZE as u16);
        page
    }

    fn create_from_buf(buf: [u8; PAGE_SIZE]) -> Self {
        let page = Page {
            data: buf,
            dirty: false,
        };
        assert!(matches!(page.page_type(), PageType::TableLeaf));
        page
    }

    fn is_dirty(&self) -> bool {
        self.dirty
    }

    fn set_dirty(&mut self) {
        self.dirty = true;
    }

    /// return true only if the page has enough space to hold the row
    fn insert(&mut self, row: &Row) -> bool {
        if row.len > self.unused_len() {
            todo!("too long! need a new page")
        } else {
            // fill in some important bits
            let cell_num = self.num_cells() + 1;
            self.set_num_cells(cell_num);
            let end = self.cell_start() as usize;
            let start = end - row.len;
            self.set_cell_offset(cell_num, start as u16);
            self.set_cell_start(start as u16);

            TableLeafCell::serialize(row, &mut self.data[start..start + row.len]);
            println!("row written to offset: {start}");
        }
        self.set_dirty();
        true
    }

    fn iter(&self) -> PageIterator<'_> {
        PageIterator {
            page: self,
            rownum: 0,
        }
    }
}

struct PageIterator<'a> {
    page: &'a Page,
    rownum: u16,
}

impl<'a> Iterator for PageIterator<'a> {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rownum >= self.page.num_cells() {
            return None;
        }
        let cell = self.page.get_cell(self.rownum);
        match cell {
            Cell::TableLeaf(cell) => Some(cell.get_row()),
            _ => todo!("decode other kinds of cells"),
        }
    }
}

struct Pager {
    pages: Vec<Option<Page>>,
    source: File,
    num_phy_pages: u64,
}

impl Pager {
    fn new(path: String) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let file_size = file.metadata()?.len();
        assert!(file_size % PAGE_SIZE as u64 == 0);
        let num_phy_pages = file_size / PAGE_SIZE as u64;
        println!("num_phy_pages {num_phy_pages}");
        let pages = vec![None; num_phy_pages as usize];
        Ok(Self {
            pages,
            source: file,
            num_phy_pages,
        })
    }

    fn get_page_mut(&mut self, page_no: usize) -> Option<&mut Page> {
        if page_no != 0 && page_no <= self.num_phy_pages as usize {
            let page_no = page_no - 1;
            if self.pages[page_no].is_none() {
                let mut buf = [0u8; PAGE_SIZE];
                let _ = self
                    .source
                    .seek(SeekFrom::Start((page_no * PAGE_SIZE) as u64));
                let _ = self.source.read_exact(&mut buf);
                let page = Page::create_from_buf(buf);
                self.pages[page_no] = Some(page);
            }
            self.pages[page_no].as_mut()
        } else {
            None
        }
    }

    fn new_page(&mut self) -> std::io::Result<&mut Page> {
        self.num_phy_pages += 1;
        self.source.set_len(self.num_phy_pages * PAGE_SIZE as u64)?;
        let page = Page::create_table_leaf_page();
        self.pages.push(Some(page));
        Ok(self.pages.last_mut().unwrap().as_mut().unwrap())
    }

    fn flush(&mut self) {
        for (page_no, page) in self.pages.iter().enumerate() {
            if let Some(page) = page {
                if page.is_dirty() {
                    let _ = self
                        .source
                        .seek(SeekFrom::Start((page_no * PAGE_SIZE) as u64));
                    let _ = self.source.write_all(&page.data);
                    let _ = self.source.flush();
                }
            }
        }
    }

    fn iter_mut(&mut self) -> PagerIterator<'_> {
        PagerIterator {
            pager: self,
            current_page: 1,
        }
    }
}

struct PagerIterator<'a> {
    pager: &'a mut Pager,
    current_page: usize,
}

impl<'a> Iterator for PagerIterator<'a> {
    type Item = Page;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_page <= self.pager.num_phy_pages as usize {
            let page = self.pager.get_page_mut(self.current_page);
            self.current_page += 1;
            // copy
            page.map(|page| *page)
        } else {
            None
        }
    }
}

struct Table {
    // table object uses pager object to fetch pages
    pager: Pager,
    writing_page: u64,
}

impl Table {
    fn new(path: String) -> Self {
        let pager = match Pager::new(path) {
            Ok(p) => p,
            Err(err) => panic!("{err}"),
        };
        let writing_page = pager.num_phy_pages;
        Self {
            pager,
            writing_page,
        }
    }

    fn insert(&mut self, row: Row) {
        let mut need_new_page = false;
        match self.pager.get_page_mut(self.writing_page as usize) {
            Some(page) => {
                if !page.insert(&row) {
                    need_new_page = true;
                }
            }
            None => need_new_page = true,
        }
        if need_new_page {
            match self.pager.new_page() {
                Ok(page) => {
                    if !page.insert(&row) {
                        panic!("can not insert page");
                    }
                    self.writing_page += 1;
                }
                Err(err) => panic!("{err:?}"),
            }
        }
    }
}
