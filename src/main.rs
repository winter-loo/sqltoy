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
                    row: Row {
                        f1: tokens.next().map(|s| s.to_string()),
                        f2: tokens.next().map(|s| s.to_string()),
                        f3: tokens.next().map(|s| s.to_string()),
                    },
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
}

impl Row {
    fn new() -> Self {
        Self {
            f1: None,
            f2: None,
            f3: None,
        }
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

    fn deserialize(input: &[u8]) -> Option<(Row, usize)> {
        let mut row = Row::new();

        let len_size = std::mem::size_of::<u32>();
        let mut tot_len: usize = len_size;
        if input.len() < tot_len {
            return None;
        }
        let len = u32::from_be_bytes([
            input[tot_len - 4],
            input[tot_len - 3],
            input[tot_len - 2],
            input[tot_len - 1],
        ]);
        if len == 0 {
            row.f1 = None;
        } else {
            let len = len as usize;
            tot_len += len;
            if input.len() < tot_len {
                return None;
            }
            row.f1 = Some(String::from_utf8(input[tot_len - len..tot_len].to_vec()).unwrap());
        }

        // deserialize f2
        tot_len += len_size;
        if input.len() < tot_len {
            return None;
        }
        let len = u32::from_be_bytes([
            input[tot_len - 4],
            input[tot_len - 3],
            input[tot_len - 2],
            input[tot_len - 1],
        ]);
        if len == 0 {
            row.f2 = None;
        } else {
            let len = len as usize;
            tot_len += len;
            if input.len() < tot_len {
                return None;
            }

            row.f2 = Some(String::from_utf8(input[tot_len - len..tot_len].to_vec()).unwrap());
        }

        // deserialize f3
        tot_len += len_size;
        if input.len() < tot_len {
            return None;
        }
        let len = u32::from_be_bytes([
            input[tot_len - 4],
            input[tot_len - 3],
            input[tot_len - 2],
            input[tot_len - 1],
        ]);
        if len == 0 {
            row.f3 = None;
        } else {
            let len = len as usize;
            tot_len += len;
            if input.len() < tot_len {
                return None;
            }

            row.f3 = Some(String::from_utf8(input[tot_len - len..tot_len].to_vec()).unwrap());
        }

        Some((row, tot_len))
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

const PAGE_SIZE: usize = 40;
const PAGE_HEADER_SIZE: usize = 2;

#[derive(Debug, Clone, Copy)]
struct Page {
    data: [u8; PAGE_SIZE],
    len: u16,
    dirty: bool,
}

impl Page {
    fn new() -> Self {
        Self {
            data: [0; PAGE_SIZE],
            // page header length
            len: PAGE_HEADER_SIZE as u16,
            dirty: false,
        }
    }

    fn create_from_buf(buf: [u8; PAGE_SIZE]) -> Self {
        // get page payload length
        let len = u16::from_be_bytes([buf[0], buf[1]]);
        Self {
            data: buf,
            len,
            dirty: false,
        }
    }

    fn is_dirty(&self) -> bool {
        self.dirty
    }

    fn set_dirty(&mut self) {
        self.dirty = true;
    }

    /// return true only if the page has enough space to hold the row
    fn insert(&mut self, row: &Row) -> bool {
        let mut buf = vec![];
        row.serialize(&mut buf);
        if PAGE_SIZE - (self.len as usize) < buf.len() {
            false
        } else {
            let len = self.len as usize;
            self.data[len..len + buf.len()].copy_from_slice(&buf);
            self.len += buf.len() as u16;
            // update page header
            self.data[0..PAGE_HEADER_SIZE].copy_from_slice(&self.len.to_be_bytes());
            self.set_dirty();
            true
        }
    }

    fn get_row_at(&self, offset: usize) -> Option<(Row, usize)> {
        Row::deserialize(&self.data[PAGE_HEADER_SIZE + offset..self.len as usize])
    }

    fn iter(&self) -> PageIterator<'_> {
        PageIterator {
            page: self,
            row_offset: 0,
        }
    }
}

struct PageIterator<'a> {
    page: &'a Page,
    row_offset: usize,
}

impl<'a> Iterator for PageIterator<'a> {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.page.get_row_at(self.row_offset);
        if let Some((row, row_size)) = row {
            self.row_offset += row_size;
            Some(row)
        } else {
            None
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
        let page = Page::new();
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
