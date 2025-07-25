use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, Write},
};

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let path = if args.len() == 2 {
        args[1].clone()
    } else {
        "::memory::".to_string()
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
            if table.rows.is_empty() {
                table.deserialize();
            }
            for row in &table.rows {
                println!("{row:#?}");
            }
        }
        Statement::Insert(insert_stmt) => {
            table.rows.push(insert_stmt.row);
        }
        Statement::Commit => {
            // persist table data into file
            match table.serialize() {
                Ok(_) => println!("commit OK"),
                Err(err) => println!("{err:?}"),
            }
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
    fn serialize(&self, out: &mut File) {
        match &self.f1 {
            Some(value) => {
                let _ = out.write(&(value.len() as u32).to_be_bytes());
                let _ = out.write(value.clone().as_bytes());
            }
            None => {
                let _ = out.write(&0u32.to_be_bytes());
            }
        }
        match &self.f2 {
            Some(value) => {
                let _ = out.write(&(value.len() as u32).to_be_bytes());
                let _ = out.write(value.clone().as_bytes());
            }
            None => {
                let _ = out.write(&0u32.to_be_bytes());
            }
        }
        match &self.f3 {
            Some(value) => {
                let _ = out.write(&(value.len() as u32).to_be_bytes());
                let _ = out.write(value.clone().as_bytes());
            }
            None => {
                let _ = out.write(&0u32.to_be_bytes());
            }
        }
    }

    fn deserialize(input: &mut File) -> std::io::Result<Self> {
        let mut row = Row::new();

        let mut len_buf = [0u8; 4];
        input.read_exact(&mut len_buf)?;
        let len = u32::from_be_bytes(len_buf);
        if len == 0 {
            row.f1 = None;
        } else {
            let mut buf = vec![0u8; len as usize];
            input.read_exact(&mut buf)?;
            row.f1 = Some(String::from_utf8(buf).unwrap());
        }

        let mut len_buf = [0u8; 4];
        input.read_exact(&mut len_buf)?;
        let len = u32::from_be_bytes(len_buf);
        if len == 0 {
            row.f2 = None;
        } else {
            let mut buf = vec![0u8; len as usize];
            input.read_exact(&mut buf)?;
            row.f2 = Some(String::from_utf8(buf).unwrap());
        }

        let mut len_buf = [0u8; 4];
        input.read_exact(&mut len_buf)?;
        let len = u32::from_be_bytes(len_buf);
        if len == 0 {
            row.f3 = None;
        } else {
            let mut buf = vec![0u8; len as usize];
            input.read_exact(&mut buf)?;
            row.f3 = Some(String::from_utf8(buf).unwrap());
        }

        Ok(row)
    }
}

struct Table {
    rows: Vec<Row>,
    pager: Option<File>,
}

impl Table {
    fn new(path: String) -> Self {
        let file = if path == "::memory::" {
            None
        } else {
            Some(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(path)
                    .unwrap()
            )
        };
        Self {
            rows: vec![],
            pager: file,
        }
    }

    fn serialize(&mut self) -> std::io::Result<()> {
        if self.pager.is_none() {
            return Ok(());
        }
        let pager = self.pager.as_mut().unwrap();
        // write from the beginning
        pager.seek(std::io::SeekFrom::Start(0))?;
        for row in &self.rows {
            row.serialize(pager);
        }
        Ok(())
    }

    fn deserialize(&mut self) {
        if self.pager.is_none() {
            return;
        }
        let pager = self.pager.as_mut().unwrap();
        while let Ok(row) = Row::deserialize(pager) {
            self.rows.push(row);
        }
    }
}
