use std::io::Write;

fn main() {
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
                sqlite3_step(stmt);
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
                "insert" => Statement::Insert(InsertStatement {}),
                "delete" => Statement::Delete(DeleteStatement {}),
                "update" => Statement::Update(UpdateStatement {}),
                _ => {
                    return Err("unknown sql statement".to_string());
                }
            };
            Ok(Sqlite3Stmt(Vdbe {stmt, table: Table::new()}))
        },
        None => unreachable!("empty line should already be filtered"),
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
    table: Table,
}

enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Create(CreateTableStmt),
    Delete(DeleteStatement),
    Update(UpdateStatement),
}

struct SelectStatement {}

struct InsertStatement {}

struct CreateTableStmt {}

struct DeleteStatement {}

struct UpdateStatement {}

fn sqlite3_step(stmt: Sqlite3Stmt) {
    let vdbe = stmt.0;
    match vdbe.stmt {
        Statement::Select(_select_stmt) => {},
        Statement::Insert(_insert_stmt) => {},
        _ => {},
    }
}

struct Row {
    f1: u64,
    f2: u64,
    f3: u64,
}

struct Table {
    rows: Vec<Row>,
}

impl Table {
    fn new() -> Self {
        Self {
            rows: vec![],
        }
    }
}
