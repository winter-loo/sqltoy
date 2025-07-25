use std::io::Write;

fn main() {
    let mut table = Table::new();

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
                    }
                }),
                "delete" => Statement::Delete(DeleteStatement {}),
                "update" => Statement::Update(UpdateStatement {}),
                _ => {
                    return Err("unknown sql statement".to_string());
                }
            };
            Ok(Sqlite3Stmt(Vdbe {stmt}))
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
}

enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Create(CreateTableStmt),
    Delete(DeleteStatement),
    Update(UpdateStatement),
}

struct SelectStatement {}

struct InsertStatement {
    row: Row,
}

struct CreateTableStmt {}

struct DeleteStatement {}

struct UpdateStatement {}

fn sqlite3_step(stmt: Sqlite3Stmt, table: &mut Table) {
    let vdbe = stmt.0;
    match vdbe.stmt {
        Statement::Select(_select_stmt) => {
            for row in &table.rows {
                println!("{row:#?}");
            }
        },
        Statement::Insert(insert_stmt) => {
            table.rows.push(insert_stmt.row);
        },
        _ => {},
    }
}

#[derive(Debug)]
struct Row {
    f1: Option<String>,
    f2: Option<String>,
    f3: Option<String>,
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
