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

        match prepare_statement(line) {
            Ok(stmt) => {
                execute_statement(stmt);
            }
            Err(errmsg) => {
                println!("{errmsg}");
            }
        }
    }
}

fn prepare_statement(line: &str) -> Result<Statement, String> {
    let mut tokens = line.split_whitespace();
    match tokens.next() {
        Some(token) => match token.to_lowercase().as_str() {
            "select" => Ok(Statement::Select(SelectStatement {})),
            "create" => Ok(Statement::Create(CreateTableStmt {})),
            "insert" => Ok(Statement::Insert(InsertStatement {})),
            "delete" => Ok(Statement::Delete(DeleteStatement {})),
            "update" => Ok(Statement::Update(UpdateStatement {})),
            _ => Err("unknown sql statement".to_string()),
        },
        None => unreachable!("empty line should already be filtered"),
    }
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

fn execute_statement(_stmt: Statement) {
    println!("inserting...");
}
