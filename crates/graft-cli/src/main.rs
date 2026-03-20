use clap::Parser;
use comfy_table::{ContentArrangement, Table};
use graft_client::Client;
use graft_core::protocol::DEFAULT_PORT;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

#[derive(Parser)]
#[command(name = "graft-cli", about = "Interactive REPL for graft")]
struct Args {
    /// Server address (host:port)
    #[arg(default_value_t = format!("127.0.0.1:{DEFAULT_PORT}"))]
    address: String,
}

fn main() {
    let args = Args::parse();

    let mut client = match Client::connect(&args.address) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to connect to {}: {e}", args.address);
            std::process::exit(1);
        }
    };

    eprintln!("connected to {}", args.address);
    eprintln!("type \\quit to exit, \\help for help\n");

    let mut rl = DefaultEditor::new().unwrap();
    let mut buffer = String::new();

    loop {
        let prompt = if buffer.is_empty() { "graft> " } else { "   ... " };

        match rl.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();

                // Commands
                if buffer.is_empty() {
                    match trimmed {
                        "\\quit" | "\\q" | "\\exit" => break,
                        "\\help" | "\\h" | "\\?" => {
                            print_help();
                            continue;
                        }
                        "" => continue,
                        _ => {}
                    }
                }

                buffer.push_str(&line);

                // Execute when line ends with semicolon
                if trimmed.ends_with(';') {
                    // Remove trailing semicolon
                    let query = buffer.trim().trim_end_matches(';').trim();
                    if !query.is_empty() {
                        let _ = rl.add_history_entry(&buffer);
                        execute_query(&mut client, query);
                    }
                    buffer.clear();
                } else {
                    buffer.push('\n');
                }
            }
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => break,
            Err(e) => {
                eprintln!("readline error: {e}");
                break;
            }
        }
    }

    eprintln!("goodbye");
}

fn execute_query(client: &mut Client, query: &str) {
    match client.query(query) {
        Ok(result) => {
            if result.columns.is_empty() {
                // Mutation with no result columns
                println!(
                    "OK ({} rows affected, {} ms)",
                    result.rows_affected, result.elapsed_ms
                );
            } else {
                let mut table = Table::new();
                table.set_content_arrangement(ContentArrangement::Dynamic);
                table.set_header(&result.columns);
                for row in &result.rows {
                    table.add_row(row);
                }
                println!("{table}");
                println!(
                    "{} row(s) ({} ms)",
                    result.rows.len(),
                    result.elapsed_ms
                );
            }
            println!();
        }
        Err(e) => {
            eprintln!("error: {e}\n");
        }
    }
}

fn print_help() {
    println!("graft CLI commands:");
    println!("  \\quit, \\q    Exit the REPL");
    println!("  \\help, \\h    Show this help");
    println!();
    println!("Enter GQL queries terminated with a semicolon (;)");
    println!("Multi-line input is supported — press Enter to continue on the next line.");
    println!();
    println!("Examples:");
    println!("  CREATE (p:Person {{name: 'Alice', age: 30}});");
    println!("  MATCH (p:Person) RETURN p.name, p.age;");
    println!("  MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name;");
    println!();
}
