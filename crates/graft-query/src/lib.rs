pub mod ast;
pub mod executor;
pub mod lexer;
pub mod planner;

lalrpop_util::lalrpop_mod!(
    #[allow(clippy::all)]
    #[allow(unused)]
    pub parser
);

pub use executor::{QueryResult, StorageAccess, Value};
pub use planner::Plan;

use lexer::Lexer;

/// Parse a GQL statement from text.
pub fn parse(input: &str) -> Result<ast::Statement, String> {
    let lexer = Lexer::new(input);
    parser::StatementParser::new()
        .parse(lexer)
        .map_err(|e| format!("{e}"))
}
