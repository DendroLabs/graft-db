use std::fmt;

// ---------------------------------------------------------------------------
// Token
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub enum Token {
    // Keywords
    Match,
    Create,
    Where,
    Return,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    Skip,
    Set,
    Delete,
    And,
    Or,
    Not,
    Null,
    True,
    False,
    As,
    Distinct,
    Is,
    Contains,
    Starts,
    Ends,
    With,

    // Literals
    Integer(i64),
    Float(f64),
    StringLit(String),

    // Identifiers
    Ident(String),

    // Symbols
    LParen,   // (
    RParen,   // )
    LBracket, // [
    RBracket, // ]
    LBrace,   // {
    RBrace,   // }
    Colon,    // :
    Comma,    // ,
    Dot,      // .
    DotDot,   // ..
    Dash,     // -
    Arrow,    // ->
    LArrow,   // <-
    Eq,       // =
    Neq,      // <>
    Lt,       // <
    Gt,       // >
    Lte,      // <=
    Gte,      // >=
    Plus,     // +
    Star,     // *
    Slash,    // /
    Percent,  // %
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::Match => write!(f, "MATCH"),
            Token::Create => write!(f, "CREATE"),
            Token::Where => write!(f, "WHERE"),
            Token::Return => write!(f, "RETURN"),
            Token::Order => write!(f, "ORDER"),
            Token::By => write!(f, "BY"),
            Token::Asc => write!(f, "ASC"),
            Token::Desc => write!(f, "DESC"),
            Token::Limit => write!(f, "LIMIT"),
            Token::Skip => write!(f, "SKIP"),
            Token::Set => write!(f, "SET"),
            Token::Delete => write!(f, "DELETE"),
            Token::And => write!(f, "AND"),
            Token::Or => write!(f, "OR"),
            Token::Not => write!(f, "NOT"),
            Token::Null => write!(f, "NULL"),
            Token::True => write!(f, "TRUE"),
            Token::False => write!(f, "FALSE"),
            Token::As => write!(f, "AS"),
            Token::Distinct => write!(f, "DISTINCT"),
            Token::Is => write!(f, "IS"),
            Token::Contains => write!(f, "CONTAINS"),
            Token::Starts => write!(f, "STARTS"),
            Token::Ends => write!(f, "ENDS"),
            Token::With => write!(f, "WITH"),
            Token::Integer(n) => write!(f, "{n}"),
            Token::Float(n) => write!(f, "{n}"),
            Token::StringLit(s) => write!(f, "'{s}'"),
            Token::Ident(s) => write!(f, "{s}"),
            Token::LParen => write!(f, "("),
            Token::RParen => write!(f, ")"),
            Token::LBracket => write!(f, "["),
            Token::RBracket => write!(f, "]"),
            Token::LBrace => write!(f, "{{"),
            Token::RBrace => write!(f, "}}"),
            Token::Colon => write!(f, ":"),
            Token::Comma => write!(f, ","),
            Token::Dot => write!(f, "."),
            Token::DotDot => write!(f, ".."),
            Token::Dash => write!(f, "-"),
            Token::Arrow => write!(f, "->"),
            Token::LArrow => write!(f, "<-"),
            Token::Eq => write!(f, "="),
            Token::Neq => write!(f, "<>"),
            Token::Lt => write!(f, "<"),
            Token::Gt => write!(f, ">"),
            Token::Lte => write!(f, "<="),
            Token::Gte => write!(f, ">="),
            Token::Plus => write!(f, "+"),
            Token::Star => write!(f, "*"),
            Token::Slash => write!(f, "/"),
            Token::Percent => write!(f, "%"),
        }
    }
}

// ---------------------------------------------------------------------------
// LexError
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct LexError {
    pub position: usize,
    pub message: String,
}

impl fmt::Display for LexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "lex error at byte {}: {}", self.position, self.message)
    }
}

impl std::error::Error for LexError {}

pub type Spanned<T> = (usize, T, usize);

// ---------------------------------------------------------------------------
// Lexer
// ---------------------------------------------------------------------------

pub struct Lexer<'input> {
    input: &'input str,
    pos: usize,
}

impl<'input> Lexer<'input> {
    pub fn new(input: &'input str) -> Self {
        Self { input, pos: 0 }
    }

    fn peek(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn peek_next(&self) -> Option<char> {
        let mut chars = self.input[self.pos..].chars();
        chars.next();
        chars.next()
    }

    fn advance(&mut self) -> Option<char> {
        let ch = self.peek()?;
        self.pos += ch.len_utf8();
        Some(ch)
    }

    fn skip_whitespace_and_comments(&mut self) {
        loop {
            while self.peek().is_some_and(|c| c.is_ascii_whitespace()) {
                self.advance();
            }
            // Line comments: //
            if self.remaining().starts_with("//") {
                while self.peek().is_some_and(|c| c != '\n') {
                    self.advance();
                }
                continue;
            }
            break;
        }
    }

    fn remaining(&self) -> &str {
        &self.input[self.pos..]
    }

    fn read_string(&mut self) -> Result<Token, LexError> {
        let quote = self.advance().unwrap();
        let start = self.pos;
        let mut value = String::new();

        loop {
            match self.peek() {
                None => {
                    return Err(LexError {
                        position: start,
                        message: "unterminated string".into(),
                    });
                }
                Some('\\') => {
                    self.advance();
                    match self.advance() {
                        Some('n') => value.push('\n'),
                        Some('t') => value.push('\t'),
                        Some('\\') => value.push('\\'),
                        Some('\'') => value.push('\''),
                        Some('"') => value.push('"'),
                        Some(c) => {
                            return Err(LexError {
                                position: self.pos - c.len_utf8(),
                                message: format!("unknown escape: \\{c}"),
                            });
                        }
                        None => {
                            return Err(LexError {
                                position: self.pos,
                                message: "unterminated escape".into(),
                            });
                        }
                    }
                }
                Some(c) if c == quote => {
                    self.advance();
                    return Ok(Token::StringLit(value));
                }
                Some(c) => {
                    self.advance();
                    value.push(c);
                }
            }
        }
    }

    fn read_number(&mut self) -> Token {
        let start = self.pos;
        while self.peek().is_some_and(|c| c.is_ascii_digit()) {
            self.advance();
        }
        // Check for decimal point followed by digit
        if self.peek() == Some('.') && self.peek_next().is_some_and(|c| c.is_ascii_digit()) {
            self.advance(); // consume .
            while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                self.advance();
            }
            let text = &self.input[start..self.pos];
            Token::Float(text.parse().unwrap())
        } else {
            let text = &self.input[start..self.pos];
            Token::Integer(text.parse().unwrap())
        }
    }

    fn read_ident_or_keyword(&mut self) -> Token {
        let start = self.pos;
        while self
            .peek()
            .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            self.advance();
        }
        let text = &self.input[start..self.pos];

        // Case-insensitive keyword matching
        match text.to_ascii_uppercase().as_str() {
            "MATCH" => Token::Match,
            "CREATE" => Token::Create,
            "WHERE" => Token::Where,
            "RETURN" => Token::Return,
            "ORDER" => Token::Order,
            "BY" => Token::By,
            "ASC" => Token::Asc,
            "DESC" => Token::Desc,
            "LIMIT" => Token::Limit,
            "SKIP" => Token::Skip,
            "SET" => Token::Set,
            "DELETE" => Token::Delete,
            "AND" => Token::And,
            "OR" => Token::Or,
            "NOT" => Token::Not,
            "NULL" => Token::Null,
            "TRUE" => Token::True,
            "FALSE" => Token::False,
            "AS" => Token::As,
            "DISTINCT" => Token::Distinct,
            "IS" => Token::Is,
            "CONTAINS" => Token::Contains,
            "STARTS" => Token::Starts,
            "ENDS" => Token::Ends,
            "WITH" => Token::With,
            _ => Token::Ident(text.to_owned()),
        }
    }

    fn next_token(&mut self) -> Result<Option<Spanned<Token>>, LexError> {
        self.skip_whitespace_and_comments();

        let start = self.pos;
        let ch = match self.peek() {
            Some(c) => c,
            None => return Ok(None),
        };

        let tok = match ch {
            '(' => {
                self.advance();
                Token::LParen
            }
            ')' => {
                self.advance();
                Token::RParen
            }
            '[' => {
                self.advance();
                Token::LBracket
            }
            ']' => {
                self.advance();
                Token::RBracket
            }
            '{' => {
                self.advance();
                Token::LBrace
            }
            '}' => {
                self.advance();
                Token::RBrace
            }
            ':' => {
                self.advance();
                Token::Colon
            }
            ',' => {
                self.advance();
                Token::Comma
            }
            '.' => {
                self.advance();
                if self.peek() == Some('.') {
                    self.advance();
                    Token::DotDot
                } else {
                    Token::Dot
                }
            }
            '+' => {
                self.advance();
                Token::Plus
            }
            '*' => {
                self.advance();
                Token::Star
            }
            '/' => {
                self.advance();
                Token::Slash
            }
            '%' => {
                self.advance();
                Token::Percent
            }
            '-' => {
                self.advance();
                if self.peek() == Some('>') {
                    self.advance();
                    Token::Arrow
                } else {
                    Token::Dash
                }
            }
            '<' => {
                self.advance();
                match self.peek() {
                    Some('-') => {
                        self.advance();
                        Token::LArrow
                    }
                    Some('=') => {
                        self.advance();
                        Token::Lte
                    }
                    Some('>') => {
                        self.advance();
                        Token::Neq
                    }
                    _ => Token::Lt,
                }
            }
            '>' => {
                self.advance();
                if self.peek() == Some('=') {
                    self.advance();
                    Token::Gte
                } else {
                    Token::Gt
                }
            }
            '=' => {
                self.advance();
                Token::Eq
            }
            '\'' | '"' => self.read_string()?,
            c if c.is_ascii_digit() => self.read_number(),
            c if c.is_ascii_alphabetic() || c == '_' => self.read_ident_or_keyword(),
            _ => {
                return Err(LexError {
                    position: start,
                    message: format!("unexpected character: {ch:?}"),
                });
            }
        };

        Ok(Some((start, tok, self.pos)))
    }
}

impl<'input> Iterator for Lexer<'input> {
    type Item = Result<Spanned<Token>, LexError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_token() {
            Ok(Some(tok)) => Some(Ok(tok)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex(input: &str) -> Vec<Token> {
        Lexer::new(input)
            .map(|r| r.unwrap())
            .map(|(_, tok, _)| tok)
            .collect()
    }

    #[test]
    fn keywords_case_insensitive() {
        assert_eq!(
            lex("match MATCH Match"),
            vec![Token::Match, Token::Match, Token::Match]
        );
    }

    #[test]
    fn identifiers() {
        assert_eq!(
            lex("foo bar_1 _x"),
            vec![
                Token::Ident("foo".into()),
                Token::Ident("bar_1".into()),
                Token::Ident("_x".into()),
            ]
        );
    }

    #[test]
    fn numbers() {
        assert_eq!(lex("42 3.14"), vec![Token::Integer(42), Token::Float(3.14)]);
    }

    #[test]
    fn strings() {
        assert_eq!(
            lex("'hello' \"world\""),
            vec![
                Token::StringLit("hello".into()),
                Token::StringLit("world".into())
            ]
        );
    }

    #[test]
    fn string_escapes() {
        assert_eq!(lex(r"'it\'s'"), vec![Token::StringLit("it's".into())]);
        assert_eq!(lex(r"'a\nb'"), vec![Token::StringLit("a\nb".into())]);
    }

    #[test]
    fn arrows() {
        assert_eq!(lex("->"), vec![Token::Arrow]);
        assert_eq!(lex("<-"), vec![Token::LArrow]);
        assert_eq!(lex("-"), vec![Token::Dash]);
    }

    #[test]
    fn comparison_operators() {
        assert_eq!(
            lex("= <> < > <= >="),
            vec![
                Token::Eq,
                Token::Neq,
                Token::Lt,
                Token::Gt,
                Token::Lte,
                Token::Gte,
            ]
        );
    }

    #[test]
    fn symbols() {
        assert_eq!(
            lex("()[]{}:,. + * / %"),
            vec![
                Token::LParen,
                Token::RParen,
                Token::LBracket,
                Token::RBracket,
                Token::LBrace,
                Token::RBrace,
                Token::Colon,
                Token::Comma,
                Token::Dot,
                Token::Plus,
                Token::Star,
                Token::Slash,
                Token::Percent,
            ]
        );
    }

    #[test]
    fn simple_query() {
        let tokens = lex("MATCH (p:Person) RETURN p.name");
        assert_eq!(
            tokens,
            vec![
                Token::Match,
                Token::LParen,
                Token::Ident("p".into()),
                Token::Colon,
                Token::Ident("Person".into()),
                Token::RParen,
                Token::Return,
                Token::Ident("p".into()),
                Token::Dot,
                Token::Ident("name".into()),
            ]
        );
    }

    #[test]
    fn skips_comments() {
        assert_eq!(
            lex("42 // comment\n7"),
            vec![Token::Integer(42), Token::Integer(7)]
        );
    }

    #[test]
    fn span_positions() {
        let toks: Vec<_> = Lexer::new("MATCH (p)").map(|r| r.unwrap()).collect();
        assert_eq!(toks[0], (0, Token::Match, 5));
        assert_eq!(toks[1], (6, Token::LParen, 7));
        assert_eq!(toks[2], (7, Token::Ident("p".into()), 8));
        assert_eq!(toks[3], (8, Token::RParen, 9));
    }

    #[test]
    fn unterminated_string_error() {
        let result: Vec<_> = Lexer::new("'oops").collect();
        assert!(result[0].is_err());
    }

    #[test]
    fn integer_then_dot_ident() {
        // "42.foo" should be Integer(42), Dot, Ident("foo")
        // because the character after . is not a digit
        assert_eq!(
            lex("42.foo"),
            vec![Token::Integer(42), Token::Dot, Token::Ident("foo".into()),]
        );
    }
}
