//! pgparser: hand-written lexer + recursive-descent/Pratt parser producing the
//! crabgresql AST for the SP2 SQL slice.

#![doc(html_root_url = "https://docs.rs/crabka-pgparser/0.3.9")]

pub mod ast;
pub mod error;
pub mod lexer;
pub mod parser;
pub mod token;

pub use error::ParseError;
pub use parser::{parse, parse_with_source};
