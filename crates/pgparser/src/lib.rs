//! pgparser: hand-written lexer + recursive-descent/Pratt parser producing the
//! crabgresql AST for the SP2 SQL slice.

#![doc(html_root_url = "https://docs.rs/crabka-pgparser/0.3.9")]

pub mod ast;
pub mod command;
pub mod error;
pub mod lexer;
pub mod parser;
pub mod plpgsql;
pub mod token;

pub use error::ParseError;
pub use parser::{
    parse, parse_with_command_identities, parse_with_source, parse_with_type_schemas,
};
pub use plpgsql::parse_plpgsql;
