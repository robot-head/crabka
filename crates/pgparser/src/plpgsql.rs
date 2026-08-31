//! Native PL/pgSQL block parser.
//!
//! Procedural words stay soft. The SQL lexer intentionally emits most of them
//! as identifiers, and this parser matches their source spelling only where the
//! PL grammar expects them. This parser hands embedded expressions and SQL
//! statements back to the ordinary SQL parser.

use std::collections::HashSet;

use crate::{
    ast::{
        Expr, PlPgSqlBlock, PlPgSqlCursorArgument, PlPgSqlDeclaration, PlPgSqlExceptionHandler,
        PlPgSqlInto, PlPgSqlLoop, PlPgSqlRaise, PlPgSqlRaiseLevel, PlPgSqlStatement, PlPgSqlTarget,
        PlPgSqlVariableConflict, RoutineType, Statement,
    },
    error::ParseError,
    lexer::lex,
    parser::{parse, parse_expression, parse_routine_type},
    token::Token,
};

#[derive(Clone)]
struct ControlScope {
    label: Option<String>,
    is_loop: bool,
}

struct PlParser<'a> {
    source: &'a str,
    tokens: Vec<(Token, usize)>,
    pos: usize,
    scopes: Vec<ControlScope>,
}

/// Parse one complete PL/pgSQL routine body.
///
/// # Errors
///
/// Returns a syntax error for malformed procedural syntax, malformed embedded
/// SQL, invalid labels, or `EXIT`/`CONTINUE` outside a legal target scope.
pub fn parse_plpgsql(source: &str) -> Result<PlPgSqlBlock, ParseError> {
    let mut parser = PlParser {
        source,
        tokens: lex(source)?,
        pos: 0,
        scopes: Vec::new(),
    };
    let (variable_conflict, print_strict_params) = parser.parse_directives()?;
    let label = parser.eat_label()?;
    let mut block = parser.parse_block(label, true)?;
    block.variable_conflict = variable_conflict;
    block.print_strict_params = print_strict_params;
    if !parser.at_eof() {
        return Err(parser.error("trailing tokens after PL/pgSQL block"));
    }
    Ok(block)
}

impl PlParser<'_> {
    fn token(&self) -> &Token {
        &self.tokens[self.pos].0
    }

    fn offset(&self) -> usize {
        self.tokens[self.pos].1
    }

    fn line(&self) -> usize {
        self.source[..self.offset()]
            .bytes()
            .filter(|byte| *byte == b'\n')
            .count()
            + 1
    }

    fn at_eof(&self) -> bool {
        matches!(self.token(), Token::Eof)
    }

    fn bump(&mut self) -> Token {
        let token = self.token().clone();
        if self.pos + 1 < self.tokens.len() {
            self.pos += 1;
        }
        token
    }

    fn error(&self, message: impl Into<String>) -> ParseError {
        ParseError::new(message, self.offset())
    }

    fn word_at(&self, pos: usize) -> Option<String> {
        let (token, offset) = self.tokens.get(pos)?;
        match token {
            Token::Ident(word) => {
                (self.source.as_bytes().get(*offset) != Some(&b'"')).then(|| word.clone())
            }
            Token::Keyword(_) => {
                let rest = &self.source[*offset..];
                let len = rest
                    .find(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
                    .unwrap_or(rest.len());
                Some(rest[..len].to_ascii_lowercase())
            }
            _ => None,
        }
    }

    fn at_word(&self, word: &str) -> bool {
        self.word_at(self.pos)
            .is_some_and(|actual| actual.eq_ignore_ascii_case(word))
    }

    fn eat_word(&mut self, word: &str) -> bool {
        if self.at_word(word) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect_word(&mut self, word: &str) -> Result<(), ParseError> {
        if self.eat_word(word) {
            Ok(())
        } else {
            Err(self.error(format!("expected {word}")))
        }
    }

    fn expect_token(&mut self, token: &Token) -> Result<(), ParseError> {
        if self.token() == token {
            self.bump();
            Ok(())
        } else {
            Err(self.error(format!("expected {token:?}, found {:?}", self.token())))
        }
    }

    fn expect_name(&mut self) -> Result<String, ParseError> {
        let offset = self.offset();
        if let Some(name) = self.word_at(self.pos) {
            self.bump();
            return Ok(name);
        }
        match self.bump() {
            Token::Ident(name) => Ok(name),
            other => Err(ParseError::new(
                format!("expected identifier, found {other:?}"),
                offset,
            )),
        }
    }

    fn parse_directives(&mut self) -> Result<(PlPgSqlVariableConflict, Option<bool>), ParseError> {
        let mut variable_conflict = PlPgSqlVariableConflict::Error;
        let mut print_strict_params = None;
        while matches!(self.token(), Token::Hash) {
            self.bump();
            let line = self.source[..self.offset()]
                .bytes()
                .filter(|b| *b == b'\n')
                .count();
            let directive = self.expect_name()?;
            let setting = self.expect_name()?;
            match directive.as_str() {
                "variable_conflict" => {
                    variable_conflict = match setting.as_str() {
                        "error" => PlPgSqlVariableConflict::Error,
                        "use_variable" => PlPgSqlVariableConflict::UseVariable,
                        "use_column" => PlPgSqlVariableConflict::UseColumn,
                        _ => {
                            return Err(self.error(format!(
                                "unrecognized #variable_conflict setting \"{setting}\""
                            )));
                        }
                    };
                }
                "print_strict_params" if matches!(setting.as_str(), "on" | "off") => {
                    print_strict_params = Some(setting == "on");
                }
                "print_strict_params" => {
                    return Err(self.error(format!(
                        "unrecognized #print_strict_params setting \"{setting}\""
                    )));
                }
                _ => {
                    return Err(
                        self.error(format!("unrecognized PL/pgSQL directive \"{directive}\""))
                    );
                }
            }
            while !self.at_eof()
                && self.source[..self.offset()]
                    .bytes()
                    .filter(|b| *b == b'\n')
                    .count()
                    == line
            {
                self.bump();
            }
        }
        Ok((variable_conflict, print_strict_params))
    }

    fn eat_label(&mut self) -> Result<Option<String>, ParseError> {
        if !matches!(self.token(), Token::Shl) {
            return Ok(None);
        }
        self.bump();
        let label = self.expect_name()?;
        self.expect_token(&Token::Shr)?;
        Ok(Some(label))
    }

    fn parse_block(
        &mut self,
        label: Option<String>,
        top_level: bool,
    ) -> Result<PlPgSqlBlock, ParseError> {
        let start = self.offset();
        let declarations = if self.eat_word("declare") {
            self.parse_declarations()?
        } else {
            Vec::new()
        };
        self.expect_word("begin")?;
        self.scopes.push(ControlScope {
            label: label.clone(),
            is_loop: false,
        });
        let statements = self.parse_statement_list(&["exception", "end"])?;
        let exceptions = if self.eat_word("exception") {
            self.parse_exception_handlers()?
        } else {
            Vec::new()
        };
        self.expect_word("end")?;
        let end_label = if self.word_at(self.pos).is_some() {
            Some(self.expect_name()?)
        } else {
            None
        };
        Self::validate_end_label(label.as_deref(), end_label.as_deref(), self.offset())?;
        if matches!(self.token(), Token::Semicolon) {
            self.bump();
        } else if !top_level {
            self.scopes.pop();
            return Err(self.error("expected `;` after END"));
        }
        self.scopes.pop();
        Ok(PlPgSqlBlock {
            variable_conflict: PlPgSqlVariableConflict::Error,
            print_strict_params: None,
            label,
            declarations,
            statements,
            exceptions,
            end_label,
            span: start..self.offset(),
        })
    }

    fn validate_end_label(
        label: Option<&str>,
        end_label: Option<&str>,
        position: usize,
    ) -> Result<(), ParseError> {
        match (label, end_label) {
            (None, Some(end)) => Err(ParseError::new(
                format!("end label \"{end}\" specified for unlabeled block"),
                position,
            )),
            (Some(start), Some(end)) if start != end => Err(ParseError::new(
                format!("end label \"{end}\" differs from block's label \"{start}\""),
                position,
            )),
            _ => Ok(()),
        }
    }

    fn parse_declarations(&mut self) -> Result<Vec<PlPgSqlDeclaration>, ParseError> {
        let mut declarations = Vec::new();
        let mut names = HashSet::new();
        while !self.at_word("begin") {
            if self.at_eof() {
                return Err(self.error("unterminated DECLARE section"));
            }
            let position = self.offset();
            let name = self.expect_name()?;
            if !names.insert(name.clone()) {
                return Err(self.error(format!("duplicate declaration \"{name}\"")));
            }
            if self.eat_word("alias") {
                self.expect_word("for")?;
                let target = match self.bump() {
                    Token::Param(number) => format!("${number}"),
                    Token::Ident(target) => target,
                    other => return Err(self.error(format!("invalid alias target {other:?}"))),
                };
                self.expect_token(&Token::Semicolon)?;
                declarations.push(PlPgSqlDeclaration::Alias {
                    name,
                    position,
                    target,
                });
                continue;
            }
            if self.cursor_declaration_starts() {
                declarations.push(self.parse_cursor_declaration(name, position)?);
                continue;
            }
            let constant = self.eat_word("constant");
            let type_start = self.pos;
            let type_end = self.find_top(self.pos, |parser, pos| {
                matches!(
                    parser.tokens[pos].0,
                    Token::Semicolon | Token::Eq | Token::Colon
                ) || parser
                    .word_at(pos)
                    .is_some_and(|word| word == "not" || word == "default")
            });
            if type_end == type_start {
                return Err(self.error("expected a declaration type"));
            }
            let ty = if type_end >= type_start + 3
                && self.tokens[type_end - 2].0 == Token::Percent
                && self.word_at(type_end - 1).as_deref() == Some("type")
            {
                let reference = self.slice_tokens(type_start, type_end - 2).trim();
                if reference.is_empty() {
                    return Err(self.error("expected variable before %TYPE"));
                }
                RoutineType::named(format!("{reference}%type"))
            } else if type_end >= type_start + 3
                && self.tokens[type_end - 2].0 == Token::Percent
                && self.word_at(type_end - 1).as_deref() == Some("rowtype")
            {
                let reference = self.slice_tokens(type_start, type_end - 2).trim();
                if reference.is_empty() {
                    return Err(self.error("expected relation before %ROWTYPE"));
                }
                RoutineType::named(format!("{reference}%rowtype"))
            } else {
                parse_routine_type(self.slice_tokens(type_start, type_end).trim())?
            };
            self.pos = type_end;
            let not_null = if self.eat_word("not") {
                self.expect_word("null")?;
                true
            } else {
                false
            };
            let default = if self.eat_word("default") || self.eat_assignment_operator() {
                let end = self.find_token(self.pos, &Token::Semicolon)?;
                let expr = self.parse_expr_range(self.pos, end)?;
                self.pos = end;
                Some(expr)
            } else {
                None
            };
            self.expect_token(&Token::Semicolon)?;
            declarations.push(PlPgSqlDeclaration::Variable {
                name,
                position,
                ty,
                constant,
                not_null,
                default,
            });
        }
        Ok(declarations)
    }

    fn cursor_declaration_starts(&self) -> bool {
        self.at_word("cursor")
            || self.at_word("scroll")
            || (self.at_word("no")
                && self
                    .word_at(self.pos + 1)
                    .is_some_and(|word| word == "scroll"))
    }

    fn parse_cursor_declaration(
        &mut self,
        name: String,
        position: usize,
    ) -> Result<PlPgSqlDeclaration, ParseError> {
        let scroll = if self.eat_word("no") {
            self.expect_word("scroll")?;
            Some(false)
        } else if self.eat_word("scroll") {
            Some(true)
        } else {
            None
        };
        self.expect_word("cursor")?;
        let mut arguments = Vec::new();
        if matches!(self.token(), Token::LParen) {
            self.bump();
            while !matches!(self.token(), Token::RParen) {
                let position = self.offset();
                let arg = self.expect_name()?;
                let end = self.find_top(self.pos, |parser, pos| {
                    matches!(parser.tokens[pos].0, Token::Comma | Token::RParen)
                });
                let ty = parse_routine_type(self.slice_tokens(self.pos, end).trim())?;
                self.pos = end;
                arguments.push((arg, ty, position));
                if matches!(self.token(), Token::Comma) {
                    self.bump();
                } else {
                    break;
                }
            }
            self.expect_token(&Token::RParen)?;
        }
        self.expect_word("for")?;
        let end = self.find_token(self.pos, &Token::Semicolon)?;
        let query = self.parse_sql_range(self.pos, end)?;
        self.pos = end;
        self.bump();
        Ok(PlPgSqlDeclaration::Cursor {
            name,
            position,
            scroll,
            arguments,
            query: Box::new(query),
        })
    }

    fn parse_exception_handlers(&mut self) -> Result<Vec<PlPgSqlExceptionHandler>, ParseError> {
        let mut handlers = Vec::new();
        while self.eat_word("when") {
            let mut conditions = vec![self.parse_condition()?];
            while self.eat_word("or") {
                conditions.push(self.parse_condition()?);
            }
            self.expect_word("then")?;
            let statements = self.parse_statement_list(&["when", "end"])?;
            handlers.push(PlPgSqlExceptionHandler {
                conditions,
                statements,
            });
        }
        if handlers.is_empty() {
            return Err(self.error("EXCEPTION requires at least one WHEN handler"));
        }
        Ok(handlers)
    }

    fn parse_condition(&mut self) -> Result<String, ParseError> {
        if self.eat_word("sqlstate") {
            return match self.bump() {
                Token::StringLit(code) => Ok(code),
                other => Err(self.error(format!("expected SQLSTATE string, found {other:?}"))),
            };
        }
        self.expect_name()
    }

    fn parse_statement_list(
        &mut self,
        stops: &[&str],
    ) -> Result<Vec<PlPgSqlStatement>, ParseError> {
        let mut statements = Vec::new();
        while !self.at_eof() && !stops.iter().any(|stop| self.at_word(stop)) {
            statements.push(self.parse_statement()?);
        }
        Ok(statements)
    }

    fn parse_statement(&mut self) -> Result<PlPgSqlStatement, ParseError> {
        let label = self.eat_label()?;
        if label.is_some()
            && !(self.at_word("begin")
                || self.at_word("declare")
                || self.at_word("loop")
                || self.at_word("while")
                || self.at_word("for")
                || self.at_word("foreach"))
        {
            return Err(self.error("a label must precede a block or loop"));
        }
        if self.at_word("begin") || self.at_word("declare") {
            return Ok(PlPgSqlStatement::Block(Box::new(
                self.parse_block(label, false)?,
            )));
        }
        if self.at_word("if") {
            return self.parse_if();
        }
        if self.at_word("case") {
            return self.parse_case();
        }
        if self.at_word("loop") {
            return self.parse_loop(label, PlPgSqlLoop::Unconditional, self.line());
        }
        if self.at_word("while") {
            let line = self.line();
            self.bump();
            let condition = self.parse_expr_to_word(&["loop"])?;
            return self.parse_loop(label, PlPgSqlLoop::While(condition), line);
        }
        if self.at_word("for") {
            return self.parse_for(label);
        }
        if self.at_word("foreach") {
            return self.parse_foreach(label);
        }
        if self.at_word("exit") || self.at_word("continue") {
            return self.parse_exit();
        }
        if self.at_word("return") {
            return self.parse_return();
        }
        if self.at_word("raise") {
            return self.parse_raise();
        }
        if self.at_word("execute") {
            return self.parse_execute();
        }
        if self.at_word("perform") {
            return self.parse_perform();
        }
        if self.at_word("open") {
            return self.parse_open();
        }
        if self.at_word("fetch") || self.at_word("move") {
            return self.parse_fetch();
        }
        if self.at_word("close") {
            return self.parse_close();
        }
        if self.at_word("get") {
            return self.parse_get_diagnostics();
        }
        if self.at_word("assert") {
            return self.parse_assert();
        }
        if self.at_word("commit") || self.at_word("rollback") {
            return self.parse_transaction();
        }
        if self.eat_word("null") {
            self.expect_token(&Token::Semicolon)?;
            return Ok(PlPgSqlStatement::Null);
        }
        if label.is_some() {
            return Err(self.error("unexpected label"));
        }
        if self.assignment_starts() {
            return self.parse_assignment();
        }
        self.parse_static_sql()
    }

    fn parse_if(&mut self) -> Result<PlPgSqlStatement, ParseError> {
        self.expect_word("if")?;
        let mut branches = Vec::new();
        let condition = self.parse_expr_to_word(&["then"])?;
        self.expect_word("then")?;
        let body = self.parse_statement_list(&["elsif", "else", "end"])?;
        branches.push((condition, body));
        while self.eat_word("elsif") {
            let condition = self.parse_expr_to_word(&["then"])?;
            self.expect_word("then")?;
            let body = self.parse_statement_list(&["elsif", "else", "end"])?;
            branches.push((condition, body));
        }
        let else_body = if self.eat_word("else") {
            self.parse_statement_list(&["end"])?
        } else {
            Vec::new()
        };
        self.expect_word("end")?;
        self.expect_word("if")?;
        self.expect_token(&Token::Semicolon)?;
        Ok(PlPgSqlStatement::If {
            branches,
            else_body,
        })
    }

    fn parse_case(&mut self) -> Result<PlPgSqlStatement, ParseError> {
        self.expect_word("case")?;
        let operand = if self.at_word("when") {
            None
        } else {
            Some(self.parse_expr_to_word(&["when"])?)
        };
        let mut arms = Vec::new();
        while self.eat_word("when") {
            let then = self.find_top_word(self.pos, &["then"])?;
            let mut expressions = Vec::new();
            let mut start = self.pos;
            for comma in self.top_level_commas(self.pos, then) {
                expressions.push(self.parse_expr_range(start, comma)?);
                start = comma + 1;
            }
            expressions.push(self.parse_expr_range(start, then)?);
            self.pos = then;
            self.expect_word("then")?;
            let body = self.parse_statement_list(&["when", "else", "end"])?;
            arms.push((expressions, body));
        }
        if arms.is_empty() {
            return Err(self.error("CASE requires at least one WHEN arm"));
        }
        let else_body = if self.eat_word("else") {
            Some(self.parse_statement_list(&["end"])?)
        } else {
            None
        };
        self.expect_word("end")?;
        self.expect_word("case")?;
        self.expect_token(&Token::Semicolon)?;
        Ok(PlPgSqlStatement::Case {
            operand,
            arms,
            else_body,
        })
    }

    fn parse_loop(
        &mut self,
        label: Option<String>,
        kind: PlPgSqlLoop,
        line: usize,
    ) -> Result<PlPgSqlStatement, ParseError> {
        self.expect_word("loop")?;
        self.scopes.push(ControlScope {
            label: label.clone(),
            is_loop: true,
        });
        let body = self.parse_statement_list(&["end"])?;
        self.expect_word("end")?;
        self.expect_word("loop")?;
        let end_label = if self.word_at(self.pos).is_some() {
            Some(self.expect_name()?)
        } else {
            None
        };
        Self::validate_end_label(label.as_deref(), end_label.as_deref(), self.offset())?;
        self.expect_token(&Token::Semicolon)?;
        self.scopes.pop();
        Ok(PlPgSqlStatement::Loop {
            label,
            kind: Box::new(kind),
            body,
            end_label,
            line,
        })
    }

    fn parse_for(&mut self, label: Option<String>) -> Result<PlPgSqlStatement, ParseError> {
        let line = self.line();
        self.expect_word("for")?;
        let targets = self.parse_targets_until_word("in")?;
        self.expect_word("in")?;
        if self.eat_word("execute") {
            let query = self.parse_expr_to_words(&["using", "loop"])?;
            let using = if self.eat_word("using") {
                self.parse_expr_list_to_word("loop")?
            } else {
                Vec::new()
            };
            return self.parse_loop(
                label,
                PlPgSqlLoop::Dynamic {
                    targets,
                    query,
                    using,
                },
                line,
            );
        }
        let reverse = self.eat_word("reverse");
        let loop_at = self.find_top_word(self.pos, &["loop"])?;
        if let Some((range, compact)) = self.find_top_dot_dot(self.pos, loop_at) {
            let variable = (targets.len() == 1)
                .then(|| &targets[0])
                .and_then(|target| {
                    (target.path.len() == 1 && target.subscripts.is_empty())
                        .then(|| target.path[0].clone())
                })
                .ok_or_else(|| self.error("integer FOR requires one scalar variable"))?;
            let lower = self.parse_expr_range(self.pos, range)?;
            self.pos = range + if compact { 1 } else { 2 };
            let upper_end = self.find_top_word(self.pos, &["by", "loop"])?;
            let upper_source = self.slice_tokens(self.pos, upper_end).trim();
            let upper = parse_expression(if compact {
                upper_source.strip_prefix('.').unwrap_or(upper_source)
            } else {
                upper_source
            })?;
            self.pos = upper_end;
            let step = if self.eat_word("by") {
                Some(self.parse_expr_to_word(&["loop"])?)
            } else {
                None
            };
            return self.parse_loop(
                label,
                PlPgSqlLoop::Integer {
                    variable,
                    reverse,
                    lower,
                    upper,
                    step,
                },
                line,
            );
        }
        if reverse {
            return Err(self.error("REVERSE is only valid for an integer FOR loop"));
        }
        if let Some(cursor) = self.word_at(self.pos) {
            let arguments = if loop_at == self.pos + 1 {
                Vec::new()
            } else if matches!(self.tokens.get(self.pos + 1), Some((Token::LParen, _))) {
                let end = self.find_token(self.pos + 1, &Token::RParen)?;
                if end + 1 != loop_at {
                    Vec::new()
                } else {
                    self.parse_cursor_argument_list_range(self.pos + 2, end)?
                }
            } else {
                Vec::new()
            };
            if loop_at == self.pos + 1 || !arguments.is_empty() {
                self.pos = loop_at;
                return self.parse_loop(
                    label,
                    PlPgSqlLoop::Cursor {
                        targets,
                        cursor,
                        arguments,
                    },
                    line,
                );
            }
        }
        let source = self.slice_tokens(self.pos, loop_at).trim().to_owned();
        let query = self.parse_sql_range(self.pos, loop_at)?;
        self.pos = loop_at;
        self.parse_loop(
            label,
            PlPgSqlLoop::Query {
                targets,
                query: Box::new(query),
                source,
            },
            line,
        )
    }

    fn parse_foreach(&mut self, label: Option<String>) -> Result<PlPgSqlStatement, ParseError> {
        let line = self.line();
        self.expect_word("foreach")?;
        let targets = self.parse_target_list()?;
        let slice = if self.eat_word("slice") {
            let position = self.offset();
            match self.bump() {
                Token::IntLit(value) => Some(
                    value
                        .parse()
                        .map_err(|_| ParseError::new("invalid FOREACH SLICE value", position))?,
                ),
                other => return Err(self.error(format!("expected SLICE count, found {other:?}"))),
            }
        } else {
            None
        };
        self.expect_word("in")?;
        self.expect_word("array")?;
        let array = self.parse_expr_to_word(&["loop"])?;
        self.parse_loop(
            label,
            PlPgSqlLoop::Foreach {
                targets,
                slice,
                array,
            },
            line,
        )
    }

    fn parse_exit(&mut self) -> Result<PlPgSqlStatement, ParseError> {
        let continuing = self.eat_word("continue");
        if !continuing {
            self.expect_word("exit")?;
        }
        let label = if !self.at_word("when") && self.word_at(self.pos).is_some() {
            Some(self.expect_name()?)
        } else {
            None
        };
        self.validate_control_target(continuing, label.as_deref())?;
        let when = if self.eat_word("when") {
            let end = self.find_token(self.pos, &Token::Semicolon)?;
            let expr = self.parse_expr_range(self.pos, end)?;
            self.pos = end;
            Some(expr)
        } else {
            None
        };
        self.expect_token(&Token::Semicolon)?;
        Ok(PlPgSqlStatement::Exit {
            continuing,
            label,
            when,
        })
    }

    fn validate_control_target(
        &self,
        continuing: bool,
        label: Option<&str>,
    ) -> Result<(), ParseError> {
        if let Some(label) = label {
            let Some(scope) = self
                .scopes
                .iter()
                .rev()
                .find(|scope| scope.label.as_deref() == Some(label))
            else {
                return Err(self.error(format!(
                    "there is no label \"{label}\" attached to an enclosing block or loop"
                )));
            };
            if continuing && !scope.is_loop {
                return Err(self.error(format!(
                    "block label \"{label}\" cannot be used in CONTINUE"
                )));
            }
            return Ok(());
        }
        if self.scopes.iter().any(|scope| scope.is_loop) {
            Ok(())
        } else if continuing {
            Err(self.error("CONTINUE cannot be used outside a loop"))
        } else {
            Err(self.error("EXIT cannot be used outside a loop, unless it has a label"))
        }
    }

    fn parse_return(&mut self) -> Result<PlPgSqlStatement, ParseError> {
        let start = self.pos;
        let line = self.source[..self.tokens[start].1]
            .bytes()
            .filter(|byte| *byte == b'\n')
            .count()
            + 1;
        self.expect_word("return")?;
        if self.eat_word("next") {
            if matches!(self.token(), Token::Semicolon) {
                self.bump();
                return Ok(PlPgSqlStatement::ReturnNext(None));
            }
            let end = self.find_token(self.pos, &Token::Semicolon)?;
            let value = self.parse_expr_range(self.pos, end)?;
            self.pos = end + 1;
            return Ok(PlPgSqlStatement::ReturnNext(Some(value)));
        }
        if self.eat_word("query") {
            if self.eat_word("execute") {
                let query = self.parse_expr_to_words(&["using"])?;
                let using = if self.eat_word("using") {
                    self.parse_expr_list_to_token(&Token::Semicolon)?
                } else {
                    Vec::new()
                };
                self.expect_token(&Token::Semicolon)?;
                return Ok(PlPgSqlStatement::ReturnQueryExecute { query, using, line });
            }
            let end = self.find_token(self.pos, &Token::Semicolon)?;
            let source = self.slice_tokens(self.pos, end).trim().to_owned();
            let query = self.parse_sql_range(self.pos, end)?;
            self.pos = end + 1;
            return Ok(PlPgSqlStatement::ReturnQuery {
                query: Box::new(query),
                source,
                line,
            });
        }
        if matches!(self.token(), Token::Semicolon) {
            self.bump();
            return Ok(PlPgSqlStatement::Return {
                value: None,
                source: None,
                line,
            });
        }
        let end = self.find_token(self.pos, &Token::Semicolon)?;
        let source = self.slice_tokens(self.pos, end).trim().to_owned();
        let value = self.parse_expr_range(self.pos, end)?;
        self.pos = end + 1;
        Ok(PlPgSqlStatement::Return {
            value: Some(value),
            source: Some(source),
            line,
        })
    }

    fn parse_raise(&mut self) -> Result<PlPgSqlStatement, ParseError> {
        let line = self.source[..self.offset()]
            .bytes()
            .filter(|byte| *byte == b'\n')
            .count()
            + 1;
        self.expect_word("raise")?;
        if matches!(self.token(), Token::Semicolon) {
            self.bump();
            return Ok(PlPgSqlStatement::Raise(PlPgSqlRaise {
                line,
                level: PlPgSqlRaiseLevel::Exception,
                condition: None,
                message: None,
                parameters: Vec::new(),
                parameter_sources: Vec::new(),
                options: Vec::new(),
            }));
        }
        let level = [
            ("debug", PlPgSqlRaiseLevel::Debug),
            ("log", PlPgSqlRaiseLevel::Log),
            ("info", PlPgSqlRaiseLevel::Info),
            ("notice", PlPgSqlRaiseLevel::Notice),
            ("warning", PlPgSqlRaiseLevel::Warning),
            ("exception", PlPgSqlRaiseLevel::Exception),
        ]
        .into_iter()
        .find_map(|(word, level)| self.eat_word(word).then_some(level))
        .unwrap_or(PlPgSqlRaiseLevel::Exception);
        let mut condition = None;
        let mut message = None;
        if self.eat_word("sqlstate") {
            condition = match self.bump() {
                Token::StringLit(code) => Some(code),
                other => {
                    return Err(self.error(format!("expected SQLSTATE string, found {other:?}")));
                }
            };
        } else if let Token::StringLit(text) = self.token().clone() {
            self.bump();
            message = Some(text);
        } else if self.word_at(self.pos).is_some_and(|word| word != "using") {
            condition = Some(self.expect_name()?);
        }
        let mut parameters = Vec::new();
        let mut parameter_sources = Vec::new();
        while matches!(self.token(), Token::Comma) {
            self.bump();
            let end = self.find_top(self.pos, |parser, pos| {
                matches!(parser.tokens[pos].0, Token::Comma | Token::Semicolon)
                    || parser.word_at(pos).is_some_and(|word| word == "using")
            });
            parameter_sources.push(self.slice_tokens(self.pos, end).trim().to_owned());
            parameters.push(self.parse_expr_range(self.pos, end)?);
            self.pos = end;
        }
        let mut options = Vec::new();
        if self.eat_word("using") {
            loop {
                let name = self.expect_name()?;
                if !self.eat_assignment_operator() {
                    return Err(self.error("expected `=` after RAISE option"));
                }
                let end = self.find_top(self.pos, |parser, pos| {
                    matches!(parser.tokens[pos].0, Token::Comma | Token::Semicolon)
                });
                options.push((name, self.parse_expr_range(self.pos, end)?));
                self.pos = end;
                if matches!(self.token(), Token::Comma) {
                    self.bump();
                } else {
                    break;
                }
            }
        }
        self.expect_token(&Token::Semicolon)?;
        Ok(PlPgSqlStatement::Raise(PlPgSqlRaise {
            line,
            level,
            condition,
            message,
            parameters,
            parameter_sources,
            options,
        }))
    }

    fn parse_execute(&mut self) -> Result<PlPgSqlStatement, ParseError> {
        let line = self.line();
        self.expect_word("execute")?;
        let query = self.parse_expr_to_words(&["into", "using"])?;
        let mut into = None;
        let mut using = Vec::new();
        loop {
            if self.eat_word("into") {
                into = Some(self.parse_into()?);
            } else if self.eat_word("using") {
                let end = self.find_top_word_or_semicolon(self.pos, &["into"])?;
                using = self.parse_expr_list_range(self.pos, end)?;
                self.pos = end;
            } else {
                break;
            }
        }
        self.expect_token(&Token::Semicolon)?;
        Ok(PlPgSqlStatement::Execute {
            query,
            into,
            using,
            line,
        })
    }

    fn parse_perform(&mut self) -> Result<PlPgSqlStatement, ParseError> {
        let start = self.pos;
        self.expect_word("perform")?;
        let end = self.find_token(self.pos, &Token::Semicolon)?;
        let source = format!("SELECT {}", self.slice_tokens(self.pos, end));
        let query = parse_one(&source)?;
        let line = self.source[..self.tokens[start].1]
            .bytes()
            .filter(|byte| *byte == b'\n')
            .count()
            + 1;
        self.pos = end + 1;
        Ok(PlPgSqlStatement::Perform {
            query: Box::new(query),
            source,
            line,
        })
    }

    fn parse_open(&mut self) -> Result<PlPgSqlStatement, ParseError> {
        let line = self.line();
        self.expect_word("open")?;
        let cursor = self.expect_name()?;
        let mut arguments = Vec::new();
        if matches!(self.token(), Token::LParen) {
            self.bump();
            let end = self.find_token(self.pos, &Token::RParen)?;
            arguments = self.parse_cursor_argument_list_range(self.pos, end)?;
            self.pos = end;
            self.expect_token(&Token::RParen)?;
        }
        let scroll = if self.eat_word("no") {
            self.expect_word("scroll")?;
            Some(false)
        } else if self.eat_word("scroll") {
            Some(true)
        } else {
            None
        };
        let mut query = None;
        let mut dynamic_query = None;
        let mut using = Vec::new();
        if self.eat_word("for") {
            if self.eat_word("execute") {
                dynamic_query = Some(self.parse_expr_to_words(&["using"])?);
                if self.eat_word("using") {
                    using = self.parse_expr_list_to_token(&Token::Semicolon)?;
                }
            } else {
                let end = self.find_token(self.pos, &Token::Semicolon)?;
                query = Some(Box::new(self.parse_sql_range(self.pos, end)?));
                self.pos = end;
            }
        }
        self.expect_token(&Token::Semicolon)?;
        Ok(PlPgSqlStatement::Open {
            cursor,
            scroll,
            arguments,
            query,
            dynamic_query,
            using,
            line,
        })
    }

    fn parse_fetch(&mut self) -> Result<PlPgSqlStatement, ParseError> {
        let move_only = self.eat_word("move");
        if !move_only {
            self.expect_word("fetch")?;
        }
        let start = self.pos;
        let semicolon = self.find_token(start, &Token::Semicolon)?;
        let into_at = (start..semicolon).find(|pos| {
            self.word_at(*pos).is_some_and(|word| word == "into")
                && self.is_top_level_between(start, *pos)
        });
        let cursor_end = into_at.unwrap_or(semicolon);
        let connector = (start..cursor_end).rev().find(|pos| {
            self.word_at(*pos)
                .is_some_and(|word| word == "from" || word == "in")
        });
        let cursor_pos = connector.map_or(cursor_end.saturating_sub(1), |pos| pos + 1);
        let cursor = match self.tokens.get(cursor_pos).map(|(token, _)| token) {
            Some(Token::Ident(name)) => name.clone(),
            other => return Err(self.error(format!("expected cursor name, found {other:?}"))),
        };
        let direction_end = connector.unwrap_or(cursor_pos);
        let direction = self.slice_tokens(start, direction_end).trim().to_string();
        let into = if let Some(into_at) = into_at {
            self.pos = into_at + 1;
            Some(self.parse_into()?)
        } else {
            self.pos = semicolon;
            None
        };
        self.expect_token(&Token::Semicolon)?;
        Ok(PlPgSqlStatement::Fetch {
            cursor,
            direction,
            into,
            move_only,
        })
    }

    fn parse_close(&mut self) -> Result<PlPgSqlStatement, ParseError> {
        self.expect_word("close")?;
        let cursor = self.expect_name()?;
        self.expect_token(&Token::Semicolon)?;
        Ok(PlPgSqlStatement::Close(cursor))
    }

    fn parse_get_diagnostics(&mut self) -> Result<PlPgSqlStatement, ParseError> {
        let start = self.pos;
        let line = self.source[..self.tokens[start].1]
            .bytes()
            .filter(|byte| *byte == b'\n')
            .count()
            + 1;
        self.expect_word("get")?;
        let stacked = if self.eat_word("stacked") {
            true
        } else {
            let _ = self.eat_word("current");
            false
        };
        self.expect_word("diagnostics")?;
        let mut items = Vec::new();
        loop {
            let target = self.parse_target()?;
            if !self.eat_assignment_operator() {
                return Err(self.error("expected `=` in GET DIAGNOSTICS"));
            }
            let item = self.expect_name()?;
            items.push((target, item));
            if matches!(self.token(), Token::Comma) {
                self.bump();
            } else {
                break;
            }
        }
        self.expect_token(&Token::Semicolon)?;
        Ok(PlPgSqlStatement::GetDiagnostics {
            stacked,
            items,
            line,
        })
    }

    fn parse_assert(&mut self) -> Result<PlPgSqlStatement, ParseError> {
        let start = self.pos;
        let line = self.source[..self.tokens[start].1]
            .bytes()
            .filter(|byte| *byte == b'\n')
            .count()
            + 1;
        self.expect_word("assert")?;
        let end = self.find_token(self.pos, &Token::Semicolon)?;
        let comma = self.top_level_commas(self.pos, end).into_iter().next();
        let condition_end = comma.unwrap_or(end);
        let condition = self.parse_expr_range(self.pos, condition_end)?;
        let message = comma
            .map(|comma| self.parse_expr_range(comma + 1, end))
            .transpose()?;
        self.pos = end + 1;
        Ok(PlPgSqlStatement::Assert {
            condition,
            message,
            line,
        })
    }

    fn parse_transaction(&mut self) -> Result<PlPgSqlStatement, ParseError> {
        let commit = self.eat_word("commit");
        if !commit {
            self.expect_word("rollback")?;
        }
        let _ = self.eat_word("work") || self.eat_word("transaction");
        let chain = if self.eat_word("and") {
            let no = self.eat_word("no");
            self.expect_word("chain")?;
            !no
        } else {
            false
        };
        self.expect_token(&Token::Semicolon)?;
        Ok(PlPgSqlStatement::Transaction { commit, chain })
    }

    fn assignment_starts(&mut self) -> bool {
        let saved = self.pos;
        let result = self.parse_target().is_ok() && self.assignment_operator_starts();
        self.pos = saved;
        result
    }

    fn assignment_operator_starts(&self) -> bool {
        matches!(self.token(), Token::Eq)
            || (matches!(self.token(), Token::Colon)
                && matches!(self.tokens.get(self.pos + 1), Some((Token::Eq, _))))
    }

    fn eat_assignment_operator(&mut self) -> bool {
        if matches!(self.token(), Token::Colon)
            && matches!(self.tokens.get(self.pos + 1), Some((Token::Eq, _)))
        {
            self.pos += 2;
            true
        } else if matches!(self.token(), Token::Eq) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn parse_assignment(&mut self) -> Result<PlPgSqlStatement, ParseError> {
        let start = self.pos;
        let line = self.source[..self.tokens[start].1]
            .bytes()
            .filter(|byte| *byte == b'\n')
            .count()
            + 1;
        let target = self.parse_target()?;
        if !self.eat_assignment_operator() {
            return Err(self.error("expected assignment operator"));
        }
        let end = self.find_token(self.pos, &Token::Semicolon)?;
        let value = self.parse_expr_range(self.pos, end)?;
        self.pos = end + 1;
        Ok(PlPgSqlStatement::Assign {
            target,
            value,
            line,
        })
    }

    fn parse_target(&mut self) -> Result<PlPgSqlTarget, ParseError> {
        let first = match self.token() {
            Token::Param(number) => {
                let number = *number;
                self.bump();
                format!("${number}")
            }
            _ => self.expect_name()?,
        };
        let mut path = vec![first];
        let mut subscripts = Vec::new();
        loop {
            if matches!(self.token(), Token::Dot) {
                self.bump();
                path.push(self.expect_name()?);
            } else if matches!(self.token(), Token::LBracket) {
                self.bump();
                let end = self.find_token(self.pos, &Token::RBracket)?;
                subscripts.push(self.parse_expr_range(self.pos, end)?);
                self.pos = end + 1;
            } else {
                break;
            }
        }
        Ok(PlPgSqlTarget { path, subscripts })
    }

    fn parse_targets_until_word(&mut self, stop: &str) -> Result<Vec<PlPgSqlTarget>, ParseError> {
        let targets = self.parse_target_list()?;
        if !self.at_word(stop) {
            return Err(self.error(format!("expected {stop} after target list")));
        }
        Ok(targets)
    }

    fn parse_target_list(&mut self) -> Result<Vec<PlPgSqlTarget>, ParseError> {
        let mut targets = Vec::new();
        loop {
            targets.push(self.parse_target()?);
            if matches!(self.token(), Token::Comma) {
                self.bump();
            } else {
                break;
            }
        }
        Ok(targets)
    }

    fn parse_into(&mut self) -> Result<PlPgSqlInto, ParseError> {
        let strict = self.eat_word("strict");
        let mut targets = Vec::new();
        loop {
            targets.push(self.parse_target()?);
            if matches!(self.token(), Token::Comma) {
                self.bump();
            } else {
                break;
            }
        }
        Ok(PlPgSqlInto { strict, targets })
    }

    fn parse_static_sql(&mut self) -> Result<PlPgSqlStatement, ParseError> {
        let start = self.pos;
        let line = self.source[..self.tokens[start].1]
            .bytes()
            .filter(|byte| *byte == b'\n')
            .count()
            + 1;
        let end = self.find_token(start, &Token::Semicolon)?;
        let mut into = None;
        let mut sql = self.slice_tokens(start, end).trim().to_string();
        let select = if self.word_at(start).is_some_and(|word| word == "select") {
            Some(start)
        } else if self.word_at(start).is_some_and(|word| word == "with") {
            (start + 1..end)
                .find(|pos| {
                    self.word_at(*pos).is_some_and(|word| {
                        ["select", "insert", "update", "delete", "merge"].contains(&word.as_str())
                    }) && self.is_top_level_between(start, *pos)
                })
                .filter(|pos| self.word_at(*pos).is_some_and(|word| word == "select"))
        } else {
            None
        };
        let returning = select
            .is_none()
            .then(|| {
                (start..end).find(|pos| {
                    self.word_at(*pos).is_some_and(|word| word == "returning")
                        && self.is_top_level_between(start, *pos)
                })
            })
            .flatten();
        let into_at = select.or(returning).and_then(|after| {
            (after + 1..end).find(|pos| {
                self.word_at(*pos).is_some_and(|word| word == "into")
                    && self.is_top_level_between(start, *pos)
            })
        });
        if let Some(into_at) = into_at {
            self.pos = into_at + 1;
            let parsed_into = self.parse_into()?;
            let after_targets = self.pos;
            sql = format!(
                "{} {}",
                self.slice_tokens(start, into_at).trim_end(),
                self.slice_tokens(after_targets, end).trim_start()
            );
            into = Some(parsed_into);
        }
        let statement = parse_one(sql.trim())?;
        self.pos = end + 1;
        Ok(PlPgSqlStatement::Sql {
            statement: Box::new(statement),
            source: self.slice_tokens(start, end).trim().to_string(),
            line,
            into,
        })
    }

    fn parse_expr_to_word(&mut self, words: &[&str]) -> Result<Expr, ParseError> {
        self.parse_expr_to_words(words)
    }

    fn parse_expr_to_words(&mut self, words: &[&str]) -> Result<Expr, ParseError> {
        let end = self.find_top_word_or_semicolon(self.pos, words)?;
        let expr = self.parse_expr_range(self.pos, end)?;
        self.pos = end;
        Ok(expr)
    }

    fn parse_expr_list_to_word(&mut self, word: &str) -> Result<Vec<Expr>, ParseError> {
        let end = self.find_top_word(self.pos, &[word])?;
        let values = self.parse_expr_list_range(self.pos, end)?;
        self.pos = end;
        Ok(values)
    }

    fn parse_expr_list_to_token(&mut self, token: &Token) -> Result<Vec<Expr>, ParseError> {
        let end = self.find_token(self.pos, token)?;
        let values = if end == self.pos {
            Vec::new()
        } else {
            self.parse_expr_list_range(self.pos, end)?
        };
        self.pos = end;
        Ok(values)
    }

    fn parse_expr_list_range(&self, start: usize, end: usize) -> Result<Vec<Expr>, ParseError> {
        let mut values = Vec::new();
        let mut item_start = start;
        for comma in self.top_level_commas(start, end) {
            values.push(self.parse_expr_range(item_start, comma)?);
            item_start = comma + 1;
        }
        values.push(self.parse_expr_range(item_start, end)?);
        Ok(values)
    }

    fn parse_cursor_argument_list_range(
        &self,
        start: usize,
        end: usize,
    ) -> Result<Vec<PlPgSqlCursorArgument>, ParseError> {
        if start == end {
            return Ok(Vec::new());
        }
        let mut arguments = Vec::new();
        let mut item_start = start;
        for comma in self.top_level_commas(start, end) {
            arguments.push(self.parse_cursor_argument_range(item_start, comma)?);
            item_start = comma + 1;
        }
        arguments.push(self.parse_cursor_argument_range(item_start, end)?);
        Ok(arguments)
    }

    fn parse_cursor_argument_range(
        &self,
        start: usize,
        end: usize,
    ) -> Result<PlPgSqlCursorArgument, ParseError> {
        let separator = self.find_top(start, |parser, pos| {
            pos < end
                && (matches!(parser.tokens[pos].0, Token::NamedArg)
                    || (matches!(parser.tokens[pos].0, Token::Colon)
                        && matches!(parser.tokens.get(pos + 1), Some((Token::Eq, _)))))
        });
        if separator >= end {
            return Ok(PlPgSqlCursorArgument::Positional(
                self.parse_expr_range(start, end)?,
            ));
        }
        let Some(name) = self.word_at(start) else {
            return Err(ParseError::new(
                "expected cursor parameter name",
                self.tokens[start].1,
            ));
        };
        if separator != start + 1 {
            return Err(ParseError::new(
                "expected cursor parameter name",
                self.tokens[start].1,
            ));
        }
        let value_start = separator
            + if matches!(self.tokens[separator].0, Token::NamedArg) {
                1
            } else {
                2
            };
        Ok(PlPgSqlCursorArgument::Named {
            name,
            value: self.parse_expr_range(value_start, end)?,
        })
    }

    fn parse_expr_range(&self, start: usize, end: usize) -> Result<Expr, ParseError> {
        if start >= end {
            return Err(ParseError::new("expected expression", self.tokens[start].1));
        }
        let source = self.slice_tokens(start, end).trim();
        if self.find_top(start, |parser, pos| {
            parser.word_at(pos).as_deref() == Some("from")
        }) >= end
        {
            return parse_expression(source);
        }
        match parse(&format!("SELECT {source}"))?.into_iter().next() {
            Some(Statement::Query(query)) => Ok(Expr::ScalarSubquery(Box::new(query))),
            _ => unreachable!("SELECT parses as a query statement"),
        }
    }

    fn parse_sql_range(&self, start: usize, end: usize) -> Result<Statement, ParseError> {
        parse_one(self.slice_tokens(start, end).trim())
    }

    fn slice_tokens(&self, start: usize, end: usize) -> &str {
        let from = self
            .tokens
            .get(start)
            .map_or(self.source.len(), |(_, at)| *at);
        let to = self
            .tokens
            .get(end)
            .map_or(self.source.len(), |(_, at)| *at);
        &self.source[from..to]
    }

    fn find_token(&self, start: usize, wanted: &Token) -> Result<usize, ParseError> {
        let found = self.find_top(start, |parser, pos| parser.tokens[pos].0 == *wanted);
        if found >= self.tokens.len() - 1 && wanted != &Token::Eof {
            Err(ParseError::new(
                format!("expected {wanted:?}"),
                self.tokens[found].1,
            ))
        } else {
            Ok(found)
        }
    }

    fn find_top_word(&self, start: usize, words: &[&str]) -> Result<usize, ParseError> {
        let found = self.find_top(start, |parser, pos| {
            parser
                .word_at(pos)
                .is_some_and(|word| words.contains(&word.as_str()))
        });
        if found >= self.tokens.len() - 1 {
            Err(ParseError::new(
                format!("expected {}", words.join(" or ")),
                self.tokens[found].1,
            ))
        } else {
            Ok(found)
        }
    }

    fn find_top_word_or_semicolon(
        &self,
        start: usize,
        words: &[&str],
    ) -> Result<usize, ParseError> {
        let found = self.find_top(start, |parser, pos| {
            matches!(parser.tokens[pos].0, Token::Semicolon)
                || parser
                    .word_at(pos)
                    .is_some_and(|word| words.contains(&word.as_str()))
        });
        if found >= self.tokens.len() - 1 {
            Err(ParseError::new(
                format!("expected {} or `;`", words.join(" or ")),
                self.tokens[found].1,
            ))
        } else {
            Ok(found)
        }
    }

    fn find_top<F>(&self, start: usize, mut stop: F) -> usize
    where
        F: FnMut(&Self, usize) -> bool,
    {
        let mut parens = 0usize;
        let mut brackets = 0usize;
        let mut sql_case = 0usize;
        let mut pos = start;
        while pos < self.tokens.len() - 1 {
            match self.tokens[pos].0 {
                Token::LParen => parens += 1,
                Token::RParen => parens = parens.saturating_sub(1),
                Token::LBracket => brackets += 1,
                Token::RBracket => brackets = brackets.saturating_sub(1),
                _ => {}
            }
            if parens == 0 && brackets == 0 {
                if self.word_at(pos).is_some_and(|word| word == "case") {
                    sql_case += 1;
                } else if self.word_at(pos).is_some_and(|word| word == "end") && sql_case > 0 {
                    sql_case -= 1;
                }
                if sql_case == 0 && stop(self, pos) {
                    return pos;
                }
            }
            pos += 1;
        }
        pos
    }

    fn top_level_commas(&self, start: usize, end: usize) -> Vec<usize> {
        let mut commas = Vec::new();
        let mut cursor = start;
        while cursor < end {
            let found = self.find_top(cursor, |parser, pos| {
                pos < end && matches!(parser.tokens[pos].0, Token::Comma)
            });
            if found >= end {
                break;
            }
            commas.push(found);
            cursor = found + 1;
        }
        commas
    }

    fn find_top_dot_dot(&self, start: usize, end: usize) -> Option<(usize, bool)> {
        let found = self.find_top(start, |parser, pos| {
            pos >= end
                || (matches!(parser.tokens[pos].0, Token::Dot)
                    && (matches!(parser.tokens.get(pos + 1), Some((Token::Dot, _)))
                        || matches!(
                            parser.tokens.get(pos + 1),
                            Some((Token::FloatLit(value), _)) if value.starts_with('.')
                        )))
        });
        (found < end).then(|| {
            let compact = matches!(
                self.tokens.get(found + 1),
                Some((Token::FloatLit(value), _)) if value.starts_with('.')
            );
            (found, compact)
        })
    }

    fn is_top_level_between(&self, start: usize, candidate: usize) -> bool {
        self.find_top(start, |_parser, pos| pos == candidate) == candidate
    }
}

fn parse_one(sql: &str) -> Result<Statement, ParseError> {
    let mut statements = parse(sql)?;
    if statements.len() != 1 {
        return Err(ParseError::new(
            "expected exactly one embedded SQL statement",
            0,
        ));
    }
    Ok(statements.remove(0))
}
