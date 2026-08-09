//! Canned-response engine.
//!
//! It gives enough surface for psql, driver integration tests, and the
//! conformance harness to exercise the wire protocol.

use std::collections::HashMap;

use bytes::Bytes;

use crate::{
    engine::{
        BoundParam, Cell, CloseTarget, Engine, ExecuteOutcome, FieldDescription, PortalDescription,
        PreparedDescription, QueryResult, Session, TxStatus, oids,
    },
    error::{PgError, sqlstate},
};

pub const STUB_VERSION: &str =
    "PostgreSQL 18.0 (crabgresql 0.1.0) on aarch64, compiled by rustc, 64-bit";

#[derive(Debug, Default, Clone)]
pub struct StubEngine {}

impl StubEngine {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

impl Engine for StubEngine {
    type Session = StubSession;

    fn connect(&self) -> StubSession {
        StubSession::default()
    }
}

/// Per-connection session for the canned stub engine. It holds no state, and
/// the transaction status is always `Idle`.
#[derive(Clone)]
struct StubPrepared {
    sql: String,
    description: PreparedDescription,
}

#[derive(Clone)]
struct StubPortal {
    sql: String,
    params: Vec<BoundParam>,
    description: PortalDescription,
    formats: Vec<i16>,
    execution: StubExecution,
}

#[derive(Clone)]
enum StubExecution {
    NotStarted,
    Rows {
        rows: Vec<Vec<Option<Cell>>>,
        tag: String,
        position: usize,
    },
    Command(String),
    Empty,
}

#[derive(Default)]
pub struct StubSession {
    prepared: HashMap<String, StubPrepared>,
    portals: HashMap<String, StubPortal>,
}

impl StubSession {
    fn canned(sql: &str) -> Result<Vec<QueryResult>, PgError> {
        match normalize(sql).as_str() {
            "" => Ok(vec![QueryResult::Empty]),
            "select 1" => {
                let rows = vec![vec![Some(int4_cell(1))]];
                let tag = select_tag(&rows);
                Ok(vec![QueryResult::Rows {
                    fields: vec![int4_field("?column?")],
                    rows,
                    tag,
                }])
            }
            "select version()" => {
                let rows = vec![vec![Some(text_cell(STUB_VERSION))]];
                let tag = select_tag(&rows);
                Ok(vec![QueryResult::Rows {
                    fields: vec![text_field("version")],
                    rows,
                    tag,
                }])
            }
            "select generate_series(1, 3)" => {
                let rows = [1, 2, 3]
                    .into_iter()
                    .map(|value| vec![Some(int4_cell(value))])
                    .collect::<Vec<_>>();
                let tag = select_tag(&rows);
                Ok(vec![QueryResult::Rows {
                    fields: vec![int4_field("generate_series")],
                    rows,
                    tag,
                }])
            }
            other => Err(PgError::error(
                sqlstate::FEATURE_NOT_SUPPORTED,
                format!("stub engine does not implement: {other}"),
            )),
        }
    }
}

impl Session for StubSession {
    async fn simple_query(&mut self, sql: &str) -> Result<Vec<QueryResult>, PgError> {
        // `pg_sleep` exists so cancellation has something to cancel.
        if let Some(secs) = normalize(sql)
            .strip_prefix("select pg_sleep(")
            .and_then(|rest| rest.strip_suffix(')'))
            .and_then(|n| n.parse::<u64>().ok())
        {
            tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
            let rows = vec![vec![Some(text_cell(""))]];
            let tag = select_tag(&rows);
            return Ok(vec![QueryResult::Rows {
                fields: vec![text_field("pg_sleep")],
                rows,
                tag,
            }]);
        }
        Self::canned(sql)
    }

    async fn parse(
        &mut self,
        name: &str,
        sql: &str,
        parameter_types: &[u32],
    ) -> Result<PreparedDescription, PgError> {
        if !name.is_empty() && self.prepared.contains_key(name) {
            return Err(PgError::error(
                sqlstate::DUPLICATE_PREPARED_STATEMENT,
                format!("prepared statement \"{name}\" already exists"),
            ));
        }
        let count = positional_parameter_count(sql).max(parameter_types.len());
        let mut types = vec![0; count];
        types[..parameter_types.len()].copy_from_slice(parameter_types);
        let fields = if normalize(sql) == "select $1" {
            vec![if types.first() == Some(&oids::INT4) {
                int4_field("?column?")
            } else {
                text_field("?column?")
            }]
        } else {
            match Self::canned(sql)?.first() {
                Some(QueryResult::Rows { fields, .. }) => fields.clone(),
                _ => vec![],
            }
        };
        let description = PreparedDescription {
            parameter_types: types,
            fields,
        };
        self.prepared.insert(
            name.to_owned(),
            StubPrepared {
                sql: sql.to_owned(),
                description: description.clone(),
            },
        );
        Ok(description)
    }

    async fn bind(
        &mut self,
        portal: &str,
        statement: &str,
        params: &[BoundParam],
        result_formats: &[i16],
    ) -> Result<PortalDescription, PgError> {
        if !portal.is_empty() && self.portals.contains_key(portal) {
            return Err(PgError::error(
                sqlstate::DUPLICATE_CURSOR,
                format!("cursor \"{portal}\" already exists"),
            ));
        }
        let prepared = self.prepared.get(statement).ok_or_else(|| {
            PgError::error(
                sqlstate::INVALID_SQL_STATEMENT_NAME,
                format!("prepared statement \"{statement}\" does not exist"),
            )
        })?;
        if params.len() != prepared.description.parameter_types.len() {
            return Err(PgError::protocol(format!(
                "bind message supplies {} parameters, but prepared statement requires {}",
                params.len(),
                prepared.description.parameter_types.len()
            )));
        }
        let formats = resolve_formats(result_formats, prepared.description.fields.len())?;
        let fields = prepared
            .description
            .fields
            .iter()
            .zip(&formats)
            .map(|(f, &format)| FieldDescription {
                format,
                ..f.clone()
            })
            .collect();
        let description = PortalDescription { fields };
        let params = params
            .iter()
            .zip(&prepared.description.parameter_types)
            .map(|(param, oid)| BoundParam {
                type_oid: Some(*oid).filter(|value| *value != 0).or(param.type_oid),
                ..param.clone()
            })
            .collect();
        self.portals.insert(
            portal.to_owned(),
            StubPortal {
                sql: prepared.sql.clone(),
                params,
                description: description.clone(),
                formats,
                execution: StubExecution::NotStarted,
            },
        );
        Ok(description)
    }

    async fn describe_statement(&mut self, name: &str) -> Result<PreparedDescription, PgError> {
        self.prepared
            .get(name)
            .map(|p| p.description.clone())
            .ok_or_else(|| {
                PgError::error(
                    sqlstate::INVALID_SQL_STATEMENT_NAME,
                    format!("prepared statement \"{name}\" does not exist"),
                )
            })
    }

    async fn describe_portal(&mut self, name: &str) -> Result<PortalDescription, PgError> {
        self.portals
            .get(name)
            .map(|p| p.description.clone())
            .ok_or_else(|| {
                PgError::error(
                    sqlstate::INVALID_CURSOR_NAME,
                    format!("portal \"{name}\" does not exist"),
                )
            })
    }

    async fn execute(&mut self, portal: &str, max_rows: u32) -> Result<ExecuteOutcome, PgError> {
        let p = self.portals.get_mut(portal).ok_or_else(|| {
            PgError::error(
                sqlstate::INVALID_CURSOR_NAME,
                format!("portal \"{portal}\" does not exist"),
            )
        })?;
        if matches!(p.execution, StubExecution::NotStarted) {
            let results = if normalize(&p.sql) == "select $1" {
                let param = p.params.first().ok_or_else(|| {
                    PgError::error(sqlstate::UNDEFINED_PARAMETER, "there is no parameter $1")
                })?;
                let rows = vec![vec![param_to_cell(param)?]];
                vec![QueryResult::Rows {
                    fields: vec![],
                    tag: select_tag(&rows),
                    rows,
                }]
            } else {
                Self::canned(&p.sql)?
            };
            p.execution = match results.into_iter().next() {
                Some(QueryResult::Rows { rows, tag, .. }) => StubExecution::Rows {
                    rows,
                    tag,
                    position: 0,
                },
                Some(QueryResult::Command { tag }) => StubExecution::Command(tag),
                _ => StubExecution::Empty,
            };
        }
        match &mut p.execution {
            StubExecution::Rows {
                rows,
                tag,
                position,
            } => {
                let remaining = rows.len() - *position;
                let take = if max_rows == 0 {
                    remaining
                } else {
                    remaining.min(max_rows as usize)
                };
                let batch = rows[*position..*position + take]
                    .iter()
                    .map(|row| {
                        row.iter()
                            .zip(&p.formats)
                            .map(|(cell, format)| {
                                cell.as_ref().map(|cell| {
                                    if *format == 1 {
                                        cell.binary.clone()
                                    } else {
                                        cell.text.clone()
                                    }
                                })
                            })
                            .collect()
                    })
                    .collect();
                *position += take;
                Ok(ExecuteOutcome::Rows {
                    rows: batch,
                    completion: (*position == rows.len()).then(|| tag.clone()),
                })
            }
            StubExecution::Command(tag) => Ok(ExecuteOutcome::CommandComplete { tag: tag.clone() }),
            StubExecution::Empty => Ok(ExecuteOutcome::EmptyQuery),
            StubExecution::NotStarted => unreachable!(),
        }
    }

    async fn close(&mut self, target: CloseTarget<'_>) -> Result<(), PgError> {
        match target {
            CloseTarget::Statement(name) => {
                self.prepared.remove(name);
            }
            CloseTarget::Portal(name) => {
                self.portals.remove(name);
            }
        }
        Ok(())
    }

    async fn sync(&mut self) -> Result<(), PgError> {
        self.portals.clear();
        Ok(())
    }

    fn tx_status(&self) -> TxStatus {
        TxStatus::Idle
    }
}

fn positional_parameter_count(sql: &str) -> usize {
    sql.as_bytes()
        .windows(2)
        .filter(|w| w[0] == b'$' && w[1].is_ascii_digit())
        .map(|w| (w[1] - b'0') as usize)
        .max()
        .unwrap_or(0)
}

fn resolve_formats(requested: &[i16], count: usize) -> Result<Vec<i16>, PgError> {
    let validate = |v| {
        if matches!(v, 0 | 1) {
            Ok(v)
        } else {
            Err(PgError::protocol(format!("invalid format code {v}")))
        }
    };
    match requested.len() {
        0 => Ok(vec![0; count]),
        1 => Ok(vec![validate(requested[0])?; count]),
        n if n == count => requested.iter().copied().map(validate).collect(),
        n => Err(PgError::protocol(format!(
            "bind message has {n} result formats but query has {count} columns"
        ))),
    }
}

fn normalize(sql: &str) -> String {
    sql.trim().trim_end_matches(';').trim().to_ascii_lowercase()
}

fn select_tag(rows: &[Vec<Option<Cell>>]) -> String {
    format!("SELECT {}", rows.len())
}

fn int4_field(name: &str) -> FieldDescription {
    FieldDescription {
        name: name.into(),
        table_oid: 0,
        column_id: 0,
        type_oid: oids::INT4,
        type_size: 4,
        type_modifier: -1,
        format: 0,
    }
}

fn text_field(name: &str) -> FieldDescription {
    FieldDescription {
        name: name.into(),
        table_oid: 0,
        column_id: 0,
        type_oid: oids::TEXT,
        type_size: -1,
        type_modifier: -1,
        format: 0,
    }
}

fn int4_cell(v: i32) -> Cell {
    Cell {
        text: Bytes::from(v.to_string()),
        binary: Bytes::copy_from_slice(&v.to_be_bytes()),
    }
}

fn text_cell(v: &str) -> Cell {
    let b = Bytes::copy_from_slice(v.as_bytes());
    Cell {
        text: b.clone(),
        binary: b,
    }
}

fn param_to_cell(param: &BoundParam) -> Result<Option<Cell>, PgError> {
    let Some(value) = &param.value else {
        return Ok(None);
    };

    match (param.type_oid, param.format) {
        (Some(oids::INT4), 0) => {
            let text = std::str::from_utf8(value)
                .map_err(|_| PgError::protocol("int4 text parameter is not UTF-8"))?;
            let int = text
                .parse::<i32>()
                .map_err(|_| PgError::protocol("int4 text parameter is invalid"))?;
            Ok(Some(int4_cell(int)))
        }
        (Some(oids::INT4), 1) => {
            let bytes = value.as_ref();
            if bytes.len() != 4 {
                return Err(PgError::protocol(format!(
                    "int4 binary parameter has length {}, expected 4",
                    bytes.len()
                )));
            }
            let int = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            Ok(Some(int4_cell(int)))
        }
        (_, 0) => Ok(Some(text_cell(
            std::str::from_utf8(value)
                .map_err(|_| PgError::protocol("text parameter is not UTF-8"))?,
        ))),
        (_, 1) => Ok(Some(Cell {
            text: value.clone(),
            binary: value.clone(),
        })),
        (_, code) => Err(PgError::protocol(format!(
            "invalid parameter format code {code}"
        ))),
    }
}
