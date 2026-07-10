//! Canned-response engine: enough surface for psql, driver integration
//! tests, and the conformance harness to exercise the wire protocol.

use bytes::Bytes;

use crate::{
    engine::{BoundParam, Cell, Engine, FieldDescription, QueryResult, Session, TxStatus, oids},
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
        StubSession
    }
}

/// Per-connection session for the canned stub engine. Holds no state; the
/// transaction status is always `Idle`.
pub struct StubSession;

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

    async fn extended_query(
        &mut self,
        sql: &str,
        params: &[BoundParam],
    ) -> Result<Vec<QueryResult>, PgError> {
        if normalize(sql) != "select $1" {
            return Self::canned(sql);
        }

        let Some(param) = params.first() else {
            return Err(PgError::error(
                sqlstate::UNDEFINED_PARAMETER,
                "there is no parameter $1",
            ));
        };

        let rows = vec![vec![param_to_cell(param)?]];
        let tag = select_tag(&rows);
        Ok(vec![QueryResult::Rows {
            fields: vec![field_for_param(param, "?column?")],
            rows,
            tag,
        }])
    }

    // Returns 0A000 for any unrecognized SQL — acceptable for the stub; a real engine reports proper codes (e.g. 26000) per statement state.
    async fn describe(&mut self, sql: &str) -> Result<Vec<FieldDescription>, PgError> {
        if normalize(sql) == "select $1" {
            return Ok(vec![text_field("?column?")]);
        }

        match Self::canned(sql)?.first() {
            Some(QueryResult::Rows { fields, .. }) => Ok(fields.clone()),
            _ => Ok(Vec::new()),
        }
    }

    async fn describe_prepared(
        &mut self,
        sql: &str,
        param_types: &[u32],
    ) -> Result<(Vec<FieldDescription>, Vec<u32>), PgError> {
        if normalize(sql) != "select $1" {
            return self
                .describe(sql)
                .await
                .map(|fields| (fields, param_types.to_vec()));
        }

        let field = if param_types.first() == Some(&oids::INT4) {
            int4_field("?column?")
        } else {
            text_field("?column?")
        };
        Ok((vec![field], param_types.to_vec()))
    }

    fn tx_status(&self) -> TxStatus {
        TxStatus::Idle
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

fn field_for_param(param: &BoundParam, name: &str) -> FieldDescription {
    let mut field = if param.type_oid == Some(oids::INT4) {
        int4_field(name)
    } else {
        text_field(name)
    };
    field.format = param.format;
    field
}
