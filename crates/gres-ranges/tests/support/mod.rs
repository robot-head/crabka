use crabka_gres_ranges::tenant::GatewaySession;
use crabka_pgwire::{
    engine::{BoundParam, Cell, ExecuteOutcome, QueryResult, Session},
    error::PgError,
};

pub trait ExtendedQueryV2 {
    async fn extended_query_v2(
        &mut self,
        sql: &str,
        params: &[BoundParam],
    ) -> Result<Vec<QueryResult>, PgError>;
}

impl ExtendedQueryV2 for GatewaySession {
    async fn extended_query_v2(
        &mut self,
        sql: &str,
        params: &[BoundParam],
    ) -> Result<Vec<QueryResult>, PgError> {
        if params.is_empty() {
            return self.simple_query(sql).await;
        }
        let description = self.parse("test_statement", sql, &[]).await?;
        self.bind("test_portal", "test_statement", params, &[])
            .await?;
        let outcome = self.execute("test_portal", 0).await?;
        self.close(crabka_pgwire::engine::CloseTarget::Portal("test_portal"))
            .await?;
        self.close(crabka_pgwire::engine::CloseTarget::Statement(
            "test_statement",
        ))
        .await?;
        Ok(vec![match outcome {
            ExecuteOutcome::Rows { rows, completion } => QueryResult::Rows {
                fields: description.fields,
                rows: rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|value| {
                                value.map(|value| Cell {
                                    text: value.clone(),
                                    binary: value,
                                })
                            })
                            .collect()
                    })
                    .collect(),
                tag: completion.unwrap_or_default(),
            },
            ExecuteOutcome::CommandComplete { tag } => QueryResult::Command { tag },
            ExecuteOutcome::EmptyQuery => QueryResult::Empty,
            _ => unreachable!("test helper only executes SQL query outcomes"),
        }])
    }
}
