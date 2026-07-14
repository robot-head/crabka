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
        let description = self.parse("", sql, &[]).await?;
        self.bind("", "", params, &[]).await?;
        let outcome = self.execute("", 0).await?;
        self.close(crabka_pgwire::engine::CloseTarget::Portal(""))
            .await?;
        self.close(crabka_pgwire::engine::CloseTarget::Statement(""))
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
