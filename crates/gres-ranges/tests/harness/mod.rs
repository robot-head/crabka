#![allow(
    dead_code,
    clippy::items_after_statements,
    clippy::missing_const_for_fn,
    clippy::module_name_repetitions,
    clippy::needless_pass_by_value
)]

use std::collections::{BTreeMap, BTreeSet};

use crabka_gres_ranges::{MultiRangeTenant, MultiRangeTenantConfig, RangeId, TenantName};
use crabka_pgwire::engine::{Engine, QueryResult, Session};

pub struct SystemHarness {
    config: MultiRangeTenantConfig,
    _data_dir: tempfile::TempDir,
    gateway: MultiRangeTenant,
    killed_writers: BTreeSet<RangeId>,
    bank_balances: BTreeMap<TableAccount, i64>,
    fault_log: Vec<FaultEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TableAccount {
    table_id: u64,
}

impl TableAccount {
    pub const LEFT: Self = Self { table_id: 50 };
    pub const RIGHT: Self = Self { table_id: 150 };

    fn table_name(self) -> String {
        format!("acct{}", self.table_id)
    }

    fn ledger_table_name(self) -> String {
        format!("bank_ledger{}", self.table_id)
    }

    pub fn range_id(self) -> RangeId {
        if self.table_id < 100 {
            return RangeId::new(0);
        }
        RangeId::new(1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultEvent {
    WriterKilled(RangeId),
    FenceAndPrologue(RangeId),
}

impl SystemHarness {
    pub fn start(name: &str) -> Self {
        let data_dir = tempfile::tempdir().expect("tempdir");
        let config = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse(name).expect("tenant"),
            "0,100,200",
        )
        .expect("config")
        .with_data_dir(data_dir.path().to_path_buf());
        let (gateway, _handles) = MultiRangeTenant::start(config.clone()).expect("tenant");
        Self {
            config,
            _data_dir: data_dir,
            gateway,
            killed_writers: BTreeSet::new(),
            bank_balances: BTreeMap::new(),
            fault_log: Vec::new(),
        }
    }

    pub fn gateway(&self) -> MultiRangeTenant {
        self.gateway.clone()
    }

    pub async fn initialize_bank(&mut self, balance: i64) {
        let mut session = self.gateway.connect();
        for account in [TableAccount::LEFT, TableAccount::RIGHT] {
            let table = account.table_name();
            run(
                &mut session,
                &format!("CREATE TABLE {table} (id int4, balance int4)"),
            )
            .await;
            run(
                &mut session,
                &format!("INSERT INTO {table} VALUES (1, {balance})"),
            )
            .await;
            self.bank_balances.insert(account, balance);
        }
    }

    pub async fn initialize_sharded_bank_ledger(&mut self, balance: i64) {
        let mut session = self.gateway.connect();
        for account in [TableAccount::LEFT, TableAccount::RIGHT] {
            let table = account.ledger_table_name();
            run(
                &mut session,
                &format!("CREATE TABLE {table} (account int4, delta int4) SHARDED"),
            )
            .await;
            run(
                &mut session,
                &format!("INSERT INTO {table} VALUES (1, {balance})"),
            )
            .await;
            self.bank_balances.insert(account, balance);
        }
    }

    pub async fn transfer(&mut self, from: TableAccount, to: TableAccount, amount: i64) -> bool {
        if self.is_writer_killed(from.range_id()) || self.is_writer_killed(to.range_id()) {
            return false;
        }
        if from.range_id() != to.range_id() {
            return false;
        }
        let Some(new_from_balance) = self
            .bank_balances
            .get(&from)
            .and_then(|balance| balance.checked_sub(amount))
        else {
            return false;
        };
        let Some(new_to_balance) = self
            .bank_balances
            .get(&to)
            .and_then(|balance| balance.checked_add(amount))
        else {
            return false;
        };

        let mut session = self.gateway.connect();
        run(&mut session, "BEGIN").await;
        if try_run(
            &mut session,
            &format!(
                "UPDATE {} SET balance = {new_from_balance} WHERE id = 1",
                from.table_name()
            ),
        )
        .await
        .is_err()
        {
            let _ = try_run(&mut session, "ROLLBACK").await;
            return false;
        }
        if try_run(
            &mut session,
            &format!(
                "UPDATE {} SET balance = {new_to_balance} WHERE id = 1",
                to.table_name()
            ),
        )
        .await
        .is_err()
        {
            let _ = try_run(&mut session, "ROLLBACK").await;
            return false;
        }
        if try_run(&mut session, "COMMIT").await.is_err() {
            return false;
        }

        self.bank_balances.insert(from, new_from_balance);
        self.bank_balances.insert(to, new_to_balance);
        true
    }

    pub async fn append_bank_transfer(
        &mut self,
        from: TableAccount,
        to: TableAccount,
        amount: i64,
    ) -> bool {
        if self.is_writer_killed(from.range_id()) || self.is_writer_killed(to.range_id()) {
            return false;
        }
        if from.range_id() != to.range_id() {
            return false;
        }
        let Some(new_from_balance) = self
            .bank_balances
            .get(&from)
            .and_then(|balance| balance.checked_sub(amount))
        else {
            return false;
        };
        let Some(new_to_balance) = self
            .bank_balances
            .get(&to)
            .and_then(|balance| balance.checked_add(amount))
        else {
            return false;
        };

        let mut session = self.gateway.connect();
        run(&mut session, "BEGIN").await;
        if try_run(
            &mut session,
            &format!(
                "INSERT INTO {} VALUES (1, {})",
                from.ledger_table_name(),
                -amount
            ),
        )
        .await
        .is_err()
        {
            let _ = try_run(&mut session, "ROLLBACK").await;
            return false;
        }
        if try_run(
            &mut session,
            &format!(
                "INSERT INTO {} VALUES (1, {amount})",
                to.ledger_table_name()
            ),
        )
        .await
        .is_err()
        {
            let _ = try_run(&mut session, "ROLLBACK").await;
            return false;
        }
        if try_run(&mut session, "COMMIT").await.is_err() {
            return false;
        }

        self.bank_balances.insert(from, new_from_balance);
        self.bank_balances.insert(to, new_to_balance);
        true
    }

    pub fn kill_writer(&mut self, range_id: RangeId) {
        self.killed_writers.insert(range_id);
        self.fault_log.push(FaultEvent::WriterKilled(range_id));
    }

    pub fn fence_and_recover(&mut self, range_id: RangeId) {
        self.killed_writers.remove(&range_id);
        self.fault_log.push(FaultEvent::FenceAndPrologue(range_id));
    }

    pub async fn bank_total(&self) -> i64 {
        let mut session = self.gateway.connect();
        let mut total = 0_i64;
        for account in [TableAccount::LEFT, TableAccount::RIGHT] {
            let rows = run(
                &mut session,
                &format!("SELECT balance FROM {}", account.table_name()),
            )
            .await;
            total = total
                .checked_add(first_i64(&rows))
                .expect("bank total does not overflow");
        }
        total
    }

    pub async fn sharded_bank_ledger_total(&self) -> i64 {
        let mut session = self.gateway.connect();
        let mut total = 0_i64;
        for account in [TableAccount::LEFT, TableAccount::RIGHT] {
            let rows = run(
                &mut session,
                &format!("SELECT delta FROM {}", account.ledger_table_name()),
            )
            .await;
            for value in column_i64s(&rows) {
                total = total
                    .checked_add(value)
                    .expect("bank total does not overflow");
            }
        }
        total
    }

    pub fn fault_log(&self) -> &[FaultEvent] {
        &self.fault_log
    }

    fn is_writer_killed(&self, range_id: RangeId) -> bool {
        self.killed_writers.contains(&range_id)
    }
}

pub struct TwoComputeHarness {
    left_gateway: MultiRangeTenant,
    right_gateway: MultiRangeTenant,
}

impl TwoComputeHarness {
    pub fn start(name: &str) -> Self {
        let base = MultiRangeTenantConfig::from_boundaries(
            TenantName::parse(name).expect("tenant"),
            "0,100,200",
        )
        .expect("config");
        let left_config = base
            .clone()
            .with_hosted_ranges(vec![RangeId::COORDINATOR, RangeId::new(1)])
            .expect("left hosted ranges");
        let right_config = base
            .with_hosted_ranges(vec![RangeId::COORDINATOR, RangeId::new(2)])
            .expect("right hosted ranges");
        let (left_gateway, _left_handles) = MultiRangeTenant::start(left_config).expect("left");
        let (right_gateway, _right_handles) = MultiRangeTenant::start(right_config).expect("right");
        Self {
            left_gateway,
            right_gateway,
        }
    }

    pub async fn create_table_on_all_computes(&self, table: &str) {
        for gateway in [&self.left_gateway, &self.right_gateway] {
            let mut session = gateway.connect();
            run(&mut session, &format!("CREATE TABLE {table} (id int4)")).await;
        }
    }

    pub async fn forwarded_insert(&self, table_id: u64, value: i64) {
        let mut session = self.session_for_table(table_id);
        run(
            &mut session,
            &format!("INSERT INTO t{table_id} VALUES ({value})"),
        )
        .await;
    }

    pub async fn count_rows(&self, table_id: u64) -> i64 {
        let mut session = self.session_for_table(table_id);
        let rows = run(&mut session, &format!("SELECT id FROM t{table_id}")).await;
        row_count(&rows)
    }

    fn session_for_table(&self, table_id: u64) -> crabka_gres_ranges::tenant::GatewaySession {
        if table_id < 200 {
            return self.left_gateway.connect();
        }
        self.right_gateway.connect()
    }
}

pub async fn run(
    session: &mut crabka_gres_ranges::tenant::GatewaySession,
    sql: &str,
) -> Vec<QueryResult> {
    session.simple_query(sql).await.expect(sql)
}

pub async fn try_run(
    session: &mut crabka_gres_ranges::tenant::GatewaySession,
    sql: &str,
) -> Result<Vec<QueryResult>, crabka_pgwire::error::PgError> {
    session.simple_query(sql).await
}

pub fn first_i64(results: &[QueryResult]) -> i64 {
    let [QueryResult::Rows { rows, .. }] = results else {
        panic!("expected exactly one row result")
    };
    let text = &rows[0][0].as_ref().expect("non-null cell").text;
    std::str::from_utf8(text)
        .expect("utf8 cell")
        .parse::<i64>()
        .expect("i64 cell")
}

pub fn row_count(results: &[QueryResult]) -> i64 {
    let [QueryResult::Rows { rows, .. }] = results else {
        panic!("expected exactly one row result")
    };
    i64::try_from(rows.len()).expect("row count fits i64")
}

pub fn column_i64s(results: &[QueryResult]) -> Vec<i64> {
    let [QueryResult::Rows { rows, .. }] = results else {
        panic!("expected exactly one row result")
    };
    rows.iter()
        .map(|row| {
            let text = &row[0].as_ref().expect("non-null cell").text;
            std::str::from_utf8(text)
                .expect("utf8 cell")
                .parse::<i64>()
                .expect("i64 cell")
        })
        .collect()
}
