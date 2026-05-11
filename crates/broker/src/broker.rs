//! Broker top-level. Real implementation lands in Task 11.

#![allow(dead_code)] // real fields and constructors land in Phase D.

use crate::handlers::HandlerTable;

pub struct Broker {
    handlers: HandlerTable,
}

impl Broker {
    pub(crate) fn handlers(&self) -> &HandlerTable {
        &self.handlers
    }
}
