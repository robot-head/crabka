//! Per-statement execution.

mod imports;
use imports::*;

#[path = "exec/assignment_values.rs"]
mod assignment_values;
#[path = "exec/base_table.rs"]
mod base_table;
#[path = "catalog_rows.rs"]
pub(crate) mod catalog_rows;
#[path = "exec/cluster.rs"]
mod cluster;
#[path = "exec/copy.rs"]
mod copy;
#[path = "exec/correlated_bind.rs"]
mod correlated_bind;
#[path = "exec/correlated_rows.rs"]
mod correlated_rows;
#[path = "ddl_alter.rs"]
pub(crate) mod ddl_alter;
pub(crate) use ddl_alter::resolve_table_access_method_oid;
#[path = "exec/ddl_drop.rs"]
mod ddl_drop;
#[path = "ddl_index.rs"]
mod ddl_index;
#[path = "ddl_inherit.rs"]
mod ddl_inherit;
#[path = "exec/ddl_misc.rs"]
mod ddl_misc;
#[path = "ddl_partition.rs"]
mod ddl_partition;
#[path = "exec/describe.rs"]
mod describe;
#[path = "exec/dml_assignments.rs"]
mod dml_assignments;
#[path = "exec/dml_returning.rs"]
mod dml_returning;
#[path = "exec/execution_context.rs"]
mod execution_context;
#[path = "exec/expr_walk.rs"]
mod expr_walk;
#[path = "exec/foreign_scan.rs"]
mod foreign_scan;
#[path = "exec/from_build.rs"]
mod from_build;
#[path = "exec/from_columns.rs"]
mod from_columns;
#[path = "exec/from_execution.rs"]
mod from_execution;
#[path = "exec/from_lateral.rs"]
mod from_lateral;
#[path = "exec/from_predicates.rs"]
mod from_predicates;
#[path = "exec/from_resolution.rs"]
mod from_resolution;
#[path = "exec/from_schema.rs"]
mod from_schema;
#[path = "exec/heap_write.rs"]
mod heap_write;
#[path = "exec/index_scan.rs"]
mod index_scan;
#[path = "exec/insert_source.rs"]
mod insert_source;
#[path = "exec/local_index_ddl.rs"]
mod local_index_ddl;
#[path = "exec/local_index_writes.rs"]
mod local_index_writes;
#[path = "exec/merge.rs"]
mod merge;
#[path = "exec/mvcc.rs"]
mod mvcc;
#[path = "exec/projection.rs"]
mod projection;
#[path = "exec/prune.rs"]
mod prune;
#[path = "exec/read.rs"]
mod read;
#[path = "exec/result_order.rs"]
mod result_order;
#[path = "exec/result_types.rs"]
mod result_types;
#[path = "exec/row_window.rs"]
mod row_window;
#[path = "exec/rule_bindings.rs"]
mod rule_bindings;
#[path = "exec/rule_images.rs"]
mod rule_images;
#[path = "exec/rule_runtime.rs"]
mod rule_runtime;
#[path = "exec/scan.rs"]
mod scan;
#[path = "exec/select.rs"]
mod select;
#[path = "exec/select_pushdown.rs"]
mod select_pushdown;
#[path = "exec/stored_scan.rs"]
mod stored_scan;
#[path = "exec/table_columns.rs"]
mod table_columns;
#[path = "exec/table_constraints.rs"]
mod table_constraints;
#[path = "exec/target_indirection.rs"]
mod target_indirection;
#[path = "exec/timestamp_write.rs"]
mod timestamp_write;
#[path = "exec/unique.rs"]
mod unique;
#[path = "exec/view_ddl.rs"]
mod view_ddl;
#[path = "exec/view_rules.rs"]
mod view_rules;
#[path = "exec/view_write.rs"]
mod view_write;
#[path = "exec/view_write_helpers.rs"]
mod view_write_helpers;
#[path = "exec/virtual_catalog.rs"]
mod virtual_catalog;
#[path = "exec/virtual_catalog_relation.rs"]
mod virtual_catalog_relation;
#[path = "exec/write_constraints.rs"]
mod write_constraints;
#[path = "exec/write_ctes.rs"]
mod write_ctes;
#[path = "exec/write_dispatch.rs"]
mod write_dispatch;
#[path = "exec/write_rows.rs"]
mod write_rows;
#[path = "exec/write_telemetry.rs"]
mod write_telemetry;
#[path = "exec/write_tree.rs"]
mod write_tree;

mod exports;
pub(crate) use exports::*;

mod shared;
use shared::*;
pub(crate) use shared::{
    DEFAULT_DATABASE, INFORMATION_SCHEMA_NAMESPACE_OID, PG_CATALOG_NAMESPACE_OID,
    PUBLIC_NAMESPACE_OID, column_mapping, permuted_row, read_seq_kv,
};

#[cfg(test)]
#[path = "exec/tests.rs"]
mod tests;
