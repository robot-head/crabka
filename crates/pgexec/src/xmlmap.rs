//! Relational SQL/XML value producers.
//!
//! These functions need a session-backed scan, unlike the scalar XML helpers
//! in [`crate::xml_fn`].  The renderer stays pure so the session owns relation
//! resolution and query execution while this module owns PostgreSQL's XML
//! shape.

use crabka_pgcatalog::Table;
use crabka_pgtypes::{ColumnType, TemporalType};
use crabka_pgwire::engine::{Cell, FieldDescription};

/// One `table_to_xml` call after scalar argument evaluation.
#[derive(Debug, Clone)]
pub(crate) struct TableXmlRequest {
    pub relation: String,
    pub nulls: bool,
    pub tableforest: bool,
    pub target_ns: String,
}

/// One `query_to_xml` call after scalar argument evaluation.
#[derive(Debug, Clone)]
pub(crate) struct QueryXmlRequest {
    pub query: String,
    pub nulls: bool,
    pub tableforest: bool,
    pub target_ns: String,
}

/// One `cursor_to_xml` call after scalar argument evaluation.
#[derive(Debug, Clone)]
pub(crate) struct CursorXmlRequest {
    pub cursor: String,
    pub count: i64,
    pub nulls: bool,
    pub tableforest: bool,
    pub target_ns: String,
}

/// One table XML schema request after scalar argument evaluation.
#[derive(Debug, Clone)]
pub(crate) struct TableXmlSchemaRequest {
    pub relation: String,
    pub nulls: bool,
    pub tableforest: bool,
    pub target_ns: String,
    pub include_data: bool,
}

/// One query XML schema request after scalar argument evaluation.
#[derive(Debug, Clone)]
pub(crate) struct QueryXmlSchemaRequest {
    pub query: String,
    pub nulls: bool,
    pub tableforest: bool,
    pub target_ns: String,
    pub include_data: bool,
}

/// One cursor XML schema request after scalar argument evaluation.
#[derive(Debug, Clone)]
pub(crate) struct CursorXmlSchemaRequest {
    pub cursor: String,
    pub nulls: bool,
    pub tableforest: bool,
    pub target_ns: String,
}

/// One schema XML producer call after scalar argument evaluation.
#[derive(Debug, Clone)]
pub(crate) struct SchemaXmlRequest {
    pub schema: String,
    pub nulls: bool,
    pub tableforest: bool,
    pub target_ns: String,
    pub include_schema: bool,
    pub include_data: bool,
}

/// Render `table_to_xml` from a table definition and its textual result rows.
pub(crate) fn table_to_xml(
    table: &Table,
    rows: &[Vec<Option<Cell>>],
    request: &TableXmlRequest,
) -> String {
    let columns = table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    rows_to_xml(
        &table.name.name,
        &table.name.name,
        &columns,
        rows,
        request.nulls,
        request.tableforest,
        &request.target_ns,
        false,
    )
}

/// Render `query_to_xml` from the fields and rows its query returned.
pub(crate) fn query_to_xml(
    fields: &[FieldDescription],
    rows: &[Vec<Option<Cell>>],
    request: &QueryXmlRequest,
) -> String {
    let columns = fields
        .iter()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    rows_to_xml(
        "table",
        "row",
        &columns,
        rows,
        request.nulls,
        request.tableforest,
        &request.target_ns,
        false,
    )
}

/// Render `cursor_to_xml` from the fetched cursor rows.
pub(crate) fn cursor_to_xml(
    fields: &[FieldDescription],
    rows: &[Vec<Option<Cell>>],
    request: &CursorXmlRequest,
) -> String {
    let columns = fields
        .iter()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    rows_to_xml(
        "table",
        "row",
        &columns,
        rows,
        request.nulls,
        request.tableforest,
        &request.target_ns,
        false,
    )
}

/// Render `table_to_xmlschema` and `table_to_xml_and_xmlschema`.
pub(crate) fn table_to_xmlschema(
    table: &Table,
    rows: &[Vec<Option<Cell>>],
    database: &str,
    request: &TableXmlSchemaRequest,
) -> String {
    let schema = table_schema(
        table,
        database,
        request.nulls,
        request.tableforest,
        &request.target_ns,
    );
    if !request.include_data {
        return schema;
    }
    let columns = table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let data = rows_to_xml(
        &table.name.name,
        &table.name.name,
        &columns,
        rows,
        request.nulls,
        request.tableforest,
        &request.target_ns,
        request.target_ns.is_empty() && !request.tableforest,
    );
    combine_schema_and_data(schema, data, request.tableforest)
}

/// Render query/cursor XSD and optionally embed it in their XML data result.
pub(crate) fn result_to_xmlschema(
    fields: &[FieldDescription],
    rows: &[Vec<Option<Cell>>],
    database: &str,
    nulls: bool,
    tableforest: bool,
    target_ns: &str,
    include_data: bool,
) -> Result<String, crate::error::ExecError> {
    let columns = fields
        .iter()
        .map(|field| {
            Ok((
                field.name.clone(),
                crate::exec::column_type_from_oid(field.type_oid)?,
            ))
        })
        .collect::<Result<Vec<_>, crate::error::ExecError>>()?;
    let schema = result_schema(&columns, database, nulls, tableforest, target_ns);
    if !include_data {
        return Ok(schema);
    }
    let names = columns
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    let data = rows_to_xml(
        "table",
        "row",
        &names,
        rows,
        nulls,
        tableforest,
        target_ns,
        target_ns.is_empty() && !tableforest,
    );
    Ok(combine_schema_and_data(schema, data, tableforest))
}

/// Render the `schema_to_xml` family from its tables and already-scanned rows.
pub(crate) fn schema_to_xml(
    tables: &[Table],
    rows: &[Vec<Vec<Option<Cell>>>],
    database: &str,
    request: &SchemaXmlRequest,
) -> String {
    let data = schema_data(tables, rows, request);
    if !request.include_schema {
        return data;
    }
    let schema = schema_schema(tables, database, request);
    if !request.include_data {
        return schema;
    }
    let offset = data
        .find('>')
        .expect("schema XML root always has a closing bracket")
        + 1;
    format!("{}\n\n{}{}", &data[..offset], schema, &data[offset..])
}

fn rows_to_xml(
    root: &str,
    forest_root: &str,
    columns: &[String],
    rows: &[Vec<Option<Cell>>],
    nulls: bool,
    tableforest: bool,
    target_ns: &str,
    schema_location: bool,
) -> String {
    let name = crabka_pgtypes::xml::sql_identifier_to_xml_name(root, false, false);
    let forest_name = crabka_pgtypes::xml::sql_identifier_to_xml_name(forest_root, false, false);
    let active_root = if tableforest { &forest_name } else { &name };
    let mut out = open_element(active_root, target_ns, schema_location);
    for (index, row) in rows.iter().enumerate() {
        if !tableforest {
            out.push_str("\n\n<row>");
        }
        for (column, value) in columns.iter().zip(row) {
            let Some(value) = value else {
                if nulls {
                    field(&mut out, column, None);
                }
                continue;
            };
            let text = String::from_utf8_lossy(&value.text);
            field(&mut out, column, Some(&text));
        }
        if !tableforest {
            out.push_str("\n</row>");
        } else {
            out.push('\n');
            out.push_str("</");
            out.push_str(&forest_name);
            out.push('>');
            if index + 1 < rows.len() {
                out.push_str("\n\n");
                out.push_str(&open_element(&forest_name, target_ns, schema_location));
            } else {
                out.push_str("\n\n");
            }
        }
    }
    if tableforest {
        if rows.is_empty() {
            out.push_str("\n</");
            out.push_str(&forest_name);
            out.push('>');
        }
    } else {
        out.push_str("\n\n");
        out.push_str("</");
        out.push_str(&name);
        out.push_str(">\n");
    }
    out
}

fn open_element(name: &str, target_ns: &str, schema_location: bool) -> String {
    let mut out = format!("<{name} xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"");
    if !target_ns.is_empty() {
        out.push_str(" xmlns=\"");
        out.push_str(&crabka_pgtypes::xml::text_node(target_ns));
        out.push('"');
    }
    if schema_location {
        if target_ns.is_empty() {
            out.push_str(" xsi:noNamespaceSchemaLocation=\"#\"");
        } else {
            out.push_str(" xsi:schemaLocation=\"");
            out.push_str(&crabka_pgtypes::xml::text_node(target_ns));
            out.push_str(" #\"");
        }
    }
    out.push('>');
    out
}

fn schema_data(
    tables: &[Table],
    rows: &[Vec<Vec<Option<Cell>>>],
    request: &SchemaXmlRequest,
) -> String {
    let root = crabka_pgtypes::xml::sql_identifier_to_xml_name(&request.schema, false, false);
    let mut out = open_element(&root, &request.target_ns, request.include_schema);
    let mut forest_rows = false;
    for (table, rows) in tables.iter().zip(rows) {
        let name = crabka_pgtypes::xml::sql_identifier_to_xml_name(&table.name.name, false, false);
        let columns = table
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        if request.tableforest {
            if forest_rows && !rows.is_empty() {
                out.push('\n');
            }
            for row in rows {
                out.push_str("\n\n<");
                out.push_str(&name);
                out.push('>');
                append_row_fields(&mut out, &columns, row, request.nulls);
                out.push('\n');
                out.push_str("</");
                out.push_str(&name);
                out.push('>');
            }
            forest_rows |= !rows.is_empty();
        } else {
            out.push_str("\n\n<");
            out.push_str(&name);
            out.push('>');
            for row in rows {
                out.push_str("\n\n<row>");
                append_row_fields(&mut out, &columns, row, request.nulls);
                out.push_str("\n</row>");
            }
            out.push_str("\n\n</");
            out.push_str(&name);
            out.push('>');
        }
    }
    if request.tableforest && forest_rows {
        out.push('\n');
    }
    out.push_str("\n\n</");
    out.push_str(&root);
    out.push_str(">\n");
    out
}

fn schema_schema(tables: &[Table], database: &str, request: &SchemaXmlRequest) -> String {
    let mut out = schema_open(&request.target_ns);
    let mut types = Vec::new();
    for table in tables {
        for column in &table.columns {
            let name = xsd_type_name(column.ty, database);
            if !types.contains(&name) {
                types.push(name);
                out.push_str("\n\n");
                out.push_str(&xsd_type_definition(column.ty, database));
            }
        }
    }
    let schema_type = format!("SchemaType.{database}.{}", request.schema);
    out.push_str("\n\n<xsd:complexType name=\"");
    out.push_str(&schema_type);
    out.push_str("\">\n  <xsd:");
    out.push_str(if request.tableforest {
        "sequence>"
    } else {
        "all>"
    });
    for table in tables {
        let element =
            crabka_pgtypes::xml::sql_identifier_to_xml_name(&table.name.name, false, false);
        let kind = if request.tableforest {
            "RowType"
        } else {
            "TableType"
        };
        out.push_str("\n    <xsd:element name=\"");
        out.push_str(&element);
        out.push_str("\" type=\"");
        out.push_str(kind);
        out.push('.');
        out.push_str(database);
        out.push('.');
        out.push_str(&request.schema);
        out.push('.');
        out.push_str(&table.name.name);
        out.push('"');
        if request.tableforest {
            out.push_str(" minOccurs=\"0\" maxOccurs=\"unbounded\"");
        }
        out.push_str("/>");
    }
    out.push_str("\n  </xsd:");
    out.push_str(if request.tableforest {
        "sequence>"
    } else {
        "all>"
    });
    let element = crabka_pgtypes::xml::sql_identifier_to_xml_name(&request.schema, false, false);
    out.push_str("\n</xsd:complexType>\n\n<xsd:element name=\"");
    out.push_str(&element);
    out.push_str("\" type=\"");
    out.push_str(&schema_type);
    out.push_str("\"/>\n\n</xsd:schema>");
    out
}

fn table_schema(
    table: &Table,
    database: &str,
    nulls: bool,
    tableforest: bool,
    target_ns: &str,
) -> String {
    let row_type = format!(
        "RowType.{database}.{}.{}",
        table.name.schema, table.name.name
    );
    let table_type = format!(
        "TableType.{database}.{}.{}",
        table.name.schema, table.name.name
    );
    let mut out = schema_open(target_ns);
    let mut types = Vec::new();
    for column in &table.columns {
        let name = xsd_type_name(column.ty, database);
        if !types.contains(&name) {
            types.push(name);
            out.push_str("\n\n");
            out.push_str(&xsd_type_definition(column.ty, database));
        }
    }
    out.push_str("\n\n<xsd:complexType name=\"");
    out.push_str(&row_type);
    out.push_str("\">\n  <xsd:sequence>");
    for column in &table.columns {
        out.push_str("\n    <xsd:element name=\"");
        out.push_str(&crabka_pgtypes::xml::sql_identifier_to_xml_name(
            &column.name,
            false,
            false,
        ));
        out.push_str("\" type=\"");
        out.push_str(&xsd_type_name(column.ty, database));
        out.push_str(if nulls {
            "\" nillable=\"true\""
        } else {
            "\" minOccurs=\"0\""
        });
        out.push_str("></xsd:element>");
    }
    out.push_str("\n  </xsd:sequence>\n</xsd:complexType>");
    let element = crabka_pgtypes::xml::sql_identifier_to_xml_name(&table.name.name, false, false);
    if !tableforest {
        out.push_str("\n\n<xsd:complexType name=\"");
        out.push_str(&table_type);
        out.push_str("\">\n  <xsd:sequence>\n    <xsd:element name=\"row\" type=\"");
        out.push_str(&row_type);
        out.push_str(
            "\" minOccurs=\"0\" maxOccurs=\"unbounded\"/>\n  </xsd:sequence>\n</xsd:complexType>",
        );
    }
    out.push_str("\n\n<xsd:element name=\"");
    out.push_str(&element);
    out.push_str("\" type=\"");
    out.push_str(if tableforest { &row_type } else { &table_type });
    out.push_str("\"/>");
    out.push_str("\n\n</xsd:schema>");
    out
}

fn result_schema(
    columns: &[(String, ColumnType)],
    database: &str,
    nulls: bool,
    tableforest: bool,
    target_ns: &str,
) -> String {
    let mut out = schema_open(target_ns);
    let mut types = Vec::new();
    for (_, ty) in columns {
        let name = xsd_type_name(*ty, database);
        if !types.contains(&name) {
            types.push(name);
            out.push_str("\n\n");
            out.push_str(&xsd_type_definition(*ty, database));
        }
    }
    out.push_str("\n\n<xsd:complexType name=\"RowType\">\n  <xsd:sequence>");
    for (name, ty) in columns {
        out.push_str("\n    <xsd:element name=\"");
        out.push_str(&crabka_pgtypes::xml::sql_identifier_to_xml_name(
            name, false, false,
        ));
        out.push_str("\" type=\"");
        out.push_str(&xsd_type_name(*ty, database));
        out.push_str(if nulls {
            "\" nillable=\"true\""
        } else {
            "\" minOccurs=\"0\""
        });
        out.push_str("></xsd:element>");
    }
    out.push_str("\n  </xsd:sequence>\n</xsd:complexType>");
    if !tableforest {
        out.push_str("\n\n<xsd:complexType name=\"TableType\">\n  <xsd:sequence>\n    <xsd:element name=\"row\" type=\"RowType\" minOccurs=\"0\" maxOccurs=\"unbounded\"/>\n  </xsd:sequence>\n</xsd:complexType>");
    }
    out.push_str("\n\n<xsd:element name=\"");
    out.push_str(if tableforest { "row" } else { "table" });
    out.push_str("\" type=\"");
    out.push_str(if tableforest { "RowType" } else { "TableType" });
    out.push_str("\"/>\n\n</xsd:schema>");
    out
}

fn combine_schema_and_data(schema: String, data: String, tableforest: bool) -> String {
    if tableforest {
        format!("{schema}\n\n{data}")
    } else {
        let offset = data
            .find('>')
            .expect("XML root always has a closing bracket")
            + 1;
        format!("{}\n\n{}{}", &data[..offset], schema, &data[offset..])
    }
}

fn schema_open(target_ns: &str) -> String {
    let mut out = "<xsd:schema\n    xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"".to_string();
    if !target_ns.is_empty() {
        out.push_str("\n    targetNamespace=\"");
        out.push_str(&crabka_pgtypes::xml::text_node(target_ns));
        out.push_str("\"\n    elementFormDefault=\"qualified\"");
    }
    out.push('>');
    out
}

fn xsd_type_name(ty: ColumnType, database: &str) -> String {
    match ty {
        ColumnType::Int2 => "SMALLINT".into(),
        ColumnType::Int4 => "INTEGER".into(),
        ColumnType::Int8 => "BIGINT".into(),
        ColumnType::Bool => "BOOLEAN".into(),
        ColumnType::Float4 => "REAL".into(),
        ColumnType::Numeric(_) => "NUMERIC".into(),
        ColumnType::Varchar(_) => "VARCHAR".into(),
        ColumnType::Char(_) => "CHAR".into(),
        ColumnType::Time | ColumnType::Temporal(TemporalType::Time, _) => "TIME".into(),
        ColumnType::Timetz | ColumnType::Temporal(TemporalType::Timetz, _) => "TIME_WTZ".into(),
        ColumnType::Timestamp | ColumnType::Temporal(TemporalType::Timestamp, _) => {
            "TIMESTAMP".into()
        }
        ColumnType::Timestamptz | ColumnType::Temporal(TemporalType::Timestamptz, _) => {
            "TIMESTAMP_WTZ".into()
        }
        ColumnType::Date => "DATE".into(),
        ColumnType::Xml => "XML".into(),
        ColumnType::Text | ColumnType::Bytea => format!("UDT.{database}.pg_catalog.{}", ty.name()),
        ColumnType::Domain(domain) => crabka_pgtypes::usertype::lookup_oid(domain.oid).map_or_else(
            || format!("Domain.{database}.public.{}", domain.name),
            |domain| format!("Domain.{database}.{}.{}", domain.schema, domain.name),
        ),
        _ => format!("UDT.{database}.pg_catalog.{}", ty.name()),
    }
}

fn xsd_type_definition(ty: ColumnType, database: &str) -> String {
    let name = xsd_type_name(ty, database);
    match ty {
        ColumnType::Int2 => format!("<xsd:simpleType name=\"{name}\">\n  <xsd:restriction base=\"xsd:short\">\n    <xsd:maxInclusive value=\"32767\"/>\n    <xsd:minInclusive value=\"-32768\"/>\n  </xsd:restriction>\n</xsd:simpleType>"),
        ColumnType::Int4 => format!("<xsd:simpleType name=\"{name}\">\n  <xsd:restriction base=\"xsd:int\">\n    <xsd:maxInclusive value=\"2147483647\"/>\n    <xsd:minInclusive value=\"-2147483648\"/>\n  </xsd:restriction>\n</xsd:simpleType>"),
        ColumnType::Int8 => format!("<xsd:simpleType name=\"{name}\">\n  <xsd:restriction base=\"xsd:long\">\n    <xsd:maxInclusive value=\"9223372036854775807\"/>\n    <xsd:minInclusive value=\"-9223372036854775808\"/>\n  </xsd:restriction>\n</xsd:simpleType>"),
        ColumnType::Float4 | ColumnType::Bool => format!("<xsd:simpleType name=\"{name}\">\n  <xsd:restriction base=\"xsd:{}\"></xsd:restriction>\n</xsd:simpleType>", if ty == ColumnType::Float4 { "float" } else { "boolean" }),
        ColumnType::Numeric(_) => format!("<xsd:simpleType name=\"{name}\">\n</xsd:simpleType>"),
        ColumnType::Varchar(_) | ColumnType::Char(_) | ColumnType::Text => format!("<xsd:simpleType name=\"{name}\">\n  <xsd:restriction base=\"xsd:string\">\n  </xsd:restriction>\n</xsd:simpleType>"),
        ColumnType::Bytea => format!("<xsd:simpleType name=\"{name}\">\n  <xsd:restriction base=\"xsd:base64Binary\">\n  </xsd:restriction>\n</xsd:simpleType>"),
        ColumnType::Time | ColumnType::Temporal(TemporalType::Time, _) => temporal_type(&name, "xsd:time", "\\p{Nd}{2}:\\p{Nd}{2}:\\p{Nd}{2}(.\\p{Nd}+)?"),
        ColumnType::Timetz | ColumnType::Temporal(TemporalType::Timetz, _) => temporal_type(&name, "xsd:time", "\\p{Nd}{2}:\\p{Nd}{2}:\\p{Nd}{2}(.\\p{Nd}+)?(\\+|-)\\p{Nd}{2}:\\p{Nd}{2}"),
        ColumnType::Timestamp | ColumnType::Temporal(TemporalType::Timestamp, _) => temporal_type(&name, "xsd:dateTime", "\\p{Nd}{4}-\\p{Nd}{2}-\\p{Nd}{2}T\\p{Nd}{2}:\\p{Nd}{2}:\\p{Nd}{2}(.\\p{Nd}+)?"),
        ColumnType::Timestamptz | ColumnType::Temporal(TemporalType::Timestamptz, _) => temporal_type(&name, "xsd:dateTime", "\\p{Nd}{4}-\\p{Nd}{2}-\\p{Nd}{2}T\\p{Nd}{2}:\\p{Nd}{2}:\\p{Nd}{2}(.\\p{Nd}+)?(\\+|-)\\p{Nd}{2}:\\p{Nd}{2}"),
        ColumnType::Date => temporal_type(&name, "xsd:date", "\\p{Nd}{4}-\\p{Nd}{2}-\\p{Nd}{2}"),
        ColumnType::Xml => "<xsd:complexType mixed=\"true\">\n  <xsd:sequence>\n    <xsd:any name=\"element\" minOccurs=\"0\" maxOccurs=\"unbounded\" processContents=\"skip\"/>\n  </xsd:sequence>\n</xsd:complexType>".into(),
        ColumnType::Domain(domain) => format!("<xsd:simpleType name=\"{name}\">\n  <xsd:restriction base=\"{}\"/>\n</xsd:simpleType>", xsd_type_name(*domain.base, database)),
        _ => format!("<xsd:simpleType name=\"{name}\">\n  <xsd:restriction base=\"xsd:string\">\n  </xsd:restriction>\n</xsd:simpleType>"),
    }
}

fn temporal_type(name: &str, base: &str, pattern: &str) -> String {
    format!(
        "<xsd:simpleType name=\"{name}\">\n  <xsd:restriction base=\"{base}\">\n    <xsd:pattern value=\"{pattern}\"/>\n  </xsd:restriction>\n</xsd:simpleType>"
    )
}

fn field(out: &mut String, name: &str, value: Option<&str>) {
    let name = crabka_pgtypes::xml::sql_identifier_to_xml_name(name, false, false);
    out.push_str("\n  <");
    out.push_str(&name);
    match value {
        Some(value) => {
            out.push('>');
            out.push_str(&crabka_pgtypes::xml::text_node(value));
            out.push_str("</");
            out.push_str(&name);
            out.push('>');
        }
        None => out.push_str(" xsi:nil=\"true\"/>"),
    }
}

fn append_row_fields(out: &mut String, columns: &[String], row: &[Option<Cell>], nulls: bool) {
    for (column, value) in columns.iter().zip(row) {
        let Some(value) = value else {
            if nulls {
                field(out, column, None);
            }
            continue;
        };
        let text = String::from_utf8_lossy(&value.text);
        field(out, column, Some(&text));
    }
}
