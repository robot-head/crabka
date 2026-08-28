//! The SQL surface of `PostgreSQL`'s `xml` type.
//!
//! Two of these are ordinary `pg_proc` functions (`xmlcomment`, `xmltext`) and
//! five are grammar the parser lowers onto function calls of the same name
//! (`XMLPARSE`, `XMLSERIALIZE`, `XMLCONCAT`, `XMLELEMENT`, `XMLFOREST`), the way `EXTRACT` and `OVERLAY`
//! are already lowered. `IS [NOT] DOCUMENT` stays a unary operator and is
//! applied in [`crate::eval`]; only its predicate is here.
//!
//! `xml` has no operators at all in `PostgreSQL` — no `=`, no `<`, no btree
//! opclass — so there is no operator half of this module. Every comparison
//! reaches [`crabka_pgtypes::ops::compare`], which refuses it by name.

use base64::Engine as _;
use crabka_pgparser::ast::{Expr, FuncArgs, FuncCall};
use crabka_pgtypes::{
    ArrayValue, ColumnType, Datum, ElemType,
    xml::{self, XmlOption},
};

use crate::{
    clock::EvalCtx,
    error::ExecError,
    eval::{infer_type, is_unknown_literal},
    func::{check_scalar_modifiers, checked_args, require_arity, type_error, undefined_function},
    scope::Scope,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum XmlFunc {
    /// `xmlcomment(text) → xml`.
    Comment,
    /// `xmltext(text) → xml`.
    Text,
    /// `XMLCONCAT(xml, …)`, lowered by the parser. Variadic in the grammar, so
    /// unlike `xmlconcat2` it has no fixed arity.
    Concat,
    /// `XMLELEMENT(NAME identifier [, value [, …]])`, lowered by the parser.
    Element,
    /// `XMLFOREST(value [AS name] [, …])`, lowered by the parser.
    Forest,
    /// `XMLPI(NAME target [, value])`, lowered by the parser.
    Pi,
    /// `XMLROOT(xml, VERSION … [, STANDALONE …])`, lowered by the parser.
    Root,
    /// `XMLPARSE(DOCUMENT|CONTENT value)`, lowered to `xmlparse(mode, value)`.
    Parse,
    /// `XMLSERIALIZE(DOCUMENT|CONTENT value AS ty [INDENT])`, lowered to
    /// `xmlserialize(mode, value, indent)` wrapped in a cast to `ty`.
    Serialize,
    /// `xml_is_well_formed_document(text) → boolean`.
    WellFormedDocument,
    /// `xml_is_well_formed_content(text) → boolean`.
    WellFormedContent,
    /// `xml_is_well_formed(text) → boolean`, under `xmloption`.
    WellFormed,
    /// `xpath(text, xml) → xml[]`.
    XPath,
    /// `xpath_exists(text, xml) → boolean`.
    XPathExists,
    /// `table_to_xml(regclass, nulls, tableforest, targetns) → xml`.
    TableToXml,
    /// `query_to_xml(text, nulls, tableforest, targetns) → xml`.
    QueryToXml,
    /// `cursor_to_xml(refcursor, count, nulls, tableforest, targetns) → xml`.
    CursorToXml,
    TableToXmlSchema,
    TableToXmlAndXmlSchema,
    QueryToXmlSchema,
    QueryToXmlAndXmlSchema,
    CursorToXmlSchema,
    SchemaToXml,
    SchemaToXmlSchema,
    SchemaToXmlAndXmlSchema,
}

fn xml_func(name: &str) -> Option<XmlFunc> {
    Some(match name {
        "xmlcomment" => XmlFunc::Comment,
        "xmltext" => XmlFunc::Text,
        "xmlconcat" => XmlFunc::Concat,
        "xmlelement" => XmlFunc::Element,
        "xmlforest" => XmlFunc::Forest,
        "xmlpi" => XmlFunc::Pi,
        "xmlroot" => XmlFunc::Root,
        "xmlparse" => XmlFunc::Parse,
        "xmlserialize" => XmlFunc::Serialize,
        "xml_is_well_formed_document" => XmlFunc::WellFormedDocument,
        "xml_is_well_formed_content" => XmlFunc::WellFormedContent,
        "xml_is_well_formed" => XmlFunc::WellFormed,
        "xpath" => XmlFunc::XPath,
        "xpath_exists" => XmlFunc::XPathExists,
        "xmlexists" => XmlFunc::XPathExists,
        "table_to_xml" => XmlFunc::TableToXml,
        "query_to_xml" => XmlFunc::QueryToXml,
        "cursor_to_xml" => XmlFunc::CursorToXml,
        "table_to_xmlschema" => XmlFunc::TableToXmlSchema,
        "table_to_xml_and_xmlschema" => XmlFunc::TableToXmlAndXmlSchema,
        "query_to_xmlschema" => XmlFunc::QueryToXmlSchema,
        "query_to_xml_and_xmlschema" => XmlFunc::QueryToXmlAndXmlSchema,
        "cursor_to_xmlschema" => XmlFunc::CursorToXmlSchema,
        "schema_to_xml" => XmlFunc::SchemaToXml,
        "schema_to_xmlschema" => XmlFunc::SchemaToXmlSchema,
        "schema_to_xml_and_xmlschema" => XmlFunc::SchemaToXmlAndXmlSchema,
        _ => return None,
    })
}

pub(crate) fn is_xml_func(name: &str) -> bool {
    xml_func(name).is_some()
}

pub(crate) fn xml_func_result_type(fc: &FuncCall, scope: &Scope) -> Result<ColumnType, ExecError> {
    let function = xml_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    if function == XmlFunc::Element {
        let (values, attributes) = element_args(fc)?;
        require_arity(fc, !values.is_empty())?;
        for value in values
            .iter()
            .chain(attributes.iter().map(|(_, value)| value))
        {
            let _ = infer_type(value, scope)?;
        }
        return Ok(ColumnType::Xml);
    }
    if function == XmlFunc::Forest {
        let values = forest_args(fc)?;
        for (_, value) in values {
            let _ = infer_type(value, scope)?;
        }
        return Ok(ColumnType::Xml);
    }
    let args = checked_args(fc)?;
    match function {
        // `xmlcomment` and `xmltext` are declared over `text` and nothing else:
        // PostgreSQL does not implicitly widen an integer to text, so
        // `xmlcomment(1)` is a missing function rather than a bad argument.
        XmlFunc::Comment | XmlFunc::Text => {
            require_arity(fc, args.len() == 1)?;
            let ty = infer_type(&args[0], scope)?;
            if !ty.is_string() && !is_unknown_literal(&args[0]) {
                return Err(undefined_function_with_arg(&fc.name, ty));
            }
            Ok(ColumnType::Xml)
        }
        XmlFunc::Concat => {
            for arg in args {
                check_xml_argument(arg, scope, "XMLCONCAT")?;
            }
            Ok(ColumnType::Xml)
        }
        XmlFunc::Element | XmlFunc::Forest => unreachable!("handled above"),
        XmlFunc::Pi => {
            require_arity(fc, args.len() == 1 || args.len() == 2)?;
            Ok(ColumnType::Xml)
        }
        XmlFunc::Root => {
            require_arity(fc, args.len() == 3)?;
            check_xml_argument(&args[0], scope, "XMLROOT")?;
            Ok(ColumnType::Xml)
        }
        // The grammar coerces `XMLPARSE`'s value to `text`, so
        // `xmlparse(content 1)` parses the string `1` rather than failing to
        // resolve. Nothing about the argument's type is checked here.
        XmlFunc::Parse => {
            require_arity(fc, args.len() == 2)?;
            Ok(ColumnType::Xml)
        }
        // The cast the parser wrapped around the call decides the reported
        // type; the call itself is always `text`.
        XmlFunc::Serialize => {
            require_arity(fc, args.len() == 3)?;
            check_xml_argument(&args[1], scope, "XMLSERIALIZE")?;
            Ok(ColumnType::Text)
        }
        XmlFunc::WellFormedDocument | XmlFunc::WellFormedContent | XmlFunc::WellFormed => {
            require_arity(fc, args.len() == 1)?;
            let ty = infer_type(&args[0], scope)?;
            if !ty.is_string() && !is_unknown_literal(&args[0]) {
                return Err(undefined_function_with_arg(&fc.name, ty));
            }
            Ok(ColumnType::Bool)
        }
        XmlFunc::XPath | XmlFunc::XPathExists => {
            require_arity(fc, args.len() == 2 || args.len() == 3)?;
            let ty = infer_type(&args[0], scope)?;
            if !ty.is_string() && !is_unknown_literal(&args[0]) {
                return Err(undefined_function_with_arg(&fc.name, ty));
            }
            check_xml_argument(&args[1], scope, "xpath")?;
            if args.len() == 3 {
                let ty = infer_type(&args[2], scope)?;
                if ty != ColumnType::Array(ElemType::Text) && !is_unknown_literal(&args[2]) {
                    return Err(undefined_function_with_arg(&fc.name, ty));
                }
            }
            Ok(if function == XmlFunc::XPath {
                ColumnType::Array(ElemType::Xml)
            } else {
                ColumnType::Bool
            })
        }
        XmlFunc::TableToXml
        | XmlFunc::QueryToXml
        | XmlFunc::TableToXmlSchema
        | XmlFunc::TableToXmlAndXmlSchema
        | XmlFunc::QueryToXmlSchema
        | XmlFunc::QueryToXmlAndXmlSchema
        | XmlFunc::SchemaToXml
        | XmlFunc::SchemaToXmlSchema
        | XmlFunc::SchemaToXmlAndXmlSchema => {
            require_arity(fc, args.len() == 4)?;
            Ok(ColumnType::Xml)
        }
        XmlFunc::CursorToXml => {
            require_arity(fc, args.len() == 5)?;
            Ok(ColumnType::Xml)
        }
        XmlFunc::CursorToXmlSchema => {
            require_arity(fc, args.len() == 4)?;
            Ok(ColumnType::Xml)
        }
    }
}

/// `function xmlcomment(integer) does not exist`, with the hint `PostgreSQL`
/// attaches to every unresolved call.
fn undefined_function_with_arg(name: &str, ty: ColumnType) -> ExecError {
    ExecError::UndefinedFunction(format!("function {name}({}) does not exist", ty.name()))
}

/// `XMLCONCAT` and `XMLSERIALIZE` take `xml` and refuse to coerce anything but
/// an untyped literal to it, with a message naming the construct.
fn check_xml_argument(arg: &Expr, scope: &Scope, construct: &str) -> Result<(), ExecError> {
    if is_unknown_literal(arg) {
        return Ok(());
    }
    let ty = infer_type(arg, scope)?;
    if ty.storage_type() == ColumnType::Xml {
        return Ok(());
    }
    Err(ExecError::TypeMismatch(format!(
        "argument of {construct} must be type xml, not type {}",
        ty.name()
    )))
}

pub(crate) fn eval_xml(
    fc: &FuncCall,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let function = xml_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    if function == XmlFunc::Element {
        let (args, attributes) = element_args(fc)?;
        let values = args
            .iter()
            .map(&mut eval_child)
            .collect::<Result<Vec<_>, _>>()?;
        let attributes = attributes
            .iter()
            .map(|(name, value)| Ok((name.as_str(), eval_child(value)?)))
            .collect::<Result<Vec<_>, ExecError>>()?;
        return element(&values, &attributes, fc, ctx);
    }
    if function == XmlFunc::Forest {
        let values = forest_args(fc)?
            .iter()
            .map(|(name, value)| Ok((name.as_str(), eval_child(value)?)))
            .collect::<Result<Vec<_>, ExecError>>()?;
        return forest(&values, ctx);
    }
    let args = checked_args(fc)?;
    let values = args
        .iter()
        .map(&mut eval_child)
        .collect::<Result<Vec<_>, _>>()?;
    match function {
        XmlFunc::Comment => {
            require_arity(fc, values.len() == 1)?;
            strict(&values[0], |text| xml::comment(text).map(Datum::Xml))
        }
        XmlFunc::Text => {
            require_arity(fc, values.len() == 1)?;
            strict(&values[0], |text| Ok(Datum::Xml(xml::text_node(text))))
        }
        // `XMLCONCAT` is strict only in the aggregate: a NULL argument is
        // skipped, and the result is NULL only when every argument was.
        XmlFunc::Concat => {
            let parts = values
                .iter()
                .filter(|value| !value.is_null())
                .map(|value| as_xml(value, "XMLCONCAT"))
                .collect::<Result<Vec<_>, _>>()?;
            if parts.is_empty() {
                return Ok(Datum::Null);
            }
            let borrowed: Vec<&str> = parts.iter().map(String::as_str).collect();
            Ok(Datum::Xml(xml::concat(&borrowed)))
        }
        XmlFunc::Element | XmlFunc::Forest => unreachable!("handled above"),
        XmlFunc::Pi => pi(&values, fc),
        XmlFunc::Root => root(&values, fc),
        XmlFunc::Parse => {
            require_arity(fc, values.len() == 2)?;
            let option = mode_of(&values[0])?;
            if values[1].is_null() {
                return Ok(Datum::Null);
            }
            let text = text_of(&values[1], ctx);
            xml::validate(&text, option)
                .map(|()| Datum::Xml(text))
                .map_err(ExecError::from)
        }
        XmlFunc::Serialize => {
            require_arity(fc, values.len() == 3)?;
            let option = mode_of(&values[0])?;
            let indent = matches!(values[2], Datum::Bool(true));
            if values[1].is_null() {
                return Ok(Datum::Null);
            }
            let text = as_xml(&values[1], "XMLSERIALIZE")?;
            if indent {
                return xml::serialize_indent(&text, option)
                    .map(Datum::Text)
                    .map_err(ExecError::from);
            }
            // Without INDENT the value is returned unchanged, and CONTENT does
            // not even parse it -- which is why `XMLSERIALIZE(CONTENT …)` works
            // on a PostgreSQL built without libxml while DOCUMENT does not.
            if option == XmlOption::Document {
                xml::require_document(&text)?;
            }
            Ok(Datum::Text(text))
        }
        XmlFunc::WellFormedDocument => well_formed(&values, XmlOption::Document, fc),
        XmlFunc::WellFormedContent => well_formed(&values, XmlOption::Content, fc),
        XmlFunc::WellFormed => well_formed(&values, ctx.xml_option, fc),
        XmlFunc::XPath | XmlFunc::XPathExists => xpath(&values, fc, function),
        XmlFunc::TableToXml => table_to_xml(&values, fc),
        XmlFunc::QueryToXml => query_to_xml(&values, fc),
        XmlFunc::CursorToXml => cursor_to_xml(&values, fc),
        XmlFunc::TableToXmlSchema => table_to_xmlschema(&values, fc, false),
        XmlFunc::TableToXmlAndXmlSchema => table_to_xmlschema(&values, fc, true),
        XmlFunc::QueryToXmlSchema => query_to_xmlschema(&values, fc, false),
        XmlFunc::QueryToXmlAndXmlSchema => query_to_xmlschema(&values, fc, true),
        XmlFunc::CursorToXmlSchema => cursor_to_xmlschema(&values, fc),
        XmlFunc::SchemaToXml => schema_to_xml(&values, fc, false, true),
        XmlFunc::SchemaToXmlSchema => schema_to_xml(&values, fc, true, false),
        XmlFunc::SchemaToXmlAndXmlSchema => schema_to_xml(&values, fc, true, true),
    }
}

fn table_to_xml(values: &[Datum], fc: &FuncCall) -> Result<Datum, ExecError> {
    require_arity(fc, values.len() == 4)?;
    if values.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    let [relation, nulls, tableforest, target_ns] = values else {
        unreachable!("arity checked above")
    };
    let (
        Datum::Text(relation),
        Datum::Bool(nulls),
        Datum::Bool(tableforest),
        Datum::Text(target_ns),
    ) = (relation, nulls, tableforest, target_ns)
    else {
        return Err(undefined_function(&fc.name));
    };
    crate::routine::request_table_xml(crate::xmlmap::TableXmlRequest {
        relation: relation.clone(),
        nulls: *nulls,
        tableforest: *tableforest,
        target_ns: target_ns.clone(),
    })
}

fn query_to_xml(values: &[Datum], fc: &FuncCall) -> Result<Datum, ExecError> {
    require_arity(fc, values.len() == 4)?;
    if values.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    let [query, nulls, tableforest, target_ns] = values else {
        unreachable!("arity checked above")
    };
    let (Datum::Text(query), Datum::Bool(nulls), Datum::Bool(tableforest), Datum::Text(target_ns)) =
        (query, nulls, tableforest, target_ns)
    else {
        return Err(undefined_function(&fc.name));
    };
    crate::routine::request_query_xml(crate::xmlmap::QueryXmlRequest {
        query: query.clone(),
        nulls: *nulls,
        tableforest: *tableforest,
        target_ns: target_ns.clone(),
    })
}

fn cursor_to_xml(values: &[Datum], fc: &FuncCall) -> Result<Datum, ExecError> {
    require_arity(fc, values.len() == 5)?;
    if values.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    let [cursor, count, nulls, tableforest, target_ns] = values else {
        unreachable!("arity checked above")
    };
    let (
        Datum::Text(cursor),
        Datum::Int4(count),
        Datum::Bool(nulls),
        Datum::Bool(tableforest),
        Datum::Text(target_ns),
    ) = (cursor, count, nulls, tableforest, target_ns)
    else {
        return Err(undefined_function(&fc.name));
    };
    crate::routine::request_cursor_xml(crate::xmlmap::CursorXmlRequest {
        cursor: cursor.clone(),
        count: i64::from(*count),
        nulls: *nulls,
        tableforest: *tableforest,
        target_ns: target_ns.clone(),
    })
}

fn table_to_xmlschema(
    values: &[Datum],
    fc: &FuncCall,
    include_data: bool,
) -> Result<Datum, ExecError> {
    require_arity(fc, values.len() == 4)?;
    if values.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    let [relation, nulls, tableforest, target_ns] = values else {
        unreachable!("arity checked above")
    };
    let (
        Datum::Text(relation),
        Datum::Bool(nulls),
        Datum::Bool(tableforest),
        Datum::Text(target_ns),
    ) = (relation, nulls, tableforest, target_ns)
    else {
        return Err(undefined_function(&fc.name));
    };
    crate::routine::request_table_xmlschema(crate::xmlmap::TableXmlSchemaRequest {
        relation: relation.clone(),
        nulls: *nulls,
        tableforest: *tableforest,
        target_ns: target_ns.clone(),
        include_data,
    })
}

fn query_to_xmlschema(
    values: &[Datum],
    fc: &FuncCall,
    include_data: bool,
) -> Result<Datum, ExecError> {
    require_arity(fc, values.len() == 4)?;
    if values.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    let [query, nulls, tableforest, target_ns] = values else {
        unreachable!("arity checked above")
    };
    let (Datum::Text(query), Datum::Bool(nulls), Datum::Bool(tableforest), Datum::Text(target_ns)) =
        (query, nulls, tableforest, target_ns)
    else {
        return Err(undefined_function(&fc.name));
    };
    crate::routine::request_query_xmlschema(crate::xmlmap::QueryXmlSchemaRequest {
        query: query.clone(),
        nulls: *nulls,
        tableforest: *tableforest,
        target_ns: target_ns.clone(),
        include_data,
    })
}

fn cursor_to_xmlschema(values: &[Datum], fc: &FuncCall) -> Result<Datum, ExecError> {
    require_arity(fc, values.len() == 4)?;
    if values.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    let [cursor, nulls, tableforest, target_ns] = values else {
        unreachable!("arity checked above")
    };
    let (Datum::Text(cursor), Datum::Bool(nulls), Datum::Bool(tableforest), Datum::Text(target_ns)) =
        (cursor, nulls, tableforest, target_ns)
    else {
        return Err(undefined_function(&fc.name));
    };
    crate::routine::request_cursor_xmlschema(crate::xmlmap::CursorXmlSchemaRequest {
        cursor: cursor.clone(),
        nulls: *nulls,
        tableforest: *tableforest,
        target_ns: target_ns.clone(),
    })
}

fn schema_to_xml(
    values: &[Datum],
    fc: &FuncCall,
    include_schema: bool,
    include_data: bool,
) -> Result<Datum, ExecError> {
    require_arity(fc, values.len() == 4)?;
    if values.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    let [schema, nulls, tableforest, target_ns] = values else {
        unreachable!("arity checked above")
    };
    let (Datum::Text(schema), Datum::Bool(nulls), Datum::Bool(tableforest), Datum::Text(target_ns)) =
        (schema, nulls, tableforest, target_ns)
    else {
        return Err(undefined_function(&fc.name));
    };
    crate::routine::request_schema_xml(crate::xmlmap::SchemaXmlRequest {
        schema: schema.clone(),
        nulls: *nulls,
        tableforest: *tableforest,
        target_ns: target_ns.clone(),
        include_schema,
        include_data,
    })
}

fn element(
    values: &[Datum],
    attributes: &[(&str, Datum)],
    fc: &FuncCall,
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    require_arity(fc, !values.is_empty())?;
    let [Datum::Text(name), content @ ..] = values else {
        return Err(type_error("text", &values[0]));
    };
    let name = xml::sql_identifier_to_xml_name(name, false, false);
    let mut attrs = String::new();
    for (name, value) in attributes {
        if !value.is_null() {
            attrs.push(' ');
            attrs.push_str(&xml::sql_identifier_to_xml_name(name, false, false));
            attrs.push_str("=\"");
            attrs.push_str(&xml::text_node(&xml_text_of(value, ctx)?));
            attrs.push('"');
        }
    }
    let mut body = String::new();
    for value in content {
        append_element_content(&mut body, value, ctx)?;
    }
    Ok(Datum::Xml(if body.is_empty() {
        format!("<{name}{attrs}/>")
    } else {
        format!("<{name}{attrs}>{body}</{name}>")
    }))
}

fn forest(values: &[(&str, Datum)], ctx: &EvalCtx) -> Result<Datum, ExecError> {
    let mut body = String::new();
    for (name, value) in values {
        if !value.is_null() {
            let name = xml::sql_identifier_to_xml_name(name, false, false);
            body.push('<');
            body.push_str(&name);
            body.push('>');
            append_element_content(&mut body, value, ctx)?;
            body.push_str("</");
            body.push_str(&name);
            body.push('>');
        }
    }
    Ok(Datum::Xml(body))
}

fn pi(values: &[Datum], fc: &FuncCall) -> Result<Datum, ExecError> {
    require_arity(fc, values.len() == 1 || values.len() == 2)?;
    let [Datum::Text(target), rest @ ..] = values else {
        return Err(type_error("text", &values[0]));
    };
    if target.eq_ignore_ascii_case("xml") {
        return Err(ExecError::Type(crabka_pgtypes::TypeError::Coded {
            sqlstate: "2200S",
            message: "invalid XML processing instruction".into(),
        }));
    }
    let target = xml::sql_identifier_to_xml_name(target, false, false);
    let value = match rest {
        [] => None,
        [Datum::Null] => return Ok(Datum::Null),
        [Datum::Text(value)] => Some(value.as_str()),
        [value] => return Err(type_error("text", value)),
        _ => unreachable!("arity checked above"),
    };
    if value.is_some_and(|value| value.contains("?>")) {
        return Err(ExecError::Type(crabka_pgtypes::TypeError::Coded {
            sqlstate: "2200S",
            message: "invalid XML processing instruction".into(),
        }));
    }
    Ok(Datum::Xml(match value {
        Some(value) => format!("<?{target} {}?>", value.trim_start_matches(' ')),
        None => format!("<?{target}?>"),
    }))
}

fn root(values: &[Datum], fc: &FuncCall) -> Result<Datum, ExecError> {
    require_arity(fc, values.len() == 3)?;
    let document = as_xml(&values[0], "XMLROOT")?;
    let version = match &values[1] {
        Datum::Null => None,
        Datum::Text(value) => Some(value.as_str()),
        value => return Err(type_error("text", value)),
    };
    let standalone = match &values[2] {
        Datum::Text(value) if value == "omitted" => xml::XmlStandalone::Omitted,
        Datum::Text(value) if value == "no_value" => xml::XmlStandalone::NoValue,
        Datum::Text(value) if value == "yes" => xml::XmlStandalone::Yes,
        Datum::Text(value) if value == "no" => xml::XmlStandalone::No,
        value => return Err(type_error("text", value)),
    };
    Ok(Datum::Xml(xml::root(&document, version, standalone)))
}

fn append_element_content(out: &mut String, value: &Datum, ctx: &EvalCtx) -> Result<(), ExecError> {
    match value {
        Datum::Null => {}
        Datum::Xml(text) => out.push_str(text),
        Datum::Array(array) => {
            for value in &array.elems {
                out.push_str("<element>");
                append_element_content(out, value, ctx)?;
                out.push_str("</element>");
            }
        }
        value => out.push_str(&xml::text_node(&xml_text_of(value, ctx)?)),
    }
    Ok(())
}

pub(crate) fn xml_text_of(value: &Datum, ctx: &EvalCtx) -> Result<String, ExecError> {
    match value {
        Datum::Bool(value) => Ok(if *value { "true" } else { "false" }.into()),
        Datum::Bytea(value) => Ok(match ctx.xml_binary {
            crate::clock::XmlBinary::Base64 => {
                base64::engine::general_purpose::STANDARD.encode(value)
            }
            crate::clock::XmlBinary::Hex => {
                value.iter().map(|byte| format!("{byte:02x}")).collect()
            }
        }),
        Datum::Timestamp(value) => {
            if crabka_pgtypes::datetime::timestamp_is_infinite(*value) {
                return Err(ExecError::Type(
                    crabka_pgtypes::TypeError::DatetimeOutOfRange {
                        message: "timestamp out of range".into(),
                    },
                ));
            }
            Ok(crabka_pgtypes::datetime::timestamp_to_text_in(
                *value,
                crabka_pgtypes::datetime::DateStyle::Iso,
                ctx.date_order,
            )
            .replacen(' ', "T", 1))
        }
        Datum::Timestamptz(value) => {
            if crabka_pgtypes::datetime::timestamptz_is_infinite(*value) {
                return Err(ExecError::Type(
                    crabka_pgtypes::TypeError::DatetimeOutOfRange {
                        message: "timestamp out of range".into(),
                    },
                ));
            }
            let mut text = crabka_pgtypes::datetime::timestamptz_to_text_in(
                *value,
                &ctx.time_zone,
                crabka_pgtypes::datetime::DateStyle::Iso,
                ctx.date_order,
            );
            let end = text.strip_suffix(" BC").map_or(text.len(), str::len);
            if let Some(offset) = text.rfind(['+', '-'])
                && end == offset + 3
            {
                text.insert_str(end, ":00");
            }
            Ok(text.replacen(' ', "T", 1))
        }
        Datum::Date(value) => {
            if crabka_pgtypes::datetime::date_is_infinite(*value) {
                return Err(ExecError::Type(
                    crabka_pgtypes::TypeError::DatetimeOutOfRange {
                        message: "date out of range".into(),
                    },
                ));
            }
            Ok(crabka_pgtypes::datetime::date_to_text(*value))
        }
        value => Ok(text_of(value, ctx)),
    }
}

fn element_args(fc: &FuncCall) -> Result<(&[Expr], &[(String, Expr)]), ExecError> {
    check_scalar_modifiers(fc)?;
    match &fc.args {
        FuncArgs::Exprs(values) => Ok((values, &[])),
        FuncArgs::Named { positional, named } => Ok((positional, named)),
        FuncArgs::Star | FuncArgs::Variadic { .. } => Err(undefined_function(&fc.name)),
    }
}

fn forest_args(fc: &FuncCall) -> Result<&[(String, Expr)], ExecError> {
    check_scalar_modifiers(fc)?;
    match &fc.args {
        FuncArgs::Named { positional, named } if positional.is_empty() => Ok(named),
        _ => Err(undefined_function(&fc.name)),
    }
}

fn xpath(values: &[Datum], fc: &FuncCall, function: XmlFunc) -> Result<Datum, ExecError> {
    require_arity(fc, values.len() == 2 || values.len() == 3)?;
    if values.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    let [Datum::Text(expression), document, namespaces @ ..] = values else {
        return Err(type_error("text", &values[0]));
    };
    let document = as_xml(document, "xpath")?;
    let namespaces = namespaces.first().map(xpath_namespaces).transpose()?;
    let items = match namespaces {
        Some(namespaces) => {
            let bindings = namespaces
                .iter()
                .map(|(prefix, uri)| (prefix.as_str(), uri.as_str()))
                .collect::<Vec<_>>();
            xml::xpath_with_namespaces(&document, expression, &bindings)
        }
        None => xml::xpath(&document, expression),
    }
    .map_err(ExecError::from)?;
    if function == XmlFunc::XPathExists {
        return Ok(Datum::Bool(!items.is_empty()));
    }
    Ok(Datum::Array(ArrayValue::new(
        ElemType::Xml,
        items.into_iter().map(Datum::Xml).collect(),
    )))
}

fn xpath_namespaces(value: &Datum) -> Result<Vec<(String, String)>, ExecError> {
    let Datum::Array(array) = value else {
        return Err(type_error("text[]", value));
    };
    if array.elem != ElemType::Text
        || array.dims.len() != 2
        || array.dims[1].len != 2
        || array.elems.len() % 2 != 0
    {
        return Err(ExecError::TypeMismatch(
            "XPath namespace mappings must be a two-dimensional text array".into(),
        ));
    }
    array
        .elems
        .chunks_exact(2)
        .map(|pair| match pair {
            [Datum::Text(prefix), Datum::Text(uri)] => Ok((prefix.clone(), uri.clone())),
            _ => Err(ExecError::TypeMismatch(
                "XPath namespace mappings must not contain nulls".into(),
            )),
        })
        .collect()
}

/// The `xml_is_well_formed_*` predicates suppress parse errors into false.
fn well_formed(values: &[Datum], option: XmlOption, fc: &FuncCall) -> Result<Datum, ExecError> {
    require_arity(fc, values.len() == 1)?;
    match &values[0] {
        Datum::Null => Ok(Datum::Null),
        Datum::Text(text) => Ok(Datum::Bool(xml::validate(text, option).is_ok())),
        other => Err(ExecError::TypeMismatch(format!(
            "argument must be type text, not type {}",
            other.column_type().map_or("unknown", ColumnType::name)
        ))),
    }
}

/// The `text` a value coerces to when the grammar wraps it in a cast, which is
/// what `XMLPARSE` does to its second argument.
pub(crate) fn text_of(value: &Datum, ctx: &EvalCtx) -> String {
    match value {
        Datum::Text(text) | Datum::Xml(text) => text.clone(),
        other => String::from_utf8_lossy(&crabka_pgtypes::encoding::encode_text_in(
            other,
            ctx.output_style(),
        ))
        .into_owned(),
    }
}

/// A strict one-argument function over the value's text.
fn strict(
    value: &Datum,
    body: impl FnOnce(&str) -> Result<Datum, crabka_pgtypes::TypeError>,
) -> Result<Datum, ExecError> {
    match value {
        Datum::Null => Ok(Datum::Null),
        Datum::Text(text) | Datum::Xml(text) => body(text).map_err(ExecError::from),
        other => Err(ExecError::TypeMismatch(format!(
            "argument must be type text, not type {}",
            other.column_type().map_or("unknown", ColumnType::name)
        ))),
    }
}

/// The `xml` text of a value that reached a construct declared over `xml`. An
/// untyped literal is coerced here, which is where its well-formedness is
/// checked.
pub(crate) fn as_xml(value: &Datum, construct: &str) -> Result<String, ExecError> {
    match value {
        Datum::Xml(text) => Ok(text.clone()),
        Datum::Text(text) => xml::validate(text, XmlOption::Content)
            .map(|()| text.clone())
            .map_err(ExecError::from),
        other => Err(ExecError::TypeMismatch(format!(
            "argument of {construct} must be type xml, not type {}",
            other.column_type().map_or("unknown", ColumnType::name)
        ))),
    }
}

/// The `DOCUMENT`/`CONTENT` word the parser lowered into a literal.
fn mode_of(value: &Datum) -> Result<XmlOption, ExecError> {
    match value {
        Datum::Text(word) if word == "document" => Ok(XmlOption::Document),
        Datum::Text(word) if word == "content" => Ok(XmlOption::Content),
        _ => Err(ExecError::Syntax(
            "XMLPARSE and XMLSERIALIZE require DOCUMENT or CONTENT".into(),
        )),
    }
}

/// `xml_is_document` — the `IS [NOT] DOCUMENT` predicate.
///
/// Total over a well-formed `xml` value: a fragment with two roots is simply
/// not a document, never an error. The predicate *can* still raise, but only
/// from coercing an untyped literal to `xml` first.
pub(crate) fn is_document(value: &Datum, negated: bool) -> Result<Datum, ExecError> {
    let text = match value {
        Datum::Null => return Ok(Datum::Null),
        Datum::Xml(text) => text.clone(),
        Datum::Text(text) => {
            xml::validate(text, XmlOption::Content)?;
            text.clone()
        }
        other => {
            return Err(ExecError::TypeMismatch(format!(
                "argument of IS {}DOCUMENT must be type xml, not type {}",
                if negated { "NOT " } else { "" },
                other.column_type().map_or("unknown", ColumnType::name)
            )));
        }
    };
    Ok(Datum::Bool(xml::is_document(&text) ^ negated))
}

/// The static half of the same check, so `SELECT 1 IS DOCUMENT` is rejected
/// while resolving the projection rather than while evaluating it.
pub(crate) fn check_is_document_operand(
    expr: &Expr,
    scope: &Scope,
    negated: bool,
) -> Result<(), ExecError> {
    if is_unknown_literal(expr) {
        return Ok(());
    }
    let ty = infer_type(expr, scope)?;
    if ty.storage_type() == ColumnType::Xml {
        return Ok(());
    }
    Err(ExecError::TypeMismatch(format!(
        "argument of IS {}DOCUMENT must be type xml, not type {}",
        if negated { "NOT " } else { "" },
        ty.name()
    )))
}
