//! The SQL surface of `PostgreSQL`'s `xml` type.
//!
//! Two of these are ordinary `pg_proc` functions (`xmlcomment`, `xmltext`) and
//! three are grammar the parser lowers onto function calls of the same name
//! (`XMLPARSE`, `XMLSERIALIZE`, `XMLCONCAT`), the way `EXTRACT` and `OVERLAY`
//! are already lowered. `IS [NOT] DOCUMENT` stays a unary operator and is
//! applied in [`crate::eval`]; only its predicate is here.
//!
//! `xml` has no operators at all in `PostgreSQL` — no `=`, no `<`, no btree
//! opclass — so there is no operator half of this module. Every comparison
//! reaches [`crabka_pgtypes::ops::compare`], which refuses it by name.

use crabka_pgparser::ast::{Expr, FuncCall};
use crabka_pgtypes::{
    ColumnType, Datum,
    xml::{self, XmlOption},
};

use crate::{
    clock::EvalCtx,
    error::ExecError,
    eval::{infer_type, is_unknown_literal},
    func::{checked_args, require_arity, undefined_function},
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
    /// `XMLPARSE(DOCUMENT|CONTENT value)`, lowered to `xmlparse(mode, value)`.
    Parse,
    /// `XMLSERIALIZE(DOCUMENT|CONTENT value AS ty [INDENT])`, lowered to
    /// `xmlserialize(mode, value, indent)` wrapped in a cast to `ty`.
    Serialize,
}

fn xml_func(name: &str) -> Option<XmlFunc> {
    Some(match name {
        "xmlcomment" => XmlFunc::Comment,
        "xmltext" => XmlFunc::Text,
        "xmlconcat" => XmlFunc::Concat,
        "xmlparse" => XmlFunc::Parse,
        "xmlserialize" => XmlFunc::Serialize,
        _ => return None,
    })
}

pub(crate) fn is_xml_func(name: &str) -> bool {
    xml_func(name).is_some()
}

pub(crate) fn xml_func_result_type(fc: &FuncCall, scope: &Scope) -> Result<ColumnType, ExecError> {
    let function = xml_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
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
    }
}

/// The `text` a value coerces to when the grammar wraps it in a cast, which is
/// what `XMLPARSE` does to its second argument.
fn text_of(value: &Datum, ctx: &EvalCtx) -> String {
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
fn as_xml(value: &Datum, construct: &str) -> Result<String, ExecError> {
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
