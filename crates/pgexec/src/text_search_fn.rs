//! PostgreSQL full-text-search scalar functions.

use std::collections::BTreeMap;

use crabka_pgparser::ast::{Expr, FuncArgs, FuncCall};
use crabka_pgtypes::{
    ArrayValue, ColumnType, Datum, ElemType, JsonbValue, Lexeme, Position, QueryTerm, TsQuery,
    TsVector, Weight, text_search::MAX_POSITION,
};
use rust_stemmers::{Algorithm, Stemmer};

use crate::{
    clock::EvalCtx,
    error::ExecError,
    func::{checked_args, require_arity, type_error, undefined_function},
    scope::Scope,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TextSearchFunc {
    ToTsVector,
    ToTsQuery,
    PlainToTsQuery,
    PhraseToTsQuery,
    WebsearchToTsQuery,
    Strip,
    NumNode,
    QueryTree,
    TsQueryPhrase,
    ArrayToTsVector,
    SetWeight,
    TsDelete,
    TsFilter,
    TsRank,
    TsRankCd,
    TsHeadline,
    TsLexize,
    JsonbToTsVector,
    /// `json_to_tsvector(json, jsonb)` — the `json` sibling. Genuinely a
    /// different function, not a spelling: it walks the document in *input*
    /// order, so `json_to_tsvector('{"b":"cat sat","a":"mat"}', '["all"]')` is
    /// `'b':1 'cat':3 'mat':7 'sat':4` where the `jsonb` form, walking canonical
    /// key order, is `'b':4 'cat':6 'mat':2 'sat':7`.
    JsonToTsVector,
}

type Catalog<'a> = Option<&'a dyn crabka_pgkv::Kv>;

/// The default parser's token metadata, in PostgreSQL's stable `tokid` order.
pub(crate) const DEFAULT_PARSER_TOKEN_TYPES: &[(i32, &str, &str)] = &[
    (1, "asciiword", "Word, all ASCII"),
    (2, "word", "Word, all letters"),
    (3, "numword", "Word, letters and digits"),
    (4, "email", "Email address"),
    (5, "url", "URL"),
    (6, "host", "Host"),
    (7, "sfloat", "Scientific notation"),
    (8, "version", "Version number"),
    (
        9,
        "hword_numpart",
        "Hyphenated word part, letters and digits",
    ),
    (10, "hword_part", "Hyphenated word part, all letters"),
    (11, "hword_asciipart", "Hyphenated word part, all ASCII"),
    (12, "blank", "Space symbols"),
    (13, "tag", "XML tag"),
    (14, "protocol", "Protocol head"),
    (15, "numhword", "Hyphenated word, letters and digits"),
    (16, "asciihword", "Hyphenated word, all ASCII"),
    (17, "hword", "Hyphenated word, all letters"),
    (18, "url_path", "URL path"),
    (19, "file", "File or path name"),
    (20, "float", "Decimal notation"),
    (21, "int", "Signed integer"),
    (22, "uint", "Unsigned integer"),
    (23, "entity", "XML entity"),
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DefaultParserToken {
    pub(crate) id: i32,
    pub(crate) text: String,
}

/// Tokenize with PostgreSQL's built-in parser categories.
///
/// The full-text vector path deliberately keeps its existing dictionary-first
/// normalization. This lexer instead preserves every lexical item, including
/// blanks and XML syntax, for the parser inspection functions.
pub(crate) fn default_parser_tokens(source: &str) -> Vec<DefaultParserToken> {
    let mut tokens = Vec::new();
    let mut offset = 0;
    while offset < source.len() {
        let rest = &source[offset..];
        if let Some(length) = xml_tag_length(rest) {
            push_token(&mut tokens, 13, &rest[..length]);
            offset += length;
            continue;
        }
        if let Some(length) = xml_entity_length(rest) {
            push_token(&mut tokens, 23, &rest[..length]);
            offset += length;
            continue;
        }
        let end = if token_starts(rest) {
            rest.char_indices()
                .skip(1)
                .find_map(|(index, character)| {
                    (character.is_whitespace() || character == '<').then_some(index)
                })
                .unwrap_or(rest.len())
        } else {
            rest.char_indices()
                .skip(1)
                .find_map(|(index, _)| {
                    let suffix = &rest[index..];
                    (token_starts(suffix)
                        || xml_tag_length(suffix).is_some()
                        || xml_entity_length(suffix).is_some())
                    .then_some(index)
                })
                .unwrap_or(rest.len())
        };
        let chunk = &rest[..end];
        if token_starts(rest) {
            let chunk = if is_url_like(chunk) {
                chunk
            } else {
                chunk.split_once('&').map_or(chunk, |(prefix, _)| prefix)
            };
            if !chunk.is_empty() {
                let length = chunk.len();
                tokenize_chunk(&mut tokens, chunk);
                offset += length;
                continue;
            }
        }
        push_token(&mut tokens, 12, chunk);
        offset += end;
    }
    tokens
}

fn token_starts(value: &str) -> bool {
    let mut characters = value.chars();
    match characters.next() {
        Some(character) if character.is_alphanumeric() || character == '_' => true,
        Some('/' | '~') => characters
            .next()
            .is_some_and(|character| !character.is_whitespace()),
        Some('+' | '-') => characters
            .next()
            .is_some_and(|character| character.is_ascii_digit()),
        _ => false,
    }
}

fn xml_tag_length(value: &str) -> Option<usize> {
    let body = value.strip_prefix('<')?;
    let first = body.trim_start_matches('/').chars().next()?;
    if !first.is_alphabetic() {
        return None;
    }
    let mut quote = None;
    for (index, character) in body.char_indices() {
        match (quote, character) {
            (Some(active), character) if character == active => quote = None,
            (None, '"' | '\'') => quote = Some(character),
            (None, '>') => return Some(index + 2),
            (None, '<') => return None,
            _ => {}
        }
    }
    None
}

fn xml_entity_length(value: &str) -> Option<usize> {
    let end = value.find(';')?;
    let entity = &value[..=end];
    is_entity(entity).then_some(entity.len())
}

fn is_url_like(value: &str) -> bool {
    value.starts_with("http://")
        || value.starts_with("https://")
        || value
            .split_once('/')
            .is_some_and(|(host, path)| !path.is_empty() && is_host(host))
}

fn tokenize_chunk(tokens: &mut Vec<DefaultParserToken>, chunk: &str) {
    if chunk.is_empty() {
        return;
    }
    if let Some((protocol, body)) = chunk
        .strip_prefix("http://")
        .map(|body| ("http://", body))
        .or_else(|| {
            chunk
                .strip_prefix("https://")
                .map(|body| ("https://", body))
        })
    {
        push_token(tokens, 14, protocol);
        tokenize_url_body(tokens, body);
        return;
    }
    if is_email(chunk) {
        push_token(tokens, 4, chunk);
        return;
    }
    if let Some((left, right)) = chunk.split_once('@') {
        if !left.is_empty() {
            tokenize_chunk(tokens, left);
        }
        push_token(tokens, 12, "@");
        if !right.is_empty() {
            tokenize_chunk(tokens, right);
        }
        return;
    }
    if let Some((host, _)) = chunk.split_once('/')
        && is_host(host)
    {
        tokenize_url_body(tokens, chunk);
        return;
    }
    let (core, trailing) = trim_trailing_punctuation(chunk);
    if !core.is_empty() {
        tokenize_core(tokens, core);
    }
    if !trailing.is_empty() {
        push_token(tokens, 12, trailing);
    }
}

fn tokenize_core(tokens: &mut Vec<DefaultParserToken>, chunk: &str) {
    if is_scientific(chunk) {
        push_token(tokens, 7, chunk);
    } else if is_version(chunk) {
        push_token(tokens, 8, chunk);
    } else if is_decimal(chunk) {
        push_token(tokens, 20, chunk);
    } else if is_signed_integer(chunk) {
        push_token(tokens, 21, chunk);
    } else if chunk.bytes().all(|byte| byte.is_ascii_digit()) {
        push_token(tokens, 22, chunk);
    } else if is_host(chunk) {
        push_token(tokens, 6, chunk);
    } else if let Some((prefix, suffix)) = chunk.rsplit_once('-')
        && is_word(prefix)
        && (is_decimal(&format!("-{suffix}")) || suffix.bytes().all(|byte| byte.is_ascii_digit()))
    {
        tokenize_core(tokens, prefix);
        tokenize_core(tokens, &format!("-{suffix}"));
    } else if chunk.contains('-') && chunk.split('-').all(is_word) {
        let id = if chunk
            .split('-')
            .all(|part| part.chars().all(char::is_alphabetic))
        {
            if chunk.is_ascii() { 16 } else { 17 }
        } else {
            15
        };
        push_token(tokens, id, chunk);
        for (index, part) in chunk.split('-').enumerate() {
            if index > 0 {
                push_token(tokens, 12, "-");
            }
            push_token(tokens, hword_part_type(part), part);
        }
    } else if is_file(chunk) {
        push_token(tokens, 19, chunk);
    } else if is_word(chunk) {
        let id = if chunk.chars().all(char::is_alphabetic) {
            if chunk.is_ascii() { 1 } else { 2 }
        } else {
            3
        };
        push_token(tokens, id, chunk);
    } else {
        let mut offset = 0;
        while offset < chunk.len() {
            let rest = &chunk[offset..];
            let word = rest
                .chars()
                .next()
                .is_some_and(|character| character.is_alphanumeric() || character == '_');
            let length = rest
                .char_indices()
                .skip(1)
                .find_map(|(index, character)| {
                    (word != (character.is_alphanumeric() || character == '_')).then_some(index)
                })
                .unwrap_or(rest.len());
            let part = &rest[..length];
            if word {
                tokenize_core(tokens, part);
            } else {
                push_token(tokens, 12, part);
            }
            offset += length;
        }
    }
}

fn hword_part_type(part: &str) -> i32 {
    if part.chars().all(char::is_alphabetic) {
        if part.is_ascii() { 11 } else { 10 }
    } else {
        9
    }
}

fn tokenize_url_body(tokens: &mut Vec<DefaultParserToken>, body: &str) {
    let (body, trailing) = trim_trailing_punctuation(body);
    if let Some((host, path)) = body.split_once('/')
        && is_host(host)
    {
        if path.is_empty() {
            push_token(tokens, 6, host);
            push_token(tokens, 12, "/");
        } else {
            push_token(tokens, 5, body);
            push_token(tokens, 6, host);
            push_token(tokens, 18, &body[host.len()..]);
        }
    } else if is_host(body) {
        push_token(tokens, 6, body);
    } else if !body.is_empty() {
        tokenize_core(tokens, body);
    }
    if !trailing.is_empty() {
        push_token(tokens, 12, trailing);
    }
}

fn trim_trailing_punctuation(chunk: &str) -> (&str, &str) {
    let end = chunk
        .trim_end_matches(|character: char| matches!(character, ',' | '.' | ';' | ':'))
        .len();
    chunk.split_at(end)
}

fn is_entity(value: &str) -> bool {
    value.ends_with(';')
        && value.strip_prefix('&').is_some_and(|body| {
            body[..body.len() - 1].starts_with('#')
                || body[..body.len() - 1].chars().all(char::is_alphanumeric)
        })
}

fn is_email(value: &str) -> bool {
    let Some((local, domain)) = value.split_once('@') else {
        return false;
    };
    !local.is_empty()
        && local.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.')
        })
        && is_host(domain)
        && domain
            .rsplit_once('.')
            .is_some_and(|(_, top_level)| top_level.len() >= 2)
}

fn is_host(value: &str) -> bool {
    value.contains('.')
        && value.split('.').all(|part| {
            !part.is_empty()
                && part.chars().all(|character| {
                    character.is_ascii_alphanumeric() || matches!(character, '-' | ':')
                })
        })
        && value
            .rsplit_once('.')
            .is_some_and(|(_, top_level)| top_level.len() >= 2)
}

fn is_version(value: &str) -> bool {
    value.split('.').count() > 2
        && value
            .split('.')
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
}

fn is_file(value: &str) -> bool {
    value.len() > 1
        && (value.starts_with('/')
            || value.starts_with('~')
            || value.contains('/')
            || value.contains('.'))
}

fn is_scientific(value: &str) -> bool {
    let Some((mantissa, exponent)) = value.split_once(['e', 'E']) else {
        return false;
    };
    is_decimal(mantissa) && is_signed_integer(exponent)
}

fn is_decimal(value: &str) -> bool {
    let Some((whole, fraction)) = value.split_once('.') else {
        return false;
    };
    !whole.is_empty()
        && !fraction.is_empty()
        && whole.parse::<i64>().is_ok()
        && fraction.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_signed_integer(value: &str) -> bool {
    value.strip_prefix(['+', '-']).is_some_and(|digits| {
        !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
    })
}

fn is_word(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|character| character.is_alphanumeric() || character == '_')
}

fn push_token(tokens: &mut Vec<DefaultParserToken>, id: i32, text: &str) {
    if !text.is_empty() {
        if id == 12
            && let Some(DefaultParserToken {
                id: previous_id,
                text: previous_text,
            }) = tokens.last_mut()
            && *previous_id == 12
        {
            previous_text.push_str(text);
            return;
        }
        tokens.push(DefaultParserToken {
            id,
            text: text.into(),
        });
    }
}

fn text_search_func(name: &str) -> Option<TextSearchFunc> {
    Some(match name {
        "to_tsvector" => TextSearchFunc::ToTsVector,
        "to_tsquery" => TextSearchFunc::ToTsQuery,
        "plainto_tsquery" => TextSearchFunc::PlainToTsQuery,
        "phraseto_tsquery" => TextSearchFunc::PhraseToTsQuery,
        "websearch_to_tsquery" => TextSearchFunc::WebsearchToTsQuery,
        "strip" => TextSearchFunc::Strip,
        "numnode" => TextSearchFunc::NumNode,
        "querytree" => TextSearchFunc::QueryTree,
        "tsquery_phrase" => TextSearchFunc::TsQueryPhrase,
        "array_to_tsvector" => TextSearchFunc::ArrayToTsVector,
        "setweight" => TextSearchFunc::SetWeight,
        "ts_delete" => TextSearchFunc::TsDelete,
        "ts_filter" => TextSearchFunc::TsFilter,
        "ts_rank" => TextSearchFunc::TsRank,
        "ts_rank_cd" => TextSearchFunc::TsRankCd,
        "ts_headline" => TextSearchFunc::TsHeadline,
        "ts_lexize" => TextSearchFunc::TsLexize,
        "jsonb_to_tsvector" => TextSearchFunc::JsonbToTsVector,
        "json_to_tsvector" => TextSearchFunc::JsonToTsVector,
        _ => return None,
    })
}

pub(crate) fn is_text_search_func(name: &str) -> bool {
    text_search_func(name).is_some()
}

pub(crate) fn text_search_result_type(
    fc: &FuncCall,
    _scope: &Scope,
) -> Result<ColumnType, ExecError> {
    let function = text_search_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let count = checked_args(fc)?.len();
    let result = match function {
        TextSearchFunc::ToTsVector
        | TextSearchFunc::ToTsQuery
        | TextSearchFunc::PlainToTsQuery
        | TextSearchFunc::PhraseToTsQuery
        | TextSearchFunc::WebsearchToTsQuery => {
            require_arity(fc, count == 1 || count == 2)?;
            if function == TextSearchFunc::ToTsVector {
                ColumnType::TsVector
            } else {
                ColumnType::TsQuery
            }
        }
        TextSearchFunc::Strip => {
            require_arity(fc, count == 1)?;
            ColumnType::TsVector
        }
        TextSearchFunc::NumNode => {
            require_arity(fc, count == 1)?;
            ColumnType::Int4
        }
        TextSearchFunc::QueryTree => {
            require_arity(fc, count == 1)?;
            ColumnType::Text
        }
        TextSearchFunc::TsQueryPhrase => {
            require_arity(fc, count == 2 || count == 3)?;
            ColumnType::TsQuery
        }
        TextSearchFunc::ArrayToTsVector => {
            require_arity(fc, count == 1)?;
            ColumnType::TsVector
        }
        TextSearchFunc::SetWeight => {
            require_arity(fc, count == 2 || count == 3)?;
            ColumnType::TsVector
        }
        TextSearchFunc::TsDelete => {
            require_arity(fc, count == 2)?;
            ColumnType::TsVector
        }
        TextSearchFunc::TsFilter => {
            require_arity(fc, count == 2)?;
            ColumnType::TsVector
        }
        TextSearchFunc::TsRank | TextSearchFunc::TsRankCd => {
            require_arity(fc, (2..=4).contains(&count))?;
            ColumnType::Float4
        }
        TextSearchFunc::TsHeadline => {
            require_arity(fc, (2..=4).contains(&count))?;
            ColumnType::Text
        }
        TextSearchFunc::TsLexize => {
            require_arity(fc, count == 2)?;
            ColumnType::Array(ElemType::Text)
        }
        TextSearchFunc::JsonbToTsVector | TextSearchFunc::JsonToTsVector => {
            require_arity(fc, count == 2 || count == 3)?;
            ColumnType::TsVector
        }
    };
    Ok(result)
}

pub(crate) fn eval_text_search(
    fc: &FuncCall,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let function = text_search_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    let args = checked_args(fc)?;
    let values = args
        .iter()
        .map(&mut eval_child)
        .collect::<Result<Vec<_>, _>>()?;
    if values.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    let catalog = ctx.catalog();

    match function {
        TextSearchFunc::ToTsVector => {
            let (config, text) = config_and_text(fc, &values, catalog)?;
            Ok(Datum::TsVector(to_tsvector(config, text, catalog)?))
        }
        TextSearchFunc::ToTsQuery => {
            let (config, text) = config_and_text(fc, &values, catalog)?;
            Ok(Datum::TsQuery(to_tsquery(config, text, catalog)?))
        }
        TextSearchFunc::PlainToTsQuery => {
            let (config, text) = config_and_text(fc, &values, catalog)?;
            Ok(Datum::TsQuery(plain_query(config, text, false, catalog)?))
        }
        TextSearchFunc::PhraseToTsQuery => {
            let (config, text) = config_and_text(fc, &values, catalog)?;
            Ok(Datum::TsQuery(plain_query(config, text, true, catalog)?))
        }
        TextSearchFunc::WebsearchToTsQuery => {
            let (config, text) = config_and_text(fc, &values, catalog)?;
            Ok(Datum::TsQuery(web_query(config, text, catalog)?))
        }
        TextSearchFunc::Strip => match values.as_slice() {
            [Datum::TsVector(vector)] => Ok(Datum::TsVector(vector.strip())),
            [got] => Err(type_error("tsvector", got)),
            _ => Err(undefined_function(&fc.name)),
        },
        TextSearchFunc::NumNode => match values.as_slice() {
            [Datum::TsQuery(query)] => Ok(Datum::Int4(
                i32::try_from(query.node_count()).unwrap_or(i32::MAX),
            )),
            [got] => Err(type_error("tsquery", got)),
            _ => Err(undefined_function(&fc.name)),
        },
        TextSearchFunc::QueryTree => match values.as_slice() {
            [Datum::TsQuery(query)] => Ok(Datum::Text(
                query_tree(query).map_or_else(|| "T".into(), |query| query.to_string()),
            )),
            [got] => Err(type_error("tsquery", got)),
            _ => Err(undefined_function(&fc.name)),
        },
        TextSearchFunc::TsQueryPhrase => query_phrase(fc, &values),
        TextSearchFunc::ArrayToTsVector => array_to_vector(fc, &values),
        TextSearchFunc::SetWeight => set_weight(fc, &values),
        TextSearchFunc::TsDelete => delete_terms(fc, &values),
        TextSearchFunc::TsFilter => filter_weights(fc, &values),
        TextSearchFunc::TsRank | TextSearchFunc::TsRankCd => rank(fc, &values),
        TextSearchFunc::TsHeadline => headline(fc, &values, catalog),
        TextSearchFunc::TsLexize => lexize(fc, &values, catalog),
        TextSearchFunc::JsonbToTsVector => jsonb_to_vector(fc, &values, catalog),
        TextSearchFunc::JsonToTsVector => json_to_vector(fc, &values, catalog),
    }
}

fn jsonb_to_vector(
    fc: &FuncCall,
    values: &[Datum],
    catalog: Catalog<'_>,
) -> Result<Datum, ExecError> {
    let (config, document, filter) = match values {
        [Datum::Jsonb(document), Datum::Jsonb(filter)] => (default_config()?, document, filter),
        [
            Datum::Text(config),
            Datum::Jsonb(document),
            Datum::Jsonb(filter),
        ] => (config.clone(), document, filter),
        [got, ..] => return Err(type_error("jsonb", got)),
        _ => return Err(undefined_function(&fc.name)),
    };
    let filter = JsonTextFilter::parse(filter)?;
    let mut pieces = Vec::new();
    collect_json_text(document, filter, &mut pieces);
    Ok(Datum::TsVector(vector_from_pieces(
        &config, &pieces, catalog,
    )?))
}

#[derive(Clone, Copy, Default)]
struct JsonTextFilter(u8);

impl JsonTextFilter {
    const STRING: u8 = 1;
    const NUMERIC: u8 = 2;
    const BOOLEAN: u8 = 4;
    const KEY: u8 = 8;
    const ALL: Self = Self(Self::STRING | Self::NUMERIC | Self::BOOLEAN | Self::KEY);

    fn parse(value: &JsonbValue) -> Result<Self, ExecError> {
        let items = match value {
            JsonbValue::Array(items) => items.as_slice(),
            JsonbValue::String(_) => std::slice::from_ref(value),
            _ => {
                return Err(ExecError::InvalidParameterValue(
                    "wrong type of jsonb filter: string or array expected".into(),
                ));
            }
        };
        if items.is_empty() {
            return Ok(Self::default());
        }
        let mut filter = Self::default();
        for item in items {
            let JsonbValue::String(item) = item else {
                return Err(ExecError::InvalidParameterValue(
                    "wrong type of jsonb filter element: string expected".into(),
                ));
            };
            if item == "all" {
                return Ok(Self::ALL);
            }
            match item.as_str() {
                "string" => filter.0 |= Self::STRING,
                "numeric" => filter.0 |= Self::NUMERIC,
                "boolean" => filter.0 |= Self::BOOLEAN,
                "key" => filter.0 |= Self::KEY,
                other => {
                    return Err(ExecError::InvalidParameterValue(format!(
                        "unrecognized jsonb_to_tsvector filter type: {other}"
                    )));
                }
            }
        }
        Ok(filter)
    }

    const fn contains(self, flag: u8) -> bool {
        self.0 & flag != 0
    }
}

fn collect_json_text(value: &JsonbValue, filter: JsonTextFilter, out: &mut Vec<String>) {
    match value {
        JsonbValue::Object(pairs) => {
            for (key, value) in pairs {
                if filter.contains(JsonTextFilter::KEY) {
                    out.push(key.clone());
                }
                collect_json_text(value, filter, out);
            }
        }
        JsonbValue::Array(items) => {
            for item in items {
                collect_json_text(item, filter, out);
            }
        }
        JsonbValue::String(value) if filter.contains(JsonTextFilter::STRING) => {
            out.push(value.clone());
        }
        JsonbValue::Number(value) if filter.contains(JsonTextFilter::NUMERIC) => {
            out.push(crabka_pgtypes::numeric::finite_to_text(value));
        }
        JsonbValue::Bool(value) if filter.contains(JsonTextFilter::BOOLEAN) => {
            out.push(value.to_string());
        }
        JsonbValue::Null | JsonbValue::String(_) | JsonbValue::Number(_) | JsonbValue::Bool(_) => {}
    }
}

/// `json_to_tsvector`: the same filter and the same lexeme accumulation, over the
/// document's own text rather than a decomposed value, so the positions follow
/// input order.
fn json_to_vector(
    fc: &FuncCall,
    values: &[Datum],
    catalog: Catalog<'_>,
) -> Result<Datum, ExecError> {
    let (config, document, filter) = match values {
        [Datum::Json(document), Datum::Jsonb(filter)] => (default_config()?, document, filter),
        [
            Datum::Text(config),
            Datum::Json(document),
            Datum::Jsonb(filter),
        ] => (config.clone(), document, filter),
        [got, ..] => return Err(type_error("json", got)),
        _ => return Err(undefined_function(&fc.name)),
    };
    let filter = JsonTextFilter::parse(filter)?;
    // `iterate_json_values` builds its lexer with `need_escapes`, so a document
    // whose escapes do not decode is refused before any lexeme is produced.
    // Without this the decode still happens -- `collect_json_document_text`
    // reaches `json::as_text` -- and a `\u0000` or an unpaired surrogate lands
    // in a stored `tsvector`, which is the corruption the other eleven `json`
    // entry points were closed against.
    crabka_pgtypes::json::validate_escapes(document)?;
    let mut pieces = Vec::new();
    collect_json_document_text(document, filter, &mut pieces);
    Ok(Datum::TsVector(vector_from_pieces(
        &config, &pieces, catalog,
    )?))
}

/// [`collect_json_text`]'s twin over `json` text: object fields in input order
/// with duplicates kept, and numbers as the token the document actually holds.
fn collect_json_document_text(value: &str, filter: JsonTextFilter, out: &mut Vec<String>) {
    use crabka_pgtypes::json::{self, Kind};
    match json::kind(value) {
        Kind::Object => {
            for (key, item) in json::object_fields(value).unwrap_or_default() {
                if filter.contains(JsonTextFilter::KEY) {
                    out.push(key);
                }
                collect_json_document_text(item, filter, out);
            }
        }
        Kind::Array => {
            for item in json::array_elements(value).unwrap_or_default() {
                collect_json_document_text(item, filter, out);
            }
        }
        Kind::String if filter.contains(JsonTextFilter::STRING) => {
            out.push(json::as_text(value.trim()));
        }
        Kind::Number if filter.contains(JsonTextFilter::NUMERIC) => {
            out.push(value.trim().to_string());
        }
        Kind::Bool if filter.contains(JsonTextFilter::BOOLEAN) => {
            out.push(value.trim().to_string());
        }
        Kind::Null | Kind::String | Kind::Number | Kind::Bool => {}
    }
}

fn vector_from_pieces(
    config: &str,
    pieces: &[String],
    catalog: Catalog<'_>,
) -> Result<TsVector, ExecError> {
    let mut lexemes = BTreeMap::<String, Vec<Position>>::new();
    let mut offset = 0_u16;
    for piece in pieces {
        for (text, position) in normalized_terms(config, piece, catalog)? {
            lexemes.entry(text).or_default().push(Position {
                position: position.saturating_add(offset).min(MAX_POSITION),
                weight: Weight::D,
            });
        }
        let words = u16::try_from(words(piece).count()).unwrap_or(MAX_POSITION);
        offset = offset
            .saturating_add(words)
            .saturating_add(1)
            .min(MAX_POSITION);
    }
    Ok(TsVector::new(
        lexemes
            .into_iter()
            .map(|(text, positions)| Lexeme { text, positions }),
    ))
}

fn config_and_text<'a>(
    fc: &FuncCall,
    values: &'a [Datum],
    catalog: Catalog<'_>,
) -> Result<(&'a str, &'a str), ExecError> {
    match values {
        [Datum::Text(text)] => {
            let config =
                crate::session::current_setting_runtime("default_text_search_config", false)?
                    .expect("registered GUC has a value");
            // The GUC value belongs to the session runtime, so it cannot be
            // returned with `values`' lifetime. Its only supported default is
            // English; validate it here and return that stable spelling.
            let simple = validate_config(&config, catalog)?;
            Ok((if simple { "simple" } else { "english" }, text))
        }
        [Datum::Text(config), Datum::Text(text)] => Ok((config, text)),
        [got] => Err(type_error("text", got)),
        [_, got] => Err(type_error("text", got)),
        _ => Err(undefined_function(&fc.name)),
    }
}

fn validate_config(config: &str, catalog: Catalog<'_>) -> Result<bool, ExecError> {
    crate::text_search_catalog::config_is_simple(catalog, config)
}

/// Build a searchable vector with no index. Positions count source tokens,
/// including stop words, as PostgreSQL's parser does.
pub(crate) fn to_tsvector(
    config: &str,
    source: &str,
    catalog: Catalog<'_>,
) -> Result<TsVector, ExecError> {
    let terms = normalized_terms(config, source, catalog)?;
    let mut lexemes = BTreeMap::<String, Vec<Position>>::new();
    for (text, position) in terms {
        lexemes.entry(text).or_default().push(Position {
            position,
            weight: Weight::D,
        });
    }
    Ok(TsVector::new(
        lexemes
            .into_iter()
            .map(|(text, positions)| Lexeme { text, positions }),
    ))
}

fn to_tsquery(config: &str, source: &str, catalog: Catalog<'_>) -> Result<TsQuery, ExecError> {
    let query = source.parse::<TsQuery>()?;
    normalize_query(config, query, catalog)
}

fn normalize_query(
    config: &str,
    query: TsQuery,
    catalog: Catalog<'_>,
) -> Result<TsQuery, ExecError> {
    let dictionaries = crate::text_search_catalog::config_dictionaries(catalog, config)?;
    if dictionaries != ["simple"] && dictionaries != ["english_stem"] {
        return mapped_query(query, &dictionaries, catalog);
    }
    let simple = dictionaries == ["simple"];
    let stemmer = Stemmer::create(Algorithm::English);
    Ok(normalize_query_inner(query, simple, &stemmer)
        .0
        .unwrap_or(TsQuery::Empty))
}

fn mapped_query(
    query: TsQuery,
    dictionaries: &[String],
    catalog: Catalog<'_>,
) -> Result<TsQuery, ExecError> {
    Ok(match query {
        TsQuery::Empty => TsQuery::Empty,
        TsQuery::Term(term) => {
            let mut groups = Vec::new();
            for dictionary in dictionaries {
                let template =
                    crate::text_search_catalog::dictionary_template(catalog, dictionary)?;
                if let crate::text_search_catalog::DictionaryTemplate::Ispell {
                    dict_file,
                    aff_file,
                } = template
                {
                    if let Some(words) = crate::text_search_ispell::query_lexize_files(
                        &term.text, &dict_file, &aff_file,
                    ) {
                        groups = words;
                        break;
                    }
                } else if let crate::text_search_catalog::DictionaryTemplate::Synonym {
                    synonyms,
                    case_sensitive,
                } = &template
                {
                    if let Some(words) =
                        crate::text_search_synonym::lexize(&term.text, synonyms, *case_sensitive)
                    {
                        groups = words.into_iter().map(|word| vec![word]).collect();
                        break;
                    }
                } else if let Some(words) = lexize_dictionary(dictionary, &term.text, catalog)? {
                    groups = words.into_iter().map(|word| vec![word]).collect();
                    break;
                }
            }
            groups
                .into_iter()
                .map(|words| {
                    words.into_iter().fold(TsQuery::Empty, |query, text| {
                        let (text, synonym_prefix) = text
                            .strip_suffix('*')
                            .map_or((text.as_str(), false), |text| (text, true));
                        combine_nonempty(
                            query,
                            TsQuery::Term(QueryTerm {
                                text: text.into(),
                                weights: term.weights.clone(),
                                prefix: term.prefix || synonym_prefix,
                            }),
                            |left, right| TsQuery::And(Box::new(left), Box::new(right)),
                        )
                    })
                })
                .reduce(|left, right| TsQuery::Or(Box::new(left), Box::new(right)))
                .unwrap_or(TsQuery::Empty)
        }
        TsQuery::Not(inner) => match mapped_query(*inner, dictionaries, catalog)? {
            TsQuery::Empty => TsQuery::Empty,
            query => TsQuery::Not(Box::new(query)),
        },
        TsQuery::And(left, right) => {
            let left = mapped_query(*left, dictionaries, catalog)?;
            let right = mapped_query(*right, dictionaries, catalog)?;
            combine_nonempty(left, right, |left, right| {
                TsQuery::And(Box::new(left), Box::new(right))
            })
        }
        TsQuery::Or(left, right) => {
            let left = mapped_query(*left, dictionaries, catalog)?;
            let right = mapped_query(*right, dictionaries, catalog)?;
            combine_nonempty(left, right, |left, right| {
                TsQuery::Or(Box::new(left), Box::new(right))
            })
        }
        TsQuery::Phrase(left, right, distance) => {
            let left = mapped_query(*left, dictionaries, catalog)?;
            let right = mapped_query(*right, dictionaries, catalog)?;
            combine_nonempty(left, right, |left, right| {
                TsQuery::Phrase(Box::new(left), Box::new(right), distance)
            })
        }
    })
}

/// The distance a vanished operand still contributes to its parent phrase, on
/// each side of what is left.
///
/// PostgreSQL's `clean_stopword_intree` carries these up the tree so that a
/// stop word removed from inside a phrase widens the gap it leaves rather than
/// closing it: `foo <-> a <-> the <-> bar` is `'foo' <3> 'bar'`, not
/// `'foo' <-> 'bar'`.
#[derive(Debug, Clone, Copy, Default)]
struct PhraseGap {
    left: u16,
    right: u16,
}

/// Strip the stop words out of a parsed query, and repair the phrase distances
/// their removal would otherwise swallow.
///
/// This is `clean_stopword_intree` with the lexizing folded in. `None` is
/// PostgreSQL's null node: the whole subtree was stop words, and the caller
/// decides what its width does to the operator above it.
fn normalize_query_inner(
    query: TsQuery,
    simple: bool,
    stemmer: &Stemmer,
) -> (Option<TsQuery>, PhraseGap) {
    let gap = PhraseGap::default();
    match query {
        TsQuery::Empty => (None, gap),
        TsQuery::Term(mut term) => {
            // The stop-word list is consulted on the folded word, before
            // stemming, exactly as `dsnowball_lexize` does: `above` is a stop
            // word, its stem `abov` is not on any list.
            let folded = term.text.to_lowercase();
            if !simple && is_stopword(&folded) {
                return (None, gap);
            }
            term.text = normalize_word(&folded, simple, stemmer);
            (Some(TsQuery::Term(term)), gap)
        }
        // `NOT` does not change the width of what it matches, so it reports
        // its child's distances unaltered.
        TsQuery::Not(inner) => {
            let (inner, gap) = normalize_query_inner(*inner, simple, stemmer);
            (inner.map(|query| TsQuery::Not(Box::new(query))), gap)
        }
        TsQuery::And(left, right) => binary_node(*left, *right, BinaryKind::And, simple, stemmer),
        TsQuery::Or(left, right) => binary_node(*left, *right, BinaryKind::Or, simple, stemmer),
        TsQuery::Phrase(left, right, distance) => {
            binary_node(*left, *right, BinaryKind::Phrase(distance), simple, stemmer)
        }
    }
}

/// Join two accumulated queries, where [`TsQuery::Empty`] stands for "nothing
/// accumulated yet" rather than for a query that matches nothing.
fn combine_nonempty(
    left: TsQuery,
    right: TsQuery,
    combine: impl FnOnce(TsQuery, TsQuery) -> TsQuery,
) -> TsQuery {
    match (left, right) {
        (TsQuery::Empty, query) | (query, TsQuery::Empty) => query,
        (left, right) => combine(left, right),
    }
}

/// Which binary operator [`binary_node`] is repairing. Only a phrase carries a
/// distance of its own, and only a phrase's distance survives the operand it
/// loses.
#[derive(Debug, Clone, Copy)]
enum BinaryKind {
    And,
    Or,
    Phrase(u16),
}

/// One binary operator's half of [`normalize_query_inner`].
fn binary_node(
    left: TsQuery,
    right: TsQuery,
    kind: BinaryKind,
    simple: bool,
    stemmer: &Stemmer,
) -> (Option<TsQuery>, PhraseGap) {
    let (left, lgap) = normalize_query_inner(left, simple, stemmer);
    let (right, rgap) = normalize_query_inner(right, simple, stemmer);
    let own = match kind {
        BinaryKind::Phrase(distance) => Some(distance),
        BinaryKind::And | BinaryKind::Or => None,
    };
    match (left, right) {
        // Both operands were stop words. A phrase sums the two children's gaps
        // with its own; a boolean operator keeps the wider of the two, which is
        // the width matching would have seen had the operands survived.
        (None, None) => {
            let width = own.map_or_else(
                || lgap.left.max(rgap.left),
                |own| lgap.left.saturating_add(own).saturating_add(rgap.left),
            );
            (
                None,
                PhraseGap {
                    left: width,
                    right: width,
                },
            )
        }
        // One operand goes, and the operator with it. A phrase pushes its own
        // distance out on the side it lost; a boolean operator forgets that
        // side entirely.
        (None, Some(right)) => {
            let gap = own.map_or(rgap, |own| PhraseGap {
                left: lgap.left.saturating_add(own).saturating_add(rgap.left),
                right: rgap.right,
            });
            (Some(right), gap)
        }
        (Some(left), None) => {
            let gap = own.map_or(lgap, |own| PhraseGap {
                left: lgap.left,
                right: lgap.right.saturating_add(own).saturating_add(rgap.right),
            });
            (Some(left), gap)
        }
        // Both operands survive. A phrase absorbs the gaps facing each other
        // into its own distance and passes the outward-facing ones up.
        (Some(left), Some(right)) => match kind {
            BinaryKind::Phrase(distance) => (
                Some(TsQuery::Phrase(
                    Box::new(left),
                    Box::new(right),
                    distance
                        .saturating_add(lgap.right)
                        .saturating_add(rgap.left),
                )),
                PhraseGap {
                    left: lgap.left,
                    right: rgap.right,
                },
            ),
            BinaryKind::And => (
                Some(TsQuery::And(Box::new(left), Box::new(right))),
                PhraseGap::default(),
            ),
            BinaryKind::Or => (
                Some(TsQuery::Or(Box::new(left), Box::new(right))),
                PhraseGap::default(),
            ),
        },
    }
}

fn query_tree(query: &TsQuery) -> Option<TsQuery> {
    match query {
        TsQuery::Empty | TsQuery::Not(_) => None,
        TsQuery::Term(_) => Some(query.clone()),
        TsQuery::And(left, right) => match (query_tree(left), query_tree(right)) {
            (Some(left), Some(right)) => Some(TsQuery::And(Box::new(left), Box::new(right))),
            (left, right) => left.or(right),
        },
        TsQuery::Or(left, right) => Some(TsQuery::Or(
            Box::new(query_tree(left)?),
            Box::new(query_tree(right)?),
        )),
        TsQuery::Phrase(left, right, distance) => Some(TsQuery::Phrase(
            Box::new(query_tree(left)?),
            Box::new(query_tree(right)?),
            *distance,
        )),
    }
}

pub(crate) fn plain_query(
    config: &str,
    source: &str,
    phrase: bool,
    catalog: Catalog<'_>,
) -> Result<TsQuery, ExecError> {
    let terms = normalized_terms(config, source, catalog)?;
    let mut groups = terms
        .into_iter()
        .fold(
            Vec::<(u16, Vec<String>)>::new(),
            |mut groups, (text, position)| {
                if let Some((last_position, texts)) = groups.last_mut()
                    && *last_position == position
                {
                    texts.push(text);
                } else {
                    groups.push((position, vec![text]));
                }
                groups
            },
        )
        .into_iter();
    let Some((mut last_position, first)) = groups.next() else {
        return Ok(TsQuery::Empty);
    };
    let mut query = first
        .into_iter()
        .map(term)
        .reduce(|left, right| TsQuery::And(Box::new(left), Box::new(right)))
        .expect("a text-search position has a lexeme");
    for (position, texts) in groups {
        let right = texts
            .into_iter()
            .map(term)
            .reduce(|left, right| TsQuery::And(Box::new(left), Box::new(right)))
            .expect("a text-search position has a lexeme");
        query = if phrase {
            TsQuery::Phrase(
                Box::new(query),
                Box::new(right),
                position.saturating_sub(last_position),
            )
        } else {
            TsQuery::And(Box::new(query), Box::new(right))
        };
        last_position = position;
    }
    Ok(query)
}

fn web_query(config: &str, source: &str, catalog: Catalog<'_>) -> Result<TsQuery, ExecError> {
    let mut parts = Vec::<(bool, TsQuery)>::new();
    let mut rest = source.trim();
    let mut next_or = false;
    while !rest.is_empty() {
        if rest.len() >= 2
            && rest[..2].eq_ignore_ascii_case("or")
            && rest[2..].chars().next().is_none_or(char::is_whitespace)
        {
            next_or = true;
            rest = rest[2..].trim_start();
            continue;
        }
        let negative = rest.starts_with('-');
        if negative {
            rest = rest[1..].trim_start();
        }
        let (piece, tail, phrase) = if let Some(quoted) = rest.strip_prefix('"') {
            match quoted.find('"') {
                Some(end) => (&quoted[..end], &quoted[end + 1..], true),
                None => (quoted, "", true),
            }
        } else {
            let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
            (&rest[..end], &rest[end..], false)
        };
        let mut query = plain_query(config, piece, phrase, catalog)?;
        if negative && query != TsQuery::Empty {
            query = TsQuery::Not(Box::new(query));
        }
        if query != TsQuery::Empty {
            parts.push((next_or, query));
            next_or = false;
        }
        rest = tail.trim_start();
    }
    let mut groups = Vec::new();
    let mut group = TsQuery::Empty;
    for (starts_group, query) in parts {
        if starts_group && group != TsQuery::Empty {
            groups.push(group);
            group = query;
        } else {
            group = combine_nonempty(group, query, |left, right| {
                TsQuery::And(Box::new(left), Box::new(right))
            });
        }
    }
    if group != TsQuery::Empty {
        groups.push(group);
    }
    Ok(groups
        .into_iter()
        .reduce(|left, right| TsQuery::Or(Box::new(left), Box::new(right)))
        .unwrap_or(TsQuery::Empty))
}

fn query_phrase(fc: &FuncCall, values: &[Datum]) -> Result<Datum, ExecError> {
    let (left, right, distance) = match values {
        [Datum::TsQuery(left), Datum::TsQuery(right)] => (left, right, 1),
        [
            Datum::TsQuery(left),
            Datum::TsQuery(right),
            Datum::Int4(distance),
        ] => {
            let distance = u16::try_from(*distance)
                .ok()
                .filter(|d| *d <= 16_384)
                .ok_or_else(|| {
                    ExecError::InvalidParameterValue("distance must be between 0 and 16384".into())
                })?;
            (left, right, distance)
        }
        [got, ..] => return Err(type_error("tsquery", got)),
        _ => return Err(undefined_function(&fc.name)),
    };
    Ok(Datum::TsQuery(TsQuery::Phrase(
        Box::new(left.clone()),
        Box::new(right.clone()),
        distance,
    )))
}

fn array_to_vector(fc: &FuncCall, values: &[Datum]) -> Result<Datum, ExecError> {
    let [Datum::Array(array)] = values else {
        return values.first().map_or_else(
            || Err(undefined_function(&fc.name)),
            |got| Err(type_error("text[]", got)),
        );
    };
    let mut entries = Vec::with_capacity(array.elems.len());
    for value in &array.elems {
        match value {
            Datum::Text(text) => entries.push(Lexeme {
                text: text.clone(),
                positions: Vec::new(),
            }),
            Datum::Null => {
                return Err(ExecError::InvalidParameterValue(
                    "text array must not contain nulls".into(),
                ));
            }
            got => return Err(type_error("text", got)),
        }
    }
    Ok(Datum::TsVector(TsVector::new(entries)))
}

fn set_weight(fc: &FuncCall, values: &[Datum]) -> Result<Datum, ExecError> {
    let (vector, weight, selected) = match values {
        [Datum::TsVector(vector), Datum::Text(weight)] => (vector, weight, None),
        [
            Datum::TsVector(vector),
            Datum::Text(weight),
            Datum::Array(selected),
        ] => (vector, weight, Some(text_array(selected)?)),
        [got, ..] => return Err(type_error("tsvector", got)),
        _ => return Err(undefined_function(&fc.name)),
    };
    let weight = one_weight(weight)?;
    Ok(Datum::TsVector(
        vector.set_weight(weight, selected.as_deref()),
    ))
}

fn delete_terms(fc: &FuncCall, values: &[Datum]) -> Result<Datum, ExecError> {
    let (vector, words) = match values {
        [Datum::TsVector(vector), Datum::Text(word)] => (vector, vec![word.clone()]),
        [Datum::TsVector(vector), Datum::Array(words)] => (vector, text_array(words)?),
        [got, ..] => return Err(type_error("tsvector", got)),
        _ => return Err(undefined_function(&fc.name)),
    };
    Ok(Datum::TsVector(vector.delete(&words)))
}

fn filter_weights(fc: &FuncCall, values: &[Datum]) -> Result<Datum, ExecError> {
    let [Datum::TsVector(vector), Datum::Array(weights)] = values else {
        return values.first().map_or_else(
            || Err(undefined_function(&fc.name)),
            |got| Err(type_error("tsvector", got)),
        );
    };
    let weights = text_array(weights)?
        .iter()
        .map(|weight| one_weight(weight))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Datum::TsVector(vector.filter_weights(&weights)))
}

fn rank(fc: &FuncCall, values: &[Datum]) -> Result<Datum, ExecError> {
    let (vector, query) = match values {
        [Datum::TsVector(vector), Datum::TsQuery(query)]
        | [
            Datum::TsVector(vector),
            Datum::TsQuery(query),
            Datum::Int4(_),
        ] => (vector, query),
        [
            Datum::Array(_),
            Datum::TsVector(vector),
            Datum::TsQuery(query),
        ]
        | [
            Datum::Array(_),
            Datum::TsVector(vector),
            Datum::TsQuery(query),
            Datum::Int4(_),
        ] => (vector, query),
        [got, ..] => return Err(type_error("tsvector", got)),
        _ => return Err(undefined_function(&fc.name)),
    };
    Ok(Datum::Float4(vector.rank(query)))
}

fn headline(fc: &FuncCall, values: &[Datum], catalog: Catalog<'_>) -> Result<Datum, ExecError> {
    let (config, source, query) = match values {
        [Datum::Text(source), Datum::TsQuery(query)]
        | [Datum::Text(source), Datum::TsQuery(query), Datum::Text(_)] => {
            (default_config()?, source.as_str(), query)
        }
        [
            Datum::Text(config),
            Datum::Text(source),
            Datum::TsQuery(query),
        ]
        | [
            Datum::Text(config),
            Datum::Text(source),
            Datum::TsQuery(query),
            Datum::Text(_),
        ] => (config.clone(), source.as_str(), query),
        [got, ..] => return Err(type_error("text", got)),
        _ => return Err(undefined_function(&fc.name)),
    };
    let wanted = query.terms();
    let mut out = String::with_capacity(source.len() + 16);
    for piece in source.split_inclusive(char::is_whitespace) {
        let word = piece.trim_matches(|character: char| !character.is_alphanumeric());
        let normalized = normalized_terms(&config, word, catalog)?;
        if normalized
            .first()
            .is_some_and(|(term, _)| wanted.contains(&term.as_str()))
        {
            let start = piece.find(word).unwrap_or(0);
            let end = start + word.len();
            out.push_str(&piece[..start]);
            out.push_str("<b>");
            out.push_str(word);
            out.push_str("</b>");
            out.push_str(&piece[end..]);
        } else {
            out.push_str(piece);
        }
    }
    Ok(Datum::Text(out))
}

/// `ts_lexize(dict, token)` — the lexemes one dictionary makes of one token.
///
/// The two templates crabka implements never decline a token, so the result is
/// an array and never SQL NULL: `{}` for a token the dictionary swallows (an
/// empty string, or a stop word), otherwise the one lexeme it produces.
/// PostgreSQL's `simple` dictionary carries no stop-word list — only
/// `english_stem` and the other snowball dictionaries do — so `simple` folds
/// case and stops there.
fn lexize(fc: &FuncCall, values: &[Datum], catalog: Catalog<'_>) -> Result<Datum, ExecError> {
    let (dictionary, token) = match values {
        [Datum::Text(dictionary), Datum::Text(token)] => (dictionary, token),
        [got] | [got, _] => return Err(type_error("regdictionary", got)),
        _ => return Err(undefined_function(&fc.name)),
    };
    let Some(lexemes) = lexize_dictionary(dictionary, token, catalog)? else {
        return Ok(Datum::Null);
    };
    Ok(Datum::Array(ArrayValue::new(
        ElemType::Text,
        lexemes.into_iter().map(Datum::Text).collect(),
    )))
}

fn lexize_dictionary(
    dictionary: &str,
    token: &str,
    catalog: Catalog<'_>,
) -> Result<Option<Vec<String>>, ExecError> {
    let template = crate::text_search_catalog::dictionary_template(catalog, dictionary)?;
    let folded = token.to_lowercase();
    Ok(match template {
        crate::text_search_catalog::DictionaryTemplate::Ispell {
            dict_file,
            aff_file,
        } => crate::text_search_ispell::lexize_files(&folded, &dict_file, &aff_file),
        crate::text_search_catalog::DictionaryTemplate::Synonym {
            synonyms,
            case_sensitive,
        } => crate::text_search_synonym::lexize(token, &synonyms, case_sensitive).map(|words| {
            words
                .into_iter()
                .map(|word| word.trim_end_matches('*').into())
                .collect()
        }),
        crate::text_search_catalog::DictionaryTemplate::Thesaurus { dict_file, .. } => {
            crate::text_search_thesaurus::lexize(token, &dict_file)
        }
        crate::text_search_catalog::DictionaryTemplate::Simple => {
            if folded.is_empty() {
                Some(Vec::new())
            } else {
                Some(vec![folded])
            }
        }
        crate::text_search_catalog::DictionaryTemplate::Snowball => {
            if folded.is_empty() || is_stopword(&folded) {
                Some(Vec::new())
            } else {
                Some(vec![
                    Stemmer::create(Algorithm::English)
                        .stem(&folded)
                        .into_owned(),
                ])
            }
        }
    })
}

/// One parser token with the dictionary result that `ts_debug` exposes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DebugToken {
    pub(crate) dictionaries: Vec<String>,
    pub(crate) dictionary: Option<String>,
    pub(crate) lexemes: Option<Vec<String>>,
}

/// Run the configuration mapping for one token from the built-in parser.
pub(crate) fn debug_token(
    config: &str,
    token: &DefaultParserToken,
    catalog: Catalog<'_>,
) -> Result<DebugToken, ExecError> {
    let (_, token_type, _) = DEFAULT_PARSER_TOKEN_TYPES
        .iter()
        .find(|&&(id, _, _)| id == token.id)
        .expect("the default parser only returns its declared token IDs");
    let dictionaries =
        crate::text_search_catalog::config_token_dictionaries(catalog, config, token_type)?;
    for index in 0..dictionaries.len() {
        let dictionary = dictionaries[index].clone();
        if let Some(lexemes) = lexize_dictionary(&dictionary, &token.text, catalog)? {
            return Ok(DebugToken {
                dictionaries,
                dictionary: Some(dictionary),
                lexemes: Some(lexemes),
            });
        }
    }
    Ok(DebugToken {
        dictionaries,
        dictionary: None,
        lexemes: None,
    })
}

fn default_config() -> Result<String, ExecError> {
    crate::session::current_setting_runtime("default_text_search_config", false)?
        .ok_or_else(|| ExecError::UnrecognizedParameter("default_text_search_config".into()))
}

/// Evaluate the immutable tsquery spellings the local GIN planner accepts.
pub(crate) fn constant_query(expr: &Expr) -> Result<Option<TsQuery>, ExecError> {
    match expr {
        Expr::Const {
            value: Datum::TsQuery(query),
            ..
        } => Ok(Some(query.clone())),
        Expr::StringLiteral(source) => source.parse::<TsQuery>().map(Some).map_err(Into::into),
        Expr::Cast {
            expr,
            ty: ColumnType::TsQuery,
        } => {
            let Some(source) = literal_text(expr) else {
                return Ok(None);
            };
            source.parse::<TsQuery>().map(Some).map_err(Into::into)
        }
        Expr::Func(call) => constant_query_call(call),
        _ => Ok(None),
    }
}

fn constant_query_call(call: &FuncCall) -> Result<Option<TsQuery>, ExecError> {
    let FuncArgs::Exprs(args) = &call.args else {
        return Ok(None);
    };
    let Some(function) = text_search_func(&call.name) else {
        return Ok(None);
    };
    if !matches!(
        function,
        TextSearchFunc::ToTsQuery
            | TextSearchFunc::PlainToTsQuery
            | TextSearchFunc::PhraseToTsQuery
            | TextSearchFunc::WebsearchToTsQuery
    ) {
        return Ok(None);
    }
    let (config, source) = match args.as_slice() {
        [source] => (default_config()?, literal_text(source)),
        [config, source] => {
            let Some(config) = literal_text(config) else {
                return Ok(None);
            };
            (config.to_string(), literal_text(source))
        }
        _ => return Ok(None),
    };
    let Some(source) = source else {
        return Ok(None);
    };
    match function {
        TextSearchFunc::ToTsQuery => to_tsquery(&config, source, None).map(Some),
        TextSearchFunc::PlainToTsQuery => plain_query(&config, source, false, None).map(Some),
        TextSearchFunc::PhraseToTsQuery => plain_query(&config, source, true, None).map(Some),
        TextSearchFunc::WebsearchToTsQuery => web_query(&config, source, None).map(Some),
        _ => Ok(None),
    }
}

fn literal_text(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::StringLiteral(text)
        | Expr::Const {
            value: Datum::Text(text),
            ..
        } => Some(text),
        Expr::Cast { expr, ty } if ty.is_string() => literal_text(expr),
        _ => None,
    }
}

fn text_array(array: &crabka_pgtypes::ArrayValue) -> Result<Vec<String>, ExecError> {
    array
        .elems
        .iter()
        .map(|value| match value {
            Datum::Text(text) => Ok(text.clone()),
            got => Err(type_error("text", got)),
        })
        .collect()
}

fn one_weight(value: &str) -> Result<Weight, ExecError> {
    let mut chars = value.chars();
    chars
        .next()
        .filter(|_| chars.next().is_none())
        .and_then(Weight::parse)
        .ok_or_else(|| ExecError::InvalidParameterValue("unrecognized weight".into()))
}

fn normalized_terms(
    config: &str,
    source: &str,
    catalog: Catalog<'_>,
) -> Result<Vec<(String, u16)>, ExecError> {
    let dictionaries = crate::text_search_catalog::config_dictionaries(catalog, config)?;
    let thesaurus = dictionaries.iter().find_map(|dictionary| {
        match crate::text_search_catalog::dictionary_template(catalog, dictionary).ok()? {
            crate::text_search_catalog::DictionaryTemplate::Thesaurus {
                dict_file,
                dictionary,
            } => Some((dict_file, dictionary)),
            _ => None,
        }
    });
    let source_words = words(source).collect::<Vec<_>>();
    let mut terms = Vec::new();
    let mut index = 0;
    let mut output_position = 1_u16;
    while index < source_words.len() {
        let word = source_words[index];
        let position = output_position.min(MAX_POSITION);
        if let Some((dict_file, dictionary)) = &thesaurus
            && let Some((consumed, lexemes)) =
                crate::text_search_thesaurus::lexize_phrase(&source_words[index..], dict_file)
        {
            let mut produced = 0_u16;
            for lexeme in lexemes {
                let lexeme = lexize_dictionary(dictionary, &lexeme, catalog)?
                    .unwrap_or_else(|| vec![lexeme]);
                for lexeme in lexeme {
                    terms.push((lexeme, position.saturating_add(produced).min(MAX_POSITION)));
                    produced = produced.saturating_add(1);
                }
            }
            index += consumed;
            output_position = output_position.saturating_add(produced.max(1));
            continue;
        }
        for dictionary in &dictionaries {
            if let Some(lexemes) = lexize_dictionary(dictionary, word, catalog)? {
                terms.extend(lexemes.into_iter().map(|lexeme| (lexeme, position)));
                break;
            }
        }
        index += 1;
        output_position = output_position.saturating_add(1);
    }
    Ok(terms)
}

fn words(source: &str) -> impl Iterator<Item = &str> {
    source
        .split(|character: char| !(character.is_alphanumeric() || character == '_'))
        .filter(|word| !word.is_empty())
}

fn normalize_word(word: &str, simple: bool, stemmer: &Stemmer) -> String {
    let lower = word.to_lowercase();
    if simple {
        lower
    } else {
        stemmer.stem(&lower).into_owned()
    }
}

fn term(text: String) -> TsQuery {
    TsQuery::Term(QueryTerm {
        text,
        weights: Vec::new(),
        prefix: false,
    })
}

fn is_stopword(word: &str) -> bool {
    matches!(
        word,
        "a" | "about"
            | "above"
            | "after"
            | "again"
            | "against"
            | "all"
            | "am"
            | "an"
            | "and"
            | "any"
            | "are"
            | "as"
            | "at"
            | "be"
            | "because"
            | "been"
            | "before"
            | "being"
            | "below"
            | "between"
            | "both"
            | "but"
            | "by"
            | "can"
            | "did"
            | "do"
            | "does"
            | "doing"
            | "don"
            | "down"
            | "during"
            | "each"
            | "few"
            | "for"
            | "from"
            | "further"
            | "had"
            | "has"
            | "have"
            | "having"
            | "he"
            | "her"
            | "here"
            | "hers"
            | "herself"
            | "him"
            | "himself"
            | "his"
            | "how"
            | "i"
            | "if"
            | "in"
            | "into"
            | "is"
            | "it"
            | "its"
            | "itself"
            | "just"
            | "me"
            | "more"
            | "most"
            | "my"
            | "myself"
            | "no"
            | "nor"
            | "not"
            | "now"
            | "of"
            | "off"
            | "on"
            | "once"
            | "only"
            | "or"
            | "other"
            | "our"
            | "ours"
            | "ourselves"
            | "out"
            | "over"
            | "own"
            | "s"
            | "same"
            | "she"
            | "should"
            | "so"
            | "some"
            | "such"
            | "t"
            | "than"
            | "that"
            | "the"
            | "their"
            | "theirs"
            | "them"
            | "themselves"
            | "then"
            | "there"
            | "these"
            | "they"
            | "this"
            | "those"
            | "through"
            | "to"
            | "too"
            | "under"
            | "until"
            | "up"
            | "very"
            | "was"
            | "we"
            | "were"
            | "what"
            | "when"
            | "where"
            | "which"
            | "while"
            | "who"
            | "whom"
            | "why"
            | "will"
            | "with"
            | "you"
            | "your"
            | "yours"
            | "yourself"
            | "yourselves"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn english_vector_stems_and_preserves_positions() {
        assert_eq!(
            to_tsvector("english", "The Fat Rats", None)
                .unwrap()
                .to_string(),
            "'fat':2 'rat':3"
        );
    }

    #[test]
    fn phrase_query_counts_stopword_positions() {
        assert_eq!(
            plain_query("english", "The Cat and Rats", true, None)
                .unwrap()
                .to_string(),
            "'cat' <2> 'rat'"
        );
    }

    #[test]
    fn query_normalization_prunes_stopwords() {
        assert_eq!(
            to_tsquery("english", "cat & the", None)
                .unwrap()
                .to_string(),
            "'cat'"
        );
    }

    #[test]
    fn web_query_groups_and_before_or() {
        let query = web_query("simple", "fat OR rat dog", None).unwrap();
        assert_eq!(query.to_string(), "'fat' | 'rat' & 'dog'");
        assert!(to_tsvector("simple", "fat", None).unwrap().matches(&query));
    }

    fn lexize_call() -> FuncCall {
        FuncCall {
            sql_syntax: false,
            name: "ts_lexize".into(),
            distinct: false,
            args: FuncArgs::Exprs(Vec::new()),
            order_by: Vec::new(),
            within_group: false,
            filter: None,
        }
    }

    fn lexemes(dictionary: &str, token: &str) -> Result<Datum, ExecError> {
        lexize(
            &lexize_call(),
            &[Datum::Text(dictionary.into()), Datum::Text(token.into())],
            None,
        )
    }

    fn text_lexemes(dictionary: &str, token: &str) -> Vec<String> {
        let Ok(Datum::Array(array)) = lexemes(dictionary, token) else {
            panic!("ts_lexize returns a text[]")
        };
        text_array(&array).expect("text elements")
    }

    #[test]
    fn snowball_lexize_stems_and_swallows_a_stop_word() {
        assert2::assert!(text_lexemes("english_stem", "skies") == vec!["sky".to_string()]);
        assert2::assert!(text_lexemes("english_stem", "Identity") == vec!["ident".to_string()]);
        assert2::assert!(text_lexemes("english_stem", "the").is_empty());
        assert2::assert!(text_lexemes("english_stem", "").is_empty());
    }

    #[test]
    fn simple_lexize_folds_case_and_keeps_stop_words() {
        assert2::assert!(text_lexemes("simple", "SkIeS") == vec!["skies".to_string()]);
        assert2::assert!(text_lexemes("simple", "the") == vec!["the".to_string()]);
        assert2::assert!(text_lexemes("simple", "").is_empty());
    }

    #[test]
    fn default_parser_preserves_words_urls_xml_and_blanks() {
        assert2::assert!(
            default_parser_tokens("cat 42 http://example.com/a <b>&amp;")
                == vec![
                    DefaultParserToken {
                        id: 1,
                        text: "cat".into(),
                    },
                    DefaultParserToken {
                        id: 12,
                        text: " ".into(),
                    },
                    DefaultParserToken {
                        id: 22,
                        text: "42".into(),
                    },
                    DefaultParserToken {
                        id: 12,
                        text: " ".into(),
                    },
                    DefaultParserToken {
                        id: 14,
                        text: "http://".into(),
                    },
                    DefaultParserToken {
                        id: 5,
                        text: "example.com/a".into(),
                    },
                    DefaultParserToken {
                        id: 6,
                        text: "example.com".into(),
                    },
                    DefaultParserToken {
                        id: 18,
                        text: "/a".into(),
                    },
                    DefaultParserToken {
                        id: 12,
                        text: " ".into(),
                    },
                    DefaultParserToken {
                        id: 13,
                        text: "<b>".into(),
                    },
                    DefaultParserToken {
                        id: 23,
                        text: "&amp;".into(),
                    },
                ]
        );
    }

    #[test]
    fn default_parser_separates_inline_xml_entities() {
        let source =
            "<myns:foo-bar_baz.blurfl>abc&nm1;def&#xa9;ghi&#245;jkl</myns:foo-bar_baz.blurfl>";
        assert2::assert!(
            default_parser_tokens(source)
                == vec![
                    DefaultParserToken {
                        id: 13,
                        text: "<myns:foo-bar_baz.blurfl>".into(),
                    },
                    DefaultParserToken {
                        id: 1,
                        text: "abc".into(),
                    },
                    DefaultParserToken {
                        id: 23,
                        text: "&nm1;".into(),
                    },
                    DefaultParserToken {
                        id: 1,
                        text: "def".into(),
                    },
                    DefaultParserToken {
                        id: 23,
                        text: "&#xa9;".into(),
                    },
                    DefaultParserToken {
                        id: 1,
                        text: "ghi".into(),
                    },
                    DefaultParserToken {
                        id: 23,
                        text: "&#245;".into(),
                    },
                    DefaultParserToken {
                        id: 1,
                        text: "jkl".into(),
                    },
                    DefaultParserToken {
                        id: 13,
                        text: "</myns:foo-bar_baz.blurfl>".into(),
                    },
                ]
        );
    }

    /// A dictionary built on a template crabka does not have never reaches the
    /// catalog, so naming it is a missing dictionary and not a missing
    /// function. Answering it with the wrong lexemes would silently change
    /// what a search matches; the 42704 says so out loud.
    #[test]
    fn an_absent_dictionary_is_refused_by_name() {
        let error = lexemes("ispell", "skies").expect_err("no ispell dictionary");
        assert2::assert!(
            error
                == ExecError::UndefinedObject(
                    "text search dictionary \"ispell\" does not exist".into()
                )
        );
    }

    /// Removing a stop word from inside a phrase widens the gap it leaves.
    /// PostgreSQL's `clean_stopword_intree` carries the vanished operand's
    /// distance up to the surviving operator, so `foo <-> a <-> the <-> bar`
    /// keeps the three-token span it described. The table is every phrase case
    /// upstream's `tsearch` corpus checks, with `a` and `s` the stop words.
    #[test]
    fn a_stop_word_inside_a_phrase_widens_the_distance_it_leaves() {
        for (source, expected) in [
            ("(1 <-> 2) <-> a", "'1' <-> '2'"),
            ("(1 <-> a) <-> 2", "'1' <2> '2'"),
            ("(a <-> 1) <-> 2", "'1' <-> '2'"),
            ("a <-> (1 <-> 2)", "'1' <-> '2'"),
            ("1 <-> (a <-> 2)", "'1' <2> '2'"),
            ("1 <-> (2 <-> a)", "'1' <-> '2'"),
            ("(1 <-> 2) <3> a", "'1' <-> '2'"),
            ("(1 <-> a) <3> 2", "'1' <4> '2'"),
            ("(a <-> 1) <3> 2", "'1' <3> '2'"),
            ("a <3> (1 <-> 2)", "'1' <-> '2'"),
            ("1 <3> (a <-> 2)", "'1' <4> '2'"),
            ("1 <3> (2 <-> a)", "'1' <3> '2'"),
            ("(1 <3> 2) <-> a", "'1' <3> '2'"),
            ("(1 <3> a) <-> 2", "'1' <4> '2'"),
            ("(a <3> 1) <-> 2", "'1' <-> '2'"),
            ("a <-> (1 <3> 2)", "'1' <3> '2'"),
            ("1 <-> (a <3> 2)", "'1' <4> '2'"),
            ("1 <-> (2 <3> a)", "'1' <-> '2'"),
            ("((a <-> 1) <-> 2) <-> s", "'1' <-> '2'"),
            ("(2 <-> (a <-> 1)) <-> s", "'2' <2> '1'"),
            ("((1 <-> a) <-> 2) <-> s", "'1' <2> '2'"),
            ("(2 <-> (1 <-> a)) <-> s", "'2' <-> '1'"),
            ("s <-> ((a <-> 1) <-> 2)", "'1' <-> '2'"),
            ("s <-> (2 <-> (a <-> 1))", "'2' <2> '1'"),
            ("s <-> ((1 <-> a) <-> 2)", "'1' <2> '2'"),
            ("s <-> (2 <-> (1 <-> a))", "'2' <-> '1'"),
            ("((a <-> 1) <-> s) <-> 2", "'1' <2> '2'"),
            ("(s <-> (a <-> 1)) <-> 2", "'1' <-> '2'"),
            ("((1 <-> a) <-> s) <-> 2", "'1' <3> '2'"),
            ("(s <-> (1 <-> a)) <-> 2", "'1' <2> '2'"),
            ("2 <-> ((a <-> 1) <-> s)", "'2' <2> '1'"),
            ("2 <-> (s <-> (a <-> 1))", "'2' <3> '1'"),
            ("2 <-> ((1 <-> a) <-> s)", "'2' <-> '1'"),
            ("2 <-> (s <-> (1 <-> a))", "'2' <2> '1'"),
            ("foo <-> (a <-> (the <-> bar))", "'foo' <3> 'bar'"),
            ("((foo <-> a) <-> the) <-> bar", "'foo' <3> 'bar'"),
            ("foo <-> a <-> the <-> bar", "'foo' <3> 'bar'"),
        ] {
            let query = to_tsquery("english", source, None).expect(source);
            assert2::assert!(query.to_string() == expected, "{source}");
        }
    }

    #[test]
    fn query_tree_removes_unindexable_negation() {
        let query = "cat & !dog".parse().unwrap();
        assert_eq!(query_tree(&query).unwrap().to_string(), "'cat'");
        assert!(query_tree(&"cat | !dog".parse().unwrap()).is_none());
    }
}
