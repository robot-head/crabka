//! `PostgreSQL` `xml`: the input text, validated but never rewritten.
//!
//! `xml_in` parses its argument only to decide whether to accept it, then keeps
//! the original bytes — `'<a  b = "1" />'::xml` prints back with its spacing
//! intact, exactly as `json` does. Everything downstream follows from that:
//! `xml_out` is the identity, `xml_send` is `textsend`, and `xml → text` is
//! declared binary-coercible in `pg_cast` because the two representations are
//! the same bytes. The parser exists for the *decisions*: is this well formed,
//! is it a document, and — for `XMLSERIALIZE … INDENT` — what does the tree
//! look like.
//!
//! # External entities are never resolved
//!
//! `PostgreSQL` installs `xmlPgEntityLoader`, which refuses every external
//! entity and DTD fetch, so
//!
//! ```text
//! XMLPARSE(DOCUMENT '<!DOCTYPE foo [<!ENTITY c SYSTEM "/etc/passwd">]><foo>&c;</foo>')
//! ```
//!
//! echoes its input with `&c;` unexpanded and never opens the file. This module
//! reaches the same answer by construction rather than by configuration: the
//! tokenizer ([`quick_xml`]) performs no I/O of any kind and does not resolve
//! entity references at all — it reports each one as an event — so [`Entities`]
//! is the only thing that can ever turn `&c;` into text, and it resolves
//! nothing it did not read out of the *internal* DTD subset of the very
//! document being parsed. There is no code path from an SQL string to a file
//! handle or a socket. `xml` would otherwise be an arbitrary-file-read
//! primitive reachable from any client that can run `SELECT`.
//!
//! # What is ported and what is approximated
//!
//! The control flow of `xml_parse` is ported exactly, because the observable
//! behaviour lives in it: `parse_xml_decl` skips a leading XML declaration and
//! validates it by hand (which is why a bad `standalone` is a *different*
//! error, with no line/caret, from everything libxml reports); then
//! `xml_doctype_in_content` promotes CONTENT to DOCUMENT when a `<!DOCTYPE>`
//! leads the input, which is why `'<!DOCTYPE a><a/><b/>'` is rejected in
//! CONTENT mode while `'<a/><b/>'` is accepted.
//!
//! The *wording* of a well-formedness complaint is libxml's, and is reproduced
//! for the constructs a SQL client actually writes ([`XmlFault`]). A malformed
//! document crabka cannot name reports `Extra content at the end of the
//! document` or `Start tag expected, '<' not found` as libxml would, but the
//! DETAIL of an exotic failure may differ in wording while agreeing on the
//! message and SQLSTATE.

use std::fmt::Write as _;

use quick_xml::{
    Reader,
    events::{BytesStart, Event},
};

use crate::TypeError;

/// `XmlOptionType` — which of SQL/XML's two well-formedness grammars applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XmlOption {
    /// A single well-formed root element, optionally preceded by a declaration,
    /// comments, processing instructions and a doctype.
    Document,
    /// A well-formed *fragment*: any number of roots, bare text, or nothing at
    /// all. `''` and `'abc'` are both valid content and neither is a document.
    Content,
}

impl XmlOption {
    /// The `errmsg` of a well-formedness failure under this option — the two
    /// grammars have distinct messages *and* distinct SQLSTATEs.
    const fn message(self) -> &'static str {
        match self {
            XmlOption::Document => "invalid XML document",
            XmlOption::Content => "invalid XML content",
        }
    }

    /// `ERRCODE_INVALID_XML_DOCUMENT` / `ERRCODE_INVALID_XML_CONTENT`.
    const fn sqlstate(self) -> &'static str {
        match self {
            XmlOption::Document => "2200M",
            XmlOption::Content => "2200N",
        }
    }
}

// ------------------------------------------------------------ error reporting

/// One libxml complaint: a message and the byte offset the caret points at.
///
/// libxml keeps parsing after a recoverable fault, so a single value can
/// produce several of these — `'<twoerrors>&idontexist;</unbalanced>'` reports
/// the undefined entity *and* the tag mismatch, and `PostgreSQL` prints both.
#[derive(Debug, Clone, PartialEq, Eq)]
struct XmlFault {
    message: String,
    /// Byte offset into the parsed slice that the `^` marker sits under.
    offset: usize,
}

/// The accumulated faults of one parse, in the order libxml would report them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Faults {
    faults: Vec<XmlFault>,
}

impl Faults {
    fn push(&mut self, message: impl Into<String>, offset: usize) {
        self.faults.push(XmlFault {
            message: message.into(),
            offset,
        });
    }

    fn is_empty(&self) -> bool {
        self.faults.is_empty()
    }

    /// `xml_errorHandler`'s DETAIL: `line N: message`, the offending line, and a
    /// caret under the reported column, repeated for each fault.
    ///
    /// `parsed` is the slice libxml saw, which for CONTENT is the input *after*
    /// any XML declaration — but line numbers and the echoed line come from the
    /// whole input, because that is the buffer libxml was handed.
    fn detail(&self, parsed: &str, prefix_len: usize) -> String {
        let mut out = String::new();
        for fault in &self.faults {
            if !out.is_empty() {
                out.push('\n');
            }
            let absolute = prefix_len + fault.offset;
            let (line_no, line, column) = locate(parsed, absolute);
            let _ = write!(out, "line {line_no}: {}", fault.message);
            out.push('\n');
            out.push_str(line);
            out.push('\n');
            // libxml counts the caret column in characters, not bytes.
            for _ in 0..column {
                out.push(' ');
            }
            out.push('^');
        }
        out
    }
}

/// The 1-based line number, the text of that line, and the character offset
/// within it, for a byte offset into `text`.
fn locate(text: &str, offset: usize) -> (usize, &str, usize) {
    let offset = offset.min(text.len());
    let before = &text[..offset];
    let line_no = before.matches('\n').count() + 1;
    let line_start = before.rfind('\n').map_or(0, |i| i + 1);
    let line_end = text[line_start..]
        .find('\n')
        .map_or(text.len(), |i| line_start + i);
    let column = text[line_start..offset].chars().count();
    (line_no, &text[line_start..line_end], column)
}

fn well_formedness_error(option: XmlOption, detail: String) -> TypeError {
    TypeError::XmlSyntax {
        sqlstate: option.sqlstate(),
        message: option.message(),
        detail,
    }
}

// ------------------------------------------------------------ XML declaration

/// `XML_ERR_*`, narrowed to the codes `parse_xml_decl` can return, each with
/// the DETAIL `errdetail_for_xml_code` gives it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeclFault {
    VersionMissing,
    MissingEncoding,
    StandaloneValue,
    NotFinished,
    InvalidChar,
}

impl DeclFault {
    /// libxml's own wording for the same fault, for the DOCUMENT path where
    /// libxml rather than `parse_xml_decl` is the one complaining.
    const fn libxml_message(self) -> &'static str {
        match self {
            DeclFault::VersionMissing => "Malformed declaration expecting version",
            DeclFault::MissingEncoding => "Missing encoding in text declaration",
            DeclFault::StandaloneValue => "standalone accepts only 'yes' or 'no'",
            DeclFault::NotFinished => "parsing XML declaration: '?>' expected",
            DeclFault::InvalidChar => "Invalid character",
        }
    }

    const fn detail(self) -> &'static str {
        match self {
            DeclFault::VersionMissing => "Malformed declaration: expecting version.",
            DeclFault::MissingEncoding => "Missing encoding in text declaration.",
            DeclFault::StandaloneValue => "standalone accepts only 'yes' or 'no'.",
            DeclFault::NotFinished => "Parsing XML declaration: '?>' expected.",
            DeclFault::InvalidChar => "Invalid character.",
        }
    }
}

/// The three facts `xmlconcat` needs out of a leading XML declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XmlDecl {
    /// Bytes the declaration occupies. `0` when there was none.
    pub len: usize,
    /// The `version` pseudo-attribute, when a declaration was present.
    pub version: Option<String>,
    /// `Some(true)` for `standalone="yes"`, `Some(false)` for `"no"`, `None`
    /// when the pseudo-attribute is absent.
    pub standalone: Option<bool>,
}

/// `parse_xml_decl`: recognise and validate a leading `<?xml … ?>`, returning
/// how many bytes it spans.
///
/// This is *not* delegated to the tokenizer. `PostgreSQL` validates the
/// declaration itself, before libxml ever sees the input, which is why a bad
/// `standalone` value is `invalid XML content: invalid XML declaration` with a
/// plain DETAIL and no caret, while every other syntax fault is libxml's.
fn parse_xml_decl(s: &str) -> Result<XmlDecl, DeclFault> {
    let none = XmlDecl {
        len: 0,
        version: None,
        standalone: None,
    };
    let bytes = s.as_bytes();
    if !s.starts_with("<?xml") {
        return Ok(none);
    }
    // `<?xml-stylesheet …?>` is a processing instruction, not a declaration:
    // libxml's test is whether the next character could continue the name.
    if s[5..].chars().next().is_some_and(is_name_char) {
        return Ok(none);
    }

    let mut p = 5;
    let skip_space = |p: &mut usize| {
        while bytes.get(*p).is_some_and(u8::is_ascii_whitespace) {
            *p += 1;
        }
    };
    let had_space = |p: usize| bytes.get(p).is_some_and(u8::is_ascii_whitespace);

    // version — the one pseudo-attribute that is not optional.
    if !had_space(p) {
        return Err(DeclFault::VersionMissing);
    }
    skip_space(&mut p);
    if !s[p..].starts_with("version") {
        return Err(DeclFault::VersionMissing);
    }
    p += 7;
    skip_space(&mut p);
    if bytes.get(p) != Some(&b'=') {
        return Err(DeclFault::VersionMissing);
    }
    p += 1;
    skip_space(&mut p);
    let version = quoted(s, &mut p).ok_or(DeclFault::VersionMissing)?;

    // encoding — optional, and must be separated by whitespace when present.
    let save = p;
    skip_space(&mut p);
    if s[p..].starts_with("encoding") {
        if !had_space(save) {
            return Err(DeclFault::MissingEncoding);
        }
        p += 8;
        skip_space(&mut p);
        if bytes.get(p) != Some(&b'=') {
            return Err(DeclFault::MissingEncoding);
        }
        p += 1;
        skip_space(&mut p);
        quoted(s, &mut p).ok_or(DeclFault::MissingEncoding)?;
    } else {
        p = save;
    }

    // standalone — optional, and accepts exactly two spellings.
    let save = p;
    let mut standalone = None;
    skip_space(&mut p);
    if s[p..].starts_with("standalone") {
        if !had_space(save) {
            return Err(DeclFault::StandaloneValue);
        }
        p += 10;
        skip_space(&mut p);
        if bytes.get(p) != Some(&b'=') {
            return Err(DeclFault::StandaloneValue);
        }
        p += 1;
        skip_space(&mut p);
        if s[p..].starts_with("'yes'") || s[p..].starts_with("\"yes\"") {
            standalone = Some(true);
            p += 5;
        } else if s[p..].starts_with("'no'") || s[p..].starts_with("\"no\"") {
            standalone = Some(false);
            p += 4;
        } else {
            return Err(DeclFault::StandaloneValue);
        }
    } else {
        p = save;
    }

    skip_space(&mut p);
    if !s[p..].starts_with("?>") {
        return Err(DeclFault::NotFinished);
    }
    p += 2;

    if !s[..p].is_ascii() {
        return Err(DeclFault::InvalidChar);
    }
    Ok(XmlDecl {
        len: p,
        version: Some(version),
        standalone,
    })
}

/// Where libxml's caret lands for a declaration fault: on the value of the
/// last pseudo-attribute it managed to read.
fn decl_fault_offset(text: &str) -> usize {
    ["standalone", "encoding", "version"]
        .into_iter()
        .find_map(|word| text.find(word).map(|at| at + word.len() + 2))
        .unwrap_or(0)
}

/// Read a `'…'` or `"…"` pseudo-attribute value, advancing past it.
fn quoted(s: &str, p: &mut usize) -> Option<String> {
    let bytes = s.as_bytes();
    let quote = *bytes.get(*p)?;
    if quote != b'\'' && quote != b'"' {
        return None;
    }
    let rest = &s[*p + 1..];
    let end = rest.find(char::from(quote))?;
    let value = rest[..end].to_string();
    *p += 1 + end + 1;
    Some(value)
}

/// `xml_doctype_in_content`: does a `<!DOCTYPE>` lead this content, past any
/// comments and processing instructions?
///
/// This is the switch that makes CONTENT parse as a DOCUMENT, and it is the
/// reason `'<!DOCTYPE a><a/><b/>'` is rejected while `'<a/><b/>'` is not.
fn doctype_in_content(s: &str) -> bool {
    let mut rest = s.trim_start_matches(|c: char| c.is_ascii_whitespace());
    loop {
        let Some(after) = rest.strip_prefix('<') else {
            return false;
        };
        if let Some(after_bang) = after.strip_prefix('!') {
            if after_bang.starts_with("DOCTYPE") {
                return true;
            }
            let Some(body) = after_bang.strip_prefix("--") else {
                return false;
            };
            let Some(close) = body.find("--") else {
                return false;
            };
            if body.as_bytes().get(close + 2) != Some(&b'>') {
                return false;
            }
            rest = &body[close + 3..];
        } else if let Some(pi) = after.strip_prefix('?') {
            let Some(end) = pi.find("?>") else {
                return false;
            };
            rest = &pi[end + 2..];
        } else {
            return false;
        }
        rest = rest.trim_start_matches(|c: char| c.is_ascii_whitespace());
    }
}

// -------------------------------------------------------------- XML name rules

/// XML 1.0 5th edition `NameStartChar`.
fn is_name_start_char(c: char) -> bool {
    matches!(c,
        ':' | '_'
        | 'A'..='Z' | 'a'..='z'
        | '\u{c0}'..='\u{d6}' | '\u{d8}'..='\u{f6}' | '\u{f8}'..='\u{2ff}'
        | '\u{370}'..='\u{37d}' | '\u{37f}'..='\u{1fff}'
        | '\u{200c}'..='\u{200d}' | '\u{2070}'..='\u{218f}'
        | '\u{2c00}'..='\u{2fef}' | '\u{3001}'..='\u{d7ff}'
        | '\u{f900}'..='\u{fdcf}' | '\u{fdf0}'..='\u{fffd}'
        | '\u{10000}'..='\u{effff}')
}

/// XML 1.0 5th edition `NameChar`.
fn is_name_char(c: char) -> bool {
    is_name_start_char(c)
        || matches!(c,
            '-' | '.' | '0'..='9' | '\u{b7}'
            | '\u{300}'..='\u{36f}' | '\u{203f}'..='\u{2040}')
}

fn is_valid_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(is_name_start_char) && chars.all(is_name_char)
}

// ------------------------------------------------------------------ entities

/// The entity table for one document: the five predefined general entities plus
/// whatever the *internal* DTD subset declared.
///
/// `external_subset` records that the doctype named a `SYSTEM` or `PUBLIC`
/// identifier crabka did not and will not fetch. libxml downgrades an undefined
/// entity to a warning in that case — it cannot know the entity is undefined
/// without reading a subset it refused to read — and `PostgreSQL` ignores
/// warnings, which is why the `DocBook` probe in `xml.sql` succeeds.
#[derive(Debug, Clone, Default)]
struct Entities {
    /// Name → replacement text, for entities declared with a literal value.
    internal: Vec<(String, String)>,
    /// Names declared `SYSTEM`/`PUBLIC`. Well defined, never resolved: a
    /// reference to one is accepted and expands to nothing.
    external: Vec<String>,
    external_subset: bool,
}

impl Entities {
    /// What `&name;` expands to. `Ok(None)` means "declared but unresolvable",
    /// which is not an error; `Err(())` means undefined.
    fn resolve(&self, name: &str) -> Result<Option<&str>, ()> {
        match name {
            "lt" => return Ok(Some("<")),
            "gt" => return Ok(Some(">")),
            "amp" => return Ok(Some("&")),
            "apos" => return Ok(Some("'")),
            "quot" => return Ok(Some("\"")),
            _ => {}
        }
        if let Some((_, value)) = self.internal.iter().find(|(n, _)| n == name) {
            return Ok(Some(value));
        }
        // An external entity is declared, so the reference is well formed; it
        // simply has no replacement text crabka is willing to obtain.
        if self.external.iter().any(|n| n == name) || self.external_subset {
            return Ok(None);
        }
        Err(())
    }

    /// Scan a doctype declaration's body for `<!ENTITY …>` declarations and for
    /// an external subset identifier.
    ///
    /// This reads the internal subset *as text*; it never opens the external
    /// one. That is the whole of crabka's DTD support, and deliberately so.
    fn absorb_doctype(&mut self, body: &str) {
        let subset_start = body.find('[');
        let head = subset_start.map_or(body, |i| &body[..i]);
        if head.contains("SYSTEM") || head.contains("PUBLIC") {
            self.external_subset = true;
        }
        let Some(start) = subset_start else { return };
        let subset = &body[start + 1..];
        let mut rest = subset;
        while let Some(at) = rest.find("<!ENTITY") {
            rest = &rest[at + 8..];
            let Some(end) = rest.find('>') else { break };
            let decl = &rest[..end];
            rest = &rest[end + 1..];
            let mut words = decl.trim_start();
            // Parameter entities (`<!ENTITY % name …>`) declare nothing a
            // general reference can name.
            if words.starts_with('%') {
                continue;
            }
            let name_end = words
                .find(|c: char| c.is_ascii_whitespace())
                .unwrap_or(words.len());
            let name = words[..name_end].to_string();
            if name.is_empty() {
                continue;
            }
            words = words[name_end..].trim_start();
            if words.starts_with("SYSTEM") || words.starts_with("PUBLIC") {
                self.external.push(name);
            } else {
                let mut p = 0;
                if let Some(value) = quoted(words, &mut p) {
                    self.internal.push((name, value));
                }
            }
        }
    }
}

// ------------------------------------------------------------------- the tree

/// One node of the parsed tree. Built only when a caller needs the *shape* of
/// the document — `XMLSERIALIZE … INDENT` and nothing else. Plain validation
/// discards it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Node {
    Element(Element),
    /// Character data with entity references already expanded, as
    /// `XML_PARSE_NOENT` leaves them.
    Text(String),
    /// A CDATA section, which survives re-serialisation as a CDATA section.
    CData(String),
    Comment(String),
    /// A processing instruction, `target data` as written.
    Pi(String),
    /// A doctype, `name [internal subset]` as written.
    DocType(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Element {
    name: String,
    /// Attribute names and their entity-expanded values, in document order.
    attrs: Vec<(String, String)>,
    children: Vec<Node>,
}

/// A parsed value: the top-level nodes, and whether it was read as a document.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Tree {
    nodes: Vec<Node>,
    as_document: bool,
}

// ------------------------------------------------------------------ the parser

/// Everything a caller can ask the parser to do, so one walk serves validation
/// and tree building alike.
struct Parser<'a> {
    input: &'a str,
    /// Drop whitespace-only text nodes, as `XML_PARSE_NOBLANKS` does. Set only
    /// when indenting, because libxml cannot indent around them otherwise.
    strip_blanks: bool,
    entities: Entities,
    faults: Faults,
}

/// One element being built, plus the source line of its start tag, which the
/// `Opening and ending tag mismatch` message names.
struct Open {
    element: Element,
    line: usize,
}

/// The byte range one tokenizer event covers.
#[derive(Debug, Clone, Copy)]
struct Span {
    before: usize,
    after: usize,
}

/// The tree under construction: finished top-level nodes and the chain of
/// elements still open.
struct Walk {
    roots: Vec<Node>,
    stack: Vec<Open>,
    seen_root: bool,
    as_document: bool,
}

impl Walk {
    /// Attach a finished node to the innermost open element, or to the top
    /// level when there is none.
    fn push(&mut self, node: Node) {
        match self.stack.last_mut() {
            Some(open) => open.element.children.push(node),
            None => self.roots.push(node),
        }
    }

    fn close(&mut self) {
        if let Some(open) = self.stack.pop() {
            self.push(Node::Element(open.element));
        }
    }

    /// Is this element a *second* root of a document? Records the fault and
    /// answers yes, so the caller can stop.
    fn second_root(&self, offset: usize, faults: &mut Faults) -> bool {
        if self.as_document && self.seen_root && self.stack.is_empty() {
            faults.push("Extra content at the end of the document", offset);
            return true;
        }
        false
    }

    /// libxml's `areBlanks` heuristic, narrowed to the cases a parser without a
    /// DTD content model can decide.
    ///
    /// A whitespace-only text node is ignorable when a tag follows it, its
    /// element already has a non-text child, and the element is not being
    /// closed immediately — which is why `<a>   </a>` keeps its spaces while
    /// `<a>   <b/>   </a>` loses both runs.
    fn blank_is_ignorable(&self, rest: &str) -> bool {
        if !rest.starts_with('<') {
            return false;
        }
        let siblings = match self.stack.last() {
            Some(open) => open.element.children.as_slice(),
            None => self.roots.as_slice(),
        };
        if siblings.is_empty() && rest.starts_with("</") {
            return false;
        }
        if matches!(siblings.last(), Some(Node::Text(_) | Node::CData(_))) {
            return false;
        }
        !matches!(siblings.first(), Some(Node::Text(_) | Node::CData(_)))
    }

    /// Unwind whatever a fault left open, so a tree is always returned.
    fn finish(mut self) -> Tree {
        while !self.stack.is_empty() {
            self.close();
        }
        Tree {
            nodes: self.roots,
            as_document: self.as_document,
        }
    }
}

impl<'a> Parser<'a> {
    fn new(input: &'a str, strip_blanks: bool) -> Self {
        Parser {
            input,
            strip_blanks,
            entities: Entities::default(),
            faults: Faults::default(),
        }
    }

    /// Walk the whole slice, building the tree and collecting faults.
    ///
    /// `as_document` selects the grammar: exactly one root element and nothing
    /// but whitespace, comments, PIs and a doctype around it, versus a fragment
    /// with no such constraint.
    fn parse(&mut self, as_document: bool) -> Tree {
        let mut reader = Reader::from_str(self.input);
        let config = reader.config_mut();
        config.check_end_names = true;
        config.allow_unmatched_ends = false;
        config.expand_empty_elements = false;
        config.trim_text(false);
        // quick-xml's own `--` scan reports a different fault than libxml's;
        // `comment` below does the check with libxml's wording instead.
        config.check_comments = false;

        let mut walk = Walk {
            roots: Vec::new(),
            stack: Vec::new(),
            seen_root: false,
            as_document,
        };

        loop {
            // The tokenizer counts in `u64`; every position is an offset into
            // `self.input`, so clamping to its length is exact on any target
            // that could hold the input in the first place.
            let offset = |position: u64| {
                usize::try_from(position)
                    .unwrap_or(self.input.len())
                    .min(self.input.len())
            };
            let before = offset(reader.buffer_position());
            let event = reader.read_event();
            let after = offset(reader.buffer_position());
            let span = Span { before, after };
            let keep_going = match event {
                Ok(Event::Eof) => false,
                Ok(Event::Start(start)) => self.open_element(&mut walk, &start, span),
                Ok(Event::Empty(start)) => self.empty_element(&mut walk, &start, span),
                Ok(Event::End(_)) => {
                    walk.close();
                    true
                }
                Ok(Event::Text(_)) => self.character_data(&mut walk, span),
                Ok(Event::CData(cdata)) => {
                    let body = String::from_utf8_lossy(cdata.as_ref()).into_owned();
                    walk.push(Node::CData(body));
                    true
                }
                Ok(Event::Comment(comment)) => {
                    self.comment(&mut walk, &comment, span);
                    true
                }
                Ok(Event::PI(pi)) => {
                    let body = String::from_utf8_lossy(pi.as_ref()).into_owned();
                    walk.push(Node::Pi(body));
                    true
                }
                Ok(Event::Decl(_)) => self.declaration(span),
                Ok(Event::DocType(doctype)) => {
                    let body = String::from_utf8_lossy(doctype.as_ref()).into_owned();
                    self.entities.absorb_doctype(&body);
                    walk.push(Node::DocType(body));
                    true
                }
                Ok(Event::GeneralRef(reference)) => {
                    let name = String::from_utf8_lossy(reference.as_ref()).into_owned();
                    let expanded = self.reference(&name, before, after);
                    if !expanded.is_empty() {
                        walk.push(Node::Text(expanded));
                    }
                    true
                }
                Err(error) => {
                    self.translate(&error, span, &walk);
                    false
                }
            };
            if !keep_going {
                break;
            }
        }

        self.check_eof(&walk);
        walk.finish()
    }

    /// The two ways a well-formed-so-far walk can still be wrong at EOF: an
    /// element left open, or a document with no root at all.
    fn check_eof(&mut self, walk: &Walk) {
        if !self.faults.is_empty() {
            return;
        }
        if let Some(open) = walk.stack.first() {
            let name = open.element.name.clone();
            let line = open.line;
            self.faults.push(
                format!("Premature end of data in tag {name} line {line}"),
                self.input.len(),
            );
        } else if walk.as_document && !walk.seen_root {
            // libxml distinguishes "there was nothing here" from "there was
            // something and it did not start a tag".
            let message = if self.input.is_empty() {
                "Document is empty"
            } else {
                "Start tag expected, '<' not found"
            };
            self.faults.push(message, self.input.len());
        }
    }

    /// A start tag. Returns whether the walk should continue.
    fn open_element(&mut self, walk: &mut Walk, start: &BytesStart<'_>, span: Span) -> bool {
        if walk.second_root(span.before, &mut self.faults) {
            return false;
        }
        let element = self.element(start, span.before);
        walk.seen_root |= walk.stack.is_empty();
        let line = self.line_of(span.before);
        walk.stack.push(Open { element, line });
        true
    }

    /// A `<x/>` tag, which opens and closes in one event.
    fn empty_element(&mut self, walk: &mut Walk, start: &BytesStart<'_>, span: Span) -> bool {
        if walk.second_root(span.before, &mut self.faults) {
            return false;
        }
        let element = self.element(start, span.before);
        walk.seen_root |= walk.stack.is_empty();
        walk.push(Node::Element(element));
        true
    }

    /// A run of character data, with its entity references already split off by
    /// the tokenizer.
    fn character_data(&mut self, walk: &mut Walk, span: Span) -> bool {
        let raw = &self.input[span.before..span.after];
        if walk.as_document
            && walk.stack.is_empty()
            && !raw
                .trim_matches(|c: char| c.is_ascii_whitespace())
                .is_empty()
        {
            // Character data outside the root is what libxml calls extra
            // content; whitespace alone around a root is allowed.
            let message = if walk.seen_root {
                "Extra content at the end of the document"
            } else {
                "Start tag expected, '<' not found"
            };
            self.faults.push(message, span.before);
            return false;
        }
        if let Some(at) = raw.find("]]>") {
            self.faults
                .push("Sequence ']]>' not allowed in content", span.before + at);
        }
        // Whitespace around a document's root element is not a node: libxml
        // discards it, which is why `XMLSERIALIZE(DOCUMENT ' <a/> ' … INDENT)`
        // is `<a/>` with no surrounding blank lines.
        if walk.as_document && walk.stack.is_empty() {
            return true;
        }
        let expanded = self.expand(raw, span.before);
        let blank = expanded
            .chars()
            .all(|c| matches!(c, ' ' | '\t' | '\r' | '\n'));
        let ignorable =
            self.strip_blanks && blank && walk.blank_is_ignorable(&self.input[span.after..]);
        if !ignorable && !expanded.is_empty() {
            walk.push(Node::Text(expanded));
        }
        true
    }

    /// A comment, which may not contain `--` however harmless the sequence
    /// looks — libxml quotes the text up to the offending pair.
    fn comment(&mut self, walk: &mut Walk, comment: &[u8], span: Span) {
        let body = String::from_utf8_lossy(comment).into_owned();
        if let Some(at) = body.find("--") {
            let head = &body[..at];
            self.faults.push(
                format!("Double hyphen within comment: <!--{head}"),
                span.before + 4 + at,
            );
        }
        walk.push(Node::Comment(body));
    }

    /// An XML declaration. Only the one at offset zero is a declaration, and
    /// the caller has already consumed and validated that one.
    fn declaration(&mut self, span: Span) -> bool {
        if span.before == 0 {
            return true;
        }
        self.faults.push(
            "XML declaration allowed only at the start of the document",
            span.before + 5,
        );
        false
    }

    fn line_of(&self, offset: usize) -> usize {
        locate(self.input, offset).0
    }

    /// Validate a start tag and turn it into an [`Element`].
    fn element(&mut self, start: &BytesStart<'_>, offset: usize) -> Element {
        let name = String::from_utf8_lossy(start.name().as_ref()).into_owned();
        if !is_valid_name(&name) {
            self.faults
                .push("StartTag: invalid element name", offset + 1);
        }
        let mut attrs: Vec<(String, String)> = Vec::new();
        // libxml's caret for an attribute-level fault sits at the end of the
        // start tag, except for an unescaped `<`, which it points straight at.
        let tag_end = offset + 1 + start.len();
        // `with_checks(false)` turns off quick-xml's own duplicate-attribute
        // scan, which is the quadratic one RUSTSEC-2026-0194 is about; the
        // names are collected here anyway, so the check rides along with the
        // single walk crabka already does.
        for attribute in start.attributes().with_checks(false) {
            let Ok(attribute) = attribute else {
                let name = self.tag_name_at(offset);
                let line = self.line_of(offset);
                self.faults.push(
                    format!("Couldn't find end of Start Tag {name} line {line}"),
                    tag_end,
                );
                break;
            };
            let key = String::from_utf8_lossy(attribute.key.as_ref()).into_owned();
            let raw = String::from_utf8_lossy(attribute.value.as_ref()).into_owned();
            if raw.contains('<') {
                // The `<` inside the value, not the one that opened the tag.
                let at = self.input[offset + 1..tag_end]
                    .find('<')
                    .map_or(tag_end, |i| offset + 1 + i);
                self.faults
                    .push("Unescaped '<' not allowed in attributes values", at);
            }
            if attrs.iter().any(|(existing, _)| *existing == key) {
                self.faults
                    .push(format!("Attribute {key} redefined"), tag_end);
            }
            let value = self.expand(&raw, tag_end);
            attrs.push((key, value));
        }
        Element {
            name,
            attrs,
            children: Vec::new(),
        }
    }

    /// Expand the entity and character references inside a run of raw text.
    fn expand(&mut self, raw: &str, offset: usize) -> String {
        if !raw.contains('&') {
            return raw.to_string();
        }
        let mut out = String::with_capacity(raw.len());
        let mut rest = raw;
        let mut at = offset;
        while let Some(amp) = rest.find('&') {
            out.push_str(&rest[..amp]);
            at += amp;
            rest = &rest[amp..];
            let Some(semi) = rest.find(';') else {
                self.faults.push("xmlParseEntityRef: no name", at + 1);
                out.push_str(rest);
                return out;
            };
            let name = &rest[1..semi];
            out.push_str(&self.reference(name, at, at + semi + 1));
            at += semi + 1;
            rest = &rest[semi + 1..];
        }
        out.push_str(rest);
        out
    }

    /// Resolve one `&name;`, recording a fault when it names nothing.
    ///
    /// `start`/`end` bracket the reference in the source; libxml's caret sits
    /// just past it.
    fn reference(&mut self, name: &str, start: usize, end: usize) -> String {
        if let Some(digits) = name.strip_prefix('#') {
            let (radix, digits, kind) = match digits.strip_prefix(['x', 'X']) {
                Some(hex) => (16, hex, "hexadecimal"),
                None => (10, digits, "decimal"),
            };
            // The caret sits on the first digit -- `&`, `#` and any `x` are
            // already consumed by the time libxml reads the value.
            let at = start + 2 + usize::from(radix == 16);
            let Ok(value) = u32::from_str_radix(digits, radix) else {
                // Two complaints, not one: libxml reports the bad digits and
                // then reports the zero it fell back to.
                self.faults
                    .push(format!("CharRef: invalid {kind} value"), at);
                self.faults
                    .push("xmlParseCharRef: invalid xmlChar value 0", at);
                return String::new();
            };
            let Some(character) = char::from_u32(value) else {
                self.faults.push(
                    format!("xmlParseCharRef: invalid xmlChar value {value}"),
                    at,
                );
                return String::new();
            };
            return character.to_string();
        }
        if name.is_empty() || !is_valid_name(name) {
            self.faults.push("xmlParseEntityRef: no name", start + 1);
            return String::new();
        }
        match self.entities.resolve(name) {
            Ok(Some(value)) => value.to_string(),
            Ok(None) => String::new(),
            Err(()) => {
                self.faults
                    .push(format!("Entity '{name}' not defined"), end);
                String::new()
            }
        }
    }

    /// Turn a tokenizer error into libxml's wording for the same fault.
    ///
    /// The tokenizer stops at the first one it cannot recover from, so at most
    /// one of these joins the faults libxml would have recovered past.
    fn translate(&mut self, error: &quick_xml::Error, span: Span, walk: &Walk) {
        use quick_xml::errors::{IllFormedError, SyntaxError};

        let open = walk
            .stack
            .last()
            .map(|open| (open.element.name.clone(), open.line));
        let (message, offset) = match error {
            quick_xml::Error::IllFormed(IllFormedError::MismatchedEndTag { expected, found }) => {
                let line = open.as_ref().map_or(1, |(_, line)| *line);
                (
                    format!("Opening and ending tag mismatch: {expected} line {line} and {found}"),
                    span.after,
                )
            }
            quick_xml::Error::IllFormed(IllFormedError::UnmatchedEndTag(found)) => (
                format!("Opening and ending tag mismatch: {found} line 1 and {found}"),
                span.after,
            ),
            // A bare `&` with no `;` anywhere after it. libxml names the
            // missing part rather than the missing terminator.
            quick_xml::Error::IllFormed(IllFormedError::UnclosedReference) => {
                ("xmlParseEntityRef: no name".to_string(), span.before + 1)
            }
            // A start tag that ran off the end of the input. libxml names the
            // tag it was reading rather than the enclosing one.
            quick_xml::Error::Syntax(
                SyntaxError::UnclosedTag
                | SyntaxError::UnclosedSingleQuotedAttributeValue
                | SyntaxError::UnclosedDoubleQuotedAttributeValue,
            ) => {
                let name = self.tag_name_at(span.before);
                let line = self.line_of(span.before);
                (
                    format!("Couldn't find end of Start Tag {name} line {line}"),
                    self.input.len(),
                )
            }
            // Everything else is some other construct that ran off the end: an
            // unterminated comment, PI, CDATA section or doctype.
            _ => match open {
                Some((name, line)) => (
                    format!("Premature end of data in tag {name} line {line}"),
                    self.input.len(),
                ),
                None => ("Start tag expected, '<' not found".to_string(), span.before),
            },
        };
        self.faults.push(message, offset);
    }

    /// The element name of the `<` at `offset`, for the messages that quote it
    /// when the tokenizer never got far enough to build a [`BytesStart`].
    fn tag_name_at(&self, offset: usize) -> &'a str {
        let rest = self.input[offset..].trim_start_matches('<');
        let end = rest.find(|c: char| !is_name_char(c)).unwrap_or(rest.len());
        &rest[..end]
    }
}

/// Run `xml_parse` over `text`, returning the tree or `PostgreSQL`'s error.
fn parse(text: &str, option: XmlOption, strip_blanks: bool) -> Result<Tree, TypeError> {
    // DOCUMENT hands the whole buffer to libxml; CONTENT validates and skips a
    // leading declaration first, and only then decides which grammar applies.
    let (body, prefix, as_document) = match option {
        // DOCUMENT hands the buffer straight to libxml, so a malformed
        // declaration is libxml's complaint rather than `parse_xml_decl`'s --
        // same fault, different reporting shape.
        XmlOption::Document => {
            if let Err(fault) = parse_xml_decl(text) {
                let mut faults = Faults::default();
                faults.push(fault.libxml_message(), decl_fault_offset(text));
                return Err(well_formedness_error(option, faults.detail(text, 0)));
            }
            (text, 0, true)
        }
        XmlOption::Content => {
            let decl = parse_xml_decl(text).map_err(|fault| TypeError::XmlSyntax {
                sqlstate: "2200N",
                message: "invalid XML content: invalid XML declaration",
                detail: fault.detail().to_string(),
            })?;
            let rest = &text[decl.len..];
            (rest, decl.len, doctype_in_content(rest))
        }
    };

    let mut parser = Parser::new(body, strip_blanks);
    let tree = parser.parse(as_document);
    if parser.faults.is_empty() {
        return Ok(tree);
    }
    Err(well_formedness_error(
        option,
        parser.faults.detail(text, prefix),
    ))
}

// ---------------------------------------------------------------- public API

/// `xml_in` / `XMLPARSE`: accept or reject `text` under `option`.
///
/// Nothing is returned because nothing is transformed — the caller keeps the
/// original bytes, which is the whole point of the type.
///
/// # Errors
///
/// `2200M` for a malformed document, `2200N` for malformed content, each
/// carrying libxml's DETAIL.
pub fn validate(text: &str, option: XmlOption) -> Result<(), TypeError> {
    parse(text, option, false).map(|_| ())
}

/// `xml_out` / `xml_out_internal`: the stored bytes with their XML declaration
/// re-rendered, which is the one way `xml` output is *not* its input.
///
/// `print_xml_decl` writes nothing for the default version with no `standalone`
/// and drops `encoding` unconditionally, so `'<?xml version="1.0"?><foo/>'::xml`
/// displays as `<foo/>` while `'<?xml version="1.1"?><foo/>'::xml` keeps its
/// declaration. Casting to `text` is a *binary* coercion in `pg_cast` and does
/// none of this — the same value is `<?xml version="1.0"?><foo/>` as `text`.
#[must_use]
pub fn output_text(stored: &str) -> String {
    let Ok(decl) = parse_xml_decl(stored) else {
        // A declaration `parse_xml_decl` rejects cannot be re-rendered, so the
        // value is printed as stored. `xml_in` would not have accepted it, but
        // `xml_out` is reachable from a binary parameter and from storage.
        return stored.to_string();
    };
    let mut out = String::new();
    let default_version = decl.version.as_deref().is_none_or(|v| v == "1.0");
    if !default_version || decl.standalone.is_some() {
        let version = decl.version.as_deref().unwrap_or("1.0");
        let _ = write!(out, "<?xml version=\"{version}\"");
        match decl.standalone {
            Some(true) => out.push_str(" standalone=\"yes\""),
            Some(false) => out.push_str(" standalone=\"no\""),
            None => {}
        }
        out.push_str("?>");
        out.push_str(&stored[decl.len..]);
        return out;
    }
    // No declaration is printed, so a newline immediately after the one that
    // was dropped would leave a blank first line. libxml eats exactly one.
    let rest = &stored[decl.len..];
    if decl.len > 0 {
        rest.strip_prefix('\n').unwrap_or(rest).to_string()
    } else {
        rest.to_string()
    }
}

/// `xml_is_document` — the `IS DOCUMENT` predicate.
///
/// Never fails: a value that does not parse as a document simply is not one.
/// The predicate can still raise, but only from coercing its operand to `xml`
/// first, which happens before this is reached.
#[must_use]
pub fn is_document(text: &str) -> bool {
    parse(text, XmlOption::Document, false).is_ok()
}

/// `xmltotext_with_options(…, indent => false)` for `XMLSERIALIZE(DOCUMENT …)`.
///
/// The result is the input, but the input must first *be* a document. CONTENT
/// without `INDENT` never reaches here: `PostgreSQL` returns the
/// binary-compatible value without parsing at all, so it succeeds even on a
/// server built without libxml.
///
/// # Errors
///
/// `2200L` `not an XML document` — a single fixed message, with no DETAIL,
/// deliberately unlike [`validate`]'s.
pub fn require_document(text: &str) -> Result<(), TypeError> {
    if is_document(text) {
        return Ok(());
    }
    Err(TypeError::Coded {
        sqlstate: "2200L",
        message: "not an XML document".to_string(),
    })
}

/// `xmltotext_with_options(…, indent => true)` — `XMLSERIALIZE … INDENT`.
///
/// Reparses with blank nodes stripped and re-serialises the tree, so the
/// result is libxml's rendering rather than the stored text: attribute quoting
/// is normalised to `"`, empty elements collapse to `<x/>`, and an internal
/// entity reference is replaced by its expansion.
///
/// # Errors
///
/// `2200L` when `option` is DOCUMENT and the value is not one.
pub fn serialize_indent(text: &str, option: XmlOption) -> Result<String, TypeError> {
    let tree = parse(text, option, true).map_err(|_| TypeError::Coded {
        sqlstate: "2200L",
        message: "not an XML document".to_string(),
    })?;
    let mut out = String::new();
    // `xmlSaveDoc` (the document path) writes character data as it stands;
    // `xmlSaveTree` (the fake-root content path) runs it through
    // `xmlEncodeEntitiesReentrant`, which turns every non-ASCII character into
    // a hexadecimal reference. Same tree, two renderings, and the only way to
    // see the difference is to serialise `<a>café</a>` both ways.
    let escape_non_ascii = !tree.as_document;
    if tree.as_document {
        // `XML_SAVE_NO_DECL` is passed unless the input carried a declaration,
        // so a declaration appears exactly when there was one to echo -- but it
        // is libxml's rendering of the parsed document, not the input's bytes.
        // `xml_parse` forces `doc->encoding` to UTF-8, which is why one appears
        // even when the input had no `encoding` pseudo-attribute.
        if let Ok(decl) = parse_xml_decl(text)
            && decl.len > 0
        {
            let version = decl.version.as_deref().unwrap_or("1.0");
            let _ = write!(out, "<?xml version=\"{version}\" encoding=\"UTF-8\"");
            match decl.standalone {
                Some(true) => out.push_str(" standalone=\"yes\""),
                Some(false) => out.push_str(" standalone=\"no\""),
                None => {}
            }
            out.push_str("?>\n");
        }
        // `xmlDocContentDumpOutput` newline-terminates every top-level node,
        // and `xmltotext_with_options` trims the last one back off -- but only
        // when the *requested* option was DOCUMENT, which is why the same value
        // serialised as CONTENT keeps a trailing blank line.
        for node in &tree.nodes {
            write_node(&mut out, node, 0, true, escape_non_ascii);
            out.push('\n');
        }
        if option == XmlOption::Document {
            while out.ends_with('\n') || out.ends_with('\r') {
                out.pop();
            }
        }
        return Ok(out);
    }

    // Non-singly-rooted content: newlines go between nodes, but never before a
    // text node -- PostgreSQL builds a fake root and iterates its children.
    let mut previous = false;
    for node in &tree.nodes {
        if previous && !matches!(node, Node::Text(_)) {
            out.push('\n');
        }
        write_node(&mut out, node, 0, true, escape_non_ascii);
        previous = true;
    }
    Ok(out)
}

/// Render one node at `depth`, two spaces per level.
fn write_node(out: &mut String, node: &Node, depth: usize, format: bool, escape_non_ascii: bool) {
    match node {
        Node::Text(text) => out.push_str(&escape_content(text, escape_non_ascii)),
        Node::CData(body) => {
            out.push_str("<![CDATA[");
            out.push_str(body);
            out.push_str("]]>");
        }
        Node::Comment(body) => {
            out.push_str("<!--");
            out.push_str(body);
            out.push_str("-->");
        }
        Node::Pi(body) => {
            out.push_str("<?");
            out.push_str(body);
            out.push_str("?>");
        }
        Node::DocType(body) => write_doctype(out, body),
        Node::Element(element) => write_element(out, element, depth, format, escape_non_ascii),
    }
}

/// `<!DOCTYPE name>`, with the internal subset broken onto its own lines the
/// way libxml's `xmlDtdDumpOutput` does.
fn write_doctype(out: &mut String, body: &str) {
    match body.find('[') {
        None => {
            out.push_str("<!DOCTYPE ");
            out.push_str(body.trim());
            out.push('>');
        }
        Some(open) => {
            let head = body[..open].trim_end();
            let subset = body[open + 1..]
                .rsplit_once(']')
                .map_or("", |(head, _)| head);
            out.push_str("<!DOCTYPE ");
            out.push_str(head);
            out.push_str(" [\n");
            let mut rest = subset.trim();
            while let Some(end) = rest.find('>') {
                out.push_str(rest[..=end].trim());
                out.push('\n');
                rest = rest[end + 1..].trim_start();
            }
            out.push_str("]>");
        }
    }
}

fn write_element(
    out: &mut String,
    element: &Element,
    depth: usize,
    format: bool,
    escape_non_ascii: bool,
) {
    out.push('<');
    out.push_str(&element.name);
    for (name, value) in &element.attrs {
        out.push(' ');
        out.push_str(name);
        out.push_str("=\"");
        out.push_str(&escape_attribute(value));
        out.push('"');
    }
    if element.children.is_empty() {
        out.push_str("/>");
        return;
    }
    out.push('>');
    // libxml indents an element's children only when none of them is character
    // data: `<val x="y">text node<val>73</val></val>` stays on one line.
    let mixed = element
        .children
        .iter()
        .any(|child| matches!(child, Node::Text(_) | Node::CData(_)));
    let format = format && !mixed;
    for child in &element.children {
        if format {
            out.push('\n');
            for _ in 0..=depth {
                out.push_str("  ");
            }
        }
        write_node(out, child, depth + 1, format, escape_non_ascii);
    }
    if format {
        out.push('\n');
        for _ in 0..depth {
            out.push_str("  ");
        }
    }
    out.push_str("</");
    out.push_str(&element.name);
    out.push('>');
}

/// `xmlEncodeEntitiesReentrant` for character data.
fn escape_content(s: &str, escape_non_ascii: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '&' => out.push_str("&amp;"),
            '\r' => out.push_str("&#13;"),
            // `xmlEncodeEntitiesReentrant` turns every non-ASCII character into
            // a hexadecimal character reference. Attribute values go through
            // `xmlAttrSerializeTxtContent` instead, which never does this — so
            // an accented character survives in an attribute and does not in a
            // text node.
            c if escape_non_ascii && !c.is_ascii() => {
                let _ = write!(out, "&#x{:X};", c as u32);
            }
            _ => out.push(c),
        }
    }
    out
}

/// The same, plus the characters that cannot survive inside `"…"`.
fn escape_attribute(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '&' => out.push_str("&amp;"),
            '"' => out.push_str("&quot;"),
            '\r' => out.push_str("&#13;"),
            '\n' => out.push_str("&#10;"),
            '\t' => out.push_str("&#9;"),
            _ => out.push(c),
        }
    }
    out
}

/// `xmlcomment(text)`: wrap the argument in `<!--` … `-->`.
///
/// The argument is not escaped, because a comment has no escapes; instead the
/// two sequences that would end it early are rejected.
///
/// # Errors
///
/// `2200S` `invalid XML comment` for an embedded `--` or a trailing `-`.
pub fn comment(arg: &str) -> Result<String, TypeError> {
    if arg.contains("--") || arg.ends_with('-') {
        return Err(TypeError::Coded {
            sqlstate: "2200S",
            message: "invalid XML comment".to_string(),
        });
    }
    Ok(format!("<!--{arg}-->"))
}

/// `xmltext(text)`: the argument as a character-data node.
///
/// `xmlEncodeSpecialChars` escapes less than [`escape_content`] does — it
/// leaves `>` alone and adds `"` — so `xmltext('a>b')` is `a>b` while a text
/// node re-serialised by `INDENT` would be `a&gt;b`.
#[must_use]
pub fn text_node(arg: &str) -> String {
    let mut out = String::with_capacity(arg.len());
    for c in arg.chars() {
        match c {
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '&' => out.push_str("&amp;"),
            '"' => out.push_str("&quot;"),
            '\r' => out.push_str("&#13;"),
            _ => out.push(c),
        }
    }
    out
}

/// `xmlconcat(…)`: join the non-null arguments, hoisting one XML declaration.
///
/// Each argument's own declaration is stripped, and a combined one is emitted
/// when every argument that had a version agreed on it, or when any argument
/// was `standalone`. That is why `xmlconcat('<foo/>', '<?xml version="1.1"
/// standalone="no"?><bar/>')` comes back with a bare `standalone="no"`
/// declaration and no version.
#[must_use]
pub fn concat(parts: &[&str]) -> String {
    // `standalone`: 1 = every part so far said yes, 0 = one said no after every
    // earlier one said yes, -1 = some part was silent. Only the first two print
    // a declaration, and the -1 arm wins as soon as any part omits the
    // pseudo-attribute -- which is why one bare `<foo/>` suppresses the whole
    // declaration however emphatic its neighbours are.
    let mut global_standalone: i8 = 1;
    let mut global_version: Option<String> = None;
    let mut version_no_value = false;
    let mut body = String::new();

    for part in parts {
        let decl = parse_xml_decl(part).unwrap_or(XmlDecl {
            len: 0,
            version: None,
            standalone: None,
        });
        if decl.standalone == Some(false) && global_standalone == 1 {
            global_standalone = 0;
        }
        if decl.standalone.is_none() {
            global_standalone = -1;
        }
        match &decl.version {
            None => version_no_value = true,
            Some(version) => match &global_version {
                None => global_version = Some(version.clone()),
                Some(existing) if existing != version => version_no_value = true,
                Some(_) => {}
            },
        }
        body.push_str(&part[decl.len..]);
    }

    if version_no_value && global_standalone < 0 {
        return body;
    }
    // `print_xml_decl` writes nothing at all for the default version with no
    // standalone, so a concatenation of two plain `1.0` documents stays plain.
    let version = if version_no_value {
        None
    } else {
        global_version.as_deref()
    };
    if version.is_none_or(|version| version == "1.0") && global_standalone == -1 {
        return body;
    }
    let mut out = String::from("<?xml");
    let _ = write!(out, " version=\"{}\"", version.unwrap_or("1.0"));
    if global_standalone == 1 {
        out.push_str(" standalone=\"yes\"");
    } else if global_standalone == 0 {
        out.push_str(" standalone=\"no\"");
    }
    out.push_str("?>");
    out.push_str(&body);
    out
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    /// The security property this module exists to hold: a `SYSTEM` entity is
    /// declared, referenced, and never resolved — no file is opened, no error
    /// mentions the filesystem, and the value round-trips verbatim.
    #[test]
    fn external_entities_are_declared_but_never_resolved() {
        let cases = [
            r#"<!DOCTYPE foo [<!ENTITY c SYSTEM "/etc/passwd">]><foo>&c;</foo>"#,
            r#"<!DOCTYPE foo [<!ENTITY c SYSTEM "/etc/no.such.file">]><foo>&c;</foo>"#,
            r#"<!DOCTYPE foo [<!ENTITY c SYSTEM "file:///etc/shadow">]><foo>&c;</foo>"#,
            r#"<!DOCTYPE foo [<!ENTITY c SYSTEM "http://127.0.0.1:1/x">]><foo>&c;</foo>"#,
        ];
        for input in cases {
            assert!(validate(input, XmlOption::Document) == Ok(()), "{input}");
            assert!(validate(input, XmlOption::Content) == Ok(()), "{input}");
        }
        // The external DTD probe: an entity the unread subset might define is
        // accepted rather than reported undefined.
        let docbook = r#"<!DOCTYPE chapter PUBLIC "-//OASIS//DTD DocBook XML V4.1.2//EN" "http://www.oasis-open.org/docbook/xml/4.1.2/docbookx.dtd"><chapter>&nbsp;</chapter>"#;
        assert!(validate(docbook, XmlOption::Document) == Ok(()));

        // The reference expands to nothing, so even a serialising path that
        // rebuilds the tree cannot leak the file's contents.
        let indented =
            serialize_indent(cases[0], XmlOption::Document).expect("indent the XXE probe");
        assert!(!indented.contains("root:"), "{indented}");
        assert!(indented.ends_with("<foo/>"), "{indented}");
    }

    /// An entity nothing declares is still an error — the permissiveness above
    /// is scoped to references the document itself declared.
    #[test]
    fn an_undeclared_entity_is_rejected() {
        let error = validate(
            "<undefinedentity>&idontexist;</undefinedentity>",
            XmlOption::Content,
        )
        .expect_err("undefined entity");
        assert!(error.sqlstate() == "2200N");
        assert!(
            error.detail().as_deref()
                == Some(
                    caret_detail(
                        "Entity 'idontexist' not defined",
                        "<undefinedentity>&idontexist;</undefinedentity>",
                        29,
                    )
                    .as_str()
                )
        );
        // An internally declared one is fine, and expands on the INDENT path.
        let declared = r#"<!DOCTYPE foo [<!ENTITY c "hi">]><foo>&c;</foo>"#;
        assert!(validate(declared, XmlOption::Document) == Ok(()));
        assert!(
            serialize_indent(declared, XmlOption::Document)
                .expect("indent")
                .ends_with("<foo>hi</foo>")
        );
    }

    /// `line 1: <message>`, the offending line, and a caret under `column`
    /// (0-based) — the shape `xml_errorHandler` writes into every DETAIL.
    fn caret_detail(message: &str, line: &str, column: usize) -> String {
        format!("line 1: {message}\n{line}\n{: <column$}^", "")
    }

    #[test]
    fn document_and_content_accept_different_shapes() {
        let cases = [
            // (input, valid as content, valid as document)
            ("", true, false),
            ("  ", true, false),
            ("abc", true, false),
            ("<abc>x</abc>", true, true),
            ("<a/><b/>", true, false),
            (" <a/> ", true, true),
            ("<!DOCTYPE a><a/>", true, true),
            // A doctype promotes content to the document grammar, so a second
            // root is rejected under BOTH options.
            ("<!DOCTYPE a><a/><b/>", false, false),
            ("<nosuchprefix:tag/>", true, true),
            ("<invalidns xmlns='&lt;'/>", true, true),
            ("<wrong", false, false),
            ("<123/>", false, false),
        ];
        for (input, content_ok, document_ok) in cases {
            assert!(
                validate(input, XmlOption::Content).is_ok() == content_ok,
                "content {input:?}"
            );
            assert!(
                validate(input, XmlOption::Document).is_ok() == document_ok,
                "document {input:?}"
            );
            assert!(is_document(input) == document_ok, "is_document {input:?}");
        }
    }

    #[test]
    fn a_bad_xml_declaration_is_its_own_error() {
        let error = validate(
            r#"<?xml version="1.0" standalone="y"?><foo/>"#,
            XmlOption::Content,
        )
        .expect_err("bad standalone");
        assert!(error.sqlstate() == "2200N");
        assert!(error.to_string() == "invalid XML content: invalid XML declaration");
        assert!(error.detail().as_deref() == Some("standalone accepts only 'yes' or 'no'."));
    }

    #[test]
    fn indenting_renders_the_tree_rather_than_the_stored_text() {
        let cases = [
            (
                "<foo><bar><val x=\"y\">42</val></bar></foo>",
                XmlOption::Document,
                "<foo>\n  <bar>\n    <val x=\"y\">42</val>\n  </bar>\n</foo>",
            ),
            (
                "<foo>   <bar></bar>    </foo>",
                XmlOption::Document,
                "<foo>\n  <bar/>\n</foo>",
            ),
            (
                "text node<foo>    <bar></bar>   </foo>",
                XmlOption::Content,
                "text node\n<foo>\n  <bar/>\n</foo>",
            ),
            (
                "<foo>73</foo><bar><val x=\"y\">42</val></bar>",
                XmlOption::Content,
                "<foo>73</foo>\n<bar>\n  <val x=\"y\">42</val>\n</bar>",
            ),
            // Mixed content is never broken across lines.
            (
                "<foo><bar><val x=\"y\">42</val><val x=\"y\">text node<val>73</val></val></bar></foo>",
                XmlOption::Content,
                concat!(
                    "<foo>\n  <bar>\n    <val x=\"y\">42</val>\n",
                    "    <val x=\"y\">text node<val>73</val></val>\n  </bar>\n</foo>"
                ),
            ),
            // Character data is escaped on the content path and not on the
            // document path -- the same tree, two libxml serialisers.
            ("<a>café</a>", XmlOption::Content, "<a>caf&#xE9;</a>"),
            ("<a>café</a>", XmlOption::Document, "<a>café</a>"),
            ("<a b=\"café\"/>", XmlOption::Document, "<a b=\"café\"/>"),
            ("", XmlOption::Content, ""),
            ("  ", XmlOption::Content, "  "),
            // Single quotes become double, and the entity is expanded.
            (
                "<a b='1' c=\"2&amp;3\">t&lt;x</a>",
                XmlOption::Content,
                "<a b=\"1\" c=\"2&amp;3\">t&lt;x</a>",
            ),
        ];
        for (input, option, expected) in cases {
            assert!(
                serialize_indent(input, option).as_deref() == Ok(expected),
                "{input:?}"
            );
        }
        // DOCUMENT keeps the declaration it was given and trims the trailing
        // newline; CONTENT drops the declaration and keeps the newline.
        let declared =
            r#"<?xml version="1.0" encoding="UTF-8"?><foo><bar><val>73</val></bar></foo>"#;
        assert!(
            serialize_indent(declared, XmlOption::Document).as_deref()
                == Ok(concat!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
                    "<foo>\n  <bar>\n    <val>73</val>\n  </bar>\n</foo>"
                ))
        );
        assert!(
            serialize_indent(declared, XmlOption::Content).as_deref()
                == Ok("<foo>\n  <bar>\n    <val>73</val>\n  </bar>\n</foo>")
        );
        assert!(
            serialize_indent("<!DOCTYPE a><a/>", XmlOption::Document).as_deref()
                == Ok("<!DOCTYPE a>\n<a/>")
        );
        assert!(
            serialize_indent("<!DOCTYPE a><a/>", XmlOption::Content).as_deref()
                == Ok("<!DOCTYPE a>\n<a/>\n")
        );
        // A non-document under DOCUMENT is 2200L, not a syntax error.
        let error = serialize_indent("<foo/><bar/>", XmlOption::Document).expect_err("two roots");
        assert!(error.sqlstate() == "2200L");
        assert!(error.to_string() == "not an XML document");
    }

    #[test]
    fn comment_text_and_concat_match_their_functions() {
        assert!(comment("test").as_deref() == Ok("<!--test-->"));
        assert!(comment("-test").as_deref() == Ok("<!---test-->"));
        assert!(comment("te st").as_deref() == Ok("<!--te st-->"));
        for bad in ["--test", "test-"] {
            let error = comment(bad).expect_err(bad);
            assert!(error.sqlstate() == "2200S");
            assert!(error.to_string() == "invalid XML comment");
        }

        // `xmlEncodeSpecialChars` escapes the quote and leaves `>` alone.
        assert!(text_node("foo & <bar>") == "foo &amp; &lt;bar&gt;");
        assert!(text_node("a\"b") == "a&quot;b");

        assert!(concat(&["hello", "you"]) == "helloyou");
        // One part with no declaration silences the merged one entirely, even
        // though the other part is emphatically `standalone="no"`.
        assert!(
            concat(&["<foo/>", r#"<?xml version="1.1" standalone="no"?><bar/>"#]) == "<foo/><bar/>"
        );
        // With every part declared, the agreed version survives -- and the
        // `standalone` of a later part does not, because the first part's
        // silence already forced the "unknown" state.
        assert!(
            concat(&[
                r#"<?xml version="1.1"?><foo/>"#,
                r#"<?xml version="1.1" standalone="no"?><bar/>"#
            ]) == r#"<?xml version="1.1"?><foo/><bar/>"#
        );
        // A default version with nothing to say prints no declaration at all.
        assert!(concat(&[r#"<?xml version="1.0"?><foo/>"#, "<bar/>"]) == "<foo/><bar/>");
    }

    #[test]
    fn malformed_values_carry_libxmls_wording() {
        let cases = [
            // (input, option, libxml message, caret column)
            (
                "<invalidentity>&</invalidentity>",
                XmlOption::Content,
                "xmlParseEntityRef: no name",
                16,
            ),
            (
                "<a b=\"1\" b=\"2\"/>",
                XmlOption::Content,
                "Attribute b redefined",
                14,
            ),
            (
                "<a>]]></a>",
                XmlOption::Content,
                "Sequence ']]>' not allowed in content",
                3,
            ),
            (
                "<123/>",
                XmlOption::Content,
                "StartTag: invalid element name",
                1,
            ),
            (
                "abc",
                XmlOption::Document,
                "Start tag expected, '<' not found",
                0,
            ),
            (
                "<a/><b/>",
                XmlOption::Document,
                "Extra content at the end of the document",
                4,
            ),
            (
                "<a b=\"<\"/>",
                XmlOption::Content,
                "Unescaped '<' not allowed in attributes values",
                6,
            ),
        ];
        for (input, option, message, column) in cases {
            let error = validate(input, option).expect_err(input);
            assert!(error.sqlstate() == option.sqlstate(), "{input:?}");
            assert!(error.to_string() == option.message(), "{input:?}");
            assert!(
                error.detail().as_deref() == Some(caret_detail(message, input, column).as_str()),
                "{input:?}"
            );
        }
    }

    /// libxml keeps going after a recoverable fault, so one value can report
    /// two complaints and `PostgreSQL` prints both.
    #[test]
    fn recoverable_faults_accumulate() {
        let error = validate("<twoerrors>&idontexist;</unbalanced>", XmlOption::Content)
            .expect_err("two faults");
        let input = "<twoerrors>&idontexist;</unbalanced>";
        let expected = format!(
            "{}\n{}",
            caret_detail("Entity 'idontexist' not defined", input, 23),
            caret_detail(
                "Opening and ending tag mismatch: twoerrors line 1 and unbalanced",
                input,
                36,
            ),
        );
        assert!(error.detail().as_deref() == Some(expected.as_str()));
    }

    /// The tokenizer never opens a file or a socket, so a document that only
    /// *mentions* one costs nothing to reject or accept.
    #[test]
    fn parsing_is_linear_in_the_input() {
        let deep = format!("{}{}", "<a>".repeat(200), "</a>".repeat(200));
        assert!(validate(&deep, XmlOption::Document) == Ok(()));
        let wide: String = std::iter::repeat_n("<a b=\"1\"/>", 5_000).collect();
        assert!(validate(&wide, XmlOption::Content) == Ok(()));
    }
}
