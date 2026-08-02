//! Lexical tokens for the SP2 SQL slice.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Token {
    Ident(String),
    Keyword(Keyword),
    IntLit(String),
    /// SP30: a decimal/exponent numeric literal (`1.5`, `.5`, `2.`, `1e10`), typed
    /// `float8` by the executor (crabgresql has no `numeric`).
    FloatLit(String),
    StringLit(String),
    LParen,
    RParen,
    Comma,
    Semicolon,
    Star,
    Plus,
    Minus,
    Slash,
    /// SP29: the `||` string-concatenation operator.
    Concat,
    /// SP31: the `::` cast operator (`expr::type`).
    TypeCast,
    /// A lone `:`. Only ever appears inside an array slice (`a[1:2]`), which the
    /// parser refuses with a clear "array slices are not supported" error; the
    /// two-byte `::` is claimed first by maximal munch.
    Colon,
    /// `[` — array literal / subscript opener, and the `int[]` type suffix.
    LBracket,
    /// `]`
    RBracket,
    /// The jsonb/array `->` operator (element by key or index).
    JsonGet,
    /// The jsonb `->>` operator (element as text).
    JsonGetText,
    /// The jsonb `#>` operator (element at a text path).
    JsonGetPath,
    /// The jsonb `#>>` operator (element at a text path, as text).
    JsonGetPathText,
    /// The jsonb/array `@>` containment operator.
    Contains,
    /// The jsonb/array `<@` "contained by" operator.
    ContainedBy,
    /// The jsonb `?` key-existence operator.
    KeyExists,
    /// The jsonb `?|` "any key exists" operator.
    KeyExistsAny,
    /// The jsonb `?&` "all keys exist" operator.
    KeyExistsAll,
    /// The array `&&` overlap operator.
    Overlaps,
    /// Range does not extend right/left and adjacency operators.
    DoesNotExtendRight,
    DoesNotExtendLeft,
    Adjacent,
    /// `<->` — adjacent-lexeme phrase query composition.
    Phrase,
    /// `!!` — prefix tsquery negation.
    TsNot,
    /// `~` — POSIX regex match when infix, bitwise NOT when prefix. One token for
    /// both: `PostgreSQL` spells them identically and only position tells them
    /// apart, so the parser (not the lexer) picks the reading.
    Tilde,
    /// `~*` — case-insensitive POSIX regex match.
    TildeCi,
    /// `!~` — negated POSIX regex match.
    NotTilde,
    /// `!~*` — negated case-insensitive POSIX regex match.
    NotTildeCi,
    /// `&` — bitwise AND on integers.
    Amp,
    /// `|` — bitwise OR on integers.
    Pipe,
    /// `#` — bitwise XOR on integers (NOT exponentiation; `PostgreSQL` spells
    /// XOR `#` and exponentiation `^`).
    Hash,
    /// `<<` — bitwise left shift.
    Shl,
    /// `>>` — bitwise right shift.
    Shr,
    /// `^` — exponentiation (`2^3` is 8). Left-associative in `PostgreSQL`.
    Caret,
    /// `%` — modulo (integer/numeric remainder).
    Percent,
    /// `@` — prefix absolute value.
    At,
    /// `@?` — does the jsonpath on the right find any item in the jsonb on the left?
    JsonPathExists,
    /// `@@` — the jsonpath predicate on the right, checked against the jsonb on the left.
    JsonPathMatch,
    /// `|/` — prefix square root (`float8`).
    SquareRoot,
    /// `||/` — prefix cube root (`float8`).
    CubeRoot,
    /// SP33: the `.` qualified-name separator (`a.col`). Only lexed when it does
    /// NOT begin a number lexeme — `.5` / `2.` stay a single `FloatLit`.
    Dot,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Param(u32),
    Eof,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keyword {
    Create,
    Unique,
    Index,
    Global,
    Local,
    Table,
    View,
    Drop,
    Insert,
    Copy,
    Into,
    Values,
    Select,
    With,
    Recursive,
    From,
    Where,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    And,
    Or,
    Not,
    True,
    False,
    Null,
    As,
    // SP4: transaction control + DML
    Begin,
    Start,
    Transaction,
    Commit,
    End,
    Rollback,
    Abort,
    Update,
    Set,
    Delete,
    Isolation,
    Level,
    Read,
    Committed,
    Repeatable,
    // SP6: row-level locking
    For,
    Share,
    // SP27: aggregates + grouping
    Group,
    Having,
    Distinct,
    All,
    // SP28: predicate + conditional expression breadth
    Is,
    In,
    Between,
    Like,
    Ilike,
    Case,
    When,
    Then,
    Else,
    Offset,
    // SP31: explicit casts
    Cast,
    // SP33: joins
    Join,
    Inner,
    Left,
    Right,
    Full,
    Outer,
    Cross,
    On,
    Using,
    Natural,
    // SP34: subqueries
    Exists,
    Any,
    Some,
    // SP38: set operations
    Union,
    Intersect,
    Except,
    // SP40: FDW DDL keywords
    Foreign,
    Data,
    Wrapper,
    Server,
    Mapping,
    User,
    Options,
    Import,
    Schema,
    To,
    If,
    CurrentUser,
    Public,
    Returning,
    /// The `ARRAY[...]` constructor. Reserved in `PostgreSQL`, so it is a keyword
    /// rather than a soft identifier.
    Array,
}

impl Keyword {
    #[must_use]
    pub fn from_word(w: &str) -> Option<Keyword> {
        Some(match w {
            "create" => Keyword::Create,
            "unique" => Keyword::Unique,
            "index" => Keyword::Index,
            "global" => Keyword::Global,
            "local" => Keyword::Local,
            "table" => Keyword::Table,
            "view" => Keyword::View,
            "drop" => Keyword::Drop,
            "insert" => Keyword::Insert,
            "copy" => Keyword::Copy,
            "into" => Keyword::Into,
            "values" => Keyword::Values,
            "select" => Keyword::Select,
            "with" => Keyword::With,
            "recursive" => Keyword::Recursive,
            "from" => Keyword::From,
            "where" => Keyword::Where,
            "order" => Keyword::Order,
            "by" => Keyword::By,
            "asc" => Keyword::Asc,
            "desc" => Keyword::Desc,
            "limit" => Keyword::Limit,
            "and" => Keyword::And,
            "or" => Keyword::Or,
            "not" => Keyword::Not,
            "true" => Keyword::True,
            "false" => Keyword::False,
            "null" => Keyword::Null,
            "as" => Keyword::As,
            // SP4: transaction control + DML
            "begin" => Keyword::Begin,
            "start" => Keyword::Start,
            "transaction" => Keyword::Transaction,
            "commit" => Keyword::Commit,
            "end" => Keyword::End,
            "rollback" => Keyword::Rollback,
            "abort" => Keyword::Abort,
            "update" => Keyword::Update,
            "set" => Keyword::Set,
            "delete" => Keyword::Delete,
            "isolation" => Keyword::Isolation,
            "level" => Keyword::Level,
            "read" => Keyword::Read,
            "committed" => Keyword::Committed,
            "repeatable" => Keyword::Repeatable,
            // SP6: row-level locking
            "for" => Keyword::For,
            "share" => Keyword::Share,
            // SP27: aggregates + grouping
            "group" => Keyword::Group,
            "having" => Keyword::Having,
            "distinct" => Keyword::Distinct,
            "all" => Keyword::All,
            // SP28: predicate + conditional expression breadth
            "is" => Keyword::Is,
            "in" => Keyword::In,
            "between" => Keyword::Between,
            "like" => Keyword::Like,
            "ilike" => Keyword::Ilike,
            "case" => Keyword::Case,
            "when" => Keyword::When,
            "then" => Keyword::Then,
            "else" => Keyword::Else,
            "offset" => Keyword::Offset,
            // SP31: explicit casts
            "cast" => Keyword::Cast,
            // SP33: joins
            "join" => Keyword::Join,
            "inner" => Keyword::Inner,
            "left" => Keyword::Left,
            "right" => Keyword::Right,
            "full" => Keyword::Full,
            "outer" => Keyword::Outer,
            "cross" => Keyword::Cross,
            "on" => Keyword::On,
            "using" => Keyword::Using,
            "natural" => Keyword::Natural,
            // SP34: subqueries
            "exists" => Keyword::Exists,
            "any" => Keyword::Any,
            "some" => Keyword::Some,
            // SP38: set operations
            "union" => Keyword::Union,
            "intersect" => Keyword::Intersect,
            "except" => Keyword::Except,
            // SP40: FDW DDL keywords
            "foreign" => Keyword::Foreign,
            "data" => Keyword::Data,
            "wrapper" => Keyword::Wrapper,
            "server" => Keyword::Server,
            "mapping" => Keyword::Mapping,
            "user" => Keyword::User,
            "options" => Keyword::Options,
            "import" => Keyword::Import,
            "schema" => Keyword::Schema,
            "to" => Keyword::To,
            "if" => Keyword::If,
            "current_user" => Keyword::CurrentUser,
            "public" => Keyword::Public,
            "returning" => Keyword::Returning,
            "array" => Keyword::Array,
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_word_round_trips_every_keyword() {
        // Every keyword must map from its lowercase spelling; a dropped arm would
        // silently demote that word to an identifier (e.g. `ASC` parsed as a column).
        let pairs: &[(&str, Keyword)] = &[
            ("create", Keyword::Create),
            ("unique", Keyword::Unique),
            ("index", Keyword::Index),
            ("global", Keyword::Global),
            ("local", Keyword::Local),
            ("table", Keyword::Table),
            ("view", Keyword::View),
            ("drop", Keyword::Drop),
            ("insert", Keyword::Insert),
            ("copy", Keyword::Copy),
            ("into", Keyword::Into),
            ("values", Keyword::Values),
            ("select", Keyword::Select),
            ("with", Keyword::With),
            ("recursive", Keyword::Recursive),
            ("from", Keyword::From),
            ("where", Keyword::Where),
            ("order", Keyword::Order),
            ("by", Keyword::By),
            ("asc", Keyword::Asc),
            ("desc", Keyword::Desc),
            ("limit", Keyword::Limit),
            ("and", Keyword::And),
            ("or", Keyword::Or),
            ("not", Keyword::Not),
            ("true", Keyword::True),
            ("false", Keyword::False),
            ("null", Keyword::Null),
            ("as", Keyword::As),
            ("begin", Keyword::Begin),
            ("start", Keyword::Start),
            ("transaction", Keyword::Transaction),
            ("commit", Keyword::Commit),
            ("end", Keyword::End),
            ("rollback", Keyword::Rollback),
            ("abort", Keyword::Abort),
            ("update", Keyword::Update),
            ("set", Keyword::Set),
            ("delete", Keyword::Delete),
            ("isolation", Keyword::Isolation),
            ("level", Keyword::Level),
            ("read", Keyword::Read),
            ("committed", Keyword::Committed),
            ("repeatable", Keyword::Repeatable),
            ("for", Keyword::For),
            ("share", Keyword::Share),
            ("group", Keyword::Group),
            ("having", Keyword::Having),
            ("distinct", Keyword::Distinct),
            ("all", Keyword::All),
            ("cast", Keyword::Cast),
            // SP33: joins
            ("join", Keyword::Join),
            ("inner", Keyword::Inner),
            ("left", Keyword::Left),
            ("right", Keyword::Right),
            ("full", Keyword::Full),
            ("outer", Keyword::Outer),
            ("cross", Keyword::Cross),
            ("on", Keyword::On),
            ("using", Keyword::Using),
            ("natural", Keyword::Natural),
            ("exists", Keyword::Exists),
            ("any", Keyword::Any),
            ("some", Keyword::Some),
            ("union", Keyword::Union),
            ("intersect", Keyword::Intersect),
            ("except", Keyword::Except),
            // SP40: FDW DDL keywords
            ("foreign", Keyword::Foreign),
            ("data", Keyword::Data),
            ("wrapper", Keyword::Wrapper),
            ("server", Keyword::Server),
            ("mapping", Keyword::Mapping),
            ("user", Keyword::User),
            ("options", Keyword::Options),
            ("import", Keyword::Import),
            ("schema", Keyword::Schema),
            ("to", Keyword::To),
            ("if", Keyword::If),
            ("current_user", Keyword::CurrentUser),
            ("public", Keyword::Public),
            ("returning", Keyword::Returning),
            ("array", Keyword::Array),
        ];
        for (word, kw) in pairs {
            assert_eq!(Keyword::from_word(word), Some(*kw), "from_word({word:?})");
        }
        // A non-keyword is an identifier, not a keyword.
        assert_eq!(Keyword::from_word("widget"), None);
        assert_eq!(Keyword::from_word("ascending"), None);
    }
}
