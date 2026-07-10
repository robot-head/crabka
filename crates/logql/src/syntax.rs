use crate::{
    ComparisonOp, DestinationLabel, DurationNanos, FieldFilter, FieldFilterExpression,
    FieldFilterLogicOp, FieldValue, IpMatcher, JsonExpressionPath, JsonExtraction,
    JsonParserConfig, LabelFormat, LabelFormatAssignment, LabelMatcher, LabelSelection,
    LabelSelectionSet, LineFilter, LineFilterOp, LineFormat, LogfmtExtraction, LogfmtParserConfig,
    MatchOp, OffsetNanos, ParseError, ParserStage, PatternParser, PipelineStage,
    QuantileDenominator, QuantileNumerator, RegexpParser, SourceLabel, StreamQuery,
    UnwrapExpression,
    filters::field_filter_expression_to_pipeline_stage,
    util::{
        QuotedChar, decode_quoted_escape, duration_unit, gcd_u64, is_ident_char, is_ident_start,
        parse_bytes_literal, parse_prometheus_duration_literal,
    },
};

#[tracing::instrument(level = "info", skip_all, fields(query = %input), err)]
pub fn parse_query(input: &str) -> Result<StreamQuery, ParseError> {
    Parser::new(input).parse()
}

#[tracing::instrument(level = "info", skip_all, fields(query = %input), err)]
pub fn parse_metric_query(input: &str) -> Result<MetricQuery, ParseError> {
    Parser::new(input).parse_metric()
}

pub fn parse_metric_label_replace_query(input: &str) -> Result<MetricLabelReplace, ParseError> {
    Parser::new(input).parse_metric_label_replace()
}

pub fn parse_metric_label_join_query(input: &str) -> Result<MetricLabelJoin, ParseError> {
    Parser::new(input).parse_metric_label_join()
}

pub fn parse_metric_scalar_comparison_query(
    input: &str,
) -> Result<MetricScalarComparison, ParseError> {
    Parser::new(input).parse_metric_scalar_comparison()
}

pub fn parse_metric_scalar_arithmetic_query(
    input: &str,
) -> Result<MetricScalarArithmetic, ParseError> {
    Parser::new(input).parse_metric_scalar_arithmetic()
}

pub fn parse_metric_binary_arithmetic_query(
    input: &str,
) -> Result<MetricBinaryArithmetic, ParseError> {
    Parser::new(input).parse_metric_binary_arithmetic()
}

pub fn parse_metric_binary_comparison_query(
    input: &str,
) -> Result<MetricBinaryComparison, ParseError> {
    Parser::new(input).parse_metric_binary_comparison()
}

pub fn parse_metric_binary_set_query(input: &str) -> Result<MetricBinarySet, ParseError> {
    Parser::new(input).parse_metric_binary_set()
}

fn parse_metric_subexpression(input: &str) -> Result<MetricQuery, ParseError> {
    Parser::new(strip_outer_metric_parentheses(input)).parse_metric()
}

fn strip_outer_metric_parentheses(input: &str) -> &str {
    let mut trimmed = input.trim();
    while let Some(inner) = outer_metric_parentheses_inner(trimmed) {
        if inner.len() >= trimmed.len() {
            break;
        }
        trimmed = inner.trim();
    }
    trimmed
}

fn outer_metric_parentheses_inner(input: &str) -> Option<&str> {
    let mut chars = input.char_indices();
    if chars.next()?.1 != '(' {
        return None;
    }

    let mut depth = 0usize;
    let mut quote_delimiter: Option<char> = None;
    let mut escaped = false;
    for (index, ch) in input.char_indices() {
        if let Some(delimiter) = quote_delimiter {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if delimiter.eq(&ch) {
                quote_delimiter = None;
            }
            continue;
        }

        match ch {
            '"' | '`' => quote_delimiter = Some(ch),
            '(' => depth = depth.saturating_add(1),
            ')' => {
                depth = depth.checked_sub(1)?;
                if matches!(depth, 0) {
                    let close_end = index.saturating_add(ch.len_utf8());
                    if matches!(close_end.cmp(&input.len()), std::cmp::Ordering::Equal) {
                        return Some(&input[1..index]);
                    }
                    return None;
                }
            }
            _ => {}
        }
    }

    None
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricQuery {
    pub aggregation: RangeAggregation,
    pub vector_aggregation: Option<VectorAggregation>,
    pub range_grouping: Option<VectorGrouping>,
    pub stream: StreamQuery,
    pub range_ns: DurationNanos,
    pub offset_ns: OffsetNanos,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricLabelReplace {
    pub query: MetricQuery,
    pub destination_label: String,
    pub replacement: String,
    pub source_label: String,
    pub pattern: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricLabelJoin {
    pub query: MetricQuery,
    pub destination_label: String,
    pub separator: String,
    pub source_labels: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricScalarComparison {
    pub query: MetricQuery,
    pub op: ComparisonOp,
    pub bool_modifier: bool,
    pub scalar: String,
    pub scalar_on_left: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricScalarArithmetic {
    pub query: MetricQuery,
    pub op: MetricScalarArithmeticOp,
    pub scalar: String,
    pub scalar_on_left: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricBinaryArithmetic {
    pub left: MetricQuery,
    pub op: MetricScalarArithmeticOp,
    pub matching: Option<MetricVectorMatching>,
    pub right: MetricQuery,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricBinaryComparison {
    pub left: MetricQuery,
    pub op: ComparisonOp,
    pub bool_modifier: bool,
    pub matching: Option<MetricVectorMatching>,
    pub right: MetricQuery,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricBinarySet {
    pub left: MetricQuery,
    pub op: MetricBinarySetOp,
    pub matching: Option<MetricVectorMatching>,
    pub right: MetricQuery,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetricVectorMatching {
    On {
        labels: Vec<String>,
        group: Option<MetricVectorGroupModifier>,
    },
    Ignoring {
        labels: Vec<String>,
        group: Option<MetricVectorGroupModifier>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetricVectorGroupModifier {
    Left(Vec<String>),
    Right(Vec<String>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricBinarySetOp {
    And,
    Or,
    Unless,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricScalarArithmeticOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Power,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RangeAggregation {
    CountOverTime,
    Rate,
    RateCounter,
    BytesRate,
    BytesOverTime,
    AbsentOverTime,
    PresentOverTime,
    SumOverTime,
    AvgOverTime,
    StdvarOverTime,
    StddevOverTime,
    QuantileOverTime(Quantile),
    MinOverTime,
    MaxOverTime,
    FirstOverTime,
    LastOverTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Quantile {
    pub numerator: QuantileNumerator,
    pub denominator: QuantileDenominator,
}

enum RangeAggregationKind {
    Standard(RangeAggregation),
    QuantileOverTime,
}

fn range_aggregation_supports_grouping(aggregation: &RangeAggregation) -> bool {
    matches!(
        aggregation,
        RangeAggregation::AvgOverTime
            | RangeAggregation::StdvarOverTime
            | RangeAggregation::StddevOverTime
            | RangeAggregation::QuantileOverTime(_)
            | RangeAggregation::MinOverTime
            | RangeAggregation::MaxOverTime
            | RangeAggregation::FirstOverTime
            | RangeAggregation::LastOverTime
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorAggregation {
    pub op: VectorAggregationOp,
    pub grouping: Option<VectorGrouping>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VectorAggregationOp {
    Sum,
    Count,
    Min,
    Max,
    Avg,
    Stddev,
    Stdvar,
    CountValues(String),
    TopK(u64),
    BottomK(u64),
    ApproxTopK(u64),
    Sort,
    SortDesc,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VectorGrouping {
    By(Vec<String>),
    Without(Vec<String>),
}

#[derive(Clone, Debug, PartialEq)]

struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn parse(mut self) -> Result<StreamQuery, ParseError> {
        let query = self.parse_stream_query(false)?;
        self.skip_ws();
        if self.pos == self.input.len() {
            Ok(query)
        } else {
            Err(self.error("expected end of query"))
        }
    }

    fn parse_metric(mut self) -> Result<MetricQuery, ParseError> {
        self.skip_ws();
        let mut vector_aggregation = self.try_parse_vector_aggregation()?;
        let prefix_grouping = vector_aggregation
            .as_ref()
            .and_then(|aggregation| aggregation.grouping.clone());
        if vector_aggregation.is_some() {
            self.expect('(')?;
            if let Some(vector_aggregation) = &mut vector_aggregation {
                self.parse_vector_aggregation_parameter(&mut vector_aggregation.op)?;
            }
        }
        let aggregation_kind = self.parse_range_aggregation()?;
        self.expect('(')?;
        let aggregation = match aggregation_kind {
            RangeAggregationKind::Standard(aggregation) => aggregation,
            RangeAggregationKind::QuantileOverTime => {
                let quantile = self.parse_quantile()?;
                self.skip_ws();
                self.expect(',')?;
                RangeAggregation::QuantileOverTime(quantile)
            }
        };
        let (stream, range_ns, offset_ns) = self.parse_metric_range_stream_query()?;
        self.expect(')')?;
        let vector_aggregation = if let Some(mut vector_aggregation) = vector_aggregation {
            self.expect(')')?;
            let suffix_grouping = self.try_parse_vector_grouping()?;
            if prefix_grouping.is_some() && suffix_grouping.is_some() {
                return Err(self.error("expected only one vector grouping clause"));
            }
            vector_aggregation.grouping = suffix_grouping.or(prefix_grouping);
            if matches!(vector_aggregation.op, VectorAggregationOp::ApproxTopK(_))
                && vector_aggregation.grouping.is_some()
            {
                return Err(self.error("approx_topk does not support grouping"));
            }
            Some(vector_aggregation)
        } else {
            None
        };
        let range_grouping = if vector_aggregation.is_none() {
            let grouping = self.try_parse_vector_grouping()?;
            if grouping.is_some() && !range_aggregation_supports_grouping(&aggregation) {
                return Err(self.error("range aggregation does not support grouping"));
            }
            grouping
        } else {
            None
        };
        self.skip_ws();
        if self.pos != self.input.len() {
            return Err(self.error("expected end of query"));
        }

        Ok(MetricQuery {
            aggregation,
            vector_aggregation,
            range_grouping,
            stream,
            range_ns,
            offset_ns,
        })
    }

    fn parse_metric_range_stream_query(
        &mut self,
    ) -> Result<(StreamQuery, DurationNanos, OffsetNanos), ParseError> {
        self.skip_ws();
        self.expect('{')?;
        let matchers = self.parse_matchers()?;
        self.expect('}')?;
        self.validate_stream_selector(&matchers)?;

        let mut pipeline = Vec::new();
        let mut range_ns = None;
        let mut offset_ns = OffsetNanos(0);
        let mut range_allows_following_pipeline = false;
        loop {
            self.skip_ws();
            if self.pos == self.input.len() {
                break;
            }

            if range_ns.is_none() && self.peek() == Some('[') {
                range_allows_following_pipeline = pipeline.is_empty();
                range_ns = Some(self.parse_range_selector()?);
                offset_ns = self.parse_range_offset()?;
                continue;
            }

            if range_ns.is_some() {
                if self.peek() == Some(')') {
                    break;
                }
                if !range_allows_following_pipeline {
                    return Err(self.error("expected ')'"));
                }
            }

            pipeline.push(self.parse_pipeline_stage()?);
        }

        let Some(range_ns) = range_ns else {
            return Err(self.error("expected range selector"));
        };

        Ok((StreamQuery { matchers, pipeline }, range_ns, offset_ns))
    }

    fn parse_metric_label_replace(mut self) -> Result<MetricLabelReplace, ParseError> {
        self.skip_ws();
        if !self.consume_keyword("label_replace") {
            return Err(self.error("expected label_replace"));
        }
        self.expect('(')?;
        let metric_query = self.parse_metric_function_argument("label_replace")?;
        self.expect(',')?;
        let destination_label = self.parse_quoted()?;
        self.expect(',')?;
        let replacement = self.parse_quoted()?;
        self.expect(',')?;
        let source_label = self.parse_quoted()?;
        self.expect(',')?;
        let pattern = self.parse_quoted()?;
        self.expect(')')?;
        self.skip_ws();
        if self.pos != self.input.len() {
            return Err(self.error("expected end of query"));
        }

        Ok(MetricLabelReplace {
            query: parse_metric_subexpression(&metric_query)?,
            destination_label,
            replacement,
            source_label,
            pattern,
        })
    }

    fn parse_metric_label_join(mut self) -> Result<MetricLabelJoin, ParseError> {
        self.skip_ws();
        if !self.consume_keyword("label_join") {
            return Err(self.error("expected label_join"));
        }
        self.expect('(')?;
        let metric_query = self.parse_metric_function_argument("label_join")?;
        self.expect(',')?;
        let destination_label = self.parse_quoted()?;
        self.expect(',')?;
        let separator = self.parse_quoted()?;
        self.expect(',')?;
        let mut source_labels = vec![self.parse_quoted()?];
        loop {
            self.skip_ws();
            if self.peek() == Some(')') {
                break;
            }
            self.expect(',')?;
            source_labels.push(self.parse_quoted()?);
        }
        self.expect(')')?;
        self.skip_ws();
        if self.pos != self.input.len() {
            return Err(self.error("expected end of query"));
        }

        Ok(MetricLabelJoin {
            query: parse_metric_subexpression(&metric_query)?,
            destination_label,
            separator,
            source_labels,
        })
    }

    fn parse_metric_function_argument(
        &mut self,
        function_name: &str,
    ) -> Result<String, ParseError> {
        self.skip_ws();
        let start = self.pos;
        let mut depth = 0usize;
        let mut quote_delimiter: Option<char> = None;
        let mut escaped = false;
        while let Some(ch) = self.peek() {
            if let Some(delimiter) = quote_delimiter {
                self.pos = self.pos.saturating_add(ch.len_utf8());
                if escaped {
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if delimiter.eq(&ch) {
                    quote_delimiter = None;
                }
                continue;
            }

            match ch {
                '"' | '`' => {
                    quote_delimiter = Some(ch);
                    self.pos = self.pos.saturating_add(ch.len_utf8());
                }
                '(' => {
                    depth = depth.saturating_add(1);
                    self.pos = self.pos.saturating_add(ch.len_utf8());
                }
                ')' => {
                    let Some(next_depth) = depth.checked_sub(1) else {
                        let message = format!("expected {function_name} metric query argument");
                        return Err(self.error(&message));
                    };
                    depth = next_depth;
                    self.pos = self.pos.saturating_add(ch.len_utf8());
                }
                ',' if matches!(depth, 0) => {
                    let metric_query = self.input[start..self.pos].trim();
                    if metric_query.is_empty() {
                        let message = format!("expected {function_name} metric query argument");
                        return Err(self.error(&message));
                    }
                    return Ok(metric_query.to_string());
                }
                _ => {
                    self.pos = self.pos.saturating_add(ch.len_utf8());
                }
            }
        }
        let message = format!("expected {function_name} metric query argument");
        Err(self.error(&message))
    }

    fn parse_metric_scalar_comparison(mut self) -> Result<MetricScalarComparison, ParseError> {
        self.skip_ws();
        let start = self.pos;
        let comparison = match self.parse_scalar_literal_text() {
            Ok(scalar) => {
                self.skip_ws();
                let op = self.parse_comparison_op()?;
                self.skip_ws();
                let bool_modifier = self.consume_keyword("bool");
                let metric_query_text = self.input[self.pos..].trim().to_string();
                if metric_query_text.is_empty() {
                    return Err(self.error("expected metric expression"));
                }
                self.pos = self.input.len();
                MetricScalarComparison {
                    query: parse_metric_subexpression(&metric_query_text)?,
                    op,
                    bool_modifier,
                    scalar,
                    scalar_on_left: true,
                }
            }
            Err(_) => {
                self.pos = start;
                let metric_query_text = self.parse_metric_expression_argument()?;
                let query = parse_metric_subexpression(&metric_query_text)?;
                let op = self.parse_comparison_op()?;
                self.skip_ws();
                let bool_modifier = self.consume_keyword("bool");
                let scalar = self.parse_scalar_literal_text()?;
                MetricScalarComparison {
                    query,
                    op,
                    bool_modifier,
                    scalar,
                    scalar_on_left: false,
                }
            }
        };
        self.skip_ws();
        if self.pos != self.input.len() {
            return Err(self.error("expected end of query"));
        }

        Ok(comparison)
    }

    fn parse_metric_scalar_arithmetic(mut self) -> Result<MetricScalarArithmetic, ParseError> {
        self.skip_ws();
        let start = self.pos;
        let arithmetic = match self.parse_scalar_literal_text() {
            Ok(scalar) => {
                self.skip_ws();
                let Some(op) = self.parse_arithmetic_op() else {
                    return Err(self.error("expected metric arithmetic operator"));
                };
                let metric_query_text = self.input[self.pos..].trim().to_string();
                if metric_query_text.is_empty() {
                    return Err(self.error("expected metric expression"));
                }
                self.pos = self.input.len();
                MetricScalarArithmetic {
                    query: parse_metric_subexpression(&metric_query_text)?,
                    op,
                    scalar,
                    scalar_on_left: true,
                }
            }
            Err(_) => {
                self.pos = start;
                let (metric_query_text, op) = self.parse_metric_arithmetic_argument()?;
                let query = parse_metric_subexpression(&metric_query_text)?;
                let scalar = self.parse_scalar_literal_text()?;
                MetricScalarArithmetic {
                    query,
                    op,
                    scalar,
                    scalar_on_left: false,
                }
            }
        };
        self.skip_ws();
        if self.pos != self.input.len() {
            return Err(self.error("expected end of query"));
        }

        Ok(arithmetic)
    }

    fn parse_metric_binary_arithmetic(mut self) -> Result<MetricBinaryArithmetic, ParseError> {
        self.skip_ws();
        let (left_text, op) = self.parse_metric_arithmetic_argument()?;
        let matching = self.parse_metric_vector_matching_modifier(true)?;
        let right_text = self.input[self.pos..].trim().to_string();
        if right_text.is_empty() {
            return Err(self.error("expected metric expression"));
        }
        self.pos = self.input.len();
        let arithmetic = MetricBinaryArithmetic {
            left: parse_metric_subexpression(&left_text)?,
            op,
            matching,
            right: parse_metric_subexpression(&right_text)?,
        };
        self.skip_ws();
        if self.pos != self.input.len() {
            return Err(self.error("expected end of query"));
        }

        Ok(arithmetic)
    }

    fn parse_metric_binary_comparison(mut self) -> Result<MetricBinaryComparison, ParseError> {
        self.skip_ws();
        let left_text = self.parse_metric_expression_argument()?;
        let op = self.parse_comparison_op()?;
        self.skip_ws();
        let bool_modifier = self.consume_keyword("bool");
        let matching = self.parse_metric_vector_matching_modifier(true)?;
        let right_text = self.input[self.pos..].trim().to_string();
        if right_text.is_empty() {
            return Err(self.error("expected metric expression"));
        }
        self.pos = self.input.len();
        let comparison = MetricBinaryComparison {
            left: parse_metric_subexpression(&left_text)?,
            op,
            bool_modifier,
            matching,
            right: parse_metric_subexpression(&right_text)?,
        };
        self.skip_ws();
        if self.pos != self.input.len() {
            return Err(self.error("expected end of query"));
        }

        Ok(comparison)
    }

    fn parse_metric_binary_set(mut self) -> Result<MetricBinarySet, ParseError> {
        self.skip_ws();
        let (left_text, op) = self.parse_metric_set_argument()?;
        let matching = self.parse_metric_vector_matching_modifier(false)?;
        let right_text = self.input[self.pos..].trim().to_string();
        if right_text.is_empty() {
            return Err(self.error("expected metric expression"));
        }
        self.pos = self.input.len();
        let set = MetricBinarySet {
            left: parse_metric_subexpression(&left_text)?,
            op,
            matching,
            right: parse_metric_subexpression(&right_text)?,
        };
        self.skip_ws();
        if self.pos != self.input.len() {
            return Err(self.error("expected end of query"));
        }

        Ok(set)
    }

    fn parse_metric_expression_argument(&mut self) -> Result<String, ParseError> {
        let start = self.pos;
        let mut depth = 0usize;
        let mut quote_delimiter: Option<char> = None;
        let mut escaped = false;
        while let Some(ch) = self.peek() {
            if let Some(delimiter) = quote_delimiter {
                self.pos = self.pos.saturating_add(ch.len_utf8());
                if escaped {
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if delimiter.eq(&ch) {
                    quote_delimiter = None;
                }
                continue;
            }

            match ch {
                '"' | '`' => {
                    quote_delimiter = Some(ch);
                    self.pos = self.pos.saturating_add(ch.len_utf8());
                }
                '(' => {
                    depth = depth.saturating_add(1);
                    self.pos = self.pos.saturating_add(ch.len_utf8());
                }
                ')' => {
                    let Some(next_depth) = depth.checked_sub(1) else {
                        return Err(self.error("expected metric expression"));
                    };
                    depth = next_depth;
                    self.pos = self.pos.saturating_add(ch.len_utf8());
                }
                '>' | '<' | '=' | '!' if matches!(depth, 0) => {
                    let metric_query = self.input[start..self.pos].trim();
                    if metric_query.is_empty() {
                        return Err(self.error("expected metric expression"));
                    }
                    return Ok(metric_query.to_string());
                }
                _ => {
                    self.pos = self.pos.saturating_add(ch.len_utf8());
                }
            }
        }
        Err(self.error("expected metric comparison operator"))
    }

    fn parse_metric_arithmetic_argument(
        &mut self,
    ) -> Result<(String, MetricScalarArithmeticOp), ParseError> {
        let start = self.pos;
        let mut depth = 0usize;
        let mut quote_delimiter: Option<char> = None;
        let mut escaped = false;
        while let Some(ch) = self.peek() {
            if let Some(delimiter) = quote_delimiter {
                self.pos = self.pos.saturating_add(ch.len_utf8());
                if escaped {
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if delimiter.eq(&ch) {
                    quote_delimiter = None;
                }
                continue;
            }

            match ch {
                '"' | '`' => {
                    quote_delimiter = Some(ch);
                    self.pos = self.pos.saturating_add(ch.len_utf8());
                }
                '(' => {
                    depth = depth.saturating_add(1);
                    self.pos = self.pos.saturating_add(ch.len_utf8());
                }
                ')' => {
                    let Some(next_depth) = depth.checked_sub(1) else {
                        return Err(self.error("expected metric expression"));
                    };
                    depth = next_depth;
                    self.pos = self.pos.saturating_add(ch.len_utf8());
                }
                '+' | '-' | '*' | '/' | '%' | '^' if matches!(depth, 0) => {
                    let metric_query = self.input[start..self.pos].trim();
                    if metric_query.is_empty() {
                        return Err(self.error("expected metric expression"));
                    }
                    let op = self.parse_arithmetic_op().expect("operator matched above");
                    return Ok((metric_query.to_string(), op));
                }
                _ => {
                    self.pos = self.pos.saturating_add(ch.len_utf8());
                }
            }
        }
        Err(self.error("expected metric arithmetic operator"))
    }

    fn parse_metric_set_argument(&mut self) -> Result<(String, MetricBinarySetOp), ParseError> {
        let start = self.pos;
        let mut depth = 0usize;
        let mut quote_delimiter: Option<char> = None;
        let mut escaped = false;
        while let Some(ch) = self.peek() {
            if let Some(delimiter) = quote_delimiter {
                self.pos = self.pos.saturating_add(ch.len_utf8());
                if escaped {
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if delimiter.eq(&ch) {
                    quote_delimiter = None;
                }
                continue;
            }

            match ch {
                '"' | '`' => {
                    quote_delimiter = Some(ch);
                    self.pos = self.pos.saturating_add(ch.len_utf8());
                }
                '(' => {
                    depth = depth.saturating_add(1);
                    self.pos = self.pos.saturating_add(ch.len_utf8());
                }
                ')' => {
                    let Some(next_depth) = depth.checked_sub(1) else {
                        return Err(self.error("expected metric expression"));
                    };
                    depth = next_depth;
                    self.pos = self.pos.saturating_add(ch.len_utf8());
                }
                _ if matches!(depth, 0) => {
                    if let Some((keyword_len, op)) = self.match_metric_set_op_at(self.pos) {
                        let metric_query = self.input[start..self.pos].trim();
                        if metric_query.is_empty() {
                            return Err(self.error("expected metric expression"));
                        }
                        self.pos = self.pos.saturating_add(keyword_len);
                        return Ok((metric_query.to_string(), op));
                    }
                    self.pos = self.pos.saturating_add(ch.len_utf8());
                }
                _ => {
                    self.pos = self.pos.saturating_add(ch.len_utf8());
                }
            }
        }
        Err(self.error("expected metric set operator"))
    }

    fn match_metric_set_op_at(&self, position: usize) -> Option<(usize, MetricBinarySetOp)> {
        for (keyword, op) in [
            ("unless", MetricBinarySetOp::Unless),
            ("and", MetricBinarySetOp::And),
            ("or", MetricBinarySetOp::Or),
        ] {
            if !self.input[position..].starts_with(keyword) {
                continue;
            }
            let previous = self.input[..position].chars().next_back();
            if previous.is_some_and(is_ident_char) {
                continue;
            }
            let end = position.saturating_add(keyword.len());
            let next = self.input[end..].chars().next();
            if next.is_some_and(is_ident_char) {
                continue;
            }
            return Some((keyword.len(), op));
        }
        None
    }

    fn parse_metric_vector_matching_modifier(
        &mut self,
        allow_group_modifier: bool,
    ) -> Result<Option<MetricVectorMatching>, ParseError> {
        if self.consume_keyword("on") {
            let labels = self.parse_grouping_labels()?;
            let group = self.parse_metric_vector_group_modifier()?;
            if matches!((allow_group_modifier, group.is_some()), (false, true)) {
                return Err(self.error("group modifiers are not supported for set operators"));
            }
            return Ok(Some(MetricVectorMatching::On { labels, group }));
        }
        if self.consume_keyword("ignoring") {
            let labels = self.parse_grouping_labels()?;
            let group = self.parse_metric_vector_group_modifier()?;
            if matches!((allow_group_modifier, group.is_some()), (false, true)) {
                return Err(self.error("group modifiers are not supported for set operators"));
            }
            return Ok(Some(MetricVectorMatching::Ignoring { labels, group }));
        }
        Ok(None)
    }

    fn parse_metric_vector_group_modifier(
        &mut self,
    ) -> Result<Option<MetricVectorGroupModifier>, ParseError> {
        if self.consume_keyword("group_left") {
            return Ok(Some(MetricVectorGroupModifier::Left(
                self.parse_optional_grouping_labels()?,
            )));
        }
        if self.consume_keyword("group_right") {
            return Ok(Some(MetricVectorGroupModifier::Right(
                self.parse_optional_grouping_labels()?,
            )));
        }
        Ok(None)
    }

    fn parse_optional_grouping_labels(&mut self) -> Result<Vec<String>, ParseError> {
        self.skip_ws();
        if self.peek() == Some('(') {
            self.parse_grouping_labels()
        } else {
            Ok(Vec::new())
        }
    }

    fn parse_arithmetic_op(&mut self) -> Option<MetricScalarArithmeticOp> {
        let op = match self.peek()? {
            '+' => MetricScalarArithmeticOp::Add,
            '-' => MetricScalarArithmeticOp::Subtract,
            '*' => MetricScalarArithmeticOp::Multiply,
            '/' => MetricScalarArithmeticOp::Divide,
            '%' => MetricScalarArithmeticOp::Modulo,
            '^' => MetricScalarArithmeticOp::Power,
            _ => return None,
        };
        self.pos = self.pos.saturating_add(1);
        Some(op)
    }

    fn parse_scalar_literal_text(&mut self) -> Result<String, ParseError> {
        self.skip_ws();
        let start = self.pos;
        if matches!(self.peek(), Some('+') | Some('-')) {
            self.pos = self.pos.saturating_add(1);
        }

        let whole_start = self.pos;
        while self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
            self.pos = self.pos.saturating_add(1);
        }
        let whole_digits = self.pos != whole_start;

        let mut fractional_digits = false;
        if self.peek() == Some('.') {
            self.pos = self.pos.saturating_add(1);
            let fractional_start = self.pos;
            while self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
                self.pos = self.pos.saturating_add(1);
            }
            fractional_digits = self.pos != fractional_start;
        }

        if matches!((whole_digits, fractional_digits), (false, false)) {
            return Err(self.error("expected scalar literal"));
        }

        if matches!(self.peek(), Some('e') | Some('E')) {
            self.pos = self.pos.saturating_add(1);
            if matches!(self.peek(), Some('+') | Some('-')) {
                self.pos = self.pos.saturating_add(1);
            }
            let exponent_start = self.pos;
            while self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
                self.pos = self.pos.saturating_add(1);
            }
            if matches!(self.pos.cmp(&exponent_start), std::cmp::Ordering::Equal) {
                return Err(self.error("expected scalar exponent"));
            }
        }

        Ok(self.input[start..self.pos].to_string())
    }

    fn try_parse_vector_aggregation(&mut self) -> Result<Option<VectorAggregation>, ParseError> {
        let Some(op) = self.try_parse_vector_aggregation_op() else {
            return Ok(None);
        };
        let grouping = self.try_parse_vector_grouping()?;
        Ok(Some(VectorAggregation { op, grouping }))
    }

    fn try_parse_vector_aggregation_op(&mut self) -> Option<VectorAggregationOp> {
        if self.consume_keyword("sum") {
            Some(VectorAggregationOp::Sum)
        } else if self.consume_keyword("count") {
            Some(VectorAggregationOp::Count)
        } else if self.consume_keyword("min") {
            Some(VectorAggregationOp::Min)
        } else if self.consume_keyword("max") {
            Some(VectorAggregationOp::Max)
        } else if self.consume_keyword("avg") {
            Some(VectorAggregationOp::Avg)
        } else if self.consume_keyword("stddev") {
            Some(VectorAggregationOp::Stddev)
        } else if self.consume_keyword("stdvar") {
            Some(VectorAggregationOp::Stdvar)
        } else if self.consume_keyword("count_values") {
            Some(VectorAggregationOp::CountValues(String::new()))
        } else if self.consume_keyword("approx_topk") {
            Some(VectorAggregationOp::ApproxTopK(0))
        } else if self.consume_keyword("topk") {
            Some(VectorAggregationOp::TopK(0))
        } else if self.consume_keyword("bottomk") {
            Some(VectorAggregationOp::BottomK(0))
        } else if self.consume_keyword("sort_desc") {
            Some(VectorAggregationOp::SortDesc)
        } else if self.consume_keyword("sort") {
            Some(VectorAggregationOp::Sort)
        } else {
            None
        }
    }

    fn parse_vector_aggregation_parameter(
        &mut self,
        op: &mut VectorAggregationOp,
    ) -> Result<(), ParseError> {
        match op {
            VectorAggregationOp::TopK(parameter)
            | VectorAggregationOp::BottomK(parameter)
            | VectorAggregationOp::ApproxTopK(parameter) => {
                self.skip_ws();
                *parameter = self.parse_u64_scalar()?;
                self.skip_ws();
                self.expect(',')?;
                Ok(())
            }
            VectorAggregationOp::CountValues(label) => {
                self.skip_ws();
                *label = self.parse_quoted()?;
                self.skip_ws();
                self.expect(',')?;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn parse_u64_scalar(&mut self) -> Result<u64, ParseError> {
        let start = self.pos;
        while self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
            self.pos = self.pos.saturating_add(1);
        }
        if self.pos == start {
            return Err(self.error("expected scalar parameter"));
        }
        self.input[start..self.pos]
            .parse::<u64>()
            .map_err(|_| self.error("expected scalar parameter"))
    }

    fn try_parse_vector_grouping(&mut self) -> Result<Option<VectorGrouping>, ParseError> {
        if self.consume_keyword("by") {
            Ok(Some(VectorGrouping::By(self.parse_grouping_labels()?)))
        } else if self.consume_keyword("without") {
            Ok(Some(VectorGrouping::Without(self.parse_grouping_labels()?)))
        } else {
            Ok(None)
        }
    }

    fn parse_grouping_labels(&mut self) -> Result<Vec<String>, ParseError> {
        self.expect('(')?;
        let mut labels = Vec::new();
        loop {
            self.skip_ws();
            if self.peek() == Some(')') {
                self.pos = self.pos.saturating_add(1);
                return Ok(labels);
            }
            labels.push(self.parse_ident()?);
            self.skip_ws();
            match self.peek() {
                Some(',') => {
                    self.pos = self.pos.saturating_add(1);
                }
                Some(')') => {
                    self.pos = self.pos.saturating_add(1);
                    return Ok(labels);
                }
                _ => return Err(self.error("expected ',' or ')'")),
            }
        }
    }

    fn parse_range_aggregation(&mut self) -> Result<RangeAggregationKind, ParseError> {
        if self.consume_keyword("count_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::CountOverTime,
            ))
        } else if self.consume_keyword("rate_counter") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::RateCounter,
            ))
        } else if self.consume_keyword("rate") {
            Ok(RangeAggregationKind::Standard(RangeAggregation::Rate))
        } else if self.consume_keyword("bytes_rate") {
            Ok(RangeAggregationKind::Standard(RangeAggregation::BytesRate))
        } else if self.consume_keyword("bytes_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::BytesOverTime,
            ))
        } else if self.consume_keyword("absent_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::AbsentOverTime,
            ))
        } else if self.consume_keyword("present_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::PresentOverTime,
            ))
        } else if self.consume_keyword("sum_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::SumOverTime,
            ))
        } else if self.consume_keyword("avg_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::AvgOverTime,
            ))
        } else if self.consume_keyword("stdvar_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::StdvarOverTime,
            ))
        } else if self.consume_keyword("stddev_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::StddevOverTime,
            ))
        } else if self.consume_keyword("quantile_over_time") {
            Ok(RangeAggregationKind::QuantileOverTime)
        } else if self.consume_keyword("min_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::MinOverTime,
            ))
        } else if self.consume_keyword("max_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::MaxOverTime,
            ))
        } else if self.consume_keyword("first_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::FirstOverTime,
            ))
        } else if self.consume_keyword("last_over_time") {
            Ok(RangeAggregationKind::Standard(
                RangeAggregation::LastOverTime,
            ))
        } else {
            Err(self.error("expected range aggregation"))
        }
    }

    fn parse_quantile(&mut self) -> Result<Quantile, ParseError> {
        self.skip_ws();
        let whole_start = self.pos;
        while self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
            self.pos = self.pos.saturating_add(1);
        }
        let has_whole = self.pos != whole_start;
        let whole = if has_whole {
            self.input[whole_start..self.pos]
                .parse::<u64>()
                .map_err(|_| self.error("expected quantile scalar"))?
        } else {
            0
        };

        let mut denominator = 1_u64;
        let mut fraction = 0_u64;
        if self.peek() == Some('.') {
            self.pos = self.pos.saturating_add(1);
            let fraction_start = self.pos;
            while self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
                self.pos = self.pos.saturating_add(1);
            }
            if matches!(self.pos.cmp(&fraction_start), std::cmp::Ordering::Equal) {
                return Err(self.error("expected quantile scalar"));
            }
            let fraction_text = &self.input[fraction_start..self.pos];
            denominator = 10_u64
                .checked_pow(u32::try_from(fraction_text.len()).unwrap_or(u32::MAX))
                .ok_or_else(|| self.error("expected quantile scalar"))?;
            fraction = fraction_text
                .parse::<u64>()
                .map_err(|_| self.error("expected quantile scalar"))?;
        }
        if matches!((has_whole, denominator), (false, 1)) {
            return Err(self.error("expected quantile scalar"));
        }

        let numerator = whole
            .checked_mul(denominator)
            .and_then(|value| value.checked_add(fraction))
            .ok_or_else(|| self.error("expected quantile scalar"))?;
        match numerator.cmp(&denominator) {
            std::cmp::Ordering::Greater => {
                return Err(self.error("quantile scalar must be between 0 and 1"));
            }
            std::cmp::Ordering::Equal | std::cmp::Ordering::Less => {}
        }

        let divisor = gcd_u64(numerator, denominator);
        Ok(Quantile {
            numerator: QuantileNumerator(numerator / divisor),
            denominator: QuantileDenominator(denominator / divisor),
        })
    }

    fn parse_stream_query(&mut self, stop_before_range: bool) -> Result<StreamQuery, ParseError> {
        self.skip_ws();
        self.expect('{')?;
        let matchers = self.parse_matchers()?;
        self.expect('}')?;
        self.validate_stream_selector(&matchers)?;

        let mut pipeline = Vec::new();
        loop {
            self.skip_ws();
            if stop_before_range && matches!(self.peek(), Some('[')) {
                break;
            }
            if self.pos == self.input.len() {
                break;
            }
            pipeline.push(self.parse_pipeline_stage()?);
        }

        Ok(StreamQuery { matchers, pipeline })
    }

    fn validate_stream_selector(&self, matchers: &[LabelMatcher]) -> Result<(), ParseError> {
        if matchers
            .iter()
            .any(|matcher| !matcher.matches_empty_value())
        {
            return Ok(());
        }

        Err(self.error(
            "selector must contain at least one label matcher that does not match the empty string",
        ))
    }

    fn parse_range_selector(&mut self) -> Result<DurationNanos, ParseError> {
        self.expect('[')?;
        self.skip_ws();
        let range_ns = self.parse_prometheus_duration()?;
        self.expect(']')?;
        Ok(DurationNanos(range_ns))
    }

    fn parse_range_offset(&mut self) -> Result<OffsetNanos, ParseError> {
        if self.consume_keyword("offset") {
            self.skip_ws();
            let negative = self.consume("-");
            let duration_ns = self.parse_prometheus_duration()?;
            if negative {
                duration_ns
                    .checked_neg()
                    .map(OffsetNanos)
                    .ok_or_else(|| self.error("range duration overflow"))
            } else {
                Ok(OffsetNanos(duration_ns))
            }
        } else {
            Ok(OffsetNanos(0))
        }
    }

    fn parse_prometheus_duration(&mut self) -> Result<i64, ParseError> {
        let mut parsed_chunk = false;
        let mut previous_unit_order = None;
        let mut seen_units = 0_u16;
        let mut total_ns = 0_i128;

        loop {
            let value_start = self.pos;
            while self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
                self.pos = self.pos.saturating_add(1);
            }
            if self.pos == value_start {
                if parsed_chunk {
                    break;
                }
                return Err(self.error("expected range duration"));
            }
            let value = self.input[value_start..self.pos]
                .parse::<i128>()
                .map_err(|_| self.error("expected range duration"))?;

            let unit_start = self.pos;
            while self.peek().is_some_and(|ch| ch.is_ascii_alphabetic()) {
                self.pos = self.pos.saturating_add(1);
            }
            let unit = &self.input[unit_start..self.pos];
            let Some((unit_order, unit_bit, multiplier)) = duration_unit(unit) else {
                return Err(self.error("expected range duration unit"));
            };
            if seen_units & unit_bit != 0 {
                return Err(self.error("repeated range duration unit"));
            }
            if let Some(previous) = previous_unit_order {
                match unit_order.cmp(&previous) {
                    std::cmp::Ordering::Greater => {}
                    std::cmp::Ordering::Equal | std::cmp::Ordering::Less => {
                        return Err(self.error("range duration units must be longest to shortest"));
                    }
                }
            }

            let chunk_ns = value
                .checked_mul(multiplier)
                .ok_or_else(|| self.error("range duration overflow"))?;
            total_ns = total_ns
                .checked_add(chunk_ns)
                .ok_or_else(|| self.error("range duration overflow"))?;
            seen_units = seen_units.saturating_add(unit_bit);
            previous_unit_order = Some(unit_order);
            parsed_chunk = true;

            if matches!(self.peek(), Some(']')) {
                break;
            }
        }

        i64::try_from(total_ns).map_err(|_| self.error("range duration overflow"))
    }

    fn parse_matchers(&mut self) -> Result<Vec<LabelMatcher>, ParseError> {
        let mut matchers = Vec::new();
        loop {
            self.skip_ws();
            if self.peek() == Some('}') {
                return Ok(matchers);
            }

            let name = self.parse_ident()?;
            self.skip_ws();
            let op = self.parse_match_op()?;
            self.skip_ws();
            let value = self.parse_quoted()?;
            matchers.push(LabelMatcher::new(name, op, value)?);

            self.skip_ws();
            match self.peek() {
                Some(',') => {
                    self.pos = self.pos.saturating_add(1);
                }
                Some('}') => return Ok(matchers),
                _ => return Err(self.error("expected ',' or '}'")),
            }
        }
    }

    fn parse_match_op(&mut self) -> Result<MatchOp, ParseError> {
        for (text, op) in [
            ("=~", MatchOp::RegexEqual),
            ("!~", MatchOp::RegexNotEqual),
            ("!=", MatchOp::NotEqual),
            ("=", MatchOp::Equal),
        ] {
            if self.consume(text) {
                return Ok(op);
            }
        }
        Err(self.error("expected label matcher operator"))
    }

    fn parse_pipeline_stage(&mut self) -> Result<PipelineStage, ParseError> {
        if let Some(filter) = self.try_parse_line_filter()? {
            return Ok(PipelineStage::LineFilter(filter));
        }

        if !self.consume("|") {
            return Err(self.error("expected pipeline stage"));
        }
        self.skip_ws();
        if self.consume_keyword("decolorize") {
            return Ok(PipelineStage::Decolorize);
        }
        if self.consume_keyword("json") {
            self.skip_ws();
            if self.pos < self.input.len() && !matches!(self.peek(), Some('|') | Some('[')) {
                return Ok(PipelineStage::Parser(ParserStage::JsonSelected(
                    self.parse_json_parser_config()?,
                )));
            }
            return Ok(PipelineStage::Parser(ParserStage::Json));
        }
        if self.consume_keyword("logfmt") {
            self.skip_ws();
            let (strict, keep_empty) = self.parse_logfmt_parser_flags();
            self.skip_ws();
            if self.pos < self.input.len() && !matches!(self.peek(), Some('|') | Some('[')) {
                return Ok(PipelineStage::Parser(ParserStage::LogfmtSelected(
                    self.parse_logfmt_parser_config(strict, keep_empty)?,
                )));
            }
            if strict || keep_empty {
                return Ok(PipelineStage::Parser(ParserStage::LogfmtConfigured(
                    LogfmtParserConfig::flags(strict, keep_empty)?,
                )));
            }
            return Ok(PipelineStage::Parser(ParserStage::Logfmt));
        }
        if self.consume_keyword("unpack") {
            return Ok(PipelineStage::Parser(ParserStage::Unpack));
        }
        if self.consume_keyword("pattern") {
            let pattern = self.parse_quoted()?;
            return Ok(PipelineStage::Parser(ParserStage::Pattern(
                PatternParser::new(pattern)?,
            )));
        }
        if self.consume_keyword("regexp") {
            let pattern = self.parse_quoted()?;
            return Ok(PipelineStage::Parser(ParserStage::Regexp(
                RegexpParser::new(pattern)?,
            )));
        }
        if self.consume_keyword("line_format") {
            let template = self.parse_quoted()?;
            return Ok(PipelineStage::LineFormat(LineFormat::new(template)?));
        }
        if self.consume_keyword("label_format") {
            return Ok(PipelineStage::LabelFormat(self.parse_label_format()?));
        }
        if self.consume_keyword("drop") {
            return Ok(PipelineStage::DropLabels(self.parse_label_selection_set()?));
        }
        if self.consume_keyword("keep") {
            return Ok(PipelineStage::KeepLabels(self.parse_label_selection_set()?));
        }
        if self.consume_keyword("unwrap") {
            self.skip_ws();
            return Ok(PipelineStage::Unwrap(self.parse_unwrap_expression()?));
        }

        let expression = self.parse_field_filter_expression()?;
        Ok(field_filter_expression_to_pipeline_stage(expression))
    }

    fn try_parse_line_filter(&mut self) -> Result<Option<LineFilter>, ParseError> {
        let op = if self.consume("|=") {
            LineFilterOp::Contains
        } else if self.consume("!=") {
            LineFilterOp::NotContains
        } else if self.consume("|~") {
            LineFilterOp::Regex
        } else if self.consume("!~") {
            LineFilterOp::NotRegex
        } else if self.consume("|>") {
            LineFilterOp::Pattern
        } else if self.consume("!>") {
            LineFilterOp::NotPattern
        } else {
            return Ok(None);
        };
        self.skip_ws();
        if self.consume_keyword("ip") {
            self.expect('(')?;
            let pattern = self.parse_quoted()?;
            self.expect(')')?;
            return LineFilter::ip(op, pattern).map(Some);
        }
        let pattern = self.parse_quoted()?;
        LineFilter::new(op, pattern).map(Some)
    }

    fn parse_label_format(&mut self) -> Result<LabelFormat, ParseError> {
        let mut assignments = Vec::new();
        loop {
            self.skip_ws();
            let destination = self.parse_ident()?;
            self.expect('=')?;
            self.skip_ws();
            let assignment = if matches!(self.peek(), Some('"') | Some('`')) {
                LabelFormatAssignment::template(destination, self.parse_quoted()?)?
            } else {
                LabelFormatAssignment::rename(destination, self.parse_ident()?)?
            };
            assignments.push(assignment);
            if !self.consume(",") {
                break;
            }
        }
        LabelFormat::new(assignments)
    }

    fn parse_unwrap_expression(&mut self) -> Result<UnwrapExpression, ParseError> {
        if self.consume_keyword("bytes") {
            self.expect('(')?;
            let label = self.parse_ident()?;
            self.expect(')')?;
            return UnwrapExpression::bytes(label);
        }
        if self.consume_keyword("duration") || self.consume_keyword("duration_seconds") {
            self.expect('(')?;
            let label = self.parse_ident()?;
            self.expect(')')?;
            return UnwrapExpression::duration(label);
        }

        UnwrapExpression::new(self.parse_ident()?)
    }

    fn parse_json_parser_config(&mut self) -> Result<JsonParserConfig, ParseError> {
        let mut extractions = Vec::new();
        loop {
            self.skip_ws();
            let destination = self.parse_ident()?;
            self.expect('=')?;
            self.skip_ws();
            let expression = self.parse_quoted()?;
            extractions.push(JsonExtraction::new(
                DestinationLabel(destination),
                JsonExpressionPath(expression),
            )?);
            if !self.consume(",") {
                break;
            }
        }
        JsonParserConfig::new(extractions)
    }

    fn parse_logfmt_parser_flags(&mut self) -> (bool, bool) {
        let mut strict = false;
        let mut keep_empty = false;
        loop {
            self.skip_ws();
            if self.consume("--strict") {
                strict = true;
            } else if self.consume("--keep-empty") {
                keep_empty = true;
            } else {
                break;
            }
        }
        (strict, keep_empty)
    }

    fn parse_logfmt_parser_config(
        &mut self,
        strict: bool,
        keep_empty: bool,
    ) -> Result<LogfmtParserConfig, ParseError> {
        let mut extractions = Vec::new();
        loop {
            self.skip_ws();
            let destination = self.parse_ident()?;
            self.skip_ws();
            let extraction = if self.consume("=") {
                let source = self.parse_quoted()?;
                LogfmtExtraction::rename(DestinationLabel(destination), SourceLabel(source))?
            } else {
                LogfmtExtraction::same(destination)?
            };
            extractions.push(extraction);
            if !self.consume(",") {
                break;
            }
        }
        LogfmtParserConfig::with_options(extractions, strict, keep_empty)
    }

    fn parse_label_selection_set(&mut self) -> Result<LabelSelectionSet, ParseError> {
        let mut selections = Vec::new();
        loop {
            self.skip_ws();
            let name = self.parse_ident()?;
            self.skip_ws();
            let selection = if self.consume("=~") {
                LabelSelection::regex(name, self.parse_quoted()?)?
            } else if self.consume("=") {
                LabelSelection::equal(name, self.parse_quoted()?)?
            } else {
                LabelSelection::name(name)?
            };
            selections.push(selection);
            if !self.consume(",") {
                break;
            }
        }
        LabelSelectionSet::new(selections)
    }

    fn parse_field_filter(&mut self) -> Result<FieldFilter, ParseError> {
        self.skip_ws();
        let name = self.parse_ident()?;
        self.skip_ws();
        let op = self.parse_comparison_op()?;
        self.skip_ws();
        let value = self.parse_field_value()?;
        FieldFilter::try_new(name, op, value)
    }

    fn parse_field_filter_primary(&mut self) -> Result<FieldFilterExpression, ParseError> {
        self.skip_ws();
        if self.consume("(") {
            let expression = self.parse_field_filter_expression()?;
            self.skip_ws();
            self.expect(')')?;
            return Ok(FieldFilterExpression::Group(Box::new(expression)));
        }
        self.parse_field_filter().map(FieldFilterExpression::Filter)
    }

    fn parse_field_filter_expression(&mut self) -> Result<FieldFilterExpression, ParseError> {
        let first = self.parse_field_filter_primary()?;
        let mut rest = Vec::new();
        loop {
            self.skip_ws();
            let op = if self.consume_keyword("and") {
                FieldFilterLogicOp::And
            } else if self.consume_keyword("or") {
                FieldFilterLogicOp::Or
            } else if self.consume(",") {
                FieldFilterLogicOp::And
            } else if self
                .peek()
                .is_some_and(|ch| is_ident_start(ch) || ch == '(')
            {
                FieldFilterLogicOp::And
            } else {
                break;
            };
            rest.push((op, self.parse_field_filter_primary()?));
        }

        if rest.is_empty() {
            Ok(first)
        } else {
            Ok(FieldFilterExpression::Chain {
                first: Box::new(first),
                rest,
            })
        }
    }

    fn parse_comparison_op(&mut self) -> Result<ComparisonOp, ParseError> {
        for (text, op) in [
            (">=", ComparisonOp::GreaterEqual),
            ("<=", ComparisonOp::LessEqual),
            ("=~", ComparisonOp::RegexEqual),
            ("!~", ComparisonOp::RegexNotEqual),
            ("==", ComparisonOp::Equal),
            ("!=", ComparisonOp::NotEqual),
            ("=", ComparisonOp::Equal),
            (">", ComparisonOp::Greater),
            ("<", ComparisonOp::Less),
        ] {
            if self.consume(text) {
                return Ok(op);
            }
        }
        Err(self.error("expected field comparison operator"))
    }

    fn parse_field_value(&mut self) -> Result<FieldValue, ParseError> {
        if self.consume_keyword("ip") {
            self.expect('(')?;
            let pattern = self.parse_quoted()?;
            self.expect(')')?;
            return Ok(FieldValue::Ip(IpMatcher::parse(&pattern)?));
        }
        if matches!(self.peek(), Some('"') | Some('`')) {
            return Ok(FieldValue::String(self.parse_quoted()?));
        }

        let start = self.pos;
        if matches!(self.peek(), Some('-')) {
            self.pos = self.pos.saturating_add(1);
        }
        while let Some(ch) = self.peek() {
            match ch {
                '.' => self.pos = self.pos.saturating_add(ch.len_utf8()),
                _ if ch.is_ascii_alphanumeric() => {
                    self.pos = self.pos.saturating_add(ch.len_utf8());
                }
                _ => break,
            }
        }
        let literal = &self.input[start..self.pos];
        match literal {
            "" | "-" => return Err(self.error("expected field comparison value")),
            _ => {}
        }
        if literal.bytes().any(|byte| byte.is_ascii_alphabetic()) {
            if let Some(duration_ns) = parse_prometheus_duration_literal(literal) {
                return Ok(FieldValue::Duration(duration_ns));
            }
            if let Some(bytes) = parse_bytes_literal(literal) {
                return Ok(FieldValue::Bytes(bytes));
            }
            return Err(self.error("expected duration or bytes field comparison value"));
        }
        literal
            .parse()
            .map(FieldValue::Number)
            .map_err(|_| self.error("expected numeric field comparison value"))
    }

    fn parse_ident(&mut self) -> Result<String, ParseError> {
        let start = self.pos;
        while let Some(ch) = self.peek() {
            if is_ident_char(ch) {
                self.pos = self.pos.saturating_add(ch.len_utf8());
            } else {
                break;
            }
        }
        if self.pos == start {
            return Err(self.error("expected label name"));
        }
        Ok(self.input[start..self.pos].to_string())
    }

    fn parse_quoted(&mut self) -> Result<String, ParseError> {
        self.skip_ws();
        if matches!(self.peek(), Some('`')) {
            self.pos = self.pos.saturating_add(1);
            let start = self.pos;
            while let Some(ch) = self.peek() {
                match ch {
                    '`' => {
                        let out = self.input[start..self.pos].to_string();
                        self.pos = self.pos.saturating_add(1);
                        return Ok(out);
                    }
                    _ => {
                        self.pos = self.pos.saturating_add(ch.len_utf8());
                    }
                }
            }
            return Err(self.error("expected closing backtick"));
        }

        self.expect('"')?;
        let mut out = String::new();
        while let Some(ch) = self.peek() {
            self.pos = self.pos.saturating_add(ch.len_utf8());
            match ch {
                '"' => return Ok(out),
                '\\' => {
                    let Some(escaped) = self.peek() else {
                        return Err(self.error("expected escaped character"));
                    };
                    self.pos = self.pos.saturating_add(escaped.len_utf8());
                    out.push(decode_quoted_escape(escaped));
                }
                _ => out.push(ch),
            }
        }
        Err(self.error("expected closing quote"))
    }

    fn expect(&mut self, expected: char) -> Result<(), ParseError> {
        self.skip_ws();
        if self.peek().is_some_and(|ch| ch == expected) {
            self.pos = self.pos.saturating_add(expected.len_utf8());
            Ok(())
        } else {
            Err(self.error(&format!("expected {}", QuotedChar(expected))))
        }
    }

    fn consume(&mut self, text: &str) -> bool {
        self.skip_ws();
        if self.input[self.pos..].starts_with(text) {
            self.pos = self.pos.saturating_add(text.len());
            true
        } else {
            false
        }
    }

    fn consume_keyword(&mut self, keyword: &str) -> bool {
        self.skip_ws();
        if !self.input[self.pos..].starts_with(keyword) {
            return false;
        }
        let end = self.pos.saturating_add(keyword.len());
        let next = self.input[end..].chars().next();
        if next.is_some_and(is_ident_char) {
            return false;
        }
        self.pos = end;
        true
    }

    fn skip_ws(&mut self) {
        loop {
            let start = self.pos;
            while let Some(ch) = self.peek() {
                if ch.is_whitespace() {
                    self.pos = self.pos.saturating_add(ch.len_utf8());
                } else {
                    break;
                }
            }
            if matches!(self.peek(), Some('#')) {
                while let Some(ch) = self.peek() {
                    self.pos = self.pos.saturating_add(ch.len_utf8());
                    if matches!(ch, '\n') {
                        break;
                    }
                }
            }
            if matches!(self.pos.cmp(&start), std::cmp::Ordering::Equal) {
                break;
            }
        }
    }

    fn peek(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn error(&self, message: &str) -> ParseError {
        ParseError::Syntax {
            message: message.to_string(),
            position: self.pos,
        }
    }
}
