//! DML RETURNING projection and image handling.

use super::*;

/// One row a `RETURNING` clause will project: the post-image (absent for
/// `DELETE`), the pre-image (absent for a plain `INSERT`), and the joined
/// source columns the clause may also reference.
pub(crate) struct ReturnedRow {
    pub(crate) new: Option<Vec<Datum>>,
    pub(crate) old: Option<Vec<Datum>>,
    pub(crate) source: Vec<Datum>,
    pub(crate) old_xmin: u64,
    pub(crate) old_xmax: u64,
    pub(crate) old_cmin: u32,
    pub(crate) old_cmax: u32,
    pub(crate) new_xmin: u64,
    pub(crate) new_xmax: u64,
    pub(crate) new_cmin: u32,
    pub(crate) new_cmax: u32,
    /// What `merge_action()` reports for this row; `None` outside `MERGE`.
    pub(crate) action: Option<&'static str>,
    /// The physical identities the OLD and NEW images describe.
    pub(crate) old_identity: u64,
    pub(crate) new_identity: u64,
}

impl ReturnedRow {
    pub(crate) fn updated(
        new: Vec<Datum>,
        old: Vec<Datum>,
        source: Vec<Datum>,
        old_identity: u64,
        new_identity: u64,
        old_xmin: u64,
        old_cmin: u32,
        new_xmin: u64,
        new_cmin: u32,
    ) -> Self {
        Self {
            new: Some(new),
            old: Some(old),
            source,
            old_xmin,
            old_xmax: new_xmin,
            old_cmin,
            old_cmax: new_cmin,
            new_xmin,
            new_xmax: 0,
            new_cmin,
            new_cmax: 0,
            action: None,
            old_identity,
            new_identity,
        }
    }
}

/// The prefix that makes an `OLD`/`NEW` image binding unreachable by a bare
/// column reference.
///
/// It cannot occur in any identifier the lexer produces, not even a quoted one,
/// which cannot contain a control character. So `RETURNING v` still resolves to
/// the one target column named `v`, as it does in `PostgreSQL`.
const IMAGE_BINDING_PREFIX: char = '\u{1}';

/// An analyzed `RETURNING` clause: the scope its expressions resolve against and
/// the projection rewritten so `OLD`/`NEW` references reach the image bindings.
pub(crate) struct ReturningSpec {
    pub(crate) scope: Scope,
    pub(crate) items: Vec<SelectItem>,
    /// Where the pre-image columns start in the combined row.
    old_offset: usize,
    /// Where the post-image columns start in the combined row.
    new_offset: usize,
    /// `MERGE` appends one `merge_action()` column after the image blocks.
    merge: bool,
    /// The system columns each `OLD`/`NEW` image carries, which
    /// [`Self::outcome`] appends to the image rows the write path hands back.
    ///
    /// The same columns the target's own block carries, because `old.ctid` and
    /// a bare `ctid` name the same thing here: one row, read through two
    /// spellings.
    images: crate::scope::SystemStamp,
    /// The target relation, kept only when it has a `VIRTUAL` generated column.
    /// The post-image a write hands back carries the NULL placeholder that goes
    /// to storage, so `RETURNING` has to produce the value the next reader would
    /// see rather than print the placeholder.
    target: Option<Box<Table>>,
    pub(crate) active: bool,
}

/// The name of the synthetic binding `merge_action()` is rewritten to. Like the
/// `OLD`/`NEW` image bindings it is unreachable by an ordinary column reference.
const MERGE_ACTION_BINDING: &str = "\u{1}merge_action";

impl ReturningSpec {
    pub(crate) fn new(
        table: &Table,
        qualifier: &str,
        returning: Option<&crabka_pgparser::ast::Returning>,
        source: Option<&Scope>,
        merge: bool,
    ) -> Result<Self, ExecError> {
        // `MERGE` lists its source relation before its target, so `RETURNING *`
        // expands source-first there and target-first everywhere else.
        let target_width = table.columns.len();
        let Some(returning) = returning else {
            return Ok(Self {
                scope: Scope::empty(),
                items: Vec::new(),
                old_offset: 0,
                new_offset: 0,
                merge,
                images: crate::scope::SystemColumns::default().stamp(table.id)?,
                target: None,
                active: false,
            });
        };
        // The visible relations: the target, then any FROM/USING/MERGE source.
        let mut scope = match source {
            Some(source) => source.clone(),
            None => Scope::single(table, qualifier),
        };
        let visible_width = scope.width();
        // PostgreSQL 18's default `old`/`new` spellings are suppressed when a
        // relation of that name is already in scope.
        let taken = |name: &str| {
            scope
                .columns
                .iter()
                .any(|c| c.qualifier.as_deref() == Some(name))
        };
        // An explicit image alias is a relation name like any other: colliding
        // with a relation in scope, or with the other image's alias, is 42712.
        for alias in [&returning.old_alias, &returning.new_alias]
            .into_iter()
            .flatten()
        {
            if taken(alias) {
                return Err(ExecError::DuplicateAlias(alias.clone()));
            }
        }
        if let (Some(old), Some(new)) = (&returning.old_alias, &returning.new_alias)
            && old == new
        {
            return Err(ExecError::DuplicateAlias(old.clone()));
        }
        // An explicit alias also suppresses the OTHER image's default spelling,
        // so `RETURNING WITH (NEW AS old) old.v` reads the post-image.
        let old_alias = returning.old_alias.clone().or_else(|| {
            (!taken("old") && returning.new_alias.as_deref() != Some("old"))
                .then(|| "old".to_string())
        });
        let new_alias = returning.new_alias.clone().or_else(|| {
            (!taken("new") && returning.old_alias.as_deref() != Some("new"))
                .then(|| "new".to_string())
        });
        // The images carry exactly the system columns the visible target block
        // does, read off the scope the caller built rather than re-derived, so
        // an image can never offer a column the row has no value for.
        let images = system_columns_of(&scope, qualifier).stamp(table.id)?;
        let old_offset = scope.width();
        scope.columns.extend(image_bindings(table, "old", &images));
        let new_offset = scope.width();
        scope.columns.extend(image_bindings(table, "new", &images));
        for image in ["old", "new"] {
            scope.columns.push(ColumnBinding {
                exposure: Exposure::Output,
                qualifier: None,
                name: whole_image_binding_name(image),
                ty: ColumnType::Record(None),
            });
        }
        if merge {
            scope.columns.push(ColumnBinding {
                exposure: Exposure::Output,
                qualifier: None,
                name: MERGE_ACTION_BINDING.to_string(),
                ty: ColumnType::Text,
            });
        }
        let mut items = Vec::new();
        for item in &returning.items {
            match item {
                // `*` spans the visible relations only — never the image aliases.
                SelectItem::Wildcard => {
                    let order: Vec<usize> = if merge {
                        (target_width..visible_width)
                            .chain(0..target_width)
                            .collect()
                    } else {
                        (0..visible_width).collect()
                    };
                    // This expands by index range rather than through
                    // `resolve_projection`, so it has to skip a USING/NATURAL
                    // join's retained input columns itself: `UPDATE … FROM a
                    // JOIN b USING (x) RETURNING *` must not return `x` thrice.
                    items.extend(
                        order
                            .into_iter()
                            .filter(|i| !scope.columns[*i].is_join_input())
                            .map(|i| {
                                let c = &scope.columns[i];
                                SelectItem::Expr {
                                    expr: Expr::Column {
                                        table: c.qualifier.clone(),
                                        name: c.name.clone(),
                                    },
                                    alias: Some(c.name.clone()),
                                }
                            }),
                    );
                }
                SelectItem::QualifiedWildcard(q) if Some(q) == old_alias.as_ref() => {
                    items.extend(image_wildcard(table, "old"));
                }
                SelectItem::QualifiedWildcard(q) if Some(q) == new_alias.as_ref() => {
                    items.extend(image_wildcard(table, "new"));
                }
                SelectItem::QualifiedWildcard(_) => items.push(item.clone()),
                SelectItem::Expr { expr, alias } => {
                    let whole_image = match expr {
                        Expr::Column { table: None, name }
                            if !scope.columns[..visible_width]
                                .iter()
                                .any(|column| column.name == *name)
                                && Some(name.as_str()) == old_alias.as_deref() =>
                        {
                            Some(("old", name))
                        }
                        Expr::Column { table: None, name }
                            if !scope.columns[..visible_width]
                                .iter()
                                .any(|column| column.name == *name)
                                && Some(name.as_str()) == new_alias.as_deref() =>
                        {
                            Some(("new", name))
                        }
                        _ => None,
                    };
                    let rewritten = whole_image.map_or_else(
                        || {
                            rewrite_image_refs(
                                expr,
                                &ImageAliases {
                                    table,
                                    old: old_alias.as_deref(),
                                    new: new_alias.as_deref(),
                                    merge,
                                },
                            )
                        },
                        |(image, _)| Expr::Column {
                            table: None,
                            name: whole_image_binding_name(image),
                        },
                    );
                    // Rewriting hides the output name a bare `old.v` or
                    // `merge_action()` would otherwise derive, so it is pinned
                    // from the spelling the user wrote.
                    let alias = alias.clone().or_else(|| match expr {
                        Expr::Func(fc) if merge && fc.name == "merge_action" => {
                            Some("merge_action".to_string())
                        }
                        Expr::Column {
                            table: Some(q),
                            name,
                        } if Some(q.as_str()) == old_alias.as_deref()
                            || Some(q.as_str()) == new_alias.as_deref() =>
                        {
                            Some(name.clone())
                        }
                        _ if whole_image.is_some() => match expr {
                            Expr::Column { name, .. } => Some(name.clone()),
                            _ => unreachable!("whole image must be a column"),
                        },
                        _ => None,
                    });
                    items.push(SelectItem::Expr {
                        expr: rewritten,
                        alias,
                    });
                }
            }
        }
        Ok(Self {
            scope,
            items,
            old_offset,
            new_offset,
            merge,
            images,
            target: has_virtual_generated(table).then(|| Box::new(table.clone())),
            active: true,
        })
    }

    pub(crate) fn outcome(
        &self,
        tag: String,
        rows: Vec<ReturnedRow>,
        ctx: &crate::clock::EvalCtx,
    ) -> Result<WriteOutcome, ExecError> {
        if !self.active {
            return Ok(WriteOutcome::command(tag));
        }
        let width = self.new_offset - self.old_offset;
        let combined: Vec<Vec<Datum>> = rows
            .into_iter()
            .map(|mut row| {
                if let Some(target) = &self.target {
                    for image in [&mut row.old, &mut row.new].into_iter().flatten() {
                        expand_virtual_generated_row(
                            target,
                            image,
                            ctx,
                            crate::scope::GeneratedReads::every(),
                        )?;
                    }
                }
                // The visible target columns show the post-image, or the
                // pre-image for a DELETE, which is what PostgreSQL projects —
                // and only the relation's DECLARED columns. The visible block's
                // own system columns arrive in `source`, where the write paths
                // put them, so the images must still be the width they were
                // written at when this copy is taken.
                let mut out = row
                    .new
                    .clone()
                    .or_else(|| row.old.clone())
                    .unwrap_or_else(|| vec![Datum::Null; width - self.images.width()]);
                out.extend(row.source);
                let (identity, xmin, xmax, cmin, cmax) = if row.new.is_some() {
                    (
                        row.new_identity,
                        row.new_xmin,
                        row.new_xmax,
                        row.new_cmin,
                        row.new_cmax,
                    )
                } else {
                    (
                        row.old_identity,
                        row.old_xmin,
                        row.old_xmax,
                        row.old_cmin,
                        row.old_cmax,
                    )
                };
                for (index, column) in self.scope.columns.iter().enumerate() {
                    if column.exposure != Exposure::SystemColumn {
                        continue;
                    }
                    out[index] = match column.name.as_str() {
                        "xmin" => crate::scope::row_xmin(xmin),
                        "xmax" => crate::scope::row_xmin(xmax),
                        "cmin" => Datum::Int4(cmin as i32),
                        "cmax" => Datum::Int4(cmax as i32),
                        "ctid" => crate::scope::row_ctid(identity),
                        _ => continue,
                    };
                }
                // Each image then takes its own copy of the system columns, so
                // `old.ctid` reads whether or not the visible block asked for a
                // bare `ctid` as well.
                if let Some(old) = &mut row.old {
                    self.images.extend_row(
                        old,
                        row.old_identity,
                        row.old_xmin,
                        row.old_xmax,
                        row.old_cmin,
                        row.old_cmax,
                    );
                }
                if let Some(new) = &mut row.new {
                    self.images.extend_row(
                        new,
                        row.new_identity,
                        row.new_xmin,
                        row.new_xmax,
                        row.new_cmin,
                        row.new_cmax,
                    );
                }
                let nulls = vec![Datum::Null; width];
                let image_width = width - self.images.width();
                let whole_image = |image: Option<&Vec<Datum>>| {
                    image.map_or(Datum::Null, |image| {
                        Datum::Record(crabka_pgtypes::RecordValue::anonymous(
                            image[..image_width].to_vec(),
                        ))
                    })
                };
                let old_whole = whole_image(row.old.as_ref());
                let new_whole = whole_image(row.new.as_ref());
                out.extend(row.old.unwrap_or_else(|| nulls.clone()));
                out.extend(row.new.unwrap_or(nulls));
                out.push(old_whole);
                out.push(new_whole);
                if self.merge {
                    out.push(row.action.map_or(Datum::Null, |a| Datum::Text(a.into())));
                }
                Ok(out)
            })
            .collect::<Result<_, ExecError>>()?;
        let (mut fields, out_exprs, tys) = resolve_projection(&self.items, &self.scope)?;
        show_image_bindings_by_their_column_names(&mut fields);
        let projected = project_rows(&out_exprs, &self.scope, &combined, ctx)?;
        let scope = Scope {
            columns: fields
                .iter()
                .zip(&tys)
                .map(|(f, ty)| ColumnBinding {
                    exposure: Exposure::Output,
                    qualifier: None,
                    name: f.name.clone(),
                    ty: *ty,
                })
                .collect(),
            ..Default::default()
        };
        Ok(WriteOutcome {
            tag,
            returning: Some(Relation {
                scope,
                rows: projected,
            }),
        })
    }
}

fn image_binding_name(image: &str, column: &str) -> String {
    format!("{IMAGE_BINDING_PREFIX}{image}.{column}")
}

fn whole_image_binding_name(image: &str) -> String {
    format!("{IMAGE_BINDING_PREFIX}{image}")
}

/// Rename any output field that derived its name from an image binding to the
/// column that binding names.
///
/// A bare `old.v` has its output name pinned from the spelling the user wrote,
/// but anything built around one does not: `RETURNING old.tableoid::regclass`
/// derives its name from the expression the rewrite left behind, so the header
/// read `\u{1}old.tableoid` — an internal name, with a control character in it,
/// on the wire. `PostgreSQL` names that column `tableoid`, which is what the
/// binding was made from.
pub(crate) fn show_image_bindings_by_their_column_names(fields: &mut [FieldDescription]) {
    for field in fields {
        if let Some(rest) = field.name.strip_prefix(IMAGE_BINDING_PREFIX) {
            field.name = rest
                .split_once('.')
                .map_or_else(|| rest.to_string(), |(_, column)| column.to_string());
        }
    }
}

/// The bindings one `OLD`/`NEW` image contributes: the relation's declared
/// columns, then the system columns `system` says the image carries.
///
/// The system bindings keep [`Exposure::Output`], unlike the visible block's:
/// nothing expands an image binding by name — `old.*` goes through
/// [`image_wildcard`], which lists the declared columns itself — so the
/// exposure has no expansion to be hidden from, and marking them otherwise
/// would only make `old.ctid` unreachable.
fn image_bindings(
    table: &Table,
    image: &str,
    system: &crate::scope::SystemStamp,
) -> Vec<ColumnBinding> {
    let mut scope = Scope::empty();
    system.extend_scope(&mut scope, image);
    table
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.ty))
        .chain(scope.columns.into_iter().map(|c| (c.name, c.ty)))
        .map(|(name, ty)| ColumnBinding {
            exposure: Exposure::Output,
            qualifier: None,
            name: image_binding_name(image, &name),
            ty,
        })
        .collect()
}

/// The system columns `scope`'s block for `qualifier` carries.
///
/// Read back off the scope rather than re-derived from the statement, so the
/// two can never disagree: the caller that built the scope already decided
/// which columns the rows under it hold.
fn system_columns_of(scope: &Scope, qualifier: &str) -> crate::scope::SystemColumns {
    let has = |name: &str| {
        scope.columns.iter().any(|c| {
            c.exposure == Exposure::SystemColumn
                && c.qualifier.as_deref() == Some(qualifier)
                && c.name == name
        })
    };
    crate::scope::SystemColumns {
        tableoid: has(crate::scope::TABLEOID_COLUMN),
        cmax: has("cmax"),
        xmax: has("xmax"),
        cmin: has("cmin"),
        xmin: has("xmin"),
        ctid: has(crate::scope::CTID_COLUMN),
    }
}

fn image_wildcard(table: &Table, image: &str) -> Vec<SelectItem> {
    table
        .columns
        .iter()
        .map(|c| SelectItem::Expr {
            expr: Expr::Column {
                table: None,
                name: image_binding_name(image, &c.name),
            },
            alias: Some(c.name.clone()),
        })
        .collect()
}

/// Point every `old.col` / `new.col` reference at its image binding.
///
/// This leaves alone the nodes that cannot contain a row reference reachable
/// from `RETURNING`: literals, parameters, and the subquery forms, which have
/// their own scope.
struct ImageAliases<'a> {
    table: &'a Table,
    pub(crate) old: Option<&'a str>,
    pub(crate) new: Option<&'a str>,
    merge: bool,
}

fn rewrite_image_refs(expr: &Expr, aliases: &ImageAliases<'_>) -> Expr {
    let recurse = |e: &Expr| Box::new(rewrite_image_refs(e, aliases));
    let recurse_all = |items: &[Expr]| -> Vec<Expr> { items.iter().map(|e| *recurse(e)).collect() };
    match expr {
        // `merge_action()` is not an ordinary function: it reports which WHEN
        // clause produced the row, so it reads a per-row binding.
        Expr::Func(fc)
            if aliases.merge
                && fc.name == "merge_action"
                && matches!(&fc.args, crabka_pgparser::ast::FuncArgs::Exprs(a) if a.is_empty()) =>
        {
            Expr::Column {
                table: None,
                name: MERGE_ACTION_BINDING.to_string(),
            }
        }
        Expr::Column {
            table: Some(qualifier),
            name,
        } => {
            let image = if Some(qualifier.as_str()) == aliases.old {
                Some("old")
            } else if Some(qualifier.as_str()) == aliases.new {
                Some("new")
            } else {
                None
            };
            match image {
                // An image reference to a column the target does not have keeps
                // its readable spelling, so resolution reports 42703 against
                // `old.nope` rather than the internal binding name. A system
                // column is not among the target's declared columns and is
                // still an image column, so it is admitted by name — and left
                // to resolve, or not, against the bindings the image laid down.
                Some(image)
                    if aliases.table.column_index(name).is_some()
                        || crate::scope::is_system_column(name) =>
                {
                    Expr::Column {
                        table: None,
                        name: image_binding_name(image, name),
                    }
                }
                Some(_) => Expr::Column {
                    table: None,
                    name: format!("{qualifier}.{name}"),
                },
                None => expr.clone(),
            }
        }
        Expr::Unary { op, expr } => Expr::Unary {
            op: *op,
            expr: recurse(expr),
        },
        Expr::Binary { op, left, right } => Expr::Binary {
            op: *op,
            left: recurse(left),
            right: recurse(right),
        },
        Expr::Func(fc) => Expr::Func(crabka_pgparser::ast::FuncCall {
            sql_syntax: fc.sql_syntax,
            name: fc.name.clone(),
            distinct: fc.distinct,
            args: match &fc.args {
                crabka_pgparser::ast::FuncArgs::Star => crabka_pgparser::ast::FuncArgs::Star,
                crabka_pgparser::ast::FuncArgs::Exprs(args) => {
                    crabka_pgparser::ast::FuncArgs::Exprs(recurse_all(args))
                }
                crabka_pgparser::ast::FuncArgs::Named { positional, named } => {
                    crabka_pgparser::ast::FuncArgs::Named {
                        positional: recurse_all(positional),
                        named: named
                            .iter()
                            .map(|(label, arg)| (label.clone(), *recurse(arg)))
                            .collect(),
                    }
                }
                crabka_pgparser::ast::FuncArgs::Variadic { positional, array } => {
                    crabka_pgparser::ast::FuncArgs::Variadic {
                        positional: recurse_all(positional),
                        array: recurse(array),
                    }
                }
            },
            // An aggregate's own sort keys are ordinary expressions over the
            // same rows, so they are rewritten exactly like its arguments.
            order_by: fc
                .order_by
                .iter()
                .map(|item| crabka_pgparser::ast::OrderItem {
                    expr: *recurse(&item.expr),
                    asc: item.asc,
                    nulls_first: item.nulls_first,
                })
                .collect(),
            within_group: fc.within_group,
            // The FILTER predicate is rewritten like an argument; dropping it
            // would turn a filtered aggregate into an unfiltered one.
            filter: fc.filter.as_deref().map(recurse),
        }),
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: recurse(expr),
            negated: *negated,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: recurse(expr),
            list: recurse_all(list),
            negated: *negated,
        },
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: recurse(expr),
            low: recurse(low),
            high: recurse(high),
            negated: *negated,
        },
        Expr::Like {
            expr,
            pattern,
            negated,
            kind,
            escape,
        } => Expr::Like {
            expr: recurse(expr),
            pattern: recurse(pattern),
            negated: *negated,
            kind: *kind,
            escape: escape.as_ref().map(|e| recurse(e)),
        },
        Expr::Case {
            operand,
            whens,
            else_result,
        } => Expr::Case {
            operand: operand.as_ref().map(|e| recurse(e)),
            whens: whens
                .iter()
                .map(|(c, r)| (*recurse(c), *recurse(r)))
                .collect(),
            else_result: else_result.as_ref().map(|e| recurse(e)),
        },
        Expr::Cast { expr, ty } => Expr::Cast {
            expr: recurse(expr),
            ty: *ty,
        },
        Expr::ArrayLiteral(items) => Expr::ArrayLiteral(recurse_all(items)),
        Expr::Row(items) => Expr::Row(recurse_all(items)),
        Expr::Subscript { base, index } => Expr::Subscript {
            base: recurse(base),
            index: recurse(index),
        },
        other => other.clone(),
    }
}
