//! The function surface of `PostgreSQL`'s seven geometric types — `point`,
//! `lseg`, `line`, `box`, `path`, `polygon` and `circle`.
//!
//! Every name here is *overloaded*: `pg_proc` has three `area`s, four `box`es
//! and five `point`s, and `PostgreSQL` picks between them by argument type.
//! gres dispatches a call by name alone, so the overload has to be resolved
//! inside this module — [`resolve`] is a cut-down `func_select_candidate` that
//! covers the two rules this family actually needs:
//!
//! * an `unknown` literal matches any position, and a call still left with more
//!   than one candidate is 42725 rather than a wrong answer;
//! * a one-argument call whose *name is a type name* is the function-call
//!   spelling of a cast, and that beats the function overloads — which is why
//!   `circle('((0,0),(1,1))')` is a `circle_in` failure rather than
//!   `circle(polygon)`.
//!
//! The arithmetic itself is `crabka_pgtypes::geometry`'s; nothing here computes
//! a coordinate. The C-level `pg_proc` names that back the *prefix* operators
//! (`poly_center`, `path_npoints`, `lseg_length`, `line_horizontal`, …) are
//! callable spellings in `PostgreSQL` too — `geometry.sql` calls `poly_center`
//! directly — so they are listed alongside the SQL-level names. The C names
//! that back the *infix* operators (`box_add`, `dist_pb`, `close_ps`, …) belong
//! with the operators and are not here.

use crabka_pgparser::ast::{Expr, FuncCall};
use crabka_pgtypes::{
    ColumnType, Datum,
    geometry::{Box2, Circle, Line, Lseg, Path, Point, Polygon},
};

use crate::{
    clock::EvalCtx,
    error::ExecError,
    eval::{infer_type, is_unknown_literal},
    func::{checked_args, domain, type_error},
    scope::Scope,
};

/// How many vertices `polygon(circle)` produces. `pg_proc` spells that overload
/// as the SQL body `select pg_catalog.polygon(12, $1)`, so the count belongs to
/// the function rather than to `circle_poly`.
const CIRCLE_POLYGON_VERTICES: i32 = 12;

/// One parameter position of a `pg_proc` entry.
///
/// `Float8` and `Int4` accept the wider numeric family because `PostgreSQL`
/// reaches them through implicit widening — `point(1, 2)` resolves to
/// `point(float8, float8)` with two `int4` arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Param {
    Point,
    Lseg,
    Line,
    Box,
    Path,
    Polygon,
    Circle,
    Float8,
    Int4,
}

impl Param {
    /// The type an argument is coerced to once this position selects it.
    fn column_type(self) -> ColumnType {
        match self {
            Param::Point => ColumnType::Point,
            Param::Lseg => ColumnType::Lseg,
            Param::Line => ColumnType::Line,
            Param::Box => ColumnType::Box,
            Param::Path => ColumnType::Path,
            Param::Polygon => ColumnType::Polygon,
            Param::Circle => ColumnType::Circle,
            Param::Float8 => ColumnType::Float8,
            Param::Int4 => ColumnType::Int4,
        }
    }

    /// Does an argument of type `ty` fill this position through a coercion
    /// `PostgreSQL` performs implicitly?
    fn accepts(self, ty: ColumnType) -> bool {
        match self {
            Param::Float8 => {
                matches!(
                    ty,
                    ColumnType::Int2
                        | ColumnType::Int4
                        | ColumnType::Int8
                        | ColumnType::Float4
                        | ColumnType::Float8
                ) || ty.is_numeric()
            }
            Param::Int4 => matches!(ty, ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8),
            other => other.column_type() == ty,
        }
    }
}

/// One resolved overload, named for the `pg_proc` entry it implements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Geo {
    BoxArea,
    CircleArea,
    PathArea,
    BoxesBoundBox,
    CircleBox,
    PointBox,
    PointsBox,
    PolyBox,
    BoxCenter,
    CircleCenter,
    LsegCenter,
    PolyCenter,
    BoxCircle,
    CrCircle,
    PolyCircle,
    /// `diagonal(box)`, which `lseg(box)` shares.
    BoxDiagonal,
    CircleDiameter,
    CircleRadius,
    BoxHeight,
    BoxWidth,
    PathIsClosed,
    PathIsOpen,
    LineHorizontal,
    LsegHorizontal,
    PointHoriz,
    LineVertical,
    LsegVertical,
    PointVert,
    LineParallel,
    LsegParallel,
    LinePerp,
    LsegPerp,
    LsegLength,
    PathLength,
    LineConstructPp,
    LsegConstruct,
    PathNpoints,
    PolyNpoints,
    PolyPath,
    PathClose,
    PathOpen,
    ConstructPoint,
    BoxPoly,
    /// `polygon(circle)` — `circle_poly` at the built-in vertex count.
    CirclePoly12,
    /// `polygon(int4, circle)`.
    CirclePoly,
    PathPoly,
    PointSlope,
}

impl Geo {
    /// The overload's declared result type.
    fn result_type(self) -> ColumnType {
        match self {
            Geo::BoxArea
            | Geo::CircleArea
            | Geo::PathArea
            | Geo::CircleDiameter
            | Geo::CircleRadius
            | Geo::BoxHeight
            | Geo::BoxWidth
            | Geo::LsegLength
            | Geo::PathLength
            | Geo::PointSlope => ColumnType::Float8,
            Geo::BoxesBoundBox | Geo::CircleBox | Geo::PointBox | Geo::PointsBox | Geo::PolyBox => {
                ColumnType::Box
            }
            Geo::BoxCenter
            | Geo::CircleCenter
            | Geo::LsegCenter
            | Geo::PolyCenter
            | Geo::ConstructPoint => ColumnType::Point,
            Geo::BoxCircle | Geo::CrCircle | Geo::PolyCircle => ColumnType::Circle,
            Geo::BoxDiagonal | Geo::LsegConstruct => ColumnType::Lseg,
            Geo::PathIsClosed
            | Geo::PathIsOpen
            | Geo::LineHorizontal
            | Geo::LsegHorizontal
            | Geo::PointHoriz
            | Geo::LineVertical
            | Geo::LsegVertical
            | Geo::PointVert
            | Geo::LineParallel
            | Geo::LsegParallel
            | Geo::LinePerp
            | Geo::LsegPerp => ColumnType::Bool,
            Geo::LineConstructPp => ColumnType::Line,
            Geo::PathNpoints | Geo::PolyNpoints => ColumnType::Int4,
            Geo::PolyPath | Geo::PathClose | Geo::PathOpen => ColumnType::Path,
            Geo::BoxPoly | Geo::CirclePoly12 | Geo::CirclePoly | Geo::PathPoly => {
                ColumnType::Polygon
            }
        }
    }
}

/// One `pg_proc` entry: its parameter list and the overload it selects.
type Candidate = (&'static [Param], Geo);

/// Every overload of `name`, or `None` when this module does not own the name.
fn candidates(name: &str) -> Option<&'static [Candidate]> {
    sql_candidates(name).or_else(|| c_candidates(name))
}

/// The overloads of the SQL-level names — the spellings `PostgreSQL`'s manual
/// documents.
fn sql_candidates(name: &str) -> Option<&'static [Candidate]> {
    use Geo as G;
    use Param as P;
    Some(match name {
        "area" => &[
            (&[P::Box], G::BoxArea),
            (&[P::Circle], G::CircleArea),
            (&[P::Path], G::PathArea),
        ],
        "bound_box" => &[(&[P::Box, P::Box], G::BoxesBoundBox)],
        "box" => &[
            (&[P::Circle], G::CircleBox),
            (&[P::Point], G::PointBox),
            (&[P::Polygon], G::PolyBox),
            (&[P::Point, P::Point], G::PointsBox),
        ],
        "center" => &[(&[P::Box], G::BoxCenter), (&[P::Circle], G::CircleCenter)],
        "circle" => &[
            (&[P::Box], G::BoxCircle),
            (&[P::Polygon], G::PolyCircle),
            (&[P::Point, P::Float8], G::CrCircle),
        ],
        "diagonal" => &[(&[P::Box], G::BoxDiagonal)],
        "diameter" => &[(&[P::Circle], G::CircleDiameter)],
        "radius" => &[(&[P::Circle], G::CircleRadius)],
        "height" => &[(&[P::Box], G::BoxHeight)],
        "width" => &[(&[P::Box], G::BoxWidth)],
        "isclosed" => &[(&[P::Path], G::PathIsClosed)],
        "isopen" => &[(&[P::Path], G::PathIsOpen)],
        "ishorizontal" => &[
            (&[P::Line], G::LineHorizontal),
            (&[P::Lseg], G::LsegHorizontal),
            (&[P::Point, P::Point], G::PointHoriz),
        ],
        "isvertical" => &[
            (&[P::Line], G::LineVertical),
            (&[P::Lseg], G::LsegVertical),
            (&[P::Point, P::Point], G::PointVert),
        ],
        "isparallel" => &[
            (&[P::Line, P::Line], G::LineParallel),
            (&[P::Lseg, P::Lseg], G::LsegParallel),
        ],
        "isperp" => &[
            (&[P::Line, P::Line], G::LinePerp),
            (&[P::Lseg, P::Lseg], G::LsegPerp),
        ],
        "line" => &[(&[P::Point, P::Point], G::LineConstructPp)],
        "lseg" => &[
            (&[P::Box], G::BoxDiagonal),
            (&[P::Point, P::Point], G::LsegConstruct),
        ],
        "npoints" => &[
            (&[P::Path], G::PathNpoints),
            (&[P::Polygon], G::PolyNpoints),
        ],
        "path" => &[(&[P::Polygon], G::PolyPath)],
        "pclose" => &[(&[P::Path], G::PathClose)],
        "popen" => &[(&[P::Path], G::PathOpen)],
        "point" => &[
            (&[P::Box], G::BoxCenter),
            (&[P::Circle], G::CircleCenter),
            (&[P::Lseg], G::LsegCenter),
            (&[P::Polygon], G::PolyCenter),
            (&[P::Float8, P::Float8], G::ConstructPoint),
        ],
        "polygon" => &[
            (&[P::Box], G::BoxPoly),
            (&[P::Circle], G::CirclePoly12),
            (&[P::Path], G::PathPoly),
            (&[P::Int4, P::Circle], G::CirclePoly),
        ],
        "slope" => &[(&[P::Point, P::Point], G::PointSlope)],
        _ => return None,
    })
}

/// The C-level `proname`s behind the prefix operators (`@@`, `#`, `@-@`, `?-`,
/// `?|`) and the two binary predicate pairs (`?-|`, `?||`). `PostgreSQL`
/// exposes every `pg_proc.proname`, so these resolve as ordinary functions.
fn c_candidates(name: &str) -> Option<&'static [Candidate]> {
    use Geo as G;
    use Param as P;
    Some(match name {
        "box_center" => &[(&[P::Box], G::BoxCenter)],
        "circle_center" => &[(&[P::Circle], G::CircleCenter)],
        "lseg_center" => &[(&[P::Lseg], G::LsegCenter)],
        "poly_center" => &[(&[P::Polygon], G::PolyCenter)],
        "lseg_length" => &[(&[P::Lseg], G::LsegLength)],
        "path_length" => &[(&[P::Path], G::PathLength)],
        "path_npoints" => &[(&[P::Path], G::PathNpoints)],
        "poly_npoints" => &[(&[P::Polygon], G::PolyNpoints)],
        "line_horizontal" => &[(&[P::Line], G::LineHorizontal)],
        "lseg_horizontal" => &[(&[P::Lseg], G::LsegHorizontal)],
        "point_horiz" => &[(&[P::Point, P::Point], G::PointHoriz)],
        "line_vertical" => &[(&[P::Line], G::LineVertical)],
        "lseg_vertical" => &[(&[P::Lseg], G::LsegVertical)],
        "point_vert" => &[(&[P::Point, P::Point], G::PointVert)],
        "line_parallel" => &[(&[P::Line, P::Line], G::LineParallel)],
        "lseg_parallel" => &[(&[P::Lseg, P::Lseg], G::LsegParallel)],
        "line_perp" => &[(&[P::Line, P::Line], G::LinePerp)],
        "lseg_perp" => &[(&[P::Lseg, P::Lseg], G::LsegPerp)],
        _ => return None,
    })
}

/// Is `name` one of this module's functions? (`func::is_scalar` folds this in.)
///
/// `length` is deliberately absent: its `lseg`/`path` overloads share the name
/// with `length(text)`, `length(bit)` and `length(bytea)`, so `func` keeps the
/// name and asks [`geometric_length_type`]/[`length_of`] whether the argument
/// is geometric.
pub(crate) fn is_geometry_func(name: &str) -> bool {
    candidates(name).is_some()
}

/// The type a one-argument call of `name` casts to, when `name` is one of the
/// seven geometric type names. `PostgreSQL`'s parser prefers this reading over
/// the function overloads whenever the single argument is still `unknown`.
fn cast_target(name: &str) -> Option<ColumnType> {
    Some(match name {
        "point" => ColumnType::Point,
        "lseg" => ColumnType::Lseg,
        "line" => ColumnType::Line,
        "box" => ColumnType::Box,
        "path" => ColumnType::Path,
        "polygon" => ColumnType::Polygon,
        "circle" => ColumnType::Circle,
        _ => return None,
    })
}

/// What a call resolved to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Resolved {
    /// A `pg_proc` overload and the parameter list that selected it, so each
    /// argument can be coerced to its position's type before the overload runs.
    Proc(Geo, &'static [Param]),
    /// The function-call spelling of a cast to a geometric type.
    Cast(ColumnType),
}

/// Resolve `name` against its argument types, where `None` is an argument
/// `PostgreSQL` still calls `unknown`.
fn resolve(name: &str, args: &[Option<ColumnType>]) -> Result<Resolved, ExecError> {
    let all = candidates(name).ok_or_else(|| undefined(name, args))?;
    // `typename('literal')` is a coercion request, and the parser reads it that
    // way before it considers the function overloads at all.
    if let [None] = args
        && let Some(target) = cast_target(name)
    {
        return Ok(Resolved::Cast(target));
    }
    // `typename(x)` where `x` is ALREADY that type is the identity coercion, not
    // a function call — `box(box '(1,2),(3,4)')` returns the box. Only the
    // identity resolves this way: `box(lseg …)` is 42883 upstream even though
    // `pg_cast` has a `box → lseg` row, because a coercion in function spelling
    // is only reached when no overload matches AND the cast is to the argument's
    // own type. Checked before the overload scan, which has no identity entry.
    if let [Some(arg)] = args
        && let Some(target) = cast_target(name)
        && *arg == target
    {
        return Ok(Resolved::Cast(target));
    }
    let mut matched = all.iter().filter(|(params, _)| {
        params.len() == args.len()
            && params
                .iter()
                .zip(args)
                .all(|(param, arg)| arg.is_none_or(|ty| param.accepts(ty)))
    });
    let Some(&(params, geo)) = matched.next() else {
        return Err(undefined(name, args));
    };
    if matched.next().is_some() {
        return Err(ambiguous(name));
    }
    Ok(Resolved::Proc(geo, params))
}

/// `PostgreSQL`'s 42883 for a call whose argument types select no overload.
fn undefined(name: &str, args: &[Option<ColumnType>]) -> ExecError {
    let spelled: Vec<&str> = args
        .iter()
        .map(|ty| ty.map_or("unknown", ColumnType::name))
        .collect();
    ExecError::UndefinedFunction(format!(
        "function {name}({}) does not exist",
        spelled.join(", ")
    ))
}

/// `PostgreSQL`'s 42725 for an all-`unknown` call that still has several
/// candidates. Only the names below reach it: every other name in this module
/// has one candidate per arity, and the seven type names take the cast reading
/// instead.
fn ambiguous(name: &str) -> ExecError {
    let message = match name {
        "area" => "function area(unknown) is not unique",
        "center" => "function center(unknown) is not unique",
        "npoints" => "function npoints(unknown) is not unique",
        "ishorizontal" => "function ishorizontal(unknown) is not unique",
        "isvertical" => "function isvertical(unknown) is not unique",
        "isparallel" => "function isparallel(unknown, unknown) is not unique",
        "isperp" => "function isperp(unknown, unknown) is not unique",
        _ => "function is not unique",
    };
    domain("42725", message)
}

/// The argument types of a call at plan time, with `unknown` left open.
fn arg_types(args: &[Expr], scope: &Scope) -> Result<Vec<Option<ColumnType>>, ExecError> {
    args.iter()
        .map(|arg| {
            if is_unknown_literal(arg) {
                Ok(None)
            } else {
                infer_type(arg, scope).map(Some)
            }
        })
        .collect()
}

/// Statically infer a geometric call's result type, validating its arity and
/// argument types.
pub(crate) fn geometry_func_result_type(
    fc: &FuncCall,
    scope: &Scope,
) -> Result<ColumnType, ExecError> {
    let args = checked_args(fc)?;
    let types = arg_types(args, scope)?;
    Ok(match resolve(&fc.name, &types)? {
        Resolved::Cast(target) => target,
        Resolved::Proc(geo, _) => geo.result_type(),
    })
}

/// Evaluate a geometric call.
///
/// Every function in the family is strict, so a NULL argument answers NULL
/// *before* the overload is resolved: a NULL Datum has no type of its own, and
/// a NULL in a typed column must not read as an `unknown` literal.
pub(crate) fn eval_geometry(
    fc: &FuncCall,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let args = checked_args(fc)?;
    let mut values = args
        .iter()
        .map(&mut eval_child)
        .collect::<Result<Vec<_>, _>>()?;
    if values.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    let types: Vec<Option<ColumnType>> = args
        .iter()
        .zip(&values)
        .map(|(arg, value)| {
            if is_unknown_literal(arg) {
                None
            } else {
                value.column_type()
            }
        })
        .collect();
    let (geo, params) = match resolve(&fc.name, &types)? {
        Resolved::Cast(target) => return cast(&values[0], target, ctx),
        Resolved::Proc(geo, params) => (geo, params),
    };
    // Run the implicit coercion the position selected: an `unknown` literal is
    // parsed into its type, and an integer at a `float8` position widens.
    for (index, param) in params.iter().enumerate() {
        let target = param.column_type();
        if types[index] != Some(target) {
            values[index] = cast(&values[index], target, ctx)?;
        }
    }
    apply(geo, &values)
}

fn cast(value: &Datum, target: ColumnType, ctx: &EvalCtx) -> Result<Datum, ExecError> {
    crate::eval::cast_value(value, target, &ctx.time_zone)
}

/// Run a resolved overload over arguments already coerced to its parameter
/// types. Every `Geo` has one arity, and it is the arity of the parameter list
/// that selected it, so a binary overload always has its second argument.
fn apply(geo: Geo, values: &[Datum]) -> Result<Datum, ExecError> {
    let value = &values[0];
    Ok(match geo {
        Geo::BoxArea => Datum::Float8(box_of(value)?.area()),
        Geo::CircleArea => Datum::Float8(circle_of(value)?.area()),
        // `area(path)` declines an open path with NULL rather than raising —
        // the one conversion in the family that does.
        Geo::PathArea => path_of(value)?.area().map_or(Datum::Null, Datum::Float8),
        Geo::CircleBox => Datum::Box(circle_of(value)?.to_box()),
        Geo::PointBox => Datum::Box(Box2::of_point(point_of(value)?)),
        Geo::PolyBox => Datum::Box(polygon_of(value)?.to_box()),
        Geo::BoxCenter => Datum::Point(box_of(value)?.center()),
        Geo::CircleCenter => Datum::Point(circle_of(value)?.to_point()),
        Geo::LsegCenter => Datum::Point(lseg_of(value)?.center()),
        Geo::PolyCenter => Datum::Point(polygon_of(value)?.to_point()),
        Geo::BoxCircle => Datum::Circle(box_of(value)?.to_circle()),
        Geo::PolyCircle => Datum::Circle(polygon_of(value)?.to_circle()),
        Geo::BoxDiagonal => Datum::Lseg(box_of(value)?.diagonal()),
        Geo::CircleDiameter => Datum::Float8(circle_of(value)?.diameter()),
        Geo::CircleRadius => Datum::Float8(circle_of(value)?.radius),
        Geo::BoxHeight => Datum::Float8(box_of(value)?.height()),
        Geo::BoxWidth => Datum::Float8(box_of(value)?.width()),
        Geo::PathIsClosed => Datum::Bool(path_of(value)?.is_closed()),
        Geo::PathIsOpen => Datum::Bool(path_of(value)?.is_open()),
        Geo::LineHorizontal => Datum::Bool(line_of(value)?.is_horizontal()),
        Geo::LsegHorizontal => Datum::Bool(lseg_of(value)?.is_horizontal()),
        Geo::LineVertical => Datum::Bool(line_of(value)?.is_vertical()),
        Geo::LsegVertical => Datum::Bool(lseg_of(value)?.is_vertical()),
        Geo::LsegLength => Datum::Float8(lseg_of(value)?.length()),
        Geo::PathLength => Datum::Float8(path_of(value)?.length()),
        Geo::PathNpoints => Datum::Int4(path_of(value)?.npoints()),
        Geo::PolyNpoints => Datum::Int4(polygon_of(value)?.npoints()),
        Geo::PolyPath => Datum::Path(polygon_of(value)?.to_path()),
        Geo::PathClose => Datum::Path(path_of(value)?.to_closed()),
        Geo::PathOpen => Datum::Path(path_of(value)?.to_open()),
        Geo::BoxPoly => Datum::Polygon(box_of(value)?.to_polygon()),
        Geo::CirclePoly12 => Datum::Polygon(
            circle_of(value)?
                .to_polygon(CIRCLE_POLYGON_VERTICES)
                .map_err(ExecError::Type)?,
        ),
        // `path_poly` is the one conversion that refuses instead of answering
        // NULL: an open path has no interior.
        Geo::PathPoly => Datum::Polygon(path_of(value)?.to_polygon().map_err(ExecError::Type)?),
        binary => return apply_binary(binary, value, &values[1]),
    })
}

/// The two-argument half of [`apply`], split out so neither match runs past a
/// screenful.
fn apply_binary(geo: Geo, left: &Datum, right: &Datum) -> Result<Datum, ExecError> {
    Ok(match geo {
        Geo::BoxesBoundBox => Datum::Box(box_of(left)?.bound_box(box_of(right)?)),
        Geo::PointsBox => Datum::Box(point_of(left)?.box_with(point_of(right)?)),
        Geo::CrCircle => Datum::Circle(point_of(left)?.circle_with(f64_of(right)?)),
        Geo::PointHoriz => Datum::Bool(point_of(left)?.is_horizontal_with(point_of(right)?)),
        Geo::PointVert => Datum::Bool(point_of(left)?.is_vertical_with(point_of(right)?)),
        Geo::LineParallel => Datum::Bool(line_of(left)?.is_parallel_to(line_of(right)?)),
        Geo::LsegParallel => Datum::Bool(lseg_of(left)?.is_parallel_to(lseg_of(right)?)),
        Geo::LinePerp => Datum::Bool(line_of(left)?.is_perpendicular_to(line_of(right)?)),
        Geo::LsegPerp => Datum::Bool(lseg_of(left)?.is_perpendicular_to(lseg_of(right)?)),
        // Two equal points name no line: `line_construct_pp` raises 22023,
        // while the identical complaint out of `line_in` is 22P02.
        Geo::LineConstructPp => Datum::Line(
            point_of(left)?
                .line_with(point_of(right)?)
                .map_err(ExecError::Type)?,
        ),
        Geo::LsegConstruct => Datum::Lseg(point_of(left)?.lseg_with(point_of(right)?)),
        Geo::ConstructPoint => Datum::Point(Point {
            x: f64_of(left)?,
            y: f64_of(right)?,
        }),
        Geo::CirclePoly => Datum::Polygon(
            circle_of(right)?
                .to_polygon(i32_of(left)?)
                .map_err(ExecError::Type)?,
        ),
        Geo::PointSlope => Datum::Float8(point_of(left)?.slope(point_of(right)?)),
        // `apply` answers every unary overload itself and only delegates the
        // binary ones, so this arm is unreachable; it keeps the match total
        // without a panic.
        _ => return Err(type_error("function", left)),
    })
}

// ---------------------------------------------------------------------------
// Argument extraction
//
// Resolution has already checked every argument's type and the coercion loop
// has already applied its position's cast, so these only fail for a call that
// reached the evaluator without a plan-time type pass — a geometric function
// in a WHERE clause over a mistyped column, where PostgreSQL reports 42883 at
// plan time and gres reports 42804 here.
// ---------------------------------------------------------------------------

fn point_of(value: &Datum) -> Result<Point, ExecError> {
    match value {
        Datum::Point(point) => Ok(*point),
        other => Err(type_error("function", other)),
    }
}

fn lseg_of(value: &Datum) -> Result<Lseg, ExecError> {
    match value {
        Datum::Lseg(lseg) => Ok(*lseg),
        other => Err(type_error("function", other)),
    }
}

fn line_of(value: &Datum) -> Result<Line, ExecError> {
    match value {
        Datum::Line(line) => Ok(*line),
        other => Err(type_error("function", other)),
    }
}

fn box_of(value: &Datum) -> Result<Box2, ExecError> {
    match value {
        Datum::Box(value) => Ok(*value),
        other => Err(type_error("function", other)),
    }
}

fn circle_of(value: &Datum) -> Result<Circle, ExecError> {
    match value {
        Datum::Circle(circle) => Ok(*circle),
        other => Err(type_error("function", other)),
    }
}

fn path_of(value: &Datum) -> Result<&Path, ExecError> {
    match value {
        Datum::Path(path) => Ok(path),
        other => Err(type_error("function", other)),
    }
}

fn polygon_of(value: &Datum) -> Result<&Polygon, ExecError> {
    match value {
        Datum::Polygon(polygon) => Ok(polygon),
        other => Err(type_error("function", other)),
    }
}

fn f64_of(value: &Datum) -> Result<f64, ExecError> {
    match value {
        Datum::Float8(x) => Ok(*x),
        other => Err(type_error("function", other)),
    }
}

fn i32_of(value: &Datum) -> Result<i32, ExecError> {
    match value {
        Datum::Int4(n) => Ok(*n),
        other => Err(type_error("function", other)),
    }
}

// ---------------------------------------------------------------------------
// The `length` overloads, which share their name with the string family
// ---------------------------------------------------------------------------

/// The result type of `length(lseg)` / `length(path)`, or `None` when `ty` is
/// neither of the two geometric overloads.
///
/// `char_length` and `character_length` have no geometric overload — only the
/// `length` spelling does — so the caller checks the name too.
pub(crate) fn geometric_length_type(ty: ColumnType) -> Option<ColumnType> {
    matches!(ty, ColumnType::Lseg | ColumnType::Path).then_some(ColumnType::Float8)
}

/// `length(lseg)` / `length(path)` over an already-evaluated argument, or
/// `None` when the value is not geometric and the string overloads still apply.
pub(crate) fn length_of(value: &Datum) -> Option<f64> {
    match value {
        Datum::Lseg(lseg) => Some(lseg.length()),
        Datum::Path(path) => Some(path.length()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgparser::ast::FuncArgs;

    use super::*;

    /// A typed argument: a literal wrapped in an explicit cast, so `infer_type`
    /// and `Datum::column_type` both report the type the fixture names.
    fn typed(text: &str, ty: ColumnType) -> (Expr, Datum) {
        let datum = crabka_pgtypes::cast::cast(
            &Datum::Text(text.to_string()),
            ty,
            &jiff::tz::TimeZone::UTC,
        )
        .expect("the fixture literal parses");
        (
            Expr::Cast {
                expr: Box::new(Expr::StringLiteral(text.to_string())),
                ty,
            },
            datum,
        )
    }

    /// A bare literal — what `PostgreSQL` still calls `unknown`.
    fn bare(text: &str) -> (Expr, Datum) {
        (
            Expr::StringLiteral(text.to_string()),
            Datum::Text(text.to_string()),
        )
    }

    fn eval(name: &str, args: Vec<(Expr, Datum)>) -> Result<Datum, ExecError> {
        let (exprs, values): (Vec<Expr>, Vec<Datum>) = args.into_iter().unzip();
        let fc = FuncCall {
            name: name.to_string(),
            distinct: false,
            args: FuncArgs::Exprs(exprs),
            filter: None,
        };
        let ctx = EvalCtx::test_default();
        let mut next = values.into_iter();
        eval_geometry(&fc, &ctx, |_| {
            Ok(next.next().expect("one value per argument"))
        })
    }

    fn rendered(value: &Datum) -> String {
        String::from_utf8(crabka_pgtypes::encoding::encode_text(
            value,
            &jiff::tz::TimeZone::UTC,
        ))
        .expect("a Datum's text encoding is valid UTF-8")
    }

    /// Every one-argument overload, against the value `PostgreSQL` 18.4 prints.
    #[test]
    fn one_argument_overloads_match_the_oracle() {
        let cases: [(&str, &str, ColumnType, &str); 33] = [
            ("area", "(0,0),(2,3)", ColumnType::Box, "6"),
            (
                "area",
                "<(0,0),2>",
                ColumnType::Circle,
                "12.566370614359172",
            ),
            ("area", "((0,0),(2,0),(2,2),(0,2))", ColumnType::Path, "4"),
            (
                "box",
                "<(1,1),2>",
                ColumnType::Circle,
                "(2.414213562373095,2.414213562373095),(-0.4142135623730949,-0.4142135623730949)",
            ),
            ("box", "(1,2)", ColumnType::Point, "(1,2),(1,2)"),
            (
                "box",
                "((0,0),(2,0),(2,2))",
                ColumnType::Polygon,
                "(2,2),(0,0)",
            ),
            ("center", "(0,0),(2,4)", ColumnType::Box, "(1,2)"),
            ("center", "<(1,2),3>", ColumnType::Circle, "(1,2)"),
            (
                "circle",
                "(0,0),(2,2)",
                ColumnType::Box,
                "<(1,1),1.4142135623730951>",
            ),
            (
                "circle",
                "((0,0),(2,0),(2,2),(0,2))",
                ColumnType::Polygon,
                "<(1,1),1.4142135623730951>",
            ),
            ("diagonal", "(0,0),(2,3)", ColumnType::Box, "[(2,3),(0,0)]"),
            ("diameter", "<(0,0),2.5>", ColumnType::Circle, "5"),
            ("radius", "<(0,0),2.5>", ColumnType::Circle, "2.5"),
            ("height", "(0,0),(2,3)", ColumnType::Box, "3"),
            ("width", "(0,0),(2,3)", ColumnType::Box, "2"),
            ("isclosed", "((0,0),(1,1))", ColumnType::Path, "t"),
            ("isclosed", "[(0,0),(1,1)]", ColumnType::Path, "f"),
            ("isopen", "[(0,0),(1,1)]", ColumnType::Path, "t"),
            ("isopen", "((0,0),(1,1))", ColumnType::Path, "f"),
            ("ishorizontal", "{0,1,0}", ColumnType::Line, "t"),
            ("ishorizontal", "[(0,0),(1,0)]", ColumnType::Lseg, "t"),
            ("isvertical", "{1,0,0}", ColumnType::Line, "t"),
            ("isvertical", "[(0,0),(0,1)]", ColumnType::Lseg, "t"),
            ("lseg", "(0,0),(2,3)", ColumnType::Box, "[(2,3),(0,0)]"),
            ("npoints", "((0,0),(1,1),(2,2))", ColumnType::Path, "3"),
            ("npoints", "((0,0),(1,1),(2,2))", ColumnType::Polygon, "3"),
            (
                "path",
                "((0,0),(1,1),(2,2))",
                ColumnType::Polygon,
                "((0,0),(1,1),(2,2))",
            ),
            ("pclose", "[(0,0),(1,1)]", ColumnType::Path, "((0,0),(1,1))"),
            ("popen", "((0,0),(1,1))", ColumnType::Path, "[(0,0),(1,1)]"),
            ("point", "(0,0),(2,4)", ColumnType::Box, "(1,2)"),
            ("point", "<(1,2),3>", ColumnType::Circle, "(1,2)"),
            ("point", "[(0,0),(2,4)]", ColumnType::Lseg, "(1,2)"),
            (
                "point",
                "((0,0),(2,0),(2,2),(0,2))",
                ColumnType::Polygon,
                "(1,1)",
            ),
        ];
        for (name, literal, ty, expected) in cases {
            let value = eval(name, vec![typed(literal, ty)])
                .unwrap_or_else(|e| panic!("{name}({literal}::{}): {e:?}", ty.name()));
            assert!(
                rendered(&value) == expected,
                "{name}({literal}::{})",
                ty.name()
            );
        }
    }

    /// `polygon(box)` and `polygon(path)`, whose results are too long for the
    /// table above to read well.
    #[test]
    fn polygon_conversions_match_the_oracle() {
        let value =
            eval("polygon", vec![typed("(0,0),(1,1)", ColumnType::Box)]).expect("polygon(box)");
        assert!(rendered(&value) == "((0,0),(0,1),(1,1),(1,0))");
        let value = eval(
            "polygon",
            vec![typed("((0,0),(1,1),(2,2))", ColumnType::Path)],
        )
        .expect("polygon(closed path)");
        assert!(rendered(&value) == "((0,0),(1,1),(2,2))");
    }

    /// Every two-argument overload.
    #[test]
    fn two_argument_overloads_match_the_oracle() {
        // name, left literal, right literal, the type both arguments take (the
        // only overload with two different parameter types is `polygon`), and
        // the value PostgreSQL 18.4 prints.
        let cases: [(&str, &str, &str, ColumnType, &str); 11] = [
            (
                "bound_box",
                "(0,0),(1,1)",
                "(2,2),(3,3)",
                ColumnType::Box,
                "(3,3),(0,0)",
            ),
            ("box", "(1,2)", "(3,4)", ColumnType::Point, "(3,4),(1,2)"),
            ("lseg", "(1,2)", "(3,4)", ColumnType::Point, "[(1,2),(3,4)]"),
            ("line", "(0,0)", "(1,1)", ColumnType::Point, "{1,-1,0}"),
            ("ishorizontal", "(0,0)", "(1,0)", ColumnType::Point, "t"),
            ("isvertical", "(0,0)", "(0,1)", ColumnType::Point, "t"),
            ("isparallel", "{1,1,0}", "{1,1,5}", ColumnType::Line, "t"),
            (
                "isparallel",
                "[(0,0),(1,1)]",
                "[(0,1),(1,2)]",
                ColumnType::Lseg,
                "t",
            ),
            ("isperp", "{1,1,0}", "{1,-1,0}", ColumnType::Line, "t"),
            (
                "isperp",
                "[(0,0),(1,1)]",
                "[(0,0),(1,-1)]",
                ColumnType::Lseg,
                "t",
            ),
            ("slope", "(0,0)", "(2,1)", ColumnType::Point, "0.5"),
        ];
        for (name, left, right, ty, expected) in cases {
            let value = eval(name, vec![typed(left, ty), typed(right, ty)])
                .unwrap_or_else(|e| panic!("{name}({left}, {right}): {e:?}"));
            assert!(rendered(&value) == expected, "{name}({left}, {right})");
        }
        // `polygon(int4, circle)` is the one mixed-type pair.
        let value = eval(
            "polygon",
            vec![
                typed("4", ColumnType::Int4),
                typed("<(0,0),1>", ColumnType::Circle),
            ],
        )
        .expect("polygon(int4, circle)");
        assert!(
            rendered(&value)
                == "((-1,0),(-6.123233995736766e-17,1),(1,1.2246467991473532e-16),(1.8369701987210297e-16,-1))"
        );
    }

    /// An integer at a `float8` position widens the way `PostgreSQL`'s implicit
    /// cast does, so `point(1, 2)` and `circle(point, 3)` both resolve.
    #[test]
    fn a_numeric_position_accepts_the_whole_integer_family() {
        let value = eval(
            "point",
            vec![
                (Expr::IntLiteral("1".into()), Datum::Int4(1)),
                (Expr::NumericLiteral("2.5".into()), Datum::Float8(2.5)),
            ],
        )
        .expect("point(int4, float8)");
        assert!(rendered(&value) == "(1,2.5)");
        let value = eval(
            "circle",
            vec![
                typed("(1,2)", ColumnType::Point),
                (Expr::IntLiteral("3".into()), Datum::Int4(3)),
            ],
        )
        .expect("circle(point, int4)");
        assert!(rendered(&value) == "<(1,2),3>");
    }

    /// `polygon(circle)` uses the twelve vertices `pg_proc` spells into its SQL
    /// body, not a caller-supplied count.
    #[test]
    fn polygon_of_a_circle_has_twelve_vertices() {
        let value =
            eval("polygon", vec![typed("<(0,0),1>", ColumnType::Circle)]).expect("polygon(circle)");
        let Datum::Polygon(polygon) = &value else {
            panic!("polygon(circle) is a polygon, got {value:?}");
        };
        assert!(polygon.npoints() == 12);
        assert!(rendered(&value).starts_with("((-1,0),(-0.8660254037844387,"));
    }

    /// `box(circle)` is the *inscribed* box, whose corner sits on the
    /// circumference at `r/√2` — not the `(1,1),(-1,-1)` bounding box the
    /// positional operators use.
    #[test]
    fn box_of_a_circle_is_inscribed() {
        let value = eval("box", vec![typed("<(0,0),1>", ColumnType::Circle)]).expect("box(circle)");
        assert!(
            rendered(&value)
                == "(0.7071067811865475,0.7071067811865475),(-0.7071067811865475,-0.7071067811865475)"
        );
    }

    /// `area(path)` declines an open path with NULL; `polygon(path)` refuses it
    /// with 22023. The neighbouring conversions disagree upstream too.
    #[test]
    fn an_open_path_is_null_for_area_and_an_error_for_polygon() {
        let open = || typed("[(0,0),(2,0),(2,2)]", ColumnType::Path);
        assert!(eval("area", vec![open()]).expect("area(open path)") == Datum::Null);
        let pg = eval("polygon", vec![open()])
            .expect_err("polygon(open path)")
            .into_pg();
        assert!(pg.code == "22023");
        assert!(pg.message == "open path cannot be converted to polygon");
    }

    /// `line(point, point)` on one point is 22023, while the same complaint out
    /// of `line_in` is 22P02.
    #[test]
    fn a_line_through_one_point_is_an_invalid_specification() {
        let pg = eval(
            "line",
            vec![
                typed("(1,1)", ColumnType::Point),
                typed("(1,1)", ColumnType::Point),
            ],
        )
        .expect_err("line(p, p) on equal points")
        .into_pg();
        assert!(pg.code == "22023");
        assert!(pg.message == "invalid line specification: must be two distinct points");
    }

    #[test]
    fn a_circle_polygon_checks_its_radius_and_its_vertex_count() {
        let zero = eval("polygon", vec![typed("<(0,0),0>", ColumnType::Circle)])
            .expect_err("polygon(circle) with radius zero");
        assert!(zero.into_pg().code == "0A000");
        let pg = eval(
            "polygon",
            vec![
                (Expr::IntLiteral("1".into()), Datum::Int4(1)),
                typed("<(0,0),1>", ColumnType::Circle),
            ],
        )
        .expect_err("polygon(1, circle)")
        .into_pg();
        assert!(pg.code == "22023");
        assert!(pg.message == "must request at least 2 points");
    }

    /// An argument type with no overload is 42883, spelled the way `PostgreSQL`
    /// spells it — with the type name, not a bare `(...)`.
    #[test]
    fn an_argument_type_with_no_overload_names_the_type() {
        let cases: [(&str, &str, ColumnType); 9] = [
            ("area", "[(0,0),(1,1)]", ColumnType::Lseg),
            ("area", "(0,0)", ColumnType::Point),
            ("npoints", "(0,0),(1,1)", ColumnType::Box),
            ("center", "[(0,0),(1,1)]", ColumnType::Lseg),
            ("isclosed", "((0,0),(1,1))", ColumnType::Polygon),
            ("diameter", "(0,0),(1,1)", ColumnType::Box),
            ("radius", "(0,0),(1,1)", ColumnType::Box),
            ("width", "<(0,0),1>", ColumnType::Circle),
            ("poly_center", "(0,0),(1,1)", ColumnType::Box),
        ];
        for (name, literal, ty) in cases {
            let pg = eval(name, vec![typed(literal, ty)])
                .expect_err("no overload accepts this type")
                .into_pg();
            assert!(pg.code == "42883", "{name}({})", ty.name());
            assert!(
                pg.message == format!("function {name}({}) does not exist", ty.name()),
                "{name}({}) said {}",
                ty.name(),
                pg.message
            );
        }
    }

    /// A non-geometric argument gets the same 42883 wording, naming its type.
    #[test]
    fn a_non_geometric_argument_is_undefined() {
        let int = || (Expr::IntLiteral("1".into()), Datum::Int4(1));
        let pg = eval("area", vec![int()])
            .expect_err("area(integer)")
            .into_pg();
        assert!(pg.code == "42883");
        assert!(pg.message == "function area(integer) does not exist");
        let pg = eval("box", vec![int(), int()])
            .expect_err("box(integer, integer)")
            .into_pg();
        assert!(pg.message == "function box(integer, integer) does not exist");
        let pg = eval("area", vec![typed("x", ColumnType::Text)])
            .expect_err("area(text)")
            .into_pg();
        assert!(pg.message == "function area(text) does not exist");
    }

    /// A one-argument call whose name is a type name is the cast spelling, and
    /// it beats the function overloads — `circle('((0,0),(1,1))')` is a
    /// `circle_in` failure, not `circle(polygon)`.
    #[test]
    fn a_type_named_call_over_a_bare_literal_is_the_cast() {
        let cases: [(&str, &str, &str); 4] = [
            ("box", "(0,0),(1,1)", "(1,1),(0,0)"),
            ("polygon", "((0,0),(1,1))", "((0,0),(1,1))"),
            ("lseg", "(0,0),(1,1)", "[(0,0),(1,1)]"),
            ("path", "((0,0),(1,1))", "((0,0),(1,1))"),
        ];
        for (name, literal, expected) in cases {
            let value = eval(name, vec![bare(literal)])
                .unwrap_or_else(|e| panic!("{name}('{literal}'): {e:?}"));
            assert!(rendered(&value) == expected, "{name}('{literal}')");
        }
        for (name, literal) in [("circle", "((0,0),(1,1))"), ("point", "(0,0),(1,1)")] {
            let pg = eval(name, vec![bare(literal)])
                .expect_err("the type's input function rejects it")
                .into_pg();
            assert!(pg.code == "22P02", "{name}('{literal}')");
        }
    }

    /// `typename(x)` where `x` is ALREADY that type is the identity coercion,
    /// which no overload table has an entry for. ONLY the identity resolves
    /// this way: `box(lseg …)` is 42883 upstream even though `pg_cast` has a
    /// `box → lseg` row, so the fallback must not be "any declared cast".
    ///
    /// Every expected value came from `PostgreSQL` 18.4.
    #[test]
    fn a_type_named_call_on_its_own_type_is_the_identity() {
        let cases: [(&str, ColumnType, &str, &str); 7] = [
            ("point", ColumnType::Point, "(1,2)", "(1,2)"),
            ("lseg", ColumnType::Lseg, "[(1,2),(3,4)]", "[(1,2),(3,4)]"),
            ("path", ColumnType::Path, "[(1,2),(3,4)]", "[(1,2),(3,4)]"),
            ("box", ColumnType::Box, "(1,2),(3,4)", "(3,4),(1,2)"),
            (
                "polygon",
                ColumnType::Polygon,
                "((1,2),(3,4))",
                "((1,2),(3,4))",
            ),
            ("line", ColumnType::Line, "{1,-1,0}", "{1,-1,0}"),
            ("circle", ColumnType::Circle, "<(1,2),3>", "<(1,2),3>"),
        ];
        for (name, ty, literal, expected) in cases {
            let value = eval(name, vec![typed(literal, ty)])
                .unwrap_or_else(|e| panic!("{name}({literal}): {e:?}"));
            assert!(rendered(&value) == expected, "{name}({literal})");
        }
        // A DIFFERENT geometric type with no matching overload stays 42883 even
        // where `pg_cast` declares the conversion.
        for (name, ty, literal, message) in [
            (
                "box",
                ColumnType::Lseg,
                "[(1,2),(3,4)]",
                "function box(lseg) does not exist",
            ),
            (
                "lseg",
                ColumnType::Point,
                "(1,2)",
                "function lseg(point) does not exist",
            ),
            (
                "line",
                ColumnType::Box,
                "(1,2),(3,4)",
                "function line(box) does not exist",
            ),
            (
                "circle",
                ColumnType::Point,
                "(1,2)",
                "function circle(point) does not exist",
            ),
        ] {
            let pg = eval(name, vec![typed(literal, ty)])
                .expect_err(name)
                .into_pg();
            assert!(
                (pg.code.as_str(), pg.message.as_str()) == ("42883", message),
                "{name}"
            );
        }
    }

    /// A name with several candidates and nothing but `unknown` arguments is
    /// 42725, exactly as `PostgreSQL` reports it.
    #[test]
    fn an_all_unknown_call_with_several_candidates_is_not_unique() {
        let cases: [(&str, usize, &str); 7] = [
            ("area", 1, "function area(unknown) is not unique"),
            ("center", 1, "function center(unknown) is not unique"),
            ("npoints", 1, "function npoints(unknown) is not unique"),
            (
                "ishorizontal",
                1,
                "function ishorizontal(unknown) is not unique",
            ),
            (
                "isvertical",
                1,
                "function isvertical(unknown) is not unique",
            ),
            (
                "isparallel",
                2,
                "function isparallel(unknown, unknown) is not unique",
            ),
            (
                "isperp",
                2,
                "function isperp(unknown, unknown) is not unique",
            ),
        ];
        for (name, arity, message) in cases {
            let args = (0..arity).map(|_| bare("(0,0),(1,1)")).collect();
            let pg = eval(name, args).expect_err("an ambiguous call").into_pg();
            assert!(pg.code == "42725", "{name}");
            assert!(pg.message == message, "{name} said {}", pg.message);
        }
    }

    /// One typed argument settles a call the all-`unknown` form could not.
    #[test]
    fn one_typed_argument_settles_an_otherwise_ambiguous_call() {
        let value = eval(
            "isparallel",
            vec![typed("{1,1,0}", ColumnType::Line), bare("{1,1,5}")],
        )
        .expect("isparallel(line, unknown)");
        assert!(value == Datum::Bool(true));
        let value = eval(
            "bound_box",
            vec![bare("(0,0),(1,1)"), typed("(2,2),(3,3)", ColumnType::Box)],
        )
        .expect("bound_box(unknown, box)");
        assert!(rendered(&value) == "(3,3),(0,0)");
    }

    /// Every function in the family is strict.
    #[test]
    fn a_null_argument_makes_the_result_null() {
        let cases: [(&str, Vec<(Expr, Datum)>); 4] = [
            ("area", vec![(Expr::NullLiteral, Datum::Null)]),
            (
                "area",
                vec![(
                    Expr::Cast {
                        expr: Box::new(Expr::NullLiteral),
                        ty: ColumnType::Box,
                    },
                    Datum::Null,
                )],
            ),
            (
                "slope",
                vec![
                    typed("(0,0)", ColumnType::Point),
                    (Expr::NullLiteral, Datum::Null),
                ],
            ),
            (
                "polygon",
                vec![
                    (Expr::NullLiteral, Datum::Null),
                    typed("<(0,0),1>", ColumnType::Circle),
                ],
            ),
        ];
        for (name, args) in cases {
            assert!(
                eval(name, args).expect("a strict call") == Datum::Null,
                "{name}"
            );
        }
    }

    /// The C-level `proname`s behind the prefix operators are ordinary callable
    /// functions in `PostgreSQL`, and `geometry.sql` calls `poly_center`.
    #[test]
    fn the_c_level_prefix_operator_names_are_callable() {
        let unary: [(&str, &str, ColumnType, &str); 12] = [
            (
                "poly_center",
                "((0,0),(2,0),(2,2),(0,2))",
                ColumnType::Polygon,
                "(1,1)",
            ),
            ("circle_center", "<(1,2),3>", ColumnType::Circle, "(1,2)"),
            ("lseg_center", "[(0,0),(2,4)]", ColumnType::Lseg, "(1,2)"),
            ("box_center", "(0,0),(2,4)", ColumnType::Box, "(1,2)"),
            ("lseg_length", "[(0,0),(3,4)]", ColumnType::Lseg, "5"),
            ("path_length", "((0,0),(3,0),(3,4))", ColumnType::Path, "12"),
            ("path_npoints", "((0,0),(1,1))", ColumnType::Path, "2"),
            ("poly_npoints", "((0,0),(1,1))", ColumnType::Polygon, "2"),
            ("line_horizontal", "{0,1,0}", ColumnType::Line, "t"),
            ("lseg_horizontal", "[(0,0),(1,0)]", ColumnType::Lseg, "t"),
            ("line_vertical", "{1,0,0}", ColumnType::Line, "t"),
            ("lseg_vertical", "[(0,0),(0,1)]", ColumnType::Lseg, "t"),
        ];
        for (name, literal, ty, expected) in unary {
            let value = eval(name, vec![typed(literal, ty)])
                .unwrap_or_else(|e| panic!("{name}({literal}): {e:?}"));
            assert!(rendered(&value) == expected, "{name}({literal})");
        }
        let binary: [(&str, &str, &str, ColumnType, &str); 6] = [
            ("point_horiz", "(0,0)", "(1,0)", ColumnType::Point, "t"),
            ("point_vert", "(0,0)", "(1,0)", ColumnType::Point, "f"),
            ("line_parallel", "{1,1,0}", "{1,1,5}", ColumnType::Line, "t"),
            ("line_perp", "{1,1,0}", "{1,-1,0}", ColumnType::Line, "t"),
            (
                "lseg_parallel",
                "[(0,0),(1,1)]",
                "[(0,1),(1,2)]",
                ColumnType::Lseg,
                "t",
            ),
            (
                "lseg_perp",
                "[(0,0),(1,1)]",
                "[(0,0),(1,-1)]",
                ColumnType::Lseg,
                "t",
            ),
        ];
        for (name, left, right, ty, expected) in binary {
            let value = eval(name, vec![typed(left, ty), typed(right, ty)])
                .unwrap_or_else(|e| panic!("{name}({left}, {right}): {e:?}"));
            assert!(rendered(&value) == expected, "{name}({left}, {right})");
        }
    }

    /// `length` stays with the string family; this module only answers for the
    /// two geometric overloads.
    #[test]
    fn length_answers_only_for_the_geometric_overloads() {
        assert!(geometric_length_type(ColumnType::Lseg) == Some(ColumnType::Float8));
        assert!(geometric_length_type(ColumnType::Path) == Some(ColumnType::Float8));
        for ty in [
            ColumnType::Text,
            ColumnType::Bytea,
            ColumnType::Bit(None),
            ColumnType::Box,
            ColumnType::Polygon,
        ] {
            assert!(geometric_length_type(ty) == None, "{}", ty.name());
        }
        let (_, lseg) = typed("[(0,0),(3,4)]", ColumnType::Lseg);
        assert!(length_of(&lseg) == Some(5.0));
        let (_, path) = typed("((0,0),(3,0),(3,4))", ColumnType::Path);
        assert!(length_of(&path) == Some(12.0));
        assert!(length_of(&Datum::Text("abc".into())) == None);
        assert!(!is_geometry_func("length"));
        assert!(!is_geometry_func("char_length"));
    }

    /// The module claims exactly the names it implements — a name claimed by
    /// mistake would be taken away from every other family.
    #[test]
    fn the_module_claims_only_the_geometric_names() {
        for name in [
            "area",
            "bound_box",
            "box",
            "center",
            "circle",
            "diagonal",
            "diameter",
            "height",
            "width",
            "radius",
            "isclosed",
            "isopen",
            "ishorizontal",
            "isvertical",
            "isparallel",
            "isperp",
            "line",
            "lseg",
            "npoints",
            "path",
            "pclose",
            "popen",
            "point",
            "polygon",
            "slope",
            "poly_center",
            "circle_center",
        ] {
            assert!(is_geometry_func(name), "{name}");
        }
        for name in [
            "length",
            "upper",
            "abs",
            "concat",
            "round",
            "sqrt",
            "text",
            "inet",
            "money",
            "varbit",
            "coalesce",
            "pi",
            "areas",
            "boxes",
            "pointless",
            "box_area",
            "path_close",
        ] {
            assert!(!is_geometry_func(name), "{name}");
        }
    }
}
