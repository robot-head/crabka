//! `PostgreSQL` geometric scalar values.

use std::hash::{Hash, Hasher};

use crate::TypeError;

/// `PostgreSQL` `point`: two IEEE-754 double-precision coordinates.
#[derive(Debug, Clone, Copy)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

/// `PostgreSQL` `path`: an ordered series of points, either open or closed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Path {
    pub closed: bool,
    pub points: Vec<Point>,
}

/// `PostgreSQL` `lseg`: a line segment between two endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Lseg {
    pub start: Point,
    pub end: Point,
}

impl Lseg {
    /// Parse `lseg_in`'s spellings: `[(x,y),(x,y)]`, `((x,y),(x,y))`,
    /// `(x,y),(x,y)` and the bare `x,y,x,y`, each optionally bracketed.
    ///
    /// # Errors
    ///
    /// Returns `22P02` for anything that is not exactly two points, and the
    /// same coordinate errors as [`Point::parse`].
    pub fn parse(input: &str) -> Result<Self, TypeError> {
        let value = input.trim();
        // Strip one optional outer delimiter pair. `lseg_in` accepts `[…]` and
        // `(…)` alike, but only one level — and a leading `(` may instead open
        // the *first point*, as in `(1,2),(3,4)`, so parentheses count as a
        // wrapper only when the opening one closes at the very end.
        let inner = value
            .strip_prefix('[')
            .and_then(|inner| inner.strip_suffix(']'))
            .or_else(|| parenthesis_wraps_whole(value).then(|| &value[1..value.len() - 1]))
            .unwrap_or(value);
        let points = split_points(inner).ok_or_else(|| invalid_lseg(input))?;
        let [start, end] = points.as_slice() else {
            return Err(invalid_lseg(input));
        };
        Ok(Self {
            start: Point::parse(start).map_err(|_| invalid_lseg(input))?,
            end: Point::parse(end).map_err(|_| invalid_lseg(input))?,
        })
    }

    /// `@-@` / `length(lseg)` — `lseg_length`.
    #[must_use]
    pub fn length(self) -> f64 {
        self.start.distance(self.end)
    }

    /// `@@` / `point(lseg)` — `lseg_center`, the midpoint.
    #[must_use]
    pub fn center(self) -> Point {
        Point {
            x: f64::midpoint(self.start.x, self.end.x),
            y: f64::midpoint(self.start.y, self.end.y),
        }
    }

    /// `lseg_sl`.
    #[must_use]
    pub fn slope(self) -> f64 {
        self.start.slope(self.end)
    }

    /// `?-` — `lseg_horizontal`.
    #[must_use]
    pub fn is_horizontal(self) -> bool {
        fp_eq(self.start.y, self.end.y)
    }

    /// `?|` — `lseg_vertical`.
    #[must_use]
    pub fn is_vertical(self) -> bool {
        fp_eq(self.start.x, self.end.x)
    }

    /// `?||` — `lseg_parallel`: equal slopes. Unlike the `line` operator this
    /// is a direct slope comparison, so two collinear segments are parallel.
    #[must_use]
    pub fn is_parallel_to(self, other: Self) -> bool {
        fp_eq(self.slope(), other.slope())
    }

    /// `?-|` — `lseg_perp`: this segment's slope equals the other's *inverse*
    /// slope.
    #[must_use]
    pub fn is_perpendicular_to(self, other: Self) -> bool {
        fp_eq(self.slope(), other.start.inverse_slope(other.end))
    }

    /// `=` — `lseg_eq`: matching endpoints in the given order, under
    /// [`Point::eq_point`].
    #[must_use]
    pub fn eq_lseg(self, other: Self) -> bool {
        self.start.eq_point(other.start) && self.end.eq_point(other.end)
    }

    /// `<>` — `lseg_ne`. Written as upstream does: *either* endpoint differing
    /// is enough, which is the negation of [`Lseg::eq_lseg`].
    #[must_use]
    pub fn ne_lseg(self, other: Self) -> bool {
        !self.eq_lseg(other)
    }

    /// SQL ordering for segments: `<`, `<=`, `>`, `>=` all compare *length*
    /// through the epsilon macros, so the endpoints are ignored entirely and
    /// `=` is a separate, structural relation ([`Lseg::eq_lseg`]). `None` where
    /// a NaN length leaves every comparison false, as in [`Circle::compare`].
    #[must_use]
    pub fn compare(self, other: Self) -> Option<std::cmp::Ordering> {
        compare_with_epsilon(self.length(), other.length())
    }

    /// `point <@ lseg` — `lseg_contain_point`, decided by a triangle equality
    /// rather than by the line equation: upstream found that far better
    /// behaved against least-significant-bit residue.
    #[must_use]
    pub fn contains_point(self, point: Point) -> bool {
        fp_eq(
            point.distance(self.start) + point.distance(self.end),
            self.start.distance(self.end),
        )
    }

    /// `<->` — `lseg_closept_point`'s distance.
    #[must_use]
    pub fn distance_to_point(self, point: Point) -> f64 {
        self.closest_point_and_distance_to_point(point).0
    }

    /// `point ## lseg` — `close_ps`: the closest point *on this segment*.
    #[must_use]
    pub fn closest_point_to(self, point: Point) -> Option<Point> {
        let (distance, closest) = self.closest_point_and_distance_to_point(point);
        (!distance.is_nan()).then_some(closest)
    }

    /// `lseg_closept_point`: drop a perpendicular from the point and clamp it
    /// to the segment, which [`Lseg::closest_point_and_distance_to_line`] does
    /// by falling back to whichever endpoint is nearer.
    fn closest_point_and_distance_to_point(self, point: Point) -> (f64, Point) {
        let perpendicular = Line::from_point_slope(point, self.start.inverse_slope(self.end));
        let (_, closest) = self.closest_point_and_distance_to_line(perpendicular);
        (closest.distance(point), closest)
    }

    /// `lseg_closept_line`: the point of this segment nearest the line, and its
    /// distance. When the two are parallel there is no single nearest point and
    /// upstream settles for the second endpoint.
    fn closest_point_and_distance_to_line(self, line: Line) -> (f64, Point) {
        if let Some(crossing) = self.intersection_point_with_line(line) {
            return (0.0, crossing);
        }
        let from_start = line.distance_to_point(self.start);
        let from_end = line.distance_to_point(self.end);
        if from_start < from_end {
            (from_start, self.start)
        } else {
            (from_end, self.end)
        }
    }

    /// `<->` — `lseg_closept_line`'s distance, for `lseg <-> line`.
    #[must_use]
    pub fn distance_to_line(self, line: Line) -> f64 {
        self.closest_point_and_distance_to_line(line).0
    }

    /// `?#` — `inter_sl`: the segment reaches the line.
    #[must_use]
    pub fn intersects_line(self, line: Line) -> bool {
        self.intersection_point_with_line(line).is_some()
    }

    /// `lseg_interpt_line`: promote the segment to a line, intersect, and keep
    /// the result only if it landed back on the segment. An intersection that
    /// lands on an endpoint is snapped to that endpoint exactly, because the
    /// two computations differ in the last bits.
    fn intersection_point_with_line(self, line: Line) -> Option<Point> {
        let extended = Line::from_point_slope(self.start, self.slope());
        let crossing = extended.intersection_point(line)?;
        if !self.contains_point(crossing) {
            return None;
        }
        if self.start.eq_point(crossing) {
            Some(self.start)
        } else if self.end.eq_point(crossing) {
            Some(self.end)
        } else {
            Some(crossing)
        }
    }

    /// `?#` — `lseg_intersect`.
    #[must_use]
    pub fn intersects(self, other: Self) -> bool {
        self.intersection_point(other).is_some()
    }

    /// `#` — `lseg_interpt`. The result is required to lie on *both* segments,
    /// which is what makes the operator symmetric even though the computation
    /// is not.
    #[must_use]
    pub fn intersection_point(self, other: Self) -> Option<Point> {
        let extended = Line::from_point_slope(other.start, other.slope());
        let crossing = self.intersection_point_with_line(extended)?;
        other.contains_point(crossing).then_some(crossing)
    }

    /// `<->` — `lseg_distance`.
    #[must_use]
    pub fn distance(self, other: Self) -> f64 {
        self.closest_point_and_distance_to_lseg(other).0
    }

    /// `##` — `close_lseg`. The result lies on the **right-hand** segment:
    /// upstream passes the arguments to `lseg_closept_lseg` the other way
    /// round. `None` when the slopes are bitwise equal, tested with C's `==`
    /// rather than the epsilon relation, so parallel segments have no answer.
    #[must_use]
    pub fn closest_point_to_lseg(self, other: Self) -> Option<Point> {
        if raw_eq(self.slope(), other.slope()) {
            return None;
        }
        let (distance, closest) = other.closest_point_and_distance_to_lseg(self);
        (!distance.is_nan()).then_some(closest)
    }

    /// `lseg_closept_lseg`: the point *of this segment* closest to `other`, and
    /// the distance between the two segments. Four candidates are tried — the
    /// crossing, this segment's feet under the other's two endpoints, and the
    /// other segment's feet under this one's — because the nearest pair can be
    /// interior on either side.
    fn closest_point_and_distance_to_lseg(self, other: Self) -> (f64, Point) {
        if let Some(crossing) = self.intersection_point(other) {
            return (0.0, crossing);
        }
        let (mut distance, mut closest) = self.closest_point_and_distance_to_point(other.start);
        let (from_end, foot) = self.closest_point_and_distance_to_point(other.end);
        if float_lt(from_end, distance) {
            distance = from_end;
            closest = foot;
        }
        let to_start = other.distance_to_point(self.start);
        if float_lt(to_start, distance) {
            distance = to_start;
            closest = self.start;
        }
        let to_end = other.distance_to_point(self.end);
        if float_lt(to_end, distance) {
            distance = to_end;
            closest = self.end;
        }
        (distance, closest)
    }
}

/// `PostgreSQL` `box`: an axis-aligned rectangle, held as its high and low
/// corners. `box_in` normalizes per coordinate, so the stored `high` is
/// always `(max x, max y)` whichever corners were written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Box2 {
    pub high: Point,
    pub low: Point,
}

impl Box2 {
    /// Parse `box_in`: `(x1,y1),(x2,y2)`, `((x1,y1),(x2,y2))` or the bare
    /// `x1,y1,x2,y2`, each with at most one optional outer parenthesis pair.
    /// Square brackets are *not* accepted, unlike `lseg_in`.
    ///
    /// # Errors
    ///
    /// `22P02` for anything that is not exactly two points; `22003` for a
    /// coordinate that overflows `float8`.
    pub fn parse(input: &str) -> Result<Self, TypeError> {
        let value = input.trim();
        let inner = if parenthesis_wraps_whole(value) {
            &value[1..value.len() - 1]
        } else {
            value
        };
        let points = split_points(inner).ok_or_else(|| invalid_box(input))?;
        let [first, second] = points.as_slice() else {
            return Err(invalid_box(input));
        };
        let corner = |text: &str| {
            Point::parse(text).map_err(|error| match error {
                TypeError::InvalidText { .. } => invalid_box(input),
                other => other,
            })
        };
        Ok(Self::normalized(corner(first)?, corner(second)?))
    }

    /// The box with the two corners sorted per coordinate, as `box_in` stores
    /// them regardless of which corners were written.
    ///
    /// The test is `box_construct`'s `float8_gt`, not a bare `>`: NaN sorts
    /// above every number in that ordering, so a NaN coordinate always lands in
    /// the *high* corner whichever corner it was written in.
    #[must_use]
    pub fn normalized(first: Point, second: Point) -> Self {
        let (high_x, low_x) = if float_gt(first.x, second.x) {
            (first.x, second.x)
        } else {
            (second.x, first.x)
        };
        let (high_y, low_y) = if float_gt(first.y, second.y) {
            (first.y, second.y)
        } else {
            (second.y, first.y)
        };
        Self {
            high: Point {
                x: high_x,
                y: high_y,
            },
            low: Point { x: low_x, y: low_y },
        }
    }

    /// `width(box)` / `height(box)` / `area(box)`.
    #[must_use]
    pub fn width(self) -> f64 {
        self.high.x - self.low.x
    }

    #[must_use]
    pub fn height(self) -> f64 {
        self.high.y - self.low.y
    }

    #[must_use]
    pub fn area(self) -> f64 {
        self.width() * self.height()
    }
}

impl Box2 {
    /// The box that bounds a value, which is what the positional operators
    /// (`<<`, `&<`, `<<|`, …) compare. A point bounds to itself.
    #[must_use]
    pub fn of_point(point: Point) -> Self {
        Self {
            high: point,
            low: point,
        }
    }

    /// The box bounding a circle.
    #[must_use]
    pub fn of_circle(circle: Circle) -> Self {
        Self {
            high: Point {
                x: circle.center.x + circle.radius,
                y: circle.center.y + circle.radius,
            },
            low: Point {
                x: circle.center.x - circle.radius,
                y: circle.center.y - circle.radius,
            },
        }
    }

    /// `<<` — `box_left`: this box's right edge is left of the other's left
    /// edge.
    #[must_use]
    pub fn strictly_left_of(self, other: Self) -> bool {
        fp_lt(self.high.x, other.low.x)
    }

    /// `>>` — `box_right`.
    #[must_use]
    pub fn strictly_right_of(self, other: Self) -> bool {
        fp_gt(self.low.x, other.high.x)
    }

    /// `&<` — `box_overleft`: does not extend to the right of.
    #[must_use]
    pub fn does_not_extend_right(self, other: Self) -> bool {
        fp_le(self.high.x, other.high.x)
    }

    /// `&>` — `box_overright`: does not extend to the left of.
    #[must_use]
    pub fn does_not_extend_left(self, other: Self) -> bool {
        fp_ge(self.low.x, other.low.x)
    }

    /// `<<|` — `box_below`.
    #[must_use]
    pub fn strictly_below(self, other: Self) -> bool {
        fp_lt(self.high.y, other.low.y)
    }

    /// `|>>` — `box_above`.
    #[must_use]
    pub fn strictly_above(self, other: Self) -> bool {
        fp_gt(self.low.y, other.high.y)
    }

    /// `&<|` — `box_overbelow`: does not extend above.
    #[must_use]
    pub fn does_not_extend_above(self, other: Self) -> bool {
        fp_le(self.high.y, other.high.y)
    }

    /// `|&>` — `box_overabove`: does not extend below.
    #[must_use]
    pub fn does_not_extend_below(self, other: Self) -> bool {
        fp_ge(self.low.y, other.low.y)
    }

    /// `<^` — `box_below_eq`. The name is a historical accident: this is *not*
    /// [`Box2::strictly_below`] relaxed to allow equality, it compares this
    /// box's top edge against the other's **bottom** edge, so two identical
    /// boxes are not below-or-equal each other.
    #[must_use]
    pub fn below_or_equal(self, other: Self) -> bool {
        fp_le(self.high.y, other.low.y)
    }

    /// `>^` — `box_above_eq`, the mirror of [`Box2::below_or_equal`] and just
    /// as misnamed.
    #[must_use]
    pub fn above_or_equal(self, other: Self) -> bool {
        fp_ge(self.low.y, other.high.y)
    }

    /// `@@` / `center(box)` / `point(box)` — `box_cn`.
    #[must_use]
    pub fn center(self) -> Point {
        Point {
            x: f64::midpoint(self.high.x, self.low.x),
            y: f64::midpoint(self.high.y, self.low.y),
        }
    }

    /// `&&` / `?#` — `box_ov`: the two boxes overlap, touching included.
    #[must_use]
    pub fn overlaps(self, other: Self) -> bool {
        fp_le(self.low.x, other.high.x)
            && fp_le(other.low.x, self.high.x)
            && fp_le(self.low.y, other.high.y)
            && fp_le(other.low.y, self.high.y)
    }

    /// `@>` / `<@` — `box_contain_box`: the other box lies inside this one or
    /// on its border.
    #[must_use]
    pub fn contains(self, other: Self) -> bool {
        fp_ge(self.high.x, other.high.x)
            && fp_le(self.low.x, other.low.x)
            && fp_ge(self.high.y, other.high.y)
            && fp_le(self.low.y, other.low.y)
    }

    /// `point <@ box` / `box @> point` — `box_contain_point`. This one is
    /// *exact*: unlike the box-on-box test above, upstream compares the corners
    /// with bare `>=`/`<=` and no epsilon.
    #[must_use]
    pub fn contains_point(self, point: Point) -> bool {
        self.high.x >= point.x
            && self.low.x <= point.x
            && self.high.y >= point.y
            && self.low.y <= point.y
    }

    /// `lseg <@ box` — `on_sb`: both endpoints are in the box.
    #[must_use]
    pub fn contains_lseg(self, lseg: Lseg) -> bool {
        self.contains_point(lseg.start) && self.contains_point(lseg.end)
    }

    /// `~=` — `box_same`, corner for corner under [`Point::eq_point`].
    #[must_use]
    pub fn same(self, other: Self) -> bool {
        self.high.eq_point(other.high) && self.low.eq_point(other.low)
    }

    /// SQL ordering for boxes: `<`, `<=`, `=`, `>=`, `>` all compare *area*
    /// through the epsilon macros, so two boxes of equal area are `=` however
    /// differently they are placed — `~=` ([`Box2::same`]) is the structural
    /// relation. `box` has no `<>` operator at all.
    #[must_use]
    pub fn compare(self, other: Self) -> Option<std::cmp::Ordering> {
        compare_with_epsilon(self.area(), other.area())
    }

    /// `<->` — `box_distance`, which is the distance between the two boxes'
    /// **centres**, not the gap between them. Two overlapping boxes are
    /// therefore a positive distance apart, and a box is zero from itself.
    #[must_use]
    pub fn distance(self, other: Self) -> f64 {
        self.center().distance(other.center())
    }

    /// `diagonal(box)` / `lseg(box)` — `box_diagonal`, the positive-slope
    /// diagonal, written high corner first.
    #[must_use]
    pub fn diagonal(self) -> Lseg {
        Lseg {
            start: self.high,
            end: self.low,
        }
    }

    /// `bound_box(box, box)` — the smallest box containing both.
    #[must_use]
    pub fn bound_box(self, other: Self) -> Self {
        Self {
            high: Point {
                x: float_max(self.high.x, other.high.x),
                y: float_max(self.high.y, other.high.y),
            },
            low: Point {
                x: float_min(self.low.x, other.low.x),
                y: float_min(self.low.y, other.low.y),
            },
        }
    }

    /// `#` — `box_intersect`: the overlapping portion, or `None` when they do
    /// not overlap at all.
    #[must_use]
    pub fn intersection(self, other: Self) -> Option<Self> {
        self.overlaps(other).then(|| Self {
            high: Point {
                x: float_min(self.high.x, other.high.x),
                y: float_min(self.high.y, other.high.y),
            },
            low: Point {
                x: float_max(self.low.x, other.low.x),
                y: float_max(self.low.y, other.low.y),
            },
        })
    }

    /// The box's four sides, in the order `box_closept_point` tries them:
    /// left, top, bottom, right. The order is load-bearing — the closest-point
    /// searches keep the *first* candidate on a tie, so a point equidistant
    /// from two sides resolves to the earlier one.
    fn sides(self) -> [Lseg; 4] {
        let upper_left = Point {
            x: self.low.x,
            y: self.high.y,
        };
        let lower_right = Point {
            x: self.high.x,
            y: self.low.y,
        };
        [
            self.low.lseg_with(upper_left),
            self.high.lseg_with(upper_left),
            self.low.lseg_with(lower_right),
            self.high.lseg_with(lower_right),
        ]
    }

    /// `<->` — `box_closept_point`'s distance, zero for a point inside.
    #[must_use]
    pub fn distance_to_point(self, point: Point) -> f64 {
        self.closest_point_and_distance_to_point(point).0
    }

    /// `point ## box` — `close_pb`: the closest point on or in the box. A point
    /// already inside is its own answer.
    #[must_use]
    pub fn closest_point_to(self, point: Point) -> Option<Point> {
        let (distance, closest) = self.closest_point_and_distance_to_point(point);
        (!distance.is_nan()).then_some(closest)
    }

    /// `box_closept_point`: O(1) — four sides, each a constant-time
    /// point-to-segment projection.
    fn closest_point_and_distance_to_point(self, point: Point) -> (f64, Point) {
        if self.contains_point(point) {
            return (0.0, point);
        }
        let mut best = (f64::NAN, point);
        for (index, side) in self.sides().into_iter().enumerate() {
            let candidate = side.closest_point_and_distance_to_point(point);
            if index == 0 || float_lt(candidate.0, best.0) {
                best = candidate;
            }
        }
        best
    }

    /// `<->` — `box_closept_lseg`'s distance, for `box <-> lseg` and
    /// `lseg <-> box` alike.
    #[must_use]
    pub fn distance_to_lseg(self, lseg: Lseg) -> f64 {
        self.closest_point_and_distance_to_lseg(lseg).0
    }

    /// `lseg ## box` — `close_sb`: the closest point on or in the box. When the
    /// segment already reaches the box the answer is the point of the *segment*
    /// nearest the box's centre, which is upstream's arbitrary pick among the
    /// two crossings.
    #[must_use]
    pub fn closest_point_to_lseg(self, lseg: Lseg) -> Option<Point> {
        let (distance, closest) = self.closest_point_and_distance_to_lseg(lseg);
        (!distance.is_nan()).then_some(closest)
    }

    /// `box_closept_lseg`: O(1) — four sides, each a constant-time
    /// segment-to-segment search.
    fn closest_point_and_distance_to_lseg(self, lseg: Lseg) -> (f64, Point) {
        if let Some(crossing) = self.intersection_point_with_lseg(lseg) {
            return (0.0, crossing);
        }
        let mut best = (f64::NAN, self.low);
        for (index, side) in self.sides().into_iter().enumerate() {
            let candidate = side.closest_point_and_distance_to_lseg(lseg);
            if index == 0 || float_lt(candidate.0, best.0) {
                best = candidate;
            }
        }
        best
    }

    /// `?#` — `inter_sb`: the segment meets the box, a segment wholly inside
    /// included.
    #[must_use]
    pub fn intersects_lseg(self, lseg: Lseg) -> bool {
        self.intersection_point_with_lseg(lseg).is_some()
    }

    /// `box_interpt_lseg`: whether the segment reaches the box, and if so the
    /// point of the segment nearest the box's centre. There are typically two
    /// crossings, so upstream picks that one rather than either.
    fn intersection_point_with_lseg(self, lseg: Lseg) -> Option<Point> {
        let bounds = Self::normalized(lseg.start, lseg.end);
        if !bounds.overlaps(self) {
            return None;
        }
        let (_, nearest_centre) = lseg.closest_point_and_distance_to_point(self.center());
        if self.contains_point(lseg.start) || self.contains_point(lseg.end) {
            return Some(nearest_centre);
        }
        self.sides()
            .into_iter()
            .any(|side| side.intersects(lseg))
            .then_some(nearest_centre)
    }

    /// `box + point` — translate both corners. No renormalization: a
    /// translation cannot reorder them.
    ///
    /// # Errors
    ///
    /// `22003` when a corner coordinate overflows.
    pub fn add_point(self, point: Point) -> Result<Self, TypeError> {
        Ok(Self {
            high: self.high.add_point(point)?,
            low: self.low.add_point(point)?,
        })
    }

    /// `box - point` — translate both corners.
    ///
    /// # Errors
    ///
    /// `22003` when a corner coordinate overflows.
    pub fn sub_point(self, point: Point) -> Result<Self, TypeError> {
        Ok(Self {
            high: self.high.sub_point(point)?,
            low: self.low.sub_point(point)?,
        })
    }

    /// `box * point` — rotate and scale. The corners go through the complex
    /// product, which can swap them round, so the result is renormalized. The
    /// rotated rectangle is *not* axis-aligned, so what comes back is its
    /// bounding box's corners rather than its own.
    ///
    /// # Errors
    ///
    /// `22003` for overflow or underflow.
    pub fn mul_point(self, point: Point) -> Result<Self, TypeError> {
        Ok(Self::normalized(
            self.high.mul_point(point)?,
            self.low.mul_point(point)?,
        ))
    }

    /// `box / point` — the inverse of [`Box2::mul_point`], likewise
    /// renormalized.
    ///
    /// # Errors
    ///
    /// `22012` for the origin, `22003` for overflow or underflow.
    pub fn div_point(self, point: Point) -> Result<Self, TypeError> {
        Ok(Self::normalized(
            self.high.div_point(point)?,
            self.low.div_point(point)?,
        ))
    }

    /// `polygon(box)` — `box_poly`: the four corners anticlockwise from the low
    /// corner.
    #[must_use]
    pub fn to_polygon(self) -> Polygon {
        Polygon {
            points: vec![
                self.low,
                Point {
                    x: self.low.x,
                    y: self.high.y,
                },
                self.high,
                Point {
                    x: self.high.x,
                    y: self.low.y,
                },
            ],
        }
    }

    /// `circle(box)` — `box_circle`: centred on the box, with the corner on the
    /// circumference, so the circle *circumscribes* the box.
    #[must_use]
    pub fn to_circle(self) -> Circle {
        let center = self.center();
        Circle {
            center,
            radius: center.distance(self.high),
        }
    }
}

/// `PostgreSQL` `circle`: a centre point and a radius.
#[derive(Debug, Clone, Copy)]
pub struct Circle {
    pub center: Point,
    pub radius: f64,
}

impl Circle {
    /// Parse `circle_in`: `<(x,y),r>`, `((x,y),r)`, `(x,y),r` or `x,y,r`.
    ///
    /// # Errors
    ///
    /// `22P02` for malformed input and for a negative radius; `22003` for a
    /// coordinate that overflows `float8`.
    pub fn parse(input: &str) -> Result<Self, TypeError> {
        let value = input.trim();
        // One optional wrapper: `<…>` always wraps, `(…)` only when its opening
        // parenthesis closes at the very end — otherwise it opens the centre.
        let inner = value
            .strip_prefix('<')
            .and_then(|inner| inner.strip_suffix('>'))
            .or_else(|| parenthesis_wraps_whole(value).then(|| &value[1..value.len() - 1]))
            .unwrap_or(value)
            .trim();
        let (center, radius) = if inner.starts_with('(') {
            let end = inner.find(')').ok_or_else(|| invalid_circle(input))?;
            let rest = inner[end + 1..]
                .trim_start()
                .strip_prefix(',')
                .ok_or_else(|| invalid_circle(input))?;
            (&inner[..=end], rest)
        } else {
            // `x,y,r`: the radius is everything after the second comma.
            let first = inner.find(',').ok_or_else(|| invalid_circle(input))?;
            let second = first
                + 1
                + inner[first + 1..]
                    .find(',')
                    .ok_or_else(|| invalid_circle(input))?;
            (&inner[..second], &inner[second + 1..])
        };
        let circle = Self {
            center: Point::parse(center).map_err(|error| match error {
                TypeError::InvalidText { .. } => invalid_circle(input),
                other => other,
            })?,
            radius: coordinate(radius, input).map_err(|error| match error {
                TypeError::InvalidText { .. } => invalid_circle(input),
                other => other,
            })?,
        };
        // `circle_in` rejects a negative radius as bad input; zero and NaN are
        // values.
        if circle.radius < 0.0 {
            return Err(invalid_circle(input));
        }
        Ok(circle)
    }

    /// `area(circle)` — `r^2 * pi`, which is also how circles order.
    ///
    /// The grouping is load-bearing: `circle_ar` squares the radius *first* and
    /// multiplies by π last, and `(115*115)*π` differs from `(π*115)*115` in the
    /// final bit.
    #[must_use]
    pub fn area(self) -> f64 {
        (self.radius * self.radius) * std::f64::consts::PI
    }

    /// `diameter(circle)`.
    #[must_use]
    pub fn diameter(self) -> f64 {
        self.radius * 2.0
    }

    /// SQL ordering for circles: every comparison operator works on *area*
    /// through `PostgreSQL`'s `FPeq`/`FPlt` macros, so areas within `EPSILON`
    /// compare equal and the centres are ignored entirely. `None` means the
    /// comparison is undefined because an area is NaN, where `PostgreSQL`'s C
    /// macros make every operator false.
    #[must_use]
    pub fn compare(self, other: Self) -> Option<std::cmp::Ordering> {
        compare_with_epsilon(self.area(), other.area())
    }

    /// `<>` — `circle_ne`, which is *not* the negation of `=`: a NaN area makes
    /// both false. `circle` is the only geometric type with a `<>` on a
    /// magnitude rather than on structure.
    #[must_use]
    pub fn ne_circle(self, other: Self) -> bool {
        fp_ne(self.area(), other.area())
    }

    /// `~=` — `circle_same`. NaN radii are equal to one another here, which
    /// `FPeq` alone would not give, so that a NaN-radius circle stays findable.
    #[must_use]
    pub fn same(self, other: Self) -> bool {
        ((self.radius.is_nan() && other.radius.is_nan()) || fp_eq(self.radius, other.radius))
            && self.center.eq_point(other.center)
    }

    /// `&&` — `circle_overlap`: the centres are no further apart than the radii
    /// sum.
    #[must_use]
    pub fn overlaps(self, other: Self) -> bool {
        fp_le(
            self.center.distance(other.center),
            self.radius + other.radius,
        )
    }

    /// `@>` / `<@` — `circle_contain`: the other circle lies inside this one.
    #[must_use]
    pub fn contains(self, other: Self) -> bool {
        fp_le(
            self.center.distance(other.center),
            self.radius - other.radius,
        )
    }

    /// `point <@ circle` / `circle @> point` — `circle_contain_pt`. Exact, not
    /// fuzzy: upstream compares the distance to the radius with a bare `<=`.
    #[must_use]
    pub fn contains_point(self, point: Point) -> bool {
        self.center.distance(point) <= self.radius
    }

    /// `<->` — `dist_pc`: the gap to the circumference, clamped at zero for a
    /// point inside. Not the distance to the centre, and not the bounding box's
    /// answer either.
    #[must_use]
    pub fn distance_to_point(self, point: Point) -> f64 {
        let gap = point.distance(self.center) - self.radius;
        if gap < 0.0 { 0.0 } else { gap }
    }

    /// `<->` — `dist_cpoly`: the polygon's distance to the centre, less the
    /// radius, clamped at zero. Complexity is that of
    /// [`Polygon::distance_to_point`], O(n) in the vertex count.
    #[must_use]
    pub fn distance_to_polygon(self, polygon: &Polygon) -> f64 {
        let gap = polygon.distance_to_point(self.center) - self.radius;
        if gap < 0.0 { 0.0 } else { gap }
    }

    /// `circle + point` — translation.
    ///
    /// # Errors
    ///
    /// `22003` when a centre coordinate overflows.
    pub fn add_point(self, point: Point) -> Result<Self, TypeError> {
        Ok(Self {
            center: self.center.add_point(point)?,
            radius: self.radius,
        })
    }

    /// `circle - point` — translation.
    ///
    /// # Errors
    ///
    /// `22003` when a centre coordinate overflows.
    pub fn sub_point(self, point: Point) -> Result<Self, TypeError> {
        Ok(Self {
            center: self.center.sub_point(point)?,
            radius: self.radius,
        })
    }

    /// `circle * point` — rotate and scale. The centre goes through the complex
    /// product while the radius scales by the point's *modulus*, so the circle
    /// stays a circle.
    ///
    /// # Errors
    ///
    /// `22003` for overflow or underflow.
    pub fn mul_point(self, point: Point) -> Result<Self, TypeError> {
        Ok(Self {
            center: self.center.mul_point(point)?,
            radius: checked_mul(self.radius, pg_hypot(point.x, point.y))?,
        })
    }

    /// `circle / point` — the inverse of [`Circle::mul_point`].
    ///
    /// # Errors
    ///
    /// `22012` for the origin, `22003` for overflow or underflow.
    pub fn div_point(self, point: Point) -> Result<Self, TypeError> {
        Ok(Self {
            center: self.center.div_point(point)?,
            radius: checked_div(self.radius, pg_hypot(point.x, point.y))?,
        })
    }

    /// `box(circle)` — `circle_box`. This is the *inscribed* box, whose corner
    /// sits on the circumference at `r/√2`; the bounding box the positional
    /// operators use is [`Box2::of_circle`], and the two differ.
    #[must_use]
    pub fn to_box(self) -> Box2 {
        let delta = self.radius / std::f64::consts::SQRT_2;
        Box2 {
            high: Point {
                x: self.center.x + delta,
                y: self.center.y + delta,
            },
            low: Point {
                x: self.center.x - delta,
                y: self.center.y - delta,
            },
        }
    }

    /// `point(circle)` / `@@` — `circle_center`.
    #[must_use]
    pub fn to_point(self) -> Point {
        self.center
    }

    /// `polygon(npts, circle)` — `circle_poly`. The vertices run *clockwise*
    /// from `(cx − r, cy)`: upstream subtracts the cosine and adds the sine.
    /// O(`npoints`).
    ///
    /// # Errors
    ///
    /// `0A000` for a zero radius — there is no polygon to make — and `22023`
    /// for fewer than two points.
    pub fn to_polygon(self, npoints: i32) -> Result<Polygon, TypeError> {
        if fp_zero(self.radius) {
            return Err(TypeError::Domain {
                sqlstate: "0A000",
                message: "cannot convert circle with radius zero to polygon",
            });
        }
        if npoints < 2 {
            return Err(TypeError::Domain {
                sqlstate: "22023",
                message: "must request at least 2 points",
            });
        }
        let step = (2.0 * std::f64::consts::PI) / f64::from(npoints);
        let points = (0..npoints)
            .map(|index| {
                let angle = step * f64::from(index);
                Point {
                    x: self.center.x - self.radius * angle.cos(),
                    y: self.center.y + self.radius * angle.sin(),
                }
            })
            .collect();
        Ok(Polygon { points })
    }

    /// `circle <-> circle`: the gap between their boundaries, clamped at zero
    /// when they overlap, as `circle_distance` computes it.
    #[must_use]
    pub fn distance(self, other: Self) -> f64 {
        // `circle_distance` subtracts the radii as one sum, and the centre
        // distance comes from `pg_hypot`; both groupings are load-bearing for
        // the last digit.
        let gap = pg_hypot(
            self.center.x - other.center.x,
            self.center.y - other.center.y,
        ) - (self.radius + other.radius);
        if gap < 0.0 { 0.0 } else { gap }
    }
}

/// `PostgreSQL` `line`: the infinite line `Ax + By + C = 0`.
#[derive(Debug, Clone, Copy)]
pub struct Line {
    pub a: f64,
    pub b: f64,
    pub c: f64,
}

impl Line {
    /// Parse `line_in`: the coefficient form `{A,B,C}`, or any two-point
    /// spelling `lseg_in` accepts, which is converted to coefficients.
    ///
    /// # Errors
    ///
    /// `22P02` for malformed input, for `A` and `B` both zero, and for two
    /// equal points; `22003` for a coefficient or coordinate that overflows
    /// `float8`, which passes through rather than being reported as syntax.
    pub fn parse(input: &str) -> Result<Self, TypeError> {
        let value = input.trim();
        if let Some(inner) = value
            .strip_prefix('{')
            .and_then(|inner| inner.strip_suffix('}'))
        {
            let fields: Vec<&str> = inner.split(',').collect();
            let [a, b, c] = fields.as_slice() else {
                return Err(invalid_line(input));
            };
            let line = Self {
                a: line_coordinate(a, input)?,
                b: line_coordinate(b, input)?,
                c: line_coordinate(c, input)?,
            };
            if line.a == 0.0 && line.b == 0.0 {
                return Err(line_specification(
                    "invalid line specification: A and B cannot both be zero",
                ));
            }
            return Ok(line);
        }
        let inner = value
            .strip_prefix('[')
            .and_then(|inner| inner.strip_suffix(']'))
            .or_else(|| parenthesis_wraps_whole(value).then(|| &value[1..value.len() - 1]))
            .unwrap_or(value);
        let points = split_points(inner).ok_or_else(|| invalid_line(input))?;
        let [start, end] = points.as_slice() else {
            return Err(invalid_line(input));
        };
        Self::from_points(line_point(start, input)?, line_point(end, input)?)
    }

    /// The line through two distinct points, as `line_in`'s two-point spelling
    /// builds it.
    ///
    /// # Errors
    ///
    /// `22P02` when the points are equal. [`Point::line_with`] is the same
    /// construction under the `line(point, point)` function's `22023`.
    pub fn from_points(start: Point, end: Point) -> Result<Self, TypeError> {
        if start.eq_point(end) {
            return Err(line_specification(
                "invalid line specification: must be two distinct points",
            ));
        }
        Ok(Self::from_point_slope(start, start.slope(end)))
    }

    /// `line_construct`: the line of a given slope through a given point,
    /// normalized to `B = -1` except for a vertical, which is written `-x + C`.
    ///
    /// The horizontal case is spelled out rather than left to the arithmetic
    /// because `0 * x` carries a sign: taking the branch pins `A` to `+0` and
    /// `C` to the ordinate itself, and the general branch flushes a `-0`
    /// intercept to `+0` for the same reason.
    #[must_use]
    pub fn from_point_slope(point: Point, slope: f64) -> Self {
        if slope.is_infinite() {
            return Self {
                a: -1.0,
                b: 0.0,
                c: point.x,
            };
        }
        if slope == 0.0 {
            return Self {
                a: 0.0,
                b: -1.0,
                c: point.y,
            };
        }
        let intercept = point.y - slope * point.x;
        Self {
            a: slope,
            b: -1.0,
            c: if intercept == 0.0 { 0.0 } else { intercept },
        }
    }

    /// `line_sl`: infinite for a vertical, zero for a horizontal.
    #[must_use]
    pub fn slope(self) -> f64 {
        if fp_zero(self.a) {
            0.0
        } else if fp_zero(self.b) {
            f64::INFINITY
        } else {
            self.a / -self.b
        }
    }

    /// `line_invsl`: the slope of any perpendicular to this line.
    fn inverse_slope(self) -> f64 {
        if fp_zero(self.a) {
            f64::INFINITY
        } else if fp_zero(self.b) {
            0.0
        } else {
            self.b / self.a
        }
    }

    /// `?-` — `line_horizontal`.
    #[must_use]
    pub fn is_horizontal(self) -> bool {
        fp_zero(self.a)
    }

    /// `?|` — `line_vertical`.
    #[must_use]
    pub fn is_vertical(self) -> bool {
        fp_zero(self.b)
    }

    /// `?||` — `line_parallel`, which upstream defines as "has no intersection
    /// point", so two *identical* lines are parallel as well.
    #[must_use]
    pub fn is_parallel_to(self, other: Self) -> bool {
        self.intersection_point(other).is_none()
    }

    /// `?-|` — `line_perp`. The axis-aligned cases are answered from the
    /// coefficients so that no infinite slope has to be divided.
    #[must_use]
    pub fn is_perpendicular_to(self, other: Self) -> bool {
        if fp_zero(self.a) {
            return fp_zero(other.b);
        }
        if fp_zero(other.a) {
            return fp_zero(self.b);
        }
        if fp_zero(self.b) {
            return fp_zero(other.a);
        }
        if fp_zero(other.b) {
            return fp_zero(self.a);
        }
        fp_eq((self.a * other.a) / (self.b * other.b), -1.0)
    }

    /// `=` — `line_eq`. Two lines are the same line when their coefficients are
    /// PROPORTIONAL, not when they match field for field: `{1,-1,0}` and
    /// `{2,-2,0}` are equal. Neither engine normalizes on input — `line
    /// '{2,-2,0}'` still prints `{2,-2,0}` — so the scale factor has to be
    /// divided out here.
    ///
    /// The ratio is taken from the first of the other line's coefficients that
    /// is not `FPzero`, so a horizontal or vertical line (where `A` or `B` is
    /// exactly zero) still finds a usable divisor.
    ///
    /// A NaN anywhere makes `line_eq` insist on exact equality instead, through
    /// `float8_eq` — under which a NaN DOES equal itself. That guard is what
    /// makes `{NaN,NaN,NaN}` equal itself: via the ratio it would reduce to
    /// `FPeq(NaN, NaN)`, which is false.
    #[must_use]
    pub fn eq_line(self, other: Self) -> bool {
        if self.a.is_nan()
            || self.b.is_nan()
            || self.c.is_nan()
            || other.a.is_nan()
            || other.b.is_nan()
            || other.c.is_nan()
        {
            return float_eq(self.a, other.a)
                && float_eq(self.b, other.b)
                && float_eq(self.c, other.c);
        }
        let ratio = if fp_zero(other.a) {
            if fp_zero(other.b) {
                if fp_zero(other.c) {
                    1.0
                } else {
                    self.c / other.c
                }
            } else {
                self.b / other.b
            }
        } else {
            self.a / other.a
        };
        fp_eq(self.a, ratio * other.a)
            && fp_eq(self.b, ratio * other.b)
            && fp_eq(self.c, ratio * other.c)
    }

    /// `?#` — `line_intersect`: the lines meet in exactly one point.
    #[must_use]
    pub fn intersects(self, other: Self) -> bool {
        self.intersection_point(other).is_some()
    }

    /// `#` — `line_interpt`. `None` when the lines are parallel, *including*
    /// when they are the same line: there is then no unique intersection, and
    /// upstream chooses to report none.
    ///
    /// NaN coefficients yield `Some` with NaN coordinates rather than `None`,
    /// because reporting `None` would claim the lines are parallel.
    #[must_use]
    pub fn intersection_point(self, other: Self) -> Option<Point> {
        let (x, y) = if fp_zero(self.b) {
            if fp_zero(other.b) {
                return None;
            }
            if fp_eq(self.a, other.a * (self.b / other.b)) {
                return None;
            }
            let x = (other.b * self.c - self.b * other.c) / (other.a * self.b - self.a * other.b);
            (x, -(other.a * x + other.c) / other.b)
        } else {
            if fp_eq(other.a, self.a * (other.b / self.b)) {
                return None;
            }
            let x = (self.b * other.c - other.b * self.c) / (self.a * other.b - other.a * self.b);
            (x, -(self.a * x + self.c) / self.b)
        };
        // On some platforms the expressions above tend to produce -0.
        Some(Point {
            x: if x == 0.0 { 0.0 } else { x },
            y: if y == 0.0 { 0.0 } else { y },
        })
    }

    /// `<->` — `line_distance`. Zero when the lines cross; otherwise the
    /// intercepts are rescaled onto a common normalization before subtracting,
    /// because `{2,-3,1}` and `{4,-6,9}` are the same pair of directions at
    /// different scales.
    #[must_use]
    pub fn distance(self, other: Self) -> f64 {
        if self.intersects(other) {
            return 0.0;
        }
        let ratio =
            if !fp_zero(self.a) && !self.a.is_nan() && !fp_zero(other.a) && !other.a.is_nan() {
                self.a / other.a
            } else if !fp_zero(self.b) && !self.b.is_nan() && !fp_zero(other.b) && !other.b.is_nan()
            {
                self.b / other.b
            } else {
                1.0
            };
        (self.c - ratio * other.c).abs() / pg_hypot(self.a, self.b)
    }

    /// `point <@ line` — `line_contain_point`: the point satisfies the equation
    /// to within `EPSILON`.
    #[must_use]
    pub fn contains_point(self, point: Point) -> bool {
        fp_zero(self.a * point.x + self.b * point.y + self.c)
    }

    /// `lseg <@ line` — `on_sl`: both endpoints satisfy the equation.
    #[must_use]
    pub fn contains_lseg(self, lseg: Lseg) -> bool {
        self.contains_point(lseg.start) && self.contains_point(lseg.end)
    }

    /// `line <-> point` — `line_closept_point`'s distance. NaN when the
    /// perpendicular cannot be intersected with the line at all, which only
    /// happens with NaN coordinates.
    #[must_use]
    pub fn distance_to_point(self, point: Point) -> f64 {
        self.closest_point_and_distance(point).0
    }

    /// `point ## line` — `close_pl`: the foot of the perpendicular from the
    /// point. `None` where `PostgreSQL` returns NULL, i.e. when the
    /// perpendicular has no intersection.
    #[must_use]
    pub fn closest_point_to(self, point: Point) -> Option<Point> {
        let (distance, closest) = self.closest_point_and_distance(point);
        (!distance.is_nan()).then_some(closest)
    }

    /// `line_closept_point`: the foot of the perpendicular, and the distance to
    /// it. On failure upstream leaves the *input* point in the out-parameter
    /// and signals through a NaN distance, which is why both are returned.
    fn closest_point_and_distance(self, point: Point) -> (f64, Point) {
        let perpendicular = Self::from_point_slope(point, self.inverse_slope());
        match perpendicular.intersection_point(self) {
            None => (f64::NAN, point),
            Some(closest) => (closest.distance(point), closest),
        }
    }

    /// `?#` — `inter_lb`: the line crosses one of the box's four sides.
    #[must_use]
    pub fn intersects_box(self, rect: Box2) -> bool {
        rect.sides()
            .into_iter()
            .any(|side| side.intersects_line(self))
    }

    /// `line <-> lseg` — `lseg_closept_line`'s distance, the same number as
    /// `lseg <-> line`.
    #[must_use]
    pub fn distance_to_lseg(self, lseg: Lseg) -> f64 {
        lseg.closest_point_and_distance_to_line(self).0
    }

    /// `line ## lseg` — `close_ls`. The result lies on the *segment*, and is
    /// NULL when the two have equal slopes, tested with C's bare `==` rather
    /// than the epsilon relation.
    #[must_use]
    pub fn closest_point_to_lseg(self, lseg: Lseg) -> Option<Point> {
        if raw_eq(lseg.slope(), self.slope()) {
            return None;
        }
        let (distance, closest) = lseg.closest_point_and_distance_to_line(self);
        (!distance.is_nan()).then_some(closest)
    }
}

/// A coefficient or coordinate inside a `line`: an overflow is the float's own
/// `22003`, while bad syntax names `line` rather than the inner type.
fn line_coordinate(value: &str, whole: &str) -> Result<f64, TypeError> {
    coordinate(value, whole).map_err(|error| match error {
        TypeError::InvalidText { .. } => invalid_line(whole),
        other => other,
    })
}

fn line_point(value: &str, whole: &str) -> Result<Point, TypeError> {
    Point::parse(value).map_err(|error| match error {
        TypeError::InvalidText { .. } => invalid_line(whole),
        other => other,
    })
}

/// Whether the value's leading `(` is closed by its final `)`, which is what
/// distinguishes the wrapper in `((1,2),(3,4))` from the first point's own
/// parenthesis in `(1,2),(3,4)`.
fn parenthesis_wraps_whole(value: &str) -> bool {
    let mut depth = 0_usize;
    for (index, byte) in value.bytes().enumerate() {
        match byte {
            b'(' => depth += 1,
            b')' => match depth.checked_sub(1) {
                Some(remaining) => {
                    depth = remaining;
                    if depth == 0 {
                        return index + 1 == value.len();
                    }
                }
                None => return false,
            },
            _ => {}
        }
    }
    false
}

/// Split a two-point body into its two point texts. A parenthesized body
/// splits on the comma *between* the points; a bare `x,y,x,y` splits at its
/// second comma so each half is an `x,y` pair.
fn split_points(inner: &str) -> Option<Vec<&str>> {
    let trimmed = inner.trim();
    if !trimmed.starts_with('(') {
        let first = trimmed.find(',')?;
        let second = first + 1 + trimmed[first + 1..].find(',')?;
        return Some(vec![&trimmed[..second], &trimmed[second + 1..]]);
    }
    let mut points = Vec::new();
    let mut rest = trimmed;
    while !rest.is_empty() {
        let start = rest.strip_prefix('(')?;
        let end = start.find(')')?;
        points.push(&start[..end]);
        rest = start[end + 1..].trim_start();
        if rest.is_empty() {
            break;
        }
        rest = rest.strip_prefix(',')?.trim_start();
        if rest.is_empty() {
            return None;
        }
    }
    Some(points)
}

impl Path {
    /// Parse `path_in`. The bracketed `[…]` spelling is the *open* path and the
    /// only one `poly_in` refuses; everything else — `((x,y),…)`, `(x,y),…`,
    /// the bare `x,y,…` and the flat `(x,y,…)` — is a closed path, including
    /// the unbracketed forms that carry no delimiter at all.
    ///
    /// # Errors
    ///
    /// Returns the same coordinate errors as [`Point::parse`], or `22P02` for
    /// malformed delimiters and point lists.
    pub fn parse(input: &str) -> Result<Self, TypeError> {
        let (open, points) = decode_point_list(input, invalid_path, true)?;
        Ok(Self {
            closed: !open,
            points,
        })
    }

    /// `#` / `npoints(path)` — `path_npoints`, which is an `int4`.
    #[must_use]
    pub fn npoints(&self) -> i32 {
        i32::try_from(self.points.len()).unwrap_or(i32::MAX)
    }

    /// `isclosed(path)`.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// `isopen(path)`.
    #[must_use]
    pub fn is_open(&self) -> bool {
        !self.closed
    }

    /// `pclose(path)` — the same vertices, closed.
    #[must_use]
    pub fn to_closed(&self) -> Self {
        Self {
            closed: true,
            points: self.points.clone(),
        }
    }

    /// `popen(path)` — the same vertices, open.
    #[must_use]
    pub fn to_open(&self) -> Self {
        Self {
            closed: false,
            points: self.points.clone(),
        }
    }

    /// SQL ordering for paths: `<`, `<=`, `=`, `>=`, `>` compare the *number of
    /// points* and nothing else — `path_n_lt` and friends, which upstream's own
    /// comment calls "as stupid as that sounds". Plain integer comparison, so
    /// unlike box and circle ordering it is total. `path` has no `<>`.
    #[must_use]
    pub fn compare(&self, other: &Self) -> std::cmp::Ordering {
        self.points.len().cmp(&other.points.len())
    }

    /// This path's segments, without allocating: an open path's are the
    /// consecutive pairs, and a closed path additionally gets the closure
    /// segment from the last vertex back to the first. A single-point open path
    /// yields none at all, which is why several path operations return `None`.
    fn edges(&self) -> impl Iterator<Item = Lseg> + '_ {
        let count = self.points.len();
        let closed = self.closed;
        (0..count).filter_map(move |index| {
            let previous = if index > 0 {
                index - 1
            } else if closed {
                count - 1
            } else {
                return None;
            };
            Some(self.points[previous].lseg_with(self.points[index]))
        })
    }

    /// `@-@` / `length(path)` — `path_length`, the sum over [`Path::edges`], so
    /// a closed path counts the closure segment. O(n).
    #[must_use]
    pub fn length(&self) -> f64 {
        self.edges().map(Lseg::length).sum()
    }

    /// `area(path)` — the shoelace formula, halved and made positive. `None`
    /// for an open path, where `PostgreSQL` returns NULL rather than treating it
    /// as closed. O(n).
    #[must_use]
    pub fn area(&self) -> Option<f64> {
        if !self.closed {
            return None;
        }
        let count = self.points.len();
        let mut area = 0.0;
        for (index, vertex) in self.points.iter().enumerate() {
            let next = self.points[(index + 1) % count];
            area += vertex.x * next.y;
            area -= vertex.y * next.x;
        }
        Some(area.abs() / 2.0)
    }

    /// `point <@ path` / `path @> point` — `on_ppath`. An open path asks
    /// whether the point lies *on* one of its segments; a closed one asks
    /// whether it lies *inside*, by the same ray-crossing count a polygon uses.
    /// O(n).
    #[must_use]
    pub fn contains_point(&self, point: Point) -> bool {
        if self.closed {
            return point_inside(point, &self.points) != 0;
        }
        self.edges().any(|edge| edge.contains_point(point))
    }

    /// `<->` — `dist_ppath`: the smallest distance to any segment. O(n).
    #[must_use]
    pub fn distance_to_point(&self, point: Point) -> f64 {
        let mut best = 0.0;
        let mut seen = false;
        for edge in self.edges() {
            let candidate = edge.distance_to_point(point);
            if !seen || float_lt(candidate, best) {
                best = candidate;
                seen = true;
            }
        }
        best
    }

    /// `<->` — `path_distance`: the Cartesian product of the two paths'
    /// segments, keeping the smallest. **O(n·m)** in the two vertex counts,
    /// which is upstream's own complexity; no allocation happens inside the
    /// loop because [`Path::edges`] yields `Copy` segments.
    ///
    /// `None` — NULL upstream — when either path has no segments at all, i.e.
    /// is a one-point open path.
    #[must_use]
    pub fn distance(&self, other: &Self) -> Option<f64> {
        let mut best: Option<f64> = None;
        for edge in self.edges() {
            for other_edge in other.edges() {
                let candidate = edge.distance(other_edge);
                if best.is_none_or(|current| float_lt(candidate, current)) {
                    best = Some(candidate);
                }
            }
        }
        best
    }

    /// `?#` — `path_inter`. A bounding-box rejection first, O(n+m), then the
    /// pairwise edge test, **O(n·m)** — again upstream's complexity, and again
    /// allocation-free.
    #[must_use]
    pub fn intersects(&self, other: &Self) -> bool {
        let (Some(bounds), Some(other_bounds)) = (self.bounding_box(), other.bounding_box()) else {
            return false;
        };
        if !bounds.overlaps(other_bounds) {
            return false;
        }
        self.edges()
            .any(|edge| other.edges().any(|other_edge| edge.intersects(other_edge)))
    }

    /// The smallest box containing every vertex, or `None` for an empty path.
    /// O(n).
    #[must_use]
    pub fn bounding_box(&self) -> Option<Box2> {
        bounding_box(&self.points)
    }

    /// `path + path` — `path_add`, plain concatenation. `None` — NULL upstream
    /// — when *either* operand is closed.
    #[must_use]
    pub fn concat(&self, other: &Self) -> Option<Self> {
        if self.closed || other.closed {
            return None;
        }
        let mut points = self.points.clone();
        points.extend_from_slice(&other.points);
        Some(Self {
            closed: false,
            points,
        })
    }

    /// `path + point` — translate every vertex. O(n).
    ///
    /// # Errors
    ///
    /// `22003` when a coordinate overflows.
    pub fn add_point(&self, point: Point) -> Result<Self, TypeError> {
        self.map_points(|vertex| vertex.add_point(point))
    }

    /// `path - point` — translate every vertex. O(n).
    ///
    /// # Errors
    ///
    /// `22003` when a coordinate overflows.
    pub fn sub_point(&self, point: Point) -> Result<Self, TypeError> {
        self.map_points(|vertex| vertex.sub_point(point))
    }

    /// `path * point` — rotate and scale every vertex. O(n).
    ///
    /// # Errors
    ///
    /// `22003` for overflow or underflow.
    pub fn mul_point(&self, point: Point) -> Result<Self, TypeError> {
        self.map_points(|vertex| vertex.mul_point(point))
    }

    /// `path / point` — the inverse of [`Path::mul_point`]. O(n).
    ///
    /// # Errors
    ///
    /// `22012` for the origin, `22003` for overflow or underflow.
    pub fn div_point(&self, point: Point) -> Result<Self, TypeError> {
        self.map_points(|vertex| vertex.div_point(point))
    }

    fn map_points(
        &self,
        transform: impl Fn(Point) -> Result<Point, TypeError>,
    ) -> Result<Self, TypeError> {
        Ok(Self {
            closed: self.closed,
            points: self
                .points
                .iter()
                .map(|vertex| transform(*vertex))
                .collect::<Result<Vec<_>, _>>()?,
        })
    }

    /// `polygon(path)` — `path_poly`. O(n).
    ///
    /// # Errors
    ///
    /// `22023` for an open path. Upstream's own comment notes this is "not very
    /// consistent" — the neighbouring conversions return NULL instead.
    pub fn to_polygon(&self) -> Result<Polygon, TypeError> {
        if !self.closed {
            return Err(TypeError::Domain {
                sqlstate: "22023",
                message: "open path cannot be converted to polygon",
            });
        }
        Ok(Polygon {
            points: self.points.clone(),
        })
    }
}

impl Point {
    /// Parse `point_in`'s ordinary `(x,y)` or `x,y` spelling.
    ///
    /// # Errors
    ///
    /// Returns `22P02` for malformed input and `22003` for a finite coordinate
    /// literal that overflows `float8`.
    pub fn parse(input: &str) -> Result<Self, TypeError> {
        let trimmed = input.trim();
        let inner = trimmed
            .strip_prefix('(')
            .and_then(|value| value.strip_suffix(')'))
            .unwrap_or(trimmed);
        let Some((x, y)) = inner.split_once(',') else {
            return Err(invalid(input));
        };
        if y.contains(',') || (trimmed.starts_with('(') != trimmed.ends_with(')')) {
            return Err(invalid(input));
        }
        Ok(Self {
            x: coordinate(x, input)?,
            y: coordinate(y, input)?,
        })
    }

    /// `point_eq_point`, which is `~=` and the equality every *other* geometric
    /// type reuses for its own corners and endpoints. Coordinates within
    /// `EPSILON` count as the same point, except when a NaN is involved: then
    /// `PostgreSQL` insists on exact equality, under which NaN *does* equal
    /// itself so that a NaN-cornered value can still be found by an index.
    ///
    /// This is a different relation from `PartialEq for Point`, which is exact
    /// throughout because an epsilon relation is not transitive and so cannot
    /// back `Hash`.
    #[must_use]
    pub fn eq_point(self, other: Self) -> bool {
        if self.x.is_nan() || self.y.is_nan() || other.x.is_nan() || other.y.is_nan() {
            return float_eq(self.x, other.x) && float_eq(self.y, other.y);
        }
        fp_eq(self.x, other.x) && fp_eq(self.y, other.y)
    }

    /// `<>` — `point_ne`, the negation of [`Point::eq_point`]. `point` has no
    /// `=` operator at all, only `~=` and `<>`.
    #[must_use]
    pub fn ne_point(self, other: Self) -> bool {
        !self.eq_point(other)
    }

    /// `<<` — `point_left`.
    #[must_use]
    pub fn is_left_of(self, other: Self) -> bool {
        fp_lt(self.x, other.x)
    }

    /// `>>` — `point_right`.
    #[must_use]
    pub fn is_right_of(self, other: Self) -> bool {
        fp_gt(self.x, other.x)
    }

    /// `<<|` and `<^` — both spell `point_below`. Unlike the box operator of
    /// the same name, `<^` on points is *strict*: `point '(1,2)' <^ point
    /// '(3,2)'` is false.
    #[must_use]
    pub fn is_below(self, other: Self) -> bool {
        fp_lt(self.y, other.y)
    }

    /// `|>>` and `>^` — both spell `point_above`, and `>^` is strict for the
    /// same reason as [`Point::is_below`].
    #[must_use]
    pub fn is_above(self, other: Self) -> bool {
        fp_gt(self.y, other.y)
    }

    /// `?-` — `point_horiz`: the two points share a horizontal.
    #[must_use]
    pub fn is_horizontal_with(self, other: Self) -> bool {
        fp_eq(self.y, other.y)
    }

    /// `?|` — `point_vert`: the two points share a vertical.
    #[must_use]
    pub fn is_vertical_with(self, other: Self) -> bool {
        fp_eq(self.x, other.x)
    }

    /// `<->` — `point_dt`, the Euclidean distance through [`pg_hypot`].
    #[must_use]
    pub fn distance(self, other: Self) -> f64 {
        pg_hypot(self.x - other.x, self.y - other.y)
    }

    /// `slope(point, point)` — `point_sl`. Infinite for a vertical pair, which
    /// includes two equal points; exactly zero for a horizontal one, rather
    /// than whatever sign the division would have produced.
    #[must_use]
    pub fn slope(self, other: Self) -> f64 {
        if fp_eq(self.x, other.x) {
            return f64::INFINITY;
        }
        if fp_eq(self.y, other.y) {
            return 0.0;
        }
        (self.y - other.y) / (self.x - other.x)
    }

    /// `point_invsl`: the slope of the perpendicular. Note the reversed
    /// subtraction in the numerator — it is the negative reciprocal, and the
    /// zero/infinity special cases are swapped relative to [`Point::slope`].
    fn inverse_slope(self, other: Self) -> f64 {
        if fp_eq(self.x, other.x) {
            return 0.0;
        }
        if fp_eq(self.y, other.y) {
            return f64::INFINITY;
        }
        (self.x - other.x) / (other.y - self.y)
    }

    /// `point + point` — componentwise.
    ///
    /// # Errors
    ///
    /// `22003` when a coordinate overflows.
    pub fn add_point(self, other: Self) -> Result<Self, TypeError> {
        Ok(Self {
            x: checked_add(self.x, other.x)?,
            y: checked_add(self.y, other.y)?,
        })
    }

    /// `point - point` — componentwise.
    ///
    /// # Errors
    ///
    /// `22003` when a coordinate overflows.
    pub fn sub_point(self, other: Self) -> Result<Self, TypeError> {
        Ok(Self {
            x: checked_sub(self.x, other.x)?,
            y: checked_sub(self.y, other.y)?,
        })
    }

    /// `point * point` — *complex* multiplication, not componentwise:
    /// `(ax,ay)(bx,by) = (ax·bx − ay·by, ax·by + ay·bx)`. That is what makes
    /// `box * point` and `path * point` rotate as well as scale.
    ///
    /// # Errors
    ///
    /// `22003` overflow, or underflow when a product of two non-zero factors
    /// flushes to zero.
    pub fn mul_point(self, other: Self) -> Result<Self, TypeError> {
        Ok(Self {
            x: checked_sub(checked_mul(self.x, other.x)?, checked_mul(self.y, other.y)?)?,
            y: checked_add(checked_mul(self.x, other.y)?, checked_mul(self.y, other.x)?)?,
        })
    }

    /// `point / point` — complex division. The divisor's squared modulus is
    /// computed once and both components divide by it, which is load-bearing:
    /// dividing by a huge point underflows on the *quotient*, not on the
    /// modulus.
    ///
    /// # Errors
    ///
    /// `22012` when the divisor is the origin, `22003` for overflow/underflow.
    pub fn div_point(self, other: Self) -> Result<Self, TypeError> {
        let modulus = checked_add(
            checked_mul(other.x, other.x)?,
            checked_mul(other.y, other.y)?,
        )?;
        Ok(Self {
            x: checked_div(
                checked_add(checked_mul(self.x, other.x)?, checked_mul(self.y, other.y)?)?,
                modulus,
            )?,
            y: checked_div(
                checked_sub(checked_mul(self.y, other.x)?, checked_mul(self.x, other.y)?)?,
                modulus,
            )?,
        })
    }

    /// `box(point)` — the degenerate box with both corners at this point.
    #[must_use]
    pub fn to_box(self) -> Box2 {
        Box2 {
            high: self,
            low: self,
        }
    }

    /// `box(point, point)` — `points_box`, which normalizes per coordinate.
    #[must_use]
    pub fn box_with(self, other: Self) -> Box2 {
        Box2::normalized(self, other)
    }

    /// `lseg(point, point)` — `lseg_construct`, which keeps the given order.
    #[must_use]
    pub fn lseg_with(self, other: Self) -> Lseg {
        Lseg {
            start: self,
            end: other,
        }
    }

    /// `line(point, point)` — `line_construct_pp`.
    ///
    /// # Errors
    ///
    /// `22023` for two equal points. The message matches `line_in`'s, but the
    /// SQLSTATE does not: the input function reports `22P02` because it is
    /// describing bad *text*, while the constructor reports a bad *parameter*.
    pub fn line_with(self, other: Self) -> Result<Line, TypeError> {
        if self.eq_point(other) {
            return Err(TypeError::Domain {
                sqlstate: "22023",
                message: "invalid line specification: must be two distinct points",
            });
        }
        Ok(Line::from_point_slope(self, self.slope(other)))
    }

    /// `circle(point, float8)` — `cr_circle`, which does not validate the
    /// radius the way `circle_in` does.
    #[must_use]
    pub fn circle_with(self, radius: f64) -> Circle {
        Circle {
            center: self,
            radius,
        }
    }
}

fn coordinate(value: &str, whole: &str) -> Result<f64, TypeError> {
    let value = value.trim();
    let parsed = value.parse::<f64>().map_err(|_| invalid(whole))?;
    let explicit_infinity = matches!(
        value.to_ascii_lowercase().as_str(),
        "inf" | "+inf" | "-inf" | "infinity" | "+infinity" | "-infinity"
    );
    if parsed.is_infinite() && !explicit_infinity {
        return Err(TypeError::float_text_out_of_range(
            value,
            "double precision",
        ));
    }
    Ok(parsed)
}

fn invalid(value: &str) -> TypeError {
    TypeError::InvalidText {
        type_name: "point",
        value: value.to_string(),
    }
}

/// `PostgreSQL`'s own `pg_hypot`, which is `x * sqrt(1 + (y/x)^2)` rather than
/// libm's `hypot`. The two disagree in the last digit, and `PostgreSQL`'s
/// expected output records its own.
fn pg_hypot(x: f64, y: f64) -> f64 {
    if x.is_infinite() || y.is_infinite() {
        return f64::INFINITY;
    }
    if x.is_nan() || y.is_nan() {
        return f64::NAN;
    }
    let (mut larger, mut smaller) = (x.abs(), y.abs());
    if larger < smaller {
        std::mem::swap(&mut larger, &mut smaller);
    }
    if larger == 0.0 {
        return 0.0;
    }
    let ratio = smaller / larger;
    // Deliberately not `mul_add`: C rounds the multiply and the add
    // separately, and a fused multiply-add can differ in the last digit.
    let squared = ratio * ratio;
    larger * (1.0 + squared).sqrt()
}

/// `PostgreSQL`'s `EPSILON` from `geo_decls.h`: the fuzz every geometric
/// comparison carries. Two coordinates closer than this are *the same*
/// coordinate as far as `geo_ops.c` is concerned, which is why the predicates
/// below are not spelled with plain `<`/`<=`. The relation is deliberately not
/// transitive, so it can never back `PartialEq`/`Hash` — the `PartialEq` impls
/// at the bottom of this file are exact for that reason.
const EPSILON: f64 = 1.0E-06;

/// `FPzero`.
fn fp_zero(value: f64) -> bool {
    value.abs() <= EPSILON
}

/// `FPeq`. The `==` arm is not redundant: it is what makes two like-signed
/// infinities equal without evaluating `Inf - Inf`, which is NaN.
fn fp_eq(left: f64, right: f64) -> bool {
    left == right || (left - right).abs() <= EPSILON
}

/// `FPne`. Deliberately *not* `!fp_eq`: with a NaN operand both `FPeq` and
/// `FPne` are false, so `circle '<(0,0),NaN>' <> circle '<(0,0),1>'` is false
/// just as `=` is. `partial_cmp` stands in for C's `!=` without tripping the
/// float-comparison lint, and agrees with it on NaN.
fn fp_ne(left: f64, right: f64) -> bool {
    left.partial_cmp(&right) != Some(std::cmp::Ordering::Equal) && (left - right).abs() > EPSILON
}

/// `FPlt`.
fn fp_lt(left: f64, right: f64) -> bool {
    left + EPSILON < right
}

/// `FPle`.
fn fp_le(left: f64, right: f64) -> bool {
    left <= right + EPSILON
}

/// `FPgt`.
fn fp_gt(left: f64, right: f64) -> bool {
    left > right + EPSILON
}

/// `FPge`.
fn fp_ge(left: f64, right: f64) -> bool {
    left + EPSILON >= right
}

/// C's bare `==` on two `float8`s, kept behind a `_eq` name so the
/// float-comparison lint lets it through. Unlike [`float_eq`] this is *not*
/// NaN-aware: `close_lseg` and `close_ls` guard on the raw C comparison, so a
/// NaN slope falls through to the distance path rather than short-circuiting.
fn raw_eq(left: f64, right: f64) -> bool {
    left == right
}

/// `float8_lt`: the btree ordering, in which NaN sorts above every number. The
/// "smallest so far" loops in `geo_ops.c` use this rather than `<`, so a NaN
/// candidate never displaces a real distance.
fn float_lt(left: f64, right: f64) -> bool {
    !left.is_nan() && (right.is_nan() || left < right)
}

/// `float8_gt`, the mirror of [`float_lt`].
fn float_gt(left: f64, right: f64) -> bool {
    !right.is_nan() && (left.is_nan() || left > right)
}

/// `float8_min`.
fn float_min(left: f64, right: f64) -> f64 {
    if float_lt(left, right) { left } else { right }
}

/// `float8_max`.
fn float_max(left: f64, right: f64) -> f64 {
    if float_gt(left, right) { left } else { right }
}

/// The shape every `*_lt`/`*_le`/`*_eq`/`*_ge`/`*_gt` family in `geo_ops.c`
/// shares: the SQL ordering operators run two *magnitudes* through the epsilon
/// macros, so `FPeq` outranks `FPlt`, and a NaN magnitude makes all five of
/// them false at once — which is `None`, not an ordering.
fn compare_with_epsilon(left: f64, right: f64) -> Option<std::cmp::Ordering> {
    if fp_eq(left, right) {
        Some(std::cmp::Ordering::Equal)
    } else if fp_lt(left, right) {
        Some(std::cmp::Ordering::Less)
    } else if fp_gt(left, right) {
        Some(std::cmp::Ordering::Greater)
    } else {
        None
    }
}

/// `float8_pl` with `PostgreSQL`'s `CHECKFLOATVAL`: a sum that goes infinite
/// from two finite operands is `22003`, not an infinity.
///
/// # Errors
///
/// `22003 value out of range: overflow`.
fn checked_add(left: f64, right: f64) -> Result<f64, TypeError> {
    let result = left + right;
    if result.is_infinite() && !left.is_infinite() && !right.is_infinite() {
        return Err(TypeError::float_overflow());
    }
    Ok(result)
}

/// `float8_mi`.
///
/// # Errors
///
/// `22003 value out of range: overflow`.
fn checked_sub(left: f64, right: f64) -> Result<f64, TypeError> {
    let result = left - right;
    if result.is_infinite() && !left.is_infinite() && !right.is_infinite() {
        return Err(TypeError::float_overflow());
    }
    Ok(result)
}

/// `float8_mul`. A product that flushes to zero from two non-zero operands is
/// an underflow, which is how `point '(1e-300,1e-300)' * point '(1e-300,…)'`
/// becomes an error rather than `(0,0)`.
///
/// # Errors
///
/// `22003 value out of range: overflow` or `: underflow`.
fn checked_mul(left: f64, right: f64) -> Result<f64, TypeError> {
    let result = left * right;
    if result.is_infinite() && !left.is_infinite() && !right.is_infinite() {
        return Err(TypeError::float_overflow());
    }
    if result == 0.0 && left != 0.0 && right != 0.0 {
        return Err(TypeError::float_underflow());
    }
    Ok(result)
}

/// `float8_div`. Note the divisor test comes *first* and spares NaN: `NaN / 0`
/// is NaN in `PostgreSQL`, while `1 / 0` is `22012`.
///
/// # Errors
///
/// `22012 division by zero`, or `22003` for overflow/underflow.
fn checked_div(left: f64, right: f64) -> Result<f64, TypeError> {
    if right == 0.0 && !left.is_nan() {
        return Err(TypeError::DivisionByZero);
    }
    let result = left / right;
    if result.is_infinite() && !left.is_infinite() {
        return Err(TypeError::float_overflow());
    }
    if result == 0.0 && left != 0.0 && !right.is_infinite() {
        return Err(TypeError::float_underflow());
    }
    Ok(result)
}

fn invalid_box(value: &str) -> TypeError {
    TypeError::InvalidText {
        type_name: "box",
        value: value.to_string(),
    }
}

fn invalid_circle(value: &str) -> TypeError {
    TypeError::InvalidText {
        type_name: "circle",
        value: value.to_string(),
    }
}

fn invalid_line(value: &str) -> TypeError {
    TypeError::InvalidText {
        type_name: "line",
        value: value.to_string(),
    }
}

/// `line_in`'s two rejections that describe the line rather than its syntax.
/// Both are 22P02 with a fixed message, as `PostgreSQL` writes them.
fn line_specification(message: &'static str) -> TypeError {
    TypeError::Domain {
        sqlstate: "22P02",
        message,
    }
}

fn invalid_lseg(value: &str) -> TypeError {
    TypeError::InvalidText {
        type_name: "lseg",
        value: value.to_string(),
    }
}

fn invalid_path(value: &str) -> TypeError {
    TypeError::InvalidText {
        type_name: "path",
        value: value.to_string(),
    }
}

fn invalid_polygon(value: &str) -> TypeError {
    TypeError::InvalidText {
        type_name: "polygon",
        value: value.to_string(),
    }
}

/// `PostgreSQL` `polygon`: a closed, non-directional vertex list. Upstream
/// caches a bounding box alongside the vertices; here it is recomputed by
/// [`Polygon::bounding_box`], which is O(n) rather than O(1) but keeps the
/// value canonical — two polygons that print alike cannot disagree about their
/// box.
///
/// `PartialEq`/`Hash` are the exact, vertex-order-sensitive relation inherited
/// from [`Point`], for storage keys. SQL's `~=` is [`Polygon::same`], which is
/// fuzzy *and* insensitive to rotation and direction — not a relation that can
/// back a hash.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Polygon {
    pub points: Vec<Point>,
}

impl Polygon {
    /// Parse `poly_in`: `((x,y),…)`, `(x,y),…`, the bare `x,y,…`, and any of
    /// those inside one extra parenthesis pair. Square brackets are rejected —
    /// a polygon is always closed, so there is no open spelling.
    ///
    /// The point count comes from the comma count, exactly as `pair_count`
    /// derives it: an even number of commas cannot describe whole pairs and is
    /// rejected outright, which is also how the empty string is rejected.
    ///
    /// # Errors
    ///
    /// `22P02` for malformed input, `22003` for a coordinate that overflows
    /// `float8`.
    pub fn parse(input: &str) -> Result<Self, TypeError> {
        let (_, points) = decode_point_list(input, invalid_polygon, false)?;
        Ok(Self { points })
    }

    /// `#` / `npoints(polygon)` — `poly_npoints`.
    #[must_use]
    pub fn npoints(&self) -> i32 {
        i32::try_from(self.points.len()).unwrap_or(i32::MAX)
    }

    /// `box(polygon)` — the cached `boundbox`, which is also what every
    /// positional operator below compares. O(n).
    #[must_use]
    pub fn bounding_box(&self) -> Box2 {
        bounding_box(&self.points).unwrap_or(Box2 {
            high: Point { x: 0.0, y: 0.0 },
            low: Point { x: 0.0, y: 0.0 },
        })
    }

    /// `@@` / `point(polygon)` — `poly_center`, which is the mean of the
    /// *vertices*, **not** the centre of the bounding box. The two agree for a
    /// rectangle and diverge for anything else, so this is worth checking
    /// against upstream rather than assuming. O(n).
    #[must_use]
    pub fn center(&self) -> Point {
        self.to_circle().center
    }

    /// `point(polygon)` — the same point as [`Polygon::center`].
    #[must_use]
    pub fn to_point(&self) -> Point {
        self.center()
    }

    /// `box(polygon)` — [`Polygon::bounding_box`].
    #[must_use]
    pub fn to_box(&self) -> Box2 {
        self.bounding_box()
    }

    /// `path(polygon)` — `poly_path`: the same vertices as a *closed* path.
    #[must_use]
    pub fn to_path(&self) -> Path {
        Path {
            closed: true,
            points: self.points.clone(),
        }
    }

    /// `circle(polygon)` — `poly_to_circle`: centred on the vertex mean, with
    /// the mean vertex distance as radius. Upstream's own comment concedes this
    /// should be weighting the edges instead. O(n).
    #[must_use]
    pub fn to_circle(&self) -> Circle {
        let count = f64::from(self.npoints());
        let mut center = Point { x: 0.0, y: 0.0 };
        for vertex in &self.points {
            center.x += vertex.x;
            center.y += vertex.y;
        }
        center.x /= count;
        center.y /= count;
        let mut radius = 0.0;
        for vertex in &self.points {
            radius += vertex.distance(center);
        }
        Circle {
            center,
            radius: radius / count,
        }
    }

    /// This polygon's edges, without allocating. A polygon is always closed, so
    /// the edge from the last vertex back to the first is included.
    fn edges(&self) -> impl Iterator<Item = Lseg> + '_ {
        let count = self.points.len();
        (0..count).map(move |index| {
            let previous = if index > 0 { index - 1 } else { count - 1 };
            self.points[previous].lseg_with(self.points[index])
        })
    }

    /// `point <@ polygon` / `polygon @> point` — `point_inside`, by ray
    /// crossing. A point *on* an edge counts as contained. O(n).
    #[must_use]
    pub fn contains_point(&self, point: Point) -> bool {
        point_inside(point, &self.points) != 0
    }

    /// `<->` — `dist_ppoly`: zero for a point inside, otherwise the smallest
    /// distance to any edge. O(n).
    #[must_use]
    pub fn distance_to_point(&self, point: Point) -> f64 {
        if self.contains_point(point) {
            return 0.0;
        }
        let mut best = f64::INFINITY;
        let mut seen = false;
        for edge in self.edges() {
            let candidate = edge.distance_to_point(point);
            if !seen || float_lt(candidate, best) {
                best = candidate;
                seen = true;
            }
        }
        best
    }

    /// `&&` — `poly_overlap`. A bounding-box rejection first, then a pairwise
    /// edge-crossing search, and only if that finds nothing does either
    /// polygon's first vertex get tested for containment in the other —
    /// containment without a crossing is exactly the nested case.
    ///
    /// **O(n·m)** in the two vertex counts, matching upstream. No allocation
    /// inside the loop: [`Polygon::edges`] yields `Copy` segments.
    #[must_use]
    pub fn overlaps(&self, other: &Self) -> bool {
        if self.points.is_empty() || other.points.is_empty() {
            return false;
        }
        if !self.bounding_box().overlaps(other.bounding_box()) {
            return false;
        }
        if self
            .edges()
            .any(|edge| other.edges().any(|other_edge| edge.intersects(other_edge)))
        {
            return true;
        }
        point_inside(self.points[0], &other.points) != 0
            || point_inside(other.points[0], &self.points) != 0
    }

    /// `@>` / `<@` — `poly_contain_poly`: a bounding-box rejection, then each
    /// of the contained polygon's edges must lie wholly inside.
    ///
    /// **O(n·m)** in the common case — the container's n edges are walked once
    /// per contained edge. The recursion inside [`lseg_inside_poly`] only ever
    /// resumes at a *later* container edge, so it cannot revisit one and the
    /// bound holds. No allocation happens in either loop.
    #[must_use]
    pub fn contains_polygon(&self, other: &Self) -> bool {
        if self.points.is_empty() || other.points.is_empty() {
            return false;
        }
        if !self.bounding_box().contains(other.bounding_box()) {
            return false;
        }
        other
            .edges()
            .all(|edge| lseg_inside_poly(edge.start, edge.end, self, 0))
    }

    /// `<->` — `poly_distance`. Zero when the polygons overlap, which has to be
    /// tested separately because the edge search alone would miss one polygon
    /// nested inside the other. Otherwise the Cartesian product of the edges.
    ///
    /// **O(n·m)**, matching upstream, allocation-free. `None` — NULL upstream —
    /// only for an empty vertex list, which `poly_in` cannot produce.
    #[must_use]
    pub fn distance(&self, other: &Self) -> Option<f64> {
        if self.overlaps(other) {
            return Some(0.0);
        }
        let mut best: Option<f64> = None;
        for edge in self.edges() {
            for other_edge in other.edges() {
                let candidate = edge.distance(other_edge);
                if best.is_none_or(|current| float_lt(candidate, current)) {
                    best = Some(candidate);
                }
            }
        }
        best
    }

    /// `<->` — `dist_polyc`, the same number as [`Circle::distance_to_polygon`].
    #[must_use]
    pub fn distance_to_circle(&self, circle: Circle) -> f64 {
        circle.distance_to_polygon(self)
    }

    /// `~=` — `poly_same`, which walks the other vertex list from every
    /// possible starting offset in *both* directions: a polygon is a closed,
    /// non-directional shape, so rotating or reversing its vertices does not
    /// change it. O(n²) worst case, as upstream.
    #[must_use]
    pub fn same(&self, other: &Self) -> bool {
        let count = self.points.len();
        if count != other.points.len() {
            return false;
        }
        (0..count).any(|offset| {
            let forward = (0..count)
                .all(|step| other.points[(offset + step) % count].eq_point(self.points[step]));
            forward
                || (0..count).all(|step| {
                    other.points[(offset + count - step) % count].eq_point(self.points[step])
                })
        })
    }

    /// `<<` — `poly_left`, and the seven that follow it, all decided from the
    /// bounding box. Unlike the box operators of the same spelling these use
    /// **bare** comparisons with no `EPSILON`, so a polygon can be strictly left
    /// of another that the equivalent boxes would call overlapping.
    #[must_use]
    pub fn strictly_left_of(&self, other: &Self) -> bool {
        self.bounding_box().high.x < other.bounding_box().low.x
    }

    /// `>>` — `poly_right`.
    #[must_use]
    pub fn strictly_right_of(&self, other: &Self) -> bool {
        self.bounding_box().low.x > other.bounding_box().high.x
    }

    /// `&<` — `poly_overleft`.
    #[must_use]
    pub fn does_not_extend_right(&self, other: &Self) -> bool {
        self.bounding_box().high.x <= other.bounding_box().high.x
    }

    /// `&>` — `poly_overright`.
    #[must_use]
    pub fn does_not_extend_left(&self, other: &Self) -> bool {
        self.bounding_box().low.x >= other.bounding_box().low.x
    }

    /// `<<|` — `poly_below`.
    #[must_use]
    pub fn strictly_below(&self, other: &Self) -> bool {
        self.bounding_box().high.y < other.bounding_box().low.y
    }

    /// `|>>` — `poly_above`.
    #[must_use]
    pub fn strictly_above(&self, other: &Self) -> bool {
        self.bounding_box().low.y > other.bounding_box().high.y
    }

    /// `&<|` — `poly_overbelow`.
    #[must_use]
    pub fn does_not_extend_above(&self, other: &Self) -> bool {
        self.bounding_box().high.y <= other.bounding_box().high.y
    }

    /// `|&>` — `poly_overabove`.
    #[must_use]
    pub fn does_not_extend_below(&self, other: &Self) -> bool {
        self.bounding_box().low.y >= other.bounding_box().low.y
    }
}

/// `make_bound_box`: the smallest box containing every point, or `None` for an
/// empty list. Uses the NaN-aware ordering, so a NaN coordinate propagates into
/// the box rather than being skipped.
fn bounding_box(points: &[Point]) -> Option<Box2> {
    let (first, rest) = points.split_first()?;
    let mut bounds = Box2 {
        high: *first,
        low: *first,
    };
    for point in rest {
        bounds.low.x = float_min(point.x, bounds.low.x);
        bounds.high.x = float_max(point.x, bounds.high.x);
        bounds.low.y = float_min(point.y, bounds.low.y);
        bounds.high.y = float_max(point.y, bounds.high.y);
    }
    Some(bounds)
}

/// `lseg_crossing`'s verdict for one polygon edge, relative to the ray running
/// right from the test point. Upstream signals "the point is on the boundary"
/// by returning `INT_MAX` from a function whose other answers are −2…2.
enum Crossing {
    OnBoundary,
    Count(i32),
}

/// `point_inside`: 0 outside, 1 inside, 2 on the boundary. The vertices are
/// taken relative to the test point, so the question becomes how many times the
/// boundary crosses the positive x-axis. O(n).
fn point_inside(point: Point, vertices: &[Point]) -> i32 {
    let Some((first, rest)) = vertices.split_first() else {
        return 0;
    };
    let origin = Point {
        x: first.x - point.x,
        y: first.y - point.y,
    };
    let mut previous = origin;
    let mut total = 0;
    for vertex in rest {
        let current = Point {
            x: vertex.x - point.x,
            y: vertex.y - point.y,
        };
        match lseg_crossing(current, previous) {
            Crossing::OnBoundary => return 2,
            Crossing::Count(count) => total += count,
        }
        previous = current;
    }
    match lseg_crossing(origin, previous) {
        Crossing::OnBoundary => return 2,
        Crossing::Count(count) => total += count,
    }
    i32::from(total != 0)
}

/// `lseg_crossing`: how the edge from `previous` to `current` — both already
/// relative to the test point — crosses the positive x-axis. Counts are doubled
/// so that an edge merely *touching* the axis can contribute half a crossing.
fn lseg_crossing(current: Point, previous: Point) -> Crossing {
    if fp_zero(current.y) {
        return crossing_along_axis(current.x, previous);
    }
    let sign = if fp_gt(current.y, 0.0) { 1 } else { -1 };
    if fp_zero(previous.y) {
        return Crossing::Count(if fp_lt(previous.x, 0.0) { 0 } else { sign });
    }
    if (sign < 0 && fp_lt(previous.y, 0.0)) || (sign > 0 && fp_gt(previous.y, 0.0)) {
        // Both endpoints on the same side of the axis: no crossing.
        return Crossing::Count(0);
    }
    if fp_ge(current.x, 0.0) && fp_gt(previous.x, 0.0) {
        return Crossing::Count(2 * sign);
    }
    if fp_lt(current.x, 0.0) && fp_le(previous.x, 0.0) {
        return Crossing::Count(0);
    }
    // The edge straddles both axes: the cross product decides which side of the
    // origin it passes.
    let cross = (current.x - previous.x) * current.y - (current.y - previous.y) * current.x;
    if fp_zero(cross) {
        return Crossing::OnBoundary;
    }
    if (sign < 0 && fp_lt(cross, 0.0)) || (sign > 0 && fp_gt(cross, 0.0)) {
        Crossing::Count(0)
    } else {
        Crossing::Count(2 * sign)
    }
}

/// The `y == 0` half of [`lseg_crossing`], split out to keep either arm
/// readable: the current vertex sits on the x-axis, so whether it counts turns
/// on which side of the origin it is and where the previous vertex was.
fn crossing_along_axis(x: f64, previous: Point) -> Crossing {
    if fp_zero(x) {
        // The test point *is* this vertex.
        return Crossing::OnBoundary;
    }
    if fp_gt(x, 0.0) {
        if fp_zero(previous.y) {
            return if fp_gt(previous.x, 0.0) {
                Crossing::Count(0)
            } else {
                Crossing::OnBoundary
            };
        }
        return Crossing::Count(if fp_lt(previous.y, 0.0) { 1 } else { -1 });
    }
    if fp_zero(previous.y) {
        return if fp_lt(previous.x, 0.0) {
            Crossing::Count(0)
        } else {
            Crossing::OnBoundary
        };
    }
    Crossing::Count(0)
}

/// `lseg_inside_poly`: is the segment `start`–`end` wholly inside `polygon`,
/// checking the polygon's edges from index `from` onwards?
///
/// The recursion is bounded: every recursive call passes a strictly larger
/// `from`, so the total work across one top-level call is O(n) edge visits per
/// branch and the whole `polygon @> polygon` test stays O(n·m). Nothing is
/// allocated — the segments are `Copy`.
fn lseg_inside_poly(start: Point, end: Point, polygon: &Polygon, from: usize) -> bool {
    let count = polygon.points.len();
    let segment = start.lseg_with(end);
    let mut previous = polygon.points[if from == 0 { count - 1 } else { from - 1 }];
    let mut inside = true;
    let mut crossed = false;

    for index in from..count {
        if !inside {
            break;
        }
        let edge = previous.lseg_with(polygon.points[index]);
        if edge.contains_point(start) {
            if edge.contains_point(end) {
                // The whole segment lies along this edge.
                return true;
            }
            inside = touched_lseg_inside_poly(start, end, edge, polygon, index + 1);
        } else if edge.contains_point(end) {
            inside = touched_lseg_inside_poly(end, start, edge, polygon, index + 1);
        } else if let Some(crossing) = segment.intersection_point(edge) {
            // An X-crossing: each half is checked separately, so the midpoint
            // test below is not needed.
            crossed = true;
            inside = lseg_inside_poly(start, crossing, polygon, index + 1)
                && lseg_inside_poly(end, crossing, polygon, index + 1);
        }
        previous = polygon.points[index];
    }

    if inside && !crossed {
        let midpoint = Point {
            x: f64::midpoint(start.x, end.x),
            y: f64::midpoint(start.y, end.y),
        };
        inside = point_inside(midpoint, &polygon.points) != 0;
    }
    inside
}

/// `touched_lseg_inside_poly`: the segment touches `edge` at `touch` but does
/// not run along it. Continue from whichever end of the edge the segment
/// reaches, if any; otherwise defer the verdict to the caller's later checks.
fn touched_lseg_inside_poly(
    touch: Point,
    far: Point,
    edge: Lseg,
    polygon: &Polygon,
    from: usize,
) -> bool {
    let reach = far.lseg_with(touch);
    if touch.eq_point(edge.start) {
        if reach.contains_point(edge.end) {
            return lseg_inside_poly(far, edge.end, polygon, from);
        }
    } else if touch.eq_point(edge.end) {
        if reach.contains_point(edge.start) {
            return lseg_inside_poly(far, edge.start, polygon, from);
        }
    } else if reach.contains_point(edge.start) {
        return lseg_inside_poly(far, edge.start, polygon, from);
    } else if reach.contains_point(edge.end) {
        return lseg_inside_poly(far, edge.end, polygon, from);
    }
    true
}

/// `pair_count` + `path_decode`: the vertex list shared by `path_in` and
/// `poly_in`. Returns whether the list used the *open* `[…]` spelling — which
/// only `path` may — together with the points.
///
/// The point count comes from the comma count, exactly as `pair_count` derives
/// it: an even number of commas cannot describe whole pairs and is rejected
/// outright, which is also how the empty string is rejected. O(n).
fn decode_point_list(
    input: &str,
    invalid: fn(&str) -> TypeError,
    allow_open: bool,
) -> Result<(bool, Vec<Point>), TypeError> {
    let malformed = || invalid(input);
    let commas = input.bytes().filter(|byte| *byte == b',').count();
    if commas % 2 == 0 {
        return Err(malformed());
    }
    let count = commas.div_ceil(2);

    let mut rest = input.trim_start();
    let mut open = false;
    let mut wrapped = false;
    if let Some(after) = rest.strip_prefix('[') {
        if !allow_open {
            return Err(malformed());
        }
        open = true;
        wrapped = true;
        rest = after;
    } else if let Some(after) = rest.strip_prefix('(') {
        // One optional wrapper, recognized the way `path_decode` does: either
        // the next thing is another `(`, or this is the value's only `(` — the
        // case that makes the flat `(0,0,1,0)` parse as two points rather than
        // as one parenthesized pair.
        let inner = after.trim_start();
        if inner.starts_with('(') || rest.matches('(').count() == 1 {
            wrapped = true;
            rest = inner;
        }
    }
    let mut points = Vec::with_capacity(count);
    for _ in 0..count {
        let (point, tail) = decode_pair(rest, input, invalid)?;
        points.push(point);
        // `path_decode` eats a following comma unconditionally, including after
        // the final pair.
        rest = tail.strip_prefix(',').unwrap_or(tail);
    }
    if wrapped {
        let closer = if open { ']' } else { ')' };
        rest = rest
            .strip_prefix(closer)
            .ok_or_else(malformed)?
            .trim_start();
    }
    if !rest.is_empty() {
        return Err(malformed());
    }
    Ok((open, points))
}

/// `pair_decode`: one `x,y` pair with or without its own parentheses, and the
/// unconsumed tail.
fn decode_pair<'a>(
    text: &'a str,
    whole: &str,
    invalid: fn(&str) -> TypeError,
) -> Result<(Point, &'a str), TypeError> {
    let malformed = || invalid(whole);
    let mut rest = text.trim_start();
    let has_parentheses = rest.starts_with('(');
    if has_parentheses {
        rest = &rest[1..];
    }
    let (x, tail) = decode_float(rest, whole, invalid)?;
    let tail = tail.strip_prefix(',').ok_or_else(malformed)?;
    let (y, mut tail) = decode_float(tail, whole, invalid)?;
    if has_parentheses {
        tail = tail.strip_prefix(')').ok_or_else(malformed)?.trim_start();
    }
    Ok((Point { x, y }, tail))
}

/// `single_decode`: `strtod`'s longest numeric prefix, with the surrounding
/// whitespace eaten on both sides, and the unconsumed tail.
fn decode_float<'a>(
    text: &'a str,
    whole: &str,
    invalid: fn(&str) -> TypeError,
) -> Result<(f64, &'a str), TypeError> {
    let trimmed = text.trim_start();
    let signed = usize::from(matches!(trimmed.as_bytes().first(), Some(b'+' | b'-')));
    let rest = &trimmed[signed..];
    let starts_with =
        |word: &str| rest.len() >= word.len() && rest[..word.len()].eq_ignore_ascii_case(word);
    let end = signed
        + if starts_with("infinity") {
            "infinity".len()
        } else if starts_with("inf") {
            "inf".len()
        } else if starts_with("nan") {
            "nan".len()
        } else {
            numeric_prefix_len(rest)
        };
    let value = coordinate(&trimmed[..end], whole).map_err(|error| match error {
        TypeError::InvalidText { .. } => invalid(whole),
        other => other,
    })?;
    Ok((value, trimmed[end..].trim_start()))
}

/// The length of the leading decimal literal in `text`: digits, an optional
/// fractional part, and an optional exponent.
fn numeric_prefix_len(text: &str) -> usize {
    let bytes = text.as_bytes();
    let mut end = 0;
    while bytes.get(end).is_some_and(u8::is_ascii_digit) {
        end += 1;
    }
    if bytes.get(end) == Some(&b'.') {
        end += 1;
        while bytes.get(end).is_some_and(u8::is_ascii_digit) {
            end += 1;
        }
    }
    if matches!(bytes.get(end), Some(b'e' | b'E')) {
        let mut exponent = end + 1;
        if matches!(bytes.get(exponent), Some(b'+' | b'-')) {
            exponent += 1;
        }
        if bytes.get(exponent).is_some_and(u8::is_ascii_digit) {
            while bytes.get(exponent).is_some_and(u8::is_ascii_digit) {
                exponent += 1;
            }
            end = exponent;
        }
    }
    end
}

impl PartialEq for Circle {
    /// Structural identity, for storage keys and hashing. SQL's `=` is a
    /// different relation — see [`Circle::compare`] — so the two are kept
    /// apart: an epsilon relation is not transitive and cannot back `Hash`.
    fn eq(&self, other: &Self) -> bool {
        self.center == other.center && float_eq(self.radius, other.radius)
    }
}

impl Eq for Circle {}

impl Hash for Circle {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.center.hash(state);
        float_bits(self.radius).hash(state);
    }
}

impl PartialEq for Line {
    /// Structural identity, for storage keys and hashing: coefficient for
    /// coefficient, with a `NaN` equal to itself. SQL's `=` is a DIFFERENT
    /// relation — see [`Line::eq_line`], which holds between any two
    /// proportional coefficient triples — so the two are kept apart: a
    /// proportional-and-epsilon relation is not transitive and cannot back
    /// `Hash`.
    fn eq(&self, other: &Self) -> bool {
        float_eq(self.a, other.a) && float_eq(self.b, other.b) && float_eq(self.c, other.c)
    }
}

impl Eq for Line {}

impl Hash for Line {
    fn hash<H: Hasher>(&self, state: &mut H) {
        float_bits(self.a).hash(state);
        float_bits(self.b).hash(state);
        float_bits(self.c).hash(state);
    }
}

impl PartialEq for Point {
    fn eq(&self, other: &Self) -> bool {
        float_eq(self.x, other.x) && float_eq(self.y, other.y)
    }
}

impl Eq for Point {}

impl Hash for Point {
    fn hash<H: Hasher>(&self, state: &mut H) {
        float_bits(self.x).hash(state);
        float_bits(self.y).hash(state);
    }
}

fn float_eq(left: f64, right: f64) -> bool {
    left == right || (left.is_nan() && right.is_nan())
}

fn float_bits(value: f64) -> u64 {
    if value.is_nan() {
        f64::NAN.to_bits()
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_input_accepts_coordinates_and_rejects_bad_shapes() {
        assert_eq!(
            Point::parse(" ( -10, Infinity ) "),
            Ok(Point {
                x: -10.0,
                y: f64::INFINITY
            })
        );
        assert_eq!(Point::parse("10,20"), Ok(Point { x: 10.0, y: 20.0 }));
        assert_eq!(Point::parse("(10 20)").unwrap_err().sqlstate(), "22P02");
        assert_eq!(Point::parse("(10,1e500)").unwrap_err().sqlstate(), "22003");
    }

    /// `lseg_in` accepts every spelling `PostgreSQL` does and rejects anything
    /// that is not exactly two points.
    #[test]
    fn lseg_input_accepts_every_two_point_spelling() {
        let expected = Lseg {
            start: Point { x: 1.0, y: 2.0 },
            end: Point { x: 3.0, y: 4.0 },
        };
        for spelling in [
            "[(1,2),(3,4)]",
            "((1,2),(3,4))",
            "(1,2),(3,4)",
            "1,2,3,4",
            "[1,2,3, 4]",
            "  [ (1,2) , (3,4) ]  ",
        ] {
            assert_eq!(Lseg::parse(spelling), Ok(expected), "{spelling}");
        }
        // NaN coordinates are values, not errors.
        let nan = Lseg::parse("[(NaN,1),(NaN,90)]").expect("NaN endpoints");
        assert!(nan.start.x.is_nan() && nan.end.x.is_nan());

        for bad in [
            "(3asdf,2 ,3,4r2)",
            "[1,2,3, 4",
            "[(,2),(3,4)]",
            "[(1,2),(3,4)",
            "[(1,2),(3)]",
            "[(1,2),(3,4),(5,6)]",
            "[(1,2)]",
        ] {
            let error = Lseg::parse(bad).expect_err(bad);
            assert_eq!(error.sqlstate(), "22P02", "{bad}");
            assert_eq!(
                error.to_string(),
                format!("invalid input syntax for type lseg: \"{bad}\""),
                "{bad}"
            );
        }
    }

    /// `line_in` takes coefficients directly and converts any two-point
    /// spelling, normalizing to `B = -1` except for a vertical line.
    /// `circle_in` accepts every spelling, keeps a zero or NaN radius as a
    /// value, and rejects a negative one. Comparison is by area through
    /// `PostgreSQL`'s epsilon macros, so the centres are ignored.
    /// `box_in` normalizes per coordinate, so the stored high corner is always
    /// `(max x, max y)` whichever corners were written — and unlike `lseg_in`
    /// it does not accept square brackets.
    #[test]
    fn box_input_normalizes_its_corners() {
        use assert2::assert;

        let unit = Box2 {
            high: Point { x: 3.0, y: 3.0 },
            low: Point { x: 1.0, y: 1.0 },
        };
        for spelling in [
            "(1.0,1.0,3.0,3.0)",
            "(3,3),(1,1)",
            "((1,1),(3,3))",
            "3,3,1,1",
        ] {
            assert!(Box2::parse(spelling) == Ok(unit), "{spelling}");
        }
        // Each coordinate is sorted independently, not the points as a pair.
        assert!(
            Box2::parse("((-8, 2), (-2, -10))")
                == Ok(Box2 {
                    high: Point { x: -2.0, y: 2.0 },
                    low: Point { x: -8.0, y: -10.0 },
                })
        );
        // A degenerate box is a point or a line: zero area, not a rejection.
        assert!(
            Box2::parse("(3,3,3,3)")
                .expect("degenerate")
                .area()
                .to_bits()
                == 0.0_f64.to_bits()
        );
        // These are exact in binary floating point, so compare bit patterns
        // rather than tripping the float-comparison lint.
        for (measured, expected) in [
            (unit.width(), 2.0_f64),
            (unit.height(), 2.0),
            (unit.area(), 4.0),
        ] {
            assert!(measured.to_bits() == expected.to_bits());
        }

        for bad in [
            "(2.3, 4.5)",
            "[1, 2, 3, 4)",
            "(1, 2, 3, 4]",
            "asdfasdf(ad",
            "(1, 2, 3, 4) x",
        ] {
            let error = Box2::parse(bad).expect_err(bad);
            assert!(error.sqlstate() == "22P02", "{bad}");
            assert!(
                error.to_string() == format!("invalid input syntax for type box: \"{bad}\""),
                "{bad}"
            );
        }
    }

    #[test]
    fn circle_input_and_area_comparison_match_postgres() {
        use std::cmp::Ordering;

        use assert2::assert;

        let expected = Circle {
            center: Point { x: 1.0, y: 2.0 },
            radius: 3.0,
        };
        for spelling in [
            "<(1,2),3>",
            "((1,2),3)",
            "(1,2),3",
            "1,2,3",
            " ( ( 1 , 2 ) , 3 ) ",
        ] {
            assert!(Circle::parse(spelling) == Ok(expected), "{spelling}");
        }
        // A zero radius is a value, not a rejection.
        assert!(
            Circle::parse("<(3,5),0>")
                .expect("zero radius")
                .radius
                .to_bits()
                == 0.0_f64.to_bits()
        );
        assert!(
            Circle::parse("<(3,5),NaN>")
                .expect("NaN radius")
                .radius
                .is_nan()
        );
        for bad in [
            "<(-100,0),-100>",
            "<(100,200),10",
            "<(100,200),10> x",
            "1abc,3,5",
            "(3,(1,2),3)",
        ] {
            let error = Circle::parse(bad).expect_err(bad);
            assert!(error.sqlstate() == "22P02", "{bad}");
            assert!(
                error.to_string() == format!("invalid input syntax for type circle: \"{bad}\""),
                "{bad}"
            );
        }

        let unit = Circle::parse("<(0,0),1>").expect("unit");
        // Areas within EPSILON are equal however far apart the centres are.
        assert!(
            unit.compare(Circle::parse("<(9,9),1.0000001>").expect("near"))
                == Some(Ordering::Equal)
        );
        assert!(unit.compare(Circle::parse("<(9,9),1.001>").expect("far")) == Some(Ordering::Less));
        // A NaN area leaves every operator undefined.
        assert!(unit.compare(Circle::parse("<(3,5),NaN>").expect("nan")) == None);
    }

    #[test]
    fn line_input_converts_points_to_coefficients() {
        for (spelling, expected) in [
            (
                "{0,-1,5}",
                Line {
                    a: 0.0,
                    b: -1.0,
                    c: 5.0,
                },
            ),
            (
                "(0,0), (6,6)",
                Line {
                    a: 1.0,
                    b: -1.0,
                    c: 0.0,
                },
            ),
            (
                "10,-10 ,-5,-4",
                Line {
                    a: -0.4,
                    b: -1.0,
                    c: -6.0,
                },
            ),
            // Horizontal and vertical are the two normalized special cases.
            (
                "[(1,3),(2,3)]",
                Line {
                    a: 0.0,
                    b: -1.0,
                    c: 3.0,
                },
            ),
            (
                "[(3,1),(3,2)]",
                Line {
                    a: -1.0,
                    b: 0.0,
                    c: 3.0,
                },
            ),
        ] {
            assert_eq!(Line::parse(spelling), Ok(expected), "{spelling}");
        }
        // A NaN coefficient equals itself, which `line_eq` relies on.
        assert_eq!(Line::parse("{NaN,NaN,NaN}"), Line::parse("{NaN,NaN,NaN}"));

        for (bad, message) in [
            ("{}", "invalid input syntax for type line: \"{}\""),
            ("{0,0}", "invalid input syntax for type line: \"{0,0}\""),
            (
                "{1, 1, a}",
                "invalid input syntax for type line: \"{1, 1, a}\"",
            ),
            (
                "{0,0,1}",
                "invalid line specification: A and B cannot both be zero",
            ),
            (
                "[(1,2),(1,2)]",
                "invalid line specification: must be two distinct points",
            ),
        ] {
            let error = Line::parse(bad).expect_err(bad);
            assert_eq!(error.sqlstate(), "22P02", "{bad}");
            assert_eq!(error.to_string(), message, "{bad}");
        }
        // A coordinate overflow is the float's own 22003, not a syntax error.
        let overflow = Line::parse("{1, 1, 1e400}").expect_err("1e400");
        assert_eq!(overflow.sqlstate(), "22003");
        assert_eq!(
            overflow.to_string(),
            "\"1e400\" is out of range for type double precision"
        );
    }

    /// `line_eq` holds between PROPORTIONAL coefficient triples, so `{1,-1,0}`
    /// and `{2,-2,0}` are the same line. Neither engine normalizes on input —
    /// `line '{2,-2,0}'` still prints `{2,-2,0}` — so the scale factor is
    /// divided out at comparison time, and the structural [`PartialEq`] that
    /// backs `Hash` stays a different, finer relation.
    ///
    /// Every expected value here was taken from `PostgreSQL` 18.4.
    #[test]
    fn line_equality_is_proportional_not_field_by_field() {
        use assert2::assert;

        let line = |text: &str| Line::parse(text).unwrap_or_else(|_| panic!("{text}"));
        // (left, right, `line_eq`, structural `PartialEq`)
        let cases: &[(&str, &str, bool, bool)] = &[
            ("{1,-1,0}", "{1,-1,0}", true, true),
            // Proportional: equal as lines, distinct as stored triples.
            ("{1,-1,0}", "{2,-2,0}", true, false),
            ("{1,-1,0}", "{-1,1,0}", true, false),
            ("{0,-1,5}", "{0,-2,10}", true, false),
            ("{1,-1,5}", "{3,-3,15}", true, false),
            // Parallel but offset: not the same line.
            ("{1,-1,0}", "{1,-1,5}", false, false),
            ("{0,-1,5}", "{0,-1,6}", false, false),
            // Not proportional at all.
            ("{1,-1,0}", "{1,-2,0}", false, false),
            // A NaN anywhere falls back to exact equality, which is what makes
            // an all-NaN line equal itself: through the ratio it would be
            // `FPeq(NaN, NaN)`, which is false.
            ("{NaN,NaN,NaN}", "{NaN,NaN,NaN}", true, true),
            ("{3,NaN,5}", "{3,NaN,5}", true, true),
            ("{3,NaN,5}", "{6,NaN,10}", false, false),
        ];
        for (left, right, equal_as_lines, equal_structurally) in cases {
            let (a, b) = (line(left), line(right));
            assert!(a.eq_line(b) == *equal_as_lines, "{left} = {right}");
            // `line_eq` is symmetric even though the ratio is taken from the
            // right operand's coefficients.
            assert!(b.eq_line(a) == *equal_as_lines, "{right} = {left}");
            assert!((a == b) == *equal_structurally, "{left} struct {right}");
        }
    }

    #[test]
    fn path_input_distinguishes_open_and_closed_paths() {
        let open = Path::parse("[(-1,2),(3,4)]").unwrap();
        assert!(!open.closed && open.points.len() == 2);
        let closed = Path::parse("((0,0),(1,1))").unwrap();
        assert!(closed.closed && closed.points.len() == 2);
        assert_eq!(Path::parse("[(0,0),]").unwrap_err().sqlstate(), "22P02");
    }
    /// `path_in` reads every spelling `poly_in` does plus the bracketed open
    /// one, and takes its point count from the comma count — so a list with an
    /// even number of commas cannot be whole pairs and is rejected.
    #[test]
    fn path_input_accepts_every_spelling_and_marks_the_bracketed_one_open() {
        use assert2::assert;

        let pair = |closed: bool| Path {
            closed,
            points: vec![Point { x: 1.0, y: 2.0 }, Point { x: 3.0, y: 4.0 }],
        };
        for (spelling, expected) in [
            ("[(1,2),(3,4)]", pair(false)),
            (" [1,2,3, 4] ", pair(false)),
            ("((1,2),(3,4))", pair(true)),
            (" ( ( 1 , 2 ) , ( 3 , 4 ) ) ", pair(true)),
            ("(1,2),(3,4)", pair(true)),
            ("1,2 ,3,4 ", pair(true)),
            ("(1,2,3,4)", pair(true)),
            (
                "((10,20))",
                Path {
                    closed: true,
                    points: vec![Point { x: 10.0, y: 20.0 }],
                },
            ),
            // A single point is a path, open or closed.
            (
                "[(1,2)]",
                Path {
                    closed: false,
                    points: vec![Point { x: 1.0, y: 2.0 }],
                },
            ),
        ] {
            assert!(Path::parse(spelling) == Ok(expected), "{spelling}");
        }
        for bad in [
            "[]",
            "[(,2),(3,4)]",
            "[(1,2),(3,4)",
            "[(0,0),]",
            "[(1,2),(3,4)] x",
        ] {
            let error = Path::parse(bad).expect_err(bad);
            assert!(error.sqlstate() == "22P02", "{bad}");
            assert!(
                error.to_string() == format!("invalid input syntax for type path: \"{bad}\""),
                "{bad}"
            );
        }
    }

    /// `poly_in` takes the same spellings as `path_in` except the bracketed
    /// one, which would mean an *open* polygon.
    #[test]
    fn polygon_input_accepts_poly_ins_spellings() {
        use assert2::assert;

        let pair = Polygon {
            points: vec![Point { x: 0.0, y: 0.0 }, Point { x: 1.0, y: 0.0 }],
        };
        for spelling in [
            "((0,0),(1,0))",
            "(0,0),(1,0)",
            "0,0,1,0",
            "(0,0,1,0)",
            "( ( 0 , 0 ) , ( 1 , 0 ) )",
            "  ((0,0),(1,0))  ",
        ] {
            assert!(Polygon::parse(spelling) == Ok(pair.clone()), "{spelling}");
        }
        // A single point is a polygon; an empty list is not.
        assert!(
            Polygon::parse("((0,0))")
                == Ok(Polygon {
                    points: vec![Point { x: 0.0, y: 0.0 }],
                })
        );
        // NaN and infinite coordinates are values, not errors.
        let wild = Polygon::parse("(NaN,0),(1,Infinity)").expect("wild coordinates");
        assert!(wild.points[0].x.is_nan() && wild.points[1].y.is_infinite());

        for bad in [
            "",
            "()",
            "[(0,0),(1,0)]",
            "(0,0),(1,0),",
            "((0,0),(1,0)",
            "((0,0),(1,0)) x",
            "0,0,1",
            "0.0",
            "(0.0 0.0",
            "(0,1,2)",
            "(0,1,2,3",
            "asdf",
        ] {
            let error = Polygon::parse(bad).expect_err(bad);
            assert!(error.sqlstate() == "22P02", "{bad}");
            assert!(
                error.to_string() == format!("invalid input syntax for type polygon: \"{bad}\""),
                "{bad}"
            );
        }
        // A coordinate overflow is the float's own 22003, not a syntax error.
        assert!(Polygon::parse("(1e400,0),(1,0)").unwrap_err().sqlstate() == "22003");
    }

    /// `<->` over every declared operand pair. Each expected value came from
    /// `PostgreSQL` 18.4, printed at full precision and compared bit for bit —
    /// these are not bounding-box answers, and several disagree with the
    /// obvious formula.
    #[test]
    fn the_distance_matrix_matches_postgres() {
        use assert2::assert;

        let far = point("(5.1,34.5)");
        let cases: [(&str, f64, f64); 16] = [
            (
                "point-point",
                far.distance(point("(-5,-12)")),
                47.584_241_088_831_08,
            ),
            (
                "point-line",
                line("{1,-1,0}").distance_to_point(far),
                20.788_939_366_884_495,
            ),
            (
                "point-lseg",
                lseg("[(1,2),(3,4)]").distance_to_point(far),
                30.572_209_602_840_292,
            ),
            (
                "point-box",
                rect("(2,2),(0,0)").distance_to_point(far),
                32.647_511_390_609_85,
            ),
            (
                "point-path",
                path("[(0,0),(3,0),(4,5),(1,6)]").distance_to_point(far),
                28.793_402_021_991_08,
            ),
            (
                // Not the bounding box's 4.100304866714181.
                "point-circle",
                circle("<(0,0),1>").distance_to_point(point("(3.5,-4.25)")),
                4.505_678_886_386_311,
            ),
            (
                "point-polygon",
                polygon("((2,0),(2,4),(0,0))").distance_to_point(far),
                30.657_136_200_238_924,
            ),
            (
                "lseg-lseg",
                lseg("[(1,2),(3,4)]").distance(lseg("[(11,22),(33,44)]")),
                19.697_715_603_592_208,
            ),
            (
                "lseg-line",
                lseg("[(1,2),(3,4)]").distance_to_line(line("{1,0,5}")),
                6.0,
            ),
            (
                "lseg-box",
                rect("((-8,2),(-2,-10))").distance_to_lseg(lseg("[(1,2),(3,4)]")),
                3.0,
            ),
            ("line-line", line("{0,-1,5}").distance(line("{0,3,0}")), 5.0),
            (
                // `box_distance` is centre-to-centre, not the gap.
                "box-box",
                rect("(2,2),(0,0)").distance(rect("((-8,2),(-2,-10))")),
                7.810_249_675_906_656,
            ),
            (
                "path-path",
                path("[(1,2),(3,4)]")
                    .distance(&path("[(0,0),(3,0),(4,5),(1,6)]"))
                    .expect("both paths have segments"),
                0.784_464_540_552_736,
            ),
            (
                "circle-circle",
                circle("<(5,1),3>").distance(circle("<(100,1),115>")),
                0.0,
            ),
            (
                "circle-polygon",
                circle("<(5,1),3>").distance_to_polygon(&polygon("((1,2),(7,8),(5,6),(3,-4))")),
                0.0,
            ),
            (
                "polygon-polygon",
                polygon("((0,0),(0,10),(10,10),(10,0))")
                    .distance(&polygon("((20,20),(20,30),(30,30))"))
                    .expect("non-empty polygons"),
                14.142_135_623_730_951,
            ),
        ];
        for (name, measured, expected) in cases {
            assert!(
                measured.to_bits() == expected.to_bits(),
                "{name}: {measured}"
            );
        }
    }

    /// `##`, `#` and `?#`. The closest-point operators are NULL-valued where
    /// there is no single answer, and `lseg ## lseg` reports a point on its
    /// *right* operand.
    #[test]
    fn closest_points_and_intersections_match_postgres() {
        use assert2::assert;

        let far = point("(5.1,34.5)");
        let rising = lseg("[(1,2),(3,4)]");
        assert!(rect("(2,2),(0,0)").closest_point_to(far) == Some(point("(2,2)")));
        assert!(line("{1,-1,0}").closest_point_to(far) == Some(point("(19.8,19.8)")));
        assert!(rising.closest_point_to(far) == Some(point("(3,4)")));
        assert!(rising.closest_point_to_lseg(lseg("[(-10,2),(-10,3)]")) == Some(point("(-10,2)")));
        // Parallel segments have no single closest point.
        assert!(rising.closest_point_to_lseg(lseg("[(0,0),(6,6)]")) == None);
        assert!(rect("((-8,2),(-2,-10))").closest_point_to_lseg(rising) == Some(point("(-2,2)")));
        assert!(line("{0,-1,5}").closest_point_to_lseg(rising) == Some(point("(3,4)")));

        assert!(
            lseg("[(0,0),(2,2)]").intersection_point(lseg("[(0,2),(2,0)]")) == Some(point("(1,1)"))
        );
        assert!(lseg("[(0,0),(1,1)]").intersection_point(lseg("[(3,3),(4,4)]")) == None);
        assert!(line("{1,-1,0}").intersection_point(line("{1,1,-2}")) == Some(point("(1,1)")));
        // Identical lines are "parallel": no unique intersection.
        assert!(line("{1,-1,0}").intersection_point(line("{1,-1,0}")) == None);
        assert!(rect("(0,0),(2,2)").intersection(rect("(1,1),(3,3)")) == Some(rect("(2,2),(1,1)")));
        assert!(rect("(0,0),(1,1)").intersection(rect("(3,3),(4,4)")) == None);

        for (name, measured, expected) in [
            (
                "box-box",
                rect("(0,0),(2,2)").overlaps(rect("(1,1),(3,3)")),
                true,
            ),
            (
                "line-box",
                line("{1,-1,0}").intersects_box(rect("(0,0),(2,2)")),
                true,
            ),
            (
                "line-box away",
                line("{1,-1,-100}").intersects_box(rect("(0,0),(2,2)")),
                false,
            ),
            (
                "line-line",
                line("{1,-1,0}").intersects(line("{1,1,0}")),
                true,
            ),
            (
                "line-line parallel",
                line("{1,-1,0}").intersects(line("{1,-1,5}")),
                false,
            ),
            (
                "lseg-box",
                rect("(1,1),(2,2)").intersects_lseg(lseg("[(0,0),(3,3)]")),
                true,
            ),
            (
                "lseg-line",
                lseg("[(0,0),(3,3)]").intersects_line(line("{1,1,-2}")),
                true,
            ),
            (
                "lseg-lseg",
                lseg("[(0,0),(2,2)]").intersects(lseg("[(0,2),(2,0)]")),
                true,
            ),
            (
                "path-path",
                path("[(0,0),(2,2)]").intersects(&path("[(0,2),(2,0)]")),
                true,
            ),
            (
                "path-path apart",
                path("[(0,0),(2,2)]").intersects(&path("[(5,5),(6,6)]")),
                false,
            ),
        ] {
            assert!(measured == expected, "{name}");
        }
    }

    /// `<@` and `@>` in every direction they are declared.
    #[test]
    fn containment_matches_postgres() {
        use assert2::assert;

        let square = polygon("((0,0),(0,10),(10,10),(10,0))");
        for (name, measured, expected) in [
            (
                "point in box",
                rect("(0,0),(2,2)").contains_point(point("(1,1)")),
                true,
            ),
            (
                "point on box edge",
                rect("(0,0),(2,2)").contains_point(point("(0,1)")),
                true,
            ),
            (
                "point out of box",
                rect("(0,0),(2,2)").contains_point(point("(3,1)")),
                false,
            ),
            (
                "point in circle",
                circle("<(0,0),3>").contains_point(point("(1,1)")),
                true,
            ),
            (
                "point on line",
                line("{1,-1,0}").contains_point(point("(4,4)")),
                true,
            ),
            (
                "point on lseg",
                lseg("[(0,0),(3,3)]").contains_point(point("(2,2)")),
                true,
            ),
            (
                "point on open path",
                path("[(0,0),(3,3)]").contains_point(point("(2,2)")),
                true,
            ),
            (
                "point in closed path",
                path("((0,0),(0,10),(10,10),(10,0))").contains_point(point("(1,1)")),
                true,
            ),
            (
                "point in polygon",
                square.contains_point(point("(5,5)")),
                true,
            ),
            (
                "point on polygon edge",
                square.contains_point(point("(0,5)")),
                true,
            ),
            (
                "point out of polygon",
                square.contains_point(point("(11,5)")),
                false,
            ),
            (
                "lseg in box",
                rect("(0,0),(2,2)").contains_lseg(lseg("[(0,0),(1,1)]")),
                true,
            ),
            (
                "lseg on line",
                line("{1,-1,0}").contains_lseg(lseg("[(0,0),(1,1)]")),
                true,
            ),
            (
                "box in box",
                rect("(0,0),(4,4)").contains(rect("(1,1),(2,2)")),
                true,
            ),
            (
                "box not in box",
                rect("(1,1),(2,2)").contains(rect("(0,0),(4,4)")),
                false,
            ),
            (
                "circle in circle",
                circle("<(0,0),3>").contains(circle("<(0,0),1>")),
                true,
            ),
            (
                "polygon in polygon",
                square.contains_polygon(&polygon("((2,2),(2,8),(8,8),(8,2))")),
                true,
            ),
            (
                "polygon not in polygon",
                polygon("((2,2),(2,8),(8,8),(8,2))").contains_polygon(&square),
                false,
            ),
            (
                "polygon overlap",
                square.overlaps(&polygon("((5,5),(5,20),(20,20))")),
                true,
            ),
        ] {
            assert!(measured == expected, "{name}");
        }
    }

    /// `?-`, `?|`, `?-|`, `?||`, `<^` and `>^`. The last two are the trap: on
    /// *points* they are `point_below`/`point_above` and strict, while on
    /// *boxes* they compare opposite edges, so a box is neither below-or-equal
    /// nor above-or-equal itself.
    #[test]
    fn predicates_match_postgres() {
        use assert2::assert;

        let unit = rect("(0,0),(2,2)");
        for (name, measured, expected) in [
            (
                "points horizontal",
                point("(3,4)").is_horizontal_with(point("(9,4)")),
                true,
            ),
            (
                "points vertical",
                point("(3,4)").is_vertical_with(point("(3,9)")),
                true,
            ),
            ("line horizontal", line("{0,-1,5}").is_horizontal(), true),
            ("line vertical", line("{1,0,5}").is_vertical(), true),
            (
                "lseg horizontal",
                lseg("[(0,-20),(30,-20)]").is_horizontal(),
                true,
            ),
            (
                "lseg vertical",
                lseg("[(-10,2),(-10,3)]").is_vertical(),
                true,
            ),
            (
                "lines perpendicular",
                line("{1,-1,0}").is_perpendicular_to(line("{1,1,0}")),
                true,
            ),
            (
                "lines parallel",
                line("{1,-1,0}").is_parallel_to(line("{1,-1,5}")),
                true,
            ),
            (
                "identical lines parallel",
                line("{1,-1,0}").is_parallel_to(line("{1,-1,0}")),
                true,
            ),
            (
                "lsegs perpendicular",
                lseg("[(0,0),(1,1)]").is_perpendicular_to(lseg("[(0,0),(1,-1)]")),
                true,
            ),
            (
                "lsegs parallel",
                lseg("[(0,0),(1,1)]").is_parallel_to(lseg("[(5,5),(6,6)]")),
                true,
            ),
            (
                "point <^ level",
                point("(1,2)").is_below(point("(3,2)")),
                false,
            ),
            (
                "point <^ lower",
                point("(1,1)").is_below(point("(3,2)")),
                true,
            ),
            (
                "point >^ level",
                point("(1,2)").is_above(point("(3,2)")),
                false,
            ),
            (
                "point >^ higher",
                point("(1,3)").is_above(point("(3,2)")),
                true,
            ),
            ("box <^ itself", unit.below_or_equal(unit), false),
            (
                "box <^ stacked",
                unit.below_or_equal(rect("(0,2),(2,5)")),
                true,
            ),
            ("box >^ itself", unit.above_or_equal(unit), false),
            (
                "box >^ stacked",
                rect("(0,2),(2,5)").above_or_equal(unit),
                true,
            ),
        ] {
            assert!(measured == expected, "{name}");
        }
    }

    /// The polygon positional operators read the bounding box, but with *bare*
    /// comparisons — unlike the identically spelled box operators, which carry
    /// `EPSILON`. A half-micron gap is therefore enough to separate two
    /// polygons and not enough to separate the same two boxes.
    #[test]
    fn polygon_positional_operators_skip_the_epsilon() {
        use assert2::assert;

        let left = polygon("((0,0),(1,1))");
        let right = polygon("((1.0000005,0),(2,2))");
        assert!(left.strictly_left_of(&right));
        assert!(!rect("(0,0),(1,1)").strictly_left_of(rect("(1.0000005,0),(2,2)")));
        assert!(right.strictly_right_of(&left));

        let low = polygon("((0,0),(1,1))");
        let high = polygon("((0,5),(1,6))");
        for (name, measured, expected) in [
            ("below", low.strictly_below(&high), true),
            ("above", high.strictly_above(&low), true),
            ("overleft", low.does_not_extend_right(&high), true),
            ("overright", low.does_not_extend_left(&high), true),
            ("overbelow", low.does_not_extend_above(&high), true),
            ("overabove", high.does_not_extend_below(&low), true),
            (
                // `~=` ignores both rotation and direction.
                "same reversed",
                polygon("((1,2),(3,4),(5,6),(7,8))").same(&polygon("((7,8),(5,6),(3,4),(1,2))")),
                true,
            ),
            (
                "same rotated",
                polygon("((1,2),(3,4),(5,6),(7,8))").same(&polygon("((5,6),(7,8),(1,2),(3,4))")),
                true,
            ),
            ("not same", low.same(&high), false),
        ] {
            assert!(measured == expected, "{name}");
        }
    }

    /// `poly_center` is the mean of the *vertices*, not the centre of the
    /// bounding box. The two agree on a rectangle, which is why the difference
    /// has to be checked on something else.
    #[test]
    fn polygon_center_is_the_vertex_mean_not_the_bounding_box() {
        use assert2::assert;

        let wedge = polygon("((0,0),(10,0),(1,1))");
        assert!(wedge.center() == point("(3.6666666666666665,0.3333333333333333)"));
        assert!(wedge.bounding_box().center() == point("(5,0.5)"));
        assert!(wedge.to_circle().center == wedge.center());
        assert!(
            wedge.to_circle().radius.to_bits() == 4.257_541_095_429_226_f64.to_bits(),
            "{}",
            wedge.to_circle().radius
        );
        // On a rectangle the two coincide, which is the trap.
        let square = polygon("((0,0),(2,0),(2,2),(0,2))");
        assert!(square.center() == square.bounding_box().center());
        assert!(square.npoints() == 4);
    }

    /// Derived quantities and the pure conversions.
    #[test]
    fn accessors_and_conversions_match_postgres() {
        use assert2::assert;

        for (name, measured, expected) in [
            (
                "closed path length",
                path("((1,2),(4,6),(0,0))").length(),
                14.447_170_528_427_769,
            ),
            (
                "open path length",
                path("[(1,2),(4,6),(0,0)]").length(),
                12.211_102_550_927_98,
            ),
            (
                "closed path area",
                path("((0,0),(2,0),(2,2),(0,2))").area().expect("closed"),
                4.0,
            ),
            // The grouping in `circle_ar` is load-bearing at this radius.
            (
                "circle area",
                circle("<(100,1),115>").area(),
                41_547.562_843_725_01,
            ),
            ("circle diameter", circle("<(1,2),3>").diameter(), 6.0),
            ("lseg length", lseg("[(1,2),(4,6)]").length(), 5.0),
            ("slope", point("(1,2)").slope(point("(3,7)")), 2.5),
            (
                "slope vertical",
                point("(1,2)").slope(point("(1,7)")),
                f64::INFINITY,
            ),
        ] {
            assert!(
                measured.to_bits() == expected.to_bits(),
                "{name}: {measured}"
            );
        }
        // An open path has no area at all, which is NULL upstream.
        assert!(path("[(0,0),(2,0),(2,2)]").area() == None);
        assert!(path("[(1,2),(3,4)]").npoints() == 2);
        assert!(!path("[(1,2),(3,4)]").is_closed() && path("[(1,2),(3,4)]").is_open());
        assert!(path("[(1,2),(3,4)]").to_closed().is_closed());
        assert!(path("((1,2),(3,4))").to_open().is_open());

        assert!(lseg("[(1,2),(4,6)]").center() == point("(2.5,4)"));
        assert!(rect("(0,0),(2,4)").center() == point("(1,2)"));
        assert!(rect("(0,0),(2,4)").diagonal() == lseg("[(2,4),(0,0)]"));
        assert!(rect("(0,0),(1,1)").bound_box(rect("(3,-2),(4,4)")) == rect("(4,4),(0,-2)"));
        assert!(rect("(0,0),(2,4)").to_circle() == circle("<(1,2),2.23606797749979>"));
        // `box(circle)` is the *inscribed* box at r/√2, not the bounding box.
        assert!(
            circle("<(1,2),3>").to_box()
                == rect(
                    "(3.1213203435596424,4.121320343559642),(-1.1213203435596424,-0.12132034355964239)"
                )
        );
        assert!(circle("<(1,2),3>").to_point() == point("(1,2)"));
        assert!(point("(1,2)").circle_with(3.0) == circle("<(1,2),3>"));
        assert!(point("(1,2)").to_box() == rect("(1,2),(1,2)"));
        assert!(point("(1,2)").box_with(point("(3,0)")) == rect("(3,2),(1,0)"));
        assert!(point("(1,2)").lseg_with(point("(3,0)")) == lseg("[(1,2),(3,0)]"));
        assert!(point("(1,2)").line_with(point("(3,0)")) == Ok(line("{-1,-1,3}")));
        // The constructor's SQLSTATE is 22023 where `line_in`'s is 22P02.
        let coincident = point("(1,2)").line_with(point("(1,2)")).expect_err("equal");
        assert!(coincident.sqlstate() == "22023");
        assert!(
            coincident.to_string() == "invalid line specification: must be two distinct points"
        );

        assert!(rect("(0,0),(1,1)").to_polygon() == polygon("((0,0),(0,1),(1,1),(1,0))"));
        assert!(polygon("((0,0),(2,0),(2,2))").to_box() == rect("(2,2),(0,0)"));
        assert!(polygon("((0,0),(2,0),(2,2))").to_path() == path("((0,0),(2,0),(2,2))"));
        assert!(path("((0,0),(2,0),(2,2))").to_polygon() == Ok(polygon("((0,0),(2,0),(2,2))")));
        let open = path("[(0,0),(1,1)]").to_polygon().expect_err("open path");
        assert!(open.sqlstate() == "22023");
        assert!(open.to_string() == "open path cannot be converted to polygon");
    }

    /// `polygon(npts, circle)` walks clockwise from `(cx − r, cy)`, and refuses
    /// a zero radius or fewer than two points.
    #[test]
    fn circle_to_polygon_matches_postgres() {
        use assert2::assert;

        assert!(
            circle("<(0,0),3>").to_polygon(5)
                == Ok(polygon(
                    "((-3,0),(-0.9270509831248424,2.8531695488854605),\
                     (2.427050983124842,1.7633557568774196),\
                     (2.427050983124843,-1.7633557568774192),\
                     (-0.9270509831248417,-2.853169548885461))"
                ))
        );
        let zero = circle("<(3,5),0>").to_polygon(12).expect_err("zero radius");
        assert!(zero.sqlstate() == "0A000");
        assert!(zero.to_string() == "cannot convert circle with radius zero to polygon");
        let sparse = circle("<(0,0),3>").to_polygon(1).expect_err("one point");
        assert!(sparse.sqlstate() == "22023");
        assert!(sparse.to_string() == "must request at least 2 points");
    }

    /// Every geometric `<`/`<=`/`>=`/`>` compares a magnitude, never the shape:
    /// area for `box` and `circle`, length for `lseg`, and the bare point count
    /// for `path`.
    #[test]
    fn ordering_compares_magnitudes_rather_than_shapes() {
        use std::cmp::Ordering;

        use assert2::assert;

        // Equal areas anywhere on the plane are `=`.
        assert!(rect("(1,1),(3,3)").compare(rect("(10,10),(12,12)")) == Some(Ordering::Equal));
        assert!(rect("(1,1),(3,3)").compare(rect("(0,0),(2,2)")) == Some(Ordering::Equal));
        assert!(rect("(0,0),(1,1)").compare(rect("(0,0),(2,2)")) == Some(Ordering::Less));
        assert!(rect("(0,0),(1,1)").same(rect("(0,0),(2,2)")) == false);
        // Equal lengths anywhere are `=`, and `=` on lseg is structural.
        assert!(lseg("[(0,0),(1,0)]").compare(lseg("[(5,5),(6,5)]")) == Some(Ordering::Equal));
        assert!(lseg("[(0,0),(1,0)]").eq_lseg(lseg("[(5,5),(6,5)]")) == false);
        assert!(lseg("[(0,0),(1,0)]").compare(lseg("[(0,0),(2,0)]")) == Some(Ordering::Less));
        // Paths compare on point count alone, open or closed.
        assert!(
            path("((0,0),(1,0),(2,2))").compare(&path("((0,0),(1,0),(2,2),(3,3))"))
                == Ordering::Less
        );
        assert!(
            path("[(0,0),(1,0),(2,2)]").compare(&path("((9,9),(8,8),(7,7))")) == Ordering::Equal
        );
        // A NaN magnitude leaves every operator false, which is no ordering.
        assert!(circle("<(3,5),NaN>").compare(circle("<(0,0),1>")) == None);
        assert!(circle("<(3,5),NaN>").ne_circle(circle("<(0,0),1>")) == false);
        assert!(circle("<(0,0),1>").ne_circle(circle("<(0,0),2>")));
    }

    /// The arithmetic operators, including the range checks `CHECKFLOATVAL`
    /// imposes: `*` and `/` are complex, so they rotate as well as scale.
    #[test]
    fn arithmetic_is_complex_and_range_checked() {
        use assert2::assert;

        assert!(point("(1,2)").add_point(point("(3,4)")) == Ok(point("(4,6)")));
        assert!(point("(1,2)").sub_point(point("(3,4)")) == Ok(point("(-2,-2)")));
        assert!(point("(1,2)").mul_point(point("(3,4)")) == Ok(point("(-5,10)")));
        assert!(point("(5.1,34.5)").mul_point(point("(10,10)")) == Ok(point("(-294,396)")));
        assert!(point("(1,2)").div_point(point("(3,4)")) == Ok(point("(0.44,0.08)")));
        assert!(
            point("(-5,-12)").div_point(point("(5.1,34.5)"))
                == Ok(point("(-0.36135365793498103,0.09151003897193036)"))
        );

        for (name, error, sqlstate) in [
            (
                "overflow",
                point("(1e300,1e300)").mul_point(point("(1e300,1e300)")),
                "22003",
            ),
            (
                "underflow",
                point("(1e-300,1e-300)").mul_point(point("(1e-300,1e-300)")),
                "22003",
            ),
            (
                "divide by the origin",
                point("(1,2)").div_point(point("(0,0)")),
                "22012",
            ),
            (
                "divisor underflows the modulus",
                point("(1e300,1e300)").div_point(point("(1e-300,1e-300)")),
                "22003",
            ),
        ] {
            assert!(error.clone().unwrap_err().sqlstate() == sqlstate, "{name}");
        }
        assert!(
            point("(1e300,1e300)")
                .mul_point(point("(1e300,1e300)"))
                .unwrap_err()
                .to_string()
                == "value out of range: overflow"
        );
        assert!(
            point("(1e-300,1e-300)")
                .mul_point(point("(1e-300,1e-300)"))
                .unwrap_err()
                .to_string()
                == "value out of range: underflow"
        );

        // Translation keeps the corners in place; scaling renormalizes them.
        assert!(rect("(1,1),(3,3)").add_point(point("(2,1)")) == Ok(rect("(5,4),(3,2)")));
        assert!(rect("(1,1),(3,3)").mul_point(point("(2,1)")) == Ok(rect("(3,9),(1,3)")));
        assert!(rect("(1,1),(3,3)").div_point(point("(2,1)")) == Ok(rect("(1.8,0.6),(0.6,0.2)")));
        assert!(
            circle("<(5,1),3>").mul_point(point("(2,1)"))
                == Ok(circle("<(9,7),6.708203932499369>"))
        );
        assert!(
            circle("<(5,1),3>").div_point(point("(2,1)"))
                == Ok(circle("<(2.2,-0.6),1.3416407864998738>"))
        );
        assert!(path("[(1,2),(3,4)]").mul_point(point("(2,1)")) == Ok(path("[(0,5),(2,11)]")));
        assert!(path("[(1,2),(3,4)]").div_point(point("(2,1)")) == Ok(path("[(0.8,0.6),(2,1)]")));

        // `path + path` concatenates, and is NULL if either side is closed.
        assert!(
            path("[(0,0),(1,0)]").concat(&path("[(2,2),(3,3)]"))
                == Some(path("[(0,0),(1,0),(2,2),(3,3)]"))
        );
        assert!(path("((0,0),(1,0),(1,1))").concat(&path("((2,2),(3,3))")) == None);
        assert!(path("[(0,0),(1,0)]").concat(&path("((2,2),(3,3))")) == None);
    }

    /// The polygon and path loops are O(n·m) in the vertex counts, as upstream.
    /// A pair of thousand-vertex polygons is a million edge pairs, which is
    /// fast — but a per-edge allocation or an extra factor of n would not be,
    /// so this doubles as a guard against either creeping back in.
    #[test]
    fn polygon_loops_stay_within_postgres_complexity() {
        use assert2::assert;

        let ring = |radius: f64, count: usize| Polygon {
            points: (0..count)
                .map(|index| {
                    #[expect(
                        clippy::cast_precision_loss,
                        reason = "index is far below 2^53; this is test scaffolding"
                    )]
                    let angle = (index as f64) * std::f64::consts::TAU / (count as f64);
                    Point {
                        x: radius * angle.cos(),
                        y: radius * angle.sin(),
                    }
                })
                .collect(),
        };
        let inner = ring(1.0, 1000);
        let outer = ring(100.0, 1000);
        let distant = Polygon {
            points: outer
                .points
                .iter()
                .map(|vertex| Point {
                    x: vertex.x + 10_000.0,
                    y: vertex.y,
                })
                .collect(),
        };

        let start = std::time::Instant::now();
        assert!(outer.contains_polygon(&inner));
        assert!(!inner.contains_polygon(&outer));
        assert!(outer.distance(&distant).expect("non-empty") > 0.0);
        assert!(outer.overlaps(&inner));
        let elapsed = start.elapsed();
        assert!(
            elapsed < std::time::Duration::from_secs(20),
            "million-edge-pair work took {elapsed:?}"
        );
    }

    fn point(text: &str) -> Point {
        Point::parse(text).expect(text)
    }

    fn lseg(text: &str) -> Lseg {
        Lseg::parse(text).expect(text)
    }

    fn line(text: &str) -> Line {
        Line::parse(text).expect(text)
    }

    fn rect(text: &str) -> Box2 {
        Box2::parse(text).expect(text)
    }

    fn circle(text: &str) -> Circle {
        Circle::parse(text).expect(text)
    }

    fn path(text: &str) -> Path {
        Path::parse(text).expect(text)
    }

    fn polygon(text: &str) -> Polygon {
        Polygon::parse(text).expect(text)
    }
}
