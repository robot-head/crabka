# Traces — Grafana screenshots

Grafana Explore running TraceQL against Crabka's traces backend through the
Tempo-compatible HTTP API. Each image is a 2880×1800 (1440×900 @ 2× DPI) capture
of a distinct query class.

## Attribute selector

`{ resource.service.name = "checkout-frontend" }`

Resource-scoped attribute match returned as a trace list.

![Attribute selector](01-attribute-selector.png)

## Error intrinsic

`{ span:status = error }`

Filtering on the `span:status` intrinsic.

![Error intrinsic](02-error-intrinsic.png)

## Duration intrinsic

`{ span:duration > 100ms }`

Numeric comparison on the `span:duration` intrinsic with a duration literal.

![Duration intrinsic](03-duration-intrinsic.png)

## Structural — descendant

`{ .http.method = "GET" } >> { .db.system = "postgresql" }`

The `>>` descendant operator joining two spansets, returning traces where a
`GET` span has a PostgreSQL span somewhere beneath it.

![Structural descendant](04-structural-descendant.png)

## Boolean composition

`{ .http.method = "POST" && span:status = error }`

A single spanset combining an attribute predicate and an intrinsic with `&&`.

![Boolean AND](05-boolean-and.png)

## Trace waterfall

The TraceByID detail view — Grafana fetches the full trace over the Tempo API
and renders the span tree with timing bars.

![Trace waterfall](06-trace-waterfall.png)
