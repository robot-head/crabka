#!/bin/sh
# Continuous SQL workload for the demo's gres.
#
# A gres with no clients emits no spans, so the trace waterfall this demo
# exists to show would be empty on a fresh `docker compose up`. This loop keeps
# a modest, steady stream of statements running: a write, an aggregate, a point
# read, a grouped read, and a periodic prune that keeps the table bounded.
#
# The last statement of each pass carries a client-supplied W3C traceparent in
# a trailing sqlcommenter comment. gres adopts it as the parent of its own
# spans, which is what an OTel-instrumented Postgres driver does automatically
# — the loop just does it by hand so the demo always contains an example.
#
# Runs on the stock postgres image purely for its psql; every other container
# here runs the all-in-one Crabka image, which carries no SQL client.
set -eu

CONN="${CRABKA_GRES_WORKLOAD_CONNINFO:-host=gres port=5433 user=demo dbname=demo}"
INTERVAL="${CRABKA_GRES_WORKLOAD_INTERVAL:-5}"
# Rows kept before the prune starts trimming the tail.
RETAIN="${CRABKA_GRES_WORKLOAD_RETAIN:-500}"

# gres deliberately has no healthcheck (see docker-compose.yml), and an
# accepted connection does not mean it has replayed its WAL yet. Wait on a
# statement round trip, not on the listener.
until psql "$CONN" -tAc 'SELECT 1' >/dev/null 2>&1; do
  echo "gres-workload: waiting for gres (${CONN})"
  sleep 2
done
echo "gres-workload: gres is answering queries"

psql "$CONN" -v ON_ERROR_STOP=1 -tAc \
  'CREATE TABLE IF NOT EXISTS demo_orders (id BIGINT PRIMARY KEY, region TEXT NOT NULL, amount_cents BIGINT NOT NULL)' \
  >/dev/null

# Hex of the requested byte count, for the two halves of a traceparent.
random_hex() {
  od -An -N"$1" -tx1 /dev/urandom | tr -d ' \n'
}

# Seed from the wall clock so a restart does not collide with the primary keys
# already in the table.
id=$(date +%s)

while true; do
  id=$((id + 1))
  amount=$(((id * 37) % 9900 + 100))
  case $((id % 3)) in
    0) region=us-east ;;
    1) region=us-west ;;
    *) region=eu-central ;;
  esac

  # Statements are individually tolerant of failure: a restarting gres should
  # pause the workload, not kill it.
  psql "$CONN" -tAc \
    "INSERT INTO demo_orders VALUES ($id, '$region', $amount)" >/dev/null || true
  psql "$CONN" -tAc \
    "SELECT count(*) FROM demo_orders" >/dev/null || true
  psql "$CONN" -tAc \
    "SELECT id, region, amount_cents FROM demo_orders WHERE id = $id" >/dev/null || true

  if [ $((id % 25)) -eq 0 ]; then
    psql "$CONN" -tAc \
      "DELETE FROM demo_orders WHERE id < $((id - RETAIN))" >/dev/null || true
  fi

  # One client-parented trace per pass. `-01` marks it sampled; gres re-derives
  # the sampling decision from the trace id at its own ratio, so a client
  # cannot force export by flipping this bit.
  traceparent="00-$(random_hex 16)-$(random_hex 8)-01"
  psql "$CONN" -tAc \
    "SELECT region, count(*) FROM demo_orders GROUP BY region /*traceparent='$traceparent'*/" \
    >/dev/null || true

  [ "$INTERVAL" = "0" ] || sleep "$INTERVAL"
done
