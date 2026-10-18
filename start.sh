#!/usr/bin/env bash
# start.sh — coffee_shop_oss scenario bootstrap
#
# Coffee shop demo on open-source Apache Flink. Docker Compose only, with the LLM
# step running on Flink's native AI functions (CREATE MODEL + ML_PREDICT).
#
# Usage:
#   cd scenarios/coffee_shop_oss
#   cp .env.example .env    # then fill in OPENAI_API_KEY
#   ./start.sh                   bring everything up and submit the jobs
#   ./start.sh --services-only   bring the stack up, submit nothing
#   ./start.sh --reset           wipe every volume first (combines with --services-only)
#
# Without a key the sales pipeline still runs; only the feedback job is skipped.

set -euo pipefail
cd "$(dirname "$0")"

# ── Options ────────────────────────────────────────────────────────────────────

SERVICES_ONLY=0
RESET=0

usage() {
  cat <<'USAGE'
Usage: ./start.sh [--services-only] [--reset]

  (no options)      Start the stack and submit the Flink SQL jobs.
  --services-only   Start the stack (containers, databases, Flink cluster, JARs)
                    but submit nothing. Use this to walk through the lab by hand,
                    or to demo submission live. The manual steps are printed at
                    the end either way.
  --reset           Wipe every volume first (docker compose down -v): PostgreSQL
                    data, the catalog store, Kafka topics and running jobs are all
                    gone, so the lab starts from nothing. Combine with
                    --services-only for a clean, empty environment.
  -h, --help        Show this message.
USAGE
}

while [ $# -gt 0 ]; do
  case "$1" in
    --services-only) SERVICES_ONLY=1; shift ;;
    --reset)         RESET=1; shift ;;
    -h|--help)       usage; exit 0 ;;
    *)               echo "ERROR: unknown option '$1'" >&2; echo >&2; usage >&2; exit 2 ;;
  esac
done

# ── Prerequisite checks ────────────────────────────────────────────────────────

require_cmd() { command -v "$1" &>/dev/null || { echo "ERROR: '$1' not found"; exit 1; }; }
require_cmd docker
require_cmd curl
require_cmd jq

# ── The manual runbook ─────────────────────────────────────────────────────────
# Printed at the end of every run: after --services-only it is the list of what to
# do next, and after a normal run it is how to redo any single step by hand.

print_manual_steps() {
  cat <<'STEPS'

  ── Run the pipeline step by step ────────────────────────────────────────────

  Every SQL Client session needs the DDL: Flink's default catalog is in-memory and
  per-session, so a table created in one session does not exist in the next.
  submit.sh handles that with -i; interactive sessions need it rendered first.
  (The one exception is the `dwh` catalog — a file-based CatalogStore persists
  catalog registrations, though not the tables inside the default catalog.)

  1. Submit job 1 — sales aggregation  (store.purchases -> purchase_report)

       docker compose exec sql-client /opt/submit.sh sales

  2. Submit job 2 — AI feedback summaries  (store.feedback -> ML_PREDICT -> Postgres)
     Needs OPENAI_API_KEY in .env. If you edit .env after the stack is up, the
     sql-client container must be re-created to pick the new value up:

       docker compose up -d --force-recreate sql-client
       docker compose exec sql-client /opt/submit.sh feedback

     Both jobs at once:  docker compose exec sql-client /opt/submit.sh

  3. Run queries in the browser: http://localhost:8084 (web SQL editor). The browser
     keeps one SQL Gateway session across reloads; run the DDL files from the
     examples menu first, then the queries from flink_sql/explore.sql.

     Or open an interactive SQL Client (renders the DDL, then loads it with -i):

       docker compose exec sql-client sh -c \
         'for f in /opt/sql/ddl/*.sql; do envsubst < $f; done > /tmp/init.sql'
       docker compose exec sql-client /opt/flink/bin/sql-client.sh -i /tmp/init.sql

     Then paste the queries from flink_sql/explore.sql one at a time — raw stream,
     enrichment join, TUMBLE, HOP, an ad hoc ML_PREDICT call, and the CDC table.
     Press q to leave a streaming result grid.

  4. Watch the events arrive:

       docker compose exec redpanda rpk topic consume store.purchases --num 3
       docker compose exec redpanda rpk topic consume store.feedback  --num 3

  5. Confirm the results landed:

       docker compose exec postgres psql -U root -d sales_report \
         -c 'SELECT count(*) FROM purchase_report;'

       docker compose exec postgres psql -U root -d sales_report \
         -c 'SELECT review_id, overall_score, summary, improvement_1
               FROM feedback_summary ORDER BY processed_at DESC LIMIT 5;'

  6. Inspect the running jobs:

       curl -s http://localhost:8081/jobs/overview | jq -r '.jobs[] | "\(.state) \(.name)"'

     ...or open http://localhost:8081 and http://localhost:3000 (Grafana).

  7. Stop a job without touching the rest (job 2 is the one that costs money).
     Note /jobs/overview calls the id `jid`, not `id` — `.id` there is null:

       curl -s http://localhost:8081/jobs/overview \
         | jq -r '.jobs[] | select(.name | contains("coffee-2")) | .jid' \
         | xargs -I{} curl -s -X PATCH "http://localhost:8081/jobs/{}"

STEPS
}

# .env is read by docker compose itself; sourced here only to decide which jobs to
# submit. Key never leaves this shell — submit.sh reads it from the container env.
if [ -f .env ]; then
  set -a; . ./.env; set +a
fi

JOBS="sales feedback"
if [ -z "${OPENAI_API_KEY:-}" ] || [ "${OPENAI_API_KEY}" = "sk-replace-me" ]; then
  JOBS="sales"
fi

# ── Step 1: Download connector JARs ───────────────────────────────────────────

if [ ! -d lib ] || [ -z "$(ls -A lib/*.jar 2>/dev/null)" ]; then
  echo "==> Downloading connector JARs..."
  bash download-jars.sh
else
  echo "==> [skip] lib/ already populated ($(ls lib/*.jar 2>/dev/null | wc -l | tr -d ' ') JARs)"
fi

# ── Step 2: Start Docker Compose services ─────────────────────────────────────

if [ "$RESET" = "1" ]; then
  echo ""
  echo "==> --reset: removing containers and every volume..."
  docker compose down -v --remove-orphans
fi

echo ""
echo "==> Starting Docker Compose services..."
docker compose up -d --remove-orphans

echo ""
echo "==> Waiting for core services to be healthy..."

wait_healthy() {
  local service="$1"
  local max_attempts="${2:-30}"
  local attempt=0
  echo -n "    $service "
  until docker compose ps "$service" 2>/dev/null | grep -q "healthy\|running"; do
    attempt=$((attempt + 1))
    if [ $attempt -ge $max_attempts ]; then
      echo " TIMEOUT"
      echo "ERROR: $service did not become healthy within $(( max_attempts * 5 ))s"
      docker compose logs "$service" --tail=20
      exit 1
    fi
    echo -n "."
    sleep 5
  done
  echo " OK"
}

wait_healthy redpanda 20
wait_healthy postgres 12
wait_healthy jobmanager 24
wait_healthy sql-gateway 24

echo ""
echo "==> All core services healthy."

# ── Step 3: Copy JARs into Flink lib (SQL Client needs them on its classpath) ──

echo ""
echo "==> Installing connector JARs into Flink containers..."
for jar in lib/*.jar; do
  [ -f "$jar" ] || continue
  filename=$(basename "$jar")
  for svc in jobmanager taskmanager sql-client; do
    docker compose exec -T "$svc" \
      sh -c "[ -f /opt/flink/lib/$filename ] || cp /opt/flink/lib/extra/$filename /opt/flink/lib/" 2>/dev/null || true
  done
done
echo "    JARs installed."

# ── --services-only stops here ────────────────────────────────────────────────
# The stack is fully up: databases seeded, topics created, generators producing,
# Flink cluster ready with the connector and model-provider JARs on its classpath.
# Nothing has been submitted, so the Flink UI shows an empty job list.

if [ "$SERVICES_ONLY" = "1" ]; then
  echo ""
  echo "══════════════════════════════════════════════════════════════"
  echo "  Coffee Shop OSS — services running, no jobs submitted"
  echo "══════════════════════════════════════════════════════════════"
  echo ""
  echo "  Environment homepage →  http://localhost        (start here; links to everything)"
  echo "  Flink Web UI        →  http://localhost:8081   (no jobs yet)"
  echo "  Redpanda Console    →  http://localhost:8082"
  echo "  Grafana             →  http://localhost:3000   (dashboards empty until a job runs)"
  echo "  SQL Editor          →  http://localhost:8084   (Flink SQL in the browser)"
  echo "  Pipeline Builder    →  http://localhost:8084/pipeline   (guided, click-through)"
  echo ""
  echo "  Kafka (from host):      localhost:19092"
  echo "  PostgreSQL (from host): localhost:5432  root/admin1   dbs sales_report, warehouse"
  if [ "$JOBS" = "sales" ]; then
    echo ""
    echo "  NOTE: OPENAI_API_KEY is not set in .env — step 2 below will refuse to run"
    echo "        until it is. Step 1 works regardless."
  fi
  print_manual_steps
  echo "  Stop everything:  docker compose down -v"
  echo "══════════════════════════════════════════════════════════════"
  exit 0
fi

# ── Step 4: Submit the Flink SQL jobs ─────────────────────────────────────────
#
# submit.sh runs inside the sql-client container: it renders the DDL and job files
# with envsubst (so the API key is never written to a file on disk), loads all DDL
# into each session with -i, and submits each job with -f.

echo ""
echo "==> Submitting Flink SQL jobs: $JOBS"
if [ "$JOBS" = "sales" ]; then
  echo "    NOTE: OPENAI_API_KEY is not set, so the feedback job (ML_PREDICT) is"
  echo "          skipped. Set it in .env, then:"
  echo "            docker compose up -d --force-recreate sql-client"
  echo "            docker compose exec sql-client /opt/submit.sh feedback"
fi
echo ""
# shellcheck disable=SC2086 — word splitting is intended, JOBS is a job-name list
docker compose exec -T sql-client /opt/submit.sh $JOBS || {
  echo ""
  echo "WARNING: at least one job failed to submit — see the output above."
}

echo ""
echo "==> Verifying..."
sleep 5
EXPECTED=$(echo $JOBS | wc -w | tr -d ' ')
RUNNING_JOBS=$(curl -s http://localhost:8081/jobs | jq -r '.jobs[] | select(.status == "RUNNING") | .id' | wc -l | tr -d ' ')
echo "    Running Flink jobs: $RUNNING_JOBS  (expected $EXPECTED)"

# ── Step 5: Status summary ────────────────────────────────────────────────────

echo ""
echo "══════════════════════════════════════════════════════════════"
echo "  Coffee Shop OSS — Setup Complete"
echo "══════════════════════════════════════════════════════════════"
echo ""
echo "  Services:"
echo "    Environment homepage →  http://localhost       (start here; links to everything)"
echo "    Flink Web UI        →  http://localhost:8081"
echo "    Redpanda Console    →  http://localhost:8082"
echo "    Grafana             →  http://localhost:3000  (anonymous admin)"
echo "    SQL Editor          →  http://localhost:8084  (Flink SQL in the browser)"
echo "    Pipeline Builder    →  http://localhost:8084/pipeline  (guided, click-through)"
echo ""
echo "  Kafka (from host):      localhost:19092"
echo "  PostgreSQL (from host): localhost:5432  root/admin1  dbs sales_report, warehouse"
echo ""
echo "  Start again without submitting anything:"
echo "    ./start.sh --reset --services-only"
print_manual_steps
echo "  Stop everything:  docker compose down -v"
echo "══════════════════════════════════════════════════════════════"
