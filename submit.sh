#!/usr/bin/env bash
# Submits the pipeline as TWO separate Flink jobs, sharing one set of DDL.
# Runs INSIDE the sql-client container, where it is mounted at /opt/submit.sh:
#
#   docker compose exec sql-client /opt/submit.sh            submit both, in order
#   docker compose exec sql-client /opt/submit.sh feedback   submit one, by name
#
# Why two jobs rather than one STATEMENT SET
# ------------------------------------------
# The feedback job is the one that calls a model: it costs money per row and depends on
# somebody else's endpoint being up. Split, it can be stopped, restarted or re-pointed
# while the sales dashboard keeps updating.
#
# Why the DDL is passed with -i
# -----------------------------
# Flink's default catalog is in-memory and scoped to a SQL client session, so a table
# created by one submission does not exist for the next. `sql-client.sh -i` loads init
# files into the session before running the -f script. Both jobs get every DDL file:
# it is metadata only, costs nothing, and no job can fail because a table was forgotten.
set -euo pipefail

readonly SQL_DIR=/opt/sql

# Infrastructure defaults match docker-compose.yml, so .env need not carry them —
# but .env wins if it does.
: "${KAFKA_BOOTSTRAP:=redpanda:9092}"
: "${PG_URL:=jdbc:postgresql://postgres:5432/sales_report}"
: "${PG_USER:=root}"
: "${PG_PASSWORD:=admin1}"
: "${PG_HOST:=postgres}"
: "${PG_PORT:=5432}"
export KAFKA_BOOTSTRAP PG_URL PG_USER PG_PASSWORD PG_HOST PG_PORT

# Which jobs to submit. Only the feedback job needs a model.
JOBS=("$@")
[ ${#JOBS[@]} -eq 0 ] && JOBS=(sales feedback)

needs_model=0
for name in "${JOBS[@]}"; do [ "$name" = "feedback" ] && needs_model=1; done

if [ "$needs_model" = "1" ]; then
  # Only the key is needed (the model is OpenAI gpt-4o-mini). A missing key fails late, inside a
  # running job, as a 401 that surfaces as a restart loop — so say so here instead.
  : "${OPENAI_API_KEY:?OPENAI_API_KEY is not set — copy .env.example to .env and fill it in}"
else
  # The DDL still gets rendered, so give envsubst something to substitute.
  : "${OPENAI_API_KEY:=unset}"
fi
export OPENAI_API_KEY

readonly SUBST='$OPENAI_API_KEY $KAFKA_BOOTSTRAP $PG_URL $PG_USER $PG_PASSWORD $PG_HOST $PG_PORT'

WORK="$(mktemp -d /tmp/pipeline.XXXXXX)"
# The SQL client appends every executed statement to a history file. CREATE MODEL
# carries the API key as a literal, so the default history would persist the key inside
# the container. Give it a throwaway file and delete both on exit.
HISTFILE_TMP="$(mktemp /tmp/sqlhist.XXXXXX)"
trap 'rm -rf "$WORK" "$HISTFILE_TMP"' EXIT

mkdir -p "$WORK/jobs"
for f in "$SQL_DIR"/jobs/*.sql; do
  envsubst "$SUBST" < "$f" > "$WORK/jobs/$(basename "$f")"
done

# -i takes ONE file, not a list. Concatenating the DDL in sorted order gives the same
# result with no ambiguity about ordering.
INIT="$WORK/init.sql"
for f in "$SQL_DIR"/ddl/*.sql; do
  printf -- '-- ===== %s =====\n' "$(basename "$f")" >> "$INIT"
  envsubst "$SUBST" < "$f" >> "$INIT"
  printf '\n' >> "$INIT"
done

job_file() {
  case "$1" in
    sales)    echo "$WORK/jobs/10_sales.sql" ;;
    feedback) echo "$WORK/jobs/20_feedback.sql" ;;
    *)        return 1 ;;
  esac
}

FAILED=""

submit_one() {
  local name="$1" file out
  if ! file="$(job_file "$name")"; then
    echo "unknown job '$name' (expected: sales, feedback)" >&2
    FAILED="$FAILED $name"
    return
  fi

  echo
  echo "--- submitting job: ${name} ---"
  # The SQL client echoes each statement as it executes, which would print the rendered
  # API key to the terminal and into scrollback. Redact it from the stream.
  out="$(/opt/flink/bin/sql-client.sh -hist "$HISTFILE_TMP" -i "$INIT" -f "$file" 2>&1 \
          | sed -e "s|${OPENAI_API_KEY}|<redacted>|g")"
  printf '%s\n' "$out" | grep -vE '^(WARNING|[A-Z][a-z]{2} [0-9]{2}, [0-9]{4})' || true

  # Judge by evidence of a submitted job, not by exit status: sql-client exits 0 after
  # "[ERROR] Could not execute SQL statement" as well. Anchoring on the 32-hex job id is
  # what makes this evidence. Both spellings are matched — older clients print
  # "Job ID: <hex>", Flink 2.3 prints "Job has been submitted with JobID <hex>".
  if ! printf '%s\n' "$out" | grep -qE 'Job ID: *[0-9a-f]{32}|JobID [0-9a-f]{32}'; then
    echo "  !! ${name} did not produce a Job ID — nothing was submitted" >&2
    FAILED="$FAILED $name"
  fi
}

if [ "$needs_model" = "1" ]; then
  echo "Submitting pipeline (chat model: OpenAI gpt-4o-mini)"
else
  echo "Submitting pipeline (no model job requested)"
fi

for name in "${JOBS[@]}"; do submit_one "$name"; done

echo
if [ -n "$FAILED" ]; then
  echo "FAILED to submit:${FAILED}" >&2
  exit 1
fi
echo "all requested jobs submitted"
