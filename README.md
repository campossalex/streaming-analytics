# Coffee Shop

Real-time sales analytics and LLM review summarization for a smoothie chain, using
**Apache Flink 2.3** and its **native AI functions**, fully self-contained with Docker
Compose. No Kubernetes, and no external Python service standing in for the model call.

The design decisions worth knowing about — the Flink 2.x SQL dialect, the
product-dimension join, the model provider — are called out under
[Implementation notes](#implementation-notes). The AI wiring follows the same pattern as `../../../streaming-rag`: `CREATE MODEL` +
`ML_PREDICT`, rendered by a `submit.sh` so the API key never touches disk.

Everything the scenario needs lives in this directory (`datagen/`, `flink_sql/`,
`grafana/`, the DDL files).

---

## Architecture

```
  ┌───────────────┐        ┌──────────────────────────────────────────┐
  │ purchases-gen │ ─────► │  store.purchases                         │
  └───────────────┘        │                (Redpanda / Kafka)        │
                           └──────────┬───────────────────────────────┘
                                      │
   PostgreSQL warehouse.master_product│  event-time temporal join
   (postgres-cdc, versioned) ─────────┤  FOR SYSTEM_TIME AS OF transaction_time
                                      ▼
                        ┌─────────────────────────────┐
                        │  Job 1: coffee-1-sales       │  TUMBLE 30s (windowing TVF)
                        │  jobs/10_sales.sql          │  item × category × state
                        └──────────────┬──────────────┘
                                       ▼
                        sales_report.purchase_report (PostgreSQL) ──► Grafana


  ┌───────────────┐        ┌──────────────────────────┐
  │ feedback-gen  │ ─────► │  store.feedback          │
  └───────────────┘        └────────────┬─────────────┘
                                        │
                          ┌─────────────▼──────────────┐
                          │  Job 2: coffee-2-feedback   │
                          │  jobs/20_feedback.sql       │
                          │                             │
                          │  WHERE overall_score < 6     │
                          │        │                     │
                          │  CREATE MODEL ai_analyze_review
                          │        │   (native Flink AI function,
                          │        │    provider = openai)
                          │  ML_PREDICT(...)             │
                          │        │                     │
                          │  REGEXP_EXTRACT × 3 → columns│
                          └──────────────┬──────────────┘
                                         ▼
                        sales_report.feedback_summary (PostgreSQL) ──► Grafana
```

Two streaming jobs run continuously: **1** sales aggregation (no model, no external
dependency), **2** review filter → `ML_PREDICT` → parsed summary. Splitting them means
job 2 — the one that costs money per row and depends on someone else's endpoint — can
be stopped, restarted, or re-pointed at a different model without the sales dashboard
ever noticing.

---

## Prerequisites

| Tool | Version |
|------|---------|
| Docker + Docker Compose v2 | `docker compose version` ≥ 2.20 |
| `curl`, `jq` | Any recent version |
| 6 GB RAM available to Docker | — |
| `OPENAI_API_KEY` | Required for job 2. The model is OpenAI `gpt-4o-mini` (endpoint and model are set in `flink_sql/ddl/02_models.sql`) |

---

## Quick Start

```bash
cd scenarios/coffee_shop_oss

cp .env.example .env
# edit .env: set OPENAI_API_KEY — that's all (the model is OpenAI gpt-4o-mini)

./start.sh
```

Then open:

| Service | URL |
|---------|-----|
| **Environment homepage** — start here; links to everything below, with live status | http://localhost |
| Grafana (2 provisioned dashboards) | http://localhost:3000 |
| Flink Web UI | http://localhost:8081 |
| Redpanda Console | http://localhost:8082 |
| Web SQL editor | http://localhost:8084 |

First rows land in `purchase_report` after one 30-second window; the first feedback
summary after a minute or two, since only ~15% of generated reviews score below 6.

If `.env` has no real key, `start.sh` submits only the sales job and tells you the exact
follow-up command for the feedback job once you set one.

---

## Step-by-Step Setup

### 1. Download JARs

```bash
./download-jars.sh
```

Downloads into `./lib/`, which is mounted read-only at `/opt/flink/lib/extra/` in the
Flink containers and copied into `/opt/flink/lib/` by their entrypoints. Safe to re-run
— present JARs are skipped.

- `flink-model-openai-2.3.0.jar` — the OpenAI model provider for `CREATE MODEL`. **Not**
  part of the Flink distribution; released separately (see
  [Implementation notes](#implementation-notes) below)
- `flink-sql-connector-kafka-5.0.0-2.2.jar` — Kafka sources and sinks
- `flink-connector-jdbc-core` + `-postgres-4.0.0-2.0.jar` — PostgreSQL upsert sinks
  and the `dwh` catalog. Flink 2.x's JDBC connector is modular: one dialect jar per
  database, on top of the core connector
- `postgresql-42.7.7.jar` — the JDBC driver
- `flink-sql-connector-postgres-cdc-3.6.0-2.2.jar` — the product dimension,
  `master_product`, read from PostgreSQL's logical-replication stream. The version
  suffix matters here: CDC releases without a Flink-version suffix (3.5.0 and earlier)
  are built against the removed 1.x `SourceFunction` API and fail on a 2.x runtime with
  `ClassNotFoundException`

### 2. Start Docker Compose

```bash
docker compose up -d
```

| Service | Host port | Credentials |
|---------|-----------|-------------|
| Environment homepage (nginx) | 80 | — |
| Flink Web UI | 8081 | — |
| Redpanda Console | 8082 | — |
| Redpanda Kafka | 19092 (`redpanda:9092` inside) | — |
| PostgreSQL `sales_report` and `warehouse` | 5432 | `root` / `admin1` |
| Grafana | 3000 | anonymous, admin role |
| Flink SQL Gateway (REST) | 8083 | — |
| Web SQL editor | 8084 | — |

Both databases are seeded from `docker-entrypoint-initdb.d` on first start:
`pg_sales_report_ddl.sql` and `pg_feedback_ddl.sql` create `sales_report`, where the jobs
write; `pg_warehouse_ddl.sql` creates `warehouse` with `master_product`, its publication
and `REPLICA IDENTITY FULL`. PostgreSQL runs with `wal_level=logical` so postgres-cdc
can stream it.

### 3. Submit the Flink SQL jobs

`start.sh` does this for you via `submit.sh`, which runs *inside* the `sql-client`
container. To bring the stack up **without** submitting anything — for walking the lab
through by hand, or demoing submission live — use:

```bash
./start.sh --services-only
```

That does everything except step 4: containers up, databases seeded, topics created,
generators producing, JARs on the Flink classpath, and an empty job list in the Flink
UI. Add `--reset` to wipe every volume first (`docker compose down -v`), so PostgreSQL,
the catalog store, the topics and any jobs start from nothing:

```bash
./start.sh --reset --services-only
```

Either way the script ends by printing the whole runbook — every step below as a
copy-pasteable command — so you never have to come back to this file for them.

Submitting by hand:

```bash
# Both jobs
docker compose exec sql-client /opt/submit.sh

# One at a time
docker compose exec sql-client /opt/submit.sh sales
docker compose exec sql-client /opt/submit.sh feedback
```

`submit.sh` renders `flink_sql/ddl/*.sql` and `flink_sql/jobs/*.sql` with `envsubst`
(`${OPENAI_API_KEY}`, `${PG_URL}`, `${PG_HOST}`, ...), loads every DDL file into the
session with `-i` — see [What survives a session](#what-survives-a-session) for why each
job needs its own copy of every `CREATE TABLE` / `CREATE MODEL` — and submits the job file
with `-f`. The rendered files live in a throwaway `mktemp -d` directory and the SQL
Client's history file is redirected to another one, deleted on exit, so the API key is
never written to a persistent path.

### 4. Work through the lab interactively

```bash
docker compose exec sql-client sh -c \
  'for f in /opt/sql/ddl/*.sql; do envsubst < $f; done > /tmp/init.sql'
docker compose exec sql-client /opt/flink/bin/sql-client.sh -i /tmp/init.sql
```

`flink_sql/explore.sql` holds the Lab 1 and Lab 2 queries —
raw stream, enrichment join, TUMBLE, HOP, the full report shape, an ad hoc `ML_PREDICT`
call. Its product-dimension query is a live CDC stream: leave it running, update a row
in PostgreSQL, and watch the change arrive. Paste them one at a time; press `q` to leave a result grid. It is outside
`ddl/` and `jobs/` on purpose, so `submit.sh` never submits it as a job.

#### In the browser: the Pipeline Builder

http://localhost:8084/pipeline is the architecture diagram as a control surface, served by
the same `sql-editor` app and sharing the editor's session. Every box is a table (or the
model), every job box a Flink job, and each button opens the SQL it runs — cut from
`flink_sql/` at request time, so the files stay the one source — in a pop-up where it can
be edited before running:

- **Create** registers a source, the model or the sink in the session; **View data** runs a
  streaming (or, for the PostgreSQL tables, bounded) query and shows its rows live;
  **Deploy** submits a job file as a long-running Flink job, and the pop-up then offers
  **Stop**.
- Boxes follow the live state: dashed until created, green once the job feeding them runs
  and the table returns a row. The state comes from the session (`SHOW TABLES`,
  `SHOW MODELS`) and the JobManager's REST API — jobs are matched to boxes by the table
  their plan writes, so a job submitted from a terminal lights up too.
- The `dwh` catalog is registered on page load, since `purchase_report` already exists in
  PostgreSQL. After a full `./start.sh`, the objects the running jobs use are created in
  the browser's session too, so they can be queried straight away.
- **Guided mode** (asked on the first visit, or `?mode=guided`) points at the next button
  with what it does and what to look for, following the labs; hovering any box explains it.
- Placeholders and the API key are handled as in the editor below: the pop-up shows
  `${OPENAI_API_KEY}`, the backend fills it in. Without a key, the model and feedback-job
  pop-ups say so.

#### The environment homepage

`homepage/index.html` is a single static file served by nginx on port 80 — the address to
put on a slide. It carries the architecture diagram (with a clickable ⓘ on each table that
opens its columns, key facts and a query to try), a card per web UI with a live
reachability dot, the lab list, and the connection details.

Every link and address is built from `location.hostname`, so the same file works on
`localhost` and on a lab VM with no configuration. The status dots `fetch` each UI with
`mode: "no-cors"` every 15 seconds — an opaque response still proves the port is serving,
while a refused connection rejects. Same page structure and CSS tokens as
`scenarios/lakehouse_oss/homepage/`. The table panels are kept in step with
`flink_sql/ddl/01_sources.sql` and the `pg_*.sql` files **by hand** — if
you change a schema there, update `TABLES` in the page.

#### In the browser: the web SQL editor

http://localhost:8084 does the same thing without a terminal. Two services provide it:

- **`sql-gateway`** — Flink's SQL Gateway, the engine the SQL Client embeds, exposed as a
  REST API on port 8083. It shares the `catalog-store` volume with `sql-client`, so `dwh`
  is visible from both.
- **`sql-editor`** — a small Flask app (`sql-editor/app.py`) that serves the page and
  proxies to the gateway, which sends no CORS headers.

It is set up for a workshop, where attendees issue the DDL themselves and then query:

- **One session per browser, kept for the whole workshop.** The session handle is stored in
  the browser, so a reload, a closed tab or a second tab reconnects to the same session,
  and the tables, views and models created in it are still there. The editor text is
  saved the same way. Sessions never expire (`sql-gateway.session.idle-timeout=0`), so a
  break or a sleeping laptop does not lose them either. *New session* discards everything
  and starts clean, after a confirmation.
- **Sessions start empty.** Attendees run the `CREATE TABLE` / `CREATE MODEL` statements
  themselves; the examples menu has each `flink_sql/ddl/` file to load, followed by the
  queries from `explore.sql`, one entry per `-- ---- title` section. To preload the DDL
  instead, the way `sql-client.sh -i` does, set `PRELOAD_DDL=true` in `.env` (or tick
  *preload DDL* before *New session*). Don't combine the two: a preloaded table makes
  the attendee's own `CREATE TABLE` fail as a duplicate.
- **Placeholders are filled in by the editor.** `${KAFKA_BOOTSTRAP}`, `${PG_URL}`,
  `${OPENAI_API_KEY}` and the rest of `submit.sh`'s list are replaced in every statement
  before it reaches the gateway, so the DDL runs exactly as written in the files. The API
  key comes from `.env`, never reaches the browser, and shows as `<redacted>` in anything
  that echoes it back (`SHOW CREATE MODEL`, error messages). Only those exact `${NAME}`
  forms are replaced; any other `$` is left alone.
- **Scripts run statement by statement**, each with its own result panel. A streaming
  `SELECT` keeps its panel live (retractions applied, as in the SQL Client's table mode)
  until you press **Cancel**, which cancels the Flink job behind it.
- **The Catalog panel** (the *Catalog* button, top left) browses catalogs, databases and
  tables in the session: `default_catalog` with the tables created in it, and `dwh`
  with the PostgreSQL tables it reflects. Click a table for its columns, double-click to
  insert its name at the cursor; **Info** opens its structure (columns, keys, watermark,
  connector) and **DDL** runs `SHOW CREATE TABLE`. It refreshes only the levels a
  `CREATE` / `DROP` / `ALTER` you ran has changed.
- **An `INSERT INTO` is a real job.** The panel links to it in the Flink UI, and it keeps
  running after the tab is closed — stop it there, as in step 7 of the runbook.
- **Restarting `sql-gateway` loses every session.** The catalog is in memory, so after
  a restart attendees must run their DDL again. `CREATE CATALOG dwh` is the exception,
  as described in [What survives a session](#what-survives-a-session).

---

## Demo Walkthrough

### Watch events arrive

Redpanda Console (http://localhost:8082) → **Topics** → `store.purchases`. Expand a
message to see the JSON the generator publishes.

### Confirm the sales pipeline

```bash
docker compose exec postgres psql -U root -d sales_report \
  -c "SELECT count(*) FROM purchase_report;"

docker compose exec postgres psql -U root -d sales_report -c \
  "SELECT dim_item, dim_state, purchase_time, fact_sum_total_purchase
     FROM purchase_report ORDER BY purchase_time DESC LIMIT 10;"
```

Grafana → **Coffee Shop — Sales Report** shows the same data, refreshing every 10s.

### Confirm the feedback pipeline

```bash
docker compose exec postgres psql -U root -d sales_report -c \
  "SELECT review_id, overall_score, product_name, summary
     FROM feedback_summary ORDER BY processed_at DESC LIMIT 5;"
```

Grafana → **Coffee Shop — Customer Feedback** shows the summaries and the two suggested
improvements per review. If the panel is empty, check the job is running
(http://localhost:8081) — a missing or wrong `OPENAI_API_KEY` fails inside the job as a
401 at the first `ML_PREDICT` call, which shows up as a restart loop rather than a
one-line error.

### Change a product price and watch it flow through

```bash
docker compose exec postgres psql -U root -d warehouse \
  -c "UPDATE master_product SET item = 'Sunrise Sunset XL' WHERE product_id = 'CS01';"
```

postgres-cdc delivers the change within a second, and the next window to close (about
30 s) carries the new name. The window that straddles the update holds **both** names:
the temporal join gives each purchase the product version valid at its own
`transaction_time`, so purchases before the commit keep the old name. Put it back the
same way with `item = 'Sunrise Sunset'`.

---

## SQL Files Reference

| File | Purpose |
|------|---------|
| `flink_sql/ddl/01_sources.sql` | Kafka sources, the postgres-cdc product dimension, the `dwh` catalog, the feedback sink |
| `flink_sql/ddl/02_models.sql` | `CREATE MODEL ai_analyze_review` |
| `flink_sql/jobs/10_sales.sql` | 30s windowed aggregation → `purchase_report` |
| `flink_sql/jobs/20_feedback.sql` | `overall_score < 6` filter → `ML_PREDICT` → `feedback_summary` |
| `flink_sql/explore.sql` | Lab 1 & 2 exploration queries and an ad hoc `ML_PREDICT` — not submitted; also the web editor's examples menu |
| `sql-editor/` | Web SQL editor: a Flask proxy (`app.py`) in front of the SQL Gateway, the page (`static/index.html`) and its shared scripts and styles, including the catalog browser (`catalog.js`) |

---

## Implementation Notes

| Concern | How this scenario does it |
|---------|---------------------------|
| Runtime | Flink **2.3** session cluster; SQL Client, SQL Gateway and the web SQL editor |
| Model inference | `CREATE MODEL` + `ML_PREDICT` — native Flink AI functions, requires Flink ≥ 2.0 and the separate `flink-model-openai` provider jar |
| Job submission | `submit.sh` (envsubst + `sql-client.sh -i/-f`), invoked by `start.sh` |
| Windowing syntax | Windowing table-valued function: `FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(ts), INTERVAL ...))`, `window_start` as a plain column — Flink 2.x dropped the legacy grouped-window form |
| Keyed sink writes | Explicit `ON CONFLICT DO NOTHING` — Flink 2.3 requires stating how same-key collisions resolve |
| Product dimension | PostgreSQL, `postgres-cdc` versioned table, event-time temporal join `FOR SYSTEM_TIME AS OF transaction_time` |
| State backend config | `state.backend.type: hashmap` + `execution.checkpointing.dir` — the Flink 2.x key names; the 1.x spelling `state.backend: filesystem` parses as a class name and job submission fails with `ClassNotFoundException: filesystem` |
| Databases | Containers seeded by `docker-entrypoint-initdb.d` |
| Grafana | Datasource, both dashboards **and** the word-cloud plugin provisioned |
| Generators | `purchases-gen` / `feedback-gen` containers |

Four of these are worth expanding on.

**The model call is native SQL.** Flink 2.3 has `CREATE MODEL` / `ML_PREDICT` built in,
so the LLM step needs no external service: `flink_sql/jobs/20_feedback.sql` builds an
enriched `review_summary` string, passes it to the model, and applies three
`REGEXP_EXTRACT`s to the model's `output`. The one catch: the `openai` provider ships as
a **separate jar** (`flink-model-openai`), not bundled with Flink, and must be fetched
and put on the classpath — `CREATE MODEL` succeeds either way (model DDL is lazy), and a
missing jar only surfaces at the first `ML_PREDICT` call, as
`"Could not find any factories that implement ModelProviderFactory"`.

**Windowing uses table-valued functions.** Flink 2.x removed the legacy grouped-window
aggregation (`GROUP BY TUMBLE(transaction_time, INTERVAL '30' SECONDS)` with
`TUMBLE_START(...)` in the SELECT list). The replacement is the windowing table-valued
function — `FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(transaction_time), INTERVAL '30' SECONDS))`
— with `window_start` / `window_end` as ordinary output columns. `flink_sql/explore.sql`
uses this form throughout, with comments explaining the mapping from the older syntax.

**Keyed sinks need an explicit conflict clause.** Flink 2.3 refuses to plan an `INSERT`
into a table with a `PRIMARY KEY` unless the query's upsert key is declared compatible,
or an `ON CONFLICT` clause says how to resolve a same-key collision. Both jobs here are
append-only per key (one row per closed window, one row per review), so nothing ever
collides — `ON CONFLICT DO NOTHING` is the option that keeps no extra state and is also
what makes a replay after a restart idempotent.

**The product dimension is a versioned CDC table.** `master_product` is read with
`postgres-cdc` — a snapshot, then PostgreSQL's logical-replication stream — and declares
a `PRIMARY KEY` plus a watermark on `update_time` (the change's commit time, from the
`op_ts` metadata column). That makes it a *versioned table*, and the sales job joins it
`FOR SYSTEM_TIME AS OF p.transaction_time`: each purchase is enriched with the product
row valid when it happened, and a replay of the topic reproduces the same report. Three
details make it work:

- **The join must be event-time.** A processing-time temporal join is only supported
  against lookup sources, and a regular join would materialise `transaction_time`, after
  which `TUMBLE` can no longer window on it. `update_time` is cast to `TIMESTAMP(3)`
  because both sides' rowtimes must have the same type.
- **The idle timeout.** `master_product`'s watermark only moves when a product changes,
  and a temporal join waits for the slower of its two inputs. `jobs/10_sales.sql` sets
  `table.exec.source.idle-timeout = 5 s`, so the CDC source is marked idle shortly after
  its last change and the join follows the purchase stream alone. Without it the job runs
  and never emits a row.
- **One replication slot per reader.** The DDL's default slot is
  `flink_master_product`, for interactive queries; the sales job overrides it with
  `/*+ OPTIONS('slot.name' = 'coffee_sales') */` so the two never collide. Slots of
  cancelled queries stay behind, inactive, until `docker compose down -v`.

---

## What survives a session

Flink's default catalog is a `GenericInMemoryCatalog`: it lives in the SQL Client's JVM
heap, backed by nothing. When the client exits, every `CREATE TABLE`, `CREATE MODEL` and
`CREATE TEMPORARY VIEW` in it is gone — not forgotten, never stored.

That does not affect running jobs. The catalog is a *compile-time* thing: at submission
the planner resolves table names and bakes the connector options — brokers, topics, JDBC
URLs, the model endpoint and its API key — into the JobGraph. The cluster never consults a
catalog again, which is why both jobs keep running after `submit.sh` exits, and why
editing `.env` does nothing until you re-create `sql-client` and resubmit.

One thing does survive: **catalog registrations**. `docker-compose.yml` enables Flink's
file-based CatalogStore on the `sql-client` container:

```yaml
table.catalog-store.kind: file
table.catalog-store.file.path: /opt/flink/catalog-store   # named volume
```

A CatalogStore persists the *configuration* of catalogs created in a session, not tables —
a table is stored inside its catalog, and an in-memory catalog stores nowhere. So after
the DDL has run once, a fresh session with no `-i`:

| Statement | Result |
|-----------|--------|
| `SHOW CATALOGS` | `default_catalog`, **`dwh`** — the registration was reloaded from `/opt/flink/catalog-store/dwh.yaml` |
| `SELECT * FROM dwh.sales_report.purchase_report` | Works — the table really exists in PostgreSQL and the JDBC catalog reflects it |
| `SHOW TABLES` | **Empty** — `purchase_stream`, `feedback`, the sinks and `ai_analyze_review` all went with the last session |

A Kafka table can never survive this way: its definition exists only inside Flink, and
there is no persistent catalog on Flink 2.3 that can hold one (the Hive metastore catalog
is the only catalog that stores arbitrary Flink tables, and its newest build is
`flink-connector-hive 3.0.0-1.20` — Flink 1.20 only). So `-i` stays mandatory, which is
fine: DDL in files is version-controlled and reviewable, and the OpenAI key is rendered
per-session instead of living in a metastore.

Two operational notes:

- The volume is created root-owned while the Flink image runs as uid 9999, so a one-shot
  `catalog-store-init` service chowns it. Without that the first `CREATE CATALOG` fails
  with `FileNotFoundException: .../dwh.yaml (Permission denied)`.
- `dwh.yaml` contains the PostgreSQL password in plaintext. That is a lab credential and
  the file lives in a Docker volume, but it is the reason the *model* is deliberately not
  a catalog — the OpenAI key is never written there.

`CREATE CATALOG IF NOT EXISTS` in `ddl/01_sources.sql` is what keeps replaying the DDL
idempotent; without it the second session would fail with *Catalog dwh already exists*.

---

## Dashboards

Two dashboards are provisioned from `grafana/dashboards/`, along with the datasource, so
there is nothing to import by hand:

| Dashboard | Source | Notes |
|-----------|--------|-------|
| **Coffee Shop — Sales Report** | `grafana-dashboard.json` at the repo root, repointed at the provisioned datasource | Labs 1–4 |
| **Coffee Shop — Customer Feedback & AI Insights** | The labday-1 template, verbatim | Bonus Track; needs the word-cloud plugin |

The feedback dashboard is the template from the labday-1 walkthrough, used
unmodified except for clearing the numeric `id`, which provisioning requires. Its panels
already reference the datasource by `uid: sales_dwh`, which is exactly the uid
`grafana/provisioning/datasources/postgres.yaml` declares, so it resolves with no edits.

**The word-cloud panel needs a plugin.** "Improvement Keywords — Word Cloud" is a
`magnesium-wordcloud-panel`, which is not a core Grafana visualization. It is installed
at container start via `GF_INSTALL_PLUGINS` in `docker-compose.yml`:

```yaml
GF_INSTALL_PLUGINS: "magnesium-wordcloud-panel"
```

Grafana pulls it from the plugin catalog on first boot (it is signed, so no
`GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS` is needed). Without it, that one panel shows
"Panel plugin not found: magnesium-wordcloud-panel" while the other nine work normally.
Verify it loaded with:

```bash
curl -s http://localhost:3000/api/plugins | grep -o 'magnesium-wordcloud-panel'
```

**The word cloud depends on the prompt.** Its query splits `improvement_1` and
`improvement_2` on whitespace, drops a stop-word list, and counts what remains — so it
only produces meaningful themes if the improvements are single words. That is why the
system prompt in `flink_sql/ddl/02_models.sql` ends with *"return one single word for
each improvement"*, matching the labday-1 walkthrough. If you loosen that instruction to
get richer advice in the table, expect the word cloud to fill with incidental vocabulary.

---

## Teardown

```bash
# Stop and remove containers + volumes
docker compose down -v

# Keep volumes (preserve PostgreSQL data across restarts)
docker compose down
```

To start over with no data, `./start.sh --reset --services-only` does the `down -v` and
brings the stack back up empty.

---

## Timestamps and timezones

`transaction_time` and `event_time` are naive strings from `datetime.now()`, so the
container's timezone silently defines what they mean. Every generator container, both
databases, and the Flink cluster are pinned to `TZ=UTC`, and the planner to
`table.local-time-zone: UTC`. The Grafana dashboards use `$__timeFrom()`/`$__timeTo()`
against these columns, so a mismatch would show up as an empty dashboard with rows
sitting in PostgreSQL — check the timezone before suspecting the pipeline.

---

## Troubleshooting

**No rows in `purchase_report`.** Check the job is running (http://localhost:8081) and
that `warehouse` seeded correctly — the temporal join drops purchases whose `product_id`
is not in `master_product`:

```bash
docker compose exec postgres psql -U root -d warehouse \
  -c "SELECT COUNT(*) FROM master_product;"   # expect 27
```

If the job is `RUNNING` and the table is seeded but nothing ever lands, the
`table.exec.source.idle-timeout` line is missing from the job: the join is waiting on
`master_product`'s watermark, which does not move until a product changes.

**"replication slot ... is active".** Two queries are reading `master_product` with the
same slot. Cancel the other one in the SQL editor or the Flink UI, or give the new one
its own slot: `FROM master_product /*+ OPTIONS('slot.name' = 'my_slot') */`.

**No rows in `feedback_summary`.** Almost always the model call, not the SQL:

```bash
docker compose logs jobmanager --tail=50 | grep -i "ModelProviderFactory\|401\|ML_PREDICT"
```

- `"Could not find any factories that implement ModelProviderFactory"` — the
  `flink-model-openai` jar is missing from `/opt/flink/lib/` on the TaskManager. Re-run
  `download-jars.sh` and the JAR-copy step in `start.sh`.
- A 401/403 in the job's exception history — `OPENAI_API_KEY` is wrong or unset. Fix
  `.env` and re-submit: `docker compose exec sql-client /opt/submit.sh feedback`.
- Job is `RUNNING` with no exceptions and still nothing lands — check that a bad review
  has actually been generated yet (~15% of reviews, one every 5–15s):
  `docker compose exec redpanda rpk topic consume store.feedback --num 3`.

**`ClassNotFoundException: filesystem` on job submission.** Something reintroduced the
1.x `state.backend: filesystem` / `state.checkpoints.dir` keys in
`docker-compose.yml`. Use `state.backend.type: hashmap` and
`execution.checkpointing.dir` instead.

**`ClassNotFoundException: ...SourceFunction` from `master_product`.** Either the
`flink-sql-connector-postgres-cdc` jar in `./lib/` is a pre-2.x build (no `-2.x` suffix
in the filename) — re-run `download-jars.sh`, which pins the `-2.2` build — or
`'scan.incremental.snapshot.enabled' = 'true'` was dropped from the DDL, which falls back
to the legacy SourceFunction-based reader.

**Incorrect syntax near the keyword 'TABLE'.** `ML_PREDICT`'s (and the windowing TVF's)
table argument must be a name — a table, a view or a `WITH` alias — or a plain
parenthesised subquery; `TABLE (SELECT ...)` does not parse. Both jobs name their input
with a `WITH` clause inside the `INSERT`, which keeps each job a single statement that
can be re-run in the same session; `explore.sql` uses `CREATE TEMPORARY VIEW` so later
queries can reuse the input.

**"Panel plugin not found: magnesium-wordcloud-panel".** The plugin download failed at
container start — usually no network egress on first boot. Re-create the container with
`docker compose up -d --force-recreate grafana`, or install it manually with
`docker compose exec grafana grafana cli plugins install magnesium-wordcloud-panel`
followed by a restart.

**The word cloud is full of filler words.** The model is returning sentence-length
improvements. Check the prompt in `flink_sql/ddl/02_models.sql` still ends with "return
one single word for each improvement", and confirm what actually landed:
`SELECT improvement_1, improvement_2 FROM feedback_summary LIMIT 5;`

**Grafana panels are empty but PostgreSQL has rows.** The dashboards query
`purchase_time` / `processed_at` against the picked time range. Widen the range, and
re-read the timezone note above.
