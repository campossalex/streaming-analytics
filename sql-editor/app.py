"""Web SQL editor for the coffee_shop_oss scenario.

A thin proxy in front of the Flink SQL Gateway. The browser cannot call the gateway
directly (it sends no CORS headers), and a gateway session starts as empty as a bare
`sql-client.sh`: the default catalog is in-memory, so every session needs the DDL.
This app does what `submit.sh` and `sql-client.sh -i` do for the terminal:

  - fills in ${KAFKA_BOOTSTRAP}, ${OPENAI_API_KEY}, ... — the same names as submit.sh's
    SUBST list — in every statement the browser runs, so a workshop attendee can type
    the DDL with placeholders and the API key never reaches the browser
  - optionally (PRELOAD_DDL=true) runs flink_sql/ddl/*.sql into each new session first,
    like `sql-client.sh -i`. Off by default: in the workshop attendees issue the DDL
    themselves, and a preloaded table makes their CREATE TABLE fail as a duplicate
  - redacts the API key from everything it sends back (SHOW CREATE MODEL, error
    messages that echo connector options, ...)
  - backs the catalog browser (static/catalog.js): SHOW / DESCRIBE / SHOW CREATE in the
    browser's own session, so it sees that session's in-memory tables too
  - backs the Pipeline Builder (static/pipeline.html, at /pipeline): the architecture
    diagram as a control surface, with what exists and what runs read from the gateway
    and the Flink JobManager
"""
import glob
import json
import os
import re
import threading
import time

import requests
from flask import Flask, Response, jsonify, request, send_from_directory

GATEWAY = os.environ.get("GATEWAY_URL", "http://sql-gateway:8083").rstrip("/")
FLINK = os.environ.get("FLINK_URL", "http://jobmanager:8081").rstrip("/")
SQL_DIR = os.environ.get("SQL_DIR", "/opt/sql")
BUILTIN_CATALOG = "default_catalog"

# Same defaults as submit.sh, so the DDL renders even when .env carries none of them.
# The model is OpenAI gpt-4o-mini, so only OPENAI_API_KEY needs setting. A missing key is
# fine here: CREATE MODEL is lazy, and only ML_PREDICT fails.
DEFAULTS = {
    "KAFKA_BOOTSTRAP": "redpanda:9092",
    "PG_URL": "jdbc:postgresql://postgres:5432/sales_report",
    "PG_USER": "root",
    "PG_PASSWORD": "admin1",
    "PG_HOST": "postgres",
    "PG_PORT": "5432",
    "OPENAI_API_KEY": "unset",
}
VARS = {k: os.environ.get(k) or v for k, v in DEFAULTS.items()}
SECRET = VARS["OPENAI_API_KEY"] if VARS["OPENAI_API_KEY"] != "unset" else ""
PRELOAD_DDL = os.environ.get("PRELOAD_DDL", "false").lower() in ("1", "true", "yes")

# Only the braced form, and only these names — the form the SQL files use. Any other
# `$` in a statement is left exactly as typed.
PLACEHOLDER = re.compile(r"\$\{(" + "|".join(DEFAULTS) + r")\}")


def substitute(sql):
    return PLACEHOLDER.sub(lambda m: VARS[m.group(1)], sql)

app = Flask(__name__, static_folder="static")


# ── SQL text handling ─────────────────────────────────────────────────────────

def split_statements(sql):
    """Split a script into statements on `;`, dropping comments.

    A character scanner rather than str.split: string literals can hold `;` or `--`,
    and explore.sql puts a `--` comment inside a WITH (...) clause.
    """
    out, buf, i, n = [], [], 0, len(sql)
    while i < n:
        c = sql[i]
        if c in ("'", "`", '"'):
            j = i + 1
            while j < n:
                if sql[j] == c:
                    if j + 1 < n and sql[j + 1] == c:  # doubled quote is an escape
                        j += 2
                        continue
                    break
                j += 1
            buf.append(sql[i:j + 1])
            i = j + 1
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            i = n if j < 0 else j
        elif sql.startswith("/*+", i):
            # A hint, not a comment: `/*+ OPTIONS('snapshot-id' = '...') */` is part of
            # the statement, and dropping it would silently run a different query.
            j = sql.find("*/", i + 3)
            j = n if j < 0 else j + 2
            buf.append(sql[i:j])
            i = j
        elif sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            i = n if j < 0 else j + 2
        elif c == ";":
            out.append("".join(buf))
            buf = []
            i += 1
        else:
            buf.append(c)
            i += 1
    out.append("".join(buf))
    return [s.strip() for s in out if s.strip()]


def ddl_files():
    return sorted(glob.glob(os.path.join(SQL_DIR, "ddl", "*.sql")))


def render_ddl():
    statements = []
    for path in ddl_files():
        with open(path) as f:
            statements += split_statements(substitute(f.read()))
    return statements


DDL = render_ddl()

BANNER = re.compile(r"^-- -{3,} (.+?)\s*$")


PIPELINE_NAME = re.compile(r"SET\s+'pipeline\.name'\s*=\s*'([^']+)'", re.I)

# Appended to each job example. The job file's SET 'pipeline.name' is a session option,
# so without this every SELECT run afterwards would show up in the Flink UI under the
# job's name.
RESET_NAME = """
-- Added by the editor: stop later statements in this session inheriting the job name.
RESET 'pipeline.name';
"""


def load_examples():
    """In workshop order: the DDL files as written — placeholders and all, substituted
    only when run — then explore.sql cut at its `-- ------ title` banners, then the job
    files, each an INSERT INTO a PostgreSQL sink."""
    examples = []
    for path in ddl_files():
        with open(path) as f:
            examples.append({"group": "DDL", "title": os.path.basename(path), "sql": f.read()})
    examples += load_explore()
    for path in sorted(glob.glob(os.path.join(SQL_DIR, "jobs", "*.sql"))):
        with open(path) as f:
            sql = f.read()
        m = PIPELINE_NAME.search(sql)
        title = os.path.basename(path) + (f" — {m.group(1)}" if m else "")
        examples.append({"group": "Jobs", "title": title,
                         "sql": sql.rstrip() + "\n" + (RESET_NAME if m else "")})
    return examples


def load_sections(path):
    """A SQL file cut at its `-- ------ title` banners: [(title, body), ...]. Whatever
    comes before the first banner (the file's header) is dropped."""
    if not os.path.isfile(path):
        return []
    sections, title, body = [], None, []
    with open(path) as f:
        for line in f:
            m = BANNER.match(line)
            if m:
                if title:
                    sections.append((title, "".join(body).strip() + "\n"))
                title, body = m.group(1), []
            elif title:
                body.append(line)
    if title:
        sections.append((title, "".join(body).strip() + "\n"))
    return sections


def load_explore():
    return [{"group": "Queries", "title": t, "sql": b}
            for t, b in load_sections(os.path.join(SQL_DIR, "explore.sql"))]


# ── Gateway access ────────────────────────────────────────────────────────────

class GatewayError(Exception):
    pass


def redact(text):
    return text.replace(SECRET, "<redacted>") if SECRET else text


def gw(method, path, body=None):
    r = requests.request(method, GATEWAY + path, json=body, timeout=60)
    text = redact(r.text)
    if r.status_code >= 400:
        raise GatewayError(error_text(text))
    return json.loads(text) if text else {}


def error_text(text):
    """The gateway returns {"errors": ["Internal server error.", "<stack trace>"]}."""
    try:
        errors = json.loads(text).get("errors") or [text]
    except ValueError:
        errors = [text]
    return "\n".join(e for e in errors if e != "Internal server error.") or text


def result_path(session, op, token=0):
    return f"/v2/sessions/{session}/operations/{op}/result/{token}?rowFormat=JSON"


def execute_and_wait(session, sql, timeout=60):
    op = gw("POST", f"/v1/sessions/{session}/statements", {"statement": sql})["operationHandle"]
    path, deadline = result_path(session, op), time.time() + timeout
    while path:
        res = gw("GET", path)
        if res.get("resultType") == "EOS":
            break
        if time.time() > deadline:
            raise GatewayError(f"timed out after {timeout}s: {sql[:80]}")
        if res.get("resultType") == "NOT_READY":
            time.sleep(0.2)
        path = res.get("nextResultUri")
    gw("DELETE", f"/v1/sessions/{session}/operations/{op}/close")


def json_error(e, status=502):
    return jsonify({"error": str(e)}), status


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/pipeline")
def pipeline_page():
    return send_from_directory(app.static_folder, "pipeline.html")


@app.get("/api/config")
def config():
    return jsonify({"preloadDdl": PRELOAD_DDL})


@app.post("/api/session")
def open_session():
    load_ddl = (request.get_json(silent=True) or {}).get("ddl", PRELOAD_DDL)
    try:
        session = gw("POST", "/v1/sessions", {"sessionName": "sql-editor"})["sessionHandle"]
        if load_ddl:
            for stmt in DDL:
                execute_and_wait(session, stmt)
    except (GatewayError, requests.RequestException) as e:
        return json_error(f"could not open a session with the DDL loaded:\n{e}")
    return jsonify({"session": session, "ddl": len(DDL) if load_ddl else 0})


@app.post("/api/split")
def split():
    return jsonify({"statements": split_statements(request.get_json()["sql"])})


@app.post("/api/run/<session>")
def run(session):
    try:
        res = gw("POST", f"/v1/sessions/{session}/statements",
                 {"statement": substitute(request.get_json()["sql"])})
    except (GatewayError, requests.RequestException) as e:
        return json_error(e, 400)
    return jsonify({"op": res["operationHandle"]})


@app.get("/api/result/<session>/<op>/<int:token>")
def result(session, op, token):
    try:
        res = gw("GET", result_path(session, op, token))
    except (GatewayError, requests.RequestException) as e:
        return json_error(e, 400)
    nxt = res.get("nextResultUri")
    return jsonify({
        "type": res.get("resultType"),
        "jobId": res.get("jobID"),
        "isQuery": res.get("isQueryResult"),
        "columns": [c["name"] for c in (res.get("results") or {}).get("columns", [])],
        "rows": (res.get("results") or {}).get("data", []),
        "next": int(nxt.split("/result/")[1].split("?")[0]) if nxt else None,
    })


# Close, not cancel: a streaming SELECT's operation is FINISHED as soon as its job is
# submitted, so /cancel refuses it ("Failed to convert the Operation Status from
# FINISHED to CANCELED"). Closing releases the result fetcher, which cancels the job.
@app.post("/api/cancel/<session>/<op>")
def cancel(session, op):
    try:
        gw("DELETE", f"/v1/sessions/{session}/operations/{op}/close")
    except (GatewayError, requests.RequestException) as e:
        return json_error(e)
    return jsonify({})


@app.post("/api/heartbeat/<session>")
def heartbeat(session):
    try:
        gw("POST", f"/v1/sessions/{session}/heartbeat")
    except (GatewayError, requests.RequestException) as e:
        return json_error(e, 410)
    return jsonify({})


@app.post("/api/close/<session>")
def close(session):
    try:
        gw("DELETE", f"/v1/sessions/{session}")
    except (GatewayError, requests.RequestException):
        pass  # already gone (idle timeout) — nothing to do
    return Response(status=204)


# ── Catalog browser ───────────────────────────────────────────────────────────
#
# Runs SHOW / DESCRIBE in the browser's own session, so the tree also shows that
# session's in-memory tables (purchase_stream, ...), which no other session can see.

def query_rows(session, sql, timeout=60):
    """Run one statement and return every row's fields. For bounded statements only."""
    op = gw("POST", f"/v1/sessions/{session}/statements", {"statement": sql})["operationHandle"]
    path, rows, columns, deadline = result_path(session, op), [], [], time.time() + timeout
    try:
        while path:
            res = gw("GET", path)
            if res.get("resultType") == "EOS":
                break
            if time.time() > deadline:
                raise GatewayError(f"timed out after {timeout}s: {sql}")
            results = res.get("results") or {}
            columns = [c["name"] for c in results.get("columns", [])] or columns
            rows += [r["fields"] for r in results.get("data", [])]
            if res.get("resultType") == "NOT_READY":
                time.sleep(0.2)
            path = res.get("nextResultUri")
    finally:
        gw("DELETE", f"/v1/sessions/{session}/operations/{op}/close")
    return columns, rows


def ident(*parts):
    return ".".join("`" + p.replace("`", "``") + "`" for p in parts)


@app.get("/api/catalog/<session>/current")
def catalog_current(session):
    """The session's current catalog and database: what an unqualified name in a DDL
    statement resolves against, so the browser knows which tree level it changed."""
    try:
        _, c = query_rows(session, "SHOW CURRENT CATALOG")
        _, d = query_rows(session, "SHOW CURRENT DATABASE")
    except (GatewayError, requests.RequestException) as e:
        return json_error(e)
    return jsonify({"catalog": c[0][0], "database": d[0][0]})


@app.get("/api/catalog/<session>/structure")
def catalog_structure(session):
    """One table's columns (DESCRIBE) and its DDL (SHOW CREATE), for the structure
    dialog. SHOW CREATE TABLE refuses a view, so a view falls back to SHOW CREATE VIEW."""
    c, d, t = (request.args.get(k) for k in ("catalog", "database", "table"))
    if not (c and d and t):
        return json_error("catalog, database and table are required", 400)
    try:
        names, rows = query_rows(session, f"DESCRIBE {ident(c, d, t)}")
        kind, ddl = "table", None
        try:
            _, out = query_rows(session, f"SHOW CREATE TABLE {ident(c, d, t)}")
        except GatewayError:
            kind = "view"
            _, out = query_rows(session, f"SHOW CREATE VIEW {ident(c, d, t)}")
        ddl = out[0][0] if out else None
    except (GatewayError, requests.RequestException) as e:
        return json_error(e)
    return jsonify({"kind": kind, "columns": [dict(zip(names, r)) for r in rows], "ddl": ddl})


@app.get("/api/catalog/<session>")
def catalog(session):
    """No arguments: the catalogs. ?catalog=: its databases. + &database=: its tables and
    views. + &table=: that table's columns, as DESCRIBE reports them."""
    c, d, t = (request.args.get(k) for k in ("catalog", "database", "table"))
    try:
        if not c:
            _, rows = query_rows(session, "SHOW CATALOGS")
            return jsonify({"items": [{"name": r[0]} for r in rows]})
        if not d:
            _, rows = query_rows(session, f"SHOW DATABASES IN {ident(c)}")
            return jsonify({"items": [{"name": r[0]} for r in rows]})
        if not t:
            # Views included: SHOW TABLES lists both. (USE would change the session's
            # current database.)
            _, rows = query_rows(session, f"SHOW TABLES IN {ident(c, d)}")
            return jsonify({"items": sorted(({"name": r[0]} for r in rows), key=lambda i: i["name"])})
        columns, rows = query_rows(session, f"DESCRIBE {ident(c, d, t)}")
        return jsonify({"items": [dict(zip(columns, r)) for r in rows]})
    except (GatewayError, requests.RequestException) as e:
        return json_error(e)


# ── Pipeline page (static/pipeline.html) ─────────────────────────────────────
#
# The architecture diagram as a control surface: each box is a table (a source, the model,
# a sink), each job box the Flink job that writes a sink. The page asks
# /api/pipeline/status what exists and what runs, and runs every action's SQL in the
# browser's own session, like the editor — the two share one session.

def flink(method, path, **kw):
    r = requests.request(method, FLINK + path, timeout=kw.pop("timeout", 30), **kw)
    if r.status_code >= 400:
        raise GatewayError(error_text(r.text))
    return r.json() if r.text else {}


ACTIVE = {"CREATED", "INITIALIZING", "RUNNING", "RESTARTING", "FAILING", "CANCELLING", "RECONCILING"}
DEFAULT_DB = (BUILTIN_CATALOG, "default_database")

# Box -> its object. `listed` is the name SHOW TABLES prints when it differs from the
# table's own (the JDBC catalog qualifies the schema); `probe` is the table the data
# probe reads when it is not the box's own (feedback_summary_sink is write-only in
# spirit: the rows are read back from PostgreSQL through dwh).
NODES = {
    "purchases":        {"table": DEFAULT_DB + ("purchase_stream",), "source": True},
    "master_product":   {"table": DEFAULT_DB + ("master_product",), "source": True},
    "feedback":         {"table": DEFAULT_DB + ("feedback",), "source": True},
    "model":            {"table": DEFAULT_DB + ("ai_analyze_review",), "model": True},
    "purchase_report":  {"table": ("dwh", "sales_report", "purchase_report"), "listed": "public.purchase_report",
                         "feed": "sales_job", "precreated": True},
    "feedback_summary": {"table": DEFAULT_DB + ("feedback_summary_sink",), "feed": "feedback_job",
                         "probe": ("dwh", "sales_report", "feedback_summary")},
}
# `name` is the job's pipeline.name: matched as well as the sink, in case a plan does not
# print its sink the way SINK_FULL expects.
EDGES = {
    "sales_job":    {"from": ["purchases", "master_product"], "to": "purchase_report",
                 "file": "jobs/10_sales.sql", "name": "coffee-1-sales"},
    "feedback_job": {"from": ["feedback", "model"], "to": "feedback_summary",
                 "file": "jobs/20_feedback.sql", "name": "coffee-2-feedback"},
}
# Pre-created on page load: purchase_report already exists in PostgreSQL, so Flink only
# needs the dwh catalog to reach it. Everything else attendees create from its box.
BOOTSTRAP = [("ddl/01_sources.sql", "PostgreSQL catalog")]

# ...unless the pipeline already uses them. A full ./start.sh submits the jobs through
# submit.sh, whose SQL Client sessions are the only ones that ever had these objects:
# they live in the in-memory default catalog. So a browser session gets them too, once
# the job using each one is running. box -> (file, section or None for all of it, edge)
SOURCES = {
    "purchases":        ("ddl/01_sources.sql", "Kafka: purchases", "sales_job"),
    "master_product":   ("ddl/01_sources.sql", "PostgreSQL: the product dimension", "sales_job"),
    "feedback":         ("ddl/01_sources.sql", "Kafka: feedback", "feedback_job"),
    "model":            ("ddl/02_models.sql", None, "feedback_job"),
    "feedback_summary": ("ddl/01_sources.sql", "PostgreSQL: the model output store", "feedback_job"),
}


def read_sql(rel):
    with open(os.path.join(SQL_DIR, rel)) as f:
        return f.read()


def section(rel, prefix):
    for title, body in load_sections(os.path.join(SQL_DIR, rel)):
        if title.startswith(prefix):
            return f"-- {title}\n{body}"
    raise KeyError(f"no '{prefix}' section in {rel}")


def explore(prefix):
    return section("explore.sql", prefix)


def without_header(sql):
    """A job file minus its leading comment block: the pop-up says what it is."""
    lines = sql.splitlines()
    i = 0
    while i < len(lines) and (lines[i].startswith("--") or not lines[i].strip()):
        i += 1
    return "\n".join(lines[i:]).strip() + "\n"


def without_banner(sql):
    """A file minus its `-- =====` header box, keeping the comments after it."""
    lines = sql.splitlines()
    if lines and lines[0].startswith("-- ="):
        end = next((i for i in range(1, len(lines)) if lines[i].startswith("-- =")), 0)
        lines = lines[end + 1:]
    return "\n".join(lines).strip() + "\n"


@app.get("/api/pipeline/sql")
def pipeline_sql():
    """Every action's default SQL, cut from flink_sql/ so the files stay the one source.
    Placeholders stay as written: /api/run fills them in, so the API key never reaches
    the browser."""
    view = lambda t, note: f"-- {note}\nSELECT * FROM {t};\n"
    batch = lambda note, q: (f"-- {note}\nSET 'execution.runtime-mode' = 'batch';\n{q}\n"
                             "SET 'execution.runtime-mode' = 'streaming';\n")
    return jsonify({
        "nodes": {
            "purchases": {"create": section("ddl/01_sources.sql", "Kafka: purchases"),
                          "view": view("purchase_stream", "Every purchase on the store.purchases topic. A streaming read: it keeps going until you cancel or close.")},
            "master_product": {"create": section("ddl/01_sources.sql", "PostgreSQL: the product dimension"),
                               "view": explore("Lab 1, Step 5")},
            "feedback": {"create": section("ddl/01_sources.sql", "Kafka: feedback"),
                         "view": view("feedback", "Every customer review on the store.feedback topic.")},
            "model": {"create": without_banner(read_sql("ddl/02_models.sql")),
                      "view": explore("The model, called interactively")},
            "purchase_report": {"view": batch(
                "purchase_report in PostgreSQL, through the dwh catalog: a bounded read, newest window first.",
                "SELECT * FROM dwh.sales_report.purchase_report ORDER BY purchase_time DESC, dim_item LIMIT 200;")},
            "feedback_summary": {"create": section("ddl/01_sources.sql", "PostgreSQL: the model output store"),
                                 "view": batch(
                "feedback_summary in PostgreSQL, read back through the dwh catalog: the model's verdicts, newest first.",
                "SELECT review_id, product_name, overall_score, summary, improvement_1, improvement_2, processed_at\n"
                "FROM dwh.sales_report.feedback_summary ORDER BY processed_at DESC LIMIT 200;")},
        },
        "edges": {e: {"sql": without_header(read_sql(spec["file"])) + RESET_NAME.lstrip("\n")}
                  for e, spec in EDGES.items()},
    })


def qualified(stmt):
    """A CREATE TABLE / CREATE MODEL in the default catalog, whatever the session's USE."""
    return re.sub(r"^CREATE\s+(TABLE|MODEL)\s+", lambda m: f"CREATE {m.group(1)} {ident(*DEFAULT_DB)}.", stmt, count=1, flags=re.I)


@app.post("/api/pipeline/bootstrap/<session>")
def pipeline_bootstrap(session):
    """Register what the pre-created boxes need (the dwh catalog, IF NOT EXISTS), and the
    objects a running pipeline already uses."""
    try:
        for rel, prefix in BOOTSTRAP:
            for stmt in split_statements(substitute(section(rel, prefix))):
                execute_and_wait(session, stmt)
        restored = restore_sources(session)
    except (GatewayError, requests.RequestException, OSError) as e:
        return json_error(e)
    return jsonify({"ok": True, "sources": restored})


# Which table a job writes, from its plan. Matching jobs to job boxes by what they write,
# not by name, keeps a job lit when an attendee edits pipeline.name in the pop-up. JDBC
# sinks print "Sink(table=[dwh.sales_report.purchase_report], ...".
SINK_FULL = re.compile(r"Sink\(table=\[([^\]]+)\]")
plan_sinks = {}


def job_sinks(jid):
    if jid not in plan_sinks:
        nodes = flink("GET", f"/jobs/{jid}/plan").get("plan", {}).get("nodes", [])
        text = " ".join(n.get("description", "") for n in nodes)
        plan_sinks[jid] = [s.replace("`", "") for s in SINK_FULL.findall(text)]
    return plan_sinks[jid]


def active_jobs():
    """Flink jobs that are up, each with the tables it writes (from its plan)."""
    active = [j for j in flink("GET", "/jobs/overview").get("jobs", [])
              if j["state"] in ACTIVE and j["state"] != "CANCELLING"]
    for j in active:
        try:
            j["sinks"] = job_sinks(j["jid"])
        except (GatewayError, requests.RequestException):
            j["sinks"] = []
    return active


def edge_jobs(active, edge):
    spec = EDGES[edge]
    table = ".".join(NODES[spec["to"]]["table"])
    return [j for j in active if table in j["sinks"] or j["name"] == spec["name"]]


def names(session, sql):
    try:
        return {r[0] for r in query_rows(session, sql)[1]}
    except GatewayError:
        return set()   # e.g. the catalog or database does not exist yet


def listed_in_default(session):
    """The default catalog's tables and models: what this session has created."""
    return (names(session, f"SHOW TABLES IN {ident(*DEFAULT_DB)}")
            | names(session, f"SHOW MODELS IN {ident(*DEFAULT_DB)}"))


def restore_sources(session):
    """Create in this session the objects whose job runs (see SOURCES). Returns the boxes
    it created. The DDL is plain CREATE, so only what the session lacks is created."""
    active = active_jobs()
    running = {e for e in EDGES if any(j["state"] == "RUNNING" for j in edge_jobs(active, e))}
    listed = listed_in_default(session)
    restored = []
    for node, (rel, prefix, edge) in SOURCES.items():
        if edge in running and NODES[node]["table"][2] not in listed:
            sql = section(rel, prefix) if prefix else read_sql(rel)
            for stmt in split_statements(substitute(sql)):
                execute_and_wait(session, qualified(stmt))
            restored.append(node)
    return restored


# ── The data probe: does a table return a row? ──
# The one status check that costs a Flink job, so it runs in the background, one at a
# time, in a session of its own, and only for a box whose feeding job runs and whose data
# is not confirmed yet. Confirmed boxes are not probed again. Both sinks are PostgreSQL
# tables read through dwh, so a probe is a bounded read that ends at once.
probe_lock = threading.Lock()
probe_wake = threading.Event()
probe_ok, probe_next, probe_wanted = set(), {}, {}
PROBE_TIMEOUT, PROBE_RETRY = 60, 15


def want_probe(node):
    with probe_lock:
        if node in probe_ok or node in probe_wanted or time.time() < probe_next.get(node, 0):
            return
        spec = NODES[node]
        probe_wanted[node] = f"SELECT 1 FROM {ident(*spec.get('probe', spec['table']))} LIMIT 1"
    probe_wake.set()


def first_row(session, sql):
    op = gw("POST", f"/v1/sessions/{session}/statements", {"statement": sql})["operationHandle"]
    path, deadline = result_path(session, op), time.time() + PROBE_TIMEOUT
    try:
        while path and time.time() < deadline:
            res = gw("GET", path)
            if (res.get("results") or {}).get("data"):
                return True
            if res.get("resultType") == "EOS":
                return False
            time.sleep(0.5)
            path = res.get("nextResultUri")
        return False
    finally:
        try:
            gw("DELETE", f"/v1/sessions/{session}/operations/{op}/close")
        except (GatewayError, requests.RequestException):
            pass


def probe_worker():
    session = None
    while True:
        probe_wake.wait()
        probe_wake.clear()
        while True:
            with probe_lock:
                if not probe_wanted:
                    break
                node, sql = next(iter(probe_wanted.items()))
            ok = False
            try:
                if session is None:
                    session = gw("POST", "/v1/sessions", {"sessionName": "pipeline-probe"})["sessionHandle"]
                    for rel, prefix in BOOTSTRAP:   # the dwh catalog, for the sinks
                        for stmt in split_statements(substitute(section(rel, prefix))):
                            execute_and_wait(session, stmt)
                    # A bounded read: batch mode ends the probe as soon as the table is read.
                    execute_and_wait(session, "SET 'execution.runtime-mode' = 'batch'")
                ok = first_row(session, sql)
            except (GatewayError, requests.RequestException):
                session = None   # gone or broken: a fresh one next time
            with probe_lock:
                probe_wanted.pop(node, None)
                if ok:
                    probe_ok.add(node)
                else:
                    probe_next[node] = time.time() + PROBE_RETRY


threading.Thread(target=probe_worker, daemon=True, name="pipeline-probe").start()


@app.get("/api/pipeline/status/<session>")
def pipeline_status(session):
    try:
        catalogs = names(session, "SHOW CATALOGS")
        listed = {
            BUILTIN_CATALOG: listed_in_default(session),
            "dwh": names(session, "SHOW TABLES IN `dwh`.`sales_report`") if "dwh" in catalogs else set(),
        }
    except requests.RequestException as e:
        return json_error(e)
    exists = {n: (spec.get("listed") or spec["table"][2]) in listed.get(spec["table"][0], set())
              for n, spec in NODES.items()}

    # Jobs, by the table they write.
    try:
        active = active_jobs()
    except (GatewayError, requests.RequestException):
        active = []
    edges = {}
    for e, spec in EDGES.items():
        jobs = [{"jid": j["jid"], "name": j["name"], "state": j["state"]} for j in edge_jobs(active, e)]
        states = {j["state"] for j in jobs}
        edges[e] = {"state": "running" if "RUNNING" in states else
                             "failing" if states & {"RESTARTING", "FAILING"} else
                             "starting" if jobs else "idle",
                    "jobs": jobs, "ready": all(exists[n] for n in spec["from"] + [spec["to"]])}

    nodes = {}
    for n, spec in NODES.items():
        feed = edges.get(spec.get("feed"), {}).get("state")
        if not exists[n]:
            with probe_lock:
                probe_ok.discard(n)   # dropped: whatever was confirmed is gone with it
            state = "inactive"
        elif feed == "running":
            want_probe(n)
            state = "live" if n in probe_ok else "starting"
        elif spec.get("model"):
            state = "live" if edges["feedback_job"]["state"] == "running" else "ready"
        elif spec.get("source"):
            state = "live"           # the topics and master_product: always flowing
        else:
            state = "ready"
        nodes[n] = {"state": state, "exists": exists[n]}

    missing = [n for n, spec in NODES.items() if spec.get("precreated") and not exists[n]]
    missing += [n for n, (_, _, e) in SOURCES.items() if not exists[n] and edges[e]["state"] == "running"]
    return jsonify({"nodes": nodes, "edges": edges, "bootstrap": bool(missing), "modelKey": bool(SECRET)})


# The data probe's jobs (want_probe): SELECT 1 FROM <table> LIMIT 1, as Flink names them.
PROBE_JOB = re.compile(r"\s*SELECT 1\s+FROM\b.*FETCH NEXT 1 ROWS ONLY\s*$", re.S | re.I)


@app.get("/api/jobs")
def jobs_list():
    """Every Flink job, newest first, for the catalog panel's Job Catalog: a pipeline job
    (with the table it writes, read from its plan) or an interactive query."""
    try:
        jobs = flink("GET", "/jobs/overview").get("jobs", [])
    except (GatewayError, requests.RequestException) as e:
        return json_error(f"could not reach the Flink JobManager: {e}")
    out = []
    for j in sorted(jobs, key=lambda j: j.get("start-time", 0), reverse=True):
        name, state = j["name"], j["state"]
        if PROBE_JOB.match(name):
            continue   # the pipeline page's own "does this table return a row?" checks
        kind = "query" if name.lstrip().upper().startswith("SELECT") else "job"
        sinks = []
        if kind == "job" and state in ACTIVE:
            try:
                sinks = job_sinks(j["jid"])
            except (GatewayError, requests.RequestException):
                pass
        out.append({"jid": j["jid"], "name": " ".join(name.split()), "state": state, "kind": kind,
                    "sinks": sinks, "start": j.get("start-time"), "duration": j.get("duration"),
                    "active": state in ACTIVE})
    return jsonify({"jobs": out})


@app.post("/api/jobs/<jid>/cancel")
def job_cancel(jid):
    try:
        flink("PATCH", f"/jobs/{jid}?mode=cancel")
    except (GatewayError, requests.RequestException) as e:
        return json_error(e)
    return jsonify({})


@app.get("/api/examples")
def examples():
    return jsonify(load_examples())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8084, threaded=True)
