#!/usr/bin/env python3
"""
Shopping Sessions pipeline in DuckDB. Implements rules.md exactly.

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)
"""
import json
import os
import sys
import threading
import time
from datetime import datetime, timezone

import duckdb

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))

T0 = time.time()


def log(msg):
    print(f"[{time.time() - T0:7.2f}s] {msg}", file=sys.stderr, flush=True)


def to_ms(iso):
    return int(datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()) * 1000


def sql_str(s):
    return "'" + s.replace("'", "''") + "'"


def csv_files(sub):
    d = os.path.join(DATA, sub)
    if not os.path.isdir(d):
        return []
    return sorted(os.path.join(d, n) for n in os.listdir(d)
                  if n.endswith(".csv") and os.path.isfile(os.path.join(d, n)))


SLOW_FILES = set()


def find_slow_files(files):
    """DuckDB's CSV reader treats a bare \\r as a line break, and the fast reader
    uses \\x02 as its null string. Files holding either byte are read
    byte-exactly with read_text instead (the generator writes neither).
    One parallel read_text scan finds them."""
    files = [f for f in files if os.path.getsize(f) > 0]
    if not files:
        return
    lst = "[" + ",".join(sql_str(f) for f in files) + "]"
    for (name,) in con.execute(f"SELECT filename FROM read_text({lst}) "
                               f"WHERE contains(content, chr(13)) OR contains(content, chr(2))").fetchall():
        SLOW_FILES.add(os.path.abspath(name))


def field_regex(n):
    fld = '("[^",]*"|[^",]*)'
    return "^" + fld + ("(," + fld + "){%d}" % (n - 1) if n > 1 else "") + "$"


def exact_sql(path, names):
    """Byte-exact path: split the file on \\n ourselves, validate each line by regex."""
    n = len(names)
    cols = ", ".join(f"NULLIF(f[{i + 1}], '') AS {c}" for i, c in enumerate(names))
    return (f"SELECT {cols} FROM (SELECT string_split(replace(line, '\"', ''), ',') AS f FROM "
            f"(SELECT unnest(string_split(content, chr(10))[2:]) AS line FROM read_text({sql_str(path)})) "
            f"WHERE regexp_full_match(line, '{field_regex(n)}'))")


def native_sql(paths, names):
    """Fast path, DuckDB's parallel CSV reader with quoting off, so it only splits
    on ',' and '\\n'. One extra column plus null_padding and a null string that
    never occurs tell the field count apart: a line is well-formed iff its last
    real column is present and the extra one is absent (lines with two or more
    extra fields are rejected by strict_mode and skipped by ignore_errors; empty
    lines are skipped). Quotes are then checked and stripped per field."""
    lst = "[" + ",".join(sql_str(p) for p in paths) + "]"
    cols = ", ".join(f"'{c}':'VARCHAR'" for c in names) + ", '_extra':'VARCHAR'"
    src = (f"read_csv({lst}, columns={{{cols}}}, delim=',', quote='', escape='', comment='', header=true, "
           f"new_line='\\n', auto_detect=false, nullstr='\\x02', strict_mode=true, ignore_errors=true, "
           f"null_padding=true)")
    strip = ", ".join(
        f"CASE WHEN length({c}) >= 2 AND starts_with({c}, '\"') AND ends_with({c}, '\"') "
        f"THEN substr({c}, 2, length({c}) - 2) ELSE {c} END AS {c}" for c in names)
    out = ", ".join(f"NULLIF({c}, '') AS {c}" for c in names)
    return (f"SELECT {out} FROM (SELECT {strip} FROM {src} WHERE {names[-1]} IS NOT NULL AND _extra IS NULL) "
            f"WHERE strpos(concat({', '.join(names)}), '\"') = 0")


def parsed_sql(files, names):
    """Apply the CSV dialect: drop malformed lines, strip quotes, '' -> NULL."""
    fast = [f for f in files if os.path.abspath(f) not in SLOW_FILES]
    slow = [f for f in files if os.path.abspath(f) in SLOW_FILES]
    parts = ([native_sql(fast, names)] if fast else []) + [exact_sql(f, names) for f in slow]
    if not parts:
        return "SELECT " + ", ".join(f"NULL::VARCHAR AS {c}" for c in names) + " WHERE false"
    return " UNION ALL ".join(f"({p})" for p in parts)


class Profiled:
    """With PROFILE_DIR set, writes DuckDB's operator timings for every CREATE/COPY
    statement to PROFILE_DIR/NN.txt. Off by default."""

    def __init__(self, con, out_dir):
        self.con, self.dir, self.n = con, out_dir, 0
        os.makedirs(out_dir, exist_ok=True)
        con.execute("PRAGMA enable_profiling = 'query_tree'")

    def execute(self, q, *a):
        if q.lstrip().startswith(("CREATE TABLE", "CREATE OR REPLACE TABLE", "COPY")):
            self.n += 1
            self.con.execute(f"SET profiling_output = '{self.dir}/{self.n:02d}.txt'")
        return self.con.execute(q, *a)

    def cursor(self):
        return self


con = duckdb.connect()
if os.environ.get("PROFILE_DIR"):
    con = Profiled(con, os.environ["PROFILE_DIR"])
con.execute(f"SET threads = {NCPU}")
con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.75))}GB'")
try:
    os.makedirs(os.path.join(SCRATCH, 'duckdb_tmp'), exist_ok=True)
except OSError:
    pass
con.execute(f"SET temp_directory = '{os.path.join(SCRATCH, 'duckdb_tmp')}'")
con.execute("SET preserve_insertion_order = false")

with open(os.path.join(DATA, "manifest.json")) as f:
    manifest = json.load(f)
WS = to_ms(manifest["window_start"])
WE = to_ms(manifest["window_end"])
DAY = 86_400_000
WEEK = 7 * DAY
GAP = 1_800_000
log(f"window {manifest['window_start']} .. {manifest['window_end']}")
find_slow_files(csv_files("events") + csv_files("clicks") + csv_files("links")
                + [os.path.join(DATA, "products.csv"), os.path.join(DATA, "campaigns.csv")])
log(f"files needing the exact reader: {len(SLOW_FILES)}")

# ---------------------------------------------------------------- timestamp macros
# F1/F2: the regex fixes the shape; hour/minute/second ranges are checked as
# two-digit strings, and DuckDB's own timestamp cast does the calendar (it rejects
# month 13, Feb 30, Feb 29 outside leap years). Checked equal to an independent
# civil-date implementation on ~4M crafted strings (all years 0001-9999 tested).
con.execute(r"""
CREATE MACRO ts_base(s) AS CASE
    WHEN substr(s, 12, 2) <= '23' AND substr(s, 15, 2) <= '59' AND substr(s, 18, 2) <= '59'
    THEN epoch_ms(TRY_CAST(substr(s, 1, 19) AS TIMESTAMP)) END
""")
con.execute(r"""
CREATE MACRO parse_ts(s) AS CASE
    WHEN regexp_full_match(s, '[0-9]+') THEN TRY_CAST(ltrim(s, '0') AS BIGINT)
    WHEN length(s) = 20 AND regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z')
        THEN ts_base(s)
    WHEN length(s) = 25
         AND regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}')
         AND substr(s, 21, 2) <= '23' AND substr(s, 24, 2) <= '59'
        THEN ts_base(s) - CASE WHEN substr(s, 20, 1) = '+' THEN 1 ELSE -1 END
                         * (substr(s, 21, 2)::BIGINT * 60 + substr(s, 24, 2)::BIGINT) * 60000
    END
""")

# ---------------------------------------------------------------- small dimension tables
con.execute(f"CREATE TABLE products AS SELECT product_id, any_value(category) AS category FROM "
            f"({parsed_sql([os.path.join(DATA, 'products.csv')], ['product_id', 'category'])}) "
            f"WHERE product_id IS NOT NULL GROUP BY product_id")
con.execute(f"CREATE TABLE campaigns AS SELECT campaign_id, any_value(channel) AS channel FROM "
            f"({parsed_sql([os.path.join(DATA, 'campaigns.csv')], ['campaign_id', 'channel'])}) "
            f"WHERE campaign_id IS NOT NULL GROUP BY campaign_id")

# ---------------------------------------------------------------- the three independent inputs
def build_links(con):
    """Linked IDs -> idmap(id, person)."""
    con.execute(f"CREATE TABLE links_raw AS SELECT a, b FROM ({parsed_sql(csv_files('links'), ['a', 'b'])}) "
                f"WHERE a IS NOT NULL AND b IS NOT NULL")
    # Integer ids in byte order of the string ids, so the smallest int names the person.
    con.execute("""
    CREATE TABLE ids AS
    SELECT id, (row_number() OVER (ORDER BY id))::INT AS n
    FROM (SELECT a AS id FROM links_raw UNION SELECT b FROM links_raw)
    """)
    con.execute("""
    CREATE TABLE edges AS
    SELECT DISTINCT u, v FROM (
        SELECT ia.n AS u, ib.n AS v FROM links_raw JOIN ids ia ON ia.id = a JOIN ids ib ON ib.id = b
        UNION ALL
        SELECT ib.n, ia.n FROM links_raw JOIN ids ia ON ia.id = a JOIN ids ib ON ib.id = b
    ) WHERE u <> v
    """)
    con.execute("CREATE TABLE lab AS SELECT n AS v, n AS l FROM ids")
    it = 0
    while True:
        it += 1
        # Hook: every vertex and its current label take the smallest neighbour label;
        # then shortcut (pointer jumping) twice. Labels only ever decrease and stay
        # within the component, and the fixed point is the component minimum.
        con.execute("""
        CREATE OR REPLACE TABLE lab2 AS
        WITH cand AS (
            SELECT v AS x, l AS c FROM lab
            UNION ALL
            SELECT e.u, lv.l FROM edges e JOIN lab lv ON lv.v = e.v
            UNION ALL
            SELECT lu.l, lv.l FROM edges e JOIN lab lu ON lu.v = e.u JOIN lab lv ON lv.v = e.v
        ), m AS (SELECT x AS v, min(c) AS l FROM cand GROUP BY x),
        j1 AS (SELECT a.v, b.l FROM m a JOIN m b ON b.v = a.l)
        SELECT a.v, b.l FROM j1 a JOIN j1 b ON b.v = a.l
        """)
        changed = con.execute("SELECT count(*) FROM lab JOIN lab2 USING (v) WHERE lab.l <> lab2.l").fetchone()[0]
        con.execute("DROP TABLE lab")
        con.execute("ALTER TABLE lab2 RENAME TO lab")
        if changed == 0:
            break
    con.execute("""
    CREATE TABLE idmap AS
    SELECT i.id, r.id AS person FROM lab JOIN ids i ON i.n = lab.v JOIN ids r ON r.n = lab.l
    """)
    log(f"links: {con.execute('SELECT count(*) FROM idmap').fetchone()[0]} ids, {it} iterations")


# Parsed rows feed macros that name a column many times, and DuckDB inlines the
# quote-stripping expression at every mention. Materializing the parsed rows
# computes it once: a clear win on small inputs, a loss on very large ones (the
# copy outweighs it). Results are identical either way.
EVENT_BYTES = sum(os.path.getsize(f) for f in csv_files("events"))
MAT = "MATERIALIZED " if EVENT_BYTES < (1 << 30) else ""


def build_events(con):
    """Events -> ev_kept(user_id, ts, is_p, price, product_id): deduped, valid."""
    # Every parsed copy is typed and judged on its own (`ok`), then dedup keeps the
    # copy with the smallest ingest_ts and the event survives only if that copy is ok.
    # ingest_ts is parsed with the same exact parser (always valid F1 per the rules).
    con.execute(f"""
    CREATE TABLE ev_kept AS
    WITH raw AS {MAT}(
        SELECT * FROM ({parsed_sql(csv_files('events'),
            ['event_id', 'user_id', 'ts', 'ingest_ts', 'event_type', 'product_id', 'price_cents'])})
        WHERE event_id IS NOT NULL
    ), typed AS (
        SELECT event_id, user_id, product_id, event_type = 'purchase' AS is_p,
               parse_ts(ts) AS t, parse_ts(ingest_ts) AS ing,
               CASE WHEN event_type = 'purchase' AND regexp_full_match(price_cents, '[0-9]+')
                    THEN TRY_CAST(ltrim(price_cents, '0') AS BIGINT) END AS price,
               event_type
        FROM raw
    ), judged AS (
        SELECT event_id, ing, user_id, t, is_p, price, product_id,
               coalesce(user_id IS NOT NULL AND t >= {WS} AND t < {WE}
                        AND event_type IN ('view', 'cart', 'purchase') AND product_id IS NOT NULL
                        AND (NOT is_p OR price BETWEEN 1 AND 10000000)
                        AND ing - t <= {DAY}, false) AS ok
        FROM typed
    ), kept AS (
        -- copies with equal ingest_ts are identical, so any one of them may win
        SELECT * FROM judged QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ing) = 1
    )
    SELECT user_id, t AS ts, is_p, price, product_id FROM kept WHERE ok
    """)
    log(f"events valid: {con.execute('SELECT count(*) FROM ev_kept').fetchone()[0]}")


def build_clicks(con):
    """Clicks -> clk_raw(user_id, click_id, campaign_id, ts): valid."""
    con.execute(f"""
    CREATE TABLE clk_raw AS
    WITH raw AS {MAT}(
        SELECT * FROM ({parsed_sql(csv_files('clicks'), ['click_id', 'user_id', 'campaign_id', 'ts'])})
        WHERE click_id IS NOT NULL
    ), typed AS (
        -- No dedup on click_id: copies of a click are identical (rules), and every use
        -- below (latest click with smallest id, distinct campaigns, distinct
        -- (person, campaign, ts)) gives the same answer with or without copies.
        SELECT click_id, user_id, campaign_id, parse_ts(ts) AS ts FROM raw
    )
    SELECT t.user_id, t.click_id, t.campaign_id, t.ts
    FROM typed t
    WHERE t.user_id IS NOT NULL AND t.ts >= {WS - WEEK} AND t.ts < {WE}
      AND t.campaign_id IN (SELECT campaign_id FROM campaigns)
    """)
    log(f"clicks valid: {con.execute('SELECT count(*) FROM clk_raw').fetchone()[0]}")


def run_parallel(*fns):
    """Run independent steps at once on their own cursors. DuckDB releases the GIL
    and schedules all of them on one shared thread pool, so small inputs no longer
    leave threads idle. Profiling runs them one after another."""
    if isinstance(con, Profiled):
        for fn in fns:
            fn(con)
        return
    errors = []

    def wrap(fn):
        try:
            fn(con.cursor())
        except BaseException as e:  # re-raised below, on the main thread
            errors.append(e)

    threads = [threading.Thread(target=wrap, args=(fn,)) for fn in fns]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise errors[0]


run_parallel(build_links, build_events, build_clicks)

# Person for each event (linked IDs map to their component's name), done inside
# the two readers below instead of materializing another 60M-row table.
EV = "(SELECT coalesce(m.person, k.user_id) AS person, k.ts, k.is_p, k.price, k.product_id " \
     "FROM ev_kept k LEFT JOIN idmap m ON m.id = k.user_id)"

def build_sessions(con):
    """sess(person, sno, start, day, n_events), purch, sess_rev."""
    # Sessionize per person in one sort, with frame-free window functions only. Each
    # event gets its previous/next ts and row number; only session starts (first event,
    # or gap > 30 min) and each person's last event are kept, and a session's event
    # count is the row-number difference to the boundary after it. Equal timestamps
    # have gap 0 and so share a session.
    con.execute(f"""
    CREATE TABLE sess AS
    WITH w AS (
        SELECT person, ts, lag(ts) OVER o AS prev, lead(ts) OVER o AS nxt, row_number() OVER o AS rn
        FROM {EV} WINDOW o AS (PARTITION BY person ORDER BY ts)
    ), edges AS (
        -- kind 0: a session start, with the count of events before it
        -- kind 1: the person's last event, with the count through it
        SELECT person, ts, kind, CASE WHEN kind = 0 THEN rn - 1 ELSE rn END AS c_n
        FROM (SELECT *, unnest(CASE WHEN is_start AND nxt IS NULL THEN [0, 1]
                                    WHEN is_start THEN [0] ELSE [1] END) AS kind
              FROM (SELECT *, (prev IS NULL OR ts - prev > {GAP}) AS is_start FROM w)
              WHERE is_start OR nxt IS NULL)
    ), spans AS (
        SELECT person, ts AS start, kind, lead(c_n) OVER o - c_n AS n_events,
               sum(1 - kind) OVER (o ROWS UNBOUNDED PRECEDING) AS sno
        FROM edges WINDOW o AS (PARTITION BY person ORDER BY c_n)
    )
    SELECT person, sno::BIGINT AS sno, start, (DATE '1970-01-01' + (start // {DAY})::INT) AS day, n_events
    FROM spans WHERE kind = 0
    """)
    # Purchases (a few % of events), each tied to its session: the latest start at or before it.
    con.execute(f"""
    CREATE TABLE purch AS
    SELECT e.person, s.sno, s.day, e.price, e.product_id
    FROM (SELECT coalesce(m.person, k.user_id) AS person, k.ts, k.price, k.product_id
          FROM ev_kept k LEFT JOIN idmap m ON m.id = k.user_id WHERE k.is_p) e
    ASOF JOIN sess s ON e.person = s.person AND e.ts >= s.start
    """)
    con.execute("DROP TABLE ev_kept")
    con.execute("CREATE TABLE sess_rev AS SELECT person, sno, sum(price) AS revenue FROM purch GROUP BY person, sno")
    log(f"sessions: {con.execute('SELECT count(*) FROM sess').fetchone()[0]}")


def build_clk(con):
    """clk(person, click_id, campaign_id, ts)."""
    # Person for each click (linked IDs map to their component's name).
    con.execute("""
    CREATE TABLE clk AS
    SELECT coalesce(m.person, c.user_id) AS person, c.click_id, c.campaign_id, c.ts
    FROM clk_raw c LEFT JOIN idmap m ON m.id = c.user_id
    """)
    con.execute("DROP TABLE clk_raw")


def build_credit(con):
    """credit(day, has_p, revenue, campaign_id): one row per session."""
    # Credit: latest click at or before the start, within 7 days; ties -> smallest click_id.
    con.execute(f"""
    CREATE TABLE credit AS
    WITH cpt AS (
        SELECT person, ts, arg_min(campaign_id, click_id) AS campaign_id FROM clk GROUP BY person, ts
    )
    SELECT x.day, r.revenue IS NOT NULL AS has_p, coalesce(r.revenue, 0) AS revenue, x.campaign_id
    FROM (
        SELECT s.person, s.sno, s.day,
               CASE WHEN c.ts IS NOT NULL AND s.start - c.ts <= {WEEK} THEN c.campaign_id ELSE 'organic' END AS campaign_id
        FROM sess s ASOF LEFT JOIN cpt c ON s.person = c.person AND s.start >= c.ts
    ) x LEFT JOIN sess_rev r ON r.person = x.person AND r.sno = x.sno
    """)


def build_assist(con):
    """assist(campaign_id, day, assisted_sessions)."""
    # Assists. A click covers session starts in [ts, ts + 7d]; per (person, campaign)
    # the covers are merged into disjoint intervals with frame-free windows (keep the
    # rows that open or close a run of clicks under 7 days apart, pair them with lead).
    # Two ASOF joins turn an interval into the range [a, b] of session numbers whose
    # start falls inside. Session numbers are consecutive per person and days never
    # decrease along them, so instead of expanding every session in [a, b], each
    # interval adds its overlap with each of the person's per-day session ranges.
    con.execute(f"""
    CREATE TABLE assist AS
    WITH ct AS (SELECT DISTINCT person, campaign_id, ts FROM clk),
    w AS (
        SELECT person, campaign_id, ts, lag(ts) OVER o AS prev, lead(ts) OVER o AS nxt
        FROM ct WINDOW o AS (PARTITION BY person, campaign_id ORDER BY ts)
    ),
    b AS (
        SELECT person, campaign_id, ts,
               prev IS NULL OR ts - prev > {WEEK} AS opens,
               nxt IS NULL OR nxt - ts > {WEEK} AS closes
        FROM w WHERE prev IS NULL OR ts - prev > {WEEK} OR nxt IS NULL OR nxt - ts > {WEEK}
    ),
    iv AS (
        -- an opening row is paired with the closing row of its run: itself, or the next kept row
        SELECT person, campaign_id, lo, hi + {WEEK} AS hi FROM (
            SELECT person, campaign_id, opens, ts AS lo,
                   CASE WHEN closes THEN ts ELSE lead(ts) OVER o END AS hi
            FROM b WINDOW o AS (PARTITION BY person, campaign_id ORDER BY ts)
        ) WHERE opens
    ),
    ab AS (
        SELECT x.person, x.campaign_id, x.first_s, s.sno AS last_s FROM (
            SELECT iv.*, s.sno AS first_s FROM iv ASOF JOIN sess s ON iv.person = s.person AND iv.lo <= s.start
        ) x ASOF JOIN sess s ON x.person = s.person AND x.hi >= s.start
        WHERE x.first_s <= s.sno
    ),
    pd AS (SELECT person, day, min(sno) AS lo, max(sno) AS hi FROM sess GROUP BY person, day)
    SELECT ab.campaign_id, pd.day,
           sum(least(ab.last_s, pd.hi) - greatest(ab.first_s, pd.lo) + 1) AS assisted_sessions
    FROM ab JOIN pd ON pd.person = ab.person AND pd.lo <= ab.last_s AND pd.hi >= ab.first_s
    GROUP BY ab.campaign_id, pd.day
    """)


# ---------------------------------------------------------------- outputs
ud_dir = os.path.join(OUT, "user_daily")
cd_dir = os.path.join(OUT, "campaign_daily")
os.makedirs(ud_dir, exist_ok=True)
os.makedirs(cd_dir, exist_ok=True)

def write_user_daily(con):
    """Write /output/user_daily."""
    con.execute(f"""
    COPY (
        WITH base AS (
            SELECT person, day, count(*) AS sessions, sum(n_events) AS events FROM sess GROUP BY person, day
        ), cat AS (
            SELECT e.person, e.day, coalesce(p.category, 'unknown') AS category, sum(e.price) AS amt
            FROM purch e LEFT JOIN products p ON p.product_id = e.product_id
            GROUP BY ALL
        ), top AS (
            SELECT person, day, arg_min(category, (-amt, category)) AS top_category, sum(amt) AS revenue_cents
            FROM cat GROUP BY person, day
        )
        SELECT b.person AS user_id, b.day, b.sessions::BIGINT AS sessions, b.events::BIGINT AS events,
               coalesce(t.revenue_cents, 0)::BIGINT AS revenue_cents, t.top_category
        FROM base b LEFT JOIN top t USING (person, day)
    ) TO '{os.path.join(ud_dir, "part-0.parquet")}' (FORMAT parquet)
    """)
    log("wrote user_daily")


run_parallel(build_sessions, build_clk)
run_parallel(build_credit, build_assist, write_user_daily)


con.execute(f"""
COPY (
    WITH cr AS (
        SELECT campaign_id, day, count(*) AS sessions, count_if(has_p) AS purchasing_sessions,
               sum(revenue) AS revenue_cents
        FROM credit GROUP BY campaign_id, day
    )
    SELECT coalesce(cr.campaign_id, a.campaign_id) AS campaign_id,
           CASE WHEN coalesce(cr.campaign_id, a.campaign_id) = 'organic' THEN 'organic' ELSE c.channel END AS channel,
           coalesce(cr.day, a.day) AS day,
           coalesce(cr.sessions, 0)::BIGINT AS sessions,
           coalesce(cr.purchasing_sessions, 0)::BIGINT AS purchasing_sessions,
           coalesce(cr.revenue_cents, 0)::BIGINT AS revenue_cents,
           coalesce(a.assisted_sessions, 0)::BIGINT AS assisted_sessions
    FROM cr FULL OUTER JOIN assist a ON a.campaign_id = cr.campaign_id AND a.day = cr.day
    LEFT JOIN campaigns c ON c.campaign_id = coalesce(cr.campaign_id, a.campaign_id)
) TO '{os.path.join(cd_dir, "part-0.parquet")}' (FORMAT parquet)
""")
log("wrote campaign_daily")
