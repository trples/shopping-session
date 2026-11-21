#!/usr/bin/env python3
"""
Shopping Sessions pipeline in DuckDB. Implements rules.md exactly.

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)
"""
import json
import mmap
import os
import sys
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


def needs_exact_reader(path):
    """DuckDB's CSV reader treats a bare \\r as a line break, and our line reader
    uses \\x01 as its (never expected) delimiter. Files holding either byte are
    read byte-exactly with read_text instead. The generator writes neither."""
    if os.path.getsize(path) == 0:
        return False
    with open(path, "rb") as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as m:
        return m.find(b"\r") != -1 or m.find(b"\x01") != -1


def lines_sql(files):
    """SQL yielding one row per data line (header skipped) with column `line`."""
    fast = [f for f in files if not needs_exact_reader(f)]
    slow = [f for f in files if f not in set(fast)]
    parts = []
    if fast:
        lst = "[" + ",".join(sql_str(f) for f in fast) + "]"
        parts.append(
            f"SELECT line FROM read_csv({lst}, columns={{'line':'VARCHAR'}}, delim='\\x01', quote='', "
            f"escape='', header=true, new_line='\\n', auto_detect=false, strict_mode=false)")
    for f in slow:
        parts.append(f"SELECT unnest(string_split(content, chr(10))[2:]) AS line FROM read_text({sql_str(f)})")
    if not parts:
        return "SELECT NULL::VARCHAR AS line WHERE false"
    return " UNION ALL ".join(parts)


def field_regex(n):
    fld = '("[^",]*"|[^",]*)'
    return "^" + fld + ("(," + fld + "){%d}" % (n - 1) if n > 1 else "") + "$"


def parsed_sql(files, names):
    """Apply the CSV dialect: drop malformed lines, strip quotes, '' -> NULL."""
    n = len(names)
    cols = ", ".join(f"NULLIF(f[{i + 1}], '') AS {c}" for i, c in enumerate(names))
    return (f"SELECT {cols} FROM (SELECT string_split(replace(line, '\"', ''), ',') AS f "
            f"FROM ({lines_sql(files)}) WHERE regexp_full_match(line, '{field_regex(n)}'))")


con = duckdb.connect()
con.execute(f"SET threads = {NCPU}")
con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.75))}GB'")
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

# ---------------------------------------------------------------- timestamp macros
con.execute("""
CREATE MACRO days_civ(y, m, d) AS (
    ((y - (m <= 2)::INT) // 400) * 146097
    + ((y - (m <= 2)::INT) % 400) * 365
    + ((y - (m <= 2)::INT) % 400) // 4
    - ((y - (m <= 2)::INT) % 400) // 100
    + (153 * (m + CASE WHEN m > 2 THEN -3 ELSE 9 END) + 2) // 5 + d - 1
    - 719468)
""")
con.execute("""
CREATE MACRO dim(y, m) AS CASE
    WHEN m = 2 THEN CASE WHEN (y % 4 = 0 AND y % 100 <> 0) OR y % 400 = 0 THEN 29 ELSE 28 END
    WHEN m IN (4, 6, 9, 11) THEN 30 ELSE 31 END
""")
con.execute("""
CREATE MACRO civ_ms(y, mo, d, h, mi, se) AS CASE
    WHEN mo BETWEEN 1 AND 12 AND d BETWEEN 1 AND dim(y, mo) AND h <= 23 AND mi <= 59 AND se <= 59
    THEN (days_civ(y, mo, d) * 86400 + h * 3600 + mi * 60 + se) * 1000 END
""")
con.execute("""
CREATE MACRO iso_ms(s) AS civ_ms(substr(s, 1, 4)::BIGINT, substr(s, 6, 2)::BIGINT, substr(s, 9, 2)::BIGINT,
                                 substr(s, 12, 2)::BIGINT, substr(s, 15, 2)::BIGINT, substr(s, 18, 2)::BIGINT)
""")
con.execute(r"""
CREATE MACRO parse_ts(s) AS CASE
    WHEN regexp_full_match(s, '[0-9]+') THEN TRY_CAST(ltrim(s, '0') AS BIGINT)
    WHEN length(s) = 20 AND regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z')
        THEN iso_ms(s)
    WHEN length(s) = 25
         AND regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}')
         AND substr(s, 21, 2)::INT <= 23 AND substr(s, 24, 2)::INT <= 59
        THEN iso_ms(s) - CASE WHEN substr(s, 20, 1) = '+' THEN 1 ELSE -1 END
                         * (substr(s, 21, 2)::BIGINT * 3600 + substr(s, 24, 2)::BIGINT * 60) * 1000
    END
""")

# ---------------------------------------------------------------- small dimension tables
con.execute(f"CREATE TABLE products AS SELECT product_id, any_value(category) AS category FROM "
            f"({parsed_sql([os.path.join(DATA, 'products.csv')], ['product_id', 'category'])}) "
            f"WHERE product_id IS NOT NULL GROUP BY product_id")
con.execute(f"CREATE TABLE campaigns AS SELECT campaign_id, any_value(channel) AS channel FROM "
            f"({parsed_sql([os.path.join(DATA, 'campaigns.csv')], ['campaign_id', 'channel'])}) "
            f"WHERE campaign_id IS NOT NULL GROUP BY campaign_id")

# ---------------------------------------------------------------- linked IDs
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

# ---------------------------------------------------------------- events
con.execute(f"""
CREATE TABLE ev_raw AS SELECT * FROM ({parsed_sql(csv_files('events'),
    ['event_id', 'user_id', 'ts', 'ingest_ts', 'event_type', 'product_id', 'price_cents'])})
WHERE event_id IS NOT NULL
""")
log(f"events parsed: {con.execute('SELECT count(*) FROM ev_raw').fetchone()[0]}")
# Dedup: the copy with the smallest ingest_ts (always F1, so string order is time order).
con.execute(f"""
CREATE TABLE ev AS
WITH kept AS (
    SELECT arg_min(struct_pack(user_id, ts, ingest_ts, event_type, product_id, price_cents), ingest_ts) AS r
    FROM ev_raw GROUP BY event_id
), typed AS (
    SELECT r.user_id, parse_ts(r.ts) AS ts, iso_ms(r.ingest_ts) AS ing, r.event_type, r.product_id,
           CASE WHEN r.event_type = 'purchase' AND regexp_full_match(r.price_cents, '[0-9]+')
                THEN TRY_CAST(ltrim(r.price_cents, '0') AS BIGINT) END AS price
    FROM kept
)
SELECT coalesce(m.person, t.user_id) AS person, t.ts, t.event_type = 'purchase' AS is_p,
       CASE WHEN t.event_type = 'purchase' THEN t.price END AS price, t.product_id
FROM typed t LEFT JOIN idmap m ON m.id = t.user_id
WHERE t.user_id IS NOT NULL AND t.ts >= {WS} AND t.ts < {WE}
  AND t.event_type IN ('view', 'cart', 'purchase') AND t.product_id IS NOT NULL
  AND (t.event_type <> 'purchase' OR t.price BETWEEN 1 AND 10000000)
  AND t.ing - t.ts <= {DAY}
""")
con.execute("DROP TABLE ev_raw")
log(f"events valid: {con.execute('SELECT count(*) FROM ev').fetchone()[0]}")

# Sessionize per person.
con.execute(f"""
CREATE TABLE evs AS
SELECT person, ts, is_p, price, product_id,
       (sum(nw) OVER (PARTITION BY person ORDER BY ts ROWS UNBOUNDED PRECEDING))::BIGINT AS sno
FROM (SELECT *, CASE WHEN ts - lag(ts) OVER (PARTITION BY person ORDER BY ts) <= {GAP} THEN 0 ELSE 1 END AS nw
      FROM ev)
""")
con.execute("DROP TABLE ev")
con.execute(f"""
CREATE TABLE sess AS
SELECT person, sno, min(ts) AS start, (DATE '1970-01-01' + (min(ts) // {DAY})::INT) AS day,
       count(*) AS n_events, coalesce(sum(price), 0) AS revenue, bool_or(is_p) AS has_p
FROM evs GROUP BY person, sno
""")
log(f"sessions: {con.execute('SELECT count(*) FROM sess').fetchone()[0]}")

# ---------------------------------------------------------------- clicks
con.execute(f"""
CREATE TABLE clk AS
WITH raw AS (
    SELECT * FROM ({parsed_sql(csv_files('clicks'), ['click_id', 'user_id', 'campaign_id', 'ts'])})
    WHERE click_id IS NOT NULL
), dd AS (
    SELECT click_id, any_value(struct_pack(user_id, campaign_id, ts)) AS r FROM raw GROUP BY click_id
), typed AS (
    SELECT click_id, r.user_id, r.campaign_id, parse_ts(r.ts) AS ts FROM dd
)
SELECT coalesce(m.person, t.user_id) AS person, t.click_id, t.campaign_id, t.ts
FROM typed t LEFT JOIN idmap m ON m.id = t.user_id
WHERE t.user_id IS NOT NULL AND t.ts >= {WS - WEEK} AND t.ts < {WE}
  AND t.campaign_id IN (SELECT campaign_id FROM campaigns)
""")
log(f"clicks valid: {con.execute('SELECT count(*) FROM clk').fetchone()[0]}")

# Credit: latest click at or before the start, within 7 days; ties -> smallest click_id.
con.execute(f"""
CREATE TABLE credit AS
WITH cpt AS (
    SELECT person, ts, arg_min(campaign_id, click_id) AS campaign_id FROM clk GROUP BY person, ts
)
SELECT s.day, s.has_p, s.revenue,
       CASE WHEN c.ts IS NOT NULL AND s.start - c.ts <= {WEEK} THEN c.campaign_id ELSE 'organic' END AS campaign_id
FROM sess s ASOF LEFT JOIN cpt c ON s.person = c.person AND s.start >= c.ts
""")

# Assists: merge each (person, campaign)'s click coverage [ts, ts + 7d] into disjoint
# intervals, find the range of session numbers whose start falls inside, expand.
con.execute(f"""
CREATE TABLE assist AS
WITH ct AS (SELECT DISTINCT person, campaign_id, ts FROM clk),
g AS (
    SELECT person, campaign_id, ts,
           sum(CASE WHEN ts - prev <= {WEEK} THEN 0 ELSE 1 END)
               OVER (PARTITION BY person, campaign_id ORDER BY ts ROWS UNBOUNDED PRECEDING) AS grp
    FROM (SELECT *, lag(ts) OVER (PARTITION BY person, campaign_id ORDER BY ts) AS prev FROM ct)
),
iv AS (SELECT person, campaign_id, min(ts) AS lo, max(ts) + {WEEK} AS hi FROM g GROUP BY person, campaign_id, grp),
a AS (SELECT iv.*, s.sno AS first_s FROM iv ASOF JOIN sess s ON iv.person = s.person AND iv.lo <= s.start),
b AS (SELECT a.*, s.sno AS last_s FROM a ASOF JOIN sess s ON a.person = s.person AND a.hi >= s.start),
pairs AS (
    SELECT person, campaign_id, unnest(range(first_s, last_s + 1)) AS sno FROM b WHERE first_s <= last_s
)
SELECT p.campaign_id, s.day, count(*) AS assisted_sessions
FROM pairs p JOIN sess s ON s.person = p.person AND s.sno = p.sno
GROUP BY p.campaign_id, s.day
""")

# ---------------------------------------------------------------- outputs
ud_dir = os.path.join(OUT, "user_daily")
cd_dir = os.path.join(OUT, "campaign_daily")
os.makedirs(ud_dir, exist_ok=True)
os.makedirs(cd_dir, exist_ok=True)

con.execute(f"""
COPY (
    WITH base AS (
        SELECT person, day, count(*) AS sessions, sum(n_events) AS events, sum(revenue) AS revenue_cents
        FROM sess GROUP BY person, day
    ), cat AS (
        SELECT e.person, s.day, coalesce(p.category, 'unknown') AS category, sum(e.price) AS amt
        FROM evs e JOIN sess s ON s.person = e.person AND s.sno = e.sno
        LEFT JOIN products p ON p.product_id = e.product_id
        WHERE e.is_p
        GROUP BY ALL
    ), top AS (
        SELECT person, day, arg_min(category, (-amt, category)) AS top_category FROM cat GROUP BY person, day
    )
    SELECT b.person AS user_id, b.day, b.sessions::BIGINT AS sessions, b.events::BIGINT AS events,
           b.revenue_cents::BIGINT AS revenue_cents, t.top_category
    FROM base b LEFT JOIN top t USING (person, day)
) TO '{os.path.join(ud_dir, "part-0.parquet")}' (FORMAT parquet)
""")
log("wrote user_daily")

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
