#!/usr/bin/env python3
"""Slow, plain-Python reference of rules.md, written independently of the DuckDB
pipeline, for cross-checking on generated data. Usage: reference.py DATA OUT
Writes OUT/user_daily.csv and OUT/campaign_daily.csv in the fixtures' format."""
import bisect
import calendar
import csv
import glob
import json
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timezone

DIG = set("0123456789")


def isdig(s):
    return len(s) > 0 and all(c in DIG for c in s)


def parse_line(line, n):
    parts = line.split(",")
    if len(parts) != n:
        return None
    out = []
    for p in parts:
        if '"' in p:
            if len(p) >= 2 and p[0] == '"' and p[-1] == '"' and '"' not in p[1:-1]:
                p = p[1:-1]
            else:
                return None
        out.append(p if p != "" else None)
    return out


def read_rows(paths, n):
    for path in paths:
        with open(path, "rb") as f:
            data = f.read().decode("utf-8")
        lines = data.split("\n")[1:]
        for line in lines:
            r = parse_line(line, n)
            if r is not None:
                yield r


def parse_ts(s):
    if s is None:
        return None
    if isdig(s):
        return int(s)
    def civil(y, mo, d, h, mi, se):
        if y < 1 or not (1 <= mo <= 12) or h > 23 or mi > 59 or se > 59:
            return None  # year 0000 can never be inside a window
        if not (1 <= d <= calendar.monthrange(y, mo)[1]):
            return None
        return calendar.timegm((y, mo, d, h, mi, se)) * 1000
    def fields(s):
        if not (s[4] == "-" and s[7] == "-" and s[10] == "T" and s[13] == ":" and s[16] == ":"):
            return None
        parts = [s[0:4], s[5:7], s[8:10], s[11:13], s[14:16], s[17:19]]
        if not all(isdig(p) for p in parts):
            return None
        return [int(p) for p in parts]
    if len(s) == 20 and s[19] == "Z":
        f = fields(s)
        return civil(*f) if f else None
    if len(s) == 25 and s[19] in "+-" and s[22] == ":":
        f = fields(s)
        oh, om = s[20:22], s[23:25]
        if not f or not isdig(oh) or not isdig(om) or int(oh) > 23 or int(om) > 59:
            return None
        base = civil(*f)
        if base is None:
            return None
        off = (int(oh) * 3600 + int(om) * 60) * 1000
        return base - off if s[19] == "+" else base + off
    return None


def main():
    data, out = sys.argv[1], sys.argv[2]
    man = json.load(open(os.path.join(data, "manifest.json")))
    iso = lambda s: calendar.timegm(datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").timetuple()) * 1000
    WS, WE = iso(man["window_start"]), iso(man["window_end"])
    WEEK = 7 * 86400000
    products = {r[0]: r[1] for r in read_rows([os.path.join(data, "products.csv")], 2)}
    campaigns = {r[0]: r[1] for r in read_rows([os.path.join(data, "campaigns.csv")], 2)}
    g = lambda sub: sorted(glob.glob(os.path.join(data, sub, "*.csv")))

    # union-find
    parent = {}
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    for a, b in read_rows(g("links"), 2):
        if a is None or b is None:
            continue
        parent.setdefault(a, a); parent.setdefault(b, b)
        ra, rb = find(a), find(b)
        if ra != rb:
            if rb.encode() < ra.encode():
                ra, rb = rb, ra
            parent[rb] = ra
    person = lambda u: find(u) if u in parent else u

    best = {}
    for r in read_rows(g("events"), 7):
        if r[0] is None:
            continue
        if r[0] not in best or r[3] < best[r[0]][3]:
            best[r[0]] = r
    evs = defaultdict(list)
    for eid, u, ts, ing, et, pid, price in best.values():
        t = parse_ts(ts)
        if u is None or t is None or not (WS <= t < WE) or et not in ("view", "cart", "purchase") or pid is None:
            continue
        pr = None
        if et == "purchase":
            if price is None or not isdig(price) or not (1 <= int(price) <= 10_000_000):
                continue
            pr = int(price)
        if iso(ing) - t > 86400000:
            continue
        evs[person(u)].append((t, pr, pid))

    clicks = {}
    for r in read_rows(g("clicks"), 4):
        if r[0] is not None:
            clicks.setdefault(r[0], r)
    pclicks = defaultdict(list)
    for cid, u, camp, ts in clicks.values():
        t = parse_ts(ts)
        if u is None or t is None or not (WS - WEEK <= t < WE) or camp not in campaigns:
            continue
        pclicks[person(u)].append((t, cid.encode(), camp))
    for v in pclicks.values():
        v.sort()

    ud = defaultdict(lambda: [0, 0, 0, defaultdict(int)])
    cd = defaultdict(lambda: [0, 0, 0, 0])
    for p, lst in evs.items():
        lst.sort(key=lambda x: x[0])
        sessions = []
        prev = None
        for e in lst:
            if prev is None or e[0] - prev > 1_800_000:
                sessions.append([])
            sessions[-1].append(e)
            prev = e[0]
        cl = pclicks.get(p, [])
        cts = [c[0] for c in cl]
        for s in sessions:
            start = s[0][0]
            day = datetime.fromtimestamp(start / 1000, timezone.utc).date()
            rev = sum(e[1] for e in s if e[1] is not None)
            hasp = any(e[1] is not None for e in s)
            row = ud[(p, day)]
            row[0] += 1; row[1] += len(s); row[2] += rev
            for e in s:
                if e[1] is not None:
                    row[3][products.get(e[2], "unknown")] += e[1]
            hi = bisect.bisect_right(cts, start)
            lo = bisect.bisect_left(cts, start - WEEK)
            q = cl[lo:hi]
            if q:
                tmax = q[-1][0]
                win = min((c for c in q if c[0] == tmax), key=lambda c: c[1])
                camp = win[2]
            else:
                camp = "organic"
            r = cd[(camp, day)]
            r[0] += 1; r[1] += hasp; r[2] += rev
            for c in set(c[2] for c in q):
                cd[(c, day)][3] += 1

    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "user_daily.csv"), "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["user_id", "day", "sessions", "events", "revenue_cents", "top_category"])
        for (p, day), (n, ne, rev, cats) in ud.items():
            top = min(cats.items(), key=lambda kv: (-kv[1], kv[0].encode()))[0] if cats else ""
            w.writerow([p, day.isoformat(), n, ne, rev, top])
    with open(os.path.join(out, "campaign_daily.csv"), "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents",
                    "assisted_sessions"])
        for (c, day), (n, np_, rev, a) in cd.items():
            w.writerow([c, "organic" if c == "organic" else campaigns[c], day.isoformat(), n, np_, rev, a])


if __name__ == "__main__":
    main()
