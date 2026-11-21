#!/usr/bin/env python3
"""Approximate stand-in for the official generator (rules.md, "The generator"),
for local correctness cross-checks and timing. Not the real distribution.
Usage: gen.py --seed N --events E --clicks C --output DIR"""
import argparse
import json
import os
import random
from datetime import datetime, timedelta, timezone

ap = argparse.ArgumentParser()
ap.add_argument("--seed", type=int, default=1)
ap.add_argument("--events", type=int, default=100000)
ap.add_argument("--clicks", type=int, default=25000)
ap.add_argument("--output", required=True)
A = ap.parse_args()
R = random.Random(A.seed)
DAY, WEEK, MIN = 86_400_000, 7 * 86_400_000, 60_000

start_day = datetime(2025, 1, 1, tzinfo=timezone.utc) + timedelta(days=R.randrange(0, 4 * 365))
if R.random() < 0.3:
    start_day = datetime(2028, 2, 20, tzinfo=timezone.utc)  # covers Feb 29, 2028
WS = int(start_day.timestamp()) * 1000
WE = WS + 14 * DAY
os.makedirs(A.output, exist_ok=True)
with open(os.path.join(A.output, "manifest.json"), "w") as f:
    json.dump({"window_start": start_day.strftime("%Y-%m-%dT%H:%M:%SZ"),
               "window_end": (start_day + timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ")}, f)

CATS = [f"cat_{i:02d}" for i in range(20)]
NPROD = 50000 if A.events >= 1_000_000 else 2000
with open(os.path.join(A.output, "products.csv"), "w") as f:
    f.write("product_id,category\n")
    for i in range(NPROD):
        f.write(f"p_{i},{R.choice(CATS)}\n")
NCAMP = 2000 if A.events >= 1_000_000 else 200
CH = ["search", "social", "email", "display", "affiliate"]
camps = [f"c_{i}" for i in range(NCAMP)]
with open(os.path.join(A.output, "campaigns.csv"), "w") as f:
    f.write("campaign_id,channel\n")
    for c in camps:
        f.write(f"{c},{R.choice(CH)}\n")
whale_camps = R.sample(camps, R.randint(3, 5))

# ---- people and ids
npeople = max(10, A.events // 30)
uid = 0
def new_id():
    global uid
    uid += 1
    return f"u{uid:x}{R.choice('abcdefgh')}"

people = []  # list of id lists
links = []
for i in range(npeople):
    whale = i < 5
    r = R.random()
    k = R.randint(3, 6) if whale else (1 if r < .45 else 2 if r < .75 else 3 if r < .88 else R.randint(4, 8))
    if i in (5, 6, 7) or R.random() < 1 / 20000:
        k = R.randint(50, 400)
        ids = [new_id() for _ in range(k)]
        order = ids[:]
        R.shuffle(order)
        for a, b in zip(order, order[1:]):
            links.append((a, b))
    else:
        ids = [new_id() for _ in range(k)]
        for j in range(1, k):
            links.append((ids[j], ids[R.randrange(j)]))
    if R.random() < 0.05:
        extra = new_id()
        links.append((extra, R.choice(ids)))
    if R.random() < 0.01:
        links.append((ids[0], ids[0]))
    people.append(ids)
devices = [new_id() for _ in range(R.randint(3, 6))]
for p in people:
    if R.random() < 0.004:
        d = R.choice(devices)
        p.append(d)
        links.append((d, p[0]))

weights = [1.0 / (i + 1) ** 0.9 for i in range(npeople)]
tot = sum(weights[5:])
for i in range(5):
    weights[i] = tot * R.uniform(0.01, 0.02) / 0.95
counts = [0] * npeople
for i in R.choices(range(npeople), weights=weights, k=A.events):
    counts[i] += 1

def pick_id(p):
    return p[0] if R.random() < 0.5 else R.choice(p)

def upper_maybe(u, rate):
    if u is None:
        return None
    if R.random() < rate:
        return u.upper()
    return u

# ---- timestamp formatting
def f1(ms):
    return datetime.fromtimestamp(ms // 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def fmt_ts(ms):
    if ms % 1000:
        return str(ms) if R.random() < 0.9 else str(ms).zfill(16)
    r = R.random()
    if r < 0.58:
        return f1(ms)
    if r < 0.68:
        off = R.randint(-40, 48) * 15
        t = datetime.fromtimestamp(ms // 1000, timezone.utc) + timedelta(minutes=off)
        sign = "+" if off >= 0 else "-"
        return t.strftime("%Y-%m-%dT%H:%M:%S") + f"{sign}{abs(off) // 60:02d}:{abs(off) % 60:02d}"
    return str(ms)

BAD_TS = ["", "2026-10-03 10:00:00", "2026-10-03T10:00:00.123Z", "2026-10-03t10:00:00Z", "2026-10-03T10:00:00z",
          "2026-10-03T10:00:00", "2026-13-03T10:00:00Z", "2026-02-30T10:00:00Z", "2027-02-29T10:00:00Z",
          "2026-10-03T24:00:00Z", "2026-10-03T10:00:60Z", "2026-10-03T10:00:00+24:00", "2026-10-03T10:00:00+05:60",
          "2026-10-03T10:00:00+0200", "1.79e12", "-5", "1791071890000.0", "86400000", "9" * 25, "１７９１０７１８９００００"]

def bad_ts(ms):
    r = R.random()
    if r < 0.3:
        return f1(WS - 1000) if R.random() < .5 else f1(WE)
    if r < 0.4:
        return str(WE + R.randint(0, 5))
    s = R.choice(BAD_TS)
    if s.startswith("2026-") and R.random() < 0.7:
        s = f1(ms)[:4] + s[4:] if "-13-" not in s and "02-" not in s else s
    return s

lines_ev = []
lines_clk = []
sess_starts = []
eid = 0
for pi, p in enumerate(people):
    n = counts[pi]
    if n == 0:
        continue
    t = R.randint(WS - 2 * 3600_000, WE + 3600_000)
    starts = [t]
    for _ in range(n):
        r = R.random()
        if r < 0.001:
            t += 30 * MIN + R.choice([0, 1])
        elif r < 0.85:
            t += R.randint(1, 600) * 1000 + (R.randint(0, 999) if R.random() < 0.3 else 0)
        else:
            t += R.randint(1, 12) * 3600_000 + R.randint(0, 3600_000)
            starts.append(t)
        if t > WE + DAY:
            t = R.randint(WS, WE)
            starts.append(t)
        eid += 1
        u = pick_id(p)
        r = R.random()
        et = "view" if r < 0.85 else "cart" if r < 0.97 else "purchase"
        prod = f"p_{min(NPROD - 1, int(R.paretovariate(1.0)) - 1)}"
        price = str(R.randint(100, 50000)) if et == "purchase" else ""
        ts_s = fmt_ts(t)
        ing = t + R.randint(0, 3600_000)
        if R.random() < 0.01:
            ing = t + R.choice([DAY, DAY + 1000, R.randint(DAY, 5 * DAY)])
        if R.random() < 0.005:
            ts_s = bad_ts(t)
        if R.random() < 0.002:
            u = "" if R.random() < 0.999 else u.upper()
        if R.random() < 0.001:
            et = R.choice(["Purchase", "cart ", "VIEW", "click", " view"])
        if et == "purchase" and R.random() < 0.05:
            price = R.choice(["19.99", "-5", "0", "", "10000001", "+100", "0250", "00", "10000000", "1"])
        if R.random() < 0.005:
            prod = R.choice(["p_X", "P_1", "", "p_999999"])
        row = [f"e{eid}", u, ts_s, f1(ing - ing % 1000 + 1000) if ing % 1000 else f1(ing), et, prod, price]
        lines_ev.append(row)
        if R.random() < 0.03:
            for k in range(R.randint(1, 3)):
                cp = row[:]
                cp[3] = f1(ing + R.randint(1, 600) * 1000 + 1000)
                if R.random() < 1 / 64:
                    cp[1] = R.choice(people[R.randrange(npeople)])
                lines_ev.append(cp)
            if R.random() < 0.02:
                row[2] = bad_ts(t)
    sess_starts.append((p, starts))

# clicks: some aligned to session starts with boundary offsets
cid = 0
whales = sess_starts[:5]
for _ in range(A.clicks):
    cid += 1
    p, starts = R.choice(sess_starts) if R.random() > 0.2 else R.choice(whales)
    s0 = R.choice(starts)
    r = R.random()
    if r < 0.1:
        t = s0 - R.choice([WEEK, WEEK + 1, 0, -1, 0])
    else:
        t = s0 - R.randint(0, 9 * DAY)
    camp = R.choice(whale_camps) if R.random() < 0.3 else R.choice(camps)
    if R.random() < 0.005:
        camp = R.choice(["c_99999", camp.upper(), "C_1", " c_1"])
    u = pick_id(p)
    if R.random() < 0.002:
        u = "" if R.random() < 0.999 else u.upper()
    ts_s = fmt_ts(t) if R.random() > 0.005 else bad_ts(t)
    row = [f"k{cid}", u, camp, ts_s]
    lines_clk.append(row)
    if R.random() < 0.02:
        lines_clk.append(row[:])
    if R.random() < 0.01:  # pair at same ms
        cid += 1
        lines_clk.append([f"k{cid}", pick_id(p), R.choice(camps), ts_s])

lines_ln = []
for a, b in links:
    if R.random() < 0.002:
        a = ""
    if R.random() < 1 / 2000:
        b = b.upper()
    lines_ln.append([a, b])
    if R.random() < 0.08:
        lines_ln.append([b, a] if R.random() < 0.5 else [a, b])

def render(row):
    out = []
    for v in row:
        out.append(f'"{v}"' if R.random() < 0.2 else v)
    s = ",".join(out)
    if R.random() < 0.0005:
        s = R.choice(["", s + ",x", ",".join(out[:-1]), s.replace(",", ',"', 1), '"' + s])
    return s

def write(sub, header, rows, nfiles):
    d = os.path.join(A.output, sub)
    os.makedirs(d, exist_ok=True)
    R.shuffle(rows)
    w = [1.0 / (i + 1) for i in range(nfiles)]
    R.shuffle(w)
    empties = set(R.sample(range(nfiles), min(nfiles - 1, R.randint(1, 3))))
    for i in empties:
        w[i] = 0
    fs = [open(os.path.join(d, f"part-{i:05d}.csv"), "w", encoding="utf-8", newline="") for i in range(nfiles)]
    for f in fs:
        f.write(header + "\n")
    idx = R.choices(range(nfiles), weights=w, k=len(rows))
    for row, i in zip(rows, idx):
        fs[i].write(render(row) + "\n")
    for f in fs:
        f.close()

scale = max(1, A.events // 1_000_000)
write("events", "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents", lines_ev, R.randint(24, 160) if scale > 1 else 8)
write("clicks", "click_id,user_id,campaign_id,ts", lines_clk, R.randint(8, 48) if scale > 1 else 4)
write("links", "user_id_a,user_id_b", lines_ln, R.randint(4, 24) if scale > 1 else 3)
print(f"events {len(lines_ev)} clicks {len(lines_clk)} links {len(lines_ln)} people {npeople}")
