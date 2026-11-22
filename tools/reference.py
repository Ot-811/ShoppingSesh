#!/usr/bin/env python3
"""Slow, literal implementation of rules.md. Writes CSV (user_daily.csv, campaign_daily.csv) to OUT.
usage: reference.py DATA OUT"""
import sys, os, glob, json, re, datetime
from collections import defaultdict

DIG = re.compile(r'^[0-9]+$')
TSRE = re.compile(r'^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(Z|([+-])([0-9]{2}):([0-9]{2}))$', re.A)

def read_lines(path):
    with open(path, 'rb') as f:
        data = f.read()
    lines = data.split(b'\n')
    if lines and lines[-1] == b'':
        lines.pop()
    return lines[1:]

def split_fields(line, n):
    parts = line.split(b',')
    if len(parts) != n:
        return None
    out = []
    for p in parts:
        if b'"' in p:
            if len(p) >= 2 and p[0:1] == b'"' and p[-1:] == b'"' and b'"' not in p[1:-1]:
                p = p[1:-1]
            else:
                return None
        out.append(p.decode('utf-8', 'surrogateescape') if p != b'' else None)
    return out

def parse_ts(s):
    if s is None: return None
    if DIG.match(s):
        return int(s)
    m = TSRE.match(s)
    if not m: return None
    y, mo, d, h, mi, se = (int(m.group(i)) for i in range(1, 7))
    if se > 59: return None
    try:
        dt = datetime.datetime(y, mo, d, h, mi, se, tzinfo=datetime.timezone.utc)
    except ValueError:
        return None
    off = 0
    if m.group(7) != 'Z':
        oh, om = int(m.group(9)), int(m.group(10))
        if oh > 23 or om > 59: return None
        off = (oh * 3600 + om * 60) * (1 if m.group(8) == '+' else -1)
    return (int(dt.timestamp()) - off) * 1000

def bkey(s): return s.encode('utf-8', 'surrogateescape')

def main():
    data, out = sys.argv[1:3]
    os.makedirs(out, exist_ok=True)
    man = json.load(open(os.path.join(data, 'manifest.json')))
    ws = parse_ts(man['window_start']); we = parse_ts(man['window_end'])
    D7 = 7 * 86400000
    cat = {}
    for l in read_lines(os.path.join(data, 'products.csv')):
        f = split_fields(l, 2); cat[f[0]] = f[1]
    camps = {}
    for l in read_lines(os.path.join(data, 'campaigns.csv')):
        f = split_fields(l, 2); camps[f[0]] = f[1]
    parent = {}
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    for p in sorted(glob.glob(os.path.join(data, 'links', '*.csv'))):
        for l in read_lines(p):
            f = split_fields(l, 2)
            if f is None or f[0] is None or f[1] is None: continue
            for x in f: parent.setdefault(x, x)
            a, b = find(f[0]), find(f[1])
            if a != b: parent[a] = b
    comp = defaultdict(list)
    for x in parent: comp[find(x)].append(x)
    name = {}
    for r, ms in comp.items():
        n = min(ms, key=bkey)
        for x in ms: name[x] = n
    person = lambda u: name.get(u, u)
    best = {}
    for p in sorted(glob.glob(os.path.join(data, 'events', '*.csv'))):
        for l in read_lines(p):
            f = split_fields(l, 7)
            if f is None or f[0] is None: continue
            cur = best.get(f[0])
            if cur is None or parse_ts(f[3]) < parse_ts(cur[3]):
                best[f[0]] = f
    events = defaultdict(list)
    for eid, f in best.items():
        _, u, ts, ing, et, pid, price = f
        if u is None: continue
        t = parse_ts(ts)
        if t is None or not (ws <= t < we): continue
        if et not in ('view', 'cart', 'purchase'): continue
        if pid is None: continue
        pr = 0
        if et == 'purchase':
            if price is None or not DIG.match(price): continue
            pr = int(price)
            if not (1 <= pr <= 10000000): continue
        if parse_ts(ing) - t > 86400000: continue
        events[person(u)].append((t, et, pr, cat.get(pid, 'unknown')))
    clicks = defaultdict(dict)
    for p in sorted(glob.glob(os.path.join(data, 'clicks', '*.csv'))):
        for l in read_lines(p):
            f = split_fields(l, 4)
            if f is None or f[0] is None: continue
            cid, u, camp, ts = f
            if u is None: continue
            t = parse_ts(ts)
            if t is None or not (ws - D7 <= t < we): continue
            if camp not in camps: continue
            clicks[person(u)][cid] = (t, cid, camp)
    ud = defaultdict(lambda: [0, 0, 0, defaultdict(int)])
    cd = defaultdict(lambda: [0, 0, 0, 0])
    day_of = lambda t: datetime.date(1970, 1, 1) + datetime.timedelta(days=t // 86400000)
    for per, evs in events.items():
        evs.sort(key=lambda e: e[0])
        sessions = []
        for e in evs:
            if sessions and e[0] - sessions[-1][-1][0] <= 1800000:
                sessions[-1].append(e)
            else:
                sessions.append([e])
        cl = list(clicks.get(per, {}).values())
        for s in sessions:
            st = s[0][0]; day = day_of(st)
            rev = sum(e[2] for e in s if e[1] == 'purchase')
            u = ud[(per, day)]
            u[0] += 1; u[1] += len(s); u[2] += rev
            for e in s:
                if e[1] == 'purchase': u[3][e[3]] += e[2]
            q = [c for c in cl if c[0] <= st and st - c[0] <= D7]
            if q:
                b = min(q, key=lambda c: (-c[0], bkey(c[1])))
                cid_, ch = b[2], camps[b[2]]
            else:
                cid_, ch = 'organic', 'organic'
            r = cd[(cid_, ch, day)]
            r[0] += 1
            if any(e[1] == 'purchase' for e in s): r[1] += 1
            r[2] += rev
            for camp in set(c[2] for c in q):
                cd[(camp, camps[camp], day)][3] += 1
    with open(os.path.join(out, 'user_daily.csv'), 'w') as f:
        f.write('user_id,day,sessions,events,revenue_cents,top_category\n')
        for (per, day), (s, e, r, cs) in ud.items():
            top = min(cs.items(), key=lambda kv: (-kv[1], bkey(kv[0])))[0] if cs else ''
            f.write(f'{per},{day},{s},{e},{r},{top}\n')
    with open(os.path.join(out, 'campaign_daily.csv'), 'w') as f:
        f.write('campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n')
        for (c, ch, day), (s, p, r, a) in cd.items():
            f.write(f'{c},{ch},{day},{s},{p},{r},{a}\n')

main()
