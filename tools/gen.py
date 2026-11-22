#!/usr/bin/env python3
"""Small dirty-data generator for differential tests. usage: gen.py OUT SEED EVENTS CLICKS"""
import sys, os, random, datetime, string
out, seed, E, K = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
rnd = random.Random(seed)
os.makedirs(out + '/events', exist_ok=True); os.makedirs(out + '/clicks', exist_ok=True); os.makedirs(out + '/links', exist_ok=True)
ws = datetime.datetime(2028, 2, 20, tzinfo=datetime.timezone.utc) if seed % 2 else datetime.datetime(2026, 10, 1, tzinfo=datetime.timezone.utc)
WS = int(ws.timestamp()) * 1000; WE = WS + 14 * 86400000
open(out + '/manifest.json', 'w').write('{"window_start": "%s", "window_end": "%s"}' % (ws.strftime('%Y-%m-%dT%H:%M:%SZ'), (ws + datetime.timedelta(days=14)).strftime('%Y-%m-%dT%H:%M:%SZ')))
cats = ['cat%02d' % i for i in range(20)]
NP = 500
open(out + '/products.csv', 'w').write('product_id,category\n' + ''.join('p%d,%s\n' % (i, rnd.choice(cats)) for i in range(NP)))
NC = 60
open(out + '/campaigns.csv', 'w').write('campaign_id,channel\n' + ''.join('c_%d,ch%d\n' % (i, i % 5) for i in range(NC)))
def rid():
    s = ''.join(rnd.choice(string.ascii_lowercase + string.digits) for _ in range(rnd.randint(3, 8)))
    return s + ('%032x' % rnd.getrandbits(128) if rnd.random() < .125 else '')
NPERS = max(10, E // 25)
persons = []; links = []
for i in range(NPERS):
    k = rnd.choice([1, 1, 1, 2, 2, 3, 4]) if i > 3 else rnd.randint(3, 6)
    if i == 5 or i == 6: k = rnd.randint(50, 120)
    ids = [rid() for _ in range(k)]
    for j in range(1, k):
        links.append((ids[j], ids[rnd.randrange(j)] if k < 50 else ids[j - 1]))
    persons.append(ids)
for _ in range(max(2, NPERS // 100)):
    dev = rid(); 
    for p in rnd.sample(persons, min(len(persons), 4)): links.append((dev, p[0]))
w = [1 / (i + 1) ** 0.8 for i in range(NPERS)]
def ts_str(ms):
    r = rnd.random()
    if r < .32 or ms % 1000: return str(ms)
    d = datetime.datetime.fromtimestamp(ms // 1000, tz=datetime.timezone.utc)
    if r < .9: return d.strftime('%Y-%m-%dT%H:%M:%SZ')
    off = rnd.randint(-10 * 60, 12 * 60)
    dd = d + datetime.timedelta(minutes=off)
    sg = '+' if off >= 0 else '-'
    return dd.strftime('%Y-%m-%dT%H:%M:%S') + '%s%02d:%02d' % (sg, abs(off) // 60, abs(off) % 60)
def bad_ts():
    return rnd.choice(['', '2026-13-01T00:00:00Z', '2028-02-30T00:00:00Z', '2027-02-29T10:00:00Z', '2026-10-03t10:00:00Z', '2026-10-03 10:00:00Z',
        '2026-10-03T10:00:00', '2026-10-03T10:00:00+0200', '2026-10-03T10:00:00+24:00', '2026-10-03T10:00:00+05:60', '1791071890000.0', '1.79e12', '-5',
        '2026-10-03T24:00:00Z', '2026-10-03T10:00:60Z', '2026-10-03T10:00:00.5Z', '86400000', '99999999999999999999999', ts_str(WE), ts_str(WS - 1), '٢٥٠', '２５０'])
def q(s):
    return '"%s"' % s if rnd.random() < .2 else s
def mutate_id(u):
    r = rnd.random()
    if r < .002: return ''
    if r < .003: return u.upper()
    return u
sessions_ts = []
evrows = []
n = 0
while n < E:
    pi = rnd.choices(range(NPERS), w)[0] if rnd.random() < .7 else rnd.randrange(NPERS)
    ids = persons[pi]
    t = WS + rnd.randrange(14 * 86400000 - 3600000)
    for _ in range(rnd.randint(1, 8)):
        n += 1
        u = ids[0] if rnd.random() < .5 else rnd.choice(ids)
        r = rnd.random()
        et = 'purchase' if r < .06 else ('cart' if r < .2 else 'view')
        if rnd.random() < .001: et = rnd.choice(['Purchase', 'cart ', 'foo', ''])
        pid = 'p%d' % rnd.randrange(NP) if rnd.random() > .01 else rnd.choice(['', 'P1', 'p99999'])
        price = str(rnd.randint(100, 50000)) if et == 'purchase' else ''
        if et == 'purchase' and rnd.random() < .08: price = rnd.choice(['', '0', '-5', '1.5', '10000001', '+5', '0250', '10000000', '٢٥٠', 'abc'])
        tm = t
        tsv = ts_str(tm) if rnd.random() > .01 else bad_ts()
        eid = 'e%d' % n
        ing = tm + rnd.choice([0, 1000, 60000, 86400000, 86401000, -5000, 86399000, 3 * 86400000]) if rnd.random() < .05 else tm + rnd.randint(0, 3000) * 1000
        ings = datetime.datetime.fromtimestamp(ing // 1000, tz=datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        copies = [(eid, mutate_id(u), tsv, ings, et, pid, price)]
        if rnd.random() < .04:
            for c in range(rnd.randint(1, 3)):
                ing2 = datetime.datetime.fromtimestamp(ing // 1000 + rnd.randint(1, 600), tz=datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                bu = u if rnd.random() > .05 else rnd.choice(ids)
                tv2 = tsv if rnd.random() > .05 else ts_str(tm)
                copies.append((eid, bu, tv2, ing2, et, pid, price))
            if rnd.random() < .3: copies[0] = (eid, copies[0][1], bad_ts(), copies[0][3], et, pid, price)
        for cp in copies: evrows.append(','.join(q(x) if rnd.random() < .5 else x for x in cp))
        step = rnd.choice([rnd.randint(1, 600) * 1000, rnd.randint(1, 600) * 1000, 1800000, 1800001, 1799999, 0, 7200000]) if rnd.random() < .95 else 0
        t += step
        if t >= WE: break
clrows = []
for i in range(K):
    pi = rnd.choices(range(NPERS), w)[0]; ids = persons[pi]
    t = WS - 7 * 86400000 + rnd.randrange(21 * 86400000)
    if rnd.random() < .1: t = (WS - 7 * 86400000 + rnd.choice([0, 1, 7 * 86400000 + 5, 14 * 86400000]) )
    camp = 'c_%d' % (rnd.randrange(NC) if rnd.random() < .7 else rnd.randrange(3))
    if rnd.random() < .005: camp = rnd.choice(['C_1', 'c_9999', ''])
    tsv = ts_str(t) if rnd.random() > .01 else bad_ts()
    row = ('k%d' % i, mutate_id(rnd.choice(ids)), camp, tsv)
    clrows.append(','.join(q(x) for x in row))
    if rnd.random() < .02: clrows.append(','.join(q(x) for x in row))
lkrows = []
for a, b in links:
    if rnd.random() < .01: a = a.upper()
    r = ','.join(q(mutate_id(x)) for x in (a, b))
    lkrows.append(r)
    if rnd.random() < .08: lkrows.append(','.join(q(x) for x in (b, a)))
    if rnd.random() < .01: lkrows.append(','.join((a, a)))
def spill(rows, d, header, nfiles):
    rnd.shuffle(rows)
    cuts = sorted(rnd.randrange(len(rows) + 1) for _ in range(nfiles - 1))
    parts = []; prev = 0
    for c in cuts + [len(rows)]: parts.append(rows[prev:c]); prev = c
    for i, p in enumerate(parts):
        extra = []
        if rnd.random() < .5 and p: extra = [rnd.choice(['', 'a,b', 'x"y,1,2,3,4,5,6', '"unbalanced,1,2,3,4,5,6'])]
        with open('%s/part-%05d.csv' % (d, i), 'w') as f:
            f.write(header + '\n')
            for r in p + extra: f.write(r + '\n')
spill(evrows, out + '/events', 'event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents', 7)
spill(clrows, out + '/clicks', 'click_id,user_id,campaign_id,ts', 4)
spill(lkrows, out + '/links', 'user_id_a,user_id_b', 3)
