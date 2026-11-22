#!/usr/bin/env python3
"""cmpcsv.py A_DIR B_DIR : order-insensitive exact compare of the two CSV tables"""
import sys, csv
def load(p):
    with open(p, newline='') as f:
        r = list(csv.reader(f))
    return r[0], sorted(r[1:])
ok = True
for t in ('user_daily', 'campaign_daily'):
    ha, a = load(f'{sys.argv[1]}/{t}.csv'); hb, b = load(f'{sys.argv[2]}/{t}.csv')
    if ha != hb or a != b:
        ok = False
        sa, sb = set(map(tuple, a)), set(map(tuple, b))
        print(t, 'DIFF rows', len(a), len(b), 'onlyA', sorted(sa - sb)[:5], 'onlyB', sorted(sb - sa)[:5])
sys.exit(0 if ok else 1)
