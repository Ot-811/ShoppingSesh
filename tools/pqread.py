#!/usr/bin/env python3
"""Minimal strict Parquet reader (PLAIN, UNCOMPRESSED, data page v1) used to check our writer.
usage: pqread.py OUT_DIR DEST_DIR   -> writes DEST_DIR/{user_daily,campaign_daily}.csv"""
import sys, os, glob, struct, datetime

class R:
    def __init__(s, b, p=0): s.b, s.p = b, p
    def byte(s): v = s.b[s.p]; s.p += 1; return v
    def varint(s):
        r = sh = 0
        while True:
            c = s.byte(); r |= (c & 0x7f) << sh; sh += 7
            if not c & 0x80: return r
    def zz(s): v = s.varint(); return (v >> 1) ^ -(v & 1)
    def binary(s): n = s.varint(); v = s.b[s.p:s.p+n]; s.p += n; return v
    def value(s, t):
        if t in (1, 2): return t == 1
        if t == 3: return s.byte()
        if t in (4, 5, 6): return s.zz()
        if t == 7: s.p += 8; return None
        if t == 8: return s.binary()
        if t in (9, 10):
            h = s.byte(); n = h >> 4; et = h & 15
            if n == 15: n = s.varint()
            return [s.value(et) for _ in range(n)]
        if t == 12: return s.struct()
        raise ValueError('type %d' % t)
    def struct(s):
        d = {}; last = 0
        while True:
            h = s.byte()
            if h == 0: return d
            t = h & 15; delta = h >> 4
            fid = last + delta if delta else s.zz()
            last = fid
            d[fid] = s.value(t)

def read_file(path):
    b = open(path, 'rb').read()
    assert b[:4] == b'PAR1' and b[-4:] == b'PAR1', 'magic'
    flen = struct.unpack('<I', b[-8:-4])[0]
    md = R(b, len(b) - 8 - flen).struct()
    schema = md[2][1:]
    cols = [(x[4].decode(), x[1], x.get(3, 0), x.get(6)) for x in schema]
    assert md[2][0][5] == len(cols)
    out = {c[0]: [] for c in cols}
    nrows = 0
    for rg in md.get(4, []):
        n = rg[3]; nrows += n
        for ci, cc in enumerate(rg[1]):
            name, ptype, rep, conv = cols[ci]
            cm = cc[3]
            assert cm[3] == [name.encode()], 'path'
            assert cm[4] == 0, 'codec'
            rd = R(b, cm[9]); assert cc[2] == cm[9]
            ph = rd.struct()
            assert ph[1] == 0 and ph[2] == ph[3]
            dph = ph[5]; assert dph[1] == n and dph[2] == 0
            page = b[rd.p:rd.p + ph[3]]
            assert rd.p + ph[3] - cm[9] == cm[7], 'sizes'
            pos = 0; present = [True] * n
            if rep == 1:
                ln = struct.unpack('<I', page[:4])[0]; pos = 4
                pr = R(page, pos); hdr = pr.varint(); assert hdr & 1
                groups = hdr >> 1; bits = page[pr.p:pr.p + groups]
                assert pr.p - 4 + groups == ln, 'deflen'
                present = [(bits[i // 8] >> (i % 8)) & 1 == 1 for i in range(n)]
                pos = 4 + ln
            for i in range(n):
                if not present[i]: out[name].append(None); continue
                if ptype == 1: v = struct.unpack('<i', page[pos:pos+4])[0]; pos += 4
                elif ptype == 2: v = struct.unpack('<q', page[pos:pos+8])[0]; pos += 8
                else:
                    l = struct.unpack('<I', page[pos:pos+4])[0]; pos += 4
                    v = page[pos:pos+l].decode('utf-8', 'surrogateescape'); pos += l
                if conv == 6: v = datetime.date(1970, 1, 1) + datetime.timedelta(days=v)
                out[name].append(v)
            assert pos == len(page), 'trailing bytes in page'
    return [c[0] for c in cols], out, nrows

def main():
    src, dst = sys.argv[1:3]
    os.makedirs(dst, exist_ok=True)
    for t in ('user_daily', 'campaign_daily'):
        files = sorted(f for f in glob.glob(f'{src}/{t}/*.parquet'))
        rows = []; header = None
        for f in files:
            h, o, n = read_file(f)
            header = header or h
            assert h == header
            for i in range(n): rows.append([o[c][i] for c in h])
        with open(f'{dst}/{t}.csv', 'w') as w:
            w.write(','.join(header) + '\n')
            for r in rows: w.write(','.join('' if v is None else str(v) for v in r) + '\n')
main()
