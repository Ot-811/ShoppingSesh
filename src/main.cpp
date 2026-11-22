// Shopping Sessions pipeline. Single static binary, no dependencies.
// Radix-partitioned, lock-free parallel pipeline with a hand-written Parquet writer.
//
//   phase 1  parse event lines (parallel)  -> partition by hash(event_id)       | links built concurrently
//   phase 2  parse click lines (parallel)  -> partition by person
//   phase 3  per event partition: dedup on event_id, resolve person -> partition by person
//   phase 4  per person partition: sort, sessionize, credit/assist, aggregate, write user_daily
//   phase 5  merge campaign accumulators, write campaign_daily
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>
#include <immintrin.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

typedef uint8_t u8;
typedef uint32_t u32;
typedef uint64_t u64;
typedef int32_t i32;
typedef int64_t i64;

static const i64 MS_DAY = 86400000LL;
static const i64 MS_30MIN = 1800000LL;

static double now_s() {
    return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count();
}
static double T0;
static void logt(const char* what) { fprintf(stderr, "[%7.3fs] %s\n", now_s() - T0, what); }
[[noreturn]] static void die(const char* msg) { fprintf(stderr, "FATAL: %s\n", msg); exit(1); }

// ---------------------------------------------------------------- hashing / strings
static inline u64 mix64(u64 a, u64 b) {
    __uint128_t r = (__uint128_t)a * b;
    return (u64)r ^ (u64)(r >> 64);
}
static inline u64 hash_bytes(const char* p, size_t n) {
    const u64 s0 = 0xa0761d6478bd642fULL, s1 = 0xe7037ed1a0b428dbULL, s2 = 0x8ebc6af09c88c6e3ULL;
    u64 h = s0 ^ n;
    size_t left = n;
    while (left >= 16) {
        u64 a, b;
        memcpy(&a, p, 8);
        memcpy(&b, p + 8, 8);
        h = mix64(a ^ s1, b ^ h);
        p += 16;
        left -= 16;
    }
    u64 a = 0, b = 0;
    if (left >= 8) {
        memcpy(&a, p, 8);
        memcpy(&b, p + left - 8, 8);
    } else if (left >= 4) {
        u32 x, y;
        memcpy(&x, p, 4);
        memcpy(&y, p + left - 4, 4);
        a = x;
        b = y;
    } else if (left > 0) {
        a = ((u64)(u8)p[0] << 16) | ((u64)(u8)p[left >> 1] << 8) | (u64)(u8)p[left - 1];
    }
    return mix64(s1 ^ n, mix64(a ^ s1, b ^ h ^ s2));
}

struct SV {
    const char* p;
    size_t n;
};
static inline u64 mk_pl(const char* p, size_t n) { return (u64)(uintptr_t)p | ((u64)n << 48); }
static inline const char* pl_ptr(u64 pl) { return (const char*)(uintptr_t)(pl & 0xFFFFFFFFFFFFULL); }
static inline size_t pl_len(u64 pl) { return (size_t)(pl >> 48); }
static inline int pl_cmp(u64 a, u64 b) {
    size_t la = pl_len(a), lb = pl_len(b);
    int c = memcmp(pl_ptr(a), pl_ptr(b), la < lb ? la : lb);
    if (c) return c;
    return la < lb ? -1 : (la > lb ? 1 : 0);
}

// ---------------------------------------------------------------- files
struct MFile {
    const char* p;
    size_t n;
};

static std::vector<MFile> map_dir(const std::string& dir) {
    std::vector<std::string> names;
    DIR* d = opendir(dir.c_str());
    if (d) {
        while (dirent* e = readdir(d)) {
            std::string nm = e->d_name;
            if (nm.size() >= 4 && nm.compare(nm.size() - 4, 4, ".csv") == 0) names.push_back(nm);
        }
        closedir(d);
    }
    std::sort(names.begin(), names.end());
    std::vector<MFile> out;
    for (auto& nm : names) {
        std::string path = dir + "/" + nm;
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) continue;
        struct stat st;
        if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode)) { close(fd); continue; }
        if (st.st_size == 0) { close(fd); continue; }
        void* m = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        if (m == MAP_FAILED) die("mmap failed");
        madvise(m, st.st_size, MADV_SEQUENTIAL);
        out.push_back({(const char*)m, (size_t)st.st_size});
    }
    return out;
}
static MFile map_file(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) die(("cannot open " + path).c_str());
    struct stat st;
    fstat(fd, &st);
    if (st.st_size == 0) { close(fd); return {nullptr, 0}; }
    void* m = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (m == MAP_FAILED) die("mmap failed");
    return {(const char*)m, (size_t)st.st_size};
}

struct Chunk {
    const char* base;
    size_t size;
    size_t lo, hi;
};
static std::vector<Chunk> make_chunks(const std::vector<MFile>& files, size_t csz) {
    std::vector<Chunk> ch;
    for (auto& f : files)
        for (size_t lo = 0; lo < f.n; lo += csz) ch.push_back({f.p, f.n, lo, std::min(f.n, lo + csz)});
    return ch;
}

// Calls fn(line_begin, line_end) for every line (header skipped) whose start lies in [lo, hi).
template <class F>
static inline void for_lines(const Chunk& c, F&& fn) {
    const char* base = c.base;
    const char* fend = base + c.size;
    const char* p;
    if (c.lo == 0) {
        const char* nl = (const char*)memchr(base, '\n', c.size);
        if (!nl) return;
        p = nl + 1;  // skip header
    } else if (base[c.lo - 1] == '\n') {
        p = base + c.lo;
    } else {
        const char* nl = (const char*)memchr(base + c.lo, '\n', c.size - c.lo);
        if (!nl) return;
        p = nl + 1;
    }
    const char* hi = base + c.hi;
    while (p < hi) {
        const char* nl = (const char*)memchr(p, '\n', fend - p);
        const char* e = nl ? nl : fend;
        fn(p, e);
        if (!nl) break;
        p = nl + 1;
    }
}


// Fused newline + field scan (AVX2): calls fn(SV* fields) for every well-formed line with N fields.
template <int N, class F>
static inline void for_lines_fast(const Chunk& c, F&& fn) {
    const char* base = c.base;
    const char* fend = base + c.size;
    const char* p;
    if (c.lo == 0) {
        const char* nl = (const char*)memchr(base, '\n', c.size);
        if (!nl) return;
        p = nl + 1;
    } else if (base[c.lo - 1] == '\n') {
        p = base + c.lo;
    } else {
        const char* nl = (const char*)memchr(base + c.lo, '\n', c.size - c.lo);
        if (!nl) return;
        p = nl + 1;
    }
    const char* hi = base + c.hi;
    const __m256i vc = _mm256_set1_epi8(','), vn = _mm256_set1_epi8('\n'), vq = _mm256_set1_epi8('"');
    while (p < hi) {
        SV out[N];
        int k = 0, q = 0;
        const char* fs = p;
        const char* x = p;
        const char* next = nullptr;
        bool ok = true;
        // close the field ending at d; returns false if the line is malformed
        auto close = [&](const char* d) -> bool {
            if (k >= N) return false;
            size_t len = (size_t)(d - fs);
            if (q) {
                if (q != 2 || len < 2 || fs[0] != '"' || d[-1] != '"') return false;
                out[k] = {fs + 1, len - 2};
            } else {
                out[k] = {fs, len};
            }
            k++;
            q = 0;
            fs = d + 1;
            return true;
        };
        while (!next) {
            if (x + 32 <= fend) {
                __m256i v = _mm256_loadu_si256((const __m256i*)x);
                u32 m = (u32)_mm256_movemask_epi8(_mm256_or_si256(
                    _mm256_or_si256(_mm256_cmpeq_epi8(v, vc), _mm256_cmpeq_epi8(v, vn)), _mm256_cmpeq_epi8(v, vq)));
                while (m) {
                    const char* d = x + __builtin_ctz(m);
                    m &= m - 1;
                    char ch = *d;
                    if (ch == '"') { q++; continue; }
                    if (!close(d)) {
                        ok = false;
                        if (ch == '\n') { next = d + 1; }
                        else {
                            const char* nl = (const char*)memchr(d, '\n', fend - d);
                            next = nl ? nl + 1 : fend;
                        }
                        break;
                    }
                    if (ch == '\n') { next = d + 1; break; }
                }
                x += 32;
            } else {
                for (; x < fend; x++) {
                    char ch = *x;
                    if (ch == '"') { q++; continue; }
                    if (ch != ',' && ch != '\n') continue;
                    if (!close(x)) {
                        ok = false;
                        const char* nl = (const char*)memchr(x, '\n', fend - x);
                        next = nl ? nl + 1 : fend;
                        break;
                    }
                    if (ch == '\n') { next = x + 1; break; }
                }
                if (!next) {  // end of file without newline: last field
                    if (!close(fend)) ok = false;
                    next = fend;
                }
            }
        }
        if (ok && k == N) fn(out);
        p = next;
    }
}

// ---------------------------------------------------------------- CSV dialect
static inline bool split_line(const char* s, const char* e, int n, SV* out) {
    int k = 0;
    const char* fs = s;
    int q = 0;
    for (const char* p = s;; ++p) {
        if (p == e || *p == ',') {
            if (k >= n) return false;
            size_t len = (size_t)(p - fs);
            if (q) {
                if (q != 2 || len < 2 || fs[0] != '"' || p[-1] != '"') return false;
                out[k] = {fs + 1, len - 2};
            } else {
                out[k] = {fs, len};
            }
            k++;
            q = 0;
            fs = p + 1;
            if (p == e) break;
        } else if (*p == '"') {
            q++;
        }
    }
    return k == n;
}

// ---------------------------------------------------------------- timestamps
static inline i64 days_from_civil(i64 y, unsigned m, unsigned d) {
    y -= m <= 2;
    const i64 era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = (unsigned)(y - era * 400);
    const unsigned doy = (153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (i64)doe - 719468;
}
static inline int d2(const char* s) {
    unsigned a = (unsigned)(s[0] - '0'), b = (unsigned)(s[1] - '0');
    if (a > 9 || b > 9) return -1;
    return (int)(a * 10 + b);
}
static inline bool parse_ts(const char* s, size_t n, i64& out) {
    if (n == 0) return false;
    if (n >= 5 && s[4] == '-') {
        if (n != 20 && n != 25) return false;
        unsigned y0 = (unsigned)(s[0] - '0'), y1 = (unsigned)(s[1] - '0'), y2 = (unsigned)(s[2] - '0'),
                 y3 = (unsigned)(s[3] - '0');
        if (y0 > 9 || y1 > 9 || y2 > 9 || y3 > 9) return false;
        int y = (int)(y0 * 1000 + y1 * 100 + y2 * 10 + y3);
        if (s[7] != '-' || s[10] != 'T' || s[13] != ':' || s[16] != ':') return false;
        int mo = d2(s + 5), d = d2(s + 8), h = d2(s + 11), mi = d2(s + 14), se = d2(s + 17);
        if (mo < 1 || mo > 12 || d < 1 || h < 0 || h > 23 || mi < 0 || mi > 59 || se < 0 || se > 59) return false;
        static const int dim[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        int dm = dim[mo - 1];
        if (mo == 2 && ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0)) dm = 29;
        if (d > dm) return false;
        i64 off = 0;
        if (n == 20) {
            if (s[19] != 'Z') return false;
        } else {
            char sg = s[19];
            if ((sg != '+' && sg != '-') || s[22] != ':') return false;
            int oh = d2(s + 20), om = d2(s + 23);
            if (oh < 0 || oh > 23 || om < 0 || om > 59) return false;
            off = (i64)(oh * 3600 + om * 60);
            if (sg == '-') off = -off;
        }
        i64 days = days_from_civil(y, (unsigned)mo, (unsigned)d);
        out = (days * 86400 + h * 3600 + mi * 60 + se - off) * 1000;
        return true;
    }
    size_t i = 0;
    while (i < n && s[i] == '0') i++;
    if (n - i > 18) {
        for (size_t k = i; k < n; k++)
            if ((unsigned)(s[k] - '0') > 9) return false;
        return false;  // too large: always outside every window
    }
    i64 v = 0;
    for (size_t k = i; k < n; k++) {
        unsigned c = (unsigned)(s[k] - '0');
        if (c > 9) return false;
        v = v * 10 + c;
    }
    out = v;
    return true;
}

// ---------------------------------------------------------------- open-addressing string map
struct MapE {
    u64 h, pl, a, b;
};
struct StrMap {
    std::vector<MapE> t;
    size_t mask = 0, cnt = 0;
    void init(size_t cap) {
        size_t c = 16;
        while (c < cap) c <<= 1;
        t.assign(c, MapE{0, 0, 0, 0});
        mask = c - 1;
        cnt = 0;
    }
    inline const MapE* find(u64 h, const char* p, size_t n) const {
        size_t i = h & mask;
        for (;;) {
            const MapE& e = t[i];
            if (e.pl == 0) return nullptr;
            if (e.h == h && pl_len(e.pl) == n && memcmp(pl_ptr(e.pl), p, n) == 0) return &e;
            i = (i + 1) & mask;
        }
    }
    inline void prefetch(u64 h) const { __builtin_prefetch(&t[h & mask]); }
    // returns entry; inserted tells whether new
    MapE* insert(u64 h, const char* p, size_t n, bool& inserted) {
        if ((cnt + 1) * 2 > t.size()) grow();
        size_t i = h & mask;
        for (;;) {
            MapE& e = t[i];
            if (e.pl == 0) {
                e.h = h;
                e.pl = mk_pl(p, n);
                cnt++;
                inserted = true;
                return &e;
            }
            if (e.h == h && pl_len(e.pl) == n && memcmp(pl_ptr(e.pl), p, n) == 0) {
                inserted = false;
                return &e;
            }
            i = (i + 1) & mask;
        }
    }
    void grow() {
        std::vector<MapE> old;
        old.swap(t);
        size_t c = old.size() * 2;
        t.assign(c, MapE{0, 0, 0, 0});
        mask = c - 1;
        for (auto& e : old) {
            if (e.pl == 0) continue;
            size_t i = e.h & mask;
            while (t[i].pl != 0) i = (i + 1) & mask;
            t[i] = e;
        }
    }
};

// ---------------------------------------------------------------- block lists
template <class T, size_t B>
struct BL {
    std::vector<T*> blocks;
    size_t last = B;
    size_t n = 0;
    inline void push(const T& v) {
        if (last == B) {
            blocks.push_back((T*)malloc(sizeof(T) * B));
            last = 0;
        }
        blocks.back()[last++] = v;
        n++;
    }
    void append_to(std::vector<T>& dst) const {
        size_t off = dst.size();
        dst.resize(off + n);
        for (size_t i = 0; i < blocks.size(); i++) {
            size_t cnt = (i + 1 == blocks.size()) ? last : B;
            memcpy(dst.data() + off, blocks[i], cnt * sizeof(T));
            off += cnt;
        }
    }
    void release() {
        for (T* b : blocks) free(b);
        blocks.clear();
        blocks.shrink_to_fit();
        n = 0;
        last = B;
    }
};

// ---------------------------------------------------------------- records
struct EvRec {
    u64 eh, epl, upl, uh;
    i64 ts;
    u32 ing_s;  // ingest_ts in whole seconds
    u32 pc;     // price (24 bits) | category (8 bits)
    u32 flags;  // bit0 valid, bits 1..2 type (0 view, 1 cart, 2 purchase)
    u32 pad;
};
struct SessRec {
    u64 key;
    i64 ts;
    u64 meta;  // price | cat << 32 | type << 40
    u64 npl;   // name pointer/len
};
struct ClickRec {
    u64 key;
    i64 ts;
    u64 cpl;
    u64 camp;
};

static const int PBITS = 10;
static const int NPART = 1 << PBITS;
typedef BL<EvRec, 1024> EvList;
typedef BL<SessRec, 1024> SessList;
typedef BL<ClickRec, 1024> ClickList;

// ---------------------------------------------------------------- globals
static int NT = 8;
static i64 WS, WE;
static i64 WS_DAY;
static int ND;
static StrMap g_prod, g_camp, g_links;  // g_links: a = name hash, b = name pl
static std::vector<std::string> g_cats;  // sorted bytewise
static std::vector<std::string> g_campid, g_campch;
static u32 g_unknown_cat;

template <class F>
static void parallel_for(size_t n, F&& fn) {
    std::atomic<size_t> next{0};
    std::vector<std::thread> th;
    for (int t = 0; t < NT; t++)
        th.emplace_back([&, t]() {
            for (;;) {
                size_t i = next.fetch_add(1);
                if (i >= n) break;
                fn(i, t);
            }
        });
    for (auto& x : th) x.join();
}

// ---------------------------------------------------------------- small lookup tables
static void load_small_tables(const std::string& data) {
    // products
    {
        MFile f = map_file(data + "/products.csv");
        std::vector<std::pair<SV, SV>> rows;
        Chunk c{f.p, f.n, 0, f.n};
        if (f.n) for_lines(c, [&](const char* s, const char* e) {
            SV fl[2];
            if (!split_line(s, e, 2, fl)) return;
            rows.push_back({fl[0], fl[1]});
        });
        std::vector<std::string> names;
        for (auto& r : rows) names.emplace_back(r.second.p, r.second.n);
        names.emplace_back("unknown");
        std::sort(names.begin(), names.end());
        names.erase(std::unique(names.begin(), names.end()), names.end());
        g_cats = names;
        g_unknown_cat = (u32)(std::lower_bound(names.begin(), names.end(), std::string("unknown")) - names.begin());
        g_prod.init(rows.size() * 2 + 16);
        for (auto& r : rows) {
            if (r.first.n == 0) continue;
            std::string cn(r.second.p, r.second.n);
            u32 ci = (u32)(std::lower_bound(names.begin(), names.end(), cn) - names.begin());
            bool ins;
            MapE* e = g_prod.insert(hash_bytes(r.first.p, r.first.n), r.first.p, r.first.n, ins);
            e->a = ci;
        }
    }
    // campaigns
    {
        MFile f = map_file(data + "/campaigns.csv");
        std::vector<std::pair<SV, SV>> rows;
        Chunk c{f.p, f.n, 0, f.n};
        if (f.n) for_lines(c, [&](const char* s, const char* e) {
            SV fl[2];
            if (!split_line(s, e, 2, fl)) return;
            rows.push_back({fl[0], fl[1]});
        });
        g_camp.init(rows.size() * 2 + 16);
        for (auto& r : rows) {
            if (r.first.n == 0) continue;
            bool ins;
            MapE* e = g_camp.insert(hash_bytes(r.first.p, r.first.n), r.first.p, r.first.n, ins);
            if (ins) {
                e->a = g_campid.size();
                g_campid.emplace_back(r.first.p, r.first.n);
                g_campch.emplace_back(r.second.p, r.second.n);
            } else {
                g_campch[e->a].assign(r.second.p, r.second.n);
            }
        }
    }
    // manifest
    {
        MFile f = map_file(data + "/manifest.json");
        std::string js(f.p, f.n);
        auto grab = [&](const char* key) -> i64 {
            size_t k = js.find(key);
            if (k == std::string::npos) die("manifest key missing");
            size_t q1 = js.find('"', js.find(':', k));
            size_t q2 = js.find('"', q1 + 1);
            i64 v;
            if (!parse_ts(js.data() + q1 + 1, q2 - q1 - 1, v)) die("bad manifest ts");
            return v;
        };
        WS = grab("window_start");
        WE = grab("window_end");
        WS_DAY = WS >= 0 ? WS / MS_DAY : -((-WS + MS_DAY - 1) / MS_DAY);
        ND = (int)((WE - 1) / MS_DAY - WS_DAY + 1);
        if (ND < 1) ND = 1;
    }
}

// ---------------------------------------------------------------- links (union-find)
static void build_links(const std::string& data) {
    std::vector<MFile> files = map_dir(data + "/links");
    StrMap tab;
    tab.init(1 << 16);
    std::vector<u32> parent;
    std::vector<u64> ipl, ih;
    auto find = [&](u32 x) {
        while (parent[x] != x) {
            parent[x] = parent[parent[x]];
            x = parent[x];
        }
        return x;
    };
    auto intern = [&](SV s) -> u32 {
        u64 h = hash_bytes(s.p, s.n);
        bool ins;
        MapE* e = tab.insert(h, s.p, s.n, ins);
        if (ins) {
            e->a = parent.size();
            parent.push_back((u32)parent.size());
            ipl.push_back(e->pl);
            ih.push_back(h);
        }
        return (u32)e->a;
    };
    for (auto& f : files) {
        Chunk c{f.p, f.n, 0, f.n};
        for_lines(c, [&](const char* s, const char* e) {
            SV fl[2];
            if (!split_line(s, e, 2, fl)) return;
            if (fl[0].n == 0 || fl[1].n == 0) return;
            u32 a = intern(fl[0]), b = intern(fl[1]);
            u32 ra = find(a), rb = find(b);
            if (ra != rb) parent[ra] = rb;
        });
    }
    size_t n = parent.size();
    std::vector<u32> best(n, 0xFFFFFFFFu);
    for (u32 i = 0; i < n; i++) {
        u32 r = find(i);
        if (best[r] == 0xFFFFFFFFu || pl_cmp(ipl[i], ipl[best[r]]) < 0) best[r] = i;
    }
    g_links.init(n * 2 + 16);
    for (u32 i = 0; i < n; i++) {
        u32 nm = best[find(i)];
        size_t s = ih[i] & g_links.mask;
        while (g_links.t[s].pl != 0) s = (s + 1) & g_links.mask;
        g_links.t[s] = MapE{ih[i], ipl[i], ih[nm], ipl[nm]};
        g_links.cnt++;
    }
}

// ---------------------------------------------------------------- phase 1: events
struct P1State {
    EvList parts[NPART];
};

static inline u32 enc_type(const SV& t, bool& ok) {
    ok = true;
    if (t.n == 4 && memcmp(t.p, "view", 4) == 0) return 0;
    if (t.n == 4 && memcmp(t.p, "cart", 4) == 0) return 1;
    if (t.n == 8 && memcmp(t.p, "purchase", 8) == 0) return 2;
    ok = false;
    return 0;
}

static void phase1_chunk(const Chunk& c, P1State& st) {
    for_lines_fast<7>(c, [&](const SV* f) {
        if (f[0].n == 0) return;
        EvRec r;
        r.eh = hash_bytes(f[0].p, f[0].n);
        r.epl = mk_pl(f[0].p, f[0].n);
        r.upl = mk_pl(f[1].p, f[1].n);
        r.uh = 0;
        r.ts = 0;
        r.pc = 0;
        r.flags = 0;
        r.pad = 0;
        i64 ing = 0, ts = 0;
        bool ing_ok = parse_ts(f[3].p, f[3].n, ing);
        r.ing_s = ing_ok ? (u32)(ing / 1000) : 0xFFFFFFFFu;
        bool valid = f[1].n > 0 && f[5].n > 0;
        bool tok;
        u32 type = enc_type(f[4], tok);
        valid = valid && tok;
        if (valid) {
            valid = parse_ts(f[2].p, f[2].n, ts) && ts >= WS && ts < WE && ing_ok && (ing - ts <= MS_DAY);
        }
        u32 price = 0, cat = 0;
        if (valid && type == 2) {
            const SV& pr = f[6];
            size_t i = 0;
            while (i < pr.n && pr.p[i] == '0') i++;
            if (pr.n == 0 || pr.n - i > 8) {
                valid = false;
            } else {
                i64 v = 0;
                for (size_t k = i; k < pr.n; k++) {
                    unsigned d = (unsigned)(pr.p[k] - '0');
                    if (d > 9) { valid = false; break; }
                    v = v * 10 + d;
                }
                if (valid && (v < 1 || v > 10000000)) valid = false;
                price = (u32)v;
            }
            if (valid) {
                const MapE* pe = g_prod.find(hash_bytes(f[5].p, f[5].n), f[5].p, f[5].n);
                cat = pe ? (u32)pe->a : g_unknown_cat;
            }
        }
        if (valid) {
            r.uh = hash_bytes(f[1].p, f[1].n);
            r.ts = ts;
            r.pc = price | (cat << 24);
            r.flags = 1u | (type << 1);
        }
        st.parts[r.eh >> (64 - PBITS)].push(r);
    });
}

// ---------------------------------------------------------------- phase 2: clicks
struct P2State {
    ClickList parts[NPART];
};

static inline void resolve_person(u64 uh, u64 upl, u64& key, u64& npl) {
    const MapE* le = g_links.find(uh, pl_ptr(upl), pl_len(upl));
    if (le) {
        key = le->a;
        npl = le->b;
    } else {
        key = uh;
        npl = upl;
    }
}

static void phase2_chunk(const Chunk& c, P2State& st) {
    const i64 lo = WS - 7 * MS_DAY;
    for_lines_fast<4>(c, [&](const SV* f) {
        if (f[0].n == 0 || f[1].n == 0) return;
        i64 ts;
        if (!parse_ts(f[3].p, f[3].n, ts) || ts < lo || ts >= WE) return;
        const MapE* ce = g_camp.find(hash_bytes(f[2].p, f[2].n), f[2].p, f[2].n);
        if (!ce) return;
        u64 uh = hash_bytes(f[1].p, f[1].n);
        u64 key, npl;
        resolve_person(uh, mk_pl(f[1].p, f[1].n), key, npl);
        ClickRec r{key, ts, mk_pl(f[0].p, f[0].n), ce->a};
        st.parts[key >> (64 - PBITS)].push(r);
    });
}

// ---------------------------------------------------------------- Thrift compact + Parquet writer
struct TW {
    std::vector<u8> b;
    std::vector<int> stack;
    int last = 0;
    void byte(u8 v) { b.push_back(v); }
    void varint(u64 v) {
        while (v >= 0x80) { b.push_back((u8)(v | 0x80)); v >>= 7; }
        b.push_back((u8)v);
    }
    void zz(i64 v) { varint(((u64)v << 1) ^ (u64)(v >> 63)); }
    void field(int id, int type) {
        int delta = id - last;
        if (delta > 0 && delta <= 15) byte((u8)((delta << 4) | type));
        else { byte((u8)type); zz(id); }
        last = id;
    }
    void i32f(int id, i64 v) { field(id, 5); zz(v); }
    void i64f(int id, i64 v) { field(id, 6); zz(v); }
    void strf(int id, const std::string& s) {
        field(id, 8);
        varint(s.size());
        b.insert(b.end(), s.begin(), s.end());
    }
    void list(int id, int elem, size_t n) {
        field(id, 9);
        if (n < 15) byte((u8)((n << 4) | elem));
        else { byte((u8)(0xF0 | elem)); varint(n); }
    }
    void sbegin(int id) { field(id, 12); stack.push_back(last); last = 0; }
    void ebegin() { stack.push_back(last); last = 0; }  // struct as list element
    void send() { byte(0); last = stack.back(); stack.pop_back(); }
};

struct PCol {
    const char* name;
    int ptype;  // 1 INT32, 2 INT64, 6 BYTE_ARRAY
    int conv;   // -1 none, 0 UTF8, 6 DATE
    bool optional;
};
struct ColBuf {
    std::vector<char> data;
    std::vector<u8> def;
    u64 nonnull = 0;
};
struct RowGroup {
    u64 rows = 0;
    std::vector<ColBuf> cols;
};

static void put32(std::vector<char>& v, u32 x) { v.insert(v.end(), (char*)&x, (char*)&x + 4); }
static void put64(std::vector<char>& v, u64 x) { v.insert(v.end(), (char*)&x, (char*)&x + 8); }
static void put_str(std::vector<char>& v, const char* p, size_t n) {
    put32(v, (u32)n);
    v.insert(v.end(), p, p + n);
}

static void write_parquet(const std::string& path, const std::vector<PCol>& cols, std::vector<RowGroup>& rgs) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) die(("cannot write " + path).c_str());
    setvbuf(f, nullptr, _IOFBF, 1 << 22);
    u64 pos = 0;
    auto W = [&](const void* p, size_t n) {
        if (n) fwrite(p, 1, n, f);
        pos += n;
    };
    W("PAR1", 4);
    u64 total_rows = 0;
    struct CC {
        u64 off, csize;
    };
    std::vector<std::vector<CC>> meta;
    std::vector<u64> rgsize;
    std::vector<u64> rgrows;
    for (auto& rg : rgs) {
        if (rg.rows == 0) continue;
        std::vector<CC> cc;
        u64 rgbytes = 0;
        for (size_t ci = 0; ci < cols.size(); ci++) {
            ColBuf& cb = rg.cols[ci];
            std::vector<char> defsec;
            if (cols[ci].optional) {
                u64 nb = (rg.rows + 7) / 8;
                TW hdr;
                hdr.varint((nb << 1) | 1);
                u32 len = (u32)(hdr.b.size() + nb);
                put32(defsec, len);
                defsec.insert(defsec.end(), hdr.b.begin(), hdr.b.end());
                defsec.insert(defsec.end(), cb.def.begin(), cb.def.end());
            }
            u64 psize = defsec.size() + cb.data.size();
            TW ph;
            ph.i32f(1, 0);
            ph.i32f(2, (i64)psize);
            ph.i32f(3, (i64)psize);
            ph.sbegin(5);
            ph.i32f(1, (i64)rg.rows);
            ph.i32f(2, 0);
            ph.i32f(3, 3);
            ph.i32f(4, 3);
            ph.send();
            ph.byte(0);
            u64 off = pos;
            W(ph.b.data(), ph.b.size());
            W(defsec.data(), defsec.size());
            W(cb.data.data(), cb.data.size());
            u64 cs = pos - off;
            cc.push_back({off, cs});
            rgbytes += cs;
        }
        meta.push_back(cc);
        rgsize.push_back(rgbytes);
        rgrows.push_back(rg.rows);
        total_rows += rg.rows;
    }
    TW m;
    m.i32f(1, 1);
    m.list(2, 12, cols.size() + 1);
    m.ebegin();
    m.strf(4, "schema");
    m.i32f(5, (i64)cols.size());
    m.send();
    for (auto& c : cols) {
        m.ebegin();
        m.i32f(1, c.ptype);
        m.i32f(3, c.optional ? 1 : 0);
        m.strf(4, c.name);
        if (c.conv >= 0) m.i32f(6, c.conv);
        if (c.conv == 0) {
            m.sbegin(10);
            m.sbegin(1);
            m.send();
            m.send();
        } else if (c.conv == 6) {
            m.sbegin(10);
            m.sbegin(6);
            m.send();
            m.send();
        }
        m.send();
    }
    m.i64f(3, (i64)total_rows);
    m.list(4, 12, meta.size());
    for (size_t g = 0; g < meta.size(); g++) {
        m.ebegin();
        m.list(1, 12, cols.size());
        for (size_t ci = 0; ci < cols.size(); ci++) {
            m.ebegin();
            m.i64f(2, (i64)meta[g][ci].off);
            m.sbegin(3);
            m.i32f(1, cols[ci].ptype);
            if (cols[ci].optional) {
                m.list(2, 5, 2);
                m.zz(0);
                m.zz(3);
            } else {
                m.list(2, 5, 1);
                m.zz(0);
            }
            m.list(3, 8, 1);
            m.varint(strlen(cols[ci].name));
            m.b.insert(m.b.end(), cols[ci].name, cols[ci].name + strlen(cols[ci].name));
            m.i32f(4, 0);
            m.i64f(5, (i64)rgrows[g]);
            m.i64f(6, (i64)meta[g][ci].csize);
            m.i64f(7, (i64)meta[g][ci].csize);
            m.i64f(9, (i64)meta[g][ci].off);
            m.send();
            m.send();
        }
        m.i64f(2, (i64)rgsize[g]);
        m.i64f(3, (i64)rgrows[g]);
        m.send();
    }
    m.strf(6, "shopsesh-cpp");
    m.byte(0);
    W(m.b.data(), m.b.size());
    u32 flen = (u32)m.b.size();
    W(&flen, 4);
    W("PAR1", 4);
    fclose(f);
}

// ---------------------------------------------------------------- phase 3/4 worker state
struct Acc {
    i64 s, ps, rev, as;
};
static const size_t RG_ROWS = 1 << 18;

struct UserOut {
    std::vector<RowGroup> rgs;
    RowGroup* cur = nullptr;
    void new_rg() {
        rgs.emplace_back();
        cur = &rgs.back();
        cur->cols.resize(6);
    }
    inline void row(u64 npl, i32 day, i64 sessions, i64 events, i64 rev, i32 topcat) {
        if (!cur || cur->rows >= RG_ROWS) {
            if (cur == nullptr || cur->rows >= RG_ROWS) new_rg();
        }
        RowGroup& g = *cur;
        put_str(g.cols[0].data, pl_ptr(npl), pl_len(npl));
        put32(g.cols[1].data, (u32)day);
        put64(g.cols[2].data, (u64)sessions);
        put64(g.cols[3].data, (u64)events);
        put64(g.cols[4].data, (u64)rev);
        ColBuf& tc = g.cols[5];
        if ((g.rows & 7) == 0) tc.def.push_back(0);
        if (topcat >= 0) {
            tc.def.back() |= (u8)(1u << (g.rows & 7));
            put_str(tc.data, g_cats[topcat].data(), g_cats[topcat].size());
            tc.nonnull++;
        }
        g.rows++;
    }
};

struct W4 {
    std::vector<SessRec> E;
    std::vector<ClickRec> C;
    std::vector<Acc> acc;
    std::vector<u32> cnt, since;
    std::vector<i64> sstart, srev;
    std::vector<u32> sday, srunend, sevs;
    std::vector<u8> spur;
    std::vector<i64> catsum;
    std::vector<u32> touched;
    UserOut uo;
};

static void process_partition(W4& w, size_t ncamp) {
    std::vector<SessRec>& E = w.E;
    std::vector<ClickRec>& C = w.C;
    if (E.empty()) return;
    std::sort(E.begin(), E.end(), [](const SessRec& a, const SessRec& b) {
        return a.key != b.key ? a.key < b.key : a.ts < b.ts;
    });
    std::sort(C.begin(), C.end(), [](const ClickRec& a, const ClickRec& b) {
        return a.key != b.key ? a.key < b.key : a.ts < b.ts;
    });
    const size_t ORG = ncamp;
    size_t ci = 0;
    size_t a = 0;
    const size_t NE = E.size();
    while (a < NE) {
        const u64 key = E[a].key;
        size_t b = a + 1;
        while (b < NE && E[b].key == key) b++;
        while (ci < C.size() && C[ci].key < key) ci++;
        size_t cj = ci;
        while (cj < C.size() && C[cj].key == key) cj++;

        w.sstart.clear(); w.srev.clear(); w.sday.clear(); w.sevs.clear(); w.spur.clear();
        i64 prev = 0;
        i32 curday = -1;
        i64 ud_s = 0, ud_e = 0, ud_r = 0;
        const u64 npl = E[a].npl;
        auto flush_day = [&]() {
            i32 top = -1;
            if (!w.touched.empty()) {
                i64 bs = -1;
                for (u32 cidx : w.touched) {
                    i64 sm = w.catsum[cidx];
                    if (sm > bs || (sm == bs && (i32)cidx < top)) { bs = sm; top = (i32)cidx; }
                    w.catsum[cidx] = 0;
                }
                w.touched.clear();
            }
            w.uo.row(npl, (i32)(WS_DAY + curday), ud_s, ud_e, ud_r, top);
        };
        for (size_t i = a; i < b; i++) {
            const SessRec& r = E[i];
            if (w.sstart.empty() || r.ts - prev > MS_30MIN) {
                i32 d = (i32)(r.ts / MS_DAY - WS_DAY);
                w.sstart.push_back(r.ts);
                w.sday.push_back((u32)d);
                w.srev.push_back(0);
                w.sevs.push_back(0);
                w.spur.push_back(0);
                if (d != curday) {
                    if (curday >= 0) flush_day();
                    curday = d;
                    ud_s = ud_e = ud_r = 0;
                }
                ud_s++;
            }
            prev = r.ts;
            const size_t si = w.sstart.size() - 1;
            w.sevs[si]++;
            ud_e++;
            if (((r.meta >> 40) & 3) == 2) {
                i64 price = (i64)(r.meta & 0xFFFFFFFFu);
                u32 cat = (u32)((r.meta >> 32) & 0xFF);
                w.srev[si] += price;
                w.spur[si] = 1;
                ud_r += price;
                if (w.catsum[cat] == 0) w.touched.push_back(cat);
                w.catsum[cat] += price;
            }
        }
        flush_day();

        const size_t m = w.sstart.size();
        // day-run ends
        w.srunend.resize(m);
        for (size_t j = m; j-- > 0;)
            w.srunend[j] = (j + 1 < m && w.sday[j + 1] == w.sday[j]) ? w.srunend[j + 1] : (u32)j;

        auto assist = [&](u32 camp, u32 from, u32 to) {  // sessions [from, to]
            u32 j = from;
            while (j <= to) {
                u32 e = std::min(w.srunend[j], to);
                w.acc[(size_t)camp * ND + w.sday[j]].as += (i64)(e - j + 1);
                j = e + 1;
            }
        };

        size_t L = ci, R = ci;
        for (size_t j = 0; j < m; j++) {
            const i64 s = w.sstart[j];
            while (R < cj && C[R].ts <= s) {
                u32 c = (u32)C[R].camp;
                if (w.cnt[c]++ == 0) w.since[c] = (u32)j;
                R++;
            }
            const i64 cutoff = s - 7 * MS_DAY;
            while (L < R && C[L].ts < cutoff) {
                u32 c = (u32)C[L].camp;
                if (--w.cnt[c] == 0 && w.since[c] <= (u32)j - 1 && j > 0) assist(c, w.since[c], (u32)j - 1);
                L++;
            }
            size_t cc = ORG;
            if (L < R) {
                size_t best = R - 1;
                const i64 t0 = C[best].ts;
                for (size_t t = R - 1; t-- > L && C[t].ts == t0;)
                    if (pl_cmp(C[t].cpl, C[best].cpl) < 0) best = t;
                cc = (size_t)C[best].camp;
            }
            Acc& A = w.acc[cc * ND + w.sday[j]];
            A.s++;
            A.ps += w.spur[j];
            A.rev += w.srev[j];
        }
        for (size_t t = L; t < R; t++) {
            u32 c = (u32)C[t].camp;
            if (w.cnt[c] > 0) {
                assist(c, w.since[c], (u32)m - 1);
                w.cnt[c] = 0;
            }
        }
        a = b;
    }
}

// ---------------------------------------------------------------- main
int main() {
    T0 = now_s();
    const char* e;
    std::string DATA = (e = getenv("DATA_DIR")) ? e : "/data";
    std::string OUT = (e = getenv("OUTPUT_DIR")) ? e : "/output";
    NT = (e = getenv("NCPU")) ? atoi(e) : (int)std::thread::hardware_concurrency();
    if (NT < 1) NT = 1;
    if (NT > 64) NT = 64;

    load_small_tables(DATA);
    logt("small tables loaded");
    const size_t ncamp = g_campid.size();

    std::vector<MFile> evfiles = map_dir(DATA + "/events");
    std::vector<MFile> clfiles = map_dir(DATA + "/clicks");
    std::vector<Chunk> evchunks = make_chunks(evfiles, 4u << 20);
    std::vector<Chunk> clchunks = make_chunks(clfiles, 4u << 20);

    // phase 1 (events) concurrently with link building
    std::vector<P1State*> p1(NT);
    for (auto& s : p1) s = new P1State();
    std::thread link_thread([&]() { build_links(DATA); });
    parallel_for(evchunks.size(), [&](size_t i, int t) { phase1_chunk(evchunks[i], *p1[t]); });
    logt("events parsed");
    link_thread.join();
    logt("links built");

    // phase 2 (clicks)
    std::vector<P2State*> p2(NT);
    for (auto& s : p2) s = new P2State();
    parallel_for(clchunks.size(), [&](size_t i, int t) { phase2_chunk(clchunks[i], *p2[t]); });
    logt("clicks parsed");

    // phase 3: dedup + resolve person
    std::vector<SessList*> p3(NT);
    for (auto& s : p3) s = new SessList[NPART];
    struct W3 {
        std::vector<EvRec> recs;
        std::vector<u32> tab;
        std::vector<u32> surv;
    };
    std::vector<W3> w3(NT);
    parallel_for(NPART, [&](size_t p, int t) {
        W3& w = w3[t];
        w.recs.clear();
        for (int x = 0; x < NT; x++) {
            p1[x]->parts[p].append_to(w.recs);
            p1[x]->parts[p].release();
        }
        const size_t N = w.recs.size();
        size_t cap = 16;
        while (cap < N * 2) cap <<= 1;
        w.tab.assign(cap, 0xFFFFFFFFu);
        const size_t mask = cap - 1;
        for (u32 i = 0; i < N; i++) {
            const EvRec& r = w.recs[i];
            size_t s = r.eh & mask;
            for (;;) {
                u32 j = w.tab[s];
                if (j == 0xFFFFFFFFu) { w.tab[s] = i; break; }
                const EvRec& q = w.recs[j];
                if (q.eh == r.eh && pl_len(q.epl) == pl_len(r.epl) &&
                    memcmp(pl_ptr(q.epl), pl_ptr(r.epl), pl_len(r.epl)) == 0) {
                    if (r.ing_s < q.ing_s) w.tab[s] = i;
                    break;
                }
                s = (s + 1) & mask;
            }
        }
        w.surv.clear();
        for (size_t s = 0; s < cap; s++) {
            u32 j = w.tab[s];
            if (j != 0xFFFFFFFFu && (w.recs[j].flags & 1)) w.surv.push_back(j);
        }
        const size_t NS = w.surv.size();
        const size_t PF = 16;
        for (size_t i = 0; i < NS; i++) {
            if (i + PF < NS) g_links.prefetch(w.recs[w.surv[i + PF]].uh);
            const EvRec& r = w.recs[w.surv[i]];
            u64 key, npl;
            resolve_person(r.uh, r.upl, key, npl);
            SessRec sr;
            sr.key = key;
            sr.ts = r.ts;
            sr.meta = (u64)(r.pc & 0xFFFFFF) | ((u64)(r.pc >> 24) << 32) | ((u64)((r.flags >> 1) & 3) << 40);
            sr.npl = npl;
            p3[t][key >> (64 - PBITS)].push(sr);
        }
    });
    for (auto& s : p1) delete s;
    logt("deduped, persons resolved");

    // phase 4: sessionize
    std::vector<W4> w4(NT);
    const i32 catn = (i32)g_cats.size();
    for (auto& w : w4) {
        w.acc.assign((ncamp + 1) * (size_t)ND, Acc{0, 0, 0, 0});
        w.cnt.assign(ncamp + 1, 0);
        w.since.assign(ncamp + 1, 0);
        w.catsum.assign(catn + 1, 0);
    }
    parallel_for(NPART, [&](size_t q, int t) {
        W4& w = w4[t];
        w.E.clear();
        w.C.clear();
        for (int x = 0; x < NT; x++) {
            p3[x][q].append_to(w.E);
            p3[x][q].release();
            p2[x]->parts[q].append_to(w.C);
            p2[x]->parts[q].release();
        }
        process_partition(w, ncamp);
    });
    logt("sessions aggregated");

    // outputs
    std::string udir = OUT + "/user_daily", cdir = OUT + "/campaign_daily";
    mkdir(OUT.c_str(), 0777);
    mkdir(udir.c_str(), 0777);
    mkdir(cdir.c_str(), 0777);
    {
        std::vector<PCol> cols = {{"user_id", 6, 0, false}, {"day", 1, 6, false},          {"sessions", 2, -1, false},
                                  {"events", 2, -1, false}, {"revenue_cents", 2, -1, false}, {"top_category", 6, 0, true}};
        std::vector<std::thread> th;
        for (int t = 0; t < NT; t++)
            th.emplace_back([&, t]() {
                char nm[64];
                snprintf(nm, sizeof nm, "/part-%d.parquet", t);
                write_parquet(udir + nm, cols, w4[t].uo.rgs);
            });
        for (auto& x : th) x.join();
    }
    logt("user_daily written");
    {
        std::vector<Acc> tot((ncamp + 1) * (size_t)ND, Acc{0, 0, 0, 0});
        for (auto& w : w4)
            for (size_t i = 0; i < tot.size(); i++) {
                tot[i].s += w.acc[i].s;
                tot[i].ps += w.acc[i].ps;
                tot[i].rev += w.acc[i].rev;
                tot[i].as += w.acc[i].as;
            }
        std::vector<PCol> cols = {{"campaign_id", 6, 0, false}, {"channel", 6, 0, false},
                                  {"day", 1, 6, false},         {"sessions", 2, -1, false},
                                  {"purchasing_sessions", 2, -1, false}, {"revenue_cents", 2, -1, false},
                                  {"assisted_sessions", 2, -1, false}};
        std::vector<RowGroup> rgs(1);
        RowGroup& g = rgs[0];
        g.cols.resize(7);
        for (size_t c = 0; c <= ncamp; c++)
            for (int d = 0; d < ND; d++) {
                const Acc& A = tot[c * ND + d];
                if (A.s == 0 && A.as == 0) continue;
                const std::string& id = c < ncamp ? g_campid[c] : std::string("organic");
                const std::string& ch = c < ncamp ? g_campch[c] : std::string("organic");
                put_str(g.cols[0].data, id.data(), id.size());
                put_str(g.cols[1].data, ch.data(), ch.size());
                put32(g.cols[2].data, (u32)(i32)(WS_DAY + d));
                put64(g.cols[3].data, (u64)A.s);
                put64(g.cols[4].data, (u64)A.ps);
                put64(g.cols[5].data, (u64)A.rev);
                put64(g.cols[6].data, (u64)A.as);
                g.rows++;
            }
        write_parquet(cdir + "/part-0.parquet", cols, rgs);
    }
    logt("campaign_daily written; done");
    // skip destructors / munmap: exit immediately
    fflush(stderr);
    _exit(0);
}
