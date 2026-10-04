#!/usr/bin/env python3
"""Gate 4: WIRE REGRESSION TEST using a pprof fixture (Go runtime/pprof output, real producer).
Since 2026-10-04 (find-event-scope) pprof is out of VIREL's scope: this is NOT a format profile.
It is kept because it exercises wire rules every VIREL stream depends on: pooled-reference
encoding (bare keys for non-string pools), Varint128 9-byte values, and per-pool skippable
state snapshots (find-pool-skip (b) ratified).

Stage A: decode the pprof protobuf (generic wire reader, NO protoc; field
         map verified against GOROOT/src/internal/profile/encode.go, the
         hand-written reference encoder) and cross-check structurally
         against `go tool pprof -raw`.
Stage B: emit native VIREL chunks (one per profile; types in the DYNAMIC id range,
         since the withdrawn otlp-profiles module no longer exists: pooled
         valuetype/function/mapping/location + sample rows as events with a
         test-only filler time word), reconstruct and prove exact
         fidelity + MODEL-LINT.
"""
import sys, os, struct, gzip, subprocess, time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from tcf_reader import Reader

GOROOT = "/usr/local/Cellar/go/1.27.1/libexec"
PPROF = f"{GOROOT}/bin/go"
MASK64 = (1 << 64) - 1

# ---------------- generic protobuf wire reader ----------------
def rd_varint(d, p):
    v = sh = 0
    while True:
        b = d[p]; p += 1
        v |= (b & 0x7F) << sh
        if b < 0x80: return v, p
        sh += 7

def packed(d):
    out, p = [], 0
    while p < len(d):
        v, p = rd_varint(d, p)
        out.append(v)
    return out

def rep_u64(f):
    out = []
    for v in f:
        out += packed(v) if isinstance(v, bytes) else [v]
    return out

def rep_i64(f):
    return [(x - (1 << 64)) if x > (1 << 63) - 1 else x for x in rep_u64(f)]

def fields(d):
    p = 0; out = defaultdict(list)
    while p < len(d):
        key, p2 = rd_varint(d, p)
        fno, wt = key >> 3, key & 7
        p = p2
        if wt == 0: v, p = rd_varint(d, p)
        elif wt == 1: v = struct.unpack_from("<q", d, p)[0]; p += 8
        elif wt == 2: n, p = rd_varint(d, p); v = d[p:p+n]; p += n
        elif wt == 5: v = struct.unpack_from("<i", d, p)[0]; p += 4
        else: raise IOError(f"wire type {wt}")
        out[fno].append(v)
    return out

def i64(x):
    return (x - (1 << 64)) if x > (1 << 63) - 1 else x

# ------------- stage A: pprof parse (field map from encode.go) --------------
def parse_pprof(raw):
    f = fields(raw)
    st = [b.decode("utf-8", "replace") for b in f.get(6, [])]
    assert st and st[0] == "", "string_table[0] must be ''"
    prof = {
        "strings": st,
        "sample_types": [(fields(v).get(1, [None])[0], fields(v).get(2, [None])[0])
                         for v in f.get(1, [])],
        "samples": [], "mappings": [], "locations": [], "functions": [],
        "time_nanos": i64(f.get(9, [0])[0]),
        "duration_nanos": i64(f.get(10, [0])[0]),
        "period": i64(f.get(12, [0])[0]),
        "period_type": None, "comments": rep_u64(f.get(13, [])),
        "drop_frames": f.get(7, [None])[0], "keep_frames": f.get(8, [None])[0],
        "default_sample_type": f.get(14, [None])[0],
    }
    pt = f.get(11, [])
    if pt:
        pf = fields(pt[0])
        prof["period_type"] = (pf.get(1, [None])[0], pf.get(2, [None])[0])
    for v in f.get(2, []):                              # Sample
        s = fields(v)
        locs = rep_u64(s.get(1, []))
        values = rep_i64(s.get(2, []))
        labels = []
        for lb in s.get(3, []):
            g = fields(lb)
            key = g.get(1, [None])[0]
            if g.get(2): labels.append((key, i64(g[2][0]), None))
            elif g.get(3): labels.append((key, None, i64(g[3][0])))
        prof["samples"].append((locs, values, labels))
    for v in f.get(3, []):                              # Mapping
        g = fields(v)
        prof["mappings"].append((
            g.get(1, [0])[0], g.get(2, [0])[0], g.get(3, [0])[0], g.get(4, [0])[0],
            g.get(5, [None])[0], g.get(6, [None])[0],
            bool(g.get(7, [0])[0]), bool(g.get(8, [0])[0]),
            bool(g.get(9, [0])[0]), bool(g.get(10, [0])[0])))
    for v in f.get(4, []):                              # Location
        g = fields(v)
        lines = [(fields(ln).get(1, [None])[0], fields(ln).get(2, [0])[0])
                 for ln in g.get(4, [])]
        prof["locations"].append((
            g.get(1, [0])[0], g.get(2, [0])[0], g.get(3, [0])[0], lines))
    for v in f.get(5, []):                              # Function
        g = fields(v)
        prof["functions"].append((
            g.get(1, [0])[0], g.get(2, [None])[0], g.get(3, [None])[0],
            g.get(4, [None])[0], i64(g.get(5, [0])[0])))
    validate_pprof(prof)
    return prof

def validate_pprof(p):
    st = p["strings"]; lmap = {l[0]: l for l in p["locations"]}
    fmap = {ff[0]: ff for ff in p["functions"]}; mmap = {m[0]: m for m in p["mappings"]}
    for (locs, _v, _l) in p["samples"]:
        for lid in locs:
            assert lid in lmap, f"sample references missing location {lid}"
    for l in p["locations"]:
        if l[1]: assert l[1] in mmap, f"location {l[0]} missing mapping {l[1]}"
        for (fid, _ln) in l[3]:
            assert fid in fmap, f"location {l[0]} missing function {fid}"
    for (ti, ui) in p["sample_types"]:
        assert 0 <= ti < len(st) and 0 <= ui < len(st), "value-type index range"

# ---------------- golden: go tool pprof -raw ----------------
def golden_raw(path):
    out = subprocess.run([f"{PPROF}", "tool", "pprof", "-raw", path],
                         capture_output=True, text=True, timeout=60)
    if out.returncode != 0:
        raise IOError(f"go tool pprof -raw failed: {out.stderr[:400]}")
    return out.stdout

def _fileline(flcol):
    """'path/to/file.go:21:0' -> (path, 21)."""
    head, _, col = flcol.rpartition(":")            # head = 'path.go:21', col = '0'
    if col.isdigit() and head:
        path, _, line = head.rpartition(":")        # path = 'path.go', line = '21'
        if line.isdigit(): return path, int(line)
        return head, 0
    return flcol, 0

def parse_golden(raw):
    g = {"samples": [], "locations": {}, "mappings": {}, "functions": set(),
         "period": None, "period_type": None, "sample_types": []}
    sec, last_loc = None, None
    for rawl in raw.splitlines():
        ln = rawl.rstrip()
        if not ln.strip(): continue
        if ln.startswith("PeriodType:"):
            _, t, u = ln.split(None, 2)
            g["period_type"] = (t, u); sec = None; continue
        if ln.startswith("Period:"): g["period"] = int(ln.split()[1]); sec = None; continue
        if ln.startswith("Time:") or ln.startswith("Duration:"): sec = None; continue
        if ln.startswith("Samples:"): sec = "cols"; continue
        if ln.startswith("Locations"): sec = "loc"; continue
        if ln.startswith("Mappings"): sec = "map"; continue
        t = ln.lstrip()
        if sec == "cols":
            for tok in t.split():
                tp, _, un = tok.partition("/")
                g["sample_types"].append((tp, un.split("[")[0]))
            sec = "rows"; continue
        if sec == "rows":
            if ": " in t:                               # sample row: count/values/locs
                body, _, rest = t.partition(": ")
                nums = [int(x) for x in body.split()]
                g["samples"].append([
                    nums[0], nums[1:], [int(x) for x in rest.split()], []])
            else:                                        # label row: no space after ':'
                row = g["samples"][-1]
                for tok in t.split():
                    k, _, v = tok.partition(":[")
                    v = v.rstrip("]")
                    try: row[3].append((k, None, int(v)))
                    except ValueError: row[3].append((k, v, None))
            continue
        if sec == "loc":
            toks = t.split()
            if toks[0].endswith(":") and toks[0][:-1].isdigit():
                lid = int(toks[0][:-1])
                addr = int(toks[1], 16)
                mmi = int(toks[2][2:]) if toks[2].startswith("M=") else 0
                sline = int(toks[-1][2:]); toks.pop()
                flcol = toks.pop()
                fname = " ".join(toks[3:])
                path, lineno = _fileline(flcol)
                g["locations"][lid] = [addr, mmi, [(fname, path, lineno, sline)]]
                g["functions"].add((fname, path, sline))
                last_loc = lid
            elif last_loc is not None and len(toks) >= 2:
                sline = int(toks[-1][2:]); toks.pop()
                flcol = toks.pop()
                fname = " ".join(toks)
                path, lineno = _fileline(flcol)
                g["locations"][last_loc][2].append((fname, path, lineno, sline))
                g["functions"].add((fname, path, sline))
            continue
        if sec == "map":
            tid, _, rest = t.partition(":")
            if not tid.strip().isdigit(): continue
            rest = rest.strip()
            ap, _, r2 = rest.partition("/")
            lp, _, r3 = r2.partition("/")
            op, _, tail = r3.partition(" ")
            if " [" in tail: tail = tail.split(" [")[0]
            tail = tail.strip()
            g["mappings"][int(tid)] = (int(ap, 16), int(lp, 16), int(op, 16), tail or None)
    return g

def crosscheck(prof, got, name):
    st = prof["strings"]; errs = []
    mt = [(st[ti] if ti is not None else None, st[ui] if ui is not None else None)
          for (ti, ui) in prof["sample_types"]]
    if mt != got["sample_types"]:
        errs.append(f"sample_types {mt} vs {got['sample_types']}")
    if prof["period_type"]:
        gt = (st[prof["period_type"][0]], st[prof["period_type"][1]])
    else:
        gt = None
    if gt != got["period_type"]:
        errs.append(f"period_type {gt} vs {got['period_type']}")
    if got["period"] is not None and prof["period"] != got["period"]:
        errs.append(f"period {prof['period']} vs {got['period']}")
    if len(prof["samples"]) != len(got["samples"]):
        errs.append(f"sample rows {len(prof['samples'])} vs {len(got['samples'])}")
    for si, ((locs, vals, labels), row) in enumerate(zip(prof["samples"], got["samples"])):
        if [v & MASK64 for v in locs] != [x & MASK64 for x in row[2]]:
            errs.append(f"row {si} locs")
        if (vals[1:] if len(vals) > 1 else []) != row[1]:
            errs.append(f"row {si} values {vals} vs {row[1]} (count col {row[0]})")
        if vals and vals[0] != row[0]:
            errs.append(f"row {si} count {vals[0]} vs {row[0]}")
        gl = []
        for (ki, stri, numi) in labels:
            if stri is not None: gl.append((st[ki] if ki is not None else None,
                                            st[stri] if stri else None, None))
            else: gl.append((st[ki] if ki is not None else None, None, numi))
        if sorted(gl) != sorted(row[3]):
            errs.append(f"row {si} labels {gl} vs {row[3]}")
    for (lid, _mapi, addr, _lines) in prof["locations"]:
        gg = got["locations"].get(lid)
        if not gg:
            if loc[2] == 0 and not loc[3]: continue
            errs.append(f"location {lid} missing in golden"); continue
        if addr != gg[0]: errs.append(f"location {lid} addr {addr:#x} vs {gg[0]:#x}")
        if int(_mapi or 0) != int(gg[1] or 0): errs.append(f"location {lid} mapping")
    myf = {(st[a[1]], st[a[3]], a[4]) for a in prof["functions"] if a[1]}
    if myf != got["functions"]:
        errs.append("functions diff: "
                    f"missing {sorted(myf - got['functions'])[:2]} "
                    f"extra {sorted(got['functions'] - myf)[:2]}")
    for (mid, start, limit, offset, filei, bidi, hf, hfn, hln, hif) in prof["mappings"]:
        gg = got["mappings"].get(mid)
        if not gg:
            if not any((start, limit, offset)): continue
            errs.append(f"mapping {mid} missing in golden"); continue
        if (start, limit, offset) != gg[:3]:
            errs.append(f"mapping {mid} addr {(start, limit, offset):#} vs {gg[:3]}")
        gf = st[filei] if filei is not None else None
        if gf != gg[3]:
            errs.append(f"mapping {mid} file {gf!r} vs {gg[3]!r}")
    return errs

# ---------------- stage B: emit native VIREL (dynamic type ids) -------------
M3 = 1 << 30          # first DYNAMIC type id (spec §5.3); was module 3, now withdrawn
TVT, TFUN, TMAP, TLOC, TLINE, TLBL, TSMP, TPER, TSTS, TMSC = \
    M3 | 1, M3 | 2, M3 | 3, M3 | 4, M3 | 5, M3 | 6, M3 | 7, M3 | 8, M3 | 9, M3 | 10
T_SLB, T_SLE, T_INS, T_CNTR = 2, 3, 4, 5
T_LOSS, T_CLR, T_EOS, T_PROV = 6, 7, 8, 9
SIG = 0x80

def le_varint(v):
    if v < 0: v &= MASK64
    out = bytearray()
    while True:
        b = v & 0x7F; v >>= 7
        if v: out.append(b | 0x80)
        else: out.append(b); return bytes(out)

def sv64(v):
    """JFR Varint128: <= 9 bytes; when the raw 9th byte is used, ALL 8 group
    bytes carry the continuation bit (gate-3 lesson: sentinel 2**64-1)."""
    v &= MASK64
    if v < (1 << 56): return le_varint(v)
    out = bytearray(0x80 | ((v >> (7 * i)) & 0x7F) for i in range(8))
    out.append((v >> 56) & 0xFF)
    return bytes(out)

def s_utf8(s):
    b = s.encode(); return bytes([3]) + sv64(len(b)) + b

def sref(key):
    """string field value: NULL (tag 0, pprof index 0) | pool ref (tag 2)."""
    if key is None or key == 0: return bytes([0])
    return bytes([2]) + sv64(key)

def n_ref(key): return sv64(key if key is not None else 0)

def event(tid, body):
    inner = le_varint(tid) + body
    size = len(inner) + len(le_varint(len(inner)))
    return le_varint(size) + inner

def fnv1a64(data):
    h = 0xcbf29ce484222325
    for c in data:
        h ^= c; h = (h * 0x100000001b3) & MASK64
    return h

def arrays(fieldvals, elem_fn):
    out = sv64(len(fieldvals))
    for v in fieldvals: out += elem_fn(v)
    return out

def metadata_event():
    st = ["root", "metadata", "region", "class", "field", "name", "id", "superType",
          "constantPool", "dimension",
          "jdk.jfr.Event", "virel.Type", "virel.String",
          "virel.slice.begin", "virel.slice.end", "virel.instant", "virel.counter",
          "virel.loss", "virel.clear", "virel.eos", "virel.provenance",
          "track", "value", "writer", "span",
          "source_format", "source_format_version", "converter", "fidelity",
          "original_payload_hash",
          "virel3.valuetype", "type", "unit",
          "virel3.function", "system_name", "filename", "start_line",
          "virel3.mapping", "limit", "offset", "file", "build_id",
          "has_functions", "has_filenames", "has_line_numbers", "has_inline_frames",
          "virel3.location", "mapping", "address", "lines",
          "virel3.line", "function", "line",
          "virel3.label", "key", "str", "num",
          "virel3.sample", "locations", "values", "labels",
          "virel3.period",
          "virel3.sampletypes", "types",
          "virel3.misc", "drop_frames", "keep_frames", "default_sample_type", "comments",
          "boolean", "long", "value2", "gmtOffset", "locale", "en_GB", "start"]
    st += [str(n) for n in list(range(0, 16)) + [100, 101, 102] +
           [M3 | k for k in range(1, 11)]]
    def ix(s):
        try:
            return st.index(s)
        except ValueError:
            raise SystemExit(f"MISSING-META-STRING {s!r}")
    def elem(nm, attrs, children):
        out = sv64(ix(nm)) + sv64(len(attrs))
        for (an, av) in attrs: out += sv64(ix(an)) + sv64(ix(av))
        out += sv64(len(children))
        for ch in children: out += ch
        return out
    def field(fn, tid, cp=False, dim=False):
        a = [("name", fn), ("class", str(tid))]
        if cp: a.append(("constantPool", "0"))
        if dim: a.append(("dimension", "1"))
        return elem("field", a, [])
    def cls(tid, nm, sup, flds, pool=False):
        a = [("name", nm), ("id", str(tid))]
        if sup: a.append(("superType", sup))
        if pool: a.append(("constantPool", "0"))
        return elem("class", a, [field(*f) for f in flds])
    types = [
        cls(100, "long", None, []),
        cls(101, "virel.String", None, []),
        cls(102, "boolean", None, []),
        cls(T_SLB, "virel.slice.begin", "jdk.jfr.Event",
            [("track", 100), ("value", 101, True)]),
        cls(T_SLE, "virel.slice.end", "jdk.jfr.Event", [("track", 100)]),
        cls(T_INS, "virel.instant", "jdk.jfr.Event",
            [("track", 100), ("value", 101, True)]),
        cls(T_CNTR, "virel.counter", "jdk.jfr.Event",
            [("track", 100), ("value", 100)]),
        cls(T_LOSS, "virel.loss", "jdk.jfr.Event", [("writer", 100), ("span", 100)]),
        cls(T_CLR, "virel.clear", "jdk.jfr.Event", [("writer", 100)]),
        cls(T_EOS, "virel.eos", "jdk.jfr.Event", []),
        cls(T_PROV, "virel.provenance", "jdk.jfr.Event",
            [("source_format", 101, True), ("source_format_version", 101, True),
             ("converter", 101, True), ("fidelity", 100),
             ("original_payload_hash", 100)]),
        cls(TVT, "virel3.valuetype", None,
            [("type", 101, True), ("unit", 101, True)], pool=True),
        cls(TFUN, "virel3.function", None,
            [("name", 101, True), ("system_name", 101, True), ("filename", 101, True),
             ("start_line", 100)], pool=True),
        cls(TMAP, "virel3.mapping", None,
            [("start", 100), ("limit", 100), ("offset", 100),
             ("file", 101, True), ("build_id", 101, True),
             ("has_functions", 102), ("has_filenames", 102),
             ("has_line_numbers", 102), ("has_inline_frames", 102)], pool=True),
        cls(TLOC, "virel3.location", None,
            [("mapping", TMAP, True), ("address", 100),
             ("lines", TLINE, False, True)], pool=True),
        cls(TLINE, "virel3.line", None,
            [("function", TFUN, True), ("line", 100)]),
        cls(TLBL, "virel3.label", None,
            [("key", 101, True), ("str", 101, True), ("num", 100)]),
        cls(TSMP, "virel3.sample", "jdk.jfr.Event",
            [("locations", TLOC, True, True), ("values", 100, False, True),
             ("labels", TLBL, False, True)]),
        cls(TPER, "virel3.period", "jdk.jfr.Event",
            [("type", TVT, True), ("value", 100)]),
        cls(TSTS, "virel3.sampletypes", "jdk.jfr.Event",
            [("types", TVT, True, True)]),
        cls(TMSC, "virel3.misc", "jdk.jfr.Event",
            [("drop_frames", 101, True), ("keep_frames", 101, True),
             ("default_sample_type", 101, True), ("comments", 101, True, True)]),
    ]
    region = elem("region", [("gmtOffset", "0"), ("locale", "en_GB")], [])
    meta = elem("metadata", [], types)
    root = elem("root", [], [meta, region])
    payload = sv64(len(st)) + b"".join(
        bytes([3]) + sv64(len(x.encode())) + x.encode() for x in st) + root
    return event(0, sv64(0) + sv64(0) + sv64(1) + bytes([0]) + payload)

def pools_event(pools):
    body = sv64(0) + sv64(0) + sv64(0) + bytes([1]) + sv64(len(pools))
    for (tid, entries) in pools:
        body += le_varint(tid) + le_varint(len(entries))
        for (key, val) in entries:
            body += le_varint(key) + val
    return event(1, body)

def to_tcf(profs, srcnames, srcbytes):
    """one native chunk per profile; returns (container, per-chunk report)"""
    out = bytearray(); per = []
    writer_base = 0xA000000000000000
    for p, srcname, srcraw in zip(profs, srcnames, srcbytes):
        meta = metadata_event()
        # pools: strings (pprof indices as keys), valuetype/function/mapping/location
        str_entries = [(i, bytes([3]) + sv64(len(s.encode())) + s.encode())
                       for i, s in enumerate(p["strings"]) if i != 0]
        fun_entries = [(f[0], sref(f[1]) + sref(f[2]) + sref(f[3]) + sv64(f[4]))
                       for f in sorted(p["functions"], key=lambda x: x[0])]
        map_entries = [(m[0], sv64(m[1]) + sv64(m[2]) + sv64(m[3]) + sref(m[4]) + sref(m[5])
                        + bytes([1 if m[6] else 0]) + bytes([1 if m[7] else 0])
                        + bytes([1 if m[8] else 0]) + bytes([1 if m[9] else 0]))
                       for m in sorted(p["mappings"], key=lambda x: x[0])]
        loc_entries = []
        for l in sorted(p["locations"], key=lambda x: x[0]):
            (_lid, mapi, addr, lines) = l
            body = (sv64(mapi or 0) + sv64(addr) +
                    arrays([x for x in lines], lambda x: sv64(x[0]) + sv64(x[1])))
            loc_entries.append((l[0], body))
        vt_entries = [(i + 1, sref(ti) + sref(ui)) for i, (ti, ui) in enumerate(p["sample_types"])]
        vt_pt = len(p["sample_types"]) + 1
        vt_entries.append((vt_pt, sref(p["period_type"][0] if p["period_type"] else None)
                           + sref(p["period_type"][1] if p["period_type"] else None)))
        cp = pools_event([(101, str_entries), (TVT, vt_entries),
                          (TFUN, fun_entries), (TMAP, map_entries), (TLOC, loc_entries)])
        body_events = []
        body_events.append((TSTS, sv64(0) + arrays([i + 1 for i in range(len(p["sample_types"]))], n_ref)))
        body_events.append((TPER, sv64(0) + n_ref(vt_pt) + sv64(p["period"])))
        misc = (sref(p["drop_frames"]) + sref(p["keep_frames"]) +
                sref(p["default_sample_type"]) + arrays(p["comments"], sref))
        body_events.append((TMSC, sv64(0) + misc))
        for (locs, vals, labels) in p["samples"]:
            body = (arrays([x for x in locs], n_ref) + arrays(vals, sv64) +
                    arrays(labels, lambda x: sref(x[0]) + sref(x[1]) + sv64(x[2] or 0)))
            body_events.append((TSMP, sv64(0) + body))    # filler time word: aggregated rows
        hdr = bytearray(76)
        hdr[0:4] = b"VIRL"   # VIREL native magic
        struct.pack_into(">HH", hdr, 4, 0, 1)
        struct.pack_into(">Q", hdr, 32, p["time_nanos"] & MASK64)
        struct.pack_into(">Q", hdr, 40, p["duration_nanos"] & MASK64)   # ticks == ns
        struct.pack_into(">Q", hdr, 56, 1_000_000_000)                  # ns axis
        struct.pack_into(">B", hdr, 64, 0)
        struct.pack_into(">H", hdr, 66, SIG << 8)
        hdr[68:76] = struct.pack(">Q", writer_base |
                                 (fnv1a64(("pprof:" + srcname).encode()) & 0x0FFFFFFFFFFFFFFF))
        struct.pack_into(">Q", hdr, 24, 76)
        struct.pack_into(">Q", hdr, 16, 76 + len(meta))
        chunk_start = len(out)
        out += hdr + meta + cp
        prov = (s_utf8("pprof") + s_utf8("1") + s_utf8("pprof_gate.py/0.1") +
                sv64(0) + sv64(fnv1a64(srcraw)))
        out += event(T_PROV, sv64(0) + prov)
        for (tid, body) in body_events:
            out += event(tid, body)
        out += event(T_EOS, b"")
        struct.pack_into(">Q", out, chunk_start + 8, len(out) - chunk_start)
        per.append((srcname, len(body_events)))
    return bytes(out), per

# ---------------- fidelity verify (reconstruct from TCF) -------------------
def _unw(v):
    return v[1] if isinstance(v, tuple) and v and v[0] == "@key" else v

def verify_chunk(container, chunk_start, chunk_len, prof, srcname):
    ch = container[chunk_start:chunk_start + chunk_len]
    r = Reader(); r.record = True
    r.run(ch)
    errs = []
    st = prof["strings"]
    # header
    sz, = struct.unpack_from(">Q", ch, 8)
    tps, = struct.unpack_from(">Q", ch, 56)
    startns, = struct.unpack_from(">Q", ch, 32)
    durns, = struct.unpack_from(">Q", ch, 40)
    if tps != 1_000_000_000: errs.append(f"tps {tps}")
    if startns != (prof["time_nanos"] & MASK64): errs.append("startNanos")
    if durns != (prof["duration_nanos"] & MASK64): errs.append("durationNanos")
    sig = struct.unpack_from(">H", ch, 66)[0]
    if not (sig >> 8) & 0x80: errs.append("signature bit missing")
    wid, = struct.unpack_from(">Q", ch, 68)
    if wid == 0: errs.append("zero writer id")
    if r.stats.get("<eos>") or r.stats.get("virel.eos") != 1: errs.append(f"eos {r.stats.get('virel.eos')}")
    if r.stats.get("virel.provenance") != 1: errs.append(f"prov {r.stats.get('virel.provenance')}")
    spool = r.pools.get(101, {})
    if {k: v for k, v in spool.items()} != {k: s for k, s in enumerate(st) if k != 0}:
        errs.append(f"string pool diff (got {len(spool)}, want {len(st)-1})")
    vt_entries = r.pools.get(TVT, {})
    want_vt = [(i + 1, (st[ti] if ti else None, st[ui] if ui else None))
               for i, (ti, ui) in enumerate(prof["sample_types"])]
    want_vt.append((len(prof["sample_types"]) + 1,
                    ((st[prof["period_type"][0]] if prof["period_type"] and prof["period_type"][0] else None),
                     (st[prof["period_type"][1]] if prof["period_type"] and prof["period_type"][1] else None))))
    got_vt = {k: (v["type"] if isinstance(v, dict) else None,
                  v["unit"] if isinstance(v, dict) else None) for k, v in vt_entries.items()}
    if dict(want_vt) != got_vt: errs.append(f"valuetype pool {got_vt} vs {want_vt}")
    fun_entries = r.pools.get(TFUN, {})
    for f in prof["functions"]:
        got = fun_entries.get(f[0])
        if not isinstance(got, dict): errs.append(f"function {f[0]} missing"); continue
        want = (st[f[1]] if f[1] else None, st[f[2]] if f[2] else None,
                st[f[3]] if f[3] else None, f[4])
        have = (got.get("name"), got.get("system_name"), got.get("filename"), got.get("start_line"))
        if have != want:
            errs.append(f"function {f[0]} {have} vs {want}")
    map_entries = r.pools.get(TMAP, {})
    for m in prof["mappings"]:
        got = map_entries.get(m[0])
        if not isinstance(got, dict): errs.append(f"mapping {m[0]} missing"); continue
        want = (m[1], m[2], m[3], st[m[4]] if m[4] else None, st[m[5]] if m[5] else None,
                m[6], m[7], m[8], m[9])
        have = (got.get("start"), got.get("limit"), got.get("offset"), got.get("file"),
                got.get("build_id"), bool(got.get("has_functions")), bool(got.get("has_filenames")),
                bool(got.get("has_line_numbers")), bool(got.get("has_inline_frames")))
        if have != want: errs.append(f"mapping {m[0]} diff")
    loc_entries = r.pools.get(TLOC, {})
    for l in prof["locations"]:
        got = loc_entries.get(l[0])
        if not isinstance(got, dict): errs.append(f"location {l[0]} missing"); continue
        if int(_unw(got.get("mapping")) or 0) != int(l[1] or 0): errs.append(f"loc {l[0]} mapping")
        if got.get("address") != l[2]: errs.append(f"loc {l[0]} addr")
        want_lines = [(x[0], x[1]) for x in l[3]]
        have_lines = [(_unw(x.get("function")), x.get("line"))
                      for x in (got.get("lines") or [])]
        if have_lines != want_lines:
            errs.append(f"loc {l[0]} lines {have_lines} vs {want_lines}")
    # sample rows in order
    rows = [(t, v) for (t, _s, _d, v) in r.records if t == TSMP]
    if len(rows) != len(prof["samples"]):
        errs.append(f"sample rows {len(rows)} vs {len(prof['samples'])}")
    for i, ((t, v), (locs, vals, labels)) in enumerate(zip(rows, prof["samples"])):
        got_locs = [int(_unw(x) or 0) for x in (v.get("locations") or [])]
        if got_locs != [x & MASK64 for x in locs]:
            errs.append(f"row {i} locs {got_locs} vs {locs}")
        got_vals = v.get("values") or []
        if [x & MASK64 for x in got_vals] != [x & MASK64 for x in vals]:
            errs.append(f"row {i} values {got_vals} vs {vals}")
        got_lbl = []
        for x in (v.get("labels") or []):
            if not isinstance(x, dict): continue
            k = x.get("key"); s_ = x.get("str"); n_ = x.get("num")
            if s_ is not None: got_lbl.append((k, s_, None))
            else: got_lbl.append((k, None, n_ & MASK64))
        want_lbl = []
        for (ki, stri, numi) in labels:
            k = st[ki] if ki is not None else None
            if stri is not None: want_lbl.append((k, st[stri] if stri else None, None))
            else: want_lbl.append((k, None, numi))
        if sorted(map(str, got_lbl)) != sorted(map(str, want_lbl)):
            errs.append(f"row {i} labels {got_lbl} vs {want_lbl}")
    # sampletypes / period / misc events
    return errs, sz

# ---------------- main ----------------
def main(fixture_cpu="/tmp/tcf-pprof-cpu.pprof", fixture_heap="/tmp/tcf-pprof-heap.pprof"):
    t0 = time.time()
    raws = [gzip.open(p, "rb").read() for p in (fixture_cpu, fixture_heap)]
    names = [os.path.basename(p)[:-6] for p in (fixture_cpu, fixture_heap)]
    profs = [parse_pprof(x) for x in raws]
    print(f"stage A: parsed {len(profs)} profiles:",
          [(len(p["samples"]) and len(p["samples"]), len(p["locations"]), len(p["functions"]),
            len(p["mappings"]), len(p["strings"])) for p in profs])
    ok = True
    for p, path, nm in zip(profs, (fixture_cpu, fixture_heap), names):
        got = parse_golden(golden_raw(path))
        errs = crosscheck(p, got, nm)
        if errs:
            ok = False
            print(f"  {nm}: PARITY FAIL:")
            for e in errs[:6]: print("   ", e)
        else:
            print(f"  {nm}: parity vs gotool -raw OK "
                  f"(rows={len(p['samples'])}, locs={len(p['locations'])}, "
                  f"funcs={len(p['functions'])}, maps={len(p['mappings'])})")
    if not ok: print("GATE 4: FAILED"); return 1
    container, per = to_tcf(profs, names, raws)
    path_out = "/tmp/tcf-native-from-pprof.tcf"
    open(path_out, "wb").write(container)
    print(f"VIREL emit: {sum(map(len, raws))} B -> {len(container)} B, {len(per)} chunks -> {path_out}")
    pos = 0; ci = 0
    while pos < len(container):
        sz, = struct.unpack_from(">Q", container, pos + 8)
        errs, _ = verify_chunk(container, pos, sz, profs[ci], names[ci])
        print(f"chunk {ci} ({names[ci]}): {'FAIL: ' + '; '.join(errs[:4]) if errs else 'verified OK'}")
        if errs: ok = False
        pos += sz; ci += 1
    if ok: print(f"GATE 4: PASSED ({time.time()-t0:.2f}s)")
    else: print("GATE 4: FAILED")
    return 0 if ok else 1

if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
