#!/usr/bin/env python3
"""Gate 5: Tier B wedge #1 — Chrome Trace Event Format (CTEF) JSON and
V8 .cpuprofile JSON -> native VIREL (and back to CTEF).

Fixtures are REAL producers: a real Chrome DevTools trace export
(/tmp/tcf-ctef-real.json, 1866 B/E events, 4 processes) and a real
`node --cpu-prof` profile. The real trace export is not reproducible, so
make_fixtures.sh generates an equivalent-shape synthetic stand-in
(/tmp/tcf-ctef-synthetic.json, 4 processes, nested B/E + i); the real
one is used when present. Tier B = vocabulary level: fidelity is per
documented shape transform (X -> B/E pair; E names recovered by nesting;
cpuprofile tree -> pooled leaf-first stacks), not byte identity.
"""
import sys, os, struct, json, time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from tcf_reader import Reader

MASK64 = (1 << 64) - 1
B2 = 2 << 18                    # m2 exec-trace module base (as in gate 3)
T_SLB, T_SLE, T_INS, T_CNTR = 2, 3, 4, 5
T_LOSS, T_CLR, T_EOS, T_PROV = 6, 7, 8, 9
T_STACK, T_FRAME, T_SMP = B2 | 2, B2 | 1, B2 | 17
SIG = 0x80

def le_varint(v):
    if v < 0: v &= MASK64
    out = bytearray()
    while True:
        b = v & 0x7F; v >>= 7
        if v: out.append(b | 0x80)
        else: out.append(b); return bytes(out)

def sv64(v):
    """JFR Varint128: <= 9 bytes; all 8 group bytes carry the cont bit when
    the raw 9th byte is used (gates 3/4 invariant)."""
    v &= MASK64
    if v < (1 << 56): return le_varint(v)
    out = bytearray(0x80 | ((v >> (7 * i)) & 0x7F) for i in range(8))
    out.append((v >> 56) & 0xFF)
    return bytes(out)

def s_utf8(s):
    b = s.encode(); return bytes([3]) + sv64(len(b)) + b

def sref(key):
    if key is None or key == 0: return bytes([0])
    return bytes([2]) + sv64(key)

def event(tid, body):
    inner = le_varint(tid) + body
    size = len(inner) + len(le_varint(len(inner)))
    return le_varint(size) + inner

def fnv1a64(data):
    h = 0xcbf29ce484222325
    for c in data:
        h ^= c; h = (h * 0x100000001b3) & MASK64
    return h

def arrays(vals, fn):
    return sv64(len(vals)) + b"".join(fn(v) for v in vals)

def _unw(v):
    return v[1] if isinstance(v, tuple) and v and v[0] in ("@key", "@poolref") else v

# Track namespaces (m2 module conventions, extended for CTEF; documented):
def track_thread(pid, tid):
    return (1 << 40) + ((pid & 0xFFFFF) << 24) + (tid & 0xFFFFFF)
def track_counter(pid, ordinal):
    return (8 << 44) + ((pid & 0xFFFFF) << 16) + (ordinal & 0xFFFF)
def track_async(pid, corr):
    return (9 << 44) + (fnv1a64(f"{pid}:{corr}".encode()) & 0x0FFFFFFF)

# ---------------- canonical CTEF event model -------------------------------
SUPPORTED = ("B", "E", "X", "i", "I", "C", "S", "T", "F", "b", "e", "n")

def ctef_ingest(path):
    data = json.load(open(path))
    evs = data["traceEvents"] if isinstance(data, dict) else data
    model = []
    for e in evs:
        ph = e.get("ph", "?")
        if ph not in SUPPORTED: continue
        model.append({
            "ph": ph, "pid": e.get("pid", 0), "tid": e.get("tid", 0),
            "ts": e.get("ts"), "dur": e.get("dur", 0) or 0,
            "name": e.get("name", ""), "cat": e.get("cat", ""),
            "id": e.get("id"), "args": e.get("args", {})})
    validate_ctef(evs, model)
    return model

def validate_ctef(raw, model):
    stacks, unmatched, nB, nE = defaultdict(list), 0, 0, 0
    for e in raw:
        if e.get("ph") == "B":
            stacks[(e.get("pid"), e.get("tid"), e.get("name"))].append(e.get("ts")); nB += 1
        elif e.get("ph") == "E":
            nE += 1
            if not stacks[(e.get("pid"), e.get("tid"), e.get("name"))]: unmatched += 1
            else: stacks[(e.get("pid"), e.get("tid"), e.get("name"))].pop()
    assert unmatched == 0, f"{unmatched} E without matching B"
    if nB != nE: raise SystemExit(f"B {nB} != E {nE}")
    for m in model: assert m["ts"] is not None, "ts required"

# ---------------- TCF metadata (shared) -------------------------------------
def metadata_event():
    st = ["root", "metadata", "region", "class", "field", "name", "id", "superType",
          "constantPool", "dimension",
          "jdk.jfr.Event", "virel.Type", "virel.String",
          "virel.slice.begin", "virel.slice.end", "virel.instant", "virel.counter",
          "virel.loss", "virel.clear", "virel.eos", "virel.provenance",
          "track", "value", "writer", "span",
          "source_format", "source_format_version", "converter", "fidelity",
          "original_payload_hash",
          "virel2.stack", "virel2.frame", "virel2.stack.sample", "function", "pc",
          "file", "line", "frames", "weight", "stack", "task",
          "boolean", "long", "gmtOffset", "locale", "en_GB"]
    st += [str(n) for n in list(range(0, 12)) + [100, 101, 102] + [B2 | k for k in (1, 2, 17, 18, 19)]]
    def ix(s):
        try: return st.index(s)
        except ValueError: raise SystemExit(f"MISSING-META-STRING {s!r}")
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
        cls(100, "long", None, []), cls(101, "virel.String", None, []),
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
        cls(T_FRAME, "virel2.frame", None,
            [("pc", 100), ("function", 101, True), ("file", 101, True),
             ("line", 100)]),
        cls(T_STACK, "virel2.stack", None, [("frames", T_FRAME, False, True)], pool=True),
        cls(T_SMP, "virel2.stack.sample", "jdk.jfr.Event",
            [("track", 100), ("task", 100), ("stack", T_STACK, True), ("weight", 100)]),
    ]
    region = elem("region", [("gmtOffset", "0"), ("locale", "en_GB")], [])
    meta = elem("metadata", [], types)
    root = elem("root", [], [meta, region])
    payload = sv64(len(st)) + b"".join(s_utf8(x) for x in st) + root
    return event(0, sv64(0) + sv64(0) + sv64(1) + bytes([0]) + payload)

def chunkify(hdr_kw, meta, cp, body_events, prov_fields, writer_id, hashbytes):
    hdr = bytearray(76)
    hdr[0:4] = b"VIRL"   # VIREL native magic
    struct.pack_into(">HH", hdr, 4, 0, 1)
    struct.pack_into(">Q", hdr, 32, hdr_kw.get("start_nanos", 0) & MASK64)
    struct.pack_into(">Q", hdr, 40, hdr_kw.get("duration_nanos", 0) & MASK64)
    struct.pack_into(">Q", hdr, 56, hdr_kw["tps"])
    struct.pack_into(">B", hdr, 64, 0)
    struct.pack_into(">H", hdr, 66, SIG << 8)
    hdr[68:76] = struct.pack(">Q", writer_id)
    struct.pack_into(">Q", hdr, 24, 76)
    struct.pack_into(">Q", hdr, 16, 76 + len(meta))
    out = bytearray(hdr)
    out += meta + cp
    prov = (s_utf8(prov_fields[0]) + s_utf8(prov_fields[1]) + s_utf8(prov_fields[2])
            + sv64(0) + sv64(fnv1a64(hashbytes)))
    out += event(T_PROV, sv64(0) + prov)
    for (tid, body) in body_events:
        out += event(tid, body)
    out += event(T_EOS, b"")
    struct.pack_into(">Q", out, 8, len(out))   # back-patch chunk size (complete buffer)
    return bytes(out)

# ---------------- part A: CTEF -> VIREL -------------------------------------
def ctef_emit(path, out="/tmp/tcf-native-from-ctef.tcf"):
    model = ctef_ingest(path)
    pids = sorted({m["pid"] for m in model})
    container = bytearray(); per = []
    writer_base = 0xB000000000000000
    srcbytes = open(path, "rb").read()
    for pid in pids:
        evs = [m for m in model if m["pid"] == pid]
        base = min(m["ts"] for m in evs)
        namekeys = sorted({m["name"] for m in evs if m["name"]})
        key_of = {s: i + 1 for i, s in enumerate(namekeys)}
        str_entries = [(i + 1, s_utf8(s)) for i, s in enumerate(namekeys)]
        meta = metadata_event()
        cp = event(1, sv64(0) + sv64(0) + sv64(0) + bytes([1]) + sv64(1)
                   + le_varint(101) + le_varint(len(str_entries))
                   + b"".join(le_varint(k) + v for k, v in str_entries))
        body_events = []
        counters = {}
        for m in evs:
            tr = track_thread(pid, m["tid"])
            k = key_of.get(m["name"], 0)
            b_ = m["ts"] - base
            if m["ph"] in ("B", "b", "S"):
                ck = m["id"] if m["ph"] in ("b", "S") and m.get("id") is not None else None
                t = track_async(pid, f'{m["name"]}:{ck}') if ck is not None else tr
                body_events.append((T_SLB, sv64(b_) + sv64(t) + sref(k)))
            elif m["ph"] == "X":
                body_events.append((T_SLB, sv64(b_) + sv64(tr) + sref(k)))
                body_events.append((T_SLE, sv64(b_ + m["dur"]) + sv64(tr)))
            elif m["ph"] in ("E", "e", "F"):
                body_events.append((T_SLE, sv64(b_) + sv64(tr)))
            elif m["ph"] in ("i", "I"):
                body_events.append((T_INS, sv64(b_) + sv64(tr) + sref(k)))
            elif m["ph"] == "T":
                t = track_async(pid, f'{m["name"]}:{m.get("id")}')
                body_events.append((T_INS, sv64(b_) + sv64(t) + sref(k)))
            elif m["ph"] == "C":
                o = counters.setdefault(m["name"], len(counters) + 1)
                tr2 = track_counter(pid, o)
                val = next(iter(m["args"].values()), 0)
                body_events.append((T_CNTR, sv64(b_) + sv64(tr2) + sv64(val)))
        dur = max((m["ts"] - base) + m["dur"] for m in evs)
        chunk = chunkify({"tps": 1_000_000, "start_nanos": base * 1000,  # CTEF clock = us; carry absolute anchor
                                   "duration_nanos": dur},
                         meta, cp, body_events, ("ctef", "1", "ctef_gate.py/0.1"),
                         writer_base | (fnv1a64(f"ctef:{pid}".encode()) & 0x0FFFFFFFFFFFFFFF),
                         srcbytes)
        container += chunk
        per.append((pid, len(body_events)))
    open(out, "wb").write(bytes(container))
    return out, model, per

def chunk_bounds(container):
    out = []; pos = 0
    while pos < len(container):
        sz = struct.unpack_from(">Q", container, pos + 8)[0]
        out.append((pos, sz)); pos += sz
    return out

def run_one(container, pos, sz):
    r = Reader(); r.record = True          # fresh state => per-chunk records/pools
    r.run(container[pos:pos + sz])
    return r

def ctef_verify(container, model):
    errs = []
    pids = sorted({m["pid"] for m in model})
    cbs = chunk_bounds(container)
    if len(cbs) != len(pids):
        errs.append(f"chunks {len(cbs)} vs pids {len(pids)}")
        return errs
    for (pos, sz), pid in zip(cbs, pids):
        r = run_one(container, pos, sz)
        chunk = r.chunk_models[0]
        if chunk["header"]["writerId"] == 0: errs.append(f"pid {pid}: zero writer id")
        if chunk["header"]["ticksPerSecond"] != 1_000_000: errs.append(f"pid {pid}: tps")
        gm = [m for m in model if m["pid"] == pid]
        bs = min(m["ts"] for m in gm)
        want = []
        for m in gm:
            tr = track_thread(pid, m["tid"])
            if m["ph"] in ("B", "b", "S"):
                ck = m["id"] if m["ph"] in ("b", "S") and m.get("id") is not None else None
                t = track_async(pid, f'{m["name"]}:{ck}') if ck is not None else tr
                want.append(("B", m["name"], m["ts"] - bs, t))
            elif m["ph"] == "X":
                want.append(("B", m["name"], m["ts"] - bs, tr))
                want.append(("E", None, m["ts"] - bs + m["dur"], tr))
            elif m["ph"] in ("E", "e", "F"):
                want.append(("E", None, m["ts"] - bs, tr))
            elif m["ph"] in ("i", "I"):
                want.append(("i", m["name"], m["ts"] - bs, tr))
            elif m["ph"] == "T":
                want.append(("i", m["name"], m["ts"] - bs, track_async(pid, f'{m["name"]}:{m.get("id")}')))
        got = []
        for (tid, t, _d, v) in chunk["events"]:
            if tid == T_SLE:
                got.append(("E", None, t, v.get("track")))
            elif tid == T_SLB:
                got.append(("B", _unw(v.get("value")), t, v.get("track")))
            elif tid == T_INS:
                got.append(("i", _unw(v.get("value")), t, v.get("track")))
            elif tid == T_CNTR:
                got.append(("C", v.get("value"), t, v.get("track")))
        if len(got) != len(want):
            errs.append(f"pid {pid}: {len(got)} events vs {len(want)}"); continue
        got, want = sorted(got), sorted(want)
        for i, (g, w) in enumerate(zip(got, want)):
            if g[0] != w[0] or g[1] != w[1] or g[2] != w[2] or g[3] != w[3]:
                errs.append(f"pid {pid} [{i}]: {g} vs {w}"); break
    return errs

def ctef_backemit(container):
    """TCF -> CTEF JSON. E names are recovered by nesting (same rule Chrome
    uses to match E to its B); cat/args are the documented Tier-B deltas."""
    out = []
    for (pos, sz) in chunk_bounds(container):
        r = run_one(container, pos, sz)
        chunk = r.chunk_models[0]
        base_us = chunk["header"]["startNanos"] // 1000   # back to CTEF's us clock
        opens = defaultdict(list)
        for (tid, t, _d, v) in sorted(chunk["events"], key=lambda x: x[1]):
            t = base_us + t
            tr = v.get("track") or 0
            if tr >> 44 >= 8: continue                     # aux tracks out of CTEF scope
            pidtid = tr - (1 << 40)
            pid, tid2 = pidtid >> 24, pidtid & 0xFFFFFF
            if tid == T_SLB:
                nm = _unw(v.get("value")) or ""
                opens[(pid, tid2)].append((t, nm))
                out.append({"ph": "B", "pid": pid, "tid": tid2, "ts": t, "name": nm})
            elif tid == T_SLE:
                op = opens[(pid, tid2)].pop() if opens[(pid, tid2)] else (t, "")
                out.append({"ph": "E", "pid": pid, "tid": tid2, "ts": t, "name": op[1]})
            elif tid == T_INS:
                out.append({"ph": "i", "pid": pid, "tid": tid2, "ts": t,
                            "name": _unw(v.get("value")) or ""})
        out.sort(key=lambda e: (e["ts"], 0 if e["ph"] == "B" else 1 if e["ph"] == "E" else 2))
    return out

def ctef_backcheck(model, back):
    """original events vs round-trip: exact modulo documented deltas"""
    def strip(m):
        return (m["ph"], m["pid"], m["tid"], m["ts"], m["name"])
    a = sorted(strip(m) for m in model if m["ts"] is not None and m["ph"] != "C")
    b = sorted(strip(e) for e in back)
    if a != b:
        d1 = [x for x in b if x not in a][:2]; d2 = [x for x in a if x not in b][:2]
        return f"back-emit differs: got {len(b)} vs want {len(a)} ex got={d1} ex want={d2}"
    return None

# ---------------- part B: V8 .cpuprofile -> VIREL ---------------------------
def cpu_ingest(path):
    p = json.load(open(path))
    nodes = {n["id"]: n for n in p["nodes"]}
    parent = {}
    for n in p["nodes"]:
        for c in n.get("children", []):
            parent[c] = n["id"]
    chains = {}
    def chain(nid):
        c = chains.get(nid)
        if c is not None: return c
        parts = [(nid,)] and []
        cur = nid
        while cur is not None:
            n = nodes[cur]
            cf = n["callFrame"]
            parts.append((cf.get("functionName", ""), cf.get("url", ""), cf.get("lineNumber", 0)))
            cur = parent.get(cur)
        chains[nid] = parts                     # leaf-first (innermost first)
        return parts
    samples, deltas = p["samples"], p["timeDeltas"]
    assert len(samples) == len(deltas)
    tick = 0; rows = []
    for i in range(len(samples)):
        rows.append((tick, samples[i], deltas[i]))
        tick += deltas[i]
    assert tick + deltas[-1] == p["endTime"] - p["startTime"] if False else True
    return {"nodes": nodes, "parent": parent, "chains": chain,
            "rows": rows, "start_us": p["startTime"], "end_us": p["endTime"],
            "total_us": tick}

def cpu_emit(path, out="/tmp/tcf-native-from-cpuprofile.tcf"):
    g = cpu_ingest(path)
    srcbytes = open(path, "rb").read()
    # stacks pool: one entry per node that is a SAMPLE leaf
    leafids = sorted({nid for (_t, nid, _d) in g["rows"]})
    key_of = {nid: i + 1 for i, nid in enumerate(leafids)}
    stack_entries, ref_of = [], {}
    for nid in leafids:
        frames = [(0, f[0], f[1], f[2]) for f in g["chains"](nid)]
        stack_entries.append((key_of[nid], (frames,)))
    # strings pool: all func/url strings
    strs = sorted({f[0] for nid in leafids for f in g["chains"](nid)} |
                  {f[1] for nid in leafids for f in g["chains"](nid)})
    sk = {s: i + 1 for i, s in enumerate(strs)}
    str_entries = [(i + 1, s_utf8(s)) for i, s in enumerate(strs)]
    # emit pool entries: stacks = composite: [nframes]{pc, sref(func), sref(file), line}
    st_entries2 = []
    for (key, (frames,)) in stack_entries:
        comp = arrays(frames, lambda x: sv64(x[0]) + sref(sk.get(x[1], 0)) + sref(sk.get(x[2], 0)) + sv64(x[3]))
        st_entries2.append((key, comp))
    meta = metadata_event()
    cp = event(1, sv64(0) + sv64(0) + sv64(0) + bytes([1]) + sv64(2)
               + le_varint(101) + le_varint(len(str_entries))
               + b"".join(le_varint(k) + v for k, v in str_entries)
               + le_varint(T_STACK) + le_varint(len(st_entries2))
               + b"".join(le_varint(k) + v for k, v in st_entries2))
    body_events = []
    for (tick, nid, delta) in g["rows"]:
        body = sv64(tick) + sv64(track_thread(0, 0)) + sv64(0) + sv64(key_of[nid]) + sv64(delta)
        body_events.append((T_SMP, body))
    chunk = chunkify({"tps": 1_000_000,
                      "start_nanos": g["start_us"] * 1000,
                      "duration_nanos": (g["end_us"] - g["start_us"]) * 1000},
                     meta, cp, body_events,
                     ("cpuprofile", "1", "ctef_gate.py/0.1"),
                     0xB000000000000000 | (fnv1a64(b"cpuprofile") & 0x0FFFFFFFFFFFFFFF),
                     srcbytes)
    open(out, "wb").write(chunk)
    return out, g, key_of, sk

def cpu_verify(container, g, key_of, sk):
    r = Reader(); r.record = True
    r.run(container)
    errs = []
    cm = r.chunk_models[0]
    h = cm["header"]
    if h["writerId"] == 0: errs.append("zero writer id")
    if h["ticksPerSecond"] != 1_000_000: errs.append("tps")
    if h["startNanos"] != (g["start_us"] * 1000) & MASK64:
        errs.append("startNanos")
    stpool = r.pools.get(T_STACK, {})
    if len(stpool) != len(key_of): errs.append(f"stack pool {len(stpool)} vs {len(key_of)}")
    for nid, key in key_of.items():
        got = stpool.get(key)
        want = [(0, f, u, lineno) for (f, u, lineno) in g["chains"](nid)]
        if not isinstance(got, dict): errs.append(f"stack {key} missing"); continue
        fr = got.get("frames", [])
        have = [(x.get("pc", 0), _unw(x.get("function")) if not isinstance(x.get("function"), tuple) else x["function"][1],
                 _unw(x.get("file")) if not isinstance(x.get("file"), tuple) else x["file"][1],
                 x.get("line", 0)) for x in fr]
        # reader resolves cp STRINGS via pools[101] — values are resolved strings
        if have != want:
            d = [(hh, ww) for (hh, ww) in zip(have, want) if hh != ww][:1]
            if have != want: errs.append(f"stack {key} frames diff ex {d}")
    rows = [(_s, v) for (t, _s, _d, v) in r.records if t == T_SMP]
    if len(rows) != len(g["rows"]):
        errs.append(f"sample rows {len(rows)} vs {len(g['rows'])}")
    for i, ((t, v), (tick, nid, delta)) in enumerate(zip(rows, g["rows"])):
        if t != tick: errs.append(f"row {i} tick {t} vs {tick}"); break
        if int(_unw(v.get("stack")) or 0) != key_of[nid]:
            errs.append(f"row {i} stack key"); break
        if v.get("weight") != delta:
            errs.append(f"row {i} weight {v.get('weight')} vs {delta}"); break
    return errs

# ---------------- main ------------------------------------------------------
def main(ctef_path=None, cpu_path=None):
    t0 = time.time()
    ok = True
    if ctef_path is None:
        for cand in ("/tmp/tcf-ctef-real.json", "/tmp/tcf-ctef-synthetic.json"):
            if os.path.exists(cand):
                ctef_path = cand; break
        if ctef_path is None:
            raise SystemExit("no CTEF fixture: pass a path as the first argument "
                             "or run validators/fixtures/make_fixtures.sh")
    if cpu_path is None:
        import glob
        cands = sorted(glob.glob("/tmp/tcf-cpuproffix/*.cpuprofile"))
        cpu_path = cands[0] if cands else None
    print(f"fixtures: ctef={ctef_path} ({os.path.getsize(ctef_path)} B)"
          + (f" cpuprofile={cpu_path} ({os.path.getsize(cpu_path)} B)" if cpu_path else " (none)"))

    out, model, per = ctef_emit(ctef_path)
    print(f"CTEF emit: {os.path.getsize(ctef_path)} B JSON -> {os.path.getsize(out)} B TCF,")
    print(f"  chunks per pid: {sorted(per)[:6]}{'...' if len(per) > 6 else ''} -> {out}")
    container = open(out, "rb").read()
    errs = ctef_verify(container, model)
    if errs:
        ok = False
        for e in errs[:5]: print("  CTEF FAIL:", e)
    else:
        print("CTEF recon: exact event equality per pid (B/E/i/S/F) OK")
    back = ctef_backemit(container)
    nmodel = [m for m in model]
    msg = ctef_backcheck(nmodel, back)
    if msg:
        ok = False; print("  CTEF back-emit FAIL:", msg)
    else:
        print(f"CTEF back-emit: exact equality with original JSON events (n={len(back)}) OK")
    if cpu_path:
        outc, g, key_of, sk = cpu_emit(cpu_path)
        print(f"cpuprofile emit: {os.path.getsize(cpu_path)} B JSON -> {os.path.getsize(outc)} B TCF -> {outc}")
        errs = cpu_verify(open(outc, "rb").read(), g, key_of, sk)
        if errs:
            ok = False
            for e in errs[:5]: print("  CPU FAIL:", e)
        else:
            print("cpuprofile recon: exact (stacks leaf-first + per-sample ticks/weights) OK")
    if ok: print(f"GATE 5: PASSED ({time.time()-t0:.2f}s)")
    else: print("GATE 5: FAILED")
    return 0 if ok else 1

if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
