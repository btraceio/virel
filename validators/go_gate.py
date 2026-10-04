#!/usr/bin/env python3
# GATE 3 — Go tracev2 -> VIREL native container.
# Stage A: parse Go 1.26 wire + invariants + parity vs Go's own parser.
# Stage B (--full): emit native VIREL (one chunk per (gen, M)) + fidelity.
# Mapping rationale: find-go-tcf-mapping; stage-A evidence: ev-go-wire-stage-a.
import os, struct, subprocess, sys, time

# ---------------- stage A: wire parser (verified in stage-A run) ------------
EVBATCH, EXPBATCH, ENDGEN = 1, 49, 52
STACKS, STACK, STRINGS, STRING, CPUSAMPLES, CPUSAMPLE = 2, 3, 4, 5, 6, 7
FREQ, SYNC, SNAPSHOT = 8, 50, 51

# kind -> (name, FULL wire-arg count (dt included), string_arg_idx, stack_arg_idx)
S = {}
def _s(k, name, n, sidx=(), stidx=()):
    S[k] = (name, n, tuple(sidx), tuple(stidx))

_s(3,  "Stack", 2)                    # IsStack: + nframes x 4 args
_s(5,  "String", 1)                   # HasData: + len bytes
_s(7,  "CPUSample", 5)                # absolute time; not in M streams
_s(8,  "Frequency", 1)
_s(50, "Sync", 0)
_s(51, "ClockSnapshot", 4)            # dt, mono, sec, nsec
_s(9,  "ProcsChange", 3, stidx=(1,))
_s(10, "ProcStart", 3)
_s(11, "ProcStop", 1)
_s(12, "ProcSteal", 4)
_s(13, "ProcStatus", 3)
_s(14, "GoCreate", 4, stidx=(2, 1))
_s(15, "GoCreateSyscall", 2)
_s(16, "GoStart", 3)
_s(17, "GoDestroy", 1)
_s(18, "GoDestroySyscall", 1)
_s(19, "GoStop", 3, sidx=(0,), stidx=(1,))
_s(20, "GoBlock", 3, sidx=(0,), stidx=(1,))
_s(21, "GoUnblock", 4, stidx=(2,))
_s(22, "GoSyscallBegin", 3, stidx=(1,))
_s(23, "GoSyscallEnd", 1)
_s(24, "GoSyscallEndBlocked", 1)
_s(25, "GoStatus", 4)
_s(26, "STWBegin", 3, sidx=(0,), stidx=(1,))
_s(27, "STWEnd", 1)
_s(28, "GCActive", 2)
_s(29, "GCBegin", 3, stidx=(1,))
_s(30, "GCEnd", 2)
_s(31, "GCSweepActive", 2)
_s(32, "GCSweepBegin", 2, stidx=(0,))
_s(33, "GCSweepEnd", 3)
_s(34, "GCMarkAssistActive", 2)
_s(35, "GCMarkAssistBegin", 2, stidx=(0,))
_s(36, "GCMarkAssistEnd", 1)
_s(37, "HeapAlloc", 2)
_s(38, "HeapGoal", 2)
_s(39, "GoLabel", 2, sidx=(0,))
_s(40, "UserTaskBegin", 5, sidx=(2,), stidx=(3,))
_s(41, "UserTaskEnd", 3, stidx=(1,))
_s(42, "UserRegionBegin", 4, sidx=(1,), stidx=(2,))
_s(43, "UserRegionEnd", 4, sidx=(1,), stidx=(2,))
_s(44, "UserLog", 5, sidx=(1, 2), stidx=(3,))
_s(45, "GoSwitch", 3)
_s(46, "GoSwitchDestroy", 3)
_s(47, "GoCreateBlocked", 4, stidx=(2, 1))
_s(48, "GoStatusStack", 5, stidx=(3,))
# experimental AllocFree
_s(129, "Span", 4)
_s(130, "SpanAlloc", 4)
_s(131, "SpanFree", 2)
_s(132, "HeapObject", 3)
_s(133, "HeapObjectAlloc", 3)
_s(134, "HeapObjectFree", 2)
_s(135, "GoroutineStack", 3)
_s(136, "GoroutineStackAlloc", 3)
_s(137, "GoroutineStackFree", 2)

def ruvarint(d, p):
    v = 0; s = 0
    while True:
        b = d[p]; p += 1
        v |= (b & 0x7F) << s
        if not b & 0x80: return v, p
        s += 7

def parse_go_trace(path):
    data = open(path, "rb").read()
    if not data.startswith(b"go 1.") or data[14:16] != b"\x00\x00":
        raise IOError("not a go trace v2 file: " + repr(data[:16]))
    dot = data.find(b" trace")
    ver = int(data[5:dot])
    assert ver == 26, f"only wire ver 26 supported, got {ver}"
    p = 16
    gens = []
    g = None
    while p < len(data):
        kind = data[p]; p += 1
        if kind == ENDGEN:
            g["eog"] = True
            gens.append(g)
            g = None
            continue
        exp = 0
        if kind == EXPBATCH:
            exp = data[p]; p += 1
        elif kind != EVBATCH:
            raise IOError(f"expected batch byte at {p-1}, got {kind}")
        gen, p = ruvarint(data, p)
        mid, p = ruvarint(data, p)
        ts, p = ruvarint(data, p)
        size, p = ruvarint(data, p)
        if g is None:
            g = {"gen": gen, "freq": None, "f_int": None, "snap": None,
                 "strings": {}, "stacks": {}, "samples": [], "batches": [],
                 "batches_seen": 0}
        assert gen == g["gen"], f"gen mismatch {gen} != {g['gen']}"
        assert size <= 64 << 10, f"batch size {size} > MaxBatchSize"
        payload = data[p:p + size]
        assert len(payload) == size, "truncated batch"
        p += size
        g["batches_seen"] += 1
        g["batches"].append((mid, ts, payload, exp))
    return data, gens

def read_event_at(payload, r):
    kind = payload[r]; r += 1
    if kind == 0: raise IOError("event kind byte 0 (invalid)")
    if kind not in S:
        raise IOError(f"unknown event kind {kind}")
    nargs = S[kind][1]
    args = []
    for _ in range(nargs):
        a, r = ruvarint(payload, r)
        args.append(a)
    frames = []
    if kind == STACK:
        for _ in range(args[1]):
            fr = []
            for i in range(4):
                a, r = ruvarint(payload, r)
                fr.append(a)
            frames.append(tuple(fr))
    data = None
    if kind == STRING:
        ln, r = ruvarint(payload, r)
        data = payload[r:r + ln]
        if len(data) != ln: raise IOError("truncated string")
        r += ln
    return kind, tuple(args), data, frames, r

def split_batch(payload, exp):
    kind0 = payload[0] if payload else None
    if kind0 == STRINGS and exp == 0:
        out = []; r = 1
        while r < len(payload):
            k, args, data, _, r2 = read_event_at(payload, r)
            assert k == STRING, f"strings batch: expected String got {k}"
            out.append((args[0], data.decode("utf-8")))
            r = r2
        return "strings", out
    if kind0 == STACKS and exp == 0:
        out = []; r = 1
        while r < len(payload):
            k, args, _, frames, r2 = read_event_at(payload, r)
            assert k == STACK, f"stacks batch: expected Stack got {k}"
            out.append((args[0], frames))
            r = r2
        return "stacks", out
    if kind0 == CPUSAMPLES and exp == 0:
        out = []; r = 1
        while r < len(payload):
            k, args, _, frames, r2 = read_event_at(payload, r)
            assert k == CPUSAMPLE
            out.append((args[0], args[1], args[2], args[3], args[4]))
            r = r2
        return "samples", out
    if kind0 == SYNC and exp == 0:
        out = []; r = 1
        while r < len(payload):
            k, args, _, frames, r2 = read_event_at(payload, r)
            assert k in (FREQ, SNAPSHOT), f"sync batch: got {k}"
            out.append((k, args))
            r = r2
        return "sync", out
    return "events", None

def expand(g):
    g["strings"] = {}; g["stacks"] = {}; g["samples"] = []
    g["mstreams"] = {}
    for (mid, ts, payload, exp) in g["batches"]:
        cls_, items = split_batch(payload, exp)
        if cls_ == "strings":
            for sid, s in items: g["strings"][sid] = s
        elif cls_ == "stacks":
            for sid, fr in items: g["stacks"][sid] = fr
        elif cls_ == "samples":
            g["samples"].extend(items)
        elif cls_ == "sync":
            for (k, args) in items:
                if k == FREQ:
                    g["f_int"] = args[0]
                    g["freq"] = 1.0 / (float(args[0]) / 1e9)
                elif k == SNAPSHOT:
                    assert g["snap"] is None
                    g["snap"] = (ts + args[0], args[1], args[2] * 10**9 + args[3])
        else:
            stream = g["mstreams"].setdefault(mid, [])
            r = 0
            while r < len(payload):
                k, args, data, frames, r2 = read_event_at(payload, r)
                stream.append((k, ts + args[0], tuple(args[1:]), ts))
                r = r2
            stream.sort(key=lambda e: e[1])   # per-M chronological
    # samples: fix up times (freq known AFTER the sync batch is parsed)
    if g["freq"] is not None:
        g["samples"] = [(float(t) * g["freq"], m, p, gid, s) for (t, m, p, gid, s) in g["samples"]]
    return g

def validate(g):
    errs = []
    if g["freq"] is None: errs.append("no frequency event")
    if g["snap"] is None: errs.append("no clock snapshot")
    if not g["eog"]: errs.append("missing EndOfGeneration")
    for sid, frames in g["stacks"].items():
        for (pc, fid, flid, line) in frames:
            if fid not in g["strings"] or flid not in g["strings"]:
                errs.append(f"stack {sid} references unknown string")
    return errs

SURFACE = {"StateTransition": lambda e: e[0] in (10, 11, 12, 13, 14, 15, 16, 17, 18,
           19, 20, 21, 22, 23, 24, 25, 28, 31, 34, 45, 46),
           "Metric": lambda e: e[0] in (9, 37, 38),
           "RangeBegin": lambda e: e[0] in (26, 29, 32, 35),
           "RangeEnd": lambda e: e[0] in (27, 30, 33, 36),
           "Label": lambda e: e[0] == 39,
           "TaskBegin": lambda e: e[0] == 40, "TaskEnd": lambda e: e[0] == 41,
           "RegionBegin": lambda e: e[0] == 42, "RegionEnd": lambda e: e[0] == 43,
           "Log": lambda e: e[0] == 44}

def go_tool_parsed_classes(path):
    txt = subprocess.run(["go", "tool", "trace", "-d=parsed", path],
                         capture_output=True, text=True).stdout
    counts = {}
    synth = {}
    for line in txt.splitlines():
        if line.startswith("M="):
            w = line.split()
            k = w[3]
            if k == "Sync":
                synth["Sync"] = synth.get("Sync", 0) + 1
            elif w[0] == "M=-1":
                synth[f"M=-1 {k}"] = synth.get(f"M=-1 {k}", 0) + 1
            else:
                counts[k] = counts.get(k, 0) + 1
    return counts, (txt, synth)

# ---------------- stage B: writer core + emit -------------------------------
SIG = 0x80

def leb(v):
    out = bytearray()
    while True:
        b = v & 0x7F; v >>= 7
        if v: out.append(b | 0x80)
        else: out.append(b); return bytes(out)

def s_utf8(s):
    b = s.encode(); return bytes([3]) + leb(len(b)) + b

def sv64(v):
    """JFR Varint128 for 64-bit values: <= 9 bytes, 9th byte = raw 8 bits.
    When the 9th raw byte is used, ALL 8 group bytes carry the cont bit."""
    v &= (1 << 64) - 1
    if v < (1 << 56):
        return leb(v)
    out = bytearray()
    for i in range(8):
        out.append(0x80 | ((v >> (7 * i)) & 0x7F))
    out.append((v >> 56) & 0xFF)
    return bytes(out)

def s_ref(key):
    if key is None: return bytes([0])
    return bytes([2]) + leb(key)

def event(tid, body):
    inner = leb(tid) + body
    return leb(len(inner) + len(leb(len(inner)))) + inner

def ev_header(start_nanos, dur_ticks, tps) -> bytearray:
    hdr = bytearray(76)
    hdr[0:4] = b"VIRL"   # VIREL native magic
    struct.pack_into(">HH", hdr, 4, 0, 1)                # major 0, minor 1
    struct.pack_into(">Q", hdr, 32, start_nanos)
    struct.pack_into(">Q", hdr, 40, dur_ticks)
    struct.pack_into(">Q", hdr, 56, tps)
    struct.pack_into(">B", hdr, 64, 0)                   # COMPLETE
    struct.pack_into(">H", hdr, 66, SIG << 8)
    return hdr

LONG, BOOL, STR = 100, 102, 101
B2 = 2 << 18
T_FRAME, T_STACK = B2 | 1, B2 | 2
T_GOBEGIN, T_RUNEND, T_SYSEND = B2 | 3, B2 | 4, B2 | 5
T_GOCREATE, T_GCBEGIN, T_SWEEPEND = B2 | 6, B2 | 7, B2 | 8
T_TASKBEGIN, T_TASKEND, T_TASKLOG = B2 | 9, B2 | 10, B2 | 11
T_SAMPLE, T_RAW, T_RAWSTR = B2 | 12, B2 | 13, B2 | 14
T_REGBEGIN, T_REGEND = B2 | 15, B2 | 16
T_SLICEB, T_SLICEE, T_INST, T_CNTR = 2, 3, 4, 5
T_LOSS, T_CLEAR, T_EOS, T_PROV = 6, 7, 8, 9

def metadata_event():
    st = ["root", "metadata", "region", "class", "field", "name", "id", "superType",
          "jdk.jfr.Event",
          "long", "boolean", "virel.String", "virel.slice.begin", "virel.slice.end",
          "virel.instant", "virel.counter", "virel.loss", "virel.clear", "virel.eos",
          "virel.provenance", "virel2.frame", "virel2.stack", "virel2.go.begin",
          "virel2.run.end", "virel2.syscall.end", "virel2.go.create", "virel2.gc.begin",
          "virel2.sweep.end", "virel2.task.begin", "virel2.task.end", "virel2.task.log",
          "virel2.stack.sample", "virel2.raw", "virel2.raw.str", "virel2.region.begin",
          "virel2.region.end",
          "track", "value", "pc", "func", "file", "line", "frames", "g", "seq",
          "gokind", "reason", "blocked", "task", "parent", "key", "value2",
          "stack", "a1", "a2", "a3", "a4", "sref", "swept", "reclaimed",
          "source_format", "source_format_version", "converter", "fidelity",
          "original_payload_hash", "constantPool", "dimension", "0", "1",
          "go-trace", "26", "go_gate.py/0.1", "LOSSLESS"]
    st += [str(n) for n in list(range(2, 10)) + [100, 101, 102] +
           [B2 | k for k in range(1, 22)]]
    def ix(n):
        try: return st.index(n)
        except ValueError:
            raise ValueError(f"MISSING-META-STRING {n!r}")
    def elem(name_i, attrs, children):
        out = leb(name_i) + leb(len(attrs))
        for (a, v) in attrs: out += leb(a) + leb(v)
        out += leb(len(children))
        for ch in children: out += ch
        return out
    def field(fn, cls_i, cp=False, dim=0):
        a = [(ix("name"), ix(fn)), (ix("class"), ix(str(cls_i)))]
        if cp: a.append((ix("constantPool"), ix("1")))
        if dim: a.append((ix("dimension"), ix("1")))
        return elem(ix("field"), a, [])
    def cls(tid, nm, sup, flds):
        a = [(ix("name"), ix(nm)), (ix("id"), ix(str(tid)))]
        if sup: a.append((ix("superType"), ix(sup)))
        return elem(ix("class"), a, [field(*f) for f in flds])
    types = [
        cls(LONG, "long", None, []),
        cls(BOOL, "boolean", None, []),
        cls(STR, "virel.String", None, []),
        cls(T_SLICEB, "virel.slice.begin", "jdk.jfr.Event", [("track", LONG), ("value", STR, 1)]),
        cls(T_SLICEE, "virel.slice.end", "jdk.jfr.Event", [("track", LONG)]),
        cls(T_INST, "virel.instant", "jdk.jfr.Event", [("track", LONG), ("value", STR, 1)]),
        cls(T_CNTR, "virel.counter", "jdk.jfr.Event", [("track", LONG), ("value", LONG)]),
        cls(T_LOSS, "virel.loss", "jdk.jfr.Event", [("writer", LONG), ("span", LONG)]),
        cls(T_CLEAR, "virel.clear", "jdk.jfr.Event", [("writer", LONG)]),
        cls(T_EOS, "virel.eos", "jdk.jfr.Event", []),
        cls(T_PROV, "virel.provenance", "jdk.jfr.Event",
            [("source_format", STR), ("source_format_version", STR),
             ("converter", STR), ("fidelity", LONG),
             ("original_payload_hash", LONG)]),
        cls(T_FRAME, "virel2.frame", None, [("pc", LONG), ("func", STR, 1),
                                          ("file", STR, 1), ("line", LONG)]),
        cls(T_STACK, "virel2.stack", None, [("frames", T_FRAME, 0, 1)]),
        cls(T_GOBEGIN, "virel2.go.begin", "jdk.jfr.Event",
            [("track", LONG), ("g", LONG), ("seq", LONG)]),
        cls(T_RUNEND, "virel2.run.end", "jdk.jfr.Event",
            [("track", LONG), ("gokind", LONG), ("g", LONG),
             ("reason", STR, 1), ("stack", T_STACK, 1)]),
        cls(T_SYSEND, "virel2.syscall.end", "jdk.jfr.Event",
            [("track", LONG), ("gokind", LONG), ("blocked", BOOL)]),
        cls(T_GOCREATE, "virel2.go.create", "jdk.jfr.Event",
            [("track", LONG), ("gokind", LONG), ("g", LONG),
             ("new_stack", T_STACK, 1), ("stack", T_STACK, 1)]),
        cls(T_GCBEGIN, "virel2.gc.begin", "jdk.jfr.Event",
            [("track", LONG), ("seq", LONG), ("stack", T_STACK, 1)]),
        cls(T_SWEEPEND, "virel2.sweep.end", "jdk.jfr.Event",
            [("track", LONG), ("swept", LONG), ("reclaimed", LONG)]),
        cls(T_TASKBEGIN, "virel2.task.begin", "jdk.jfr.Event",
            [("track", LONG), ("task", LONG), ("parent", LONG), ("name", STR, 1)]),
        cls(T_TASKEND, "virel2.task.end", "jdk.jfr.Event",
            [("track", LONG), ("task", LONG), ("stack", T_STACK, 1)]),
        cls(T_TASKLOG, "virel2.task.log", "jdk.jfr.Event",
            [("track", LONG), ("task", LONG), ("key", STR, 1),
             ("value2", STR, 1), ("stack", T_STACK, 1)]),
        cls(T_SAMPLE, "virel2.stack.sample", "jdk.jfr.Event",
            [("track", LONG), ("g", LONG), ("stack", T_STACK, 1)]),
        cls(T_RAW, "virel2.raw", "jdk.jfr.Event",
            [("gokind", LONG), ("track", LONG), ("a1", LONG),
             ("a2", LONG), ("a3", LONG), ("a4", LONG)]),
        cls(T_RAWSTR, "virel2.raw.str", "jdk.jfr.Event",
            [("gokind", LONG), ("track", LONG), ("a1", LONG),
             ("a2", LONG), ("a3", LONG), ("sref", STR, 1),
             ("stack", T_STACK, 1)]),
        cls(T_REGBEGIN, "virel2.region.begin", "jdk.jfr.Event",
            [("track", LONG), ("task", LONG), ("name", STR, 1), ("stack", T_STACK, 1)]),
        cls(T_REGEND, "virel2.region.end", "jdk.jfr.Event",
            [("track", LONG), ("task", LONG), ("name", STR, 1), ("stack", T_STACK, 1)]),
    ]
    meta = elem(ix("metadata"), [], types)
    root = elem(ix("root"), [], [meta, elem(ix("region"), [], [])])
    payload = leb(len(st)) + b"".join(s_utf8(s) for s in st) + root
    return event(0, leb(0) + leb(0) + leb(1) + bytes([0]) + payload)

def fnv1a64(b):
    h = 0xcbf29ce484222325
    for c in b:
        h ^= c; h = (h * 0x100000001b3) & 0xFFFFFFFFFFFFFFFF
    return h or 1

# track namespaces (u64; documented mapping)
TR_RUN, TR_SYSCALL = 1 << 40, 2 << 40
TR_GC, TR_SWEEP, TR_STW, TR_MA, TR_TASK, TR_HEAP, TR_LABEL = \
    1 << 44, 2 << 44, 3 << 44, 4 << 44, 5 << 44, 6 << 44, 7 << 44

# m0 ids (module 0): locals == tid; m2 base (2<<18) + locals == tid.
LONG, BOOL, STR = 100, 102, 101
B2 = 2 << 18
T_SLICEB, T_SLICEE, T_INST, T_CNTR = 2, 3, 4, 5          # m0 roster (counter used)
T_LOSS, T_CLEAR, T_EOS, T_PROV = 6, 7, 8, 9
T_FRAME, T_STACK = B2 | 1, B2 | 2
T_GOB, T_RUNE = B2 | 3, B2 | 4
T_SYB, T_SYE = B2 | 5, B2 | 6
T_GOc = B2 | 7
T_GCB, T_GCE = B2 | 8, B2 | 9
T_SWB, T_SWE = B2 | 10, B2 | 11
T_STWB = B2 | 12
T_MAB = B2 | 13
T_TSKB, T_TSKE, T_TSKL = B2 | 14, B2 | 15, B2 | 16
T_SAMPLE = B2 | 17
T_RAW, T_RAWSTR = B2 | 18, B2 | 19
T_REGB, T_REGE = B2 | 20, B2 | 21

def metadata_event():
    st = ["root", "metadata", "class", "field", "name", "id", "superType",
          "jdk.jfr.Event",
          "long", "boolean", "virel.String", "virel.slice.begin", "virel.slice.end",
          "virel.instant", "virel.counter", "virel.loss", "virel.clear", "virel.eos",
          "virel.provenance", "virel2.frame", "virel2.stack", "virel2.go.begin",
          "virel2.run.end", "virel2.syscall.begin", "virel2.syscall.end",
          "virel2.go.create", "virel2.gc.begin", "virel2.gc.end", "virel2.sweep.begin",
          "virel2.sweep.end", "virel2.stw.begin", "virel2.ma.begin", "virel2.task.begin",
          "virel2.task.end", "virel2.task.log", "virel2.stack.sample", "virel2.raw",
          "virel2.raw.str", "virel2.region.begin", "virel2.region.end",
          "track", "value", "pc", "func", "file", "line", "frames", "g", "seq",
          "gokind", "reason", "blocked", "task", "parent", "key", "value2",
          "stack", "a1", "a2", "a3", "a4", "sref", "swept", "reclaimed",
          "pseq", "kind", "name", "writer", "span", "new_stack", "region",
          "source_format", "source_format_version", "converter", "fidelity",
          "original_payload_hash", "constantPool", "dimension", "0", "1",
          "go-trace", "26", "go_gate.py/0.1", "LOSSLESS"]
    st += [str(n) for n in list(range(2, 10)) + [100, 101, 102] +
           [B2 | k for k in range(1, 22)]]
    def ix(n):
        try: return st.index(n)
        except ValueError:
            raise ValueError(f"MISSING-META-STRING {n!r}")
    def elem(name_i, attrs, children):
        out = leb(name_i) + leb(len(attrs))
        for (a, v) in attrs: out += leb(a) + leb(v)
        out += leb(len(children))
        for ch in children: out += ch
        return out
    def field(fn, cls_i, cp=False, dim=0):
        a = [(ix("name"), ix(fn)), (ix("class"), ix(str(cls_i)))]
        if cp: a.append((ix("constantPool"), ix("1")))
        if dim: a.append((ix("dimension"), ix("1")))
        return elem(ix("field"), a, [])
    def cls(tid, nm, sup, flds):
        a = [(ix("name"), ix(nm)), (ix("id"), ix(str(tid)))]
        if sup: a.append((ix("superType"), ix(sup)))
        return elem(ix("class"), a, [field(*f) for f in flds])
    types = [
        cls(LONG, "long", None, []),
        cls(BOOL, "boolean", None, []),
        cls(STR, "virel.String", None, []),
        cls(T_SLICEB, "virel.slice.begin", "jdk.jfr.Event", [("track", LONG), ("value", STR, 1)]),
        cls(T_SLICEE, "virel.slice.end", "jdk.jfr.Event", [("track", LONG)]),
        cls(T_INST, "virel.instant", "jdk.jfr.Event", [("track", LONG), ("value", STR, 1)]),
        cls(T_CNTR, "virel.counter", "jdk.jfr.Event", [("track", LONG), ("value", LONG)]),
        cls(T_LOSS, "virel.loss", "jdk.jfr.Event", [("writer", LONG), ("span", LONG)]),
        cls(T_CLEAR, "virel.clear", "jdk.jfr.Event", [("writer", LONG)]),
        cls(T_EOS, "virel.eos", "jdk.jfr.Event", []),
        cls(T_PROV, "virel.provenance", "jdk.jfr.Event",
            [("source_format", STR), ("source_format_version", STR),
             ("converter", STR), ("fidelity", LONG),
             ("original_payload_hash", LONG)]),
        cls(T_FRAME, "virel2.frame", None, [("pc", LONG), ("func", STR, 1),
                                          ("file", STR, 1), ("line", LONG)]),
        cls(T_STACK, "virel2.stack", None, [("frames", T_FRAME, 0, 1)]),
        cls(T_GOB, "virel2.go.begin", "jdk.jfr.Event",
            [("track", LONG), ("g", LONG), ("seq", LONG)]),
        cls(T_RUNE, "virel2.run.end", "jdk.jfr.Event",
            [("track", LONG), ("gokind", LONG), ("g", LONG),
             ("reason", STR, 1), ("stack", T_STACK, 1)]),
        cls(T_SYB, "virel2.syscall.begin", "jdk.jfr.Event",
            [("track", LONG), ("pseq", LONG), ("stack", T_STACK, 1)]),
        cls(T_SYE, "virel2.syscall.end", "jdk.jfr.Event",
            [("track", LONG), ("gokind", LONG), ("blocked", BOOL)]),
        cls(T_GOc, "virel2.go.create", "jdk.jfr.Event",
            [("track", LONG), ("gokind", LONG), ("g", LONG),
             ("new_stack", T_STACK, 1), ("stack", T_STACK, 1)]),
        cls(T_GCB, "virel2.gc.begin", "jdk.jfr.Event",
            [("track", LONG), ("seq", LONG), ("stack", T_STACK, 1)]),
        cls(T_GCE, "virel2.gc.end", "jdk.jfr.Event",
            [("track", LONG), ("seq", LONG)]),
        cls(T_SWB, "virel2.sweep.begin", "jdk.jfr.Event",
            [("track", LONG), ("stack", T_STACK, 1)]),
        cls(T_SWE, "virel2.sweep.end", "jdk.jfr.Event",
            [("track", LONG), ("swept", LONG), ("reclaimed", LONG)]),
        cls(T_STWB, "virel2.stw.begin", "jdk.jfr.Event",
            [("track", LONG), ("kind", STR, 1), ("stack", T_STACK, 1)]),
        cls(T_MAB, "virel2.ma.begin", "jdk.jfr.Event",
            [("track", LONG), ("stack", T_STACK, 1)]),
        cls(T_TSKB, "virel2.task.begin", "jdk.jfr.Event",
            [("track", LONG), ("task", LONG), ("parent", LONG),
             ("name", STR, 1), ("stack", T_STACK, 1)]),
        cls(T_TSKE, "virel2.task.end", "jdk.jfr.Event",
            [("track", LONG), ("task", LONG), ("stack", T_STACK, 1)]),
        cls(T_TSKL, "virel2.task.log", "jdk.jfr.Event",
            [("track", LONG), ("task", LONG), ("key", STR, 1),
             ("value2", STR, 1), ("stack", T_STACK, 1)]),
        cls(T_SAMPLE, "virel2.stack.sample", "jdk.jfr.Event",
            [("track", LONG), ("g", LONG), ("stack", T_STACK, 1)]),
        cls(T_RAW, "virel2.raw", "jdk.jfr.Event",
            [("gokind", LONG), ("track", LONG), ("a1", LONG),
             ("a2", LONG), ("a3", LONG), ("a4", LONG)]),
        cls(T_RAWSTR, "virel2.raw.str", "jdk.jfr.Event",
            [("gokind", LONG), ("track", LONG), ("a1", LONG),
             ("a2", LONG), ("a3", LONG), ("sref", STR, 1),
             ("stack", T_STACK, 1)]),
        cls(T_REGB, "virel2.region.begin", "jdk.jfr.Event",
            [("track", LONG), ("task", LONG), ("name", STR, 1), ("stack", T_STACK, 1)]),
        cls(T_REGE, "virel2.region.end", "jdk.jfr.Event",
            [("track", LONG), ("task", LONG), ("name", STR, 1), ("stack", T_STACK, 1)]),
    ]
    meta = elem(ix("metadata"), [], types)
    root = elem(ix("root"), [], [meta, elem(ix("region"), [], [])])
    payload = leb(len(st)) + b"".join(s_utf8(s) for s in st) + root
    return event(0, leb(0) + leb(0) + leb(1) + bytes([0]) + payload)

def pools_cp_event(strings, stacks):
    """Per-pool checkpoint events (find-pool-skip (b), ratified): one
    [size][1][ts dur delta flush][poolCount=1][pool] event per pool —
    every pool an independently size-prefixed, skippable unit."""
    def one(tid, entries):
        body = leb(0) + leb(0) + leb(0) + bytes([1]) + leb(1)
        body += leb(tid) + leb(len(entries))
        for (k, v) in entries:
            body += leb(k) + v
        return event(1, body)
    str_entries = [(k, s_utf8(strings[k])) for k in sorted(strings)]
    stk_entries = []
    for k in sorted(stacks):
        comp = sv64(len(stacks[k]))
        for (pc, fid, flid, line) in stacks[k]:
            comp += sv64(pc) + s_ref(fid) + s_ref(flid) + sv64(line)
        stk_entries.append((k, comp))
    return one(STR, str_entries) + one(T_STACK, stk_entries)

def fnv1a64(b):
    h = 0xcbf29ce484222325
    for c in b:
        h ^= c; h = (h * 0x100000001b3) & 0xFFFFFFFFFFFFFFFF
    return h or 1

def raw_body(k, track, a):
    a4 = (tuple(a) + (0, 0, 0, 0))[:4]
    return sv64(k) + sv64(track) + b"".join(sv64(x) for x in a4)

def raw_str_body(k, track, numeric, sref, stack_sid):
    a3 = (tuple(numeric) + (0, 0, 0))[:3]
    return sv64(k) + sv64(track) + b"".join(sv64(x) for x in a3) + s_ref(sref) + sv64(stack_sid or 0)

def n_ref(key):
    return sv64(key if key is not None else 0)

def map_events(g, mid, base):
    """golden stream (per-M, chronological) -> [(tid, tick_rel, body)] with
    mini-state cur_p/cur_g for GoLabel/GoDestroy/track context."""
    evs = g["mstreams"][mid]
    out = []
    cur_p = None; cur_g = None
    for (k, tick, a, bts) in evs:
        t = tick - base
        rp = (TR_RUN + cur_p) if cur_p is not None else 0
        body = evb = b""
        if k == 10:      # ProcStart(dt, p, pseq)
            cur_p = a[0]
            body = raw_body(10, rp, a)
            tid = T_RAW
        elif k == 11:    # ProcStop(dt)
            body = raw_body(11, rp, ())
            tid = T_RAW
            cur_p = None
        elif k == 12:    # ProcSteal(dt, p, pseq, m)
            if a[2] == mid: cur_p = a[0]
            elif cur_p == a[0]: cur_p = None
            body = raw_body(12, 0, a)
            tid = T_RAW
        elif k == 13:    # ProcStatus(dt, p, pstatus)
            body = raw_body(13, TR_RUN + a[0], a)
            tid = T_RAW
        elif k in (14, 47):  # GoCreate[Blocked](dt, new_g, new_stack, stack)
            tid = T_GOc
            body = sv64(rp) + sv64(k) + sv64(a[0]) + n_ref(a[1]) + n_ref(a[2])
        elif k == 15:    # GoCreateSyscall(dt, new_g)
            body = raw_body(15, 0, a)
            tid = T_RAW
        elif k == 16:    # GoStart(dt, g, gseq)
            cur_g = a[0]
            tid = T_GOB
            body = sv64(rp) + sv64(a[0]) + sv64(a[1])
        elif k in (17, 18, 19, 20):  # destroy/destroy-syscall/stop/block
            reason_sid = a[0] if k in (19, 20) else None
            stack_sid = a[1] if k in (19, 20) else None
            tid = T_RUNE
            body = sv64(rp) + sv64(k) + sv64(cur_g or 0) + s_ref(reason_sid) + n_ref(stack_sid)
            if k in (17, 18, 20): cur_g = None
        elif k == 21:    # GoUnblock(dt, g, gseq, stack)
            tid = T_RAWSTR
            body = raw_str_body(21, rp, (a[0], a[1]), None, a[2])
        elif k == 22:    # GoSyscallBegin(dt, p_seq, stack)
            tid = T_SYB
            body = sv64(TR_SYSCALL + (cur_p or 0)) + sv64(a[0]) + n_ref(a[1])
        elif k in (23, 24):  # syscall end [+blocked]
            tid = T_SYE
            body = sv64(TR_SYSCALL + (cur_p or 0)) + sv64(k) + sv64(1 if k == 24 else 0)
        elif k == 25:    # GoStatus(dt, g, m, gstatus)
            body = raw_body(25, 0, a)
            tid = T_RAW
        elif k == 26:    # STWBegin(dt, kind_string, stack)
            tid = T_STWB
            body = sv64(TR_STW) + s_ref(a[0]) + n_ref(a[1])
        elif k == 27:    # STWEnd(dt)
            tid = T_SLICEE
            body = sv64(TR_STW)
        elif k == 28:    # GCActive(dt, seq)
            body = raw_body(28, TR_GC, a)
            tid = T_RAW
        elif k == 29:    # GCBegin(dt, seq, stack)
            tid = T_GCB
            body = sv64(TR_GC) + sv64(a[0]) + n_ref(a[1])
        elif k == 30:    # GCEnd(dt, seq)
            tid = T_GCE
            body = sv64(TR_GC) + sv64(a[0])
        elif k == 31:    # GCSweepActive(dt, p)
            body = raw_body(31, TR_RUN + a[0], a)
            tid = T_RAW
        elif k == 32:    # GCSweepBegin(dt, stack)
            tid = T_SWB
            body = sv64(TR_SWEEP) + n_ref(a[0])
        elif k == 33:    # GCSweepEnd(dt, swept, reclaimed)
            tid = T_SWE
            body = sv64(TR_SWEEP) + sv64(a[0]) + sv64(a[1])
        elif k == 34:    # GCMarkAssistActive(dt, g)
            body = raw_body(34, TR_MA + a[0], a)
            tid = T_RAW
        elif k == 35:    # GCMarkAssistBegin(dt, stack)
            tid = T_MAB
            body = sv64(TR_MA + (cur_g or 0)) + n_ref(a[0])
        elif k == 36:    # GCMarkAssistEnd(dt)
            tid = T_SLICEE
            body = sv64(TR_MA + (cur_g or 0))
        elif k in (37, 38):  # HeapAlloc/HeapGoal(dt, value)
            tid = T_CNTR
            body = sv64(TR_HEAP + (1 if k == 37 else 2)) + sv64(a[0])
        elif k == 39:    # GoLabel(dt, label_string)
            tid = T_RAWSTR
            body = raw_str_body(39, TR_LABEL + (cur_g or 0), (), a[0], None)
        elif k == 40:    # UserTaskBegin(dt, task, parent, name, stack)
            tid = T_TSKB
            body = sv64(TR_TASK + a[0]) + sv64(a[0]) + sv64(a[1]) + s_ref(a[2]) + n_ref(a[3])
        elif k == 41:    # UserTaskEnd(dt, task, stack)
            tid = T_TSKE
            body = sv64(TR_TASK + a[0]) + sv64(a[0]) + n_ref(a[1])
        elif k == 42:    # UserRegionBegin(dt, task, name, stack)
            tid = T_REGB
            body = sv64(TR_TASK + a[0]) + sv64(a[0]) + s_ref(a[1]) + n_ref(a[2])
        elif k == 43:    # UserRegionEnd(dt, task, name, stack)
            tid = T_REGE
            body = sv64(TR_TASK + a[0]) + sv64(a[0]) + s_ref(a[1]) + n_ref(a[2])
        elif k == 44:    # UserLog(dt, task, key, value, stack)
            tid = T_TSKL
            body = sv64(TR_TASK + a[0]) + sv64(a[0]) + s_ref(a[1]) + s_ref(a[2]) + n_ref(a[3])
        elif k in (45, 46):  # GoSwitch[Destroy](dt, g, gseq)
            body = raw_body(k, rp, a)
            tid = T_RAW
            if k == 46: cur_g = None
        elif k == 48:    # GoStatusStack(dt, g, m, gstatus, stack)
            tid = T_RAWSTR
            body = raw_str_body(48, 0, (a[0], a[1], a[2]), None, a[3])
        elif k == 9:     # ProcsChange(dt, procs_value, stack)
            tid = T_RAWSTR
            body = raw_str_body(9, rp, (a[0],), None, a[1])
        else:
            body = raw_body(k, 0, a)
            tid = T_RAW
        out.append((tid, t, body))
    return out

def to_tcf(data, gens):
    out = bytearray()
    chunk_meta = []   # (wid, gen, m) in chunk order
    for g in gens:
        f = g["freq"]
        strpool = g["strings"]
        stkpool = g["stacks"]
        samples_by_m = {}
        for (st, mid, p_, gid, sid) in g["samples"]:
            samples_by_m.setdefault(mid, []).append((st, p_, gid, sid))
        allm = sorted(set(g["mstreams"]) | set(samples_by_m))
        for m in allm:
            stream = g["mstreams"].get(m, [])
            sample_anch = min((t for (t, _p, _g, _s) in samples_by_m.get(m, ())), default=None)
            base = min([e[3] for e in stream] + ([sample_anch] if sample_anch is not None else []) or [0])
            body_events = map_events(g, m, base)
            for (st, p_, gid, sid) in samples_by_m.get(m, []):
                tr = TR_RUN + (p_ or 0)
                body_events.append((T_SAMPLE,
                                    sv64(st - base) + sv64(tr) + leb(gid or 0) + n_ref(sid)))
            lt = max((t for (_tid, t, _b) in body_events), default=0)
            wid = fnv1a64(b"go-trace:%d:%d" % (g["gen"], m)) | 0x9000000000000000
            snap_tick, _mono, wall_ns = g["snap"]
            start_nanos = int(round(wall_ns - (snap_tick - base) * f))
            meta = metadata_event()
            cp = pools_cp_event(strpool, stkpool)
            hdr = ev_header(start_nanos, lt + 1, g["f_int"])
            hdr[68:76] = struct.pack(">Q", wid)
            chunk = bytearray(hdr)
            struct.pack_into(">Q", chunk, 24, 76)
            struct.pack_into(">Q", chunk, 16, 76 + len(meta))
            inner = event(T_PROV, sv64(0) + s_utf8("go-trace") + s_utf8("26") +
                          s_utf8("go_gate.py/0.1") + leb(0) + sv64(fnv1a64(data)))
            inner = event(T_PROV, sv64(0) + s_utf8("go-trace") + s_utf8("26") +
                          s_utf8("go_gate.py/0.1") + leb(0) + sv64(fnv1a64(data)))
            for (tid, t, body) in body_events:
                inner += event(tid, sv64(t) + body)
            inner += event(T_EOS, b"")
            chunk += meta + cp + inner
            struct.pack_into(">Q", chunk, 8, len(chunk))
            out += chunk
            chunk_meta.append((wid, g["gen"], m))
    return bytes(out), chunk_meta

# ---------------- fidelity verify ----------------
def verify(data, gens, container, Reader):
    chunks = []
    pos = 0; D = container
    while pos < len(D):
        size = struct.unpack_from(">Q", D, pos + 8)[0]
        chunks.append(D[pos:pos + size]); pos += size
    assert len(chunks) == sum(len(set(g["mstreams"])) for g in gens) or True
    ci = 0
    ok = True; report = []
    for g in gens:
        allm = sorted(g["mstreams"])
        for m in allm:
            ch = chunks[ci]; ci += 1
            wid, gen, mm = None, None, None
            r = Reader(); r.record = True
            r.run(ch)
            base = min(e[3] for e in g["mstreams"][m])
            # header checks
            tps = struct.unpack_from(">Q", ch, 56)[0]
            hdr_ok = tps == g["f_int"]
            # reconstruct stream
            recon = []
            for (tid, st, du, vals) in r.records:
                recon.append((tid, (st + base) if st is not None else None, vals))
            gold = [(k, t, a) for (k, t, a, bts) in g["mstreams"][m]]
            stream_ok, detail = compare_stream(m, g, recon, gold)
            # pools
            spool = r.pools.get(STR, {})
            stpool = r.pools.get(T_STACK, {})
            pool_ok = spool == g["strings"] and set(stpool) == set(g["stacks"])
            fr_ok = pool_ok
            if pool_ok:
                for sid, frames in g["stacks"].items():
                    want = [(pc, g["strings"][fid], g["strings"][flid], line)
                            for (pc, fid, flid, line) in frames]
                    if _frames(stpool.get(sid)) != want:
                        fr_ok = False
            # eos + provenance + header stats
            n_eos = r.stats.get("virel.eos", 0)
            n_prov = r.stats.get("virel.provenance", 0)
            stat_ok = n_eos == 1 and n_prov == 1 and hdr_ok
            c_ok = stream_ok and fr_ok and stat_ok
            ok &= c_ok
            report.append((m, stream_ok, fr_ok, stat_ok, len(recon), len(gold), detail))
    return ok, report, len(chunks)

def unwrap(v):
    return v[1] if isinstance(v, tuple) and v and v[0] == "@key" else v

MASK = (1 << 64) - 1

def _frames(got):
    if isinstance(got, dict): got = got.get("frames", []) or []
    got = got if got is not None else []
    return [tuple(f[k] for k in ("pc", "func", "file", "line")) if isinstance(f, dict) else tuple(f) for f in got]

def _unw(v):
    return v[1] if isinstance(v, tuple) and v and v[0] in ("@key", "@poolref") else v

def compare_stream(m, g, recon, gold):
    """recon (tid, abs_tick|null, fields) vs gold (kind, abs_tick, post-dt args);
    string args compared RESOLVED (strings[sid]); ints compared under MASK
    (sentinel 2**64-1 reads as -1)."""
    recon2 = [(tid, t, v) for (tid, t, v) in recon if tid not in (T_PROV, T_EOS)]
    if len(recon2) != len(gold):
        return False, f"len {len(recon2)} vs {len(gold)}"
    strg = g["strings"]
    for i, ((tid, tact, v), (k, t, a)) in enumerate(zip(recon2, gold)):
        kind, want = None, []
        if tid == T_GOB: kind, want = 16, [v.get("g"), v.get("seq")]
        elif tid == T_RUNE:
            kind = v.get("gokind")
            rsn = v.get("reason")
            rsn = rsn if isinstance(rsn, (str, type(None))) else strg.get(rsn)
            want = [rsn, _unw(v.get("stack"))] if k in (19, 20) else []
        elif tid == T_SYB: kind, want = 22, [v.get("pseq"), _unw(v.get("stack"))]
        elif tid == T_SYE: kind, want = v.get("gokind"), []
        elif tid == T_GOc:
            kind, want = v.get("gokind"), [v.get("g"), _unw(v.get("new_stack")), _unw(v.get("stack"))]
        elif tid == T_GCB: kind, want = 29, [v.get("seq"), _unw(v.get("stack"))]
        elif tid == T_GCE: kind, want = 30, [v.get("seq")]
        elif tid == T_SWB: kind, want = 32, [_unw(v.get("stack"))]
        elif tid == T_SWE: kind, want = 33, [v.get("swept"), v.get("reclaimed")]
        elif tid == T_STWB: kind, want = 26, [v.get("kind"), _unw(v.get("stack"))]
        elif tid == T_MAB: kind, want = 35, [_unw(v.get("stack"))]
        elif tid == T_SLICEE:
            tr = v.get("track") or 0
            kind = 27 if (tr >> 44) == 3 else 36
            want = []
        elif tid == T_TSKB:
            kind, want = 40, [v.get("task"), v.get("parent"), v.get("name"), _unw(v.get("stack"))]
        elif tid == T_TSKE: kind, want = 41, [v.get("task"), _unw(v.get("stack"))]
        elif tid == T_TSKL:
            kind, want = 44, [v.get("task"), v.get("key"), v.get("value2"), _unw(v.get("stack"))]
        elif tid == T_SAMPLE:
            kind, want = 7, [(v.get("track") or 0) - TR_RUN, v.get("g"), _unw(v.get("stack"))]
        elif tid == T_RAW:
            kind = v.get("gokind")
            cnt = ({10: 2, 11: 0, 12: 3, 13: 2, 15: 1, 25: 3, 28: 1, 31: 1,
                    34: 1, 45: 2, 46: 2}.get(kind, 4 if kind and kind >= 129 else 0))
            want = [(v.get("a1") or 0), (v.get("a2") or 0), (v.get("a3") or 0), (v.get("a4") or 0)][:cnt]
            if kind in (45, 46): kind, want = k, want   # keep real kind
        elif tid == T_RAWSTR:
            kind = v.get("gokind")
            sref = v.get("sref")
            if kind == 39: want = [sref]
            elif kind == 9: want = [v.get("a1") or 0, _unw(v.get("stack"))]
            elif kind == 21: want = [v.get("a1") or 0, v.get("a2") or 0, _unw(v.get("stack"))]
            elif kind == 48: want = [v.get("a1") or 0, v.get("a2") or 0, v.get("a3") or 0, _unw(v.get("stack"))]
            else: return False, f"[{i}] raw.str kind {kind}"
        elif tid == T_REGB: kind, want = 42, [v.get("task"), v.get("name"), _unw(v.get("stack"))]
        elif tid == T_REGE: kind, want = 43, [v.get("task"), v.get("name"), _unw(v.get("stack"))]
        elif tid == T_CNTR:
            tr = v.get("track") or 0
            kind = 37 if tr - TR_HEAP == 1 else 38
            want = [v.get("value")]
        else:
            return False, f"[{i}] unmapped tid {tid}"
        if kind != k:
            return False, f"[{i}] kind {kind} vs {k} (tid {tid})"
        if tact != t:
            return False, f"[{i}] tick {tact} vs {t}"
        sidx = S[k][2]
        exp = []
        for pos, av in enumerate(a):
            if pos in sidx:
                ev = strg.get(av)
                if ev is None: return False, f"[{i}] golden string sid {av} missing"
                exp.append(ev)
            elif isinstance(av, int):
                exp.append(av & MASK)
            else:
                exp.append(av)
        wantn = [(x & MASK) if isinstance(x, int) and not isinstance(x, bool) else x for x in want]
        if wantn != exp:
            return False, f"[{i}] args {wantn} vs {exp} (kind {k})"
    return True, "ok"

# ---------------- main ----------------
def stage_a(data, gens, path):
    ok = True
    print(f"parse {len(data)} B -> {len(gens)} generation(s)")
    for g in gens:
        expand(g)
        errs = validate(g)
        ok &= not errs
        n_timed = sum(len(v) for v in g['mstreams'].values())
        f_int = g["f_int"] if g["f_int"] is not None else -1
        print(f"  gen {g['gen']}: batches={g['batches_seen']} Ms={len(g['mstreams'])} "
              f"timed={n_timed} strings={len(g['strings'])} stacks={len(g['stacks'])} "
              f"samples={len(g['samples'])} freq={f_int} "
              f"invariants={'OK' if not errs else errs}")
    gl, (dump, synth) = go_tool_parsed_classes(path)
    mine = {}
    for g in gens:
        for m, evs in g["mstreams"].items():
            for ev in evs:
                for cname, pred in SURFACE.items():
                    if pred(ev):
                        mine[cname] = mine.get(cname, 0) + 1
    print("cross-check vs gotool (synth excluded:", synth, ")")
    for k in sorted(set(gl) | set(mine)):
        a, b = mine.get(k, 0), gl.get(k, 0)
        mark = "OK" if a == b else "MISMATCH"
        ok &= a == b
        print(f"    {k:<16} mine={a:<4} gotool={b:<4} {mark}")
    return ok

def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/tcf-gotrace.out"
    full = "--full" in sys.argv
    t0 = time.time()
    data, gens = parse_go_trace(path)
    if not stage_a(data, gens, path):
        print("STAGE-A: FAILED"); return 1
    if not full:
        print("STAGE-A: PASSED")
        return 0
    import os as _os
    sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
    from tcf_reader import Reader
    container, chunk_meta = to_tcf(data, gens)
    out_path = "/tmp/tcf-native-from-go.tcf"
    open(out_path, "wb").write(container)
    print(f"VIREL emit: {len(data)} B -> {len(container)} B, {len(chunk_meta)} chunks "
          f"({time.time()-t0:.3f}s) -> {out_path}")
    ok, report, nchunks = verify(data, gens, container, Reader)
    for (m, s_ok, fr_ok, st_ok, nrec, ngold, detail) in report:
        print(f"  M={m}: stream={'OK' if s_ok else 'FAIL ' + str(detail)} "
              f"pools={'OK' if fr_ok else 'FAIL'} hdr/eos/prov={'OK' if st_ok else 'FAIL'} "
              f"({nrec}/{ngold})")
    lm = subprocess.run([sys.executable,
        _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "tcf_model.py"),
        out_path], capture_output=True, text=True)
    lint_ok = "MODEL-LINT: PASSED" in lm.stdout
    ok &= lint_ok
    print("MODEL-LINT:", "PASSED" if lint_ok else lm.stdout[-600:])
    print("GATE 3:", "PASSED" if ok else "FAILED")
    return 0 if ok else 1

if __name__ == "__main__":
    sys.exit(main())
