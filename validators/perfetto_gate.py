#!/usr/bin/env python3
"""perfetto_gate.py — GATE 2: Perfetto TracePacket stream -> native VIREL, round-trip against a golden model.

No protoc; generic protobuf wire decode. Field numbers verified against perfetto sources 2026-10-03
(framing and clocks re-checked 2026-10-04, ev-perfetto-split-clock):
  Trace:       packet=1 -- a file is the bytes of one Trace message, so every packet is
               framed as 0x0A + varint length + TracePacket (other top-level fields are skipped)
  TracePacket: timestamp=8 trusted_packet_sequence_id=10 track_event=11 interned_data=12
               sequence_flags=13 (bit SEQ_CLEARED=1 / bit NEEDS=2; 3 = both) previous_packet_dropped=42
               clock_snapshot=6 timestamp_clock_id=58 trace_packet_defaults=59
  ClockSnapshot: clocks=1 primary_trace_clock=2; Clock: clock_id=1 timestamp=2 is_incremental=3
               builtin clocks: REALTIME=1 BOOTTIME=6 (default packet clock)
  TrackEvent:  type=9 (SLICE_BEGIN=1 SLICE_END=2 INSTANT=3 COUNTER=4) track_uuid=11
               name_iid=10 name=23 categories=22 counter_value=30 double_counter_value=44
  InternedData:event_categories=1 event_names=2; entry {iid=1, name=2}
  TrackDescriptor: uuid=1 name=2 counter=8 (counter events need a counter track)

Mapping (per coverage matrix / otlp finding):
  trusted_packet_sequence_id -> VIREL writer id (ONE WRITER PER CHUNK; one chunk per sequence)
  InternedData entries       -> mid-chunk checkpoint pool (type virel.String)
  SLICE_BEGIN/SLICE_END pairs-> two declared events (streaming cannot back-patch a folded
                                duration into an already-written BEGIN position; slices are
                                reconstructed on read exactly like CTEF B/E)
  INSTANT / COUNTER          -> instant / counter event types
  previous_packet_dropped    -> virel.loss sync event; SEQ_INCREMENTAL_STATE_CLEARED -> virel.clear
  packet timestamp (BOOTTIME ns) -> chunk-relative ticks; ticksPerSecond = 1e9 (tick == ns)
  chunk startNanos (UTC ns)  -> REALTIME of the first event, via the latest ClockSnapshot whose
                                BOOTTIME is at or before it (REALTIME - BOOTTIME offset)
Not handled (the gate fails loudly instead of misreading): packets on a clock other than BOOTTIME
(timestamp_clock_id / trace_packet_defaults, incl. SDK incremental clocks), compressed packets.
"""
import struct, sys
from collections import defaultdict
sys.path.insert(0, __file__.rsplit("/", 1)[0])
from tcf_reader import Reader, Cursor

# ---------------- protobuf wire ----------------
def rd_varint(d, p):
    v = sh = 0
    while True:
        b = d[p]; p += 1
        v |= (b & 0x7F) << sh
        if b < 0x80: return v, p
        sh += 7

def le_varint(v):
    if v < 0: v &= (1 << 64) - 1
    out = bytearray()
    while True:
        b = v & 0x7F; v >>= 7
        if v: out.append(b | 0x80)
        else: out.append(b); return bytes(out)

def wf(fno): return le_varint((fno << 3) | 0)      # varint
def wl(fno): return le_varint((fno << 3) | 2)      # length-delim

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

TP_TS, TP_SEQ, TP_TE, TP_INT, TP_FLAGS, TP_DROP, TP_DESC = 8, 10, 11, 12, 13, 42, 60
TP_CLOCK, TP_CLOCK_ID, TP_DEFAULTS = 6, 58, 59
TRACE_PACKET = 1                        # Trace.packet
CS_CLOCKS, CS_PRIMARY, CK_ID, CK_TS, CK_INCR = 1, 2, 1, 2, 3
CLK_REALTIME, CLK_BOOTTIME = 1, 6
TE_TYPE, TE_NAME_IID, TE_TRACK, TE_NAME, TE_CATS, TE_COUNTER = 9, 10, 11, 23, 22, 30
ID_CATS, ID_NAMES, EI_IID, EI_NAME = 1, 2, 1, 2
SEQ_NEEDS, SEQ_CLEARED = 2, 1

# ---------------- stage 1: synthetic perfetto stream + golden model ----------------
# Packet timestamps are BOOTTIME (ns since boot), not UNIX time. The service sequence (id 1)
# carries a ClockSnapshot that relates BOOTTIME to REALTIME, as traced does at session start.
T0 = 3_600_000_000_000                       # BOOTTIME: one hour after boot
REAL_AT_T0 = 1_759_600_000_000_000_000       # REALTIME (UNIX ns) at BOOTTIME == T0 - 1000
SERVICE_SEQ = 1

def is_cleared(flags):
    return flags is not None and (flags & SEQ_CLEARED) != 0

def synth():
    buf = bytearray(); golden = {}
    def packet(p_: bytes): buf.extend(wl(TRACE_PACKET) + le_varint(len(p_)) + p_)
    def clock(cid, ts):
        c = wf(CK_ID) + le_varint(cid) + wf(CK_TS) + le_varint(ts)
        return wl(CS_CLOCKS) + le_varint(len(c)) + c
    snap = clock(CLK_BOOTTIME, T0 - 1000) + clock(CLK_REALTIME, REAL_AT_T0) + wf(CS_PRIMARY) + le_varint(CLK_BOOTTIME)
    packet(wf(TP_TS) + le_varint(T0 - 1000) + wf(TP_SEQ) + le_varint(SERVICE_SEQ) +
           wf(TP_CLOCK_ID) + le_varint(CLK_BOOTTIME) + wl(TP_CLOCK) + le_varint(len(snap)) + snap)
    iids = {}
    def interned(seq, table, name):
        k = iids.setdefault((seq, table), {})
        if name in k: return k[name]
        k[name] = iid = len(k) + 1
        pend[(seq, table)].append((iid, name))
        return iid
    pend = defaultdict(list)
    def flush_interned(seq):
        if not pend[(seq, ID_NAMES)] and not pend[(seq, ID_CATS)]: return
        payload = b""
        for (iid, name) in pend[(seq, ID_NAMES)]:
            payload += wl(ID_NAMES) + le_varint(len(wf(EI_IID) + le_varint(iid) + wl(EI_NAME) + le_varint(len(name)) + name.encode())) \
                       + wf(EI_IID) + le_varint(iid) + wl(EI_NAME) + le_varint(len(name)) + name.encode()
        for (iid, name) in pend[(seq, ID_CATS)]:
            payload += wl(ID_CATS) + le_varint(len(wf(EI_IID) + le_varint(iid) + wl(EI_NAME) + le_varint(len(name)) + name.encode())) \
                       + wf(EI_IID) + le_varint(iid) + wl(EI_NAME) + le_varint(len(name)) + name.encode()
        packet(wf(TP_SEQ) + le_varint(seq) + wl(TP_INT) + le_varint(len(payload)) + payload)
        pend[(seq, ID_NAMES)].clear(); pend[(seq, ID_CATS)].clear()
    for seq in (7, 11):
        g = golden[seq] = {"slices": [], "instants": [], "counters": [], "loss": 0, "clear": 1,
                           "start_unix": REAL_AT_T0 + (T0 + seq * 10) - (T0 - 1000)}
        # track descriptor; the first packet of a sequence clears incremental state (SDK behaviour)
        name = f"track-{seq}"
        packet(wf(TP_SEQ) + le_varint(seq) + wf(TP_FLAGS) + le_varint(SEQ_CLEARED) +
               wl(TP_DESC) + le_varint(len(wf(1) + le_varint(101 + seq) + wl(2) + le_varint(len(name)) + name.encode())) +
               wf(1) + le_varint(101 + seq) + wl(2) + le_varint(len(name)) + name.encode())
        # counter events must target a track whose descriptor declares `counter` (field 8)
        cname = f"counter-{seq}"
        cdesc = wf(1) + le_varint(201 + seq) + wl(2) + le_varint(len(cname)) + cname.encode() + wl(8) + le_varint(0)
        packet(wf(TP_SEQ) + le_varint(seq) + wl(TP_DESC) + le_varint(len(cdesc)) + cdesc)
        w = interned(seq, ID_NAMES, "work"); io = interned(seq, ID_NAMES, "io")
        cat = interned(seq, ID_CATS, "io-cat")
        flush_interned(seq)
        for i in range(3):
            ts = T0 + seq * 10 + i * 4
            def te(seq=seq, **b):
                pkt = wf(TP_TS) + le_varint(b["ts"]) + wf(TP_SEQ) + le_varint(seq) + \
                      wl(TP_TE) + le_varint(len(b["body"])) + b["body"]
                packet(pkt)
            te(seq=seq, ts=ts, body=wf(TE_TYPE) + le_varint(1) + wf(TE_TRACK) + le_varint(101 + seq) +
                              wf(TE_NAME_IID) + le_varint(w))
            g["slices"].append([ts, None, "work"])
            te(seq=seq, ts=ts + 1, body=wf(TE_TYPE) + le_varint(3) + wf(TE_TRACK) + le_varint(101 + seq) +
                                   wf(TE_NAME_IID) + le_varint(io))
            g["instants"].append((ts + 1, "io"))
            te(seq=seq, ts=ts + 2, body=wf(TE_TYPE) + le_varint(4) + wf(TE_TRACK) + le_varint(201 + seq) +
                                   wf(TE_COUNTER) + le_varint(42 + i))
            g["counters"].append(42 + i)
            te(seq=seq, ts=ts + 3, body=wf(TE_TYPE) + le_varint(2) + wf(TE_TRACK) + le_varint(101 + seq))
            g["slices"][i][1] = ts + 3
        packet(wf(TP_SEQ) + le_varint(seq) + wf(TP_DROP) + bytes([1]) + wf(TP_FLAGS) + le_varint(SEQ_NEEDS))
        g["loss"] += 1
        # a reset packet commonly carries CLEARED|NEEDS (3): must count as a clear (bit test)
        packet(wf(TP_SEQ) + le_varint(seq) + wf(TP_FLAGS) + le_varint(SEQ_CLEARED | SEQ_NEEDS))
        g["clear"] += 1
    return bytes(buf), golden

# ---------------- stage 2: VIREL native container ----------------
SIG = 0x80
E_BASE = 1_700_000_000_000_000_000          # envelope: chunk base for ticks
W64 = (1 << 64) - 1

def s_utf8(s):
    b = s.encode(); return bytes([3]) + le_varint(len(b)) + b

def s_ref(key): return bytes([2]) + le_varint(key)

def event(tid, body):
    inner = le_varint(tid) + body
    size = len(inner) + len(le_varint(len(inner)))
    return le_varint(size) + inner

def ev_header(nanos_of_first) -> bytearray:
    hdr = bytearray(76)
    hdr[0:4] = b"VIRL"   # VIREL native magic
    struct.pack_into(">HH", hdr, 4, 0, 1)
    struct.pack_into(">Q", hdr, 32, nanos_of_first)      # startNanos (UTC ns)
    struct.pack_into(">Q", hdr, 56, 1_000_000_000)       # ticksPerSecond = 1e9 => tick == ns
    struct.pack_into(">B", hdr, 64, 0)                   # COMPLETE
    struct.pack_into(">H", hdr, 66, SIG << 8)            # @66: signature | encoding-default(0)
    return hdr

def tcf_string_pool_event(pool_keys):
    # checkpoint: [ts][dur][delta=0][flush][poolCount][typeId][count][{key, value}]
    body = le_varint(0) + le_varint(0) + le_varint(0) + bytes([1]) + le_varint(1) \
           + le_varint(1) + le_varint(len(pool_keys))
    for (s, k) in sorted(pool_keys.items(), key=lambda kv: kv[1]):
        body += le_varint(k) + s_utf8(s)
    return event(1, body)

def metadata_event():
    strings = ["root", "metadata", "region", "class", "field", "name", "id", "superType",
               "constantPool", "jdk.jfr.Event", "virel.Type", "virel.slice.begin", "virel.slice.end",
               "virel.instant", "virel.counter", "virel.loss", "virel.clear", "virel.String",
               "track", "value", "writer", "span",
               "long", "0", "1", "2", "3", "4", "5", "6", "7", "100", "gmtOffset", "locale", "en_GB"]
    P = [s if False else s for s in strings]
    def ix(s): return strings.index(s)
    def elem(name_i, attrs, children):
        # element refs are BARE pool indices (no tags) in the XML grammar
        out = le_varint(name_i) + le_varint(len(attrs))
        for (an, av) in attrs: out += le_varint(an) + le_varint(av)
        out += le_varint(len(children))
        for ch in children: out += ch
        return out
    def field(fn, tid, cp=False):
        a = [(ix("name"), ix(fn)), (ix("class"), ix(str(tid)))]   # linked-type attr = "class"
        if cp: a.append((ix("constantPool"), ix("0")))
        return elem(ix("field"), a, [])
    def cls(tid, nm, sup, flds):
        a = [(ix("name"), ix(nm)), (ix("id"), ix(str(tid)))]
        if sup: a.append((ix("superType"), ix(sup)))
        return elem(ix("class"), a, [field(*f) for f in flds])
    types = [
        cls(100, "long", None, []),
        cls(1, "virel.String", None, []),
        cls(2, "virel.slice.begin", "jdk.jfr.Event", [("track", 100), ("value", 1)]),
        cls(3, "virel.slice.end", "jdk.jfr.Event", [("track", 100)]),
        cls(4, "virel.instant", "jdk.jfr.Event", [("track", 100), ("value", 1)]),
        cls(5, "virel.counter", "jdk.jfr.Event", [("track", 100), ("value", 100)]),
        cls(6, "virel.loss", "jdk.jfr.Event", [("writer", 100), ("span", 100)]),
        cls(7, "virel.clear", "jdk.jfr.Event", [("writer", 100)]),
    ]
    region = elem(ix("region"), [(ix("gmtOffset"), ix("0"))], [])
    meta = elem(ix("metadata"), [], types)
    root = elem(ix("root"), [], [meta, region])
    payload = le_varint(len(strings)) + b"".join(s_utf8(s) for s in strings) + root
    # native meta preamble = [ts][dur][metadataId] then envelope [tag][payload] (no inner length)
    return event(0, le_varint(0) + le_varint(0) + le_varint(1) + bytes([0]) + payload)

def clock_offsets(packets):
    """ClockSnapshot packets -> sorted [(boottime, realtime - boottime)]"""
    offs = []
    for pkt in packets:
        f = fields(pkt)
        for cs in f.get(TP_CLOCK, []):
            clocks = {}
            for c in fields(cs).get(CS_CLOCKS, []):
                cf = fields(c)
                if cf.get(CK_INCR, [0])[0]:
                    continue
                clocks[cf[CK_ID][0]] = cf[CK_TS][0]
            if CLK_BOOTTIME in clocks and CLK_REALTIME in clocks:
                offs.append((clocks[CLK_BOOTTIME], clocks[CLK_REALTIME] - clocks[CLK_BOOTTIME]))
    return sorted(offs)

def boot_to_unix(offs, ts):
    prior = [o for (b, o) in offs if b <= ts]
    if not prior:
        raise ValueError(f"no BOOTTIME/REALTIME ClockSnapshot at or before {ts}")
    return ts + prior[-1]

def to_tcf(packets_by_seq, golden, offs):
    """per sequence -> one native chunk; returns container bytes"""
    out = bytearray()
    writer_prefix = 0xC0FFEE0000000000
    for seq, packets in sorted(packets_by_seq.items()):
        # translate
        pool = {}
        def pk(s):
            if s not in pool: pool[s] = len(pool) + 1
            return pool[s]
        body_events = []   # (tid, body) in packet order
        base = None
        for pkt in packets:
            f = fields(pkt)
            # interned data first
            for idata in f.get(TP_INT, []):
                g = fields(idata)
                for tno in (ID_NAMES, ID_CATS):
                    for entry in g.get(tno, []):
                        e = fields(entry)
                        iid = e[1][0]
                        nm = e[2][0].decode()
                        pool_key = pk(nm)
                        # NOTE: use iid -> key mapping identical for both tables
                        evs_map.setdefault((seq, tno, iid), pool_key)
            if TP_DEFAULTS in f or f.get(TP_CLOCK_ID, [CLK_BOOTTIME])[0] != CLK_BOOTTIME:
                raise ValueError(f"seq {seq}: packet clock other than BOOTTIME is not supported")
            for dropped in f.get(TP_DROP, []):
                body_events.append((6, le_varint(W64 & (writer_prefix + seq)) + le_varint(1)))
            if is_cleared(f.get(TP_FLAGS, [None])[-1]):
                body_events.append((7, le_varint(W64 & (writer_prefix + seq))))
            if TP_TE not in f: continue
            ts = f[TP_TS][0]
            if base is None: base = ts
            tick = ts - base
            for te in f.get(TP_TE, []):
                g = fields(te)
                et = g[TE_TYPE][0]
                track = g.get(TE_TRACK, [0])[0]
                hdr_words = []
                te_event_ticks = ts - base
                if et == 1:
                    nm_k = evs_map[(seq, ID_NAMES, g[TE_NAME_IID][0])]
                    body_events.append((2, le_varint(te_event_ticks) + le_varint(track) + s_ref(nm_k)))
                elif et == 2:
                    body_events.append((3, le_varint(te_event_ticks) + le_varint(track)))
                elif et == 3:
                    nm_k = evs_map[(seq, ID_NAMES, g[TE_NAME_IID][0])]
                    body_events.append((4, le_varint(te_event_ticks) + le_varint(track) + s_ref(nm_k)))
                elif et == 4:
                    body_events.append((5, le_varint(te_event_ticks) + le_varint(track) + le_varint(g[TE_COUNTER][0])))
        meta = metadata_event()
        cp = tcf_string_pool_event(pool)
        chunk_start = len(out)
        hdr = ev_header(boot_to_unix(offs, base) if base is not None else E_BASE)   # startNanos = REALTIME
        hdr[68:76] = struct.pack(">Q", writer_prefix + seq)     # writer id
        last_tick = max((0,) + tuple(rd_varint(b_, 0)[0] for (t_, b_) in body_events if t_ in (2, 3, 4, 5)))
        struct.pack_into(">Q", hdr, 40, last_tick + 1)          # chunk duration (ns == ticks here)
        struct.pack_into(">Q", hdr, 24, 76)                     # metadata position
        struct.pack_into(">Q", hdr, 16, 76 + len(meta))         # last checkpoint position
        out += hdr
        out += meta
        out += cp
        end_off = len(out)
        for (tid, body) in body_events:
            out += event(tid, body)
        struct.pack_into(">Q", out, chunk_start + 8, len(out) - chunk_start)
    return bytes(out)

evs_map = {}
# ---------------- stage 3: verify vs golden ----------------
def stream_packets(data):
    """top level of a Trace message: yield each `packet` (field 1), skip any other field"""
    p = 0
    while p < len(data):
        key, p = rd_varint(data, p)
        fno, wt = key >> 3, key & 7
        if wt == 2:
            n, p = rd_varint(data, p)
            if fno == TRACE_PACKET and n:
                yield data[p:p + n]
            p += n
        elif wt == 0: _, p = rd_varint(data, p)
        elif wt == 1: p += 8
        elif wt == 5: p += 4
        else: raise IOError(f"top-level wire type {wt}")
        if p > len(data):
            raise IOError("truncated trace: packet runs past end of data")

def grouped(data):
    g = defaultdict(list)
    for pkt in stream_packets(data):
        f = fields(pkt)
        seq = f.get(TP_SEQ, [None])[0]
        g[seq].append(f)
    return g

def main():
    data, golden = synth()
    t0 = time.time()
    pkt_groups = {}
    packets = list(stream_packets(data))
    for pkt in packets:
        f = fields(pkt)
        seq = f.get(TP_SEQ, [None])[0]
        if seq == SERVICE_SEQ:
            continue                   # service packets: clock snapshots only, no chunk
        pkt_groups.setdefault(seq, []).append(pkt)
    container = to_tcf(pkt_groups, golden, clock_offsets(packets))
    open("/tmp/tcf-native-from-perfetto.tcf", "wb").write(container)
    print(f"perfetto stream {len(data)} B -> VIREL {len(container)} B ({time.time()-t0:.3f}s)")

    # read with THE reader (one implementation, both container lines)
    r = Reader(); r.record = True
    r.run(container)
    ok = True
    # reassociate records per writer: order of chunks == sorted seqs
    seqs = sorted(golden)
    # collect per-chunk records by re-running per chunk for clarity
    import io as _io
    checks = []
    per_chunk = []
    pos = 0
    D = container
    while pos < len(D):
        size = struct.unpack_from(">q", D, pos + 8)[0]
        per_chunk.append(D[pos:pos + size]); pos += size
    for chunk_bytes in per_chunk:
        sub = Reader(); sub.record = True
        sub.run(chunk_bytes)
        per_chunk.append
        records = sub.records
        # map writer id -> seq
        wid = struct.unpack_from(">Q", chunk_bytes, 68)[0]
        seq = wid - 0xC0FFEE0000000000
        g = golden[seq]
        spool = sub.pools.get(1, {})
        begins = [rec for rec in records if rec[0] == 2]
        ends = [rec for rec in records if rec[0] == 3]
        insts = [rec for rec in records if rec[0] == 4]
        cntrs = [rec for rec in records if rec[0] == 5]
        loss = [rec for rec in records if rec[0] == 6]
        clear = [rec for rec in records if rec[0] == 7]
        begin_names = [(b[3].get("value")) for b in begins]
        inst_names = [(i[3].get("value")) for i in insts]
        durs = [e[1] - b[1] for b, e in zip(begins, ends)]
        g_durs = [end - begin for (begin, end, _) in g["slices"]]
        g_instants = [nm for (ts, nm) in g["instants"]]
        c1 = len(begins) == len(ends) == len(g["slices"]) and begin_names == ["work"] * len(g["slices"])
        c2 = durs == g_durs
        c3 = len(insts) == len(g["instants"]) and inst_names == g_instants
        c4 = [c[3].get("value") for c in cntrs] == g["counters"]
        c5 = len(loss) == g["loss"] and len(clear) == g["clear"]
        c6 = all(b[3].get("track") == 101 + seq for b in begins)
        c7 = struct.unpack_from(">Q", chunk_bytes, 32)[0] == g["start_unix"]
        checks.append((seq, c1, c2, c3, c4, c5, c6, c7))
        ok &= c1 and c2 and c3 and c4 and c5 and c6 and c7
    for (seq, a, b, c, d_, e, f_, h) in checks:
        print(f"  seq {seq}: slices {a} durations {b} instants {c} counters {d_} loss/clear {e} tracks {f_} wallclock {h}")
    print("GATE 2:", "PASSED" if ok else "FAILED")
    return 0 if ok else 1

if __name__ == "__main__":
    import time
    sys.exit(main())
