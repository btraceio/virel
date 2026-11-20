#!/usr/bin/env python3
"""tcf_reader.py — THE reference implementation under test.

One reader for:
  * native VIREL chunks  (major 0/1, VIREL-signature bit set, 76B header, writer-id)
  * JFR legacy annex   (majors 1/2, signature clear, 68B header, byte-frozen semantics)
  * foreign chunks     (unknown major/minor: bridged via magic scan, never interpreted)

Grammar sources (verified, with citations in ../nodes/):
  ev-chunk-header, ev-framing-constants, ev-content-encodings, ev-metadata-event,
  ev-parser-dynamics + direct reads of ChunkParser/ParserFactory/RecordingInput/StringParser.

Exit code 0 = gate passed (strict size accounting held for the whole container).
"""
import struct, sys, time
from collections import defaultdict

MAGIC_ANNEX = b"FLR\x00"          # JFR annex magic (ChunkHeader.java:43)
MAGIC_NATIVE = b"VIRL"             # VIREL native chunk magic
MAGICS = (MAGIC_ANNEX, MAGIC_NATIVE)
FLAG_STREAMING = 0x04              # native header low flags byte @67, bit 2 (spec §3.9.6)
TYPE_END_OF_PARTITION = 8          # m0 registered END_OF_PARTITION (spec §4.4)

class ModelComposite(dict):
    """composite value that REMEMBERS its named-field model form"""
    __slots__ = ("model_fields",)
    def __init__(self, out, fl):
        super().__init__(out); self.model_fields = fl

# ---------- chunk extent (spec §3.9, §3.9.6) ----------

def walk_streaming_chunk(d, pos):
    """walk a streaming chunk's size-prefixed events from offset 76 through END_OF_PARTITION.
    Returns (length, complete, last_state_snapshot_offset) with the offset chunk-relative,
    0 if none. An incomplete chunk (no terminator) runs to the end of the data."""
    p, last_snapshot = pos + 76, 0
    try:
        while p < len(d):
            cur = Cursor(d, p)
            n = cur.varint()
            if n <= 0 or p + n > len(d):
                break
            tid = cur.varint()
            if tid == 1:
                last_snapshot = p - pos
            p += n
            if tid == TYPE_END_OF_PARTITION:
                return p - pos, True, last_snapshot
    except (IOError, IndexError):
        pass
    return len(d) - pos, False, last_snapshot

def is_streaming_header(d, pos):
    """streaming state: native magic, STREAMING flag set, size field 0 (spec §3.9.6). A
    non-zero size is authoritative even while the flag is still set (mid-finalization)."""
    return (pos + 76 <= len(d) and d[pos:pos + 4] == MAGIC_NATIVE and d[pos + 67] & FLAG_STREAMING != 0
            and struct.unpack_from(">Q", d, pos + 8)[0] == 0)

def chunk_extent(d, pos):
    """(length, complete) of the chunk starting at pos.
    A finalized chunk declares its size at offset 8. A streaming chunk (native, size 0,
    STREAMING flag) is never back-patched by its writer: it extends through its
    END_OF_PARTITION event, found by walking size-prefixed events. A streaming chunk cut
    before its terminator runs to the end of the data and is incomplete."""
    if pos + 76 > len(d):
        return len(d) - pos, False
    size = struct.unpack_from(">Q", d, pos + 8)[0]
    if size:
        return min(size, len(d) - pos), pos + size <= len(d)
    if not is_streaming_header(d, pos):
        return len(d) - pos, False
    n, complete, _ = walk_streaming_chunk(d, pos)
    return n, complete

# ---------- cursor ----------

class Cursor:
    __slots__ = ("d", "pos", "end")
    def __init__(self, d, pos=0, end=None):
        self.d, self.pos, self.end = d, pos, len(d) if end is None else end
    def assert_avail(self, n, w):
        if self.pos + n > self.end: raise IOError(f"{w}: need {n} bytes at {self.pos}, end {self.end}")
    def u1(self):
        self.assert_avail(1, "u1"); b = self.d[self.pos]; self.pos += 1; return b
    def varint(self):  # LEB128, <=9 bytes, 9th = raw 8 bits  [RecordingInput.readLong L298-355]
        self.assert_avail(1, "varint")
        v = 0; shift = 0
        for i in range(9):
            b = self.d[self.pos]; self.pos += 1
            if i == 8:
                v |= (b & 0xFF) << shift; break
            v |= (b & 0x7F) << shift
            if b < 0x80: break
            shift += 7
        return v
    def s64(self):     # two's-complement 64-bit interpretation
        v = self.varint()
        return v - (1 << 64) if v & (1 << 63) else v
    def i32(self): v = self.varint(); v &= 0xFFFFFFFF; return v - (1 << 32) if v & (1 << 31) else v
    def f32(self):
        self.assert_avail(4, "f32"); v = struct.unpack_from(">f", self.d, self.pos)[0]; self.pos += 4; return v
    def f64(self):
        self.assert_avail(8, "f64"); v = struct.unpack_from(">d", self.d, self.pos)[0]; self.pos += 8; return v
    def bytes_(self, n):
        self.assert_avail(n, "bytes"); r = self.d[self.pos:self.pos+n]; self.pos += n; return r

# ---------- metadata (annex: binarized-XML v2; mirrors JDK MetadataReader.java) ----------

class Field:
    __slots__ = ("name", "type_id", "dimension", "constant_pool", "annotation_refs")

class AnnType: __slots__ = ("id", "name", "super", "simple", "fields", "attrs")
class Type(AnnType): pass

def read_metadata_payload(cur):
    """returns (pool, root_element, region_attrs)"""
    pool_count = cur.varint()                      # int size = string pool size
    pool = []
    for _ in range(pool_count):                    # StringParser
        tag = cur.u1()
        if tag == 0: pool.append(None)
        elif tag == 1: pool.append("")
        elif tag == 2:
            pool.append(f"<poolref:{cur.varint()}>")   # not expected at top level; keep visible
        elif tag == 3:
            n = cur.varint(); pool.append(cur.bytes_(n).decode("utf-8", "replace"))
        elif tag == 4:                             # char array: count + count VARINT code units
            n = cur.varint()
            units = [cur.varint() for _ in range(n)]
            pool.append("".join(chr(u) for u in units))
        elif tag == 5:
            n = cur.varint(); pool.append(cur.bytes_(n).decode("latin-1"))
        else: raise IOError(f"unknown string tag {tag}")
    def element():
        nI = cur.varint()
        raw_a = []
        attrs = {}
        for _ in range(cur.varint()):
            aI = cur.varint(); vI = cur.varint()
            raw_a.append((aI, vI)); attrs[pool[aI]] = pool[vI]
        children = [element() for _ in range(cur.varint())]
        return {"name": pool[nI] if nI < len(pool) else "<ref!>", "n": nI,
                "attrs": attrs, "a": raw_a, "c": len(children),
                "children": children}
    return pool, element()

def build_types(pool, root):
    types = {}
    def find(name, node):
        for c in node["children"]:
            if c["name"] == name: return c
        return None
    meta = find("metadata", root) or {}
    for t in meta.get("children", []):
        if t["name"] != "class": continue   # ELEMENT_TYPE is literally "class" (MetadataDescriptor)
        a = t["attrs"]
        ty = Type(); ty.id = int(a.get("id", "-1")); ty.name = a.get("name", "?")
        ty.super = a.get("super-type", None) or a.get("superType", None)
        ty.simple = "simple-type" in a or "simpleType" in a
        ty.fields = []; ty.attrs = a
        types[ty.id] = ty
    for t in meta.get("children", []):
        if t["name"] != "class": continue   # ELEMENT_TYPE is literally "class" (MetadataDescriptor)
        a = t["attrs"]; ty = types[int(a.get("id", "-1"))]
        for c in t["children"]:
            if c["name"] != "field": continue
            f = Field(); fa = c["attrs"]
            f.name = fa.get("name", "?")
            f.type_id = int(fa.get("class", fa.get("type-id", fa.get("typeId", "-1"))))  # ATTRIBUTE_TYPE_ID = "class"
            f.dimension = int(fa.get("dimension", "0"))
            f.constant_pool = "constant-pool" in fa or "constantPool" in fa
            ty.fields.append(f)
    return types

# ---------- field parsers [ParserFactory.createPrimitiveParser] ----------

PRIM = ("int", "long", "short", "char", "byte", "boolean", "float", "double", "java.lang.String")

class Reader:
    def __init__(self):
        self.record = False
        self.records = []
        self.state_snapshot_models = []
        self.chunk_models = []
        self.meta_preamble = None
        self.meta_pool = None
        self.meta_root = None
        self.meta_encoding = 0
        self.types = {}
        self.pools = defaultdict(dict)      # type_id -> {key: value}
        self.stats = defaultdict(int)       # type name -> event count
        self.event_bytes = defaultdict(int) # typeId -> parsed payload bytes (size accounting per event)
        self.locks = {}                     # typeId -> (has_duration, has_thread, has_stack) — inferred once, verified always
        self.foreign_spans = []
        self.chunks = 0

    # -- value parsing; returns python value or (kind,) marker
    def value(self, cur, f, inside_type=None):
        ty = self.types.get(f.type_id)
        name = ty.name if ty else f"#{f.type_id}"
        if f.dimension and f.dimension > 0:                # array: count + elements
            count = cur.varint()
            fe = Field(); fe.name = f.name; fe.type_id = f.type_id
            fe.dimension = 0; fe.constant_pool = f.constant_pool
            return [self.value(cur, fe) for _ in range(count)]
        if name in ("java.lang.String", "virel.String", "String"):     # string-kind fields: tag-encoded (tag 2 = pool ref)
            return self.string(cur, f)
        if f.constant_pool:                                # non-string pooled: varint key (ConstantValueParser)
            return ("@key", cur.varint())
        if name in ("bytes", "virel.bytes"):
            n = cur.varint(); return {"v": cur.bytes_(n)}
        if name in ("int", "long"):
            v = cur.s64()
            return v if name == "long" else (v & 0xFFFFFFFF if not (v & 0x80000000) else v - (1 << 32)) if False else (v - (1 << 32) if name == "int" and v & 0x80000000 else v)
        if name == "short":
            v = cur.s64() & 0xFFFF
            return v - (1 << 16) if v & (1 << 15) else v
        if name == "char":
            return cur.varint()
        if name == "byte":
            return cur.u1() - 256 if False else cur.u1()
        if name == "boolean":
            return cur.u1() != 0
        if name == "float":  return cur.f32()
        if name == "double": return cur.f64()
        if ty is not None and ty.fields:                   # declared composite object
            return self.composite(cur, ty)
        raise IOError(f"cannot parse field '{f.name}' of type '{name}'")

    def string(self, cur, f=None):
        tag = cur.u1()
        if tag == 0: return None
        if tag == 1: return ""
        if tag == 2:
            key = cur.varint()                             # resolve against the string type's pool
            if f is not None and f.type_id in self.pools:
                return self.pools[f.type_id].get(key)
            return ("@poolref", key)
        if tag == 3:
            n = cur.varint(); return cur.bytes_(n).decode("utf-8", "replace")
        if tag == 4:                               # char array: count + count VARINT code units
            n = cur.varint()
            return "".join(chr(cur.varint()) for _ in range(n))
        if tag == 5:
            n = cur.varint(); return cur.bytes_(n).decode("latin-1")
        raise IOError(f"unknown string tag {tag}")

    def composite(self, cur, ty):
        return self.composite_model(cur, ty, model=False)

    def composite_model(self, cur, ty, model=True):
        out = {}; fl = []
        for f in ty.fields:
            v = self.value(cur, f, ty)
            out[f.name] = v
            if model and self.record: fl.append({"name": f.name, "value": {"v": v}})
        if model and self.record is not None and model:
            return ModelComposite(out, fl)
        return out

    # -- state snapshot (JFR annex calls its type-1 record a checkpoint)
    def state_snapshot(self, cur, end, native):
        t0_ = cur.varint()   # timestamp (annex: absolute ticks; non-monotonic)
        t1_ = cur.varint()   # duration
        delta_ = cur.varint()
        flush_ = cur.u1()
        if native and flush_ & 0x7e:
            raise IOError("reserved state-snapshot flag bit set")
        has_summary = native and bool(flush_ & 0x80)
        model_ = {"startTime": t0_, "duration": t1_, "delta": delta_,
                  "flush": bool(flush_ & 1), "summaryVersion": 0,
                  "hasTypeSummary": has_summary, "typeSummary": [], "pools": []}
        if has_summary:
            model_["summaryVersion"] = cur.varint()
            if model_["summaryVersion"] != 1:
                raise IOError("unsupported state-snapshot summary version")
            ntypes = cur.varint()
            previous = -1
            for _ in range(ntypes):
                tid = cur.varint(); count = cur.varint()
                if tid <= previous or tid in (0, 1) or count <= 0:
                    raise IOError(f"bad state-snapshot type summary id {tid}")
                previous = tid
                model_["typeSummary"].append({"typeId": tid, "emittedCount": count})
        pool_count = cur.varint()
        if self.record: self.state_snapshot_models.append(model_)
        for _ in range(pool_count):
            tid = cur.varint()
            count = cur.varint()
            ty = self.types.get(tid)
            str_ty = (ty is not None and not ty.fields)  # no-field pooled type = string pool
            entries_ = []
            for _ in range(count):
                key = cur.varint()
                if str_ty or (ty is not None and not ty.fields):
                    val = self.string(cur, None) if str_ty else None
                elif ty is not None and ty.fields:
                    val = self.composite_model(cur, ty)
                else:
                    raise IOError(f"checkpoint pool for unknown type {tid}")
                self.pools[tid][key] = val
                if self.record:
                    entries_.append({"key": key, "value": {"v": val}})
            if self.record:
                model_["pools"].append({"typeId": tid, "entries": entries_})
        if cur.pos != end: raise IOError(f"state snapshot size mismatch: end {end} vs {cur.pos}")

    # -- events walk (annex: absolute ticks; empirically lock implicit words per typeId)
    def walk_events(self, cur, chunk_start, header_size, chunk_end, native=False):
        pos = chunk_start + header_size
        cur.pos = pos
        while cur.pos < chunk_end:
            ev_start = cur.pos
            size = cur.varint()
            if size <= 0 or ev_start + size > chunk_end: raise IOError(f"bad event size {size} at {ev_start}")
            end = ev_start + size
            tid = cur.varint()
            if tid == 0:                                    # metadata event (re-read via header pointer; skip here)
                cur.pos = end; self.stats["<metadata>"] += 1; continue
            if tid == 1:
                self.state_snapshot(cur, end, native); self.stats["<state-snapshot>"] += 1; continue
            if cur.pos == end:                              # zero-payload event
                self.event_bytes[tid] = size; self.stats[self.tname(tid)] += 1; continue
            ty = self.types.get(tid)
            if ty is None:
                cur.pos = end                               # unknown/undeclared: SKIP by size (events skip, pools abort)
                self.stats[f"<skipped #{tid}>"] += 1
                self.event_bytes[tid] = size
                continue
            if not ty.fields:                               # no-payload declared type
                cur.pos = end; self.event_bytes[tid] = size
                self.stats[self.tname(tid)] += 1; continue
            ty.is_event = ty.super in ("jdk.jfr.Event", "virel.Event")
            if ty.is_event:
                # words: [startTicks = startTime field][durationTicks = duration field, iff declared]
                # [EventParser.java:68-69: hasDuration = 'duration' field present; startIndex = hasDuration?2:1]
                start_tick = cur.s64()                      # startTime word
                dur_tick = None
                if any(f.name == "duration" for f in ty.fields):
                    dur_tick = cur.s64()                    # durationTicks word
                start_val = start_tick
                vals = {}
                if self.record: vals["startTime"] = start_val
                for f in ty.fields:
                    if f.name in ("startTime", "duration"):
                        continue                # both consumed as the event's time words
                    v = self.value(cur, f, ty)
                    if self.record: vals[f.name] = v
                if self.record: self.records.append((tid, start_tick, dur_tick, vals))
            else:
                vals = {}
                for f in ty.fields: v = self.value(cur, f, ty); vals[f.name] = v
                if self.record: self.records.append((tid, None, None, vals))
            if cur.pos != end:
                raise IOError(f"event walk mismatch eventId={tid} @{ev_start}: end {end} vs {cur.pos}")
            self.event_bytes[tid] = size
            self.stats[self.tname(tid)] += 1
        return cur.pos == chunk_end


    def tname(self, tid):
        ty = self.types.get(tid); return ty.name if ty else f"#{tid}"

    # ---------- container level ----------
    def accept_candidate(self, pos):
        """(classification, header_size) for a magic candidate, else None."""
        d = self.d
        if pos + 12 > len(d): return None
        m4 = d[pos:pos+4]
        if m4 not in MAGICS: return None
        major = struct.unpack_from(">H", d, pos + 4)[0]
        minor = struct.unpack_from(">H", d, pos + 6)[0]
        flags = struct.unpack_from(">H", d, pos + 66)[0] if pos + 68 <= len(d) else 0
        sig = (flags >> 8) & 0x80
        if m4 == MAGIC_NATIVE:
            if sig and major == 0:
                return ("native", 76, major, minor)
            return None
            return ("native", 76, major, minor)
        if not sig and major in (1, 2):                # byte-frozen JFR annex
            return ("annex", 68, major, minor)
        return None                                    # foreign: bridge

    def accept_candidate_metadata(self, mcur):
        """valid iff pointer resolves to a [size][typeId=0] event within the chunk."""
        if mcur.pos < 0 or mcur.pos + 6 > mcur.end: return False
        try:
            probe = Cursor(self.d, mcur.pos, mcur.end)
            size = probe.varint()
            tid = probe.varint()
            return tid == 0 and size > 0
        except IOError:
            return False

    def run(self, data):
        self.d = data
        self.chunk_models = []
        pos = 0; nForeign = 0
        while pos < len(data):
            c = self.accept_candidate(pos)
            if c is None:
                nForeign += 1
                nm = min(x for x in (data.find(MAGIC_ANNEX, pos + 1),
                                     data.find(MAGIC_NATIVE, pos + 1)) if x >= 0) \
                     if any(x >= 0 for x in (data.find(MAGIC_ANNEX, pos + 1),
                                             data.find(MAGIC_NATIVE, pos + 1))) else -1
                if nm < 0:
                    self.foreign_spans.append((pos, len(data))); break
                self.foreign_spans.append((pos, nm)); pos = nm; continue
            kind, hdr_size, major, minor = c
            cur = Cursor(data, pos)
            size = size_field = struct.unpack_from(">q", data, pos + 8)[0]
            if size == 0:                       # streaming chunk: extent = walk to END_OF_PARTITION
                size, _complete = chunk_extent(data, pos)
            meta_off = struct.unpack_from(">q", data, pos + 24)[0]
            flags = struct.unpack_from(">H", data, pos + 66)[0]
            start_nanos = struct.unpack_from(">q", data, pos + 32)[0]
            dur_nanos = struct.unpack_from(">q", data, pos + 40)[0]
            start_ticks = struct.unpack_from(">q", data, pos + 48)[0]
            tps = struct.unpack_from(">q", data, pos + 56)[0]
            print(f"chunk @{pos}: class {kind} major {major}.{minor} size {size} "
                  f"meta@{meta_off} nanos {start_nanos}..{start_nanos+dur_nanos} ticks@{start_ticks} freq {tps}")
            if kind == "annex" or kind == "native":
                # metadata at header pointer
                mcur = Cursor(data, pos + meta_off, pos + size)
                if self.accept_candidate_metadata(mcur):
                    mcur.varint()                # size (payload event size)
                    mcur.varint()                # typeId == 0
                    ts_m = mcur.varint()         # timestamp
                    dur_m = mcur.varint()        # duration
                    mid_m = mcur.varint()        # metadataId (cross-chunk dedup id)
                    enc_m = mcur.varint() if kind == "native" else 0   # envelope encoding tag (0 = xml-v2)
                    pool, root = read_metadata_payload(mcur)
                    self.types = build_types(pool, root)
                    nty = sum(1 for t in self.types.values() if t.super)
                    print(f"  metadata: {nty} event types of {len(self.types)} declared, pool {len(pool)} strings")
                    if self.record:
                        self.meta_preamble = (ts_m, dur_m, mid_m)
                        self.meta_pool = pool
                        self.meta_root = root
                        self.meta_encoding = enc_m
                # event walk (pool + metadata + user events), strict accounting
                self.walk_events(Cursor(data, pos + hdr_size), pos, hdr_size, pos + size,
                                 kind == "native")
                self.chunks += 1
                if self.record:
                    meta_model = None
                    if self.meta_pool is not None:
                        meta_model = {"startTime": self.meta_preamble[0], "duration": self.meta_preamble[1],
                                      "metadataId": self.meta_preamble[2],
                                      "encodingId": 0,
                                      "stringPool": self.meta_pool,
                                      "root": self.meta_root}
                    wid = struct.unpack_from(">Q", data, pos + 68)[0] if hdr_size == 76 else None
                    self.chunk_models.append(
                        {"index": len(self.chunk_models), "class": kind.upper() if kind != "native" else "NATIVE",
                         "major": major, "minor": minor,
                         "header": {"magic": data[pos:pos+4], "major": major, "minor": minor,
                                    "chunkSize": size_field, "stateSnapshotOffset": meta_off and struct.unpack_from(">q", data, pos + 16)[0] or 0,
                                    "metadataOffset": meta_off, "startNanos": start_nanos,
                                    "durationNanos": dur_nanos, "startTicks": start_ticks,
                                    "ticksPerSecond": tps, "state": struct.unpack_from(">B", data, pos + 64)[0],
                                    "flags": flags, "writerId": wid},
                         "major_minor": (major, minor),
                         "metadata": meta_model,
                         "stateSnapshots": self.state_snapshot_models,
                         "events": self.records,
                         "foreignSpans": [{"firstOffset": a, "nextOffset": b} for (a, b) in self.foreign_spans]})
            pos += size if size > 0 else len(data)
        print(f"chunks parsed: {self.chunks}; foreign spans: {len(self.foreign_spans)}")
        print("--- event counts (type: count) ---")
        for k, v in sorted(self.stats.items(), key=lambda kv: -kv[1]):
            base = k.split(".")[-1]
            print(f"  {k}: {v}")
        return self

def self_test():
    """Native-draft parsing, foreign bridging, and the type-summary flag."""
    import io
    buf = bytearray()
    def emit_native():
        start = len(buf)
        flags = (0x80) << 8 | (0)              # @66: signature bit + encoding default 0
        hdr = bytearray(76)
        hdr[0:4] = MAGIC_NATIVE; struct.pack_into(">HH", hdr, 4, 0, 1)   # native draft = distinct magic + signature
        struct.pack_into(">q", hdr, 8, 0)      # size back-patched
        struct.pack_into(">q", hdr, 32, 1720000000000000000)
        struct.pack_into(">q", hdr, 40, 100000000)
        struct.pack_into(">q", hdr, 48, 400)
        struct.pack_into(">q", hdr, 56, 4000000000)
        hdr[64] = 0
        struct.pack_into(">H", hdr, 66, flags)
        struct.pack_into(">Q", hdr, 68, 0xA5A5BEEF12345678)   # writer id (u64)
        buf.extend(hdr)
        return start
    def emit_event(tid, payload):
        body = bytes([tid]) + payload
        size_prefix = len(body) + 1            # includes own varint width
        pre = bytearray(); v = size_prefix; pre.append(v & 0x7F)
        while v > 0x7F: v >>= 7; pre.append(0x80 | (v & 0x7F))
        buf.extend(bytes(pre) + body)
    emit_native()
    # one event: typeId=2 (dyn), dur absent, int+string-pool fields
    def leb(v):
        out = bytearray([v & 0x7F]); v >>= 7
        while v: out.append(0x80 | (v & 0x7F)); v >>= 7
        return bytes(out)
    payload = leb(420) + leb(1) + b"x"     # [int 420][tag3 len1 "x"]
    emit_event(2, payload)
    struct.pack_into(">q", buf, 8, len(buf))
    # foreign chunk: JFR-major-3 shaped header w/ unknown layout
    fstart = len(buf)
    fh = bytearray(20); fh[0:4] = MAGIC_ANNEX; struct.pack_into(">HH", fh, 4, 3, 0)  # "future JFR 3" = foreign
    struct.pack_into(">q", fh, 8, 20)
    buf.extend(fh)
    # another native chunk
    s2 = emit_native()
    # Native type-1 state snapshot with SUMMARY_PRESENT: cumulative counts for ids 2 and 9,
    # no pools. The header's offset-16 pointer is chunk-relative.
    snap = len(buf)
    emit_event(1, leb(0) + leb(0) + leb(0) + bytes([0x80]) + leb(1)
               + leb(2) + leb(2) + leb(3) + leb(9) + leb(1) + leb(0))
    struct.pack_into(">q", buf, s2 + 16, snap - s2)
    struct.pack_into(">q", buf, s2 + 8, len(buf) - s2)
    r = Reader(); r.run(bytes(buf))
    ok = r.stats.get(2, 0) == 0  # placeholder — real assertions in annex run
    print(f"selftest native chunks: {r.chunks}, foreign: {len(r.foreign_spans)}")
    return (r.chunks == 2 and len(r.foreign_spans) == 1
            and r.stats["<state-snapshot>"] == 1)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        ok = self_test(); sys.exit(0 if ok else 1)
    path = sys.argv[1]
    data = open(path, "rb").read()
    t0 = time.time()
    r = Reader(); r.run(data)
    print(f"parsed {len(data)} bytes in {time.time()-t0:.2f}s; total events: {sum(r.stats.values())}")
