#!/usr/bin/env python3
"""virel_container.py — reference implementation of the VIRC container (spec §3, draft 0.1).

VIREL has two file kinds (spec §3.1): a VIRL file is one native event stream (§3.9; written
directly by a native producer, concatenable, streamable); a VIRC file is this container of
framed streams of any format. sniff() tells them apart from the leading bytes.

Format-agnostic: the container never parses payload bytes. Splitters below only find
record boundaries (spec §3.4.3, informative) and never change the bytes.

  ContainerWriter  : header, STREAM/DATA records (closed or live OPEN), MANIFEST, END
  ContainerReader  : validation, record walk, recovery scan, streams, byte-exact extraction,
                     manifest cross-check, PREAMBLE/RESYNC seeking
  split_*          : per-format record splitters used by the collector
CLI:
  virel_container.py collect OUT FORMAT:PATH [FORMAT:PATH ...]   (FORMAT = format id)
  virel_container.py ls FILE
  virel_container.py extract FILE STREAM_ID OUT
"""
import os, struct, sys

SIGNATURE = b"VIRC\r\n\x1a\n"          # VIRC container (spec §3.2)
VIRL_MAGIC = b"VIRL"                    # native event stream / chunk magic (spec §3.9)
CONTAINER_HEADER = 32
REC_MAGIC = b"VREC"
REC_HEADER = 56
END_RECORD = REC_HEADER + 8
KIND_PAD, KIND_STREAM, KIND_DATA, KIND_MANIFEST, KIND_END = 0, 1, 2, 3, 4
KIND_NAMES = {0: "PAD", 1: "STREAM", 2: "DATA", 3: "MANIFEST", 4: "END"}
F_STREAM_END, F_PAYLOAD_CRC, F_PREAMBLE, F_RESYNC = 1, 2, 4, 8
STATE_OPEN, STATE_CLOSED = 0xFF, 0x00
OPEN_LENGTH = (1 << 64) - 1
REPEATABLE_KEYS = {"clock-anchor", "derived-from"}

# ---------------- CRC-32C (Castagnoli, reflected 0x82F63B78) ----------------
_CRC_TABLE = []
for _i in range(256):
    _c = _i
    for _ in range(8):
        _c = (_c >> 1) ^ 0x82F63B78 if _c & 1 else _c >> 1
    _CRC_TABLE.append(_c)

def crc32c(data, crc=0):
    crc ^= 0xFFFFFFFF
    t = _CRC_TABLE
    for b in data:
        crc = t[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF

# ---------------- VIREL Varint128 (spec §2: <= 9 bytes, 9th byte raw) ----------------
def enc_varint(v):
    if v < 0:
        raise ValueError("varint must be unsigned")
    out = bytearray()
    for _ in range(8):
        if v < 0x80:
            out.append(v)
            return bytes(out)
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    if v > 0xFF:
        raise ValueError("value exceeds 64 bits")
    out.append(v)
    return bytes(out)

def dec_varint(d, p):
    v = 0
    for i in range(8):
        b = d[p]; p += 1
        v |= (b & 0x7F) << (7 * i)
        if b < 0x80:
            return v, p
    return v | (d[p] << 56), p + 1

def enc_bytes(b):
    return enc_varint(len(b)) + b

def dec_bytes(d, p):
    n, p = dec_varint(d, p)
    if p + n > len(d):
        raise ValueError("length past end")
    return bytes(d[p:p + n]), p + n

# ---------------- descriptors (spec §3.4) ----------------
def encode_descriptor(entries):
    """entries: list of (key, value-bytes-or-str)"""
    out = bytearray(enc_varint(len(entries)))
    for k, v in entries:
        if isinstance(v, str):
            v = v.encode()
        out += enc_bytes(k.encode()) + enc_bytes(bytes(v))
    return bytes(out)

def decode_descriptor(d):
    n, p = dec_varint(d, 0)
    out = []
    for _ in range(n):
        k, p = dec_bytes(d, p)
        v, p = dec_bytes(d, p)
        out.append((k.decode(), v))
    if p != len(d):
        raise ValueError("trailing bytes in descriptor")
    return out

def encode_anchor(clock, ticks, frequency, unix_nanos, uncertainty_nanos):
    return enc_bytes(clock.encode()) + enc_varint(ticks) + enc_varint(frequency) + \
        enc_varint(unix_nanos) + enc_varint(uncertainty_nanos)

def decode_anchor(v):
    clock, p = dec_bytes(v, 0)
    ticks, p = dec_varint(v, p)
    freq, p = dec_varint(v, p)
    unix, p = dec_varint(v, p)
    unc, p = dec_varint(v, p)
    return {"clock": clock.decode(), "ticks": ticks, "frequency": freq,
            "unixNanos": unix, "uncertaintyNanos": unc}

# ---------------- record header ----------------
def pack_static(kind, stream_id, sequence):
    s = struct.pack(">4sHHII", REC_MAGIC, kind, REC_HEADER, stream_id, sequence)
    return s + struct.pack(">I", crc32c(s))

def pack_dynamic(state, flags, length, t0, t1, payload_crc):
    d = struct.pack(">BBHQQQI", state, flags, 0, length, t0, t1, payload_crc)
    return d + struct.pack(">I", crc32c(struct.pack(">BBHQQQI", STATE_CLOSED, flags, 0, length, t0, t1, payload_crc)))

# ---------------- writer ----------------
class ContainerWriter:
    def __init__(self, f, container_id=0):
        self.f = f
        self.streams = {}          # sid -> dict(keys, records, bytes, t0, t1, ended, first_stream_record)
        self.index = []            # (offset, sid, seq, t0, t1, flags)
        self.open = None           # live record state
        self.next_sid = 1
        h = struct.pack(">8sHHHHQI", SIGNATURE, 0, 0, CONTAINER_HEADER, 0, container_id, 0)
        f.write(h + struct.pack(">I", crc32c(h)))
        self.pos = CONTAINER_HEADER

    def _emit(self, kind, sid, seq, payload, flags=0, t0=0, t1=0):
        if self.open:
            raise RuntimeError("a record is OPEN; close it first (spec §3.6)")
        off = self.pos
        hdr = pack_static(kind, sid, seq) + pack_dynamic(STATE_CLOSED, flags | F_PAYLOAD_CRC, len(payload), t0, t1, crc32c(payload))
        self.f.write(hdr + payload)
        self.pos += len(hdr) + len(payload)
        return off

    def add_stream(self, fmt, entries=()):
        sid = self.next_sid; self.next_sid += 1
        keys = [("format", fmt)] + list(entries)
        off = self._emit(KIND_STREAM, sid, 0, encode_descriptor(keys))
        framing = dict((k, v) for k, v in keys).get("record-framing", "stream")
        self.streams[sid] = {"records": 0, "bytes": 0, "t0": 0, "t1": 0, "ended": False,
                             "first": off, "framing": framing}
        return sid

    def update_stream(self, sid, entries):
        self._emit(KIND_STREAM, sid, 0, encode_descriptor(list(entries)))

    def _account(self, sid, off, n, flags, t0, t1, end):
        st = self.streams[sid]
        if st["ended"]:
            raise RuntimeError(f"stream {sid} already ended")
        seq = st["records"]
        st["records"] += 1; st["bytes"] += n
        if t0 and (not st["t0"] or t0 < st["t0"]): st["t0"] = t0
        if t1 and t1 > st["t1"]: st["t1"] = t1
        st["ended"] = end
        self.index.append((off, sid, seq, t0, t1, flags | F_PAYLOAD_CRC | (F_STREAM_END if end else 0)))

    def write(self, sid, payload, flags=0, t0=0, t1=0, end=False):
        seq = self.streams[sid]["records"]
        off = self._emit(KIND_DATA, sid, seq, payload, flags | (F_STREAM_END if end else 0), t0, t1)
        self._account(sid, off, len(payload), flags, t0, t1, end)
        return off

    # live records (spec §3.6)
    def open_record(self, sid, flags=0, t0=0):
        if self.open:
            raise RuntimeError("only one record may be OPEN")
        seq = self.streams[sid]["records"]
        off = self.pos
        hdr = pack_static(KIND_DATA, sid, seq) + struct.pack(">BBHQQQI", STATE_OPEN, 0, 0, OPEN_LENGTH, t0, 0, 0) + b"\0\0\0\0"
        self.f.write(hdr)
        self.pos += len(hdr)
        self.open = {"sid": sid, "off": off, "flags": flags, "t0": t0, "n": 0, "crc": 0}
        return off

    def append(self, data):
        self.f.write(data)
        self.pos += len(data)
        self.open["n"] += len(data)
        self.open["crc"] = crc32c(data, self.open["crc"])
        self.f.flush()

    def close_record(self, t1=0, end=False):
        o = self.open
        flags = o["flags"] | F_PAYLOAD_CRC | (F_STREAM_END if end else 0)
        dyn = pack_dynamic(STATE_CLOSED, flags, o["n"], o["t0"], t1, o["crc"])
        self.f.flush()
        here = self.f.tell()
        self.f.seek(o["off"] + 21)
        self.f.write(dyn[1:])              # every dynamic field and its CRC first ...
        self.f.flush()
        self.f.seek(o["off"] + 20)
        self.f.write(dyn[:1])              # ... then state = CLOSED last
        self.f.flush()
        self.f.seek(here)
        self.open = None
        self._account(o["sid"], o["off"], o["n"], o["flags"], o["t0"], t1, end)

    def close(self, manifest=True, index=True):
        for sid, st in self.streams.items():
            if not st["ended"]:
                self.write(sid, b"", end=True)
        mo = 0
        if manifest:
            m = bytearray(enc_varint(1) + enc_varint(len(self.streams)))
            for sid, st in sorted(self.streams.items()):
                m += b"".join(enc_varint(x) for x in (sid, st["first"], st["records"], st["bytes"],
                                                       st["t0"], st["t1"], 1 if st["ended"] else 0))
            rows = self.index if index else []
            m += enc_varint(len(rows))
            for row in rows:
                m += b"".join(enc_varint(x) for x in row)
            mo = self._emit(KIND_MANIFEST, 0, 0, bytes(m))
        self._emit(KIND_END, 0, 0, struct.pack(">Q", mo))
        self.f.flush()

# ---------------- reader ----------------
class Record:
    __slots__ = ("offset", "kind", "header_size", "stream_id", "sequence", "state", "flags",
                 "length", "t0", "t1", "payload_crc", "payload_start", "payload_end",
                 "truncated", "crc_ok")

    def payload(self, data):
        return bytes(data[self.payload_start:self.payload_end])

class Stream:
    def __init__(self, sid):
        self.sid = sid
        self.keys = []             # merged (key, value) in arrival order
        self.data = []             # DATA records in file order
        self.errors = []

    def get(self, key, default=None):
        for k, v in self.keys:
            if k == key:
                return v.decode()
        return default

    @property
    def fmt(self): return self.get("format")

    @property
    def framing(self): return self.get("record-framing", "stream")

    def records_by_seq(self):
        return sorted(self.data, key=lambda r: r.sequence)

    def gaps(self):
        seqs = sorted(r.sequence for r in self.data)
        have = set(seqs)
        top = seqs[-1] if seqs else -1
        return [s for s in range(top + 1) if s not in have]

    def complete(self):
        recs = self.records_by_seq()
        return (bool(recs) and not self.gaps() and recs[-1].flags & F_STREAM_END != 0
                and all(not r.truncated and r.crc_ok for r in recs)
                and len({r.sequence for r in recs}) == len(recs))

class ContainerReader:
    def __init__(self, data):
        self.data = data
        self.records = []
        self.damaged = []          # (first, next) byte spans skipped by recovery
        self.errors = []
        self.streams = {}
        self.manifest = None
        self.clean = False
        self._header()
        self._walk()
        self._streams()
        self._manifest()

    def _header(self):
        d = self.data
        if len(d) < CONTAINER_HEADER or d[:8] != SIGNATURE:
            raise ValueError("not a VIREL container (signature)")
        _, major, minor, hsize, flags, cid, _ = struct.unpack_from(">8sHHHHQI", d, 0)
        if major != 0:
            raise ValueError(f"unsupported container major {major}")
        if hsize < CONTAINER_HEADER or len(d) < hsize:
            raise ValueError("bad container header size")
        if struct.unpack_from(">I", d, hsize - 4)[0] != crc32c(d[:hsize - 4]):
            raise ValueError("container header CRC mismatch")
        self.major, self.minor, self.header_size, self.flags, self.container_id = major, minor, hsize, flags, cid

    def _record_at(self, p):
        """validate a record header at p; return Record or None"""
        d = self.data
        if p + 20 > len(d) or d[p:p + 4] != REC_MAGIC:
            return None
        if struct.unpack_from(">I", d, p + 16)[0] != crc32c(d[p:p + 16]):
            return None
        if p + REC_HEADER > len(d):
            return None
        r = Record()
        _, r.kind, r.header_size, r.stream_id, r.sequence = struct.unpack_from(">4sHHII", d, p)
        r.state, r.flags, _, r.length, r.t0, r.t1, r.payload_crc = struct.unpack_from(">BBHQQQI", d, p + 20)
        if r.header_size < REC_HEADER:
            return None
        r.offset = p
        r.payload_start = p + r.header_size
        if r.payload_start > len(d):
            return None
        if r.state == STATE_CLOSED:
            dyn = struct.pack(">BBHQQQI", STATE_CLOSED, r.flags, 0, r.length, r.t0, r.t1, r.payload_crc)
            if struct.unpack_from(">I", d, p + 52)[0] != crc32c(dyn):
                return None                # torn read or damage
            r.payload_end = min(len(d), r.payload_start + r.length)
            r.truncated = r.payload_start + r.length > len(d)
        elif r.state == STATE_OPEN:
            r.payload_end = len(d)        # OPEN: payload runs to end of file (spec §3.6)
            r.truncated = True             # not final until CLOSED
        else:
            return None
        r.crc_ok = True
        if r.state == STATE_CLOSED and not r.truncated and r.flags & F_PAYLOAD_CRC:
            r.crc_ok = crc32c(d[r.payload_start:r.payload_end]) == r.payload_crc
            if not r.crc_ok:
                self.errors.append(f"payload CRC mismatch: record @{p} stream {r.stream_id} seq {r.sequence}")
        return r

    def _walk(self):
        d = self.data
        p = self.header_size
        while p < len(d):
            r = self._record_at(p)
            if r is None:
                q = d.find(REC_MAGIC, p + 1)
                while q != -1 and self._record_at(q) is None:
                    q = d.find(REC_MAGIC, q + 1)
                nxt = q if q != -1 else len(d)
                self.damaged.append((p, nxt))
                p = nxt
                continue
            self.records.append(r)
            if r.kind == KIND_END and r.state == STATE_CLOSED and not r.truncated and r.payload_end == len(d):
                self.clean = True
            p = r.payload_end

    def _streams(self):
        for r in self.records:
            if r.kind == KIND_STREAM:
                st = self.streams.setdefault(r.stream_id, Stream(r.stream_id))
                if r.truncated or not r.crc_ok:
                    st.errors.append(f"damaged STREAM record @{r.offset}")
                    continue
                try:
                    entries = decode_descriptor(r.payload(self.data))
                except Exception as e:
                    st.errors.append(f"bad descriptor @{r.offset}: {e}")
                    continue
                first = not st.keys
                for k, v in entries:
                    if k == "record-framing" and not first:
                        st.errors.append("record-framing outside the first STREAM record")
                    prior = [pv for (pk, pv) in st.keys if pk == k]
                    if prior and k not in REPEATABLE_KEYS:
                        if prior[0] != v:
                            st.errors.append(f"add-only violation: key {k} changed")
                        continue
                    st.keys.append((k, v))
            elif r.kind == KIND_DATA:
                st = self.streams.setdefault(r.stream_id, Stream(r.stream_id))
                if not st.keys:
                    st.errors.append(f"DATA before STREAM (seq {r.sequence})")
                st.data.append(r)

    def _manifest(self):
        ends = [r for r in self.records if r.kind == KIND_END and not r.truncated]
        if not ends:
            return
        mo = struct.unpack_from(">Q", self.data, ends[-1].payload_start)[0]
        if mo == 0:
            return
        mrec = self._record_at(mo) if mo < len(self.data) else None
        if mrec is None or mrec.kind != KIND_MANIFEST or mrec.truncated or not mrec.crc_ok:
            self.errors.append(f"END points at an invalid MANIFEST @{mo}")
            return
        d = mrec.payload(self.data)
        ver, p = dec_varint(d, 0)
        n, p = dec_varint(d, p)
        streams = []
        for _ in range(n):
            row = []
            for _ in range(7):
                v, p = dec_varint(d, p); row.append(v)
            streams.append(dict(zip(("streamId", "firstStreamRecord", "dataRecords", "payloadBytes",
                                     "timeStartNanos", "timeEndNanos", "ended"), row)))
        n, p = dec_varint(d, p)
        index = []
        for _ in range(n):
            row = []
            for _ in range(6):
                v, p = dec_varint(d, p); row.append(v)
            index.append(dict(zip(("recordOffset", "streamId", "sequence", "timeStartNanos",
                                   "timeEndNanos", "flags"), row)))
        self.manifest = {"version": ver, "streams": streams, "index": index, "offset": mo}

    # ---- queries ----
    def extract(self, sid):
        """stream framing -> bytes; message framing -> list of bytes (spec §3.1)"""
        st = self.streams[sid]
        parts = [r.payload(self.data) for r in st.records_by_seq()]
        return parts if st.framing == "message" else b"".join(parts)

    def resync(self, sid, sequence):
        """bytes a decoder needs to start at RESYNC record `sequence`: PREAMBLE records + that record onward"""
        st = self.streams[sid]
        recs = st.records_by_seq()
        start = next(r for r in recs if r.sequence == sequence)
        if not start.flags & F_RESYNC:
            raise ValueError(f"record {sequence} is not a RESYNC point")
        pre = [r for r in recs if r.flags & F_PREAMBLE]
        tail = [r for r in recs if r.sequence >= sequence]
        return b"".join(r.payload(self.data) for r in pre + tail)

    def verify_manifest(self):
        """manifest vs records; returns a list of disagreements (records win)"""
        if self.manifest is None:
            return []
        bad = []
        for m in self.manifest["streams"]:
            st = self.streams.get(m["streamId"])
            if st is None:
                bad.append(f"manifest stream {m['streamId']} has no records"); continue
            recs = st.data
            first = min((r.offset for r in self.records if r.kind == KIND_STREAM and r.stream_id == st.sid), default=None)
            actual = (first, len(recs), sum(r.payload_end - r.payload_start for r in recs),
                      1 if any(r.flags & F_STREAM_END for r in recs) else 0)
            want = (m["firstStreamRecord"], m["dataRecords"], m["payloadBytes"], m["ended"])
            if actual != want:
                bad.append(f"stream {st.sid}: records {actual} != manifest {want}")
        by_off = {r.offset: r for r in self.records}
        for row in self.manifest["index"]:
            r = by_off.get(row["recordOffset"])
            if r is None or (r.stream_id, r.sequence, r.t0, r.t1, r.flags) != \
                    (row["streamId"], row["sequence"], row["timeStartNanos"], row["timeEndNanos"], row["flags"]):
                bad.append(f"index row @{row['recordOffset']} disagrees with its record")
        return bad

# ---------------- splitters (spec §3.4.3; boundaries only, bytes unchanged) ----------------
def _leb(d, p):
    """plain LEB128 (protobuf / Go uvarint, up to 10 bytes)"""
    v = sh = 0
    while True:
        b = d[p]; p += 1
        v |= (b & 0x7F) << sh
        if b < 0x80:
            return v, p
        sh += 7

def split_chunks(data):
    """virel / jfr: one chunk per record; RESYNC on every chunk; time range from the chunk header.
    Streaming VIRL chunks (size 0) end at their END_OF_PARTITION event (spec §3.9.6)."""
    from tcf_reader import chunk_extent
    out, p = [], 0
    while p < len(data):
        size, complete = chunk_extent(data, p)
        if not complete or size < 68:
            out.append((data[p:], 0, 0, 0))   # not splittable further: rest as one record
            break
        start, dur = struct.unpack_from(">QQ", data, p + 32)
        out.append((data[p:p + size], F_RESYNC, start, start + dur if dur else 0))
        p += size
    return out

def sniff(data):
    """file kind from the leading bytes (spec §3.8): 'virc', 'virl', or a convenience format"""
    if data[:8] == SIGNATURE:
        return "virc"
    if data[:4] == VIRL_MAGIC:
        return "virl"
    if data[:4] == b"FLR\0":
        return "jfr"
    if data[:5] == b"go 1.":
        return "go-trace"
    return None

def split_go_trace(data):
    """16-byte header (PREAMBLE), then one record per generation (RESYNC). Format 1.26 ends a
    generation with EvEndOfGeneration (0x34); earlier formats change the batch `gen`."""
    if not data.startswith(b"go 1.") or len(data) < 16:
        return [(data, 0, 0, 0)]
    version = int(data[5:7])
    out = [(data[:16], F_PREAMBLE, 0, 0)]
    p, start, cur_gen = 16, 16, None
    try:
        while p < len(data):
            b = data[p]
            if b == 0x34 and version >= 26:
                p += 1
                out.append((data[start:p], F_RESYNC, 0, 0)); start = p; cur_gen = None
                continue
            if b not in (0x01, 0x31):
                raise ValueError(f"unknown batch byte {b:#x} @{p}")
            q = p + (2 if b == 0x31 else 1)
            gen, q = _leb(data, q)
            _, q = _leb(data, q)       # M
            _, q = _leb(data, q)       # ts
            size, q = _leb(data, q)
            if version < 26 and cur_gen is not None and gen != cur_gen and p > start:
                out.append((data[start:p], F_RESYNC, 0, 0)); start = p
            cur_gen = gen
            p = q + size
            if p > len(data):
                raise ValueError("batch past end")
    except (ValueError, IndexError):
        pass                           # cut or unknown: the rest becomes one non-resync record
    if start < len(data):
        out.append((data[start:], F_RESYNC if version < 26 and start > 16 else 0, 0, 0))
    return out

def split_perfetto(data, max_bytes=1 << 20):
    """runs of whole TracePackets; a record ends after a traced write pass
    (TracingServiceEvent.read_tracing_buffers_completed, packet field 69 / field 4) or at max_bytes.
    RESYNC on the first record and on records that start after a write pass."""
    out, p, start, resync_next = [], 0, 0, True
    def packet_ends_pass(pkt):
        q = 0
        while q < len(pkt):
            key, q = _leb(pkt, q)
            fno, wt = key >> 3, key & 7
            if wt == 0: _, q = _leb(pkt, q)
            elif wt == 1: q += 8
            elif wt == 5: q += 4
            elif wt == 2:
                n, q = _leb(pkt, q)
                if fno == 69:
                    sub, r = pkt[q:q + n], 0
                    while r < len(sub):
                        k2, r = _leb(sub, r)
                        if k2 >> 3 == 4 and k2 & 7 == 0:
                            v, r = _leb(sub, r)
                            if v: return True
                        elif k2 & 7 == 0: _, r = _leb(sub, r)
                        elif k2 & 7 == 2: n2, r = _leb(sub, r); r += n2
                        elif k2 & 7 == 1: r += 8
                        elif k2 & 7 == 5: r += 4
                        else: return False
                q += n
            else:
                return False
        return False
    try:
        while p < len(data):
            key, q = _leb(data, p)
            if key & 7 != 2:
                raise ValueError("unexpected top-level wire type")
            n, q = _leb(data, q)
            if q + n > len(data):
                raise ValueError("packet past end")
            ends = (key >> 3) == 1 and packet_ends_pass(data[q:q + n])
            p = q + n
            if ends or p - start >= max_bytes:
                out.append((data[start:p], F_RESYNC if resync_next else 0, 0, 0))
                start, resync_next = p, ends
    except (ValueError, IndexError):
        pass
    if start < len(data):
        out.append((data[start:], F_RESYNC if resync_next else 0, 0, 0))
    return out

def split_whole(data):
    return [(data, 0, 0, 0)]

SPLITTERS = {"virel": split_chunks, "jfr": split_chunks, "go-trace": split_go_trace,
             "perfetto": split_perfetto}

# ---------------- collector ----------------
def collect(out_path, inputs, interleave=True):
    """inputs: list of (format, bytes-or-list-of-documents, extra descriptor entries)"""
    plans = []
    with open(out_path, "wb") as f:
        w = ContainerWriter(f, container_id=int.from_bytes(os.urandom(8), "big") or 1)
        for fmt, payload, extra in inputs:
            if isinstance(payload, list):              # message framing: one document per record
                sid = w.add_stream(fmt, [("record-framing", "message")] + list(extra))
                parts = [(doc, F_RESYNC, 0, 0) for doc in payload]
            else:
                sid = w.add_stream(fmt, list(extra))
                parts = SPLITTERS.get(fmt, split_whole)(payload)
            plans.append([sid, parts, 0])
        # round-robin interleaving exercises multi-stream files (spec §3.1)
        while any(i < len(parts) for _, parts, i in plans):
            for plan in plans:
                sid, parts, i = plan
                if i < len(parts):
                    chunk, flags, t0, t1 = parts[i]
                    w.write(sid, chunk, flags, t0, t1, end=(i == len(parts) - 1))
                    plan[2] += 1
                if not interleave:
                    break
        w.close()

def _main(argv):
    if len(argv) >= 3 and argv[0] == "collect":
        inputs = []
        for spec in argv[2:]:
            fmt, path = spec.split(":", 1)
            inputs.append((fmt, open(path, "rb").read(), [("source", os.path.abspath(path))]))
        collect(argv[1], inputs)
        return 0
    if len(argv) == 2 and argv[0] == "ls":
        r = ContainerReader(open(argv[1], "rb").read())
        print(f"container id {r.container_id:#x} clean={r.clean} records={len(r.records)} damaged={r.damaged}")
        for sid, st in sorted(r.streams.items()):
            n = sum(x.payload_end - x.payload_start for x in st.data)
            print(f"  stream {sid}: format={st.fmt} framing={st.framing} records={len(st.data)} bytes={n} "
                  f"complete={st.complete()} gaps={st.gaps()} errors={st.errors}")
        for e in r.errors + r.verify_manifest():
            print("  !", e)
        return 0
    if len(argv) == 4 and argv[0] == "extract":
        r = ContainerReader(open(argv[1], "rb").read())
        out = r.extract(int(argv[2]))
        if isinstance(out, list):
            os.makedirs(argv[3], exist_ok=True)
            for i, doc in enumerate(out):
                open(os.path.join(argv[3], f"{i:06d}.bin"), "wb").write(doc)
        else:
            open(argv[3], "wb").write(out)
        return 0
    print(__doc__)
    return 2

if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))
