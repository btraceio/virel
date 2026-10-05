#!/usr/bin/env python3
"""container_gate.py — GATE 6: the format-agnostic VIREL container (spec §3) on real inputs.

Inputs (built fresh in a work directory; a missing toolchain skips its input and says so):
  jfr       real multi-chunk JDK Flight Recorder file (jdk.jfr API, forced chunk rotation)
  go-trace  real Go execution trace (go run, runtime/trace)
  virel     native VIREL from the C++ writer prototype, incl. signal-handler stack samples
  perfetto  perfetto_gate's synthetic trace plus a traced write-pass packet (resync point)
  otlp-traces  three ExportTraceServiceRequest messages (record-framing: message)
  ctef      a small trace-event JSON document (whole-file record)

Checks:
  1 pack   collector interleaves all streams; reader: clean, every stream complete,
           byte-exact (stream framing) / document-exact (message framing), manifest agrees
  2 decode extracted jfr/virel parse with tcf_reader; extracted go trace parses with go tool
  3 resync go: PREAMBLE + any generation onward parses (dropping a middle generation is
           reported only: Go's parser detects it in some traces and not in others);
           jfr: the JDK reads any chunk onward
  4 recover truncation at many offsets never crashes, prefixes stay byte-exact, gaps reported;
           a corrupted record header becomes a damaged span + one gap; a corrupted payload
           byte fails its payload CRC
  5 live   OPEN record visible to a reader with payload to EOF; after close it is CLOSED
  6 model  the packed container maps onto doc/tcf-model.avsc and lints clean
  7 kaitai doc/tcf-framing.ksy compiles (KSC=path or kaitai-struct-compiler on PATH, plus the
           kaitaistruct runtime) and its parser agrees with the reference reader; skipped otherwise
"""
import hashlib, io, json, os, shutil, struct, subprocess, sys, tempfile, contextlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import virel_container as vc
from tcf_reader import Reader

KEEP_WORK = bool(os.environ.get("VIREL_GATE_WORK"))     # set it to keep and reuse inputs
WORK = os.environ.get("VIREL_GATE_WORK") or tempfile.mkdtemp(prefix="virel-container-")

JAVA_SRC = """
import jdk.jfr.*;
import java.nio.file.Path;
public class Rot {
    public static void main(String[] a) throws Exception {
        Recording main = new Recording(Configuration.getConfiguration("profile"));
        main.setDestination(Path.of(a[0]));
        main.start();
        long s = 0;
        for (int round = 0; round < 4; round++) {
            long end = System.currentTimeMillis() + 500;
            java.util.List<byte[]> keep = new java.util.ArrayList<>();
            while (System.currentTimeMillis() < end) {
                for (int i = 0; i < 1000; i++) { s += i; keep.add(new byte[128]); }
                if (keep.size() > 20000) keep.clear();
            }
            // Starting another recording makes JFR begin a new repository chunk, so the
            // dump of `main` holds several chunks (size-based rotation is not deterministic).
            try (Recording rot = new Recording()) { rot.start(); rot.stop(); }
        }
        main.stop();
        System.out.println(s);
    }
}
"""
GO_SRC = """package main
import ("os"; "runtime/trace"; "sync"; "time")
func main() {
    f, _ := os.Create(os.Args[1]); trace.Start(f)
    var wg sync.WaitGroup
    for g := 0; g < 4; g++ { wg.Add(1); go func() { defer wg.Done()
        end := time.Now().Add(3500 * time.Millisecond); x := 0
        for time.Now().Before(end) { for i := 0; i < 10000; i++ { x += i }; time.Sleep(time.Millisecond) }
        _ = x }() }
    wg.Wait(); trace.Stop(); f.Close()
}
"""

def quiet(fn, *a):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        return fn(*a)

def build_jfr():
    if not shutil.which("java"):
        return None, "java not found"
    path = os.path.join(WORK, "rec.jfr")
    if not os.path.exists(path):
        src = os.path.join(WORK, "Rot.java")
        open(src, "w").write(JAVA_SRC)
        r = subprocess.run(["java", src, path], capture_output=True, text=True, timeout=120)
        if r.returncode != 0 or not os.path.exists(path):
            return None, "java recording failed: " + r.stderr[-300:]
    return open(path, "rb").read(), None

def build_go():
    if not shutil.which("go"):
        return None, "go not found"
    path = os.path.join(WORK, "go.trace")
    if not os.path.exists(path):
        d = os.path.join(WORK, "gosrc"); os.makedirs(d, exist_ok=True)
        open(os.path.join(d, "main.go"), "w").write(GO_SRC)
        open(os.path.join(d, "go.mod"), "w").write("module gatetrace\n\ngo 1.22\n")
        r = subprocess.run(["go", "run", ".", path], cwd=d, capture_output=True, text=True, timeout=300)
        if r.returncode != 0:
            return None, "go run failed: " + r.stderr[-300:]
    return open(path, "rb").read(), None

def jfr_chunks(data, name):
    """the JDK's own parser: returns the chunk count `jfr summary` reports, or None"""
    if not shutil.which("jfr"):
        return None
    p = os.path.join(WORK, name)
    open(p, "wb").write(data)
    r = subprocess.run(["jfr", "summary", p], capture_output=True, text=True, timeout=120)
    for line in r.stdout.splitlines():
        if line.strip().startswith("Chunks:") and r.returncode == 0:
            return int(line.split(":")[1])
    return None

def go_parses(data, name):
    p = os.path.join(WORK, name)
    open(p, "wb").write(data)
    r = subprocess.run(["go", "tool", "trace", "-d=parsed", p], capture_output=True, text=True, timeout=300)
    return r.returncode == 0, (r.stderr or r.stdout)[-200:]

def build_virel():
    """native VIREL from the C++ writer prototype (benchmarks/generation_writer.cpp), with
    signal-handler stack samples, i.e. the realistic 'VIREL from our profiler' input"""
    cxx = os.environ.get("CXX", "clang++")
    if not shutil.which(cxx):
        return None, f"{cxx} not found"
    out = os.path.join(WORK, "native.virel")
    if not os.path.exists(out):
        src = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "benchmarks", "generation_writer.cpp")
        exe = os.path.join(WORK, "generation_writer")
        r = subprocess.run([cxx, "-std=c++17", "-O2", "-pthread", src, "-o", exe], capture_output=True, text=True, timeout=300)
        if r.returncode != 0:
            return None, "build failed: " + r.stderr[-300:]
        r = subprocess.run([exe, "--mode", "generation", "--events", "20000", "--producers", "4",
                            "--rotate-us", "20000", "--signal-hz", "1000", "--output", out, "--verify"],
                           capture_output=True, text=True, timeout=120)
        if r.returncode != 0:
            return None, "writer failed: " + r.stderr[-300:]
    return open(out, "rb").read(), None

def build_perfetto():
    import perfetto_gate as pg
    data, _ = pg.synth()
    pkts = list(pg.stream_packets(data))
    svc = pg.wf(pg.TP_SEQ) + pg.le_varint(1) + pg.wl(69) + pg.le_varint(2) + pg.wf(4) + pg.le_varint(1)
    half = len(pkts) // 2
    out = bytearray()
    for i, pk in enumerate(pkts):
        out += pg.wl(1) + pg.le_varint(len(pk)) + pk
        if i == half:
            out += pg.wl(1) + pg.le_varint(len(svc)) + svc     # end of a traced write pass
    return bytes(out)

def _leb(v):
    out = bytearray()
    while True:
        b = v & 0x7F; v >>= 7
        out.append(b | (0x80 if v else 0))
        if not v: return bytes(out)

def _tag(f, wt): return _leb((f << 3) | wt)

def otlp_requests():
    def ld(f, b): return _tag(f, 2) + _leb(len(b)) + b
    reqs = []
    for i in range(3):
        t0 = 1_759_600_000_000_000_000 + i * 1_000_000
        span = ld(1, bytes(16)) + ld(2, bytes([i + 1]) * 8) + ld(5, f"span-{i}".encode()) + \
               _tag(7, 1) + struct.pack("<Q", t0) + _tag(8, 1) + struct.pack("<Q", t0 + 500)
        reqs.append(ld(1, ld(2, ld(2, span))))   # ExportTraceServiceRequest.resource_spans.scope_spans.spans
    return reqs

def sha(b): return hashlib.sha256(b).hexdigest()[:16]

def kaitai_check(data, r, by_fmt, check, skipped):
    """compile doc/tcf-framing.ksy with kaitai-struct-compiler (KSC env or PATH) and compare the
    generated parser with the reference reader on the real container and its chunk streams"""
    ksc = os.environ.get("KSC") or shutil.which("kaitai-struct-compiler")
    try:
        import kaitaistruct
    except ImportError:
        ksc = None
    if not ksc:
        skipped.append(("kaitai", "kaitai-struct-compiler (KSC) or the kaitaistruct runtime not available"))
        print("  [SKIP] kaitai: compiler or runtime not available")
        return
    ksy = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "doc", "tcf-framing.ksy")
    out = os.path.join(WORK, "ksy")
    c = subprocess.run([ksc, "-t", "python", "--outdir", out, ksy], capture_output=True, text=True, timeout=300)
    check("kaitai: tcf-framing.ksy compiles", c.returncode == 0, (c.stderr or c.stdout)[-300:])
    if c.returncode != 0:
        return
    sys.path.insert(0, out)
    from tcf_framing import TcfFraming
    from kaitaistruct import KaitaiStream
    k = TcfFraming.from_bytes(data)
    same = len(k.records) == len(r.records)
    for kr, rr in zip(k.records, r.records):
        same &= (int(kr.kind), kr.stream_id, kr.sequence, kr.state, kr.flags, kr.payload_length, kr.time_start,
                 kr.time_end, kr.payload_crc) == (rr.kind, rr.stream_id, rr.sequence, rr.state, rr.flags, rr.length,
                                                  rr.t0, rr.t1, rr.payload_crc)
        if rr.kind == vc.KIND_STREAM:
            same &= [(e.key, bytes(e.value)) for e in kr.payload.entries] == vc.decode_descriptor(rr.payload(data))
    check(f"kaitai: {len(k.records)} record headers and all descriptors equal the reference reader", same)
    km = next(x.payload for x in k.records if int(x.kind) == vc.KIND_MANIFEST)
    rows = [dict(zip(("recordOffset", "streamId", "sequence", "timeStartNanos", "timeEndNanos", "flags"),
                     (e.record_offset.value, e.stream_id.value, e.sequence.value, e.time_start.value,
                      e.time_end.value, e.flags.value))) for e in km.index]
    big = sum(1 for row in rows if row["timeStartNanos"] >= 1 << 56)
    check(f"kaitai: manifest index equals the reference ({len(rows)} rows, {big} 9-byte Varint128 times)",
          rows == r.manifest["index"] and big > 0)
    for fmt in ("virel", "jfr"):
        if fmt in by_fmt:
            st = by_fmt[fmt]
            ks = TcfFraming.ChunkStream(KaitaiStream(io.BytesIO(r.extract(st.sid))))
            sizes = [ch.header.chunk_size for ch in ks.chunks]
            want = [x.payload_end - x.payload_start for x in st.records_by_seq() if x.payload_end > x.payload_start]
            natives = {ch.header.is_native for ch in ks.chunks}
            check(f"kaitai: {fmt} chunk stream parses, {len(sizes)} chunk sizes equal the records, is_native={natives}",
                  sizes == want and natives == {fmt == "virel"})

def main():
    ok = True
    def check(label, cond, detail=""):
        nonlocal ok
        ok &= bool(cond)
        print(f"  [{'OK' if cond else 'FAIL'}] {label}" + (f" -- {detail}" if detail and not cond else ""))
    skipped = []
    inputs = []
    jfr, why = build_jfr()
    if jfr is None: skipped.append(("jfr", why))
    else: inputs.append(("jfr", jfr, [("producer", "OpenJDK JFR")]))
    gotr, why = build_go()
    if gotr is None: skipped.append(("go-trace", why))
    else:
        inputs.append(("go-trace", gotr, [("producer", "go runtime/trace")]))
    native, why = build_virel()
    if native is None: skipped.append(("virel", why))
    else: inputs.append(("virel", native, [("producer", "generation_writer prototype")]))
    inputs.append(("perfetto", build_perfetto(), [("producer", "perfetto_gate synth")]))
    inputs.append(("otlp-traces", otlp_requests(), []))
    inputs.append(("ctef", b'{"traceEvents":[{"name":"x","ph":"i","ts":1,"pid":1,"tid":1}]}', []))
    out = os.path.join(WORK, "packed.virel")
    vc.collect(out, inputs)
    data = open(out, "rb").read()
    print(f"work dir {WORK}; container {len(data)} B, inputs: " +
          ", ".join(f"{f}({sum(map(len, p)) if isinstance(p, list) else len(p)} B)" for f, p, _ in inputs))
    for what, why in skipped:
        print(f"  [SKIP] {what}: {why}")

    print("1 pack")
    r = vc.ContainerReader(data)
    check("clean close (valid END at EOF)", r.clean)
    check("no damaged spans / errors", not r.damaged and not r.errors, f"{r.damaged} {r.errors}")
    check("manifest present and agrees with records", r.manifest is not None and not r.verify_manifest(), r.verify_manifest())
    by_fmt = {st.fmt: st for st in r.streams.values()}
    for fmt, payload, _ in inputs:
        st = by_fmt[fmt]
        got = r.extract(st.sid)
        exact = got == payload
        check(f"{fmt}: complete, {len(st.data)} records, extraction exact", st.complete() and exact and not st.errors,
              f"complete={st.complete()} exact={exact} errors={st.errors}")
    if "otlp-traces" in by_fmt:
        check("otlp-traces: message framing yields 3 documents", len(r.extract(by_fmt["otlp-traces"].sid)) == 3)
    sids = [rec.stream_id for rec in r.records if rec.kind == vc.KIND_DATA]
    check("records of different streams interleave", any(a != b for a, b in zip(sids, sids[1:])))

    print("2 decode")
    for fmt in ("jfr", "virel"):
        if fmt in by_fmt:
            rd = Reader(); rd.record = True
            quiet(rd.run, r.extract(by_fmt[fmt].sid))
            check(f"{fmt}: extracted stream parses ({rd.chunks} chunks, {len(rd.foreign_spans)} foreign)",
                  rd.chunks >= 1 and not rd.foreign_spans)
    if "go-trace" in by_fmt:
        good, msg = go_parses(r.extract(by_fmt["go-trace"].sid), "extracted.trace")
        check("go-trace: extracted stream parses with go tool trace", good, msg)

    print("3 resync")
    if "go-trace" in by_fmt:
        st = by_fmt["go-trace"]
        recs = st.records_by_seq()
        pre = [x for x in recs if x.flags & vc.F_PREAMBLE]
        gens = [x for x in recs if x.flags & vc.F_RESYNC]
        check(f"go-trace: 1 PREAMBLE record + {len(gens)} RESYNC generation records", len(pre) == 1 and len(gens) >= 3)
        mid = gens[len(gens) // 2].sequence
        good, msg = go_parses(r.resync(st.sid, mid), "resync.trace")
        check(f"go-trace: PREAMBLE + generation seq {mid} onward parses", good, msg)
        # Dropping a middle generation is invalid (spec §3.4.3), but the Go parser does not
        # always detect it: some traces fail ("inconsistent status"), others parse with the
        # generation silently missing. Reported, not gated: it is Go's behaviour, not ours.
        dropped = b"".join(x.payload(data) for x in recs if x.sequence != mid)
        bad, _ = go_parses(dropped, "dropmiddle.trace")
        print(f"  [INFO] go-trace: middle generation dropped -> go tool trace "
              f"{'ACCEPTED it (gap undetected)' if bad else 'rejected it'}")
    if "jfr" in by_fmt:
        st = by_fmt["jfr"]
        n = len(st.data)
        check(f"jfr: one record per chunk, {n} chunks (multi-chunk recording)", n >= 3 and all(x.flags & vc.F_RESYNC for x in st.data))
        full = jfr_chunks(r.extract(st.sid), "extracted.jfr")
        mid = n // 2
        tail = jfr_chunks(r.resync(st.sid, mid), "resync.jfr")
        check(f"jfr: JDK `jfr summary` reads the extracted stream ({full} chunks) and the resync from chunk {mid} ({tail} chunks)",
              full == n and tail == n - mid, f"full={full} tail={tail}")
    if "perfetto" in by_fmt:
        st = by_fmt["perfetto"]
        res = [x.sequence for x in st.records_by_seq() if x.flags & vc.F_RESYNC]
        check(f"perfetto: write-pass boundary gives a second RESYNC record ({res})", len(res) == 2)

    print("4 recover")
    crashes, prefix_bad = 0, 0
    step = max(1, len(data) // 97)
    for cut in list(range(vc.CONTAINER_HEADER, len(data), step)) + [len(data) - 1, len(data) - 30]:
        try:
            rc = vc.ContainerReader(data[:cut])
        except Exception as e:
            crashes += 1
            continue
        for fmt, payload, _ in inputs:
            st = next((s for s in rc.streams.values() if s.fmt == fmt), None)
            if st is None or st.gaps():
                continue
            got = rc.extract(st.sid)
            if isinstance(got, list):
                if any(a != b for a, b in zip(got[:-1], payload)):   # last doc may be cut
                    prefix_bad += 1
            elif not payload.startswith(got):
                prefix_bad += 1
        if rc.clean:
            prefix_bad += 1                                              # a cut file must not look clean
    check(f"truncation at {len(data) // step + 2} offsets: no crash, prefixes byte-exact, never clean",
          crashes == 0 and prefix_bad == 0, f"crashes={crashes} prefix_bad={prefix_bad}")
    datas = [x for x in r.records if x.kind == vc.KIND_DATA]
    victim = datas[len(datas) // 2]
    broken = bytearray(data); broken[victim.offset + 9] ^= 0xFF         # inside the static part
    rb = vc.ContainerReader(bytes(broken))
    vst = rb.streams[victim.stream_id]
    others = [s for s in rb.streams.values() if s.sid != victim.stream_id]
    check("corrupted record header: one damaged span, a gap at its sequence, other streams complete",
          len(rb.damaged) == 1 and vst.gaps() == [victim.sequence] and all(s.complete() for s in others),
          f"damaged={rb.damaged} gaps={vst.gaps()}")
    broken = bytearray(data); broken[victim.payload_start] ^= 0xFF
    rp = vc.ContainerReader(bytes(broken))
    check("corrupted payload byte: payload CRC mismatch, stream incomplete",
          any("payload CRC" in e for e in rp.errors) and not rp.streams[victim.stream_id].complete())
    check("record from scan: reader resumes on the next valid record after damage",
          len(rb.records) == len(r.records) - 1)

    print("5 live")
    lp = os.path.join(WORK, "live.virel")
    with open(lp, "wb") as f:
        w = vc.ContainerWriter(f, container_id=7)
        sid = w.add_stream("x-live")
        w.write(sid, b"first;")
        w.open_record(sid, vc.F_RESYNC)
        w.append(b"second-")
        snap = open(lp, "rb").read()
        w.append(b"part")
        w.close_record(end=True)
        w.close()
    rs = vc.ContainerReader(snap)
    last = rs.records[-1]
    check("live: OPEN last record, payload to EOF, stream not complete, not clean",
          last.state == vc.STATE_OPEN and last.payload(snap) == b"second-" and not rs.streams[sid].complete() and not rs.clean)
    rf = vc.ContainerReader(open(lp, "rb").read())
    check("live: after close, CLOSED, complete, byte-exact, manifest agrees",
          rf.clean and rf.streams[sid].complete() and rf.extract(sid) == b"first;second-part" and not rf.verify_manifest())

    print("6 model")
    import tcf_model
    model = quiet(tcf_model.to_model, data)
    errs = tcf_model.lint(model)
    ndec = sum(1 for st in model["streams"] if st["chunks"] is not None)
    check(f"Avro model of the packed container lints clean ({len(model['streams'])} streams, {ndec} decoded)",
          not errs and ndec == sum(1 for f, _, _ in inputs if f in ("virel", "jfr")), errs[:3])

    print("7 kaitai")
    kaitai_check(data, r, by_fmt, check, skipped)

    print("GATE 6:", "PASSED" if ok else "FAILED", "(skipped: " + ", ".join(s for s, _ in skipped) + ")" if skipped else "")
    return 0 if ok else 1

if __name__ == "__main__":
    try:
        sys.exit(main())
    finally:
        if not KEEP_WORK:
            shutil.rmtree(WORK, ignore_errors=True)
