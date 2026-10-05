#!/usr/bin/env python3
"""virl_stream_gate.py — GATE 7: VIRL as a file kind of its own, and streaming chunks (spec §3.1, §3.9, §3.9.6).

Producer: the C++ native writer prototype (benchmarks/generation_writer.cpp), with signal-handler
stack samples and 20 ms generation rotation, built into a work directory.

  1 pipe     --streaming 1 writes through a FIFO (no seek possible) and completes; the finalized
             mode on the same FIFO fails at its header back-patch (the problem streaming solves)
  2 valid    both outputs pass benchmarks/validate_generation_output.py (independent chunk
             attachment, pools, stacks, summaries, byte accounting); tcf_reader reads them
  3 tail     prefixes of the streaming file (a live reader's views): every chunk whose
             END_OF_PARTITION is present is complete and parses; the tail chunk is incomplete;
             nothing crashes
  4 concat   finalized + streaming + finalized concatenated: one valid VIRL stream
  5 virc     streaming VIRL packed into VIRC one chunk per record, VIRC itself written to a FIFO
             (container streaming profile: CLOSED records only); extraction byte-exact; sniffing
  6 final    finalization: every complete streaming chunk gets size + last snapshot, STREAMING
             cleared last, FINAL-CHUNK on the last chunk; only those header bytes change;
             idempotent; in place == copy; validates as finalized output; a truncated file keeps
             its incomplete tail streaming; a mid-finalization header (size set, flag still set)
             reads correctly
  7 kaitai   the generated parser walks streaming chunks to END_OF_PARTITION and sizes finalized
             ones (skipped without KSC/kaitaistruct)
"""
import contextlib, io, json, os, shutil, subprocess, sys, tempfile, threading

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "benchmarks"))
import virel_container as vc
from tcf_reader import Reader, chunk_extent
from validate_generation_output import validate

KEEP_WORK = bool(os.environ.get("VIREL_GATE_WORK"))
WORK = os.environ.get("VIREL_GATE_WORK") or tempfile.mkdtemp(prefix="virl-stream-")
ARGS = ["--mode", "generation", "--events", "20000", "--producers", "4", "--rotate-us", "20000",
        "--signal-hz", "1000", "--verify"]

def through_fifo(argv, out_path):
    """run the writer with --output on a FIFO drained by a reader thread; returns (rc, stdout, stderr)"""
    fifo = os.path.join(WORK, "fifo")
    if os.path.exists(fifo):
        os.unlink(fifo)
    os.mkfifo(fifo)
    def drain():
        with open(fifo, "rb") as src, open(out_path, "wb") as dst:
            shutil.copyfileobj(src, dst)
    t = threading.Thread(target=drain); t.start()
    r = subprocess.run(argv + ["--output", fifo], capture_output=True, text=True, timeout=120)
    t.join(timeout=60)
    return r.returncode, r.stdout, r.stderr

def chunks_of(data):
    out, p = [], 0
    while p < len(data):
        n, ok = chunk_extent(data, p)
        out.append((p, n, ok))
        p += n
    return out

def read_quiet(data):
    rd = Reader(); rd.record = True
    with contextlib.redirect_stdout(io.StringIO()):
        rd.run(data)
    return rd

def main():
    ok = True
    def check(label, cond, detail=""):
        nonlocal ok
        ok &= bool(cond)
        print(f"  [{'OK' if cond else 'FAIL'}] {label}" + (f" -- {detail}" if detail and not cond else ""))
    cxx = os.environ.get("CXX", "clang++")
    exe = os.path.join(WORK, "generation_writer")
    src = os.path.join(HERE, "..", "benchmarks", "generation_writer.cpp")
    b = subprocess.run([cxx, "-std=c++17", "-O2", "-Wall", "-Wextra", "-Wpedantic", "-pthread", src, "-o", exe],
                       capture_output=True, text=True, timeout=300)
    if b.returncode != 0:
        print("build failed:", b.stderr[-400:]); print("GATE 7: FAILED"); return 1
    print(f"work dir {WORK}")

    print("1 pipe")
    stream_path = os.path.join(WORK, "stream.virl")
    rc, out, err = through_fifo([exe] + ARGS + ["--streaming", "1"], stream_path)
    check("streaming writer completes through a FIFO", rc == 0, err[-200:])
    stream_row = json.loads(out) if rc == 0 else None
    rc2, _, err2 = through_fifo([exe, "--mode", "generation", "--events", "2000", "--producers", "2",
                                 "--rotate-us", "20000"], os.path.join(WORK, "finalized-fifo.virl"))
    check("finalized writer fails on the same FIFO at its header back-patch", rc2 != 0 and "header write" in err2,
          f"rc={rc2} {err2[-120:]}")
    final_path = os.path.join(WORK, "final.virl")
    r = subprocess.run([exe] + ARGS + ["--output", final_path], capture_output=True, text=True, timeout=120)
    final_row = json.loads(r.stdout)

    print("2 valid")
    sdata, fdata = open(stream_path, "rb").read(), open(final_path, "rb").read()
    for name, path, row in (("streaming", stream_path, stream_row), ("finalized", final_path, final_row)):
        try:
            res = validate(path, row)
            check(f"{name}: validator passes ({res['chunks']} chunks, {res['committed']} events, "
                  f"{res['samples']} samples, {res['bytes']} B)", True)
        except Exception as e:
            check(f"{name}: validator passes", False, repr(e))
    hdr = [(sdata[p + 8:p + 16], sdata[p + 67]) for p, _, _ in chunks_of(sdata)]
    check("streaming: every chunk header has size 0 and the STREAMING flag",
          all(sz == bytes(8) and fl & 0x04 for sz, fl in hdr))
    rd = read_quiet(sdata)
    check(f"streaming: tcf_reader reads {rd.chunks} chunks, no foreign spans",
          rd.chunks == stream_row["chunks"] and not rd.foreign_spans)

    print("3 tail")
    crashes, bad = 0, 0
    whole = chunks_of(sdata)
    step = max(1, len(sdata) // 61)
    for cut in range(77, len(sdata), step):
        pre = sdata[:cut]
        try:
            parts = chunks_of(pre)
        except Exception:
            crashes += 1; continue
        complete = [x for x in parts if x[2]]
        expect = sum(1 for p, n, _ in whole if p + n <= cut)
        if len(complete) != expect or (parts and parts[-1][2] and parts[-1][0] + parts[-1][1] != cut):
            bad += 1
        if complete:
            end = complete[-1][0] + complete[-1][1]
            try:
                if read_quiet(pre[:end]).chunks != len(complete):
                    bad += 1
            except Exception:
                crashes += 1
    check(f"live tail at {len(range(77, len(sdata), step))} prefixes: complete chunks = chunks whose "
          f"END_OF_PARTITION arrived, all parse; no crash", crashes == 0 and bad == 0,
          f"crashes={crashes} bad={bad}")

    print("4 concat")
    cat = fdata + sdata + fdata
    parts = chunks_of(cat)
    rd = read_quiet(cat)
    n = final_row["chunks"] * 2 + stream_row["chunks"]
    check(f"finalized+streaming+finalized: {len(parts)} chunks, all complete, reader parses {rd.chunks}",
          len(parts) == n and all(x[2] for x in parts) and rd.chunks == n and not rd.foreign_spans)

    print("5 virc")
    virc_path = os.path.join(WORK, "packed.virc")
    fifo = os.path.join(WORK, "fifo2")
    os.mkfifo(fifo)
    got = {}
    def drain():
        with open(fifo, "rb") as s_:
            got["data"] = s_.read()
    t = threading.Thread(target=drain); t.start()
    vc.collect(fifo, [("virel", sdata, [("producer", "generation_writer --streaming")])], interleave=False)
    t.join(timeout=60)
    open(virc_path, "wb").write(got["data"])
    cr = vc.ContainerReader(got["data"])
    st = next(iter(cr.streams.values()))
    check(f"VIRC written through a FIFO (CLOSED records only): clean, {len(st.data)} records = chunks, byte-exact",
          cr.clean and st.complete() and len(st.data) == stream_row["chunks"] and cr.extract(st.sid) == sdata
          and all(x.state == vc.STATE_CLOSED for x in cr.records))
    check("sniff: VIRC container vs VIRL file vs bare JFR/Go",
          (vc.sniff(got["data"]), vc.sniff(sdata), vc.sniff(fdata), vc.sniff(b"FLR\0..."), vc.sniff(b"go 1.26 trace\0\0\0"))
          == ("virc", "virl", "virl", "jfr", "go-trace"))

    print("6 final")
    fin, nfin = vc.finalize_virl(sdata)
    starts = [p for p, _, _ in chunks_of(sdata)]
    allowed = set()
    for p in starts:
        allowed.update(range(p + 8, p + 24)); allowed.add(p + 67)
    changed = [i for i in range(len(sdata)) if sdata[i] != fin[i]]
    check(f"finalize: {nfin} chunks; {len(changed)} changed bytes, all in header size/last-snapshot/flags",
          nfin == stream_row["chunks"] and changed and set(changed) <= allowed and len(fin) == len(sdata))
    heads = [(int.from_bytes(fin[p + 8:p + 16], "big"), fin[p + 67]) for p in starts]
    check("finalize: every header finalized (size > 0, STREAMING clear), FINAL-CHUNK only on the last",
          all(sz > 0 and not fl & 0x04 for sz, fl in heads)
          and [bool(fl & 0x02) for _, fl in heads] == [False] * (len(heads) - 1) + [True])
    again, n2 = vc.finalize_virl(fin)
    inplace = os.path.join(WORK, "inplace.virl")
    open(inplace, "wb").write(sdata)
    vc.finalize_virl_file(inplace)
    check("finalize: idempotent, and in place == copy", again == fin and n2 == 0 and open(inplace, "rb").read() == fin)
    try:
        res = validate(inplace, dict(stream_row, streaming=False))
        check(f"finalize: output validates as finalized ({res['chunks']} chunks)", True)
    except Exception as e:
        check("finalize: output validates as finalized", False, repr(e))
    cut = starts[len(starts) // 2] + 500
    tf, tn = vc.finalize_virl(sdata[:cut])
    tparts = chunks_of(tf)
    check(f"finalize truncated file: {tn} complete chunks finalized, incomplete tail stays streaming, no FINAL-CHUNK",
          tn == len(starts) // 2 and tparts[-1][2] is False and tf[tparts[-1][0] + 67] & 0x04
          and not any(tf[p + 67] & 0x02 for p, _, _ in tparts))
    mid = bytearray(sdata)
    p0, n0, _ = chunks_of(sdata)[0]
    mid[p0 + 8:p0 + 16] = n0.to_bytes(8, "big")           # size written, STREAMING not yet cleared
    check("mid-finalization header (size set, flag still set): extent from size, same chunk",
          chunk_extent(bytes(mid), p0) == (n0, True))
    rd = read_quiet(fin + sdata)
    check("finalized + streaming concatenation parses", rd.chunks == 2 * stream_row["chunks"] and not rd.foreign_spans)

    print("7 kaitai")
    ksc = os.environ.get("KSC") or shutil.which("kaitai-struct-compiler")
    try:
        import kaitaistruct
    except ImportError:
        ksc = None
    skipped = []
    if not ksc:
        skipped.append("kaitai"); print("  [SKIP] kaitai: compiler or runtime not available")
    else:
        ksy = os.path.join(HERE, "..", "doc", "tcf-framing.ksy")
        outd = os.path.join(WORK, "ksy")
        c = subprocess.run([ksc, "-t", "python", "--outdir", outd, ksy], capture_output=True, text=True, timeout=300)
        check("kaitai: tcf-framing.ksy compiles", c.returncode == 0, c.stderr[-200:])
        sys.path.insert(0, outd)
        from tcf_framing import TcfFraming
        from kaitaistruct import KaitaiStream
        ks = TcfFraming.ChunkStream(KaitaiStream(io.BytesIO(cat)))
        # `events` exists only on streaming chunks (conditional field in the .ksy)
        sizes = [(len(getattr(ch, "events", None) or []), ch.header.is_streaming) for ch in ks.chunks]
        streaming_ok = all(s_ and ch.events[-1].type_id.value == 8 for ch, (_, s_) in zip(ks.chunks, sizes) if s_)
        check(f"kaitai: concatenation parses ({len(sizes)} chunks, {sum(1 for _, s_ in sizes if s_)} streaming walked "
              f"to END_OF_PARTITION)", len(sizes) == n and streaming_ok
              and sum(1 for _, s_ in sizes if s_) == stream_row["chunks"])
        kf = TcfFraming.ChunkStream(KaitaiStream(io.BytesIO(fin)))
        check("kaitai: finalized chunks are sized (no event walk)",
              len(kf.chunks) == stream_row["chunks"] and not any(ch.header.is_streaming for ch in kf.chunks))
        k2 = TcfFraming.from_bytes(got["data"])
        check("kaitai: the VIRC container parses with the VIRC signature", len(k2.records) == len(cr.records))

    print("GATE 7:", "PASSED" if ok else "FAILED", f"(skipped: {', '.join(skipped)})" if skipped else "")
    return 0 if ok else 1

if __name__ == "__main__":
    try:
        sys.exit(main())
    finally:
        if not KEEP_WORK:
            shutil.rmtree(WORK, ignore_errors=True)
