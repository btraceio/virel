# VIREL — Versioned In-band Runtime Event Ledger

VIREL is a binary format for recording runtime event traces — CPU samples,
execution spans, counters, logs — with three properties that existing trace
formats trade off against each other:

- **Low overhead at write time.** Chunked, self-describing records with
  in-band varint framing and chunk-local interning — the same write-path
  economics as JFR and Go execution traces.
- **Every stream extracts byte-exactly.** A VIRC container carries native
  VIREL, JFR, Go traces, Perfetto and OTLP side by side, without converting
  any of them.
- **Migration first.** An existing producer's output becomes VIREL data by
  being framed as a stream — no encoder change, no schema registry, no
  protoc, no codec dependencies.

> **Status: draft 0.1.** The protocol is revised in place during design;
> some container numerics are still provisional (marked in the spec).
> Not yet suitable as a stable storage format.

## The model in one minute

VIREL has two file kinds, told apart by their first bytes:

```
VIRL — one native event stream         VIRC — a container of framed streams
┌────────┬────────┬────────┬─────┐      ┌──────────┬─────────┬─────────┬───┐
│ chunk  │ chunk  │ chunk  │ ... │      │ 32-byte  │ STREAM  │  DATA   │...│
└────────┴────────┴────────┴─────┘      │ header   │ record  │ records │   │
  concatenable: cat a.virl b.virl        └──────────┴─────────┴─────────┴───┘
  writable without seeking (pipes,          each record frames one payload
  sockets, append-only storage)             stream: virel, jfr, go-trace,
                                            perfetto, otlp-traces, ...
```

- A **VIRL file** is what a runtime writes directly: a concatenation of
  self-contained chunks, each carrying its own metadata, pools and clock
  anchor. Files from different writers concatenate safely.
- A **VIRC file** is what a collector writes: a header, then records, each
  framing one stream of one payload format. The container never parses
  payload bytes; it indexes, timestamps and extracts streams exactly as
  they were written.

A conforming reader reads VIRL files and `virel` streams. Decoding anything
else — JFR, Go traces, Perfetto — is optional and plugged in per format.

## Try it

The reference readers and seven verification gates live in `validators/`.
The quickest checks need only Python 3:

```sh
python3 -B validators/tcf_reader.py --selftest      # native chunks + foreign bridging
python3 -B validators/perfetto_gate.py              # Perfetto → VIREL round trip
```

Gates 3-5 add Go, JDK and Node.js fixtures (`validators/fixtures/
make_fixtures.sh` regenerates them); gates 6-7 exercise the VIRC container
and VIRL streaming end to end. Every gate prints a final pass line — the
full matrix of what each gate checks, and which gate to run for which
change, is in [AGENTS.md](AGENTS.md).

The native writer prototype (C++, ASan/UBSan-clean, signal-handler safe)
and its benchmark suites are in `benchmarks/` — see
[benchmarks/README.md](benchmarks/README.md).

## Documentation

Start here if you are new; read the spec when you need the wire:

1. [tutorials/01-mental-model.md](tutorials/01-mental-model.md) — the
   problem VIREL solves, the two file kinds, and how chunks carry their own
   schema.
2. [tutorials/02-reading-virl.md](tutorials/02-reading-virl.md) — walk a
   real VIRL file byte by byte with the reference reader.
3. [tutorials/03-writing-virl.md](tutorials/03-writing-virl.md) — produce a
   native stream and validate it.
4. [tutorials/04-virc-container.md](tutorials/04-virc-container.md) — pack
   a JFR file, a Go trace and a native stream into one container, and
   extract them byte-exactly.
5. [tutorials/05-glossary.md](tutorials/05-glossary.md) — every term, one
   line each.

Then:

- [doc/TraceContainerFormat.md](doc/TraceContainerFormat.md) — the
  normative specification. Section 0 is the overview; the wire starts at
  §3.
- [doc/tcf-rfc.md](doc/tcf-rfc.md) — the same protocol as an RFC-style
  draft for review.
- [doc/writer-design.md](doc/writer-design.md) — how the native writer is
  structured and why.
- `.knowledgebase/memories/` — the distilled design decisions behind the
  spec: why `FLR\0` stays JFR-only, why type-id bindings are add-only, what
  the writer benchmarks established.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md): which gates to run for which
change, and the writing rules for the specification.

## License

[Apache-2.0](LICENSE).
