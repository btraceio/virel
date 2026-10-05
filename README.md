# VIREL

Versioned In-band Runtime Event Ledger: a draft binary format for fine-grained
runtime observability, designed for low-overhead recording and self-contained,
self-describing chunks.

The protocol remains draft **0.1**, revised in place during design. VIREL has two
file kinds: a **VIRL** file (magic `VIRL`) is one native event stream that is
concatenable and can be written without seeking (streaming chunks); a **VIRC** file
(signature `VIRC\r\n\x1a\n`) is a format-agnostic container whose streams may be
native VIREL, JFR (`FLR\0`, byte-frozen), Go trace, Perfetto, OTLP and others. Native bootstrap records are
Metadata and State Snapshot, with optional cumulative event-type counts in the
latter.

- [Protocol specification](doc/TraceContainerFormat.md)
- [RFC](doc/tcf-rfc.md)
- [Current work and open writer issues](WORK.md)
- [Writer capture/encoding benchmark](benchmarks/README.md)
- [Pooled generation/native writer benchmark](benchmarks/generation-writer.md)
- [Writer direction and proposed generation ownership](doc/writer-design.md)
- [Investigation index](INDEX.md) and [historical state](STATE.md)
- [Active investigation memory](.investigations/jfr-spec/INDEX.md) and
  [working state](.investigations/jfr-spec/STATE.md), maintained with Pi's
  investigation-memory workflow
- [Schemas and framing artifacts](doc/)
- [Reference readers and conversion validators](validators/)

## Run the self-contained checks

[AGENTS.md](AGENTS.md) is the complete guide to verification and benchmarks:
every gate, its prerequisites and fixtures (`validators/fixtures/`), its
expected pass line, which gates to run for which change, writer builds and
sanitizers, and how evidence is recorded. The quickest checks need only
Python 3; run them from the repository root:

```sh
python3 -B validators/tcf_reader.py --selftest
python3 -B validators/perfetto_gate.py
python3 -B validators/build_tcf_model_avsc.py
```

The other gates need Go, a JDK, Node.js or a C++ compiler, and some read `/tmp`
fixtures. `validators/fixtures/make_fixtures.sh` regenerates those fixtures,
except the real Chrome trace used by gate 5 (see AGENTS.md). Existing gate
results establish representation/fidelity, not native recording performance.
Historical `tcf` filenames remain to preserve links and imports.
