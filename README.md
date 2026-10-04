# VIREL

Versioned In-band Runtime Event Ledger: a draft binary format for fine-grained
runtime observability, designed for low-overhead recording and self-contained,
self-describing chunks.

The protocol remains draft **0.1**, revised in place during design. Native magic
is `VIRL`; `FLR\0` belongs to the byte-frozen JFR annex. Bootstrap records are
Metadata and State Snapshot, with optional cumulative event-type counts in the
latter.

- [Protocol specification](doc/TraceContainerFormat.md)
- [RFC](doc/tcf-rfc.md)
- [Current work and open writer issues](WORK.md)
- [Investigation index](INDEX.md) and [historical state](STATE.md)
- [Schemas and framing artifacts](doc/)
- [Reference readers and conversion validators](validators/)

## Run the self-contained checks

Python 3 is required; run from the repository root:

```sh
python3 -B validators/tcf_reader.py --selftest
python3 -B validators/perfetto_gate.py
python3 -B validators/build_tcf_model_avsc.py
```

The other conversion gates require external recordings and producer tools;
their historical `/tmp` fixture paths are not bundled test data. Existing gate
results establish representation/fidelity, not native recording performance.
Historical `tcf` filenames remain to preserve links and imports.

## Origin

Extracted on 2026-10-04 from `.investigations/jfr-spec` in an OpenJDK checkout at
commit `b22ccb9af5fc7d097f5bf24e0e87a1c02c5810dd`.
Evidence nodes citing `src/hotspot/...` or `src/jdk.jfr/...` refer to that
[OpenJDK source tree](https://github.com/openjdk/jdk/tree/b22ccb9af5fc7d097f5bf24e0e87a1c02c5810dd),
not files in this repository. Historical `.investigations/jfr-spec/` paths now
resolve relative to this repository's root. No OpenJDK source tree is copied.
