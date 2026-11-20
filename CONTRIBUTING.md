# Contributing to VIREL

VIREL is a draft binary format for fine-grained runtime event traces, with
reference readers and verification gates. Thank you for helping improve it.

## What the repository contains

| Path | What it is |
|---|---|
| `doc/TraceContainerFormat.md` | the normative specification (draft 0.1) |
| `doc/tcf-rfc.md` | the RFC-style draft for circulation |
| `doc/` (`.ksy`, `.avsc`, `.cddl`, `.proto`) | machine-readable artifacts |
| `validators/` | reference readers, container writer/reader and gates |
| `benchmarks/` | the native writer prototype and its runners |
| `.knowledgebase/memories/` | distilled design decisions and constraints |

## Before you change anything

Read the relevant part of the specification. The protocol is draft **0.1**
and is revised in place: design changes do not consume new version numbers,
and the version constants in `doc/` must not be bumped.

Two writing rules apply:

- `doc/TraceContainerFormat.md` and `doc/tcf-rfc.md` follow ASD-STE100
  (controlled English): short sentences, one idea per sentence, approved
  verbs only. Keep that register when editing them.
- Behavioral claims in documentation must be backed by the code, a gate, or
  a benchmark. If you cannot point at one, mark the claim as a proposal.

## Verify your change

Run everything from the repository root. Each gate prints a final pass
line. Full details: [AGENTS.md](AGENTS.md).

| Change | Minimum verification |
|---|---|
| Spec or RFC text only | the cross-reference check in AGENTS.md; keep the RFC in sync |
| `doc/*.ksy` | Kaitai compile (python, java); gates 6 and 7 |
| Avro model builder or `validators/tcf_model.py` | build + no drift; gates 3 and 6 |
| `validators/tcf_reader.py`, `validators/virel_container.py` | selftest; all gates 2-7 |
| One format gate | that gate plus its fixture; selftest |
| `benchmarks/generation_writer.cpp` | release build with 0 warnings; gate 7; gate 6; benchmark suites in verify mode |

The quickest checks need only Python 3:

```sh
python3 -B validators/tcf_reader.py --selftest
python3 -B validators/perfetto_gate.py
```

Some gates need Go, a JDK or Node.js and read fixtures from `/tmp`;
`validators/fixtures/make_fixtures.sh` regenerates them.

## Pull requests

- One logical change per PR. Describe what changed and why, and list the
  gates you ran with their pass lines.
- Add a fix or a test for every behavior change. Benchmark claims need
  numbers from the runners, not estimates.
- Keep generated or scratch artifacts out of the diff.
