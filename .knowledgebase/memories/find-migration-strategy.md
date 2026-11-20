---
id: find-migration-strategy
title: Migration ease is THE traction criterion, decomposed into four testable properties
kind: finding
tags: [adoption, migration, design]
applies_to: [doc/TraceContainerFormat.md, validators/]
source: jfr-spec/find-migration-strategy
check: python3 validators/container_gate.py
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


For the format to get traction it needs extremely easy migration from all
popular major formats. "Extremely easy" decomposes into four testable
properties, all verified by the gates:

1. **Expressible** — the data model subsumes each source (gates 2/3/4/5).
2. **Streaming single-pass, bounded memory** — in-band sizes + chunk-local
   pools (find-converter-cost).
3. **No infrastructure prerequisites** — no protoc, no schema registry, no
   codec dependencies, no random access beyond a chunk header read.
   In-stream declared types ARE the no-registry story.
4. **Bidirectional bootstrap** — emitters FROM VIREL to legacy views (CTEF
   JSON, pprof) keep existing UIs working before native support lands; the
   CTEF back-emitter round-trips a real Chrome trace exactly (gate 5).

Conformance ladder (spec §6.5): Tier A = lossless round-trip suites (jfr /
go-trace / perfetto); Tier B = documented shape transform (ctef, v8
cpuprofile). Remaining Tier B formats (perf.data, ftrace, CTF/LTTng, ETW)
are per-format future work with no design risk.
