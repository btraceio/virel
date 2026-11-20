---
id: find-event-scope
title: VIREL is event-oriented only; OTLP traces/logs are carried byte-exactly, never converted
kind: finding
tags: [scope, otlp, pprof, design]
applies_to: [doc/TraceContainerFormat.md, validators/pprof_gate.py]
source: jfr-spec/find-event-scope
check: python3 validators/pprof_gate.py
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


VIREL records and carries timestamped event streams (JFR, Go trace,
Perfetto, trace-event JSON, OTLP spans/logs). Formats without timestamped
events are out of scope: aggregated profiles (pprof, OTLP Profiles), heap
snapshots (HPROF, V8 heap profiles), folded stacks. Fitting pprof into the
event model forced invented filler time words — that misfit is why the scope
was cut. Consequences: no converted VIREL form for OTLP traces/logs — they
ride byte-exactly as `otlp-traces`/`otlp-logs` (+ `-json`) streams;
"replacing OTLP as a telemetry protocol" is an explicit non-goal (VIREL is
the recording/storage container; OTLP is the export protocol it carries).
Because OTLP export requests do not concatenate, the descriptor gained
`record-framing`: `stream` (default, records concatenate) vs `message` (each
DATA record is one whole document). Gate 4 survives as a wire regression
test only (pooled-ref split, Varint128, pool skip), with dynamic type ids.
