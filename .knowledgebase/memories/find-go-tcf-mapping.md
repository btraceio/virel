---
id: find-go-tcf-mapping
title: Go trace v2 wire facts for gate 3 (corrected iota table, per-batch anchoring, generation is the chunk unit)
kind: finding
tags: [go, tracev2, gate3, converter]
applies_to: [validators/go_gate.py, doc/TraceContainerFormat.md]
source: jfr-spec/find-go-tcf-mapping
check: sh validators/fixtures/make_fixtures.sh && python3 validators/go_gate.py /tmp/tcf-gotrace.out --full
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


Wire facts verified byte-level against Go 1.26-format traces (GOROOT
internal/trace + tracev2 read completely, cross-checked with
`go tool trace -d=wire`):

- Header = `go 1.%d trace\0\0\0` (16 B). FORMAT version != compiler version:
  Go 1.27 writes `go 1.26 trace` (ver 26). Reader accepts 22/23/25/26; v1
  (11/19/21) is a different parser.
- Corrected iota table: timed kinds START AT 9 (ProcsChange=9 ... GoStatus
  Stack=48); 1 EventBatch, 2 Stacks, 4 Strings, 6 CPUSamples, 8 Frequency,
  49 ExperimentalBatch (exp byte precedes gen), **50 Sync, 51 ClockSnapshot,
  52 EndOfGeneration = a bare frame byte closing the generation**. A
  transcription off-by-one was caught by raw bytes — always verify iota
  tables against `go tool trace -d=wire`.
- Batch framing = header-level only: [kind][gen][m][time][size uvarint],
  size max 64 KiB; no size framing inside payloads.
- Timestamps are PER-BATCH anchored: abs tick = batch.time + dt; the cursor
  re-anchors per batch, never accumulating across batches of an M.
- Frequency f (uvarint) = TICKS PER SECOND (ns/tick = 1e9/f).
- M id sentinel NoThread = 2**64-1. Go string id 0 means "no string"
  (internal/trace/base.go) — it maps to VIREL pool key 0 (null).
- `go tool trace` SYNTHESIZES non-wire events (first/last Sync, some initial
  M states) — cross-checks must exclude them.

Mapping: one generation = one native chunk (tables are valid per
generation); writer id derived from (gen, m); EndOfGeneration ->
END_OF_PARTITION; per-gen string/stack tables -> chunk pools; stacks ->
pooled composite `tcf2.stack`; track ids namespaced by kind. Full fidelity
criterion: per (gen, M) event sequence, args, and exact abs ticks (1 ns).
