---
id: find-rotation-amortization
title: Measured: pool-generation rotation cadence dominates output bytes; cheap event references alone do not
kind: finding
tags: [rotation, pools, benchmark, writer]
applies_to: [benchmarks/, doc/writer-design.md]
source: jfr-spec/find-rotation-amortization
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


In the native writer fixture (one producer, 64-value registered workload):
median 17.5 bytes/event without rotation, 25.8 with a 10 ms rotation target,
87.7 with a 1 ms target — because each chunk repeats the full schema and
pool definitions (definitions grew from 64 per run to ~39k across 40k
events; at 1 ms nearly every event carries a repeated string definition).
Lesson: chunk/pool reset cadence is a first-order output-size knob;
pooled references alone do not guarantee compact output. Scope: this
fixture and producer-local slicing; cross-producer dedup and different
cadences change the result. Open consequence to investigate: periodic
partial-buffer publication without forcing chunk rotation, measuring
producer overhead and capture-to-publication delay separately.
