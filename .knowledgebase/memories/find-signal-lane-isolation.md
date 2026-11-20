---
id: find-signal-lane-isolation
title: Signal-handler recording requires isolated per-thread lanes; the normal path must never be re-entered
kind: finding
tags: [signals, writer, design, constraint]
applies_to: [benchmarks/generation_writer.cpp, doc/writer-design.md]
source: jfr-spec/find-signal-lane-isolation
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


A profiling signal handler interrupts its thread at any instruction. It must
never touch the interrupted thread's normal buffer, pool slice or hazard
slot; making the normal path re-entrant would tax every event. Therefore:
a per-thread signal lane with its own buffers, committed cursor, hazard
slot and nesting guard; initial-exec TLS; registration before the first
signal; retirement with the signal blocked; drop-and-count on any
unavailable resource. Platforms: Linux and macOS (not Windows). Stack
strategy for lanes: resolved to the S3 global lock-free table (see
find-signal-stack-decision).
