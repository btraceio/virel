---
id: find-generic-tracing
title: No JVM concept is load-bearing in the native format; any runtime declares its vocabulary in-stream
kind: finding
tags: [design, schema, generic]
applies_to: [doc/TraceContainerFormat.md]
source: jfr-spec/find-generic-tracing
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


The format generalizes beyond the JVM because none of its required
properties is JVM-specific: metadata events declare arbitrary types/fields
(only bootstrap ids 0 and 1 are reserved); poolability is schema-driven
(declared poolable types, not hardcoded names); time is chunk-anchored
(chunk-local ticks + ticksPerSecond + UTC start, no clock assumptions);
chunks are self-contained writer boundaries, so heterogeneous producers
coexist by concatenation; chunk/pool cadence is producer policy. Live proof:
async-profiler already declares its own `profiler.*` event types alongside
JDK types in one .jfr. Limits to keep documented: no native map type
(attributes = arrays of key/value composites); one time axis per chunk
(cross-stream skew is a consumer concern, resolved via clock anchors);
byte-array fields for attachments, not a bulk-blob channel.
