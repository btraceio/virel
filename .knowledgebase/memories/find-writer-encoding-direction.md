---
id: find-writer-encoding-direction
title: Accepted writer direction: producer-side encoding with pooled references; background drain and publication
kind: finding
tags: [writer, decision, cpu, pools]
applies_to: [benchmarks/, doc/writer-design.md]
source: jfr-spec/find-writer-encoding-direction
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


Priority order: minimum producer (application-thread) latency first; a small
potential latency win must not be bought with potentially substantial extra
CPU. Accepted direction: producers encode events with pooled references;
background writers drain buffers, publish chunks and perform sink I/O. Full
staging (encode on the consumer side) remains an experimental alternative,
not a rejected one — it is an accepted preference, not proof that staging
has a measured CPU penalty. Measurement discipline going forward: separate
registered handles from ordinary pool lookups and misses; focus metrics on
allocation, contention, misses and rotation.
