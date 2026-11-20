---
id: find-signal-stack-decision
title: Signal-lane stacks use the S3 global lock-free table with two nesting levels
kind: finding
tags: [signals, stacks, lock-free, decision]
applies_to: [benchmarks/generation_writer.cpp, doc/writer-design.md]
source: jfr-spec/find-signal-stack-decision
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


Decision for signal-handler stack recording: **S3 — a global lock-free
table written from handlers — with two nesting levels per thread.** At the
target scale (512/1024 frames, 64 threads) staging raw frames in the handler
saved 1-4 us of handler p50 but cost 3-12x background CPU per sample and
4-8 KiB lane records; S3 has ~18-byte lane records, cheap background work,
no table-age sensitivity and zero lost-CAS races in timed runs. Shape: per
generation — entry array, fixed frame arena, atomic buckets; handler hashes
and probes, on miss reserves a slot, fills it, release-CAS publishes; a lost
race reuses the equal entry; exhaustion drops the sample (no inline
fallback for pooled stack fields). The background defines referenced keys
before the events that use them. Two lanes per thread via an atomic depth
counter; deeper arrivals are dropped and counted. Pending: Linux many-core
CAS contention, ARM memory ordering, a real stack walker, lane ring sizes.
