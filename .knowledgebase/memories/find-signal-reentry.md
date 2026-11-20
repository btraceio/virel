---
id: find-signal-reentry
title: The S3 signal table tolerates re-entrant delivery; lanes do not — hence one lane per nesting level
kind: finding
tags: [signals, reentrancy, lock-free]
applies_to: [benchmarks/generation_writer.cpp]
source: jfr-spec/find-signal-reentry
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


A handler can be interrupted on its own thread by another signal (second
profiling source, or SA_NODEFER). The S3 table tolerates this: every
operation is an atomic RMW or acquire load plus stack locals, nothing waits.
A nested handler inserting the same stack while the outer holds a reserved
slot publishes first; the outer's CAS fails and it reuses the equal entry
(wasting its slot) — that is the only cross-thread race path, and it is
benign. The per-thread lane does NOT tolerate re-entry (single-writer
cursor/seal/hazard slot), so there is one lane per nesting level, claimed by
an atomic depth counter; nested handlers finish before the outer resumes, so
levels form a stack; arrivals past the last level are dropped and counted.
Verified by deterministic SIGPROF injection with SA_NODEFER at four points
(handler start, mid-insert, between space check and append, mid-append);
release and ASan/UBSan outputs valid. TSan defers signals and cannot
exercise nesting — use ASan/UBSan for signal paths.
