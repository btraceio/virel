---
id: find-writer-scale-targets
title: Writer scale targets: 512-frame stacks (1024 must not degrade badly), 64 writer threads, Linux+macOS signals
kind: finding
tags: [requirement, scale, writer]
applies_to: [benchmarks/, doc/writer-design.md]
source: jfr-spec/find-writer-scale-targets
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


Planning targets for the native writer: max stack depth 512 frames (1024
must not degrade terribly); 64 concurrently writing threads as the planning
point (64+ core systems are rare); signal-handler recording on Linux and
macOS, not Windows. Consequences: fixed per-entry frame arrays are
infeasible at these depths — stack tables use a frame arena; staging raw
frames costs up to 8 KiB of lane buffer per sample at depth 1024; contention
and drain-thread capacity must be measured at 64 threads (a 12-CPU host
cannot show real 64-way concurrency — treat such measurements as noise for
contention claims).
