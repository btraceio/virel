---
id: find-superset-goal
title: Superset goal: JFR, Go trace and Perfetto as sub-configurations; writer id discovered at chunk attach
kind: finding
tags: [goal, design, writer-id]
applies_to: [doc/TraceContainerFormat.md]
source: jfr-spec/find-superset-goal
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


The format's design goal: represent JFR, Go trace v2 and Perfetto as
sub-configurations of one format family, judged by lossless round-trip tests
(source trace -> mapped stream -> re-extraction reproduces information
content: events, ordering, interning, clock anchors, loss markers). Byte
compatibility is required only for JFR (byte-frozen annex); Go/Perfetto are
lossless-representation class. Success criterion: one reader implementation
plus the profile definitions validated by round-trip suites. Both Go
(batch header mID) and Perfetto (trusted_packet_sequence_id) carry writer
identities on every batch, so the native header grew a u64 random writer id
at offset 68 (76-byte native header) — writer identity must be discoverable
at chunk attach, before any event.
