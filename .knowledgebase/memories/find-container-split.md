---
id: find-container-split
title: Two file kinds: VIRL (one native stream) and VIRC (container); VIRL chunks stream without seeking
kind: finding
tags: [container, streaming, framing, design]
applies_to: [doc/TraceContainerFormat.md, validators/virl_stream_gate.py]
source: jfr-spec/find-container-split
check: python3 validators/virl_stream_gate.py
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


VIREL has two file kinds, told apart by leading bytes: **VIRL** — one native
event stream, single-provenance, concatenable (`cat a.virl b.virl` is valid),
writable directly by producers; **VIRC** — the format-agnostic container
(signature `VIRC\r\n\x1a\n`). Streaming chunks (native only, spec §3.9.6)
remove the back-patched header, which was the real blocker for pipes: the
header is written once (size 0, STREAMING flag @67 bit 2), the chunk ends at
a mandatory END_OF_PARTITION event (type id 8), and readers find the extent
by walking size-prefixed events. A finalizer turns a streaming chunk into a
finalized one in place (writes size/snapshot/FINAL-CHUNK posthoc fields
first, clears STREAMING last); event bytes never change and finalization is
idempotent. Streaming = live transport; finalized = O(1) skipping and
distribution. On non-seekable VIRC sinks only CLOSED records are written.
GATE 7 covers all of it (FIFO writing, live tail, concatenation, sniffing,
finalize-on-close).
