---
id: find-agnostic-container
title: The VIRC container never interprets payloads; streams extract byte-exactly
kind: finding
tags: [container, framing, design, migration]
applies_to: [doc/TraceContainerFormat.md, validators/virel_container.py]
source: jfr-spec/find-agnostic-container
check: python3 validators/container_gate.py
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


VIRC is a format-agnostic container: a stream's bytes are any payload format
(`virel`, `jfr`, `go-trace`, `perfetto`, OTLP, ...), framed identically, and
reconstructing a stream by concatenating its DATA records in sequence order
must yield the original bytes exactly. The container never parses payload
bytes and must not treat any format specially. Usage model: a collector
assembles finished inputs; exactly one writer per container at a time; only
`virel` decoding is mandatory — every other format decodes through optional
payload decoders (plugins) selected by the descriptor's `format` id, while
every stream stays listable, indexable and byte-exactly extractable. This is
the zero-conversion migration path: an existing producer's output becomes
VIREL data by being framed as a stream.
