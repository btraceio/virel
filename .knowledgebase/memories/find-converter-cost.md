---
id: find-converter-cost
title: Reference converters are cheap by construction and double as the conformance validators
kind: finding
tags: [converters, streaming, validators, design]
applies_to: [validators/, doc/TraceContainerFormat.md]
source: jfr-spec/find-converter-cost
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


Conversion into VIREL is single-pass and bounded by construction, and the
round-trip conformance validators and the reference converters are the same
artifact (the ingest leg of each suite is the converter).

- **Go trace v2 -> VIREL**: one Go generation -> one native chunk. Go's own
  table batches arrive at generation end, so any streaming interpreter must
  buffer one generation — inherent to the SOURCE format. Memory bound =
  generation size (Go targets ~16 MiB partitions).
- **Perfetto -> VIREL**: pure streaming, no buffering. InternedData arrives
  inline in the packet that first references it; the converter emits it as a
  state-snapshot event immediately before that packet's events (mid-chunk
  snapshots are allowed). Loss/clear markers map to loss sync kinds;
  compressed packets decompress to plain.
- Both are table-driven (precomputed event-code/type registrations); no
  schema inference at conversion time. Converter assigns/propagates u64
  writer ids (Go mID, Perfetto sequence id) through the header slot.

Ruled out: "conversion requires full-trace parse" (worst case is Go's own
generation granularity); separate validator implementations.
