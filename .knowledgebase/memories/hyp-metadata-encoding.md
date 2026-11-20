---
id: hyp-metadata-encoding
title: Metadata encoding is per-event selectable; header @66 default is advisory, the envelope tag is authoritative
kind: finding
tags: [metadata, schema, encoding, design]
applies_to: [doc/TraceContainerFormat.md]
source: jfr-spec/hyp-metadata-encoding
check: python3 validators/tcf_reader.py --selftest
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


Schema model and schema serialization are decoupled. Every metadata event
payload starts with `[varint encoding-id][payload]` — the authoritative
selector (~1 byte; metadata events are rare). The native header's @66 high
byte advertises the chunk's default encoding for fast sniffing by tools, but
is advisory only. Registry (add-only): 0 = binarized-XML v2 (legacy, JFR
subset), 1 = flat typed descriptor, 2 = protobuf, 3 = CBOR, 4 = JSON.
Within-chunk encoding changes are permitted but discouraged. Every encoding
deserializes into the same internal type model, so parser construction is
encoding-independent, and a conforming reader implements every registered
encoding. Ruled out: metadata encoding as a stream-global constant; any
out-of-band schema requirement.
