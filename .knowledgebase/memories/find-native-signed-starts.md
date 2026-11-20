---
id: find-native-signed-starts
title: Accepted: native chunk-relative start offsets are signed (ZigZag varint); durations stay unsigned — propagation still pending
kind: finding
tags: [timestamps, native, pending]
applies_to: [doc/TraceContainerFormat.md, validators/tcf_reader.py]
source: jfr-spec/find-native-signed-starts
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


Completion-time recording of nested duration events can append an earlier
start after a later one, and starts may precede the chunk anchor. Accepted
decision: native chunk-relative start offsets are SIGNED, ZigZag-encoded
through the existing nine-byte varint; durations remain unsigned; append
order is preserved independently of time order. The JFR annex keeps
absolute ticks and its byte-frozen encoding. STATUS: accepted in discussion;
propagation to the spec text, schemas, readers and converters is still
pending — check WORK.md / spec before relying on it.
