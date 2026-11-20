---
id: find-subset-direction
title: One reader, version-keyed profiles; JFR is the byte-frozen legacy subset, read optionally
kind: finding
tags: [compatibility, jfr, design]
applies_to: [doc/TraceContainerFormat.md]
source: jfr-spec/find-subset-direction
check: python3 validators/tcf_reader.py --selftest
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


The reader model: one implementation of the reader spec reads the native
format and the JFR annex (majors 1.x/2.x as a byte-frozen, normative
sub-annex). Old JFR readers are NOT designed around — they fail fast at
pool/structure level on any meaningful extension, so compatibility with them
was abandoned deliberately. Since the container split this is further
loosened: only `virel` decoding is mandatory; `jfr` is an optional payload
decoder (plugin) — but JFR chunks share the native chunk grammar (§3.9),
which makes that plugin cheap. Legacy annex majors stay byte-frozen; the
native line decides its own encodings. Superset-in-v2-stream compatibility
designs are ruled out: old readers abort anyway.
